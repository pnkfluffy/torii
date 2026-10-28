"""Native session lifetime follows durable Torii ownership, using fake homes only."""

import asyncio
from contextlib import nullcontext
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import control_api
from coordinator.store import Store
from tests.test_worker_storage import StorageSupport


SID = '00000000-0000-4000-8000-000000000001'
OTHER = '00000000-0000-4000-8000-000000000002'
THIRD = '00000000-0000-4000-8000-000000000003'
OPENCODE = 'ses_' + 'a' * 26


class SessionStorageTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.store = Store(self.root / 'state')
        self.addCleanup(lambda: self.store.close())
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'Test',?,1)",
                              (str(self.root),))
        self.job = self.store.task_create('1:2', 'Research')
        self.store.put('session_delete', True)
        self.home = self.root / 'profile'

    def worker(self, provider='claude', status='done', result=True, sid=SID, job=None):
        job = job or self.job
        cursor = self.store.db.execute('''INSERT INTO workers(task,topic,provider,prompt,session,cwd,
            status,result,created,updated,iteration) VALUES (?,'1:2',?,'test',?,?,?, ?,1,1,?)''',
            (job['id'], provider, sid, str(self.root), status,
             json.dumps({'success': True, 'session_id': sid}) if result else None, job['iteration']))
        worker = cursor.lastrowid
        if result:
            self.store.db.execute('''INSERT INTO messages(topic,kind,text,source_worker,created,updated)
                VALUES ('1:2','worker_result','result',?,1,1)''', (worker,))
        self.store.db.commit()
        return worker

    def close_round(self, outcome='done'):
        with self.store.db:
            self.store.db.execute('UPDATE tasks SET status=? WHERE id=?', (outcome, self.job['id']))
            self.store.db.execute('UPDATE task_iterations SET closed=2,outcome=? WHERE task=? AND k=0',
                                  (outcome, self.job['id']))

    def file(self, provider='claude', sid=SID, home=None):
        home = home or self.home
        relative = ('projects/-fixture/' + sid + '.jsonl' if provider == 'claude' else
                    'sessions/2026/01/01/rollout-2026-01-01T00-00-00-' + sid + '.jsonl')
        path = home / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b'fixture session data')
        return path

    def remember(self, worker=None, provider='claude', sid=SID, home=None, parent=None, created=True, host=None):
        from coordinator.session_storage import remember
        return remember(self.store, provider, sid, str(home or self.home), str(self.root),
                        worker=worker, parent=parent, created=created, host=host)

    async def sweep(self):
        from coordinator.session_storage import storage_for
        return await storage_for(self.store).sweep()

    def test_switch_is_off_by_default_strict_and_local(self):
        self.store.db.execute("DELETE FROM settings WHERE key='session_delete'")
        result = control_api.call(self.store, 'settings.get', source='cli')
        self.assertIs(result.data.get('session_delete'), False)
        result = control_api.call(self.store, 'cleanup.sessions', {'enabled': 'yes'}, source='cli')
        self.assertFalse(result.ok)
        result = control_api.call(self.store, 'cleanup.sessions', {'enabled': True}, source='mcp')
        self.assertFalse(result.ok)
        result = control_api.call(self.store, 'cleanup.sessions', {'enabled': True}, source='cli')
        self.assertTrue(result.ok, result)
        self.assertIs(self.store.get('session_delete'), True)

    async def test_closed_worker_files_deleted_for_claude_and_codex(self):
        for provider in ('claude', 'codex'):
            with self.subTest(provider=provider):
                path = self.file(provider)
                worker = self.worker(provider)
                self.remember(worker, provider)
                self.close_round()
                await self.sweep()
                self.assertFalse(path.exists())
                self.assertTrue(self.store.db.execute('SELECT result FROM workers WHERE id=?', (worker,)).fetchone()[0])

    async def test_open_waiting_and_active_rounds_keep_all_sessions(self):
        path = self.file()
        worker = self.worker()
        self.remember(worker)
        for status in ('open', 'waiting', 'interrupted'):
            with self.subTest(status=status):
                self.store.db.execute('UPDATE tasks SET status=? WHERE id=?', (status, self.job['id']))
                await self.sweep()
                self.assertTrue(path.exists())
        self.close_round()
        for status in ('queued', 'running', 'waiting_for_quota', 'waiting_for_secret'):
            other = self.worker(status=status, sid=OTHER, result=False)
            await self.sweep()
            self.assertTrue(path.exists(), status)
            self.store.db.execute("UPDATE workers SET status='interrupted',result='{}' WHERE id=?", (other,))
            self.store.db.execute('''INSERT INTO messages(topic,kind,text,source_worker,created,updated)
                VALUES ('1:2','worker_result','saved',?,1,1)''', (other,))

    async def test_result_and_durable_result_message_required(self):
        path = self.file()
        worker = self.worker(result=False)
        self.remember(worker)
        self.close_round('dropped')
        await self.sweep()
        self.assertTrue(path.exists())
        self.store.db.execute("UPDATE workers SET result='{}' WHERE id=?", (worker,))
        await self.sweep()
        self.assertTrue(path.exists())
        self.store.db.execute('''INSERT INTO messages(topic,kind,text,source_worker,created,updated)
            VALUES ('1:2','worker_result','saved',?,1,1)''', (worker,))
        await self.sweep()
        self.assertFalse(path.exists())

    async def test_closing_job_stores_results_for_cancelled_workers_before_cleanup(self):
        path = self.file()
        worker = self.worker()
        self.remember(worker)
        cancelled = [self.worker(status=status, result=False, sid=OTHER)
                     for status in ('queued', 'waiting_for_secret', 'waiting_for_quota')]
        self.store.task_update(self.job['id'], status='dropped', note='Owner stopped the job')
        for identifier in cancelled:
            row = self.store.db.execute('SELECT * FROM workers WHERE id=?', (identifier,)).fetchone()
            self.assertEqual(row['status'], 'interrupted')
            self.assertEqual(json.loads(row['result'])['failure_code'], 'owner_stopped')
            self.assertIsNotNone(self.store.db.execute('SELECT 1 FROM messages WHERE source_worker=?', (identifier,)).fetchone())
        await self.sweep()
        self.assertFalse(path.exists())

    async def test_closing_job_with_switch_off_keeps_existing_result_behavior(self):
        worker = self.worker(status='waiting_for_quota', result=False)
        self.store.put('session_delete', False)
        self.store.task_update(self.job['id'], status='dropped', note='Owner stopped the job')
        row = self.store.db.execute('SELECT * FROM workers WHERE id=?', (worker,)).fetchone()
        self.assertEqual(row['status'], 'interrupted')
        self.assertIsNone(row['result'])

    async def test_closing_job_preserves_result_of_steer_requeued_worker(self):
        from coordinator.service import Service
        worker = self.worker()
        self.store.db.execute('UPDATE workers SET fresh=0 WHERE id=?', (worker,))
        original = self.store.db.execute('SELECT result FROM workers WHERE id=?', (worker,)).fetchone()[0]
        result = control_api.call(self.store, 'workers.steer', {'worker': worker, 'prompt': 'Continue'}, source='cli')
        self.assertTrue(result.ok, result)
        service = Service(self.store, None, SimpleNamespace(), self.root)
        await service.controls_once()
        self.assertEqual(self.store.db.execute('SELECT status FROM workers WHERE id=?', (worker,)).fetchone()[0], 'queued')
        self.store.task_update(self.job['id'], status='dropped', note='Owner stopped the job')
        row = self.store.db.execute('SELECT status,result FROM workers WHERE id=?', (worker,)).fetchone()
        self.assertEqual(row['status'], 'interrupted')
        self.assertEqual(row['result'], original)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM messages WHERE source_worker=? AND kind='worker_result'",
                                               (worker,)).fetchone()[0], 1)

    async def test_unreadable_home_backs_off_once_per_home_and_recovers(self):
        from coordinator import session_storage
        first = self.file()
        second = self.file(sid=OTHER)
        self.remember(self.worker())
        self.remember(self.worker(sid=OTHER), sid=OTHER)
        other_home = self.root / 'other-profile'
        other = self.file(sid=THIRD, home=other_home)
        self.remember(self.worker(sid=THIRD), sid=THIRD, home=other_home)
        self.close_round()
        now = time.time()
        with patch.object(session_storage.time, 'time', return_value=now):
            with patch.object(session_storage, 'session_files', side_effect=PermissionError('fixture')) as scan:
                await self.sweep()
                self.assertEqual(scan.call_count, 2)
                retries = [row[0] for row in self.store.db.execute('SELECT retry_at FROM native_sessions')]
                self.assertEqual(len(set(retries)), 1)
                first_delay = retries[0] - now
                self.assertGreaterEqual(first_delay, 300)
                await self.sweep()
                self.assertEqual(scan.call_count, 2)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM problems WHERE area='cleanup'").fetchone()[0], 2)
        with patch.object(session_storage.time, 'time', return_value=now + first_delay + 1):
            with patch.object(session_storage, 'session_files', side_effect=ValueError('fixture ambiguity')) as scan:
                await self.sweep()
                self.assertEqual(scan.call_count, 2)
            second_retry = self.store.db.execute('SELECT MIN(retry_at) FROM native_sessions').fetchone()[0]
            self.assertGreater(second_retry - (now + first_delay + 1), first_delay)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM problems WHERE area='cleanup'").fetchone()[0], 2)
        self.store.close()
        self.store = Store(self.root / 'state')
        with patch.object(session_storage.time, 'time', return_value=second_retry + 1):
            await self.sweep()
        for path in (first, second, other):
            self.assertFalse(path.exists())

    async def test_unknown_interactive_memory_and_config_untouched(self):
        path = self.file()
        interactive = self.file(sid=OTHER)
        imported = self.file(sid=THIRD)
        for name in ('memory/notes.md', 'settings.json', 'auth.json'):
            saved = self.home / name
            saved.parent.mkdir(parents=True, exist_ok=True)
            saved.write_text('made-up fixture')
        worker = self.worker()
        self.remember(worker)
        self.remember(self.worker(sid=THIRD), sid=THIRD, created=False)
        self.close_round()
        await self.sweep()
        self.assertFalse(path.exists())
        for saved in (interactive, imported, self.home / 'memory/notes.md', self.home / 'settings.json', self.home / 'auth.json'):
            self.assertTrue(saved.exists())

    async def test_switch_off_has_no_deletion_or_backlog_scan(self):
        path = self.file()
        self.remember(self.worker())
        self.close_round()
        self.store.put('session_delete', False)
        await self.sweep()
        self.assertTrue(path.exists())
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM session_deletions').fetchone()[0], 0)
        self.assertIsNone(self.store.get('session_cleanup_cursor'))

    async def test_parent_current_and_previous_retained_after_confirmation(self):
        old, previous, current = (self.file(sid=sid) for sid in (SID, OTHER, THIRD))
        self.remember(parent='coordinator', sid=SID)
        self.remember(parent='coordinator', sid=OTHER)
        await self.sweep()
        self.assertTrue(old.exists())
        self.remember(parent='coordinator', sid=THIRD)
        self.store.put('coordinator_session', THIRD)
        await self.sweep()
        self.assertFalse(old.exists())
        self.assertTrue(previous.exists())
        self.assertTrue(current.exists())

    async def test_shared_active_session_is_retained(self):
        path = self.file()
        self.remember(self.worker())
        other_job = self.store.task_create('1:2', 'Other')
        self.worker(status='running', result=False, job=other_job)
        self.close_round()
        await self.sweep()
        self.assertTrue(path.exists())

    async def test_symlink_outside_root_and_parent_swap_are_refused(self):
        from coordinator import session_storage
        outside = self.root / 'outside'
        target = self.file(home=outside)
        self.home.mkdir()
        (self.home / 'projects').symlink_to(outside / 'projects', target_is_directory=True)
        self.remember(self.worker())
        self.close_round()
        await self.sweep()
        self.assertTrue(target.exists())
        with self.assertRaises((ValueError, OSError)):
            session_storage.remove_file(self.home / 'projects', {'path': '../outside', 'device': 0, 'inode': 0})

    async def test_crash_after_record_before_delete_resumes_and_does_not_delete_replacement(self):
        from coordinator import session_storage
        path = self.file()
        self.remember(self.worker())
        self.close_round()
        with patch.object(session_storage, 'remove_file', side_effect=RuntimeError('simulated crash')):
            await self.sweep()
        planned = self.store.db.execute('SELECT * FROM session_deletions').fetchone()
        self.assertIsNotNone(planned)
        self.assertTrue(path.exists())
        self.store.close()
        self.store = Store(self.root / 'state')
        self.store.db.execute('UPDATE session_deletions SET retry_at=0')
        await self.sweep()
        self.assertFalse(path.exists())
        path.write_text('new occupant')
        await self.sweep()
        self.assertEqual(path.read_text(), 'new occupant')

    async def test_replaced_file_kept_after_a_pending_plan(self):
        from coordinator import session_storage
        path = self.file()
        self.remember(self.worker())
        self.close_round()
        with patch.object(session_storage, 'remove_file', side_effect=RuntimeError('simulated crash')):
            await self.sweep()
        saved = path.with_suffix('.saved')
        path.rename(saved)
        path.write_text('replacement')
        self.store.db.execute('UPDATE session_deletions SET retry_at=0')
        await self.sweep()
        self.assertEqual(path.read_text(), 'replacement')
        self.assertTrue(saved.exists())

    async def test_directory_swap_after_plan_cannot_escape_the_session_root(self):
        from coordinator import session_storage
        path = self.file()
        self.remember(self.worker())
        self.close_round()
        with patch.object(session_storage, 'remove_file', side_effect=RuntimeError('simulated crash')):
            await self.sweep()
        outside = self.root / 'outside'
        outside.mkdir()
        target = outside / path.name
        target.write_text('unknown session')
        saved = path.parent.with_name('saved-project')
        path.parent.rename(saved)
        path.parent.symlink_to(outside, target_is_directory=True)
        self.store.db.execute('UPDATE session_deletions SET retry_at=0')
        await self.sweep()
        self.assertEqual(target.read_text(), 'unknown session')
        self.assertTrue((saved / path.name).exists())

    async def test_native_writer_lock_blocks_deletion_until_released(self):
        from coordinator import session_storage
        path = self.file()
        self.remember(self.worker())
        self.close_round()
        with session_storage.session_lease(self.store, {'provider': 'claude', 'session': SID}):
            await self.sweep()
        self.assertTrue(path.exists())
        self.store.db.execute('UPDATE session_deletions SET retry_at=0')
        await self.sweep()
        self.assertFalse(path.exists())

    async def test_open_code_uses_native_delete_with_isolated_data_home(self):
        from coordinator import session_storage
        home = self.store.directory / 'opencode/workers/1'
        data = home / 'data/opencode'
        data.mkdir(parents=True)
        (data / 'opencode.db').write_bytes(b'fake database')
        worker = self.worker('opencode', sid=OPENCODE)
        self.remember(worker, 'opencode', OPENCODE, home=home)
        self.close_round()
        delete = AsyncMock(return_value=None)
        with patch.object(session_storage, 'delete_opencode', delete):
            await self.sweep()
        delete.assert_awaited_once()
        plan = delete.call_args.args[1]
        self.assertEqual(plan['session'], OPENCODE)
        self.assertEqual(plan['home'], str(home))
        self.assertTrue((data / 'opencode.db').exists())

    async def test_backlog_uses_launch_evidence_and_is_bounded(self):
        from coordinator import session_storage
        path = self.file()
        worker = self.worker()
        directory = self.store.directory / 'hosts' / ('worker-' + str(worker))
        directory.mkdir(parents=True)
        (directory / 'spec.json').write_text(json.dumps({'fresh': True, 'provider': 'claude',
            'cwd': str(self.root), 'env': {'CLAUDE_CONFIG_DIR': str(self.home)}}))
        (directory / 'exit.json').write_text('{"returncode":0}')
        imported = self.file(sid=OTHER)
        self.worker(sid=OTHER)
        self.close_round()
        with patch.object(session_storage, 'BATCH', 1):
            await self.sweep()
            self.assertLessEqual(self.store.get('session_cleanup_cursor', 0), 1)
            await self.sweep()
        self.assertFalse(path.exists())
        self.assertTrue(imported.exists())

    async def test_backlog_recovers_closed_worker_after_host_log_removal(self):
        path = self.file()
        worker = self.worker()
        self.store.put('accounts', {'fixture': {'config_dir': str(self.home)}})
        self.store.db.execute("UPDATE workers SET account_alias='fixture' WHERE id=?", (worker,))
        self.close_round()
        await self.sweep()
        self.assertFalse(path.exists())

    async def test_account_reconfiguration_cannot_redirect_a_recorded_deletion(self):
        path = self.file()
        other_home = self.root / 'other-account'
        unrelated = self.file(home=other_home)
        worker = self.worker()
        self.remember(worker)
        self.store.put('accounts', {'fixture': {'config_dir': str(other_home)}})
        self.close_round()
        await self.sweep()
        self.assertFalse(path.exists())
        self.assertTrue(unrelated.exists())

    async def test_parent_account_switch_keeps_the_same_generation(self):
        original = self.file()
        self.remember(parent='coordinator')
        other_home = self.root / 'second-account'
        self.file(home=other_home)
        self.remember(parent='coordinator', home=other_home, created=False)
        self.remember(parent='coordinator', sid=OTHER)
        await self.sweep()
        self.assertTrue(original.exists())
        generations = [row[0] for row in self.store.db.execute('SELECT generation FROM native_sessions ORDER BY id')]
        self.assertEqual(generations, [1, 1, 2])

    async def test_running_host_keeps_final_worker_session(self):
        path = self.file()
        worker = self.worker()
        host = 'worker-' + str(worker)
        directory = self.store.directory / 'hosts' / host
        directory.mkdir(parents=True)
        (directory / 'spec.json').write_text('{}')
        (directory / 'pid').write_text(str(os.getpid()))
        self.remember(worker, host=host)
        self.close_round()
        await self.sweep()
        self.assertTrue(path.exists())

    async def test_shared_codex_session_root_is_pinned_and_other_session_kept(self):
        shared = self.root / 'shared'
        path = self.file('codex', home=shared)
        interactive = self.file('codex', sid=OTHER, home=shared)
        self.home.mkdir()
        (self.home / 'sessions').symlink_to(shared / 'sessions', target_is_directory=True)
        self.remember(self.worker('codex'), 'codex')
        self.close_round()
        await self.sweep()
        self.assertFalse(path.exists())
        self.assertTrue(interactive.exists())

    async def test_coordinator_launch_is_not_confirmed_until_native_acknowledgment(self):
        from coordinator import session_storage
        session = SimpleNamespace(store=self.store, key='coordinator', provider='claude',
                                  host_id='coordinator-fixture', session_id=SID)
        session_storage.launch_parent(session, {'provider': 'claude', 'env': {'CLAUDE_CONFIG_DIR': str(self.home)},
                                               'cwd': str(self.root), 'fresh': True})
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM native_sessions').fetchone()[0], 0)
        session_storage.confirm_parent(session)
        session_storage.confirm_parent(session)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM native_sessions').fetchone()[0], 1)

    async def test_deletion_plan_cannot_resume_in_a_reopened_round(self):
        from coordinator.workers import can_resume_finished
        worker = self.worker()
        self.store.db.execute('UPDATE workers SET fresh=0 WHERE id=?', (worker,))
        path = self.file()
        self.remember(worker)
        self.close_round()
        await self.sweep()
        self.assertFalse(path.exists())
        message = self.store.message_save('1:2', 'owner', 'Reopen')
        self.store.wait_open_round(self.job['id'], message['id'], opened_by='owner')
        row = dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (worker,)).fetchone())
        self.assertFalse(can_resume_finished(self.store, row))

    async def test_native_opencode_command_deletes_only_requested_session_and_verifies_it(self):
        from coordinator import session_storage
        home = self.store.directory / 'opencode/workers/1'
        data = home / 'data/opencode'
        data.mkdir(parents=True)
        with sqlite3.connect(str(data / 'opencode.db')) as db:
            db.execute('CREATE TABLE session(id TEXT PRIMARY KEY,parent_id TEXT)')
            db.executemany('INSERT INTO session VALUES (?,NULL)', ((OPENCODE,), ('ses_' + 'b' * 26,)))
        binary = self.root / 'fake-opencode-delete'
        binary.write_text('#!' + sys.executable + '\n' + '''import json, os, sqlite3, sys
from pathlib import Path
assert sys.argv[1:3] == ['session', 'delete']
assert os.environ['OPENCODE_AUTH_CONTENT'] == '{}'
database = Path(os.environ['XDG_DATA_HOME']) / 'opencode/opencode.db'
with sqlite3.connect(str(database)) as db:
    db.execute('DELETE FROM session WHERE id=?', (sys.argv[3],))
''')
        binary.chmod(0o700)
        self.store.put('opencode_binary', str(binary))
        worker = self.worker('opencode', sid=OPENCODE)
        self.remember(worker, 'opencode', OPENCODE, home=home)
        self.close_round()
        await self.sweep()
        with sqlite3.connect(str(data / 'opencode.db')) as db:
            self.assertEqual(db.execute('SELECT id FROM session').fetchall(), [('ses_' + 'b' * 26,)])
        row = self.store.db.execute('SELECT state FROM session_deletions').fetchone()
        self.assertEqual(row['state'], 'deleted')
        self.assertEqual(list((self.store.directory / 'session-cleanup').iterdir()), [])

    async def test_opencode_parent_database_waits_for_current_and_unconfirmed_hosts(self):
        from coordinator import session_storage
        home = self.root / 'parent-profile'
        for index, sid in enumerate((OPENCODE, 'ses_' + 'b' * 26, 'ses_' + 'c' * 26)):
            self.remember(provider='opencode', sid=sid, home=home, parent='coordinator', host='parent-' + str(index))
        pending = SimpleNamespace(store=self.store, key='other', host_id='unconfirmed', provider='opencode')
        session_storage.launch_parent(pending, {'provider': 'opencode', 'env': {'XDG_DATA_HOME': str(home / 'data')},
                                               'cwd': str(self.root), 'fresh': True})
        running = {'parent-2', 'unconfirmed'}
        delete = AsyncMock(return_value=None)
        with patch('coordinator.worker_storage.processes_gone', side_effect=lambda path: path.name not in running):
            with patch.object(session_storage, 'delete_opencode', delete):
                await self.sweep()
                delete.assert_not_awaited()
                running.remove('parent-2')
                self.store.db.execute('UPDATE native_sessions SET retry_at=0')
                await self.sweep()
                delete.assert_not_awaited()
                running.clear()
                self.store.put('other_host', {'id': 'unconfirmed', 'provider': 'opencode'})
                self.store.db.execute('UPDATE native_sessions SET retry_at=0')
                await self.sweep()
                delete.assert_not_awaited()
                self.store.put('other_host', None)
                self.store.db.execute('UPDATE native_sessions SET retry_at=0')
                self.store.db.execute('UPDATE session_deletions SET retry_at=0')
                await self.sweep()
        delete.assert_awaited_once()
        self.assertEqual(delete.call_args.args[1]['session'], OPENCODE)

    async def test_parent_home_is_recorded_before_the_host_can_start(self):
        from coordinator import parent_state
        home = self.root / 'parent-profile'
        session = SimpleNamespace(store=self.store, key='coordinator', account_alias='fixture', model='fixture',
                                  runner=SimpleNamespace(state_dir=self.store.directory), _event_consumed=None)

        async def start_host(*args, **kwargs):
            saved = self.store.get('coordinator_native_launch')
            self.assertEqual(saved['home'], str(home))
            self.assertEqual(saved['host'], session.host_id)
            return SimpleNamespace()

        spec = {'provider': 'opencode', 'env': {'XDG_DATA_HOME': str(home / 'data')},
                'cwd': str(self.root), 'fresh': True}
        with patch.object(parent_state.HostClient, 'launch', side_effect=start_host):
            self.assertTrue(await parent_state.launch(session, spec, 'fixture'))

    async def test_failed_opencode_delete_removes_isolated_scratch_and_retries(self):
        home = self.root / 'opencode-profile'
        data = home / 'data/opencode'
        data.mkdir(parents=True)
        with sqlite3.connect(str(data / 'opencode.db')) as db:
            db.execute('CREATE TABLE session(id TEXT PRIMARY KEY,parent_id TEXT)')
            db.execute('INSERT INTO session VALUES (?,NULL)', (OPENCODE,))
        binary = self.root / 'fake-opencode-delete'
        binary.write_text('#!' + sys.executable + '\nraise SystemExit(1)\n')
        binary.chmod(0o700)
        self.store.put('opencode_binary', str(binary))
        self.remember(self.worker('opencode', sid=OPENCODE), 'opencode', OPENCODE, home=home)
        self.close_round()
        await self.sweep()
        self.assertEqual(list((self.store.directory / 'session-cleanup').iterdir()), [])
        plan = self.store.db.execute('SELECT state,attempts,retry_at FROM session_deletions').fetchone()
        self.assertEqual((plan['state'], plan['attempts']), ('pending', 1))
        self.assertGreater(plan['retry_at'], time.time())
        with sqlite3.connect(str(data / 'opencode.db')) as db:
            self.assertEqual(db.execute('SELECT id FROM session').fetchall(), [(OPENCODE,)])

    async def test_opencode_already_deleted_session_cleans_crash_leftover_scratch(self):
        home = self.root / 'opencode-profile'
        native = self.remember(self.worker('opencode', sid=OPENCODE), 'opencode', OPENCODE, home=home)
        self.store.db.execute('''INSERT INTO session_deletions(native,provider,session,home,root,path)
            SELECT id,provider,session,home,root,session FROM native_sessions WHERE id=?''', (native,))
        plan = self.store.db.execute('SELECT id FROM session_deletions').fetchone()[0]
        scratch = self.store.directory / 'session-cleanup' / str(plan)
        scratch.mkdir(parents=True)
        (scratch / 'leftover').write_text('fixture')
        self.close_round()
        await self.sweep()
        self.assertFalse(scratch.exists())
        self.assertEqual(self.store.db.execute('SELECT state FROM session_deletions').fetchone()[0], 'deleted')

    async def test_opencode_unknown_child_prevents_cascading_delete(self):
        from coordinator import session_storage
        home = self.store.directory / 'opencode/workers/1'
        data = home / 'data/opencode'
        data.mkdir(parents=True)
        child = 'ses_' + 'b' * 26
        with sqlite3.connect(str(data / 'opencode.db')) as db:
            db.execute('CREATE TABLE session(id TEXT PRIMARY KEY,parent_id TEXT)')
            db.executemany('INSERT INTO session VALUES (?,?)', ((OPENCODE, None), (child, OPENCODE)))
        self.store.put('opencode_binary', str(self.root / 'must-not-run'))
        self.remember(self.worker('opencode', sid=OPENCODE), 'opencode', OPENCODE, home=home)
        self.close_round()
        with patch.object(asyncio, 'create_subprocess_exec', new=AsyncMock()) as process:
            await self.sweep()
        process.assert_not_awaited()
        with sqlite3.connect(str(data / 'opencode.db')) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM session').fetchone()[0], 2)


class SessionLaunchTests(StorageSupport, unittest.IsolatedAsyncioTestCase):
    async def test_optional_recording_failure_cannot_stop_a_live_worker(self):
        for failure in ('database', 'commit', 'home'):
            with self.subTest(failure=failure):
                worker, _ = self.spawn()
                if failure == 'database':
                    self.store.db.execute('''CREATE TRIGGER reject_host BEFORE INSERT ON native_session_hosts
                        BEGIN SELECT RAISE(ABORT, 'fixture database error'); END''')
                    context = nullcontext()
                elif failure == 'commit':
                    self.store.db.execute('DROP TRIGGER reject_host')
                    self.store.db.execute('''CREATE TABLE deferred_guard(worker INTEGER
                        REFERENCES workers(id) DEFERRABLE INITIALLY DEFERRED)''')
                    self.store.db.execute('''CREATE TRIGGER reject_commit AFTER INSERT ON native_session_hosts
                        BEGIN INSERT INTO deferred_guard VALUES (999); END''')
                    self.store.db.commit()
                    context = nullcontext()
                else:
                    self.store.db.execute('DROP TRIGGER reject_commit')
                    context = patch('coordinator.session_storage.home_for', return_value='relative-home')
                with context:
                    done = await self.pool().run(worker, self.store.topic('1:2'))
                self.assertEqual(done['status'], 'done')
                self.assertTrue(json.loads(done['result'])['success'])
                self.assertIsNotNone(done['session'])
                self.assertEqual(self.store.db.execute('SELECT count(*) FROM native_sessions WHERE worker=?',
                                                       (worker['id'],)).fetchone()[0], 0)
                self.assertIsNotNone(self.store.db.execute("SELECT 1 FROM problems WHERE code='session-record-failed'").fetchone())

    def test_missing_worker_recording_is_ignored(self):
        from coordinator.session_storage import remember
        self.assertIsNone(remember(self.store, 'claude', SID, str(self.root / 'profile'), str(self.root),
                                   worker=999, created=True))
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM native_sessions').fetchone()[0], 0)

    async def test_worker_observation_records_the_actual_launch_home(self):
        worker, _ = self.spawn()
        pool = self.pool()
        done = await pool.run(worker, self.store.topic('1:2'))
        self.assertEqual(done['status'], 'done')
        row = self.store.db.execute('SELECT * FROM native_sessions WHERE worker=?', (worker['id'],)).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row['home'], str(self.root / 'profile'))
        self.assertEqual(row['session'], done['session'])
