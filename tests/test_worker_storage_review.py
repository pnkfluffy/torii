"""Worker temp cleanup and native relaunch regression coverage."""

import asyncio
import json
import os
from pathlib import Path
import shutil
import threading
import sqlite3
import time
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import control_api, worker_storage
from coordinator.providers import ProviderRunner, RunResult
from coordinator.native_protocol import RunControl
from coordinator.service import Service
from tests.test_worker_storage import SID, TOPIC, StorageSupport
from tests.test_codex_secrets_goals import SERVER


GO_MODULE_CACHE = '''
scratch = Path(os.environ['TMPDIR'])
module = scratch / 'home' / 'go' / 'pkg' / 'mod' / 'example.com' / 'm@v1.0.0'
(module / 'internal' / 'pkg').mkdir(parents=True)
(module / 'go.mod').write_text('module example.com/m')
(module / 'internal' / 'pkg' / 'pkg.go').write_text('package pkg')
for path in (module / 'go.mod', module / 'internal' / 'pkg' / 'pkg.go'):
    path.chmod(0o444)
for path in (module / 'internal' / 'pkg', module / 'internal', module):
    path.chmod(0o555)
'''

SMALL = '''
scratch = Path(os.environ['TMPDIR'])
(scratch / 'build.log').write_text('scratch')
'''


def writable_tree(base):
    if not os.path.isdir(base):
        return
    os.chmod(base, 0o700)
    for root, dirs, _ in os.walk(base):
        for name in dirs:
            path = os.path.join(root, name)
            if not os.path.islink(path):
                os.chmod(path, 0o700)


class ReviewSupport(StorageSupport):
    def setUp(self):
        super().setUp()
        self.addCleanup(writable_tree, str(self.base))

    def fake_claude(self, pre):
        binary = self.root / 'fake-review-claude'
        script = '''#!/usr/bin/env python3
import json, os, sys, time
from pathlib import Path
sid = sys.argv[sys.argv.index('--session-id') + 1] if '--session-id' in sys.argv else Path(sys.argv[sys.argv.index('--resume') + 1]).stem
transcript = Path(os.environ['CLAUDE_CONFIG_DIR']) / 'projects' / 'test' / (sid + '.jsonl')
transcript.parent.mkdir(parents=True, exist_ok=True)
transcript.write_text('{}\\n')
PRE
print(json.dumps({'type': 'system', 'subtype': 'init', 'session_id': sid}), flush=True)
for line in sys.stdin:
    event = json.loads(line)
    print(json.dumps({**event, 'isReplay': True}), flush=True)
    print(json.dumps({'type': 'result', 'subtype': 'success', 'session_id': sid, 'result': 'done'}), flush=True)
'''.replace('SID', repr(SID)).replace('PRE', pre)
        binary.write_text(script)
        binary.chmod(0o700)
        return ProviderRunner(self.store.directory, {'claude': str(binary)})

    async def drain(self, service):
        storage = getattr(service.workers, 'storage', None)
        if storage is not None:
            while storage.tasks:
                await asyncio.gather(*list(storage.tasks.values()))
                await asyncio.sleep(0)

    def failures(self):
        return self.store.db.execute(
            "SELECT COUNT(*) FROM problems WHERE code='temp-cleanup-failed'").fetchone()[0]


class CleanupLimitsTests(ReviewSupport, unittest.IsolatedAsyncioTestCase):
    async def test_normal_cleanup_waits_do_not_exhaust_the_error_budget(self):
        for waiting in ('proof', 'children'):
            with self.subTest(waiting=waiting):
                worker, _ = self.spawn()
                self.scratch(worker).mkdir()
                self.saved_host(worker)
                self.store.worker_complete(worker['id'], {'success': True}, 'done')
                service = Service(self.store, None, self.fake_claude(SMALL), self.root)
                storage = service.workers.storage
                with self.temp_scope(), patch('time.time', return_value=1800000000.0) as clock, \
                        patch('coordinator.close_saves.preserve_worker_proofs',
                              AsyncMock(return_value=waiting != 'proof')) as proofs, \
                        patch('coordinator.worker_storage._processes_gone', return_value=waiting != 'children') as gone:
                    for attempt in range(25):
                        storage.schedule(worker['id'])
                        await self.drain(service)
                        self.assertEqual(storage.state(worker['id'])['retry_at'] - clock.return_value,
                                         min(300, 2 ** (attempt + 1)))
                        clock.return_value += 3600
                    state = storage.state(worker['id'])
                    self.assertEqual((state['pending'], state['attempts']), (1, 0))
                    self.assertEqual(self.failures(), 0)
                    self.assertTrue(self.scratch(worker).is_dir())
                    proofs.return_value = gone.return_value = True
                    storage.schedule(worker['id'])
                    await self.drain(service)
                self.assertEqual(storage.state(worker['id'])['pending'], 0)
                self.assertFalse(self.scratch(worker).exists())

    async def test_transient_cleanup_errors_are_recorded_hourly_per_worker(self):
        workers = []
        storage = worker_storage.WorkerStorage(self.store)
        for _ in range(2):
            worker, _ = self.spawn()
            self.store.worker_complete(worker['id'], {'success': True}, 'done')
            workers.append(worker)
        with patch('time.time', return_value=1800000000.0) as clock:
            for _ in range(2):
                for worker in workers:
                    storage.retry(worker['id'], OSError('temporary refusal'))
            self.assertEqual(self.failures(), 2)
            clock.return_value += 3600
            for worker in workers:
                storage.retry(worker['id'], OSError('temporary refusal'))
        rows = self.store.db.execute(
            "SELECT worker,COUNT(*) FROM problems WHERE code='temp-cleanup-failed' GROUP BY worker ORDER BY worker").fetchall()
        self.assertEqual([tuple(row) for row in rows], [(worker['id'], 2) for worker in workers])

    async def test_cleanup_error_at_cap_handles_a_missing_worker_row(self):
        self.store.db.commit()
        self.store.db.execute('PRAGMA foreign_keys=OFF')
        with self.store.db:
            self.store.db.execute('INSERT INTO worker_temp_state(worker,pending,attempts) VALUES (9999,2,19)')
        self.store.db.execute('PRAGMA foreign_keys=ON')
        storage = worker_storage.WorkerStorage(self.store)
        storage.retry(9999, OSError('missing worker'))
        state = storage.state(9999)
        self.assertEqual((state['pending'], state['attempts']), (0, 20))
        row = self.store.db.execute(
            "SELECT worker,topic,task,detail FROM problems WHERE code='temp-cleanup-failed'").fetchone()
        self.assertEqual(tuple(row), (9999, None, None, 'missing worker'))

    async def test_relaunch_uses_next_host_version_and_preserves_native_session(self):
        for suffix in ('', '-resume-17'):
            with self.subTest(suffix=suffix):
                worker, _ = self.spawn()
                base = 'worker-' + str(worker['id']) + suffix
                if suffix:
                    self.store.db.execute('UPDATE workers SET host=? WHERE id=?', (base, worker['id']))
                service = Service(self.store, None, self.fake_claude(SMALL), self.root)
                hosts, sessions = [], []
                with self.temp_scope():
                    for _ in range(3):
                        if hosts:
                            self.store.db.execute("UPDATE workers SET status='queued',fresh=0 WHERE id=?", (worker['id'],))
                            worker = service.workers.get(worker['id'])
                        done = await service.workers.run(worker, self.store.topic(TOPIC))
                        self.assertTrue(json.loads(done['result'])['success'], done)
                        await self.drain(service)
                        hosts.append(done['host'])
                        sessions.append(done['session'])
                self.assertEqual(hosts, [base, base + '.2', base + '.3'])
                self.assertTrue(sessions[0])
                self.assertEqual(sessions, [sessions[0]] * 3)

    async def test_temp_cleanup_stops_after_twenty_failures_and_reconciliation_keeps_it_stopped(self):
        worker, _ = self.spawn()
        self.scratch(worker).mkdir()
        self.saved_host(worker)
        self.store.worker_complete(worker['id'], {'success': True}, 'done')
        service = Service(self.store, None, self.fake_claude(SMALL), self.root)
        storage = service.workers.storage
        with self.temp_scope(), self.assertLogs('coordinator.problems', level='ERROR') as terminal, \
                patch('time.time', return_value=1800000000.0) as clock, \
                patch('coordinator.worker_storage._scratch_paths', side_effect=OSError('permanent refusal')) as failed:
            for _ in range(25):
                storage.schedule()
                await self.drain(service)
                await storage.reconcile()
                clock.return_value += 301
            self.assertEqual(failed.call_count, 20)
        state = storage.state(worker['id'])
        self.assertEqual((state['pending'], state['attempts']), (0, 20))
        self.assertEqual(self.failures(), 3)
        self.assertEqual(len(terminal.output), 1)
        self.assertTrue(self.scratch(worker).is_dir())

    async def test_null_spec_environment_requeues_cleanup_without_restart(self):
        worker, _ = self.spawn()
        self.scratch(worker).mkdir()
        host = self.saved_host(worker)
        (host / 'spec.json').write_text('{"worker_temp":true,"env":null}')
        self.store.worker_complete(worker['id'], {'success': True}, 'done')
        storage = worker_storage.WorkerStorage(self.store)
        self.addCleanup(storage.close)
        with self.temp_scope():
            storage.schedule()
            await asyncio.gather(*list(storage.tasks.values()), return_exceptions=True)
            await asyncio.sleep(0)
            state = storage.state(worker['id'])
            self.assertEqual((state['pending'], state['attempts']), (1, 1))
            (host / 'spec.json').write_text(json.dumps({'worker_temp': True, 'env': {'TMPDIR': str(self.scratch(worker))}}))
            with patch('time.time', return_value=state['retry_at']):
                storage.schedule()
                await asyncio.gather(*list(storage.tasks.values()))
                await asyncio.sleep(0)
        self.assertEqual(storage.state(worker['id'])['pending'], 0)
        self.assertFalse(self.scratch(worker).exists())


class StorageTransactionTests(StorageSupport, unittest.IsolatedAsyncioTestCase):
    async def test_storage_updates_preserve_caller_rollback(self):
        for operation in ('remember', 'retry', 'reconcile', 'prepare', 'cleanup', 'schedule'):
            with self.subTest(operation=operation), self.temp_scope():
                worker, _ = self.spawn()
                worker_id = worker['id']
                self.store.worker_complete(worker_id, {'success': True})
                storage = worker_storage.WorkerStorage(self.store)
                scratch = self.scratch(worker)
                scratch.mkdir()
                (scratch / 'build.log').write_text('scratch')
                self.saved_host(worker)
                with self.store.transaction():
                    self.store.db.execute('UPDATE worker_temp_state SET path=?,pending=? WHERE worker=?',
                                          (str(scratch), int(operation in ('cleanup', 'schedule')), worker_id))
                before = storage.state(worker_id)
                detached = self.base / ('detached-' + str(worker_id))
                arguments, expected = {
                    'remember': ((worker_id, [detached]), {'pending': 1, 'staged': json.dumps([str(detached)])}),
                    'retry': ((worker_id,), {'pending': 1, 'attempts': 0}),
                    'reconcile': ((), {'pending': 1}),
                    'prepare': ((worker_id,), {'finished': 0, 'epoch': before['epoch'] + 1}),
                    'cleanup': ((worker_id,), {'pending': 0, 'staged': '[]'}),
                    'schedule': ((worker_id,), {'pending': 2})
                }[operation]
                try:
                    with self.assertRaisesRegex(RuntimeError, 'rollback storage'):
                        with self.store.transaction():
                            self.store.put('storage_transaction_probe', operation)
                            result = getattr(storage, operation)(*arguments)
                            if asyncio.iscoroutine(result):
                                await result
                            for key, value in expected.items():
                                self.assertEqual(storage.state(worker_id)[key], value)
                            raise RuntimeError('rollback storage')
                    self.assertIsNone(self.store.get('storage_transaction_probe'))
                    self.assertEqual(storage.state(worker_id), before)
                finally:
                    storage.close()
                    await asyncio.gather(*list(storage.tasks.values()), return_exceptions=True)


class ReadOnlyTreeRepro(ReviewSupport, unittest.IsolatedAsyncioTestCase):
    async def test_go_module_cache_in_worker_temp_is_deleted(self):
        worker, _ = self.spawn()
        service = Service(self.store, None, self.fake_claude(GO_MODULE_CACHE), self.root)
        with self.temp_scope():
            done = await service.workers.run(worker, self.store.topic(TOPIC))
            self.assertTrue(json.loads(done['result'])['success'], done)
            await self.drain(service)
            after_finish = self.failures()
            for _ in range(5):
                await service.workers_once()
                await self.drain(service)
            after_five_loops = self.failures()
            looping = asyncio.create_task(service.workers_loop())
            await asyncio.sleep(2)
            looping.cancel()
            await asyncio.gather(looping, return_exceptions=True)
            after_loop = self.failures()
            left = sorted(path.name[:40] for path in self.base.iterdir())
        detail = self.store.db.execute(
            "SELECT detail FROM problems WHERE code='temp-cleanup-failed' ORDER BY id DESC LIMIT 1").fetchone()
        print('\nREPRO-A left=%s failures_after_finish=%d failures_after_5_workers_once=%d '
              'failures_after_2s_workers_loop=%d last_detail=%s' % (
                  left, after_finish, after_five_loops, after_loop, detail[0] if detail else None))
        self.assertEqual(left, [])
        self.assertEqual(after_five_loops, 0)


class SteerDuringDeleteRepro(ReviewSupport, unittest.IsolatedAsyncioTestCase):
    def request(self, request_id):
        row = self.store.db.execute('SELECT state,result FROM service_requests WHERE id=?',
                                    (request_id,)).fetchone()
        return row['state'], json.loads(row['result'] or 'null')

    async def steer(self, service, worker_id):
        steered = control_api.call(self.store, 'workers.steer', {'worker': worker_id, 'prompt': 'One more fix.'},
                                   source='mcp', topic=TOPIC)
        self.assertTrue(steered.ok, steered)
        await service.controls_once()
        state = self.request(steered.data['request'])
        callback = self.store.db.execute(
            "SELECT text FROM messages WHERE kind='callback' ORDER BY id DESC LIMIT 1").fetchone()
        return state, callback[0] if callback else None

    async def test_switch_on_steer_while_temp_is_deleting_resumes(self):
        worker, _ = self.spawn()
        pre = "scratch = Path(os.environ['TMPDIR'])\nwith Path(%r).open('a') as log:\n    log.write(json.dumps(sorted(p.name for p in scratch.iterdir())) + '\\n')\n" % str(self.root / 'seen') + SMALL
        service = Service(self.store, None, self.fake_claude(pre), self.root)
        entered = threading.Event()
        release = threading.Event()
        actual = shutil.rmtree

        def slow(path, *args, **kwargs):
            if '.deleting-' in str(path):
                entered.set()
                release.wait(30)
            return actual(path, *args, **kwargs)

        slow.avoids_symlink_attacks = actual.avoids_symlink_attacks
        with self.temp_scope(), patch('shutil.rmtree', slow):
            try:
                await service.workers_once()
                self.assertTrue(await asyncio.wait_for(asyncio.to_thread(entered.wait, 30), 35))
                status = service.workers.get(worker['id'])['status']
                pending = not service.worker_tasks[worker['id']].done()
                state, callback = await self.steer(service, worker['id'])
                self.assertEqual(state, ('done', {'receipt': 'resumed'}))
                self.assertFalse(pending)
                await service.workers_once()
                await asyncio.wait_for(asyncio.gather(*service.worker_tasks.values()), 10)
                seen = [json.loads(line) for line in (self.root / 'seen').read_text().splitlines()]
                self.assertEqual(seen, [[], []])
            finally:
                release.set()
                await asyncio.gather(*service.worker_tasks.values(), return_exceptions=True)
                await self.drain(service)
                await service.workers.cleanup_finished()
                await self.drain(service)
        row = service.workers.get(worker['id'])
        print('\nREPRO-B-on status_at_steer=%s run_task_pending=%s request=%s status_after=%s callback=%s' % (
            status, pending, state, row['status'], callback))
        self.assertEqual(state, ('done', {'receipt': 'resumed'}))


class FailedLaunchRepro(ReviewSupport, unittest.IsolatedAsyncioTestCase):
    async def test_resume_after_a_failed_host_launch_starts(self):
        worker, _ = self.spawn('codex')
        service = Service(self.store, None, self.runner('codex'), self.root)
        with self.temp_scope(), patch.object(service.workers.runner, '_external_writer', AsyncMock(return_value=False)):
            failed = self.store.directory / 'hosts' / ('worker-%d' % worker['id'])
            failed.mkdir(parents=True)
            (failed / 'spec.json').write_text(json.dumps({'env': {'TMPDIR': str(self.scratch(worker))},
                                                          'worker_temp': True}))
            self.scratch(worker).mkdir()
            with self.store.db:
                self.store.db.execute("UPDATE workers SET session=?,fresh=0,host=? WHERE id=?",
                                      (SID, failed.name, worker['id']))
            self.store.worker_complete(worker['id'], {'success': False, 'session_id': SID,
                                                      'failure_code': 'start_failed', 'error': 'host timeout'}, 'done')
            steered = control_api.call(self.store, 'workers.steer', {'worker': worker['id'], 'prompt': 'Retry.'},
                                       source='mcp', topic=TOPIC)
            self.assertTrue(steered.ok, steered)
            await service.controls_once()
            await service.workers_once()
            await asyncio.gather(*service.worker_tasks.values(), return_exceptions=True)
        row = service.workers.get(worker['id'])
        result = json.loads(row['result'])
        print('\nREPRO-D status=%s success=%s error=%s' % (row['status'], result.get('success'), result.get('error')))
        self.assertTrue(result.get('success'), result)


class CleanupRecoveryTests(ReviewSupport, unittest.IsolatedAsyncioTestCase):
    async def test_mode_zero_and_readonly_trees_preserve_symlink_targets(self):
        outside = self.root / 'outside'
        outside.mkdir()
        (outside / 'kept').write_text('proof')
        outside.chmod(0o555)
        self.addCleanup(outside.chmod, 0o700)
        pre = GO_MODULE_CACHE + '''
zero = scratch / 'zero'
zero.mkdir()
(zero / 'file').write_text('scratch')
zero.chmod(0)
readonly = scratch / 'readonly-file'
readonly.mkdir()
(readonly / 'file').write_text('scratch')
readonly.chmod(0o555)
(scratch / 'link').symlink_to(OUTSIDE, target_is_directory=True)
'''.replace('OUTSIDE', repr(str(outside)))
        worker, _ = self.spawn()
        service = Service(self.store, None, self.fake_claude(pre), self.root)
        with self.temp_scope():
            done = await service.workers.run(worker, self.store.topic(TOPIC))
            await self.drain(service)
        self.assertTrue(json.loads(done['result'])['success'], done)
        self.assertEqual(list(self.base.iterdir()), [])
        self.assertEqual((outside / 'kept').read_text(), 'proof')
        self.assertEqual(outside.stat().st_mode & 0o777, 0o555)
        self.assertEqual(self.failures(), 0)

    async def test_delete_failure_is_throttled_and_retried_with_backoff(self):
        worker, _ = self.spawn()
        service = Service(self.store, None, self.fake_claude(SMALL), self.root)
        actual = shutil.rmtree
        attempts = []

        def failing(path, *args, **kwargs):
            if '.deleting-' in str(path):
                attempts.append(str(path))
                if len(attempts) < 3:
                    raise PermissionError('fixture read-only volume')
            return actual(path, *args, **kwargs)

        failing.avoids_symlink_attacks = actual.avoids_symlink_attacks
        now = time.time()
        with self.temp_scope(), patch('shutil.rmtree', failing), patch('time.time', return_value=now):
            await service.workers.run(worker, self.store.topic(TOPIC))
            await self.drain(service)
            for _ in range(5):
                await service.workers_once()
                await self.drain(service)
            self.assertEqual(len(attempts), 1)
            self.assertEqual(self.failures(), 1)
            with patch('time.time', return_value=now + 3):
                await service.workers_once()
                await self.drain(service)
            self.assertEqual(len(attempts), 2)
            self.assertEqual(self.failures(), 1)
            with patch('time.time', return_value=now + 8):
                await service.workers_once()
                await self.drain(service)
        self.assertEqual(len(attempts), 3)
        self.assertEqual(list(self.base.iterdir()), [])

    async def test_corrupt_spec_is_throttled_and_retried_with_backoff(self):
        worker, _ = self.spawn()
        self.scratch(worker).mkdir()
        host = self.saved_host(worker)
        (host / 'spec.json').write_text('{broken')
        self.store.worker_complete(worker['id'], {'success': True}, 'done')
        service = Service(self.store, None, self.fake_claude(SMALL), self.root)
        now = time.time()
        with self.temp_scope(), patch('time.time', return_value=now):
            for _ in range(6):
                await service.workers_once()
                await self.drain(service)
            self.assertEqual(self.failures(), 1)
            self.assertTrue(self.scratch(worker).exists())
            with patch('time.time', return_value=now + 3):
                await service.workers_once()
                await self.drain(service)
            self.assertEqual(self.failures(), 1)
            (host / 'spec.json').write_text(json.dumps({'worker_temp': True, 'env': {'TMPDIR': str(self.scratch(worker))}}))
            with patch('time.time', return_value=now + 8):
                await service.workers_once()
                await self.drain(service)
        self.assertFalse(self.scratch(worker).exists())

    async def test_dead_host_without_exit_keeps_temp_until_both_pids_are_dead(self):
        worker, _ = self.spawn()
        self.scratch(worker).mkdir()
        directory = self.store.directory / 'hosts' / ('worker-' + str(worker['id']))
        directory.mkdir(parents=True)
        (directory / 'spec.json').write_text('{}')
        import subprocess
        import sys
        children = [subprocess.Popen([sys.executable, '-c', 'import sys; sys.stdin.read()'], stdin=subprocess.PIPE)
                    for _ in range(2)]
        for child in children:
            self.addCleanup(lambda process=child: process.poll() is None and process.kill())
        (directory / 'pid').write_text(str(children[0].pid))
        (directory / 'child.pid').write_text(str(children[1].pid))
        self.store.worker_complete(worker['id'], {'success': True}, 'done')
        service = Service(self.store, None, self.fake_claude(SMALL), self.root)
        now = time.time()
        with self.temp_scope(), patch('time.time', return_value=now):
            await service.workers_once()
            await self.drain(service)
            self.assertTrue(self.scratch(worker).exists())
            children[1].communicate(timeout=10)
            with patch('time.time', return_value=now + 3):
                await service.workers_once()
                await self.drain(service)
            self.assertTrue(self.scratch(worker).exists())
            children[0].communicate(timeout=10)
            with patch('time.time', return_value=now + 8):
                await service.workers_once()
                await self.drain(service)
        self.assertFalse(self.scratch(worker).exists())

    async def test_startup_dispatch_and_shutdown_do_not_wait_for_delete(self):
        worker, _ = self.spawn()
        self.scratch(worker).mkdir()
        self.saved_host(worker)
        self.store.worker_complete(worker['id'], {'success': True}, 'done')
        service = Service(self.store, None, self.fake_claude(SMALL), self.root, pair_only=True)
        entered, release = threading.Event(), threading.Event()
        actual = shutil.rmtree

        def slow(path, *args, **kwargs):
            if '.deleting-' in str(path):
                entered.set()
                release.wait(30)
            return actual(path, *args, **kwargs)

        slow.avoids_symlink_attacks = actual.avoids_symlink_attacks
        with self.temp_scope(), patch('shutil.rmtree', slow), patch.object(service.extension, 'start', AsyncMock()), \
                patch('coordinator.reactions.check_administrators', AsyncMock()), \
                patch.object(service, 'announce_start', AsyncMock()), patch.object(service, 'supervised', return_value=[]):
            startup = asyncio.create_task(service.run())
            try:
                self.assertTrue(await asyncio.wait_for(asyncio.to_thread(entered.wait, 10), 12))
                self.assertTrue(startup.done())
                await asyncio.wait_for(service.workers_once(), 1)
                tasks = list(service.workers.storage.tasks.values())
                for task in tasks:
                    task.cancel()
                await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 1)
                self.assertFalse(release.is_set())
            finally:
                release.set()
                await asyncio.gather(startup, return_exceptions=True)
                await self.drain(service)


class CleanupCostTests(ReviewSupport, unittest.IsolatedAsyncioTestCase):
    async def test_startup_and_later_passes_read_bounded_rows_with_5000_final_workers(self):
        now = time.time()
        with self.store.db:
            self.store.db.executemany('''INSERT INTO workers(task,topic,provider,status,prompt,cwd,created,updated)
                VALUES (?,?,?,?,?,?,?,?)''', [(self.task['id'], TOPIC, 'claude', 'done', 'p', str(self.root), now, now)] * 5000)
        hosts = self.store.directory / 'hosts'
        hosts.mkdir()
        service = Service(self.store, None, self.fake_claude(SMALL), self.root)
        rows, listings = [], []
        factory = self.store.db.row_factory
        actual = os.scandir

        def counted(cursor, row):
            rows.append(tuple(row))
            return sqlite3.Row(cursor, row)

        def listed(path):
            if Path(path) == hosts:
                listings.append(str(path))
            return actual(path)

        self.store.db.row_factory = counted
        try:
            with self.temp_scope(), patch('os.scandir', listed):
                await service.workers.cleanup_finished()
                self.assertLessEqual(len(rows), 40)
                await self.drain(service)
                rows.clear()
                for _ in range(3):
                    await service.workers.cleanup_finished()
                    self.assertLessEqual(len(rows), 40)
                    await self.drain(service)
                    rows.clear()
                self.assertLessEqual(len(listings), 1)
        finally:
            self.store.db.row_factory = factory

    async def test_worker_with_no_folder_leaves_the_cleanup_queue(self):
        worker, _ = self.spawn()
        self.scratch(worker).mkdir()
        self.store.worker_complete(worker['id'], {'success': True}, 'done')
        service = Service(self.store, None, self.fake_claude(SMALL), self.root)
        with self.temp_scope():
            await service.workers.cleanup_finished()
            await self.drain(service)
            self.assertFalse(self.scratch(worker).exists())
            self.assertIsNone(self.store.get('worker_temp_repositories:' + str(worker['id'])))
            reads = []
            factory = self.store.db.row_factory
            def counted(cursor, row):
                reads.append(tuple(row))
                return sqlite3.Row(cursor, row)
            self.store.db.row_factory = counted
            try:
                for _ in range(5):
                    await service.workers.cleanup_finished()
                    await self.drain(service)
            finally:
                self.store.db.row_factory = factory
        self.assertLessEqual(len(reads), 5)


class RelaunchTests(ReviewSupport, unittest.IsolatedAsyncioTestCase):
    def probe(self):
        return '''
scratch = Path(os.environ['TMPDIR'])
with Path(SEEN).open('a') as log:
    log.write(json.dumps(sorted(path.name for path in scratch.iterdir())) + '\\n')
(scratch / 'build-proof').write_text('keep')
'''.replace('SEEN', repr(str(self.root / 'scratch-seen')))

    def quota_runner(self, second=False):
        if second:
            other = self.root / 'second'
            other.mkdir()
            profiles = self.store.get('accounts')
            profiles['second'] = {'config_dir': str(other), 'enabled': True}
            self.store.put('accounts', profiles)
            statuses = self.store.get('account_status')
            statuses['second'] = {'identity': {'logged_in': True, 'email': 'second@example.test'},
                                  'observed_at': time.time(), 'usage': {'seven_day': {'utilization': 1}}}
            self.store.put('account_status', statuses)
        runner = self.fake_claude(self.probe())
        native = runner.run
        attempts = []

        async def run(*args, **options):
            result = await native(*args, **options)
            attempts.append(result)
            if len(attempts) == 1:
                transcript = self.root / 'profile' / 'projects' / 'test' / (result.session_id + '.jsonl')
                transcript.parent.mkdir(parents=True, exist_ok=True)
                transcript.write_text('{}\n')
                result.transcript_path = str(transcript)
                result.success = False
                result.quota_limited = True
                result.managed = False
                result.rate_limit_info = {'status': 'rejected', 'rateLimitType': 'five_hour',
                                          'resetsAt': time.time() + 3600}
            return result

        runner.run = run
        return runner

    def seen(self):
        return [json.loads(line) for line in (self.root / 'scratch-seen').read_text().splitlines()]

    async def test_account_rotation_preserves_temp_until_worker_finishes(self):
        worker, _ = self.spawn()
        service = Service(self.store, None, self.quota_runner(second=True), self.root)
        with self.temp_scope():
            done = await service.workers.run(worker, self.store.topic(TOPIC))
            self.assertTrue(json.loads(done['result'])['success'], done)
            self.assertEqual(self.seen(), [[], ['build-proof']])
            await self.drain(service)
        self.assertFalse(self.scratch(worker).exists())

    async def test_waiting_for_quota_requeue_preserves_temp_until_final(self):
        worker, _ = self.spawn()
        service = Service(self.store, None, self.quota_runner(), self.root)
        with self.temp_scope():
            waiting = await service.workers.run(worker, self.store.topic(TOPIC))
            self.assertEqual(waiting['status'], 'waiting_for_quota')
            self.assertEqual((self.scratch(worker) / 'build-proof').read_text(), 'keep')
            self.store.put('account_blocks', {})
            self.store.put('account_status', {'one': {'observed_at': time.time(),
                'identity': {'logged_in': True, 'email': 'one@example.test'}, 'usage': {}}})
            await service.workers_once()
            await asyncio.gather(*service.worker_tasks.values())
            done = service.workers.get(worker['id'])
            self.assertTrue(json.loads(done['result'])['success'], done)
            self.assertEqual(self.seen(), [[], ['build-proof']])
            await self.drain(service)
        self.assertFalse(self.scratch(worker).exists())

    async def test_codex_attached_rotation_preserves_the_same_temp_folder(self):
        worker, _ = self.spawn('codex')
        self.store.db.execute("UPDATE workers SET status='running',session=?,fresh=0 WHERE id=?", (SID, worker['id']))
        binary = self.root / 'fake-rotate-codex'
        script = SERVER.replace('SID', repr(SID)).replace('ROOT', repr(str(self.root)))
        binary.write_text(script.replace('goal = None', self.probe() + '\ngoal = None', 1))
        binary.chmod(0o700)
        service = Service(self.store, None, ProviderRunner(self.store.directory, {'codex': str(binary)}), self.root)
        pool = service.workers
        options = {'worker_id': worker['id'], 'host_id': 'worker-' + str(worker['id']),
                   'instructions': None, 'control': RunControl()}
        with self.temp_scope(), patch.object(pool.runner, '_external_writer', AsyncMock(return_value=False)):
            first = await pool.runner.run('codex', 'build', self.root, None, fresh=True, **options)
            self.assertTrue(first.success, first)
            with patch.object(pool.codex_accounts, 'record_rate_limit'), \
                    patch.object(pool.codex_accounts, 'automatic', return_value=True), \
                    patch.object(pool.codex_accounts, 'select', return_value='next'):
                result = await pool.codex_attached(pool.get(worker['id']), self.store.topic(TOPIC),
                                                  RunResult(SID, quota_limited=True), options, lambda alias: None)
            self.assertTrue(result.success, result)
            self.assertEqual(self.seen(), [[], ['build-proof']])
            self.store.worker_complete(worker['id'], {'success': True, 'session_id': SID}, 'done')
            await pool.cleanup_finished()
            await self.drain(service)
        self.assertFalse(self.scratch(worker).exists())


class OpenCodeInstructionTests(ReviewSupport, unittest.IsolatedAsyncioTestCase):
    async def test_opencode_omits_results_that_external_directory_permission_denies(self):
        from tests.fake_opencode import install_fake_opencode
        from tests.test_opencode_runtime import ACCOUNT
        binary = install_fake_opencode(self.root / 'opencode-fake')
        key_home = self.root / 'opencode-key'
        key_home.mkdir()
        (key_home / 'key').write_text('fake-only')
        (key_home / 'key').chmod(0o600)
        self.store.put('opencode_accounts', {'one': dict(ACCOUNT)})
        self.store.put('opencode_active_account', 'one')
        self.store.put('opencode_enabled', True)
        self.store.put('opencode_binary', str(binary))
        for enabled in (False, True):
            with self.subTest(worker_temp=enabled), self.temp_scope(), \
                    patch('coordinator.providers.platform', 'linux'), \
                    patch('coordinator.opencode_accounts.account_home', return_value=key_home), \
                    patch('coordinator.opencode_runtime.managed_preference_paths', return_value=()):
                self.store.put('worker_temp', enabled)
                worker, spawned = self.spawn('opencode')
                service = Service(self.store, None, ProviderRunner(self.store.directory, opencode_store=self.store), self.root)
                done = await service.workers.run(worker, self.store.topic(TOPIC))
                self.assertTrue(json.loads(done['result'])['success'], done)
                from coordinator.workers import host_directory
                spec = json.loads((host_directory(self.store.directory, done) / 'spec.json').read_text())
                payload = spec['initial_payload']
                self.assertNotIn(str(self.expected_results), payload)
                self.assertNotIn('results folder', payload)
                config = json.loads((Path(spec['env']['XDG_CONFIG_HOME']) / 'opencode/opencode.json').read_text())
                self.assertEqual(config['permission']['external_directory'], 'deny')
                self.assertEqual(spawned.data['results'], str(self.expected_results))
                self.assertEqual(self.expected_results.stat().st_mode & 0o777, 0o700)
                await self.drain(service)
