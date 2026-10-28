import asyncio
import base64
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import unittest
from unittest.mock import patch

from coordinator import control_api, control_ui, envelopes, mcp
from coordinator.service import Service
from coordinator.store import Store
from coordinator.vault import FileVault
from tests import test_envelopes as te
from tests import test_group_setup as tg


OLD = 'GITHUB_TOKEN'
NEW = 'GITHUB_PERSONAL_TOKEN'


class Interrupted(BaseException):
    pass


class SecretRenameTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = te.EndToEndTests.asyncSetUp
    asyncTearDown = te.EndToEndTests.asyncTearDown

    def vault(self):
        key = self.root / 'fixture-key'
        key.write_bytes(bytes(range(32)))
        key.chmod(0o600)
        self.service.vault = FileVault(self.state, key_file=key)
        self.store.vault = self.service.vault
        self.service.workers.vault = self.service.vault
        return self.service.vault

    def call(self, op, **params):
        with self.store.db:
            return control_api.call(self.store, op, params, topic=te.TOPIC, source='cli')

    async def fill(self, name=OLD, value=te.VALUE, task=None):
        result = self.call('secret.ask', name=name, reason='Release', consumer='release worker', task=task)
        self.assertTrue(result.ok, result.text)
        envelope = result.data['envelope']
        await envelopes.run_effects(self.store, self.telegram)
        number = 1 + self.store.db.execute('SELECT count(*) FROM updates').fetchone()[0]
        self.assertEqual(self.store.accept(te.private(number, value, date=self.telegram.date + 1)), 'envelope_fill')
        return envelope

    def snapshot(self):
        tables = [row[0] for row in self.store.db.execute("SELECT name FROM sqlite_master WHERE type='table'")
                  if row[0] not in ('service_requests', 'cli_calls')]
        return {table: [tuple(row) for row in self.store.db.execute('SELECT * FROM ' + table + ' ORDER BY rowid')]
                for table in tables}

    def queued(self, name=OLD, new_name=NEW):
        result = self.call('secret.rename', name=name, new_name=new_name)
        self.assertEqual(result.state, 'queued', result.text)
        return result.data['request']

    def outcome(self, request):
        row = self.store.db.execute('SELECT state,result FROM service_requests WHERE id=?', (request,)).fetchone()
        return row['state'], json.loads(row['result']) if row['result'] else None

    async def rename(self, name=OLD, new_name=NEW):
        request = self.queued(name, new_name)
        await self.service.controls_once()
        state, result = self.outcome(request)
        self.assertEqual(state, 'done', result)
        return result

    async def held_row(self):
        with self.store.db:
            self.store.put('coordinator_home_topic', te.TOPIC)
        project = self.root / 'held-project'
        project.mkdir()
        outcomes = []

        async def create_topic(chat, name):
            outcomes.append(self.store.accept(tg.group_update(901, 'Work sent before linking', thread=11)))
            return 11

        self.telegram.create_topic = create_topic
        result = self.call('topic.create', name='Held project', cwd=str(project))
        self.assertEqual(result.state, 'queued', result.text)
        request = result.data['request']
        for _ in range(15):
            if self.outcome(request)[0] not in ('queued', 'sending'):
                break
            await self.service.controls_once()
        self.assertEqual(self.outcome(request)[0], 'done', self.outcome(request))
        self.assertEqual(outcomes, ['topic_creating'])
        early = self.store.db.execute('SELECT topic FROM messages WHERE telegram_message=901').fetchone()
        self.assertEqual(early['topic'], '-10042:11')
        self.assertEqual(self.store.get('topic_create_held:-10042:11'), [])

    async def held_row_crash(self, point):
        self.vault()
        await self.fill()
        task = self.store.task_create(te.TOPIC, 'Held-row crash recovery', secrets=[OLD, 'GH_ALIAS=' + OLD])
        await self.held_row()
        await self.assert_crash_recovers(point, task)

    async def test_8_held_row_before_journal(self):
        await self.held_row_crash('before_journal')

    async def test_8_held_row_before_copy(self):
        await self.held_row_crash('before_copy')

    async def test_8_held_row_after_copy(self):
        await self.held_row_crash('after_copy')

    async def test_8_held_row_readback(self):
        await self.held_row_crash('readback')

    async def test_8_held_row_store_rollback(self):
        await self.held_row_crash('store_rollback')

    async def test_8_held_row_before_delete(self):
        await self.held_row_crash('before_delete')

    async def test_8_held_row_after_delete(self):
        await self.held_row_crash('after_delete')

    async def test_8_held_row_before_settlement(self):
        await self.held_row_crash('before_settlement')

    async def test_9_held_row_vault_calls_have_durable_store_steps(self):
        vault = self.vault()
        await self.fill()
        await self.held_row()
        calls = []

        def observe(method):
            original = getattr(vault, method)

            def checked(*args):
                self.assertFalse(self.store.db.in_transaction, 'Vault calls run outside transactions')
                with sqlite3.connect(str(self.state / 'state.sqlite')) as other:
                    if method == 'put':
                        journal = other.execute('SELECT value FROM settings WHERE key=?',
                                                (envelopes.RENAME_KEY,)).fetchone()
                        self.assertEqual(json.loads(journal[0])['phase'], 'prepared')
                    if method == 'delete':
                        filled = other.execute("SELECT name FROM envelopes WHERE state='filled'").fetchall()
                        self.assertEqual(filled, [(NEW,)])
                calls.append(method)
                return original(*args)

            return checked

        with patch.object(vault, 'names', side_effect=observe('names')), \
                patch.object(vault, 'get', side_effect=observe('get')), \
                patch.object(vault, 'put', side_effect=observe('put')), \
                patch.object(vault, 'delete', side_effect=observe('delete')):
            await self.rename()
        self.assertIn('put', calls)
        self.assertIn('delete', calls)

    async def test_1_each_refusal_leaves_vault_bytes_and_store_rows_unchanged(self):
        vault = self.vault()
        await self.fill()
        await self.fill('MISSING_VALUE')
        vault.delete('MISSING_VALUE')
        vault.put('ORPHAN_KEY', 'FAKE-ORPHAN')
        taken = []
        for state in ('open', 'armed', 'filled', 'superseded', 'cancelled', 'expired', 'revoked'):
            target = 'TAKEN_' + state.upper()
            envelope = await self.fill(target)
            taken.append((state, envelope))
            vault.delete(target)
        self.call('secret.ask', name='UNFILLED_KEY', reason='Test', consumer='test')
        with self.store.db:
            self.store.db.execute("UPDATE envelopes SET state='open',chat=NULL WHERE name='UNFILLED_KEY'")
            for state, envelope in taken:
                self.store.db.execute('UPDATE envelopes SET state=? WHERE id=?', (state, envelope))
        cases = [('lowercase', NEW, 'secret name'), (OLD, 'PATH', 'secret name'),
                 ('A', NEW, 'secret name'), (OLD, OLD, 'different'),
                 ('UNKNOWN_KEY', NEW, 'filled'), ('UNFILLED_KEY', NEW, 'filled'),
                 ('MISSING_VALUE', NEW, 'stored'), (OLD, 'ORPHAN_KEY', 'vault'),
                 (OLD, 'ENVIRONMENT_KEY', 'environment')]
        cases.extend((OLD, 'TAKEN_' + state.upper(), 'envelope') for state in
                     ('open', 'armed', 'filled', 'superseded', 'cancelled', 'expired', 'revoked'))
        for name, new_name, reason in cases:
            with self.subTest(name=name, new_name=new_name), patch.dict(os.environ, {'ENVIRONMENT_KEY': ''}):
                before, sealed = self.snapshot(), vault.path.read_bytes()
                result = self.call('secret.rename', name=name, new_name=new_name)
                if result.state == 'queued':
                    await self.service.controls_once()
                    state, data = self.outcome(result.data['request'])
                    text = data['text']
                else:
                    state, text = result.state, result.text
                self.assertEqual(state, 'refused')
                self.assertIn(reason, text)
                self.assertNotIn('\n', text)
                self.assertEqual(self.snapshot(), before)
                self.assertEqual(vault.path.read_bytes(), sealed)
        for state in ('open', 'armed'):
            with self.subTest(source_state=state):
                opened = self.call('secret.ask', name=OLD, reason='Rotate', consumer='test').data['envelope']
                with self.store.db:
                    self.store.db.execute('UPDATE envelopes SET state=? WHERE id=?', (state, opened))
                try:
                    before, sealed = self.snapshot(), vault.path.read_bytes()
                    request = self.queued()
                    await self.service.controls_once()
                    result, data = self.outcome(request)
                    self.assertEqual(result, 'refused')
                    self.assertIn('open or armed', data['text'])
                    self.assertEqual(self.snapshot(), before)
                    self.assertEqual(vault.path.read_bytes(), sealed)
                finally:
                    with self.store.db:
                        self.store.db.execute("UPDATE envelopes SET state='cancelled' WHERE id=?", (opened,))

    async def test_2_verified_move_keeps_value_out_of_results_logs_and_database(self):
        vault = self.vault()
        await self.fill()
        write = vault.put
        with patch.object(vault, 'put', side_effect=RuntimeError(te.VALUE)):
            request = self.queued()
            await self.service.controls_once()
        self.assertEqual(self.outcome(request)[0], 'failed')
        with patch.object(vault, 'put', side_effect=lambda name, value: write(name, 'FAKE-WRONG-COPY')):
            request = self.queued()
            await self.service.controls_once()
        self.assertEqual(self.outcome(request)[0], 'failed')
        self.assertEqual(vault.get(OLD), te.VALUE)
        self.assertEqual(self.call('secret.list').data[0]['name'], OLD)
        before, sealed = self.snapshot(), vault.path.read_bytes()
        for name in (OLD, NEW):
            self.assertEqual(self.call('secret.ask', name=name, reason='Test', consumer='test').state, 'refused')
            self.assertEqual(self.call('secret.revoke', name=name).state, 'refused')
        other = self.queued('UNRELATED_KEY', 'UNRELATED_NEW_KEY')
        await self.service.controls_once()
        self.assertEqual(self.outcome(other)[0], 'refused')
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(vault.path.read_bytes(), sealed)
        remove = vault.delete
        checked = []

        def delete(name):
            self.assertEqual(name, OLD)
            fingerprint = self.store.db.execute("SELECT fingerprint FROM envelopes WHERE name=? AND state='filled'",
                                                (NEW,)).fetchone()[0]
            self.assertEqual(vault.fingerprint(vault.get(NEW)), fingerprint)
            self.assertEqual(vault.get(OLD), vault.get(NEW))
            checked.append(name)
            return remove(name)

        with patch.object(vault, 'delete', side_effect=delete):
            result = await self.rename()
        self.assertEqual(checked, [OLD])
        self.assertEqual(vault.names(), [NEW])
        self.assertEqual(vault.get(NEW), te.VALUE)
        self.store.db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        forms = [te.VALUE.encode(), te.VALUE.encode().hex().encode(), base64.b64encode(te.VALUE.encode())]
        for path in self.state.rglob('*'):
            if path.is_file():
                for form in forms:
                    self.assertNotIn(form, path.read_bytes(), str(path))
        self.assertNotIn(te.VALUE, json.dumps(result))

    async def test_3_metadata_history_and_secret_views_move_with_shared_keys(self):
        vault = self.vault()
        task = self.store.task_create(te.TOPIC, 'Used this key', worktree=str(self.work), secrets=[OLD])
        other = self.store.task_create(te.TOPIC, 'Another job', secrets=[OLD])
        await self.fill()
        await self.fill(task=task['id'])
        old_use = envelopes.load_for_worker(self.store, vault, self.store.task_get(task['id']))[1]
        worker = self.call('workers.spawn', task=task['id'], provider='claude', prompt='release').data['worker']
        with self.store.db:
            envelopes.record_use(self.store, [(self.store.db.execute(
                "SELECT id FROM envelopes WHERE name=? AND state='filled'", (OLD,)).fetchone()[0], OLD)], worker)
        before = self.call('secret.list', name=OLD).data[0]
        self.assertEqual(envelopes.load_for_worker(self.store, vault, self.store.task_get(other['id']))[0],
                         {OLD: te.VALUE})
        rows = [dict(row) for row in self.store.db.execute('SELECT * FROM envelopes ORDER BY id')]
        events = [dict(row) for row in self.store.db.execute('SELECT * FROM envelope_events ORDER BY id')]
        await self.rename()
        self.assertEqual([dict(row) for row in self.store.db.execute('SELECT * FROM envelopes ORDER BY id')],
                         [dict(row, name=NEW) for row in rows])
        self.assertEqual([dict(row) for row in self.store.db.execute('SELECT * FROM envelope_events ORDER BY id')],
                         [dict(row, name=NEW) for row in events])
        self.assertEqual(self.call('secret.list', name=NEW).data, [dict(before, name=NEW)])
        self.assertEqual(self.call('secret.list', name=OLD).data, [])
        listing, buttons = control_ui._secrets_view(self.store, 0)
        detail, controls = control_ui._secret_view(self.store, te.TOPIC, NEW)
        self.assertNotIn(OLD, listing + detail + json.dumps(buttons + controls))
        self.assertIn(NEW, listing + detail)
        self.assertIsNone(control_ui._secret_view(self.store, te.TOPIC, OLD))
        self.assertEqual(envelopes.load_for_worker(self.store, vault, self.store.task_get(other['id']))[0],
                         {OLD: te.VALUE})
        with self.store.db:
            envelopes.record_use(self.store, old_use, worker)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM envelope_events WHERE name=?', (OLD,)).fetchone()[0], 0)

    async def test_4_all_job_statuses_keep_environment_names_and_launch_and_resume(self):
        vault = self.vault()
        await self.fill()
        await self.fill('FAKE_OTHER_ENV', 'FAKE-OTHER-VALUE')
        tasks, environments = [], []
        for status in ('open', 'done', 'dropped'):
            task = self.store.task_create(te.TOPIC, status, worktree=str(self.work),
                secrets=[OLD, 'GH_ALIAS=' + OLD, NEW + '=' + OLD, 'FAKE_OTHER_ENV',
                         'FAKE_OTHER_ALIAS=FAKE_OTHER_ENV'])
            self.store.task_update(task['id'], status=status)
            tasks.append(task)
            environments.append(envelopes.load_for_worker(self.store, vault, self.store.task_get(task['id']))[0])
        untouched = self.store.task_create(te.TOPIC, 'Untouched', secrets=['FAKE_OTHER_ENV'])
        self.binary.write_text(te.FAKE_WORKER.replace(
            "sid=sys.argv[sys.argv.index('--session-id')+1]",
            "flag='--session-id' if '--session-id' in sys.argv else '--resume'\nsid=Path(sys.argv[sys.argv.index(flag)+1]).stem\n"
            "transcript=Path(os.environ['CLAUDE_CONFIG_DIR'])/'projects'/'fixture'/(sid+'.jsonl')\n"
            "transcript.parent.mkdir(parents=True,exist_ok=True)\n"
            "with transcript.open('a') as output: output.write('{}\\n')").replace(
            "value=os.environ['GITHUB_TOKEN']", "value=os.environ['GITHUB_TOKEN']\nassert os.environ['GH_ALIAS']==value\n"
            "assert os.environ['GITHUB_PERSONAL_TOKEN']==value\n"
            "assert os.environ['FAKE_OTHER_ENV']=='FAKE-OTHER-VALUE'"))
        worker = self.call('workers.spawn', task=tasks[0]['id'], provider='claude', prompt='before').data['worker']
        await self.service.workers_once()
        await asyncio.gather(*self.service.worker_tasks.values())
        self.assertTrue(json.loads(self.service.workers.get(worker)['result'])['success'],
                        self.service.workers.get(worker)['result'])
        result = await self.rename()
        self.assertIn('3 jobs', result['text'])
        expected = envelopes.parse_declarations([OLD + '=' + NEW, 'GH_ALIAS=' + NEW, NEW,
                                                 'FAKE_OTHER_ENV', 'FAKE_OTHER_ALIAS=FAKE_OTHER_ENV'])
        for task, environment in zip(tasks, environments):
            current = self.store.task_get(task['id'])
            self.assertEqual(current['secrets'], expected)
            self.assertEqual(envelopes.parse_declarations(current['secrets']), current['secrets'])
            self.assertEqual(envelopes.load_for_worker(self.store, vault, current)[0], environment)
        self.assertEqual(self.store.task_get(untouched['id'])['secrets'], ['FAKE_OTHER_ENV'])
        self.assertEqual(environments[0][OLD], te.VALUE)
        steered = self.call('workers.steer', worker=worker, prompt='Resume with the same environment')
        self.assertEqual(steered.state, 'queued', steered.text)
        await self.service.controls_once()
        await self.service.workers_once()
        await asyncio.gather(*self.service.worker_tasks.values())
        resumed = self.service.workers.get(worker)
        self.assertTrue(json.loads(resumed['result'])['success'], resumed['result'])
        self.assertIn('resume-', resumed['host'])
        spawned = self.call('workers.spawn', task=tasks[0]['id'], provider='claude', prompt='after').data['worker']
        await self.service.workers_once()
        await asyncio.gather(*self.service.worker_tasks.values())
        self.assertTrue(json.loads(self.service.workers.get(spawned)['result'])['success'])
        self.assertEqual((self.work / 'worker-saw').read_text(), str(len(te.VALUE)))

    async def test_5_every_crash_point_recovers_after_reopening_store_and_vault(self):
        self.vault()
        await self.fill()
        task = self.store.task_create(te.TOPIC, 'Crash recovery', secrets=[OLD, 'GH_ALIAS=' + OLD])
        for point in ('before_journal', 'before_copy', 'after_copy', 'readback', 'store_rollback',
                      'before_delete', 'after_delete', 'before_settlement'):
            with self.subTest(point=point):
                await self.assert_crash_recovers(point, task)
                await self.rename(NEW, OLD)

    async def assert_crash_recovers(self, point, task):
        vault = self.service.vault
        request = self.queued()
        put, get, delete, setting, settle = vault.put, vault.get, vault.delete, self.store.put, self.store.service_request_settle

        def crash_put(name, value):
            if point == 'before_copy':
                raise Interrupted()
            result = put(name, value)
            if point == 'after_copy':
                raise Interrupted()
            return result

        def crash_get(name):
            if point == 'before_journal' and name == OLD:
                raise Interrupted()
            result = get(name)
            if point == 'readback' and name == NEW:
                raise Interrupted()
            return result

        def crash_setting(key, value):
            setting(key, value)
            if point == 'store_rollback' and key == 'secret_rename_v1' and value['phase'] == 'moved':
                raise Interrupted()

        def crash_delete(name):
            if point == 'before_delete':
                raise Interrupted()
            result = delete(name)
            if point == 'after_delete':
                raise Interrupted()
            return result

        def crash_settle(*args, **kwargs):
            if point == 'before_settlement':
                raise Interrupted()
            return settle(*args, **kwargs)

        with patch.object(vault, 'put', side_effect=crash_put), patch.object(vault, 'get', side_effect=crash_get), \
                patch.object(vault, 'delete', side_effect=crash_delete), \
                patch.object(self.store, 'put', side_effect=crash_setting), \
                patch.object(self.store, 'service_request_settle', side_effect=crash_settle):
            with self.assertRaises(Interrupted):
                await self.service.controls_once()
        self.store.close()
        self.store = Store(self.state)
        vault = FileVault(self.state, key_file=self.root / 'fixture-key')
        self.service = Service(self.store, te.FakeTelegram(), object(), self.root, vault=vault)
        for row in self.store.db.execute("SELECT name,fingerprint FROM envelopes WHERE state='filled'"):
            self.assertIn(row['name'], vault.names(), 'Store shows a filled name missing from the vault')
            self.assertEqual(vault.fingerprint(vault.get(row['name'])), row['fingerprint'])
        self.assertEqual(envelopes.load_for_worker(self.store, vault, self.store.task_get(task['id']))[0],
                         {OLD: te.VALUE, 'GH_ALIAS': te.VALUE})
        self.assertEqual(self.queued(), request)
        await self.service.controls_once()
        self.assertEqual(self.outcome(request)[0], 'done')
        self.assertIn('1 jobs', self.outcome(request)[1]['text'])
        self.assertEqual(vault.names(), [NEW])
        self.assertIsNone(self.store.get('secret_rename_v1'))
        self.assertEqual(vault.get(NEW), te.VALUE)


    async def test_6_result_counts_jobs_without_telegram_or_card_edits(self):
        self.vault()
        await self.fill()
        before = {table: [tuple(row) for row in self.store.db.execute('SELECT * FROM ' + table)]
                  for table in ('outbox', 'messages', 'envelope_effects')}
        calls = list(self.telegram.calls)
        result = await self.rename()
        self.assertIn(OLD, result['text'])
        self.assertIn(NEW, result['text'])
        self.assertIn('0 jobs', result['text'])
        self.assertEqual(self.telegram.calls, calls)
        for table, rows in before.items():
            self.assertEqual([tuple(row) for row in self.store.db.execute('SELECT * FROM ' + table)], rows)

    async def test_7_ctl_and_service_path_without_mcp_tools_or_secrets_buttons(self):
        self.vault()
        await self.fill()
        manifest = subprocess.run([sys.executable, '-m', 'coordinator', '--state-dir', str(self.state), 'ctl', 'list'],
                                  capture_output=True, text=True, check=True)
        op = next((row for row in json.loads(manifest.stdout) if row['id'] == 'secret.rename'), None)
        self.assertIsNotNone(op)
        self.assertEqual((op['kind'], op['transport'], op['params']),
                         ('write', 'service', {'name': 'secret_name', 'new_name': 'secret_name'}))
        before = mcp.tools_hash()
        self.assertNotIn('secret_rename', {tool['name'] for tool in mcp.tools_list()})
        with self.assertRaisesRegex(ValueError, 'Unknown tool'):
            mcp.dispatch(self.store, {'method': 'tools/call', 'params': {'name': 'secret_rename',
                'arguments': {'name': OLD, 'new_name': NEW}}})
        with (self.state / 'service.lock').open('w') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            done = subprocess.run([sys.executable, '-m', 'coordinator', '--state-dir', str(self.state),
                'ctl', 'call', 'secret.rename', 'name=' + OLD, 'new_name=' + NEW], capture_output=True, text=True)
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        queued = json.loads(done.stdout)
        self.assertEqual(queued['state'], 'queued')
        self.assertEqual(self.service.vault.names(), [OLD])
        await self.service.controls_once()
        self.assertEqual(self.outcome(queued['data']['request'])[0], 'done')
        self.assertEqual(mcp.tools_hash(), before)
        for screen in (control_ui._secrets_view(self.store, 0), control_ui._secret_view(self.store, te.TOPIC, NEW)):
            self.assertNotIn('Rename', json.dumps(screen))
            self.assertNotIn('secret.rename', json.dumps(screen))


if __name__ == '__main__':
    unittest.main()
