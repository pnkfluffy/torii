import errno
import fcntl
import asyncio
import json
import os
import random
import string
import subprocess
import sqlite3
import sys
import shutil
import time
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from coordinator import control_api, mcp, records
from coordinator.store import Store


def texts(store, job):
    return [entry['text'] for entry in records.journal(records.journal_path(store.directory, job))]


def fake_pat():
    """A random-looking GitHub token shape that real gitleaks 8.30 flags. It is not a real token."""
    chance = random.Random(714)
    return 'ghp_' + ''.join(chance.choice(string.ascii_letters + string.digits) for _ in range(36))


class RecordCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store(Path(self.temp.name))
        self.addCleanup(self.store.close)
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('-1:2',-1,2,'Test','')")


class RecordTests(RecordCase):
    def test_create_update_and_close(self):
        job = self.store.task_create('-1:2', 'First')
        filename = records.path(self.store.directory, job)
        data, body = records.read(filename)
        self.assertEqual((data['title'], data['status'], body['ask']), ('First', 'open', 'First'))
        self.assertTrue(data['created'].endswith('Z'))
        self.store.task_update(job['id'], 'cli', title='Changed', note='A note')
        data, body = records.read(filename)
        self.assertEqual((data['title'], set(body), texts(self.store, job)), ('Changed', {'ask', 'outcome'}, ['A note']))
        self.store.task_update(job['id'], status='done')
        data, body = records.read(filename)
        self.assertEqual(data['status'], 'done')
        self.assertIsNotNone(data['closed'])
        self.assertEqual(records.check(self.store)['count'], 0)

    def test_ask_uses_event_text_and_preserves_owner_json(self):
        human_text = 'Please check the result.\nRun the test again.'
        envelope = json.dumps({'text': human_text, 'session_id': 'test-session', 'pid': 42,
                               'log_path': '/test/worker.log', 'transcript_path': '/test/worker.jsonl',
                               'cwd': '/test/worktree'})
        for kind, source, expected in (
            ('worker_result', envelope, human_text),
            ('worker_result', json.dumps({'log_path': '/test/worker.log'}), ''),
            ('event', envelope, human_text),
            ('event', 'Plain event text.', 'Plain event text.'),
            ('owner', envelope, envelope),
        ):
            with self.subTest(kind=kind, expected=expected):
                origin = self.store.message_save('-1:2', kind, source)
                result = control_api.call(self.store, 'tasks.create', {'title': 'Check result', 'message': origin['id']})
                self.assertTrue(result.ok, result.text)
                _, body = records.read(records.path(self.store.directory, result.data))
                self.assertEqual(body['ask'], expected)

    def test_durable_settings_and_rules_restore_without_secret_values(self):
        from coordinator.policy import chat_path
        personal = self.store.directory / 'personal.md'
        personal.write_text('Personal rule\n')
        chat = chat_path(self.store, '-1:2')
        chat.parent.mkdir(parents=True)
        chat.write_text('Chat rule\n')
        with self.store.db:
            self.store.put('codex_model', 'example-model')
            self.store.put('worker_model', 'sk-' + 'x' * 40)
            self.store.put('account_status', {'token': 'not for the ledger'})
            self.store.put('session_transcripts', {'1': '/private/session'})
            self.store.put('loadout.swaps', [{'message': 123}])
            self.store.put('worker_idle_alerts', {'1': {'progress': 9}})
            self.store.put('telegram_update_v1', {'message': 456, 'phase': 'checking'})
            self.store.put('close_saves_keep', [5])
            self.store.put('claude_worker_effort', 'high')
        records.backfill(self.store)
        root = self.store.directory / 'records'
        settings = json.loads((root / 'settings.json').read_text())
        self.assertEqual(settings['codex_model'], 'example-model')
        self.assertNotIn('worker_model', settings)
        self.assertNotIn('account_status', settings)
        self.assertNotIn('session_transcripts', settings)
        self.assertNotIn('loadout.swaps', settings)
        self.assertNotIn('worker_idle_alerts', settings)
        self.assertNotIn('telegram_update_v1', settings)
        self.assertEqual(settings['close_saves_keep'], [5])
        self.assertEqual(settings['claude_worker_effort'], 'high')
        self.assertNotIn('sk-' + 'x' * 40, (root / 'settings.json').read_text())
        recovered = Store(Path(self.temp.name) / 'restored-config')
        self.addCleanup(recovered.close)
        records.restore(recovered, root)
        self.assertEqual(recovered.get('codex_model'), 'example-model')
        self.assertEqual((recovered.directory / 'personal.md').read_text(), 'Personal rule\n')
        self.assertEqual(chat_path(recovered, '-1:2').read_text(), 'Chat rule\n')
        self.assertEqual(records.check(recovered)['count'], 0)
        chat_path(recovered, '-1:2').write_text('Changed\n')
        self.assertGreater(records.check(recovered)['count'], 0)

    def test_backup_names_personal_and_tag_rules_and_restores_tag_order(self):
        (self.store.directory / 'personal.md').write_text('Shared rule\n')
        (self.store.directory / 'tags').mkdir()
        (self.store.directory / 'tags' / 'dev_agent.md').write_text('Dev rule\n')
        (self.store.directory / 'tags' / 'personal_agent.md').write_text('Personal agent rule\n')
        result = control_api.call(self.store, 'chat.tags', {'topic': '-1:2', 'tags': 'personal_agent,dev_agent'})
        self.assertTrue(result.ok, result.text)
        records.backfill(self.store)
        self.assertEqual(records.check(self.store)['count'], 0)
        root = self.store.directory / 'records'
        self.assertEqual((root / 'rules' / 'personal.md').read_text(), 'Shared rule\n')
        self.assertEqual(json.loads((root / 'rules.json').read_text()),
                         {'personal': 'rules/personal.md', 'tags/dev_agent': 'rules/tags/dev_agent.md',
                          'tags/personal_agent': 'rules/tags/personal_agent.md'})
        records.sync(self.store.directory)
        committed = subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', 'HEAD'], cwd=root, text=True)
        self.assertIn('rules/tags/dev_agent.md', committed.splitlines())
        recovered = Store(Path(self.temp.name) / 'restored-tags')
        self.addCleanup(recovered.close)
        source = Path(self.temp.name) / 'backup'
        shutil.copytree(root, source, ignore=shutil.ignore_patterns('.git'))
        records.restore(recovered, source)
        self.assertEqual((recovered.directory / 'personal.md').read_text(), 'Shared rule\n')
        self.assertEqual((recovered.directory / 'tags' / 'dev_agent.md').read_text(), 'Dev rule\n')
        self.assertEqual((recovered.directory / 'tags' / 'personal_agent.md').read_text(), 'Personal agent rule\n')
        self.assertEqual(recovered.get('chat_tags:-1:2'), ['personal_agent', 'dev_agent'])
        self.assertEqual(records.check(recovered)['count'], 0)

    def test_restore_refuses_a_ledger_with_legacy_rule_names(self):
        old = Path(self.temp.name) / 'old-ledger'
        records.backfill(self.store)
        shutil.copytree(self.store.directory / 'records', old)
        (old / 'rules').mkdir()
        (old / 'rules' / 'global.md').write_text('Old shared rule\n')
        (old / 'rules.json').write_text(json.dumps({'global': 'rules/global.md'}))
        recovered = Store(Path(self.temp.name) / 'restored-old')
        self.addCleanup(recovered.close)
        with self.assertRaisesRegex(ValueError, 'Invalid rules manifest'):
            records.restore(recovered, old)
        self.assertFalse((recovered.directory / 'global.md').exists())

    def test_l4_restore_failure_leaves_partial_state_and_blocks_retry(self):
        """Reviewer repro: a failed restore leaves nothing behind and a retry works."""
        from coordinator.policy import chat_path
        chat = chat_path(self.store, '-1:2')
        chat.parent.mkdir(parents=True)
        chat.write_text('Chat rule\n')
        (self.store.directory / 'personal.md').write_text('Personal rule\n')
        self.store.task_create('-1:2', 'Restored job')
        records.backfill(self.store)
        clone = Path(self.temp.name) / 'clone'
        shutil.copytree(self.store.directory / 'records', clone)
        held = (clone / 'rules' / 'personal.md').read_text()
        (clone / 'rules' / 'personal.md').unlink()
        fresh = Store(Path(self.temp.name) / 'fresh')
        self.addCleanup(fresh.close)

        def leftovers():
            return ((fresh.directory / 'records').exists(), chat_path(fresh, '-1:2').exists(),
                    (fresh.directory / 'personal.md').exists(), sorted(p.name for p in fresh.directory.glob('.records-restore-*')),
                    fresh.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0], len(fresh.topics()))

        with self.assertRaises(OSError):
            records.restore(fresh, clone)
        partial = (fresh.directory / 'records').exists(), chat_path(fresh, '-1:2').exists()
        self.assertEqual(leftovers(), (False, False, False, [], 0, 0))
        (clone / 'rules' / 'personal.md').write_text(held)
        original = records.replace
        calls = []

        def failing(target, content):
            calls.append(target)
            if len(calls) == 2:
                raise OSError('disk full')
            return original(target, content)

        with patch.object(records, 'replace', side_effect=failing), self.assertRaises(OSError):
            records.restore(fresh, clone)
        self.assertEqual(len(calls), 2)
        self.assertEqual(leftovers(), (False, False, False, [], 0, 0))
        try:
            records.restore(fresh, clone)
            retried = 'ok'
        except (OSError, ValueError) as error:
            retried = str(error)
        self.assertEqual((partial, retried), ((False, False), 'ok'))
        self.assertEqual((fresh.directory / 'personal.md').read_text(), 'Personal rule\n')
        self.assertEqual(chat_path(fresh, '-1:2').read_text(), 'Chat rule\n')
        self.assertEqual(fresh.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0], 1)
        self.assertEqual(records.check(fresh)['count'], 0)

    def skipped(self):
        return [row[0] for row in self.store.db.execute(
            "SELECT detail FROM problems WHERE area='records' AND code='rules-skipped' ORDER BY id")]

    def test_secret_shaped_rule_is_not_written(self):
        personal = self.store.directory / 'personal.md'
        secret = 'sk-' + 'x' * 40
        personal.write_text('credential: ' + secret)
        records.backfill(self.store)
        self.assertFalse((self.store.directory / 'records' / 'rules' / 'personal.md').exists())
        personal.write_text('See https://user:' + 'pass' + '@example.invalid/path for setup.')
        records.backfill(self.store)
        self.assertFalse((self.store.directory / 'records' / 'rules' / 'personal.md').exists())
        self.assertEqual(self.skipped(), ['personal.md'])

    def test_unsafe_rule_file_is_skipped_and_others_are_backed_up(self):
        from coordinator.policy import chat_path
        unsafe = chat_path(self.store, '-1:2')
        unsafe.parent.mkdir(parents=True)
        unsafe.write_text('credential: sk-' + 'x' * 40 + '\n')
        (self.store.directory / 'personal.md').write_text('Personal rule\n')
        records.backfill(self.store)
        root = self.store.directory / 'records'
        mapping = json.loads((root / 'rules.json').read_text())
        self.assertNotIn('-1:2', mapping)
        self.assertEqual((root / mapping['personal']).read_text(), 'Personal rule\n')
        self.assertTrue((root / 'settings.json').exists())
        self.assertEqual(self.skipped(), ['chats/-1:2/chat.md'])
        self.assertEqual(records.check(self.store)['count'], 0)

    def test_vault_name_mapping_before_closing_punctuation_is_backed_up(self):
        from coordinator.policy import chat_path
        chat = chat_path(self.store, '-1:2')
        chat.parent.mkdir(parents=True)
        provider = 'CLOUD' + 'FLARE'
        line = '(e.g. ' + provider + '_API_TOKEN=PERSONAL_' + provider + '_API_TOKEN)'
        content = ('- Declare a secret on the job just before the step that needs it\n' + line + '\n'
                   'Use secrets=GITHUB_TOKEN). Or secrets=FIRST_TOKEN,SECOND_TOKEN].\n')
        chat.write_text(content)
        records.backfill(self.store)
        root = self.store.directory / 'records'
        self.assertEqual((root / json.loads((root / 'rules.json').read_text())['-1:2']).read_text(), content)
        self.assertEqual(self.skipped(), [])
        records.check_drift(self.store.directory, [])
        self.assertGreater(self.store.get('records_drift_checked', 0), 0)
        chance = random.Random(8679)
        token = ''.join(chance.choice(string.ascii_letters + string.digits) for _ in range(40))
        for value in ('(API_TOKEN=' + token + ')', '(API_TOKEN=ABC).' + token, 'API_TOKEN=ABC.' + token,
                      'API_TOKEN=ABC:' + token):
            self.assertFalse(records.safe_setting(value, lambda text: text, rules=True), value)

    def test_rule_secret_names_are_backed_up_and_drift_check_passes(self):
        from coordinator.policy import chat_path
        chat = chat_path(self.store, '-1:2')
        chat.parent.mkdir(parents=True)
        provider = 'CLOUD' + 'FLARE'
        content = ('secrets=' + provider + '_API_TOKEN=PERSONAL_' + provider + '_API_TOKEN\n'
                   'secrets=GITHUB_TOKEN\nsecrets=FIRST_TOKEN,SECOND_TOKEN\n')
        chat.write_text(content)
        records.backfill(self.store)
        root = self.store.directory / 'records'
        mapping = json.loads((root / 'rules.json').read_text())
        self.assertEqual((root / mapping['-1:2']).read_text(), content)
        self.assertEqual(records.check(self.store)['count'], 0)
        records.check_drift(self.store.directory, [])
        self.assertGreater(self.store.get('records_drift_checked', 0), 0)
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM problems WHERE area='records' AND code='drift-check-failed'").fetchone()[0], 0)
        self.assertFalse(records.safe_setting('API_TOKEN=PERSONAL_' + provider + '_API_TOKEN', lambda value: value))
        self.assertFalse(records.safe_setting(content, lambda value: value.replace('GITHUB_TOKEN', '[token]'), rules=True))

    def test_rule_random_token_is_not_written(self):
        chance = random.Random(714)
        token = ''.join(chance.choice(string.ascii_letters + string.digits) for _ in range(40))
        (self.store.directory / 'personal.md').write_text('API_TOKEN=' + token)
        records.backfill(self.store)
        self.assertFalse((self.store.directory / 'records' / 'rules' / 'personal.md').exists())

    def test_check_does_not_snapshot_config_if_vault_is_unavailable(self):
        from coordinator.vault import VaultError
        with patch('coordinator.store._known_token_scrubber', side_effect=VaultError('unavailable')):
            with self.assertRaises(VaultError):
                records.check(self.store)
        self.assertFalse((self.store.directory / 'records' / 'settings.json').exists())

    def test_short_vault_value_is_not_in_config_or_rule_copies(self):
        class FakeVault:
            def scrub_values(self):
                return ['short-value']
        self.store.vault = FakeVault()
        with self.store.db:
            self.store.put('codex_model', 'short-value')
        records.backfill(self.store)
        settings = (self.store.directory / 'records' / 'settings.json').read_text()
        self.assertNotIn('short-value', settings)
        self.assertNotIn('codex_model', json.loads(settings))
        (self.store.directory / 'personal.md').write_text('short-value')
        records.check(self.store)
        self.assertFalse((self.store.directory / 'records' / 'rules' / 'personal.md').exists())

    def test_daily_drift_check_reports_and_repairs_a_edited_record(self):
        job = self.store.task_create('-1:2', 'First')
        records.backfill(self.store)
        filename = records.path(self.store.directory, job)
        records.system_entry(self.store, job, 'report', 'Verified result', outbox=1)
        records.backfill(self.store)
        edited = filename.read_text().replace('title: "First"', 'title: "Edited"')
        filename.write_text(edited.replace('Verified result', 'Altered result'))
        records.check_drift(self.store.directory, [])
        data, body = records.read(filename)
        self.assertEqual((data['title'], body['outcome']), ('First', 'Verified result'))
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM problems WHERE area='records' AND code='drift'").fetchone()[0], 1)
        records.check_drift(self.store.directory, [])
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM problems WHERE area='records' AND code='drift'").fetchone()[0], 1)

    def test_daily_drift_check_preserves_existing_closed_ask_and_outcome(self):
        envelope = json.dumps({'text': 'Check result.', 'log_path': '/test/worker.log'})
        message = self.store.message_save('-1:2', 'worker_result', envelope)
        job = self.store.task_create('-1:2', 'Check result', origin=message['id'])
        self.store.task_update(job['id'], status='dropped', note='Drop reason.')
        filename = records.path(self.store.directory, job)
        data, body = records.read(filename)
        body.update(ask=envelope, outcome='')
        legacy = records.render(data, body)
        filename.write_text(legacy)

        records.check_drift(self.store.directory, [])
        self.assertEqual(filename.read_text(), legacy)
        self.assertEqual(records.check(self.store)['count'], 0)
        records.write(self.store, job)
        self.assertEqual(filename.read_text(), legacy)

    def test_record_redacts_before_writing_and_remains_consistent(self):
        token = 'sk-' + 'a' * 40
        job = self.store.task_create('-1:2', 'api_key=' + token)
        self.store.task_update(job['id'], 'cli', note='api_key=' + token)
        filename = records.path(self.store.directory, job)
        data, body = records.read(filename)
        self.assertEqual(data['title'], 'api_key=[redacted]')
        self.assertEqual(body['ask'], 'api_key=[redacted]')
        self.assertEqual(texts(self.store, job), ['api_key=[redacted]'])
        self.assertNotIn(token, filename.read_text())
        self.assertNotIn(token, filename.with_suffix('.jsonl').read_text())
        self.assertEqual(records.check(self.store)['count'], 0)
        self.assertEqual(records.redact('api_key=[redacted]')[1], 0)
        self.assertEqual(records.redact('key=[redacted]`.')[1], 0)
        self.assertEqual(records.redact('key=[redacted]extra')[1], 0)
        self.assertEqual(records.redact('secret:\n- next item')[1], 0)
        self.assertEqual(records.redact('CUSTOM_NAME=abcdefghijk'), ('CUSTOM_NAME=abcdefghijk', 0))

    def test_atomic_rewrite_keeps_old_content_until_replace(self):
        job = self.store.task_create('-1:2', 'First')
        filename = records.path(self.store.directory, job)
        original = filename.read_bytes()
        replace = records.os.replace
        def observe(source, destination):
            self.assertEqual(filename.read_bytes(), original)
            self.assertEqual(Path(source).parent, filename.parent)
            self.assertEqual(self.store.task_get(job['id'])['title'], 'Second')
            replace(source, destination)
        with patch('coordinator.records.os.replace', side_effect=observe):
            self.store.task_update(job['id'], title='Second')
        self.assertEqual(records.read(filename)[0]['title'], 'Second')

    def test_concurrent_rewrites_keep_the_newer_version(self):
        job = self.store.task_create('-1:2', 'Old')
        root = Path(self.temp.name)
        ready, second, release = (root / name for name in ('ready', 'second', 'release'))
        code = """import sys,time
from pathlib import Path
from coordinator import records
from coordinator.store import Store
store = Store(Path(sys.argv[1]), existing=True)
task = store.task_get(int(sys.argv[2]))
if sys.argv[3] == 'first':
    original = records.replace
    def hold(path, content):
        Path(sys.argv[4]).touch()
        while not Path(sys.argv[6]).exists():
            time.sleep(.01)
        original(path, content)
    records.replace = hold
else:
    Path(sys.argv[5]).touch()
records.write(store, task)
store.close()
"""
        def start(which):
            return subprocess.Popen([sys.executable, '-c', code, str(root), str(job['id']), which,
                                     str(ready), str(second), str(release)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        first = start('first')
        self.addCleanup(lambda: first.poll() is None and first.kill())
        deadline = time.monotonic() + 30
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(.01)
        self.assertTrue(ready.exists())
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET title='New' WHERE id=?", (job['id'],))
        other = start('second')
        self.addCleanup(lambda: other.poll() is None and other.kill())
        deadline = time.monotonic() + 30
        while not second.exists() and time.monotonic() < deadline:
            time.sleep(.01)
        self.assertTrue(second.exists())
        release.touch()
        for process in (first, other):
            output, error = process.communicate(timeout=30)
            self.assertEqual(process.returncode, 0, output + error)
        self.assertEqual(records.read(records.path(self.store.directory, job))[0]['title'], 'New')

    def test_create_writes_after_inserting_row(self):
        replace = records.os.replace
        def observe(source, destination):
            self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0], 1)
            replace(source, destination)
        with patch('coordinator.records.os.replace', side_effect=observe):
            self.store.task_create('-1:2', 'First')

    def status(self, job):
        return self.store.db.execute('SELECT status FROM tasks WHERE id=?', (job['id'],)).fetchone()[0]

    def test_disk_full_record_write_does_not_block_job_close(self):
        job = self.store.task_create('-1:2', 'First')
        def full(*args, **kwargs):
            raise OSError(errno.ENOSPC, 'No space left on device')
        with patch('coordinator.records.os.fsync', side_effect=full):
            self.store.task_update(job['id'], 'cli', status='done', note='finished')
        self.assertEqual(self.status(job), 'done')

    def test_disk_full_record_write_does_not_block_job_create(self):
        def full(*args, **kwargs):
            raise OSError(errno.ENOSPC, 'No space left on device')
        with patch('coordinator.records.os.fsync', side_effect=full):
            self.store.task_create('-1:2', 'Second')
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0], 1)

    def test_unwritable_chat_folder_does_not_block_job_close(self):
        job = self.store.task_create('-1:2', 'First')
        folder = records.path(self.store.directory, job).parent
        os.chmod(folder, 0o500)
        self.addCleanup(os.chmod, folder, 0o700)
        self.store.task_update(job['id'], status='done')
        self.assertEqual(self.status(job), 'done')

    def test_check_ignores_informational_worker_spawn(self):
        job = self.store.task_create('-1:2', 'First')
        with self.store.db:
            self.store.db.execute("""INSERT INTO workers(task,topic,provider,prompt,created,updated)
                VALUES (?,?,'codex','work',1,1)""", (job['id'], job['topic']))
        result = records.check(self.store)
        self.assertEqual(result['count'], 0, result)

    def test_check_ignores_informational_follow_up(self):
        parent = self.store.task_create('-1:2', 'Parent')
        self.store.task_update(parent['id'], status='done')
        self.store.task_create('-1:2', 'Child', parent=parent['id'])
        result = records.check(self.store)
        self.assertEqual(result['count'], 0, result)

    def test_rolled_back_update_leaves_record_ahead_of_row(self):
        job = self.store.task_create('-1:2', 'First')
        with patch('coordinator.reactions.set_desired', side_effect=RuntimeError('later step')):
            with self.assertRaises(RuntimeError):
                self.store.task_update(job['id'], status='done')
        self.assertEqual(self.status(job), 'open')
        self.assertEqual(records.read(records.path(self.store.directory, job))[0]['status'], 'open')


    def test_chat_close_of_waiting_job_matches_check(self):
        job = self.store.task_create('-1:2', 'First')
        self.store.task_wait('-1:2', 'owner', 'Pick A or B', task=job['id'])
        self.assertEqual(self.store.task_get(job['id'])['status'], 'waiting')
        time.sleep(1.1)
        with self.store.transaction():
            self.store.wait_close_topic('-1:2')
        self.assertEqual(self.store.task_get(job['id'])['status'], 'dropped')
        result = records.check(self.store)
        self.assertEqual(result['count'], 0, result)


    def test_failing_rows_raise_one_problem_each_and_do_not_starve_new_rows(self):
        jobs = [self.store.task_create('-1:2', 'Job %d' % index) for index in range(40)]
        fresh = self.store.task_create('-1:2', 'Fresh')
        with self.store.db:
            self.store.db.execute('DELETE FROM stale_records')
            for job in jobs:
                self.store.db.execute('INSERT INTO stale_records(task,version) VALUES (?,1)', (job['id'],))
            self.store.db.execute('INSERT INTO stale_records(task,version) VALUES (?,2)', (fresh['id'],))
        real = records.write
        def fail_old(store, task, **options):
            if task['id'] != fresh['id']:
                raise OSError('disk full')
            return real(store, task, **options)
        with patch('coordinator.records.write', side_effect=fail_old):
            records.sweep(self.store)
            records.sweep(self.store)
        rows = self.store.db.execute("SELECT task,detail FROM problems WHERE area='records' AND code='rewrite-failed'").fetchall()
        self.assertEqual(len(rows), 40)
        self.assertEqual(len(rows), len({row[0] for row in rows}))
        self.assertRegex(rows[0][1], r'^item=%d failures=1 OSError: disk full at test_records\.py:\d+ fail_old' % rows[0][0])
        with patch('coordinator.records.write', side_effect=fail_old):
            records.sweep(self.store)
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM problems WHERE code='rewrite-failed'").fetchone()[0], 40)
        self.assertIsNone(self.store.db.execute('SELECT 1 FROM stale_records WHERE task=?', (fresh['id'],)).fetchone())

    def test_record_is_fixed_by_next_sweep_after_one_failure(self):
        job = self.store.task_create('-1:2', 'Retry')
        self.store.task_update(job['id'], status='done')
        with self.store.db:
            self.store.db.execute('INSERT OR REPLACE INTO stale_records(task,version) VALUES (?,1)', (job['id'],))
        with patch('coordinator.records.write', side_effect=OSError('disk full')):
            records.sweep(self.store)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM stale_records').fetchone()[0], 1)
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM problems WHERE code='rewrite-failed'").fetchone()[0], 1)
        records.sweep(self.store)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM stale_records').fetchone()[0], 1)
        with patch('coordinator.sweeps.time.time', return_value=time.time() + 61):
            records.sweep(self.store)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM stale_records').fetchone()[0], 0)
        self.assertIsNone(self.store.db.execute('SELECT 1 FROM sweep_failures').fetchone())
        self.assertEqual(records.read(records.path(self.store.directory, job))[0]['status'], 'done')

    def test_failed_record_is_retried_after_commit(self):
        with patch('coordinator.records.write', side_effect=ValueError('bad record')):
            job = self.store.task_create('-1:2', 'Retry')
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM stale_records').fetchone()[0], 1)
        with patch('coordinator.sweeps.time.time', return_value=time.time() + 61):
            records.sweep(self.store)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM stale_records').fetchone()[0], 0)
        self.assertEqual(records.read(records.path(self.store.directory, job))[0]['status'], 'open')

    def test_render_sheds_links_and_iterations_before_text(self):
        job = self.store.task_create('-1:2', 'Big job')
        with self.store.db:
            self.store.db.execute('CREATE TABLE IF NOT EXISTS keep_links (id TEXT PRIMARY KEY, task INTEGER NOT NULL, locator TEXT NOT NULL, reason TEXT NOT NULL)')
            for index in range(1500):
                self.store.db.execute('INSERT INTO keep_links VALUES (?,?,?,?)',
                                      ('l%d' % index, job['id'], 'dir:/' + 'x' * 150 + str(index), 'r' * 40))
            for k in range(1, 4):
                self.store.db.execute("INSERT INTO task_iterations(task,k,opened,opened_by,reason) VALUES (?,?,?,?,?)",
                                      (job['id'], k, float(k), 'owner', 'z' * 2000))
        data, body = records.project(self.store, self.store.task_get(job['id']))
        self.assertTrue(all(len(row['reason'] or '') <= 280 for row in data['iterations']))
        text = records.render(data, dict(body, notes=[]))
        self.assertLessEqual(len(text.encode()), 16384)
        self.assertEqual(data['title'], 'Big job')
        self.assertEqual(body['ask'], 'Big job')
        self.assertTrue(data['links'])
        self.assertEqual(data['links'][-1]['locator'][-4:], '1499')

    def test_render_never_raises_with_every_field_large(self):
        data = dict(id=1, uid='u', chat='c', number=1, title='t' * 1000, status='open', finish=None, created=1.0, closed=None,
                    parent=None, blocked_by=list(range(3000)), follow_ups=list(range(3000)), pr=None, commit=None,
                    workers={'a': 1}, links=[{'locator': 'l' * 200, 'reason': 'r' * 200}] * 500, stage=None,
                    iterations=[dict(k=k, opened=float(k), closed=None, opened_by='owner', outcome=None, reason='z' * 280)
                                for k in range(500)], results_size={'gb': 2.5, 'over_2gb': True})
        text = records.render(data, dict(ask='a' * 1000, outcome='o' * 2000))
        self.assertLessEqual(len(text.encode()), 16384)
        self.assertIn('"k":499', text)

    def test_caps_only_change_record(self):
        with self.assertLogs('coordinator.records', level='WARNING') as captured:
            job = self.store.task_create('-1:2', 'a' * 1200, notes='\n'.join(['é' * 400] * 50))
        self.assertTrue(any('truncated' in entry for entry in captured.output))
        filename = records.path(self.store.directory, job)
        data, body = records.read(filename)
        self.assertLessEqual(len(filename.read_bytes()), 16384)
        self.assertLessEqual(len(body['ask']), 4000)
        self.assertEqual(len(self.store.task_get(job['id'])['title']), 1200)
        self.assertEqual(len(self.store.task_get(job['id'])['notes'].splitlines()), 50)

    def test_keep_save_records_reason_and_default_destination(self):
        job = self.store.task_create('-1:2', 'First')
        source = Path(self.temp.name) / 'proof.txt'
        source.write_text('proof')
        missing = control_api.call(self.store, 'keep.save', {'path': str(source), 'task': job['id']}, topic='-1:2')
        self.assertFalse(missing.ok)
        result = control_api.call(self.store, 'keep.save', {'path': str(source), 'task': job['id'],
                                  'reason': 'Owner asked for proof'}, topic='-1:2')
        self.assertTrue(result.ok, result.text)
        self.assertEqual(Path(result.data['locator'][4:]).read_text(), 'proof')
        data, _ = records.read(records.path(self.store.directory, job))
        self.assertEqual(data['links'], [{'locator': result.data['locator'], 'reason': 'Owner asked for proof'}])
        self.assertEqual(source.read_text(), 'proof')

    def test_keep_links_are_redacted_and_survive_restore(self):
        job = self.store.task_create('-1:2', 'First')
        source = Path(self.temp.name) / 'proof.txt'
        source.write_text('proof')
        secret = 'sk-' + 'a' * 40
        saved = control_api.call(self.store, 'keep.save', {'path': str(source), 'task': job['id'],
                                 'reason': 'token=' + secret}, topic='-1:2')
        self.assertTrue(saved.ok, saved.text)
        filename = records.path(self.store.directory, job)
        data, _ = records.read(filename)
        self.assertEqual(data['links'][0]['reason'], 'token=[redacted]')
        self.assertNotIn(secret, filename.read_text())
        self.assertEqual(records.check(self.store)['count'], 0)
        records.write_chats(self.store)
        restored = Store(Path(self.temp.name) / 'restored')
        self.addCleanup(restored.close)
        records.restore(restored, self.store.directory / 'records')
        records.sweep(restored)
        self.assertEqual(records.read(records.path(restored.directory, job))[0]['links'], data['links'])
        self.assertEqual(records.check(restored)['count'], 0)

    def test_keep_queue_retries_a_protected_snapshot(self):
        job = self.store.task_create('-1:2', 'First')
        source = Path(self.temp.name) / 'proof.txt'
        source.write_text('proof')
        target = Path(self.temp.name) / 'offline'
        result = control_api.call(self.store, 'keep.save', {'path': str(source), 'task': job['id'],
                                  'reason': 'Owner asked for proof', 'to': 'dir:' + str(target)}, topic='-1:2')
        self.assertEqual(result.state, 'queued')
        snapshot = self.store.directory / 'keep' / 'queue' / ('%s-%s' % (job['id'], result.data['queued']))
        self.assertEqual(snapshot.read_text(), 'proof')
        source.rename(source.with_name('later.txt'))
        target.mkdir()
        partial = target / (result.data['queued'] + '-proof.txt')
        partial.write_text('pro')
        [detail] = [row[0] for row in self.store.db.execute(
            "SELECT detail FROM problems WHERE area='records' AND code='keep-delivery-failed'")]
        self.assertRegex(detail, r'^item=%s failures=1 OSError: Destination directory is unavailable at '
                                 r'records\.py:\d+ deliver_keep < ' % result.data['queued'])
        records.retry_keep(self.store)
        self.assertEqual(partial.read_text(), 'pro')
        with self.store.db:
            self.store.db.execute('UPDATE sweep_failures SET retry=0')
        with patch.object(Store, '_migrate_task_uids', side_effect=AssertionError('unexpected migration')):
            records.retry_keep_in_thread(self.store.directory)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM keep_queue').fetchone()[0], 0)
        self.assertIsNone(self.store.db.execute('SELECT 1 FROM sweep_failures').fetchone())
        data, _ = records.read(records.path(self.store.directory, job))
        self.assertEqual(Path(data['links'][0]['locator'][4:]).read_text(), 'proof')

    def test_large_and_invalid_destination_refuse_with_clear_reason(self):
        job = self.store.task_create('-1:2', 'First')
        source = Path(self.temp.name) / 'large'
        with source.open('wb') as stream:
            stream.truncate(10 * 1024 * 1024 + 1)
        params = {'path': str(source), 'task': job['id'], 'reason': 'proof'}
        result = control_api.call(self.store, 'keep.save', params, topic='-1:2')
        self.assertFalse(result.ok)
        self.assertIn('large=true', result.text)
        result = control_api.call(self.store, 'keep.save', dict(params, to='unsupported:page'), topic='-1:2')
        self.assertFalse(result.ok)
        self.assertIn('absolute dir:', result.text)

    def test_week_old_queue_notifies_once_and_is_visible_in_status(self):
        from coordinator import controls
        job = self.store.task_create('-1:2', 'First')
        source = Path(self.temp.name) / 'proof.txt'
        source.write_text('proof')
        result = control_api.call(self.store, 'keep.save', {'path': str(source), 'task': job['id'],
                                  'reason': 'Owner asked for proof', 'to': 'dir:' + str(source / 'offline')}, topic='-1:2')
        self.assertEqual(result.state, 'queued')
        with self.store.db:
            self.store.db.execute('UPDATE keep_queue SET created=?', (time.time() - 8 * 86400,))
        self.assertIn('Keep queue: 1', controls._status_card(self.store, '-1:2'))
        records.retry_keep(self.store)
        records.retry_keep(self.store)
        count = self.store.db.execute("SELECT COUNT(*) FROM outbox WHERE text LIKE '%has waited 7 days%'").fetchone()[0]
        self.assertEqual(count, 1)
        notice = self.store.db.execute("SELECT text FROM outbox WHERE text LIKE '%has waited 7 days%'").fetchone()[0]
        self.assertIn('keep.drop id=' + result.data['queued'], notice)

    def test_queue_follows_corrected_default_and_can_be_dropped(self):
        from coordinator import close_saves
        job = self.store.task_create('-1:2', 'First')
        root = close_saves.scratch(self.store, job['id'], create=True)
        source = root / 'proof'
        source.write_text('proof')
        missing = Path(self.temp.name) / 'missing'
        setting = control_api.call(self.store, 'keep.destination', {'to': 'dir:' + str(missing)}, topic='-1:2', source='telegram')
        self.assertFalse(setting.ok)
        old = Path(self.temp.name) / 'old'
        old.mkdir()
        self.assertTrue(control_api.call(self.store, 'keep.destination', {'to': 'dir:' + str(old)}, topic='-1:2', source='telegram').ok)
        old.rename(missing)
        saved = control_api.call(self.store, 'keep.save', {'path': str(source), 'reason': 'proof'}, topic='-1:2')
        self.assertEqual(saved.state, 'queued')
        self.assertFalse(close_saves.kept(self.store, job['id']))
        fixed = Path(self.temp.name) / 'fixed'
        fixed.mkdir()
        self.assertTrue(control_api.call(self.store, 'keep.destination', {'to': 'dir:' + str(fixed)}, topic='-1:2', source='telegram').ok)
        with self.store.db:
            self.store.db.execute('UPDATE sweep_failures SET retry=0')
        records.retry_keep(self.store)
        self.assertEqual(len(list(fixed.iterdir())), 1)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM keep_queue').fetchone()[0], 0)
        saved = control_api.call(self.store, 'keep.save', {'path': str(source), 'reason': 'proof', 'to': 'dir:' + str(old)}, topic='-1:2')
        with self.store.db:
            self.store.db.execute('UPDATE sweep_failures SET retry=0')
        records.retry_keep(self.store)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM keep_queue').fetchone()[0], 1)
        with patch.object(records, 'QUEUE_COUNT_LIMIT', 1):
            refused = control_api.call(self.store, 'keep.save', {'path': str(source), 'reason': 'proof'}, topic='-1:2')
            self.assertFalse(refused.ok)
            self.assertIn('queue is full', refused.text)
        self.assertTrue(control_api.call(self.store, 'keep.drop', {'id': saved.data['queued']}, topic='-1:2').ok)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM keep_queue').fetchone()[0], 0)

    def test_project_default_and_byte_limit(self):
        project = Path(self.temp.name) / 'project'
        project.mkdir()
        with self.store.db:
            self.store.db.execute('UPDATE topics SET cwd=? WHERE id=?', (str(project), '-1:2'))
        self.store.task_create('-1:2', 'First')
        source = Path(self.temp.name) / 'proof'
        source.write_text('proof')
        with patch.object(records, 'QUEUE_BYTES_LIMIT', 4):
            result = control_api.call(self.store, 'keep.save', {'path': str(source), 'reason': 'proof'}, topic='-1:2')
            self.assertFalse(result.ok)
        result = control_api.call(self.store, 'keep.save', {'path': str(source), 'reason': 'proof'}, topic='-1:2')
        self.assertTrue(result.ok)
        self.assertEqual(Path(result.data['locator'][4:]).parent, project / 'KEEP')

    def test_project_keep_manifest_records_delivery(self):
        project = Path(self.temp.name) / 'project'
        project.mkdir()
        with self.store.db:
            self.store.db.execute('UPDATE topics SET cwd=? WHERE id=?', (str(project), '-1:2'))
        job = self.store.task_create('-1:2', 'First')
        source = Path(self.temp.name) / 'proof.txt'
        source.write_text('proof')
        result = control_api.call(self.store, 'keep.save', {'path': str(source), 'reason': 'Owner proof',
                                  'task': job['id']}, topic='-1:2')
        self.assertTrue(result.ok, result.text)
        line = (project / 'KEEP' / 'MANIFEST.md').read_text().splitlines()
        self.assertEqual(len(line), 1)
        self.assertIn(Path(result.data['locator'][4:]).name, line[0])
        self.assertIn('Owner proof', line[0])
        self.assertIn('job %s' % job['id'], line[0])
        self.assertIn(str(source), line[0])
        self.assertIn(records.datetime.now(records.timezone.utc).date().isoformat(), line[0])

    def test_retry_cleans_only_unreferenced_queue_copies(self):
        job = self.store.task_create('-1:2', 'First')
        source = Path(self.temp.name) / 'proof'
        source.write_text('proof')
        result = control_api.call(self.store, 'keep.save', {'path': str(source), 'reason': 'proof',
                                  'to': 'dir:' + str(source / 'offline')}, topic='-1:2')
        folder = self.store.directory / 'keep' / 'queue'
        live = folder / ('%s-%s' % (job['id'], result.data['queued']))
        stray = folder / ('2-' + 'a' * 32)
        temporary = folder / ('.' + 'b' * 32)
        other = folder / 'owner-file'
        for item in (stray, temporary, other):
            item.write_text('proof')
        records.retry_keep(self.store)
        self.assertTrue(live.exists())
        self.assertTrue(other.exists())
        self.assertFalse(stray.exists())
        self.assertFalse(temporary.exists())

    def test_scratch_clear_recovers_after_process_death(self):
        from coordinator import close_saves
        path = close_saves.scratch(self.store, topic='-1:2', create=True)
        (path / 'file').write_text('proof')
        code = """import asyncio, os, sys
from pathlib import Path
from coordinator import close_saves, workspaces
from coordinator.store import Store
async def die(target, **options):
    os._exit(9)
workspaces.delete_tree = die
asyncio.run(close_saves.clear_chat_scratch(Store(Path(sys.argv[1])), '-1:2'))
"""
        done = subprocess.run([os.sys.executable, '-c', code, self.temp.name], capture_output=True)
        self.assertEqual(done.returncode, 9)
        asyncio.run(close_saves.clear_chat_scratch(self.store, '-1:2'))
        self.assertFalse(path.exists())
        self.assertFalse(list(path.parent.glob('*.deleting-*')))

    def test_outcome_is_the_last_worker_result_and_is_capped(self):
        job = self.store.task_create('-1:2', 'First')
        with self.store.db:
            worker = self.store.db.execute("""INSERT INTO workers(task,topic,provider,prompt,created,updated)
                VALUES (?,?,'codex','work',1,1)""", (job['id'], job['topic'])).lastrowid
            self.store.db.execute("""INSERT INTO messages(topic,kind,text,source_worker,created,updated)
                VALUES (?,'worker_result',?,?,1,1)""", (job['topic'], json.dumps({'text': 'result' * 834}), worker))
        self.store.task_update(job['id'], status='done')
        data, body = records.read(records.path(self.store.directory, job))
        self.assertTrue(body['outcome'].endswith('…[+1004 chars]'))
        self.assertEqual(data['workers'], {'codex': 1})
        self.assertEqual(records.check(self.store)['count'], 0)

    def test_check_detects_difference_and_backfill_is_idempotent(self):
        job = self.store.task_create('-1:2', 'First')
        filename = records.path(self.store.directory, job)
        records.backfill(self.store)
        before = filename.read_bytes()
        records.backfill(self.store)
        self.assertEqual(filename.read_bytes(), before)
        data, body = records.read(filename)
        data['status'] = 'dropped'
        filename.write_text(records.render(data, body))
        result = records.check(self.store)
        self.assertEqual(result['count'], 1)
        self.assertIn('status', result['differences'][0]['fields'])

    def test_clone_restore_preserves_records_and_interrupts_open_jobs(self):
        job = self.store.task_create('-1:2', 'First')
        self.store.task_update(job['id'], 'cli', note='first note')
        self.store.task_update(job['id'], 'cli', note='second note')
        self.store.task_update(job['id'], notes='replacement')
        self.assertEqual(texts(self.store, job), ['first note', 'second note'])
        self.assertNotIn('replacement', records.path(self.store.directory, job).read_text())
        with self.store.db:
            self.store.db.execute("UPDATE task_iterations SET closed=200,outcome='done' WHERE task=?", (job['id'],))
            self.store.db.execute("INSERT INTO task_iterations(task,k,opened,opened_by) VALUES (?,1,300,'owner')", (job['id'],))
            self.store.db.execute('UPDATE tasks SET iteration=1 WHERE id=?', (job['id'],))
        closed = self.store.task_create('-1:2', 'Finished', parent=job['id'])
        self.store.task_update(closed['id'], status='done')
        records.backfill(self.store)
        source = self.store.directory / 'records'
        subprocess.run(['git', 'init', '-q', str(source)], check=True)
        subprocess.run(['git', '-C', str(source), 'add', '.'], check=True)
        subprocess.run(['git', '-C', str(source), '-c', 'user.name=Test', '-c', 'user.email=test@example.invalid',
                        'commit', '-qm', 'records'], check=True)
        clone = Path(self.temp.name) / 'clone'
        subprocess.run(['git', 'clone', '-q', str(source), str(clone)], check=True)
        recovered = Store(Path(self.temp.name) / 'restored')
        self.addCleanup(recovered.close)
        self.assertEqual(records.restore(recovered, clone), {'records': 2, 'topics': 1})
        self.assertEqual(recovered.task_get(job['id'])['status'], 'interrupted')
        self.assertEqual(recovered.task_get(closed['id'])['parent'], job['id'])
        self.assertEqual(recovered.topic('-1:2')['enabled'], 0)
        self.assertEqual(recovered.task_iterations(closed['id'])[0]['outcome'], 'done')
        self.assertEqual([row['k'] for row in recovered.task_iterations(job['id'])], [0, 1])
        self.assertEqual(recovered.task_get(job['id'])['notes'], 'first note\nsecond note')
        self.assertEqual(recovered.db.execute('SELECT COUNT(*) FROM messages').fetchone()[0], 0)
        self.assertEqual(records.check(recovered)['count'], 0, records.check(recovered))

    def test_torn_journal_line_is_skipped_and_next_note_starts_a_new_line(self):
        job = self.store.task_create('-1:2', 'First')
        self.store.task_update(job['id'], 'cli', note='first note')
        target = records.journal_path(self.store.directory, job)
        with target.open('a') as stream:
            stream.write('{"at":"2026-11-11T00:00:00Z","by":"cli","kind":"note","te')
        self.assertEqual(texts(self.store, job), ['first note'])
        self.store.task_update(job['id'], 'cli', note='second note')
        self.assertEqual(texts(self.store, job), ['first note', 'second note'])
        self.assertEqual(len(target.read_text().splitlines()), 3)

    def test_reviewer_cases_preserve_prose(self):
        sha = 'f74ee5dd5f7ebff280a84e56c1ab976406047f7c'
        job = self.store.task_create('-1:2', 'Key commit: ' + sha)
        for line in ('x' * 265 + ' token=' + 'aB7' * 20, 'We checked the key', 'result: everything-passed'):
            self.store.task_update(job['id'], 'cli', note=line)
        data, body = records.read(records.path(self.store.directory, job))
        self.assertEqual(data['title'], 'Key commit: ' + sha)
        self.assertEqual(texts(self.store, job)[1:], ['We checked the key', 'result: everything-passed'])
        self.assertTrue(texts(self.store, job)[0].endswith('token=[redacted]'))
        records.write_chats(self.store)
        self.assertEqual(records.sync(self.store.directory)['blocked'], [])

    def test_high_confidence_rules_and_individual_notes(self):
        sha = 'f74ee5dd5f7ebff280a84e56c1ab976406047f7c'
        prose = 'Key commit: ' + sha + '\nRun basic checks\nKeyError: missing-field-name'
        self.assertEqual(records.redact(prose), (prose, 0))
        secret = 'aB7' * 12
        self.assertEqual(records.redact('token=' + secret)[0], 'token=[redacted]')
        self.assertEqual(records.redact('token=ordinary')[1], 0)
        self.assertEqual(records.redact('bearer aaaaaaaaaaaaaaaaaaaaa')[1], 0)
        self.assertEqual(records.redact('bearer internationalization')[1], 0)
        self.assertEqual(records.redact('bearer abcdefghijklmnopqrstuvwxyz')[1], 1)
        self.assertEqual(records.redact('bearer ' + secret)[1], 1)

    @unittest.skipUnless(shutil.which('gitleaks'), 'gitleaks required')
    def test_rules_names_only_assignment_commits_through_both_scanners(self):
        root = self.store.directory / 'records'
        rules = root / 'rules'
        rules.mkdir(parents=True, exist_ok=True)
        name = 'rules/chat-0123456789abcdef.md'
        example = ('Declare secrets by name (e.g. SERVICE_API_TOKEN=SOME_VAULT_NAME).\n'
                   'Use (SERVICE_API_TOKEN=WORK_SERVICE_ACCOUNT_TOKEN_NAME).\n')
        (root / name).write_text(example)
        result = records.sync(self.store.directory)
        self.assertEqual(result['blocked'], [])
        self.assertEqual(result['commits'], 1)
        committed = subprocess.check_output(['git', '-C', str(root), 'show', 'HEAD:' + name], text=True)
        self.assertTrue(committed == example)

    def test_rules_real_token_still_blocks_the_commit(self):
        root = self.store.directory / 'records'
        (root / 'rules').mkdir(parents=True, exist_ok=True)
        name = 'rules/chat-0123456789abcdef.md'
        (root / name).write_text('SERVICE_API_TOKEN=' + fake_pat() + '\n')
        result = records.sync(self.store.directory)
        self.assertEqual(result['blocked'], ['rules/chat-0123456789abcdef'])
        self.assertEqual(result['commits'], 0)

    def test_rules_name_exception_keeps_tokens_and_other_files_blocked(self):
        example = 'SERVICE_API_TOKEN=WORK_SERVICE_ACCOUNT_TOKEN_NAME'
        rule = 'rules/chat-0123456789abcdef.md'
        self.assertFalse(records.scan_content(example, rule))
        self.assertTrue(records.scan_content(example, 'chat-0123456789abcdef/0001.md'))
        for secret in (fake_pat(), 'AKIA' + 'A' * 16):
            text = 'SERVICE_API_TOKEN=' + secret
            self.assertTrue(records.scan_content(text, rule))

    @unittest.skipUnless(shutil.which('gitleaks'), 'gitleaks required')
    def test_gitleaks_still_blocks_a_real_token_in_rules(self):
        root = self.store.directory / 'records'
        (root / 'rules').mkdir(parents=True, exist_ok=True)
        (root / 'rules' / 'chat-0123456789abcdef.md').write_text('API_TOKEN=' + fake_pat() + '\n')
        with patch.object(records, 'scan_content', return_value=False):
            result = records.sync(self.store.directory)
        self.assertEqual(result['blocked'], ['rules/chat-0123456789abcdef'])
        self.assertEqual(result['commits'], 0)

    def test_generic_gitleaks_prose_exception_does_not_hide_token(self):
        def report(line):
            return json.dumps([{'RuleID': 'generic-api-key', 'File': 'record.md',
                                'StartLine': 1, 'StartColumn': 1, 'EndColumn': len(line)}])
        prose = 'Key facts: ordinary-english-description'
        self.assertIsNone(records.gitleaks_blocked(report(prose), lambda finding: prose))
        sensitive = 'Key facts: ghp_' + 'Ab7' * 12
        self.assertEqual(records.gitleaks_blocked(report(sensitive), lambda finding: sensitive), 'record.md')
        with self.assertRaises(RuntimeError):
            records.gitleaks_blocked('[]', lambda finding: '')

    def test_hex_credentials_remain_redacted(self):
        keys = ('API_KEY', 'github_token')
        values = ('0123456789abcdef' * 4, '0123456789abcdef' * 2 + '01234567')
        for key, value in zip(keys, values):
            with self.subTest(key=key):
                self.assertEqual(records.redact(key + ': ' + value)[1], 1)

    def test_known_token_formats_are_redacted(self):
        values = ['sk-' + 'a' * 20, 'ghp_' + 'a' * 20, 'gho_' + 'a' * 20,
                  'github_pat_' + 'a' * 20, 'xoxb-' + 'a' * 20, 'AKIA' + 'A' * 16,
                  'AIza' + 'a' * 30, 'glpat-' + 'a' * 20,
                  'eyJ' + 'a' * 10 + '.' + 'a' * 10 + '.' + 'a' * 10,
                  '-----BEGIN PRIVATE KEY-----\nsynthetic\n-----END PRIVATE KEY-----']
        for index, value in enumerate(values):
            with self.subTest(format=index):
                self.assertEqual(records.redact(value), ('[redacted]', 1))

    def test_large_commit_does_not_starve_event_loop(self):
        job = self.store.task_create('-1:2', 'First')
        records.journal_append(self.store, job, [{'at': 'x', 'by': 'cli', 'kind': 'note', 'text': 'ordinary safe text ' * 40}] * 6000,
                               limit=None)
        async def run():
            task = asyncio.create_task(records.sync_async(self.store.directory))
            largest = 0
            ticks = 0
            while not task.done():
                start = time.monotonic()
                await asyncio.sleep(0.01)
                largest = max(largest, time.monotonic() - start)
                ticks += 1
            self.assertEqual((await task)['commits'], 1)
            self.assertGreater(ticks, 5)
            self.assertLess(largest, 0.3)
        asyncio.run(run())

    @unittest.skipUnless(shutil.which('gitleaks'), 'gitleaks required')
    def test_gitleaks_blocks_only_the_job_with_a_finding(self):
        clean = self.store.task_create('-1:2', 'First')
        self.store.task_update(clean['id'], 'cli', note='first')
        records.write_chats(self.store)
        self.assertEqual(records.sync(self.store.directory)['commits'], 1)
        dirty = self.store.task_create('-1:2', 'Second')
        self.store.task_update(clean['id'], 'cli', note='second')
        secret = fake_pat()
        with records.journal_path(self.store.directory, dirty).open('a') as stream:
            stream.write(json.dumps({'at': 'x', 'by': 'cli', 'kind': 'note', 'text': 'GITHUB=' + secret}) + '\n')
        with patch('coordinator.records.scan_content', return_value=False):
            result = records.sync(self.store.directory)
        self.assertEqual(result['blocked'], [str(records.path(self.store.directory, dirty).with_suffix('').relative_to(
            self.store.directory / 'records'))])
        self.assertEqual(result['commits'], 1)
        log = subprocess.check_output(['git', '-C', str(self.store.directory / 'records'), 'log', '-p'], text=True)
        self.assertNotIn(secret, log)

    @unittest.skipUnless(shutil.which('gitleaks'), 'gitleaks required')
    def test_record_redacts_secret_before_writing_and_commits(self):
        token = 'sk-' + 'a' * 40
        job = self.store.task_create('-1:2', 'Protect records')
        self.store.task_update(job['id'], 'cli', note='api_key=' + token)
        self.store.task_update(job['id'], 'cli', note='key=' + 'aB7' * 16)
        filename = records.path(self.store.directory, job)
        self.assertEqual(texts(self.store, job), ['api_key=[redacted]', 'key=' + 'aB7' * 16])
        self.assertNotIn(token, filename.with_suffix('.jsonl').read_text())
        self.assertEqual(records.check(self.store)['count'], 0)
        records.write_chats(self.store)
        result = records.sync(self.store.directory)
        self.assertEqual(result['blocked'], [])
        self.assertTrue(result['sha'])

    def test_closed_redacted_record_and_restore(self):
        token = 'sk-' + 'a' * 40
        job = self.store.task_create('-1:2', 'x' * 295 + token)
        self.store.task_update(job['id'], 'cli', status='done', note='api_key=' + token)
        data, body = records.read(records.path(self.store.directory, job))
        self.assertEqual(data['status'], 'done')
        self.assertEqual(records.check(self.store)['count'], 0)
        records.write_chats(self.store)
        recovered = Store(Path(self.temp.name) / 'closed-restored')
        self.addCleanup(recovered.close)
        records.restore(recovered, self.store.directory / 'records')
        self.assertEqual(recovered.task_get(job['id'])['notes'], 'api_key=[redacted]')
        records.sweep(recovered)
        self.assertEqual(records.check(recovered)['count'], 0)

class TaskLinkTests(RecordCase):
    def test_links_cycles_ready_and_replacement_event(self):
        from coordinator import control_api
        first = self.store.task_create('-1:2', 'First')
        second = self.store.task_create('-1:2', 'Second')
        def link(task, kind, target):
            return control_api.call(self.store, 'tasks.link', {'task': task, 'kind': kind, 'to': target})
        self.assertTrue(link(first['id'], 'related', second['id']).ok)
        self.assertIn(first['id'], [row['id'] for row in control_api.call(self.store, 'followup.ready', {}).data])
        self.assertEqual(len(control_api.call(self.store, 'tasks.get', {'task': second['id']}).data['links']), 1)
        self.assertFalse(link(first['id'], 'related', first['id']).ok)
        self.assertTrue(link(first['id'], 'blocked_by', second['id']).ok)
        self.assertFalse(link(second['id'], 'blocked_by', first['id']).ok)
        self.assertTrue(link(first['id'], 'replaces', second['id']).ok)
        self.store.task_update(first['id'], status='done')
        self.store.task_update(first['id'], status='done')
        events = self.store.db.execute("SELECT text FROM messages WHERE kind='event' AND topic=?", ('-1:2',)).fetchall()
        self.assertEqual(len(events), 1)
        self.assertIn(str(second['id']), events[0][0])
        data, _ = records.read(records.path(self.store.directory, first))
        self.assertEqual(data['replaces'], [second['id']])


class FollowupTests(RecordCase):
    def test_proposal_lifecycle_and_records(self):
        from coordinator import control_api
        parent = self.store.task_create('-1:2', 'Parent')
        blocker = self.store.task_create('-1:2', 'Blocker')
        result = control_api.call(self.store, 'followup.propose',
                                  {'parent': parent['id'], 'title': 'Later', 'why': 'Needed', 'blocked_by': blocker['id']})
        self.assertTrue(result.ok, result.text)
        item = result.data
        records.sweep(self.store)
        data, body = records.read(records.path(self.store.directory, self.store.task_get(item['id'])))
        self.assertEqual((data['status'], data['parent'], data['blocked_by'], texts(self.store, item)),
                         ('proposed', parent['id'], [blocker['id']], ['Needed']))
        self.assertEqual(self.store.task_get(item['id'])['notes'], 'Needed')
        self.assertEqual(records.read(records.path(self.store.directory, parent))[0]['follow_ups'], [item['id']])
        refused = control_api.call(self.store, 'workers.spawn', {'task': item['id'], 'provider': 'codex', 'prompt': 'Do it'})
        self.assertFalse(refused.ok)
        self.assertTrue(control_api.call(self.store, 'tasks.update', {'task': item['id'], 'status': 'open'}).ok)
        ready = control_api.call(self.store, 'followup.ready', {'topic': '-1:2'}).data
        self.assertNotIn(item['id'], [row['id'] for row in ready])
        self.store.task_update(blocker['id'], status='done')
        ready = control_api.call(self.store, 'followup.ready', {'topic': '-1:2'}).data
        self.assertIn(item['id'], [row['id'] for row in ready])
        self.assertEqual(records.check(self.store)['count'], 0)

    def test_ready_filters_by_project_across_chats(self):
        with self.store.db:
            self.store.db.execute("UPDATE topics SET cwd='/projects/shared' WHERE id='-1:2'")
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('-1:3',-1,3,'Other','/projects/shared')")
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('-1:4',-1,4,'Else','/projects/elsewhere')")
        first = self.store.task_create('-1:2', 'First')
        second = self.store.task_create('-1:3', 'Second')
        third = self.store.task_create('-1:4', 'Third')
        result = control_api.call(self.store, 'followup.ready', {'project': 'shared'})
        self.assertTrue(result.ok, result.text)
        self.assertEqual({row['id'] for row in result.data}, {first['id'], second['id']})
        self.assertNotIn(third['id'], {row['id'] for row in result.data})

    def test_ready_prioritizes_chat_and_excludes_live_and_stale_proposals(self):
        from coordinator import control_api
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('-2:3',-2,3,'Other','')")
        other = self.store.task_create('-2:3', 'Other')
        parent = self.store.task_create('-1:2', 'Here')
        item = control_api.call(self.store, 'followup.propose', {'parent': parent['id'], 'title': 'Later', 'why': 'Reason'}).data
        with self.store.db:
            self.store.db.execute("INSERT INTO workers(task,topic,provider,prompt,created,updated,status) VALUES (?,?,'codex','do',1,1,'queued')", (parent['id'], parent['topic']))
            self.store.db.execute("UPDATE tasks SET updated=1 WHERE id=?", (item['id'],))
        ready = control_api.call(self.store, 'followup.ready', {'topic': '-1:2'}).data
        self.assertEqual([row['id'] for row in ready], [item['id'], other['id']])
        stale = control_api.call(self.store, 'tasks.stale', {'hours': 1}).data['tasks']
        self.assertNotIn(item['id'], [row['id'] for row in stale])

    def test_proposal_limit_and_chat_scope(self):
        from coordinator import control_api
        parent = self.store.task_create('-1:2', 'Parent')
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('-2:3',-2,3,'Other','')")
        other = self.store.task_create('-2:3', 'Other')
        result = control_api.call(self.store, 'followup.propose', {'parent': other['id'], 'title': 'X', 'why': 'Y'}, topic='-1:2', source='mcp')
        self.assertFalse(result.ok)
        items = []
        for index in range(50):
            result = control_api.call(self.store, 'followup.propose', {'parent': parent['id'], 'title': str(index), 'why': 'Y'})
            self.assertTrue(result.ok, result.text)
            items.append(result.data)
        self.store.task_update(items[0]['id'], status='open')
        self.store.task_create('-1:2', 'Child', parent=items[1]['id'])
        other_item = control_api.call(self.store, 'followup.propose', {'parent': other['id'], 'title': 'Other', 'why': 'Y'}).data
        control_api.call(self.store, 'followup.propose', {'parent': parent['id'], 'title': 'Fill', 'why': 'Y'})
        result = control_api.call(self.store, 'followup.propose', {'parent': parent['id'], 'title': 'Extra', 'why': 'Y'})
        self.assertTrue(result.ok, result.text)
        self.assertEqual(self.store.task_get(items[0]['id'])['status'], 'open')
        self.assertEqual(self.store.task_get(items[1]['id'])['status'], 'proposed')
        dropped = self.store.task_get(items[2]['id'])
        self.assertEqual(dropped['status'], 'dropped')
        self.assertIn('Dropped to make room', dropped['notes'])
        _, body = records.read(records.path(self.store.directory, dropped))
        self.assertEqual(body['outcome'], 'Dropped to make room for a new follow-up proposal.')
        self.assertEqual(self.store.task_get(other_item['id'])['status'], 'proposed')
        count = self.store.db.execute("SELECT COUNT(*) FROM tasks WHERE topic=? AND status='proposed'", ('-1:2',)).fetchone()[0]
        self.assertEqual(count, 50)
        refused = control_api.call(self.store, 'followup.propose', {'parent': parent['id'], 'title': '', 'why': 'Y'})
        self.assertFalse(refused.ok)
        self.assertEqual(self.store.task_get(items[3]['id'])['status'], 'proposed')

    def test_restore_preserves_followup_blockers(self):
        parent = self.store.task_create('-1:2', 'Parent')
        blocker = self.store.task_create('-1:2', 'Blocker')
        item = control_api.call(self.store, 'followup.propose',
                                {'parent': parent['id'], 'title': 'Later', 'why': 'Needed', 'blocked_by': blocker['id']}).data
        records.backfill(self.store)
        source = self.store.directory / 'records'
        subprocess.run(['git', 'init', '-q', str(source)], check=True)
        subprocess.run(['git', '-C', str(source), 'add', '.'], check=True)
        subprocess.run(['git', '-C', str(source), '-c', 'user.name=T', '-c', 'user.email=t@example.invalid', 'commit', '-qm', 'r'], check=True)
        clone = Path(self.temp.name) / 'clone'
        subprocess.run(['git', 'clone', '-q', str(source), str(clone)], check=True)
        recovered = Store(Path(self.temp.name) / 'restored')
        self.addCleanup(recovered.close)
        records.restore(recovered, clone)
        self.assertEqual(recovered.task_get(item['id'])['blocked_by'], [blocker['id']])
        self.assertEqual(records.check(recovered)['count'], 0)

    def test_restore_preserves_all_link_kinds_across_chats(self):
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('-1:3',-1,3,'Other','')")
        first = self.store.task_create('-1:2', 'First')
        second = self.store.task_create('-1:3', 'Second')
        for kind in ('blocked_by', 'discovered_from', 'related', 'replaces'):
            self.store.task_link(first['id'], second['id'], kind)
        self.store.task_note(first['id'], 'Round-trip journal entry', blob=False)
        links = control_api.call(self.store, 'tasks.get', {'task': second['id']}).data['links']
        self.assertEqual({row['kind'] for row in links}, {'blocked_by', 'discovered_from', 'related', 'replaces'})
        self.store.task_update(first['id'], status='done')
        events = self.store.db.execute("SELECT topic,text FROM messages WHERE kind='event' ORDER BY topic").fetchall()
        self.assertEqual([row['topic'] for row in events], ['-1:2', '-1:3'])
        self.assertTrue(all(str(second['id']) in row['text'] for row in events))
        records.backfill(self.store)
        recovered = Store(Path(self.temp.name) / 'restored-links')
        self.addCleanup(recovered.close)
        records.restore(recovered, self.store.directory / 'records')
        rows = recovered.db.execute('SELECT task,target,kind FROM task_links ORDER BY kind').fetchall()
        self.assertEqual([(row[0], row[1], row[2]) for row in rows],
                         [(first['id'], second['id'], kind) for kind in ('blocked_by', 'discovered_from', 'related', 'replaces')])
        self.assertEqual(texts(recovered, recovered.task_get(first['id'])), ['Round-trip journal entry'])
        self.assertEqual(records.check(recovered)['count'], 0)

    def test_old_records_default_new_link_fields(self):
        job = self.store.task_create('-1:2', 'Old')
        filename = records.path(self.store.directory, job)
        filename.write_text('\n'.join(line for line in filename.read_text().split('\n')
                                      if not line.startswith(('discovered_from: ', 'related: ', 'replaces: '))))
        data, _ = records.read(filename)
        self.assertEqual([data[key] for key in ('discovered_from', 'related', 'replaces')], [[], [], []])

    def test_proposal_cannot_wait(self):
        parent = self.store.task_create('-1:2', 'Parent')
        item = control_api.call(self.store, 'followup.propose', {'parent': parent['id'], 'title': 'Later', 'why': 'Y'}).data
        result = control_api.call(self.store, 'tasks.wait',
                                  {'task': item['id'], 'kind': 'time', 'ask': 'Later', 'at': '2030-01-01T00:00:00+00:00'})
        self.assertFalse(result.ok)
        self.assertEqual(self.store.task_get(item['id'])['status'], 'proposed')


class JournalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        environment = {key: value for key, value in os.environ.items()
                       if key not in ('TORII_WORKER_ID', 'TORII_TASK_ID')}
        environment['HOME'] = self.temp.name
        env_patch = patch.dict(os.environ, environment, clear=True)
        env_patch.start()
        self.addCleanup(env_patch.stop)
        self.store = Store(Path(self.temp.name) / 'state')
        self.addCleanup(lambda: self.store.close())
        with self.store.db:
            self.store.put('owner', 7)
            self.store.put('group', -1)
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('-1:2',-1,2,'Test','',1)")
        self.job = self.store.task_create('-1:2', 'First')

    def note(self, text, **kwargs):
        return control_api.call(self.store, 'tasks.note', {'task': self.job['id'], 'text': text}, **kwargs)

    def test_twenty_processes_append_whole_lines_with_worker_author(self):
        worker = self.store.db.execute('''INSERT INTO workers(task,topic,provider,prompt,created,updated)
            VALUES (?,?,?,?,?,?)''', (self.job['id'], '-1:2', 'codex', 'Work', 1, 1)).lastrowid
        self.store.db.commit()
        env = dict(os.environ, TORII_WORKER_ID=str(worker), TORII_TASK_ID=str(self.job['id']),
                   PYTHONPATH=str(Path(__file__).resolve().parents[1]))
        command = [sys.executable, '-m', 'coordinator', '--state-dir', str(self.store.directory), 'ctl', 'call', 'tasks.note',
                   'task=%d' % self.job['id']]
        processes = [subprocess.Popen(command + ['text=note %02d ' % index + 'x' * 1500], env=env,
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE) for index in range(20)]
        for process in processes:
            output, error = process.communicate(timeout=120)
            self.assertEqual(process.returncode, 0, output + error)
        lines = records.journal_path(self.store.directory, self.job).read_text().splitlines()
        entries = [json.loads(line) for line in lines]
        self.assertEqual(sorted(entry['text'][:7] for entry in entries), ['note %02d' % index for index in range(20)])
        self.assertEqual({entry['by'] for entry in entries}, {'worker:%d' % worker})
        self.assertTrue(all(len(entry['text']) == 1508 for entry in entries))

    def test_authors_follow_the_caller(self):
        self.assertTrue(self.note('cli').ok)
        self.assertTrue(self.note('mcp', source='mcp', topic='-1:2').ok)
        self.assertTrue(self.note('program', from_name='Lane').ok)
        worker = self.store.db.execute('''INSERT INTO workers(task,topic,provider,prompt,created,updated)
            VALUES (?,?,?,?,?,?)''', (self.job['id'], '-1:2', 'codex', 'Work', 1, 1)).lastrowid
        self.assertTrue(self.note('worker', source_worker=worker).ok)
        self.assertTrue(control_api.call(self.store, 'tasks.update', {'task': self.job['id'], 'note': 'update'},
                                         source='mcp', topic='-1:2').ok)
        entries = records.journal(records.journal_path(self.store.directory, self.job))
        self.assertEqual([(entry['by'], entry['text']) for entry in entries],
                         [('cli', 'cli'), ('coordinator:Test', 'mcp'), ('program:Lane', 'program'),
                          ('worker:%d' % worker, 'worker'), ('coordinator:Test', 'update')])
        self.assertEqual(self.store.task_get(self.job['id'])['notes'], 'update')
        fetched = control_api.call(self.store, 'tasks.get', {'task': self.job['id']}).data
        self.assertEqual(fetched['journal'], str(records.journal_path(self.store.directory, self.job)))
        self.assertEqual([entry['text'] for entry in fetched['entries']], ['cli', 'mcp', 'program', 'worker', 'update'])

    def test_read_only_folder_fails_the_note_but_not_the_close(self):
        folder = records.path(self.store.directory, self.job).parent
        folder.chmod(0o500)
        self.addCleanup(folder.chmod, 0o700)
        result = self.note('kept nowhere')
        self.assertEqual((result.ok, result.state), (False, 'failed'))
        self.assertEqual(self.store.db.execute(
            "SELECT COUNT(*) FROM problems WHERE area='records' AND code='journal-failed' AND task=?",
            (self.job['id'],)).fetchone()[0], 1)
        self.assertFalse(records.journal_path(self.store.directory, self.job).exists())
        closed = control_api.call(self.store, 'tasks.update', {'task': self.job['id'], 'status': 'done', 'note': 'Verified',
                                                                 'summary': 'Verified.'})
        self.assertTrue(closed.ok, closed.text)
        self.assertEqual((self.store.task_get(self.job['id'])['status'], self.store.task_get(self.job['id'])['notes']),
                         ('done', 'Verified'))

    def test_planted_tokens_are_stored_redacted_with_a_count(self):
        secrets = Path(self.temp.name) / '.config' / 'torii-secrets'
        secrets.mkdir(parents=True)
        (secrets / 'vendor.token').write_text('plainvaluewithoutanyshape\n')
        token = 'sk-' + 'a' * 40
        with patch('coordinator.store._secret_homes', return_value={Path(self.temp.name)}):
            result = self.note('used %s and plainvaluewithoutanyshape' % token)
        self.assertTrue(result.ok, result.text)
        content = records.journal_path(self.store.directory, self.job).read_text()
        self.assertNotIn(token, content)
        self.assertEqual(json.loads(content)['text'], 'used [redacted] and [token]')
        self.assertEqual(result.data['redacted'], 2)
        self.assertIn('2 secret-like values were redacted', result.text)

    def test_note_length_limit(self):
        refused = self.note('x' * 2001)
        self.assertEqual((refused.ok, refused.state), (False, 'refused'))
        self.assertIn('results/', refused.text)
        self.assertFalse(records.journal_path(self.store.directory, self.job).exists())
        self.assertTrue(self.note('x' * 2000).ok)

    def test_full_journal_rolls_over_and_torii_notes_are_cut(self):
        target = records.journal_path(self.store.directory, self.job)
        entry = dict(at='x', by='migration', kind='note', text='')
        entry['text'] = 'x' * (records.JOURNAL_LIMIT - len(json.dumps(entry).encode()) - 1)
        target.write_text(json.dumps(entry) + '\n')
        result = self.note('next part')
        self.assertTrue(result.ok, result.text)
        part = target.with_name(target.stem + '.1.jsonl')
        self.assertEqual(result.data['journal'], str(part))
        self.assertEqual(target.stat().st_size, records.JOURNAL_LIMIT)
        self.store.task_note(self.job['id'], 'y' * 2500, 'coordinator:Test')
        clipped = 'y' * 2000 + '…[+500 chars]'
        self.assertEqual(texts(self.store, self.job), [entry['text'], 'next part', clipped])
        self.assertEqual(self.store.task_get(self.job['id'])['notes'], 'y' * 2500)
        fetched = control_api.call(self.store, 'tasks.get', {'task': self.job['id']}).data
        self.assertEqual(fetched['journal'], str(target))
        self.assertEqual(fetched['journal_parts'], [str(target), str(part)])
        self.assertEqual(fetched['entries'][-1]['text'], clipped)

    def test_torii_housekeeping_stays_out_of_the_journal(self):
        target = records.journal_path(self.store.directory, self.job)
        self.store.task_note(self.job['id'], 'Idle trim at 2026-11-21 freed 0 bytes of dependency, build and cache output.')
        self.store.task_note(self.job['id'], 'Worker 7 runs on Claude Opus 5.5.')
        self.assertFalse(target.exists())
        self.assertIn('Idle trim', self.store.task_get(self.job['id'])['notes'])
        parent = self.store.task_create('-1:2', 'Parent')
        propose = lambda title, why: control_api.call(self.store, 'followup.propose',
                                                      {'parent': parent['id'], 'title': title, 'why': why}).data
        old = propose('Old', 'Why old')
        for index in range(50):
            propose('New %d' % index, 'Why')
        self.assertEqual(self.store.task_get(old['id'])['status'], 'dropped')
        self.assertEqual(texts(self.store, old), ['Why old'])
        self.assertEqual(self.store.task_iterations(old['id'])[-1]['reason'],
                         'Dropped to make room for a new follow-up proposal.')
        self.assertFalse(target.exists())

    def test_journal_writers_are_on_the_allowlist(self):
        import ast
        allowed = {('control_api.py', 'op_tasks_create'), ('control_api.py', 'op_tasks_note'),
                   ('control_api.py', 'op_telegram_send'), ('store.py', '_task_update'),
                   ('records.py', 'op_propose'), ('records.py', 'record_result')}
        found = set()
        for source in sorted((Path(__file__).resolve().parents[1] / 'coordinator').rglob('*.py')):
            tree = ast.parse(source.read_text(encoding='utf-8'))
            for function in ast.walk(tree):
                if not isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                for call in ast.walk(function):
                    if not isinstance(call, ast.Call):
                        continue
                    name = getattr(call.func, 'attr', None) or getattr(call.func, 'id', None)
                    authored = name == 'task_note' and (len(call.args) > 2 or any(k.arg == 'by' for k in call.keywords))
                    if name == 'system_entry' or authored:
                        found.add((source.name, function.name))
        self.assertEqual(found - allowed, set(), 'A new journal writer must be an agent action, not Torii housekeeping.')

    def test_result_entry_keeps_only_the_final_report(self):
        with self.store.db:
            worker = self.store.db.execute('''INSERT INTO workers(task,topic,provider,prompt,created,updated)
                VALUES (?,?,?,?,?,?)''', (self.job['id'], '-1:2', 'claude', 'Work', 1, 1)).lastrowid
        text = ('Still running the gate.\n\nLater, after a background task finished (2 earlier notes left out): '
                'Gate passed. PR https://github.com/o/r/pull/9')
        self.store.worker_complete(worker, {'success': True, 'text': text})
        records.result_entries_in_thread(self.store.directory)
        self.assertEqual(texts(self.store, self.job), ['Gate passed. PR https://github.com/o/r/pull/9'])
        self.assertEqual(records.final_text('Only one report'), 'Only one report')

    def test_batch_rollover_keeps_whole_entries_in_numeric_order(self):
        target = records.journal_path(self.store.directory, self.job)
        entries = [dict(at='x', by='cli', kind='note', text='界%02d' % index) for index in range(12)]
        limit = len((json.dumps(entries[0], ensure_ascii=False) + '\n').encode())
        oversized = dict(at='x', by='cli', kind='result', text='x' * limit)
        last = records.journal_append(self.store, self.job, entries + [oversized, entries[0]], limit)
        self.assertEqual(last, target.with_name(target.stem + '.13.jsonl'))
        self.assertEqual(records.journal(target), entries + [oversized, entries[0]])
        self.assertEqual(records.journal(last, 4), entries[-2:] + [oversized, entries[0]])
        sizes = [part.stat().st_size for part in records.journal_parts(target)]
        self.assertEqual(sizes[:12] + sizes[13:], [limit] * 13)
        self.assertGreater(sizes[12], limit)

    def test_restore_and_backfill_use_rolled_notes_and_reports(self):
        target = records.journal_path(self.store.directory, self.job)
        entries = [dict(at='x', by='cli', kind=kind, text=text) for kind, text in
                   [('note', 'first'), ('note', 'second'), ('report', 'outcome'), ('note', 'third')]]
        records.journal_append(self.store, self.job, entries, limit=70)
        records.backfill(self.store)
        self.assertEqual(records.read(records.path(self.store.directory, self.job))[1]['outcome'], 'outcome')
        self.assertEqual(records.check(self.store)['count'], 0)
        restored = Store(self.store.directory.parent / 'restored')
        self.addCleanup(restored.close)
        records.restore(restored, self.store.directory / 'records')
        self.assertEqual(restored.task_get(self.job['id'])['notes'], 'first\nsecond\nthird')
        self.assertEqual(records.journal(records.journal_path(restored.directory, self.job)), entries)
        records.backfill(restored)
        self.assertEqual(records.check(restored)['count'], 0)
        self.assertGreater(len(records.journal_parts(target)), 1)

    def test_worker_results_only_include_new_keep_items_across_parts(self):
        old = {'path': 'dir:/old', 'reason': 'old proof'}
        new = {'path': 'dir:/new', 'reason': 'new proof'}
        target = records.journal_path(self.store.directory, self.job)
        records.journal_append(self.store, self.job, [dict(at='x', by='torii', kind='result', text='old result',
                                                          storage={'kept': [dict(old, bytes=10)]})])
        for folder, size in (('scratch', 1), ('worktrees', 2), ('results', 3)):
            directory = self.store.directory / folder / ('task-%d' % self.job['id'])
            directory.mkdir(parents=True, exist_ok=True)
            (directory / 'proof').write_bytes(b'x' * size)
        with self.store.db:
            for item in (old, new):
                self.store.db.execute('INSERT INTO keep_links VALUES (?,?,?,?)',
                                      (item['path'], self.job['id'], item['path'], item['reason']))
        append = records.journal_append
        for text in ('first result', 'second result'):
            with self.store.db:
                worker = self.store.db.execute('''INSERT INTO workers(task,topic,provider,prompt,created,updated)
                    VALUES (?,?,?,?,?,?)''', (self.job['id'], '-1:2', 'codex', 'Work', 1, 1)).lastrowid
            self.store.worker_complete(worker, {'success': True, 'text': text})
            with patch.object(records, 'journal_append', side_effect=lambda store, task, entries: append(store, task, entries, 1)):
                records.result_entries_in_thread(self.store.directory)
        entries = records.journal(target)
        self.assertEqual([entry['text'] for entry in entries], ['old result', 'first result', 'second result'])
        self.assertEqual(entries[0]['storage']['kept'], [dict(old, bytes=10)])
        sizes = dict(scratch=1, worktree=2, results=3, kept_count=2)
        self.assertEqual(entries[1]['storage'], dict(sizes, kept_added=[new]))
        self.assertEqual(entries[2]['storage'], sizes)
        self.assertEqual(len(records.journal_parts(target)), 3)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM worker_ledger_pending').fetchone()[0], 0)

    def test_old_notes_import_whole_once(self):
        lines = ['line %d ' % index + 'z' * 100 for index in range(600)] + ['w' * 100000]
        old = self.store.task_create('-1:2', 'Old', notes='\n'.join(lines))
        other = self.store.task_create('-1:2', 'Other', notes='earlier')
        self.assertGreater(len(old['notes']), 160000)
        self.store.task_update(other['id'], 'coordinator:Test', note='later')
        self.assertEqual([(entry['by'], entry['text']) for entry in records.journal(records.journal_path(self.store.directory, other))],
                         [('migration', 'earlier'), ('coordinator:Test', 'later')])
        self.assertEqual(records.import_all_notes(self.store, limit=1), len(lines))
        self.assertFalse(self.store.get('journal_import')['done'])
        target = records.journal_path(self.store.directory, old)
        self.assertEqual([entry['text'] for entry in records.journal(target)], lines)
        before = target.read_bytes()
        self.assertEqual(records.import_all_notes(self.store), 0)
        self.assertTrue(self.store.get('journal_import')['done'])
        self.assertEqual(records.import_all_notes(self.store), 0)
        with self.store.db:
            self.store.put('journal_import', {'cursor': 0})
        self.assertEqual(records.import_all_notes(self.store), 0)
        self.assertEqual(target.read_bytes(), before)
        self.assertEqual(len(records.journal(records.journal_path(self.store.directory, other))), 2)

    def test_old_notes_import_scrubs_outside_the_write_lock_and_resumes(self):
        jobs = [self.store.task_create('-1:2', 'Old %d' % index, notes='one %d\ntwo' % index) for index in range(3)]
        held = []
        redact = records.redact
        def watch(text, *args, **kwargs):
            held.append(self.store.db.in_transaction)
            return redact(text, *args, **kwargs)
        append = records.journal_append
        def fail_second(store, task, entries, limit=records.JOURNAL_LIMIT):
            if task['id'] == jobs[1]['id']:
                raise OSError('disk')
            return append(store, task, entries, limit)
        with patch.object(records, 'redact', watch), patch.object(records, 'journal_append', fail_second):
            self.assertEqual(records.import_all_notes(self.store), 4)
        self.assertEqual(held, [False] * 6)
        self.assertEqual(self.store.get('journal_import'), {'cursor': jobs[2]['id'], 'done': False})
        [detail] = [row[0] for row in self.store.db.execute(
            "SELECT detail FROM problems WHERE area='records' AND code='journal-import-failed' AND task=?", (jobs[1]['id'],))]
        self.assertRegex(detail, r'^item=%d failures=1 OSError: disk at test_records\.py:\d+ fail_second' % jobs[1]['id'])
        self.assertEqual(records.import_all_notes_in_thread(self.store.directory), 0)
        with patch('coordinator.sweeps.time.time', return_value=time.time() + 61):
            self.assertEqual(records.import_all_notes_in_thread(self.store.directory), 2)
        self.assertEqual(self.store.get('journal_import'), {'cursor': jobs[2]['id'], 'done': True})
        self.assertEqual([texts(self.store, job) for job in jobs], [['one 0', 'two'], ['one 1', 'two'], ['one 2', 'two']])

    def test_records_sweep_does_not_remind_about_unreported_job(self):
        from types import SimpleNamespace
        from coordinator.service import Service
        owner = self.store.message_save('-1:2', 'owner', 'Do work', telegram_message=1)
        job = self.store.task_create('-1:2', 'Work', origin=owner['id'])
        self.store.put('report_reminders_since', time.time() - 60)
        self.store.put('journal_import', {'done': True})
        self.store.task_update(job['id'], status='done')
        closed = self.store.task_iterations(job['id'])[-1]['closed']
        service = SimpleNamespace(store=self.store)
        with patch('coordinator.service.asyncio.sleep', AsyncMock(side_effect=[None, asyncio.CancelledError])), \
                patch('time.time', return_value=closed + 901):
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(Service.records_sweep_loop(service))
        self.assertIsNone(self.store.db.execute("SELECT id FROM messages WHERE kind='event'").fetchone())
        self.assertIsNone(self.store.pending_delivery())

    def test_service_imports_old_notes_in_a_worker_thread(self):
        import threading
        from coordinator.service import Service
        seen = []
        def fake(directory, vault):
            seen.append((threading.current_thread() is threading.main_thread(), Path(directory), vault))
        sleeps = []
        async def sleep(delay):
            sleeps.append(delay)
            if len(sleeps) > 2:
                raise asyncio.CancelledError
        service = type('Fake', (), {})()
        service.store, service.vault = self.store, 'vault'
        with patch.object(records, 'import_all_notes_in_thread', fake, create=True), patch.object(records, 'sweep'), \
                patch('coordinator.service.asyncio.sleep', sleep):
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(Service.records_sweep_loop(service))
            with self.store.db:
                self.store.put('journal_import', {'cursor': 9, 'done': True})
            sleeps.clear()
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(Service.records_sweep_loop(service))
        self.assertEqual(seen, [(False, self.store.directory, 'vault')] * 2)

    def test_record_rewrites_during_appends_lose_no_line(self):
        import threading
        target = records.path(self.store.directory, self.job)
        content = target.read_text()
        stop = threading.Event()
        def rewrite():
            while not stop.is_set():
                records.replace(target, content)
        thread = threading.Thread(target=rewrite)
        thread.start()
        try:
            for index in range(100):
                self.assertTrue(self.note('note %d' % index).ok)
        finally:
            stop.set()
            thread.join()
        self.assertEqual([entry['text'] for entry in records.journal(records.journal_path(self.store.directory, self.job))],
                         ['note %d' % index for index in range(100)])

    def test_reader_skips_unknown_kind_and_reads_extra_keys(self):
        target = records.journal_path(self.store.directory, self.job)
        unknown = json.dumps({'at': '2026-11-11T00:00:00Z', 'by': 'b/worker:3', 'kind': 'vote', 'text': 'later kind'})
        extra = json.dumps({'at': '2026-11-11T00:00:01Z', 'by': 'b/worker:3', 'kind': 'note', 'text': 'peer', 'via': 'b#1'})
        target.write_text(unknown + '\n' + extra + '\n')
        self.assertTrue(self.note('local').ok)
        self.assertEqual([entry['text'] for entry in records.journal(target)], ['peer', 'local'])
        self.assertEqual(records.journal(target)[0]['via'], 'b#1')
        self.assertEqual(target.read_text().splitlines()[:2], [unknown, extra])


class LedgerCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store(Path(self.temp.name) / 'state')
        self.addCleanup(lambda: self.store.close())
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('-1:2',-1,2,'Test','')")
        self.root = self.store.directory / 'records'

    def git(self, *args):
        return subprocess.run(['git', '-C', str(self.root), *args], check=True, capture_output=True, text=True).stdout

    def commits(self):
        return self.git('log', '--format=%s').splitlines()

    def files(self, revision='HEAD'):
        return self.git('show', '--name-only', '--format=', revision).split()

    def relative(self, job, suffix='.md'):
        return str(records.path(self.store.directory, job).with_suffix(suffix).relative_to(self.root))

    def tick(self):
        records.write_chats(self.store)
        return records.sync(self.store.directory)


class CommitterTests(LedgerCase):
    def test_first_run_is_one_import_commit_then_one_commit_per_job(self):
        first = self.store.task_create('-1:2', 'First')
        second = self.store.task_create('-1:2', 'Second')
        self.store.task_update(first['id'], 'cli', note='old')
        result = self.tick()
        self.assertEqual((result['commits'], self.commits()), (1, ['Import records']))
        self.assertIn(self.relative(first, '.jsonl'), self.files())
        self.store.task_update(first['id'], 'cli', title='First changed', note='proof at results/x')
        self.store.task_update(second['id'], status='done', stage='merged')
        result = self.tick()
        self.assertEqual(result['commits'], 2)
        subjects = self.commits()[:2]
        self.assertIn('job %d open: First changed' % first['id'], subjects)
        self.assertIn('job %d done merged: Second' % second['id'], subjects)
        for revision in ('HEAD', 'HEAD~1'):
            files = self.files(revision)
            job = first if 'First' in self.git('log', '-1', '--format=%s', revision) else second
            self.assertTrue(files and all(name.startswith(self.relative(job)[:-3]) for name in files), files)
        self.assertEqual(self.git('status', '--porcelain', '--', '*.md', '*.jsonl', 'chats.yaml'), '')

    def test_three_changes_in_one_tick_make_one_commit(self):
        job = self.store.task_create('-1:2', 'First')
        self.tick()
        self.store.task_update(job['id'], title='One')
        self.store.task_update(job['id'], 'cli', note='Two')
        self.store.task_update(job['id'], stage='ready')
        self.assertEqual(self.tick()['commits'], 1)
        self.assertEqual(sorted(self.files()), sorted([self.relative(job), self.relative(job, '.jsonl')]))
        self.assertEqual(self.tick()['commits'], 0)

    def test_planted_token_blocks_only_that_job(self):
        clean = self.store.task_create('-1:2', 'Clean')
        dirty = self.store.task_create('-1:2', 'Dirty')
        self.tick()
        self.store.task_update(clean['id'], title='Clean changed')
        with records.journal_path(self.store.directory, dirty).open('a') as stream:
            stream.write(json.dumps({'at': 'x', 'by': 'cli', 'kind': 'note', 'text': 'api_key=' + 'sk-' + 'a' * 40}) + '\n')
        result = self.tick()
        self.assertEqual(result['commits'], 1)
        self.assertEqual(result['blocked'], [self.relative(dirty)[:-3]])
        self.assertEqual(self.files(), [self.relative(clean)])
        self.assertNotIn('sk-', self.git('log', '-p'))
        self.assertIn(self.relative(dirty, '.jsonl'), self.git('status', '--porcelain'))

    def test_deleted_tracked_file_is_restored_and_not_committed(self):
        job = self.store.task_create('-1:2', 'First')
        self.tick()
        head = self.git('rev-parse', 'HEAD')
        target = records.path(self.store.directory, job)
        content = target.read_bytes()
        target.unlink()
        result = self.tick()
        self.assertEqual(result['restored'], [self.relative(job)])
        self.assertEqual(target.read_bytes(), content)
        self.assertEqual(self.git('rev-parse', 'HEAD'), head)

    def test_old_notes_logs_and_temp_files_are_not_committed(self):
        job = self.store.task_create('-1:2', 'First')
        folder = records.path(self.store.directory, job).with_suffix('')
        folder.mkdir()
        (folder / 'notes.log').write_text('["old"]\n')
        (records.path(self.store.directory, job).parent / '.record-abc').write_text('temp')
        self.tick()
        tracked = self.git('ls-files').split()
        self.assertEqual(sorted(tracked), sorted(['chats.yaml', self.relative(job)]))

    def test_stale_index_lock_is_removed(self):
        self.store.task_create('-1:2', 'First')
        self.tick()
        job = self.store.task_create('-1:2', 'Second')
        lock = self.root / '.git' / 'index.lock'
        lock.write_text('')
        with self.assertRaises(RuntimeError):
            self.tick()
        os.utime(lock, (time.time() - 700, time.time() - 700))
        self.assertEqual(self.tick()['commits'], 1)
        self.assertFalse(lock.exists())
        self.assertIn(self.relative(job), self.files())

    def test_killed_child_next_tick_commits(self):
        jobs = [self.store.task_create('-1:2', 'Job %d' % index) for index in range(40)]
        self.tick()
        for job in jobs:
            self.store.task_update(job['id'], 'cli', note='changed')
        process = subprocess.Popen([sys.executable, '-m', 'coordinator.records'], stdin=subprocess.PIPE,
                                   stdout=subprocess.DEVNULL, cwd=str(Path(__file__).resolve().parents[1]))
        process.stdin.write(json.dumps([str(self.store.directory), None, None, False]).encode())
        process.stdin.close()
        deadline = time.monotonic() + 30
        while len(self.commits()) < 3 and time.monotonic() < deadline:
            time.sleep(0.01)
        process.kill()
        process.wait()
        deadline = time.monotonic() + 30
        while (self.root / '.git' / 'index.lock').exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        with (self.store.directory / 'records-sync.lock').open('a') as lock:
            while True:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    self.assertLess(time.monotonic(), deadline, 'Killed child still holds the records sync lock')
                    time.sleep(0.01)
            fcntl.flock(lock, fcntl.LOCK_UN)
        self.tick()
        self.assertEqual(self.git('status', '--porcelain', '--', '*.md', '*.jsonl'), '')
        self.assertEqual(len(self.commits()), 41)

    def test_hour_old_torii_index_lock_is_removed_and_next_tick_commits(self):
        job = self.store.task_create('-1:2', 'First')
        self.tick()
        lock = self.root / '.git' / 'torii-index.lock'
        lock.write_text('')
        os.utime(lock, (time.time() - 3600, time.time() - 3600))
        self.store.task_update(job['id'], 'cli', note='next')
        self.assertEqual(self.tick()['commits'], 1)
        self.assertFalse(lock.exists())
        self.assertIn(self.relative(job, '.jsonl'), self.files())

    def test_kill_inside_a_commit_then_next_tick_commits(self):
        jobs = [self.store.task_create('-1:2', 'Job %d' % index) for index in range(3)]
        self.tick()
        for job in jobs:
            self.store.task_update(job['id'], 'cli', note='changed')
        shim = Path(self.temp.name) / 'shim'
        shim.mkdir()
        (shim / 'git').write_text('#!/bin/sh\ncase " $* " in *" update-index "*)\n'
                                  '  if [ -n "$GIT_INDEX_FILE" ]; then : > "$GIT_INDEX_FILE.lock"; kill -9 $PPID; exit 1; fi;;\n'
                                  'esac\nexec %s "$@"\n' % shutil.which('git'))
        (shim / 'git').chmod(0o755)
        child = subprocess.run([sys.executable, '-m', 'coordinator.records'], stdout=subprocess.DEVNULL,
                               input=json.dumps([str(self.store.directory), None, None, False]).encode(),
                               cwd=str(Path(__file__).resolve().parents[1]),
                               env=dict(os.environ, PATH=str(shim) + os.pathsep + os.environ['PATH']))
        self.assertEqual(child.returncode, -9)
        self.assertTrue(list((self.root / '.git').glob('torii-index*.lock')))
        self.assertGreaterEqual(self.tick()['commits'], 3)
        self.assertEqual(self.git('status', '--porcelain', '--', '*.md', '*.jsonl'), '')
        self.assertFalse(list((self.root / '.git').glob('torii-index*')))

    def test_loose_objects_are_packed_after_commits(self):
        job = self.store.task_create('-1:2', 'First')
        self.tick()
        def counts():
            return dict(line.split(': ', 1) for line in self.git('count-objects', '-v').splitlines())
        largest = 0
        with patch.object(records, 'GC_AUTO', 40, create=True):
            for index in range(100):
                self.store.task_update(job['id'], 'cli', note='change %d' % index)
                self.assertEqual(self.tick()['commits'], 1)
                largest = max(largest, int(counts()['count']))
        self.assertLessEqual(largest, 50)
        self.assertGreater(int(counts()['in-pack']), 300)

    def test_timed_out_import_resumes_where_it_stopped(self):
        jobs = [self.store.task_create('-1:2', 'Job %d' % index) for index in range(10)]
        records.write_chats(self.store)
        scanned = []
        stop = [True]
        scan = records.scan_content
        def stop_at_eighth(text, filename):
            if stop[0] and filename == self.relative(jobs[7]):
                raise TimeoutError('Records commit ran past its time limit')
            scanned.append(filename)
            return scan(text, filename)
        with patch.object(records, 'CHUNK', 3, create=True), patch.object(records, 'scan_content', stop_at_eighth):
            with self.assertRaises(TimeoutError):
                records.sync(self.store.directory)
            self.assertEqual(self.commits(), ['Update 3 records', 'Import records'])
            scanned.clear()
            stop[0] = False
            result = records.sync(self.store.directory)
        self.assertEqual(sorted(scanned), sorted([self.relative(job) for job in jobs[6:10]] + ['chats.yaml']))
        self.assertEqual(result['commits'], 5)
        self.assertEqual(self.git('status', '--porcelain', '--', '*.md', '*.jsonl', 'chats.yaml'), '')

    def test_broken_git_or_missing_git_keeps_job_ops_and_sets_error(self):
        job = self.store.task_create('-1:2', 'First')
        git = self.root / '.git'
        git.write_text('gitdir: /nonexistent\n')
        state = {}
        asyncio.run(records.commit_tick(self.store, state))
        self.assertIn('error', self.store.get('records_sync_status'))
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM problems WHERE area='records' AND code='commit-failed'").fetchone()[0], 1)
        self.store.task_update(job['id'], 'cli', status='done', note='closed anyway')
        self.assertEqual(records.read(records.path(self.store.directory, job))[0]['status'], 'done')
        git.unlink()
        with patch.dict(os.environ, {'PATH': str(Path(self.temp.name) / 'empty')}):
            asyncio.run(records.commit_tick(self.store, {}))
        self.assertIn('error', self.store.get('records_sync_status'))
        self.store.task_update(job['id'], 'cli', note='still works')
        self.assertEqual(records.journal(records.journal_path(self.store.directory, job))[-1]['text'], 'still works')
        asyncio.run(records.commit_tick(self.store, {}))
        status = self.store.get('records_sync_status')
        self.assertNotIn('error', status)
        self.assertTrue(status['sha'])


class HistoryTests(LedgerCase):
    def worker_result(self, job, text):
        with self.store.db:
            worker = self.store.db.execute('''INSERT INTO workers(task,topic,provider,prompt,created,updated)
                VALUES (?,?,?,?,?,?)''', (job['id'], '-1:2', 'codex', 'Work', 1, 1)).lastrowid
        self.store.worker_complete(worker, {'success': True, 'text': text})
        records.result_entries_in_thread(self.store.directory)

    def close(self, job, **fields):
        return control_api.call(self.store, 'tasks.update', dict(task=job['id'], **fields), source='mcp', topic='-1:2')

    def test_refs_come_from_the_whole_result_and_stay_a_capped_set(self):
        job = self.store.task_create('-1:2', 'Work')
        sha = 'ab' * 20
        keep = str(Path.home() / 'Projects' / 'x' / 'KEEP' / 'final.png')
        state = str(self.store.directory / 'results' / ('task-%d' % job['id']) / 'proof.txt')
        text = 'x' * 5000 + '\nPR https://github.com/o/r/pull/7, commit %s, proof %s and %s. Hash %s' % (sha, state, keep, 'c' * 64)
        self.worker_result(job, text)
        self.worker_result(job, 'Again https://github.com/o/r/pull/7')
        data, _ = records.read(records.path(self.store.directory, job))
        self.assertEqual(data['refs'], ['https://github.com/o/r/pull/7', sha, 'results/task-%d/proof.txt' % job['id'],
                                        '~/Projects/x/KEEP/final.png'])
        with self.store.transaction():
            records.add_refs(self.store, job['id'], ['%040x' % index for index in range(30)])
        refs = records.read(records.path(self.store.directory, job))[0]['refs']
        self.assertEqual((len(refs), refs[-1]), (records.REFS, '%040x' % 29))
        self.assertEqual(self.refs(job), 34)

    def refs(self, job, prefix=None):
        rows = [row[0] for row in self.store.db.execute('SELECT ref FROM task_refs WHERE task=? ORDER BY rowid', (job['id'],))]
        return len(rows) if prefix is None else [row for row in rows if row.startswith(prefix)]

    def test_folder_refs_take_bare_folders_and_scratch_and_skip_one_job(self):
        text = ('See /s/results/task-12, scratch/task-4/run.py and `results/task-12/a b`. Not results/task-9x, '
                'myresults/task-3/x or https://github.com/o/r/pull/7 or %s.' % ('ab' * 20))
        self.assertEqual(records.folder_refs(text), ['results/task-12', 'scratch/task-4/run.py', 'results/task-12/a'])
        self.assertEqual(records.folder_refs(text, skip=12), ['scratch/task-4/run.py'])

    def test_a_folder_ref_with_a_trailing_slash_protects_the_files_inside(self):
        from coordinator import close_saves
        refs = records.refs_in('Proofs: results/task-5/proof/ (see) and /s/scratch/task-6/ and KEEP/x/')
        self.assertEqual(refs, ['results/task-5/proof', 'scratch/task-6', 'KEEP/x'])
        root = Path('/s/results/task-5')
        self.assertTrue(close_saves.result_referenced(root / 'proof' / 'a.log', root, {'5': set(refs)}))

    def test_each_write_of_job_text_records_the_folders_it_names(self):
        job = self.store.task_create('-1:2', 'Use results/task-1/a.md', notes='and scratch/task-2/b')
        self.store.task_note(job['id'], 'then results/task-3/c.json')
        self.store.task_update(job['id'], notes='results/task-4')
        with self.store.db:
            self.store.db.execute('''INSERT INTO workers(task,topic,provider,prompt,created,updated)
                VALUES (?,?,?,?,?,?)''', (job['id'], '-1:2', 'codex', 'Work', 1, 1))
        worker = dict(self.store.db.execute('SELECT * FROM workers WHERE task=?', (job['id'],)).fetchone())
        with self.store.transaction():
            from coordinator.workers import remember_steer
            remember_steer(self.store, worker, 'Also read results/task-5/d.py', 1)
        self.assertEqual(self.refs(job, ''), ['results/task-1/a.md', 'scratch/task-2/b', 'results/task-3/c.json',
                                              'results/task-4', 'results/task-5/d.py'])

    def test_a_worker_result_records_the_folders_its_changed_results_files_name(self):
        job = self.store.task_create('-1:2', 'Work')
        folder = self.store.directory / 'results' / ('task-%d' % job['id'])
        (folder / 'sub').mkdir(parents=True)
        (folder / 'old.md').write_text('results/task-90/old')
        self.assertEqual(records.text_refs(folder, time.time() + 60), [])
        self.worker_result(job, 'First')
        self.assertEqual(self.refs(job, 'results/task-9'), ['results/task-90/old'])
        (folder / 'plan.md').write_text('Needs results/task-91/x.md and results/task-%d/self.md' % job['id'])
        (folder / 'sub' / 'run.sh').write_text('cat /s/scratch/task-92/in.txt')
        (folder / 'tool').write_text('#!/bin/sh\ncat results/task-93/y')
        (folder / 'tool').chmod(0o755)
        (folder / 'data.json').write_text('results/task-94/z')
        (folder / 'node_modules').mkdir()
        (folder / 'node_modules' / 'x.js').write_text('results/task-95/z')
        self.worker_result(job, 'Second')
        self.assertEqual(sorted(self.refs(job, 'results/task-9') + self.refs(job, 'scratch/')),
                         ['results/task-90/old', 'results/task-91/x.md', 'results/task-93/y', 'scratch/task-92/in.txt'])
        self.assertEqual(self.refs(job, 'results/task-%d' % job['id']), [])

    def test_a_commit_next_to_a_key_path_in_refs_is_not_a_secret(self):
        line = 'refs: ["results/task-7/briefs/key-guard-one-pass.txt","' + 'a3' * 20 + '"]'
        finding = {'RuleID': 'generic-api-key', 'StartLine': 2, 'StartColumn': 22, 'EndColumn': len(line), 'File': 'x.md'}
        self.assertIsNone(records.gitleaks_blocked(json.dumps([finding]), lambda item: '---\n' + line + '\n'))
        token = 'refs: ["results/task-7/key.txt","ghp_' + 'A' * 36 + '"]'
        self.assertEqual(records.gitleaks_blocked(json.dumps([finding]), lambda item: '---\n' + token + '\n'), 'x.md')

    def test_agent_close_needs_a_summary_and_merged_needs_proof(self):
        job = self.store.task_create('-1:2', 'Ship it', finish='merged')
        refused = self.close(job, status='done')
        self.assertFalse(refused.ok)
        self.assertIn('summary=', refused.text)
        refused = self.close(job, status='done', summary='Shipped the change. Nothing open.')
        self.assertFalse(refused.ok)
        self.assertIn('No PR: <reason>', refused.text)
        self.assertIn('finish=answered', refused.text)
        self.assertFalse(self.close(job, status='done', summary='x' * 4001 + ' https://github.com/o/r/pull/1').ok)
        self.assertFalse(self.close(job, summary='Not closing').ok)
        for summary in ('Merged https://github.com/o/r/pull/3.', 'Landed as ' + 'f' * 40 + '.',
                        'Edited the live rule file.\nNo PR: live-file-only change, owner message 90244.'):
            with self.subTest(summary=summary):
                done = self.close(job, status='done', summary=summary)
                self.assertTrue(done.ok, done.text)
                data, body = records.read(records.path(self.store.directory, job))
                self.assertEqual(body['outcome'], summary)
                self.assertEqual(self.store.task_iterations(job['id'])[-1]['summary'], summary)
                with self.store.db:
                    self.store.db.execute("UPDATE tasks SET status='open' WHERE id=?", (job['id'],))
        self.assertIn('https://github.com/o/r/pull/3', records.read(records.path(self.store.directory, job))[0]['refs'])
        other = self.store.task_create('-1:2', 'Question', finish='merged')
        self.assertTrue(self.close(other, status='done', finish='answered', summary='Answered in chat.').ok)
        dropped = self.store.task_create('-1:2', 'Drop')
        self.assertFalse(self.close(dropped, status='dropped', note='Not needed').ok)
        self.assertTrue(self.close(dropped, status='dropped', note='Not needed', summary='Dropped: the owner chose another way.').ok)
        _, body = records.read(records.path(self.store.directory, dropped))
        self.assertEqual(body['outcome'], 'Dropped: the owner chose another way.')
        program = self.store.task_create('-1:2', 'Lane job')
        refused = control_api.call(self.store, 'tasks.update', {'task': program['id'], 'status': 'done'}, from_name='Lane')
        self.assertIn('summary=', refused.text)
        torii = self.store.task_create('-1:2', 'Auto', finish='merged')
        self.store.task_update(torii['id'], status='done', stage='merged')
        self.assertEqual(self.store.task_get(torii['id'])['status'], 'done')

    def test_reopen_keeps_the_summary_and_the_record_shows_it(self):
        job = self.store.task_create('-1:2', 'Undo')
        self.worker_result(job, 'Worker report: did the thing.')
        summary = 'Decision: keep A because B. No PR: answered in chat, owner message 1.'
        self.assertTrue(self.close(job, status='done', summary=summary).ok)
        asyncio.run(records.commit_tick(self.store, {}))
        asyncio.run(records.commit_tick(self.store, {}))
        self.assertTrue(self.store.task_get(job['id'])['log'])
        reopened = self.close(job, status='open')
        self.assertTrue(reopened.ok, reopened.text)
        asyncio.run(records.commit_tick(self.store, {}))
        self.assertEqual(self.store.task_iterations(job['id'])[-1]['summary'], summary)
        self.assertEqual(records.read(records.path(self.store.directory, job))[1]['outcome'], summary)
        self.assertIn('Worker report', self.store.db.execute('SELECT text FROM messages WHERE kind=?', ('worker_result',)).fetchone()[0])
        closed_again = self.close(job, status='done', summary='Second close.')
        self.assertTrue(closed_again.ok, closed_again.text)
        again = self.close(job, status='done', summary='A third summary.')
        self.assertFalse(again.ok)
        self.assertIn('already done', again.text)
        self.assertEqual(self.store.task_iterations(job['id'])[-1]['summary'], 'Second close.')

    def test_every_agent_close_path_runs_the_close_checks(self):
        job = self.store.task_create('-1:2', 'Ship', finish='merged')
        request = {'jsonrpc': '2.0', 'id': 1, 'method': 'tools/call',
                   'params': {'name': 'tasks_update', 'arguments': {'task': job['id'], 'status': 'done'}}}
        out = asyncio.run(mcp.dispatch_async(self.store, request, topic='-1:2'))
        self.assertIn('summary=', json.dumps(out))
        request['params']['arguments']['summary'] = 'Shipped.'
        out = asyncio.run(mcp.dispatch_async(self.store, request, topic='-1:2'))
        self.assertIn('No PR: <reason>', json.dumps(out))
        for source in ('cli', 'mcp'):
            refused = control_api.call(self.store, 'tasks.update', {'task': job['id'], 'status': 'done'}, source=source, topic='-1:2')
            self.assertIn('summary=', refused.text)
            refused = asyncio.run(control_api.call_async(self.store, 'tasks.update', {'task': job['id'], 'status': 'dropped',
                                                                                      'note': 'x'}, source=source, topic='-1:2'))
            self.assertIn('summary=', refused.text)
        self.assertEqual(self.store.task_get(job['id'])['status'], 'open')
        request['params']['arguments']['summary'] = 'Shipped https://github.com/o/r/pull/4.'
        asyncio.run(mcp.dispatch_async(self.store, request, topic='-1:2'))
        self.assertEqual(self.store.task_get(job['id'])['status'], 'done')

    def test_format_two_leaves_out_empty_fields_and_reads_format_one(self):
        job = self.store.task_create('-1:2', 'Format')
        target = records.path(self.store.directory, job)
        text = target.read_text()
        self.assertNotIn('commit:', text)
        self.assertNotIn('## Notes', text)
        self.assertNotIn(': null', text)
        self.assertNotIn(': []', text)
        self.assertRegex(text, r'iterations: \[\{"k":0,"opened":"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ","opened_by":"create"\}\]')
        data, body = records.read(target)
        old = dict(data, commit=None, created=self.store.task_get(job['id'])['created'])
        old['iterations'] = [dict(item, opened=self.store.task_iterations(job['id'])[0]['opened']) for item in data['iterations']]
        header = '\n'.join(key + ': ' + json.dumps(old[key]) for key in (*records.FIELDS, 'commit') if key not in ('refs', 'log'))
        target.write_text('---\n' + header + '\n---\n## Ask\n> Format\n\n## Outcome\n> \n\n## Notes\n\n')
        self.assertEqual(records.read(target), (data, body))
        self.assertEqual(records.check(self.store)['count'], 0)

    def test_close_moves_the_journal_into_history_and_keeps_the_record(self):
        job = self.store.task_create('-1:2', 'Close me')
        self.store.task_update(job['id'], 'cli', note='Decision: keep it small.')
        self.worker_result(job, 'Done in https://github.com/o/r/pull/5')
        journal = self.relative(job, '.jsonl')
        self.assertTrue(self.close(job, status='done', summary='Shipped https://github.com/o/r/pull/5. Decision: small.').ok)
        before = self.git('rev-parse', 'HEAD') if (self.root / '.git').exists() else None
        asyncio.run(records.commit_tick(self.store, {}))
        asyncio.run(records.commit_tick(self.store, {}))
        task = self.store.task_get(job['id'])
        self.assertTrue(task['log'])
        self.assertNotEqual(task['log'], before)
        self.assertFalse((self.root / journal).exists())
        self.assertNotIn(journal, self.git('ls-files').split())
        self.assertIn('Decision: keep it small.', self.git('show', task['log'] + ':' + journal))
        data, body = records.read(records.path(self.store.directory, job))
        self.assertEqual((data['log'], body['outcome']), (task['log'], 'Shipped https://github.com/o/r/pull/5. Decision: small.'))
        self.assertEqual(self.git('status', '--porcelain', '--', '*.md', '*.jsonl'), '')
        fetched = control_api.call(self.store, 'tasks.get', {'task': job['id']})
        self.assertIn('Full journal: git -C %s show %s:%s' % (self.root, task['log'], journal), fetched.text)
        self.assertEqual(records.check(self.store)['count'], 0)
        head = self.git('rev-parse', 'HEAD')
        asyncio.run(records.commit_tick(self.store, {}))
        self.assertEqual(self.git('rev-parse', 'HEAD'), head)
        self.assertIsNone(self.store.db.execute('SELECT 1 FROM record_compactions').fetchone())
        restored = Store(Path(self.temp.name) / 'restored')
        self.addCleanup(restored.close)
        records.restore(restored, self.root)
        self.assertEqual(restored.task_get(job['id'])['log'], task['log'])
        records.backfill(restored)
        self.assertEqual(records.check(restored)['count'], 0)
        again, again_body = records.read(records.path(restored.directory, job))
        self.assertEqual(again_body, body)
        self.assertEqual({key: again[key] for key in ('refs', 'log', 'closed', 'iterations')},
                         {key: data[key] for key in ('refs', 'log', 'closed', 'iterations')})

    def test_log_command_reads_an_archived_journal_from_the_archive_repository(self):
        job = self.store.task_create('-1:2', 'Archived')
        archive = self.store.directory / 'records-pre-dense.git'
        with self.store.transaction():
            self.store.put('records_archive', {'repo': 'old/history', 'sha': 'abc123', 'git_dir': str(archive)})
            self.store.task_update(job['id'], status='done')
            self.store.db.execute("UPDATE tasks SET log='old/history@abc123' WHERE id=?", (job['id'],))
        self.assertEqual(control_api.call(self.store, 'tasks.get', {'task': job['id']}).data['log_command'],
                         'git --git-dir %s show abc123:%s' % (archive, self.relative(job, '.jsonl')))

    def test_dirty_journal_waits_for_its_commit(self):
        job = self.store.task_create('-1:2', 'Late report')
        self.store.task_update(job['id'], 'cli', note='first')
        self.tick()
        self.store.task_update(job['id'], status='dropped', note='Not needed')
        records.journal_append(self.store, job, [dict(at='x', by='cli', kind='note', text='late')])
        groups = records.closed_groups(self.store)
        group = self.relative(job, '')
        with patch.object(records, 'scan_content', side_effect=lambda text, name: 'late' in text):
            result = records.sync(self.store.directory, compact=list(groups))
        self.assertEqual(result['compacted'], {})
        self.assertTrue((self.root / (group + '.jsonl')).exists())
        result = records.sync(self.store.directory, compact=list(groups))
        self.assertIn(group, result['compacted'])
        records.record_logs(self.store, groups, result['compacted'])
        self.assertIn('late', self.git('show', self.store.task_get(job['id'])['log'] + ':' + group + '.jsonl'))
