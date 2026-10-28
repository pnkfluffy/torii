import subprocess
import sys
from pathlib import Path
import tempfile
import unittest

from coordinator import control_api, mcp
from coordinator.store import Store


HOME_TOPIC = '9:8'


class TaskFinishTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.store = Store(self.root / 'state', token_file=self.root / 'unused')
        self.addCleanup(lambda: self.store.close())
        with self.store.db:
            for topic, chat, thread in (('1:2', 1, 2), (HOME_TOPIC, 9, 8)):
                self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd) VALUES (?,?,?,?,?)',
                                      (topic, chat, thread, 'Finish', str(self.root)))
            self.store.put('coordinator_home_topic', HOME_TOPIC)

    def call(self, op, **params):
        return control_api.call(self.store, op, params, topic='1:2')

    def job(self, finish=None):
        return self.store.task_create('1:2', 'Finish this', worktree=str(self.root), finish=finish)

    def test_create_uses_chat_finish_default_and_explicit_finish_wins(self):
        with self.store.db:
            self.store.put('finish_default:1:2', 'answered')
        created = self.call('tasks.create', title='Research')
        self.assertTrue(created.ok, created.text)
        self.assertEqual(created.data['finish'], 'answered')
        explicit = self.call('tasks.create', title='Deliver', finish='delivered')
        self.assertTrue(explicit.ok, explicit.text)
        self.assertEqual(explicit.data['finish'], 'delivered')
        self.store.close()
        self.store = Store(self.root / 'state', token_file=self.root / 'unused')
        self.assertEqual(self.call('tasks.create', title='After restart').data['finish'], 'answered')

    def test_fresh_chats_have_no_finish_default_and_unset_finish_keeps_legacy_close(self):
        home = self.call('tasks.create', title='Code', topic=HOME_TOPIC)
        self.assertTrue(home.ok, home.text)
        self.assertIsNone(home.data['finish'])
        task = self.job()
        self.assertIsNone(task['finish'])
        self.assertTrue(self.call('tasks.update', task=task['id'], status='done').ok)

    def test_home_chat_can_set_merged_and_clear_its_default(self):
        configured = control_api.call(self.store, 'tasks.finish_default',
                                      {'topic': HOME_TOPIC, 'finish': 'merged'}, source='cli')
        self.assertTrue(configured.ok, configured.text)
        self.assertEqual(self.call('tasks.create', title='Code', topic=HOME_TOPIC).data['finish'], 'merged')
        cleared = control_api.call(self.store, 'tasks.finish_default', {'topic': HOME_TOPIC}, source='cli')
        self.assertTrue(cleared.ok, cleared.text)
        self.assertIsNone(cleared.data['finish'])
        self.assertIsNone(self.call('tasks.create', title='Research', topic=HOME_TOPIC).data['finish'])

    def test_finish_can_change_while_open_and_is_validated(self):
        task = self.job('merged')
        for finish in ('answered', 'delivered', 'live', 'merged'):
            result = self.call('tasks.update', task=task['id'], finish=finish)
            self.assertTrue(result.ok, result.text)
            self.assertEqual(result.data['finish'], finish)
        result = self.call('tasks.update', task=task['id'], finish='unknown')
        self.assertFalse(result.ok, result.text)
        self.assertEqual(self.store.task_get(task['id'])['finish'], 'merged')
        self.assertTrue(self.call('tasks.update', task=task['id'], status='dropped', note='Cancelled').ok)
        result = self.call('tasks.update', task=task['id'], finish='answered')
        self.assertFalse(result.ok, result.text)
        self.assertEqual(self.store.task_get(task['id'])['finish'], 'merged')

    def test_finish_can_change_in_the_call_that_closes_the_job(self):
        task = self.job('merged')
        result = self.call('tasks.update', task=task['id'], finish='answered', status='done')
        self.assertTrue(result.ok, result.text)
        self.assertEqual((result.data['finish'], result.data['status']), ('answered', 'done'))

    def test_changing_home_keeps_finish_unset_and_explicit_setting_wins(self):
        with self.store.db:
            self.store.put('coordinator_home_topic', '1:2')
        created = self.call('tasks.create', title='New home')
        previous = self.call('tasks.create', title='Previous home', topic=HOME_TOPIC)
        self.assertIsNone(created.data['finish'])
        self.assertIsNone(previous.data['finish'])
        with self.store.db:
            self.store.put('finish_default:1:2', 'answered')
        self.assertEqual(self.call('tasks.create', title='Override').data['finish'], 'answered')

    def test_local_cli_sets_or_clears_topic_default(self):
        for source in ('mcp', 'telegram'):
            result = control_api.call(self.store, 'tasks.finish_default',
                                      {'topic': '1:2', 'finish': 'answered'}, source=source)
            self.assertFalse(result.ok, result.text)
            self.assertIn('local CLI', result.text)
        command = [sys.executable, '-m', 'coordinator', '--state-dir', str(self.store.directory), 'ctl', 'call']
        done = subprocess.run(command + ['tasks.finish_default', 'topic=1:2', 'finish=answered'],
                              capture_output=True, text=True, timeout=15)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertEqual(self.call('tasks.create', title='Configured').data['finish'], 'answered')
        done = subprocess.run(command + ['tasks.finish_default', 'topic=1:2'],
                              capture_output=True, text=True, timeout=15)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIsNone(self.call('tasks.create', title='Cleared').data['finish'])
        self.assertNotIn('tasks_finish_default', mcp.TOOLS)
