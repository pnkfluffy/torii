import tempfile
import unittest
from pathlib import Path

from coordinator import control_api, mcp
from coordinator.policy import usage_policy
from coordinator.store import Store


class UsageEffortTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name))
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test',?,1)",
                                  (self.temp.name,))
        self.task = self.store.task_create('1:2', 'Build', worktree=self.temp.name)

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def test_policy_is_created_and_reads_edits(self):
        path = Path(self.temp.name) / 'USAGE.md'
        self.assertTrue(path.is_file())
        self.assertEqual(usage_policy(self.store.directory),
                         (Path(__file__).resolve().parents[1] / 'coordinator/defaults/USAGE.md').read_text())
        path.write_text('Edited owner policy')
        self.assertEqual(usage_policy(self.store.directory), 'Edited owner policy')
        another = Store(self.store.directory)
        self.assertEqual(usage_policy(another.directory), 'Edited owner policy')
        another.close()

    def test_policy_falls_back_to_default_when_file_is_removed(self):
        (Path(self.temp.name) / 'USAGE.md').unlink()
        self.assertEqual(usage_policy(self.store.directory),
                         (Path(__file__).resolve().parents[1] / 'coordinator/defaults/USAGE.md').read_text())

    def test_default_policy_names_codex_models_and_default(self):
        (Path(self.temp.name) / 'USAGE.md').unlink()
        policy = usage_policy(self.store.directory)
        for model in ('gpt-6.1-sol', 'gpt-6-luna', 'gpt-6-astra'):
            self.assertIn(model, policy)
        self.assertIn('Most development work: Codex', policy)
        self.assertIn('Saved secrets and worker goals work with either provider.', policy)
        self.assertIn('Workers default to medium', policy)

    def test_worker_effort_defaults_and_accepts_explicit_levels(self):
        schema = mcp.input_schema(control_api.BY_ID['workers.spawn'])['properties']['effort']
        self.assertEqual(schema['enum'], ['low', 'medium', 'high', 'max'])
        for requested in (None, 'low', 'medium', 'high', 'max'):
            params = {'task': self.task['id'], 'provider': 'codex', 'prompt': 'Build'}
            if requested:
                params['effort'] = requested
            created = control_api.call(self.store, 'workers.spawn', params, topic='1:2')
            self.assertTrue(created.ok, created.text)
            worker = control_api.call(self.store, 'workers.get', created.data, topic='1:2')
            self.assertEqual(worker.data['effort'], requested or 'medium')
            listed = control_api.call(self.store, 'workers.list', {'task': self.task['id']}, topic='1:2')
            self.assertIn('effort', listed.data['workers'][0])
            self.assertIn('effort ' + (requested or 'medium'), listed.text)
        refused = control_api.call(self.store, 'workers.spawn', dict(params, effort='xhigh'), topic='1:2')
        self.assertFalse(refused.ok)

    def test_job_status_shows_active_worker_effort(self):
        from coordinator.controls import _status_card
        created = control_api.call(self.store, 'workers.spawn',
                                   {'task': self.task['id'], 'provider': 'codex',
                                    'prompt': 'Build', 'effort': 'high'}, topic='1:2')
        self.assertTrue(created.ok)
        self.assertIn('%d high' % created.data['worker'], _status_card(self.store, '1:2'))
