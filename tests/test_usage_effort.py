import tempfile
import unittest
from pathlib import Path

from coordinator import control_api, mcp
from coordinator.policy import usage_policy
from coordinator.store import Store


class UsageEffortTests(unittest.TestCase):
    def test_d1_chat_policy_exact_combination_and_isolation(self):
        from coordinator.policy import CHAT_POLICY, CHAT_POLICY_LIMIT, chat_policy
        base = 'Default policy.  \n\n\n'
        (self.store.directory / 'USAGE.md').write_text(base)
        header = ('\n\n## This chat\n\n'
                  'The owner set these delegation instructions for this chat. They are part of this usage policy. '
                  'Where they conflict with the sections above, follow them in this chat. They do not change the '
                  'coordinator rules (secrets, the vault, Trash, telegram.send as the only owner channel) or '
                  'owner-only choices such as the Codex account.\n\n')
        self.assertEqual(CHAT_POLICY_LIMIT, 2000)
        self.assertEqual(CHAT_POLICY, header)
        self.assertEqual(chat_policy(self.store, 'coordinator'), base)
        self.store.put('coordinator_delegation', {'text': 'Home only.', 'revision': 1})
        self.store.put('coordinator:1:2_delegation', {'text': 'Use Sol.', 'revision': 1})
        self.assertEqual(chat_policy(self.store, 'coordinator:1:2'), base.rstrip('\n') + header + 'Use Sol.\n')
        self.assertEqual(chat_policy(self.store, 'coordinator:1:3'), base)
        self.store.put('coordinator:1:2_delegation', None)
        self.assertEqual(chat_policy(self.store, 'coordinator:1:2'), base)

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

    def test_default_policy_is_neutral_and_keeps_provider_effort_and_reply_guidance(self):
        (Path(self.temp.name) / 'USAGE.md').unlink()
        policy = usage_policy(self.store.directory)
        for model in ('sol', 'luna', 'astra', 'opus', 'fable', 'sonnet', 'haiku', 'example-host'):
            with self.subTest(model=model):
                self.assertNotRegex(policy.lower(), r'\b' + model + r'\b')
        self.assertNotRegex(policy, r'\d')
        self.assertNotIn('fan-out', policy)
        self.assertNotIn('Run independent pieces in parallel', policy)
        self.assertIn('## Coordinators\n\nOwner messages come first.', policy)
        self.assertIn('Use connected providers', policy)
        self.assertIn('policy.set', policy)
        self.assertNotIn('for development', policy)
        self.assertNotIn('for frontend', policy)
        self.assertIn('Saved secrets and worker goals work with either provider.', policy)
        self.assertIn('Workers default to medium', policy)
        self.assertIn('## Review', policy)
        self.assertIn('## Economy', policy)

    def test_default_policy_places_opencode_worker_bullet_after_saved_secrets(self):
        (Path(self.temp.name) / 'USAGE.md').unlink()
        policy = usage_policy(self.store.directory)
        bullet = ('- OpenCode workers (`provider="opencode"`) run only after the owner turns them on. '
                  "They have no goals and no secrets, and their spend is an estimate from the account's prices.")
        lines = policy.splitlines()
        saved_secrets = ('- Saved secrets and worker goals work with either provider. '
                         "The requesting job's worker receives the values.")
        self.assertEqual(policy.count(bullet), 1)
        self.assertEqual(lines[lines.index(saved_secrets) + 1], bullet)

    def test_worker_effort_defaults_and_accepts_explicit_levels(self):
        schema = mcp.input_schema(control_api.BY_ID['workers.spawn'])['properties']['effort']
        self.assertEqual(schema['enum'], ['low', 'medium', 'high', 'max'])
        for requested in (None, 'low', 'medium', 'high', 'max'):
            params = {'task': self.task['id'], 'provider': 'codex', 'prompt': 'Build %s' % requested}
            if requested:
                params['effort'] = requested
            created = control_api.call(self.store, 'workers.spawn', params, topic='1:2')
            self.assertTrue(created.ok, created.text)
            worker = control_api.call(self.store, 'workers.get', {'worker': created.data['worker']}, topic='1:2')
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
