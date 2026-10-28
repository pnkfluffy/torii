import json
import unittest

from coordinator import control_api, control_ui
from tests import test_workers as tw


TOPIC = tw.TOPIC
OTHER = '-10042:5'


class SecretSharingJobTests(tw.SecretWorkerTestsSupport, unittest.IsolatedAsyncioTestCase):
    asyncTearDown = tw.WorkerTests.asyncTearDown

    def worker_for(self, task):
        with self.store.db:
            row = self.store.db.execute('''INSERT INTO workers
                (task,topic,provider,prompt,cwd,workspace,created,updated) VALUES (?,?,?,?,?,?,?,?)''',
                (task['id'], task['topic'], 'claude', 'build this', str(self.root),
                 json.dumps({'cwd': str(self.root)}), 1.0, 1.0))
        return dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (row.lastrowid,)).fetchone())

    async def use_key(self, task, used_at):
        self.store.task_update(task['id'], secrets=['GITHUB_TOKEN'])
        worker = self.worker_for(self.store.task_get(task['id']))
        await self.pool.run(worker, self.store.topic(task['topic']))
        self.assertTrue(self.result(worker)['success'], self.result(worker))
        with self.store.db:
            self.store.db.execute("UPDATE envelope_events SET created=? WHERE event='use' AND worker=?",
                                  (used_at, worker['id']))

    def views(self):
        listed = control_api.call(self.store, 'secret.list', source='mcp')
        with self.store.db:
            card, _ = control_ui._secrets_view(self.store, 0)
            detail, _ = control_ui._secret_view(self.store, TOPIC, 'GITHUB_TOKEN')
        return listed, card, detail

    async def test_review_same_job_number_in_two_channels_counts_as_two_jobs(self):
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', (OTHER, -10042, 5, 'Workshop', str(self.root)))
        envelope = self.fill('GITHUB_TOKEN', 'FAKE-ENVELOPE-VALUE-0123456789')
        with self.store.db:
            self.store.db.execute('UPDATE envelopes SET filled_at=1790000060 WHERE id=?', (envelope,))
        workshop = self.store.task_create(OTHER, 'Workshop build', worktree=str(self.root))
        self.assertEqual((self.task['number'], workshop['number']), (1, 1))
        self.assertNotEqual(self.task['id'], workshop['id'])
        for index, task in enumerate((self.task, workshop), 1):
            await self.use_key(task, 1790000000 + 100 * index)
        used = sorted(row[0] for row in self.store.db.execute(
            "SELECT DISTINCT w.task FROM envelope_events e JOIN workers w ON w.id=e.worker WHERE e.event='use'"))
        self.assertEqual(used, sorted([self.task['id'], workshop['id']]))
        listed, card, detail = self.views()
        for label, value in [('use rows name distinct task ids', used), ('secret.list text', listed.text),
                             ('secret.list jobs data', listed.data[0].get('jobs')),
                             ('/secrets card', card), ('/secrets key card', detail)]:
            print('\n[regression] ' + label + ':\n' + (
                value if isinstance(value, str) else json.dumps(value)), flush=True)
        self.assertEqual(len(listed.data[0].get('jobs', [])), 2, listed.text)
        expected = [{'id': workshop['id'], 'topic': OTHER, 'channel': 'Workshop', 'number': 1},
                    {'id': self.task['id'], 'topic': TOPIC, 'channel': 'Torii', 'number': 1}]
        self.assertEqual(listed.data[0]['jobs'], expected)
        self.assertTrue(listed.text.endswith(', Workshop job 1, Torii job 1'), listed.text)
        self.assertIn(' · Workshop job 1, Torii job 1\n', card)
        self.assertIn('\nJobs: Workshop job 1, Torii job 1\n', detail)
        self.assertIn('\nFor: c · any job\n', detail)
        torii_second = self.store.task_create(TOPIC, 'Torii second', worktree=str(self.root))
        workshop_second = self.store.task_create(OTHER, 'Workshop second', worktree=str(self.root))
        self.assertEqual((torii_second['number'], workshop_second['number']), (2, 2))
        for index, task in enumerate((torii_second, workshop_second, self.task), 3):
            await self.use_key(task, 1790000000 + 100 * index)
        listed, card, detail = self.views()
        self.assertEqual([job['id'] for job in listed.data[0]['jobs']],
                         [self.task['id'], workshop_second['id'], torii_second['id'], workshop['id']])
        summary = 'Torii job 1, Workshop job 2, Torii job 2 (+1 more)'
        self.assertTrue(listed.text.endswith(', ' + summary), listed.text)
        self.assertIn(' · ' + summary + '\n', card)
        self.assertIn('\nJobs: ' + summary + '\n', detail)
