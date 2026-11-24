import os
import subprocess
import sys
from pathlib import Path
import time
import unittest

from coordinator import control_api, worker_storage
from tests.test_worker_storage import StorageSupport, TOPIC


class HandoffTests(StorageSupport, unittest.TestCase):
    def test_spawn_attaches_snapshot_and_handles_missing(self):
        first, _ = self.spawn()
        self.assertEqual(first['handoff'], '')
        self.expected_results.joinpath('HANDOFF.md').write_text('goal\nfinish work\n')
        second, _ = self.spawn('codex')
        self.assertEqual(second['handoff'], 'goal\nfinish work\n')

    def test_measurement_counts_each_end(self):
        worker, _ = self.spawn()
        start = time.time() - 1
        for fresh in (False, True):
            self.store.db.execute('UPDATE workers SET status=?,started_at=?,updated=? WHERE id=?',
                                  ('running', start, time.time() + 1, worker['id']))
            if fresh:
                (self.expected_results / 'HANDOFF.md').write_text('fresh')
            self.store.worker_complete(worker['id'], {'success': True})
        self.store.db.commit()
        script = Path(__file__).resolve().parents[1] / 'scripts' / 'handoff-share.py'
        output = subprocess.check_output([sys.executable, str(script), str(self.store.directory / 'state.sqlite')],
                                         text=True)
        self.assertIn('1/2 (50.0%)', output)

    def test_truncation(self):
        self.expected_results.mkdir(parents=True)
        path = self.expected_results / 'HANDOFF.md'
        path.write_text('line\n' * 61)
        brief = worker_storage.handoff_brief(self.store, self.task['id'])
        self.assertEqual(brief.count('line\n'), 60)
        self.assertIn('[Handoff truncated.]', brief)
        path.write_text('x' * 100000)
        self.assertLess(len(worker_storage.handoff_brief(self.store, self.task['id'])), 33000)
        self.assertIn('[Handoff truncated.]', worker_storage.handoff_brief(self.store, self.task['id']))

    def test_completion_marks_fresh_missing_and_old(self):
        for offset, expected in ((None, 1), (-10, 1), (10, 0)):
            with self.subTest(offset=offset):
                worker, _ = self.spawn('codex' if offset == -10 else 'claude')
                start = time.time()
                self.store.db.execute('UPDATE workers SET status=?,started_at=? WHERE id=?',
                                      ('running', start, worker['id']))
                path = self.expected_results / 'HANDOFF.md'
                if offset is not None:
                    path.write_text('goal')
                    os.utime(path, (start + offset, start + offset))
                self.store.worker_complete(worker['id'], {'success': True})
                got = control_api.call(self.store, 'workers.get', {'worker': worker['id']}, topic=TOPIC)
                job = control_api.call(self.store, 'tasks.get', {'task': self.task['id']}, topic=TOPIC)
                self.assertEqual(got.data['no_handoff'], expected)
                self.assertEqual(job.data['workers'][-1]['no_handoff'], expected)


class HandoffDeliveryTests(StorageSupport, unittest.IsolatedAsyncioTestCase):
    async def test_worker_receives_snapshot_and_update_request(self):
        from coordinator.providers import RunResult
        from coordinator.service import Service

        self.expected_results.mkdir(parents=True)
        path = self.expected_results / 'HANDOFF.md'
        path.write_text('original handoff\n')
        worker, _ = self.spawn()
        path.write_text('changed after spawn\n')
        received = []

        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                received.append(options['instructions'])
                path.write_text('updated during work\n')
                return RunResult(session, text='done', success=True)

        service = Service(self.store, None, Runner(), self.root)
        finished = await service.workers.run(service.workers.get(worker['id']), self.store.topic(TOPIC))
        self.assertIn('original handoff', received[0])
        self.assertNotIn('changed after spawn', received[0])
        self.assertIn('Before you end, update', received[0])
        self.assertEqual(finished['no_handoff'], 0)
