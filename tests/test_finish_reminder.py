import unittest

from coordinator.service import Service
from coordinator.store import Store
from tests.test_job_disposition import JobDispositionSupport
from tests.test_service import TOPIC


class FinishReminderTests(JobDispositionSupport, unittest.IsolatedAsyncioTestCase):
    def stamped(self, finish='live'):
        job = self.store.task_create(TOPIC, 'Ship the feature', worktree=str(self.root), finish=finish)
        self.store.task_update(job['id'], stage=finish)
        return self.store.task_get(job['id'])

    def reminders(self):
        return [row['text'] for row in self.callbacks() if 'reached its finish stage' in row['text']]

    def add_worker(self, job, status):
        with self.store.db:
            self.store.db.execute('''INSERT INTO workers(task,topic,provider,prompt,cwd,workspace,status,created,updated,iteration)
                VALUES (?,?,?,?,?,?,?,?,?,?)''', (job['id'], TOPIC, 'codex', 'Ship it', str(self.root), '{}',
                                                  status, 1.0, 1.0, job['iteration']))

    async def test_stamped_job_without_worker_gets_one_silent_reminder(self):
        job = self.stamped()
        await self.settle()
        self.assertEqual(self.reminders(), ['Job %d reached its finish stage (live). Close it with its result, '
                                            'or note what is left.' % job['number']])
        await self.settle()
        await self.settle()
        self.assertEqual(len(self.reminders()), 1)
        self.assertEqual(self.store.task_get(job['id'])['status'], 'open')
        self.assertEqual(self.store.waits_list(owner=True), [])
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], 0)

    async def test_restart_does_not_repeat_the_reminder(self):
        self.stamped()
        await self.settle()
        self.assertEqual(len(self.reminders()), 1)
        self.assertFalse(self.store.db.in_transaction)
        self.store.close()
        self.store = Store(self.root / 'state')
        self.service = Service(self.store, self.telegram, self.runner, self.root)
        self.assertEqual(self.service.jobs.finish_reminders(TOPIC), 0)
        self.assertEqual(len(self.reminders()), 1)

    async def test_unstamped_or_other_stage_gets_no_reminder(self):
        self.store.task_create(TOPIC, 'No finish', worktree=str(self.root))
        job = self.store.task_create(TOPIC, 'Live finish', worktree=str(self.root), finish='live')
        self.store.task_update(job['id'], stage='merged')
        await self.settle()
        self.assertEqual(self.reminders(), [])

    async def test_active_or_parked_worker_blocks_the_reminder(self):
        for status in ('queued', 'running', 'waiting_for_secret', 'waiting_for_quota'):
            with self.subTest(status=status):
                job = self.stamped()
                self.add_worker(job, status)
                self.assertEqual(self.service.jobs.finish_reminders(TOPIC), 0)
                self.store.db.execute("UPDATE workers SET status='done' WHERE task=?", (job['id'],))
                self.assertEqual(self.service.jobs.finish_reminders(TOPIC), 1)

    async def test_armed_wait_blocks_the_reminder(self):
        job = self.stamped()
        item = self.store.task_wait(TOPIC, 'time', 'Check the deploy', task=job['id'], at='2099-01-01T00:00:00+00:00')
        self.store.db.execute("UPDATE tasks SET status='open' WHERE id=?", (job['id'],))
        await self.settle()
        self.assertEqual(self.reminders(), [])
        self.store.db.execute("UPDATE waits SET state='cancelled' WHERE id=?", (item['id'],))
        await self.settle()
        self.assertEqual(len(self.reminders()), 1)

    async def test_new_round_may_remind_again(self):
        job = self.stamped()
        await self.settle()
        self.store.task_update(job['id'], status='done')
        self.store.wait_open_round(job['id'], None, opened_by='owner')
        await self.settle()
        self.assertEqual(len(self.reminders()), 1)
        self.store.task_update(job['id'], stage='live')
        await self.settle()
        self.assertEqual(self.reminders()[-1], 'Job %d.1 reached its finish stage (live). Close it with its result, '
                                               'or note what is left.' % job['number'])
        self.assertEqual(len(self.reminders()), 2)


if __name__ == '__main__':
    unittest.main()
