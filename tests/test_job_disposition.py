import asyncio
import json
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from coordinator import control_api
from tests.test_service import FakeSession, ServiceTestsSupport, TOPIC


class JobDispositionSupport(ServiceTestsSupport):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.session = FakeSession('parent')
        self.session.busy = False
        self.session.protocol = SimpleNamespace(background=[])
        self.session.idle = lambda: not self.session.busy and not self.session.protocol.background
        self.service.sessions[TOPIC] = self.session
        self.sessions.append(self.session)
        self.service.last_turn[TOPIC] = time.monotonic()

    def job(self):
        return self.store.task_create(TOPIC, 'Build the feature', worktree=str(self.root))

    def callbacks(self):
        return [dict(row) for row in self.store.db.execute("SELECT * FROM messages WHERE kind='callback' ORDER BY id")]

    async def settle(self):
        pending = bool(self.store.messages_pending())
        await self.service.feed_once()
        if self.service.feed_tasks:
            await asyncio.gather(*set(self.service.feed_tasks.values()))
        if pending:
            await self.service.feed_once()


class JobDispositionTests(JobDispositionSupport, unittest.IsolatedAsyncioTestCase):
    async def test_interrupted_worker_leaves_job_open_without_nudge_wait_or_resume(self):
        self.job()
        worker = self.worker(provider='codex')
        self.store.worker_complete(worker, {'success': False, 'failure_code': 'interrupted'}, 'interrupted')
        await self.settle()
        await self.settle()
        await self.service.workers_once()
        self.assertEqual(self.callbacks(), [])
        self.assertEqual(self.store.waits_list(owner=True), [])
        self.assertEqual([j['status'] for j in self.store.tasks_list()], ['open', 'open'])
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM workers').fetchone()[0], 1)

    async def test_restart_notice_asks_the_owner_to_resume(self):
        worker = self.worker(provider='codex')
        self.store.worker_complete(worker, {'success': False, 'failure_code': 'interrupted'}, 'interrupted')
        await self.announce(0)
        texts = [r['text'] for r in self.store.db.execute('SELECT text FROM outbox')]
        self.assertTrue(any('1 agent was stopped. Ask me to resume them.' in text for text in texts))

    async def test_secret_fill_after_park_starts_the_parked_worker(self):
        task = self.store.task_create(TOPIC, 'Needs a key', worktree=str(self.root), secrets=['FIXTURE_KEY'])
        now = 1.0
        with self.store.db:
            worker = self.store.db.execute('''INSERT INTO workers (task,topic,provider,prompt,cwd,workspace,status,created,updated)
                VALUES (?,?,?,?,?,?,'waiting_for_secret',?,?)''', (task['id'], TOPIC, 'codex', 'use the key', str(self.root),
                                                                   json.dumps({'cwd': str(self.root)}), now, now)).lastrowid
            envelope = self.store.db.execute('''INSERT INTO envelopes(name,reason,consumer,task,topic,state,created,expires,updated)
                VALUES ('FIXTURE_KEY','fixture','worker',?,?,'open',1,9999999999,1)''', (task['id'], TOPIC)).lastrowid
        await self.settle()
        await self.settle()
        self.assertEqual(self.service.workers.get(worker)['status'], 'waiting_for_secret')
        with self.store.db:
            self.store.db.execute("UPDATE envelopes SET state='filled',filled_at=2 WHERE id=?", (envelope,))
        with patch.object(self.service.workers, 'run', new_callable=AsyncMock) as run:
            await self.service.workers_once()
            if self.service.worker_tasks:
                await asyncio.gather(*self.service.worker_tasks.values())
        self.assertEqual(run.await_count, 1)

    async def test_parked_worker_and_coordinator_stop_keep_the_feed_running(self):
        parked = self.worker(provider='codex')
        task = self.service.workers.get(parked)['task']
        now = 1.0
        with self.store.db:
            self.store.db.execute("UPDATE workers SET status='waiting_for_secret' WHERE id=?", (parked,))
            stopped = self.store.db.execute('''INSERT INTO workers (task,topic,provider,prompt,cwd,workspace,created,updated)
                VALUES (?,?,?,?,?,?,?,?)''', (task, TOPIC, 'codex', 'second step', str(self.root),
                                              json.dumps({'cwd': str(self.root)}), now, now)).lastrowid
        result = control_api.call(self.store, 'workers.stop', {'worker': stopped}, source='mcp', topic=TOPIC)
        self.assertTrue(result.ok, result.text)
        await self.service.controls_once()
        self.store.db.execute("UPDATE messages SET delivered='received' WHERE delivered='pending'")
        await self.settle()
        self.store.db.execute("UPDATE messages SET delivered='received' WHERE delivered='pending'")
        for _ in range(2):
            await self.service.feed_once()


if __name__ == '__main__':
    unittest.main()
