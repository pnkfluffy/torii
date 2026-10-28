import asyncio
import json
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from coordinator import control_api
from tests.test_service import FakeSession, ServiceTestsSupport, TOPIC


class JobDispositionSupport(ServiceTestsSupport):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.store.put('waits_enabled', True)
        self.store.put('auto_resume', True)
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
    async def test_one_nudge_then_owner_wait_and_restart_does_not_repeat_it(self):
        job = self.job()
        await self.settle()
        self.assertEqual(len(self.callbacks()), 1)
        self.assertIn('has no worker', self.callbacks()[0]['text'])
        self.assertEqual(self.store.task_get(job['id'])['status'], 'open')
        await self.settle()
        await self.settle()
        self.assertEqual(self.store.task_get(job['id'])['status'], 'waiting')
        waits = self.store.waits_list(owner=True)
        self.assertEqual(len(waits), 1)
        self.assertEqual([b['text'] for row in waits[0]['detail']['buttons'] for b in row], ['Resume', 'Drop'])
        self.assertEqual(len(self.callbacks()), 1)

    async def test_spawn_in_nudge_turn_keeps_job_open(self):
        job = self.job()
        await self.settle()
        result = control_api.call(self.store, 'workers.spawn', {'task': job['id'], 'provider': 'codex', 'prompt': 'Build it'})
        self.assertTrue(result.ok, result.text)
        await self.settle()
        await self.settle()
        self.assertEqual(self.store.task_get(job['id'])['status'], 'open')
        self.assertEqual(self.store.waits_list(owner=True), [])

    async def test_each_idle_job_gets_one_nudge_at_the_same_settle(self):
        jobs = [self.job() for _ in range(3)]
        await self.settle()
        self.assertEqual(len(self.callbacks()), 3)
        self.assertEqual([self.store.task_get(job['id'])['status'] for job in jobs], ['open'] * 3)
        await self.settle()
        self.assertEqual([self.store.task_get(job['id'])['status'] for job in jobs], ['waiting'] * 3)

    async def test_idle_job_after_restart_without_parent_still_gets_a_nudge(self):
        self.job()
        self.service.sessions.clear()
        await self.settle()
        self.assertEqual(len(self.callbacks()), 1)

    async def test_pending_message_busy_turn_and_background_delay_settle(self):
        self.job()
        self.session.busy = True
        await self.settle()
        self.assertEqual(self.callbacks(), [])
        self.session.busy = False
        self.session.protocol.background = ['child']
        await self.settle()
        self.assertEqual(self.callbacks(), [])
        self.session.protocol.background = []
        message = self.store.message_save(TOPIC, 'owner', 'hold')
        await self.service.feed_once()
        self.assertEqual(self.callbacks(), [])
        self.session.hold.set()
        await asyncio.gather(*set(self.service.feed_tasks.values()))
        self.assertEqual(self.store.db.execute('SELECT delivered FROM messages WHERE id=?', (message['id'],)).fetchone()[0], 'received')
        await self.settle()
        self.assertEqual(len(self.callbacks()), 1)

    async def test_owner_stop_waits_but_coordinator_stop_is_held(self):
        for source, expected in (('telegram', 'waiting'), ('mcp', 'open')):
            with self.subTest(source=source):
                worker = self.worker(provider='codex')
                result = control_api.call(self.store, 'workers.stop', {'worker': worker}, source=source, topic=TOPIC)
                self.assertTrue(result.ok, result.text)
                await self.service.controls_once()
                await self.settle()
                job = self.store.task_get(self.service.workers.get(worker)['task'])
                self.assertEqual(job['status'], expected)
                if source == 'telegram':
                    self.assertIn('Stopped by you', self.store.waits_list(owner=True)[0]['ask'])

    async def test_secret_wait_keeps_parked_worker_without_disposition(self):
        worker = self.worker(provider='codex')
        with self.store.db:
            self.store.db.execute("UPDATE workers SET status='waiting_for_secret' WHERE id=?", (worker,))
        await self.settle()
        self.assertEqual(self.callbacks(), [])
        await self.settle()
        self.assertEqual(self.service.workers.get(worker)['status'], 'waiting_for_secret')
        self.assertEqual(self.store.task_get(self.service.workers.get(worker)['task'])['status'], 'open')
        self.assertEqual(self.store.waits_list(owner=True), [])
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM workers').fetchone()[0], 1)

    async def test_switch_off_has_no_nudge_wait_status_change_or_resume(self):
        self.store.put('auto_resume', False)
        job = self.job()
        worker = self.worker(provider='codex')
        self.store.worker_complete(worker, {'success': False, 'failure_code': 'interrupted'}, 'interrupted')
        await self.settle()
        await self.settle()
        await self.service.workers_once()
        self.assertEqual(self.callbacks(), [])
        self.assertEqual(self.store.waits_list(owner=True), [])
        self.assertEqual([j['status'] for j in self.store.tasks_list()], ['open', 'open'])
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM workers').fetchone()[0], 1)

    async def test_restart_notice_switch_off_keeps_existing_wording(self):
        self.store.put('auto_resume', False)
        worker = self.worker(provider='codex')
        self.store.worker_complete(worker, {'success': False, 'failure_code': 'interrupted'}, 'interrupted')
        await self.announce(0)
        texts = [r['text'] for r in self.store.db.execute('SELECT text FROM outbox')]
        self.assertTrue(any('1 agent was stopped. Ask me to resume them.' in text for text in texts))

    async def test_restart_notice_switch_on_retires_the_manual_resume_instruction(self):
        worker = self.worker(provider='codex')
        self.store.worker_complete(worker, {'success': False, 'failure_code': 'interrupted'}, 'interrupted')
        await self.announce(0)
        texts = [r['text'] for r in self.store.db.execute('SELECT text FROM outbox')]
        self.assertTrue(any('checking 1 stopped job for auto-resume' in text for text in texts))
        self.assertFalse(any('Ask me to resume them' in text for text in texts))

    async def test_switch_is_local_defaults_off_and_requires_waits(self):
        self.store.db.execute("DELETE FROM settings WHERE key='auto_resume'")
        self.assertFalse(self.store.get('auto_resume', False))
        result = control_api.call(self.store, 'tasks.auto_resume', {'enabled': True}, source='mcp')
        self.assertFalse(result.ok)
        self.store.put('waits_enabled', False)
        result = control_api.call(self.store, 'tasks.auto_resume', {'enabled': True})
        self.assertFalse(result.ok)
        self.store.put('waits_enabled', True)
        result = control_api.call(self.store, 'tasks.auto_resume', {'enabled': True})
        self.assertTrue(result.ok, result.text)
        self.assertTrue(self.store.get('auto_resume'))
        result = control_api.call(self.store, 'waits.set_enabled', {'enabled': False})
        self.assertFalse(result.ok)
        self.assertTrue(self.store.get('waits_enabled'))
