import asyncio
import json
import time
import unittest
from unittest.mock import patch

from coordinator import opencode_subscription, opencode_runtime
from coordinator.service import Service
from tests import test_opencode_subscription, test_opencode_runtime
from tests.fake_opencode import SESSION_ID


class WorkerRecoveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.t = test_opencode_runtime.OpenCodeRuntimeTests()
        self.t.addCleanup = self.addCleanup
        self.t.setUp()
        self.store = self.t.store
        self.pool, self.task = self.t.make_pool()
        self.service = Service(self.store, None, self.t.runner, self.t.cwd)
        self.service.workers = self.pool

    def notices(self):
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM messages WHERE kind='callback'").fetchone()[0], 0)
        rows = self.store.db.execute("SELECT topic,text FROM outbox WHERE text LIKE 'Worker % is paused%'").fetchall()
        self.assertTrue(all(row['topic'] == self.store.get('coordinator_home_topic') for row in rows))
        return [row['text'] for row in rows]

    def prompts(self):
        return ['\n\n'.join(part['text'] for part in call['message']['params']['prompt'])
                for call in self.t.calls() if call['message'].get('method') == 'session/prompt']

    async def recheck(self, when):
        with patch('coordinator.opencode_runtime.time.time', return_value=when):
            await self.service.workers_once()
            await asyncio.wait_for(asyncio.gather(*self.service.worker_tasks.values()), 30)

    async def persistent_refusal(self, status, **scenario):
        self.t.scenario(http_error=status, **scenario)
        wid = self.t.spawn(self.task).data['worker']
        await self.pool.run(self.pool.get(wid), self.store.topic('1:2'))
        self.assertEqual(len(self.notices()), 1)
        for delay in (300, 600, 1200, 2400, 3600, 3600):
            block = self.store.get('opencode_account_blocks')['one']
            self.assertEqual(block['until'] - block['observed_at'], delay)
            before = len(self.prompts())
            await self.recheck(block['until'] - 1)
            self.assertEqual(len(self.prompts()), before)
            await self.recheck(block['until'] + 1)
            self.assertEqual(len(self.notices()), 1)
            self.assertEqual((self.pool.get(wid)['status'], self.pool.get(wid)['session']),
                             ('waiting_for_quota', SESSION_ID))
            self.assertEqual(sum('short prompt' in text for text in self.prompts()), 1)
            self.assertTrue(all(len(text) <= 100 for text in self.prompts()[1:]))
            sessions = json.loads((self.store.directory / 'opencode/workers' / str(wid) /
                                   'data/fake-sessions.json').read_text())
            self.assertEqual(sum('sole authorized owner' in text for text in sessions[SESSION_ID]), 1)
        self.t.scenario(history_reply=True)
        await self.recheck(self.store.get('opencode_account_blocks')['one']['until'] + 1)
        done = self.pool.get(wid)
        self.assertEqual((done['status'], done['session'], done['account_alias']), ('done', SESSION_ID, 'one'))
        self.assertTrue(json.loads(done['result'])['success'])
        self.assertEqual(len(self.notices()), 1)
        self.assertEqual(sum('short prompt' in text for text in self.prompts()), 1)
        self.assertNotIn('one', self.store.get('opencode_account_blocks'))
        self.t.scenario(http_error=status, **scenario)
        second = self.t.spawn(self.task).data['worker']
        await self.pool.run(self.pool.get(second), self.store.topic('1:2'))
        block = self.store.get('opencode_account_blocks')['one']
        self.assertEqual(block['until'] - block['observed_at'], 300)
        self.assertEqual(len(self.notices()), 2)

    async def test_credit_rechecks_keep_one_notice_and_one_brief_with_capped_backoff(self):
        await self.persistent_refusal(402)

    async def test_weekly_rechecks_keep_one_notice_and_one_brief_with_capped_backoff(self):
        body = dict(test_opencode_runtime.WEEKLY_REFUSAL)
        body.pop('resetAt')
        await self.persistent_refusal(429, response_body=json.dumps(body))

    async def assert_launch_floor(self, **scenario):
        self.t.scenario(http_error=429, **scenario)
        wid = self.t.spawn(self.task).data['worker']
        before = time.time()
        await self.pool.run(self.pool.get(wid), self.store.topic('1:2'))
        block = self.store.get('opencode_account_blocks')['one']
        self.assertGreaterEqual(block['until'], before + 300)
        self.assertFalse(block['reset_known'])
        await self.recheck(block['observed_at'] + 299)
        self.assertEqual(len(self.prompts()), 1)
        self.assertEqual(len(self.notices()), 1)

    async def test_past_weekly_reset_has_a_launch_floor(self):
        body = dict(test_opencode_runtime.WEEKLY_REFUSAL, resetAt=int((time.time() - 30) * 1000))
        await self.assert_launch_floor(response_body=json.dumps(body))

    async def test_retry_after_zero_has_a_launch_floor(self):
        await self.assert_launch_floor(response_headers={'Retry-After': '0'})

    async def test_owner_follow_up_during_wait_is_sent_once_without_the_brief(self):
        self.t.scenario(http_error=402)
        wid = self.t.spawn(self.task).data['worker']
        await self.pool.run(self.pool.get(wid), self.store.topic('1:2'))
        with self.store.db:
            self.store.db.execute("UPDATE workers SET prompt=prompt || ? WHERE id=?",
                                  ('\n\nFollow-up:\nUse the blue option.', wid))
        for index in range(2):
            await self.recheck(self.store.get('opencode_account_blocks')['one']['until'] + 1)
        self.assertEqual(sum('short prompt' in text for text in self.prompts()), 1)
        self.assertEqual(sum('Use the blue option.' in text for text in self.prompts()), 1)
        self.assertEqual(len(self.notices()), 1)

    def test_provider_marker_reader_rejects_a_different_launch(self):
        root = self.t.root / 'marker'
        root.mkdir()
        launch = root / 'guard-launch.json'
        launch.write_text(json.dumps({'version': 1, 'launch': 'a' * 32}))
        opencode_runtime.prepare_error(root, SESSION_ID)
        marker = root / 'provider-error.json'
        value = json.loads(marker.read_text())
        error = {'name': 'APIError', 'status': 402, 'code': None, 'reset_at': None, 'message': 'No credit'}
        value['error'] = error
        marker.write_text(json.dumps(value))
        self.assertEqual(opencode_runtime.parent_error(root, SESSION_ID), error)
        launch.write_text(json.dumps({'version': 1, 'launch': 'b' * 32}))
        self.assertIsNone(opencode_runtime.parent_error(root, SESSION_ID))
        launch.write_text(json.dumps({'version': 1, 'launch': 'a' * 32}))
        self.assertEqual(opencode_runtime.parent_error(root, SESSION_ID), error)


class GoWorkerRecoveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.t = test_opencode_subscription.OpenCodeSubscriptionTests()
        self.t.addCleanup = self.addCleanup
        self.t.setUp()

    async def test_subscription_credit_refresh_keeps_the_launch_floor_and_backoff(self):
        self.t.scenario(http_error=402)
        wid = self.t.spawn()
        await self.t.run_worker(wid)
        for delay in (300, 600):
            block = self.t.store.get('opencode_account_blocks')['go']
            self.assertEqual(block['until'] - block['observed_at'], delay)
            before = sum(call['message'].get('method') == 'initialize' for call in self.t.calls())
            with patch('coordinator.opencode_subscription.time.time', return_value=block['until'] - 1):
                await opencode_subscription.refresh_accounts(self.t.store)
                await self.t.service.workers_once()
                await asyncio.gather(*self.t.service.worker_tasks.values())
            self.assertEqual(self.t.store.get('opencode_account_blocks')['go'], block)
            self.assertEqual(sum(call['message'].get('method') == 'initialize' for call in self.t.calls()), before)
            with patch('coordinator.opencode_subscription.time.time', return_value=block['until'] + 1):
                await self.t.resume(wid)
        self.assertEqual(self.t.pool.get(wid)['status'], 'waiting_for_quota')
        self.assertEqual(self.t.store.db.execute("SELECT count(*) FROM messages WHERE kind='callback'").fetchone()[0], 0)
        self.assertEqual(self.t.store.db.execute("SELECT count(*) FROM outbox WHERE topic='1:2'").fetchone()[0], 1)
        block = self.t.store.get('opencode_account_blocks')['go']
        before = len(self.t.requests)
        with patch('coordinator.opencode_subscription.time.time', return_value=block['until'] + 1):
            monitor = asyncio.create_task(opencode_subscription.monitor_accounts(self.t.store))
            try:
                await asyncio.sleep(4.2)
            finally:
                monitor.cancel()
                await asyncio.gather(monitor, return_exceptions=True)
            self.assertEqual(opencode_subscription.launch_state(self.t.store, 'go'), 'ready')
        self.assertEqual(len(self.t.requests) - before, 1)
