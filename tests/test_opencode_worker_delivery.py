import asyncio
import json
import time
import unittest
from unittest.mock import patch

from coordinator import control_api, opencode_subscription, opencode_runtime
from coordinator.service import Service
from coordinator.session import AccountsUnavailable
from tests import test_opencode_subscription, test_opencode_runtime


FOLLOW_UP = 'Owner follow-up: also add the blue option.'


class WorkerDeliveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.t = test_opencode_runtime.OpenCodeRuntimeTests()
        self.t.addCleanup = self.addCleanup
        self.t.setUp()
        self.store = self.t.store
        self.pool, self.task = self.t.make_pool()
        self.service = Service(self.store, None, self.t.runner, self.t.cwd)
        self.service.workers = self.pool

    async def settle(self):
        await asyncio.wait_for(asyncio.gather(*self.service.worker_tasks.values()), 60)

    async def at(self, when):
        with patch('coordinator.opencode_runtime.time.time', return_value=when):
            await self.service.workers_once()
            await self.settle()

    def block(self):
        return dict(self.store.get('opencode_account_blocks')['one'])

    def prompts(self):
        return ['\n\n'.join(part['text'] for part in call['message']['params']['prompt'])
                for call in self.t.calls() if call['message'].get('method') == 'session/prompt']

    async def start(self, **scenario):
        self.t.scenario(unique_sessions=True, **scenario)
        wid = self.t.spawn(self.task).data['worker']
        await self.pool.run(self.pool.get(wid), self.store.topic('1:2'))
        return wid

    async def steer_while_paused(self, pause):
        wid = await self.start()
        self.assertEqual(self.pool.get(wid)['status'], 'done')
        if pause == 'chat':
            opencode_runtime.record_parent_block(self.store, 'one', 'out_of_credit')
        else:
            self.task = self.store.task_create('1:2', 'Other job', worktree=str(self.t.cwd))
            other = await self.start(http_error=402)
            self.assertNotEqual(self.pool.get(wid)['session'], self.pool.get(other)['session'])
        before = len(self.prompts())
        called = control_api.call(self.store, 'workers.steer', {'worker': wid, 'prompt': FOLLOW_UP})
        self.assertTrue(called.ok, called.text)
        await self.service.controls_once()
        await self.service.workers_once()
        await self.settle()
        parked = self.pool.get(wid)
        self.assertEqual((parked['status'], len(self.prompts())), ('waiting_for_quota', before))
        self.t.scenario(history_reply=True)
        await self.at(self.block()['until'] + 1)
        done = self.pool.get(wid)
        result = json.loads(done['result'])
        self.assertEqual((done['status'], done['session']), ('done', parked['session']))
        self.assertTrue(result['success'], result)
        self.assertIn(FOLLOW_UP, result['text'])
        self.assertEqual(sum(FOLLOW_UP in text for text in self.prompts()), 1)

    async def test_follow_up_survives_the_chats_account_pause(self):
        await self.steer_while_paused('chat')

    async def test_follow_up_survives_another_workers_account_pause(self):
        await self.steer_while_paused('worker')

    async def hour_wait(self):
        wid = await self.start(http_error=402)
        for _ in range(4):
            await self.at(self.block()['until'] + 1)
        self.assertEqual(self.block()['until'] - self.block()['observed_at'], 3600)
        return wid

    async def test_stopped_workers_backoff_does_not_leak_to_a_later_worker(self):
        wid = await self.hour_wait()
        block = self.block()
        self.store.service_request('workers.stop', {'worker': wid})
        await self.service.controls_once()
        self.assertEqual(self.pool.get(wid)['status'], 'interrupted')
        with patch('coordinator.opencode_runtime.time.time', return_value=block['until'] + 3 * 86400):
            other = await self.start(http_error=429)
        self.assertEqual(self.pool.get(other)['status'], 'waiting_for_quota')
        fresh = self.block()
        self.assertEqual(fresh['until'] - fresh['observed_at'], 300)

    async def test_an_unowned_legacy_block_cannot_supply_a_workers_retry_step(self):
        now = time.time()
        self.store.put('opencode_account_blocks', {'one': {
            'until': now + 3600, 'observed_at': now, 'retry_seconds': 3600,
            'reason': 'out_of_credit', 'reset_known': False}})
        wid = await self.start(http_error=429)
        self.assertEqual(self.pool.get(wid)['status'], 'waiting_for_quota')
        self.assertEqual(self.prompts(), [])
        await self.at(now + 3601)
        self.assertEqual(len(self.prompts()), 1)
        block = self.block()
        self.assertEqual(block['until'] - block['observed_at'], 300)

    async def test_chat_retry_keeps_a_workers_own_backoff(self):
        from coordinator import parent
        wid = await self.start(http_error=402)
        block = self.block()
        self.store.put('coordinator_account_notice', {
            'key': ['opencode', 'one', block['until'], 'out_of_credit']})
        await parent.retry_paused(self.store, 'coordinator_account')
        self.assertEqual(opencode_runtime.parent_ready(self.store), 'one')
        before = len(self.prompts())
        await self.service.workers_once()
        await self.settle()
        self.assertEqual(self.pool.get(wid)['status'], 'waiting_for_quota')
        self.assertEqual(len(self.prompts()), before)
        self.assertEqual(self.block()['until'], block['until'])

    async def test_chat_keeps_its_own_five_minute_schedule(self):
        await self.hour_wait()
        block = self.block()
        now = block['observed_at'] + 1
        with patch('coordinator.opencode_runtime.time.time', return_value=now):
            with self.assertRaises(AccountsUnavailable) as caught:
                opencode_runtime.parent_ready(self.store)
            self.service.pause_parent('1:2', caught.exception)
        self.assertEqual(caught.exception.reset_at - block['observed_at'], 300)
        self.assertEqual(self.store.get('coordinator_account_retry_at') - now, 299)
        self.assertIn('every five minutes', self.store.get('coordinator_account_notice')['text'])
        with patch('coordinator.opencode_runtime.time.time', return_value=now + 300):
            self.assertEqual(opencode_runtime.parent_ready(self.store), 'one')

    async def test_another_worker_does_not_inherit_an_active_workers_hour_backoff(self):
        wid = await self.hour_wait()
        block = self.block()
        self.task = self.store.task_create('1:2', 'Concurrent job', worktree=str(self.t.cwd))
        with patch('coordinator.opencode_runtime.time.time', return_value=block['observed_at'] + 301):
            other = await self.start(http_error=429)
        self.assertNotEqual(wid, other)
        fresh = self.block()
        self.assertEqual((self.pool.get(wid)['status'], self.pool.get(other)['status']),
                         ('waiting_for_quota', 'waiting_for_quota'))
        self.assertEqual(fresh['until'] - fresh['observed_at'], 300)
        self.assertEqual(fresh['reason'], 'rate_limited')

    async def test_chat_refusal_does_not_reset_a_workers_continuous_wait(self):
        wid = await self.start(http_error=402)
        await self.at(self.block()['until'] + 1)
        block = self.block()
        before = len(self.prompts())
        with patch('coordinator.opencode_runtime.time.time', return_value=block['observed_at'] + 1):
            opencode_runtime.record_parent_block(self.store, 'one', 'out_of_credit')
        await self.at(block['observed_at'] + 302)
        self.assertEqual(len(self.prompts()), before)
        await self.at(block['until'] + 1)
        self.assertEqual(self.pool.get(wid)['status'], 'waiting_for_quota')
        next_block = self.block()
        self.assertEqual(next_block['until'] - next_block['observed_at'], 1200)

    async def test_home_pause_notice_states_the_deadline_with_a_comma(self):
        await self.start(http_error=402)
        block = self.block()
        topic, text = self.store.db.execute('SELECT topic,text FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
        self.assertEqual(topic, self.store.get('coordinator_home_topic'))
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM messages WHERE kind='callback'").fetchone()[0], 0)
        self.assertIn(time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(block['until'])), text)
        self.assertIn(', and resumes it automatically.', text)


class GoWorkerDeliveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.t = test_opencode_subscription.OpenCodeSubscriptionTests()
        self.t.addCleanup = self.addCleanup
        self.t.setUp()

    async def test_chat_usage_failure_rechecks_without_inheriting_a_workers_hour_wait(self):
        self.t.scenario(http_error=402)
        wid = self.t.spawn()
        await self.t.run_worker(wid)
        for _ in range(4):
            block = self.t.store.get('opencode_account_blocks')['go']
            with patch('coordinator.opencode_subscription.time.time', return_value=block['until'] + 1):
                await self.t.resume(wid)
        block = self.t.store.get('opencode_account_blocks')['go']
        self.assertEqual(block['until'] - block['observed_at'], 3600)
        now = block['observed_at'] + 301
        self.t.http_status = 503
        before = sum(call['message'].get('method') == 'initialize' for call in self.t.calls())
        with patch('coordinator.opencode_subscription.time.time', return_value=now):
            await opencode_subscription.refresh_accounts(self.t.store)
            with self.assertRaises(AccountsUnavailable) as caught:
                opencode_runtime.parent_ready(self.t.store)
            self.t.service.pause_parent('1:2', caught.exception)
        self.assertEqual(caught.exception.reset_at - now, 300)
        self.assertEqual(self.t.store.get('coordinator_account_retry_at') - now, 300)
        self.assertEqual(self.t.store.get('opencode_account_blocks')['go']['until'], block['until'])
        self.assertEqual(sum(call['message'].get('method') == 'initialize' for call in self.t.calls()), before)

    async def test_follow_up_survives_a_subscription_usage_pause_before_launch(self):
        self.t.scenario()
        wid = self.t.spawn()
        done = await self.t.run_worker(wid)
        self.assertEqual(done['status'], 'done')
        self.t.payload = self.t.usage_payload(limited=True)
        await opencode_subscription.refresh_accounts(self.t.store)
        block = self.t.store.get('opencode_account_blocks')['go']
        before = sum(call['message'].get('method') == 'session/prompt' for call in self.t.calls())
        called = self.t.call('workers.steer', worker=wid, prompt=FOLLOW_UP)
        self.assertTrue(called.ok, called.text)
        await self.t.service.controls_once()
        await self.t.service.workers_once()
        await asyncio.gather(*self.t.service.worker_tasks.values())
        self.assertEqual(self.t.pool.get(wid)['status'], 'waiting_for_quota')
        self.assertEqual(sum(call['message'].get('method') == 'session/prompt' for call in self.t.calls()), before)
        self.t.payload = self.t.usage_payload()
        self.t.scenario(history_reply=True)
        with patch('coordinator.opencode_subscription.time.time', return_value=block['until'] + 1):
            await self.t.resume(wid)
        resumed = self.t.pool.get(wid)
        result = json.loads(resumed['result'])
        self.assertEqual((resumed['status'], resumed['session']), ('done', done['session']))
        self.assertTrue(result['success'])
        self.assertIn(FOLLOW_UP, result['text'])
