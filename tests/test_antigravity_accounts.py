"""Offline Antigravity usage, account selection, quota waiting and cards."""

import asyncio
import json
import time
import unittest
from unittest.mock import patch

from coordinator import antigravity_accounts, control_api
from coordinator.accounts import AccountBroker
from coordinator.providers import RunResult
from coordinator.workers import WorkerPool
from tests.fake_agy import QUOTA, sign_in
from tests.test_antigravity_runtime import AntigravityCase, capture


def quota(**remaining):
    """Usage data with one weekly bucket per named group. Resets are one day ahead."""
    reset = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(time.time() + 86400))
    return {'groups': [{'name': name.replace('_', ' '), 'buckets': [
        {'id': name + '-weekly', 'window': 'weekly', 'remaining_fraction': value, 'reset_time': reset}]}
        for name, value in remaining.items()]}


class AntigravityUsageTests(AntigravityCase):
    def setUp(self):
        super().setUp()
        self.register()
        self.broker = antigravity_accounts.AntigravityBroker(self.store)

    def add(self, alias):
        sign_in(self.account.parent / alias)
        self.assertTrue(self.cli('antigravity.account', alias=alias).ok)

    def test_parse_reads_the_live_usage_reply(self):
        document = capture('usage')[0]['msg']
        groups = antigravity_accounts.parse_usage(document)
        self.assertEqual([(group['name'], [(b['id'], b['window'], b['remaining']) for b in group['buckets']])
                          for group in groups],
                         [(document['command']['data']['groups'][0]['name'], [('gemini-weekly', 'weekly', 0.9310864210128784)]),
                          (document['command']['data']['groups'][1]['name'], [('3p-weekly', 'weekly', 0.9792940020561218)])])
        self.assertEqual(groups[0]['buckets'][0]['resets_at'], 1795673963.0)
        for broken in ({}, {'status': 'ERROR'}, {'status': 'SUCCESS', 'command': {'name': 'model', 'data': {}}},
                       {'status': 'SUCCESS', 'command': {'name': 'usage', 'data': {'groups': [{'buckets': [
                           {'remaining_fraction': 2}, {'remaining_fraction': 'x'}, 'bad']}, 'bad']}}}):
            with self.subTest(broken=broken):
                self.assertIsNone(antigravity_accounts.parse_usage(broken))

    async def test_usage_comes_from_the_official_command_with_the_account_home(self):
        status = await antigravity_accounts.refresh_usage(self.store, 'one')
        call = self.calls()[-1]
        self.assertEqual((call['argv'], call['home']), (['-p', '/usage', '--output-format', 'json'], str(self.account)))
        self.assertEqual([(bucket['window'], bucket['remaining']) for bucket in status['usage']['groups'][0]['buckets']],
                         [('5h', QUOTA['groups'][0]['buckets'][0]['remaining_fraction']),
                          ('weekly', QUOTA['groups'][0]['buckets'][1]['remaining_fraction'])])
        self.assertEqual(self.store.get('antigravity_account_status')['one']['usage'], status['usage'])

    async def test_failed_usage_read_keeps_the_last_snapshot_and_sign_out_is_recorded(self):
        first = await antigravity_accounts.refresh_usage(self.store, 'one')
        self.scenario(usage_text='not json', usage_exit=1)
        second = await antigravity_accounts.refresh_usage(self.store, 'one')
        self.assertEqual((second['usage'], second['logged_in']), (first['usage'], True))
        (self.account / 'signed-in').unlink()
        third = await antigravity_accounts.refresh_usage(self.store, 'one')
        self.assertEqual((third['usage'], third['logged_in']), (first['usage'], False))
        self.assertIsNone(self.broker.select())

    async def test_selection_prefers_the_active_account_then_the_most_usage_left(self):
        self.add('two')
        self.add('three')
        self.scenario(usage_by_home={'one': quota(a=0.2), 'two': quota(a=0.4), 'three': quota(a=0.9, b=0.0)})
        for alias in ('one', 'two', 'three'):
            await antigravity_accounts.refresh_usage(self.store, alias)
        self.assertEqual(self.broker.selection_order(), ['one', 'three', 'two'])
        self.scenario(usage_by_home={'one': quota(a=0.0, b=0.0), 'two': quota(a=0.4), 'three': quota(a=0.9)})
        await antigravity_accounts.refresh_usage(self.store, 'one')
        self.assertEqual((self.broker.select(), self.broker.select('one'), self.broker.held('one')), ('three', None, True))
        self.assertAlmostEqual(self.broker.earliest_reset('one'), time.time() + 86400, delta=5)
        self.assertEqual(self.broker.select('two'), 'two')

    async def test_a_usage_refusal_holds_the_account_until_the_empty_bucket_resets(self):
        self.scenario(usage=quota(a=0.0, b=0.6))
        refused = RunResult(None, quota_limited=True, failure_code='quota_limited')
        await self.broker.record_result('one', refused)
        until = self.store.get('antigravity_account_blocks')['one']
        self.assertAlmostEqual(until, time.time() + 86400, delta=5)
        self.assertEqual(refused.rate_limit_info, {'status': 'rejected', 'resets_at': until})
        self.assertEqual((self.broker.held('one'), self.broker.capacity(None)), (True, False))
        await antigravity_accounts.refresh_usage(self.store, 'one')
        self.assertTrue(self.broker.held('one'))
        self.scenario(usage=quota(a=0.3, b=0.6))
        await antigravity_accounts.refresh_usage(self.store, 'one')
        self.assertEqual((self.broker.held('one'), self.store.get('antigravity_account_blocks')), (False, {}))
        self.scenario(usage=quota(a=0.5))
        await self.broker.record_result('one', RunResult(None, quota_limited=True))
        self.assertAlmostEqual(self.store.get('antigravity_account_blocks')['one'],
                               time.time() + antigravity_accounts.RECHECK_SECONDS, delta=5)
        await self.broker.record_result('one', RunResult(None, success=True))
        self.assertEqual(self.store.get('antigravity_account_blocks'), {})

    async def test_monitor_reads_new_accounts_and_skips_signed_out_ones(self):
        self.add('two')
        statuses = self.store.get('antigravity_account_status')
        statuses['two']['logged_in'] = False
        self.store.put('antigravity_account_status', statuses)
        with patch('coordinator.antigravity_accounts.asyncio.sleep', side_effect=asyncio.CancelledError):
            with self.assertRaises(asyncio.CancelledError):
                await antigravity_accounts.monitor(self.store)
        self.assertEqual([call['home'] for call in self.calls()], [str(self.account)])
        with patch('coordinator.antigravity_accounts.asyncio.sleep', side_effect=asyncio.CancelledError):
            with self.assertRaises(asyncio.CancelledError):
                await antigravity_accounts.monitor(self.store)
        self.assertEqual(len(self.calls()), 1)

    async def test_monitor_keeps_running_after_one_account_fails(self):
        self.add('two')
        seen = []

        async def refresh(store, alias):
            seen.append(alias)
            if alias == 'one':
                raise antigravity_accounts.Refused('Antigravity needs writable private home folders.')
            return await real(store, alias)

        real = antigravity_accounts.refresh_usage
        with patch('coordinator.antigravity_accounts.refresh_usage', refresh), \
                patch('coordinator.antigravity_accounts.asyncio.sleep', side_effect=[None, asyncio.CancelledError]):
            with self.assertRaises(asyncio.CancelledError):
                await antigravity_accounts.monitor(self.store)
        self.assertEqual(seen, ['one', 'two'], 'a failed account waits for its backoff')
        self.assertIn('usage', self.store.get('antigravity_account_status')['two'])
        rows = self.store.db.execute("SELECT area,code,detail FROM problems WHERE code='antigravity-usage-failed'").fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['area'], 'accounts')
        self.assertRegex(rows[0]['detail'], r'^item=one failures=1 Refused: Antigravity needs writable private home '
                                            r'folders\. at test_antigravity_accounts\.py:\d+ refresh')

    def test_gemini_week_prefers_the_gemini_models_group(self):
        bucket = lambda window, left: {'id': window, 'window': window, 'remaining': left, 'resets_at': None}
        usage = {'groups': [{'name': 'Claude and GPT models', 'buckets': [bucket('weekly', 1.0)]},
                            {'name': 'Gemini 4 Argon', 'buckets': [bucket('weekly', 0.5)]},
                            {'name': 'Gemini Models', 'buckets': [bucket('5h', 0.2), bucket('weekly', 0.9)]}]}
        self.assertEqual(antigravity_accounts.gemini_week(usage)['remaining'], 0.9)
        self.assertEqual(antigravity_accounts.gemini_week({'groups': usage['groups'][:2]})['remaining'], 0.5)
        self.assertIsNone(antigravity_accounts.gemini_week({'groups': usage['groups'][:1]}))

    async def test_active_marker_shows_only_with_several_accounts(self):
        from coordinator import control_ui
        self.scenario(usage_by_home={'one': quota(Gemini_Models=0.9), 'two': quota(Gemini_Models=0.8)})
        await antigravity_accounts.refresh_usage(self.store, 'one')
        self.assertEqual(antigravity_accounts.health(self.store).splitlines()[1], '🟢 one: 90%, week resets in 23h 59m')
        self.assertNotIn('· active', control_ui._accounts_view(self.store, 0)[0])
        self.add('two')
        await antigravity_accounts.refresh_usage(self.store, 'two')
        self.assertEqual(antigravity_accounts.health(self.store).splitlines()[1:],
                         ['🟢 one: 90%, week resets in 23h 59m · active', '🟢 two: 80%, week resets in 23h 59m'])
        self.assertIn('🟢 one: 90%, week resets in 23h 59m · active\n', control_ui._accounts_view(self.store, 0)[0])

    async def test_cards_show_usage_and_reset_times(self):
        self.add('two')
        self.scenario(usage_by_home={'one': quota(Gemini_Models=0.93, Claude_and_GPT_models=0.0),
                                     'two': quota(Gemini_Models=0.0)})
        self.assertEqual(antigravity_accounts.health(self.store).splitlines()[1],
                         '🟡 one: ?, usage not read yet · active')
        for alias in ('one', 'two'):
            await antigravity_accounts.refresh_usage(self.store, alias)
        statuses = self.store.get('antigravity_account_status')
        statuses['one']['tier'] = 'Fixture Plan'
        self.store.put('antigravity_account_status', statuses)
        self.assertEqual(antigravity_accounts.health(self.store).splitlines(), [
            '**Antigravity left** (week)',
            '🟢 one: 93%, week resets in 23h 59m · active',
            '🔴 two: 0%, back in 23h 59m'])
        card = antigravity_accounts.card(self.store, 'one').splitlines()
        self.assertEqual(card[:2], ['Antigravity one · active', 'Plan: Fixture Plan'])
        self.assertRegex(card[2], r'^Gemini Models, week: 93% left; resets \d{4}-\d\d-\d\d \d\d:\d\d:\d\d UTC$')
        self.assertEqual(antigravity_accounts.card(self.store, 'two').splitlines()[-1],
                         'At its usage limit; resets in 23h 59m.')
        listed = self.cli('accounts.list').text
        self.assertIn('Antigravity one · active', listed)
        self.assertIn('**Antigravity left** (week)', control_api.call(self.store, 'health.show', {}).text)
        from coordinator.controls import _help
        self.assertIn('🔴 two: 0%, back in', _help(self.store))


class AntigravityQuotaWaitTests(AntigravityCase):
    def setUp(self):
        super().setUp()
        self.register()
        self.task = self.job()
        self.pool = WorkerPool(self.store, self.runner, AccountBroker(self.store))
        self.broker = self.pool.broker('antigravity')

    async def run_spawned(self, **options):
        spawned = self.spawn(self.task, **options)
        self.assertTrue(spawned.ok, spawned.text)
        return await asyncio.wait_for(self.pool.run(self.pool.get(spawned.data['worker']), self.store.topic('1:2')), 30)

    async def test_a_refused_worker_waits_and_new_work_moves_to_the_next_account(self):
        self.scenario(quota=True, usage=quota(a=0.0))
        worker = await self.run_spawned()
        result = json.loads(worker['result'])
        self.assertEqual((worker['status'], result['failure_code'], worker['account_alias']),
                         ('waiting_for_quota', 'quota_limited', 'one'))
        self.assertAlmostEqual(result['waiting_until'], time.time() + 86400, delta=10)
        notices = [row['text'] for row in self.store.db.execute("SELECT text FROM outbox WHERE topic='1:2'")]
        self.assertTrue(any(text.startswith('Worker %d is waiting for Antigravity usage to reset at ' % worker['id'])
                            for text in notices), notices)
        self.assertFalse(await self.broker.ready(dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (worker['id'],)).fetchone())))
        refused = self.spawn(self.task, prompt='more work')
        self.assertEqual((refused.ok, refused.text), (False, 'No Antigravity account is signed in and available.'))
        sign_in(self.account.parent / 'two')
        self.assertTrue(self.cli('antigravity.account', alias='two').ok)
        self.scenario(usage_by_home={'one': quota(a=0.0), 'two': quota(a=0.8)})
        self.assertEqual(self.broker.select(), 'two')
        self.assertFalse(await self.broker.ready(dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (worker['id'],)).fetchone())))
        second = await self.run_spawned(prompt='more work')
        self.assertEqual((second['status'], second['account_alias'], json.loads(second['result'])['success']),
                         ('done', 'two', True))

    async def test_a_waiting_session_stays_on_its_own_account(self):
        first = await self.run_spawned()
        self.assertEqual((first['status'], first['account_alias']), ('done', 'one'))
        sign_in(self.account.parent / 'two')
        self.assertTrue(self.cli('antigravity.account', alias='two').ok)
        self.store.put('antigravity_account_blocks', {'one': time.time() + 3600})
        with self.store.db:
            self.store.db.execute("UPDATE workers SET status='queued',prompt='again',host=NULL WHERE id=?", (first['id'],))
        before = len(self.calls('initialize'))
        waiting = await asyncio.wait_for(self.pool.run(self.pool.get(first['id']), self.store.topic('1:2')), 30)
        self.assertEqual((waiting['status'], waiting['account_alias'], waiting['session']),
                         ('waiting_for_quota', 'one', first['session']))
        self.assertEqual(len(self.calls('initialize')), before)
        self.assertFalse(await self.broker.ready(dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (first['id'],)).fetchone())))
        self.assertAlmostEqual(json.loads(waiting['result'])['waiting_until'], time.time() + 3600, delta=10)
        self.store.put('antigravity_account_blocks', {})
        self.assertTrue(await self.broker.ready(dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (first['id'],)).fetchone())))


if __name__ == '__main__':
    unittest.main()
