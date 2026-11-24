import json
import logging
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from coordinator import control_api
from coordinator.account_status import SAME_WINDOW_SECONDS, refresh_accounts
from coordinator.accounts import (LIMIT_CONTINUATION, AccountBroker, account_block, account_label, account_pick,
                                  discover_accounts, listed_accounts, switch_threshold)
from coordinator.failures import TaskFailure
from coordinator.providers import RunResult
from coordinator.store import Store


class AccountTestsSupport:
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store = Store(self.root / 'state')
        self.sid = '20000000-0000-4000-8000-000000000001'
        now = time.time()
        accounts = {}
        status = {}
        for alias, email, reset in (('primary', 'primary@example.com', 3600),
                                    ('second', 'second@example.com', 7200)):
            directory = self.root / alias
            directory.mkdir()
            accounts[alias] = {'config_dir': str(directory), 'enabled': True}
            if alias == 'primary':
                transcript = directory / 'projects' / 'project-hash' / (self.sid + '.jsonl')
                transcript.parent.mkdir(parents=True)
                transcript.write_text('{}\n')
            status[alias] = {'identity': {'email': email, 'logged_in': True},
                             'observed_at': now,
                             'usage': {'five_hour': {'utilization': 10, 'resets_at': now + 1800},
                                       'seven_day': {'utilization': 10, 'resets_at': now + reset},
                                       'seven_day_fable': {'utilization': 10, 'resets_at': now + reset}}}
        with self.store.db:
            self.store.put('accounts', accounts)
            self.store.put('account_status', status)
        self.transcript = self.root / 'primary' / 'projects' / 'project-hash' / (self.sid + '.jsonl')
        with self.store.db:
            self.store.put('session_transcripts', {self.sid: str(self.transcript)})
        self.broker = AccountBroker(self.store)

    async def asyncTearDown(self):
        self.store.close()
        self.temp.cleanup()

    def meter(self, alias, key, utilization, resets_at):
        status = self.store.get('account_status')
        status[alias]['usage'][key] = {'utilization': utilization, 'resets_at': resets_at}
        with self.store.db:
            self.store.put('account_status', status)


class ModelAwareAccountTests(AccountTestsSupport, unittest.IsolatedAsyncioTestCase):
    async def test_selection_uses_model_and_owner_pick(self):
        reset = time.time() + 3600
        self.store.put('claude_account_pick', 'primary')
        self.meter('primary', 'seven_day_fable', 99, reset)
        for model in ('opus', 'sonnet', 'haiku', 'claude-opus-5-5'):
            self.assertEqual(self.broker.select(model=model), 'primary')
        for model in ('fable', 'claude-fable-5-2', 'native-default'):
            self.assertEqual(self.broker.select(model=model), 'second')
        self.assertIsNone(self.broker.earliest_reset(model='opus'))
        self.assertEqual(self.broker.earliest_reset(model='fable'), reset)
        self.meter('primary', 'seven_day', 99, reset)
        for model in ('opus', 'fable', 'native-default'):
            self.assertEqual(self.broker.select(model=model), 'second')

    def third_account(self):
        directory = self.root / 'third'
        directory.mkdir()
        now = time.time()
        accounts = self.store.get('accounts')
        accounts['third'] = {'config_dir': str(directory), 'enabled': True}
        status = self.store.get('account_status')
        status['third'] = {'identity': {'email': 'third@example.com', 'logged_in': True}, 'observed_at': now,
                           'usage': {'seven_day': {'utilization': 10, 'resets_at': now + 600},
                                     'seven_day_fable': {'utilization': 10, 'resets_at': now + 600}}}
        with self.store.db:
            self.store.put('accounts', accounts)
            self.store.put('account_status', status)

    async def test_per_model_picks_send_fable_and_other_models_to_their_accounts(self):
        self.third_account()
        self.assertEqual(self.broker.select(model='opus'), 'third')
        self.store.put('claude_account_pick', {'fable': 'primary', 'other': 'second'})
        for model in ('fable', 'claude-fable-5-2'):
            self.assertEqual(self.broker.select(model=model), 'primary')
        for model in ('opus', 'sonnet', 'claude-opus-5-5', 'claude-sonnet-5-5', 'native-default'):
            self.assertEqual(self.broker.select(model=model), 'second')
        self.meter('primary', 'seven_day_fable', 99, time.time() + 3600)
        self.assertEqual(self.broker.select(model='fable'), 'third')
        self.assertEqual(self.broker.select(model='opus'), 'second')
        self.assertEqual(self.broker.selection_reason(self.store.get('accounts'), 'third', model='opus'),
                         'reason=owner_pick')

    async def test_old_single_pick_sends_every_model_to_one_account(self):
        self.third_account()
        self.store.put('claude_account_pick', 'second')
        for model in ('fable', 'opus', 'sonnet', 'native-default'):
            self.assertEqual(self.broker.select(model=model), 'second')

    async def test_one_group_pick_leaves_other_group_automatic(self):
        self.third_account()
        self.store.put('claude_account_pick', {'fable': 'second', 'other': None})
        self.assertEqual(self.broker.select(model='fable'), 'second')
        self.assertEqual(self.broker.select(model='opus'), 'third')

    async def test_fable_rate_limit_blocks_only_fable(self):
        reset = time.time() + 3600
        self.broker.record_rate_limit('primary', {'rateLimitType': 'seven_day_fable',
                                                  'resetsAt': reset}, rejected=True)
        self.assertEqual(self.broker.select(model='opus'), 'primary')
        self.assertEqual(self.broker.select(model='fable'), 'second')
        self.assertEqual(self.broker.select(model='native-default'), 'second')
        self.assertIsNone(self.broker.earliest_reset(model='opus'))
        self.assertEqual(self.broker.earliest_reset(model='fable'), reset)
        self.broker.record_rate_limit('primary', {'rateLimitType': 'seven_day',
                                                  'resetsAt': reset}, rejected=True)
        self.assertEqual(self.broker.select(model='opus'), 'second')
        self.assertEqual(self.broker.select(model='fable'), 'second')

    async def test_worker_uses_requested_model(self):
        self.meter('primary', 'seven_day_fable', 99, time.time() + 3600)
        launched = []
        async def run(provider, prompt, cwd, session_id, **options):
            launched.append(options['account_alias'])
            return RunResult(session_id, success=True)
        for model in ('opus', 'fable', None):
            await self.broker.run(run, 'claude', 'work', self.root, self.sid, fresh=True, model=model)
        self.assertEqual(launched, ['primary', 'second', 'second'])

    async def test_every_account_question_needs_a_model(self):
        for ask in (lambda: self.broker.select(), lambda: self.broker.activate(),
                    lambda: self.broker.earliest_reset(), lambda: self.broker.selection_order(),
                    lambda: self.broker.account_state('primary'), lambda: self.broker.is_full('primary'),
                    lambda: listed_accounts(self.store), lambda: account_pick(self.store),
                    lambda: account_block(self.store, 'primary')):
            with self.assertRaises(TypeError):
                ask()

    async def test_displays_keep_an_account_whose_fable_meter_alone_is_full(self):
        from coordinator import controls, parent
        from coordinator.setup_flow import setup_state
        now = time.time()
        for alias in ('primary', 'second'):
            self.meter(alias, 'seven_day_fable', 100, now + 3600)
            self.meter(alias, 'seven_day', 78, now + 7200)
        with self.store.db:
            self.store.put('worker_model', 'opus')
        status = controls._status_card(self.store, None)
        self.assertNotIn('all accounts limited', status)
        self.assertIn('primary@example.com', status)
        self.assertIn('Next account: primary@example.com',
                      controls._current_claude(self.store, self.store.get('accounts')))
        self.assertIn('primary@example.com (enabled) · next dispatch', controls._accounts(self.store))
        card = controls._account_card(self.store, 'primary')
        self.assertNotIn('Account state: limited', card)
        self.assertIsNone(controls.blocked(self.store, 'primary'))
        self.assertEqual(setup_state(self.store)['main'], 'claude')
        with self.store.db:
            self.store.put('worker_model', 'fable')
        self.assertIn('all accounts limited', controls._status_card(self.store, None))
        with self.store.db:
            self.store.put('claude_account_pick', {'fable': 'second', 'other': 'primary'})
            self.store.put('coordinator_model', 'claude-fable-5-2')
            self.store.put('account_blocks', {'second': {'reason': 'quota', 'until': now + 60},
                                              'primary': {'reason': 'quota', 'until': now + 60}})
            self.store.put('coordinator_account_notice', {'key': ['claude']})
        with patch('coordinator.account_status.refresh_accounts', new_callable=AsyncMock) as refresh:
            await parent.retry_paused(self.store, 'coordinator_account')
        self.assertEqual(refresh.await_args.kwargs['aliases'], ['second'])
        self.assertIn('primary', self.store.get('account_blocks'))


class HeldAccountTests(AccountTestsSupport, unittest.IsolatedAsyncioTestCase):
    async def refresh(self, used=0, error=None, reset=None, other=0):
        current = self.store.get('account_status')['primary']['usage']

        async def collect(account):
            snapshot = {'observed_at': time.time(), 'usage': {
                key: {'utilization': used if key == 'seven_day' else other,
                      'resets_at': reset if key == 'seven_day' and reset is not None else window['resets_at']}
                for key, window in current.items()}}
            if error:
                snapshot['error'] = error
            return snapshot

        return await refresh_accounts(self.store, collect, aliases=['primary'])

    def held(self, reason='quota', used=100, until=True):
        reset = self.store.get('account_status')['primary']['usage']['seven_day']['resets_at']
        self.meter('primary', 'seven_day', used, reset)
        with self.store.db:
            self.store.put('account_blocks', {})
            self.broker.hold('primary', reset if until else None, reason)
        return reset

    async def assert_next_run(self, alias):
        launched = []

        async def run(provider, prompt, cwd, session_id, **options):
            launched.append(options['account_alias'])
            return RunResult(session_id, success=True)

        result = await self.broker.run(run, 'claude', 'work', self.root, self.sid, fresh=True)
        self.assertTrue(result.success)
        self.assertEqual(launched, [alias])

    async def test_refused_past_reset_reading_stays_held_until_refusal_expiry(self):
        for until in (True, False):
            with self.subTest(until=until):
                self.meter('primary', 'seven_day', 100, time.time() + 3600)
                self.held(until=until)
                past = time.time() - 1
                await self.refresh(reset=past)
                refused_at = time.time()
                with patch('time.time', return_value=refused_at), self.store.db:
                    self.broker.record_rate_limit('primary', None, rejected=True)
                await self.refresh(reset=past)
                self.assertIn('primary', self.broker.blocks())
                expiry = refused_at + self.broker.REJECTION_HOLD_SECONDS
                self.assertEqual(self.broker.blocks()['primary']['until'], expiry)
                with patch('time.time', return_value=expiry - 1):
                    self.assertIn('primary', self.broker.blocks())
                with patch('time.time', return_value=expiry + 1):
                    self.assertNotIn('primary', self.broker.blocks())

    async def test_threshold_and_other_holds_keep_cached_lower_readings(self):
        for reason in ('threshold', 'manual'):
            with self.subTest(reason=reason):
                self.held(reason, used=99)
                self.assertEqual(self.broker.select(model='native-default'), 'second')
                await self.refresh()
                self.assertEqual(self.broker.select(model='native-default'), 'second')
                self.assertEqual(self.store.get('account_status')['primary']['usage']['seven_day']['utilization'], 99)

    async def test_cached_lower_reading_keeps_threshold_hold_and_runs_other_account(self):
        reset = self.store.get('account_status')['primary']['usage']['seven_day']['resets_at']
        with self.store.db:
            self.broker.record_rate_limit('primary', {'rateLimitType': 'seven_day',
                                                     'utilization': .99, 'resetsAt': reset})
        with patch('time.time', return_value=time.time() + 60):
            await self.refresh(used=96)
            self.assertEqual(self.broker.blocks()['primary']['reason'], 'threshold')
            self.assertEqual(self.store.get('account_status')['primary']['usage']['seven_day']['utilization'], 99)
            await self.assert_next_run('second')

    async def test_cached_lower_reading_keeps_local_reserve_hold_and_runs_other_account(self):
        reset = self.store.get('account_status')['primary']['usage']['seven_day']['resets_at']
        with patch('coordinator.accounts.temporary_local_switch_thresholds', return_value={'primary': .8}):
            with self.store.db:
                self.broker.record_rate_limit('primary', {'rateLimitType': 'seven_day',
                                                         'utilization': .81, 'resetsAt': reset})
            with patch('time.time', return_value=time.time() + 60):
                await self.refresh(used=78)
                self.assertEqual(self.broker.blocks()['primary']['reason'], 'threshold')
                self.assertEqual(self.store.get('account_status')['primary']['usage']['seven_day']['utilization'], 81)
                await self.assert_next_run('second')

    async def test_cached_lower_reading_after_release_keeps_new_full_window_and_runs_other_account(self):
        now = time.time()
        self.held()
        await self.refresh()
        await self.assert_next_run('primary')
        for minute, used in ((10, 50), (20, 90), (30, 99)):
            with patch('time.time', return_value=now + minute * 60):
                await self.refresh(used=used)
        with patch('time.time', return_value=now + 35 * 60):
            await self.refresh(used=90)
            self.assertEqual(self.broker.account_state('primary', 'native-default')['state'], 'limited')
            self.assertEqual(self.store.get('account_status')['primary']['usage']['seven_day']['utilization'], 99)
            await self.assert_next_run('second')

    async def test_removed_reserve_clears_threshold_hold_after_quota_release(self):
        now = time.time()
        reset = now + 2 * 86400
        self.meter('second', 'seven_day', 10, now + 6 * 86400)
        self.meter('primary', 'seven_day', 100, reset)
        self.held()
        await self.refresh()
        reserve = {'primary': .8}
        with patch('coordinator.accounts.temporary_local_switch_thresholds', side_effect=lambda: dict(reserve)):
            with patch('time.time', return_value=now + 25 * 60), self.store.db:
                self.broker.record_rate_limit('primary', {'status': 'allowed_warning', 'rateLimitType': 'seven_day',
                                                         'utilization': .81, 'resetsAt': reset})
            self.assertEqual(self.broker.account_state('primary', 'native-default')['state'], 'limited')
            reserve.clear()
            with patch('time.time', return_value=now + 30 * 60):
                await self.refresh(used=81)
                self.assertEqual(self.broker.select(model='native-default'), 'primary')
                await self.assert_next_run('primary')

    async def test_untimed_threshold_clears_after_quota_release_and_new_window(self):
        now = time.time()
        self.meter('second', 'seven_day', 10, now + 10 * 86400)
        reset = self.held()
        await self.refresh()
        with patch('time.time', return_value=now + 25 * 60), self.store.db:
            self.broker.record_rate_limit('primary', {'status': 'allowed_warning', 'rateLimitType': 'seven_day',
                                                     'utilization': .995})
        self.assertEqual(self.broker.account_state('primary', 'native-default')['state'], 'limited')
        self.assertIsNone(self.broker.blocks()['primary']['until'])
        with patch('time.time', return_value=reset + 600):
            await self.refresh(used=5, reset=reset + 7 * 86400)
            self.assertEqual(self.broker.select(model='native-default'), 'primary')
            await self.assert_next_run('primary')

    async def assert_quota_cached_weekly_stays_full(self, threshold, used, cached):
        now = time.time()
        five_reset = now + 45 * 60
        week_reset = now + 2 * 86400
        self.meter('primary', 'five_hour', cached, five_reset)
        self.meter('primary', 'seven_day', cached, week_reset)
        with patch('coordinator.accounts.temporary_local_switch_thresholds', return_value={'primary': threshold}):
            with self.store.db:
                self.broker.record_rate_limit('primary', {'status': 'allowed_warning', 'rateLimitType': 'seven_day',
                                                         'utilization': used / 100, 'resetsAt': week_reset})
                self.broker.record_rate_limit('primary', {'status': 'rejected', 'rateLimitType': 'five_hour',
                                                         'utilization': 1.0, 'resetsAt': five_reset}, rejected=True)
            with patch('time.time', return_value=now + 60):
                await self.refresh(used=cached, other=cached)
                self.assertEqual(self.store.get('account_status')['primary']['usage']['seven_day']['utilization'], used)
                await self.assert_next_run('second')
            with patch('time.time', return_value=five_reset + 120):
                await self.refresh(used=cached, other=cached)
                self.assertEqual(self.store.get('account_status')['primary']['usage']['seven_day']['utilization'], used)
                await self.assert_next_run('second')

    async def test_quota_cached_reading_keeps_weekly_threshold_after_five_hour_reset(self):
        await self.assert_quota_cached_weekly_stays_full(.99, 99.5, 96)

    async def test_quota_cached_reading_keeps_weekly_reserve_after_five_hour_reset(self):
        await self.assert_quota_cached_weekly_stays_full(.8, 81, 78)

    async def test_quota_release_trusts_only_the_named_window_and_keeps_other_cached_usage(self):
        keys = ('seven_day', 'seven_day_fable')
        for named in (*keys, None):
            with self.subTest(named=named):
                reset = time.time() + 3600
                with self.store.db:
                    self.store.put('account_blocks', {})
                for key in keys:
                    self.meter('primary', key, 70, reset)
                info = {'status': 'rejected', 'utilization': 1.0, 'resetsAt': reset}
                if named:
                    info['rateLimitType'] = named
                with self.store.db:
                    self.broker.record_rate_limit('primary', info, rejected=True)
                await self.refresh(used=0, other=0)
                usage = self.store.get('account_status')['primary']['usage']
                self.assertEqual({key: usage[key]['utilization'] for key in keys},
                                 {key: 0 if not named or key == named else 70 for key in keys})
                await self.assert_next_run('primary')

    async def test_quota_release_ignores_non_window_saved_usage(self):
        self.held()
        status = self.store.get('account_status')
        status['primary']['usage']['five_hour'] = None
        with self.store.db:
            self.store.put('account_status', status)

        async def collect(account):
            return {'observed_at': time.time(), 'usage': {
                key: {'utilization': 0, 'resets_at': time.time() + 1800}
                for key in ('seven_day', 'seven_day_fable')}}

        await refresh_accounts(self.store, collect, aliases=['primary'])
        await self.assert_next_run('primary')

    async def test_refused_trial_rotates_reholds_and_requires_a_changed_holding_window(self):
        reset = self.held()
        await self.refresh()
        launched = []

        async def run(provider, prompt, cwd, session_id, **options):
            launched.append(options['account_alias'])
            if options['account_alias'] == 'primary':
                return RunResult(session_id, error='limit', quota_limited=True,
                                 rate_limit_info={'rateLimitType': 'seven_day', 'resetsAt': reset},
                                 transcript_path=str(self.transcript))
            self.assertFalse(options['fresh'])
            self.assertEqual(session_id, self.sid)
            self.assertTrue(prompt.startswith(LIMIT_CONTINUATION))
            return RunResult(session_id, success=True)

        result = await self.broker.run(run, 'claude', 'work', self.root, self.sid, fresh=True)
        self.assertTrue(result.success)
        self.assertEqual(launched, ['primary', 'second'])
        self.assertEqual(self.broker.select(model='native-default'), 'second')
        self.assertEqual(self.broker.blocks()['primary']['reason'], 'quota')
        self.broker = AccountBroker(self.store)
        await self.refresh()
        self.assertEqual(self.broker.select(model='native-default'), 'second')
        await self.refresh(other=1)
        self.assertEqual(self.broker.select(model='native-default'), 'second')
        await self.refresh(used=1)
        self.assertEqual(self.broker.select(model='native-default'), 'primary')

    async def test_refused_reading_changes_reset_or_expires_at_refusal_reset(self):
        for change in ('reset', 'expiry'):
            with self.subTest(change=change):
                reset = self.held()
                await self.refresh()
                with self.store.db:
                    self.broker.record_rate_limit('primary', None, rejected=True)
                await self.refresh()
                self.assertEqual(self.broker.select(model='native-default'), 'second')
                if change == 'reset':
                    await self.refresh(reset=reset - SAME_WINDOW_SECONDS - 1)
                else:
                    with patch('time.time', return_value=reset + 1):
                        await self.refresh()
                        self.assertTrue(self.broker.available(self.store.get('accounts'), 'primary', model='native-default'))
                self.assertEqual(self.broker.select(model='native-default'), 'primary')

    async def test_reset_drift_does_not_release_a_refused_reading_until_a_new_window(self):
        reset = self.held()
        await self.refresh()
        with self.store.db:
            self.broker.record_rate_limit('primary', {'rateLimitType': 'seven_day', 'resetsAt': reset}, rejected=True)
        for drift in (.000001, -.000001, 1, -1):
            with self.subTest(drift=drift):
                await self.refresh(reset=reset + drift)
                self.assertIn('primary', self.broker.blocks())
                await self.assert_next_run('second')
        await self.refresh(reset=reset + SAME_WINDOW_SECONDS + 1)
        await self.assert_next_run('primary')

    async def test_five_hour_refusal_does_not_hold_or_switch(self):
        self.broker.record_rate_limit('primary', {'rateLimitType': 'five_hour', 'utilization': 1,
                                                  'resetsAt': time.time() + 600}, rejected=True)
        self.assertNotIn('primary', self.broker.blocks())
        self.assertEqual(self.broker.select(model='native-default'), 'primary')

    async def test_threshold_and_local_reserve_keep_holds_at_or_above_the_switch_point(self):
        for threshold, used in ((.98, 98), (.98, 100), (.8, 80), (.8, 81)):
            with self.subTest(threshold=threshold, used=used), patch(
                    'coordinator.accounts.temporary_local_switch_thresholds', return_value={'primary': threshold}):
                self.held()
                await self.refresh(used=used)
                self.assertEqual(self.broker.select(model='native-default'), 'second')
                self.assertIn('primary', self.broker.blocks())

    async def test_error_with_under_limit_usage_keeps_timed_and_untimed_holds(self):
        for until in (True, False):
            with self.subTest(until=until):
                self.held(until=until)
                self.assertTrue(await self.refresh(error='usage_unavailable'))
                self.assertEqual(self.broker.select(model='native-default'), 'second')
                self.assertIn('primary', self.broker.blocks())


class AccountTests(AccountTestsSupport, unittest.IsolatedAsyncioTestCase):
    async def test_quota_held_fresh_zero_is_used_by_next_run_and_shown_on_card(self):
        from coordinator.controls import _account_card
        reset = self.store.get('account_status')['primary']['usage']['seven_day']['resets_at']
        self.meter('primary', 'seven_day', 100, reset)
        with self.store.db:
            self.broker.hold('primary', reset, 'quota')
        self.assertEqual(self.broker.select(model='native-default'), 'second')

        async def collect(account):
            return {'observed_at': time.time(), 'usage': {
                key: dict(window, utilization=0) for key, window in
                self.store.get('account_status')['primary']['usage'].items()}}

        await refresh_accounts(self.store, collect, aliases=['primary'])
        self.assertEqual(self.broker.select(model='native-default'), 'primary')
        launched = []

        async def run(provider, prompt, cwd, session_id, **options):
            launched.append(options['account_alias'])
            return RunResult(session_id, success=True)

        result = await self.broker.run(run, 'claude', 'work', self.root, self.sid, fresh=True)
        self.assertTrue(result.success)
        self.assertEqual(launched, ['primary'])
        card = _account_card(self.store, 'primary')
        self.assertIn('Weekly (all models): 🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩 100%', card)
        self.assertNotIn('Paused:', card)
        self.assertNotIn('blocked', card)

    async def test_documented_local_overrides_without_thresholds_keep_default(self):
        override = self.root / 'local-overrides.json'
        override.write_text(json.dumps({'unrelated_setting': []}))
        with patch('coordinator.accounts.LOCAL_OVERRIDES_PATH', override), \
                patch.object(logging.getLogger('coordinator.accounts'), 'warning') as warning:
            for _ in range(50):
                self.assertEqual(switch_threshold(self.store, 'primary'), .99)
                self.assertEqual(self.broker.select(model='native-default'), 'primary')
        warning.assert_not_called()

    async def test_invalid_local_overrides_warn_once_per_distinct_error_during_selection(self):
        override = self.root / 'local-overrides.json'
        with patch('coordinator.accounts.LOCAL_OVERRIDES_PATH', override), \
                patch.object(logging.getLogger('coordinator.accounts'), 'warning') as warning:
            for contents, count in (('{broken', 1), ('[broken', 2),
                                    (json.dumps({'account_switch_thresholds': {'primary': True}}), 3),
                                    ('{broken', 3), ('null', 4)):
                override.write_text(contents)
                for _ in range(50):
                    self.assertEqual(self.broker.select(model='native-default'), 'primary')
                self.assertEqual(warning.call_count, count)
            self.assertTrue(all(call.args[0] == 'invalid local account overrides: %s'
                                for call in warning.call_args_list))

    async def test_valid_local_overrides_apply_during_repeated_selection_and_file_changes(self):
        override = self.root / 'local-overrides.json'
        self.meter('primary', 'seven_day', 90, time.time() + 300)
        with patch('coordinator.accounts.LOCAL_OVERRIDES_PATH', override), \
                patch.object(logging.getLogger('coordinator.accounts'), 'warning') as warning:
            for threshold, selected in ((.8, 'second'), (.95, 'primary')):
                override.write_text(json.dumps({'unrelated_setting': [],
                                                'account_switch_thresholds': {'primary': threshold}}))
                for _ in range(50):
                    self.assertEqual(switch_threshold(self.store, 'primary'), threshold)
                    self.assertEqual(switch_threshold(self.store, 'second'), .99)
                    self.assertEqual(self.broker.select(model='native-default'), selected)
        warning.assert_not_called()

    async def test_owner_pick_prefers_available_account_and_returns_after_each_pause(self):
        profiles = self.store.get('accounts')
        profiles['aaa'] = dict(profiles['primary'])
        baseline = self.store.get('account_status')
        baseline['aaa'] = {'identity': {'logged_in': True, 'email': 'aaa@example.com'},
                           'observed_at': time.time(), 'usage': {'seven_day': {'utilization': 1,
                           'resets_at': time.time() + 10000}}}
        with self.store.db:
            self.store.put('accounts', profiles)
            self.store.put('account_status', baseline)
            self.store.put('claude_account_pick', 'second')
        for cause in ('limited', 'held', 'reserve', 'signed_out', 'disabled'):
            with self.subTest(cause=cause), patch('coordinator.accounts.temporary_local_switch_thresholds',
                    return_value={'second': .8} if cause == 'reserve' else {}):
                self.assertEqual(self.broker.select(model='native-default'), 'second')
                if cause in ('limited', 'reserve'):
                    self.meter('second', 'seven_day', 99 if cause == 'limited' else 81, time.time() + 1800)
                elif cause == 'held':
                    self.broker.hold('second', time.time() + 1800, 'quota')
                elif cause == 'signed_out':
                    status = self.store.get('account_status')
                    status['second']['identity']['logged_in'] = False
                    self.store.put('account_status', status)
                else:
                    accounts = self.store.get('accounts')
                    accounts['second']['enabled'] = False
                    self.store.put('accounts', accounts)
                self.assertEqual(self.broker.select(model='native-default'), 'primary')
                self.store.put('accounts', profiles)
                self.store.put('account_status', baseline)
                self.store.put('account_blocks', {})
                self.assertEqual(self.broker.select(model='native-default'), 'second')
        self.assertEqual(self.broker.select(attempted={'second'}, model='native-default'), 'primary')

    async def test_workers_use_pick_and_finish_on_their_original_account(self):
        launched = []
        async def run(provider, prompt, cwd, session_id, **options):
            launched.append(options['account_alias'])
            control_api.call(self.store, 'account.use', {'alias': 'primary'})
            return RunResult(session_id, success=True)
        control_api.call(self.store, 'account.use', {'alias': 'second'})
        result = await self.broker.run(run, 'claude', 'work', self.root, self.sid, fresh=True)
        self.assertTrue(result.success)
        self.assertEqual(launched, ['second'])
        self.assertEqual(self.broker.select(model='native-default'), 'primary')

    async def test_pick_usage_switches_silently_to_a_spare_account(self):
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('home',1,1,'Home','',1)")
        self.store.put('coordinator_home_topic', 'home')
        self.store.put('claude_account_pick', 'second')
        def notices():
            return [row['text'] for row in self.store.db.execute('SELECT text FROM outbox ORDER BY id')]
        reset = time.time() + 3600
        self.broker.record_rate_limit('second', {'rateLimitType': 'five_hour', 'utilization': .98, 'resetsAt': reset})
        self.broker.activate(model='native-default')
        self.assertEqual(notices(), [])
        self.store.put('account_blocks', {})
        self.meter('second', 'five_hour', 1, reset)
        for meter in ('seven_day', 'seven_day_fable'):
            self.broker.record_rate_limit('second', {'rateLimitType': meter, 'utilization': .99, 'resetsAt': reset})
            self.broker.activate(model='native-default')
            self.broker.activate(model='native-default')
            self.assertEqual(notices(), [])
            self.assertEqual(self.broker.select(model='native-default'), 'primary')
            if meter == 'seven_day':
                self.broker.record_rate_limit('second', {'rateLimitType': 'seven_day_fable',
                    'utilization': .99, 'resetsAt': reset})
                self.broker.activate(model='native-default')
                self.assertEqual(notices(), [])
                self.meter('second', 'seven_day_fable', 1, reset)
            self.store.put('account_blocks', {})
            self.meter('second', meter, 1, reset)
            self.broker.activate(model='native-default')
        self.assertEqual(self.broker.select(model='native-default'), 'second')

    async def test_local_threshold_override_is_optional_and_alias_scoped(self):
        override = self.root / 'local-overrides.json'
        with patch('coordinator.accounts.LOCAL_OVERRIDES_PATH', override):
            self.assertEqual(switch_threshold(self.store, 'orbit'), .99)
            override.write_text(json.dumps({'account_switch_thresholds': {'orbit': .8}}))
            self.assertEqual(switch_threshold(self.store, 'orbit'), .8)
            self.assertEqual(switch_threshold(self.store, 'primary'), .99)
            override.write_text('{broken')
            with self.assertLogs('coordinator.accounts', level='WARNING'):
                self.assertEqual(switch_threshold(self.store, 'orbit'), .99)
            override.write_text(json.dumps({'account_switch_thresholds': {'orbit': True}}))
            with self.assertLogs('coordinator.accounts', level='WARNING'):
                self.assertEqual(switch_threshold(self.store, 'orbit'), .99)

    async def test_retry_skip_and_empty_selection_logs_name_the_actual_cause(self):
        accounts = self.store.get('accounts')
        with self.store.db:
            self.store.put('active_account', 'primary')
        self.assertEqual(self.broker.selection_reason(accounts, 'primary', {'primary'}, model='native-default'), 'reason=attempted')
        with self.assertLogs('coordinator.accounts', level='INFO') as logs:
            self.assertEqual(self.broker.activate(accounts, {'primary'}, model='native-default'), 'second')
        self.assertIn('reason=attempted', logs.output[0])
        with self.store.db:
            self.store.put('active_account', 'primary')
        now = time.time()
        for alias in accounts:
            self.meter(alias, 'seven_day', 99, now + 120)
            self.meter(alias, 'five_hour', 10, now + 120)
            self.meter(alias, 'seven_day_fable', 10, now + 120)
        with self.assertLogs('coordinator.accounts', level='INFO') as logs:
            self.assertIsNone(self.broker.activate(accounts, model='native-default'))
        self.assertIn('no eligible account from=primary@example.com', logs.output[0])
        self.assertNotIn('to=None', logs.output[0])
        for alias in accounts:
            self.meter(alias, 'seven_day', 10, now + 120)
        with self.assertLogs('coordinator.accounts', level='INFO') as logs:
            self.assertEqual(self.broker.activate(accounts, model='native-default'), 'primary')
        self.assertIn('reason=account_available', logs.output[0])

    async def test_local_threshold_changes_fullness_selection_and_reset_for_each_window(self):
        override = self.root / 'local-overrides.json'
        override.write_text(json.dumps({'account_switch_thresholds': {'orbit': .8}}))
        accounts = self.store.get('accounts')
        accounts['orbit'] = dict(accounts['primary'])
        status = self.store.get('account_status')
        status['orbit'] = dict(status['primary'])
        with self.store.db:
            self.store.put('accounts', accounts)
            self.store.put('account_status', status)
        now = time.time()
        with patch('coordinator.accounts.LOCAL_OVERRIDES_PATH', override):
            for window in ('seven_day',):
                self.meter('orbit', window, 80, now + 300)
                self.meter('primary', window, 80, now + 200)
                self.assertTrue(self.broker.is_full('orbit', 'native-default'), window)
                self.assertFalse(self.broker.is_full('primary', 'native-default'), window)
                self.assertFalse(self.broker.available(accounts, 'orbit', model='native-default'), window)
                self.assertNotEqual(self.broker.select(accounts, model='native-default'), 'orbit')
                self.assertEqual(self.broker.earliest_reset({'orbit': accounts['orbit']}, model='native-default'), now + 300)
                self.meter('orbit', window, 10, now + 300)
                self.meter('primary', window, 10, now + 200)
            self.meter('orbit', 'seven_day_fable', 85, now + 300)
            self.assertFalse(self.broker.is_full('orbit', 'native-default'))
            self.meter('orbit', 'seven_day_fable', 99, now + 300)
            self.assertTrue(self.broker.is_full('orbit', 'native-default'))

    async def test_reserve_applies_only_to_the_all_model_meter(self):
        override = self.root / 'local-overrides.json'
        override.write_text(json.dumps({'account_switch_thresholds': {'primary': .8}}))
        accounts = self.store.get('accounts')
        now = time.time()
        with patch('coordinator.accounts.LOCAL_OVERRIDES_PATH', override):
            self.assertEqual(switch_threshold(self.store, 'primary', 'seven_day_fable'), .99)
            self.assertEqual(switch_threshold(self.store, 'primary', 'seven_day'), .8)
            self.meter('primary', 'seven_day_fable', 85, now + 300)
            self.meter('primary', 'seven_day', 60, now + 300)
            for model in ('fable', 'opus'):
                self.assertEqual(self.broker.account_state('primary', model)['state'], 'available', model)
            self.meter('primary', 'seven_day', 81, now + 300)
            for model in ('fable', 'opus'):
                self.assertEqual(self.broker.account_state('primary', model)['state'], 'limited', model)
            self.meter('second', 'seven_day_fable', 85, now + 300)
            self.meter('second', 'seven_day', 85, now + 300)
            self.assertEqual(self.broker.account_state('second', 'fable')['state'], 'available')

    async def test_removing_override_releases_threshold_block_and_reserve_label(self):
        from coordinator.control_ui import _accounts_view
        from coordinator.controls import _accounts
        override = self.root / 'local-overrides.json'
        override.write_text(json.dumps({'account_switch_thresholds': {'primary': .8}}))
        with patch('coordinator.accounts.LOCAL_OVERRIDES_PATH', override):
            self.meter('primary', 'seven_day', 80, time.time() + 300)
            self.broker.record_rate_limit('primary', {'rateLimitType': 'seven_day',
                                                       'utilization': .8, 'resetsAt': time.time() + 300})
            self.assertIn('reserve 20%', _accounts_view(self.store, 0)[0])
            self.assertIn('reserve 20% (local override)', _accounts(self.store))
            self.assertTrue(self.broker.is_full('primary', 'native-default'))
            override.unlink()
            self.assertFalse(self.broker.is_full('primary', 'native-default'))
            self.assertNotIn('reserve', _accounts_view(self.store, 0)[0])

    async def test_quota_worker_resumes_same_session_on_second_account(self):
        calls = []

        async def run(provider, prompt, cwd, sid, **options):
            calls.append((provider, prompt, sid, options.copy()))
            if len(calls) == 1:
                return RunResult(sid, pid=42, quota_limited=True,
                                 rate_limit_info={'status': 'rejected', 'rateLimitType': 'seven_day',
                                                  'resetsAt': time.time() + 3600},
                                 transcript_path=str(self.transcript))
            return RunResult(sid, success=True, transcript_path=str(self.transcript))

        result = await self.broker.run(run, 'claude', 'same task prompt', self.root, self.sid,
                                       fresh=False, model='opus')
        self.assertTrue(result.success)
        self.assertEqual(len(calls), 2)
        self.assertEqual([call[2] for call in calls], [self.sid, self.sid])
        self.assertEqual(calls[1][1], LIMIT_CONTINUATION + 'same task prompt')
        self.assertEqual(calls[0][3]['account_alias'], 'primary')
        self.assertEqual(calls[1][3]['account_alias'], 'second')
        target = Path(calls[1][3]['resume_path'])
        self.assertEqual(target.parent.parent.parent, self.root / 'second')
        self.assertTrue(target.is_symlink())
        self.assertEqual(target.resolve(), self.transcript.resolve())
        self.assertIn('primary', self.store.get('account_blocks'))

    async def test_a_rotation_is_reported_as_an_accounts_problem(self):
        seen = []

        async def run(provider, prompt, cwd, sid, **options):
            if options['account_alias'] == 'primary':
                return RunResult(sid, pid=1, quota_limited=True, transcript_path=str(self.transcript),
                                 rate_limit_info={'status': 'rejected', 'resetsAt': time.time() + 3600})
            return RunResult(sid, success=True, transcript_path=str(self.transcript))
        await self.broker.run(run, 'claude', 'work', self.root, self.sid, fresh=False, model='opus',
                              on_problem=lambda *problem: seen.append(problem))
        self.assertEqual(seen, [('accounts', 'rotated',
                                 'from=primary@example.com to=second@example.com model=opus')])

    async def test_a_failed_launch_after_rotation_keeps_the_first_pid_and_resumes(self):
        fresh = []

        async def run(provider, prompt, cwd, sid, **options):
            fresh.append(options['fresh'])
            if options['account_alias'] == 'primary':
                return RunResult(sid, pid=123, quota_limited=True, transcript_path=str(self.transcript),
                                 rate_limit_info={'status': 'rejected', 'resetsAt': time.time() + 3600})
            return RunResult(sid, error='Account could not launch')
        result = await self.broker.run(run, 'claude', 'work', self.root, self.sid, fresh=True)
        self.assertEqual(result.pid, 123)
        self.assertEqual(fresh, [True, False])

    async def test_a_non_quota_failure_stays_on_its_account(self):
        calls = []

        async def run(provider, prompt, cwd, sid, **options):
            calls.append(options['account_alias'])
            return RunResult(sid, pid=1, error='provider failed')
        result = await self.broker.run(run, 'claude', 'work', self.root, self.sid, fresh=False)
        self.assertEqual(calls, ['primary'])
        self.assertEqual(result.error, 'provider failed')

    async def test_a_resume_without_its_transcript_never_starts_the_provider(self):
        async def run(*args, **kwargs):
            self.fail('Provider must not start without the saved transcript')
        self.transcript.unlink()
        with self.assertRaisesRegex(TaskFailure, 'conversation file is unavailable'):
            await self.broker.run(run, 'claude', 'work', self.root, self.sid, fresh=False)
        with self.store.db:
            self.store.put('session_transcripts', {})
        with patch('pathlib.Path.home', return_value=self.root / 'home'):
            result = await self.broker.run(run, 'claude', 'work', self.root, self.sid, fresh=False)
        self.assertEqual(result.failure_code, 'transcript_missing')

    async def test_a_warning_below_99_percent_records_usage_without_blocking(self):
        async def run(provider, prompt, cwd, sid, **options):
            return RunResult(sid, success=True, rate_limit_info={
                'status': 'allowed_warning', 'rateLimitType': 'seven_day', 'utilization': .8})
        await self.broker.run(run, 'claude', 'work', self.root, self.sid, fresh=False)
        self.assertEqual(self.store.get('account_status')['primary']['usage']['seven_day']['utilization'], 80)
        self.assertEqual(self.store.get('account_blocks', {}), {})
        self.assertEqual(self.broker.select(model='native-default'), 'primary')

    async def test_old_five_hour_readings_do_not_exclude_an_account(self):
        self.meter('primary', 'five_hour', 100, time.time() + 1800)
        self.broker.record_rate_limit('primary', {'status': 'allowed_warning', 'rateLimitType': 'five_hour',
                                                  'utilization': .98, 'resetsAt': time.time() + 1800})
        self.assertEqual(self.broker.select(model='native-default'), 'primary')

    async def test_expired_blocks_are_pruned_and_committed_on_read(self):
        with self.store.db:
            self.store.put('account_blocks', {'primary': {'until': time.time() - 5},
                                              'second': {'until': time.time() + 600}})
        self.assertEqual(self.broker.select(model='native-default'), 'primary')
        self.assertFalse(self.store.db.in_transaction)
        self.assertEqual(list(self.store.get('account_blocks')), ['second'])

    async def test_pruning_inside_a_callers_transaction_joins_it_without_committing(self):
        expired = {'primary': {'until': time.time() - 10}}
        with self.store.db:
            self.store.put('account_blocks', expired)
        self.store.db.execute('BEGIN')
        self.store.put('marker', 'uncommitted')
        self.assertEqual(self.broker.blocks(), {})
        self.assertTrue(self.store.db.in_transaction)
        self.store.db.rollback()
        self.assertIsNone(self.store.get('marker'))
        self.assertEqual(self.store.get('account_blocks'), expired)

    async def test_legacy_home_transcript_resumes(self):
        home = self.root / 'home'
        legacy = home / '.claude' / 'projects' / 'old-project' / (self.sid + '.jsonl')
        legacy.parent.mkdir(parents=True)
        legacy.write_text('{}\n')
        self.transcript.unlink()
        discover_accounts(self.store, self.root / 'missing')
        calls = []

        async def run(provider, prompt, cwd, sid, **options):
            calls.append(options.copy())
            return RunResult(sid, success=True, transcript_path=options['resume_path'])

        for saved in ({self.sid: str(legacy)}, {}):
            calls.clear()
            with self.store.db:
                self.store.put('session_transcripts', saved)
            with patch('pathlib.Path.home', return_value=home):
                result = await self.broker.run(run, 'claude', 'continue', self.root, self.sid,
                                               fresh=False, model='opus')
            self.assertTrue(result.success, result.error)
            resume = Path(calls[0]['resume_path'])
            self.assertEqual(resume.resolve(), legacy.resolve())
            self.assertEqual(resume.parents[2], self.root / calls[0]['account_alias'])
        self.assertTrue(legacy.is_file())
        self.assertFalse(legacy.is_symlink())

    async def test_an_exhausted_pool_stops_after_each_account_once(self):
        calls = []

        async def run(provider, prompt, cwd, sid, **options):
            calls.append(options['account_alias'])
            return RunResult(sid, pid=1, quota_limited=True, transcript_path=str(self.transcript))

        result = await self.broker.run(run, 'claude', 'work', self.root, self.sid, fresh=False)
        self.assertFalse(result.success)
        self.assertEqual(sorted(calls), ['primary', 'second'])
        self.assertEqual(sorted(self.store.get('account_blocks')), ['primary', 'second'])
        self.assertIsNone(self.broker.select(model='native-default'))

    async def test_a_single_model_weekly_warning_leaves_the_three_meters_alone(self):
        for limit_type in ('seven_day_opus', 'seven_day_sonnet', 'overage'):
            with self.store.db:
                self.broker.record_rate_limit('primary', {'status': 'allowed_warning', 'rateLimitType': limit_type,
                                                          'utilization': .96, 'resetsAt': time.time() + 5 * 86400})
            usage = self.store.get('account_status')['primary']['usage']
            self.assertEqual([usage[key]['utilization'] for key in ('seven_day', 'seven_day_fable')],
                             [10, 10], limit_type)
            self.assertEqual(self.broker.select(model='native-default'), 'primary', limit_type)
        with self.store.db:
            self.broker.record_rate_limit('primary', {'status': 'rejected', 'rateLimitType': 'seven_day_opus'}, True)
        self.assertEqual(self.broker.select(model='native-default'), 'second')

    async def test_a_fable_weekly_warning_at_99_percent_fills_the_fable_meter(self):
        with self.store.db:
            self.broker.record_rate_limit('primary', {'status': 'allowed_warning',
                                                      'rateLimitType': 'seven_day_overage_included',
                                                      'utilization': .99, 'resetsAt': time.time() + 3 * 86400})
        self.assertEqual(self.store.get('account_status')['primary']['usage']['seven_day_fable']['utilization'], 99)
        self.assertTrue(self.broker.is_full('primary', 'native-default'))
        self.assertEqual(self.broker.select(model='native-default'), 'second')

    async def test_the_first_event_of_a_new_window_does_not_bring_back_the_old_reading(self):
        now = time.time()
        self.meter('primary', 'seven_day', 97, now - 10)
        self.assertFalse(self.broker.is_full('primary', 'native-default'))
        with self.store.db:
            self.broker.record_rate_limit('primary', {'status': 'allowed', 'rateLimitType': 'seven_day',
                                                      'resetsAt': int(now) + 5 * 3600})
        self.assertEqual(self.store.get('account_status')['primary']['usage']['seven_day'],
                         {'resets_at': int(now) + 5 * 3600})
        self.assertFalse(self.broker.is_full('primary', 'native-default'))

    async def test_rejection_without_reset_time_holds_through_the_next_usage_check(self):
        with self.store.db:
            self.broker.record_rate_limit('primary', {'status': 'rejected'}, rejected=True)
        status = self.store.get('account_status')
        status['primary']['observed_at'] = time.time() + 1
        with self.store.db:
            self.store.put('account_status', status)
        self.assertEqual(self.broker.select(model='native-default'), 'second')
        later = time.time() + AccountBroker.REJECTION_HOLD_SECONDS + 1
        with patch('coordinator.accounts.time.time', return_value=later):
            self.assertEqual(self.broker.select(model='native-default'), 'primary')

    async def test_threshold_block_without_reset_clears_on_a_newer_usage_check(self):
        with self.store.db:
            self.broker.record_rate_limit('primary', {'status': 'allowed_warning', 'utilization': .99})
        self.assertEqual(self.broker.select(model='native-default'), 'second')
        status = self.store.get('account_status')
        status['primary']['observed_at'] = time.time() + 1
        status['primary']['usage']['seven_day']['utilization'] = 20
        with self.store.db:
            self.store.put('account_status', status)
        self.assertEqual(self.broker.select(model='native-default'), 'primary')

    async def test_any_meter_at_99_percent_excludes_account_and_98_stays_eligible(self):
        resets_at = time.time() + 120
        for key in ('seven_day', 'seven_day_fable'):
            self.meter('primary', key, 97, resets_at)
            self.assertEqual(self.broker.select(model='native-default'), 'primary', key)
            self.meter('primary', key, 98, resets_at)
            self.assertEqual(self.broker.select(model='native-default'), 'primary', key)
            self.meter('primary', key, 99, resets_at)
            self.assertEqual(self.broker.select(model='native-default'), 'second', key)
            self.assertEqual(self.broker.account_state('primary', 'native-default')['state'], 'limited', key)
            self.meter('primary', key, 10, resets_at)

    async def test_a_full_meter_whose_reset_has_passed_does_not_block(self):
        for key in ('seven_day', 'seven_day_fable'):
            self.meter('primary', key, 99, time.time() - 1)
        self.assertEqual(self.broker.account_state('primary', 'native-default'), {'state': 'available'})
        self.assertTrue(self.broker.available(self.store.get('accounts'), 'primary', model='native-default'))

    async def test_earliest_reset_is_the_soonest_account_to_clear_all_its_limits(self):
        now = time.time()
        self.meter('primary', 'seven_day', 99, now + 300)
        self.meter('second', 'seven_day', 99, now + 200)
        with self.store.db:
            self.store.put('account_blocks', {'primary': {'until': now + 100, 'reason': 'quota'}})
        self.assertIsNone(self.broker.select(model='native-default'))
        self.assertEqual(self.broker.earliest_reset(model='native-default'), now + 200)

    async def test_restart_discovery_ignores_live_like_unregistered_hidden_folder(self):
        root = self.root / 'hidden-profiles'
        profiles = {}
        status = {}
        for number in range(1, 5):
            directory = root / ('.torii-%08x' % number)
            directory.mkdir(parents=True)
            (directory / '.claude.json').touch()
            if number <= 3:
                alias = 'account%d' % number
                profiles[alias] = {'config_dir': str(directory.resolve()), 'enabled': True}
                status[alias] = {'identity': {'email': alias + '@example.test', 'logged_in': True}}
        with self.store.db:
            self.store.put('mode', 'group')
            self.store.put('accounts', profiles)
            self.store.put('account_status', status)
        self.store.close()
        self.store = Store(self.root / 'state')
        self.assertEqual(discover_accounts(self.store, root), profiles)

        async def collect(profile):
            return {'identity': {'email': 'account@example.test', 'logged_in': True}}

        await refresh_accounts(self.store, collect)
        self.assertEqual(sorted(self.store.get('accounts')), sorted(profiles))
        self.assertEqual(len(list(root.iterdir())), 4)

    async def test_discovery_only_adopts_recorded_installer_hidden_folder_once(self):
        root = self.root / 'hidden-profiles'
        for name in ('.torii-11111111', '.torii-22222222'):
            directory = root / name
            directory.mkdir(parents=True)
            (directory / '.claude.json').touch()
        recorded = str((root / '.torii-22222222').resolve())
        with self.store.db:
            self.store.put('setup_claude_dir', recorded)
        profiles = discover_accounts(self.store, root)
        self.assertEqual(profiles['torii-22222222']['config_dir'], recorded)
        self.assertNotIn('torii-11111111', profiles)
        self.assertIsNone(self.store.get('setup_claude_dir'))
        self.assertEqual(discover_accounts(self.store, root), profiles)

    async def test_discovery_skips_folders_without_a_login_and_keeps_an_owner_disable(self):
        root = self.root / 'profiles'
        for alias in ('ready', 'empty'):
            (root / alias).mkdir(parents=True)
        (root / 'ready' / '.claude.json').write_text('{}')
        self.assertEqual(sorted(discover_accounts(self.store, root)), ['primary', 'ready', 'second'])
        with self.store.db:
            control_api.call(self.store, 'account.disable', {'alias': 'ready'})
        discover_accounts(self.store, root)

        async def signed_in(account):
            return {'identity': {'email': Path(account['config_dir']).name + '@example.com', 'logged_in': True},
                    'usage': {}, 'observed_at': time.time()}
        await refresh_accounts(self.store, signed_in)
        self.assertFalse(self.store.get('accounts')['ready']['enabled'])

    def test_discovery_skips_previous_and_removed_profile_folders(self):
        root = self.root / 'profiles'
        for name in ('previous', 'removed', 'fresh'):
            directory = root / name
            directory.mkdir(parents=True)
            (directory / '.claude.json').write_text(name)
        profiles = self.store.get('accounts')
        profiles['primary']['previous_config_dirs'] = [str(root / 'previous' / '..' / 'previous')]
        with self.store.db:
            self.store.put('accounts', profiles)
            self.store.put('account_dirs_removed', [str(root / 'removed' / '..' / 'removed')])
        self.assertEqual(sorted(discover_accounts(self.store, root)), ['fresh', 'primary', 'second'])
        self.assertEqual((root / 'previous' / '.claude.json').read_text(), 'previous')
        self.assertEqual((root / 'removed' / '.claude.json').read_text(), 'removed')

    def test_remove_preserves_files_and_discovery_does_not_restore_account(self):
        old = self.root / 'older'
        old.mkdir()
        (old / '.claude.json').write_text('older login marker')
        (self.root / 'primary' / '.claude.json').write_text('current login marker')
        profiles = self.store.get('accounts')
        profiles['primary']['previous_config_dirs'] = [str(old)]
        snapshots = self.store.get('account_status')
        snapshots['primary']['identity']['logged_in'] = False
        with self.store.db:
            self.store.put('accounts', profiles)
            self.store.put('account_status', snapshots)
            result = control_api.call(self.store, 'account.remove', {'alias': 'primary'})
        self.assertTrue(result.ok, result.text)
        self.assertEqual(result.text, 'Removed primary@example.com.')
        self.assertEqual(self.store.get('account_dirs_removed'), [str((self.root / 'primary').resolve()),
                                                                  str(old.resolve())])
        self.assertEqual(sorted(discover_accounts(self.store, self.root)), ['second'])
        self.assertEqual(self.transcript.read_text(), '{}\n')
        self.assertEqual((old / '.claude.json').read_text(), 'older login marker')
        self.assertEqual((self.root / 'primary' / '.claude.json').read_text(), 'current login marker')

    def test_transcript_roots_find_history_in_previous_config_dirs(self):
        new = self.root / 'new-primary'
        new.mkdir()
        profiles = self.store.get('accounts')
        profiles['primary']['config_dir'] = str(new)
        profiles['primary']['previous_config_dirs'] = [str(self.root / 'primary'), str(self.root / 'primary')]
        with self.store.db:
            self.store.put('accounts', profiles)
            self.store.put('session_transcripts', {})
        with patch('coordinator.accounts.Path.home', return_value=self.root):
            roots = self.broker.transcript_roots()
            self.assertIn(str(new), roots)
            self.assertEqual(roots.count(str(self.root / 'primary')), 1)
            self.assertEqual(self.broker.transcript(self.sid), str(self.transcript.resolve()))
        resumed = Path(self.broker.resume_transcript('primary', self.sid, str(self.transcript)))
        self.assertTrue(resumed.is_symlink())
        self.assertEqual(resumed.resolve(), self.transcript.resolve())
        self.assertEqual(self.transcript.read_text(), '{}\n')

    async def test_a_weekly_reset_that_already_passed_does_not_rank_first(self):
        now = time.time()
        self.meter('primary', 'seven_day', 40, now - 60)
        self.meter('second', 'seven_day', 40, now + 2 * 86400)
        self.assertEqual(self.broker.select(model='native-default'), 'second')

    async def test_selection_uses_soonest_all_model_weekly_reset(self):
        status = self.store.get('account_status')
        now = time.time()
        status['primary']['usage']['seven_day']['resets_at'] = now + 5000
        status['second']['usage']['seven_day']['resets_at'] = now + 1000
        with self.store.db:
            self.store.put('account_status', status)
        self.assertEqual(self.broker.select(model='native-default'), 'second')

    async def test_stale_usage_still_chooses_by_last_known_meters_and_disabled_never_starts(self):
        status = self.store.get('account_status')
        for alias in ('primary', 'second'):
            status[alias]['observed_at'] = time.time() - 3600
        status['primary']['usage']['seven_day']['utilization'] = 99
        with self.store.db:
            self.store.put('account_status', status)
        self.assertEqual(self.broker.account_state('primary', 'native-default')['state'], 'limited')
        self.assertEqual(self.broker.account_state('second', 'native-default')['state'], 'stale')
        self.assertEqual(self.broker.select(model='native-default'), 'second')
        with self.store.db:
            accounts = self.store.get('accounts')
            accounts['second']['enabled'] = False
            self.store.put('accounts', accounts)
        self.assertIsNone(self.broker.select(model='native-default'))
        self.assertEqual(self.broker.earliest_reset(model='native-default'), status['primary']['usage']['seven_day']['resets_at'])

    async def test_earliest_reset_survives_an_account_whose_usage_was_reset(self):
        from coordinator.controls import op_account_reset
        status = self.store.get('account_status')
        status['second']['usage']['seven_day']['utilization'] = 100
        with self.store.db:
            self.store.put('account_status', status)
            op_account_reset(self.store, None, 'primary')
        self.assertEqual(self.broker.select(model='native-default'), 'primary')
        self.assertEqual(self.broker.earliest_reset(model='native-default'), status['second']['usage']['seven_day']['resets_at'])

    async def test_signed_out_default_accounts_are_dropped(self):
        with self.store.db:
            self.store.put('accounts', {'default': {'config_dir': str(self.root), 'enabled': True},
                                        **self.store.get('accounts')})
            self.store.put('account_status', {'default': {'identity': {'logged_in': False}},
                                              **self.store.get('account_status')})
            self.store.put('active_account', 'primary')
        discover_accounts(self.store, self.root / 'missing')
        self.assertNotIn('default', self.store.get('accounts'))
        self.assertNotIn('default', self.store.get('account_status'))

    def test_activate_reselects_free_accounts_and_logs_only_changes(self):
        with self.store.db:
            self.store.put('active_account', 'second')
        with self.assertLogs('coordinator.accounts', level='INFO') as logs:
            self.assertEqual(self.broker.activate(model='native-default'), 'primary')
            self.assertEqual(self.broker.activate(model='native-default'), 'primary')
        self.assertEqual(len(logs.output), 1)
        self.assertIn('from=second@example.com to=primary@example.com reason=sooner_reset', logs.output[0])
        self.assertEqual(listed_accounts(self.store, 'native-default')[1], 'primary')
        with patch.object(self.store, 'put', wraps=self.store.put) as put:
            self.assertEqual(self.broker.activate(model='native-default'), 'primary')
        put.assert_not_called()

    def test_each_meter_crossing_is_logged_once_and_rotation_explains_it(self):
        self.broker.activate(model='native-default')
        reset = time.time() + 3600
        with self.assertLogs('coordinator.accounts', level='INFO') as logs:
            for meter in ('seven_day', 'seven_day_fable'):
                for used in (98, 99):
                    self.broker.record_usage('primary', {meter: {'utilization': used, 'resets_at': reset}})
            self.assertEqual(self.broker.activate(model='native-default'), 'second')
        crossings = [line for line in logs.output if 'switch point crossed' in line]
        self.assertEqual(len(crossings), 2)
        for meter in ('seven_day', 'seven_day_fable'):
            self.assertEqual(sum('meter=' + meter + ' ' in line for line in crossings), 1)
        self.assertIn('reason=seven_day used_pct=99 switch_point_pct=99.0 reset=', logs.output[-1])
        self.assertTrue(all('account=primary@example.com' in line for line in crossings))

    def test_native_rate_limit_crossing_and_hold_are_logged_once(self):
        reset = time.time() + 3600
        info = {'rateLimitType': 'seven_day', 'utilization': .99, 'resetsAt': reset}
        with self.assertLogs('coordinator.accounts', level='INFO') as logs:
            self.broker.record_rate_limit('primary', info)
            self.broker.record_rate_limit('primary', info)
        self.assertEqual(sum('switch point crossed' in line for line in logs.output), 1)
        self.assertEqual(sum('account limited' in line for line in logs.output), 1)

    async def test_poll_and_native_rate_limit_share_crossing_detection(self):
        reset = time.time() + 3600
        async def collect(account):
            return {'usage': {'seven_day': {'utilization': 99, 'resets_at': reset}}}
        with self.assertLogs('coordinator.accounts', level='INFO') as logs:
            await refresh_accounts(self.store, collect, aliases=['primary'])
            await refresh_accounts(self.store, collect, aliases=['primary'])
            self.broker.record_rate_limit('primary', {'rateLimitType': 'seven_day', 'utilization': .99, 'resetsAt': reset})
        self.assertEqual(sum('switch point crossed' in line for line in logs.output), 1)

    def test_activate_logs_hold_disabled_and_signed_out_reasons(self):
        for reason in ('hold', 'disabled', 'signed_out'):
            with self.subTest(reason=reason):
                accounts = self.store.get('accounts')
                status = self.store.get('account_status')
                accounts['primary']['enabled'] = reason != 'disabled'
                status['primary']['identity']['logged_in'] = reason != 'signed_out'
                with self.store.db:
                    self.store.put('accounts', accounts)
                    self.store.put('account_status', status)
                    self.store.put('active_account', 'primary')
                    self.store.put('account_blocks', {'primary': {'reason': 'quota', 'until': time.time() + 600}} if reason == 'hold' else {})
                with self.assertLogs('coordinator.accounts', level='INFO') as logs:
                    self.assertEqual(self.broker.activate(model='native-default'), 'second')
                self.assertIn('reason=' + reason, logs.output[0])
                if reason == 'hold':
                    self.assertIn('hold_reason=quota until=', logs.output[0])

    async def test_discovery_reselects_the_soonest_account(self):
        self.assertEqual(self.broker.select(model='native-default'), 'primary')
        with self.store.db:
            self.store.put('active_account', 'second')
        discover_accounts(self.store, self.root / 'missing')
        self.assertEqual(AccountBroker(self.store).activate(model='native-default'), 'primary')

    async def test_accounts_display_email_and_an_unreadable_identity_is_not_listed(self):
        status = self.store.get('account_status')
        status['second'].pop('identity')
        with self.store.db:
            self.store.put('account_status', status)
        self.assertEqual(account_label(self.store, 'primary'), 'primary@example.com')
        self.assertNotIn('second', listed_accounts(self.store, 'native-default')[0])

    async def test_a_profile_without_a_signed_in_login_is_not_listed_or_chosen(self):
        spare = self.root / 'spare'
        spare.mkdir()
        with self.store.db:
            accounts = self.store.get('accounts')
            accounts['spare'] = {'config_dir': str(spare), 'enabled': False, 'awaiting_login': True}
            accounts['primary']['enabled'] = False
            self.store.put('accounts', accounts)
            status = self.store.get('account_status')
            status['second']['identity'] = {'logged_in': False}
            status['second']['error'] = 'login_required'
            self.store.put('account_status', status)
        self.assertEqual(listed_accounts(self.store, 'native-default'), (['primary'], None))
        self.assertIsNone(self.broker.select(model='native-default'))

    def test_every_claude_environment_names_the_profile_and_drops_service_and_login_variables(self):
        leaked = {'CLAUDE_CONFIG_DIR': '/home-login', 'CMUX_SOCKET_PATH': 'x', 'CODEX_THREAD_ID': 'x',
                  'TELEGRAM_BOT_TOKEN': 'x', 'TORII_VAULT_KEY': 'x', 'ANTHROPIC_API_KEY': 'x',
                  'ANTHROPIC_AUTH_TOKEN': 'x', 'ANTHROPIC_PROFILE': 'x', 'CLAUDE_SECURESTORAGE_CONFIG_DIR': 'x',
                  'ANTHROPIC_FOUNDRY_API_KEY': 'x', 'AWS_BEARER_TOKEN_BEDROCK': 'x',
                  'ANTHROPIC_CUSTOM_HEADERS': 'x', 'CLAUDE_CODE_CLIENT_KEY_PASSPHRASE': 'x',
                  'CLAUDE_CODE_OAUTH_TOKEN': 'x', 'CLAUDECODE': 'x'}
        profile = str(self.root / 'primary')
        with patch.dict(os.environ, dict(leaked, TORII_ENV_KEEP='kept')):
            for env in (AccountBroker(self.store).environment('primary'),
                        AccountBroker.profile_environment({'config_dir': profile}),
                        AccountBroker.signin_environment(profile)):
                self.assertEqual(env['CLAUDE_CONFIG_DIR'], profile)
                self.assertEqual(env['TORII_ENV_KEEP'], 'kept')
                self.assertEqual([name for name in leaked if name in env], ['CLAUDE_CONFIG_DIR'])
