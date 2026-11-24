import time
from unittest.mock import AsyncMock, patch

from coordinator import parent, parent_state
from coordinator.accounts import AccountBroker
from coordinator.codex_accounts import CodexBroker, _status_from_limits, credit_line, refresh_codex_accounts
from coordinator.health import _codex
from coordinator.providers import RunResult
from coordinator.workers import WorkerPool
from tests.test_codex_accounts import CodexFixture, TOPIC


NOTICE = 'ChatGPT weekly limits are used; ChatGPT work now runs on credits.'
WEEK = 10080


def meter(used, reset, has, balance, limit='codex', **extra):
    return dict({'limitId': limit, 'primary': {'usedPercent': used, 'windowDurationMins': WEEK, 'resetsAt': reset},
                 'secondary': None, 'credits': {'hasCredits': has, 'unlimited': False, 'balance': balance}}, **extra)


class CodexCreditTests(CodexFixture):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.automatic()
        self.register({'codex': (self.default, 100, 24)})
        self.broker = CodexBroker(self.store)
        self.fund('codex', '22849')

    def fund(self, alias, balance, has=True, unlimited=False, allowed=False, **fields):
        status = self.store.get('codex_account_status')
        status[alias].update(credits={'has_credits': has, 'unlimited': unlimited, 'balance': balance},
                             usage_allowed=allowed, **fields)
        with self.store.db:
            self.store.put('codex_account_status', status)

    def credit_notices(self):
        return [row[0] for row in self.store.db.execute('SELECT text FROM outbox WHERE text=?', (NOTICE,))]

    async def test_included_allowance_precedes_descending_credit_balances_for_workers_and_parents(self):
        homes = {alias: self.home(alias) for alias in ('free-a', 'free-b', 'low', 'high', 'unlimited')}
        self.register({'codex': (self.default, 100, 1), 'low': (homes['low'], 100, 2),
                       'high': (homes['high'], 100, 72), 'unlimited': (homes['unlimited'], 100, 96),
                       'free-a': (homes['free-a'], 10, 24), 'free-b': (homes['free-b'], 50, 48)})
        self.fund('codex', '23082')
        self.fund('low', '22849')
        self.fund('high', '39485')
        self.fund('unlimited', None, has=False, unlimited=True)
        expected = ['free-a', 'free-b', 'unlimited', 'high', 'codex', 'low']
        attempted = set()
        for alias in expected:
            self.assertEqual(self.broker.select(attempted), alias)
            attempted.add(alias)
        self.assertIsNone(self.broker.select(attempted))
        self.assertEqual(self.broker.selection_order(), expected)
        lines = _codex(self.store).split('\n')[1:]
        labels = [line.split(' ', 1)[1].split(': ', 1)[0] for line in lines]
        self.assertEqual(labels, [self.broker.label(alias) for alias in expected])
        self.assertIn(' · active', lines[0])
        self.assertEqual(self.broker.parent_account(), 'free-a')
        with self.store.db:
            self.broker.record_rate_limit('free-a', None, rejected=True)
            self.broker.record_rate_limit('free-b', None, rejected=True)
        self.assertEqual(self.broker.parent_account(), 'unlimited')
        self.assertEqual(self.credit_notices(), [])

    async def test_credit_sort_is_numeric_and_ties_use_alias(self):
        a, b = self.home('a'), self.home('b')
        self.register({'codex': (self.default, 100, 1), 'a': (a, 100, 24), 'b': (b, 100, 48)})
        self.fund('codex', '9')
        self.fund('a', '10.25')
        self.fund('b', '10.25')
        self.assertEqual(self.broker.select(), 'a')
        self.assertEqual(self.broker.select({'a'}), 'b')

    async def test_zero_missing_invalid_or_unconfirmed_credits_keep_the_account_limited(self):
        for balance, has in [('0', True), ('-1', True), (None, True), ('NaN', True),
                             ('Infinity', True), ('invalid', True), ('10', False)]:
            with self.subTest(balance=balance, has=has):
                self.fund('codex', balance, has=has)
                self.assertIsNone(self.broker.select())
                self.assertIsNone(self.broker.parent_account())
                self.assertTrue(self.broker.exhausted('codex'))
        self.fund('codex', '0.001')
        self.assertEqual(self.broker.select(), 'codex')

    async def test_disabled_and_signed_out_funded_accounts_do_not_run(self):
        accounts = self.store.get('codex_accounts')
        accounts['codex']['enabled'] = False
        self.store.put('codex_accounts', accounts)
        self.assertIsNone(self.broker.select())
        accounts['codex']['enabled'] = True
        self.store.put('codex_accounts', accounts)
        status = self.store.get('codex_account_status')
        status['codex']['identity']['logged_in'] = False
        self.store.put('codex_account_status', status)
        self.assertIsNone(self.broker.select())

    async def test_server_spend_blocks_are_not_bypassed_by_a_balance(self):
        for reached, spend, expected in [(None, False, 'codex'), ('rate_limit_reached', False, 'codex'),
                                          (None, True, None), ('workspace_owner_credits_depleted', False, None),
                                          ('workspace_member_usage_limit_reached', False, None)]:
            with self.subTest(reached=reached, spend=spend):
                snapshot = _status_from_limits({'ordinaryUsageAllowed': False, 'rateLimits': {
                    'credits': {'hasCredits': True, 'unlimited': False, 'balance': '22849'},
                    'rateLimitReachedType': reached, 'spendControlReached': spend}})
                status = self.store.get('codex_account_status')
                status['codex'].update(snapshot)
                self.store.put('codex_account_status', status)
                self.assertEqual(self.broker.select(), expected)

    async def test_threshold_hold_clears_with_credits_and_meter_updates_keep_credits_current(self):
        self.store.put('codex_account_blocks', {'codex': {
            'reason': 'threshold', 'until': time.time() + 3600, 'observed_at': time.time()}})
        self.assertEqual(self.broker.select(), 'codex')
        for balance, has, expected in [('22849', True, 'codex'), ('0', False, None)]:
            with self.store.db:
                self.broker.record_rate_limit('codex', {
                    'limitId': 'codex', 'primary': {'usedPercent': 100, 'windowDurationMins': 10080,
                                                   'resetsAt': time.time() + 3600},
                    'credits': {'hasCredits': has, 'unlimited': False, 'balance': balance}})
            self.assertEqual(self.broker.select(), expected)

    async def test_model_meter_credits_do_not_replace_the_account_balance(self):
        with self.store.db:
            self.broker.record_rate_limit('codex', {'limitId': 'model', 'credits': {
                'hasCredits': False, 'unlimited': False, 'balance': '0'}})
        self.assertEqual(self.broker.select(), 'codex')
        self.assertEqual(self.broker.snapshot('codex')['credits']['balance'], '22849')

    async def test_sparse_spend_control_notification_blocks_credits(self):
        with self.store.db:
            self.broker.record_rate_limit('codex', {'spendControlReached': True})
        self.assertIsNone(self.broker.select())

    async def test_credit_rejection_requires_new_positive_credits_and_explicit_server_permission(self):
        with self.store.db:
            self.broker.record_rate_limit('codex', None, rejected=True)
        block = self.store.get('codex_account_blocks')['codex']
        self.assertTrue(block['credits'])
        for delta, balance, allowed, error in [(0, '22849', True, None), (1, '22849', False, None),
                                               (1, '22849', None, None), (1, '0', True, None),
                                               (1, '22849', True, 'timeout')]:
            with self.subTest(delta=delta, balance=balance, allowed=allowed, error=error):
                self.fund('codex', balance, allowed=False, credits_allowed=allowed,
                          observed_at=block['observed_at'] + delta, error=error)
                with patch('coordinator.codex_accounts.time.time', return_value=block['observed_at'] + 600):
                    self.assertIsNone(self.broker.select())
                    self.assertIsNone(self.broker.parent_account())
        self.fund('codex', '22849', allowed=False, credits_allowed=True,
                  observed_at=block['observed_at'] + 1, error=None)
        self.assertEqual(self.broker.select(), 'codex')
        self.assertNotIn('codex', self.broker.blocks())

    async def test_meter_updates_do_not_replace_or_clear_a_credit_rejection(self):
        limits = {'primary': {'usedPercent': 100, 'windowDurationMins': 10080,
                              'resetsAt': time.time() + 3600},
                  'credits': {'hasCredits': False, 'unlimited': False, 'balance': '0'}}
        with self.store.db:
            self.broker.record_rate_limit('codex', limits, rejected=True)
        block = self.broker.blocks()['codex']
        with self.store.db:
            self.broker.record_rate_limit('codex', limits)
        self.assertEqual(self.broker.blocks()['codex'], block)
        self.assertIsNone(self.broker.select())

        async def collect(account):
            return _status_from_limits({'ordinaryUsageAllowed': True, 'rateLimits': {
                'credits': {'hasCredits': True, 'unlimited': False, 'balance': '10'},
                'spendControlReached': False, 'rateLimitReachedType': None}})

        await refresh_codex_accounts(self.store, collect)
        self.assertEqual(self.broker.select(), 'codex')

    async def test_parent_depletion_notification_before_rejection_keeps_the_credit_hold(self):
        limits = {'primary': {'usedPercent': 100, 'windowDurationMins': 10080,
                              'resetsAt': time.time() + 3600},
                  'credits': {'hasCredits': False, 'unlimited': False, 'balance': '0'}}
        with self.store.db:
            self.broker.record_rate_limit('codex', limits, parent=True)
            self.broker.record_rate_limit('codex', limits, rejected=True, parent=True)
        block = self.broker.blocks()['codex']
        self.assertTrue(block['credits'])
        self.assertIsNone(block['until'])
        self.fund('codex', '0', allowed=False, observed_at=block['observed_at'] + 1)
        with patch('coordinator.codex_accounts.time.time', return_value=block['observed_at'] + 8 * 86400):
            self.assertIsNone(self.broker.parent_account())

    async def test_rejected_turn_can_rotate_to_another_funded_account(self):
        other = self.home('other')
        self.register({'codex': (self.default, 100, 24), 'other': (other, 100, 48)})
        self.fund('codex', '23082')
        self.fund('other', '22849')
        calls = []

        async def run(provider, prompt, cwd, session, **options):
            alias = options['account_alias']
            calls.append((alias, session))
            return RunResult('thread-1', success=alias == 'other', quota_limited=alias == 'codex')

        result = await self.broker.run(run, 'codex', 'work', self.root, None)
        self.assertTrue(result.success)
        self.assertEqual(calls, [('codex', None), ('other', 'thread-1')])
        self.assertEqual(self.broker.select(), 'other')
        self.assertEqual(self.credit_notices(), [NOTICE])

    async def test_credit_notice_is_once_at_home_across_workers_and_an_outage(self):
        home = self.separate_home_topic()

        async def run(provider, prompt, cwd, session, **options):
            return RunResult(session, success=True)

        for _ in range(3):
            await CodexBroker(self.store).run(run, 'codex', 'work', self.root, None)
        self.assertEqual(self.credit_notices(), [NOTICE])
        self.assertEqual(self.store.db.execute('SELECT topic FROM outbox WHERE text=?', (NOTICE,)).fetchone()[0], home)
        pool = WorkerPool(self.store, None, AccountBroker(self.store), codex_accounts=self.broker)
        for _ in range(3):
            self.assertFalse(pool.check_recovery())
        self.assertEqual(self.credit_notices(), [NOTICE])
        self.register({'codex': (self.default, 10, 24)})
        pool.check_recovery()
        self.assertIsNone(self.store.get('codex_credits_since'))
        self.register({'codex': (self.default, 100, 24)})
        self.fund('codex', '10')
        await self.broker.run(run, 'codex', 'work', self.root, None)
        self.assertEqual(self.credit_notices(), [NOTICE, NOTICE])

    async def test_no_home_does_not_consume_the_credit_notice(self):
        self.store.put('coordinator_home_topic', None)
        self.broker.notify_credit_use('codex')
        self.assertIsNone(self.store.get('codex_credits_since'))
        self.store.put('coordinator_home_topic', TOPIC)
        self.broker.notify_credit_use('codex')
        self.assertEqual(self.credit_notices(), [NOTICE])

    async def test_credits_keep_work_on_chatgpt_and_allow_chatgpt_recovery(self):
        pool = WorkerPool(self.store, None, AccountBroker(self.store), codex_accounts=self.broker)
        with patch.object(pool.accounts, 'select', return_value='claude'):
            self.assertFalse(pool.can_move(self.worker()))
            self.fund('codex', '0')
            self.assertTrue(pool.can_move(self.worker()))
        self.store.put('fallback_since', {'codex': time.time()})
        self.assertFalse(pool.check_recovery())
        self.fund('codex', '22849')
        self.assertTrue(pool.check_recovery())
        self.assertFalse(pool.check_recovery())
        self.assertEqual(self.store.get('fallback_since'), {})

    async def test_chat_pick_keeps_chatgpt_with_credits_and_moves_only_at_zero(self):
        key = 'coordinator'
        self.store.put(key + '_selection', {'provider': 'codex', 'revision': 1})
        self.assertFalse(parent_state.provider_out(self.store, key, 'codex'))
        self.fund('codex', '0')
        self.assertTrue(parent_state.provider_out(self.store, key, 'codex'))

    async def test_parent_retry_does_not_remove_a_rejected_credit_block(self):
        with self.store.db:
            self.broker.record_rate_limit('codex', None, rejected=True, parent=True)
            self.store.put('pause_notice', {'key': ['codex']})
        with patch('coordinator.codex_accounts.refresh_codex_accounts', AsyncMock()):
            await parent.retry_paused(self.store, 'pause')
        self.assertIsNone(self.broker.parent_account())

    async def test_health_shows_formatted_balances_including_zero_and_unlimited(self):
        self.assertIn(' · 22k credits', _codex(self.store))
        self.assertNotIn('🔴', _codex(self.store))
        self.fund('codex', '0')
        self.assertIn('🔴', _codex(self.store))
        self.assertIn(' · 0 credits', _codex(self.store))
        self.fund('codex', None, unlimited=True)
        self.assertIn(' · unlimited credits', _codex(self.store))
        cases = (('0', '0 credits'), ('0.999', '0 credits'), ('12.500', '12 credits'),
                 ('850.999', '850 credits'), ('999.9999999999999999999999999999', '999 credits'),
                 ('1000', '1k credits'), ('1000.999', '1k credits'),
                 ('1999.999999999999999999999999999', '1k credits'),
                 ('22849.2978915', '22k credits'), ('39485.76555925', '39k credits'),
                 ('999999.999', '999k credits'), ('1000000', '1000k credits'),
                 (None, '? credits'), ('-1', '? credits'), ('NaN', '? credits'),
                 ('Infinity', '? credits'), ('invalid', '? credits'))
        for balance, expected in cases:
            with self.subTest(balance=balance):
                self.assertEqual(credit_line({'credits': {'balance': balance}}), expected)
        self.assertEqual(credit_line({}), '? credits')

    async def test_manual_selection_uses_active_account_credits_until_zero(self):
        self.automatic(False)
        self.assertEqual(self.broker.select(), 'codex')
        self.assertEqual(self.broker.parent_account(), 'codex')
        self.fund('codex', '0')
        self.assertIsNone(self.broker.select())
        self.assertIsNone(self.broker.parent_account())


class CodexCreditRecoveryTests(CodexFixture):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.automatic()
        self.now = time.time()
        self.register({'codex': (self.default, 100, 1)})
        self.broker = CodexBroker(self.store)

    def fund(self, balance, has=True, allowed=False):
        status = self.store.get('codex_account_status')
        status['codex'].update(credits={'has_credits': has, 'unlimited': False, 'balance': balance},
                               usage_allowed=allowed)
        with self.store.db:
            self.store.put('codex_account_status', status)

    async def refresh(self, response):
        async def collect(account):
            return _status_from_limits(response)
        await refresh_codex_accounts(self.store, collect)

    async def after_weekly_reset(self):
        later = self.now + 2 * 3600
        with patch('coordinator.codex_accounts.time.time', return_value=later):
            await self.refresh({'ordinaryUsageAllowed': True, 'rateLimits': meter(
                0, later + 7 * 86400, False, '0', spendControlReached=False, rateLimitReachedType=None)})
            return self.broker.select(), self.broker.parent_account(), dict(self.broker.blocks())

    async def test_a_worker_drains_credits_then_weekly_allowance_returns(self):
        self.fund('0.05')
        drained = meter(100, self.now + 3600, False, '0')

        async def run(provider, prompt, cwd, session, **options):
            return RunResult('thread-1', success=False, quota_limited=True, failure_code='quota_limited',
                             rate_limit_info=drained)

        await self.broker.run(run, 'codex', 'work', self.root, None)
        selected, parent, blocks = await self.after_weekly_reset()
        self.assertEqual((selected, parent, blocks), ('codex', 'codex', {}))

    async def test_b_parent_sees_zero_balance_then_weekly_allowance_returns(self):
        self.fund('0.05')
        drained = meter(100, self.now + 3600, False, '0', spendControlReached=None, rateLimitReachedType=None)
        with self.store.db:
            self.broker.record_rate_limit('codex', drained, parent=True)
        if self.broker.exhausted('codex', parent=True):
            with self.store.db:
                self.broker.record_rate_limit('codex', drained, rejected=True, parent=True)
        selected, parent, blocks = await self.after_weekly_reset()
        self.assertEqual((selected, parent, blocks), ('codex', 'codex', {}))

    async def test_c_live_shaped_credit_mode_runs_and_survives_the_live_update(self):
        live = {'ordinaryUsageAllowed': False, 'rateLimits': meter(
            100, self.now + 3600, True, '39485.8532317500', spendControlReached=False,
            rateLimitReachedType='rate_limit_reached')}
        await self.refresh(live)
        self.assertEqual(self.broker.select(), 'codex')
        self.assertEqual(self.broker.parent_account(), 'codex')
        update = meter(100, self.now + 3600, True, '39485.8070967500', spendControlReached=None,
                       rateLimitReachedType=None)
        with self.store.db:
            self.broker.record_rate_limit('codex', update, parent=True)
        self.assertFalse(self.broker.exhausted('codex', parent=True))
        self.assertEqual(self.broker.snapshot('codex')['credits']['balance'], '39485.8070967500')
        self.assertEqual(self.broker.select(), 'codex')

    async def test_d_one_rejection_in_credit_mode_then_live_status_still_funded(self):
        live = {'ordinaryUsageAllowed': False, 'rateLimits': meter(
            100, self.now + 3600, True, '39485.8532317500', spendControlReached=False,
            rateLimitReachedType='rate_limit_reached')}
        await self.refresh(live)
        with self.store.db:
            self.broker.record_rate_limit('codex', None, rejected=True)
        with patch('coordinator.codex_accounts.time.time', return_value=self.now + 600):
            await self.refresh(live)
            self.assertEqual(self.broker.select(), 'codex')

    async def test_e_model_meter_rejection_on_funded_account_under_allowance_keeps_its_hold(self):
        self.register({'codex': (self.default, 10, 24)})
        self.fund('100', allowed=True)
        with self.store.db:
            self.broker.record_rate_limit('codex', meter(100, self.now + 3600, True, '100', limit='gpt-6-sol'),
                                          rejected=True)
        self.assertIsNone(self.broker.select())
        with patch('coordinator.codex_accounts.time.time', return_value=self.now + 5):
            await self.refresh({'ordinaryUsageAllowed': True, 'rateLimits': meter(
                10, self.now + 86400, True, '100', spendControlReached=False, rateLimitReachedType=None)})
            self.assertIsNone(self.broker.select())
            self.assertIsNone(self.broker.parent_account())
        with patch('coordinator.codex_accounts.time.time', return_value=self.now + 3601):
            self.assertEqual(self.broker.select(), 'codex')
            self.assertEqual(self.broker.parent_account(), 'codex')
            self.assertEqual(self.broker.blocks(), {})

    async def test_f_credits_null_update_keeps_the_funded_account(self):
        self.fund('39485')
        update = meter(100, self.now + 3600, True, '0')
        update['credits'] = None
        with self.store.db:
            self.broker.record_rate_limit('codex', update, parent=True)
        self.assertEqual(self.broker.select(), 'codex')
