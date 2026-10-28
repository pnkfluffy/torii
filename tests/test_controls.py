import asyncio
import json
from pathlib import Path
import re
import os
import stat
import time
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from coordinator.controls import _HELP, _USAGE, _meter, _until, handle_control
from coordinator.formatting import markdown_to_html
from coordinator.store import Store


class ControlTests(unittest.TestCase):
    def test_model_parent_opencode_refusals(self):
        from coordinator import control_api
        self.store.db.execute('UPDATE topics SET cwd=?', (self.temp.name,))
        self.store.put('coordinator_home_topic', '1:2')
        self.store.put('opencode_accounts', {'example-host': {'model': 'example-model'}})
        cases = (
            ({'topic': '1:2'}, True, 'example-host', 'The home chat stays on Claude or ChatGPT.'),
            ({}, True, 'example-host', 'Pick OpenCode for one chat with topic.'),
            ({'topic': '1:3'}, None, 'example-host', 'OpenCode is off. Turn on delegation.opencode first.'),
            ({'topic': '1:3'}, False, 'example-host', 'OpenCode is off. Turn on delegation.opencode first.'),
            ({'topic': '1:3'}, 'true', 'example-host', 'OpenCode is off. Turn on delegation.opencode first.'),
            ({'topic': '1:3'}, True, None, 'Select an OpenCode account with opencode.use first.'),
            ({'topic': '1:3'}, True, 'missing', 'Select an OpenCode account with opencode.use first.'),
            ({'topic': '1:3', 'effort': 'high'}, True, 'example-host', 'OpenCode chat agents have no thinking setting.'),
            ({'topic': '1:3', 'model': 'wrong'}, True, 'example-host',
             'The OpenCode model comes from the account that opencode.use selects: example-model.'),
        )
        for params, enabled, active, expected in cases:
            with self.subTest(params=params, enabled=enabled, active=active):
                self.store.put('opencode_enabled', enabled)
                self.store.put('opencode_active_account', active)
                before = [tuple(row) for row in self.store.db.execute('SELECT * FROM settings ORDER BY key')]
                result = control_api.call(self.store, 'model.parent', dict(params, provider='opencode'))
                self.assertFalse(result.ok, result.text)
                self.assertEqual(result.text, expected)
                self.assertEqual([tuple(row) for row in self.store.db.execute('SELECT * FROM settings ORDER BY key')], before)
                self.assertEqual(self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0], 0)
                self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 0)

    def test_model_parent_opencode_stores_null_model_and_effort(self):
        from coordinator import control_api, parent_state
        self.store.db.execute('UPDATE topics SET cwd=?', (self.temp.name,))
        self.store.put('coordinator_home_topic', '1:2')
        self.store.put('opencode_enabled', True)
        self.store.put('opencode_accounts', {'example-host': {'model': 'example-model'}, 'other': {'model': 'example-next-model'}})
        self.store.put('opencode_active_account', 'example-host')
        key = 'coordinator:1:3'
        self.store.put(key + '_selection_retry_at', 123)
        self.store.put(key + '_selection_notice', 'paused')
        self.store.put(key + '_selection_cleared', True)
        self.store.put(key + '_selection_revision', 4)
        for model in (None, 'example-model'):
            with self.subTest(model=model):
                result = control_api.call(self.store, 'model.parent', {'topic': '1:3', 'provider': 'opencode', 'model': model})
                self.assertTrue(result.ok, result.text)
                expected = {'provider': 'opencode', 'model': None, 'effort': None, 'revision': 5}
                self.assertEqual(result.data, {'topic': '1:3', 'selection': expected})
                self.assertEqual(self.store.get(key + '_selection'), expected)
                if model is None:
                    self.assertEqual(result.text, 'Chat 1:3 agent: OpenCode on account example-host (example-model).'
                                     ' Active turns and native background tasks settle before a change. Worker settings stay unchanged.')
        for suffix in ('_selection_retry_at', '_selection_notice', '_selection_cleared'):
            self.assertIsNone(self.store.get(key + suffix))
        self.assertIsNone(self.store.get('parent_selection'))
        self.assertIsNone(self.store.get('coordinator_selection'))
        switched = control_api.call(self.store, 'opencode.use', {'alias': 'other'})
        self.assertTrue(switched.ok, switched.text)
        self.assertEqual(parent_state.effective_agent(self.store, key)['model'], 'example-next-model')
        self.assertEqual(self.store.get(key + '_selection'), expected)
        cleared = control_api.call(self.store, 'model.parent', {'topic': '1:3'})
        self.assertTrue(cleared.ok, cleared.text)
        self.assertIsNone(self.store.get(key + '_selection'))
        self.assertTrue(self.store.get(key + '_selection_cleared'))

    def test_model_parent_opencode_subscription_uses_subscription_name_in_reply_and_status_card(self):
        from coordinator import control_api
        from coordinator.controls import _status_card
        self.store.db.execute('UPDATE topics SET cwd=?', (self.temp.name,))
        self.store.put('coordinator_home_topic', '1:2')
        self.store.put('opencode_enabled', True)
        self.store.put('opencode_accounts', {'go': {'provider': 'opencode-go', 'model': 'example-model'}})
        self.store.put('opencode_active_account', 'go')

        result = control_api.call(self.store, 'model.parent', {'topic': '1:3', 'provider': 'opencode'})

        self.assertTrue(result.ok, result.text)
        self.assertEqual(result.text, 'Chat 1:3 agent: OpenCode subscription (example-model).'
                         ' Active turns and native background tasks settle before a change. Worker settings stay unchanged.')
        line = next(row for row in _status_card(self.store, '1:3').splitlines() if row.startswith('Chat agent:'))
        self.assertEqual(line, 'Chat agent: OpenCode subscription example-model · picked for this chat')

    def test_model_parent_opencode_repeat_says_already_set_without_writing(self):
        from coordinator import control_api
        self.store.db.execute('UPDATE topics SET cwd=?', (self.temp.name,))
        self.store.put('coordinator_home_topic', '1:2')
        self.store.put('opencode_enabled', True)
        self.store.put('opencode_accounts', {'example-host': {'model': 'example-model'}})
        self.store.put('opencode_active_account', 'example-host')
        params = {'topic': '1:3', 'provider': 'opencode'}
        first = control_api.call(self.store, 'model.parent', params)
        self.assertTrue(first.ok, first.text)
        before = [tuple(row) for row in self.store.db.execute('SELECT * FROM settings ORDER BY key')]

        repeated = control_api.call(self.store, 'model.parent', params)

        self.assertTrue(repeated.ok, repeated.text)
        self.assertEqual(repeated.text, 'Chat 1:3 agent is already set to OpenCode on account example-host (example-model).')
        self.assertEqual([tuple(row) for row in self.store.db.execute('SELECT * FROM settings ORDER BY key')], before)

    def test_status_card_opencode_line_shows_spend_and_no_thinking(self):
        from coordinator import opencode_accounts
        from coordinator.controls import _status_card
        from tests.test_opencode_parent import OpenCodeParentTests
        from tests.support import until
        saved_home, account_home = os.environ.get('HOME'), opencode_accounts.account_home
        self.addCleanup(lambda: self.assertEqual(os.environ.get('HOME'), saved_home))
        self.addCleanup(lambda: self.assertIs(opencode_accounts.account_home, account_home))
        chat = OpenCodeParentTests()
        chat.addCleanup = self.addCleanup
        chat.setUp()

        def line():
            return next(row for row in _status_card(chat.store, 'chat').splitlines() if row.startswith('Chat agent:'))

        async def check():
            try:
                chat.scenario(no_cost=True)
                session = await chat.start()
                self.assertEqual(line(), 'Chat agent: OpenCode one example-model · picked for this chat')
                await chat.send(session, chat.row())
                await until(session.idle)
                self.assertEqual(line(), 'Chat agent: OpenCode one example-model · picked for this chat')
                await session.stop()
                chat.scenario(cost=1.234)
                session = await chat.start()
                await chat.send(session, chat.row())
                await until(session.idle)
                expected = 'Chat agent: OpenCode one example-model · API spend (estimate) $1.23 this session · picked for this chat'
                self.assertEqual(line(), expected)
                await session.stop()
                chat.store.close()
                chat.store = Store(chat.root / 'state')
                chat.addCleanup(chat.store.close)
                self.assertEqual(line(), expected)
                chat.scenario(cost=2.345)
                loaded = await chat.start()
                self.assertEqual(loaded.session_id, session.session_id)
                self.assertEqual(chat.calls('session/load')[-1]['message']['params']['sessionId'], session.session_id)
                self.assertEqual(line(), expected)
                await chat.send(loaded, chat.row())
                await until(loaded.idle)
                self.assertEqual(line(), 'Chat agent: OpenCode one example-model · API spend (estimate) $2.35 this session · picked for this chat')
                await loaded.stop()
                for model, cost in (('example-model', 0.001726), ('synthetic-no-price', 0)):
                    with self.subTest(model=model, native_cost=cost):
                        accounts = chat.store.get('opencode_accounts')
                        accounts['one'] = {'provider': 'opencode-go', 'model': model}
                        chat.store.put('opencode_accounts', accounts)
                        chat.store.put('opencode_account_status', {'one': {'observed_at': time.time(), 'usage': {
                            'rolling': {'status': 'ok', 'utilization': 10, 'resets_at': time.time() + 3600}}}})
                        chat.scenario(cost=cost)
                        subscription = await chat.start()
                        await chat.send(subscription, chat.row())
                        await until(subscription.idle)
                        config = json.loads((chat.data / 'config/opencode/opencode.json').read_text())
                        self.assertNotIn('models', config['provider']['opencode-go'])
                        self.assertEqual(chat.store.get(chat.session_key + '_opencode')['usd'], cost)
                        self.assertEqual(line(), 'Chat agent: OpenCode subscription %s · picked for this chat' % model)
                        await subscription.stop()
                chat.store.put(chat.session_key + '_host', {'provider': 'codex', 'model': 'gpt-6.1-sol',
                                                          'picked': False, 'effort': 'max'})
                self.assertEqual(line(), 'Chat agent: ChatGPT gpt-6.1-sol · xhigh thinking · default · waiting to apply '
                                 'OpenCode subscription synthetic-no-price · picked for this chat')
                chat.store.put(chat.session_key + '_host', None)
            finally:
                await chat.asyncTearDown()

        asyncio.run(check())

    def test_status_card_claude_and_codex_lines_unchanged(self):
        from coordinator.controls import _status_card
        self.store.put('coordinator_home_topic', '1:2')
        for provider, model, effort, expected in (
                ('claude', 'opus', 'max', 'Claude opus · max thinking · picked for this chat'),
                ('codex', 'gpt-6.1-sol', 'max', 'ChatGPT gpt-6.1-sol · xhigh thinking · picked for this chat')):
            with self.subTest(provider=provider):
                self.store.put('coordinator:1:3_selection', {'provider': provider, 'model': model, 'effort': effort, 'revision': 1})
                line = next(row for row in _status_card(self.store, '1:3').splitlines() if row.startswith('Chat agent:'))
                self.assertEqual(line, 'Chat agent: ' + expected)
                self.store.put('coordinator:1:3_selection', None)
                self.store.put('coordinator:1:3_provider', provider)
                self.store.put('coordinator:1:3_host', {'provider': provider, 'model': model, 'effort': effort, 'picked': False})
                line = next(row for row in _status_card(self.store, '1:3').splitlines() if row.startswith('Chat agent:'))
                self.assertEqual(line, 'Chat agent: ' + expected.replace('picked for this chat', 'default'))
                self.store.put('coordinator:1:3_host', None)

    def test_status_card_displays_generic_opencode_model_ids_safely(self):
        from coordinator.controls import _status_card
        self.store.put('coordinator_home_topic', '1:2')
        self.store.put('opencode_active_account', 'example-host')
        self.store.put('coordinator:1:3_selection', {
            'provider': 'opencode', 'model': None, 'effort': None, 'revision': 1})
        models = (
            ('example-host/example-model', 'example-host/example-model'),
            ('a' * 200, 'a' * 200),
            ('a' * 201, 'unknown'),
            ('model with space', 'unknown'),
            ('model\nwith newline', 'unknown'),
        )
        for model, shown in models:
            with self.subTest(model=model):
                self.store.put('opencode_accounts', {'example-host': {'model': model}})
                lines = _status_card(self.store, '1:3').splitlines()
                chat_agent = next(line for line in lines if line.startswith('Chat agent:'))
                model_line = next(line for line in lines if line.startswith('Models:'))
                self.assertEqual(chat_agent, 'Chat agent: OpenCode example-host %s · picked for this chat' % shown)
                self.assertEqual(model_line, 'Models: Claude workers opus · chat agent ' + shown)

    def test_status_card_omits_parent_selection_for_chat_with_own_pick(self):
        from coordinator.controls import _status_card
        self.store.put('coordinator_home_topic', '1:2')
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:4',1,4,'other',?,1)",
                              (self.temp.name,))
        self.store.put('parent_selection', {'provider': 'codex', 'model': 'gpt-6.1-sol'})
        self.store.put('coordinator:1:3_selection', {
            'provider': 'claude', 'model': 'claude-opus-5-5', 'effort': 'max', 'revision': 1})

        picked = _status_card(self.store, '1:3')
        unpicked = _status_card(self.store, '1:4')

        self.assertNotIn('Parent selection:', picked)
        self.assertIn('Parent selection: codex gpt-6.1-sol after active work settles.', unpicked)

    def test_d8_chat_policy_status_preview_and_show(self):
        from coordinator import control_api, mcp
        from coordinator.controls import _status_card
        from coordinator.policy import chat_policy, usage_policy
        self.store.put('coordinator_home_topic', '1:2')
        self.store.db.execute('UPDATE topics SET cwd=?', (self.temp.name,))
        baseline = _status_card(self.store, '1:2')
        self.assertNotIn('Delegation:', baseline)
        text = 'Alpha  \n' + 'b' * 100
        for topic, key in (('1:2', 'coordinator'), ('1:3', 'coordinator:1:3')):
            self.store.put(key + '_delegation', {'text': text, 'revision': 1})
            card = _status_card(self.store, topic)
            preview = 'Delegation: own instructions · ' + ' '.join(text.split())[:60] + '…'
            self.assertEqual(card.splitlines()[3], preview)
            shown = control_api.call(self.store, 'policy.show', {'topic': topic})
            self.assertEqual(shown.text, chat_policy(self.store, key))
        card = _status_card(self.store, '1:2')
        self.assertEqual('\n'.join(line for line in card.splitlines() if not line.startswith('Delegation:')), baseline)
        self.assertEqual(control_api.call(self.store, 'policy.show').text, usage_policy(self.store.directory))
        response = mcp.dispatch(self.store,
            {'method': 'tools/call', 'params': {'name': 'policy_show'}}, topic='1:2')
        self.assertEqual(response['content'][0]['text'], usage_policy(self.store.directory) + '\nnull')
        self.store.put('coordinator_delegation', {'text': 'Short.', 'revision': 2})
        self.assertEqual(_status_card(self.store, '1:2').splitlines()[3], 'Delegation: own instructions · Short.')

    def test_claude_use_saves_pick_requests_fresh_usage_and_automatic_clears_it(self):
        from coordinator import control_api
        with self.store.db:
            result = control_api.call(self.store, 'account.use', {'alias': 'work'}, topic='1:3')
        self.assertEqual((result.ok, result.text), (True, 'work@example.com is picked for Claude; its usage is being checked.'))
        self.assertEqual(self.store.get('claude_account_pick'), 'work')
        self.assertEqual(self.store.get('claude_pick_refresh')['topic'], '1:3')
        request = self.store.get('claude_pick_refresh')
        control_api.call(self.store, 'account.use', {'alias': 'work'})
        self.assertGreater(self.store.get('claude_pick_refresh')['id'], request['id'])
        result = control_api.call(self.store, 'accounts.claude_auto')
        self.assertEqual((result.ok, result.text), (True, 'Claude account choice is automatic. Running chats stay on their account while it can take work.'))
        self.assertIsNone(self.store.get('claude_account_pick'))
        self.assertIsNone(self.store.get('claude_pick_refresh'))

    def test_claude_use_refuses_unknown_signed_out_and_disabled_aliases(self):
        from coordinator import control_api
        cases = [('missing', 'not registered'), ('work', 'not signed in'), ('primary', 'disabled')]
        status = self.store.get('account_status')
        status['work']['identity']['logged_in'] = False
        self.store.put('account_status', status)
        accounts = self.store.get('accounts')
        accounts['primary']['enabled'] = False
        self.store.put('accounts', accounts)
        for alias, reason in cases:
            result = control_api.call(self.store, 'account.use', {'alias': alias})
            self.assertEqual(result.state, 'refused')
            self.assertIn(reason, result.text)
        self.assertIsNone(self.store.get('claude_account_pick'))
        self.assertIsNone(self.store.get('claude_pick_refresh'))

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name))
        for alias in ('primary', 'work'):
            (Path(self.temp.name) / alias).mkdir()
        with self.store.db:
            self.store.put('account_status', {alias: {'identity': {'email': alias + '@example.com', 'logged_in': True},
                                                     'observed_at': time.time(), 'usage': {
                                                         'five_hour': {'utilization': 0},
                                                         'seven_day': {'utilization': 0, 'resets_at': time.time() + 3600}}}
                                              for alias in ('primary', 'work')})
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('1:2',1,2,'test','')")
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('1:3',1,3,'other','')")
            self.store.put('accounts', {'primary': {'config_dir': str(Path(self.temp.name) / 'primary'), 'enabled': True},
                                        'work': {'config_dir': str(Path(self.temp.name) / 'work'), 'enabled': True}})

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def command(self, text):
        with self.store.db:
            self.assertTrue(handle_control(self.store, '1:2', 42, text))
        row = self.store.db.execute('SELECT * FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
        self.assertEqual((row['kind'], row['reply_to']), ('status', 42))
        return row['text']


    def account_report(self):
        from coordinator import control_api
        return control_api.call(self.store, 'accounts.list').text

    def test_settings_show_resolves_claude_and_chatgpt_defaults_and_overrides(self):
        from coordinator import control_api
        with self.store.db:
            self.store.put('coordinator_home_topic', '1:2')
            self.store.put('coordinator_provider', 'claude')
        self.assertIn('Models: Claude workers opus · chat agent sonnet', control_api.call(
            self.store, 'settings.show', {'topic': '1:2'}).text)
        for provider, label, setting, default, override in (
                ('claude', 'Claude', 'coordinator_model', 'sonnet', 'claude-sonnet-4-6'),
                ('codex', 'ChatGPT', 'codex_model', 'gpt-6.1-sol', 'gpt-6-luna')):
            with self.subTest(provider=provider), self.store.db:
                self.store.put('coordinator_provider', provider)
                self.store.put(setting, None)
                text = control_api.call(self.store, 'settings.show', {'topic': '1:2'}).text
                self.assertIn('chat agent ' + default, text)
                self.assertIn('Chat agent: ' + label, text)
                self.store.put(setting, override)
                self.assertIn('chat agent ' + override, control_api.call(
                    self.store, 'settings.show', {'topic': '1:2'}).text)

    def test_settings_show_reports_the_running_model_per_channel(self):
        from coordinator import control_api
        with self.store.db:
            self.store.put('coordinator_home_topic', '1:2')
            self.store.put('coordinator_host', {'provider': 'codex', 'model': 'gpt-6.1-sol'})
            self.store.put('coordinator:1:3_host', {'provider': 'claude', 'model': 'claude-opus-5-5'})
            self.store.put('coordinator_provider', 'claude')
            self.store.put('coordinator:1:3_provider', 'codex')
            self.store.db.execute("UPDATE topics SET provider='claude' WHERE id='1:2'")
            self.store.db.execute("UPDATE topics SET provider='codex' WHERE id='1:3'")
            self.store.put('codex_model', 'gpt-6-luna')
        for topic, label, model in (('1:2', 'ChatGPT', 'gpt-6.1-sol'), ('1:3', 'Claude', 'claude-opus-5-5')):
            text = control_api.call(self.store, 'settings.show', {'topic': topic}).text
            self.assertIn('Chat agent: ' + label, text)
            self.assertIn('chat agent ' + model, text)
            self.assertNotIn('service default', text)

    def test_settings_show_labels_the_fallback_chat_provider(self):
        from coordinator import control_api
        text = control_api.call(self.store, 'settings.show', {'topic': '1:2'}).text
        self.assertIn('Chat agent: Claude', text)
        with self.store.db:
            self.store.put('accounts', {})
            self.store.put('codex_accounts', {'gpt': {
                'config_dir': str(Path(self.temp.name) / 'primary'), 'enabled': True}})
            self.store.put('codex_account_status', {'gpt': {'identity': {
                'email': 'gpt@example.com', 'logged_in': True}}})
            self.store.put('worker_model', None)
        text = control_api.call(self.store, 'settings.show', {'topic': '1:2'}).text
        self.assertIn('Chat agent: ChatGPT', text)
        self.assertIn('Models: Claude workers native default · chat agent gpt-6.1-sol', text)





    def test_showing_an_account_that_is_no_longer_registered_says_so(self):
        text = self.command('/accounts show default')
        self.assertTrue(text.startswith('Commands take no arguments now. Use the buttons below.'), text)
        self.assertNotIn('Usage:', text)

    def test_service_restart_takes_a_long_multi_line_reason_from_telegram(self):
        reason = 'First line of why.\n' + 'Second line, long. ' * 30
        self.command('/service restart ' + reason)
        request = self.store.db.execute("SELECT params FROM service_requests WHERE op='service.restart'").fetchone()
        self.assertEqual(json.loads(request[0])['reason'], reason.strip())
        self.assertIsNone(self.store.get('restart_card'))

    def test_usage_meter_fills_with_what_is_left(self):
        cases = (
            (0, '🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩 100%'),
            (1, '🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩 99%'),
            (50, '🟩🟩🟩🟩🟩⬜⬜⬜⬜⬜ 50%'),
            (89.5, '🟩🟩⬜⬜⬜⬜⬜⬜⬜⬜ 10.5%'),
            (90, '🟩⬜⬜⬜⬜⬜⬜⬜⬜⬜ 10%'),
            (97, '🟩⬜⬜⬜⬜⬜⬜⬜⬜⬜ 3%'),
            (99.5, '🟩⬜⬜⬜⬜⬜⬜⬜⬜⬜ 0.5%'),
            (100, '⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜ 0%'),
            (120, '⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜ 0%'),
            (-5, '🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩 100%'),
        )
        for used, expected in cases:
            with self.subTest(used=used):
                self.assertEqual(_meter(used), expected)

    def test_reserve_paused_card_shows_zero_usage_as_full_and_pause_reason(self):
        from coordinator.controls import _account_card
        now = 1900000000
        status = self.store.get('account_status')
        status['primary']['observed_at'] = now
        status['primary']['usage'] = {'five_hour': {'utilization': 0, 'blocked': True},
                                     'seven_day': {'utilization': 81, 'resets_at': now + 11520}}
        with self.store.db:
            self.store.put('account_status', status)
            self.store.put('account_blocks', {'primary': {'reason': 'threshold', 'until': now + 11520}})
        with patch('coordinator.accounts.switch_threshold', return_value=.8), \
                patch('coordinator.controls.switch_threshold', side_effect=lambda store, alias=None: .8 if alias else .98), \
                patch('time.time', return_value=now):
            text = _account_card(self.store, 'primary')
        self.assertIn('5-hour: 🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩 100%', text)
        self.assertIn('Weekly (all models): 🟩🟩⬜⬜⬜⬜⬜⬜⬜⬜ 19%', text)
        self.assertIn('Paused: weekly reached its 20% reserve · back in 3h 12m', text)
        self.assertIn('Fable weekly: no data', text)

    def test_until_reset(self):
        now = 1_000
        with patch('coordinator.controls.time.time', return_value=now):
            self.assertEqual(_until(now + 3 * 24 * 60 * 60 + 4 * 60 * 60), '3d 4h')
            self.assertEqual(_until(now + 90 * 60), '1h 30m')
            self.assertEqual(_until(now - 1), 'now')
            self.assertEqual(_until(None), 'now')

    def test_account_card_reports_missing_usage_without_a_reset(self):
        from coordinator import control_api
        now = time.time()
        with self.store.db:
            self.store.put('account_status', {'work': {
                'identity': {'logged_in': True, 'email': 'work@example.com'},
                'observed_at': now,
                'usage': {'five_hour': {'utilization': 0},
                          'seven_day': {'utilization': 98, 'resets_at': now + 7200}}}})
            self.store.put('account_blocks', {'work': {'until': None, 'reason': 'quota'}})
            card = control_api.call(self.store, 'account.show', {'alias': 'work'}).text
        self.assertIn('5-hour: 🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩 100% · resets later', card)
        self.assertIn('Fable weekly: no data', card)
        self.assertIn('Paused: Claude refused requests · back in 1h 59m', card)

    def test_account_dashboard_without_switching_or_starting_provider(self):
        with self.store.db:
            self.store.put('active_account', 'primary')
            self.store.put('last_account', 'work')
            self.store.put('account_status', {'work': {
                'identity': {'name': 'Work account', 'email': 'work@example.com', 'type': 'Claude Max',
                             'logged_in': True},
                'observed_at': 1900000000,
                'usage': {'five_hour': {'utilization': 12, 'resets_at': '2030-03-17T12:00:00Z'},
                          'seven_day': {'utilization': 43}, 'seven_day_fable': {'utilization': 81}}}})
        with patch('subprocess.Popen', side_effect=AssertionError('process started')):
            text = self.command('/accounts')
        for value in ('work@example.com', '88%', '57%', '19%', '5h', 'week', 'Fable'):
            self.assertIn(value, text)
        self.assertNotIn('Work account', text)
        self.assertEqual(self.store.tasks_list(), [])

    def test_no_available_account_marks_none_active(self):
        with self.store.db:
            self.store.put('account_blocks', {alias: {'until': time.time() + 3600} for alias in ('primary', 'work')})
        text = self.command('/accounts')
        self.assertTrue(text.endswith('No Claude account can take work now.'))
        self.assertNotIn('· active', text)
        self.assertTrue(self.command('/accounts').endswith('No Claude account can take work now.'))

    def test_status_card_names_the_wait_when_every_account_is_limited(self):
        from coordinator import control_api
        with self.store.db:
            self.store.put('account_blocks', {alias: {'until': time.time() + 7200} for alias in ('primary', 'work')})
        text = control_api.call(self.store, 'settings.show', topic='1:2').text
        self.assertRegex(text, r'Account: all accounts limited · resets in 1h 5\dm')
        with self.store.db:
            self.store.put('account_blocks', {})
            for alias in ('primary', 'work'):
                control_api.call(self.store, 'account.disable', {'alias': alias})
        text = control_api.call(self.store, 'settings.show', topic='1:2').text
        self.assertIn('Account: none available\n', text)

    def test_new_identity_never_displays_prior_stream_usage(self):
        with self.store.db:
            self.store.put('account_status', {'primary': {
                'identity': {'email': 'new@example.com', 'logged_in': True}, 'error': 'usage_unavailable',
                'checked_at': 1}})
            self.store.put('model_account_usage', {'primary': {'opus': {'utilization': .91, 'observed_at': 1}}})
        text = self.command('/accounts')
        self.assertIn('new@example.com', text)
        self.assertIn('🟡 new@example.com: ? · ? · ?', text)
        self.assertNotIn('91%', text)
        self.assertNotIn('Last agent observation', text)

    def test_dashboard_distinguishes_missing_stale_and_duplicate_usage(self):
        with self.store.db:
            self.store.put('accounts', dict(self.store.get('accounts'),
                                            spare={'config_dir': self.temp.name, 'enabled': True}))
            self.store.put('account_status', {
                'spare': {'identity': {'email': 'same@example.com', 'logged_in': True}, 'observed_at': 1,
                          'usage': {'seven_day': {'utilization': 0, 'resets_at': 2}},
                          'error': 'timeout', 'checked_at': 3},
                'work': {'identity': {'email': 'same@example.com', 'logged_in': True}}})
        text = self.command('/accounts')
        self.assertIn('🟡 same@example.com: ? · 100% · ?', text)
        self.assertIn('stale since 1970-01-01 00:00:01 UTC', text)
        self.assertIn('stale since unknown', text)
        self.assertIn('· same login as same@example.com', text)
        self.assertNotIn('/accounts refresh', text)

    def test_accounts_render_usage_bars_and_relative_resets(self):
        now = 1_000
        with self.store.db:
            self.store.put('account_status', {'primary': {
                'identity': {'email': 'primary@example.com', 'logged_in': True},
                'usage': {
                    'five_hour': {'utilization': 100, 'resets_at': now - 1},
                    'seven_day_fable': {'utilization': 46, 'resets_at': now + 60 * 60 + 2 * 60},
                    'seven_day': {'utilization': 12.5, 'resets_at': now + 24 * 60 * 60},
                }}, 'work': {'identity': {'email': 'work@example.com', 'logged_in': True}}})
        with patch('coordinator.controls.time.time', return_value=now):
            text = self.command('/accounts')
        self.assertIn('primary@example.com: 0% · 87.5% · 54%', text)
        self.assertIn('(left: 5h · week · Fable)', text)
        self.assertIn('work@example.com: ? · ? · ?', text)
        self.assertEqual(text.count('primary@example.com'), 1)
        self.assertEqual(text.count('work@example.com'), 1)
        self.assertEqual(text.count('? · ? · ?'), 1)
        window_lines = [line for line in text.splitlines()
                        if line.startswith(('5-hour:', 'Fable weekly:', 'Weekly (all models):'))]
        self.assertTrue(all('UTC' not in line for line in window_lines))
        self.assertLess(len(text), 4096)

    def test_setup_and_help_work_without_accounts(self):
        with self.store.db:
            self.store.put('accounts', {})
        for command in ('/setup', '/start'):
            text = self.command(command)
            self.assertIn('Open /accounts to sign one in.', text)
            self.assertIn('/help', text)
        self.assertIn('/projects', self.command('/help'))
        self.assertIn('No accounts', self.command('/accounts'))

    def test_help_has_the_exact_approved_text(self):
        expected = """Torii runs Claude and ChatGPT agents on your projects. Send a normal message in a project's topic to give it work. Send another to continue.

**Accounts**
/accounts - usage, sign-ins, resets, models

**Projects**
/projects - your projects, new projects, this topic's folder

**Work**
/tldr - catch up on this topic since your last message
/waiting - what is waiting on you
/goal CONDITION - keep the agent working until CONDITION holds

**Service**
/health - running agents and system load
/secrets - stored keys: rotate, revoke, ask again
/ping - check that Torii is listening
/help - this list"""
        self.assertEqual(_HELP, expected)
        self.assertEqual(self.command('/help'), expected.replace('/waiting - what is waiting on you\n', ''))
        self.store.put('waits_enabled', True)
        self.assertEqual(self.command('/help'), expected)

    def test_usage_keeps_only_top_commands_and_hidden_aliases(self):
        self.assertEqual(set(_USAGE), {'/help', '/setup', '/start', '/accounts', '/projects', '/update',
                                      '/project', '/health', '/tldr', '/waiting', '/secrets', '/service'})
        for command in ('/accounts', '/projects', '/secrets', '/help', '/health', '/waiting'):
            self.assertEqual(_USAGE[command], command)

    def test_removed_subcommands_render_the_top_card_without_running_their_operations(self):
        from coordinator import control_api
        forms = (
            '/accounts show primary', '/accounts show missing', '/accounts login primary',
            '/accounts enable work', '/accounts disable primary', '/accounts reset work',
            '/accounts use work', '/accounts add', '/accounts add codex', '/accounts cancel',
            '/accounts codex reset gpt', '/accounts codex reset gpt confirm',
            '/accounts codex auto on', '/accounts codex auto off', '/accounts anything',
            '/projects root ' + self.temp.name, '/projects anything',
            '/secrets rotate EXAMPLE_KEY', '/secrets revoke EXAMPLE_KEY',
            '/secrets revoke EXAMPLE_KEY confirm', '/secrets anything', '/help extra', '/health extra',
        )
        with self.store.db:
            self.store.put('codex_accounts', {'gpt': {'config_dir': self.temp.name, 'enabled': True}})
        before = [tuple(row) for row in self.store.db.execute('SELECT * FROM settings ORDER BY key')]
        with patch('coordinator.control_api.call', wraps=control_api.call) as call, \
                patch('coordinator.control_ui.control_report') as render:
            for text in forms:
                with self.subTest(text=text), self.store.db:
                    call.reset_mock()
                    render.reset_mock()
                    self.assertTrue(handle_control(self.store, '1:2', 42, text))
                    command, *args = text.split()
                    op = {'/accounts': 'accounts.list', '/projects': 'projects.list',
                          '/secrets': 'secret.list', '/health': 'health.show'}.get(command)
                    if op:
                        self.assertEqual([entry.args[1] for entry in call.call_args_list], [op])
                        self.assertEqual(call.call_args.args[2], {})
                    else:
                        call.assert_not_called()
                    self.assertEqual(render.call_args.args[3:6], (command, args, 42))
                    self.assertEqual([tuple(row) for row in self.store.db.execute('SELECT * FROM settings ORDER BY key')], before)
        self.assertEqual(self.store.service_requests_pending(), [])
        self.assertEqual(self.store.tasks_list(), [])

    def test_named_codex_reset_operation_refuses_live_usage_below_95_percent(self):
        from coordinator import control_api
        with self.store.db:
            self.store.db.execute("UPDATE topics SET enabled=1 WHERE id='1:2'")
            self.store.put('codex_accounts', {'gpt': {'config_dir': self.temp.name, 'enabled': True}})
            self.store.put('codex_account_status', {'gpt': {'identity': {'logged_in': True},
                                                          'usage': {'seven_day': {'utilization': 99}}}})
        server = AsyncMock()
        server.request.side_effect = [
            {'account': {'type': 'chatgpt'}},
            {'ordinaryUsageAllowed': True, 'rateLimits': {'primary': {
                'usedPercent': 10, 'windowDurationMins': 10080}},
             'rateLimitResetCredits': {'availableCount': 2}},
        ]
        with patch('coordinator.codex_accounts.AppServer', return_value=server), \
                patch('coordinator.codex_accounts.link_home', side_effect=lambda directory: directory):
            result = control_api.call(self.store, 'account.codex_reset', {'alias': 'gpt'}, topic='1:2')
        self.assertFalse(result.ok)
        self.assertEqual(result.text, 'This Codex account is below 95% on its resettable meters')
        self.assertEqual([entry.args[0] for entry in server.request.call_args_list],
                         ['account/read', 'account/rateLimits/read'])
        server.stop.assert_awaited_once()
        self.assertEqual(self.store.service_requests_pending(), [])

    def test_help_renders_only_four_bold_section_headers(self):
        text = self.command('/help')
        rendered = markdown_to_html(text)
        self.assertEqual([line for line in rendered.splitlines() if line.startswith('<b>')],
                         ['<b>Accounts</b>', '<b>Projects</b>', '<b>Work</b>', '<b>Service</b>'])
        self.assertEqual(re.findall(r'<(/?)([a-z]+)(?: [^>]*)?>', rendered),
                         [('', 'b'), ('/', 'b'), ('', 'b'), ('/', 'b'),
                          ('', 'b'), ('/', 'b'), ('', 'b'), ('/', 'b')])
        for placeholder in ('CONDITION',):
            self.assertIn(placeholder, rendered)
        self.assertLessEqual(len(rendered), 4096)

    def test_defaults_and_plain_messages(self):
        text = self.command('/accounts@my_bot')
        for value in ('primary@example.com', '· next'):
            self.assertIn(value, text)
        self.assertNotIn('planning', text)
        self.assertNotIn('last_account', text)
        self.assertNotIn('worker_limit', text)
        self.assertFalse(handle_control(self.store, '1:2', 43, '/workers'))
        with self.store.db:
            self.assertFalse(handle_control(self.store, '1:2', 43, 'implement feature'))
        self.assertEqual(self.store.tasks_list(), [])



    def test_policy_show_uses_file_or_bundled_default(self):
        from coordinator import control_api
        from coordinator.policy import usage_policy
        (self.store.directory / 'USAGE.md').unlink()
        self.assertEqual(control_api.call(self.store, 'policy.show').text,
                         (Path(__file__).resolve().parents[1] / 'coordinator/defaults/USAGE.md').read_text())
        policy = 'Use one worker.\n'
        (self.store.directory / 'USAGE.md').write_text(policy)
        self.assertEqual(control_api.call(self.store, 'policy.show').text, policy)
        self.assertEqual(usage_policy(self.store.directory), policy)

    def test_policy_set_replaces_file_atomically_and_privately(self):
        from coordinator import control_api
        target = self.store.directory / 'USAGE.md'
        target.write_text('Old policy')
        replacements = []
        replace = os.replace
        fsync = os.fsync

        def inspect_replace(source, destination):
            source = Path(source)
            self.assertEqual(source.parent, self.store.directory)
            self.assertEqual(destination, target)
            self.assertEqual(target.read_text(), 'Old policy')
            self.assertEqual(source.read_text(), 'Use one worker.')
            self.assertEqual(stat.S_IMODE(source.stat().st_mode), 0o600)
            replacements.append(source)
            replace(source, destination)

        with patch('coordinator.controls.os.replace', side_effect=inspect_replace), \
                patch('coordinator.controls.os.fsync', wraps=fsync) as synced:
            saved = control_api.call(self.store, 'policy.set', {'text': 'Use one worker.'})
        self.assertTrue(saved.ok)
        self.assertEqual(saved.text, 'Usage policy saved. Coordinators load it with their next message; new workers at launch.')
        self.assertEqual(target.read_text(), 'Use one worker.')
        self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o600)
        self.assertEqual(len(replacements), 1)
        synced.assert_called_once()
        self.assertFalse(replacements[0].exists())

    def test_policy_set_refuses_invalid_text_and_cleans_failed_write(self):
        from coordinator import control_api
        target = self.store.directory / 'USAGE.md'
        target.write_text('Old policy')
        for text in ('', '   ', 'a' * 4001):
            self.assertFalse(control_api.call(self.store, 'policy.set', {'text': text}).ok)
        with patch('coordinator.controls.os.replace', side_effect=OSError('write failed')):
            result = control_api.call(self.store, 'policy.set', {'text': 'New policy'})
        self.assertEqual(result.state, 'failed')
        self.assertEqual(target.read_text(), 'Old policy')
        self.assertEqual(list(self.store.directory.glob('.USAGE-*')), [])

    def test_changes_persist_without_paths(self):
        from coordinator import control_api
        with self.store.db:
            self.assertTrue(control_api.call(self.store, 'account.disable', {'alias': 'primary'}).ok)
            self.assertTrue(control_api.call(self.store, 'model.worker', {'model': 'codex-model'}).ok)
        self.store.close()
        self.store = Store(Path(self.temp.name))
        self.assertIsNone(self.store.get('active_account'))
        self.assertIsNone(self.store.get('account_selection'))
        self.assertEqual(self.store.get('worker_model'), 'codex-model')
        self.assertFalse(self.store.get('accounts')['primary']['enabled'])
        self.assertEqual(self.store.get('accounts')['work']['config_dir'], str(Path(self.temp.name) / 'work'))


    def test_codex_model_control_sets_and_clears_the_setting(self):
        from coordinator import control_api
        with self.store.db:
            reply = control_api.call(self.store, 'model.codex', {'model': 'gpt-6-luna'})
            self.assertTrue(reply.ok)
            self.assertEqual(self.store.get('codex_model'), 'gpt-6-luna')
            cleared = control_api.call(self.store, 'model.codex', {'model': None})
        self.assertIn('Codex CLI default', cleared.text)
        self.assertIsNone(self.store.get('codex_model'))
        self.assertNotIn('/model', self.command('/help'))


    def test_removed_commands_do_not_mutate(self):
        for text in ('/account work', '/model worker opus', '/policy guidance', '/topic enable'):
            before = list(self.store.db.execute("SELECT * FROM settings ORDER BY key"))
            self.assertFalse(handle_control(self.store, '1:2', 42, text))
            self.assertEqual([tuple(r) for r in before],
                             [tuple(r) for r in self.store.db.execute("SELECT * FROM settings ORDER BY key")])
        self.assertNotIn('/accounts select', self.command('/help'))
        self.assertIsNone(self.store.get('active_account'))
        self.assertIsNone(self.store.get('account_selection'))


    def test_rotate_is_gone_and_rotation_needs_no_stored_setting(self):
        from coordinator import control_api
        for text in ('/rotate', '/rotate on', '/rotate off'):
            self.assertFalse(handle_control(self.store, '1:2', 42, text))
        self.assertIsNone(self.store.get('auto_rotate'))
        self.assertNotIn('/rotate', self.command('/help'))
        card = control_api.call(self.store, 'settings.show', topic='1:2').text
        self.assertNotIn('rotation', card.casefold())
        self.assertNotIn('Switch on quota', card)

    def test_blocked_account_card_has_no_manual_pin_operation(self):
        from coordinator import control_api
        until = time.time() + 3 * 3600 + 30
        with self.store.db:
            self.store.put('account_blocks', {'work': {'until': until, 'reason': 'quota'}})
            self.store.put('account_status', {'work': {'identity': {'logged_in': True}, 'observed_at': time.time(), 'usage': {
                'five_hour': {'utilization': 0}, 'seven_day': {'utilization': 40, 'resets_at': until}}}})
            card = control_api.call(self.store, 'account.show', {'alias': 'work'}).text
            result = control_api.call(self.store, 'account.select', {'alias': 'work'})
        self.assertIn('Account: work (not signed in)', card)
        self.assertIn('Paused:', card)
        self.assertFalse(result.ok)
        self.assertIn('Unknown operation', result.text)
        self.assertIsNone(self.store.get('active_account'))

    def test_blocked_window_keeps_real_usage_and_account_hold_gets_one_short_line(self):
        from coordinator import control_api
        from coordinator.account_status import refresh_accounts
        until = time.time() + 5 * 3600 + 30
        with self.store.db:
            self.store.put('account_blocks', {'work': {'until': until, 'reason': 'quota'}})
            self.store.put('account_status', {'work': {'identity': {'logged_in': True}, 'observed_at': time.time(), 'usage': {
                'five_hour': {'utilization': 30, 'resets_at': until},
                'seven_day': {'utilization': 40, 'resets_at': until, 'blocked': True}}}})
            held = control_api.call(self.store, 'account.show', {'alias': 'work'}).text
        async def collect(account):
            return {'observed_at': time.time(), 'usage': {
                key: {'utilization': 30, 'resets_at': until} for key in ('five_hour', 'seven_day_fable', 'seven_day')}}
        asyncio.run(refresh_accounts(self.store, collect, aliases=['work']))
        wide = control_api.call(self.store, 'account.show', {'alias': 'work'}).text
        self.assertIn('Weekly (all models): 🟩🟩🟩🟩🟩🟩⬜⬜⬜⬜ 60% · resets in 5h 0m', held)
        self.assertIn('5-hour: 🟩🟩🟩🟩🟩🟩🟩⬜⬜⬜ 70% · resets in 5h 0m', held)
        self.assertNotIn('Out ·', held)
        self.assertIn('Paused:', held)
        self.assertNotIn('Paused:', wide)
        self.assertIn('Weekly (all models): 🟩🟩🟩🟩🟩🟩🟩⬜⬜⬜ 70%', wide)
        self.assertNotIn('Login: ', wide)
        self.assertNotIn('blocked', wide.casefold())

    def test_coordinator_model_is_an_owner_setting_for_the_next_launch(self):
        from coordinator import control_api
        with self.store.db:
            result = control_api.call(self.store, 'model.coordinator', {'model': 'claude-opus-5-5'})
        self.assertTrue(result.ok)
        self.assertEqual(self.store.get('coordinator_model'), 'claude-opus-5-5')
        self.assertNotIn('/model', self.command('/help'))
        self.assertEqual(control_api.call(self.store, 'settings.get').data['coordinator_model'], 'claude-opus-5-5')


    def test_change_project_folder_only_moves_the_folder(self):
        import tempfile as _tempfile
        moved = Path(_tempfile.mkdtemp())
        self.addCleanup(lambda: __import__('shutil').rmtree(moved, ignore_errors=True))
        with self.store.db:
            self.store.db.execute("UPDATE topics SET cwd='/old/path',provider='claude',session='s-1',"
                                  "name='SharedMcp',enabled=1 WHERE id='1:2'")
        before = dict(self.store.topic('1:2'))
        reply = self.command('/project folder ' + str(moved))
        self.assertEqual(reply, f'Project folder: /old/path → {moved.resolve()}. Sessions and settings unchanged.')
        after = dict(self.store.topic('1:2'))
        self.assertEqual(after['cwd'], str(moved.resolve()))
        for field in ('name', 'provider', 'session', 'enabled', 'waiting_job', 'source_pid'):
            self.assertEqual(after[field], before[field], field)
        self.assertIsNone(self.store.get('session_transcripts'))
        self.assertIn('already', self.command('/project folder ' + str(moved)))

    def test_change_project_folder_refuses_and_names_the_reason(self):
        with self.store.db:
            self.store.db.execute("UPDATE topics SET cwd='/old/path',enabled=1 WHERE id='1:2'")
        self.assertIn('absolute path', self.command('/project folder relative/dir'))
        self.assertIn('not an existing folder', self.command('/project folder /no/such/folder/here'))
        self.assertIn('not an existing folder', self.command('/project folder /etc/hosts'))
        self.assertEqual(self.store.topic('1:2')['cwd'], '/old/path')

    def test_change_project_folder_refuses_while_the_topic_has_work(self):
        import tempfile as _tempfile
        moved = Path(_tempfile.mkdtemp())
        self.addCleanup(lambda: __import__('shutil').rmtree(moved, ignore_errors=True))
        with self.store.db:
            self.store.db.execute("UPDATE topics SET cwd='/old/path',enabled=1 WHERE id='1:2'")
            task = self.store.task_create('1:2', 'work')
        reply = self.command('/project folder ' + str(moved))
        self.assertIn('open work', reply)
        self.assertEqual(self.store.topic('1:2')['cwd'], '/old/path')
        self.store.task_update(task['id'], status='done')
        self.assertIn('→', self.command('/project folder ' + str(moved)))

    def test_an_unbound_topic_has_no_folder_to_move(self):
        self.assertIn('no project folder yet', self.command('/project folder /tmp'))

    def test_cached_usage_is_sanitized(self):
        with self.store.db:
            status = self.store.get('account_status')
            status['work'].update(observed_at=time.time(), usage={'five_hour': {'utilization': 50,
                                   'resets_at': time.time() + 3600}})
            self.store.put('account_status', status)
            self.store.put('account_blocks', {'work': {'until': time.time() + 3600, 'reason': '/secret/token'}})
        text = self.account_report()
        self.assertIn('50%', text)
        self.assertIn('back in ', text)
        self.assertNotIn('/secret', text)

    def test_dropped_commands_are_ignored_and_pause_is_safe(self):
        with self.store.db:
            for index in range(12):
                self.store.task_create('1:2', 'private task ' + str(index))
            self.store.task_create('1:3', 'other task')
        self.assertFalse(handle_control(self.store, '1:2', 42, '/queue'))
        self.assertFalse(handle_control(self.store, '1:2', 42, '/status'))
        text = self.command('/setup')
        self.assertFalse(handle_control(self.store, '1:2', 42, '/settings'))
        self.assertNotIn('private task 0', text)
        for dropped in ('/pause', '/resume', '/pause now', '/resume extra'):
            self.assertFalse(handle_control(self.store, '1:2', 42, dropped))
        self.assertIsNone(self.store.get('paused'))
        self.assertFalse(self.store.topic('1:2')['enabled'])
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0], 13)

    def test_atomic_with_caller_and_no_process_calls(self):
        with patch('subprocess.Popen', side_effect=AssertionError('process started')):
            self.command('/setup')
            self.command('/accounts')
            self.command('/accounts select work')
        count = self.store.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0]
        with self.assertRaises(RuntimeError):
            with self.store.db:
                __import__('coordinator.control_api', fromlist=['call']).call(self.store, 'model.codex', {'model': 'gpt-6-luna'})
                raise RuntimeError('rollback')
        self.assertIsNone(self.store.get('codex_model'))
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], count)

    def test_register_and_configure_without_any_connected_account(self):
        from coordinator import control_api
        with self.store.db:
            self.store.put('accounts', {})
            self.store.put('bot_username', 'torii_test_bot')
        with patch('subprocess.Popen', side_effect=AssertionError('provider started')), \
                patch('coordinator.signin.accounts_root', return_value=Path(self.temp.name) / 'accounts'):
            self.assertIn('Starting Claude sign-in', control_api.call(
                self.store, 'account.add', topic='1:2', source='telegram').text)
            self.assertEqual(self.store.get('accounts'), {})
            self.assertIn('Stopping the Claude sign-in', control_api.call(
                self.store, 'account.signin_cancel', topic='1:2', source='telegram').text)
            self.assertIn('No accounts', self.command('/accounts'))
        self.assertEqual(self.store.tasks_list(), [])


    def test_provider_switches_and_model_reset(self):
        from coordinator import control_api
        with patch('coordinator.controls.shutil.which', return_value=None):
            self.assertIn('not installed', control_api.call(self.store, 'delegation.codex', {'enabled': True}).text)
        with patch('coordinator.controls.shutil.which', return_value='/bin/fake'):
            control_api.call(self.store, 'delegation.codex', {'enabled': False})
        self.assertFalse(self.store.get('codex_enabled'))
        self.assertIsNone(self.store.get('opencode_enabled'))
        with self.store.db:
            self.store.put('account_blocks', {'work': {'until': None}})
        control_api.call(self.store, 'account.reset', {'alias': 'work'})
        self.assertNotIn('work', self.store.get('account_blocks'))

    def test_status_card_shows_usage_as_a_bar_and_the_running_task(self):
        self.store.put('account_status', {'primary': {'identity': {'email': 'primary@example.com', 'logged_in': True},
            'observed_at': time.time(), 'usage': {
            'five_hour': {'utilization': 62, 'resets_at': time.time() + 2 * 3600 + 15 * 60 + 30}}}})
        self.store.task_create('1:2', 'do the thing')

        from coordinator import control_api
        text = control_api.call(self.store, 'settings.show', topic='1:2').text
        self.assertIn('Account: primary@example.com · 🟩🟩🟩🟩⬜⬜⬜⬜⬜⬜ 38% · resets in 2h 15m', text)
        self.assertIn(': open', text.splitlines()[-1])
        self.assertLessEqual(len(text.splitlines()), 12)
        status = self.store.get('account_status')
        status['primary'].update(observed_at=time.time() - 3600, error='timeout')
        with self.store.db:
            self.store.put('account_status', status)
        text = control_api.call(self.store, 'settings.show', topic='1:2').text
        self.assertRegex(text, r'Account: primary@example\.com · \S+ 38% · resets in 2h \d+m · stale since 20\d\d-')

    def test_topic_command_is_removed_and_setup_can_enable_new_topic(self):
        from coordinator import control_api
        self.assertFalse(handle_control(self.store, '1:2', 42, '/topic enable'))
        self.assertNotIn('/topic', self.command('/help'))
        with self.store.db:
            result = control_api.call(self.store, 'topic.enable', {}, topic='1:2')
        self.assertTrue(result.ok)
        self.assertTrue(self.store.topic('1:2')['enabled'])




if __name__ == '__main__':
    unittest.main()
