"""Offline Piece 1 checks against the shared Grok ACP fake."""

import ast
import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import control_api, control_ui, grok_accounts, signin
from coordinator.grok_accounts import AcpClient, AcpError, GrokBroker, collect_grok_status
from coordinator.providers import RunResult
from coordinator.service import Service
from coordinator.store import Store
from tests.fake_grok import FIXTURE, install_fake_grok


class GrokAccountsTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='grok-test-')
        self.root = Path(self.temp.name)
        self.binary = install_fake_grok(self.root / 'bin')
        self.store = Store(self.root / 'state')
        self.path = patch.dict(os.environ, {'PATH': str(self.binary.parent) + os.pathsep + os.environ['PATH']})
        self.path.start()

    def tearDown(self):
        self.path.stop()
        self.store.close()
        self.temp.cleanup()

    def configure(self, **accounts):
        (self.binary.parent / 'accounts.json').write_text(json.dumps(accounts))

    def home(self, name='one'):
        home = self.root / name
        home.mkdir(exist_ok=True)
        return home

    def calls(self):
        path = self.binary.parent / 'calls.jsonl'
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    async def status(self, name='one', **config):
        self.configure(**{name: dict(signed_in=True, **config)})
        return await collect_grok_status({'config_dir': str(self.home(name))}, timeout=3)

    def register(self, alias='one', enabled=True, logged_in=True, usage=None, **snapshot):
        home = self.home(alias)
        with self.store.db:
            profiles = self.store.get('grok_accounts', {}) or {}
            profiles[alias] = {'config_dir': str(home), 'enabled': enabled}
            self.store.put('grok_accounts', profiles)
            snapshots = self.store.get('grok_account_status', {}) or {}
            snapshots[alias] = {'identity': {'logged_in': logged_in, 'email': alias + '@example.test'},
                                'usage': usage or {}, 'observed_at': time_now(), **snapshot}
            self.store.put('grok_account_status', snapshots)
        return home

    def call(self, op, **params):
        with self.store.db:
            return control_api.call(self.store, op, params)

    async def client(self, **config):
        self.configure(one=config)
        client = AcpClient('grok', grok_accounts.grok_environment(self.home()))
        await client.start()
        self.addAsyncCleanup(client.stop)
        return client

    async def test_environment_uses_named_home_and_removes_inherited_credentials(self):
        home = self.home()
        parent = {'HOME': str(self.root), 'XAI_API_KEY': 'test-only-value', 'GROK_AUTH_PATH': '/unused',
                  'GROK_HOME': '/unused', 'GROK_OTHER': 'test-only-value', 'XAI_OTHER': 'test-only-value',
                  'TELEGRAM_BOT_TOKEN': 'test-only-value', 'BOT_TOKEN': 'test-only-value',
                  'TORII_VAULT_KEY': 'test-only-value', 'TORII_VAULT_KEY_FILE': '/unused', 'CMUX_SOCKET': '/unused'}
        self.configure(one={'signed_in': True})
        with patch.dict(os.environ, parent):
            env = grok_accounts.grok_environment(home)
            self.assertEqual({key: env[key] for key in ('GROK_HOME', 'GROK_DISABLE_AUTOUPDATER', 'GROK_LOGIN_DEVICE_FLOW', 'HOME')},
                             {'GROK_HOME': str(home), 'GROK_DISABLE_AUTOUPDATER': '1',
                              'GROK_LOGIN_DEVICE_FLOW': '1', 'HOME': str(self.root)})
            self.assertFalse((set(parent) - {'HOME', 'GROK_HOME'}) & set(env))
            result = await collect_grok_status({'config_dir': str(home)}, timeout=3)
        self.assertNotIn('error', result)
        for call in self.calls():
            self.assertFalse((set(parent) - {'HOME', 'GROK_HOME'}) & set(call['env_names']))
            self.assertNotIn('test-only-value', json.dumps(call))
            self.assertEqual(set(call), {'message', 'home', 'argv', 'env_names'})

    def test_config_exact_keys_private_mode_installer_and_idempotence(self):
        home = self.home()
        path = home / 'config.toml'
        path.write_text('[cli]\ninstaller = "npm"\nauto_update = true\n')
        grok_accounts.write_config(home)
        first = path.read_bytes()
        grok_accounts.write_config(home)
        self.assertEqual(path.read_bytes(), first)
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        section = None
        actual = {}
        for line in path.read_text().splitlines():
            if line.startswith('['):
                section = line[1:-1]
                actual[section] = {}
            elif '=' in line:
                key, value = line.split('=', 1)
                actual[section][key.strip()] = json.loads(value.split('#', 1)[0].strip())
        expected = {'cli': {'installer': 'npm', 'auto_update': False, 'use_leader': False},
                    'auth': {'preferred_method': 'oidc', 'login_device_flow': True},
                    'features': {'telemetry': False, 'ask_user_question': False, 'auto_wake': False},
                    'telemetry': {'trace_upload': False, 'otel_enabled': False},
                    'claude_compat': {'imported': True}}
        for provider in ('claude', 'cursor', 'codex'):
            expected['compat.' + provider] = {'skills': False, 'rules': False, 'agents': False,
                                              'mcps': False, 'hooks': False, 'sessions': False}
        self.assertEqual(actual, expected)
        self.assertIn('background completion or queued input can still start turns', path.read_text())
        self.assertIn('repo MCP/LSP, hooks, permissions, instructions and skills', path.read_text())

    def test_config_keeps_unmanaged_keys(self):
        home = self.home()
        path = home / 'config.toml'
        path.write_text('[cli]\ninstaller = "npm"\ncolor = true\n[extra]\nsetting = "keep"\n')
        grok_accounts.write_config(home)
        self.assertIn('color = true', path.read_text())
        self.assertIn('[extra]\nsetting = "keep"', path.read_text())

    def test_roots_are_private_and_only_config_only_torii_home_is_trashed(self):
        trash = self.root / 'trash'
        trash.mkdir()
        with patch('coordinator.grok_accounts.Path.home', return_value=self.root), \
                patch('coordinator.grok_accounts.host_os.trash_directory', return_value=trash):
            root = grok_accounts.accounts_root()
            self.assertEqual(root, self.root / '.grok-accounts')
            self.assertEqual(stat.S_IMODE(root.stat().st_mode), 0o700)
            home = root / '.torii-1234abcd'
            grok_accounts.write_config(home)
            (trash / home.name).mkdir()
            grok_accounts.clear_home(home)
            self.assertFalse(home.exists())
            self.assertEqual(len(list(trash.iterdir())), 2)
            home.mkdir()
            (home / 'signed-in').write_text('keep')
            grok_accounts.clear_home(home)
            self.assertEqual((home / 'signed-in').read_text(), 'keep')
            outside = self.home('outside')
            grok_accounts.clear_home(outside)
            self.assertTrue(outside.is_dir())

    async def test_status_exact_wire_sequence_and_weekly_used_scale(self):
        result = await self.status(used=37, reset='2026-10-20T03:04:05+09:00')
        self.assertNotIn('error', result)
        self.assertEqual(result['usage'], {'seven_day': {'utilization': 37, 'resets_at': 1792433045.0}})
        self.assertEqual(result['identity'], {'logged_in': True, 'email': 'worker@example.test', 'type': 'SuperGrok'})
        self.assertTrue(result['usage_allowed'])
        self.assertIs(result['on_demand_enabled'], False)
        self.assertEqual(result['onDemandCap'], {'val': 5000})
        calls = self.calls()
        self.assertEqual([call['message']['method'] for call in calls],
                         ['initialize', 'authenticate', '_x.ai/auth/info', '_x.ai/billing'])
        self.assertEqual(calls[0]['message']['params'], {'protocolVersion': 1, 'clientCapabilities': {
            'fs': {'readTextFile': False, 'writeTextFile': False}, 'terminal': False},
            'clientInfo': {'name': 'torii', 'version': '1'}})
        self.assertEqual(calls[1]['message']['params'], {'methodId': 'cached_token'})
        for call in calls:
            self.assertEqual(call['message']['jsonrpc'], '2.0')
            self.assertEqual(call['argv'], ['--no-auto-update', 'agent', '--no-leader', 'stdio'])

    async def test_signed_out_never_requests_device_login(self):
        self.configure(one={'signed_in': False, 'auth_info': {'email': 'cached@example.test'}})
        result = await collect_grok_status({'config_dir': str(self.home())}, timeout=3)
        self.assertEqual((result['error'], result['identity']), ('login_required', {'logged_in': False}))
        self.assertEqual([call['message']['method'] for call in self.calls()], ['initialize', 'authenticate'])

    async def test_missing_home_and_missing_binary(self):
        self.assertEqual((await collect_grok_status({'config_dir': str(self.root / 'missing')}))['error'], 'login_required')
        self.assertEqual((await collect_grok_status({'config_dir': str(self.home())}, str(self.root / 'missing')))['error'],
                         'cli_unavailable')
        self.assertEqual(self.calls(), [])

    async def test_gate_user_and_team_blocks_refuse_usage(self):
        for config in ({'gate': {'message': 'upgrade', 'url': None, 'label': None}},
                       {'auth_info': {'userBlockedReason': 'blocked'}},
                       {'auth_info': {'teamBlockedReasons': ['blocked']}}):
            with self.subTest(config=config):
                result = await self.status(**config)
                self.assertFalse(result['usage_allowed'])
                self.assertEqual(result['identity']['type'], 'needs SuperGrok or X Premium+')

    async def test_known_auth_and_identity_blocks_survive_billing_failure(self):
        for config in ({'gate': {'message': 'upgrade'}}, {'auth_info': {'userBlockedReason': 'blocked'}}):
            self.configure(one={'signed_in': True, 'hang': ['_x.ai/billing'], **config})
            result = await collect_grok_status({'config_dir': str(self.home())}, timeout=0.3)
            self.assertEqual(result['error'], 'timeout')
            self.assertFalse(result['usage_allowed'])
            self.assertEqual(result['identity']['type'], 'needs SuperGrok or X Premium+')
            self.register()
            self.store.put('grok_account_status', {'one': result})
            self.store.put('grok_active_account', 'one')
            self.assertIsNone(GrokBroker(self.store).select())

    async def test_unknown_periods_are_generic_and_fraction_is_not_rescaled(self):
        for period in ('USAGE_PERIOD_TYPE_MONTHLY', 'USAGE_PERIOD_TYPE_UNSPECIFIED', 'future_period', 'WEEKLY', None):
            with self.subTest(period=period):
                result = await self.status(used=42.5, period=period, reset='2026-06-08T00:00:00Z')
                self.assertEqual(result['usage'], {'limit': {'utilization': 42.5, 'resets_at': 1780876800.0}})

    async def test_absent_nullable_and_bad_meter_fields_stay_unknown(self):
        for config in (None, {}, {'currentPeriod': {'type': 'USAGE_PERIOD_TYPE_WEEKLY'}},
                       {'creditUsagePercent': None}, {'creditUsagePercent': True}, {'creditUsagePercent': '42.5'}):
            with self.subTest(config=config):
                result = await self.status(billing={'config': config})
                window, = result['usage'].values()
                self.assertNotIn('utilization', window)
                self.assertNotIn('resets_at', window)
        for reset in (None, 'tomorrow', '2026-06-08', '2026-06-08T00:00:00'):
            result = await self.status(reset=reset)
            self.assertNotIn('resets_at', result['usage']['seven_day'])
        result = await self.status(reset='2026-06-08t00:00:00z')
        self.assertEqual(result['usage']['seven_day']['resets_at'], 1780876800)

    async def test_two_homes_keep_independent_snapshots(self):
        self.register('one')
        self.register('two')
        self.store.put('grok_enabled', True)
        self.configure(one={'signed_in': True, 'used': 37, 'email': 'one@example.test'},
                       two={'signed_in': True, 'used': 100, 'email': 'two@example.test', 'reset': '2099-01-01T00:00:00Z'})
        self.assertFalse(await grok_accounts.refresh_grok_accounts(self.store))
        broker = GrokBroker(self.store)
        self.assertEqual(broker.snapshot('one')['usage']['seven_day']['utilization'], 37)
        self.assertEqual(broker.snapshot('two')['usage']['seven_day']['utilization'], 100)
        self.assertFalse(broker.exhausted('one'))
        self.assertTrue(broker.exhausted('two'))
        self.assertEqual(broker.account_state('two'), {'state': 'limited', 'until': 4070908800.0})
        self.assertEqual({call['home'] for call in self.calls()}, {'one', 'two'})

    async def test_timeout_and_malformed_replies_stop_process(self):
        for config, expected in (({'hang': True}, 'timeout'), ({'malformed': True}, 'usage_unavailable')):
            self.configure(one=config)
            result = await collect_grok_status({'config_dir': str(self.home())}, timeout=0.2)
            self.assertEqual(result['error'], expected)

    async def test_acp_rejects_agent_requests_and_preserves_protocol_errors(self):
        client = await self.client(client_request=True)
        result = await asyncio.wait_for(client.request('initialize'), 3)
        self.assertEqual(result['protocolVersion'], 1)
        response = self.calls()[1]['message']
        self.assertEqual(response, {'jsonrpc': '2.0', 'id': 'client-request',
                                    'error': {'code': -32601, 'message': 'Method not found'}})
        with self.assertRaises(AcpError) as context:
            await client.request('x.ai/auth/info')
        self.assertEqual(context.exception.code, -32601)

    async def test_acp_stop_bounds_hang_and_reaps_only_its_process(self):
        client = await self.client(hang=True)
        pending = asyncio.create_task(client.request('initialize'))
        await asyncio.sleep(0.05)
        await asyncio.wait_for(client.stop(), 4)
        self.assertIsNotNone(client.process.returncode)
        outcome, = await asyncio.gather(pending, return_exceptions=True)
        self.assertIsInstance(outcome, ValueError)

    async def test_shared_fake_device_approve_deny_cancel_and_never(self):
        for outcome in ('approve', 'deny', 'never'):
            client = await self.client(device=outcome)
            await client.request('initialize')
            pending = asyncio.create_task(client.request('authenticate', {'methodId': 'grok.com', '_meta': {'request_seq': 7}}))
            await asyncio.sleep(0)
            url = await client.request('_x.ai/auth/get_url')
            self.assertEqual(url, {'auth_url': 'https://accounts.x.ai/oauth2/device?user_code=ABCD-EFGH', 'mode': 'device'})
            if outcome == 'approve':
                self.assertEqual((await pending)['_meta']['email'], 'worker@example.test')
            elif outcome == 'deny':
                with self.assertRaises(AcpError):
                    await pending
            else:
                cancelled = await client.request('_x.ai/auth/cancel', {'request_seq': 8})
                self.assertEqual(cancelled, {'cancelled': False})
                self.assertFalse(pending.done())
                self.assertEqual(await client.request('_x.ai/auth/cancel', {'request_seq': 7}), {'cancelled': True})
                with self.assertRaises(AcpError) as context:
                    await pending
                self.assertEqual(context.exception.message, 'Authentication cancelled')
            await client.stop()

    async def test_shared_fake_prompt_chunks_interject_cancel_quota_and_eof(self):
        client = await self.client(signed_in=True, prompt_delay=10)
        await client.request('initialize')
        self.assertEqual(await client.request('session/new', {'cwd': '/workspace', 'mcpServers': []}), {'sessionId': 'session-test'})
        self.assertEqual(await client.request('session/load', {'sessionId': 'session-test'}), {})
        pending = asyncio.create_task(client.request('session/prompt', {'sessionId': 'session-test', 'prompt': []}))
        await asyncio.sleep(0)
        self.assertEqual(await client.request('_x.ai/interject', {'sessionId': 'session-test', 'text': 'change',
                                                                 'interjectionId': 'test'}), {'status': 'queued'})
        await client.send({'method': 'session/cancel', 'params': {'sessionId': 'session-test'}})
        self.assertEqual(await pending, {'stopReason': 'cancelled'})
        self.assertEqual([message['method'] for message in client.notifications],
                         ['session/update', 'session/update', '_x.ai/session/interjection'])
        client.process.stdin.close()
        self.assertEqual(await asyncio.wait_for(client.process.wait(), 2), 0)
        quota = await self.client(signed_in=True, quota=True)
        with self.assertRaises(AcpError) as context:
            await quota.request('session/prompt', {'sessionId': 'session-test'})
        self.assertEqual((context.exception.code, context.exception.data),
                         (-32003, {'message': 'You have used all your free usage.'}))

    async def test_shared_fake_recorded_errors_and_sigterm(self):
        client = await self.client()
        self.assertIsNone((await client.request('_x.ai/auth/info'))['email'])
        for method, code in (('session/new', -32000), ('session/load', -32603)):
            with self.assertRaises(AcpError) as context:
                await client.request(method)
            self.assertEqual(context.exception.code, code)
        os.kill(client.process.pid, signal.SIGTERM)
        self.assertEqual(await asyncio.wait_for(client.process.wait(), 2), 0)

    async def test_shared_fake_end_turn_and_headless_recorded_error(self):
        client = await self.client(signed_in=True)
        result = await client.request('session/prompt', {'sessionId': 'session-test', 'prompt': []})
        self.assertEqual(result, {'stopReason': 'end_turn'})
        self.assertEqual(''.join(message['params']['update']['content']['text'] for message in client.notifications),
                         'hello world')
        client.process.stdin.close()
        self.assertEqual(await asyncio.wait_for(client.process.wait(), 2), 0)
        completed = subprocess.run(['grok', '--headless', '--json', 'hello'],
                                   env=grok_accounts.grok_environment(self.home()), capture_output=True, text=True, timeout=2)
        self.assertEqual(completed.returncode, 0)
        self.assertIn('Not signed in', json.loads(completed.stdout)['message'])

    async def test_refresh_retains_dated_usage_on_failure_and_recovers_identity(self):
        self.register(usage={'seven_day': {'utilization': 37}})
        self.store.put('grok_enabled', True)
        collect = AsyncMock(return_value={'checked_at': 10, 'error': 'timeout'})
        self.assertTrue(await grok_accounts.refresh_grok_accounts(self.store, collect))
        self.assertEqual(GrokBroker(self.store).snapshot('one')['usage'], {'seven_day': {'utilization': 37}})
        collect.return_value = {'checked_at': 11, 'error': 'login_required'}
        await grok_accounts.refresh_grok_accounts(self.store, collect)
        self.assertEqual(GrokBroker(self.store).snapshot('one')['identity'],
                         {'email': 'one@example.test', 'logged_in': False})
        self.configure(one={'signed_in': True, 'used': 42.5})
        await grok_accounts.refresh_grok_accounts(self.store)
        self.assertNotIn('error', GrokBroker(self.store).snapshot('one'))

    async def test_refresh_checks_pending_enable_skips_disabled_and_changed_profiles(self):
        self.register('one', enabled=False, logged_in=False)
        self.register('two', enabled=False)
        profiles = self.store.get('grok_accounts')
        profiles['one']['awaiting_login'] = True
        self.store.put('grok_accounts', profiles)
        self.store.put('grok_enabled', True)
        self.configure(one={'signed_in': True})
        await grok_accounts.refresh_grok_accounts(self.store)
        self.assertEqual(GrokBroker(self.store).accounts()['one']['enabled'], True)
        self.assertEqual({call['home'] for call in self.calls()}, {'one'})

        async def changed(account):
            self.store.put('grok_accounts', {})
            return {'identity': {'logged_in': False}}

        await grok_accounts.refresh_grok_accounts(self.store, changed)
        self.assertTrue(GrokBroker(self.store).snapshot('one')['identity']['logged_in'])

    async def test_monitor_and_service_do_not_start_while_off_or_without_accounts(self):
        collect = AsyncMock()
        self.register()
        self.assertFalse(await grok_accounts.refresh_grok_accounts(self.store, collect))
        collect.assert_not_awaited()
        with patch('coordinator.grok_accounts.collect_grok_status', collect):
            await grok_accounts.monitor_grok_accounts(self.store)
        collect.assert_not_awaited()
        runner = SimpleNamespace(binaries={'grok': 'grok'})
        service = Service(self.store, SimpleNamespace(), runner, self.root)
        self.assertIsInstance(service.grok_accounts, GrokBroker)
        self.assertNotIn('grok_accounts', dict(service.supervised()))
        self.store.put('grok_enabled', True)
        self.assertIn('grok_accounts', dict(service.supervised()))
        self.store.put('grok_accounts', {})
        self.assertNotIn('grok_accounts', dict(service.supervised()))

    async def test_monitor_refreshes_every_300_seconds_or_poke_and_stops_off(self):
        self.register()
        self.store.put('grok_enabled', True)
        collect = AsyncMock(return_value=False)
        clock = [0]

        async def sleep(seconds):
            self.assertEqual(seconds, 2)
            if collect.await_count == 2:
                self.store.put('grok_enabled', False)
            else:
                clock[0] += 150

        with patch('coordinator.grok_accounts.refresh_grok_accounts', collect), \
                patch('coordinator.grok_accounts.time.monotonic', side_effect=lambda: clock[0]), \
                patch('coordinator.grok_accounts.asyncio.sleep', sleep):
            await grok_accounts.monitor_grok_accounts(self.store)
        self.assertEqual(collect.await_count, 2)
        self.store.put('grok_enabled', True)
        collect.reset_mock()

        async def poke(seconds):
            if collect.await_count == 1:
                self.store.put('grok_account_status_refresh_requested', 1)
            else:
                self.store.put('grok_enabled', False)

        with patch('coordinator.grok_accounts.refresh_grok_accounts', collect), \
                patch('coordinator.grok_accounts.time.monotonic', return_value=0), \
                patch('coordinator.grok_accounts.asyncio.sleep', poke):
            await grok_accounts.monitor_grok_accounts(self.store)
        self.assertEqual(collect.await_count, 2)

    async def test_broker_manual_selection_no_reserve_and_resume_pin(self):
        self.register('one', usage={'seven_day': {'utilization': 98, 'resets_at': 4070908800}})
        self.register('two')
        broker = GrokBroker(self.store)
        self.assertFalse(broker.automatic())
        self.assertIsNone(broker.select())
        self.store.put('grok_active_account', 'one')
        self.assertEqual(broker.select(), 'one')
        self.assertEqual(broker.alias_for_environment(broker.environment('two')), 'two')
        run = AsyncMock(return_value=RunResult('native-test', success=True))
        result = await broker.run_active(run, 'grok', 'hello', str(self.root), 'native-test')
        self.assertEqual(result.error, 'Grok workers are off.')
        run.assert_not_awaited()
        self.store.put('grok_enabled', True)
        result = await broker.run_active(run, 'grok', 'hello', str(self.root), 'native-test', alias='two')
        self.assertTrue(result.success)
        run.assert_awaited_once_with('grok', 'hello', str(self.root), 'native-test', account_alias='two')
        self.register('two', logged_in=False)
        self.assertEqual((await broker.run_active(run, 'grok', 'hello', str(self.root), 'native-test', alias='two')).failure_code,
                         'accounts_unavailable')
        self.register('one', usage={'limit': {'utilization': 100}})
        self.assertIsNone(broker.select())
        self.assertTrue((await broker.run_active(run, 'grok', 'hello', str(self.root), 'native-test')).quota_limited)

    def test_broker_blocks_expire_and_gated_accounts_never_selected(self):
        self.register()
        self.store.put('grok_active_account', 'one')
        broker = GrokBroker(self.store)
        with self.store.db:
            broker.record_rate_limit('one', None, rejected=True)
        self.assertTrue(broker.exhausted('one'))
        self.store.put('grok_account_blocks', {'one': {'until': 1, 'reason': 'quota'}})
        self.assertFalse(broker.exhausted('one'))
        self.register(usage_allowed=False)
        self.assertTrue(broker.authenticated('one'))
        self.assertFalse(broker.signed_in('one'))
        self.assertIsNone(broker.select())

    def test_completed_run_requests_fresh_billing_without_parsing_per_turn_meters(self):
        self.register(usage={'seven_day': {'utilization': 37}})
        broker = GrokBroker(self.store)
        with self.store.db:
            broker.record_rate_limit('one', None, rejected=False)
        self.assertGreater(self.store.get('grok_account_status_refresh_requested', 0), 0)
        with self.store.db:
            broker.record_rate_limit('one', {'config': {'creditUsagePercent': 100}}, rejected=False)
        self.assertGreater(self.store.get('grok_account_status_refresh_requested', 0), 0)
        self.assertEqual(broker.snapshot('one')['usage'], {'seven_day': {'utilization': 37}})
        self.assertFalse(broker.exhausted('one'))

    def test_controls_switch_model_and_three_way_account_helpers(self):
        settings = self.call('settings.get').data
        self.assertIs(settings['grok_enabled'], False)
        self.assertIsNone(settings['grok_model'])
        with patch('coordinator.controls.shutil.which', return_value=None):
            result = self.call('delegation.grok', enabled=True)
        self.assertFalse(result.ok)
        self.assertIn('grok is not installed', result.text)
        self.assertIs(self.call('settings.get').data['grok_enabled'], False)
        self.assertTrue(self.call('delegation.grok', enabled=True).ok)
        self.assertTrue(self.call('model.grok', model='grok-test').ok)
        self.assertEqual(self.call('settings.get').data['grok_model'], 'grok-test')
        self.assertTrue(self.call('model.grok').ok)
        self.assertIsNone(self.call('settings.get').data['grok_model'])
        self.register()
        self.assertEqual(signin.registered_provider(self.store, 'one'), 'grok')
        self.assertEqual(signin.provider_name({'provider': 'grok'}), 'Grok')
        self.assertEqual(signin.signin_label(self.store, 'one', 'grok'), 'one@example.test')
        self.assertIn('one', signin.taken_aliases(self.store))
        self.assertTrue(signin.account_authenticated(self.store, 'one', 'grok'))
        self.assertTrue(self.call('account.use', alias='one').ok)
        self.assertEqual(self.store.get('grok_active_account'), 'one')
        self.assertIsNone(self.store.get('codex_active_account'))
        self.assertTrue(self.call('account.disable', alias='one').ok)
        self.assertFalse(self.call('account.use', alias='one').ok)
        self.assertTrue(self.call('account.enable', alias='one').ok)
        self.assertTrue(self.call('account.use', alias='one').ok)
        self.assertTrue(self.call('delegation.grok', enabled=False).ok)

    def test_grok_cannot_be_parent_spawned_or_signed_in_in_piece_one(self):
        self.assertEqual(control_api._coerce(self.store, 'worker_provider', 'grok'), ('grok', None))
        for op in ('workers.spawn', 'account.add', 'topic.bind', 'model.parent'):
            self.assertFalse(self.call(op, provider='grok').ok)
        self.assertEqual(control_api.find('workers.spawn').params['provider'], 'provider')
        self.assertEqual(control_api.find('account.add').params['provider'], 'provider?')
        self.register(logged_in=False)
        result = self.call('account.add', alias='one')
        self.assertEqual(result.text, 'Grok workers are off.')
        self.store.put('grok_enabled', True)
        self.assertEqual(self.call('account.add', alias='one').text, 'Grok sign-in is not available yet.')
        self.assertIsNone(signin.pending(self.store))

    def test_accounts_without_grok_match_existing_snapshot(self):
        self.store.put('accounts', {})
        text, rows = control_ui._accounts_view(self.store, 0)
        self.assertEqual(text, '**Accounts**\nNo accounts yet. Add one to start work. Project setup works before sign-in.')
        self.assertEqual(rows, [[('Add Claude', {'op': 'account.add', 'params': {}, 'page': 'accounts:0'}),
                                 ('Add ChatGPT', {'op': 'account.add', 'params': {'provider': 'codex'}, 'page': 'accounts:0'})],
                                [('Models & Codex ›', {'view': 'settings'})]])
        self.register()
        self.store.put('grok_accounts', {})
        self.assertEqual(control_ui._accounts_view(self.store, 0), (text, rows))

    def test_populated_accounts_without_grok_match_origin_main_snapshot(self):
        claude = self.home('claude-home')
        codex = self.home('codex-home')
        self.store.put('accounts', {'work': {'config_dir': str(claude), 'enabled': True}})
        self.store.put('account_status', {'work': {'identity': {'logged_in': True, 'email': 'work@example.test'},
                       'observed_at': time_now(), 'usage': {'five_hour': {'utilization': 20},
                                                            'seven_day': {'utilization': 37}}}})
        self.store.put('codex_accounts', {'chat': {'config_dir': str(codex), 'enabled': True}})
        self.store.put('codex_account_status', {'chat': {'identity': {'logged_in': True, 'email': 'chat@example.test'},
                       'observed_at': time_now(), 'usage': {'seven_day': {'utilization': 42.5}}}})
        self.store.put('codex_active_account', 'chat')
        text, rows = control_ui._accounts_view(self.store, 0)
        self.assertEqual(text, '**Accounts** · Claude switches at 98% used; Codex switches at 95% used with auto-switch\n'
                         '\n**Claude** (left: 5h · week · Fable)\n🟢 work@example.test: 80% · 63% · ? · next\n'
                         '\n**ChatGPT** · auto-switch off (left: 5h · week)\n🟢 chat@example.test: ? · 57.5% · active')
        self.assertEqual(rows, [[('Add Claude', {'op': 'account.add', 'params': {}, 'page': 'accounts:0'}),
                                 ('Add ChatGPT', {'op': 'account.add', 'params': {'provider': 'codex'}, 'page': 'accounts:0'})],
                                [('Turn on ChatGPT auto-switch', {'op': 'accounts.codex_auto',
                                  'params': {'enabled': True}, 'page': 'accounts:0'})],
                                [('Models & Codex ›', {'view': 'settings'})]])

    def test_grok_rows_week_generic_unknown_gate_and_account_views(self):
        self.store.put('accounts', {})
        self.register(usage={'seven_day': {'utilization': 37}})
        self.register('two', usage={'limit': {'utilization': 42.5}})
        self.store.put('grok_active_account', 'one')
        text, rows = control_ui._accounts_view(self.store, 0)
        self.assertIn('one@example.test: week 63% left · active', text)
        self.assertIn('two@example.test: limit 57.5% left', text)
        labels = [label for row in rows for label, action in row]
        self.assertIn('Use two@example.test for Grok', labels)
        self.assertFalse(any('Add Grok' in label for label in labels))
        self.register('two', usage={'limit': {}})
        self.assertIn('limit ? left', control_ui._accounts_view(self.store, 0)[0])
        self.register('two', usage_allowed=False)
        text, rows = control_ui._accounts_view(self.store, 0)
        self.assertIn('needs SuperGrok or X Premium+', text)
        self.assertFalse(any(label == 'Use two@example.test for Grok' for row in rows for label, action in row))
        self.register('two', logged_in=False)
        text, rows = control_ui._account_view(self.store, 'two')
        self.assertIn('(Grok) is signed out', text)
        self.assertEqual(rows, [[('Remove', {'view': 'remove:two'})]])
        self.register('two', enabled=False)
        self.assertIn('(Grok) is signed in but turned off', control_ui._account_view(self.store, 'two')[0])

    def test_forbidden_auth_endpoints_absent_and_grok_never_reads_token_files(self):
        forbidden = ('getBearer' + 'Token', 'getApi' + 'Key', 'setApi' + 'Key', 'auth/' + 'logout')
        for path in Path('coordinator').glob('*.py'):
            source = path.read_text()
            for name in forbidden:
                self.assertNotIn(name, source, str(path))
            for call in (node for node in ast.walk(ast.parse(source)) if isinstance(node, ast.Call)):
                if isinstance(call.func, ast.Attribute) and call.func.attr in ('read_text', 'read_bytes', 'open'):
                    self.assertNotIn('auth' + '.json', ast.get_source_segment(source, call), str(path))
        self.assertNotIn('auth' + '.json', Path(grok_accounts.__file__).read_text())

    def test_fixture_covers_recorded_and_explicit_source_shapes(self):
        rows = {row['name']: row for row in (json.loads(line) for line in FIXTURE.read_text().splitlines())}
        self.assertEqual(rows['initialize']['response']['result']['_meta']['agentVersion'], '1.0.52')
        for name in ('authenticate', 'device_url', 'auth_cancel', 'billing', 'prompt', 'interject'):
            self.assertEqual(rows[name]['source'], 'public-source')
        self.assertEqual(rows['load_missing']['response']['error']['data']['code'], 'FS_NOT_FOUND')
        self.assertNotIn('id', rows['cancel_notification']['request'])
        self.assertIn('Not signed in', rows['headless_signed_out']['response']['message'])
        self.assertNotIn('/Users/', FIXTURE.read_text())


def time_now():
    return datetime.now(timezone.utc).timestamp()
