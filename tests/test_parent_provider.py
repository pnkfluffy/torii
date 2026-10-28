import json
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch
import uuid

from coordinator import parent
from coordinator.accounts import AccountBroker
from coordinator.codex_accounts import CodexBroker
from coordinator.providers import ProviderRunner
from coordinator.session import AccountsUnavailable, message_uuid
from coordinator.store import Store
from tests.support import isolate_shared_mcp_home


class ParentProviderTests(unittest.IsolatedAsyncioTestCase):
    async def test_opencode_pick_never_falls_back(self):
        from coordinator.control_api import Refused
        self.claude()
        key = 'coordinator:other'
        self.store.put(key + '_selection', {'provider': 'opencode', 'model': None, 'effort': None, 'revision': 1})
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('other',1,2,'Other',?,1)",
                              (str(self.root),))
        self.store.db.commit()
        reset = time.time() + 3600
        self.store.put('opencode_accounts', {'one': {'model': 'test'}})
        self.store.put('opencode_active_account', 'one')
        with patch('coordinator.session.CoordinatorSession.start', AsyncMock(side_effect=AssertionError('Claude fallback'))), \
                patch('coordinator.codex_session.CodexCoordinatorSession.start', AsyncMock(side_effect=AssertionError('Codex fallback'))):
            for case in ('off', 'no-account', 'blocked', 'key', 'version', 'go'):
                with self.subTest(case=case):
                    self.store.put('opencode_enabled', case != 'off')
                    self.store.put('opencode_active_account', None if case == 'no-account' else 'one')
                    self.store.put('opencode_account_blocks', {'one': {'until': reset}} if case == 'blocked' else {})
                    self.store.put('opencode_accounts', {'one': {'model': 'test', 'provider': 'opencode-go'}
                                                       if case == 'go' else {'model': 'test'}})
                    with patch('coordinator.opencode_accounts._check_binary', side_effect=Refused('version') if case == 'version' else None), \
                            patch('coordinator.opencode_accounts.check_key', side_effect=Refused('key') if case == 'key' else None):
                        self.assertFalse(parent.available(self.store, self.accounts, self.codex, self.runner, key))
                        unavailable = parent.unavailable(self.store, self.accounts, self.codex, key)
                        self.assertEqual(unavailable.provider, 'opencode')
                        with self.assertRaises(AccountsUnavailable) as caught:
                            await parent.start(self.store, self.runner, self.root, None, 'policy', topic='other')
                        self.assertEqual(caught.exception.provider, 'opencode')
                        if case == 'blocked':
                            self.assertEqual(caught.exception.reset_at, reset)
            self.store.put('opencode_accounts', {'one': {'model': 'test'}})
            self.store.put('opencode_account_blocks', {})
            with patch('coordinator.opencode_accounts.check_ready'):
                self.assertEqual(await parent.choose_parent(self.store, self.accounts, self.codex, key, self.runner),
                                 ('opencode', 'one'))
                self.assertTrue(parent.available(self.store, self.accounts, self.codex, self.runner, key))
                with patch('coordinator.opencode_parent.OpenCodeParentSession.start',
                           AsyncMock(side_effect=AccountsUnavailable(reset, provider='opencode', alias='one'))):
                    with self.assertRaises(AccountsUnavailable) as caught:
                        await parent.start(self.store, self.runner, self.root, None, 'policy', topic='other')
                    self.assertEqual((caught.exception.provider, caught.exception.reset_at), ('opencode', reset))

    async def test_claude_and_codex_picks_never_choose_opencode(self):
        self.claude()
        self.store.put('opencode_accounts', {'one': {'model': 'test'}})
        self.store.put('opencode_active_account', 'one')
        self.store.put('opencode_enabled', True)
        with patch('coordinator.opencode_runtime.parent_ready', side_effect=AssertionError('Unpicked OpenCode'), create=True):
            for pick, expected in ((None, 'claude'), ('claude', 'claude'), ('codex', 'codex')):
                with self.subTest(pick=pick):
                    self.store.put('coordinator_selection', {'provider': pick, 'model': None, 'effort': None, 'revision': 1}
                                   if pick else None)
                    self.assertEqual((await self.choose())[0], expected)
            self.store.put('account_blocks', {'team': {'until': time.time() + 3600, 'reason': 'quota'}})
            self.store.put('codex_account_blocks', {'personal': {'until': time.time() + 3600, 'reason': 'quota'}})
            with self.assertRaises(AccountsUnavailable) as caught:
                await self.choose()
            self.assertNotEqual(caught.exception.provider, 'opencode')

    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)
        isolate_shared_mcp_home(self, self.root)
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        self.binary = self.root / 'codex'
        self.binary.touch()
        self.binary.chmod(0o700)
        self.runner = ProviderRunner(self.store.directory, {'codex': str(self.binary)})
        self.accounts, self.codex = AccountBroker(self.store), CodexBroker(self.store)
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('Atlas',1,1,'Atlas',?,1)",
                              (str(self.root),))
        self.store.put('coordinator_home_topic', 'Atlas')
        self.add_codex('personal')
        self.add_codex('work')
        self.store.put('codex_active_account', 'personal')
        self.store.put('codex_auto_switch', True)
        self.store.db.commit()

    def add_codex(self, alias):
        profile = self.root / alias
        profile.mkdir()
        self.store.put('codex_accounts', {**self.store.get('codex_accounts', {}), alias: {
            'config_dir': str(profile), 'enabled': True}})
        self.store.put('codex_account_status', {**self.store.get('codex_account_status', {}), alias: {
            'identity': {'email': alias + '@example.test', 'logged_in': True}}})

    def claude(self, limited=False):
        profile = self.root / 'claude'
        profile.mkdir()
        self.store.put('accounts', {'team': {'config_dir': str(profile), 'enabled': True}})
        self.store.put('account_status', {'team': {'identity': {'email': 'team@example.test', 'logged_in': True}}})
        if limited:
            self.store.put('account_blocks', {'team': {'until': time.time() + 1000, 'reason': 'quota'}})

    async def choose(self):
        return await parent.choose_parent(self.store, self.accounts, self.codex, runner=self.runner)

    async def test_claude_precedes_chatgpt_without_activating_an_account(self):
        self.claude()
        with patch.object(self.accounts, 'activate', side_effect=AssertionError('Selection must not activate')):
            self.assertEqual(await self.choose(), ('claude', 'team'))

    async def test_explicit_codex_parent_keeps_owner_account_when_claude_is_available(self):
        self.claude()
        self.store.put('parent_selection', {'provider': 'codex', 'model': 'gpt-6-astra'})
        self.assertEqual(await self.choose(), ('codex', 'personal'))
        self.assertEqual(self.store.get('codex_active_account'), 'personal')
        self.store.put('codex_account_blocks', {'personal': {'reason': 'quota', 'until': time.time() + 3600}})
        self.assertFalse(parent.available(self.store, self.accounts, self.codex, self.runner))
        with self.assertRaises(AccountsUnavailable) as caught:
            await self.choose()
        self.assertEqual(caught.exception.provider, 'codex')

    async def test_explicit_claude_parent_uses_codex_fallback(self):
        self.claude(limited=True)
        self.store.put('parent_selection', {'provider': 'claude', 'model': 'claude-opus-5-5'})
        self.assertTrue(parent.available(self.store, self.accounts, self.codex, self.runner))
        self.assertEqual(await self.choose(), ('codex', self.codex.active()))
        fallback = SimpleNamespace(provider='codex')
        with patch('coordinator.session.CoordinatorSession.start', AsyncMock(side_effect=AccountsUnavailable(None))), \
                patch('coordinator.codex_session.CodexCoordinatorSession.start', AsyncMock(return_value=fallback)) as codex_start, \
                patch.object(self.accounts, 'select', return_value=next(iter(self.store.get('accounts')))):
            self.assertIs(await parent.start(self.store, self.runner, self.root, 'model', 'policy'), fallback)
            codex_start.assert_awaited_once()

    async def test_chatgpt_parent_ignores_automatic_worker_rotation_and_95_percent_holds(self):
        self.store.put('codex_account_blocks', {'personal': {'reason': 'threshold', 'until': time.time() + 3600}})
        self.store.put('codex_account_status', {**self.store.get('codex_account_status'), 'personal': {
            'identity': {'email': 'personal@example.test', 'logged_in': True},
            'usage': {'five_hour': {'utilization': 96, 'resets_at': time.time() + 3600}}}})
        self.assertEqual(await self.choose(), ('codex', 'personal'))
        self.assertEqual(self.codex.select(), 'work')

    async def test_exhausted_active_chatgpt_account_pauses_even_when_another_has_room(self):
        reset = time.time() + 3600
        self.store.put('codex_account_blocks', {'personal': {'reason': 'quota', 'until': reset}})
        with self.assertRaises(AccountsUnavailable) as caught:
            await self.choose()
        self.assertEqual((caught.exception.provider, caught.exception.alias, caught.exception.reset_at),
                         ('codex', 'personal', reset))
        self.assertEqual(self.store.get('codex_active_account'), 'personal')

    async def test_running_host_keeps_its_provider_and_old_hosts_mean_claude(self):
        for provider in ('codex', None):
            host = {'id': 'owned', 'account': 'old'}
            if provider:
                host['provider'] = provider
            self.store.put('coordinator_host', host)
            with patch('coordinator.host.HostClient.recover_state', AsyncMock(return_value='running')), \
                    patch.object(self.accounts, 'select', side_effect=AssertionError('Running parent must stay')):
                self.assertEqual(await self.choose(), (provider or 'claude', 'old'))

    async def test_exited_hosts_keep_the_provider_for_history_replay(self):
        for provider in ('claude', 'codex'):
            with self.subTest(provider=provider):
                host = {'id': 'exited', 'account': 'old', 'provider': provider}
                self.store.put('coordinator_host', host)
                self.store.put('coordinator_turn', [123])
                with patch('coordinator.host.HostClient.recover_state', AsyncMock(return_value='exited')):
                    self.assertEqual(await self.choose(), (provider, 'old'))
                self.assertEqual(self.store.get('coordinator_host'), host)
                self.assertEqual(self.store.get('coordinator_turn'), [123])

    async def test_missing_claude_transcript_requeues_and_starts_chatgpt(self):
        from coordinator.accounts import TaskFailure
        self.claude(limited=True)
        self.store.put('coordinator_session', str(uuid.uuid4()))
        self.store.put('coordinator_provider', 'claude')
        row = self.store.message_save('Atlas', 'owner', 'waiting')
        self.store.message_sending(row['id'])
        self.store.message_delivered(row['id'], 'uncertain')
        self.store.put('coordinator_lost', [row['id']])
        with patch('coordinator.accounts.AccountBroker.transcript', side_effect=TaskFailure('missing', 'requeue')), \
                patch('coordinator.codex_session.CodexCoordinatorSession.start', AsyncMock(return_value=SimpleNamespace())):
            session = await parent.start(self.store, self.runner, self.root, 'model', 'policy')
        self.assertTrue(session.switched)
        self.assertEqual(self.store.get('coordinator_provider'), 'codex')
        self.assertIsNone(self.store.get('coordinator_lost'))
        self.assertIn(row['id'], [item['id'] for item in self.store.messages_pending()])

    async def test_provider_switch_starts_fresh_and_settles_against_the_old_transcript(self):
        self.claude(limited=True)
        old = str(uuid.uuid4())
        self.store.put('coordinator_session', old)
        self.store.put('coordinator_provider', 'claude')
        row = self.store.message_save('Atlas', 'owner', 'waiting')
        self.store.message_sending(row['id'])
        self.store.message_delivered(row['id'], 'uncertain')
        self.store.put('coordinator_lost', [row['id']])
        transcript = self.root / 'old.jsonl'
        transcript.write_text(json.dumps({'type': 'user', 'uuid': message_uuid(row['id'])}) + '\n')

        async def fresh(*args, **kwargs):
            self.assertIsNone(self.store.get('coordinator_session'))
            self.store.put('coordinator_session', str(uuid.uuid4()))
            return SimpleNamespace(provider='codex')

        with patch('coordinator.accounts.AccountBroker.transcript', return_value=transcript), \
                patch('coordinator.codex_session.CodexCoordinatorSession.start', side_effect=fresh):
            session = await parent.start(self.store, self.runner, self.root, 'model', 'policy')
        self.assertTrue(session.switched)
        self.assertEqual(session.switch['from'], 'claude')
        self.assertIsNone(self.store.get('coordinator_lost'))
        self.assertEqual([item['id'] for item in self.store.messages_pending()], [row['id']])
        self.assertEqual(self.store.get('coordinator_provider'), 'codex')

    async def test_claude_activation_race_returns_to_the_saved_codex_thread(self):
        self.claude()
        old = str(uuid.uuid4())
        self.store.put('coordinator_session', old)
        self.store.put('coordinator_provider', 'codex')

        async def resumed(*args, **kwargs):
            self.assertEqual(self.store.get('coordinator_session'), old)
            return SimpleNamespace(provider='codex')

        with patch('coordinator.session.CoordinatorSession.start', AsyncMock(side_effect=AccountsUnavailable(None))), \
                patch('coordinator.codex_session.CodexCoordinatorSession.start', side_effect=resumed):
            session = await parent.start(self.store, self.runner, self.root, 'model', 'policy')
        self.assertFalse(session.switched)
        self.assertEqual(self.store.get('coordinator_session'), old)

    async def test_first_chatgpt_launch_has_no_provider_switch_notice(self):
        with patch('coordinator.codex_session.CodexCoordinatorSession.start', AsyncMock(return_value=SimpleNamespace())):
            session = await parent.start(self.store, self.runner, self.root, 'model', 'policy')
        self.assertFalse(session.switched)

    async def test_unmanaged_or_missing_codex_never_runs_a_parent(self):
        for managed, binary in ((False, str(self.binary)), (True, None)):
            with patch.object(self.codex, 'managed', return_value=managed), \
                    patch('coordinator.parent.codex_binary', return_value=binary):
                with self.assertRaises(AccountsUnavailable):
                    await self.choose()

    def test_rejection_keeps_the_actual_reset_when_worker_rotation_is_off(self):
        self.store.put('codex_auto_switch', False)
        reset = time.time() + 3600
        self.codex.record_rate_limit('personal', {'primary': {
            'usedPercent': 100, 'windowDurationMins': 300, 'resetsAt': reset}}, rejected=True)
        self.assertEqual(self.codex.account_state('personal', parent=True)['until'], reset)

    def test_connected_agents_ignores_opencode_account_when_workers_are_off(self):
        self.store.put('opencode_enabled', False)
        self.store.put('opencode_accounts', {'example-host': {'model': 'test'}})
        self.store.put('opencode_active_account', 'example-host')
        self.assertEqual(parent.connected_agents(self.store), ['ChatGPT'])

    def test_connected_agents_keeps_current_names_when_opencode_has_no_account(self):
        self.store.put('opencode_enabled', True)
        self.assertEqual(parent.connected_agents(self.store), ['ChatGPT'])

    def test_connected_agents_keeps_current_names_when_opencode_active_account_is_missing(self):
        self.store.put('opencode_enabled', True)
        self.store.put('opencode_active_account', 'missing')
        self.store.put('opencode_accounts', {})
        self.assertEqual(parent.connected_agents(self.store), ['ChatGPT'])

    def test_connected_agents_appends_opencode_after_existing_providers(self):
        self.store.put('opencode_enabled', True)
        self.store.put('opencode_active_account', 'example-host')
        self.store.put('opencode_accounts', {'example-host': {'model': 'test'}})
        profile = self.root / 'claude'
        profile.mkdir()
        claude_accounts = {'team': {'config_dir': str(profile), 'enabled': True}}
        claude_status = {'team': {'identity': {'email': 'team@example.test', 'logged_in': True}}}
        cases = (
            ('OpenCode alone', False, False, ['OpenCode workers (no goals or secrets; spend is an estimate)']),
            ('Claude only', True, False, ['Claude', 'OpenCode workers (no goals or secrets; spend is an estimate)']),
            ('ChatGPT only', False, True, ['ChatGPT', 'OpenCode workers (no goals or secrets; spend is an estimate)']),
            ('Claude and ChatGPT', True, True,
             ['Claude', 'ChatGPT', 'OpenCode workers (no goals or secrets; spend is an estimate)']),
        )
        for name, claude, chatgpt, expected in cases:
            with self.subTest(providers=name):
                self.store.put('accounts', claude_accounts if claude else {})
                self.store.put('account_status', claude_status if claude else {})
                with patch.object(CodexBroker, 'managed', return_value=chatgpt):
                    self.assertEqual(parent.connected_agents(self.store), expected)
