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
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('home',1,1,'Home',?,1)",
                              (str(self.root),))
        self.store.put('coordinator_home_topic', 'home')
        self.add_codex('a')
        self.add_codex('b')
        self.store.put('codex_active_account', 'a')
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
        self.store.put('accounts', {'c': {'config_dir': str(profile), 'enabled': True}})
        self.store.put('account_status', {'c': {'identity': {'email': 'c@example.test', 'logged_in': True}}})
        if limited:
            self.store.put('account_blocks', {'c': {'until': time.time() + 1000, 'reason': 'quota'}})

    async def choose(self):
        return await parent.choose_parent(self.store, self.accounts, self.codex, runner=self.runner)

    async def test_claude_precedes_chatgpt_without_activating_an_account(self):
        self.claude()
        with patch.object(self.accounts, 'activate', side_effect=AssertionError('Selection must not activate')):
            self.assertEqual(await self.choose(), ('claude', 'c'))

    async def test_chatgpt_parent_ignores_automatic_worker_rotation_and_95_percent_holds(self):
        self.store.put('codex_account_blocks', {'a': {'reason': 'threshold', 'until': time.time() + 3600}})
        self.store.put('codex_account_status', {**self.store.get('codex_account_status'), 'a': {
            'identity': {'email': 'a@example.test', 'logged_in': True},
            'usage': {'five_hour': {'utilization': 96, 'resets_at': time.time() + 3600}}}})
        self.assertEqual(await self.choose(), ('codex', 'a'))
        self.assertEqual(self.codex.select(), 'b')

    async def test_exhausted_active_chatgpt_account_pauses_even_when_another_has_room(self):
        reset = time.time() + 3600
        self.store.put('codex_account_blocks', {'a': {'reason': 'quota', 'until': reset}})
        with self.assertRaises(AccountsUnavailable) as caught:
            await self.choose()
        self.assertEqual((caught.exception.provider, caught.exception.alias, caught.exception.reset_at),
                         ('codex', 'a', reset))
        self.assertEqual(self.store.get('codex_active_account'), 'a')

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
        row = self.store.message_save('home', 'owner', 'waiting')
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
        row = self.store.message_save('home', 'owner', 'waiting')
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
        self.assertTrue(any(row['kind'] == 'callback' for row in self.store.messages_pending()))
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
        self.codex.record_rate_limit('a', {'primary': {
            'usedPercent': 100, 'windowDurationMins': 300, 'resetsAt': reset}}, rejected=True)
        self.assertEqual(self.codex.account_state('a', parent=True)['until'], reset)
