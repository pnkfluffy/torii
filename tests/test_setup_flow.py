import asyncio
from contextlib import redirect_stdout
import io
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import __main__ as cli
from coordinator.service import Service
from coordinator.setup_flow import PRIVATE_REFUSAL, setup_state, setup_status, tick
from coordinator.store import Store
from tests.test_group_setup import CHAT, HOME, GroupTelegram, pair_group
from tests.test_service import FakeRunner, FakeSession


class SetupFlowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store = Store(self.root / 'state')
        pair_group(self.store)
        self.telegram = GroupTelegram()
        self.runner = FakeRunner()
        self.factory = AsyncMock(return_value=FakeSession('fake-session'))
        self.service = Service(self.store, self.telegram, self.runner, self.root, session_factory=self.factory)

    async def asyncTearDown(self):
        self.store.close()
        self.temp.cleanup()

    async def test_fresh_execution_latch_blocks_feed_workers_tldr_and_session(self):
        self.store.message_save(HOME, 'owner', 'held')
        self.store.service_request('tldr', {'topic': HOME})
        self.assertFalse(await self.service.feed_once())
        self.assertFalse(await self.service.workers_once())
        self.assertFalse(await self.service.tldr_once())
        self.assertIsNone(await self.service.start_session(HOME))
        self.factory.assert_not_awaited()
        self.assertFalse(self.store.get('pair_only'))

    async def test_signed_in_claude_flips_execution_latch(self):
        self.store.put('accounts', {'owner': {'config_dir': str(self.root), 'enabled': True}})
        with patch('coordinator.accounts.signed_in', return_value=True):
            await tick(self.service)
        self.assertEqual(self.store.get('execution'), 'agents')
        self.assertFalse(self.store.get('pair_only'))
        self.assertFalse(self.store.get('restart_requested_v2'))

    async def test_chatgpt_only_leaves_pairing(self):
        from coordinator.setup_flow import replace_pairing, setup_text
        pair_group(self.store)
        directory = self.store.directory / 'codex-accounts' / '.torii-12345678'
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        directory.parent.chmod(0o700)
        self.store.directory.chmod(0o700)
        self.store.put('codex_accounts', {'gpt': {'enabled': True, 'config_dir': str(directory)}})
        self.store.put('codex_account_status', {'gpt': {'identity': {
            'logged_in': True, 'email': 'test@example.test'}}})
        self.store.put('execution', 'pairing')
        self.store.put('setup_requests', {'check_accounts': 1})
        with patch('shutil.which', return_value='/fake/codex'), \
                patch('coordinator.setup_flow.check_setup_accounts', AsyncMock()):
            await tick(self.service)
            self.assertEqual(self.store.get('execution'), 'agents')
            self.assertTrue(self.service.wake('feed').is_set())
            state = setup_state(self.store)
            self.assertTrue(state['agents'])
            self.assertEqual(state['main'], 'chatgpt')
            text = setup_text(state)
            self.assertIn('Main chat: ChatGPT.', text)
            self.assertNotIn('Connect Claude to start', text)
            replace_pairing(self.store)
            self.assertEqual(self.store.get('execution'), 'agents')
        if self.service.setup_accounts_task:
            await self.service.setup_accounts_task

    def test_setup_text_names_the_available_main_provider(self):
        from coordinator.setup_flow import setup_text
        state = {'agents': True, 'claude': True, 'codex': True, 'codex_installed': True,
                 'forced': False, 'main': 'claude'}
        self.assertIn('Main chat: Claude', setup_text(state))
        state.update(agents=False, claude=False, codex=False, main=None)
        self.assertIn('Connect Claude or ChatGPT', setup_text(state))

    async def test_live_group_with_no_execution_key_still_runs_agents(self):
        self.store.db.execute("DELETE FROM settings WHERE key='execution'")
        with self.store.db:
            self.store.db.execute('UPDATE topics SET enabled=1,cwd=? WHERE id=?', (str(self.root), HOME))
        self.store.message_save(HOME, 'owner', 'work')
        self.assertTrue(setup_state(self.store)['agents'])
        self.assertFalse(self.service.pair_only)
        self.assertTrue(await self.service.feed_once())
        await asyncio.gather(*self.service.feed_tasks.values())
        self.factory.assert_awaited_once()
        self.assertEqual(self.factory.return_value.sent[0][1].split('] ', 1)[1], 'work')
        self.assertIsNone(self.store.get('execution'))

    def test_setup_status_card_is_stable_and_controls_are_shared(self):
        with patch('coordinator.setup_flow.setup_state', return_value={
                'agents': False, 'claude': False, 'codex': False, 'codex_installed': True, 'forced': False}):
            setup_status(self.store)
            card = self.store.get('setup_card')
            setup_status(self.store)
            self.assertEqual(self.store.get('setup_card'), card)
        self.assertEqual([action['op'] for action in self.store.get('control_ui:' + HOME)['actions']],
                         ['setup.claude', 'setup.chatgpt'])
        self.assertEqual(self.store.db.execute('SELECT text FROM outbox WHERE id=?', (card,)).fetchone()[0],
                         'Torii is paired. Connect Claude or ChatGPT to start working.\nClaude: not connected\nChatGPT: not connected')

    async def test_explicit_pair_only_blocks_agents_and_vault_operations(self):
        from coordinator.control_api import call
        from coordinator.vault import FakeVault
        self.store.put('execution', 'agents')
        service = Service(self.store, self.telegram, self.runner, self.root, pair_only=True,
                          session_factory=self.factory, vault=FakeVault())
        self.assertFalse(await service.feed_once())
        self.assertFalse(await service.workers_once())
        self.assertFalse(await service.tldr_once())
        result = call(self.store, 'secret.revoke', {'name': 'FAKE_KEY'})
        self.assertFalse(result.ok)
        self.assertIn('pair', result.text)

    async def test_private_serve_refuses_before_transport_and_waits(self):
        self.store.put('mode', 'private')
        with patch('coordinator.__main__.Telegram', side_effect=AssertionError('Must not read token')), \
                patch('asyncio.get_running_loop') as loop, \
                patch('asyncio.Event') as event:
            event.return_value.wait = AsyncMock()
            with self.assertLogs('coordinator.__main__', level='ERROR') as output:
                await cli.serve(SimpleNamespace(token_file=self.root / 'no-token'), self.store)
            event.return_value.wait.assert_awaited_once()
        self.assertIn(PRIVATE_REFUSAL, output.output[0])
        self.assertEqual(self.store.get('setup_problem'), 'private_mode_removed')
        self.assertTrue(self.store.get('pair_only'))
        self.assertEqual(self.telegram.calls, [])

    def test_private_cli_status_and_pair_print_exact_refusal(self):
        self.store.put('mode', 'private')
        self.store.db.commit()
        with redirect_stdout(io.StringIO()) as output:
            self.assertEqual(cli.pair(SimpleNamespace(replace=False), self.store), 1)
        self.assertEqual(output.getvalue().strip(), PRIVATE_REFUSAL)
        with patch('sys.argv', ['coordinator', '--state-dir', str(self.store.directory),
                              '--token-file', str(self.root / 'no-token'), 'status']), redirect_stdout(io.StringIO()) as output:
            self.assertEqual(cli.main(), 1)
        self.assertEqual(output.getvalue().strip(), PRIVATE_REFUSAL)

    def test_private_replace_clears_old_routing_and_preserves_project_history(self):
        old = '7:20'
        self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,session) VALUES (?,?,?,?,?,?)',
                             (old, 7, 20, 'Old project', str(self.root), 'native-session'))
        message = self.store.message_save(old, 'owner', 'saved request')
        task = self.store.task_create(old, 'Saved task', worktree=str(self.root))
        self.store.put('mode', 'private')
        keys = ['_'.join(parts) for parts in [('bot', 'topics'), ('topic', 'requests'), ('second', 'project'),
                ('creating', 'topics'), ('pending', 'bind'), ('topic', 'fallbacks'), ('switched', 'projects')]] + ['first_project']
        for key in keys:
            self.store.put(key, {'saved': True})
        accounts = {'owner': {'config_dir': str(self.root), 'enabled': True}}
        self.store.put('accounts', accounts)
        with patch('coordinator.accounts.signed_in', return_value=True), redirect_stdout(io.StringIO()) as output:
            self.assertEqual(cli.pair(SimpleNamespace(replace=True, yes=True), self.store), 0)
        self.assertIn('?startgroup=', output.getvalue())
        self.assertIsNone(self.store.get('owner'))
        self.assertIsNone(self.store.chat())
        self.assertIsNone(self.store.get('control_topic'))
        self.assertEqual(self.store.get('execution'), 'agents')
        self.assertEqual(self.store.get('accounts'), accounts)
        for key in keys:
            self.assertIsNone(self.store.get(key))
        self.assertFalse(any(topic['chat'] == 7 for topic in self.store.topics()))
        self.assertEqual(self.store.topic(old)['session'], 'native-session')
        self.assertEqual(self.store.task_get(task['id'])['worktree'], str(self.root))
        self.assertEqual(self.store.db.execute('SELECT text FROM messages WHERE id=?', (message['id'],)).fetchone()[0], 'saved request')
        self.assertEqual(self.store.db.execute('PRAGMA foreign_key_check').fetchall(), [])
