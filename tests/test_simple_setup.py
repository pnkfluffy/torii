import asyncio
from contextlib import redirect_stdout
import hashlib
import io
import os
from pathlib import Path
import plistlib
import re
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from coordinator.setup_flow import PRIVATE_REFUSAL
from coordinator.store import Store
from coordinator.telegram import Telegram
from tests.test_group_setup import CHAT, HOME, pair_group
from tests.test_setup_installer import INSTALLER, SERVICE_INSTALLER


class SimpleSetupTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store = Store(self.root / 'state')
        self.globals = INSTALLER['setup'].__globals__
        self.patches = [patch('pathlib.Path.home', return_value=self.root),
                        patch.dict(os.environ, {'HOME': str(self.root), 'TORII_STATE_DIR': str(self.store.directory)}),
                        patch('coordinator.telegram.Telegram._request', side_effect=AssertionError('No Telegram API'))]
        for handle in self.patches:
            handle.start()

    async def asyncTearDown(self):
        self.store.close()
        for handle in reversed(self.patches):
            handle.stop()
        self.temp.cleanup()

    async def test_service_installs_before_link_and_waits_only_on_local_state(self):
        events = []
        api = SimpleNamespace(set_default_admin_rights=AsyncMock())
        bot = {'id': 99, 'username': 'test_bot'}
        def install():
            self.assertEqual(self.store.get('execution'), 'pairing')
            self.assertIsNotNone(self.store.get('pairing'))
            events.append('install')
        async def wait(store, code, username):
            self.assertEqual(events, ['install'])
            self.assertEqual(store.get('pairing')['hash'], hashlib.sha256(code.encode()).hexdigest())
            self.assertRegex(code, r'^[A-Za-z0-9_-]{1,64}$')
            self.assertEqual(username, 'test_bot')
            events.append('link')
            pair_group(store)
        with patch.dict(self.globals, check_prereqs=Mock(return_value=([], False)),
                        service_running=Mock(return_value=False), obtain_token=AsyncMock(return_value=(api, bot)),
                        confirm_busy_bot=AsyncMock(), install=install, wait_running=wait), redirect_stdout(io.StringIO()) as out:
            self.assertEqual(await INSTALLER['setup'](), 0)
        api.set_default_admin_rights.assert_awaited_once()
        self.assertEqual(events, ['install', 'link'])
        self.assertIn('Torii setup', out.getvalue())
        self.assertIn('Checking this Mac... ok', out.getvalue())
        self.assertIn('Bot @test_bot ok. Starting Torii in the background...', out.getvalue())
        self.assertEqual(self.store.get('owner'), 7)

    async def test_running_service_never_installs_or_polls_updates(self):
        self.store.put('bot_username', 'test_bot')
        self.store.db.commit()
        with patch.dict(self.globals, check_prereqs=Mock(return_value=([], False)),
                        service_running=Mock(return_value=True), install=Mock(side_effect=AssertionError('Already running')),
                        wait_running=AsyncMock()), redirect_stdout(io.StringIO()):
            self.assertEqual(await INSTALLER['setup'](), 0)
        source = Path(INSTALLER['ROOT'] / 'scripts/setup.py').read_text()
        self.assertNotIn('getUpdates', source)

    async def test_wait_paired_prints_each_terminal_step(self):
        async def progress(delay):
            if self.store.get('owner') is None:
                with self.store.db:
                    self.store.put('owner', 7)
                    self.store.put('group', CHAT)
                    self.store.put('owner_name', 'Owner')
                    self.store.put('group_name', 'Projects')
            else:
                self.store.put('control_topic', HOME)
        with patch('asyncio.sleep', progress), redirect_stdout(io.StringIO()) as out:
            await INSTALLER['wait_paired'](self.store)
        self.assertEqual(out.getvalue().splitlines(), [
            'Waiting for you to add the bot... (Ctrl-C to stop)', 'Paired with Owner in "Projects".',
            'Waiting for Topics to be turned on... (see the message in the group)',
            'Done. Open the Torii topic in your group and tap Add ChatGPT or Connect Claude.'])

    async def test_wait_timeout_prints_rerun_guidance(self):
        with redirect_stdout(io.StringIO()), self.assertRaisesRegex(ValueError, 'Run setup again on your Mac'):
            await INSTALLER['wait_paired'](self.store, timeout=0)

    def test_group_link_prints_exact_screens_and_rights(self):
        with patch('builtins.input', return_value=''), patch('subprocess.run') as run, \
                patch.dict(self.globals, link=INSTALLER['link']), patch('sys.platform', 'darwin'), \
                patch.dict(os.environ, {}, clear=True), redirect_stdout(io.StringIO()) as out:
            INSTALLER['open_group_link']('test_bot', 'abc_def-123')
        url = 'https://t.me/test_bot?startgroup=abc_def-123&admin=manage_topics+delete_messages+pin_messages'
        self.assertEqual(run.call_args.args[0], ['open', url])
        self.assertEqual(out.getvalue().splitlines(), [
            'Next: add the bot to a Telegram group you own.',
            'No group yet? In Telegram tap New Group, give it a name, and create it. Then come back here and press Enter.',
            'Opening Telegram... pick your group, then tap "Add as admin".',
            'If nothing opened, use this link (valid 30 minutes): ' + url])

    async def test_botfather_in_app_option_is_printed_next_to_hidden_token_prompt(self):
        with patch('getpass.getpass', return_value='FAKE-TOKEN') as hidden, \
                patch.dict(self.globals, check_bot=AsyncMock(return_value=(None, {'username': 'test_bot'}))), \
                redirect_stdout(io.StringIO()) as out:
            await INSTALLER['obtain_token'](self.root / 'config', self.store)
        self.assertIn('No bot yet? Open https://t.me/BotFather?startapp, or send /newbot to @BotFather.', out.getvalue())
        hidden.assert_called_once_with('Paste the bot token from @BotFather (input is hidden): ')
        self.assertNotIn('FAKE-TOKEN', out.getvalue())

    async def test_default_admin_rights_include_manage_topics_delete_and_pin(self):
        api = Telegram.__new__(Telegram)
        api.call = AsyncMock()
        await api.set_default_admin_rights()
        call = api.call.await_args
        self.assertEqual(call.args, ('setMyDefaultAdministratorRights',))
        rights = call.kwargs['rights']
        for key in ('can_manage_topics', 'can_delete_messages', 'can_pin_messages'):
            self.assertTrue(rights[key])
        self.assertFalse(rights['can_promote_members'])

    def test_default_and_compat_installer_have_no_pair_only_in_plist(self):
        for enabled in (False, True):
            with patch('coordinator.host_os.linux', return_value=False), patch('sys.platform', 'darwin'), redirect_stdout(io.StringIO()):
                SERVICE_INSTALLER['install'](enabled, write_only=True)
            target = self.root / 'Library/LaunchAgents/local.telegram-agent-coordinator.plist'
            self.assertNotIn('--pair-only', plistlib.loads(target.read_bytes())['ProgramArguments'])
        with patch('coordinator.host_os.linux', return_value=False), patch('sys.platform', 'darwin'), redirect_stdout(io.StringIO()):
            SERVICE_INSTALLER['install'](write_only=True, pair_only=True)
        self.assertIn('--pair-only', plistlib.loads(target.read_bytes())['ProgramArguments'])

    def test_linux_default_and_explicit_pair_only_unit(self):
        for pair_only in (False, True):
            args = SimpleNamespace(enable_agents=False, pair_only=pair_only, coordinator_model='fake-model', write_only=True)
            with patch('os.getuid', return_value=1000), redirect_stdout(io.StringIO()):
                SERVICE_INSTALLER['install_linux'](args)
            unit = (self.root / '.config/systemd/user/local.telegram-agent-coordinator.service').read_text()
            self.assertEqual('--pair-only' in unit, pair_only)

    def test_private_setup_status_exact_refusal(self):
        self.store.put('mode', 'private')
        self.store.db.commit()
        with redirect_stdout(io.StringIO()) as out:
            self.assertEqual(INSTALLER['status'](), 1)
        self.assertEqual(out.getvalue().strip(), PRIVATE_REFUSAL)

    async def test_private_setup_refuses_until_explicit_conversion(self):
        self.store.put('mode', 'private')
        self.store.db.commit()
        with patch('builtins.input', return_value='n'), redirect_stdout(io.StringIO()) as out:
            self.assertEqual(await INSTALLER['setup'](), 1)
        self.assertIn(PRIVATE_REFUSAL, out.getvalue())
        self.assertEqual(self.store.get('mode'), 'private')
        with patch('builtins.input', return_value='y'), \
                patch.dict(self.globals, check_prereqs=Mock(return_value=([], True))), redirect_stdout(io.StringIO()):
            self.assertEqual(await INSTALLER['setup'](), 1)
        self.assertIsNone(self.store.get('mode'))
        self.assertEqual(self.store.get('execution'), 'pairing')
