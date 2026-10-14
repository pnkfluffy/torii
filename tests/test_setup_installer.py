import asyncio
from contextlib import redirect_stdout
import fcntl
import hashlib
import io
import os
from pathlib import Path
import runpy
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from coordinator import __main__ as cli
from coordinator.store import Store
from tests.test_group_setup import group_update, pair_group, GroupTelegram

ROOT = Path(__file__).resolve().parents[1]
INSTALLER = runpy.run_path(str(ROOT / 'scripts/setup.py'))
SERVICE_INSTALLER = runpy.run_path(str(ROOT / 'scripts/install-service.py'))


def no_external(argv, *args, **kwargs):
    name = Path(argv[0] if isinstance(argv, (list, tuple)) else argv).name
    if name == 'security':
        raise AssertionError('Tests must never start /usr/bin/security')
    raise AssertionError('Unexpected external command ' + name)


def login_launcher(run):
    async def launch(*argv, **kwargs):
        result = run(list(argv), **kwargs)
        return SimpleNamespace(returncode=result.returncode, wait=AsyncMock(return_value=result.returncode))
    return launch


class SetupPrerequisiteTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.binary = self.root / 'bin' / 'codex'
        self.binary.parent.mkdir()
        self.binary.write_bytes(b'\xcf\xfa\xed\xfefake Codex')
        self.binary.chmod(0o700)
        run = patch('subprocess.run', return_value=SimpleNamespace(returncode=0))
        run.start()
        self.addCleanup(run.stop)

    def test_codex_without_helper_stops_setup_with_the_repair(self):
        with patch.dict(os.environ, {'PATH': str(self.binary.parent)}), patch('sys.platform', 'darwin'):
            rows, fatal = INSTALLER['check_prereqs']()
        self.assertTrue(fatal)
        self.assertIn('optional', dict(rows)['Claude CLI'])
        self.assertIn('codex-code-mode-host is missing or not executable', dict(rows)['Codex CLI'])
        self.assertIn('Update or reinstall Codex 0.143.0 or newer the same way you installed it (for npm, without --omit=optional)', dict(rows)['Codex CLI'])
        self.assertIn('run setup again or restart Torii', dict(rows)['Codex CLI'])

    def test_codex_with_helper_meets_setup_prerequisites(self):
        helper = self.binary.parent / 'codex-code-mode-host'
        helper.write_text('#!/bin/sh\nexit 0\n')
        helper.chmod(0o700)
        with patch.dict(os.environ, {'PATH': str(self.binary.parent)}), patch('sys.platform', 'darwin'):
            rows, fatal = INSTALLER['check_prereqs']()
        self.assertFalse(fatal)
        self.assertEqual(dict(rows)['Codex CLI'], 'ok')


class SetupInstallerTestsSupport:
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.patches = [patch('pathlib.Path.home', return_value=self.root),
                        patch.dict(os.environ, {'HOME': str(self.root), 'TORII_STATE_DIR': str(self.root / 'state'),
                                                'CODEX_HOME': str(self.root / 'codex')}),
                        patch('subprocess.run', side_effect=no_external),
                        patch('asyncio.create_subprocess_exec', side_effect=lambda *args, **kwargs: no_external(args)),
                        patch('coordinator.telegram.Telegram._request', side_effect=AssertionError('No Telegram in tests'))]
        for handle in self.patches:
            handle.start()
        self.store = Store(self.root / 'state')
        self.globals = INSTALLER['setup'].__globals__
        browser = patch.dict(self.globals, open_group_link=Mock(), connect_claude=AsyncMock(return_value=False))
        browser.start()
        self.patches.append(browser)
        self.api = SimpleNamespace(call=AsyncMock(), send=AsyncMock(return_value={'message_id': 900}))
        self.bot = {'id': 100, 'is_bot': True, 'username': 'test_bot', 'has_topics_enabled': False}

    async def asyncTearDown(self):
        self.store.close()
        for handle in reversed(self.patches):
            handle.stop()
        self.temp.cleanup()


class SetupInstallerTests(SetupInstallerTestsSupport, unittest.IsolatedAsyncioTestCase):
    async def test_security_guard_covers_sync_and_async_launch(self):
        import subprocess
        with self.assertRaisesRegex(AssertionError, 'security'):
            subprocess.run(['/usr/bin/security', 'help'])
        with self.assertRaisesRegex(AssertionError, 'security'):
            await asyncio.create_subprocess_exec('/usr/bin/security')

    async def test_service_start_check_success_and_launchd_denial(self):
        globals_ = SERVICE_INSTALLER['wait_for_start'].__globals__
        with patch.dict(globals_, service_running=Mock(side_effect=[False, True])), \
                patch('time.sleep'), patch('subprocess.run', side_effect=AssertionError('No launchctl on success')):
            SERVICE_INSTALLER['wait_for_start'](self.root / 'state', 'gui/7', 'local.test', timeout=2)
        launchd = SimpleNamespace(returncode=0, stdout='pid = 123\nlast exit code = 78\n')
        with patch('subprocess.run', side_effect=[launchd, SimpleNamespace(returncode=0)]) as run:
            with self.assertRaisesRegex(ValueError, 'did not start.*Last exit status: 78'):
                SERVICE_INSTALLER['wait_for_start'](self.root / 'state', 'gui/7', 'local.test', timeout=0)
        self.assertEqual(run.call_args.args[0], ['launchctl', 'bootout', 'gui/7/local.test'])
        with patch('subprocess.run', side_effect=[launchd, SimpleNamespace(returncode=0)]):
            with self.assertRaisesRegex(ValueError, 'external drives.*run scripts/run-service.sh'):
                SERVICE_INSTALLER['wait_for_start'](Path('/Volumes/Torii/state'),
                                                    'gui/7', 'local.test', timeout=0)

    async def test_install_checks_bootstrap_result_before_success_message(self):
        globals_ = SERVICE_INSTALLER['install'].__globals__
        checked = Mock()
        with patch.dict(globals_, unload=Mock(), wait_for_start=checked), \
                patch('coordinator.host_os.linux', return_value=False), patch('sys.platform', 'darwin'), \
                patch('subprocess.run', return_value=SimpleNamespace(returncode=0)) as run, \
                redirect_stdout(io.StringIO()) as out:
            SERVICE_INSTALLER['install'](True)
        self.assertEqual(run.call_args.args[0][:2], ['launchctl', 'bootstrap'])
        checked.assert_called_once()
        self.assertIn('Service installed', out.getvalue())

    async def test_installer_login_failure_and_ctrl_c_fall_through(self):
        pair_group(self.store, bot=self.bot)
        for outcome in (KeyboardInterrupt(), OSError('fake failed')):
            with patch('asyncio.create_subprocess_exec', side_effect=outcome), redirect_stdout(io.StringIO()) as out:
                self.assertFalse(await INSTALLER['connect_claude'](self.store))
            self.assertIn('The bot will ask you to connect Claude', out.getvalue())
            self.assertEqual(self.store.get('execution'), 'pairing')

    async def test_reinstall_discovers_internal_profile_without_login(self):
        from coordinator.signin import SignIns
        directory = self.root / '.claude-accounts/.torii-12345678'
        directory.mkdir(parents=True)
        (directory / '.claude.json').touch()
        with self.store.db:
            self.store.put('setup_claude_dir', str(directory.resolve()))
        with patch.object(SignIns, 'identity', AsyncMock(return_value={'email': 'owner@example.test'})), \
                redirect_stdout(io.StringIO()):
            self.assertTrue(await INSTALLER['connect_claude'](self.store))
        self.assertEqual(self.store.get('execution'), 'agents')

    async def test_reinstall_keeps_owner_disabled_profiles_disabled(self):
        from coordinator.signin import SignIns
        with self.store.db:
            self.store.put('accounts', {'disabled': {'config_dir': str(self.root), 'enabled': False}})
        login = Mock(return_value=SimpleNamespace(returncode=1))
        with patch.object(SignIns, 'identity', AsyncMock(side_effect=AssertionError('Disabled profile'))), \
                patch('asyncio.create_subprocess_exec', login_launcher(login)), \
                redirect_stdout(io.StringIO()):
            self.assertFalse(await INSTALLER['connect_claude'](self.store))
        login.assert_called_once()
        self.assertIsNone(self.store.get('setup_claude_dir'))
        self.assertFalse(self.store.get('accounts')['disabled']['enabled'])

    async def test_unlogged_profile_does_not_skip_installer_login(self):
        from coordinator.signin import SignIns
        directory = self.root / '.claude-accounts/leftover'
        directory.mkdir(parents=True)
        (directory / '.claude.json').touch()
        login = Mock(return_value=SimpleNamespace(returncode=0))
        with patch.object(SignIns, 'identity', AsyncMock(side_effect=[None, {'email': 'owner@example.test'}])), \
                patch('asyncio.create_subprocess_exec', login_launcher(login)), \
                redirect_stdout(io.StringIO()):
            self.assertTrue(await INSTALLER['connect_claude'](self.store))
        login.assert_called_once()
        self.assertNotEqual(login.call_args.kwargs['cwd'], str(directory))
        self.assertIsNone(self.store.get('setup_claude_dir'))

    async def test_failed_identity_cannot_be_adopted_on_restart(self):
        from coordinator.accounts import discover_accounts
        from coordinator.signin import SignIns
        async def login(store, directory):
            (directory / '.claude.json').touch()
            return True
        with patch.dict(self.globals, claude_login=login), \
                patch.object(SignIns, 'identity', AsyncMock(return_value=None)), redirect_stdout(io.StringIO()):
            self.assertFalse(await INSTALLER['connect_claude'](self.store))
        self.assertIsNone(self.store.get('setup_claude_dir'))
        self.assertEqual(discover_accounts(self.store), {})

    async def test_paired_rerun_skips_enabled_claude_unless_connect_is_explicit(self):
        pair_group(self.store, bot=self.bot)
        with self.store.db:
            self.store.put('accounts', {'owner': {'config_dir': str(self.root), 'enabled': True}})
        for connect in (None, True):
            login = AsyncMock(return_value=True)
            with patch('coordinator.setup_terminal.interactive_mac', return_value=True), \
                    patch.dict(self.globals, state_dir=lambda: self.store.directory, service_running=lambda directory: True,
                               connect_claude=login, status=Mock(), setup_done=Mock()), redirect_stdout(io.StringIO()):
                self.assertEqual(await INSTALLER['setup'](connect=connect), 0)
            self.assertEqual(login.await_count, int(connect is True))

    async def test_invalid_bot_and_webhook_are_checked_before_saved_token(self):
        folder = self.root / 'config'
        for rejected in ({'is_bot': False, 'username': 'owner'}, self.bot):
            async def check(method, **kwargs):
                self.assertFalse((folder / 'bot-token').exists())
                if method == 'getMe':
                    return rejected
                return {'url': 'https://example.test/hook'}
            with patch('coordinator.setup_terminal.interactive_mac', return_value=True), \
                    patch('coordinator.setup_terminal.obtain_candidate', side_effect=['FAKE-INVALID-BOT', EOFError()]), \
                    patch('coordinator.telegram.Telegram.call', side_effect=check), redirect_stdout(io.StringIO()):
                with self.assertRaises(EOFError):
                    await INSTALLER['obtain_token'](folder, self.store)
            self.assertFalse((folder / 'bot-token').exists())
            self.assertEqual(list(folder.iterdir()), [])

    def test_tty_refusal_precedes_state_and_credentials(self):
        with patch('sys.stdin.isatty', return_value=False), patch('sys.stdout.isatty', return_value=False), \
                patch.dict(self.globals, obtain_token=Mock(side_effect=AssertionError('No token'))), \
                patch.dict(self.globals, Store=Mock(side_effect=AssertionError('No state'))), redirect_stdout(io.StringIO()) as out:
            self.assertEqual(INSTALLER['main']([]), 1)
        self.assertIn('Run this in your own terminal:', out.getvalue())

    def test_status_is_read_only_and_prints_no_ids_or_token(self):
        pair_group(self.store, user=72818291, bot=self.bot)
        directory = self.root / '.config/telegram-agent-coordinator'
        directory.mkdir(parents=True)
        (directory / 'bot-token').write_text('FAKE-TOKEN-STATUS-ONLY')
        before = (self.root / 'state/state.sqlite').stat().st_mtime_ns
        with patch('sys.stdin.isatty', return_value=False), redirect_stdout(io.StringIO()) as out:
            self.assertEqual(INSTALLER['main'](['--status']), 0)
        text = out.getvalue()
        self.assertIn('Bot: @test_bot', text)
        self.assertIn('Mode: group', text)
        self.assertNotIn('72818291', text)
        self.assertNotIn('FAKE-TOKEN', text)
        self.assertEqual((self.root / 'state/state.sqlite').stat().st_mtime_ns, before)

    def test_prerequisites_are_all_fake_and_codex_is_optional(self):
        def fake_run(argv, **kwargs):
            if Path(argv[0]).name == 'security':
                no_external(argv)
            return SimpleNamespace(returncode=0)
        with patch('shutil.which', side_effect=lambda name: '/fake/claude' if name == 'claude' else None), \
                patch('subprocess.run', side_effect=fake_run), patch('sys.platform', 'darwin'):
            rows, fatal = INSTALLER['check_prereqs']()
            self.assertFalse(fatal)
            self.assertIn('optional', dict(rows)['Codex CLI'])
            with patch('sys.version_info', (3, 8, 0)):
                self.assertTrue(INSTALLER['check_prereqs']()[1])
            with patch('shutil.which', return_value=None):
                self.assertTrue(INSTALLER['check_prereqs']()[1])
            with patch('sys.platform', 'linux'), patch('os.getuid', return_value=0):
                self.assertTrue(INSTALLER['check_prereqs']()[1])

    def test_codex_alone_meets_prerequisites(self):
        with patch('shutil.which', side_effect=lambda name: '/fake/codex' if name == 'codex' else None), \
                patch('subprocess.run', return_value=SimpleNamespace(returncode=0)), \
                patch('sys.platform', 'darwin'):
            self.assertFalse(INSTALLER['check_prereqs']()[1])

    async def test_hidden_token_storage_reuse_and_webhook_refusal(self):
        from coordinator.telegram import Telegram
        token = 'FAKE-INSTALLER-TOKEN-01234'
        with patch('coordinator.telegram.Telegram.call', AsyncMock(side_effect=[self.bot, {}])), \
                patch('getpass.getpass', return_value=token) as hidden, redirect_stdout(io.StringIO()) as out:
            api, bot = await INSTALLER['obtain_token'](self.root / 'config')
        self.assertEqual(bot['username'], 'test_bot')
        self.assertEqual((self.root / 'config/bot-token').stat().st_mode & 0o777, 0o600)
        self.assertNotIn(token, out.getvalue())
        hidden.assert_called_once_with('Paste the bot token from @BotFather (input is hidden): ')
        with patch.object(Telegram, 'call', AsyncMock(side_effect=[self.bot, {}])), patch('builtins.input', return_value='Y'), \
                patch('getpass.getpass', side_effect=AssertionError('Reuse needs no token')), redirect_stdout(io.StringIO()):
            await INSTALLER['obtain_token'](self.root / 'config')
        with patch.object(Telegram, 'call', AsyncMock(side_effect=[self.bot, {'url': 'https://example.test/hook'}])):
            with self.assertRaisesRegex(ValueError, 'webhook'):
                await INSTALLER['check_bot'](self.root / 'config/bot-token')

    async def test_busy_bot_signals_require_explicit_yes_before_pairing(self):
        for responses in (([{'command': 'old'}], {}), ([], {'description': 'Used'})):
            self.api.call.reset_mock()
            self.api.call.side_effect = responses
            with patch('builtins.input', return_value='n'), redirect_stdout(io.StringIO()) as out:
                with self.assertRaisesRegex(ValueError, 'new bot'):
                    await INSTALLER['confirm_busy_bot'](self.store, self.api, self.bot)
            self.assertIn('may already be in use', out.getvalue())
            self.assertNotIn('getUpdates', [call.args[0] for call in self.api.call.await_args_list])
        self.api.call.reset_mock()
        self.api.call.side_effect = [[], {}]
        with patch('builtins.input', side_effect=AssertionError('New bot needs no confirmation')):
            await INSTALLER['confirm_busy_bot'](self.store, self.api, self.bot)
        self.api.call.assert_awaited()

    async def test_setup_errors_name_token_network_and_service(self):
        from coordinator.telegram import TelegramError
        for error, expected in ((TelegramError(401), 'Telegram rejected this token'),
                                (TelegramError(502), 'Telegram could not be reached'),
                                (OSError('synthetic'), 'service failed to install or start')):
            with patch.dict(self.globals, require_tty=lambda: True, setup=AsyncMock(side_effect=error)), \
                    redirect_stdout(io.StringIO()) as out:
                self.assertEqual(await asyncio.to_thread(INSTALLER['main'], []), 1)
            self.assertIn(expected, out.getvalue())

    def test_verify_interpreter_respects_override(self):
        verification = runpy.run_path(str(ROOT / 'scripts/verify-local.py'))
        with patch.dict(os.environ, {'TORII_PYTHON': os.sys.executable}):
            self.assertEqual(verification['interpreter'](), os.sys.executable)
        with patch.dict(os.environ, {'TORII_PYTHON': str(self.root / 'missing-python')}):
            with self.assertRaisesRegex(RuntimeError, 'TORII_PYTHON must name'):
                verification['interpreter']()

    async def test_open_link_uses_platform_display_and_always_prints_fallback(self):
        opener = INSTALLER['open_group_link']
        for platform, display, expected in (('darwin', '', 'open'), ('linux', ':0', 'xdg-open'), ('linux', '', None)):
            with patch('sys.platform', platform), patch.dict(os.environ, {'DISPLAY': display, 'WAYLAND_DISPLAY': ''}, clear=True), \
                    patch('subprocess.run', return_value=SimpleNamespace(returncode=0)) as run, patch('builtins.input', return_value=''), redirect_stdout(io.StringIO()) as out:
                opener('test_bot', 'fake-link-code')
            self.assertIn('https://t.me/test_bot?startgroup=fake-link-code&admin=manage_topics+delete_messages+pin_messages', out.getvalue())
            if expected:
                self.assertEqual(run.call_args.args[0][0], expected)
            else:
                run.assert_not_called()

    async def test_open_link_uses_browsers_single_executable_when_set(self):
        opener = INSTALLER['open_group_link']
        browser = '/path with spaces/browser'
        with patch('sys.platform', 'darwin'), patch.dict(os.environ, {'BROWSER': browser}, clear=True), \
                patch('subprocess.run', return_value=SimpleNamespace(returncode=0)) as run, patch('builtins.input', return_value=''), redirect_stdout(io.StringIO()) as out:
            opener('test_bot', 'fake-link-code')
        self.assertEqual(run.call_args.args[0], [browser, 'https://t.me/test_bot?startgroup=fake-link-code&admin=manage_topics+delete_messages+pin_messages'])
        self.assertFalse(run.call_args.kwargs['check'])
        self.assertEqual(run.call_args.kwargs['timeout'], 10)
        self.assertIn('https://t.me/test_bot?startgroup=fake-link-code&admin=manage_topics+delete_messages+pin_messages', out.getvalue())

    async def test_bot_token_cross_volume_replace_uses_private_destination_staging(self):
        import errno
        from coordinator.bot_token import save_token

        directory = self.root / '.config/telegram-agent-coordinator'
        staging = self.root / 'separate-state'
        staging.mkdir()
        replace = os.replace
        calls = []

        def cross_volume_once(source, destination):
            calls.append((Path(source), Path(destination)))
            if len(calls) == 1:
                raise OSError(errno.EXDEV, 'Cross-device link')
            return replace(source, destination)

        with patch('coordinator.bot_token.os.replace', side_effect=cross_volume_once):
            saved = save_token('FAKE-CROSS-VOLUME-TOKEN', directory, temporary_folder=staging)
        self.assertEqual(saved, directory / 'bot-token')
        self.assertEqual(saved.read_text(), 'FAKE-CROSS-VOLUME-TOKEN\n')
        self.assertEqual(saved.stat().st_mode & 0o777, 0o600)
        self.assertEqual([path.name for path in directory.iterdir()], ['bot-token'])
        self.assertEqual(list(staging.iterdir()), [])
