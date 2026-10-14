import asyncio
from contextlib import contextmanager, redirect_stdout
import io
import json
import os
from pathlib import Path
import plistlib
import runpy
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from coordinator import isolation
from coordinator.accounts import AccountBroker, discover_accounts
from coordinator.attachment_paths import attachment_paths
from coordinator.codex_accounts import CodexBroker, discover_codex_accounts, refresh_codex_accounts
from coordinator.control_api import Refused
from coordinator.controls import op_delegation_codex
from coordinator.setup_flow import setup_state
from tests.test_group_setup import pair_group
from coordinator.shared_mcp import write_config
from coordinator.signin import SignIns, op_account_add
from coordinator.store import Store

ROOT = Path(__file__).resolve().parents[1]
SETUP = runpy.run_path(str(ROOT / 'scripts/setup.py'))
INSTALL = runpy.run_path(str(ROOT / 'scripts/install-service.py'))


class CodexInstallTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.binary = self.root / 'bin' / 'codex'
        self.binary.parent.mkdir()
        self.binary.write_bytes(b'\xcf\xfa\xed\xfefake Codex')
        self.binary.chmod(0o700)

    def test_missing_or_non_executable_helper_names_the_repair(self):
        helper = self.binary.parent / 'codex-code-mode-host'
        for present in (False, True):
            with self.subTest(present=present):
                if present:
                    helper.write_text('fake helper')
                    helper.chmod(0o600)
                problem = isolation.codex_install_problem(str(self.binary))
                self.assertIn('codex-code-mode-host is missing or not executable', problem)
                self.assertIn('Update or reinstall Codex 0.143.0 or newer the same way you installed it (for npm, without --omit=optional)', problem)
                self.assertIn('restart Torii', problem)

    def test_native_symlinks_and_package_resources_find_the_helper(self):
        launcher = self.root / 'codex'
        launcher.symlink_to(self.binary)
        for directory in (self.binary.parent, self.root / 'codex-resources'):
            with self.subTest(directory=directory):
                directory.mkdir(exist_ok=True)
                helper = directory / 'codex-code-mode-host'
                helper.write_text('fake helper')
                helper.chmod(0o700)
                self.assertIsNone(isolation.codex_install_problem(str(launcher)))
                helper.chmod(0o600)

    def test_npm_launcher_resolves_optional_dependency_and_legacy_vendor(self):
        for layout in ('sibling', 'nested', 'legacy'):
            with self.subTest(layout=layout):
                scope = self.root / layout / 'node_modules' / '@openai'
                package = scope / 'codex'
                launcher = package / 'bin' / 'codex.js'
                launcher.parent.mkdir(parents=True)
                launcher.write_text('fake npm launcher')
                launcher.chmod(0o700)
                vendor = (package / 'vendor' if layout == 'legacy' else
                          (scope if layout == 'sibling' else package / 'node_modules' / '@openai') /
                          'codex-darwin-arm64' / 'vendor')
                binary = vendor / 'aarch64-apple-darwin' / 'bin' / 'codex'
                binary.parent.mkdir(parents=True)
                binary.write_text('fake native binary')
                helper = binary.parent / 'codex-code-mode-host'
                with patch('sys.platform', 'darwin'), patch('platform.machine', return_value='arm64'):
                    self.assertIsNotNone(isolation.codex_install_problem(str(launcher)))
                    helper.write_text('fake helper')
                    helper.chmod(0o600)
                    self.assertIsNotNone(isolation.codex_install_problem(str(launcher)))
                    helper.chmod(0o700)
                    self.assertIsNone(isolation.codex_install_problem(str(launcher)))

    def pnpm_shim(self):
        home = self.root / 'pnpm'
        launcher = home / 'global/5/node_modules/@openai/codex/bin/codex.js'
        launcher.parent.mkdir(parents=True)
        launcher.write_text('fake npm launcher')
        helper = launcher.parent.parent / 'node_modules/@openai/codex-darwin-arm64/vendor/aarch64-apple-darwin/bin/codex-code-mode-host'
        helper.parent.mkdir(parents=True)
        helper.write_text('fake helper')
        helper.chmod(0o700)
        shim = home / 'codex'
        shim.write_text('#!/bin/sh\nexec node "$(dirname "$0")/global/5/node_modules/@openai/codex/bin/codex.js" "$@"\n')
        shim.chmod(0o700)
        return shim

    def test_pnpm_shell_shim_with_complete_package_does_not_warn(self):
        self.assertIsNone(isolation.codex_install_problem(str(self.pnpm_shim())))

    def test_volta_native_shim_symlink_does_not_warn(self):
        shim = self.root / 'volta-shim'
        shim.write_bytes(b'\xcf\xfa\xed\xfefake Volta')
        shim.chmod(0o700)
        launcher = self.root / 'codex'
        launcher.symlink_to(shim)
        self.assertIsNone(isolation.codex_install_problem(str(launcher)))

    def test_asdf_and_mise_script_and_native_shims_do_not_warn(self):
        for manager in ('asdf', 'mise'):
            with self.subTest(manager=manager):
                directory = self.root / manager
                directory.mkdir()
                script = directory / 'codex'
                script.write_text('#!/bin/sh\nexec ' + manager + ' exec codex "$@"\n')
                script.chmod(0o700)
                self.assertIsNone(isolation.codex_install_problem(str(script)))
                binary = directory / manager
                binary.write_bytes(b'\x7fELFfake manager')
                binary.chmod(0o700)
                link = directory / 'shims/codex'
                link.parent.mkdir()
                link.symlink_to(binary)
                self.assertIsNone(isolation.codex_install_problem(str(link)))

    def test_unknown_codex_file_does_not_warn(self):
        self.binary.write_text('unknown executable')
        self.assertIsNone(isolation.codex_install_problem(str(self.binary)))

    def test_elf_native_binary_requires_an_executable_helper(self):
        self.binary.write_bytes(b'\x7fELFfake Codex')
        self.assertIsNotNone(isolation.codex_install_problem(str(self.binary)))
        helper = self.binary.parent / 'codex-code-mode-host'
        helper.write_text('fake helper')
        helper.chmod(0o600)
        self.assertIsNotNone(isolation.codex_install_problem(str(self.binary)))
        helper.chmod(0o700)
        self.assertIsNone(isolation.codex_install_problem(str(self.binary)))

    def test_unsearchable_npm_dependency_directory_does_not_warn(self):
        launcher = self.root / 'prefix/lib/node_modules/@openai/codex/bin/codex.js'
        launcher.parent.mkdir(parents=True)
        launcher.write_text('fake npm launcher')
        launcher.chmod(0o700)
        blocked = self.root / 'node_modules'
        blocked.mkdir()
        blocked.chmod(0)
        self.addCleanup(blocked.chmod, 0o700)
        with patch('sys.platform', 'darwin'), patch('platform.machine', return_value='arm64'), \
                patch('pathlib.Path.is_file', side_effect=PermissionError(13, 'Permission denied', str(blocked))):
            self.assertIsNone(isolation.codex_install_problem(str(launcher)))

    def test_filesystem_errors_do_not_escape_install_check(self):
        helper = self.binary.parent / 'codex-code-mode-host'
        helper.write_text('fake helper')
        helper.chmod(0o700)
        for operation in ('shutil.which', 'pathlib.Path.resolve', 'pathlib.Path.open',
                          'pathlib.Path.is_file', 'os.access'):
            with self.subTest(operation=operation), \
                    patch('shutil.which', return_value=str(self.binary)), \
                    patch(operation, side_effect=OSError('Cannot inspect install')):
                self.assertIsNone(isolation.codex_install_problem(str(self.binary)))

    def test_npm_versions_before_code_mode_host_do_not_require_it(self):
        for version in ('0.132.0', '0.142.5', '0.142.5+build', '0.143.0', '0.160.1'):
            with self.subTest(version=version):
                package = self.root / version / 'node_modules/@openai/codex'
                launcher = package / 'bin/codex.js'
                launcher.parent.mkdir(parents=True)
                launcher.write_text('fake npm launcher')
                launcher.chmod(0o700)
                (package / 'package.json').write_text(json.dumps({'version': version}))
                with patch('sys.platform', 'darwin'), patch('platform.machine', return_value='arm64'):
                    problem = isolation.codex_install_problem(str(launcher))
                if version.startswith(('0.132.', '0.142.')):
                    self.assertIsNone(problem)
                else:
                    self.assertIn('codex-code-mode-host is missing or not executable', problem)

    def test_pnpm_codex_only_prerequisites_are_not_fatal(self):
        shim = self.pnpm_shim()
        with patch.dict(os.environ, {'PATH': str(shim.parent)}), patch('sys.platform', 'darwin'), \
                patch('subprocess.run', return_value=SimpleNamespace(returncode=0)) as run:
            rows, fatal = SETUP['check_prereqs']()
        run.assert_called_once_with([str(shim), '--version'], capture_output=True, timeout=10)
        self.assertEqual(dict(rows)['Codex CLI'], 'ok')
        self.assertFalse(fatal)

    def test_setup_done_invites_a_request_when_either_agent_is_connected(self):
        globals_ = SETUP['setup_done'].__globals__
        for claude, codex in ((False, False), (True, False), (False, True), (True, True)):
            with self.subTest(claude=claude, codex=codex), \
                    patch.dict(globals_, setup_state=Mock(return_value={'claude': claude, 'codex': codex})), \
                    redirect_stdout(io.StringIO()) as output:
                SETUP['setup_done'](None)
            action = ('send your first request.' if claude or codex else
                      'tap Add ChatGPT or Connect Claude.')
            self.assertEqual(output.getvalue(), 'Done. Open the Torii topic in your group and ' + action + '\n')

    def test_no_codex_install_does_not_warn_a_claude_only_install(self):
        self.assertIsNone(isolation.codex_install_problem(None))
        self.assertIsNone(isolation.codex_install_problem(str(self.root / 'missing')))


class NormalInstallerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name).resolve()
        env = patch.dict(os.environ, {'HOME': str(self.home), 'CODEX_HOME': str(self.home / '.codex'),
                                     'CLAUDE_CONFIG_DIR': str(self.home / '.claude')})
        env.start()
        self.addCleanup(env.stop)
        for name, value in (('pathlib.Path.home', self.home), ('sys.platform', 'darwin'),
                            ('coordinator.host_os.SYSTEM', 'darwin')):
            handle = patch(name, return_value=value) if name.endswith('.home') else patch(name, value)
            handle.start()
            self.addCleanup(handle.stop)
        binary_lookup = patch('shutil.which', return_value=None)
        binary_lookup.start()
        self.addCleanup(binary_lookup.stop)
        for module in ('subprocess.run', 'asyncio.create_subprocess_exec'):
            handle = patch(module, side_effect=AssertionError('No real native CLI or Keychain in tests'))
            handle.start()
            self.addCleanup(handle.stop)
        for name in ('.codex/auth.json', '.codex-accounts/foreign/auth.json', '.claude/projects/saved.jsonl', '.claude.json', '.config/other-app/config'):
            path = self.home / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('FOREIGN SENTINEL')
        self.native = self.home / '.local/bin/claude'
        self.native.parent.mkdir(parents=True)
        self.native.write_text('fake native binary')
        self.native.chmod(0o700)
        self.store = Store(self.home / '.local/state/telegram-agent-coordinator')
        self.addCleanup(self.store.close)

    @contextmanager
    def forbid_foreign_access(self):
        active = True
        home = self.home
        def audit(event, args):
            if not active or event not in ('open', 'os.listdir', 'os.scandir', 'os.mkdir', 'os.rename', 'os.symlink'):
                return
            for value in args[:2] if event in ('os.rename', 'os.symlink') else args[:1]:
                if not isinstance(value, (str, bytes, os.PathLike)):
                    continue
                path = Path(os.fsdecode(value)).absolute()
                for forbidden in (home / '.codex', home / '.codex-accounts', home / '.claude', home / '.claude.json'):
                    if path == forbidden or forbidden in path.parents:
                        raise AssertionError('Foreign path access: ' + str(path))
                config = home / '.config'
                if path == config or config in path.parents:
                    allowed = (config, config / 'telegram-agent-coordinator',
                               config / 'telegram-agent-coordinator/bot-token')
                    if path not in allowed or event in ('os.listdir', 'os.scandir'):
                        raise AssertionError('Foreign config access: ' + str(path))
        sys.addaudithook(audit)
        try:
            yield
        finally:
            active = False

    def test_service_plist_preserves_browser_when_set(self):
        browser = '/usr/bin/true'
        with self.forbid_foreign_access(), patch('coordinator.host_os.linux', return_value=False), \
                patch.dict(os.environ, {'BROWSER': browser}), redirect_stdout(io.StringIO()):
            INSTALL['install'](True, write_only=True)
        plist = plistlib.loads((self.home / 'Library/LaunchAgents/local.telegram-agent-coordinator.plist').read_bytes())
        self.assertEqual(plist['EnvironmentVariables'], {
            'PATH': '/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin', 'BROWSER': browser})

    def test_legacy_token_installer_keeps_temporary_files_out_of_config(self):
        legacy = runpy.run_path(str(ROOT / 'scripts/setup-telegram.py'))
        with self.forbid_foreign_access(), patch('getpass.getpass', return_value='fake-test-token'), \
                patch.dict(legacy['main'].__globals__, state_dir=lambda: self.store.directory), \
                redirect_stdout(io.StringIO()):
            self.assertEqual(legacy['main'](), 0)
        self.assertEqual(list((self.home / '.config/telegram-agent-coordinator').iterdir()),
                         [self.home / '.config/telegram-agent-coordinator/bot-token'])



if __name__ == '__main__':
    unittest.main()
