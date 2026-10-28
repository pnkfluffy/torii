"""Offline Muse pin, billing environment, owner switch and macOS write boundary."""

import hashlib
import json
import os
from pathlib import Path
import pwd
import shlex
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from coordinator import control_api, mcp, muse_accounts
from coordinator.store import Store


class MuseAccountsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='muse-m1-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.home = self.root / 'owner'
        self.home.mkdir()
        self.env = patch.dict(os.environ, {'HOME': str(self.home)}, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        muse_accounts._checked.clear()

    def call(self, name, source='cli', **params):
        with self.store.db:
            return control_api.call(self.store, name, params, source=source)

    def fake(self, output=None, exit_code=0):
        output = muse_accounts.PINNED_VERSION_BYTES if output is None else output
        binary = self.root / 'fake-muse'
        data = ('#!/bin/sh\n'
                '[ "$#" -eq 1 ] && [ "$1" = "--version" ] || exit 64\n'
                'printf %s ' + shlex.quote(output.decode()) + '\n'
                'exit ' + str(exit_code) + '\n').encode()
        data += b'\n' * (1024 - len(data))
        binary.write_bytes(data)
        binary.chmod(0o700)
        self.pin = patch.multiple(muse_accounts, PINNED_SIZE=len(data),
                                  PINNED_SHA256=hashlib.sha256(data).hexdigest())
        self.pin.start()
        self.addCleanup(self.pin.stop)
        return binary

    def portable_boundary(self):
        if sys.platform == 'darwin':
            return patch.object(muse_accounts, 'write_boundary', wraps=muse_accounts.write_boundary)
        return patch.object(muse_accounts, 'write_boundary', side_effect=lambda argv, roots: list(argv))

    def test_pin_constants(self):
        self.assertEqual(muse_accounts.PINNED_VERSION, '1.4.9-R6297.1')
        self.assertEqual(muse_accounts.PINNED_VERSION_BYTES, b'Muse Code 1.4.9 (1.4.9-R6297.1)\n')
        self.assertEqual(muse_accounts.PINNED_SHA256,
                         '2b856618ae3de3a63956e6d510b849ad707b16e5d2ed196299f3fe5ef2e3ff5e')
        self.assertEqual(muse_accounts.PINNED_SIZE, 368615952)
        self.assertEqual(muse_accounts.STABLE_FINGERPRINT,
                         'sha256:5d3b43e5ccdb7dc4f5a32c380f5f2cde0f10e3059b93610afb2e73f73b1d914c')

    def test_environment_excludes_billing_tokens_overrides_and_unknown_names(self):
        forbidden = ('META_API_KEY', 'OPENAI_API_KEY', 'ANTHROPIC_API_KEY', 'OTHER_API_KEY',
                     'GH_TOKEN', 'GITHUB_TOKEN', 'GH_ENTERPRISE_TOKEN', 'GITHUB_ENTERPRISE_TOKEN',
                     'CODEX_HOME', 'MUSE_AUTH_PATH', 'MUSE_TRANSPORT_TRACE',
                     'MUSE_PERMISSION_PROFILE_FAIL_OPEN', 'MUSE_EXPERIMENTAL_SDK_ENABLED',
                     'MUSE_SOMETHING_NEW', 'TBH_CREDENTIAL_BACKEND', 'TBH_SOMETHING_NEW',
                     'UNKNOWN_VALUE')
        with patch.dict(os.environ, dict.fromkeys(forbidden, 'synthetic-only')):
            env = muse_accounts.environment(self.root / 'worker')
        self.assertFalse(set(forbidden) & set(env))
        self.assertEqual(env['MUSE_NO_AUTO_UPDATE'], '1')
        self.assertEqual(env['TBH_DISABLE_TELEMETRY'], '1')
        self.assertEqual({n for n in env if n.startswith(('MUSE_', 'TBH_'))},
                         {'MUSE_NO_AUTO_UPDATE', 'TBH_DISABLE_TELEMETRY'})

    def test_environment_private_folders_and_open_stub(self):
        home = self.root / 'worker'
        with patch.dict(os.environ, PATH='/usr/bin:/bin', LANG='C', TERM='dumb'):
            env = muse_accounts.environment(home)
        self.assertEqual(env['HOME'], str(home))
        for name in ('HOME', 'XDG_CONFIG_HOME', 'XDG_DATA_HOME', 'XDG_STATE_HOME', 'XDG_CACHE_HOME', 'TMPDIR'):
            path = Path(env[name])
            self.assertTrue(path == home or home in path.parents)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o700)
        self.assertEqual((env['LANG'], env['TERM']), ('C', 'dumb'))
        stub = Path(env['PATH'].split(os.pathsep)[0]) / 'open'
        self.assertIn(home, stub.parents)
        result = subprocess.run([str(stub), 'https://example.test'], env=env, capture_output=True, timeout=10)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b'Torii blocked browser launch.', result.stderr)
        self.assertTrue(env['PATH'].endswith('/usr/bin:/bin'))
        self.assertNotIn('GH_HOST', env)

    def test_environment_preserves_only_named_git_routes(self):
        routes = dict(SSH_AUTH_SOCK='/synthetic/socket', GIT_SSH='/synthetic/ssh',
                      GIT_SSH_COMMAND='ssh -F /synthetic/config', GIT_SSH_VARIANT='ssh',
                      GIT_CONFIG_SYSTEM='/synthetic/system', GIT_CONFIG_NOSYSTEM='1',
                      GIT_CONFIG_COUNT='1', GIT_CONFIG_KEY_0='url.ssh://example.test.insteadOf',
                      GIT_CONFIG_VALUE_0='https://example.test', GIT_ASKPASS='/synthetic/ask',
                      SSH_ASKPASS='/synthetic/ask', GH_HOST='example.test',
                      GIT_CONFIG_GLOBAL='/synthetic/global', GH_CONFIG_DIR='/synthetic/gh')
        with patch.dict(os.environ, dict(routes, GIT_CONFIG_KEY_1='unrequested', GIT_OTHER='absent')):
            env = muse_accounts.environment(self.root / 'worker')
        for name, value in routes.items():
            self.assertEqual(env[name], value)
        self.assertNotIn('GIT_CONFIG_KEY_1', env)
        self.assertNotIn('GIT_OTHER', env)

    def test_environment_git_and_gh_fallbacks(self):
        config = self.home / '.gitconfig'
        config.write_text('')
        gh = self.home / '.config/gh'
        gh.mkdir(parents=True)
        env = muse_accounts.environment(self.root / 'worker')
        self.assertEqual(env['GIT_CONFIG_GLOBAL'], str(config))
        self.assertEqual(env['GH_CONFIG_DIR'], str(gh))

    def test_environment_refuses_symlinked_home_and_subfolders(self):
        link = self.root / 'linked'
        link.symlink_to(self.home, target_is_directory=True)
        with self.assertRaises(control_api.Refused):
            muse_accounts.environment(link)
        worker = self.root / 'worker'
        worker.mkdir()
        (worker / '.local').symlink_to(self.home, target_is_directory=True)
        with self.assertRaises(control_api.Refused):
            muse_accounts.environment(worker)

    def test_binary_checks_exact_version_and_disposable_private_home(self):
        binary = self.fake()
        original = subprocess.run
        runs = []

        def run(argv, **kwargs):
            runs.append((argv, kwargs))
            return original(argv, **kwargs)

        with self.portable_boundary() as boundary, patch.object(muse_accounts.subprocess, 'run', side_effect=run):
            muse_accounts.check_binary(str(binary))
            muse_accounts.check_binary(str(binary))
        self.assertEqual(len(runs), 1)
        argv, kwargs = runs[0]
        self.assertEqual(argv[-2:], [str(binary), '--version'])
        self.assertEqual(kwargs['stdin'], subprocess.DEVNULL)
        self.assertEqual(kwargs['stderr'], subprocess.DEVNULL)
        self.assertEqual(kwargs['timeout'], 10)
        self.assertEqual(kwargs['stdout'], subprocess.PIPE)
        self.assertNotEqual(kwargs['env']['HOME'], str(self.home))
        self.assertFalse(Path(kwargs['env']['HOME']).exists())
        self.assertEqual(boundary.call_count, 1)
        self.assertEqual(boundary.call_args.kwargs, {})

    def test_binary_refuses_missing_newline_extra_output_and_nonzero_exit(self):
        cases = [(muse_accounts.PINNED_VERSION_BYTES.rstrip(b'\n'), 0),
                 (muse_accounts.PINNED_VERSION_BYTES + b'extra\n', 0),
                 (muse_accounts.PINNED_VERSION_BYTES, 1)]
        for output, code in cases:
            with self.subTest(output=output, code=code):
                binary = self.fake(output, code)
                with self.portable_boundary(), self.assertRaisesRegex(control_api.Refused, 'Set muse.binary'):
                    muse_accounts.check_binary(str(binary))
                self.assertFalse(muse_accounts._checked)

    def test_binary_refuses_changed_byte_wrong_size_and_unpinned_file_before_exec(self):
        binary = self.fake()
        original = binary.read_bytes()
        for data in (original[:-1] + b' ', original + b'\n', b'#!/bin/sh\nexit 0\n'):
            binary.write_bytes(data)
            with self.subTest(size=len(data)), patch.object(muse_accounts.subprocess, 'run') as run:
                with self.assertRaisesRegex(control_api.Refused, 'Set muse.binary'):
                    muse_accounts.check_binary(str(binary))
                run.assert_not_called()

    def test_binary_refuses_symlink_group_other_write_wrong_owner_and_bad_paths(self):
        binary = self.fake()
        link = self.root / 'linked-muse'
        link.symlink_to(binary)
        for path in (None, 1, binary, 'relative', str(self.root), str(self.root / 'missing'), str(link)):
            with self.subTest(path=path), self.assertRaises(control_api.Refused):
                muse_accounts.check_binary(path)
        for mode in (0o720, 0o702):
            binary.chmod(mode)
            with self.subTest(mode=mode), self.portable_boundary(), self.assertRaises(control_api.Refused):
                muse_accounts.check_binary(str(binary))
        binary.chmod(0o700)
        with patch.object(muse_accounts.os, 'getuid', return_value=os.getuid() + 1):
            with self.assertRaises(control_api.Refused):
                muse_accounts.check_binary(str(binary))

    def test_binary_cache_revalidates_changed_content_and_permissions(self):
        binary = self.fake()
        with self.portable_boundary():
            muse_accounts.check_binary(str(binary))
        stamp = binary.stat()
        data = binary.read_bytes()
        binary.write_bytes(data[:-1] + b' ')
        os.utime(str(binary), ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
        with self.assertRaises(control_api.Refused):
            muse_accounts.check_binary(str(binary))
        binary.write_bytes(data)
        binary.chmod(0o720)
        with self.assertRaises(control_api.Refused):
            muse_accounts.check_binary(str(binary))

    def test_binary_timeout_and_execution_errors_refuse_and_remove_home(self):
        binary = self.fake()
        for error in (OSError('synthetic'), subprocess.TimeoutExpired('fake', 10)):
            homes = []

            def fail(argv, **kwargs):
                homes.append(Path(kwargs['env']['HOME']))
                raise error

            with self.portable_boundary(), patch.object(muse_accounts.subprocess, 'run', side_effect=fail):
                with self.assertRaisesRegex(control_api.Refused, 'Set muse.binary'):
                    muse_accounts.check_binary(str(binary))
            self.assertTrue(homes)
            self.assertTrue(all(not home.exists() for home in homes))
            self.assertFalse(muse_accounts._checked)

    def test_default_off_enable_requires_pin_and_darwin_disable_is_always_available(self):
        data = self.call('settings.get').data
        self.assertEqual({n: v for n, v in data.items() if n.startswith('muse_')},
                         {'muse_enabled': False, 'muse_binary': None})
        with patch.object(muse_accounts.subprocess, 'run') as run:
            self.assertFalse(self.call('delegation.muse', enabled=True).ok)
            run.assert_not_called()
        binary = self.fake()
        with self.portable_boundary():
            self.assertTrue(self.call('muse.binary', path=str(binary)).ok)
        with patch('sys.platform', 'linux'):
            self.assertFalse(self.call('delegation.muse', enabled=True).ok)
        self.assertFalse(self.store.get('muse_enabled', False))
        with patch('sys.platform', 'darwin'):
            self.assertTrue(self.call('delegation.muse', enabled=True).ok)
        self.assertTrue(self.call('settings.get').data['muse_enabled'])
        binary.chmod(0o720)
        self.assertFalse(self.call('delegation.muse', enabled=True).ok)
        with patch('sys.platform', 'linux'):
            self.assertTrue(self.call('delegation.muse', enabled=False).ok)
        self.assertFalse(self.call('settings.get').data['muse_enabled'])

    def test_binary_setting_saves_only_after_check_and_never_searches_path(self):
        with patch.object(muse_accounts.subprocess, 'run') as run:
            self.assertIsNone(muse_accounts.binary(self.store))
            self.assertFalse(self.call('muse.binary', path='/missing').ok)
            self.assertIsNone(self.store.get('muse_binary'))
            run.assert_not_called()
        binary = self.fake()
        with self.portable_boundary():
            result = self.call('muse.binary', path=str(binary))
        self.assertTrue(result.ok, result.text)
        self.assertEqual(muse_accounts.binary(self.store), str(binary))
        self.assertFalse(self.store.get('muse_enabled', False))
        self.assertFalse(self.call('muse.binary', path='/missing').ok)
        self.assertEqual(muse_accounts.binary(self.store), str(binary))

    def test_operations_are_write_local_only_without_worker_or_mcp_exposure(self):
        for name, params in (('delegation.muse', {'enabled': True}), ('muse.binary', {'path': '/missing'})):
            self.assertEqual(control_api.find(name).kind, control_api.WRITE)
            for source in ('mcp', 'telegram'):
                result = self.call(name, source=source, **params)
                self.assertFalse(result.ok)
                self.assertIn('local CLI', result.text)
            self.assertNotIn(name.replace('.', '_'), mcp.TOOLS)
        for enabled in (False, True):
            self.store.put('muse_enabled', enabled)
            self.assertFalse(self.call('workers.spawn', task=1, provider='muse', prompt='hello').ok)
        self.assertIn('muse', control_api.WORKER_PROVIDERS)

    def test_account_root_and_home_are_private_direct_real_children(self):
        root = muse_accounts.accounts_root()
        self.assertEqual(root, self.home / '.muse-accounts')
        self.assertEqual(stat.S_IMODE(root.stat().st_mode), 0o700)
        root.chmod(0o755)
        self.assertEqual(muse_accounts.accounts_root(), root)
        self.assertEqual(stat.S_IMODE(root.stat().st_mode), 0o700)
        home = root / '.torii-a1b2c3d4'
        home.mkdir(mode=0o700)
        self.assertEqual(muse_accounts.account_home(str(home)), home)
        for name in ('../x', '.torii-12345678', str(root / 'other'), str(root / '.torii-ABCDEF12'),
                     str(root / '.torii-1234567'), str(root / 'nested/.torii-12345678'),
                     str(root / 'nested/../.torii-a1b2c3d4'), str(self.root / '.torii-12345678')):
            with self.subTest(name=name), self.assertRaises(control_api.Refused):
                muse_accounts.account_home(name)
        link = root / '.torii-12345678'
        link.symlink_to(home, target_is_directory=True)
        with self.assertRaises(control_api.Refused):
            muse_accounts.account_home(str(link))

    def test_account_root_symlink_and_file_are_refused(self):
        root = self.home / '.muse-accounts'
        root.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(control_api.Refused):
            muse_accounts.accounts_root()
        with self.assertRaises(control_api.Refused):
            muse_accounts.account_home(str(root / '.torii-12345678'))
        with patch.object(muse_accounts.Path, 'home', return_value=self.root / 'another-home'):
            other = self.root / 'another-home'
            other.mkdir()
            (other / '.muse-accounts').write_text('synthetic')
            with self.assertRaises(control_api.Refused):
                muse_accounts.accounts_root()

    def test_boundary_refuses_unsupported_platform_missing_sandbox_and_bad_roots(self):
        worker = self.root / 'worker'
        worker.mkdir()
        link = self.root / 'linked-worker'
        link.symlink_to(worker, target_is_directory=True)
        file = self.root / 'file-root'
        file.write_text('synthetic')
        with patch('sys.platform', 'linux'), self.assertRaises(control_api.Refused):
            muse_accounts.write_boundary(['/bin/true'], [worker])
        with patch('sys.platform', 'darwin'), patch.object(muse_accounts.os, 'access', return_value=False):
            with self.assertRaises(control_api.Refused):
                muse_accounts.write_boundary(['/bin/true'], [worker])
        with patch('sys.platform', 'darwin'), patch.object(muse_accounts.os, 'access', return_value=True):
            for argv, roots in (([], [worker]), (['/bin/true'], []), (['/bin/true'], ['/']),
                                (['/bin/true'], ['relative']), (['/bin/true'], [worker / 'missing']),
                                (['/bin/true'], [link]), (['/bin/true'], [file])):
                with self.subTest(argv=argv, roots=roots), self.assertRaises(control_api.Refused):
                    muse_accounts.write_boundary(argv, roots)
            with patch.object(muse_accounts.json, 'dumps', side_effect=ValueError('synthetic')):
                with self.assertRaises(control_api.Refused):
                    muse_accounts.write_boundary(['/bin/true'], [worker])

    def test_boundary_refuses_roots_containing_passwd_library_even_with_different_home(self):
        passwd_home = self.root / 'passwd-home'
        library = passwd_home / 'Library'
        library.mkdir(parents=True)
        with patch('sys.platform', 'darwin'), patch.object(muse_accounts.os, 'access', return_value=True), \
                patch.object(pwd, 'getpwuid', return_value=type('User', (), {'pw_dir': str(passwd_home)})()):
            for root in (library, passwd_home, self.root, library.with_name('LIBRARY'),
                         passwd_home.with_name('PASSWD-HOME')):
                with self.subTest(root=root), self.assertRaises(control_api.Refused):
                    muse_accounts.write_boundary(['/bin/true'], [root])

    @unittest.skipUnless(sys.platform == 'darwin', 'macOS sandbox-exec enforces this boundary')
    def test_planted_profile_in_worker_root_is_never_used(self):
        worker = self.root / 'worker'
        worker.mkdir()
        planted = worker / '.muse-boundary-planted.sb'
        planted.write_text('(version 1)\n(allow default)\n')
        marker = self.root / 'outside'
        argv = muse_accounts.write_boundary(['/bin/sh', '-c', 'echo unsafe > ' + shlex.quote(str(marker))], [worker])
        if argv[1] == '-f' and worker in Path(argv[2]).parents:
            Path(argv[2]).write_text(planted.read_text())
        result = subprocess.run(argv, capture_output=True, timeout=10)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(marker.exists())
        self.assertEqual(argv[:2], ['/usr/bin/sandbox-exec', '-p'])

    @unittest.skipUnless(sys.platform == 'darwin', 'macOS sandbox-exec enforces this boundary')
    def test_boundary_allows_only_roots_and_dev_blocks_real_home_and_open(self):
        worker = self.root / 'worker'
        worker.mkdir()
        extra = self.root / 'results with "quotes"'
        extra.mkdir()
        outside = self.root / 'outside'
        real_home = self.root / 'fake-passwd-home'
        real_home.mkdir()
        real_target = real_home / ('torii-muse-m1-denied-' + worker.parent.name)
        script = ('import json, os, pathlib, subprocess, sys\n'
                  'results = {}\n'
                  'for name, value in json.loads(sys.argv[1]).items():\n'
                  '    try:\n'
                  '        pathlib.Path(value).write_text("synthetic boundary proof")\n'
                  '        results[name] = "written"\n'
                  '    except PermissionError:\n'
                  '        results[name] = "denied"\n'
                  'try:\n'
                  '    p = subprocess.run(["/usr/bin/open"], capture_output=True, timeout=5)\n'
                  '    results["open"] = "denied" if p.returncode < 0 else "executed"\n'
                  'except PermissionError:\n'
                  '    results["open"] = "denied"\n'
                  'print(json.dumps(results))\n')
        env = muse_accounts.environment(worker / 'home')
        paths = dict(inside=str(Path(env['HOME']) / 'inside'), extra=str(extra / 'inside'),
                     outside=str(outside), real_home=str(real_target), dev='/dev/null')
        argv = muse_accounts.write_boundary([sys.executable, '-c', script, json.dumps(paths)], [worker, extra])
        result = subprocess.run(argv, env=env, capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout),
                         dict(inside='written', extra='written', outside='denied', real_home='denied',
                              dev='written', open='denied'))
        self.assertTrue(Path(paths['inside']).is_file())
        self.assertTrue(Path(paths['extra']).is_file())
        self.assertFalse(outside.exists())
        self.assertFalse(os.path.lexists(str(real_target)))
        self.assertEqual(argv[:2], ['/usr/bin/sandbox-exec', '-p'])
        self.assertEqual(argv[2],
                         '(version 1)\n(allow default)\n(deny file-write*\n    (require-all\n'
                         '        (require-not (subpath ' + json.dumps(str(worker), ensure_ascii=False) + '))\n'
                         '        (require-not (subpath ' + json.dumps(str(extra), ensure_ascii=False) + '))\n'
                         '        (require-not (subpath "/dev"))))\n'
                         '(deny process-exec (literal "/usr/bin/open"))\n')

    @unittest.skipUnless(sys.platform == 'darwin', 'macOS sandbox-exec enforces this boundary')
    def test_broken_profile_never_executes_child(self):
        marker = self.root / 'executed'
        argv = muse_accounts.write_boundary(['/bin/sh', '-c', 'echo unsafe > ' + shlex.quote(str(marker))], [self.root])
        argv[2] = '(invalid-profile\n'
        result = subprocess.run(argv, capture_output=True, timeout=10)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(marker.exists())


if __name__ == '__main__':
    unittest.main()
