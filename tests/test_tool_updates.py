from contextlib import redirect_stderr
import io
import json
import os
from pathlib import Path
import subprocess
import sys
from unittest.mock import patch

from coordinator import updates
from tests.test_updates import TemporaryStore, plan, tool


class CommandFailureTests(TemporaryStore):
    def test_quoted_password_suffix_does_not_enter_stderr_tail(self):
        suffix = 'synthetic-private-password-suffix'
        for prefix in ('synthetic-prefix,', 'synthetic-prefix;', 'synthetic-prefix ', 'synthetic-"prefix,',
                       'synthetic-\\prefix,'):
            for quote in ('"', "'"):
                with self.subTest(prefix=prefix, quote=quote):
                    value = (prefix + suffix).replace('\\', '\\\\').replace(quote, '\\' + quote)
                    stderr = '{"password": ' + quote + value + quote + '}'
                    captured = io.StringIO()
                    with redirect_stderr(captured), self.assertRaises(RuntimeError) as caught:
                        updates.watch.command([sys.executable, '-c',
                                               'import sys; sys.stderr.write(sys.argv[1]); sys.exit(7)', stderr])
                    self.assertIn('{"password": [token]}', str(caught.exception))
                    self.assertNotIn(suffix, str(caught.exception))
                    self.assertNotIn(suffix, captured.getvalue())

    def test_command_error_keeps_redacted_single_line_stderr_tail(self):
        secrets = ['sk-test-' + 'z' * 450, '123456789:' + 'A' * 35, 'fake-bearer-value']
        stderr = ('discarded marker\n' + 'x' * 500 + '\napi_key=' + secrets[0] + '\n' + secrets[1] +
                  '\nAuthorization: Bearer ' + secrets[2] + '\nMissing optional dependency native_codex\n')
        captured = io.StringIO()
        with redirect_stderr(captured), self.assertRaises(RuntimeError) as caught:
            updates.watch.command([sys.executable, '-c',
                                   'import sys; sys.stderr.write(sys.argv[1]); sys.exit(7)', stderr])
        message = str(caught.exception)
        prefix = 'Command failed: ' + sys.executable + ': '
        self.assertTrue(message.startswith(prefix))
        self.assertTrue(message.endswith('Missing optional dependency native_codex'))
        self.assertLessEqual(len(message) - len(prefix), 400)
        self.assertNotIn('\n', message)
        self.assertNotIn('discarded marker', message)
        self.assertNotIn('z' * 30, message)
        for secret in secrets:
            self.assertNotIn(secret, message)
        self.assertIn('[token]', message)

    def test_command_error_without_stderr_keeps_existing_message(self):
        with redirect_stderr(io.StringIO()), self.assertRaises(RuntimeError) as caught:
            updates.watch.command([sys.executable, '-c', 'raise SystemExit(1)'])
        self.assertEqual(str(caught.exception), 'Command failed: ' + sys.executable)


class RuntimeInstallTests(TemporaryStore):
    def setUp(self):
        super().setUp()
        self.prefix = self.root / 'runtime'
        self.prefix.mkdir()
        self.manifest = self.prefix / 'package.json'
        self.manifest.write_text(json.dumps(dict(dependencies={
            '@anthropic-ai/claude-code': '1.0.0', '@openai/codex': '1.0.0'})))
        self.make_tool(self.prefix, '@anthropic-ai/claude-code', '1.0.0')
        self.make_tool(self.prefix, '@openai/codex', '1.0.0')
        self.cli = tool('Codex', prefix=str(self.prefix))
        self.cli['binary'] = str(self.prefix / 'node_modules/@openai/codex/cli.py')
        self.record('tools', watchdog_armed=True, attempted=[self.cli], **plan(['fix'], [self.cli]))
        self.tracking = (str(self.store.directory / 'state.sqlite'), 'test')
        self.before = self.snapshot()

    def make_tool(self, prefix, package, version):
        folder = prefix / 'node_modules' / package
        folder.mkdir(parents=True, exist_ok=True)
        (folder / 'package.json').write_text(json.dumps(dict(name=package, version=version,
                                                           bin={'codex' if package == '@openai/codex' else 'claude': 'cli.py'})))
        binary = folder / 'cli.py'
        binary.write_text('#!' + sys.executable + '\nprint(' + repr(version) + ')\n')
        binary.chmod(0o755)

    def snapshot(self):
        return {str(p.relative_to(self.prefix)): p.read_bytes() for p in self.prefix.rglob('*') if p.is_file()}

    def npm(self, argv, *args):
        destination = Path(argv[argv.index('--prefix') + 1])
        self.assertNotEqual(destination, self.prefix)
        self.assertEqual(self.snapshot(), self.before)
        self.assertEqual(json.loads((destination / 'package.json').read_text()), json.loads(self.manifest.read_text()))
        dependencies = json.loads((destination / 'package.json').read_text())['dependencies']
        for value in argv:
            if value.startswith('@') and '@' in value[1:]:
                package, version = value.rsplit('@', 1)
                dependencies[package] = version
        for package, version in dependencies.items():
            self.make_tool(destination, package, version)
        (destination / 'package.json').write_text(json.dumps(dict(dependencies=dependencies)))
        (destination / 'package-lock.json').write_text('{"lockfileVersion": 3}\n')

    def test_timed_out_install_keeps_live_runtime_and_rolls_back_torii(self):
        self.record('tools', watchdog_armed=True, attempted=[], **plan(['fix'], [self.cli]))
        def timeout(argv, *args):
            self.npm(argv, *args)
            raise RuntimeError('npm install timed out')
        with patch.object(updates.watch, 'tracked_install', side_effect=timeout), \
                patch.object(updates.watch, 'rollback_code', return_value='Torii restored to aaaaaaa.'), \
                patch.object(updates, 'git', return_value='b' * 40):
            self.assertTrue(self.once())
            self.assertEqual(self.snapshot(), self.before)
        record = self.store.get(updates.KEY)
        self.assertEqual(record['phase'], 'failed')
        self.assertIsNone(self.store.get('restart_requested_v2'))
        self.assertEqual(updates.outcome(record), 'Update failed: npm install timed out\n'
                         'Codex restored to 1.0.0.\nTorii restored to aaaaaaa.')

    def test_command_stderr_reaches_card_result_and_watchdog_log(self):
        self.record('tools', watchdog_armed=True, attempted=[], **plan(['fix'], [self.cli]))
        def fail(argv, *args):
            updates.watch.command([sys.executable, '-c',
                                   'import sys; sys.stderr.write(sys.argv[1]); sys.exit(1)',
                                   'Missing optional native package\n{"access_token":"fake-json-token"}'])
        with patch.object(updates.watch, 'tracked_install', side_effect=fail), \
                patch.object(updates.watch, 'rollback_code', return_value='Torii restored.'), \
                patch.object(updates, 'git', return_value='b' * 40), redirect_stderr(io.StringIO()):
            self.assertTrue(self.once())
        result = self.store.get(updates.KEY)['result']
        self.assertIn('Missing optional native package', result)
        self.assertIn(result, self.latest_text())
        self.assertIn(result, updates.outcome(self.store.get(updates.KEY)))
        log = self.store.directory / 'update-watchdogs/test/watchdog.log'
        self.assertIn('Missing optional native package', log.read_text())
        for text in (result, self.latest_text(), log.read_text()):
            self.assertNotIn('fake-json-token', text)

    def test_unwritable_watchdog_log_does_not_prevent_rollback(self):
        self.record('tools', watchdog_armed=True, attempted=[], **plan(['fix'], [self.cli]))
        real_open = Path.open
        def open_file(path, *args, **kwargs):
            if path.name == 'watchdog.log':
                raise OSError('Log unavailable')
            return real_open(path, *args, **kwargs)
        with patch.object(updates.watch, 'tracked_install', side_effect=RuntimeError('Install failed')), \
                patch.object(updates.watch, 'rollback_code', return_value='Torii restored.') as rollback, \
                patch.object(updates, 'git', return_value='b' * 40), \
                patch.object(Path, 'open', autospec=True, side_effect=open_file):
            self.assertTrue(self.once())
        rollback.assert_called_once()
        self.assertIn('Install failed', self.latest_text())
        self.assertIn('Codex restored to 1.0.0.', self.latest_text())

    def test_next_update_removes_only_stale_runtime_directories(self):
        stale = [self.root / ('.runtime-update-' + ending) for ending in ('recovered', 'incomplete')]
        for stage in stale:
            stage.mkdir()
            (stage / 'previous-runtime').write_text('old modules')
        unrelated = self.root / '.runtime-other'
        unrelated.mkdir()
        link = self.root / '.runtime-update-link'
        link.symlink_to(unrelated, target_is_directory=True)
        file = self.root / '.runtime-update-file'
        file.write_text('keep')
        self.record('arming', **plan(['fix'], [self.cli]))
        with patch.object(updates, 'git', return_value='a' * 40), \
                patch.object(updates.watch, 'process_identity', return_value='process'), \
                patch.object(updates, 'start_watchdog', return_value=444):
            self.assertTrue(self.once())
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'armed')
        self.assertEqual([stage.exists() for stage in stale], [False, False])
        self.assertTrue(unrelated.is_dir())
        self.assertTrue(link.is_symlink())
        self.assertEqual(file.read_text(), 'keep')
        self.assertEqual(self.snapshot(), self.before)

    def test_success_exchanges_modules_and_restores_offline(self):
        original_inode = (self.prefix / 'node_modules').stat().st_ino
        exchange = updates.watch.exchange
        exchanges = []
        def swap(left, right):
            if Path(left).name == 'node_modules':
                self.assertTrue(Path(left).is_dir())
                self.assertTrue(Path(right).is_dir())
                exchanges.append((Path(left).stat().st_ino, Path(right).stat().st_ino))
            exchange(left, right)
        with patch.object(updates.watch, 'tracked_install', side_effect=self.npm), \
                patch.object(updates.watch, 'exchange', side_effect=swap):
            runtime = updates.watch.install(self.cli, '2.0.0', self.tracking)
        self.assertEqual(exchanges, [((self.prefix / 'node_modules').stat().st_ino, original_inode)])
        self.assertEqual((Path(runtime['stage']) / 'node_modules').stat().st_ino, original_inode)
        self.assertEqual(updates.watch.version(self.cli['binary']), '2.0.0')
        self.assertNotEqual((self.prefix / 'node_modules').stat().st_ino, original_inode)
        with patch.object(updates.watch, 'tracked_install', side_effect=AssertionError('network restore')), \
                patch.object(updates.watch, 'install', side_effect=AssertionError('network restore')):
            record = self.store.get(updates.KEY)
            self.assertEqual(updates.watch.restore_tools(record, database=self.tracking[0]), ['Codex restored to 1.0.0.'])
            self.assertEqual(updates.watch.restore_tools(record, database=self.tracking[0]), ['Codex restored to 1.0.0.'])
        self.assertEqual(self.snapshot(), self.before)
        self.assertEqual((self.prefix / 'node_modules').stat().st_ino, original_inode)

    def test_staged_js_checks_and_rollback_without_node_on_path(self):
        make_tool = self.make_tool
        def make_js(prefix, package, version):
            make_tool(prefix, package, version)
            folder = prefix / 'node_modules' / package
            manifest = folder / 'package.json'
            data = json.loads(manifest.read_text())
            data['bin'] = {name: 'cli.js' for name in data['bin']}
            manifest.write_text(json.dumps(data))
            binary = folder / 'cli.js'
            binary.write_text('#!/usr/bin/env node\nconsole.log(' + json.dumps(version) + ');\n')
            binary.chmod(0o755)
        commands = self.root / 'npm-bin'
        commands.mkdir()
        node = commands / 'node'
        node.write_text('#!' + sys.executable + '\nimport json, sys\nfrom pathlib import Path\n'
                        'line = Path(sys.argv[1]).read_text().splitlines()[1]\n'
                        'print(json.loads(line[len("console.log("):-2]))\n')
        node.chmod(0o755)
        for package in updates.watch.CLI_PACKAGES.values():
            make_js(self.prefix, package, '1.0.0')
        self.cli['binary'] = str(self.prefix / 'node_modules/@openai/codex/cli.js')
        self.record('tools', attempted=[self.cli])
        self.before = self.snapshot()
        resolve = Path.resolve
        def resolve_node(path, *args, **kwargs):
            return node if str(path) == '/opt/homebrew/bin/node' else resolve(path, *args, **kwargs)
        with patch.dict(os.environ, PATH='/usr/bin:/bin'), \
                patch.object(Path, 'resolve', autospec=True, side_effect=resolve_node), \
                patch.object(self, 'make_tool', side_effect=make_js), \
                patch.object(updates.watch, 'tracked_install', side_effect=self.npm):
            updates.watch.install(self.cli, '2.0.0', self.tracking)
            self.assertEqual(updates.watch.version(self.cli['binary'], exact=True), '2.0.0')
            self.assertTrue(updates.watch.tools_match(self.store.get(updates.KEY), 'latest'))
            self.assertEqual(updates.watch.restore_tools(self.store.get(updates.KEY), database=self.tracking[0]),
                             ['Codex restored to 1.0.0.'])
            self.assertTrue(updates.watch.tools_match(self.store.get(updates.KEY), 'current'))
        self.assertEqual(self.snapshot(), self.before)

    def test_failed_post_swap_check_restores_original_without_npm(self):
        real_version = updates.watch.version
        def version(binary, *args, **kwargs):
            actual = real_version(binary, *args, **kwargs)
            if str(binary) == self.cli['binary'] and actual == '2.0.0':
                raise RuntimeError('post-swap check failed')
            return actual
        with patch.object(updates.watch, 'tracked_install', side_effect=self.npm) as npm, \
                patch.object(updates.watch, 'version', side_effect=version):
            with self.assertRaisesRegex(RuntimeError, 'post-swap check failed'):
                updates.watch.install(self.cli, '2.0.0', self.tracking)
        self.assertEqual(npm.call_count, 1)
        self.assertEqual(self.snapshot(), self.before)
        self.assertEqual(updates.watch.version(self.cli['binary']), '1.0.0')

    def test_missing_unchanged_tool_in_stage_does_not_replace_live_runtime(self):
        def incomplete(argv, *args):
            destination = Path(argv[argv.index('--prefix') + 1])
            self.make_tool(destination, '@openai/codex', '2.0.0')
        with patch.object(updates.watch, 'tracked_install', side_effect=incomplete):
            with self.assertRaises(RuntimeError):
                updates.watch.install(self.cli, '2.0.0', self.tracking)
        self.assertEqual(self.snapshot(), self.before)

    def test_slow_install_can_outlast_previous_five_minute_limit(self):
        self.record('tools', attempted=[])
        child = type('Child', (), dict(pid=444, returncode=0, poll=lambda self: None))()
        waits = []
        def communicate(timeout):
            waits.append(timeout)
            if timeout < 480 and len(waits) == 1:
                raise subprocess.TimeoutExpired('npm', timeout)
            return '', ''
        child.communicate = communicate
        with patch.object(updates.watch.subprocess, 'Popen', return_value=child), \
                patch.object(updates.watch.os, 'killpg'):
            updates.watch.tracked_install(['/opt/homebrew/bin/npm'], *self.tracking)
        self.assertEqual(len(waits), 1)
        self.assertGreaterEqual(waits[0], 480)
        self.assertIsNone(self.store.get(updates.KEY).get('installation'))

    def test_failed_manifest_swap_restores_modules_and_original_manifest(self):
        exchange = updates.watch.exchange
        def fail_metadata(left, right):
            if Path(left).name == 'package.json':
                raise OSError('manifest swap failed')
            exchange(left, right)
        with patch.object(updates.watch, 'tracked_install', side_effect=self.npm), \
                patch.object(updates.watch, 'exchange', side_effect=fail_metadata):
            with self.assertRaisesRegex(OSError, 'manifest swap failed'):
                updates.watch.install(self.cli, '2.0.0', self.tracking)
        self.assertEqual(self.snapshot(), self.before)
        self.assertEqual(updates.watch.version(self.cli['binary']), '1.0.0')

    def test_process_death_after_exchange_recovers_from_saved_journal_without_npm(self):
        code = ('import os, shutil\nfrom pathlib import Path\nfrom coordinator import updates\n' +
                'tool=' + repr(self.cli) + '\n' +
                'def install(argv,*args):\n' +
                '    stage=Path(argv[argv.index("--prefix")+1])\n' +
                '    shutil.copytree(tool["prefix"],stage,dirs_exist_ok=True)\n' +
                '    binary=stage/"node_modules/@openai/codex/cli.py"\n' +
                '    binary.write_text(binary.read_text().replace("1.0.0","2.0.0"))\n' +
                'exchange=updates.watch.exchange\n' +
                'def swap(left,right):\n' +
                '    exchange(left,right)\n' +
                '    if Path(left).name=="node_modules": os._exit(23)\n' +
                'updates.watch.exchange=swap\nupdates.watch.tracked_install=install\n' +
                'updates.watch.local_npm=lambda: "/opt/homebrew/bin/npm"\n' +
                'updates.watch.install(tool,"2.0.0",' + repr(self.tracking) + ')\n')
        child = subprocess.run([sys.executable, '-c', code], cwd=updates.ROOT, capture_output=True, timeout=20)
        self.assertEqual(child.returncode, 23, child.stderr)
        self.assertEqual(updates.watch.version(self.cli['binary']), '2.0.0')
        with patch.object(updates.watch, 'install', side_effect=AssertionError('npm called')):
            restored = updates.watch.restore_tools(self.store.get(updates.KEY), database=self.tracking[0])
        self.assertEqual(restored, ['Codex restored to 1.0.0.'])
        self.assertEqual(self.snapshot(), self.before)

    def test_two_tools_in_one_prefix_restore_in_reverse_order(self):
        original = self.before
        claude = tool(prefix=str(self.prefix))
        claude['binary'] = str(self.prefix / 'node_modules/@anthropic-ai/claude-code/cli.py')
        with patch.object(updates.watch, 'tracked_install', side_effect=self.npm):
            updates.watch.install(self.cli, '2.0.0', self.tracking)
            self.before = self.snapshot()
            updates.watch.write(self.store.db, 'test', attempted=[self.cli, claude])
            updates.watch.install(claude, '2.0.0', self.tracking)
        self.assertEqual([updates.watch.version(t['binary']) for t in (self.cli, claude)], ['2.0.0', '2.0.0'])
        record = self.store.get(updates.KEY)
        restored = updates.watch.restore_tools(record, database=self.tracking[0])
        self.assertEqual(restored, ['Claude Code restored to 1.0.0.', 'Codex restored to 1.0.0.'])
        self.assertEqual(self.snapshot(), original)
        self.assertEqual(updates.watch.restore_tools(record, database=self.tracking[0]), restored)

    def test_local_npm_ignores_path_and_rejects_router_at_known_location(self):
        self.local_npm_patch.stop()
        def resolve(path):
            return Path('/usr/local/lib/node_modules/npm/bin/npm-cli.js' if str(path) == '/usr/local/bin/npm'
                        else '/router/shim.py')
        with patch.object(Path, 'resolve', autospec=True, side_effect=resolve), \
                patch.object(updates.watch.os, 'access', return_value=True), \
                patch.dict(os.environ, PATH='/router'):
            self.assertEqual(updates.watch.local_npm(), '/usr/local/bin/npm')

    def test_local_npm_falls_back_beside_resolved_node(self):
        self.local_npm_patch.stop()
        node = self.root / 'nvm/bin/node'
        node.parent.mkdir(parents=True)
        node.touch()
        link = self.root / 'node'
        link.symlink_to(node)
        npm = node.parent / 'npm'
        cli = self.root / 'npm-cli.js'
        cli.touch()
        npm.symlink_to(cli)
        with patch.object(updates.watch.shutil, 'which', return_value=str(link)), \
                patch.object(updates.watch.os, 'access', side_effect=lambda path, mode: path == str(npm)):
            self.assertEqual(updates.watch.local_npm(), str(npm))
