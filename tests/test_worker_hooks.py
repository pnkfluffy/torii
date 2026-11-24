import json
import os
import sys
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from coordinator.extension import available
from coordinator.worker_hooks import codex_trust_override, install


WRITES = available('docs')


class WorkerHooksTests(unittest.TestCase):
    def test_install_preserves_existing_hooks_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for provider, filename, matcher in (('claude', 'settings.json', 'Bash|Read|Grep' + ('|Write' if WRITES else '')),
                                                ('codex', 'hooks.json', 'Bash|exec_command|unified_exec|shell|local_shell|shell_command'
                                                 + ('|apply_patch' if WRITES else ''))):
                with self.subTest(provider=provider):
                    home = root / provider
                    home.mkdir()
                    path = home / filename
                    existing = {'matcher': 'Bash', 'hooks': [{'type': 'command', 'command': 'existing-guard'}]}
                    path.write_text(json.dumps({'other': 1, 'hooks': {'PreToolUse': [existing], 'Stop': []}}))
                    install(provider, home, root)
                    first = path.read_bytes()
                    install(provider, home, root)
                    data = json.loads(path.read_text())
                    self.assertEqual(path.read_bytes(), first)
                    self.assertEqual(data['other'], 1)
                    self.assertEqual(data['hooks']['Stop'], [])
                    self.assertEqual(data['hooks']['PreToolUse'][0], existing)
                    self.assertEqual(data['hooks']['PreToolUse'][1]['matcher'], matcher)
                    self.assertIn('secret_guard.py', data['hooks']['PreToolUse'][1]['hooks'][0]['command'])

    def test_update_replaces_only_the_previous_torii_command(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch('coordinator.worker_hooks.__file__', str(root / 'old' / 'worker_hooks.py')):
                install('claude', root, root, root)
            with patch('coordinator.worker_hooks.__file__', str(root / 'new' / 'worker_hooks.py')):
                install('claude', root, root, root)
            hooks = json.loads((root / 'settings.json').read_text())['hooks']['PreToolUse']
            self.assertEqual(len(hooks), 1)
            self.assertIn('/new/secret_guard.py', hooks[0]['hooks'][0]['command'])

    def test_matcher_change_updates_the_one_torii_entry(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old = {'matcher': 'Bash|Read|Grep', 'hooks': [{'type': 'command', 'command': 'python3 old --torii-worker-hook'}]}
            other = {'matcher': 'Bash', 'hooks': [{'type': 'command', 'command': 'existing-guard'}]}
            (root / 'settings.json').write_text(json.dumps({'hooks': {'PreToolUse': [old, other, dict(old)]}}))
            _, matcher, command = install('claude', root, root, root)
            hooks = json.loads((root / 'settings.json').read_text())['hooks']['PreToolUse']
            self.assertEqual(hooks, [{'matcher': matcher, 'hooks': [{'type': 'command', 'command': command}]}, other])
            self.assertEqual(matcher, 'Bash|Read|Grep' + ('|Write' if WRITES else ''))

    def test_external_edit_is_not_overwritten_when_detected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / 'settings.json'
            path.write_text('{}')
            original = json.dump
            def edit_during_write(*args, **kwargs):
                original(*args, **kwargs)
                path.write_text('{"other": true}')
            with patch('coordinator.worker_hooks.json.dump', side_effect=edit_during_write):
                with self.assertRaises(RuntimeError):
                    install('claude', root, root, root)
            self.assertEqual(json.loads(path.read_text()), {'other': True})
            self.assertEqual(list(root.glob('.torii-hook-*')), [])

    def test_invalid_settings_are_not_replaced(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / 'settings.json'
            path.write_text('{invalid')
            with self.assertRaises(ValueError):
                install('claude', root, root)
            self.assertEqual(path.read_text(), '{invalid')

    def test_symlink_to_shared_settings_is_not_modified(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            home = root / 'account'
            home.mkdir()
            shared = root / 'shared.json'
            shared.write_text('{}')
            link = home / 'hooks.json'
            link.symlink_to(shared)
            install('codex', home, root)
            self.assertFalse(link.is_symlink())
            self.assertEqual(shared.read_text(), '{}')
            self.assertEqual(len(json.loads(link.read_text())['hooks']['PreToolUse']), 1)


class CodexHookTrustTests(unittest.IsolatedAsyncioTestCase):
    async def test_trusts_only_the_installed_hook(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path, matcher, command = install('codex', root, root, root)
            key = str(path) + ':pre_tool_use:1:0'
            digest = 'sha256:' + 'a' * 64
            guard = {'key': key, 'currentHash': digest, 'eventName': 'preToolUse',
                     'handlerType': 'command', 'command': command, 'matcher': matcher,
                     'sourcePath': str(path), 'enabled': True}
            other = {**guard, 'key': str(path) + ':pre_tool_use:0:0', 'command': 'untrusted-hook'}
            server = AsyncMock()
            server.request.return_value = {'data': [{'hooks': [other, guard]}]}
            with patch('coordinator.codex_accounts.AppServer', return_value=server):
                override = await codex_trust_override('codex', {}, root, (path, matcher, command))
            self.assertEqual(override, 'hooks.state={' + json.dumps(key) + '={trusted_hash=' + json.dumps(digest) + '}}')
            self.assertNotIn('untrusted-hook', override)
            server.stop.assert_awaited_once()

    async def test_trust_check_is_cached_for_unchanged_account_and_binary(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            installed = install('codex', root, root, root)
            path, matcher, command = installed
            server = AsyncMock()
            server.request.return_value = {'data': [{'hooks': [{'key': 'key', 'currentHash': 'sha256:' + 'a' * 64,
                'eventName': 'preToolUse', 'handlerType': 'command', 'command': command, 'matcher': matcher,
                'sourcePath': str(path), 'enabled': True}]}]}
            with patch('coordinator.codex_accounts.AppServer', return_value=server):
                await codex_trust_override('codex', {}, root, installed)
                await codex_trust_override('codex', {}, root / 'another-worktree', installed)
            server.start.assert_awaited_once()

    async def test_invalid_trust_response_is_cached_temporarily(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            installed = install('codex', root, root, root)
            server = AsyncMock()
            server.request.return_value = {'data': [{'hooks': []}]}
            with patch('coordinator.codex_accounts.AppServer', return_value=server):
                for cwd in (root, root / 'another-worktree'):
                    with self.assertRaises(RuntimeError):
                        await codex_trust_override('codex', {}, cwd, installed)
            server.start.assert_awaited_once()

    async def test_disabled_or_missing_guard_refuses_start(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            installed = install('codex', root, root, root)
            server = AsyncMock()
            server.request.return_value = {'data': [{'hooks': []}]}
            with patch('coordinator.codex_accounts.AppServer', return_value=server):
                with self.assertRaises(RuntimeError):
                    await codex_trust_override('codex', {}, root, installed)


FAKE_CODEX = r'''
import hashlib, json, os, re, sys
from pathlib import Path
settings = sys.argv[1:sys.argv.index('app-server')]
state = {}
for value in settings:
    if value.startswith('hooks.state='):
        for key, digest in re.findall(r'("(?:[^"\\]|\\.)*")=\{trusted_hash="(sha256:[0-9a-f]{64})"\}', value):
            state[json.loads(key)] = digest
path = (Path(os.environ['CODEX_HOME']) / 'hooks.json').resolve()
for line in sys.stdin:
    message = json.loads(line)
    if 'id' not in message:
        continue
    result = {}
    if message['method'] == 'hooks/list':
        hooks = []
        for group_index, group in enumerate(json.loads(path.read_text())['hooks']['PreToolUse']):
            for hook_index, hook in enumerate(group['hooks']):
                key = '%s:pre_tool_use:%d:%d' % (path, group_index, hook_index)
                digest = 'sha256:' + hashlib.sha256(hook['command'].encode()).hexdigest()
                hooks.append({'key': key, 'currentHash': digest, 'eventName': 'preToolUse', 'handlerType': 'command',
                              'command': hook['command'], 'matcher': group['matcher'], 'sourcePath': str(path),
                              'enabled': True, 'trustStatus': 'trusted' if state.get(key) == digest else 'untrusted'})
        result = {'data': [{'cwd': message['params']['cwds'][0], 'hooks': hooks}]}
    print(json.dumps({'id': message['id'], 'result': result}), flush=True)
'''


class CodexHookIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_app_server_round_trip_trusts_only_guard(self):
        from coordinator.codex_accounts import AppServer
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / 'bin' / 'codex'
            binary.parent.mkdir()
            binary.write_text('#!' + sys.executable + ' -I\n' + FAKE_CODEX)
            binary.chmod(0o700)
            home = root / 'codex'
            home.mkdir()
            (home / 'hooks.json').write_text(json.dumps({'hooks': {'PreToolUse': [
                {'matcher': 'Bash', 'hooks': [{'type': 'command', 'command': 'echo other'}]}]}}))
            installed = install('codex', home, root, root)
            env = {'HOME': str(root), 'CODEX_HOME': str(home), 'TMPDIR': str(root), 'PATH': str(binary.parent)}
            override = await codex_trust_override(str(binary), env, root, installed)
            server = AppServer(str(binary), env)
            try:
                await server.start(settings=('-c', override), cwd=str(root), experimental=True)
                response = await server.request('hooks/list', {'cwds': [str(root)]})
            finally:
                await server.stop()
            hooks = response['data'][0]['hooks']
            guard = next(entry for entry in hooks if entry.get('command') == installed[2])
            other = next(entry for entry in hooks if entry.get('command') == 'echo other')
            self.assertEqual(guard['trustStatus'], 'trusted')
            self.assertEqual(other['trustStatus'], 'untrusted')
