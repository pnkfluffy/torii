"""Native checks of the public snapshot's Claude sign-in boundary."""

import builtins
import os
from pathlib import Path
import subprocess
import unittest
from unittest.mock import AsyncMock, patch

from coordinator.host import HostClient
from coordinator.native_protocol import RunControl
from coordinator.tldr import summarize
from tests import test_session as session_tests


INHERITED_CLAUDE_CREDENTIALS = ('ANTHROPIC_API_KEY', 'ANTHROPIC_AUTH_TOKEN', 'ANTHROPIC_PROFILE',
                               'CLAUDE_SECURESTORAGE_CONFIG_DIR', 'CLAUDE_CODE_OAUTH_TOKEN',
                               'CLAUDE_CODE_OAUTH_REFRESH_TOKEN', 'ANTHROPIC_FOUNDRY_API_KEY',
                               'AWS_BEARER_TOKEN_BEDROCK', 'ANTHROPIC_CUSTOM_HEADERS',
                               'CLAUDE_CODE_CLIENT_KEY_PASSPHRASE')


class PublicLaunchBoundaryTests(session_tests.CoordinatorSessionTestsSupport, unittest.IsolatedAsyncioTestCase):
    async def test_public_mode_puts_no_token_in_any_launch_env(self):
        self.account_service()
        forbidden = {'CLAUDE_CODE_OAUTH_TOKEN', 'CLAUDE_CODE_OAUTH_REFRESH_TOKEN', 'ANTHROPIC_BASE_URL',
                     'ANTHROPIC_AUTH_TOKEN', 'ANTHROPIC_API_KEY', 'CLAUDE_CODE_SDK_HAS_OAUTH_REFRESH', 'NODE_EXTRA_CA_CERTS'}
        forbidden.update(INHERITED_CLAUDE_CREDENTIALS)
        inherited = dict.fromkeys(INHERITED_CLAUDE_CREDENTIALS, 'synthetic-inherited')
        inherited['TORII_ENV_KEEP'] = 'kept'
        original_launch = HostClient.launch
        original_open = builtins.open
        original_read = Path.read_text
        original_popen = subprocess.Popen

        def check_file(path):
            if isinstance(path, (str, Path)) and Path(path).name == '.credentials.json':
                self.fail('Public mode opened a credential file')

        def open_file(path, *args, **kwargs):
            check_file(path)
            return original_open(path, *args, **kwargs)

        def read_file(path, *args, **kwargs):
            check_file(path)
            return original_read(path, *args, **kwargs)

        def popen(argv, *args, **kwargs):
            if isinstance(argv, (list, tuple)) and Path(argv[0]).name == 'security':
                self.fail('Public mode called security')
            return original_popen(argv, *args, **kwargs)

        with patch.dict(os.environ, inherited), \
                patch.object(HostClient, 'launch', AsyncMock(side_effect=original_launch)) as launches, \
                patch('builtins.open', side_effect=open_file), patch.object(Path, 'read_text', read_file), \
                patch('subprocess.Popen', side_effect=popen):
            parent = await self.start(topic='home')
            worker = await self.runner.run('claude', 'hello', self.root, None, fresh=True, account_alias='a', control=RunControl())
            self.assertTrue(worker.success, worker.error)
            self.store.message_save('home', 'owner', 'Summarize', telegram_message=1)
            row = self.store.enqueue_report('home', 'One update')
            self.store.delivered(row, 2)
            with patch('coordinator.tldr._native', AsyncMock(return_value={'result': 'Done'})) as native:
                self.assertEqual(await summarize(self.store, 'home', 3, parent.router,
                                                self.runner.extension, str(self.binary)), 'Done')
            self.assertEqual(len(launches.await_args_list), 2)
            for name, call in zip(('parent', 'worker'), launches.await_args_list):
                with self.subTest(launch=name):
                    self.assertEqual(call.args[1]['env']['TORII_ENV_KEEP'], 'kept')
                    self.assertEqual(forbidden & set(call.args[1]['env']), set())
                    self.assertEqual(call.kwargs['private']['env'], {})
                    self.assertEqual(call.args[1]['env'], parent.router.environment('a'))
            with self.subTest(launch='tldr'):
                self.assertEqual(forbidden & set(native.await_args.args[1]), set())
                self.assertEqual(native.await_args.args[1]['TORII_ENV_KEEP'], 'kept')
                self.assertEqual(native.await_args.args[1], parent.router.environment('a'))

    async def test_account_utilities_drop_inherited_claude_credentials(self):
        from coordinator.account_status import collect_status
        from coordinator.signin import SignIns

        profile = self.root / 'profile'
        profile.mkdir()
        inherited = dict.fromkeys(INHERITED_CLAUDE_CREDENTIALS, 'synthetic-inherited')
        inherited['TORII_ENV_KEEP'] = 'kept'
        signin = SignIns(self.store, str(self.binary))
        calls = []

        async def unavailable(*args, **kwargs):
            calls.append(kwargs['env'])
            raise FileNotFoundError('Synthetic missing binary')

        with patch.dict(os.environ, inherited), \
                patch('asyncio.create_subprocess_exec', side_effect=unavailable):
            status = await collect_status({'config_dir': str(profile)}, str(self.binary))
            self.assertEqual(status['error'], 'cli_unavailable')
            self.assertIsNone(await signin.identity(profile))
            with self.assertRaises(FileNotFoundError):
                await signin.attempt(profile, 1)
        self.assertEqual(len(calls), 3)
        for name, env in zip(('status', 'identity', 'signin'), calls):
            with self.subTest(launch=name):
                self.assertEqual(env['CLAUDE_CONFIG_DIR'], str(profile))
                self.assertEqual(env['TORII_ENV_KEEP'], 'kept')
                self.assertEqual(set(INHERITED_CLAUDE_CREDENTIALS) & set(env), set())
