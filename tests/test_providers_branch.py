"""Adapters without provider branches cannot borrow another provider's launch or result."""

import ast
import asyncio
import inspect
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, patch

from coordinator import providers
from coordinator.providers import ProviderRunner
from coordinator.store import Store


class ProviderBranchTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        self.runner = ProviderRunner(self.store.directory, binaries={'zz': 'dummy-adapter'},
                                     opencode_store=self.store)
        self.adapter = SimpleNamespace(argv=Mock(return_value=['dummy-adapter', 'acp']))
        adapters = patch.dict(providers.ADAPTERS, {'zz': Mock(return_value=self.adapter)})
        adapters.start()
        self.addCleanup(adapters.stop)

    def test_unknown_environment_never_reads_codex_or_service_environment(self):
        """Kills m0-env-fallthrough, including an assigned Codex broker home."""
        self.runner.codex_broker = Mock()
        with patch('coordinator.providers.child_environment') as child:
            for provider in ('zz', 'opencode'):
                for alias in (None, 'dummy-account'):
                    with self.subTest(provider=provider, account_alias=alias):
                        with self.assertRaisesRegex(ValueError, '^Unknown provider$'):
                            self.runner._environment(provider, alias)
            child.assert_not_called()
            self.runner.codex_broker.environment.assert_not_called()

    def test_unknown_argv_branch_raises(self):
        """Kills m0-else-codex at the branch behind the unchanged admission gate."""
        tree = ast.parse(inspect.getsource(providers))
        branch = next(node for node in ast.walk(tree)
                      if isinstance(node, ast.If) and isinstance(node.body[0], ast.Assign)
                      and any(isinstance(target, ast.Name) and target.id == 'command'
                              for target in node.body[0].targets))
        code = compile(ast.Module(body=[branch], type_ignores=[]), providers.__file__, 'exec')
        scope = dict(provider='zz', acp_factory=None, command=['dummy-adapter'],
                     effort='medium', control=None, secret_names=(), fresh=True, self=self.runner)
        with self.assertRaisesRegex(ValueError, '^Unknown provider$'):
            exec(code, providers.__dict__.copy(), scope)

    async def test_unknown_adapter_is_refused_without_a_host_or_environment(self):
        """A registered adapter alone must not receive a Codex home or inherited environment."""
        self.runner.codex_broker = Mock()
        self.runner.codex_broker.environment.return_value = {'CODEX_HOME': '/dummy-codex-home'}
        with patch('coordinator.providers.HostClient.launch', AsyncMock(side_effect=AssertionError('host launched'))) as launch, \
                patch('coordinator.providers.child_environment') as child:
            with self.assertRaisesRegex(ValueError, '^Unknown provider$'):
                await self.runner.run('zz', 'dummy prompt', self.root, None,
                                      fresh=True, account_alias='dummy-account')
            launch.assert_not_awaited()
            child.assert_not_called()
            self.runner.codex_broker.environment.assert_not_called()
        self.assertFalse((self.store.directory / 'hosts').exists())

    async def test_unknown_environment_branch_refuses_even_with_a_patched_environment(self):
        """Kills m0-env-else without relying on _environment's own guard."""
        with patch.object(self.runner, '_environment', return_value={
                'CODEX_HOME': '/dummy-codex-home', 'DUMMY_SERVICE_VALUE': 'dummy-only'}) as environment, \
                patch('coordinator.providers.HostClient.launch', AsyncMock(side_effect=AssertionError('host launched'))) as launch:
            with self.assertRaisesRegex(ValueError, '^Unknown provider$'):
                await self.runner.run('zz', 'dummy prompt', self.root, None, fresh=True)
            environment.assert_not_called()
            launch.assert_not_awaited()
        self.assertFalse((self.store.directory / 'hosts').exists())

    async def test_unknown_successful_adapter_result_raises_without_opencode_finish(self):
        """Kills m0-result-else after a successful attached protocol completes."""
        directory = self.store.directory / 'hosts' / 'dummy-host'
        directory.mkdir(parents=True)
        spec = dict(env={'XDG_CONFIG_HOME': str(self.root / 'config'),
                         'XDG_DATA_HOME': str(self.root / 'data')},
                    account='dummy-account', initial_payload='dummy prompt', fresh=True,
                    model=None, cwd=str(self.root))
        (directory / 'spec.json').write_text(json.dumps(spec))
        process = Mock(child_pid=123, returncode=0, directory=directory)
        process.connect = AsyncMock()
        process.next_event = AsyncMock(return_value=None)
        process.wait = AsyncMock(return_value=0)
        process.stop = AsyncMock()
        process.history.return_value = []
        closed = asyncio.get_running_loop().create_future()
        closed.set_result(None)
        protocol = Mock(text='dummy success', spend=None, failure=None,
                        outcome='succeeded', input_closed=closed)
        protocol.start = AsyncMock()

        def successful_protocol(adapter, host, control, session_id, observe_session, **options):
            observe_session('dummy-native-session')
            return protocol

        with patch('coordinator.providers.HostClient.attach', return_value=process), \
                patch('coordinator.providers.HostClient.launch', AsyncMock()) as launch, \
                patch.object(self.runner, '_environment') as environment, \
                patch('coordinator.providers.native_ids.valid', return_value=True), \
                patch('coordinator.providers.AcpProtocol', side_effect=successful_protocol), \
                patch('coordinator.providers.opencode_runtime.finish') as finish:
            with self.assertRaisesRegex(ValueError, '^Unknown provider$'):
                await self.runner.run('zz', 'dummy prompt', self.root, None,
                                      fresh=True, attach=True, host_id='dummy-host')
            finish.assert_not_called()
            launch.assert_not_awaited()
            environment.assert_not_called()
        protocol.start.assert_awaited_once()
        process.wait.assert_awaited_once()
        process.detach.assert_called_once()
