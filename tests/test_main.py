import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1]


class McpImportTests(unittest.TestCase):
    def check_project_scripts(self, relative):
        with tempfile.TemporaryDirectory() as temporary:
            project = Path(temporary)
            scripts = project / relative
            scripts.parent.mkdir(parents=True, exist_ok=True)
            scripts.write_text("from pathlib import Path\nPath('scripts-executed').write_text('fixture')\n")
            result = subprocess.run(
                [sys.executable, '-c', 'import coordinator.mcp'], cwd=project,
                env=dict(os.environ, PYTHONPATH=str(ROOT)), capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertFalse((project / 'scripts-executed').exists())

    def test_mcp_import_ignores_project_scripts_module(self):
        self.check_project_scripts('scripts.py')

    def test_mcp_import_ignores_project_scripts_package(self):
        self.check_project_scripts('scripts/__init__.py')


class RestartTests(unittest.TestCase):
    def test_watchdog_starts_a_daemon_and_exits_after_30_seconds(self):
        from coordinator.__main__ import restart_watchdog
        events = []
        with patch('coordinator.__main__.threading.Thread') as thread, \
                patch('coordinator.__main__.time.sleep', side_effect=lambda value: events.append(('sleep', value))), \
                patch('coordinator.__main__.os._exit', side_effect=lambda code: events.append(('exit', code))):
            restart_watchdog()
            self.assertEqual(thread.call_args.kwargs['name'], 'restart-watchdog')
            self.assertTrue(thread.call_args.kwargs['daemon'])
            thread.return_value.start.assert_called_once_with()
            self.assertEqual(events, [])
            thread.call_args.kwargs['target']()
        self.assertEqual(events, [('sleep', 30), ('exit', 1)])

    def test_restart_closes_store_and_logging_before_executor_shutdown(self):
        from coordinator.__main__ import main
        release = threading.Event()
        completed = threading.Event()
        events = []

        async def serve(args, store):
            loop = asyncio.get_running_loop()
            started = asyncio.Event()

            def blocking():
                loop.call_soon_threadsafe(started.set)
                release.wait(2)
                completed.set()

            loop.run_in_executor(None, blocking)
            await asyncio.wait_for(started.wait(), 1)
            return True

        class Exited(BaseException):
            pass

        def exit(code):
            events.append(('exit', code))
            self.assertFalse(completed.is_set())
            release.set()
            raise Exited()

        args = SimpleNamespace(command='serve', state_dir=ROOT, token_file=None)
        store = Mock()
        store.close.side_effect = lambda: events.append('store')
        try:
            with patch('coordinator.__main__.parser') as parser, \
                    patch('coordinator.__main__.Store', return_value=store), \
                    patch('coordinator.__main__.serve', serve), \
                    patch('coordinator.__main__.os.open', return_value=42), \
                    patch('coordinator.__main__.fcntl.flock'), \
                    patch('coordinator.__main__.os.close', side_effect=lambda fd: events.append(('lock', fd))), \
                    patch('logging.shutdown', side_effect=lambda: events.append('logging')), \
                    patch('coordinator.__main__.os._exit', side_effect=exit):
                parser.return_value.parse_args.return_value = args
                with self.assertRaises(Exited):
                    main()
            self.assertEqual(events[:4], [('lock', 42), 'store', 'logging', ('exit', 0)])
        finally:
            release.set()


class CommandLineTests(unittest.TestCase):
    """The CLI runs the same operations, against the database the service shares."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.state = Path(self.temp.name) / 'state'

    def tearDown(self):
        self.temp.cleanup()

    def run_ctl(self, *arguments):
        completed = subprocess.run(
            [sys.executable, '-m', 'coordinator', '--state-dir', str(self.state),
             '--token-file', str(Path(self.temp.name) / 'no-token'), 'ctl'] + list(arguments),
            cwd=ROOT, capture_output=True, text=True, timeout=120,
            env=dict(os.environ, PYTHONPYCACHEPREFIX=str(Path(self.temp.name) / 'bytecode')))
        return completed.returncode, completed.stdout

    def test_list_and_describe_read_the_manifest_without_writing(self):
        from coordinator import control_api
        code, out = self.run_ctl('list')
        self.assertEqual(code, 0)
        self.assertEqual([op['id'] for op in json.loads(out)], [op.id for op in control_api.OPS])
        code, out = self.run_ctl('list', '--kind', 'read')
        self.assertEqual(code, 0)
        self.assertTrue(all(op['kind'] == 'read' for op in json.loads(out)))
        code, out = self.run_ctl('describe', 'tasks.create')
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)['params'], {'title': 'str', 'message': 'int?', 'topic': 'topic?',
                                                   'cross_topic': 'bool?', 'notes': 'text?',
                                                   'secrets': 'secret_names?', 'finish': 'finish?', 'parent': 'int?'})
        code, out = self.run_ctl('describe', 'tasks.update')
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)['params'], {'task': 'int', 'title': 'str?', 'status': 'status?',
                                                   'notes': 'text?', 'note': 'text?', 'secrets': 'secret_names?',
                                                   'stage': 'stage?', 'pr': 'str?', 'finish': 'finish?'})
        code, out = self.run_ctl('describe', 'no.such.op')
        self.assertEqual(code, 2)
        self.assertFalse(json.loads(out)['ok'])

    def test_a_write_takes_key_value_pairs(self):
        code, out = self.run_ctl('call', 'model.worker', 'model=opus')
        self.assertEqual(code, 0)
        reply = json.loads(out)
        self.assertEqual((reply['ok'], reply['state'], reply['kind']), (True, 'done', 'write'))
        self.assertIn('worker model: opus', reply['text'])
        code, out = self.run_ctl('call', 'settings.get')
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)['data']['normal_model'], 'opus')

    def test_removed_operation_is_unknown(self):
        code, out = self.run_ctl('call', 'telegram.token')
        self.assertEqual(code, 3)
        self.assertEqual(json.loads(out)['state'], 'refused')

    def test_a_bad_parameter_exits_non_zero_without_changing_a_setting(self):
        code, out = self.run_ctl('call', 'model.worker', 'model=a/b')
        self.assertEqual(code, 3)
        self.assertIn('Send a model id', json.loads(out)['text'])
        code, out = self.run_ctl('call', 'model.worker', 'justthekey')
        self.assertEqual(code, 2)
        self.assertIn('key=value', json.loads(out)['error'])
        code, out = self.run_ctl('call', 'settings.get')
        self.assertEqual(json.loads(out)['data']['normal_model'], 'opus')
        self.assertNotIn('planning_model', json.loads(out)['data'])

    def test_service_operation_needs_running_service(self):
        code, out = self.run_ctl('call', 'service.restart', 'reason=update')
        self.assertEqual(code, 3)
        self.assertIn('needs the running service', json.loads(out)['text'])



class HomeDirTests(unittest.TestCase):
    def test_home_is_the_code_checkout_unless_torii_home_names_a_folder(self):
        from unittest.mock import patch
        from coordinator.__main__ import home_dir
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('TORII_HOME', None)
            self.assertEqual(home_dir(), ROOT)
        with tempfile.TemporaryDirectory() as folder:
            with patch.dict(os.environ, {'TORII_HOME': folder}):
                self.assertEqual(home_dir(), Path(folder).resolve())
            for bad in ('relative/path', str(Path(folder) / 'missing')):
                with patch.dict(os.environ, {'TORII_HOME': bad}), self.assertRaises(SystemExit):
                    home_dir()

if __name__ == '__main__':
    unittest.main()


class PairCommandTests(unittest.TestCase):
    def test_group_link_and_hidden_alias_use_local_identity(self):
        from contextlib import redirect_stdout
        import io
        from types import SimpleNamespace
        from coordinator.__main__ import pair, parser
        from coordinator.store import Store
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch('pathlib.Path.home', return_value=root):
                store = Store(root / 'state')
                with store.db:
                    store.put('bot_username', 'test_bot')
                args = parser().parse_args(['--state-dir', str(root / 'state'), 'pair'])
                with redirect_stdout(io.StringIO()) as out:
                    self.assertEqual(pair(args, store), 0)
                self.assertIn('https://t.me/test_bot?startgroup=', out.getvalue())
                self.assertNotIn('/pair ', out.getvalue())
                with redirect_stdout(io.StringIO()) as out:
                    self.assertEqual(pair(SimpleNamespace(group=True, replace=False), store), 0)
                self.assertEqual(store.get('execution'), 'pairing')
                self.assertIn('https://t.me/test_bot?startgroup=', out.getvalue())
                self.assertIn('&admin=manage_topics+delete_messages+pin_messages', out.getvalue())
                self.assertNotIn('/pair ', out.getvalue())
                store.close()


class ServeTestsSupport:
    def setUp(self):
        from coordinator.store import Store
        from coordinator import log
        self.temporary = tempfile.TemporaryDirectory(prefix='service-startup-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.state = self.root / 'state'
        self.store = Store(self.state)
        self.addCleanup(self.store.close)
        self.addCleanup(log.reset)

    async def start(self, before_runner):
        from contextlib import ExitStack
        from unittest.mock import AsyncMock
        from coordinator import __main__ as cli
        args = SimpleNamespace(state_dir=self.state, token_file=self.root / 'unused',
                               pair_only=False, coordinator_model='fixture')
        service = Mock(run=AsyncMock(), restart_reason=None)
        with ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, {'HOME': str(self.root), 'PATH': os.defpath}))
            stack.enter_context(patch.object(cli, 'home_dir', return_value=self.root))
            stack.enter_context(patch('coordinator.host_os.prepare_trash'))
            stack.enter_context(patch('coordinator.log.running_commit', return_value='fixture'))
            stack.enter_context(patch('coordinator.accounts.discover_accounts'))
            stack.enter_context(patch('coordinator.codex_accounts.discover_codex_accounts'))
            stack.enter_context(patch('coordinator.isolation.codex_install_problem', return_value=None))
            stack.enter_context(patch.object(cli, 'Telegram', return_value=Mock(
                call=AsyncMock(side_effect=[{}, {'username': 'fixture_bot', 'id': 42}]))))
            stack.enter_context(patch('coordinator.providers.ProviderRunner', side_effect=before_runner))
            stack.enter_context(patch('coordinator.service.Service', return_value=service))
            stack.enter_context(patch('coordinator.vault.vault_from_environment', return_value=None))
            stack.enter_context(patch.object(asyncio.get_running_loop(), 'add_signal_handler'))
            self.assertFalse(await cli.serve(args, self.store))
        service.run.assert_awaited_once()


class OptionalStartupTests(ServeTestsSupport, unittest.IsolatedAsyncioTestCase):
    async def test_serve_with_native_extension_keeps_path_and_creates_no_wrapper(self):
        from coordinator import extension

        def runner(*args, **kwargs):
            self.assertEqual(os.environ['PATH'], os.defpath)
            self.assertFalse((self.state / 'bin').exists())
            return Mock()

        with patch('coordinator.extension.active', return_value=extension.Native()):
            await self.start(runner)
        self.assertEqual(self.store.db.execute(
            "SELECT count(*) FROM problems").fetchone()[0], 0)
