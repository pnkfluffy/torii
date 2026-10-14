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
                                                   'secrets': 'secret_names?'})
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
