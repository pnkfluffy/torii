import asyncio
import base64
from contextlib import redirect_stdout
import io
import json
import logging
import os
from pathlib import Path
import pty
import re
import select
import signal
import subprocess
import sys
import tempfile
import termios
import threading
import unittest
from unittest.mock import AsyncMock, Mock, patch

from coordinator import setup_terminal as terminal
from coordinator.store import Store
from coordinator.telegram import TelegramError
from tests.test_setup_installer import INSTALLER, no_external


FAKE_TOKEN = '123456789:' + 'A' * 35
MANUAL_TOKEN = '987654321:' + 'B' * 35
CHILD = '''import json, sys, time
settings = json.loads(sys.stdin.readline())
for delay, frame in settings:
    time.sleep(delay)
    sys.stdout.write(frame)
    sys.stdout.flush()
time.sleep(30)
'''


class TokenTerminalTests(unittest.TestCase):
    def setUp(self):
        self.master, self.slave = pty.openpty()
        self.saved = termios.tcgetattr(self.slave)
        self.real_open = open
        self.real_popen = subprocess.Popen
        self.processes = []
        self.threads = []
        self.launches = []
        self.frames = [(0, 'READY\n'), (0.01, 'TOKEN ' + FAKE_TOKEN + '\n')]
        self.on_show = None
        self.output = io.StringIO()
        self.logs = io.StringIO()
        self.handler = logging.StreamHandler(self.logs)
        logging.getLogger().addHandler(self.handler)
        self.addCleanup(logging.getLogger().removeHandler, self.handler)
        self.patches = [patch('builtins.open', self.open_tty),
                        patch('coordinator.setup_terminal.subprocess.Popen', self.launch),
                        patch('coordinator.setup_terminal.open_url', self.open_url),
                        patch('coordinator.setup_terminal.print_link', side_effect=lambda url: print(url)),
                        patch.object(terminal, 'START_TIMEOUT', 0.2), patch.object(terminal, 'READ_TIMEOUT', 0.2)]
        for handle in self.patches:
            handle.start()
        self.addCleanup(self.cleanup)

    def cleanup(self):
        terminal.restore_input()
        for handle in reversed(self.patches):
            handle.stop()
        for process in self.processes:
            if process.poll() is None:
                process.kill()
                process.wait()
            if process.stdout:
                process.stdout.close()
        for thread in self.threads:
            thread.join(1)
        os.close(self.master)
        os.close(self.slave)

    def open_tty(self, path, *args, **kwargs):
        if path == '/dev/tty':
            return os.fdopen(os.dup(self.slave), 'r+b', buffering=0)
        return self.real_open(path, *args, **kwargs)

    def launch(self, argv, **kwargs):
        self.launches.append((argv, kwargs))
        self.assertEqual(argv, ['/usr/bin/osascript', '-l', 'JavaScript', str(terminal.HELPERS / 'setup-clipboard.js')])
        self.assertEqual(kwargs, {'stdout': subprocess.PIPE, 'stderr': subprocess.DEVNULL})
        self.assertFalse(termios.tcgetattr(self.slave)[3] & (termios.ECHO | termios.ECHONL))
        process = self.real_popen([sys.executable, '-c', CHILD], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  stderr=subprocess.DEVNULL)
        process.stdin.write((json.dumps(self.frames) + '\n').encode())
        process.stdin.close()
        self.processes.append(process)
        return process

    def open_url(self, url):
        self.assertEqual(url, terminal.BOTFATHER)
        if self.on_show:
            self.on_show()

    def paste(self, text=MANUAL_TOKEN):
        os.write(self.master, (text + '\n').encode())

    def later(self, action, delay=0.35):
        thread = threading.Timer(delay, action)
        self.threads.append(thread)
        thread.start()

    def candidate(self):
        try:
            with redirect_stdout(self.output):
                result = terminal.obtain_candidate()
            self.assertFalse(termios.tcgetattr(self.slave)[3] & termios.ECHO)
        finally:
            terminal.restore_input()
        self.assertEqual(termios.tcgetattr(self.slave), self.saved)
        self.assertTrue(all(process.poll() is not None for process in self.processes))
        for value in (FAKE_TOKEN, MANUAL_TOKEN):
            self.assertNotIn(value, self.output.getvalue())
            self.assertNotIn(value, self.logs.getvalue())
            self.assertNotIn(value, repr(self.launches))
        return result

    def test_fake_helper_pickup_is_hidden_and_reaped(self):
        self.assertEqual(self.candidate(), FAKE_TOKEN)
        self.assertIn(terminal.BOTFATHER, self.output.getvalue())
        self.assertIn('No paste is needed.', self.output.getvalue())

    def test_retry_does_not_reopen_botfather_or_reprint_qr(self):
        self.assertEqual(self.candidate(), FAKE_TOKEN)
        with patch.object(terminal, 'open_url') as opened, patch.object(terminal, 'print_link') as shown:
            self.frames = [(0, 'READY\n')]
            self.later(lambda: self.paste(''), 0.05)
            try:
                with redirect_stdout(self.output):
                    self.assertEqual(terminal.obtain_candidate(show_screen=False), '')
            finally:
                terminal.restore_input()
        opened.assert_not_called()
        shown.assert_not_called()

    def test_ask_permission_can_outlast_read_timeout(self):
        self.frames = [(0, 'READY\nASK\n'), (0.35, 'TOKEN ' + FAKE_TOKEN + '\n')]
        self.assertEqual(self.candidate(), FAKE_TOKEN)
        self.assertNotIn(terminal.PASTE, self.output.getvalue())

    def test_token_ctrl_c_through_real_asyncio_run_is_clean(self):
        self.frames = [(0, 'READY\n')]
        self.on_show = lambda: signal.raise_signal(signal.SIGINT)
        globals_ = INSTALLER['setup'].__globals__
        async def setup(**kwargs):
            terminal.obtain_candidate()
            await asyncio.sleep(0)
        with patch.dict(globals_, setup=setup, require_tty=lambda: True), redirect_stdout(self.output):
            self.assertEqual(INSTALLER['main']([]), 1)
        self.assertIn('Cancelled.', self.output.getvalue())
        self.assertEqual(termios.tcgetattr(self.slave), self.saved)

    def test_late_paste_is_hidden_through_check_and_flushed_before_login(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            store = Store(root / 'state')
            globals_ = INSTALLER['setup'].__globals__
            async def check(path):
                self.assertFalse(termios.tcgetattr(self.slave)[3] & termios.ECHO)
                self.paste(FAKE_TOKEN)
                self.assertFalse(select.select([self.master], [], [], 0.05)[0])
                return None, {'username': 'test_bot'}
            async def launch(*args, **kwargs):
                self.assertEqual(termios.tcgetattr(self.slave), self.saved)
                self.assertFalse(select.select([self.slave], [], [], 0)[0])
                return Mock(returncode=1, wait=AsyncMock(return_value=1))
            async def run():
                await INSTALLER['obtain_token'](root / 'config', store)
                self.assertFalse(termios.tcgetattr(self.slave)[3] & termios.ECHO)
                await INSTALLER['connect_claude'](store)
            try:
                with patch.object(terminal, 'interactive_mac', return_value=True), \
                        patch.dict(globals_, check_bot=check), patch('asyncio.create_subprocess_exec', launch), \
                        patch('pathlib.Path.home', return_value=root), \
                        redirect_stdout(self.output):
                    asyncio.run(run())
            finally:
                store.close()
            self.assertEqual(termios.tcgetattr(self.slave), self.saved)

    def screen(self):
        data = b''
        while select.select([self.master], [], [], 0.05)[0]:
            data += os.read(self.master, 4096)
        return data

    def test_gate_discards_pending_token_and_flushes_remaining_lines(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = root / '.config/telegram-agent-coordinator'
            config.mkdir(parents=True)
            (config / 'bot-token').write_text(FAKE_TOKEN)
            store = Store(root / 'state')
            login = AsyncMock(return_value=False)
            async def launch(*args):
                self.assertEqual(termios.tcgetattr(self.slave), self.saved)
                self.assertFalse(select.select([self.slave], [], [], 0)[0])
                return await login(*args)
            terminal.hide_input()
            self.paste(FAKE_TOKEN)
            self.paste('extra pending line')
            try:
                with patch.object(terminal, 'interactive_mac', return_value=True), \
                        patch('pathlib.Path.home', return_value=root), \
                        patch.dict(INSTALLER['setup'].__globals__, claude_login=launch), \
                        redirect_stdout(self.output):
                    self.assertFalse(asyncio.run(INSTALLER['connect_claude'](store)))
            finally:
                store.close()
            login.assert_awaited_once()
            self.assertIn('Press Enter to sign in to Claude in your browser, or Ctrl-C to skip.', self.output.getvalue())
            self.assertIn("Setup already has the bot token, so you don't need to paste it.", self.output.getvalue())
            self.assertNotIn(FAKE_TOKEN, self.output.getvalue())
            self.assertNotIn(FAKE_TOKEN.encode(), self.screen())
            self.assertEqual(termios.tcgetattr(self.slave), self.saved)

    def test_gate_ctrl_c_and_eof_skip_login_and_restore_echo(self):
        for interrupt in (True, False):
            with self.subTest(ctrl_c=interrupt), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                store = Store(root / 'state')
                login = AsyncMock(side_effect=AssertionError('Gate must skip login'))
                async def run():
                    terminal.hide_input()
                    loop = asyncio.get_running_loop()
                    if interrupt:
                        loop.call_later(0.05, signal.raise_signal, signal.SIGINT)
                    else:
                        loop.call_later(0.05, os.write, self.master, b'\x04')
                    self.assertFalse(await INSTALLER['connect_claude'](store))
                try:
                    with patch.object(terminal, 'interactive_mac', return_value=True), \
                            patch('pathlib.Path.home', return_value=root), \
                            patch.dict(INSTALLER['setup'].__globals__, claude_login=login), \
                            redirect_stdout(self.output):
                        asyncio.run(run())
                finally:
                    store.close()
                login.assert_not_awaited()
                self.assertEqual(termios.tcgetattr(self.slave), self.saved)
        self.assertEqual(self.output.getvalue().count('Skipped.'), 2)

    def test_gate_waits_with_echo_hidden_until_enter(self):
        async def run():
            terminal.hide_input()
            task = asyncio.create_task(terminal.hidden_line('Press Enter'))
            await asyncio.sleep(0.06)
            self.assertFalse(task.done())
            self.assertFalse(termios.tcgetattr(self.slave)[3] & termios.ECHO)
            self.paste('')
            self.assertEqual(await asyncio.wait_for(task, 1), '')
            self.assertFalse(termios.tcgetattr(self.slave)[3] & termios.ECHO)
        with redirect_stdout(self.output):
            asyncio.run(run())
        self.assertEqual(self.screen(), b'')
        terminal.restore_input()
        self.assertEqual(termios.tcgetattr(self.slave), self.saved)

    def test_intermediate_prompts_flush_show_answers_then_hide_typing(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = Store(Path(temporary) / 'state')
            root = Path(temporary)
            bot = {'id': 99, 'username': 'test_bot'}
            api = Mock(call=AsyncMock(side_effect=[[{'command': 'old'}], {}]))
            answers = []
            def answer(text):
                self.assertEqual(termios.tcgetattr(self.slave), self.saved)
                self.assertFalse(select.select([self.slave], [], [], 0)[0])
                self.paste('y')
                answers.append(text)
                return os.read(self.slave, 4096).decode().strip()
            async def check(path):
                self.assertFalse(termios.tcgetattr(self.slave)[3] & termios.ECHO)
                self.paste(FAKE_TOKEN)
                if not answers:
                    raise TelegramError(502)
                return api, bot
            async def run():
                await INSTALLER['obtain_token'](root / 'config', store)
                self.assertFalse(termios.tcgetattr(self.slave)[3] & termios.ECHO)
                await INSTALLER['confirm_busy_bot'](store, api, bot)
                self.assertFalse(termios.tcgetattr(self.slave)[3] & termios.ECHO)
            try:
                with patch.object(terminal, 'interactive_mac', return_value=True), \
                        patch.dict(INSTALLER['setup'].__globals__, check_bot=check), \
                        patch('builtins.input', side_effect=answer), redirect_stdout(self.output):
                    asyncio.run(run())
                screen = self.screen()
                self.assertIn(b'y\r\n', screen)
                self.assertNotIn(FAKE_TOKEN.encode(), screen)
                self.assertEqual(answers, ['Retry checking this token? [Y/n] ', 'Use this bot for Torii anyway? [y/N] '])
            finally:
                terminal.restore_input()
                store.close()

    def test_setup_restores_echo_on_every_exit(self):
        for outcome in (None, ValueError('synthetic'), KeyboardInterrupt(), SystemExit(7)):
            with self.subTest(outcome=type(outcome).__name__), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                async def token(*args):
                    terminal.hide_input()
                    return Mock(set_default_admin_rights=AsyncMock()), {'id': 99, 'username': 'test_bot'}
                async def busy(*args):
                    if outcome is not None:
                        raise outcome
                with patch.object(terminal, 'interactive_mac', return_value=True), \
                        patch('pathlib.Path.home', return_value=root), \
                        patch.dict(INSTALLER['setup'].__globals__, state_dir=lambda: root / 'state',
                                   check_prereqs=lambda: ([], False), service_running=lambda *args: False,
                                   obtain_token=token, confirm_busy_bot=busy, install=Mock(), wait_running=AsyncMock()), \
                        redirect_stdout(self.output):
                    if outcome is None:
                        self.assertEqual(asyncio.run(INSTALLER['setup'](connect=False)), 0)
                    else:
                        with self.assertRaises(type(outcome)):
                            asyncio.run(INSTALLER['setup'](connect=False))
                self.assertEqual(termios.tcgetattr(self.slave), self.saved)

    def test_every_prompt_flushes_pending_input(self):
        self.paste(FAKE_TOKEN)
        def answer(text):
            self.assertFalse(select.select([self.slave], [], [], 0)[0])
            return 'fresh answer'
        with patch('builtins.input', side_effect=answer):
            self.assertEqual(terminal.prompt('Continue?'), 'fresh answer')

    def test_claude_skip_then_wait_ctrl_c_through_real_asyncio_run_is_clean(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = Store(Path(temporary) / 'state')
            globals_ = INSTALLER['setup'].__globals__
            async def login(store):
                signal.raise_signal(signal.SIGINT)
                await asyncio.sleep(0)
            async def setup(**kwargs):
                self.assertFalse(await INSTALLER['connect_claude'](store))
                if hasattr(asyncio.current_task(), 'cancelling'):
                    self.assertEqual(asyncio.current_task().cancelling(), 0)
                asyncio.get_running_loop().call_later(0.01, signal.raise_signal, signal.SIGINT)
                await INSTALLER['wait_paired'](store)
            try:
                with patch.dict(globals_, setup=setup, _connect_claude=login, require_tty=lambda: True), \
                        redirect_stdout(self.output):
                    self.assertEqual(INSTALLER['main']([]), 1)
            finally:
                store.close()
            self.assertIn('Skipped.', self.output.getvalue())
            self.assertIn('Waiting for you to add the bot...', self.output.getvalue())
            self.assertIn('Cancelled.', self.output.getvalue())

    def test_manual_line_wins_when_both_sources_are_ready(self):
        self.frames = [(0, 'READY\nTOKEN ' + FAKE_TOKEN + '\n')]
        self.on_show = self.paste
        self.assertEqual(self.candidate(), MANUAL_TOKEN)

    def test_manual_line_wins_if_it_arrives_while_helper_is_reaped(self):
        stop = terminal.stop_helper
        def reap(process):
            stop(process)
            if process is not None:
                self.paste()
                self.assertEqual(select.select([self.slave], [], [], 1)[0], [self.slave])
        with patch.object(terminal, 'stop_helper', reap):
            self.assertEqual(self.candidate(), MANUAL_TOKEN)

    def test_auto_pickup_flushes_partial_hidden_input(self):
        self.on_show = lambda: os.write(self.master, MANUAL_TOKEN.encode())
        self.assertEqual(self.candidate(), FAKE_TOKEN)
        os.write(self.master, b'\n')
        self.assertEqual(os.read(self.slave, 4096), b'\n')

    def test_deny_nil_permission_and_fixed_error_use_hidden_paste(self):
        for code in ('DENY', 'NIL', 'PERMISSION', 'ERROR', 'TIMEOUT'):
            with self.subTest(code=code):
                self.frames = [(0, code + '\n')]
                self.later(self.paste)
                self.assertEqual(self.candidate(), MANUAL_TOKEN)
                self.assertIn(terminal.PASTE, self.output.getvalue())

    def test_stalled_start_and_stalled_read_stop_watcher(self):
        for frames in ([], [(0, 'READY\nREADING\n')]):
            self.frames = frames
            self.later(self.paste)
            self.assertEqual(self.candidate(), MANUAL_TOKEN)
            self.assertIn(terminal.PASTE, self.output.getvalue())

    def test_watch_deadline_and_one_time_hint_keep_paste_available(self):
        self.frames = [(0, 'READY\n'), (0.05, 'IDLE\n')]
        with patch.object(terminal, 'WATCH_FOR', 0.1), patch.object(terminal, 'HINT_AFTER', 0.01):
            self.later(self.paste)
            self.assertEqual(self.candidate(), MANUAL_TOKEN)
        self.assertEqual(self.output.getvalue().count(terminal.HINT), 1)
        self.assertIn(terminal.PASTE, self.output.getvalue())

    def test_partial_frames_are_assembled_privately(self):
        self.frames = [(0, 'REA'), (0.01, 'DY\nTOKEN '), (0.01, FAKE_TOKEN + '\n')]
        self.assertEqual(self.candidate(), FAKE_TOKEN)

    def test_bad_or_oversize_frames_never_reach_output(self):
        for frame in ('TOKEN wrong\n', 'TOKEN ' + 'X' * 300, 'PRIVATE ERROR ' + FAKE_TOKEN + '\n'):
            self.frames = [(0, 'READY\n' + frame)]
            self.later(self.paste)
            self.assertEqual(self.candidate(), MANUAL_TOKEN)
            self.assertNotIn(frame, self.output.getvalue())

    def test_oversize_startup_frame_still_shows_url_and_paste_fallback(self):
        self.frames = [(0, 'X' * 300)]
        self.later(self.paste)
        self.assertEqual(self.candidate(), MANUAL_TOKEN)
        self.assertIn(terminal.BOTFATHER, self.output.getvalue())
        self.assertIn(terminal.PASTE, self.output.getvalue())

    def test_ctrl_c_restores_tty_and_reaps_helper(self):
        self.frames = [(0, 'READY\n')]
        self.on_show = lambda: signal.raise_signal(signal.SIGINT)
        with redirect_stdout(self.output), self.assertRaises(KeyboardInterrupt):
            terminal.obtain_candidate()
        self.assertEqual(termios.tcgetattr(self.slave), self.saved)
        self.assertTrue(all(process.poll() is not None for process in self.processes))

    def test_tty_eof_restores_tty_and_reaps_helper(self):
        self.frames = [(0, 'READY\n')]
        self.on_show = lambda: os.write(self.master, b'\x04')
        with redirect_stdout(self.output), self.assertRaises(EOFError):
            terminal.obtain_candidate()
        self.assertEqual(termios.tcgetattr(self.slave), self.saved)
        self.assertTrue(all(process.poll() is not None for process in self.processes))

    def test_helper_launch_failure_uses_paste(self):
        with patch.object(terminal.subprocess, 'Popen', side_effect=OSError('synthetic')):
            self.on_show = self.paste
            self.assertEqual(self.candidate(), MANUAL_TOKEN)
        self.assertIn(terminal.PASTE, self.output.getvalue())

    def test_helper_eof_uses_paste(self):
        self.frames = [(0, 'READY\n')]
        def end():
            self.processes[-1].terminate()
            self.later(self.paste, 0.05)
        self.on_show = end
        self.assertEqual(self.candidate(), MANUAL_TOKEN)
        self.assertIn(terminal.PASTE, self.output.getvalue())

    def test_token_shape_is_for_pickup_only(self):
        for valid in ('1:' + 'A' * 20, '9' * 20 + ':' + '_-' * 64):
            self.assertIsNotNone(terminal.TOKEN.fullmatch(valid))
        for invalid in ('1:' + 'A' * 19, '1:' + 'A' * 129, '9' * 21 + ':' + 'A' * 35, FAKE_TOKEN + '\n'):
            self.assertIsNone(terminal.TOKEN.fullmatch(invalid))


class QrTests(unittest.TestCase):
    def test_qr_text_is_on_stdin_and_absent_from_process_arguments(self):
        url = 'https://t.me/test_bot?startgroup=FAKE-PAIRING-CODE'
        white = b'\xff\xff\xff\xff' * 23 * 23
        with patch('subprocess.run', return_value=Mock(stdout=json.dumps(
                {'size': 23, 'rgba': base64.b64encode(white).decode()}).encode())) as run:
            terminal.qr_matrix(url)
        self.assertNotIn(url, repr(run.call_args.args))
        self.assertEqual(run.call_args.kwargs['input'], url.encode())

    def test_ssh_is_not_an_interactive_mac(self):
        with patch('sys.platform', 'darwin'), patch('sys.stdin.isatty', return_value=True), \
                patch('sys.stdout.isatty', return_value=True), patch.dict(os.environ, {'SSH_CONNECTION': 'fake ssh'}):
            self.assertFalse(terminal.interactive_mac())

    def test_bitmap_validation_rejects_bad_size_pixels_and_border(self):
        white = b'\xff\xff\xff\xff' * (23 * 23)
        invalid = [{'size': 22, 'rgba': ''}, {'size': True, 'rgba': ''}, {'size': 23, 'rgba': 'bad base64'},
                   {'size': 23, 'rgba': base64.b64encode(white[:-4]).decode()},
                   {'size': 23, 'rgba': base64.b64encode(b'\x00\x00\x00\xff' + white[4:]).decode()},
                   {'size': 23, 'rgba': base64.b64encode(white[:100] + b'\x01\x01\x01\xff' + white[104:]).decode()}]
        for value in invalid:
            with patch('subprocess.run', return_value=Mock(stdout=json.dumps(value).encode())), self.assertRaises(ValueError):
                terminal.qr_matrix(terminal.BOTFATHER)

    def test_narrow_terminal_prints_url_without_clipped_qr(self):
        matrix = [[0] * 29 for _ in range(29)]
        with patch.object(terminal, 'qr_matrix', return_value=matrix), \
                patch('shutil.get_terminal_size', return_value=os.terminal_size((37, 24))), redirect_stdout(io.StringIO()) as output:
            terminal.print_link(terminal.BOTFATHER)
        self.assertEqual(output.getvalue(), terminal.BOTFATHER + '\n')

    def test_real_terminal_width_takes_priority_over_columns_environment(self):
        class Output(io.StringIO):
            def fileno(self):
                return 42
        output = Output()
        with patch.object(terminal, 'qr_matrix', return_value=[[0] * 29 for _ in range(29)]), \
                patch('os.get_terminal_size', return_value=os.terminal_size((20, 24))) as size, \
                patch.dict(os.environ, {'COLUMNS': '80'}), redirect_stdout(output):
            terminal.print_link(terminal.BOTFATHER)
        size.assert_called_once_with(42)
        self.assertEqual(output.getvalue(), terminal.BOTFATHER + '\n')

    def test_qr_failure_prints_only_url_and_no_child_output(self):
        for error in (subprocess.TimeoutExpired('fake', 10), ValueError('bad QR'), OSError('synthetic')):
            with patch.object(terminal, 'qr_matrix', side_effect=error), redirect_stdout(io.StringIO()) as output:
                terminal.print_link(terminal.BOTFATHER)
            self.assertEqual(output.getvalue(), terminal.BOTFATHER + '\n')

    def test_matrix_is_square_binary_with_quiet_zone_and_color_reset(self):
        matrix = [[1] * 21 for _ in range(21)]
        rendered = terminal.render_qr(matrix, 80)
        for line in rendered.splitlines():
            self.assertTrue(line.startswith('\x1b[30m\x1b[107m'))
            self.assertTrue(line.endswith('\x1b[0m'))
        rows = self.reconstruct(rendered)
        self.assertEqual(rows[4:25], [[0] * 4 + row + [0] * 4 for row in matrix])
        self.assertFalse(any(any(row) for row in rows[:4] + rows[25:]))
        for bad in ([[1]], [[2] * 21 for _ in range(21)], [[0] * 20 for _ in range(21)]):
            with self.assertRaises(ValueError):
                terminal.render_qr(bad, 80)

    @staticmethod
    def reconstruct(rendered):
        rows = []
        for line in re.sub(r'\x1b\[[0-9;]*m', '', rendered).splitlines():
            bits = [' ▄▀█'.index(char) for char in line]
            rows.extend([[value >> 1 for value in bits], [value & 1 for value in bits]])
        return rows

    def test_open_is_fixed_bounded_and_never_uses_a_shell(self):
        with patch('subprocess.run') as run:
            terminal.open_url(terminal.BOTFATHER)
        self.assertEqual(run.call_args.args[0], ['/usr/bin/open', terminal.BOTFATHER])
        self.assertEqual(run.call_args.kwargs, {'stdout': subprocess.DEVNULL, 'stderr': subprocess.DEVNULL,
                                                'timeout': 10, 'check': False})

    @unittest.skipUnless(Path('/usr/bin/osascript').is_file(), 'Core Image requires macOS')
    def test_actual_coreimage_terminal_glyphs_decode_to_both_links(self):
        decoder = Path(__file__).parent / 'helpers/decode-qr.js'
        urls = [terminal.BOTFATHER, 'https://t.me/test_bot?startgroup=FAKE&admin=manage_topics+delete_messages+pin_messages',
                'https://t.me/' + 'a' * 29 + 'bot?startgroup=' + 'A' * 64 + '&admin=manage_topics+delete_messages+pin_messages']
        for url in urls:
            matrix = terminal.qr_matrix(url)
            rows = self.reconstruct(terminal.render_qr(matrix, 80))
            rows = rows[:len(rows[0])]
            scale = 8
            rgba = b''.join(b''.join((b'\x00\x00\x00\xff' if bit else b'\xff\xff\xff\xff') * scale
                                    for bit in row) * scale for row in rows[::-1])
            with tempfile.TemporaryDirectory() as temporary:
                bitmap = Path(temporary) / 'bitmap.json'
                bitmap.write_text(json.dumps({'size': len(rows[0]) * scale, 'rgba': base64.b64encode(rgba).decode()}))
                result = subprocess.run(['/usr/bin/osascript', '-l', 'JavaScript', str(decoder), str(bitmap)],
                                        capture_output=True, timeout=10, check=True)
            self.assertEqual(json.loads(result.stdout), [url])


class InteractiveSetupTestsSupport:
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        self.globals = INSTALLER['setup'].__globals__
        self.patches = [patch('pathlib.Path.home', return_value=self.root),
                        patch('coordinator.telegram.Telegram._request', side_effect=AssertionError('No Telegram')),
                        patch('subprocess.run', side_effect=no_external),
                        patch('asyncio.create_subprocess_exec', side_effect=lambda *args, **kwargs: no_external(args)),
                        patch.object(terminal, 'interactive_mac', return_value=True)]
        for handle in self.patches:
            handle.start()
            self.addCleanup(handle.stop)


class InteractiveSetupTests(InteractiveSetupTestsSupport, unittest.IsolatedAsyncioTestCase):
    async def test_bad_token_rearms_watcher_and_network_failure_retries_same_token(self):
        bot = {'username': 'test_bot'}
        with patch.object(terminal, 'obtain_candidate', side_effect=[FAKE_TOKEN, MANUAL_TOKEN]) as pickup, \
                patch.object(terminal, 'hide_input'), \
                patch.dict(self.globals, check_bot=AsyncMock(side_effect=[TelegramError(401), TelegramError(502), (None, bot)])), \
                patch('builtins.input', return_value='Y'), redirect_stdout(io.StringIO()) as output:
            self.assertEqual(await INSTALLER['obtain_token'](self.root / 'config', self.store), (None, bot))
        self.assertEqual(pickup.call_count, 2)
        self.assertIn("That token didn't work. Copy it again, or paste it here.", output.getvalue())
        self.assertIn('Bot @test_bot ok.', output.getvalue())
        self.assertNotIn(FAKE_TOKEN, output.getvalue())
        self.assertNotIn(MANUAL_TOKEN, output.getvalue())
        self.assertEqual((self.root / 'config/bot-token').stat().st_mode & 0o777, 0o600)

    async def test_claude_defaults_before_install_and_explicit_skip_works(self):
        for connect in (None, False):
            events = []
            async def token(*args):
                events.append('token')
                return Mock(set_default_admin_rights=AsyncMock()), {'id': 99, 'username': 'test_bot'}
            async def login(store):
                events.append('claude')
                return False
            with patch.dict(self.globals, state_dir=lambda: self.store.directory, check_prereqs=lambda: ([], False),
                            service_running=lambda directory: False, obtain_token=token, confirm_busy_bot=AsyncMock(),
                            connect_claude=login, install=lambda: events.append('install'), wait_running=AsyncMock()), \
                    redirect_stdout(io.StringIO()):
                self.assertEqual(await INSTALLER['setup'](connect=connect), 0)
            self.assertEqual(events, ['token', 'claude', 'install'] if connect is None else ['token', 'install'])

    async def test_claude_login_kills_and_reaps_child_that_ignores_termination(self):
        process = Mock(returncode=None)
        events = []
        async def wait():
            events.append('wait')
            if process.returncode is None:
                raise asyncio.CancelledError()
            return process.returncode
        process.wait = wait
        process.terminate.side_effect = lambda: events.append('terminate')
        process.kill.side_effect = lambda: (events.append('kill'), setattr(process, 'returncode', -9))
        async def timeout(awaitable, seconds):
            awaitable.close()
            raise asyncio.TimeoutError()
        with patch.object(terminal, 'interactive_mac', return_value=False), \
                patch('asyncio.create_subprocess_exec', AsyncMock(return_value=process)), \
                patch('asyncio.wait_for', timeout), self.assertRaises(asyncio.CancelledError):
            await INSTALLER['claude_login'](self.store, self.root)
        self.assertEqual(events, ['wait', 'terminate', 'kill', 'wait'])

    async def test_nonmac_keeps_paste_and_explicit_login_after_pairing(self):
        with patch.object(terminal, 'interactive_mac', return_value=False), \
                patch('getpass.getpass', return_value=FAKE_TOKEN) as hidden, \
                patch.dict(self.globals, check_bot=AsyncMock(return_value=(None, {'username': 'test_bot'}))), \
                redirect_stdout(io.StringIO()):
            await INSTALLER['obtain_token'](self.root / 'config', self.store)
        hidden.assert_called_once()
        events = []
        async def connect(store):
            events.append('claude')
        async def pair(*args):
            events.append('pair')
        with patch.object(terminal, 'interactive_mac', return_value=False), \
                patch.dict(self.globals, state_dir=lambda: self.store.directory, check_prereqs=lambda: ([], False),
                           service_running=lambda directory: True, check_bot=AsyncMock(return_value=(None, {'username': 'test_bot'})),
                           connect_claude=connect, wait_running=pair), redirect_stdout(io.StringIO()):
            await INSTALLER['setup'](connect=True)
        self.assertEqual(events, ['pair', 'claude'])

    async def test_claude_ctrl_c_reaps_login_and_restores_tty(self):
        master, slave = pty.openpty()
        saved = termios.tcgetattr(slave)
        real_open = open
        process = Mock(returncode=None)
        async def wait():
            if process.returncode is None:
                attrs = termios.tcgetattr(slave)
                attrs[3] &= ~termios.ECHO
                termios.tcsetattr(slave, termios.TCSANOW, attrs)
                signal.raise_signal(signal.SIGINT)
                await asyncio.sleep(10)
            return process.returncode
        process.wait = wait
        process.terminate.side_effect = lambda: setattr(process, 'returncode', -15)
        def tty_open(path, *args, **kwargs):
            return os.fdopen(os.dup(slave), 'r+b', buffering=0) if path == '/dev/tty' else real_open(path, *args, **kwargs)
        try:
            with patch('builtins.open', tty_open), patch('asyncio.create_subprocess_exec', AsyncMock(return_value=process)), \
                    patch.object(terminal, 'hidden_line', AsyncMock(return_value='')), \
                    redirect_stdout(io.StringIO()) as output:
                self.assertFalse(await INSTALLER['connect_claude'](self.store))
            process.terminate.assert_called_once()
            self.assertEqual(termios.tcgetattr(slave), saved)
            self.assertEqual(output.getvalue().count('Skipped.'), 1)
            self.assertIsNone(self.store.get('setup_claude_dir'))
        finally:
            os.close(master)
            os.close(slave)

    def test_group_screen_keeps_group_instructions_and_adds_qr(self):
        with patch('builtins.input', return_value=''), patch.object(terminal, 'open_url') as opened, \
                patch.object(terminal, 'print_link') as shown, redirect_stdout(io.StringIO()) as output:
            INSTALLER['open_group_link']('test_bot', 'FAKE')
        self.assertEqual(opened.call_args, shown.call_args)
        self.assertIn('startgroup=FAKE&admin=manage_topics+delete_messages+pin_messages', opened.call_args.args[0])
        self.assertIn('Next: add the bot to a Telegram group you own.', output.getvalue())
        self.assertIn('Telegram is opening on this Mac. To use your phone instead, scan this:', output.getvalue())
        self.assertIn('If nothing opened, use this link or QR (valid 30 minutes):', output.getvalue())

    def test_group_link_is_printed_once(self):
        with patch('builtins.input', return_value=''), patch.object(terminal, 'open_url'), \
                patch.object(terminal, 'print_link', side_effect=lambda url: print(url)), redirect_stdout(io.StringIO()) as output:
            INSTALLER['open_group_link']('test_bot', 'FAKE')
        self.assertEqual(output.getvalue().count('https://t.me/test_bot?'), 1)

    async def test_skip_flag_reaches_setup(self):
        setup = AsyncMock(return_value=0)
        with patch.dict(self.globals, setup=setup, require_tty=lambda: True), redirect_stdout(io.StringIO()):
            result = await asyncio.to_thread(INSTALLER['main'], ['--skip-claude'])
        self.assertEqual(result, 0)
        setup.assert_awaited_once_with(connect=False)
