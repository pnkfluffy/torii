"""Private terminal input and macOS setup links."""

import asyncio
import base64
from contextlib import contextmanager
import copy
import json
import os
from pathlib import Path
import re
import selectors
import shutil
import subprocess
import sys
import termios
import time


HELPERS = Path(__file__).resolve().parent / 'helpers'
BOTFATHER = 'https://t.me/BotFather?startapp'
PASTE = 'Copy the token, come back here, press Command-V, then Return.'
HINT = "Still waiting. If the copy doesn't arrive, paste the token here."
TOKEN = re.compile(r'[0-9]{1,20}:[A-Za-z0-9_-]{20,128}')
FRAME_LIMIT = 256
START_TIMEOUT = 5
READ_TIMEOUT = 10
HINT_AFTER = 60
WATCH_FOR = 300
_pending_terminal = None


def interactive_mac():
    return (sys.platform == 'darwin' and sys.stdin.isatty() and sys.stdout.isatty()
            and not any(os.environ.get(key) for key in ('SSH_CONNECTION', 'SSH_CLIENT', 'SSH_TTY')))


def open_url(url):
    try:
        subprocess.run(['/usr/bin/open', url], stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL, timeout=10, check=False)
    except (OSError, subprocess.SubprocessError):
        pass


def qr_matrix(url):
    result = subprocess.run(['/usr/bin/osascript', '-l', 'JavaScript', str(HELPERS / 'setup-qr.js')],
                            input=url.encode('utf-8'), capture_output=True, timeout=10, check=True)
    if len(result.stdout) > 200000:
        raise ValueError('QR output too large')
    value = json.loads(result.stdout)
    size = value['size']
    if type(size) is not int or not 23 <= size <= 179 or (size - 23) % 4:
        raise ValueError('Invalid QR size')
    rgba = base64.b64decode(value['rgba'], validate=True)
    if len(rgba) != size * size * 4:
        raise ValueError('Invalid QR bitmap')
    pixels = [rgba[index:index + 4] for index in range(0, len(rgba), 4)]
    if any(pixel not in (b'\x00\x00\x00\xff', b'\xff\xff\xff\xff') for pixel in pixels):
        raise ValueError('Invalid QR modules')
    rows = [[int(pixels[y * size + x][0] == 0) for x in range(size)] for y in range(size)]
    if any(rows[0]) or any(rows[-1]) or any(row[0] or row[-1] for row in rows):
        raise ValueError('Invalid QR border')
    return [row[1:-1] for row in rows[1:-1]][::-1]


def render_qr(matrix, columns):
    size = len(matrix)
    if not 21 <= size <= 177 or (size - 21) % 4 or any(
            len(row) != size or any(type(bit) is not int or bit not in (0, 1) for bit in row) for row in matrix):
        raise ValueError('Invalid QR matrix')
    width = size + 8
    if width >= columns:
        return ''
    white = [0] * width
    rows = [white] * 4 + [[0] * 4 + row + [0] * 4 for row in matrix] + [white] * 5
    return ''.join('\x1b[30m\x1b[107m' + ''.join(' ▄▀█'[top * 2 + bottom] for top, bottom in zip(rows[y], rows[y + 1]))
                   + '\x1b[0m\n' for y in range(0, len(rows), 2))


def print_link(url):
    try:
        matrix = qr_matrix(url)
        try:
            columns = os.get_terminal_size(sys.stdout.fileno()).columns
        except (OSError, ValueError):
            columns = shutil.get_terminal_size().columns
        rendered = render_qr(matrix, columns)
    except (OSError, subprocess.SubprocessError, ValueError, TypeError, KeyError):
        rendered = ''
    if rendered:
        print(rendered, end='')
    print(url, flush=True)


def flush_input():
    try:
        with open('/dev/tty', 'r+b', buffering=0) as tty:
            termios.tcflush(tty.fileno(), termios.TCIFLUSH)
    except (OSError, termios.error):
        pass


def restore_input():
    global _pending_terminal
    if _pending_terminal is not None:
        fd, saved = _pending_terminal
        _pending_terminal = None
        try:
            termios.tcflush(fd, termios.TCIFLUSH)
            termios.tcsetattr(fd, termios.TCSANOW, saved)
        finally:
            os.close(fd)


def hide_input():
    global _pending_terminal
    if _pending_terminal is not None:
        return
    with open('/dev/tty', 'r+b', buffering=0) as tty:
        fd = tty.fileno()
        saved = termios.tcgetattr(fd)
        attrs = copy.deepcopy(saved)
        attrs[3] = (attrs[3] | termios.ICANON | termios.ISIG) & ~(termios.ECHO | termios.ECHONL)
        termios.tcsetattr(fd, termios.TCSANOW, attrs)
        _pending_terminal = (os.dup(fd), saved)


async def hidden_line(text):
    hide_input()
    print(text, flush=True)
    with open('/dev/tty', 'r+b', buffering=0) as tty, selectors.DefaultSelector() as selector:
        selector.register(tty, selectors.EVENT_READ)
        while not selector.select(0):
            await asyncio.sleep(0.05)
        return manual_line(tty.fileno())


def prompt(text='', hidden=False, retain_hidden=False):
    flush_input()
    if not hidden:
        restore_input()
    try:
        return input(text)
    finally:
        if retain_hidden:
            hide_input()


@contextmanager
def terminal(hidden=False, retain_hidden=False):
    global _pending_terminal
    try:
        tty = open('/dev/tty', 'r+b', buffering=0)
    except OSError:
        raise ValueError('Run setup in your own terminal. A controlling terminal is required.') from None
    with tty:
        fd = tty.fileno()
        saved = termios.tcgetattr(fd)
        if _pending_terminal is not None:
            pending_fd, saved = _pending_terminal
            _pending_terminal = None
            os.close(pending_fd)
        completed = False
        try:
            termios.tcflush(fd, termios.TCIFLUSH)
            if hidden:
                attrs = copy.deepcopy(saved)
                attrs[3] = (attrs[3] | termios.ICANON | termios.ISIG) & ~(termios.ECHO | termios.ECHONL)
                termios.tcsetattr(fd, termios.TCSANOW, attrs)
            yield fd
            completed = True
        finally:
            try:
                termios.tcflush(fd, termios.TCIFLUSH)
            finally:
                if completed and retain_hidden:
                    _pending_terminal = (os.dup(fd), saved)
                else:
                    termios.tcsetattr(fd, termios.TCSANOW, saved)


def stop_helper(process):
    if process is None:
        return
    try:
        if process.poll() is None:
            try:
                process.terminate()
            except ProcessLookupError:
                pass
        try:
            process.wait(timeout=1)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=1)
    finally:
        process.stdout.close()


def manual_line(fd):
    value = os.read(fd, 4096)
    if not value:
        raise EOFError()
    if b'\n' not in value:
        return ''
    return value.split(b'\n', 1)[0].decode('utf-8', errors='replace').strip()


def token_screen():
    print('Create your Torii bot')
    print('BotFather is opening on this Mac. To use your phone instead, scan this:')
    print_link(BOTFATHER)
    print('Tap Create a New Bot, pick a name and a username ending in "bot", then tap Copy next to the token.')
    print('Setup picks up the copied token by itself, from this Mac or your iPhone.')
    print('Or paste it here and press Return (it stays hidden):')
    print('No Create a New Bot button? Send /newbot to @BotFather.', flush=True)


def obtain_candidate(show_screen=True):
    with terminal(hidden=True, retain_hidden=True) as fd, selectors.DefaultSelector() as selector:
        process = None
        try:
            selector.register(fd, selectors.EVENT_READ, 'tty')
            started = time.monotonic()
            deadline = started + START_TIMEOUT
            opened = False
            hinted = False
            buffer = b''

            def fallback():
                nonlocal process
                if process is not None:
                    selector.unregister(process.stdout)
                    stop_helper(process)
                    process = None
                if not opened:
                    show()
                print(PASTE, flush=True)

            def show():
                nonlocal opened
                if show_screen:
                    open_url(BOTFATHER)
                    token_screen()
                opened = True

            try:
                process = subprocess.Popen(['/usr/bin/osascript', '-l', 'JavaScript', str(HELPERS / 'setup-clipboard.js')],
                                           stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
                os.set_blocking(process.stdout.fileno(), False)
                selector.register(process.stdout, selectors.EVENT_READ, 'helper')
            except OSError:
                stop_helper(process)
                process = None
                show()
                print(PASTE, flush=True)
            while True:
                now = time.monotonic()
                if not hinted and now - started >= HINT_AFTER:
                    print(HINT, flush=True)
                    hinted = True
                if process is not None and (now >= deadline or now - started >= WATCH_FOR):
                    fallback()
                events = selector.select(0.1)
                if any(key.data == 'tty' for key, _ in events):
                    return manual_line(fd)
                for key, _ in events:
                    if key.data != 'helper' or process is None:
                        continue
                    chunk = os.read(process.stdout.fileno(), FRAME_LIMIT + 1)
                    buffer += chunk
                    if not chunk:
                        fallback()
                        break
                    while b'\n' in buffer and process is not None:
                        frame, buffer = buffer.split(b'\n', 1)
                        if len(frame) > FRAME_LIMIT:
                            fallback()
                            break
                        if frame == b'READY' and not opened:
                            show()
                            deadline = time.monotonic() + READ_TIMEOUT
                        elif frame in (b'READING', b'IDLE') and opened:
                            deadline = time.monotonic() + READ_TIMEOUT
                        elif frame == b'ASK' and opened:
                            deadline = started + WATCH_FOR
                        elif frame.startswith(b'TOKEN ') and opened:
                            candidate = frame[6:].decode('ascii', errors='replace').strip()
                            if not TOKEN.fullmatch(candidate):
                                fallback()
                                break
                            selector.unregister(process.stdout)
                            stop_helper(process)
                            process = None
                            if selector.select(0):
                                return manual_line(fd)
                            print('Copied token picked up. No paste is needed.', flush=True)
                            return candidate
                        else:
                            fallback()
                            break
                    if process is not None and len(buffer) > FRAME_LIMIT:
                        fallback()
        finally:
            stop_helper(process)
