"""Pinned Muse binary, private child environment and macOS write boundary."""

import asyncio
import hashlib
import json
import os
from pathlib import Path
import pwd
import re
import signal
import stat
import subprocess
import sys
import tempfile
import time

from .control_api import Refused
from .opencode_runtime import ENVIRONMENT_NAMES
from . import msp_protocol


PINNED_VERSION = '1.4.9-R6297.1'
PINNED_VERSION_BYTES = b'Muse Code 1.4.9 (1.4.9-R6297.1)\n'
PINNED_SHA256 = '2b856618ae3de3a63956e6d510b849ad707b16e5d2ed196299f3fe5ef2e3ff5e'
PINNED_SIZE = 368615952
STABLE_FINGERPRINT = 'sha256:5d3b43e5ccdb7dc4f5a32c380f5f2cde0f10e3059b93610afb2e73f73b1d914c'
PIN_REFUSAL = 'Set muse.binary to the pinned Muse Code 1.4.9-R6297.1 binary.'
ROUTING_NAMES = frozenset(('SSH_AUTH_SOCK', 'GIT_SSH', 'GIT_SSH_COMMAND', 'GIT_SSH_VARIANT',
                           'GIT_CONFIG_SYSTEM', 'GIT_CONFIG_NOSYSTEM', 'GIT_CONFIG_COUNT',
                           'GIT_ASKPASS', 'SSH_ASKPASS', 'GH_HOST'))
PRIVATE_FOLDERS = {'XDG_CONFIG_HOME': '.config', 'XDG_DATA_HOME': '.local/share',
                   'XDG_STATE_HOME': '.local/state', 'XDG_CACHE_HOME': '.cache', 'TMPDIR': 'tmp'}
_ACCOUNT = re.compile(r'\.torii-[0-9a-f]{8}\Z')
_CONFIG_ENTRY = re.compile(r'GIT_CONFIG_(?:KEY|VALUE)_([0-9]+)\Z')
_checked = set()


def _private_directory(path):
    if path.is_symlink():
        raise Refused('Muse folders must be real private folders, not symlinks.')
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
        raise Refused('Muse folders must be owned by this user.')
    path.chmod(0o700)
    return path


def environment(home):
    home = Path(home)
    if not home.is_absolute():
        raise Refused('Muse home must be an absolute private folder.')
    try:
        _private_directory(home)
        home = home.resolve()
        env = {name: os.environ[name] for name in ENVIRONMENT_NAMES | ROUTING_NAMES if name in os.environ}
        count = os.environ.get('GIT_CONFIG_COUNT', '')
        if count.isascii() and count.isdecimal():
            for name, value in os.environ.items():
                match = _CONFIG_ENTRY.fullmatch(name)
                if match and int(match[1]) < int(count):
                    env[name] = value
        for name, fallback in (('GIT_CONFIG_GLOBAL', Path.home() / '.gitconfig'),
                               ('GH_CONFIG_DIR', Path.home() / '.config/gh')):
            if name in os.environ:
                env[name] = os.environ[name]
            elif (fallback.is_file() if name == 'GIT_CONFIG_GLOBAL' else fallback.is_dir()):
                env[name] = str(fallback)
        env['HOME'] = str(home)
        _private_directory(home / '.local')
        for name, relative in PRIVATE_FOLDERS.items():
            env[name] = str(_private_directory(home / relative))
        stub_dir = _private_directory(home / '.torii-bin')
        with tempfile.NamedTemporaryFile(dir=stub_dir, prefix='.open-', delete=False) as handle:
            os.fchmod(handle.fileno(), 0o700)
            handle.write(b'#!/bin/sh\nprintf "%s\\n" "Torii blocked browser launch." >&2\nexit 126\n')
        os.replace(handle.name, stub_dir / 'open')
        env['PATH'] = str(stub_dir) + os.pathsep + env.get('PATH', os.defpath)
        env['MUSE_NO_AUTO_UPDATE'] = '1'
        env['TBH_DISABLE_TELEMETRY'] = '1'
        return env
    except (OSError, ValueError, RuntimeError) as error:
        raise Refused('Muse needs writable private home folders.') from error


def write_boundary(argv, roots, *, pending_roots=(), files=()):
    """Pass a fresh inline profile with directory roots and exact Git files."""
    if sys.platform != 'darwin' or not os.access('/usr/bin/sandbox-exec', os.X_OK):
        raise Refused('Muse requires the macOS sandbox-exec write boundary.')
    if not argv or not roots:
        raise Refused('Muse needs a command and explicit worker write roots.')
    try:
        allowed = []
        library = Path(pwd.getpwuid(os.getuid()).pw_dir).resolve() / 'Library'
        for value in (*roots, *pending_roots):
            path = Path(value)
            if not path.is_absolute() or path.is_symlink():
                raise Refused('Muse write roots must be absolute real folders.')
            path = path.resolve()
            if (path == library or path in library.parents or path.exists() and
                    any(parent.exists() and path.samefile(parent) for parent in (library, *library.parents))):
                raise Refused('Muse write roots must not contain the real home Library.')
            pending = value in pending_roots and not path.exists()
            owner_path = path.parent if pending else path
            if not owner_path.is_dir() or path == Path(path.anchor) or owner_path.stat().st_uid != os.getuid():
                raise Refused('Muse write roots must be worker folders owned by this user.')
            if path not in allowed:
                allowed.append(path)
        lines = ['(version 1)', '(allow default)', '(deny file-write*', '    (require-all']
        lines.extend('        (require-not (subpath ' + json.dumps(str(path), ensure_ascii=False) + '))'
                     for path in allowed)
        for value in files:
            path = Path(value)
            if (not path.is_absolute() or path.is_symlink() or path.parent.resolve() != path.parent
                    or not path.parent.is_dir() or path.parent.stat().st_uid != os.getuid()
                    or path.exists() and (not path.is_file() or path.stat().st_uid != os.getuid())):
                raise Refused('Muse Git files must have real folders owned by this user.')
            lines.append('        (require-not (literal ' + json.dumps(str(path), ensure_ascii=False) + '))')
        lines.extend(['        (require-not (subpath "/dev"))))',
                      '(deny process-exec (literal "/usr/bin/open"))'])
        return ['/usr/bin/sandbox-exec', '-p', '\n'.join(lines) + '\n', *argv]
    except (OSError, ValueError, RuntimeError, KeyError) as error:
        raise Refused('Muse write boundary could not be created. Muse was not started.') from error


def _fingerprint(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def check_binary(path):
    try:
        if not isinstance(path, str) or not Path(path).is_absolute():
            raise Refused(PIN_REFUSAL)
        info = os.lstat(path)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or info.st_mode & 0o022 or info.st_size != PINNED_SIZE):
            raise Refused(PIN_REFUSAL)
        fingerprint = _fingerprint(info)
        if fingerprint in _checked:
            return
        digest = hashlib.sha256()
        with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), 'rb') as handle:
            if _fingerprint(os.fstat(handle.fileno())) != fingerprint:
                raise Refused(PIN_REFUSAL)
            for chunk in iter(lambda: handle.read(1024 * 1024), b''):
                digest.update(chunk)
        if digest.hexdigest() != PINNED_SHA256 or _fingerprint(os.lstat(path)) != fingerprint:
            raise Refused(PIN_REFUSAL)
        with tempfile.TemporaryDirectory(prefix='torii-muse-version-') as temporary:
            env = environment(temporary)
            completed = subprocess.run(write_boundary([path, '--version'], [temporary]),
                                       stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                       stderr=subprocess.DEVNULL, timeout=10, env=env)
        if (completed.returncode != 0 or completed.stdout != PINNED_VERSION_BYTES
                or _fingerprint(os.lstat(path)) != fingerprint):
            raise Refused(PIN_REFUSAL)
        _checked.add(fingerprint)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError, Refused):
        raise Refused(PIN_REFUSAL) from None


def op_binary(store, ctx, path):
    if ctx.source != 'cli':
        raise Refused('Muse binary selection is a local CLI setting.')
    check_binary(path)
    store.put('muse_binary', path)
    return 'Pinned Muse Code binary path set. Only the pinned Muse Code 1.4.9-R6297.1 binary runs.'


def binary(store):
    return store.get('muse_binary') or None


def accounts_root():
    try:
        return _private_directory(Path.home() / '.muse-accounts')
    except (OSError, ValueError, RuntimeError) as error:
        raise Refused('Muse accounts need a real private .muse-accounts folder.') from error


def account_home(path):
    root = accounts_root()
    try:
        home = Path(path)
        if not home.is_absolute() or home.parent != root or not _ACCOUNT.fullmatch(home.name) or home.is_symlink():
            raise Refused('Muse account home must be a direct .torii-<8 hex> child of .muse-accounts.')
        return _private_directory(home)
    except (OSError, TypeError, ValueError, RuntimeError) as error:
        raise Refused('Muse account home must be a real private account folder.') from error


def owner_settings(store):
    return dict(muse_enabled=store.get('muse_enabled', False) is True, muse_binary=binary(store))


class MuseClient:
    """Transient MSP account requests. Device replies never enter a host journal."""

    def __init__(self, binary, home):
        self.binary = binary
        self.home = Path(home)
        self.process = None
        self.notifications = []

    async def start(self):
        env = environment(self.home)
        argv = write_boundary([self.binary, 'serve', '--provider', 'meta', '--contributor-mode', 'off',
                               '--disable-request-user-input'], [self.home])
        self.process = await asyncio.create_subprocess_exec(
            *argv, env=env, cwd=self.home, stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            limit=msp_protocol.FRAME_LIMIT, start_new_session=True)

    async def send(self, method, params=None, request_id=None):
        if method not in msp_protocol.METHODS:
            raise ValueError('MSP method is not allowed')
        event = {'jsonrpc': '2.0', 'method': method}
        if params is not None:
            event['params'] = params
        if request_id is not None:
            event['id'] = request_id
        self.process.stdin.write(msp_protocol.encode_frame(event))
        await self.process.stdin.drain()

    async def read(self):
        raw = await self.process.stdout.readline()
        if not raw:
            raise ValueError('Muse stopped before replying')
        event = msp_protocol.decode_frame(raw)
        if 'method' in event:
            if 'id' in event or not isinstance(event.get('params', {}), dict):
                raise ValueError('Unexpected MSP request')
            self.notifications.append(event)
        return event

    async def request(self, method, params=None):
        request_id = 'torii-' + msp_protocol.uuid7()
        await self.send(method, params, request_id)
        while True:
            event = await self.read()
            if event.get('id') == request_id and 'method' not in event:
                if 'error' in event:
                    raise msp_protocol.MspRequestError(method, event['error'])
                result = event.get('result')
                if not isinstance(result, dict):
                    raise ValueError('MSP result must be an object')
                return result

    async def notification(self, method):
        while True:
            for event in self.notifications:
                if event['method'] == method:
                    self.notifications.remove(event)
                    return event.get('params', {})
            await self.read()

    async def stop(self):
        if self.process is None:
            return
        process = self.process
        async def wait_group():
            while True:
                try:
                    os.killpg(process.pid, 0)
                except ProcessLookupError:
                    return
                except PermissionError:
                    pass
                await asyncio.sleep(.05)
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(wait_group(), 5)
        except asyncio.CancelledError:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
            raise
        except asyncio.TimeoutError:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(wait_group(), 5)
            except asyncio.TimeoutError:
                pass
        await process.wait()
        self.process = None


class MuseBroker:
    """The one registered Muse account. Labels are display text only."""

    def __init__(self, store):
        self.store = store

    def accounts(self):
        return {alias: dict(profile) for alias, profile in (self.store.get('muse_accounts', {}) or {}).items()
                if isinstance(profile, dict)}

    def label(self, alias):
        value = self.accounts().get(alias, {}).get('label')
        return value if isinstance(value, str) and value else alias

    def signed_in(self, alias):
        directory = self.accounts().get(alias, {}).get('config_dir')
        snapshot = (self.store.get('muse_account_status', {}) or {}).get(alias) or {}
        return (isinstance(directory, str) and Path(directory).is_dir()
                and (snapshot.get('identity') or {}).get('logged_in') is True
                and snapshot.get('error') != 'login_required')

    def authenticated(self, alias):
        return self.signed_in(alias)

    def active(self):
        alias = self.store.get('muse_active_account')
        return alias if alias in self.accounts() else None

    def held(self, alias, now=None):
        until = (self.store.get('muse_account_blocks', {}) or {}).get(alias)
        return type(until) in (int, float) and (time.time() if now is None else now) < until

    def available(self):
        alias = self.active()
        return self.signed_in(alias) and not self.held(alias)

    def run_ready(self, alias=None):
        if self.store.get('muse_enabled', False) is not True:
            raise Refused('Muse workers are off.')
        active = self.active()
        if alias is not None and alias != active:
            raise Refused('Muse requires the active account.')
        if self.held(active):
            until = self.store.get('muse_account_blocks')[active]
            raise Refused('Muse is at its usage limit until ' + time.strftime('%Y-%m-%d %H:%M %Z', time.localtime(until)) + '.')
        if not self.signed_in(active):
            raise Refused('No Muse account is signed in.')
        account_home(self.accounts()[active]['config_dir'])
        return active

    async def run(self, run, provider, prompt, cwd, session_id, *, notice_topic=None, on_account=None, **options):
        alias = self.run_ready(options.pop('account_alias', None))
        if on_account:
            on_account(alias)
        return await run(provider, prompt, cwd, session_id, account_alias=alias, **options)
