"""Antigravity account homes, the official agy binary, usage and account selection.

Torii never opens agy's own files under an account home. The official CLI
reports sign-in state, the model list and usage.
"""

import asyncio
from datetime import datetime
import json
import os
from pathlib import Path
import re
import shlex
import signal
import stat
import subprocess
import time

from . import host_os, sweeps
from .control_api import Refused
from .opencode_runtime import ENVIRONMENT_NAMES


ROUTING_NAMES = frozenset(('SSH_AUTH_SOCK', 'GIT_SSH', 'GIT_SSH_COMMAND', 'GIT_SSH_VARIANT',
                           'GIT_CONFIG_SYSTEM', 'GIT_CONFIG_NOSYSTEM', 'GIT_CONFIG_COUNT',
                           'GIT_ASKPASS', 'SSH_ASKPASS', 'GH_HOST'))
PRIVATE_FOLDERS = {'XDG_CONFIG_HOME': 'xdg-config', 'XDG_DATA_HOME': 'xdg-data',
                   'XDG_STATE_HOME': 'xdg-state', 'XDG_CACHE_HOME': 'xdg-cache', 'TMPDIR': 'tmp'}
INSTALLER = 'https://antigravity.google/cli/install.sh'
CHECK_SECONDS = 30
USAGE_SECONDS = 90
USAGE_REFRESH = 900
USAGE_STALE = 2 * USAGE_REFRESH
RECHECK_SECONDS = 900
EMPTY = 0.005
DEFAULT_MODEL = 'gemini-4-neon-high'
_CONFIG_ENTRY = re.compile(r'GIT_CONFIG_(?:KEY|VALUE)_([0-9]+)\Z')
_MODEL_LINE = re.compile(r'([A-Za-z0-9][A-Za-z0-9_.:-]{0,127})\t(.{1,200})\Z')


def default_binary():
    return str(Path.home() / '.local/share/agy/bin/agy')


def binary(store):
    """The one agy path. Workers can write the state database, so no setting chooses the binary."""
    return default_binary()


def check_binary(path):
    try:
        if not isinstance(path, str) or not Path(path).is_absolute():
            raise ValueError
        info = os.stat(path)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o022 or not os.access(path, os.X_OK):
            raise ValueError
    except (OSError, ValueError):
        folder = Path(default_binary()).parent
        raise Refused('Install the official agy binary at %s with `curl -fsSL %s | bash -s -- --dir %s`. '
                      'It must be a private executable owned by this user.'
                      % (default_binary(), INSTALLER, shlex.quote(str(folder)))) from None
    return path


def op_delegation(store, ctx, enabled):
    from .antigravity_runtime import check_sandbox, sandboxed, write_boundary
    if enabled:
        check_binary(binary(store))
        if sandboxed(store):
            check_sandbox()
            write_boundary(['true'], [accounts_root()])
    store.put('antigravity_enabled', enabled)
    return 'Antigravity workers are ' + ('on.' if enabled else 'off.')


def _private_directory(path):
    if path.is_symlink():
        raise Refused('Antigravity folders must be real private folders, not symlinks.')
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
        raise Refused('Antigravity folders must be owned by this user.')
    path.chmod(0o700)
    return path


def accounts_root():
    try:
        return _private_directory(Path.home() / '.local/share/agy/accounts')
    except OSError as error:
        raise Refused('Antigravity accounts need a real private folder.') from error


def jobs_root():
    """The per-worker overlays of account homes. Workers cannot write or read this folder."""
    try:
        return _private_directory(Path.home() / '.local/share/agy/jobs')
    except OSError as error:
        raise Refused('Antigravity workers need a real private folder.') from error


def account_home(alias, create=False):
    from .controls import _ALIAS
    if not isinstance(alias, str) or not _ALIAS.fullmatch(alias):
        raise Refused('Send a valid Antigravity account alias.')
    home = accounts_root() / alias
    try:
        if not create and not home.is_dir():
            raise Refused('Antigravity account %s has no profile folder.' % alias)
        return _private_directory(home)
    except OSError as error:
        raise Refused('Antigravity account home must be a real private folder.') from error


def worker_environment(home, service):
    """The service environment of a worker without the boundary, with the profile of environment().

    Google key variables are left out, so they cannot replace the account sign-in.
    """
    kept = {name: value for name, value in service.items() if not name.upper().startswith(('GEMINI_', 'GOOGLE_'))}
    return {**kept, **environment(home)}


def environment(home, display=False):
    """A clean child environment. HOME selects the agy profile.

    Browser launchers resolve to a stub. With display, the stub saves the link
    that agy opens during sign-in to the file named by TORII_AGY_LINK.
    On macOS, open is a browser launcher too.
    """
    home = Path(home)
    try:
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
        env['HOME'] = str(_private_directory(home))
        for name, folder in PRIVATE_FOLDERS.items():
            env[name] = str(_private_directory(home / folder))
        stubs = _private_directory(home / '.torii-bin')
        script = ('#!/bin/sh\n[ -n "$TORII_AGY_LINK" ] && printf "%s\\n" "$1" > "$TORII_AGY_LINK"\nexit 0\n'
                  if display else '#!/bin/sh\nexit 0\n')
        for name in host_os.browser_stub_names():
            stub = stubs / name
            temporary = stubs / ('.' + name + '.' + str(os.getpid()))
            temporary.write_text(script)
            temporary.chmod(0o700)
            os.replace(temporary, stub)
        env['PATH'] = str(stubs) + os.pathsep + env.get('PATH', os.defpath)
        env['NO_COLOR'] = '1'
        if display:
            env['DISPLAY'] = ':0'
        return env
    except OSError as error:
        raise Refused('Antigravity needs writable private home folders.') from error


def models_call(store, alias):
    home = account_home(alias)
    return [check_binary(binary(store)), 'models'], environment(home), home


def read_models(store, alias):
    return run_models(*models_call(store, alias))


def run_models(command, env, home):
    """Ask the official CLI for the account's models. A signed-out profile returns None."""
    try:
        completed = subprocess.run(command, env=env, cwd=home, stdin=subprocess.DEVNULL,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=CHECK_SECONDS)
    except (OSError, subprocess.SubprocessError):
        raise Refused('The agy binary did not answer.') from None
    if completed.returncode != 0:
        if 'sign in' in completed.stdout.lower():
            return None
        raise Refused('The agy binary could not list models. Try again.')
    models = [match.groups() for match in map(_MODEL_LINE.fullmatch, completed.stdout.splitlines()) if match]
    return [{'id': model, 'name': name} for model, name in models]


async def end_group(process, signals=(signal.SIGTERM, signal.SIGKILL), seconds=5):
    """Stop a child and its process group. Every wait has a limit, so a child that cannot be reaped never blocks Torii."""
    ended = asyncio.ensure_future(process.wait())
    try:
        for number in signals:
            try:
                os.killpg(process.pid, number)
            except ProcessLookupError:
                pass
            except PermissionError:
                try:
                    process.send_signal(number)
                except ProcessLookupError:
                    pass
            if (await asyncio.wait((ended,), timeout=seconds))[0]:
                return
    finally:
        ended.cancel()


def parse_usage(document):
    """The usage groups of `agy -p /usage --output-format json`, or None when the reply has none."""
    command = document.get('command') if isinstance(document, dict) else None
    data = command.get('data') if isinstance(command, dict) and command.get('name') == 'usage' else None
    if document.get('status') != 'SUCCESS' or not isinstance(data, dict) or not isinstance(data.get('groups'), list):
        return None
    groups = []
    for group in data['groups']:
        buckets = []
        for bucket in (group.get('buckets') if isinstance(group, dict) else None) or []:
            remaining = bucket.get('remaining_fraction') if isinstance(bucket, dict) else None
            if type(remaining) not in (int, float) or not 0 <= remaining <= 1:
                continue
            try:
                reset = datetime.fromisoformat(str(bucket.get('reset_time')).replace('Z', '+00:00')).timestamp()
            except ValueError:
                reset = None
            buckets.append({'id': str(bucket.get('id'))[:80], 'window': str(bucket.get('window'))[:40],
                            'remaining': remaining, 'resets_at': reset})
        if buckets:
            groups.append({'name': ' '.join(str(group.get('name')).split())[:80], 'buckets': buckets})
    return groups or None


async def read_usage(store, alias):
    """Ask the official CLI for usage. Return groups, 'signed_out' or None when the read failed."""
    try:
        command = [check_binary(binary(store)), '-p', '/usage', '--output-format', 'json']
        home = account_home(alias)
        process = await asyncio.create_subprocess_exec(
            *command, env=environment(home), cwd=home, stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL, start_new_session=True)
    except (Refused, OSError):
        return None
    try:
        output, _ = await asyncio.wait_for(process.communicate(), USAGE_SECONDS)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        await end_group(process, (signal.SIGKILL,))
        raise
    try:
        document = json.loads(output)
    except ValueError:
        return None
    if isinstance(document, dict) and 'authentication' in str(document.get('error', '')).lower():
        return 'signed_out'
    return parse_usage(document) if isinstance(document, dict) else None


async def refresh_usage(store, alias):
    """Save one account's usage from the official CLI. A failed read keeps the last snapshot."""
    try:
        usage = await read_usage(store, alias)
    except asyncio.TimeoutError:
        usage = None
    with store.db:
        statuses = store.get('antigravity_account_status', {}) or {}
        status = dict(statuses.get(alias) or {})
        status['usage_checked'] = time.time()
        if usage == 'signed_out':
            status['logged_in'] = False
        elif usage is not None:
            status['usage'] = {'groups': usage, 'observed_at': time.time()}
        statuses[alias] = status
        store.put('antigravity_account_status', statuses)
        blocks = store.get('antigravity_account_blocks', {}) or {}
        if isinstance(usage, list) and alias in blocks and not any(empty_buckets(group) for group in usage):
            del blocks[alias]
            store.put('antigravity_account_blocks', blocks)
    return status


def empty_buckets(group):
    now = time.time()
    return [bucket for bucket in group['buckets'] if bucket['remaining'] <= EMPTY
            and not (bucket['resets_at'] is not None and bucket['resets_at'] <= now)]


def exhausted_until(status):
    """When every model group is empty, the earliest time one group has usage again."""
    groups = ((status or {}).get('usage') or {}).get('groups') or []
    now = time.time()
    resets = []
    for group in groups:
        empty = empty_buckets(group)
        if not empty:
            return None
        known = [bucket['resets_at'] for bucket in empty if bucket['resets_at'] is not None]
        resets.append(max(known) if len(known) == len(empty) else now + RECHECK_SECONDS)
    return min(resets) if resets else None


async def monitor(store):
    """Refresh usage for signed-in accounts on a slow clock. A new account is read at once."""
    while True:
        statuses = store.get('antigravity_account_status', {}) or {}
        await sweeps.arun(store, 'antigravity-usage', [
            alias for alias in accounts(store) if (statuses.get(alias) or {}).get('logged_in') is True
            and time.time() - (statuses.get(alias) or {}).get('usage_checked', 0) >= USAGE_REFRESH],
            lambda alias: refresh_usage(store, alias), area='accounts')
        await asyncio.sleep(5)


def _window(bucket):
    return {'5h': '5 h', 'weekly': 'week'}.get(bucket['window'], bucket['window'])


def _state(store, broker, alias):
    status = (store.get('antigravity_account_status', {}) or {}).get(alias) or {}
    usage = status.get('usage') or {}
    if not broker.signed_in(alias):
        return '⚪', 'not signed in', usage
    if broker.held(alias):
        return '🔴', 'limit reached', usage
    if not usage or time.time() - usage.get('observed_at', 0) > USAGE_STALE:
        return '🟡', 'usage not read yet' if not usage else 'usage is stale', usage
    return '🟢', '', usage


def gemini_week(usage):
    """The weekly bucket of the Gemini Models group, else of the first Gemini group, or None."""
    groups = [group for group in usage.get('groups') or [] if group['name'].casefold().startswith('gemini')]
    groups.sort(key=lambda group: group['name'].casefold() not in ('gemini models', 'gemini'))
    return next((bucket for group in groups for bucket in group['buckets'] if bucket['window'] == 'weekly'), None)


def lines(store):
    """(alias, mark, line) per account, active first: the Gemini weekly share left and when the week resets.

    The active marker shows only when there is a choice between accounts.
    """
    from .controls import _until
    from .health import _plain
    broker = AntigravityBroker(store)
    shown = []
    for alias in sorted(broker.accounts(), key=lambda name: (name != broker.active(), broker.label(name).casefold())):
        mark, state, usage = _state(store, broker, alias)
        week = gemini_week(usage)
        left = '%d%%' % round(week['remaining'] * 100) if week else '?'
        if mark == '🔴':
            until = broker.limited_until(alias)
            state = 'back in ' + _until(until) if until else 'back later'
        elif mark != '⚪' and week and week['resets_at'] and week['resets_at'] > time.time():
            state += (', ' if state else '') + 'week resets in ' + _until(week['resets_at'])
        name = _plain(broker.label(alias))
        line = f'{mark} {name}: {state}' if mark == '⚪' else f'{mark} {name}: {left}' + (', ' + state if state else '')
        shown.append((alias, mark, line + (' · active' if alias == broker.active() and len(broker.accounts()) > 1 else '')))
    return shown


def health(store):
    """The /health and /help section, or None when no account is registered."""
    shown = [line for _, _, line in lines(store)]
    return '\n'.join(['**Antigravity left** (week)'] + shown) if shown else None


def card(store, alias):
    """One account for /accounts."""
    from .controls import _timestamp, resets
    broker = AntigravityBroker(store)
    mark, state, usage = _state(store, broker, alias)
    status = (store.get('antigravity_account_status', {}) or {}).get(alias) or {}
    lines = ['Antigravity %s%s' % (broker.label(alias), ' · active' if alias == broker.active() else '')]
    if status.get('tier'):
        lines.append('Plan: ' + ' '.join(str(status['tier']).split())[:80])
    if mark == '⚪':
        lines.append('Not signed in.')
    for group in usage.get('groups') or []:
        for bucket in group['buckets']:
            lines.append('%s, %s: %d%% left; resets %s' % (
                group['name'], _window(bucket), round(bucket['remaining'] * 100),
                _timestamp(bucket['resets_at']) if bucket['resets_at'] else 'unknown'))
    if mark != '⚪' and not usage:
        lines.append('Usage has not been checked yet.')
    if mark == '🔴':
        lines.append('At its usage limit; ' + resets(broker.limited_until(alias)) + '.')
    return '\n'.join(lines)


def cards(store):
    return [card(store, alias) for alias in sorted(accounts(store))]


def accounts(store):
    return {alias: dict(profile) for alias, profile in (store.get('antigravity_accounts', {}) or {}).items()
            if isinstance(profile, dict)}


def record_check(store, alias, models):
    statuses = store.get('antigravity_account_status', {}) or {}
    status = dict(statuses.get(alias) or {})
    status.update(logged_in=models is not None, checked=time.time())
    if models is not None:
        status['models'] = models
    statuses[alias] = status
    store.put('antigravity_account_status', statuses)
    return status


def op_account(store, ctx, alias, label=None):
    """Register a profile folder that the owner signed in with the official CLI."""
    for key in ('accounts', 'codex_accounts', 'opencode_accounts'):
        if alias in (store.get(key, {}) or {}):
            raise Refused('That alias belongs to another provider. Choose a different Antigravity account alias.')
    account_home(alias)
    models = read_models(store, alias)
    if models is None:
        raise Refused('Antigravity account %s is not signed in. Sign in with the official CLI first.' % alias)
    profiles = accounts(store)
    previous = profiles.get(alias, {})
    profiles[alias] = dict(previous, label=label or previous.get('label') or alias,
                           created=previous.get('created', time.time()))
    store.put('antigravity_accounts', profiles)
    record_check(store, alias, models)
    if store.get('antigravity_active_account') not in profiles:
        store.put('antigravity_active_account', alias)
    return 'Antigravity account %s is registered with %d models.' % (alias, len(models))


def op_use(store, ctx, alias):
    if alias not in accounts(store):
        raise Refused('Unknown Antigravity account. Register it with antigravity.account first.')
    store.put('antigravity_active_account', alias)
    return 'Antigravity account %s selected for new work.' % alias


def offered(store, alias):
    """The model ids that the account's last model check listed."""
    status = (store.get('antigravity_account_status', {}) or {}).get(alias) or {}
    return {model['id'] for model in status.get('models') or [] if isinstance(model, dict) and isinstance(model.get('id'), str)}


def model_refusal(store, model):
    """Why a new worker cannot have this model, or None when a registered account offers it."""
    model = model or DEFAULT_MODEL
    if any(model in offered(store, alias) for alias in accounts(store)):
        return None
    models = sorted(set().union(*(offered(store, alias) for alias in accounts(store))))
    return ('No Antigravity account offers model %s. Offered: %s.' % (model, ', '.join(models) or 'none'))[:900]


def owner_settings(store):
    broker = AntigravityBroker(store)
    return dict(antigravity_enabled=store.get('antigravity_enabled', False) is True,
                antigravity_sandbox=store.get('antigravity_sandbox', False) is True,
                antigravity_active_account=broker.active())


class AntigravityBroker:
    """Choose the account for new work. A saved session stays on its own account."""

    def __init__(self, store):
        self.store = store

    def accounts(self):
        return accounts(self.store)

    def label(self, alias):
        value = self.accounts().get(alias, {}).get('label')
        return value if isinstance(value, str) and value else alias

    def signed_in(self, alias):
        status = (self.store.get('antigravity_account_status', {}) or {}).get(alias) or {}
        return alias in self.accounts() and status.get('logged_in') is True

    def active(self):
        alias = self.store.get('antigravity_active_account')
        return alias if alias in self.accounts() else None

    def limited_until(self, alias):
        """When a limited account has usage again, or None when it is not limited.

        A refused run sets a block. A usage read that shows every model group empty also limits.
        """
        now = time.time()
        block = (self.store.get('antigravity_account_blocks', {}) or {}).get(alias)
        status = (self.store.get('antigravity_account_status', {}) or {}).get(alias)
        times = [value for value in (block, exhausted_until(status))
                 if type(value) in (int, float) and value > now]
        return max(times) if times else None

    def held(self, alias):
        return self.limited_until(alias) is not None

    def usable(self, alias):
        return self.signed_in(alias) and not self.held(alias)

    def headroom(self, alias):
        status = (self.store.get('antigravity_account_status', {}) or {}).get(alias) or {}
        groups = (status.get('usage') or {}).get('groups') or []
        return max((min(bucket['remaining'] for bucket in group['buckets']) for group in groups), default=0)

    def selection_order(self, model=None):
        """The active account first, then the other usable accounts with the most usage left.

        With model, only the accounts that offer it.
        """
        active = self.active()
        others = sorted((alias for alias in self.accounts() if alias != active),
                        key=lambda alias: (-self.headroom(alias), alias))
        return [alias for alias in ([active] if active else []) + others
                if self.usable(alias) and (model is None or model in offered(self.store, alias))]

    def select(self, alias=None, model=None):
        """The account for new work. With alias, only that account: a saved session stays on it."""
        if alias is not None:
            return alias if self.usable(alias) else None
        order = self.selection_order(model)
        return order[0] if order else None

    def default_model(self):
        return DEFAULT_MODEL

    def capacity(self, model):
        """Any usable account. The model offer is checked by the spawn refusal, not here."""
        return self.select() is not None

    def earliest_reset(self, alias=None):
        times = [self.limited_until(name) for name in ([alias] if alias else self.accounts())
                 if self.signed_in(name)]
        times = [value for value in times if value is not None]
        return min(times) if times else None

    @staticmethod
    def _pinned(worker):
        """The account a waiting worker must use: its own account once it has a session."""
        return worker['account_alias'] if worker['session'] and not worker['fresh'] else None

    async def ready(self, worker):
        return self.select(self._pinned(worker), model=worker['model'] or DEFAULT_MODEL) is not None

    def reset(self, worker):
        return self.earliest_reset(self._pinned(worker))

    async def record_result(self, alias, result, worker_id=None):
        """Save the model that ran. After a usage refusal, read usage and hold the account until it has usage again."""
        model = (result.spend or {}).get('model') if isinstance(result.spend, dict) else None
        if worker_id is not None and isinstance(model, str):
            with self.store.db:
                self.store.db.execute('UPDATE workers SET model=? WHERE id=?', (model, worker_id))
        if alias is None:
            return
        if result.success:
            blocks = self.store.get('antigravity_account_blocks', {}) or {}
            if alias in blocks:
                with self.store.db:
                    del blocks[alias]
                    self.store.put('antigravity_account_blocks', blocks)
            return
        if not result.quota_limited:
            return
        status = await refresh_usage(self.store, alias)
        now = time.time()
        resets = [bucket['resets_at'] for group in (status.get('usage') or {}).get('groups') or []
                  for bucket in empty_buckets(group) if bucket['resets_at']]
        until = min(resets, default=now + RECHECK_SECONDS)
        with self.store.db:
            blocks = self.store.get('antigravity_account_blocks', {}) or {}
            blocks[alias] = until
            self.store.put('antigravity_account_blocks', blocks)
        result.rate_limit_info = {'status': 'rejected', 'resets_at': until}

    def run_ready(self, alias=None, model=None):
        """Return the account for this run, or refuse. A saved session passes its own account."""
        if self.store.get('antigravity_enabled', False) is not True:
            raise Refused('Antigravity workers are off.')
        if alias is None:
            alias = self.select(model=model)
            if alias is None:
                raise Refused('No Antigravity account is signed in and available.' if model is None else
                              'No Antigravity account that offers %s is signed in and available.' % model)
        elif not self.signed_in(alias):
            raise Refused('Antigravity account %s is not signed in.' % self.label(alias))
        account_home(alias)
        return alias

    async def run(self, run, provider, prompt, cwd, session_id, *, notice_topic=None, on_account=None, **options):
        from .providers import RunResult
        saved = options.pop('account_alias', None)
        fresh = options.get('fresh')
        try:
            alias = self.run_ready(None if fresh else saved, options.get('model'))
        except Refused as error:
            code = 'account_changed' if not fresh and saved else 'accounts_unavailable'
            return RunResult(session_id, failure_code=code, error=str(error))
        if not fresh and saved and self.held(alias):
            return RunResult(session_id, quota_limited=True, failure_code='quota_limited',
                             error='Antigravity account %s is at its usage limit.' % self.label(alias))
        if on_account:
            on_account(alias)
        result = await run(provider, prompt, cwd, session_id, account_alias=alias, **options)
        await self.record_result(alias, result, options.get('worker_id'))
        return result
