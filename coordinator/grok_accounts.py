"""Manually selected Grok homes and short-lived ACP account checks."""

import asyncio
from datetime import datetime
import json
import os
from pathlib import Path
import re
import signal
import tempfile
import time
import uuid

from . import host_os, problems
from .account_status import identity_label
from .accounts import _dict, _number
from .providers import RunResult, child_environment


REFRESH_SECONDS = 300
WINDOWS = ('seven_day', 'limit')
NEEDS_PLAN = 'needs SuperGrok or X Premium+'
INITIALIZE = {'protocolVersion': 1,
              'clientCapabilities': {'fs': {'readTextFile': False, 'writeTextFile': False}, 'terminal': False},
              'clientInfo': {'name': 'torii', 'version': '1'}}
CONFIG = {
    'cli': {'auto_update': 'false', 'use_leader': 'false'},
    'auth': {'preferred_method': '"oidc"', 'login_device_flow': 'true'},
    'features': {'telemetry': 'false', 'ask_user_question': 'false',
                 'auto_wake': 'false # Best effort; background completion or queued input can still start turns.'},
    'telemetry': {'trace_upload': 'false', 'otel_enabled': 'false'},
    'claude_compat': {'imported': 'true # Folder trust also covers repo MCP/LSP, hooks, permissions, instructions and skills.'},
    **{'compat.' + provider: {name: 'false' for name in ('skills', 'rules', 'agents', 'mcps', 'hooks', 'sessions')}
       for provider in ('claude', 'cursor', 'codex')},
}


def accounts_root():
    root = Path.home() / '.grok-accounts'
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    root.chmod(0o700)
    return root


def clear_home(home):
    """Trash only a Torii pre-login home containing its config and nothing else."""
    home = Path(home)
    if (home.is_symlink() or not home.is_dir() or home.parent.resolve() != accounts_root().resolve()
            or not re.fullmatch(r'\.torii-[0-9a-f]{8}', home.name)):
        return
    entries = list(home.iterdir())
    if any(entry.name != 'config.toml' or entry.is_symlink() or not entry.is_file() for entry in entries):
        return
    host_os.prepare_trash()
    trash = host_os.trash_directory()
    target = trash / home.name
    if target.exists() or target.is_symlink():
        target = trash / (home.name + '-' + uuid.uuid4().hex)
    home.rename(target)


def grok_environment(home):
    """Keep HOME, drop inherited Grok and xAI credentials, and use only the named Grok home."""
    env = {key: value for key, value in child_environment().items()
           if not key.upper().startswith(('GROK_', 'XAI_'))}
    env.update(GROK_HOME=str(home), GROK_DISABLE_AUTOUPDATER='1', GROK_LOGIN_DEVICE_FLOW='1')
    return env


def write_config(home):
    """Merge Torii's keys atomically while preserving installer and other existing settings.

    Folder trust also covers repo-local MCP/LSP, hooks, permissions, instructions and skills.
    These account settings do not grant folder trust.
    """
    home = Path(home)
    home.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = home / 'config.toml'
    text = path.read_text() if path.exists() else ''
    lines = text.splitlines()
    section = None
    remaining = {name: dict(keys) for name, keys in CONFIG.items()}
    output = []

    def finish():
        if section in remaining:
            output.extend(key + ' = ' + value for key, value in remaining.pop(section).items())

    for line in lines:
        header = re.fullmatch(r'\s*\[([^\[\]]+)\]\s*(?:#.*)?', line)
        if header:
            finish()
            section = header.group(1).strip()
        key = re.match(r'\s*([A-Za-z0-9_-]+)\s*=', line)
        if section in CONFIG and key and key.group(1) in CONFIG[section]:
            name = key.group(1)
            line = name + ' = ' + CONFIG[section][name]
            remaining.get(section, {}).pop(name, None)
        output.append(line)
    finish()
    for section, keys in remaining.items():
        if output and output[-1]:
            output.append('')
        output.append('[' + section + ']')
        output.extend(key + ' = ' + value for key, value in keys.items())
    rendered = '\n'.join(output) + '\n'
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=home, prefix='.config-', delete=False) as temp:
        os.fchmod(temp.fileno(), 0o600)
        temp.write(rendered)
        temp.flush()
        os.fsync(temp.fileno())
        temporary = temp.name
    os.replace(temporary, path)


def plan_label(tier):
    return tier.strip() if isinstance(tier, str) and tier.strip() else 'Grok (plan unknown)'


class AcpError(Exception):
    def __init__(self, error):
        self.code = error.get('code')
        self.data = error.get('data')
        self.message = error.get('message')
        super().__init__('Grok ACP request failed')


class AcpClient:
    """One ACP process with a single reader and independently awaited JSON-RPC requests."""

    def __init__(self, binary, env):
        self.binary = binary
        self.env = env
        self.process = None
        self.reader = None
        self.counter = 0
        self.pending = {}
        self.notifications = []
        self.failure = None

    async def start(self, cwd=None):
        self.process = await asyncio.create_subprocess_exec(
            self.binary, '--no-auto-update', 'agent', '--no-leader', 'stdio', env=self.env,
            cwd=cwd or self.env['GROK_HOME'], start_new_session=True,
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            limit=1024 * 1024)
        self.reader = asyncio.create_task(self._read())
        return self

    async def send(self, message):
        self.process.stdin.write((json.dumps(dict(message, jsonrpc='2.0')) + '\n').encode())
        await self.process.stdin.drain()

    async def _read(self):
        try:
            while True:
                line = await self.process.stdout.readline()
                if not line:
                    raise ValueError('The Grok ACP process stopped')
                message = json.loads(line)
                if not isinstance(message, dict) or message.get('jsonrpc') != '2.0':
                    raise ValueError('The Grok ACP process sent an unexpected message')
                if 'method' in message:
                    if 'id' in message:
                        await self.send({'id': message['id'], 'error': {'code': -32601, 'message': 'Method not found'}})
                    else:
                        self.notifications.append(message)
                    continue
                future = self.pending.get(message.get('id'))
                if future is not None and not future.done():
                    if 'error' in message:
                        future.set_exception(AcpError(_dict(message['error'])))
                    else:
                        future.set_result(_dict(message.get('result')))
        except (OSError, ValueError, TypeError) as error:
            self.failure = error
            for future in self.pending.values():
                if not future.done():
                    future.set_exception(error)

    async def request(self, method, params=None):
        if self.failure:
            raise self.failure
        self.counter += 1
        number = self.counter
        future = asyncio.get_running_loop().create_future()
        self.pending[number] = future
        try:
            await self.send({'id': number, 'method': method, 'params': params or {}})
            return await future
        finally:
            self.pending.pop(number, None)

    async def stop(self):
        if self.process is None:
            return
        self.process.stdin.close()
        for sig, seconds in ((None, 0.5), (signal.SIGTERM, 2), (signal.SIGKILL, 2)):
            if sig is not None:
                try:
                    os.killpg(self.process.pid, sig)
                except ProcessLookupError:
                    pass
            try:
                await asyncio.wait_for(self.process.wait(), seconds)
                break
            except asyncio.TimeoutError:
                continue
        if self.reader is not None:
            self.reader.cancel()
            await asyncio.gather(self.reader, return_exceptions=True)
        for future in self.pending.values():
            if not future.done():
                future.set_exception(ValueError('The Grok ACP process stopped'))


def _reset(value):
    if not isinstance(value, str) or not re.fullmatch(
            r'\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[Zz]|[+-]\d{2}:\d{2})', value):
        return None
    try:
        parsed = datetime.fromisoformat(value.upper().replace('Z', '+00:00'))
        return parsed.timestamp() if parsed.tzinfo is not None else None
    except (ValueError, OverflowError, OSError):
        return None


def _identity(auth, info, billing):
    meta = _dict(auth.get('_meta'))
    allowed = meta.get('gate') is None and not info.get('userBlockedReason') and not info.get('teamBlockedReasons')
    tier = billing.get('subscription_tier') or meta.get('subscription_tier')
    return {'identity': {'email': identity_label(meta.get('email') or info.get('email')), 'logged_in': True,
                         'type': plan_label(tier) if allowed else NEEDS_PLAN},
            'usage_allowed': bool(allowed), 'gate': meta.get('gate'),
            'userBlockedReason': info.get('userBlockedReason'), 'teamBlockedReasons': info.get('teamBlockedReasons') or []}


def _status(auth, info, billing):
    """Billing percentages are used on a 0-100 scale. onDemandCap.val stays in USD cents."""
    config = _dict(billing.get('config'))
    period = _dict(config.get('currentPeriod'))
    window = {}
    used = config.get('creditUsagePercent')
    if _number(used):
        window['utilization'] = used
    reset = _reset(period.get('end'))
    if reset is not None:
        window['resets_at'] = reset
    return {**_identity(auth, info, billing), 'observed_at': time.time(),
            'usage': {'seven_day' if period.get('type') == 'USAGE_PERIOD_TYPE_WEEKLY' else 'limit': window},
            'on_demand_enabled': billing.get('on_demand_enabled'), 'onDemandCap': config.get('onDemandCap')}


async def collect_grok_status(account, binary='grok', timeout=30):
    """Authenticate cached login and read display-only identity and billing, without a model turn."""
    result = {'checked_at': time.time()}
    home = account.get('config_dir')
    if not home or not Path(home).is_dir():
        return dict(result, error='login_required')
    client = None
    try:
        write_config(home)
        client = AcpClient(binary, grok_environment(home))

        async def check():
            await client.start()
            await client.request('initialize', INITIALIZE)
            try:
                auth = await client.request('authenticate', {'methodId': 'cached_token'})
            except AcpError as error:
                if error.code == -32000 and error.message == 'Authentication required':
                    return dict(result, identity={'logged_in': False}, error='login_required')
                raise
            result.update(_identity(auth, {}, {}))
            info = await client.request('_x.ai/auth/info')
            result.update(_identity(auth, info, {}))
            billing = await client.request('_x.ai/billing')
            return dict(result, **_status(auth, info, billing))
        return await asyncio.wait_for(check(), timeout)
    except FileNotFoundError:
        return dict(result, error='cli_unavailable')
    except asyncio.TimeoutError:
        return dict(result, error='timeout')
    except (AcpError, OSError, ValueError, TypeError, KeyError, AttributeError):
        return dict(result, error='usage_unavailable')
    finally:
        if client is not None:
            await client.stop()


async def refresh_grok_accounts(store, collect=None):
    if store.get('grok_enabled', False) is not True:
        return False
    collect = collect or collect_grok_status
    failed = False
    broker = GrokBroker(store)
    for alias, account in list(broker.accounts().items()):
        if store.get('grok_enabled', False) is not True:
            break
        if not account.get('enabled') and not account.get('awaiting_login'):
            continue
        snapshot = await collect(account)
        failed = failed or 'error' in snapshot
        with store.db:
            if broker.accounts().get(alias) != account:
                continue
            snapshots = _dict(store.get('grok_account_status', {}))
            previous = _dict(snapshots.get(alias))
            if snapshot.get('error') == 'login_required':
                snapshot['identity'] = {**_dict(previous.get('identity')), **_dict(snapshot.get('identity')),
                                        'logged_in': False}
            if 'identity' in snapshot and snapshot['identity'] != previous.get('identity'):
                previous = {}
            if 'error' in snapshot and snapshot['error'] != previous.get('error'):
                problems.record(store, 'accounts', 'grok-status-' + snapshot['error'].replace('_', '-'))
            snapshots[alias] = {**previous, **snapshot}
            if 'error' not in snapshot:
                snapshots[alias].pop('error', None)
            store.put('grok_account_status', snapshots)
            if account.get('awaiting_login') and broker.authenticated(alias):
                accounts = broker.accounts()
                accounts[alias] = dict(account, enabled=True)
                accounts[alias].pop('awaiting_login', None)
                store.put('grok_accounts', accounts)
    return failed


async def monitor_grok_accounts(store, binary='grok'):
    next_refresh = 0
    completed_request = 0
    while store.get('grok_enabled', False) is True and GrokBroker(store).accounts():
        request = store.get('grok_account_status_refresh_requested', 0) or 0
        if time.monotonic() >= next_refresh or request > completed_request:
            await refresh_grok_accounts(store, lambda account: collect_grok_status(account, binary))
            completed_request = request
            next_refresh = time.monotonic() + REFRESH_SECONDS
        await asyncio.sleep(2)


class GrokBroker:
    """Grok uses only the owner's manual pick, with no discovery or automatic switching."""

    FRESH_SECONDS = 600
    REJECTION_HOLD_SECONDS = 300

    def __init__(self, store):
        self.store = store

    def accounts(self):
        return {alias: dict(profile) for alias, profile in _dict(self.store.get('grok_accounts', {})).items()
                if isinstance(profile, dict)}

    def snapshot(self, alias):
        return _dict(_dict(self.store.get('grok_account_status', {})).get(alias))

    def label(self, alias):
        return identity_label(_dict(self.snapshot(alias).get('identity')).get('email')) or alias

    def authenticated(self, alias):
        snapshot = self.snapshot(alias)
        return (alias in self.accounts() and _dict(snapshot.get('identity')).get('logged_in') is True
                and snapshot.get('error') != 'login_required')

    def signed_in(self, alias):
        return (_dict(self.accounts().get(alias)).get('enabled') is True and self.authenticated(alias)
                and self.snapshot(alias).get('usage_allowed') is not False)

    def automatic(self):
        return False

    def active(self):
        alias = self.store.get('grok_active_account')
        return alias if alias in self.accounts() else None

    def environment(self, alias):
        home = _dict(self.accounts().get(alias)).get('config_dir')
        if not home:
            raise ValueError('Grok account home is unavailable')
        return grok_environment(home)

    def alias_for_environment(self, env):
        home = env.get('GROK_HOME')
        target = Path(home).resolve() if home else None
        return next((alias for alias, profile in self.accounts().items()
                     if profile.get('config_dir') and Path(profile['config_dir']).resolve() == target), None)

    def blocks(self):
        saved = _dict(self.store.get('grok_account_blocks', {}))
        kept = {alias: block for alias, block in saved.items() if isinstance(block, dict)
                and (block.get('until') is None or _number(block.get('until')) and block['until'] > time.time())}
        for alias, block in list(kept.items()):
            snapshot = self.snapshot(alias)
            if (block.get('until') is None and _number(snapshot.get('observed_at'))
                    and snapshot['observed_at'] > (block.get('observed_at') or 0)
                    and not snapshot.get('error') and snapshot.get('usage_allowed') is not False
                    and any(_number(window.get('utilization')) and window['utilization'] < 100
                            for window in self._windows(alias))):
                kept.pop(alias)
        if kept != saved:
            outer = self.store.db.in_transaction
            self.store.put('grok_account_blocks', kept)
            if not outer:
                self.store.db.commit()
        return kept

    def _windows(self, alias):
        usage = _dict(self.snapshot(alias).get('usage'))
        return [_dict(usage.get(key)) for key in WINDOWS]

    def _full(self, alias):
        return [window for window in self._windows(alias)
                if _number(window.get('utilization')) and window['utilization'] >= 100
                and not (_number(window.get('resets_at')) and window['resets_at'] <= time.time())]

    def exhausted(self, alias):
        return bool(self.blocks().get(alias) or self._full(alias))

    def account_state(self, alias):
        block = self.blocks().get(alias)
        full = self._full(alias)
        if block or full or self.snapshot(alias).get('usage_allowed') is False:
            resets = [window['resets_at'] for window in full if _number(window.get('resets_at'))]
            return {'state': 'limited', 'until': block.get('until') if block else max(resets) if resets else None}
        snapshot = self.snapshot(alias)
        observed = snapshot.get('observed_at')
        if not _number(observed) or snapshot.get('error') or time.time() - observed > self.FRESH_SECONDS:
            return {'state': 'stale', 'since': observed}
        return {'state': 'available'}

    def select(self):
        alias = self.active()
        return alias if alias and self.signed_in(alias) and not self.exhausted(alias) else None

    def record_rate_limit(self, alias, info, rejected=False):
        if alias not in self.accounts():
            return
        self.store.put('grok_account_status_refresh_requested', time.time())
        full = self._full(alias)
        if not rejected and not full:
            return
        resets = [window['resets_at'] for window in full if _number(window.get('resets_at'))]
        blocks = self.blocks()
        blocks[alias] = {'until': max(resets) if resets else time.time() + self.REJECTION_HOLD_SECONDS,
                         'reason': 'quota', 'observed_at': time.time()}
        self.store.put('grok_account_blocks', blocks)

    async def run_active(self, run, provider, prompt, cwd, session_id, notice_topic=None, on_account=None,
                         alias=None, **options):
        if self.store.get('grok_enabled', False) is not True:
            return RunResult(session_id, error='Grok workers are off.', failure_code='accounts_unavailable')
        alias = alias if alias is not None else self.active()
        if not alias or not self.signed_in(alias):
            return RunResult(session_id, error='The chosen Grok account is signed out, disabled or ' + NEEDS_PLAN + '.',
                             failure_code='accounts_unavailable')
        if self.exhausted(alias):
            return RunResult(session_id, error='The chosen Grok account is at its usage limit.',
                             quota_limited=True, failure_code='quota_limited')
        if on_account:
            on_account(alias)
        result = await run(provider, prompt, cwd, session_id, account_alias=alias, **options)
        with self.store.db:
            self.record_rate_limit(alias, result.rate_limit_info, result.quota_limited and not result.success)
        return result
