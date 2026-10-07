"""Read native Claude account usage without a model turn or token access."""

import asyncio
from datetime import datetime
import json
import math
from pathlib import Path
import subprocess
import sys
import time

from . import problems
from .accounts import AccountBroker, account_label, identity_email
from . import extension


REFRESH_SECONDS = 300
ACTIVE_REFRESH_SECONDS = 60
REFRESH_RETRY_SECONDS = 30
REFRESH_RETRY_MAX_SECONDS = 300
SAME_WINDOW_SECONDS = 300
USAGE_CACHE_SECONDS = 3600


def identity_label(value):
    return ' '.join(value.split())[:120] if isinstance(value, str) else ''


def normalize_usage(limits):
    """Native usage percentages are 0–100, not the stream event's 0–1."""
    if not isinstance(limits, dict):
        return {}
    candidates = {key: limits.get(key) for key in ('five_hour', 'seven_day')}
    scoped = limits.get('model_scoped')
    if isinstance(scoped, list):
        for window in scoped:
            if isinstance(window, dict) and str(window.get('display_name', '')).lower() == 'fable':
                candidates['seven_day_fable'] = window
    result = {}
    for key, window in candidates.items():
        if not isinstance(window, dict):
            continue
        used = window.get('utilization')
        if type(used) not in (int, float) or not math.isfinite(used) or not 0 <= used <= 100:
            continue
        result[key] = {'utilization': used}
        reset = window.get('resets_at')
        if isinstance(reset, str):
            try:
                parsed = datetime.fromisoformat(reset.replace('Z', '+00:00'))
                if parsed.tzinfo:
                    result[key]['resets_at'] = parsed.timestamp()
            except (ValueError, OverflowError, OSError):
                pass
    return result


async def _stop(process):
    if process.returncode is None:
        try:
            process.terminate()
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(process.wait(), 2)
        except asyncio.TimeoutError:
            try:
                process.kill()
            except ProcessLookupError:
                pass
            await process.wait()


async def _native(command, env, timeout, request=None, input_data=None):
    process = await asyncio.create_subprocess_exec(
        *command, env=env, cwd=AccountBroker.working_directory(env), stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL, limit=1024 * 1024)
    try:
        if input_data is not None:
            output, _ = await asyncio.wait_for(
                process.communicate(input_data.encode()), timeout)
            if process.returncode:
                raise ValueError('Native command failed')
            return json.loads(output)
        if request is None:
            output, _ = await asyncio.wait_for(process.communicate(), timeout)
            return json.loads(output)

        async def read_response():
            process.stdin.write((json.dumps(request) + '\n').encode())
            await process.stdin.drain()
            while True:
                line = await process.stdout.readline()
                if not line:
                    raise ValueError('No native usage response')
                event = json.loads(line)
                response = event.get('response', {})
                if event.get('type') == 'control_response' and response.get('request_id') == request['request_id']:
                    if response.get('subtype') != 'success':
                        raise ValueError('Native usage is unsupported')
                    return response['response']
        return await asyncio.wait_for(read_response(), timeout)
    finally:
        await _stop(process)


async def collect_status(account, binary='claude', timeout=20):
    result = {'checked_at': time.time()}
    directory = account.get('config_dir')
    if not directory or not Path(directory).is_dir():
        return dict(result, error='login_required')
    env = AccountBroker.profile_environment(account)
    try:
        identity = await _native([binary, 'auth', 'status'], env, timeout)
        plan = identity.get('subscriptionType')
        result['identity'] = {'name': identity_label(identity.get('orgName')),
                              'email': identity_label(identity.get('email')),
                              'type': 'Claude ' + plan.title() if plan in ('max', 'pro', 'team', 'enterprise') else 'Claude (plan unknown)',
                              'logged_in': identity.get('loggedIn') is True}
        if not result['identity']['logged_in']:
            return dict(result, error='login_required')
        response = await _native([binary, '--safe-mode', '--print', '--input-format', 'stream-json',
                                  '--output-format', 'stream-json', '--verbose', '--no-session-persistence', '--tools', ''],
                                 env, timeout, {'type': 'control_request', 'request_id': 'account-usage',
                                                'request': {'subtype': 'get_usage', 'skip_behaviors': True}})
        usage = normalize_usage(response.get('rate_limits'))
        if response.get('rate_limits_available') is not True or not usage:
            return dict(result, error='usage_unavailable')
        result['usage'] = usage
        result['observed_at'] = time.time()
        return result
    except FileNotFoundError:
        return dict(result, error='cli_unavailable')
    except asyncio.TimeoutError:
        return dict(result, error='timeout')
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        return dict(result, error='usage_unavailable')


def blocked_windows(store, alias):
    block = AccountBroker(store).blocks().get(alias)
    return {key: (block or {}).get('until') for key in ('five_hour', 'seven_day', 'seven_day_fable')} if block else {}


def keep_highest(usage, previous, observed_at=None):
    """Usage only grows inside one window, so a lower read of the same window within an hour is
    Claude's cached snapshot, which it serves for up to an hour after a failed fetch. The higher
    value stays for that hour. A lower read after it is a real drop, such as a larger plan. Windows only
    move forward, so a read of an earlier window is also a cached snapshot, and the stored window stays."""
    usage = dict(usage)
    for key, old in previous.items():
        window = usage.get(key)
        if not isinstance(old, dict) or not isinstance(window, dict):
            continue
        before, after = old.get('resets_at'), window.get('resets_at')
        timed = isinstance(before, (int, float)) and isinstance(after, (int, float))
        if timed and after <= before - SAME_WINDOW_SECONDS:
            usage[key] = old
            continue
        same = timed and abs(before - after) < SAME_WINDOW_SECONDS
        seen = old.get('recorded_at', observed_at)
        recent = isinstance(seen, (int, float)) and time.time() - seen < USAGE_CACHE_SECONDS
        if same and recent and isinstance(old.get('utilization'), (int, float)) and \
                old['utilization'] > window.get('utilization', 0):
            usage[key] = dict(window, utilization=old['utilization'], recorded_at=seen)
    return usage


def hold_blocked(usage, previous, windows):
    """A blocked window keeps its last known value and reset; a fresh read can report 0 while the limit holds."""
    usage = dict(usage)
    for key, until in windows.items():
        old = previous.get(key) if isinstance(previous.get(key), dict) else {}
        window = dict(usage.get(key) or {})
        current = not (isinstance(old.get('resets_at'), (int, float)) and old['resets_at'] <= time.time())
        if current and old.get('utilization', 0) > window.get('utilization', 0):
            window.update({name: old[name] for name in ('utilization', 'resets_at') if name in old})
        if until and not window.get('resets_at'):
            window['resets_at'] = until
        window['blocked'] = True
        usage[key] = window
    return usage


async def refresh_accounts(store, collect=collect_status, aliases=None):
    """Update allowlisted snapshots. A failed refresh keeps clearly dated prior usage."""
    failed = False
    for alias, account in list(store.get('accounts', {}).items()):
        if not account.get('enabled', True) and not account.get('awaiting_login'):
            continue
        if aliases is not None and alias not in aliases:
            continue
        snapshot = await collect(account)
        if (account.get('awaiting_login') and (snapshot.get('identity') or {}).get('logged_in') is True
                and not await extension.active().login_ready(account['config_dir'])):
            snapshot = dict(snapshot, identity=dict(snapshot['identity'], logged_in=False), error='login_required')
        failed = failed or ('error' in snapshot and account.get('enabled') is True)
        with store.db:
            if store.get('accounts', {}).get(alias) != account:
                continue
            if snapshot.get('error') == 'login_required' and 'identity' not in snapshot:
                snapshot['identity'] = {'logged_in': False}
            snapshots = store.get('account_status', {})
            previous = snapshots.get(alias, {})
            identity = snapshot.get('identity') or {}
            previous_identity = previous.get('identity') or {}
            if ('identity' in snapshot and identity.get('logged_in') is not True
                    and not identity_email(identity) and identity_email(previous_identity)):
                snapshot['identity'] = {key: previous_identity[key] for key in ('email', 'type', 'name')
                                        if key in previous_identity}
                snapshot['identity']['logged_in'] = False
            if 'identity' in snapshot and snapshot['identity'] != previous.get('identity'):
                previous = {}
            if 'usage' in snapshot:
                snapshot['usage'] = hold_blocked(keep_highest(snapshot['usage'], previous.get('usage') or {},
                                                              previous.get('observed_at')),
                                                 previous.get('usage') or {}, blocked_windows(store, alias))
            if 'error' in snapshot and snapshot['error'] != previous.get('error'):
                problems.record(store, 'accounts', 'status-' + snapshot['error'].replace('_', '-'),
                                'account=' + account_label(store, alias))
            snapshots[alias] = {**previous, **snapshot}
            if 'error' not in snapshot:
                snapshots[alias].pop('error', None)
            store.put('account_status', snapshots)
            AccountBroker(store).log_crossings(alias, previous, snapshots[alias])
            if account.get('awaiting_login'):
                identity = snapshots[alias].get('identity') or {}
                if (identity.get('logged_in') is True and identity_email(identity)
                        and snapshots[alias].get('error') != 'login_required'):
                    accounts = dict(store.get('accounts', {}))
                    accounts[alias] = dict(account, enabled=True)
                    accounts[alias].pop('awaiting_login', None)
                    store.put('accounts', accounts)
    return failed


async def check_accounts(store):
    """Run one deliberate setup pass, allowing time for native permission dialogs."""
    async def collect(account):
        return await collect_status(account, timeout=180)
    await refresh_accounts(store, collect)
    accounts = store.get('accounts', {})
    snapshots = store.get('account_status', {})
    return {alias: (snapshots[alias].get('error') or 'ready') if alias in snapshots else 'not_checked'
            for alias, account in accounts.items() if account.get('enabled', True)}


def _next_check(interval, retry, failures):
    return time.monotonic() + (min(REFRESH_RETRY_MAX_SECONDS, retry * 2 ** (failures - 1)) if failures else interval)


async def monitor_accounts(store, on_refresh=None):
    """Probe every account each five minutes and the last selected account each minute; each probe starts Claude twice."""
    next_refresh = 0
    next_active = 0
    completed_request = 0
    failures = 0
    active_failures = 0
    while True:
        request = store.get('account_status_refresh_requested', 0)
        active = store.get('active_account')
        if time.monotonic() >= next_refresh or request > completed_request:
            failures = failures + 1 if await refresh_accounts(store) else 0
            if on_refresh:
                on_refresh()
            completed_request = request
            next_refresh = _next_check(REFRESH_SECONDS, REFRESH_RETRY_SECONDS, failures)
            next_active = _next_check(ACTIVE_REFRESH_SECONDS, ACTIVE_REFRESH_SECONDS, 0)
        elif active and time.monotonic() >= next_active:
            active_failures = active_failures + 1 if await refresh_accounts(store, aliases=[active]) else 0
            if on_refresh:
                on_refresh()
            next_active = _next_check(ACTIVE_REFRESH_SECONDS, ACTIVE_REFRESH_SECONDS, active_failures)
        await asyncio.sleep(2)
