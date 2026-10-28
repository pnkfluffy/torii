"""One OpenCode subscription, its usage windows and bounded quota holds."""

import asyncio
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from http.client import HTTPException
import json
import math
import os
import stat
import time
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, Request, build_opener

from .control_api import Refused


USAGE_URL = 'https://opencode.ai/zen/go/v1/usage'
WINDOWS = {'rolling': '5-hour', 'weekly': 'weekly', 'monthly': 'monthly'}
RECHECK_SECONDS = 300
HTTP_TIMEOUT = 15
MAX_RESET_SECONDS = 32 * 86400
AUTH_ERRORS = ('key_invalid', 'subscription_required')
ERRORS = {'key_invalid': 'OpenCode subscription key is invalid.',
          'subscription_required': 'OpenCode subscription required.',
          'key_file': 'OpenCode subscription key file is missing, unreadable or unsafe.',
          'usage_unavailable': 'OpenCode subscription usage is unavailable; Torii will check again.'}


def is_subscription(account):
    return account.get('provider') == 'opencode-go'


def accounts(store):
    from . import opencode_accounts
    return ({alias: account for alias, account in opencode_accounts.accounts(store).items() if is_subscription(account)}
            if store.get('opencode_enabled', False) is True else {})


def number(value):
    return type(value) in (int, float) and math.isfinite(value)


def bounded_reset(value):
    return min(value, time.time() + MAX_RESET_SECONDS) if number(value) else None


def reset_time(value):
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return bounded_reset(parsed.timestamp()) if parsed.tzinfo is not None else None
    except (ValueError, OverflowError, OSError):
        return None


def parse_usage(payload):
    result = {}
    for name in WINDOWS:
        window = payload['usage'][name]
        used = window['percent']
        if window['status'] not in ('ok', 'rate-limited') or not number(used) or used < 0:
            raise ValueError('Invalid subscription usage window')
        result[name] = {'status': window['status'], 'utilization': used,
                        'resets_at': reset_time(window.get('resetsAt'))}
    return result


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def collect_status(alias):
    from .opencode_accounts import MAX_KEY_BYTES, PINNED_VERSION, account_home, check_key
    snapshot = {'observed_at': time.time()}
    try:
        check_key(alias)
        with os.fdopen(os.open(account_home(alias) / 'key', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), 'rb') as handle:
            info = os.fstat(handle.fileno())
            if (not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600
                    or info.st_uid != os.getuid() or not 0 < info.st_size <= MAX_KEY_BYTES):
                raise Refused('Unsafe key file')
            key = handle.read(MAX_KEY_BYTES + 1).decode().strip()
        if not key or '\r' in key or '\n' in key:
            raise Refused('Invalid key file')
    except (Refused, OSError, ValueError):
        return dict(snapshot, error='key_file')
    try:
        request = Request(USAGE_URL, headers={'Authorization': 'Bearer ' + key, 'Accept': 'application/json',
                                            'User-Agent': 'opencode/' + PINNED_VERSION})
        with build_opener(NoRedirect()).open(request, timeout=HTTP_TIMEOUT) as response:
            payload = json.loads(response.read(65537))
        return dict(snapshot, usage=parse_usage(payload))
    except HTTPError as error:
        code = 'key_invalid' if error.code == 401 else 'usage_unavailable'
        with error:
            if error.code == 403:
                try:
                    payload = json.loads(error.read(65537))
                    if (isinstance(payload, dict) and payload.get('type') == 'error'
                            and isinstance(payload.get('error'), dict)
                            and payload['error'].get('type') == 'EntitlementError'):
                        code = 'subscription_required'
                except (OSError, HTTPException, ValueError, TypeError, RecursionError):
                    pass
        return dict(snapshot, error=code)
    except (URLError, OSError, HTTPException, ValueError, TypeError, KeyError, OverflowError, RecursionError):
        return dict(snapshot, error='usage_unavailable')


def limited_windows(snapshot):
    return [window for window in snapshot.get('usage', {}).values()
            if window['status'] == 'rate-limited' or window['utilization'] >= 100]


def request_refresh(store, alias):
    snapshot = (store.get('opencode_account_status', {}) or {}).get(alias, {})
    requested = store.get('opencode_account_status_refresh_requested', 0) or 0
    if requested <= snapshot.get('observed_at', 0):
        store.put('opencode_account_status_refresh_requested', time.time())


def account_block(store, alias, worker_id=None):
    block = (store.get('opencode_account_blocks', {}) or {}).get(alias) or {}
    if block.get('retry_seconds') and (worker_id is None or block.get('worker_id') != worker_id):
        shared = block.get('account_until', block['until'] if block.get('reset_known') else
                           block['observed_at'] + RECHECK_SECONDS)
        return dict(block, until=shared)
    return block


def refusal_block(previous, reason, reset=None):
    now = time.time()
    known = number(reset) and reset > now
    delay = min(max(RECHECK_SECONDS, previous.get('retry_seconds', 0) * 2), 3600)
    return {'until': max(reset, now + RECHECK_SECONDS) if known else now + delay,
            'observed_at': now, 'reason': reason, 'reset_known': known,
            'retry_seconds': RECHECK_SECONDS if known else delay}


def record_limit(store, alias, limits=None, request_refresh=True, reason='quota', *, backoff=False,
                 previous=None, worker_id=None):
    if alias not in accounts(store):
        return
    now = time.time()
    snapshot = (store.get('opencode_account_status', {}) or {}).get(alias, {})
    resets = [window.get('resets_at') for window in limited_windows(snapshot)]
    resets.append((limits or {}).get('retry_at'))
    resets = [bounded_reset(value) for value in resets if number(value) and value > now]
    blocks = store.get('opencode_account_blocks', {}) or {}
    current = account_block(store, alias)
    if current.get('reset_known') and current.get('until', 0) > now:
        resets.append(bounded_reset(current['until']))
    blocks[alias] = (refusal_block(previous if previous is not None else blocks.get(alias, {}), reason,
                                  max(resets) if resets else None) if backoff else
                    {'until': max(resets) if resets else now + RECHECK_SECONDS,
                     'observed_at': now, 'reason': reason, 'reset_known': bool(resets)})
    if backoff:
        blocks[alias].update(worker_id=worker_id, account_until=max(resets + [now + RECHECK_SECONDS]))
    store.put('opencode_account_blocks', blocks)
    if request_refresh:
        store.put('opencode_account_status_refresh_requested', now)


def record_unavailable(store, alias):
    if alias not in (store.get('opencode_account_blocks', {}) or {}):
        record_limit(store, alias, request_refresh=False, reason='usage_unavailable')
    request_refresh(store, alias)


async def refresh_accounts(store, aliases=None):
    failed = False
    for alias, account in list(accounts(store).items()):
        if aliases is not None and alias not in aliases:
            continue
        snapshot = await asyncio.to_thread(collect_status, alias)
        with store.db:
            if accounts(store).get(alias) != account:
                continue
            statuses = store.get('opencode_account_status', {}) or {}
            statuses[alias] = snapshot
            store.put('opencode_account_status', statuses)
            failed = failed or 'error' in snapshot
            blocks = store.get('opencode_account_blocks', {}) or {}
            block = blocks.get(alias)
            worker_hold = bool(block and block.get('retry_seconds'))
            worker_wait = worker_hold
            if worker_wait and block.get('worker_id') is not None:
                worker = store.db.execute('SELECT status FROM workers WHERE id=?', (block['worker_id'],)).fetchone()
                worker_wait = worker is not None and worker['status'] == 'waiting_for_quota'
            deadline = block['until'] if worker_wait else account_block(store, alias).get('until', 0)
            if worker_hold and deadline > time.time():
                resets = [bounded_reset(window['resets_at']) for window in limited_windows(snapshot)
                          if number(window.get('resets_at')) and window['resets_at'] > block.get('account_until', block['until'])]
                if resets:
                    blocks[alias] = dict(block, until=max([block['until'], *resets]),
                                         account_until=max(resets), reset_known=True)
                    store.put('opencode_account_blocks', blocks)
                elif account_block(store, alias).get('until', 0) <= time.time() and (
                        'error' in snapshot or limited_windows(snapshot)):
                    blocks[alias] = dict(block, account_until=time.time() + RECHECK_SECONDS)
                    store.put('opencode_account_blocks', blocks)
                continue
            if limited_windows(snapshot):
                record_limit(store, alias, request_refresh=False, backoff=worker_wait,
                             worker_id=block.get('worker_id') if worker_wait else None)
            elif (block and 'error' not in snapshot and snapshot['observed_at'] > block['observed_at']
                  and not worker_wait and (not block['reset_known'] or snapshot['observed_at'] >= deadline)):
                blocks.pop(alias)
                store.put('opencode_account_blocks', blocks)
            elif block and 'error' in snapshot and deadline <= time.time():
                record_limit(store, alias, request_refresh=False, reason=block['reason'], backoff=worker_wait,
                             worker_id=block.get('worker_id') if worker_wait else None)
    return failed


async def monitor_accounts(store):
    next_refresh = 0
    completed_request = 0
    while True:
        profiles = accounts(store)
        request = store.get('opencode_account_status_refresh_requested', 0) or 0
        blocks = store.get('opencode_account_blocks', {}) or {}
        due = any(blocks.get(alias, {}).get('until', float('inf')) <= time.time()
                  and launch_state(store, alias) != 'ready' for alias in profiles)
        if not profiles:
            next_refresh = 0
        elif time.monotonic() >= next_refresh or request > completed_request or due:
            await refresh_accounts(store)
            completed_request = request
            next_refresh = time.monotonic() + RECHECK_SECONDS
        await asyncio.sleep(2)


def launch_state(store, alias, worker_id=None):
    if alias not in accounts(store):
        return 'usage_unavailable'
    snapshot = (store.get('opencode_account_status', {}) or {}).get(alias, {})
    fresh = time.time() - snapshot.get('observed_at', 0) < RECHECK_SECONDS
    if fresh and snapshot.get('error') in AUTH_ERRORS:
        return snapshot['error']
    block = account_block(store, alias, worker_id)
    if block and not (block.get('retry_seconds') and block['until'] <= time.time() and fresh
                      and snapshot.get('observed_at', 0) > block['observed_at'] and snapshot.get('usage')
                      and not snapshot.get('error') and not limited_windows(snapshot)):
        return block['reason']
    if not fresh or snapshot.get('error') or not snapshot.get('usage'):
        return 'usage_unavailable'
    return 'quota' if limited_windows(snapshot) else 'ready'


def available(store, alias, worker_id=None):
    return launch_state(store, alias, worker_id) == 'ready'


async def ready(store, alias, worker_id=None):
    if alias not in accounts(store):
        return False
    snapshot = (store.get('opencode_account_status', {}) or {}).get(alias, {})
    block = account_block(store, alias, worker_id)
    now = time.time()
    state = launch_state(store, alias, worker_id)
    if state in AUTH_ERRORS:
        return True
    if (not snapshot or now - snapshot.get('observed_at', 0) >= RECHECK_SECONDS
            or block and block['until'] <= now):
        with store.db:
            request_refresh(store, alias)
    return state == 'ready'


def retry_at(data):
    headers = data.get('responseHeaders') or {}
    if not isinstance(headers, dict):
        return None
    value = next((value for name, value in headers.items() if name.lower() == 'retry-after'), None)
    try:
        seconds = float(value)
        return bounded_reset(time.time() + max(0, seconds)) if math.isfinite(seconds) else None
    except (ValueError, TypeError):
        try:
            return bounded_reset(parsedate_to_datetime(value).timestamp())
        except (ValueError, TypeError, OverflowError, AttributeError):
            return None


def limit_info(data, message=''):
    body = data.get('responseBody', '')
    text = (body if isinstance(body, str) else json.dumps(body)) + ' ' + message
    quota = ('GoUsageLimitError' in text or any(value in text.lower() for value in (
        '5-hour usage limit reached', 'weekly usage limit reached', 'monthly usage limit reached')))
    return {'retry_at': retry_at(data)} if data.get('statusCode') == 429 or quota else None


def card(store, alias):
    snapshot = (store.get('opencode_account_status', {}) or {}).get(alias, {})
    lines = ['OpenCode subscription']
    if snapshot.get('error'):
        lines.append(ERRORS.get(snapshot['error'], ERRORS['usage_unavailable']))
    elif snapshot.get('usage'):
        for name, label in WINDOWS.items():
            window = snapshot['usage'][name]
            reset = window['resets_at']
            when = datetime.fromtimestamp(reset, timezone.utc).strftime('%Y-%m-%d %H:%M UTC') if reset else 'unknown'
            lines.append('%s: %g%% left; resets %s' % (label, max(0, 100 - window['utilization']), when))
    else:
        lines.append('Usage has not been checked yet.')
    block = account_block(store, alias)
    if block:
        until = datetime.fromtimestamp(block['until'], timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        lines.append('Paused until ' + until + ('; usage will be rechecked.' if not block['reset_known'] else '.'))
    return '\n'.join(lines)
