"""Select Codex (ChatGPT) account homes without copying credentials or conversation history.

Each Codex account has its own CODEX_HOME, because a login lives in that home's auth.json and its
refresh token must have one owner. The home Torii's Codex already used (CODEX_HOME, or ~/.codex)
stays the first account and keeps its history in place. An added home under ~/.codex-accounts links
the first home's threads, settings, instructions, and skills, so they stay in one place. Every
Codex process gets CODEX_SQLITE_HOME set to the first home, so all accounts share one thread index.
A thread started under one account therefore resumes under another.

By default Codex work runs on one active account that the owner or the coordinator picks, and the owner
hears once per account and window when that account reaches 80% used or hits its limit. With automatic
switching on, Torii moves work between accounts at 95% by the rules it uses for Claude.
"""

import asyncio
import json
import logging
import math
import os
from pathlib import Path
import shlex
import shutil
import time
import uuid

from . import problems
from .account_status import _stop, identity_label
from .accounts import LIMIT_CONTINUATION, SWITCH_THRESHOLD, _dict, _number, identity_email
from .providers import RunResult, child_environment
from .usage_notice import queue_low_capacity_notice


DEFAULT_ALIAS = 'codex'
THREADS = ('sessions', 'archived_sessions', 'thread-writer-locks')
SHARED = frozenset(THREADS + ('config.toml', '.config.toml.lock', '.personality_migration', '.sandbox_migration',
                              'AGENTS.md', 'agents', 'attachments', 'history.jsonl', 'hooks.json', 'memories',
                              'mcp-oauth-locks', 'prompts', 'rules', 'skills'))
SCRATCH = frozenset({'auth.json', 'installation_id', 'models_cache.json', 'version.json', 'cache', 'log', 'tmp',
                     '.tmp', 'shell_snapshots', 'plugins'})
INHERITED = ('CODEX_HOME', 'CODEX_SQLITE_HOME', 'CODEX_API_KEY', 'CODEX_ACCESS_TOKEN')
WINDOWS = ('five_hour', 'seven_day')
WINDOW_NAMES = {'five_hour': '5-hour', 'seven_day': 'weekly'}
NOTICE_USED = 80
DAY_MINUTES = 24 * 60
REFRESH_SECONDS = 300
REFRESH_RETRY_SECONDS = 30
RESET_CREDIT_SCHEMA = {'version': '0.159.2', 'details': True, 'credit_id': True}
logger = logging.getLogger(__name__)


def default_home():
    """The home Torii's Codex used before accounts existed. It stays the first account."""
    configured = os.environ.get('CODEX_HOME')
    return Path(configured or Path.home() / '.codex').resolve()


def accounts_root():
    return Path.home() / '.codex-accounts'


def link_home(home):
    """Link the first home's shared entries into an added home, and return the home.

    Only entries that do not depend on the login are shared. The login, its caches, and its plugins stay
    private, and so does any entry this list does not name. SQLite files are shared through
    CODEX_SQLITE_HOME instead. An entry that already exists is never replaced. The thread folders must
    resolve to the shared ones, or a thread started under another account would not resume here.
    """
    home = Path(home)
    shared = default_home()
    if home.resolve() == shared:
        return home
    for name in THREADS:
        (shared / name).mkdir(mode=0o700, parents=True, exist_ok=True)
    for entry in sorted(shared.iterdir()):
        target = home / entry.name
        if entry.name not in SHARED or target.exists() or target.is_symlink():
            continue
        target.symlink_to(entry)
    for name in THREADS:
        if (home / name).resolve() != (shared / name).resolve():
            raise ValueError('The Codex account home keeps its own %s folder instead of the shared one' % name)
    return home


def clear_home(home):
    """Empty a home that a sign-in made but did not register.

    The links go first, never their targets. What Codex wrote there itself goes next, including a login
    that was not kept. A home that holds anything else stays as it is.
    """
    home = Path(home)
    if home.resolve() == default_home() or not home.is_dir():
        return
    for path in home.iterdir():
        if path.is_symlink():
            path.unlink()
    entries = list(home.iterdir())
    if any(path.name not in SCRATCH for path in entries):
        return
    for path in entries:
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()


def codex_environment(home):
    """The one place a Codex account enters an environment. There is no fallback to another login."""
    env = {key: value for key, value in child_environment().items() if key not in INHERITED}
    env['CODEX_HOME'] = str(home)
    env['CODEX_SQLITE_HOME'] = str(default_home())
    return env


def plan_label(plan):
    return ('ChatGPT ' + plan.replace('_', ' ').title() if isinstance(plan, str) and plan and plan != 'unknown'
            else 'ChatGPT (plan unknown)')


def _windows(limits):
    """Usage windows of one Codex rate-limit snapshot, named by length. App-server and rollout spellings both work."""
    limits = _dict(limits)
    windows = {}
    for key in ('primary', 'secondary'):
        window = _dict(limits.get(key))
        used = window.get('usedPercent', window.get('used_percent'))
        minutes = window.get('windowDurationMins', window.get('window_minutes'))
        if not _number(used) or not _number(minutes):
            continue
        entry = {'utilization': used}
        reset = window.get('resetsAt', window.get('resets_at'))
        if _number(reset):
            entry['resets_at'] = reset
        windows['seven_day' if minutes >= DAY_MINUTES else 'five_hour'] = entry
    return windows


def usage_windows(limits):
    """The account's ordinary Codex meter. Other meters, such as a model's own limit, return nothing."""
    limit = _dict(limits).get('limitId', _dict(limits).get('limit_id'))
    return _windows(limits) if limit in (None, 'codex') else {}


def full_windows(usage):
    now = time.time()
    return [window for window in (_dict(usage.get(key)) for key in WINDOWS)
            if _number(window.get('utilization')) and window['utilization'] >= SWITCH_THRESHOLD * 100
            and not (_number(window.get('resets_at')) and window['resets_at'] <= now)]


def usage_line(usage):
    from .controls import resets
    return '; '.join('%s %d%% used, %s' % (WINDOW_NAMES[key], math.floor(window['utilization']),
                                            resets(window.get('resets_at')))
                     for key, window in ((key, _dict(usage.get(key))) for key in WINDOWS)
                     if _number(window.get('utilization')))


def low_windows(usage, out=False):
    """Windows at 80% used or more, by name and reset time. An account that is out and shows no such window
    counts its weekly window, or an unnamed one."""
    now = time.time()
    low = {key: _dict(usage.get(key)).get('resets_at') for key in WINDOWS
           if _number(_dict(usage.get(key)).get('utilization')) and usage[key]['utilization'] >= NOTICE_USED
           and not (_number(usage[key].get('resets_at')) and usage[key]['resets_at'] <= now)}
    if out and not low:
        weekly = _dict(usage.get('seven_day')).get('resets_at')
        low = {'seven_day': weekly} if _number(weekly) and weekly > now else {'limit': None}
    return low


class AppServerError(Exception):
    pass


class AppServer:
    """A short-lived `codex app-server` spoken to over stdio JSON-RPC, for account checks and sign-in.

    It runs without plugins. With them, Codex starts a marketplace refresh that stages a full copy of each
    configured marketplace in the home, and stopping the server mid-refresh leaves that copy behind.
    """

    def __init__(self, binary, env):
        self.binary = binary
        self.env = env
        self.process = None
        self.counter = 0
        self.notifications = []

    async def start(self, settings=(), cwd=None):
        self.process = await asyncio.create_subprocess_exec(
            self.binary, *settings,
            'app-server', '--listen', 'stdio://', '--disable', 'plugins', env=self.env,
            cwd=cwd or str(Path.home()),
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            limit=1024 * 1024, start_new_session=True)
        await self.request('initialize', {'clientInfo': {'name': 'torii', 'version': '1'}})
        await self.send({'method': 'initialized', 'params': {}})
        return self

    async def send(self, message):
        self.process.stdin.write((json.dumps(message) + '\n').encode())
        await self.process.stdin.drain()

    async def read(self):
        line = await self.process.stdout.readline()
        if not line:
            raise ValueError('The Codex app-server stopped')
        message = json.loads(line)
        if not isinstance(message, dict):
            raise ValueError('The Codex app-server sent an unexpected message')
        if 'method' in message and 'id' not in message:
            self.notifications.append(message)
        return message

    async def request(self, method, params=None):
        """The result of one request. Notifications that arrive meanwhile are kept for `notification`."""
        self.counter += 1
        number = self.counter
        await self.send({'id': number, 'method': method, 'params': params or {}})
        while True:
            message = await self.read()
            if message.get('id') == number and 'method' not in message:
                if 'error' in message:
                    raise AppServerError(str(_dict(message['error']).get('message') or method)[:200])
                return _dict(message.get('result'))

    async def notification(self, method, match):
        """The parameters of the first matching notification. The caller bounds the wait."""
        while True:
            for message in self.notifications:
                params = _dict(message.get('params'))
                if message.get('method') == method and match(params):
                    self.notifications.remove(message)
                    return params
            await self.read()

    async def stop(self):
        if self.process is not None:
            await _stop(self.process)


async def collect_codex_status(account, binary='codex', timeout=30):
    """Read one account's login and usage through the app-server. No model turn runs."""
    result = {'checked_at': time.time()}
    directory = account.get('config_dir')
    if not directory or not Path(directory).is_dir():
        return dict(result, error='login_required')
    server = None
    try:
        server = AppServer(binary, codex_environment(link_home(directory)))

        async def check():
            await server.start()
            signed_in = _dict((await server.request('account/read')).get('account'))
            if signed_in.get('type') != 'chatgpt':
                return dict(result, identity={'logged_in': False}, error='login_required')
            result['identity'] = {'email': identity_label(signed_in.get('email')),
                                  'type': plan_label(signed_in.get('planType')), 'logged_in': True}
            result.update(_status_from_limits(await server.request('account/rateLimits/read')))
            return result
        return await asyncio.wait_for(check(), timeout)
    except FileNotFoundError:
        return dict(result, error='cli_unavailable')
    except asyncio.TimeoutError:
        return dict(result, error='timeout')
    except (AppServerError, OSError, ValueError, TypeError, KeyError, AttributeError):
        return dict(result, error='usage_unavailable')
    finally:
        if server is not None:
            await server.stop()


def _status_from_limits(response):
    limits = _dict(response.get('rateLimits'))
    credits = _dict(limits.get('credits'))
    resets = _dict(response.get('rateLimitResetCredits'))
    count = resets.get('availableCount')
    result = {'usage': usage_windows(limits),
              'credits': {'has_credits': credits.get('hasCredits') is True, 'unlimited': credits.get('unlimited') is True,
                          'balance': identity_label(credits.get('balance'))},
              'resets_available': count if type(count) is int else 0,
              'reset_expires': None,
              'usage_allowed': (response.get('ordinaryUsageAllowed') is not False
                                and not limits.get('rateLimitReachedType')
                                and limits.get('spendControlReached') is not True),
              'observed_at': time.time()}
    if RESET_CREDIT_SCHEMA['details'] and isinstance(resets.get('credits'), list):
        available = []
        for credit in resets['credits']:
            credit = _dict(credit)
            if credit.get('status') != 'available' or not isinstance(credit.get('id'), str) or not credit['id']:
                continue
            expires = credit.get('expiresAt')
            available.append({'id': credit['id'], 'expires_at': float(expires) if _number(expires) else None})
        available.sort(key=lambda credit: credit['expires_at'] if credit['expires_at'] is not None else math.inf)
        result['reset_credits'] = available
        result['reset_expires'] = next((credit['expires_at'] for credit in available
                                       if credit['expires_at'] is not None), None)
    return result


async def redeem_codex_reset(account, binary='codex', guarded=False, limited=False, timeout=30):
    """Redeem one banked reset in this account home and return its refreshed Codex limits."""
    directory = account.get('config_dir')
    if not directory or not Path(directory).is_dir():
        raise ValueError('Codex account home is unavailable')
    server = AppServer(binary, codex_environment(link_home(directory)))
    state = {'sent': False, 'confirmed': False}

    async def redeem():
        await server.start()
        signed_in = _dict((await server.request('account/read')).get('account'))
        if signed_in.get('type') != 'chatgpt':
            raise ValueError('Codex account is not signed in')
        before = _status_from_limits(await server.request('account/rateLimits/read'))
        if before['resets_available'] < 1:
            raise ValueError('This Codex account has no banked reset')
        if guarded and not (limited or before['usage_allowed'] is False or full_windows(before['usage'])):
            raise ValueError('This Codex account is below 95% on its resettable meters')
        params = {'idempotencyKey': str(uuid.uuid4())}
        if RESET_CREDIT_SCHEMA['credit_id'] and before.get('reset_credits'):
            params['creditId'] = before['reset_credits'][0]['id']
        logger.info('reset spend ordered=%s', 'yes' if 'creditId' in params else 'no')
        state['sent'] = True
        outcome = (await server.request('account/rateLimitResetCredit/consume', params)).get('outcome')
        if outcome != 'reset':
            raise ValueError({'nothingToReset': 'No Codex usage window can be reset',
                              'noCredit': 'This Codex account has no banked reset',
                              'alreadyRedeemed': 'This reset request was already redeemed'}.get(
                                  outcome, 'Codex did not confirm the reset'))
        state['confirmed'] = True
        try:
            return {'redeemed': True, **_status_from_limits(await server.request('account/rateLimits/read'))}
        except (AppServerError, OSError, ValueError, TypeError, KeyError, AttributeError):
            return {'redeemed': True, 'error': 'The reset succeeded, but refreshed limits are unavailable'}

    try:
        return await asyncio.wait_for(redeem(), timeout)
    except (asyncio.TimeoutError, OSError):
        if state['confirmed']:
            return {'redeemed': True, 'error': 'The reset succeeded, but refreshed limits are unavailable'}
        if state['sent']:
            return {'redeemed': None, 'error': 'The reset result is unknown; check usage before trying again'}
        raise
    finally:
        await server.stop()


def discover_codex_accounts(store):
    """Register the existing Codex home as the first account, and drop the old hold on all of Codex.

    Until a check confirms a ChatGPT login there, Codex keeps running on the service's own login.
    """
    accounts = {alias: dict(profile) for alias, profile in _dict(store.get('codex_accounts', {})).items()
                if isinstance(profile, dict)}
    home = default_home()
    homes = {Path(profile['config_dir']).resolve() for profile in accounts.values() if profile.get('config_dir')}
    homes.update(Path(directory).resolve() for profile in accounts.values()
                 for directory in profile.get('previous_config_dirs', []))
    homes.update(Path(directory).resolve() for directory in store.get('account_dirs_removed', []))
    taken = set(accounts) | set(_dict(store.get('accounts', {})))
    with store.db:
        if home not in homes and (home / 'auth.json').is_file():
            alias = next(name for name in [DEFAULT_ALIAS] + ['%s-%d' % (DEFAULT_ALIAS, number)
                                                             for number in range(2, len(taken) + 3)]
                         if name not in taken)
            accounts[alias] = {'config_dir': str(home), 'enabled': False, 'awaiting_login': True}
            store.put('codex_accounts', accounts)
            logger.info('codex account discovered')
        store.db.execute("DELETE FROM settings WHERE key='codex_block'")
    return accounts


async def refresh_codex_accounts(store, collect=None):
    """Update each checked account's snapshot. A failed check keeps the dated prior usage."""
    if collect is None:
        async def collect(account):
            return await collect_codex_status(account)
    failed = False
    broker = CodexBroker(store)
    for alias, account in list(broker.accounts().items()):
        if not account.get('enabled') and not account.get('awaiting_login'):
            continue
        snapshot = await collect(account)
        failed = failed or ('error' in snapshot and (account.get('enabled') is True
                                                     or snapshot['error'] != 'login_required'))
        with store.db:
            if broker.accounts().get(alias) != account:
                continue
            snapshots = _dict(store.get('codex_account_status', {}))
            previous = _dict(snapshots.get(alias))
            if snapshot.get('error') == 'login_required' and 'identity' not in snapshot:
                snapshot['identity'] = {'logged_in': False}
            identity = _dict(snapshot.get('identity'))
            previous_identity = _dict(previous.get('identity'))
            if ('identity' in snapshot and identity.get('logged_in') is not True
                    and not identity_email(identity) and identity_email(previous_identity)):
                snapshot['identity'] = {key: previous_identity[key] for key in ('email', 'type', 'name')
                                        if key in previous_identity}
                snapshot['identity']['logged_in'] = False
            if 'identity' in snapshot and snapshot['identity'] != previous.get('identity'):
                previous = {}
            if 'error' in snapshot and snapshot['error'] != previous.get('error'):
                problems.record(store, 'accounts', 'codex-status-' + snapshot['error'].replace('_', '-'),
                                'account=' + broker.label(alias))
            snapshots[alias] = {**previous, **snapshot}
            if 'error' not in snapshot:
                snapshots[alias].pop('error', None)
            store.put('codex_account_status', snapshots)
            if account.get('awaiting_login') and broker.authenticated(alias):
                accounts = broker.accounts()
                accounts[alias] = dict(account, enabled=True)
                accounts[alias].pop('awaiting_login', None)
                store.put('codex_accounts', accounts)
            if 'usage' in snapshot:
                broker.notify_limit(alias)
    return failed


async def monitor_codex_accounts(store, binary='codex'):
    next_refresh = 0
    completed_request = 0
    failures = 0
    while True:
        request = store.get('codex_account_status_refresh_requested', 0) or 0
        if time.monotonic() >= next_refresh or request > completed_request:
            failed = await refresh_codex_accounts(store, lambda account: collect_codex_status(account, binary))
            failures = failures + 1 if failed else 0
            completed_request = request
            next_refresh = time.monotonic() + (min(REFRESH_SECONDS, REFRESH_RETRY_SECONDS * 2 ** (failures - 1))
                                               if failures else REFRESH_SECONDS)
        await asyncio.sleep(2)


class CodexBroker:
    """Choose the Codex account for each launch: the active one, or by AccountBroker's rules when automatic."""

    FRESH_SECONDS = 600
    REJECTION_HOLD_SECONDS = 300

    def __init__(self, store):
        self.store = store

    def accounts(self):
        accounts = {alias: dict(profile) for alias, profile in _dict(self.store.get('codex_accounts', {})).items()
                    if isinstance(profile, dict)}
        return accounts

    def snapshot(self, alias):
        return _dict(_dict(self.store.get('codex_account_status', {})).get(alias))

    def label(self, alias):
        email = _dict(self.snapshot(alias).get('identity')).get('email')
        return email.strip() if isinstance(email, str) and email.strip() else alias

    def authenticated(self, alias):
        directory = _dict(self.accounts().get(alias)).get('config_dir')
        snapshot = self.snapshot(alias)
        identity = _dict(snapshot.get('identity'))
        return (isinstance(directory, str) and Path(directory).is_dir() and identity.get('logged_in') is True
                and bool(identity_email(identity)) and snapshot.get('error') != 'login_required')

    def signed_in(self, alias):
        return _dict(self.accounts().get(alias)).get('enabled') is True and self.authenticated(alias)

    def managed(self):
        """Whether any registered home has a confirmed ChatGPT login. Until then Codex runs on the service's own."""
        return any(self.authenticated(alias) for alias in self.accounts())

    def automatic(self):
        return self.store.get('codex_auto_switch', False) is True

    def active(self):
        """The owner's pick, else a signed-in default home or first signed-in alias, else the first home."""
        accounts = self.accounts()
        chosen = self.store.get('codex_active_account')
        if chosen in accounts:
            return chosen
        if not accounts:
            return None
        home = default_home()
        fallback = next((alias for alias, profile in sorted(accounts.items()) if profile.get('config_dir')
                         and Path(profile['config_dir']).resolve() == home), min(accounts))
        return (fallback if self.signed_in(fallback) else
                next((alias for alias in sorted(accounts) if self.signed_in(alias)), fallback))

    def current(self):
        """The account the next Codex launch uses, for display."""
        return self.select() if self.automatic() else self.active()

    def exhausted(self, alias, parent=False):
        """Whether Codex refuses work on the account now: a rejection holds it, or a window is used up."""
        block = self.blocks().get(alias)
        snapshot = self.snapshot(alias)
        return (bool(block) and (block.get('reason') == 'quota' or not parent and self.automatic())
                or snapshot.get('usage_allowed') is False
                or any(window['utilization'] >= 100 for window in full_windows(_dict(snapshot.get('usage')))))

    def parent_account(self):
        alias = self.active()
        return alias if self.managed() and self.signed_in(alias) and not self.exhausted(alias, parent=True) else None

    def environment(self, alias):
        directory = _dict(self.accounts().get(alias)).get('config_dir')
        if not directory:
            raise ValueError('Codex account home is unavailable')
        return codex_environment(link_home(directory))

    def login_command(self, alias):
        directory = _dict(self.accounts().get(alias)).get('config_dir')
        if not directory:
            raise ValueError('Codex account home is unavailable')
        return 'env CODEX_HOME=' + shlex.quote(directory) + ' codex login --device-auth'

    def alias_for_environment(self, env):
        home = env.get('CODEX_HOME')
        target = Path(home).resolve() if home else None
        return next((alias for alias, profile in self.accounts().items()
                     if profile.get('config_dir') and Path(profile['config_dir']).resolve() == target), None)

    def blocks(self):
        now = time.time()
        saved = _dict(self.store.get('codex_account_blocks', {}))
        kept = {alias: block for alias, block in saved.items() if isinstance(block, dict) and
                (block.get('until') is None or (_number(block['until']) and block['until'] > now))}
        for alias, block in list(kept.items()):
            snapshot = self.snapshot(alias)
            if (block.get('until') is None and _number(snapshot.get('observed_at'))
                    and snapshot['observed_at'] > (block.get('observed_at') or 0)
                    and not full_windows(_dict(snapshot.get('usage')))):
                kept.pop(alias)
        if kept != saved:
            outer = self.store.db.in_transaction
            self.store.put('codex_account_blocks', kept)
            if not outer:
                self.store.db.commit()
        return kept

    def account_state(self, alias, parent=False):
        block = self.blocks().get(alias)
        if block and (block.get('reason') == 'quota' or not parent and self.automatic()):
            return {'state': 'limited', 'until': block.get('until')}
        snapshot = self.snapshot(alias)
        full = [window for window in full_windows(_dict(snapshot.get('usage')))
                if not parent and self.automatic() or window['utilization'] >= 100]
        if full or snapshot.get('usage_allowed') is False:
            resets = [window['resets_at'] for window in full if _number(window.get('resets_at'))]
            return {'state': 'limited', 'until': max(resets) if resets else None}
        observed = snapshot.get('observed_at')
        if not _number(observed) or snapshot.get('error') or time.time() - observed > self.FRESH_SECONDS:
            return {'state': 'stale', 'since': observed}
        return {'state': 'available'}

    def select(self, attempted=()):
        """The account for the next launch, or None.

        With automatic switching on, this is the signed-in account under 95% whose weekly limit resets
        soonest. With it off, it is the active account while that account is signed in and not exhausted.
        """
        if not self.automatic():
            alias = self.active()
            return (alias if alias and alias not in attempted and self.signed_in(alias) and not self.exhausted(alias)
                    else None)
        eligible = [alias for alias in self.accounts() if alias not in attempted and self.signed_in(alias)
                    and self.account_state(alias)['state'] != 'limited']

        def weekly_reset(alias):
            reset = _dict(_dict(self.snapshot(alias).get('usage')).get('seven_day')).get('resets_at')
            return reset if _number(reset) and reset > time.time() else math.inf
        return min(eligible, key=lambda alias: (weekly_reset(alias), alias)) if eligible else None

    def listed(self):
        """Signed-in accounts sorted by email, with the next choice first, and that choice."""
        aliases = sorted((alias for alias in self.accounts() if self.authenticated(alias)),
                         key=lambda alias: (self.label(alias).casefold(), alias))
        active = self.current()
        if active not in aliases:
            return aliases, None
        return [active] + [alias for alias in aliases if alias != active], active

    def earliest_reset(self):
        aliases = [alias for alias in self.accounts() if self.signed_in(alias)] if self.automatic() else [self.active()]
        resets = [state['until'] for state in (self.account_state(alias) for alias in aliases if alias)
                  if state['state'] == 'limited' and _number(state.get('until'))]
        return min(resets) if resets else None

    def headroom(self, alias):
        """One line of an account's usage, banked resets, and state, for the owner."""
        snapshot = self.snapshot(alias)
        parts = [usage_line(_dict(snapshot.get('usage'))) or 'usage not checked yet']
        banked = snapshot.get('resets_available')
        if type(banked) is int:
            parts.append('%d banked reset%s' % (banked, '' if banked == 1 else 's'))
        state = self.account_state(alias)
        parts.append('not signed in' if not self.signed_in(alias) else
                     'limited' if state['state'] == 'limited' else state['state'])
        return self.label(alias) + ' · ' + ' · '.join(parts)

    def notify_limit(self, alias, limits=None, rejected=False, topic=None, ran=False):
        """Tell the owner once per account and window that Codex work is running low.

        With automatic switching off, this covers the active account at 80% used in any window, or after a
        rejection. With it on, it covers the account in use once no other account is under 95%.
        """
        usage = {**_dict(self.snapshot(alias).get('usage')), **(_windows(limits) if rejected else {})}
        low = low_windows(usage, rejected or self.exhausted(alias))
        others = [other for other in self.accounts() if other != alias and self.signed_in(other)]
        due = (bool(low) and (ran or alias == self.current()) and
               (alias == self.active() if not self.automatic() else
                not any(self.account_state(other)['state'] != 'limited' for other in others)))
        topics = [row['id'] for row in self.store.topics() if row['enabled']]
        topic = topic or self.store.get('coordinator_home_topic') or (topics[0] if topics else None)
        message = self.limit_text(alias, usage, rejected, others) if due else ''
        if not queue_low_capacity_notice(self.store, 'Codex', alias, low, topic, message,
                                         send=due, reset_tolerance=30 * 60, clear_recovered=True):
            return
        logger.info('codex limit notice account=%s windows=%s rejected=%s', self.label(alias), ','.join(low), rejected)

    def limit_text(self, alias, usage, rejected, others):
        snapshot = self.snapshot(alias)
        lines = ['Codex account %s %s.' % (self.label(alias), 'hit its usage limit: Codex rejected a turn' if rejected
                                           else 'has 20% or less of its usage left')]
        if usage_line(usage):
            lines.append('Usage: ' + usage_line(usage) + '.')
        if type(snapshot.get('resets_available')) is int:
            lines.append('Banked usage resets on this account: %d.' % snapshot['resets_available'])
        lines.append('Other Codex accounts:\n' + '\n'.join(self.headroom(other) for other in sorted(
            others, key=lambda name: self.label(name).casefold())) if others else 'No other Codex account is signed in.')
        redeem = 'To use a banked reset, tap Reset in /accounts or ask the coordinator to call account.redeem.'
        parent_paused = bool(self.store.get('coordinator_account_retry_at') and alias == self.active())
        if parent_paused:
            lines.append('The main chat waits until you pick another ChatGPT account or the limit resets. '
                         'It never switches ChatGPT accounts on its own.')
        if self.automatic():
            lines.append('Automatic switching is on for workers, and this is the last Codex account under 95%. ' + redeem)
        else:
            lines.append((('The main chat and Codex workers wait until you switch' if parent_paused else
                           'Codex workers wait until you switch') + ' the active account or the limit resets. ' if rejected
                          else '') + 'Automatic switching is off, so Codex stays on this account. Ask the coordinator '
                         'to switch the active Codex account, or use /accounts. ' + redeem)
        return '\n'.join(lines)

    def record_rate_limit(self, alias, limits, rejected=False, topic=None):
        """Save the meter a run reported. Hold the account at 95%, or after a rejection until its limit resets.

        While automatic switching is off, the owner also hears about it once per window.
        """
        if not alias or alias not in self.accounts():
            return
        windows = usage_windows(limits)
        if windows:
            snapshots = _dict(self.store.get('codex_account_status', {}))
            snapshot = dict(_dict(snapshots.get(alias)))
            snapshot['usage'] = {**_dict(snapshot.get('usage')), **windows}
            snapshots[alias] = snapshot
            self.store.put('codex_account_status', snapshots)
        full = full_windows(_windows(limits) if rejected else windows) if self.automatic() or rejected else []
        if not (rejected or full):
            self.notify_limit(alias, limits, rejected, topic, ran=True)
            return
        resets = [window['resets_at'] for window in full
                  if _number(window.get('resets_at')) and window['resets_at'] > time.time()]
        until = max(resets) if resets else (time.time() + self.REJECTION_HOLD_SECONDS if rejected else None)
        blocks = self.blocks()
        blocks[alias] = {'until': until, 'reason': 'quota' if rejected else 'threshold', 'observed_at': time.time()}
        self.store.put('codex_account_blocks', blocks)
        self.store.put('codex_account_status_refresh_requested', time.time())
        logger.info('codex account limited account=%s', self.label(alias))
        self.notify_limit(alias, limits, rejected, topic, ran=True)

    async def run(self, run, provider, prompt, cwd, session_id, notice_topic=None, stopped=None, **options):
        """Run on the chosen account. With automatic switching on, a usage-limit rejection resumes the same
        thread on the next account.

        Until a registered home has a confirmed ChatGPT login, Codex runs on the service's own login as it did
        before accounts. That covers the first check after an upgrade and a login by API key.
        """
        on_account = options.pop('on_account', None)
        if not self.managed():
            return await run(provider, prompt, cwd, session_id, **options)
        if not self.automatic():
            return await self.run_active(run, provider, prompt, cwd, session_id, notice_topic, on_account, **options)
        attempted = set()
        previous = None
        model = options.get('model') or 'native-default'
        last = RunResult(session_id, error='No Codex account is available: each enabled one is signed out or at '
                         'its usage limit.', failure_code='accounts_unavailable')
        while True:
            alias = self.select(attempted)
            if alias is None:
                return last
            attempted.add(alias)
            if on_account:
                on_account(alias)
            if previous:
                logger.info('codex account rotated from=%s to=%s', self.label(previous), self.label(alias))
                if options.get('on_problem'):
                    options['on_problem']('accounts', 'codex-rotated', 'from=%s to=%s model=%s' % (
                        self.label(previous), self.label(alias), model))
            logger.info('codex account selected account=%s model=%s', self.label(alias), model)
            last = await run(provider, prompt, cwd, session_id, account_alias=alias, **options)
            with self.store.db:
                self.record_rate_limit(alias, last.rate_limit_info, last.quota_limited and not last.success)
            if last.success or not last.quota_limited or (stopped and stopped()) or not self.select(attempted):
                return last
            previous = alias
            if last.session_id:
                session_id = last.session_id
                options['fresh'] = False
                if not prompt.startswith(LIMIT_CONTINUATION):
                    prompt = LIMIT_CONTINUATION + prompt

    async def run_active(self, run, provider, prompt, cwd, session_id, notice_topic, on_account, **options):
        """Run once on the active account. A signed-out or exhausted account runs nothing, so the worker waits."""
        alias = self.active()
        label = self.label(alias) if alias else 'none'
        if not alias or not self.signed_in(alias):
            return RunResult(session_id, error='The active Codex account %s is not signed in or is disabled.' % label,
                             failure_code='accounts_unavailable')
        if self.exhausted(alias):
            return RunResult(session_id, error='The active Codex account %s is at its usage limit.' % label,
                             quota_limited=True, failure_code='quota_limited')
        if on_account:
            on_account(alias)
        logger.info('codex account selected account=%s model=%s', label, options.get('model') or 'native-default')
        result = await run(provider, prompt, cwd, session_id, account_alias=alias, **options)
        with self.store.db:
            self.record_rate_limit(alias, result.rate_limit_info, result.quota_limited and not result.success,
                                   notice_topic)
        return result
