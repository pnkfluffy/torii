"""Isolated OpenCode workers on the pinned ACP adapter."""

import asyncio
from email.utils import parsedate_to_datetime
import hashlib
import json
import math
import os
import pwd
import re
import shutil
import sqlite3
import stat
import sys
import time
import uuid
from pathlib import Path
import tempfile

from . import native_ids
from .acp_protocol import AcpAdapter
from .control_api import Refused


FLAGS = {'OPENCODE_DISABLE_AUTOUPDATE': '1', 'OPENCODE_DISABLE_MODELS_FETCH': '1',
         'OPENCODE_DISABLE_PROJECT_CONFIG': '1', 'OPENCODE_DISABLE_EXTERNAL_SKILLS': '1',
         'OPENCODE_DISABLE_LSP_DOWNLOAD': '1', 'OPENCODE_DISABLE_SHARE': '1',
         'OPENCODE_AUTH_CONTENT': '{}'}
ENVIRONMENT_NAMES = frozenset(('PATH', 'LANG', 'LANGUAGE', 'LC_ALL', 'LC_CTYPE', 'LC_MESSAGES',
                               'LC_COLLATE', 'LC_NUMERIC', 'LC_TIME', 'LC_MONETARY',
                               'SHELL', 'USER', 'LOGNAME', 'TERM', 'TZ'))
PRIVATE_FOLDERS = {'HOME': ('home', ''), 'XDG_CONFIG_HOME': ('config', '.config'),
                   'XDG_DATA_HOME': ('data', '.local/share'), 'XDG_CACHE_HOME': ('cache', '.cache'),
                   'XDG_STATE_HOME': ('state', '.local/state'), 'TMPDIR': ('tmp', None)}
SOURCE = 'OpenCode estimate from account prices'


def environment(root, base):
    root = Path(root).resolve()
    env = {name: value for name, value in base.items() if name in ENVIRONMENT_NAMES}
    for folder in (root, *(root / name for name, _ in PRIVATE_FOLDERS.values())):
        folder.mkdir(parents=True, exist_ok=True, mode=0o700)
        folder.chmod(0o700)
    env.update({name: str(root / folder) for name, (folder, _) in PRIVATE_FOLDERS.items()})
    env.update(FLAGS)
    env['OPENCODE_TEST_MANAGED_CONFIG_DIR'] = str(root / 'managed')
    return env


def managed_preference_paths():
    if sys.platform != 'darwin':
        return ()
    directory = Path('/Library/Managed Preferences')
    user = pwd.getpwuid(os.getuid()).pw_name or 'user'
    return (directory / user / 'ai.opencode.managed.plist', directory / 'ai.opencode.managed.plist')


def managed_config_clear():
    try:
        for path in managed_preference_paths():
            try:
                path.lstat()
            except FileNotFoundError:
                continue
            return False
    except (OSError, KeyError):
        return False
    return True


def write_private(path, data):
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='wb', dir=path.parent, prefix='.opencode-', delete=False) as handle:
            temporary = Path(handle.name)
            os.fchmod(handle.fileno(), 0o600)
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def read_object(path, limit=1024):
    try:
        with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), 'rb') as handle:
            info = os.fstat(handle.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size > limit:
                return None
            data = handle.read(limit + 1)
        if len(data) > limit:
            return None
        value = json.loads(data)
        return value if isinstance(value, dict) else None
    except (OSError, ValueError):
        return None


def incomplete_marker(root, session_id):
    marker = read_object(Path(root) / 'stream-incomplete.json')
    return (bool(session_id) and marker is not None and type(marker.get('version')) is int
            and type(marker.get('consecutive')) is int and marker.get('version') == 1
            and marker.get('consecutive') == 3 and marker.get('code') == 'provider_stream_incomplete'
            and marker.get('session_id') == session_id)


def worker_config(account, key_path, cwd, shared_mcp_entries=None):
    model = account['model']
    if account.get('provider') == 'opencode-go':
        config = {'share': 'disabled', 'autoupdate': False, 'enabled_providers': ['opencode-go'],
                  'provider': {'opencode-go': {'options': {'apiKey': '{file:' + str(key_path) + '}'}}},
                  'lsp': False, 'formatter': False,
                  'model': 'opencode-go/' + model, 'small_model': 'opencode-go/' + model, 'mcp': {},
                  'permission': {'edit': 'allow', 'bash': 'allow', 'webfetch': 'allow', 'external_directory': 'deny'}}
    else:
        config = {'share': 'disabled', 'autoupdate': False, 'enabled_providers': ['torii-host'],
                  'provider': {'torii-host': {'npm': '@ai-sdk/openai-compatible',
                      'options': {'baseURL': account['base_url'], 'apiKey': '{file:' + str(key_path) + '}'},
                      'models': {model: {'limit': {'context': account['context'], 'output': account['output']},
                          'cost': {'input': account['price_in'], 'cache_read': account['price_cached'],
                                   'output': account['price_out']}}}}},
                  'lsp': False, 'formatter': False,
                  'model': 'torii-host/' + model, 'small_model': 'torii-host/' + model, 'mcp': {},
                  'permission': {'edit': 'allow', 'bash': 'allow', 'webfetch': 'allow', 'external_directory': 'deny'}}
    config['$schema'] = 'https://opencode.ai/config.json'
    for name, entry in (shared_mcp_entries or {}).items():
        config['mcp'][name] = {'type': 'remote', 'url': entry['url'], 'headers': entry['headers'], 'oauth': False}
        config['permission'][re.sub(r'[^a-zA-Z0-9_-]', '_', name) + '_*'] = 'allow'
    instructions = Path(cwd) / 'AGENTS.md'
    if instructions.is_file():
        config['instructions'] = [str(instructions)]
    return config


def write_config(root, account, key_path, cwd, shared_mcp_entries=None):
    directory = Path(root) / 'config' / 'opencode'
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    directory.chmod(0o700)
    npmrc = directory / '.npmrc'
    npmrc.write_text('offline=true\n')
    npmrc.chmod(0o600)
    package = directory / 'package.json'
    if not package.exists():
        package.write_text('{"private": true}\n')
        package.chmod(0o600)
    plugins = directory / 'plugins'
    plugins.mkdir(mode=0o700, exist_ok=True)
    plugins.chmod(0o700)
    write_private(plugins / 'guard.js', Path(__file__).with_name('opencode_guard.js').read_bytes())
    launch = {'version': 1, 'launch': uuid.uuid4().hex}
    write_private(Path(root) / 'guard-launch.json', (json.dumps(launch) + '\n').encode())
    config = worker_config(account, key_path, cwd, shared_mcp_entries)
    path = directory / 'opencode.json'
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', dir=directory, prefix='.opencode-', delete=False) as handle:
            temporary = Path(handle.name)
            json.dump(config, handle)
            handle.write('\n')
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()
    return path


def argv(binary, cwd):
    return [str(binary), 'acp', '--cwd', str(cwd)]


def parent_root(store, key):
    root = store.directory / 'opencode' / 'parents' / hashlib.sha256(key.encode()).hexdigest()[:16]
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    root.chmod(0o700)
    return root


def parent_environment(root, base):
    return environment(root, base)


def parent_config(account, key_path, cwd, instructions_path):
    model = account['model']
    subscription = account.get('provider') == 'opencode-go'
    provider = 'opencode-go' if subscription else 'torii-host'
    profile = {'options': {'apiKey': '{file:' + str(key_path) + '}'}}
    if not subscription:
        profile.update(npm='@ai-sdk/openai-compatible', models={model: {
            'limit': {'context': account['context'], 'output': account['output']},
            'cost': {'input': account['price_in'], 'cache_read': account['price_cached'], 'output': account['price_out']}}})
        profile['options']['baseURL'] = account['base_url']
    return {'share': 'disabled', 'autoupdate': False, 'enabled_providers': [provider],
            'provider': {provider: profile}, 'lsp': False, 'formatter': False,
            'model': provider + '/' + model, 'small_model': provider + '/' + model, 'mcp': {},
            'permission': {'*': 'deny', 'torii_*': 'allow'},
            'instructions': [str(instructions_path)] + ([str(Path(cwd) / 'AGENTS.md')]
                             if (Path(cwd) / 'AGENTS.md').is_file() else [])}


def write_parent_config(root, account, key_path, cwd, instructions_path):
    directory = Path(root) / 'config' / 'opencode'
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    directory.chmod(0o700)
    write_private(directory / '.npmrc', b'offline=true\n')
    if not (directory / 'package.json').exists():
        write_private(directory / 'package.json', b'{"private": true}\n')
    plugins = directory / 'plugins'
    plugins.mkdir(mode=0o700, exist_ok=True)
    plugins.chmod(0o700)
    write_private(plugins / 'guard.js', Path(__file__).with_name('opencode_guard.js').read_bytes())
    write_private(Path(root) / 'guard-launch.json',
                  (json.dumps({'version': 1, 'launch': uuid.uuid4().hex}) + '\n').encode())
    path = directory / 'opencode.json'
    write_private(path, (json.dumps(parent_config(account, key_path, cwd, instructions_path)) + '\n').encode())
    return path


def parent_receipts(root, session_id, rows, *, all_sessions=False):
    root = Path(root).resolve()
    directory = root / 'data' / 'opencode'
    try:
        directory.resolve().relative_to(root)
        database, wal = directory / 'opencode.db', directory / 'opencode.db-wal'
        if database.is_symlink() or wal.is_symlink() or not database.is_file():
            return set()
        with tempfile.TemporaryDirectory(prefix='torii-opencode-receipts-') as temporary:
            snapshot = Path(temporary) / database.name
            shutil.copyfile(database, snapshot)
            if wal.is_file():
                shutil.copyfile(wal, Path(temporary) / wal.name)
            with sqlite3.connect(snapshot.as_uri() + '?mode=rw', uri=True) as db:
                parts = db.execute("SELECT p.id, p.message_id, json_extract(p.data, '$.text') "
                    "FROM part AS p JOIN message AS m ON m.id = p.message_id AND m.session_id = p.session_id "
                    "WHERE (? OR p.session_id = ?) AND json_extract(m.data, '$.role') = 'user' "
                    "AND json_extract(p.data, '$.type') = 'text'", (all_sessions, session_id))
                wanted = set(rows)
                return {int(row) for _, _, text in parts if isinstance(text, str)
                        for row in re.findall(r'\bmessage=(\d+) kind=', text) if int(row) in wanted}
    except (OSError, ValueError, sqlite3.Error, TypeError):
        return set()


def parent_ready(store):
    from . import opencode_accounts, opencode_subscription
    from .session import AccountsUnavailable
    alias = opencode_accounts.active(store)
    block = opencode_subscription.account_block(store, alias)
    until = block.get('until')
    try:
        if store.get('opencode_enabled', False) is not True or alias is None:
            raise Refused('OpenCode is unavailable.')
        opencode_accounts.check_ready(store)
        if until is not None and until > time.time():
            raise Refused('OpenCode is paused.')
        if opencode_subscription.is_subscription(opencode_accounts.accounts(store)[alias]) and not opencode_subscription.available(store, alias):
            raise Refused('OpenCode subscription is unavailable.')
    except Refused:
        raise AccountsUnavailable(until, provider='opencode', alias=alias)
    return alias


def record_parent_block(store, alias, reason, retry_at=None):
    from . import opencode_accounts, opencode_subscription
    account = opencode_accounts.accounts(store).get(alias)
    if account is None or opencode_subscription.is_subscription(account):
        return
    now = time.time()
    blocks = store.get('opencode_account_blocks', {}) or {}
    blocks[alias] = {'until': retry_at if retry_at is not None else now + opencode_subscription.RECHECK_SECONDS,
                     'observed_at': now, 'reason': reason, 'reset_known': retry_at is not None}
    store.put('opencode_account_blocks', blocks)


def parent_retry_at(info):
    data = (info.get('error') or {}).get('data') or {}
    headers = data.get('responseHeaders') or {}
    if not isinstance(headers, dict):
        return None
    value = next((value for name, value in headers.items() if name.lower() == 'retry-after'), None)
    try:
        seconds = float(value)
    except (ValueError, TypeError):
        try:
            return parsedate_to_datetime(value).timestamp()
        except (ValueError, TypeError, OverflowError, AttributeError):
            return None
    completed = (info.get('time') or {}).get('completed')
    if (math.isfinite(seconds) and seconds >= 0 and type(completed) in (int, float)
            and math.isfinite(completed) and completed > 0):
        retry_at = completed / 1000 + seconds
        return retry_at if math.isfinite(retry_at) else None
    return None


def parent_adapter(problem, error):
    worker = adapter(problem, error=error)
    capture = {}

    async def guard_ready(protocol):
        spec = json.loads((protocol.process.directory / 'spec.json').read_text())
        root = Path(spec['env']['XDG_CONFIG_HOME']).parent
        launch, ready = read_object(root / 'guard-launch.json'), read_object(root / 'guard-ready.json')
        if (not launch or launch != ready or type(launch.get('version')) is not int or launch['version'] != 1
                or not isinstance(launch.get('launch'), str) or len(launch['launch']) != 32):
            protocol.complete('failed', 'provider_guard_unavailable')
        else:
            capture.update(root=root, launch=launch['launch'])

    def before_prompt(session_id):
        prepare_error(capture['root'], session_id)

    async def permission(params):
        problem('coordinator', 'opencode-permission-rejected', 'OpenCode requested a tool outside the parent config.')
        option = next((item.get('optionId') for item in params.get('options', [])
                       if item.get('kind') == 'reject_once'), None)
        return {'outcome': {'outcome': 'selected', 'optionId': option}} if option is not None else {
            'outcome': {'outcome': 'cancelled'}}

    def cumulative_usage(result, update):
        spend = usage(result, update)
        cost = (update or {}).get('cost') or {}
        amount = cost.get('amount')
        if cost.get('currency') == 'USD' and type(amount) in (int, float) and math.isfinite(amount) and amount >= 0:
            spend['usd_estimate'] = amount
        return spend

    return AcpAdapter(argv=argv, environment=parent_environment, auth_step=guard_ready,
                      check_version=worker.check_version, valid_session_id=worker.valid_session_id,
                      permission=permission, classify_error=worker.classify_error, queued_receipt='queued',
                      usage=cumulative_usage, allow_version_change_on_reattach=True, before_prompt=before_prompt)


def classify_error(method, error):
    text = json.dumps(error).lower()
    if method == 'session/load' and ('sessionnotfound' in text or 'not found' in text
            or error.get('data') == {'service': 'session'}):
        return 'session_missing'
    data = error.get('data')
    if method == 'session/prompt' and isinstance(data, dict) and data.get('errorName') == 'ContextOverflowError':
        return 'context_overflow'
    return 'provider_error'


def prepare_error(root, session_id):
    launch = read_object(Path(root) / 'guard-launch.json')
    write_private(Path(root) / 'provider-error.json', (json.dumps({
        'version': 1, 'launch': launch['launch'], 'session_id': session_id, 'error': None}) + '\n').encode())


def parent_error(root, session_id):
    marker = read_object(Path(root) / 'provider-error.json', limit=4096)
    launch = read_object(Path(root) / 'guard-launch.json')
    if (marker and launch and marker.get('version') == 1 and marker.get('launch') == launch.get('launch')
            and marker.get('session_id') == session_id and isinstance(marker.get('error'), dict)):
        return marker['error']
    return None


def records(root, session_id):
    """Read a stopped worker's database through a disposable copy, including its WAL.

    SQLite never opens the source database, so it cannot create or change its
    shared-memory or WAL files. Only the adapter's worker-local files are read.
    """
    root = Path(root).resolve()
    directory = root / 'data' / 'opencode'
    try:
        directory.resolve().relative_to(root)
        database = directory / 'opencode.db'
        wal = directory / 'opencode.db-wal'
        if database.is_symlink() or wal.is_symlink() or not database.is_file():
            return None
        with tempfile.TemporaryDirectory(prefix='torii-opencode-records-') as temporary:
            snapshot = Path(temporary) / 'opencode.db'
            shutil.copyfile(database, snapshot)
            if wal.is_file():
                shutil.copyfile(wal, Path(temporary) / wal.name)
            with sqlite3.connect(snapshot.as_uri() + '?mode=rw', uri=True) as db:
                rows = db.execute('SELECT id, data FROM message WHERE session_id=? '
                                  'ORDER BY time_created DESC, id DESC', (session_id,))
                latest = next(((mid, data) for mid, raw in rows
                               for data in (json.loads(raw),) if data.get('role') == 'assistant'), None)
                if latest is None:
                    return None
                mid, info = latest
                parts = [json.loads(row[0]) for row in db.execute(
                    'SELECT data FROM part WHERE session_id=? AND message_id=? ORDER BY id', (session_id, mid))]
                return info, parts
    except (OSError, ValueError, sqlite3.Error, TypeError, AttributeError):
        return None


def finish(result, root, alias, error, *, launch_root=None, subscription=False):
    from . import opencode_accounts, opencode_subscription, opencode_recovery
    detail = error.get('message', '')
    if detail.startswith('Internal error: '):
        detail = detail[len('Internal error: '):]
    if result.failure_code != 'owner_stopped':
        try:
            opencode_accounts.check_key(alias)
        except Refused:
            result.success = False
            result.failure_code = 'auth'
            detail = 'key file is missing or unreadable'
        else:
            saved = records(root, result.session_id) if result.session_id and (result.success or error) else None
            if saved is None and result.success:
                result.success = False
                result.failure_code = 'provider_records_unavailable'
            if saved is not None:
                info, parts = saved
                if result.failure_code in ('provider_error', 'context_overflow'):
                    provider = opencode_recovery.provider_error(info.get('error') or {})
                    result.failure_code = opencode_recovery.classify(provider, result.failure_code)
                    if result.failure_code in ('out_of_credit', 'rate_limited', 'usage_limited'):
                        result.rate_limit_info = {'retry_at': provider['reset_at']}
                if subscription and not result.success:
                    data = (info.get('error') or {}).get('data') or {}
                    limits = opencode_subscription.limit_info(data, detail)
                    if limits is not None:
                        result.rate_limit_info = limits
                        result.quota_limited = True
                        result.failure_code = 'quota_limited'
                if result.success and info.get('finish') == 'length':
                    result.success = False
                    result.failure_code = 'max_tokens'
                if result.success:
                    completed = []
                    for part in parts:
                        if part.get('type') == 'step-start':
                            completed.clear()
                        elif part.get('type') == 'text' and (part.get('time') or {}).get('end') is not None:
                            completed.append(part.get('text', ''))
                    result.text = ''.join(completed)[-200000:]
            if not result.success and result.failure_code in ('provider_error', 'context_overflow'):
                marker = parent_error(launch_root or root, result.session_id)
                if marker:
                    result.failure_code = opencode_recovery.classify(marker, result.failure_code)
                    result.rate_limit_info = {'retry_at': opencode_subscription.bounded_reset(marker.get('reset_at'))}
        if incomplete_marker(launch_root or root, result.session_id):
            result.success = False
            result.failure_code = 'provider_stream_incomplete'
            detail = ''
        if not result.success and result.failure_code in ('out_of_credit', 'rate_limited', 'usage_limited'):
            result.quota_limited = True
        if subscription and not result.success:
            if not result.quota_limited:
                result.rate_limit_info = opencode_subscription.limit_info(error.get('data') or {}, detail)
                if result.rate_limit_info is not None:
                    result.quota_limited = True
                    result.failure_code = 'quota_limited'
            detail = ''
    if not result.success:
        key_path = str(opencode_accounts.account_home(alias) / 'key')
        if key_path in detail or '{file:' in detail:
            detail = ''
        result.error = ('%s failed: %s%s' % (
            'OpenCode subscription' if subscription else 'OpenCode account ' + alias, result.failure_code or 'provider_error', ': ' + detail if detail else '.'))[:300]


def usage(result, last_update, previous_cost=0):
    tokens = result.get('usage') or {}
    cost = (last_update or {}).get('cost') or {}
    amount = cost.get('amount')
    estimate = (max(0, amount - previous_cost) if cost.get('currency') == 'USD'
                and type(amount) in (int, float) and math.isfinite(amount) and amount > 0 else None)
    spend = {'usd_estimate': estimate, 'source': SOURCE}
    for target, name in (('input_tokens', 'inputTokens'), ('output_tokens', 'outputTokens'),
                         ('cached_tokens', 'cachedReadTokens')):
        value = tokens.get(name, 0)
        spend[target] = value if type(value) is int and value >= 0 else 0
    return spend


def adapter(problem, previous_cost=0, error=None):
    from . import opencode_accounts
    launch_root = None

    def isolated_environment(root, base):
        nonlocal launch_root
        root = Path(root)
        launch_root = root / 'launches' / uuid.uuid4().hex
        env = environment(launch_root, base)
        data = root / 'data'
        data.mkdir(parents=True, exist_ok=True, mode=0o700)
        data.chmod(0o700)
        env['XDG_DATA_HOME'] = str(data)
        return env

    async def guard_ready(protocol):
        nonlocal launch_root
        if launch_root is None:
            spec = json.loads((protocol.process.directory / 'spec.json').read_text())
            launch_root = Path(spec['env']['XDG_CONFIG_HOME']).parent
        launch = read_object(launch_root / 'guard-launch.json')
        ready = read_object(launch_root / 'guard-ready.json')
        if (not launch or not ready or type(launch.get('version')) is not int or launch.get('version') != 1
                or type(ready.get('version')) is not int or ready != launch
                or not isinstance(launch.get('launch'), str) or len(launch['launch']) != 32):
            protocol.complete('failed', 'provider_guard_unavailable')

    async def permission(params):
        problem('worker', 'opencode-permission-rejected', 'OpenCode requested permission outside the worker config.')
        option = next((item.get('optionId') for item in params.get('options', [])
                       if item.get('kind') == 'reject_once'), None)
        return {'outcome': {'outcome': 'selected', 'optionId': option}} if option is not None else {
            'outcome': {'outcome': 'cancelled'}}

    def classify(method, value):
        if error is not None and method == 'session/prompt':
            error.update(value)
        return classify_error(method, value)

    def before_prompt(session_id):
        prepare_error(launch_root, session_id)

    return AcpAdapter(argv=argv, environment=isolated_environment, auth_step=guard_ready,
                      check_version=lambda result: (result.get('agentInfo') or {}).get('version') == opencode_accounts.PINNED_VERSION,
                      valid_session_id=lambda sid: native_ids.valid('opencode', sid), permission=permission,
                      classify_error=classify, queued_receipt='queued',
                      usage=lambda result, update: usage(result, update, previous_cost),
                      allow_version_change_on_reattach=True, before_prompt=before_prompt)


class OpenCodeBroker:
    def __init__(self, store):
        self.store = store

    def waiting_result(self, alias, worker_id):
        if worker_id is None:
            return {}
        row = self.store.db.execute('SELECT result,account_alias FROM workers WHERE id=?', (worker_id,)).fetchone()
        result = json.loads(row['result'] or '{}') if row and row['account_alias'] == alias else {}
        return result if result.get('status') == 'waiting_for_quota' else {}

    def block(self, alias, worker_id=None):
        from . import opencode_subscription
        block = opencode_subscription.account_block(self.store, alias, worker_id)
        previous = self.waiting_result(alias, worker_id)
        until = previous.get('waiting_until')
        if previous.get('retry_seconds') and isinstance(until, (int, float)) and until > block.get('until', 0):
            block = dict(block, until=until)
        return block

    def select(self, alias=None, worker_id=None):
        from . import opencode_accounts, opencode_subscription
        alias = alias or opencode_accounts.active(self.store)
        account = opencode_accounts.accounts(self.store).get(alias, {})
        if self.store.get('opencode_enabled', False) is not True or not account:
            return None
        if not opencode_subscription.is_subscription(account):
            block = self.block(alias, worker_id)
            return (None if block.get('reason') in ('out_of_credit', 'rate_limited', 'usage_limited')
                    and block.get('until', 0) > time.time() else alias)
        return alias if opencode_subscription.available(self.store, alias, worker_id) else None

    async def ready(self, alias, worker_id=None):
        from . import opencode_accounts, opencode_subscription
        account = opencode_accounts.accounts(self.store).get(alias, {})
        if opencode_subscription.is_subscription(account):
            if opencode_subscription.launch_state(self.store, alias, worker_id) in opencode_subscription.AUTH_ERRORS:
                return True
            if self.block(alias, worker_id).get('until', 0) > time.time():
                return False
            return await opencode_subscription.ready(self.store, alias, worker_id)
        return self.select(alias, worker_id) is not None

    def earliest_reset(self, alias=None, worker_id=None):
        from . import opencode_accounts
        alias = alias or opencode_accounts.active(self.store)
        return self.block(alias, worker_id).get('until')

    def record_result(self, alias, result, worker_id=None):
        from . import opencode_accounts, opencode_subscription
        blocks = self.store.get('opencode_account_blocks', {}) or {}
        current = blocks.get(alias, {})
        previous = self.waiting_result(alias, worker_id)
        if previous.get('retry_seconds') and current.get('worker_id') == worker_id:
            previous = dict(previous, retry_seconds=max(previous.get('retry_seconds', 0),
                                                        current.get('retry_seconds', 0)))
        if result.success:
            if current.get('retry_seconds') and current.get('worker_id') in (None, worker_id):
                with self.store.db:
                    blocks.pop(alias)
                    self.store.put('opencode_account_blocks', blocks)
        elif opencode_subscription.is_subscription(opencode_accounts.accounts(self.store).get(alias, {})) and result.quota_limited:
            with self.store.db:
                opencode_subscription.record_limit(self.store, alias, result.rate_limit_info, backoff=True,
                                         previous=previous, worker_id=worker_id)
        elif result.failure_code in ('out_of_credit', 'rate_limited', 'usage_limited'):
            with self.store.db:
                blocks[alias] = opencode_subscription.refusal_block(previous, result.failure_code,
                                                        (result.rate_limit_info or {}).get('retry_at'))
                block = blocks[alias]
                block.update(worker_id=worker_id, account_until=(block['until'] if block['reset_known'] else
                                                                 block['observed_at'] + opencode_subscription.RECHECK_SECONDS))
                self.store.put('opencode_account_blocks', blocks)

    async def run(self, run, provider, prompt, cwd, session_id, *, notice_topic=None, on_account=None, **options):
        from . import opencode_accounts, opencode_subscription
        from .providers import RunResult
        alias = options.pop('account_alias', None)
        worker_id = options.get('worker_id')
        if self.store.get('opencode_enabled', False) is not True:
            raise Refused('OpenCode workers are off.')
        if options.get('fresh') and not opencode_subscription.is_subscription(opencode_accounts.accounts(self.store).get(alias, {})):
            alias = opencode_accounts.active(self.store)
            if alias is None:
                raise Refused('No OpenCode account is active.')
            opencode_accounts.check_key(alias)
        elif not resume_account_ready(self.store, alias):
            return RunResult(session_id, failure_code='account_changed',
                             error='The saved OpenCode account is no longer ready.')
        if on_account:
            on_account(alias)
        if opencode_subscription.is_subscription(opencode_accounts.accounts(self.store).get(alias, {})):
            state = opencode_subscription.launch_state(self.store, alias, worker_id)
            if state in opencode_subscription.AUTH_ERRORS:
                return RunResult(session_id, failure_code='auth',
                                 error='OpenCode subscription failed: %s' % opencode_subscription.ERRORS[state])
            if not await self.ready(alias, worker_id):
                if state == 'usage_unavailable':
                    with self.store.db:
                        opencode_subscription.record_unavailable(self.store, alias)
                    return RunResult(session_id, failure_code='accounts_unavailable',
                                     error='OpenCode subscription usage is unavailable; Torii will check again.')
                return RunResult(session_id, quota_limited=True, failure_code='quota_limited',
                                 error='OpenCode subscription is paused; usage will be checked again.')
        elif not self.select(alias, worker_id):
            block = self.block(alias, worker_id)
            return RunResult(session_id, quota_limited=True, failure_code=block.get('reason', 'accounts_unavailable'),
                             error='OpenCode account %s is paused; Torii will check again.' % alias)
        result = await run(provider, prompt, cwd, session_id, account_alias=alias, **options)
        self.record_result(alias, result, worker_id)
        return result


def resume_account_ready(store, alias):
    from . import opencode_accounts
    account = opencode_accounts.accounts(store).get(alias)
    if not isinstance(account, dict) or account.get('enabled', True) is not True:
        return False
    try:
        opencode_accounts.check_key(alias)
    except Refused:
        return False
    return True


ADAPTERS = {'opencode': adapter}
