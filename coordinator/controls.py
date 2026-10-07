"""Local owner controls. The authenticated caller owns the SQLite transaction."""

from datetime import datetime, timezone
import asyncio
from concurrent.futures import ThreadPoolExecutor
import math
import os
import tempfile
import logging
import re
import shutil
import time
from pathlib import Path

from .control_api import Refused, Result
from .policy import COORDINATOR_MODEL, usage_policy
from .account_status import identity_label
from .accounts import (AccountBroker, account_block, account_label, authenticated, listed_accounts, signed_in,
                       switch_threshold)


_ALIAS = re.compile(r'[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z')
_MODEL = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z')
_USAGE = {
    '/help': '/help', '/setup': '/setup [new NAME|use PROJECT_OR_PATH|cancel] (link this channel to a project)',
    '/start': '/start', '/accounts': '/accounts', '/projects': '/projects', '/health': '/health',
    '/tldr': '/tldr (summarize messages since your last message in this topic)',
    '/project': '/project new NAME, or /project folder /absolute/path',
    '/service': '/service restart REASON',
    '/secrets': '/secrets',
}
_HELP = '''Torii runs Claude and ChatGPT agents on your projects. Send a normal message in a project's topic to give it work. Send another to continue.

**Accounts**
/accounts - usage, sign-ins, resets, models

**Projects**
/projects - your projects, new projects, this topic's folder

**Work**
/tldr - catch up on this topic since your last message
/goal CONDITION - keep the agent working until CONDITION holds

**Service**
/health - running agents and system load
/secrets - stored keys: rotate, revoke, ask again
/ping - check that Torii is listening
/help - this list'''


def _number(value):
    return type(value) in (int, float) and math.isfinite(value)


def _bar(left):
    segments = math.ceil(max(0, min(100, left)) / 10)
    return '🟩' * segments + '⬜' * (10 - segments)


def _meter(used):
    left = _remaining(used)
    return f'{_bar(left)} {left:g}%'


def _remaining(used):
    return max(0, min(100, 100 - used))


def _window_left(window, block=None):
    value = window.get('utilization')
    reset = window.get('resets_at')
    held = window.get('blocked') is True and not (_number(reset) and reset <= time.time())
    if not _number(value) and held:
        return 0, held
    return (_remaining(value) if _number(value) else None), False


def _until(reset):
    if not _number(reset):
        return 'now'
    remaining = math.floor(reset - time.time())
    if remaining <= 0:
        return 'now'
    if remaining >= 24 * 60 * 60:
        days = remaining // (24 * 60 * 60)
        hours = remaining % (24 * 60 * 60) // (60 * 60)
        return f'{days}d {hours}h'
    hours = remaining // (60 * 60)
    minutes = remaining % (60 * 60) // 60
    return f'{hours}h {minutes}m'


def _timestamp(value):
    if not _number(value):
        return 'unknown'
    try:
        return datetime.fromtimestamp(value, timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    except (OverflowError, OSError, ValueError):
        return 'unknown'


def _safe(value, pattern, default='unknown'):
    return value if isinstance(value, str) and pattern.fullmatch(value) else default


def _mapping(value):
    return value if isinstance(value, dict) else {}


def blocked(store, alias):
    return account_block(store, alias)


def resets(until):
    return 'resets in ' + _until(until) if _number(until) else 'resets later'


def _account_card(store, alias, active=False):
    account = _mapping(_mapping(store.get('accounts', {})).get(alias))
    snapshot = _mapping(_mapping(store.get('account_status', {})).get(alias))
    identity = _mapping(snapshot.get('identity'))
    block = blocked(store, alias)

    def label(key, default):
        return identity_label(identity.get(key)) or default

    state = ('not signed in' if not authenticated(store, alias, account) else
             'enabled' if account.get('enabled') is True else 'disabled')
    display = account_label(store, alias)
    lines = [f'Account: {display} ({state})' + (' · next dispatch' if active else ''),
             'Type: ' + label('type', 'Claude — not checked yet')]
    threshold = switch_threshold(store, alias)
    if threshold != switch_threshold(store):
        lines.append(f'reserve {round((1 - threshold) * 100)}% (local override)')
    if state.startswith('not signed in'):
        lines.append('Sign in from /accounts.')
    usage = _mapping(snapshot.get('usage'))
    for key, title in (('five_hour', '5-hour'), ('seven_day_fable', 'Fable weekly'), ('seven_day', 'Weekly (all models)')):
        window = _mapping(usage.get(key))
        value = window.get('utilization')
        reset = window.get('resets_at')
        left, held = _window_left(window)
        if left == 0 and not _number(value):
            until = reset if held and _number(reset) else block.get('until') if block else None
            lines.append(f'{title}: {_bar(0)} {resets(until)}')
        elif _number(value):
            lines.append(f'{title}: {_meter(value)} · {resets(reset)}')
        else:
            lines.append(f'{title}: no data')
    state = AccountBroker(store).account_state(alias)
    if state['state'] == 'stale':
        lines.append('Account state: stale since ' + _timestamp(state.get('since')) + '.')
    elif state['state'] == 'limited':
        broker = AccountBroker(store)
        hold = broker.blocks().get(alias)
        windows = broker.full_windows(alias)
        if hold and hold.get('reason') != 'threshold':
            cause = 'Claude refused requests' if hold.get('reason') == 'quota' else 'account hold'
        elif windows:
            meter = max(windows, key=lambda key: windows[key]['resets_at'] if _number(windows[key].get('resets_at')) else 0)
            title = {'five_hour': '5-hour', 'seven_day': 'weekly', 'seven_day_fable': 'Fable weekly'}[meter]
            point = f'{round(threshold * 100)}%' if threshold == switch_threshold(store) else f'its {round((1 - threshold) * 100)}% reserve'
            cause = f'{title} reached {point}'
        else:
            cause = 'account hold'
        until = state.get('until')
        if not _number(until) and hold:
            resets_at = [window.get('resets_at') for window in broker.full_windows(alias, snapshot).values()]
            until = max((reset for reset in resets_at if _number(reset) and reset > time.time()), default=None)
        back = 'back in ' + _until(until) if _number(until) else 'reset unknown'
        lines.append(f'Paused: {cause} · {back}')
    else:
        lines.append('Account state: available.')
    observed = snapshot.get('observed_at')
    if _number(observed):
        stale = ' (stale)' if time.time() - observed > 600 else ''
        lines.append('Usage checked: ' + _timestamp(observed) + stale + '. Cached values.')
    else:
        lines.append('Usage: unknown. The background check has not returned usage yet.')
    errors = {'login_required': 'Sign in from /accounts.',
              'cli_unavailable': 'Claude CLI is unavailable on the host. Install it to check usage.',
              'timeout': 'The last usage check timed out. Saved values may be old.',
              'usage_unavailable': 'Claude did not return usage. Check sign-in and update the Claude CLI on the host.'}
    error_message = errors.get(snapshot.get('error'))
    if error_message:
        lines.append(error_message)
        lines.append('Last check attempt: ' + _timestamp(snapshot.get('checked_at')))
    return '\n'.join(lines)


def _codex(store, alias):
    return alias in _mapping(store.get('codex_accounts', {}))


def codex_heading(store):
    from .codex_accounts import CodexBroker
    return 'Codex accounts · automatic switching ' + ('on' if CodexBroker(store).automatic() else 'off')


def _codex_card(store, alias, active=False):
    from .codex_accounts import CodexBroker
    broker = CodexBroker(store)
    snapshot = broker.snapshot(alias)
    identity = _mapping(snapshot.get('identity'))
    state = ('not signed in' if not broker.authenticated(alias) else
             'enabled' if broker.accounts()[alias].get('enabled') is True else 'disabled')
    lines = [f'Account: {broker.label(alias)} ({state})' + (' · active' if active else ''),
             'Type: ' + (identity_label(identity.get('type')) or 'Codex — not checked yet')]
    if state == 'not signed in':
        lines.append('Sign in from /accounts.')
    usage = _mapping(snapshot.get('usage'))
    for key, title in (('five_hour', '5-hour'), ('seven_day', 'Weekly')):
        window = _mapping(usage.get(key))
        if _number(window.get('utilization')):
            lines.append(f"{title}: {_meter(window['utilization'])} · resets in {_until(window.get('resets_at'))}")
    if not any(_number(_mapping(usage.get(key)).get('utilization')) for key in ('five_hour', 'seven_day')):
        lines.append('Usage: no data')
    credits = _mapping(snapshot.get('credits'))
    if credits:
        balance = identity_label(credits.get('balance')) or '0'
        lines.append('Credits: ' + ('unlimited' if credits.get('unlimited') else
                                    balance if credits.get('has_credits') else 'none'))
    if type(snapshot.get('resets_available')) is int:
        lines.append('Usage resets available: %d' % snapshot['resets_available'])
    account_state = broker.account_state(alias)
    if account_state['state'] == 'stale':
        lines.append('Account state: stale since ' + _timestamp(account_state.get('since')) + '.')
    elif account_state['state'] == 'limited':
        lines.append('Account state: limited ' + resets(account_state.get('until')) + '.')
    else:
        lines.append('Account state: available.')
    observed = snapshot.get('observed_at')
    if _number(observed):
        lines.append('Usage checked: ' + _timestamp(observed) + (' (stale)' if time.time() - observed > 600 else '') +
                     '. Cached values.')
    else:
        lines.append('Usage: unknown. The background check has not returned usage yet.')
    errors = {'login_required': 'Sign in from /accounts.',
              'cli_unavailable': 'Codex CLI is unavailable on the host. Install it to check usage.',
              'timeout': 'The last usage check timed out. Saved values may be old.',
              'usage_unavailable': 'Codex did not return usage. Check sign-in and update the Codex CLI on the host.'}
    if errors.get(snapshot.get('error')):
        lines.append(errors[snapshot['error']])
        lines.append('Last check attempt: ' + _timestamp(snapshot.get('checked_at')))
    return '\n'.join(lines)


def _change(store, setting, value):
    store.put(setting, value)
    if setting == 'codex_active_account':
        store.put('coordinator_account_retry_at', None)
        store.put('coordinator_account_notice', None)
    logging.getLogger(__name__).info('setting changed key=%s', setting)


def _project_path(store, value):
    """An absolute folder, or the unique name of a folder in the project catalog."""
    from .onboarding import _catalog, _existing_path
    matches = [entry for entry in _catalog(store) if entry['name'].casefold() == value.casefold()]
    if len(matches) > 1:
        raise Refused('Several projects have that name. Send the full folder path.')
    if matches:
        return Path(matches[0]['path'])
    return None


def _change_folder(store, topic_id, raw):
    """Point this topic at a moved project. Only topics.cwd changes.

    The saved conversation is resumed from its own absolute transcript path, not
    from the folder, so the session survives the move untouched.
    """
    topic = store.topic(topic_id)
    if not topic or not topic.get('cwd'):
        return 'This channel has no project folder yet. Use Link this topic in /projects first.'
    value = raw.strip()
    candidate = _project_path(store, value)
    if candidate is None:
        try:
            candidate = Path(value).expanduser()
        except (OSError, ValueError, RuntimeError):
            return 'That is not a usable folder path.'
        if not candidate.is_absolute():
            return 'Send an absolute path. It must start with a slash.'
        if not candidate.is_dir():
            return f'{candidate} is not an existing folder on this Mac.'
    busy = store.db.execute("SELECT status FROM tasks WHERE topic=? AND status='open' ORDER BY id LIMIT 1",
                            (topic_id,)).fetchone()
    if busy:
        return (f'This channel has {busy["status"]} work. Let it finish '
                'before moving the project folder.')
    new = str(candidate.resolve())
    old = topic['cwd']
    if new == old:
        return f'Project folder is already {new}.'
    store.db.execute('UPDATE topics SET cwd=? WHERE id=?', (new, topic_id))
    logging.getLogger(__name__).info('project folder changed topic=%s', topic_id)
    return f'Project folder: {old} → {new}. Sessions and settings unchanged.'


def _status_card(store, topic_id):
    """The facts the owner asks for most. Everything else is one tap down."""
    topic = store.topic(topic_id) or {}
    listed, account_alias = listed_accounts(store)
    alias = account_label(store, account_alias) if account_alias else 'none available'
    worker = store.get('worker_model', 'opus')
    five = _mapping(_mapping(_mapping(_mapping(store.get('account_status', {})).get(account_alias)).get('usage')).get('five_hour'))
    percent = five.get('utilization')
    accounts = _mapping(store.get('accounts', {}))
    if not account_alias and any(signed_in(store, name, accounts.get(name)) for name in listed):
        account = 'all accounts limited · ' + resets(AccountBroker(store).earliest_reset())
    elif not account_alias:
        account = 'none available'
    elif _number(percent):
        account = f'{alias} · {_meter(percent)} · resets in {_until(five.get("resets_at"))}'
    else:
        account = alias + ' · usage not checked yet'
    state = AccountBroker(store).account_state(account_alias) if account_alias else {}
    if state.get('state') == 'stale' and _number(state.get('since')):
        account += ' · stale since ' + _timestamp(state['since'])
    main = store.get('coordinator_session_topic') or store.get('coordinator_home_topic')
    key = 'coordinator' if topic_id is None or topic_id == main else 'coordinator:' + topic_id
    host = store.get(key + '_host') or {}
    from .codex_accounts import CodexBroker
    provider = host.get('provider') or store.get(key + '_provider') or (
        'claude' if account_alias or not CodexBroker(store).managed() else 'codex')
    setting = 'codex_model' if provider == 'codex' else 'coordinator_model'
    default = 'gpt-6.1-sol' if provider == 'codex' else COORDINATOR_MODEL
    coordinator = host.get('model') or store.get(setting) or default
    task = store.db.execute("SELECT id,number,title,status FROM tasks WHERE topic=? AND status='open'"
                            ' ORDER BY id LIMIT 1', (topic_id,)).fetchone()
    active = (store.db.execute("SELECT id,effort FROM workers WHERE task=? AND status IN "
                               "('queued','running','waiting_for_secret','waiting_for_quota','needs_input') ORDER BY id",
                               (task['id'],)).fetchall() if task else [])
    lines = [
        (topic.get('name') or 'No project set up yet') +
        ('' if not topic else ' · ' + ('accepting work' if topic.get('enabled') else 'disabled')),
        topic.get('cwd') or 'No folder bound. Use /setup.',
        'Chat agent: ' + ('ChatGPT' if provider == 'codex' else 'Claude'),
        'Models: Claude workers ' + ('native default' if worker is None else _safe(worker, _MODEL)) +
        ' · chat agent ' + _safe(coordinator, _MODEL),
        'Account: ' + account,
        (f"Job {task['number']}: {task['status']} · {task['title']}" +
         ((' · workers ' + ', '.join('%d %s' % (row['id'], row['effort']) for row in active)) if active else ''))
        if task else 'No open job in this channel.',
    ]
    if store.get('setup_problem'):
        lines.append('Setup problem: ' + store.get('setup_problem'))
    if store.get('group_owner_changed'):
        lines.append(store.get('group_owner_changed'))
    return '\n'.join(lines)


def _accounts(store):
    from .codex_accounts import CodexBroker
    ordered, active = listed_accounts(store)
    aliases = [alias for alias in ordered if _safe(alias, _ALIAS, '')]
    codex, codex_active = CodexBroker(store).listed()
    codex = [alias for alias in codex if _safe(alias, _ALIAS, '')]
    if not aliases and not codex:
        return 'No accounts are registered. Open /accounts to add one.'
    sections = [codex_heading(store) + '\n\n' + '\n\n'.join(_codex_card(store, alias, alias == codex_active)
                                                   for alias in codex)] if codex else []
    if not aliases:
        return sections[0]
    cards = []
    identities = {}
    snapshots = _mapping(store.get('account_status', {}))
    for alias in aliases:
        card = _account_card(store, alias, alias == active)
        email = _mapping(_mapping(snapshots.get(alias)).get('identity')).get('email')
        if isinstance(email, str) and email:
            first_alias = identities.setdefault(email, alias)
            if first_alias != alias:
                card += '\nShares this login and usage pool with ' + first_alias + '.'
        cards.append(card)
    empty = '' if active else 'No account is available for the next dispatch.\n\n'
    return '\n\n'.join([empty + '\n\n'.join(cards)] + sections)


def _current_account(store):
    from .codex_accounts import CodexBroker
    codex = CodexBroker(store)
    accounts = _mapping(store.get('accounts', {}))
    if codex.accounts():
        current = codex.current()
        line = ('Next Codex account: ' + (codex.label(current) if current else 'none available') + ' · ' +
                ('automatic switching on' if codex.automatic() else 'active account, automatic switching off'))
        return line + '\n\n' + (_current_claude(store, accounts) if accounts else _accounts(store))
    return _current_claude(store, accounts)


def _current_claude(store, accounts):
    if not accounts:
        return _accounts(store)
    active = listed_accounts(store)[1]
    heading = 'Next account: ' + (account_label(store, active) if active else 'none available')
    if not active:
        return heading + '\n\n' + _accounts(store)
    return heading + '\n\n' + _account_card(store, active, True)


def op_settings_show(store, ctx, topic=None):
    return _status_card(store, topic)


def op_settings_get(store, ctx):
    from .onboarding import configured_root
    root = configured_root(store)
    data = dict(normal_model=store.get('worker_model', 'opus'),
                codex_enabled=store.get('codex_enabled', True),
                codex_model=store.get('codex_model'),
                codex_auto_switch=store.get('codex_auto_switch', False) is True,
                codex_active_account=_codex_active_label(store),
                coordinator_home_topic=store.get('coordinator_home_topic'),
                coordinator_model=store.get('coordinator_model'),
                projects_root=str(root) if root else None,
                account_switch_threshold=switch_threshold(store))
    return Result(True, '\n'.join('%s: %s' % (key, value) for key, value in sorted(data.items())), data)


def _codex_active_label(store):
    from .codex_accounts import CodexBroker
    broker = CodexBroker(store)
    active = broker.active()
    return broker.label(active) if active else None


def op_topics_list(store, ctx):
    rows = [{key: topic[key] for key in ('id', 'name', 'cwd', 'provider', 'enabled')}
            for topic in store.topics()]
    text = '\n'.join('%s · %s · %s · %s' % (row['id'], row['name'], row['cwd'] or 'no folder',
                                            'accepting work' if row['enabled'] else 'disabled')
                     for row in rows) or 'No channels are linked yet.'
    return Result(True, text, rows)


def op_topic_show(store, ctx, topic=None):
    row = store.topic(topic)
    if not row:
        raise Refused('That channel is not linked yet. Use /setup first.')
    tasks = store.tasks_list(topic=topic)
    data = dict(row, tasks=tasks)
    text = _status_card(store, topic)
    if tasks:
        text += '\n' + '\n'.join('Job %d: %s' % (task['number'], task['status']) for task in tasks)
    return Result(True, text, data)


def op_accounts_list(store, ctx):
    return _accounts(store)


def op_account_show(store, ctx, alias):
    if _codex(store, alias):
        from .codex_accounts import CodexBroker
        return _codex_card(store, alias, alias == CodexBroker(store).current())
    return _account_card(store, alias, alias == listed_accounts(store)[1])


def op_account_current(store, ctx):
    return _current_account(store)


def op_policy_show(store, ctx):
    return usage_policy(store.directory)


def op_topic_bind(store, ctx, topic, cwd, name, provider=None, session=None, enable=None, source_pid=None):
    try:
        store.bind(topic, cwd, name, provider, session, bool(enable), source_pid)
    except ValueError as error:
        raise Refused(str(error) + '.')
    return 'Channel %s is linked to %s. Accepting work: %s.' % (topic, cwd, bool(enable))


def op_topic_home(store, ctx, topic, clear=False):
    if clear:
        if store.get('coordinator_home_topic') != topic:
            raise Refused('That channel is not home.')
        if not store.get('coordinator_session_topic'):
            store.put('coordinator_session_topic', topic)
        store.put('coordinator_home_topic', None)
        return 'Home channel cleared. Saved conversations keep their owners.'
    row = store.topic(topic)
    if not row or not row['cwd'] or not row['enabled']:
        raise Refused('Choose an enabled, linked channel first.')
    if store.get('coordinator_home_topic') == topic:
        return 'This channel is already home.'
    if not store.get('coordinator_session_topic'):
        store.put('coordinator_session_topic', store.get('coordinator_home_topic'))
    store.put('coordinator_home_topic', topic)
    return 'Home channel saved.'


def op_topic_folder(store, ctx, topic=None, project=''):
    return _change_folder(store, topic, project)


def op_topic_rename(store, ctx, topic=None, name=''):
    if not store.topic(topic):
        raise Refused('This channel is not linked yet. Use /setup first.')
    try:
        previous = store.rename_topic(topic, name)
    except ValueError as error:
        raise Refused(str(error)) from error
    return f'Channel label: {previous} → {name}. A later Telegram title update will replace it.'


def op_topic_enable(store, ctx, topic=None):
    if not store.topic(topic):
        raise Refused('This channel is not linked yet. Use /setup first.')
    store.db.execute('UPDATE topics SET enabled=1 WHERE id=?', (topic,))
    logging.getLogger(__name__).info('topic enabled topic=%s', topic)
    return 'This channel accepts work. Send a request when you are ready.'


def op_topic_disable(store, ctx, topic=None):
    if not store.topic(topic):
        raise Refused('This channel is not linked yet. Use /setup first.')
    store.db.execute('UPDATE topics SET enabled=0 WHERE id=?', (topic,))
    logging.getLogger(__name__).info('topic disabled topic=%s', topic)
    return 'This channel will not claim new work. Running work continues until it finishes.'


def op_delegation_codex(store, ctx, enabled):
    state = 'on' if enabled else 'off'
    if enabled and not shutil.which('codex'):
        raise Refused('codex is not installed on this Mac. Install and sign in to its CLI locally, then enable it here.')
    _change(store, 'codex_enabled', enabled)
    return f'codex delegation: {state}. This applies to future dispatches and loaded worker guidance.'


def op_model_worker(store, ctx, model=None):
    _change(store, 'worker_model', model)
    return f"worker model: {model or 'native default'}. This setting applies to future dispatches."


def op_model_codex(store, ctx, model=None):
    _change(store, 'codex_model', model)
    return (f"codex model: {model or 'Codex CLI default'}. "
            'This setting applies to future Codex dispatches.')


def op_model_coordinator(store, ctx, model=None):
    _change(store, 'coordinator_model', model)
    return (f"coordinator model: {model or 'service default'}. "
            'This setting applies at the next coordinator launch.')


def op_policy_set(store, ctx, text):
    policy = text.strip()
    if not policy or len(policy) > 4000:
        raise Refused('Keep usage policy to 4000 characters or fewer.')
    path = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=store.directory,
                                         prefix='.USAGE-', delete=False) as temporary:
            path = Path(temporary.name)
            os.fchmod(temporary.fileno(), 0o600)
            temporary.write(policy)
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(path, store.directory / 'USAGE.md')
    finally:
        if path is not None:
            path.unlink(missing_ok=True)
    logging.getLogger(__name__).info('setting changed key=USAGE.md length=%s', len(policy))
    return 'Usage policy saved. Coordinators load it with their next message; new workers at launch.'


def _registered(store, alias):
    accounts = dict(_mapping(store.get('accounts', {})))
    if alias not in accounts or not isinstance(accounts[alias], dict):
        raise Refused('Account is not registered. Use /accounts to list registered aliases.')
    return accounts


def _set_codex_enabled(store, alias, enabled):
    from .codex_accounts import CodexBroker
    broker = CodexBroker(store)
    accounts = broker.accounts()
    profile = accounts[alias]
    profile.pop('awaiting_login', None)
    checking = enabled and not broker.authenticated(alias)
    accounts[alias] = dict(profile, enabled=False, awaiting_login=True) if checking else dict(profile, enabled=enabled)
    _change(store, 'codex_accounts', accounts)
    store.put('codex_account_status_refresh_requested', time.time())
    if checking:
        return f'Checking sign-in for {broker.label(alias)}. Torii enables it when the check passes.'
    return (f"Account {broker.label(alias)} is {'enabled' if enabled else 'disabled'}. "
            'Existing credentials and in-flight work are unchanged.')


def _set_enabled(store, alias, enabled):
    if _codex(store, alias):
        return _set_codex_enabled(store, alias, enabled)
    accounts = _registered(store, alias)
    profile = dict(accounts[alias])
    profile.pop('awaiting_login', None)
    checking = enabled and not authenticated(store, alias, profile)
    accounts[alias] = dict(profile, enabled=False, awaiting_login=True) if checking else dict(profile, enabled=enabled)
    _change(store, 'accounts', accounts)
    store.put('account_status_refresh_requested', time.time())
    display = account_label(store, alias)
    if checking:
        return f'Checking sign-in for {display}. Torii enables it when the check passes.'
    return (f"Account {display} is {'enabled' if enabled else 'disabled'}. "
            'Existing credentials and in-flight work are unchanged.')


def op_account_enable(store, ctx, alias):
    return _set_enabled(store, alias, True)


def op_account_disable(store, ctx, alias):
    return _set_enabled(store, alias, False)


def op_account_use(store, ctx, alias):
    from .codex_accounts import CodexBroker
    broker = CodexBroker(store)
    if not _codex(store, alias):
        if alias in _mapping(store.get('accounts', {})):
            raise Refused('Only Codex accounts have an active account. Torii switches Claude accounts automatically.')
        raise Refused('That Codex account is not registered. Open /accounts to see your accounts.')
    if not broker.signed_in(alias):
        raise Refused(f'{broker.label(alias)} is not signed in or is disabled. Sign it in or enable it first.')
    _change(store, 'codex_active_account', alias)
    text = (f'{broker.label(alias)} is now the active Codex account. New and resumed Codex work runs on it; '
            'a turn that is running finishes on its current account.')
    if broker.exhausted(alias):
        text += ' It is at its usage limit, so Codex work waits until it resets.'
    if broker.automatic():
        text += (' Automatic switching is on, so Torii still picks the account whose weekly limit resets soonest. '
                 'This account is used when you turn automatic switching off.')
    return text


def op_accounts_codex_auto(store, ctx, enabled):
    from .codex_accounts import CodexBroker
    _change(store, 'codex_auto_switch', enabled is True)
    if enabled is True:
        return ('Automatic Codex account switching is on. At 95% or a usage-limit rejection, Torii moves Codex work '
                'to the signed-in account whose weekly limit resets soonest.')
    broker = CodexBroker(store)
    active = broker.active()
    return ('Automatic Codex account switching is off. Codex work stays on the active account %s, and Torii tells '
            'you when it reaches 80%% used or its limit.' % (broker.label(active) if active else 'none'))


def op_account_reset(store, ctx, alias):
    if _codex(store, alias):
        from .codex_accounts import CodexBroker
        blocks = dict(_mapping(store.get('codex_account_blocks', {})))
        blocks.pop(alias, None)
        _change(store, 'codex_account_blocks', blocks)
        snapshot = dict(_mapping(store.get('codex_account_status', {})))
        if alias in snapshot:
            snapshot[alias] = dict(snapshot[alias], usage={}, observed_at=None)
            _change(store, 'codex_account_status', snapshot)
        store.put('codex_account_status_refresh_requested', time.time())
        return (f'Cleared the cached usage limit for {CodexBroker(store).label(alias)}. '
                'The next dispatch checks fresh usage.')
    _registered(store, alias)
    blocks = dict(_mapping(store.get('account_blocks', {})))
    blocks.pop(alias, None)
    _change(store, 'account_blocks', blocks)
    snapshot = dict(_mapping(store.get('account_status', {})))
    if alias in snapshot:
        snapshot[alias] = dict(snapshot[alias], usage={}, observed_at=None)
        _change(store, 'account_status', snapshot)
    store.put('account_status_refresh_requested', time.time())
    return f'Cleared the cached usage limit for {account_label(store, alias)}. The next dispatch checks fresh usage.'


def _redeem_setup(store, ctx, alias):
    from .codex_accounts import CodexBroker
    broker = CodexBroker(store)
    account = broker.accounts().get(alias)
    if account is None:
        raise Refused('That Codex account is not registered. Open /accounts to see your accounts.')
    topics = [row['id'] for row in store.topics() if row['enabled']]
    candidates = (ctx.topic, store.get('coordinator_home_topic'), *topics)
    topic = next((candidate for candidate in candidates if candidate and store.topic(candidate)
                  and store.topic(candidate)['enabled']), None)
    if topic is None:
        raise Refused('No enabled owner channel is available for the reset notice.')
    limited = bool(broker.blocks().get(alias))
    return broker, account, topic, limited


def _redeem_finish(store, broker, alias, topic, refreshed):
    from .codex_accounts import usage_line
    snapshots = dict(store.get('codex_account_status', {}) or {})
    if 'usage' in refreshed:
        snapshots[alias] = {**broker.snapshot(alias), **{key: value for key, value in refreshed.items()
                                                         if key != 'redeemed'}}
        store.put('codex_account_status', snapshots)
        blocks = dict(store.get('codex_account_blocks', {}) or {})
        blocks.pop(alias, None)
        store.put('codex_account_blocks', blocks)
    store.put('codex_account_status_refresh_requested', time.time())
    detail = usage_line(refreshed['usage']) if 'usage' in refreshed else refreshed['error']
    notice = ('Codex banked reset redeemed for %s. %s.' if refreshed['redeemed'] else
              'Codex banked reset outcome is unknown for %s. %s.') % (broker.label(alias), detail)
    store.enqueue_report(topic, notice)
    return Result(refreshed['redeemed'] is True, notice, refreshed,
                  state='done' if refreshed['redeemed'] is True else 'failed')


def _redeem_codex_account(store, ctx, alias, guarded):
    from .codex_accounts import redeem_codex_reset
    broker, account, topic, limited = _redeem_setup(store, ctx, alias)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(lambda: asyncio.run(redeem_codex_reset(account, guarded=guarded, limited=limited)))
        try:
            refreshed = future.result()
        except ValueError as error:
            raise Refused(str(error)) from error
    return _redeem_finish(store, broker, alias, topic, refreshed)


async def redeem_codex_account(store, topic, alias):
    from .codex_accounts import redeem_codex_reset
    from .control_api import Context
    broker, account, topic, limited = _redeem_setup(store, Context('telegram', topic), alias)
    refreshed = await redeem_codex_reset(account, guarded=True, limited=limited)
    with store.db:
        return _redeem_finish(store, broker, alias, topic, refreshed)


def op_account_codex_reset(store, ctx, alias):
    return _redeem_codex_account(store, ctx, alias, True)


def op_account_redeem(store, ctx):
    from .codex_accounts import CodexBroker
    alias = CodexBroker(store).active()
    if not alias:
        raise Refused('No active Codex account is registered.')
    return _redeem_codex_account(store, ctx, alias, True)


def op_accounts_discover(store, ctx):
    from .accounts import discover_accounts
    registered = discover_accounts(store)
    display = [account_label(store, alias) + (' · not signed in'
               if not authenticated(store, alias, profile) else '')
               for alias, profile in sorted(registered.items())]
    return Result(True, 'Registered account profiles: ' + (', '.join(display) or 'none found') + '.', display)



def _grammar(store, topic_id, command, args, text):
    """Return (op, params, render), a finished reply, or None for the usage text.

    Guards stay exactly where they were: a command that does not match its guard
    still gets the usage reply, never a refusal from an operation.
    """
    remainder = text.split(maxsplit=1)[1] if args else ''
    if command in ('/setup', '/start'):
        parts = remainder.split(maxsplit=1)
        action = parts[0].lower() if parts else ''
        value = parts[1].strip() if len(parts) > 1 else ''
        if action == 'new' and value:
            return 'topic.setup_new', {'name': value}, 'menu'
        if action == 'use' and value:
            return 'topic.setup_use', {'project': value}, 'menu'
        if action == 'cancel':
            return 'topic.setup_cancel', {}, 'menu'
        from .onboarding import setup_command
        return setup_command(store, topic_id, remainder)
    if command == '/projects':
        return 'projects.list', {}, 'menu'
    if command == '/help':
        return _HELP
    if command == '/health':
        return 'health.show', {}, 'menu'
    if command == '/accounts':
        return 'accounts.list', {}, 'menu'
    if command == '/project' and args[:1] == ['new']:
        from .onboarding import _create
        store.put('project_setup:' + topic_id, {'stage': 'project_new'})
        return _create(store, topic_id, ' '.join(args[1:]))
    if command == '/project' and len(args) >= 2 and args[0] == 'folder':
        return 'topic.folder', {'project': text.split(maxsplit=2)[2]}, 'menu'
    if command == '/secrets':
        return 'secret.list', {}, 'menu'
    if command == '/service' and len(args) >= 2 and args[0] == 'restart':
        return 'service.restart', {'reason': text.split(maxsplit=2)[2]}, 'menu'
    return None


def handle_control(store, topic_id: str, message_id: int, text: str, in_place: bool = False) -> bool:
    """Handle recognized controls without starting a provider or committing the caller."""
    from . import control_api
    from .control_ui import control_report
    parts = text.split() if isinstance(text, str) else []
    command = parts[0].split('@', 1)[0] if parts else ''
    if command not in _USAGE:
        return False
    args = parts[1:]
    if command == '/tldr' and not args:
        store.service_request('tldr', {'topic': topic_id, 'message': message_id})
        return True
    matched = _grammar(store, topic_id, command, args, text)
    render = 'menu'
    if isinstance(matched, tuple):
        op, params, render = matched
        reply = control_api.call(store, op, params, topic=topic_id, source='telegram',
                                 message=message_id).text
    else:
        reply = matched if isinstance(matched, str) else 'Usage: ' + _USAGE[command]
    control_report(store, topic_id, reply, command, args, message_id, in_place)
    return True
