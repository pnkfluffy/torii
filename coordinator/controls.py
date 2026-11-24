"""Local owner controls. The authenticated caller owns the SQLite transaction."""

from datetime import datetime, timezone
import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import math
import os
import logging
import re
import shutil
import sys
import time
from pathlib import Path

from .control_api import Refused, Result
from .policy import PARENT_EFFORT
from .account_status import identity_label
from .accounts import (PICK_GROUPS, AccountBroker, account_block, account_label, account_picks, authenticated,
                       listed_accounts, picks_value, signed_in, switch_threshold)
from .parent import session_key
from .parent_state import effective_agent, opencode_pick_problem
from .providers import codex_reasoning_effort


_ALIAS = re.compile(r'[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z')
_MODEL = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z')
_OPENCODE_MODEL_DISPLAY = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.:/-]{0,199}\Z')
_USAGE = {
    '/help': '/help', '/setup': '/setup [new NAME|use PROJECT_OR_PATH|cancel] (link this channel to a project)',
    '/start': '/start', '/accounts': '/accounts', '/projects': '/projects', '/health': '/health',
    '/login': '/login PROVIDER EMAIL (sign a listed account in again)',
    '/tldr': '/tldr (summarize messages since your last message in this topic)',
    '/project': '/project new NAME, or /project folder /absolute/path',
    '/service': '/service restart REASON',
    '/update': '/update',
    '/secrets': '/secrets',
    '/waiting': '/waiting',
    '/keep': '/keep [to dir:/absolute/path]',
    '/status': '/status',
}
_HELP = '''Torii runs Claude and ChatGPT agents on your projects. Send a normal message in a project's topic to give it work. Send another to continue.

**Accounts**
/accounts - usage, sign-ins, resets, models
/login PROVIDER EMAIL - sign a listed account in again

**Projects**
/projects - your projects, new projects, this topic's folder

**Work**
/tldr - catch up on this topic since your last message
/waiting - what is waiting on you
/goal CONDITION - keep the agent working until CONDITION holds
/keep - show or set this chat's keep destination

**Service**
/health - running agents and system load
/secrets - stored keys: rotate, revoke, ask again
/ping - check that Torii is listening
/help - this list'''


def _help(store):
    from .health import _codex, _subscription
    from .extension import optional
    antigravity = optional('antigravity_accounts')
    sections = [_HELP, _subscription(store), _codex(store) or '**ChatGPT left** (week)\nNo accounts',
                antigravity.health(store) if antigravity else None]
    return '\n\n'.join(filter(None, sections))


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


def _window_left(window):
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
    return account_block(store, alias, 'opus')


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
    for key, title in (('seven_day_fable', 'Fable weekly'), ('seven_day', 'Weekly (all models)')):
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
    state = AccountBroker(store).account_state(alias, 'opus')
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
            title = {'seven_day': 'weekly', 'seven_day_fable': 'Fable weekly'}[meter]
            point = f'{round(switch_threshold(store, alias, meter) * 100)}%' if meter == 'seven_day_fable' or threshold == switch_threshold(store) else f'its {round((1 - threshold) * 100)}% reserve'
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
    from .policy import chat_tags
    topic = store.topic(topic_id) or {}
    worker = store.get('worker_model', 'opus')
    listed, account_alias = listed_accounts(store, worker)
    alias = account_label(store, account_alias) if account_alias else 'none available'
    weekly = _mapping(_mapping(_mapping(_mapping(store.get('account_status', {})).get(account_alias)).get('usage')).get('seven_day'))
    percent = weekly.get('utilization')
    accounts = _mapping(store.get('accounts', {}))
    if not account_alias and any(signed_in(store, name, accounts.get(name)) for name in listed):
        account = 'all accounts limited · ' + resets(AccountBroker(store).earliest_reset(model=worker))
    elif not account_alias:
        account = 'none available'
    elif _number(percent):
        account = f'{alias} · {_meter(percent)} · resets in {_until(weekly.get("resets_at"))}'
    else:
        account = alias + ' · usage not checked yet'
    state = AccountBroker(store).account_state(account_alias, worker) if account_alias else {}
    if state.get('state') == 'stale' and _number(state.get('since')):
        account += ' · stale since ' + _timestamp(state['since'])
    main = store.get('coordinator_session_topic') or store.get('coordinator_home_topic')
    key = 'coordinator' if topic_id is None or topic_id == main else 'coordinator:' + topic_id
    host = store.get(key + '_host') or {}
    from .codex_accounts import CodexBroker
    provider = host.get('provider') or store.get(key + '_provider') or (
        'claude' if account_alias or not CodexBroker(store).managed() else 'codex')
    agent = effective_agent(store, key)
    pending = bool(host and (host.get('picked', False) != agent['picked'] or
                   agent['picked'] and agent['revision'] != store.get(key + '_selection_revision')))
    selected = agent
    if host:
        agent = dict(agent, provider=provider, model=host.get('model') or effective_agent(store, key, provider)['model'],
                     effort=host.get('effort') or effective_agent(store, key, provider)['effort'],
                     account=host.get('account'),
                     picked=host.get('picked', False))
    elif not agent['selection']:
        agent = effective_agent(store, key, provider, host.get('model'))
    provider, coordinator = agent['provider'], agent['model']

    def agent_line(value):
        if value['provider'] == 'opencode':
            from . import opencode_accounts, opencode_subscription
            alias = value.get('account') or opencode_accounts.active(store)
            account = opencode_accounts.accounts(store).get(alias) or {}
            subscription = opencode_subscription.is_subscription(account)
            model = _safe(value['model'], _OPENCODE_MODEL_DISPLAY)
            line = 'OpenCode subscription %s' % model if subscription else 'OpenCode %s %s' % (alias or 'none', model)
            usd = (store.get(key + '_opencode') or {}).get('usd')
            if not subscription and _number(usd):
                line += ' · API spend (estimate) $%.2f this session' % usd
            return line + ' · picked for this chat'
        effort = codex_reasoning_effort(value['effort']) if value['provider'] == 'codex' else value['effort']
        return '%s %s · %s thinking · %s' % (
            {'codex': 'ChatGPT', 'antigravity': 'Antigravity'}.get(value['provider'], 'Claude'),
            _safe(value['model'], _MODEL), effort,
            'picked for this chat' if value['picked'] else 'default')

    chat_agent = agent_line(agent)
    if pending:
        chat_agent += ' · waiting to apply ' + agent_line(selected)
    task = store.db.execute("SELECT id,number,title,status FROM tasks WHERE topic=? AND status='open'"
                            ' ORDER BY id LIMIT 1', (topic_id,)).fetchone()
    active = (store.db.execute("SELECT id,effort FROM workers WHERE task=? AND status IN "
                               "('queued','running','waiting_for_secret','waiting_for_quota') ORDER BY id",
                               (task['id'],)).fetchall() if task else [])
    lines = [
        (topic.get('name') or 'No project set up yet') +
        ('' if not topic else ' · ' + ('accepting work' if topic.get('enabled') else 'disabled')),
        topic.get('cwd') or 'No folder bound. Use /setup.',
        'Chat agent: ' + chat_agent,
        'Models: Claude workers ' + ('native default' if worker is None else _safe(worker, _MODEL)) +
        ' · chat agent ' + _safe(coordinator, _OPENCODE_MODEL_DISPLAY if provider == 'opencode' else _MODEL),
        'Account: ' + account,
        (f"Job {task['number']}: {task['status']} · {task['title']}" +
         ((' · workers ' + ', '.join('%d %s' % (row['id'], row['effort']) for row in active)) if active else ''))
        if task else 'No open job in this channel.',
    ]
    tags = chat_tags(store, topic_id)
    if tags:
        lines.insert(3, 'Tags: ' + ', '.join(tags))
    proposed = store.db.execute("SELECT COUNT(*) FROM tasks WHERE topic=? AND status='proposed'", (topic_id,)).fetchone()[0]
    lines.insert(-1, 'Proposed follow-ups: %d' % proposed)
    queued = store.db.execute('SELECT COUNT(*) FROM keep_queue q JOIN tasks t ON t.id=q.task WHERE t.topic=?', (topic_id,)).fetchone()[0]
    lines.insert(-1, 'Keep queue: %d' % queued)
    idle_hours = store.get('idle_trim', 0)
    lines.insert(-1, 'Idle trim: ' + ('off' if not idle_hours else 'after %s hours' % idle_hours))
    if store.get('setup_problem'):
        lines.append('Setup problem: ' + store.get('setup_problem'))
    if store.get('group_owner_changed'):
        lines.append(store.get('group_owner_changed'))
    sync = store.get('records_sync_status', {})
    if sync:
        lines.append('Records git: commit %s · last %s%s%s%s' % (
            (sync.get('sha') or 'none')[:12], sync.get('last_commit') or 'never',
            ' · push to %s install/%s, ahead %s' % (_records_provider(store.get('records_sync_remote')),
                                                   store.get('records_sync_install'), sync.get('ahead', '?'))
            if store.get('records_sync', False) is True else '',
            ' · blocked %d' % len(sync['blocked']) if sync.get('blocked') else '',
            ' · ' + sync['error'] if sync.get('error') else ''))
    selection = store.get('parent_selection')
    if selection and not store.get(key + '_selection'):
        lines.append('Parent selection: %s %s after active work settles.' %
                     (selection['provider'], selection['model']))
    return '\n'.join(lines)


def _accounts(store):
    from .codex_accounts import CodexBroker
    from . import opencode_subscription
    ordered, active = listed_accounts(store, 'opus')
    aliases = [alias for alias in ordered if _safe(alias, _ALIAS, '')]
    codex, codex_active = CodexBroker(store).listed()
    codex = [alias for alias in codex if _safe(alias, _ALIAS, '')]
    from .extension import optional
    antigravity = optional('antigravity_accounts')
    sections = [codex_heading(store) + '\n\n' + '\n\n'.join(_codex_card(store, alias, alias == codex_active)
                                                   for alias in codex)] if codex else []
    if antigravity:
        sections.extend(antigravity.cards(store))
    opencode = opencode_subscription.card(store)
    if opencode:
        sections.append(opencode)
    if not aliases:
        return '\n\n'.join(sections) if sections else 'No accounts are registered. Open /accounts to add one.'
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
    active = listed_accounts(store, 'opus')[1]
    heading = 'Next account: ' + (account_label(store, active) if active else 'none available')
    if not active:
        return heading + '\n\n' + _accounts(store)
    return heading + '\n\n' + _account_card(store, active, True)


def op_settings_show(store, ctx, topic=None):
    return _status_card(store, topic)


def op_cleanup_sessions(store, ctx, enabled):
    _change(store, 'session_delete', enabled)
    from .session_storage import request
    request(store)
    return 'Cleanup of finished native sessions older than 14 days is ' + ('on.' if enabled else 'off.')


def op_service_updates(store, ctx, enabled):
    _change(store, 'telegram_updates_enabled', enabled)
    if not enabled:
        from .updates import KEY, OFF, recover, watch
        recover(store)
        record = store.get(KEY) or {}
        if record.get('phase') in ('checking', 'offer', 'queued', 'checks', 'arming') and not record.get('old_pid'):
            with store.db:
                outbox = store.enqueue_report(record['topic'], OFF, kind='status', edit=record.get('message'),
                                              reply_markup={'inline_keyboard': []})
                watch.write(store.db, record['id'], expected=record['phase'], phase='cancelled',
                            result=OFF, outbox=outbox, reported=True)
    return 'Updates from Telegram are ' + ('on. Use /update.' if enabled else 'off.')


def op_cleanup_cow_worktrees(store, ctx, enabled):
    _change(store, 'cow_worktrees', enabled)
    from .workspaces import volume_mounted
    mounted = '' if volume_mounted(store.directory) else ' The volume is not mounted now, so new trees use git worktree add.'
    return 'Copy-on-write worktrees are ' + ('on.' if enabled else 'off.') + (mounted if enabled else '')


def op_cleanup_idle_trim(store, ctx, enabled, hours=None):
    if hours is not None and hours < 1:
        raise Refused('Hours must be one or more.')
    value = (hours or 24) if enabled else 0
    _change(store, 'idle_trim', value)
    return 'Idle trimming is ' + ('off.' if not value else 'on after %d hours.' % value)


def _records_provider(remote):
    from .records import provider
    try:
        return provider(remote or '').name
    except ValueError:
        return 'no provider'


def op_records_sync(store, ctx, enabled, remote=None, install=None):
    from .records import INSTALL, PROVIDERS, provider
    named = store.get('records_sync_install')
    if install and named and install != named and store.get('records_sync_status', {}).get('last_push'):
        raise Refused('The install name is fixed after the first push. It stays %s.' % named)
    remote = remote or store.get('records_sync_remote')
    install = install or named
    if enabled:
        if (not remote or remote.startswith('-') or any(c.isspace() for c in remote)
                or '?' in remote or '#' in remote or re.search(r'://[^/]*@', remote)):
            raise Refused('Set a remote without credentials, whitespace, query or fragment.')
        try:
            provider(remote)
        except ValueError:
            raise Refused('The records remote needs a provider for its host: %s.' % ', '.join(
                kind.name for kind in PROVIDERS)) from None
        if not install or not INSTALL.fullmatch(install):
            raise Refused('Set install to a name of 2 to 31 lowercase letters, digits or hyphens, starting with a letter.')
    store.put('records_sync', enabled)
    if remote:
        store.put('records_sync_remote', remote)
    if install:
        store.put('records_sync_install', install)
    return 'Records push is ' + ('on to install/%s.' % install if enabled else 'off.')

def rebuild_rules(text):
    """Parse and check the JSON list for the close_saves_rebuildable setting; empty text clears it."""
    try:
        rules = json.loads(text) if text.strip() else []
    except ValueError:
        raise Refused('Send rebuildable as a JSON list.') from None
    keys = {'project', 'paths', 'manifest', 'ref', 'command'}
    if not isinstance(rules, list) or not all(
            isinstance(rule, dict) and keys <= set(rule) and set(rule) <= keys | {'prefix'}
            and Path(str(rule['project'])).is_absolute() and isinstance(rule['paths'], list) and rule['paths']
            and all(isinstance(value, str) and value for value in [*rule['paths'], rule['manifest'], rule['ref'],
                                                                    rule['command'], rule.get('prefix', 'x')])
            for rule in rules):
        raise Refused('Each rebuildable rule needs project (absolute), paths (list), manifest, ref and command; prefix is optional.')
    return rules


def op_cleanup_close_rules(store, ctx, discard_names=None, keep=None, rebuildable=None, requeue=None):
    from . import close_saves
    changes = {}
    if discard_names is not None:
        names = list(dict.fromkeys(name.strip() for name in discard_names.split(',') if name.strip()))
        if any(name in ('.', '..') or '/' in name or '\\' in name for name in names):
            raise Refused('Send discard_names as comma-separated folder or file names without paths.')
        changes['discard_names'] = names
    if keep is not None:
        try:
            ids = list(dict.fromkeys(int(value.strip()) for value in keep.split(',') if value.strip()))
        except ValueError:
            raise Refused('Send keep as comma-separated positive task IDs.') from None
        if any(task_id <= 0 for task_id in ids):
            raise Refused('Send keep as comma-separated positive task IDs.')
        changes['keep'] = ids
    if rebuildable is not None:
        changes['rebuildable'] = rebuild_rules(rebuildable)
    prefixes = []
    with store.transaction():
        for name, value in changes.items():
            _change(store, 'close_saves_' + name, value)
            if value:
                prefixes.extend(close_saves.RULES.get(name, ()))
        requeued = 0
        if requeue is not False and prefixes:
            candidates = store.db.execute(f"""SELECT id FROM tasks WHERE copy='held'
                AND status IN ({','.join('?' for _ in close_saves.CLOSED)}) AND (""" +
                ' OR '.join('copy_reason LIKE ?' for _ in prefixes) + ')',
                close_saves.CLOSED + tuple(prefix + '%' for prefix in prefixes)).fetchall()
            for row in candidates:
                if not close_saves.kept(store, row['id']):
                    store.db.execute("UPDATE tasks SET copy='saving',copy_reason=NULL WHERE id=?", (row['id'],))
                    requeued += 1
    data = {name: store.get(name, []) for name in ('close_saves_discard_names', 'close_saves_keep', 'close_saves_rebuildable')}
    data['requeued'] = requeued
    return Result(True, 'Close-save rules updated. Requeued %d held jobs.' % requeued, data)


def op_cleanup_retry(store, ctx, task):
    from . import close_saves
    if close_saves.kept(store, task):
        raise Refused('Task %d is in close_saves_keep. Remove it from keep before retrying.' % task)
    current = store.task_get(task)
    if not current or current['status'] not in close_saves.CLOSED:
        raise Refused('Choose an existing closed job for cleanup.retry.')
    close_saves.request(store, task)
    return 'Close save queued for task %d.' % task


def op_settings_get(store, ctx):
    from .onboarding import configured_root
    from .opencode_accounts import owner_settings
    from .extension import optional
    antigravity = optional('antigravity_accounts')
    root = configured_root(store)
    data = dict(cow_worktrees=store.get('cow_worktrees', False) is True,
                records_sync=store.get('records_sync', False) is True,
                records_sync_remote=store.get('records_sync_remote'),
                records_sync_install=store.get('records_sync_install'),
                idle_trim=store.get('idle_trim', 0),
                close_saves_discard_names=store.get('close_saves_discard_names', []),
                close_saves_keep=store.get('close_saves_keep', []),
                session_delete=store.get('session_delete', False) is True,
                normal_model=store.get('worker_model', 'opus'),
                codex_enabled=store.get('codex_enabled', True),
                codex_model=store.get('codex_model'),
                codex_auto_switch=store.get('codex_auto_switch', False) is True,
                telegram_updates_enabled=store.get('telegram_updates_enabled', False) is True,
                codex_active_account=_codex_active_label(store),
                coordinator_home_topic=store.get('coordinator_home_topic'),
                coordinator_model=store.get('coordinator_model'),
                parent_selection=store.get('parent_selection'),
                projects_root=str(root) if root else None,
                account_switch_threshold=switch_threshold(store),
                **owner_settings(store), **(antigravity.owner_settings(store) if antigravity else {}))
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
    from . import opencode_subscription
    if alias in opencode_subscription.accounts(store):
        return opencode_subscription.card(store, alias)
    if _codex(store, alias):
        from .codex_accounts import CodexBroker
        return _codex_card(store, alias, alias == CodexBroker(store).current())
    return _account_card(store, alias, alias == listed_accounts(store, 'opus')[1])


def op_account_current(store, ctx):
    return _current_account(store)


def op_policy_show(store, ctx, topic=None):
    from .policy import join_layers, layers, torii_instructions
    row = store.topic(topic) if topic else None
    key = session_key(store, row['cwd'], topic) if row else 'coordinator'
    fixed = torii_instructions(store, key, 'coordinator', create_scratch=False)
    rules = layers(store, key, 'coordinator')
    sizes = '\n'.join('%s: %d bytes' % (name, len(text.encode())) for name, text in [('Torii.md', fixed)] + rules)
    return sizes + '\n\n' + fixed + '\n\n' + join_layers(rules)


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
    row = store.topic(topic)
    if not row:
        raise Refused('This channel is not linked yet. Use /setup first.')
    if row['chat'] != store.chat():
        raise Refused('Choose a linked channel in the current Telegram group.')
    if row['thread'] <= 1:
        raise Refused('The General topic cannot be renamed with topic.rename.')
    from .topics import lifecycle
    if lifecycle(store, topic):
        raise Refused('This chat is closing or closed.')
    if not 1 <= len(name.encode('utf-16-le')) // 2 <= 128:
        raise Refused('Send a Telegram topic name of 1 to 128 characters.')
    pending = store.db.execute("""SELECT 1 FROM service_requests WHERE op='topic.rename'
        AND state IN ('queued','sending','uncertain') AND json_extract(params,'$.topic')=?""", (topic,)).fetchone()
    if not pending and row['name'] == name and row['telegram_title'] == name:
        return 'The Telegram topic and channel label already use this name.'
    request = store.service_request('topic.rename', {'topic': topic, 'name': name,
                                                   '_topic': ctx.topic or topic, '_chat': row['chat']})
    return Result(True, 'Telegram topic rename queued. Torii will send the result here.',
                  {'request': request}, state='queued')


def op_topic_enable(store, ctx, topic=None):
    if not store.topic(topic):
        raise Refused('This channel is not linked yet. Use /setup first.')
    from .topics import lifecycle
    if lifecycle(store, topic):
        raise Refused('This chat is closing or its Telegram topic was deleted or closed. It cannot be enabled.')
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


def op_delegation_opencode(store, ctx, enabled):
    from .opencode_accounts import check_ready
    if enabled:
        check_ready(store)
    _change(store, 'opencode_enabled', enabled)
    return 'OpenCode delegation is ' + ('on.' if enabled else 'off.')


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


def op_model_parent(store, ctx, provider=None, model=None, topic=None, effort=None, message=None):
    if provider == 'opencode' and topic is None and opencode_pick_problem(store):
        raise Refused(opencode_pick_problem(store))
    if provider == 'antigravity' and topic is None:
        from .extension import optional
        problem = optional('antigravity_parent').pick_problem(store)
        if problem:
            raise Refused(problem)
        model = model or optional('antigravity_accounts').DEFAULT_MODEL
    if topic is not None:
        from . import topics
        row = store.topic(topic)
        if not row or not row['chat'] or not row['cwd']:
            raise Refused('Choose a linked channel first.')
        if topics.lifecycle(store, topic):
            raise Refused('This chat is closing or closed.')
        key = session_key(store, row['cwd'], topic)
        if provider == 'opencode':
            from . import opencode_accounts, opencode_subscription
            problem = opencode_pick_problem(store)
            if problem:
                raise Refused(problem)
            alias = opencode_accounts.active(store)
            account = opencode_accounts.accounts(store)[alias]
            label = ('OpenCode subscription (%s)' % account['model'] if opencode_subscription.is_subscription(account) else
                     'OpenCode on account %s (%s)' % (alias, account['model']))
            opencode_text = 'Chat %s agent: %s.' % (topic, label)
            model, effort = None, None
        if provider == 'antigravity':
            from .extension import optional
            pick_problem = optional('antigravity_parent').pick_problem
            problem = pick_problem(store)
            if problem:
                raise Refused(problem)
        previous = store.get(key + '_selection') or {}
        if provider and (provider, model, effort) == tuple(previous.get(field) for field in (
                'provider', 'model', 'effort')):
            if store.get(key + '_start_failed') is not None:
                store.put(key + '_start_failed', None)
            return Result(True, ('Chat %s agent is already set to %s.' % (topic, label)
                                 if provider == 'opencode' else
                                 'Chat %s agent is already set to this provider, model and thinking.' % topic),
                          {'topic': topic, 'selection': previous})
        revision = max(previous.get('revision', 0), store.get(key + '_selection_revision', 0)) + 1
        selection = {'provider': provider, 'model': model, 'effort': effort, 'revision': revision} if provider else None
        _change(store, key + '_selection', selection)
        if previous and not selection:
            store.put(key + '_selection_cleared', True)
        elif selection and store.get(key + '_selection_cleared'):
            store.put(key + '_selection_cleared', None)
        for suffix in ('_selection_retry_at', '_selection_notice', '_start_failed'):
            if store.get(key + suffix) is not None:
                store.put(key + suffix, None)
        return Result(True, (opencode_text if provider == 'opencode' else 'Chat %s agent: %s %s · %s thinking.' % (
            topic, provider, model or 'provider default', effort or 'default') if selection else
            'Chat %s agent: default provider, model and thinking.' % topic) +
            ' Active turns and native background tasks settle before a change. Worker settings stay unchanged.',
            {'topic': topic, 'selection': selection})
    if effort is not None:
        raise Refused('Supply topic for chat thinking, or use effort.parent for global parent thinking.')
    if provider != 'opencode' and bool(provider) != bool(model):
        raise Refused('Supply both provider and model, or omit both for automatic parent selection.')
    selection = {'provider': provider, 'model': model} if provider else None
    if provider != (store.get('parent_selection') or {}).get('provider'):
        store.put('parent_selection_revision', store.get('parent_selection_revision', 0) + 1)
    _change(store, 'parent_selection', selection)
    if store.get('coordinator_start_failed') is not None:
        store.put('coordinator_start_failed', None)
    return Result(True, ('Parent chats: %s %s.' % (provider, model or 'account model') if selection else
                         'Parent chats: automatic provider and model selection.') +
                  ' Active turns and native background tasks settle before a change. Worker settings stay unchanged.',
                  {'parent_selection': selection})


def op_effort_parent(store, ctx, provider, effort=None):
    _change(store, provider + '_parent_effort', effort)
    return ('%s parent thinking: %s. Applies at the next parent launch; '
            'a changed launch fingerprint resumes the same native session after active work settles.' %
            (provider, effort or PARENT_EFFORT))


def op_policy_chat(store, ctx, topic, message=None, append=None):
    from . import topics
    from .policy import chat_path, atomic_rule_write, _clean
    row = store.topic(topic)
    if not row or not row['chat'] or not row['cwd']:
        raise Refused('Choose a linked channel first.')
    if topics.lifecycle(store, topic):
        raise Refused('This chat is closing or closed.')
    if message is None:
        raise Refused('Pass message=N for the owner message that asked.')
    topics._asking(store, ctx, message)
    if append is None:
        raise Refused('Supply append to add a chat rule.')
    append = append.strip()
    if not append:
        raise Refused('Supply a chat rule.')
    path = chat_path(store, topic)
    current = path.read_text() if path.exists() else ''
    updated = current.rstrip() + ('\n' if current.strip() else '') + append + '\n'
    if len(_clean(updated)) > 4000:
        raise Refused('Keep chat rules to 4000 characters. HTML comments do not count.')
    atomic_rule_write(path, updated)
    return 'Chat rules saved. The agent loads them with its next message.'


def op_policy_set(store, ctx, text):
    from .policy import atomic_rule_write, personal_path, rule_lines, _clean
    updated = text.strip() + '\n'
    if not rule_lines(updated) or len(_clean(updated)) > 4000:
        raise Refused('Keep personal rules to 1–4000 characters. HTML comments do not count.')
    with store.transaction():
        atomic_rule_write(personal_path(store, create=False), updated)
    return 'Personal rules saved. Coordinators load them with their next message.'


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


_PICK_SCOPES = {None: 'Claude', 'fable': 'Claude Fable', 'other': 'Claude models other than Fable'}


def _owner_asked(store, ctx, message):
    if ctx is None:
        return
    if ctx.source_worker is not None:
        raise Refused('Workers do not change accounts. Report the need; the coordinator asks the owner.')
    if ctx.source == 'mcp':
        from dataclasses import replace
        from .topics import _asking
        _asking(store, replace(ctx, topic=ctx.topic or ctx.caller), message)


def op_account_use(store, ctx, alias, topic=None, models=None, message=None):
    from .codex_accounts import CodexBroker
    _owner_asked(store, ctx, message)
    broker = CodexBroker(store)
    if models and _codex(store, alias):
        raise Refused('models applies only to Claude accounts.')
    if not _codex(store, alias):
        accounts = _registered(store, alias)
        display = account_label(store, alias)
        if not authenticated(store, alias, accounts[alias]):
            raise Refused(f'{display} is not signed in. Sign it in first.')
        if accounts[alias].get('enabled') is not True:
            raise Refused(f'{display} is disabled. Enable it first.')
        request_id = store.get('claude_pick_refresh_id', 0) + 1
        groups = [models] if models else list(PICK_GROUPS)
        _change(store, 'claude_account_pick', picks_value({**account_picks(store), **dict.fromkeys(groups, alias)}))
        store.put('claude_pick_refresh_id', request_id)
        store.put('claude_pick_refresh', {'alias': alias, 'id': request_id, 'topic': ctx.topic, 'models': groups})
        store.put('account_status_refresh_requested', time.time())
        return f'{display} is picked for {_PICK_SCOPES[models]}; its usage is being checked.'
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


def op_accounts_claude_auto(store, ctx, models=None, message=None):
    _owner_asked(store, ctx, message)
    picks = picks_value({**account_picks(store), **dict.fromkeys([models] if models else PICK_GROUPS)})
    _change(store, 'claude_account_pick', picks)
    if picks is None:
        store.put('claude_pick_refresh', None)
    return (f'{"Claude" if not models else _PICK_SCOPES[models]} account choice is automatic. '
            'Running chats stay on their account while it can take work.')


def op_accounts_codex_auto(store, ctx, enabled):
    from .codex_accounts import CodexBroker
    _change(store, 'codex_auto_switch', enabled is True)
    if enabled is True:
        return ('Automatic Codex account switching is on. At 99% or a usage-limit rejection, Torii moves Codex work '
                'to the signed-in account whose weekly limit resets soonest. When included allowance is used, '
                'Torii uses prepaid credits, largest balance first, down to zero.')
    broker = CodexBroker(store)
    active = broker.active()
    return ('Automatic Codex account switching is off. Codex work stays on the active account %s. '
            'Torii reports a rejected turn only to the home chat when work cannot continue.' %
            (broker.label(active) if active else 'none'))


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
    _owner_asked(store, ctx, None)
    return _redeem_codex_account(store, ctx, alias, True)


def op_account_redeem(store, ctx, message=None):
    from .codex_accounts import CodexBroker
    _owner_asked(store, ctx, message)
    alias = CodexBroker(store).active()
    if not alias:
        raise Refused('No active Codex account is registered.')
    return _redeem_codex_account(store, ctx, alias, True)


def op_accounts_codex_auto_redeem(store, ctx, accounts=None):
    if ctx.source_worker is not None:
        raise Refused('Codex automatic reset redemption is an owner setting on the local CLI.')
    if accounts is not None:
        try:
            names = json.loads(accounts)
            if not isinstance(names, list) or any(not isinstance(name, str) or not name.strip() for name in names):
                raise ValueError()
        except ValueError:
            raise Refused('Send a JSON list of Codex account emails or aliases, or [] to turn it off.') from None
        _change(store, 'codex_auto_redeem', list(dict.fromkeys(name.strip() for name in names)) or None)
    names = store.get('codex_auto_redeem') or []
    return Result(True, ('Torii redeems one banked reset when one of these Codex accounts reaches its limit: %s.' %
                         ', '.join(names)) if names else 'Torii redeems no banked Codex reset on its own.',
                  {'codex_auto_redeem': names})


def _grammar(store, topic_id, command, args, text):
    """Return (op, params, render), a finished reply, or None for the usage text.

    Guards stay exactly where they were: a command that does not match its guard
    still gets the usage reply, never a refusal from an operation.
    """
    remainder = text.split(maxsplit=1)[1] if args else ''
    if command == '/status' and not args:
        return 'settings.show', {}, 'menu'
    if command == '/keep':
        if not args:
            return 'keep.destination', {}, 'menu'
        if len(args) >= 2 and args[0] == 'to':
            return 'keep.destination', {'to': text.split(maxsplit=2)[2]}, 'menu'
        return None
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
        return _help(store)
    if command == '/health':
        return 'health.show', {}, 'menu'
    if command == '/accounts':
        return 'accounts.list', {}, 'menu'
    if command == '/login':
        from .signin import login_command
        return login_command(store, args)
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
    if command == '/waiting' and not args:
        from .waits import pending_data, pending_text
        scope = None if store.get('coordinator_home_topic') == topic_id else topic_id
        reply = store.wait_hook('list-failed', lambda: pending_text(store, pending_data(store, scope, owner=True)))
        reply = reply or 'The waiting list is unavailable. Try again.'
        store.enqueue_report(topic_id, reply, reply_to=message_id)
        return True
    if command == '/update' and not args:
        from .updates import request
        request(store, topic_id, message_id)
        return True
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
