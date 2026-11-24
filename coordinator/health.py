"""Read-only owner health summary from saved workers and bounded host probes."""

import math
import re
import time

from . import host_os
from .controls import _mapping, _timestamp, _until, _window_left
from . import extension


def _size(value):
    return f'{value / 1024 ** 3:.1f}G'


def _plain(value):
    return re.sub(r'([\\`*_\[\]~|])', r'\\\1', ' '.join(str(value).split()))


def _disk(path):
    lines = host_os.disk_usage_output(path).splitlines()
    if len(lines) < 2:
        return None
    fields = lines[-1].split()
    try:
        total, free = int(fields[1]) * 1024, int(fields[3]) * 1024
    except (IndexError, ValueError):
        return None
    return f'{_size(free)} free / {_size(total)}'


def system_pressure(state_dir):
    values = host_os.read_pressure()
    lines = []
    if values['load1'] is not None and values['cores'] is not None:
        lines.append(f'CPU: {values["load1"]:.1f} load / {values["cores"]} cores')
    if values['memory_used'] is not None and values['memory_total'] is not None:
        detail = f'RAM: {_size(values["memory_used"])} used / {_size(values["memory_total"])}'
        if values['pressure'] is not None:
            detail += host_os.pressure_detail(values["pressure"])
        if values['swap_used'] is not None:
            detail += ', swap ' + _size(values['swap_used'])
        lines.append(detail)
    for label, path in (('Disk /: ', '/'), ('Disk state: ', state_dir)):
        disk = _disk(path)
        if disk:
            lines.append(label + disk)
    return lines


def _account_order(mark, alias, usable, reset):
    if mark in ('🟢', '🟡'):
        return (0, usable.get(alias, math.inf))
    return (1, reset if reset is not None else math.inf) if mark == '🔴' else (2, math.inf)


def account_mark(authenticated, enabled, state):
    """The mark and state words of one account, shared by /health and /accounts."""
    if not authenticated:
        return '⚪', 'signed out'
    if not enabled:
        return '⚫', 'off'
    if state['state'] == 'limited':
        return '🔴', _back(state.get('until'))
    if state['state'] == 'stale':
        return '🟡', 'stale since ' + _timestamp(state.get('since'))
    return '🟢', ''


def _back(until):
    return 'back in ' + _until(until) if type(until) in (int, float) and until > time.time() else 'back later'


def _future(value):
    return type(value) in (int, float) and value > time.time()


def _banked(snapshot):
    banked = snapshot.get('resets_available')
    if type(banked) is not int or banked <= 0:
        return ''
    expires = snapshot.get('reset_expires')
    return (' · %d reset%s banked' % (banked, '' if banked == 1 else 's')
            + (', first expires in ' + _until(expires).replace('d 0h', 'd') if _future(expires) else ''))


def claude_line(store, router, alias):
    """One Claude account line for /health and /accounts: (mark, reset used for ordering, text)."""
    from .accounts import account_label, authenticated
    profile = _mapping(_mapping(store.get('accounts', {})).get(alias))
    label = _plain(account_label(store, alias))
    state = router.account_state(alias, 'opus')
    mark, words = account_mark(authenticated(store, alias, profile), profile.get('enabled') is True, state)
    if mark in ('⚪', '⚫'):
        return mark, None, f'{mark} {label}: ' + words
    usage = _mapping(_mapping(_mapping(store.get('account_status', {})).get(alias)).get('usage'))
    windows = []
    for key in ('seven_day', 'seven_day_fable'):
        remaining, _ = _window_left(_mapping(usage.get(key)))
        windows.append(f'{remaining:g}%' if remaining is not None else '?')
    parts = [words]
    shown = state.get('until') if mark == '🔴' and _future(state.get('until')) else None
    if mark != '🔴':
        weekly = _mapping(usage.get('seven_day')).get('resets_at')
        if _future(weekly):
            shown = weekly
            parts.append('week resets in ' + _until(weekly))
        fable = router.account_state(alias, 'fable')
        if fable['state'] == 'limited':
            parts.append('Fable paused, ' + _back(fable.get('until')))
    state_text = ', '.join(filter(None, parts))
    return mark, shown, f'{mark} {label}: ' + ' · '.join(windows) + (', ' + state_text if state_text else '')


def codex_line(broker, alias, active, count):
    """One ChatGPT account line for /health and /accounts: (mark, reset used for ordering, text)."""
    from .codex_accounts import credit_line
    label = _plain(broker.label(alias))
    state = broker.account_state(alias)
    mark, words = account_mark(broker.authenticated(alias), broker.signed_in(alias), state)
    if mark in ('⚪', '⚫'):
        return mark, None, f'{mark} {label}: ' + words
    snapshot = broker.snapshot(alias)
    week = _mapping(_mapping(snapshot.get('usage')).get('seven_day'))
    remaining, _ = _window_left(week)
    parts = [words]
    shown = state.get('until') if mark == '🔴' and _future(state.get('until')) else None
    if mark != '🔴' and _future(week.get('resets_at')):
        shown = week['resets_at']
        parts.append('week resets in ' + _until(week['resets_at']))
    state_text = ', '.join(filter(None, parts))
    return mark, shown, (f'{mark} {label}: ' + (f'{remaining:g}%' if remaining is not None else '?')
                         + (', ' + state_text if state_text else '')
                         + (' · active' if alias == active and count > 1 else '')
                         + _banked(snapshot) + ' · ' + credit_line(snapshot))


def _subscription(store):
    from .accounts import AccountBroker, account_label
    router = AccountBroker(store)
    usable = {alias: index for index, alias in enumerate(router.selection_order(model='opus'))}
    aliases = sorted((alias for alias, profile in _mapping(store.get('accounts', {})).items()
                      if alias and isinstance(profile, dict)),
                     key=lambda alias: (account_label(store, alias).casefold(), alias))
    entries = []
    for alias in aliases:
        mark, shown, line = claude_line(store, router, alias)
        entries.append((_account_order(mark, alias, usable, shown), line))
    entries = [line for _, line in sorted(entries, key=lambda entry: entry[0])]
    return '\n'.join(['**Subscriptions left** (week · Fable)'] + (entries or ['No accounts']))


def _codex(store):
    from .codex_accounts import CodexBroker
    broker = CodexBroker(store)
    listed, active = broker.listed()
    order = broker.selection_order()
    if active in order:
        order.remove(active)
        order.insert(0, active)
    usable = {alias: index for index, alias in enumerate(order)}
    aliases = listed + sorted((alias for alias in broker.accounts() if alias not in listed),
                              key=lambda alias: (broker.label(alias).casefold(), alias))
    entries = []
    for alias in aliases:
        mark, shown, line = codex_line(broker, alias, active, len(aliases))
        entries.append((_account_order(mark, alias, usable, shown), line))
    entries = [line for _, line in sorted(entries, key=lambda entry: entry[0])]
    return '\n'.join(['**ChatGPT left** (week)'] + entries) if entries else None


def format_health(topics, workers, pressure, subscription, parents=None, extension_line=None, codex=None, activity=None,
                  antigravity=None, opencode=None):
    groups = {}
    for worker in workers:
        if worker['status'] == 'running':
            groups.setdefault(worker['topic'], []).append(worker)
    parents = parents or {}
    activity = activity or {}
    running = (sum(worker['status'] == 'running' for members in groups.values() for worker in members)
               + sum(parents.get(topic['id'], '').startswith('live') for topic in topics))
    lines = [f'**Agents running: {running}**']
    def sort_key(topic):
        parent = parents.get(topic['id'], '')
        count = len(groups.get(topic['id'], [])) + parent.startswith('live')
        latest = activity.get(topic['id'])
        return (-count, latest is None, -latest if latest is not None else 0, topic['name'])

    for topic in sorted(topics, key=sort_key):
        members = groups.pop(topic['id'], [])
        parent = parents.get(topic['id'])
        count = len(members) + bool(parent and parent.startswith('live'))
        if not topic['enabled'] and not count:
            continue
        lines.append(f'**{_plain(topic["name"])}**: {count} running'
                     + (' · parent ' + parent if parent else ''))
        for worker in members:
            number = worker['number'] if worker['number'] is not None else '?'
            lines.append(f'- Job {number}: {_plain(worker["title"] or "Untitled job")}')
    lines.extend(['', '**System pressure**'] + (pressure or ['Unavailable']))
    lines.extend(['', subscription])
    if extension_line:
        lines.append(extension_line)
    for section in (codex, antigravity, opencode):
        if section:
            lines.extend(['', section])
    return '\n'.join(lines)


def op_health_show(store, ctx):
    from .host import host_state
    from . import opencode_subscription

    workers = [dict(row) for row in store.db.execute(
        """SELECT w.topic,w.status,t.number,t.title FROM workers w
        LEFT JOIN tasks t ON t.id=w.task WHERE w.status='running' ORDER BY w.id""")]
    topics = store.topics()
    activity = {row['topic']: row['latest'] for row in store.db.execute(
        '''SELECT topic, MAX(when_at) AS latest FROM (
            SELECT topic, MAX(updated) AS when_at FROM workers GROUP BY topic
            UNION ALL
            SELECT topic, MAX(created) AS when_at FROM messages GROUP BY topic
        ) GROUP BY topic''')}
    home = store.get('coordinator_session_topic') or store.get('coordinator_home_topic')
    if home is None:
        home = next((topic['id'] for topic in topics if topic['enabled'] and store.get('coordinator_session')), None)
    parents = {}
    for topic in topics:
        key = 'coordinator' if topic['id'] == home else 'coordinator:' + topic['id']
        host = store.get(key + '_host')
        if host and host_state(store.directory / 'hosts' / host['id']) == 'running':
            if host.get('provider', 'claude') == 'codex':
                from .codex_accounts import CodexBroker
                label = CodexBroker(store).label(host.get('account'))
                parents[topic['id']] = 'live (ChatGPT %s)' % _plain(label or 'unknown')
            elif host.get('provider') == 'opencode':
                parents[topic['id']] = 'live (OpenCode %s)' % _plain(host.get('account') or 'unknown')
            elif host.get('provider') == 'antigravity':
                parents[topic['id']] = 'live (Antigravity %s)' % _plain(host.get('account') or 'unknown')
            else:
                parents[topic['id']] = 'live (Claude)'
        elif store.get('coordinator_account_retry_at'):
            parents[topic['id']] = 'paused until ' + _timestamp(store.get('coordinator_account_retry_at'))
        else:
            parents[topic['id']] = 'wound down'
    antigravity = extension.optional('antigravity_accounts')
    return format_health(topics, workers, system_pressure(store.directory), _subscription(store), parents,
                         extension.active().health(store), _codex(store), activity,
                         antigravity.health(store) if antigravity else None, opencode_subscription.card(store))
