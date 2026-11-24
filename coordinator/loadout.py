"""Model defaults and owner-authorized temporary changes."""

from copy import deepcopy
from datetime import datetime, timezone
from functools import lru_cache
import json
from pathlib import Path
import re
import time
import unicodedata

from .control_api import Refused, Result
from .extension import optional


SETTINGS = {
    'coordinators.claude.model': 'coordinator_model',
    'coordinators.claude.effort': 'claude_parent_effort',
    'coordinators.codex.model': 'codex_model',
    'coordinators.codex.effort': 'codex_parent_effort',
    'workers.claude.model': 'worker_model',
    'workers.codex.model': 'codex_model',
    'workers.claude.effort': 'claude_worker_effort',
    'workers.codex.effort': 'codex_worker_effort',
}


def topic_for_key(store, key):
    return (store.get('coordinator_session_topic') or store.get('coordinator_home_topic')) if key == 'coordinator' else key.split(':', 1)[1]


def active(store, topic=None, now=None):
    now = time.time() if now is None else now
    rows = store.get('loadout.swaps', []) or []
    return [row for row in rows if not row.get('ended') and row['until'] > now and
            (row['topic'] is None or topic is not None and row['topic'] == topic)]


@lru_cache(maxsize=1)
def _template(path, mtime):
    """Cache template data by path and modification time."""
    return json.loads(path.read_text())


def effective(store, topic=None, explicit=None, base=None):
    path = Path(__file__).parent / 'defaults' / 'loadout.json'
    values = deepcopy(_template(path, path.stat().st_mtime_ns))
    for path, setting in SETTINGS.items():
        value = store.get(setting)
        if value is not None:
            group, provider, field = path.split('.')
            values[group][provider][field] = value
    key = 'coordinator' if not topic or topic in (store.get('coordinator_session_topic'), store.get('coordinator_home_topic')) else 'coordinator:' + topic
    selection = store.get(key + '_selection') or store.get('parent_selection') or {}
    provider = selection.get('provider')
    if provider in ('claude', 'codex'):
        for field in ('model', 'effort'):
            if selection.get(field) is not None:
                values['coordinators'][provider][field] = selection[field]
    rows = active(store, topic)
    swaps = {row['key']: row['value'] for row in sorted(rows, key=lambda item: (item['topic'] is not None, item['id']))}
    for layer in (base or {}, swaps, explicit or {}):
        for path, value in layer.items():
            if value is not None and path in SETTINGS:
                group, provider, field = path.split('.')
                values[group][provider][field] = value
    return values, rows


def render(store, topic):
    lines = []
    for row in active(store, topic):
        date = datetime.fromtimestamp(row['until'], timezone.utc).strftime('%Y-%m-%d')
        value = json.dumps(row['value'])
        text = ('route %s work to %s' % (row['key'].split('.', 1)[1], value)
                if row['key'].startswith('routes.') else 'set %s to %s' % (row['key'], value))
        lines.append('Until %s (owner %s): %s' % (date, row['message'], text))
    return '\n'.join(lines)


def op_effort_worker(store, ctx, provider, effort=None):
    store.put(provider + '_worker_effort', effort)
    return 'Worker effort for %s: %s.' % (provider, effort or 'template default')


def _scope(store, ctx, topic):
    if ctx.source != 'mcp':
        return
    if ctx.topic is None:
        raise Refused('This tool needs the calling chat.')
    if topic != ctx.topic and (topic is not None or ctx.topic != store.get('coordinator_home_topic')):
        raise Refused('Use only your own chat. All-chat swaps require the home chat.')


def _retained(rows, now):
    return [row for row in rows if (row.get('ended') or row['until']) > now - 7 * 86400]


def op_show(store, ctx, topic=None):
    topic = topic or ctx.topic
    _scope(store, ctx, topic)
    values, rows = effective(store, topic)
    return Result(True, json.dumps({'values': values, 'swaps': rows}, sort_keys=True),
                  {'values': values, 'swaps': rows})


def op_swap(store, ctx, key, value, until, reason, message, topic=None):
    from .control_api import _coerce
    from .topics import _asking
    _asking(store, ctx, message)
    _scope(store, ctx, topic)
    routing = optional('routing')
    route_kinds = routing.KINDS if routing is not None else {}
    if key not in SETTINGS and key.removeprefix('routes.') not in route_kinds:
        raise Refused('Unknown loadout key.')
    if len(value) > 80 or not value.strip() or any(unicodedata.category(char) in ('Cc', 'Cf', 'Zl', 'Zp') for char in value):
        raise Refused('Value must be 1 to 80 characters on one line.')
    if key not in SETTINGS:
        routing.parse_order(value)
    else:
        kind = key.rsplit('.', 1)[1]
        _, problem = _coerce(store, kind, value)
        if problem:
            raise Refused('Invalid value for ' + key + '.')
        if kind == 'model':
            claude = value.startswith('claude-') or value in ('opus', 'sonnet', 'haiku', 'fable')
            if claude != ('.claude.' in key):
                raise Refused('Model must match the provider.')
    if len(reason) > 160 or '\n' in reason or '\r' in reason:
        raise Refused('Reason must be at most 160 characters on one line.')
    now = time.time()
    match = re.fullmatch(r'([1-9][0-9]*)([dhm])', until)
    try:
        end = now + int(match[1]) * {'d': 86400, 'h': 3600, 'm': 60}[match[2]] if match else datetime.fromisoformat(until.replace('Z', '+00:00')).timestamp()
        if not match and datetime.fromisoformat(until.replace('Z', '+00:00')).tzinfo is None:
            raise ValueError()
    except (ValueError, OverflowError, OSError):
        raise Refused('until must be a duration or ISO 8601 time with timezone.')
    if end <= now or end > now + 30 * 86400:
        raise Refused('until must be in the next 30 days.')
    with store.transaction():
        rows = _retained(store.get('loadout.swaps', []) or [], now)
        ident = (store.get('loadout.next_id', 0) or 0) + 1
        row = {'id': ident, 'key': key, 'value': value, 'topic': topic, 'until': end,
               'reason': reason, 'message': message, 'created': now}
        rows.append(row)
        store.put('loadout.swaps', rows)
        store.put('loadout.next_id', ident)
    return Result(True, 'Swap %d created.' % ident, row)


def op_unswap(store, ctx, id):
    now = time.time()
    with store.transaction():
        rows = _retained(store.get('loadout.swaps', []) or [], now)
        for row in rows:
            if row['id'] == id and not row.get('ended') and row['until'] > now:
                _scope(store, ctx, row['topic'])
                row['ended'] = now
                store.put('loadout.swaps', rows)
                return Result(True, 'Swap %d ended.' % id, row)
    raise Refused('No active swap with that id.')
