"""Route each kind of work to an ordered list of provider/model@effort entries.

The first entry that can take the worker is its normal route. The owner changes a list
with routing.set. The Google plan runs Gemini models only, so an Antigravity entry must
name a Gemini model.
"""

import re

from .control_api import Refused, Result
from .adapters import LABELS
from .moves import STOP, capacity, chain, label, refusal, text


KINDS = {
    'judgment': 'hardest judgment, ambitious plans.',
    'design': 'UI, planning, design, review, synthesis, or dev work with several PRs per design doc, many launch paths, or security choices.',
    'general': 'all other dev and research.',
    'small': 'one save, lookup, small edit or status check.',
    'writing': 'creative writing, documentation.',
    'image': 'image generation and edits.',
}
DEFAULT_ROUTES = {
    'judgment': ['claude/claude-fable-5-2@high', 'claude/claude-opus-5-5@high'],
    'design': ['claude/claude-opus-5-5'],
    'general': ['codex/gpt-6.2-sol@high'],
    'small': ['codex/gpt-6-luna'],
    'writing': ['antigravity/gemini-4-argon-high', 'claude/claude-opus-5-5'],
    'image': ['antigravity/gemini-4-neon-high', 'codex'],
}
TEXT_KINDS = tuple(kind for kind in KINDS if kind != 'image')
_ENTRY = re.compile(r'(claude|codex|antigravity)(?:/([A-Za-z0-9][A-Za-z0-9._:-]{0,79}))?'
                    r'(?:@(low|medium|high|xhigh|max))?\Z')
CLAUDE_ALIASES = ('opus', 'sonnet', 'haiku', 'fable')


def parse(text):
    """One route entry as {'provider', 'model', 'effort'}, or STOP. Refuses an entry the plans do not allow."""
    text = text.strip()
    if text == STOP:
        return STOP
    match = _ENTRY.fullmatch(text)
    if not match:
        raise Refused('Route %r must be provider[/model][@effort] with provider claude, codex or antigravity.' % text[:80])
    provider, model, effort = match.groups()
    claude_model = model is not None and (model.startswith('claude-') or model in CLAUDE_ALIASES)
    if provider == 'antigravity' and not (model or '').startswith('gemini-'):
        raise Refused('An Antigravity route must name a Gemini model: the Google plan runs Gemini work only.')
    if provider == 'claude' and model is not None and not claude_model:
        raise Refused('A Claude route must name a Claude model.')
    if provider == 'codex' and model is not None and (claude_model or model.startswith('gemini-')):
        raise Refused('A ChatGPT route must name a ChatGPT model.')
    return {'provider': provider, 'model': model, 'effort': effort}


def parse_order(order):
    entries = [parse(item) for item in order.split(',') if item.strip()]
    if not entries or entries[0] == STOP:
        raise Refused('A route list needs at least one route before stop.')
    if STOP in entries[:-1]:
        raise Refused('stop can only end a route list.')
    return entries


def routes(store, topic=None):
    """The route list of each kind: an active loadout swap, else the owner's list, else the default."""
    saved = dict(store.get('routes') or {})
    from .loadout import active
    for row in sorted(active(store, topic), key=lambda item: (item['topic'] is not None, item['id'])):
        if row['key'].startswith('routes.'):
            saved[row['key'].split('.', 1)[1]] = row['value'].split(',')
    values = {}
    for kind, default in DEFAULT_ROUTES.items():
        try:
            values[kind] = parse_order(', '.join(saved[kind])) if isinstance(saved.get(kind), list) else [
                parse(item) for item in default]
        except Refused:
            values[kind] = [parse(item) for item in default]
    return values


def check_kind(kind):
    if kind not in KINDS:
        raise Refused('Unknown kind. Use ' + ', '.join(KINDS) + '.')


def candidates(store, kind, task, goal=None):
    """The entries of the kind's list that can take this worker, in order."""
    check_kind(kind)
    return [entry for entry in routes(store, task['topic'])[kind] if entry != STOP and refusal(store, entry, task, goal) is None
            and (entry['provider'] != 'claude' or store.get('accounts'))]


def pick(store, kind, task, goal=None):
    found = candidates(store, kind, task, goal)
    if not found:
        raise Refused('No route of kind %s can take this worker now. See routing.show.' % kind)
    return found[0]


def op_show(store, ctx):
    table = routes(store)
    saved = store.get('routes') or {}
    lines = ['%s: %s%s' % (kind, ' → '.join(text(entry) for entry in table[kind]),
                           '' if kind in saved else ' (default)') for kind in KINDS]
    data = {kind: {'routes': [text(entry) for entry in table[kind]], 'use': KINDS[kind], 'default': kind not in saved}
            for kind in KINDS}
    return Result(True, '\n'.join(lines), data)


def op_set(store, ctx, kind, order=None):
    if kind != 'all':
        check_kind(kind)
    entries = parse_order(order) if order is not None else None
    with store.transaction():
        saved = dict(store.get('routes') or {})
        for name in TEXT_KINDS if kind == 'all' else (kind,):
            if entries is None:
                saved.pop(name, None)
            else:
                saved[name] = [text(entry) for entry in entries]
        store.put('routes', saved)
    shown = 'default' if entries is None else ' → '.join(text(entry) for entry in entries)
    return Result(True, 'Routes for %s: %s.' % (kind, shown), {'routes': saved})


class Brokers:
    """The account brokers that capacity reads, for callers outside the worker pool."""

    def __init__(self, store):
        from .accounts import AccountBroker
        from .codex_accounts import CodexBroker
        from .extension import optional
        self.store = store
        self.accounts = AccountBroker(store)
        self.codex_accounts = CodexBroker(store)
        antigravity = optional('antigravity_accounts')
        self.brokers = {'claude': self.accounts, 'codex': self.codex_accounts}
        if antigravity is not None:
            self.brokers['antigravity'] = antigravity.AntigravityBroker(store)


def order_line(pool):
    """One line for /accounts: the route of general work now, and what is out."""
    first = routes(pool.store)['general'][0]
    entries = chain(pool.store, {'provider': first['provider'], 'model': first['model'], 'kind': 'general'})
    usable = [entry for entry in entries if capacity(pool, entry)]
    out = sorted({LABELS[entry['provider']] for entry in entries if entry not in usable})
    broker = pool.codex_accounts
    names = ['ChatGPT credits' if entry['provider'] == 'codex' and broker.automatic() and broker.select()
             and broker.uses_credits(broker.select()) else label(entry) for entry in usable]
    return ('Route now: ' + (' → '.join(names) or 'none, work waits')
            + (' · out: ' + ', '.join(out) if out else '')
            + (' · owner routes: ' + ', '.join(sorted(pool.store.get('routes'))) if pool.store.get('routes') else ''))


def table_lines(store):
    """The routing table for the Models view, one line per kind."""
    return ['Routes:'] + ['%s: %s' % (kind, ' → '.join(label(entry) if entry != STOP else STOP
                                                       for entry in routes(store)[kind])) for kind in KINDS]
