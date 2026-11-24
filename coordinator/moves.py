"""Move work to the next route with capacity when its provider runs out.

A route is {'provider', 'model', 'effort'}. The queue of a piece of work is its own route,
then the route list of its kind when the routing table is installed, then the same tier on
the other of Claude and ChatGPT. Work with no kind and chat agents use the general list.
A route list names Claude, ChatGPT and Antigravity only. OpenCode work can move out.
"""

import json
import time

from .adapters import CAPS, LABELS
from .extension import optional


EQUIVALENT = {
    'claude': {'claude-fable-5-2': 'gpt-6.2-sol', 'fable': 'gpt-6.2-sol', 'claude-opus-5-5': 'gpt-6.2-sol',
               'opus': 'gpt-6.2-sol', 'claude-sonnet-5-5': 'gpt-6.1-terra', 'sonnet': 'gpt-6.1-terra',
               'claude-haiku-5-5': 'gpt-6-luna', 'haiku': 'gpt-6-luna'},
    'codex': {'gpt-6.2-sol': 'claude-opus-5-5', 'gpt-6.1-sol': 'claude-opus-5-5',
              'gpt-6.1-terra': 'claude-sonnet-5-5', 'gpt-6-luna': 'claude-haiku-5-5'},
}
FRONTIER = {'claude': 'claude-opus-5-5', 'codex': 'gpt-6.2-sol'}
STOP = 'stop'
HANDOFF = ('This job moved to you from another agent that stopped at its usage limit. Its session is not '
           'available to you. Read the job handoff and the results folder. They can be older than its last '
           'work, so inspect the actual state of the work before you continue. Check actual state before '
           'repeating actions with external effects, such as pushes, PR creation or sends.\n\n')


def text(entry):
    if entry == STOP:
        return STOP
    return entry['provider'] + ('/' + entry['model'] if entry['model'] else '') + ('@' + entry['effort'] if entry['effort'] else '')


def same(entry, other):
    """Whether a list entry names the route of this work. Work with no model runs its provider's
    default, so each entry of its provider names it."""
    return (entry != STOP and entry['provider'] == other['provider']
            and (other['model'] is None or entry['model'] in (None, other['model'])))


def label(entry):
    model = entry['model'] or ''
    if entry['provider'] == 'claude' and model.startswith('claude-'):
        parts = model.split('-')[1:]
        version = '.'.join(part for part in parts if part.isdigit())
        return 'Claude ' + ' '.join(part.title() for part in parts if not part.isdigit()) + (' ' + version if version else '')
    return LABELS[entry['provider']] + (' ' + model if model else '')


def refusal(store, entry, task, goal=None):
    """Why this route cannot take the worker, or None. Usage limits are not checked here."""
    provider = entry['provider']
    if provider == 'codex' and not store.get('codex_enabled', True):
        return 'Codex delegation is disabled.'
    from .claude_settings import plugin_switches
    from .workers import spawn_refusal
    if not CAPS[provider].plugins and any(plugin_switches(store, task['topic']).values()):
        return 'Plugins run on Claude and Codex only.'
    return spawn_refusal(store, provider, task, goal, entry['model'])


def chain(store, work):
    """Every route a piece of work may run on, in order.

    Work on the route it was given starts from that route, then walks its list. Work that
    Torii moved walks its list from the start, so it returns to an earlier route. An image
    list and a list that ends with stop have no tail. Antigravity enters only through a list.
    """
    own = {'provider': work['provider'], 'model': work['model'], 'effort': None}
    kind = work.get('kind') or 'general'
    routing = optional('routing')
    entries = list(routing.routes(store, work.get('topic'))[kind]) if routing is not None else []
    if not work.get('routed_from'):
        entries = [own] + [entry for entry in entries if not same(entry, own)]
    elif not entries:
        entries = [own]
    if entries[-1] == STOP:
        return entries[:-1]
    if kind == 'image':
        return entries
    first = entries[0]
    present = {entry['provider'] for entry in entries}
    for provider in ('claude', 'codex'):
        if provider not in present:
            model = EQUIVALENT.get(first['provider'], {}).get(first['model'] or '') or FRONTIER[provider]
            entries = entries + [{'provider': provider, 'model': model, 'effort': first['effort']}]
    return entries


def capacity(pool, entry):
    """Whether any account of the route's provider can run a worker on that model now. A route
    with no model asks for its provider's default model."""
    broker = pool.brokers.get(entry['provider'])
    if broker is None:
        return False
    return broker.capacity(entry['model'] or broker.default_model())


def started(worker):
    return not worker['fresh'] or bool(CAPS[worker['provider']].early_session and worker.get('session'))


def next_route(pool, worker, task, failed=False):
    """The route a worker should move to now, or None to stay.

    A worker that has not started stays while its own route has capacity, unless a route
    before it in its queue has capacity again. failed means its own route just refused it.
    A started worker finishes where it is: it moves only when its own route has no capacity.
    Capacity is for the provider, not for the account of the worker. A started worker whose
    session stays on one account waits for that account while another account of its provider
    has capacity: it does not leave the provider.
    """
    if worker['provider'] not in CAPS:
        return None
    if worker['provider'] == 'codex' and not (pool.codex_accounts.managed() and pool.codex_accounts.automatic()
                                              and pool.store.get('codex_enabled', True)):
        return None
    if CAPS[worker['provider']].switched and pool.store.get(worker['provider'] + '_enabled', False) is not True:
        return None
    entries = chain(pool.store, worker)
    own = {'provider': worker['provider'], 'model': worker['model'], 'effort': None}
    if started(worker) and capacity(pool, own):
        return None
    for entry in entries:
        if same(entry, own):
            if not failed and capacity(pool, own):
                return None
            continue
        if refusal(pool.store, entry, task, worker['goal']) is None and capacity(pool, entry):
            return entry
    return None


def _home(store):
    home = store.get('coordinator_home_topic')
    topic = store.topic(home) if home else None
    return home if topic and topic['enabled'] else None


def move(pool, worker, entry, result=None):
    """Requeue a worker on another route. Notify home once per outage of its provider.

    A started worker keeps its session when the new route is on its own provider, which must
    be Claude or ChatGPT. On another provider it starts a new session from a handoff: its
    brief with its steers, and the HANDOFF.md of its job.
    """
    store = pool.store
    old = {'provider': worker['provider'], 'model': worker['model'], 'effort': None}
    out = not capacity(pool, dict(old, model=None))
    payload = result if result is not None else json.loads(worker['result'] or '{}')
    kept = bool(started(worker) and worker['session'] and entry['provider'] == worker['provider']
                and CAPS[entry['provider']].keeps_session)
    prompt = handoff = None
    if kept:
        payload = dict(payload, status='waiting_for_quota')
    elif started(worker):
        from .worker_storage import handoff_brief
        from .workers import continuation_brief
        prompt = HANDOFF + continuation_brief(store, worker).removeprefix(HANDOFF)
        handoff = handoff_brief(store, worker['task']) or None
    with store.db:
        changed = store.db.execute(
            "UPDATE workers SET provider=?,model=?,effort=?,session=?,fresh=?,account_alias=NULL,status='queued',"
            "pid=NULL,result=?,prompt=COALESCE(?,prompt),handoff=COALESCE(?,handoff),"
            "routed_from=COALESCE(routed_from,?),updated=? "
            "WHERE id=? AND status IN ('queued','running','waiting_for_quota')",
            (entry['provider'], entry['model'], entry['effort'] or worker['effort'],
             worker['session'] if kept else None, int(not kept), json.dumps(payload) if payload else None,
             prompt, handoff, text(old), time.time(), worker['id'])).rowcount
        if not changed:
            return False
        name = LABELS.get(worker['provider'], worker['provider'].title())
        if out and entry['provider'] != worker['provider']:
            store.task_note(worker['task'], '%s is out; worker %d runs on %s.' % (name, worker['id'], label(entry)))
            since = dict(store.get('fallback_since') or {})
            if since.get(worker['provider']) is None:
                since[worker['provider']] = time.time()
                store.put('fallback_since', since)
                home = _home(store)
                if home:
                    store.enqueue_report(home, '%s is out. New %s work now runs on %s.' % (name, name, label(entry)))
        else:
            store.task_note(worker['task'], 'Worker %d runs on %s.' % (worker['id'], label(entry)))
    return True


def check_recovery(pool):
    """Send one notice for each provider that is back after an outage notice. True when one is back."""
    store = pool.store
    back = False
    since = dict(store.get('fallback_since') or {})
    for provider in sorted(since):
        if not capacity(pool, {'provider': provider, 'model': None, 'effort': None}):
            continue
        del since[provider]
        store.put('fallback_since', since)
        home = _home(store)
        name = LABELS.get(provider, provider.title())
        back = True
        if home:
            store.enqueue_report(home, '%s is back. New %s work runs on %s again.' % (name, name, name))
    return back
