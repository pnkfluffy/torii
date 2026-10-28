"""Durable retry and bounded context recovery for an OpenCode parent."""

import hashlib
import json
import re
import time

from . import opencode_accounts, parent_state


RETRY_SECONDS = 30
MAX_RETRY_SECONDS = 300
CATCHUP_CHARS = 8000


def provider_error(error):
    from . import opencode_subscription
    data = error.get('data') or {}
    body = data.get('responseBody') or {}
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except ValueError:
            body = {}
    body = body if isinstance(body, dict) else {}
    detail = body.get('error') or {}
    detail = detail if isinstance(detail, dict) else {}
    reset = body.get('resetAt')
    return {'name': error.get('name'), 'status': data.get('statusCode', detail.get('status')),
            'code': detail.get('code', body.get('code')),
            'reset_at': (opencode_subscription.bounded_reset(reset / 1000) if type(reset) in (int, float) else None)
                        or opencode_subscription.retry_at(data),
            'message': detail.get('message') or data.get('message') or error.get('message')}


def classify(error, fallback='provider_error'):
    if error.get('code') == 'weekly_included_input_token_limit_exceeded':
        return 'usage_limited'
    status = error.get('status')
    if type(status) is int and status in (401, 402, 403, 429):
        return {401: 'auth', 402: 'out_of_credit', 403: 'auth', 429: 'rate_limited'}[status]
    if error.get('name') == 'ContextOverflowError':
        return 'context_overflow'
    return fallback


def state(store, key):
    return store.get(key + '_opencode_recovery') or {}


def save(store, key, value):
    store.put(key + '_opencode_recovery', value or None)


def identity(store):
    alias = opencode_accounts.active(store)
    account = opencode_accounts.accounts(store).get(alias) or {}
    return hashlib.sha256(json.dumps([alias, account, opencode_accounts.binary(store)],
                                    sort_keys=True).encode()).hexdigest()


def owner_cursor(store, key):
    return store.db.execute("SELECT coalesce(max(id),0) FROM messages WHERE topic=? AND kind IN ('owner','forwarded')",
                            (key.split(':', 1)[1],)).fetchone()[0]


def confirm(session):
    from . import opencode_runtime
    if not session.session_id:
        return
    rows = [row['id'] for row in session.store.db.execute(
        "SELECT id FROM messages WHERE topic=? AND (delivered='uncertain' OR receipt='written')",
        (session.key.split(':', 1)[1],))]
    seen = opencode_runtime.parent_receipts(session.root, session.session_id, rows, all_sessions=True) if rows else set()
    session.store.messages_confirm(seen, 'transcript')


def unanswered(store, key, rows):
    from . import opencode_runtime
    topic = key.split(':', 1)[1]
    uncertain = [row['id'] for row in store.db.execute(
        "SELECT id FROM messages WHERE topic=? AND delivered='uncertain'", (topic,))]
    if uncertain:
        seen = opencode_runtime.parent_receipts(opencode_runtime.parent_root(store, key),
                                               store.get(key + '_session'), uncertain, all_sessions=True)
        store.messages_confirm(seen, 'transcript')
    recovery = store.get(key + '_lost_turns') or {}
    wanted = set(rows) | set(store.get(key + '_turn') or []) | set(store.get(key + '_lost') or [])
    wanted.update(state(store, key).get('rows', []))
    wanted.update(int(row) for row in recovery)
    messages = [dict(row) for row in store.db.execute(
        'SELECT id,kind,text,delivered FROM messages WHERE topic=? ORDER BY id', (topic,))]
    noise = set()
    for row in messages:
        if row['kind'] == 'restarted' or row['kind'] == 'callback' and (
                str(row['id']) in recovery or row['text'].startswith('Your last turn stopped before it finished.')):
            noise.add(row['id'])
            wanted.update(recovery.get(str(row['id']), []))
            if row['kind'] == 'callback' and (row['id'] in wanted or row['delivered'] in ('pending', 'sent', 'uncertain')):
                wanted.update(int(item) for item in re.findall(r'\bmessage=(\d+)\b', row['text']))
        elif row['delivered'] in ('pending', 'sent', 'uncertain'):
            wanted.add(row['id'])
    actual = [row['id'] for row in messages if row['id'] in wanted and row['id'] not in noise]
    store.db.executemany("UPDATE messages SET delivered='received',receipt='superseded',updated=? WHERE id=? "
                         "AND (receipt IS NULL OR receipt!='superseded')",
                         [(time.time(), row) for row in noise])
    store.requeue_messages(actual)
    store.put(key + '_lost_turns', None)
    store.put(key + '_lost', None)
    store.put(key + '_turn', None)
    return actual


def failed(store, key, session_id, code, detail, rows, owner=None):
    previous = state(store, key)
    same = previous.get('identity') == identity(store) and previous.get('code') == code
    attempts = previous.get('attempts', 0) + 1 if same else 1
    phase = 'fresh' if code == 'context_overflow' else 'retry'
    if previous.get('phase') in ('catchup', 'resuming') and code not in (
            'usage_limited', 'rate_limited', 'out_of_credit', 'auth'):
        phase = 'paused'
    with store.db:
        actual = unanswered(store, key, rows)
        current = {'identity': identity(store), 'code': code, 'detail': detail, 'attempts': attempts,
                   'retry_at': time.time() + min(MAX_RETRY_SECONDS, RETRY_SECONDS * 2 ** min(attempts - 1, 8)),
                   'owner': owner_cursor(store, key) if owner is None else owner, 'phase': phase, 'rows': actual,
                   'previous_session': session_id}
        if phase == 'retry':
            from . import opencode_runtime
            seen = sorted(opencode_runtime.parent_receipts(opencode_runtime.parent_root(store, key), session_id, actual))
            if seen:
                text = parent_state.LOST_TURN % ', '.join('message=%d' % row for row in seen)
                notice = previous.get('notice')
                reused = store.db.execute("UPDATE messages SET text=?,delivered='pending',receipt=NULL,updated=? "
                                          "WHERE id=? AND topic=? AND kind='callback'",
                                          (text, time.time(), notice, key.split(':', 1)[1])).rowcount
                if not reused:
                    notice = store.message_save(key.split(':', 1)[1], 'callback', text)['id']
                current['notice'] = notice
                store.put(key + '_lost_turns', {str(notice): seen})
                store.db.executemany("UPDATE messages SET delivered='received',receipt='written',updated=? WHERE id=?",
                                     [(time.time(), row) for row in seen])
        save(store, key, current)
        if phase == 'paused':
            store.enqueue_report(key.split(':', 1)[1],
                'This chat is paused: its fresh catch-up failed (%s). '
                'Torii will not retry it on its own. The selected OpenCode model and your saved messages remain. '
                'After the host problem is fixed, send a new message to try one fresh catch-up. %s' % (code, detail))


def catchup(store, key, current):
    topic = key.split(':', 1)[1]
    rows = set(current['rows'])
    previews = []
    for row in store.db.execute('SELECT id,kind,text FROM messages WHERE topic=? ORDER BY id', (topic,)):
        if row['id'] not in rows:
            continue
        item = {'id': row['id'], 'kind': row['kind'], 'preview': row['text'][:500]}
        if len(previews) >= 10 or len(json.dumps(previews + [item])) > 4000:
            break
        previews.append(item)
    prefix = ('The earlier conversation was too long for the host, so Torii started a fresh session on the same model. '
              'These are previews of unanswered saved messages. Wait for the saved messages to follow before acting on them. '
              'Inspect current state before repeating any earlier effects. ')
    tasks = store.tasks_list(status='open', topic=topic)
    suffix = '. Unanswered messages: ' + json.dumps({'count': len(rows), 'shown': previews}, separators=(',', ':'))
    text = prefix + 'Open jobs: ' + parent_state.jobs_summary(
        store, tasks, CATCHUP_CHARS - len(prefix) - len(suffix) - len('Open jobs: ')) + suffix
    return store.message_save(topic, 'restarted', text)['id']


def prepare(session):
    store, key = session.store, session.key
    current = state(store, key)
    if not current:
        if store.get(key + '_turn') and session.session_id:
            from . import opencode_runtime
            saved = opencode_runtime.records(session.root, session.session_id)
            if saved and (saved[0].get('error') or {}).get('name') == 'ContextOverflowError':
                failed(store, key, session.session_id, 'provider_error',
                       'Previous native turn failed; its original provider error was not retained. Recheck on this session.',
                       store.get(key + '_turn'))
                current = state(store, key)
        if not current:
            return True
    if current['identity'] != identity(store) and store.get(key + '_switch'):
        with store.db:
            unanswered(store, key, current['rows'])
            save(store, key, None)
        return True
    if current['phase'] == 'catchup':
        failed(store, key, session.session_id, 'disconnected',
               'The fresh catch-up host stopped while Torii was offline.', current['rows'], current['owner'])
        current = state(store, key)
    changed = current['identity'] != identity(store) or current['owner'] != owner_cursor(store, key)
    if changed:
        current.update(identity=identity(store), owner=owner_cursor(store, key), attempts=0, retry_at=0)
        if current['phase'] == 'paused':
            current['phase'] = 'fresh'
    elif current['phase'] == 'paused' or current['retry_at'] > time.time():
        return False
    with store.db:
        if current['phase'] == 'fresh':
            current['rows'] = unanswered(store, key, current['rows'])
            current['notice'] = catchup(store, key, current)
            store.put(key + '_session', None)
            store.put(key + '_opencode', None)
        save(store, key, current)
    return True


def observed(store, key, session_id):
    current = state(store, key)
    if current.get('phase') == 'fresh':
        current.update(phase='catchup', session=session_id)
        save(store, key, current)


def completed(store, key, rows):
    current = state(store, key)
    if current.get('phase') == 'catchup':
        if current.get('notice') in rows:
            current['phase'] = 'resuming'
            save(store, key, current)
    elif current and rows and (current.get('phase') == 'retry' or set(rows) & set(current['rows'])):
        save(store, key, None)
