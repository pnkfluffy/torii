"""Durable direct replies for normally completed parent turns."""

import json

from . import reactions
from .usage_notice import clear_unknown_limit_notice


FORWARD_RULES = {'opencode': 'all', 'claude': 'owner', 'codex': 'owner'}


def started(session, turn, messages, outbox=None):
    store = session.store
    topic = (session.key.split(':', 1)[1] if session.key != 'coordinator' else
             store.get('coordinator_session_topic') or store.get('coordinator_home_topic'))
    if outbox is None:
        outbox = outbox_cursor(store, topic)
    with store.transaction():
        owner = max((row['id'] for message in messages for row in store.db.execute(
            "SELECT id FROM messages WHERE id=? AND kind='owner'", (message,))), default=0)
        if owner:
            owner_key = session.key + '_pause_owner'
            store.put(owner_key, max(owner, store.get(owner_key) or 0))
        store.db.execute('''INSERT OR IGNORE INTO parent_reply_turns
            (host,turn,provider,topic,outbox_start,messages) VALUES (?,?,?,?,?,?)
            ON CONFLICT(host,turn) DO UPDATE SET outbox_start=MIN(outbox_start,excluded.outbox_start)
            WHERE checked=0''',
                         (session.host_id, str(turn), session.provider, topic, outbox, json.dumps(sorted(messages))))


def completed(session, turn, messages, text):
    store = session.store
    with store.transaction():
        clear_unknown_limit_notice(store, session.provider.title(), session.account_alias)
        pause_key = session.key + '_selection' if store.get(session.key + '_selection') else 'coordinator_account'
        notice = store.get(pause_key + '_notice')
        if (isinstance(notice, dict) and
                (notice.get('key') or [])[:2] == [session.provider, session.account_alias]):
            store.put(pause_key + '_notice', None)
        saved = store.db.execute('SELECT * FROM parent_reply_turns WHERE host=? AND turn=?',
                                 (session.host_id, str(turn))).fetchone()
        if saved is None:
            owners = [dict(row) for message in messages for row in store.db.execute(
                "SELECT * FROM messages WHERE id=? AND kind='owner' AND turn_started IS NOT NULL "
                "AND delivered IN ('received','sent')", (message,))]
            if not owners:
                return
            started(session, turn, messages, min(owner['turn_outbox'] for owner in owners))
            saved = store.db.execute('SELECT * FROM parent_reply_turns WHERE host=? AND turn=?',
                                     (session.host_id, str(turn))).fetchone()
        if saved['checked']:
            return
        carried = sorted(set(json.loads(saved['messages'])) | set(messages))
        owners = [dict(row) for message in carried for row in store.db.execute(
            "SELECT * FROM messages WHERE id=? AND kind='owner' AND delivered IN ('received','sent')", (message,))]
        store.db.execute('UPDATE parent_reply_turns SET checked=1,messages=? WHERE host=? AND turn=?',
                         (json.dumps([owner['id'] for owner in owners]), session.host_id, str(turn)))
        store.db.executemany('''INSERT INTO parent_reply_owners(message,host,turn) VALUES (?,?,?)
            ON CONFLICT(message) DO UPDATE SET host=excluded.host,turn=excluded.turn''',
                             [(owner['id'], session.host_id, str(turn)) for owner in owners])
        if store.db.execute("SELECT 1 FROM outbox WHERE topic=? AND id>? "
                            "AND kind NOT IN ('local-note','owner-waiting','status','wait-ask') LIMIT 1",
                            (saved['topic'], saved['outbox_start'])).fetchone():
            return
        for owner in owners:
            if not store.db.execute('SELECT 1 FROM tasks WHERE origin=?', (owner['id'],)).fetchone():
                reactions.set_desired(store, owner['id'], None)
        if (not text.strip() or owners and all(owner['reacted_to'] is not None for owner in owners) or
                FORWARD_RULES[session.provider] == 'owner' and not owners):
            return
        newest = owners[-1] if owners else None
        outbox = store.enqueue_report(saved['topic'], text, kind='parent-reply',
            reply_to=(newest['telegram_message'] or newest['reacted_to']) if newest else None)
        store.db.execute('UPDATE parent_reply_turns SET outbox=? WHERE host=? AND turn=?',
                         (outbox, session.host_id, str(turn)))


def delivered(store, outbox):
    row = store.db.execute('SELECT messages FROM parent_reply_turns WHERE outbox=?', (outbox,)).fetchone()
    if row:
        for message in json.loads(row['messages']):
            if not store.db.execute('SELECT 1 FROM tasks WHERE origin=?', (message,)).fetchone():
                reactions.set_desired(store, message, '👌')


def outbox_cursor(store, topic):
    return store.db.execute("SELECT COALESCE(MAX(id),0) FROM outbox WHERE topic=? AND kind!='local-note'",
                            (topic,)).fetchone()[0]
