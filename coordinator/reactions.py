"""Service-owned reaction policy, delivery, and owner reaction intake."""

import logging
import time

from . import problems
from .telegram import TelegramError

logger = logging.getLogger(__name__)
REPLY_TOOL = 'mcp__torii__telegram_send'
DELEGATION_TOOLS = ('Agent', 'Task', 'Workflow', 'mcp__torii__workers_spawn', 'collabAgentToolCall')
EMOJI = ('👀', '👨\u200d💻', '🤔', '👌')


def reaction_target(store, message):
    row = store.db.execute("SELECT m.*,t.chat FROM messages m JOIN topics t ON t.id=m.topic "
                           "WHERE m.id=? AND m.kind='owner'", (message,)).fetchone()
    if row is None:
        return None
    if row['telegram_message'] is not None:
        return dict(row, table='messages')
    if row['reacted_to'] is not None:
        return stored_target(store, row['chat'], row['reacted_to'])
    return None


def stored_target(store, chat, telegram_message):
    for table in ('outbox', 'messages'):
        row = store.db.execute('SELECT r.*,t.chat FROM ' + table + ' r JOIN topics t ON t.id=r.topic '
                               'WHERE t.chat=? AND r.telegram_message=? ORDER BY r.id DESC LIMIT 1',
                               (chat, telegram_message)).fetchone()
        if row is not None:
            return dict(row, table=table)
    return None


def set_desired(store, message, emoji):
    if emoji is not None and emoji not in EMOJI:
        raise ValueError('Unsupported bot reaction')
    target = reaction_target(store, message)
    if target is None:
        return
    store.db.execute("UPDATE " + target['table'] + """ SET reaction_desired=?,
        reaction_state=CASE WHEN reaction_state='disabled' THEN 'disabled' ELSE 'pending' END,
        reaction_attempts=0,reaction_retry=0
        WHERE id=? AND reaction_desired IS NOT ?""", (emoji, target['id'], emoji))


def handed_off(store, message):
    store.db.execute("""UPDATE messages SET turn_started=?,turn_eyes=0,turn_tools=0,
        turn_outbox=(SELECT COALESCE(MAX(id),0) FROM outbox WHERE topic=messages.topic)
        WHERE id=? AND kind='owner' AND (telegram_message IS NOT NULL OR reacted_to IS NOT NULL)
        AND turn_started IS NULL""",
                     (time.time(), message))


def parent_tools(store, messages, content):
    if not isinstance(content, list):
        return
    names = [block['name'] for block in content
             if isinstance(block, dict) and block.get('type') == 'tool_use'
             and isinstance(block.get('name'), str) and block['name'] != REPLY_TOOL]
    if not names:
        return
    delegated = any(name in DELEGATION_TOOLS for name in names)
    for message in messages:
        row = store.db.execute("""UPDATE messages SET turn_tools=turn_tools+? WHERE id=?
            AND turn_started IS NOT NULL AND turn_eyes=0
            AND NOT EXISTS (SELECT 1 FROM tasks WHERE origin=messages.id)
            AND turn_outbox=(SELECT COALESCE(MAX(id),0) FROM outbox WHERE topic=messages.topic)""",
                               (len(names), message))
        if row.rowcount and (delegated or store.db.execute(
                'SELECT turn_tools FROM messages WHERE id=?', (message,)).fetchone()[0] >= 2):
            set_desired(store, message, '👀')
            store.db.execute('UPDATE messages SET turn_eyes=1 WHERE id=?', (message,))


def finish_turn(store, messages):
    for message in messages:
        row = store.db.execute("""SELECT turn_eyes FROM messages m WHERE id=? AND turn_started IS NOT NULL
            AND NOT EXISTS (SELECT 1 FROM tasks WHERE origin=m.id)""", (message,)).fetchone()
        if row and row['turn_eyes']:
            set_desired(store, message, None)
        store.db.execute('UPDATE messages SET turn_started=NULL,turn_eyes=0 WHERE id=?', (message,))


def worker_started(store, worker):
    task = store.task_get(worker['task']) if worker['task'] else None
    if task and task['status'] == 'open':
        set_desired(store, task['origin'], '👨\u200d💻' if worker['work'] == 'dev' else '🤔')


async def deliver_reaction(store, telegram):
    row = store.db.execute('''SELECT 'messages' AS source,m.id AS id,m.topic,m.telegram_message,m.reaction_desired,
        m.reaction_attempts,m.reaction_retry,m.created,t.chat FROM messages m JOIN topics t ON t.id=m.topic
        WHERE m.telegram_message IS NOT NULL AND m.reaction_state='pending' AND m.reaction_retry<=?
        UNION ALL SELECT 'outbox',o.id,o.topic,o.telegram_message,o.reaction_desired,o.reaction_attempts,
        o.reaction_retry,NULL,t.chat FROM outbox o JOIN topics t ON t.id=o.topic
        WHERE o.telegram_message IS NOT NULL AND o.reaction_state='pending' AND o.reaction_retry<=?
        ORDER BY reaction_retry,id,source LIMIT 1''', (time.time(), time.time())).fetchone()
    if not row:
        return False
    desired = row['reaction_desired']
    try:
        await telegram.call('setMessageReaction', chat_id=row['chat'], message_id=row['telegram_message'],
                            reaction=[{'type': 'emoji', 'emoji': desired}] if desired else [])
    except TelegramError as error:
        disabled = error.code in (400, 403) or row['reaction_attempts'] >= 4
        with store.db:
            store.db.execute('UPDATE ' + row['source'] + ''' SET reaction_state=?,reaction_attempts=reaction_attempts+1,
                reaction_retry=? WHERE id=? AND (reaction_desired IS ? OR ?)''',
                             ('disabled' if disabled else 'pending',
                              time.time() + (error.retry_after or min(60, 2 ** (row['reaction_attempts'] + 1))),
                              row['id'], desired, disabled))
            logger.info('reaction failed code=%s disabled=%s', error.code, disabled)
            problems.record(store, 'telegram', 'reaction-timeout' if error.reason == 'timeout' else 'reaction-failed',
                            ('outbox=%s ' % row['id'] if row['source'] == 'outbox' else '') +
                            'telegram=%s disabled=%s attempts=%s' % (error.code, disabled, row['reaction_attempts'] + 1),
                            message=row['id'] if row['source'] == 'messages' else None, topic=row['topic'])
    else:
        with store.db:
            store.db.execute('UPDATE ' + row['source'] + """ SET reaction_sent=?,
                reaction_state=CASE WHEN reaction_desired IS ? THEN 'sent' ELSE 'pending' END WHERE id=?""",
                             (desired, desired, row['id']))
        logger.info('reaction sent source=%s message=%s after=%.2f', row['source'], row['id'],
                    time.time() - row['created'] if row['created'] else 0)
    return True


def accept_reaction(store, reaction):
    user = reaction.get('user') or {}
    chat = reaction.get('chat') or {}
    message = reaction.get('message_id')
    if (type(user.get('id')) is not int or user['id'] != store.get('owner') or user.get('is_bot')
            or reaction.get('actor_chat') or type(chat.get('id')) is not int or type(message) is not int):
        return 'ignored'
    old = reaction.get('old_reaction') or []
    added = [item for item in reaction.get('new_reaction') or [] if item not in old]
    labels = []
    for item in added:
        if item.get('type') == 'emoji' and isinstance(item.get('emoji'), str):
            labels.append(item['emoji'])
        elif item.get('type') == 'custom_emoji':
            labels.append('(custom emoji)')
        elif item.get('type') == 'paid':
            labels.append('⭐')
    if not labels:
        return 'ignored'
    if not any(topic['chat'] == chat['id'] for topic in store.topics()):
        return 'ignored'
    target = stored_target(store, chat['id'], message)
    if target is None:
        logger.info('owner reaction dropped unknown chat=%s message=%s', chat['id'], message)
        return 'ignored'
    if not store.topic(target['topic'])['enabled']:
        return 'ignored'
    store._save_owner_message(target['topic'], None, 'Reacted ' + ' '.join(labels),
                              reply=message, reacted_to=message)
    return 'queued'


async def check_administrators(store, telegram):
    chats = sorted({topic['chat'] for topic in store.topics() if topic['chat'] < 0})
    if not chats:
        return
    try:
        me = await telegram.call('getMe')
    except TelegramError as error:
        problems.record(store, 'telegram', 'reaction-admin-check-failed', 'getMe telegram=%s' % error.code)
        return
    for chat in chats:
        try:
            member = await telegram.call('getChatMember', chat_id=chat, user_id=me['id'])
        except TelegramError as error:
            problems.record(store, 'telegram', 'reaction-admin-check-failed', 'chat=%s telegram=%s' % (chat, error.code))
            continue
        if member['status'] not in ('administrator', 'creator'):
            problems.record(store, 'telegram', 'reaction-admin-required',
                            'chat=%s status=%s; owner reactions require bot administrator status' % (chat, member['status']))
