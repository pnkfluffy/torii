"""Envelopes: ask for a credential by name, take it in the private chat, and seal it in the vault.

Every function that runs inside `Store.accept` or inside an operation handler executes
statements only. The caller owns the transaction. Vault calls never run inside one.

The HANDOFF envelope carries a Claude sign-in code. The owner pastes it in the topic of the
sign-in card, and `signin.intercept_topic_code` hands it to the waiting sign-in in memory
through `store.signin`. It never reaches the vault.
"""

from datetime import datetime, timezone
import hashlib
import json
import logging
import os
import re
import secrets
import sqlite3
import time

from . import problems
from .control_api import Refused, Result

logger = logging.getLogger(__name__)

LIFETIME = 600
HANDOFF = 'CLAUDE_SIGNIN_CODE'
LIVE = ('open', 'armed')
NAME = re.compile(r'[A-Z][A-Z0-9_]{1,63}\Z')
RESERVED = frozenset((
    'PATH', 'HOME', 'USER', 'SHELL', 'TMPDIR', 'PWD', 'LANG', 'BOT_TOKEN', 'NODE_OPTIONS',
    'NODE_EXTRA_CA_CERTS', 'BASH_ENV', 'ENV', 'PROMPT_COMMAND', 'IFS', 'SHELLOPTS', 'HTTP_PROXY',
    'HTTPS_PROXY', 'ALL_PROXY', 'NO_PROXY', 'SSL_CERT_FILE', 'SSL_CERT_DIR', 'REQUESTS_CA_BUNDLE',
    'RUST_LOG', 'RUST_BACKTRACE', 'ZDOTDIR', 'TMP', 'TEMP', 'NODE_REPL_AUTH_TOKEN'))
RESERVED_PREFIXES = ('LC_', 'PYTHON', 'DYLD_', 'LD_', 'CLAUDE', 'ANTHROPIC_', 'CODEX_', 'OPENAI_', 'TORII_',
                     'TELEGRAM', 'CMUX_', 'GIT_', 'XDG_', 'PERL5', 'RUBYOPT')


def valid_name(name):
    return (isinstance(name, str) and bool(NAME.fullmatch(name)) and name not in RESERVED
            and not name.startswith(RESERVED_PREFIXES))


def parse_declarations(value):
    """Task secrets as a sorted list of `NAME` or `ENV_NAME=VAULT_NAME`. Raise ValueError when invalid."""
    entries = value.split(',') if isinstance(value, str) else value
    if not isinstance(entries, list):
        raise ValueError('not a list')
    targets = {}
    for entry in entries:
        if not isinstance(entry, str):
            raise ValueError('not a name')
        entry = entry.strip()
        if not entry and isinstance(value, str) and not value.strip():
            continue
        environment, separator, stored = entry.partition('=')
        stored = stored if separator else environment
        if not valid_name(environment) or not valid_name(stored):
            raise ValueError('invalid name')
        if targets.setdefault(environment, stored) != stored:
            raise ValueError('one environment name maps to two vault names')
    return sorted(name if name == stored else name + '=' + stored for name, stored in targets.items())


def declared(entry):
    """(environment name, vault name) for one parsed declaration."""
    environment, _, stored = entry.partition('=')
    return environment, stored or environment


def token_hash(token):
    return hashlib.sha256(token.encode()).hexdigest()


def _row(store, envelope_id):
    row = store.db.execute('SELECT * FROM envelopes WHERE id=?', (envelope_id,)).fetchone()
    return dict(row) if row else None


def _task_line(store, envelope):
    task = store.task_get(envelope['task']) if envelope['task'] else None
    return 'Job: ' + ('%d %s' % (task['number'], task['title'][:200]) if task else 'any job')


def ask_text(store, envelope):
    if envelope['name'] == HANDOFF:
        return envelope['reason'] + '\n3. Paste it here as one message within 10 minutes. I delete it at once.'
    return ('Envelope: ' + envelope['name'] + '\nWhy: ' + envelope['reason'][:200] +
            '\nUsed by: ' + envelope['consumer'][:200] + '\n' + _task_line(store, envelope) +
            '\nTap Fill privately within 10 minutes and send the value there in one message. '
            'Never paste it in this channel.')


def _cancel_button(envelope):
    return [{'text': 'Cancel', 'callback_data': 'envelope:%d:cancel' % envelope['id']}]


def card_message(store, outbox_id):
    """The Telegram message that shows a card, delivered or waiting to be edited."""
    row = store.db.execute('SELECT delivered,telegram_message,edit_message FROM outbox WHERE id=?',
                           (outbox_id,)).fetchone() if outbox_id else None
    if not row:
        return None
    return row['telegram_message'] if row['delivered'] and row['telegram_message'] else row['edit_message']


def update_card(store, envelope, text, reply_markup=None):
    """Clear the current card's buttons from the database, queue the edit, and follow it."""
    store.db.execute('UPDATE outbox SET reply_markup=NULL WHERE id=?', (envelope['card_outbox'],))
    outbox = store.enqueue_report(envelope['topic'], text, kind='envelope', reply_markup=reply_markup,
                                  edit=card_message(store, envelope['card_outbox']))
    store.db.execute('UPDATE envelopes SET card_outbox=? WHERE id=?', (outbox, envelope['id']))
    envelope['card_outbox'] = outbox
    return outbox


CLOSED_TEXT = {'cancelled': 'Cancelled.', 'expired': 'Expired. Use Rotate to ask again.',
               'revoked': 'Revoked. The stored value was removed.'}
SIGNIN_CLOSED = {'cancelled': 'Claude sign-in cancelled. No account was added.',
                 'expired': 'The sign-in link expired. No account was added.',
                 'revoked': 'Claude sign-in stopped. No account was added.'}


def close(store, envelope, state, event, source=None, reason=None):
    """The single path that closes an envelope: live to cancelled or expired, live or filled to revoked."""
    now = time.time()
    store.db.execute('UPDATE envelopes SET state=?,updated=? WHERE id=?', (state, now, envelope['id']))
    envelope['state'] = state
    store.envelope_event(envelope['id'], envelope['name'], event, reason=reason, source=source)
    if envelope['name'] == HANDOFF:
        from .signin import RETRY_DATA
        retry = {'inline_keyboard': [[{'text': 'Try again', 'callback_data': RETRY_DATA}]]}
        update_card(store, envelope, SIGNIN_CLOSED[state], retry if state == 'expired' else None)
        return
    update_card(store, envelope, 'Envelope: ' + envelope['name'] + '\n' + CLOSED_TEXT[state])


def sweep(store, now=None):
    now = time.time() if now is None else now
    rows = [dict(row) for row in store.db.execute(
        "SELECT * FROM envelopes WHERE state IN ('open','armed') AND expires<=? ORDER BY id", (now,))]
    for envelope in rows:
        close(store, envelope, 'expired', 'expire', source='sweep')
    return len(rows)


def cancel(store, envelope_id, source):
    envelope = _row(store, envelope_id)
    if not envelope or envelope['state'] not in LIVE:
        return 'envelope_closed'
    close(store, envelope, 'cancelled', 'cancel', source=source)
    return 'envelope_cancel'


def ask(store, topic, name, reason, consumer, task=None, source='cli'):
    if name in os.environ:
        raise Refused(name + ' is set in the service environment. Choose another name.')
    username = store.get('bot_username')
    if not username:
        raise Refused('The service has not confirmed the bot identity yet.')
    if task is not None and not store.task_get(task):
        raise Refused('Unknown job.')
    sweep(store)
    if store.db.execute("SELECT 1 FROM envelopes WHERE name=? AND state IN ('open','armed')", (name,)).fetchone():
        raise Refused('An envelope for ' + name + ' is already open.')
    token = secrets.token_urlsafe(24)
    now = time.time()
    try:
        cursor = store.db.execute('''INSERT INTO envelopes
            (name,reason,consumer,task,topic,token_hash,state,created,expires,updated)
            VALUES (?,?,?,?,?,?,'open',?,?,?)''',
                                  (name, reason, consumer, task, topic, token_hash(token), now, now + LIFETIME, now))
    except sqlite3.IntegrityError as error:
        raise Refused('An envelope for ' + name + ' is already open.') from error
    envelope = _row(store, cursor.lastrowid)
    store.envelope_event(envelope['id'], name, 'ask', source=source)
    fill = [{'text': 'Fill privately', 'url': 'https://t.me/' + username + '?start=' + token}]
    keyboard = ([] if name == HANDOFF else [fill]) + [_cancel_button(envelope)]
    outbox = store.enqueue_report(topic, ask_text(store, envelope), kind='envelope',
                                  reply_markup={'inline_keyboard': keyboard})
    store.db.execute('UPDATE envelopes SET card_outbox=? WHERE id=?', (outbox, envelope['id']))
    return envelope['id']


def op_secret_ask(store, ctx, topic, name, reason, consumer, task=None):
    envelope = ask(store, topic, name, reason, consumer, task, ctx.source)
    return Result(True, 'Envelope for %s posted. The owner fills it privately.' % name,
                  {'envelope': envelope, 'name': name})



REPLIES = {
    'bare_start': 'Open an Envelope card in your Torii channel and tap Fill privately.',
    'token_unknown': 'This link is not valid.',
    'token_used': 'This link was already used.',
    'token_expired': 'This link expired. Use Rotate to ask again.',
    'token_closed': 'This envelope was closed. Use Rotate to ask again.',
    'busy': 'Another envelope is waiting for its value. Send that value or /cancel first.',
    'nothing_to_cancel': 'No envelope is waiting for a value.',
    'no_armed': 'No envelope is waiting for a value, so I deleted your message. Tap Fill privately on an Envelope card first.',
    'not_text': 'Send the value as one text message. I deleted your message.',
    'too_short': 'The value must have at least 16 characters. I deleted your message. Send the value again.',
    'too_long': 'The value must have at most 4096 characters. I deleted your message. Send the value again.',
    'multi_line': 'Send the value as one line. I deleted your message. Send the value again.',
    'not_printable': 'The value has characters that cannot be printed. I deleted your message. Send the value again.',
    'command': 'The value cannot start with /. I deleted your message. Send the value again.',
    'numeric': 'A value made only of digits is refused. I deleted your message. Send the value again.',
    'vault_failed': 'The vault write failed. Your message was deleted. Send the value again.',
    'internal': 'Something failed while I handled your message. I deleted it. Send the value again.',
    'handoff_closed': 'That sign-in is no longer waiting for a code, so I deleted your message.',
}
SIGNIN_REPLIES = {
    'bare_start': ('To finish the Claude sign-in, paste the code in your Torii group, in the topic with the '
                   'sign-in card.'),
    'nothing_to_cancel': ('Nothing is waiting here. To stop the Claude sign-in, tap Cancel on its card in your '
                          'Torii group.'),
    'no_armed': ('I deleted your message. Paste the Claude sign-in code in your Torii group, in the topic with the '
                 'sign-in card, not here.'),
}
MAX_ATTEMPTS = 5


def _replies(store):
    """The replies for a private chat with nothing armed. They point to the card while a Claude sign-in runs."""
    from .signin import pending
    record = pending(store)
    signin = record and record.get('provider') != 'codex' and record.get('state') != 'cancelled'
    return SIGNIN_REPLIES if signin else REPLIES


def _clock(value):
    return datetime.fromtimestamp(value, timezone.utc).strftime('%Y-%m-%d %H:%M UTC')


def value_problem(message):
    text = message.get('text')
    if not isinstance(text, str) or not text:
        return 'not_text'
    if '\n' in text or '\r' in text:
        return 'multi_line'
    if not text.isprintable():
        return 'not_printable'
    if text.startswith('/'):
        return 'command'
    if len(text) < 16:
        return 'too_short'
    if len(text) > 4096:
        return 'too_long'
    if text.isdigit():
        return 'numeric'
    return None


def _refusal(store, message):
    sender = message.get('from')
    chat = message.get('chat')
    if not isinstance(sender, dict) or not isinstance(chat, dict):
        return 'invalid'
    if sender.get('is_bot') or message.get('sender_chat'):
        return 'bot_or_channel'
    if type(sender.get('id')) is not int or sender['id'] != store.get('owner'):
        return 'not_owner'
    if chat.get('type') != 'private' or chat.get('id') != sender['id']:
        return 'chat_mismatch'
    if type(message.get('message_id')) is not int:
        return 'invalid'
    return None


def _command(message):
    text = message.get('text')
    parts = text.split() if isinstance(text, str) else []
    return parts[0].split('@', 1)[0] if parts else ''


def prepare_private(store, message):
    """Check a private message, consume a stale sign-in arm, and write the vault."""
    now = time.time()
    authorized = False
    try:
        refusal = _refusal(store, message)
        if refusal:
            return {'ignored': refusal}
        authorized = True
        if _command(message) in ('/start', '/cancel'):
            return {'now': now, 'command': _command(message)}
        armed = store.db.execute("SELECT id,name FROM envelopes WHERE state='armed' AND chat=? AND expires>?",
                                 (message['chat']['id'], now)).fetchone()
        if not armed:
            return {'now': now, 'problem': 'no_armed'}
        intake = {'now': now, 'envelope': armed['id'], 'name': armed['name']}
        problem = value_problem(message)
        if problem:
            return dict(intake, problem=problem)
        if armed['name'] == HANDOFF:
            signin = getattr(store, 'signin', None)
            if signin is None or not signin.hand(armed['id'], message['text']):
                return dict(intake, problem='handoff_closed')
            return dict(intake, handed=len(message['text']))
        if store.vault is None:
            return dict(intake, problem='vault_failed:unavailable')
        from .vault import VaultError
        try:
            return dict(intake, stored=store.vault.put(armed['name'], message['text']))
        except VaultError as error:
            problems.record(store, 'secrets', 'vault-write-failed', 'name=%s code=%s' % (armed['name'], error.code))
            return dict(intake, problem='vault_failed:' + error.code)
    except Exception as error:
        logger.error('envelope intake check failed type=%s', type(error).__name__)
        problems.record(store, 'secrets', 'intake-failed', type(error).__name__, level=logging.ERROR)
        return {'now': now, 'problem': 'internal'} if authorized else {'ignored': 'internal'}


def _effect(store, envelope, kind, chat, message=None, text=None, thread=None):
    store.db.execute('''INSERT INTO envelope_effects(envelope,kind,chat,message,text,created,thread)
        VALUES (?,?,?,?,?,?,?)''', (envelope, kind, chat, message, text, time.time(), thread))


def _reply(store, envelope, chat, text, thread=None):
    _effect(store, envelope, 'reply', chat, text=text, thread=thread)


def _arm(store, envelope, chat, thread, now, source):
    if envelope is None:
        return 'token_unknown'
    if envelope['expires'] <= now or envelope['state'] == 'expired':
        return 'token_expired'
    if envelope['state'] in ('cancelled', 'revoked'):
        return 'token_closed'
    if envelope['state'] != 'open':
        return 'token_used'
    if store.db.execute("SELECT 1 FROM envelopes WHERE state='armed' AND expires>? AND chat=? AND id!=?",
                        (now, chat, envelope['id'])).fetchone():
        return 'busy'
    store.db.execute("UPDATE envelopes SET state='armed',chat=?,armed_at=?,arm_thread=?,updated=? WHERE id=?",
                     (chat, now, thread, now, envelope['id']))
    store.envelope_event(envelope['id'], envelope['name'], 'arm', source=source)
    text = ask_text(store, envelope).split('\nTap Fill privately')[0] + '\nWaiting for the value in the private chat.'
    update_card(store, envelope, text, {'inline_keyboard': [_cancel_button(envelope)]})
    return None


def _start(store, message, chat, now):
    parts = message['text'].split()
    thread = None
    if len(parts) != 2:
        _reply(store, None, chat, _replies(store)['bare_start'], thread)
        return 'envelope_start'
    row = store.db.execute('SELECT * FROM envelopes WHERE token_hash=?', (token_hash(parts[1]),)).fetchone()
    envelope = dict(row) if row else None
    outcome = _arm(store, envelope, chat, thread, now, 'intake')
    if outcome is None:
        _reply(store, envelope['id'], chat, 'Send the Claude sign-in code now in one message. I will delete it '
               'after I pass it to Claude. Send /cancel to stop.' if envelope['name'] == HANDOFF else
               'Send the value for ' + envelope['name'] +
               ' now in one message. I will delete it after I store it. Send /cancel to stop.', thread)
        return 'envelope_arm'
    store.envelope_event(envelope and envelope['id'], envelope and envelope['name'], 'reject',
                         reason=outcome, source='intake')
    _reply(store, envelope and envelope['id'], chat, REPLIES[outcome], thread)
    return 'envelope_reject'


def _cancel_private(store, chat, thread=None):
    states = "('armed')"
    row = store.db.execute('SELECT * FROM envelopes WHERE state IN ' + states + ' AND chat=?', (chat,)).fetchone()
    if not row:
        _reply(store, None, chat, _replies(store)['nothing_to_cancel'], thread)
        return 'envelope_reject'
    close(store, dict(row), 'cancelled', 'cancel', source='intake')
    _reply(store, row['id'], chat, 'Cancelled ' + row['name'] + '.', thread)
    return 'envelope_cancel'


def _fill(store, intake, chat, thread=None):
    envelope = _row(store, intake['envelope'])
    if envelope['state'] != 'armed':
        raise RuntimeError('envelope left the armed state')
    length, fingerprint = intake['stored']
    now = intake['now']
    store.db.execute("UPDATE envelopes SET state='superseded',updated=? WHERE name=? AND state='filled'",
                     (now, envelope['name']))
    store.db.execute("UPDATE envelopes SET state='filled',filled_at=?,length=?,fingerprint=?,updated=? WHERE id=?",
                     (now, length, fingerprint, now, envelope['id']))
    store.envelope_event(envelope['id'], envelope['name'], 'fill', source='intake')
    store.envelope_event(envelope['id'], envelope['name'], 'use', source='intake:readback')
    task = store.task_get(envelope['task']) if envelope['task'] else None
    job = ('job %d' % task['number'] if task else 'any job')
    store.message_secret_filled(envelope['topic'], envelope['id'],
                                'Secret %s, declared on %s, is filled.' % (envelope['name'], job))
    facts = 'Length %d, fingerprint %s' % (length, fingerprint)
    _reply(store, envelope['id'], chat, '%s stored. %s.' % (envelope['name'], facts), thread)
    update_card(store, envelope, 'Envelope: %s\nFilled. %s, at %s.' % (envelope['name'], facts, _clock(now)))
    return 'envelope_fill'


def _hand(store, intake, chat, thread=None):
    envelope = _row(store, intake['envelope'])
    if envelope['state'] != 'armed':
        raise RuntimeError('envelope left the armed state')
    handed(store, envelope, intake['handed'], intake['now'], 'intake:handoff')
    _reply(store, envelope['id'], chat, 'Code received. I passed it to Claude and deleted your message.', thread)
    return 'envelope_fill'


def handed(store, envelope, length, now, source):
    """Record that the sign-in code went to Claude, by length only, and show it on the card."""
    store.db.execute("UPDATE envelopes SET state='superseded',updated=? WHERE name=? AND state='filled'",
                     (now, envelope['name']))
    store.db.execute("UPDATE envelopes SET state='filled',filled_at=?,length=?,updated=? WHERE id=?",
                     (now, length, now, envelope['id']))
    store.envelope_event(envelope['id'], envelope['name'], 'fill', source=source)
    update_card(store, envelope, envelope['reason'] + '\nCode received. Checking it with Claude.')


def _apply(store, message, intake, chat):
    thread = None
    sweep(store, intake['now'])
    command = intake.get('command')
    if command == '/start':
        return _start(store, message, chat, intake['now'])
    if command == '/cancel':
        return _cancel_private(store, chat, thread)
    _effect(store, intake.get('envelope'), 'delete', chat, message['message_id'])
    if 'stored' in intake:
        return _fill(store, intake, chat, thread)
    if 'handed' in intake:
        return _hand(store, intake, chat, thread)
    problem = intake['problem']
    store.envelope_event(intake.get('envelope'), intake.get('name'), 'reject', reason=problem, source='intake')
    reason = problem.split(':', 1)[0]
    reply = (_replies(store) if reason == 'no_armed' else REPLIES)[reason]
    _reply(store, intake.get('envelope'), chat, reply, thread)
    return 'envelope_reject'


def accept_private(store, message, intake):
    """Write the outcome of `prepare_private` inside the update's transaction. Never raises."""
    if 'ignored' in intake:
        logger.info('envelope private ignored reason=%s', intake['ignored'])
        return 'ignored'
    chat = message['chat']['id']
    store.db.execute('SAVEPOINT envelope_intake')
    try:
        outcome = _apply(store, message, intake, chat)
        store.db.execute('RELEASE envelope_intake')
        return outcome
    except Exception as error:
        store.db.execute('ROLLBACK TO envelope_intake')
        store.db.execute('RELEASE envelope_intake')
        logger.error('envelope intake failed type=%s', type(error).__name__)
        if not intake.get('command'):
            _effect(store, intake.get('envelope'), 'delete', chat, message['message_id'], thread=message.get('message_thread_id'))
        store.envelope_event(intake.get('envelope'), intake.get('name'), 'reject', reason='internal', source='intake')
        _reply(store, intake.get('envelope'), chat, REPLIES['internal'], message.get('message_thread_id'))
        return 'envelope_failed'


def _delete_failed(store, effect):
    envelope = _row(store, effect['envelope']) if effect['envelope'] else None
    name = envelope['name'] if envelope else None
    store.envelope_event(effect['envelope'], name, 'delete_failed', source='effects')
    advice = ' and consider rotating ' + name + '.' if name else '.'
    if effect['chat'] == store.get('owner'):
        _reply(store, effect['envelope'], effect['chat'], 'I could not delete your message. Delete it yourself' + advice)
    elif envelope and name == HANDOFF:
        store.enqueue_report('%s:%s' % (effect['chat'], effect['thread']) if effect['thread'] else envelope['topic'],
                             'I could not delete the message with the sign-in code. '
                             'Delete it yourself.')
        return
    elif envelope:
        store.enqueue_report(envelope['topic'], 'I could not delete your reply in this channel. Delete it yourself' + advice)
    if envelope and envelope['card_outbox']:
        current = store.db.execute('SELECT text,reply_markup FROM outbox WHERE id=?',
                                   (envelope['card_outbox'],)).fetchone()
        markup = json.loads(current['reply_markup']) if current['reply_markup'] else None
        update_card(store, envelope, current['text'] + '\nI could not delete a message that may hold the value. '
                    'Delete it yourself' + advice, markup)


def _effect_error(store, effect, error):
    final = effect['attempts'] + 1 >= MAX_ATTEMPTS
    gone = effect['kind'] == 'delete' and error.code == 400 and error.reason == 'message_not_found'
    if gone:
        store.db.execute("UPDATE envelope_effects SET state='done' WHERE id=?", (effect['id'],))
    elif effect['kind'] == 'delete' and (error.code == 400 or final):
        store.db.execute("UPDATE envelope_effects SET state='failed',attempts=attempts+1 WHERE id=?", (effect['id'],))
        _delete_failed(store, effect)
    elif final:
        store.db.execute("UPDATE envelope_effects SET state='failed',attempts=attempts+1 WHERE id=?", (effect['id'],))
    else:
        store.effect_failed(effect, error.retry_after)
    logger.info('envelope effect failed effect=%s kind=%s code=%s', effect['id'], effect['kind'], error.code)
    if not gone:
        problems.record(store, 'secrets', 'effect-failed', 'effect=%s kind=%s telegram=%s final=%s' % (
            effect['id'], effect['kind'], error.code, final))


async def run_effects(store, telegram):
    """Run due Telegram side effects of intake in id order. Each outcome commits on its own."""
    from .telegram import TelegramError
    rows = [dict(row) for row in store.db.execute(
        "SELECT * FROM envelope_effects WHERE state='pending' AND next_attempt<=? ORDER BY id", (time.time(),))]
    for effect in rows:
        try:
            if effect['kind'] == 'delete':
                await telegram.call('deleteMessage', chat_id=effect['chat'], message_id=effect['message'])
            else:
                parameters = {'chat_id': effect['chat'], 'text': effect['text']}
                if effect['thread']:
                    parameters['message_thread_id'] = effect['thread']
                await telegram.call('sendMessage', **parameters)
        except TelegramError as error:
            with store.db:
                _effect_error(store, effect, error)
            continue
        with store.db:
            store.db.execute("UPDATE envelope_effects SET state='done',attempts=attempts+1 WHERE id=?", (effect['id'],))
    return bool(rows)


def intercept_topic_reply(store, topic, message):
    """A reply to a live Ask card never becomes an owner message. It may hold the value."""
    reply = (message.get('reply_to_message') or {}).get('message_id')
    if type(reply) is not int or reply == message.get('message_thread_id'):
        return False
    for row in store.db.execute("SELECT * FROM envelopes WHERE topic=? AND state IN ('open','armed')", (topic,)):
        if row['name'] == HANDOFF:
            from .signin import pending
            record = pending(store)
            if record and record.get('envelope') == row['id'] and record.get('state') in ('code', 'checking'):
                continue
        if card_message(store, row['card_outbox']) == reply:
            _effect(store, row['id'], 'delete', message['chat']['id'], message['message_id'])
            store.enqueue_report(topic, 'I removed your reply. ' + (
                'Paste the code as one text message.' if row['name'] == HANDOFF else 'Use Fill privately to send values.'))
            store.envelope_event(row['id'], row['name'], 'reject', reason='topic_reply', source='intake')
            return True
    return False


class SecretsPending(Exception):
    def __init__(self, names):
        self.names = names
        super().__init__('Waiting for secrets: ' + ', '.join(names))


def waiting_secrets(store, task):
    """Declared secrets that are not filled yet but have a live Envelope the owner can still fill."""
    return [name for _, name in (declared(entry) for entry in task['secrets'])
            if not store.db.execute("SELECT 1 FROM envelopes WHERE name=? AND state='filled'", (name,)).fetchone()
            and store.db.execute("SELECT 1 FROM envelopes WHERE name=? AND state IN ('open','armed')",
                                 (name,)).fetchone()]


def load_for_worker(store, vault, task):
    """Read a task's declared secrets before its worker starts. Return ({ENV_NAME: value}, [(envelope, name)])."""
    from .failures import TaskFailure
    from .vault import VaultError
    entries = [declared(entry) for entry in task['secrets']]
    filled = {}
    missing = []
    elsewhere = []
    for _, name in entries:
        row = store.db.execute("SELECT id,task FROM envelopes WHERE name=? AND state='filled'", (name,)).fetchone()
        if not row:
            missing.append(name)
        elif row['task'] is not None and row['task'] != task['id']:
            elsewhere.append(name)
        else:
            filled[name] = row['id']
    waiting = waiting_secrets(store, task)
    unasked = [name for name in missing if name not in waiting]
    if unasked:
        states = [(_latest(store, name) or {}).get('state', 'none') for name in unasked]
        ended = any(state in ('expired', 'cancelled') for state in states)
        problems.record(store, 'secrets', 'wait-ended' if ended else 'not-filled',
                        'names=%s states=%s' % (','.join(unasked), ','.join(states)), topic=task['topic'],
                        task=task['id'])
        raise TaskFailure('Secrets are not filled: %s.' % ', '.join(unasked),
                          'Ask the owner with secret.ask, then spawn again.')
    if elsewhere:
        problems.record(store, 'secrets', 'other-task', 'names=' + ','.join(elsewhere), topic=task['topic'],
                        task=task['id'])
        raise TaskFailure('Secrets were asked for another job: %s.' % ', '.join(elsewhere),
                          'Ask again for this job, or ask without a job.')
    if waiting:
        raise SecretsPending(waiting)
    values = {}
    for environment, name in entries:
        try:
            if vault is None:
                raise VaultError('unavailable')
            values[environment] = vault.get(name)
        except VaultError as error:
            logger.info('vault read failed name=%s code=%s', name, error.code)
            problems.record(store, 'secrets', 'vault-read-failed', 'name=%s code=%s' % (name, error.code),
                            topic=task['topic'], task=task['id'])
            raise TaskFailure('The vault read failed for %s.' % name,
                              'Inspect the service log for the vault error code.') from None
    return values, sorted((envelope, name) for name, envelope in filled.items())


def record_use(store, used, worker):
    """One use row per secret read for a worker. The caller owns the transaction."""
    for envelope, name in used:
        store.envelope_event(envelope, name, 'use', worker=worker, source='worker:%d' % worker)


def _latest(store, name):
    row = store.db.execute('SELECT * FROM envelopes WHERE name=? ORDER BY id DESC LIMIT 1', (name,)).fetchone()
    return dict(row) if row else None


def op_secret_list(store, ctx, name=None):
    names = [row[0] for row in store.db.execute(
        'SELECT DISTINCT name FROM envelopes WHERE (? IS NULL OR name=?) AND name!=? ORDER BY name',
        (name, name, HANDOFF))]
    rows = []
    for each in names:
        latest = _latest(store, each)
        use = store.db.execute("""SELECT created,source FROM envelope_events WHERE name=? AND event='use'
            ORDER BY id DESC LIMIT 1""", (each,)).fetchone()
        rows.append({'name': each, 'state': latest['state'], 'length': latest['length'],
                     'fingerprint': latest['fingerprint'], 'filled_at': latest['filled_at'],
                     'consumer': latest['consumer'], 'task': latest['task'], 'topic': latest['topic'],
                     'last_use': use['created'] if use else None, 'last_use_source': use['source'] if use else None})
    lines = []
    for row in rows:
        line = row['name'] + ': ' + row['state']
        if row['length'] is not None:
            line += ', length %d, fingerprint %s, filled %s' % (row['length'], row['fingerprint'], _clock(row['filled_at']))
        line += ', used by ' + row['consumer'] + (', job id %d' % row['task'] if row['task'] else ', any job')
        if row['last_use']:
            line += ', last use %s by %s' % (_clock(row['last_use']), row['last_use_source'])
        lines.append(line)
    return Result(True, '\n'.join(lines) or 'No secrets were asked for yet.', rows)


def op_secret_rotate(store, ctx, name, topic=None):
    latest = _latest(store, name)
    if not latest:
        raise Refused('No envelope was asked for ' + name + '. Use secret.ask.')
    envelope = ask(store, topic or latest['topic'], name, latest['reason'], latest['consumer'], latest['task'],
                   ctx.source)
    return Result(True, 'Envelope for %s posted again. The stored value stays until the new one is filled.' % name,
                  {'envelope': envelope, 'name': name})


def revoke(store, name, removed):
    """Close every live or filled envelope for a name after the vault delete. The caller owns the transaction."""
    rows = [dict(row) for row in store.db.execute(
        "SELECT * FROM envelopes WHERE name=? AND state IN ('open','armed','filled') ORDER BY id", (name,))]
    for envelope in rows:
        close(store, envelope, 'revoked', 'revoke', source='service:revoke')
    if not rows and not removed:
        latest = _latest(store, name)
        store.envelope_event(latest and latest['id'], name, 'revoke', reason='nothing_stored', source='service:revoke')
        return name + ' had no stored value. Nothing changed.'
    if not rows:
        store.envelope_event(None, name, 'revoke', source='service:revoke')
    topic = _latest(store, name)['topic']
    store.enqueue_report(topic, name + ' revoked. Workers that are already running keep their copy until they exit.')
    return name + ' revoked.'
