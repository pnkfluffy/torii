"""Envelopes: ask for a credential by name, take it in the private chat, and seal it in the vault.

Private intake mutations use `Store.accept`'s transaction. Control handlers use Store
transactions to serialize asks with rename reservations. Vault calls run outside transactions.

The HANDOFF envelope carries a Claude sign-in code. The owner pastes it in the topic of the
sign-in card, and `signin.intercept_topic_code` hands it to the waiting sign-in in memory
through `store.signin`. It never reaches the vault.
"""

from datetime import datetime, timezone
import hmac
import json
import logging
import os
import re
import sqlite3
import time

from . import problems
from .control_api import Refused, Result

logger = logging.getLogger(__name__)

LIFETIME = 86400
CODE_LIFETIME = 600
TOLD_WINDOW = 600
HANDOFF = 'CLAUDE_SIGNIN_CODE'
FILL_START = 'fill'
LIVE = ('open', 'armed')
NAME = re.compile(r'[A-Z][A-Z0-9_]{1,63}\Z')
GENERIC_SECRET_WORDS = frozenset(
    'ACCESS API AUTH BEARER CLIENT CREDENTIAL CREDENTIALS DEV DEVELOPMENT ID KEY PASSWORD PRIVATE PROD '
    'PRODUCTION PUBLIC REFRESH SANDBOX SECRET STAGING TEST TOKEN'.split())
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


def _row(store, envelope_id):
    row = store.db.execute('SELECT * FROM envelopes WHERE id=?', (envelope_id,)).fetchone()
    return dict(row) if row else None


def _task_line(store, envelope):
    task = store.task_get(envelope['task']) if envelope['task'] else None
    return 'Job: ' + ('%d %s' % (task['number'], task['title'][:200]) if task else 'any job')


def _details(store, envelope, history=None):
    return ('Envelope: ' + envelope['name'] + '\nWhy: ' + envelope['reason'][:200] +
            ('\n' + history if history else '') +
            '\nUsed by: ' + envelope['consumer'][:200] + '\n' + _task_line(store, envelope))


def ask_text(store, envelope):
    if envelope['name'] == HANDOFF:
        return envelope['reason'] + '\n3. Paste it here as one message within 10 minutes. I delete it at once.'
    if envelope['state'] == 'armed':
        instruction = ('\nThe prompt is in your private chat with me. Reply to it there with the value within '
                       '24 hours, or tap Fill privately to open the chat.')
    else:
        instruction = '\nTap Fill privately within 24 hours and send the value there in one message.'
    filled, expired = store.db.execute("""SELECT MAX(filled_at IS NOT NULL OR state='filled'), MAX(state='expired')
        FROM envelopes WHERE name=? AND id<?""", (envelope['name'], envelope['id'])).fetchone()
    history = 'Torii has never stored a value under this name.'
    if filled:
        history = 'Torii stored a value under this name earlier.'
    elif expired:
        history = 'An earlier request for this name expired without a value.'
    return _details(store, envelope, history) + instruction + '\nNever paste it in this channel.'


def _private_chat_button(store):
    return [{'text': 'Fill privately', 'url': 'https://t.me/' + store.get('bot_username')}]


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


CLOSED_TEXT = {'cancelled': 'Cancelled.', 'expired': 'Expired. Tap Ask again to request it again.',
               'revoked': 'Revoked. The stored value was removed.'}
SIGNIN_CLOSED = {'cancelled': 'Claude sign-in cancelled. No account was added.',
                 'expired': 'The sign-in link expired. No account was added.',
                 'revoked': 'Claude sign-in stopped. No account was added.'}


def close(store, envelope, state, event, source=None, reason=None):
    """The single path that closes an envelope: live to cancelled or expired, live or filled to revoked."""
    armed = envelope['state'] == 'armed'
    updated = time.time()
    store.db.execute('UPDATE envelopes SET state=?,updated=? WHERE id=?', (state, updated, envelope['id']))
    envelope['state'] = state
    store.envelope_event(envelope['id'], envelope['name'], event, reason=reason, source=source)
    if envelope['name'] == HANDOFF:
        from .signin import RETRY_DATA
        retry = {'inline_keyboard': [[{'text': 'Try again', 'callback_data': RETRY_DATA}]]}
        update_card(store, envelope, SIGNIN_CLOSED[state], retry if state == 'expired' else None)
        return
    retry = {'inline_keyboard': [[{'text': 'Ask again', 'callback_data': 'envelope:%d:again' % envelope['id']}]]}
    update_card(store, envelope, 'Envelope: ' + envelope['name'] + '\n' + CLOSED_TEXT[state],
                retry if state == 'expired' else None)
    if armed and not (state == 'cancelled' and source in ('intake', 'button')):
        _reply(store, envelope['id'], envelope['chat'], "%s %s, so I won't take a value for it." % (envelope['name'], state))


def sweep(store, now=None):
    now = time.time() if now is None else now
    rows = [dict(row) for row in store.db.execute(
        "SELECT * FROM envelopes WHERE state IN ('open','armed') AND expires<=? ORDER BY id", (now,))]
    for envelope in rows:
        close(store, envelope, 'expired', 'expire', source='sweep')
    _prompt_open(store, now)
    return len(rows)


def cancel(store, envelope_id, source):
    envelope = _row(store, envelope_id)
    if not envelope or envelope['state'] not in LIVE:
        return 'envelope_closed'
    close(store, envelope, 'cancelled', 'cancel', source=source)
    return 'envelope_cancel'


def fill_private(store, envelope_id):
    sweep(store)
    envelope = _row(store, envelope_id)
    if envelope and envelope['name'] != HANDOFF and envelope['state'] == 'expired':
        return 'envelope_expired'
    if not envelope or envelope['name'] == HANDOFF or envelope['state'] != 'armed':
        return 'envelope_closed'
    return 'envelope_fill'


def ask(store, topic, name, reason, consumer, task=None, source='cli', edit=None):
    if rename_reserved(store, name):
        raise Refused('A saved key rename is unfinished for this name. Run the same rename again first.')
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
    now = time.time()
    try:
        cursor = store.db.execute('''INSERT INTO envelopes
            (name,reason,consumer,task,topic,state,created,expires,updated)
            VALUES (?,?,?,?,?,'open',?,?,?)''',
                                  (name, reason, consumer, task, topic, now,
                                   now + (CODE_LIFETIME if name == HANDOFF else LIFETIME), now))
    except sqlite3.IntegrityError as error:
        raise Refused('An envelope for ' + name + ' is already open.') from error
    envelope = _row(store, cursor.lastrowid)
    store.envelope_event(envelope['id'], name, 'ask', source=source)
    if name != HANDOFF:
        _prompt_open(store, now)
        envelope = _row(store, envelope['id'])
    keyboard = [_cancel_button(envelope)]
    if name != HANDOFF:
        fill = (_private_chat_button(store) if envelope['state'] == 'armed' else
                [{'text': 'Fill privately', 'callback_data': 'envelope:%d:fill' % envelope['id']}])
        keyboard.insert(0, fill)
    outbox = store.enqueue_report(topic, ask_text(store, envelope), kind='envelope',
                                  reply_markup={'inline_keyboard': keyboard}, edit=edit)
    store.db.execute('UPDATE envelopes SET card_outbox=? WHERE id=?', (outbox, envelope['id']))
    return envelope['id']


def again(store, envelope_id):
    envelope = _row(store, envelope_id)
    if not envelope or envelope['state'] != 'expired' or envelope['name'] == HANDOFF:
        return 'envelope_closed'
    text = 'Envelope: ' + envelope['name'] + '\nExpired. Asked again.'
    if (_latest(store, envelope['name'])['id'] != envelope_id or
            store.db.execute("SELECT 1 FROM envelopes WHERE name=? AND state IN ('open','armed')",
                             (envelope['name'],)).fetchone()):
        current = store.db.execute('SELECT text,reply_markup FROM outbox WHERE id=?', (envelope['card_outbox'],)).fetchone()
        if current['text'] != text or current['reply_markup']:
            update_card(store, envelope, text)
        return 'envelope_closed'
    store.db.execute('SAVEPOINT envelope_again')
    try:
        ask(store, envelope['topic'], envelope['name'], envelope['reason'], envelope['consumer'],
            envelope['task'], source='button')
    except Refused as refusal:
        store.db.execute('ROLLBACK TO envelope_again')
        return Result(False, str(refusal), state='refused')
    finally:
        store.db.execute('RELEASE envelope_again')
    update_card(store, envelope, text)
    return 'envelope_again'


def _stored_refusal(store, ctx, name):
    """Refuse an ask for a stored name. Tell a worker's coordinator how to declare it. Never touch the value."""
    text = (name + ' is stored. Do not ask the owner again. The coordinator declares it on the job with '
            'tasks.update task=N secrets=' + name + ' (or ENV_NAME=' + name + '), and the next worker run gets it '
            'in the environment. Use secret.rotate only if the stored value is wrong. secret.list shows stored names.')
    worker = ctx.source_worker and store.db.execute(
        'SELECT w.id,t.id AS task,t.number,t.topic,t.secrets FROM workers w JOIN tasks t ON t.id=w.task WHERE w.id=?',
        (ctx.source_worker,)).fetchone()
    if not worker:
        return Result(False, text, {'name': name, 'stored': True}, state='refused')
    head = 'Job %d: worker ' % worker['number']
    tail = ' asked the owner for %s, which is already stored. ' % name
    if store.db.execute("SELECT 1 FROM messages WHERE kind='event' AND topic=? AND created>? AND text GLOB ?",
                        (worker['topic'], time.time() - TOLD_WINDOW, head + '*' + tail + '*')).fetchone():
        return Result(False, text + " This job's coordinator was already told.", {'name': name, 'stored': True},
                      state='refused')
    entries = parse_declarations(json.loads(worker['secrets'])) if worker['secrets'] else []
    if any(declared(entry)[1] == name for entry in entries):
        step = ('%s is already declared on the job. Worker %d started before that; steer or respawn it to load it.'
                % (name, worker['id']))
    else:
        try:
            step = 'Declare it: tasks.update task=%d secrets=%s' % (
                worker['task'], ','.join(parse_declarations(entries + [name])))
        except ValueError:
            step = ('The job maps environment name %s to another vault name (secrets=%s). Choose which value the '
                    'worker needs, then declare it with a free environment name, for example %s_STORED=%s.'
                    % (name, ','.join(entries), name, name))
    store.message_event(worker['topic'], head + str(worker['id']) + tail + 'I refused the ask and posted no card. '
                        + step, source_worker=worker['id'])
    return Result(False, text + " I told this job's coordinator.", {'name': name, 'stored': True}, state='refused')


def _secret_words(name):
    return set(re.findall(r'CLIENT_ID(?=_|$)|[^_]+', name)) - GENERIC_SECRET_WORDS


def op_secret_ask(store, ctx, topic, name, reason, consumer, task=None, confirm=False):
    with store.transaction():
        if store.db.execute("SELECT 1 FROM envelopes WHERE name=? AND state='filled'", (name,)).fetchone():
            return _stored_refusal(store, ctx, name)
        words = _secret_words(name)
        if words and not confirm:
            matches = [row['name'] for row in store.db.execute(
                "SELECT DISTINCT name FROM envelopes WHERE state='filled' AND name!=? ORDER BY name", (HANDOFF,))
                if words & _secret_words(row['name'])]
            if matches:
                return Result(False, 'Stored names that may match: ' + ', '.join(matches) +
                              '. Map one on the job with tasks.update secrets=ENV_NAME=VAULT_NAME. '
                              'If you need a different value, ask again with confirm=true.',
                              {'name': name, 'matches': matches}, state='refused')
        envelope = ask(store, topic, name, reason, consumer, task, ctx.source)
    return Result(True, 'Envelope for %s posted. The owner fills it privately.' % name,
                  {'envelope': envelope, 'name': name})



REPLIES = {
    'bare_start': 'No credential asks are waiting. When a job asks for one, I post its prompt here.',
    'nothing_to_cancel': 'No credential ask is waiting for a value.',
    'no_armed': 'No credential ask is waiting for a value, so I deleted your message.',
    'closed_prompt': 'That ask is no longer waiting for a value, so I deleted your message and stored nothing.',
    'not_text': 'Send the value as one text message. I deleted your message.',
    'too_long': 'The value must have at most 4096 characters. I deleted your message. Send the value again.',
    'multi_line': 'Send the value as one line. I deleted your message. Send the value again.',
    'not_printable': 'The value has characters that cannot be printed. I deleted your message. Send the value again.',
    'command': 'The value cannot start with /. I deleted your message. Send the value again.',
    'vault_failed': 'The vault write failed. Your message was deleted. Send the value again.',
    'internal': 'Something failed while I handled your message. I deleted it. Send the value again.',
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
    if len(text) > 4096:
        return 'too_long'
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


def _waiting(store, chat, now):
    """The asks that take a value in this private chat, oldest first."""
    return [dict(row) for row in store.db.execute("""SELECT * FROM envelopes
        WHERE state='armed' AND chat=? AND expires>? AND name!=? ORDER BY id""", (chat, now, HANDOFF))]


PROMPT_NAME = re.compile(r'(?:Envelope: |Send the value for )([A-Z][A-Z0-9_]{1,63})(?:\n| now in one message)')


def _replied(store, message, now):
    """The ask that a reply to a bot message names, or None for a plain message.

    A reply to any message that is not the owner's own is never plain. A recorded prompt names its ask. A copy
    that was never recorded (a retried send, or a prompt from before the upgrade) names it by its first line.
    """
    reply = message.get('reply_to_message')
    if not isinstance(reply, dict):
        return None
    sender = reply.get('from')
    if isinstance(sender, dict) and sender.get('id') == message['from']['id']:
        return None
    chat = message['chat']['id']
    row = store.db.execute('SELECT * FROM envelopes WHERE chat=? AND prompt_message=? ORDER BY id DESC LIMIT 1',
                           (chat, reply.get('message_id'))).fetchone() if type(reply.get('message_id')) is int else None
    if row is None:
        text = reply.get('text')
        match = PROMPT_NAME.match(text) if isinstance(text, str) else None
        row = match and store.db.execute("""SELECT * FROM envelopes WHERE chat=? AND name=? AND name!=?
            AND state='armed' AND expires>? ORDER BY id DESC LIMIT 1""", (chat, match.group(1), HANDOFF, now)).fetchone()
        if not row:
            return {'live': False, 'problem': 'unknown_reply'}
    return dict(row, live=row['state'] == 'armed' and row['expires'] > now and row['name'] != HANDOFF,
                problem='closed_prompt')


def _target(store, message, now):
    """The ask a value message fills: the prompt it replies to, else the oldest prompt sent before it.

    A plain message never fills a newer ask when an older prompted ask was still open when it was sent.
    """
    replied = _replied(store, message, now)
    if replied is not None:
        if replied['live']:
            return replied, None
        return (replied if 'id' in replied else None), replied['problem']
    chat = message['chat']['id']
    waiting = _waiting(store, chat, now)
    if not waiting:
        return None, 'no_armed'
    date = message.get('date')
    if type(date) is not int:
        return waiting[0], 'before_prompt'
    target = next((row for row in waiting if type(row['prompt_date']) is int
                   and 0 < row['prompt_date'] < date), None)
    if target is None:
        return waiting[0], 'before_prompt'
    older = store.db.execute("""SELECT * FROM envelopes WHERE chat=? AND name!=? AND id<? AND prompt_date<?
        AND CASE WHEN state='armed' THEN expires WHEN state IN ('expired','cancelled','revoked') THEN MIN(updated,expires)
            WHEN state IN ('filled','superseded') THEN filled_at END>=? ORDER BY id LIMIT 1""",
                             (chat, HANDOFF, target['id'], date, date)).fetchone()
    return (dict(older), 'sent_while_open') if older else (target, None)


def prepare_private(store, message):
    """Check a private message, pick the ask it fills, and write the vault."""
    now = time.time()
    authorized = False
    try:
        refusal = _refusal(store, message)
        if refusal:
            return {'ignored': refusal}
        authorized = True
        if _command(message) in ('/start', '/cancel'):
            return {'now': now, 'command': _command(message)}
        target, problem = _target(store, message, now)
        intake = {'now': now} if target is None else {'now': now, 'envelope': target['id'], 'name': target['name']}
        problem = problem or value_problem(message)
        if problem:
            return dict(intake, problem=problem)
        if store.vault is None:
            return dict(intake, problem='vault_failed:unavailable')
        from .vault import VaultError
        try:
            return dict(intake, stored=store.vault.put(target['name'], message['text']))
        except VaultError as error:
            problems.record(store, 'secrets', 'vault-write-failed', 'name=%s code=%s' % (target['name'], error.code))
            return dict(intake, problem='vault_failed:' + error.code)
    except Exception as error:
        problems.record(store, 'secrets', 'intake-failed', type(error).__name__, level=logging.ERROR, every=3600)
        return {'now': now, 'problem': 'internal'} if authorized else {'ignored': 'internal'}


def _effect(store, envelope, kind, chat, message=None, text=None, thread=None):
    store.db.execute('''INSERT INTO envelope_effects(envelope,kind,chat,message,text,created,thread)
        VALUES (?,?,?,?,?,?,?)''', (envelope, kind, chat, message, text, time.time(), thread))


def _reply(store, envelope, chat, text, thread=None):
    _effect(store, envelope, 'reply', chat, text=text, thread=thread)


def _value_prompt(store, envelope):
    return (_details(store, envelope) + '\nReply to this message with the value in one message within 24 hours. '
            'I delete it after I store it. Reply /cancel to stop.')


def _prompt(store, envelope, chat):
    """Send this ask's own prompt to the private chat. Its delivery date gates plain value messages."""
    _reply(store, envelope['id'], chat, _value_prompt(store, envelope))
    effect = store.db.execute('SELECT last_insert_rowid()').fetchone()[0]
    store.db.execute('UPDATE envelopes SET prompt_effect=?,prompt_message=NULL,prompt_date=NULL WHERE id=?',
                     (effect, envelope['id']))


def _prompt_pending(store, envelope):
    return envelope['prompt_effect'] is not None and store.db.execute(
        "SELECT 1 FROM envelope_effects WHERE id=? AND state='pending'", (envelope['prompt_effect'],)).fetchone()


def _prompt_open(store, now):
    """Give every live ask its own prompt in the owner's private chat."""
    chat = store.get('owner')
    if type(chat) is not int:
        return
    for row in store.db.execute("""SELECT * FROM envelopes WHERE name!=? AND expires>?
            AND (state='open' OR (state='armed' AND chat=? AND prompt_effect IS NULL)) ORDER BY id""",
                                (HANDOFF, now, chat)).fetchall():
        envelope = dict(row)
        if envelope['state'] == 'open':
            _arm(store, envelope, chat, now)
        else:
            _prompt(store, envelope, chat)


def _arm(store, envelope, chat, now):
    expires = now + LIFETIME
    store.db.execute("UPDATE envelopes SET state='armed',chat=?,armed_at=?,expires=?,updated=? WHERE id=?",
                     (chat, now, expires, now, envelope['id']))
    store.envelope_event(envelope['id'], envelope['name'], 'arm', source='direct_prompt')
    envelope.update(state='armed', chat=chat, expires=expires)
    _prompt(store, envelope, chat)
    if envelope['card_outbox']:
        update_card(store, envelope, ask_text(store, envelope),
                    {'inline_keyboard': [_private_chat_button(store), _cancel_button(envelope)]})


def _start(store, message, chat, now):
    """Send again each prompt that never arrived. Otherwise list the asks that wait."""
    if message['text'] == '/start ' + FILL_START:
        _effect(store, None, 'delete', chat, message['message_id'])
    elif len(message['text'].split()) > 1:
        _reply(store, None, chat, _replies(store)['bare_start'])
        return 'envelope_start'
    waiting = _waiting(store, chat, now)
    if not waiting:
        _reply(store, None, chat, _replies(store)['bare_start'])
        return 'envelope_start'
    missing = [row for row in waiting if row['prompt_message'] is None and not _prompt_pending(store, row)]
    for row in missing:
        _prompt(store, row, chat)
    if not missing:
        _reply(store, None, chat, 'Waiting for ' + ', '.join(row['name'] for row in waiting) +
               '. Reply to its prompt above with the value.')
    return 'envelope_arm'


def _cancel_private(store, message, chat, now):
    replied = _replied(store, message, now)
    if replied is not None:
        row = replied if replied['live'] else None
    else:
        waiting = _waiting(store, chat, now)
        row = waiting[0] if waiting else None
    if not row:
        _reply(store, None, chat, _replies(store)['nothing_to_cancel'])
        return 'envelope_reject'
    _reply(store, row['id'], chat, 'Cancelled ' + row['name'] + '.')
    row.pop('live', None)
    close(store, row, 'cancelled', 'cancel', source='intake')
    return 'envelope_cancel'


def _fill(store, intake, chat):
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
    waiting = _waiting(store, chat, now)
    rest = ' Still waiting: %s.' % ', '.join(row['name'] for row in waiting) if waiting else ''
    _reply(store, envelope['id'], chat, '%s stored. %s.%s' % (envelope['name'], facts, rest))
    update_card(store, envelope, 'Envelope: %s\nFilled. %s, at %s.' % (envelope['name'], facts, _clock(now)))
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
    sweep(store, intake['now'])
    command = intake.get('command')
    if command == '/start':
        return _start(store, message, chat, intake['now'])
    if command == '/cancel':
        return _cancel_private(store, message, chat, intake['now'])
    _effect(store, intake.get('envelope'), 'delete', chat, message['message_id'])
    if 'stored' in intake:
        return _fill(store, intake, chat)
    problem = intake['problem']
    store.envelope_event(intake.get('envelope'), intake.get('name'), 'reject', reason=problem, source='intake')
    reason = problem.split(':', 1)[0]
    if reason == 'before_prompt':
        reply = ('That message was sent before I asked for %s, so I deleted it and stored nothing. '
                 'Reply to its prompt with the value.') % intake['name']
    elif reason == 'sent_while_open':
        reply = ('That message was sent while I was still waiting for %s. I deleted it and stored nothing. '
                 'Reply to the prompt you mean.') % intake['name']
    elif reason == 'unknown_reply':
        waiting = _waiting(store, chat, intake['now'])
        reply = ('That reply does not match a waiting credential ask, so I deleted it and stored nothing. ' +
                 ('Waiting for %s. Reply to its prompt.' % ', '.join(row['name'] for row in waiting)
                  if waiting else 'No credential ask is waiting.'))
    else:
        reply = (_replies(store) if reason == 'no_armed' else REPLIES)[reason]
    _reply(store, intake.get('envelope'), chat, reply)
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
        problems.record(store, 'secrets', 'intake-apply-failed', type(error).__name__, level=logging.ERROR, every=3600)
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
        envelope = _row(store, effect['envelope']) if effect['kind'] == 'reply' and effect['envelope'] else None
        prompt = envelope is not None and envelope['prompt_effect'] == effect['id']
        if prompt and envelope['state'] != 'armed':
            with store.db:
                store.db.execute("UPDATE envelope_effects SET state='done' WHERE id=?", (effect['id'],))
            continue
        try:
            if effect['kind'] == 'delete':
                await telegram.call('deleteMessage', chat_id=effect['chat'], message_id=effect['message'])
            else:
                parameters = {'chat_id': effect['chat'], 'text': effect['text']}
                if effect['thread']:
                    parameters['message_thread_id'] = effect['thread']
                sent = await telegram.call('sendMessage', **parameters)
        except TelegramError as error:
            with store.db:
                _effect_error(store, effect, error)
            continue
        with store.db:
            store.db.execute("UPDATE envelope_effects SET state='done',attempts=attempts+1 WHERE id=?", (effect['id'],))
            sent = sent if prompt and isinstance(sent, dict) else {}
            if type(sent.get('message_id')) is int and type(sent.get('date')) is int and sent['date'] > 0:
                store.db.execute("""UPDATE envelopes SET prompt_message=?,prompt_date=?
                    WHERE id=? AND state='armed' AND prompt_effect=?""",
                                 (sent['message_id'], sent['date'], envelope['id'], effect['id']))
    return bool(rows)


def intercept_topic_reply(store, topic, message):
    """A reply to a live Ask card never becomes an owner message. It may hold the value."""
    reply = (message.get('reply_to_message') or {}).get('message_id')
    if type(reply) is not int or reply == message.get('message_thread_id'):
        return False
    for row in store.db.execute("SELECT * FROM envelopes WHERE topic=? AND state IN ('open','armed')",
                                (topic,)).fetchall():
        if row['name'] == HANDOFF:
            from .signin import pending
            record = pending(store)
            if record and record.get('envelope') == row['id'] and record.get('state') in ('code', 'checking'):
                continue
        if card_message(store, row['card_outbox']) == reply:
            _effect(store, row['id'], 'delete', message['chat']['id'], message['message_id'])
            store.enqueue_report(topic, 'I removed your reply. ' + (
                'Paste the code as one text message.' if row['name'] == HANDOFF else
                'Reply to its prompt in your private chat with me, or tap Fill privately.'))
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
    for _, name in entries:
        row = store.db.execute("SELECT id FROM envelopes WHERE name=? AND state='filled'", (name,)).fetchone()
        if not row:
            missing.append(name)
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
    for envelope, _ in used:
        store.envelope_event(envelope, _row(store, envelope)['name'], 'use', worker=worker, source='worker:%d' % worker)


def _latest(store, name):
    row = store.db.execute('SELECT * FROM envelopes WHERE name=? ORDER BY id DESC LIMIT 1', (name,)).fetchone()
    return dict(row) if row else None


def used_jobs_text(jobs):
    if not jobs:
        return 'no jobs yet'
    return ', '.join('%s job %d' % (' '.join(job['channel'].split())[:32], job['number']) for job in jobs[:3]) + (
        ' (+%d more)' % (len(jobs) - 3) if len(jobs) > 3 else '')


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
        rows[-1]['jobs'] = [dict(row) for row in store.db.execute("""SELECT t.id,t.topic,c.name AS channel,t.number
            FROM envelope_events e JOIN workers w ON w.id=e.worker JOIN tasks t ON t.id=w.task
            JOIN topics c ON c.id=t.topic WHERE e.name=? AND e.event='use'
            GROUP BY t.id ORDER BY MAX(e.created) DESC,MAX(e.id) DESC""", (each,))]
    lines = []
    for row in rows:
        line = row['name'] + ': ' + row['state']
        if row['length'] is not None:
            line += ', length %d, fingerprint %s, filled %s' % (row['length'], row['fingerprint'], _clock(row['filled_at']))
        line += ', used by ' + row['consumer'] + (', job id %d' % row['task'] if row['task'] else ', any job')
        if row['last_use']:
            line += ', last use %s by %s' % (_clock(row['last_use']), row['last_use_source'])
        line += ', ' + used_jobs_text(row['jobs'])
        lines.append(line)
    return Result(True, '\n'.join(lines) or 'No secrets were asked for yet.', rows)


def op_secret_rotate(store, ctx, name, topic=None):
    with store.transaction():
        latest = _latest(store, name)
        if not latest:
            raise Refused('No envelope was asked for ' + name + '. Use secret.ask.')
        envelope = ask(store, topic or latest['topic'], name, latest['reason'], latest['consumer'], latest['task'],
                       ctx.source)
    return Result(True, 'Envelope for %s posted again. The stored value stays until the new one is filled.' % name,
                  {'envelope': envelope, 'name': name})


RENAME_KEY = 'secret_rename_v1'


def rename_reserved(store, name):
    pending = store.get(RENAME_KEY)
    return pending is not None and name in (pending['name'], pending['new_name'])


def _rename_source(store, name, new_name):
    if not valid_name(name) or not valid_name(new_name):
        raise Refused('Both names must be valid secret names.')
    if name == new_name:
        raise Refused('Choose two different secret names.')
    if new_name in os.environ:
        raise Refused(new_name + ' is set in the service environment. Choose another name.')
    source = store.db.execute("SELECT * FROM envelopes WHERE name=? AND state='filled' ORDER BY id DESC LIMIT 1",
                              (name,)).fetchone()
    if source is None:
        raise Refused(name + ' has no filled envelope.')
    if store.db.execute("SELECT 1 FROM envelopes WHERE name=? AND state IN ('open','armed')", (name,)).fetchone():
        raise Refused(name + ' has an open or armed envelope. Close it before renaming.')
    if store.db.execute('SELECT 1 FROM envelopes WHERE name=?', (new_name,)).fetchone():
        raise Refused(new_name + ' already has an envelope. Choose another name.')
    return source


def rename(store, vault, name, new_name):
    """Copy and verify, commit all name references, then remove the source. Keep a journal until settlement."""
    from .vault import VaultError
    pending = store.get(RENAME_KEY)
    if pending is not None and (pending['name'], pending['new_name']) != (name, new_name):
        raise Refused('Another saved key rename is unfinished. Run that rename again first.')
    if vault is None:
        raise VaultError('unavailable')
    if pending is None:
        source = _rename_source(store, name, new_name)
        if new_name in vault.names():
            raise Refused(new_name + ' already exists in the vault. Choose another name.')
        try:
            value = vault.get(name)
        except VaultError as error:
            if error.code == 'missing':
                raise Refused(name + ' has no stored value in the vault.') from None
            raise
        if not hmac.compare_digest(vault.fingerprint(value), source['fingerprint']):
            raise Refused(name + ' does not match its filled envelope fingerprint.')
        pending = {'name': name, 'new_name': new_name, 'fingerprint': source['fingerprint'],
                   'phase': 'prepared', 'jobs': 0}
        with store.transaction():
            _rename_source(store, name, new_name)
            store.put(RENAME_KEY, pending)
    expected = pending['fingerprint']
    if pending['phase'] == 'prepared':
        value = vault.get(name)
        if not hmac.compare_digest(vault.fingerprint(value), expected):
            raise VaultError('readback_mismatch')
        if new_name not in vault.names() or not hmac.compare_digest(vault.fingerprint(vault.get(new_name)), expected):
            vault.put(new_name, value)
        if not hmac.compare_digest(vault.fingerprint(vault.get(new_name)), expected):
            raise VaultError('readback_mismatch')
        with store.transaction():
            store.db.execute('UPDATE envelopes SET name=? WHERE name=?', (new_name, name))
            store.db.execute('UPDATE envelope_events SET name=? WHERE name=?', (new_name, name))
            jobs = 0
            for task in store.db.execute('SELECT id,secrets FROM tasks WHERE secrets IS NOT NULL').fetchall():
                entries = parse_declarations(json.loads(task['secrets']))
                if not any(declared(entry)[1] == name for entry in entries):
                    continue
                rewritten = parse_declarations([environment + '=' + (new_name if stored == name else stored)
                                                for environment, stored in map(declared, entries)])
                store.db.execute('UPDATE tasks SET secrets=? WHERE id=?', (json.dumps(rewritten), task['id']))
                jobs += 1
            pending = dict(pending, phase='moved', jobs=jobs)
            store.put(RENAME_KEY, pending)
    if not hmac.compare_digest(vault.fingerprint(vault.get(new_name)), expected):
        raise VaultError('readback_mismatch')
    vault.delete(name)
    return '%s renamed to %s. Declarations changed for %d jobs.' % (name, new_name, pending['jobs'])


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
