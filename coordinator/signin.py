"""Add a Claude or Codex account from Telegram with the native sign-in in a fresh home.

For Claude, the service runs `claude auth login --claudeai` with CLAUDE_CONFIG_DIR set to
a new folder ~/.claude-accounts/.torii-HEX and posts the sign-in link on an Envelope card.
The owner pastes the code in the topic of the card. The store deletes that message and
hands the code to this driver in memory. The driver writes it to the waiting process on
stdin and drops it.
The code never enters the vault, logs, the state database, or replies.

For Codex, the service starts a device-code login through `codex app-server` in a new
home ~/.codex-accounts/.torii-HEX and posts the link and one-time code. The owner enters
the code on the ChatGPT page, so nothing comes back through Telegram. Codex writes the
login into that home only.

Torii keeps the profile key and folder as internal identifiers, and displays the login
email. One sign-in runs at a time.
"""

import asyncio
import logging
import os
from pathlib import Path
import re
import secrets
import signal
import string
import subprocess
import time
from urllib.parse import parse_qs, unquote, urlsplit

from . import codex_accounts, problems
from .account_status import _native, _stop, identity_label
from .accounts import AccountBroker, account_label, authenticated
from .control_api import Refused, Result


KEY = 'account_signin'
RETRY_KEY = 'account_signin_retry'
_ACCOUNT_SETTINGS = {'claude': ('accounts', 'account_status', 'account_blocks'),
                     'codex': ('codex_accounts', 'codex_account_status', 'codex_account_blocks')}
RETRY_DATA = 'signin:retry'
CODEX_RETRY_DATA = 'signin:retry:codex'
CANCEL_SIGNIN_DATA = 'signin:cancel:'
ATTEMPTS = 3
LINK_SECONDS = 60
CODE_SECONDS = 600
EXIT_SECONDS = 60
DEVICE_SECONDS = 16 * 60
_ANSI = re.compile(r'\x1b\[[0-?]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)')
_URL = re.compile(r'https://\S+(?=\s)')
_HOSTS = ('claude.ai', 'claude.com', 'anthropic.com')
_CODE_SHAPE = re.compile(r'[A-Za-z0-9._~-]{8,2048}#[A-Za-z0-9._~-]{8,2048}\Z'
                         r'|(?=[^#]*[0-9])(?=[^#]*[a-z])(?=[^#]*[A-Z])[A-Za-z0-9_-]{48,4096}\Z')
_DEVICE_URL = re.compile(r'https://(?:[a-z0-9-]+\.)*(?:openai|chatgpt)\.com(?:/[A-Za-z0-9._~/%?=&-]*)?\Z')
_USER_CODE = re.compile(r'[A-Z0-9]{3,8}(-[A-Z0-9]{3,8}){0,3}\Z')
_SECRET_PREFIXES = ('sk-ant-', 'sk-', 'ghp_', 'gho_', 'github_pat_', 'xoxb-', 'xoxp-', 'AKIA')
_FORWARDED = ('forward_origin', 'forward_from', 'forward_from_chat', 'forward_sender_name', 'forward_date')
logger = logging.getLogger(__name__)


def accounts_root():
    return Path.home() / '.claude-accounts'


def pending(store):
    record = store.get(KEY)
    return record if isinstance(record, dict) else None


def cancel_data(record):
    """The Cancel button data of one Codex sign-in. A button from an earlier sign-in never matches a later one."""
    return CANCEL_SIGNIN_DATA + Path(record['config_dir']).name


def cancel_matches(store, data):
    record = pending(store)
    return bool(record) and data == cancel_data(record)


def sign_in_link(output):
    for match in _URL.finditer(_ANSI.sub('', output)):
        host = urlsplit(match.group(0)).hostname or ''
        if any(host == name or host.endswith('.' + name) for name in _HOSTS):
            return match.group(0)
    return None


def account_name(email, taken, prefix=''):
    """A free alias from the part of an email before @, or account, with -2, -3, ... on a collision."""
    local = email.split('@', 1)[0] if isinstance(email, str) else ''
    base = prefix + (re.sub(r'[^A-Za-z0-9_-]+', '-', local).strip('-_')[:56 - len(prefix)] or 'account')
    taken = set(taken) | {'default'}
    if base not in taken:
        return base
    return next(base + '-' + str(number) for number in range(2, len(taken) + 3)
                if base + '-' + str(number) not in taken)


def taken_aliases(store):
    """Claude and Codex aliases share one namespace, so an alias names one account."""
    return (set(store.get('accounts', {}) or {}) | set(store.get('codex_accounts', {}) or {})
            | {codex_accounts.DEFAULT_ALIAS})


def register_claude_account(store, directory, identity):
    profiles = dict(store.get('accounts', {}) or {})
    pending_alias = Path(directory).name.lstrip('.')
    pending = profiles.get(pending_alias)
    if (isinstance(pending, dict) and pending.get('awaiting_login')
            and pending.get('config_dir') == str(Path(directory).resolve())):
        profiles.pop(pending_alias)
        snapshots = dict(store.get('account_status', {}) or {})
        snapshots.pop(pending_alias, None)
        store.put('account_status', snapshots)
    alias = account_name(identity.get('email'), taken_aliases(store))
    snapshots = dict(store.get('account_status', {}) or {})
    email = identity_label(identity.get('email'))
    snapshots[alias] = {'identity': {'email': email, 'logged_in': True}}
    profiles[alias] = {'config_dir': str(directory), 'enabled': bool(email),
                       **({} if email else {'awaiting_login': True})}
    store.put('accounts', profiles)
    store.put('account_status', snapshots)
    store.put('account_status_refresh_requested', time.time())
    if store.get('setup_claude_dir') == str(Path(directory).resolve()):
        store.put('setup_claude_dir', None)
    return alias


def provider_name(record):
    return 'Codex' if (record or {}).get('provider') == 'codex' else 'Claude'


def registered_provider(store, alias):
    for provider, (key, _, _) in _ACCOUNT_SETTINGS.items():
        if isinstance((store.get(key, {}) or {}).get(alias), dict):
            return provider
    raise Refused('Account is not registered. Use /accounts to list registered aliases.')


def account_authenticated(store, alias, provider):
    if provider == 'codex':
        return codex_accounts.CodexBroker(store).authenticated(alias)
    return authenticated(store, alias, (store.get('accounts', {}) or {}).get(alias))


def signin_label(store, alias, provider):
    return (codex_accounts.CodexBroker(store).label(alias) if provider == 'codex'
            else account_label(store, alias))


def op_account_add(store, ctx, topic=None, provider=None, alias=None):
    record = pending(store)
    if record:
        raise Refused('A %s sign-in is still open. Finish it or tap Cancel on its card first.' % provider_name(record))
    if alias:
        provider = registered_provider(store, alias)
        if account_authenticated(store, alias, provider):
            raise Refused('%s is already signed in.' % signin_label(store, alias, provider))
    topic = topic or ctx.topic
    if not topic:
        raise Refused('Start Add account from a Telegram channel. Torii posts the sign-in card there.')
    if not store.get('bot_username'):
        raise Refused('The service has not confirmed the bot identity yet. Try again in a minute.')
    codex = provider == 'codex'
    root = codex_accounts.accounts_root() if codex else accounts_root()
    directory = root / ('.torii-' + secrets.token_hex(4))
    store.put(KEY, {'topic': topic, 'config_dir': str(directory), 'state': 'starting', 'attempt': 0,
                    'created': False, 'pid': None, 'envelope': None, 'requested': time.time(), 'target': alias,
                    **({'provider': 'codex'} if codex else {})})
    store.put(RETRY_KEY, None)
    logger.info('account signin requested provider=%s', 'codex' if codex else 'claude')
    return Result(True, 'Starting %s sign-in. The sign-in card follows in a moment.' % ('Codex' if codex else 'Claude'),
                  state='queued')


def op_account_remove(store, ctx, alias):
    provider = registered_provider(store, alias)
    display = signin_label(store, alias, provider)
    if account_authenticated(store, alias, provider):
        raise Refused('%s is signed in. Only signed-out accounts can be removed.' % display)
    if (pending(store) or {}).get('target') == alias:
        raise Refused('A sign-in for this account is open. Finish or cancel it first.')
    key, status_key, blocks_key = _ACCOUNT_SETTINGS[provider]
    profiles = dict(store.get(key, {}) or {})
    profile = profiles.pop(alias)
    directories = [str(Path(directory).resolve()) for directory in store.get('account_dirs_removed', []) or []]
    directories.extend(str(Path(directory).resolve()) for directory in
                       [profile.get('config_dir')] + profile.get('previous_config_dirs', []) if directory)
    store.put('account_dirs_removed', list(dict.fromkeys(directories)))
    store.put(key, profiles)
    for setting in (status_key, blocks_key):
        values = dict(store.get(setting, {}) or {})
        values.pop(alias, None)
        store.put(setting, values)
    if provider == 'codex' and store.get('codex_active_account') == alias:
        store.put('codex_active_account', None)
    logger.info('account removed provider=%s', provider)
    return Result(True, 'Removed %s.' % display)


def op_account_signin_cancel(store, ctx):
    record = pending(store)
    if not record or record.get('state') == 'cancelled':
        raise Refused('No account sign-in is open.')
    store.put(KEY, dict(record, state='cancelled'))
    logger.info('account signin cancel requested')
    return Result(True, 'Stopping the %s sign-in.' % provider_name(record))


CODE_POINTER = 'The Claude sign-in card is waiting for its code. Paste only the code here as one message.'
CHECKING_POINTER = 'Torii is checking the sign-in code with Claude. The sign-in card shows the result.'


def looks_like_code(text):
    return isinstance(text, str) and bool(_CODE_SHAPE.fullmatch(text))


def intercept_topic_code(store, topic, message):
    """Delete code-like values before routing, and hand only this sign-in's own plain text code to Claude."""
    record = pending(store)
    text = message.get('text') or message.get('caption')
    state = (record or {}).get('state')
    if not record or state not in ('code', 'checking') or not isinstance(text, str):
        return False
    from .envelopes import _effect, _row, handed
    envelope = _row(store, record['envelope']) if record.get('envelope') else None
    card = store.db.execute('SELECT topic FROM outbox WHERE id=?',
                            (envelope['card_outbox'],)).fetchone() if envelope and envelope['card_outbox'] else None
    if topic not in (record.get('topic'), card and card['topic']):
        return False
    secret = text.startswith(_SECRET_PREFIXES)
    code = looks_like_code(text)
    link = sign_in_link(envelope['reason'] + '\n') if envelope else None
    states = parse_qs(urlsplit(link).query, keep_blank_values=True).get('state') if link else None
    malformed = (state == 'code' and not code and not secret
                 and ((states and states[0] and states[0] in text)
                      or any(looks_like_code(word)
                             and ('#' not in word or states is None or word.split('#', 1)[1] == states[0])
                             for word in (part.strip(string.punctuation) for part in text.split()))))
    if not code and not secret and not malformed:
        return False
    _effect(store, record.get('envelope'), 'delete', message['chat']['id'], message['message_id'],
            thread=message.get('message_thread_id'))
    forwarded = any(key in message for key in _FORWARDED)
    valid = (code and not secret and not forwarded and 'caption' not in message
             and (states is None or ('#' in text and unquote(text.split('#', 1)[1]) == states[0])))
    signin = getattr(store, 'signin', None)
    if not valid:
        store.enqueue_report(topic, 'I deleted that message. Paste only the code, as one message, with nothing around it.'
                             if malformed else 'I deleted that forwarded message. Paste the code as your own message.'
                             if forwarded else "I deleted that message. It isn't the code for this sign-in. "
                             'Paste the code the current link shows.')
        kind = 'refused_code'
    elif (state == 'code' and envelope and envelope['state'] in ('open', 'armed') and signin is not None
            and signin.hand(envelope['id'], text)):
        handed(store, envelope, len(text), time.time(), 'topic:handoff')
        kind = 'code'
    else:
        store.enqueue_report(topic, 'I deleted your message because it looked like the sign-in code. ' + (
            'Torii is already checking a code with Claude.' if state == 'checking' else
            'This sign-in is not waiting for a code. Check the sign-in card.'))
        kind = 'late_code'
    logger.info('account signin topic message kind=%s', kind)
    return True


def topic_code_pointer(store, topic, message):
    """Offer the sign-in pointer only after normal routing finds no enabled agent."""
    record = pending(store)
    if not record or record.get('state') not in ('code', 'checking'):
        return False
    from .envelopes import _row
    envelope = _row(store, record['envelope']) if record.get('envelope') else None
    card = store.db.execute('SELECT topic FROM outbox WHERE id=?',
                            (envelope['card_outbox'],)).fetchone() if envelope and envelope['card_outbox'] else None
    if topic not in (record.get('topic'), card and card['topic']):
        return False
    store.enqueue_report(topic, CHECKING_POINTER if record['state'] == 'checking' else CODE_POINTER,
                         reply_to=message['message_id'])
    return True


def setup_signin(store, topic):
    record = pending(store)
    if not record:
        return False
    if record.get('state') == 'code' and record.get('envelope'):
        from .envelopes import _row, ask_text, card_message
        envelope = _row(store, record['envelope'])
        store.db.execute('UPDATE outbox SET reply_markup=NULL WHERE id=?', (envelope['card_outbox'],))
        message = card_message(store, envelope['card_outbox'])
        if message:
            store.enqueue_report(envelope['topic'], 'Moved below.', kind='envelope', edit=message)
        else:
            store.db.execute('UPDATE outbox SET delivered=1 WHERE id=?', (envelope['card_outbox'],))
        outbox = store.enqueue_report(topic, ask_text(store, envelope), kind='envelope',
                                     reply_markup={'inline_keyboard': [[{'text': 'Cancel',
                                         'callback_data': 'envelope:%d:cancel' % envelope['id']}]]})
        store.db.execute('UPDATE envelopes SET card_outbox=? WHERE id=?', (outbox, envelope['id']))
    else:
        store.enqueue_report(topic, 'Still checking your Claude code.' if record.get('state') == 'checking' else
                             'Starting sign-in. The card follows in a moment.')
    return True


def _leftover(pid, marker):
    try:
        command = subprocess.run(['ps', '-o', 'command=', '-p', str(pid)], capture_output=True,
                                 text=True, timeout=5).stdout
    except (OSError, subprocess.SubprocessError):
        return False
    return marker in command


def _started(pid):
    """The start time `ps` reports for a process, so a reused PID is not taken for the sign-in's own process."""
    try:
        return subprocess.run(['ps', '-o', 'lstart=', '-p', str(pid)], capture_output=True,
                              text=True, timeout=5).stdout.strip() or None
    except (OSError, subprocess.SubprocessError):
        return None


class _Stop(Exception):
    def __init__(self, text, retry=True, state='cancelled'):
        super().__init__(text)
        self.text = text
        self.retry = retry
        self.state = state


CANCELLED = 'Claude sign-in cancelled. No account was added.'
REJECTED = 'Claude rejected that code. It may be wrong or expired. Try again with the new sign-in card below.'
CODEX_CANCELLED = 'Codex sign-in cancelled. No account was added.'


class SignIns:
    """Drive the one pending sign-in. The service owns this object and its process."""

    def __init__(self, store, binary='claude', attempts=ATTEMPTS, link_seconds=LINK_SECONDS,
                 code_seconds=CODE_SECONDS, exit_seconds=EXIT_SECONDS, poll=0.1, codex_binary='codex',
                 device_seconds=DEVICE_SECONDS):
        self.store = store
        self.binary = binary
        self.codex_binary = codex_binary
        self.device_seconds = device_seconds
        self.attempts = attempts
        self.link_seconds = link_seconds
        self.code_seconds = code_seconds
        self.exit_seconds = exit_seconds
        self.poll = poll
        self.code = None
        self.waiting = None
        store.signin = self

    def hand(self, envelope, value):
        """Take the code for the envelope this driver waits on. Called when the owner pastes it."""
        if self.waiting != envelope or self.code is not None:
            return False
        self.code = value
        return True

    def update(self, **fields):
        with self.store.db:
            record = pending(self.store)
            if record:
                self.store.put(KEY, dict(record, **fields))
                self.store.put('signin_state', fields.get('state', record.get('state')))

    def envelope_state(self, record):
        row = self.store.db.execute('SELECT state FROM envelopes WHERE id=?',
                                    (record.get('envelope'),)).fetchone() if record.get('envelope') else None
        return row['state'] if row else None

    def card(self, record, text, retry=False):
        from .envelopes import _row, update_card
        again = CODEX_RETRY_DATA if record.get('provider') == 'codex' else RETRY_DATA
        markup = {'inline_keyboard': [[{'text': 'Try again', 'callback_data': again}]]} if retry else None
        envelope = _row(self.store, record['envelope']) if record.get('envelope') else None
        if envelope:
            update_card(self.store, envelope, text, markup)
        else:
            self.store.enqueue_report(record['topic'], text, kind='envelope', reply_markup=markup)

    def settle(self, record, state):
        if not record.get('envelope'):
            return
        states = "('open','armed','expired')"
        cursor = self.store.db.execute('UPDATE envelopes SET state=?,updated=? WHERE id=? AND state IN ' + states,
                                       (state, time.time(), record['envelope']))
        if cursor.rowcount:
            self.store.envelope_event(record['envelope'], None, 'cancel' if state == 'cancelled' else 'expire',
                                      source='signin')

    def remove_folder(self, record):
        directory = Path(record['config_dir'])
        codex = record.get('provider') == 'codex'
        root = codex_accounts.accounts_root() if codex else accounts_root()
        if not record.get('created') or directory.parent != root or directory.is_symlink():
            return True
        key = 'codex_accounts' if codex else 'accounts'
        if any(isinstance(profile, dict) and profile.get('config_dir') and
               Path(profile['config_dir']).resolve() == directory.resolve()
               for profile in (self.store.get(key, {}) or {}).values()):
            return False
        if codex:
            codex_accounts.clear_home(directory)
        try:
            directory.rmdir()
        except FileNotFoundError:
            pass
        except OSError:
            logger.info('account signin folder kept because it is not empty')
            return False
        return True

    def finish(self, record, stop):
        self.code = None
        self.waiting = None
        self.remove_folder(record)
        with self.store.db:
            self.settle(record, stop.state)
            self.store.put(KEY, None)
            self.store.put('signin_state', None)
            self.store.put(RETRY_KEY, {'provider': record.get('provider', 'claude'),
                                      'target': record.get('target')} if stop.retry else None)
            self.card(record, stop.text, stop.retry)
            if stop.text not in (CANCELLED, CODEX_CANCELLED):
                problems.record(self.store, 'accounts', 'signin-' + stop.state, stop.text)
        logger.info('account signin ended state=%s', stop.state)

    def recover(self):
        record = pending(self.store)
        if not record or record.get('state') == 'starting':
            return
        pid = record.get('pid')
        codex = record.get('provider') == 'codex'
        if (type(pid) is int and pid > 1 and _leftover(pid, ' app-server' if codex else ' auth login')
                and (not codex or (record.get('started') and record['started'] == _started(pid)))):
            try:
                os.killpg(pid, signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                pass
        if record.get('state') == 'cancelled':
            self.finish(record, _Stop(CODEX_CANCELLED if codex else CANCELLED, retry=False))
        else:
            self.finish(record, _Stop('The %s sign-in stopped because Torii restarted. No account was added.'
                                      % provider_name(record)))

    async def run(self):
        self.recover()
        while True:
            record = pending(self.store)
            if record and record.get('state') == 'starting':
                await self.drive(record)
            elif record and record.get('state') == 'cancelled':
                self.finish(record, _Stop(CODEX_CANCELLED if record.get('provider') == 'codex' else CANCELLED,
                                          retry=False))
            await asyncio.sleep(self.poll)

    def watch(self, process, deadline):
        record = pending(self.store)
        if not record or record.get('state') == 'cancelled' or self.envelope_state(record) == 'cancelled':
            raise _Stop(CANCELLED, retry=False)
        if self.envelope_state(record) == 'expired' or time.time() >= deadline:
            return 'late'
        if process.returncode is not None:
            return 'exited'
        return None

    async def drive(self, record):
        if record.get('provider') == 'codex':
            await self.drive_codex(record)
            return
        directory = Path(record['config_dir'])
        try:
            accounts_root().mkdir(mode=0o700, exist_ok=True)
            directory.mkdir(mode=0o700)
        except OSError:
            self.finish(record, _Stop('Torii could not create a new Claude profile folder. No account was added.'))
            return
        self.update(created=True)
        try:
            for attempt in range(1, self.attempts + 1):
                identity = await self.attempt(directory, attempt)
                if identity is not None:
                    if pending(self.store).get('state') == 'cancelled':
                        raise _Stop(CANCELLED, retry=False)
                    self.succeed(pending(self.store), identity)
                    return
                if attempt < self.attempts:
                    with self.store.db:
                        self.card(pending(self.store), REJECTED)
            raise _Stop('Claude did not accept %d codes. No account was added.' % self.attempts)
        except _Stop as stop:
            self.finish(pending(self.store) or dict(record, created=True), stop)

    async def attempt(self, directory, attempt):
        process = await asyncio.create_subprocess_exec(
            self.binary, 'auth', 'login', '--claudeai', env=AccountBroker.signin_environment(directory),
            cwd=str(directory), stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT, start_new_session=True)
        output = []

        async def pump():
            while True:
                chunk = await process.stdout.read(4096)
                if not chunk:
                    return
                output.append(chunk.decode('utf-8', 'replace'))
                del output[:-8]

        reader = asyncio.ensure_future(pump())
        try:
            self.update(state='link', attempt=attempt, pid=process.pid, envelope=None)
            logger.info('account signin started attempt=%s', attempt)
            deadline = time.time() + self.link_seconds
            link = None
            while link is None:
                if self.watch(process, deadline):
                    raise _Stop('Claude did not show a sign-in link. No account was added.')
                link = sign_in_link(''.join(output))
                await asyncio.sleep(self.poll)
            output.clear()
            self.post_card(link, attempt)
            code = await self.wait_code(process, time.time() + self.code_seconds)
            self.update(state='checking')
            output.clear()
            process.stdin.write(code.encode() + b'\n')
            code = None
            await process.stdin.drain()
            process.stdin.close()
            if not await self.wait_result(process, output):
                return None
            output.clear()
            return await self.identity(directory) if process.returncode == 0 else None
        except (BrokenPipeError, ConnectionResetError):
            return None
        finally:
            output.clear()
            await _stop(process)
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)

    async def wait_result(self, process, output):
        deadline = time.time() + self.exit_seconds
        progress = time.time() + 10
        shown = False
        while process.returncode is None:
            if self.watch(process, deadline) == 'late':
                return False
            text = _ANSI.sub('', ''.join(output)).casefold()
            if any(marker in text for marker in ('error', 'invalid', 'expired', 'try again', 'press enter to retry')):
                return False
            if not shown and time.time() >= progress:
                with self.store.db:
                    self.card(pending(self.store), 'Still checking with Claude…')
                shown = True
            await asyncio.sleep(self.poll)
        return process.returncode == 0

    def post_card(self, link, attempt):
        from .envelopes import HANDOFF, ask
        record = pending(self.store)
        display = account_label(self.store, record['target']) if record.get('target') else None
        reason = (('Sign in %s again' % display if display else 'Add a Claude account') +
                  (' (new link)' if attempt > 1 else '') +
                  '\n1. Tap this link and sign in to %s:\n' % (display or 'the Claude account to add') + link +
                  '\n2. Copy the code it shows.')
        try:
            with self.store.db:
                record = pending(self.store)
                envelope = ask(self.store, record['topic'], HANDOFF, reason, 'claude auth login', source='signin')
                self.store.put(KEY, dict(record, state='code', envelope=envelope))
        except Refused as refusal:
            raise _Stop('Torii could not post the sign-in card. ' + str(refusal) + ' No account was added.') from None
        self.waiting = envelope
        logger.info('account signin card posted attempt=%s', attempt)

    async def wait_code(self, process, deadline):
        while self.code is None:
            state = self.watch(process, deadline)
            if state == 'late':
                raise _Stop('The sign-in link expired. No account was added.', state='expired')
            if state == 'exited':
                raise _Stop('Claude sign-in stopped before the code arrived. No account was added.')
            await asyncio.sleep(self.poll)
        code, self.code, self.waiting = self.code, None, None
        return code

    async def identity(self, directory):
        try:
            status = await _native([self.binary, 'auth', 'status'], AccountBroker.signin_environment(directory), 20)
        except (OSError, ValueError, asyncio.TimeoutError):
            return None
        if not isinstance(status, dict) or status.get('loggedIn') is not True:
            return None
        plan = status.get('subscriptionType')
        return {'email': status.get('email'),
                'type': 'Claude ' + plan.title() if plan in ('max', 'pro', 'team', 'enterprise')
                else 'Claude (plan unknown)'}

    def known(self, email):
        """The registered alias whose last signed-in identity has this email, if any."""
        wanted = identity_label(email).casefold()
        accounts = self.store.get('accounts', {}) or {}
        for alias, snapshot in (self.store.get('account_status', {}) or {}).items():
            identity = snapshot.get('identity') if isinstance(snapshot, dict) else None
            if (wanted and alias in accounts and isinstance(identity, dict)
                    and identity_label(identity.get('email')).casefold() == wanted):
                return alias
        return None

    def target_matches(self, record, email):
        target = record.get('target')
        if not target:
            return False
        _, status_key, _ = _ACCOUNT_SETTINGS[record.get('provider', 'claude')]
        snapshot = (self.store.get(status_key, {}) or {}).get(target) or {}
        previous = identity_label((snapshot.get('identity') or {}).get('email'))
        return not previous or previous.casefold() == email.casefold()

    def repoint(self, record, alias, identity):
        provider = record.get('provider', 'claude')
        key, status_key, _ = _ACCOUNT_SETTINGS[provider]
        email = identity_label(identity.get('email'))
        kind = codex_accounts.plan_label(identity.get('plan')) if provider == 'codex' else identity.get('type')
        with self.store.db:
            profiles = dict(self.store.get(key, {}) or {})
            profile = dict(profiles[alias])
            previous = list(profile.get('previous_config_dirs', []))
            if profile.get('config_dir'):
                previous.append(profile['config_dir'])
            profile.update(config_dir=record['config_dir'], enabled=True,
                           previous_config_dirs=list(dict.fromkeys(str(Path(path).resolve()) for path in previous)))
            profile.pop('awaiting_login', None)
            profiles[alias] = profile
            snapshots = dict(self.store.get(status_key, {}) or {})
            snapshots[alias] = {'identity': {'email': email, 'logged_in': True, **({'type': kind} if kind else {})}}
            self.store.put(key, profiles)
            self.store.put(status_key, snapshots)
            self.store.put(status_key + '_refresh_requested', time.time())
            self.store.put(KEY, None)
            self.store.put('signin_state', None)
            self.card(record, '%s is signed in again. Torii uses it by the same rules as before.' %
                      signin_label(self.store, alias, provider))
        logger.info('account signin repointed provider=%s account=%s', provider, email or alias)

    def succeed(self, record, identity):
        from .setup_flow import account_check
        with self.store.db:
            account_check(self.store)
        email = identity_label(identity.get('email'))
        target = record.get('target')
        target_display = account_label(self.store, target) if target else None
        if self.target_matches(record, email):
            self.repoint(record, target, identity)
            identity.clear()
            return
        alias = self.known(email)
        if alias and not account_authenticated(self.store, alias, 'claude'):
            self.repoint(record, alias, identity)
            identity.clear()
            return
        if alias:
            display = account_label(self.store, alias)
            identity.clear()
            removed = self.remove_folder(record)
            with self.store.db:
                if self.store.get('setup_claude_dir') == str(Path(record['config_dir']).resolve()):
                    self.store.put('setup_claude_dir', None)
                self.store.put(KEY, None)
                self.store.put('signin_state', None)
                self.card(record, ('You signed in as %s, which Torii already has. Nothing changed. '
                                   '%s is still signed out.' % (email, target_display)) if target else
                          'This Claude account is already added as %s. Torii kept %s and added no new '
                                  'account.%s' % (display, display, '' if removed else
                                                  ' The new profile folder holds files, so Torii kept it.'))
            logger.info('account signin matched existing account=%s', display)
            return
        with self.store.db:
            email = identity_label(identity.get('email'))
            alias = register_claude_account(self.store, record['config_dir'], identity)
            identity.clear()
            self.store.put(KEY, None)
            self.store.put('signin_state', None)
            message = ('Claude account %s is signed in and enabled. Torii rotates to it by the same rules as the other accounts.'
                       % email if email else 'Claude login confirmed for profile %s, but its email could not be read. '
                       'Torii left it disabled.' % alias)
            self.card(record, ('You signed in as %s, not %s. Torii added %s as a new account. '
                               '%s is still signed out.' % (email, target_display, email, target_display))
                      if target else message)
        logger.info('account signin added account=%s', email or alias + ' · not signed in')

    async def drive_codex(self, record):
        directory = Path(record['config_dir'])
        try:
            directory.parent.mkdir(mode=0o700, exist_ok=True)
            directory.mkdir(mode=0o700)
        except (OSError, ValueError):
            self.finish(record, _Stop('Torii could not create a new Codex home folder. No account was added.'))
            return
        self.update(created=True)
        try:
            identity = await self.codex_login(directory)
            if (pending(self.store) or {}).get('state') == 'cancelled':
                raise _Stop(CODEX_CANCELLED, retry=False)
            self.succeed_codex(pending(self.store), identity)
        except _Stop as stop:
            self.finish(pending(self.store) or dict(record, created=True), stop)

    async def codex_login(self, directory):
        """Run one device-code login in the new home and return the signed-in identity."""
        server = None
        try:
            try:
                server = codex_accounts.AppServer(self.codex_binary, codex_accounts.codex_environment(
                    codex_accounts.link_home(directory)))
                await asyncio.wait_for(server.start(), self.link_seconds)
                login = await asyncio.wait_for(server.request('account/login/start', {'type': 'chatgptDeviceCode'}),
                                               self.link_seconds)
            except FileNotFoundError:
                raise _Stop('Codex is not installed on this Mac. No account was added.', retry=False) from None
            except (asyncio.TimeoutError, codex_accounts.AppServerError, OSError, ValueError):
                raise _Stop('Codex did not start a device sign-in. No account was added.') from None
            if (pending(self.store) or {}).get('state') == 'cancelled':
                raise _Stop(CODEX_CANCELLED, retry=False)
            self.update(state='approve', pid=server.process.pid, started=_started(server.process.pid))
            self.post_device_card(login)
            params = await self.wait_device(server, login.get('loginId'))
            if params.get('success') is not True:
                reason = identity_label(params.get('error'))[:200]
                raise _Stop('Codex did not finish the sign-in%s. No account was added.' %
                            (': ' + reason if reason else ''))
            try:
                signed_in = await asyncio.wait_for(server.request('account/read'), self.exit_seconds)
            except (asyncio.TimeoutError, codex_accounts.AppServerError, OSError, ValueError):
                raise _Stop('Codex signed in, but Torii could not read the account. No account was added.') from None
            account = signed_in.get('account') if isinstance(signed_in.get('account'), dict) else {}
            if account.get('type') != 'chatgpt':
                raise _Stop('Codex signed in without a ChatGPT account. No account was added.')
            return {'email': account.get('email'), 'plan': account.get('planType')}
        finally:
            if server is not None:
                await server.stop()

    def post_device_card(self, login):
        url, code = login.get('verificationUrl'), login.get('userCode')
        if (not isinstance(url, str) or not _DEVICE_URL.fullmatch(url) or not isinstance(code, str)
                or not _USER_CODE.fullmatch(code)):
            raise _Stop('Codex returned an unexpected sign-in link. No account was added.')
        record = pending(self.store)
        display = codex_accounts.CodexBroker(self.store).label(record['target']) if record.get('target') else None
        text = (('Sign in %s again' % display if display else 'Add a Codex account') +
                '\n1. Open this link and sign in to %s:\n' % ('the ChatGPT account for ' + display if display else
                                                          'the ChatGPT account to add') + url +
                '\n2. Enter this one-time code: ' + code +
                '\nThe code expires in 15 minutes. Torii adds the account when ChatGPT confirms the sign-in.'
                '\nIf ChatGPT says device code sign-in is off, turn on device code authorization for Codex in '
                'ChatGPT Settings → Security, then start again.'
                '\nUse a private browser window if another ChatGPT account is signed in there. Enter only a code '
                'that Torii sent for your own request.')
        markup = {'inline_keyboard': [[{'text': 'Cancel sign-in', 'callback_data': cancel_data(record)}]]}
        with self.store.db:
            self.store.enqueue_report(record['topic'], text, kind='envelope', reply_markup=markup)
        logger.info('account signin device card posted provider=codex')

    async def wait_device(self, server, login_id):
        """The login's completion. A cancel or an expiry cancels the login in Codex first."""
        done = asyncio.ensure_future(server.notification(
            'account/login/completed', lambda params: params.get('loginId') == login_id))
        deadline = time.time() + self.device_seconds
        try:
            while not done.done():
                record = pending(self.store)
                if not record or record.get('state') == 'cancelled':
                    stop = _Stop(CODEX_CANCELLED, retry=False)
                elif time.time() >= deadline:
                    stop = _Stop('The Codex sign-in code expired. No account was added.', state='expired')
                else:
                    await asyncio.sleep(self.poll)
                    continue
                done.cancel()
                await asyncio.gather(done, return_exceptions=True)
                try:
                    await asyncio.wait_for(server.request('account/login/cancel', {'loginId': login_id}), 5)
                except (asyncio.TimeoutError, codex_accounts.AppServerError, OSError, ValueError):
                    pass
                raise stop
            return done.result()
        except (OSError, ValueError):
            raise _Stop('Codex sign-in stopped before ChatGPT confirmed it. No account was added.') from None
        finally:
            if not done.done():
                done.cancel()
                await asyncio.gather(done, return_exceptions=True)

    def known_codex(self, email):
        """The registered Codex alias whose last signed-in identity has this email, if any."""
        wanted = identity_label(email).casefold()
        broker = codex_accounts.CodexBroker(self.store)
        return next((alias for alias in broker.accounts() if wanted and identity_label(
            (broker.snapshot(alias).get('identity') or {}).get('email')).casefold() == wanted), None)

    def succeed_codex(self, record, identity):
        from .setup_flow import account_check
        with self.store.db:
            account_check(self.store)
        email = identity_label(identity.get('email'))
        target = record.get('target')
        target_display = codex_accounts.CodexBroker(self.store).label(target) if target else None
        if self.target_matches(record, email):
            self.repoint(record, target, identity)
            return
        alias = self.known_codex(email)
        if alias and not account_authenticated(self.store, alias, 'codex'):
            self.repoint(record, alias, identity)
            return
        if alias:
            display = codex_accounts.CodexBroker(self.store).label(alias)
            removed = self.remove_folder(record)
            with self.store.db:
                self.store.put(KEY, None)
                self.store.put('signin_state', None)
                self.card(record, ('You signed in as %s, which Torii already has. Nothing changed. '
                                   '%s is still signed out.' % (email, target_display)) if target else
                          'This Codex account is already added as %s. Torii kept %s and added no new '
                                  'account.%s' % (display, display, '' if removed else
                                                  ' The new home folder holds files, so Torii kept it.'))
            logger.info('account signin matched existing codex account=%s', display)
            return
        with self.store.db:
            broker = codex_accounts.CodexBroker(self.store)
            keep = broker.signed_in(broker.active())
            accounts = dict(self.store.get('codex_accounts', {}) or {})
            alias = account_name(email, taken_aliases(self.store), prefix=codex_accounts.DEFAULT_ALIAS + '-')
            snapshots = dict(self.store.get('codex_account_status', {}) or {})
            snapshots[alias] = {'identity': {'email': email, 'type': codex_accounts.plan_label(identity.get('plan')),
                                             'logged_in': True}}
            accounts[alias] = {'config_dir': record['config_dir'], 'enabled': bool(email),
                               **({} if email else {'awaiting_login': True})}
            self.store.put('codex_accounts', accounts)
            self.store.put('codex_account_status', snapshots)
            activate = bool(email) and not keep
            if activate:
                self.store.put('codex_active_account', alias)
            self.store.put('codex_account_status_refresh_requested', time.time())
            self.store.put(KEY, None)
            self.store.put('signin_state', None)
            self.card(record, ('You signed in as %s, not %s. Torii added %s as a new account. '
                               '%s is still signed out.' % (email, target_display, email, target_display)) if target else
                      ('Codex account %s is signed in and enabled.' % email +
                               (' It is now the active Codex account.' if activate else '')) if email else
                      'Codex login confirmed for home %s, but its email could not be read. Torii left it disabled.'
                      % alias)
        logger.info('account signin added codex account=%s', email or alias + ' · not signed in')
