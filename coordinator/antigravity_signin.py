"""Add an Antigravity account from Telegram through the official agy ACP server.

Torii starts `agy acp` in the account's own profile home and asks it to authenticate
with the Google account method. agy opens a loopback listener and hands the Google
link to a browser stub, which saves it. The owner signs in on any device. Google then
returns to `http://localhost:<port>/auth/callback`, which only this host can load, so
the owner pastes that address in the topic. Torii deletes the message, checks that it
belongs to this sign-in and loads it once on loopback. agy keeps the login in its own
profile. Torii never reads it. The pasted address stays in memory only.
"""

import asyncio
import json
import logging
from pathlib import Path
import re
import secrets
import time
from urllib.parse import parse_qs, urlsplit

from . import antigravity_accounts
from .acp_protocol import INITIALIZE
from .control_api import Refused, Result


METHOD = 'oauth-personal'
CANCELLED = 'Antigravity sign-in cancelled. No account was added.'
WAITING = 'antigravity'
FORWARD_SECONDS = 10
_CALLBACK = re.compile(r'https?://(?:localhost|127\.0\.0\.1)(?::[0-9]{1,5})?/\S*')
logger = logging.getLogger(__name__)


def ready(store):
    if store.get('antigravity_enabled', False) is not True:
        raise Refused('Antigravity workers are off.')
    return antigravity_accounts.check_binary(antigravity_accounts.binary(store))


def free_alias(taken):
    base = 'antigravity'
    return next(name for name in ([base] + [base + '-%d' % number for number in range(2, len(taken) + 3)])
                if name not in taken and not (antigravity_accounts.accounts_root() / name).exists())


def op_signin(store, ctx, topic=None, alias=None):
    """Start a sign-in. The owner opens the link on any device."""
    from .signin import pending, provider_name, taken_aliases
    record = pending(store)
    if record:
        raise Refused('A %s sign-in is still open. Finish it or tap Cancel on its card first.' % provider_name(record))
    if alias and alias not in antigravity_accounts.accounts(store):
        raise Refused('Unknown Antigravity account.')
    return op_add(store, ctx, topic, alias, taken_aliases(store))


def op_add(store, ctx, topic, alias, taken):
    """Queue the sign-in. With alias, sign in again to that registered account's own profile."""
    from .signin import KEY, RETRY_KEY, starting
    ready(store)
    broker = antigravity_accounts.AntigravityBroker(store)
    if alias and broker.signed_in(alias):
        raise Refused('%s is already signed in.' % broker.label(alias))
    topic = topic or ctx.topic
    if not topic:
        raise Refused('Start Add account from a Telegram channel. Torii posts the sign-in card there.')
    if not store.get('bot_username'):
        raise Refused('The service has not confirmed the bot identity yet. Try again in a minute.')
    name = alias or free_alias(taken)
    store.put(KEY, {'topic': topic, 'config_dir': str(antigravity_accounts.accounts_root() / name), 'state': 'starting',
                    'attempt': 0, 'created': False, 'pid': None, 'envelope': None, 'requested': time.time(),
                    'target': alias, 'alias': name, 'provider': 'antigravity',
                    'card_outbox': starting(store, topic, 'antigravity')})
    store.put(RETRY_KEY, None)
    logger.info('account signin requested provider=antigravity')
    return Result(True, 'Starting Antigravity sign-in. The sign-in card follows in a moment.', state='queued')


def parse_link(text):
    """Accept only a Google sign-in link that returns to a loopback port of this host."""
    url = text.strip() if isinstance(text, str) else ''
    try:
        parsed = urlsplit(url)
        query = parse_qs(parsed.query, keep_blank_values=True)
        back = urlsplit(query['redirect_uri'][0]) if len(query.get('redirect_uri', [])) == 1 else None
        if (len(url) > 8192 or any(character.isspace() for character in url) or parsed.scheme != 'https'
                or parsed.netloc != 'accounts.google.com' or not parsed.path.startswith('/o/oauth2/')
                or parsed.fragment or query.get('response_type') != ['code'] or len(query.get('state', [])) != 1
                or not 0 < len(query['state'][0]) <= 512 or back is None or back.scheme != 'http'
                or back.hostname not in ('localhost', '127.0.0.1') or back.username is not None
                or back.query or back.fragment or back.port is None or back.port < 1024):
            return None
    except (ValueError, KeyError):
        return None
    return {'url': url, 'port': back.port, 'path': back.path or '/', 'state': query['state'][0]}


def parse_callback(text, link):
    """The loopback request line for a pasted return address, or None when it is not this sign-in's."""
    try:
        parsed = urlsplit(text.strip())
        query = parse_qs(parsed.query, keep_blank_values=True)
        if (len(text) > 8192 or parsed.scheme != 'http' or parsed.hostname not in ('localhost', '127.0.0.1')
                or parsed.username is not None or parsed.fragment or parsed.port != link['port']
                or (parsed.path or '/') != link['path'] or query.get('state') != [link['state']]
                or any(character.isspace() or ord(character) < 33 or ord(character) > 126 for character in text.strip())):
            return None
        codes, errors = query.get('code', []), query.get('error', [])
        if not ((len(codes) == 1 and codes[0] and not errors) or (len(errors) == 1 and errors[0] and not codes)):
            return None
    except (ValueError, AttributeError):
        return None
    return link['path'] + '?' + parsed.query


async def forward(port, target):
    """Load the return address once on loopback. No proxy, no redirect."""
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection('127.0.0.1', port), FORWARD_SECONDS)
    except (OSError, asyncio.TimeoutError):
        return False
    try:
        writer.write(('GET %s HTTP/1.1\r\nHost: localhost:%d\r\nConnection: close\r\n\r\n' % (target, port)).encode())
        await writer.drain()
        status = (await asyncio.wait_for(reader.readline(), FORWARD_SECONDS)).split()
        return len(status) >= 2 and status[1][:1] in (b'2', b'3')
    except (OSError, asyncio.TimeoutError):
        return False
    finally:
        writer.close()


def intercept(store, topic, message):
    """Delete a pasted loopback address and hand this sign-in's own address to the driver in memory."""
    from .envelopes import _effect
    from .signin import _FORWARDED, pending
    record = pending(store)
    text = message.get('text') or message.get('caption')
    if (not record or record.get('state') not in ('code', 'checking') or topic != record.get('topic')
            or not isinstance(text, str) or not _CALLBACK.search(text)):
        return False
    _effect(store, None, 'delete', message['chat']['id'], message['message_id'], thread=message.get('message_thread_id'))
    signin = getattr(store, 'signin', None)
    link = getattr(signin, 'antigravity_link', None)
    target = parse_callback(text, link) if link and 'caption' not in message else None
    if any(key in message for key in _FORWARDED):
        store.enqueue_report(topic, 'I deleted that forwarded message. Paste the address as your own message.')
    elif target is None:
        store.enqueue_report(topic, "I deleted that message. It isn't the return address of this sign-in. "
                             'Paste the full address from the browser, starting with http://localhost, as one message.')
    elif record['state'] != 'code' or not signin.hand(WAITING, target):
        store.enqueue_report(topic, 'I deleted that message. Torii is already checking a return address.')
    logger.info('antigravity signin topic message handled')
    return True


class Client:
    """A minimal JSON-RPC client on the sign-in process. Nothing is journaled."""

    def __init__(self, process):
        self.process = process
        self.pending = {}
        self.counter = 0
        self.reader = asyncio.ensure_future(self.read())

    async def read(self):
        while True:
            line = await self.process.stdout.readline()
            if not line:
                break
            try:
                event = json.loads(line)
            except ValueError:
                continue
            future = self.pending.pop(event.get('id'), None) if isinstance(event, dict) and 'method' not in event else None
            if future is not None and not future.done():
                future.set_result(event)
        for future in self.pending.values():
            if not future.done():
                future.set_result({'error': {'message': 'agy stopped'}})

    def request(self, method, params):
        self.counter += 1
        future = asyncio.get_running_loop().create_future()
        self.pending[self.counter] = future
        self.process.stdin.write((json.dumps({'jsonrpc': '2.0', 'id': self.counter, 'method': method,
                                              'params': params}) + '\n').encode())
        return future

    async def close(self):
        if self.process.stdin is not None:
            self.process.stdin.close()
        await antigravity_accounts.end_group(self.process)
        self.reader.cancel()
        await asyncio.gather(self.reader, return_exceptions=True)


def settle_card(store, record, text):
    """Replace the sign-in card with the result. Its button and link go away."""
    from .envelopes import card_message
    card = record.get('card_outbox')
    if card is not None:
        store.db.execute('UPDATE outbox SET reply_markup=NULL WHERE id=?', (card,))
        store.db.execute('UPDATE outbox SET retired=1 WHERE id=? AND delivered=0 AND edit_message IS NULL', (card,))
    store.enqueue_report(record['topic'], text, kind='envelope', edit=card_message(store, card))


def finish(signins, record, text, state='failed'):
    from . import problems
    from .signin import KEY, RETRY_KEY
    store = signins.store
    with store.db:
        store.put(KEY, None)
        store.put(RETRY_KEY, None)
        settle_card(store, record, text)
        if text != CANCELLED:
            problems.record(store, 'accounts', 'signin-' + state, text)
    logger.info('account signin ended provider=antigravity state=%s', state)


class Stop(Exception):
    def __init__(self, text, state='failed'):
        super().__init__(text)
        self.text = text
        self.state = state


async def drive(signins, record):
    """Run one sign-in to its end. Every exit settles the pending record and the card."""
    from .signin import _started, pending
    store = signins.store
    alias = record['alias']
    client = None
    link_file = None

    async def until(done, seconds, late):
        deadline = time.time() + seconds
        while not done():
            current = pending(store)
            if not current or current.get('state') == 'cancelled':
                raise Stop(CANCELLED, 'cancelled')
            if time.time() >= deadline:
                raise Stop(late)
            await asyncio.sleep(signins.poll)

    try:
        try:
            binary = ready(store)
            home = antigravity_accounts.account_home(alias, create=True)
            env = antigravity_accounts.environment(home, display=True)
        except Refused as refusal:
            raise Stop(str(refusal) + ' No account was added.') from None
        link_file = Path(env['TMPDIR']) / ('torii-signin-' + secrets.token_hex(8))
        env['TORII_AGY_LINK'] = str(link_file)
        process = await asyncio.create_subprocess_exec(
            binary, 'acp', env=env, cwd=str(home), stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL, start_new_session=True)
        client = Client(process)
        signins.update(state='link', created=True, pid=process.pid, started=_started(process.pid))
        no_start = 'Antigravity did not start the sign-in. No account was added.'
        started = client.request('initialize', INITIALIZE)
        await until(started.done, signins.link_seconds, no_start)
        if (started.result().get('result') or {}).get('protocolVersion') != 1:
            raise Stop(no_start)
        granted = client.request('authenticate', {'methodId': METHOD})
        await until(lambda: granted.done() or (link_file.exists() and link_file.read_text().endswith('\n')),
                    signins.link_seconds, no_start)
        if not granted.done():
            link = parse_link(link_file.read_text())
            link_file.unlink()
            if link is None:
                raise Stop('Antigravity returned an unexpected sign-in link. No account was added.')
            signins.antigravity_link = link
            signins.code = None
            signins.waiting = WAITING
            again = alias if record.get('target') else None
            deadline = time.time() + signins.code_seconds
            post_card(signins, link, again)
            while not granted.done():
                await until(lambda: granted.done() or signins.code is not None,
                            deadline - time.time(), 'The Antigravity sign-in link expired. No account was added.')
                if granted.done():
                    break
                target, signins.code = signins.code, None
                signins.update(state='checking')
                delivered = await forward(link['port'], target)
                target = None
                if delivered:
                    await until(granted.done, signins.exit_seconds,
                                'Antigravity did not confirm the sign-in. No account was added.')
                else:
                    signins.update(state='code')
                    store.enqueue_report(record['topic'], 'Antigravity did not accept that address. '
                                         'Paste the address from the browser again.')
        if 'error' in granted.result():
            reason = str((granted.result()['error'] or {}).get('message', '')).lower()
            raise Stop('The Antigravity sign-in link expired. No account was added.'
                       if 'cancel' in reason or 'timed out' in reason or 'timeout' in reason
                       else 'Google did not grant the sign-in. No account was added.')
        await client.close()
        client = None
        try:
            models = await asyncio.to_thread(antigravity_accounts.run_models, *antigravity_accounts.models_call(store, alias))
        except Refused:
            models = None
        if models is None:
            raise Stop('Antigravity did not confirm an account login. No account was added.')
        succeed(signins, pending(store) or record, models)
    except Stop as stop:
        finish(signins, pending(store) or record, stop.text, stop.state)
    finally:
        signins.antigravity_link = None
        signins.code = None
        signins.waiting = None
        if client is not None:
            await client.close()
        if link_file is not None:
            link_file.unlink(missing_ok=True)
        signins.remove_folder(record)


def post_card(signins, link, again):
    """Post the sign-in card in the place of the Starting message."""
    from .envelopes import card_message
    from .signin import cancel_data, pending
    store = signins.store
    record = pending(store)
    title = 'Sign in to Antigravity account %s again' % again if again else 'Add an Antigravity account'
    text = (title + '\n1. Open this link and sign in to the Google account:\n' + link['url'] +
            "\n2. After you approve, the page doesn't load. That is expected. Copy that page's address "
            '(it starts with http://localhost:%d) and paste it here as one message. I delete it at once.' % link['port'] +
            '\nThe link works for 5 minutes. Paste only an address from a link that Torii sent for your own request.')
    markup = {'inline_keyboard': [[{'text': 'Cancel sign-in', 'callback_data': cancel_data(record)}]]}
    with store.db:
        previous = record.get('card_outbox')
        if previous is not None:
            store.db.execute('UPDATE outbox SET reply_markup=NULL WHERE id=?', (previous,))
            store.db.execute('UPDATE outbox SET retired=1 WHERE id=? AND delivered=0 AND edit_message IS NULL', (previous,))
        card = store.enqueue_report(record['topic'], text, kind='envelope', reply_markup=markup,
                                    edit=card_message(store, previous))
        signins.update(state='code', card_outbox=card)
    logger.info('account signin card posted provider=antigravity')


def succeed(signins, record, models):
    from .signin import KEY
    store = signins.store
    alias = record['alias']
    with store.db:
        profiles = antigravity_accounts.accounts(store)
        previous = profiles.get(alias, {})
        profiles[alias] = dict(previous, label=previous.get('label') or alias, created=previous.get('created', time.time()))
        store.put('antigravity_accounts', profiles)
        antigravity_accounts.record_check(store, alias, models)
        blocks = store.get('antigravity_account_blocks', {}) or {}
        if blocks.pop(alias, None) is not None:
            store.put('antigravity_account_blocks', blocks)
        if store.get('antigravity_active_account') not in profiles:
            store.put('antigravity_active_account', alias)
        store.put(KEY, None)
        settle_card(store, record, 'Antigravity account %s is signed in.' % profiles[alias]['label'])
    logger.info('account signin added provider=antigravity')
