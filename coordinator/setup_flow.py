"""Group setup, account readiness, and lifecycle recovery."""

import asyncio
import logging
import time

from . import problems
from .pairing import describe_sender, link
from .telegram import TelegramError


async def check_setup_accounts(store, requested):
    from .account_status import check_accounts
    from .codex_accounts import refresh_codex_accounts
    from . import problems
    try:
        await check_accounts(store)
        await refresh_codex_accounts(store)
    except Exception as error:
        problems.record(store, 'accounts', 'setup-check-failed', type(error).__name__)
        topic = store.get('control_topic')
        if topic:
            with store.db:
                store.enqueue_report(topic, 'Account check could not finish. Send /setup to retry.')
    else:
        with store.db:
            store.put('setup_accounts_checked', {'checked': time.time(), 'profiles': enabled_profiles(store)})
    finally:
        with store.db:
            requests = store.get('setup_requests', {}) or {}
            if requests.get('check_accounts') == requested:
                requests.pop('check_accounts', None)
            store.put('setup_requests', requests)


def enabled_profiles(store):
    return {registry + ':' + alias: profile.get('config_dir')
            for registry in ('accounts', 'codex_accounts')
            for alias, profile in (store.get(registry, {}) or {}).items() if profile.get('enabled')}


def chatgpt_ready(store):
    import shutil
    from .codex_accounts import CodexBroker
    broker = CodexBroker(store)
    binary = shutil.which('codex')
    return bool(binary and broker.managed() and broker.signed_in(broker.active()))


def agents_ready(store):
    from .accounts import signed_in
    status = store.get('account_status', {}) or {}
    return any(signed_in(store, alias, profile) and not status.get(alias, {}).get('error')
               for alias, profile in (store.get('accounts', {}) or {}).items()) or chatgpt_ready(store)


def setup_state(store):
    import shutil
    from .accounts import AccountBroker, signed_in
    from .codex_accounts import CodexBroker
    claude = any(signed_in(store, alias, profile) for alias, profile in (store.get('accounts', {}) or {}).items())
    broker = CodexBroker(store)
    main = 'claude' if AccountBroker(store).select() else (
        'chatgpt' if chatgpt_ready(store) and broker.parent_account() else None)
    return {'claude': claude, 'codex': any(broker.signed_in(alias) for alias in broker.accounts()),
            'codex_installed': bool(shutil.which('codex')),
            'main': main,
            'agents': store.get('execution', 'agents') == 'agents',
            'forced': store.get('forced_pair_only', False)}


def setup_text(state):
    text = 'Torii is ready. Send me what you want done.' if state['agents'] else 'Torii is paired. Connect Claude or ChatGPT to start working.'
    text += '\nClaude: ' + ('connected' if state['claude'] else 'not connected')
    text += '\nChatGPT: ' + ('connected' if state['codex'] else 'not connected')
    if state.get('main') == 'chatgpt':
        text += "\nMain chat: ChatGPT. When its account runs out, I'll tell you and wait for you to pick another."
    elif state.get('main') == 'claude':
        text += '\nMain chat: Claude'
    if state['forced']:
        text += '\nThis service was started with --pair-only. On the computer run ./scripts/install-service.py.'
    return text


def setup_status(store, force=False, prefix='', in_place=True):
    from .control_ui import emit, _key
    from .signin import pending, setup_signin
    topic = store.get('control_topic')
    if not topic:
        return
    form = store.get('project_setup:' + topic, {}) or {}
    if not force and form.get('stage') in ('new', 'existing', 'first', 'creating'):
        return
    if pending(store):
        if force:
            setup_signin(store, topic)
        return
    state = setup_state(store)
    text = setup_text(state)
    rows = []
    if not state['claude']:
        rows.append([('Connect Claude', {'op': 'setup.claude'})])
    if state['codex_installed'] and not state['codex']:
        rows.append([('Add ChatGPT', {'op': 'setup.chatgpt'})])
    if (store.get('setup_requests', {}) or {}).get('check_accounts'):
        text += '\nChecking accounts. This can take three minutes per account.'
    if prefix and prefix != text:
        text = prefix + '\n\n' + text
    previous = store.get('setup_card')
    card = store.db.execute('SELECT * FROM outbox WHERE id=?', (previous,)).fetchone() if previous else None
    ui = store.get(_key(topic), {}) or {}
    flat = [action for row in rows for _, action in row]
    if not force and card and card['text'] == text and ui.get('actions') == flat:
        return
    if card and card['topic'] == topic:
        store.put(_key(topic), dict(ui, outbox=previous))
    from .envelopes import card_message
    emit(store, topic, text, rows, in_place=in_place, where='group_setup',
         edit_message=card_message(store, previous) if in_place and card and card['topic'] == topic else None)
    store.put('setup_card', store.get(_key(topic))['outbox'])


PRIVATE_REFUSAL = ('This install used private chat mode, which Torii no longer supports. '
                   'Run `python3 -m coordinator pair --replace` (or setup again) to move it to a group. '
                   'Your projects and their history stay on this Mac.')
TOPICS_OFF = ('Paired. One step left: turn on Topics so each project gets its own thread.\n'
              'Tap the group name → Edit → Topics → turn on → Save.')
RIGHTS = 'I need to be an admin with **Manage topics** to create project topics.'
SECOND_GROUP = ("This Torii bot is already set up in another group, so I'm leaving this one.\n"
                'To move Torii here, run `python3 -m coordinator pair --replace` on your Mac.')
ANONYMOUS = ("I can't tell who sent this because you're posting anonymously. Turn off **Remain anonymous** "
             'in your admin settings, then run setup again.')
OWNER_LEFT = 'The Torii owner left this group, so Torii is paused. Rejoin the group to resume.'
REMOVED = ('The bot was removed from the group. Add it back from the link below, or run '
           '`pair --replace` to use a different group.')
GENERAL = 'Torii works inside topics. Use the **Torii** topic, or `/project new <name>`.'


def blocked(store):
    return (store.get('execution') == 'pairing' or store.get('mode') == 'private'
            or store.get('setup_problem') in ('owner_left', 'removed'))


def account_check(store):
    requests = store.get('setup_requests', {}) or {}
    requests['check_accounts'] = time.time()
    store.put('setup_requests', requests)


def restore_control_topic(store):
    if store.get('control_topic') or store.get('mode') != 'group' or store.get('owner') is None:
        return
    home = store.get('coordinator_home_topic')
    topic = store.topic(home) if home else None
    if topic and topic['enabled'] and topic['chat'] == store.chat():
        store.put('control_topic', home)


def general_topic(store, chat):
    topic = str(chat) + ':0'
    store.db.execute('INSERT OR IGNORE INTO topics(id,chat,thread,name,cwd) VALUES (?,?,?,?,?)',
                     (topic, chat, 0, 'General', ''))
    return topic


def notice(store, chat, text, markup=None, key=None, every=None):
    key = key or 'group_notice:' + str(chat) + ':' + text
    now = time.time()
    previous = store.get(key)
    if previous is not None and (every is None or now - previous < every):
        return
    topic = store.get('control_topic') if chat == store.chat() else None
    outbox = store.enqueue_report(topic or general_topic(store, chat), text, kind='group-setup', reply_markup=markup)
    store.put(key, now)
    return outbox


def has_rights(member, forum=True):
    return member.get('status') == 'creator' or (member.get('status') == 'administrator'
                                               and (not forum or member.get('can_manage_topics', False)))


def rights_notice(store, chat):
    notice(store, chat, RIGHTS, {'inline_keyboard': [[{'text': 'Fix permissions',
           'url': link(store.get('bot_username') or '', 'fix')}]]}, key='rights_notice:' + str(chat))


def topics_notice(store):
    from .control_ui import emit
    if store.get('setup_problem') not in ('owner_left', 'removed'):
        store.put('setup_problem', 'topics_off')
    topic = general_topic(store, store.chat())
    if store.get('topics_card'):
        return
    emit(store, topic, TOPICS_OFF, [[('Check again', {'op': 'setup.topics_check'})]], where='topics')
    store.put('topics_card', store.get('control_ui:' + topic)['outbox'])


def membership(store, event):
    chat = event['chat']['id']
    if event['chat']['type'] not in ('group', 'supergroup'):
        return 'ignored'
    member = event['new_chat_member']
    status = member['status']
    if chat == store.chat():
        store.put('group_type', event['chat']['type'])
    joins = store.get('group_joins', {}) or {}
    if status in ('left', 'kicked'):
        joins.pop(str(chat), None)
        store.put('group_joins', joins)
        if chat == store.chat():
            store.put('setup_problem', 'removed')
            if event['chat']['type'] == 'group':
                store.put('group_migration_removal', chat)
        return 'service_event'
    if status not in ('member', 'administrator', 'creator', 'restricted'):
        return 'ignored'
    joins[str(chat)] = dict(joins.get(str(chat), {}), performer=event['from']['id'], member=member,
                           joined=joins.get(str(chat), {}).get('joined', time.time()), type=event['chat']['type'])
    store.put('group_joins', joins)
    if chat == store.chat():
        store.put('group_migration_removal', None)
        if store.get('setup_problem') == 'removed' and event['from']['id'] != store.get('owner'):
            return 'service_event'
        if not has_rights(member, event['chat'].get('is_forum', True)):
            if store.get('setup_problem') != 'owner_left':
                store.put('setup_problem', 'rights')
            rights_notice(store, chat)
        else:
            store.put('rights_notice:' + str(chat), None)
            if store.get('setup_problem') in ('removed', 'rights'):
                store.put('setup_problem', None)
        store.put('group_check_requested', True)
    elif store.chat() is None and not has_rights(member, event['chat'].get('is_forum', True)):
        rights_notice(store, chat)
    return 'service_event'


def migrate(store, old, new):
    if store.chat() not in (old, new):
        return False
    if store.chat() == new:
        return True
    import json
    store.db.execute('PRAGMA defer_foreign_keys=ON')
    prefix = str(old) + ':'
    for topic in [row for row in store.topics() if row['chat'] == old]:
        target = str(new) + ':' + str(topic['thread'])
        for table in ('tasks', 'workers', 'messages', 'outbox', 'attachments', 'envelopes', 'problems'):
            store.db.execute('UPDATE ' + table + ' SET topic=? WHERE topic=?', (target, topic['id']))
        store.db.execute('UPDATE topics SET id=?,chat=? WHERE id=?', (target, new, topic['id']))
    def replace(value):
        if isinstance(value, str) and value.startswith(prefix):
            return str(new) + value[len(str(old)):]
        if isinstance(value, list):
            return [replace(item) for item in value]
        if isinstance(value, dict):
            return {replace(key): replace(item) for key, item in value.items()}
        return value
    for row in store.db.execute('SELECT key,value FROM settings').fetchall():
        key = row['key'].replace(prefix, str(new) + ':')
        value = replace(json.loads(row['value']))
        if key != row['key']:
            store.db.execute('DELETE FROM settings WHERE key=?', (row['key'],))
        store.put(key, value)
    store.put('group', new)
    store.put('group_type', 'supergroup')
    if store.get('group_migration_removal') == old:
        store.put('group_migration_removal', None)
        if store.get('setup_problem') == 'removed':
            store.put('setup_problem', None)
    store.put('group_check_requested', True)
    joins = store.get('group_joins', {}) or {}
    if str(old) in joins:
        joins[str(new)] = joins.pop(str(old))
        store.put('group_joins', joins)
    return True


def owner_event(store, message):
    if message['chat']['id'] != store.chat():
        return False
    owner = store.get('owner')
    if message.get('left_chat_member', {}).get('id') == owner or 'chat_owner_left' in message:
        store.put('setup_problem', 'owner_left')
        notice(store, store.chat(), OWNER_LEFT, key='owner_left_notice')
        return True
    if any(user.get('id') == owner for user in message.get('new_chat_members', [])):
        if store.get('setup_problem') == 'owner_left':
            store.put('setup_problem', None)
            store.put('owner_left_notice', None)
            store.enqueue_report(store.get('control_topic') or general_topic(store, store.chat()), 'Resumed.')
        return True
    if 'chat_owner_changed' in message:
        store.put('group_owner_changed', 'The group now has a different Telegram owner')
        logging.getLogger(__name__).info('group Telegram owner changed chat=%s', store.chat())
        return True
    return False


def replace_pairing(store):
    from .accounts import signed_in
    old_chat = store.chat()
    old_owner = store.get('owner')
    legacy = store.get('mode') == 'private'
    with store.db:
        store.db.execute('UPDATE outbox SET retired=1 WHERE delivered=0 AND topic IN '
                         '(SELECT id FROM topics WHERE chat=?)', (old_owner if legacy else old_chat,))
        store.db.execute('UPDATE topics SET enabled=0 WHERE chat=?', (old_owner if legacy else old_chat,))
        if legacy:
            store.db.execute('UPDATE topics SET chat=0 WHERE chat=?', (old_owner,))
        keys = ['owner', 'group', 'mode', 'control_topic', 'pairing', 'coordinator_home_topic',
                'setup_problem', 'setup_card', 'topics_card', 'group_joins', 'group_check_requested',
                'first_project', 'group_projects', 'group_is_forum', 'owner_name', 'group_name',
                'owner_left_notice', 'group_owner_changed', 'pair_delete']
        keys += ['group_type', 'group_migration_removal']
        keys += ['_'.join(parts) for parts in [('bot', 'topics'), ('topic', 'requests'), ('second', 'project'),
                 ('creating', 'topics'), ('pending', 'bind'), ('topic', 'fallbacks'), ('switched', 'projects')]]
        for key in keys:
            store.db.execute('DELETE FROM settings WHERE key=?', (key,))
        ready = agents_ready(store)
        store.put('execution', 'agents' if ready else 'pairing')


def op_setup_claude(store, ctx):
    from .signin import op_account_add
    return op_account_add(store, ctx, topic=store.get('control_topic'), provider='claude')


def op_setup_chatgpt(store, ctx):
    from .signin import op_account_add
    return op_account_add(store, ctx, topic=store.get('control_topic'), provider='codex')


def op_setup_topics_check(store, ctx):
    store.put('group_check_requested', True)
    return 'Checking Topics.'


def topic_unreachable(store, row, reason):
    if reason == 'topics_off':
        topics_notice(store)
        store.put('group_check_requested', True)
        return True
    topic = store.topic(row['topic'])
    if not row['thread'] or not topic:
        return False
    store.db.execute('UPDATE topics SET enabled=0 WHERE id=?', (topic['id'],))
    store.db.execute('UPDATE outbox SET retired=1 WHERE topic=? AND delivered=0', (topic['id'],))
    problems.record(store, 'telegram', 'topic-gone', 'Group topic deleted.', topic=topic['id'])
    if topic['id'] == store.get('control_topic'):
        store.put('control_topic', None)
        store.put('setup_card', None)
    else:
        notice(store, store.chat(), 'The topic for **' + topic['name'] + '** was deleted. '
               'Use /project new <name> to make a new one.', key='gone_notice:' + topic['id'])
    store.put('group_check_requested', True)
    return True


async def tick(service):
    store = service.store
    refresh_setup = False
    now = time.time()
    if store.get('mode') == 'private':
        return
    with store.db:
        restore_control_topic(store)
    joins = store.get('group_joins', {}) or {}
    for chat, entry in list(joins.items()):
        if int(chat) == store.chat():
            continue
        if store.chat() is None and now - entry['joined'] < 600:
            continue
        group_type = store.get('group_type', 'group' if store.get('group_is_forum') is False else None)
        if group_type == 'group' and entry.get('type') == 'supergroup':
            continue
        try:
            await service.telegram.call('getChat', chat_id=int(chat))
        except TelegramError:
            continue
        text = SECOND_GROUP if store.chat() else SECOND_GROUP.split('\n')[0]
        await service.telegram.call('sendMessage', chat_id=int(chat), text=text)
        await service.telegram.call('leaveChat', chat_id=int(chat))
        with store.db:
            joins = store.get('group_joins', {}) or {}
            joins.pop(chat, None)
            store.put('group_joins', joins)
    if store.get('group_migration_removal') == store.chat() and store.chat() is not None:
        chat = await service.telegram.call('getChat', chat_id=store.chat())
        if chat.get('migrate_to_chat_id'):
            with store.db:
                migrate(store, store.chat(), chat['migrate_to_chat_id'])
    if store.chat() is None or store.get('setup_problem') == 'removed':
        return
    requested = store.get('group_check_requested')
    waiting = not store.get('control_topic') or store.get('setup_problem') in ('rights', 'topics_off')
    if (requested or waiting) and (requested or now >= getattr(service, 'group_next_check', 0)):
        service.group_next_check = now + 60
        chat = await service.telegram.call('getChat', chat_id=store.chat())
        if chat.get('migrate_to_chat_id'):
            with store.db:
                migrate(store, store.chat(), chat['migrate_to_chat_id'])
            return
        bot = store.get('bot_id')
        if bot is None:
            me = await service.telegram.call('getMe')
            bot = me['id']
            with store.db:
                store.put('bot_id', bot)
                store.put('bot_username', me['username'])
        member = await service.telegram.call('getChatMember', chat_id=store.chat(), user_id=bot)
        with store.db:
            store.put('group_check_requested', False)
            store.put('group_is_forum', chat.get('is_forum', False))
            if chat.get('type'):
                store.put('group_type', chat['type'])
            if not has_rights(member, chat.get('is_forum', False)):
                if store.get('setup_problem') != 'owner_left':
                    store.put('setup_problem', 'rights')
                rights_notice(store, store.chat())
                return
            store.put('rights_notice:' + str(store.chat()), None)
            if store.get('setup_problem') == 'rights':
                store.put('setup_problem', None)
            if not chat.get('is_forum'):
                topics_notice(store)
                return
            if store.get('setup_problem') == 'topics_off':
                store.put('setup_problem', None)
            card = store.get('topics_card')
            if card:
                from .envelopes import card_message
                store.enqueue_report(general_topic(store, store.chat()), 'Topics are on.', edit=card_message(store, card))
                store.put('topics_card', None)
        if not store.get('control_topic'):
            try:
                thread = await service.telegram.create_topic(store.chat(), 'Torii')
            except TelegramError as error:
                if error.reason != 'topics_off':
                    raise
                with store.db:
                    topics_notice(store)
                return
            with store.db:
                topic = str(store.chat()) + ':' + str(thread)
                store.db.execute('INSERT OR IGNORE INTO topics(id,chat,thread,name,cwd) VALUES (?,?,?,?,?)',
                                 (topic, store.chat(), thread, 'Torii', ''))
                store.enqueue_report(general_topic(store, store.chat()),
                                     'Torii is set up in this group. Continue in the **Torii** topic.')
                store.put('control_topic', topic)
                refresh_setup = True
    checking = getattr(service, 'setup_accounts_task', None)
    if checking and checking.done():
        service.setup_accounts_task = None
        checking.result()
    requested = (store.get('setup_requests', {}) or {}).get('check_accounts')
    if requested and getattr(service, 'setup_accounts_task', None) is None:
        service.setup_accounts_task = asyncio.create_task(check_setup_accounts(store, requested))
    ready = agents_ready(store)
    checking_claude = requested or getattr(service, 'setup_accounts_task', None)
    if store.get('execution') == 'pairing' and ready and (chatgpt_ready(store) or not checking_claude):
        with store.db:
            store.put('execution', 'agents')
            refresh_setup = True
        service.wake('feed').set()
    if store.get('control_topic') and (refresh_setup or store.get('execution') == 'pairing'):
        with store.db:
            setup_status(store, force=refresh_setup)
    if store.get('setup_problem') in ('rights', 'owner_left'):
        return
    for entry in list(store.get('group_projects', [])):
        member = await service.telegram.call('getChatMember', chat_id=store.chat(), user_id=store.get('bot_id'))
        if not has_rights(member):
            with store.db:
                store.put('setup_problem', 'rights')
                rights_notice(store, store.chat())
            return
        try:
            thread = await service.telegram.create_topic(store.chat(), entry['name'][:128])
        except TelegramError as error:
            if error.reason != 'topics_off':
                raise
            with store.db:
                store.put('group_check_requested', True)
                topics_notice(store)
            return
        with store.db:
            target = str(store.chat()) + ':' + str(thread)
            store.db.execute('INSERT OR IGNORE INTO topics(id,chat,thread,name,cwd) VALUES (?,?,?,?,?)',
                             (target, store.chat(), thread, entry['name'], ''))
            store.bind(target, entry['cwd'], entry['name'], provider='claude', enabled=True)
            if entry['release_held']:
                store.db.execute('UPDATE messages SET topic=? WHERE topic=? AND delivered=?',
                                 (target, entry['source'], 'pending'))
            store.put('project_setup:' + entry['source'], None)
            store.put('group_projects', [item for item in store.get('group_projects', []) if item != entry])
            store.enqueue_report(target, entry['name'] + ' is ready. Send work here.')
        service.wake('feed').set()
