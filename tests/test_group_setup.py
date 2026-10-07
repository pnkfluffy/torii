import asyncio
import hashlib
import json
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import __main__ as cli
from coordinator.service import Service
from coordinator.setup_flow import ANONYMOUS, GENERAL, OWNER_LEFT, RIGHTS, SECOND_GROUP, TOPICS_OFF, tick
from coordinator.store import Store
from coordinator.telegram import Telegram, TelegramError, _reason
from coordinator.telegram_updates import update_problem
from tests.test_service import FakeRunner, FakeTelegram


CHAT = -10042
HOME = '-10042:10'


def group_update(number, text='', user=7, thread=10, **extra):
    message = {'message_id': number, 'text': text, 'from': {'id': user, 'is_bot': False},
               'chat': {'id': CHAT, 'type': 'supergroup', 'title': 'Test group', 'is_forum': True}}
    if thread is not None:
        message['message_thread_id'] = thread
    message.update(extra)
    return {'update_id': number, 'message': message}


def pair_group(store, user=7, bot=None):
    with store.db:
        store.put('owner', user)
        store.put('group', CHAT)
        store.put('mode', 'group')
        store.put('execution', 'pairing')
        store.put('control_topic', HOME)
        store.put('bot_username', (bot or {}).get('username', 'test_bot'))
        store.put('bot_id', (bot or {}).get('id', 99))
        store.db.execute('INSERT OR IGNORE INTO topics(id,chat,thread,name,cwd) VALUES (?,?,?,?,?)',
                         (HOME, CHAT, 10, 'Torii', ''))
    return HOME


def member_update(number, chat=CHAT, status='administrator', rights=True, actor=7, forum=True):
    return {'update_id': number, 'my_chat_member': {'chat': {'id': chat, 'type': 'supergroup', 'is_forum': forum},
            'from': {'id': actor}, 'old_chat_member': {'status': 'left', 'user': {'id': 99}},
            'new_chat_member': {'status': status, 'can_manage_topics': rights, 'user': {'id': 99}}}}


class GroupTelegram(FakeTelegram):
    def __init__(self):
        super().__init__()
        self.forum = True
        self.creator = True
        self.rights = True
        self.thread = 10
        self.create_error = None

    async def call(self, method, **params):
        self.calls.append((method, params))
        if method == 'getMe':
            return {'id': 99, 'username': 'test_bot'}
        if method == 'getChat':
            return {'id': params['chat_id'], 'is_forum': self.forum}
        if method == 'getChatMember':
            if params['user_id'] == 99:
                return {'status': 'administrator', 'can_manage_topics': self.rights}
            return {'status': 'creator' if self.creator else 'administrator'}
        return True

    async def create_topic(self, chat, name):
        self.calls.append(('createForumTopic', {'chat_id': chat, 'name': name}))
        if self.create_error:
            raise self.create_error
        self.thread += 1
        return self.thread


class GroupSetupTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store = Store(self.root / 'state')
        self.telegram = GroupTelegram()
        self.service = Service(self.store, self.telegram, FakeRunner(), self.root)
        self.home = patch('pathlib.Path.home', return_value=self.root)
        self.home.start()

    async def asyncTearDown(self):
        checking = getattr(self.service, 'setup_accounts_task', None)
        if checking:
            checking.cancel()
            await asyncio.gather(checking, return_exceptions=True)
        self.store.close()
        self.home.stop()
        self.temp.cleanup()

    def texts(self):
        return [row[0] for row in self.store.db.execute('SELECT text FROM outbox ORDER BY id')]

    async def pair(self, number=1, text='/start', thread=None, **extra):
        code = self.store.pairing_code()
        result = await self.service.accept_update(group_update(number, text + ' ' + code, thread=thread, **extra))
        return result, code

    async def test_start_in_general_pairs_creator_and_deletes_code(self):
        for number, command in enumerate(('/start', '/start@test_bot'), 1):
            with self.subTest(command=command):
                from coordinator.setup_flow import replace_pairing
                replace_pairing(self.store)
                result, code = await self.pair(number, command)
                self.assertEqual(result, 'paired')
                self.assertEqual((self.store.get('owner'), self.store.chat(), self.store.get('mode')), (7, CHAT, 'group'))
                self.assertIsNone(self.store.get('pairing'))
                self.assertIn(('deleteMessage', {'chat_id': CHAT, 'message_id': number}), self.telegram.calls)
                self.assertFalse(any(topic['enabled'] for topic in self.store.topics()))
                self.assertEqual(self.store.get('execution'), 'pairing')
                self.assertNotIn(code, str(self.texts()))

    async def test_basic_group_pairs_then_shows_topics_off(self):
        self.telegram.forum = False
        result, _ = await self.pair(chat={'id': CHAT, 'type': 'group', 'title': 'Test group'})
        self.assertEqual(result, 'paired')
        await tick(self.service)
        self.assertIn(TOPICS_OFF, self.texts())

    async def test_permanent_pairing_delete_failure_is_not_retried_on_later_updates(self):
        original = self.telegram.call
        for code in (400, 403):
            with self.subTest(code=code):
                from coordinator.setup_flow import replace_pairing
                replace_pairing(self.store)
                async def call(method, **params):
                    if method == 'deleteMessage':
                        raise TelegramError(code)
                    return await original(method, **params)
                with patch.object(self.telegram, 'call', side_effect=call) as transport:
                    self.assertEqual((await self.pair(code))[0], 'paired')
                    self.assertIsNone(self.store.get('pair_delete'))
                    await self.service.accept_update(group_update(code + 1, '/ping'))
                self.assertEqual(sum(item.args[0] == 'deleteMessage' for item in transport.call_args_list), 1)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM problems WHERE code='delete-failed'").fetchone()[0], 2)

    async def test_transient_pairing_delete_failure_still_retries(self):
        original = self.telegram.call
        async def call(method, **params):
            if method == 'deleteMessage':
                raise TelegramError(429, retry_after=1)
            return await original(method, **params)
        with patch.object(self.telegram, 'call', side_effect=call):
            await self.pair()
        self.assertIsNotNone(self.store.get('pair_delete'))
        await self.service.accept_update(group_update(2, '/ping'))
        self.assertIsNone(self.store.get('pair_delete'))

    async def test_stale_control_card_edit_does_not_disable_a_reachable_topic(self):
        pair_group(self.store)
        row_id = self.store.enqueue_report(HOME, 'Connected.', edit=999)
        self.telegram.send = AsyncMock(side_effect=[TelegramError(400, reason='thread_not_found'),
                                                    {'message_id': 1000}])
        self.assertTrue(await self.service.deliver_once())
        self.assertEqual(self.store.get('control_topic'), HOME)
        row = self.store.db.execute('SELECT * FROM outbox WHERE id=?', (row_id,)).fetchone()
        self.assertIsNone(row['edit_message'])
        self.assertFalse(row['retired'])
        self.assertTrue(await self.service.deliver_once())
        self.assertEqual(self.store.db.execute('SELECT delivered FROM outbox WHERE id=?', (row_id,)).fetchone()[0], 1)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM problems WHERE code='topic-gone'").fetchone()[0], 0)

    async def test_terminal_connected_claude_pairs_without_stale_connect_card(self):
        from coordinator.signin import SignIns
        from tests.test_setup_installer import INSTALLER
        with patch.dict(INSTALLER['setup'].__globals__, claude_login=AsyncMock(return_value=True)), \
                patch.object(SignIns, 'identity', AsyncMock(return_value={'email': 'owner@example.test'})), \
                patch('coordinator.setup_terminal.interactive_mac', return_value=False):
            self.assertTrue(await INSTALLER['connect_claude'](self.store))
        self.assertEqual(self.store.get('execution'), 'agents')
        self.store.put('execution', 'pairing')
        result, _ = await self.pair()
        self.assertEqual(result, 'paired')
        await tick(self.service)
        self.assertEqual(self.store.get('execution'), 'agents')
        self.assertNotIn('Connect a Claude account', '\n'.join(self.texts()))
        topic = self.store.get('control_topic')
        actions = self.store.get('control_ui:' + topic)['actions']
        self.assertNotIn('setup.claude', [action['op'] for action in actions])
        card = self.store.db.execute('SELECT text FROM outbox WHERE id=?', (self.store.get('setup_card'),)).fetchone()[0]
        self.assertEqual(card, 'Torii is ready. Send me what you want done.\nClaude: connected\nChatGPT: not connected\nMain chat: Claude')

    async def test_manual_pair_inside_topic_keeps_setup_guide(self):
        result, _ = await self.pair(text='/pair', thread=4)
        self.assertEqual(result, 'paired')
        self.assertIsNotNone(self.store.topic('-10042:4'))
        self.assertTrue(any('Link this channel' in text for text in self.texts()))

    async def test_creator_required_and_code_not_burned(self):
        self.telegram.creator = False
        result, code = await self.pair()
        self.assertEqual(result, 'unauthorized')
        self.assertEqual(self.store.get('pairing')['hash'], hashlib.sha256(code.encode()).hexdigest())
        self.assertIsNone(self.store.get('owner'))

    async def test_bot_and_anonymous_sender_rejected_without_burning(self):
        for number, extra in enumerate(({'from': {'id': 7, 'is_bot': True}}, {'sender_chat': {'id': CHAT}}), 1):
            result, code = await self.pair(number, **extra)
            self.assertEqual(result, 'unauthorized')
            self.assertEqual(self.store.get('pairing')['hash'], hashlib.sha256(code.encode()).hexdigest())
        self.assertIn(ANONYMOUS, self.texts())

    async def test_expired_wrong_reused_and_other_group_codes_refused(self):
        code = self.store.pairing_code()
        with self.store.db:
            self.store.put('pairing', {'hash': hashlib.sha256(code.encode()).hexdigest(), 'expires': 0})
        for number, value in enumerate((code, 'wrong'), 1):
            self.assertEqual(await self.service.accept_update(group_update(number, '/start ' + value, thread=None)), 'unauthorized')
        self.assertEqual(self.texts().count('That setup link has expired. Run setup again on your Mac.'), 1)
        result, code = await self.pair(3)
        self.assertEqual(result, 'paired')
        self.assertEqual(await self.service.accept_update(group_update(4, '/start ' + code)), 'unauthorized')
        new = self.store.pairing_code()
        self.assertEqual(await self.service.accept_update(group_update(5, '/start ' + new,
                         chat={'id': -200, 'type': 'supergroup'})), 'unauthorized')
        self.assertIsNotNone(self.store.get('pairing'))

    async def test_membership_performer_is_only_a_cross_check(self):
        self.store.accept(member_update(1, actor=8))
        with self.assertLogs('coordinator.service', level='INFO') as captured:
            result, _ = await self.pair(2)
        self.assertEqual(result, 'paired')
        self.assertTrue(any('performer differs' in line for line in captured.output))

    async def test_topics_on_creates_home_and_exact_screens(self):
        await self.pair()
        await tick(self.service)
        self.assertEqual(self.store.get('control_topic'), '-10042:11')
        self.assertIn('Torii is set up in this group. Continue in the **Torii** topic.', self.texts())
        self.assertTrue(any(text.startswith('Torii is paired. Connect Claude or ChatGPT to start working.') for text in self.texts()))

    def callback(self, number, user=7):
        state = self.store.get('control_ui:-10042:0')
        row = self.store.db.execute('SELECT id FROM outbox WHERE id=?', (self.store.get('topics_card'),)).fetchone()
        self.store.delivered(row['id'], 80)
        return {'update_id': number, 'callback_query': {'id': 'topics', 'from': {'id': user},
                'message': {'message_id': 80, 'chat': {'id': CHAT, 'type': 'supergroup'}},
                'data': 'torii:' + state['token'] + ':0'}}

    async def test_topics_check_off_on_and_non_owner_toasts(self):
        self.telegram.forum = False
        await self.pair()
        await tick(self.service)
        result = await self.service.accept_update(self.callback(2, 8))
        self.assertEqual(result, 'topics_unauthorized')
        self.assertIsNone(self.store.get('control_topic'))
        result = await self.service.accept_update(self.callback(3))
        self.assertEqual(result, 'topics_off')
        await self.service.answer_callback('topics', result)
        self.assertEqual(self.telegram.calls[-1][1]['text'], 'Topics are still off.')
        self.telegram.forum = True
        self.assertEqual(await self.service.accept_update(self.callback(4)), 'topics_on')
        self.assertIn('Topics are on.', self.texts())

    async def test_topics_callback_rejects_non_ascii_index(self):
        self.telegram.forum = False
        await self.pair()
        await tick(self.service)
        update = self.callback(2)
        update['callback_query']['data'] = update['callback_query']['data'][:-1] + '²'
        self.assertEqual(await self.service.accept_update(update), 'stale_callback')
        self.assertIsNone(self.store.get('control_topic'))

    async def test_membership_rights_and_demotion(self):
        for number, status, rights in ((1, 'member', False), (2, 'administrator', False)):
            self.store.accept(member_update(number, status=status, rights=rights))
            self.assertIn(RIGHTS, self.texts())
        pair_group(self.store)
        self.store.accept(member_update(3, rights=False))
        self.assertEqual(self.store.get('setup_problem'), 'rights')
        self.store.accept(member_update(4))
        self.assertIsNone(self.store.get('setup_problem'))

    async def test_rights_checked_before_topic_creation(self):
        await self.pair()
        self.telegram.rights = False
        await tick(self.service)
        self.assertEqual(self.store.get('setup_problem'), 'rights')
        self.assertFalse(any(method == 'createForumTopic' for method, _ in self.telegram.calls))

    async def test_second_group_refused_and_unpaired_join_times_out(self):
        pair_group(self.store)
        self.store.accept(member_update(1, chat=-200))
        await tick(self.service)
        self.assertIn(('sendMessage', {'chat_id': -200, 'text': SECOND_GROUP}), self.telegram.calls)
        self.assertIn(('leaveChat', {'chat_id': -200}), self.telegram.calls)
        from coordinator.setup_flow import replace_pairing
        replace_pairing(self.store)
        self.store.accept(member_update(2, chat=-300))
        await tick(self.service)
        self.assertNotIn(('leaveChat', {'chat_id': -300}), self.telegram.calls)
        with patch('coordinator.setup_flow.time.time', return_value=time.time() + 601):
            await tick(self.service)
        self.assertIn(('leaveChat', {'chat_id': -300}), self.telegram.calls)

    async def test_removed_blocks_delivery_and_only_owner_can_resume(self):
        pair_group(self.store)
        for number, status in enumerate(('left', 'kicked'), 1):
            self.store.accept(member_update(number, status=status))
            self.assertEqual(self.store.get('setup_problem'), 'removed')
            self.store.enqueue_report(HOME, 'A result')
            self.assertIsNone(self.store.pending_delivery())
        self.store.accept(member_update(3, actor=8))
        self.assertEqual(self.store.get('setup_problem'), 'removed')
        self.store.accept(member_update(4))
        self.assertIsNone(self.store.get('setup_problem'))
        self.assertIsNotNone(self.store.pending_delivery())

    async def test_migration_moves_topics_and_references_without_second_group_leave(self):
        pair_group(self.store)
        message = self.store.message_save(HOME, 'owner', 'Held work')
        self.store.enqueue_report(HOME, 'A result')
        self.assertEqual(self.store.accept(group_update(1, thread=None, migrate_to_chat_id=-200)), 'service_event')
        self.assertEqual(self.store.chat(), -200)
        self.assertEqual(self.store.get('control_topic'), '-200:10')
        self.assertEqual(self.store.messages_pending()[0]['topic'], '-200:10')
        self.assertEqual(self.store.pending_delivery()['chat'], -200)
        self.assertEqual(self.store.accept(group_update(2, thread=None, migrate_from_chat_id=CHAT,
                         chat={'id': -200, 'type': 'supergroup'})), 'service_event')
        self.assertEqual(self.store.db.execute('PRAGMA foreign_key_check').fetchall(), [])
        self.assertFalse(any(method == 'leaveChat' for method, _ in self.telegram.calls))

    async def test_forum_missing_returns_to_topics_screen(self):
        await self.pair()
        self.telegram.create_error = TelegramError(400, reason='topics_off')
        await tick(self.service)
        self.assertIn(TOPICS_OFF, self.texts())
        self.assertIsNone(self.store.get('control_topic'))
        self.assertEqual(_reason({'description': 'CHANNEL_FORUM_MISSING'}), 'topics_off')

    async def test_owner_events_pause_resume_and_changed_owner_does_not_pause(self):
        pair_group(self.store)
        self.store.put('execution', 'agents')
        for number, extra in ((1, {'left_chat_member': {'id': 7}}), (3, {'chat_owner_left': {}})):
            self.store.accept(group_update(number, **extra))
            self.assertEqual(self.store.get('setup_problem'), 'owner_left')
            self.assertFalse(await self.service.feed_once())
            self.assertFalse(await self.service.workers_once())
            self.assertFalse(await self.service.tldr_once())
            self.assertIn(OWNER_LEFT, self.texts())
            self.store.accept(group_update(number + 1, new_chat_members=[{'id': 7}]))
            self.assertIsNone(self.store.get('setup_problem'))
            self.assertIn('Resumed.', self.texts())
        self.store.accept(group_update(5, chat_owner_changed={'new_owner': {'id': 8}}))
        self.assertIsNone(self.store.get('setup_problem'))
        self.assertEqual(self.store.get('group_owner_changed'), 'The group now has a different Telegram owner')

    async def test_first_request_moves_to_new_project_topic(self):
        pair_group(self.store)
        self.store.put('projects_root', str(self.root))
        self.assertEqual(self.store.accept(group_update(1, 'build a website')), 'held')
        self.store.put('project_setup:' + HOME, dict(self.store.get('project_setup:' + HOME), name_armed=True))
        self.assertEqual(self.store.accept(group_update(2, 'Website')), 'control')
        await tick(self.service)
        project = self.store.topic('-10042:11')
        self.assertEqual((project['name'], project['enabled']), ('Website', 1))
        self.assertTrue((Path(project['cwd']) / '.git').is_dir())
        self.assertEqual(self.store.messages_pending()[0]['text'], 'build a website')
        self.assertEqual(self.store.messages_pending()[0]['topic'], project['id'])
        self.assertEqual(self.store.topic(HOME)['cwd'], '')

    async def test_project_new_in_general_creates_project_topic(self):
        pair_group(self.store)
        self.store.put('projects_root', str(self.root))
        self.assertEqual(self.store.accept(group_update(1, '/project new Website', thread=None)), 'control')
        await tick(self.service)
        self.assertEqual(self.store.topic('-10042:11')['name'], 'Website')

    async def test_project_new_keeps_pending_work_in_existing_project(self):
        pair_group(self.store)
        self.store.put('projects_root', str(self.root))
        source = '-10042:20'
        self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                             (source, CHAT, 20, 'Old', str(self.root)))
        self.store.message_save(source, 'owner', 'Work for Old')
        self.assertEqual(self.store.accept(group_update(1, '/project new Website', thread=20)), 'control')
        await tick(self.service)
        self.assertEqual(self.store.messages_pending()[0]['topic'], source)
        self.assertEqual(self.store.topic('-10042:11')['name'], 'Website')

    async def test_malformed_pairing_and_callback_do_not_call_telegram(self):
        self.store.pairing_code()
        malformed = group_update(1, '/start ' + self.store.pairing_code(), thread=None)
        malformed['message']['chat'].pop('id')
        self.assertEqual(await self.service.accept_update(malformed), 'ignored')
        callback = {'update_id': 2, 'callback_query': {'id': 'bad', 'from': {'id': 7},
                    'message': {'message_id': 1, 'chat': {'id': CHAT, 'type': 'supergroup'}}, 'data': 1}}
        self.assertEqual(await self.service.accept_update(callback), 'unauthorized')
        self.assertEqual(self.telegram.calls, [])

    async def test_deleted_project_and_deleted_home_recovery(self):
        pair_group(self.store)
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('-10042:20',-10042,20,'Website','',1)")
        self.store.enqueue_report('-10042:20', 'Done')
        self.telegram.send = AsyncMock(side_effect=TelegramError(400, reason='thread_not_found'))
        self.assertTrue(await self.service.deliver_once())
        self.assertEqual(self.store.topic('-10042:20')['enabled'], 0)
        self.assertIn('The topic for **Website** was deleted. Use /project new <name> to make a new one.', self.texts())
        self.assertTrue(await self.service.deliver_once())
        self.assertIsNone(self.store.get('control_topic'))
        await tick(self.service)
        self.assertEqual(self.store.get('control_topic'), '-10042:11')

    async def test_general_guidance_is_owner_only_and_rate_limited(self):
        pair_group(self.store)
        self.store.accept(group_update(1, 'hello', thread=None, user=8))
        self.assertNotIn(GENERAL, self.texts())
        self.store.accept(group_update(2, 'hello', thread=None))
        self.store.accept(group_update(3, 'hello again', thread=None))
        self.assertEqual(self.texts().count(GENERAL), 1)

    async def test_membership_validation_and_transport_allowed_updates(self):
        self.assertIsNone(update_problem(member_update(1)))
        malformed = member_update(2)
        malformed['my_chat_member']['new_chat_member']['user']['id'] = '99'
        self.assertIsNotNone(update_problem(malformed))
        api = Telegram.__new__(Telegram)
        api._call = lambda method, data, timeout: data
        result = await api.updates(0)
        self.assertIn('my_chat_member', result['allowed_updates'])
