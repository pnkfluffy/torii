import asyncio
from contextlib import redirect_stdout
import io
from itertools import permutations
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, Mock, patch

from coordinator import __main__ as cli
from coordinator.service import Service
from coordinator.session import AccountsUnavailable
from coordinator.setup_flow import TOPICS_OFF, tick
from coordinator.store import Store
from coordinator.telegram import TelegramError
from tests.test_group_setup import CHAT, GroupTelegram, group_update, member_update
from tests.test_service import FakeRunner, FakeSession
from tests.test_setup_installer import INSTALLER


HOME = '-10042:4'
PROJECT = '-10042:5'


def live_shape(store, root):
    with store.db:
        store.db.execute('DELETE FROM settings')
        for key, value in {'account_signin': None, 'bot_username': 'test_bot',
                           'coordinator_home_topic': HOME, 'forced_pair_only': False,
                           'group': CHAT, 'mode': 'group', 'owner': 7,
                           'pair_only': False, 'pairing': None}.items():
            store.put(key, value)
        for topic, thread, name in ((HOME, 4, 'Main'), (PROJECT, 5, 'Project')):
            store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                             (topic, CHAT, thread, name, str(root)))


class GroupSetupRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.home = patch('pathlib.Path.home', return_value=self.root)
        self.home.start()
        self.transport = patch('coordinator.telegram.Telegram._request', side_effect=AssertionError('No Telegram API'))
        self.transport.start()
        self.store = Store(self.root / 'state')
        self.telegram = GroupTelegram()
        self.service = Service(self.store, self.telegram, FakeRunner(), self.root)

    async def asyncTearDown(self):
        checking = getattr(self.service, 'setup_accounts_task', None)
        if checking:
            checking.cancel()
            await asyncio.gather(checking, return_exceptions=True)
        self.store.close()
        self.transport.stop()
        self.home.stop()
        self.temp.cleanup()

    def snapshot(self):
        return ([tuple(row) for row in self.store.db.execute('SELECT key,value FROM settings ORDER BY key')],
                [tuple(row) for row in self.store.db.execute('SELECT * FROM topics ORDER BY id')])

    async def setup_ticks(self, count=2, reset_checks=True):
        ticks = 0

        async def pause(*args):
            nonlocal ticks
            ticks += 1
            if reset_checks:
                self.service.group_next_check = 0
            await asyncio.sleep(0)
            if ticks == count:
                raise asyncio.CancelledError()

        with patch('coordinator.service.pause', pause):
            with self.assertRaises(asyncio.CancelledError):
                await self.service.supervise('group_setup', self.service.group_setup)
        self.assertEqual(ticks, count)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM problems WHERE code='loop-crashed'").fetchone()[0], 0)

    async def test_live_home_survives_two_service_setup_ticks_without_rights_poll(self):
        live_shape(self.store, self.root)
        self.assertEqual({row[0] for row in self.store.db.execute('SELECT key FROM settings')},
                         {'account_signin', 'bot_username', 'coordinator_home_topic', 'forced_pair_only',
                          'group', 'mode', 'owner', 'pair_only', 'pairing'})
        before = self.snapshot()[1]
        for rights in (True, False):
            with self.subTest(rights=rights):
                self.telegram.rights = rights
                await self.setup_ticks()
                self.assertEqual(self.telegram.calls, [])
                self.assertIsNone(self.store.get('setup_problem'))
                self.assertEqual(self.snapshot()[1], before)
                self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 0)
        self.assertEqual(await self.service.accept_update(group_update(101, 'status please', thread=4)), 'queued')
        self.assertEqual(self.service.home_topic(), HOME)
        self.assertEqual([(row['topic'], row['text']) for row in self.store.messages_pending()], [(HOME, 'status please')])
        self.assertIsNone(self.store.get('execution'))
        self.assertIsNone(self.store.get('bot_id'))

    async def test_reinstall_live_home_preserves_pairing_and_topics(self):
        live_shape(self.store, self.root)
        before = self.snapshot()
        unexpected = Mock(side_effect=AssertionError('Already set up'))
        globals_ = INSTALLER['setup'].__globals__
        with patch.dict(os.environ, {'TORII_STATE_DIR': str(self.root / 'state')}), \
                patch.dict(globals_, service_running=Mock(return_value=True), status=Mock(),
                           install=unexpected, check_prereqs=unexpected, check_bot=unexpected,
                           obtain_token=unexpected, wait_running=unexpected), \
                patch.object(Store, 'pairing_code', unexpected), redirect_stdout(io.StringIO()) as output:
            self.assertEqual(await INSTALLER['setup'](), 0)
        self.assertNotIn('startgroup=', output.getvalue())
        self.assertIsNone(self.store.get('execution'))
        self.assertEqual(self.snapshot()[1], before[1])
        for key in ('owner', 'group', 'coordinator_home_topic', 'pairing'):
            self.assertEqual(self.store.get(key), {'owner': 7, 'group': CHAT, 'coordinator_home_topic': HOME, 'pairing': None}[key])

    async def test_project_new_in_home_while_claude_unavailable(self):
        live_shape(self.store, self.root)
        with self.store.db:
            self.store.put('projects_root', str(self.root / 'projects'))
        (self.root / 'projects').mkdir()
        sessions = {}
        unavailable = True

        async def factory(store, runner, cwd, model, instructions, topic=None):
            if unavailable:
                raise AccountsUnavailable(time.time() + 0.3)
            session = FakeSession('fake-session-' + topic)
            sessions[topic] = session
            return session

        self.service.session_factory = factory
        await tick(self.service)
        self.assertEqual(await self.service.accept_update(group_update(201, 'please fix the build in project 9', thread=4)), 'queued')
        try:
            await self.service.feed_once()
            self.assertFalse(sessions)
            self.assertEqual(await self.service.accept_update(group_update(202, '/project new Foo', thread=4)), 'control')
            await self.service.controls_once()
            await tick(self.service)
            self.assertIsNotNone(self.store.topic(str(CHAT) + ':' + str(self.telegram.thread)))
            unavailable = False
            with self.store.db:
                self.store.put('coordinator_account_retry_at', None)
            self.service.parent_retry.clear()
            await self.service.feed_once()
            await asyncio.gather(*self.service.feed_tasks.values())
            pending = self.store.db.execute("SELECT topic FROM messages WHERE text='please fix the build in project 9'").fetchone()
            receivers = [topic for topic, session in sessions.items()
                         if any('fix the build' in text for _, text in session.sent)]
            self.assertEqual(pending['topic'], HOME)
            self.assertEqual(receivers, [HOME])
        finally:
            await asyncio.gather(*self.service.feed_tasks.values())
            for session in sessions.values():
                await session.stop()

    def test_pair_replace_defaults_to_no_and_requires_tty_or_yes(self):
        live_shape(self.store, self.root)
        before = self.snapshot()
        args = cli.parser().parse_args(['pair', '--replace'])
        for tty, response in ((True, ''), (True, 'n'), (False, 'y')):
            with self.subTest(tty=tty, response=response), patch('sys.stdin.isatty', return_value=tty), \
                    patch('builtins.input', return_value=response) as prompt, redirect_stdout(io.StringIO()):
                self.assertEqual(cli.pair(args, self.store), 1)
                if tty:
                    prompt.assert_called_once_with('Move this install to a group now? Your projects and history stay. [y/N] ')
                else:
                    prompt.assert_not_called()
                self.assertEqual(self.snapshot(), before)

    def test_pair_replace_yes_allows_noninteractive_replacement(self):
        live_shape(self.store, self.root)
        args = cli.parser().parse_args(['pair', '--replace', '--yes'])
        with patch('sys.stdin.isatty', return_value=False), patch('builtins.input', side_effect=AssertionError('No prompt')), \
                redirect_stdout(io.StringIO()):
            self.assertEqual(cli.pair(args, self.store), 0)
        self.assertIsNone(self.store.get('owner'))
        self.assertIsNone(self.store.chat())
        self.assertEqual(self.store.get('execution'), 'pairing')
        self.assertTrue(self.store.get('pairing'))
        self.assertFalse(any(row['enabled'] for row in self.store.topics()))

    async def test_account_check_without_control_topic_does_not_restart_setup_loop(self):
        with self.store.db:
            self.store.put('owner', 7)
            self.store.put('group', CHAT)
            self.store.put('mode', 'group')
            self.store.put('setup_requests', {'check_accounts': time.time()})
        self.telegram.forum = False
        with patch('coordinator.account_status.check_accounts', AsyncMock(side_effect=RuntimeError('Synthetic check failure'))), \
                patch('coordinator.codex_accounts.refresh_codex_accounts', AsyncMock()):
            await self.setup_ticks(4, reset_checks=False)
        self.assertFalse(self.store.get('setup_requests').get('check_accounts'))
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM problems WHERE code='setup-check-failed'").fetchone()[0], 1)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox WHERE topic IS NULL').fetchone()[0], 0)

    async def test_topics_off_keeps_output_and_delivers_after_topics_return(self):
        live_shape(self.store, self.root)
        output = [self.store.enqueue_report(PROJECT, text) for text in ('Result one', 'Result two')]
        self.telegram.send = AsyncMock(side_effect=TelegramError(400, reason='topics_off'))
        self.assertTrue(await self.service.deliver_once())
        self.assertTrue(all(self.store.topic(topic)['enabled'] for topic in (HOME, PROJECT)))
        self.assertEqual(self.store.get('setup_problem'), 'topics_off')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox WHERE retired=1').fetchone()[0], 0)
        self.assertIn(TOPICS_OFF, [row[0] for row in self.store.db.execute('SELECT text FROM outbox')])
        self.telegram.send = AsyncMock(return_value={'message_id': 900})
        self.assertTrue(await self.service.deliver_once())
        self.assertEqual(self.telegram.send.call_args.args[0]['thread'], 0)
        self.assertFalse(await self.service.deliver_once())
        self.store.put('group_check_requested', True)
        await tick(self.service)
        self.assertIsNone(self.store.get('setup_problem'))
        for message in (901, 902):
            self.telegram.send.return_value = {'message_id': message}
            self.assertTrue(await self.service.deliver_once())
        rows = self.store.db.execute('SELECT id,delivered,retired FROM outbox WHERE topic=? ORDER BY id', (PROJECT,)).fetchall()
        self.assertEqual([tuple(row) for row in rows], [(row, 1, 0) for row in output])

    async def test_pair_lookup_failure_consumes_update_keeps_code_and_allows_retry(self):
        code = self.store.pairing_code()
        pairing = self.store.get('pairing')
        call = self.telegram.call

        async def failing(method, **params):
            if method == 'getChatMember':
                raise TelegramError(400)
            return await call(method, **params)

        self.telegram.call = failing
        self.assertEqual(await self.service.accept_update(group_update(1, '/start ' + code, thread=None)), 'pair_retry')
        self.assertEqual(self.store.get('pairing'), pairing)
        self.assertIsNotNone(self.store.db.execute('SELECT 1 FROM updates WHERE id=1').fetchone())
        self.assertEqual(self.store.get('offset'), 2)
        self.assertEqual(await self.service.accept_update(group_update(2, 'hello', user=8)), 'unauthorized')
        self.assertEqual(self.store.get('offset'), 3)
        notice = self.store.pending_delivery()
        self.assertEqual((notice['chat'], notice['thread']), (CHAT, 0))
        self.assertIn('/start again', notice['text'])
        self.telegram.call = call
        self.assertEqual(await self.service.accept_update(group_update(3, '/start ' + code, thread=None)), 'paired')

    async def test_basic_group_upgrade_handles_membership_before_migration(self):
        await self.upgrade(('removed', 'joined', 'to', 'from'))

    async def test_basic_group_upgrade_handles_migration_before_membership(self):
        await self.upgrade(('from', 'to', 'removed', 'joined'))

    async def test_basic_group_upgrade_handles_all_event_orders(self):
        for index, order in enumerate(permutations(('removed', 'joined', 'to', 'from'))):
            with self.subTest(order=order):
                self.store.close()
                self.store = Store(self.root / ('state-' + str(index)))
                self.telegram = GroupTelegram()
                self.service = Service(self.store, self.telegram, FakeRunner(), self.root)
                await self.upgrade(order)

    async def upgrade(self, order):
        new = -200
        code = self.store.pairing_code()
        self.telegram.forum = False
        basic = {'id': CHAT, 'type': 'group'}
        self.assertEqual(await self.service.accept_update(group_update(1, '/start ' + code, thread=None, chat=basic)), 'paired')
        await tick(self.service)
        with self.store.db:
            self.store.db.execute("DELETE FROM settings WHERE key='group_type'")
        events = {'removed': member_update(2, status='left', forum=False),
                  'joined': member_update(3, chat=new),
                  'to': group_update(4, thread=None, chat=basic, migrate_to_chat_id=new),
                  'from': group_update(5, thread=None, chat={'id': new, 'type': 'supergroup', 'is_forum': True}, migrate_from_chat_id=CHAT)}
        events['removed']['my_chat_member']['chat']['type'] = 'group'
        self.telegram.forum = True
        for number, name in enumerate(order, 2):
            event = dict(events[name], update_id=number)
            await self.service.accept_update(event)
            await tick(self.service)
            self.assertFalse(any(method == 'leaveChat' for method, _ in self.telegram.calls))
        self.assertEqual(self.store.chat(), new)
        self.assertIsNone(self.store.get('setup_problem'))
        self.assertTrue(self.store.get('control_topic').startswith(str(new) + ':'))
        self.assertEqual(self.store.get('owner'), 7)
        self.assertEqual(self.store.db.execute('PRAGMA foreign_key_check').fetchall(), [])
