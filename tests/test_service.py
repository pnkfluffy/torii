import asyncio
import json
from pathlib import Path
import re
from types import SimpleNamespace
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch
import uuid

from coordinator import extension
from coordinator.control_api import BRIEF_LIMIT
from coordinator.native_protocol import NativeProtocol, steer_wire
from coordinator.providers import RunResult
from coordinator.reactions import deliver_reaction
from coordinator.service import RESTART_GUARD_SECONDS, Service, _Restart, boot_time
from coordinator.session import message_tag, message_uuid
from coordinator.store import Store
from coordinator.telegram import TelegramError
from coordinator.workers import steer_outcome
from tests.support import restart_work_rows, stop_test_hosts


TOPIC = '-10042:4'


def tag(message, kind='owner', topic=TOPIC, name='Torii'):
    return '[topic=%s name="%s" message=%d kind=%s] ' % (topic, name, message, kind)


def owner_update(update_id, text, reply=None):
    message = {'message_id': update_id, 'message_thread_id': 4, 'text': text,
               'from': {'id': 7}, 'chat': {'id': -10042, 'type': 'supergroup'}}
    if reply is not None:
        message['reply_to_message'] = {'message_id': reply}
    return {'update_id': update_id, 'message': message}


class FakeTelegram:
    def __init__(self):
        self.sent = []
        self.calls = []
        self.pending_updates = []

    async def send(self, row):
        self.sent.append(dict(row))
        return {'message_id': 1000 + len(self.sent)}

    async def call(self, method, **params):
        self.calls.append((method, params))
        if method == 'getMe':
            return {'id': 99, 'username': 'test_bot'}
        if method == 'getChatMember':
            return {'status': 'administrator'}
        return True

    async def updates(self, offset):
        updates, self.pending_updates = self.pending_updates, []
        await asyncio.sleep(0)
        return updates


class FakeSession:
    def __init__(self, session_id):
        self.session_id = session_id
        self.closed = False
        self.sent = []
        self.hold = asyncio.Event()

    async def send(self, message_id, text, row_id=None):
        if self.closed:
            return 'closed'
        self.sent.append((message_id, text))
        if text.endswith('hold'):
            await self.hold.wait()
        return 'received'

    async def wait_closed(self):
        return None

    async def stop(self):
        self.closed = True
        self.hold.set()


class FakeRunner:
    def __init__(self):
        self.calls = []
        self.result = None
        self.extension = extension.active()

    async def run(self, provider, prompt, cwd, session, **options):
        self.calls.append((provider, prompt, str(cwd), session, options))
        if self.result:
            return self.result
        return RunResult(session, text='finished', success=True)


class ServiceTestsSupport:
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)
        self.addCleanup(stop_test_hosts, self.root)
        self.store = Store(self.root / 'state')
        with self.store.db:
            self.store.put('owner', 7)
            self.store.put('group', -10042)
            self.store.put('coordinator_home_topic', TOPIC)
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', (TOPIC, -10042, 4, 'Torii', str(self.root)))
        self.telegram = FakeTelegram()
        self.runner = FakeRunner()
        self.sessions = []

        async def factory(store, runner, cwd, model, instructions, topic=None):
            key = 'coordinator' if topic == TOPIC else 'coordinator:' + topic
            sid = store.get(key + '_session') or str(uuid.uuid4())
            with store.db:
                store.put(key + '_session', sid)
                store.put(key + '_fresh', False)
            session = FakeSession(sid)
            self.sessions.append(session)
            return session

        self.service = Service(self.store, self.telegram, self.runner, self.root, session_factory=factory)

    async def asyncTearDown(self):
        for session in self.sessions:
            await session.stop()
        for task in self.service.feed_tasks.values():
            task.cancel()
        if self.service.feed_tasks:
            await asyncio.gather(*self.service.feed_tasks.values(), return_exceptions=True)
        self.store.close()

    def lose_next_resume(self, topic):
        factory = self.service.session_factory

        async def lost(store, runner, cwd, model, instructions, topic=None):
            if topic == lost_topic:
                self.service.session_factory = factory
                key = 'coordinator' if topic == TOPIC else 'coordinator:' + topic
                with store.db:
                    store.put(key + '_session', None)
                session = await factory(store, runner, cwd, model, instructions, topic=topic)
                session.resume_failed = True
                return session
            return await factory(store, runner, cwd, model, instructions, topic=topic)

        lost_topic = topic
        self.service.session_factory = lost

    async def drain_feed(self):
        await self.service.feed_once()
        if self.service.feed_tasks:
            await asyncio.gather(*self.service.feed_tasks.values())
        await self.service.feed_once()

    def worker(self, goal=None, topic=TOPIC, provider='claude'):
        task = self.store.task_create(topic, 'Build service', worktree=str(self.root))
        now = 1.0
        with self.store.db:
            row = self.store.db.execute('''INSERT INTO workers
                (task,topic,provider,prompt,cwd,workspace,goal,created,updated)
                VALUES (?,?,?,?,?,?,?,?,?)''',
                (task['id'], topic, provider, 'do the task', str(self.root),
                 json.dumps({'cwd': str(self.root)}), goal, now, now))
        return row.lastrowid

    async def announce(self, checked):
        with self.store.db:
            self.store.put('service_run', {'state': 'running', 'started': 1000.0, 'checked': checked})
        await self.service.announce_start()

    def enable_claude_account(self):
        home = self.root / 'primary'
        home.mkdir(exist_ok=True)
        with self.store.db:
            self.store.put('accounts', {'primary': {'config_dir': str(home), 'enabled': True}})
            self.store.put('account_status', {'primary': {'identity': {
                'email': 'primary@example.com', 'logged_in': True}, 'observed_at': time.time(),
                'usage': {'seven_day': {'utilization': 10, 'resets_at': time.time() + 3600}}}})

    async def assert_secret_filled_survives_restart(self, sent_before_crash):
        notice = self.store.message_save(TOPIC, 'secret_filled', 'Secret API_TOKEN is filled.')
        if sent_before_crash:
            self.store.message_sending(notice['id'])
        for number in range(12):
            row = self.store.message_save(TOPIC, 'owner', 'newer %d' % number)
            self.store.message_sending(row['id'])
        self.store.messages_uncertain()
        with self.store.db:
            self.store.put('coordinator_session', str(uuid.uuid4()))
        await self.service.start_session()
        await self.drain_feed()
        sent = [(identity, text) for identity, text in self.sessions[0].sent
                if 'kind=secret_filled' in text]
        self.assertEqual(sent, [(message_uuid(notice['id']), tag(notice['id'], 'secret_filled') + notice['text'])])
        self.assertEqual(self.store.db.execute('SELECT delivered FROM messages WHERE id=?',
                                               (notice['id'],)).fetchone()[0], 'received')

    async def run_until(self, loops, condition):
        tasks = [asyncio.create_task(loop()) for loop in loops]
        try:
            async def wait():
                while not condition():
                    await asyncio.sleep(0.01)
            await asyncio.wait_for(wait(), 2)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    def reacted(self):
        return [params['message_id'] for method, params in self.telegram.calls if method == 'setMessageReaction']

    def running_worker(self, receipt):
        worker = self.worker()
        with self.store.db:
            self.store.db.execute("UPDATE workers SET status='running' WHERE id=?", (worker,))

        class Control:
            async def steer(self, message_id, prompt):
                return receipt
        self.service.workers.controls[worker] = Control()
        return worker

    def request(self, request_id):
        row = self.store.db.execute('SELECT state,result FROM service_requests WHERE id=?', (request_id,)).fetchone()
        return row['state'], json.loads(row['result'])

    def notes(self):
        return [row[0] for row in self.store.db.execute("SELECT text FROM messages WHERE kind='callback' ORDER BY id")]

    def problems(self):
        return [dict(row) for row in self.store.db.execute(
            'SELECT area,code,detail,topic,task,worker,message FROM problems ORDER BY id')]


class ServiceTests(ServiceTestsSupport, unittest.IsolatedAsyncioTestCase):
    async def test_named_codex_reset_is_queued_then_refused_below_95_percent(self):
        from coordinator import control_api
        self.store.put('codex_accounts', {'gpt': {'config_dir': str(self.root), 'enabled': True}})
        self.store.put('codex_account_status', {'gpt': {'identity': {'logged_in': True},
                                                      'usage': {'seven_day': {'utilization': 99}}}})
        queued = control_api.call(self.store, 'account.codex_reset', {'alias': 'gpt'}, topic=TOPIC, source='telegram')
        self.assertTrue(queued.ok)
        self.assertEqual(queued.state, 'queued')
        self.assertEqual(queued.text, 'Banked reset queued. Torii will send the result here.')
        server = AsyncMock()
        server.request.side_effect = [
            {'account': {'type': 'chatgpt'}},
            {'ordinaryUsageAllowed': True, 'rateLimits': {'primary': {
                'usedPercent': 10, 'windowDurationMins': 10080}},
             'rateLimitResetCredits': {'availableCount': 2}},
        ]
        with patch('coordinator.codex_accounts.AppServer', return_value=server), \
                patch('coordinator.codex_accounts.link_home', side_effect=lambda directory: directory):
            self.assertTrue(await self.service.controls_once())
            self.assertFalse(await self.service.controls_once())
        request = self.store.db.execute('SELECT state,result FROM service_requests WHERE id=?',
                                        (queued.data['request'],)).fetchone()
        notice = 'This Codex account is below 95% on its resettable meters'
        self.assertEqual((request['state'], json.loads(request['result'])), ('refused', {'text': notice}))
        self.assertEqual([row[0] for row in self.store.db.execute('SELECT text FROM outbox')], [notice])
        self.assertEqual([entry.args[0] for entry in server.request.call_args_list],
                         ['account/read', 'account/rateLimits/read'])
        server.stop.assert_awaited_once()

    async def test_each_start_installs_the_seven_commands_for_the_owner(self):
        from coordinator.onboarding import COMMAND_MENU
        self.service.poll_succeeded = True
        for _ in range(2):
            with patch('coordinator.service.asyncio.sleep', side_effect=asyncio.CancelledError):
                with self.assertRaises(asyncio.CancelledError):
                    await self.service.command_menu()
        self.assertEqual(self.telegram.calls, [
            ('setMyCommands', {'commands': COMMAND_MENU, 'scope': {
                'type': 'chat_member', 'chat_id': -10042, 'user_id': 7}}),
        ] * 2)

    async def test_provider_switch_queues_continuity_and_one_home_notice(self):
        saved = str(uuid.uuid4())
        self.store.put('coordinator_session', saved)
        session = FakeSession(str(uuid.uuid4()))
        session.switched = True
        session.switch = {'from': 'claude', 'to': 'codex', 'reason': 'every Claude account is out of usage'}
        self.service.session_factory = AsyncMock(return_value=session)
        await self.service.start_session(TOPIC)
        notes = [row['text'] for row in self.store.messages_pending() if row['kind'] == 'restarted']
        self.assertEqual(len(notes), 1)
        self.assertIn('moved from Claude to ChatGPT', notes[0])
        self.assertIn('Channel continuity:', notes[0])
        self.assertNotIn('The service restarted.', notes[0])
        await self.service.start_session(TOPIC)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM outbox WHERE text LIKE 'Main chat now runs%'"
                                              ).fetchone()[0], 1)

    async def test_pause_notice_reaches_each_waiting_topic_once_and_available_parent_bypasses_retry(self):
        from coordinator.session import AccountsUnavailable
        other = '-10042:5'
        self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                              (other, -10042, 5, 'Other', str(self.root)))
        self.service.session_factory = AsyncMock(side_effect=AccountsUnavailable(time.time() + 3600, provider='both'))
        self.assertIsNone(await self.service.start_session(TOPIC))
        self.assertIsNone(await self.service.start_session(other))
        self.assertIsNone(await self.service.start_session(other))
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM outbox WHERE text LIKE 'The main chat is paused:%'"
                                              ).fetchone()[0], 2)
        self.service.session_factory = AsyncMock(return_value=FakeSession('resumed'))
        with patch.object(self.service, 'parent_available', return_value=True):
            self.assertIsNotNone(await self.service.start_session(other))
        self.assertIsNone(self.store.get('coordinator_account_retry_at'))

    def test_pause_notice_names_a_signed_out_chatgpt_pick(self):
        from coordinator import parent
        for alias in ('old', 'new'):
            profile = self.root / alias
            profile.mkdir()
            self.store.put('codex_accounts', {**self.store.get('codex_accounts', {}), alias: {
                'enabled': True, 'config_dir': str(profile)}})
        self.store.put('codex_account_status', {
            'old': {'identity': {'logged_in': False}, 'error': 'login_required'},
            'new': {'identity': {'email': 'new@example.test', 'logged_in': True}}})
        self.store.put('codex_active_account', 'old')
        self.service.pause_parent(TOPIC, parent.unavailable(
            self.store, self.service.accounts, self.service.codex_accounts))
        self.assertEqual(self.service.codex_accounts.active(), 'old')
        self.assertIsNone(self.service.codex_accounts.parent_account())
        text = self.store.get('coordinator_account_notice')['text']
        self.assertIn('ChatGPT account old is signed out or turned off', text)
        self.assertIn('Add it again or pick another account with /accounts', text)
        self.assertIn('Your messages are saved.', text)
        self.assertNotIn('unknown', text)

    def test_pause_notice_names_a_disabled_chatgpt_pick(self):
        from coordinator import parent
        profile = self.root / 'gpt'
        profile.mkdir()
        self.store.put('codex_accounts', {'gpt': {'enabled': False, 'config_dir': str(profile)}})
        self.store.put('codex_account_status', {
            'gpt': {'identity': {'email': 'gpt@example.test', 'logged_in': True}}})
        self.store.put('codex_active_account', 'gpt')
        self.service.pause_parent(TOPIC, parent.unavailable(
            self.store, self.service.accounts, self.service.codex_accounts))
        text = self.store.get('coordinator_account_notice')['text']
        self.assertIn('ChatGPT account gpt@example.test is signed out or turned off', text)
        self.assertIn('Add it again or pick another account with /accounts', text)
        self.assertIsNone(self.service.codex_accounts.parent_account())

    def test_pause_notice_changes_when_the_same_chatgpt_pick_runs_out_of_usage(self):
        from coordinator import parent
        for alias in ('old', 'new'):
            profile = self.root / alias
            profile.mkdir()
            self.store.put('codex_accounts', {**self.store.get('codex_accounts', {}), alias: {
                'enabled': True, 'config_dir': str(profile)}})
        statuses = {
            'old': {'identity': {'logged_in': False}, 'error': 'login_required'},
            'new': {'identity': {'email': 'new@example.test', 'logged_in': True}}}
        self.store.put('codex_account_status', statuses)
        self.store.put('codex_active_account', 'old')
        error = parent.unavailable(self.store, self.service.accounts, self.service.codex_accounts)
        self.service.pause_parent(TOPIC, error)
        self.assertIn('signed out or turned off', self.store.get('coordinator_account_notice')['text'])
        statuses['old'] = {'identity': {'email': 'old@example.test', 'logged_in': True}, 'usage_allowed': False}
        self.store.put('codex_account_status', statuses)
        next_error = parent.unavailable(self.store, self.service.accounts, self.service.codex_accounts)
        self.assertEqual((error.provider, error.alias, error.reset_at),
                         (next_error.provider, next_error.alias, next_error.reset_at))
        self.service.pause_parent(TOPIC, next_error)
        text = self.store.get('coordinator_account_notice')['text']
        self.assertIn('ChatGPT account old@example.test is out of usage', text)
        self.assertNotIn('signed out or turned off', text)
        self.service.pause_parent(TOPIC, next_error)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM outbox WHERE text LIKE 'The main chat is paused:%'"
                                              ).fetchone()[0], 2)

    def test_pause_notices_omit_unknown_resets_and_keep_known_resets(self):
        from coordinator.session import AccountsUnavailable
        profile = self.root / 'gpt'
        profile.mkdir()
        self.store.put('codex_accounts', {'gpt': {'enabled': True, 'config_dir': str(profile)}})
        self.store.put('codex_account_status', {'gpt': {
            'identity': {'email': 'gpt@example.test', 'logged_in': True}, 'usage_allowed': False}})
        for reset in (None, time.time() + 3600):
            for provider in ('claude', 'both', 'codex'):
                with self.subTest(provider=provider, reset=reset):
                    self.service.pause_parent(TOPIC, AccountsUnavailable(reset, provider=provider, alias='gpt'))
                    text = self.store.get('coordinator_account_notice')['text']
                    self.assertNotIn('unknown', text)
                    self.assertIn('Your messages are saved.', text)
                    self.assertEqual('Earliest reset:' in text, reset is not None and provider != 'codex')
                    self.assertEqual('until ' in text, reset is not None and provider == 'codex')

    def test_codex_transcript_confirms_an_uncertain_message(self):
        from coordinator.codex_session import codex_transcript_receipts
        row = self.store.message_save(TOPIC, 'owner', 'request')
        self.store.message_sending(row['id'])
        self.store.message_delivered(row['id'], 'uncertain')
        path = self.root / 'rollout.jsonl'
        path.write_text(json.dumps({'type': 'event_msg', 'payload': {'type': 'user_message',
            'message': tag(row['id']) + 'request'}}) + '\n')
        session = SimpleNamespace(transcript_path=path, transcript_receipts=codex_transcript_receipts)
        self.service.confirm_from_transcript(TOPIC, session)
        receipt = self.store.db.execute('SELECT receipt FROM messages WHERE id=?', (row['id'],)).fetchone()[0]
        self.assertEqual(receipt, 'transcript')

    async def test_policy_edit_reaches_existing_parent_on_next_message(self):
        original = self.service.session_factory
        launched = []

        async def record(store, runner, cwd, model, instructions, topic=None):
            launched.append(instructions)
            return await original(store, runner, cwd, model, instructions, topic=topic)

        self.service.session_factory = record
        self.store.message_save(TOPIC, 'owner', 'first')
        await self.service.feed_once()
        await asyncio.gather(*self.service.feed_tasks.values())
        self.assertNotIn('Current usage policy:', launched[0])
        self.assertIn('Registered channels:', launched[0])
        self.assertEqual(self.sessions[0].sent[0][1], tag(1) + 'first')
        (self.store.directory / 'USAGE.md').write_text('Use one worker.')
        self.store.message_save(TOPIC, 'owner', 'second')
        await self.service.feed_once()
        await asyncio.gather(*self.service.feed_tasks.values())
        self.assertIn('Current usage policy:\nUse one worker.', self.sessions[0].sent[-1][1])

    async def test_last_usable_account_warning_is_once_per_reset_and_clears_with_spare(self):
        now = time.time()
        accounts = {}
        status = {}
        for alias, used, reset in (('first', 83, now + 600), ('second', 95, now + 300)):
            directory = self.root / alias
            directory.mkdir()
            accounts[alias] = {'config_dir': str(directory), 'enabled': True}
            status[alias] = {'identity': {'logged_in': True, 'email': alias + '@example.com'},
                             'observed_at': now,
                             'usage': {'five_hour': {'utilization': used, 'resets_at': reset}}}
        with self.store.db:
            self.store.put('accounts', accounts)
            self.store.put('account_status', status)
        self.service.notice_last_account()
        self.service.notice_last_account()
        reports = list(self.store.db.execute("SELECT topic, text FROM outbox WHERE text LIKE 'Heads-up:%'"))
        self.assertEqual(len(reports), 1)
        self.assertEqual(reports[0]['topic'], TOPIC)
        self.assertIn('first@example.com', reports[0]['text'])
        self.assertIn('83% of its 5-hour limit', reports[0]['text'])
        self.assertIn('JST', reports[0]['text'])
        status['first']['usage']['five_hour']['resets_at'] = now + 5 * 60 * 60
        with self.store.db:
            self.store.put('account_status', status)
        self.service.notice_last_account()
        self.service.notice_last_account()
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM outbox WHERE text LIKE 'Heads-up:%'").fetchone()[0], 2)
        status['second']['usage']['five_hour']['utilization'] = 10
        with self.store.db:
            self.store.put('account_status', status)
        self.service.notice_last_account()
        self.assertNotIn('Claude', self.store.get('low_capacity_warnings', {}))
        status['second']['usage']['five_hour']['utilization'] = 95
        with self.store.db:
            self.store.put('account_status', status)
        self.service.notice_last_account()
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM outbox WHERE text LIKE 'Heads-up:%'").fetchone()[0], 3)

    async def test_last_usable_account_warning_respects_orbit_reserve(self):
        now = time.time()
        directory = self.root / 'orbit'
        directory.mkdir()
        with self.store.db:
            self.store.put('accounts', {'orbit': {'config_dir': str(directory), 'enabled': True}})
            self.store.put('account_status', {'orbit': {'identity': {'logged_in': True,
                                                                    'email': 'orbit@example.com'},
                                                      'observed_at': now,
                                                      'usage': {'seven_day_fable': {'utilization': 60,
                                                                                   'resets_at': now + 600}}}})
        override = self.root / 'local-overrides.json'
        override.write_text(json.dumps({'account_switch_thresholds': {'orbit': .8}}))
        with patch('coordinator.accounts.LOCAL_OVERRIDES_PATH', override):
            self.service.notice_last_account()
            self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM outbox WHERE text LIKE 'Heads-up:%'").fetchone()[0], 1)
            self.assertIn('Fable weekly', self.store.db.execute("SELECT text FROM outbox WHERE text LIKE 'Heads-up:%'").fetchone()[0])
            status = self.store.get('account_status')
            status['orbit']['usage']['seven_day_fable']['utilization'] = 80
            with self.store.db:
                self.store.put('account_status', status)
            self.service.notice_last_account()
            self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM outbox WHERE text LIKE 'Heads-up:%'").fetchone()[0], 1)

    async def test_three_owner_messages_are_sent_separately_in_order(self):
        for number, text in enumerate(('first', 'second', 'third'), 1):
            self.assertEqual(self.store.accept(owner_update(number, text)), 'queued')
        await self.drain_feed()
        self.assertEqual([text for _, text in self.sessions[0].sent],
                         [tag(1) + 'first', tag(2) + 'second', tag(3) + 'third'])
        rows = list(self.store.db.execute('SELECT id,delivered,receipt FROM messages ORDER BY id'))
        self.assertEqual([row['delivered'] for row in rows], ['received'] * 3)
        self.assertEqual([row['receipt'] for row in rows], ['written'] * 3)
        self.assertEqual(len({message_id for message_id, _ in self.sessions[0].sent}), 3)

    async def test_reply_quote_and_goal_pass_to_session(self):
        self.store.accept(owner_update(1, 'original'))
        with self.store.db:
            self.store.put('control_ui:' + TOPIC, {'pending': 'policy', 'outbox': 0})
        self.assertEqual(self.store.accept(owner_update(2, '/goal finish all checks', reply=1)), 'queued')
        await self.drain_feed()
        self.assertIn('Replying to: original\n/goal finish all checks', self.sessions[0].sent[1][1])
        self.assertEqual(self.store.tasks_list(), [])

    async def test_message_arriving_during_a_turn_is_sent_before_it_finishes(self):
        self.store.accept(owner_update(1, 'hold'))
        await self.service.feed_once()
        await asyncio.sleep(0)
        self.assertEqual(len(self.sessions[0].sent), 1)
        self.store.accept(owner_update(2, 'follow'))
        await self.service.feed_once()
        await asyncio.sleep(0)
        self.assertEqual([text for _, text in self.sessions[0].sent],
                         [tag(1) + 'hold', tag(2) + 'follow'])
        self.assertEqual(self.store.db.execute("SELECT delivered FROM messages WHERE telegram_message=2").fetchone()[0],
                         'received')
        self.sessions[0].hold.set()
        await self.drain_feed()

    async def test_image_preparation_keeps_message_order(self):
        self.store.accept(owner_update(1, 'image'))
        with self.store.db:
            self.store.db.execute("UPDATE messages SET images='[]' WHERE telegram_message=1")
        preparing = asyncio.Event()
        release = asyncio.Event()

        async def slow_images(store, telegram, message):
            preparing.set()
            await release.wait()
            return []

        with patch('coordinator.service.prepare_attachments', slow_images):
            first_feed = asyncio.create_task(self.service.feed_once())
            await preparing.wait()
            self.store.accept(owner_update(2, 'later'))
            release.set()
            await first_feed
            await self.drain_feed()
        self.assertEqual([text for _, text in self.sessions[0].sent],
                         [tag(1) + 'image', tag(2) + 'later'])

    async def test_owner_files_reach_the_coordinator_and_a_failed_one_does_not_block_the_feed(self):
        downloads = []

        async def download_file(file_id, max_bytes):
            downloads.append((file_id, max_bytes))
            if file_id == 'broken':
                raise OSError('network down')
            return b'{"ok": true}'

        self.telegram.download_file = download_file
        for number, file_id, name in ((1, 'config', 'config.json'), (2, 'broken', 'notes.md')):
            update = owner_update(number, '')
            del update['message']['text']
            update['message']['document'] = {'file_id': file_id, 'file_name': name, 'mime_type': 'application/json'}
            update['message']['caption'] = 'part %d' % number
            self.assertEqual(self.store.accept(update), 'queued')
        self.store.accept(owner_update(3, 'after'))
        with patch('coordinator.media.DOWNLOAD_DELAYS', (0, 0)):
            await self.drain_feed()
        self.assertEqual(downloads, [('config', 20 * 1024 * 1024)] + [('broken', 20 * 1024 * 1024)] * 3)
        sent = [text for _, text in self.sessions[0].sent]
        self.assertEqual(len(sent), 3)
        saved = self.store.directory / 'files' / 'message-1' / 'config.json'
        self.assertTrue(sent[0].startswith(tag(1) + 'part 1\n\nOwner-provided files for this message:\n'))
        self.assertIn('%s (application/json, 12 bytes)' % saved, sent[0])
        self.assertTrue(sent[1].startswith(tag(2) + 'part 2'))
        self.assertIn('notes.md (application/json): download failed', sent[1])
        self.assertEqual(sent[2], tag(3) + 'after')
        self.assertEqual([row[0] for row in self.store.db.execute('SELECT delivered FROM messages ORDER BY id')],
                         ['received'] * 3)
        reports = [row[0] for row in self.store.db.execute('SELECT text FROM outbox ORDER BY id')]
        self.assertEqual(len(reports), 1)
        self.assertTrue(reports[0].startswith('`notes.md` could not be downloaded.'))

    async def test_worker_completion_is_saved_once_then_fed(self):
        self.enable_claude_account()
        worker = self.worker(goal='all checks pass')
        await self.service.workers_once()
        await asyncio.gather(*self.service.worker_tasks.values())
        row = self.store.db.execute('SELECT * FROM messages WHERE source_worker=?', (worker,)).fetchone()
        self.assertEqual(row['kind'], 'worker_result')
        self.assertEqual(json.loads(row['text'])['task'], 1)
        self.assertEqual(self.store.db.execute('SELECT status FROM workers WHERE id=?', (worker,)).fetchone()[0], 'done')
        self.store.worker_complete(worker, {'success': True})
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM messages WHERE source_worker=?', (worker,)).fetchone()[0], 1)
        await self.drain_feed()
        self.assertIn('"worker": 1', self.sessions[0].sent[0][1])
        self.assertEqual(self.runner.calls[0][4]['initial_steer'], '/goal all checks pass')

    async def test_waiting_worker_stays_idle_until_its_secret_card_closes(self):
        worker = self.worker()
        task = self.store.db.execute('SELECT task FROM workers WHERE id=?', (worker,)).fetchone()[0]
        self.store.task_update(task, secrets=['GITHUB_TOKEN'])
        with self.store.db:
            envelope = self.store.db.execute('''INSERT INTO envelopes(name,reason,consumer,task,topic,state,created,
                expires,updated) VALUES ('GITHUB_TOKEN','r','c',?,?,'open',1,9999999999,1)''', (task, TOPIC)).lastrowid
        await self.service.workers_once()
        await asyncio.gather(*self.service.worker_tasks.values())
        self.assertEqual(self.store.db.execute('SELECT status FROM workers WHERE id=?', (worker,)).fetchone()[0],
                         'waiting_for_secret')
        self.assertEqual(self.problems(), [])
        self.service.worker_tasks.clear()
        self.assertFalse(await self.service.workers_once())
        self.assertEqual(self.service.worker_tasks, {})
        with self.store.db:
            self.store.db.execute("UPDATE envelopes SET state='expired' WHERE id=?", (envelope,))
        self.assertTrue(await self.service.workers_once())
        await asyncio.gather(*self.service.worker_tasks.values())
        result = json.loads(self.store.db.execute('SELECT result FROM workers WHERE id=?', (worker,)).fetchone()[0])
        self.assertEqual(result['failure_code'], 'start_failed')
        self.assertIn('Secrets are not filled: GITHUB_TOKEN.', result['error'])
        self.assertEqual(self.runner.calls, [])
        self.assertEqual([(row['code'], row['detail']) for row in self.problems()],
                         [('wait-ended', 'names=GITHUB_TOKEN states=expired'), ('start_failed', result['failure_detail'])])

    async def test_every_steered_message_kind_carries_its_topic_id_and_name(self):
        self.enable_claude_account()
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', ('-10042:5', -10042, 5, 'Torii brainstorm', str(self.root)))
        await self.service.start_session()
        self.sessions[0].closed = True
        brainstorm = dict(owner_update(1, 'idea'), update_id=10)
        brainstorm['message']['message_thread_id'] = 5
        self.store.accept(brainstorm)
        worker = self.worker(topic='-10042:5')
        await self.service.workers_once()
        await asyncio.gather(*self.service.worker_tasks.values())
        self.store.accept(owner_update(2, 'home'))
        await self.drain_feed()
        brainstorm_sent = [text for _, text in self.service.sessions['-10042:5'].sent]
        self.assertTrue(brainstorm_sent[0].startswith('[topic=-10042:5'))
        self.assertIn('kind=restarted', brainstorm_sent[0])
        idea = next(text for text in brainstorm_sent if 'kind=owner' in text)
        result = next(text for text in brainstorm_sent if 'kind=worker_result' in text)
        self.assertEqual(idea, tag(1, topic='-10042:5', name='Torii brainstorm') + 'idea')
        self.assertTrue(result.startswith(tag(2, 'worker_result', '-10042:5', 'Torii brainstorm')))
        self.assertEqual(json.loads(result.split('] ', 1)[1])['worker'], worker)
        home = self.service.sessions[TOPIC]
        self.assertEqual([text for _, text in home.sent if 'kind=owner' in text], [tag(3) + 'home'])
        self.assertNotIn('kind=worker_result', ' '.join(text for _, text in home.sent))

    async def test_live_shaped_restart_notice_fits_host_request(self):
        tasks, workers = restart_work_rows(90, 50)
        stale_ids = [self.store.message_save(TOPIC, 'restarted', 'stale-notice-' + 'x' * 499987)['id']
                     for _ in range(10)]
        owner = self.store.message_save(TOPIC, 'owner', 'owner message retained')
        with self.store.db:
            self.store.db.execute("UPDATE messages SET delivered='uncertain'")
            self.store.put('coordinator_session', str(uuid.uuid4()))
        for worker in workers:
            worker_id = self.worker(goal=worker['goal'])
            with self.store.db:
                self.store.db.execute("UPDATE workers SET cwd=?,provider=?,status='running' WHERE id=?",
                                      (worker['cwd'], worker['provider'], worker_id))
        with patch.object(self.store, 'tasks_list', return_value=tasks) as listed:
            session = await self.service.start_session()
        listed.assert_called_once_with(status='open', topic=None)
        notice = dict(self.store.db.execute('SELECT * FROM messages WHERE id=?',
                                          (self.service.restart_messages[TOPIC],)).fetchone())
        prompt = notice['text']
        tagged = message_tag(dict(self.store.topic(TOPIC)), notice) + prompt
        event = NativeProtocol.user(session, message_uuid(notice['id']), tagged)
        line = (json.dumps({'op': 'write', 'line': json.dumps(event)}) + '\n').encode()
        print('live-shaped host request bytes: %d' % len(line), flush=True)
        self.assertLess(len(line), 32 * 1024)
        jobs = json.loads(prompt.split('Open jobs: ', 1)[1].split('. Registered workers:', 1)[0])
        active = json.loads(prompt.split('Registered workers: ', 1)[1].split('. Messages with', 1)[0])
        uncertain = json.loads(prompt.split('characters): ', 1)[1].split('. Inspect', 1)[0])
        self.assertEqual((jobs['count'], len(jobs['shown']), jobs['not_shown']), (90, 50, 40))
        self.assertEqual([row['id'] for row in jobs['shown']],
                         [row['id'] for row in sorted(tasks, key=lambda row: (row['updated'], row['id']),
                                                      reverse=True)[:50]])
        self.assertTrue(all(set(row) == {'id', 'topic', 'number', 'title'} for row in jobs['shown']))
        self.assertEqual((active['count'], len(active['shown']), active['not_shown']), (50, 30, 20))
        self.assertEqual([row['id'] for row in active['shown']], list(range(50, 20, -1)))
        self.assertTrue(all(set(row) == {'id', 'task', 'provider', 'status'} for row in active['shown']))
        self.assertEqual(uncertain, {'count': 1, 'shown': [{key: owner[key] for key in ('id', 'topic', 'kind', 'text')}], 'not_shown': 0})
        self.assertTrue(all(row['id'] not in stale_ids for row in uncertain['shown']))
        self.assertIn('tasks.get', prompt)
        self.assertIn('tasks.list', prompt)
        self.assertIn('workers.list', prompt)

    async def test_reattach_notice_excludes_only_stale_restarted_rows(self):
        stale = self.store.message_save(TOPIC, 'restarted', 'stale restarted content')
        owner = self.store.message_save(TOPIC, 'owner', 'uncertain owner content')
        result = self.store.message_save(TOPIC, 'worker_result', 'uncertain worker content')
        with self.store.db:
            self.store.db.execute("UPDATE messages SET delivered='uncertain'")
            self.store.put('coordinator_session', str(uuid.uuid4()))
        await self.service.start_session()
        prompt = self.store.db.execute('SELECT text FROM messages WHERE id=?',
                                      (self.service.restart_messages[TOPIC],)).fetchone()[0]
        summary = json.loads(prompt.split('characters): ', 1)[1].split('. Inspect', 1)[0])
        self.assertEqual(summary['count'], 2)
        self.assertEqual(summary['not_shown'], 0)
        self.assertEqual([row['id'] for row in summary['shown']], [result['id'], owner['id']])
        self.assertNotIn(stale['text'], prompt)

    async def test_closed_session_resumes_same_id_and_sends_restart_first(self):
        await self.service.start_session()
        saved = self.sessions[0].session_id
        self.sessions[0].closed = True
        self.store.accept(owner_update(1, 'continue'))
        await self.drain_feed()
        self.assertEqual(len(self.sessions), 2)
        self.assertEqual(self.sessions[1].session_id, saved)
        self.assertIn('service restarted', self.sessions[1].sent[0][1].lower())
        self.assertEqual(self.sessions[1].sent[1][1], tag(1) + 'continue')

    async def test_idle_parent_winds_down_and_resumes_in_its_channel(self):
        other = '-10042:5'
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', (other, -10042, 5, 'Ideas', str(self.root / 'ideas')))
        self.store.message_save(TOPIC, 'owner', 'home')
        self.store.message_save(other, 'owner', 'idea')
        await self.drain_feed()
        home, ideas = self.service.sessions[TOPIC], self.service.sessions[other]
        saved = ideas.session_id
        with patch('coordinator.service.PARENT_IDLE', 0):
            await self.service.feed_once()
        self.assertTrue(home.closed)
        self.assertTrue(ideas.closed)
        self.assertEqual(self.store.get('coordinator:' + other + '_session'), saved)
        self.store.message_save(other, 'worker_result', 'finished')
        await self.drain_feed()
        self.assertEqual(self.service.sessions[other].session_id, saved)
        self.assertEqual([text.split('] ', 1)[1] for _, text in self.service.sessions[other].sent],
                         ['finished'])
        self.assertTrue(all('kind=worker_result' not in text for _, text in home.sent))

    async def test_secret_filled_wakes_a_wound_down_parent_in_its_channel(self):
        other = '-10042:5'
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', (other, -10042, 5, 'Ideas', str(self.root / 'ideas')))
        await self.service.start_session(other)
        saved = self.service.sessions[other].session_id
        with patch('coordinator.service.PARENT_IDLE', 0):
            await self.service.feed_once()
        self.assertIn(other, self.service.wound_down)
        self.store.message_save(other, 'secret_filled', 'Secret API_TOKEN, declared on job 1 Build service, is filled.')
        await self.drain_feed()
        self.assertEqual(self.service.sessions[other].session_id, saved)
        self.assertEqual(self.service.sessions[other].sent[-1][1].split('] ', 1)[1],
                         'Secret API_TOKEN, declared on job 1 Build service, is filled.')
        self.assertNotIn(TOPIC, self.service.sessions)

    async def test_pending_secret_filled_survives_restart_with_newer_uncertain_messages(self):
        await self.assert_secret_filled_survives_restart(False)

    async def test_sent_secret_filled_survives_restart_with_newer_uncertain_messages(self):
        await self.assert_secret_filled_survives_restart(True)

    async def test_transcript_confirmed_secret_filled_is_not_sent_again(self):
        notice = self.store.message_save(TOPIC, 'secret_filled', 'Secret API_TOKEN is filled.')
        self.store.message_sending(notice['id'])
        self.store.messages_uncertain()
        transcript = self.root / 'transcript.jsonl'
        transcript.write_text(json.dumps({'type': 'user', 'uuid': message_uuid(notice['id'])}) + '\n')
        factory = self.service.session_factory

        async def with_transcript(*args, **kwargs):
            session = await factory(*args, **kwargs)
            session.transcript_path = str(transcript)
            return session

        self.service.session_factory = with_transcript
        with self.store.db:
            self.store.put('coordinator_session', str(uuid.uuid4()))
        await self.service.start_session()
        await self.drain_feed()
        self.assertFalse(any('kind=secret_filled' in text for _, text in self.sessions[0].sent))
        self.assertEqual(tuple(self.store.db.execute('SELECT delivered,receipt FROM messages WHERE id=?',
                                                     (notice['id'],)).fetchone()), ('received', 'transcript'))

    async def test_crashed_parent_restarts_at_once_and_lists_its_uncertain_message(self):
        other = '-10042:5'
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', (other, -10042, 5, 'Ideas', str(self.root / 'ideas')))
        row = self.store.message_save(other, 'owner', 'do the thing')['id']
        await self.drain_feed()
        crashed = self.service.sessions[other]
        crashed.closed = True
        self.store.messages_unconfirmed([row], 'session_closed')
        await self.drain_feed()
        restarted = self.service.sessions[other]
        self.assertIsNot(restarted, crashed)
        self.assertEqual(restarted.session_id, crashed.session_id)
        notice, = [text for _, text in restarted.sent]
        self.assertTrue(notice.startswith('[topic=-10042:5 name="Ideas" message=2 kind=restarted] The service'))
        self.assertIn('do the thing', notice)
        self.assertNotIn(TOPIC, self.service.sessions)

    async def test_one_channel_parent_failing_to_start_leaves_other_channels_served(self):
        other = '-10042:5'
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', (other, -10042, 5, 'Ideas', str(self.root / 'ideas')))
        factory = self.service.session_factory
        attempts = []

        async def failing(store, runner, cwd, model, instructions, topic=None):
            if topic == other:
                attempts.append(topic)
                raise RuntimeError('Provider host did not become ready')
            return await factory(store, runner, cwd, model, instructions, topic=topic)

        self.service.session_factory = failing
        idea = self.store.message_save(other, 'owner', 'idea')['id']
        self.store.message_save(TOPIC, 'owner', 'home')
        await self.drain_feed()
        await self.drain_feed()
        self.assertEqual([text for _, text in self.service.sessions[TOPIC].sent], [tag(2) + 'home'])
        self.assertNotIn(other, self.service.sessions)
        self.assertEqual(attempts, [other])
        self.assertEqual(self.store.db.execute('SELECT delivered FROM messages WHERE id=?', (idea,)).fetchone()[0],
                         'pending')
        self.assertEqual([(row['code'], row['topic']) for row in self.problems()], [('start-failed', other)])

    async def test_parent_that_fails_at_startup_starts_after_the_retry_delay(self):
        factory = self.service.session_factory
        attempts = []

        async def slow_host(store, runner, cwd, model, instructions, topic=None):
            attempts.append(topic)
            if len(attempts) == 1:
                raise RuntimeError('Provider host did not become ready')
            return await factory(store, runner, cwd, model, instructions, topic=topic)

        with self.store.db:
            self.store.put('coordinator_session', str(uuid.uuid4()))
        row = self.store.message_save(TOPIC, 'owner', 'in flight')['id']
        with self.store.db:
            self.store.db.execute("UPDATE messages SET delivered='uncertain',receipt='service_restarted' "
                                  "WHERE id=?", (row,))
        self.service.session_factory = slow_host
        self.assertIsNone(await self.service.start_session())
        await self.drain_feed()
        self.assertEqual(attempts, [TOPIC])
        self.service.parent_retry[TOPIC] = 0
        await self.drain_feed()
        notice, = [text for _, text in self.service.sessions[TOPIC].sent]
        self.assertIn('kind=restarted] The service restarted', notice)
        self.assertIn('in flight', notice)
        self.assertEqual(self.service.parent_retry, {})

    async def test_parent_gated_by_an_account_wait_at_startup_starts_after_the_reset(self):
        with self.store.db:
            self.store.put('coordinator_session', str(uuid.uuid4()))
            self.store.put('coordinator_account_retry_at', time.time() + 3600)
        row = self.store.message_save(TOPIC, 'owner', 'in flight')['id']
        with self.store.db:
            self.store.db.execute("UPDATE messages SET delivered='uncertain',receipt='service_restarted' "
                                  "WHERE id=?", (row,))
        self.assertIsNone(await self.service.start_session())
        with self.store.db:
            self.store.put('coordinator_account_retry_at', None)
        await self.drain_feed()
        notice, = [text for _, text in self.service.sessions[TOPIC].sent]
        self.assertIn('in flight', notice)

    async def test_parent_waiting_for_a_weekly_reset_starts_once_an_account_frees_up(self):
        with self.store.db:
            self.store.put('coordinator_account_retry_at', time.time() + 5 * 86400)
        self.assertIsNone(await self.service.start_session())
        self.enable_claude_account()
        self.assertIsNotNone(await self.service.start_session())
        self.assertIsNone(self.store.get('coordinator_account_retry_at'))

    async def test_disabled_channel_is_not_retried_without_a_message(self):
        other = '-10042:5'
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', (other, -10042, 5, 'Ideas', str(self.root / 'ideas')))
        factory = self.service.session_factory
        attempts = []

        async def failing(store, runner, cwd, model, instructions, topic=None):
            attempts.append(topic)
            if len(attempts) == 1:
                raise RuntimeError('Provider host did not become ready')
            return await factory(store, runner, cwd, model, instructions, topic=topic)

        self.service.session_factory = failing
        self.assertIsNone(await self.service.start_session(other))
        with self.store.db:
            self.store.db.execute('UPDATE topics SET enabled=0 WHERE id=?', (other,))
        self.service.parent_retry[other] = 0
        await self.drain_feed()
        self.assertEqual(attempts, [other])
        self.assertNotIn(other, self.service.sessions)

    async def test_main_parent_confirms_other_channels_rows_from_the_shared_transcript(self):
        other = '-10042:5'
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', (other, -10042, 5, 'Ideas', str(self.root / 'ideas')))
            self.store.put('coordinator_session', str(uuid.uuid4()))
        row = self.store.message_save(other, 'owner', 'answered before the split')['id']
        with self.store.db:
            self.store.db.execute("UPDATE messages SET delivered='uncertain',receipt='service_restarted' "
                                  "WHERE id=?", (row,))
        transcript = self.root / 'shared.jsonl'
        transcript.write_text(json.dumps({'type': 'user', 'uuid': message_uuid(row)}) + '\n')
        factory = self.service.session_factory

        async def shared(*args, **kwargs):
            session = await factory(*args, **kwargs)
            session.transcript_path = str(transcript)
            return session

        self.service.session_factory = shared
        await self.service.start_session()
        self.assertEqual(tuple(self.store.db.execute('SELECT delivered,receipt FROM messages WHERE id=?',
                                                     (row,)).fetchone()), ('received', 'transcript'))

    async def test_main_channel_fallback_note_keeps_every_channel_task(self):
        other = '-10042:5'
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', (other, -10042, 5, 'Ideas', str(self.root / 'ideas')))
        self.store.task_create(other, 'Ideas task')
        self.store.message_save(TOPIC, 'owner', 'first')
        await self.drain_feed()
        prior = self.service.sessions[TOPIC]
        prior.closed = True
        self.lose_next_resume(TOPIC)
        await self.drain_feed()
        note, = [text for _, text in self.service.sessions[TOPIC].sent]
        self.assertIn('Channel continuity:', note)
        self.assertIn('Ideas task', note)

    async def test_parent_with_a_background_task_is_not_wound_down(self):
        self.store.message_save(TOPIC, 'owner', 'check CI in the background')
        await self.drain_feed()
        session = self.service.sessions[TOPIC]
        session.protocol = SimpleNamespace(background=[{'task_id': 'b1'}])
        with patch('coordinator.service.PARENT_IDLE', 0):
            await self.service.feed_once()
            self.assertFalse(session.closed)
            session.protocol.background = []
            await self.service.feed_once()
        self.assertTrue(session.closed)

    async def test_main_channel_keeps_the_main_session_through_account_wait_and_disable(self):
        other = '-10042:5'
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', (other, -10042, 5, 'Torii brainstorm', str(self.root)))
            self.store.put('coordinator_account_retry_at', time.time() + 3600)
        self.assertIsNone(await self.service.start_session())
        self.assertEqual(self.store.get('coordinator_home_topic'), TOPIC)
        with self.store.db:
            self.store.put('coordinator_account_retry_at', None)
            self.store.db.execute('UPDATE topics SET enabled=0 WHERE id=?', (TOPIC,))
        self.assertEqual(self.service.session_key(other), 'coordinator:' + other)

    async def test_resume_failure_starts_fresh_with_channel_continuity(self):
        self.store.message_save(TOPIC, 'owner', 'first')
        await self.drain_feed()
        prior = self.service.session
        prior.closed = True
        self.lose_next_resume(TOPIC)
        self.store.task_create(TOPIC, 'Keep working', notes='Needs review')
        self.store.message_save(TOPIC, 'owner', 'next')
        await self.drain_feed()
        current = self.service.session
        self.assertNotEqual(current.session_id, prior.session_id)
        self.assertIn('Keep working', current.sent[0][1])
        self.assertIn('Needs review', current.sent[0][1])
        self.assertIn('first', current.sent[0][1])

    async def test_saved_channel_session_survives_service_restart(self):
        other = '-10042:5'
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', (other, -10042, 5, 'Ideas', str(self.root / 'ideas')))
        self.store.message_save(other, 'owner', 'first')
        await self.drain_feed()
        saved = self.service.sessions[other].session_id
        await self.service.sessions[other].stop()
        self.service = Service(self.store, self.telegram, self.runner, self.root,
                               session_factory=self.service.session_factory)
        self.store.message_save(other, 'owner', 'next')
        await self.drain_feed()
        self.assertEqual(self.service.sessions[other].session_id, saved)
        self.assertEqual(self.service.sessions[other].sent[-1][1].split('] ', 1)[1], 'next')

    async def test_pending_restart_notice_stays_with_its_channel_after_restart(self):
        other = '-10042:5'
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', (other, -10042, 5, 'Ideas', str(self.root / 'ideas')))
            self.store.put('coordinator:' + other + '_session', str(uuid.uuid4()))
        self.store.message_save(other, 'restarted', 'Resume Ideas')
        self.store.message_save(other, 'owner', 'continue')
        self.service = Service(self.store, self.telegram, self.runner, self.root,
                               session_factory=self.service.session_factory)
        await self.drain_feed()
        notice, request = [text for _, text in self.service.sessions[other].sent]
        self.assertTrue(notice.startswith('[topic=-10042:5 name="Ideas" message=3 kind=restarted] The service'))
        self.assertEqual(request, tag(2, topic=other, name='Ideas') + 'continue')
        self.assertNotIn(TOPIC, self.service.sessions)

    async def test_fresh_channel_note_lists_its_workers_and_leaves_out_waiting_messages(self):
        other = '-10042:5'
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', (other, -10042, 5, 'Ideas', str(self.root / 'ideas')))
        worker = self.worker(topic=other)
        self.store.message_save(other, 'restarted', 'The service restarted. Stale notice.')
        self.store.message_save(other, 'owner', 'please create task X')
        await self.drain_feed()
        note, request = [text for _, text in self.service.sessions[other].sent]
        self.assertIn('kind=restarted] Channel continuity:', note)
        self.assertIn('"id": %d' % worker, note)
        self.assertIn('"status": "queued"', note)
        self.assertNotIn('please create task X', note)
        self.assertEqual(request, tag(2, topic=other, name='Ideas') + 'please create task X')

    async def test_channel_prompt_lists_only_its_topic(self):
        other = '-10042:5'
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', (other, -10042, 5, 'Ideas', str(self.root / 'ideas')))
        self.store.task_create(TOPIC, 'Home task')
        self.store.task_create(other, 'Ideas task')
        captured = {}
        factory = self.service.session_factory

        async def record(store, runner, cwd, model, instructions, topic=None):
            captured[topic] = (str(cwd), instructions)
            return await factory(store, runner, cwd, model, instructions, topic=topic)

        self.service.session_factory = record
        await self.service.start_session(other)
        await self.service.start_session(TOPIC)
        self.assertEqual(captured[other][0], captured[TOPIC][0])
        self.assertIn('"name":"Ideas"', captured[other][1])
        self.assertNotIn('"name":"Torii"', captured[other][1])
        self.assertIn('"name":"Ideas"', captured[TOPIC][1])
        self.assertIn('"name":"Torii"', captured[TOPIC][1])
        self.assertNotIn('Ideas task', captured[other][1] + captured[TOPIC][1])
        self.assertNotIn('Home task', captured[other][1] + captured[TOPIC][1])

    async def test_channel_parent_is_told_it_serves_this_channel(self):
        other = '-10042:5'
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', (other, -10042, 5, 'Ideas', str(self.root / 'ideas')))
        captured = {}
        factory = self.service.session_factory

        async def record(store, runner, cwd, model, instructions, topic=None):
            captured[topic] = instructions
            return await factory(store, runner, cwd, model, instructions, topic=topic)

        self.service.session_factory = record
        await self.service.start_session(other)
        self.assertTrue(captured[other].startswith(
            "You are Torii's persistent coordinator for the sole Telegram owner in this channel.\n"))
        self.assertNotIn('across all registered', captured[other])
        self.assertIn('\nRegistered channels: [{"id":"-10042:5"', captured[other])

    async def test_startup_doubts_written_messages_that_no_replay_or_transcript_confirmed(self):
        rows = [self.store.message_save(TOPIC, 'owner', text)['id'] for text in ('written', 'replayed', 'found', 'sent')]
        with self.store.db:
            for row, (delivered, receipt) in zip(rows, (('received', 'written'), ('received', 'replayed'),
                                                        ('received', 'transcript'), ('sent', None))):
                self.store.db.execute('UPDATE messages SET delivered=?,receipt=? WHERE id=?', (delivered, receipt, row))
        self.store.messages_uncertain()
        states = [row[0] for row in self.store.db.execute('SELECT delivered FROM messages ORDER BY id')]
        self.assertEqual(states, ['uncertain', 'received', 'received', 'uncertain'])

    async def test_resumed_session_clears_uncertain_messages_found_in_its_transcript(self):
        rows = [self.store.message_save(TOPIC, 'owner', text)['id'] for text in ('seen', 'queued', 'lost')]
        with self.store.db:
            self.store.db.execute("UPDATE messages SET delivered='uncertain',receipt='uncertain'")
        transcript = self.root / 'transcript.jsonl'
        transcript.write_text(json.dumps({'type': 'user', 'uuid': message_uuid(rows[0])}) + '\n' +
                              json.dumps({'type': 'attachment', 'attachment': {
                                  'type': 'queued_command', 'source_uuid': message_uuid(rows[1])}}) + '\n')
        factory = self.service.session_factory

        async def with_transcript(*args, **kwargs):
            session = await factory(*args, **kwargs)
            session.transcript_path = str(transcript)
            return session

        self.service.session_factory = with_transcript
        with self.store.db:
            self.store.put('coordinator_session', str(uuid.uuid4()))
        await self.service.start_session()
        states = [tuple(row) for row in self.store.db.execute('SELECT delivered,receipt FROM messages WHERE id IN (?,?,?) ORDER BY id', rows)]
        self.assertEqual(states, [('received', 'transcript'), ('received', 'transcript'), ('uncertain', 'uncertain')])
        restart = self.store.db.execute("SELECT text FROM messages WHERE kind='restarted'").fetchone()[0]
        self.assertIn('"count":1', restart)
        self.assertIn('"text":"lost"', restart)
        self.assertNotIn('"text":"seen"', restart)

    async def test_restart_waits_for_outbox_then_exits(self):
        with self.store.db:
            request = self.store.service_request('service.restart', {'reason': 'upgrade'})
            self.store.enqueue_report(TOPIC, 'saved report')
        self.assertFalse(await self.service.restart_once())
        await self.service.controls_once()
        self.assertEqual(self.store.db.execute('SELECT state FROM service_requests WHERE id=?',
                                               (request,)).fetchone()[0], 'done')
        self.assertFalse(await self.service.restart_once())
        await self.service.deliver_once()
        with self.assertRaises(_Restart):
            await self.service.restart_once()
        self.assertIsNone(self.store.get('restart_requested_v2'))
        self.assertEqual(len(self.telegram.sent), 1)
        backups = list((self.store.directory / 'backups').glob('state-before-restart-*.sqlite'))
        self.assertEqual(len(backups), 1)
        self.assertEqual(backups[0].stat().st_mode & 0o777, 0o600)
        with sqlite3.connect(str(backups[0])) as copy:
            self.assertEqual(copy.execute('SELECT COUNT(*) FROM outbox WHERE delivered=1').fetchone()[0], 1)
            settings = {key: json.loads(value) for key, value in copy.execute(
                "SELECT key,value FROM settings WHERE key IN ('restart_requested_v2','last_restart_v2')")}
            self.assertIsNone(settings['restart_requested_v2'])
            self.assertIsInstance(settings['last_restart_v2'], float)

    async def test_restart_backup_failure_keeps_request_and_does_not_exit(self):
        with self.store.db:
            self.store.put('restart_requested_v2', {'reason': 'upgrade'})
        with patch.object(self.store, 'backup_for_restart', side_effect=OSError('disk full')):
            with self.assertRaisesRegex(OSError, 'disk full'):
                await self.service.restart_once()
        self.assertIsNotNone(self.store.get('restart_requested_v2'))
        self.assertIsNone(self.store.get('last_restart_v2'))

    async def test_repeat_restart_is_refused_without_a_second_backup(self):
        with self.store.db:
            self.store.put('last_restart_v2', time.time() - RESTART_GUARD_SECONDS + 10)
            request = self.store.service_request('service.restart', {'reason': 'again'})
        await self.service.controls_once()
        row = self.store.db.execute('SELECT state,result FROM service_requests WHERE id=?',
                                    (request,)).fetchone()
        self.assertEqual(row['state'], 'refused')
        self.assertIn('restarted recently', json.loads(row['result'])['text'])
        self.assertIsNone(self.store.get('restart_requested_v2'))
        self.assertFalse((self.store.directory / 'backups').exists())

    async def test_restart_keeps_five_newest_backups(self):
        folder = self.store.directory / 'backups'
        folder.mkdir()
        for index in range(6):
            (folder / ('state-before-restart-%d.sqlite' % index)).write_bytes(b'old')
        with self.store.db:
            self.store.put('restart_requested_v2', {'reason': 'upgrade'})
        with self.assertRaises(_Restart):
            await self.service.restart_once()
        names = sorted(path.name for path in folder.glob('state-before-restart-*.sqlite'))
        self.assertEqual(len(names), 5)
        self.assertNotIn('state-before-restart-0.sqlite', names)
        self.assertNotIn('state-before-restart-1.sqlite', names)

    async def test_restart_waits_for_delayed_outbox_retry(self):
        with self.store.db:
            self.store.put('restart_requested_v2', {'reason': 'upgrade'})
            self.store.enqueue_report(TOPIC, 'retry later')
            self.store.db.execute('UPDATE outbox SET next_attempt=9999999999')
        self.assertIsNone(self.store.pending_delivery())
        self.assertFalse(await self.service.restart_once())
        self.assertIsNotNone(self.store.get('restart_requested_v2'))

    async def test_poll_persists_batch_before_callback_ack(self):
        self.telegram.pending_updates = [owner_update(1, 'one'), owner_update(2, 'two')]
        task = asyncio.create_task(self.service.poll())
        try:
            async def wait():
                while self.store.db.execute('SELECT COUNT(*) FROM messages').fetchone()[0] < 2:
                    await asyncio.sleep(0.01)
            await asyncio.wait_for(wait(), 2)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(self.store.get('offset'), 3)

    async def test_poll_skips_invalid_and_unknown_updates_without_rolling_back_the_batch(self):
        malformed = {'update_id': 2, 'message': []}
        self.telegram.updates = AsyncMock(side_effect=[
            [owner_update(1, 'First'), malformed, malformed,
             {'update_id': 3, 'future_update': {}},
             {'update_id': 4, 'callback_query': {'id': 'bad', 'from': [], 'message': None}},
             owner_update(5, 'Last')], asyncio.CancelledError()])
        with self.assertRaises(asyncio.CancelledError):
            await self.service.poll()
        self.assertEqual(self.store.get('offset'), 6)
        self.assertEqual([row['text'] for row in self.store.messages_pending()], ['First', 'Last'])
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM problems').fetchone()[0], 3)

    async def test_poll_leaves_new_messages_without_a_reaction(self):
        with patch('coordinator.service.REACTION_IDLE', 60):
            idle = asyncio.create_task(self.service.reactions())
            try:
                await asyncio.sleep(0.05)
                self.telegram.pending_updates = [owner_update(5, 'hello')]
                await self.run_until([self.service.poll], lambda: bool(self.store.messages_pending()))
                await asyncio.sleep(0.05)
                self.assertEqual(self.reacted(), [])
            finally:
                idle.cancel()
                await asyncio.gather(idle, return_exceptions=True)
        self.assertEqual(self.store.db.execute('SELECT reaction_state FROM messages').fetchone()[0], 'sent')

    async def test_reaction_loop_drains_every_desired_reaction_without_idling(self):
        for update_id in (1, 2, 3):
            self.store.accept(owner_update(update_id, 'message %d' % update_id))
            self.store.task_create(TOPIC, 'Job', origin=update_id)
        with patch('coordinator.service.REACTION_IDLE', 60):
            await self.run_until([self.service.reactions], lambda: len(self.reacted()) == 3)
        self.assertEqual(self.reacted(), [1, 2, 3])

    async def test_a_reaction_timeout_is_its_own_problem(self):
        self.store.accept(owner_update(5, 'hello'))
        self.store.task_create(TOPIC, 'Job', origin=1)
        failures = [TelegramError('network-or-invalid-response', reason='timeout'),
                    TelegramError('network-or-invalid-response')]

        async def call(method, **params):
            raise failures.pop(0)
        self.telegram.call = call
        with self.assertLogs('coordinator.problems', 'WARNING'):
            await deliver_reaction(self.store, self.telegram)
            with self.store.db:
                self.store.db.execute('UPDATE messages SET reaction_retry=0')
            await deliver_reaction(self.store, self.telegram)
        self.assertEqual([(row['code'], row['detail']) for row in self.problems()], [
            ('reaction-timeout', 'telegram=network-or-invalid-response disabled=False attempts=1'),
            ('reaction-failed', 'telegram=network-or-invalid-response disabled=False attempts=2')])

    async def test_owner_reaction_wakes_parent_as_an_owner_reply(self):
        with self.store.db:
            outbox = self.store.enqueue_report(TOPIC, 'The job report')
            self.store.delivered(outbox, 600)
        self.telegram.pending_updates = [{'update_id': 100, 'message_reaction': {
            'chat': {'id': -10042}, 'message_id': 600, 'user': {'id': 7},
            'old_reaction': [], 'new_reaction': [{'type': 'emoji', 'emoji': '👍'}]}}]
        with patch('coordinator.service.FEED_IDLE', 60):
            await self.run_until([self.service.poll, self.service.feed, self.service.reactions],
                                 lambda: self.sessions and self.sessions[0].sent)
        await asyncio.gather(*self.service.feed_tasks.values())
        self.assertEqual(self.sessions[0].sent[0][1], tag(1) + 'Replying to: The job report\nReacted 👍')
        self.assertEqual(self.reacted(), [])

    async def test_owner_reaction_in_disabled_channel_stores_no_message_and_starts_no_parent(self):
        topic = '-10042:9'
        with self.store.db:
            self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,0)',
                                  (topic, -10042, 9, 'Unbound project', ''))
            outbox = self.store.enqueue_report(topic, 'Setup guide: pick a folder with /setup')
            self.store.delivered(outbox, 700)
        typed = owner_update(1, 'hello')
        typed['message']['message_thread_id'] = 9
        self.assertEqual(self.store.accept(typed), 'disabled')
        self.telegram.pending_updates = [{'update_id': 100, 'message_reaction': {
            'chat': {'id': -10042}, 'message_id': 700, 'user': {'id': 7},
            'old_reaction': [], 'new_reaction': [{'type': 'emoji', 'emoji': '👍'}]}}]
        await self.run_until([self.service.poll], lambda: self.store.get('offset') == 101)
        await self.service.feed_once()
        await asyncio.gather(*self.service.feed_tasks.values())
        self.assertNotIn(topic, self.service.sessions)
        self.assertEqual(self.sessions, [])
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM messages WHERE topic=?', (topic,)).fetchone()[0], 0)
        self.assertIsNone(self.store.get('coordinator:' + topic))

    async def test_reaction_loop_delivers_pending_reactions_without_timing_turns(self):
        from coordinator.reactions import handed_off, set_desired
        self.store.accept(owner_update(5, 'hello'))
        session = await self.service.start_session(TOPIC)
        session.busy = True
        with patch('coordinator.reactions.time.time', return_value=time.time() - 20):
            handed_off(self.store, 1)
        loop = asyncio.create_task(self.service.reactions())
        try:
            await asyncio.sleep(0.05)
            self.assertEqual(self.reacted(), [])
            with self.store.db:
                set_desired(self.store, 1, '👀')
            self.service.wake('reactions').set()
            async def wait():
                while self.reacted() != [5]:
                    await asyncio.sleep(0.01)
            await asyncio.wait_for(wait(), 2)
        finally:
            loop.cancel()
            await asyncio.gather(loop, return_exceptions=True)

    async def test_poll_wakes_idle_delivery_and_feed_at_once(self):
        with patch('coordinator.service.DELIVERY_IDLE', 60), patch('coordinator.service.FEED_IDLE', 60):
            idle = [asyncio.create_task(self.service.deliver()), asyncio.create_task(self.service.feed())]
            try:
                await asyncio.sleep(0.05)
                self.telegram.pending_updates = [owner_update(5, '/ping'), owner_update(6, 'hello')]
                await self.run_until([self.service.poll], lambda: self.telegram.sent and self.sessions and self.sessions[0].sent)
            finally:
                for task in idle:
                    task.cancel()
                await asyncio.gather(*idle, return_exceptions=True)
        self.assertEqual(self.telegram.sent[0]['reply_to'], 5)
        self.assertEqual(self.sessions[0].sent[0][1], '[topic=-10042:4 name="Torii" message=1 kind=owner] hello')

    async def test_poll_retries_a_network_failure_within_a_second(self):
        replies = [TelegramError('network-or-invalid-response'), [owner_update(5, 'hello')]]

        async def updates(offset):
            await asyncio.sleep(0)
            reply = replies.pop(0) if replies else []
            if isinstance(reply, Exception):
                raise reply
            return reply
        self.telegram.updates = updates
        with self.assertLogs('coordinator.service', 'INFO') as logs:
            await self.run_until([self.service.poll], lambda: self.store.db.execute(
                'SELECT COUNT(*) FROM messages').fetchone()[0] == 1)
        self.assertIn('poll retry code=network-or-invalid-response', '\n'.join(logs.output))

    async def test_owner_message_logs_its_age_and_steer_timing_without_an_instant_reaction(self):
        update = owner_update(5, 'hello')
        update['message']['date'] = int(time.time()) - 3
        with self.assertLogs('coordinator', 'INFO') as logs:
            self.store.accept(update)
            self.store.accept(owner_update(6, 'undated'))
            await self.run_until([self.service.reactions, self.service.feed],
                                 lambda: self.sessions and len(self.sessions[0].sent) == 2)
            await asyncio.gather(*self.service.feed_tasks.values())
        output = '\n'.join(logs.output)
        age = re.search(r'owner message saved message=1 telegram_age=([0-9.]+)', output)
        self.assertIsNotNone(age)
        self.assertGreaterEqual(float(age.group(1)), 3)
        self.assertIn('owner message saved message=2 telegram_age=unknown', output)
        self.assertEqual(self.reacted(), [])
        self.assertRegex(output, r'coordinator steer message=2 receipt=received after=[0-9.]+')
        self.assertNotIn('-10042', output)

    async def test_delivery_retry_does_not_create_worker(self):
        with self.store.db:
            self.store.enqueue_report(TOPIC, 'reply')
        async def failed(row):
            raise TelegramError(429, retry_after=1)
        self.telegram.send = failed
        self.assertTrue(await self.service.deliver_once())
        self.assertEqual(self.store.db.execute('SELECT attempts FROM outbox').fetchone()[0], 1)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM workers').fetchone()[0], 0)

    async def test_missing_image_becomes_one_text_notice(self):
        image = self.root / 'missing.png'
        image.write_bytes(b'photo')
        with self.store.db:
            self.store.enqueue_report(TOPIC, 'Caption', image=str(image))
        image.unlink()
        self.assertTrue(await self.service.deliver_once())
        row = self.store.db.execute('SELECT text,image,attempts,delivered FROM outbox').fetchone()
        self.assertIn('image could not be sent', row['text'])
        self.assertIn('Caption', row['text'])
        self.assertIsNone(row['image'])
        self.assertEqual((row['attempts'], row['delivered']), (0, 0))
        self.assertEqual(self.telegram.sent, [])
        self.assertTrue(await self.service.deliver_once())
        self.assertFalse(await self.service.deliver_once())
        self.assertEqual(len(self.telegram.sent), 1)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], 1)
        self.assertEqual(self.store.db.execute('SELECT delivered FROM outbox').fetchone()[0], 1)

    async def test_uneditable_card_is_sent_as_a_new_message(self):
        with self.store.db:
            self.store.enqueue_report(TOPIC, 'page two', edit=77)
        async def refused(row):
            if row.get('edit_message'):
                raise TelegramError(400)
            return {'message_id': 78}
        self.telegram.send = refused
        await self.service.deliver_once()
        await self.service.deliver_once()
        row = self.store.db.execute('SELECT delivered,telegram_message,edit_message FROM outbox').fetchone()
        self.assertEqual(tuple(row), (1, 78, None))

    async def test_steer_to_an_ended_turn_is_refused_with_one_note(self):
        worker = self.running_worker('closed')
        steer = self.store.service_request('workers.steer', {'worker': worker, 'prompt': 'also this'})
        await self.service.controls_once()
        self.assertEqual(self.request(steer), ('refused', {'receipt': 'closed'}))
        self.assertEqual(self.notes(),
                         ['Steer request %d for worker %d was not delivered: Its turn had ended.' % (steer, worker)])
        self.assertEqual([(row['code'], row['worker']) for row in self.problems()], [('steer-closed', worker)])

    async def test_stop_after_worker_finishes_notifies_parent_once(self):
        worker = self.running_worker('closed')
        request = self.store.service_request('workers.stop', {'worker': worker})
        self.store.worker_complete(worker, {'success': True}, 'needs_input')
        self.assertTrue(await self.service.controls_once())
        self.assertEqual(self.request(request), ('refused', {'text': 'Worker is not running.'}))
        self.assertEqual(self.notes(),
                         ['Stop request %d for worker %d was refused: Worker is not running.' % (request, worker)])
        self.assertFalse(await self.service.controls_once())
        self.assertEqual(len(self.notes()), 1)

    async def test_refused_steer_to_done_worker_records_problem(self):
        worker = self.worker()
        request = self.store.service_request('workers.steer', {'worker': worker, 'prompt': 'continue'})
        self.store.worker_complete(worker, {'success': True, 'session_id': str(uuid.uuid4())})
        with self.assertLogs('coordinator.problems', 'WARNING'):
            await self.service.controls_once()
        self.assertEqual(self.request(request), ('refused', {'text': 'Worker cannot receive input.'}))
        self.assertEqual([(row['code'], row['detail'], row['worker']) for row in self.problems()],
                         [('steer-refused', 'op=workers.steer request=%d' % request, worker)])
        await self.drain_feed()
        self.assertIn('worker %d' % worker, self.sessions[0].sent[-1][1])
        self.assertIn('Worker cannot receive input.', self.sessions[0].sent[-1][1])
        self.assertIn('not delivered', self.sessions[0].sent[-1][1])

    async def test_queued_steer_over_brief_limit_notifies_parent(self):
        worker = self.worker()
        request = self.store.service_request('workers.steer', {'worker': worker, 'prompt': 'x' * BRIEF_LIMIT})
        self.assertTrue(await self.service.controls_once())
        self.assertEqual(self.request(request),
                         ('refused', {'text': 'The queued brief would exceed %d characters.' % BRIEF_LIMIT}))
        self.assertEqual(self.notes(),
                         ['Steer request %d for worker %d was not delivered: '
                          'The queued brief would exceed %d characters.' % (request, worker, BRIEF_LIMIT)])

    async def test_needs_input_steer_resumes_saved_session_for_both_providers(self):
        self.enable_claude_account()
        for provider in ('claude', 'codex'):
            with self.subTest(provider=provider):
                session_id = str(uuid.uuid4())
                transcript = self.root / 'primary' / 'projects' / 'test' / (session_id + '.jsonl')
                transcript.parent.mkdir(parents=True, exist_ok=True)
                transcript.write_text('{}\n')

                class Runner:
                    def __init__(self):
                        self.calls = []

                    async def run(self, provider, prompt, cwd, session, **options):
                        self.calls.append((prompt, session, options['host_id']))
                        text = 'Which path should I use?' if len(self.calls) == 1 else 'Completed the task.'
                        return RunResult(session or session_id, text=text, success=True,
                                         transcript_path=str(transcript))

                runner = Runner()
                self.service.runner = runner
                self.service.workers.runner = runner
                worker = self.worker(provider=provider)
                await self.service.workers_once()
                await asyncio.gather(*self.service.worker_tasks.values())
                self.assertEqual(self.service.workers.get(worker)['status'], 'needs_input')
                saved_session = self.service.workers.get(worker)['session']
                request = self.store.service_request('workers.steer', {'worker': worker, 'prompt': 'Use the first path.'})
                await self.service.controls_once()
                self.assertEqual(self.request(request), ('done', {'receipt': 'resumed'}))
                self.assertEqual(self.service.workers.get(worker)['status'], 'queued')
                await self.service.workers_once()
                await asyncio.gather(*self.service.worker_tasks.values())
                self.assertEqual(self.service.workers.get(worker)['status'], 'done')
                self.assertEqual(runner.calls[1], ('Use the first path.', saved_session,
                                                   'worker-%d-resume-%d' % (worker, request)))
                results = [json.loads(row[0]) for row in self.store.db.execute(
                    "SELECT text FROM messages WHERE source_worker=? AND kind='worker_result' ORDER BY id", (worker,))]
                self.assertEqual([result['status'] for result in results], ['needs_input', 'done'])
                self.assertEqual(results[-1]['text'], 'Completed the task.')

    async def test_restart_keeps_queued_resume(self):
        session_id = str(uuid.uuid4())
        worker = self.worker(provider='codex')
        with self.store.db:
            self.store.db.execute("UPDATE workers SET status='needs_input',session=?,fresh=0 WHERE id=?",
                                  (session_id, worker))
        request = self.store.service_request('workers.steer', {'worker': worker, 'prompt': 'Continue after restart.'})
        await self.service.controls_once()
        self.assertEqual(self.request(request), ('done', {'receipt': 'resumed'}))
        restarted = Service(self.store, self.telegram, self.runner, self.root, pair_only=False)
        await restarted.workers_once()
        await asyncio.gather(*restarted.worker_tasks.values())
        self.assertEqual(self.runner.calls[-1][1:4], ('Continue after restart.', str(self.root), session_id))
        self.assertEqual(restarted.workers.get(worker)['status'], 'done')

    async def test_goal_queued_mid_turn_is_recorded_as_queued_then_received(self):
        worker = self.running_worker('queued')
        goal = self.store.service_request('workers.goal', {'worker': worker, 'condition': 'tests pass'})
        await self.service.controls_once()
        self.assertEqual(self.request(goal), ('done', {'receipt': 'queued'}))
        steer_outcome(self.store, 'torii-v2-control-%d' % goal, 'received')
        self.assertEqual(self.request(goal), ('done', {'receipt': 'received'}))
        self.assertEqual(self.notes(), [])
        self.assertEqual(self.problems(), [])

    async def test_startup_settles_an_uncertain_steer_the_transcript_shows_and_notes_the_rest(self):
        worker = self.worker()
        transcript = self.root / 'session.jsonl'
        seen, lost = (self.store.service_request('workers.steer', {'worker': worker, 'prompt': text})
                      for text in ('seen', 'lost'))
        transcript.write_text(json.dumps({'type': 'attachment', 'attachment': {
            'type': 'queued_command', 'source_uuid': steer_wire('torii-v2-control-%d' % seen)}}) + '\n')
        with self.store.db:
            self.store.db.execute("UPDATE service_requests SET state='sending'")
        self.store.worker_complete(worker, {'success': True, 'transcript_path': str(transcript)})
        self.store.requests_uncertain()
        self.service.reconcile_steers()
        self.assertEqual(self.request(seen), ('done', {'receipt': 'received', 'reconciled': True}))
        self.assertEqual(self.request(lost), ('uncertain', {'receipt': 'uncertain', 'noted': True}))
        self.assertEqual(self.notes(), ['Steer request %d to worker %d may not have reached it: the worker ended '
                                        'before the message showed in its transcript.' % (lost, worker)])
        self.service.reconcile_steers()
        self.assertEqual(len(self.notes()), 1)
        self.assertEqual([(row['code'], row['detail'], row['worker']) for row in self.problems()],
                         [('steer-not-arrived', 'request=%d status=done' % lost, worker)])

    async def test_queued_stop_publishes_result_and_running_goal_receipt(self):
        queued = self.worker()
        stop = self.store.service_request('workers.stop', {'worker': queued})
        await self.service.controls_once()
        self.assertEqual(self.store.db.execute('SELECT state FROM service_requests WHERE id=?', (stop,)).fetchone()[0], 'done')
        self.assertEqual(self.store.db.execute('SELECT kind FROM messages WHERE source_worker=?', (queued,)).fetchone()[0],
                         'worker_result')
        running = self.worker()
        with self.store.db:
            self.store.db.execute("UPDATE workers SET status='running' WHERE id=?", (running,))
        class Control:
            async def steer(self, message_id, prompt):
                self.sent = (message_id, prompt)
                return 'received'
        control = Control()
        self.service.workers.controls[running] = control
        goal = self.store.service_request('workers.goal', {'worker': running, 'condition': 'done'})
        await self.service.controls_once()
        self.assertEqual(control.sent[1], '/goal done')
        self.assertEqual(self.store.db.execute('SELECT state FROM service_requests WHERE id=?', (goal,)).fetchone()[0],
                         'done')

    async def test_worker_steer_without_a_receipt_is_a_problem(self):
        running = self.worker()
        task = self.store.db.execute('SELECT task FROM workers WHERE id=?', (running,)).fetchone()[0]
        with self.store.db:
            self.store.db.execute("UPDATE workers SET status='running' WHERE id=?", (running,))
        class Control:
            async def steer(self, message_id, prompt):
                return 'uncertain'
        self.service.workers.controls[running] = Control()
        request = self.store.service_request('workers.steer', {'worker': running, 'prompt': 'also check logs'})
        with self.assertLogs('coordinator.problems', 'WARNING'):
            await self.service.controls_once()
        self.assertEqual(self.store.db.execute('SELECT state FROM service_requests WHERE id=?',
                                               (request,)).fetchone()[0], 'uncertain')
        self.assertEqual(self.problems(), [{'area': 'worker', 'code': 'steer-uncertain',
                                            'detail': 'op=workers.steer request=%d' % request, 'topic': TOPIC,
                                            'task': task, 'worker': running, 'message': None}])
        self.assertNotIn('also check logs', json.dumps(self.problems()))

    async def test_coordinator_steer_without_a_receipt_is_a_problem(self):
        self.store.accept(owner_update(5, 'first'))
        await self.drain_feed()
        async def uncertain(message_id, text, row_id=None):
            return 'uncertain'
        self.sessions[0].send = uncertain
        self.store.accept(owner_update(6, 'second secret words'))
        with self.assertLogs('coordinator.problems', 'WARNING'):
            await self.drain_feed()
        message = self.store.db.execute('SELECT id FROM messages WHERE telegram_message=6').fetchone()[0]
        self.assertEqual(self.problems(), [{'area': 'coordinator', 'code': 'steer-uncertain', 'detail': None,
                                            'topic': TOPIC, 'task': None, 'worker': None, 'message': message}])

    async def test_each_failed_delivery_attempt_is_a_problem(self):
        with self.store.db:
            outbox = self.store.enqueue_report(TOPIC, 'reply text')
        async def failed(row):
            raise TelegramError(429, retry_after=1)
        self.telegram.send = failed
        with self.assertLogs('coordinator.problems', 'WARNING'):
            await self.service.deliver_once()
        self.assertEqual(self.problems(), [{'area': 'outbox', 'code': 'send-failed',
            'detail': 'outbox=%d kind=report telegram=429 reason=None retry_after=1 attempts=1' % outbox,
            'topic': TOPIC, 'task': None, 'worker': None, 'message': None}])

    async def test_repeated_poll_failures_save_one_row_per_minute(self):
        replies = [TelegramError(502, retry_after=0.001)] * 3 + [[owner_update(5, 'hello')]]

        async def updates(offset):
            await asyncio.sleep(0)
            reply = replies.pop(0) if replies else []
            if isinstance(reply, Exception):
                raise reply
            return reply
        self.telegram.updates = updates
        with self.assertLogs('coordinator.problems', 'WARNING') as logs:
            await self.run_until([self.service.poll], lambda: self.store.db.execute(
                'SELECT COUNT(*) FROM messages').fetchone()[0] == 1)
        self.assertEqual([(row['code'], row['detail']) for row in self.problems()],
                         [('poll-502', 'retry_after=0.001 failures=1')])
        self.assertEqual(len(logs.output), 1)

    async def test_restart_request_saves_its_reason(self):
        with self.store.db:
            self.store.service_request('service.restart', {'reason': 'deploy the problems table'})
        with self.assertLogs('coordinator.problems', 'WARNING'):
            await self.service.controls_once()
        self.assertEqual([(row['code'], row['detail']) for row in self.problems()],
                         [('restart-requested', 'deploy the problems table')])

    async def test_fresh_boot_without_interrupted_workers_announces_to_enabled_channels(self):
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('other',1,8,'other','',1)")
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('disabled',1,9,'disabled','',0)")
        with patch('coordinator.service.boot_time', return_value=200):
            await self.announce(100)
        rows = self.store.db.execute('SELECT topic,text FROM outbox').fetchall()
        self.assertEqual({row['topic'] for row in rows}, {TOPIC, 'other'})
        self.assertTrue(all(row['text'] == 'Torii just came back online.' for row in rows))
        self.service.pair_only = True
        await self.announce(100)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], 2)

    async def test_service_only_restart_with_running_workers_posts_nothing(self):
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('other',1,8,'other','',1)")
            worker = self.worker()
            self.store.db.execute("UPDATE workers SET status='running' WHERE id=?", (worker,))
        with patch('coordinator.service.boot_time', return_value=50), patch('coordinator.host.HostClient') as host, patch.object(
                self.service.workers, 'run', new=AsyncMock(return_value=None)):
            host.return_value.recover_state = AsyncMock(return_value='running')
            host.return_value.connect = AsyncMock()
            await self.announce(100)
            await asyncio.gather(*self.service.worker_tasks.values())
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], 0)

    async def test_start_counts_only_workers_reconciled_as_interrupted_in_each_channel(self):
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('other',1,8,'other','',1)")
            workers = [self.worker(), self.worker(), self.worker(topic='other')]
            self.store.db.executemany("UPDATE workers SET status='running' WHERE id=?",
                                      [(worker,) for worker in workers])
        await self.announce(100)
        rows = {row['topic']: row['text'] for row in self.store.db.execute('SELECT topic,text FROM outbox')}
        self.assertEqual(rows, {TOPIC: 'Torii just came back online - 2 agents were stopped. Ask me to resume them.',
                                'other': 'Torii just came back online - 1 agent was stopped. Ask me to resume them.'})
        self.assertEqual([row['status'] for row in self.store.db.execute('SELECT status FROM workers ORDER BY id')],
                         ['interrupted'] * 3)

    async def test_service_only_restart_posts_only_where_agents_stopped(self):
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('other',1,8,'other','',1)")
            worker = self.worker()
            self.store.db.execute("UPDATE workers SET status='running' WHERE id=?", (worker,))
        with patch('coordinator.service.boot_time', return_value=50):
            await self.announce(100)
        rows = {row['topic']: row['text'] for row in self.store.db.execute('SELECT topic,text FROM outbox')}
        self.assertEqual(rows, {TOPIC: 'Torii just came back online - 1 agent was stopped. Ask me to resume them.'})

    async def test_agents_stopped_in_a_disabled_channel_are_reported_in_the_home_channel(self):
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('other',1,8,'other','',1)")
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('disabled',1,9,'disabled','',0)")
            worker = self.worker(topic='disabled')
            self.store.db.execute("UPDATE workers SET status='running' WHERE id=?", (worker,))
        with patch('coordinator.service.boot_time', return_value=200):
            await self.announce(100)
        rows = {row['topic']: row['text'] for row in self.store.db.execute('SELECT topic,text FROM outbox')}
        self.assertEqual(rows, {TOPIC: 'Torii just came back online - 1 agent was stopped. Ask me to resume them.',
                                'other': 'Torii just came back online.'})

    async def test_stops_reconciled_by_a_failed_start_are_counted_by_the_next_start(self):
        with self.store.db:
            workers = [self.worker(), self.worker()]
            self.store.db.executemany("UPDATE workers SET status='running' WHERE id=?", [(w,) for w in workers])
        calls = []

        async def recover(host):
            calls.append(host)
            if len(calls) == 2:
                raise ConnectionResetError('host socket reset')
            return 'dead'
        with patch('coordinator.service.boot_time', return_value=50):
            with patch('coordinator.host.HostClient.recover_state', recover), \
                    self.assertRaises(ConnectionResetError):
                await self.announce(100)
            self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], 0)
            await self.service.announce_start()
        rows = {row['topic']: row['text'] for row in self.store.db.execute('SELECT topic,text FROM outbox')}
        self.assertEqual(rows, {TOPIC: 'Torii just came back online - 2 agents were stopped. Ask me to resume them.'})

    async def test_stops_reach_an_enabled_channel_when_the_stored_home_is_disabled(self):
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('other',1,8,'other','',1)")
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('disabled',1,9,'disabled','',0)")
            self.store.put('coordinator_home_topic', 'disabled')
            worker = self.worker(topic='disabled')
            self.store.db.execute("UPDATE workers SET status='running' WHERE id=?", (worker,))
        with patch('coordinator.service.boot_time', return_value=50):
            await self.announce(100)
        rows = {row['topic']: row['text'] for row in self.store.db.execute('SELECT topic,text FROM outbox')}
        self.assertEqual(rows, {TOPIC: 'Torii just came back online - 1 agent was stopped. Ask me to resume them.'})

    async def test_start_cut_short_after_a_reboot_still_announces_on_the_next_start(self):
        with self.store.db:
            self.store.put('service_run', {'state': 'exited', 'cause': 'signal', 'ended': 100.0})
        entered = asyncio.Event()
        factory = self.service.session_factory

        async def hung(store, runner, cwd, model, instructions, topic=None):
            entered.set()
            await asyncio.Event().wait()

        async def restart():
            raise _Restart()
        self.service.session_factory = hung
        with patch('coordinator.service.boot_time', return_value=200), \
                self.assertLogs('coordinator.problems', 'WARNING'):
            first = asyncio.create_task(self.service.run())
            await entered.wait()
            first.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await first
            second = Service(self.store, self.telegram, self.runner, self.root, session_factory=factory)
            second.supervised = lambda: [('restart', restart)]
            await second.run()
            self.assertEqual([row['text'] for row in self.store.db.execute('SELECT text FROM outbox')],
                             ['Torii just came back online.'])
            third = Service(self.store, self.telegram, self.runner, self.root, session_factory=factory)
            third.supervised = lambda: [('restart', restart)]
            await third.run()
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], 1)

    def test_boot_time_reads_sysctl_seconds(self):
        with patch('coordinator.host_os.SYSTEM', 'darwin'), patch('coordinator.service.subprocess.run') as run:
            run.return_value.stdout = '{ sec = 123, usec = 456 }\n'
            self.assertEqual(boot_time(), 123)
            run.assert_called_once_with(['sysctl', '-n', 'kern.boottime'], capture_output=True, text=True,
                                        check=True, timeout=1)

    async def test_start_and_exit_keep_the_last_startup_check(self):
        with self.store.db:
            self.store.put('service_run', {'state': 'running', 'started': 90.0, 'checked': 100.0})
        with self.assertLogs('coordinator.problems', 'ERROR'):
            self.service.record_start()
        self.assertEqual(self.store.get('service_run')['checked'], 100.0)
        self.service.record_exit('restart requested')
        self.assertEqual(self.store.get('service_run')['checked'], 100.0)
        with patch('coordinator.service.boot_time', return_value=50):
            await self.service.announce_start()
        self.assertGreater(self.store.get('service_run')['checked'], 100.0)

    async def test_each_start_records_how_the_previous_run_ended(self):
        with self.assertLogs('coordinator.problems', 'WARNING'):
            self.service.record_start()
        self.assertEqual(self.store.get('service_run')['state'], 'running')
        with self.assertLogs('coordinator.problems', 'ERROR') as logs:
            Service(self.store, self.telegram, self.runner, self.root).record_start()
        self.assertIn('ERROR:coordinator.problems:problem area=service code=unclean-exit', logs.output[0])

        async def restart():
            self.service.restart_reason = 'deploy'
            raise _Restart()
        self.service.supervised = lambda: [('restart', restart)]
        with self.assertLogs('coordinator.problems', 'WARNING'):
            await self.service.run()
        self.assertEqual(self.store.get('service_run')['cause'], 'restart reason=deploy')
        with self.assertLogs('coordinator.problems', 'WARNING'):
            Service(self.store, self.telegram, self.runner, self.root).record_start()
        rows = self.problems()
        self.assertEqual([row['code'] for row in rows], ['started', 'unclean-exit', 'unclean-exit', 'started'])
        self.assertEqual(rows[0]['detail'], 'previous exit: unknown, no record')
        self.assertRegex(rows[1]['detail'], r'^previous run started \S+ ended without an exit record$')
        self.assertEqual(rows[3]['detail'], 'previous exit: restart reason=deploy')


class NativeServiceTests(ServiceTestsSupport, unittest.IsolatedAsyncioTestCase):
    async def test_public_service_starts_and_closes_and_logs_the_extension_mode(self):
        async def restart():
            raise _Restart()

        self.service.supervised = lambda: [('restart', restart)]
        self.service.pair_only = True
        with patch.object(self.service.extension, 'start', AsyncMock()) as start, \
                patch.object(self.service.extension, 'close', AsyncMock()) as close, \
                self.assertLogs('coordinator.service', level='INFO') as logs:
            await self.service.run()
        start.assert_awaited_once_with()
        close.assert_awaited_once_with()
        self.assertTrue(any('claude launch extension=native' in line for line in logs.output))
