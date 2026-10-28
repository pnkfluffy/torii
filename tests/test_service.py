import asyncio
from argparse import Namespace
from contextlib import redirect_stdout
from io import BytesIO, StringIO
import json
import os
from pathlib import Path
import re
from types import SimpleNamespace
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import AsyncMock, Mock, patch
import uuid

from coordinator import close_saves, extension, parent_state, workspaces
from coordinator.control_api import BRIEF_LIMIT
from coordinator.media import attachment_instructions
from coordinator.native_protocol import NativeProtocol, RunControl, steer_wire
from coordinator.providers import RunResult
from coordinator.reactions import deliver_reaction
from coordinator.service import RESTART_GUARD_SECONDS, Service, _Restart, _TaskFailed, boot_time
from coordinator.session import message_tag, message_uuid
from coordinator.store import Store
from coordinator.telegram import TelegramError
from coordinator.workers import reconcile_steers, steer_outcome
from tests.support import restart_work_rows, settle, stop_test_hosts


TOPIC = '-10042:4'
REPLY_FIRST_LINE = (
    "Owner message: if it starts a worker or a long task, confirm that in one line first.\n")


ORIGIN_MAIN_CLOSE = {'request': {'created': 1000.0,
             'id': 1,
             'op': 'topic.close',
             'params': {'_chat': -10042, '_topic': '-10042:4', 'message': 1, 'topic': '-10042:9'},
             'result': {'phase': 'card',
                        'revision': 1,
                        'snapshot': {'cwd': '{root}/projects/closing-project',
                                     'enabled': 1,
                                     'git': {'repository': False},
                                     'identity': '{project_identity}',
                                     'jobs': [],
                                     'name': 'Closing project',
                                     'shared': [],
                                     'topic': '-10042:9',
                                     'trash_allowed': True,
                                     'workers': []}},
             'state': 'confirming',
             'updated': 1000.0},
 'card': {'as_document': 0,
          'created': 1000.0,
          'attempts': 0,
          'delivered': 0,
          'edit_message': None,
          'id': 1,
          'image': None,
          'kind': 'status',
          'next_attempt': 0.0,
          'reaction_attempts': 0,
          'reaction_desired': None,
          'reaction_first_failed': None,
          'reaction_retry': 0.0,
          'reaction_sent': None,
          'reaction_state': 'sent',
          'reply_markup': {'inline_keyboard': [[{'callback_data': 'torii:0123456789abcdef:0',
                                                 'text': 'Close and move folder to Trash'}],
                                               [{'callback_data': 'torii:0123456789abcdef:1',
                                                 'text': 'Close, keep folder'}],
                                               [{'callback_data': 'torii:0123456789abcdef:2',
                                                 'text': 'Cancel'}]]},
          'reply_to': None,
          'retired': 0,
          'telegram_message': None,
          'text': 'Close "Closing project"?\n'
                  'Its agent stops. The Telegram topic and all its messages will be deleted. This '
                  'cannot be undone.\n'
                  '\n'
                  'Open jobs to mark dropped:\n'
                  'None.\n'
                  '\n'
                  'Folder: {root}/projects/closing-project\n'
                  'Other chats using this folder or a folder inside or above it: None.\n'
                  'This folder is not a git repository.',
          'topic': '-10042:4'},
 'reply': {'cli': {'exit_code': 0,
                   'payload': {'data': {'request': 1},
                               'kind': 'write',
                               'ok': True,
                               'op': 'topic.close',
                               'state': 'queued',
                               'text': 'Close confirmation queued. Torii will send the result '
                                       'here.'}},
           'mcp': {'content': [{'text': 'Close confirmation queued. Torii will send the result '
                                        'here.\n'
                                        '{"request":1}',
                                'type': 'text'}],
                   'isError': False}}}


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
        self.key = 'coordinator'
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
            self.store.put('coordinator_model', 'claude-opus-5-5')
            self.store.put('claude_parent_effort', 'max')
            self.store.put('codex_parent_effort', 'max')
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
            session.key = key
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
    async def test_nonessential_loop_pauses_once_and_message_delivery_and_workers_continue(self):
        names = {'command_menu': 'command menu', 'reactions': 'reactions', 'signins': 'account sign-in',
                 'tldr': 'summaries', 'worktree_cleanup': 'worktree cleanup',
                 'host_logs': 'host log cleanup', 'session_cleanup': 'native session cleanup', 'waits': 'wait triggers'}
        if self.store.code_requests is not None:
            names.update(self.store.code_requests.loop_names())
        self.enable_claude_account()
        factory = self.service.session_factory
        for name in names:
            with self.subTest(loop=name):
                self.service = Service(self.store, self.telegram, self.runner, self.root, session_factory=factory)
                self.service.task_restart_backoff = 0
                failing = AsyncMock(side_effect=sqlite3.InterfaceError('Synthetic loop error'))
                core = {'poll', 'deliver', 'feed', 'workers'}
                loops = [(key, failing if key == name else start if key in core else asyncio.Event().wait)
                         for key, start in self.service.supervised()]
                self.service.supervised = lambda: loops
                notice = ('Torii paused its %s after repeated errors and will retry it by itself. '
                          'Everything else keeps running.') % names[name]
                before = self.store.db.execute('SELECT MAX(id) FROM problems').fetchone()[0] or 0

                async def until(condition):
                    while not condition() and not run.done():
                        await asyncio.sleep(.01)

                with patch.object(self.service.extension, 'start', AsyncMock()), \
                        patch.object(self.service.extension, 'close', AsyncMock()), \
                        self.assertLogs('coordinator', 'ERROR'):
                    run = asyncio.create_task(self.service.run())
                    try:
                        await asyncio.wait_for(until(lambda: any(
                            row['text'] == notice or row['text'].startswith('Torii stopped its ' + name + ' loop')
                            for row in self.telegram.sent)), 3)
                        self.assertIn(notice, [row['text'] for row in self.telegram.sent])
                        self.assertFalse(run.done(), 'The %s loop stopped Torii after six failures' % name)
                        self.assertEqual(failing.await_count, 6)
                        rows = self.store.db.execute(
                            "SELECT code,detail FROM problems WHERE area='service' AND id>? AND code!='started'",
                            (before,)).fetchall()
                        self.assertEqual([row['code'] for row in rows], ['loop-crashed'] * 6 + ['loop-stopped'])
                        self.assertIn('loop=%s InterfaceError: Synthetic loop error' % name, rows[-1]['detail'])
                        update_id = 9000 + list(names).index(name)
                        self.telegram.pending_updates.append(owner_update(update_id, 'continue after ' + name))
                        worker = self.worker()
                        await asyncio.wait_for(until(lambda: any(
                            'continue after ' + name in text for session in self.sessions for _, text in session.sent)
                            and self.store.db.execute('SELECT status FROM workers WHERE id=?',
                                                      (worker,)).fetchone()[0] == 'done'), 3)
                        self.assertFalse(run.done())
                        self.assertTrue(any('continue after ' + name in text
                                            for session in self.sessions for _, text in session.sent))
                        result = self.store.db.execute('SELECT status,result FROM workers WHERE id=?', (worker,)).fetchone()
                        self.assertEqual(result['status'], 'done')
                        self.assertTrue(json.loads(result['result'])['success'])
                        with self.store.db:
                            self.store.enqueue_report(TOPIC, 'delivery after ' + name)
                        self.service.wake('deliver').set()
                        self.service.wake(name).set()
                        await asyncio.wait_for(until(lambda: any(
                            row['text'] == 'delivery after ' + name for row in self.telegram.sent)), 3)
                        self.assertFalse(run.done())
                        self.assertEqual(failing.await_count, 6)
                        notices = [row for row in self.telegram.sent if row['text'] == notice]
                        self.assertEqual([(row['topic'], row['text']) for row in notices], [(TOPIC, notice)])
                        self.assertEqual(self.store.db.execute(
                            'SELECT COUNT(*) FROM outbox WHERE topic=? AND text=?', (TOPIC, notice)).fetchone()[0], 1)
                        self.assertEqual(self.store.db.execute(
                            "SELECT COUNT(*) FROM problems WHERE area='service' AND code='loop-stopped' AND id>?",
                            (before,)).fetchone()[0], 1)
                        self.assertEqual(self.store.get('service_run')['state'], 'running')
                    finally:
                        run.cancel()
                        await asyncio.gather(run, return_exceptions=True)

    async def test_paused_loop_starts_normally_on_next_service_run(self):
        self.service.task_restart_backoff = 0
        start = AsyncMock(side_effect=RuntimeError('Synthetic cleanup error'))
        self.service.supervised = lambda: [('host_logs', start), ('deliver', self.service.deliver)]
        notice = ('Torii paused its host log cleanup after repeated errors and will retry it by itself. '
                  'Everything else keeps running.')

        async def until_notice():
            while not any(row['text'] == notice or row['text'].startswith('Torii stopped its host_logs loop')
                          for row in self.telegram.sent) and not run.done():
                await asyncio.sleep(.01)

        with patch.object(self.service.extension, 'start', AsyncMock()), \
                patch.object(self.service.extension, 'close', AsyncMock()), self.assertLogs('coordinator', 'ERROR'):
            run = asyncio.create_task(self.service.run())
            try:
                await asyncio.wait_for(until_notice(), 3)
                self.assertIn(notice, [row['text'] for row in self.telegram.sent])
                self.assertFalse(run.done(), 'The cleanup failure stopped the first service run')
            finally:
                run.cancel()
                await asyncio.gather(run, return_exceptions=True)
        started = asyncio.Event()

        async def healthy():
            started.set()
            await asyncio.Event().wait()

        start.side_effect = healthy
        with patch.object(self.service.extension, 'start', AsyncMock()), \
                patch.object(self.service.extension, 'close', AsyncMock()):
            run = asyncio.create_task(self.service.run())
            try:
                await asyncio.wait_for(started.wait(), 3)
                self.assertFalse(run.done())
                self.assertEqual(start.await_count, 7)
                self.assertEqual(self.store.get('service_run')['state'], 'running')
                self.assertEqual(len([row for row in self.telegram.sent if row['text'] == notice]), 1)
                self.assertEqual(self.store.db.execute(
                    "SELECT COUNT(*) FROM problems WHERE code='loop-stopped'").fetchone()[0], 1)
            finally:
                run.cancel()
                await asyncio.gather(run, return_exceptions=True)

    async def test_essential_loop_failure_still_stops_service(self):
        factory = self.service.session_factory
        for name in ('poll', 'restart_watch', 'updates'):
            with self.subTest(loop=name):
                self.service = Service(self.store, self.telegram, self.runner, self.root, session_factory=factory)
                self.service.task_restart_backoff = 0
                failing = AsyncMock(side_effect=RuntimeError('Synthetic essential loop error'))
                stopped = asyncio.Event()
                before = self.store.db.execute('SELECT MAX(id) FROM problems').fetchone()[0] or 0

                async def other_loop():
                    try:
                        await asyncio.Event().wait()
                    finally:
                        stopped.set()

                self.service.supervised = lambda: [(name, failing), ('deliver', other_loop)]
                with patch.object(self.service.extension, 'start', AsyncMock()), \
                        patch.object(self.service.extension, 'close', AsyncMock()), \
                        self.assertLogs('coordinator', 'ERROR'), self.assertRaisesRegex(
                            _TaskFailed, 'The %s service loop failed 6 times.' % name):
                    await asyncio.wait_for(self.service.run(), 3)
                self.assertEqual(failing.await_count, 6)
                self.assertTrue(stopped.is_set())
                codes = [row['code'] for row in self.store.db.execute(
                    "SELECT code FROM problems WHERE area='service' AND id>? ORDER BY id", (before,))]
                self.assertEqual(codes, ['started'] + ['loop-crashed'] * 6 + ['loop-failed'])
                self.assertEqual(self.store.get('service_run')['cause'],
                                 'loop failed: The %s service loop failed 6 times.' % name)
                self.assertEqual(list(self.store.db.execute('SELECT text FROM outbox')), [])

    async def test_nonessential_loop_retries_with_capped_cooldown_and_one_notice(self):
        self.service.task_restart_backoff = 0
        clock = SimpleNamespace(now=0)
        clock.monotonic = lambda: clock.now
        sleeps = []
        attempts = 0

        async def failing():
            nonlocal attempts
            attempts += 1
            if attempts > 10:
                return 'recovered'
            clock.now += 100
            raise RuntimeError('Synthetic repeated failure')

        async def sleep(seconds):
            sleeps.append(seconds)

        with patch('coordinator.service.time', clock), \
                patch('coordinator.service.asyncio', SimpleNamespace(sleep=sleep, CancelledError=asyncio.CancelledError, Event=asyncio.Event)), \
                self.assertLogs('coordinator', 'ERROR'):
            result = await self.service.supervise('host_logs', failing)
        self.assertEqual(result, 'recovered', 'The loop must retry after its cool-down')
        self.assertEqual(attempts, 11)
        self.assertEqual(sleeps, [0] * 5 + [900, 1800, 3600, 7200, 7200])
        rows = self.problems()
        self.assertEqual([row['code'] for row in rows], ['loop-crashed'] * 6 + ['loop-stopped'] + ['loop-crashed'] * 4)
        notices = list(self.store.db.execute('SELECT topic,text FROM outbox'))
        self.assertEqual([(row['topic'], row['text']) for row in notices], [(TOPIC,
            'Torii paused its host log cleanup after repeated errors and will retry it by itself. '
            'Everything else keeps running.')])

    async def test_nonessential_loop_resets_crash_budget_after_healthy_running(self):
        self.service.task_restart_backoff = 0
        clock = SimpleNamespace(now=0)
        clock.monotonic = lambda: clock.now
        attempts = 0
        sleeps = []

        async def failing():
            nonlocal attempts
            attempts += 1
            if attempts == 16:
                return 'recovered'
            if attempts in (6, 11):
                clock.now += 600
            raise RuntimeError('Synthetic isolated failure')

        async def sleep(seconds):
            sleeps.append(seconds)

        with patch('coordinator.service.time', clock), \
                patch('coordinator.service.asyncio', SimpleNamespace(sleep=sleep, CancelledError=asyncio.CancelledError, Event=asyncio.Event)), \
                self.assertLogs('coordinator', 'ERROR'):
            result = await self.service.supervise('worktree_cleanup', failing)
        self.assertEqual(result, 'recovered', 'Ten healthy minutes must reset the consecutive crash budget')
        self.assertEqual(attempts, 16)
        self.assertEqual(sleeps, [0] * 15)
        self.assertEqual([row['code'] for row in self.problems()], ['loop-crashed'] * 15)
        self.assertIn('attempt=1', self.problems()[5]['detail'])
        self.assertIn('attempt=1', self.problems()[10]['detail'])
        self.assertEqual(list(self.store.db.execute('SELECT text FROM outbox')), [])

    async def test_nonessential_pause_reporting_survives_database_errors(self):
        from coordinator import problems
        record = problems.record
        enqueue = self.store.enqueue_report
        self.service.task_restart_backoff = 0
        for target in ('problem', 'notice'):
            with self.subTest(write=target):
                failing = AsyncMock(side_effect=[RuntimeError('Synthetic cleanup failure')] * 6 + ['recovered'])

                def save(store, area, code, *args, **kwargs):
                    if target == 'problem' and code == 'loop-stopped':
                        raise sqlite3.OperationalError('Synthetic database full')
                    return record(store, area, code, *args, **kwargs)

                def send(topic, text, *args, **kwargs):
                    if target == 'notice' and text.startswith(('Torii stopped its', 'Torii paused its')):
                        raise sqlite3.OperationalError('Synthetic database full')
                    return enqueue(topic, text, *args, **kwargs)

                with patch('coordinator.service.problems.record', side_effect=save), \
                        patch.object(self.store, 'enqueue_report', side_effect=send), \
                        patch('coordinator.service.asyncio', SimpleNamespace(
                            sleep=AsyncMock(), CancelledError=asyncio.CancelledError, Event=asyncio.Event)), \
                        self.assertLogs('coordinator', 'ERROR'):
                    result = await self.service.supervise('host_logs', failing)
                self.assertEqual(result, 'recovered')
                self.assertEqual(failing.await_count, 7)

    async def test_nonessential_crashes_keep_sanitized_pause_diagnostics(self):
        self.service.task_restart_backoff = 0
        error = sqlite3.InterfaceError('Synthetic cleanup failure for fixture@example.invalid')
        failing = AsyncMock(side_effect=[error] * 6 + ['recovered'])
        with patch('coordinator.service.asyncio', SimpleNamespace(
                sleep=AsyncMock(), CancelledError=asyncio.CancelledError, Event=asyncio.Event)), self.assertLogs('coordinator', 'ERROR'):
            await self.service.supervise('host_logs', failing)
        rows = self.problems()
        self.assertEqual([row['code'] for row in rows], ['loop-crashed'] * 6 + ['loop-stopped'])
        for row in rows:
            self.assertEqual(row['area'], 'service')
            self.assertIn('loop=host_logs', row['detail'])
            self.assertIn('InterfaceError: Synthetic cleanup failure for [email]', row['detail'])
            self.assertNotIn('fixture@example.invalid', row['detail'])

    def test_opencode_pause_texts_name_the_account_and_no_fallback(self):
        from coordinator.session import AccountsUnavailable
        from datetime import datetime
        topic = '-10042:5'
        key = 'coordinator:' + topic + '_selection'
        self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                              (topic, -10042, 5, 'OpenCode', str(self.root)))
        self.store.put(key, {'provider': 'opencode', 'model': None, 'effort': None, 'revision': 1})
        reset = time.time() + 3600
        when = datetime.fromtimestamp(reset).astimezone().strftime('%Y-%m-%d %H:%M %Z')
        cases = [
            (False, 'one', {}, 'rate_limited', 'OpenCode is off. Turn on delegation.opencode to continue.'),
            (True, None, {}, 'rate_limited', 'no OpenCode account is selected. Select one with opencode.use to continue.'),
            (True, 'one', {'provider': 'opencode-go'}, 'quota',
             "OpenCode subscription account one is out of usage until %s. I'll continue at the reset." % when),
            (True, 'one', {}, 'rate_limited',
             "the host of OpenCode account one is rate limited. I'll try again at %s." % when),
            (True, 'one', {}, 'out_of_credit',
             "OpenCode account one is out of credit at its host. Add credit there. I'll check again every five minutes."),
            (True, 'one', {}, 'auth',
             "the key of OpenCode account one failed. Fix the key file. I'll check again every five minutes.")]
        for enabled, alias, account, reason, text in cases:
            with self.subTest(text=text):
                self.store.put('account_limit_notices', {})
                self.store.put('opencode_enabled', enabled)
                self.store.put('opencode_active_account', alias)
                self.store.put('opencode_accounts', {'one': account})
                self.store.put('opencode_account_blocks', {'one': {'reason': reason, 'until': reset}})
                self.service.pause_parent(topic, AccountsUnavailable(reset, provider='opencode', alias=alias))
                notice = self.store.get(key + '_notice')
                self.assertEqual(notice['text'], 'This chat is paused: ' + text +
                                 ' Torii does not move this chat to another agent. Your messages are saved.')
                self.assertEqual(self.store.get(key + '_retry_at'), reset)
                self.assertEqual(self.store.db.execute('SELECT text FROM outbox ORDER BY id DESC LIMIT 1').fetchone()[0],
                                 notice['text'])

    async def test_opencode_continuity_label(self):
        for source, target, expected in (
                ('claude', 'opencode', 'Claude to OpenCode'),
                ('opencode', 'codex', 'OpenCode to ChatGPT'),
                ('opencode', 'opencode', 'OpenCode to OpenCode')):
            with self.subTest(source=source, target=target):
                self.service.sessions.clear()
                session = FakeSession(str(uuid.uuid4()))
                session.switched = True
                session.switch = {'from': source, 'to': target, 'reason': 'the owner changed the pick'}
                self.service.session_factory = AsyncMock(return_value=session)
                await self.service.start_session(TOPIC)
                notes = [row['text'] for row in self.store.messages_pending() if row['kind'] == 'restarted']
                self.assertEqual(len(notes), 1)
                self.assertTrue(notes[0].startswith('This chat moved from ' + expected +
                                ' because the owner changed the pick. Your earlier conversation is not in this context.\n\n'))

    async def test_d4_chat_policy_next_message_clear_isolation_and_rerouted_result(self):
        from coordinator import control_api
        from coordinator.policy import chat_policy, usage_policy
        other = '-10042:9'
        self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?, -10042, 9, ?, ?, 1)',
                              (other, 'Other', str(self.root)))
        self.store.db.commit()
        home = await self.service.start_session(TOPIC)
        sibling = await self.service.start_session(other)
        sibling.idle = lambda: True
        async def deliver(topic, text, kind='owner'):
            self.store.message_save(topic, kind, text)
            await self.service.feed_once()
            await asyncio.gather(*self.service.feed_tasks.values())
        with self.store.db:
            self.assertTrue(control_api.call(self.store, 'policy.chat', {'topic': TOPIC, 'text': 'Home text.'}).ok)
        await deliver(TOPIC, 'changed')
        self.assertTrue(home.sent[-1][1].endswith('Current usage policy:\n' + chat_policy(self.store, 'coordinator')))
        await deliver(TOPIC, 'again')
        self.assertNotIn('Current usage policy:', home.sent[-1][1])
        await deliver(other, 'sibling')
        self.assertNotIn('Current usage policy:', sibling.sent[-1][1])
        with self.store.db:
            self.assertTrue(control_api.call(self.store, 'policy.chat', {'topic': TOPIC}).ok)
        await deliver(TOPIC, 'cleared')
        self.assertTrue(home.sent[-1][1].endswith('Current usage policy:\n' + usage_policy(self.store.directory)))
        self.store.put('coordinator_delegation', {'text': 'Home reroute.', 'revision': 1})
        self.store.put('coordinator:' + other + '_delegation', {'text': 'Disabled text.', 'revision': 1})
        self.store.db.execute('UPDATE topics SET enabled=0 WHERE id=?', (other,))
        self.store.db.commit()
        await deliver(other, 'worker finished', 'worker_result')
        self.assertTrue(home.sent[-1][1].endswith('Current usage policy:\n' + chat_policy(self.store, 'coordinator')))
        self.assertNotIn('Disabled text.', home.sent[-1][1])
        self.assertEqual(len(self.sessions), 2)

    async def test_cli_event_without_service_reaches_its_topic_on_next_drain(self):
        other = '-10042:5'
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', (other, -10042, 5, 'Atlas', str(self.root)))
        result = subprocess.run([sys.executable, '-m', 'coordinator', '--state-dir', str(self.store.directory),
                                 '--token-file', str(self.root / 'no-token'), 'ctl', 'call', 'events.post',
                                 'topic=' + other, 'text=Approval needed.'],
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        data = json.loads(result.stdout)
        self.assertTrue(data['ok'])
        event, = self.store.messages_pending()
        self.assertEqual(data['data'], {'message': event['id']})
        self.assertFalse((self.store.directory / 'service.lock').exists())
        await self.drain_feed()
        session = self.service.sessions[other]
        self.assertEqual(session.sent, [(message_uuid(event['id']),
                                        tag(event['id'], 'event', other, 'Atlas') + 'Approval needed.')])
        self.assertNotIn(TOPIC, self.service.sessions)
        self.assertEqual(self.store.db.execute('SELECT delivered FROM messages WHERE id=?',
                                               (event['id'],)).fetchone()[0], 'received')
        self.assertEqual(self.store.messages_pending(), [])
        self.assertFalse(await self.service.deliver_once())
        self.assertEqual(self.telegram.sent, [])

    async def test_feed_notices_cli_event_while_idle_without_a_telegram_update(self):
        observed = asyncio.Event()
        from coordinator.service import FEED_IDLE, pause
        self.assertEqual(FEED_IDLE, 0.1)

        async def post_during_idle(wake, seconds):
            if not observed.is_set():
                self.store.message_event(TOPIC, 'Lane stopped.')
                observed.set()
            await pause(wake, seconds)

        with patch('coordinator.service.pause', side_effect=post_during_idle):
            await self.run_until([self.service.feed], lambda: bool(self.sessions and self.sessions[0].sent))
        self.assertTrue(observed.is_set())
        self.assertIn('kind=event] Lane stopped.', self.sessions[0].sent[0][1])
        self.assertEqual(self.telegram.calls, [])
        self.assertEqual(self.telegram.sent, [])

    async def test_event_gets_no_reaction_telegram_send_or_owner_turn_counters(self):
        from coordinator.reactions import finish_turn, handed_off, parent_tools, reaction_target, set_desired
        event = self.store.message_event(TOPIC, 'Lane stopped.')
        await self.drain_feed()
        with self.store.db:
            handed_off(self.store, event['id'])
            parent_tools(self.store, [event['id']], [{'type': 'tool_use', 'name': 'Agent'}])
            set_desired(self.store, event['id'], '👀')
            finish_turn(self.store, [event['id']])
        self.assertIsNone(reaction_target(self.store, event['id']))
        row = self.store.db.execute('SELECT * FROM messages WHERE id=?', (event['id'],)).fetchone()
        self.assertEqual((row['reaction_state'], row['reaction_desired'], row['turn_started'],
                          row['turn_eyes'], row['turn_tools'], row['turn_outbox']),
                         ('sent', None, None, 0, 0, None))
        self.assertFalse(await deliver_reaction(self.store, self.telegram))
        self.assertFalse(await self.service.deliver_once())
        self.assertEqual(self.telegram.sent, [])
        self.assertEqual(self.telegram.calls, [])
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 0)

    async def test_uncertain_event_has_the_same_restart_delivery_as_a_worker_result(self):
        session = await self.service.start_session(TOPIC)
        with patch.object(session, 'send', new=AsyncMock(return_value='uncertain')):
            rows = [self.store.message_save(TOPIC, kind, 'Delivery uncertain.')
                    for kind in ('worker_result', 'event')]
            await self.drain_feed()
        await session.stop()
        await self.service.start_session(TOPIC)
        for row in rows:
            self.assertEqual(self.store.db.execute('SELECT delivered FROM messages WHERE id=?',
                                                   (row['id'],)).fetchone()[0], 'uncertain')
        self.assertFalse(any(row['kind'] in ('event', 'worker_result') for row in self.store.messages_pending()))
        self.assertEqual(self.telegram.sent, [])

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

    async def test_command_menu_tracks_telegram_update_setting(self):
        self.service.poll_succeeded = True
        sleeps = []
        def wait(seconds):
            sleeps.append(seconds)
            if len(sleeps) == 1:
                self.store.put('telegram_updates_enabled', True)
            else:
                raise asyncio.CancelledError()
        with patch('coordinator.service.asyncio.sleep', side_effect=wait):
            with self.assertRaises(asyncio.CancelledError):
                await self.service.command_menu()
        commands = [params['commands'] for method, params in self.telegram.calls if method == 'setMyCommands']
        self.assertNotIn('update', [item['command'] for item in commands[0]])
        self.assertIn('update', [item['command'] for item in commands[1]])

    async def test_successful_poll_marks_update_healthy_at_startup_commit(self):
        from coordinator import updates
        self.store.put(updates.KEY, {'id': 'test', 'phase': 'restart', 'old_pid': -1})
        self.service.updater.commit = 'b' * 40
        self.telegram.updates = AsyncMock(side_effect=[[], asyncio.CancelledError()])
        with patch.object(updates.watch, 'process_identity', return_value='new process'):
            with self.assertRaises(asyncio.CancelledError):
                await self.service.poll()
        self.assertEqual(self.store.get(updates.KEY)['health']['commit'], 'b' * 40)
        self.assertEqual(self.store.get(updates.KEY)['health']['process'], 'new process')

    async def test_usage_provider_switch_keeps_continuity_without_a_topic_notice(self):
        saved = str(uuid.uuid4())
        self.store.put('coordinator_session', saved)
        session = FakeSession(str(uuid.uuid4()))
        session.switched = True
        session.switch = {'from': 'claude', 'to': 'codex', 'reason': 'every Claude account is out of usage'}
        self.service.session_factory = AsyncMock(return_value=session)
        await self.service.start_session(TOPIC)
        notes = [row['text'] for row in self.store.messages_pending() if row['kind'] == 'restarted']
        self.assertEqual(len(notes), 1)
        self.assertIn('This chat moved from Claude to ChatGPT', notes[0])
        self.assertIn('Channel continuity:', notes[0])
        self.assertNotIn('The service restarted.', notes[0])
        await self.service.start_session(TOPIC)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM outbox WHERE topic=? "
                                               "AND text LIKE 'This chat now runs%'", (TOPIC,)).fetchone()[0], 0)

    def continuity_jobs(self):
        other = '-10042:5'
        self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                              (other, -10042, 5, 'Other', str(self.root)))
        closed = self.store.task_create(TOPIC, 'Closed job')
        self.store.task_update(closed['id'], status='done')
        jobs = []
        for number in range(59):
            topic = TOPIC if number % 2 == 0 else other
            job = self.store.task_create(topic, 'Job %02d "quoted" é 🦊 ' % number + 'x' * 200,
                                         notes='old notes ' + 'n' * 600)
            with self.store.db:
                self.store.db.execute('UPDATE tasks SET updated=? WHERE id=?', (number, job['id']))
            jobs.append(dict(job, updated=number))
        job = self.store.task_create(TOPIC, 'Ship continuity', notes='next: merge after CI')
        self.store.task_update(job['id'], stage='reviewed')
        self.store.task_update(job['id'], stage='ci')
        with self.store.db:
            self.store.db.execute('UPDATE tasks SET updated=1000 WHERE id=?', (job['id'],))
            self.store.db.executemany('''INSERT INTO workers
                (task,topic,provider,prompt,status,work,created,updated) VALUES (?,?,?,?,?,?,?,?)''',
                [(closed['id'], TOPIC, 'codex', 'closed-job-work-%d' % number,
                  'interrupted', 'development', 1, 1) for number in range(200)])
            finished = self.store.db.execute('''INSERT INTO workers
                (task,topic,provider,prompt,status,work,result,created,updated) VALUES (?,?,?,?,?,?,?,?,?)''',
                (job['id'], TOPIC, 'codex', 'Open PR', 'done', 'development', json.dumps({
                    'success': True, 'failure_code': None, 'text': 'PR 12 opened' + 'r' * 300,
                    'transcript_path': str(self.root / 'worker-transcript.jsonl')}), 2, 2)).lastrowid
            running = self.store.db.execute('''INSERT INTO workers
                (task,topic,provider,prompt,status,work,created,updated) VALUES (?,?,?,?,?,?,?,?)''',
                (job['id'], TOPIC, 'claude', 'Check CI', 'running', 'review', 3, 3)).lastrowid
        jobs.append(dict(job, updated=1000))
        return jobs, finished, running

    async def continuity_notice(self, mode, topic=TOPIC, full_tail=False):
        key = 'coordinator' if topic == TOPIC else 'coordinator:' + topic
        self.store.put(key + '_session', 'saved-session')
        reports = self.store.db.execute("SELECT count(*) FROM outbox WHERE topic=? "
                                       "AND text LIKE 'This chat now runs%'", (topic,)).fetchone()[0]
        previous = self.service.sessions.get(topic)
        if previous:
            previous.closed = True
        session = FakeSession('saved-session')
        if mode == 'switch':
            session.switched = True
            session.switch = {'from': 'claude', 'to': 'codex', 'reason': 'the owner selected ChatGPT'}
        elif mode == 'fallback':
            session.resume_failed = True
        else:
            for _ in range(10):
                row = self.store.message_save(topic, 'owner' if full_tail else 'restarted',
                                              'stale-notice-' + 'x' * 499987)
                self.store.db.execute("UPDATE messages SET delivered='uncertain' WHERE id=?", (row['id'],))
        if full_tail and mode != 'restart':
            for _ in range(5):
                row = self.store.message_save(topic, 'owner', 'recent-message-' + 'x' * 500)
                self.store.message_delivered(row['id'], 'received')
        self.service.session_factory = AsyncMock(return_value=session)
        await self.service.start_session(topic)
        notices = [row for row in self.store.messages_pending()
                   if row['topic'] == topic and row['kind'] == 'restarted']
        self.assertEqual(len(notices), 1)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM outbox WHERE topic=? "
                                              "AND text LIKE 'This chat now runs%'", (topic,)).fetchone()[0],
                         reports + (mode == 'switch'))
        return notices[0], session

    async def test_parent_jobs_summary_bounds_both_continuity_notices(self):
        jobs, finished, running = self.continuity_jobs()
        transcript = self.root / 'worker-transcript.jsonl'
        transcript.write_text('transcript-only-content')
        original_mode = transcript.stat().st_mode
        transcript.chmod(0)
        try:
            with self.assertRaises(PermissionError):
                with open(transcript) as handle:
                    handle.read()
            for mode in ('switch', 'restart', 'fallback'):
                notice, session = await self.continuity_notice(mode, full_tail=True)
                with self.subTest(mode=mode):
                    prompt = notice['text']
                    self.assertLessEqual(len(prompt), 8000)
                    self.assertLessEqual(len(prompt), parent_state.RESTART_NOTICE_CHARS)
                    self.assertGreater(len(prompt), 7000)
                    self.assertIn('stale-notice-' if mode == 'restart' else 'recent-message-', prompt)
                    if mode != 'restart':
                        tail = json.loads(prompt.split('. Recent messages: ', 1)[1])
                        self.assertEqual(len(tail), 5)
                        self.assertTrue(all(len(row['text']) == 500 for row in tail))
                    else:
                        tail = json.JSONDecoder().raw_decode(prompt.split('characters): ', 1)[1])[0]
                        self.assertEqual((tail['count'], len(tail['shown']), tail['not_shown']), (10, 7, 3))
                    summary = json.loads(prompt.split('Open jobs: ', 1)[1].split('. ', 1)[0])
                    self.assertEqual(summary['count'], 60)
                    self.assertEqual(summary['not_shown'], 60 - len(summary['shown']))
                    self.assertLessEqual(len(summary['shown']), parent_state.RESTART_TASKS)
                    shown = summary['shown']
                    expected_order = sorted(jobs, key=lambda row: (row['updated'], row['id']), reverse=True)
                    self.assertEqual([row['id'] for row in shown],
                                     [row['id'] for row in expected_order[:len(shown)]])
                    self.assertEqual(shown[0], {
                        'id': jobs[-1]['id'], 'topic': TOPIC, 'number': jobs[-1]['number'],
                        'title': 'Ship continuity', 'stage': 'ci', 'notes': 'next: merge after CI',
                        'finished': {'id': finished, 'success': True, 'failure_code': None,
                                     'text': ('PR 12 opened' + 'r' * 300)[:200]},
                    })
                    self.assertEqual(summary['live_workers'], [
                        {'id': running, 'task': jobs[-1]['id'], 'provider': 'claude',
                         'status': 'running', 'work': 'review'}])
                    self.assertEqual(summary['live_workers_not_shown'], 0)
                    listed_workers = [worker['id'] for worker in summary['live_workers']]
                    self.assertTrue(set(range(1, 201)).isdisjoint(listed_workers))
                    self.assertNotIn('closed-job-work-', prompt)
                    self.assertNotIn('transcript-only-content', prompt)
                    tagged = message_tag(dict(self.store.topic(TOPIC)), notice) + prompt
                    event = NativeProtocol.user(session, message_uuid(notice['id']), tagged)
                    wire = (json.dumps({'op': 'write', 'line': json.dumps(event)}) + '\n').encode()
                    self.assertLess(len(wire), 32 * 1024)
        finally:
            transcript.chmod(original_mode)

    async def test_parent_jobs_summary_keeps_oversized_newest_job_with_full_tails(self):
        self.store.task_create(TOPIC, 'Older job')
        newest = self.store.task_create(TOPIC, '界🦊' * 100, notes='界🦊' * 250)
        with self.store.db:
            self.store.db.execute('''INSERT INTO workers
                (task,topic,provider,prompt,status,result,created,updated) VALUES (?,?,?,?,?,?,?,?)''',
                (newest['id'], TOPIC, 'codex', 'Finished', 'done', json.dumps({
                    'success': True, 'text': '界🦊' * 100}), 1, 1))
        for mode in ('restart', 'switch'):
            notice, _ = await self.continuity_notice(mode, full_tail=True)
            with self.subTest(mode=mode):
                prompt = notice['text']
                summary = json.JSONDecoder().raw_decode(prompt.split('Open jobs: ', 1)[1])[0]
                self.assertEqual([row['id'] for row in summary['shown'][:1]], [newest['id']])
                self.assertEqual(summary['count'], 2)
                self.assertEqual(summary['not_shown'], 2 - len(summary['shown']))
                self.assertEqual(summary['shown'][0]['title'], '界🦊' * 100)
                if mode == 'restart':
                    self.assertEqual(summary['shown'][0]['notes'], '')
                    text = summary['shown'][0]['finished']['text']
                    self.assertTrue(text.endswith('…'))
                    self.assertLess(len(text), 200)
                else:
                    self.assertTrue(summary['shown'][0]['notes'].endswith('…'))
                    self.assertEqual(summary['shown'][0]['finished']['text'], '界🦊' * 100)
                self.assertIn('recent-message-' if mode == 'switch' else 'stale-notice-', prompt)
                self.assertLessEqual(len(prompt), parent_state.RESTART_NOTICE_CHARS)

    def test_parent_jobs_summary_trims_oldest_workers_and_counts_all_omissions(self):
        job = self.store.task_create(TOPIC, 'Worker trim')
        with self.store.db:
            self.store.db.executemany('''INSERT INTO workers
                (task,topic,provider,prompt,status,work,created,updated) VALUES (?,?,?,?,?,?,?,?)''',
                [(job['id'], TOPIC, 'codex', 'Check', 'queued', 'development', 1, 1) for _ in range(35)])
        expected = {'count': 1, 'not_shown': 1, 'shown': [], 'live_workers_not_shown': 32,
                    'live_workers': [{'id': worker, 'task': job['id'], 'provider': 'codex',
                                      'status': 'queued', 'work': 'development'} for worker in (35, 34, 33)]}
        limit = len(json.dumps(expected, separators=(',', ':'), sort_keys=True))
        summary = parent_state.jobs_summary(self.store, [job], limit)
        self.assertEqual(json.loads(summary), expected)
        self.assertLessEqual(len(summary), limit)

    async def test_parent_jobs_summary_puts_older_live_job_before_ten_newer_jobs(self):
        for number in range(10):
            self.store.task_create(TOPIC, 'Newer job %d' % number, notes='long notes ' + 'n' * 1000)
        older = self.store.task_create(TOPIC, 'Older live job', notes='still running')
        with self.store.db:
            self.store.db.execute('UPDATE tasks SET updated=0 WHERE id=?', (older['id'],))
            worker = self.store.db.execute('''INSERT INTO workers
                (task,topic,provider,prompt,status,work,created,updated) VALUES (?,?,?,?,?,?,?,?)''',
                (older['id'], TOPIC, 'codex', 'Build', 'running', 'development', 1, 1)).lastrowid
        for mode in ('restart', 'fallback'):
            notice, _ = await self.continuity_notice(mode, full_tail=True)
            with self.subTest(mode=mode):
                summary = json.JSONDecoder().raw_decode(notice['text'].split('Open jobs: ', 1)[1])[0]
                self.assertEqual(summary['shown'][0]['id'], older['id'])
                self.assertEqual(summary['live_workers'], [
                    {'id': worker, 'task': older['id'], 'provider': 'codex',
                     'status': 'running', 'work': 'development'}])
                self.assertLess(len(summary['shown']), 11)

    async def test_parent_jobs_summary_keeps_live_workers_when_last_job_is_cut(self):
        jobs = [self.store.task_create(TOPIC, 'Overflow %02d ' % number + 't' * 20, notes='long notes ' + 'n' * 1000)
                for number in range(12)]
        with self.store.db:
            self.store.db.executemany('''INSERT INTO workers
                (id,task,topic,provider,prompt,status,work,created,updated) VALUES (?,?,?,?,?,?,?,?,?)''',
                [(100001 + number, jobs[-1]['id'], TOPIC, 'codex', 'Old work',
                  'interrupted', 'development', 1, 1) for number in range(6)])
            live = []
            for job in jobs:
                worker = self.store.db.execute('''INSERT INTO workers
                    (task,topic,provider,prompt,status,work,created,updated) VALUES (?,?,?,?,?,?,?,?)''',
                    (job['id'], TOPIC, 'codex', 'Build', 'running', 'development', 1, 1)).lastrowid
                live.append({'id': worker, 'task': job['id'], 'provider': 'codex',
                             'status': 'running', 'work': 'development'})
        for mode in ('restart', 'fallback', 'switch'):
            notice, _ = await self.continuity_notice(mode, full_tail=True)
            with self.subTest(mode=mode):
                prompt = notice['text']
                summary = json.JSONDecoder().raw_decode(prompt.split('Open jobs: ', 1)[1])[0]
                self.assertEqual(summary['live_workers'], list(reversed(live)))
                self.assertEqual(summary['live_workers_not_shown'], 0)
                shown = summary['shown']
                self.assertGreater(len(shown), 1)
                self.assertLess(len(shown), 12)
                self.assertEqual(len(shown[0]['notes']), 500)
                self.assertLess(len(shown[-1]['notes']), 500)
                self.assertTrue(all(set(job) == {'id', 'topic', 'number', 'title', 'stage', 'notes', 'finished'}
                                    for job in shown))
                self.assertEqual(summary['not_shown'], 12 - len(shown))
                for worker in range(100001, 100007):
                    self.assertNotIn('"id":%d' % worker, prompt)
                self.assertNotIn('interrupted', prompt)
                self.assertLessEqual(len(prompt), parent_state.RESTART_NOTICE_CHARS)

    def test_parent_jobs_summary_lists_only_the_four_live_statuses_with_work(self):
        from coordinator.control_api import WORKER_STATUSES
        job = self.store.task_create(TOPIC, 'Worker statuses')
        workers = {}
        with self.store.db:
            for status in WORKER_STATUSES:
                workers[status] = self.store.db.execute('''INSERT INTO workers
                    (task,topic,provider,prompt,status,work,created,updated) VALUES (?,?,?,?,?,?,?,?)''',
                    (job['id'], TOPIC, 'codex', 'Build', status, 'development', 1, 1)).lastrowid
        summary = json.loads(parent_state.jobs_summary(self.store, [job]))
        self.assertEqual(summary['live_workers'], [
            {'id': workers[status], 'task': job['id'], 'provider': 'codex', 'status': status, 'work': 'development'}
            for status in ('running', 'waiting_for_quota', 'waiting_for_secret', 'queued')])
        self.assertEqual(summary['live_workers_not_shown'], 0)
        self.assertEqual(summary['shown'][0], {
            'id': job['id'], 'topic': TOPIC, 'number': job['number'], 'title': 'Worker statuses',
            'stage': None, 'notes': '', 'finished': {
                'id': workers['done'], 'success': None, 'failure_code': None, 'text': ''}})

    def test_parent_jobs_summary_caps_live_workers_at_thirty_newest(self):
        job = self.store.task_create(TOPIC, 'Worker cap')
        with self.store.db:
            self.store.db.executemany('''INSERT INTO workers
                (task,topic,provider,prompt,status,work,created,updated) VALUES (?,?,?,?,?,?,?,?)''',
                [(job['id'], TOPIC, 'codex', 'Build', 'running', 'development', 1, 1) for _ in range(35)])
        summary = json.loads(parent_state.jobs_summary(self.store, [job]))
        self.assertEqual([worker['id'] for worker in summary['live_workers']], list(range(35, 5, -1)))
        self.assertEqual(summary['live_workers_not_shown'], 5)
        self.assertEqual([row['id'] for row in summary['shown']], [job['id']])

    def test_parent_jobs_summary_keeps_workers_from_jobs_beyond_fifty(self):
        jobs = [self.store.task_create(TOPIC, 'Small job %d' % number) for number in range(60)]
        with self.store.db:
            self.store.db.executemany('''INSERT INTO workers
                (task,topic,provider,prompt,status,work,created,updated) VALUES (?,?,?,?,?,?,?,?)''',
                [(job['id'], TOPIC, 'codex', 'Build', 'running', 'development', 1, 1) for job in reversed(jobs)])
        summary = json.loads(parent_state.jobs_summary(self.store, jobs))
        self.assertEqual([worker['task'] for worker in summary['live_workers']], [job['id'] for job in jobs[:30]])
        self.assertEqual(summary['live_workers_not_shown'], 30)
        shown = {job['id'] for job in summary['shown']}
        self.assertLessEqual(len(shown), 50)
        self.assertNotIn(jobs[0]['id'], shown)
        self.assertEqual(summary['live_workers'][0]['task'], jobs[0]['id'])

    async def test_parent_jobs_summary_orders_by_updated_then_id(self):
        jobs = [self.store.task_create(TOPIC, title) for title in ('First', 'Second', 'Third')]
        with self.store.db:
            self.store.db.executemany('UPDATE tasks SET updated=? WHERE id=?',
                                      [(30, jobs[0]['id']), (10, jobs[1]['id']), (30, jobs[2]['id'])])
        for mode in ('switch', 'restart'):
            notice, _ = await self.continuity_notice(mode)
            with self.subTest(mode=mode):
                summary = json.JSONDecoder().raw_decode(notice['text'].split('Open jobs: ', 1)[1])[0]
                self.assertEqual([row['title'] for row in summary['shown']], ['Third', 'First', 'Second'])

    def test_parent_jobs_summary_cuts_text_in_priority_order_with_bounded_encodes(self):
        older = self.store.task_create(TOPIC, 'Older')
        title, notes, text = '界🦊' * 100, '界🦊' * 250, '界🦊' * 100
        job = self.store.task_create(TOPIC, title, notes=notes)
        with self.store.db:
            finished = self.store.db.execute('''INSERT INTO workers
                (task,topic,provider,prompt,status,result,created,updated) VALUES (?,?,?,?,?,?,?,?)''',
                (job['id'], TOPIC, 'codex', 'Finished', 'done', json.dumps({
                    'success': True, 'text': text}), 1, 1)).lastrowid
        for heads in ((title, '界🦊' * 3 + '…', text),
                      (title, '', '界🦊' * 3 + '…'), ('界🦊' * 3 + '…', '', ''), ('', '', '')):
            with self.subTest(heads=heads):
                expected = {'count': 1, 'not_shown': 0, 'live_workers': [], 'live_workers_not_shown': 0, 'shown': [{
                    'id': job['id'], 'topic': TOPIC, 'number': job['number'], 'stage': None,
                    'title': heads[0], 'notes': heads[1], 'finished': {
                        'id': finished, 'success': True, 'failure_code': None, 'text': heads[2]}}]}
                limit = len(json.dumps(expected, separators=(',', ':'), sort_keys=True))
                with patch.object(parent_state.json, 'dumps', wraps=json.dumps) as encode:
                    actual = parent_state.jobs_summary(self.store, [job], limit)
                self.assertEqual(json.loads(actual), expected)
                self.assertLessEqual(len(actual), limit)
                self.assertLessEqual(encode.call_count, 40)
        dropped = json.loads(parent_state.jobs_summary(self.store, [older, job], limit - 1))
        self.assertEqual(dropped, {'count': 2, 'not_shown': 2, 'shown': [],
                                   'live_workers': [], 'live_workers_not_shown': 0})

    async def test_parent_jobs_summary_empty_channel_and_fallback_queue_no_notice(self):
        other = '-10042:5'
        self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                              (other, -10042, 5, 'Other', str(self.root)))
        for fallback in (False, True):
            with self.subTest(fallback=fallback):
                session = FakeSession('new-session')
                session.resume_failed = fallback
                self.service.session_factory = AsyncMock(return_value=session)
                previous = self.service.sessions.get(other)
                if previous:
                    previous.closed = True
                self.assertIs(await self.service.start_session(other), session)
                self.assertEqual(list(self.store.messages_pending()), [])

    async def test_parent_jobs_summary_preserves_home_and_channel_scope(self):
        jobs, finished, running = self.continuity_jobs()
        other = '-10042:5'
        job = self.store.task_create(other, 'Other channel CI', notes='other channel notes')
        self.store.task_update(job['id'], stage='ci')
        with self.store.db:
            self.store.db.execute('UPDATE tasks SET updated=2000 WHERE id=?', (job['id'],))
            worker = self.store.db.execute('''INSERT INTO workers
                (task,topic,provider,prompt,status,work,created,updated) VALUES (?,?,?,?,?,?,?,?)''',
                (job['id'], other, 'codex', 'Other CI', 'running', 'development', 1, 1)).lastrowid
        for mode in ('switch', 'restart'):
            home, _ = await self.continuity_notice(mode)
            channel, _ = await self.continuity_notice(mode, other)
            with self.subTest(mode=mode):
                home_jobs = json.JSONDecoder().raw_decode(home['text'].split('Open jobs: ', 1)[1])[0]
                channel_jobs = json.JSONDecoder().raw_decode(channel['text'].split('Open jobs: ', 1)[1])[0]
                self.assertEqual(home_jobs['count'], 61)
                self.assertEqual(home_jobs['shown'][0]['id'], job['id'])
                self.assertEqual(home_jobs['live_workers'], [
                    {'id': worker, 'task': job['id'], 'provider': 'codex', 'status': 'running', 'work': 'development'},
                    {'id': running, 'task': jobs[-1]['id'], 'provider': 'claude', 'status': 'running', 'work': 'review'}])
                self.assertEqual(channel_jobs['count'], 30)
                self.assertEqual(channel_jobs['live_workers'], [
                    {'id': worker, 'task': job['id'], 'provider': 'codex', 'status': 'running', 'work': 'development'}])
                self.assertTrue(all(row['topic'] == other for row in channel_jobs['shown']))
                self.assertNotIn('Ship continuity', channel['text'])
                self.assertEqual(len([row for row in self.store.messages_pending()
                                      if row['kind'] == 'restarted']), 2)

    async def test_parent_jobs_summary_also_covers_resume_fallback(self):
        job = self.store.task_create(TOPIC, 'Fallback job', notes='next: merge after CI')
        self.store.task_update(job['id'], stage='ci')
        switched, _ = await self.continuity_notice('switch')
        fallback, _ = await self.continuity_notice('fallback')
        decode = json.JSONDecoder().raw_decode
        self.assertEqual(decode(fallback['text'].split('Open jobs: ', 1)[1])[0],
                         decode(switched['text'].split('Open jobs: ', 1)[1])[0])
        self.assertEqual(decode(fallback['text'].split('Open jobs: ', 1)[1])[0]['shown'][0]['stage'], 'ci')
        self.assertTrue(fallback['text'].startswith('Channel continuity: Open jobs: '))
        self.assertLessEqual(len(fallback['text']), parent_state.RESTART_NOTICE_CHARS)

    async def test_parent_jobs_summary_caps_heads_workers_and_uses_latest_finished_result(self):
        jobs, finished, running = self.continuity_jobs()
        job = jobs[-1]
        self.store.task_update(job['id'], title='Long title ' + 't' * 300)
        self.store.task_update(job['id'], notes='next: merge after CI' + 'n' * 600)
        with self.store.db:
            self.store.db.execute('UPDATE workers SET updated=10 WHERE id=?', (finished,))
            self.store.db.executemany('''INSERT INTO workers
                (task,topic,provider,prompt,status,work,created,updated) VALUES (?,?,?,?,?,?,?,?)''',
                [(job['id'], TOPIC, 'codex', 'Check CI', 'queued', 'development', 1, 1) for _ in range(90)])
            latest = self.store.db.execute('''INSERT INTO workers
                (task,topic,provider,prompt,status,work,result,created,updated) VALUES (?,?,?,?,?,?,?,?,?)''',
                (job['id'], TOPIC, 'codex', 'Failed check', 'done', 'development', json.dumps({
                    'success': False, 'failure_code': 'start_failed', 'text': 'CI failed' + 'f' * 300}), 1, 20)).lastrowid
            self.store.db.execute('''INSERT INTO workers
                (task,topic,provider,prompt,status,work,result,created,updated) VALUES (?,?,?,?,?,?,?,?,?)''',
                (job['id'], TOPIC, 'codex', 'Older completion', 'done', 'development',
                 json.dumps({'success': True, 'text': 'stale result'}), 1, 15))
        for mode in ('switch', 'restart'):
            notice, _ = await self.continuity_notice(mode)
            summary = json.JSONDecoder().raw_decode(notice['text'].split('Open jobs: ', 1)[1])[0]
            with self.subTest(mode=mode):
                shown = summary['shown'][0]
                self.assertEqual(shown['title'], ('Long title ' + 't' * 300)[:200] + '…')
                self.assertEqual(shown['notes'], ('next: merge after CI' + 'n' * 600)[:500])
                self.assertEqual(shown['finished'], {
                    'id': latest, 'success': False, 'failure_code': 'start_failed',
                    'text': ('CI failed' + 'f' * 300)[:200]})
                self.assertEqual((len(summary['live_workers']), summary['live_workers_not_shown']), (30, 61))
                self.assertEqual(summary['live_workers'][0]['id'], latest - 1)
                self.assertLessEqual(len(notice['text']), parent_state.RESTART_NOTICE_CHARS)
                self.assertEqual(self.store.task_get(job['id'])['status'], 'open')

    async def test_parent_jobs_summary_bounds_escaped_messages_and_empty_jobs(self):
        for _ in range(10):
            message = self.store.message_save(TOPIC, 'owner', '\\"\n🦊' * 500)
            self.store.message_delivered(message['id'], 'uncertain')
        for mode in ('switch', 'restart'):
            notice, session = await self.continuity_notice(mode)
            with self.subTest(mode=mode):
                prompt = notice['text']
                summary = json.JSONDecoder().raw_decode(prompt.split('Open jobs: ', 1)[1])[0]
                self.assertEqual(summary, {'count': 0, 'shown': [], 'not_shown': 0,
                                           'live_workers': [], 'live_workers_not_shown': 0})
                self.assertLessEqual(len(prompt), parent_state.RESTART_NOTICE_CHARS)
                tagged = message_tag(dict(self.store.topic(TOPIC)), notice) + prompt
                event = NativeProtocol.user(session, message_uuid(notice['id']), tagged)
                wire = (json.dumps({'op': 'write', 'line': json.dumps(event)}) + '\n').encode()
                self.assertLess(len(wire), 32 * 1024)

    async def test_parent_jobs_summary_keeps_at_most_the_newest_fifty_jobs(self):
        jobs, finished, running = self.continuity_jobs()
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET title='Small job',notes=NULL WHERE status='open'")
        for mode in ('switch', 'restart'):
            notice, _ = await self.continuity_notice(mode)
            summary = json.JSONDecoder().raw_decode(notice['text'].split('Open jobs: ', 1)[1])[0]
            with self.subTest(mode=mode):
                self.assertEqual((summary['count'], len(summary['shown']), summary['not_shown']), (60, 50, 10))
                self.assertEqual([row['id'] for row in summary['shown']], [row['id'] for row in reversed(jobs[-50:])])

    async def test_each_switched_topic_gets_its_own_provider_notice(self):
        other = '-10042:5'
        self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                              (other, -10042, 5, 'Other', str(self.root)))

        def switched_session(store, runner, cwd, model, instructions, topic=None):
            session = FakeSession(str(uuid.uuid4()))
            session.switched = True
            session.switch = {'from': 'claude', 'to': 'codex', 'reason': 'the owner selected ChatGPT'}
            return session

        self.service.session_factory = AsyncMock(side_effect=switched_session)
        await self.service.start_session(TOPIC)
        await self.service.start_session(other)
        for topic in (TOPIC, other):
            count = self.store.db.execute("SELECT count(*) FROM outbox WHERE topic=? "
                                          "AND text LIKE 'This chat now runs on%'", (topic,)).fetchone()[0]
            self.assertEqual(count, 1)
        self.assertFalse(any(row['text'].startswith('Main chat')
                             for row in self.store.db.execute('SELECT text FROM outbox')))

    async def test_pause_notice_reaches_only_home_once_and_available_parent_bypasses_retry(self):
        from coordinator.session import AccountsUnavailable
        other = '-10042:5'
        self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                              (other, -10042, 5, 'Other', str(self.root)))
        self.service.session_factory = AsyncMock(side_effect=AccountsUnavailable(time.time() + 3600, provider='both'))
        self.assertIsNone(await self.service.start_session(TOPIC))
        self.assertIsNone(await self.service.start_session(other))
        self.assertIsNone(await self.service.start_session(other))
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM outbox WHERE text LIKE 'The main chat is paused:%'"
                                              ).fetchone()[0], 1)
        self.assertEqual([row[0] for row in self.store.db.execute("SELECT topic FROM outbox WHERE text LIKE 'The main chat is paused:%'")], [TOPIC])
        self.service.session_factory = AsyncMock(return_value=FakeSession('resumed'))
        with patch.object(self.service, 'parent_available', return_value=True):
            self.assertIsNotNone(await self.service.start_session(other))
        self.assertIsNone(self.store.get('coordinator_account_retry_at'))

    async def test_owner_message_rechecks_claude_and_codex_usage_pauses(self):
        from coordinator.session import AccountsUnavailable
        for provider in ('claude', 'codex'):
            with self.subTest(provider=provider):
                profile = self.root / provider
                profile.mkdir()
                account_key = 'accounts' if provider == 'claude' else 'codex_accounts'
                status_key = 'account_status' if provider == 'claude' else 'codex_account_status'
                blocks_key = 'account_blocks' if provider == 'claude' else 'codex_account_blocks'
                alias = 'selected'
                now = time.time()
                snapshot = {'identity': {'email': 'selected@example.test', 'logged_in': True},
                            'observed_at': now, 'usage': {}, 'usage_allowed': True}
                self.store.put(account_key, {alias: {'enabled': True, 'config_dir': str(profile)}})
                self.store.put(status_key, {alias: snapshot})
                self.store.put(blocks_key, {alias: {'until': now + 3600, 'observed_at': now, 'reason': 'quota'},
                                           'other': {'until': now + 7200, 'reason': 'quota'}})
                self.store.put('claude_account_pick' if provider == 'claude' else 'codex_active_account', alias)
                self.store.put('parent_selection', {'provider': provider, 'model': None, 'effort': None})
                self.service.sessions.clear()
                self.service.pause_parent(TOPIC, AccountsUnavailable(now + 3600, provider=provider, alias=alias))
                row = self.store.message_save(TOPIC, 'owner', 'Usage is available. Continue.')
                factory = self.service.session_factory

                async def available_factory(*args, **kwargs):
                    ready = (self.service.accounts.select() if provider == 'claude' else
                             self.service.codex_accounts.parent_account())
                    if ready != alias:
                        raise AccountsUnavailable(now + 3600, provider=provider, alias=alias)
                    session = await factory(*args, **kwargs)
                    session.provider = provider
                    session.account_alias = alias
                    return session

                self.service.session_factory = available_factory
                refresh_path = ('coordinator.account_status.refresh_accounts' if provider == 'claude' else
                                'coordinator.codex_accounts.refresh_codex_accounts')
                try:
                    with patch(refresh_path, new_callable=AsyncMock) as refresh:
                        await self.service.feed_once()
                        await asyncio.gather(*self.service.feed_tasks.values())
                finally:
                    self.service.session_factory = factory
                self.assertEqual(self.store.db.execute('SELECT delivered FROM messages WHERE id=?',
                                                       (row['id'],)).fetchone()[0], 'received')
                self.assertIsNone(self.store.get('coordinator_account_retry_at'))
                self.assertEqual(self.store.get(blocks_key), {'other': {'until': now + 7200, 'reason': 'quota'}})
                self.assertEqual(refresh.await_count, 1)
                self.assertEqual(self.store.get('codex_active_account') if provider == 'codex' else
                                 self.store.get('claude_account_pick'), alias)

    async def test_slow_account_recheck_does_not_hold_other_chats(self):
        from coordinator.session import AccountsUnavailable
        other = '-10042:5'
        self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                              (other, -10042, 5, 'Other', str(self.root)))
        self.store.put('coordinator:' + other + '_selection',
                       {'provider': 'claude', 'model': None, 'effort': None, 'revision': 1})
        profile = self.root / 'claude'
        profile.mkdir()
        alias = 'selected'
        now = time.time()
        self.store.put('accounts', {alias: {'enabled': True, 'config_dir': str(profile)}})
        self.store.put('account_status', {alias: {'identity': {'email': 'selected@example.test', 'logged_in': True},
                                                  'observed_at': now, 'usage': {}, 'usage_allowed': True}})
        self.store.put('account_blocks', {alias: {'until': now + 3600, 'observed_at': now, 'reason': 'quota'}})
        self.store.put('claude_account_pick', alias)
        self.service.pause_parent(other, AccountsUnavailable(now + 3600, provider='claude', alias=alias))
        paused = self.store.message_save(other, 'owner', 'Usage is available. Continue.')
        waiting = self.store.message_save(TOPIC, 'owner', 'A message for another chat.')
        release = asyncio.Event()
        factory = self.service.session_factory

        async def available_factory(store, runner, cwd, model, instructions, topic=None):
            if topic == other and self.service.accounts.select() != alias:
                raise AccountsUnavailable(now + 3600, provider='claude', alias=alias)
            return await factory(store, runner, cwd, model, instructions, topic=topic)

        async def slow_refresh(*args, **kwargs):
            await release.wait()

        def delivered(row):
            return self.store.db.execute('SELECT delivered FROM messages WHERE id=?', (row['id'],)).fetchone()[0]

        self.service.session_factory = available_factory
        try:
            with patch('coordinator.account_status.refresh_accounts', new_callable=AsyncMock,
                       side_effect=slow_refresh) as refresh:
                await asyncio.wait_for(self.service.feed_once(), 5)
                await asyncio.gather(*self.service.feed_tasks.values())
                self.assertEqual(delivered(waiting), 'received')
                self.assertEqual(delivered(paused), 'pending')
                again = self.store.message_save(other, 'owner', 'Another message during the recheck.')
                await asyncio.wait_for(self.service.feed_once(), 5)
                self.assertEqual(delivered(again), 'pending')
                self.service.wake('feed').clear()
                release.set()
                await asyncio.wait_for(self.service.wake('feed').wait(), 5)
                await self.service.feed_once()
                await asyncio.gather(*self.service.feed_tasks.values())
                self.assertEqual((delivered(paused), delivered(again)), ('received', 'received'))
                self.assertEqual(refresh.await_count, 1)
        finally:
            release.set()
            self.service.session_factory = factory

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
            'message': tag(row['id']) + REPLY_FIRST_LINE + 'request'}}) + '\n')
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
        self.assertEqual(self.sessions[0].sent[0][1], tag(1) + REPLY_FIRST_LINE + 'first')
        policy = 'Use one worker.\n## Coordinators\nReply first.\n## Effort\nKeep.\n'
        (self.store.directory / 'USAGE.md').write_text(policy)
        self.store.message_save(TOPIC, 'owner', 'second')
        await self.service.feed_once()
        await asyncio.gather(*self.service.feed_tasks.values())
        self.assertIn('Current usage policy:\n' + policy, self.sessions[0].sent[-1][1])

    async def test_rejected_claude_workers_report_once_at_home_without_project_callbacks(self):
        project = '-10042:55'
        self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                              (project, -10042, 55, 'Project', str(self.root)))
        self.enable_claude_account()
        reset = time.time() + 3600
        transcript = self.root / 'rejected.jsonl'
        transcript.write_text('{}\n')
        self.runner.result = RunResult('rejected-session', quota_limited=True,
                                      transcript_path=str(transcript),
                                      rate_limit_info={'status': 'rejected', 'rateLimitType': 'five_hour',
                                                       'resetsAt': reset})
        for attempt in range(2):
            identifier = self.worker(topic=project)
            finished = await self.service.workers.run(self.service.workers.get(identifier), self.store.topic(project))
            self.assertEqual(finished['status'], 'waiting_for_quota')
        self.assertEqual(list(self.store.db.execute("SELECT topic,text FROM messages WHERE kind='callback'")), [])
        reports = [tuple(row) for row in self.store.db.execute('SELECT topic,text FROM outbox')]
        self.assertEqual([topic for topic, text in reports], [TOPIC])
        self.assertIn('waiting for Claude usage', reports[0][1])

    async def test_claude_usage_refresh_and_local_reserve_do_not_queue_warnings(self):
        from coordinator.account_status import refresh_accounts
        now = time.time()
        directory = self.root / 'orbit'
        directory.mkdir()
        self.store.put('accounts', {'orbit': {'config_dir': str(directory), 'enabled': True}})
        used = 60

        async def collect(account):
            return {'identity': {'logged_in': True, 'email': 'orbit@example.com'},
                    'observed_at': time.time(),
                    'usage': {'seven_day_fable': {'utilization': used, 'resets_at': now + 3600}}}

        override = self.root / 'local-overrides.json'
        override.write_text(json.dumps({'account_switch_thresholds': {'orbit': .8}}))
        with patch('coordinator.accounts.LOCAL_OVERRIDES_PATH', override):
            for used in (60, 79, 80):
                self.assertFalse(await refresh_accounts(self.store, collect))
                self.assertEqual(self.store.get('account_status')['orbit']['usage']['seven_day_fable']['utilization'], used)
                self.assertEqual(list(self.store.db.execute('SELECT text FROM outbox')), [])
            self.assertIsNone(self.service.accounts.select())

    async def test_owner_prompt_puts_reply_first_between_tag_and_text(self):
        row = self.store.message_save(TOPIC, 'owner', 'What changed?')
        await self.drain_feed()
        self.assertEqual(self.sessions[0].sent, [
            (message_uuid(row['id']), tag(row['id']) + REPLY_FIRST_LINE + row['text'])])

    async def test_forwarded_user_reaches_parent_as_information_with_quoted_origin(self):
        incoming = owner_update(1, 'Read the forwarded report.')
        name = 'Fixture "Sender"\nSecond line'
        incoming['message']['forward_origin'] = {
            'type': 'hidden_user', 'sender_user_name': name, 'date': 1}
        self.assertEqual(self.store.accept(incoming), 'queued')
        row, = self.store.messages_pending()
        self.assertEqual(row['kind'], 'forwarded')
        await self.drain_feed()
        self.assertEqual(self.sessions[0].sent, [(message_uuid(row['id']),
            tag(row['id'], 'forwarded') + 'Forwarded from ' + json.dumps(name) + ':\nRead the forwarded report.')])
        self.assertEqual(self.store.db.execute('SELECT delivered FROM messages WHERE id=?',
                                              (row['id'],)).fetchone()[0], 'received')

    async def test_forwarded_modern_and_legacy_origins_reach_parent_with_names(self):
        cases = [
            ({'forward_origin': {'type': 'user', 'sender_user': {
                'id': 99, 'first_name': 'Fixture', 'last_name': 'Sender'}, 'date': 1}}, 'Fixture Sender'),
            ({'forward_origin': {'type': 'channel', 'chat': {'id': -123, 'title': 'Fixture Channel'},
                                 'message_id': 9, 'date': 1}}, 'Fixture Channel'),
            ({'forward_origin': {'type': 'chat', 'sender_chat': {'id': -124, 'title': 'Fixture Chat'},
                                 'date': 1}}, 'Fixture Chat'),
            ({'forward_origin': {'type': 'hidden_user', 'sender_user_name': 'Fixture Hidden', 'date': 1}},
             'Fixture Hidden'),
            ({'forward_from': {'id': 99, 'first_name': 'Fixture Legacy'}, 'forward_date': 1}, 'Fixture Legacy'),
            ({'forward_sender_name': 'Fixture Private', 'forward_date': 1}, 'Fixture Private'),
            ({'forward_from_chat': {'id': -123, 'title': 'Fixture Old Channel'}}, 'Fixture Old Channel'),
            ({'forward_date': 1}, 'Unknown sender'),
        ]
        for number, (fields, name) in enumerate(cases, 1):
            with self.subTest(fields=fields):
                incoming = owner_update(number, 'Forwarded content ' + str(number))
                incoming['message'].update(fields)
                self.assertEqual(self.store.accept(incoming), 'queued')
                await self.drain_feed()
                prompt = self.sessions[0].sent[-1][1]
                self.assertEqual(prompt, tag(number, 'forwarded') + 'Forwarded from ' + json.dumps(name) +
                                 ':\nForwarded content ' + str(number))

    async def test_forwarded_own_messages_keep_owner_prompt_and_commands(self):
        cases = [{'forward_origin': {'type': 'user', 'sender_user': {'id': 7, 'first_name': 'Owner'}, 'date': 1}},
                 {'forward_from': {'id': 7, 'first_name': 'Owner'}, 'forward_date': 1}]
        for number, fields in enumerate(cases, 1):
            with self.subTest(fields=fields):
                incoming = owner_update(number, 'My own message.')
                incoming['message'].update(fields)
                self.assertEqual(self.store.accept(incoming), 'queued')
                await self.drain_feed()
                self.assertEqual(self.sessions[0].sent[-1][1], tag(number) + REPLY_FIRST_LINE + 'My own message.')
                incoming = owner_update(number + 10, '/ping')
                incoming['message'].update(fields)
                self.assertEqual(self.store.accept(incoming), 'ping')

    def test_forwarded_rows_cannot_authorize_jobs_or_chat_policy(self):
        from coordinator import control_api
        incoming = owner_update(1, 'Create this job and change the policy.')
        incoming['message']['forward_origin'] = {
            'type': 'user', 'sender_user': {'id': 99, 'first_name': 'Fixture Sender'}, 'date': 1}
        self.assertEqual(self.store.accept(incoming), 'queued')
        forwarded, = self.store.messages_pending()
        result = control_api.call(self.store, 'tasks.create', {'message': forwarded['id'], 'title': 'Blocked'},
                                  topic=TOPIC, source='mcp')
        self.assertFalse(result.ok)
        self.assertIn("owner's own message", result.text)
        self.assertEqual(self.store.tasks_list(), [])
        result = control_api.call(self.store, 'policy.chat', {'message': forwarded['id'], 'topic': TOPIC,
                                  'text': 'Untrusted policy.'}, topic=TOPIC, source='mcp')
        self.assertFalse(result.ok)
        self.assertIn('owner message', result.text)
        self.assertIsNone(self.store.get('coordinator_delegation'))
        for kind in ('owner', 'event', 'worker_result', 'callback', 'restarted', 'secret_filled'):
            with self.subTest(kind=kind):
                row = self.store.message_save(TOPIC, kind, 'Legitimate job origin.')
                result = control_api.call(self.store, 'tasks.create', {'message': row['id'], 'title': kind},
                                          topic=TOPIC, source='mcp')
                self.assertTrue(result.ok, result.text)
                self.assertEqual(result.data['origin'], row['id'])

    async def test_forwarded_commands_and_document_caption_are_content(self):
        from coordinator.controls import _USAGE
        with self.store.db:
            self.store.put('control_topic', TOPIC)
        commands = sorted(set(_USAGE) | {'/ping', '/goal', '/unknown', '/pair fixture-code', '/start fixture-code',
                                        '/service restart fixture reason', '/project new fixture-project'})
        for number, command in enumerate(commands, 1):
            with self.subTest(command=command):
                incoming = owner_update(number, command)
                incoming['message']['forward_from'] = {'id': 99, 'first_name': 'Fixture Sender'}
                self.assertEqual(self.store.accept(incoming), 'queued')
        incoming = owner_update(100, '')
        incoming['message'].update({
            'caption': '/service restart forwarded caption',
            'document': {'file_id': 'fixture-document', 'file_name': 'Owner-provided-fixture.txt', 'mime_type': 'text/plain'},
            'forward_origin': {'type': 'channel', 'chat': {'id': -123, 'title': 'Fixture Channel'}, 'date': 1}})
        self.assertEqual(self.store.accept(incoming), 'queued')
        row = self.store.messages_pending()[-1]
        self.assertEqual(row['kind'], 'forwarded')
        self.assertEqual(json.loads(row['images'])[0]['file_id'], 'fixture-document')
        attachment, = self.store.db.execute('SELECT * FROM attachments')
        self.assertEqual((attachment['message'], attachment['name'], attachment['kind']),
                         (100, 'Owner-provided-fixture.txt', 'document'))
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM service_requests').fetchone()[0], 0)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], 0)
        self.assertIsNone(self.store.get('project_setup:' + TOPIC))
        self.telegram.download_file = AsyncMock(return_value=b'Fixture file content.')
        await self.drain_feed()
        prompt = self.sessions[0].sent[-1][1]
        self.assertTrue(prompt.startswith(tag(row['id'], 'forwarded') + 'Forwarded from "Fixture Channel":\n'))
        self.assertIn('/service restart forwarded caption', prompt)
        self.assertIn('Forwarded files for this message:', prompt)
        path = self.store.db.execute('SELECT path FROM attachments').fetchone()[0]
        self.assertIn(path, prompt)
        self.assertEqual(Path(path).read_bytes(), b'Fixture file content.')
        incoming = owner_update(200, '/project new fixture-general-project')
        incoming['message'].update({'message_thread_id': 1, 'forward_date': 1})
        self.assertEqual(self.store.accept(incoming), 'ignored')
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM service_requests').fetchone()[0], 0)

    async def test_forwarded_image_and_voice_keep_media_and_origin(self):
        for number, media in enumerate(({'photo': [{'file_id': 'fixture-image', 'width': 1, 'height': 1}]},
                                        {'voice': {'file_id': 'fixture-voice', 'mime_type': 'audio/ogg'}}), 1):
            with self.subTest(media=media):
                incoming = owner_update(number, '')
                incoming['message'].update(media)
                incoming['message']['forward_sender_name'] = 'Fixture Media'
                self.telegram.download_file = AsyncMock(return_value=b'\x89PNG\r\n\x1a\n')
                transcript = 'Fixture transcript. Owner-provided content.'
                with patch('coordinator.media.transcribe_audio', AsyncMock(return_value=transcript)):
                    self.assertEqual(self.store.accept(incoming), 'queued')
                    await self.drain_feed()
                prompt = self.sessions[0].sent[-1][1]
                self.assertTrue(prompt.startswith(tag(number, 'forwarded') + 'Forwarded from "Fixture Media":\n'))
                self.assertNotIn('\n\nOwner-provided images for this message:', prompt)
                self.assertNotIn('\n\nOwner-provided files for this message:', prompt)
                attachment = self.store.db.execute('SELECT path FROM attachments WHERE message=?', (number,)).fetchone()
                self.assertTrue(Path(attachment['path']).is_file())
                self.assertIn(attachment['path'], prompt)
                if 'voice' in media:
                    self.assertIn(transcript, prompt)

    def test_forwarded_messages_preserve_sender_and_topic_gates(self):
        fields = {'forward_origin': {'type': 'user', 'sender_user': {'id': 99, 'first_name': 'Fixture Sender'}}}
        for number, (extra, expected) in enumerate((
                ({'from': {'id': 99}}, 'unauthorized'),
                ({'from': {'id': 777000}, 'sender_chat': {'id': -123}, 'is_automatic_forward': True}, 'ignored'),
                ({'from': {'id': 123, 'is_bot': True}}, 'ignored')), 1):
            with self.subTest(extra=extra):
                incoming = owner_update(number, '/service restart fixture')
                incoming['message'].update(fields, **extra)
                self.assertEqual(self.store.accept(incoming), expected)
        incoming = owner_update(10, '/ping')
        incoming['message'].update(fields)
        self.assertEqual(self.store.accept(incoming), 'queued')
        self.assertEqual([row['kind'] for row in self.store.messages_pending()], ['forwarded'])
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM service_requests').fetchone()[0], 0)

    def test_forwarded_messages_do_not_fill_controls_or_set_up_projects(self):
        from coordinator.control_ui import prompt
        self.assertTrue(prompt(self.store, TOPIC, 'policy', 10))
        card = self.store.db.execute('SELECT id FROM outbox ORDER BY id DESC LIMIT 1').fetchone()[0]
        self.store.delivered(card, 100)
        policy = self.store.directory / 'USAGE.md'
        before = policy.read_text()
        incoming = owner_update(1, 'Untrusted policy.', reply=100)
        incoming['message']['forward_sender_name'] = 'Fixture Sender'
        self.assertEqual(self.store.accept(incoming), 'queued')
        self.assertEqual(policy.read_text(), before)
        self.assertEqual(self.store.messages_pending()[0]['kind'], 'forwarded')
        self.assertEqual(self.store.accept(owner_update(3, 'Owner policy.', reply=100)), 'control')
        self.assertEqual(policy.read_text(), 'Owner policy.')
        with self.store.db:
            self.store.put('control_topic', TOPIC)
            self.store.db.execute('UPDATE topics SET cwd=? WHERE id=?', ('', TOPIC))
        incoming = owner_update(2, 'fixture-project')
        incoming['message']['forward_sender_name'] = 'Fixture Sender'
        self.assertEqual(self.store.accept(incoming), 'queued')
        self.assertIsNone(self.store.get('project_setup:' + TOPIC))
        with self.store.db:
            self.store.db.execute('UPDATE topics SET enabled=0 WHERE id=?', (TOPIC,))
            self.store.put('project_setup:' + TOPIC, {'stage': 'new'})
        incoming = owner_update(4, 'fixture-disabled-project')
        incoming['message']['forward_sender_name'] = 'Fixture Sender'
        self.assertEqual(self.store.accept(incoming), 'disabled')
        self.assertEqual(self.store.get('project_setup:' + TOPIC), {'stage': 'new'})
        self.assertEqual(self.store.topic(TOPIC)['cwd'], '')
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM service_requests').fetchone()[0], 0)

    async def test_owner_image_prompt_keeps_attachment_instructions_after_text(self):
        update = owner_update(1, 'Describe this image.')
        update['message']['photo'] = [{'file_id': 'photo', 'width': 1, 'height': 1}]
        self.telegram.download_file = AsyncMock(return_value=b'\x89PNG\r\n\x1a\n')
        self.assertEqual(self.store.accept(update), 'queued')
        row, = self.store.messages_pending()
        await self.drain_feed()
        attachment, = [dict(item) for item in self.store.db.execute('SELECT * FROM attachments')]
        self.assertTrue(Path(attachment['path']).is_file())
        instructions = attachment_instructions([attachment], coordinator=True)
        self.assertIn('Owner-provided images for this message:', instructions)
        self.assertEqual(self.sessions[0].sent, [
            (message_uuid(row['id']), tag(row['id']) + REPLY_FIRST_LINE + row['text'] + instructions)])

    async def test_non_owner_prompts_remain_exactly_tag_and_text(self):
        session = await self.service.start_session(TOPIC)
        for kind in ('worker_result', 'event', 'secret_filled', 'callback', 'restarted'):
            with self.subTest(kind=kind):
                row = self.store.message_save(TOPIC, kind, 'Saved message.\nSecond line.')
                await self.drain_feed()
                self.assertEqual(session.sent[-1], (message_uuid(row['id']), tag(row['id'], kind) + row['text']))

    async def test_three_owner_messages_are_sent_separately_in_order(self):
        for number, text in enumerate(('first', 'second', 'third'), 1):
            self.assertEqual(self.store.accept(owner_update(number, text)), 'queued')
        await self.drain_feed()
        self.assertEqual([text for _, text in self.sessions[0].sent],
                         [tag(1) + REPLY_FIRST_LINE + 'first', tag(2) + REPLY_FIRST_LINE + 'second', tag(3) + REPLY_FIRST_LINE + 'third'])
        rows = list(self.store.db.execute('SELECT id,delivered,receipt FROM messages ORDER BY id'))
        self.assertEqual([row['delivered'] for row in rows], ['received'] * 3)
        self.assertEqual([row['receipt'] for row in rows], ['written'] * 3)
        self.assertEqual(len({message_id for message_id, _ in self.sessions[0].sent}), 3)

    async def test_late_rejected_receipts_preserve_recovery_and_closed_input_is_requeued(self):
        cases = [('received', 'received', 'replayed', ('received', 'replayed')),
                 ('uncertain', 'received', 'replayed', ('received', 'replayed')),
                 ('received', 'pending', None, ('pending', None)),
                 ('uncertain', 'pending', None, ('pending', None)),
                 ('closed', 'sent', None, ('pending', None))]
        for receipt, delivered, saved, expected in cases:
            with self.subTest(receipt=receipt, delivered=delivered):
                row = self.store.message_save(TOPIC, 'owner', 'request')

                async def rejected_send(message_id, text, row_id=None):
                    with self.store.db:
                        self.store.db.execute('UPDATE messages SET delivered=?,receipt=? WHERE id=?',
                                              (delivered, saved, row_id))
                    return receipt

                session = SimpleNamespace(key='coordinator', rejected=True, send=rejected_send)
                self.assertEqual(await self.service._send_message(row, 'request', session), 'closed')
                actual = self.store.db.execute('SELECT delivered,receipt FROM messages WHERE id=?',
                                               (row['id'],)).fetchone()
                self.assertEqual(tuple(actual), expected)

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
                         [tag(1) + REPLY_FIRST_LINE + 'hold', tag(2) + REPLY_FIRST_LINE + 'follow'])
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
                         [tag(1) + REPLY_FIRST_LINE + 'image', tag(2) + REPLY_FIRST_LINE + 'later'])

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
        self.assertTrue(sent[0].startswith(tag(1) + REPLY_FIRST_LINE + 'part 1\n\nOwner-provided files for this message:\n'))
        self.assertIn('%s (application/json, 12 bytes)' % saved, sent[0])
        self.assertTrue(sent[1].startswith(tag(2) + REPLY_FIRST_LINE + 'part 2'))
        self.assertIn('notes.md (application/json): download failed', sent[1])
        self.assertEqual(sent[2], tag(3) + REPLY_FIRST_LINE + 'after')
        self.assertEqual([row[0] for row in self.store.db.execute('SELECT delivered FROM messages ORDER BY id')],
                         ['received'] * 3)
        reports = [row[0] for row in self.store.db.execute('SELECT text FROM outbox ORDER BY id')]
        self.assertEqual(len(reports), 1)
        self.assertEqual(reports[0], '`notes.md` could not be downloaded. Your message went to the coordinator '
                         'without the file. Send the file again.')

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

    async def test_owner_message_reaches_parent_while_worker_result_write_is_pending(self):
        worker = self.worker()
        await self.service.start_session()
        session = self.sessions[0]
        result = self.store.worker_complete(worker, {'success': True, 'text': 'finished'})
        writing = asyncio.Event()
        release = asyncio.Event()
        owner_written = asyncio.Event()

        async def held_send(message_id, text, row_id=None):
            if 'kind=worker_result' in text:
                writing.set()
                await release.wait()
            session.sent.append((message_id, text))
            if 'kind=owner' in text:
                owner_written.set()
            return 'received'

        session.send = held_send
        try:
            await self.service.feed_once()
            await asyncio.wait_for(writing.wait(), 2)
            result_task = self.service.feed_tasks[result['id']]
            self.assertEqual(tuple(self.store.db.execute('SELECT delivered,receipt FROM messages WHERE id=?',
                                                       (result['id'],)).fetchone()), ('sent', None))
            self.assertEqual(session.sent, [])
            self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], 0)
            self.store.accept(owner_update(1, 'owner message while result waits'))
            owner_id = self.store.db.execute("SELECT id FROM messages WHERE kind='owner'").fetchone()[0]
            await self.service.feed_once()
            await asyncio.wait_for(owner_written.wait(), 2)
            self.assertEqual(tuple(self.store.db.execute('SELECT delivered,receipt FROM messages WHERE id=?',
                                                       (result['id'],)).fetchone()), ('sent', None))
            self.assertEqual(tuple(self.store.db.execute('SELECT delivered,receipt FROM messages WHERE id=?',
                                                       (owner_id,)).fetchone()), ('received', 'written'))
            self.assertEqual([text for _, text in session.sent], [tag(owner_id) + REPLY_FIRST_LINE + 'owner message while result waits'])
            release.set()
            await asyncio.wait_for(result_task, 2)
            await self.service.feed_once()
            self.assertEqual(tuple(self.store.db.execute('SELECT delivered,receipt FROM messages WHERE id=?',
                                                       (result['id'],)).fetchone()), ('received', 'written'))
            self.assertEqual([text for _, text in session.sent], [
                tag(owner_id) + REPLY_FIRST_LINE + 'owner message while result waits',
                tag(result['id'], 'worker_result') + result['text']])
            self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], 0)
        finally:
            release.set()

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

    async def test_closed_job_interrupts_secret_waiting_worker_before_secret_card_check(self):
        for job_status in ('done', 'dropped'):
            with self.subTest(job_status=job_status):
                secret_name = 'GITHUB_TOKEN_' + job_status.upper()
                worker = self.worker()
                task = self.store.db.execute('SELECT task FROM workers WHERE id=?', (worker,)).fetchone()[0]
                self.store.task_update(task, secrets=[secret_name])
                with self.store.db:
                    envelope = self.store.db.execute('''INSERT INTO envelopes(name,reason,consumer,task,topic,state,
                        created,expires,updated) VALUES (?,'r','c',?,?,'open',1,9999999999,1)''',
                        (secret_name, task, TOPIC)).lastrowid
                await self.service.workers_once()
                await asyncio.gather(*self.service.worker_tasks.values())
                self.assertEqual(self.store.db.execute('SELECT status FROM workers WHERE id=?', (worker,)).fetchone()[0],
                                 'waiting_for_secret')
                self.assertEqual(self.runner.calls, [])
                self.service.worker_tasks.clear()
                card_before = dict(self.store.db.execute('SELECT * FROM envelopes WHERE id=?', (envelope,)).fetchone())
                with self.store.db:
                    self.store.db.execute('UPDATE tasks SET status=? WHERE id=?', (job_status, task))
                self.assertEqual(card_before,
                                 dict(self.store.db.execute('SELECT * FROM envelopes WHERE id=?', (envelope,)).fetchone()))
                self.assertFalse(await self.service.workers_once())
                await asyncio.gather(*self.service.worker_tasks.values())
                worker_status = self.store.db.execute('SELECT status FROM workers WHERE id=?', (worker,)).fetchone()[0]
                self.assertEqual(worker_status, 'interrupted')
                self.assertEqual(self.runner.calls, [])
                self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM messages WHERE source_worker=? "
                                                       "AND kind='worker_result'", (worker,)).fetchone()[0], 0)
                self.assertEqual(self.store.db.execute('SELECT * FROM problems WHERE worker=?', (worker,)).fetchall(), [])
                self.assertEqual(card_before,
                                 dict(self.store.db.execute('SELECT * FROM envelopes WHERE id=?', (envelope,)).fetchone()))
                tables = ('tasks', 'workers', 'envelopes', 'messages', 'problems', 'outbox')
                before_second_pass = {
                    table: [tuple(row) for row in self.store.db.execute('SELECT * FROM %s ORDER BY rowid' % table)]
                    for table in tables
                }
                self.assertFalse(await self.service.workers_once())
                await asyncio.gather(*self.service.worker_tasks.values())
                after_second_pass = {
                    table: [tuple(row) for row in self.store.db.execute('SELECT * FROM %s ORDER BY rowid' % table)]
                    for table in tables
                }
                self.assertEqual(before_second_pass, after_second_pass)

    async def test_closed_job_interrupts_quota_waiting_worker_before_account_selection(self):
        task = self.store.task_create(TOPIC, 'Build service', worktree=str(self.root))
        worker = self.write_lease_worker(task, 'quota', status='waiting_for_quota', provider='claude')
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET status='done' WHERE id=?", (task['id'],))
        self.assertFalse(await self.service.workers_once())
        await asyncio.gather(*self.service.worker_tasks.values())
        self.assertEqual(self.service.workers.get(worker)['status'], 'interrupted')
        self.assertEqual(self.runner.calls, [])

    def write_lease_worker(self, task, name, work='dev', status='queued', session=None, fresh=1,
                           provider='codex'):
        with self.store.db:
            row = self.store.db.execute('''INSERT INTO workers
                (task,topic,provider,prompt,cwd,workspace,work,status,session,fresh,created,updated)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',
                (task['id'], TOPIC, provider, name, str(self.root), json.dumps({'cwd': str(self.root)}),
                 work, status, session, fresh, 1.0, 1.0))
        return row.lastrowid

    def write_lease_runner(self, names):
        probe = SimpleNamespace(calls=[], sessions={}, entered={name: asyncio.Event() for name in names},
                                release={name: asyncio.Event() for name in names})

        async def run(provider, prompt, cwd, session, **options):
            probe.calls.append(prompt)
            probe.sessions[prompt] = (session, options['fresh'])
            options['on_start'](123)
            probe.entered[prompt].set()
            await probe.release[prompt].wait()
            return RunResult(session or str(uuid.uuid4()), text='finished', success=True)

        self.runner.run = run
        return probe

    async def finish_write_lease_workers(self, probe):
        for release in probe.release.values():
            release.set()
        await asyncio.gather(*self.service.worker_tasks.values())
        self.service.worker_tasks.clear()

    async def test_worker_write_lease_holds_only_same_job_dev_workers(self):
        job = self.store.task_create(TOPIC, 'J', worktree=str(self.root))
        other = self.store.task_create(TOPIC, 'K', worktree=str(self.root))
        first = self.write_lease_worker(job, 'D1')
        second = self.write_lease_worker(job, 'D2')
        self.write_lease_worker(job, 'R1', work='research')
        self.write_lease_worker(job, 'R2', work='research')
        self.write_lease_worker(other, 'D3')
        before = self.service.workers.get(second)
        probe = self.write_lease_runner(('D1', 'D2', 'R1', 'R2', 'D3'))
        try:
            self.assertTrue(await self.service.workers_once())
            await asyncio.wait_for(asyncio.gather(*(probe.entered[name].wait()
                                   for name in ('D1', 'R1', 'R2', 'D3'))), 10)
            for _ in range(2):
                self.assertFalse(await self.service.workers_once())
            self.assertCountEqual(probe.calls, ['D1', 'R1', 'R2', 'D3'])
            self.assertEqual(self.service.workers.get(second), before)
            self.assertEqual(before['status'], 'queued')
            self.assertFalse((self.store.directory / 'hosts' / ('worker-%d' % second)).exists())
            probe.release['D1'].set()
            await self.service.worker_tasks[first]
            self.assertTrue(await self.service.workers_once())
            await asyncio.wait_for(probe.entered['D2'].wait(), 10)
            self.assertEqual(self.service.workers.get(second)['status'], 'running')
            self.assertFalse(await self.service.workers_once())
            self.assertEqual(probe.calls.count('D2'), 1)
        finally:
            await self.finish_write_lease_workers(probe)
        self.assertEqual(self.service.workers.get(second)['status'], 'done')
        self.assertTrue(json.loads(self.service.workers.get(second)['result'])['success'])
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM messages WHERE source_worker=?',
                                              (second,)).fetchone()[0], 1)

    async def test_worker_write_lease_holds_older_resume_and_recovered_quota(self):
        job = self.store.task_create(TOPIC, 'J', worktree=str(self.root))
        other = self.store.task_create(TOPIC, 'K', worktree=str(self.root))
        session = str(uuid.uuid4())
        resumed = self.write_lease_worker(job, 'resume', status='done', session=session, fresh=0)
        quota = self.write_lease_worker(job, 'quota', status='waiting_for_quota', provider='claude')
        first = self.write_lease_worker(job, 'running')
        self.write_lease_worker(job, 'R1', work='research')
        self.write_lease_worker(job, 'R2', work='research')
        self.write_lease_worker(other, 'other')
        probe = self.write_lease_runner(('resume', 'quota', 'running', 'R1', 'R2', 'other'))
        try:
            self.assertTrue(await self.service.workers_once())
            await asyncio.wait_for(asyncio.gather(*(probe.entered[name].wait()
                                   for name in ('running', 'R1', 'R2', 'other'))), 10)
            self.assertEqual(self.service.workers.get(quota)['status'], 'waiting_for_quota')
            with self.store.db:
                self.store.db.execute("UPDATE workers SET status='queued' WHERE id=?", (resumed,))
            before = self.service.workers.get(resumed)
            self.enable_claude_account()
            self.assertFalse(await self.service.workers_once())
            self.assertEqual(self.service.workers.get(resumed), before)
            self.assertEqual(self.service.workers.get(quota)['status'], 'queued')
            self.assertFalse(await self.service.workers_once())
            self.assertCountEqual(probe.calls, ['running', 'R1', 'R2', 'other'])
            probe.release['running'].set()
            await self.service.worker_tasks[first]
            self.assertTrue(await self.service.workers_once())
            await asyncio.wait_for(probe.entered['resume'].wait(), 10)
            self.assertEqual(probe.sessions['resume'], (session, False))
            self.assertEqual(self.service.workers.get(quota)['status'], 'queued')
            self.assertFalse(await self.service.workers_once())
            probe.release['resume'].set()
            await self.service.worker_tasks[resumed]
            self.assertTrue(await self.service.workers_once())
            await asyncio.wait_for(probe.entered['quota'].wait(), 10)
            self.assertFalse(await self.service.workers_once())
            self.assertEqual(probe.calls.count('resume'), 1)
            self.assertEqual(probe.calls.count('quota'), 1)
        finally:
            await self.finish_write_lease_workers(probe)
        for worker in (resumed, quota):
            row = self.service.workers.get(worker)
            self.assertEqual(row['status'], 'done')
            self.assertTrue(json.loads(row['result'])['success'])

    async def test_worker_write_lease_reattaches_running_writer_after_restart(self):
        job = self.store.task_create(TOPIC, 'J', worktree=str(self.root))
        session = str(uuid.uuid4())
        first = self.write_lease_worker(job, 'running', status='running', session=session, fresh=0)
        second = self.write_lease_worker(job, 'queued')
        with self.store.db:
            self.store.db.execute('UPDATE workers SET host=?,account_alias=? WHERE id=?',
                                  ('worker-%d' % first, 'saved', first))
        before = self.service.workers.get(second)
        probe = self.write_lease_runner(('running', 'queued'))
        self.runner.run = AsyncMock(wraps=self.runner.run)
        try:
            with patch('coordinator.host.HostClient.recover_state', new=AsyncMock(return_value='running')) as recover, \
                    patch('coordinator.host.HostClient.connect', new=AsyncMock()) as connect:
                await self.service.workers_once()
                self.assertEqual(set(self.service.worker_tasks), {first})
                await asyncio.wait_for(probe.entered['running'].wait(), 10)
                recover.assert_awaited_once()
                connect.assert_awaited_once()
            self.assertTrue(self.runner.run.await_args.kwargs['attach'])
            self.assertEqual(probe.sessions['running'], (session, False))
            self.assertFalse(await self.service.workers_once())
            self.assertEqual(probe.calls, ['running'])
            self.assertEqual(self.service.workers.get(second), before)
            probe.release['running'].set()
            await self.service.worker_tasks[first]
            self.assertEqual(self.service.workers.get(first)['status'], 'done')
            self.assertTrue(await self.service.workers_once())
            await asyncio.wait_for(probe.entered['queued'].wait(), 10)
            self.assertEqual(probe.calls, ['running', 'queued'])
            self.assertFalse(self.runner.run.await_args.kwargs['attach'])
        finally:
            await self.finish_write_lease_workers(probe)
        self.assertEqual(self.service.workers.get(second)['status'], 'done')

    async def test_worker_write_lease_holds_launching_writer_without_yield(self):
        job = self.store.task_create(TOPIC, 'J', worktree=str(self.root))
        first = self.write_lease_worker(job, 'D1')
        second = self.write_lease_worker(job, 'D2')
        before = self.service.workers.get(second)
        probe = self.write_lease_runner(('D1', 'D2'))
        try:
            self.assertTrue(await self.service.workers_once())
            self.assertEqual(self.service.workers.get(first)['status'], 'queued')
            self.assertFalse(await self.service.workers_once())
            self.assertEqual(set(self.service.worker_tasks), {first})
            self.assertEqual(self.service.workers.get(second), before)
            await asyncio.wait_for(probe.entered['D1'].wait(), 10)
            self.assertEqual(probe.calls, ['D1'])
            probe.release['D1'].set()
            await self.service.worker_tasks[first]
            self.assertTrue(await self.service.workers_once())
            await asyncio.wait_for(probe.entered['D2'].wait(), 10)
            self.assertEqual(probe.calls, ['D1', 'D2'])
            self.assertFalse(await self.service.workers_once())
        finally:
            await self.finish_write_lease_workers(probe)
        self.assertEqual(self.service.workers.get(second)['status'], 'done')

    async def test_worker_write_lease_research_started_first_does_not_hold_dev(self):
        job = self.store.task_create(TOPIC, 'J', worktree=str(self.root))
        research = self.write_lease_worker(job, 'research', work='research')
        dev = self.write_lease_worker(job, 'dev')
        probe = self.write_lease_runner(('research', 'dev'))
        try:
            self.assertLess(research, dev)
            self.assertTrue(await self.service.workers_once())
            self.assertEqual(set(self.service.worker_tasks), {research, dev})
            await asyncio.wait_for(asyncio.gather(*(event.wait() for event in probe.entered.values())), 10)
            self.assertEqual(probe.calls, ['research', 'dev'])
            self.assertFalse(await self.service.workers_once())
        finally:
            await self.finish_write_lease_workers(probe)
        self.assertEqual(self.service.workers.get(dev)['status'], 'done')

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
        self.assertEqual(idea, tag(1, topic='-10042:5', name='Torii brainstorm') + REPLY_FIRST_LINE + 'idea')
        self.assertTrue(result.startswith(tag(2, 'worker_result', '-10042:5', 'Torii brainstorm')))
        self.assertEqual(json.loads(result.split('] ', 1)[1])['worker'], worker)
        home = self.service.sessions[TOPIC]
        self.assertEqual([text for _, text in home.sent if 'kind=owner' in text], [tag(3) + REPLY_FIRST_LINE + 'home'])
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
        jobs = json.JSONDecoder().raw_decode(prompt.split('Open jobs: ', 1)[1])[0]
        active = jobs['live_workers']
        uncertain = json.loads(prompt.split('characters): ', 1)[1].split('. Inspect', 1)[0])
        self.assertEqual(jobs['count'], 90)
        self.assertLess(len(jobs['shown']), 50)
        self.assertEqual(jobs['not_shown'], 90 - len(jobs['shown']))
        self.assertEqual([row['id'] for row in jobs['shown']],
                         [row['id'] for row in sorted(tasks, key=lambda row: (
                             row['id'] <= 50, row['updated'], row['id']), reverse=True)[:len(jobs['shown'])]])
        self.assertTrue(all(set(row) == {'id', 'topic', 'number', 'title', 'stage', 'notes', 'finished'}
                            for row in jobs['shown']))
        self.assertEqual((len(active), jobs['live_workers_not_shown']), (30, 20))
        self.assertEqual([row['id'] for row in active], list(range(50, 20, -1)))
        self.assertTrue(all(set(row) == {'id', 'task', 'provider', 'status', 'work'} for row in active))
        self.assertLessEqual(len(prompt), parent_state.RESTART_NOTICE_CHARS)
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
        self.assertEqual(self.sessions[1].sent[1][1], tag(1) + REPLY_FIRST_LINE + 'continue')

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
        self.assertEqual([text for _, text in self.service.sessions[TOPIC].sent], [tag(2) + REPLY_FIRST_LINE + 'home'])
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
        self.assertEqual(self.service.sessions[other].sent[-1][1].split('] ', 1)[1], REPLY_FIRST_LINE + 'next')

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
        self.assertEqual(request, tag(2, topic=other, name='Ideas') + REPLY_FIRST_LINE + 'continue')
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
        summary = json.JSONDecoder().raw_decode(note.split('Open jobs: ', 1)[1])[0]
        self.assertEqual(summary['live_workers'], [
            {'id': worker, 'task': summary['shown'][0]['id'], 'provider': 'claude',
             'status': 'queued', 'work': 'research'}])
        self.assertNotIn('please create task X', note)
        self.assertEqual(request, tag(2, topic=other, name='Ideas') + REPLY_FIRST_LINE + 'please create task X')

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
        def restarting():
            self.assertFalse(self.store.outbox_has_pending())
            self.assertEqual(len(list((self.store.directory / 'backups').glob(
                'state-before-restart-*.sqlite'))), 1)

        self.service.on_restart = Mock(side_effect=restarting)
        with self.store.db:
            request = self.store.service_request('service.restart', {'reason': 'upgrade'})
            self.store.enqueue_report(TOPIC, 'saved report')
        self.assertFalse(await self.service.restart_once())
        await self.service.controls_once()
        self.assertEqual(self.store.db.execute('SELECT state FROM service_requests WHERE id=?',
                                               (request,)).fetchone()[0], 'done')
        self.assertFalse(await self.service.restart_once())
        self.service.on_restart.assert_not_called()
        await self.service.deliver_once()
        with self.assertRaises(_Restart):
            await self.service.restart_once()
        self.service.on_restart.assert_called_once_with()
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

    async def test_queued_external_restart_cannot_interrupt_live_update(self):
        from coordinator import updates
        self.store.service_request('service.restart', {'reason': 'deploy'})
        self.store.put(updates.KEY, dict(id='update', phase='tools', watchdog_pid=444,
                                       watchdog_process='watchdog'))
        with patch.object(updates.watch, 'process_identity', return_value='watchdog'):
            await self.service.controls_once()
        row = self.store.db.execute('SELECT state FROM service_requests ORDER BY id DESC LIMIT 1').fetchone()
        self.assertEqual(row['state'], 'refused')
        self.assertIsNone(self.store.get('restart_requested_v2'))

    async def test_update_restart_records_ownership_and_shutdown_grace(self):
        from coordinator import updates
        from coordinator.__main__ import RESTART_DEADLINE_SECONDS
        from coordinator.service import SHUTDOWN_SECONDS
        self.store.put(updates.KEY, dict(id='update', phase='restart', old_pid=os.getpid(),
                                       old_process='service'))
        self.store.put('restart_requested_v2', dict(update_id='update', reason='Telegram update'))
        with patch.object(updates.watch, 'process_identity', return_value='service'):
            with self.assertRaises(_Restart):
                await self.service.restart_once()
        record = self.store.get(updates.KEY)
        self.assertEqual(record.get('restart_from'), dict(pid=os.getpid(), process='service'))
        self.assertGreater(record['shutdown_grace'], RESTART_DEADLINE_SECONDS + SHUTDOWN_SECONDS)
        self.assertIsInstance(record['restart_started'], float)

    async def test_restart_backup_failure_keeps_request_and_does_not_exit(self):
        self.service.on_restart = Mock()
        with self.store.db:
            self.store.put('restart_requested_v2', {'reason': 'upgrade'})
        with patch.object(self.store, 'backup_for_restart', side_effect=OSError('disk full')):
            with self.assertRaisesRegex(OSError, 'disk full'):
                await self.service.restart_once()
        self.assertIsNotNone(self.store.get('restart_requested_v2'))
        self.assertIsNone(self.store.get('last_restart_v2'))
        self.service.on_restart.assert_not_called()

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

    async def test_network_poll_problems_are_limited_to_one_per_ten_minutes(self):
        now = [1700000000]
        gaps = iter((599, 1, 0))

        async def sleep(seconds):
            now[0] += next(gaps)

        self.telegram.updates = AsyncMock(side_effect=[
            TelegramError('network-or-invalid-response')] * 3 + [asyncio.CancelledError()])
        with patch('coordinator.problems.time.time', side_effect=lambda: now[0]), \
                patch('coordinator.service.asyncio.sleep', side_effect=sleep), \
                self.assertLogs('coordinator.problems', 'WARNING'):
            with self.assertRaises(asyncio.CancelledError):
                await self.service.poll()
        rows = self.problems()
        self.assertEqual([row['code'] for row in rows], ['poll-network-or-invalid-response'] * 2)
        self.assertIn('repeated=1', rows[-1]['detail'])

    async def test_daily_sweep_prunes_problems_even_with_stray_reports_off(self):
        from coordinator.stray_report import next_night
        now = [1700000000]
        with self.store.db:
            self.store.db.executemany('INSERT INTO problems(created,area,code) VALUES (?,?,?)',
                [(now[0] - 31 * 86400, 'service', 'old'), (now[0], 'service', 'recent')])
        delays = []

        async def pause(event, delay):
            delays.append(delay)
            if len(delays) == 1:
                with self.store.db:
                    self.store.db.execute('INSERT INTO problems(created,area,code) VALUES (?,?,?)',
                                          (now[0] - 31 * 86400, 'service', 'next-day'))
                now[0] = next_night(now[0])
                return
            raise asyncio.CancelledError

        with patch('coordinator.service.time.time', side_effect=lambda: now[0]), \
                patch('coordinator.service.pause', side_effect=pause):
            with self.assertRaises(asyncio.CancelledError):
                await self.service.stray_report_loop()
        self.assertEqual([row['code'] for row in self.problems()], ['recent'])
        self.assertTrue(all(delay is not None and 0 < delay <= 86400 for delay in delays))

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
            ('reaction-timeout', 'telegram=network-or-invalid-response disabled=False attempts=1')])

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
        self.assertEqual(self.sessions[0].sent[0][1], tag(1) + REPLY_FIRST_LINE + 'Replying to: The job report\nReacted 👍')
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
        self.assertEqual(self.sessions[0].sent[0][1], tag(1) + REPLY_FIRST_LINE + 'hello')

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

    async def test_claude_long_tool_steer_is_queued_and_delivered_once_after_boundary(self):
        from coordinator import control_api

        worker = self.running_worker('closed')
        writes = []
        closed = asyncio.Event()
        sid = str(uuid.uuid4())

        class Input:
            def write(self, data):
                writes.append(json.loads(data))

            async def drain(self):
                protocol.handle({'type': 'command_lifecycle', 'session_id': sid,
                                 'command_uuid': writes[-1]['uuid'], 'state': 'queued'})

            def close(self):
                closed.set()

            async def wait_closed(self):
                await closed.wait()

        process = SimpleNamespace(stdin=Input(), past_user=lambda wire: False)
        outcomes = []

        def outcome(message_id, state):
            outcomes.append((message_id, state))
            steer_outcome(self.store, message_id, state)

        control = RunControl(on_outcome=outcome)
        protocol = NativeProtocol('claude', process, control, sid, lambda value: value, reattaching=True)
        self.service.workers.controls[worker] = control
        try:
            for event in (
                    {'type': 'system', 'subtype': 'init'},
                    {'type': 'command_lifecycle', 'command_uuid': 'initial', 'state': 'queued'},
                    {'type': 'result', 'subtype': 'success'},
                    {'type': 'command_lifecycle', 'command_uuid': 'initial', 'state': 'started'},
                    {'type': 'system', 'subtype': 'task_started', 'task_id': 'long-tool', 'tool_use_id': 'tool-1'},
                    {'type': 'tool_progress', 'tool_use_id': 'tool-1-heartbeat-0'}):
                protocol.handle(dict(event, session_id=sid), replay=True)
            called = control_api.call(self.store, 'workers.steer', {'worker': worker, 'prompt': 'also this'})
            request = called.data['request']
            self.assertTrue(await self.service.controls_once())
            self.assertEqual(self.request(request), ('done', {'receipt': 'queued'}))
            self.assertEqual(called.state, 'queued')
            self.assertEqual(called.text, "Steer queued; delivered at the worker's next input point.")
            self.assertEqual(self.notes(), [])
            self.assertEqual(self.problems(), [])
            self.assertEqual(outcomes, [])
            await self.service.controls_once()
            mid = 'torii-v2-control-' + str(request)
            self.assertEqual(await control.steer(mid, 'also this'), 'queued')
            self.assertEqual([entry['message']['content'] for entry in writes], ['also this'])
            wire = writes[0]['uuid']
            protocol.handle({'type': 'system', 'subtype': 'task_notification', 'session_id': sid,
                             'task_id': 'long-tool', 'tool_use_id': 'tool-1'})
            protocol.handle({'type': 'command_lifecycle', 'session_id': sid,
                             'command_uuid': wire, 'state': 'started'})
            protocol.handle(dict(writes[0], isReplay=True))
            self.assertEqual(self.request(request), ('done', {'receipt': 'received'}))
            self.assertEqual(outcomes, [(mid, 'received')])
            self.assertEqual(await control.steer(mid, 'also this'), 'queued')
            self.assertEqual([entry['message']['content'] for entry in writes], ['also this'])
            protocol.handle({'type': 'result', 'subtype': 'success', 'session_id': sid})
            self.assertEqual(await control.steer('late', 'late'), 'closed')
        finally:
            protocol.disconnect()
            protocol.input_closed.cancel()
            await asyncio.gather(protocol.input_closed, return_exceptions=True)

    async def test_rejected_markup_delivers_text_and_unblocks_next_message(self):
        with self.store.db:
            self.store.enqueue_report(TOPIC, 'Button report', reply_markup={
                'inline_keyboard': [[{'text': 'ok', 'callback_data': 'x' * 70}]]})
            self.store.enqueue_report(TOPIC, 'Next report')
        send = self.telegram.send

        async def refused(row):
            if row.get('reply_markup'):
                raise TelegramError(400)
            return await send(row)

        self.telegram.send = refused
        self.assertTrue(await self.service.deliver_once())
        self.assertEqual(self.telegram.sent, [])
        with patch('coordinator.store.time.time', return_value=time.time() + 301):
            self.assertTrue(await self.service.deliver_once())
        self.assertEqual([row['text'] for row in self.telegram.sent], ['Button report'])
        self.assertIsNone(self.telegram.sent[0]['reply_markup'])
        self.assertTrue(await self.service.deliver_once())
        self.assertEqual([row['text'] for row in self.telegram.sent], ['Button report', 'Next report'])
        self.assertFalse(self.store.outbox_has_pending())
        self.assertFalse(await self.service.deliver_once())

    async def test_transient_markup_failure_keeps_buttons_for_retry(self):
        for code in (429, 502):
            with self.subTest(code=code):
                markup = {'inline_keyboard': [[{'text': 'ok', 'callback_data': 'ok'}]]}
                with self.store.db:
                    outbox = self.store.enqueue_report(TOPIC, 'Button report', reply_markup=markup)
                self.telegram.send = AsyncMock(side_effect=TelegramError(code, retry_after=1))
                self.assertTrue(await self.service.deliver_once())
                row = self.store.db.execute('SELECT * FROM outbox WHERE id=?', (outbox,)).fetchone()
                self.assertEqual(json.loads(row['reply_markup']), markup)
                self.assertEqual((row['attempts'], row['delivered']), (1, 0))
                self.assertGreater(row['next_attempt'], time.time())
                self.store.delivered(outbox, 1000 + code)

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
        self.store.worker_complete(worker, {'success': True})
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
        self.store.task_update(self.service.workers.get(worker)['task'], status='done')
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

    async def test_finished_worker_steer_resumes_newest_open_job_session_for_both_providers(self):
        from coordinator import control_api

        self.enable_claude_account()
        for provider in ('claude', 'codex'):
            for initial_text in ('Two schemas found. Tell me which to migrate.',
                                 'Done. Want me to open the PR?'):
                with self.subTest(provider=provider, text=initial_text):
                    session_id = str(uuid.uuid4())
                    transcript = self.root / 'primary' / 'projects' / 'test' / (session_id + '.jsonl')
                    transcript.parent.mkdir(parents=True, exist_ok=True)
                    transcript.write_text('{}\n')
                    started = asyncio.Event()
                    release = asyncio.Event()

                    class Runner:
                        def __init__(self):
                            self.calls = []

                        async def run(self, provider, prompt, cwd, session, **options):
                            self.calls.append((prompt, session, options['host_id'], options['fresh']))
                            if len(self.calls) == 1:
                                return RunResult(session or session_id, text=initial_text, success=True,
                                                 transcript_path=str(transcript))
                            started.set()
                            await release.wait()
                            return RunResult(session, text='Migration completed.', success=True)

                    runner = Runner()
                    self.service.runner = runner
                    self.service.workers.runner = runner
                    worker = self.worker(provider=provider)
                    await self.service.workers_once()
                    await asyncio.wait_for(asyncio.gather(*self.service.worker_tasks.values()), 2)
                    finished = self.service.workers.get(worker)
                    self.assertEqual(finished['status'], 'done')
                    saved_session = finished['session']
                    count = self.store.db.execute('SELECT COUNT(*) FROM workers').fetchone()[0]
                    with self.store.db:
                        accepted = control_api.call(self.store, 'workers.steer',
                            {'worker': worker, 'prompt': 'Migrate the first schema.'}, topic=TOPIC)
                    self.assertTrue(accepted.ok, accepted.text)
                    request = accepted.data['request']
                    await self.service.controls_once()
                    self.assertEqual(self.request(request), ('done', {'receipt': 'resumed'}))
                    resumed = self.service.workers.get(worker)
                    self.assertEqual((resumed['status'], resumed['session'], resumed['fresh'], resumed['host']),
                        ('queued', saved_session, 0, 'worker-%d-resume-%d' % (worker, request)))
                    self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM workers').fetchone()[0], count)
                    try:
                        await self.service.workers_once()
                        await asyncio.wait_for(started.wait(), 2)
                        self.assertFalse(await self.service.workers_once())
                        self.assertEqual(len(runner.calls), 2)
                        self.assertEqual(runner.calls[1], ('Migrate the first schema.', saved_session,
                            'worker-%d-resume-%d' % (worker, request), False))
                        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM messages "
                            "WHERE source_worker=? AND kind='worker_result'", (worker,)).fetchone()[0], 1)
                    finally:
                        release.set()
                        await asyncio.wait_for(asyncio.gather(*self.service.worker_tasks.values()), 2)
                    self.assertEqual(self.service.workers.get(worker)['status'], 'done')
                    results = [json.loads(row[0]) for row in self.store.db.execute(
                        "SELECT text FROM messages WHERE source_worker=? AND kind='worker_result' ORDER BY id",
                        (worker,))]
                    self.assertEqual([result['text'] for result in results], [initial_text, 'Migration completed.'])
                    self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM workers').fetchone()[0], count)
                    self.assertTrue(all(result['status'] == 'done' for result in results))

    async def test_finished_worker_resume_refuses_older_and_closed_jobs_for_both_providers(self):
        from coordinator import control_api

        for provider in ('claude', 'codex'):
            with self.subTest(provider=provider):
                worker = self.worker(provider=provider)
                self.store.worker_complete(worker, {'success': True, 'session_id': str(uuid.uuid4())})
                finished = self.service.workers.get(worker)
                with self.store.db:
                    newer = self.store.db.execute('''INSERT INTO workers
                        (task,topic,provider,prompt,cwd,workspace,status,created,updated)
                        VALUES (?,?,?,?,?,?,'done',1,1)''',
                        (finished['task'], TOPIC, provider, 'newer worker', str(self.root), '{}')).lastrowid
                    self.store.db.execute('UPDATE workers SET session=?,fresh=0 WHERE id=?',
                                          (str(uuid.uuid4()), newer))
                    older = control_api.call(self.store, 'workers.steer',
                        {'worker': worker, 'prompt': 'Continue.'}, topic=TOPIC)
                self.assertEqual((older.ok, older.text),
                                 (False, 'Worker cannot receive input. Status: done.'))
                for refused_worker in (worker, newer):
                    if refused_worker == newer:
                        with self.store.db:
                            self.store.db.execute("UPDATE tasks SET status='done' WHERE id=?", (finished['task'],))
                            closed = control_api.call(self.store, 'workers.steer',
                                {'worker': newer, 'prompt': 'Continue.'}, topic=TOPIC)
                        self.assertEqual((closed.ok, closed.text),
                                         (False, 'Worker cannot receive input. Status: done.'))
                    refused = self.store.service_request('workers.steer',
                        {'worker': refused_worker, 'prompt': 'Already queued before the job changed.'})
                    try:
                        with self.assertLogs('coordinator.problems', 'WARNING'):
                            await self.service.controls_once()
                        self.assertEqual(self.request(refused),
                                         ('refused', {'text': 'Worker cannot receive input.'}))
                        self.assertEqual(self.service.workers.get(refused_worker)['status'], 'done')
                    finally:
                        with self.store.db:
                            self.store.db.execute("UPDATE workers SET status='interrupted' WHERE status='queued'")

    async def test_finished_worker_resume_requires_session_and_no_live_task(self):
        from coordinator import control_api

        for provider in ('claude', 'codex'):
            with self.subTest(provider=provider):
                worker = self.worker(provider=provider)
                self.store.worker_complete(worker, {'success': True})
                with self.store.db:
                    missing = control_api.call(self.store, 'workers.steer',
                        {'worker': worker, 'prompt': 'Continue.'}, topic=TOPIC)
                self.assertEqual((missing.ok, missing.text),
                                 (False, 'Worker cannot receive input. Status: done.'))
                request = self.store.service_request('workers.steer', {'worker': worker, 'prompt': 'Continue.'})
                with self.assertLogs('coordinator.problems', 'WARNING'):
                    await self.service.controls_once()
                self.assertEqual(self.request(request), ('refused', {'text': 'Worker cannot receive input.'}))
                with self.store.db:
                    self.store.db.execute('UPDATE workers SET session=? WHERE id=?', (str(uuid.uuid4()), worker))
                release = asyncio.Event()
                task = asyncio.create_task(release.wait())
                self.service.worker_tasks[worker] = task
                try:
                    with self.store.db:
                        queued = control_api.call(self.store, 'workers.steer',
                            {'worker': worker, 'prompt': 'Continue.'}, topic=TOPIC)
                    self.assertTrue(queued.ok, queued.text)
                    with self.assertLogs('coordinator.problems', 'WARNING'):
                        await self.service.controls_once()
                    self.assertEqual(self.request(queued.data['request']),
                                     ('refused', {'text': 'Worker cannot receive input.'}))
                    self.assertEqual(self.service.workers.get(worker)['status'], 'done')
                    self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM messages "
                        "WHERE source_worker=? AND kind='worker_result'", (worker,)).fetchone()[0], 1)
                    self.assertEqual(self.runner.calls, [])
                finally:
                    release.set()
                    await task

    async def test_finished_worker_resume_refuses_failed_fresh_run_for_both_providers(self):
        from coordinator import control_api

        self.enable_claude_account()
        for provider in ('claude', 'codex'):
            for boundary in ('control_api', 'service'):
                with self.subTest(provider=provider, boundary=boundary):
                    worker = self.worker(provider=provider)
                    session = str(uuid.uuid4())
                    with self.store.db:
                        self.store.db.execute('UPDATE workers SET session=? WHERE id=?', (session, worker))
                    self.runner.result = RunResult(session, failure_code='start_failed',
                                                   error='Provider host exited before becoming ready')
                    before = len(self.runner.calls)
                    with self.assertLogs('coordinator.problems', 'WARNING'):
                        await self.service.workers_once()
                        await asyncio.wait_for(asyncio.gather(*self.service.worker_tasks.values()), 2)
                    row = self.service.workers.get(worker)
                    result = json.loads(row['result'])
                    self.assertEqual((row['status'], row['session'], row['fresh'], result['failure_code']),
                                     ('done', session, 1, 'start_failed'))
                    self.assertFalse(result.get('transcript_path'))
                    try:
                        if boundary == 'control_api':
                            with self.store.db:
                                refused = control_api.call(self.store, 'workers.steer',
                                    {'worker': worker, 'prompt': 'Continue.'}, topic=TOPIC)
                            self.assertEqual((refused.ok, refused.text),
                                             (False, 'Worker cannot receive input. Status: done.'))
                            self.assertEqual(self.store.db.execute(
                                "SELECT COUNT(*) FROM service_requests WHERE state='queued' "
                                "AND op!='_cleanup.host_logs'").fetchone()[0], 0)
                        else:
                            request = self.store.service_request('workers.steer',
                                {'worker': worker, 'prompt': 'Already queued before the failure.'})
                            with self.assertLogs('coordinator.problems', 'WARNING'):
                                await self.service.controls_once()
                            self.assertEqual(self.request(request),
                                             ('refused', {'text': 'Worker cannot receive input.'}))
                        await self.service.controls_once()
                        await self.service.workers_once()
                        await asyncio.wait_for(asyncio.gather(*self.service.worker_tasks.values()), 2)
                        self.assertEqual(self.service.workers.get(worker)['status'], 'done')
                        self.assertEqual(len(self.runner.calls), before + 1)
                        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM messages "
                            "WHERE source_worker=? AND kind='worker_result'", (worker,)).fetchone()[0], 1)
                    finally:
                        with self.store.db:
                            self.store.db.execute("UPDATE workers SET status='interrupted' WHERE status='queued'")
                            self.store.db.execute("UPDATE service_requests SET state='refused' WHERE state='queued'")

    async def test_finished_goal_unmet_worker_steer_resumes_same_session(self):
        from coordinator import control_api

        self.enable_claude_account()
        goal = 'all checks pass'
        root = self.root
        for provider, unmet, met in (('codex', 'budgetLimited', 'complete'), ('claude', 'failed', 'met')):
            with self.subTest(provider=provider):
                session_id = str(uuid.uuid4())
                calls = []

                class Runner:
                    async def run(self, provider, prompt, cwd, session, **options):
                        calls.append((provider, prompt, session, options['fresh'], options['host_id'],
                                      options.get('goal'),
                                      options['current_goal']() if options.get('current_goal') else None,
                                      options.get('initial_steer'), options.get('resume_path')))
                        sid = session or session_id
                        options['on_session'](sid)
                        transcript = root / 'primary' / 'projects' / 'test' / (sid + '.jsonl')
                        if provider == 'claude':
                            transcript.parent.mkdir(parents=True, exist_ok=True)
                            transcript.write_text('{}\n')
                        return RunResult(sid, text='work remains' if len(calls) == 1 else 'verified',
                                         success=True, goal_status={'status': unmet if len(calls) == 1 else met},
                                         transcript_path=str(transcript) if provider == 'claude' else None)

                self.service.workers.runner = Runner()
                worker = self.worker(provider=provider, goal=goal)
                with self.assertLogs('coordinator.problems', 'WARNING'):
                    await self.service.workers_once()
                    await asyncio.wait_for(asyncio.gather(*self.service.worker_tasks.values()), 2)
                finished = self.service.workers.get(worker)
                session_id = finished['session']
                self.assertTrue(session_id)
                transcript = root / 'primary' / 'projects' / 'test' / (session_id + '.jsonl')
                self.assertEqual((finished['status'], finished['session'], finished['fresh']), ('done', session_id, 0))
                result = json.loads(finished['result'])
                self.assertEqual((result['success'], result['failure_code'], result['failure_detail']),
                                 (False, 'goal_unmet', unmet))
                with self.store.db:
                    accepted = control_api.call(self.store, 'workers.steer',
                        {'worker': worker, 'prompt': 'Finish the checks.'}, topic=TOPIC)
                self.assertTrue(accepted.ok, accepted.text)
                request = accepted.data['request']
                await self.service.controls_once()
                self.assertEqual(self.request(request), ('done', {'receipt': 'resumed'}))
                resumed = self.service.workers.get(worker)
                self.assertEqual((resumed['status'], resumed['session'], resumed['fresh']), ('queued', session_id, 0))
                self.assertEqual(self.store.db.execute(
                    'SELECT COUNT(*) FROM workers WHERE task=?', (finished['task'],)).fetchone()[0], 1)
                await self.service.workers_once()
                await asyncio.wait_for(asyncio.gather(*self.service.worker_tasks.values()), 2)
                self.assertEqual([call[:5] for call in calls], [
                    (provider, 'do the task', None if provider == 'codex' else session_id, True, 'worker-%d' % worker),
                    (provider, 'Finish the checks.', session_id, False, 'worker-%d-resume-%d' % (worker, request))])
                for call in calls:
                    if provider == 'codex':
                        self.assertEqual(call[5:8], (goal, goal, None))
                    else:
                        self.assertEqual(call[5:8], (None, None, '/goal ' + goal))
                self.assertEqual(calls[1][8], str(transcript) if provider == 'claude' else None)
                final = self.service.workers.get(worker)
                self.assertEqual((final['status'], final['session'], final['fresh']), ('done', session_id, 0))
                results = [json.loads(row[0]) for row in self.store.db.execute(
                    "SELECT text FROM messages WHERE source_worker=? AND kind='worker_result' ORDER BY id", (worker,))]
                self.assertEqual([(item['success'], item['failure_code'], item['failure_detail']) for item in results],
                                 [(False, 'goal_unmet', unmet), (True, None, None)])
                self.assertEqual([(row['code'], row['detail']) for row in self.problems() if row['worker'] == worker],
                                 [('goal_unmet', unmet)])
                self.assertEqual(self.notes(), [])

    async def test_restart_keeps_queued_resume(self):
        session_id = str(uuid.uuid4())
        worker = self.worker(provider='codex')
        with self.store.db:
            self.store.db.execute("UPDATE workers SET status='done',session=?,fresh=0 WHERE id=?",
                                  (session_id, worker))
        request = self.store.service_request('workers.steer', {'worker': worker, 'prompt': 'Continue after restart.'})
        await self.service.controls_once()
        self.assertEqual(self.request(request), ('done', {'receipt': 'resumed'}))
        self.store.close()
        self.store = Store(self.root / 'state')
        restarted = Service(self.store, self.telegram, self.runner, self.root, pair_only=False)
        await restarted.workers_once()
        await asyncio.gather(*restarted.worker_tasks.values())
        await restarted.workers_once()
        self.assertEqual(len(self.runner.calls), 1)
        self.assertEqual(self.runner.calls[0][1:4], ('Continue after restart.', str(self.root), session_id))
        self.assertEqual(restarted.workers.get(worker)['status'], 'done')

    async def queued_worker_for_close(self, resume):
        from coordinator import control_api

        self.runner.calls.clear()
        task = self.store.task_create(TOPIC, 'Close before launch', worktree=str(self.root))
        with self.store.db:
            spawned = control_api.call(self.store, 'workers.spawn',
                {'task': task['id'], 'provider': 'codex', 'prompt': 'Do the task.'}, topic=TOPIC)
        self.assertTrue(spawned.ok, spawned.text)
        worker = spawned.data['worker']
        if resume:
            session = str(uuid.uuid4())
            with self.store.db:
                self.store.db.execute("UPDATE workers SET status='done',session=?,fresh=0 WHERE id=?",
                                      (session, worker))
                steered = control_api.call(self.store, 'workers.steer',
                    {'worker': worker, 'prompt': 'Continue the task.'}, topic=TOPIC)
            self.assertTrue(steered.ok, steered.text)
            await self.service.controls_once()
            self.assertEqual(self.request(steered.data['request']), ('done', {'receipt': 'resumed'}))
            row = self.service.workers.get(worker)
            self.assertEqual((row['status'], row['session'], row['fresh']), ('queued', session, 0))
        return task, worker

    def assert_closed_worker_interrupted(self, worker):
        self.assertEqual(len(self.runner.calls), 0)
        self.assertEqual(self.service.workers.get(worker)['status'], 'interrupted')
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM messages "
            "WHERE source_worker=? AND kind='worker_result'", (worker,)).fetchone()[0], 0)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM problems').fetchone()[0], 0)

    async def test_closed_job_never_launches_queued_worker(self):
        for status in ('done', 'dropped'):
            for resume in (False, True):
                with self.subTest(status=status, resume=resume):
                    task, worker = await self.queued_worker_for_close(resume)
                    waiting = []
                    with self.store.db:
                        for state in ('waiting_for_secret', 'waiting_for_quota'):
                            waiting.append(self.store.db.execute('''INSERT INTO workers
                                (task,topic,provider,prompt,cwd,workspace,status,created,updated)
                                VALUES (?,?,'codex','Waiting',?,'{}',?,1,1)''',
                                (task['id'], TOPIC, str(self.root), state)).lastrowid)
                    self.store.task_update(task['id'], status=status)
                    with self.subTest(phase='close'):
                        self.assertEqual([self.service.workers.get(item)['status'] for item in (worker, *waiting)],
                                         ['interrupted', 'interrupted', 'interrupted'])
                    await self.service.workers_once()
                    await asyncio.wait_for(asyncio.gather(*self.service.worker_tasks.values()), 2)
                    for closed_worker in (worker, *waiting):
                        self.assert_closed_worker_interrupted(closed_worker)

    async def test_job_closed_during_worker_launch_never_runs(self):
        for status in ('done', 'dropped'):
            for resume in (False, True):
                for legacy_close in (False, True):
                    with self.subTest(status=status, resume=resume, legacy_close=legacy_close):
                        task, worker = await self.queued_worker_for_close(resume)
                        closing = Store(self.root / 'state')

                        def close_during_launch(store, vault, saved_task):
                            self.assertEqual(saved_task['status'], 'open')
                            if legacy_close:
                                with closing.db:
                                    closing.db.execute('UPDATE tasks SET status=? WHERE id=?', (status, task['id']))
                            else:
                                closing.task_update(task['id'], status=status)
                            return {}, []

                        try:
                            with patch('coordinator.workers.load_for_worker', side_effect=close_during_launch) as load:
                                await self.service.workers_once()
                                await asyncio.wait_for(asyncio.gather(*self.service.worker_tasks.values()), 2)
                            load.assert_called_once()
                        finally:
                            closing.close()
                        self.assert_closed_worker_interrupted(worker)

    async def test_closed_job_with_legacy_queued_worker_ignores_missing_worktree(self):
        for status in ('done', 'dropped'):
            for worker_status in ('queued', 'waiting_for_secret'):
                with self.subTest(status=status, worker_status=worker_status):
                    worktree = self.root / ('removed-' + status + '-' + worker_status)
                    worktree.mkdir()
                    task = self.store.task_create(TOPIC, 'Legacy closed job', worktree=str(worktree))
                    with self.store.db:
                        self.store.db.execute('UPDATE tasks SET status=? WHERE id=?', (status, task['id']))
                        worker = self.store.db.execute('''INSERT INTO workers
                            (task,topic,provider,prompt,cwd,workspace,status,created,updated)
                            VALUES (?,?,'codex','Legacy queued worker',?,'{}',?,1,1)''',
                            (task['id'], TOPIC, str(worktree), worker_status)).lastrowid
                    worktree.rmdir()
                    await self.service.workers_once()
                    await asyncio.wait_for(asyncio.gather(*self.service.worker_tasks.values()), 2)
                    self.assert_closed_worker_interrupted(worker)

    async def test_goal_queued_mid_turn_is_recorded_as_queued_then_received(self):
        worker = self.running_worker('queued')
        goal = self.store.service_request('workers.goal', {'worker': worker, 'condition': 'tests pass'})
        await self.service.controls_once()
        self.assertEqual(self.request(goal), ('done', {'receipt': 'queued'}))
        steer_outcome(self.store, 'torii-v2-control-%d' % goal, 'received')
        self.assertEqual(self.request(goal), ('done', {'receipt': 'received'}))
        self.assertEqual(self.notes(), [])
        self.assertEqual(self.problems(), [])

    async def test_done_queued_steers_for_claude_and_codex_are_unchanged(self):
        requests = []
        workers = []
        for provider in ('claude', 'codex'):
            worker = self.worker(provider=provider)
            workers.append(worker)
            self.store.worker_complete(worker, {'success': True})
            for op in ('workers.steer', 'workers.goal'):
                request = self.store.service_request(op, {'worker': worker})
                self.store.service_request_settle(request, 'done', {'receipt': 'queued', 'provider': provider})
                requests.append(request)
        before = [tuple(self.store.db.execute('SELECT state,result FROM service_requests WHERE id=?',
                                              (request,)).fetchone()) for request in requests]
        messages_before = self.store.db.execute('SELECT COUNT(*) FROM messages').fetchone()[0]
        for worker in workers:
            reconcile_steers(self.store, worker)
        after = [tuple(self.store.db.execute('SELECT state,result FROM service_requests WHERE id=?',
                                             (request,)).fetchone()) for request in requests]
        self.assertEqual(after, before)
        self.assertEqual(self.problems(), [])
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM messages').fetchone()[0], messages_before)

    async def test_reconciling_finished_opencode_worker_leaves_running_workers_steers_unchanged(self):
        finished = self.worker(provider='opencode')
        running = self.worker(provider='opencode')
        self.store.worker_complete(finished, {'success': True})
        with self.store.db:
            self.store.db.execute("UPDATE workers SET status='running' WHERE id=?", (running,))
        requests = {}
        for worker in (finished, running):
            requests[worker] = []
            for state, receipt in (('done', 'queued'), ('uncertain', 'uncertain')):
                request = self.store.service_request('workers.steer', {'worker': worker, 'prompt': 'lost'})
                self.store.service_request_settle(request, state, {'receipt': receipt})
                requests[worker].append(request)
        before = [dict(self.store.db.execute('SELECT * FROM service_requests WHERE id=?',
                                            (request,)).fetchone()) for request in requests[running]]
        reconcile_steers(self.store, finished)
        for request in requests[finished]:
            self.assertEqual(self.request(request), ('uncertain', {'receipt': 'uncertain', 'noted': True}))
        after = [dict(self.store.db.execute('SELECT * FROM service_requests WHERE id=?',
                                           (request,)).fetchone()) for request in requests[running]]
        self.assertEqual(after, before)
        self.assertEqual(len(self.notes()), 2)
        self.assertEqual([(row['code'], row['worker']) for row in self.problems()],
                         [('steer-uncertain', finished), ('steer-uncertain', finished)])

    async def test_reconciling_finished_worker_leaves_uncertain_stop_unchanged(self):
        worker = self.worker()
        self.store.worker_complete(worker, {'success': True})
        request = self.store.service_request('workers.stop', {'worker': worker})
        self.store.service_request_settle(request, 'uncertain', {'receipt': 'uncertain'})
        before = dict(self.store.db.execute('SELECT * FROM service_requests WHERE id=?', (request,)).fetchone())
        messages_before = self.store.db.execute('SELECT COUNT(*) FROM messages').fetchone()[0]
        reconcile_steers(self.store, worker)
        after = dict(self.store.db.execute('SELECT * FROM service_requests WHERE id=?', (request,)).fetchone())
        self.assertEqual(after, before)
        self.assertEqual(self.problems(), [])
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM messages').fetchone()[0], messages_before)

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

    async def test_restart_settles_a_seen_steer_without_writing_it_again(self):
        worker = self.worker()
        request = self.store.service_request('workers.steer', {'worker': worker, 'prompt': 'seen'})
        host_directory = self.store.directory / 'hosts' / ('worker-%d' % worker)
        host_directory.mkdir(parents=True)
        writes = host_directory / 'writes.jsonl'
        wire = steer_wire('torii-v2-control-%d' % request)
        event = {'type': 'user', 'uuid': wire, 'session_id': 'worker-session', 'parent_tool_use_id': None,
                 'message': {'role': 'user', 'content': 'seen'}}
        writes.write_text(json.dumps({'seq': 1, 'line': json.dumps(event)}) + '\n')
        transcript = self.root / 'session.jsonl'
        transcript.write_text(json.dumps({'type': 'attachment', 'attachment': {
            'type': 'queued_command', 'source_uuid': wire}}) + '\n')
        with self.store.db:
            self.store.db.execute("UPDATE workers SET status='running',result=? WHERE id=?",
                                  (json.dumps({'transcript_path': str(transcript)}), worker))
            self.store.db.execute("UPDATE service_requests SET state='sending' WHERE id=?", (request,))
        session_factory = self.service.session_factory
        self.store.close()
        self.store = Store(self.root / 'state')
        self.service = Service(self.store, self.telegram, self.runner, self.root, session_factory=session_factory)

        class Control:
            def __init__(inner_self):
                inner_self.sent = []

            async def steer(inner_self, message_id, prompt):
                inner_self.sent.append((message_id, prompt))
                repeated = {'type': 'user', 'uuid': steer_wire(message_id), 'session_id': 'worker-session',
                            'parent_tool_use_id': None, 'message': {'role': 'user', 'content': prompt}}
                with writes.open('a') as stream:
                    stream.write(json.dumps({'seq': 2, 'line': json.dumps(repeated)}) + '\n')
                return 'received'

        control = Control()
        self.service.workers.controls[worker] = control
        self.store.requests_uncertain()
        self.service.reconcile_steers()
        await self.service.controls_once()
        self.assertEqual(self.request(request), ('done', {'receipt': 'received', 'reconciled': True}))
        self.assertEqual(control.sent, [])
        from coordinator.host import HostClient
        written = [json.loads(line) for line in HostClient(host_directory).written_lines()]
        self.assertEqual([row['uuid'] for row in written if row.get('type') == 'user'], [wire])

    async def test_startup_keeps_an_unproven_steer_uncertain_and_does_not_rewrite_it(self):
        worker = self.worker()
        request = self.store.service_request('workers.steer', {'worker': worker, 'prompt': 'unproven'})
        host_directory = self.store.directory / 'hosts' / ('worker-%d' % worker)
        host_directory.mkdir(parents=True)
        writes = host_directory / 'writes.jsonl'
        wire = steer_wire('torii-v2-control-%d' % request)
        event = {'type': 'user', 'uuid': wire, 'session_id': 'worker-session', 'parent_tool_use_id': None,
                 'message': {'role': 'user', 'content': 'unproven'}}
        writes.write_text(json.dumps({'seq': 1, 'line': json.dumps(event)}) + '\n')
        self.store.worker_complete(worker, {'success': True})
        with self.store.db:
            self.store.db.execute("UPDATE service_requests SET state='sending' WHERE id=?", (request,))
        session_factory = self.service.session_factory
        self.store.close()
        self.store = Store(self.root / 'state')
        self.service = Service(self.store, self.telegram, self.runner, self.root, session_factory=session_factory)

        class Control:
            async def steer(inner_self, message_id, prompt):
                repeated = {'type': 'user', 'uuid': steer_wire(message_id), 'session_id': 'worker-session',
                            'parent_tool_use_id': None, 'message': {'role': 'user', 'content': prompt}}
                with writes.open('a') as stream:
                    stream.write(json.dumps({'seq': 2, 'line': json.dumps(repeated)}) + '\n')
                return 'received'

        self.service.workers.controls[worker] = Control()
        self.store.requests_uncertain()
        self.service.reconcile_steers()
        await self.service.controls_once()
        self.assertEqual(self.request(request), ('uncertain', {'receipt': 'uncertain', 'noted': True}))
        note, = self.notes()
        self.assertEqual(note, 'Steer request %d to worker %d may not have reached it: the worker ended '
                         'before the message showed in its transcript.' % (request, worker))
        self.service.reconcile_steers()
        self.assertEqual(self.notes(), [note])
        from coordinator.host import HostClient
        written = [json.loads(line) for line in HostClient(host_directory).written_lines()]
        self.assertEqual([row['uuid'] for row in written if row.get('type') == 'user'], [wire])
        await self.drain_feed()
        self.assertEqual([text for _, text in self.sessions[-1].sent if note in text],
                         [tag(self.store.db.execute("SELECT id FROM messages WHERE kind='callback'").fetchone()[0],
                              'callback') + note])

    async def test_failed_parent_notification_does_not_replay_finished_worker(self):
        from coordinator import control_api

        self.enable_claude_account()
        worker = self.worker()
        await self.service.workers_once()
        await asyncio.wait_for(asyncio.gather(*self.service.worker_tasks.values()), 2)
        worker_before = dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (worker,)).fetchone())
        task_number = self.store.db.execute('SELECT number FROM tasks WHERE id=?',
                                            (worker_before['task'],)).fetchone()[0]
        notification = control_api.call(self.store, 'telegram.send',
            {'topic': TOPIC, 'text': 'The worker finished.', 'task': task_number}, topic=TOPIC, source='mcp')
        self.assertTrue(notification.ok, notification.text)
        outbox_id = notification.data['outbox']
        self.assertEqual(tuple(self.store.db.execute('SELECT kind,text FROM outbox WHERE id=?',
                                                     (outbox_id,)).fetchone()),
                         ('report', 'The worker finished.'))

        async def failed(row):
            raise TelegramError(502)

        self.telegram.send = failed
        for attempt in range(2):
            with self.store.db:
                self.store.db.execute('UPDATE outbox SET next_attempt=0 WHERE id=?', (outbox_id,))
            self.assertTrue(await self.service.deliver_once())
            self.assertFalse(await self.service.workers_once())
            self.assertEqual(dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (worker,)).fetchone()),
                             worker_before)
            self.assertEqual(len(self.runner.calls), 1)
            self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM messages WHERE source_worker=? "
                                                   "AND kind='worker_result'", (worker,)).fetchone()[0], 1)
        self.assertEqual(self.store.db.execute('SELECT attempts FROM outbox WHERE id=?', (outbox_id,)).fetchone()[0], 2)

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
            self.store.put('service_run', {'state': 'exited', 'cause': 'signal', 'ended': 100.0, 'checked': 100.0})
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

    async def test_h_record_start_keeps_the_supplied_stamp_or_none(self):
        for code in ('stamp-at-start', None):
            with self.subTest(code=code), patch('coordinator.log.code_stamp', return_value='later'), \
                    patch('coordinator.service.code_stamp', return_value='later', create=True):
                instance = (Service(self.store, self.telegram, self.runner, self.root, code=code)
                            if code is not None else Service(self.store, self.telegram, self.runner, self.root))
                with self.assertLogs('coordinator.problems'):
                    instance.record_start()
                self.assertEqual(self.store.get('service_run')['code'], code)

    async def test_T4_damaged_owned_folder_does_not_block_service_start(self):
        from coordinator import process_ownership as ownership
        from tests.test_process_ownership import FakeBackend
        directory = self.store.directory / 'hosts' / 'damaged-owned-host'
        directory.mkdir(parents=True)
        (directory / 'job.plist').write_bytes(b'')
        loops = []
        async def workers():
            await self.service.host_logs.clean()
            loops.append('workers')
            await self.service.workers_once()
            raise _Restart()
        self.service.supervised = lambda: [('workers', workers)]
        with patch.object(self.service.extension, 'start', AsyncMock()), \
                patch.object(self.service.extension, 'close', AsyncMock()), \
                patch.object(ownership, 'backend', FakeBackend()):
            await self.service.run()
        self.assertEqual(loops, ['workers'])
        self.assertTrue(self.sessions)
        self.assertTrue(self.store.get('service_run')['cause'].startswith('restart'))
        rows = list(self.store.db.execute("SELECT code,detail FROM problems WHERE area='worker'"))
        self.assertEqual(len(rows), 1)
        self.assertIn('damaged-owned-host', rows[0]['detail'])

    async def test_T8_dead_worker_cleans_before_bootout_and_dispatch_survives_errors(self):
        from coordinator import host, process_ownership as ownership
        from coordinator.workers import host_directory
        from tests.test_process_ownership import FakeBackend, member, save_owner
        for failure in ('none', 'empty', 'partial', 'oserror', 'timeout'):
            with self.subTest(failure=failure):
                fake = FakeBackend([member(101, 1001)])
                workers = [self.worker(provider='opencode'), self.worker(provider='opencode')]
                directories = []
                for worker in workers:
                    self.store.db.execute("UPDATE workers SET status='running' WHERE id=?", (worker,))
                    row = dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (worker,)).fetchone())
                    directory = Path(host_directory(self.store.directory, row))
                    directory.mkdir(parents=True)
                    ownership.create_job(directory, [], self.root, '', directory.name)
                    save_owner(directory)
                    ownership.write(directory / 'pid', 100)
                    directories.append(directory)
                if failure in ('empty', 'partial'):
                    (directories[0] / 'job.plist').write_bytes(b'' if failure == 'empty' else b'<plist>')
                order = []
                real_cleanup = ownership.cleanup
                def cleanup(directory):
                    order.append(('cleanup', directory.name))
                    return real_cleanup(directory)
                def bootout(label):
                    name = label.removeprefix('local.torii-host.')
                    order.append(('bootout', name))
                    if name == directories[0].name:
                        if failure == 'oserror':
                            raise OSError('injected bootout failure')
                        if failure == 'timeout':
                            raise subprocess.TimeoutExpired('launchctl', 15)
                    fake.bootouts.append(label)
                before = self.store.db.execute("SELECT count(*) FROM problems WHERE area='worker' AND code LIKE 'ownership-%'").fetchone()[0]
                with patch.object(ownership, 'backend', fake), patch.object(host, 'alive', return_value=False), \
                        patch.object(ownership, 'cleanup', side_effect=cleanup), \
                        patch.object(fake, 'bootout', side_effect=bootout):
                    self.assertTrue(await self.service.workers_once())
                for worker, directory in zip(workers, directories):
                    row = self.store.db.execute('SELECT * FROM workers WHERE id=?', (worker,)).fetchone()
                    self.assertEqual((row['status'], json.loads(row['result'])['failure_code']),
                                     ('interrupted', 'interrupted'))
                    if ('bootout', directory.name) in order:
                        self.assertLess(order.index(('cleanup', directory.name)), order.index(('bootout', directory.name)))
                after = self.store.db.execute("SELECT count(*) FROM problems WHERE area='worker' AND code LIKE 'ownership-%'").fetchone()[0]
                self.assertEqual(after - before, int(failure != 'none'))
                self.assertTrue((directories[1] / 'job-booted-out').exists())
                self.assertEqual(fake.table, {})

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
        self.assertNotIn('code', self.store.get('service_run'))
        with self.assertLogs('coordinator.problems', 'WARNING'):
            Service(self.store, self.telegram, self.runner, self.root).record_start()
        rows = self.problems()
        self.assertEqual([row['code'] for row in rows], ['started', 'unclean-exit', 'unclean-exit', 'started'])
        self.assertEqual(rows[0]['detail'], 'previous exit: unknown, no record')
        self.assertRegex(rows[1]['detail'], r'^previous run started \S+ ended without an exit record$')
        self.assertEqual(rows[3]['detail'], 'previous exit: restart reason=deploy')


class DisabledParentTests(ServiceTestsSupport, unittest.IsolatedAsyncioTestCase):
    def other_topic(self, enabled=True):
        with self.store.db:
            self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES(?,-10042,1,?,?,?)',
                                  ('other', 'Other', str(self.root), enabled))

    def disable(self):
        from coordinator import control_api
        with self.store.db:
            result = control_api.call(self.store, 'topic.disable', {'topic': 'other'})
        self.assertTrue(result.ok, result.text)

    async def run_passes(self, check):
        async def passes():
            try:
                await check()
            finally:
                for session in list(self.sessions):
                    if hasattr(session, 'detach'):
                        await session.stop()
                        self.sessions.remove(session)
            raise _Restart()

        async def supervise(name, start):
            await start()

        self.service.supervised = lambda: [('passes', passes)]
        with patch.object(self.service.extension, 'start', AsyncMock()), \
                patch.object(self.service.extension, 'close', AsyncMock()), \
                patch.object(self.service, 'supervise', side_effect=supervise):
            await self.service.run()

    async def test_D1_restart_stops_disabled_host_and_reattaches_enabled_host(self):
        hosts = {}
        for topic, enabled in (('disabled', 0), ('enabled', 1)):
            with self.store.db:
                self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES(?,1,1,?,?,?)',
                                      (topic, topic, str(self.root), enabled))
                key = self.service.session_key(topic)
                self.store.put(key + '_session', topic + '-native')
                self.store.put(key + '_host', {'id': topic + '-host'})
            hosts[topic] = FakeSession(topic + '-native')
            hosts[topic].idle = lambda: True
            self.sessions.append(hosts[topic])
        factory = self.service.session_factory
        attached = []

        async def reattach(store, runner, cwd, model, instructions, topic=None):
            if topic in hosts:
                attached.append(topic)
                return hosts[topic]
            return await factory(store, runner, cwd, model, instructions, topic=topic)

        async def passes():
            for _ in range(3):
                await self.service.feed_once()
            raise _Restart()

        self.service.session_factory = reattach
        self.service.supervised = lambda: [('passes', passes)]
        with patch.object(self.service.extension, 'start', AsyncMock()), \
                patch.object(self.service.extension, 'close', AsyncMock()):
            await self.service.run()
        self.assertTrue(hosts['disabled'].closed, 'disabled host survived startup and three feed passes')
        self.assertFalse(hosts['enabled'].closed)
        self.assertEqual(attached.count('enabled'), 1)
        self.assertIs(self.service.sessions['enabled'], hosts['enabled'])
        self.assertEqual(self.store.get('coordinator:disabled_session'), 'disabled-native')
        self.assertFalse(any(row['topic'] == 'disabled' and row['kind'] == 'restarted'
                             for row in self.store.messages_pending()))

    async def test_D2_disabled_parent_waits_for_background_and_feed_then_stops_next_pass(self):
        self.other_topic()
        session = await self.service.start_session('other')
        session.protocol = SimpleNamespace(background=[])
        session.busy = False
        session.turn_open = False
        session.idle = lambda: not (session.busy or session.turn_open or session.protocol.background)
        self.disable()
        for state in ('busy', 'turn_open', 'background', 'feed'):
            with self.subTest(state=state):
                session.busy = state == 'busy'
                session.turn_open = state == 'turn_open'
                session.protocol.background = ['work'] if state == 'background' else []
                if state == 'feed':
                    row = self.store.message_save('other', 'owner', 'hold')
                    self.service.feed_tasks[row['id']] = asyncio.create_task(asyncio.Event().wait())
                await self.service.feed_once()
                self.assertFalse(session.closed)
        task = self.service.feed_tasks[row['id']]
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self.service.feed_tasks.clear()
        with self.assertLogs('coordinator.service', level='INFO') as logs:
            await self.service.feed_once()
            await self.service.feed_once()
        self.assertTrue(session.closed)
        self.assertEqual(logs.output, ['INFO:coordinator.service:parent stopped topic=other reason=disabled'])
        self.assertEqual(self.problems(), [])
        self.assertEqual(self.telegram.sent, [])
        self.assertEqual(list(self.store.db.execute('SELECT * FROM outbox')), [])

    async def test_D1_disabled_host_attaches_even_while_accounts_are_paused(self):
        self.other_topic(False)
        self.store.put('coordinator:other_host', {'id': 'existing'})
        self.store.put('coordinator_account_retry_at', time.time() + 3600)
        self.service.parent_available = lambda: False
        session = await self.service.start_session('other', reattach_only=True)
        self.assertIsNotNone(session)
        self.assertEqual(self.problems(), [])
        self.assertEqual(list(self.store.db.execute('SELECT * FROM outbox')), [])

    async def test_D1_unreadable_disabled_host_is_stopped_without_a_problem_or_notice(self):
        self.other_topic(False)
        self.store.put('coordinator:other_host', {'id': 'unreadable'})
        self.store.put('coordinator:other_session', 'saved-native')
        self.service.session_factory = AsyncMock(side_effect=ValueError('unreadable host state'))
        with patch('coordinator.host.HostClient.stop', AsyncMock()) as stop, \
                self.assertLogs('coordinator.service', level='INFO') as logs:
            self.assertIsNone(await self.service.start_session('other', reattach_only=True))
        stop.assert_awaited_once_with()
        self.assertEqual(logs.output, ['INFO:coordinator.service:parent stopped topic=other reason=disabled '
                                       'state=unavailable error=ValueError'])
        self.assertIsNone(self.store.get('coordinator:other_host'))
        self.assertEqual(self.store.get('coordinator:other_session'), 'saved-native')
        self.assertEqual(self.problems(), [])
        self.assertEqual(list(self.store.db.execute('SELECT * FROM outbox')), [])

    async def test_D1_failed_disabled_host_stop_does_not_block_later_startup_reattach(self):
        self.other_topic(False)
        with self.store.db:
            self.store.put('coordinator:other_host', {'id': 'unreachable'})
            self.store.put('coordinator:other_session', 'saved-native')
            self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES(?,1,2,?,?,1)',
                                  ('later', 'Later', str(self.root)))
            self.store.put('coordinator:later_host', {'id': 'later-host'})
        factory = self.service.session_factory

        async def reattach(store, runner, cwd, model, instructions, topic=None):
            if topic == 'other':
                raise ValueError('unreadable host state')
            return await factory(store, runner, cwd, model, instructions, topic=topic)

        async def check():
            self.assertIn('later', self.service.sessions)
            self.assertFalse(self.service.sessions['later'].closed)
            self.assertEqual(self.store.get('coordinator:other_host'), {'id': 'unreachable'})
            self.assertEqual(self.store.get('coordinator:other_session'), 'saved-native')
            self.assertFalse(any(row['topic'] == 'other' for row in self.problems()))

        self.service.session_factory = reattach
        for error in (FileNotFoundError, ConnectionError):
            with self.subTest(error=error.__name__):
                self.service.sessions.clear()
                with patch('coordinator.host.HostClient.stop', AsyncMock(side_effect=error('socket gone'))) as stop, \
                        self.assertLogs('coordinator.service', level='WARNING') as logs:
                    await self.run_passes(check)
                stop.assert_awaited_once_with()
                self.assertEqual(logs.output, ['WARNING:coordinator.service:parent stop failed topic=other '
                                               'reason=disabled error=' + error.__name__])

    async def test_D8_disabled_home_leftover_host_is_tracked_after_restart(self):
        with self.store.db:
            self.store.db.execute('UPDATE topics SET enabled=0 WHERE id=?', (TOPIC,))
            self.store.put('coordinator_session', 'home-native')
            self.store.put('coordinator_host', {'id': 'home-host'})
        host = FakeSession('home-native')
        host.idle = lambda: True
        self.sessions.append(host)
        attached = []

        async def reattach(store, runner, cwd, model, instructions, topic=None):
            attached.append(topic)
            return host

        async def check():
            for _ in range(3):
                await self.drain_feed()
            self.assertFalse(host.closed, 'home parent stopped before PARENT_IDLE')
            with patch('coordinator.service.PARENT_IDLE', 0):
                for _ in range(3):
                    await self.drain_feed()

        self.service.session_factory = reattach
        await self.run_passes(check)
        self.assertEqual(attached, [TOPIC])
        self.assertIs(self.service.sessions.get(TOPIC), host)
        self.assertTrue(host.closed, 'disabled home parent host left running and untracked after restart')
        self.assertEqual(self.store.get('coordinator_session'), 'home-native')

    async def test_D9_worker_result_with_disabled_home_and_disabled_chat(self):
        self.other_topic(False)
        with self.store.db:
            self.store.db.execute('UPDATE topics SET enabled=0 WHERE id=?', (TOPIC,))
        worker = self.worker(topic='other')
        with self.store.db:
            self.store.db.execute("UPDATE workers SET status='running' WHERE id=?", (worker,))
        self.store.worker_complete(worker, {'success': True, 'text': 'done'})
        for _ in range(3):
            await self.drain_feed()
        result, = self.store.db.execute('SELECT * FROM messages WHERE source_worker=?', (worker,))
        self.assertEqual(result['delivered'], 'received')
        self.assertNotIn('other', self.service.sessions)
        self.assertTrue(any('topic=other ' in text and 'kind=worker_result' in text
                            for _, text in self.service.sessions[TOPIC].sent))

    async def test_D8_home_exemption_follows_current_home_after_move(self):
        from coordinator import control_api
        self.other_topic()
        with self.store.db:
            result = control_api.call(self.store, 'topic.home', {'topic': 'other'})
            self.store.db.execute('UPDATE topics SET enabled=0')
        self.assertTrue(result.ok, result.text)
        self.assertEqual(self.store.get('coordinator_session_topic'), TOPIC)
        self.assertEqual(self.service.home_topic(), 'other')
        self.assertFalse(parent_state.topic_enabled(self.store, 'coordinator'))
        self.assertFalse(parent_state.topic_enabled(self.store, 'coordinator:' + TOPIC))
        self.assertTrue(parent_state.topic_enabled(self.store, 'coordinator:other'))
        self.assertIsNone(await self.service.start_session(TOPIC))
        self.service.parent_retry['other'] = 0
        await self.service.feed_once()
        home = self.service.sessions['other']
        home.idle = lambda: True
        home.relaunch = Mock()
        self.store.put('parent_selection', {'provider': 'codex', 'model': 'changed'})
        with patch('coordinator.service.parent_state.selection_changed', return_value=True):
            await self.service.feed_once()
        home.relaunch.assert_called_once_with()
        self.assertFalse(home.closed)
        self.store.put('parent_selection', None)
        worker = self.worker(topic='other')
        self.store.worker_complete(worker, {'success': True, 'text': 'done'})
        await self.drain_feed()
        row, = self.store.db.execute('SELECT * FROM messages WHERE source_worker=?', (worker,))
        self.assertEqual(row['delivered'], 'received')
        self.assertTrue(any(identity == message_uuid(row['id']) for identity, _ in home.sent))

    async def test_D2_uncertain_disabled_host_state_stops_instead_of_waiting_forever(self):
        self.other_topic()
        session = await self.service.start_session('other')
        session._recovery_uncertain = True
        session.idle = lambda: False
        self.disable()
        with self.assertLogs('coordinator.service', level='INFO') as logs:
            await self.service.feed_once()
        self.assertTrue(session.closed)
        self.assertEqual(logs.output, ['INFO:coordinator.service:parent stopped topic=other reason=disabled state=unavailable'])

    async def test_D4_disabled_closed_crashed_selection_retry_and_pending_never_start(self):
        self.other_topic()
        session = await self.service.start_session('other')
        session.idle = lambda: True
        session.relaunch = Mock()
        self.disable()
        self.store.put('parent_selection', {'provider': 'codex', 'model': 'changed'})
        with patch('coordinator.service.parent_state.selection_changed', return_value=True):
            await self.service.feed_once()
        session.relaunch.assert_not_called()
        self.assertTrue(session.closed)
        self.service.wound_down.clear()
        original = self.service.session_factory

        async def start(*args, **kwargs):
            current = await original(*args, **kwargs)
            current.idle = lambda: True
            return current

        factory = AsyncMock(side_effect=start)
        self.service.session_factory = factory
        for state in ('closed', 'crashed', 'retry'):
            with self.subTest(state=state):
                if state == 'retry':
                    self.service.sessions.pop('other')
                else:
                    session.closed = True
                    session.rejected = state == 'crashed'
                self.service.parent_retry['other'] = 0
                for kind in ('owner', 'restarted', 'callback', 'secret_filled', 'event'):
                    self.store.message_save('other', kind, 'pending')
                await self.drain_feed()
                self.assertIsNone(await self.service.start_session('other'))
        factory.assert_not_awaited()

    async def test_D4_disable_during_launch_preparation_does_not_create_a_host(self):
        from coordinator import parent
        from coordinator.providers import ProviderRunner
        self.other_topic()
        self.enable_claude_account()
        self.service.session_factory = parent.start
        runner = ProviderRunner(self.store.directory, {'claude': str(self.root / 'fake-claude')})
        self.service.runner = runner

        async def disable(env):
            self.disable()
            return {}

        with patch.object(runner, '_external_writer', AsyncMock(return_value=False)), \
                patch.object(runner, 'launch_secrets', side_effect=disable), \
                patch('coordinator.host.HostClient.launch', AsyncMock()) as launch:
            self.assertIsNone(await self.service.start_session('other'))
        launch.assert_not_awaited()
        self.assertIsNone(self.store.get('coordinator:other_host'))
        self.assertEqual(self.problems(), [])

    async def enabled_observation(self):
        self.other_topic()
        self.store.put('coordinator:other_session', 'saved-native')
        self.store.put('coordinator:other_host', {'id': 'saved-host'})
        calls = []
        factory = self.service.session_factory

        async def record(*args, topic=None, **kwargs):
            calls.append(topic)
            session = await factory(*args, topic=topic, **kwargs)
            session.idle = lambda: True
            session.relaunch = Mock()
            return session

        self.service.session_factory = record
        observed = {}

        async def check():
            session = self.service.sessions['other']
            observed['startup'] = calls[:]
            await self.drain_feed()
            observed['restart'] = [text.split('] ', 1)[1] for _, text in session.sent]
            from coordinator.service import PARENT_IDLE
            now = 100000.0
            self.service.last_turn = {topic: now for topic in self.service.sessions}
            with patch('coordinator.service.time.monotonic', return_value=now + PARENT_IDLE - 1):
                await self.service.feed_once()
            observed['before_idle'] = session.closed
            with patch('coordinator.service.time.monotonic', return_value=now + PARENT_IDLE):
                await self.service.feed_once()
            observed['at_idle'] = session.closed
            observed['native'] = self.store.get('coordinator:other_session')
            self.store.message_save('other', 'owner', 'continue')
            await self.drain_feed()
            resumed = self.service.sessions['other']
            observed['wake'] = [text.split('] ', 1)[1] for _, text in resumed.sent]
            self.store.put('parent_selection', {'provider': 'codex', 'model': 'changed'})
            with patch('coordinator.service.parent_state.selection_changed', return_value=True):
                await self.service.feed_once()
            observed['selection_relaunches'] = resumed.relaunch.call_count
            resumed.closed = True
            self.service.wound_down.clear()
            await self.service.feed_once()
            observed['closed_restarts'] = calls.count('other')

        await self.run_passes(check)
        return observed

    async def test_D5_enabled_matches_origin_main_observation(self):
        observed = await self.enabled_observation()
        self.assertEqual(observed, {'at_idle': True,
         'before_idle': False,
         'closed_restarts': 3,
         'native': 'saved-native',
         'restart': ['The service restarted. Open jobs: '
                     '{"count":0,"live_workers":[],"live_workers_not_shown":0,"not_shown":0,"shown":[]}. '
                     'Messages with uncertain delivery (newest 10 at most, text cut to 500 characters): '
                     '{"count":0,"not_shown":0,"shown":[]}. Inspect current state before acting. Do not '
                     'repeat completed work. Use tasks.get for job notes, and tasks.list and workers.list '
                     'for the rest.'],
         'selection_relaunches': 1,
         'startup': ['-10042:4', 'other'],
         'wake': [REPLY_FIRST_LINE + 'continue']})

    async def test_D6_disabled_workers_finish_and_result_reaches_home(self):
        self.other_topic()
        self.enable_claude_account()
        worker = self.worker(topic='other')
        entered, release = asyncio.Event(), asyncio.Event()
        run = self.runner.run

        async def held(*args, **kwargs):
            entered.set()
            await release.wait()
            return await run(*args, **kwargs)

        with patch.object(self.runner, 'run', side_effect=held):
            await self.service.workers_once()
            await asyncio.wait_for(entered.wait(), 2)
            task = self.service.worker_tasks[worker]
            self.disable()
            await self.service.feed_once()
            await self.service.workers_once()
            self.assertIs(self.service.worker_tasks[worker], task)
            self.assertFalse(task.done())
            release.set()
            await asyncio.gather(task)
        await self.drain_feed()
        self.assertNotIn('other', self.service.sessions)
        result, = self.store.db.execute('SELECT * FROM messages WHERE source_worker=?', (worker,))
        self.assertEqual(result['delivered'], 'received')
        self.assertEqual(self.store.db.execute('SELECT status FROM workers WHERE id=?', (worker,)).fetchone()[0], 'done')
        self.assertTrue(any('topic=other ' in text and 'kind=worker_result' in text
                            for _, text in self.service.sessions[TOPIC].sent))


class DisabledNativeParentTests(ServiceTestsSupport, unittest.IsolatedAsyncioTestCase):
    other_topic = DisabledParentTests.other_topic
    disable = DisabledParentTests.disable
    run_passes = DisabledParentTests.run_passes

    async def asyncSetUp(self):
        await super().asyncSetUp()
        from tests.support import isolate_shared_mcp_home
        from tests.test_codex_parent import APP_SERVER
        from tests.test_parent_turns import FAKE
        from coordinator.providers import ProviderRunner
        isolate_shared_mcp_home(self, self.root)
        self.other_topic()
        self.enable_claude_account()
        claude, codex = self.root / 'fake-claude', self.root / 'fake-codex'
        claude.write_text(FAKE.replace("if text=='slow':", "if text.endswith('hold'):")
                          .replace("root/'slow.go'", "root/'claude-release'"))
        codex.write_text(APP_SERVER.replace(
            "assert 'policy' in params['developerInstructions'] and params['model']=='test-codex'",
            "assert params['model']=='test-codex'").replace(
            "        elif text.endswith('host-request'):",
            "            while not (root/'codex-release').exists():\n"
            "                time.sleep(.01)\n"
            "            complete()\n"
            "        elif text.endswith('host-request'):"))
        claude.chmod(0o700)
        codex.chmod(0o700)
        (self.root / 'chatgpt').mkdir()
        with self.store.db:
            self.store.put('codex_accounts', {'g': {'config_dir': str(self.root / 'chatgpt'), 'enabled': True}})
            self.store.put('codex_account_status', {'g': {'identity': {'logged_in': True, 'email': 'g@example.test'}}})
            self.store.put('codex_active_account', 'g')
            self.store.put('codex_model', 'test-codex')
        self.runner = ProviderRunner(self.store.directory, {'claude': str(claude), 'codex': str(codex)})
        self.service = Service(self.store, self.telegram, self.runner, self.root)

    async def start_native(self, provider, topic):
        from tests.support import until
        self.store.put('parent_selection', {'provider': provider,
                                           'model': 'test-codex' if provider == 'codex' else 'test-claude'})
        session = await self.service.start_session(topic)
        self.assertIsNotNone(session, self.problems())
        self.sessions.append(session)
        self.assertEqual(await session.send(str(uuid.uuid4()), 'ready'), 'received')
        await until(session.idle)
        return session

    async def assert_native_intake_flood(self, provider):
        from tests.support import until
        session = await self.start_native(provider, 'other')
        self.store.put('coordinator_intake_batching', True)
        turns = []
        session.on_turn = lambda text: turns.append((text, session._turn_rows - session._unreplayed))

        def native_inputs():
            prompts = []
            for line in session.process.written_lines():
                event = json.loads(line)
                if provider == 'claude':
                    text = (event.get('message') or {}).get('content')
                else:
                    parts = event.get('params', {}).get('input', [])
                    text = parts[0].get('text') if event.get('method') == 'turn/start' and parts else None
                if isinstance(text, str) and re.search(r'\bmessage=\d+ kind=', text):
                    prompts.append(text)
            return prompts

        held = self.store.message_save('other', 'owner', 'hold', telegram_message=4001)
        await self.service.feed_once()
        await asyncio.gather(*set(self.service.feed_tasks.values()))
        await until(lambda: session.busy and len(native_inputs()) == 1)
        workers = [self.store.message_save('other', 'worker_result' if number % 2 == 0 else 'event',
                   json.dumps({'worker': number + 1, 'task': number + 1, 'success': number not in (4, 12),
                               'text': 'result-%d' % number})) for number in range(30)]
        owner = self.store.message_save('other', 'owner', 'Answer this request.', telegram_message=4002)
        urgent = [workers[number]['id'] for number in (4, 12)]
        expected = [owner['id']] + urgent + [row['id'] for row in workers if row['id'] not in urgent]
        release = self.root / (provider + '-release')
        try:
            self.assertTrue(await self.service.feed_once())
            await until(lambda: len(native_inputs()) == 2)
            first = native_inputs()[1]
            self.assertEqual([int(row) for row in re.findall(r'\bmessage=(\d+) kind=', first)], expected[:20])
            self.assertLessEqual(len(first), 8000)
            self.assertEqual([row['id'] for row in self.store.messages_pending()],
                             [row['id'] for row in workers if row['id'] not in expected[:20]])
            self.assertEqual(turns, [])
        finally:
            release.touch()
        await asyncio.gather(*set(self.service.feed_tasks.values()))
        await until(lambda: session.idle() and len(turns) == 2)
        self.assertEqual(turns[0][1], {held['id']})
        self.assertEqual(turns[1][1], set(expected[:20]))
        self.assertEqual({tuple(row) for row in self.store.db.execute(
            'SELECT delivered,receipt FROM messages WHERE batch_id=?', (owner['id'],))}, {('received', 'replayed')})
        while await self.service.deliver_once():
            pass
        self.assertTrue(any(row['reply_to'] == 4002 for row in self.telegram.sent))
        owner_state = self.store.db.execute(
            'SELECT reaction_desired,turn_started,turn_eyes FROM messages WHERE id=?', (owner['id'],)).fetchone()
        self.assertEqual(tuple(owner_state), ('👌', None, 0))
        self.assertEqual(len(self.store.messages_pending()), 11)
        self.assertTrue(await self.service.feed_once())
        await asyncio.gather(*set(self.service.feed_tasks.values()))
        await until(lambda: session.idle() and len(turns) == 3)
        prompts = native_inputs()[1:]
        batches = [[int(row) for row in re.findall(r'\bmessage=(\d+) kind=', text)] for text in prompts]
        self.assertEqual(batches, [expected[:20], expected[20:]])
        self.assertEqual(sorted(row for batch in batches for row in batch), sorted(expected))
        self.assertEqual({tuple(row) for row in self.store.db.execute(
            'SELECT delivered,receipt FROM messages')}, {('received', 'replayed')})
        self.assertEqual(self.store.messages_pending(), [])
        self.assertTrue(all(len(text) <= 8000 for text in prompts))
        for row in workers + [owner]:
            prompt = next(text for text in prompts if 'message=%d kind=' % row['id'] in text)
            self.assertIn(row['text'], prompt)
        print('native %s flood: owner answered in next turn; batches=20,11; 31 row bodies received once' % provider)

    async def test_native_claude_flood_answers_owner_next_turn_and_receipts_all_rows(self):
        await self.assert_native_intake_flood('claude')

    async def test_native_codex_flood_answers_owner_next_turn_and_receipts_all_rows(self):
        await self.assert_native_intake_flood('codex')

    async def assert_native_restart(self, provider, mid_turn):
        from coordinator.host import HostClient
        from tests.support import until
        other = await self.start_native(provider, 'other')
        native, host = other.session_id, other.host_id
        directory = self.store.directory / 'hosts' / host
        if mid_turn:
            self.store.message_save('other', 'owner', 'hold')
            await self.service.feed_once()
            await asyncio.gather(*self.service.feed_tasks.values())
            await until(lambda: other.busy)
        self.disable()
        home = await self.start_native(provider, TOPIC)
        for session in (home, other):
            await session.detach()
            self.sessions.remove(session)
        self.assertEqual(HostClient(directory).state, 'running')
        self.service = Service(self.store, self.telegram, self.runner, self.root)
        notices = self.store.db.execute("SELECT count(*) FROM messages WHERE topic='other' AND kind='restarted'").fetchone()[0]

        async def check():
            self.sessions.extend(self.service.sessions.values())
            prior_problems = self.problems()
            attached = self.service.sessions.get('other')
            self.assertIsNotNone(attached, 'startup abandoned the disabled native host')
            self.assertEqual(attached.host_id, host)
            self.assertEqual(self.service.sessions[TOPIC].host_id, home.host_id)
            if mid_turn:
                replies = []

                def reply(text):
                    replies.append(text)
                    self.store.enqueue_report('other', text)

                attached.on_turn = reply
                for _ in range(3):
                    await self.service.feed_once()
                    await asyncio.sleep(0)
                self.assertFalse(attached.closed)
                (self.root / (provider + '-release')).touch()
                await until(lambda: bool(replies) and attached.idle())
                while await self.service.deliver_once():
                    pass
                self.assertEqual([row['text'] for row in self.telegram.sent if row['topic'] == 'other'], replies)
            else:
                await until(attached.idle)
            with self.assertLogs('coordinator.service', level='INFO') as logs:
                await self.service.feed_once()
                await self.service.feed_once()
            self.assertTrue(attached.closed)
            self.assertEqual(HostClient(directory).state, 'exited')
            self.assertEqual(sum('parent stopped topic=other reason=disabled' in line for line in logs.output), 1)
            self.assertEqual(self.store.get('coordinator:other_session'), native)
            self.assertIsNone(self.store.get('coordinator:other_host'))
            self.assertFalse(self.service.sessions[TOPIC].closed)
            self.assertEqual(self.store.db.execute("SELECT count(*) FROM messages WHERE topic='other' AND kind='restarted'").fetchone()[0], notices)
            self.assertEqual(self.problems(), prior_problems)

        await self.run_passes(check)

    async def test_D1_claude_native_host_is_recovered_and_stopped(self):
        await self.assert_native_restart('claude', False)

    async def test_D1_codex_native_host_is_recovered_and_stopped(self):
        await self.assert_native_restart('codex', False)

    async def test_D3_claude_restart_finishes_turn_and_delivers_reply_before_stop(self):
        await self.assert_native_restart('claude', True)

    async def test_D3_codex_restart_finishes_turn_and_delivers_reply_before_stop(self):
        await self.assert_native_restart('codex', True)

    async def assert_stop_before_first_input(self, restart):
        from tests.support import until
        session = await self.service.start_session('other')
        self.sessions.append(session)
        await until(lambda: session.turn_open)
        self.assertFalse(session.idle())
        native = session.session_id
        self.disable()
        if restart:
            await session.detach()
            self.sessions.remove(session)
            self.service = Service(self.store, self.telegram, self.runner, self.root)
            session = await self.service.start_session('other', reattach_only=True)
            self.sessions.append(session)
            await until(lambda: not session._unread_reattach_events)
        await self.service.feed_once()
        self.assertTrue(session.closed, 'initialization without input kept a disabled parent alive')
        self.assertEqual(self.store.get('coordinator:other_session'), native)
        self.assertEqual(self.problems(), [])
        self.assertEqual(list(self.store.db.execute('SELECT * FROM outbox')), [])

    async def test_D2_claude_initialization_without_input_stops_when_disabled(self):
        await self.assert_stop_before_first_input(False)

    async def test_D1_claude_initialization_without_input_stops_after_restart(self):
        await self.assert_stop_before_first_input(True)

    async def assert_enable_resumes(self, provider):
        from coordinator import control_api, parent
        from tests.support import until
        session = await self.start_native(provider, 'other')
        native = session.session_id
        self.disable()
        session.relaunch()
        self.assertFalse(session.rejected, 'disabled native parent still accepts a relaunch')
        await self.service.feed_once()
        self.assertTrue(session.closed)
        with patch('coordinator.parent.choose_parent', AsyncMock(side_effect=AssertionError('no account selection'))), \
                patch('coordinator.parent_state.launch', AsyncMock(side_effect=AssertionError('no new host'))):
            self.assertIsNone(await self.service.start_session('other'))
            self.assertIsNone(await parent.start(self.store, self.runner, self.root, 'model', 'policy', topic='other'))
            self.store.put('coordinator:other_host', {'id': session.host_id, 'account': session.account_alias,
                                                     'model': session.model, 'provider': provider})
            recovered = await parent.start(self.store, self.runner, self.root, 'model', 'policy', topic='other')
            self.assertEqual(recovered.host_id, session.host_id)
            await recovered.stop()
        with self.store.db:
            result = control_api.call(self.store, 'topic.enable', {'topic': 'other'})
        self.assertTrue(result.ok, result.text)
        self.store.message_save('other', 'owner', 'continue')
        await self.drain_feed()
        resumed = self.service.sessions['other']
        self.sessions.append(resumed)
        await until(resumed.idle)
        self.assertEqual(resumed.session_id, native)
        self.assertNotEqual(resumed.host_id, session.host_id)
        self.assertNotEqual(resumed.process.child_pid, session.process.child_pid)
        self.assertEqual(self.store.db.execute("SELECT delivered FROM messages WHERE topic='other' AND kind='owner'").fetchone()[0], 'received')

    async def test_D4_D7_claude_blocks_relaunch_then_enable_resumes_native_session(self):
        await self.assert_enable_resumes('claude')

    async def test_D4_D7_codex_blocks_relaunch_then_enable_resumes_native_session(self):
        await self.assert_enable_resumes('codex')


class NativeServiceTests(ServiceTestsSupport, unittest.IsolatedAsyncioTestCase):
    async def test_restart_finishes_when_extension_close_never_returns(self):
        cancelled = asyncio.Event()

        async def stuck():
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        async def restart():
            self.service.restart_reason = 'upgrade'
            self.service.exit_for_restart()

        session = SimpleNamespace(detach=AsyncMock(), stop=AsyncMock())
        self.service.sessions[TOPIC] = session
        self.service.pair_only = True
        self.service.on_restart = Mock()
        self.service.supervised = lambda: [('restart', restart)]
        with patch('coordinator.service.SHUTDOWN_SECONDS', .02), \
                patch.object(self.service.extension, 'start', AsyncMock()), \
                patch.object(self.service.extension, 'close', AsyncMock(side_effect=stuck)) as close, \
                self.assertLogs('coordinator.service', level='ERROR') as logs:
            await asyncio.wait_for(self.service.run(), .5)
        self.assertTrue(cancelled.is_set())
        self.service.on_restart.assert_called_once_with()
        close.assert_awaited_once_with()
        session.detach.assert_awaited_once_with()
        session.stop.assert_not_awaited()
        self.assertEqual(self.store.get('service_run')['cause'], 'restart reason=upgrade')
        self.assertTrue(any('service shutdown timed out' in line for line in logs.output))

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


ORIGIN_MAIN_CHAT_CREATE = {'topics': [{'id': '-10042:10',
             'chat': -10042,
             'thread': 10,
             'name': 'Torii',
             'telegram_title': None,
             'cwd': '@ROOT@',
             'provider': None,
             'session': None,
             'enabled': 1,
             'waiting_job': None,
             'source_pid': None},
            {'id': '-10042:11',
             'chat': -10042,
             'thread': 11,
             'name': 'Baseline',
             'telegram_title': 'Baseline',
             'cwd': '@ROOT@/projects/baseline',
             'provider': None,
             'session': None,
             'enabled': 1,
             'waiting_job': None,
             'source_pid': None}],
 'settings': [['accounts', '{}'],
              ['bot_id', '99'],
              ['bot_username', '"test_bot"'],
              ['claude_parent_effort', '"max"'],
              ['codex_parent_effort', '"max"'],
              ['control_topic', '"-10042:10"'],
              ['coordinator_home_topic', '"-10042:10"'],
              ['coordinator_model', '"claude-opus-5-5"'],
              ['execution', '"agents"'],
              ['forced_pair_only', 'false'],
              ['group', '-10042'],
              ['mode', '"group"'],
              ['owner', '7'],
              ['pair_only', 'false'],
              ['projects_root', '"@ROOT@/projects"']],
 'params': {'name': 'Baseline', 'settings': {}, '_topic': '-10042:10', '_chat': -10042},
 'progress': {'phase': 'finished',
              'cwd': '@ROOT@/projects/baseline',
              'owned': True,
              'thread': 11,
              'topic': '-10042:11',
              'text': 'Chat created: topic=-10042:11\nName: Baseline\nFolder: @ROOT@/projects/baseline'},
 'outbox': [{'topic': '-10042:11',
             'text': 'Baseline is ready.\n'
                     'Project folder: @ROOT@/projects/baseline\n'
                     'Chat agent: Claude claude-opus-5-5 · max thinking · default claude-opus-5-5.',
             'kind': 'report'},
            {'topic': '-10042:10',
             'text': 'Chat created: topic=-10042:11\nName: Baseline\nFolder: @ROOT@/projects/baseline',
             'kind': 'report'}],
 'calls': [['createForumTopic', {'chat_id': -10042, 'name': 'Baseline'}]]}


ORIGIN_MAIN_VALID_CHAT_CREATE = {'reply': {'ok': True,
           'text': 'Chat creation queued. Torii will send the result here.',
           'data': {'request': 1},
           'state': 'queued'},
 'queued': {'op': 'topic.create',
            'params': '{"provider": "codex", "model": "gpt-6-luna", "name": "Valid pick", "settings": {}, '
                      '"_topic": "-10042:10", "_chat": -10042}',
            'state': 'queued',
            'result': None},
 'topics': [{'id': '-10042:10',
             'chat': -10042,
             'thread': 10,
             'name': 'Torii',
             'telegram_title': None,
             'cwd': '@ROOT@',
             'provider': None,
             'session': None,
             'enabled': 1,
             'waiting_job': None,
             'source_pid': None},
            {'id': '-10042:11',
             'chat': -10042,
             'thread': 11,
             'name': 'Valid pick',
             'telegram_title': 'Valid pick',
             'cwd': '@ROOT@/projects/valid-pick',
             'provider': None,
             'session': None,
             'enabled': 1,
             'waiting_job': None,
             'source_pid': None}],
 'settings': [['accounts', '{}'],
              ['bot_id', '99'],
              ['bot_username', '"test_bot"'],
              ['claude_parent_effort', '"max"'],
              ['codex_parent_effort', '"max"'],
              ['control_topic', '"-10042:10"'],
              ['coordinator:-10042:11_selection',
               '{"provider": "codex", "model": "gpt-6-luna", "effort": null, "revision": 1}'],
              ['coordinator_home_topic', '"-10042:10"'],
              ['coordinator_model', '"claude-opus-5-5"'],
              ['execution', '"agents"'],
              ['forced_pair_only', 'false'],
              ['group', '-10042'],
              ['mode', '"group"'],
              ['owner', '7'],
              ['pair_only', 'false'],
              ['projects_root', '"@ROOT@/projects"']],
 'params': {'provider': 'codex',
            'model': 'gpt-6-luna',
            'name': 'Valid pick',
            'settings': {},
            '_topic': '-10042:10',
            '_chat': -10042},
 'progress': {'phase': 'finished',
              'cwd': '@ROOT@/projects/valid-pick',
              'owned': True,
              'thread': 11,
              'topic': '-10042:11',
              'text': 'Chat created: topic=-10042:11\n'
                      'Name: Valid pick\n'
                      'Folder: @ROOT@/projects/valid-pick\n'
                      'Chat agent: ChatGPT gpt-6-luna · xhigh thinking · picked for this chat.'},
 'outbox': [{'topic': '-10042:11',
             'text': 'Valid pick is ready.\n'
                     'Project folder: @ROOT@/projects/valid-pick\n'
                     'Chat agent: ChatGPT gpt-6-luna · xhigh thinking · picked for this chat.',
             'kind': 'report'},
            {'topic': '-10042:10',
             'text': 'Chat created: topic=-10042:11\n'
                     'Name: Valid pick\n'
                     'Folder: @ROOT@/projects/valid-pick\n'
                     'Chat agent: ChatGPT gpt-6-luna · xhigh thinking · picked for this chat.',
             'kind': 'report'}],
 'calls': [['createForumTopic', {'chat_id': -10042, 'name': 'Valid pick'}]]}

class TopicCreationTests(unittest.IsolatedAsyncioTestCase):
    async def test_create_delegation_refuses_surrogates_and_controls_before_queue_on_cli_and_mcp(self):
        from argparse import Namespace
        from contextlib import redirect_stdout
        from io import StringIO
        from coordinator import mcp
        from coordinator.__main__ import ctl
        from tests.test_group_setup import HOME
        owner = self.store.message_save(HOME, 'owner', 'Create a delegated chat.')['id']
        cases = [('\ud800', 'surrogate characters'), ('\udfff', 'surrogate characters')]
        cases += [(chr(code), 'control characters other than newline and tab')
                  for code in (*range(32), *range(127, 160)) if code not in (9, 10)]
        for source in ('cli', 'mcp'):
            for character, character_class in cases:
                for text in ('Use Sol.' + character + 'Use Claude.', character + 'Use Sol.' + character):
                    with self.subTest(source=source, text=repr(text)):
                        before = {table: [tuple(row) for row in self.store.db.execute('SELECT * FROM ' + table + ' ORDER BY 1')]
                                  for table in ('settings', 'topics', 'outbox', 'service_requests')}
                        changes = self.store.db.total_changes
                        fields = dict(name='Refused', delegation=text, message=owner)
                        refusal = 'Chat delegation instructions cannot contain ' + character_class + '.'
                        if source == 'cli':
                            output = StringIO()
                            args = Namespace(operation='call', op='topic.create', topic=HOME,
                                             pairs=[key + '=' + str(value) for key, value in fields.items()])
                            with patch('coordinator.__main__.service_running', return_value=True), redirect_stdout(output):
                                status = ctl(args, self.store)
                            result = json.loads(output.getvalue())
                            self.assertEqual((status, result['ok'], result['state'], result['text']),
                                             (3, False, 'refused', refusal))
                        else:
                            response = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
                                'name': 'topic_create', 'arguments': fields}}, topic=HOME)
                            self.assertTrue(response['isError'], response)
                            self.assertEqual(response['content'][0]['text'], refusal + '\nnull')
                        await self.service.controls_once()
                        after = {table: [tuple(row) for row in self.store.db.execute('SELECT * FROM ' + table + ' ORDER BY 1')]
                                 for table in before}
                        self.assertEqual(after, before)
                        self.assertEqual(self.store.db.total_changes, changes + (source == 'cli'))
                        if source == 'cli':
                            self.assertEqual(tuple(self.store.db.execute(
                                'SELECT op,state FROM cli_calls ORDER BY id DESC LIMIT 1').fetchone()),
                                             ('topic.create', 'refused'))
                        self.assertEqual(list(self.projects.iterdir()), [])
                        self.assertEqual(self.telegram.calls, [])

    async def test_create_delegation_accepts_newline_and_tab_on_cli_and_mcp(self):
        from argparse import Namespace
        from contextlib import redirect_stdout
        from io import StringIO
        from coordinator import mcp
        from coordinator.__main__ import ctl
        from tests.test_group_setup import HOME
        owner = self.store.message_save(HOME, 'owner', 'Create delegated chats.')['id']
        for source in ('cli', 'mcp'):
            text = 'Use Sol.\n\tUse Claude for UI. ' + source
            fields = dict(name='Whitespace ' + source, delegation=text, message=owner)
            if source == 'cli':
                output = StringIO()
                args = Namespace(operation='call', op='topic.create', topic=HOME,
                                 pairs=[key + '=' + str(value) for key, value in fields.items()])
                with patch('coordinator.__main__.service_running', return_value=True), redirect_stdout(output):
                    status = ctl(args, self.store)
                self.assertEqual(status, 0, output.getvalue())
                number = json.loads(output.getvalue())['data']['request']
            else:
                response = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
                    'name': 'topic_create', 'arguments': fields}}, topic=HOME)
                self.assertFalse(response['isError'], response)
                number = json.loads(response['content'][0]['text'].split('\n', 1)[1])['request']
            self.assertEqual(json.loads(self.request(number)['params'])['delegation'], text)
            self.assertEqual((await self.finish(number))['state'], 'done')
            target = json.loads(self.request(number)['result'])['topic']
            self.assertEqual(self.store.get('coordinator:' + target + '_delegation'), {'text': text, 'revision': 1})

    async def test_d9_delegation_link_atomic_first_launch_ready_and_event(self):
        from coordinator.policy import chat_policy
        number = self.create('Delegated', delegation='  Use Sol.  ')
        await self.service.controls_once()
        await self.service.controls_once()
        original = self.store.link_created_topic
        own = {'text': 'Use Sol.', 'revision': 1}
        def linked(*args, **kwargs):
            original(*args, **kwargs)
            observer = Store(self.root / 'state')
            try:
                self.assertTrue(observer.topic('-10042:11')['enabled'])
                self.assertEqual(observer.get('coordinator:-10042:11_delegation'), own)
                saved = observer.db.execute('SELECT result FROM service_requests WHERE id=?', (number,)).fetchone()
                self.assertEqual(json.loads(saved['result'])['phase'], 'linked')
            finally:
                observer.close()
        trace = []
        self.store.db.set_trace_callback(trace.append)
        try:
            with patch.object(self.store, 'link_created_topic', side_effect=linked):
                await self.service.controls_once()
        finally:
            self.store.db.set_trace_callback(None)
        self.assertEqual(trace.count('COMMIT'), 1)
        self.assertEqual([sql for sql in trace if sql.startswith('BEGIN')], ['BEGIN '])
        self.assertEqual((await self.finish(number))['state'], 'done')
        target = '-10042:11'
        self.factory.return_value.key = 'coordinator:' + target
        session = await self.service.start_session(target)
        self.assertEqual(session.usage_policy, chat_policy(self.store, 'coordinator:' + target))
        folder = str(self.projects / 'delegated')
        agent = 'Chat agent: Claude claude-opus-5-5 · max thinking · default.'
        ready = 'Delegated is ready.\nProject folder: ' + folder + '\n' + agent
        ready += '\nDelegation updated.'
        event = 'Chat created: topic=' + target + '\nName: Delegated\nFolder: ' + folder + '\n' + agent
        event += '\nDelegation: own instructions (8 characters).'
        self.assertEqual([row[0] for row in self.store.db.execute('SELECT text FROM outbox WHERE topic=?', (target,))], [ready])
        self.assertEqual(self.events()[-1]['text'], event)
        self.assertEqual(self.store.db.execute('SELECT text FROM outbox ORDER BY id DESC LIMIT 1').fetchone()[0], event)

    async def test_d10_delegation_cap_before_effects_and_restart_each_phase(self):
        from coordinator.control_api import call
        from tests.test_group_setup import HOME
        for text in ('x' * 2001, '   '):
            result = call(self.store, 'topic.create', {'name': 'Refused', 'delegation': text}, topic=HOME)
            self.assertFalse(result.ok, result.text)
            self.assertEqual(result.text, 'Keep chat delegation instructions to 2000 characters or fewer (this text has 2001).'
                             if len(text) > 2000 else 'Send text of 1 to 16000 characters for delegation.')
            self.assertEqual(list(self.projects.iterdir()), [])
            self.assertEqual(self.telegram.calls, [])
            self.assertEqual(self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0], 0)
        for checkpoint in range(5):
            name = 'Delegation restart ' + str(checkpoint)
            number = self.create(name, delegation='Use Sol.')
            for unused in range(checkpoint):
                await self.service.controls_once()
            self.restart()
            self.assertEqual((await self.finish(number))['state'], 'done')
            target = json.loads(self.request(number)['result'])['topic']
            self.assertEqual(self.store.get('coordinator:' + target + '_delegation'), {'text': 'Use Sol.', 'revision': 1})
            self.assertEqual(len([entry for entry in self.created() if entry['name'] == name]), 1)
            self.assertEqual(len([path for path in self.projects.iterdir() if path.name == 'delegation-restart-' + str(checkpoint)]), 1)
            self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox WHERE topic=?', (target,)).fetchone()[0], 1)

    async def test_d11_old_queued_create_without_delegation_finishes_unchanged(self):
        number = self.create('Old shape')
        self.assertNotIn('delegation', json.loads(self.request(number)['params']))
        self.restart()
        self.assertEqual((await self.finish(number))['state'], 'done')
        self.assertIsNone(self.store.get('coordinator:-10042:11_delegation'))
        agent = 'Chat agent: Claude claude-opus-5-5 · max thinking · default.'
        ready = 'Old shape is ready.\nProject folder: ' + str(self.projects / 'old-shape') + '\n' + agent
        self.assertEqual([row[0] for row in self.store.db.execute("SELECT text FROM outbox WHERE topic='-10042:11'")], [ready])
        self.assertEqual(self.events()[-1]['text'], 'Chat created: topic=-10042:11\nName: Old shape\nFolder: ' +
                         str(self.projects / 'old-shape') + '\n' + agent)

    async def test_f2d_cli_and_mcp_mismatches_refuse_before_queue_folder_or_telegram(self):
        from contextlib import redirect_stdout
        from io import StringIO
        from coordinator import mcp
        from coordinator.__main__ import ctl
        from tests.test_group_setup import HOME
        owner = self.store.message_save(HOME, 'owner', 'Create a picked chat.')['id']
        cases = [('claude', 'gpt-6-luna', 'Send a Claude model with provider=claude, for example claude-opus-5-5.'),
                 ('codex', 'claude-opus-5-5', 'Send a ChatGPT model with provider=codex, for example gpt-6.1-sol.'),
                 ('codex', 'opus', 'Send a ChatGPT model with provider=codex, for example gpt-6.1-sol.')]
        for source in ('cli', 'mcp'):
            for provider, model, text in cases:
                with self.subTest(source=source, provider=provider, model=model):
                    before = {table: [tuple(row) for row in self.store.db.execute('SELECT * FROM ' + table + ' ORDER BY 1')]
                              for table in ('settings', 'topics', 'outbox', 'service_requests')}
                    fields = dict(name='Invalid', provider=provider, model=model, message=owner)
                    if source == 'cli':
                        output = StringIO()
                        args = SimpleNamespace(operation='call', op='topic.create', topic=HOME,
                                               pairs=[key + '=' + str(value) for key, value in fields.items()])
                        with patch('coordinator.__main__.service_running', return_value=True), redirect_stdout(output):
                            status = ctl(args, self.store)
                        result = json.loads(output.getvalue())
                        self.assertEqual((status, result['ok'], result['state'], result['text']),
                                         (3, False, 'refused', text))
                    else:
                        response = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
                            'name': 'topic_create', 'arguments': fields}}, topic=HOME)
                        self.assertTrue(response['isError'], response)
                        self.assertEqual(response['content'][0]['text'], text + '\nnull')
                    await self.service.controls_once()
                    after = {table: [tuple(row) for row in self.store.db.execute('SELECT * FROM ' + table + ' ORDER BY 1')]
                             for table in before}
                    self.assertEqual(after, before)
                    self.assertEqual(list(self.projects.iterdir()), [])
                    self.assertEqual(self.telegram.calls, [])

    async def test_f2e_valid_create_matches_computed_origin_main(self):
        from coordinator.control_api import call
        from tests.test_group_setup import HOME
        result = call(self.store, 'topic.create', dict(provider='codex', model='gpt-6-luna', name='Valid pick'), topic=HOME)
        reply = dict(ok=result.ok, text=result.text, data=result.data, state=result.state)
        self.assertEqual(reply, ORIGIN_MAIN_VALID_CHAT_CREATE['reply'])
        number = result.data['request']
        request = self.request(number)
        queued = {field: request[field] for field in ('op', 'params', 'state', 'result')}
        self.assertEqual((await self.finish(number))['state'], 'done')
        actual = dict(reply=reply, queued=queued, topics=self.store.topics(),
                      settings=[list(row) for row in self.store.db.execute('SELECT key,value FROM settings ORDER BY key')],
                      params=json.loads(self.request(number)['params']),
                      progress=json.loads(self.request(number)['result']),
                      outbox=[{key: row[key] for key in ('topic', 'text', 'kind')}
                              for row in self.store.db.execute('SELECT * FROM outbox ORDER BY id')],
                      calls=self.telegram.calls)
        actual['progress'].pop('identity')
        actual = json.loads(json.dumps(actual).replace(str(self.root), '@ROOT@'))
        self.assertEqual(actual, ORIGIN_MAIN_VALID_CHAT_CREATE)

    async def test_t1_create_pick_is_visible_with_link_in_one_transaction(self):
        number = self.create('Atomic pick', provider='codex', model='gpt-6-luna', effort='low')
        await self.service.controls_once()
        await self.service.controls_once()
        trace = []
        original = self.store.link_created_topic
        selection = dict(provider='codex', model='gpt-6-luna', effort='low', revision=1)

        def linked(*args, **kwargs):
            original(*args, **kwargs)
            observer = Store(self.root / 'state')
            try:
                self.assertTrue(observer.topic('-10042:11')['enabled'])
                self.assertEqual(observer.get('coordinator:-10042:11_selection'), selection)
            finally:
                observer.close()

        self.store.db.set_trace_callback(trace.append)
        try:
            with patch.object(self.store, 'link_created_topic', side_effect=linked):
                await self.service.controls_once()
        finally:
            self.store.db.set_trace_callback(None)
        self.assertEqual([sql for sql in trace if sql.startswith('BEGIN')], ['BEGIN '])
        self.assertEqual(trace.count('COMMIT'), 1)
        self.assertEqual(self.store.get('coordinator:-10042:11_selection'), selection)
        self.assertEqual(json.loads(self.request(number)['result'])['phase'], 'linked')

    async def test_t1_pick_sql_failure_rolls_back_topic_and_settings(self):
        number = self.create('Pick rollback', provider='codex', effort='low')
        await self.service.controls_once()
        await self.service.controls_once()
        with self.store.db:
            self.store.db.execute("""CREATE TRIGGER fail_pick BEFORE INSERT ON settings
                WHEN new.key='coordinator:-10042:11_selection'
                BEGIN SELECT RAISE(ABORT,'fixture pick save failure'); END""")
        self.assertEqual((await self.finish(number))['state'], 'failed')
        self.assertIsNone(self.store.topic('-10042:11'))
        self.assertIsNone(self.store.get('coordinator:-10042:11_selection'))

    async def test_t3_exact_ready_and_result_agent_lines(self):
        cases = [({}, 'Claude claude-opus-5-5 · max thinking · default'),
                 (dict(provider='claude', model='claude-sonnet-5', effort='high'),
                  'Claude claude-sonnet-5 · high thinking · picked for this chat'),
                 (dict(provider='codex', model='gpt-6-luna', effort='low'),
                  'ChatGPT gpt-6-luna · low thinking · picked for this chat')]
        for index, (pick, label) in enumerate(cases):
            name = 'Agent ' + str(index)
            number = self.create(name, **pick)
            self.assertEqual((await self.finish(number))['state'], 'done')
            target = '-10042:' + str(11 + index)
            folder = str(self.projects / ('agent-' + str(index)))
            ready = name + ' is ready.\nProject folder: ' + folder + '\nChat agent: ' + label + '.'
            event = 'Chat created: topic=' + target + '\nName: ' + name + '\nFolder: ' + folder + '\nChat agent: ' + label + '.'
            self.assertEqual([row[0] for row in self.store.db.execute('SELECT text FROM outbox WHERE topic=?', (target,))],
                             [ready])
            self.assertEqual(self.events()[-1]['text'], event)
            self.assertEqual(self.store.db.execute('SELECT text FROM outbox ORDER BY id DESC LIMIT 1').fetchone()[0], event)

    async def test_t4_no_pick_matches_computed_origin_main(self):
        number = self.create('Baseline')
        await self.finish(number)
        actual = dict(topics=self.store.topics(),
                      settings=[list(row) for row in self.store.db.execute('SELECT key,value FROM settings ORDER BY key')],
                      params=json.loads(self.request(number)['params']),
                      progress=json.loads(self.request(number)['result']),
                      outbox=[{key: row[key] for key in ('topic', 'text', 'kind')}
                              for row in self.store.db.execute('SELECT * FROM outbox ORDER BY id')],
                      calls=self.telegram.calls)
        actual['progress'].pop('identity')
        actual = json.loads(json.dumps(actual).replace(str(self.root), '@ROOT@'))
        expected = json.loads(json.dumps(ORIGIN_MAIN_CHAT_CREATE))
        agent = 'Chat agent: Claude claude-opus-5-5 · max thinking · default.'
        expected['outbox'][0]['text'] = expected['outbox'][0]['text'].replace('default claude-opus-5-5.', 'default.')
        expected['outbox'][1]['text'] += '\n' + agent
        expected['progress']['text'] += '\n' + agent
        self.assertEqual(actual, expected)
        self.assertFalse(any(key.endswith('_selection') for key, value in actual['settings']))

    async def test_t5_invalid_picks_refuse_before_queue_folder_or_telegram(self):
        from coordinator.control_api import call
        from tests.test_group_setup import HOME
        owner = self.store.message_save(HOME, 'owner', 'Create a picked chat.')['id']
        for source in ('cli', 'mcp'):
            for pick in (dict(model='gpt-6-luna'), dict(effort='low'), dict(provider='opencode'),
                         dict(provider='unknown'), dict(provider='codex', model='bad model'),
                         dict(provider='claude', effort='xhigh'), dict(provider='codex', effort=4)):
                with self.subTest(source=source, pick=pick):
                    before = list(self.store.db.execute('SELECT * FROM settings ORDER BY key'))
                    result = call(self.store, 'topic.create', dict(pick, name='Invalid', message=owner),
                                  source=source, topic=HOME)
                    self.assertFalse(result.ok, result.text)
                    self.assertEqual(list(self.store.db.execute('SELECT * FROM settings ORDER BY key')), before)
                    self.assertEqual(self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0], 0)
                    self.assertEqual(list(self.projects.iterdir()), [])
                    self.assertEqual(self.telegram.calls, [])

    async def test_t6_restart_each_create_step_keeps_one_pick_folder_and_topic(self):
        for checkpoint in range(5):
            name = 'Picked restart ' + str(checkpoint)
            number = self.create(name, provider='codex', model='gpt-6-luna', effort='low')
            for unused in range(checkpoint):
                await self.service.controls_once()
            self.restart()
            self.assertEqual((await self.finish(number))['state'], 'done')
            target = '-10042:' + str(11 + checkpoint)
            self.assertEqual(self.store.get('coordinator:' + target + '_selection'),
                             dict(provider='codex', model='gpt-6-luna', effort='low', revision=1))
            self.assertEqual(len([row for row in self.created() if row['name'] == name]), 1)
            self.assertEqual(len([path for path in self.projects.iterdir() if path.name == 'picked-restart-' + str(checkpoint)]), 1)
            writes = []
            self.store.db.set_trace_callback(writes.append)
            try:
                await self.service.controls_once()
            finally:
                self.store.db.set_trace_callback(None)
            self.assertFalse(any('INSERT' in sql or 'UPDATE settings' in sql for sql in writes), writes)

    async def test_t7_old_queued_create_finishes_with_no_pick(self):
        from tests.test_group_setup import HOME
        params = dict(ORIGIN_MAIN_CHAT_CREATE['params'])
        params['_topic'] = HOME
        number = self.store.service_request('topic.create', params)
        self.restart()
        self.assertEqual((await self.finish(number))['state'], 'done')
        self.assertIsNone(self.store.get('coordinator:-10042:11_selection'))
        self.assertFalse(parent_state.effective_agent(self.store, 'coordinator:-10042:11')['picked'])

    async def test_t7_queued_pick_stays_out_of_settings_until_link_for_rollback(self):
        number = self.create('Rollback compatible', provider='codex', model='gpt-6-luna', effort='low')
        params = json.loads(self.request(number)['params'])
        self.assertEqual(params['settings'], {})
        self.assertEqual({key: params[key] for key in ('provider', 'model', 'effort')},
                         dict(provider='codex', model='gpt-6-luna', effort='low'))
        self.restart()
        self.assertEqual((await self.finish(number))['state'], 'done')
        self.assertEqual(self.store.get('coordinator:-10042:11_selection'),
                         dict(provider='codex', model='gpt-6-luna', effort='low', revision=1))

    async def test_created_chat_has_no_own_pick_and_starts_on_default_agent(self):
        from coordinator import parent
        from coordinator.control_api import call
        from tests.test_group_setup import HOME
        self.store.put('parent_selection', {'provider': 'claude', 'model': 'claude-opus-5-5'})
        picked = call(self.store, 'model.parent', {'topic': HOME, 'provider': 'codex',
                      'model': 'gpt-6.1-sol', 'effort': 'low'})
        self.assertTrue(picked.ok, picked.text)
        number = self.create('Default agent')
        self.assertEqual((await self.finish(number))['state'], 'done')
        target = '-10042:11'
        key = self.service.session_key(target)
        self.assertIsNone(self.store.get(key + '_selection'))
        agent = parent_state.effective_agent(self.store, key)
        self.assertFalse(agent['picked'])
        self.assertEqual((agent['provider'], agent['model'], agent['effort']),
                         ('claude', 'claude-opus-5-5', 'max'))
        session = FakeSession('default-parent')
        self.service.session_factory = parent.start
        with patch('coordinator.parent.AccountBroker.select', return_value='fixture'), \
                patch('coordinator.parent.CoordinatorSession.start', AsyncMock(return_value=session)) as default_start, \
                patch('coordinator.parent.CodexCoordinatorSession.start', AsyncMock()) as picked_start:
            self.assertIs(await self.service.start_session(target), session)
        self.assertEqual(default_start.await_args.args[3], 'claude-opus-5-5')
        self.assertEqual(default_start.await_args.kwargs['topic'], target)
        picked_start.assert_not_awaited()
        self.assertEqual(self.store.get(self.service.session_key(HOME) + '_selection'), picked.data['selection'])

    async def test_create_c3_ready_queue_error_keeps_link_and_retries_without_cleanup(self):
        number = self.create('Queue retry')
        for unused in range(3):
            await self.service.controls_once()
        with patch.object(self.store, 'enqueue_report', side_effect=sqlite3.OperationalError('fixture outbox save failure')):
            with self.assertRaises(sqlite3.OperationalError):
                await self.service.controls_once()
        self.assertTrue(self.store.topic('-10042:11')['enabled'])
        self.assertTrue((self.projects / 'queue-retry' / '.git').is_dir())
        self.assertFalse(any(method == 'deleteForumTopic' for method, params in self.telegram.calls))
        self.assertEqual((await self.finish(number))['state'], 'done')
        self.assertEqual(len(self.created()), 1)

    async def test_create_c4_api_create_during_real_project_new_preserves_both_queues(self):
        from coordinator.controls import handle_control
        from coordinator.setup_flow import tick
        from tests.test_group_setup import HOME
        with self.store.db:
            self.assertTrue(handle_control(self.store, HOME, 1500, '/project new Manual project'))
        manual = self.store.get('group_projects')
        form = self.store.get('project_setup:' + HOME)
        number = self.create('API project')
        await self.service.controls_once()
        self.assertEqual(self.store.get('group_projects'), manual)
        self.assertEqual(self.store.get('project_setup:' + HOME), form)
        await tick(self.service)
        self.assertEqual((await self.finish(number))['state'], 'done')
        self.assertEqual({row['name'] for row in self.created()}, {'Manual project', 'API project'})
        linked = [row for row in self.store.topics() if row['name'] in ('Manual project', 'API project')]
        self.assertEqual(len(linked), 2)
        self.assertTrue(all(row['enabled'] and Path(row['cwd']).is_dir() for row in linked))

    async def test_create_c3_real_sql_failure_rolls_back_link_and_carried_settings(self):
        number = self.create('Rollback')
        await self.service.controls_once()
        await self.service.controls_once()
        request = self.request(number)
        params = json.loads(request['params'])
        params['settings'] = {'coordinator:-10042:11_fixture': {'value': 'carried'}}
        with self.store.db:
            self.store.db.execute('UPDATE service_requests SET params=? WHERE id=?', (json.dumps(params), number))
            self.store.db.execute("""CREATE TRIGGER fail_carried_setting BEFORE INSERT ON settings
                WHEN new.key='coordinator:-10042:11_fixture' BEGIN SELECT RAISE(ABORT,'fixture save failure'); END""")
        self.assertEqual((await self.finish(number))['state'], 'failed')
        self.assertIsNone(self.store.topic('-10042:11'))
        self.assertIsNone(self.store.get('coordinator:-10042:11_fixture'))
        self.assertTrue((self.trash / 'rollback' / '.git').is_dir())
        self.assertIn(('deleteForumTopic', {'chat_id': -10042, 'message_thread_id': 11}), self.telegram.calls)

    async def test_create_c1_link_settings_and_checkpoint_commit_once(self):
        number = self.create('Atomic settings')
        await self.service.controls_once()
        await self.service.controls_once()
        request = self.request(number)
        params = json.loads(request['params'])
        params['settings'] = {'coordinator:-10042:11_selection': {'provider': 'codex', 'model': 'fixture'}}
        with self.store.db:
            self.store.db.execute('UPDATE service_requests SET params=? WHERE id=?', (json.dumps(params), number))
        trace = []
        self.store.db.set_trace_callback(trace.append)
        try:
            await self.service.controls_once()
        finally:
            self.store.db.set_trace_callback(None)
        self.assertEqual([statement for statement in trace if statement == 'COMMIT'], ['COMMIT'])
        self.assertEqual([statement for statement in trace if statement.startswith('BEGIN')], ['BEGIN '])
        observer = Store(self.root / 'state')
        try:
            self.assertTrue(observer.topic('-10042:11')['enabled'])
            self.assertEqual(observer.get('coordinator:-10042:11_selection'), params['settings']['coordinator:-10042:11_selection'])
            saved = observer.db.execute('SELECT result FROM service_requests WHERE id=?', (number,)).fetchone()
            self.assertEqual(json.loads(saved['result'])['phase'], 'linked')
        finally:
            observer.close()

    async def test_create_c2_crash_after_folder_creation_cleans_owned_folder_without_retry(self):
        from coordinator import topics

        class Crash(BaseException):
            pass

        number = self.create('Interrupted folder', provider='codex', effort='low')
        original = topics.onboarding._create_folder

        def crash_after_folder(*args, **kwargs):
            original(*args, **kwargs)
            raise Crash()

        with patch.object(topics.onboarding, '_create_folder', side_effect=crash_after_folder):
            with self.assertRaises(Crash):
                await self.service.controls_once()
        self.restart()
        result = await self.finish(number)
        self.assertEqual(result['state'], 'failed')
        self.assertFalse(self.created())
        self.assertFalse((self.projects / 'interrupted-folder').exists())
        self.assertTrue((self.trash / 'interrupted-folder' / '.git').is_dir())
        self.assertIsNone(self.store.get('coordinator:-10042:11_selection'))

    async def asyncSetUp(self):
        from tests.test_group_setup import GroupTelegram, HOME, pair_group
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.projects = self.root / 'projects'
        self.projects.mkdir()
        self.trash = self.root / 'trash'
        self.trash.mkdir()
        self.store = Store(self.root / 'state')
        pair_group(self.store)
        with self.store.db:
            self.store.put('execution', 'agents')
            self.store.put('coordinator_model', 'claude-opus-5-5')
            self.store.put('claude_parent_effort', 'max')
            self.store.put('codex_parent_effort', 'max')
            self.store.put('projects_root', str(self.projects))
        self.store.bind(HOME, self.root, 'Torii', enabled=True)
        self.telegram = GroupTelegram()
        self.factory = AsyncMock(return_value=FakeSession('new-parent'))
        self.service = Service(self.store, self.telegram, FakeRunner(), self.root, session_factory=self.factory)
        self.trash_patch = patch('coordinator.host_os.trash_directory', return_value=self.trash)
        self.trash_patch.start()
        self.addCleanup(self.trash_patch.stop)
        self.onboarding_trash_patch = patch('coordinator.onboarding.trash_directory', return_value=self.trash)
        self.onboarding_trash_patch.start()
        self.addCleanup(self.onboarding_trash_patch.stop)

    async def asyncTearDown(self):
        for task in self.service.feed_tasks.values():
            if not task.done():
                task.cancel()
        if self.service.feed_tasks:
            await asyncio.gather(*self.service.feed_tasks.values(), return_exceptions=True)
        self.store.close()
        self.temp.cleanup()

    def create(self, name, **params):
        from coordinator.control_api import call
        from tests.test_group_setup import HOME
        result = call(self.store, 'topic.create', dict(params, name=name), topic=HOME)
        self.assertTrue(result.ok, result.text)
        self.assertEqual(result.state, 'queued')
        return result.data['request']

    def request(self, number):
        return dict(self.store.db.execute('SELECT * FROM service_requests WHERE id=?', (number,)).fetchone())

    async def finish(self, number):
        for _ in range(15):
            if self.request(number)['state'] not in ('queued', 'sending'):
                return self.request(number)
            await self.service.controls_once()
        self.fail('The topic request did not settle in 15 service passes.')

    def restart(self):
        self.store.close()
        self.store = Store(self.root / 'state')
        self.store.requests_uncertain()
        self.service = Service(self.store, self.telegram, FakeRunner(), self.root, session_factory=self.factory)

    def created(self):
        return [params for method, params in self.telegram.calls if method == 'createForumTopic']

    def events(self):
        return [dict(row) for row in self.store.db.execute("SELECT * FROM messages WHERE kind='event' ORDER BY id")]

    async def test_create_c1_folder_topic_atomic_link_ready_event_and_ingestion_window(self):
        from tests.test_group_setup import group_update
        order = []
        original_create = self.telegram.create_topic
        original_link = self.store.link_created_topic
        original_report = self.store.enqueue_report
        original_event = self.store.message_event

        async def topic(chat, name):
            self.assertTrue((self.projects / 'ordered' / '.git').is_dir())
            order.extend(['folder', 'topic'])
            thread = await original_create(chat, name)
            target = str(chat) + ':' + str(thread)
            before = self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0]
            outcome = self.store.accept(group_update(901, 'Work sent before linking', thread=thread))
            self.assertNotIn(outcome, ('setup', 'disabled', 'control'))
            self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], before)
            self.assertIsNone(await self.service.start_session(target))
            await self.service.feed_once()
            self.factory.assert_not_awaited()
            self.assertFalse((self.store.topic(target) or {}).get('cwd'))
            return thread

        def link(*args, **kwargs):
            order.append('link')
            result = original_link(*args, **kwargs)
            target = self.store.topic('-10042:11')
            self.assertEqual((target['name'], target['cwd'], target['enabled']),
                             ('Ordered', str(self.projects / 'ordered'), 1))
            return result

        def report(target, text, *args, **kwargs):
            if target == '-10042:11':
                self.assertTrue(self.store.topic(target)['enabled'])
                self.assertIn('Ordered', text)
                self.assertIn(str(self.projects / 'ordered'), text)
                self.assertIn('Claude', text)
                self.assertIn('claude-opus-5-5', text)
                order.append('ready')
            return original_report(target, text, *args, **kwargs)

        def event(target, text):
            self.assertIn('-10042:11', text)
            self.assertIn('Ordered', text)
            self.assertIn(str(self.projects / 'ordered'), text)
            order.append('event')
            return original_event(target, text)

        number = self.create('Ordered')
        with patch.object(self.telegram, 'create_topic', side_effect=topic), \
                patch.object(self.store, 'link_created_topic', side_effect=link), \
                patch.object(self.store, 'enqueue_report', side_effect=report), \
                patch.object(self.store, 'message_event', side_effect=event):
            result = await self.finish(number)
        self.assertEqual(result['state'], 'done')
        self.assertEqual(order, ['folder', 'topic', 'link', 'ready', 'event'])
        self.assertEqual(len(self.created()), 1)
        early = self.store.db.execute('SELECT * FROM messages WHERE telegram_message=901').fetchone()
        self.assertIsNotNone(early)
        self.assertEqual(early['topic'], '-10042:11')
        self.assertEqual(self.store.get('project_setup:-10042:11'), None)

    async def test_create_c2_restart_at_each_pass_never_repeats_folder_or_telegram(self):
        for checkpoint in range(7):
            with self.subTest(checkpoint=checkpoint):
                name = 'Restart ' + str(checkpoint)
                number = self.create(name)
                before = len(self.created())
                for _ in range(checkpoint):
                    if self.request(number)['state'] == 'queued':
                        await self.service.controls_once()
                self.restart()
                result = await self.finish(number)
                self.assertIn(result['state'], ('done', 'refused', 'failed', 'uncertain'))
                self.assertLessEqual(len(self.created()) - before, 1)
                self.assertEqual(len([path for path in self.projects.iterdir() if path.name == 'restart-' + str(checkpoint)]),
                                 int(result['state'] == 'done'))
                self.assertEqual(len([entry for entry in self.created() if entry['name'] == name]),
                                 int(result['state'] == 'done'))

    async def test_create_c2_ambiguous_telegram_result_is_never_retried(self):
        original_create = self.telegram.create_topic

        async def created_then_crashed(chat, name):
            await original_create(chat, name)
            raise asyncio.CancelledError()

        number = self.create('Ambiguous', provider='codex', effort='low')
        with patch.object(self.telegram, 'create_topic', side_effect=created_then_crashed):
            for _ in range(5):
                try:
                    await self.service.controls_once()
                except asyncio.CancelledError:
                    break
            else:
                self.fail('The fake crash did not interrupt topic creation.')
        self.restart()
        await self.finish(number)
        self.assertEqual(len(self.created()), 1)
        self.assertFalse((self.projects / 'ambiguous').exists())
        self.assertTrue(any('ambig' in row['text'].lower() or 'unknown' in row['text'].lower()
                            or 'may' in row['text'].lower() for row in self.events()))

    async def test_create_c3_folder_failure_makes_no_topic_and_reports_to_asking_agent(self):
        occupied = self.projects / 'occupied'
        occupied.mkdir()
        (occupied / 'keep.txt').write_bytes(b'owner data')
        number = self.create('Occupied')
        result = await self.finish(number)
        self.assertNotEqual(result['state'], 'done')
        self.assertEqual(self.created(), [])
        self.assertEqual((occupied / 'keep.txt').read_bytes(), b'owner data')
        self.assertTrue(self.events())
        self.assertFalse(any(self.trash.iterdir()))

    async def test_create_c3_topic_failure_moves_only_new_folder_to_trash(self):
        self.telegram.create_error = TelegramError(400)
        number = self.create('Failed topic')
        result = await self.finish(number)
        self.assertNotEqual(result['state'], 'done')
        self.assertEqual(len(self.created()), 1)
        self.assertFalse((self.projects / 'failed-topic').exists())
        self.assertTrue((self.trash / 'failed-topic' / '.git').is_dir())
        self.assertTrue(self.events())

    async def test_create_c3_save_failure_deletes_topic_moves_folder_and_reports(self):
        number = self.create('Failed save')
        with patch.object(self.store, 'link_created_topic', side_effect=sqlite3.OperationalError('save failed')):
            result = await self.finish(number)
        self.assertNotEqual(result['state'], 'done')
        self.assertIn(('deleteForumTopic', {'chat_id': -10042, 'message_thread_id': 11}), self.telegram.calls)
        self.assertFalse((self.projects / 'failed-save').exists())
        self.assertTrue((self.trash / 'failed-save' / '.git').is_dir())
        self.assertFalse((self.store.topic('-10042:11') or {}).get('enabled'))
        self.assertTrue(self.events())

    async def test_create_c3_save_and_delete_failure_leaves_unlinked_topic_and_says_so(self):
        original_call = self.telegram.call

        async def delete_fails(method, **params):
            if method == 'deleteForumTopic':
                self.telegram.calls.append((method, params))
                raise TelegramError(403)
            return await original_call(method, **params)

        number = self.create('Unlinked')
        with patch.object(self.store, 'link_created_topic', side_effect=sqlite3.OperationalError('save failed')), \
                patch.object(self.telegram, 'call', side_effect=delete_fails):
            result = await self.finish(number)
        self.assertNotEqual(result['state'], 'done')
        self.assertFalse((self.store.topic('-10042:11') or {}).get('enabled'))
        self.assertFalse((self.projects / 'unlinked').exists())
        self.assertTrue(self.events())
        self.assertTrue(any('unlink' in row['text'].lower() for row in self.events()))

    async def test_create_c3_existing_folder_is_preserved_after_topic_failure(self):
        existing = self.root / 'existing'
        existing.mkdir()
        (existing / 'keep.txt').write_bytes(b'keep')
        self.telegram.create_error = TelegramError(400)
        number = self.create('Existing', cwd=str(existing))
        result = await self.finish(number)
        self.assertNotEqual(result['state'], 'done')
        self.assertEqual((existing / 'keep.txt').read_bytes(), b'keep')
        self.assertFalse(any(self.trash.iterdir()))

    async def test_create_c3_ready_delivery_failure_uses_outbox_retry_and_keeps_link(self):
        number = self.create('Retry ready')
        result = await self.finish(number)
        self.assertEqual(result['state'], 'done')
        self.telegram.send = AsyncMock(side_effect=[TelegramError(429, retry_after=0), {'message_id': 808}])
        self.assertTrue(await self.service.deliver_once())
        row = self.store.db.execute("SELECT * FROM outbox WHERE topic='-10042:11'").fetchone()
        self.assertFalse(row['delivered'])
        self.assertTrue(self.store.topic('-10042:11')['enabled'])
        with self.store.db:
            self.store.db.execute('UPDATE outbox SET next_attempt=0 WHERE id=?', (row['id'],))
        self.assertTrue(await self.service.deliver_once())
        self.assertTrue(self.store.db.execute('SELECT delivered FROM outbox WHERE id=?', (row['id'],)).fetchone()[0])
        self.assertEqual(len(self.created()), 1)

    async def test_create_c4_two_creates_and_owner_setup_state_are_independent(self):
        from tests.test_group_setup import HOME
        stages = ({'stage': 'new'}, {'stage': 'project_new', 'name_armed': True})
        for index, stage in enumerate(stages):
            with self.subTest(stage=stage):
                with self.store.db:
                    self.store.put('project_setup:' + HOME, stage)
                one = self.create('Parallel ' + str(index) + ' A')
                two = self.create('Parallel ' + str(index) + ' B')
                self.assertEqual((await self.finish(one))['state'], 'done')
                self.assertEqual((await self.finish(two))['state'], 'done')
                self.assertEqual(self.store.get('project_setup:' + HOME), stage)
        self.assertEqual(len(self.created()), 4)
        self.assertEqual(len({entry['name'] for entry in self.created()}), 4)
        self.assertEqual(len([entry for entry in self.store.topics() if entry['cwd'] != str(self.root) and entry['enabled']]), 4)


class CloseTopicsTestsSupport(ServiceTestsSupport):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.projects = self.root / 'projects'
        self.projects.mkdir()
        self.project = self.projects / 'closing-project'
        self.project.mkdir()
        self.home_project = self.root / 'home-project'
        self.home_project.mkdir()
        with self.store.db:
            self.store.db.execute('UPDATE topics SET cwd=? WHERE id=?', (str(self.home_project), TOPIC))
        self.trash = self.root / 'scratch-trash'
        self.trash.mkdir()
        self.closing_topic = '-10042:9'
        self.callback_number = 9000
        with self.store.db:
            self.store.put('projects_root', str(self.projects))
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''',
                (self.closing_topic, -10042, 9, 'Closing project', str(self.project)))
        self.trash_patch = patch('coordinator.topics.host_os.trash_directory', return_value=self.trash)
        self.trash_patch.start()
        self.addCleanup(self.trash_patch.stop)

    def close_call(self, topic=None, asking=TOPIC, source='cli', message=None):
        from coordinator.control_api import call
        params = {'topic': topic or self.closing_topic}
        if message is not None:
            params['message'] = message
        with self.store.db:
            return call(self.store, 'topic.close', params, topic=asking, source=source)

    def request(self, request_id):
        return dict(self.store.db.execute('SELECT * FROM service_requests WHERE id=?',
                                         (request_id,)).fetchone())

    def latest_close_card(self, topic=TOPIC):
        for row in self.store.db.execute('SELECT * FROM outbox WHERE topic=? ORDER BY id DESC', (topic,)):
            if row['reply_markup'] and 'Close, keep folder' in row['reply_markup']:
                card = dict(row)
                card['markup'] = json.loads(card['reply_markup'])
                return card
        self.fail('A close confirmation card was not queued in the asking chat.')

    def card_labels(self, card):
        return [button['text'] for row in card['markup']['inline_keyboard'] for button in row]

    def tap(self, card, label, user=7, store=None):
        store = store or self.store
        self.callback_number += 1
        button = next(button for row in card['markup']['inline_keyboard'] for button in row
                      if button['text'] == label)
        target = store.topic(card['topic'])
        return store.accept({'update_id': self.callback_number, 'callback_query': {
            'id': str(self.callback_number), 'from': {'id': user}, 'data': button['callback_data'],
            'message': {'message_id': card['telegram_message'] or 1000 + card['id'],
                        'message_thread_id': target['thread'],
                        'chat': {'id': target['chat'], 'type': 'supergroup'}}}})

    async def confirmation(self, asking=TOPIC):
        message = self.store.message_save(asking, 'owner', 'Close this chat.') if asking != TOPIC else None
        result = self.close_call(asking=asking, message=message['id'] if message else None)
        self.assertTrue(result.ok, result.text)
        self.assertEqual(result.state, 'queued')
        await self.service.controls_once()
        return result.data['request'], self.latest_close_card(asking)

    async def finish_close(self, request_id):
        for unused in range(30):
            request = self.request(request_id)
            if request['state'] in ('done', 'refused', 'failed', 'cancelled'):
                return request
            await self.service.controls_once()
        self.fail('The approved close did not settle after 30 deterministic service passes.')

    def add_shared_topic(self, path, enabled=False):
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET cwd=excluded.cwd,enabled=excluded.enabled''',
                ('-10042:10', -10042, 10, 'Shared project', str(path), int(enabled)))


class CloseTopicsTests(CloseTopicsTestsSupport, unittest.IsolatedAsyncioTestCase):
    def message_close(self, folder, message=None, source='mcp'):
        from coordinator.control_api import call
        params = {'topic': self.closing_topic, 'folder': folder}
        if message is not None:
            params['message'] = message
        with self.store.db:
            return call(self.store, 'topic.close', params, topic=TOPIC, source=source)

    def assert_no_close_card(self):
        self.assertEqual(self.store.db.execute(
            'SELECT count(*) FROM outbox WHERE reply_markup IS NOT NULL').fetchone()[0], 0)
        self.assertEqual(self.store.db.execute(
            "SELECT count(*) FROM settings WHERE key LIKE 'topic_close_card:%'").fetchone()[0], 0)

    def assert_close_untouched(self, task):
        self.assertEqual(self.store.topic(self.closing_topic)['enabled'], 1)
        self.assertEqual(self.store.task_get(task['id'])['status'], 'open')
        self.assertEqual(self.store.get('topic_lifecycle:' + self.closing_topic), None)
        self.assertTrue(self.project.is_dir())
        self.assertEqual(list(self.trash.iterdir()), [])
        self.assertEqual(self.telegram.calls, [])
        self.assert_no_close_card()

    def close_report(self):
        row = self.store.db.execute(
            "SELECT text FROM messages WHERE topic=? AND kind='event' ORDER BY id DESC LIMIT 1", (TOPIC,)).fetchone()
        self.assertIsNotNone(row)
        report, = self.store.db.execute('SELECT text FROM outbox WHERE topic=? AND kind=?', (TOPIC, 'report'))
        self.assertEqual(report['text'], row['text'])
        return row['text']

    async def assert_message_close_finishes(self, folder, source):
        first = self.store.task_create(self.closing_topic, 'First open job', notes='Preserved notes.')
        second = self.store.task_create(self.closing_topic, 'Second open job')
        done = self.store.task_create(self.closing_topic, 'Previously finished job')
        self.store.task_update(done['id'], status='done')
        message = self.store.message_save(TOPIC, 'owner', 'Close that chat and ' + folder + ' its folder.')['id']
        payload = b'Keep these project bytes.\n'
        (self.project / 'evidence.bin').write_bytes(payload)
        session = await self.service.start_session(self.closing_topic)
        native_id = session.session_id
        result = self.message_close(folder, message, source)
        self.assertTrue(result.ok, result.text)
        self.assertEqual(result.state, 'queued')
        request_id = result.data['request']
        self.assertEqual(json.loads(self.request(request_id)['params'])['folder'], folder)
        settled = await self.finish_close(request_id)
        self.assertEqual(settled['state'], 'done', settled['result'])
        self.assertEqual(self.store.topic(self.closing_topic)['enabled'], 0)
        self.assertTrue(session.closed)
        self.assertEqual(self.store.get(session.key + '_session'), native_id)
        self.assertEqual(self.telegram.calls, [('deleteForumTopic', {'chat_id': -10042, 'message_thread_id': 9})])
        for job in (first, second):
            self.assertEqual(self.store.task_get(job['id'])['status'], 'dropped')
        self.assertEqual(self.store.task_get(first['id'])['notes'], 'Preserved notes.')
        self.assertEqual(self.store.task_get(done['id'])['status'], 'done')
        self.assert_no_close_card()
        report = self.close_report()
        self.assertIn('#%s First open job' % first['number'], report.splitlines())
        self.assertIn('#%s Second open job' % second['number'], report.splitlines())
        self.assertNotIn('#%s Previously finished job' % done['number'], report.splitlines())
        destination = self.trash / self.project.name if folder == 'trash' else self.project
        self.assertEqual((destination / 'evidence.bin').read_bytes(), payload)
        self.assertEqual(self.project.exists(), folder == 'keep')
        self.assertIn('Folder ' + ('moved to Trash: ' if folder == 'trash' else 'kept: ') + str(destination), report)
        if folder == 'keep':
            self.assertEqual(list(self.trash.iterdir()), [])

    async def test_message_confirmed_trash_mcp_closes_without_card_and_names_dropped_jobs(self):
        await self.assert_message_close_finishes('trash', 'mcp')

    async def test_message_confirmed_keep_cli_closes_without_card_and_names_dropped_jobs(self):
        await self.assert_message_close_finishes('keep', 'cli')

    async def test_message_close_replaces_refreshed_delivered_card_and_rejects_old_taps(self):
        job = self.store.task_create(self.closing_topic, 'Open job')
        request_id, card = await self.confirmation()
        await self.service.deliver_once()
        self.store.task_update(job['id'], note='Worker progress note')
        self.assertEqual(self.tap(card, 'Close, keep folder'), 'control_callback')
        fresh = self.latest_close_card()
        self.assertIn('Review this fresh confirmation.', fresh['text'])
        await self.service.deliver_once()
        message = self.store.message_save(TOPIC, 'owner', 'Close it, keep the folder.')['id']
        result = self.message_close('keep', message)
        self.assertTrue(result.ok, result.text)
        self.assertEqual(self.request(request_id)['state'], 'cancelled')
        for previous in (card, fresh):
            saved = self.store.db.execute('SELECT reply_markup,telegram_message FROM outbox WHERE id=?',
                                          (previous['id'],)).fetchone()
            self.assertIsNone(saved['reply_markup'])
            edit, = self.store.db.execute('SELECT * FROM outbox WHERE edit_message=?',
                                          (saved['telegram_message'],))
            self.assertEqual(json.loads(edit['reply_markup']), {'inline_keyboard': []})
            self.assertEqual(self.tap(previous, 'Close and move folder to Trash'), 'already_done')
        self.assertEqual(self.store.topic(self.closing_topic)['enabled'], 1)
        self.assertEqual((await self.finish_close(result.data['request']))['state'], 'done')
        self.assertEqual(self.store.topic(self.closing_topic)['enabled'], 0)
        self.assertEqual(self.store.task_get(job['id'])['status'], 'dropped')
        self.assertTrue(self.project.is_dir())
        self.assertEqual(list(self.trash.iterdir()), [])
        self.assertEqual(self.telegram.calls, [('deleteForumTopic', {'chat_id': -10042, 'message_thread_id': 9})])
        for previous in (card, fresh):
            self.assertEqual(self.tap(previous, 'Close and move folder to Trash'), 'already_done')
        self.assertTrue(self.project.is_dir())

    async def assert_tap_race_finishes(self, request_id, job, result, tap, folder):
        tapped = await self.finish_close(request_id)
        if result.ok:
            settled = await self.finish_close(result.data['request'])
            self.assertIn(settled['state'], ('done', 'refused'))
        self.assertEqual(self.telegram.calls, [('deleteForumTopic', {'chat_id': -10042, 'message_thread_id': 9})],
                         'The competing closes left the chat stuck closing.')
        self.assertEqual(self.store.task_get(job['id'])['status'], 'dropped')
        self.assertEqual(self.store.topic(self.closing_topic)['enabled'], 0)
        self.assertEqual(self.store.get('topic_lifecycle:' + self.closing_topic)['state'], 'deleted')
        if tap == 'control_callback':
            self.assertEqual(tapped['state'], 'done')
            self.assertFalse(result.ok)
            self.assertIn('already closing', result.text)
            self.assertEqual(self.store.db.execute(
                "SELECT count(*) FROM service_requests WHERE op!='_cleanup.host_logs'").fetchone()[0], 1)
            self.assertEqual(self.project.exists(), folder == 'keep')
        else:
            self.assertEqual(tap, 'already_done')
            self.assertEqual(tapped['state'], 'cancelled')
            self.assertTrue(result.ok, result.text)
            self.assertEqual(settled['state'], 'done')
            self.assertTrue(self.project.is_dir())
        self.assertEqual(self.store.service_requests_pending(), [])

    async def assert_tap_during_message_snapshot(self, folder):
        from coordinator import topics
        job = self.store.task_create(self.closing_topic, 'Open job')
        request_id, card = await self.confirmation()
        await self.service.deliver_once()
        message = self.store.message_save(TOPIC, 'owner', 'Close it, keep the folder.')['id']
        service_side = Store(self.root / 'state')
        self.addCleanup(service_side.close)
        real = topics._shared_folders
        fired = []

        def racing(store, topic, resolved):
            if store is self.store and not fired:
                label = 'Close, keep folder' if folder == 'keep' else 'Close and move folder to Trash'
                fired.append(self.tap(card, label, store=service_side))
            return real(store, topic, resolved)

        with patch('coordinator.topics._shared_folders', racing):
            result = self.message_close('keep', message)
        self.assertEqual(fired, ['control_callback'])
        await self.assert_tap_race_finishes(request_id, job, result, fired[0], folder)

    async def test_message_close_snapshot_race_preserves_keep_tap(self):
        await self.assert_tap_during_message_snapshot('keep')

    async def test_message_close_snapshot_race_preserves_trash_tap(self):
        await self.assert_tap_during_message_snapshot('trash')

    async def assert_threaded_tap_during_message_close(self, delay):
        self.initialize_git()
        for index in range(230):
            (self.project / ('notes-%03d.txt' % index)).write_text('Untracked project content.\n')
        job = self.store.task_create(self.closing_topic, 'Open job')
        request_id, card = await self.confirmation()
        await self.service.deliver_once()
        message = self.store.message_save(TOPIC, 'owner', 'Close it, keep the folder.')['id']
        outcome = {}

        def service_tap():
            time.sleep(delay)
            service_side = Store(self.root / 'state')
            try:
                outcome['tap'] = self.tap(card, 'Close, keep folder', store=service_side)
            except Exception as error:
                outcome['error'] = error
            finally:
                service_side.close()

        thread = threading.Thread(target=service_tap)
        thread.start()
        try:
            result = self.message_close('keep', message)
        finally:
            thread.join(timeout=35)
        self.assertFalse(thread.is_alive())
        if 'error' in outcome:
            raise outcome['error']
        await self.assert_tap_race_finishes(request_id, job, result, outcome['tap'], 'keep')

    async def test_message_close_threaded_keep_tap_at_zero(self):
        await self.assert_threaded_tap_during_message_close(0)

    async def test_message_close_threaded_keep_tap_at_10ms(self):
        await self.assert_threaded_tap_during_message_close(0.01)

    async def test_message_close_threaded_keep_tap_at_30ms(self):
        await self.assert_threaded_tap_during_message_close(0.03)

    async def test_message_close_threaded_keep_tap_at_60ms(self):
        await self.assert_threaded_tap_during_message_close(0.06)

    async def test_message_close_replaces_undelivered_card_and_trashes_folder(self):
        request_id, card = await self.confirmation()
        message = self.store.message_save(TOPIC, 'owner', 'Close it and move its folder to Trash.')['id']
        result = self.message_close('trash', message)
        self.assertTrue(result.ok, result.text)
        self.assertEqual(self.request(request_id)['state'], 'cancelled')
        self.assertIsNone(self.store.db.execute('SELECT reply_markup FROM outbox WHERE id=?',
                                               (card['id'],)).fetchone()[0])
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox WHERE edit_message IS NOT NULL').fetchone()[0], 0)
        self.assertEqual(self.tap(card, 'Close, keep folder'), 'already_done')
        self.assertEqual((await self.finish_close(result.data['request']))['state'], 'done')
        self.assertFalse(self.project.exists())
        self.assertTrue((self.trash / self.project.name).is_dir())

    async def test_refused_message_close_leaves_waiting_card_active(self):
        request_id, card = await self.confirmation()
        self.add_shared_topic(self.project)
        message = self.store.message_save(TOPIC, 'owner', 'Close it and move its folder to Trash.')['id']
        result = self.message_close('trash', message)
        self.assertFalse(result.ok)
        self.assertIn('cannot be moved to Trash', result.text)
        self.assertEqual(self.request(request_id)['state'], 'confirming')
        self.assertEqual(self.latest_close_card()['id'], card['id'])
        self.assertEqual(self.store.topic(self.closing_topic)['enabled'], 1)
        self.assertTrue(self.project.is_dir())

    async def test_message_keep_close_refuses_trash_tap_on_concurrently_queued_card(self):
        request_id = self.close_call().data['request']
        message = self.store.message_save(TOPIC, 'owner', 'Close it, keep the folder.')['id']
        with patch('coordinator.topics._requests', return_value=[]):
            result = self.message_close('keep', message)
        self.assertTrue(result.ok, result.text)
        await self.service.controls_once()
        card = self.latest_close_card()
        self.assertEqual((await self.finish_close(result.data['request']))['state'], 'done')
        self.assertEqual(self.tap(card, 'Close and move folder to Trash'), 'control_callback')
        self.assertEqual(self.request(request_id)['state'], 'refused')
        self.assertTrue(self.project.is_dir())
        self.assertEqual(list(self.trash.iterdir()), [])
        self.assertEqual(self.telegram.calls, [('deleteForumTopic', {'chat_id': -10042, 'message_thread_id': 9})])

    async def test_message_close_mcp_schema_and_dispatch_accept_folder_and_report_no_jobs(self):
        from coordinator import mcp
        tools = mcp.dispatch(self.store, {'method': 'tools/list'})['tools']
        schema = next(tool['inputSchema'] for tool in tools if tool['name'] == 'topic_close')
        self.assertEqual(schema['properties'].get('folder'), {'type': 'string', 'enum': ['trash', 'keep']})
        self.assertNotIn('folder', schema['required'])
        self.assertIn('message', schema['required'])
        message = self.store.message_save(TOPIC, 'owner', 'Close the chat, keep its folder.')['id']
        result = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
            'name': 'topic_close', 'arguments': {'topic': self.closing_topic, 'message': message, 'folder': 'keep'}}},
            topic=TOPIC)
        self.assertFalse(result['isError'], result)
        request_id = self.store.db.execute('SELECT id FROM service_requests').fetchone()[0]
        self.assertEqual((await self.finish_close(request_id))['state'], 'done')
        self.assertEqual(self.store.topic(self.closing_topic)['enabled'], 0)
        self.assertTrue(self.project.is_dir())
        self.assert_no_close_card()
        self.assertIn('None.', self.close_report().splitlines())

    async def test_message_close_refuses_general_home_and_existing_close_at_call_time(self):
        from coordinator.control_api import call
        task = self.store.task_create(self.closing_topic, 'Keep open')
        message = self.store.message_save(TOPIC, 'owner', 'Close a chat.')['id']
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', ('-10042:1', -10042, 1, 'General', ''))
        for topic, reason in (('-10042:1', 'General'), (TOPIC, 'home')):
            with self.subTest(topic=topic), self.store.db:
                result = call(self.store, 'topic.close', {'topic': topic, 'message': message, 'folder': 'keep'},
                              topic=TOPIC, source='mcp')
                self.assertFalse(result.ok)
                self.assertEqual(result.state, 'refused')
                self.assertIn(reason, result.text)
                self.assertEqual(self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0], 0)
                self.assert_close_untouched(task)
        first = self.message_close('keep', message)
        self.assertTrue(first.ok, first.text)
        second = self.message_close('keep', message)
        self.assertFalse(second.ok)
        self.assertIn('already', second.text)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0], 1)
        self.assert_close_untouched(task)

    async def test_message_close_requires_message_on_cli_and_mcp(self):
        task = self.store.task_create(self.closing_topic, 'Keep open')
        for source in ('cli', 'mcp'):
            with self.subTest(source=source):
                result = self.message_close('keep', source=source)
                self.assertFalse(result.ok)
                self.assertEqual(result.state, 'refused')
                self.assertIn('message', result.text)
                self.assertEqual(self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0], 0)
                self.assert_close_untouched(task)

    async def test_message_close_refuses_forwarded_and_event_messages(self):
        task = self.store.task_create(self.closing_topic, 'Keep open')
        for kind in ('forwarded', 'event'):
            for source in ('cli', 'mcp'):
                with self.subTest(kind=kind, source=source):
                    message = self.store.message_save(TOPIC, kind, 'Close the chat.')['id']
                    result = self.message_close('keep', message, source)
                    self.assertFalse(result.ok)
                    self.assertEqual(result.state, 'refused')
                    self.assertIn('owner message', result.text)
                    self.assertEqual(self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0], 0)
                    self.assert_close_untouched(task)

    async def test_message_close_mcp_refuses_owner_message_from_another_chat(self):
        task = self.store.task_create(self.closing_topic, 'Keep open')
        message = self.store.message_save(self.closing_topic, 'owner', 'Close the chat.')['id']
        result = self.message_close('keep', message)
        self.assertFalse(result.ok)
        self.assertEqual(result.state, 'refused')
        self.assertIn('another channel', result.text)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0], 0)
        self.assert_close_untouched(task)

    async def test_message_close_refuses_invalid_folder_value_at_call_time(self):
        task = self.store.task_create(self.closing_topic, 'Keep open')
        message = self.store.message_save(TOPIC, 'owner', 'Close the chat.')['id']
        result = self.message_close('other', message)
        self.assertFalse(result.ok)
        self.assertEqual(result.state, 'refused')
        self.assertIn('trash', result.text.lower())
        self.assertIn('keep', result.text.lower())
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0], 0)
        self.assert_close_untouched(task)

    async def test_message_close_refuses_unfinished_workers_at_call_time(self):
        task = self.store.task_create(self.closing_topic, 'Keep open')
        message = self.store.message_save(TOPIC, 'owner', 'Close the chat.')['id']
        for provider in ('claude', 'codex', 'opencode'):
            worker = self.worker(topic=self.closing_topic, provider=provider)
            with self.subTest(provider=provider):
                result = self.message_close('keep', message)
                self.assertFalse(result.ok)
                self.assertEqual(result.state, 'refused')
                self.assertIn('Unfinished workers:', result.text)
                self.assertIn(str(worker), result.text)
                self.assertEqual(self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0], 0)
                self.assert_close_untouched(task)
            with self.store.db:
                self.store.db.execute("UPDATE workers SET status='done' WHERE id=?", (worker,))

    async def test_message_close_refuses_shared_trash_at_call_time_but_keep_closes(self):
        task = self.store.task_create(self.closing_topic, 'Drop only after keep')
        message = self.store.message_save(TOPIC, 'owner', 'Close the shared chat.')['id']
        self.add_shared_topic(self.project, enabled=True)
        result = self.message_close('trash', message)
        self.assertFalse(result.ok)
        self.assertEqual(result.state, 'refused')
        self.assertIn('trash', result.text.lower())
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0], 0)
        self.assert_close_untouched(task)
        result = self.message_close('keep', message)
        self.assertTrue(result.ok, result.text)
        self.assertEqual((await self.finish_close(result.data['request']))['state'], 'done')
        self.assertEqual(self.store.topic(self.closing_topic)['enabled'], 0)
        self.assertEqual(self.store.topic('-10042:10')['enabled'], 1)
        self.assertEqual(self.store.task_get(task['id'])['status'], 'dropped')
        self.assertTrue(self.project.is_dir())
        self.assertEqual(list(self.trash.iterdir()), [])
        self.assert_no_close_card()

    async def test_message_close_refuses_unsafe_trash_snapshots_at_call_time(self):
        task = self.store.task_create(self.closing_topic, 'Keep open')
        message = self.store.message_save(TOPIC, 'owner', 'Close the chat.')['id']
        outside = self.root / 'outside'
        outside.mkdir()
        link = self.projects / 'link'
        link.symlink_to(self.project, target_is_directory=True)
        child = self.project / 'child'
        child.mkdir()
        for folder in (outside, self.projects, link, self.projects / 'missing', child):
            with self.subTest(folder=folder):
                with self.store.db:
                    self.store.db.execute('UPDATE topics SET cwd=? WHERE id=?', (str(folder), self.closing_topic))
                if folder == child:
                    self.add_shared_topic(self.project)
                result = self.message_close('trash', message)
                self.assertFalse(result.ok)
                self.assertEqual(result.state, 'refused')
                self.assertIn('trash', result.text.lower())
                self.assertEqual(self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0], 0)
                self.assert_close_untouched(task)

    async def test_message_close_worker_start_after_call_refuses_without_card_or_effects(self):
        task = self.store.task_create(self.closing_topic, 'Keep open')
        message = self.store.message_save(TOPIC, 'owner', 'Close the chat.')['id']
        result = self.message_close('keep', message)
        self.assertTrue(result.ok, result.text)
        worker = self.worker(topic=self.closing_topic, provider='codex')
        settled = await self.finish_close(result.data['request'])
        self.assertEqual(settled['state'], 'refused', settled['result'])
        report = self.close_report()
        self.assertIn('Nothing was closed.', report)
        self.assertIn('Unfinished workers:', report)
        self.assertIn(str(worker), report)
        self.assert_close_untouched(task)

    async def test_message_close_shared_folder_after_call_refuses_trash_without_effects(self):
        task = self.store.task_create(self.closing_topic, 'Keep open')
        message = self.store.message_save(TOPIC, 'owner', 'Close the chat.')['id']
        result = self.message_close('trash', message)
        self.assertTrue(result.ok, result.text)
        self.add_shared_topic(self.project)
        settled = await self.finish_close(result.data['request'])
        self.assertEqual(settled['state'], 'refused', settled['result'])
        self.assertIn('Nothing was closed.', self.close_report())
        self.assert_close_untouched(task)

    async def test_card_close_report_names_actual_dropped_jobs(self):
        task = self.store.task_create(self.closing_topic, 'Actually dropped')
        completed = self.store.task_create(self.closing_topic, 'Finished before drop')
        request_id, card = await self.confirmation()
        self.tap(card, 'Close, keep folder')
        self.store.task_update(completed['id'], status='done')
        self.assertEqual((await self.finish_close(request_id))['state'], 'done')
        report = self.store.db.execute(
            "SELECT text FROM messages WHERE topic=? AND kind='event' ORDER BY id DESC LIMIT 1", (TOPIC,)).fetchone()['text']
        self.assertIn('#%s Actually dropped' % task['number'], report.splitlines())
        self.assertNotIn('#%s Finished before drop' % completed['number'], report.splitlines())

    async def test_card_close_report_says_no_jobs_when_none_were_dropped(self):
        request_id, card = await self.confirmation()
        self.tap(card, 'Close, keep folder')
        self.assertEqual((await self.finish_close(request_id))['state'], 'done')
        report = self.store.db.execute(
            "SELECT text FROM messages WHERE topic=? AND kind='event' ORDER BY id DESC LIMIT 1", (TOPIC,)).fetchone()['text']
        self.assertIn('None.', report.splitlines())

    async def test_K9_concurrent_close_marker_prevents_job_and_rebind_after_stale_read(self):
        from coordinator.control_api import call
        key = 'topic_lifecycle:' + self.closing_topic
        original_get = self.store.get
        for op, params in (('tasks.create', {'topic': self.closing_topic, 'title': 'Racing job'}),
                           ('topic.bind', {'topic': self.closing_topic, 'cwd': str(self.project), 'name': 'Again', 'enable': True})):
            first = True

            def stale_read(name, *args):
                nonlocal first
                if name == key and first:
                    first = False
                    observer = Store(self.root / 'state')
                    try:
                        with observer.db:
                            observer.put(key, {'state': 'closing', 'request': 1})
                    finally:
                        observer.close()
                    return None
                return original_get(name, *args)

            with patch.object(self.store, 'get', side_effect=stale_read):
                result = call(self.store, op, params)
            self.assertFalse(result.ok, result.text)
            self.assertIsNone(self.store.db.execute('SELECT 1 FROM tasks WHERE title=?', ('Racing job',)).fetchone())
            with self.store.db:
                self.store.put(key, None)

    async def test_K9_late_outbox_retirement_reads_marker_in_the_insert(self):
        key = 'topic_lifecycle:' + self.closing_topic
        with self.store.db:
            self.store.put(key, {'state': 'deleted', 'request': 1})
        original_get = self.store.get

        def stale_read(name, *args):
            return None if name == key else original_get(name, *args)

        with patch.object(self.store, 'get', side_effect=stale_read), self.store.db:
            row = self.store.enqueue_report(self.closing_topic, 'Delayed sender')
        self.assertEqual(self.store.db.execute('SELECT retired FROM outbox WHERE id=?', (row,)).fetchone()[0], 1)

    async def test_K9_closed_chat_rejects_late_reports_jobs_and_rebinding(self):
        from coordinator.control_api import call
        request_id, card = await self.confirmation()
        self.tap(card, 'Close, keep folder')
        await self.finish_close(request_id)
        for op, params in (('telegram.send', {'topic': self.closing_topic, 'text': 'Late report'}),
                           ('tasks.create', {'topic': self.closing_topic, 'title': 'Late job'}),
                           ('topic.bind', {'topic': self.closing_topic, 'cwd': str(self.project), 'name': 'Again', 'enable': True})):
            with self.store.db:
                result = call(self.store, op, params)
            self.assertFalse(result.ok, result.text)
        with self.store.db:
            outgoing = self.store.enqueue_report(self.closing_topic, 'Late service notice')
        self.assertEqual(self.store.db.execute('SELECT retired FROM outbox WHERE id=?', (outgoing,)).fetchone()[0], 1)
        self.assertEqual(self.store.topic(self.closing_topic)['enabled'], 0)

    def git(self, *args):
        return subprocess.run(['git', '-C', str(self.project), *args], check=True,
                              capture_output=True, text=True, timeout=10).stdout

    def initialize_git(self):
        self.git('init', '-q', '-b', 'main')
        self.git('config', 'user.name', 'Fixture')
        self.git('config', 'user.email', 'fixture@example.invalid')
        self.git('-c', 'core.hooksPath=/dev/null', '-c', 'commit.gpgSign=false',
                 'commit', '--allow-empty', '-qm', 'Initial commit')

    async def assert_close_matches_origin_main(self, source):
        from coordinator import mcp
        from coordinator.__main__ import ctl
        with patch('time.time', return_value=1000.0), patch(
                'coordinator.control_ui.secrets.token_hex', return_value='0123456789abcdef'):
            message = self.store.message_save(TOPIC, 'owner', 'Close this chat.')['id']
            if source == 'cli':
                output = StringIO()
                with patch('coordinator.__main__.service_running', return_value=True), redirect_stdout(output):
                    status = ctl(Namespace(operation='call', op='topic.close',
                                           pairs=['topic=' + self.closing_topic, 'message=' + str(message)],
                                           topic=TOPIC), self.store)
                reply = {'exit_code': status, 'payload': json.loads(output.getvalue())}
            else:
                reply = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
                    'name': 'topic_close', 'arguments': {'topic': self.closing_topic, 'message': message}}},
                    topic=TOPIC)
            self.assertEqual(reply, ORIGIN_MAIN_CLOSE['reply'][source])
            await self.service.controls_once()
        expected = json.loads(json.dumps(ORIGIN_MAIN_CLOSE).replace('{root}', str(self.root)))
        metadata = self.project.stat()
        expected['request']['result']['snapshot']['identity'] = [metadata.st_dev, metadata.st_ino]
        request = self.request(1)
        request['params'] = json.loads(request['params'])
        request['result'] = json.loads(request['result'])
        self.assertEqual(request, expected['request'])
        card, = self.store.db.execute('SELECT * FROM outbox')
        card = dict(card)
        card['reply_markup'] = json.loads(card['reply_markup'])
        self.assertEqual(card, expected['card'])
        self.assertEqual(self.store.topic(self.closing_topic)['enabled'], 1)
        self.assertTrue(self.project.is_dir())
        self.assertFalse(self.telegram.calls)

    async def test_close_matches_origin_main_cli(self):
        await self.assert_close_matches_origin_main('cli')

    async def test_close_matches_origin_main_mcp(self):
        await self.assert_close_matches_origin_main('mcp')

    async def test_K2_close_refuses_unknown_general_and_home_topics(self):
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,0)''', ('-10042:0', -10042, 0, 'General', ''))
        for topic in ('-10042:987654', '-10042:0', TOPIC):
            with self.subTest(topic=topic):
                result = self.close_call(topic)
                if result.ok:
                    await self.service.controls_once()
                    self.assertIn(self.request(result.data['request'])['state'], ('refused', 'failed'))
                else:
                    self.assertEqual(result.state, 'refused')
        self.assertFalse(self.telegram.calls)
        self.assertEqual(self.store.topic(self.closing_topic)['enabled'], 1)
        self.assertIsNone(self.store.db.execute(
            'SELECT 1 FROM outbox WHERE reply_markup LIKE ?', ('%Close, keep folder%',)).fetchone())

    async def test_K2_unfinished_workers_of_every_provider_refuse_and_name_the_worker(self):
        for provider in ('claude', 'codex', 'opencode'):
            for status in ('queued', 'running', 'parked', 'waiting_for_secret', 'waiting_for_quota'):
                with self.subTest(provider=provider, status=status):
                    task = self.store.task_create(self.closing_topic, 'Unfinished job')
                    with self.store.db:
                        row = self.store.db.execute('''INSERT INTO workers
                            (task,topic,provider,prompt,cwd,status,created,updated)
                            VALUES (?,?,?,?,?,?,?,?)''',
                            (task['id'], self.closing_topic, provider, 'unfinished brief',
                             str(self.project), status, time.time(), time.time()))
                    worker_id = row.lastrowid
                    result = self.close_call()
                    if result.ok:
                        await self.service.controls_once()
                        request = self.request(result.data['request'])
                        self.assertIn(request['state'], ('refused', 'failed'))
                        notice = request['result'] or ''
                    else:
                        notice = result.text
                    self.assertIn(str(worker_id), notice)
                    self.assertEqual(self.store.topic(self.closing_topic)['enabled'], 1)
                    self.assertFalse(self.telegram.calls)
                    with self.store.db:
                        self.store.db.execute("UPDATE workers SET status='done' WHERE id=?", (worker_id,))
                        self.store.task_update(task['id'], status='done')

    async def test_K2_second_close_waiting_for_owner_is_refused(self):
        request_id, card = await self.confirmation()
        result = self.close_call()
        if result.ok:
            await self.service.controls_once()
            self.assertEqual(self.request(result.data['request'])['state'], 'refused')
        else:
            self.assertEqual(result.state, 'refused')
        self.assertEqual(self.request(request_id)['state'], 'confirming')
        self.assertEqual(self.latest_close_card()['id'], card['id'])
        self.assertEqual(self.store.topic(self.closing_topic)['enabled'], 1)

    async def test_K3_unused_folder_card_names_irreversible_effects_and_open_jobs(self):
        task = self.store.task_create(self.closing_topic, 'Keep these notes', notes='Evidence stays.')
        request_id, card = await self.confirmation()
        self.assertIn('Closing project', card['text'])
        self.assertIn(str(self.project), card['text'])
        self.assertIn(str(task['number']), card['text'])
        self.assertIn(task['title'], card['text'])
        self.assertIn('agent', card['text'].lower())
        self.assertIn('stop', card['text'].lower())
        self.assertIn('all its messages', card['text'].lower())
        self.assertIn('cannot be undone', card['text'].lower())
        self.assertIn('not a git repository', card['text'].lower())
        self.assertEqual(self.card_labels(card),
                         ['Close and move folder to Trash', 'Close, keep folder', 'Cancel'])
        self.assertEqual(self.request(request_id)['state'], 'confirming')
        self.assertEqual(self.store.task_get(task['id'])['status'], 'open')
        self.assertEqual(self.store.topic(self.closing_topic)['enabled'], 1)
        self.assertFalse(self.telegram.calls)
        self.assertTrue(self.project.is_dir())

    async def test_K3_shared_folders_and_ancestors_never_offer_trash(self):
        child = self.project / 'nested'
        child.mkdir()
        for path in (self.project, child, self.projects):
            with self.subTest(path=path):
                self.add_shared_topic(path, enabled=False)
                request_id, card = await self.confirmation()
                self.assertNotIn('Close and move folder to Trash', self.card_labels(card))
                self.assertIn('Shared project', card['text'])
                self.tap(card, 'Cancel')
                await self.finish_close(request_id)
                with self.store.db:
                    self.store.db.execute('UPDATE topics SET cwd=? WHERE id=?', ('', '-10042:10'))

    async def test_K3_outside_root_root_itself_symlink_and_missing_folder_never_offer_trash(self):
        outside = self.root / 'outside'
        outside.mkdir()
        link = self.projects / 'link'
        link.symlink_to(self.project, target_is_directory=True)
        for folder in (outside, self.projects, link, self.projects / 'missing'):
            with self.subTest(folder=folder):
                with self.store.db:
                    self.store.db.execute('UPDATE topics SET cwd=? WHERE id=?',
                                          (str(folder), self.closing_topic))
                request_id, card = await self.confirmation()
                self.assertNotIn('Close and move folder to Trash', self.card_labels(card))
                self.assertIn(str(folder), card['text'])
                self.tap(card, 'Cancel')
                await self.finish_close(request_id)

    async def test_K3_git_card_reports_branch_dirty_untracked_and_commits_without_remote(self):
        self.initialize_git()
        tracked = self.project / 'tracked.txt'
        tracked.write_text('original\n')
        self.git('add', 'tracked.txt')
        self.git('-c', 'core.hooksPath=/dev/null', '-c', 'commit.gpgSign=false',
                 'commit', '-qm', 'Add tracked file')
        tracked.write_text('changed\n')
        (self.project / 'untracked.txt').write_text('untracked\n')
        request_id, card = await self.confirmation()
        self.assertIn('main', card['text'])
        self.assertIn('tracked.txt', card['text'])
        self.assertIn('untracked.txt', card['text'])
        self.assertIn('remote', card['text'].lower())
        self.assertIn('2', card['text'])
        self.tap(card, 'Cancel')
        await self.finish_close(request_id)

    async def test_K4_confirm_disables_stops_drops_retires_then_deletes_and_keeps_notes(self):
        task = self.store.task_create(self.closing_topic, 'Dropped job', notes='Retained notes.')
        session = await self.service.start_session(self.closing_topic)
        request_id, card = await self.confirmation()
        with self.store.db:
            outgoing = self.store.enqueue_report(self.closing_topic, 'An unsent report.')
        original_stop = session.stop
        original_call = self.telegram.call
        effects = []

        async def stop():
            self.assertEqual(self.store.topic(self.closing_topic)['enabled'], 0)
            self.assertEqual(self.store.task_get(task['id'])['status'], 'open')
            effects.append('stop')
            await original_stop()

        async def telegram_call(method, **params):
            if method == 'deleteForumTopic':
                self.assertTrue(session.closed)
                self.assertEqual(self.store.task_get(task['id'])['status'], 'dropped')
                row = self.store.db.execute('SELECT * FROM outbox WHERE id=?', (outgoing,)).fetchone()
                self.assertEqual(row['retired'], 1)
                self.assertEqual(params, {'chat_id': -10042, 'message_thread_id': 9})
                effects.append('delete')
            return await original_call(method, **params)

        session.stop = stop
        self.telegram.call = telegram_call
        self.tap(card, 'Close, keep folder')
        settled = await self.finish_close(request_id)
        self.assertEqual(settled['state'], 'done')
        self.assertEqual(effects, ['stop', 'delete'])
        session.stop = original_stop
        self.assertEqual(self.store.task_get(task['id'])['notes'], 'Retained notes.')
        self.assertTrue(self.project.is_dir())
        self.assertIsNotNone(self.store.topic(self.closing_topic))
        result_event = self.store.db.execute(
            "SELECT text FROM messages WHERE topic=? AND kind='event' ORDER BY id DESC LIMIT 1", (TOPIC,)).fetchone()
        self.assertIsNotNone(result_event)
        self.assertIn('Closing project', result_event['text'])

    async def test_K4_telegram_delete_refusal_closes_topic_and_reports_manual_deletion(self):
        original_call = self.telegram.call

        async def telegram_call(method, **params):
            if method == 'deleteForumTopic':
                self.telegram.calls.append((method, params))
                raise TelegramError(403)
            return await original_call(method, **params)

        self.telegram.call = telegram_call
        request_id, card = await self.confirmation()
        self.tap(card, 'Close, keep folder')
        self.assertEqual((await self.finish_close(request_id))['state'], 'done')
        methods = [method for method, params in self.telegram.calls]
        self.assertEqual(methods, ['deleteForumTopic', 'closeForumTopic'])
        text = '\n'.join(row['text'] for row in self.store.db.execute('SELECT text FROM outbox WHERE topic=?', (TOPIC,)))
        self.assertIn('closed', text.lower())
        self.assertIn('403', text)
        self.assertTrue('hand' in text.lower() or 'manually' in text.lower())
        self.assertEqual(self.store.topic(self.closing_topic)['enabled'], 0)

    async def assert_message_close_telegram_errors(self, delete_description, close_description, outcome):
        from coordinator.control_api import call
        from coordinator.telegram import Telegram
        from urllib.error import HTTPError

        thread = 20 + self.store.db.execute('SELECT count(*) FROM topics').fetchone()[0]
        topic = '-10042:' + str(thread)
        with self.store.db:
            self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                                 (topic, -10042, thread, 'Closing project', str(self.project)))
            message = self.store.message_save(TOPIC, 'owner', 'Close that chat and keep its folder.')['id']
            queued = call(self.store, 'topic.close', {'topic': topic, 'folder': 'keep', 'message': message},
                          topic=TOPIC, source='mcp')
        self.assertTrue(queued.ok, queued.text)
        self.assertEqual(queued.state, 'queued')
        descriptions = {'deleteForumTopic': delete_description, 'closeForumTopic': close_description}
        calls = []
        api = object.__new__(Telegram)
        api._token = 'fake-topic-test-token'

        def respond(request, timeout):
            method = request.full_url.rsplit('/', 1)[-1]
            calls.append((method, json.loads(request.data)))
            description = descriptions[method]
            if description:
                body = json.dumps({'ok': False, 'error_code': 400, 'description': description}).encode()
                raise HTTPError('https://telegram.invalid', 400, 'Bad Request', {}, BytesIO(body))
            return BytesIO(b'{"ok":true,"result":true}')

        with patch.object(self.telegram, 'call', api.call), \
                patch('coordinator.telegram.opener.open', side_effect=respond):
            settled = await self.finish_close(queued.data['request'])
        self.assertEqual(settled['state'], 'done')
        progress = json.loads(settled['result'])
        self.assertEqual(progress['outcome'], outcome)
        self.assertEqual(self.store.get('topic_lifecycle:' + topic),
                         {'state': outcome, 'request': queued.data['request']})
        expected_calls = [('deleteForumTopic', {'chat_id': -10042, 'message_thread_id': thread})]
        if outcome != 'deleted' or close_description:
            expected_calls.append(('closeForumTopic', {'chat_id': -10042, 'message_thread_id': thread}))
        self.assertEqual(calls, expected_calls)
        expected = ('Chat "Closing project" ' + outcome +
                    '. Its agent stopped. Open jobs were marked dropped.\nNone.')
        if outcome != 'deleted':
            expected += '\nThe topic still needs deleting by hand: Telegram request failed (400)'
            if outcome == 'disabled':
                expected += '\nClosing it also failed: Telegram request failed (400)'
        expected += '\nFolder kept: ' + str(self.project)
        self.assertEqual(progress['text'], expected)
        report = self.store.db.execute('SELECT text FROM outbox WHERE topic=? AND kind=? ORDER BY id DESC LIMIT 1',
                                       (TOPIC, 'report')).fetchone()
        self.assertEqual(report['text'], expected)
        self.assertEqual(self.store.topic(topic)['enabled'], 0)
        for description in descriptions.values():
            if description:
                self.assertNotIn(description, settled['result'])

    async def test_message_close_delete_error_descriptions(self):
        cases = [('Bad Request: TOPIC_ID_INVALID', 'deleted'),
                 ('Bad Request: TOPIC_DELETED', 'deleted'),
                 ('Bad Request: message thread not found', 'deleted'),
                 ('Bad Request: not enough rights to delete the topic', 'closed'),
                 ('Bad Request: CHAT_ADMIN_REQUIRED', 'closed'),
                 ('Bad Request: CHAT_WRITE_FORBIDDEN', 'closed'),
                 ('Bad Request: the chat is not a forum', 'closed'),
                 ('Bad Request: the chat is not a supergroup forum', 'closed'),
                 ('Bad Request: CHANNEL_FORUM_MISSING', 'closed'),
                 ('Bad Request: invalid forum topic identifier specified', 'closed'),
                 ('Bad Request: unrecognized topic failure', 'closed')]
        for description, outcome in cases:
            with self.subTest(description=description):
                await self.assert_message_close_telegram_errors(description, None, outcome)

    async def test_message_close_fallback_error_descriptions(self):
        cases = [('Bad Request: TOPIC_ID_INVALID', 'deleted'),
                 ('Bad Request: TOPIC_DELETED', 'deleted'),
                 ('Bad Request: message thread not found', 'deleted'),
                 ('Bad Request: not enough rights to close or open the topic', 'disabled'),
                 ('Bad Request: CHAT_ADMIN_REQUIRED', 'disabled'),
                 ('Bad Request: CHAT_WRITE_FORBIDDEN', 'disabled'),
                 ('Bad Request: the chat is not a forum', 'disabled'),
                 ('Bad Request: the chat is not a supergroup forum', 'disabled'),
                 ('Bad Request: CHANNEL_FORUM_MISSING', 'disabled'),
                 ('Bad Request: invalid forum topic identifier specified', 'disabled'),
                 ('Bad Request: unrecognized topic failure', 'disabled')]
        for description, outcome in cases:
            with self.subTest(description=description):
                await self.assert_message_close_telegram_errors(
                    'Bad Request: not enough rights to delete the topic', description, outcome)

    async def test_K5_only_owner_can_confirm_and_second_owner_tap_is_already_done(self):
        request_id, card = await self.confirmation()
        self.assertEqual(self.tap(card, 'Close, keep folder', user=8), 'unauthorized')
        await self.service.controls_once()
        self.assertEqual(self.request(request_id)['state'], 'confirming')
        self.assertEqual(self.store.topic(self.closing_topic)['enabled'], 1)
        self.assertFalse(self.telegram.calls)
        self.tap(card, 'Close, keep folder')
        await self.finish_close(request_id)
        calls = list(self.telegram.calls)
        self.assertEqual(self.tap(card, 'Close, keep folder'), 'already_done')
        await self.service.controls_once()
        self.assertEqual(self.telegram.calls, calls)

    async def test_K5_cancel_changes_no_topic_or_folder_or_jobs(self):
        task = self.store.task_create(self.closing_topic, 'Still open', notes='Still here.')
        request_id, card = await self.confirmation()
        self.tap(card, 'Cancel')
        await self.finish_close(request_id)
        self.assertEqual(self.store.topic(self.closing_topic)['enabled'], 1)
        self.assertEqual(self.store.task_get(task['id'])['status'], 'open')
        self.assertTrue(self.project.is_dir())
        self.assertFalse(self.telegram.calls)

    async def test_K6_another_chat_claiming_folder_refreshes_card_without_closing(self):
        request_id, card = await self.confirmation()
        self.add_shared_topic(self.project)
        self.tap(card, 'Close and move folder to Trash')
        await self.service.controls_once()
        refreshed = self.latest_close_card()
        self.assertNotEqual(refreshed['id'], card['id'])
        self.assertNotIn('Close and move folder to Trash', self.card_labels(refreshed))
        self.assertEqual(self.store.topic(self.closing_topic)['enabled'], 1)
        self.assertTrue(self.project.is_dir())
        self.assertFalse(self.telegram.calls)

    async def test_K6_modified_git_content_with_same_status_refreshes_card_without_closing(self):
        self.initialize_git()
        tracked = self.project / 'tracked.txt'
        tracked.write_text('base\n')
        self.git('add', 'tracked.txt')
        self.git('-c', 'core.hooksPath=/dev/null', '-c', 'commit.gpgSign=false',
                 'commit', '-qm', 'Tracked')
        tracked.write_text('first change\n')
        before_status = self.git('status', '--porcelain=v1')
        request_id, card = await self.confirmation()
        tracked.write_text('second change\n')
        self.assertEqual(self.git('status', '--porcelain=v1'), before_status)
        self.tap(card, 'Close, keep folder')
        await self.service.controls_once()
        self.assertNotEqual(self.latest_close_card()['id'], card['id'])
        self.assertEqual(self.store.topic(self.closing_topic)['enabled'], 1)
        self.assertFalse(self.telegram.calls)

    async def test_K6_new_job_refreshes_card_without_dropping_it(self):
        request_id, card = await self.confirmation()
        task = self.store.task_create(self.closing_topic, 'Created after card')
        self.tap(card, 'Close, keep folder')
        await self.service.controls_once()
        refreshed = self.latest_close_card()
        self.assertNotEqual(refreshed['id'], card['id'])
        self.assertIn(task['title'], refreshed['text'])
        self.assertEqual(self.store.task_get(task['id'])['status'], 'open')
        self.assertEqual(self.store.topic(self.closing_topic)['enabled'], 1)
        self.assertFalse(self.telegram.calls)

    async def test_K6_worker_start_after_card_posts_fresh_card_and_changes_nothing(self):
        task = self.store.task_create(self.closing_topic, 'A worker may start')
        request_id, card = await self.confirmation()
        with self.store.db:
            worker = self.store.db.execute('''INSERT INTO workers
                (task,topic,provider,prompt,cwd,status,created,updated)
                VALUES (?,?,?,?,?,'running',?,?)''',
                (task['id'], self.closing_topic, 'opencode', 'Just started', str(self.project), time.time(), time.time()))
        self.tap(card, 'Close, keep folder')
        await self.service.controls_once()
        refreshed = self.store.db.execute('''SELECT * FROM outbox
            WHERE topic=? AND reply_markup IS NOT NULL ORDER BY id DESC LIMIT 1''', (TOPIC,)).fetchone()
        self.assertNotEqual(refreshed['id'], card['id'])
        self.assertIn(str(worker.lastrowid), refreshed['text'])
        self.assertEqual(self.request(request_id)['state'], 'confirming')
        self.assertEqual(self.store.topic(self.closing_topic)['enabled'], 1)
        self.assertEqual(self.store.task_get(task['id'])['status'], 'open')
        self.assertFalse(self.telegram.calls)

    async def test_K7_confirmed_close_survives_service_restart_before_execution(self):
        request_id, card = await self.confirmation()
        self.tap(card, 'Close, keep folder')
        self.store.requests_uncertain()
        self.service = Service(self.store, self.telegram, self.runner, self.root,
                               session_factory=self.service.session_factory)
        self.assertEqual((await self.finish_close(request_id))['state'], 'done')
        self.assertEqual([method for method, params in self.telegram.calls].count('deleteForumTopic'), 1)
        self.assertEqual(self.store.topic(self.closing_topic)['enabled'], 0)

    async def test_K7_restart_stops_persisted_host_without_launching_another_parent(self):
        from coordinator.host import HostClient
        request_id, card = await self.confirmation()
        key = self.service.session_key(self.closing_topic)
        with self.store.db:
            self.store.put(key + '_host', {'id': 'fixture-parent-host', 'provider': 'codex',
                                         'account': 'fixture', 'model': 'fixture'})
            self.store.put(key + '_session', 'native-session-stays')
        self.tap(card, 'Close, keep folder')
        self.store.requests_uncertain()
        factory = Mock(side_effect=AssertionError('Closing must not launch a parent.'))
        self.service = Service(self.store, self.telegram, self.runner, self.root, session_factory=factory)
        with patch.object(HostClient, 'stop', new_callable=AsyncMock) as stop:
            self.assertEqual((await self.finish_close(request_id))['state'], 'done')
        stop.assert_awaited_once()
        factory.assert_not_called()
        self.assertEqual(self.store.get(key + '_session'), 'native-session-stays')

    async def crash_close_at(self, phase):
        from coordinator import topics

        class SimulatedCrash(BaseException):
            pass

        payload = b'Restart-safe project evidence.\n'
        (self.project / 'evidence.bin').write_bytes(payload)
        session = await self.service.start_session(self.closing_topic)
        self.store.task_create(self.closing_topic, 'Drop after restart', notes='Keep after restart.')
        request_id, card = await self.confirmation()
        self.tap(card, 'Close and move folder to Trash')
        original_save = topics._save
        original_call = self.telegram.call
        external_deleted = False
        crashed = False

        def interrupt_save(store, request, progress, state='queued'):
            nonlocal crashed
            original_save(store, request, progress, state)
            if progress.get('phase') == phase and not crashed:
                crashed = True
                raise SimulatedCrash()

        async def telegram_call(method, **params):
            nonlocal external_deleted
            if method == 'deleteForumTopic':
                if external_deleted:
                    self.telegram.calls.append((method, params))
                    raise TelegramError(400, reason='thread_not_found')
                external_deleted = True
            return await original_call(method, **params)

        self.telegram.call = telegram_call
        with patch.object(topics, '_save', side_effect=interrupt_save):
            with self.assertRaises(SimulatedCrash):
                await self.finish_close(request_id)
        self.assertTrue(crashed)
        self.store.requests_uncertain()
        self.service = Service(self.store, self.telegram, self.runner, self.root,
                               session_factory=self.service.session_factory)
        settled = await self.finish_close(request_id)
        self.assertEqual(settled['state'], 'done')
        self.assertEqual(json.loads(settled['result'])['outcome'], 'deleted')
        self.assertEqual(self.store.topic(self.closing_topic)['enabled'], 0)
        self.assertTrue(session.closed)
        self.assertFalse(self.project.exists())
        destinations = [path for path in self.trash.iterdir() if (path / 'evidence.bin').is_file()]
        self.assertEqual(len(destinations), 1)
        self.assertEqual((destinations[0] / 'evidence.bin').read_bytes(), payload)
        self.assertEqual(len(self.store.db.execute(
            "SELECT id FROM messages WHERE topic=? AND kind='event'", (TOPIC,)).fetchall()), 1)
        self.assertFalse(any(method == 'closeForumTopic' for method, params in self.telegram.calls))

    async def test_K7_crash_after_parent_stop_reconciles(self):
        await self.crash_close_at('stopped')

    async def test_K7_crash_while_dropping_jobs_reconciles(self):
        await self.crash_close_at('dropped')

    async def test_K7_crash_while_retiring_outbox_reconciles(self):
        await self.crash_close_at('retired')

    async def test_K7_crash_after_telegram_delete_treats_missing_topic_as_success(self):
        await self.crash_close_at('telegram')

    async def test_K7_crash_after_trash_rename_does_not_move_a_second_folder(self):
        await self.crash_close_at('folder')

    async def test_K7_crash_while_reporting_does_not_duplicate_result_event(self):
        await self.crash_close_at('finished')

    async def test_K8_trash_move_uses_suffix_preserves_bytes_and_never_hard_deletes_folder(self):
        payload = b'\x00retained project bytes\xff\n'
        (self.project / 'evidence.bin').write_bytes(payload)
        source_identity = self.project.stat()
        existing = self.trash / self.project.name
        existing.mkdir()
        (existing / 'older.txt').write_text('Older folder stays.')
        request_id, card = await self.confirmation()
        self.tap(card, 'Close and move folder to Trash')
        original_unlink = Path.unlink

        def unlink(path, *args, **kwargs):
            if path == self.project or self.project in path.parents:
                raise AssertionError('Close must not unlink a project folder or its files.')
            return original_unlink(path, *args, **kwargs)

        with patch('shutil.rmtree', side_effect=AssertionError('Close must never hard-delete a folder.')):
            with patch.object(Path, 'unlink', new=unlink):
                self.assertEqual((await self.finish_close(request_id))['state'], 'done')
        self.assertFalse(self.project.exists())
        self.assertEqual((existing / 'older.txt').read_text(), 'Older folder stays.')
        moved = [path for path in self.trash.iterdir() if path != existing]
        self.assertEqual(len(moved), 1)
        self.assertEqual(moved[0].name, self.project.name + '-1')
        self.assertEqual((moved[0] / 'evidence.bin').read_bytes(), payload)
        moved_identity = moved[0].stat()
        self.assertEqual((moved_identity.st_dev, moved_identity.st_ino),
                         (source_identity.st_dev, source_identity.st_ino))

    async def test_K9_deleted_topic_cannot_enable_restart_parent_or_retry_outbox(self):
        from coordinator.control_api import call
        with self.store.db:
            outgoing = self.store.enqueue_report(self.closing_topic, 'Pending before deletion.')
        request_id, card = await self.confirmation()
        self.tap(card, 'Close, keep folder')
        await self.finish_close(request_id)
        with self.store.db:
            result = call(self.store, 'topic.enable', {'topic': self.closing_topic})
        self.assertFalse(result.ok)
        self.assertEqual(self.store.topic(self.closing_topic)['enabled'], 0)
        self.assertIsNone(await self.service.start_session(self.closing_topic))
        self.assertEqual(self.store.db.execute('SELECT retired FROM outbox WHERE id=?', (outgoing,)).fetchone()[0], 1)
        self.assertIsNone(self.store.db.execute('''SELECT 1 FROM outbox
            WHERE topic=? AND delivered=0 AND retired=0''', (self.closing_topic,)).fetchone())

    async def test_K10_closing_asking_chat_sends_outcome_and_event_to_home(self):
        request_id, card = await self.confirmation(asking=self.closing_topic)
        self.tap(card, 'Close, keep folder')
        await self.finish_close(request_id)
        home_reports = [row['text'] for row in self.store.db.execute('SELECT text FROM outbox WHERE topic=?', (TOPIC,))]
        self.assertTrue(any('Closing project' in text for text in home_reports))
        self.assertIsNotNone(self.store.db.execute(
            "SELECT 1 FROM messages WHERE topic=? AND kind='event'", (TOPIC,)).fetchone())
        self.assertIsNone(self.store.db.execute('''SELECT 1 FROM outbox
            WHERE topic=? AND delivered=0 AND retired=0''', (self.closing_topic,)).fetchone())

    async def test_K2_closing_already_disabled_chat_has_same_confirmation_and_finishes(self):
        with self.store.db:
            self.store.db.execute('UPDATE topics SET enabled=0 WHERE id=?', (self.closing_topic,))
        request_id, card = await self.confirmation()
        self.tap(card, 'Close, keep folder')
        self.assertEqual((await self.finish_close(request_id))['state'], 'done')
        self.assertEqual(self.store.topic(self.closing_topic)['enabled'], 0)
        self.assertTrue(self.project.is_dir())


class FeedStallTests(ServiceTestsSupport, unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self):
        await self.service.shutdown([])
        await super().asyncTearDown()

    async def idle_chat(self, files=0):
        with self.store.db:
            self.store.put('close_clears_space', True)
            other = '-10042:5'
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', (other, -10042, 5, 'Ideas', str(self.root / 'ideas')))
        self.store.message_save(other, 'owner', 'idea')
        await self.drain_feed()
        path = close_saves.scratch(self.store, topic=other)
        (path / 'many-files').mkdir()
        for index in range(files):
            (path / 'many-files' / ('f%d' % index)).write_bytes(b'x')
        return other, path

    async def test_large_idle_chat_scratch_does_not_delay_another_chat(self):
        other, path = await self.idle_chat(1)
        message = self.store.message_save(TOPIC, 'owner', 'continue in another chat')
        deleting, deleted, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        original = workspaces.delete_tree

        async def delete(target, **options):
            self.assertEqual(target, path)
            deleting.set()
            await release.wait()
            await original(target, **options)
            deleted.set()

        with patch('coordinator.workspaces.delete_tree', delete):
            try:
                with patch('coordinator.service.PARENT_IDLE', 0):
                    await settle(self.service.feed_once())
                await settle(asyncio.gather(*self.service.feed_tasks.values()))
                await settle(deleting.wait())
                self.assertFalse(deleted.is_set(), 'another chat waited for scratch deletion')
                self.assertEqual(self.store.db.execute('SELECT delivered FROM messages WHERE id=?',
                                                       (message['id'],)).fetchone()[0], 'received')
                self.assertIn(other, self.service.wound_down)
            finally:
                release.set()
            await settle(deleted.wait())
        self.assertFalse(path.exists())

    async def test_switch_off_starts_no_chat_scratch_clear(self):
        other, path = await self.idle_chat()
        with self.store.db:
            self.store.put('close_clears_space', False)
        with patch('coordinator.close_saves.clear_chat_scratch', new_callable=AsyncMock) as clear:
            with patch('coordinator.service.PARENT_IDLE', 0):
                await self.service.feed_once()
            await asyncio.sleep(0)
            clear.assert_not_called()
        self.assertIn(other, self.service.wound_down)
        self.assertTrue(path.exists())

    async def test_one_scratch_clear_per_chat_until_it_finishes(self):
        other, path = await self.idle_chat()
        started = asyncio.Queue()
        release = asyncio.Event()
        calls = []

        async def clear(store, topic):
            calls.append(topic)
            started.put_nowait(topic)
            await release.wait()

        with patch('coordinator.close_saves.clear_chat_scratch', clear):
            with patch('coordinator.service.PARENT_IDLE', 0):
                await self.service.feed_once()
            self.assertEqual(await asyncio.wait_for(started.get(), 2), other)
            self.store.message_save(other, 'owner', 'wake up while clearing')
            self.store.message_save(TOPIC, 'owner', 'another chat')
            await self.drain_feed()
            with patch('coordinator.service.PARENT_IDLE', 0):
                await self.service.feed_once()
            self.assertEqual(await asyncio.wait_for(started.get(), 2), TOPIC)
            self.assertEqual(calls.count(other), 1)
            self.assertEqual(calls.count(TOPIC), 1)
            release.set()
            await asyncio.gather(*self.service.chat_scratch_tasks.values())
            self.assertFalse(self.service.chat_scratch_tasks)
            self.store.message_save(other, 'owner', 'wake up after clearing')
            await self.drain_feed()
            with patch('coordinator.service.PARENT_IDLE', 0):
                await self.service.feed_once()
            self.assertEqual(await asyncio.wait_for(started.get(), 2), other)
            self.assertEqual(calls.count(other), 2)

    async def test_chat_scratch_errors_are_logged_and_recorded_without_stopping_delivery(self):
        other, path = await self.idle_chat()
        for error in (OSError('fixture scratch delete failed'), RuntimeError('fixture unexpected delete failure')):
            with self.subTest(error=type(error).__name__):
                self.store.message_save(other, 'owner', 'wake up to retry cleanup')
                await self.drain_feed()
                message = self.store.message_save(TOPIC, 'owner', 'delivery despite cleanup failure')
                with patch('coordinator.workspaces.delete_tree', side_effect=error), \
                        self.assertLogs('coordinator.problems', 'WARNING') as logs:
                    with patch('coordinator.service.PARENT_IDLE', 0):
                        await self.service.feed_once()
                    await asyncio.gather(*self.service.chat_scratch_tasks.values())
                    await asyncio.gather(*self.service.feed_tasks.values())
                failures = [row for row in self.problems() if row['topic'] == other]
                self.assertEqual(failures[-1]['area'], 'workspaces')
                self.assertEqual(failures[-1]['code'], 'chat-scratch-delete-failed')
                self.assertIn(str(error), failures[-1]['detail'])
                self.assertTrue(any('chat-scratch-delete-failed' in line for line in logs.output))
                self.assertEqual(self.store.db.execute('SELECT delivered FROM messages WHERE id=?',
                                                       (message['id'],)).fetchone()[0], 'received')
                self.assertFalse(self.service.chat_scratch_tasks)
                self.assertTrue(path.exists())

    async def test_shutdown_cancels_and_joins_chat_scratch_clear(self):
        other, path = await self.idle_chat()
        started, cancelled = asyncio.Event(), asyncio.Event()

        async def clear(store, topic):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        with patch('coordinator.close_saves.clear_chat_scratch', clear):
            with patch('coordinator.service.PARENT_IDLE', 0):
                await self.service.feed_once()
            await asyncio.wait_for(started.wait(), 2)
            task = self.service.chat_scratch_tasks[other]
            await self.service.shutdown([])
        self.assertTrue(cancelled.is_set())
        self.assertTrue(task.cancelled())
        self.assertFalse(self.service.chat_scratch_tasks)
        self.assertEqual(self.problems(), [])
        self.assertTrue(path.exists())

    async def test_idle_wind_down_does_not_stall_the_feed_while_space_loop_deletes(self):
        with self.store.db:
            self.store.put('close_clears_space', True)
        other = '-10042:5'
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', (other, -10042, 5, 'Ideas', str(self.root / 'ideas')))
        self.store.message_save(other, 'owner', 'idea')
        await self.drain_feed()
        self.assertTrue(close_saves.scratch(self.store, topic=other).is_dir())
        held = asyncio.Event()

        async def space_loop_deleting():
            async with workspaces._repository_lock(self.store.directory, 'closed-copy-deletion'):
                held.set()
                await asyncio.sleep(3)

        holder = asyncio.create_task(space_loop_deleting())
        await held.wait()
        started = time.monotonic()
        with patch('coordinator.service.PARENT_IDLE', 0):
            await self.service.feed_once()
        elapsed = time.monotonic() - started
        await holder
        self.assertLess(elapsed, 1.0, 'feed_once (all chats) stalled %.1f s waiting for the deletion lock' % elapsed)
