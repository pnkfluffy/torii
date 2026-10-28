import asyncio
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

from coordinator.account_status import refresh_accounts
from coordinator.providers import ProviderRunner, RunResult
from coordinator.service import Service
from coordinator.store import Store
from tests.support import isolate_shared_mcp_home, read_jsonl, settle, until, stop_test_hosts


TOPIC = '-10042:4'

FAKE = '''#!/usr/bin/env python3
import json,os,sys,time
from pathlib import Path
home=Path(os.environ['CLAUDE_CONFIG_DIR'])
with (Path(__file__).parent/'launches.jsonl').open('a') as log:
    log.write(json.dumps({'home':home.name,'argv':sys.argv[1:]})+'\\n')
if '--session-id' in sys.argv:
    sid=sys.argv[sys.argv.index('--session-id')+1]
    transcript=home/'projects'/'p'/(sid+'.jsonl')
    transcript.parent.mkdir(parents=True,exist_ok=True)
    transcript.write_text('{}\\n')
else:
    sid=Path(sys.argv[sys.argv.index('--resume')+1]).name[:-6]
print(json.dumps({'type':'system','subtype':'init','session_id':sid}),flush=True)
for line in sys.stdin:
    print(json.dumps({**json.loads(line),'isReplay':True}),flush=True)
    if home.name in ('primary','first') and not (Path(__file__).parent/'rejected').exists():
        print(json.dumps({'type':'rate_limit_event','session_id':sid,'rate_limit_info':
            {'status':'rejected','resetsAt':int(time.time())+3600,'rateLimitType':'five_hour'}}),flush=True)
        (Path(__file__).parent/'rejected').write_text('1')
        time.sleep(120)
        sys.exit(1)
    print(json.dumps({'type':'result','subtype':'success','session_id':sid,'result':'resumed'}),flush=True)
'''


class QuotaWaitTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)
        isolate_shared_mcp_home(self, self.root)
        self.addCleanup(stop_test_hosts, self.root)
        self.store = Store(self.root / 'state')
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', (TOPIC, -10042, 4, 'Torii', str(self.root)))
            self.store.put('coordinator_home_topic', TOPIC)
        self.task = self.store.task_create(TOPIC, 'Build', worktree=str(self.root))

    async def asyncTearDown(self):
        self.store.close()

    def accounts(self, aliases):
        now = time.time()
        with self.store.db:
            for alias in aliases:
                (self.root / alias).mkdir(exist_ok=True)
            self.store.put('accounts', {alias: {'config_dir': str(self.root / alias), 'enabled': True}
                                        for alias in aliases})
            self.store.put('account_status', {alias: {'identity': {'email': alias + '@example.com', 'logged_in': True},
                'observed_at': now, 'usage': {'five_hour': {'utilization': 10, 'resets_at': now + 1800},
                'seven_day': {'utilization': 10, 'resets_at': now + 3600 * (index + 1)},
                'seven_day_fable': {'utilization': 10, 'resets_at': now + 3600 * (index + 1)}}}
                for index, alias in enumerate(aliases)})

    def worker(self):
        with self.store.db:
            row = self.store.db.execute('''INSERT INTO workers
                (task,topic,provider,prompt,cwd,workspace,goal,model,created,updated)
                VALUES (?,?,?,?,?,?,?,?,?,?)''',
                (self.task['id'], TOPIC, 'claude', 'build this', str(self.root),
                 json.dumps({'cwd': str(self.root)}), None, None, 1.0, 1.0))
        return dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (row.lastrowid,)).fetchone())

    def callbacks(self):
        return [row[0] for row in self.store.db.execute("SELECT text FROM messages WHERE kind='callback' ORDER BY id")]

    async def test_steers_on_a_parked_worker_send_no_extra_wait_notices(self):
        self.accounts(['primary'])
        sid = '20000000-0000-4000-8000-000000000091'
        transcript = self.root / 'primary' / 'projects' / 'test' / (sid + '.jsonl')
        transcript.parent.mkdir(parents=True)
        transcript.write_text('{}\n')
        calls = []
        reset = time.time() + 3600

        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                calls.append(prompt)
                return RunResult(session, quota_limited=True, transcript_path=str(transcript),
                                 rate_limit_info={'status': 'rejected', 'rateLimitType': 'five_hour',
                                                  'resetsAt': reset})

        service = Service(self.store, None, Runner(), self.root)
        worker = self.worker()
        waiting = await service.workers.run(worker, self.store.topic(TOPIC))
        self.assertEqual(waiting['status'], 'waiting_for_quota')
        self.assertEqual(self.callbacks(), [])
        for text in ('first follow-up', 'second follow-up'):
            request = self.store.service_request('workers.steer', {'worker': worker['id'], 'prompt': text})
            await service.controls_once()
            self.assertEqual(self.store.db.execute('SELECT state FROM service_requests WHERE id=?',
                                                   (request,)).fetchone()[0], 'done')
            await service.workers_once()
            await asyncio.gather(*service.worker_tasks.values())
        self.assertEqual(service.workers.get(worker['id'])['status'], 'waiting_for_quota')
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.callbacks(), [])
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox WHERE topic=?', (TOPIC,)).fetchone()[0], 1)

    async def test_a_worker_that_parks_after_its_job_closed_does_not_relaunch(self):
        self.accounts(['primary'])
        sid = '20000000-0000-4000-8000-000000000092'
        transcript = self.root / 'primary' / 'projects' / 'test' / (sid + '.jsonl')
        transcript.parent.mkdir(parents=True)
        transcript.write_text('{}\n')
        calls = []
        store, task = self.store, self.task

        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                calls.append(prompt)
                if len(calls) == 1:
                    store.task_update(task['id'], status='done')
                    return RunResult(session, quota_limited=True, transcript_path=str(transcript),
                                     rate_limit_info={'status': 'rejected', 'rateLimitType': 'five_hour',
                                                      'resetsAt': time.time() + 3600})
                return RunResult(session, success=True, transcript_path=str(transcript))

        service = Service(self.store, None, Runner(), self.root)
        worker = self.worker()
        await service.workers.run(worker, self.store.topic(TOPIC))
        with self.store.db:
            self.store.put('account_blocks', {})
        await service.workers_once()
        await asyncio.gather(*service.worker_tasks.values())
        self.assertEqual((service.workers.get(worker['id'])['status'], len(calls)), ('interrupted', 1))

    async def stop_during_rejection(self, aliases):
        self.accounts(aliases)
        binary = self.root / 'fake-claude'
        binary.write_text(FAKE)
        binary.chmod(0o700)
        runner = ProviderRunner(self.store.directory, {'claude': str(binary)})
        service = Service(self.store, None, runner, self.root)
        self.service = service
        worker = self.worker()
        await service.workers_once()
        await until((self.root / 'rejected').exists, 'the fake claude to emit a rejected rate limit')
        await until(lambda: service.workers.get(worker['id'])['last_seq'] >= 3, 'the rejection to be consumed')
        self.assertEqual(service.workers.get(worker['id'])['status'], 'running')
        stop = self.store.service_request('workers.stop', {'worker': worker['id']})
        await asyncio.wait_for(service.controls_once(), 30)
        request = self.store.db.execute('SELECT state,result FROM service_requests WHERE id=?', (stop,)).fetchone()
        final = service.workers.get(worker['id'])
        launches = [launch['home'] for launch in read_jsonl(self.root / 'launches.jsonl')]
        return request, final, launches

    async def test_stop_after_a_rejection_interrupts_the_worker(self):
        request, final, launches = await self.stop_during_rejection(['primary'])
        self.assertEqual(request[0], 'done')
        self.assertEqual(final['status'], 'interrupted')
        self.assertEqual(self.callbacks(), [])

    async def test_a_stopped_worker_does_not_resume_by_itself_after_the_reset(self):
        request, final, launches = await self.stop_during_rejection(['primary'])
        self.assertEqual(request[0], 'done')
        payload = json.loads(final['result'])
        payload['waiting_until'] = time.time() - 1
        with self.store.db:
            self.store.db.execute('UPDATE workers SET result=? WHERE id=?', (json.dumps(payload), final['id']))
        self.accounts(['primary'])
        with self.store.db:
            self.store.put('account_blocks', {})
        await self.service.workers_once()
        await asyncio.wait_for(asyncio.gather(*self.service.worker_tasks.values()), 30)
        after = self.service.workers.get(final['id'])
        launches = [launch['home'] for launch in read_jsonl(self.root / 'launches.jsonl')]
        self.assertEqual(launches, ['primary'])
        self.assertEqual(after['status'], 'interrupted')

    async def test_stop_after_a_rejection_does_not_relaunch_on_another_account(self):
        request, final, launches = await self.stop_during_rejection(['first', 'second'])
        self.assertEqual(request[0], 'done')
        self.assertEqual(launches, ['first'])
        self.assertEqual(final['status'], 'interrupted')


class WaitNoticeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(stop_test_hosts, self.root)
        self.store = Store(self.root / 'state')
        self.now = time.time()
        self.reset_a = self.now + 100.187177
        for alias in ('a', 'b'):
            (self.root / alias).mkdir()
        with self.store.db:
            self.store.put('accounts', {alias: {'config_dir': str(self.root / alias), 'enabled': True}
                                        for alias in ('a', 'b')})
            self.store.put('account_status', {
                'a': {'identity': {'email': 'a@example.com', 'logged_in': True}, 'observed_at': self.now,
                      'usage': {'five_hour': {'utilization': 100, 'resets_at': self.reset_a},
                                'seven_day': {'utilization': 40, 'resets_at': self.now + 86400.187197}}},
                'b': {'identity': {'email': 'b@example.com', 'logged_in': True}, 'observed_at': self.now,
                      'usage': {'five_hour': {'utilization': 100, 'resets_at': self.now + 7200.253114},
                                'seven_day': {'utilization': 40, 'resets_at': self.now + 90000.253137}}}})
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', ('home', -100, 1, 'Home', str(self.root)))
            self.store.put('coordinator_home_topic', 'home')
        self.runner = ProviderRunner(self.store.directory, {'claude': str(self.root / 'missing-claude')})
        self.service = Service(self.store, None, self.runner, self.root, coordinator_model='model')

    async def asyncTearDown(self):
        self.store.close()

    def wait_notices(self):
        return [row[0] for row in self.store.db.execute('SELECT text FROM outbox ORDER BY id')
                if 'No Claude account is available' in row[0]]

    async def test_a_usage_refresh_during_the_wait_does_not_send_a_second_notice(self):
        self.store.message_save('home', 'owner', 'hello')
        await self.service.feed_once()
        self.assertEqual(len(self.wait_notices()), 1)
        self.assertEqual(self.store.get('coordinator_account_retry_at'), self.reset_a)
        snapshots = self.store.get('account_status')

        async def later_check(account):
            alias = Path(account['config_dir']).name
            usage = {key: dict(window) for key, window in snapshots[alias]['usage'].items()}
            for window in usage.values():
                window['resets_at'] += 0.4
            return {'checked_at': self.now + 300, 'observed_at': self.now + 300,
                    'identity': snapshots[alias]['identity'], 'usage': usage}

        await refresh_accounts(self.store, later_check)
        self.assertEqual(self.store.get('account_status')['a']['usage']['five_hour']['resets_at'],
                         self.reset_a + 0.4)
        with patch('coordinator.service.time.time', return_value=self.reset_a + 0.01):
            await self.service.feed_once()
        self.assertEqual(len(self.wait_notices()), 1, self.wait_notices())


class OpenCodeQuotaWaitTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        from coordinator import opencode_accounts
        from tests.test_opencode_parent import OpenCodeParentTests
        saved_home, account_home = os.environ.get('HOME'), opencode_accounts.account_home
        self.addCleanup(lambda: self.assertEqual(os.environ.get('HOME'), saved_home))
        self.addCleanup(lambda: self.assertIs(opencode_accounts.account_home, account_home))
        self.clock = Mock(wraps=time)
        self.clock_patches = [patch(module + '.time', self.clock) for module in (
            'coordinator.service', 'coordinator.opencode_subscription', 'coordinator.opencode_runtime')]
        for clock in self.clock_patches:
            self.addCleanup(clock.stop)
        self.chat = OpenCodeParentTests()
        self.chat.addCleanup = self.addCleanup
        self.chat.setUp()
        self.store = self.chat.store
        self.service = Service(self.store, None, self.chat.runner, self.chat.cwd)
        self.addAsyncCleanup(self.stop_parents)
        binary = self.chat.binary
        source = binary.read_text()
        source = source.replace("status = scenario['http_error']", """while scenario.get('error_release') and not Path(scenario['error_release']).exists():
                    time.sleep(0.01)
                status = scenario['http_error']""")
        source = source.replace("{'error': {'name': 'APIError',", """{'time': {'completed': scenario.get('completed')},
                       'error': {'name': 'APIError',""")
        source = source.replace("}[status]", "}.get(status, 'synthetic rejection')")
        binary.write_text(source)

    async def stop_parents(self):
        parents = dict.fromkeys(session for session in [*self.chat.parents, *self.service.sessions.values()]
                                if session is not None)
        results = await settle(asyncio.gather(*(session.stop() for session in parents), return_exceptions=True))
        for result in results:
            if isinstance(result, BaseException):
                raise result

    async def failed_turn(self, **scenario):
        release = self.chat.root / ('error-release-%d' % len(self.chat.parents))
        self.chat.scenario(error_message='synthetic rejection', response_body='synthetic rejection',
                           error_release=str(release), **scenario)
        session = await self.chat.start()
        self.service.sessions['chat'] = session
        row = self.chat.row()
        try:
            await self.chat.send(session, row)
        finally:
            release.touch()
        await settle(session.wait_closed())
        receipt = self.chat.persisted(row)
        self.assertEqual((receipt['delivered'], receipt['receipt']), ('received', 'written'))
        self.assertTrue(session.closed)
        self.assertIsNotNone(session.process.returncode)
        self.assertIsNone(self.store.get(self.chat.session_key + '_host'))
        self.assertEqual(session.error, {'code': -32603, 'message': 'Internal error: synthetic rejection',
                                       'data': {'service': 'session', 'errorName': 'APIError'}})
        return session

    async def paused(self, session, until):
        key = self.chat.session_key + '_selection'
        self.assertIsNone(await self.service.start_session('chat'))
        self.assertEqual(self.store.get(key + '_retry_at'), until)
        notices = self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0]
        with patch('coordinator.service.time.time', return_value=until - 1):
            self.assertIsNone(await self.service.start_session('chat'))
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], notices)
        self.store.message_save('chat', 'owner', 'saved during the pause')
        self.assertEqual(len(self.chat.calls('initialize')), len(self.chat.parents))
        self.assertEqual(self.store.get('opencode_active_account'), 'one')
        self.assertEqual(self.store.get(self.chat.session_key + '_session'), session.session_id)
        self.assertTrue(any(row['topic'] == 'chat' and row['text'] == 'saved during the pause'
                            for row in self.store.messages_pending()))

    async def wake(self, session, when):
        self.chat.scenario()
        with patch('coordinator.service.time.time', return_value=when + 1):
            resumed = await self.service.start_session('chat')
        self.assertIsNotNone(resumed)
        self.chat.parents.append(resumed)
        self.assertEqual((resumed.account_alias, resumed.session_id), ('one', session.session_id))
        self.assertEqual(self.chat.calls('session/load')[-1]['message']['params']['sessionId'], session.session_id)
        self.assertIsNone(self.store.get(self.chat.session_key + '_selection_retry_at'))
        await self.chat.send(resumed, self.chat.row('after reset'))
        await until(resumed.idle)
        await resumed.stop()

    async def test_opencode_subscription_chat_pauses_until_reset_on_same_account(self):
        from coordinator import opencode_subscription
        for clock in self.clock_patches:
            clock.start()
        now = int(time.time())
        self.clock.time.return_value = now + 0.5
        reset = now + 3600
        accounts = self.store.get('opencode_accounts')
        accounts['one'] = {'provider': 'opencode-go', 'model': 'example-model'}
        self.store.put('opencode_accounts', accounts)
        snapshot = {'observed_at': now, 'usage': {'rolling': {
            'status': 'ok', 'utilization': 10, 'resets_at': reset}}}
        self.store.put('opencode_account_status', {'one': snapshot})
        session = await self.failed_turn(http_error=429, response_headers={'retry-after': '3600'}, completed=now * 1000)
        block = self.store.get('opencode_account_blocks')['one']
        self.assertEqual((block['reason'], block['until'], block['reset_known']), ('quota', reset, True))
        self.assertGreater(self.store.get('opencode_account_status_refresh_requested'), now)
        await self.paused(session, reset)
        self.assertEqual(self.store.get(self.chat.session_key + '_selection_notice')['text'],
                         "This chat is paused: OpenCode subscription account one is out of usage until " +
                         time.strftime('%Y-%m-%d %H:%M %Z', time.localtime(reset)) +
                         ". I'll continue at the reset. Torii does not move this chat to another agent. Your messages are saved.")

        def refresh(alias):
            return dict(snapshot, observed_at=reset + 1)

        self.clock.time.return_value = reset + 1
        with patch('coordinator.opencode_subscription.collect_status', side_effect=refresh):
            await opencode_subscription.refresh_accounts(self.store)
        await self.wake(session, reset)

    async def test_opencode_subscription_reset_keeps_store_clock_real(self):
        from coordinator import opencode_subscription
        real_time = time.time
        entered, release = asyncio.Event(), asyncio.Event()
        refresh_accounts = opencode_subscription.refresh_accounts

        async def refresh(store):
            entered.set()
            await release.wait()
            return await refresh_accounts(store)

        with patch.object(opencode_subscription, 'refresh_accounts', side_effect=refresh):
            reset = asyncio.create_task(self.test_opencode_subscription_chat_pauses_until_reset_on_same_account())
            try:
                await settle(entered.wait())
                before = real_time()
                row = self.chat.row('clock observer')
                after = real_time()
                self.assertGreaterEqual(row['created'], before)
                self.assertLessEqual(row['created'], after)
            finally:
                release.set()
                await settle(reset)

    async def test_cleanup_joins_a_parent_stop_already_in_progress(self):
        self.chat.scenario()
        session = await self.chat.start()
        entered, release, joining = asyncio.Event(), asyncio.Event(), asyncio.Event()
        request, stop = session.process.request, session.stop
        cleanup = next(callback for callback, _, _ in self._cleanups if asyncio.iscoroutinefunction(callback))

        async def held_request(method, *args, **kwargs):
            if method == 'close_stdin':
                entered.set()
                await release.wait()
            return await request(method, *args, **kwargs)

        async def joined_stop():
            joining.set()
            await stop()

        with patch.object(session.process, 'request', side_effect=held_request), \
                patch.object(session, 'stop', side_effect=joined_stop):
            stopping = asyncio.create_task(stop())
            cleaning = None
            try:
                await settle(entered.wait())
                cleaning = asyncio.create_task(cleanup())
                await until(lambda: joining.is_set() or cleaning.done())
                self.assertTrue(joining.is_set(), 'Fixture cleanup returned while parent.stop() was blocked')
                self.assertFalse(cleaning.done())
                self.assertEqual(self.store.db.execute('SELECT 1').fetchone()[0], 1)
            finally:
                release.set()
                await settle(asyncio.gather(stopping, *([cleaning] if cleaning else [])))
        self.assertTrue(session.closed)
        self.assertIsNotNone(session.process.returncode)
        self.assertTrue(all(task.done() for task in session.tasks))
        self.assertIsNone(self.store.get(self.chat.session_key + '_host'))

    async def test_opencode_host_429_402_401_403_pause_and_recheck(self):
        from email.utils import formatdate
        for status, reason in ((429, 'rate_limited'), (402, 'out_of_credit'), (401, 'auth'), (403, 'auth')):
            for header in ('seconds', 'date', 'absent', 'invalid'):
                with self.subTest(status=status, header=header):
                    now = time.time()
                    completed = int((now - 75) * 1000)
                    absolute = int(now) + 600
                    headers = {'seconds': {'Retry-After': '600.5'}, 'date': {'retry-after': formatdate(absolute, usegmt=True)},
                               'absent': {}, 'invalid': {'retry-after': 'not a date'}}[header]
                    self.store.put('opencode_account_blocks', {})
                    session = await self.failed_turn(http_error=status, response_headers=headers, completed=completed)
                    block = self.store.get('opencode_account_blocks')['one']
                    expected = (completed / 1000 + 600.5 if header == 'seconds' else absolute
                                if header == 'date' else block['observed_at'] + 300)
                    self.assertEqual(block, {'reason': reason, 'until': expected, 'observed_at': block['observed_at'],
                                             'reset_known': header in ('seconds', 'date')})
                    await self.paused(session, expected)
                    await self.wake(session, expected)

    def pause_notices(self):
        return [row['text'] for row in self.store.db.execute(
            "SELECT text FROM outbox WHERE topic=? AND text LIKE 'This chat is paused:%' ORDER BY id",
            (self.store.get('coordinator_home_topic'),))]

    def pause_problems(self):
        return self.store.db.execute(
            "SELECT count(*) FROM problems WHERE area='accounts' AND code='unavailable'").fetchone()[0]

    async def credit_pause(self):
        failed = await self.failed_turn(http_error=402)
        self.assertIsNone(await self.service.start_session('chat'))
        return failed, dict(self.store.get('opencode_account_blocks')['one'])

    async def feed_credit_retry(self):
        await self.service.feed_once()
        await settle(asyncio.gather(*self.service.feed_tasks.values()))
        session = self.service.sessions['chat']
        if not session.closed:
            await until(session.idle)
        return session

    async def test_periodic_credit_retry_calls_provider_and_clears_block_on_success(self):
        failed, block = await self.credit_pause()
        self.chat.scenario()
        with patch('coordinator.service.time.time', return_value=block['until'] + 1):
            resumed = await self.feed_credit_retry()
        self.assertIsNot(resumed, failed)
        self.assertFalse(resumed.closed)
        self.assertEqual(resumed.session_id, failed.session_id)
        self.assertEqual(len(self.chat.calls('session/prompt')), 2)
        self.assertNotIn('one', self.store.get('opencode_account_blocks', {}))
        self.assertIsNone(self.store.get('coordinator:chat_selection_notice'))
        print('periodic credit retry: provider prompts=2, same native session, account block cleared')

    async def test_due_chat_retry_is_not_postponed_by_a_shared_account_block(self):
        from coordinator import opencode_runtime
        failed, block = await self.credit_pause()
        self.chat.scenario()
        with patch('coordinator.service.time.time', return_value=block['until'] + 1):
            opencode_runtime.record_parent_block(self.store, 'one', 'out_of_credit')
            resumed = await self.feed_credit_retry()
            self.assertIsNot(resumed, failed, 'the due chat only re-read a renewed account block')
            self.assertEqual(len(self.chat.calls('session/prompt')), 2)
            self.assertNotIn('one', self.store.get('opencode_account_blocks', {}))
        self.assertEqual(resumed.session_id, failed.session_id)
        print('due chat retry: renewed shared block did not postpone the provider call')

    async def test_new_owner_message_retries_credit_pause_immediately(self):
        failed, block = await self.credit_pause()
        self.chat.scenario()
        self.store.put('opencode_account_blocks', {'one': block, 'two': dict(block)})
        owner = self.chat.row('Credit is available. Continue.')
        resumed = await self.feed_credit_retry()
        self.assertIsNot(resumed, failed, 'a new owner message did not retry the paused parent')
        self.assertLess(time.time(), block['until'])
        self.assertEqual(resumed.session_id, failed.session_id)
        self.assertEqual(self.chat.persisted(owner)['delivered'], 'received')
        self.assertNotIn('one', self.store.get('opencode_account_blocks', {}))
        self.assertEqual(self.store.get('opencode_account_blocks')['two'], block)
        print('owner credit retry: new message delivered before deadline on the same native session')

    async def test_failed_owner_credit_retry_repauses_once_without_repeated_notice(self):
        failed, block = await self.credit_pause()
        original = self.pause_notices()
        owner = self.chat.row('Please try again now.')
        await self.service.feed_once()
        retried = self.service.sessions['chat']
        self.assertIsNot(retried, failed, 'owner message never caused a provider retry')
        await settle(retried.wait_closed())
        for _ in range(3):
            await self.service.feed_once()
        self.assertEqual(len(self.chat.calls('session/prompt')), 2)
        self.assertEqual(self.pause_notices(), original)
        self.assertEqual(self.pause_problems(), 1)
        self.assertEqual(self.store.get('opencode_account_blocks')['one']['reason'], 'out_of_credit')
        self.assertEqual(self.chat.persisted(owner)['delivered'], 'pending')

    async def test_worker_and_event_messages_do_not_retry_credit_pause(self):
        failed, block = await self.credit_pause()
        for kind in ('worker_result', 'callback', 'event', 'forwarded'):
            self.chat.row('Background update', kind)
            await self.service.feed_once()
        self.assertIs(self.service.sessions['chat'], failed)
        self.assertEqual(len(self.chat.calls('session/prompt')), 1)
        self.assertEqual(self.store.get('opencode_account_blocks')['one'], block)
        self.assertEqual(len(self.pause_notices()), 1)

    async def test_periodic_failed_credit_retry_calls_provider_once_per_interval(self):
        failed, block = await self.credit_pause()
        for _ in range(2):
            with patch('coordinator.service.time.time', return_value=block['until'] + 1):
                await self.service.feed_once()
                retried = self.service.sessions['chat']
                self.assertIsNot(retried, failed)
                await settle(retried.wait_closed())
                await self.service.feed_once()
                block = dict(self.store.get('opencode_account_blocks')['one'])
                self.assertEqual(block['until'] - block['observed_at'], 300)
                before = len(self.chat.calls('session/prompt'))
                for _ in range(3):
                    await self.service.feed_once()
                self.assertEqual(len(self.chat.calls('session/prompt')), before)
                failed = retried
        self.assertEqual(len(self.chat.calls('session/prompt')), 3)
        self.assertEqual(len(self.pause_notices()), 1)
        self.chat.scenario()
        with patch('coordinator.service.time.time', return_value=block['until'] + 1):
            resumed = await self.feed_credit_retry()
        self.assertEqual(resumed.session_id, failed.session_id)
        self.assertEqual(len(self.chat.calls('session/prompt')), 4)
        self.assertNotIn('one', self.store.get('opencode_account_blocks', {}))

    async def test_owner_credit_retry_survives_service_restart(self):
        failed, block = await self.credit_pause()
        self.service = Service(self.store, None, self.chat.runner, self.chat.cwd)
        self.chat.scenario()
        owner = self.chat.row('Continue after credit was added.')
        resumed = await self.feed_credit_retry()
        self.assertEqual(resumed.session_id, failed.session_id)
        self.assertEqual(self.chat.persisted(owner)['delivered'], 'received')
        self.assertLess(time.time(), block['until'])

    async def test_owner_credit_retry_with_batched_intake(self):
        failed, block = await self.credit_pause()
        self.store.put('coordinator_intake_batching', True)
        self.chat.scenario()
        owner = self.chat.row('Continue with the saved requests.')
        for _ in range(3):
            resumed = await self.feed_credit_retry()
        self.assertEqual(resumed.session_id, failed.session_id)
        self.assertEqual(self.chat.persisted(owner)['delivered'], 'received')
        self.assertLess(time.time(), block['until'])
        self.assertNotIn('one', self.store.get('opencode_account_blocks', {}))

    async def test_owner_credit_retry_uses_a_legacy_recovery_cursor(self):
        failed, block = await self.credit_pause()
        self.store.put('coordinator:chat_pause_owner', None)
        self.chat.scenario()
        owner = self.chat.row('Credit added after the earlier service stopped.')
        resumed = await self.feed_credit_retry()
        self.assertIsNot(resumed, failed)
        self.assertEqual(self.chat.persisted(owner)['delivered'], 'received')
        self.assertLess(time.time(), block['until'])

    async def test_second_chat_paused_at_selection_keeps_its_notice_across_shared_retries(self):
        with self.store.db:
            self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                                  ('chat2', 1, 3, 'chat2', str(self.chat.cwd)))
            self.store.put('coordinator:chat2_selection',
                           {'provider': 'opencode', 'model': None, 'effort': None, 'revision': 1})
        await self.credit_pause()
        self.chat.scenario(error_message='synthetic rejection', response_body='synthetic rejection',
                           http_error=402, unique_sessions=True)
        self.store.message_save('chat2', 'owner', 'Waiting in the second chat.')
        self.assertIsNone(await self.service.start_session('chat2'))

        def notices():
            return dict(self.store.db.execute(
                "SELECT topic,count(*) FROM outbox WHERE text LIKE 'This chat is paused:%' GROUP BY topic"))

        self.assertEqual(notices(), {'home': 1})
        prompts = len(self.chat.calls('session/prompt'))
        for _ in range(2):
            due = max(self.store.get('coordinator:%s_selection_retry_at' % topic) for topic in ('chat', 'chat2'))
            with patch('coordinator.service.time.time', return_value=due + 1):
                for _ in range(3):
                    await self.service.feed_once()
                    for session in list(self.service.sessions.values()):
                        await settle(session.wait_closed())
            self.assertGreater(len(self.chat.calls('session/prompt')), prompts)
            prompts = len(self.chat.calls('session/prompt'))
            self.assertEqual(notices(), {'home': 1})
            self.assertEqual(self.pause_problems(), 2)

    async def test_owner_retries_on_different_opencode_accounts_recheck_each_account(self):
        from coordinator import opencode_accounts, parent
        with self.store.db:
            self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                                  ('chat2', 1, 3, 'chat2', str(self.chat.cwd)))
            self.store.put('coordinator:chat2_selection',
                           {'provider': 'opencode', 'model': None, 'effort': None, 'revision': 1})
        failed, block = await self.credit_pause()
        with self.store.db:
            self.store.put('opencode_active_account', 'two')
            self.store.put('opencode_account_blocks', {'one': block, 'two': dict(block)})
        self.store.message_save('chat2', 'owner', 'Waiting on the second account.')
        self.assertIsNone(await self.service.start_session('chat2'))
        self.assertEqual(self.store.get('coordinator:chat2_selection_notice')['key'][:2], ['opencode', 'two'])
        self.store.put('opencode_active_account', 'one')
        release = asyncio.Event()
        rechecked = []
        retry_paused = parent.retry_paused

        async def slow_retry(store, pause_key):
            await retry_paused(store, pause_key)
            rechecked.append(opencode_accounts.active(store))
            await release.wait()

        try:
            with patch('coordinator.parent.retry_paused', slow_retry), \
                    patch('coordinator.service.PARENT_RECHECK_WAIT', 0.01):
                self.chat.row('Credit was added to the first account.')
                self.assertIsNone(await self.service.start_session('chat'))
                self.store.put('opencode_active_account', 'two')
                self.store.message_save('chat2', 'owner', 'Credit was added to the second account.')
                self.assertIsNone(await self.service.start_session('chat2'))
                self.assertEqual(rechecked, ['one', 'two'])
                blocks = self.store.get('opencode_account_blocks')
                self.assertLessEqual(blocks['one']['until'], time.time())
                self.assertLessEqual(blocks['two']['until'], time.time())
        finally:
            release.set()
            await settle(asyncio.gather(*self.service.parent_rechecks.values()))

    async def repeated_outage(self):
        session = await self.failed_turn(http_error=402)
        native = session.session_id
        self.assertIsNone(await self.service.start_session('chat'))
        for recheck in range(3):
            block = self.store.get('opencode_account_blocks')['one']
            self.assertFalse(block['reset_known'])
            with patch('coordinator.service.time.time', return_value=block['until'] + 1):
                resumed = await self.service.start_session('chat')
                self.assertIsNotNone(resumed)
                self.chat.parents.append(resumed)
                self.assertEqual(resumed.session_id, native)
                await self.chat.send(resumed, self.chat.row('recheck %d' % recheck))
                await resumed.wait_closed()
                self.assertEqual(self.store.get('opencode_account_blocks')['one']['reason'], 'out_of_credit')
                self.assertIsNone(await self.service.start_session('chat'))
        self.assertEqual(len(self.chat.calls('session/load')), 3)
        self.assertEqual(len(self.pause_notices()), 1, self.pause_notices())
        self.assertEqual(self.pause_problems(), 1)

    async def test_picked_chat_posts_once_across_three_402_rechecks_and_relaunches(self):
        await self.repeated_outage()

    async def test_home_chat_without_own_pick_posts_once_across_three_rechecks(self):
        from coordinator import opencode_runtime
        self.store.put('coordinator_home_topic', 'chat')
        self.store.put('coordinator:chat_selection', None)

        async def unavailable(*args, **kwargs):
            opencode_runtime.parent_ready(self.store)

        self.service.session_factory = unavailable
        now = time.time()
        for recheck in range(4):
            with patch('coordinator.service.time.time', return_value=now + recheck * 301):
                opencode_runtime.record_parent_block(self.store, 'one', 'out_of_credit')
                self.assertIsNone(await self.service.start_session('chat'))
        self.assertEqual(self.store.get('coordinator_account_notice')['topics'], ['chat'])
        self.assertEqual(len(self.pause_notices()), 1, self.pause_notices())
        self.assertEqual(self.pause_problems(), 1)

    async def test_relaunch_preserves_notice_before_another_402(self):
        failed = await self.failed_turn(http_error=402)
        self.assertIsNone(await self.service.start_session('chat'))
        original = self.store.get('coordinator:chat_selection_notice')
        until_reset = self.store.get('opencode_account_blocks')['one']['until']
        with patch('coordinator.service.time.time', return_value=until_reset + 1):
            resumed = await self.service.start_session('chat')
            self.assertIsNotNone(resumed)
            self.chat.parents.append(resumed)
            self.assertEqual(resumed.session_id, failed.session_id)
            self.assertEqual(self.store.get('coordinator:chat_selection_notice'), original)
            await self.chat.send(resumed, self.chat.row('still out of credit'))
            await resumed.wait_closed()
            self.assertIsNone(await self.service.start_session('chat'))
        self.assertEqual(len(self.pause_notices()), 1)
        self.assertEqual(self.pause_problems(), 1)

    async def test_unknown_reset_rechecks_keep_the_existing_notice_after_a_relaunch(self):
        from coordinator import opencode_runtime
        await self.failed_turn(http_error=402)
        self.assertIsNone(await self.service.start_session('chat'))
        original = self.store.get('coordinator:chat_selection_notice')
        self.assertIsInstance(original['key'][2], float)
        for recheck in range(3):
            previous = self.store.get('opencode_account_blocks')['one']['until']
            with patch('coordinator.service.time.time', return_value=previous + 1):
                opencode_runtime.record_parent_block(self.store, 'one', 'out_of_credit')
                resumed = await self.service.start_session('chat')
                self.assertIsNotNone(resumed)
                self.chat.parents.append(resumed)
                await self.chat.send(resumed, self.chat.row('scheduled credit retry %d' % recheck))
                await resumed.wait_closed()
                self.assertIsNone(await self.service.start_session('chat'))
        self.assertEqual(len(self.chat.calls('initialize')), 4)
        self.assertEqual(len(self.chat.calls('session/prompt')), 4)
        self.assertEqual(len(self.pause_notices()), 1, self.pause_notices())
        self.assertEqual(self.pause_problems(), 1)
        self.assertEqual(self.store.get('coordinator:chat_selection_notice'), original)

    async def test_changed_reason_account_or_provider_posts_a_new_notice(self):
        from coordinator import opencode_runtime
        from coordinator.session import AccountsUnavailable
        now = time.time()
        for index, (alias, reason) in enumerate((('one', 'out_of_credit'), ('one', 'auth'), ('two', 'auth'))):
            self.store.put('opencode_active_account', alias)
            self.store.put('coordinator:chat_selection_retry_at', None)
            with patch('coordinator.service.time.time', return_value=now):
                opencode_runtime.record_parent_block(self.store, alias, reason)
                self.assertIsNone(await self.service.start_session('chat'))
                self.assertIsNone(await self.service.start_session('chat'))
            self.assertEqual(len(self.pause_notices()), index + 1)
            self.assertEqual(self.pause_problems(), index + 1)
        self.assertIn('account two failed', self.pause_notices()[-1])
        self.service.pause_parent('chat', AccountsUnavailable(now + 300, provider='claude', alias='two'))
        self.assertEqual(self.pause_problems(), 4)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM outbox WHERE topic='home'").fetchone()[0], 4)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM outbox WHERE topic='chat'").fetchone()[0], 0)

    async def test_successful_turn_then_another_402_posts_a_new_notice(self):
        failed = await self.failed_turn(http_error=402)
        self.assertIsNone(await self.service.start_session('chat'))
        self.chat.scenario()
        until_reset = self.store.get('opencode_account_blocks')['one']['until']
        with patch('coordinator.service.time.time', return_value=until_reset + 1):
            resumed = await self.service.start_session('chat')
            self.chat.parents.append(resumed)
            self.assertEqual(resumed.session_id, failed.session_id)
            await self.chat.send(resumed, self.chat.row('working again'))
            await until(resumed.idle)
            await resumed.stop()
            self.assertIsNone(self.store.get('coordinator:chat_selection_notice'))
            self.chat.scenario(http_error=402, error_message='synthetic rejection')
            failed_again = await self.service.start_session('chat')
            self.chat.parents.append(failed_again)
            await self.chat.send(failed_again, self.chat.row('later outage'))
            await failed_again.wait_closed()
            self.assertIsNone(await self.service.start_session('chat'))
        self.assertEqual(len(self.pause_notices()), 2)
        self.assertEqual(self.pause_problems(), 2)

    async def test_known_opencode_reset_keeps_tolerance_and_relaunch_behavior(self):
        from coordinator import opencode_runtime
        from coordinator.session import AccountsUnavailable
        now = time.time()
        reset = now + 600
        for retry, count in ((reset, 1), (reset + 59, 1), (reset + 60, 2)):
            opencode_runtime.record_parent_block(self.store, 'one', 'rate_limited', retry)
            self.service.pause_parent('chat', AccountsUnavailable(retry, provider='opencode', alias='one'))
            self.assertEqual(len(self.pause_notices()), count)
        self.chat.scenario()
        with patch('coordinator.service.time.time', return_value=reset + 61):
            resumed = await self.service.start_session('chat')
        self.assertIsNotNone(resumed)
        self.chat.parents.append(resumed)
        self.assertIsNone(self.store.get('coordinator:chat_selection_notice'))

    async def test_opencode_retries_do_not_pause_or_relaunch_before_final_error(self):
        release = self.chat.root / 'finish-retries'
        self.chat.scenario(http_error=429, error_message='synthetic rejection', error_release=str(release))
        session = await self.chat.start()
        self.service.sessions['chat'] = session
        try:
            await self.chat.send(session, self.chat.row())
            await until(lambda: len(self.chat.calls('session/prompt')) == 1)
            self.assertTrue(session.busy and session.turn_open)
            self.assertEqual(self.store.get('opencode_account_blocks', {}), {})
            self.assertIs(await self.service.start_session('chat'), session)
            self.assertIsNone(self.store.get(self.chat.session_key + '_selection_retry_at'))
            self.assertEqual(len(self.chat.calls('initialize')), 1)
        finally:
            release.touch()
        await session.wait_closed()
        self.assertEqual(self.store.get('opencode_account_blocks')['one']['reason'], 'rate_limited')

    async def test_opencode_other_status_and_acp_text_do_not_block(self):
        for status in (400, 404, 500):
            with self.subTest(status=status):
                self.chat.scenario(http_error=status, error_message='429 out of credit invalid key')
                row = self.chat.row()
                session = await self.chat.start()
                await self.chat.send(session, row)
                await session.wait_closed()
                self.assertEqual(self.store.get('opencode_account_blocks', {}), {})

    def test_parent_block_does_not_change_worker_selection(self):
        from coordinator import opencode_runtime
        now = time.time()
        self.store.put('opencode_account_blocks', {'other': {'until': now + 900}})
        opencode_runtime.record_parent_block(self.store, 'one', 'auth')
        block = self.store.get('opencode_account_blocks')['one']
        self.assertEqual((block['reason'], block['until'], block['reset_known']), ('auth', block['observed_at'] + 300, False))
        self.assertEqual(self.store.get('opencode_account_blocks')['other'], {'until': now + 900})
        self.assertEqual(opencode_runtime.OpenCodeBroker(self.store).select(), 'one')
        from coordinator.session import AccountsUnavailable
        with self.assertRaises(AccountsUnavailable) as raised:
            opencode_runtime.parent_ready(self.store)
        self.assertEqual((raised.exception.provider, raised.exception.alias, raised.exception.reset_at),
                         ('opencode', 'one', block['until']))
