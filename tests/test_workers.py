import asyncio
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

from coordinator.accounts import LIMIT_CONTINUATION, AccountBroker
from coordinator.providers import ProviderRunner, RunResult
from coordinator.store import Store
from coordinator.vault import FakeVault
from coordinator.workers import WorkerPool


TOPIC = '-10042:4'

from tests.support import isolate_shared_mcp_home, stop_test_hosts

class WorkerTests(unittest.IsolatedAsyncioTestCase):
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
            home = self.root / 'primary'
            home.mkdir()
            self.store.put('accounts', {'primary': {'config_dir': str(home), 'enabled': True}})
            self.store.put('account_status', {'primary': {'identity': {'email': 'primary@example.com', 'logged_in': True},
                'observed_at': time.time(), 'usage': {'seven_day': {'utilization': 10,
                'resets_at': time.time() + 3600}}}})
        self.task = self.store.task_create(TOPIC, 'Build', worktree=str(self.root))

    async def asyncTearDown(self):
        self.store.close()

    async def test_worker_start_callback_reacts_but_reattachment_does_not(self):
        message = self.store.message_save(TOPIC, 'owner', 'Build', telegram_message=501)
        self.store.db.execute('UPDATE tasks SET origin=? WHERE id=?', (message['id'], self.task['id']))
        observed = []
        store = self.store

        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                options['on_start'](123)
                observed.append(store.db.execute('SELECT reaction_desired FROM messages WHERE id=?',
                                                 (message['id'],)).fetchone()[0])
                return RunResult(session or '00000000-0000-4000-8000-000000000001', text='done', success=True)

        worker = self.worker()
        self.store.db.execute("UPDATE workers SET work='dev' WHERE id=?", (worker['id'],))
        pool = WorkerPool(self.store, Runner(), AccountBroker(self.store))
        await pool.run(worker, self.store.topic(TOPIC))
        self.assertEqual(observed, ['👨\u200d💻'])
        attached = self.worker()
        self.store.db.execute("UPDATE workers SET status='running',account_alias='primary',session=? WHERE id=?",
                              ('00000000-0000-4000-8000-000000000002', attached['id']))
        await pool.run(attached, self.store.topic(TOPIC))
        self.assertEqual(observed, ['👨\u200d💻', '👨\u200d💻'])

    async def test_worker_gets_current_policy_and_effort(self):
        (self.store.directory / 'USAGE.md').write_text('Use one worker.')
        self.worker('codex')
        captured = {}

        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                captured.update(options)
                return RunResult(session or '00000000-0000-4000-8000-000000000001', text='done', success=True)

        row = dict(self.store.db.execute('SELECT * FROM workers ORDER BY id DESC LIMIT 1').fetchone())
        await WorkerPool(self.store, Runner(), AccountBroker(self.store)).run(row, self.store.topic(TOPIC))
        self.assertEqual(captured['effort'], 'medium')
        self.assertIn('Current usage policy:\nUse one worker.', captured['instructions'])
        self.assertNotIn('Owner delegation ' + 'settings', captured['instructions'])
        self.assertIn('Job: ', captured['instructions'])
        self.assertEqual(captured['model'], 'gpt-6.1-sol')

    def worker(self, provider='claude', goal=None, model=None):
        with self.store.db:
            row = self.store.db.execute('''INSERT INTO workers
                (task,topic,provider,prompt,cwd,workspace,goal,model,created,updated)
                VALUES (?,?,?,?,?,?,?,?,?,?)''',
                (self.task['id'], TOPIC, provider, 'build this', str(self.root),
                 json.dumps({'cwd': str(self.root)}), goal, model, 1.0, 1.0))
        return dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (row.lastrowid,)).fetchone())

    async def test_two_workers_share_task_worktree_without_capacity_gate(self):
        entered = []
        release = asyncio.Event()

        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                entered.append((provider, str(cwd), session))
                await release.wait()
                return RunResult(session, text='done', success=True)

        pool = WorkerPool(self.store, Runner(), AccountBroker(self.store))
        first, second = self.worker(), self.worker()
        tasks = [asyncio.create_task(pool.run(row, self.store.topic(TOPIC))) for row in (first, second)]
        try:
            async def both_started():
                while len(entered) < 2:
                    await asyncio.sleep(0.01)
            await asyncio.wait_for(both_started(), 2)
            self.assertEqual({cwd for _, cwd, _ in entered}, {str(self.root)})
            self.assertEqual(len({session for _, _, session in entered}), 2)
        finally:
            release.set()
            await asyncio.gather(*tasks)
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM messages WHERE kind='worker_result'").fetchone()[0], 2)

    async def run_with_text(self, provider, text):
        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                return RunResult(session or '00000000-0000-4000-8000-000000000001', text=text, success=True)

        worker = self.worker(provider)
        done = await WorkerPool(self.store, Runner(), AccountBroker(self.store)).run(worker, self.store.topic(TOPIC))
        return done, json.loads(done['result'])

    async def test_final_question_is_recorded_as_needs_input(self):
        done, result = await self.run_with_text('codex', 'I found two schemas.\n\n**Which one should I migrate?**\n')
        self.assertEqual(done['status'], 'needs_input')
        self.assertTrue(result['needs_input'])
        self.assertEqual(result['status'], 'needs_input')

    async def test_final_report_is_recorded_as_done(self):
        done, result = await self.run_with_text('codex', 'Did the tests pass?\nYes. All 40 tests pass.')
        self.assertEqual(done['status'], 'done')
        self.assertFalse(result['needs_input'])

    async def test_missing_task_worktree_saves_failure_message(self):
        worker = self.worker()
        with self.store.db:
            self.store.db.execute('UPDATE tasks SET worktree=? WHERE id=?', (str(self.root / 'gone'), self.task['id']))
        pool = WorkerPool(self.store, object(), AccountBroker(self.store))
        await pool.run(worker, self.store.topic(TOPIC))
        result = json.loads(self.store.db.execute('SELECT result FROM workers WHERE id=?', (worker['id'],)).fetchone()[0])
        self.assertEqual(result['failure_code'], 'start_failed')
        self.assertFalse(result['success'])
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM messages WHERE source_worker=?',
                                               (worker['id'],)).fetchone()[0], 1)

    async def test_codex_off_uses_the_delegation_control(self):
        with self.store.db:
            self.store.put('codex_enabled', False)
        worker = self.worker('codex')
        await WorkerPool(self.store, object(), AccountBroker(self.store)).run(
            worker, self.store.topic(TOPIC))
        result = json.loads(self.store.db.execute('SELECT result FROM workers WHERE id=?',
                                                  (worker['id'],)).fetchone()[0])
        self.assertNotIn('enable Codex', result['failure_detail'])
        self.assertEqual(result['failure_detail'], 'Codex delegation is disabled.')

    async def test_goal_is_steered_after_prompt_and_status_reaches_message(self):
        binary = self.root / 'fake-claude'
        binary.write_text('''#!/usr/bin/env python3
import json,sys
from pathlib import Path
root=Path(__file__).parent
sid=sys.argv[sys.argv.index('--session-id')+1]
print(json.dumps({'type':'system','subtype':'init','session_id':sid}),flush=True)
for line in sys.stdin:
    event=json.loads(line)
    with (root/'inputs.jsonl').open('a') as log:
        log.write(json.dumps(event)+'\\n')
    print(json.dumps({**event,'isReplay':True}),flush=True)
    text=event['message']['content']
    if text.startswith('/goal '):
        print(json.dumps({'type':'goal_status','session_id':sid,'status':'met',
            'condition':text[6:],'reason':'verified','iterations':3}),flush=True)
        print(json.dumps({'type':'result','subtype':'success','session_id':sid,'result':'complete'}),flush=True)
''')
        binary.chmod(0o700)
        runner = ProviderRunner(self.store.directory, {'claude': str(binary)})
        pool = WorkerPool(self.store, runner, AccountBroker(self.store))
        worker = self.worker(goal='all checks pass')
        await asyncio.wait_for(pool.run(worker, self.store.topic(TOPIC)), 5)
        inputs = [json.loads(line)['message']['content']
                  for line in (self.root / 'inputs.jsonl').read_text().splitlines()]
        self.assertIn('build this', inputs[0])
        self.assertEqual(inputs[1], '/goal all checks pass')
        result = json.loads(self.store.db.execute('SELECT result FROM workers WHERE id=?', (worker['id'],)).fetchone()[0])
        self.assertEqual(result['goal_status']['status'], 'met')
        message = json.loads(self.store.db.execute('SELECT text FROM messages WHERE source_worker=?',
                                                   (worker['id'],)).fetchone()[0])
        self.assertEqual(message['goal_status']['reason'], 'verified')

    async def test_quota_rotation_restarts_in_a_fresh_host_directory_and_resumes_the_session(self):
        binary = self.root / 'fake-claude'
        binary.write_text('''#!/usr/bin/env python3
import json,os,sys
from pathlib import Path
home=Path(os.environ['CLAUDE_CONFIG_DIR'])
with (Path(__file__).parent/'launches.jsonl').open('a') as log:
    log.write(json.dumps({'home':home.name,'argv':sys.argv[1:]})+'\\n')
if '--session-id' in sys.argv:
    sid=sys.argv[sys.argv.index('--session-id')+1]
    transcript=home/'projects'/'p'/(sid+'.jsonl')
    transcript.parent.mkdir(parents=True)
    transcript.write_text('{}\\n')
else:
    sid=Path(sys.argv[sys.argv.index('--resume')+1]).name[:-6]
print(json.dumps({'type':'system','subtype':'init','session_id':sid}),flush=True)
for line in sys.stdin:
    print(json.dumps({**json.loads(line),'isReplay':True}),flush=True)
    if home.name=='first':
        print(json.dumps({'type':'rate_limit_event','session_id':sid,'rate_limit_info':
            {'status':'rejected','resetsAt':9999999999,'rateLimitType':'seven_day'}}),flush=True)
        print(json.dumps({'type':'system','subtype':'background_tasks_changed','session_id':sid,
            'tasks':[{'task_id':'a1','task_type':'local_agent','description':'review'}]}),flush=True)
        print(json.dumps({'type':'result','subtype':'success','is_error':True,
            'terminal_reason':'api_error','origin':{'kind':'task-notification'},
            'session_id':sid}),flush=True)
        assert sys.stdin.read()==''
        sys.exit(1)
    print(json.dumps({'type':'result','subtype':'success','session_id':sid,'result':'resumed'}),flush=True)
''')
        binary.chmod(0o700)
        for alias in ('first', 'second'):
            (self.root / alias).mkdir()
        with self.store.db:
            self.store.put('accounts', {alias: {'config_dir': str(self.root / alias), 'enabled': True}
                                        for alias in ('first', 'second')})
            now = time.time()
            self.store.put('account_status', {alias: {'identity': {'email': alias + '@example.com', 'logged_in': True},
                'observed_at': now, 'usage': {'five_hour': {'utilization': 10, 'resets_at': now + 1800},
                'seven_day': {'utilization': 10, 'resets_at': now + (3600 if alias == 'first' else 7200)},
                'seven_day_fable': {'utilization': 10, 'resets_at': now + (3600 if alias == 'first' else 7200)}}}
                for alias in ('first', 'second')})
        runner = ProviderRunner(self.store.directory, {'claude': str(binary)})
        pool = WorkerPool(self.store, runner, AccountBroker(self.store))
        worker = self.worker()
        (self.store.directory / 'hosts' / ('worker-' + str(worker['id']))).mkdir(parents=True)
        done = await asyncio.wait_for(pool.run(worker, self.store.topic(TOPIC)), 20)
        self.assertTrue(json.loads(done['result'])['success'])
        launches = [json.loads(line) for line in (self.root / 'launches.jsonl').read_text().splitlines()]
        self.assertEqual([launch['home'] for launch in launches], ['first', 'second'])
        session = launches[0]['argv'][launches[0]['argv'].index('--session-id') + 1]
        resumed = launches[1]['argv'][launches[1]['argv'].index('--resume') + 1]
        self.assertEqual(Path(resumed).resolve(), (self.root / 'first' / 'projects' / 'p' / (session + '.jsonl')).resolve())
        self.assertEqual(done['session'], session)
        base = 'worker-' + str(worker['id'])
        self.assertEqual(done['host'], base + '.3')
        hosts = self.store.directory / 'hosts'
        self.assertEqual(sorted(path.name for path in hosts.iterdir()), [base, base + '.2', base + '.3'])
        self.assertTrue((hosts / (base + '.2') / 'exit.json').is_file())
        self.assertEqual(json.loads((hosts / (base + '.3') / 'spec.json').read_text())['env']['CLAUDE_CONFIG_DIR'],
                         str(self.root / 'second'))

    async def test_failed_task_notification_reports_worker_result(self):
        binary = self.root / 'fake-claude'
        binary.write_text('''#!/usr/bin/env python3
import json,sys
sid=sys.argv[sys.argv.index('--session-id')+1]
print(json.dumps({'type':'system','subtype':'init','session_id':sid}),flush=True)
sys.stdin.readline()
print(json.dumps({'type':'system','subtype':'background_tasks_changed','session_id':sid,
    'tasks':[{'task_id':'a1','task_type':'local_agent','description':'review'}]}),flush=True)
print(json.dumps({'type':'result','subtype':'success','is_error':True,'session_id':sid,
    'terminal_reason':'api_error','origin':{'kind':'task-notification'}}),flush=True)
assert sys.stdin.read()==''
''')
        binary.chmod(0o700)
        pool = WorkerPool(self.store, ProviderRunner(self.store.directory, {'claude': str(binary)}),
                          AccountBroker(self.store))
        worker = self.worker()
        done = await asyncio.wait_for(pool.run(worker, self.store.topic(TOPIC)), 5)
        self.assertEqual(done['status'], 'done')
        result = json.loads(done['result'])
        self.assertFalse(result['success'])
        self.assertFalse(result['quota_limited'])
        self.assertNotEqual(result['failure_code'], 'pending_work')
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM messages WHERE kind='worker_result' AND source_worker=?",
                                               (worker['id'],)).fetchone()[0], 1)

    async def test_worker_waits_once_then_resumes_after_the_earliest_reset(self):
        from coordinator.service import Service
        sid = '20000000-0000-4000-8000-000000000091'
        transcript = self.root / 'primary' / 'projects' / 'test' / (sid + '.jsonl')
        transcript.parent.mkdir(parents=True)
        transcript.write_text('{}\n')
        calls = []
        reset = time.time() + 3600

        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                calls.append((prompt, session, options.get('fresh'), options.get('account_alias'),
                              options.get('initial_steer')))
                if len(calls) == 1:
                    return RunResult(session, quota_limited=True, transcript_path=str(transcript),
                                     rate_limit_info={'status': 'rejected', 'rateLimitType': 'five_hour',
                                                      'resetsAt': reset})
                return RunResult(session, text='resumed', success=True, transcript_path=str(transcript))

        runner = Runner()
        worker = self.worker(goal='finish the same goal')
        service = Service(self.store, None, runner, self.root)
        waiting = await service.workers.run(worker, self.store.topic(TOPIC))
        self.assertEqual(waiting['status'], 'waiting_for_quota')
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM messages WHERE kind='callback'").fetchone()[0], 1)
        payload = json.loads(waiting['result'])
        self.assertEqual(payload['waiting_until'], reset)
        payload['waiting_until'] = time.time() - 1
        with self.store.db:
            self.store.db.execute('UPDATE workers SET result=? WHERE id=?',
                                  (json.dumps(payload), worker['id']))
            self.store.put('account_blocks', {'primary': {'until': time.time() - 1, 'reason': 'quota'}})
            status = self.store.get('account_status')
            status['primary']['observed_at'] = time.time()
            self.store.put('account_status', status)
        await service.workers_once()
        await asyncio.gather(*service.worker_tasks.values())
        complete = service.workers.get(worker['id'])
        self.assertEqual(complete['status'], 'done')
        self.assertEqual(len(calls), 2)
        self.assertEqual(LIMIT_CONTINUATION + calls[0][0], calls[1][0])
        self.assertEqual(calls[0][1], calls[1][1])
        self.assertEqual(calls[0][4], calls[1][4])
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM messages WHERE kind='callback'").fetchone()[0], 1)

    async def test_worker_that_waits_before_its_first_launch_starts_fresh_after_the_wait(self):
        from coordinator.service import Service
        calls = []

        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                calls.append(options.get('fresh'))
                return RunResult(session, text='ok', success=True)

        def usage(utilization):
            with self.store.db:
                status = self.store.get('account_status')
                status['primary']['usage']['seven_day']['utilization'] = utilization
                self.store.put('account_status', status)

        usage(96)
        worker = self.worker()
        service = Service(self.store, None, Runner(), self.root)
        waiting = await service.workers.run(worker, self.store.topic(TOPIC))
        self.assertEqual((waiting['status'], waiting['fresh'], calls), ('waiting_for_quota', 1, []))
        usage(10)
        payload = json.loads(waiting['result'])
        payload['waiting_until'] = time.time() - 1
        with self.store.db:
            self.store.db.execute('UPDATE workers SET result=? WHERE id=?', (json.dumps(payload), worker['id']))
        await service.workers_once()
        await asyncio.gather(*service.worker_tasks.values())
        self.assertEqual(calls, [True])
        self.assertEqual(service.workers.get(worker['id'])['status'], 'done')


class WorkerProblemTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = WorkerTests.asyncSetUp
    asyncTearDown = WorkerTests.asyncTearDown
    worker = WorkerTests.worker

    def problems(self):
        return [tuple(row) for row in self.store.db.execute(
            'SELECT area,code,detail,topic,task,worker FROM problems ORDER BY id')]

    async def test_a_failed_worker_saves_its_code_and_detail_when_it_ends(self):
        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                options['on_problem']('worker', 'input-failed', 'BrokenPipeError at host.py:96')
                return RunResult(session, error='Provider execution failed: RuntimeError',
                                 failure_code='execution_failed',
                                 failure_detail='RuntimeError: Provider changed the native thread at native_protocol.py:228')

        worker = self.worker()
        with self.assertLogs('coordinator.problems', 'WARNING') as logs:
            await WorkerPool(self.store, Runner(), AccountBroker(self.store)).run(worker, self.store.topic(TOPIC))
        self.assertEqual(self.problems(), [
            ('worker', 'input-failed', 'BrokenPipeError at host.py:96', TOPIC, self.task['id'], worker['id']),
            ('worker', 'execution_failed',
             'RuntimeError: Provider changed the native thread at native_protocol.py:228',
             TOPIC, self.task['id'], worker['id'])])
        self.assertIn('problem area=worker code=execution_failed topic=%s task=%d worker=%d' % (
            TOPIC, self.task['id'], worker['id']), logs.output[1])

    async def test_background_work_and_a_dead_host_are_problems_and_a_final_question_is_not(self):
        outcomes = [RunResult(None, error='The worker exited while its background work was still running.',
                              failure_code='pending_work'),
                    RunResult(None, error='The provider host stopped without an exit record.',
                              failure_code='host_died', failure_detail='HostDied at host.py:121'),
                    RunResult(None, text='Which schema should I migrate?', success=True)]

        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                return outcomes.pop(0)

        pool = WorkerPool(self.store, Runner(), AccountBroker(self.store))
        workers = [self.worker() for _ in range(3)]
        with self.assertLogs('coordinator.problems', 'WARNING'):
            for worker in workers:
                await pool.run(worker, self.store.topic(TOPIC))
        self.assertEqual(pool.get(workers[2]['id'])['status'], 'needs_input')
        self.assertEqual([row[:3] + row[5:] for row in self.problems()], [
            ('worker', 'pending_work', 'The worker exited while its background work was still running.',
             workers[0]['id']),
            ('worker', 'host_died', 'HostDied at host.py:121', workers[1]['id'])])

    async def test_a_successful_worker_saves_no_problem(self):
        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                return RunResult(session, text='done', success=True)

        await WorkerPool(self.store, Runner(), AccountBroker(self.store)).run(self.worker(), self.store.topic(TOPIC))
        self.assertEqual(self.problems(), [])


class SecretWorkerTestsSupport:
    async def asyncSetUp(self):
        await WorkerTests.asyncSetUp(self)
        self.vault = FakeVault()
        self.calls = []
        test = self

        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                test.calls.append(options)
                return RunResult(session, text='done', success=True)

        self.pool = WorkerPool(self.store, Runner(), AccountBroker(self.store), self.vault)

    def fill(self, name, value, task=None):
        self.vault.values[name] = value
        with self.store.db:
            return self.store.db.execute('''INSERT INTO envelopes(name,reason,consumer,task,topic,state,created,expires,
                length,fingerprint,updated) VALUES (?,'r','c',?,?,'filled',1,601,?,'abcd1234',1)''',
                                         (name, task, TOPIC, len(value))).lastrowid

    def declare(self, *entries):
        self.store.task_update(self.task['id'], secrets=list(entries))

    def result(self, worker):
        return json.loads(self.store.db.execute('SELECT result FROM workers WHERE id=?', (worker['id'],)).fetchone()[0])

    def uses(self):
        return [tuple(row) for row in self.store.db.execute(
            "SELECT envelope,name,worker,source FROM envelope_events WHERE event='use' ORDER BY id")]

    async def assert_needs_input_resumes_keep_secrets(self, provider, clear, versioned=False):
        from coordinator.service import Service
        vault_calls = len(self.vault.calls)
        self.declare('NPM_TOKEN')
        worker = self.worker(provider)
        session_id = '00000000-0000-4000-8000-000000000001'
        transcript = self.root / 'primary' / 'projects' / 'test' / (session_id + '.jsonl')
        transcript.parent.mkdir(parents=True, exist_ok=True)
        transcript.write_text('{}\n')
        calls = []
        store = self.store

        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                calls.append(dict(options, session=session))
                name = options['host_id'] + ('.2' if versioned else '')
                directory = store.directory / 'hosts' / name
                directory.mkdir(parents=True)
                (directory / 'spec.json').write_text(json.dumps({'secret_names': options.get('secret_names', [])}))
                options['on_host'](name)
                options['on_session'](session_id)
                return RunResult(session_id, text='Which path should I use?', success=True,
                                 transcript_path=str(transcript))

        service = Service(self.store, None, Runner(), self.root, vault=self.vault)
        await service.workers_once()
        await asyncio.gather(*service.worker_tasks.values())
        self.assertTrue(calls[0]['fresh'])
        self.assertEqual(calls[0]['secrets'], {'NPM_TOKEN': 'FAKE-NPM-VALUE-0123456789'})
        self.assertEqual(service.workers.get(worker['id'])['status'], 'needs_input')
        if clear:
            self.declare()
        for _ in range(3):
            request = self.store.service_request('workers.steer', {'worker': worker['id'], 'prompt': 'Continue.'})
            await service.controls_once()
            resumed = service.workers.get(worker['id'])
            self.assertEqual(resumed['status'], 'queued')
            self.assertEqual(resumed['host'], 'worker-%d-resume-%d' % (worker['id'], request))
            await service.workers_once()
            await asyncio.gather(*service.worker_tasks.values())
            self.assertEqual(service.workers.get(worker['id'])['status'], 'needs_input')
            self.assertEqual(calls[-1]['session'], session_id)
            self.assertFalse(calls[-1]['fresh'])
            self.assertEqual(calls[-1]['secrets'], None if clear else {'NPM_TOKEN': 'FAKE-NPM-VALUE-0123456789'})
            if provider == 'codex':
                self.assertEqual(calls[-1]['secret_names'], ['NPM_TOKEN'])
        self.assertEqual(len([use for use in self.uses() if use[2] == worker['id']]), 1 if clear else 4)
        self.assertEqual(len(self.vault.calls) - vault_calls, 1 if clear else 4)

    async def assert_refused(self, text):
        worker = self.worker()
        await self.pool.run(worker, self.store.topic(TOPIC))
        self.assertEqual(self.calls, [])
        result = self.result(worker)
        self.assertEqual((result['success'], result['failure_code']), (False, 'start_failed'))
        self.assertIn(text, result['error'])
        self.assertEqual(self.uses(), [])
        self.assertFalse(self.store.db.in_transaction)

    def problems(self):
        return [tuple(row) for row in self.store.db.execute(
            'SELECT area,code,detail,topic,task,worker FROM problems ORDER BY id')]


class SecretWorkerTests(SecretWorkerTestsSupport, unittest.IsolatedAsyncioTestCase):
    asyncTearDown = WorkerTests.asyncTearDown

    worker = WorkerTests.worker

    async def test_attached_worker_moved_by_a_limit_keeps_its_secrets_on_the_next_account(self):
        from coordinator.service import Service
        self.fill('NPM_TOKEN', 'FAKE-NPM-VALUE-0123456789')
        self.declare('NPM_TOKEN')
        second = self.root / 'second'
        second.mkdir()
        with self.store.db:
            accounts = self.store.get('accounts')
            accounts['second'] = {'config_dir': str(second), 'enabled': True}
            self.store.put('accounts', accounts)
            status = self.store.get('account_status')
            status['second'] = {'identity': {'email': 'second@example.com', 'logged_in': True},
                                'observed_at': time.time(),
                                'usage': {'seven_day': {'utilization': 10, 'resets_at': time.time() + 7200}}}
            self.store.put('account_status', status)
        sid = '20000000-0000-4000-8000-000000000077'
        transcript = self.root / 'primary' / 'projects' / 'p' / (sid + '.jsonl')
        transcript.parent.mkdir(parents=True)
        transcript.write_text('{}\n')
        worker = self.worker()
        with self.store.db:
            self.store.db.execute("UPDATE workers SET status='running',session=?,fresh=0,host='worker-x',"
                                  "account_alias='primary' WHERE id=?", (sid, worker['id']))
        calls = []

        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                calls.append(dict(options, prompt=prompt))
                if options.get('attach'):
                    return RunResult(session, quota_limited=True, transcript_path=str(transcript),
                                     rate_limit_info={'status': 'rejected', 'resetsAt': time.time() + 3600})
                return RunResult(session, text='ok', success=True, transcript_path=str(transcript))

        service = Service(self.store, None, Runner(), self.root, vault=self.vault)
        await service.workers.run(worker, self.store.topic(TOPIC))
        await service.workers_once()
        await asyncio.gather(*service.worker_tasks.values())
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[1]['account_alias'], 'second')
        self.assertEqual(calls[1]['secrets'], {'NPM_TOKEN': 'FAKE-NPM-VALUE-0123456789'})
        self.assertEqual(calls[1]['prompt'], LIMIT_CONTINUATION + 'build this')
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM messages WHERE kind='callback'").fetchone()[0], 0)
        self.assertEqual(service.workers.get(worker['id'])['status'], 'done')

    async def test_declared_secrets_reach_only_the_worker_options(self):
        first = self.fill('GITHUB_TOKEN_ORG', 'FAKE-ENVELOPE-VALUE-0123456789', task=self.task['id'])
        second = self.fill('NPM_TOKEN', 'FAKE-NPM-VALUE-0123456789')
        self.declare('GH_TOKEN=GITHUB_TOKEN_ORG', 'NPM_TOKEN')
        worker = self.worker()
        await self.pool.run(worker, self.store.topic(TOPIC))
        options = self.calls[0]
        self.assertEqual(options['secrets'], {'GH_TOKEN': 'FAKE-ENVELOPE-VALUE-0123456789',
                                              'NPM_TOKEN': 'FAKE-NPM-VALUE-0123456789'})
        self.assertIn('Secrets in the environment: GH_TOKEN, NPM_TOKEN.', options['instructions'])
        self.assertNotIn('FAKE-', options['instructions'])
        self.assertEqual(self.uses(), [(first, 'GITHUB_TOKEN_ORG', worker['id'], 'worker:%d' % worker['id']),
                                       (second, 'NPM_TOKEN', worker['id'], 'worker:%d' % worker['id'])])
        self.assertTrue(self.result(worker)['success'])
        self.assertFalse(self.store.db.in_transaction)

    async def test_codex_worker_receives_secrets_declared_after_queueing(self):
        self.fill('NPM_TOKEN', 'FAKE-NPM-VALUE-0123456789')
        worker = self.worker('codex')
        self.declare('NPM_TOKEN')
        done = await self.pool.run(worker, self.store.topic(TOPIC))
        self.assertEqual(done['status'], 'done')
        self.assertEqual(self.calls[0]['secrets'], {'NPM_TOKEN': 'FAKE-NPM-VALUE-0123456789'})
        self.assertEqual(self.calls[0]['secret_names'], ['NPM_TOKEN'])
        self.assertTrue(self.result(worker)['success'])
        self.assertEqual(len(self.uses()), 1)

    async def test_task_without_secrets_passes_none(self):
        await self.pool.run(self.worker(), self.store.topic(TOPIC))
        self.assertIsNone(self.calls[0]['secrets'])
        self.assertNotIn('Secrets in the environment', self.calls[0]['instructions'])

    async def test_codex_account_transfer_reloads_only_its_job_secrets(self):
        self.fill('NPM_TOKEN', 'FAKE-NPM-VALUE-0123456789')
        self.declare('NPM_TOKEN')
        worker = self.worker('codex', goal='finish')
        broker = self.pool.codex_accounts
        options = {'secrets': None, 'secret_names': ['NPM_TOKEN'], 'goal': 'finish', 'account_alias': 'old'}
        broker.run = AsyncMock(return_value=RunResult('00000000-0000-4000-8000-000000000001', success=True))
        with patch.object(broker, 'record_rate_limit'), patch.object(broker, 'automatic', return_value=True), \
                patch.object(broker, 'select', return_value='next'):
            result = await self.pool.codex_attached(worker, self.store.topic(TOPIC),
                RunResult('00000000-0000-4000-8000-000000000001', quota_limited=True), options, lambda alias: None)
        sent = broker.run.await_args.kwargs
        self.assertTrue(result.success)
        self.assertEqual(sent['secrets'], {'NPM_TOKEN': 'FAKE-NPM-VALUE-0123456789'})
        self.assertIn('Secrets in the environment: NPM_TOKEN.', sent['instructions'])
        self.assertNotIn('FAKE-', sent['instructions'])
        self.assertEqual(sent['secret_names'], ['NPM_TOKEN'])
        self.assertFalse(sent['attach'])
        self.assertFalse(sent['fresh'])
        self.assertEqual(sent['goal'], 'finish')

    async def test_codex_resume_keeps_memory_excluded_after_secret_declarations_are_cleared(self):
        worker = self.worker('codex')
        directory = self.store.directory / 'hosts' / ('worker-' + str(worker['id']))
        directory.mkdir(parents=True)
        (directory / 'spec.json').write_text(json.dumps({'secret_names': ['NPM_TOKEN']}))
        await self.pool.run(worker, self.store.topic(TOPIC))
        self.assertEqual(self.calls[0]['secret_names'], ['NPM_TOKEN'])
        self.assertIsNone(self.calls[0]['secrets'])

    async def test_needs_input_resumes_keep_names_after_declarations_are_cleared(self):
        self.fill('NPM_TOKEN', 'FAKE-NPM-VALUE-0123456789')
        for provider in ('codex', 'claude'):
            with self.subTest(provider=provider):
                await self.assert_needs_input_resumes_keep_secrets(provider, clear=True)

    async def test_needs_input_resumes_deliver_unchanged_declarations(self):
        self.fill('NPM_TOKEN', 'FAKE-NPM-VALUE-0123456789')
        for provider in ('codex', 'claude'):
            with self.subTest(provider=provider):
                await self.assert_needs_input_resumes_keep_secrets(provider, clear=False)

    async def test_needs_input_resumes_keep_names_from_versioned_hosts(self):
        self.fill('NPM_TOKEN', 'FAKE-NPM-VALUE-0123456789')
        await self.assert_needs_input_resumes_keep_secrets('codex', clear=True, versioned=True)

    async def test_codex_resume_unions_names_from_earlier_hosts_without_other_workers(self):
        worker = self.worker('codex')
        for name, secret in (('worker-%d.2' % worker['id'], 'NPM_TOKEN'),
                             ('worker-%d-resume-7.2' % worker['id'], 'GH_TOKEN'),
                             ('worker-%d' % (worker['id'] * 10), 'OTHER_TOKEN')):
            directory = self.store.directory / 'hosts' / name
            directory.mkdir(parents=True)
            (directory / 'spec.json').write_text(json.dumps({'secret_names': [secret]}))
        with self.store.db:
            self.store.db.execute("UPDATE workers SET host=? WHERE id=?",
                                  ('worker-%d-resume-8' % worker['id'], worker['id']))
        await self.pool.run(worker, self.store.topic(TOPIC))
        self.assertEqual(self.calls[0]['secret_names'], ['GH_TOKEN', 'NPM_TOKEN'])
        self.assertIsNone(self.calls[0]['secrets'])

    async def test_missing_secret_fails_before_the_provider_starts(self):
        self.fill('NPM_TOKEN', 'FAKE-NPM-VALUE-0123456789')
        self.declare('GITHUB_TOKEN', 'NPM_TOKEN')
        await self.assert_refused('Secrets are not filled: GITHUB_TOKEN.')
        self.assertEqual(self.vault.calls, [])
        rows = self.problems()
        self.assertEqual(rows[0], ('secrets', 'not-filled', 'names=GITHUB_TOKEN states=none', TOPIC, self.task['id'], None))
        self.assertEqual(rows[1][:2], ('worker', 'start_failed'))
        self.assertNotIn('FAKE-', json.dumps(rows))

    async def test_worker_waits_for_an_asked_secret_and_starts_when_it_is_filled(self):
        with self.store.db:
            envelope = self.store.db.execute('''INSERT INTO envelopes(name,reason,consumer,task,topic,state,created,
                expires,updated) VALUES ('GITHUB_TOKEN','r','c',?,?,'open',1,9999999999,1)''',
                                             (self.task['id'], TOPIC)).lastrowid
        self.declare('GITHUB_TOKEN')
        worker = self.worker()
        for _ in range(2):
            waiting = await self.pool.run(self.pool.get(worker['id']), self.store.topic(TOPIC))
        self.assertEqual(waiting['status'], 'waiting_for_secret')
        self.assertEqual(self.calls, [])
        self.assertEqual(self.problems(), [])
        notes = [row['text'] for row in self.store.db.execute("SELECT text FROM messages WHERE kind='callback'")]
        self.assertEqual(notes, ['Worker %d waits for GITHUB_TOKEN. It starts when the owner fills the secret card.'
                                 % worker['id']])
        self.vault.values['GITHUB_TOKEN'] = 'FAKE-ENVELOPE-VALUE-0123456789'
        with self.store.db:
            self.store.db.execute("UPDATE envelopes SET state='filled' WHERE id=?", (envelope,))
        done = await self.pool.run(self.pool.get(worker['id']), self.store.topic(TOPIC))
        self.assertEqual(done['status'], 'done')
        self.assertEqual(self.calls[0]['secrets'], {'GITHUB_TOKEN': 'FAKE-ENVELOPE-VALUE-0123456789'})
        self.assertTrue(self.result(worker)['success'])

    async def test_secret_asked_for_another_task_is_refused(self):
        other = self.store.task_create(TOPIC, 'Other')
        self.fill('GITHUB_TOKEN', 'FAKE-ENVELOPE-VALUE-0123456789', task=other['id'])
        self.declare('GITHUB_TOKEN')
        await self.assert_refused('Secrets were asked for another job: GITHUB_TOKEN.')

    async def test_vault_read_error_names_the_secret(self):
        self.fill('GITHUB_TOKEN', 'FAKE-ENVELOPE-VALUE-0123456789')
        self.declare('GITHUB_TOKEN')
        self.vault.fail_next = 'integrity'
        await self.assert_refused('The vault read failed for GITHUB_TOKEN.')
        self.assertEqual(self.problems()[0][:3], ('secrets', 'vault-read-failed', 'name=GITHUB_TOKEN code=integrity'))
        self.assertNotIn('FAKE-', json.dumps(self.problems()))
