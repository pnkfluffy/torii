import asyncio
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from coordinator.account_status import refresh_accounts
from coordinator.providers import ProviderRunner, RunResult
from coordinator.service import Service
from coordinator.store import Store
from tests.support import isolate_shared_mcp_home, until, stop_test_hosts


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
        self.assertEqual(len(self.callbacks()), 1)
        for text in ('first follow-up', 'second follow-up'):
            request = self.store.service_request('workers.steer', {'worker': worker['id'], 'prompt': text})
            await service.controls_once()
            self.assertEqual(self.store.db.execute('SELECT state FROM service_requests WHERE id=?',
                                                   (request,)).fetchone()[0], 'done')
            await service.workers_once()
            await asyncio.gather(*service.worker_tasks.values())
        self.assertEqual(service.workers.get(worker['id'])['status'], 'waiting_for_quota')
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(self.callbacks()), 1)

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
        launches = [json.loads(line)['home'] for line in (self.root / 'launches.jsonl').read_text().splitlines()]
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
        launches = [json.loads(line)['home'] for line in (self.root / 'launches.jsonl').read_text().splitlines()]
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
