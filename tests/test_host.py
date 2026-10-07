import asyncio
import gc
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import time
import unittest
import uuid
from unittest.mock import patch

from coordinator.host import HostClient, alive
from coordinator.native_protocol import RunControl
from coordinator.providers import ProviderRunner
from coordinator.accounts import AccountBroker
from coordinator.service import Service
from coordinator.store import Store
from tests.support import isolate_shared_mcp_home, stop_test_hosts


class FakeTelegram:
    async def updates(self, offset):
        await asyncio.sleep(.1)
        return []

    async def send(self, row):
        return {'message_id': row['id']}

    async def call(self, method, **params):
        if method == 'getMe':
            return {'id': 99, 'username': 'torii_test_bot'}
        if method == 'getChatMember':
            return {'status': 'administrator'}
        return True


class HostTestsSupport:
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)
        isolate_shared_mcp_home(self, self.root)
        self.addCleanup(stop_test_hosts, self.root)
        self.state = self.root / 'state'
        self.state.mkdir(mode=0o700)
        self.binary = self.root / 'fake'
        self.binary.write_text('''#!/usr/bin/env python3
import json,os,sys,time
from pathlib import Path
root=Path(__file__).parent
sid=sys.argv[sys.argv.index('--session-id')+1] if '--session-id' in sys.argv else sys.argv[sys.argv.index('--resume')+1]
if '--append-system-prompt' in sys.argv:
    print(json.dumps({'type':'system','subtype':'init','session_id':sid}),flush=True)
    for line in sys.stdin:
        event=json.loads(line)
        with (root/'inputs.jsonl').open('a') as stream:
            stream.write(json.dumps({'pid':os.getpid(),'event':event})+'\\n')
        print(json.dumps({**event,'isReplay':True}),flush=True)
        print(json.dumps({'type':'result','subtype':'success','session_id':sid,'result':'done'}),flush=True)
else:
    prompt=json.loads(sys.stdin.readline())['message']['content']
    print(json.dumps({'type':'system','subtype':'init','session_id':sid}),flush=True)
    while 'wait' in prompt and not (root/('release-'+str(os.getpid()))).exists():
        time.sleep(.02)
    print(json.dumps({'type':'result','subtype':'success','session_id':sid,'result':'worker done'}),flush=True)
''')
        self.binary.chmod(0o700)
        self.account_store = Store(self.state / 'account-selection')
        self.add_named_account(self.account_store)
        self.broker = AccountBroker(self.account_store)

    def add_named_account(self, store):
        profile = self.root / 'claude-profile'
        profile.mkdir(exist_ok=True)
        with store.db:
            store.put('accounts', {'test': {'config_dir': str(profile), 'enabled': True}})
            store.put('account_status', {'test': {'identity': {'email': 'test@example.com', 'logged_in': True},
                'observed_at': time.time(), 'usage': {'five_hour': {'utilization': 10},
                'seven_day': {'utilization': 10, 'resets_at': time.time() + 3600}}}})

    async def asyncTearDown(self):
        self.account_store.close()

    def runner(self, binaries=None):
        return ProviderRunner(self.state, binaries=binaries, account_broker=self.broker)

    async def until(self, predicate):
        async def wait():
            while not predicate():
                await asyncio.sleep(.02)
        await asyncio.wait_for(wait(), 40)


class HostTests(HostTestsSupport, unittest.IsolatedAsyncioTestCase):
    async def test_client_detach_preserves_provider_and_replays_only_new_events(self):
        directory = self.state / 'hosts' / 'worker-direct'
        script = ('import json,sys; print(json.dumps({"n":1}),flush=True); '
                  'assert sys.stdin.readline().strip()=="release"; '
                  'print(json.dumps({"n":2}),flush=True); print(json.dumps({"n":3}),flush=True)')
        spec = {'argv': [sys.executable, '-u', '-c', script], 'cwd': str(self.root),
                'env': {'PATH': os.environ['PATH']}, 'provider': 'claude', 'locks': []}
        first = await HostClient.launch(directory, spec)
        self.assertEqual(directory.stat().st_mode & 0o777, 0o700)
        for name in ('spec.json', 'pid', 'child.pid', 'events.jsonl', 'stderr.log', 'writes.jsonl'):
            self.assertEqual((directory / name).stat().st_mode & 0o777, 0o600)
        self.assertEqual(json.loads(await first.next_event()), {'n': 1})
        first.ack()
        child_pid = first.child_pid
        first.detach()
        del first
        gc.collect()
        self.assertTrue(alive(child_pid))
        await HostClient(directory).request('write', line='release')
        await self.until(lambda: (directory / 'exit.json').exists())
        saved = []
        second = HostClient.attach(directory, last_seq=1, on_seq=saved.append)
        values = []
        while True:
            event = await second.next_event()
            if event is None:
                break
            values.append(json.loads(event)['n'])
            second.ack()
        self.assertEqual(values, [2, 3])
        self.assertEqual(saved, [2, 3])
        self.assertEqual(second.returncode, 0)
        second.detach()

    async def test_test_root_cleanup_stops_detached_host_and_child(self):
        directory = self.state / 'hosts' / 'worker-cleanup'
        spec = {'argv': [sys.executable, '-u', '-c', 'import time; time.sleep(60)'],
                'cwd': str(self.root), 'env': {'PATH': os.environ['PATH']},
                'provider': 'claude', 'locks': []}
        client = await HostClient.launch(directory, spec)
        host_pid = int((directory / 'pid').read_text())
        child_pid = client.child_pid
        client.detach()
        (directory / 'pid').unlink()
        stop_test_hosts(self.root)
        await self.until(lambda: HostClient(directory).state != 'running' and not alive(child_pid))
        self.assertFalse(alive(host_pid))

    async def test_host_accepts_one_mib_write_request(self):
        directory = self.state / 'hosts' / 'worker-large-write'
        script = ('import json,sys; '
                  '[(print(json.dumps({"line":line.rstrip(chr(10))}),flush=True)) for line in sys.stdin]')
        spec = {'argv': [sys.executable, '-u', '-c', script], 'cwd': str(self.root),
                'env': {'PATH': os.environ['PATH']}, 'provider': 'claude', 'locks': []}
        client = await HostClient.launch(directory, spec)
        try:
            overhead = len((json.dumps({'op': 'write', 'line': ''}) + '\n').encode())
            line = 'x' * (1024 * 1024 - overhead)
            self.assertEqual(len((json.dumps({'op': 'write', 'line': line}) + '\n').encode()), 1024 * 1024)
            response = await asyncio.wait_for(client.request('write', line=line), 10)
            self.assertEqual(response, {'ok': True})
            self.assertEqual(json.loads((directory / 'writes.jsonl').read_text()), {'seq': 1, 'line': line})
            event = await asyncio.wait_for(client.next_event(), 10)
            self.assertEqual(json.loads(event), {'line': line})
            client.ack()
        finally:
            await client.stop()
            client.detach()

    async def test_socket_survives_sighup_and_stop_kills_provider_group(self):
        directory = self.state / 'hosts' / 'worker-socket'
        script = ('import json,signal,sys; signal.signal(signal.SIGTERM,signal.SIG_IGN); '
                  '[(print(json.dumps({"line":line.strip()}),flush=True)) for line in sys.stdin]')
        spec = {'argv': [sys.executable, '-u', '-c', script], 'cwd': str(self.root),
                'env': {'PATH': os.environ['PATH']}, 'provider': 'claude', 'locks': []}
        client = await HostClient.launch(directory, spec)
        os.kill(int((directory / 'pid').read_text()), signal.SIGHUP)
        self.assertEqual(client.state, 'running')
        await client.request('write', line='first')
        self.assertEqual(json.loads(await client.next_event()), {'line': 'first'})
        client.ack()
        client.detach()
        client = HostClient.attach(directory, last_seq=1)
        await client.request('write', line='second')
        self.assertEqual(json.loads(await client.next_event()), {'line': 'second'})
        client.ack()
        await client.stop()
        self.assertEqual(client.returncode, -signal.SIGKILL)
        client.detach()

    async def test_launch_timeout_stops_its_host_and_provider(self):
        directory = self.state / 'hosts' / 'worker-timeout'
        ready = self.root / 'child-ready'
        script = ('import pathlib,signal,sys,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); '
                  'pathlib.Path(sys.argv[1]).touch(); time.sleep(30)')
        spec = {'argv': [sys.executable, '-u', '-c', script], 'cwd': str(self.root),
                'env': {'PATH': os.environ['PATH']}, 'provider': 'claude', 'locks': []}
        spec['argv'].append(str(ready))

        def clock():
            return time.monotonic() + (61 if ready.exists() else 0)

        with patch.object(HostClient, 'state', property(lambda self: 'dead')):
            with patch('coordinator.host.time', type('Clock', (), {'monotonic': staticmethod(clock)})):
                with self.assertRaisesRegex(RuntimeError, 'did not become ready'):
                    await HostClient.launch(directory, spec)

        host_pid = int((directory / 'pid').read_text())
        child_pid = int((directory / 'child.pid').read_text())
        self.assertFalse(alive(host_pid))
        self.assertFalse(alive(child_pid))

    async def test_codex_startup_resumes_from_journaled_request(self):
        sid = 'd7798257-5b85-4464-a5c1-a6cd11182c88'
        binary = self.root / 'codex'
        binary.write_text('''#!/usr/bin/env python3
import json,sys
def read():
    return json.loads(sys.stdin.readline())
def emit(event):
    print(json.dumps(event),flush=True)
first=read()
assert first['method']=='initialize'
emit({'id':first['id'],'result':{}})
assert read()['method']=='initialized'
request=read()
assert request['method']=='thread/resume'
emit({'id':request['id'],'result':{'thread':{'id':'d7798257-5b85-4464-a5c1-a6cd11182c88'}}})
request=read()
assert request['method']=='turn/start'
emit({'id':request['id'],'result':{'turn':{'id':'turn-1'}}})
emit({'method':'turn/started','params':{'threadId':'d7798257-5b85-4464-a5c1-a6cd11182c88','turn':{'id':'turn-1'}}})
emit({'method':'turn/completed','params':{'threadId':'d7798257-5b85-4464-a5c1-a6cd11182c88','turn':{'id':'turn-1','status':'completed'}}})
sys.stdin.read()
''')
        binary.chmod(0o700)
        directory = self.state / 'hosts' / 'worker-codex'
        spec = {'argv': [str(binary), 'app-server', '--listen', 'stdio://'],
                'cwd': str(self.root), 'env': {'PATH': os.environ['PATH']},
                'provider': 'codex', 'locks': ['codex-' + sid],
                'initial_payload': 'task', 'fresh': False, 'model': None}
        first = await HostClient.launch(directory, spec)
        await first.request('write', line=json.dumps({'id': 1, 'method': 'initialize',
            'params': {'clientInfo': {'name': 'torii', 'version': '1'}}}))
        self.assertEqual(json.loads(await first.next_event())['id'], 1)
        first.ack()
        first.detach()
        saved = []
        runner = ProviderRunner(self.state, binaries={'codex': str(binary)})
        result = await runner.run('codex', 'task', self.root, sid, fresh=False,
                                  control=RunControl(receipt_timeout=.3), host_id='worker-codex',
                                  attach=True, last_seq=1, on_seq=saved.append)
        self.assertTrue(result.success, result.error)
        self.assertTrue(saved)
        self.assertTrue(all(seq > 1 for seq in saved))

    async def test_claude_initial_input_recovers_without_repeating_it(self):
        runner = self.runner({'claude': str(self.binary)})
        for wrote in (False, True):
            sid = str(uuid.uuid4())
            name = 'worker-initial-' + str(int(wrote))
            directory = self.state / 'hosts' / name
            spec = {'argv': [str(self.binary), '--session-id', sid,
                             '--input-format', 'stream-json', '--replay-user-messages'],
                    'cwd': str(self.root), 'env': {'PATH': os.environ['PATH']},
                    'provider': 'claude', 'locks': ['claude-' + sid],
                    'initial_payload': 'original task', 'fresh': True, 'model': None}
            first = await HostClient.launch(directory, spec)
            if wrote:
                await first.request('write', line=json.dumps({'type': 'user', 'uuid': str(uuid.uuid4()),
                    'session_id': sid, 'parent_tool_use_id': None,
                    'message': {'role': 'user', 'content': 'original task'}}))
            first.detach()
            result = await runner.run('claude', 'changed task', self.root, sid, fresh=True,
                                      control=RunControl(), host_id=name, attach=True, account_alias='test')
            self.assertTrue(result.success, result.error)
            lines = HostClient(directory).written_lines()
            self.assertEqual(len(lines), 1)
            self.assertEqual(json.loads(lines[0])['message']['content'], 'original task')

    async def test_service_shutdown_and_restart_reattach_both_hosts(self):
        store = Store(self.state)
        self.add_named_account(store)
        with store.db:
            store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                             ('home', -100, 1, 'Home', str(self.root)))
            store.put('coordinator_home_topic', 'home')
            task = store.task_create('home', 'Work', worktree=str(self.root))
            worker_id = store.db.execute('''INSERT INTO workers
                (task,topic,provider,prompt,cwd,workspace,created,updated)
                VALUES (?,?,?,?,?,?,1,1)''',
                (task['id'], 'home', 'claude', 'wait', str(self.root), '{}')).lastrowid
        runner = self.runner({'claude': str(self.binary)})
        service = Service(store, FakeTelegram(), runner, self.root)
        first_run = asyncio.create_task(service.run())
        try:
            worker_dir = self.state / 'hosts' / ('worker-' + str(worker_id))
            await self.until(lambda: service.session is not None and
                             store.db.execute('SELECT pid FROM workers WHERE id=?',
                                              (worker_id,)).fetchone()[0] is not None)
            coordinator_dir = self.state / 'hosts' / store.get('coordinator_host')['id']
            await self.until(lambda: (coordinator_dir / 'child.pid').exists())
            worker_pid = int((worker_dir / 'child.pid').read_text())
            coordinator_pid = int((coordinator_dir / 'child.pid').read_text())
            first_run.cancel()
            await asyncio.gather(first_run, return_exceptions=True)
            await self.until(lambda: (worker_dir / 'events.jsonl').read_text())
            self.assertTrue(alive(worker_pid))
            self.assertTrue(alive(coordinator_pid))
            store.close()
            store = Store(self.state)
            runner = ProviderRunner(self.state, binaries={'claude': str(self.binary)})
            resumed = Service(store, FakeTelegram(), runner, self.root)
            second_run = asyncio.create_task(resumed.run())
            try:
                await self.until(lambda: resumed.session is not None and
                                 worker_id in resumed.worker_tasks)
                self.assertEqual(int((coordinator_dir / 'child.pid').read_text()), coordinator_pid)
                self.assertEqual(int((worker_dir / 'child.pid').read_text()), worker_pid)
                row = store.message_save('home', 'owner', 'after restart')
                await self.until(lambda: store.db.execute('SELECT delivered FROM messages WHERE id=?',
                    (row['id'],)).fetchone()[0] == 'received')
                inputs = [json.loads(line) for line in (self.root / 'inputs.jsonl').read_text().splitlines()]
                self.assertTrue(any(item['pid'] == coordinator_pid and
                    'after restart' in item['event']['message']['content'] for item in inputs))
                (self.root / ('release-' + str(worker_pid))).touch()
                await self.until(lambda: store.db.execute('SELECT status FROM workers WHERE id=?',
                    (worker_id,)).fetchone()[0] == 'done')
                result = store.db.execute('SELECT kind FROM messages WHERE source_worker=?',
                                          (worker_id,)).fetchone()
                self.assertEqual(result['kind'], 'worker_result')
            finally:
                second_run.cancel()
                await asyncio.gather(second_run, return_exceptions=True)
        finally:
            if not first_run.done():
                first_run.cancel()
                await asyncio.gather(first_run, return_exceptions=True)
            store.close()

    async def test_replay_saved_while_detached_confirms_the_message_after_reattach(self):
        store = Store(self.state)
        self.add_named_account(store)
        with store.db:
            store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                             ('home', -100, 1, 'Home', str(self.root)))
            store.put('coordinator_home_topic', 'home')
        runner = ProviderRunner(self.state, binaries={'claude': str(self.binary)})
        service = Service(store, FakeTelegram(), runner, self.root)
        first_run = asyncio.create_task(service.run())
        try:
            row = store.message_save('home', 'owner', 'before restart')
            await self.until(lambda: store.db.execute('SELECT receipt FROM messages WHERE id=?',
                (row['id'],)).fetchone()[0] == 'replayed')
            coordinator_pid = int((self.state / 'hosts' / store.get('coordinator_host')['id'] /
                                   'child.pid').read_text())
            first_run.cancel()
            await asyncio.gather(first_run, return_exceptions=True)
            with store.db:
                store.put('coordinator_host_seq', 0)
                store.db.execute("UPDATE messages SET delivered='received',receipt='written' WHERE id=?",
                                 (row['id'],))
            store.close()
            store = Store(self.state)
            resumed = Service(store, FakeTelegram(),
                              ProviderRunner(self.state, binaries={'claude': str(self.binary)}), self.root)
            second_run = asyncio.create_task(resumed.run())
            try:
                await self.until(lambda: store.db.execute('SELECT delivered,receipt FROM messages WHERE id=?',
                    (row['id'],)).fetchone()[:] == ('received', 'replayed'))
                self.assertEqual(int((self.state / 'hosts' / store.get('coordinator_host')['id'] /
                                      'child.pid').read_text()), coordinator_pid)
            finally:
                second_run.cancel()
                await asyncio.gather(second_run, return_exceptions=True)
        finally:
            if not first_run.done():
                first_run.cancel()
                await asyncio.gather(first_run, return_exceptions=True)
            store.close()

    async def test_host_killed_without_exit_record_is_reaped_and_fails_as_host_died(self):
        runner = self.runner({'claude': str(self.binary)})
        directory = self.state / 'hosts' / 'worker-dead'
        run = asyncio.create_task(runner.run('claude', 'wait', self.root, str(uuid.uuid4()), fresh=True,
                                             control=RunControl(), host_id='worker-dead', account_alias='test'))
        await self.until(lambda: (directory / 'events.jsonl').exists() and (directory / 'events.jsonl').read_text())
        host_pid = int((directory / 'pid').read_text())
        os.kill(host_pid, signal.SIGKILL)
        result = await asyncio.wait_for(run, 8)
        self.assertEqual(result.failure_code, 'host_died')
        self.assertFalse(result.success)
        self.assertEqual(HostClient(directory).state, 'dead')
        self.assertFalse(alive(host_pid))

    async def test_finished_detached_worker_and_dead_host(self):
        store = Store(self.state)
        self.add_named_account(store)
        service2 = None
        try:
            with store.db:
                store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                                 ('home', -100, 1, 'Home', str(self.root)))
                store.put('coordinator_home_topic', 'home')
                task = store.task_create('home', 'Work', worktree=str(self.root))
                ids = []
                for prompt in ('wait', 'wait', 'wait'):
                    ids.append(store.db.execute('''INSERT INTO workers
                        (task,topic,provider,prompt,cwd,workspace,created,updated)
                        VALUES (?,?,?,?,?,?,1,1)''',
                        (task['id'], 'home', 'claude', prompt, str(self.root), '{}')).lastrowid)
            runner = ProviderRunner(self.state, binaries={'claude': str(self.binary)})
            service = Service(store, FakeTelegram(), runner, self.root, pair_only=True)
            service.pair_only = False
            await service.workers_once()
            await self.until(lambda: all((self.state / 'hosts' / ('worker-' + str(i)) / 'pid').exists()
                                         for i in ids))
            await self.until(lambda: all((self.state / 'hosts' / ('worker-' + str(i)) /
                                         'events.jsonl').read_text() for i in ids))
            await self.until(lambda: all(store.db.execute('SELECT last_seq FROM workers WHERE id=?',
                                         (i,)).fetchone()[0] >= 1 for i in ids))
            for task in service.worker_tasks.values():
                task.cancel()
            await asyncio.gather(*service.worker_tasks.values(), return_exceptions=True)
            finished = self.state / 'hosts' / ('worker-' + str(ids[0]))
            (self.root / ('release-' + (finished / 'child.pid').read_text())).touch()
            try:
                await self.until(lambda: (finished / 'exit.json').exists())
            except asyncio.TimeoutError:
                self.fail(json.dumps({'files': [p.name for p in self.root.iterdir()],
                    'events': (finished / 'events.jsonl').read_text(),
                    'stderr': (finished / 'stderr.log').read_text(),
                    'pid': (finished / 'child.pid').read_text()}))
            dead = self.state / 'hosts' / ('worker-' + str(ids[1]))
            host_pid = int((dead / 'pid').read_text())
            os.kill(host_pid, signal.SIGKILL)
            os.waitpid(host_pid, 0)
            await self.until(lambda: not alive(host_pid))
            service2 = Service(store, FakeTelegram(), runner, self.root)
            await service2.workers_once()
            await self.until(lambda: store.db.execute('SELECT status FROM workers WHERE id=?',
                (ids[0],)).fetchone()[0] == 'done')
            finished_row = store.db.execute('SELECT result,last_seq FROM workers WHERE id=?',
                                            (ids[0],)).fetchone()
            self.assertEqual(json.loads(finished_row['result'])['text'], 'worker done')
            self.assertGreater(finished_row['last_seq'], 1)
            dead_row = store.db.execute('SELECT status,result FROM workers WHERE id=?', (ids[1],)).fetchone()
            self.assertEqual(dead_row['status'], 'interrupted', dead_row['result'])
            third = self.state / 'hosts' / ('worker-' + str(ids[2]))
            self.assertEqual(HostClient(third).state, 'running',
                json.dumps({'events': (third / 'events.jsonl').read_text(),
                            'exit': (third / 'exit.json').exists(),
                            'files': [p.name for p in third.iterdir()]}))
            with store.db:
                store.service_request('workers.stop', {'worker': ids[2]})
            await service2.controls_once()
            stopped = store.db.execute('SELECT status,result FROM workers WHERE id=?', (ids[2],)).fetchone()
            self.assertEqual(stopped['status'], 'interrupted')
            self.assertEqual(json.loads(stopped['result'])['failure_code'], 'owner_stopped')
        finally:
            if service2:
                for task in service2.worker_tasks.values():
                    task.cancel()
                await asyncio.gather(*service2.worker_tasks.values(), return_exceptions=True)
            store.close()
