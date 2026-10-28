import asyncio
from contextlib import contextmanager
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch

from coordinator import control_api, host
from coordinator.accounts import AccountBroker
from coordinator.providers import ProviderRunner
from coordinator.service import Service
from coordinator.store import Store
from tests.support import FakeVault
from coordinator.workers import WorkerPool, host_directory
from tests.fake_opencode import install_fake_opencode
from tests.support import _process_table, isolate_shared_mcp_home, read_jsonl, settle, stop_test_hosts, until
from tests.test_codex_secrets_goals import SERVER
from tests.test_opencode_runtime import ACCOUNT


FAKE_CLAUDE = '''#!/usr/bin/env python3
import json,sys,time
from pathlib import Path
root=Path(__file__).parent
sid=sys.argv[sys.argv.index('--session-id')+1]
print(json.dumps({'type':'system','subtype':'init','session_id':sid}),flush=True)
for line in sys.stdin:
    event=json.loads(line)
    with (root/'inputs.jsonl').open('a') as log:
        log.write(json.dumps(event)+'\\n')
    print(json.dumps({**event,'isReplay':True}),flush=True)
    while True:
        time.sleep(.05)
'''


PROCESS_PROBE = '''import json,os,subprocess,sys
from pathlib import Path
grandchild=subprocess.Popen([sys.executable,'-c','import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(300)'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
probe=Path(PROBE)
pending=probe.with_suffix('.tmp')
pending.write_text(json.dumps({'provider':os.getpid(),'grandchild':grandchild.pid}))
pending.replace(probe)
'''


GATED_HOST = '''import asyncio,os,sys
from pathlib import Path
from coordinator import host
directory=Path(sys.argv[1])
phase=sys.argv[4]
def gate():
    Path(sys.argv[3]).touch()
    os.read(int(sys.argv[2]),1)
write=host._write
def gated_write(path,value):
    if (phase=='before-pid' and path.name=='pid'
            or phase=='before-child-pid' and path.name=='child.pid'):
        gate()
    write(path,value)
host._write=gated_write
launch=host.extension.active().launch_process
async def gated_launch(*args,**kwargs):
    if phase=='before-child':
        gate()
    return await launch(*args,**kwargs)
host.extension.active().launch_process=gated_launch
serve=asyncio.start_unix_server
async def gated_serve(*args,**kwargs):
    if phase=='before-socket':
        gate()
    return await serve(*args,**kwargs)
asyncio.start_unix_server=gated_serve
host.main(str(directory))
'''


class WorkerLaunchStopTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        isolate_shared_mcp_home(self, self.root)
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        self.addCleanup(stop_test_hosts, self.root)
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test',?,1)",
                              (str(self.root),))
        self.task = self.store.task_create('1:2', 'Build', worktree=str(self.root))
        self.key_home = self.root / 'dummy-account'
        self.key_home.mkdir()
        (self.key_home / 'key').write_text('dummy-only\n')
        (self.key_home / 'key').chmod(0o600)
        self.store.put('opencode_accounts', {'one': dict(ACCOUNT)})
        self.store.put('opencode_active_account', 'one')
        self.store.put('opencode_enabled', True)
        profile = self.root / 'claude-profile'
        profile.mkdir()
        self.store.put('accounts', {'primary': {'config_dir': str(profile), 'enabled': True}})
        self.store.put('account_status', {'primary': {
            'identity': {'email': 'primary@example.com', 'logged_in': True}, 'observed_at': time.time(),
            'usage': {'seven_day': {'utilization': 10, 'resets_at': time.time() + 3600}}}})
        for target, options in (
                ('coordinator.providers.platform', {'new': 'linux'}),
                ('coordinator.opencode_accounts.account_home', {'return_value': self.key_home})):
            patched = patch(target, **options)
            patched.start()
            self.addCleanup(patched.stop)

    @contextmanager
    def paused_launch(self, phase, root):
        read_fd, write_fd = os.pipe()
        marker = root / 'launch-paused'
        launchers = []
        popen = subprocess.Popen
        launch_host = host.HostClient.launch

        def launch(args, *fields, **options):
            is_host = args[1:4] == ['-m', 'coordinator', 'host']
            if is_host:
                args = [args[0], '-u', '-c', GATED_HOST, args[-1], str(read_fd), str(marker), phase]
                options['pass_fds'] = (read_fd,)
            process = popen(args, *fields, **options)
            if is_host:
                launchers.append(process)
            return process

        async def launch_with_short_stop(*args, **options):
            return await launch_host(*args, **dict(options, stop_timeout=.05))

        try:
            with patch('coordinator.host.subprocess.Popen', launch), \
                    patch.object(host.HostClient, 'launch', launch_with_short_stop):
                yield marker, lambda: os.write(write_fd, b'x'), launchers
        finally:
            os.write(write_fd, b'x')
            os.close(write_fd)
            os.close(read_fd)

    def make_worker(self, provider, root):
        binary = install_fake_opencode(root / 'fake') if provider == 'opencode' else root / ('fake-' + provider)
        if provider == 'opencode':
            (binary.parent / 'scenario.json').write_text(json.dumps({'wait_cancel': True}))
            self.store.put('opencode_binary', str(binary))
            source = binary.read_text()
        elif provider == 'codex':
            source = SERVER.replace('SID', repr('00000000-0000-4000-8000-000000000001')).replace('ROOT', repr(str(root)))
        else:
            source = FAKE_CLAUDE
        probe = PROCESS_PROBE.replace('PROBE', repr(str(root / 'probe.json')))
        binary.write_text(source.split('\n', 1)[0] + '\n' + probe + source.split('\n', 1)[1])
        binary.chmod(0o700)
        runner = ProviderRunner(self.store.directory, binaries={provider: str(binary)}, opencode_store=self.store)
        pool = WorkerPool(self.store, runner, AccountBroker(self.store))
        worker_id = self.store.db.execute('''INSERT INTO workers
            (task,topic,provider,prompt,cwd,workspace,created,updated)
            VALUES (?,'1:2',?,'work',?,'{}',1,1)''', (self.task['id'], provider, str(self.root))).lastrowid
        return pool, pool.get(worker_id)

    def turn_started(self, provider, root):
        if provider == 'claude':
            return (root / 'inputs.jsonl').exists()
        if provider == 'codex':
            return (root / 'turn-started').exists()
        return any(call['message'].get('method') == 'session/prompt'
                   for call in read_jsonl(root / 'fake/calls.jsonl'))

    async def check_stop(self, provider, phase, delay):
        root = self.root / (provider + '-' + phase + '-' + str(delay))
        root.mkdir()
        pool, worker = self.make_worker(provider, root)
        with patch.dict(os.environ, {'FAKE_HOLD_TURN': '1'}), \
                self.paused_launch(phase, root) as (marker, release, launchers):
            running = asyncio.create_task(pool.run(worker, self.store.topic('1:2')))
            directory = host_directory(self.store.directory, worker)
            service = None
            try:
                if phase == 'running':
                    await until(lambda: pool.get(worker['id'])['pid'] is not None, 'worker ready')
                    await until(lambda: self.turn_started(provider, root), 'provider prompt')
                else:
                    await until(marker.exists, 'host launch gate')
                    self.assertFalse((directory / 'sock').exists())
                    if phase in ('before-child-pid', 'before-socket'):
                        await until((root / 'probe.json').exists, 'provider process tree')
                await asyncio.sleep(delay)
                observed = {'provider': provider, 'phase': phase, 'delay': delay,
                            'host_pid': (directory / 'pid').exists(),
                            'child_pid': (directory / 'child.pid').exists(),
                            'socket': (directory / 'sock').exists()}
                try:
                    if delay == .1:
                        service = Service(self.store, None, pool.runner, self.root, vault=FakeVault())
                        service.workers = pool
                        reply = control_api.call(self.store, 'workers.stop', {'worker': worker['id']}, topic='1:2')
                        self.assertTrue(reply.ok, reply.text)
                        await asyncio.wait_for(service.controls_once(), 10)
                    else:
                        await asyncio.wait_for(pool.stop(worker['id']), 10)
                except (OSError, RuntimeError) as error:
                    observed['stop_error'] = type(error).__name__
                release()
                try:
                    done = await settle(asyncio.shield(running))
                    result = json.loads(done['result'])
                    observed.update(status=done['status'], failure=result['failure_code'])
                except asyncio.TimeoutError:
                    observed['status'] = 'still running'
                if service is not None:
                    if running.done():
                        await settle(asyncio.gather(*service.stop_tasks.values()))
                    request = self.store.db.execute('SELECT state,result FROM service_requests WHERE id=?',
                                                    (reply.data['request'],)).fetchone()
                    observed['request_state'] = request['state']
                pids = json.loads((root / 'probe.json').read_text()) if (root / 'probe.json').exists() else {}
                pids['child'] = host.HostClient(directory).child_pid
                def remaining():
                    table = _process_table()
                    return [name for name, pid in pids.items()
                            if pid in table and not table[pid][2].startswith('Z')]
                if observed['status'] == 'interrupted':
                    await until(lambda: not remaining(), 'provider process tree exit')
                table = _process_table()
                observed['process_states'] = {name: table[pid][2] if pid in table else 'exited'
                                              for name, pid in pids.items()}
                observed['remaining'] = remaining()
                observed['prompt_started'] = self.turn_started(provider, root)
                if observed['status'] == 'interrupted':
                    await until(lambda: not any(process.poll() is None for process in launchers), 'host exit')
                observed['host_alive'] = any(process.poll() is None for process in launchers)
                print(json.dumps(observed, sort_keys=True), flush=True)
                self.assertNotIn('stop_error', observed, observed)
                self.assertEqual((observed['status'], observed.get('failure')), ('interrupted', 'owner_stopped'), observed)
                self.assertEqual(observed['remaining'], [], observed)
                self.assertFalse(observed['host_alive'], observed)
                self.assertEqual(observed['prompt_started'], phase == 'running', observed)
                if provider == 'opencode':
                    while pool.storage.tasks:
                        await asyncio.gather(*list(pool.storage.tasks.values()))
                        await asyncio.sleep(0)
                    self.assertEqual(pool.storage.state(worker['id'])['pending'], 0)
                    launch = Path(json.loads((directory / 'spec.json').read_text())['env']['XDG_CONFIG_HOME']).parent
                    self.assertFalse((launch / 'tmp').exists())
                    self.assertFalse((launch / 'home/Library/Caches').exists())
                if service is not None:
                    self.assertEqual(observed['request_state'], 'done')
                    self.assertEqual(json.loads(request['result']), {'stop': 'stopped'})
            finally:
                release()
                if not running.done():
                    await until(lambda: pool.get(worker['id'])['pid'] is not None or running.done(), 'cleanup readiness')
                    await settle(pool.stop(worker['id']))
                await settle(asyncio.gather(running, return_exceptions=True))

    async def check_sweep(self, provider):
        pool, worker = self.make_worker(provider, self.root)
        with self.assertRaises(host.HostDied):
            await pool.stop(worker['id'])
        self.assertNotIn(worker['id'], pool.stopping)
        for phase in ('before-pid', 'before-child', 'before-child-pid', 'before-socket', 'running'):
            for delay in (0, .02, .1):
                with self.subTest(provider=provider, phase=phase, delay=delay):
                    await self.check_stop(provider, phase, delay)

    def test_process_probe_is_complete_when_visible(self):
        for partial in ('', '{"provider":'):
            with self.subTest(partial=partial):
                root = self.root / ('probe-' + str(len(partial)))
                root.mkdir()
                probe = root / 'probe.json'
                observed = []

                def interrupted_write(path, value):
                    with path.open('w') as output:
                        output.write(partial)
                        output.flush()
                        observed.append(json.loads(probe.read_text()) if probe.exists() else {})
                        output.write(value[len(partial):])
                    return len(value)

                with patch.object(Path, 'write_text', interrupted_write), patch('subprocess.Popen') as launch:
                    launch.return_value.pid = 12345
                    exec(PROCESS_PROBE.replace('PROBE', repr(str(probe))), {})
                self.assertEqual(observed, [{}])
                self.assertEqual(json.loads(probe.read_text()), {'provider': os.getpid(), 'grandchild': 12345})

    async def test_codex_stop_delay_sweep(self):
        await self.check_sweep('codex')

    async def test_opencode_stop_delay_sweep(self):
        await self.check_sweep('opencode')

    async def test_claude_stop_delay_sweep(self):
        await self.check_sweep('claude')
