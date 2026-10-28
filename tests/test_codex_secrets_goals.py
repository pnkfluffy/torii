import asyncio
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, patch
import uuid

from coordinator.native_protocol import RunControl
from coordinator.providers import CODEX_SECRET_OVERRIDES, ProviderRunner
from coordinator.store import Store
from tests.support import isolate_shared_mcp_home, settle, stop_test_hosts, until, until_exists


SERVER = '''#!/usr/bin/env python3
import fcntl, json, os, subprocess, sys, threading, time
from pathlib import Path
sid = SID
root = Path(ROOT)
ownership = (root / 'app-server.lock').open('a')
fcntl.flock(ownership, fcntl.LOCK_EX | fcntl.LOCK_NB)
with (root / 'launches.jsonl').open('a') as launches:
    launches.write(json.dumps({'pid': os.getpid(), 'argv': sys.argv}) + '\\n')
goals_enabled = 'features.goals=true' in sys.argv
goal = None
turn = None
prompt_started = False
cleared = False
def emit(value):
    print(json.dumps(value), flush=True)
def notify(method, **params):
    emit({'method': method, 'params': {'threadId': sid, **params}})
def answer(request, value):
    emit({'id': request['id'], 'result': value})
def completed():
    value = os.environ.get('FAKE_SECRET', 'no-secret')
    notify('item/completed', turnId=turn, item={'type': 'agentMessage', 'text': 'used ' + value})
    notify('turn/completed', turn={'id': turn, 'status': 'completed'})
def finish_later():
    global turn
    while not (root / 'finish-turn').exists():
        time.sleep(.01)
    if turn == 'turn-1':
        completed()
        turn = None
for line in sys.stdin:
    request = json.loads(line)
    method = request.get('method')
    params = request.get('params') or {}
    print(json.dumps(request), file=sys.stderr, flush=True)
    if method and method.startswith('thread/goal/') and not goals_enabled:
        emit({'id': request['id'], 'error': {'code': -32600, 'message': 'goals feature is disabled'}})
        continue
    if method == 'initialize':
        answer(request, {})
    elif method == 'config/read':
        config = {'shell_environment_policy': {'inherit': 'none', 'ignore_default_excludes': False,
            'exclude': ['*SECRET*'], 'include_only': ['PATH'], 'filters': {'*TOKEN*': 'exclude'}}}
        for index, arg in enumerate(sys.argv[:-1]):
            if arg != '-c':
                continue
            key, value = sys.argv[index + 1].split('=', 1)
            try:
                value = json.loads(value)
            except ValueError:
                continue
            target = config
            parts = key.split('.')
            for part in parts[:-1]:
                target = target.setdefault(part, {})
            target[parts[-1]] = value
            if key == 'shell_environment_policy.exclude':
                target.pop('filters', None)
        if os.environ.get('FAKE_CONFIG_REPLACE'):
            config['shell_environment_policy']['set'] = {'FAKE_SECRET': 'configured-placeholder'}
        if os.environ.get('FAKE_CONFIG_UNSAFE'):
            config['memories']['generate_memories'] = True
        if os.environ.get('FAKE_STRUCTURED_FEATURE'):
            config['features']['multi_agent_v2'] = {'enabled': config['features']['multi_agent_v2']}
        answer(request, {'config': config})
    elif method in ('thread/start', 'thread/resume'):
        environment = dict(os.environ)
        if os.environ.get('FAKE_SECRET'):
            policy = config['shell_environment_policy']
            assert policy['inherit'] == 'all'
            if not policy['ignore_default_excludes']:
                environment = {k: v for k, v in environment.items() if not any(s in k for s in ('KEY', 'SECRET', 'TOKEN'))}
            if policy['exclude'] or policy['include_only']:
                environment.pop('FAKE_SECRET', None)
        child = subprocess.run([sys.executable, '-c',
            "import json,os; print(json.dumps({k:len(os.environ[k]) for k in ('FAKE_SECRET','NPM_TOKEN') if k in os.environ}))"],
            env=environment, text=True, capture_output=True, check=True)
        (root / 'shell-seen').write_text(child.stdout)
        answer(request, {'thread': {'id': sid}})
    elif method == 'turn/start':
        prompt_started = True
        turn = 'turn-1'
        answer(request, {'turn': {'id': turn}})
        notify('turn/started', turn={'id': turn})
        (root / 'turn-started').touch()
        if os.environ.get('FAKE_WAIT_FINISH'):
            threading.Thread(target=finish_later, daemon=True).start()
        if goal is None and not os.environ.get('FAKE_HOLD_TURN') or os.environ.get('FAKE_PROMPT_FAST'):
            if os.environ.get('FAKE_SECRET'):
                print('diagnostic ' + os.environ['FAKE_SECRET'], file=sys.stderr, flush=True)
            completed()
    elif method == 'thread/goal/set':
        if os.environ.get('FAKE_GOAL_SET_ERROR'):
            emit({'id': request['id'], 'error': {'code': -32600, 'message': 'goal refused'}})
            continue
        goal = dict(goal or {}, **params)
        goal.setdefault('objective', 'missing')
        goal.setdefault('status', 'active')
        if goal['status'] == 'active':
            if params.get('objective') == 'changed goal' or os.environ.get('FAKE_BLOCK_RESUMED_GOAL') and not prompt_started:
                (root / 'change-sent').touch()
                while not (root / 'ack-change').exists():
                    time.sleep(.01)
        answer(request, {'goal': goal})
        notify('thread/goal/updated', goal=goal)
        if goal['status'] == 'active' and turn is None:
            if os.environ.get('FAKE_DELAY_GOAL_TURN'):
                time.sleep(.2)
            turn = 'turn-2'
            notify('turn/started', turn={'id': turn})
            (root / 'continued').touch()
        if goal['status'] == 'active' and turn == 'turn-1':
            continuation = os.environ.get('FAKE_GOAL_CONTINUATION')
            if continuation:
                notify('item/completed', turnId=turn, item={'type': 'agentMessage', 'text': 'first turn'})
                if not os.environ.get('FAKE_GOAL_OVERLAP'):
                    completed()
                turn = 'turn-2'
                if continuation == 'started':
                    notify('turn/started', turn={'id': turn})
                elif continuation == 'item-started':
                    notify('item/started', turnId=turn, item={'type': 'agentMessage'})
                notify('item/completed', turnId=turn, item={'type': 'agentMessage', 'text': 'final goal result'})
                goal['status'] = 'complete'
                notify('thread/goal/updated', goal=goal)
                notify('turn/completed', turn={'id': turn,
                    'status': os.environ.get('FAKE_GOAL_FINAL_STATUS', 'completed')})
                turn = None
                continue
            if not os.environ.get('FAKE_PROMPT_FAST'):
                completed()
            if os.environ.get('FAKE_GOAL_STATUS'):
                goal['status'] = os.environ['FAKE_GOAL_STATUS']
                notify('thread/goal/updated', goal=goal)
                turn = None
            else:
                turn = 'turn-2'
                notify('turn/started', turn={'id': turn})
                (root / 'continued').touch()
    elif method == 'turn/steer':
        assert params['expectedTurnId'] == turn
        answer(request, {'turnId': turn})
    elif method == 'thread/goal/get':
        if os.environ.get('FAKE_GOAL_GET_ERROR'):
            emit({'id': request['id'], 'error': {'code': -32601, 'message': 'unsupported'}})
        else:
            answer(request, {'goal': goal})
        if cleared and turn is not None:
            completed()
            turn = None
    elif method == 'thread/goal/clear':
        goal = None
        cleared = True
        answer(request, {'cleared': True})
        notify('thread/goal/cleared')
'''


class CodexSecretsGoalsTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.addCleanup(self.temporary.cleanup)
        isolate_shared_mcp_home(self, self.root)
        self.addCleanup(stop_test_hosts, self.root)
        self.sid = str(uuid.uuid4())
        self.binary = self.root / 'fake-codex'
        self.binary.write_text(SERVER.replace('SID', repr(self.sid)).replace('ROOT', repr(str(self.root))))
        self.binary.chmod(0o700)
        self.runner = ProviderRunner(self.root / 'state', binaries={'codex': str(self.binary)})
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        self.runner.opencode_store = self.store

    async def run_worker(self, control=None, **options):
        fresh = options.pop('fresh', True)
        return await self.runner.run('codex', 'job prompt', self.root, None if fresh else self.sid,
                                     fresh=fresh, control=control or RunControl(), **options)

    def service_workers(self, count=1):
        from coordinator import control_api
        from coordinator.service import Service
        from tests.support import FakeVault
        topic = '-10042:4'
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?, -10042, 4, 'Torii', ?, 1)",
                              (topic, str(self.root)))
        workers = []
        for index in range(count):
            task = self.store.task_create(topic, 'Queue test %d' % index, worktree=str(self.root))
            spawned = control_api.call(self.store, 'workers.spawn', {
                'task': task['id'], 'provider': 'codex', 'prompt': 'job prompt'})
            self.assertTrue(spawned.ok, spawned.text)
            workers.append(spawned.data['worker'])
        return Service(self.store, None, self.runner, self.root, vault=FakeVault()), topic, workers

    def request(self, op, worker, **params):
        from coordinator import control_api
        called = control_api.call(self.store, op, dict(params, worker=worker))
        self.assertTrue(called.ok, called.text)
        return self.store.db.execute('SELECT MAX(id) FROM service_requests').fetchone()[0]

    def request_state(self, request):
        return self.store.db.execute('SELECT state FROM service_requests WHERE id=?', (request,)).fetchone()[0]

    async def reattach_with_pending_goal(self):
        from coordinator.service import Service
        from tests.support import FakeVault
        first, topic, (worker,) = self.service_workers()
        run = asyncio.create_task(first.workers.run(first.workers.get(worker), self.store.topic(topic)))
        await until_exists(self.root / 'turn-started')
        request = self.request('workers.goal', worker, condition='saved late goal')
        queue = asyncio.create_task(first.controls_once())
        await until(lambda: self.request_state(request) == 'sending')
        await until(lambda: not first.workers.controls[worker]._ready.is_set())
        queue.cancel()
        await first.shutdown([queue, run])
        self.store.requests_uncertain()
        self.assertEqual(self.request_state(request), 'uncertain')
        second = Service(self.store, None, self.runner, self.root, vault=FakeVault())
        run = asyncio.create_task(second.workers.run(second.workers.get(worker), self.store.topic(topic)))
        await until(lambda: worker in second.workers.controls)
        return second, run, worker

    async def test_a_late_goal_does_not_hold_other_controls_or_stops(self):
        with patch.dict(os.environ, {'FAKE_HOLD_TURN': '1', 'FAKE_WAIT_FINISH': '1'}):
            service, topic, (held, other) = self.service_workers(2)
            run = asyncio.create_task(service.workers.run(service.workers.get(held), self.store.topic(topic)))
            await until_exists(self.root / 'turn-started')
            self.request('workers.goal', held, condition='late goal')
            steer = self.request('workers.steer', other, prompt='note for the other job')
            other_stop = self.request('workers.stop', other)
            held_stop = self.request('workers.stop', held)
            try:
                await asyncio.wait_for(service.controls_once(), 5)
                self.assertEqual(self.request_state(steer), 'done')
                self.assertEqual(self.request_state(other_stop), 'done')
                self.assertEqual(self.request_state(held_stop), 'done')
                self.assertIn('note for the other job', service.workers.get(other)['prompt'])
                done = await settle(run)
                self.assertEqual(json.loads(done['result'])['failure_code'], 'owner_stopped')
                self.assertFalse((self.root / 'finish-turn').exists())
            finally:
                await service.shutdown([run])

    async def test_a_stop_in_the_launch_window_does_not_hold_other_controls(self):
        from coordinator.host import HostClient
        original = HostClient.launch
        entered = asyncio.Event()
        gate = asyncio.Event()

        async def gated(directory, spec, **options):
            entered.set()
            await gate.wait()
            return await original(directory, spec, **options)

        with patch.object(HostClient, 'launch', gated):
            service, topic, (worker, other) = self.service_workers(2)
            run = asyncio.create_task(service.workers.run(service.workers.get(worker), self.store.topic(topic)))
            await asyncio.wait_for(entered.wait(), 10)
            stop = self.request('workers.stop', worker)
            repeated = self.request('workers.stop', worker)
            steer = self.request('workers.steer', other, prompt='continue the other job')
            with patch.object(self.store, 'service_request_settle', wraps=self.store.service_request_settle) as settles:
                try:
                    await asyncio.wait_for(service.controls_once(), 2)
                    self.assertEqual(self.request_state(steer), 'done')
                    self.assertEqual(self.request_state(stop), 'sending')
                    self.assertEqual(self.request_state(repeated), 'sending')
                    self.assertFalse(run.done())
                    gate.set()
                    done = await settle(run)
                    await until(lambda: self.request_state(stop) == self.request_state(repeated) == 'done')
                    await service.controls_once()
                    await service.controls_once()
                    self.assertEqual(json.loads(done['result'])['failure_code'], 'owner_stopped')
                    self.assertEqual(done['status'], 'interrupted')
                    self.assertFalse((self.root / 'turn-started').exists())
                    for request in (stop, repeated):
                        self.assertEqual([call.args[1] for call in settles.call_args_list
                                          if call.args[0] == request], ['sending', 'done'])
                    self.assertEqual(self.store.db.execute(
                        "SELECT COUNT(*) FROM messages WHERE source_worker=? AND kind='worker_result'",
                        (worker,)).fetchone()[0], 1)
                finally:
                    gate.set()
                    await service.shutdown([run])

    async def test_a_waiting_control_error_reaches_the_supervised_loop(self):
        service, topic, (worker,) = self.service_workers()
        self.store.db.execute("UPDATE workers SET status='running' WHERE id=?", (worker,))
        control = RunControl()

        async def fail(message_id, condition):
            raise RuntimeError('control receipt failed')

        control.bind(None, fail)
        service.workers.controls[worker] = control
        request = self.request('workers.goal', worker, condition='goal that raises')
        try:
            await service.controls_once()
            await until(lambda: service.control_tasks[worker].done())
            with self.assertRaisesRegex(RuntimeError, 'control receipt failed'):
                await service.controls_once()
            self.assertEqual(self.request_state(request), 'sending')
        finally:
            await service.shutdown([])

    async def test_goal_replies_do_not_block_the_controls_loop(self):
        service, topic, (worker,) = self.service_workers()
        self.store.db.execute("UPDATE workers SET status='running' WHERE id=?", (worker,))
        gate = asyncio.Event()

        async def receive(message_id, condition):
            await gate.wait()
            return 'received'

        try:
            gate.clear()
            control = RunControl()
            control.bind(None, receive)
            service.workers.controls[worker] = control
            request = self.request('workers.goal', worker, condition='late goal')
            await asyncio.wait_for(service.controls_once(), 2)
            self.assertEqual(self.request_state(request), 'sending')
            gate.set()
            await until(lambda: self.request_state(request) == 'done')
            await service.controls_once()
        finally:
            gate.set()
            await service.shutdown([])

    async def test_a_stop_before_the_first_on_demand_attempt_prevents_launch(self):
        original = ProviderRunner._run
        entered = asyncio.Event()
        gate = asyncio.Event()

        async def gated(runner, provider, prompt, cwd, session_id, **options):
            entered.set()
            await gate.wait()
            return await original(runner, provider, prompt, cwd, session_id, **options)

        with patch.object(ProviderRunner, '_run', gated):
            service, topic, (worker, other) = self.service_workers(2)
            run = asyncio.create_task(service.workers.run(service.workers.get(worker), self.store.topic(topic)))
            await asyncio.wait_for(entered.wait(), 10)
            stop = self.request('workers.stop', worker)
            steer = self.request('workers.steer', other, prompt='note for the other job')
            try:
                await asyncio.wait_for(service.controls_once(), 2)
                self.assertEqual(self.request_state(steer), 'done')
                self.assertIn(worker, service.workers.stopping)
                gate.set()
                done = await settle(run)
                await until(lambda: self.request_state(stop) == 'done')
                self.assertEqual(json.loads(done['result'])['failure_code'], 'owner_stopped')
                self.assertFalse((self.root / 'state/hosts').exists())
            finally:
                gate.set()
                await service.shutdown([run])

    async def test_a_missing_host_without_a_live_worker_task_is_uncertain(self):
        from coordinator.host import HostDied
        service, topic, (worker,) = self.service_workers()
        for task_state in ('missing', 'done'):
            with self.subTest(task=task_state):
                self.store.db.execute("UPDATE workers SET status='running' WHERE id=?", (worker,))
                control = RunControl()
                control.stop_before_launch = True
                service.workers.controls[worker] = control
                if task_state == 'done':
                    task = asyncio.create_task(asyncio.sleep(0))
                    await task
                    service.workers.tasks[worker] = task
                request = self.request('workers.stop', worker)
                with patch('coordinator.workers.HostClient.attach', side_effect=HostDied('host unavailable')):
                    await service.controls_once()
                self.assertEqual(self.request_state(request), 'uncertain')
                self.assertNotIn(worker, service.workers.stopping)
                self.assertEqual(service.workers.get(worker)['status'], 'running')
        await service.shutdown([])

    async def test_a_stop_before_an_on_demand_launch_closes_a_pending_control(self):
        control = RunControl()
        pending = asyncio.create_task(control.steer('pending', 'queued note'))
        await asyncio.sleep(0)
        try:
            result = await self.run_worker(control, stopped=lambda: True)
            self.assertEqual(result.failure_code, 'owner_stopped')
            self.assertFalse(result.success)
            self.assertEqual(await asyncio.wait_for(pending, 2), 'closed')
            self.assertFalse((self.root / 'launches.jsonl').exists())
            self.assertFalse((self.root / 'state/hosts').exists())
        finally:
            control.close()
            await asyncio.gather(pending, return_exceptions=True)

    async def test_a_stop_after_a_goal_restart_does_not_reload_revoked_secrets(self):
        from coordinator.providers import RunResult
        directory = self.root / 'old-host'
        directory.mkdir()
        (directory / 'spec.json').write_text(json.dumps({'secret_names': ['FAKE_SECRET']}))
        control = RunControl()
        pending = asyncio.create_task(control.goal('pending', 'late goal'))
        await asyncio.sleep(0)
        ended = RunResult(self.sid, failure_code='goal_feature_required',
                          goal_status={'objective': 'late goal'}, log_path=str(directory / 'stderr.log'))
        with patch.object(self.runner, '_run', AsyncMock(return_value=ended)):
            try:
                def revoked():
                    raise ValueError('the secret has been revoked')

                result = await self.run_worker(control, fresh=False, attach=True, reload_secrets=revoked,
                                               stopped=lambda: True)
                self.assertEqual(result.failure_code, 'owner_stopped')
                self.assertFalse(result.success)
                self.assertEqual(await asyncio.wait_for(pending, 2), 'closed')
            finally:
                control.close()
                await asyncio.gather(pending, return_exceptions=True)

    async def test_a_stop_during_launch_records_a_single_worker_result(self):
        from coordinator.host import HostClient
        original = HostClient.launch
        entered = asyncio.Event()
        gate = asyncio.Event()

        async def gated(directory, spec, **options):
            entered.set()
            await gate.wait()
            return await original(directory, spec, **options)

        with patch.object(HostClient, 'launch', gated):
            service, topic, (worker, other) = self.service_workers(2)
            run = asyncio.create_task(service.workers.run(service.workers.get(worker), self.store.topic(topic)))
            await asyncio.wait_for(entered.wait(), 10)
            stop = self.request('workers.stop', worker)
            steer = self.request('workers.steer', other, prompt='note for the other job')
            try:
                await asyncio.wait_for(service.controls_once(), 2)
                self.assertEqual(self.request_state(steer), 'done')
                self.assertIn(worker, service.workers.stopping)
                gate.set()
                done = await settle(run)
                await until(lambda: self.request_state(stop) == 'done')
                self.assertEqual(json.loads(done['result'])['failure_code'], 'owner_stopped')
                self.assertFalse((self.root / 'turn-started').exists())
                self.assertEqual(self.store.db.execute(
                    "SELECT COUNT(*) FROM messages WHERE source_worker=? AND kind='worker_result'",
                    (worker,)).fetchone()[0], 1)
            finally:
                gate.set()
                await service.shutdown([run])

    async def test_a_stop_during_launch_never_needs_a_second_stop(self):
        from coordinator.host import HostClient
        original = HostClient.launch
        entered = asyncio.Event()
        gate = asyncio.Event()

        async def gated(directory, spec, **options):
            entered.set()
            await gate.wait()
            return await original(directory, spec, **options)

        with patch.dict(os.environ, {'FAKE_HOLD_TURN': '1'}), patch.object(HostClient, 'launch', gated):
            service, topic, (worker, other) = self.service_workers(2)
            run = asyncio.create_task(service.workers.run(service.workers.get(worker), self.store.topic(topic)))
            await asyncio.wait_for(entered.wait(), 10)
            stop = self.request('workers.stop', worker)
            steer = self.request('workers.steer', other, prompt='note for the other job')
            try:
                await asyncio.wait_for(service.controls_once(), 2)
                self.assertEqual(self.request_state(stop), 'sending')
                self.assertEqual(self.request_state(steer), 'done')
                self.assertIn(worker, service.workers.stopping)
                self.assertFalse(run.done())
                gate.set()
                done = await settle(run)
                await until(lambda: self.request_state(stop) == 'done')
                self.assertEqual(json.loads(done['result'])['failure_code'], 'owner_stopped')
                self.assertFalse((self.root / 'turn-started').exists())
            finally:
                gate.set()
                await service.shutdown([run])

    async def test_shutdown_cancels_a_waiting_stop_without_a_false_receipt(self):
        from coordinator.host import HostClient
        entered = asyncio.Event()
        gate = asyncio.Event()

        async def gated(directory, spec, **options):
            entered.set()
            await gate.wait()

        with patch.object(HostClient, 'launch', gated):
            service, topic, (worker,) = self.service_workers()
            run = asyncio.create_task(service.workers.run(service.workers.get(worker), self.store.topic(topic)))
            await asyncio.wait_for(entered.wait(), 10)
            stop = self.request('workers.stop', worker)
            try:
                await asyncio.wait_for(service.controls_once(), 2)
                pending = list(service.stop_tasks.values())
                self.assertEqual(len(pending), 1)
                self.assertEqual(self.request_state(stop), 'sending')
                await asyncio.wait_for(service.shutdown([run]), 5)
                self.assertTrue(all(task.cancelled() for task in pending))
                self.assertEqual(self.request_state(stop), 'sending')
                self.store.requests_uncertain()
                self.assertEqual(self.request_state(stop), 'uncertain')
            finally:
                await service.shutdown([run])

    async def test_a_steer_after_reattachment_does_not_hold_the_stop(self):
        with patch.dict(os.environ, {'FAKE_HOLD_TURN': '1', 'FAKE_WAIT_FINISH': '1'}):
            service, run, worker = await self.reattach_with_pending_goal()
            self.request('workers.steer', worker, prompt='steer after restart')
            stop = self.request('workers.stop', worker)
            try:
                await asyncio.wait_for(service.controls_once(), 5)
                self.assertEqual(self.request_state(stop), 'done')
                done = await settle(run)
                self.assertEqual(json.loads(done['result'])['failure_code'], 'owner_stopped')
                self.assertFalse((self.root / 'finish-turn').exists())
            finally:
                await service.shutdown([run])

    async def test_a_stop_between_codex_hosts_prevents_the_continuation(self):
        original = ProviderRunner._run
        entered = asyncio.Event()
        gate = asyncio.Event()

        async def gated(runner, provider, prompt, cwd, session_id, **options):
            if options.get('goal_only'):
                entered.set()
                await gate.wait()
            return await original(runner, provider, prompt, cwd, session_id, **options)

        with patch.dict(os.environ, {'FAKE_HOLD_TURN': '1', 'FAKE_WAIT_FINISH': '1'}), \
                patch.object(ProviderRunner, '_run', gated):
            service, run, worker = await self.reattach_with_pending_goal()
            (self.root / 'finish-turn').touch()
            await asyncio.wait_for(entered.wait(), 10)
            old = service.workers.get(worker)['host']
            self.assertTrue((self.root / 'state/hosts' / old / 'exit.json').exists())
            stop = self.request('workers.stop', worker)
            queue = asyncio.create_task(service.controls_once())
            try:
                await until(lambda: worker in service.workers.stopping)
                gate.set()
                await asyncio.wait_for(queue, 5)
                done = await settle(run)
                self.assertEqual(self.request_state(stop), 'done')
                self.assertEqual(done['status'], 'interrupted')
                self.assertEqual(json.loads(done['result'])['failure_code'], 'owner_stopped')
                self.assertFalse((self.root / 'continued').exists())
                self.assertEqual(len((self.root / 'launches.jsonl').read_text().splitlines()), 1)
                results = self.store.db.execute(
                    "SELECT COUNT(*) FROM messages WHERE source_worker=? AND kind='worker_result'", (worker,)).fetchone()[0]
                self.assertEqual(results, 1)
                await service.controls_once()
                self.assertEqual(self.request_state(stop), 'done')
            finally:
                gate.set()
                await service.shutdown([queue, run])

    async def test_an_activated_goal_receipt_does_not_set_the_goal_twice(self):
        with patch.dict(os.environ, {'FAKE_HOLD_TURN': '1', 'FAKE_WAIT_FINISH': '1'}):
            control = RunControl()
            run = asyncio.create_task(self.run_worker(control))
            await until_exists(self.root / 'turn-started')
            await until(lambda: control._ready.is_set())
            goal = asyncio.create_task(control.goal('late', 'late goal'))
            await until(lambda: not control._ready.is_set())
            (self.root / 'finish-turn').touch()
            self.assertEqual(await settle(goal), 'received')
            self.assertEqual(await settle(control.goal('explicit-update', 'late goal')), 'received')
            self.assertEqual(await settle(control.goal('clear', 'clear')), 'received')
            self.assertTrue((await settle(run)).success)
        requests = [json.loads(json.loads(line)['line'])
                    for path in (self.root / 'state/hosts').glob('*/writes.jsonl')
                    for line in path.read_text().splitlines()]
        self.assertEqual(sum(r.get('method') == 'thread/goal/set' for r in requests), 2)

    async def test_controls_for_one_worker_keep_their_receipt_order(self):
        with patch.dict(os.environ, {'FAKE_HOLD_TURN': '1', 'FAKE_WAIT_FINISH': '1'}):
            service, topic, (worker,) = self.service_workers()
            run = asyncio.create_task(service.workers.run(service.workers.get(worker), self.store.topic(topic)))
            await until_exists(self.root / 'turn-started')
            await until(lambda: service.workers.controls[worker]._ready.is_set())
            goal = self.request('workers.goal', worker, condition='late goal')
            first = self.request('workers.steer', worker, prompt='first note')
            second = self.request('workers.steer', worker, prompt='second note')
            try:
                await asyncio.wait_for(service.controls_once(), 5)
                await until(lambda: not service.workers.controls[worker]._ready.is_set())
                self.assertEqual(self.request_state(goal), 'sending')
                self.assertEqual(self.request_state(first), 'queued')
                self.assertEqual(self.request_state(second), 'queued')
                (self.root / 'finish-turn').touch()
                await until(lambda: self.request_state(goal) == 'done')
                await service.controls_once()
                await until(lambda: self.request_state(first) == 'done')
                self.assertEqual(self.request_state(second), 'queued')
                await service.controls_once()
                await until(lambda: self.request_state(second) == 'done')
                self.assertEqual(await settle(service.workers.controls[worker].goal('clear', 'clear')), 'received')
                self.assertTrue(json.loads((await settle(run))['result'])['success'])
                requests = [json.loads(json.loads(line)['line'])
                            for path in (self.root / 'state/hosts').glob('*/writes.jsonl')
                            for line in path.read_text().splitlines()]
                self.assertEqual([r['params']['input'][0]['text'] for r in requests
                                  if r.get('method') == 'turn/steer'], ['first note', 'second note'])
            finally:
                await service.shutdown([run])

    async def test_a_stop_during_continuation_launch_stops_the_new_host(self):
        from coordinator.host import HostClient
        original = HostClient.launch
        entered = asyncio.Event()
        gate = asyncio.Event()

        async def gated(directory, spec, **options):
            if spec.get('goal_only'):
                entered.set()
                await gate.wait()
            return await original(directory, spec, **options)

        with patch.dict(os.environ, {'FAKE_HOLD_TURN': '1', 'FAKE_WAIT_FINISH': '1'}), \
                patch.object(HostClient, 'launch', gated):
            service, run, worker = await self.reattach_with_pending_goal()
            (self.root / 'finish-turn').touch()
            await asyncio.wait_for(entered.wait(), 10)
            stop = self.request('workers.stop', worker)
            queue = asyncio.create_task(service.controls_once())
            try:
                await until(lambda: worker in service.workers.stopping)
                gate.set()
                await asyncio.wait_for(queue, 5)
                done = await settle(run)
                self.assertEqual(self.request_state(stop), 'done')
                self.assertEqual(json.loads(done['result'])['failure_code'], 'owner_stopped')
                self.assertFalse((self.root / 'continued').exists())
                directory = self.root / 'state/hosts' / done['host']
                self.assertTrue((directory / 'exit.json').exists())
                self.assertEqual((directory / 'writes.jsonl').read_text(), '')
            finally:
                gate.set()
                await service.shutdown([queue, run])

    async def test_terminal_error_details_are_scrubbed_before_leaving_the_runner(self):
        self.binary.write_text(self.binary.read_text().replace(
            "notify('turn/completed', turn={'id': turn, 'status': 'completed'})",
            "notify('turn/completed', turn={'id': turn, 'status': 'failed', "
            "'error': {'message': value, 'additionalDetails': 'request ' + value, "
            "'codexErrorInfo': 'other'}})"))
        value = 'FAKE-TERMINAL-ERROR-SECRET'
        result = await self.run_worker(secrets={'FAKE_SECRET': value})
        self.assertFalse(result.success)
        detail = json.loads(result.failure_detail)
        self.assertNotIn(value, result.failure_detail)
        self.assertEqual(detail['codexErrorInfo'], 'other')
        self.assertEqual(detail['message'], '[secret FAKE_SECRET]')
        self.assertEqual(detail['additionalDetails'], 'request [secret FAKE_SECRET]')

    async def test_private_values_reach_shell_and_every_torii_capture_is_scrubbed_on_launch_and_resume(self):
        value = 'FAKE"quoted\\value-0123456789'
        for fresh in (True, False):
            with self.subTest(fresh=fresh), patch.dict(os.environ, {'FAKE_STRUCTURED_FEATURE': '1'}), \
                    patch.object(self.runner, '_external_writer', AsyncMock(return_value=False)):
                result = await self.run_worker(fresh=fresh, secrets={'FAKE_SECRET': value, 'NPM_TOKEN': 'FAKE-NPM-0123456789'})
                self.assertTrue(result.success, result.failure_detail or result.error)
                self.assertEqual(json.loads((self.root / 'shell-seen').read_text()),
                                 {'FAKE_SECRET': len(value), 'NPM_TOKEN': 19})
                self.assertEqual(result.text, 'used [secret FAKE_SECRET]')
                directory = Path(result.log_path).parent
                spec = json.loads((directory / 'spec.json').read_text())
                self.assertNotIn('FAKE_SECRET', spec['env'])
                self.assertNotIn('NPM_TOKEN', spec['env'])
                arguments = spec['argv']
                for override in ('features.shell_snapshot=false', 'features.shell_snapshot_v2=false',
                                 'features.multi_agent=false', 'features.multi_agent_v2=false',
                                 'features.memories=false',
                                 'memories.dedicated_tools=false',
                                 'memories.generate_memories=false', 'otel.tool_result.max_bytes=0',
                                 'otel.log_agent_responses=false', 'otel.log_guardian_assessments=false'):
                    self.assertIn(override, arguments)
                for path in directory.iterdir():
                    if path.is_file():
                        text = path.read_text()
                        for form in (value, json.dumps(value)[1:-1], json.dumps(json.dumps(value)[1:-1])[1:-1]):
                            self.assertNotIn(form, text, path.name)
                clean = await self.run_worker()
                self.assertTrue(clean.success, clean.error)
                self.assertEqual(json.loads((self.root / 'shell-seen').read_text()), {})
                self.assertNotIn('FAKE_SECRET', os.environ)

    async def test_runtime_and_stripped_names_are_refused_before_host_launch(self):
        for name in ('NODE_REPL_AUTH_TOKEN', 'CODEX_EXEC_SERVER_NOISE_AUTH_TOKEN', 'OPENAI_IDENTITY_TOKEN_FILE',
                     'OPENAI_FEDERATION_RULE_ID', 'OPENAI_WORKLOAD_IDENTITY_CONTEXT', 'RUST_LOG', 'HOME', 'BASH_ENV'):
            with self.subTest(name=name), patch('coordinator.providers.HostClient.launch', AsyncMock()) as launch:
                result = await self.run_worker(secrets={name: 'FAKE-0123456789-value'})
                self.assertFalse(result.success)
                self.assertIn(name, result.failure_detail)
                self.assertIn('stripped or control the runtime', result.failure_detail)
                launch.assert_not_awaited()

    async def test_resume_with_names_only_keeps_overrides_and_checks_without_values(self):
        with patch.object(self.runner, '_external_writer', AsyncMock(return_value=False)):
            result = await self.run_worker(fresh=False, secret_names=['NPM_TOKEN'])
        self.assertTrue(result.success, result.failure_detail or result.error)
        self.assertEqual(json.loads((self.root / 'shell-seen').read_text()), {})
        directory = Path(result.log_path).parent
        spec = json.loads((directory / 'spec.json').read_text())
        self.assertEqual(spec['secret_names'], ['NPM_TOKEN'])
        for override in CODEX_SECRET_OVERRIDES:
            self.assertIn(override, spec['argv'])
        requests = [json.loads(json.loads(line)['line']) for line in
                    (directory / 'writes.jsonl').read_text().splitlines()]
        methods = [request.get('method') for request in requests]
        resume = methods.index('thread/resume')
        self.assertEqual(methods.count('config/read'), 2)
        self.assertIn('config/read', methods[:resume])
        self.assertIn('config/read', methods[resume + 1:])

    async def test_effective_configuration_that_replaces_values_or_enables_memory_is_refused(self):
        for flag, reason in (('FAKE_CONFIG_REPLACE', 'replaces declared secrets: FAKE_SECRET'),
                             ('FAKE_CONFIG_UNSAFE', 'prevents safe secret delivery')):
            with self.subTest(flag=flag), patch.dict(os.environ, {flag: '1'}):
                result = await self.run_worker(secrets={'FAKE_SECRET': 'FAKE-0123456789-value'})
                self.assertFalse(result.success)
                self.assertIn(reason, result.failure_detail)
                requests = (Path(result.log_path).parent / 'writes.jsonl').read_text()
                self.assertNotIn('thread/start', requests)
                self.assertNotIn('job prompt', requests)

    async def test_reattachment_refuses_a_host_without_the_secret_controls(self):
        directory = self.root / 'state' / 'hosts' / 'worker-unsafe'
        directory.mkdir(parents=True)
        (directory / 'spec.json').write_text(json.dumps({'argv': ['fake-codex', 'app-server'],
                                                        'secret_names': ['FAKE_SECRET']}))
        with patch('coordinator.providers.HostClient.attach') as attach:
            result = await self.run_worker(fresh=False, attach=True, host_id='worker-unsafe',
                                            secret_names=['FAKE_SECRET'])
        self.assertFalse(result.success)
        self.assertIn('without its safe launch controls', result.failure_detail)
        attach.assert_not_called()

    async def test_native_goal_stages_before_prompt_continues_changes_with_ack_and_clears(self):
        control = RunControl()
        run = asyncio.create_task(self.run_worker(control, goal='initial goal'))
        await until_exists(self.root / 'continued')
        self.assertFalse(run.done())
        change = asyncio.create_task(control.goal('change', 'changed goal'))
        await until_exists(self.root / 'change-sent')
        self.assertFalse(change.done())
        (self.root / 'ack-change').touch()
        self.assertEqual(await settle(change), 'received')
        self.assertEqual(await settle(control.goal('clear', 'clear')), 'received')
        result = await settle(run)
        self.assertTrue(result.success, (result.failure_detail or result.error or '') + '\n' + Path(result.log_path).read_text())
        self.assertIsNone(result.goal_status)
        writes = [json.loads(json.loads(line)['line']) for line in
                  (Path(result.log_path).parent / 'writes.jsonl').read_text().splitlines()]
        methods = [request.get('method') for request in writes]
        paused = next(index for index, request in enumerate(writes)
                      if (request.get('params') or {}).get('status') == 'paused')
        active = next(index for index, request in enumerate(writes)
                      if (request.get('params') or {}).get('status') == 'active')
        self.assertLess(paused, methods.index('turn/start'))
        self.assertLess(methods.index('turn/start'), active)
        self.assertIn('thread/goal/get', methods)
        self.assertIn('thread/goal/clear', methods)
        self.assertNotIn('/goal ', json.dumps(writes))

    async def test_goal_state_request_failure_is_not_reported_as_success(self):
        with patch.dict(os.environ, {'FAKE_GOAL_GET_ERROR': '1'}):
            result = await settle(self.run_worker(goal='initial goal'))
        self.assertFalse(result.success)
        self.assertEqual(result.failure_code, 'goal_failed')

    async def test_goal_continuations_keep_the_last_turn_result_and_success(self):
        for first_event in ('started', 'item-started', 'item-completed'):
            for overlap in ('', '1'):
                for status in ('completed', 'failed'):
                    with self.subTest(first_event=first_event, overlap=overlap, status=status), patch.dict(
                            os.environ, {'FAKE_GOAL_CONTINUATION': first_event, 'FAKE_GOAL_OVERLAP': overlap,
                                         'FAKE_GOAL_FINAL_STATUS': status}):
                        result = await settle(self.run_worker(goal='initial goal'))
                        self.assertEqual(result.text, 'final goal result', result.error)
                        self.assertEqual(result.success, status == 'completed', result.error)
                        self.assertEqual(result.goal_status['status'], 'complete')
                        self.assertEqual(result.session_id, self.sid)

    async def test_worker_records_the_goal_continuations_final_turn(self):
        service, topic, workers = self.service_workers(count=2)
        for worker, status in zip(workers, ('completed', 'failed')):
            with self.subTest(status=status), patch.dict(os.environ, {
                    'FAKE_GOAL_CONTINUATION': 'item-completed', 'FAKE_GOAL_FINAL_STATUS': status}):
                self.store.db.execute('UPDATE workers SET goal=? WHERE id=?', ('initial goal', worker))
                saved = await settle(service.workers.run(service.workers.get(worker), self.store.topic(topic)))
                result = json.loads(saved['result'])
                self.assertEqual(saved['status'], 'done')
                self.assertEqual(result['text'], 'final goal result', result['error'])
                self.assertEqual(result['success'], status == 'completed', result['error'])
                self.assertEqual(saved['session'], self.sid)

    async def test_a_new_process_uses_the_latest_owner_goal(self):
        control = RunControl()
        run = asyncio.create_task(self.run_worker(control, goal='stale goal', current_goal=lambda: 'current goal'))
        await until_exists(self.root / 'continued')
        self.assertEqual(await settle(control.goal('clear', 'clear')), 'received')
        result = await settle(run)
        self.assertTrue(result.success, result.failure_detail or result.error)
        spec = json.loads((Path(result.log_path).parent / 'spec.json').read_text())
        self.assertEqual(spec['initial_goal'], 'current goal')
        self.assertNotIn('stale goal', (Path(result.log_path).parent / 'writes.jsonl').read_text())

    async def test_inactive_native_goals_end_at_the_turn_boundary(self):
        for status in ('complete', 'paused', 'blocked', 'usageLimited', 'budgetLimited'):
            with self.subTest(status=status), patch.dict(os.environ, {'FAKE_GOAL_STATUS': status}):
                result = await settle(self.run_worker(goal='initial goal'))
                self.assertTrue(result.success, result.failure_detail or result.error)
                self.assertEqual(result.goal_status['status'], status)

    async def test_a_prompt_that_finishes_before_goal_activation_keeps_its_worker_open(self):
        with patch.dict(os.environ, {'FAKE_PROMPT_FAST': '1'}):
            control = RunControl()
            run = asyncio.create_task(self.run_worker(control, goal='initial goal'))
            await until_exists(self.root / 'continued')
            self.assertFalse(run.done())
            self.assertEqual(await settle(control.goal('clear', 'clear')), 'received')
            result = await settle(run)
        self.assertTrue(result.success, result.failure_detail or result.error)

    async def test_a_goal_can_be_set_on_a_worker_that_started_without_one(self):
        with patch.dict(os.environ, {'FAKE_HOLD_TURN': '1', 'FAKE_WAIT_FINISH': '1'}):
            control = RunControl()
            run = asyncio.create_task(self.run_worker(control))
            await until_exists(self.root / 'turn-started')
            goal = asyncio.create_task(control.goal('set', 'new goal'))
            await asyncio.sleep(.1)
            (self.root / 'finish-turn').touch()
            self.assertEqual(await settle(goal), 'received')
            await until_exists(self.root / 'continued')
            self.assertFalse(run.done())
            self.assertEqual(await settle(control.goal('clear', 'clear')), 'received')
            result = await settle(run)
        self.assertTrue(result.success, result.failure_detail or result.error)
        launches = [json.loads(line) for line in (self.root / 'launches.jsonl').read_text().splitlines()]
        self.assertEqual(len(launches), 2)
        self.assertNotIn('features.goals=true', launches[0]['argv'])
        self.assertIn('features.goals=true', launches[1]['argv'])
        self.assertEqual(result.session_id, self.sid)

    async def test_a_secret_goal_worker_reattaches_without_another_process_or_reverting_its_goal(self):
        control = RunControl()
        sequence = []
        run = asyncio.create_task(self.run_worker(control, goal='initial goal', host_id='worker-recover',
            secrets={'FAKE_SECRET': 'FAKE-0123456789-value'}, on_seq=sequence.append))
        await until_exists(self.root / 'continued')
        (self.root / 'ack-change').touch()
        self.assertEqual(await settle(control.goal('change', 'changed goal')), 'received')
        run.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await run
        control = RunControl()
        with patch('coordinator.providers.HostClient.launch', AsyncMock(side_effect=AssertionError('second process'))):
            run = asyncio.create_task(self.run_worker(control, fresh=False, attach=True,
                                                      host_id='worker-recover', last_seq=sequence[-1]))
            receipt = await settle(control.goal('clear-after-restart', 'clear'))
            if receipt != 'received':
                result = await settle(run)
                self.fail(result.failure_detail or result.error or receipt)
            result = await settle(run)
        self.assertTrue(result.success, result.failure_detail or result.error)
        self.assertEqual(result.session_id, self.sid)
        self.assertEqual(result.text, 'used [secret FAKE_SECRET]')
        requests = [json.loads(json.loads(line)['line']) for line in
                    (Path(result.log_path).parent / 'writes.jsonl').read_text().splitlines()]
        self.assertEqual(sum(request.get('method') == 'thread/start' for request in requests), 1)
        self.assertEqual(sum((request.get('params') or {}).get('objective') == 'initial goal'
                             for request in requests), 1)

    async def test_launch_commands_cover_goal_modes(self):
        for fresh in (True, False):
            for secret in (False, True):
                for objective in (None, 'spawn goal'):
                    with self.subTest(fresh=fresh, secret=secret, goal=objective), \
                            patch.dict(os.environ, {'FAKE_GOAL_STATUS': 'complete'}), \
                            patch.object(self.runner, '_external_writer', AsyncMock(return_value=False)):
                        result = await settle(self.run_worker(fresh=fresh, goal=objective,
                            secrets={'FAKE_SECRET': 'fixture'} if secret else None))
                        self.assertTrue(result.success, result.failure_detail or result.error)
                        self.assertEqual(result.session_id, self.sid)
                        spec = json.loads((Path(result.log_path).parent / 'spec.json').read_text())
                        expected = [str(self.binary), '-c', 'model_reasoning_effort=medium']
                        if objective:
                            expected += ['-c', 'features.goals=true']
                        if secret:
                            for override in CODEX_SECRET_OVERRIDES:
                                expected += ['-c', override]
                        self.assertEqual(spec['argv'], expected + ['app-server', '--listen', 'stdio://'])
        self.assertEqual(len((self.root / 'launches.jsonl').read_text().splitlines()), 8)

    async def test_goal_tools_follow_the_launch_goal(self):
        for fresh, objective, current in ((True, None, None), (False, None, None),
                                          (True, 'spawn goal', None), (False, None, 'current goal'),
                                          (True, 'stale goal', 'queued goal')):
            with self.subTest(fresh=fresh, goal=objective, current=current), \
                    patch.dict(os.environ, {'FAKE_GOAL_STATUS': 'complete'}), \
                    patch.object(self.runner, '_external_writer', AsyncMock(return_value=False)):
                result = await settle(self.run_worker(fresh=fresh, goal=objective,
                    current_goal=(lambda: current) if current else None))
                self.assertTrue(result.success, result.failure_detail or result.error)
                spec = json.loads((Path(result.log_path).parent / 'spec.json').read_text())
                self.assertEqual('features.goals=true' in spec['argv'], bool(current or objective))

    async def test_a_mid_turn_goal_waits_for_exit_and_a_concurrent_steer_arrives_once(self):
        with patch.dict(os.environ, {'FAKE_HOLD_TURN': '1', 'FAKE_WAIT_FINISH': '1',
                                     'FAKE_BLOCK_RESUMED_GOAL': '1', 'FAKE_DELAY_GOAL_TURN': '1'}):
            control = RunControl()
            run = asyncio.create_task(self.run_worker(control))
            await until_exists(self.root / 'turn-started')
            goal = asyncio.create_task(control.goal('late-goal', 'mid-turn goal'))
            await asyncio.sleep(.1)
            self.assertFalse(goal.done(), 'A mid-turn goal cannot be received before the feature is available.')
            (self.root / 'finish-turn').touch()
            await until_exists(self.root / 'change-sent')
            steer = asyncio.create_task(control.steer('during-goal', 'keep this instruction'))
            await asyncio.sleep(.1)
            self.assertFalse(goal.done())
            self.assertFalse(steer.done())
            (self.root / 'ack-change').touch()
            self.assertEqual(await settle(goal), 'received')
            self.assertEqual(await settle(steer), 'received')
            self.assertEqual(await settle(control.steer('during-goal', 'do not repeat')), 'received')
            self.assertEqual(await settle(control.goal('clear', 'clear')), 'received')
            result = await settle(run)
        self.assertTrue(result.success, result.failure_detail or result.error)
        self.assertEqual(result.session_id, self.sid)
        launches = [json.loads(line) for line in (self.root / 'launches.jsonl').read_text().splitlines()]
        self.assertEqual(len(launches), 2)
        self.assertNotIn('features.goals=true', launches[0]['argv'])
        self.assertIn('features.goals=true', launches[1]['argv'])
        requests = [json.loads(json.loads(line)['line'])
                    for path in (self.root / 'state/hosts').glob('*/writes.jsonl')
                    for line in path.read_text().splitlines()]
        self.assertEqual(sum(r.get('method') == 'thread/start' for r in requests), 1)
        resumes = [r for r in requests if r.get('method') == 'thread/resume']
        self.assertEqual(len(resumes), 1)
        self.assertEqual(resumes[0]['params']['threadId'], self.sid)
        self.assertEqual(sum(r.get('method') == 'turn/start' for r in requests), 1)
        steers = [r for r in requests if r.get('method') == 'turn/steer']
        self.assertEqual(len(steers), 1)
        self.assertEqual(steers[0]['params']['input'][0]['text'], 'keep this instruction')

    async def test_a_saved_late_goal_recovers_a_secret_worker_on_the_same_thread(self):
        from coordinator import control_api
        from coordinator.accounts import AccountBroker
        from tests.support import FakeVault
        from coordinator.workers import WorkerPool
        topic = '-10042:4'
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?, -10042, 4, 'Torii', ?, 1)",
                              (topic, str(self.root)))
        task = self.store.task_create(topic, 'Late secret goal', worktree=str(self.root))
        self.store.task_update(task['id'], secrets=['FAKE_SECRET'])
        vault = FakeVault()
        value = 'FAKE-reloaded-value-0123456789'
        vault.values['FAKE_SECRET'] = value
        self.store.db.execute("""INSERT INTO envelopes(name,reason,consumer,task,topic,state,created,expires,
            length,fingerprint,updated) VALUES ('FAKE_SECRET','r','c',?,?,'filled',1,601,?,'abcd1234',1)""",
                              (task['id'], topic, len(value)))
        spawned = control_api.call(self.store, 'workers.spawn', {
            'task': task['id'], 'provider': 'codex', 'prompt': 'job prompt'})
        self.assertTrue(spawned.ok, spawned.text)
        pool = WorkerPool(self.store, self.runner, AccountBroker(self.store), vault)
        worker = pool.get(spawned.data['worker'])
        with patch.dict(os.environ, {'FAKE_HOLD_TURN': '1', 'FAKE_WAIT_FINISH': '1'}):
            run = asyncio.create_task(pool.run(worker, self.store.topic(topic)))
            await until_exists(self.root / 'turn-started')
            run.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await run
            changed = control_api.call(self.store, 'workers.goal', {'worker': worker['id'], 'condition': 'recovered goal'})
            self.assertTrue(changed.ok, changed.text)
            run = asyncio.create_task(pool.run(pool.get(worker['id']), self.store.topic(topic)))
            (self.root / 'finish-turn').touch()
            await until_exists(self.root / 'continued')
            control = pool.controls[worker['id']]
            self.assertEqual(await settle(control.steer('recovered-steer', 'continue safely')), 'received')
            self.assertEqual(await settle(control.goal('clear', 'clear')), 'received')
            done = await settle(run)
        result = json.loads(done['result'])
        self.assertTrue(result['success'], result.get('error'))
        self.assertEqual(result['session_id'], self.sid)
        self.assertEqual(json.loads((self.root / 'shell-seen').read_text()), {'FAKE_SECRET': len(value)})
        self.assertEqual(len((self.root / 'launches.jsonl').read_text().splitlines()), 2)
        self.assertEqual(result['text'], 'used [secret FAKE_SECRET]')

    async def test_a_rejected_late_goal_is_never_reported_as_received(self):
        with patch.dict(os.environ, {'FAKE_HOLD_TURN': '1', 'FAKE_WAIT_FINISH': '1', 'FAKE_GOAL_SET_ERROR': '1'}):
            control = RunControl()
            run = asyncio.create_task(self.run_worker(control))
            await until_exists(self.root / 'turn-started')
            goal = asyncio.create_task(control.goal('rejected', 'unavailable goal'))
            await asyncio.sleep(.1)
            (self.root / 'finish-turn').touch()
            self.assertNotEqual(await settle(goal), 'received')
            result = await settle(run)
        self.assertFalse(result.success)
        self.assertEqual(result.session_id, self.sid)

    async def test_a_late_goal_host_reattaches_with_its_current_goal(self):
        hosts = []
        with patch.dict(os.environ, {'FAKE_HOLD_TURN': '1', 'FAKE_WAIT_FINISH': '1'}):
            control = RunControl()
            run = asyncio.create_task(self.run_worker(control, host_id='worker-late', on_host=hosts.append))
            await until_exists(self.root / 'turn-started')
            goal = asyncio.create_task(control.goal('late', 'late goal'))
            await asyncio.sleep(.1)
            (self.root / 'finish-turn').touch()
            self.assertEqual(await settle(goal), 'received')
            await until_exists(self.root / 'continued')
            (self.root / 'ack-change').touch()
            self.assertEqual(await settle(control.goal('change', 'changed goal')), 'received')
            run.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await run
            control = RunControl()
            with patch('coordinator.providers.HostClient.launch', AsyncMock(side_effect=AssertionError('third process'))):
                run = asyncio.create_task(self.run_worker(control, fresh=False, attach=True, host_id=hosts[-1]))
                self.assertEqual(await settle(control.steer('after-restart', 'continue safely')), 'received')
                self.assertEqual(await settle(control.goal('clear', 'clear')), 'received')
                result = await settle(run)
        self.assertTrue(result.success, result.failure_detail or result.error)
        self.assertEqual(result.session_id, self.sid)
        self.assertEqual(len(hosts), 2)
        self.assertEqual(len((self.root / 'launches.jsonl').read_text().splitlines()), 2)

    async def test_clearing_a_goal_less_worker_preserves_the_finished_turn_without_another_prompt(self):
        with patch.dict(os.environ, {'FAKE_HOLD_TURN': '1', 'FAKE_WAIT_FINISH': '1'}):
            control = RunControl()
            run = asyncio.create_task(self.run_worker(control))
            await until_exists(self.root / 'turn-started')
            clear = asyncio.create_task(control.goal('clear', 'clear'))
            await asyncio.sleep(.1)
            (self.root / 'finish-turn').touch()
            self.assertEqual(await settle(clear), 'received')
            result = await settle(run)
        self.assertTrue(result.success, result.failure_detail or result.error)
        self.assertEqual(result.text, 'used no-secret')
        requests = [json.loads(json.loads(line)['line'])
                    for path in (self.root / 'state/hosts').glob('*/writes.jsonl')
                    for line in path.read_text().splitlines()]
        self.assertEqual(sum(r.get('method') == 'turn/start' for r in requests), 1)
        self.assertFalse(any(r.get('method') == 'thread/goal/set' for r in requests))
