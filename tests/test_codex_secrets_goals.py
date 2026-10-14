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
from tests.support import isolate_shared_mcp_home, settle, stop_test_hosts, until_exists


SERVER = '''#!/usr/bin/env python3
import json, os, subprocess, sys, time
from pathlib import Path
sid = SID
root = Path(ROOT)
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
for line in sys.stdin:
    request = json.loads(line)
    method = request.get('method')
    params = request.get('params') or {}
    print(json.dumps(request), file=sys.stderr, flush=True)
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
        if goal is None and not os.environ.get('FAKE_HOLD_TURN') or os.environ.get('FAKE_PROMPT_FAST'):
            if os.environ.get('FAKE_SECRET'):
                print('diagnostic ' + os.environ['FAKE_SECRET'], file=sys.stderr, flush=True)
            completed()
    elif method == 'thread/goal/set':
        goal = dict(goal or {}, **params)
        goal.setdefault('objective', 'missing')
        goal.setdefault('status', 'active')
        if goal['status'] == 'active':
            assert prompt_started
            if params.get('objective') == 'changed goal':
                (root / 'change-sent').touch()
                while not (root / 'ack-change').exists():
                    time.sleep(.01)
        answer(request, {'goal': goal})
        notify('thread/goal/updated', goal=goal)
        if goal['status'] == 'active' and turn == 'turn-1':
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

    async def run_worker(self, control=None, **options):
        fresh = options.pop('fresh', True)
        return await self.runner.run('codex', 'job prompt', self.root, None if fresh else self.sid,
                                     fresh=fresh, control=control or RunControl(), **options)

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
        with patch.dict(os.environ, {'FAKE_HOLD_TURN': '1'}):
            control = RunControl()
            run = asyncio.create_task(self.run_worker(control))
            self.assertEqual(await settle(control.goal('set', 'new goal')), 'received')
            await until_exists(self.root / 'continued')
            self.assertFalse(run.done())
            self.assertEqual(await settle(control.goal('clear', 'clear')), 'received')
            result = await settle(run)
        self.assertTrue(result.success, result.failure_detail or result.error)

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
