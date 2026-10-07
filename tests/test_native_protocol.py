import asyncio
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch
import uuid

from coordinator.native_protocol import NativeProtocol, RunControl
from coordinator.accounts import AccountBroker
from coordinator.providers import ProviderRunner
from coordinator.store import Store
from tests.support import SCALE, isolate_shared_mcp_home, settle, until_exists, stop_test_hosts


class NativeProtocolTestsSupport:
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)
        isolate_shared_mcp_home(self, self.root)
        self.addCleanup(stop_test_hosts, self.root)
        self.binary = self.root / 'native'
        self.sid = str(uuid.uuid4())
        self.runner = ProviderRunner(self.root / 'state', binaries={
            'claude': str(self.binary), 'codex': str(self.binary)})
        self.store = Store(self.root / 'state')
        profile = self.root / 'profile'
        profile.mkdir()
        with self.store.db:
            self.store.put('accounts', {'native-test': {'config_dir': str(profile), 'enabled': True}})
            self.store.put('account_status', {'native-test': {'identity': {
                'email': 'native-test@example.com', 'logged_in': True}, 'observed_at': time.time(),
                'usage': {'seven_day': {'utilization': 10, 'resets_at': time.time() + 3600}}}})
        self.runner.account_broker = AccountBroker(self.store)

    def tearDown(self):
        self.store.close()

    def fake(self, body):
        self.binary.write_text('#!/usr/bin/env python3\nimport sys,json,time,os\n'
            'def emit(v):\n print(json.dumps(v),flush=True)\n'
            'def read():\n return json.loads(sys.stdin.readline())\n'
            + 'sid=' + repr(self.sid) + '\n' + body)
        self.binary.chmod(0o700)

    async def run_native(self, provider, control, **options):
        if provider == 'claude':
            options.setdefault('account_alias', 'native-test')
        return await self.runner.run(provider, 'initial task', self.root, self.sid,
                                     fresh=False, control=control, **options)

    def claude(self, tail):
        self.fake("""
assert '--input-format' in sys.argv and '--replay-user-messages' in sys.argv
initial=read()
assert initial['message']['content']=='initial task'
emit({'type':'system','subtype':'init','session_id':sid})
steer=read()
assert steer['message']['content']=='new instruction'
echo={**steer,'isReplay':True}
result={'type':'result','subtype':'success','session_id':sid,'result':'done'}
""" + tail)

    def codex(self, tail):
        self.fake("""
assert sys.argv[1:] == ['-c','model_reasoning_effort=medium','-c','features.goals=true','app-server','--listen','stdio://']
r=read(); assert r['method']=='initialize'
emit({'id':r['id'],'result':{}})
assert read()['method']=='initialized'
r=read(); assert r['method']=='thread/resume' and r['params']['threadId']==sid
emit({'id':r['id'],'result':{'thread':{'id':sid}}})
r=read(); assert r['method']=='turn/start'
emit({'id':r['id'],'result':{'turn':{'id':'turn-1'}}})
r=read(); assert r['method']=='turn/steer'
assert r['params']['expectedTurnId']=='turn-1'
assert r['params']['input'][0]['text']=='new instruction'
terminal={'method':'turn/completed','params':{'threadId':sid,'turn':{'id':'turn-1','status':'completed'}}}
ack={'id':r['id'],'result':{'turnId':'turn-1'}}
""" + tail)

    async def exercise(self, provider, control=None):
        control = control or RunControl(receipt_timeout=.3 * SCALE)
        task = asyncio.create_task(self.run_native(provider, control))
        receipt = await settle(control.steer('message-1', 'new instruction'))
        result = await settle(task)
        return control, receipt, result


class NativeProtocolTests(NativeProtocolTestsSupport, unittest.IsolatedAsyncioTestCase):
    async def test_steer_reports_uncertain_when_the_host_socket_disappears(self):
        class Stdin:
            def write(self, data):
                pass

            async def drain(self):
                raise FileNotFoundError('host socket disappeared')

            async def wait_closed(self):
                pass

        class Process:
            stdin = Stdin()

        protocol = NativeProtocol('claude', Process(), RunControl(), self.sid, lambda sid: sid, persistent=True)
        self.assertEqual(await protocol.steer('message', 'follow-up'), 'uncertain')

    async def test_claudes_own_authentication_failure_is_an_auth_failure(self):
        self.fake("""
read()
emit({'type':'system','subtype':'init','session_id':sid})
emit({'type':'result','subtype':'success','session_id':sid,'is_error':True,
      'result':'Failed to authenticate. API Error: 401 Invalid authentication credentials'})
sys.stdin.read()
""")
        result = await settle(self.run_native('claude', RunControl()))
        self.assertFalse(result.success)
        self.assertEqual(result.failure_code, 'auth_failed')

    async def test_claude_mid_run_ack_and_stable_process(self):
        self.claude("emit(echo)\nemit(result)\nassert sys.stdin.read()==''\n")
        control, receipt, result = await self.exercise('claude')
        self.assertEqual(receipt, 'received')
        self.assertTrue(result.success, result.error)
        self.assertEqual(await control.steer('late', 'late'), 'closed')
        self.assertEqual(await control.steer('message-1', 'new instruction'), 'received')
        with self.assertRaises(ProcessLookupError):
            os.kill(result.pid, 0)

    async def test_initial_goal_is_steered_after_prompt_and_status_is_saved(self):
        self.fake("""
initial=read()
assert initial['message']['content']=='initial task'
emit({'type':'system','subtype':'init','session_id':sid})
goal=read()
assert goal['message']['content']=='/goal checks pass'
emit({**goal,'isReplay':True})
emit({'type':'goal_status','session_id':sid,'status':'met',
      'condition':'checks pass','reason':'verified','iterations':2})
emit({'type':'result','subtype':'success','session_id':sid,'result':'done'})
assert sys.stdin.read()==''
""")
        result = await settle(self.run_native('claude', RunControl(), initial_steer='/goal checks pass'))
        self.assertTrue(result.success, result.error)
        self.assertEqual(result.goal_status,
                         {'status': 'met', 'condition': 'checks pass', 'reason': 'verified', 'iterations': 2})

    async def test_claude_result_before_echo_requires_following_result(self):
        self.claude("emit(result)\nemit(echo)\nemit(result)\nassert sys.stdin.read()==''\n")
        _, receipt, result = await self.exercise('claude')
        self.assertEqual(receipt, 'received')
        self.assertTrue(result.success, result.error)

    async def test_claude_result_with_background_work_waits_for_the_following_result(self):
        self.fake("""
read()
emit({'type':'system','subtype':'init','session_id':sid})
emit({'type':'system','subtype':'background_tasks_changed','session_id':sid,
      'tasks':[{'task_id':'a1','task_type':'local_agent','description':'review'}]})
emit({'type':'result','subtype':'success','session_id':sid,'result':'waiting for the review'})
time.sleep(.2)
emit({'type':'system','subtype':'background_tasks_changed','session_id':sid,'tasks':[]})
emit({'type':'result','subtype':'success','session_id':sid,'result':'review finished'})
assert sys.stdin.read()==''
""")
        result = await settle(self.run_native('claude', RunControl()))
        self.assertTrue(result.success, result.error)
        self.assertEqual(result.text, 'review finished')

    async def test_claude_task_notification_quota_result_ends_with_background_work(self):
        self.fake("""
read()
emit({'type':'system','subtype':'init','session_id':sid})
emit({'type':'system','subtype':'background_tasks_changed','session_id':sid,
      'tasks':[{'task_id':'a1','task_type':'local_agent','description':'review'}]})
emit({'type':'rate_limit_event','session_id':sid,'rate_limit_info':
      {'status':'rejected','rateLimitType':'five_hour','resetsAt':9999999999}})
emit({'type':'assistant','session_id':sid,'error':'rate_limit','is_api_error_message':True,
      'message':{'stop_reason':'stop_sequence'}})
emit({'type':'result','subtype':'success','session_id':sid,'is_error':True,
      'terminal_reason':'api_error','stop_reason':'stop_sequence',
      'origin':{'kind':'task-notification'}})
assert sys.stdin.read()==''
""")
        result = await settle(self.run_native('claude', RunControl()), 3)
        self.assertFalse(result.success)
        self.assertTrue(result.quota_limited)
        self.assertEqual(result.rate_limit_info['rateLimitType'], 'five_hour')
        self.assertNotEqual(result.failure_code, 'pending_work')

    async def test_claude_task_notification_failure_ends_with_background_work(self):
        self.fake("""
read()
emit({'type':'system','subtype':'init','session_id':sid})
emit({'type':'system','subtype':'background_tasks_changed','session_id':sid,
      'tasks':[{'task_id':'a1','task_type':'local_agent','description':'review'}]})
emit({'type':'result','subtype':'error_during_execution','session_id':sid,'is_error':True,
      'terminal_reason':'api_error','origin':{'kind':'task-notification'}})
assert sys.stdin.read()==''
""")
        result = await settle(self.run_native('claude', RunControl()), 3)
        self.assertFalse(result.success)
        self.assertFalse(result.quota_limited)
        self.assertNotEqual(result.failure_code, 'pending_work')

    async def test_claude_exit_with_background_work_is_pending_work(self):
        self.fake("""
read()
emit({'type':'system','subtype':'init','session_id':sid})
emit({'type':'system','subtype':'background_tasks_changed','session_id':sid,
      'tasks':[{'task_id':'a1','task_type':'local_bash','description':'tests'}]})
emit({'type':'result','subtype':'success','session_id':sid,'result':'tests run in the background'})
""")
        result = await settle(self.run_native('claude', RunControl()))
        self.assertFalse(result.success)
        self.assertEqual(result.failure_code, 'pending_work')

    async def test_claude_queued_lifecycle_is_the_receipt_and_the_later_start_is_reported(self):
        self.claude("""
emit({'type':'command_lifecycle','command_uuid':steer['uuid'],'state':'queued','session_id':sid})
emit({**result,'result':'first'})
time.sleep(1.2)
emit({'type':'command_lifecycle','command_uuid':steer['uuid'],'state':'started','session_id':sid})
emit(echo)
emit({**result,'result':'second'})
assert sys.stdin.read()==''
""")
        outcomes = []
        control = RunControl(receipt_timeout=.3 * SCALE, on_outcome=lambda *args: outcomes.append(args))
        _, receipt, result = await self.exercise('claude', control)
        self.assertEqual(receipt, 'queued')
        self.assertTrue(result.success, result.error)
        self.assertEqual(result.text, 'second')
        self.assertEqual(outcomes, [('message-1', 'received')])

    async def test_claude_queued_steer_cancelled_before_start_is_uncertain(self):
        self.claude("""
emit({'type':'command_lifecycle','command_uuid':steer['uuid'],'state':'queued','session_id':sid})
emit({'type':'command_lifecycle','command_uuid':steer['uuid'],'state':'cancelled','session_id':sid})
emit(result)
assert sys.stdin.read()==''
""")
        outcomes = []
        control = RunControl(receipt_timeout=.3 * SCALE, on_outcome=lambda *args: outcomes.append(args))
        _, receipt, result = await self.exercise('claude', control)
        self.assertEqual(receipt, 'queued')
        self.assertTrue(result.success, result.error)
        self.assertEqual(outcomes, [('message-1', 'uncertain')])

    async def test_claude_late_echo_after_the_receipt_timeout_reports_received(self):
        self.claude("time.sleep(1.2)\nemit(echo)\nemit(result)\nassert sys.stdin.read()==''\n")
        outcomes = []
        control = RunControl(receipt_timeout=.3, on_outcome=lambda *args: outcomes.append(args))
        _, receipt, result = await self.exercise('claude', control)
        self.assertEqual(receipt, 'uncertain')
        self.assertTrue(result.success, result.error)
        self.assertEqual(outcomes, [('message-1', 'received')])

    async def test_claude_written_without_ack_is_uncertain(self):
        self.claude("emit(result)\nassert sys.stdin.read()==''\n")
        _, receipt, result = await self.exercise('claude')
        self.assertEqual(receipt, 'uncertain')
        self.assertTrue(result.success, result.error)

    async def test_claude_wrong_uuid_is_not_receipt(self):
        self.claude("echo['uuid']='wrong'\nemit(echo)\nemit(result)\nsys.stdin.read()\n")
        _, receipt, _ = await self.exercise('claude')
        self.assertEqual(receipt, 'uncertain')

    async def test_claude_wrong_session_fails_and_settles_receipt(self):
        self.claude("echo['session_id']='" + str(uuid.uuid4()) + "'\nemit(echo)\ntime.sleep(60)\n")
        _, receipt, result = await self.exercise('claude')
        self.assertEqual(receipt, 'uncertain')
        self.assertFalse(result.success)

    async def test_codex_ack_after_terminal_is_received(self):
        self.codex("emit(terminal)\nemit(ack)\nsys.stdin.read()\n")
        _, receipt, result = await self.exercise('codex')
        self.assertEqual(receipt, 'received')
        self.assertTrue(result.success, result.error)

    async def test_codex_ack_before_terminal_is_received(self):
        self.codex("emit(ack)\nemit(terminal)\nsys.stdin.read()\n")
        _, receipt, result = await self.exercise('codex')
        self.assertEqual(receipt, 'received')
        self.assertTrue(result.success, result.error)

    async def test_codex_terminal_rejects_steer_without_replay(self):
        self.codex("emit(terminal)\nemit({'id':r['id'],'error':{'code':-32600,'message':'no active turn'}})\nassert sys.stdin.read()==''\n")
        _, receipt, result = await self.exercise('codex')
        self.assertEqual(receipt, 'closed')
        self.assertTrue(result.success, result.error)

    async def test_codex_missing_receipt_is_uncertain(self):
        self.codex("emit(terminal)\nsys.stdin.read()\n")
        _, receipt, result = await self.exercise('codex')
        self.assertEqual(receipt, 'uncertain')
        self.assertTrue(result.success, result.error)

    async def test_codex_wrong_turn_receipt_is_uncertain(self):
        self.codex("ack['result']['turnId']='wrong'\nemit(ack)\nemit(terminal)\nsys.stdin.read()\n")
        _, receipt, _ = await self.exercise('codex')
        self.assertEqual(receipt, 'uncertain')

    async def test_cancel_settles_steer_and_releases_native_lock(self):
        self.claude("open('steer-read','w').close()\ntime.sleep(60)\n")
        control = RunControl()
        started = asyncio.Event()
        pids = []
        def on_start(pid):
            pids.append(pid)
            started.set()
        task = asyncio.create_task(self.run_native('claude', control, on_start=on_start))
        steering = asyncio.create_task(control.steer('message-1', 'new instruction'))
        await started.wait()
        await until_exists(self.root / 'steer-read')
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(await settle(steering), 'uncertain')
        with self.assertRaises(ProcessLookupError):
            os.kill(pids[0], 0)
        self.claude("emit(echo)\nemit(result)\nsys.stdin.read()\n")
        _, receipt, result = await self.exercise('claude')
        self.assertEqual(receipt, 'received')
        self.assertTrue(result.success)

    async def test_control_rebind_preserves_old_receipt_and_allows_new_input(self):
        self.claude("emit(echo)\nemit(result)\nsys.stdin.read()\n")
        control, _, _ = await self.exercise('claude')
        task = asyncio.create_task(self.run_native('claude', control))
        await asyncio.sleep(0)
        self.assertEqual(await control.steer('message-2', 'new instruction'), 'received')
        self.assertTrue((await settle(task)).success)
        self.assertEqual(await control.steer('message-1', 'new instruction'), 'received')

    async def test_session_callback_precedes_steer_and_failure(self):
        self.codex("emit(ack)\nemit(terminal)\nsys.stdin.read()\n")
        control = RunControl()
        sessions = []
        task = asyncio.create_task(self.run_native('codex', control, on_session=sessions.append))
        self.assertEqual(await control.steer('message-1', 'new instruction'), 'received')
        self.assertEqual(sessions, [self.sid])
        self.assertTrue((await task).success)

    async def test_claude_echo_after_result_without_next_terminal_fails(self):
        self.claude("emit(result)\nemit(echo)\n")
        _, receipt, result = await self.exercise('claude')
        self.assertEqual(receipt, 'received')
        self.assertFalse(result.success)

    async def test_fresh_codex_persists_session_before_failure(self):
        self.fake("""
r=read(); emit({'id':r['id'],'result':{}})
read()
r=read(); assert r['method']=='thread/start'
emit({'id':r['id'],'result':{'thread':{'id':sid}}})
r=read(); assert r['method']=='turn/start'
emit({'id':r['id'],'result':{'turn':{'id':'turn-1'}}})
emit({'method':'turn/started','params':{'threadId':sid,'turn':{'id':'turn-1'}}})
emit({'method':'account/rateLimits/updated','params':{'rateLimits':{'primary':{'resetsAt':9999999999}}}})
emit({'method':'turn/completed','params':{'threadId':sid,'turn':{'id':'turn-1','status':'failed','error':{'codexErrorInfo':'usageLimitExceeded','message':'quota'}}}})
sys.stdin.read()
""")
        sessions = []
        result = await settle(self.runner.run('codex', 'initial task', self.root,
            None, fresh=True, control=RunControl(), on_session=sessions.append))
        self.assertEqual(sessions, [self.sid])
        self.assertEqual(result.session_id, self.sid)
        self.assertFalse(result.success)
        self.assertTrue(result.quota_limited)
        self.assertEqual(result.rate_limit_info, {'primary': {'resetsAt': 9999999999}})

    async def test_fresh_codex_accepts_thread_notice_before_thread_started(self):
        self.fake("""
r=read(); emit({'id':r['id'],'result':{}})
read()
r=read(); assert r['method']=='thread/start'
emit({'id':r['id'],'result':{'thread':{'id':sid}}})
emit({'method':'mcpServer/startupStatus/updated','params':{'threadId':sid,'status':'ready'}})
emit({'method':'thread/started','params':{'thread':{'id':sid}}})
r=read(); assert r['method']=='turn/start'
emit({'id':r['id'],'result':{'turn':{'id':'turn-1'}}})
emit({'method':'turn/started','params':{'threadId':sid,'turn':{'id':'turn-1'}}})
emit({'method':'item/completed','params':{'threadId':sid,'turnId':'turn-1','item':{'type':'agentMessage','text':'done'}}})
emit({'method':'turn/completed','params':{'threadId':sid,'turn':{'id':'turn-1','status':'completed'}}})
sys.stdin.read()
""")
        sessions = []
        result = await settle(self.runner.run('codex', 'initial task', self.root,
            None, fresh=True, control=RunControl(), on_session=sessions.append))
        self.assertTrue(result.success, result.error)
        self.assertEqual(sessions, [self.sid])
        self.assertEqual(result.session_id, self.sid)

    async def test_codex_subagent_thread_events_leave_the_main_turn_running(self):
        child = str(uuid.uuid4())
        self.fake("""
child=%r
r=read(); emit({'id':r['id'],'result':{}})
read()
r=read(); assert r['method']=='thread/start'
emit({'id':r['id'],'result':{'thread':{'id':sid}}})
emit({'method':'thread/started','params':{'thread':{'id':sid}}})
r=read(); assert r['method']=='turn/start'
emit({'id':r['id'],'result':{'turn':{'id':'turn-1'}}})
emit({'method':'turn/started','params':{'threadId':sid,'turn':{'id':'turn-1'}}})
emit({'method':'item/completed','params':{'threadId':sid,'turnId':'turn-1','item':{'type':'subAgentActivity',
      'kind':'started','agentThreadId':child,'agentPath':'/root/comment_review'}}})
emit({'method':'thread/started','params':{'thread':{'id':child}}})
emit({'method':'thread/status/changed','params':{'threadId':child,'status':{'type':'idle'}}})
emit({'method':'mcpServer/startupStatus/updated','params':{'threadId':child,'name':'codex_apps','status':'starting'}})
emit({'method':'thread/status/changed','params':{'threadId':child,'status':{'type':'active','activeFlags':[]}}})
emit({'method':'turn/started','params':{'threadId':child,'turn':{'id':'child-turn','status':'inProgress'}}})
emit({'method':'item/completed','params':{'threadId':child,'turnId':'child-turn','item':{'type':'agentMessage','text':'child notes'}}})
emit({'method':'turn/completed','params':{'threadId':child,'turn':{'id':'child-turn','status':'completed'}}})
emit({'method':'item/completed','params':{'threadId':sid,'turnId':'turn-1','item':{'type':'agentMessage','text':'main report'}}})
emit({'method':'turn/completed','params':{'threadId':sid,'turn':{'id':'turn-1','status':'completed'}}})
sys.stdin.read()
""" % child)
        sessions = []
        result = await settle(self.runner.run('codex', 'initial task', self.root,
            None, fresh=True, control=RunControl(), on_session=sessions.append))
        self.assertTrue(result.success, result.error)
        self.assertEqual(result.text, 'main report')
        self.assertEqual(sessions, [self.sid])

    async def test_early_exit_settles_waiting_control_without_hanging(self):
        self.fake("sys.exit(1)\n")
        control, receipt, result = await self.exercise('codex')
        self.assertEqual(receipt, 'closed')
        self.assertFalse(result.success)

    async def test_codex_wrong_native_uuid_stops_before_turn(self):
        self.fake("""
r=read(); emit({'id':r['id'],'result':{}})
read()
r=read(); emit({'id':r['id'],'result':{'thread':{'id':'invalid'}}})
time.sleep(60)
""")
        _, receipt, result = await self.exercise('codex')
        self.assertEqual(receipt, 'closed')
        self.assertFalse(result.success)
        self.assertEqual(result.session_id, self.sid)
        with self.assertRaises(ProcessLookupError):
            os.kill(result.pid, 0)

    async def test_steer_caller_cancellation_does_not_resubmit(self):
        self.claude("""
open('steer-read','w').close()
while not os.path.exists('release-echo'):
    time.sleep(.01)
emit(echo)
emit(result)
assert sys.stdin.read()==''
""")
        control = RunControl()
        task = asyncio.create_task(self.run_native('claude', control))
        steering = asyncio.create_task(control.steer('message-1', 'new instruction'))
        await until_exists(self.root / 'steer-read')
        steering.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await steering
        (self.root / 'release-echo').touch()
        self.assertEqual(await control.steer('message-1', 'new instruction'), 'received')
        self.assertTrue((await task).success)

    async def test_backpressured_claude_steering_times_out_without_replay(self):
        self.fake("""
read()
emit({'type':'system','subtype':'init','session_id':sid})
time.sleep(60)
""")
        control = RunControl(receipt_timeout=.1)
        task = asyncio.create_task(self.run_native('claude', control))
        try:
            receipt = await settle(control.steer('large', 'x' * (4 * 1024 * 1024)))
            self.assertEqual(receipt, 'uncertain')
            self.assertEqual(await control.steer('large', 'do not resend'), 'uncertain')
        finally:
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

    async def test_fresh_codex_thread_start_backpressure_is_bounded_and_reaped(self):
        self.fake("""
r=read(); emit({'id':r['id'],'result':{}})
assert read()['method']=='initialized'
time.sleep(60)
""")
        control = RunControl(receipt_timeout=.1)
        result = await settle(self.runner.run('codex', 'initial task', self.root,
            None, fresh=True, control=control, model='x' * (32 * 1024 * 1024)))
        self.assertFalse(result.success)
        self.assertEqual(await control.steer('after-exit', 'follow-up'), 'closed')
        with self.assertRaises(ProcessLookupError):
            os.kill(result.pid, 0)


class RequestTimeoutTests(unittest.IsolatedAsyncioTestCase):
    async def test_codex_initialize_waits_as_long_as_thread_start(self):
        class Input:
            def write(self, data):
                pass

            async def drain(self):
                pass

            async def wait_closed(self):
                await asyncio.Event().wait()

        class Process:
            stdin = Input()

            def past_request(self, request_id):
                return None

        timeouts = []

        async def expire(awaitable, timeout):
            awaitable.close()
            timeouts.append(timeout)
            raise asyncio.TimeoutError()

        protocol = NativeProtocol('codex', Process(), RunControl(receipt_timeout=30), None, lambda value: value)
        waited = {}
        try:
            with patch('coordinator.native_protocol.asyncio.wait_for', expire):
                for method in ('initialize', 'thread/start', 'turn/start'):
                    with self.assertRaises(asyncio.TimeoutError):
                        await protocol.request(method, {})
                    waited[method] = timeouts[-1]
        finally:
            protocol.input_closed.cancel()
            await asyncio.gather(protocol.input_closed, return_exceptions=True)
        self.assertEqual(waited['initialize'], waited['thread/start'])
        self.assertEqual(waited['initialize'], 10 * waited['turn/start'])


class PersistentCodexProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from types import SimpleNamespace
        self.events = []
        self.sid = str(uuid.uuid4())
        self.turn = 0
        self.race = False
        self.input_closed = asyncio.Event()
        owner = self

        class Stdin:
            def write(self, data):
                owner.events.append(json.loads(data))

            async def drain(self):
                request = owner.events[-1]
                method = request.get('method')
                if method == 'initialize':
                    result = {}
                elif method == 'thread/start':
                    result = {'thread': {'id': owner.sid}}
                elif method == 'turn/start':
                    if owner.race:
                        owner.race = False
                        owner.complete()
                    if owner.protocol.turn_id is None:
                        owner.turn += 1
                    turn = {'id': owner.protocol.turn_id or 'turn-%d' % owner.turn}
                    owner.protocol.handle({'method': 'turn/started', 'params': {
                        'threadId': owner.sid, 'turn': turn}})
                    result = {'turn': turn}
                elif method == 'turn/steer':
                    if owner.race:
                        owner.race = False
                        owner.complete()
                        owner.protocol.handle({'id': request['id'], 'error': {'code': -1}})
                        return
                    result = {'turnId': request['params']['expectedTurnId']}
                else:
                    return
                owner.protocol.handle({'id': request['id'], 'result': result})

            async def wait_closed(self):
                await owner.input_closed.wait()

            def close(self):
                owner.input_closed.set()

        self.process = SimpleNamespace(stdin=Stdin(), written_lines=lambda: [],
                                       past_request=lambda number: None, max_request_id=lambda: 0)
        self.protocol = NativeProtocol('codex', self.process, RunControl(), None,
                                       lambda sid: sid, persistent=True)
        await self.protocol.start('', Path('/tmp'), True, 'test-model', developer_instructions='policy')

    async def asyncTearDown(self):
        self.protocol.disconnect()
        self.input_closed.set()
        await self.protocol.input_closed

    def complete(self):
        return self.protocol.handle({'method': 'turn/completed', 'params': {
            'threadId': self.sid, 'turn': {'id': self.protocol.turn_id, 'status': 'completed'}}})

    async def test_handshake_waits_for_input_and_multiple_turns_share_the_process(self):
        self.assertEqual([event['method'] for event in self.events],
                         ['initialize', 'initialized', 'thread/start'])
        params = self.events[-1]['params']
        self.assertEqual(params['developerInstructions'], 'policy')
        self.assertEqual(params['approvalPolicy'], 'never')
        self.assertEqual(params['sandbox'], 'danger-full-access')
        self.assertEqual(await self.protocol.control.steer('first', 'one'), 'received')
        self.assertEqual(await self.protocol.control.steer('second', 'two'), 'received')
        self.assertEqual(self.events[-1]['params']['clientUserMessageId'], 'second')
        self.assertEqual(self.events[-1]['method'], 'turn/start')
        self.assertNotIn('expectedTurnId', self.events[-1]['params'])
        self.assertEqual(self.complete(), {'type': 'turn.completed', 'turn_id': 'turn-1'})
        self.assertFalse(self.protocol.terminal)
        self.assertFalse(self.input_closed.is_set())
        self.assertEqual(await self.protocol.control.steer('third', 'three'), 'received')
        self.assertEqual(self.protocol.turn_id, 'turn-2')

    async def test_steer_racing_turn_completion_starts_the_next_turn_once(self):
        await self.protocol.steer('first', 'one')
        self.race = True
        self.assertEqual(await self.protocol.steer('second', 'two'), 'received')
        self.assertEqual([event['method'] for event in self.events[-2:]],
                         ['turn/start', 'turn/start'])
        self.assertEqual(self.protocol.turn_id, 'turn-2')

    async def test_host_request_is_refused_and_the_parent_remains_available(self):
        event = self.protocol.handle({'id': 900, 'method': 'item/tool/requestUserInput', 'params': {}})
        self.assertEqual(event['type'], 'host.request')
        await asyncio.sleep(0)
        self.assertEqual(self.events[-1]['error']['code'], -32601)
        self.assertEqual(await self.protocol.steer('first', 'one'), 'received')
        self.assertFalse(self.protocol.closed)

    async def test_overlapping_completion_and_late_notices_keep_the_old_turn_active(self):
        await self.protocol.steer('first', 'one')
        self.protocol.handle({'id': 900, 'result': {'turn': {'id': 'turn-2'}}})
        self.assertEqual(self.protocol.turn_id, 'turn-1')
        params = {'threadId': self.sid, 'turnId': 'turn-2',
                  'item': {'type': 'agentMessage', 'text': 'second'}}
        self.assertEqual(self.protocol.handle({'method': 'item/completed', 'params': params}),
                         {'type': 'item.completed', 'turn_id': 'turn-2',
                          'item': {'type': 'agent_message', 'text': 'second'}})
        completed = {'method': 'turn/completed', 'params': {
            'threadId': self.sid, 'turn': {'id': 'turn-2', 'status': 'completed'}}}
        self.assertEqual(self.protocol.handle(completed), {'type': 'turn.completed', 'turn_id': 'turn-2'})
        self.assertEqual(self.protocol.turn_id, 'turn-1')
        self.assertIsNone(self.protocol.handle(completed))
        self.protocol.handle({'id': 900, 'result': {'turn': {'id': 'turn-2'}}})
        self.protocol.handle({'method': 'turn/started', 'params': {
            'threadId': self.sid, 'turn': {'id': 'turn-2'}}})
        self.assertEqual(self.protocol.turn_id, 'turn-1')
        self.assertIsNone(self.protocol.handle({'method': 'item/completed', 'params': params}))
        completed['params']['turn']['id'] = 'unknown-turn'
        self.assertIsNone(self.protocol.handle(completed))
        self.complete()
        self.assertIsNone(self.protocol.turn_id)

    async def test_tool_items_and_user_receipts_are_observable_and_old_items_are_ignored(self):
        await self.protocol.steer('first', 'one')
        params = {'threadId': self.sid, 'turnId': 'turn-1', 'item': {
            'type': 'mcpToolCall', 'server': 'torii', 'tool': 'telegram_send'}}
        event = self.protocol.handle({'method': 'item/started', 'params': params})
        self.assertEqual(event['message']['content'][0]['name'], 'mcp__torii__telegram_send')
        params['item'] = {'type': 'userMessage', 'clientUserMessageId': 'first'}
        self.assertEqual(self.protocol.handle({'method': 'item/completed', 'params': params}),
                         {'type': 'user.received', 'turn_id': 'turn-1', 'message_id': 'first'})
        self.complete()
        self.assertIsNone(self.protocol.handle({'method': 'item/completed', 'params': params}))


class NativeControlTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.process = type('Process', (), {'stdin': type('Input', (), {'wait_closed': AsyncMock(),
                                                                      'private': AsyncMock()})()})()
        self.protocol = NativeProtocol('claude', self.process, RunControl(), None, lambda value: value)
        self.addAsyncCleanup(self.cleanup)

    async def cleanup(self):
        self.protocol.disconnect()
        self.protocol.input_closed.cancel()
        await asyncio.gather(self.protocol.input_closed, return_exceptions=True)

    async def test_control_request_goes_to_the_handler_and_is_consumed(self):
        calls = []
        self.protocol.on_control_request = lambda protocol, event, replay: calls.append((protocol, event, replay)) or True
        event = {'type': 'control_request', 'request': {'subtype': 'local-operation'}}
        self.assertIsNone(self.protocol.handle(event))
        self.assertEqual(calls, [(self.protocol, event, False)])

    async def test_unhandled_control_request_writes_nothing(self):
        self.assertIsNone(self.protocol.on_control_request)
        self.assertIsNone(self.protocol.failure)
        self.protocol.handle({'type': 'control_request', 'request': {'subtype': 'local-operation'}})
        self.process.stdin.private.assert_not_awaited()

    async def test_native_refresh_request_is_consumed_without_a_handler(self):
        event = {'type': 'control_request', 'request_id': 'refresh-1',
                 'request': {'subtype': 'oauth_token_refresh'}, 'session_id': 'unexpected'}
        for replay in (False, True):
            with self.subTest(replay=replay):
                self.assertIsNone(self.protocol.handle(event, replay=replay))
                self.assertIsNone(self.protocol.session_id)
                self.assertIsNone(self.protocol.failure)
                self.assertEqual(self.protocol._tasks, set())
                self.process.stdin.private.assert_not_awaited()

    async def test_native_refresh_request_defers_to_an_installed_handler(self):
        event = {'type': 'control_request', 'request': {'subtype': 'oauth_token_refresh'}}
        calls = []
        self.protocol.on_control_request = lambda protocol, event, replay: calls.append((event, replay)) or False
        for replay in (False, True):
            self.assertIs(self.protocol.handle(event, replay=replay), event)
        self.assertEqual(calls, [(event, False), (event, True)])

    async def test_track_keeps_and_cancels_tasks(self):
        task = self.protocol.track(asyncio.Event().wait())
        self.assertIn(task, self.protocol._tasks)
        await asyncio.sleep(0)
        self.protocol.disconnect()
        await asyncio.gather(task, return_exceptions=True)
        self.assertTrue(task.cancelled())
