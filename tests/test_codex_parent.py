import asyncio
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch
import uuid

from coordinator.codex_session import CodexCoordinatorSession, codex_transcript_receipts, toml
from coordinator.providers import ProviderRunner
from coordinator.session import message_tag, message_uuid
from coordinator.store import Store
from tests.support import isolate_shared_mcp_home, stop_test_hosts, until, settle


APP_SERVER = '''#!/usr/bin/env python3
import json,os,sys,time,uuid
from pathlib import Path
root=Path(__file__).parent
home=Path(os.environ['CODEX_HOME'])
with (root/'launches').open('a') as log:
    log.write(json.dumps(sys.argv[1:])+'\\n')
thread=None
turn=None
number=0
def emit(value):
    print(json.dumps(value),flush=True)
def complete():
    global turn
    emit({'method':'item/completed','params':{'threadId':thread,'turnId':turn,'item':{'type':'agentMessage','text':'done'}}})
    emit({'method':'turn/completed','params':{'threadId':thread,'turn':{'id':turn,'status':'completed'}}})
    turn=None
for line in sys.stdin:
    request=json.loads(line)
    with (root/'requests').open('a') as log:
        log.write(json.dumps(request)+'\\n')
    method=request.get('method')
    params=request.get('params') or {}
    if method=='initialize':
        emit({'id':request['id'],'result':{}})
    elif method in ('thread/start','thread/resume'):
        assert params['approvalPolicy']=='never' and params['sandbox']=='danger-full-access'
        assert 'policy' in params['developerInstructions'] and params['model']=='test-codex'
        if method=='thread/resume' and (root/'reject-resume').exists():
            emit({'id':request['id'],'error':{'code':-1}})
            continue
        thread=params.get('threadId') or str(uuid.uuid4())
        path=home/'sessions'/('rollout-test-'+thread+'.jsonl')
        path.parent.mkdir(parents=True,exist_ok=True)
        path.touch(exist_ok=True)
        emit({'method':'thread/started','params':{'thread':{'id':thread}}})
        emit({'id':request['id'],'result':{'thread':{'id':thread,'path':str(path)}}})
    elif method in ('turn/start','turn/steer'):
        text=params['input'][0]['text']
        with path.open('a') as log:
            log.write(json.dumps({'type':'response_item','payload':{'role':'user','content':[{'type':'input_text','text':text}]}})+'\\n')
        if text.endswith('reject-once') and not (root/'rejected').exists():
            (root/'rejected').touch()
            emit({'id':request['id'],'error':{'code':-32600,'message':'ExpectedTurnMismatch'}})
            continue
        if text.endswith('end-race') and turn is not None:
            old=turn
            number+=1
            turn='turn-'+str(number)
            if (root/'started-first').exists():
                emit({'method':'turn/started','params':{'threadId':thread,'turn':{'id':turn}}})
            emit({'id':request['id'],'result':{'turn':{'id':turn}}})
            emit({'method':'item/completed','params':{'threadId':thread,'turnId':old,'item':{'type':'agentMessage','text':'old'}}})
            for tool in ('commandExecution','fileChange'):
                emit({'method':'item/started','params':{'threadId':thread,'turnId':turn,'item':{'type':tool}}})
            emit({'method':'item/completed','params':{'threadId':thread,'turnId':turn,'item':{'type':'agentMessage','text':'done'}}})
            (root/'race-ready').touch()
            while not (root/'release-old').exists():
                time.sleep(.005)
            emit({'method':'turn/completed','params':{'threadId':thread,'turn':{'id':old,'status':'completed'}}})
            if not (root/'started-first').exists():
                emit({'method':'turn/started','params':{'threadId':thread,'turn':{'id':turn}}})
            while not (root/'release-new').exists():
                time.sleep(.005)
            emit({'method':'turn/completed','params':{'threadId':thread,'turn':{'id':turn,'status':'completed'}}})
            turn=None
            continue
        if method=='turn/steer' and params.get('expectedTurnId')!=turn:
            emit({'id':request['id'],'error':{'code':-32600,'message':'ExpectedTurnMismatch'}})
            continue
        if method=='turn/start' and turn is None:
            number+=1
            turn='turn-'+str(number)
            emit({'id':request['id'],'result':{'turn':{'id':turn}}})
            emit({'method':'turn/started','params':{'threadId':thread,'turn':{'id':turn}}})
        elif method=='turn/start':
            emit({'id':request['id'],'result':{'turn':{'id':turn}}})
        else:
            emit({'id':request['id'],'result':{'turnId':turn}})
        if text.endswith('hold') and not text.endswith('quota-hold'):
            for tool in ('commandExecution','fileChange'):
                emit({'method':'item/started','params':{'threadId':thread,'turnId':turn,'item':{'type':tool}}})
        elif text.endswith('host-request'):
            emit({'id':'server-request','method':'item/tool/requestUserInput','params':{}})
        elif text.endswith('quota') or text.endswith('quota-hold'):
            emit({'method':'account/rateLimits/updated','params':{'rateLimits':{'primary':{
                'usedPercent':100,'windowDurationMins':300,'resetsAt':time.time()+3600}}}})
            if text.endswith('quota'):
                emit({'method':'turn/completed','params':{'threadId':thread,'turn':{'id':turn,'status':'failed',
                      'error':{'codexErrorInfo':'usageLimitExceeded'}}}})
        elif text.endswith('exit'):
            complete()
            sys.exit(0)
        else:
            complete()
    elif request.get('id')=='server-request' and 'error' in request:
        complete()
'''


class CodexParentTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)
        isolate_shared_mcp_home(self, self.root)
        self.addCleanup(stop_test_hosts, self.root)
        self.binary = self.root / 'codex'
        self.binary.write_text(APP_SERVER)
        self.binary.chmod(0o700)
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        self.profile = self.root / 'gpt'
        self.profile.mkdir()
        self.store.put('codex_accounts', {'gpt': {'enabled': True, 'config_dir': str(self.profile)}})
        self.store.put('codex_account_status', {'gpt': {'identity': {
            'email': 'gpt@example.test', 'logged_in': True}}})
        self.store.put('codex_model', 'test-codex')
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('home',1,1,'Home',?,1)",
                              (str(self.root),))
        self.store.put('coordinator_home_topic', 'home')
        self.store.db.commit()
        self.runner = ProviderRunner(self.store.directory, {'codex': str(self.binary)})
        self.session = None
        self.turns = []
        self.next_message = 100

    async def asyncTearDown(self):
        if self.session:
            await self.session.stop()

    async def start(self):
        self.session = await CodexCoordinatorSession.start(self.store, self.runner, self.root, 'ignored',
                            'policy', on_turn=self.turns.append, receipt_timeout=1)
        return self.session

    def message(self, number):
        return dict(self.store.db.execute('SELECT * FROM messages WHERE id=?', (number,)).fetchone())

    async def send(self, text):
        self.next_message += 1
        row = self.store.message_save('home', 'owner', text, telegram_message=self.next_message)
        self.store.message_sending(row['id'])
        receipt = await self.session.send(message_uuid(row['id']), message_tag(self.store.topic('home'), row) + text,
                                          row_id=row['id'])
        self.store.message_delivered(row['id'], 'written' if receipt == 'received' else 'uncertain')
        return row, receipt

    async def test_parent_uses_one_host_for_multiple_turns_and_clears_reactions(self):
        session = await self.start()
        self.assertTrue(session.idle())
        pid = session.process.pid
        row, receipt = await self.send('hold')
        self.assertEqual(receipt, 'received')
        await until(lambda: self.message(row['id'])['reaction_desired'] == '👀')
        self.assertTrue(session.busy)
        self.assertEqual((await self.send('finish'))[1], 'received')
        await until(session.idle)
        self.assertIsNone(self.message(row['id'])['reaction_desired'])
        self.assertEqual((await self.send('again'))[1], 'received')
        await until(lambda: len(self.turns) == 2)
        self.assertEqual(session.process.pid, pid)
        self.assertEqual(self.message(row['id'])['receipt'], 'replayed')
        argv = json.loads((self.root / 'launches').read_text().splitlines()[0])
        self.assertIn('mcp_servers.torii.default_tools_approval_mode="approve"', argv)
        self.assertIn('mcp_servers.torii.omit_tools_from=["deferred", "code_mode"]', argv)
        self.assertIn('model_reasoning_effort="xhigh"', argv)

    async def test_reattach_rebuilds_an_active_turn_without_repeating_handshake_or_input(self):
        session = await self.start()
        row, _ = await self.send('hold')
        await until(lambda: session.protocol.turn_id is not None and session.busy)
        pid, sid = session.process.pid, session.session_id
        await session.detach()
        session = await self.start()
        self.assertEqual((session.process.pid, session.session_id), (pid, sid))
        self.assertTrue(session.busy)
        self.assertEqual((await self.send('finish'))[1], 'received')
        await until(session.idle)
        requests = [json.loads(line) for line in (self.root / 'requests').read_text().splitlines()]
        self.assertEqual(sum(request.get('method') == 'initialize' for request in requests), 1)
        self.assertEqual(sum(request.get('method') == 'thread/start' for request in requests), 1)
        self.assertEqual(sum(request.get('method') == 'turn/start' for request in requests), 2)
        self.assertEqual(codex_transcript_receipts(session.transcript_path, [row['id']]), {row['id']})

    async def test_dead_host_resumes_and_a_rejected_resume_starts_a_fresh_thread(self):
        session = await self.start()
        old = session.session_id
        await session.stop()
        session = await self.start()
        self.assertEqual(session.session_id, old)
        await session.stop()
        (self.root / 'reject-resume').touch()
        session = await self.start()
        self.assertTrue(session.resume_failed)
        self.assertNotEqual(session.session_id, old)
        requests = [json.loads(line) for line in (self.root / 'requests').read_text().splitlines()]
        self.assertEqual(sum(request.get('method') == 'initialize' for request in requests), 3)

    async def test_account_pick_relaunches_when_idle_and_keeps_the_thread(self):
        session = await self.start()
        old = session.session_id
        await self.send('hold')
        other = self.root / 'other'
        other.mkdir()
        self.store.put('codex_accounts', {**self.store.get('codex_accounts'), 'other': {
            'enabled': True, 'config_dir': str(other)}})
        self.store.put('codex_account_status', {**self.store.get('codex_account_status'), 'other': {
            'identity': {'logged_in': True, 'email': 'other@example.test'}}})
        self.store.put('codex_active_account', 'other')
        self.assertTrue(session.movable())
        self.assertEqual((await self.send('finish'))[1], 'received')
        await settle(session.wait_closed())
        session = await self.start()
        self.assertEqual(session.session_id, old)
        self.assertEqual(session.account_alias, 'other')

    async def test_changed_tools_fingerprint_relaunches_a_reattached_idle_parent(self):
        session = await self.start()
        await session.detach()
        with patch('coordinator.mcp.tools_hash', return_value='changed'):
            session = await self.start()
        self.assertTrue(session.outdated)
        await settle(session.wait_closed())

    async def test_host_request_is_refused_without_losing_the_parent(self):
        session = await self.start()
        self.assertEqual((await self.send('host-request'))[1], 'received')
        await until(lambda: bool(self.turns))
        self.assertFalse(session.closed)
        self.assertEqual((await self.send('again'))[1], 'received')

    async def test_limit_preserves_the_recorded_turn_and_holds_the_active_account(self):
        session = await self.start()
        row, _ = await self.send('quota')
        await settle(session.wait_closed())
        self.assertTrue(session.rejected)
        self.assertIn('gpt', self.store.get('codex_account_blocks'))
        self.assertIsNone(self.store.get('coordinator_lost'))
        callbacks = [row['text'] for row in self.store.messages_pending() if row['kind'] == 'callback']
        self.assertTrue(any('message=%d' % row['id'] in text for text in callbacks))

    async def test_stale_turn_id_still_delivers_into_the_active_turn(self):
        session = await self.start()
        await self.send('hold')
        await until(lambda: session.protocol.turn_id is not None)
        session.protocol.turn_id = 'stale-turn'
        row, receipt = await self.send('finish')
        self.assertEqual(receipt, 'received')
        await until(session.idle)
        self.assertEqual(self.message(row['id'])['receipt'], 'replayed')
        requests = [json.loads(line) for line in (self.root / 'requests').read_text().splitlines()]
        self.assertFalse(any(request.get('method') == 'turn/steer' for request in requests))

    async def _end_of_turn_race(self, started_first=False, restart=False):
        from coordinator.reactions import finish_turn
        if started_first:
            (self.root / 'started-first').touch()
        session = await self.start()
        pid, sid = session.process.pid, session.session_id
        first, _ = await self.send('hold')
        await until(lambda: self.message(first['id'])['reaction_desired'] == '👀')
        with patch('coordinator.reactions.finish_turn', wraps=finish_turn) as finish:
            second, receipt = await self.send('end-race')
            self.assertEqual(receipt, 'received')
            await until(lambda: session.closed or self.message(second['id'])['reaction_desired'] == '👀')
            self.assertFalse(session.closed, session._failure)
            self.assertEqual(session.protocol.turn_id, 'turn-1')
            if restart:
                await session.detach()
                session = await self.start()
                self.assertEqual((session.process.pid, session.session_id), (pid, sid))
                self.assertEqual(session.protocol.turn_id, 'turn-1')
            (self.root / 'release-old').touch()
            await until(lambda: self.turns or session.closed)
            self.assertFalse(session.closed, session._failure)
            self.assertEqual(self.turns, ['old'])
            self.assertEqual(session.protocol.turn_id, 'turn-2')
            self.assertFalse(session.idle())
            self.assertIsNone(self.message(first['id'])['reaction_desired'])
            self.assertIsNone(self.message(first['id'])['turn_started'])
            self.assertEqual(self.message(second['id'])['reaction_desired'], '👀')
            self.assertIsNotNone(self.message(second['id'])['turn_started'])
            (self.root / 'release-new').touch()
            await until(lambda: session.idle() or session.closed)
            self.assertFalse(session.closed, session._failure)
            self.assertEqual(self.turns, ['old', 'done'])
            self.assertEqual([call.args[1] for call in finish.call_args_list],
                             [{first['id']}, {second['id']}])
        self.assertIsNone(session._failure)
        self.assertEqual(session.process.pid, pid)
        self.assertIsNone(session.process.returncode)
        self.assertIsNone(session.protocol.turn_id)
        self.assertTrue(session.idle())
        for row in (first, second):
            self.assertEqual(self.message(row['id'])['receipt'], 'replayed')
            self.assertEqual(self.message(row['id'])['delivered'], 'received')
            self.assertIsNone(self.message(row['id'])['reaction_desired'])
            self.assertIsNone(self.message(row['id'])['turn_started'])
        requests = [json.loads(line) for line in (self.root / 'requests').read_text().splitlines()]
        self.assertEqual(sum(request.get('method') == 'turn/start' for request in requests), 2)
        self.assertEqual(sum(request.get('method') == 'initialize' for request in requests), 1)
        self.assertFalse(self.store.messages_pending())

    async def test_response_then_old_completion_keeps_both_turns(self):
        await self._end_of_turn_race()

    async def test_new_turn_started_before_old_completion_keeps_both_turns(self):
        await self._end_of_turn_race(started_first=True)

    async def test_reattach_replays_response_before_old_completion(self):
        await self._end_of_turn_race(restart=True)

    async def test_reattach_replays_started_before_old_completion(self):
        await self._end_of_turn_race(started_first=True, restart=True)

    async def test_rejected_input_is_requeued_and_survives_reattach_then_answers(self):
        from coordinator.service import Service
        from tests.test_service import FakeTelegram
        session = await self.start()
        service = Service(self.store, FakeTelegram(), self.runner, self.root)
        row = self.store.message_save('home', 'owner', 'reject-once')
        prompt = message_tag(self.store.topic('home'), row) + row['text']
        self.assertEqual(await service._send_message(row, prompt, session), 'unsupported')
        self.assertEqual(self.message(row['id'])['delivered'], 'pending')
        self.assertNotIn(row['id'], session._unreplayed)
        self.assertTrue(session.idle())
        await session.detach()
        session = await self.start()
        self.assertTrue(session.idle())
        self.assertNotIn(row['id'], session._unreplayed)
        self.assertEqual(await service._send_message(row, prompt, session), 'received')
        await until(session.idle)
        self.assertEqual(self.message(row['id'])['receipt'], 'replayed')

    async def test_restore_requeues_a_rejection_before_service_recorded_it(self):
        session = await self.start()
        row = self.store.message_save('home', 'owner', 'rejected')
        self.store.message_sending(row['id'])
        wire = message_uuid(row['id'])
        session._rows[wire] = row['id']
        session._track({row['id']})
        request = {'id': 999, 'method': 'turn/steer', 'params': {'clientUserMessageId': wire}}
        with patch.object(session.process, 'written_lines', return_value=[json.dumps(request)]), \
                patch.object(session.process, 'past_response', return_value={'id': 999, 'error': {'code': -32600}}):
            session._restore_state([])
        self.assertEqual(self.message(row['id'])['delivered'], 'pending')
        self.assertNotIn(row['id'], session._unreplayed)
        self.assertNotIn(row['id'], session._turn_rows)
        self.assertTrue(session.idle())

    async def test_restore_uses_the_latest_attempt_after_a_rejected_input(self):
        session = await self.start()
        row = self.store.message_save('home', 'owner', 'retried')
        self.store.message_sending(row['id'])
        wire = message_uuid(row['id'])
        session._rows[wire] = row['id']
        attempts = [json.dumps({'id': number, 'method': 'turn/start',
                               'params': {'clientUserMessageId': wire}}) for number in (998, 999)]
        responses = {998: {'error': {'code': -32600}}, 999: {'result': {'turn': {'id': 'turn-1'}}}}
        with patch.object(session.process, 'written_lines', return_value=attempts), \
                patch.object(session.process, 'past_response', side_effect=responses.get):
            session._restore_state([])
        self.assertEqual(self.message(row['id'])['receipt'], 'replayed')
        self.assertNotIn(row['id'], session._unreplayed)

    async def test_rate_limit_waits_for_the_current_turn_and_keeps_its_reply(self):
        session = await self.start()
        row, receipt = await self.send('quota-hold')
        self.assertEqual(receipt, 'received')
        await until(lambda: bool(self.store.get('codex_account_blocks')))
        self.assertTrue(session.busy)
        self.assertFalse(session.rejected)
        self.assertIsNone(session.process.returncode)
        self.assertEqual((await self.send('finish'))[1], 'received')
        await settle(session.wait_closed())
        self.assertTrue(session.rejected)
        self.assertEqual(self.turns, ['done'])
        self.assertIsNone(self.message(row['id'])['reaction_desired'])
        self.assertFalse(any(row['kind'] == 'callback' for row in self.store.messages_pending()))

    async def test_rate_limit_relaunches_an_idle_session(self):
        session = await self.start()
        session._apply_event({'rate_limits': {'primary': {
            'usedPercent': 100, 'windowDurationMins': 300, 'resetsAt': time.time() + 3600}}})
        await settle(session.wait_closed())
        self.assertTrue(session.rejected)

    async def test_rate_limit_waits_for_unread_reattach_events(self):
        session = await self.start()
        session._unread_reattach_events = 1
        session._apply_event({'rate_limits': {'primary': {
            'usedPercent': 100, 'windowDurationMins': 300, 'resetsAt': time.time() + 3600}}})
        self.assertFalse(session.rejected)
        session._unread_reattach_events = 0
        session._apply_event({'type': 'turn.completed'})
        await settle(session.wait_closed())
        self.assertTrue(session.rejected)

    async def test_reattach_keeps_the_limit_pending_until_the_active_turn_finishes(self):
        session = await self.start()
        await self.send('quota-hold')
        await until(lambda: bool(self.store.get('codex_account_blocks')))
        pid = session.process.pid
        await session.detach()
        session = await self.start()
        self.assertEqual(session.process.pid, pid)
        self.assertFalse(session.rejected)
        self.assertEqual((await self.send('finish'))[1], 'received')
        await settle(session.wait_closed())
        self.assertEqual(self.turns, ['done'])

    async def test_exited_host_replays_completion_and_settles_unreceived_input(self):
        from coordinator import parent
        session = await self.start()
        row, _ = await self.send('hold')
        await until(lambda: self.message(row['id'])['reaction_desired'] == '👀')
        directory, sid = session.process.directory, session.session_id
        await session.detach()
        self.session = None
        await session.process.request('write', line=json.dumps({'id': 900, 'method': 'turn/start', 'params': {
            'threadId': sid, 'input': [{'type': 'text', 'text': 'exit'}]}}))
        from coordinator.host import HostClient
        await until(lambda: HostClient(directory).state == 'exited')
        unknown = self.store.message_save('home', 'owner', 'unreceived')
        self.store.message_sending(unknown['id'])
        self.store.message_delivered(unknown['id'], 'written')
        request = {'id': 901, 'method': 'turn/start', 'params': {
            'clientUserMessageId': message_uuid(unknown['id']), 'threadId': sid,
            'input': [{'type': 'text', 'text': 'unreceived'}]}}
        with (directory / 'writes.jsonl').open('a') as stream:
            stream.write(json.dumps({'seq': 901, 'line': json.dumps(request)}) + '\n')
        session = self.session = await parent.start(self.store, self.runner, self.root, 'ignored', 'policy',
                                                    on_turn=self.turns.append, receipt_timeout=1)
        self.assertEqual(session.process.directory, directory)
        await settle(session.wait_closed())
        self.assertEqual(self.turns, ['done'])
        self.assertIsNone(self.message(row['id'])['reaction_desired'])
        self.assertEqual(self.message(unknown['id'])['receipt'], 'session_closed')

    def test_rollout_scanner_requires_a_user_record_and_toml_maps_use_equals(self):
        path = self.root / 'rollout.jsonl'
        path.write_text('\n'.join(json.dumps(entry) for entry in [
            {'type': 'response_item', 'payload': {'role': 'assistant', 'content': [{'text': 'message=1 kind=owner'}]}},
            {'type': 'response_item', 'payload': {'role': 'user', 'content': [{'text': '[message=2 kind=owner]'}]}},
            {'type': 'event_msg', 'payload': {'type': 'user_message', 'message': '[message=3 kind=owner]'}},
        ]))
        self.assertEqual(codex_transcript_receipts(path, [1, 2, 3, 4]), {2, 3})
        self.assertEqual(toml({'PYTHONPATH': '/tmp'}), '{"PYTHONPATH" = "/tmp"}')

    async def test_service_pauses_on_the_active_account_and_resumes_after_an_owner_pick(self):
        from coordinator.controls import op_account_use
        from coordinator.service import Service
        from tests.test_service import FakeTelegram
        self.store.put('owner', 1)
        self.store.put('group', 1)
        self.store.put('mode', 'group')
        self.store.put('codex_auto_switch', True)
        service = Service(self.store, FakeTelegram(), self.runner, self.root)
        self.session = await service.start_session('home')
        self.assertEqual(self.session.provider, 'codex')
        await self.send('quota')
        await settle(self.session.wait_closed())
        waiting = self.store.message_save('home', 'owner', 'again')
        self.assertIsNone(await service.start_session('home'))
        self.assertIsNone(await service.start_session('home'))
        notices = list(self.store.db.execute("SELECT text FROM outbox WHERE text LIKE 'The main chat is paused:%'"))
        self.assertEqual(len(notices), 1)
        self.assertIn('does not switch ChatGPT accounts on its own', notices[0]['text'])
        other = self.root / 'other'
        other.mkdir()
        self.store.put('codex_accounts', {**self.store.get('codex_accounts'), 'other': {
            'enabled': True, 'config_dir': str(other)}})
        self.store.put('codex_account_status', {**self.store.get('codex_account_status'), 'other': {
            'identity': {'logged_in': True, 'email': 'other@example.test'}}})
        with self.store.db:
            op_account_use(self.store, None, 'other')
        self.assertIsNone(self.store.get('coordinator_account_retry_at'))
        self.assertIsNone(self.store.get('coordinator_account_notice'))
        self.assertTrue(await service.feed_once())
        self.session = service.session
        await asyncio.gather(*service.feed_tasks.values())
        await until(self.session.idle)
        self.assertEqual(self.session.account_alias, 'other')
        self.assertEqual(self.message(waiting['id'])['delivered'], 'received')
