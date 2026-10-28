import asyncio
import hashlib
import json
import os
from pathlib import Path
import tempfile
import sys
import time
import unittest
from unittest.mock import AsyncMock, patch
import uuid

from coordinator.codex_session import CodexCoordinatorSession, codex_transcript_receipts, toml
from coordinator import control_api
from coordinator.cli_identity import identity, PACKAGES
from coordinator.mcp import tools_hash
from coordinator.codex_accounts import CODEX_SKILLS_POINTER
from coordinator.providers import ProviderRunner
from coordinator.session import AccountsUnavailable, message_tag, message_uuid
from coordinator.store import Store
from tests.support import expected_codex_skills_pointer, isolate_shared_mcp_home, read_jsonl, stop_test_hosts, until, settle


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
    async def test_a2_parent_skills_paths_match_each_launch_environment(self):
        other = self.root / 'account Y'
        other.mkdir()
        self.store.put('codex_accounts', {**self.store.get('codex_accounts'), 'Y': {
            'enabled': True, 'config_dir': str(other)}})
        self.store.put('codex_account_status', {**self.store.get('codex_account_status'), 'Y': {
            'identity': {'email': 'y@example.test', 'logged_in': True}}})
        notes = []
        for alias, home in (('gpt', self.profile), ('Y', other)):
            with self.subTest(account=alias):
                self.store.put('codex_active_account', alias)
                session = await self.start()
                try:
                    spec = json.loads((session.process.directory / 'spec.json').read_text())
                    env = spec['env']
                    self.assertEqual(env['CODEX_HOME'], str(home))
                    self.assertEqual(env['HOME'], os.environ['HOME'])
                    request = [item for item in read_jsonl(self.root / 'requests')
                               if item.get('method') in ('thread/start', 'thread/resume')][-1]
                    note = request['params']['developerInstructions'].split('\n## Codex skills\n\n')[1]
                    notes.append(note)
                    self.assertEqual(note, expected_codex_skills_pointer(env['HOME'], home))
                    self.assertNotIn('$CODEX_HOME', note)
                    self.assertNotIn('~/', note)
                    self.assertNotIn('If CODEX_HOME is unset', note)
                    self.assertIn('skills.include_instructions=false', spec['argv'])
                finally:
                    await session.stop()
        self.assertNotEqual(*notes)

    def skills_pointer(self):
        alias = self.store.get('codex_active_account') or 'gpt'
        return expected_codex_skills_pointer(os.environ['HOME'], self.store.get('codex_accounts')[alias]['config_dir'])

    async def test_codex_skills_pointer_is_outside_this_chat_section(self):
        from coordinator.policy import chat_policy
        self.store.put('coordinator_delegation', {'text': 'Research to Sol.\nUI to Claude.', 'revision': 1})
        session = await self.start()
        requests = read_jsonl(self.root / 'requests')
        prompt = next(row['params']['developerInstructions'] for row in requests if row.get('method') == 'thread/start')
        sections = prompt.split('\n## ')
        chat = next(section for section in sections if section.startswith('This chat\n'))
        self.assertIn('Research to Sol.\nUI to Claude.', chat)
        self.assertNotIn(self.skills_pointer(), chat)
        self.assertEqual(prompt.count(self.skills_pointer()), 1)
        self.assertEqual(session.usage_policy, chat_policy(self.store, 'coordinator'))

    async def test_codex_skills_pointer_ends_all_launch_prompts(self):
        baseline = '370f8625e13e9103bd0f20b3ed2ea3a7d1799a130732c33b4c65df0515a3d53b'
        (self.store.directory / 'USAGE.md').write_text('Fixture usage.\n')
        session = await self.start()
        await session.stop()
        (self.root / 'reject-resume').touch()
        session = await self.start()
        await session.stop()
        starts = [row for row in read_jsonl(self.root / 'requests')
                  if row.get('method') in ('thread/start', 'thread/resume')]
        self.assertEqual({row['method'] for row in starts}, {'thread/start', 'thread/resume'})
        self.assertTrue(all(hashlib.sha256(json.dumps(row['params']['developerInstructions'].replace(self.skills_pointer(), CODEX_SKILLS_POINTER)).encode()).hexdigest() == baseline for row in starts))
        self.assertGreaterEqual(len(starts), 2)

    async def test_d3_chat_policy_in_codex_developer_instructions_and_sibling_default(self):
        from coordinator.policy import chat_policy, usage_policy
        self.store.put('coordinator_delegation', {'text': 'Home text.', 'revision': 1})
        self.store.put('coordinator:sibling_delegation', {'text': 'Sibling text.', 'revision': 1})
        session = await self.start()
        starts = [row for row in read_jsonl(self.root / 'requests') if row.get('method') == 'thread/start']
        self.assertEqual(starts[-1]['params']['developerInstructions'],
                         'policy\nCurrent usage policy:\n' + chat_policy(self.store, 'coordinator') + '\n## Codex skills\n\n' + self.skills_pointer())
        await session.stop()
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('plain',1,2,'Plain',?,1)",
                              (str(self.root),))
        self.store.db.commit()
        self.session = await CodexCoordinatorSession.start(self.store, self.runner, self.root, 'ignored',
                                                           'policy', topic='plain', receipt_timeout=1)
        starts = [row for row in read_jsonl(self.root / 'requests') if row.get('method') == 'thread/start']
        self.assertEqual(starts[-1]['params']['developerInstructions'],
                         'policy\nCurrent usage policy:\n' + usage_policy(self.store.directory) + '\n## Codex skills\n\n' + self.skills_pointer())

    def expected_parent_overrides(self, effort='medium'):
        values = {'model_reasoning_effort': effort, 'mcp_servers.torii.command': sys.executable,
                  'mcp_servers.torii.args': ['-m', 'coordinator', '--state-dir', str(self.store.directory),
                                            'mcp', '--topic=home'],
                  'mcp_servers.torii.env': {'PYTHONPATH': str(Path(__file__).resolve().parents[1])},
                  'mcp_servers.torii.required': True, 'mcp_servers.torii.startup_timeout_sec': 30,
                  'mcp_servers.torii.default_tools_approval_mode': 'approve',
                  'mcp_servers.torii.omit_tools_from': ['deferred', 'code_mode'],
                  'skills.include_instructions': False}
        return [part for key, value in values.items() for part in ('-c', key + '=' + toml(value))]

    def expected_parent_fingerprint(self, effort='medium'):
        content = {'tools': tools_hash(), 'model': 'test-codex', 'effort': effort,
                   'instructions': 'policy\nCurrent usage policy:\n' + (self.store.directory / 'USAGE.md').read_text() + '\n## Codex skills\n\n' + self.skills_pointer(),
                   'overrides': self.expected_parent_overrides(effort), 'binary': str(self.binary),
                   'cli': identity(str(self.binary), PACKAGES['codex'])}
        return hashlib.sha256(json.dumps(content, sort_keys=True, separators=(',', ':')).encode()).hexdigest()

    async def test_parent_effort_default_launch_bytes(self):
        await self.start()
        expected = self.expected_parent_overrides() + ['app-server', '--listen', 'stdio://']
        self.assertEqual(json.dumps(read_jsonl(self.root / 'launches')[0]).encode(), json.dumps(expected).encode())
        self.assertEqual(self.store.get('coordinator_host')['fingerprint'], self.expected_parent_fingerprint())

    async def test_parent_gets_full_coordinators_usage_section(self):
        policy = 'Shared.\n## Coordinators\nOwner only.\n## Effort\nKeep.\n'
        (self.store.directory / 'USAGE.md').write_text(policy)
        await self.start()
        request = next(item for item in read_jsonl(self.root / 'requests') if item.get('method') == 'thread/start')
        self.assertEqual(request['params']['developerInstructions'], 'policy\nCurrent usage policy:\n' + policy + '\n## Codex skills\n\n' + self.skills_pointer())

    async def test_codex_skills_fingerprint_includes_absolute_note(self):
        (self.store.directory / 'USAGE.md').write_text('Fixture usage.\n')
        launch = AsyncMock(return_value=False)
        with patch('coordinator.codex_session.codex_binary', return_value='/fixture/codex'), \
                patch.object(CodexCoordinatorSession, 'overrides', return_value=['-c', 'fixture=true']), \
                patch('coordinator.mcp.tools_hash', return_value='fixture-tools'), \
                patch('coordinator.cli_identity.identity', return_value='fixture-cli'), \
                patch('coordinator.parent_state.launch', launch):
            await self.start()
        expected = {'tools': 'fixture-tools', 'model': 'test-codex', 'effort': 'medium',
            'instructions': 'policy\nCurrent usage policy:\nFixture usage.\n\n## Codex skills\n\n' + self.skills_pointer(),
            'overrides': ['-c', 'fixture=true'], 'binary': '/fixture/codex', 'cli': 'fixture-cli'}
        self.assertEqual(launch.await_args.args[2], hashlib.sha256(json.dumps(
            expected, sort_keys=True, separators=(',', ':')).encode()).hexdigest())

    async def test_codex_skills_parent_launch_matches_origin_main(self):
        """Switch-on launch captured from origin/main at d5d0a00eb9dd42547135705bb88dca995b8e5c0b."""
        self.store.put('codex_parent_effort', 'max')
        (self.store.directory / 'USAGE.md').write_text('Fixture usage.\n')
        session = await self.start()
        try:
            command = json.dumps(read_jsonl(self.root / 'launches')[-1])
            for old, new in ((str(self.root), '<worktree>'), (str(Path(__file__).resolve().parents[1]), '<repo>'),
                             (sys.executable, '<python>')):
                command = command.replace(old, new)
            self.assertEqual(hashlib.sha256(command.encode()).hexdigest(), '043c01187b0e4a5c86391fdb7caadbd96a6b380c1497fe923444bbf56eb44b5c')
            requests = read_jsonl(self.root / 'requests')
            prompt = [item['params']['developerInstructions'] for item in requests
                      if item.get('method') in ('thread/start', 'thread/resume')][-1]
            self.assertEqual(hashlib.sha256(json.dumps(prompt.replace(self.skills_pointer(), CODEX_SKILLS_POINTER)).encode()).hexdigest(), '370f8625e13e9103bd0f20b3ed2ea3a7d1799a130732c33b4c65df0515a3d53b')
            self.assertEqual(prompt.count(self.skills_pointer()), 1)
            self.assertTrue(prompt.endswith('\n## Codex skills\n\n' + self.skills_pointer()))
            self.assertEqual(self.store.get('coordinator_host')['fingerprint'], self.expected_parent_fingerprint('xhigh'))
        finally:
            await session.stop()

    async def test_signed_out_accounts_keep_a_healthy_parent(self):
        self.store.put('codex_active_account', 'gpt')
        session = await self.start()
        sid, host, pid = session.session_id, session.host_id, session.process.pid
        fingerprint = self.expected_parent_fingerprint()
        self.assertEqual(self.store.get('coordinator_host')['fingerprint'], fingerprint)
        await session.detach()
        self.store.put('codex_account_status', {'gpt': {
            'identity': {'logged_in': False}, 'error': 'login_required'}})
        self.assertIsNone(session.router.parent_account())
        session = await self.start()
        self.assertFalse(session.outdated)
        self.assertEqual((session.session_id, session.host_id, session.process.pid), (sid, host, pid))
        self.assertEqual(self.store.get('coordinator_host')['fingerprint'], fingerprint)
        self.assertTrue(session.idle())
        self.assertFalse(session.rejected)
        self.assertFalse(session.closed)
        self.assertEqual((await self.send('still healthy'))[1], 'received')
        await until(lambda: len(self.turns) == 1)
        self.assertFalse(session.closed)
        self.assertFalse(session.rejected)
        self.assertEqual((session.session_id, session.host_id, session.process.pid), (sid, host, pid))

    async def test_eligible_account_pick_replaces_parent_on_the_same_native_session(self):
        session = await self.start()
        sid, host = session.session_id, session.host_id
        await session.detach()
        other = self.root / 'other'
        other.mkdir()
        self.store.put('codex_accounts', {**self.store.get('codex_accounts'), 'other': {
            'enabled': True, 'config_dir': str(other)}})
        self.store.put('codex_account_status', {**self.store.get('codex_account_status'), 'other': {
            'identity': {'logged_in': True, 'email': 'other@example.test'}}})
        self.store.put('codex_active_account', 'other')
        session = await self.start()
        self.assertTrue(session.outdated)
        self.assertEqual(session.session_id, sid)
        await settle(session.wait_closed())
        self.assertTrue(session.rejected)
        session = await self.start()
        self.assertEqual(session.account_alias, 'other')
        self.assertEqual(session.session_id, sid)
        self.assertNotEqual(session.host_id, host)
        self.assertEqual(self.store.get('coordinator_host')['fingerprint'], self.expected_parent_fingerprint())
        request = [row for row in read_jsonl(self.root / 'requests')
                   if row.get('method') == 'thread/resume'][-1]
        self.assertEqual(request['params']['threadId'], sid)
        self.assertTrue(request['params']['developerInstructions'].endswith(self.skills_pointer()))

    async def test_eligible_account_keeps_the_same_parent(self):
        session = await self.start()
        sid, host, pid = session.session_id, session.host_id, session.process.pid
        fingerprint = self.expected_parent_fingerprint()
        await session.detach()
        session = await self.start()
        self.assertEqual(session.router.parent_account(), 'gpt')
        self.assertFalse(session.outdated)
        self.assertFalse(session.rejected)
        self.assertFalse(session.closed)
        self.assertEqual((session.session_id, session.host_id, session.process.pid), (sid, host, pid))
        self.assertEqual(self.store.get('coordinator_host')['fingerprint'], fingerprint)

    async def test_signed_out_accounts_without_a_host_cannot_start(self):
        self.store.put('codex_account_status', {'gpt': {
            'identity': {'logged_in': False}, 'error': 'login_required'}})
        self.assertIsNone(self.store.get('coordinator_host'))
        with self.assertRaises(AccountsUnavailable) as raised:
            await self.start()
        self.assertEqual(str(raised.exception), 'No agent account is available')
        self.assertIsNone(self.store.get('coordinator_host'))
        self.assertFalse((self.root / 'launches').exists())

    async def test_signed_out_parent_note_uses_host_account_when_the_pick_differs(self):
        session = await self.start()
        sid, host, pid = session.session_id, session.host_id, session.process.pid
        fingerprint = self.expected_parent_fingerprint()
        self.assertEqual((await self.send('hold'))[1], 'received')
        await until(lambda: session.busy and session.protocol.turn_id is not None)
        await session.detach()
        other = self.root / 'other'
        other.mkdir()
        self.store.put('codex_accounts', {**self.store.get('codex_accounts'), 'other': {
            'enabled': True, 'config_dir': str(other)}})
        self.store.put('codex_active_account', 'other')
        self.store.put('codex_account_status', {alias: {
            'identity': {'logged_in': False}, 'error': 'login_required'} for alias in ('gpt', 'other')})
        session = await self.start()
        self.assertIsNone(session.router.parent_account())
        self.assertEqual(session.router.active(), 'other')
        self.assertFalse(session.outdated)
        self.assertTrue(session.busy)
        self.assertFalse(session.rejected)
        self.assertEqual((session.session_id, session.host_id, session.process.pid), (sid, host, pid))
        self.assertEqual(self.store.get('coordinator_host')['fingerprint'], fingerprint)

    async def test_parent_effort_levels_reach_launch_options_and_clear_restores_default(self):
        for effort, native in (('max', 'xhigh'), ('low', 'low'), ('medium', 'medium'),
                               ('high', 'high'), (None, 'medium')):
            with self.subTest(effort=effort):
                result = control_api.call(self.store, 'effort.parent', {'provider': 'codex', 'effort': effort})
                self.assertTrue(result.ok, result.text)
                session = await self.start()
                expected = self.expected_parent_overrides(native) + ['app-server', '--listen', 'stdio://']
                self.assertEqual(json.dumps(read_jsonl(self.root / 'launches')[-1]).encode(),
                                 json.dumps(expected).encode())
                self.assertEqual(self.store.get('coordinator_host')['fingerprint'],
                                 self.expected_parent_fingerprint(native))
                await session.stop()

    async def test_claude_parent_effort_leaves_codex_launch_unchanged(self):
        before = CodexCoordinatorSession.overrides(self.store)
        self.assertTrue(control_api.call(self.store, 'effort.parent', {'provider': 'claude', 'effort': 'low'}).ok)
        self.assertEqual(CodexCoordinatorSession.overrides(self.store), before)
        await self.start()
        self.assertEqual(read_jsonl(self.root / 'launches')[0],
                         self.expected_parent_overrides() + ['app-server', '--listen', 'stdio://'])
        self.assertEqual(self.store.get('coordinator_host')['fingerprint'], self.expected_parent_fingerprint())

    async def test_parent_effort_changed_fingerprint_resumes_same_native_session(self):
        session = await self.start()
        sid, host = session.session_id, session.host_id
        await session.detach()
        self.assertTrue(control_api.call(self.store, 'effort.parent', {'provider': 'codex', 'effort': 'high'}).ok)
        session = await self.start()
        self.assertTrue(session.outdated)
        await settle(session.wait_closed())
        session = await self.start()
        self.assertEqual(session.session_id, sid)
        self.assertNotEqual(session.host_id, host)
        self.assertEqual(self.store.get('coordinator_host')['fingerprint'], self.expected_parent_fingerprint('high'))
        requests = read_jsonl(self.root / 'requests')
        resumed = [request for request in requests if request.get('method') == 'thread/resume']
        self.assertEqual(resumed[-1]['params']['threadId'], sid)

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
        argv = read_jsonl(self.root / 'launches')[0]
        self.assertIn('mcp_servers.torii.default_tools_approval_mode="approve"', argv)
        self.assertIn('mcp_servers.torii.omit_tools_from=["deferred", "code_mode"]', argv)
        self.assertIn('model_reasoning_effort="medium"', argv)

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
        requests = read_jsonl(self.root / 'requests')
        self.assertEqual(sum(request.get('method') == 'initialize' for request in requests), 1)
        self.assertEqual(sum(request.get('method') == 'thread/start' for request in requests), 1)
        self.assertEqual(sum(request.get('method') == 'turn/start' for request in requests), 2)
        self.assertEqual(codex_transcript_receipts(session.transcript_path, [row['id']]), {row['id']})

    def assert_queued_reply_cursor(self, tool_reply=False, previous_host=None, batching=True, persisted=False):
        from coordinator import parent_replies, reactions
        self.store.put('coordinator_intake_batching', batching)
        session = CodexCoordinatorSession(self.store, self.runner, None, 1, 'coordinator')
        session.host_id = 'queued-host'
        previous = CodexCoordinatorSession(self.store, self.runner, None, 1, 'coordinator')
        previous.host_id = previous_host or session.host_id
        owners = [self.store.message_save('home', 'owner', text, telegram_message=number)
                  for number, text in ((701, 'Previous owner'), (702, 'Queued owner'))]
        for row in owners:
            if persisted and row['id'] == owners[1]['id']:
                self.assertTrue(self.store.message_batch_sending([row['id']]))
            else:
                self.store.message_sending(row['id'])
            with self.store.db:
                reactions.handed_off(self.store, row['id'])
            self.store.messages_confirm([row['id']], 'replayed')
        cursor = parent_replies.outbox_cursor(self.store, 'home')
        parent_replies.started(previous, 'old-turn', {owners[0]['id']}, cursor)
        parent_replies.completed(previous, 'old-turn', {owners[0]['id']}, 'Previous answer. ' * 400)
        previous_outputs = list(self.store.db.execute('SELECT * FROM outbox ORDER BY id'))
        self.assertGreater(len(previous_outputs), 1)
        if tool_reply:
            with self.store.db:
                self.store.enqueue_report('home', 'Owner-directed tool answer', reply_to=702)
        session._reply_started('new-turn', cursor, {owners[1]['id']})
        session._reply_started('new-turn', rows={owners[1]['id']})
        parent_replies.completed(session, 'new-turn', {owners[1]['id']}, 'Next owner answer')
        output = [dict(row) for row in self.store.db.execute('SELECT * FROM outbox WHERE reply_to=702 ORDER BY id')]
        if tool_reply:
            self.assertEqual([row['text'] for row in output], ['Owner-directed tool answer'])
        elif previous_host is not None or not batching and not persisted:
            self.assertEqual(output, [])
        else:
            self.assertEqual([row['text'] for row in output], ['Next owner answer'])

    def test_queued_native_start_keeps_its_reply_after_previous_split_automatic_answer(self):
        self.assert_queued_reply_cursor()

    def test_queued_native_start_keeps_owner_directed_tool_output_as_duplicate_guard(self):
        self.assert_queued_reply_cursor(tool_reply=True)

    def test_queued_native_start_does_not_ignore_another_hosts_output(self):
        self.assert_queued_reply_cursor(previous_host='another-host')

    def test_legacy_native_start_keeps_fifo_reply_guard_while_batching_is_off(self):
        self.assert_queued_reply_cursor(batching=False)

    def test_saved_batch_native_start_corrects_reply_cursor_while_batching_is_off(self):
        self.assert_queued_reply_cursor(batching=False, persisted=True)

    async def test_batch_reattach_keeps_every_owner_on_the_same_native_turn(self):
        session = await self.start()
        rows = [self.store.message_save('home', 'owner', text, telegram_message=number)
                for number, text in ((601, 'first hold'), (602, 'second hold'))]
        ids = {row['id'] for row in rows}
        anchor = rows[-1]['id']
        self.assertTrue(self.store.message_batch_sending([anchor, rows[0]['id']]))
        prompt = '\n\n'.join(message_tag(self.store.topic('home'), row) + row['text'] for row in reversed(rows))
        self.assertEqual(await session.send(message_uuid(anchor), prompt, row_id=anchor), 'received')
        await until(lambda: all(self.message(row)['reaction_desired'] == '👀' for row in ids))
        self.assertEqual({self.message(row)['receipt'] for row in ids}, {'replayed'})
        pid = session.process.pid
        await session.detach()
        session = await self.start()
        self.assertEqual(session.process.pid, pid)
        self.assertEqual(self.store.get(session.key + '_turn'), sorted(ids))
        self.assertEqual((await self.send('finish'))[1], 'received')
        await until(session.idle)
        self.assertEqual([(self.message(row)['reaction_desired'], self.message(row)['turn_started'])
                          for row in sorted(ids)], [(None, None), (None, None)])
        requests = read_jsonl(self.root / 'requests')
        self.assertEqual(sum(request.get('params', {}).get('clientUserMessageId') == message_uuid(anchor)
                             for request in requests), 1)
        self.assertEqual(codex_transcript_receipts(session.transcript_path, ids), ids)

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
        requests = read_jsonl(self.root / 'requests')
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

    async def test_cli_change_marks_attached_parent_outdated_and_keeps_thread(self):
        session = await self.start()
        original = self.store.get('coordinator_host')['fingerprint']
        sid = session.session_id
        await session.detach()
        self.binary.write_text(APP_SERVER + '\n')
        session = await self.start()
        self.assertTrue(session.outdated)
        self.assertEqual(session.session_id, sid)
        await settle(session.wait_closed())
        session = await self.start()
        self.assertEqual(session.session_id, sid)
        self.assertNotEqual(self.store.get('coordinator_host')['fingerprint'], original)
        await session.detach()
        session = await self.start()
        self.assertFalse(session.outdated)

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
        requests = read_jsonl(self.root / 'requests')
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
        requests = read_jsonl(self.root / 'requests')
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
        session._rows[wire] = {row['id']}
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
        session._rows[wire] = {row['id']}
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
