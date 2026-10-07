import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch
import uuid

from coordinator.host import HostClient
from coordinator.providers import ProviderRunner
from coordinator.service import Service
from coordinator.session import (LOST_TURN, RESTART_MESSAGE_CHARS, RESTART_MESSAGE_HEAD, RESTART_MESSAGES,
                                 CoordinatorSession, message_tag, message_uuid, transcript_receipts)
from coordinator.store import Store
from tests.support import restart_work_rows, stop_test_hosts


NEXT_QUOTA = '[topic=home name="Home" message=2 kind=owner] next-quota'


def lost_note(message, *rows):
    return '[topic=home name="Home" message=%d kind=callback] ' % message + LOST_TURN % ', '.join(
        'message=%d' % row for row in rows)


class CoordinatorSessionTestsSupport:
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(stop_test_hosts, self.root)
        home = self.root / 'torii-home'
        home.mkdir()
        (home / '.claude.json').write_text(json.dumps({'mcpServers': {
            name: {'type': 'http', 'url': 'http://127.0.0.1:1/' + name, 'headers': {}}
            for name in ('example-mcp', 'other-mcp')}}))
        environment = patch.dict(os.environ, {'HOME': str(home)})
        environment.start()
        self.addCleanup(environment.stop)
        overrides = self.root / 'local-overrides.json'
        overrides.write_text(json.dumps({'shared_mcp_servers': ['example-mcp', 'other-mcp']}))
        selection = patch('coordinator.accounts.LOCAL_OVERRIDES_PATH', overrides)
        selection.start()
        self.addCleanup(selection.stop)
        self.binary = self.root / 'claude'
        self.binary.write_text('''#!/usr/bin/env python3
import json,os,sys,time
from pathlib import Path

root=Path(__file__).parent
sid=sys.argv[sys.argv.index('--session-id')+1] if '--session-id' in sys.argv else sys.argv[sys.argv.index('--resume')+1]
sid=Path(sid).stem if '/' in sid else sid
profile=os.environ.get('CLAUDE_CONFIG_DIR')
resume=sys.argv[sys.argv.index('--resume')+1] if '--resume' in sys.argv else None
transcript=Path(resume) if resume and '/' in resume else (Path(profile)/'projects'/'test'/(sid+'.jsonl') if profile else None)
if profile and ('--session-id' in sys.argv or '/' not in resume):
    transcript.parent.mkdir(parents=True,exist_ok=True)
    transcript.touch()
seen=set(json.loads(line)['uuid'] for line in transcript.read_text().splitlines() if line.strip()) if transcript and transcript.exists() else set()
with (root/'launches.jsonl').open('a') as log:
    log.write(json.dumps({'argv':sys.argv[1:],'pid':os.getpid(),'profile':profile})+'\\n')
if '--resume' in sys.argv and (root/'resume-fails').exists():
    sys.stdin.readline()
    time.sleep(.5)
    sys.exit(1)
if '--resume' in sys.argv and (root/'resume-crashes').exists():
    for line in sys.stdin:
        if line.strip()=='release':
            (root/'release-echo').touch()
            break
    sys.exit(1)
print(json.dumps({'type':'system','subtype':'init','session_id':sid}),flush=True)
commands=set()
def begin(event):
    commands.add(event['uuid'])
    print(json.dumps({'type':'command_lifecycle','command_uuid':event['uuid'],'state':'queued','session_id':sid}),flush=True)
    print(json.dumps({'type':'command_lifecycle','command_uuid':event['uuid'],'state':'started','session_id':sid}),flush=True)
    print(json.dumps({**event,'isReplay':True}),flush=True)
    print(json.dumps({'type':'tool_progress','tool_use_id':'normal-tool','elapsed_time_seconds':1}),flush=True)
def complete():
    if (root/'pause-completion').exists():
        (root/'completion.ready').touch()
        while not (root/'completion.release').exists():
            time.sleep(.02)
    for wire in commands:
        print(json.dumps({'type':'command_lifecycle','command_uuid':wire,'state':'completed','session_id':sid}),flush=True)
    commands.clear()
for line in sys.stdin:
    event=json.loads(line)
    if event.get('type')=='control_response':
        continue
    with (root/'inputs.jsonl').open('a') as log:
        log.write(json.dumps({**event,'profile':profile})+'\\n')
    if event.get('uuid') in seen:
        print(json.dumps({**event,'isReplay':True}),flush=True)
        print(json.dumps({'type':'command_lifecycle','command_uuid':event['uuid'],'state':'completed','session_id':sid}),flush=True)
        continue
    seen.add(event.get('uuid'))
    if transcript:
        with transcript.open('a') as log:
            log.write(json.dumps({'type':'user','uuid':event.get('uuid')})+'\\n')
    text=event['message']['content']
    if text.endswith('reject-everywhere'):
        (root/'reject-all').touch()
    if text.endswith('next-quota') and profile and profile.endswith('/a'):
        print(json.dumps({'type':'result','subtype':'success','session_id':sid,'result':'done:hold'}),flush=True)
        print(json.dumps({'type':'rate_limit_event','rate_limit_info':{'status':'rejected','resetsAt':time.time()+3600}}),flush=True)
        print(json.dumps({**event,'isReplay':True}),flush=True)
        print(json.dumps({'type':'result','subtype':'error','session_id':sid,'is_error':True,'result':'out of credits'}),flush=True)
        continue
    if text=='die':
        sys.exit(3)
    if text=='bg':
        print(json.dumps({'type':'system','subtype':'background_tasks_changed','session_id':sid,
                          'tasks':[{'id':'b1','status':'running'}]}),flush=True)
    if text=='bg-done':
        begin(event)
        time.sleep(.3)
        print(json.dumps({'type':'system','subtype':'background_tasks_changed','session_id':sid,'tasks':[]}),flush=True)
        print(json.dumps({'type':'system','subtype':'task_notification','session_id':sid,'task_id':'bg-1','status':'completed'}),flush=True)
        print(json.dumps({'type':'result','subtype':'success','session_id':sid,'result':'done:bg-done'}),flush=True)
        complete()
        time.sleep(.3)
        print(json.dumps({'type':'system','subtype':'status','session_id':sid}),flush=True)
        continue
    if text=='cmds':
        begin(event)
        print(json.dumps({'type':'result','subtype':'success','session_id':sid,'result':'done:cmds'}),flush=True)
        complete()
        time.sleep(.3)
        print(json.dumps({'type':'system','subtype':'commands_changed','session_id':sid,'commands':[]}),flush=True)
        continue
    if text=='no-echo':
        (root/'no-echo.started').touch()
        continue
    if text=='span-reject':
        print(json.dumps({**event,'isReplay':True}),flush=True)
        (root/'span.started').touch()
        while not (root/'span.go').exists():
            time.sleep(.02)
        print(json.dumps({'type':'rate_limit_event','rate_limit_info':{'status':'rejected','resetsAt':time.time()+3600}}),flush=True)
        print(json.dumps({'type':'result','subtype':'error','session_id':sid,'is_error':True,'result':"You're out of usage credits"}),flush=True)
        continue
    if text=='life-hold':
        held=event['uuid']
        print(json.dumps({'type':'command_lifecycle','command_uuid':held,'state':'queued','session_id':sid}),flush=True)
        print(json.dumps({'type':'command_lifecycle','command_uuid':held,'state':'started','session_id':sid}),flush=True)
        print(json.dumps({**event,'isReplay':True}),flush=True)
        print(json.dumps({'type':'tool_progress','tool_use_id':'life-tool','elapsed_time_seconds':1}),flush=True)
        (root/'life-hold.started').touch()
        continue
    if text=='life-next':
        for line in ({'type':'command_lifecycle','command_uuid':event['uuid'],'state':'queued','session_id':sid},
                     {'type':'result','subtype':'success','session_id':sid,'result':'done:life-hold'},
                     {'type':'command_lifecycle','command_uuid':held,'state':'completed','session_id':sid},
                     {'type':'command_lifecycle','command_uuid':event['uuid'],'state':'started','session_id':sid},
                     {**event,'isReplay':True},
                     {'type':'result','subtype':'success','session_id':sid,'result':'done:life-next'},
                     {'type':'command_lifecycle','command_uuid':event['uuid'],'state':'completed','session_id':sid}):
            time.sleep(.05)
            print(json.dumps(line),flush=True)
        continue
    if (text.endswith('quota') and profile and profile.endswith('/a')) or (root/'reject-all').exists():
        print(json.dumps({'type':'rate_limit_event','rate_limit_info':{'status':'rejected','resetsAt':time.time()+3600}}),flush=True)
        print(json.dumps({**event,'isReplay':True}),flush=True)
        print(json.dumps({'type':'result','subtype':'error','session_id':sid,'is_error':True,'result':"You're out of usage credits"}),flush=True)
        continue
    begin(event)
    if text=='bad-line':
        print('not json',flush=True)
        (root/'bad-line.ready').touch()
        while not (root/'bad-line.release').exists():
            time.sleep(.02)
    if text=='threshold':
        print(json.dumps({'type':'rate_limit_event','session_id':sid,'rate_limit_info':
            {'status':'allowed_warning','rateLimitType':'seven_day','utilization':.95,'resetsAt':time.time()+3600}}),flush=True)
        print(json.dumps({'type':'result','subtype':'success','session_id':sid,'result':'done:threshold'}),flush=True)
        complete()
        continue
    if text=='slow-error':
        print(json.dumps({'type':'assistant','session_id':sid,'message':{'content':[
            {'type':'tool_use','id':'read','name':'Read','input':{}},
            {'type':'tool_use','id':'search','name':'WebSearch','input':{}}]}}),flush=True)
        (root/'slow.started').touch()
        while not (root/'slow.go').exists():
            time.sleep(.02)
        print(json.dumps({'type':'result','subtype':'error_during_execution','session_id':sid,
                          'is_error':True,'result':'API Error: 500'}),flush=True)
        complete()
        continue
    if text=='slow':
        print(json.dumps({'type':'assistant','session_id':sid,'message':{'content':[
            {'type':'tool_use','id':'read','name':'Read','input':{}},
            {'type':'tool_use','id':'search','name':'WebSearch','input':{}}]}}),flush=True)
        (root/'slow.started').touch()
        while not (root/'slow.go').exists():
            time.sleep(.02)
        print(json.dumps({'type':'assistant','session_id':sid,'message':{'role':'assistant','content':[]}}),flush=True)
        time.sleep(.3)
        print(json.dumps({'type':'result','subtype':'success','session_id':sid,'result':'done:slow'}),flush=True)
        complete()
        continue
    if text=='hold':
        (root/'hold.started').touch()
        continue
    if text=='follow' and (root/'hold.started').exists():
        print(json.dumps({'type':'result','subtype':'success','session_id':sid,'result':'done:hold'}),flush=True)
    print(json.dumps({'type':'result','subtype':'success','session_id':sid,'result':'done:'+text}),flush=True)
    complete()
''')
        self.binary.chmod(0o700)
        self.store = Store(self.root / 'state')
        profile = self.root / 'claude-profile'
        profile.mkdir()
        with self.store.db:
            self.store.put('accounts', {'test': {'config_dir': str(profile), 'enabled': True}})
            self.store.put('account_status', {'test': {'identity': {'email': 'test@example.com', 'logged_in': True},
                'observed_at': time.time(), 'usage': {'five_hour': {'utilization': 10},
                'seven_day': {'utilization': 10, 'resets_at': time.time() + 3600}}}})
        self.runner = ProviderRunner(self.store.directory, {'claude': str(self.binary)})
        self.sessions = []
        self.turns = []

    async def asyncTearDown(self):
        if hasattr(self, 'service'):
            for task in self.service.feed_tasks.values():
                task.cancel()
            if self.service.feed_tasks:
                await asyncio.gather(*self.service.feed_tasks.values(), return_exceptions=True)
            if self.service.session:
                await self.service.session.stop()
        for session in self.sessions:
            await session.stop()
        self.store.close()

    async def start(self, **options):
        model = options.pop('model', 'model')
        instructions = options.pop('instructions', 'instructions')
        session = await CoordinatorSession.start(self.store, self.runner, self.root, model,
                                                  instructions, on_turn=self.turns.append, **options)
        self.sessions.append(session)
        return session

    async def reattach(self, session, **options):
        await session.detach()
        self.sessions.remove(session)
        return await self.start(**options)

    async def finished_parent(self, **options):
        session = await self.start(**options)
        self.assertEqual(await session.send(str(uuid.uuid4()), 'one'), 'received')
        await self.until(lambda: self.turns == ['done:one'])
        return session

    async def assert_changed_input_relaunches(self, first, **options):
        old_host = first.host_id
        old_fingerprint = self.store.get('coordinator_host')['fingerprint']
        attached = await self.reattach(first, **options)
        self.assertEqual(attached.host_id, old_host)
        self.assertTrue(attached.outdated)
        self.assertTrue(attached.movable())
        await attached.wait_closed()
        replacement = await self.start(**options)
        self.assertNotEqual(replacement.host_id, old_host)
        new_record = self.store.get('coordinator_host')
        self.assertNotEqual(new_record['fingerprint'], old_fingerprint)
        self.assertEqual(new_record['id'], replacement.host_id)
        again = await self.reattach(replacement, **options)
        self.assertEqual(again.host_id, replacement.host_id)
        self.assertFalse(again.outdated)
        self.assertFalse(again.movable())

    async def queued_command_survives_restart(self, kind, *, completed=False):
        source = self.binary.read_text()
        before = '            print(json.dumps(line),flush=True)\n        continue'
        after = '''            print(json.dumps(line),flush=True)
            if line.get('result') == 'done:life-hold':
                (root/'queued.pause').touch()
                while not (root/'queued.release').exists():
                    time.sleep(.02)
        continue'''
        if completed:
            after = after.replace("line.get('result') == 'done:life-hold'",
                                  "line.get('state') == 'completed' and line.get('command_uuid') == held")
        self.assertEqual(source.count(before), 1)
        self.binary.write_text(source.replace(before, after))
        with self.store.db:
            self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                                  ('home', -100, 1, 'Home', str(self.root)))
        first = await self.start(topic='home')
        for text, row_kind in [('life-hold', 'owner'), ('life-next', kind)]:
            row = self.store.message_save('home', row_kind, text)
            self.store.message_sending(row['id'])
            self.assertEqual(await first.send(message_uuid(row['id']), text, row_id=row['id']), 'received')
            self.store.message_delivered(row['id'], 'written')
            if text == 'life-hold':
                await self.until(lambda: (self.root / 'life-hold.started').exists())
        await self.until(lambda: (self.root / 'queued.pause').exists() and self.turns == ['done:life-hold'])
        self.assertTrue(first.protocol.pending)
        self.assertTrue(first._unreplayed)
        self.assertFalse(first.idle())
        self.assertEqual(first._turn_rows, set())
        with self.store.db:
            host = self.store.get('coordinator_host')
            host.pop('fingerprint', None)
            self.store.put('coordinator_host', host)
        self.store.messages_uncertain()
        attached = await self.reattach(first, topic='home')
        service = Service(self.store, None, self.runner, self.root, coordinator_model='model')
        service.confirm_from_transcript('home', attached)
        if attached.rejected:
            await asyncio.wait_for(attached.wait_closed(), 5)
            replacement = await self.start(topic='home')
            await replacement.send(str(uuid.uuid4()), 'wake')
            await self.until(lambda: 'done:wake' in self.turns)
        else:
            (self.root / 'queued.release').touch()
            await self.until(lambda: 'done:life-next' in self.turns)
        self.assertIn('done:life-next', self.turns)
        return attached

    async def until(self, predicate):
        async def wait():
            while not predicate():
                await asyncio.sleep(.01)
        await asyncio.wait_for(wait(), 5)

    def launches(self):
        path = self.root / 'launches.jsonl'
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def transcript(self, session):
        return str((self.root / 'claude-profile' / 'projects' / 'test' / (session.session_id + '.jsonl')).resolve())

    def callbacks(self):
        return [row[0] for row in self.store.db.execute("SELECT text FROM messages WHERE kind='callback' ORDER BY id")]

    def inputs(self):
        return [json.loads(line) for line in (self.root / 'inputs.jsonl').read_text().splitlines()]

    def account_service(self):
        a = self.root / 'a'
        b = self.root / 'b'
        a.mkdir()
        b.mkdir()
        with self.store.db:
            self.store.put('accounts', {'a': {'config_dir': str(a), 'enabled': True},
                                        'b': {'config_dir': str(b), 'enabled': True}})
            now = time.time()
            self.store.put('account_status', {alias: {'identity': {'email': alias + '@example.com', 'logged_in': True},
                'observed_at': now, 'usage': {'five_hour': {'utilization': 10},
                'seven_day': {'utilization': 10, 'resets_at': now + (3600 if alias == 'a' else 7200)}}}
                for alias in ('a', 'b')})
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', ('home', -100, 1, 'Home', str(self.root)))
        self.store.put('coordinator_home_topic', 'home')
        self.service = Service(self.store, None, self.runner, self.root, coordinator_model='model')
        return self.service

    async def feed_until(self, predicate):
        async def drive():
            while not predicate():
                await self.service.feed_once()
                await asyncio.sleep(.02)
        await asyncio.wait_for(drive(), 20)

    def owner_rows(self, *texts):
        with self.store.db:
            self.store.db.execute('''INSERT OR IGNORE INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES ('home',-100,1,'Home',?,1)''', (str(self.root),))
        return [self.store.message_save('home', 'owner', text)['id'] for text in texts]

    def delivery(self, row):
        return tuple(self.store.db.execute('SELECT delivered,receipt FROM messages WHERE id=?', (row,)).fetchone())

    async def deliver(self, session, row, text):
        self.store.message_sending(row)
        receipt = await session.send(message_uuid(row), text, row_id=row)
        self.store.message_delivered(row, receipt)
        return receipt

    def problems(self):
        return [(row['area'], row['code'], row['detail']) for row in self.store.db.execute(
            'SELECT area,code,detail FROM problems ORDER BY id')]


class CoordinatorSessionTests(CoordinatorSessionTestsSupport, unittest.IsolatedAsyncioTestCase):
    async def test_tool_description_change_relaunches_idle_parent_once(self):
        from dataclasses import replace
        from coordinator import mcp
        first = await self.finished_parent()
        native_id = first.session_id
        op = mcp.TOOLS['telegram_send']
        with patch.dict(mcp.TOOLS, {'telegram_send': replace(op, description=op.description + ' Updated.')}):
            await self.assert_changed_input_relaunches(first)
        self.assertEqual(self.sessions[-1].session_id, native_id)
        await self.until(lambda: len(self.launches()) == 2)
        self.assertEqual(len(self.launches()), 2)

    async def test_native_handoff_and_result_drive_tool_reactions(self):
        self.account_service()
        message = self.store.message_save('home', 'owner', 'slow', telegram_message=501)
        session = await self.start(topic='home')
        self.assertIsNone(message['turn_started'])
        self.assertEqual(await session.send(message_uuid(message['id']), 'slow', row_id=message['id']), 'received')
        await self.until(lambda: (self.root / 'slow.started').exists())
        await self.until(lambda: self.store.db.execute('SELECT turn_tools FROM messages WHERE id=?',
                                                       (message['id'],)).fetchone()[0] == 2)
        row = self.store.db.execute('SELECT turn_started FROM messages WHERE id=?', (message['id'],)).fetchone()
        self.assertIsNotNone(row['turn_started'])
        self.assertEqual(self.store.db.execute('SELECT reaction_desired FROM messages WHERE id=?',
                                              (message['id'],)).fetchone()[0], '👀')
        (self.root / 'slow.go').touch()
        await self.until(lambda: self.turns == ['done:slow'])
        row = self.store.db.execute('SELECT reaction_desired,turn_started FROM messages WHERE id=?',
                                     (message['id'],)).fetchone()
        self.assertEqual(tuple(row), (None, None))

    async def test_native_error_clears_tool_reaction_without_a_reply(self):
        from coordinator.reactions import deliver_reaction
        self.account_service()
        message = self.store.message_save('home', 'owner', 'slow-error', telegram_message=501)
        session = await self.start(topic='home')
        self.assertEqual(await session.send(message_uuid(message['id']), 'slow-error', row_id=message['id']), 'received')
        await self.until(lambda: (self.root / 'slow.started').exists())
        await self.until(lambda: self.store.db.execute('SELECT turn_tools FROM messages WHERE id=?',
                                                       (message['id'],)).fetchone()[0] == 2)
        telegram = AsyncMock()
        await deliver_reaction(self.store, telegram)
        telegram.call.assert_awaited_with('setMessageReaction', chat_id=-100, message_id=501,
                                         reaction=[{'type': 'emoji', 'emoji': '👀'}])
        (self.root / 'slow.go').touch()
        await self.until(lambda: self.turns == ['API Error: 500'])
        row = self.store.db.execute('SELECT reaction_desired,turn_started,turn_eyes FROM messages WHERE id=?',
                                    (message['id'],)).fetchone()
        self.assertEqual(tuple(row), (None, None, 0))
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], 0)
        await deliver_reaction(self.store, telegram)
        telegram.call.assert_awaited_with('setMessageReaction', chat_id=-100, message_id=501, reaction=[])

    async def test_native_quick_result_clears_handoff_without_a_reaction(self):
        self.account_service()
        message = self.store.message_save('home', 'owner', 'hello', telegram_message=501)
        session = await self.start(topic='home')
        self.assertEqual(await session.send(message_uuid(message['id']), 'hello', row_id=message['id']), 'received')
        await self.until(lambda: self.turns == ['done:hello'])
        row = self.store.db.execute('SELECT reaction_desired,turn_started FROM messages WHERE id=?',
                                     (message['id'],)).fetchone()
        self.assertEqual(tuple(row), (None, None))

    async def test_reattach_preserves_replayed_turn_after_an_earlier_result(self):
        from coordinator.reactions import deliver_reaction
        self.account_service()
        first = await self.finished_parent(topic='home')
        row = self.store.message_save('home', 'owner', 'slow', telegram_message=501)['id']
        self.store.message_sending(row)
        self.assertEqual(await first.send(message_uuid(row), 'slow', row_id=row), 'received')
        await self.until(lambda: (self.root / 'slow.started').exists())
        await self.until(lambda: self.store.db.execute('SELECT receipt FROM messages WHERE id=?',
                                                       (row,)).fetchone()[0] == 'replayed')
        message = self.store.db.execute('SELECT * FROM messages WHERE id=?', (row,)).fetchone()
        self.assertEqual((message['delivered'], message['receipt']), ('received', 'replayed'))
        await self.until(lambda: self.store.db.execute('SELECT turn_tools FROM messages WHERE id=?',
                                                       (row,)).fetchone()[0] == 2)
        telegram = AsyncMock()
        await deliver_reaction(self.store, telegram)
        telegram.call.assert_awaited_with('setMessageReaction', chat_id=-100, message_id=501,
                                         reaction=[{'type': 'emoji', 'emoji': '👀'}])
        attached = await self.reattach(first, topic='home')
        self.assertEqual(attached.host_id, first.host_id)
        self.assertEqual(attached._turn_rows, {row})
        (self.root / 'slow.go').touch()
        await self.until(lambda: self.turns == ['done:one', 'done:slow'])
        message = self.store.db.execute('SELECT reaction_desired,turn_started,turn_eyes FROM messages WHERE id=?',
                                        (row,)).fetchone()
        self.assertEqual(tuple(message), (None, None, 0))
        await deliver_reaction(self.store, telegram)
        telegram.call.assert_awaited_with('setMessageReaction', chat_id=-100, message_id=501, reaction=[])

    async def test_launch_stores_fingerprint_and_unchanged_reattach_stays_on_host(self):
        first = await self.finished_parent()
        fingerprint = self.store.get('coordinator_host')['fingerprint']
        self.assertEqual(len(fingerprint), 64)
        attached = await self.reattach(first)
        self.assertEqual(attached.host_id, first.host_id)
        self.assertFalse(attached.outdated)
        self.assertFalse(attached.movable())
        self.assertEqual(len(self.launches()), 1)

    async def test_memory_setting_change_relaunches_idle_parent_once(self):
        first = await self.finished_parent()
        memory = self.store.directory / 'coordinator-memory'
        memory.rmdir()
        target = self.root / 'shared-memory'
        target.mkdir()
        memory.symlink_to(target, target_is_directory=True)
        await self.assert_changed_input_relaunches(first)

    async def test_shared_mcp_config_change_relaunches_idle_parent_once(self):
        first = await self.finished_parent()
        source = Path.home() / '.claude.json'
        document = json.loads(source.read_text())
        document['mcpServers']['example-mcp']['url'] = 'http://127.0.0.1:2/example-mcp'
        source.write_text(json.dumps(document))
        await self.assert_changed_input_relaunches(first)

    async def test_coordinator_mcp_content_change_relaunches_idle_parent_once(self):
        config = self.root / 'mcp.json'
        config.write_text('{"mcpServers":{}}')
        first = await self.finished_parent(mcp_config=config)
        config.write_text('{"mcpServers":{"changed":{}}}')
        await self.assert_changed_input_relaunches(first, mcp_config=config)

    async def test_instruction_change_relaunches_idle_parent_once(self):
        first = await self.finished_parent()
        await self.assert_changed_input_relaunches(first, instructions='changed instructions')

    async def test_parent_flags_change_launch_fingerprint(self):
        first = await self.finished_parent()
        original_fingerprint = self.store.get('coordinator_host')['fingerprint']
        with patch('coordinator.session.PARENT_FLAGS', ('--system-prompt-snapshot', 'on')):
            await self.assert_changed_input_relaunches(first)
        self.assertNotEqual(self.store.get('coordinator_host')['fingerprint'], original_fingerprint)

    async def test_model_change_relaunches_idle_parent_once(self):
        first = await self.finished_parent()
        await self.assert_changed_input_relaunches(first, model='changed-model')

    async def test_host_without_fingerprint_relaunches_when_idle(self):
        first = await self.finished_parent()
        record = self.store.get('coordinator_host')
        record.pop('fingerprint')
        with self.store.db:
            self.store.put('coordinator_host', record)
        attached = await self.reattach(first)
        self.assertTrue(attached.outdated)
        self.assertTrue(attached.movable())
        await attached.wait_closed()
        replacement = await self.start()
        self.assertIn('fingerprint', self.store.get('coordinator_host'))
        self.assertNotEqual(replacement.host_id, first.host_id)

    async def test_usage_policy_change_does_not_relaunch_parent(self):
        first = await self.finished_parent()
        fingerprint = self.store.get('coordinator_host')['fingerprint']
        (self.store.directory / 'USAGE.md').write_text('Use one worker.')
        attached = await self.reattach(first)
        self.assertFalse(attached.outdated)
        self.assertFalse(attached.movable())
        self.assertEqual(self.store.get('coordinator_host')['fingerprint'], fingerprint)
        self.assertEqual(len(self.launches()), 1)

    async def test_changed_input_waits_for_a_running_turn_before_relaunch(self):
        first = await self.start()
        self.assertEqual(await first.send(str(uuid.uuid4()), 'slow'), 'received')
        await self.until(lambda: (self.root / 'slow.started').exists() and first.busy)
        attached = await self.reattach(first, instructions='changed instructions')
        self.assertTrue(attached.outdated)
        self.assertTrue(attached.movable())
        self.assertFalse(attached.rejected)
        self.assertEqual(attached.host_id, first.host_id)
        (self.root / 'slow.go').touch()
        await self.until(lambda: self.turns == ['done:slow'])
        await attached.wait_closed()
        self.assertTrue(attached.rejected)
        self.assertEqual(len(self.launches()), 1)

    async def test_changed_input_relaunches_after_reattached_events_are_read(self):
        first = await self.start()
        self.assertEqual(await first.send(str(uuid.uuid4()), 'cmds'), 'received')
        await self.until(lambda: self.turns == ['done:cmds'])
        events = [json.loads(line) for line in Path(first.log_path).read_text().splitlines()]
        result_seq = next(event['seq'] for event in events
                          if json.loads(event['data']).get('type') == 'result')
        await self.until(lambda: self.store.get('coordinator_host_seq') > result_seq)
        with self.store.db:
            self.store.put('coordinator_host_seq', result_seq)
        attached = await self.reattach(first, instructions='changed instructions')
        self.assertGreater(attached._unread_reattach_events, 0)
        await attached.wait_closed()
        self.assertTrue(attached.rejected)
        self.assertEqual(len(self.launches()), 1)

    async def test_queued_owner_survives_restart(self):
        await self.queued_command_survives_restart('owner')

    async def test_queued_worker_result_survives_restart(self):
        await self.queued_command_survives_restart('worker_result')

    async def test_task_notification_turn_survives_restart(self):
        self.binary.write_text(self.binary.read_text() + '''    if text == 'notification':
        print(json.dumps({'type':'system','subtype':'task_notification','session_id':sid,
                          'task_id':'bg-1','status':'completed','summary':'Background work completed'}),flush=True)
        (root/'notification.pause').touch()
        while not (root/'notification.release').exists():
            time.sleep(.02)
        print(json.dumps({'type':'result','subtype':'success','session_id':sid,
                          'result':'background completion handled'}),flush=True)
''')
        first = await self.start()
        await first.send(str(uuid.uuid4()), 'notification')
        await self.until(lambda: (self.root / 'notification.pause').exists() and first.turn_open and
                         self.turns == ['done:notification'])
        self.assertFalse(first.idle())
        with self.store.db:
            host = self.store.get('coordinator_host')
            host.pop('fingerprint', None)
            self.store.put('coordinator_host', host)
        attached = await self.reattach(first)
        if attached.rejected:
            await asyncio.wait_for(attached.wait_closed(), 5)
        else:
            (self.root / 'notification.release').touch()
            await self.until(lambda: 'background completion handled' in self.turns)
        self.assertIn('background completion handled', self.turns)

    async def test_progress_turn_becomes_idle_and_stale_parent_relaunches_after_completion(self):
        first = await self.finished_parent()
        await self.until(lambda: not first.protocol.pending)
        self.assertFalse(first._recovery_uncertain)
        self.assertTrue(first.idle())
        (self.root / 'pause-completion').touch()
        self.assertEqual(await first.send(str(uuid.uuid4()), 'slow'), 'received')
        await self.until(lambda: (self.root / 'slow.started').exists())
        first.stale = True
        (self.root / 'slow.go').touch()
        await self.until(lambda: (self.root / 'completion.ready').exists() and 'done:slow' in self.turns)
        self.assertTrue(first.protocol.pending)
        self.assertFalse(first.rejected)
        (self.root / 'completion.release').touch()
        await asyncio.wait_for(first.wait_closed(), 5)
        self.assertTrue(first.rejected)
        self.assertTrue(first.idle())
        replacement = await self.start()
        self.assertEqual(replacement.session_id, first.session_id)
        await self.until(lambda: len(self.launches()) == 2)

    async def test_progress_history_relaunches_outdated_parent_at_start(self):
        first = await self.finished_parent()
        await self.until(lambda: not first.protocol.pending)
        attached = await self.reattach(first, instructions='changed instructions')
        self.assertTrue(attached.outdated)
        self.assertTrue(attached.rejected)
        self.assertFalse(attached._recovery_uncertain)
        await asyncio.wait_for(attached.wait_closed(), 5)
        replacement = await self.start(instructions='changed instructions')
        self.assertEqual(replacement.session_id, first.session_id)
        await self.until(lambda: len(self.launches()) == 2)
        again = await self.reattach(replacement, instructions='changed instructions')
        self.assertFalse(again.outdated)
        self.assertEqual(len(self.launches()), 2)

    async def test_progress_history_with_queued_owner_relaunches_once_after_consumption(self):
        attached = await self.queued_command_survives_restart('owner', completed=True)
        await asyncio.wait_for(attached.wait_closed(), 5)
        self.assertTrue(attached.rejected)
        self.assertEqual(self.turns, ['done:life-hold', 'done:life-next'])
        inputs = [json.loads(line) for line in (self.root / 'inputs.jsonl').read_text().splitlines()]
        queued = [event for event in inputs if event['message']['content'] == 'life-next']
        self.assertEqual(len(queued), 1)
        self.assertFalse(self.callbacks())
        replacement = await self.start(topic='home')
        self.assertEqual(replacement.session_id, attached.session_id)
        await self.until(lambda: len(self.launches()) == 2)
        again = await self.reattach(replacement, topic='home')
        self.assertFalse(again.outdated)
        self.assertEqual(len(self.launches()), 2)

    async def test_bad_live_line_recovers_at_first_idle_completion_after_result(self):
        first = await self.finished_parent()
        await self.until(lambda: not first.protocol.pending)
        (self.root / 'pause-completion').touch()
        self.assertEqual(await first.send(str(uuid.uuid4()), 'bad-line'), 'received')
        await self.until(lambda: (self.root / 'bad-line.ready').exists() and first._recovery_uncertain)
        first.stale = True
        self.assertFalse(first.rejected)
        self.assertFalse(first.idle())
        (self.root / 'bad-line.release').touch()
        await self.until(lambda: (self.root / 'completion.ready').exists() and 'done:bad-line' in self.turns)
        self.assertTrue(first._recovery_uncertain)
        self.assertTrue(first.protocol.pending)
        self.assertFalse(first.rejected)
        (self.root / 'completion.release').touch()
        await asyncio.wait_for(first.wait_closed(), 5)
        self.assertTrue(first.rejected)
        self.assertFalse(first._recovery_uncertain)
        self.assertTrue(first.idle())

    async def test_missing_writes_and_unhandled_objects_do_not_delay_idle_reattach(self):
        self.binary.write_text(self.binary.read_text().replace(
            "'type':'tool_progress'", "'type':'future_native_event'"))
        first = await self.finished_parent()
        await self.until(lambda: not first.protocol.pending)
        (first.process.directory / 'writes.jsonl').rename(self.root / 'saved-writes.jsonl')
        self.assertEqual(first.process.written_lines(), [])
        history = HostClient.history
        with patch.object(HostClient, 'history', lambda client, seq: list(history(client, seq)) + [
                '{}', '{"type":"future_native_event"}']):
            attached = await self.reattach(first, instructions='changed instructions')
        self.assertTrue(attached.rejected)
        self.assertFalse(attached._recovery_uncertain)
        await asyncio.wait_for(attached.wait_closed(), 5)

    async def test_unreadable_host_writes_delay_relaunch_until_a_later_result(self):
        first = await self.finished_parent()
        with self.store.db:
            host = self.store.get('coordinator_host')
            host.pop('fingerprint', None)
            self.store.put('coordinator_host', host)
        with patch.object(HostClient, 'written_lines', side_effect=OSError('unreadable')):
            attached = await self.reattach(first)
        self.assertTrue(attached._recovery_uncertain)
        self.assertFalse(attached.rejected)
        self.assertEqual(await attached.send(str(uuid.uuid4()), 'two'), 'received')
        await self.until(lambda: self.turns == ['done:one', 'done:two'])
        await attached.wait_closed()
        self.assertTrue(attached.rejected)

    async def test_non_object_host_write_delays_relaunch_until_a_later_result(self):
        first = await self.finished_parent()
        with self.store.db:
            host = self.store.get('coordinator_host')
            host.pop('fingerprint', None)
            self.store.put('coordinator_host', host)
        written_lines = HostClient.written_lines
        with patch.object(HostClient, 'written_lines', lambda client: written_lines(client) + ['[]']):
            attached = await self.reattach(first)
        self.assertTrue(attached._recovery_uncertain)
        self.assertFalse(attached.rejected)
        self.assertEqual(await attached.send(str(uuid.uuid4()), 'two'), 'received')
        await self.until(lambda: self.turns == ['done:one', 'done:two'])
        await attached.wait_closed()
        self.assertTrue(attached.rejected)

    async def test_coordinator_session_never_receives_secrets(self):
        calls = []
        launch = HostClient.launch

        async def recorded(directory, spec, **options):
            calls.append(options)
            return await launch(directory, spec, **options)

        with patch('coordinator.parent_state.HostClient.launch', recorded):
            await self.start()
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]['private']['env'], {})

    async def test_two_topic_parents_use_distinct_sessions_and_shared_cwd(self):
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES ('home',1,2,'Home',?,1)''', (str(self.root),))
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES ('other',1,3,'Other',?,1)''', (str(self.root / 'other'),))
            self.store.put('coordinator_home_topic', 'home')
        home = await self.start(topic='home')
        other = await self.start(topic='other')
        self.assertNotEqual(home.session_id, other.session_id)
        self.assertEqual(self.store.get('coordinator_session'), home.session_id)
        self.assertEqual(self.store.get('coordinator:other_session'), other.session_id)
        homes = [json.loads((self.store.directory / 'hosts' / session.host_id / 'spec.json').read_text())
                 for session in (home, other)]
        self.assertEqual([item['cwd'] for item in homes], [str(self.root)] * 2)

    async def test_account_notice_goes_to_the_main_channel_when_another_shares_its_folder(self):
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES ('main',1,2,'Main',?,1)''', (str(self.root),))
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES ('brainstorm',1,3,'Brainstorm',?,1)''', (str(self.root),))
            self.store.put('coordinator_home_topic', 'main')
            self.store.put('coordinator_rotation_from', 'old')
        brainstorm = await self.start(topic='brainstorm')
        self.assertIsNone(self.store.get('coordinator_session'))
        self.assertEqual(self.store.get('coordinator:brainstorm_session'), brainstorm.session_id)
        self.assertIsNone(self.store.db.execute('SELECT 1 FROM outbox').fetchone())
        self.assertIsNotNone(self.store.db.execute(
            "SELECT 1 FROM problems WHERE area='accounts' AND code='rotated'").fetchone())

    async def test_channel_parent_resumes_its_own_session_after_stop(self):
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES ('home',1,2,'Home',?,1)''', (str(self.root),))
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES ('other',1,3,'Other',?,1)''', (str(self.root / 'other'),))
            self.store.put('coordinator_home_topic', 'home')
        first = await self.start(topic='other')
        self.assertEqual(await first.send(str(uuid.uuid4()), 'one'), 'received')
        await self.until(lambda: self.turns == ['done:one'])
        await first.stop()
        second = await self.start(topic='other')
        await self.until(lambda: len(self.launches()) == 2)
        args = self.launches()[1]['argv']
        self.assertEqual(Path(args[args.index('--resume') + 1]).resolve(), Path(self.transcript(first)).resolve())
        self.assertEqual(second.session_id, first.session_id)
        self.assertIsNone(self.store.get('coordinator_session'))

    async def test_rejected_turn_rotates_and_names_the_same_message_to_the_new_parent(self):
        service = self.account_service()
        row = self.store.message_save('home', 'owner', 'quota')
        await self.feed_until(lambda: len(self.launches()) == 2 and len(self.inputs()) == 2)
        launches = self.launches()
        self.assertEqual([entry['profile'] for entry in launches],
                         [str(self.root / 'a'), str(self.root / 'b')])
        transferred = self.root / 'b' / 'projects' / 'test' / (service.session.session_id + '.jsonl')
        self.assertEqual(launches[1]['argv'][launches[1]['argv'].index('--resume') + 1], str(transferred))
        self.assertTrue(transferred.is_symlink())
        self.assertEqual(transferred.resolve(), (self.root / 'a' / 'projects' / 'test' /
                                                  (service.session.session_id + '.jsonl')).resolve())
        self.assertEqual([event['message']['content'] for event in self.inputs()],
                         ['[topic=home name="Home" message=1 kind=owner] quota', lost_note(2, 1)])
        self.assertEqual(self.store.db.execute('SELECT delivered FROM messages WHERE id=?',
                                               (row['id'],)).fetchone()[0], 'received')
        self.assertEqual(self.store.get('last_account'), 'b')
        self.assertEqual(self.store.get('account_blocks')['a']['reason'], 'quota')
        hosts = sorted((self.store.directory / 'hosts').glob('coordinator-*'))
        self.assertEqual(len(hosts), 2)
        current = self.store.get('coordinator_host')
        self.assertEqual(current['account'], 'b')
        self.assertIn(current['id'], [path.name for path in hosts])
        self.assertEqual({json.loads((path / 'spec.json').read_text())['env']['CLAUDE_CONFIG_DIR'] for path in hosts},
                         {str(self.root / 'a'), str(self.root / 'b')})
        notices = [row['text'] for row in self.store.db.execute('SELECT text FROM outbox')]
        self.assertEqual(notices, [])

    async def test_a_turn_that_spans_a_restart_and_is_rejected_names_its_message(self):
        self.account_service()
        first = await self.start(topic='home')
        row = self.store.message_save('home', 'owner', 'span-reject')['id']
        self.assertEqual(await first.send(message_uuid(row), 'span-reject', row_id=row), 'received')
        await self.until(lambda: (self.root / 'span.started').exists())
        await first.detach()
        self.sessions.remove(first)
        attached = await self.start(topic='home')
        self.assertEqual(attached.host_id, first.host_id)
        (self.root / 'span.go').touch()
        await attached.wait_closed()
        self.assertEqual(self.callbacks(), [LOST_TURN % ('message=%d' % row)])

    async def test_a_service_stop_during_a_relaunch_settles_after_the_old_parent_exits(self):
        self.account_service()
        first = await self.start(topic='home')
        row = self.store.message_save('home', 'owner', 'span-reject')['id']
        await first.send(message_uuid(row), 'span-reject', row_id=row)
        await self.until(lambda: (self.root / 'span.started').exists())
        with patch.object(first.process, 'stop', AsyncMock()):
            first.relaunch(first._turn_rows | first._unreplayed)
            await first.detach()
        self.sessions.remove(first)
        self.assertEqual(self.callbacks(), [])
        self.assertEqual(self.store.get(first.key + '_lost'), [row])
        await HostClient(self.runner.state_dir / 'hosts' / first.host_id).stop()
        await self.start(topic='home')
        self.assertEqual(self.callbacks(), [LOST_TURN % ('message=%d' % row)])
        self.assertIsNone(self.store.get(first.key + '_lost'))

    async def test_a_failed_settle_keeps_its_messages_for_the_next_start(self):
        self.account_service()
        first = await self.start(topic='home')
        shown = self.store.message_save('home', 'owner', 'recorded')['id']
        self.assertEqual(await first.send(message_uuid(shown), 'recorded', row_id=shown), 'received')
        await self.until(lambda: 'done:recorded' in self.turns)
        unsent = self.store.message_save('home', 'owner', 'unsent')['id']
        with self.store.db:
            self.store.put(first.key + '_lost', [shown, unsent])
        with patch.object(self.store, 'message_save', side_effect=sqlite3.OperationalError('locked')):
            with self.assertRaises(sqlite3.OperationalError):
                first._settle({shown, unsent})
        self.assertEqual(self.store.get(first.key + '_lost'), [shown, unsent])

    async def test_an_uncertain_message_the_transcript_lacks_is_sent_again(self):
        self.account_service()
        first = await self.start(topic='home')
        row = self.store.message_save('home', 'owner', 'never-arrived')['id']
        with self.store.db:
            self.store.db.execute("UPDATE messages SET delivered='uncertain' WHERE id=?", (row,))
        first._settle({row})
        self.assertEqual(self.store.db.execute('SELECT delivered FROM messages WHERE id=?', (row,)).fetchone()[0],
                         'pending')

    async def test_parent_moves_after_a_turn_reaches_the_fixed_95_percent_point(self):
        self.account_service()
        first = await self.start(topic='home')
        self.assertEqual(first.account_alias, 'a')
        self.assertEqual(await first.send(str(uuid.uuid4()), 'threshold'), 'received')
        await self.until(lambda: self.turns == ['done:threshold'])
        self.assertEqual(self.store.get('active_account'), 'b')
        await first.wait_closed()
        self.assertTrue(first.rejected)

    async def test_parent_finishes_queued_turns_before_restarting_at_threshold(self):
        self.account_service()
        source = self.binary.read_text()
        before = '            print(json.dumps(line),flush=True)\n        continue'
        after = '''            if line.get('state') == 'completed' and line.get('command_uuid') == event['uuid']:
                complete()
            print(json.dumps(line),flush=True)
        continue'''
        self.assertEqual(source.count(before), 1)
        self.binary.write_text(source.replace(before, after))
        (self.root / 'pause-completion').touch()
        first = await self.start(topic='home')
        self.assertEqual(await first.send(str(uuid.uuid4()), 'life-hold'), 'received')
        await self.until(lambda: (self.root / 'life-hold.started').exists())
        status = self.store.get('account_status')
        status['a']['usage']['five_hour']['utilization'] = 96
        with self.store.db:
            self.store.put('account_status', status)
        self.assertEqual(first.router.activate(), 'b')
        self.assertEqual(await first.send(str(uuid.uuid4()), 'life-next'), 'received')
        await self.until(lambda: (self.root / 'completion.ready').exists()
                         and self.turns == ['done:life-hold', 'done:life-next'])
        self.assertEqual(self.turns, ['done:life-hold', 'done:life-next'])
        self.assertFalse(first.rejected)
        self.assertFalse(first.closed)
        self.assertFalse(first.idle())
        self.assertEqual(len(self.launches()), 1)
        (self.root / 'completion.release').touch()
        await first.wait_closed()
        self.assertTrue(first.rejected)

    async def test_reattached_parent_mid_turn_finishes_before_restarting_at_threshold(self):
        self.account_service()
        first = await self.start(topic='home')
        self.assertEqual(first.account_alias, 'a')
        self.assertEqual(await first.send(str(uuid.uuid4()), 'slow'), 'received')
        await self.until(lambda: (self.root / 'slow.started').exists() and first.busy)
        await first.detach()
        self.sessions.remove(first)
        status = self.store.get('account_status')
        status['a']['usage']['five_hour']['utilization'] = 96
        with self.store.db:
            self.store.put('account_status', status)
        attached = await self.start(topic='home')
        self.assertEqual(attached.host_id, first.host_id)
        self.assertEqual(attached.router.activate(), 'b')
        (self.root / 'slow.go').touch()
        await self.until(lambda: self.turns == ['done:slow'])
        self.assertEqual(self.turns, ['done:slow'])
        await attached.wait_closed()
        self.assertTrue(attached.rejected)

    async def test_owner_coordinator_model_setting_wins_over_the_command_line_default(self):
        self.account_service()
        with self.store.db:
            self.store.put('coordinator_model', 'claude-opus-5-5')
        row = self.store.message_save('home', 'owner', 'hello')['id']
        await self.feed_until(lambda: self.launches() and self.store.db.execute(
            'SELECT delivered FROM messages WHERE id=?', (row,)).fetchone()[0] == 'received')
        argv = self.launches()[0]['argv']
        snapshot_flag = argv.index('--system-prompt-snapshot')
        self.assertEqual(argv[snapshot_flag:snapshot_flag + 2], ['--system-prompt-snapshot', 'off'])
        self.assertEqual(argv[argv.index('--model') + 1], 'claude-opus-5-5')
        self.assertEqual(argv[argv.index('--effort') + 1], 'max')
        settings = json.loads(argv[argv.index('--settings') + 1])
        memory_directory = (self.store.directory / 'coordinator-memory').resolve()
        self.assertEqual(settings, {'ultracode': False, 'autoMemoryDirectory': str(memory_directory),
            'claudeMdExcludes': [str(parent / name) for parent in Path.home().parents
                                for name in ('CLAUDE.md', 'CLAUDE.local.md', '.claude/CLAUDE.md')]})
        self.assertTrue(memory_directory.is_dir())

    async def test_parent_memory_setting_resolves_an_existing_directory_symlink(self):
        self.account_service()
        target = self.root / 'shared-memory'
        target.mkdir()
        link = self.store.directory / 'coordinator-memory'
        link.symlink_to(target, target_is_directory=True)
        row = self.store.message_save('home', 'owner', 'hello')['id']
        await self.feed_until(lambda: self.launches() and self.store.db.execute(
            'SELECT delivered FROM messages WHERE id=?', (row,)).fetchone()[0] == 'received')
        argv = self.launches()[0]['argv']
        settings = json.loads(argv[argv.index('--settings') + 1])
        self.assertEqual(settings['autoMemoryDirectory'], str(target.resolve()))
        self.assertTrue(target.is_dir())

    async def test_command_line_coordinator_model_applies_without_the_setting(self):
        self.account_service()
        row = self.store.message_save('home', 'owner', 'hello')['id']
        await self.feed_until(lambda: self.launches() and self.store.db.execute(
            'SELECT delivered FROM messages WHERE id=?', (row,)).fetchone()[0] == 'received')
        argv = self.launches()[0]['argv']
        self.assertEqual(argv[argv.index('--model') + 1], 'model')

    async def test_rejection_names_a_message_replayed_after_the_previous_turn_ended(self):
        self.account_service()
        self.store.message_save('home', 'owner', 'hold')
        self.store.message_save('home', 'owner', 'next-quota')
        await self.feed_until(lambda: len(self.launches()) == 2 and len(self.inputs()) == 3)
        self.assertEqual([event['message']['content'] for event in self.inputs()],
                         ['[topic=home name="Home" message=1 kind=owner] hold', NEXT_QUOTA, lost_note(3, 2)])

    async def test_all_accounts_blocked_notifies_once_and_waits_for_reset(self):
        self.account_service()
        factory = self.service.session_factory
        attempts = []

        async def counted(*args, **kwargs):
            attempts.append(True)
            return await factory(*args, **kwargs)

        self.service.session_factory = counted
        self.store.message_save('home', 'owner', 'reject-everywhere')
        await self.feed_until(lambda: self.store.get('coordinator_account_retry_at') is not None)
        launches = len(self.launches())
        self.assertEqual(launches, 2)
        self.assertEqual(len(attempts), 3)
        self.assertGreater(self.store.get('coordinator_account_retry_at'), time.time())
        for _ in range(5):
            await self.service.feed_once()
        self.assertEqual(len(self.launches()), launches)
        self.assertEqual(len(attempts), 3)
        notices = [row['text'] for row in self.store.db.execute('SELECT text FROM outbox')]
        self.assertEqual(len([text for text in notices if 'No Claude account is available' in text]), 1)
        self.assertIn(datetime.fromtimestamp(self.store.get('coordinator_account_retry_at'),
                                         timezone.utc).isoformat(), notices[-1])
        retry_at = self.store.get('coordinator_account_retry_at')
        with patch('coordinator.service.time.time', return_value=retry_at + 1):
            with self.store.db:
                status = self.store.get('account_status')
                for snapshot in status.values():
                    snapshot['observed_at'] = retry_at + 1
                self.store.put('account_status', status)
            await self.feed_until(lambda: len(self.launches()) == 3)
        self.assertEqual(len(attempts), 4)

    async def test_two_turns_in_one_process(self):
        (self.root / 'mcp.json').write_text('{"mcpServers":{}}')
        session = await self.start(mcp_config=self.root / 'mcp.json')
        first = str(uuid.uuid4())
        second = str(uuid.uuid4())
        self.assertEqual(await session.send(first, 'one'), 'received')
        await self.until(lambda: len(self.turns) == 1)
        self.assertFalse(session.busy)
        self.assertEqual(await session.send(second, 'two'), 'received')
        await self.until(lambda: len(self.turns) == 2)
        self.assertEqual(await session.send(second, 'ignored duplicate'), 'received')
        self.assertEqual(self.turns, ['done:one', 'done:two'])
        self.assertFalse(session.closed)
        self.assertEqual(len(self.launches()), 1)
        self.assertEqual([event['uuid'] for event in self.inputs()], [first, second])
        self.assertFalse(session.busy)
        self.assertEqual(Path(session.log_path).stat().st_mode & 0o777, 0o600)
        args = self.launches()[0]['argv']
        self.assertEqual(args[args.index('--session-id') + 1], session.session_id)
        self.assertIn('instructions\nCurrent usage policy:\n', args[args.index('--append-system-prompt') + 1])
        self.assertEqual(args[args.index('--mcp-config') + 1], str(self.root / 'mcp.json'))

    async def test_parent_launches_with_shared_mcp_and_keeps_values_out_of_captures(self):
        home = self.root / 'home'
        home.mkdir()
        value = 'FAKE-SHARED-MCP-HEADER-VALUE'
        entries = {name: {'type': 'http', 'url': 'http://127.0.0.1:1/' + name,
                          'headers': {'Authorization': value}}
                   for name in ('example-mcp', 'other-mcp')}
        (home / '.claude.json').write_text(json.dumps({'mcpServers': entries}))
        script = self.binary.read_text()
        script = script.replace('root=Path(__file__).parent\n',
                                'root=Path(__file__).parent\n'
                                "config=json.loads(Path(sys.argv[sys.argv.index('--mcp-config')+2]).read_text())\n"
                                "print(config['mcpServers']['example-mcp']['headers']['Authorization'],"
                                "file=sys.stderr,flush=True)\n")
        self.binary.write_text(script)
        with patch.dict(os.environ, {'HOME': str(home)}):
            session = await self.start()
        await self.until(lambda: len(self.launches()) == 1)
        args = self.launches()[0]['argv']
        index = args.index('--mcp-config')
        shared_mcp_path = Path(args[index + 2])
        self.assertEqual(args[index + 1:], [str(self.store.directory / 'coordinator-mcp.json'),
                                             str(shared_mcp_path)])
        coordinator_config = json.loads((self.store.directory / 'coordinator-mcp.json').read_text())
        self.assertIs(coordinator_config['mcpServers']['torii']['alwaysLoad'], True)
        self.assertEqual(shared_mcp_path.parent, self.store.directory)
        shared_mcp_config = json.loads(shared_mcp_path.read_text())
        self.assertEqual(shared_mcp_config, {'mcpServers': entries})
        self.assertNotIn('alwaysLoad', shared_mcp_config['mcpServers']['example-mcp'])
        self.assertEqual(shared_mcp_path.stat().st_mode & 0o777, 0o600)
        host = self.runner.state_dir / 'hosts' / session.host_id
        for path in host.iterdir():
            if path.is_file():
                self.assertNotIn(value, path.read_text(errors='replace'), path.name)
        self.assertNotIn(value, json.dumps(self.problems()))
        self.assertNotIn(value, ''.join(self.store.db.iterdump()))
        self.assertNotIn(value, (self.store.directory / 'coordinator-mcp.json').read_text())

    async def test_mid_turn_message_is_received_without_closing_process(self):
        session = await self.start()
        self.assertEqual(await session.send(str(uuid.uuid4()), 'hold'), 'received')
        await self.until(lambda: (self.root / 'hold.started').exists())
        self.assertTrue(session.busy)
        follow = str(uuid.uuid4())
        self.assertEqual(await session.send(follow, 'follow'), 'received')
        await self.until(lambda: len(self.turns) == 2)
        self.assertEqual(self.turns, ['done:hold', 'done:follow'])
        self.assertFalse(session.closed)
        self.assertEqual(len(self.launches()), 1)

    async def test_written_input_is_the_receipt_without_waiting_for_the_turn(self):
        session = await self.start(receipt_timeout=30)
        started = time.monotonic()
        self.assertEqual(await session.send(str(uuid.uuid4()), 'no-echo'), 'received')
        self.assertLess(time.monotonic() - started, 10)
        await self.until(lambda: (self.root / 'no-echo.started').exists())
        self.assertEqual(await session.send(str(uuid.uuid4()), 'two'), 'received')
        await self.until(lambda: self.turns == ['done:two'])

    async def test_late_replay_clears_an_uncertain_message(self):
        session = await self.start()
        row, = self.owner_rows('late')
        with self.store.db:
            self.store.db.execute("UPDATE messages SET delivered='uncertain',receipt='uncertain' WHERE id=?", (row,))
        await session.send(message_uuid(row), 'late', row_id=row)
        await self.until(lambda: self.delivery(row) == ('received', 'replayed'))

    async def test_session_closing_before_replay_leaves_written_messages_uncertain(self):
        session = await self.start()
        quiet, dying = self.owner_rows('no-echo', 'die')
        self.assertEqual(await self.deliver(session, quiet, 'no-echo'), 'received')
        await self.until(lambda: (self.root / 'no-echo.started').exists())
        await self.deliver(session, dying, 'die')
        await session.wait_closed()
        self.assertEqual(self.delivery(quiet), ('uncertain', 'session_closed'))
        self.assertEqual(self.delivery(dying)[0], 'uncertain')

    async def test_replayed_message_stays_received_when_the_session_closes(self):
        session = await self.start()
        row, = self.owner_rows('one')
        self.assertEqual(await self.deliver(session, row, 'one'), 'received')
        await self.until(lambda: self.turns == ['done:one'])
        await session.stop()
        self.assertEqual(self.delivery(row)[0], 'received')

    async def test_parent_selection_attaches_exited_claude_and_replays_reactions_and_receipts(self):
        from coordinator import parent
        from coordinator.host import HostClient
        session = await self.start()
        quiet, completed = self.owner_rows('no-echo', 'one')
        await self.deliver(session, quiet, 'no-echo')
        with self.store.db:
            self.store.db.execute("UPDATE messages SET receipt='written' WHERE id=?", (quiet,))
        await self.until(lambda: (self.root / 'no-echo.started').exists())
        directory = session.process.directory
        await session.detach()
        self.sessions.remove(session)
        await session.process.request('write', line=json.dumps(session.protocol.user(message_uuid(completed), 'one')))
        self.store.message_sending(completed)
        session.process.stdin.close()
        await self.until(lambda: HostClient(directory).state == 'exited')
        session = await parent.start(self.store, self.runner, self.root, 'model', 'instructions',
                                     on_turn=self.turns.append)
        self.sessions.append(session)
        self.assertEqual(session.process.directory, directory)
        await session.wait_closed()
        self.assertEqual(self.turns, ['done:one'])
        self.assertEqual(self.delivery(quiet), ('uncertain', 'session_closed'))
        self.assertEqual(self.delivery(completed), ('received', 'replayed'))
        self.assertIsNone(self.store.db.execute('SELECT reaction_desired FROM messages WHERE id=?',
                                              (completed,)).fetchone()[0])

    async def test_resume_after_stop_uses_saved_session(self):
        session = await self.start()
        self.assertEqual(await session.send(str(uuid.uuid4()), 'one'), 'received')
        await self.until(lambda: self.turns == ['done:one'])
        await session.stop()
        resumed = await self.start()
        self.assertEqual(await resumed.send(str(uuid.uuid4()), 'two'), 'received')
        await self.until(lambda: self.turns == ['done:one', 'done:two'])
        args = self.launches()[1]['argv']
        self.assertEqual(args[args.index('--resume') + 1], self.transcript(session))
        self.assertEqual(resumed.session_id, session.session_id)

    async def test_resumed_parent_reads_changed_shared_mcp_source(self):
        home = self.root / 'home'
        home.mkdir()
        source = home / '.claude.json'
        first_entry = {'type': 'http', 'url': 'http://127.0.0.1:1/work', 'headers': {}}
        second_entry = {'type': 'http', 'url': 'http://127.0.0.1:2/work', 'headers': {}}
        source.write_text(json.dumps({'mcpServers': {'example-mcp': first_entry}}))
        with patch.dict(os.environ, {'HOME': str(home)}):
            first = await self.start()
            self.assertEqual(await first.send(str(uuid.uuid4()), 'one'), 'received')
            await self.until(lambda: self.turns == ['done:one'])
            await first.stop()
            source.write_text(json.dumps({'mcpServers': {'example-mcp': second_entry}}))
            resumed = await self.start()
        await self.until(lambda: len(self.launches()) == 2)
        args = self.launches()[1]['argv']
        self.assertIn('--resume', args)
        shared_mcp_path = Path(args[-1])
        self.assertEqual(args[-2:], [str(self.store.directory / 'coordinator-mcp.json'), str(shared_mcp_path)])
        self.assertEqual(json.loads(shared_mcp_path.read_text()),
                         {'mcpServers': {'example-mcp': second_entry}})
        self.assertEqual(resumed.session_id, first.session_id)

    async def test_resumed_parent_that_exits_early_keeps_its_saved_session(self):
        session = await self.start()
        self.assertEqual(await session.send(str(uuid.uuid4()), 'one'), 'received')
        await self.until(lambda: self.turns == ['done:one'])
        await session.stop()
        (self.root / 'resume-crashes').touch()
        crashed = await self.start()
        await HostClient(self.runner.state_dir / 'hosts' / crashed.host_id).request('write', line='release')
        await self.until(lambda: (self.root / 'release-echo').exists())
        await crashed.wait_closed()
        (self.root / 'resume-crashes').unlink()
        (self.root / 'resume-fails').touch()
        failed = await self.start()
        row, = self.owner_rows('lost')
        await self.deliver(failed, row, 'lost')
        await failed.wait_closed()
        (self.root / 'resume-fails').unlink()
        resumed = await self.start()
        await self.until(lambda: len(self.launches()) == 4)
        self.assertEqual([launch['argv'][launch['argv'].index('--resume') + 1] for launch in self.launches()[1:]],
                         [self.transcript(session)] * 3)
        self.assertEqual(resumed.session_id, session.session_id)
        self.assertEqual(self.store.get('coordinator_session'), session.session_id)
        self.assertEqual(self.delivery(row)[0], 'uncertain')

    async def test_service_keeps_the_saved_session_when_a_resumed_parent_exits_early(self):
        service = self.account_service()

        def replayed(row):
            return self.store.db.execute('SELECT receipt FROM messages WHERE id=?', (row,)).fetchone()[0] == 'replayed'

        first = self.store.message_save('home', 'owner', 'hello')['id']
        await self.feed_until(lambda: replayed(first))
        saved = self.store.get('coordinator_session')
        (self.root / 'resume-crashes').touch()
        await service.session.stop()
        await self.feed_until(lambda: len(self.launches()) >= 2)
        resumed = service.session
        await HostClient(self.runner.state_dir / 'hosts' / resumed.host_id).request('write', line='release')
        await self.until(lambda: (self.root / 'release-echo').exists())
        await service.session.wait_closed()
        (self.root / 'resume-crashes').unlink()
        later = self.store.message_save('home', 'owner', 'later')['id']
        await self.feed_until(lambda: replayed(later))
        self.assertEqual(self.store.get('coordinator_session'), saved)
        self.assertEqual(service.session.session_id, saved)
        self.assertTrue(all('--resume' in launch['argv'] for launch in self.launches()[1:]))

    async def test_resume_without_its_transcript_starts_a_fresh_session(self):
        profile = self.root / 'a'
        profile.mkdir()
        with self.store.db:
            self.store.put('accounts', {'a': {'config_dir': str(profile), 'enabled': True}})
            self.store.put('account_status', {'a': {'identity': {'email': 'a@example.com', 'logged_in': True},
                'observed_at': time.time(), 'usage': {'seven_day': {'utilization': 10,
                'resets_at': time.time() + 3600}}}})
        session = await self.start()
        self.assertEqual(await session.send(str(uuid.uuid4()), 'one'), 'received')
        await self.until(lambda: self.turns == ['done:one'])
        await session.stop()
        for transcript in profile.glob('projects/*/' + session.session_id + '.jsonl'):
            transcript.unlink()
        fresh = await self.start()
        await self.until(lambda: len(self.launches()) == 2)
        args = self.launches()[1]['argv']
        self.assertNotIn('--resume', args)
        self.assertEqual(args[args.index('--session-id') + 1], fresh.session_id)
        self.assertNotEqual(fresh.session_id, session.session_id)
        self.assertTrue(fresh.resume_failed)
        self.assertEqual(self.store.get('coordinator_session'), fresh.session_id)
        lost, = [detail for area, code, detail in self.problems() if code == 'resume-lost']
        self.assertIn(session.session_id, lost)

    async def test_reattached_parent_still_sees_its_running_background_task(self):
        session = await self.start()
        self.assertEqual(await session.send(str(uuid.uuid4()), 'bg'), 'received')
        await self.until(lambda: self.turns == ['done:bg'])
        self.assertEqual(session.protocol.background, [{'id': 'b1', 'status': 'running'}])
        await session.detach()
        self.sessions.remove(session)
        attached = await self.start()
        self.assertEqual(attached.host_id, session.host_id)
        self.assertEqual(attached.protocol.background, [{'id': 'b1', 'status': 'running'}])

    async def test_provider_event_after_the_turn_counts_as_activity(self):
        session = await self.start()
        self.assertEqual(await session.send(str(uuid.uuid4()), 'bg-done'), 'received')
        await self.until(lambda: self.turns == ['done:bg-done'])
        ended = session.last_turn_at
        await self.until(lambda: session.last_turn_at > ended)

    async def test_skill_list_change_after_the_turn_is_not_activity(self):
        session = await self.start()
        self.assertEqual(await session.send(str(uuid.uuid4()), 'cmds'), 'received')
        await self.until(lambda: self.turns == ['done:cmds'])
        ended = session.last_turn_at
        seq = self.store.get('coordinator_host_seq')
        await self.until(lambda: self.store.get('coordinator_host_seq') > seq)
        self.assertEqual(session.last_turn_at, ended)

    async def test_reattached_main_parent_confirms_another_channels_replayed_row(self):
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES ('home',1,2,'Home',?,1)''', (str(self.root),))
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES ('other',1,3,'Other',?,1)''', (str(self.root / 'other'),))
            self.store.put('coordinator_home_topic', 'home')
        session = await self.start(topic='home')
        await session.detach()
        self.sessions.remove(session)
        row = self.store.message_save('other', 'owner', 'queued before the split')['id']
        with self.store.db:
            self.store.db.execute("UPDATE messages SET delivered='uncertain',receipt='service_restarted' "
                                  "WHERE id=?", (row,))
        attached = await self.start(topic='home')
        self.assertEqual(await attached.send(message_uuid(row), 'queued before the split'), 'received')
        await self.until(lambda: self.delivery(row)[0] == 'received')

    async def test_dead_process_closes_session(self):
        session = await self.start()
        await session.send(str(uuid.uuid4()), 'die')
        await session.wait_closed()
        self.assertTrue(session.closed)
        self.assertEqual(await session.send(str(uuid.uuid4()), 'later'), 'closed')
        self.assertEqual(self.store.get('coordinator_session'), session.session_id)

    async def test_process_exit_the_service_did_not_ask_for_is_an_error_problem(self):
        session = await self.start()
        with self.assertLogs('coordinator.problems', 'ERROR') as logs:
            await session.send(str(uuid.uuid4()), 'die')
            await session.wait_closed()
        self.assertEqual(self.problems(), [('coordinator', 'session-closed', 'exit=3 failure=none')])
        self.assertIn('ERROR:coordinator.problems:problem area=coordinator code=session-closed', logs.output[0])

    async def test_reader_failure_is_saved_and_named_when_the_session_closes(self):
        session = await self.start()

        def broken(event):
            raise RuntimeError('reader broke')
        session.protocol.handle = broken
        with self.assertLogs('coordinator.problems', 'ERROR'):
            await session.send(str(uuid.uuid4()), 'hello')
            await session.wait_closed()
        (area, code, detail), closed = self.problems()
        self.assertEqual((area, code), ('coordinator', 'reader-failed'))
        self.assertRegex(detail, r'^RuntimeError: reader broke at test_session\.py:\d+$')
        self.assertEqual(closed[:2], ('coordinator', 'session-closed'))
        self.assertIn('failure=RuntimeError: reader broke', closed[2])

    async def test_requested_stop_and_detach_save_no_problem(self):
        session = await self.start()
        await session.stop()
        self.sessions.remove(session)
        session = await self.start()
        await session.detach()
        self.sessions.remove(session)
        attached = await self.start()
        self.assertEqual(attached.host_id, session.host_id)
        self.assertEqual(self.problems(), [])

    async def test_lock_refuses_second_owner(self):
        session = await self.start()
        with self.assertRaises(BlockingIOError):
            await CoordinatorSession.start(self.store, self.runner, self.root, 'model', 'instructions')
        self.assertFalse(session.closed)
        await self.until(lambda: (self.root / 'launches.jsonl').exists())
        self.assertEqual(len(self.launches()), 1)

    async def test_external_writer_refuses_start(self):
        async def external(provider, session_id):
            return True
        self.runner._external_writer = external
        with self.assertRaisesRegex(RuntimeError, 'external native process'):
            await CoordinatorSession.start(self.store, self.runner, self.root, 'model', 'instructions')
        self.assertFalse((self.root / 'launches.jsonl').exists())

    def test_restarted_prompt_lists_saved_work(self):
        prompt = CoordinatorSession.restarted_prompt(
            [{'id': 4, 'topic': 'home', 'number': 1, 'title': 'Build', 'updated': 1}],
            [{'id': 7, 'task': 4, 'provider': 'codex', 'status': 'running'}])
        self.assertIn('Open jobs: {"count":1,"not_shown":0,"shown":[', prompt)
        self.assertIn('"id":4,"number":1,"title":"Build","topic":"home"', prompt)
        self.assertIn('Registered workers: {"count":1,"not_shown":0,"shown":[', prompt)
        self.assertIn('"id":7,"provider":"codex","status":"running","task":4', prompt)


class RestartPromptTests(unittest.TestCase):
    def test_uncertain_messages_are_capped_by_count_and_size(self):
        rows = [{'id': number, 'topic': 'home', 'kind': 'owner', 'text': 'm%d ' % number + 'x' * 30000}
                for number in range(1, 41)]
        prompt = CoordinatorSession.restarted_prompt([], [], rows)
        summary = json.loads(prompt.split('characters): ', 1)[1].split('. Inspect', 1)[0])
        self.assertEqual(summary['count'], 40)
        self.assertEqual([row['id'] for row in summary['shown']], list(range(40, 40 - len(summary['shown']), -1)))
        self.assertLessEqual(len(summary['shown']), RESTART_MESSAGES)
        self.assertLessEqual(sum(len(row['text']) for row in summary['shown']), RESTART_MESSAGE_CHARS)
        self.assertEqual(summary['not_shown'], 40 - len(summary['shown']))
        self.assertTrue(all(len(row['text']) <= RESTART_MESSAGE_HEAD + 1 for row in summary['shown']))
        self.assertLess(len(prompt), RESTART_MESSAGE_CHARS + 2000)

    def test_large_restart_prompt_has_fixed_bound(self):
        tasks, workers = restart_work_rows(1000, 1000)
        tasks[0]['title'] = 'z' * 201
        rows = [{'id': number, 'topic': 'home', 'kind': 'owner', 'text': 'x' * 30000}
                for number in range(1, 41)]
        prompt = CoordinatorSession.restarted_prompt(tasks, workers, rows)
        self.assertLess(len(prompt), 32000)
        jobs = json.loads(prompt.split('Open jobs: ', 1)[1].split('. Registered workers:', 1)[0])
        active = json.loads(prompt.split('Registered workers: ', 1)[1].split('. Messages with', 1)[0])
        self.assertEqual((jobs['count'], len(jobs['shown']), jobs['not_shown']), (1000, 50, 950))
        self.assertEqual((active['count'], len(active['shown']), active['not_shown']), (1000, 30, 970))
        self.assertEqual(next(row['title'] for row in jobs['shown'] if row['id'] == 1), 'z' * 200 + '…')

    def test_short_uncertain_messages_are_shown_whole(self):
        rows = [{'id': 1, 'topic': 'home', 'kind': 'owner', 'text': 'check the build'}]
        prompt = CoordinatorSession.restarted_prompt([], [], rows)
        self.assertIn('"text":"check the build"', prompt)
        self.assertIn('"not_shown":0', prompt)

    def test_transcript_shows_turn_starts_and_mid_turn_queued_messages(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'session.jsonl'
            started, queued, missing, attachment_parent = (message_uuid(number) for number in (1, 2, 3, 4))
            path.write_text('\n'.join(json.dumps(entry) for entry in (
                {'type': 'user', 'uuid': started, 'message': {'role': 'user', 'content': 'one'}},
                {'type': 'attachment', 'uuid': str(uuid.uuid4()),
                 'attachment': {'type': 'queued_command', 'source_uuid': queued, 'prompt': 'two'}},
                {'type': 'attachment', 'uuid': str(uuid.uuid4()), 'parentUuid': attachment_parent,
                 'attachment': {'type': 'total_tokens_reminder'}},
                {'type': 'assistant', 'message': {'content': 'mentions ' + missing}})) + '\nnot json ' + missing + '\n')
            self.assertEqual(transcript_receipts(path, {started, queued, missing, attachment_parent}),
                             {started, queued})


class MessageTagTests(unittest.TestCase):
    def test_tag_names_the_topic_id_and_quotes_the_name(self):
        row = {'id': 7, 'kind': 'owner'}
        first = message_tag({'id': '-100:68', 'name': 'Torii'}, row)
        second = message_tag({'id': '-100:118', 'name': 'Torii'}, row)
        self.assertEqual(first, '[topic=-100:68 name="Torii" message=7 kind=owner] ')
        self.assertNotEqual(first, second)
        self.assertEqual(message_tag({'id': '-100:5', 'name': 'a] [topic=-100:68 "x"'}, {'id': 8, 'kind': 'worker_result'}),
                         '[topic=-100:5 name="a] [topic=-100:68 \\"x\\"" message=8 kind=worker_result] ')


class NativeSwitchingTests(CoordinatorSessionTestsSupport, unittest.IsolatedAsyncioTestCase):
    async def eligible_parent(self):
        self.account_service()
        return await self.start(topic='home')

    def status(self, alias, **changes):
        snapshots = self.store.get('account_status')
        snapshots[alias].update(changes)
        self.store.put('account_status', snapshots)

    async def test_native_parent_stays_on_an_eligible_account_after_a_sooner_reset(self):
        first = await self.eligible_parent()
        self.status('b', usage={'seven_day': {'utilization': 10, 'resets_at': time.time() + 600}})
        self.assertEqual(first.router.activate(), 'b')
        self.assertIsNone(first._account_move())
        self.assertEqual(await first.send(str(uuid.uuid4()), 'hello'), 'received')
        await self.until(lambda: first.idle())
        self.assertFalse(first.rejected)

    async def test_native_parent_moves_at_turn_end_when_its_account_is_full(self):
        first = await self.eligible_parent()
        (self.root / 'pause-completion').touch()
        self.assertEqual(await first.send(str(uuid.uuid4()), 'threshold'), 'received')
        await self.until(lambda: self.turns == ['done:threshold'])
        self.assertFalse(first.rejected)
        (self.root / 'completion.release').touch()
        await first.wait_closed()
        self.assertTrue(first.rejected)
        self.assertEqual(self.store.get('coordinator_rotation_from'), 'a')
        self.assertIsInstance(self.store.get('account_status_refresh_requested'), float)
        second = await self.start(topic='home')
        self.assertEqual(second.session_id, first.session_id)
        spec = json.loads((self.runner.state_dir / 'hosts' / second.host_id / 'spec.json').read_text())
        self.assertEqual(spec['env'], second.router.environment('b'))
        self.assertIn('--resume', spec['argv'])
        resume = Path(spec['argv'][spec['argv'].index('--resume') + 1])
        self.assertEqual(resume.parent.parent.parent, self.root / 'b')
        self.assertTrue(resume.samefile(first.router.transcript(first.session_id)))
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM problems WHERE area='accounts' AND code='rotated'").fetchone()[0], 1)

    async def test_native_parent_moves_for_each_ineligibility_cause(self):
        first = await self.eligible_parent()
        baseline = self.store.get('account_status')
        accounts = self.store.get('accounts')
        cases = ('reserve', 'quota', 'signed_out', 'disabled', 'threshold')
        for cause in cases:
            with self.subTest(cause=cause):
                self.store.put('accounts', accounts)
                self.store.put('account_status', baseline)
                (self.root / 'local-overrides.json').write_text('{}')
                self.store.put('account_blocks', {})
                if cause in ('reserve', 'threshold'):
                    (self.root / 'local-overrides.json').write_text(json.dumps({'account_switch_thresholds': {'a': .8}}))
                    first.router.record_rate_limit('a', {'rateLimitType': 'seven_day',
                        'status': 'allowed_warning', 'utilization': .85, 'resetsAt': time.time() + 3600})
                elif cause == 'quota':
                    first.router.record_rate_limit('a', {'status': 'rejected', 'resetsAt': time.time() + 3600}, True)
                elif cause == 'signed_out':
                    self.status('a', identity={'logged_in': False})
                else:
                    changed = self.store.get('accounts')
                    changed['a']['enabled'] = False
                    self.store.put('accounts', changed)
                self.assertEqual(first._account_move(), 'b')
                self.assertTrue(first.movable())

    async def test_native_parent_waits_for_background_tasks_before_moving(self):
        first = await self.eligible_parent()
        self.assertEqual(await first.send(str(uuid.uuid4()), 'bg'), 'received')
        await self.until(lambda: first.protocol.background and self.turns == ['done:bg'])
        first.router.record_rate_limit('a', {'rateLimitType': 'seven_day',
            'status': 'allowed_warning', 'utilization': .96, 'resetsAt': time.time() + 3600})
        self.assertTrue(first.movable())
        self.assertFalse(first.idle())
        self.assertEqual(await first.send(str(uuid.uuid4()), 'bg-done'), 'received')
        await first.wait_closed()
        self.assertTrue(first.rejected)

    async def test_native_parent_stays_when_no_other_account_is_eligible(self):
        first = await self.eligible_parent()
        for alias in ('a', 'b'):
            first.router.record_rate_limit(alias, {'status': 'rejected', 'resetsAt': time.time() + 3600}, True)
        self.assertIsNone(first._account_move())
        self.assertFalse(first.movable())

    async def test_native_parent_send_relaunches_before_writing_when_ineligible_and_idle(self):
        first = await self.eligible_parent()
        self.assertEqual(await first.send(str(uuid.uuid4()), 'one'), 'received')
        await self.until(first.idle)
        inputs = self.inputs()
        first.router.record_rate_limit('a', {'status': 'rejected', 'resetsAt': time.time() + 3600}, True)
        self.assertEqual(await first.send(str(uuid.uuid4()), 'next'), 'closed')
        await first.wait_closed()
        self.assertEqual(self.inputs(), inputs)

    async def test_hard_rejection_with_background_tasks_tells_the_parent(self):
        first = await self.eligible_parent()
        self.assertEqual(await first.send(str(uuid.uuid4()), 'bg'), 'received')
        await self.until(lambda: first.protocol.background and self.turns == ['done:bg'])
        self.assertEqual(await first.send(str(uuid.uuid4()), 'quota'), 'received')
        await first.wait_closed()
        callbacks = [row['text'] for row in self.store.messages_pending() if row['kind'] == 'callback']
        notes = [text for text in callbacks if text.startswith('Background tasks stopped')]
        self.assertEqual(len(notes), 1)
        self.assertIn('Check their state', notes[0])
        first._settle([])
        self.assertEqual(len([row for row in self.store.messages_pending() if row['text'].startswith('Background tasks stopped')]), 1)

    def test_public_fingerprint_has_no_extension_keys(self):
        self.assertEqual(CoordinatorSession._launch_inputs(self.store, 'model', 'instructions', None).fingerprint,
                         CoordinatorSession._launch_inputs(self.store, 'model', 'instructions', None, {}).fingerprint)
