import asyncio
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch
import uuid

from coordinator.providers import ProviderRunner
from coordinator.service import Service
from coordinator.session import CoordinatorSession
from coordinator.store import Store
from tests.support import isolate_shared_mcp_home, read_jsonl, stop_test_hosts, until


FAKE = '''#!/usr/bin/env python3
import json,os,sys,time
from pathlib import Path

root=Path(__file__).parent
sid=sys.argv[sys.argv.index('--session-id')+1] if '--session-id' in sys.argv else sys.argv[sys.argv.index('--resume')+1]
sid=Path(sid).stem if '/' in sid else sid
profile=os.environ.get('CLAUDE_CONFIG_DIR')
if '--session-id' in sys.argv:
    transcript=Path(profile)/'projects'/'test'/(sid+'.jsonl')
    transcript.parent.mkdir(parents=True,exist_ok=True)
    transcript.touch()
with (root/'launches.jsonl').open('a') as log:
    log.write(json.dumps({'argv':sys.argv[1:],'profile':profile})+'\\n')
def emit(event):
    print(json.dumps(dict(event,session_id=sid)),flush=True)
emit({'type':'system','subtype':'init'})
for line in sys.stdin:
    event=json.loads(line)
    text=event['message']['content']
    print(json.dumps({**event,'isReplay':True}),flush=True)
    if text=='bg-work':
        emit({'type':'system','subtype':'background_tasks_changed','tasks':[{'task_id':'b1','task_type':'local_bash','description':'build'}]})
        emit({'type':'result','subtype':'success','result':'done:bg-work'})
        while not (root/'bg-finish').exists():
            time.sleep(.02)
        for auto in ({'type':'system','subtype':'background_tasks_changed','tasks':[]},
                     {'type':'system','subtype':'task_notification','task_id':'b1','status':'completed'},
                     {'type':'system','subtype':'init'},
                     {'type':'assistant','message':{'role':'assistant','content':[{'type':'tool_use','id':'t1','name':'Bash','input':{}}]}},
                     {'type':'user','message':{'role':'user','content':[{'type':'tool_result','tool_use_id':'t1','content':'ok'}]}},
                     {'type':'assistant','message':{'role':'assistant','content':[{'type':'text','text':'build finished'}]}},
                     {'type':'result','subtype':'success','result':'done:auto'}):
            emit(auto)
            if auto['type']=='assistant':
                (root/'auto.started').touch()
                while (root/'hold-auto').exists() and not (root/'release-auto').exists():
                    time.sleep(.02)
        (root/'auto.finished').touch()
        continue
    if text=='slow':
        (root/'slow.started').touch()
        while not (root/'slow.go').exists():
            time.sleep(.02)
        emit({'type':'assistant','message':{'role':'assistant','content':[{'type':'text','text':'working'}]}})
        emit({'type':'result','subtype':'success','result':'done:slow'})
        continue
    emit({'type':'result','subtype':'success','result':'done:'+text})
'''


class ParentTurnTests(unittest.IsolatedAsyncioTestCase):
    """A parent whose account is full must not be stopped while Claude runs the turn a finished background task starts."""

    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)
        isolate_shared_mcp_home(self, self.root)
        self.addCleanup(stop_test_hosts, self.root)
        binary = self.root / 'claude'
        binary.write_text(FAKE)
        binary.chmod(0o700)
        self.store = Store(self.root / 'state')
        now = time.time()
        for alias in ('a', 'b'):
            (self.root / alias).mkdir()
        with self.store.db:
            self.store.put('accounts', {alias: {'config_dir': str(self.root / alias), 'enabled': True}
                                        for alias in ('a', 'b')})
            self.store.put('account_status', {alias: {
                'identity': {'email': alias + '@example.com', 'logged_in': True}, 'observed_at': now,
                'usage': {'five_hour': {'utilization': 10},
                          'seven_day': {'utilization': 10, 'resets_at': now + (3600 if alias == 'a' else 7200)}}}
                for alias in ('a', 'b')})
        self.runner = ProviderRunner(self.store.directory, {'claude': str(binary)})
        self.turns = []
        self.session = None

    async def asyncTearDown(self):
        if self.session:
            await self.session.stop()
        self.store.close()

    async def test_full_account_waits_for_the_turn_a_finished_background_task_starts(self):
        self.session = await CoordinatorSession.start(self.store, self.runner, self.root, 'model', 'instructions',
                                                      on_turn=self.turns.append)
        self.assertEqual(self.session.account_alias, 'a')
        self.assertEqual(await self.session.send(str(uuid.uuid4()), 'bg-work'), 'received')
        await until(lambda: self.turns == ['done:bg-work'])
        status = self.store.get('account_status')
        status['a']['usage']['five_hour']['utilization'] = 96
        with self.store.db:
            self.store.put('account_status', status)
        (self.root / 'bg-finish').touch()
        await until(lambda: self.turns == ['done:bg-work', 'done:auto'], 'the background completion turn')
        self.assertEqual(self.turns, ['done:bg-work', 'done:auto'],
                         'the parent was stopped for rotation before Claude finished the turn that the '
                         'background task completion started (rejected=%s closed=%s)' %
                         (self.session.rejected, self.session.closed))

    async def test_owner_message_to_a_reattached_mid_turn_parent_on_a_full_account_does_not_stop_the_turn(self):
        first = await CoordinatorSession.start(self.store, self.runner, self.root, 'model', 'instructions',
                                               on_turn=self.turns.append)
        self.assertEqual(await first.send(str(uuid.uuid4()), 'slow'), 'received')
        await until(lambda: (self.root / 'slow.started').exists() and first.busy)
        await first.detach()
        status = self.store.get('account_status')
        status['a']['usage']['five_hour']['utilization'] = 96
        with self.store.db:
            self.store.put('account_status', status)
        self.session = await CoordinatorSession.start(self.store, self.runner, self.root, 'model', 'instructions',
                                                      on_turn=self.turns.append)
        self.assertEqual(self.session.host_id, first.host_id)
        receipt = await self.session.send(str(uuid.uuid4()), 'hello')
        (self.root / 'slow.go').touch()
        await until(lambda: 'done:slow' in self.turns, 'the reattached slow turn')
        self.assertIn('done:slow', self.turns,
                      'send() stopped the reattached parent mid-turn (receipt=%s rejected=%s closed=%s)' %
                      (receipt, self.session.rejected, self.session.closed))

    async def test_owner_message_during_the_background_completion_turn_does_not_stop_it(self):
        (self.root / 'hold-auto').touch()
        self.session = await CoordinatorSession.start(self.store, self.runner, self.root, 'model', 'instructions',
                                                      on_turn=self.turns.append)
        self.assertEqual(await self.session.send(str(uuid.uuid4()), 'bg-work'), 'received')
        await until(lambda: self.turns == ['done:bg-work'])
        (self.root / 'bg-finish').touch()
        await until(lambda: (self.root / 'auto.started').exists())
        await until(lambda: self.session.turn_open and self.session.protocol.background_completion,
                    'the background completion turn to start')
        status = self.store.get('account_status')
        status['a']['usage']['five_hour']['utilization'] = 96
        with self.store.db:
            self.store.put('account_status', status)
        receipt = await self.session.send(str(uuid.uuid4()), 'hello')
        (self.root / 'release-auto').touch()
        await until(lambda: 'done:auto' in self.turns, 'the steered background completion turn')
        self.assertIn('done:auto', self.turns,
                      'send() stopped the parent during the background completion turn (receipt=%s rejected=%s)' %
                      (receipt, self.session.rejected))

    async def test_worker_result_recovery_across_parent_restart_delivers_once(self):
        async def recover(recorded):
            root = self.root / ('recorded' if recorded else 'unrecorded')
            root.mkdir()
            binary = root / 'claude'
            source = FAKE.replace(
                "    text=event['message']['content']",
                "    text=event['message']['content']\n"
                "    with (root/'inputs.jsonl').open('a') as log:\n"
                "        log.write(json.dumps({'pid':os.getpid(),'session_id':sid,'uuid':event.get('uuid'),"
                "'text':text})+'\\n')")
            branch = "    if 'kind=worker_result' in text:\n"
            if recorded:
                branch += "        with transcript.open('a') as log:\n"
                branch += "            log.write(json.dumps({'type':'user','uuid':event['uuid']})+'\\n')\n"
            branch += "        (root/'result.started').touch()\n"
            branch += "        while not (root/'result.go').exists():\n"
            branch += "            time.sleep(.02)\n"
            branch += "        emit({'type':'result','subtype':'success','result':'done:worker-result'})\n"
            branch += "        continue\n"
            binary.write_text(source.replace("    if text=='slow':", branch + "    if text=='slow':"))
            binary.chmod(0o700)
            state = Store(root / 'state')
            now = time.time()
            profile = root / 'profile'
            profile.mkdir()
            topic = 'home'
            with state.db:
                state.put('owner', 7)
                state.put('group', 100)
                state.put('coordinator_home_topic', topic)
                state.put('accounts', {'primary': {'config_dir': str(profile), 'enabled': True}})
                state.put('account_status', {'primary': {
                    'identity': {'email': 'primary@example.com', 'logged_in': True}, 'observed_at': now,
                    'usage': {'five_hour': {'utilization': 10},
                              'seven_day': {'utilization': 10, 'resets_at': now + 3600}}}})
                state.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES(?,?,?,?,?,1)',
                                 (topic, 100, 1, 'Home', str(root)))
            runner = ProviderRunner(state.directory, {'claude': str(binary)})
            service = Service(state, None, runner, root)
            session = None
            restarted_state = None
            restarted_service = None
            restarted_session = None
            try:
                task = state.task_create(topic, 'Build service', worktree=str(root))
                with state.db:
                    worker = state.db.execute('''INSERT INTO workers
                        (task,topic,provider,prompt,cwd,workspace,created,updated)
                        VALUES (?,?,'claude','complete the task',?,'{}',?,?)''',
                        (task['id'], topic, str(root), now, now)).lastrowid
                with patch.object(runner, 'run', wraps=runner.run) as provider_run:
                    self.assertTrue(await service.workers_once())
                    await asyncio.gather(*service.worker_tasks.values())
                    result = dict(state.db.execute('SELECT * FROM messages WHERE source_worker=?',
                                                   (worker,)).fetchone())
                    self.assertEqual(result['kind'], 'worker_result')
                    session = await service.start_session(topic)
                    await service.feed_once()
                    await until(lambda: (root / 'result.started').exists() and
                                     result['id'] in session._turn_rows)
                    await asyncio.gather(*service.feed_tasks.values())
                    self.assertTrue(session.busy)
                    self.assertEqual(state.db.execute('SELECT delivered FROM messages WHERE id=?',
                                                       (result['id'],)).fetchone()[0], 'received')
                    session.relaunch(session._turn_rows)
                    await session.wait_closed()
                    state.close()
                    state = None
                    restarted_state = Store(root / 'state')
                    restarted_service = Service(restarted_state, None, runner, root)
                    restarted_session = await restarted_service.start_session(topic)
                    self.assertEqual(restarted_state.db.execute(
                        'SELECT delivered FROM messages WHERE id=?', (result['id'],)).fetchone()[0],
                        'received' if recorded else 'pending')
                    await restarted_service.feed_once()
                    if not recorded:
                        await until(lambda: sum(1 for event in read_jsonl(root / 'inputs.jsonl')
                            if event['uuid'] == str(uuid.uuid5(uuid.NAMESPACE_URL,
                                'torii-message:' + str(result['id']))) and
                            'kind=worker_result' in event['text']) == 2)
                        (root / 'result.go').touch()
                    await asyncio.wait_for(asyncio.gather(*restarted_service.feed_tasks.values()), 3)
                    await restarted_service.feed_once()
                    await until(restarted_session.idle)
                    message_uuid_value = str(uuid.uuid5(uuid.NAMESPACE_URL,
                                                       'torii-message:' + str(result['id'])))
                    inputs = read_jsonl(root / 'inputs.jsonl')
                    results = [item for item in inputs if item['uuid'] == message_uuid_value and
                               'kind=worker_result' in item['text']]
                    callbacks = [item['text'] for item in inputs if 'kind=callback' in item['text']]
                    self.assertEqual(provider_run.await_count, 1)
                    self.assertEqual(restarted_state.db.execute(
                        "SELECT COUNT(*) FROM messages WHERE source_worker=? AND kind='worker_result'",
                        (worker,)).fetchone()[0], 1)
                    self.assertEqual(restarted_state.db.execute(
                        'SELECT status FROM workers WHERE id=?', (worker,)).fetchone()[0], 'done')
                    if recorded:
                        self.assertEqual(len(results), 1)
                        self.assertEqual(len(callbacks), 1)
                        self.assertIn('message=%d' % result['id'], callbacks[0])
                    else:
                        self.assertEqual(len(results), 2)
                        self.assertNotEqual(results[0]['pid'], results[1]['pid'])
                        self.assertEqual(callbacks, [])
                    self.assertFalse(await restarted_service.workers_once())
                    self.assertEqual(provider_run.await_count, 1)
            finally:
                for current_service in (service, restarted_service):
                    if current_service:
                        for task in current_service.feed_tasks.values():
                            if not task.done():
                                task.cancel()
                        if current_service.feed_tasks:
                            await asyncio.gather(*current_service.feed_tasks.values(), return_exceptions=True)
                for current_session in (session, restarted_session):
                    if current_session and not current_session.closed:
                        await current_session.stop()
                if state:
                    state.close()
                if restarted_state:
                    restarted_state.close()

        await recover(True)
        await recover(False)
