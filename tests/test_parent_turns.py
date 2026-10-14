import asyncio
from pathlib import Path
import tempfile
import time
import unittest
import uuid

from coordinator.providers import ProviderRunner
from coordinator.session import CoordinatorSession
from coordinator.store import Store
from tests.support import isolate_shared_mcp_home, stop_test_hosts


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
            time.sleep(.2)
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
        time.sleep(.3)
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

    async def until(self, predicate, timeout=5):
        async def wait():
            while not predicate():
                await asyncio.sleep(.01)
        await asyncio.wait_for(wait(), timeout)

    async def test_full_account_waits_for_the_turn_a_finished_background_task_starts(self):
        self.session = await CoordinatorSession.start(self.store, self.runner, self.root, 'model', 'instructions',
                                                      on_turn=self.turns.append)
        self.assertEqual(self.session.account_alias, 'a')
        self.assertEqual(await self.session.send(str(uuid.uuid4()), 'bg-work'), 'received')
        await self.until(lambda: self.turns == ['done:bg-work'])
        status = self.store.get('account_status')
        status['a']['usage']['five_hour']['utilization'] = 96
        with self.store.db:
            self.store.put('account_status', status)
        (self.root / 'bg-finish').touch()
        try:
            await self.until(lambda: (self.root / 'auto.finished').exists() or self.session.closed, 10)
            await asyncio.sleep(.5)
        except asyncio.TimeoutError:
            pass
        self.assertEqual(self.turns, ['done:bg-work', 'done:auto'],
                         'the parent was stopped for rotation before Claude finished the turn that the '
                         'background task completion started (rejected=%s closed=%s)' %
                         (self.session.rejected, self.session.closed))

    async def test_owner_message_to_a_reattached_mid_turn_parent_on_a_full_account_does_not_stop_the_turn(self):
        first = await CoordinatorSession.start(self.store, self.runner, self.root, 'model', 'instructions',
                                               on_turn=self.turns.append)
        self.assertEqual(await first.send(str(uuid.uuid4()), 'slow'), 'received')
        await self.until(lambda: (self.root / 'slow.started').exists() and first.busy)
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
        try:
            await self.until(lambda: 'done:slow' in self.turns or self.session.closed, 5)
            await asyncio.sleep(.5)
        except asyncio.TimeoutError:
            pass
        self.assertIn('done:slow', self.turns,
                      'send() stopped the reattached parent mid-turn (receipt=%s rejected=%s closed=%s)' %
                      (receipt, self.session.rejected, self.session.closed))

    async def test_owner_message_during_the_background_completion_turn_does_not_stop_it(self):
        (self.root / 'hold-auto').touch()
        self.session = await CoordinatorSession.start(self.store, self.runner, self.root, 'model', 'instructions',
                                                      on_turn=self.turns.append)
        self.assertEqual(await self.session.send(str(uuid.uuid4()), 'bg-work'), 'received')
        await self.until(lambda: self.turns == ['done:bg-work'])
        (self.root / 'bg-finish').touch()
        await self.until(lambda: (self.root / 'auto.started').exists())
        await asyncio.sleep(.3)
        status = self.store.get('account_status')
        status['a']['usage']['five_hour']['utilization'] = 96
        with self.store.db:
            self.store.put('account_status', status)
        receipt = await self.session.send(str(uuid.uuid4()), 'hello')
        (self.root / 'release-auto').touch()
        try:
            await self.until(lambda: 'done:auto' in self.turns or self.session.closed, 10)
            await asyncio.sleep(.5)
        except asyncio.TimeoutError:
            pass
        self.assertIn('done:auto', self.turns,
                      'send() stopped the parent during the background completion turn (receipt=%s rejected=%s)' %
                      (receipt, self.session.rejected))
