"""Opt-in native switching contract. All Claude processes use a local fake and throwaway profiles."""

import asyncio
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch

from coordinator import extension
from coordinator.accounts import AccountBroker, LIMIT_CONTINUATION
from coordinator.account_status import _native
from coordinator.providers import ProviderRunner
from coordinator.native_protocol import RunControl
from coordinator.session import CoordinatorSession, LOST_TURN, message_tag, message_uuid
from coordinator.store import Store
from coordinator.tldr import summarize
from tests.support import isolate_shared_mcp_home, stop_test_hosts, until


FAKE_CLAUDE = '''#!/usr/bin/env python3
import json,os,sys,time
from pathlib import Path
root=Path(__file__).parent
profile=Path(os.environ['CLAUDE_CONFIG_DIR'])
def emit(event):
    print(json.dumps(event),flush=True)
def log(event):
    with (root/'calls.jsonl').open('a') as stream:
        stream.write(json.dumps(event)+'\\n')
log({'argv':sys.argv[1:],'keys':sorted(os.environ),'profile':profile.name})
if sys.argv[1:3]==['auth','status']:
    emit({'loggedIn':True,'email':profile.name+'@example.invalid'})
    sys.exit(0)
if '--output-format' in sys.argv and sys.argv[sys.argv.index('--output-format')+1]=='json':
    sys.stdin.read()
    emit({'result':'Done'})
    sys.exit(0)
resume=sys.argv[sys.argv.index('--resume')+1] if '--resume' in sys.argv else None
sid=Path(resume).stem if resume else sys.argv[sys.argv.index('--session-id')+1]
transcript=Path(resume) if resume and '/' in resume else profile/'projects'/'contract'/(sid+'.jsonl')
transcript.parent.mkdir(parents=True,exist_ok=True)
transcript.touch(exist_ok=True)
emit({'type':'system','subtype':'init','session_id':sid})
parent='--append-system-prompt' in sys.argv
for line in sys.stdin:
    event=json.loads(line)
    text=event['message']['content']
    log({'input':text,'profile':profile.name})
    action=text.rsplit('] ',1)[-1] if parent else text
    with transcript.open('a') as stream:
        stream.write(json.dumps({'type':'user','uuid':event['uuid']})+'\\n')
    emit({'type':'command_lifecycle','command_uuid':event['uuid'],'state':'started','session_id':sid})
    emit({**event,'isReplay':True})
    rejected=(action.endswith('reject') and profile.name=='a')
    if action=='background' or action=='soft-limit':
        emit({'type':'system','subtype':'background_tasks_changed','tasks':[{'task_id':'helper','description':'background check'}]})
    if action=='finish-background':
        emit({'type':'system','subtype':'background_tasks_changed','tasks':[]})
    used=.96 if action in ('soft-limit','finish-background') else .5
    emit({'type':'rate_limit_event','session_id':sid,'rate_limit_info':{
        'rateLimitType':'seven_day','status':'rejected' if rejected else 'allowed',
        'utilization':used,'resetsAt':time.time()+3600}})
    emit({'type':'result','subtype':'error' if rejected else 'success','session_id':sid,
          'is_error':rejected,'result':'Usage limit reached' if rejected else 'Done'})
    emit({'type':'command_lifecycle','command_uuid':event['uuid'],'state':'completed','session_id':sid})
    if not parent:
        break
'''


@unittest.skipUnless(os.environ.get('TORII_SWITCHING_CONTRACT') == '1',
                     'Set TORII_SWITCHING_CONTRACT=1 for the fake Claude switching contract')
class ClaudeSwitchingContract(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='torii-switching-')
        self.root = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)
        self.addCleanup(stop_test_hosts, self.root)
        isolate_shared_mcp_home(self, self.root)
        extension.use(extension.Native())
        self.addCleanup(extension.use, extension.Native())
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        self.profiles = self.root / 'profiles'
        self.profiles.mkdir()
        for alias in ('a', 'b'):
            (self.profiles / alias).mkdir()
        self.store.put('accounts', {alias: {'config_dir': str(self.profiles / alias), 'enabled': True}
                                    for alias in ('a', 'b')})
        self.reset_accounts()
        self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                              ('home', -100, 1, 'Home', str(self.root)))
        self.store.put('coordinator_home_topic', 'home')
        self.binary = self.root / 'claude'
        self.binary.write_text(FAKE_CLAUDE)
        self.binary.chmod(0o700)
        security = self.root / 'security'
        security.write_text('#!/bin/sh\nprintf attempted >> "$(dirname "$0")/security-called"\nexit 99\n')
        security.chmod(0o700)
        self.environment = patch.dict(os.environ, {'PATH': str(self.root) + os.pathsep + os.environ['PATH']})
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.expected_keys = set(os.environ) | {'CLAUDE_CONFIG_DIR', 'HOME', 'PATH'}
        self.runner = ProviderRunner(self.store.directory, binaries={'claude': str(self.binary)},
                                     account_broker=AccountBroker(self.store))
        self.sessions = []
        self.addAsyncCleanup(self.close_sessions)

    def reset_accounts(self):
        now = time.time()
        self.store.put('account_blocks', {})
        self.store.put('account_status', {alias: {'identity': {'logged_in': True, 'email': alias + '@example.invalid'},
            'observed_at': now, 'usage': {'seven_day': {'utilization': 50, 'resets_at': now + offset}}}
            for alias, offset in (('a', 3600), ('b', 7200))})

    async def close_sessions(self):
        for session in self.sessions:
            await session.stop()

    async def start(self):
        session = await CoordinatorSession.start(self.store, self.runner, self.root, 'model', 'instructions',
                                                  receipt_timeout=10, topic='home')
        self.sessions.append(session)
        return session

    async def send(self, session, text):
        row = self.store.message_save('home', 'owner', text)
        receipt = await session.send(message_uuid(row['id']), message_tag(self.store.topic('home'), row) + text, row['id'])
        self.assertEqual(receipt, 'received')
        return row

    def calls(self):
        return [json.loads(line) for line in (self.root / 'calls.jsonl').read_text().splitlines()]

    async def test_switching_contract(self):
        original_popen = subprocess.Popen
        security_calls = []

        def guarded_popen(argv, *args, **kwargs):
            if isinstance(argv, (list, tuple)) and Path(argv[0]).name == 'security':
                security_calls.append(True)
                raise AssertionError('Native mode called security')
            return original_popen(argv, *args, **kwargs)

        with patch('subprocess.Popen', side_effect=guarded_popen):
            identity = await _native([str(self.binary), 'auth', 'status', '--json'],
                                     self.runner.account_broker.environment('a'), 10)
            self.assertEqual(identity['email'], 'a@example.invalid')
            first = await self.start()
            await self.send(first, 'hello')
            await until(first.idle)
            self.assertEqual(first.account_alias, 'a')
            status = self.store.get('account_status')
            status['b']['usage']['seven_day']['resets_at'] = time.time() + 600
            self.store.put('account_status', status)
            self.assertEqual(first.router.activate(), 'b')
            await self.send(first, 'sooner')
            await until(first.idle)
            self.assertFalse(first.rejected)
            await self.send(first, 'soft-limit')
            await until(lambda: not first.turn_open and first.protocol.background)
            self.assertFalse(first.rejected)
            self.assertEqual(first._account_move(), 'b')
            await self.send(first, 'finish-background')
            await asyncio.wait_for(first.wait_closed(), 20)
            second = await self.start()
            self.assertEqual(second.account_alias, 'b')
            await until(lambda: len([row for row in self.calls() if 'argv' in row and '--append-system-prompt' in row['argv']]) == 2)
            launches = [row for row in self.calls() if 'argv' in row and '--append-system-prompt' in row['argv']]
            self.assertEqual([row['profile'] for row in launches], ['a', 'b'])
            resume = Path(launches[-1]['argv'][launches[-1]['argv'].index('--resume') + 1])
            self.assertEqual(resume.parent.parent.parent, self.profiles / 'b')
            self.assertTrue(resume.samefile(first.router.transcript(first.session_id)))
            await second.stop()
            self.reset_accounts()
            third = await self.start()
            self.assertEqual(third.account_alias, 'a')
            await self.send(third, 'background')
            await until(lambda: not third.turn_open and third.protocol.background)
            rejected = await self.send(third, 'reject')
            await asyncio.wait_for(third.wait_closed(), 20)
            callbacks = [row['text'] for row in self.store.messages_pending() if row['kind'] == 'callback']
            self.assertIn(LOST_TURN % ('message=%d' % rejected['id']), callbacks)
            self.assertEqual(len([text for text in callbacks if text.startswith('Background tasks stopped')]), 1)
            fourth = await self.start()
            self.assertEqual(fourth.account_alias, 'b')
            await fourth.stop()
            self.reset_accounts()
            result = await self.runner.account_broker.run(self.runner.run, 'claude', 'worker-reject',
                                                          self.root, None, fresh=True, control=RunControl())
            self.assertTrue(result.success, result.error)
            worker_launches = [row for row in self.calls() if 'argv' in row and '--input-format' in row['argv']
                               and '--append-system-prompt' not in row['argv']]
            self.assertEqual([row['profile'] for row in worker_launches], ['a', 'b'])
            self.assertIn('--resume', worker_launches[1]['argv'])
            self.assertTrue(any(LIMIT_CONTINUATION in row.get('input', '') and row['profile'] == 'b'
                                for row in self.calls()))
            self.store.message_save('home', 'owner', 'Summarize', telegram_message=100)
            outbox = self.store.enqueue_report('home', 'One update')
            self.store.delivered(outbox, 101)
            self.assertEqual(await summarize(self.store, 'home', 102, self.runner.account_broker,
                                            self.runner.extension, str(self.binary)), 'Done')
            summaries = [row for row in self.calls() if 'argv' in row and '--safe-mode' in row['argv']]
            self.assertEqual(len(summaries), 1)
            self.assertIn('CLAUDE_CONFIG_DIR', summaries[0]['keys'])
            for row in self.calls():
                if 'keys' in row:
                    unexpected = set(row['keys']) - self.expected_keys
                    self.assertTrue(all(key.startswith('TORII_') for key in unexpected), unexpected)
            self.assertEqual(security_calls, [])
            self.assertFalse((self.root / 'security-called').exists())
