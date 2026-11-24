import asyncio
import importlib.util
from dataclasses import asdict
import json
import os
import sqlite3
from pathlib import Path
import tempfile
import subprocess
import sys
import unittest
import uuid
from unittest.mock import AsyncMock, patch

from coordinator import host_os
from coordinator.extension import available
from coordinator.providers import ProviderRunner, retry_locked
from coordinator.accounts import AccountBroker
from coordinator.store import Store

from tests.support import settle, short_host_stop, stop_test_hosts, until, until_exists, until_text

class ProviderTestsSupport:
    def setUp(self):
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
        overrides.write_text('{}')
        selection = patch('coordinator.accounts.LOCAL_OVERRIDES_PATH', overrides)
        selection.start()
        self.addCleanup(selection.stop)
        self.binary = self.root / 'fake'
        self.sid = str(uuid.uuid4())
        self.store = Store(self.root / 'state')
        self.store.put('chat_config:home', {'mcp_servers': ['example-mcp', 'other-mcp']})
        probe = patch('coordinator.providers.codex_inherited', new_callable=AsyncMock, return_value={})
        probe.start()
        self.addCleanup(probe.stop)
        self.profile = self.root / 'profile'
        self.profile.mkdir()
        self.store.put('accounts', {'profile': {'config_dir': str(self.profile), 'enabled': True}})
        self.store.put('account_status', {'profile': {'observed_at': __import__('time').time(),
                                                      'identity': {'logged_in': True, 'email': 'test@example.com'}}})
        self.broker = AccountBroker(self.store)
        self.runner = ProviderRunner(self.root / 'state', binaries={'claude': str(self.binary), 'codex': str(self.binary)},
                                     account_broker=self.broker)
        if importlib.util.find_spec('coordinator.worker_hooks'):
            trust = patch('coordinator.worker_hooks.codex_trust_override', new_callable=AsyncMock,
                          return_value='hooks.state."fixture".trusted_hash="sha256:fixture"')
            trust.start()
            self.addCleanup(trust.stop)

    def tearDown(self):
        self.store.close()

    def fake(self, events, exit_code=0, wait=False):
        script = '#!/usr/bin/env python3\nimport sys,json,time\n'
        script += 'data=sys.stdin.read()\n'
        script += 'assert "private-prompt" not in " ".join(sys.argv)\n'
        script += 'print(json.dumps({"argv":sys.argv,"input":data}),file=sys.stderr)\n'
        for event in events:
            script += 'print(' + repr(json.dumps(event)) + ',flush=True)\n'
        if wait:
            script += 'time.sleep(60)\n'
        script += 'sys.exit(%d)\n' % exit_code
        self.binary.write_text(script)
        self.binary.chmod(0o700)

    async def run_provider(self, provider='claude', fresh=False, **kw):
        kw.setdefault('topic', 'home')
        if provider == 'claude':
            kw.setdefault('account_alias', 'profile')
        return await self.runner.run(provider, 'private-prompt', self.root, None if fresh else self.sid, fresh=fresh, **kw)

    def secret_fake(self, session=None):
        sid = session or self.sid
        self.binary.write_text('''#!/usr/bin/env python3
import json,os,sys
from pathlib import Path
sys.stdin.read()
value=os.environ['FAKE_SECRET']
Path(%r).write_text(str(len(value)))
print(json.dumps({'type':'assistant','session_id':%r,'text':'token '+value}),flush=True)
print('raw '+value,file=sys.stderr,flush=True)
print(json.dumps({'type':'result','subtype':'success','session_id':%r,'result':'used '+value,
    'structured_output':{'echo':value,'nested':[value]}}),flush=True)
''' % (str(self.root / 'seen'), sid, sid))
        self.binary.chmod(0o700)


class ProviderTests(ProviderTestsSupport, unittest.IsolatedAsyncioTestCase):
    async def test_claude_worker_installs_account_hook_and_keeps_existing(self):
        settings = self.profile / 'settings.json'
        existing = {'matcher': 'Bash', 'hooks': [{'type': 'command', 'command': 'existing-guard'}]}
        settings.write_text(json.dumps({'hooks': {'PreToolUse': [existing]}}))
        self.fake([{'type': 'result', 'subtype': 'success', 'session_id': self.sid, 'result': 'done'}])
        result = await asyncio.wait_for(self.run_provider(worker_id=12), 15)
        self.assertTrue(result.success, result.error)
        hooks = json.loads(settings.read_text())['hooks']['PreToolUse']
        self.assertEqual(hooks[0], existing)
        self.assertEqual(hooks[1]['matcher'], 'Bash|Read|Grep' + ('|Write' if available('docs') else ''))
        self.assertIn('secret_guard.py', hooks[1]['hooks'][0]['command'])

    async def test_retry_rolls_back_stale_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory))
            other = sqlite3.connect(str(Path(directory) / 'state.sqlite'), timeout=0.2)
            try:
                store.db.execute('BEGIN')
                store.db.execute('SELECT count(*) FROM settings').fetchone()
                other.execute("INSERT INTO settings(key,value) VALUES ('retry-test','1')")
                other.commit()
                def write():
                    store.db.execute("INSERT INTO settings(key,value) VALUES ('retry-test-2','2')")
                await asyncio.wait_for(retry_locked(write, retry_store=store), 5)
                self.assertIsNotNone(store.db.execute("SELECT * FROM settings WHERE key='retry-test-2'").fetchone())
                self.assertIsNotNone(store.db.execute("SELECT * FROM problems WHERE code='db-locked'").fetchone())
            finally:
                other.close()
                store.db.close()

    async def test_retry_stops_after_deadline(self):
        def locked():
            raise sqlite3.OperationalError('database is locked')
        with patch('coordinator.providers.LOCK_RETRY_SECONDS', 0):
            with self.assertRaises(sqlite3.OperationalError):
                await retry_locked(locked)

    async def test_worker_exit_reaps_pipe_holding_child_without_reporting(self):
        from coordinator.host import alive
        pidfile = self.root / 'child.pid'
        self.binary.write_text('''#!/usr/bin/env python3
import json,subprocess,sys
from pathlib import Path
sys.stdin.read()
child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
Path(%r).write_text(str(child.pid))
print(json.dumps({'type': 'result', 'subtype': 'success', 'session_id': %r, 'result': 'finished'}), flush=True)
''' % (str(pidfile), self.sid))
        self.binary.chmod(0o700)
        result = await asyncio.wait_for(self.run_provider(worker_id=12), 10)
        self.assertTrue(result.success, result.error)
        self.assertEqual(result.text, 'finished')
        self.assertFalse(alive(int(pidfile.read_text())))

    async def test_background_reap_reporting_and_empty_run(self):
        cases = [('claude', True, False), ('claude', False, False), ('codex', True, False)]
        if host_os.macos():
            cases.append(('claude', True, True))
        for provider, spawn, detached in cases:
            with self.subTest(provider=provider, spawn=spawn, detached=detached):
                pidfile = self.root / ('child-%s-%s-%s' % (provider, spawn, detached))
                self.binary.write_text('''#!/usr/bin/env python3
import json,subprocess,sys
from pathlib import Path
sys.stdin.read()
if %r:
    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'],
        stdin=subprocess.DEVNULL, stdout=None, stderr=subprocess.DEVNULL,
        start_new_session=%r)
    Path(%r).write_text(str(child.pid))
events = %r
for event in events:
    print(json.dumps(event),flush=True)
''' % (spawn, detached, str(pidfile),
       ([{'type': 'result', 'subtype': 'success', 'session_id': self.sid, 'result': 'finished'}]
        if provider == 'claude' else [{'type': 'thread.started', 'thread_id': self.sid},
              {'type': 'item.completed', 'item': {'type': 'agent_message', 'text': 'finished'}},
              {'type': 'turn.completed'}])))
                self.binary.chmod(0o700)
                result = await asyncio.wait_for(self.run_provider(provider=provider, worker_id=12), 25)
                self.assertTrue(result.success, result.error)
                self.assertEqual(result.text, 'finished')
                if spawn:
                    pid = pidfile.read_text()
                    self.assertEqual(result.failure_code, 'background_killed')
                    self.assertIn(pid, result.failure_detail)
                    self.assertIn(' -c ', result.failure_detail)
                    state = subprocess.run(['/bin/ps', '-p', pid, '-o', 'stat='],
                                           capture_output=True, text=True).stdout.strip()
                    self.assertTrue(not state or state.startswith('Z'), state)
                    with self.store.db:
                        self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                                              (pid, -100, int(pid), 'Test', str(self.root)))
                        task = self.store.task_create(pid, 'Work', worktree=str(self.root))
                        worker = self.store.db.execute('''INSERT INTO workers
                            (task,topic,provider,prompt,cwd,workspace,created,updated)
                            VALUES (?,?,?,?,?,?,1,1)''',
                            (task['id'], pid, provider, 'work', str(self.root), '{}')).lastrowid
                    message = self.store.worker_complete(worker, asdict(result))
                    payload = json.loads(message['text'])
                    self.assertEqual(payload['failure_code'], 'background_killed')
                    self.assertIn(pid, payload['failure_detail'])
                    self.assertTrue(payload['success'])
                    self.assertEqual(payload['text'], 'finished')
                else:
                    self.assertIsNone(result.failure_code)
                    self.assertIsNone(result.failure_detail)

    async def test_background_reap_preserves_codex_failure_detail(self):
        for quota in (False, True):
            with self.subTest(quota=quota):
                error = {'detail': 'original detail'}
                if quota:
                    error['codex_error_info'] = 'usage_limit_exceeded'
                self.binary.write_text('''#!/usr/bin/env python3
import json,subprocess,sys
sys.stdin.read()
subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
print(json.dumps({'type': 'thread.started', 'thread_id': %r}), flush=True)
print(json.dumps({'type': 'turn.failed', 'error': %r}), flush=True)
''' % (self.sid, error))
                self.binary.chmod(0o700)
                result = await asyncio.wait_for(self.run_provider('codex', worker_id=12), 25)
                self.assertFalse(result.success)
                self.assertEqual(result.failure_code, 'quota_limited' if quota else 'background_killed')
                self.assertIn('original detail', result.failure_detail)
                self.assertIn('background_killed:', result.failure_detail)

    async def test_c5_workers_never_receive_topic_management_tools(self):
        self.store.put('chat_config:home', {'mcp_servers': []})
        self.fake([{'type': 'result', 'subtype': 'success', 'session_id': self.sid, 'result': 'done'}])
        for provider in ('claude', 'codex'):
            with self.subTest(provider=provider):
                if provider == 'codex':
                    self.fake([{'type': 'thread.started', 'thread_id': self.sid},
                               {'type': 'turn.completed', 'usage': {}}])
                result = await self.run_provider(provider)
                self.assertTrue(result.success, result.error)
                spec = json.loads((Path(result.log_path).parent / 'spec.json').read_text())
                args = spec['argv']
                self.assertFalse(any('mcp_servers.torii' in arg or arg in ('topic_create', 'topic_close', 'model_parent') for arg in args))
                if provider == 'claude':
                    self.assertEqual(args[args.index('--mcp-config') + 1], '{"mcpServers":{}}')

    async def test_claude_resume_and_structured(self):
        self.fake([{'type':'result','subtype':'success','session_id':self.sid,'result':'done','structured_output':{'action':'report'}}])
        result = await self.run_provider(instructions='policy')
        self.assertTrue(result.success)
        self.assertEqual(result.structured, {'action':'report'})
        self.assertEqual(result.session_id, self.sid)
        self.assertEqual(Path(result.log_path).stat().st_mode & 0o777, 0o600)
        log = Path(result.log_path).read_text()
        self.assertIn('--resume', log)
        self.assertIn('policy', log)
        self.assertNotIn('--fork-session', log)
        argv = json.loads(log.splitlines()[0])['argv']
        self.assertEqual(argv[argv.index('--effort') + 1], 'medium')
        self.assertEqual(json.loads(argv[argv.index('--settings') + 1]), {'ultracode': False,
            'claudeMdExcludes': [str(parent / name) for parent in Path.home().parents
                                for name in ('CLAUDE.md', 'CLAUDE.local.md', '.claude/CLAUDE.md')]})

    async def test_claude_worker_settings_carry_plugin_switches(self):
        for plugins in (None, {'pstack@pstack-claude': True, 'other@market': False}):
            with self.subTest(plugins=plugins):
                self.fake([{'type': 'result', 'subtype': 'success', 'session_id': self.sid, 'result': 'done'}])
                result = await self.run_provider(plugins=plugins)
                self.assertTrue(result.success, result.error)
                argv = json.loads(Path(result.log_path).read_text().splitlines()[0])['argv']
                settings = json.loads(argv[argv.index('--settings') + 1])
                self.assertEqual(settings.get('enabledPlugins'), plugins)

    async def test_claude_worker_launches_with_shared_mcp_on_fresh_and_resume(self):
        home = self.root / 'home'
        home.mkdir()
        value = 'FAKE-SHARED-MCP-HEADER-VALUE'
        entries = {name: {'type': 'http', 'url': 'http://127.0.0.1:1/' + name,
                          'headers': {'Authorization': value}}
                   for name in ('example-mcp', 'other-mcp')}
        (home / '.claude.json').write_text(json.dumps({'mcpServers': entries}))
        self.binary.write_text('''#!/usr/bin/env python3
import json,sys
from pathlib import Path
sys.stdin.read()
sid=sys.argv[sys.argv.index('--session-id')+1] if '--session-id' in sys.argv else sys.argv[sys.argv.index('--resume')+1]
config=json.loads(Path(sys.argv[sys.argv.index('--mcp-config')+1]).read_text())
value=config['mcpServers']['example-mcp']['headers']['Authorization']
print(value,file=sys.stderr,flush=True)
print(json.dumps({'type':'result','subtype':'success','session_id':sid,'result':value}),flush=True)
''')
        self.binary.chmod(0o700)
        with patch.dict(os.environ, {'HOME': str(home)}):
            for fresh in (True, False):
                result = await self.run_provider(fresh=fresh)
                self.assertTrue(result.success, result.error)
                self.assertNotIn(value, result.text)
                host = Path(result.log_path).parent
                spec = json.loads((host / 'spec.json').read_text())
                shared_mcp_path = Path(spec['argv'][-1])
                self.assertEqual(spec['argv'][-2], '--mcp-config')
                self.assertNotIn('--strict-mcp-config', spec['argv'])
                self.assertEqual(shared_mcp_path.parent, self.store.directory)
                for path in host.iterdir():
                    if path.is_file():
                        self.assertNotIn(value, path.read_text(errors='replace'), path.name)
        self.assertEqual(json.loads(shared_mcp_path.read_text()),
                         {'mcpServers': entries})
        self.assertNotIn(value, json.dumps([tuple(row) for row in self.store.db.execute(
            'SELECT * FROM problems')]))
        self.assertNotIn(value, ''.join(self.store.db.iterdump()))

    async def test_claude_worker_uses_only_its_topic_selection(self):
        self.store.put('chat_config:home', {'mcp_servers': ['example-mcp']})
        self.store.put('chat_config:other', {'mcp_servers': ['other-mcp']})
        self.fake([{'type': 'result', 'subtype': 'success', 'session_id': self.sid, 'result': 'done'}])
        selected = await self.run_provider()
        self.assertTrue(selected.success, selected.error)
        args = json.loads((Path(selected.log_path).parent / 'spec.json').read_text())['argv']
        config = json.loads(Path(args[args.index('--mcp-config') + 1]).read_text())
        self.assertEqual(list(config['mcpServers']), ['example-mcp'])
        empty = await self.run_provider(topic='empty')
        self.assertTrue(empty.success, empty.error)
        args = json.loads((Path(empty.log_path).parent / 'spec.json').read_text())['argv']
        self.assertEqual(json.loads(args[args.index('--mcp-config') + 1]), {'mcpServers': {}})

    async def test_codex_does_not_get_shared_mcp_config(self):
        home = self.root / 'home'
        home.mkdir()
        (home / '.claude.json').write_text(json.dumps({'mcpServers': {
            'example-mcp': {'type': 'http', 'url': 'http://127.0.0.1:1/work', 'headers': {}},
            'other-mcp': {'type': 'http', 'url': 'http://127.0.0.1:1/personal', 'headers': {}}}}))
        with patch.dict(os.environ, {'HOME': str(home)}):
            self.fake([{'type': 'thread.started', 'thread_id': self.sid},
                       {'type': 'item.completed', 'item': {'type': 'agent_message', 'text': 'done'}},
                       {'type': 'turn.completed'}])
            codex = await self.run_provider('codex')
        self.assertTrue(codex.success, codex.error)
        codex_args = json.loads((Path(codex.log_path).parent / 'spec.json').read_text())['argv']
        self.assertNotIn('--mcp-config', codex_args)

    async def test_default_worker_omits_shared_config(self):
        self.store.put('chat_config:home', {'mcp_servers': []})
        self.fake([{'type': 'result', 'subtype': 'success', 'session_id': self.sid, 'result': 'done'}])
        result = await self.run_provider()
        self.assertTrue(result.success, result.error)
        args = json.loads((Path(result.log_path).parent / 'spec.json').read_text())['argv']
        self.assertEqual(args[args.index('--mcp-config') + 1], '{"mcpServers":{}}')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM problems').fetchone()[0], 0)

    async def test_missing_and_invalid_shared_mcp_source_omit_stale_config(self):
        self.fake([{'type': 'result', 'subtype': 'success', 'session_id': self.sid, 'result': 'done'}])
        source = self.root / 'torii-home' / '.claude.json'
        first = await self.run_provider()
        self.assertTrue(first.success, first.error)
        source.unlink()
        missing = await self.run_provider()
        source.write_text('{')
        invalid = await self.run_provider()
        for result in (missing, invalid):
            self.assertTrue(result.success, result.error)
            args = json.loads((Path(result.log_path).parent / 'spec.json').read_text())['argv']
            self.assertEqual(args[args.index('--mcp-config') + 1], '{"mcpServers":{}}')
        self.assertEqual([tuple(row) for row in self.store.db.execute(
            "SELECT area,code,detail FROM problems WHERE area='shared-mcp' ORDER BY id")], [
            ('shared-mcp', 'source-missing',
             'entries=example-mcp,other-mcp'),
            ('shared-mcp', 'source-invalid-json',
             'entries=example-mcp,other-mcp')])

    async def test_secrets_reach_the_child_and_are_scrubbed_from_every_capture(self):
        for value in ('FAKE-ENVELOPE-VALUE-0123456789', 'FAKE"quoted\\value-0123456789'):
            with self.subTest(value=value):
                self.secret_fake()
                result = await self.run_provider(secrets={'FAKE_SECRET': value})
                self.assertTrue(result.success, result.error)
                self.assertEqual((self.root / 'seen').read_text(), str(len(value)))
                host = Path(result.log_path).parent
                files = {path.name: path.read_text() for path in host.iterdir() if path.is_file()}
                self.assertTrue({'spec.json', 'events.jsonl', 'stderr.log', 'writes.jsonl'} <= set(files))
                for name, text in files.items():
                    for form in (value, json.dumps(value)[1:-1], json.dumps(json.dumps(value)[1:-1])[1:-1]):
                        self.assertNotIn(form, text, name)
                self.assertEqual(files['events.jsonl'].count('[secret FAKE_SECRET]'), 4)
                self.assertEqual(files['stderr.log'].count('[secret FAKE_SECRET]'), 1)
                self.assertEqual(result.text, 'used [secret FAKE_SECRET]')
                self.assertEqual(result.structured, {'echo': '[secret FAKE_SECRET]', 'nested': ['[secret FAKE_SECRET]']})

    async def test_codex_secrets_require_a_private_app_server_before_creating_a_host(self):
        with patch('coordinator.providers.HostClient.launch', AsyncMock(side_effect=AssertionError('host launched'))) as launch:
            result = await self.run_provider('codex', secrets={'FAKE_SECRET': 'FAKE-SECRET-VALUE'})
        self.assertFalse(result.success)
        self.assertEqual(result.failure_code, 'start_failed')
        self.assertIn('Codex secret delivery requires a private worker app-server.', result.failure_detail)
        self.assertFalse((self.root / 'state' / 'hosts').exists())
        launch.assert_not_awaited()

    async def test_events_are_parsed_before_scrubbing(self):
        self.secret_fake()
        result = await self.run_provider(secrets={'FAKE_SECRET': self.sid})
        self.assertTrue(result.success, result.error)
        self.assertEqual(result.session_id, self.sid)
        self.assertEqual(result.text, 'used [secret FAKE_SECRET]')

    async def test_spawn_without_secrets_adds_no_names(self):
        self.binary.write_text('''#!/usr/bin/env python3
import json,os,sys
sys.stdin.read()
print(json.dumps({'type':'result','subtype':'success','session_id':%r,'result':str('FAKE_SECRET' in os.environ)}))
''' % self.sid)
        self.binary.chmod(0o700)
        result = await self.run_provider()
        self.assertTrue(result.success, result.error)
        self.assertEqual(result.text, 'False')
        spec = json.loads((Path(result.log_path).parent / 'spec.json').read_text())
        self.assertNotIn('FAKE_SECRET', spec['env'])

    async def test_last_goal_status_is_returned(self):
        self.fake([{'type': 'goal_status', 'session_id': self.sid, 'status': 'failed',
                    'condition': 'checks pass', 'reason': 'first attempt', 'iterations': 1},
                   {'type': 'goal_status', 'session_id': self.sid, 'status': 'met',
                    'condition': 'checks pass', 'reason': 'verified', 'iterations': 2},
                   {'type': 'result', 'subtype': 'success', 'session_id': self.sid, 'result': 'done'}])
        result = await self.run_provider()
        self.assertTrue(result.success)
        self.assertEqual(result.goal_status, {'status': 'met', 'condition': 'checks pass',
                                              'reason': 'verified', 'iterations': 2})

    async def test_codex_resume(self):
        self.fake([{'type':'thread.started','thread_id':self.sid}, {'type':'item.completed','item':{'type':'agent_message','text':'done'}}, {'type':'turn.completed'}])
        result = await self.run_provider('codex')
        self.assertTrue(result.success)
        self.assertEqual(result.text, 'done')
        self.assertIn('resume', Path(result.log_path).read_text())

    async def test_mismatch_is_failure(self):
        self.fake([{'type':'result','subtype':'success','session_id':str(uuid.uuid4()),'result':'done'}])
        result = await self.run_provider()
        self.assertFalse(result.success)
        self.assertEqual(result.session_id, self.sid)

    async def test_mismatched_session_reaps_child_and_releases_lock(self):
        self.fake([{'type': 'result', 'session_id': str(uuid.uuid4())}], wait=True)
        result = await asyncio.wait_for(self.run_provider(), timeout=10)
        self.assertFalse(result.success)
        with self.assertRaises(ProcessLookupError):
            os.kill(result.pid, 0)
        self.fake([{'type': 'result', 'subtype': 'success', 'session_id': self.sid, 'result': 'done'}])
        self.assertTrue((await self.run_provider()).success)

    async def test_missing_terminal_is_failure(self):
        self.fake([{'type':'system','session_id':self.sid}])
        self.assertFalse((await self.run_provider()).success)

    async def test_error_terminal_and_nonzero(self):
        for event, code in [({'type':'result','session_id':self.sid,'is_error':True,'result':'failed'},0), ({'type':'result','subtype':'success','session_id':self.sid,'result':'done'},2)]:
            self.fake([event], code)
            self.assertFalse((await self.run_provider()).success)

    async def test_fresh_codex_preserves_observed_failed_session(self):
        self.fake([{'type':'thread.started','thread_id':self.sid}, {'type':'turn.failed','error':{'message':'failed'}}])
        result = await self.run_provider('codex', fresh=True)
        self.assertFalse(result.success)
        self.assertEqual(result.session_id, self.sid)

    async def test_resume_requires_uuid(self):
        with self.assertRaises(ValueError):
            await self.runner.run('claude', 'p', self.root, None, fresh=False)
        with self.assertRaises(ValueError):
            await self.runner.run('codex', 'p', self.root, 'friendly-name', fresh=False)

    async def test_standalone_cancel_reaps_process_and_releases_lock(self):
        self.fake([], wait=True)
        started = asyncio.Event()
        pids = []
        def on_start(pid):
            pids.append(pid)
            started.set()
        task = asyncio.create_task(self.run_provider(on_start=on_start))
        await started.wait()
        duplicate = await self.run_provider()
        self.assertFalse(duplicate.success)
        self.assertIn('owned', duplicate.error)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        with self.assertRaises(ProcessLookupError):
            os.kill(pids[0], 0)
        self.fake([{'type':'result','subtype':'success','session_id':self.sid,'result':'done'}])
        self.assertTrue((await self.run_provider()).success)

    async def test_fresh_claude_supplies_and_preserves_uuid_on_failure(self):
        self.binary.write_text('''#!/usr/bin/env python3
import json,sys
sys.stdin.read()
sid=sys.argv[sys.argv.index('--session-id')+1]
print(json.dumps({'type':'result','session_id':sid,'subtype':'error_during_execution','is_error':True}))
''')
        self.binary.chmod(0o700)
        result = await self.run_provider(fresh=True, model='claude-fable-5-1')
        self.assertFalse(result.success)
        self.assertEqual(str(uuid.UUID(result.session_id)), result.session_id)

    async def test_external_writer_blocks_start(self):
        async def owned(provider, session_id):
            return True
        self.runner._external_writer = owned
        result = await self.run_provider()
        self.assertFalse(result.success)
        self.assertIsNone(result.pid)
        self.assertIn('external', result.error)

    async def test_callback_failure_reaps_child(self):
        self.fake([], wait=True)
        pids = []
        def fails(pid):
            pids.append(pid)
            raise RuntimeError('durability failed')
        result = await self.run_provider(on_start=fails)
        self.assertFalse(result.success)
        with self.assertRaises(ProcessLookupError):
            os.kill(pids[0], 0)

    async def test_locked_event_ack_retries_without_stopping_worker(self):
        self.fake([{'type': 'result', 'subtype': 'success', 'session_id': self.sid, 'result': 'done'}])
        calls = []
        def on_seq(seq):
            calls.append(seq)
            if len(calls) == 1:
                raise sqlite3.OperationalError('database is locked')
        result = await asyncio.wait_for(self.run_provider(worker_id=12, on_seq=on_seq), 10)
        self.assertTrue(result.success, result.error)
        self.assertEqual(result.text, 'done')
        self.assertEqual(calls[:2], [calls[0], calls[0]])

    async def test_transport_token_not_inherited(self):
        from unittest.mock import patch
        self.binary.write_text('''#!/usr/bin/env python3
import json,sys,os
sys.stdin.read()
assert 'TELEGRAM_BOT_TOKEN' not in os.environ
assert 'BOT_TOKEN' not in os.environ
sid=sys.argv[sys.argv.index('--resume')+1]
print(json.dumps({'type':'result','session_id':sid,'subtype':'success','result':'done'}))
''')
        self.binary.chmod(0o700)
        with patch.dict(os.environ, {'TELEGRAM_BOT_TOKEN':'test-secret','BOT_TOKEN':'test-secret'}):
            result = await self.run_provider()
        self.assertTrue(result.success)
        self.assertNotIn('test-secret', Path(result.log_path).read_text())

    async def test_quota_requires_structured_rejection(self):
        for info, error, expected in [
            ({'status': 'allowed_warning', 'utilization': .8}, None, False),
            ({'status': 'allowed', 'overageStatus': 'rejected'}, 'rate_limit', False),
            ({'status': 'rejected', 'resetsAt': 2000000000}, 'rate_limit', True),
        ]:
            self.fake([{'type': 'rate_limit_event', 'rate_limit_info': info},
                       {'type': 'assistant', 'error': error},
                       {'type': 'result', 'session_id': self.sid, 'is_error': True}])
            result = await self.run_provider()
            self.assertEqual(result.quota_limited, expected)
            self.assertFalse(result.success)
            self.assertNotIn('overageStatus', result.rate_limit_info)

    async def test_codex_quota_requires_usage_limit_discriminator_and_records_reset(self):
        reset = 9999999999
        for payload in ({'rate_limits': {'primary': {'resets_at': reset}}},
                        {'info': {'rate_limits': {'primary': {'resets_at': reset}}}}):
            self.fake([{'type': 'thread.started', 'thread_id': self.sid}, payload,
                       {'type': 'turn.failed', 'error': {'codex_error_info': 'usage_limit_exceeded'}}])
            result = await self.run_provider('codex')
            self.assertTrue(result.quota_limited)
            self.assertEqual(result.failure_code, 'quota_limited')
            self.assertEqual(result.rate_limit_info, {'primary': {'resets_at': reset}})

        self.fake([{'type': 'thread.started', 'thread_id': self.sid},
                   {'type': 'turn.failed', 'error': {'message': 'network error'}}])
        result = await self.run_provider('codex')
        self.assertFalse(result.quota_limited)
        self.assertNotEqual(result.failure_code, 'quota_limited')

    async def test_profile_resume_uses_original_file_and_isolated_credentials(self):
        from unittest.mock import patch
        transcript = self.root / (self.sid + '.jsonl')
        transcript.write_text('{}\n')
        profile = self.root / 'profile'
        self.binary.write_text("""#!/usr/bin/env python3
import json,sys,os
from pathlib import Path
sys.stdin.read()
assert os.environ['CLAUDE_CONFIG_DIR'].endswith('/profile')
assert 'ANTHROPIC_API_KEY' not in os.environ
assert 'ANTHROPIC_AUTH_TOKEN' not in os.environ
path=Path(sys.argv[sys.argv.index('--resume')+1])
assert path.is_absolute() and path.is_file()
print(json.dumps({'type':'result','session_id':path.stem,'subtype':'success','result':'done'}))
""")
        self.binary.chmod(0o700)
        with patch.dict(os.environ, {'ANTHROPIC_API_KEY': 'fake-key', 'ANTHROPIC_AUTH_TOKEN': 'fake-token'}):
            result = await self.run_provider(resume_path=str(transcript))
        self.assertTrue(result.success, result.error)
        self.assertIsNone(result.error)
        self.assertIsNone(result.failure_code)
        self.assertEqual(result.transcript_path, str(transcript.resolve()))

    async def test_resume_ignores_provisional_hook_id_but_validates_conversation(self):
        self.fake([{'type': 'system', 'subtype': 'hook_started', 'session_id': str(uuid.uuid4())},
                   {'type': 'system', 'subtype': 'init', 'session_id': self.sid},
                   {'type': 'result', 'subtype': 'success', 'session_id': self.sid, 'result': 'done'}])
        result = await self.run_provider()
        self.assertTrue(result.success, result.error)
        self.assertEqual(result.session_id, self.sid)

    async def test_cancel_after_native_parent_exit_kills_pipe_holding_descendants(self):
        from coordinator.host import alive
        short_host_stop(self)
        for ignore_term in (False, True):
            with self.subTest(ignore_term=ignore_term):
                child_path = self.root / ('descendant-%s.pid' % ignore_term)
                child = ("import os,signal; from pathlib import Path; "
                         + ("signal.signal(signal.SIGTERM, signal.SIG_IGN); " if ignore_term else "")
                         + "Path(" + repr(str(child_path)) + ").write_text(str(os.getpid())); signal.pause()")
                self.binary.write_text('#!/usr/bin/env python3\nimport subprocess,sys\n'
                                       'sys.stdin.read()\n'
                                       'subprocess.Popen([sys.executable, "-c", ' + repr(child) + '])\n')
                self.binary.chmod(0o700)
                pids = []
                task = asyncio.create_task(self.run_provider(on_start=pids.append))
                child_pid = None
                try:
                    child_pid = int(await until_text(child_path))
                    await until(lambda: pids and not alive(pids[0]), 'the native parent to exit')
                    self.assertTrue(child_path.exists())
                    with self.assertRaises(ProcessLookupError):
                        os.kill(pids[0], 0)
                    os.kill(child_pid, 0)
                    self.assertFalse(task.done())
                    task.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await settle(task)
                    await until(lambda: not alive(child_pid), 'the pipe-holding descendant to terminate')
                    with self.assertRaises(ProcessLookupError):
                        os.kill(child_pid, 0)
                    self.fake([{'type': 'result', 'subtype': 'success', 'session_id': self.sid, 'result': 'done'}])
                    self.assertTrue((await settle(self.run_provider())).success)
                finally:
                    if not task.done():
                        task.cancel()
                        await settle(asyncio.gather(task, return_exceptions=True))
                    if child_pid:
                        try:
                            os.kill(child_pid, 9)
                        except ProcessLookupError:
                            pass

    async def test_repeated_cancellation_finishes_owned_cleanup_and_releases_lock(self):
        from coordinator.host import HostClient
        short_host_stop(self)
        ready = self.root / 'ready'
        terminated = self.root / 'term-seen'
        self.binary.write_text('#!/usr/bin/env python3\nimport signal,sys\nfrom pathlib import Path\n'
            'signal.signal(signal.SIGTERM, lambda *_: Path(' + repr(str(terminated)) + ').touch())\n'
            'sys.stdin.read()\nPath(' + repr(str(ready)) + ').touch()\nwhile True:\n    signal.pause()\n')
        self.binary.chmod(0o700)
        cleanup_started = asyncio.Event()
        cleanup_release = asyncio.Event()
        cleanup_finished = asyncio.Event()
        cancellation_observed = asyncio.Event()
        cleanups = []
        stop = HostClient.stop

        async def gated_stop(process):
            cleanups.append(asyncio.current_task())
            cleanup_started.set()
            await cleanup_release.wait()
            await stop(process)
            cleanup_finished.set()

        pids = []
        with patch.object(HostClient, 'stop', gated_stop):
            task = asyncio.create_task(self.run_provider(on_start=pids.append))
            try:
                await until_exists(ready)
                self.assertTrue(ready.exists())
                task.cancel()
                await settle(cleanup_started.wait())
                task.cancel()
                asyncio.get_running_loop().call_soon(cancellation_observed.set)
                await settle(cancellation_observed.wait())
                self.assertFalse(task.done())
                self.assertFalse(cleanups[0].done())
                self.assertFalse(cleanup_release.is_set())
                self.assertEqual((await settle(self.run_provider())).failure_code, 'session_busy')
                cleanup_release.set()
                await until_exists(terminated)
                self.assertTrue(terminated.exists())
                with self.assertRaises(asyncio.CancelledError):
                    await settle(task)
                self.assertTrue(cleanup_finished.is_set())
                with self.assertRaises(ProcessLookupError):
                    os.kill(pids[0], 0)
                self.fake([{'type': 'result', 'subtype': 'success', 'session_id': self.sid, 'result': 'done'}])
                self.assertTrue((await settle(self.run_provider())).success)
            finally:
                cleanup_release.set()
                if not task.done():
                    task.cancel()
                    await settle(asyncio.gather(task, return_exceptions=True))
                await settle(asyncio.gather(*cleanups, return_exceptions=True))
                for pid in pids:
                    try:
                        os.killpg(pid, 9)
                    except ProcessLookupError:
                        pass


class NativeProviderTests(ProviderTestsSupport, unittest.IsolatedAsyncioTestCase):
    async def test_public_claude_launch_env_is_the_profile_env(self):
        from coordinator.host import HostClient
        launch = HostClient.launch
        self.fake([{'type': 'result', 'subtype': 'success', 'session_id': self.sid, 'result': 'Done'}])
        with patch.object(HostClient, 'launch', AsyncMock(side_effect=launch)) as capture:
            result = await self.run_provider()
        self.assertTrue(result.success, result.error)
        self.assertEqual(capture.await_args.args[1]['env'], self.broker.environment('profile'))
        self.assertEqual(capture.await_args.kwargs['private']['env'], {})

    async def test_public_run_records_its_own_rate_limits(self):
        info = {'status': 'allowed', 'rateLimitType': 'seven_day', 'utilization': .5}
        self.fake([{'type': 'rate_limit_event', 'rate_limit_info': info},
                   {'type': 'result', 'subtype': 'success', 'session_id': self.sid, 'result': 'Done'}])
        result = await self.run_provider()
        self.assertTrue(result.success, result.error)
        self.assertFalse(result.managed)
        self.assertEqual(result.rate_limit_info, info)
