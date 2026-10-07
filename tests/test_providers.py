import asyncio
import json
import os
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest.mock import AsyncMock, patch

from coordinator.providers import ProviderRunner
from coordinator.accounts import AccountBroker
from coordinator.store import Store

from tests.support import stop_test_hosts

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
        overrides.write_text(json.dumps({'shared_mcp_servers': ['example-mcp', 'other-mcp']}))
        selection = patch('coordinator.accounts.LOCAL_OVERRIDES_PATH', overrides)
        selection.start()
        self.addCleanup(selection.stop)
        self.binary = self.root / 'fake'
        self.sid = str(uuid.uuid4())
        self.store = Store(self.root / 'state')
        self.profile = self.root / 'profile'
        self.profile.mkdir()
        self.store.put('accounts', {'profile': {'config_dir': str(self.profile), 'enabled': True}})
        self.store.put('account_status', {'profile': {'observed_at': __import__('time').time(),
                                                      'identity': {'logged_in': True, 'email': 'test@example.com'}}})
        self.broker = AccountBroker(self.store)
        self.runner = ProviderRunner(self.root / 'state', binaries={'claude': str(self.binary), 'codex': str(self.binary)},
                                     account_broker=self.broker)

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

    async def test_claude_structured_decision_runs_without_ultracode(self):
        self.fake([{'type':'result','subtype':'success','session_id':self.sid,'result':'done','structured_output':{'action':'report'}}])
        result = await self.run_provider(schema={'type': 'object'})
        self.assertTrue(result.success)
        argv = json.loads(Path(result.log_path).read_text().splitlines()[0])['argv']
        self.assertEqual(json.loads(argv[argv.index('--settings') + 1]), {'ultracode': False,
            'claudeMdExcludes': [str(parent / name) for parent in Path.home().parents
                                for name in ('CLAUDE.md', 'CLAUDE.local.md', '.claude/CLAUDE.md')]})

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
                self.assertEqual(shared_mcp_path.parent, self.store.directory)
                for path in host.iterdir():
                    if path.is_file():
                        self.assertNotIn(value, path.read_text(errors='replace'), path.name)
        self.assertEqual(json.loads(shared_mcp_path.read_text()),
                         {'mcpServers': entries})
        self.assertNotIn(value, json.dumps([tuple(row) for row in self.store.db.execute(
            'SELECT * FROM problems')]))
        self.assertNotIn(value, ''.join(self.store.db.iterdump()))

    async def test_structured_claude_and_codex_do_not_get_shared_mcp_config(self):
        home = self.root / 'home'
        home.mkdir()
        (home / '.claude.json').write_text(json.dumps({'mcpServers': {
            'example-mcp': {'type': 'http', 'url': 'http://127.0.0.1:1/work', 'headers': {}},
            'other-mcp': {'type': 'http', 'url': 'http://127.0.0.1:1/personal', 'headers': {}}}}))
        self.fake([{'type': 'result', 'subtype': 'success', 'session_id': self.sid, 'result': 'done'}])
        with patch.dict(os.environ, {'HOME': str(home)}):
            structured = await self.run_provider(schema={'type': 'object'})
            self.fake([{'type': 'thread.started', 'thread_id': self.sid},
                       {'type': 'item.completed', 'item': {'type': 'agent_message', 'text': 'done'}},
                       {'type': 'turn.completed'}])
            codex = await self.run_provider('codex')
        self.assertTrue(structured.success, structured.error)
        self.assertTrue(codex.success, codex.error)
        structured_args = json.loads((Path(structured.log_path).parent / 'spec.json').read_text())['argv']
        self.assertEqual(structured_args[structured_args.index('--json-schema') + 2:], ['--tools', ''])
        self.assertNotIn('--mcp-config', structured_args)
        codex_args = json.loads((Path(codex.log_path).parent / 'spec.json').read_text())['argv']
        self.assertNotIn('--mcp-config', codex_args)

    async def test_default_worker_omits_shared_config(self):
        (self.root / 'local-overrides.json').write_text('{}')
        self.fake([{'type': 'result', 'subtype': 'success', 'session_id': self.sid, 'result': 'done'}])
        result = await self.run_provider()
        self.assertTrue(result.success, result.error)
        args = json.loads((Path(result.log_path).parent / 'spec.json').read_text())['argv']
        self.assertNotIn('--mcp-config', args)
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
            self.assertNotIn('--mcp-config', args)
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
        for ignore_term in (False, True):
            with self.subTest(ignore_term=ignore_term):
                child_path = self.root / 'descendant.pid'
                if child_path.exists():
                    child_path.unlink()
                child = ("import os,signal,time; from pathlib import Path; "
                         + ("signal.signal(signal.SIGTERM, signal.SIG_IGN); " if ignore_term else "")
                         + "Path(" + repr(str(child_path)) + ").write_text(str(os.getpid())); time.sleep(60)")
                self.binary.write_text('#!/usr/bin/env python3\nimport subprocess,sys\n'
                                       'sys.stdin.read()\n'
                                       'subprocess.Popen([sys.executable, "-c", ' + repr(child) + '])\n')
                self.binary.chmod(0o700)
                pids = []
                task = asyncio.create_task(self.run_provider(on_start=pids.append))
                child_pid = None
                try:
                    for _ in range(300):
                        if child_path.exists() and child_path.read_text() and pids:
                            try:
                                os.kill(pids[0], 0)
                            except ProcessLookupError:
                                break
                        await asyncio.sleep(.01)
                    self.assertTrue(child_path.exists())
                    child_pid = int(child_path.read_text())
                    with self.assertRaises(ProcessLookupError):
                        os.kill(pids[0], 0)
                    os.kill(child_pid, 0)
                    self.assertFalse(task.done())
                    task.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await asyncio.wait_for(task, 8)
                    for _ in range(200):
                        try:
                            os.kill(child_pid, 0)
                        except ProcessLookupError:
                            break
                        await asyncio.sleep(.01)
                    with self.assertRaises(ProcessLookupError):
                        os.kill(child_pid, 0)
                    self.fake([{'type': 'result', 'subtype': 'success', 'session_id': self.sid, 'result': 'done'}])
                    self.assertTrue((await self.run_provider()).success)
                finally:
                    if not task.done():
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                    if child_pid:
                        try:
                            os.kill(child_pid, 9)
                        except ProcessLookupError:
                            pass

    async def test_repeated_cancellation_finishes_owned_cleanup_and_releases_lock(self):
        ready = self.root / 'ready'
        terminated = self.root / 'term-seen'
        self.binary.write_text('#!/usr/bin/env python3\nimport signal,time,sys\nfrom pathlib import Path\n'
            'signal.signal(signal.SIGTERM, lambda *_: Path(' + repr(str(terminated)) + ').touch())\n'
            'sys.stdin.read()\nPath(' + repr(str(ready)) + ').touch()\ntime.sleep(60)\n')
        self.binary.chmod(0o700)
        pids = []
        task = asyncio.create_task(self.run_provider(on_start=pids.append))
        try:
            for _ in range(300):
                if ready.exists():
                    break
                await asyncio.sleep(.01)
            self.assertTrue(ready.exists())
            task.cancel()
            for _ in range(300):
                if terminated.exists():
                    break
                await asyncio.sleep(.01)
            self.assertTrue(terminated.exists())
            task.cancel()
            await asyncio.sleep(.05)
            self.assertFalse(task.done())
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, 8)
            with self.assertRaises(ProcessLookupError):
                os.kill(pids[0], 0)
            self.fake([{'type': 'result', 'subtype': 'success', 'session_id': self.sid, 'result': 'done'}])
            self.assertTrue((await self.run_provider()).success)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
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
