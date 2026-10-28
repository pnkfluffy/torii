"""The owner's Codex model setting reaches the Codex command line, and nothing else."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest.mock import AsyncMock, patch

from coordinator.accounts import AccountBroker
from coordinator.native_protocol import RunControl
from coordinator.providers import ProviderRunner
from coordinator.store import Store
from coordinator.workers import WorkerPool
from tests.support import isolate_shared_mcp_home, stop_test_hosts
from tests.test_codex_secrets_goals import SERVER

_FAKE = """#!/usr/bin/env python3
import json, sys
data = sys.stdin.read()
print(json.dumps({'argv': sys.argv, 'input': data}), file=sys.stderr)
sid = %r
if 'exec' in sys.argv:
    print(json.dumps({'type': 'thread.started', 'thread_id': sid}), flush=True)
    print(json.dumps({'type': 'item.completed', 'item': {'type': 'agent_message', 'text': 'done'}}), flush=True)
    print(json.dumps({'type': 'turn.completed'}), flush=True)
else:
    print(json.dumps({'type': 'result', 'subtype': 'success', 'session_id': sid, 'result': 'done'}), flush=True)
"""

_APP_SERVER = """#!/usr/bin/env python3
import json, sys
def emit(value):
    print(json.dumps(value), flush=True)
def read():
    return json.loads(sys.stdin.readline())
sid = %r
assert sys.argv[1:] == ['-c', 'model_reasoning_effort=medium', 'app-server', '--listen', 'stdio://']
request = read()
emit({'id': request['id'], 'result': {}})
read()
request = read()
print(json.dumps({'argv': sys.argv, 'resume': request['params']}), file=sys.stderr)
emit({'id': request['id'], 'result': {'thread': {'id': sid}}})
request = read()
emit({'id': request['id'], 'result': {'turn': {'id': 'turn-1'}}})
emit({'method': 'turn/started', 'params': {'threadId': sid, 'turn': {'id': 'turn-1'}}})
emit({'method': 'turn/completed', 'params': {'threadId': sid, 'turn': {'id': 'turn-1', 'status': 'completed'}}})
sys.stdin.read()
"""

_CODEX_EXEC = ['-c', 'model_reasoning_effort=medium', 'exec', 'resume', '--json',
               '--dangerously-bypass-approvals-and-sandbox', '--skip-git-repo-check']


class CodexModelTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        isolate_shared_mcp_home(self, self.root)
        self.sid = str(uuid.uuid4())
        self.binary = self.root / 'fake'
        self.binary.write_text(_FAKE % self.sid)
        self.binary.chmod(0o700)

    def tearDown(self):
        self.tmp.cleanup()

    async def argv(self, provider, codex_model=None, key='argv', **options):
        """What one dispatch actually sent, as the provider saw it."""
        runner = ProviderRunner(self.root / 'state', codex_model=codex_model,
                                binaries={'claude': str(self.binary), 'codex': str(self.binary)})
        if provider == 'claude':
            from coordinator.accounts import AccountBroker
            from coordinator.store import Store
            store = Store(self.root / 'state')
            profile = self.root / 'profile'
            profile.mkdir(exist_ok=True)
            with store.db:
                store.put('accounts', {'profile': {'config_dir': str(profile), 'enabled': True}})
                store.put('account_status', {'profile': {'identity': {'logged_in': True, 'email': 'test@example.com'},
                                                        'observed_at': __import__('time').time()}})
            runner.account_broker = AccountBroker(store)
            options['account_alias'] = 'profile'
        result = await runner.run(provider, 'task', self.root, self.sid, fresh=False, **options)
        self.assertTrue(result.success, result.error)
        for line in Path(result.log_path).read_text().splitlines():
            entry = json.loads(line)
            if key in entry:
                return entry[key]
        raise AssertionError('the fake provider recorded no command line')

    async def test_a_set_codex_model_becomes_the_model_flag(self):
        argv = await self.argv('codex', codex_model=lambda: 'gpt-6-luna')
        self.assertEqual(argv[1:], _CODEX_EXEC + ['--model', 'gpt-6-luna', self.sid, '-'])

    async def test_an_unset_codex_model_leaves_the_command_line_to_the_codex_default(self):
        unset = await self.argv('codex')
        cleared = await self.argv('codex', codex_model=lambda: None)
        self.assertEqual(unset[1:], _CODEX_EXEC + [self.sid, '-'])
        self.assertEqual(cleared, unset)
        self.assertNotIn('--model', unset)

    async def test_an_explicit_model_still_wins_over_the_setting(self):
        argv = await self.argv('codex', codex_model=lambda: 'gpt-6-luna', model='gpt-6-other')
        self.assertEqual(argv.count('--model'), 1)
        self.assertEqual(argv[argv.index('--model') + 1], 'gpt-6-other')

    async def test_the_steered_app_server_run_starts_its_thread_on_the_same_model(self):
        self.binary.write_text(_APP_SERVER % self.sid)
        self.binary.chmod(0o700)
        recorded = await self.argv('codex', codex_model=lambda: 'gpt-6-luna',
                                   control=RunControl(receipt_timeout=.3), key='resume')
        self.assertEqual(recorded['model'], 'gpt-6-luna')
        self.assertEqual(recorded['threadId'], self.sid)

    async def test_claude_dispatch_never_takes_the_codex_model(self):
        argv = await self.argv('claude', codex_model=lambda: 'gpt-6-luna')
        self.assertNotIn('--model', argv)
        self.assertNotIn('gpt-6-luna', argv)

    async def test_effort_levels_reach_both_native_commands(self):
        for level in ('low', 'medium', 'high', 'max'):
            codex = await self.argv('codex', effort=level)
            self.assertEqual(codex[1:3], ['-c', 'model_reasoning_effort=' +
                                            ('xhigh' if level == 'max' else level)])
            claude = await self.argv('claude', effort=level)
            self.assertEqual(claude[claude.index('--effort') + 1], level)


if __name__ == '__main__':
    unittest.main()


class CodexWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_codex_worker_starts_through_the_account_broker_and_real_runner(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = Store(root / 'state')
            binary = root / 'codex'
            binary.write_text((_APP_SERVER % str(uuid.uuid4())).replace(
                "'app-server',", "'-c', 'skills.include_instructions=false', 'app-server',"))
            binary.chmod(0o700)
            with store.db:
                store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                    VALUES ('-10042:4',-10042,4,'Torii',?,1)''', (str(root),))
            task = store.task_create('-10042:4', 'Build', worktree=str(root))
            with store.db:
                row = store.db.execute('''INSERT INTO workers (task,topic,provider,prompt,cwd,workspace,created,updated)
                    VALUES (?,'-10042:4','codex','build this',?,?,1,1)''',
                    (task['id'], str(root), json.dumps({'cwd': str(root)})))
            worker = dict(store.db.execute('SELECT * FROM workers WHERE id=?', (row.lastrowid,)).fetchone())
            pool = WorkerPool(store, ProviderRunner(store.directory, binaries={'codex': str(binary)}),
                              AccountBroker(store))
            result = json.loads((await pool.run(worker, store.topic('-10042:4')))['result'])
            store.close()
        self.assertTrue(result['success'], result.get('error'))


class CodexSkillCatalogTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name).resolve()
        self.addCleanup(self.tmp.cleanup)
        isolate_shared_mcp_home(self, self.root)
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        self.addCleanup(stop_test_hosts, self.root)
        self.binary = self.root / 'fake-codex'
        self.sid = str(uuid.uuid4())
        self.runner = ProviderRunner(self.store.directory, binaries={'codex': str(self.binary)})
        self.pool = WorkerPool(self.store, self.runner, AccountBroker(self.store))

    async def launch(self, fresh, **options):
        self.binary.chmod(0o700)
        with patch.object(self.runner, '_external_writer', AsyncMock(return_value=False)):
            result = await self.runner.run('codex', 'task', self.root, None if fresh else self.sid,
                                           fresh=fresh, **options)
        self.assertTrue(result.success, result.failure_detail or result.error)
        return json.loads((Path(result.log_path).parent / 'spec.json').read_text())['argv']

    async def test_goal_less_codex_launch_modes(self):
        expected = {
            (True, False, False): 'c2259f9d76d85bb59a3aba8e421a15cbb9f176c0d17b086a62950478a13db7b1',
            (True, True, False): '50c6a9db29a97c9042393a6dfebdcc80b4a8f261dbe12a9a4457165ea3471dc6',
            (True, True, True): '2a3df7af2026695bc37f6aed5ba4732689c9fe0b98df884d7d7bf6f481270335',
            (False, False, False): 'adfe125a24481adf8ae78cfb24ed8b8de8328ace90cfacbb01e136e4256f2f7b',
            (False, True, False): '50c6a9db29a97c9042393a6dfebdcc80b4a8f261dbe12a9a4457165ea3471dc6',
            (False, True, True): '2a3df7af2026695bc37f6aed5ba4732689c9fe0b98df884d7d7bf6f481270335',
        }
        for fresh in (True, False):
            for controlled in (False, True):
                for secret in (False, True) if controlled else (False,):
                    with self.subTest(fresh=fresh, controlled=controlled, secret=secret):
                        if controlled:
                            self.binary.write_text(SERVER.replace('SID', repr(self.sid)).replace('ROOT', repr(str(self.root))))
                            options = dict(control=RunControl(), secrets={'FAKE_SECRET': 'fixture'} if secret else None)
                        else:
                            observed = self.root / ('observed-' + str(uuid.uuid4()))
                            self.binary.write_text((_FAKE % self.sid) + (
                                'from pathlib import Path\nimport time\n'
                                'deadline=time.monotonic()+10\n'
                                'while not Path(%r).exists() and time.monotonic()<deadline:\n'
                                '    time.sleep(.005)\n' % str(observed)))
                            options = dict(on_session=lambda sid: observed.touch())
                        command = json.dumps(await self.launch(fresh, **options))
                        self.assertNotIn('features.goals=true', command)
                        command = command.replace(str(self.root), '<worktree>').replace(self.sid, '<session>')
                        self.assertEqual(hashlib.sha256(command.encode()).hexdigest(), expected[(fresh, controlled, secret)])
