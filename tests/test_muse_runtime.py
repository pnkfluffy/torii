"""Fresh Muse workers use fake MSP, scratch homes and fake coalition ownership."""

import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace

from coordinator import control_api, muse_accounts, muse_runtime, native_ids, process_ownership
from coordinator.accounts import AccountBroker
from coordinator.host import HostClient, alive
from coordinator.providers import ProviderRunner, RunResult
from coordinator.store import Store
from coordinator.workers import WorkerPool, can_resume_finished, host_directory, spawn_refusal
from tests.fake_muse import SESSION
from tests.support import read_jsonl, stop_test_hosts, until
from tests.test_process_ownership import FakeJobs


OTHER = '00000000-0000-4000-8000-000000000099'


class MuseRuntimeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='torii-m4-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.addCleanup(stop_test_hosts, self.root)
        self.home = self.root / 'owner'
        self.home.mkdir()
        self.patch(patch.dict(os.environ, HOME=str(self.home)))
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        self.account = muse_accounts.accounts_root() / '.torii-1234abcd'
        self.account.mkdir()
        self.cwd = self.root / 'worktree'
        self.cwd.mkdir()
        self.binary = self.root / 'fake-muse'
        self.scenario_path = self.root / 'scenario.json'
        self.calls_path = self.account / 'calls.jsonl'
        self.scenario()
        self.binary.write_text('#!' + sys.executable + '\n'
            'import sys\n'
            'sys.path.insert(0, ' + repr(str(Path(__file__).resolve().parents[1])) + ')\n'
            'from tests.fake_muse import main\n'
            'sys.argv = [sys.argv[0], "--home", ' + repr(str(self.account)) + ', "--scenario", ' +
            repr(str(self.scenario_path)) + ', "--calls", ' + repr(str(self.calls_path)) + ']\n'
            'raise SystemExit(main())\n')
        self.binary.chmod(0o700)
        self.store.put('muse_enabled', True)
        self.store.put('muse_binary', str(self.binary))
        self.store.put('muse_accounts', {'one': {'config_dir': str(self.account), 'label': 'fixture'}})
        self.store.put('muse_active_account', 'one')
        self.store.put('muse_account_status', {'one': {'identity': {'logged_in': True}}})
        self.pin = self.patch(patch.object(muse_accounts, 'check_binary'))
        self.patch(patch('coordinator.providers.platform', 'darwin'))
        self.jobs = FakeJobs()
        self.addCleanup(self.jobs.close)
        self.patch(patch.object(process_ownership, 'backend', self.jobs))
        if sys.platform != 'darwin':
            self.patch(patch.object(muse_accounts, 'write_boundary', side_effect=lambda argv, roots, **options: list(argv)))
        self.runner = ProviderRunner(self.store.directory, opencode_store=self.store)
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test',?,1)",
                              (str(self.cwd),))
        self.task = self.store.task_create('1:2', 'Muse test', worktree=str(self.cwd))

    def patch(self, patcher):
        value = patcher.start()
        self.addCleanup(patcher.stop)
        return value

    def scenario(self, **options):
        self.scenario_path.write_text(json.dumps(options))

    def calls(self):
        return read_jsonl(self.calls_path)

    def spawn(self, **options):
        with self.store.db:
            return control_api.call(self.store, 'workers.spawn', dict(task=self.task['id'], provider='muse',
                                    prompt='fixture prompt', **options), topic='1:2')

    async def run_worker(self, **options):
        try:
            return await asyncio.wait_for(self.runner.run('muse', 'fixture prompt', self.cwd, None,
                                                         fresh=True, **options), 20)
        except BaseException:
            for name in ('stderr.log', 'fake-host-errors.log', 'events.jsonl', 'writes.jsonl'):
                for path in (self.store.directory / 'hosts').glob('*/' + name):
                    print(str(path) + '\n' + path.read_text())
            raise

    def spec(self, result):
        return json.loads(Path(result.log_path).with_name('spec.json').read_text())

    async def test_fresh_success_text_session_spec_environment_and_no_secret_launcher(self):
        self.scenario(account_session_id=OTHER)
        with patch.dict(os.environ, META_API_KEY='synthetic-only', CODEX_HOME='/synthetic-only'), \
                patch.object(self.runner, 'launch_secrets', AsyncMock(side_effect=AssertionError('secret launch'))) as secrets, \
                patch('coordinator.providers.opencode_runtime.finish', side_effect=AssertionError('OpenCode finish')) as finish:
            result = await self.run_worker(model='muse-wonder-1.0', effort='max')
        self.assertTrue(result.success, result)
        self.assertEqual((result.session_id, result.text), (SESSION, 'fixture answer'))
        self.assertEqual(result.spend['input_tokens'], 12)
        secrets.assert_not_awaited()
        finish.assert_not_called()
        spec = self.spec(result)
        self.assertEqual(spec['ownership'], 'coalition')
        self.assertEqual(spec['account'], 'one')
        self.assertEqual(spec['model'], 'muse-wonder-1.0')
        turn = next(item for item in self.calls() if item.get('method') == 'turn/start')
        self.assertEqual(turn['params']['reasoningEffort'], 'max')
        self.assertEqual(spec['locks'], [])
        self.assertNotIn('META_API_KEY', spec['env'])
        self.assertNotIn('CODEX_HOME', spec['env'])
        self.assertEqual(spec['env']['HOME'], str(self.account))
        self.assertTrue(spec['initial_payload'].startswith(muse_runtime.AGENTS_LINE + '\n\n'))
        expected = [str(self.binary), 'serve', '--provider', 'meta', '--contributor-mode', 'off',
                    '--disable-request-user-input', '--disable-sandbox']
        self.assertEqual(spec['argv'][-len(expected):], expected)
        self.assertNotIn('--model', spec['argv'])
        if sys.platform == 'darwin':
            self.assertEqual(spec['argv'][:2], ['/usr/bin/sandbox-exec', '-p'])
            self.assertEqual(spec['write_boundary'], {'profile': spec['argv'][2], 'mode': 'inline'})
            for root in (self.account, self.cwd, Path(spec['env']['TMPDIR'])):
                self.assertIn(json.dumps(str(root)), spec['argv'][2])
            self.assertNotIn(json.dumps(str(Path(result.log_path).parent)), spec['argv'][2])
        self.assertTrue((self.store.directory / 'locks' / ('muse-' + SESSION + '.lock')).is_file())
        self.assertFalse((self.store.directory / 'locks' / ('muse-' + OTHER + '.lock')).exists())
        self.assertEqual(json.loads(Path(result.log_path).with_name('owner.json').read_text())['cleanup'], 'ok')
        self.assertFalse(alive(result.pid))
        self.assertTrue(self.jobs.bootouts)
        self.assertNotIn('muse', __import__('coordinator.providers', fromlist=['ADAPTERS']).ADAPTERS)

    def test_broker_uses_one_signed_in_rule_for_authentication_and_worker_admission(self):
        broker = muse_accounts.MuseBroker(self.store)
        self.assertEqual(broker.label('one'), 'fixture')
        for error, missing, expected in ((None, False, True), ('login_required', False, False),
                                          (None, True, False)):
            with self.subTest(error=error, missing=missing):
                self.store.put('muse_account_status', {'one': {'identity': {'logged_in': True}, 'error': error}})
                accounts = {'one': {'config_dir': str(self.account / 'missing' if missing else self.account)}}
                self.store.put('muse_accounts', accounts)
                self.assertEqual(broker.signed_in('one'), expected)
                self.assertEqual(broker.authenticated('one'), expected)
                self.assertEqual(broker.available(), expected)
                if expected:
                    self.assertEqual(broker.run_ready(), 'one')
                else:
                    with self.assertRaisesRegex(control_api.Refused, 'No Muse account is signed in'):
                        broker.run_ready()

    def test_contract_constants_and_adapter(self):
        self.assertEqual(muse_runtime.MODELS, ('muse-spark-1.3', 'muse-wonder-1.0', 'muse-wonder-full-1.0'))
        self.assertEqual(muse_runtime.DEFAULT_MODEL, 'muse-wonder-1.0')
        adapter = muse_runtime.adapter(lambda *args: None, error={})
        self.assertEqual(adapter.exit_seconds, 15)
        self.assertIsNone(adapter.queued_receipt)

    async def test_pool_default_model_results_and_temp_instructions(self):
        pool = WorkerPool(self.store, self.runner, AccountBroker(self.store))
        spawned = self.spawn(model=None)
        self.assertTrue(spawned.ok, spawned.text)
        worker = pool.get(spawned.data['worker'])
        done = await asyncio.wait_for(pool.run(worker, self.store.topic('1:2')), 20)
        result = json.loads(done['result'])
        self.assertTrue(result['success'], result)
        self.assertEqual(done['account_alias'], 'one')
        opened = next(item for item in self.calls() if item.get('method') == 'session/start')
        self.assertEqual(opened['params']['modelId'], 'muse-wonder-1.0')
        spec = json.loads(Path(result['log_path']).with_name('spec.json').read_text())
        self.assertIn('"results":', spec['initial_payload'])
        self.assertIn("$TMPDIR is this worker's own temp folder", spec['initial_payload'])
        self.assertTrue(spec['worker_temp'])
        self.assertFalse(can_resume_finished(self.store, done))

    async def test_ack_is_not_success_and_stop_interrupts_then_cleans_coalition(self):
        self.scenario(hold=True, ignore_cancel=True)
        pool = WorkerPool(self.store, self.runner, AccountBroker(self.store))
        spawned = self.spawn()
        self.assertTrue(spawned.ok, spawned.text)
        worker = pool.get(spawned.data['worker'])
        running = asyncio.create_task(pool.run(worker, self.store.topic('1:2')))
        self.addAsyncCleanup(self.stop_running, pool, worker['id'], running)
        await until(lambda: any(item.get('method') == 'turn/start' for item in self.calls()), 'Muse turn')
        self.assertFalse(running.done())
        self.assertEqual(pool.get(worker['id'])['status'], 'running')
        await asyncio.wait_for(pool.stop(worker['id']), 20)
        done = await asyncio.wait_for(running, 20)
        result = json.loads(done['result'])
        self.assertFalse(result['success'])
        self.assertEqual((done['status'], result['failure_code']), ('interrupted', 'owner_stopped'))
        methods = [item.get('method') for item in self.calls()]
        self.assertIn('turn/interrupt', methods)
        self.assertIn('turn/cancel', methods)
        self.assertEqual(json.loads(Path(result['log_path']).with_name('owner.json').read_text())['cleanup'], 'ok')
        self.assertFalse(alive(result['pid']))

    async def stop_running(self, pool, worker_id, task):
        if not task.done():
            try:
                await asyncio.wait_for(pool.stop(worker_id), 20)
            finally:
                task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def test_switch_off_refuses_spawn_and_direct_launch(self):
        self.store.put('muse_enabled', False)
        self.assertIn('off', self.spawn().text)
        with patch('coordinator.providers.HostClient.launch', AsyncMock()) as launch:
            result = await self.run_worker()
        self.assertFalse(result.success)
        launch.assert_not_awaited()
        self.pin.assert_not_called()

    async def test_nonfresh_refused_before_launch_even_on_attach(self):
        with patch('coordinator.providers.HostClient.launch', AsyncMock()) as launch, \
                patch('coordinator.providers.HostClient.attach') as attach:
            for attaching in (False, True):
                result = await self.runner.run('muse', 'fixture prompt', self.cwd, SESSION,
                                               fresh=False, attach=attaching)
                self.assertFalse(result.success)
            launch.assert_not_awaited()
            attach.assert_not_called()
        self.assertIn('fresh', spawn_refusal(self.store, 'muse', self.task, fresh=False).lower())

    async def test_platform_pin_and_account_gates_refuse_before_launch(self):
        with patch('coordinator.providers.HostClient.launch', AsyncMock()) as launch:
            with patch('coordinator.providers.platform', 'linux'):
                self.assertFalse((await self.run_worker()).success)
            with patch.object(muse_accounts, 'check_binary', side_effect=control_api.Refused('bad pin')):
                self.assertFalse((await self.run_worker()).success)
            for key, value in (('muse_accounts', {}), ('muse_account_status', {}),
                               ('muse_account_status', {'one': {'identity': {'logged_in': False}}}),
                               ('muse_account_blocks', {'one': time.time() + 300})):
                previous = self.store.get(key)
                self.store.put(key, value)
                self.assertFalse((await self.run_worker()).success, key)
                self.assertFalse(self.spawn().ok, key)
                self.store.put(key, previous)
            launch.assert_not_awaited()

    def test_held_refusal_has_reset_and_expired_hold_is_ready(self):
        self.store.put('muse_account_blocks', {'one': 2000000000})
        with patch('coordinator.muse_accounts.time.time', return_value=1900000000):
            text = self.spawn().text
            self.assertIn('usage limit until', text)
            self.assertIn(time.strftime('%Y-%m-%d %H:%M', time.localtime(2000000000)), text)
        with patch('coordinator.muse_accounts.time.time', return_value=2100000000):
            self.assertTrue(self.spawn().ok)

    async def test_goals_secrets_and_invalid_models_refused(self):
        self.assertFalse(self.spawn(goal='objective').ok)
        self.assertFalse(self.spawn(model='muse-x').ok)
        self.assertTrue(self.spawn(model='muse-wonder-full-1.0').ok)
        with patch('coordinator.providers.HostClient.launch', AsyncMock()) as launch:
            for options in (dict(goal='objective'), dict(secrets={'DUMMY_VALUE': 'synthetic-only'}),
                            dict(secret_names=['DUMMY_VALUE']), dict(model='muse-x')):
                self.assertFalse((await self.run_worker(**options)).success, options)
            launch.assert_not_awaited()
        task = dict(self.task, secrets=['DUMMY_VALUE'])
        self.assertIn('vault secrets', spawn_refusal(self.store, 'muse', task))

    async def test_broken_boundary_never_launches_unwrapped(self):
        with patch.object(muse_accounts, 'write_boundary', side_effect=control_api.Refused('boundary unavailable')), \
                patch('coordinator.providers.HostClient.launch', AsyncMock()) as launch:
            self.assertFalse((await self.run_worker()).success)
            launch.assert_not_awaited()

    async def test_model_refusals_preserve_message(self):
        for scenario in (
                {'open_error': {'code': -32602, 'message': 'muse-wonder-1.0 not available',
                                'data': {'kind': 'invalidParams', 'details': {'modelId': 'muse-wonder-1.0'}}}},
                {'terminal': 'failed', 'error': {'kind': 'modelError', 'message': 'muse-wonder-1.0 not available'}},
                {'terminal': 'failed', 'error': {'kind': 'configError', 'message': 'muse-wonder-1.0 not available'}}):
            self.scenario(**scenario)
            result = await self.run_worker()
            self.assertFalse(result.success, result)
            self.assertEqual(result.failure_code, 'model_unavailable')
            self.assertIn('muse-wonder-1.0 not available', result.error)

    async def test_quota_result_has_reset_and_never_waits_or_retries(self):
        self.scenario(terminal='failed', error={'kind': 'modelError', 'message': 'fixture quota',
                                              'quota': {'resetsAtMs': 2000000000123}})
        pool = WorkerPool(self.store, self.runner, AccountBroker(self.store))
        spawned = self.spawn()
        done = await asyncio.wait_for(pool.run(pool.get(spawned.data['worker']), self.store.topic('1:2')), 20)
        result = json.loads(done['result'])
        self.assertEqual(done['status'], 'done')
        self.assertEqual(result['failure_code'], 'quota_limited')
        self.assertTrue(result['quota_limited'])
        self.assertEqual(result['rate_limit_info'], {'status': 'rejected', 'resets_at': 2000000000.123})
        self.assertEqual(sum(item.get('method') == 'turn/start' for item in self.calls()), 1)

    async def test_login_lane_and_protocol_failures_record_status_and_problems(self):
        for scenario, failure in (({'lane': 'loggedOut'}, 'login_required'),
                                  ({'lane': 'apiKey'}, 'billing_lane'),
                                  ({'fingerprint': 'bad'}, 'protocol_mismatch')):
            self.scenario(**scenario)
            result = await self.run_worker()
            self.assertFalse(result.success)
            self.assertEqual(result.failure_code, failure)
            if failure == 'login_required':
                self.assertFalse(self.store.get('muse_account_status')['one']['identity']['logged_in'])
                self.store.put('muse_account_status', {'one': {'identity': {'logged_in': True}}})
            else:
                self.assertTrue(self.store.db.execute('SELECT 1 FROM problems WHERE area=? AND code=?',
                    ('muse', failure.replace('_', '-'))).fetchone())

    def test_native_id_only_canonical_uuid(self):
        self.assertTrue(native_ids.valid('muse', SESSION))
        for invalid in ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'.upper(), SESSION.replace('-', ''),
                        '../session', None, 42, 'ses_' + 'a' * 26):
            self.assertFalse(native_ids.valid('muse', invalid), invalid)

    def test_finish_keeps_model_detail(self):
        result = RunResult(None, failure_code='model_unavailable')
        muse_runtime.finish(result, {'account': 'one', 'detail': 'fixture model unavailable'}, self.store)
        self.assertIn('fixture model unavailable', result.error)

    async def test_process_exit_before_initialize_is_failure_and_does_not_cancel_caller(self):
        self.scenario(exit_on='initialize')
        result = await self.run_worker()
        self.assertFalse(result.success)
        self.assertEqual(result.failure_code, 'disconnected')
        self.assertFalse(alive(result.pid))

    async def test_terminal_then_ignored_eof_stops_host_after_exit_grace(self):
        self.scenario(ignore_eof=True)
        with patch.object(muse_runtime, 'adapter', return_value=SimpleNamespace(exit_seconds=.05, queued_receipt=None)):
            result = await self.run_worker()
        self.assertTrue(result.success, result)
        self.assertFalse(alive(result.pid))
        self.assertEqual(json.loads(Path(result.log_path).with_name('owner.json').read_text())['cleanup'], 'ok')

    async def test_terminal_with_missing_stop_all_ack_still_has_bounded_exit(self):
        self.scenario(drop_stop_all_ack=True)
        with patch.object(muse_runtime, 'adapter', return_value=SimpleNamespace(exit_seconds=.05, queued_receipt=None)):
            result = await self.run_worker()
        self.assertTrue(result.success, result)
        self.assertFalse(alive(result.pid))
        self.assertIn('task/stopAll', [item.get('method') for item in self.calls()])

    async def test_admission_ack_then_process_exit_without_terminal_is_failure(self):
        self.scenario(hold=True, exit_on='task/stopAll')
        cancelled = []
        running = asyncio.create_task(self.run_worker(on_cancel=lambda cancel: cancelled.append(cancel)))
        try:
            await until(lambda: any(item.get('method') == 'turn/start' for item in self.calls()), 'admitted turn')
            self.assertFalse(running.done())
            directory = next((self.store.directory / 'hosts').iterdir())
            await HostClient(directory).stop()
            result = await asyncio.wait_for(running, 20)
            self.assertFalse(result.success)
            self.assertFalse(alive(result.pid))
            self.assertIsNone(cancelled[-1])
        finally:
            if not running.done():
                running.cancel()
            await asyncio.gather(running, return_exceptions=True)

    @unittest.skipUnless(sys.platform == 'darwin', 'macOS sandbox-exec enforces this boundary')
    async def test_worktree_commit_uses_main_git_root_inside_boundary(self):
        repo = self.root / 'repository'
        repo.mkdir()
        def git(*args):
            return subprocess.run(['/usr/bin/git', '-C', str(repo), *args], capture_output=True,
                                  text=True, check=True, timeout=10)
        git('init')
        git('config', 'user.name', 'Fixture')
        git('config', 'user.email', 'fixture@example.invalid')
        git('config', 'commit.gpgsign', 'false')
        (repo / 'file').write_text('before\n')
        git('add', 'file')
        git('commit', '-m', 'seed')
        worktree = self.root / 'commit-worktree'
        branch = 'torii/task-' + str(self.task['id'])
        git('worktree', 'add', '-b', branch, str(worktree))
        (worktree / 'file').write_text('after\n')
        env = muse_accounts.environment(self.account)
        results = self.root / 'commit-results'
        results.mkdir()
        roots = muse_runtime.write_roots(worktree, env, results)
        self.assertNotIn(repo / '.git', roots)
        self.store.task_update(self.task['id'], worktree=str(worktree))
        with self.store.db:
            self.store.db.execute("UPDATE topics SET cwd=? WHERE id='1:2'", (str(repo),))
        grant = await muse_runtime.git_writes(self.store, {'task': self.task['id']}, worktree, roots)
        command = ['/usr/bin/git', '-C', str(worktree), 'commit', '-am', 'boundary commit']
        committed = subprocess.run(muse_accounts.write_boundary(command, [*roots, *grant.roots], files=grant.files), env=env,
                                   capture_output=True, text=True, timeout=10)
        self.assertEqual(committed.returncode, 0, committed.stderr)
        self.assertEqual(git('show', branch + ':file').stdout, 'after\n')

    async def test_restart_interrupts_live_host_tree_without_second_prompt(self):
        child_pid = self.account / 'descendant.pid'
        script = ('import subprocess, sys\nfrom pathlib import Path\n'
                  'child = subprocess.Popen([sys.executable, "-c", '
                  '"import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(300)"])\n'
                  'Path(' + repr(str(child_pid)) + ').write_text(str(child.pid))\n')
        self.scenario(hold=True, script=script)
        pool = WorkerPool(self.store, self.runner, AccountBroker(self.store))
        wid = self.spawn().data['worker']
        running = asyncio.create_task(pool.run(pool.get(wid), self.store.topic('1:2')))
        self.addAsyncCleanup(self.stop_running, pool, wid, running)
        await until(child_pid.exists, 'Muse descendant')
        await until(lambda: pool.get(wid)['session'] == SESSION, 'Muse session')
        directory = host_directory(self.store.directory, pool.get(wid))
        native_pid = int((directory / 'child.pid').read_text())
        descendant = int(child_pid.read_text())
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)
        self.assertEqual(pool.get(wid)['status'], 'running')
        self.assertTrue(alive(native_pid))
        self.store.put('muse_enabled', False)
        with self.store.db:
            self.store.db.execute('UPDATE workers SET account_alias=NULL WHERE id=?', (wid,))
        replacement = WorkerPool(self.store, ProviderRunner(self.store.directory, opencode_store=self.store),
                                 AccountBroker(self.store))
        done = await asyncio.wait_for(replacement.run(replacement.get(wid), self.store.topic('1:2')), 30)
        result = json.loads(done['result'])
        self.assertEqual((done['status'], result['failure_code']), ('interrupted', 'interrupted'))
        await until(lambda: not alive(native_pid) and not alive(descendant), 'Muse tree stopped')
        self.assertEqual(json.loads((directory / 'owner.json').read_text())['cleanup'], 'ok')
        self.assertEqual(sum(item.get('method') == 'turn/start' for item in self.calls()), 1)
        self.assertEqual(sum(item.get('method') == 'session/start' for item in self.calls()), 1)
        for fresh in (False, True):
            result = await self.runner.run('muse', 'never send again', self.cwd, SESSION,
                                           host_id=directory.name, attach=True, fresh=fresh)
            self.assertEqual(result.failure_code, 'interrupted')

    @unittest.skipUnless(sys.platform == 'darwin', 'macOS sandbox-exec enforces this boundary')
    async def test_child_cannot_forge_exit_and_stop_cleans_coalition(self):
        gate = self.account / 'host-path'
        outcome = self.account / 'forged-exit'
        script = ('import json, time\nfrom pathlib import Path\n'
                  'gate = Path(' + repr(str(gate)) + ')\n'
                  'while not gate.exists():\n    time.sleep(.01)\n'
                  'try:\n    (Path(gate.read_text()) / "exit.json").write_text(json.dumps({"returncode": 0}))\n'
                  'except OSError:\n    value = "denied"\nelse:\n    value = "planted"\n'
                  'Path(' + repr(str(outcome)) + ').write_text(value)\n')
        self.scenario(hold=True, ignore_cancel=True, script=script)
        pool = WorkerPool(self.store, self.runner, AccountBroker(self.store))
        wid = self.spawn().data['worker']
        running = asyncio.create_task(pool.run(pool.get(wid), self.store.topic('1:2')))
        self.addAsyncCleanup(self.stop_running, pool, wid, running)
        await until(lambda: any(item.get('method') == 'turn/start' for item in self.calls()), 'turn admitted')
        directory = host_directory(self.store.directory, pool.get(wid))
        gate.write_text(str(directory))
        await until(outcome.exists, 'exit write attempt')
        self.assertEqual(outcome.read_text(), 'denied')
        self.assertEqual(HostClient(directory).state, 'running')
        self.assertFalse((directory / 'exit.json').exists())
        await asyncio.wait_for(pool.stop(wid), 30)
        done = await asyncio.wait_for(running, 30)
        result = json.loads(done['result'])
        self.assertEqual((done['status'], result['failure_code']), ('interrupted', 'owner_stopped'))
        self.assertFalse(alive(result['pid']))
        self.assertIn('turn/interrupt', [item.get('method') for item in self.calls()])
        self.assertEqual(json.loads((directory / 'owner.json').read_text())['cleanup'], 'ok')
        self.assertTrue(self.jobs.bootouts)

    def git_fixture(self):
        repo = self.root / 'project'
        repo.mkdir()
        def git(cwd, *args):
            return subprocess.run(['/usr/bin/git', '-C', str(cwd), *args], capture_output=True,
                                  text=True, check=True, timeout=10)
        git(repo, 'init', '-q')
        git(repo, 'config', 'user.name', 'Fixture')
        git(repo, 'config', 'user.email', 'fixture@example.invalid')
        git(repo, 'config', 'commit.gpgsign', 'false')
        (repo / 'file').write_text('before\n')
        git(repo, 'add', 'file')
        git(repo, 'commit', '-qm', 'seed')
        self.cwd = self.store.directory / 'worktrees' / ('task-' + str(self.task['id']))
        git(repo, 'worktree', 'add', '-b', 'torii/task-' + str(self.task['id']), str(self.cwd))
        self.store.task_update(self.task['id'], worktree=str(self.cwd))
        with self.store.db:
            self.store.db.execute("UPDATE topics SET cwd=? WHERE id='1:2'", (str(repo),))
        return repo, git

    @unittest.skipUnless(sys.platform == 'darwin', 'macOS sandbox-exec enforces this boundary')
    async def test_packed_job_branch_recreates_ref_and_reflog_folders_before_launch(self):
        repo, git = self.git_fixture()
        branch = 'torii/task-' + str(self.task['id'])
        ref = repo / '.git/refs/heads' / branch
        log = repo / '.git/logs/refs/heads' / branch
        git(repo, 'pack-refs', '--all', '--prune')
        self.assertFalse(ref.parent.exists())
        log.parent.rename(self.root / 'saved-branch-logs')
        self.assertFalse(log.parent.exists())
        (self.cwd / 'file').write_text('packed branch commit\n')
        self.scenario(script='import subprocess\nsubprocess.run(' + repr([
            '/usr/bin/git', '-C', str(self.cwd), 'commit', '-am', 'packed branch commit']) + ', check=True, stdout=subprocess.DEVNULL)\n')
        pool = WorkerPool(self.store, self.runner, AccountBroker(self.store))
        spawned = self.spawn()
        self.assertTrue(spawned.ok, spawned.text)
        done = await asyncio.wait_for(pool.run(pool.get(spawned.data['worker']), self.store.topic('1:2')), 20)
        result = json.loads(done['result'])
        self.assertTrue(result['success'], result)
        for folder in (ref.parent, log.parent):
            self.assertTrue(folder.is_dir())
            self.assertEqual(folder.stat().st_uid, os.getuid())
        self.assertEqual(git(repo, 'show', branch + ':file').stdout, 'packed branch commit\n')

    @unittest.skipUnless(sys.platform == 'darwin', 'macOS sandbox-exec enforces this boundary')
    async def test_fake_muse_cannot_plant_hook_or_write_shared_git_metadata(self):
        repo, git = self.git_fixture()
        other = self.root / 'other-worktree'
        git(repo, 'worktree', 'add', '-b', 'other', str(other))
        common = repo / '.git'
        marker = self.root / 'hook-escaped'
        attempts = self.account / 'write-attempts.json'
        targets = [common / 'hooks/post-commit', common / 'config', common / 'config.worktree',
                   common / 'info/exclude', common / 'packed-refs', common / 'HEAD',
                   common / 'worktrees/other-worktree/HEAD', common / 'refs/heads/other',
                   common / 'logs/refs/heads/other', common / 'logs/HEAD']
        script = ('import json\nfrom pathlib import Path\nresults = {}\n'
                  'for name in ' + repr(list(map(str, targets))) + ':\n'
                  '    path = Path(name)\n    try:\n'
                  '        path.write_text(' + repr('#!/bin/sh\necho escaped > ' + str(marker) + '\n') + ')\n'
                  '        path.chmod(0o700)\n'
                  '    except OSError:\n        results[name] = "denied"\n'
                  '    else:\n        results[name] = "allowed"\n'
                  'Path(' + repr(str(attempts)) + ').write_text(json.dumps(results))\n')
        self.scenario(script=script)
        pool = WorkerPool(self.store, self.runner, AccountBroker(self.store))
        done = await pool.run(pool.get(self.spawn().data['worker']), self.store.topic('1:2'))
        self.assertTrue(json.loads(done['result'])['success'], done['result'])
        self.assertEqual(json.loads(attempts.read_text()), {str(path): 'denied' for path in targets})
        (other / 'file').write_text('other\n')
        git(other, 'commit', '-qam', 'owner commit')
        self.assertFalse(marker.exists())

    @unittest.skipUnless(sys.platform == 'darwin', 'macOS sandbox-exec enforces this boundary')
    async def test_rewritten_gitfile_never_changes_next_muse_git_grant(self):
        repo, git = self.git_fixture()
        victim = self.root / 'victim'
        victim.mkdir()
        git(victim, 'init', '-q')
        original = (victim / '.git/config').read_text()
        self.scenario(script='from pathlib import Path\nPath(' + repr(str(self.cwd / '.git')) +
                      ').write_text(' + repr('gitdir: ' + str(victim / '.git') + '\n') + ')\n')
        pool = WorkerPool(self.store, self.runner, AccountBroker(self.store))
        first = await pool.run(pool.get(self.spawn().data['worker']), self.store.topic('1:2'))
        self.assertTrue(json.loads(first['result'])['success'], first['result'])
        outcome = self.account / 'victim-write'
        self.scenario(script='from pathlib import Path\ntry:\n    Path(' + repr(str(victim / '.git/config')) +
                      ').write_text("planted")\nexcept OSError:\n    result = "denied"\n'
                      'else:\n    result = "allowed"\nPath(' + repr(str(outcome)) + ').write_text(result)\n')
        second = await pool.run(pool.get(self.spawn().data['worker']), self.store.topic('1:2'))
        result = json.loads(second['result'])
        self.assertTrue(result['success'], result)
        self.assertEqual(outcome.read_text(), 'denied')
        self.assertEqual((victim / '.git/config').read_text(), original)
        profile = self.spec(SimpleNamespace(log_path=result['log_path']))['write_boundary']['profile']
        self.assertNotIn(str(victim), profile)
        self.assertNotIn('(subpath ' + json.dumps(str(repo / '.git')) + ')', profile)
