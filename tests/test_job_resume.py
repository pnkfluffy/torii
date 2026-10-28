import asyncio
import json
import subprocess
import time
import unittest
from unittest.mock import AsyncMock, patch

from coordinator.providers import RunResult
from coordinator.native_protocol import RunControl
from coordinator import control_api, workspaces
from coordinator.service import Service
from coordinator.store import Store
from coordinator.telegram import TelegramError
from tests import test_worktree_cleanup as workspace_fixtures
from tests.test_job_disposition import JobDispositionSupport
from tests.test_providers import ProviderTestsSupport
from tests.test_service import FakeRunner, FakeTelegram, TOPIC


class JobResumeTests(JobDispositionSupport, unittest.IsolatedAsyncioTestCase):
    def failed(self, code='interrupted', text='', success=False):
        worker = self.worker(provider='codex')
        with self.store.db:
            self.store.db.execute("UPDATE workers SET session='native-session',fresh=0,model='gpt-6.1-sol',effort='max',work='dev' WHERE id=?", (worker,))
        self.store.worker_complete(worker, {'success': success, 'text': text, 'session_id': 'native-session',
                                           'failure_code': code}, 'interrupted' if code == 'interrupted' else 'done')
        return self.service.workers.get(worker)

    async def resume(self):
        with patch.object(self.service.workers, 'run', new_callable=AsyncMock) as run:
            await self.service.workers_once()
            if self.service.worker_tasks:
                await asyncio.gather(*self.service.worker_tasks.values())
        return run

    def latest(self, task):
        return dict(self.store.db.execute('SELECT * FROM workers WHERE task=? ORDER BY id DESC LIMIT 1', (task,)).fetchone())

    async def test_host_death_resumes_same_native_session_and_launch_settings_once(self):
        old = self.failed()
        await self.settle()
        await self.resume()
        new = self.latest(old['task'])
        self.assertNotEqual(new['id'], old['id'])
        self.assertEqual(tuple(new[k] for k in ('session', 'fresh', 'provider', 'model', 'effort', 'work', 'iteration')),
                         ('native-session', 0, 'codex', 'gpt-6.1-sol', 'max', 'dev', 0))
        self.assertIn('inspect the actual state', new['prompt'])
        await self.resume()
        self.assertEqual(self.latest(old['task'])['id'], new['id'])

    async def test_three_resumes_use_zero_two_and_ten_minutes_then_owner_wait(self):
        old = self.failed()
        clock = 1000
        for attempt, delay in enumerate((0, 120, 600), 1):
            with patch('time.time', return_value=clock):
                await self.settle()
            round_row = self.store.task_iterations(old['task'])[-1]
            self.assertEqual((round_row['attempts'], round_row['resume_at']), (attempt - 1, clock + delay))
            with patch('time.time', return_value=clock + delay):
                await self.resume()
            new = self.latest(old['task'])
            self.assertNotEqual(new['id'], old['id'])
            self.store.worker_complete(new['id'], {'success': False, 'failure_code': 'internal_error', 'error': 'provider died'})
            old = new
            clock += delay + 1
        await self.settle()
        self.assertEqual(self.store.task_get(old['task'])['status'], 'waiting')
        self.assertIn('3', self.store.waits_list(owner=True)[0]['ask'])
        self.assertIn('provider died', self.store.waits_list(owner=True)[0]['ask'])

    async def test_partial_and_failed_turn_are_unfinished(self):
        for text, success in (('RESULT: partial tests remain', True), ('investigating…', False)):
            with self.subTest(text=text):
                old = self.failed(code=None, text=text, success=success)
                await self.settle()
                round_row = self.store.task_iterations(old['task'])[-1]
                self.assertEqual(round_row['cause'], 'unfinished')
                await self.resume()
                self.assertNotEqual(self.latest(old['task'])['id'], old['id'])

    async def test_done_and_blocked_reset_attempts_and_do_not_resume(self):
        for marker in ('RESULT: done', 'RESULT: blocked owner decision'):
            old = self.failed(code=None, text=marker, success=True)
            self.store.db.execute('UPDATE task_iterations SET attempts=3 WHERE task=?', (old['task'],))
            await self.settle()
            self.assertEqual(self.store.task_iterations(old['task'])[-1]['attempts'], 0)
            self.assertEqual(self.latest(old['task'])['id'], old['id'])

    async def test_branch_progress_resets_resume_bound(self):
        subprocess.run(['git', 'init', '-q', str(self.root)], check=True)
        def commit(message):
            subprocess.run(['git', '-C', str(self.root), '-c', 'user.name=Test', '-c', 'user.email=test@example.com',
                            'commit', '--allow-empty', '-qm', message], check=True)
        commit('First')
        old = self.failed()
        await self.settle()
        await self.resume()
        new = self.latest(old['task'])
        self.store.db.execute('UPDATE task_iterations SET attempts=3 WHERE task=?', (old['task'],))
        commit('Progress')
        self.store.worker_complete(new['id'], {'success': False, 'failure_code': 'internal_error'})
        await self.settle()
        self.assertEqual(self.store.task_iterations(old['task'])[-1]['attempts'], 0)
        self.assertEqual(self.store.task_get(old['task'])['status'], 'interrupted')

    async def test_quota_interrupts_until_reset_without_spending_attempts(self):
        old = self.failed(code='quota_limited')
        with self.store.db:
            result = json.loads(old['result'])
            result['waiting_until'] = 2000
            self.store.db.execute("UPDATE workers SET status='waiting_for_quota',result=? WHERE id=?", (json.dumps(result), old['id']))
        with patch('time.time', return_value=1000), patch.object(self.service.codex_accounts, 'select', return_value=None):
            await self.settle()
            await self.resume()
        job = self.store.task_get(old['task'])
        self.assertEqual(job['status'], 'interrupted')
        self.assertEqual(self.latest(old['task'])['id'], old['id'])
        self.assertEqual(self.store.task_iterations(old['task'])[-1]['attempts'], 0)
        with patch('time.time', return_value=2001):
            await self.resume()
        self.assertNotEqual(self.latest(old['task'])['id'], old['id'])

    async def test_offline_failure_is_remembered_until_network_returns(self):
        self.service.network_available = False
        old = self.failed()
        await self.settle()
        await self.resume()
        row = self.store.task_iterations(old['task'])[-1]
        self.assertEqual((row['cause'], row['attempts']), ('offline', 0))
        self.assertEqual(self.latest(old['task'])['id'], old['id'])
        self.service.network_available = True
        await self.resume()
        self.assertNotEqual(self.latest(old['task'])['id'], old['id'])
        self.assertEqual(self.store.task_iterations(old['task'])[-1]['attempts'], 0)

    async def test_completed_result_during_poll_outage_does_not_resume(self):
        self.service.network_available = False
        for marker in ('RESULT: done', 'RESULT: blocked owner decision'):
            with self.subTest(marker=marker):
                old = self.failed(code=None, text=marker, success=True)
                await self.settle()
                self.assertEqual(self.store.task_get(old['task'])['status'], 'open')
                self.service.network_available = True
                await self.resume()
                self.assertEqual(self.latest(old['task'])['id'], old['id'])
                self.service.network_available = False

    async def test_opencode_reset_still_requires_the_saved_account_to_be_ready(self):
        old = self.failed(code='quota_limited')
        with self.store.db:
            self.store.put('opencode_enabled', True)
            self.store.db.execute("""UPDATE workers SET provider='opencode',status='waiting_for_quota',account_alias='fixture',result=?
                WHERE id=?""", (json.dumps({'success': False, 'failure_code': 'quota_limited', 'waiting_until': 1000}), old['id']))
        broker = self.service.workers.broker('opencode')
        with patch('time.time', return_value=2000), patch.object(broker, 'ready', new_callable=AsyncMock, return_value=False):
            await self.resume()
        self.assertEqual(self.latest(old['task'])['id'], old['id'])
        with patch.object(broker, 'ready', new_callable=AsyncMock, return_value=True):
            await self.resume()
        self.assertNotEqual(self.latest(old['task'])['id'], old['id'])
        self.assertEqual(self.latest(old['task'])['session'], 'native-session')

    async def test_resume_insert_rollback_and_restart_do_not_double_spawn(self):
        old = self.failed()
        await self.settle()
        with self.assertRaisesRegex(RuntimeError, 'crash'):
            with self.store.transaction():
                await self.resume()
                raise RuntimeError('crash')
        self.assertEqual(self.latest(old['task'])['id'], old['id'])
        self.service.worker_tasks.clear()
        await self.resume()
        latest = self.latest(old['task'])
        await self.resume()
        self.assertEqual(self.latest(old['task'])['id'], latest['id'])

    async def test_host_without_exit_record_is_recovered_and_resumed(self):
        worker = self.worker(provider='codex')
        with self.store.db:
            self.store.db.execute("UPDATE workers SET status='running',session='saved-session',fresh=0 WHERE id=?", (worker,))
        with patch('coordinator.host.HostClient.recover_state', new_callable=AsyncMock, return_value='dead'), \
                patch.object(self.runner, 'run', AsyncMock(return_value=RunResult('saved-session', success=True))) as run:
            await self.service.workers_once()
            await asyncio.gather(*self.service.worker_tasks.values())
            await self.service.workers_once()
            await asyncio.gather(*self.service.worker_tasks.values())
        resumed = self.latest(self.service.workers.get(worker)['task'])
        self.assertEqual((resumed['id'], resumed['status'], resumed['session'], resumed['fresh']),
                         (worker, 'done', 'saved-session', 0))
        self.assertTrue(json.loads(resumed['result'])['success'])
        run.assert_awaited_once()
        args, options = run.await_args
        self.assertEqual(args[3], 'saved-session')
        self.assertFalse(options['fresh'])
        self.assertFalse(options['attach'])

    async def test_store_reopen_preserves_schedule_bound_and_one_resume_row(self):
        old = self.failed()
        await self.settle()
        def restart():
            self.store.close()
            self.store = Store(self.root / 'state')
            self.service = Service(self.store, self.telegram, self.runner, self.root)
        restart()
        await self.resume()
        new = self.latest(old['task'])
        self.assertNotEqual(new['id'], old['id'])
        restart()
        await self.resume()
        self.assertEqual(self.latest(old['task'])['id'], new['id'])
        self.assertEqual(self.store.task_iterations(old['task'])[-1]['attempts'], 1)
        self.assertEqual(self.store.db.execute('PRAGMA foreign_key_check').fetchall(), [])

    async def test_account_failure_joins_one_owner_wait_and_config_nudges(self):
        first = self.failed(code='auth_failed')
        second = self.failed(code='accounts_unavailable')
        await self.settle()
        await self.settle()
        waits = self.store.waits_list(owner=True)
        self.assertEqual(len(waits), 1)
        self.assertEqual({j['task'] for j in waits[0]['jobs']}, {first['task'], second['task']})
        self.assertEqual(self.store.task_iterations(first['task'])[-1]['cause'], 'account')
        config = self.failed(code='model_unavailable')
        await self.settle()
        self.assertEqual(self.store.task_get(config['task'])['status'], 'open')
        self.assertTrue(any('configuration needs attention' in row['text'] for row in self.callbacks()))
        await self.settle()
        self.assertEqual(self.store.task_get(config['task'])['status'], 'waiting')

    async def test_account_available_resumes_shared_jobs_without_spending_attempts(self):
        first = self.failed(code='auth_failed')
        second = self.failed(code='accounts_unavailable')
        await self.settle()
        with patch.object(self.service.codex_accounts, 'select', return_value='fixture-account'):
            await self.resume()
        for old in (first, second):
            new = self.latest(old['task'])
            self.assertNotEqual(new['id'], old['id'])
            self.assertEqual((new['session'], new['iteration']), ('native-session', 1))
            self.assertEqual(self.store.task_iterations(old['task'])[-1]['attempts'], 0)
        self.assertEqual(self.store.waits_list(owner=True), [])

    async def test_owner_buttons_resume_same_session_or_drop_without_new_worker(self):
        for choice in ('Resume', 'Drop'):
            old = self.failed()
            with self.store.db:
                self.store.db.execute('UPDATE task_iterations SET attempts=3 WHERE task=?', (old['task'],))
            await self.settle()
            item = next(w for w in self.store.waits_list(owner=True) if any(j['task'] == old['task'] for j in w['jobs']))
            self.store.wait_fire(item['id'], choice=choice)
            job = self.store.task_get(old['task'])
            self.assertEqual(job['status'], 'open' if choice == 'Resume' else 'dropped')
            if choice == 'Resume':
                self.assertEqual((self.latest(job['id'])['session'], self.latest(job['id'])['iteration']), ('native-session', 1))
            else:
                self.assertEqual(self.latest(job['id'])['id'], old['id'])

    async def test_poll_failure_sets_network_flag_and_success_clears_it(self):
        failed = asyncio.Event()
        recovered = asyncio.Event()
        async def updates(offset):
            if not failed.is_set():
                failed.set()
                raise TelegramError(503, 'offline')
            recovered.set()
            await asyncio.Event().wait()
        self.telegram.updates = updates
        running = asyncio.create_task(self.service.poll())
        try:
            await asyncio.wait_for(failed.wait(), 2)
            await asyncio.sleep(0)
            self.assertFalse(self.service.network_available)
        finally:
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)
        calls = 0
        async def successful(offset):
            nonlocal calls
            calls += 1
            if calls == 1:
                return []
            recovered.set()
            await asyncio.Event().wait()
        self.telegram.updates = successful
        running = asyncio.create_task(self.service.poll())
        try:
            await asyncio.wait_for(recovered.wait(), 2)
            self.assertTrue(self.service.network_available)
        finally:
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)


class FinalResultTests(ProviderTestsSupport, unittest.IsolatedAsyncioTestCase):
    async def test_final_result_with_stuck_child_settles_when_switch_on(self):
        self.store.put('auto_resume', True)
        self.fake([{'type': 'result', 'subtype': 'success', 'session_id': self.sid, 'result': 'RESULT: done'}], wait=True)
        with patch('coordinator.providers.WORKER_EXIT_GRACE', 0.1):
            result = await asyncio.wait_for(self.run_provider(worker_id=1), 10)
        self.assertTrue(result.success, result.error)
        self.assertEqual(result.text, 'RESULT: done')


class QuotaRebuildTests(unittest.TestCase):
    setUp = workspace_fixtures.WorktreeCleanupTests.setUp
    tearDown = workspace_fixtures.WorktreeCleanupTests.tearDown
    git = workspace_fixtures.WorktreeCleanupTests.git
    call = workspace_fixtures.WorktreeCleanupTests.call
    target = workspace_fixtures.WorktreeCleanupTests.target

    def test_quota_saves_copy_and_rebuilds_wip_before_same_session_resume(self):
        task = workspace_fixtures.WorktreeCleanupTests.job(self, closed=False)
        target = self.target(task)
        (target / 'README.md').write_text('unfinished work\n')
        with self.store.db:
            self.store.put('auto_resume', True)
            self.store.put('waits_enabled', True)
            self.store.put('close_clears_space', True)
        worker = self.call('workers.spawn', task=task['id'], provider='codex', prompt='Finish it').data['worker']
        reset = time.time() + 3600
        with self.store.db:
            self.store.db.execute("""UPDATE workers SET status='waiting_for_quota',session='saved-session',fresh=0,result=?
                WHERE id=?""", (json.dumps({'success': False, 'failure_code': 'quota_limited', 'waiting_until': reset}), worker))
        runner = FakeRunner()
        runner.result = RunResult('saved-session', text='RESULT: done', success=True)
        service = Service(self.store, FakeTelegram(), runner, self.project)
        async def exercise():
            with patch.object(service.codex_accounts, 'select', return_value=None):
                await service.workers_once()
                self.assertEqual(self.store.task_get(task['id'])['status'], 'interrupted')
                await workspaces.cleanup_closed_copies(self.store)
                self.assertEqual(self.store.task_get(task['id'])['copy'], 'cleared')
                self.assertFalse(target.exists())
                with patch('time.time', return_value=reset + 1):
                    await service.workers_once()
                    await asyncio.gather(*service.worker_tasks.values())
            self.assertEqual((target / 'README.md').read_text(), 'unfinished work\n')
            self.assertEqual(runner.calls[0][3], 'saved-session')
            self.assertFalse(runner.calls[0][4]['fresh'])
            self.assertEqual(self.store.task_iterations(task['id'])[-1]['attempts'], 0)
            self.assertIsNotNone(self.store.task_iterations(task['id'])[-1]['rebuild_seconds'])
        asyncio.run(exercise())


class NativeFinalResultTests(ProviderTestsSupport, unittest.IsolatedAsyncioTestCase):
    async def test_native_worker_final_result_is_drained_before_resume(self):
        self.store.put('auto_resume', True)
        self.binary.write_text('''#!/usr/bin/env python3
import json,sys,time
sys.stdin.readline()
print(json.dumps({'type':'result','subtype':'success','session_id':%r,'result':'RESULT: done'}),flush=True)
time.sleep(60)
''' % self.sid)
        self.binary.chmod(0o700)
        with patch('coordinator.providers.WORKER_EXIT_GRACE', 0.1):
            result = await asyncio.wait_for(self.run_provider(worker_id=1, control=RunControl()), 10)
        self.assertTrue(result.success, result.error)
        self.assertEqual(result.text, 'RESULT: done')
