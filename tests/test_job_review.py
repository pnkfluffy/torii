import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch
from tests.test_job_disposition import JobDispositionSupport
from tests.test_service import TOPIC

class ReviewRegressionTests(JobDispositionSupport, unittest.IsolatedAsyncioTestCase):
    async def test_muse_available_account_resumes_quota_and_account_jobs(self):
        self.store.put('muse_enabled', True)
        broker = self.service.workers.broker('muse')
        for code in ('quota_limited', 'auth_failed'):
            with self.subTest(code=code):
                old = self.failed(code=code, provider='muse')
                await self.settle()
                with patch.object(broker, 'available', return_value=True):
                    await self.resume()
                new = self.latest(old['task'])
                self.assertNotEqual(new['id'], old['id'])
                self.assertEqual((new['provider'], new['fresh'], new['resume_of']), ('muse', 1, old['id']))

    async def test_settle_records_a_bad_job_and_keeps_processing(self):
        broken = self.failed()
        self.store.db.execute("UPDATE workers SET result='invalid json' WHERE id=?", (broken['id'],))
        healthy = self.job()
        await self.settle()
        self.assertEqual(self.store.task_get(healthy['id'])['status'], 'open')
        self.assertEqual(len(self.callbacks()), 1)
        self.assertIn('has no worker', self.callbacks()[0]['text'])
        problem = self.store.db.execute("SELECT * FROM problems WHERE area='jobs' AND task=?", (broken['task'],)).fetchone()
        self.assertEqual(problem['code'], 'settle-failed')
        self.assertEqual(problem['detail'], 'JSONDecodeError')

    async def test_resume_records_a_bad_job_and_keeps_processing(self):
        broken = self.failed()
        healthy = self.failed()
        await self.settle()
        self.store.db.execute("UPDATE workers SET provider='unknown' WHERE id=?", (broken['id'],))
        await self.resume()
        self.assertNotEqual(self.latest(healthy['task'])['id'], healthy['id'])
        problem = self.store.db.execute("SELECT * FROM problems WHERE area='jobs' AND task=?", (broken['task'],)).fetchone()
        self.assertEqual(problem['code'], 'resume-failed')

    async def test_account_wait_error_does_not_block_other_resumes(self):
        account = self.failed(code='auth_failed')
        await self.settle()
        healthy = self.failed()
        await self.settle()
        with patch.object(self.service.codex_accounts, 'select', side_effect=RuntimeError('fixture failure')):
            await self.resume()
        self.assertNotEqual(self.latest(healthy['task'])['id'], healthy['id'])
        problem = self.store.db.execute("SELECT * FROM problems WHERE area='jobs' AND code='account-resume-failed'").fetchone()
        self.assertEqual(problem['detail'], 'RuntimeError')
        self.assertEqual(self.store.task_get(account['task'])['status'], 'waiting')

    async def test_any_parked_worker_blocks_disposition_for_all_stop_paths(self):
        for status in ('waiting_for_secret', 'waiting_for_quota'):
            for code in ('owner_stopped', 'coordinator_stopped', 'internal_error'):
                with self.subTest(status=status, code=code):
                    parked = self.worker(provider='codex')
                    task = self.service.workers.get(parked)['task']
                    with self.store.db:
                        self.store.db.execute('UPDATE workers SET status=? WHERE id=?', (status, parked))
                        newer = self.store.db.execute('''INSERT INTO workers
                            (task,topic,provider,prompt,cwd,workspace,status,result,created,updated)
                            VALUES (?,?,?,?,?,?,'done',?,1,1)''',
                            (task, TOPIC, 'codex', 'next step', str(self.root), '{}',
                             json.dumps({'success': False, 'failure_code': code}))).lastrowid
                        self.store.db.execute('UPDATE task_iterations SET attempts=3 WHERE task=?', (task,))
                    await self.settle()
                    await self.settle()
                    self.assertEqual(self.service.workers.get(parked)['status'], status)
                    self.assertEqual(self.store.task_get(task)['status'], 'open')
                    self.assertEqual(self.latest(task)['id'], newer)
                    self.assertEqual(self.store.waits_list(owner=True), [])
                    self.assertEqual(self.callbacks(), [])

    async def test_condition_failure_of_an_automatic_resume_is_a_crash(self):
        for code in ('quota_limited', 'auth_failed', 'interrupted'):
            with self.subTest(code=code):
                old = self.failed()
                await self.settle()
                await self.resume()
                new = self.latest(old['task'])
                result = {'success': False, 'failure_code': code, 'network_down': code == 'interrupted'}
                self.store.worker_complete(new['id'], result)
                await self.settle()
                row = self.store.task_iterations(old['task'])[-1]
                self.assertEqual((row['cause'], row['attempts']), ('crash', 1))
                self.assertEqual(self.store.task_get(old['task'])['status'], 'interrupted')

    async def test_account_firing_preserves_attempts_and_names_the_queued_worker(self):
        old = self.failed(code='auth_failed')
        self.store.db.execute('UPDATE task_iterations SET attempts=2 WHERE task=?', (old['task'],))
        await self.settle()
        with patch.object(self.service.codex_accounts, 'select', return_value='fixture-account'):
            await self.resume()
        new = self.latest(old['task'])
        self.assertNotEqual(new['id'], old['id'])
        self.assertEqual(self.store.task_iterations(old['task'])[-1]['attempts'], 2)
        callbacks = '\n'.join(row['text'] for row in self.callbacks())
        self.assertIn('Worker %d queued to continue.' % new['id'], callbacks)
        self.assertNotIn('Decide the next step.', callbacks)

    async def test_failed_blocked_result_does_not_reset_attempts(self):
        old = self.failed(code=None, text='RESULT: blocked owner decision', success=False)
        self.store.db.execute('UPDATE task_iterations SET attempts=3 WHERE task=?', (old['task'],))
        await self.settle()
        self.assertEqual(self.store.task_get(old['task'])['status'], 'waiting')
        self.assertEqual(self.store.task_iterations(old['task'])[-1]['attempts'], 3)

    async def test_missing_result_line_waits_for_disposition_without_replay(self):
        for network_down in (False, True):
            with self.subTest(network_down=network_down):
                old = self.failed(code=None, text='The change is ready.', success=True)
                self.store.db.execute('UPDATE workers SET result=? WHERE id=?',
                    (json.dumps({'success': True, 'text': 'The change is ready.', 'network_down': network_down}), old['id']))
                await self.settle()
                await self.resume()
                self.assertEqual(self.latest(old['task'])['id'], old['id'])
                self.assertEqual(self.store.task_get(old['task'])['status'], 'open')
                await self.settle()
                self.assertEqual(self.store.task_get(old['task'])['status'], 'waiting')

    async def test_missing_result_line_can_be_required_after_policy_rollout(self):
        old = self.failed(code=None, text='The change is ready.', success=True)
        with patch('coordinator.lifecycle.REQUIRE_RESULT_LINE', True):
            await self.settle()
        self.assertEqual(self.store.task_iterations(old['task'])[-1]['cause'], 'unfinished')
        await self.resume()
        self.assertNotEqual(self.latest(old['task'])['id'], old['id'])

    async def test_automatic_resume_posts_one_callback_with_cause_and_bound(self):
        old = self.failed()
        await self.settle()
        await self.resume()
        new = self.latest(old['task'])
        await self.resume()
        expected = 'Job 1: worker %d resumed after crash, attempt 1 of 3' % new['id']
        self.assertEqual([row['text'] for row in self.callbacks()], [expected])

    async def test_last_detail_uses_only_the_short_error_clipped_to_160(self):
        old = self.failed(code=None, text='RAW FINAL TEXT', success=False)
        error = 'short error ' + 'x' * 200
        self.store.db.execute('UPDATE workers SET result=? WHERE id=?',
            (json.dumps({'success': False, 'text': 'RAW FINAL TEXT', 'failure_detail': 'RAW EXCEPTION', 'error': error}), old['id']))
        self.store.db.execute('UPDATE task_iterations SET attempts=3 WHERE task=?', (old['task'],))
        await self.settle()
        ask = self.store.waits_list(owner=True)[0]['ask']
        detail = ask.split('(last detail: ', 1)[1].split('). Resume', 1)[0]
        self.assertEqual(len(detail), 160)
        self.assertTrue(detail.startswith('short error '))
        self.assertNotIn('RAW', ask)

    async def test_account_name_and_disposition_buttons_match_the_ask(self):
        self.failed(code='auth_failed')
        await self.settle()
        self.assertTrue(self.store.waits_list(owner=True)[0]['ask'].startswith('Codex workers'))
        job = self.job()
        await self.settle()
        await self.settle()
        item = next(item for item in self.store.waits_list(owner=True) if any(link['task'] == job['id'] for link in item['jobs']))
        self.assertEqual(item['ask'], 'Job 2 was left undecided after its last result. Resume or drop?')
        self.assertEqual([button['text'] for row in item['detail']['buttons'] for button in row], ['Resume', 'Drop'])

    async def test_unavailable_held_copy_creates_an_owner_wait_with_its_reason(self):
        old = self.failed()
        await self.settle()
        with self.store.db:
            self.store.put('close_clears_space', True)
            self.store.db.execute("UPDATE tasks SET copy='held',copy_reason='fixture branch needs attention' WHERE id=?", (old['task'],))
            self.store.db.execute('UPDATE workers SET cwd=? WHERE id=?', (str(self.root / 'missing'), old['id']))
        await self.resume()
        self.assertEqual(self.latest(old['task'])['id'], old['id'])
        self.assertEqual(self.store.task_get(old['task'])['status'], 'waiting')
        self.assertIn('fixture branch needs attention', self.store.waits_list(owner=True)[0]['ask'])


    def failed(self, code='interrupted', text='', success=False, provider='codex', status=None):
        worker = self.worker(provider=provider)
        with self.store.db:
            self.store.db.execute("UPDATE workers SET session='native-session',fresh=0,model='m',effort='max',work='dev' WHERE id=?", (worker,))
        self.store.worker_complete(worker, {'success': success, 'text': text, 'session_id': 'native-session', 'failure_code': code}, status or ('interrupted' if code == 'interrupted' else 'done'))
        return self.service.workers.get(worker)

    async def resume(self):
        with patch.object(self.service.workers, 'run', new_callable=AsyncMock) as run:
            await self.service.workers_once()
            if self.service.worker_tasks:
                await asyncio.gather(*self.service.worker_tasks.values())
        return run

    def latest(self, task):
        return dict(self.store.db.execute('SELECT * FROM workers WHERE task=? ORDER BY id DESC LIMIT 1', (task,)).fetchone())

    def count(self, task):
        return self.store.db.execute('SELECT COUNT(*) FROM workers WHERE task=?', (task,)).fetchone()[0]

    async def test_r1_muse_quota_crashes_workers_loop(self):
        with self.store.db:
            self.store.put('muse_enabled', True)
        old = self.failed(code='quota_limited', provider='muse')
        await self.settle()
        row = self.store.task_iterations(old['task'])[-1]
        print('\nR1 cause=%s status=%s' % (row['cause'], self.store.task_get(old['task'])['status']))
        for attempt in range(3):
            try:
                await self.resume()
                print('R1 workers_once attempt %d ok' % attempt)
            except Exception as error:
                print('R1 workers_once attempt %d raised %s: %s' % (attempt, type(error).__name__, error))
                raise

    async def test_r1b_muse_quota_stops_the_service_workers_loop(self):
        self.store.put('muse_enabled', True)
        old = self.failed(code='quota_limited', provider='muse')
        await self.settle()
        passes = 0

        async def three_passes():
            nonlocal passes
            for _ in range(3):
                await self.resume()
                passes += 1
        self.service.task_restart_limit = 2
        self.service.task_restart_backoff = 0
        await self.service.supervise('workers', three_passes)
        self.assertEqual(passes, 3)
        self.assertEqual(self.latest(old['task'])['id'], old['id'])

    async def test_r2_held_copy_never_resumes_and_nobody_is_told(self):
        with self.store.db:
            self.store.put('close_clears_space', True)
        old = self.failed(code='quota_limited')
        with self.store.db:
            result = json.loads(old['result'])
            result['waiting_until'] = 1
            self.store.db.execute("UPDATE workers SET status='waiting_for_quota',result=? WHERE id=?", (json.dumps(result), old['id']))
        with patch.object(self.service.codex_accounts, 'select', return_value=None):
            await self.settle()
        job = self.store.task_get(old['task'])
        print('\nR2 after quota: status=%s copy=%s' % (job['status'], job['copy']))
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET copy='held',copy_reason='secret-looking ignored files are held: .env' WHERE id=?", (old['task'],))
        with patch.object(self.service.codex_accounts, 'select', return_value='fixture'):
            for _ in range(5):
                await self.resume()
                await self.settle()
        job = self.store.task_get(old['task'])
        print('R2 after reset: status=%s copy=%s workers=%d owner_waits=%d callbacks=%d' % (job['status'], job['copy'], self.count(old['task']), len(self.store.waits_list(owner=True)), len(self.callbacks())))
        self.assertNotEqual(self.count(old['task']), 1, 'held copy: job never resumes and no wait/nudge is created')

    async def test_r3_auth_failed_resumes_forever(self):
        old = self.failed(code='auth_failed')
        task = old['task']
        spawned = 0
        with patch.object(self.service.codex_accounts, 'select', return_value='fixture-account'):
            for cycle in range(6):
                await self.settle()
                await self.resume()
                new = self.latest(task)
                if new['id'] != old['id']:
                    spawned += 1
                    self.store.worker_complete(new['id'], {'success': False, 'session_id': 'native-session', 'failure_code': 'auth_failed', 'error': 'not logged in'}, 'done')
                    self.store.db.execute("UPDATE messages SET delivered='received' WHERE delivered='pending'")
                    old = self.service.workers.get(new['id'])
        row = self.store.task_iterations(task)[-1]
        print('\nR3 auth_failed cycles=6 new_workers=%d attempts=%s round=%s status=%s' % (spawned, row['attempts'], row['k'], self.store.task_get(task)['status']))
        self.assertLessEqual(spawned, 3, 'account failures are never bounded')

    async def test_r5_resume_button_tells_coordinator_to_decide_and_a_second_worker_starts(self):
        from coordinator import control_api
        old = self.failed()
        with self.store.db:
            self.store.db.execute('UPDATE task_iterations SET attempts=3 WHERE task=?', (old['task'],))
        await self.settle()
        item = next((w for w in self.store.waits_list(owner=True) if any((j['task'] == old['task'] for j in w['jobs']))))
        self.store.wait_fire(item['id'], choice='Resume')
        resumed = self.latest(old['task'])
        callback = self.store.db.execute("SELECT text FROM messages WHERE kind='callback' ORDER BY id DESC LIMIT 1").fetchone()[0]
        print('\nR5 resumed worker=%s status=%s callback=%r' % (resumed['id'], resumed['status'], callback))
        result = control_api.call(self.store, 'workers.spawn', {'task': old['task'], 'provider': 'codex', 'prompt': 'Take another pass at the job'}, source='mcp', topic=TOPIC)
        active = self.store.db.execute("SELECT id FROM workers WHERE task=? AND status IN ('queued','running')", (old['task'],)).fetchall()
        print('R5 coordinator spawn ok=%s text=%r active_workers=%s' % (result.ok, result.text, [r[0] for r in active]))
        self.assertNotIn('Decide the next step', callback, 'coordinator is told to decide while a continuation is already queued')
        self.assertIn('Worker %d queued to continue.' % resumed['id'], callback)

    async def test_r6_off_control(self):
        with self.store.db:
            self.store.put('auto_resume', False)
        await self.test_r6_secret_fill_after_park_starts_nothing()

    async def test_r6_secret_fill_after_park_starts_nothing(self):
        task = self.store.task_create(TOPIC, 'Needs a key', worktree=str(self.root), secrets=['FIXTURE_KEY'])
        now = 1.0
        with self.store.db:
            worker = self.store.db.execute("INSERT INTO workers (task,topic,provider,prompt,cwd,workspace,status,created,updated)\n                VALUES (?,?,?,?,?,?,'waiting_for_secret',?,?)", (task['id'], TOPIC, 'codex', 'use the key', str(self.root), json.dumps({'cwd': str(self.root)}), now, now)).lastrowid
            envelope = self.store.db.execute("INSERT INTO envelopes(name,reason,consumer,task,topic,state,created,expires,updated)\n                VALUES ('FIXTURE_KEY','fixture','worker',?,?,'open',1,9999999999,1)", (task['id'], TOPIC)).lastrowid
        await self.settle()
        await self.settle()
        parked = self.service.workers.get(worker)
        print('\nR6 after two settles: job=%s worker=%s owner_wait=%r' % (self.store.task_get(task['id'])['status'], parked['status'], [w['ask'] for w in self.store.waits_list(owner=True)]))
        with self.store.db:
            self.store.db.execute("UPDATE envelopes SET state='filled',filled_at=2 WHERE id=?", (envelope,))
        run = await self.resume()
        print('R6 after the owner fills the secret: worker=%s launches=%d' % (self.service.workers.get(worker)['status'], run.await_count))
        self.assertEqual(run.await_count, 1, 'filling the secret card no longer starts the parked worker')

    async def test_r7_parked_worker_plus_coordinator_stop_crashes_feed(self):
        from coordinator import control_api
        parked = self.worker(provider='codex')
        task = self.service.workers.get(parked)['task']
        now = 1.0
        with self.store.db:
            self.store.db.execute("UPDATE workers SET status='waiting_for_secret' WHERE id=?", (parked,))
            stopped = self.store.db.execute('INSERT INTO workers (task,topic,provider,prompt,cwd,workspace,created,updated)\n                VALUES (?,?,?,?,?,?,?,?)', (task, TOPIC, 'codex', 'second step', str(self.root), json.dumps({'cwd': str(self.root)}), now, now)).lastrowid
        result = control_api.call(self.store, 'workers.stop', {'worker': stopped}, source='mcp', topic=TOPIC)
        await self.service.controls_once()
        print('\nR7 stop=%s parked=%s stopped=%s code=%s' % (result.ok, self.service.workers.get(parked)['status'], self.service.workers.get(stopped)['status'], json.loads(self.service.workers.get(stopped)['result'])['failure_code']))
        self.store.db.execute("UPDATE messages SET delivered='received' WHERE delivered='pending'")
        await self.settle()
        print('R7 first settle ok, callbacks=%d' % len(self.callbacks()))
        self.store.db.execute("UPDATE messages SET delivered='received' WHERE delivered='pending'")
        for attempt in range(2):
            try:
                await self.service.feed_once()
                print('R7 feed_once %d ok' % attempt)
            except Exception as error:
                print('R7 feed_once %d raised %s: %s' % (attempt, type(error).__name__, error))
                raise

    async def test_r4_result_done_with_failure_resumes_forever(self):
        old = self.failed(code=None, text='All checks pass.\nRESULT: done', success=False)
        task = old['task']
        spawned = 0
        for cycle in range(6):
            await self.settle()
            with patch('time.time', return_value=10 ** 10 + cycle * 1000):
                await self.resume()
            new = self.latest(task)
            if new['id'] != old['id']:
                spawned += 1
                self.store.worker_complete(new['id'], {'success': False, 'session_id': 'native-session', 'text': 'All checks pass.\nRESULT: done'}, 'done')
                old = self.service.workers.get(new['id'])
        row = self.store.task_iterations(task)[-1]
        print('\nR4 success=false+RESULT: done cycles=6 new_workers=%d attempts=%s status=%s owner_waits=%d' % (spawned, row['attempts'], self.store.task_get(task)['status'], len(self.store.waits_list(owner=True))))
        self.assertLessEqual(spawned, 3, 'unfinished bound is reset by the failed result itself')
