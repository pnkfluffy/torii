import asyncio
import hashlib
from dataclasses import asdict
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, Mock, patch

from coordinator import host_os, parent_state
from coordinator.host import HostClient, _reap_launchers, alive
from coordinator.accounts import LIMIT_CONTINUATION, AccountBroker
from coordinator.codex_accounts import CODEX_SKILLS_POINTER, codex_skills_pointer
from coordinator.native_protocol import RunControl
from coordinator.providers import ProviderRunner, RunResult
from coordinator.policy import usage_policy
from coordinator.store import Store
from tests.support import FakeVault
from coordinator.workers import WORKER_POLICY, WorkerPool, can_resume_finished


TOPIC = '-10042:4'

from tests.support import expected_codex_skills_pointer, isolate_shared_mcp_home, read_jsonl, settle, stop_test_hosts, until, until_exists, until_text
from tests.test_codex_secrets_goals import SERVER as CODEX_GOAL_SERVER

class WorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_on_demand_tools_follow_goals_changed_while_queued_or_waiting(self):
        from coordinator import control_api
        import uuid
        binary = self.root / 'fake-codex'
        binary.write_text(CODEX_GOAL_SERVER.replace('SID', repr(str(uuid.uuid4()))).replace('ROOT', repr(str(self.root))))
        binary.chmod(0o700)
        pool = WorkerPool(self.store, ProviderRunner(self.store.directory, binaries={'codex': str(binary)}),
                          AccountBroker(self.store))
        for status in ('queued', 'waiting_for_secret', 'waiting_for_quota'):
            for condition in ('queued goal', 'clear'):
                with self.subTest(status=status, condition=condition), \
                        patch.dict(os.environ, {'FAKE_GOAL_STATUS': 'complete'}):
                    spawned = control_api.call(self.store, 'workers.spawn', {
                        'task': self.task['id'], 'provider': 'codex', 'prompt': status + condition, 'goal': 'old goal'})
                    self.assertTrue(spawned.ok, spawned.text)
                    worker = pool.get(spawned.data['worker'])
                    self.store.db.execute('UPDATE workers SET status=? WHERE id=?', (status, worker['id']))
                    reply = control_api.call(self.store, 'workers.goal', {'worker': worker['id'], 'condition': condition})
                    self.assertTrue(reply.ok, reply.text)
                    if status == 'waiting_for_quota':
                        self.store.db.execute("UPDATE workers SET status='queued' WHERE id=?", (worker['id'],))
                    done = await pool.run(worker, self.store.topic(TOPIC))
                    result = json.loads(done['result'])
                    self.assertTrue(result['success'], result.get('error'))
                    spec = json.loads(Path(result['log_path']).with_name('spec.json').read_text())
                    self.assertEqual(spec['initial_goal'], None if condition == 'clear' else condition)
                    self.assertEqual('features.goals=true' in spec['argv'], condition != 'clear')

    async def test_d12_chat_delegation_does_not_reach_workers(self):
        from coordinator.policy import worker_usage_policy
        self.store.put('coordinator_home_topic', TOPIC)
        self.store.put('coordinator_delegation', {'text': 'Chat-only instructions.', 'revision': 1})
        self.store.put('coordinator:' + TOPIC + '_delegation', {'text': 'Other chat-only instructions.', 'revision': 1})
        captured = {}
        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                captured.update(options)
                return RunResult(session or '00000000-0000-4000-8000-000000000001', text='done', success=True)
        for provider in ('claude', 'codex'):
            worker = self.worker(provider)
            await WorkerPool(self.store, Runner(), AccountBroker(self.store)).run(worker, self.store.topic(TOPIC))
            expected = (WORKER_POLICY + '\nCurrent usage policy:\n' + worker_usage_policy(self.store.directory) +
                        '\nJob: ' + json.dumps({'id': self.task['id'], 'number': self.task['number'],
                                                 'topic': 'Torii', 'worktree': self.task['worktree'],
                                                 'results': str(self.store.directory / 'results' / ('task-' + str(self.task['id'])))}))
            expected += ("\n$TMPDIR is this worker's own temp folder; Torii deletes it when the worker finishes. "
                         "Files there may be deleted outright."
                         "\nKeep reports, logs and proofs in the job's results folder.")
            if provider == 'codex':
                expected += '\n' + CODEX_SKILLS_POINTER
            self.assertEqual(captured['instructions'], expected)

    def test_worker_policy_does_not_require_a_final_question(self):
        self.assertNotIn('If you stop to ask the owner or the coordinator a question, '
                         'end your final message with that question.', WORKER_POLICY)

    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)
        isolate_shared_mcp_home(self, self.root)
        self.addCleanup(stop_test_hosts, self.root)
        self.store = Store(self.root / 'state')
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', (TOPIC, -10042, 4, 'Torii', str(self.root)))
            home = self.root / 'primary'
            home.mkdir()
            self.store.put('accounts', {'primary': {'config_dir': str(home), 'enabled': True}})
            self.store.put('account_status', {'primary': {'identity': {'email': 'primary@example.com', 'logged_in': True},
                'observed_at': time.time(), 'usage': {'seven_day': {'utilization': 10,
                'resets_at': time.time() + 3600}}}})
        self.task = self.store.task_create(TOPIC, 'Build', worktree=str(self.root))

    async def asyncTearDown(self):
        self.store.close()

    async def check_absolute_skills_launches(self, attached):
        homes = {alias: self.root / ('account ' + alias) for alias in ('X', 'Y')}
        for home in homes.values():
            home.mkdir()
        self.store.put('codex_accounts', {alias: {'enabled': True, 'config_dir': str(home)}
                                         for alias, home in homes.items()})
        self.store.put('codex_account_status', {alias: {'identity': {
            'email': alias + '@example.test', 'logged_in': True}, 'observed_at': time.time()}
            for alias in homes})
        binary = self.root / 'fake-codex'
        runner = ProviderRunner(self.store.directory, binaries={'codex': str(binary)})
        pool = WorkerPool(self.store, runner, AccountBroker(self.store))
        notes = []
        for alias, home in homes.items():
            with self.subTest(account=alias, attached=attached):
                sid = '00000000-0000-4000-8000-00000000000' + str(len(notes) + 1)
                binary.write_text(CODEX_GOAL_SERVER.replace('SID', repr(sid)).replace('ROOT', repr(str(self.root))))
                binary.chmod(0o700)
                self.store.put('codex_active_account', alias)
                worker = self.worker('codex')
                if attached:
                    worker.update(session=sid, fresh=0)
                    with patch.object(pool.codex_accounts, 'record_rate_limit'), \
                            patch.object(pool.codex_accounts, 'automatic', return_value=True), \
                            patch.object(pool.codex_accounts, 'select', return_value=alias), \
                            patch.object(runner, '_external_writer', AsyncMock(return_value=False)):
                        result = await pool.codex_attached(worker, self.store.topic(TOPIC),
                            RunResult(sid, quota_limited=True), {'instructions': None, 'control': RunControl()}, lambda value: None)
                    self.assertTrue(result.success, result.error)
                    directory = Path(result.log_path).parent
                else:
                    finished = await pool.run(worker, self.store.topic(TOPIC))
                    result = json.loads(finished['result'])
                    self.assertTrue(result['success'], result.get('error'))
                    directory = Path(result['log_path']).parent
                spec = json.loads((directory / 'spec.json').read_text())
                env = spec['env']
                self.assertEqual(env['CODEX_HOME'], str(home))
                self.assertEqual(env['HOME'], os.environ['HOME'])
                expected = expected_codex_skills_pointer(env['HOME'], home)
                payload = spec['initial_payload']
                note = payload[payload.index('Skills:'):].split('\n\n', 1)[0]
                notes.append(note)
                self.assertEqual(note, expected)
                self.assertNotIn('$CODEX_HOME', note)
                self.assertNotIn('~/', note)
                self.assertNotIn('If CODEX_HOME is unset', note)
                self.assertIn('skills.include_instructions=false', spec['argv'])
                requests = read_jsonl(directory / 'stderr.log')
                turn = next(item for item in requests if item.get('method') == 'turn/start')
                self.assertIn(expected, turn['params']['input'][0]['text'])
        self.assertNotEqual(*notes)

    async def test_a1_worker_skills_paths_match_each_launch_environment(self):
        await self.check_absolute_skills_launches(False)

    async def test_a3_attached_worker_transfer_skills_paths_match_each_launch_environment(self):
        await self.check_absolute_skills_launches(True)

    def test_a4_absolute_note_preserves_origin_main_text(self):
        for alias in ('X', 'Y'):
            with self.subTest(account=alias):
                env = {'HOME': '/fixture/launch home', 'CODEX_HOME': '/fixture/account ' + alias}
                note = codex_skills_pointer(env)
                self.assertEqual(note, expected_codex_skills_pointer(env['HOME'], env['CODEX_HOME']))
                self.assertNotIn('$CODEX_HOME', note)
                self.assertNotIn('~/', note)
                self.assertNotIn('If CODEX_HOME is unset', note)

    async def test_worker_start_callback_reacts_but_reattachment_does_not(self):
        message = self.store.message_save(TOPIC, 'owner', 'Build', telegram_message=501)
        self.store.db.execute('UPDATE tasks SET origin=? WHERE id=?', (message['id'], self.task['id']))
        observed = []
        store = self.store

        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                options['on_start'](123)
                observed.append(store.db.execute('SELECT reaction_desired FROM messages WHERE id=?',
                                                 (message['id'],)).fetchone()[0])
                return RunResult(session or '00000000-0000-4000-8000-000000000001', text='done', success=True)

        worker = self.worker()
        self.store.db.execute("UPDATE workers SET work='dev' WHERE id=?", (worker['id'],))
        pool = WorkerPool(self.store, Runner(), AccountBroker(self.store))
        await pool.run(worker, self.store.topic(TOPIC))
        self.assertEqual(observed, ['👨\u200d💻'])
        attached = self.worker()
        self.store.db.execute("UPDATE workers SET status='running',account_alias='primary',session=? WHERE id=?",
                              ('00000000-0000-4000-8000-000000000002', attached['id']))
        await pool.run(attached, self.store.topic(TOPIC))
        self.assertEqual(observed, ['👨\u200d💻', '👨\u200d💻'])

    async def test_worker_gets_current_policy_and_effort(self):
        (self.store.directory / 'USAGE.md').write_text('Use one worker.\n\nKeep spaces.  \nUnicode 日本語\n')
        self.worker('codex')
        captured = {}

        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                captured.update(options)
                return RunResult(session or '00000000-0000-4000-8000-000000000001', text='done', success=True)

        row = dict(self.store.db.execute('SELECT * FROM workers ORDER BY id DESC LIMIT 1').fetchone())
        await WorkerPool(self.store, Runner(), AccountBroker(self.store)).run(row, self.store.topic(TOPIC))
        self.assertEqual(captured['effort'], 'medium')
        self.assertEqual(captured['instructions'].split('Current usage policy:\n', 1)[1].split('\nJob: ', 1)[0],
                         usage_policy(self.store.directory))
        self.assertNotIn('Owner delegation ' + 'settings', captured['instructions'])
        self.assertIn('Job: ', captured['instructions'])
        self.assertEqual(captured['model'], 'gpt-6.1-sol')

    async def test_a5_instruction_baselines_for_other_providers(self):
        """Switch-on instruction baselines after shared job storage rules."""
        expected = {
            'codex': ['be91201e877d04b89421b7a5fd85cc35a360c3591bbccfe16d6611e3310377a5', '0d5478e2324b50efebaeda101d8a01aa87f281031986c26f83a89db85a54f51c'],
            'claude': ['f6db27bd6d73df055b5f0e99f062633d41115010fe1b67524725e22d264dd623', '6818991975483c876e717e9393227a78226e2a0d9464abe06f0df857c6ab4f40'],
        }
        from coordinator import extension
        if not extension.active().worker_policy:
            expected = {
                'codex': ['58142726bdc5752189955282fdc295a678e219ce8bd6be4256dcdb660b367e78', '834e8907337f42a5c2f4ad7a1c75712e13fed5197a16feb68e2ffcfbfb4b49c4'],
                'claude': ['9e9c8a09692914804f433d00f75001fb7320aa4fe0ab1532ba39672c06428d9b', 'cdaa7649aa404fed388904835debb954924be8c5df032d750be0fbf08d017bbb'],
            }
        (self.store.directory / 'USAGE.md').write_text('Fixture usage.\n')

        async def run(*args, **options):
            return await args[0](*args[1:], **options)

        for provider in ('codex', 'claude', 'opencode'):
            for secret in (False, True):
                for fresh in (True, False):
                    with self.subTest(provider=provider, secret=secret, fresh=fresh):
                        worker = self.worker(provider)
                        worker['fresh'] = int(fresh)
                        runner = SimpleNamespace(run=AsyncMock(return_value=RunResult(None, text='done', success=True)))
                        pool = WorkerPool(self.store, runner, AccountBroker(self.store))
                        broker = SimpleNamespace(run=run, run_active=run)
                        with patch.object(pool, 'broker', return_value=broker), \
                                patch('coordinator.workers.spawn_refusal', return_value=None), \
                                patch('coordinator.workers.load_for_worker', return_value=({'TEST_KEY': 'fixture'} if secret else {}, [])):
                            await pool.run(worker, self.store.topic(TOPIC))
                        instructions = runner.run.await_args.kwargs['instructions']
                        temp_note = ("\n$TMPDIR is this worker's own temp folder; Torii deletes it when the worker finishes. "
                                     "Files there may be deleted outright.")
                        if provider != 'opencode':
                            temp_note += "\nKeep reports, logs and proofs in the job's results folder."
                        self.assertEqual(instructions.count(temp_note), 1)
                        baseline = instructions.replace(temp_note, '', 1)
                        job_line = instructions.split('\nJob: ', 1)[1].split('\n', 1)[0]
                        job = json.loads(job_line)
                        if provider == 'opencode':
                            self.assertNotIn('results', job)
                        else:
                            self.assertEqual(job.pop('results'),
                                             str(self.store.directory / 'results' / ('task-' + str(self.task['id']))))
                        normalized = baseline.replace(job_line, json.dumps(job)).replace(str(self.root), '<worktree>').replace(str(host_os.trash_directory()), '<trash>')
                        normalized = normalized.replace('macOS Trash', 'host Trash').replace('Linux Trash', 'host Trash')
                        self.assertEqual(hashlib.sha256(json.dumps(normalized).encode()).hexdigest(),
                                         expected['codex' if provider == 'codex' else 'claude'][secret])
                        self.assertEqual(instructions.count(CODEX_SKILLS_POINTER), int(provider == 'codex'))
                        if provider == 'codex':
                            self.assertTrue(instructions.endswith('\n' + CODEX_SKILLS_POINTER))

    async def test_codex_skills_pointer_ends_account_transfer_instructions(self):
        """Switch-on baselines captured from origin/main at d5d0a00eb9dd42547135705bb88dca995b8e5c0b."""
        expected = ['520d1046edace5d8af1fec6206ac56a4d5ada13a7809e54fbceec1463edf1adf', 'f88b95717f252982ce5d680bcbc8576f8366bfe6b0da0bef739aa377d723664e']
        pool = WorkerPool(self.store, SimpleNamespace(run=AsyncMock()), AccountBroker(self.store))
        broker = pool.codex_accounts
        for secret in (False, True):
            with self.subTest(secret=secret):
                broker.run = AsyncMock(return_value=RunResult(None, success=True))
                with patch.object(broker, 'record_rate_limit'), patch.object(broker, 'automatic', return_value=True), \
                        patch.object(broker, 'select', return_value='next'), \
                        patch('coordinator.workers.load_for_worker', return_value=({'TEST_KEY': 'fixture'} if secret else {}, [])):
                    await pool.codex_attached(self.worker('codex'), self.store.topic(TOPIC),
                        RunResult(None, quota_limited=True), {'instructions': None}, lambda alias: None)
                instructions = broker.run.await_args.kwargs['instructions']
                self.assertEqual(hashlib.sha256(json.dumps(instructions).encode()).hexdigest(), expected[secret])
                self.assertEqual(instructions.count(CODEX_SKILLS_POINTER), 1)
                self.assertTrue(instructions.endswith(CODEX_SKILLS_POINTER))

    async def test_worker_omits_only_coordinators_sections(self):
        cases = (
            ('middle', 'Before.  \n\n## Coordinators\nOwner only.\n### Replies\nReply now.\n\n'
             '## Effort\nKeep this.  \n日本語\n', 'Before.  \n\n## Effort\nKeep this.  \n日本語\n'),
            ('end', 'Before.\n\n## Coordinators\nOwner only.', 'Before.\n\n'),
            ('trailing whitespace', 'Before.\n## Coordinators \t\nOwner only.\n## Review\nKeep.',
             'Before.\n## Review\nKeep.'),
            ('repeated', '## Coordinators\nFirst.\n## Work\nKeep.\n## Coordinators\nLast.\n',
             '## Work\nKeep.\n'),
            ('near matches', ' ## Coordinators\nKeep.\n## Coordinators extra\nKeep too.\n### Coordinators\nEnd.',
             ' ## Coordinators\nKeep.\n## Coordinators extra\nKeep too.\n### Coordinators\nEnd.'),
        )
        captured = {}

        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                captured.update(options)
                return RunResult(session or '00000000-0000-4000-8000-000000000001', text='done', success=True)

        for provider in ('claude', 'codex'):
            for name, policy, expected in cases:
                with self.subTest(provider=provider, section=name):
                    (self.store.directory / 'USAGE.md').write_text(policy)
                    row = self.worker(provider)
                    await WorkerPool(self.store, Runner(), AccountBroker(self.store)).run(row, self.store.topic(TOPIC))
                    self.assertEqual(captured['instructions'].split('Current usage policy:\n', 1)[1].split('\nJob: ', 1)[0],
                                     expected)

    def worker(self, provider='claude', goal=None, model=None):
        with self.store.db:
            row = self.store.db.execute('''INSERT INTO workers
                (task,topic,provider,prompt,cwd,workspace,goal,model,created,updated)
                VALUES (?,?,?,?,?,?,?,?,?,?)''',
                (self.task['id'], TOPIC, provider, 'build this', str(self.root),
                 json.dumps({'cwd': str(self.root)}), goal, model, 1.0, 1.0))
        return dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (row.lastrowid,)).fetchone())

    def test_finished_resume_gate_supports_only_resumable_providers(self):
        self.store.put('opencode_enabled', True)
        worker = self.worker()
        self.store.db.execute("UPDATE workers SET status='done',fresh=0,session='saved' WHERE id=?", (worker['id'],))
        worker = dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (worker['id'],)).fetchone())
        for provider in ('claude', 'codex', 'opencode', 'unknown'):
            with self.subTest(provider=provider):
                worker['provider'] = provider
                self.assertEqual(can_resume_finished(self.store, worker), provider in ('claude', 'codex', 'opencode'))

    async def test_finished_worker_resumes_after_wait_reopens_job_with_session_cleanup_off(self):
        from coordinator import control_api
        from coordinator.service import Service
        self.store.put('session_delete', False)
        self.store.put('waits_enabled', True)
        worker = self.worker()
        session_id = '00000000-0000-4000-8000-000000000001'
        transcript = self.root / 'primary' / 'projects' / 'test' / (session_id + '.jsonl')
        transcript.parent.mkdir(parents=True)
        transcript.write_text('{}\n')
        self.store.worker_complete(worker['id'], {'success': True, 'session_id': session_id,
                                                  'transcript_path': str(transcript)})
        self.store.task_wait(TOPIC, 'signal', 'Wait for CI', task=self.task['id'], signal='ci.passed')
        self.assertEqual(self.store.task_get(self.task['id'])['status'], 'waiting')
        message = self.store.message_save(TOPIC, 'owner', 'CI passed, continue.')
        reopened = self.store.wait_open_round(self.task['id'], message['id'], opened_by='owner')
        self.assertEqual((reopened['status'], reopened['iteration']), ('open', worker['iteration'] + 1))
        calls = []

        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                calls.append((prompt, session, options['fresh']))
                return RunResult(session, text='done', success=True, transcript_path=str(transcript))

        service = Service(self.store, None, Runner(), self.root)
        with self.store.db:
            requested = control_api.call(self.store, 'workers.steer', {
                'worker': worker['id'], 'prompt': 'CI passed, continue.'}, topic=TOPIC, source='cli')
        self.assertTrue(requested.ok, requested.text)
        await service.controls_once()
        queued = service.workers.get(worker['id'])
        self.assertEqual((queued['status'], queued['session'], queued['fresh']), ('queued', session_id, 0))
        finished = await service.workers.run(queued, self.store.topic(TOPIC))
        self.assertEqual(finished['status'], 'done')
        self.assertTrue(json.loads(finished['result'])['success'])
        self.assertEqual(calls, [('CI passed, continue.', session_id, False)])

    def test_finished_worker_with_deletion_plan_cannot_be_steered_after_job_reopens(self):
        from coordinator import control_api
        from coordinator.session_storage import remember
        self.store.put('session_delete', True)
        worker = self.worker()
        session_id = '00000000-0000-4000-8000-000000000001'
        self.store.worker_complete(worker['id'], {'success': True, 'session_id': session_id})
        with self.store.db:
            native = remember(self.store, 'claude', session_id, str(self.root / 'primary'), str(self.root),
                              worker=worker['id'], created=True)
            self.store.db.execute('''INSERT INTO session_deletions(native,provider,session,home,root,path)
                SELECT id,provider,session,home,root,? FROM native_sessions WHERE id=?''',
                ('test/' + session_id + '.jsonl', native))
        self.store.task_update(self.task['id'], status='done')
        message = self.store.message_save(TOPIC, 'owner', 'Reopen')
        reopened = self.store.wait_open_round(self.task['id'], message['id'], opened_by='owner')
        self.assertEqual(reopened['iteration'], worker['iteration'] + 1)
        for state in ('pending', 'deleted'):
            with self.subTest(state=state), self.store.db:
                self.store.db.execute('UPDATE session_deletions SET state=? WHERE native=?', (state, native))
                requested = control_api.call(self.store, 'workers.steer', {
                    'worker': worker['id'], 'prompt': 'Continue.'}, topic=TOPIC, source='cli')
                self.assertFalse(requested.ok)
                self.assertIn('Worker cannot receive input.', requested.text)
        self.assertIsNone(self.store.db.execute(
            "SELECT 1 FROM service_requests WHERE op='workers.steer'").fetchone())

    def recorded_worker(self, cleanup=None, exited=True):
        from tests.test_process_ownership import save_owner
        worker = self.worker('codex')
        directory = self.store.directory / 'hosts' / ('worker-' + str(worker['id']))
        directory.mkdir(parents=True)
        spec = dict(argv=[sys.executable, '-c', 'pass'], cwd=str(self.root), env={}, provider='codex', locks=[])
        if cleanup is not None:
            spec['ownership'] = 'coalition'
            save_owner(directory)
            record = json.loads((directory / 'owner.json').read_text())
            record.update(cleanup=cleanup, reported=True)
            (directory / 'owner.json').write_text(json.dumps(record))
        (directory / 'spec.json').write_text(json.dumps(spec))
        (directory / 'pid').write_text('100')
        (directory / 'child.pid').write_text('101')
        (directory / 'events.jsonl').touch()
        (directory / 'writes.jsonl').touch()
        if exited:
            (directory / 'exit.json').write_text(json.dumps({'returncode': 0}))
        with self.store.db:
            self.store.db.execute("UPDATE workers SET status='running',host=?,session=?,fresh=0 WHERE id=?",
                                  (directory.name, '00000000-0000-4000-8000-000000000009', worker['id']))
        pool = WorkerPool(self.store, ProviderRunner(self.store.directory), AccountBroker(self.store))
        return pool, pool.get(worker['id']), directory

    def recovery_service(self):
        from coordinator.service import Service
        from tests.test_host import FakeTelegram
        service = Service(self.store, FakeTelegram(), ProviderRunner(self.store.directory), self.root, pair_only=True)
        service.pair_only = False
        return service

    async def test_finished_host_repeats_stop_only_for_unfinished_owned_cleanup(self):
        from coordinator import host
        for exited in (False, True):
            for cleanup in (None, 'ok', 'stale', 'pending', 'failed'):
                with self.subTest(exited=exited, cleanup=cleanup):
                    pool, worker, directory = self.recorded_worker(cleanup, exited=exited)
                    finished = RunResult(worker['session'], text='finished', success=True)
                    with patch.object(host, 'alive', return_value=False), \
                            patch.object(pool.runner, 'run', AsyncMock(return_value=finished)), \
                            patch.object(HostClient, 'stop', AsyncMock()) as stop:
                        done = await pool.run(worker, self.store.topic(TOPIC))
                    self.assertEqual(stop.await_count, int(cleanup in ('pending', 'failed')))
                    self.assertEqual(done['status'], 'done')
                    self.assertTrue(json.loads(done['result'])['success'])

    async def test_running_unowned_host_gets_one_stop_before_worker_finishes(self):
        from coordinator import host
        pool, worker, directory = self.recorded_worker(exited=False)
        finished = RunResult(worker['session'], text='finished', success=True)
        with patch.object(host, 'alive', return_value=True), \
                patch.object(pool.runner, 'run', AsyncMock(return_value=finished)), \
                patch.object(HostClient, 'stop', AsyncMock()) as stop:
            done = await pool.run(worker, self.store.topic(TOPIC))
        stop.assert_awaited_once()
        self.assertEqual(done['status'], 'done')

    async def test_failed_owned_cleanup_finishes_once_and_owner_stop_can_finish_another_worker(self):
        from coordinator import host, process_ownership as ownership
        from tests.test_process_ownership import FakeBackend, member
        fake = FakeBackend([member(101, unique=1001)])
        fake.sticky = True
        for owner_stop in (False, True):
            with self.subTest(owner_stop=owner_stop):
                pool, worker, directory = self.recorded_worker('failed')
                finished = RunResult(worker['session'], text='finished', success=True)
                with patch.object(ownership, 'backend', fake), patch.object(host, 'alive', return_value=False), \
                        patch.object(pool.runner, 'run', AsyncMock(return_value=finished)), \
                        patch.object(ownership, 'cleanup', wraps=ownership.cleanup) as cleanup:
                    if owner_stop:
                        await pool.stop(worker['id'])
                        done = pool.get(worker['id'])
                    else:
                        done = await pool.run(worker, self.store.topic(TOPIC))
                    await pool.stop(worker['id'])
                    self.assertEqual(cleanup.call_count, 3)
                self.assertEqual(done['status'], 'interrupted')
                self.assertEqual(json.loads(done['result'])['failure_code'], 'cleanup_failed')
                self.assertEqual(ownership.read_owner(directory)['cleanup_attempts'], 3)
                self.assertEqual(self.store.db.execute(
                    "SELECT COUNT(*) FROM messages WHERE kind='worker_result' AND source_worker=?",
                    (worker['id'],)).fetchone()[0], 1)
                rows = list(self.store.db.execute("SELECT code,detail FROM problems WHERE worker=?", (worker['id'],)))
                self.assertEqual([row['code'] for row in rows], ['cleanup_failed'])
                self.assertIn('101', rows[0]['detail'])

    async def test_dead_host_cleanup_failure_finishes_instead_of_reentering_the_workers_loop(self):
        from coordinator import host, process_ownership as ownership
        from tests.test_process_ownership import FakeBackend, member
        pool, worker, directory = self.recorded_worker('pending', exited=False)
        fake = FakeBackend([member(101, unique=1001)])
        fake.sticky = True
        service = self.recovery_service()
        with patch.object(ownership, 'backend', fake), patch.object(host, 'alive', return_value=False):
            for _ in range(5):
                await service.workers_once()
        done = pool.get(worker['id'])
        self.assertEqual(done['status'], 'interrupted')
        self.assertEqual(json.loads(done['result'])['failure_code'], 'cleanup_failed')
        self.assertEqual(ownership.read_owner(directory)['cleanup_attempts'], 3)
        self.assertEqual(self.store.db.execute(
            "SELECT COUNT(*) FROM problems WHERE worker=? AND code='cleanup_failed'", (worker['id'],)).fetchone()[0], 1)

    async def test_reused_pid_after_boot_does_not_delay_or_signal_during_worker_recovery(self):
        from coordinator import host, process_ownership as ownership
        from tests.test_process_ownership import FakeBackend, member
        pool, worker, directory = self.recorded_worker('stale', exited=False)
        fake = FakeBackend([member(unique=2000, resource=99)])
        fake.boot_id = 'next-boot'
        service = self.recovery_service()
        resumed = RunResult(worker['session'], text='resumed', success=True)
        with patch.object(ownership, 'backend', fake), patch.object(host, 'alive', return_value=True), \
                patch('coordinator.workers.alive', return_value=False), \
                patch.object(service.runner, 'run', AsyncMock(return_value=resumed)) as run:
            await asyncio.wait_for(service.workers_once(), 1)
            await asyncio.gather(*service.worker_tasks.values())
            if pool.get(worker['id'])['status'] == 'queued':
                await asyncio.wait_for(service.workers_once(), 1)
                await asyncio.gather(*service.worker_tasks.values())
        done = pool.get(worker['id'])
        self.assertEqual(done['status'], 'done')
        self.assertTrue(json.loads(done['result'])['success'])
        self.assertEqual(done['session'], worker['session'])
        self.assertEqual(run.await_count, 1)
        args, options = run.await_args
        self.assertEqual(args[3], worker['session'])
        self.assertFalse(options['fresh'])
        self.assertFalse(options['attach'])
        self.assertEqual(fake.signals, [])
        self.assertEqual(fake.now, 0)

    async def test_exited_worker_with_reused_host_pid_finishes_without_waiting_on_that_process(self):
        from coordinator import host
        pool, worker, directory = self.recorded_worker()
        finished = RunResult(worker['session'], text='finished', success=True)
        with patch.object(host, 'alive', return_value=True), \
                patch.object(pool.runner, 'run', AsyncMock(return_value=finished)):
            done = await asyncio.wait_for(pool.run(worker, self.store.topic(TOPIC)), 1)
        self.assertEqual(done['status'], 'done')
        self.assertTrue(json.loads(done['result'])['success'])

    async def test_owner_stop_with_reused_pid_is_handled_by_the_controls_loop(self):
        from coordinator import host
        pool, worker, directory = self.recorded_worker(exited=False)
        service = self.recovery_service()
        with self.store.db:
            request = self.store.service_request('workers.stop', {'worker': worker['id']})
        with patch.object(host, 'alive', side_effect=lambda pid: int(pid) == 100), \
                patch.object(host, 'HOST_TIMEOUT', .01), \
                patch.object(HostClient, 'request', AsyncMock(side_effect=FileNotFoundError('fixture missing socket'))):
            await service.controls_once()
        self.assertEqual(self.store.db.execute('SELECT state FROM service_requests WHERE id=?',
                                              (request,)).fetchone()[0], 'uncertain')
        self.assertEqual(pool.get(worker['id'])['status'], 'running')

    async def test_cleanup_failure_does_not_suppress_runner_cancellation(self):
        from coordinator import host
        pool, worker, directory = self.recorded_worker()
        finished = RunResult(worker['session'], text='finished', success=True)
        with patch.object(pool.runner, 'run', AsyncMock(return_value=finished)), \
                patch.object(pool, 'codex_attached', AsyncMock(side_effect=asyncio.CancelledError)), \
                patch.object(HostClient, 'stop', AsyncMock(side_effect=RuntimeError('fixture stop failure'))):
            with self.assertRaises(asyncio.CancelledError):
                await pool.run(worker, self.store.topic(TOPIC))
        self.assertEqual(pool.get(worker['id'])['status'], 'running')
        self.assertNotIn(worker['id'], pool.tasks)
        self.assertEqual(self.store.db.execute(
            "SELECT COUNT(*) FROM messages WHERE kind='worker_result' AND source_worker=?",
            (worker['id'],)).fetchone()[0], 0)

    async def ending_worker(self, outcome, stubborn=False, detached=False):
        session = '00000000-0000-4000-8000-000000000009'
        worker = self.worker('codex')
        directory = self.store.directory / 'hosts' / ('worker-' + str(worker['id']))
        binary = self.root / 'ending-provider.py'
        binary.write_text('''import json,os,signal,subprocess,sys,time
from pathlib import Path
root=Path(sys.argv[1])
outcome=sys.argv[2]
sid=sys.argv[3]
child=None
def emit(event):
    print(json.dumps(event),flush=True)
for line in sys.stdin:
    request=json.loads(line)
    method=request.get('method')
    if method=='initialize':
        emit({'id':request['id'],'result':{}})
    elif method in ('thread/start','thread/resume'):
        emit({'id':request['id'],'result':{'thread':{'id':sid}}})
    elif method=='turn/start':
        child=subprocess.Popen([sys.executable,'-c',
            'import os,signal,sys,time; from pathlib import Path; '
            'signal.signal(signal.SIGTERM,signal.SIG_IGN if sys.argv[2]=="True" else signal.SIG_DFL); '
            'Path(sys.argv[1]).write_text(str(os.getpid())); deadline=time.monotonic()+120; '
            'exec("while time.monotonic()<deadline and not Path(sys.argv[3]).exists(): time.sleep(.01)")',
            str(root/'ending-child'),sys.argv[4],str(root/'release-child')],start_new_session=sys.argv[5]=='True',
            stdin=subprocess.DEVNULL,stdout=None if outcome=='held-child' else subprocess.DEVNULL,
            stderr=subprocess.DEVNULL)
        while not (root/'ending-child').exists():
            time.sleep(.01)
        emit({'id':request['id'],'result':{'turn':{'id':'turn-one'}}})
        emit({'method':'turn/started','params':{'threadId':sid,'turn':{'id':'turn-one'}}})
        if outcome=='exception':
            emit({'id':'unsupported','method':'unsupported/interaction','params':{}})
            time.sleep(120)
        elif outcome in ('success','held','held-child'):
            emit({'method':'item/completed','params':{'threadId':sid,'turnId':'turn-one',
                'item':{'type':'agentMessage','text':'finished'}}})
            emit({'method':'turn/completed','params':{'threadId':sid,
                'turn':{'id':'turn-one','status':'completed'}}})
            (root/'ending-terminal').touch()
            if outcome=='held':
                while not (root/'release-provider').exists():
                    time.sleep(.01)
            if outcome=='held-child':
                break
        else:
            (root/'ending-active').touch()
    elif method=='turn/steer':
        emit({'id':request['id'],'result':{'turnId':'turn-one'}})
if child is not None:
    try:
        child.wait(timeout=.1)
    except subprocess.TimeoutExpired:
        pass
''')
        spec = dict(argv=[sys.executable, '-u', str(binary), str(self.root), outcome, session,
                          str(stubborn), str(detached)], cwd=str(self.root), env={'PATH': os.environ['PATH']},
                    provider='codex', locks=['codex-' + session], initial_payload='build this', fresh=True, model=None)
        if detached:
            spec['ownership'] = 'coalition'
        client = await HostClient.launch(directory, spec, stop_timeout=.1)
        self.addCleanup(client.detach)
        self.addAsyncCleanup(client.stop)
        with self.store.db:
            self.store.db.execute("UPDATE workers SET status='running',host=?,session=?,fresh=0,account_alias='fixture' WHERE id=?",
                                  (directory.name, session, worker['id']))
        runner = ProviderRunner(self.store.directory)
        pool = WorkerPool(self.store, runner, AccountBroker(self.store))
        return pool, pool.get(worker['id']), client

    async def assert_host_exited(self, client):
        host = int((client.directory / 'pid').read_text())
        self.assertTrue((client.directory / 'exit.json').is_file())
        self.assertEqual(client.state, 'exited')
        def reaped():
            _reap_launchers()
            return not alive(host)
        await until(reaped, 'the ended host to be reaped')
        self.assertFalse(alive(host), 'host is alive after worker completion')

    async def assert_worker_processes_ended(self, worker, client):
        child = int(await until_text(self.root / 'ending-child'))
        status = worker['status']
        self.assertFalse(alive(child), 'worker status=%s but provider child pid=%s is alive' % (status, child))
        self.assertFalse(alive(client.child_pid), 'provider is alive after worker completion')
        await self.assert_host_exited(client)
        self.assertEqual(self.store.db.execute(
            "SELECT COUNT(*) FROM messages WHERE kind='worker_result' AND source_worker=?",
            (worker['id'],)).fetchone()[0], 1)

    async def test_attached_protocol_exception_stops_tree_before_recording_result(self):
        pool, worker, client = await self.ending_worker('exception')
        done = await settle(pool.run(worker, self.store.topic(TOPIC)))
        self.assertEqual(done['status'], 'done')
        self.assertEqual(json.loads(done['result'])['failure_code'], 'execution_failed')
        await self.assert_worker_processes_ended(done, client)

    async def test_cleanup_failure_keeps_worker_running_without_a_final_result(self):
        pool, worker, client = await self.ending_worker('exception')
        with patch.object(HostClient, 'stop', AsyncMock(side_effect=RuntimeError('fixture stop failure'))):
            done = await settle(pool.run(worker, self.store.topic(TOPIC)))
        self.assertEqual(done['status'], 'running')
        self.assertEqual(pool.get(worker['id'])['status'], 'running')
        self.assertTrue(alive(client.child_pid))
        self.assertEqual(self.store.db.execute(
            "SELECT COUNT(*) FROM messages WHERE source_worker=?", (worker['id'],)).fetchone()[0], 0)
        self.assertTrue(self.store.db.execute(
            "SELECT 1 FROM problems WHERE worker=? AND code='stop-failed'", (worker['id'],)).fetchone())

    async def test_runner_exception_stops_host_before_recording_result(self):
        pool, worker, client = await self.ending_worker('active')
        with patch.object(pool.runner, 'run', AsyncMock(side_effect=RuntimeError('fixture runner failure'))):
            done = await settle(pool.run(worker, self.store.topic(TOPIC)))
        self.assertEqual(done['status'], 'done')
        self.assertEqual(json.loads(done['result'])['failure_code'], 'start_failed')
        self.assertFalse(alive(client.child_pid))
        await self.assert_host_exited(client)

    async def test_detached_child_is_stopped_without_signalling_an_unrelated_process(self):
        from coordinator import process_ownership
        detached = sys.platform == 'darwin'
        unrelated = await asyncio.create_subprocess_exec(sys.executable, '-c', 'import time; time.sleep(120)',
                                                         start_new_session=True)
        async def cleanup():
            if unrelated.returncode is None:
                unrelated.terminate()
            await unrelated.wait()
        self.addAsyncCleanup(cleanup)
        pool, worker, client = await self.ending_worker('exception', stubborn=True, detached=detached)
        done = await settle(pool.run(worker, self.store.topic(TOPIC)))
        await self.assert_worker_processes_ended(done, client)
        self.assertIsNone(unrelated.returncode)
        self.assertTrue(alive(unrelated.pid))
        if detached:
            record = process_ownership.read_owner(client.directory)
            self.assertEqual(record['cleanup'], 'ok')
            self.assertIn(int((self.root / 'ending-child').read_text()), record['killed_pids'])

    async def test_success_stops_tree_and_preserves_finished_session_resume(self):
        pool, worker, client = await self.ending_worker('success')
        done = await settle(pool.run(worker, self.store.topic(TOPIC)))
        self.assertTrue(json.loads(done['result'])['success'], done['result'])
        await self.assert_worker_processes_ended(done, client)
        self.assertTrue(can_resume_finished(self.store, done))

    async def test_owner_stop_still_interrupts_worker_and_stops_tree(self):
        pool, worker, client = await self.ending_worker('active')
        running = asyncio.create_task(pool.run(worker, self.store.topic(TOPIC)))
        async def cleanup():
            await client.stop()
            await settle(asyncio.gather(running, return_exceptions=True))
        self.addAsyncCleanup(cleanup)
        await until_exists(self.root / 'ending-active')
        await until(lambda: pool.controls[worker['id']]._ready.is_set())
        self.assertEqual(await settle(pool.controls[worker['id']].steer('live-steer', 'continue')), 'received')
        await settle(pool.stop(worker['id']))
        done = await settle(running)
        self.assertEqual(done['status'], 'interrupted')
        self.assertEqual(json.loads(done['result'])['failure_code'], 'owner_stopped')
        await self.assert_worker_processes_ended(done, client)

    async def test_survivor_is_killed_and_recorded_before_worker_completion(self):
        pool, worker, client = await self.ending_worker('exception', stubborn=True)
        with self.assertLogs('coordinator.host', level='INFO') as logs:
            done = await settle(pool.run(worker, self.store.topic(TOPIC)))
        await self.assert_worker_processes_ended(done, client)
        child = int((self.root / 'ending-child').read_text())
        record = json.loads((client.directory / 'stop.json').read_text())
        self.assertIn(child, record['killed_pids'])
        self.assertTrue(any('host children killed pids=' in line and str(child) in line for line in logs.output),
                        logs.output)
        rows = list(self.store.db.execute("SELECT code,detail FROM problems WHERE worker=?", (worker['id'],)))
        self.assertFalse(any(row['code'] == 'process-killed' for row in rows),
                         [(row['code'], row['detail']) for row in rows])

    async def test_held_terminal_result_keeps_worker_running_until_provider_exits(self):
        pool, worker, client = await self.ending_worker('held')
        running = asyncio.create_task(pool.run(worker, self.store.topic(TOPIC)))
        try:
            await until_exists(self.root / 'ending-terminal')
            self.assertEqual(pool.get(worker['id'])['status'], 'running')
            self.assertTrue(alive(client.child_pid))
            self.assertTrue(alive(int((self.root / 'ending-child').read_text())))
            self.assertFalse(running.done())
        finally:
            (self.root / 'release-provider').touch()
            done = await settle(running)
        self.assertTrue(json.loads(done['result'])['success'], done['result'])
        await self.assert_worker_processes_ended(done, client)

    async def test_terminal_result_with_child_holding_stdout_waits_for_eof(self):
        pool, worker, client = await self.ending_worker('held-child', detached=sys.platform == 'darwin')
        running = asyncio.create_task(pool.run(worker, self.store.topic(TOPIC)))
        try:
            await until_exists(self.root / 'ending-terminal')
            await until(lambda: not alive(client.child_pid), 'the provider to exit while its child holds stdout')
            self.assertEqual(pool.get(worker['id'])['status'], 'running')
            self.assertTrue(alive(int((self.root / 'ending-child').read_text())))
            self.assertFalse(running.done())
        finally:
            (self.root / 'release-child').touch()
            done = await settle(running)
        self.assertTrue(json.loads(done['result'])['success'], done['result'])
        await self.assert_worker_processes_ended(done, client)

    async def test_stop_task_cancellation_propagates_during_grace(self):
        pool = WorkerPool(self.store, object(), AccountBroker(self.store))
        worker = self.worker()
        entered = asyncio.Event()
        release = asyncio.Event()

        async def cancel_grace():
            entered.set()
            await release.wait()

        grace = asyncio.create_task(cancel_grace())
        pool.cancels[worker['id']] = lambda: grace
        host = SimpleNamespace(stop=AsyncMock(), detach=Mock())
        with patch('coordinator.workers.HostClient.attach', return_value=host):
            stopping = asyncio.create_task(pool.stop(worker['id']))
            try:
                await asyncio.wait_for(entered.wait(), 2)
                stopping.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(stopping, 2)
                host.stop.assert_not_awaited()
                host.detach.assert_called_once_with()
                self.assertNotIn(worker['id'], pool.stopping)
            finally:
                stopping.cancel()
                grace.cancel()
                await asyncio.wait_for(asyncio.gather(stopping, grace, return_exceptions=True), 2)

    async def test_two_workers_share_task_worktree_without_capacity_gate(self):
        entered = []
        release = asyncio.Event()

        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                entered.append((provider, str(cwd), session))
                await release.wait()
                return RunResult(session, text='done', success=True)

        pool = WorkerPool(self.store, Runner(), AccountBroker(self.store))
        first, second = self.worker(), self.worker()
        tasks = [asyncio.create_task(pool.run(row, self.store.topic(TOPIC))) for row in (first, second)]
        try:
            async def both_started():
                while len(entered) < 2:
                    await asyncio.sleep(0.01)
            await asyncio.wait_for(both_started(), 2)
            self.assertEqual({cwd for _, cwd, _ in entered}, {str(self.root)})
            self.assertEqual(len({session for _, _, session in entered}), 2)
        finally:
            release.set()
            await asyncio.gather(*tasks)
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM messages WHERE kind='worker_result'").fetchone()[0], 2)

    async def run_with_text(self, provider, text):
        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                return RunResult(session or '00000000-0000-4000-8000-000000000001', text=text, success=True)

        worker = self.worker(provider)
        done = await WorkerPool(self.store, Runner(), AccountBroker(self.store)).run(worker, self.store.topic(TOPIC))
        return done, json.loads(done['result'])

    async def test_final_report_is_recorded_as_done(self):
        done, result = await self.run_with_text('codex', 'Did the tests pass?\nYes. All 40 tests pass.')
        self.assertEqual(done['status'], 'done')

    async def test_parent_jobs_summary_reads_real_worker_completions(self):
        outcomes = [RunResult(None, text='PR 12 opened', success=True),
                    RunResult(None, error='Provider failed', failure_code='provider_failed'),
                    RunResult(None, text='', success=True)]
        expected = [('PR 12 opened', True, None), ('', False, 'provider_failed'), ('', True, None)]
        completed = []
        for outcome, (text, success, failure_code) in zip(outcomes, expected):
            self.task = self.store.task_create(TOPIC, 'Completion %d' % len(completed), worktree=str(self.root))
            runner = SimpleNamespace(run=AsyncMock(return_value=outcome))
            with patch.object(self.store, 'worker_complete', wraps=self.store.worker_complete) as finish:
                done = await WorkerPool(self.store, runner, AccountBroker(self.store)).run(
                    self.worker(), self.store.topic(TOPIC))
            finish.assert_called_once()
            self.assertEqual(done['status'], 'done')
            completed.append(done['id'])
            summary = json.loads(parent_state.jobs_summary(self.store, [self.store.task_get(self.task['id'])]))
            self.assertEqual(summary['shown'][0]['finished'], {
                'id': done['id'], 'text': text, 'success': success, 'failure_code': failure_code})
            self.assertEqual(summary['live_workers'], [])
            self.assertEqual(summary['live_workers_not_shown'], 0)
        rows = list(self.store.db.execute("SELECT id,result FROM workers WHERE status='done' ORDER BY id"))
        self.assertEqual([row['id'] for row in rows], completed)
        for row in rows:
            result = json.loads(row['result'])
            self.assertIsInstance(result, dict)
            self.assertIn('text', result)
            self.assertTrue(result['text'] is None or isinstance(result['text'], str))

    async def test_missing_task_worktree_saves_failure_message(self):
        worker = self.worker()
        with self.store.db:
            self.store.db.execute('UPDATE tasks SET worktree=? WHERE id=?', (str(self.root / 'gone'), self.task['id']))
        pool = WorkerPool(self.store, object(), AccountBroker(self.store))
        await pool.run(worker, self.store.topic(TOPIC))
        result = json.loads(self.store.db.execute('SELECT result FROM workers WHERE id=?', (worker['id'],)).fetchone()[0])
        self.assertEqual(result['failure_code'], 'start_failed')
        self.assertFalse(result['success'])
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM messages WHERE source_worker=?',
                                               (worker['id'],)).fetchone()[0], 1)

    async def test_codex_off_uses_the_delegation_control(self):
        with self.store.db:
            self.store.put('codex_enabled', False)
        worker = self.worker('codex')
        await WorkerPool(self.store, object(), AccountBroker(self.store)).run(
            worker, self.store.topic(TOPIC))
        result = json.loads(self.store.db.execute('SELECT result FROM workers WHERE id=?',
                                                  (worker['id'],)).fetchone()[0])
        self.assertNotIn('enable Codex', result['failure_detail'])
        self.assertEqual(result['failure_detail'], 'Codex delegation is disabled.')

    async def test_goal_is_steered_after_prompt_and_status_reaches_message(self):
        binary = self.root / 'fake-claude'
        binary.write_text('''#!/usr/bin/env python3
import json,sys
from pathlib import Path
root=Path(__file__).parent
sid=sys.argv[sys.argv.index('--session-id')+1]
print(json.dumps({'type':'system','subtype':'init','session_id':sid}),flush=True)
for line in sys.stdin:
    event=json.loads(line)
    with (root/'inputs.jsonl').open('a') as log:
        log.write(json.dumps(event)+'\\n')
    print(json.dumps({**event,'isReplay':True}),flush=True)
    text=event['message']['content']
    if text.startswith('/goal '):
        print(json.dumps({'type':'goal_status','session_id':sid,'status':'met',
            'condition':text[6:],'reason':'verified','iterations':3}),flush=True)
        print(json.dumps({'type':'result','subtype':'success','session_id':sid,'result':'complete'}),flush=True)
''')
        binary.chmod(0o700)
        runner = ProviderRunner(self.store.directory, {'claude': str(binary)})
        pool = WorkerPool(self.store, runner, AccountBroker(self.store))
        worker = self.worker(goal='all checks pass')
        await asyncio.wait_for(pool.run(worker, self.store.topic(TOPIC)), 5)
        inputs = [event['message']['content'] for event in read_jsonl(self.root / 'inputs.jsonl')]
        self.assertIn('build this', inputs[0])
        self.assertEqual(inputs[1], '/goal all checks pass')
        result = json.loads(self.store.db.execute('SELECT result FROM workers WHERE id=?', (worker['id'],)).fetchone()[0])
        self.assertEqual(result['goal_status']['status'], 'met')
        message = json.loads(self.store.db.execute('SELECT text FROM messages WHERE source_worker=?',
                                                   (worker['id'],)).fetchone()[0])
        self.assertEqual(message['goal_status']['reason'], 'verified')

    async def test_quota_rotation_restarts_in_a_fresh_host_directory_and_resumes_the_session(self):
        binary = self.root / 'fake-claude'
        binary.write_text('''#!/usr/bin/env python3
import json,os,sys
from pathlib import Path
home=Path(os.environ['CLAUDE_CONFIG_DIR'])
with (Path(__file__).parent/'launches.jsonl').open('a') as log:
    log.write(json.dumps({'home':home.name,'argv':sys.argv[1:]})+'\\n')
if '--session-id' in sys.argv:
    sid=sys.argv[sys.argv.index('--session-id')+1]
    transcript=home/'projects'/'p'/(sid+'.jsonl')
    transcript.parent.mkdir(parents=True)
    transcript.write_text('{}\\n')
else:
    sid=Path(sys.argv[sys.argv.index('--resume')+1]).name[:-6]
print(json.dumps({'type':'system','subtype':'init','session_id':sid}),flush=True)
for line in sys.stdin:
    print(json.dumps({**json.loads(line),'isReplay':True}),flush=True)
    if home.name=='first':
        print(json.dumps({'type':'rate_limit_event','session_id':sid,'rate_limit_info':
            {'status':'rejected','resetsAt':9999999999,'rateLimitType':'seven_day'}}),flush=True)
        print(json.dumps({'type':'system','subtype':'background_tasks_changed','session_id':sid,
            'tasks':[{'task_id':'a1','task_type':'local_agent','description':'review'}]}),flush=True)
        print(json.dumps({'type':'result','subtype':'success','is_error':True,
            'terminal_reason':'api_error','origin':{'kind':'task-notification'},
            'session_id':sid}),flush=True)
        assert sys.stdin.read()==''
        sys.exit(1)
    print(json.dumps({'type':'result','subtype':'success','session_id':sid,'result':'resumed'}),flush=True)
''')
        binary.chmod(0o700)
        for alias in ('first', 'second'):
            (self.root / alias).mkdir()
        with self.store.db:
            self.store.put('accounts', {alias: {'config_dir': str(self.root / alias), 'enabled': True}
                                        for alias in ('first', 'second')})
            now = time.time()
            self.store.put('account_status', {alias: {'identity': {'email': alias + '@example.com', 'logged_in': True},
                'observed_at': now, 'usage': {'five_hour': {'utilization': 10, 'resets_at': now + 1800},
                'seven_day': {'utilization': 10, 'resets_at': now + (3600 if alias == 'first' else 7200)},
                'seven_day_fable': {'utilization': 10, 'resets_at': now + (3600 if alias == 'first' else 7200)}}}
                for alias in ('first', 'second')})
        runner = ProviderRunner(self.store.directory, {'claude': str(binary)})
        pool = WorkerPool(self.store, runner, AccountBroker(self.store))
        worker = self.worker()
        (self.store.directory / 'hosts' / ('worker-' + str(worker['id']))).mkdir(parents=True)
        done = await asyncio.wait_for(pool.run(worker, self.store.topic(TOPIC)), 20)
        self.assertTrue(json.loads(done['result'])['success'])
        launches = read_jsonl(self.root / 'launches.jsonl')
        self.assertEqual([launch['home'] for launch in launches], ['first', 'second'])
        session = launches[0]['argv'][launches[0]['argv'].index('--session-id') + 1]
        resumed = launches[1]['argv'][launches[1]['argv'].index('--resume') + 1]
        self.assertEqual(Path(resumed).resolve(), (self.root / 'first' / 'projects' / 'p' / (session + '.jsonl')).resolve())
        self.assertEqual(done['session'], session)
        base = 'worker-' + str(worker['id'])
        self.assertEqual(done['host'], base + '.3')
        hosts = self.store.directory / 'hosts'
        self.assertEqual(sorted(path.name for path in hosts.iterdir()), [base, base + '.2', base + '.3'])
        self.assertTrue((hosts / (base + '.2') / 'exit.json').is_file())
        self.assertEqual(json.loads((hosts / (base + '.3') / 'spec.json').read_text())['env']['CLAUDE_CONFIG_DIR'],
                         str(self.root / 'second'))

    async def check_claude_worker_report(self, tail, expected, steer=False, report='full report with checks'):
        binary = self.root / 'fake-claude'
        binary.write_text('''#!/usr/bin/env python3
import json,sys
from pathlib import Path
def emit(event):
    print(json.dumps(event),flush=True)
sid=sys.argv[sys.argv.index('--session-id')+1]
emit({'type':'system','subtype':'init','session_id':sid})
prompt=json.loads(sys.stdin.readline())
emit({**prompt,'isReplay':True})
emit({'type':'system','subtype':'background_tasks_changed','session_id':sid,
    'tasks':[{'task_id':'a1','task_type':'local_bash','description':'tests'}]})
result={'type':'result','subtype':'success','session_id':sid,'result':''' + repr(report) + '''}
emit(result)
''' + tail + "\nassert sys.stdin.read()==''\n")
        binary.chmod(0o700)
        pool = WorkerPool(self.store, ProviderRunner(self.store.directory, {'claude': str(binary)}),
                          AccountBroker(self.store))
        worker = self.worker()
        task = asyncio.create_task(pool.run(worker, self.store.topic(TOPIC)))
        if steer:
            await until_exists(self.root / 'report-ready')
            receipt = await pool.controls[worker['id']].steer('report-steer', 'new instruction')
            self.assertEqual(receipt, 'received')
        done = await asyncio.wait_for(task, 10)
        result = json.loads(done['result'])
        self.assertTrue(result['success'], result.get('error'))
        self.assertEqual(result['text'], expected)
        messages = self.store.db.execute(
            "SELECT text FROM messages WHERE kind='worker_result' AND source_worker=?", (worker['id'],)).fetchall()
        self.assertEqual(len(messages), 1)
        self.assertEqual(json.loads(messages[0]['text'])['text'], expected)

    async def test_claude_worker_report_appends_self_woken_turns(self):
        await self.check_claude_worker_report('''
emit({'type':'system','subtype':'task_notification','session_id':sid})
emit({**result,'result':'tests finished'})
emit({'type':'system','subtype':'background_tasks_changed','session_id':sid,'tasks':[]})
emit({**result,'result':'late follow-up'})
''', 'full report with checks\n\nLater, after a background task finished (1 earlier note left out): late follow-up')

    async def test_claude_worker_steer_replaces_report_before_empty_late_turn(self):
        await self.check_claude_worker_report('''
emit({**result,'result':'old late follow-up'})
(Path(__file__).parent/'report-ready').touch()
steer=json.loads(sys.stdin.readline())
emit({'type':'command_lifecycle','command_uuid':steer['uuid'],'state':'started','session_id':sid})
emit({**steer,'isReplay':True})
emit({**result,'result':'new report after steer'})
emit({**result,'result':'new late note'})
emit({'type':'system','subtype':'background_tasks_changed','session_id':sid,'tasks':[]})
emit({**result,'result':''})
''', 'new report after steer\n\nLater, after a background task finished: new late note', steer=True)

    async def test_claude_worker_empty_late_turn_keeps_report(self):
        await self.check_claude_worker_report('''
emit({'type':'system','subtype':'background_tasks_changed','session_id':sid,'tasks':[]})
emit({**result,'result':''})
''', 'full report with checks')

    async def test_claude_worker_many_wakes_keep_newest_late_note_with_count(self):
        await self.check_claude_worker_report('''
for number in range(500):
    if number==499:
        emit({'type':'system','subtype':'background_tasks_changed','session_id':sid,'tasks':[]})
    emit({**result,'result':'wake '+str(number)+': '+'x'*1024})
''', 'full report with checks\n\nLater, after a background task finished (499 earlier notes left out): '
             'wake 499: ' + 'x' * 1024)

    async def test_claude_worker_empty_report_uses_late_note_without_label(self):
        await self.check_claude_worker_report('''
emit({'type':'system','subtype':'background_tasks_changed','session_id':sid,'tasks':[]})
emit({**result,'result':'tests finished'})
''', 'tests finished', report='')

    async def test_claude_worker_duplicate_report_keeps_late_text(self):
        await self.check_claude_worker_report('''
emit({**result,'result':'tests finished'})
emit({'type':'system','subtype':'background_tasks_changed','session_id':sid,'tasks':[]})
emit(result)
''', 'full report with checks\n\nLater, after a background task finished: tests finished')

    async def test_failed_task_notification_reports_worker_result(self):
        binary = self.root / 'fake-claude'
        binary.write_text('''#!/usr/bin/env python3
import json,sys
sid=sys.argv[sys.argv.index('--session-id')+1]
print(json.dumps({'type':'system','subtype':'init','session_id':sid}),flush=True)
sys.stdin.readline()
print(json.dumps({'type':'system','subtype':'background_tasks_changed','session_id':sid,
    'tasks':[{'task_id':'a1','task_type':'local_agent','description':'review'}]}),flush=True)
print(json.dumps({'type':'result','subtype':'success','is_error':True,'session_id':sid,
    'terminal_reason':'api_error','origin':{'kind':'task-notification'}}),flush=True)
assert sys.stdin.read()==''
''')
        binary.chmod(0o700)
        pool = WorkerPool(self.store, ProviderRunner(self.store.directory, {'claude': str(binary)}),
                          AccountBroker(self.store))
        worker = self.worker()
        done = await asyncio.wait_for(pool.run(worker, self.store.topic(TOPIC)), 5)
        self.assertEqual(done['status'], 'done')
        result = json.loads(done['result'])
        self.assertFalse(result['success'])
        self.assertFalse(result['quota_limited'])
        self.assertNotEqual(result['failure_code'], 'pending_work')
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM messages WHERE kind='worker_result' AND source_worker=?",
                                               (worker['id'],)).fetchone()[0], 1)

    async def test_worker_waits_once_then_resumes_after_the_earliest_reset(self):
        from coordinator.service import Service
        sid = '20000000-0000-4000-8000-000000000091'
        transcript = self.root / 'primary' / 'projects' / 'test' / (sid + '.jsonl')
        transcript.parent.mkdir(parents=True)
        transcript.write_text('{}\n')
        calls = []
        reset = time.time() + 3600

        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                calls.append((prompt, session, options.get('fresh'), options.get('account_alias'),
                              options.get('initial_steer')))
                if len(calls) == 1:
                    return RunResult(session, quota_limited=True, transcript_path=str(transcript),
                                     rate_limit_info={'status': 'rejected', 'rateLimitType': 'five_hour',
                                                      'resetsAt': reset})
                return RunResult(session, text='resumed', success=True, transcript_path=str(transcript))

        self.store.put('coordinator_home_topic', TOPIC)
        runner = Runner()
        worker = self.worker(goal='finish the same goal')
        service = Service(self.store, None, runner, self.root)
        waiting = await service.workers.run(worker, self.store.topic(TOPIC))
        self.assertEqual(waiting['status'], 'waiting_for_quota')
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM messages WHERE kind='callback'").fetchone()[0], 0)
        self.assertEqual([row[0] for row in self.store.db.execute('SELECT topic FROM outbox')], [TOPIC])
        payload = json.loads(waiting['result'])
        self.assertEqual(payload['waiting_until'], reset)
        payload['waiting_until'] = time.time() - 1
        with self.store.db:
            self.store.db.execute('UPDATE workers SET result=? WHERE id=?',
                                  (json.dumps(payload), worker['id']))
            self.store.put('account_blocks', {'primary': {'until': time.time() - 1, 'reason': 'quota'}})
            status = self.store.get('account_status')
            status['primary']['observed_at'] = time.time()
            self.store.put('account_status', status)
        await service.workers_once()
        await asyncio.gather(*service.worker_tasks.values())
        complete = service.workers.get(worker['id'])
        self.assertEqual(complete['status'], 'done')
        self.assertEqual(len(calls), 2)
        self.assertEqual(LIMIT_CONTINUATION + calls[0][0], calls[1][0])
        self.assertEqual(calls[0][1], calls[1][1])
        self.assertEqual(calls[0][4], calls[1][4])
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM messages WHERE kind='callback'").fetchone()[0], 0)
        self.assertEqual([row[0] for row in self.store.db.execute('SELECT topic FROM outbox')], [TOPIC])

    async def test_reattached_claude_limits_resume_on_another_account_without_a_steer(self):
        from coordinator.service import Service
        second = self.root / 'second'
        second.mkdir()
        accounts = self.store.get('accounts')
        accounts['second'] = {'config_dir': str(second), 'enabled': True}
        status = self.store.get('account_status')
        status['second'] = dict(status['primary'], identity={'email': 'second@example.com', 'logged_in': True})
        self.store.put('accounts', accounts)
        self.store.put('account_status', status)
        for meter in ('five_hour', 'seven_day'):
            with self.subTest(meter=meter):
                self.store.put('account_blocks', {})
                worker = self.worker()
                sid = '20000000-0000-4000-8000-%012d' % worker['id']
                transcript = self.root / 'primary' / 'projects' / 'test' / (sid + '.jsonl')
                transcript.parent.mkdir(parents=True, exist_ok=True)
                transcript.write_text('{}\n')
                self.store.db.execute("UPDATE workers SET status='running',session=?,fresh=0,account_alias='primary' WHERE id=?",
                                      (sid, worker['id']))
                calls = []

                class Runner:
                    async def run(self, provider, prompt, cwd, session, **options):
                        calls.append((options.get('attach'), options.get('account_alias'), session, prompt))
                        if options.get('attach'):
                            return RunResult(session, quota_limited=True, transcript_path=str(transcript),
                                             rate_limit_info={'status': 'rejected', 'rateLimitType': meter,
                                                              'resetsAt': time.time() + 3600})
                        return RunResult(session, text='resumed', success=True, transcript_path=str(transcript))

                service = Service(self.store, None, Runner(), self.root)
                waiting = await service.workers.run(worker, self.store.topic(TOPIC))
                self.assertEqual((waiting['status'], waiting['session']), ('waiting_for_quota', sid))
                await service.workers_once()
                await asyncio.gather(*service.worker_tasks.values())
                finished = service.workers.get(worker['id'])
                self.assertTrue(json.loads(finished['result'])['success'])
                self.assertEqual((finished['session'], finished['account_alias']), (sid, 'second'))
                self.assertEqual(calls, [(True, None, sid, 'build this'),
                                         (False, 'second', sid, LIMIT_CONTINUATION + 'build this')])

    async def test_every_claude_account_rejects_then_the_worker_resumes_after_a_reset(self):
        from coordinator.service import Service
        home = '-10042:5'
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,-10042,5,'Home',?,1)",
                              (home, str(self.root)))
        self.store.put('coordinator_home_topic', home)
        second = self.root / 'second'
        second.mkdir()
        accounts = self.store.get('accounts')
        accounts['second'] = {'config_dir': str(second), 'enabled': True}
        status = self.store.get('account_status')
        status['second'] = dict(status['primary'], identity={'email': 'second@example.com', 'logged_in': True})
        self.store.put('accounts', accounts)
        self.store.put('account_status', status)
        calls = []
        reset = time.time() + 3600
        recovered = False
        transcript_root = self.root / 'primary' / 'projects' / 'test'

        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                calls.append((options['account_alias'], session, prompt))
                transcript = transcript_root / (session + '.jsonl')
                transcript.parent.mkdir(parents=True, exist_ok=True)
                transcript.write_text('{}\n')
                return RunResult(session, success=recovered, text='resumed' if recovered else '',
                                 quota_limited=not recovered, transcript_path=str(transcript),
                                 rate_limit_info=None if recovered else {
                                     'status': 'rejected', 'rateLimitType': 'seven_day', 'resetsAt': reset})

        runner = Runner()
        service = Service(self.store, None, runner, self.root)
        worker = self.worker()
        waiting = await service.workers.run(worker, self.store.topic(TOPIC))
        self.assertEqual(waiting['status'], 'waiting_for_quota')
        self.assertEqual([alias for alias, _, _ in calls], ['primary', 'second'])
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM messages WHERE kind='callback'").fetchone()[0], 0)
        notices = self.store.db.execute('SELECT topic,text FROM outbox').fetchall()
        self.assertEqual([row['topic'] for row in notices], [home])
        self.assertIn('waiting for Claude usage to reset', notices[0]['text'])
        await service.workers_once()
        self.assertEqual(len(calls), 2)
        recovered = True
        with patch('coordinator.accounts.time.time', return_value=reset + 1):
            await service.workers_once()
            await asyncio.gather(*service.worker_tasks.values())
        finished = service.workers.get(worker['id'])
        self.assertTrue(json.loads(finished['result'])['success'])
        self.assertEqual(finished['session'], waiting['session'])
        self.assertEqual([session for _, session, _ in calls], [waiting['session']] * 3)
        self.assertEqual(calls[-1][2], LIMIT_CONTINUATION + 'build this')

    async def test_worker_that_waits_before_its_first_launch_starts_fresh_after_the_wait(self):
        from coordinator.service import Service
        calls = []

        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                calls.append(options.get('fresh'))
                return RunResult(session, text='ok', success=True)

        def usage(utilization):
            with self.store.db:
                status = self.store.get('account_status')
                status['primary']['usage']['seven_day']['utilization'] = utilization
                self.store.put('account_status', status)

        usage(98)
        worker = self.worker()
        service = Service(self.store, None, Runner(), self.root)
        waiting = await service.workers.run(worker, self.store.topic(TOPIC))
        self.assertEqual((waiting['status'], waiting['fresh'], calls), ('waiting_for_quota', 1, []))
        usage(10)
        payload = json.loads(waiting['result'])
        payload['waiting_until'] = time.time() - 1
        with self.store.db:
            self.store.db.execute('UPDATE workers SET result=? WHERE id=?', (json.dumps(payload), worker['id']))
        await service.workers_once()
        await asyncio.gather(*service.worker_tasks.values())
        self.assertEqual(calls, [True])
        self.assertEqual(service.workers.get(worker['id'])['status'], 'done')


class WorkerGoalTests(unittest.IsolatedAsyncioTestCase):
    asyncTearDown = WorkerTests.asyncTearDown
    worker = WorkerTests.worker

    async def asyncSetUp(self):
        await WorkerTests.asyncSetUp(self)
        self.store.db.executescript('''
            CREATE TEMP TABLE goal_worker_statuses(worker INTEGER, status TEXT);
            CREATE TEMP TRIGGER record_goal_worker_status AFTER UPDATE OF status ON workers
            BEGIN INSERT INTO goal_worker_statuses VALUES (NEW.id, NEW.status); END;
        ''')

    def native_runner(self, provider, worker, goal_status, quota_limited=False):
        sid = '00000000-0000-4000-8000-%012d' % worker['id']
        binary = self.root / ('fake-' + provider)
        if provider == 'codex':
            script = CODEX_GOAL_SERVER.replace('SID', repr(sid)).replace('ROOT', repr(str(self.root)))
        else:
            script = '''#!/usr/bin/env python3
import json,sys
sid=sys.argv[sys.argv.index('--session-id')+1]
print(json.dumps({'type':'system','subtype':'init','session_id':sid}),flush=True)
for line in sys.stdin:
    event=json.loads(line)
    print(json.dumps({**event,'isReplay':True}),flush=True)
    text=event['message']['content']
    if text.startswith('/goal '):
        if QUOTA_LIMITED:
            print(json.dumps({'type':'rate_limit_event','session_id':sid,'rate_limit_info':
                {'status':'rejected','resetsAt':4102444800,'rateLimitType':'five_hour'}}),flush=True)
        print(json.dumps({'type':'goal_status','session_id':sid,'status':GOAL_STATUS,
            'condition':text[6:],'reason':'verified','iterations':3}),flush=True)
        print(json.dumps({'type':'result','subtype':'success','session_id':sid,'result':'complete'}),flush=True)
'''.replace('GOAL_STATUS', repr(goal_status)).replace('QUOTA_LIMITED', repr(quota_limited))
        binary.write_text(script)
        binary.chmod(0o700)
        return ProviderRunner(self.store.directory, {provider: str(binary)})

    async def check_run(self, worker, runner, success=True, code=None, detail=None, attaching=False,
                        native_status=None):
        native = []
        run = runner.run

        async def capture(*args, **options):
            result = await run(*args, **options)
            native.append(asdict(result))
            return result

        if attaching:
            with self.store.db:
                self.store.db.execute("UPDATE workers SET status='running',account_alias='primary',session=? WHERE id=?",
                                      ('attached-session', worker['id']))
        pool = WorkerPool(self.store, runner, AccountBroker(self.store))
        with patch.object(runner, 'run', AsyncMock(side_effect=capture)) as called, \
                patch.object(pool, '_orphaned', return_value=False):
            finished = await asyncio.wait_for(pool.run(worker, self.store.topic(TOPIC)), 10)
        called.assert_awaited_once()
        self.assertEqual(called.call_args.kwargs['attach'], attaching)
        self.assertEqual(len(native), 1)
        result = json.loads(finished['result'])
        if native_status is not None:
            self.assertEqual(result['goal_status']['status'], native_status)
        self.assertEqual(result['success'], success)
        self.assertEqual(result['failure_code'], code)
        self.assertEqual(result['failure_detail'], detail)
        status = 'interrupted' if code == 'owner_stopped' else 'done'
        expected = dict(native[0], success=success, failure_code=code, failure_detail=detail,
                        worker=worker['id'], task=self.task['id'], topic=TOPIC,
                        provider=worker['provider'], status=status, cwd=str(self.root))
        self.assertEqual(result, expected)
        self.assertEqual(finished['status'], status)
        self.assertEqual(finished['session'], native[0]['session_id'])
        if success or code == 'goal_unmet':
            self.assertEqual(finished['fresh'], 0)
        messages = list(self.store.db.execute(
            'SELECT kind,topic,text FROM messages WHERE source_worker=?', (worker['id'],)))
        self.assertEqual([(row['kind'], row['topic']) for row in messages], [('worker_result', TOPIC)])
        self.assertEqual(json.loads(messages[0]['text']), expected)
        problems = [tuple(row) for row in self.store.db.execute(
            'SELECT area,code,detail,topic,task FROM problems WHERE worker=?', (worker['id'],))]
        self.assertEqual(problems, [] if success else [('worker', code, detail, TOPIC, self.task['id'])])
        states = [row[0] for row in self.store.db.execute(
            'SELECT status FROM goal_worker_statuses WHERE worker=?', (worker['id'],))]
        self.assertNotIn('waiting_for_quota', states)
        return finished

    async def test_codex_native_unmet_goals_are_unsuccessful_done(self):
        for status in ('budgetLimited', 'usageLimited', 'blocked', 'paused'):
            with self.subTest(status=status), patch.dict(os.environ, {'FAKE_GOAL_STATUS': status}):
                worker = self.worker('codex', goal='all checks pass')
                await self.check_run(worker, self.native_runner('codex', worker, status),
                                     success=False, code='goal_unmet', detail=status,
                                     native_status=status)

    async def test_codex_native_complete_goal_stays_successful(self):
        with patch.dict(os.environ, {'FAKE_GOAL_STATUS': 'complete'}):
            worker = self.worker('codex', goal='all checks pass')
            await self.check_run(worker, self.native_runner('codex', worker, 'complete'), native_status='complete')

    async def test_claude_native_failed_goal_is_unsuccessful_done(self):
        worker = self.worker(goal='all checks pass')
        await self.check_run(worker, self.native_runner('claude', worker, 'failed'),
                             success=False, code='goal_unmet', detail='failed',
                             native_status='failed')

    async def test_claude_native_met_goal_stays_successful(self):
        worker = self.worker(goal='all checks pass')
        await self.check_run(worker, self.native_runner('claude', worker, 'met'), native_status='met')

    async def test_successful_claude_unmet_goal_with_quota_flag_never_waits(self):
        worker = self.worker(goal='all checks pass')
        finished = await self.check_run(
            worker, self.native_runner('claude', worker, 'failed', quota_limited=True),
            success=False, code='goal_unmet', detail='failed', native_status='failed')
        result = json.loads(finished['result'])
        self.assertTrue(result['quota_limited'])
        self.assertEqual(result['rate_limit_info']['status'], 'rejected')
        self.assertNotIn('waiting_until', result)

    async def test_goal_without_native_status_stays_successful(self):
        for provider in ('claude', 'codex'):
            for goal_status in (None, {}, {'reason': 'no status'}, {'status': None}, {'status': ''},
                                {'status': 1}, {'status': []}, 'paused', []):
                with self.subTest(provider=provider, goal_status=goal_status):
                    worker = self.worker(provider, goal='all checks pass')
                    runner = SimpleNamespace(run=AsyncMock(return_value=RunResult(
                        'saved-session', success=True, goal_status=goal_status)))
                    await self.check_run(worker, runner)

    async def test_worker_without_goal_stays_successful(self):
        for provider in ('claude', 'codex'):
            with self.subTest(provider=provider):
                runner = SimpleNamespace(run=AsyncMock(return_value=RunResult('saved-session', success=True)))
                await self.check_run(self.worker(provider), runner)

    async def test_unmet_goal_preserves_existing_failures(self):
        for provider in ('claude', 'codex'):
            for code in ('owner_stopped', 'execution_failed'):
                with self.subTest(provider=provider, code=code):
                    detail = 'original ' + code
                    runner = SimpleNamespace(run=AsyncMock(return_value=RunResult(
                        'saved-session', error='original error', failure_code=code, failure_detail=detail,
                        goal_status={'status': 'budgetLimited'})))
                    await self.check_run(self.worker(provider, goal='all checks pass'), runner,
                                         success=False, code=code, detail=detail)

    async def test_unmet_goal_keeps_observed_session_without_a_transcript(self):
        for provider, status in (('codex', 'budgetLimited'), ('claude', 'failed')):
            with self.subTest(provider=provider):
                async def run(*args, **options):
                    options['on_session']('saved-session')
                    return RunResult('saved-session', text='work remains', success=True, error='unchanged',
                                     goal_status={'status': status, 'objective': 'all checks pass'})

                runner = SimpleNamespace(run=AsyncMock(side_effect=run))
                await self.check_run(self.worker(provider, goal='all checks pass'), runner,
                                     success=False, code='goal_unmet', detail=status)

    async def test_reattached_goal_uses_the_same_completion_rule(self):
        for provider, status, unmet in (('codex', 'budgetLimited', True), ('claude', 'failed', True),
                                        ('codex', 'complete', False), ('claude', 'met', False)):
            with self.subTest(provider=provider, status=status):
                async def run(*args, **options):
                    options['on_session']('attached-session')
                    return RunResult('attached-session', success=True, goal_status={'status': status})

                runner = SimpleNamespace(run=AsyncMock(side_effect=run))
                await self.check_run(self.worker(provider, goal='all checks pass'), runner,
                                     success=not unmet, code='goal_unmet' if unmet else None,
                                     detail=status if unmet else None, attaching=True)


class WorkerProblemTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = WorkerTests.asyncSetUp
    asyncTearDown = WorkerTests.asyncTearDown
    worker = WorkerTests.worker

    def problems(self):
        return [tuple(row) for row in self.store.db.execute(
            'SELECT area,code,detail,topic,task,worker FROM problems ORDER BY id')]

    async def test_a_failed_worker_saves_its_code_and_detail_when_it_ends(self):
        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                options['on_problem']('worker', 'input-failed', 'BrokenPipeError at host.py:96')
                return RunResult(session, error='Provider execution failed: RuntimeError',
                                 failure_code='execution_failed',
                                 failure_detail='RuntimeError: Provider changed the native thread at native_protocol.py:228')

        worker = self.worker()
        with self.assertLogs('coordinator.problems', 'WARNING') as logs:
            await WorkerPool(self.store, Runner(), AccountBroker(self.store)).run(worker, self.store.topic(TOPIC))
        self.assertEqual(self.problems(), [
            ('worker', 'input-failed', 'BrokenPipeError at host.py:96', TOPIC, self.task['id'], worker['id']),
            ('worker', 'execution_failed',
             'RuntimeError: Provider changed the native thread at native_protocol.py:228',
             TOPIC, self.task['id'], worker['id'])])
        self.assertIn('problem area=worker code=execution_failed topic=%s task=%d worker=%d' % (
            TOPIC, self.task['id'], worker['id']), logs.output[1])

    async def test_background_work_and_a_dead_host_are_problems_and_a_final_question_is_not(self):
        outcomes = [RunResult(None, error='The worker exited while its background work was still running.',
                              failure_code='pending_work'),
                    RunResult(None, error='The provider host stopped without an exit record.',
                              failure_code='host_died', failure_detail='HostDied at host.py:121'),
                    RunResult(None, text='Which schema should I migrate?', success=True)]

        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                return outcomes.pop(0)

        pool = WorkerPool(self.store, Runner(), AccountBroker(self.store))
        workers = [self.worker() for _ in range(3)]
        with self.assertLogs('coordinator.problems', 'WARNING'):
            for worker in workers:
                await pool.run(worker, self.store.topic(TOPIC))
        self.assertEqual(pool.get(workers[2]['id'])['status'], 'done')
        self.assertEqual([row[:3] + row[5:] for row in self.problems()], [
            ('worker', 'pending_work', 'The worker exited while its background work was still running.',
             workers[0]['id']),
            ('worker', 'host_died', 'HostDied at host.py:121', workers[1]['id'])])

    async def test_a_successful_worker_saves_no_problem(self):
        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                return RunResult(session, text='done', success=True)

        await WorkerPool(self.store, Runner(), AccountBroker(self.store)).run(self.worker(), self.store.topic(TOPIC))
        self.assertEqual(self.problems(), [])


class SecretWorkerTestsSupport:
    async def asyncSetUp(self):
        await WorkerTests.asyncSetUp(self)
        self.vault = FakeVault()
        self.calls = []
        test = self

        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                test.calls.append(options)
                return RunResult(session, text='done', success=True)

        self.pool = WorkerPool(self.store, Runner(), AccountBroker(self.store), self.vault)

    def fill(self, name, value, task=None):
        self.vault.values[name] = value
        with self.store.db:
            return self.store.db.execute('''INSERT INTO envelopes(name,reason,consumer,task,topic,state,created,expires,
                length,fingerprint,updated) VALUES (?,'r','c',?,?,'filled',1,601,?,'abcd1234',1)''',
                                         (name, task, TOPIC, len(value))).lastrowid

    def declare(self, *entries):
        self.store.task_update(self.task['id'], secrets=list(entries))

    def result(self, worker):
        return json.loads(self.store.db.execute('SELECT result FROM workers WHERE id=?', (worker['id'],)).fetchone()[0])

    def uses(self):
        return [tuple(row) for row in self.store.db.execute(
            "SELECT envelope,name,worker,source FROM envelope_events WHERE event='use' ORDER BY id")]

    async def assert_finished_resumes_keep_secrets(self, provider, clear, versioned=False):
        from coordinator.service import Service
        vault_calls = len(self.vault.calls)
        self.declare('NPM_TOKEN')
        worker = self.worker(provider)
        session_id = '00000000-0000-4000-8000-000000000001'
        transcript = self.root / 'primary' / 'projects' / 'test' / (session_id + '.jsonl')
        transcript.parent.mkdir(parents=True, exist_ok=True)
        transcript.write_text('{}\n')
        calls = []
        store = self.store

        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                calls.append(dict(options, session=session))
                name = options['host_id'] + ('.2' if versioned else '')
                directory = store.directory / 'hosts' / name
                directory.mkdir(parents=True)
                (directory / 'spec.json').write_text(json.dumps({'secret_names': options.get('secret_names', [])}))
                options['on_host'](name)
                options['on_session'](session_id)
                return RunResult(session_id, text='Which path should I use?', success=True,
                                 transcript_path=str(transcript))

        service = Service(self.store, None, Runner(), self.root, vault=self.vault)
        await service.workers_once()
        await asyncio.gather(*service.worker_tasks.values())
        self.assertTrue(calls[0]['fresh'])
        self.assertEqual(calls[0]['secrets'], {'NPM_TOKEN': 'FAKE-NPM-VALUE-0123456789'})
        self.assertEqual(service.workers.get(worker['id'])['status'], 'done')
        if clear:
            self.declare()
        for _ in range(3):
            request = self.store.service_request('workers.steer', {'worker': worker['id'], 'prompt': 'Continue.'})
            await service.controls_once()
            resumed = service.workers.get(worker['id'])
            self.assertEqual(resumed['status'], 'queued')
            self.assertEqual(resumed['host'], 'worker-%d-resume-%d' % (worker['id'], request))
            await service.workers_once()
            await asyncio.gather(*service.worker_tasks.values())
            self.assertEqual(service.workers.get(worker['id'])['status'], 'done')
            self.assertEqual(calls[-1]['session'], session_id)
            self.assertFalse(calls[-1]['fresh'])
            self.assertEqual(calls[-1]['secrets'], None if clear else {'NPM_TOKEN': 'FAKE-NPM-VALUE-0123456789'})
            if provider == 'codex':
                self.assertEqual(calls[-1]['secret_names'], ['NPM_TOKEN'])
        self.assertEqual(len([use for use in self.uses() if use[2] == worker['id']]), 1 if clear else 4)
        self.assertEqual(len(self.vault.calls) - vault_calls, 1 if clear else 4)

    async def assert_refused(self, text):
        worker = self.worker()
        await self.pool.run(worker, self.store.topic(TOPIC))
        self.assertEqual(self.calls, [])
        result = self.result(worker)
        self.assertEqual((result['success'], result['failure_code']), (False, 'start_failed'))
        self.assertIn(text, result['error'])
        self.assertEqual(self.uses(), [])
        self.assertFalse(self.store.db.in_transaction)

    def problems(self):
        return [tuple(row) for row in self.store.db.execute(
            'SELECT area,code,detail,topic,task,worker FROM problems ORDER BY id')]


class SecretWorkerTests(SecretWorkerTestsSupport, unittest.IsolatedAsyncioTestCase):
    asyncTearDown = WorkerTests.asyncTearDown

    worker = WorkerTests.worker

    async def test_sharing_allows_another_jobs_key(self):
        other = self.store.task_create(TOPIC, 'Other')
        envelope = self.fill('GITHUB_TOKEN', 'FAKE-ENVELOPE-VALUE-0123456789', task=other['id'])
        self.declare('GITHUB_TOKEN')
        worker = self.worker()
        done = await self.pool.run(worker, self.store.topic(TOPIC))
        self.assertTrue(self.result(worker)['success'], self.result(worker))
        self.assertEqual(done['status'], 'done')
        self.assertEqual(self.calls[0]['secrets'], {'GITHUB_TOKEN': 'FAKE-ENVELOPE-VALUE-0123456789'})
        self.assertEqual(self.uses(), [(envelope, 'GITHUB_TOKEN', worker['id'], 'worker:%d' % worker['id'])])
        self.assertEqual(self.problems(), [])
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(len(self.uses()), 1)

    async def test_obsolete_sharing_setting_does_not_restrict_keys_or_change_data(self):
        from coordinator import control_api
        other = self.store.task_create(TOPIC, 'Other')
        self.fill('GITHUB_TOKEN', 'FAKE-ENVELOPE-VALUE-0123456789', task=other['id'])
        self.declare('GITHUB_TOKEN')
        for value in (False, True, 1):
            with self.subTest(value=value):
                with self.store.db:
                    self.store.put('secret_sharing', value)
                before = tuple(self.store.db.execute("SELECT * FROM settings WHERE key='secret_sharing'").fetchone())
                worker = self.worker()
                done = await self.pool.run(worker, self.store.topic(TOPIC))
                self.assertEqual(done['status'], 'done')
                self.assertTrue(self.result(worker)['success'], self.result(worker))
                self.assertEqual(self.calls[-1]['secrets'], {'GITHUB_TOKEN': 'FAKE-ENVELOPE-VALUE-0123456789'})
                settings = control_api.call(self.store, 'settings.get', source='cli')
                self.assertNotIn('secret_sharing', settings.data)
                self.assertEqual(tuple(self.store.db.execute(
                    "SELECT * FROM settings WHERE key='secret_sharing'").fetchone()), before)

    async def test_sharing_preserves_unasked_expired_cancelled_and_vault_failures(self):
        other = self.store.task_create(TOPIC, 'Other')
        self.fill('GITHUB_TOKEN', 'FAKE-ENVELOPE-VALUE-0123456789', task=other['id'])
        self.declare('GITHUB_TOKEN')
        worker = self.worker()
        await self.pool.run(worker, self.store.topic(TOPIC))
        self.assertTrue(self.result(worker)['success'], self.result(worker))
        uses = self.uses()
        for state, code in (('none', 'not-filled'), ('expired', 'wait-ended'), ('cancelled', 'wait-ended')):
            with self.subTest(state=state):
                name = state.upper() + '_TOKEN'
                if state != 'none':
                    envelope = self.fill(name, 'FAKE-UNAVAILABLE-0123456789', task=other['id'])
                    with self.store.db:
                        self.store.db.execute('UPDATE envelopes SET state=? WHERE id=?', (state, envelope))
                self.declare('GITHUB_TOKEN', name)
                failed = self.worker()
                await self.pool.run(failed, self.store.topic(TOPIC))
                self.assertIn('Secrets are not filled: %s.' % name, self.result(failed)['error'])
                self.assertEqual(self.problems()[-2][:3], ('secrets', code, 'names=%s states=%s' % (name, state)))
                self.assertEqual(self.uses(), uses)
        self.declare('GITHUB_TOKEN')
        self.vault.fail_next = 'integrity'
        failed = self.worker()
        await self.pool.run(failed, self.store.topic(TOPIC))
        self.assertIn('The vault read failed for GITHUB_TOKEN.', self.result(failed)['error'])
        self.assertEqual(self.problems()[-2][:3], ('secrets', 'vault-read-failed', 'name=GITHUB_TOKEN code=integrity'))
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.uses(), uses)

    async def test_sharing_reloads_another_jobs_key_on_finished_resumes(self):
        other = self.store.task_create(TOPIC, 'Other')
        self.fill('NPM_TOKEN', 'FAKE-NPM-VALUE-0123456789', task=other['id'])
        self.declare('NPM_TOKEN')
        worker = self.worker()
        await self.pool.run(worker, self.store.topic(TOPIC))
        self.assertTrue(self.result(worker)['success'], self.result(worker))
        for provider in ('claude', 'codex'):
            with self.subTest(provider=provider):
                await self.assert_finished_resumes_keep_secrets(provider, clear=False)

    async def test_sharing_resumes_with_mixed_job_keys_and_preserves_failure_checks(self):
        from coordinator import control_api
        from coordinator.service import Service
        other = self.store.task_create(TOPIC, 'Other')
        third = self.store.task_create(TOPIC, 'Third')
        self.fill('GITHUB_TOKEN', 'FAKE-ENVELOPE-VALUE-0123456789', task=other['id'])
        self.fill('DEPLOY_KEY', 'FAKE-DEPLOY-0123456789', task=third['id'])
        self.fill('NPM_TOKEN', 'FAKE-NPM-VALUE-0123456789')
        self.declare('GITHUB_TOKEN', 'GH_TOKEN=DEPLOY_KEY', 'NPM_TOKEN')
        calls = []
        session_id = '00000000-0000-4000-8000-000000000001'
        transcript = self.root / 'primary' / 'projects' / 'test' / (session_id + '.jsonl')
        transcript.parent.mkdir(parents=True, exist_ok=True)
        transcript.write_text('{}\n')

        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                calls.append(options)
                options['on_session'](session_id)
                return RunResult(session_id, text='done', success=True, transcript_path=str(transcript))

        service = Service(self.store, None, Runner(), self.root, vault=self.vault)
        worker = self.worker()
        await service.workers.run(worker, self.store.topic(TOPIC))
        self.assertTrue(self.result(worker)['success'], self.result(worker))

        async def resume():
            with self.store.db:
                requested = control_api.call(self.store, 'workers.steer', {'worker': worker['id'], 'prompt': 'Continue'},
                                             topic=TOPIC, source='mcp')
            self.assertTrue(requested.ok, requested.text)
            await service.controls_once()
            queued = service.workers.get(worker['id'])
            self.assertEqual((queued['status'], queued['session'], queued['fresh']), ('queued', session_id, 0))
            await service.workers.run(queued, self.store.topic(TOPIC))

        await resume()
        self.assertEqual(calls[-1]['secrets'], {'GITHUB_TOKEN': 'FAKE-ENVELOPE-VALUE-0123456789',
                                              'GH_TOKEN': 'FAKE-DEPLOY-0123456789', 'NPM_TOKEN': 'FAKE-NPM-VALUE-0123456789'})
        self.assertFalse(calls[-1]['fresh'])
        self.assertEqual(len(self.uses()), 6)
        self.declare('GITHUB_TOKEN', 'NEVER_ASKED')
        await resume()
        self.assertIn('Secrets are not filled: NEVER_ASKED.', self.result(worker)['error'])
        self.assertEqual(len(calls), 2)
        for state in ('expired', 'cancelled'):
            with self.subTest(state=state):
                name = state.upper() + '_TOKEN'
                envelope = self.fill(name, 'FAKE-UNAVAILABLE-0123456789', task=other['id'])
                with self.store.db:
                    self.store.db.execute('UPDATE envelopes SET state=? WHERE id=?', (state, envelope))
                self.declare('GITHUB_TOKEN', name)
                await resume()
                self.assertIn('Secrets are not filled: %s.' % name, self.result(worker)['error'])
                self.assertEqual(self.problems()[-2][1], 'wait-ended')
                self.assertEqual(len(calls), 2)
        self.declare('GITHUB_TOKEN')
        self.vault.fail_next = 'integrity'
        await resume()
        self.assertIn('The vault read failed for GITHUB_TOKEN.', self.result(worker)['error'])
        self.assertEqual(len(calls), 2)
        self.assertEqual(len(self.uses()), 6)

    async def test_sharing_reloads_another_jobs_key_on_a_codex_account_transfer(self):
        other = self.store.task_create(TOPIC, 'Other')
        envelope = self.fill('NPM_TOKEN', 'FAKE-NPM-VALUE-0123456789', task=other['id'])
        self.declare('NPM_TOKEN')
        worker = self.worker('codex', goal='finish')
        broker = self.pool.codex_accounts
        broker.run = AsyncMock(return_value=RunResult('saved-session', success=True))
        with patch.object(broker, 'record_rate_limit'), patch.object(broker, 'automatic', return_value=True), \
                patch.object(broker, 'select', return_value='next'):
            result = await self.pool.codex_attached(worker, self.store.topic(TOPIC),
                RunResult('saved-session', quota_limited=True), {'secret_names': ['NPM_TOKEN']}, lambda alias: None)
        self.assertTrue(result.success)
        self.assertEqual(broker.run.await_args.kwargs['secrets'], {'NPM_TOKEN': 'FAKE-NPM-VALUE-0123456789'})
        self.assertEqual(self.uses(), [(envelope, 'NPM_TOKEN', worker['id'], 'worker:%d' % worker['id'])])

    async def test_attached_worker_moved_by_a_limit_keeps_its_secrets_on_the_next_account(self):
        from coordinator.service import Service
        self.fill('NPM_TOKEN', 'FAKE-NPM-VALUE-0123456789')
        self.declare('NPM_TOKEN')
        second = self.root / 'second'
        second.mkdir()
        with self.store.db:
            accounts = self.store.get('accounts')
            accounts['second'] = {'config_dir': str(second), 'enabled': True}
            self.store.put('accounts', accounts)
            status = self.store.get('account_status')
            status['second'] = {'identity': {'email': 'second@example.com', 'logged_in': True},
                                'observed_at': time.time(),
                                'usage': {'seven_day': {'utilization': 10, 'resets_at': time.time() + 7200}}}
            self.store.put('account_status', status)
        sid = '20000000-0000-4000-8000-000000000077'
        transcript = self.root / 'primary' / 'projects' / 'p' / (sid + '.jsonl')
        transcript.parent.mkdir(parents=True)
        transcript.write_text('{}\n')
        worker = self.worker()
        with self.store.db:
            self.store.db.execute("UPDATE workers SET status='running',session=?,fresh=0,host='worker-x',"
                                  "account_alias='primary' WHERE id=?", (sid, worker['id']))
        calls = []

        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                calls.append(dict(options, prompt=prompt))
                if options.get('attach'):
                    return RunResult(session, quota_limited=True, transcript_path=str(transcript),
                                     rate_limit_info={'status': 'rejected', 'resetsAt': time.time() + 3600})
                return RunResult(session, text='ok', success=True, transcript_path=str(transcript))

        service = Service(self.store, None, Runner(), self.root, vault=self.vault)
        await service.workers.run(worker, self.store.topic(TOPIC))
        await service.workers_once()
        await asyncio.gather(*service.worker_tasks.values())
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[1]['account_alias'], 'second')
        self.assertEqual(calls[1]['secrets'], {'NPM_TOKEN': 'FAKE-NPM-VALUE-0123456789'})
        self.assertEqual(calls[1]['prompt'], LIMIT_CONTINUATION + 'build this')
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM messages WHERE kind='callback'").fetchone()[0], 0)
        self.assertEqual(service.workers.get(worker['id'])['status'], 'done')

    async def test_declared_secrets_reach_only_the_worker_options(self):
        first = self.fill('GITHUB_TOKEN_ORG', 'FAKE-ENVELOPE-VALUE-0123456789', task=self.task['id'])
        second = self.fill('NPM_TOKEN', 'FAKE-NPM-VALUE-0123456789')
        self.declare('GH_TOKEN=GITHUB_TOKEN_ORG', 'NPM_TOKEN')
        worker = self.worker()
        await self.pool.run(worker, self.store.topic(TOPIC))
        options = self.calls[0]
        self.assertEqual(options['secrets'], {'GH_TOKEN': 'FAKE-ENVELOPE-VALUE-0123456789',
                                              'NPM_TOKEN': 'FAKE-NPM-VALUE-0123456789'})
        self.assertIn('Secrets in the environment: GH_TOKEN, NPM_TOKEN.', options['instructions'])
        self.assertNotIn('FAKE-', options['instructions'])
        self.assertEqual(self.uses(), [(first, 'GITHUB_TOKEN_ORG', worker['id'], 'worker:%d' % worker['id']),
                                       (second, 'NPM_TOKEN', worker['id'], 'worker:%d' % worker['id'])])
        self.assertTrue(self.result(worker)['success'])
        self.assertFalse(self.store.db.in_transaction)

    async def test_codex_worker_receives_secrets_declared_after_queueing(self):
        self.fill('NPM_TOKEN', 'FAKE-NPM-VALUE-0123456789')
        worker = self.worker('codex')
        self.declare('NPM_TOKEN')
        done = await self.pool.run(worker, self.store.topic(TOPIC))
        self.assertEqual(done['status'], 'done')
        self.assertEqual(self.calls[0]['secrets'], {'NPM_TOKEN': 'FAKE-NPM-VALUE-0123456789'})
        self.assertEqual(self.calls[0]['secret_names'], ['NPM_TOKEN'])
        self.assertTrue(self.result(worker)['success'])
        self.assertEqual(len(self.uses()), 1)

    async def test_task_without_secrets_passes_none(self):
        await self.pool.run(self.worker(), self.store.topic(TOPIC))
        self.assertIsNone(self.calls[0]['secrets'])
        self.assertNotIn('Secrets in the environment', self.calls[0]['instructions'])

    async def test_codex_account_transfer_reloads_only_its_job_secrets(self):
        self.fill('NPM_TOKEN', 'FAKE-NPM-VALUE-0123456789')
        self.declare('NPM_TOKEN')
        worker = self.worker('codex', goal='finish')
        broker = self.pool.codex_accounts
        options = {'secrets': None, 'secret_names': ['NPM_TOKEN'], 'goal': 'finish', 'account_alias': 'old'}
        broker.run = AsyncMock(return_value=RunResult('00000000-0000-4000-8000-000000000001', success=True))
        with patch.object(broker, 'record_rate_limit'), patch.object(broker, 'automatic', return_value=True), \
                patch.object(broker, 'select', return_value='next'):
            result = await self.pool.codex_attached(worker, self.store.topic(TOPIC),
                RunResult('00000000-0000-4000-8000-000000000001', quota_limited=True), options, lambda alias: None)
        sent = broker.run.await_args.kwargs
        self.assertTrue(result.success)
        self.assertEqual(sent['secrets'], {'NPM_TOKEN': 'FAKE-NPM-VALUE-0123456789'})
        self.assertIn('Secrets in the environment: NPM_TOKEN.', sent['instructions'])
        self.assertNotIn('FAKE-', sent['instructions'])
        self.assertEqual(sent['secret_names'], ['NPM_TOKEN'])
        self.assertFalse(sent['attach'])
        self.assertFalse(sent['fresh'])
        self.assertEqual(sent['goal'], 'finish')

    async def test_codex_resume_keeps_memory_excluded_after_secret_declarations_are_cleared(self):
        worker = self.worker('codex')
        directory = self.store.directory / 'hosts' / ('worker-' + str(worker['id']))
        directory.mkdir(parents=True)
        (directory / 'spec.json').write_text(json.dumps({'secret_names': ['NPM_TOKEN']}))
        await self.pool.run(worker, self.store.topic(TOPIC))
        self.assertEqual(self.calls[0]['secret_names'], ['NPM_TOKEN'])
        self.assertIsNone(self.calls[0]['secrets'])

    async def test_finished_resumes_keep_names_after_declarations_are_cleared(self):
        self.fill('NPM_TOKEN', 'FAKE-NPM-VALUE-0123456789')
        for provider in ('codex', 'claude'):
            with self.subTest(provider=provider):
                await self.assert_finished_resumes_keep_secrets(provider, clear=True)

    async def test_finished_resumes_deliver_unchanged_declarations(self):
        self.fill('NPM_TOKEN', 'FAKE-NPM-VALUE-0123456789')
        for provider in ('codex', 'claude'):
            with self.subTest(provider=provider):
                await self.assert_finished_resumes_keep_secrets(provider, clear=False)

    async def test_finished_resumes_keep_names_from_versioned_hosts(self):
        self.fill('NPM_TOKEN', 'FAKE-NPM-VALUE-0123456789')
        await self.assert_finished_resumes_keep_secrets('codex', clear=True, versioned=True)

    async def test_codex_resume_unions_names_from_earlier_hosts_without_other_workers(self):
        worker = self.worker('codex')
        for name, secret in (('worker-%d.2' % worker['id'], 'NPM_TOKEN'),
                             ('worker-%d-resume-7.2' % worker['id'], 'GH_TOKEN'),
                             ('worker-%d' % (worker['id'] * 10), 'OTHER_TOKEN')):
            directory = self.store.directory / 'hosts' / name
            directory.mkdir(parents=True)
            (directory / 'spec.json').write_text(json.dumps({'secret_names': [secret]}))
        with self.store.db:
            self.store.db.execute("UPDATE workers SET host=? WHERE id=?",
                                  ('worker-%d-resume-8' % worker['id'], worker['id']))
        await self.pool.run(worker, self.store.topic(TOPIC))
        self.assertEqual(self.calls[0]['secret_names'], ['GH_TOKEN', 'NPM_TOKEN'])
        self.assertIsNone(self.calls[0]['secrets'])

    async def test_missing_secret_fails_before_the_provider_starts(self):
        self.fill('NPM_TOKEN', 'FAKE-NPM-VALUE-0123456789')
        self.declare('GITHUB_TOKEN', 'NPM_TOKEN')
        await self.assert_refused('Secrets are not filled: GITHUB_TOKEN.')
        self.assertEqual(self.vault.calls, [])
        rows = self.problems()
        self.assertEqual(rows[0], ('secrets', 'not-filled', 'names=GITHUB_TOKEN states=none', TOPIC, self.task['id'], None))
        self.assertEqual(rows[1][:2], ('worker', 'start_failed'))
        self.assertNotIn('FAKE-', json.dumps(rows))

    async def test_worker_waits_for_an_asked_secret_and_starts_when_it_is_filled(self):
        with self.store.db:
            envelope = self.store.db.execute('''INSERT INTO envelopes(name,reason,consumer,task,topic,state,created,
                expires,updated) VALUES ('GITHUB_TOKEN','r','c',?,?,'open',1,9999999999,1)''',
                                             (self.task['id'], TOPIC)).lastrowid
        self.declare('GITHUB_TOKEN')
        worker = self.worker()
        for _ in range(2):
            waiting = await self.pool.run(self.pool.get(worker['id']), self.store.topic(TOPIC))
        self.assertEqual(waiting['status'], 'waiting_for_secret')
        self.assertEqual(self.calls, [])
        self.assertEqual(self.problems(), [])
        notes = [row['text'] for row in self.store.db.execute("SELECT text FROM messages WHERE kind='callback'")]
        self.assertEqual(notes, ['Worker %d waits for GITHUB_TOKEN. It starts when the owner fills the secret card.'
                                 % worker['id']])
        self.vault.values['GITHUB_TOKEN'] = 'FAKE-ENVELOPE-VALUE-0123456789'
        with self.store.db:
            self.store.db.execute("UPDATE envelopes SET state='filled' WHERE id=?", (envelope,))
        done = await self.pool.run(self.pool.get(worker['id']), self.store.topic(TOPIC))
        self.assertEqual(done['status'], 'done')
        self.assertEqual(self.calls[0]['secrets'], {'GITHUB_TOKEN': 'FAKE-ENVELOPE-VALUE-0123456789'})
        self.assertTrue(self.result(worker)['success'])

    async def test_vault_read_error_names_the_secret(self):
        self.fill('GITHUB_TOKEN', 'FAKE-ENVELOPE-VALUE-0123456789')
        self.declare('GITHUB_TOKEN')
        self.vault.fail_next = 'integrity'
        await self.assert_refused('The vault read failed for GITHUB_TOKEN.')
        self.assertEqual(self.problems()[0][:3], ('secrets', 'vault-read-failed', 'name=GITHUB_TOKEN code=integrity'))
        self.assertNotIn('FAKE-', json.dumps(self.problems()))
