"""Failures that used to escape as a generic internal error or a refused signal."""

import asyncio
import json
from pathlib import Path
import signal
import tempfile
import time
import unittest
from unittest.mock import patch
import uuid

from coordinator import log, workspaces
from coordinator.accounts import AccountBroker, find_transcript
from coordinator.failures import TaskFailure
from coordinator.providers import RunResult
from coordinator.store import Store
from coordinator.workers import WorkerPool
from tests.support import settle
from tests.test_store import update


class Runner:
    def __init__(self):
        self.calls = []

    async def run(self, provider, prompt, cwd, session, **options):
        self.calls.append(session)
        return RunResult(session, success=True)


class RefusedSignalTests(unittest.IsolatedAsyncioTestCase):
    """os.killpg can refuse. The shielded cleanup must not turn that into the
    exception the caller sees."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.log_path = log.configure(Path(self.temp.name) / 'state', stderr=False)

    def tearDown(self):
        log.reset()
        self.temp.cleanup()

    async def spawned(self, command):
        """Start the command and return once its process exists and it waits for output."""
        started = asyncio.Event()
        spawn = asyncio.create_subprocess_exec

        async def recorded(*args, **options):
            process = await spawn(*args, **options)
            started.set()
            return process

        with patch.object(workspaces.asyncio, 'create_subprocess_exec', recorded):
            task = asyncio.create_task(command)
            await settle(started.wait())
        return task

    async def test_cancelling_a_command_still_raises_cancellation_when_signals_are_refused(self):
        attempted = []

        def refuse(pid, number):
            attempted.append(number)
            raise PermissionError(1, 'Operation not permitted')

        task = await self.spawned(workspaces._command('sleep', '0.4'))
        with patch.object(workspaces.os, 'killpg', refuse):
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(attempted, [signal.SIGTERM, signal.SIGKILL])
        self.assertEqual(self.log_path.read_text().count('cannot signal workspace process group'), 2)

    async def test_a_vanished_process_group_is_still_silent(self):
        gone = []

        def vanished(pid, number):
            gone.append(number)
            raise ProcessLookupError()

        task = await self.spawned(workspaces._command('sleep', '0.4'))
        with patch.object(workspaces.os, 'killpg', vanished):
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(gone, [signal.SIGTERM, signal.SIGKILL])
        self.assertNotIn('cannot signal', self.log_path.read_text())


    async def test_cancelling_during_spawn_still_stops_the_new_process_group(self):
        real_killpg = workspaces.os.killpg
        signalled = []

        def record(pid, number):
            signalled.append((pid, number))
            real_killpg(pid, number)

        with patch.object(workspaces.os, 'killpg', record):
            task = asyncio.create_task(workspaces._command('sleep', '30'))
            await asyncio.sleep(0)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual([number for _, number in signalled][:1], [signal.SIGTERM])
        with self.assertRaises(ProcessLookupError):
            real_killpg(signalled[0][0], 0)

class DuplicateTranscriptTests(unittest.IsolatedAsyncioTestCase):
    """One session ID in two saved transcripts is an ownership question for the
    owner, not an internal error."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store = Store(self.root / 'state')
        self.session = str(uuid.uuid4())
        self.paths = []
        for alias in ('one', 'two'):
            folder = self.root / alias / 'projects' / ('-Users-owner-' + alias)
            folder.mkdir(parents=True)
            transcript = folder / (self.session + '.jsonl')
            transcript.write_text('{}\n')
            self.paths.append(str(transcript.resolve()))
        with self.store.db:
            self.store.put('accounts', {alias: {'config_dir': str(self.root / alias), 'enabled': True}
                                        for alias in ('one', 'two')})
            self.store.put('account_status', {alias: {'identity': {'logged_in': True}}
                                              for alias in ('one', 'two')})
        self.router = AccountBroker(self.store)

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def test_find_transcript_names_every_duplicate(self):
        with self.assertRaises(TaskFailure) as caught:
            find_transcript(self.session, [self.root / 'one', self.root / 'two'])
        for path in self.paths:
            self.assertIn(path, caught.exception.cause)
        self.assertIn('move or remove', caught.exception.action)

    async def test_resuming_an_ambiguous_session_refuses_before_a_provider_starts(self):
        runner = Runner()
        with self.assertRaises(TaskFailure):
            await self.router.run(runner.run, 'claude', 'work', self.root, self.session, fresh=False)
        self.assertEqual(runner.calls, [])

    async def test_a_worker_reports_the_cause_instead_of_a_generic_start_failure(self):
        self.store.accept(update(1, '/pair ' + self.store.pairing_code()), creator=True)
        self.store.bind('-10042:4', str(self.root), 'project', enabled=True)
        topic = self.store.topic('-10042:4')
        task = self.store.task_create(topic['id'], 'Work', worktree=str(self.root))
        pool = WorkerPool(self.store, Runner(), self.router)
        with self.store.db:
            worker_id = self.store.db.execute('''INSERT INTO workers
                (task,topic,provider,prompt,cwd,session,fresh,created,updated)
                VALUES (?,?,?,?,?,?,?,?,?)''',
                (task['id'], topic['id'], 'claude', 'work', str(self.root),
                 self.session, 0, time.time(), time.time())).lastrowid
        evidence = await pool.run(pool.get(worker_id), topic)
        self.assertEqual(evidence['status'], 'done')
        evidence = json.loads(evidence['result'])
        self.assertFalse(evidence['success'])
        for path in self.paths:
            self.assertIn(path, evidence['error'])
        self.assertNotIn('Worker could not start', evidence['error'])
        stored = json.loads(self.store.db.execute(
            'SELECT result FROM workers WHERE id=?', (worker_id,)).fetchone()[0])
        self.assertEqual(stored['error'], evidence['error'])


class WorkerStartFailureTests(unittest.IsolatedAsyncioTestCase):
    """A worker that cannot start names its cause, never a bare class name or a private path."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store = Store(self.root / 'state')

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    async def test_a_start_failure_stores_an_owner_safe_cause(self):
        private = '/Users/owner/.claude/profiles/work/settings.json'

        async def refuse(provider, prompt, cwd, session, **options):
            raise PermissionError(13, 'Permission denied', private)

        self.store.accept(update(1, '/pair ' + self.store.pairing_code()), creator=True)
        self.store.bind('-10042:4', str(self.root), 'project', enabled=True)
        topic = self.store.topic('-10042:4')
        task = self.store.task_create(topic['id'], 'Work', worktree=str(self.root))
        with self.store.db:
            self.store.put('accounts', {'primary': {'config_dir': str(self.root), 'enabled': True}})
            self.store.put('account_status', {'primary': {'identity': {
                'email': 'primary@example.com', 'logged_in': True}, 'observed_at': time.time(),
                'usage': {'seven_day': {'utilization': 10, 'resets_at': time.time() + 3600}}}})
        pool = WorkerPool(self.store, Runner(), AccountBroker(self.store))
        pool.runner.run = refuse
        with self.store.db:
            worker_id = self.store.db.execute('''INSERT INTO workers
                (task,topic,provider,prompt,cwd,created,updated)
                VALUES (?,?,?,?,?,?,?)''',
                (task['id'], topic['id'], 'claude', 'work', str(self.root),
                 time.time(), time.time())).lastrowid
        row = await pool.run(pool.get(worker_id), topic)
        evidence = json.loads(row['result'])

        detail = evidence['failure_detail']
        self.assertEqual(evidence['failure_code'], 'start_failed')
        self.assertRegex(detail, r'^PermissionError: .*Permission denied.*settings\.json.* at test_error_paths\.py:\d+$')
        self.assertLessEqual(len(detail), 200)
        self.assertNotIn('/Users/owner', detail)
        self.assertNotIn('/Users/owner', evidence['error'])
        self.assertIn(detail, evidence['error'])
        message = json.loads(self.store.db.execute('SELECT text FROM messages WHERE source_worker=?',
                                                   (worker_id,)).fetchone()[0])
        self.assertEqual(message['failure_detail'], detail)


if __name__ == '__main__':
    unittest.main()
