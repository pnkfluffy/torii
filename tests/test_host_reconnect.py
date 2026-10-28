import asyncio
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch
import uuid

from coordinator.accounts import AccountBroker
from coordinator.host import HostClient, alive
from coordinator.native_protocol import RunControl
from coordinator.providers import ProviderRunner, RunResult
from coordinator.service import Service
from coordinator.store import Store
from coordinator.workers import WorkerPool
from tests.support import isolate_shared_mcp_home, read_jsonl, settle, stop_test_hosts, until


SERVER = '''#!/usr/bin/env python3
import json, os, sys, threading, time
from pathlib import Path
root = Path(__file__).parent
sid = SID
lock = threading.Lock()
def emit(event):
    with lock:
        print(json.dumps(event), flush=True)
def finish():
    while not (root / 'tick').exists():
        time.sleep(.01)
    emit({'method': 'item/started', 'params': {'threadId': sid, 'turnId': 'turn-1',
        'item': {'type': 'commandExecution'}}})
    (root / 'ticked').touch()
    while not (root / 'finish').exists():
        time.sleep(.01)
    emit({'method': 'turn/completed', 'params': {'threadId': sid,
        'turn': {'id': 'turn-1', 'status': 'completed'}}})
for line in sys.stdin:
    request = json.loads(line)
    with (root / 'calls.jsonl').open('a') as stream:
        stream.write(json.dumps({'pid': os.getpid(), 'method': request.get('method'),
            'params': request.get('params')}) + '\\n')
    method = request.get('method')
    if method == 'initialize':
        emit({'id': request['id'], 'result': {}})
    elif method in ('thread/start', 'thread/resume'):
        emit({'id': request['id'], 'result': {'thread': {'id': sid}}})
    elif method == 'turn/start':
        emit({'id': request['id'], 'result': {'turn': {'id': 'turn-1'}}})
        emit({'method': 'turn/started', 'params': {'threadId': sid, 'turn': {'id': 'turn-1'}}})
        (root / 'ready').touch()
        threading.Thread(target=finish).start()
    elif method == 'turn/steer':
        emit({'id': request['id'], 'result': {'turnId': 'turn-1'}})
'''


class ReconnectTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)
        isolate_shared_mcp_home(self, self.root)
        self.addCleanup(stop_test_hosts, self.root)

    def journal(self, name='journal'):
        directory = self.root / name
        directory.mkdir()
        (directory / 'pid').write_text(str(os.getpid()))
        client = HostClient(directory)
        self.addCleanup(client.detach)
        return client, directory / 'events.jsonl'

    def record(self, seq):
        return (json.dumps({'seq': seq, 'data': json.dumps({'method': 'event-' + str(seq)})}) + '\n').encode()

    async def test_partial_event_is_reread_without_acknowledging_or_losing_it(self):
        client, path = self.journal()
        record = self.record(1)
        path.write_bytes(record[:12])
        pending = asyncio.create_task(client.next_event())
        self.addAsyncCleanup(self.cancel, pending)
        await asyncio.sleep(.1)
        self.assertFalse(pending.done(), 'An unfinished event must wait for its newline')
        self.assertIsNone(client.event_sequence)
        self.assertEqual(client.last_seq, 0)
        with path.open('ab') as stream:
            stream.write(record[12:])
        self.assertEqual(json.loads(await settle(pending)), {'method': 'event-1'})
        client.ack()
        self.assertEqual(client.last_seq, 1)

    async def cancel(self, task):
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def test_all_replay_readers_skip_invalid_complete_records_and_retry_partial_tail(self):
        client, path = self.journal()
        path.write_bytes(b'not-json\n[]\n\xff\n' + self.record(1) + self.record(2)[:12])
        self.assertEqual(list(client.history(1)), [json.dumps({'method': 'event-1'})])
        self.assertEqual(list(client.history_after(0)), [json.dumps({'method': 'event-1'})])
        self.assertIsNone(client.past_response(7))
        self.assertFalse(client.past_echo('missing'))
        self.assertEqual(json.loads(await client.next_event()), {'method': 'event-1'})
        client.ack()
        with path.open('ab') as stream:
            stream.write(self.record(2)[12:])
        self.assertEqual(json.loads(await client.next_event()), {'method': 'event-2'})
        client.ack()
        self.assertEqual(client.last_seq, 2)

    async def test_sparse_event_journal_keeps_the_first_record_after_the_hole(self):
        client, path = self.journal()
        with path.open('wb') as stream:
            stream.seek(32 * 1024 * 1024)
            stream.write(self.record(1))
        self.assertEqual(list(client.history(1)), [json.dumps({'method': 'event-1'})])
        self.assertEqual(json.loads(await client.next_event()), {'method': 'event-1'})
        client.ack()
        self.assertEqual(client.last_seq, 1)

    async def test_partial_write_journal_is_not_treated_as_a_complete_request(self):
        client, path = self.journal()
        path.touch()
        request = json.dumps({'line': json.dumps({'id': 1, 'method': 'initialize'})}) + '\n'
        writes = path.with_name('writes.jsonl')
        writes.write_text(request + '{')
        self.assertEqual(client.written_lines(), [json.dumps({'id': 1, 'method': 'initialize'})])

    async def test_running_codex_survives_sparse_replay_without_another_prompt_or_process(self):
        sid = str(uuid.uuid4())
        binary = self.root / 'codex'
        binary.write_text(SERVER.replace('SID', repr(sid)))
        binary.chmod(0o700)
        runner = ProviderRunner(self.root / 'state', binaries={'codex': str(binary)})
        saved = []
        first = asyncio.create_task(runner.run('codex', 'original task', self.root, sid, fresh=False,
            host_id='worker-codex', control=RunControl(), on_seq=saved.append))
        self.addAsyncCleanup(self.cancel, first)
        await until(lambda: (self.root / 'ready').exists() and len(saved) >= 4)
        directory = self.root / 'state/hosts/worker-codex'
        pid = HostClient(directory).child_pid
        await self.cancel(first)
        self.assertTrue(alive(pid))
        with (directory / 'events.jsonl').open('r+b') as stream:
            stream.truncate(0)
        (self.root / 'tick').touch()
        await until(lambda: (self.root / 'ticked').exists() and (directory / 'events.jsonl').stat().st_size > 0)
        control = RunControl()
        second = asyncio.create_task(runner.run('codex', 'original task', self.root, sid, fresh=False,
            host_id='worker-codex', control=control, attach=True, last_seq=max(saved)))
        self.addAsyncCleanup(self.cancel, second)
        receipt = asyncio.create_task(control.steer('check', 'continue working'))
        self.addAsyncCleanup(self.cancel, receipt)
        self.assertEqual(await settle(receipt), 'received')
        self.assertEqual(HostClient(directory).child_pid, pid)
        self.assertTrue(alive(pid))
        (self.root / 'finish').touch()
        result = await settle(second)
        self.assertTrue(result.success, result.error)
        self.assertEqual(result.pid, pid)
        calls = read_jsonl(self.root / 'calls.jsonl')
        self.assertEqual({call['pid'] for call in calls}, {pid})
        self.assertEqual([call['method'] for call in calls],
                         ['initialize', 'initialized', 'thread/resume', 'turn/start', 'turn/steer'])

    def store_worker(self, provider='codex', session=True):
        store = Store(self.root / 'state')
        self.addCleanup(store.close)
        store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('home',-100,1,'Home',?,1)",
                         (str(self.root),))
        task = store.task_create('home', 'Resume', worktree=str(self.root))
        sid = str(uuid.uuid4()) if session else None
        wid = store.db.execute('''INSERT INTO workers
            (task,topic,provider,prompt,cwd,workspace,session,fresh,status,host,account_alias,created,updated)
            VALUES (?, 'home', ?, 'saved task', ?, '{}', ?, ?, 'running', 'worker-dead', 'test', 1, 1)''',
            (task['id'], provider, str(self.root), sid, not session)).lastrowid
        store.db.commit()
        return store, wid, sid

    async def test_host_dying_during_reattach_requeues_the_same_session_without_a_failure_report(self):
        store, wid, sid = self.store_worker()
        run = AsyncMock(side_effect=[RunResult(sid, failure_code='host_died'), RunResult(sid, success=True)])
        pool = WorkerPool(store, type('Runner', (), {'run': run})(), AccountBroker(store))
        first = await pool.run(pool.get(wid), store.topic('home'))
        self.assertEqual(first['status'], 'queued')
        self.assertEqual(first['session'], sid)
        self.assertEqual(store.db.execute('SELECT COUNT(*) FROM messages WHERE source_worker=?', (wid,)).fetchone()[0], 0)
        done = await pool.run(first, store.topic('home'))
        self.assertEqual(done['status'], 'done')
        self.assertTrue(json.loads(done['result'])['success'])
        self.assertEqual(run.await_count, 2)
        args, options = run.await_args
        self.assertEqual(args[3], sid)
        self.assertTrue(args[1].startswith('You were interrupted by a Torii restart and are being resumed.'))
        self.assertIn('Inspect your prior progress', args[1])
        self.assertIn('pushes, PR creation or sends', args[1])
        self.assertTrue(args[1].endswith('saved task'))
        self.assertFalse(options['fresh'])
        self.assertFalse(options['attach'])

    async def test_service_resumes_a_missing_host_instead_of_ending_the_worker(self):
        store, wid, sid = self.store_worker()
        run = AsyncMock(return_value=RunResult(sid, success=True))
        runner = type('Runner', (), {'run': run})()
        service = Service(store, object(), runner, self.root)
        self.addCleanup(service.workers.storage.close)
        with patch.object(service.jobs, 'resume_due', AsyncMock()):
            await service.workers_once()
            await asyncio.gather(*service.worker_tasks.values())
            if service.workers.get(wid)['status'] == 'queued':
                await service.workers_once()
                await asyncio.gather(*service.worker_tasks.values())
        self.assertEqual(service.workers.get(wid)['status'], 'done')
        self.assertTrue(json.loads(service.workers.get(wid)['result'])['success'])
        self.assertEqual(run.await_count, 1)
        args, options = run.await_args
        self.assertEqual(args[3], sid)
        self.assertFalse(options['fresh'])
        self.assertFalse(options['attach'])

    async def test_an_unavailable_socket_does_not_stop_a_host_that_is_still_alive(self):
        store, wid, sid = self.store_worker()
        directory = store.directory / 'hosts/worker-dead'
        directory.mkdir(parents=True)
        (directory / 'pid').write_text(str(os.getpid()))
        runner = type('Runner', (), {'run': AsyncMock()})()
        service = Service(store, object(), runner, self.root)
        self.addCleanup(service.workers.storage.close)
        with patch.object(service.jobs, 'resume_due', AsyncMock()), \
                patch.object(HostClient, 'connect', AsyncMock(side_effect=RuntimeError('unavailable'))), \
                patch.object(HostClient, 'stop', AsyncMock()) as stop:
            await service.workers_once()
        self.assertEqual(service.workers.get(wid)['status'], 'running')
        stop.assert_not_awaited()
        runner.run.assert_not_awaited()

    async def test_unreachable_host_retries_with_cooldown_then_resumes_after_cleanup(self):
        store, wid, sid = self.store_worker()
        directory = store.directory / 'hosts/worker-dead'
        directory.mkdir(parents=True)
        (directory / 'pid').write_text(str(os.getpid()))
        runner = type('Runner', (), {'run': AsyncMock(return_value=RunResult(sid, success=True))})()
        service = Service(store, object(), runner, self.root)
        self.addCleanup(service.workers.storage.close)

        async def stopped():
            (directory / 'exit.json').write_text(json.dumps({'returncode': -9}))

        with patch.object(service.jobs, 'resume_due', AsyncMock()), \
                patch.object(HostClient, 'connect', AsyncMock(side_effect=RuntimeError('unavailable'))) as connect, \
                patch.object(HostClient, 'stop', AsyncMock(side_effect=stopped)) as stop, \
                patch('coordinator.service.time', wraps=time) as clock:
            clock.monotonic.return_value = 100
            await service.workers_once()
            await service.workers_once()
            self.assertEqual(connect.await_count, 1)
            stop.assert_not_awaited()
            clock.monotonic.return_value = 106
            await service.workers_once()
            self.assertEqual(connect.await_count, 2)
            stop.assert_not_awaited()
            clock.monotonic.return_value = 112
            await service.workers_once()
            await asyncio.gather(*service.worker_tasks.values())
            if service.workers.get(wid)['status'] == 'queued':
                await service.workers_once()
                await asyncio.gather(*service.worker_tasks.values())
        self.assertEqual(connect.await_count, 3)
        stop.assert_awaited_once()
        self.assertEqual(runner.run.await_count, 1)
        args, options = runner.run.await_args
        self.assertEqual(args[3], sid)
        self.assertFalse(options['fresh'])
        self.assertFalse(options['attach'])
        self.assertTrue(json.loads(service.workers.get(wid)['result'])['success'])
        self.assertEqual(service.worker_reattach_retries, {})
        self.assertEqual(store.db.execute("SELECT COUNT(*) FROM problems WHERE worker=? AND code='reattach-lost'",
                                         (wid,)).fetchone()[0], 1)

    async def test_socket_probe_timeout_is_bounded_and_a_later_connection_clears_retries(self):
        store, wid, sid = self.store_worker()
        directory = store.directory / 'hosts/worker-dead'
        directory.mkdir(parents=True)
        (directory / 'pid').write_text(str(os.getpid()))
        runner = type('Runner', (), {'run': AsyncMock(return_value=RunResult(sid, success=True))})()
        service = Service(store, object(), runner, self.root)
        self.addCleanup(service.workers.storage.close)
        cancelled = asyncio.Event()

        async def connect():
            try:
                await asyncio.Future()
            finally:
                cancelled.set()

        with patch.object(service.jobs, 'resume_due', AsyncMock()), \
                patch.object(HostClient, 'connect', AsyncMock(side_effect=connect)), \
                patch.object(HostClient, 'stop', AsyncMock()) as stop, \
                patch('coordinator.service.WORKER_REATTACH_TIMEOUT', .01), \
                patch('coordinator.service.time', wraps=time) as clock:
            clock.monotonic.return_value = 100
            await asyncio.wait_for(service.workers_once(), 2)
            self.assertTrue(cancelled.is_set())
            self.assertEqual(service.workers.get(wid)['status'], 'running')
            runner.run.assert_not_awaited()
            clock.monotonic.return_value = 106
            with patch.object(HostClient, 'connect', AsyncMock()):
                await service.workers_once()
                await asyncio.gather(*service.worker_tasks.values())
        self.assertEqual(service.worker_reattach_retries, {})
        stop.assert_not_awaited()
        self.assertTrue(runner.run.await_args.kwargs['attach'])
        self.assertEqual(store.db.execute("SELECT COUNT(*) FROM problems WHERE worker=? AND code='reattach-lost'",
                                         (wid,)).fetchone()[0], 0)

    async def test_unreachable_host_with_failed_cleanup_cannot_resume_or_retry_forever(self):
        store, wid, sid = self.store_worker()
        directory = store.directory / 'hosts/worker-dead'
        directory.mkdir(parents=True)
        (directory / 'pid').write_text(str(os.getpid()))
        runner = type('Runner', (), {'run': AsyncMock()})()
        service = Service(store, object(), runner, self.root)
        self.addCleanup(service.workers.storage.close)
        with patch.object(service.jobs, 'resume_due', AsyncMock()), \
                patch.object(HostClient, 'connect', AsyncMock(side_effect=RuntimeError('unavailable'))) as connect, \
                patch.object(HostClient, 'stop', AsyncMock(side_effect=RuntimeError('cannot stop'))) as stop, \
                patch('coordinator.service.time', wraps=time) as clock:
            for now in (100, 106, 112, 118):
                clock.monotonic.return_value = now
                await service.workers_once()
        self.assertEqual(connect.await_count, 3)
        stop.assert_awaited_once()
        runner.run.assert_not_awaited()
        worker = service.workers.get(wid)
        self.assertEqual(worker['status'], 'interrupted')
        self.assertEqual(json.loads(worker['result'])['failure_code'], 'cleanup_failed')
