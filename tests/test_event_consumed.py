import asyncio
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from coordinator.host import HostClient
from coordinator.store import Store


class EventConsumedTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)
        self.store = self.open()
        self.commits = 0
        self.store.db.set_trace_callback(self.trace)

    def open(self):
        store = Store(self.root / 'state')
        self.addCleanup(store.db.close)
        return store

    def trace(self, statement):
        self.commits += statement == 'COMMIT'

    def workers(self, count):
        with self.store.db:
            self.store.db.execute("INSERT OR IGNORE INTO topics(id,chat,thread,name,cwd) VALUES ('topic',1,1,'topic','/')")
            ids = [self.store.db.execute("""INSERT INTO workers(topic,provider,prompt,status,created,updated)
                VALUES ('topic','claude','work','running',1,1)""").lastrowid for _ in range(count)]
        self.store.consumed_flush()
        self.store.consumed_changes = self.store.db.total_changes
        self.commits = 0
        return ids

    def saved(self, worker, store=None):
        return (store or self.store).db.execute('SELECT last_seq FROM workers WHERE id=?', (worker,)).fetchone()[0]

    def journal(self, count):
        directory = self.root / 'host'
        directory.mkdir(exist_ok=True)
        (directory / 'pid').write_text(str(os.getpid()))
        (directory / 'events.jsonl').write_text(''.join(
            json.dumps({'seq': seq, 'data': json.dumps({'event': seq})}) + '\n' for seq in range(1, count + 1)))
        return directory

    def attach(self, directory, worker, store=None):
        store = store or self.store
        client = HostClient.attach(directory, last_seq=self.saved(worker, store),
                                   on_seq=store.consumer(worker))
        self.addCleanup(client.detach)
        return client

    async def consume(self, client, count):
        seen = []
        for _ in range(count):
            seen.append(json.loads(await client.next_event())['event'])
            client.ack()
        return seen

    def test_events_of_many_workers_are_saved_in_one_commit(self):
        ids = self.workers(70)
        for seq in range(1, 201):
            for worker in ids:
                self.store.consumer(worker)(seq)
        self.assertEqual(self.commits, 0)
        self.assertEqual({self.saved(worker) for worker in ids}, {0})
        self.store.consumed_flush()
        self.assertEqual(self.commits, 1)
        self.assertEqual({self.saved(worker) for worker in ids}, {200})
        self.store.consumed_flush()
        self.assertEqual(self.commits, 1)

    def test_coordinator_session_sequence_is_saved_with_the_workers(self):
        worker, = self.workers(1)
        self.store.consumer('coordinator_host_seq')(7)
        self.store.consumer(worker)(3)
        self.assertIsNone(self.store.get('coordinator_host_seq'))
        self.store.consumed_flush()
        self.assertEqual(self.commits, 1)
        self.assertEqual((self.store.get('coordinator_host_seq'), self.saved(worker)), (7, 3))

    def test_event_with_a_saved_effect_is_saved_at_once(self):
        worker, = self.workers(1)
        self.store.consumer(worker)(1)
        self.assertEqual(self.saved(worker), 0)
        with self.store.db:
            self.store.put('effect', True)
        self.store.consumer(worker)(2)
        self.assertEqual(self.saved(worker), 2)
        self.store.consumer(worker)(3)
        self.assertEqual(self.saved(worker), 2)

    def test_locked_database_is_not_waited_for(self):
        worker, = self.workers(1)
        other = sqlite3.connect(self.store.directory / 'state.sqlite')
        self.addCleanup(other.close)
        other.execute('BEGIN IMMEDIATE')
        started = time.monotonic()
        self.store.consumer(worker)(4)
        self.store.consumed_flush()
        self.assertLess(time.monotonic() - started, 5)
        self.assertEqual(self.store.consumed, {worker: 4})
        self.assertEqual(self.store.db.execute('PRAGMA busy_timeout').fetchone()[0], 30000)
        other.rollback()
        self.store.consumed_flush()
        self.assertEqual(self.store.consumed, {})
        self.assertEqual(self.saved(worker), 4)

    def lock_briefly(self):
        other = sqlite3.connect(self.store.directory / 'state.sqlite', check_same_thread=False)
        other.isolation_level = None
        other.execute('BEGIN IMMEDIATE')
        timer = threading.Timer(1.0, lambda: other.execute('COMMIT'))
        timer.start()
        self.addCleanup(other.close)
        self.addCleanup(timer.join)

    async def test_event_with_a_saved_effect_waits_for_a_short_lock(self):
        worker, = self.workers(1)
        with self.store.db:
            self.store.put('effect', 7)
        self.lock_briefly()
        self.store.consumer(worker)(7)
        self.assertEqual(self.store.consumed, {})
        await self.shutdown()
        self.assertEqual(self.saved(worker, self.open()), 7)

    async def test_stopped_service_waits_for_a_short_lock(self):
        worker, = self.workers(1)
        self.store.consumer(worker)(8)
        self.lock_briefly()
        await self.shutdown()
        self.assertEqual(self.saved(worker, self.open()), 8)

    async def test_shutdown_past_its_deadline_still_saves(self):
        worker, = self.workers(1)
        self.store.consumer(worker)(5)

        async def stubborn():
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                await asyncio.sleep(60)
        child = asyncio.create_task(stubborn())
        await asyncio.sleep(0)
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(self.shutdown([child]), 0.2)
        self.assertEqual(self.saved(worker), 5)

    def test_open_transaction_is_not_committed_by_a_flush(self):
        worker, = self.workers(1)
        self.store.consumer(worker)(1)
        with self.store.transaction():
            self.store.put('half', True)
            self.store.consumed_flush()
            self.assertTrue(self.store.db.in_transaction)
        self.assertEqual(self.store.consumed, {worker: 1})

    async def test_crash_replays_only_events_without_effects(self):
        worker, = self.workers(1)
        directory = self.journal(6)
        client = self.attach(directory, worker)
        self.assertEqual(await self.consume(client, 2), [1, 2])
        await client.next_event()
        with self.store.db:
            self.store.put('effect', 3)
        client.ack()
        self.assertEqual(await self.consume(client, 2), [4, 5])
        self.assertEqual(self.commits, 2)
        restarted = self.open()
        self.assertEqual(self.saved(worker, restarted), 3)
        again = self.attach(directory, worker, restarted)
        self.assertEqual(list(again.history(again.last_seq)), [json.dumps({'event': seq}) for seq in (1, 2, 3)])
        self.assertEqual(await self.consume(again, 3), [4, 5, 6])
        restarted.consumed_flush()
        self.assertEqual(self.saved(worker, restarted), 6)

    async def test_sequence_is_saved_before_a_write_to_the_host(self):
        worker, = self.workers(1)
        client = self.attach(self.journal(2), worker)
        await self.consume(client, 2)
        self.assertEqual(self.saved(worker), 0)
        with patch('coordinator.host.asyncio.open_unix_connection', side_effect=ConnectionRefusedError):
            with self.assertRaises(ConnectionRefusedError):
                await client.request('write', line='{}')
        self.assertEqual(self.saved(worker), 2)

    async def shutdown(self, tasks=()):
        from coordinator.service import Service

        class Extension:
            async def close(self):
                pass

        service = Service.__new__(Service)
        service.store, service.sessions, service.extension = self.store, {}, Extension()
        service.feed_tasks = service.worker_tasks = service.control_tasks = service.stop_tasks = {}
        await service.shutdown(list(tasks))

    async def test_stopped_service_saves_buffered_sequences(self):
        worker, = self.workers(1)
        self.store.consumer(worker)(9)
        await self.shutdown()
        self.assertEqual(self.saved(worker), 9)


if __name__ == '__main__':
    unittest.main()
