import asyncio
import json
import os
import shutil
import sqlite3
import time
import unittest
from unittest.mock import patch

from coordinator import opencode_cleanup
from tests import test_session_storage as session_fixtures
from tests import test_worktree_cleanup as fixtures


class OpenCodeCleanupTests(unittest.TestCase):
    tearDown = fixtures.WorktreeCleanupTests.tearDown
    git = fixtures.WorktreeCleanupTests.git
    call = fixtures.WorktreeCleanupTests.call
    worker = fixtures.WorktreeCleanupTests.worker

    def setUp(self):
        fixtures.WorktreeCleanupTests.setUp(self)
        fixture = patch('coordinator.host_os.trash_directory', return_value=self.trash)
        fixture.start()
        self.addCleanup(fixture.stop)

    def make_worker(self, status='done', worker_status='done', age=4 * 86400):
        task = self.call('tasks.create', title='OpenCode').data
        with self.store.db:
            self.store.db.execute('UPDATE tasks SET status=? WHERE id=?', (status, task['id']))
            self.store.db.execute("UPDATE tasks SET copy='cleared' WHERE id=?", (task['id'],))
        (self.state / 'scratch' / ('task-' + str(task['id']))).rmdir()
        worker = self.worker(task['id'], self.project, status=worker_status, provider='opencode')
        root = self.state / 'opencode' / 'workers' / str(worker)
        db = root / 'data' / 'opencode' / 'opencode.db'
        db.parent.mkdir(parents=True)
        con = sqlite3.connect(db)
        con.executescript('''CREATE TABLE session(id, parent_id, directory, tokens_input, tokens_output,
            tokens_reasoning, tokens_cache_read, tokens_cache_write, cost);
            CREATE TABLE message(id, session_id, time_created, data);''')
        session = ('s1', None, '/project', 10, 4, 2, 3, 1, 0.5)
        data = {'role': 'assistant', 'time': {'created': 1000, 'completed': 2000}, 'modelID': 'model',
                'providerID': 'provider', 'tokens': {'input': 10, 'output': 4, 'reasoning': 2,
                'cache': {'read': 3, 'write': 1}, 'total': 20}, 'cost': 0.5}
        con.execute('INSERT INTO session VALUES (?,?,?,?,?,?,?,?,?)', session)
        con.execute('INSERT INTO message VALUES (?,?,?,?)', ('m1', 's1', 999, json.dumps(data)))
        con.commit()
        con.close()
        old = time.time() - age
        os.utime(db, (old, old))
        return task, worker, root, session

    def run_sweep(self):
        with patch('coordinator.worker_storage._processes_gone', return_value=True), \
             patch('coordinator.stray_report.process_paths', return_value=[]), \
             patch('coordinator.stray_report.launch_paths', return_value=[]):
            asyncio.run(opencode_cleanup.sweep(self.store))

    def test_old_worker_usage_saved_before_removal(self):
        task, worker, root, session = self.make_worker()
        self.run_sweep()
        self.assertFalse(root.exists())
        self.assertTrue((self.trash / ('torii-opencode-worker-' + str(worker)) / 'data/opencode/opencode.db').is_file())
        usage = self.state / 'opencode' / 'usage' / (str(worker) + '.jsonl')
        rows = [json.loads(line) for line in usage.read_text().splitlines()]
        self.assertEqual(rows[0], {'type': 'session', 'values': list(session)})
        self.assertEqual(rows[1], {'type': 'message', 'id': 'm1', 'session_id': 's1', 'time_created': 999,
                         'api_created': 1000, 'completed': 2000, 'model': 'model', 'provider': 'provider',
                         'input': 10, 'output': 4, 'reasoning': 2, 'cache_read': 3, 'cache_write': 1,
                         'total': 20, 'cost': 0.5})
        freed = self.store.db.execute('SELECT bytes_freed FROM task_iterations WHERE task=?', (task['id'],)).fetchone()[0]
        self.assertGreater(freed, 0)

    def test_fresh_shared_memory_file_does_not_block_removal(self):
        _, _, root, _ = self.make_worker()
        (root / 'data' / 'opencode' / 'opencode.db-shm').write_bytes(b'index')
        self.run_sweep()
        self.assertFalse(root.exists())

    def test_wal_database_is_saved_without_resetting_age(self):
        _, worker, root, _ = self.make_worker()
        database = root / 'data' / 'opencode' / 'opencode.db'
        con = sqlite3.connect(database)
        con.execute('PRAGMA journal_mode=WAL')
        con.close()
        old = time.time() - 4 * 86400
        for path in database.parent.iterdir():
            os.utime(path, (old, old))
        self.run_sweep()
        self.assertFalse(root.exists())
        usage = self.state / 'opencode' / 'usage' / (str(worker) + '.jsonl')
        self.assertIn('"m1"', usage.read_text())

    def test_recent_worker_is_kept(self):
        _, _, root, _ = self.make_worker(age=60)
        self.run_sweep()
        self.assertTrue(root.exists())

    def test_nonterminal_job_is_kept(self):
        for status in ('open', 'waiting', 'interrupted'):
            with self.subTest(status=status):
                _, _, root, _ = self.make_worker(status=status)
                self.run_sweep()
                self.assertTrue(root.exists())

    def test_unfinished_worker_is_kept(self):
        for status in ('queued', 'running', 'waiting_for_secret', 'waiting_for_quota'):
            with self.subTest(status=status):
                _, _, root, _ = self.make_worker(worker_status=status)
                self.run_sweep()
                self.assertTrue(root.exists())

    def test_bad_home_does_not_stop_the_sweep(self):
        _, bad, bad_root, _ = self.make_worker()
        database = bad_root / 'data' / 'opencode' / 'opencode.db'
        database.unlink()
        database.symlink_to(database)
        old = time.time() - 4 * 86400
        os.utime(database, (old, old), follow_symlinks=False)
        _, _, root, _ = self.make_worker()
        self.run_sweep()
        self.assertTrue(bad_root.exists())
        self.assertFalse(root.exists())
        [detail] = [row[0] for row in self.store.db.execute(
            "SELECT detail FROM problems WHERE area='cleanup' AND code='opencode-worker-cleanup-failed' AND worker=?",
            (bad,))]
        self.assertRegex(detail, r'^item=%d failures=1 \w+Error: .* at \w+\.py:\d+ ' % bad)
        self.run_sweep()
        self.assertEqual(self.store.db.execute(
            "SELECT COUNT(*) FROM problems WHERE code='opencode-worker-cleanup-failed'").fetchone()[0], 1)

    def test_failed_remnant_backs_off_and_success_clears(self):
        remnants = [self.state / 'opencode' / 'workers' / ('%d.deleting-' % number + 'a' * 32) for number in (7, 8)]
        for remnant in remnants:
            (remnant / 'data').mkdir(parents=True)
        real = opencode_cleanup.host_os.move_to_trash

        def remove(path, **options):
            if path == remnants[0]:
                raise PermissionError('fixture remnant refusal')
            return real(path, **options)

        with patch.object(opencode_cleanup.host_os, 'move_to_trash', side_effect=remove) as removed:
            self.run_sweep()
            self.run_sweep()
        self.assertEqual([call.args[0] for call in removed.call_args_list], remnants)
        self.assertTrue(remnants[0].exists())
        self.assertFalse(remnants[1].exists())
        [detail] = [row[0] for row in self.store.db.execute(
            "SELECT detail FROM problems WHERE code='opencode-remnant-failed'")]
        self.assertRegex(detail, r'^item=7\.deleting-a+ failures=1 PermissionError: fixture remnant refusal at '
                                 r'test_opencode_cleanup\.py:\d+ remove < ')
        with patch('coordinator.sweeps.time.time', return_value=time.time() + 120):
            self.run_sweep()
        self.assertFalse(remnants[0].exists())
        self.assertIsNone(self.store.db.execute('SELECT 1 FROM sweep_failures').fetchone())

    def test_uncleared_copy_is_kept(self):
        task, _, root, _ = self.make_worker()
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET copy='saving' WHERE id=?", (task['id'],))
        self.run_sweep()
        self.assertTrue(root.exists())

    def test_open_file_keeps_worker(self):
        _, _, root, _ = self.make_worker()
        with patch('coordinator.worker_storage._processes_gone', return_value=True), \
             patch('coordinator.stray_report.process_paths', return_value=[root / 'data' / 'opencode' / 'opencode.db']), \
             patch('coordinator.stray_report.launch_paths', return_value=[]):
            asyncio.run(opencode_cleanup.sweep(self.store))
        self.assertTrue(root.exists())

    def test_live_job_link_keeps_worker(self):
        _, _, root, _ = self.make_worker()
        task = self.call('tasks.create', title='Active').data
        scratch = self.state / 'scratch' / ('task-' + str(task['id']))
        scratch.mkdir(parents=True, exist_ok=True)
        (scratch / 'worker-home').symlink_to(root, target_is_directory=True)
        self.run_sweep()
        self.assertTrue(root.exists())

    def test_save_failure_keeps_worker(self):
        _, _, root, _ = self.make_worker()
        with patch('coordinator.opencode_cleanup.save_usage', side_effect=OSError('save failed')):
            self.run_sweep()
        self.assertTrue(root.exists())

    def test_retry_keeps_usage_from_earlier_save(self):
        _, worker, root, _ = self.make_worker()
        target = self.state / 'opencode' / 'usage' / (str(worker) + '.jsonl')
        opencode_cleanup.save_usage(root, target, self.state)
        database = root / 'data' / 'opencode' / 'opencode.db'
        with sqlite3.connect(database) as con:
            con.execute('DELETE FROM message')
        old = time.time() - 4 * 86400
        os.utime(database, (old, old))
        self.run_sweep()
        rows = [json.loads(line) for line in target.read_text().splitlines()]
        self.assertFalse(root.exists())
        self.assertEqual([row['id'] for row in rows if row['type'] == 'message'], ['m1'])

    def test_live_process_keeps_worker(self):
        _, _, root, _ = self.make_worker()
        with patch('coordinator.worker_storage._processes_gone', return_value=False):
            asyncio.run(opencode_cleanup.sweep(self.store))
        self.assertTrue(root.exists())

    def test_detached_remnant_is_removed(self):
        remnant = self.state / 'opencode' / 'workers' / ('7.deleting-' + 'a' * 32)
        (remnant / 'data').mkdir(parents=True)
        other = self.state / 'opencode' / 'workers' / 'notes.deleting-x'
        other.mkdir()
        self.run_sweep()
        self.assertFalse(remnant.exists())
        self.assertTrue(other.exists())


OPENCODE = session_fixtures.OPENCODE


class OpenCodeSessionCleanupTests(unittest.IsolatedAsyncioTestCase):
    setUp = session_fixtures.SessionStorageTests.setUp
    worker = session_fixtures.SessionStorageTests.worker
    close_round = session_fixtures.SessionStorageTests.close_round
    remember = session_fixtures.SessionStorageTests.remember
    sweep = session_fixtures.SessionStorageTests.sweep

    def database(self, home, rows):
        path = home / 'data/opencode/opencode.db'
        path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(path) as db:
            db.executescript('''CREATE TABLE session(id, parent_id, directory, tokens_input, tokens_output,
                tokens_reasoning, tokens_cache_read, tokens_cache_write, cost);
                CREATE TABLE message(id, session_id, time_created, data);''')
            for sid, parent in rows:
                db.execute('INSERT INTO session VALUES (?,?,?,?,?,?,?,?,?)', (sid, parent, '/project', 10, 4, 2, 3, 1, 0.5))
            db.execute('INSERT INTO message VALUES (?,?,?,?)', ('m1', OPENCODE, 999,
                       json.dumps({'role': 'assistant', 'tokens': {'total': 20}})))
        old = time.time() - 15 * 86400
        os.utime(path, (old, old))
        return path

    def finished_worker(self, rows):
        worker = self.worker('opencode', sid=OPENCODE)
        home = self.store.directory / 'opencode' / 'workers' / str(worker)
        self.remember(worker, 'opencode', OPENCODE, home=home)
        self.close_round()
        return worker, self.database(home, rows)

    async def test_private_worker_database_and_subagent_go_to_trash_as_one_file(self):
        child = 'ses_' + 'b' * 26
        worker, path = self.finished_worker(((OPENCODE, None), (child, OPENCODE)))
        auth = path.with_name('auth.json')
        auth.write_text('fixture')
        await self.sweep()
        self.assertFalse(path.exists())
        self.assertTrue(auth.exists())
        with sqlite3.connect(next(self.trash.iterdir())) as db:
            self.assertEqual(dict(db.execute('SELECT id,parent_id FROM session')), {OPENCODE: None, child: OPENCODE})
        usage = self.store.directory / 'opencode' / 'usage' / (str(worker) + '.jsonl')
        self.assertIn('"total":20', usage.read_text())

    async def test_worker_home_cleanup_can_follow_session_cleanup(self):
        worker, path = self.finished_worker(((OPENCODE, None),))
        with opencode_cleanup.worker_home_lease(self.store.directory, worker):
            await self.sweep()
        self.assertTrue(path.exists())
        await self.sweep()
        self.assertFalse(path.exists())
        self.store.put('session_delete', False)
        self.store.db.execute("UPDATE tasks SET copy='cleared' WHERE id=?", (self.job['id'],))
        self.store.db.commit()
        shutil.rmtree(self.store.directory / 'scratch' / ('task-' + str(self.job['id'])), ignore_errors=True)
        home = path.parents[2]
        with patch('coordinator.worker_storage._processes_gone', return_value=True), \
             patch('coordinator.stray_report.launch_paths', return_value=[]):
            await opencode_cleanup.sweep(self.store)
        self.assertFalse(home.exists())
        self.assertTrue((self.trash / ('torii-opencode-worker-' + str(worker))).is_dir())
        usage = self.store.directory / 'opencode' / 'usage' / (str(worker) + '.jsonl')
        self.assertIn('"total":20', usage.read_text())

    async def test_unrelated_session_keeps_private_database(self):
        _, path = self.finished_worker(((OPENCODE, None), ('ses_' + 'b' * 26, None)))
        await self.sweep()
        self.assertTrue(path.exists())
        self.assertFalse(self.trash.exists())

    async def test_live_subagent_keeps_private_database(self):
        child = 'ses_' + 'b' * 26
        _, path = self.finished_worker(((OPENCODE, None), (child, OPENCODE)))
        active = self.store.task_create('1:2', 'Resume a child')
        self.worker('opencode', status='running', sid=child, job=active)
        await self.sweep()
        self.assertTrue(path.exists())

    async def test_uncheckpointed_data_keeps_database(self):
        _, path = self.finished_worker(((OPENCODE, None),))
        sidecar = path.with_name(path.name + '-wal')
        sidecar.write_bytes(b'pending write')
        age = path.stat().st_mtime
        os.utime(sidecar, (age, age))
        await self.sweep()
        self.assertTrue(path.exists())
        self.assertTrue(sidecar.exists())

    async def test_save_usage_failure_keeps_database(self):
        _, path = self.finished_worker(((OPENCODE, None),))
        with patch('coordinator.opencode_cleanup.save_usage', side_effect=OSError('save failed')):
            await self.sweep()
        self.assertTrue(path.exists())
        self.assertFalse(self.trash.exists())

    async def test_process_path_and_live_job_link_keep_database(self):
        _, path = self.finished_worker(((OPENCODE, None),))
        with patch('coordinator.stray_report.process_paths', return_value=[path]):
            await self.sweep()
        self.assertTrue(path.exists())
        active = self.store.task_create('1:2', 'Link to data')
        link = self.store.directory / 'scratch' / ('task-' + str(active['id'])) / 'provider-data'
        link.parent.mkdir(parents=True, exist_ok=True)
        link.symlink_to(path.parent)
        await self.sweep()
        self.assertTrue(path.exists())

    async def test_shared_chat_database_kept_when_it_contains_a_newer_session(self):
        newer, current = ('ses_' + key * 26 for key in ('b', 'c'))
        for sid in (OPENCODE, newer, current):
            self.remember(provider='opencode', sid=sid, parent='coordinator')
        self.store.put('coordinator_session', current)
        path = self.database(self.home, ((OPENCODE, None), (current, None)))
        await self.sweep()
        self.assertTrue(path.exists())
        self.assertFalse(self.trash.exists())

    async def test_retired_single_session_chat_database_can_move(self):
        for sid in (OPENCODE, 'ses_' + 'b' * 26, 'ses_' + 'c' * 26):
            self.remember(provider='opencode', sid=sid, parent='coordinator')
        path = self.database(self.home, ((OPENCODE, None),))
        await self.sweep()
        self.assertFalse(path.exists())
        self.assertIn('opencode.db', next(self.trash.iterdir()).name)
