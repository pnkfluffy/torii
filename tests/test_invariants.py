import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from coordinator import invariants
from coordinator.store import Store


class InvariantTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store(Path(self.temp.name))
        self.addCleanup(self.store.close)
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('1:2',1,2,'Home','')")
            self.store.put('coordinator_home_topic', '1:2')

    def test_budget_and_daily_throttle(self):
        now = time.time()
        job = self.store.task_create('1:2', 'Work')
        with self.store.db:
            self.store.record_mark_stale(job['id'])
            self.store.db.execute('UPDATE stale_records SET since=? WHERE task=?', (now - 599, job['id']))
        checks = (next(item for item in invariants.CHECKS if item[0] == 'record-rewrite'),)
        with patch.object(invariants, 'CHECKS', checks):
            invariants.check_pending(self.store, now)
            self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], 0)
            invariants.check_pending(self.store, now + 2)
            invariants.check_pending(self.store, now + 3)
            self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], 1)
            with self.store.db:
                self.store.db.execute('INSERT OR REPLACE INTO stale_records(task,version,since) VALUES (?,1,?)', (job['id'], now - 600))
            with patch('coordinator.problems.time.time', return_value=now + invariants.DAY + 3):
                invariants.check_pending(self.store, now + invariants.DAY + 3)
            self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], 2)

    def test_each_pending_check_obeys_its_budget(self):
        now = 1000000
        for check in invariants.CHECKS:
            code, budget, _ = check
            with self.subTest(code=code):
                store = Mock()
                store.db.execute.return_value.fetchall.return_value = []
                with patch.object(invariants, 'CHECKS', (check,)), patch.object(invariants, 'notice') as notice:
                    store.db.execute.return_value.fetchone.return_value = (now - budget + 1,)
                    invariants.check_pending(store, now)
                    notice.assert_not_called()
                    store.db.execute.return_value.fetchone.return_value = (now - budget,)
                    invariants.check_pending(store, now)
                    self.assertEqual(notice.call_count, 1)

    def test_each_pending_query_detects_an_old_row(self):
        now = time.time()
        for check in invariants.CHECKS:
            code, budget, _ = check
            with self.subTest(code=code), tempfile.TemporaryDirectory() as directory:
                store = Store(Path(directory))
                try:
                    with store.db:
                        store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('1:2',1,2,'Home','')")
                        store.put('coordinator_home_topic', '1:2')
                    job = store.task_create('1:2', code)
                    recent = now - budget + 1
                    with store.db:
                        if code == 'saving':
                            store.db.execute("UPDATE tasks SET status='done',copy='saving' WHERE id=?", (job['id'],))
                            store.db.execute('UPDATE task_iterations SET closed=? WHERE task=?', (recent, job['id']))
                        elif code == 'record-rewrite':
                            store.db.execute('INSERT OR REPLACE INTO stale_records(task,version,since) VALUES (?,1,?)', (job['id'], recent))
                        elif code == 'outbox':
                            store.enqueue_report('1:2', 'Queued')
                            store.db.execute('UPDATE outbox SET created=?', (recent,))
                        elif code == 'worker-temp':
                            store.db.execute("""INSERT INTO workers(task,topic,provider,prompt,status,created,updated)
                                VALUES (?,'1:2','codex','Work','done',?,?)""", (job['id'], recent, recent))
                        elif code == 'held':
                            store.db.execute("UPDATE tasks SET copy='held',updated=? WHERE id=?", (recent, job['id']))
                        elif code == 'sweep':
                            store.db.execute("INSERT INTO sweep_failures(sweep,item,failures,since,failed,retry) "
                                             "VALUES ('fade',?,2,?,?,?)", (job['id'], recent, now, now + 60))
                        elif code == 'finish':
                            store.db.execute("UPDATE tasks SET finish='live' WHERE id=?", (job['id'],))
                            store.db.execute("INSERT INTO task_stamps(task,stage,created,iteration) VALUES (?,'live',?,0)", (job['id'], recent))
                    existing = store.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0]
                    with patch.object(invariants, 'CHECKS', (check,)):
                        invariants.check_pending(store, now)
                        self.assertEqual(store.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], existing)
                        invariants.check_pending(store, now + 2)
                        self.assertEqual(store.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], existing + 1)
                finally:
                    store.close()

    def test_failed_query_does_not_stop_other_checks(self):
        now = time.time()
        checks = (('broken', 1, 'SELECT no_such_column FROM tasks'),
                  ('old', 1, 'SELECT 1'))
        with patch.object(invariants, 'CHECKS', checks):
            invariants.check_pending(self.store, now)
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM problems WHERE area='invariants' AND code='check-failed'").fetchone()[0], 1)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], 1)

    def test_alerts_name_oldest_jobs_and_count_the_rest(self):
        now = time.time()
        jobs = [self.store.task_create('1:2', 'Work') for _ in range(4)]
        with self.store.db:
            for index, job in enumerate(jobs):
                self.store.db.execute("UPDATE tasks SET copy='held', updated=?, finish='live' WHERE id=?",
                                      (now - invariants.DAY - 10 + index, job['id']))
                self.store.db.execute("INSERT INTO task_stamps(task,stage,created,iteration) VALUES (?,'live',?,0)",
                                      (job['id'], now - invariants.DAY - 10 + index))
        checks = tuple(check for check in invariants.CHECKS if check[0] in ('held', 'finish'))
        with patch.object(invariants, 'CHECKS', checks), patch.object(invariants, 'notice') as notice:
            invariants.check_pending(self.store, now)
        texts = {call.args[1]: call.args[2] for call in notice.call_args_list}
        names = '#1 (Home), #2 (Home), #3 (Home), and 1 more'
        self.assertEqual(texts['held'], 'Job records not saved for over a day: ' + names)
        self.assertEqual(texts['finish'], 'Jobs reached their finish stage but are still open after 6 h: ' + names)

    def test_sweep_alert_names_failing_sweeps_and_ignores_gone_items(self):
        now = time.time()
        with self.store.db:
            self.store.db.executemany('INSERT INTO sweep_failures(sweep,item,failures,since,failed,retry) VALUES (?,?,9,?,?,?)', [
                ('fade', '1', now - invariants.DAY - 10, now - 60, now + 60),
                ('closed-copy-delete', 'stage', now - invariants.DAY - 10, now - 60, now + 60),
                ('idle-trim', '2', now - 9 * invariants.DAY, now - 3 * invariants.DAY, now - 2 * invariants.DAY),
                ('scratch-git-discard', '3', now - 60, now - 60, now + 60)])
        checks = tuple(check for check in invariants.CHECKS if check[0] == 'sweep')
        with patch.object(invariants, 'CHECKS', checks), patch.object(invariants, 'notice') as notice:
            invariants.check_pending(self.store, now)
        self.assertEqual(notice.call_args.args[2], 'Periodic cleanup has failed for over a day: closed-copy-delete, fade.')
        with self.store.db:
            self.store.db.execute("DELETE FROM sweep_failures WHERE sweep IN ('fade','closed-copy-delete')")
        with patch.object(invariants, 'CHECKS', checks), patch.object(invariants, 'notice') as notice:
            invariants.check_pending(self.store, now)
        notice.assert_not_called()

    def test_idle_large_names_jobs_and_sizes(self):
        now = time.time()
        jobs = [self.store.task_create('1:2', 'Work') for _ in range(4)]
        with self.store.db:
            for index, job in enumerate(jobs):
                self.store.db.execute('UPDATE tasks SET updated=? WHERE id=?', (now - invariants.DAY - 10 + index, job['id']))
        with patch.object(invariants.records, 'storage_bytes', return_value=2 * invariants.GIB), \
             patch.object(invariants.os.path, 'lexists', return_value=True), \
             patch.object(invariants, 'large_tree', return_value=False), patch.object(invariants, 'notice') as notice:
            invariants.check_storage(self.store, now)
        calls = [call.args[2] for call in notice.call_args_list if call.args[1] == 'idle-large']
        self.assertEqual(calls, ['Idle open jobs over 5 GiB: #1 (Home, 6.0 GiB), #2 (Home, 6.0 GiB), #3 (Home, 6.0 GiB), and 1 more'])

    def test_idle_size_counts_absent_folders_as_empty_and_names_a_failed_path(self):
        now = time.time()
        job = self.store.task_create('1:2', 'Work')
        with self.store.db:
            self.store.db.execute('UPDATE tasks SET updated=? WHERE id=?', (now - invariants.DAY - 10, job['id']))
        scratch = self.store.directory / 'scratch' / ('task-%d' % job['id'])
        locked = scratch / 'locked'
        locked.mkdir(parents=True)
        (locked / 'file').write_text('x')
        rows = "SELECT detail FROM problems WHERE code='idle-size-failed'"
        with patch.object(invariants, 'large_tree', return_value=False):
            invariants.check_storage(self.store, now)
            self.assertEqual(self.store.db.execute(rows).fetchall(), [])
            locked.chmod(0)
            self.addCleanup(locked.chmod, 0o700)
            if os.access(locked, os.R_OK):
                self.skipTest('permissions are not enforced')
            with self.store.db:
                self.store.put('invariant_storage_checked', 0)
            invariants.check_storage(self.store, now)
        [row] = self.store.db.execute(rows).fetchall()
        self.assertEqual(row['detail'], 'Job storage could not be measured: %s (EACCES)' % locked)

    def test_uncommitted_counts_only_record_files(self):
        import subprocess
        root = self.store.directory / 'records'
        (root / 'chat-1').mkdir(parents=True)
        subprocess.run(['git', 'init', '-q', str(root)], check=True)
        (root / 'chat-1' / '0001').mkdir()
        (root / 'chat-1' / '0001' / 'notes.log').write_text('legacy\n')
        with patch.object(invariants, 'pending_since', return_value=None) as pending:
            invariants.check_git(self.store, time.time())
            (root / 'chat-1' / '0001.md').write_text('record\n')
            invariants.check_git(self.store, time.time())
        self.assertEqual([call.args[2] for call in pending.call_args_list if call.args[1] == 'uncommitted'], [False, True])
