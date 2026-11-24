import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from coordinator import close_saves, host_os, idle_trim, job_limits
from tests import test_worktree_cleanup as fixtures

GB = 1024 ** 3


class JobLimitsTests(unittest.TestCase):
    setUp = fixtures.WorktreeCleanupTests.setUp
    tearDown = fixtures.WorktreeCleanupTests.tearDown
    git = fixtures.WorktreeCleanupTests.git
    call = fixtures.WorktreeCleanupTests.call
    target = fixtures.WorktreeCleanupTests.target
    job = fixtures.WorktreeCleanupTests.job

    def test_worker_idle_alerts_and_progress(self):
        task = self.job(closed=False)
        with self.store.db:
            self.store.db.execute('UPDATE topics SET enabled=1 WHERE id=?', (fixtures.TOPIC,))
            self.store.put('coordinator_home_topic', fixtures.TOPIC)
            self.store.db.execute('''INSERT INTO workers(task,topic,provider,prompt,status,pid,created,updated)
                VALUES (?,?, 'claude','work','running',999999,?,?)''',
                (task['id'], fixtures.TOPIC, 1000, 1000))
        with patch.object(job_limits, 'child_commands', return_value=['eval loop']):
            self.assertEqual(job_limits.idle_workers(self.store, 8199), 0)
            self.assertEqual(job_limits.idle_workers(self.store, 8200), 1)
            self.assertEqual(job_limits.idle_workers(self.store, 15399), 0)
            self.assertEqual(job_limits.idle_workers(self.store, 15400), 1)
            with self.store.db:
                self.store.db.execute('UPDATE workers SET last_seq=1,updated=15500')
            self.assertEqual(job_limits.idle_workers(self.store, 15500), 0)
            self.assertEqual(job_limits.idle_workers(self.store, 22699), 0)
            self.assertEqual(job_limits.idle_workers(self.store, 22700), 1)
        events, reports = self.notices()
        self.assertEqual(len(events), 3)
        self.assertFalse(reports)
        self.assertIn('job %s' % task['number'], events[0])
        self.assertIn('eval loop', events[0])
        self.assertIn('If it waits on a loop that cannot end, steer it or stop that loop.', events[0])

    def test_worker_idle_survives_reattach_and_old_record(self):
        task = self.job(closed=False)
        with self.store.db:
            self.store.db.execute('UPDATE topics SET enabled=1 WHERE id=?', (fixtures.TOPIC,))
            worker = self.store.db.execute('''INSERT INTO workers(task,topic,provider,prompt,status,created,updated,last_seq)
                VALUES (?,?,'claude','work','running',1000,2000,3)''', (task['id'], fixtures.TOPIC)).lastrowid
            self.store.put('worker_idle_alerts', {str(worker): {'progress': [3, 2000], 'since': 2000, 'alert': 0}})
            self.store.db.execute('UPDATE workers SET updated=8000 WHERE id=?', (worker,))
        with patch.object(job_limits, 'child_commands', return_value=[]):
            self.assertEqual(job_limits.idle_workers(self.store, 9199), 0)
            self.assertEqual(job_limits.idle_workers(self.store, 9200), 1)
        self.assertEqual(self.store.get('worker_idle_alerts', {})[str(worker)]['since'], 2000)

    def test_worker_idle_skips_waits(self):
        task = self.job(closed=False)
        with self.store.db:
            self.store.db.execute('UPDATE topics SET enabled=1 WHERE id=?', (fixtures.TOPIC,))
            self.store.db.execute('''INSERT INTO workers(task,topic,provider,prompt,status,created,updated)
                VALUES (?,?,'claude','work','running',1000,1000)''', (task['id'], fixtures.TOPIC))
            self.store.db.execute("INSERT INTO waits(kind,key,topic,ask,created,updated) VALUES ('owner','idle-test',?,'ask',1000,1000)", (fixtures.TOPIC,))
            self.store.db.execute('INSERT INTO wait_jobs(wait,task,created) VALUES (last_insert_rowid(),?,1000)', (task['id'],))
        self.assertEqual(job_limits.idle_workers(self.store, 9000), 0)
        with self.store.db:
            self.store.db.execute("UPDATE waits SET state='cancelled'")
        self.assertEqual(job_limits.idle_workers(self.store, 16199), 0)
        with patch.object(job_limits, 'child_commands', return_value=[]):
            self.assertEqual(job_limits.idle_workers(self.store, 16200), 1)
        for status in ('waiting_for_secret', 'waiting_for_quota', 'waiting_for_owner'):
            with self.store.db:
                self.store.db.execute('UPDATE workers SET status=?', (status,))
            self.assertEqual(job_limits.idle_workers(self.store, 30000), 0)

    def test_child_commands_trim_and_missing_pid(self):
        output = "7 /bin/sh -c . /x/shell-snapshot; eval '" + 'x' * 200 + "'\n" + "7 second\n7 third\n7 fourth\n8 other\n"
        with patch('coordinator.job_limits.subprocess.run') as run:
            run.return_value.stdout = output
            commands = job_limits.child_commands(7)
        self.assertEqual(len(commands), 3)
        self.assertTrue(commands[0].startswith("eval '"))
        self.assertEqual(len(commands[0]), 150)
        with patch('coordinator.job_limits.subprocess.run', side_effect=OSError):
            self.assertEqual(job_limits.child_commands(999999), [])

    def notices(self):
        events = [row['text'] for row in self.store.db.execute("SELECT text FROM messages WHERE kind='event' ORDER BY id")]
        reports = [row['text'] for row in self.store.db.execute("SELECT text FROM outbox WHERE text LIKE 'Storage alarm%' ORDER BY id")]
        return events, reports

    def test_size_alarm_once_per_ten_gb_of_growth(self):
        with self.store.db:
            self.store.put('coordinator_home_topic', fixtures.TOPIC)
        task = self.job(closed=False)
        sizes = {'results': 0}

        def usage(label, root):
            return sizes[label], None, {label + '/big': sizes[label]}

        expected = []
        with patch.object(job_limits, 'usage', side_effect=usage), \
                patch.object(job_limits, 'bounded_scan', side_effect=lambda store, function, *args: function(*args)):
            for scratch, worktree, alarm in ((9 * GB, GB - 1, False), (6 * GB, 4.5 * GB, True), (10 * GB, 5 * GB, False),
                                             (10 * GB, 10.4 * GB, False), (10 * GB, 10.6 * GB, True),
                                             (3 * GB, 2 * GB, False), (9 * GB, 5.9 * GB, False), (9 * GB, 6.1 * GB, True)):
                sizes.update(scratch=scratch, worktree=worktree)
                self.assertEqual(job_limits.sweep(self.state), int(alarm), (scratch, worktree))
                expected += [alarm]
        events, reports = self.notices()
        self.assertEqual(len(events), expected.count(True))
        self.assertEqual(events, reports)
        self.assertIn('job %s' % task['number'], events[0])
        self.assertIn('"Keep job code"', events[0])
        self.assertIn('project', events[0])
        self.assertIn('10.5 GB', events[0])
        self.assertIn('scratch/big 6.0 GB, worktree/big 4.5 GB', events[0])

    def test_results_size_is_noted_and_flagged_over_2_gb_without_a_message(self):
        from coordinator import records
        with self.store.db:
            self.store.put('coordinator_home_topic', fixtures.TOPIC)
        task = self.job(closed=False)
        sizes = {'scratch': 0, 'worktree': 0, 'results': 2 * GB + 1}

        def usage(label, root):
            return sizes[label], None, {label + '/big': sizes[label]}

        def sweep():
            with patch.object(job_limits, 'usage', side_effect=usage), \
                    patch.object(job_limits, 'bounded_scan', side_effect=lambda store, function, *args: function(*args)):
                self.assertEqual(job_limits.sweep(self.state), 0)
            return self.call('tasks.get', task=task['id'])

        result = sweep()
        self.assertEqual(result.data['results_size'], {'gb': 2.0, 'over_2gb': True})
        self.assertIn('Results: 2.0 GB, over 2 GB', result.text)
        data, _ = records.read(records.write(self.store, self.store.task_get(task['id'])))
        self.assertEqual(data['results_size'], {'gb': 2.0, 'over_2gb': True})
        sizes['results'] = GB
        result = sweep()
        self.assertEqual(result.data['results_size'], {'gb': 1.0, 'over_2gb': False})
        self.assertIn('Results: 1.0 GB.', result.text)
        self.assertEqual(self.notices(), ([], []))
        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        self.assertIsNone(sweep().data['results_size'])

    def test_alarm_uses_real_folder_sizes(self):
        task = self.job(closed=False)
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        (scratch / 'out').mkdir()
        (scratch / 'out' / 'data.bin').write_bytes(b'x' * 100000)
        size, newest, parts = job_limits.usage('scratch', scratch)
        self.assertGreaterEqual(size, 100000)
        self.assertGreaterEqual(parts['scratch/out'], 100000)
        self.assertGreater(newest, time.time() - 60)
        with patch.object(job_limits, 'LIMIT', 50000):
            self.assertEqual(job_limits.sweep(self.state), 1)
            self.assertEqual(job_limits.sweep(self.state), 0)
        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        job_limits.sweep(self.state)
        self.assertIsNone(self.store.db.execute('SELECT 1 FROM job_size_alarms WHERE task=?', (task['id'],)).fetchone())

    def trash_here(self):
        trash = patch('coordinator.host_os.trash_directory', return_value=self.trash)
        trash.start()
        self.addCleanup(trash.stop)

    def test_open_trim_moves_stale_unused_scratch_to_the_trash(self):
        self.trash_here()
        task = self.job(closed=False)
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        for name in ('old', 'cited', 'opened', 'KEEP'):
            (scratch / name).mkdir()
            (scratch / name / 'data.bin').write_bytes(b'x' * 1000)
        (scratch / 'HANDOFF.md').write_text('handoff')
        repo = scratch / 'repo'
        repo.mkdir()
        self.git('init', '-q', cwd=repo)
        (repo / 'unsaved.txt').write_text('uncommitted work')
        self.store.task_note(task['id'], 'Proof is in scratch/task-%d/cited/data.bin' % task['id'])
        opened = [str(scratch / 'opened' / 'data.bin')]
        with patch.object(job_limits, 'open_paths', return_value=opened):
            self.assertEqual(job_limits.trim_open(self.store, time.time() + 3600), 0)
            self.assertTrue((scratch / 'old').exists())
            self.assertGreater(job_limits.trim_open(self.store, time.time() + 49 * 3600), 0)
        self.assertFalse((scratch / 'old').exists())
        for name in ('cited', 'opened', 'KEEP', 'HANDOFF.md', 'repo/unsaved.txt'):
            self.assertTrue((scratch / name).exists(), name)
        [folder] = self.trash.glob('torii-trim-task-%d-scratch-*' % task['id'])
        self.assertTrue((folder / 'old' / 'data.bin').is_file())
        self.assertIn(str(folder), self.store.task_get(task['id'])['notes'])

    def test_open_trim_failure_skips_one_job_and_backs_off(self):
        self.trash_here()
        tasks = [self.job(closed=False) for _ in range(2)]
        stale = [close_saves.scratch(self.store, task['id'], create=True) / 'old.bin' for task in tasks]
        for path in stale:
            path.write_bytes(b'x')
        real = job_limits.move_out

        def move_out(store, task, *args):
            if task['id'] == tasks[0]['id']:
                raise PermissionError('fixture move failure')
            return real(store, task, *args)

        now = time.time() + 49 * 3600
        with patch.object(job_limits, 'open_paths', return_value=[]), \
                patch.object(job_limits, 'move_out', move_out), \
                self.assertLogs('coordinator.problems', 'WARNING'):
            self.assertGreater(job_limits.trim_open(self.store, now), 0)
        self.assertEqual([path.exists() for path in stale], [True, False])
        [detail] = [row[0] for row in self.store.db.execute(
            "SELECT detail FROM problems WHERE code='open-trim-failed' AND task=?", (tasks[0]['id'],))]
        self.assertRegex(detail, r'^item=%d/scratch failures=1 PermissionError: fixture move failure at '
                                 r'test_job_limits\.py:\d+ move_out < job_limits\.py:\d+ trim_folder' % tasks[0]['id'])
        with patch.object(job_limits, 'open_paths', return_value=[]):
            self.assertEqual(job_limits.trim_open(self.store, now), 0)
            self.assertTrue(stale[0].exists())
            with patch('coordinator.sweeps.time.time', return_value=time.time() + 120):
                self.assertGreater(job_limits.trim_open(self.store, now), 0)
        self.assertFalse(stale[0].exists())
        self.assertIsNone(self.store.db.execute('SELECT 1 FROM sweep_failures').fetchone())

    def test_size_failure_skips_one_job_and_records_its_cause(self):
        tasks = [self.job(closed=False) for _ in range(2)]
        measured = []

        def usage(label, root):
            if 'task-%d' % tasks[0]['id'] in str(root):
                raise OSError('fixture size failure')
            measured.append(root)
            return 0, None, {}

        with patch.object(job_limits, 'usage', usage), \
                patch.object(job_limits, 'bounded_scan', side_effect=lambda store, function, *args: function(*args)), \
                self.assertLogs('coordinator.problems', 'WARNING'):
            job_limits.sweep(self.state)
        self.assertTrue(measured and all('task-%d' % tasks[1]['id'] in str(root) for root in measured))
        [detail] = [row[0] for row in self.store.db.execute(
            "SELECT detail FROM problems WHERE code='job-size-failed' AND task=?", (tasks[0]['id'],))]
        self.assertRegex(detail, r'^item=%d failures=1 OSError: fixture size failure at test_job_limits\.py:\d+ usage'
                         % tasks[0]['id'])

    def test_park_check_failure_skips_one_job(self):
        tasks = [self.job(closed=False) for _ in range(2)]
        for task in tasks:
            close_saves.scratch(self.store, task['id'], create=True)
        with self.store.db:
            self.store.db.execute('UPDATE tasks SET updated=0')
        real = close_saves.kept

        def kept(store, task_id):
            if task_id == tasks[0]['id']:
                raise ValueError('fixture keep failure')
            return real(store, task_id)

        with patch.object(close_saves, 'kept', kept), \
                patch.object(job_limits, 'path_users', return_value={}), \
                self.assertLogs('coordinator.problems', 'WARNING'):
            self.assertEqual(close_saves.park_idle(self.store), [tasks[1]['id']])
        self.assertRegex(self.store.db.execute("SELECT detail FROM problems WHERE code='park-failed'").fetchone()[0],
                         r'^item=%d failures=1 ValueError: fixture keep failure' % tasks[0]['id'])

    def test_open_trim_skips_parked_saving_and_closed_jobs(self):
        self.trash_here()
        task = self.job(closed=False)
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        (scratch / 'old.bin').write_bytes(b'x')
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET copy='saving' WHERE id=?", (task['id'],))
        with patch.object(job_limits, 'open_paths', return_value=[]):
            self.assertEqual(job_limits.trim_open(self.store, time.time() + 49 * 3600), 0)
            with self.store.db:
                self.store.db.execute("UPDATE tasks SET copy='present',status='done' WHERE id=?", (task['id'],))
            self.assertEqual(job_limits.trim_open(self.store, time.time() + 49 * 3600), 0)
        self.assertTrue((scratch / 'old.bin').exists())

    def test_sweep_keeps_systemd_script_when_job_is_too_recent_to_park(self):
        self.trash_here()
        task = self.job(closed=False)
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        script = scratch / 'watchdog' / 'run.sh'
        script.parent.mkdir()
        script.write_text('echo ok\n')
        now = time.time() + 49 * 3600
        with self.store.db:
            self.store.db.execute('''INSERT INTO workers(task,topic,provider,prompt,status,created,updated)
                VALUES (?,?,'claude','work','done',?,?)''', (task['id'], fixtures.TOPIC, now - 60, now - 60))
        unit = ('watchdog.service', 'ExecStart=/bin/sh %s\n' % script)
        with patch.object(job_limits, 'low_disk', return_value=False), \
                patch.object(job_limits, 'open_paths', return_value=[]), \
                patch.object(job_limits, 'systemd_references', return_value=[unit]):
            job_limits.sweep(self.state, now)
        self.assertFalse(close_saves.parked(self.store, self.store.task_get(task['id'])))
        self.assertTrue(script.is_file())

    def test_open_trim_keeps_scratch_on_unit_scan_failure_and_reports_once(self):
        self.trash_here()
        task = self.job(closed=False)
        stale = close_saves.scratch(self.store, task['id'], create=True) / 'old.bin'
        stale.write_bytes(b'x')
        now = time.time() + 49 * 3600
        with patch.object(job_limits, 'systemd_references', side_effect=OSError('scan failed')):
            for _ in range(2):
                self.assertEqual(job_limits.trim_open(self.store, now), 0)
        self.assertTrue(stale.is_file())
        rows = self.store.db.execute("SELECT detail FROM problems WHERE area='cleanup' AND code='path-use-scan-failed'")
        self.assertEqual([row['detail'] for row in rows], ['systemd path use scan failed (OSError)'])

    def test_open_trim_live_worker_reference_keeps_files(self):
        self.trash_here()
        task = self.job(closed=False)
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        (scratch / 'input').mkdir()
        (scratch / 'input' / 'model.bin').write_bytes(b'x')
        with self.store.db:
            self.store.db.execute('''INSERT INTO workers(task,topic,provider,prompt,status,created,updated)
                VALUES (?,?,'claude',?,'running',1000,1000)''',
                (task['id'], fixtures.TOPIC, 'Read scratch/task-%d/input/model.bin' % task['id']))
        with patch.object(job_limits, 'open_paths', return_value=[]):
            self.assertEqual(job_limits.trim_open(self.store, time.time() + 49 * 3600), 0)
        self.assertTrue((scratch / 'input' / 'model.bin').exists())

    def test_r1_active_workers_keep_stale_scratch(self):
        self.trash_here()
        task = self.job(closed=False)
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        (scratch / 'dataset').mkdir()
        data = scratch / 'dataset' / 'train.bin'
        data.write_bytes(b'x' * 1000)
        self.assert_active_worker_keeps_entry(task, data, time.time() + 49 * 3600)

    def test_running_worker_keeps_input_its_script_reads_through_the_job_folder(self):
        self.trash_here()
        task = self.job(closed=False)
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        (scratch / 'inputs').mkdir()
        (scratch / 'inputs' / 'corpus.bin').write_bytes(b'x' * 1000)
        (scratch / 'train.py').write_text("from pathlib import Path\nROOT = Path('%s')\n"
                                          "DATA = ROOT / 'inputs' / 'corpus.bin'\n" % scratch)
        now = time.time() + 49 * 3600
        with self.store.db:
            worker = self.store.db.execute("""INSERT INTO workers(task,topic,provider,prompt,status,created,updated)
                VALUES (?,?,'claude',?,'running',1000,1000)""",
                (task['id'], fixtures.TOPIC, 'Job scratch: %s. Run train.py again.' % scratch)).lastrowid
        with patch.object(job_limits, 'open_paths', return_value=[]):
            job_limits.trim_open(self.store, now)
            self.assertTrue((scratch / 'inputs' / 'corpus.bin').is_file())
            with self.store.db:
                self.store.db.execute("UPDATE workers SET status='done' WHERE id=?", (worker,))
            self.assertEqual(job_limits.trim_open(self.store, now), 0)
        self.assertTrue((scratch / 'inputs' / 'corpus.bin').is_file())

    def test_move_out_keeps_an_entry_written_inside_after_the_plan(self):
        self.trash_here()
        task = self.job(closed=False)
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        for name in ('written', 'quiet'):
            (scratch / name / 'deep').mkdir(parents=True)
            (scratch / name / 'deep' / 'data.bin').write_bytes(b'x' * 1000)
        now = time.time() + 49 * 3600
        chosen = job_limits.trim_plan(scratch, {}, [], now, *job_limits.folder_limits('scratch'), job_limits.CAP_AGE)
        self.assertEqual(sorted(Path(entry[0]).name for entry in chosen), ['quiet', 'written'])
        _, stamp = close_saves.scan_task_references(self.store, task['id'], {})
        os.utime(scratch / 'written' / 'deep' / 'data.bin', (now, now))
        self.assertGreater(job_limits.move_out(self.store, task, scratch, chosen, stamp), 0)
        self.assertTrue((scratch / 'written' / 'deep' / 'data.bin').is_file())
        self.assertFalse((scratch / 'quiet').exists())

    def test_move_out_keeps_entries_that_vanish_or_sit_on_another_file_system(self):
        self.trash_here()
        task = self.job(closed=False)
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        for name in ('gone', 'mounted', 'quiet'):
            (scratch / name).mkdir()
            (scratch / name / 'data.bin').write_bytes(b'x' * 1000)
        now = time.time() + 49 * 3600
        chosen = job_limits.trim_plan(scratch, {}, [], now, *job_limits.folder_limits('scratch'), job_limits.CAP_AGE)
        _, stamp = close_saves.scan_task_references(self.store, task['id'], {})
        shutil.rmtree(scratch / 'gone')
        device = (scratch / 'mounted').lstat().st_dev
        lstat = Path.lstat

        def other_device(path):
            info = lstat(path)
            if path.name != 'mounted':
                return info
            return os.stat_result((*info[:2], device + 1, *info[3:]))
        with patch.object(Path, 'lstat', other_device):
            self.assertGreater(job_limits.move_out(self.store, task, scratch, chosen, stamp), 0)
        self.assertTrue((scratch / 'mounted' / 'data.bin').is_file())
        self.assertFalse((scratch / 'quiet').exists())

    def test_r2_active_workers_keep_results_over_cap(self):
        self.trash_here()
        task = self.job(closed=False)
        results = self.state / 'results' / ('task-' + str(task['id']))
        (results / 'checkpoints').mkdir(parents=True)
        checkpoint = results / 'checkpoints' / 'step-1000.pt'
        checkpoint.write_bytes(b'x' * 200000)
        with patch.object(job_limits, 'RESULTS_CAP', 100000):
            self.assert_active_worker_keeps_entry(task, checkpoint, time.time() + 13 * 3600)

    def assert_active_worker_keeps_entry(self, task, path, now):
        with self.store.db:
            worker = self.store.db.execute('''INSERT INTO workers(task,topic,provider,prompt,status,created,updated)
                VALUES (?,?,'claude','Continue the job.','running',1000,1000)''', (task['id'], fixtures.TOPIC)).lastrowid
        with patch.object(job_limits, 'open_paths', return_value=[]):
            for status in ('running', 'queued', 'waiting_for_secret', 'waiting_for_quota'):
                with self.subTest(status=status):
                    with self.store.db:
                        self.store.db.execute('UPDATE workers SET status=? WHERE id=?', (status, worker))
                    self.assertEqual(job_limits.trim_open(self.store, now), 0)
                    self.assertTrue(path.exists())
            with self.store.db:
                self.store.db.execute("UPDATE workers SET status='done' WHERE id=?", (worker,))
            self.assertGreater(job_limits.trim_open(self.store, now), 0)
        self.assertFalse(path.exists())

    def test_open_trim_waits_for_scratch_guard(self):
        self.trash_here()
        task = self.job(closed=False)
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        data = scratch / 'old.bin'
        data.write_bytes(b'x' * 1000)
        now = time.time() + 49 * 3600
        with patch.object(job_limits, 'open_paths', return_value=[]):
            with idle_trim.scratch_guard(self.store, task['id']):
                self.assertEqual(job_limits.trim_open(self.store, now), 0)
                self.assertTrue(data.exists())
            self.assertGreater(job_limits.trim_open(self.store, now), 0)
        self.assertFalse(data.exists())

    def test_open_paths_lists_open_files(self):
        task = self.job(closed=False)
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        path = scratch / 'held.bin'
        with open(path, 'wb'):
            self.assertIn(str(path.resolve()), job_limits.open_paths(self.state.resolve()))

    def test_proc_paths_rechecks_owner_after_a_permission_error(self):
        process = self.root / 'proc-fixture'
        fd = process / 'fd'
        fd.mkdir(parents=True)
        (fd / '0').touch()
        (process / 'cmdline').write_bytes(b'/bin/true\0')
        original_stat = Path.stat

        def readlink(path):
            if path == fd / '0':
                raise PermissionError(path)
            raise FileNotFoundError(path)

        def probe(next_owner):
            owners = iter((os.getuid(), next_owner))

            def stat(path, **kwargs):
                if path == fd:
                    return SimpleNamespace(st_uid=next(owners))
                return original_stat(path, **kwargs)

            with patch.object(Path, 'stat', autospec=True, side_effect=stat), patch.object(os, 'readlink', side_effect=readlink):
                return list(host_os.proc_paths(process))

        self.assertEqual(probe(os.getuid() + 1), ['/bin/true'])
        with self.assertRaises(PermissionError):
            probe(os.getuid())

    def test_systemd_keeps_active_or_triggered_units_and_ignores_stale_transients(self):
        transient = self.root / 'transient'
        transient.mkdir()
        for name in ('failed-transient.service', 'unloaded-transient.service', 'running-transient.service'):
            (transient / name).write_text('[Service]\nExecStart=/bin/sh /scratch/watchdog.sh\n')

        def unit(name, active, substate, **fields):
            state = dict(Id=name, Names=name, LoadState='loaded', ActiveState=active, SubState=substate, **fields)
            return '\n'.join('%s=%s' % (key, value) for key, value in state.items())

        def show(systemctl, operation, *args):
            if operation == 'list-unit-files':
                return 'enabled.socket enabled enabled\n'
            return '\n\n'.join((
                unit('failed.service', 'failed', 'failed', ExecStart='/scratch/failed.sh'),
                unit('inactive.service', 'inactive', 'dead', ExecStart='/scratch/inactive.sh'),
                unit('failed-transient.service', 'failed', 'failed', Transient='yes',
                     FragmentPath=transient / 'failed-transient.service'),
                unit('running-transient.service', 'active', 'running', Transient='yes',
                     FragmentPath=transient / 'running-transient.service'),
                unit('active.timer', 'active', 'waiting', Triggers='triggered.service'),
                unit('triggered.service', 'failed', 'failed', ExecStart='/scratch/triggered.sh'),
                unit('watch.path', 'active', 'waiting', Triggers='watch.service failed-transient.service'),
                unit('watch.service', 'inactive', 'dead', WorkingDirectory='/scratch/watch'),
                unit('enabled.socket', 'inactive', 'dead', Triggers='socket.service'),
                unit('socket.service', 'inactive', 'dead', ExecStart='/scratch/socket.sh'),
                unit('disabled.timer', 'inactive', 'dead', Triggers='inactive.service'),
            ))

        with patch.object(host_os, 'systemctl_output', side_effect=show):
            refs = dict(host_os.systemd_live_references('systemctl', (self.root,), transient))
        self.assertEqual(set(refs), {'active.timer', 'triggered.service', 'enabled.socket', 'socket.service',
                                     'watch.path', 'watch.service', 'running-transient.service'})
        self.assertIn('ExecStart=/bin/sh /scratch/watchdog.sh', refs['running-transient.service'])
        self.assertIn('/scratch/triggered.sh', refs['triggered.service'])


    @unittest.skipUnless(host_os.linux(), 'uses Linux procfs')
    def test_open_paths_finds_a_relative_script_argument_under_scratch(self):
        task = self.job(closed=False)
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        script = scratch / 'watch dog.py'
        script.write_text('import time\ntime.sleep(60)\n')
        with subprocess.Popen([sys.executable, str(script.relative_to(self.state))], cwd=self.state) as process:
            try:
                paths = job_limits.open_paths(self.state, owners=True)
                self.assertEqual(paths[str(script)], str(process.pid))
            finally:
                process.terminate()

    def test_scratch_cap_trims_oldest_first(self):
        self.trash_here()
        task = self.job(closed=False)
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        for name in ('first', 'second', 'third'):
            (scratch / name).write_bytes(b'x' * 100000)
            time.sleep(0.02)
        size = job_limits.usage('scratch', scratch)[0]
        with patch.object(job_limits, 'open_paths', return_value=[]), \
                patch.object(job_limits, 'SCRATCH_STALE', 10 ** 9), patch.object(job_limits, 'SCRATCH_CAP', size - 50000):
            self.assertEqual(job_limits.trim_open(self.store, time.time() + 3600), 0)
            self.assertGreater(job_limits.trim_open(self.store, time.time() + 13 * 3600), 0)
        self.assertEqual(sorted(path.name for path in scratch.iterdir()), ['second', 'third'])

    def test_r3_scratch_cap_excludes_git_and_keep_referenced_entries(self):
        self.trash_here()
        task = self.job(closed=False)
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        repo = scratch / 'repo'
        repo.mkdir()
        self.git('init', '-q', cwd=repo)
        (repo / 'big.bin').write_bytes(b'x' * 400000)
        cited = scratch / 'cited.bin'
        cited.write_bytes(b'x' * 400000)
        keep = self.project / 'KEEP'
        keep.mkdir()
        (keep / 'MANIFEST.md').write_text('Proof: scratch/task-%d/cited.bin\n' % task['id'])
        notes = scratch / 'notes.txt'
        notes.write_text('small notes')
        with patch.object(job_limits, 'open_paths', return_value=[]), patch.object(job_limits, 'SCRATCH_CAP', 100000):
            self.assertEqual(job_limits.trim_open(self.store, time.time() + 13 * 3600), 0)
        self.assertTrue(notes.exists())
        self.assertTrue(cited.exists())
        self.assertTrue((repo / 'big.bin').exists())

    def test_r4_results_cap_excludes_rounds_and_reports(self):
        self.trash_here()
        task = self.job(closed=False)
        results = self.state / 'results' / ('task-' + str(task['id']))
        (results / 'rounds' / '0').mkdir(parents=True)
        saved = results / 'rounds' / '0' / 'ignored.tar'
        saved.write_bytes(b'x' * 300000)
        report = results / 'report.md'
        report.write_text('report\n' * 30000)
        proof = results / 'proof.png'
        proof.write_bytes(b'x' * 1000)
        with patch.object(job_limits, 'open_paths', return_value=[]), patch.object(job_limits, 'RESULTS_CAP', 100000):
            self.assertEqual(job_limits.trim_open(self.store, time.time() + 13 * 3600), 0)
        self.assertTrue(proof.exists())
        self.assertTrue(saved.exists())
        self.assertTrue(report.exists())

    def test_r5_nested_reports_and_records_stay(self):
        self.trash_here()
        task = self.job(closed=False)
        results = self.state / 'results' / ('task-' + str(task['id']))
        names = ('report.md', 'HANDOFF.md', 'recipe.md', *sorted(close_saves.RECORDS - {'recipe.md'}))
        for name in names:
            folder = results / (name + '-files')
            folder.mkdir(parents=True)
            (folder / name).write_text('record')
            (folder / 'frames.bin').write_bytes(b'x' * 200000)
        disposable = results / 'disposable.bin'
        disposable.write_bytes(b'x' * 200000)
        with patch.object(job_limits, 'open_paths', return_value=[]), patch.object(job_limits, 'RESULTS_CAP', 100000):
            self.assertGreater(job_limits.trim_open(self.store, time.time() + 13 * 3600), 0)
        self.assertFalse(disposable.exists())
        for name in names:
            self.assertTrue((results / (name + '-files') / name).exists(), name)

    def test_results_over_cap_trim_keeps_records_rounds_and_bundles(self):
        self.trash_here()
        task = self.job(closed=False)
        results = self.state / 'results' / ('task-' + str(task['id']))
        (results / 'rounds' / '0').mkdir(parents=True)
        (results / 'rounds' / '0' / 'saved.json').write_text('{}')
        (results / 'big').mkdir()
        (results / 'big' / 'frames.bin').write_bytes(b'x' * 200000)
        (results / 'bundles').mkdir()
        self.git('bundle', 'create', results / 'bundles' / 'work.bundle', 'HEAD')
        for name in ('HANDOFF.md', 'report.md', 'recipe.md'):
            (results / name).write_text(name)
        (results / 'later.bin').write_bytes(b'x' * 1000)
        with patch.object(job_limits, 'open_paths', return_value=[]), patch.object(job_limits, 'RESULTS_CAP', 100000):
            self.assertGreater(job_limits.trim_open(self.store, time.time() + 13 * 3600), 0)
        self.assertFalse((results / 'big').exists())
        for name in ('rounds/0/saved.json', 'bundles/work.bundle', 'HANDOFF.md', 'report.md', 'recipe.md', 'later.bin'):
            self.assertTrue((results / name).exists(), name)

    def test_low_disk_parks_sooner_and_expires_only_old_torii_trash(self):
        self.trash_here()
        task = self.job(closed=False)
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        (scratch / 'notes.txt').write_text('notes')
        with self.store.db:
            self.store.db.execute('UPDATE tasks SET updated=? WHERE id=?', (time.time() - 3 * 3600, task['id']))
        self.trash.mkdir(exist_ok=True)
        (self.trash / 'torii-trim-task-1-scratch-old').mkdir()
        (self.trash / 'torii-trim-task-1-scratch-old' / 'a.bin').write_bytes(b'x')
        (self.trash / 'owner-photos').mkdir()
        with patch.object(job_limits, 'low_disk', return_value=False):
            job_limits.sweep(self.state)
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'present')
        with patch.object(job_limits, 'low_disk', return_value=True):
            job_limits.sweep(self.state)
        self.assertTrue(close_saves.parked(self.store, self.store.task_get(task['id'])))
        self.assertTrue((self.trash / 'torii-trim-task-1-scratch-old').exists())
        with patch.object(job_limits, 'low_disk', return_value=True):
            job_limits.sweep(self.state, now=time.time() + 2 * 86400)
        self.assertFalse((self.trash / 'torii-trim-task-1-scratch-old').exists())
        self.assertTrue((self.trash / 'owner-photos').exists())
        self.assertTrue(self.store.db.execute("SELECT 1 FROM problems WHERE code='low-disk'").fetchone())

    def test_r6_trash_expiry_keeps_owner_items_and_removes_matching_metadata(self):
        trash = self.trash / 'files'
        metadata = self.trash / 'info'
        trash.mkdir(parents=True)
        metadata.mkdir()
        kept = ('torii-dev-backup', 'torii-settings.json')
        expired = ('torii-trim-task-1-scratch-abcde123', 'torii-orphan-task-1-' + 'a' * 65)
        (trash / kept[0]).mkdir()
        (trash / kept[0] / 'notes.md').write_text('owner notes')
        (trash / kept[1]).write_text('{}')
        for name in expired:
            (trash / name).mkdir()
            (trash / name / 'data.bin').write_bytes(b'x' * 1000)
        for name in (*kept, *expired):
            (metadata / (name + '.trashinfo')).write_text('[Trash Info]\n')
        now = time.time()
        self.assertEqual(host_os.expire_trash(now + 3600, trash), 0)
        self.assertEqual(len(list(metadata.iterdir())), 4)
        self.assertGreater(host_os.expire_trash(now + 2 * 86400, trash), 0)
        self.assertEqual(sorted(path.name for path in trash.iterdir()), sorted(kept))
        self.assertEqual(sorted(path.name for path in metadata.iterdir()), sorted(name + '.trashinfo' for name in kept))


if __name__ == '__main__':
    unittest.main()
