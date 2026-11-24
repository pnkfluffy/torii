import time
import fcntl
import os
from pathlib import Path
import sqlite3
from unittest.mock import patch
import unittest

from coordinator import close_saves, control_api, idle_trim, scan_process
from tests import test_worktree_cleanup as fixtures


class IdleTrimTests(unittest.TestCase):
    setUp = fixtures.WorktreeCleanupTests.setUp
    tearDown = fixtures.WorktreeCleanupTests.tearDown
    git = fixtures.WorktreeCleanupTests.git
    call = fixtures.WorktreeCleanupTests.call
    target = fixtures.WorktreeCleanupTests.target
    worker = fixtures.WorktreeCleanupTests.worker
    job = fixtures.WorktreeCleanupTests.job

    def age(self, task):
        with self.store.db:
            self.store.db.execute('UPDATE tasks SET updated=? WHERE id=?', (time.time() - 90000, task['id']))

    def sweep(self):
        with patch('coordinator.close_saves.Path.home', return_value=self.root):
            idle_trim.sweep(self.state)

    def test_off_and_recent_and_live_workers(self):
        task = self.job(closed=False)
        self.age(task)
        scratch = self.state / 'scratch' / ('task-' + str(task['id']))
        scratch.mkdir(parents=True, exist_ok=True)
        (scratch / 'file').write_text('keep')
        self.sweep()
        self.assertTrue((scratch / 'file').exists())
        self.call('cleanup.idle_trim', enabled=True)
        worker = self.worker(task['id'], self.target(task))
        for status in ('queued', 'running', 'waiting_for_secret', 'waiting_for_quota'):
            with self.store.db:
                self.store.db.execute('UPDATE workers SET status=? WHERE id=?', (status, worker))
            self.sweep()
            self.assertTrue((scratch / 'file').exists())
        with self.store.db:
            self.store.db.execute("UPDATE workers SET status='done', updated=? WHERE task=?", (time.time(), task['id']))
        self.sweep()
        self.assertTrue((scratch / 'file').exists())

    def test_waiting_job_and_scratch_references(self):
        task = self.job(closed=False)
        other = self.job(closed=False)
        scratch = self.state / 'scratch' / ('task-' + str(task['id']))
        scratch.mkdir(parents=True, exist_ok=True)
        (scratch / 'referenced').write_text('keep')
        (scratch / 'discard').write_text('discard')
        (scratch / 'landing-checkpoint.md').write_text('live landing state')
        (scratch / '.cache').mkdir()
        (scratch / '.cache' / 'stale').write_text('discard')
        (scratch / 'rounds').mkdir()
        (scratch / 'nested' / 'KEEP').mkdir(parents=True)
        (scratch / 'nested' / 'KEEP' / 'file').write_text('keep')
        chat = self.state / 'scratch' / 'topic--1'
        chat.mkdir()
        (chat / 'file').write_text('chat')
        self.call('tasks.update', task=other['id'], note=str(scratch / 'referenced'))
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET status='waiting',updated=? WHERE id=?", (time.time() - 90000, task['id']))
            self.store.db.execute("UPDATE tasks SET copy='present' WHERE id=?", (task['id'],))
        outbox = self.store.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0]
        self.call('cleanup.idle_trim', enabled=True)
        self.sweep()
        self.assertTrue((scratch / 'referenced').exists())
        self.assertTrue((scratch / 'discard').exists())
        self.assertTrue((scratch / 'landing-checkpoint.md').exists())
        self.assertFalse((scratch / '.cache').exists())
        self.assertTrue((scratch / 'rounds').exists())
        self.assertTrue((scratch / 'nested' / 'KEEP' / 'file').exists())
        self.assertTrue((chat / 'file').exists())
        self.assertIn('Idle trim at ', self.store.task_get(task['id'])['notes'])
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'present')
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], outbox)

    def test_armed_wait_prompt_protects_scratch_reference(self):
        task = self.job(closed=False)
        scratch = self.state / 'scratch' / ('task-' + str(task['id']))
        scratch.mkdir(parents=True, exist_ok=True)
        checkpoint = scratch / 'checkpoint.md'
        checkpoint.write_text('landing state')
        cache = scratch / '.cache'
        cache.mkdir()
        (cache / 'checkpoint').write_text('referenced cache')
        self.call('tasks.wait', task=task['id'], kind='time', ask='Continue later', at='2030-01-01T00:00:00Z',
                  then_provider='codex', then_prompt=str(checkpoint) + ' ' + str(cache / 'checkpoint'))
        self.age(task)
        self.call('cleanup.idle_trim', enabled=True)
        self.sweep()
        self.assertTrue(checkpoint.exists())
        self.assertTrue((cache / 'checkpoint').exists())

    def test_armed_wait_prevents_trim(self):
        task = self.job(closed=False)
        scratch = self.state / 'scratch' / ('task-' + str(task['id']))
        (scratch / '.cache').mkdir(parents=True)
        self.call('tasks.wait', task=task['id'], kind='time', ask='Continue later', at='2030-01-01T00:00:00Z')
        self.age(task)
        self.call('cleanup.idle_trim', enabled=True)
        self.sweep()
        self.assertTrue((scratch / '.cache').exists())
        self.assertIsNone(self.store.db.execute('SELECT 1 FROM idle_trims WHERE task=?', (task['id'],)).fetchone())

    def test_wait_armed_during_scan_prevents_trim(self):
        task = self.job(closed=False)
        scratch = self.state / 'scratch' / ('task-' + str(task['id']))
        (scratch / '.cache').mkdir(parents=True)
        self.age(task)
        self.call('cleanup.idle_trim', enabled=True)
        original = idle_trim.scan_candidates
        def scan(*args):
            self.call('tasks.wait', task=task['id'], kind='time', ask='Continue later', at='2030-01-01T00:00:00Z')
            self.age(task)
            return original(*args)
        with patch('coordinator.idle_trim.scan_candidates', side_effect=scan), \
                patch.object(idle_trim, 'bounded_scan', side_effect=lambda store, function, *args: function(*args)):
            self.sweep()
        self.assertTrue((scratch / '.cache').exists())
        self.assertIsNone(self.store.db.execute('SELECT 1 FROM idle_trims WHERE task=?', (task['id'],)).fetchone())

    def test_nested_protected_folder_added_after_scan_is_restored(self):
        task = self.job(closed=False)
        scratch = self.state / 'scratch' / ('task-' + str(task['id']))
        nested = scratch / '.cache' / 'nested'
        nested.mkdir(parents=True)
        self.age(task)
        self.call('cleanup.idle_trim', enabled=True)
        original = close_saves.scan_task_references
        called = []
        def add_keep(*args, **kwargs):
            result = original(*args, **kwargs)
            if not called:
                (nested / 'KEEP').mkdir()
                called.append(True)
            return result
        with patch.object(close_saves, 'scan_task_references', side_effect=add_keep):
            self.sweep()
        self.assertTrue(called)
        self.assertTrue((nested / 'KEEP').is_dir())

    def test_reference_decision_runs_without_writer_and_failure_preserves_scratch(self):
        task = self.job(closed=False)
        scratch = self.state / 'scratch' / ('task-' + str(task['id']))
        cache = scratch / '.cache'
        cache.mkdir(parents=True)
        (cache / 'file').write_text('keep')
        self.age(task)
        self.call('cleanup.idle_trim', enabled=True)
        run = scan_process.run
        calls = []
        def checking(function, *args, **kwargs):
            if function is idle_trim.scratch_referenced:
                with sqlite3.connect(str(self.state / 'state.sqlite'), timeout=0) as db:
                    db.execute('BEGIN IMMEDIATE')
                calls.append(True)
                if len(calls) == 2:
                    raise scan_process.ScanError('scan timed out')
            return run(function, *args, **kwargs)
        with patch.object(scan_process, 'run', side_effect=checking), \
                self.assertLogs('coordinator.problems', level='WARNING'):
            self.sweep()
        self.assertEqual(len(calls), 2)
        [detail] = [row[0] for row in self.store.db.execute(
            "SELECT detail FROM problems WHERE code='idle-trim-failed' AND task=?", (task['id'],))]
        self.assertRegex(detail, r'^item=%d failures=1 ScanError: cleanup scan failed at idle_trim\.py:\d+ bounded_scan < idle_trim\.py:\d+ run_job' % task['id'])
        idle_trim.spawn_guard(self.store, task['id'])
        self.assertEqual((cache / 'file').read_text(), 'keep')

    def test_scan_error_survives_problem_reporting_failure(self):
        with patch.object(scan_process, 'run', side_effect=TimeoutError('scan timed out')), \
                patch.object(idle_trim.problems, 'record', side_effect=RuntimeError('report failed')), \
                self.assertLogs('coordinator.idle_trim', level='WARNING'):
            with self.assertRaises(scan_process.ScanError) as raised:
                idle_trim.bounded_scan(self.store, idle_trim.scratch_referenced, None, None, {})
        self.assertIsInstance(raised.exception.__cause__, TimeoutError)
        with patch.object(scan_process, 'MAX_REFERENCE_COUNT', 0), \
                patch.object(idle_trim.problems, 'record', side_effect=RuntimeError('report failed')), \
                self.assertLogs('coordinator.idle_trim', level='WARNING'):
            with self.assertRaises(scan_process.ScanError):
                idle_trim.bounded_reference_values(self.store, {'1': {'scratch/task-1/a'}}, 1)

    def test_reference_input_limit_preserves_scratch_and_records_problem(self):
        task = self.job(closed=False)
        scratch = self.state / 'scratch' / ('task-' + str(task['id']))
        cache = scratch / '.cache'
        cache.mkdir(parents=True)
        (cache / 'file').write_text('keep')
        self.age(task)
        self.call('cleanup.idle_trim', enabled=True)
        refs = {str(task['id']): {'scratch/task-%s/a' % task['id'], 'scratch/task-%s/b' % task['id']}}
        with patch.object(close_saves, 'scan_references', return_value=refs), \
                patch.object(scan_process, 'MAX_REFERENCE_COUNT', 1):
            self.sweep()
        self.assertEqual((cache / 'file').read_text(), 'keep')
        self.assertIsNotNone(self.store.db.execute(
            "SELECT 1 FROM problems WHERE area='cleanup' AND code='scan-failed'").fetchone())

    def test_no_worktree_and_held_copy(self):
        task = self.call('tasks.create', title='No worktree').data
        scratch = self.state / 'scratch' / ('task-' + str(task['id']))
        (scratch / '.cache').mkdir(parents=True)
        (scratch / '.cache' / 'file').write_text('cache')
        self.age(task)
        self.call('cleanup.idle_trim', enabled=True)
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET copy='held' WHERE id=?", (task['id'],))
        self.sweep()
        self.assertTrue((scratch / '.cache').exists())
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET copy='saving' WHERE id=?", (task['id'],))
        self.sweep()
        self.assertTrue((scratch / '.cache').exists())
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET copy='cleared' WHERE id=?", (task['id'],))
        before = self.store.task_get(task['id'])['updated']
        self.sweep()
        self.assertFalse((scratch / '.cache').exists())
        self.assertEqual(self.store.task_get(task['id'])['updated'], before)

    def test_nested_git_clone_is_not_trimmed(self):
        task = self.job(closed=False)
        root = self.target(task)
        (root / '.gitignore').write_text('dist/\n')
        clone = root / 'dist' / 'clone'
        (clone / '.git').mkdir(parents=True)
        (clone / 'file').write_text('keep')
        self.age(task)
        self.call('cleanup.idle_trim', enabled=True)
        self.sweep()
        self.assertTrue((clone / 'file').exists())

    def test_large_script_is_skipped_in_strict_scan(self):
        root = self.root / 'scripts'
        root.mkdir()
        large = root / 'large.sh'
        large.write_bytes(b'x' * (1024 * 1024 + 1))
        self.assertEqual(close_saves.result_script_references(root, 1024 * 1024, strict=True), [])

    def test_reference_scan_runs_once_before_any_trim_marker(self):
        first = self.job(closed=False)
        second = self.job(closed=False)
        self.age(first)
        self.age(second)
        self.call('cleanup.idle_trim', enabled=True)
        original = close_saves.scan_references
        def scan(*args, **kwargs):
            self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM idle_trims WHERE active=1').fetchone()[0], 0)
            return original(*args, **kwargs)
        with patch('coordinator.idle_trim.close_saves.scan_references', side_effect=scan) as refs:
            self.sweep()
        self.assertEqual(refs.call_count, 1)

    def test_ignored_dependencies_and_donor(self):
        task = self.job(closed=False)
        root = self.target(task)
        (root / '.gitignore').write_text('node_modules/\ndist/\n')
        (root / 'README.md').write_text('uncommitted')
        (root / 'untracked').write_text('uncommitted')
        for name in ('node_modules', 'dist'):
            (root / name).mkdir()
            (root / name / 'file').write_text(name)
        (self.project / 'donor').symlink_to(root / 'node_modules', target_is_directory=True)
        self.age(task)
        self.call('cleanup.idle_trim', enabled=True)
        self.sweep()
        self.assertTrue((root / 'node_modules' / 'file').exists())
        self.assertFalse((root / 'dist').exists())
        self.assertEqual((root / 'README.md').read_text(), 'uncommitted')
        self.assertEqual((root / 'untracked').read_text(), 'uncommitted')

    def counted_donor_scans(self, error=None):
        calls = []

        def scan(store, function, *args):
            if function is idle_trim.donor_links:
                calls.append(args)
                if error:
                    raise scan_process.ScanError('scan timed out')
            return function(*args)
        return calls, patch.object(idle_trim, 'bounded_scan', side_effect=scan)

    def test_donor_scan_is_skipped_without_dependency_folders(self):
        tasks = [self.job(closed=False) for _ in range(2)]
        for task in tasks:
            (self.state / 'scratch' / ('task-' + str(task['id'])) / '.cache').mkdir(parents=True)
            self.age(task)
        self.call('cleanup.idle_trim', enabled=True)
        calls, scans = self.counted_donor_scans()
        with scans:
            self.sweep()
        self.assertEqual(calls, [])
        self.assertFalse(any((self.state / 'scratch' / ('task-' + str(task['id'])) / '.cache').exists() for task in tasks))

    def test_donor_scan_timeout_runs_once_per_sweep_and_keeps_dependencies(self):
        tasks = [self.job(closed=False) for _ in range(3)]
        for task in tasks:
            root = self.target(task)
            (root / '.gitignore').write_text('node_modules/\n')
            (root / 'node_modules').mkdir()
            (root / 'node_modules' / 'file').write_text('dependency')
            (self.state / 'scratch' / ('task-' + str(task['id'])) / '.cache').mkdir(parents=True)
            self.age(task)
        self.call('cleanup.idle_trim', enabled=True)
        calls, scans = self.counted_donor_scans(error=True)
        with scans, self.assertLogs('coordinator.problems', level='WARNING'):
            self.sweep()
        self.assertEqual(len(calls), 1)
        self.assertTrue(all((self.target(task) / 'node_modules' / 'file').exists() for task in tasks))
        self.assertEqual([(self.state / 'scratch' / ('task-' + str(task['id'])) / '.cache').exists() for task in tasks],
                         [True, False, False])

    def test_switch_validation(self):
        self.assertEqual(self.call('settings.get').data['idle_trim'], 0)
        self.assertFalse(control_api.call(self.store, 'cleanup.idle_trim', {'enabled': True, 'hours': -1}).ok)
        self.assertFalse(control_api.call(self.store, 'cleanup.idle_trim', {'enabled': True}, source='mcp').ok)

    def test_tracked_file_in_ignored_directory_is_kept(self):
        task = self.job(closed=False)
        root = self.target(task)
        (root / '.gitignore').write_text('dist/\n')
        (root / 'dist').mkdir()
        (root / 'dist' / 'tracked').write_text('before')
        self.git('add', '-f', 'dist/tracked', cwd=root)
        self.git('commit', '-qm', 'track output', cwd=root)
        (root / 'dist' / 'tracked').write_text('modified')
        self.age(task)
        self.call('cleanup.idle_trim', enabled=True)
        self.sweep()
        self.assertEqual((root / 'dist' / 'tracked').read_text(), 'modified')

    def test_donor_link_inside_another_dependency_is_found(self):
        task = self.job(closed=False)
        root = self.target(task)
        first = root / 'a' / 'node_modules'
        second = root / 'b' / 'node_modules'
        first.mkdir(parents=True)
        second.mkdir(parents=True)
        (second / 'donor').symlink_to(first, target_is_directory=True)
        self.assertIn(first, idle_trim.donor_links((root,), [first, second]))

    def test_reference_scan_failure_backs_off_and_success_clears(self):
        task = self.job(closed=False)
        scratch = self.state / 'scratch' / ('task-' + str(task['id']))
        (scratch / '.cache').mkdir(parents=True)
        self.age(task)
        self.call('cleanup.idle_trim', enabled=True)
        with patch('coordinator.idle_trim.close_saves.scan_references', side_effect=OSError('read failed')) as scan:
            with self.assertLogs('coordinator.problems', level='WARNING'):
                self.sweep()
            self.sweep()
        self.assertEqual(scan.call_count, 1)
        [detail] = [row[0] for row in self.store.db.execute("SELECT detail FROM problems WHERE code='idle-trim-failed'")]
        self.assertRegex(detail, r'^item=references failures=1 OSError: read failed at ')
        self.assertTrue((scratch / '.cache').exists())
        with patch('coordinator.sweeps.time.time', return_value=time.time() + 120):
            self.sweep()
        self.assertFalse((scratch / '.cache').exists())
        self.assertIsNone(self.store.db.execute('SELECT 1 FROM sweep_failures').fetchone())

    def test_one_failed_job_does_not_stop_the_next(self):
        tasks = [self.job(closed=False) for _ in range(2)]
        for task in tasks:
            (self.state / 'scratch' / ('task-' + str(task['id'])) / '.cache').mkdir(parents=True)
            self.age(task)
        self.call('cleanup.idle_trim', enabled=True)
        real = idle_trim.recover_stage

        def recover(store, task_id):
            if task_id == tasks[0]['id']:
                raise ValueError('fixture stage failure')
            return real(store, task_id)

        with patch.object(idle_trim, 'recover_stage', recover), \
                self.assertLogs('coordinator.problems', level='WARNING'):
            self.sweep()
        self.assertTrue((self.state / 'scratch' / ('task-' + str(tasks[0]['id'])) / '.cache').exists())
        self.assertFalse((self.state / 'scratch' / ('task-' + str(tasks[1]['id'])) / '.cache').exists())
        [detail] = [row[0] for row in self.store.db.execute(
            "SELECT detail FROM problems WHERE code='idle-trim-failed' AND task=?", (tasks[0]['id'],))]
        self.assertRegex(detail, r'^item=%d failures=1 ValueError: fixture stage failure at test_idle_trim\.py:\d+ recover '
                         % tasks[0]['id'])
        self.assertEqual(self.store.db.execute("SELECT failures FROM sweep_failures WHERE sweep='idle-trim'").fetchone()[0], 1)

    def test_reference_scan_strict_mode_propagates_read_error(self):
        root = self.root / 'scripts'
        root.mkdir()
        with patch('coordinator.close_saves.os.walk', side_effect=OSError('read failed')):
            with self.assertRaises(OSError):
                close_saves.result_script_references(root, strict=True)

    def test_unreadable_donor_scan_keeps_scratch(self):
        task = self.job(closed=False)
        scratch = self.state / 'scratch' / ('task-' + str(task['id']))
        scratch.mkdir(parents=True, exist_ok=True)
        (scratch / 'file').write_text('keep')
        (scratch / '.cache').mkdir()
        (self.target(task) / '.gitignore').write_text('node_modules/\n')
        (self.target(task) / 'node_modules').mkdir()
        self.age(task)
        self.call('cleanup.idle_trim', enabled=True)
        with patch('coordinator.idle_trim.donor_links', side_effect=PermissionError('read failed')), \
                patch.object(idle_trim, 'bounded_scan', side_effect=lambda store, function, *args: function(*args)):
            with self.assertLogs('coordinator.problems', level='WARNING'):
                self.sweep()
        self.assertTrue((scratch / 'file').exists())
        self.assertTrue((scratch / '.cache').exists())

    def test_uncommitted_stage_restores_after_interrupted_rename(self):
        task = self.job(closed=False)
        scratch = self.state / 'scratch' / ('task-' + str(task['id']))
        source = scratch / '.cache'
        source.mkdir(parents=True)
        (source / 'data').write_text('keep')
        stage, record = idle_trim.prepare_stage(self.store, task['id'], source)
        with self.assertRaisesRegex(RuntimeError, 'crash'):
            with self.store.transaction():
                self.store.db.execute('INSERT INTO idle_trims(task,idle_since,active) VALUES (?,0,1)', (task['id'],))
                idle_trim.stage_move(self.store, task['id'], source, stage, record)
                raise RuntimeError('crash')
        self.assertFalse(source.exists())
        self.assertTrue((stage / 'data' / 'data').exists())
        idle_trim.recover_pending(self.store)
        self.assertEqual((source / 'data').read_text(), 'keep')
        self.assertFalse(stage.exists())

    def test_stage_recovery_failure_skips_one_job(self):
        tasks = [self.job(closed=False) for _ in range(2)]
        for task in tasks:
            (self.state / 'idle-trim-stages' / ('task-' + str(task['id']))).mkdir(parents=True)
        recovered = []

        def recover(store, task_id):
            if task_id == tasks[0]['id']:
                raise KeyError('phase')
            recovered.append(task_id)

        with patch.object(idle_trim, 'recover_stage', recover), \
                patch.object(idle_trim, 'bounded_scan', side_effect=lambda store, function, *args: function(*args)), \
                self.assertLogs('coordinator.problems', 'WARNING'):
            idle_trim.recover_pending(self.store)
        self.assertEqual(recovered, [tasks[1]['id']])
        self.assertRegex(self.store.db.execute(
            "SELECT detail FROM problems WHERE code='stage-recovery-failed' AND task=?", (tasks[0]['id'],)).fetchone()[0],
            r"^item=%d failures=1 KeyError: 'phase' at test_idle_trim\.py:\d+ recover < idle_trim\.py:\d+ recover_unlocked"
            % tasks[0]['id'])

    def test_recovery_keeps_both_paths_when_source_is_recreated(self):
        task = self.job(closed=False)
        source = self.state / 'scratch' / ('task-' + str(task['id'])) / '.cache'
        source.mkdir(parents=True)
        (source / 'old').write_text('old')
        stage, record = idle_trim.prepare_stage(self.store, task['id'], source)
        with self.assertRaisesRegex(RuntimeError, 'crash'):
            with self.store.transaction():
                self.store.db.execute('INSERT INTO idle_trims(task,idle_since,active) VALUES (?,0,1)', (task['id'],))
                idle_trim.stage_move(self.store, task['id'], source, stage, record)
                raise RuntimeError('crash')
        source.mkdir()
        (source / 'new').write_text('new')
        idle_trim.recover_pending(self.store)
        self.assertEqual((source / 'new').read_text(), 'new')
        self.assertEqual((stage / 'data' / 'old').read_text(), 'old')
        self.assertIsNotNone(self.store.db.execute(
            "SELECT 1 FROM problems WHERE code='stage-collision'").fetchone())
        with self.assertRaises(control_api.Refused):
            idle_trim.spawn_guard(self.store, task['id'])
        self.assertEqual((stage / 'data' / 'old').read_text(), 'old')
        self.assertEqual((source / 'new').read_text(), 'new')

    def test_ready_committed_stage_is_restored_before_retry(self):
        task = self.job(closed=False)
        source = self.state / 'scratch' / ('task-' + str(task['id'])) / '.cache'
        source.mkdir(parents=True)
        (source / 'data').write_text('keep')
        stage, record = idle_trim.prepare_stage(self.store, task['id'], source)
        with self.store.transaction():
            self.store.db.execute('INSERT INTO idle_trims(task,idle_since,active) VALUES (?,0,1)', (task['id'],))
            idle_trim.stage_move(self.store, task['id'], source, stage, record)
        idle_trim.recover_pending(self.store)
        self.assertEqual((source / 'data').read_text(), 'keep')
        self.assertFalse(stage.exists())

    def test_spawn_restores_ready_stage_inside_existing_transaction(self):
        task = self.job(closed=False)
        source = self.state / 'scratch' / ('task-' + str(task['id'])) / '.cache'
        source.mkdir(parents=True)
        (source / 'data').write_text('keep')
        stage, record = idle_trim.prepare_stage(self.store, task['id'], source)
        with self.store.transaction():
            self.store.db.execute('INSERT INTO idle_trims(task,idle_since,active) VALUES (?,0,1)', (task['id'],))
            idle_trim.stage_move(self.store, task['id'], source, stage, record)
        with self.store.transaction():
            idle_trim.spawn_guard(self.store, task['id'])
        self.assertEqual((source / 'data').read_text(), 'keep')
        self.assertFalse(stage.exists())
        self.assertFalse((self.state / 'idle-trim-quarantine').exists())

    def test_scratch_creation_restores_staged_root_before_mkdir(self):
        task = self.job(closed=False)
        source = close_saves.scratch(self.store, task['id'], create=True)
        (source / 'data').write_text('keep')
        stage, record = idle_trim.prepare_stage(self.store, task['id'], source)
        with self.store.transaction():
            self.store.db.execute('INSERT INTO idle_trims(task,idle_since,active) VALUES (?,0,1)', (task['id'],))
            idle_trim.stage_move(self.store, task['id'], source, stage, record)
        with self.store.transaction():
            actual = close_saves.scratch(self.store, task['id'], create=True)
        self.assertEqual((actual / 'data').read_text(), 'keep')
        self.assertFalse(stage.exists())

    def test_incomplete_deletion_is_quarantined_without_blocking_spawn(self):
        task = self.job(closed=False)
        source = self.state / 'scratch' / ('task-' + str(task['id'])) / '.cache'
        source.mkdir(parents=True)
        (source / 'data').write_text('partial')
        stage, record = idle_trim.prepare_stage(self.store, task['id'], source)
        with self.store.transaction():
            self.store.db.execute('INSERT INTO idle_trims(task,idle_since,active) VALUES (?,0,1)', (task['id'],))
            idle_trim.stage_move(self.store, task['id'], source, stage, record)
            self.store.db.execute('UPDATE idle_trims SET active=3 WHERE task=?', (task['id'],))
        record['phase'] = 'deleting'
        close_saves.atomic_json(stage / 'intent.json', record)
        idle_trim.recover_pending(self.store)
        self.assertFalse(stage.exists())
        self.assertFalse(source.exists())
        idle_trim.spawn_guard(self.store, task['id'])
        quarantined = list((self.state / 'idle-trim-quarantine').iterdir())
        self.assertEqual((quarantined[0] / 'data' / 'data').read_text(), 'partial')
        self.assertEqual(self.store.db.execute('SELECT active FROM idle_trims WHERE task=?', (task['id'],)).fetchone()[0], 0)

    def test_incomplete_deletion_recovers_when_source_parent_is_gone(self):
        task = self.job(closed=False)
        source = self.state / 'scratch' / ('task-' + str(task['id'])) / '.cache'
        source.mkdir(parents=True)
        (source / 'data').write_text('partial')
        stage, record = idle_trim.prepare_stage(self.store, task['id'], source)
        with self.store.transaction():
            self.store.db.execute('INSERT INTO idle_trims(task,idle_since,active) VALUES (?,0,1)', (task['id'],))
            idle_trim.stage_move(self.store, task['id'], source, stage, record)
        record['phase'] = 'deleting'
        close_saves.atomic_json(stage / 'intent.json', record)
        source.parent.rename(self.state / 'moved-scratch')
        idle_trim.spawn_guard(self.store, task['id'])
        quarantined = list((self.state / 'idle-trim-quarantine').iterdir())
        self.assertEqual((quarantined[0] / 'data' / 'data').read_text(), 'partial')
        self.assertEqual(self.store.db.execute('SELECT active FROM idle_trims WHERE task=?', (task['id'],)).fetchone()[0], 0)

    def test_active_trim_refuses_spawn_and_stale_mark_recovers(self):
        task = self.job(closed=False)
        fd = idle_trim.lock_file(self.state, task['id'])
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.store.db:
                self.store.db.execute('INSERT INTO idle_trims(task,idle_since,active) VALUES (?,0,1)', (task['id'],))
            with self.assertRaises(control_api.Refused):
                idle_trim.spawn_guard(self.store, task['id'])
        finally:
            os.close(fd)
        with self.store.db:
            idle_trim.spawn_guard(self.store, task['id'])
        self.assertEqual(self.store.db.execute('SELECT active FROM idle_trims WHERE task=?', (task['id'],)).fetchone()[0], 0)

    def test_next_worker_gets_reminder_without_changing_deduplication(self):
        task = self.job(closed=False)
        with self.store.db:
            self.store.db.execute('INSERT INTO idle_trims(task,idle_since,trimmed) VALUES (?,0,1)', (task['id'],))
        first = self.call('workers.spawn', task=task['id'], provider='codex', prompt='Continue the job')
        second = self.call('workers.spawn', task=task['id'], provider='codex', prompt='Continue the job')
        self.assertEqual(first.data['worker'], second.data['worker'])
        self.assertTrue(self.store.db.execute('SELECT 1 FROM idle_trim_workers WHERE worker=?',
                                              (first.data['worker'],)).fetchone())
