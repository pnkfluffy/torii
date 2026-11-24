import asyncio
import hashlib
import inspect
import shutil
import subprocess
import json
import os
from pathlib import Path
import plistlib
import time
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import cleanup, close_saves, control_api, extension, job_limits, workspaces
from coordinator.service import Service
from tests import test_worktree_cleanup as fixtures


class CloseSavesTests(unittest.TestCase):
    """Strict close-save rules. Done and dropped jobs use the loose fallback, tested in test_closed_job_clear."""

    def setUp(self):
        fixtures.WorktreeCleanupTests.setUp(self)
        strict = patch.object(close_saves, 'TERMINAL', ())
        strict.start()
        self.addCleanup(strict.stop)

    tearDown = fixtures.WorktreeCleanupTests.tearDown
    git = fixtures.WorktreeCleanupTests.git
    call = fixtures.WorktreeCleanupTests.call
    target = fixtures.WorktreeCleanupTests.target
    worker = fixtures.WorktreeCleanupTests.worker
    commit = fixtures.WorktreeCleanupTests.commit
    git_directory = fixtures.WorktreeCleanupTests.git_directory
    submodule_job = fixtures.WorktreeCleanupTests.submodule_job

    def job(self, closed=False):
        return fixtures.WorktreeCleanupTests.job(self, closed=closed)

    def test_dev_spawn_creates_worktree(self):
        task = self.call('tasks.create', title='Dev').data
        result = self.call('workers.spawn', task=task['id'], provider='codex', prompt='Build', work='dev')
        worker = self.store.db.execute('SELECT cwd FROM workers WHERE id=?', (result.data['worker'],)).fetchone()
        self.assertEqual(worker['cwd'], self.store.task_get(task['id'])['worktree'])
        self.assertTrue(Path(worker['cwd']).is_dir())

    def test_opencode_research_uses_registered_worktree(self):
        provider = 'opencode'
        with patch('coordinator.workers.spawn_refusal', return_value=None):
            task = self.call('tasks.create', title=provider).data
            result = self.call('workers.spawn', task=task['id'], provider=provider, prompt='Read', work='research')
            worker = dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (result.data['worker'],)).fetchone())
            cwd = Path(worker['cwd'])
            self.assertEqual(str(cwd), self.store.task_get(task['id'])['worktree'])
            self.assertEqual((cwd / 'README.md').read_text(), 'base\n')
            self.assertNotIn('scratch', json.loads(worker['workspace']))

    def test_close_without_worktree_preserves_results_and_clears_scratch(self):
        task = self.call('tasks.create', title='Research').data
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        (scratch / 'notes.txt').write_text('notes')
        result = self.state / 'results' / ('task-' + str(task['id']))
        result.mkdir(parents=True)
        (result / 'report.txt').write_text('report')
        with self.store.db:
            message = self.store.enqueue_report('1:2', 'Pending', image=str(scratch / 'notes.txt'))
        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        self.drain()
        self.assertTrue(scratch.exists())
        self.store.delivered(message, 100)
        self.assertEqual(self.close(task)['copy'], 'cleared')
        self.assertFalse(scratch.exists())
        self.assertEqual((result / 'report.txt').read_text(), 'report')
        self.assertTrue((self.folder(task) / 'saved.json').is_file())

    def test_armed_wait_preserves_job_scratch_until_final_close(self):
        task = self.job()
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        (scratch / 'tool.py').write_text('tool')
        wait = self.call('tasks.wait', task=task['id'], kind='signal', ask='Continue later', signal='ready').data
        self.drain()
        self.assertEqual((scratch / 'tool.py').read_text(), 'tool')
        self.call('pending.cancel', wait=wait['id'], reason='Resume')
        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        self.drain()
        self.assertFalse(scratch.exists())

    def test_cancelled_watch_releases_scratch_of_done_job(self):
        task = self.job()
        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        (scratch / 'tool.py').write_text('tool')
        wait = self.call('tasks.wait', task=task['id'], kind='signal', ask='Check later', signal='ready', watch=True).data
        self.drain()
        self.assertEqual((scratch / 'tool.py').read_text(), 'tool')
        self.call('pending.cancel', wait=wait['id'], reason='No longer needed')
        self.drain()
        self.assertFalse(scratch.exists())

    def test_fired_watch_releases_scratch_when_done_job_cannot_reopen(self):
        task = self.job()
        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        (scratch / 'tool.py').write_text('tool')
        wait = self.call('tasks.wait', task=task['id'], kind='signal', ask='Check later', signal='ready', watch=True).data
        self.drain()
        self.assertEqual((scratch / 'tool.py').read_text(), 'tool')
        self.store.put('topic_lifecycle:' + task['topic'], {'state': 'closed'})
        self.store.wait_fire(wait['id'], outcome='seen')
        self.drain()
        self.assertFalse(scratch.exists())

    def closed_then_reopened(self):
        task = self.job()
        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        self.call('tasks.update', task=task['id'], status='open')
        current = self.store.task_get(task['id'])
        self.assertEqual((current['status'], current['copy']), ('open', 'saving'))
        return current

    def drain(self):
        asyncio.run(workspaces.cleanup_closed_copies(self.store))

    def close(self, task):
        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        self.drain()
        return self.store.task_get(task['id'])

    def folder(self, task):
        return self.state / 'results' / ('task-' + str(task['id'])) / 'rounds' / str(task['iteration'])

    def manifest(self, task):
        return json.loads((self.folder(task) / 'saved.json').read_text())

    def reopen(self, task):
        self.call('tasks.update', task=task['id'], status='open')
        with self.store.db:
            self.store.db.execute("INSERT INTO task_iterations(task,k,opened,opened_by) VALUES (?,?,?,'owner')",
                                  (task['id'], task['iteration'] + 1, time.time()))
            self.store.db.execute('UPDATE tasks SET iteration=? WHERE id=?', (task['iteration'] + 1, task['id']))
        return self.call('worktree.create', task=task['id'])

    def assert_cleared(self, task):
        self.assertFalse(self.target(task).exists(), 'A verified close must clear the owned copy')
        stages = [record for _, record in workspaces._deletion_stages(self.store.directory)]
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'cleared', str(stages))
        self.assertFalse(self.trash.exists())

    def slug_job(self):
        task = self.job()
        target = self.target(task)
        branch = 'torii/task-%s-slug' % task['id']
        self.git('switch', '-c', branch, cwd=target)
        head = self.commit(target, 'unpushed slug commit\n')
        (target / 'README.md').write_bytes(b'dirty\r\nbytes\x00\xff\n')
        (target / 'new.bin').write_bytes(b'new\x00\x80\n')
        expected = {str(p.relative_to(target)): p.read_bytes()
                    for p in target.rglob('*') if p.is_file() and p.name != '.git'}
        return task, branch, head, expected

    def results_job(self):
        task = self.job()
        result = self.folder(task).parent.parent
        result.mkdir(parents=True, exist_ok=True)
        bulk = result / 'bulk.bin'
        with bulk.open('wb') as handle:
            handle.truncate(10 * 1024 * 1024 + 1)
        return task, result, bulk

    def assert_results_trimmed(self, task, result, bulk):
        checksum = close_saves.digest(bulk)
        self.assertEqual(self.close(task)['copy'], 'cleared')
        self.assertFalse(bulk.exists())
        self.assertFalse(close_saves.scratch(self.store, task['id']).exists())
        records = [json.loads(line) for line in (result / 'moved-to-scratch.txt').read_text().splitlines()]
        self.assertIn(dict(checksum, path='bulk.bin'), records)
        return records

    def test_results_oversized_moves_to_scratch_before_clear(self):
        task, result, bulk = self.results_job()
        original = workspaces.delete_tree

        async def inspect_scratch(path, **kwargs):
            if path.is_relative_to(self.folder(task) / 'extras'):
                self.assertFalse(bulk.exists())
                self.assertTrue(any(p.name.startswith('bulk.bin') for p in close_saves.files(path)))
            return await original(path, **kwargs)

        with patch.object(workspaces, 'delete_tree', side_effect=inspect_scratch):
            self.assert_results_trimmed(task, result, bulk)

    def test_trim_keeps_file_referenced_by_open_job(self):
        task, result, bulk = self.results_job()
        other = self.job()
        self.call('tasks.update', task=other['id'], note='Import ' + str(bulk))
        with patch('coordinator.close_saves.pwd.getpwuid', return_value=SimpleNamespace(pw_dir=str(self.root))):
            self.close(task)
        self.assertTrue(bulk.exists())

    def test_launch_agent_references_use_account_home_not_home_environment(self):
        task, result, bulk = self.results_job()
        account_home = self.root / 'account-home'
        launch = account_home / 'Library' / 'LaunchAgents'
        launch.mkdir(parents=True)
        with (launch / 'test.plist').open('wb') as handle:
            plistlib.dump({'ProgramArguments': ['tool', str(bulk)]}, handle)
        with patch.dict(os.environ, {'HOME': str(self.root / 'service-home')}), \
                patch('coordinator.close_saves.pwd.getpwuid', return_value=SimpleNamespace(pw_dir=str(account_home))):
            refs = close_saves.result_references(self.state)
        self.assertTrue(close_saves.result_referenced(bulk, result, refs))

    def test_note_reference_ending_a_sentence_keeps_file(self):
        task, result, bulk = self.results_job()
        other = self.job()
        self.call('tasks.update', task=other['id'], note='Import ' + str(bulk) + '.')
        with patch('coordinator.close_saves.pwd.getpwuid', return_value=SimpleNamespace(pw_dir=str(self.root))):
            self.close(task)
        self.assertTrue(bulk.exists())

    def test_note_reference_followed_by_colon_keeps_file(self):
        task, result, bulk = self.results_job()
        other = self.job()
        self.call('tasks.update', task=other['id'], note='Input ' + str(bulk) + ': keep it')
        with patch('coordinator.close_saves.pwd.getpwuid', return_value=SimpleNamespace(pw_dir=str(self.root))):
            self.close(task)
        self.assertTrue(bulk.exists())

    def test_trim_keeps_file_imported_by_open_job_script(self):
        task, result, bulk = self.results_job()
        other = self.job()
        source = self.state / 'scratch' / ('task-' + str(other['id'])) / 'publish.py'
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text('import_from = ' + repr(str(bulk)))
        with patch('coordinator.close_saves.pwd.getpwuid', return_value=SimpleNamespace(pw_dir=str(self.root))):
            self.close(task)
        self.assertTrue(bulk.exists())

    def test_script_folder_reference_survives_trim_and_fade(self):
        task, result, bulk = self.results_job()
        family = result / 'batch' / 'assets'
        family.mkdir(parents=True)
        target = family / 'index.json.gz'
        bulk.rename(target)
        other = self.job()
        source = self.state / 'scratch' / ('task-' + str(other['id'])) / 'verify.py'
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text("import os\nBASE = " + repr(str(family)) + "\nos.path.join(BASE, 'index.json.gz')\n")
        with patch('coordinator.close_saves.pwd.getpwuid', return_value=SimpleNamespace(pw_dir=str(self.root))):
            self.close(task)
            asyncio.run(close_saves.fade_results(self.store))
        self.assertTrue(target.exists())

    def test_permission_block_skips_until_folder_changes(self):
        task, result, bulk = self.results_job()
        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET copy='cleared' WHERE id=?", (task['id'],))
        original = Path.rename
        calls = []
        def denied(path, destination):
            if path == bulk:
                calls.append(path)
                raise PermissionError(13, 'Permission denied', str(path))
            return original(path, destination)
        with patch.object(Path, 'rename', denied), patch('coordinator.close_saves.pwd.getpwuid', return_value=SimpleNamespace(pw_dir=str(self.root))):
            refs = close_saves.result_references(self.state)
            with self.assertLogs('coordinator.workspaces', level='WARNING') as logs:
                asyncio.run(close_saves.trim_results(self.store, task, self.root, refs))
                asyncio.run(close_saves.trim_results(self.store, task, self.root, refs))
                asyncio.run(close_saves.fade_results(self.store))
            self.assertEqual(len(calls), 1)
            self.assertEqual(len(logs.output), 1)
            row = self.store.db.execute('SELECT path,reason FROM results_blocked WHERE task=?', (task['id'],)).fetchone()
            self.assertEqual(row['path'], str(bulk))
            self.assertIn('Permission denied', row['reason'])
            (result / 'changed').touch()
            with self.assertLogs('coordinator.workspaces', level='WARNING'):
                asyncio.run(close_saves.fade_results(self.store))
            self.assertEqual(len(calls), 2)
        self.assertTrue(bulk.exists())

    def test_trim_fails_closed_when_reference_scan_fails(self):
        task, result, bulk = self.results_job()
        with patch.object(close_saves, 'async_scan_references', side_effect=OSError('unavailable')):
            self.close(task)
        self.assertTrue(bulk.exists())

    def test_trim_scans_once_off_loop_for_multiple_jobs(self):
        first, _, first_bulk = self.results_job()
        second, _, second_bulk = self.results_job()
        for task in (first, second):
            self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        original = close_saves.scan_process.run
        calls = []
        main_thread = threading.get_ident()
        def scan(function, *args, **kwargs):
            if function is close_saves.result_references:
                calls.append(threading.get_ident())
            return original(function, *args, **kwargs)
        with patch('coordinator.close_saves.pwd.getpwuid', return_value=SimpleNamespace(pw_dir=str(self.root))), \
                patch.object(close_saves.scan_process, 'run', side_effect=scan):
            self.drain()
        self.assertEqual(len(calls), 1)
        self.assertNotEqual(calls[0], main_thread)
        self.assertFalse(first_bulk.exists())
        self.assertFalse(second_bulk.exists())

    def counted_scans(self):
        original = close_saves.scan_process.run
        calls = []

        def scan(function, *args, **kwargs):
            calls.append(function.__name__)
            return original(function, *args, **kwargs)
        return calls, patch.object(close_saves.scan_process, 'run', side_effect=scan)

    def test_trim_of_many_results_files_batches_its_scans(self):
        task, result, bulk = self.results_job()
        names = [a + b for a in 'abcdefghijklmn' for b in 'abcdefghijklmn'][:120]
        for name in names:
            self.sized(result / 'many' / (name + '.bin'), 1)
        calls, counting = self.counted_scans()
        with counting:
            records = self.assert_results_trimmed(task, result, bulk)
        moved = [record for record in records if record['path'].startswith('many/')]
        self.assertGreater(len(moved), 60)
        self.assertEqual([record['path'] for record in moved if (result / record['path']).exists()], [])
        per_file = ('digest_with_stamp', 'digests_with_stamps', 'scan_preconditions', 'task_reference_index')
        self.assertLess(sum(calls.count(name) for name in per_file), 30)

    def test_verify_hashes_many_saved_files_in_few_scans(self):
        task = self.job()
        target = self.target(task)
        (target / '.gitignore').write_text('out/\n')
        (target / 'out').mkdir()
        for index in range(80):
            (target / 'out' / ('part-%03d.txt' % index)).write_text('ignored %d\n' % index)
        calls, counting = self.counted_scans()
        with counting:
            closed = self.close(task)
        self.assertEqual(closed['copy'], 'cleared', closed['copy_reason'])
        self.assertEqual(len(self.manifest(task)['ignored']['copied']), 80)
        self.assertLess(calls.count('digest_with_stamp') + calls.count('digests_with_stamps'), 20)
        self.reopen(task)
        self.assertEqual((target / 'out' / 'part-079.txt').read_text(), 'ignored 79\n')

    def sized(self, path, mib):
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('wb') as handle:
            handle.truncate(mib * 1024 * 1024)

    def test_waiting_close_keeps_handoff_source(self):
        task, result, bulk = self.results_job()
        parts = [result / ('archive.zip.%03d' % i) for i in range(1, 5)]
        for part in parts:
            self.sized(part, 42)
        image = result / 'images' / 'sample.png'
        self.sized(image, 53)
        with self.store.db:
            self.store.db.execute('UPDATE tasks SET notes=? WHERE id=?',
                                  ('Keep %s until the transfer is complete.' % result, task['id']))
        self.worker(task['id'], self.target(task), status='done')
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET status='waiting' WHERE id=?", (task['id'],))
        self.drain()
        current = self.store.task_get(task['id'])
        self.assertEqual((current['status'], current['copy']), ('waiting', 'cleared'))
        self.assertEqual([p.name for p in parts + [image, bulk] if not p.exists()], [])
        self.assertFalse((result / 'trim-preview.txt').exists())

    def test_results_interrupted_pause_keeps_results(self):
        for cause in ('quota', 'offline', 'crash'):
            with self.subTest(cause=cause):
                task, result, bulk = self.results_job()
                with self.store.db:
                    self.store.db.execute("UPDATE task_iterations SET cause=?,resume_at=? WHERE task=? AND k=?",
                                          (cause, time.time() + 3600, task['id'], task['iteration']))
                    self.store.db.execute("UPDATE tasks SET status='interrupted' WHERE id=?", (task['id'],))
                self.drain()
                self.assertEqual(self.store.task_get(task['id'])['copy'], 'cleared')
                self.assertTrue(bulk.is_file())
                self.assertEqual(bulk.stat().st_size, 10 * 1024 * 1024 + 1)
                self.assertFalse((result / 'moved-to-scratch.txt').exists())

    def test_sparsebundle_bands_stay_together(self):
        task, result, bulk = self.results_job()
        image = result / 'private-results.sparsebundle'
        for index in range(16):
            self.sized(image / 'bands' / format(index, 'x'), 8)
        (image / 'Info.plist').write_text('<plist/>')
        self.assert_results_trimmed(task, result, bulk)
        self.assertEqual(sorted(p.name for p in (image / 'bands').iterdir()),
                         sorted(format(i, 'x') for i in range(16)))

    def test_results_bundles_and_split_archives_stay(self):
        task, result, bulk = self.results_job()
        names = ('archive.zip.001', 'archive.zip.002', 'archive.7z.001', 'archive.7z.002',
                 'archive.part1.rar', 'archive.part2.rar', 'other.z01', 'other.zip',
                 'other.r00', 'other.rar', 'custom.001.bin', 'custom.002.bin',
                 'output.bundle/data', 'Photos.photoslibrary/data', 'App.app/Contents/data',
                 'extensionless/Contents/Info.plist', 'extensionless/Contents/data')
        for name in names:
            self.sized(result / name, 11)
        self.assert_results_trimmed(task, result, bulk)
        self.assertEqual([name for name in names if not (result / name).exists()], [])

    def test_results_keep_and_copy_holds_skip_trim(self):
        for reason in ('keep', 'lock'):
            with self.subTest(reason=reason):
                task, result, bulk = self.results_job()
                if reason == 'keep':
                    self.store.put('close_saves_keep', [task['id']])
                else:
                    self.git('worktree', 'lock', str(self.target(task)))
                current = self.close(task)
                self.assertEqual(current['copy'], 'held' if reason == 'lock' else 'cleared')
                self.assertTrue(bulk.is_file())
                self.assertEqual(bulk.stat().st_size, 10 * 1024 * 1024 + 1)
                self.assertFalse((result / 'moved-to-scratch.txt').exists())

    def test_results_final_close_after_pause_trims(self):
        task, result, bulk = self.results_job()
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET status='waiting' WHERE id=?", (task['id'],))
        self.drain()
        self.assertTrue(bulk.is_file())
        self.assertEqual(bulk.stat().st_size, 10 * 1024 * 1024 + 1)
        self.assert_results_trimmed(task, result, bulk)

    def test_results_dropped_job_trims(self):
        task, result, bulk = self.results_job()
        self.call('tasks.update', task=task['id'], status='dropped', summary='Closed in test.', note='No longer needed')
        self.drain()
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'cleared')
        self.assertFalse(bulk.exists())
        self.assertEqual(json.loads((result / 'moved-to-scratch.txt').read_text())['path'], 'bulk.bin')

    def test_results_keep_manifest_folder_protects_children(self):
        task, result, bulk = self.results_job()
        kept = result / 'site' / 'nested' / 'large.bin'
        self.sized(kept, 11)
        (self.project / 'KEEP').mkdir()
        (self.project / 'KEEP' / 'MANIFEST.md').write_text('- `%s/`\n' % (result / 'site'))
        self.assert_results_trimmed(task, result, bulk)
        self.assertTrue(kept.is_file())
        self.assertEqual(kept.stat().st_size, 11 * 1024 * 1024)

    def test_results_keep_manifest_path_and_hash_stay(self):
        task, result, bulk = self.results_job()
        keep = self.project / 'KEEP'
        keep.mkdir()
        names = ('by path.bin', 'by-hash.bin')
        for name in names:
            (result / name).write_bytes(bulk.read_bytes())
        (result / names[0]).write_bytes(b'p' + bulk.read_bytes()[1:])
        checksum = close_saves.digest(result / names[1])['sha256']
        (keep / 'MANIFEST.md').write_text('- <%s>\n- `%s`\n' % (result / names[0], checksum))
        bulk.write_bytes(b'x' + bulk.read_bytes()[1:])
        self.assert_results_trimmed(task, result, bulk)
        self.assertTrue(all((result / name).exists() for name in names))

    def test_results_rounds_and_git_bundle_stay(self):
        task, result, bulk = self.results_job()
        recovery = result / 'rounds' / 'earlier' / 'recovery.bin'
        recovery.parent.mkdir(parents=True)
        recovery.write_bytes(bulk.read_bytes())
        bundle = result / 'branch.bundle'
        bundle.write_bytes(bulk.read_bytes())
        self.assert_results_trimmed(task, result, bulk)
        self.assertTrue(recovery.exists())
        self.assertTrue(bundle.exists())

    def test_results_pending_send_stays_until_delivered(self):
        task, result, bulk = self.results_job()
        image = result / 'pending.bin'
        image.write_bytes(bulk.read_bytes())
        with self.store.db:
            outbox = self.store.enqueue_report(fixtures.TOPIC, 'file', image=str(image))
        self.assert_results_trimmed(task, result, bulk)
        self.assertTrue(image.exists())
        self.assertFalse(self.store.db.execute('SELECT delivered FROM outbox WHERE id=?', (outbox,)).fetchone()[0])

    def test_results_under_caps_are_untouched(self):
        control, result, bulk = self.results_job()
        task = self.job()
        small = self.folder(task).parent.parent
        small.mkdir(parents=True, exist_ok=True)
        proof = small / 'proof.txt'
        proof.write_text('small proof\n')
        before = proof.stat()
        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        self.assert_results_trimmed(control, result, bulk)
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'cleared')
        self.assertEqual(proof.read_text(), 'small proof\n')
        self.assertEqual((proof.stat().st_ino, proof.stat().st_mtime_ns), (before.st_ino, before.st_mtime_ns))
        self.assertFalse((small / 'moved-to-scratch.txt').exists())

    def test_results_total_cap_moves_largest_first(self):
        task, result, bulk = self.results_job()
        names = ('alpha.bin', 'beta.bin', 'gamma.bin', 'delta.bin', 'epsilon.bin', 'zeta.bin')
        for name, size in zip(names, (10, 10, 10, 10, 10, 9)):
            with (result / name).open('wb') as handle:
                handle.truncate(size * 1024 * 1024)
        records = self.assert_results_trimmed(task, result, bulk)
        self.assertEqual([entry['path'] for entry in records], ['bulk.bin', 'alpha.bin'])
        self.assertFalse((result / 'alpha.bin').exists())
        self.assertTrue(all((result / name).exists() for name in names[1:]))

    def test_results_move_retry_keeps_one_record(self):
        task, result, bulk = self.results_job()
        checksum = close_saves.digest(bulk)
        original = Path.rename

        def interrupt_after_move(path, destination):
            moved = original(path, destination)
            if path == bulk:
                raise OSError('interrupted after results move')
            return moved

        with patch.object(Path, 'rename', interrupt_after_move):
            self.assertEqual(self.close(task)['copy'], 'held')
        self.assertFalse(bulk.exists())
        self.assertEqual(self.close(task)['copy'], 'cleared')
        self.assertFalse(close_saves.scratch(self.store, task['id']).exists())
        records = [json.loads(line) for line in (result / 'moved-to-scratch.txt').read_text().splitlines()]
        self.assertEqual(records, [dict(checksum, path='bulk.bin')])

    def test_reflog_only_commit_slug_on(self):
        task, branch, _, _ = self.slug_job()
        target = self.target(task)
        self.git('stash', '-u', cwd=target)
        self.git('switch', '-c', 'side', cwd=target)
        lost = self.commit(target, 'side only\n')
        self.git('switch', branch, cwd=target)
        self.git('branch', '-D', 'side', cwd=target)
        self.git('stash', 'pop', cwd=target)
        self.assertIn(lost, self.git('rev-list', '--all', '--reflog').stdout.split())
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'cleared', closed['copy_reason'] or str(list(workspaces._deletion_stages(self.state))))
        self.assert_reflog_saved(task, lost)

    def test_reflog_only_commit_job_branch_off(self):
        task = self.job()
        target = self.target(task)
        self.git('switch', '-c', 'side', cwd=target)
        lost = self.commit(target, 'side only\n')
        self.git('switch', 'torii/task-%s' % task['id'], cwd=target)
        self.git('branch', '-D', 'side', cwd=target)
        self.assertIn(lost, self.git('rev-list', '--all', '--reflog').stdout.split())
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'cleared', closed['copy_reason'] or str(list(workspaces._deletion_stages(self.state))))
        self.assert_reflog_saved(task, lost)

    def assert_reflog_saved(self, task, lost):
        prefix = 'refs/torii/saved/task-%s/%s/' % (task['id'], task['iteration'])
        refs = self.git('for-each-ref', '--format=%(refname)', prefix).stdout.split()
        self.assertIn(lost, self.git('rev-list', *refs).stdout.split())
        bundle = self.folder(task) / 'branch.bundle'
        self.assertTrue(bundle.is_file())
        restored = self.root / 'bundle-only.git'
        self.git('init', '--bare', '-q', restored)
        self.git('fetch', bundle, '+refs/*:refs/*', cwd=restored)
        self.assertIn(lost, self.git('rev-list', '--all', cwd=restored).stdout.split())
        self.git('reflog', 'expire', '--expire=now', '--all')
        self.git('gc', '-q', '--prune=now')
        self.assertEqual(self.git('cat-file', '-t', lost).stdout.strip(), 'commit')

    def test_slug_renamed_between_stage_and_delete(self):
        task, branch, _, expected = self.slug_job()
        original = close_saves.stage

        async def rename_after(*args):
            await original(*args)
            self.git('branch', '-m', branch, branch + '-renamed')

        with patch.object(close_saves, 'stage', side_effect=rename_after):
            closed = self.close(task)
        reason = 'saved branch was renamed or deleted after staging: refs/heads/' + branch
        self.assertEqual((closed['copy'], closed['copy_reason']), ('held', reason))
        stage, record = next(workspaces._deletion_stages(self.state))
        self.assertEqual(record['error'], reason)
        self.assertEqual((stage / 'copy' / 'README.md').read_bytes(), expected['README.md'])
        self.assertFalse(record.get('deletion_started'))
        self.drain()
        rows = self.store.db.execute("SELECT detail FROM problems WHERE area='cleanup' AND code='copy-held' AND task=?",
                                     (task['id'],)).fetchall()
        self.assertEqual([row['detail'] for row in rows], [reason])
        self.git('branch', '-m', branch + '-renamed', branch)
        self.call('cleanup.retry', task=task['id'])
        self.drain()
        self.assert_cleared(task)

    def loop_passes(self, seconds=1.0):
        storage = SimpleNamespace(reconcile=AsyncMock(), schedule=lambda: None)
        real = workspaces.cleanup_closed_copies
        passes = []

        async def counted(store, **options):
            passes.append(1)
            return await real(store, **options)

        async def exercise():
            event = asyncio.Event()
            fake = SimpleNamespace(store=self.store, wake=lambda name: event)
            try:
                await asyncio.wait_for(Service.worktree_cleanup_loop(fake), seconds)
            except asyncio.TimeoutError:
                pass

        with patch('coordinator.worker_storage.storage_for', return_value=storage), \
                patch.object(workspaces, 'cleanup_closed_copies', new=counted):
            asyncio.run(exercise())
        return len(passes)

    def renamed_after_stage(self):
        task, branch, _, _ = self.slug_job()
        original = close_saves.stage

        async def rename_after(*args):
            await original(*args)
            self.git('branch', '-m', branch, branch + '-renamed')

        with patch.object(close_saves, 'stage', side_effect=rename_after):
            self.close(task)
        return task

    def test_held_staged_copy_does_not_spin_cleanup_loop(self):
        self.renamed_after_stage()
        self.assertLessEqual(self.loop_passes(), 3, 'loop must not rerun back to back for a held staged copy')

    def test_reopened_held_staged_copy_does_not_spin(self):
        task = self.renamed_after_stage()
        self.call('tasks.update', task=task['id'], status='open')
        self.assertLessEqual(self.loop_passes(), 3)

    def test_held_staged_copy_with_expired_retry_does_not_spin(self):
        self.renamed_after_stage()
        with self.store.db:
            self.store.db.execute('UPDATE sweep_failures SET retry=?', (time.time() - 1,))
        self.assertLessEqual(self.loop_passes(), 3)

    def test_scope_shared_branch_reflog_and_stash(self):
        other = self.git('commit-tree', self.base + '^{tree}', '-p', self.base,
                         '-m', 'other branch reflog only').stdout.strip()
        self.git('update-ref', '--create-reflog', 'refs/heads/unrelated', other)
        self.git('update-ref', 'refs/heads/unrelated', self.base)
        self.git('update-ref', '--create-reflog', 'refs/worktree/main-only', other)
        self.git('update-ref', 'refs/worktree/main-only', self.base)
        (self.project / 'README.md').write_text('stash one\n')
        self.git('stash')
        (self.project / 'README.md').write_text('stash two\n')
        self.git('stash')
        older_stash = self.git('rev-parse', 'stash@{1}').stdout.strip()
        task = self.job()
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'cleared', closed['copy_reason'])
        pins = self.manifest(task)['reflog_refs'].values()
        self.assertNotIn(other, pins)
        self.assertNotIn(older_stash, pins)
        self.assertEqual(list(pins), [])

    def test_reflog_chain_pins_only_its_tip(self):
        task = self.job()
        target = self.target(task)
        lost = [self.commit(target, 'reflog-only %d\n' % n) for n in range(6)]
        self.git('reset', '--hard', self.base, cwd=target)
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'cleared', closed['copy_reason'])
        self.assertEqual(list(self.manifest(task)['reflog_refs'].values()), [lost[-1]])
        self.assert_reflog_saved(task, lost[0])

    def test_reflog_old_oid_is_saved(self):
        task = self.job()
        target = self.target(task)
        lost = self.commit(target, 'old reflog object\n')
        self.git('reset', '--hard', self.base, cwd=target)
        log = self.git_directory(target) / 'logs' / 'HEAD'
        log.write_bytes(log.read_bytes().splitlines(keepends=True)[-1])
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'cleared', closed['copy_reason'])
        self.assert_reflog_saved(task, lost)

    def test_index_only_commit_is_saved(self):
        task, _ = self.submodule_job(initialized=False, closed=False)
        target = self.target(task)
        lost = self.git('commit-tree', self.base + '^{tree}', '-p', self.base,
                         '-m', 'index-only commit').stdout.strip()
        self.git('update-index', '--cacheinfo', '160000,' + lost + ',sub', cwd=target)
        self.git('commit', '-qm', 'index pointer', cwd=target)
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'cleared', closed['copy_reason'])
        self.assert_reflog_saved(task, lost)

    def test_detached_without_job_branch_has_plain_hold_reason(self):
        task = self.job()
        target = self.target(task)
        self.git('switch', '--detach', cwd=target)
        lost = self.commit(target, 'detached only\n')
        (target / 'dirty.txt').write_text('dirty\n')
        self.git('branch', '-D', 'torii/task-%s' % task['id'])
        closed = self.close(task)
        self.assertEqual((closed['copy'], closed['copy_reason']),
                         ('held', 'worktree HEAD is detached and the saved job branch is missing'))
        self.assertEqual(self.git('rev-parse', 'HEAD', cwd=target).stdout.strip(), lost)
        self.assertEqual((target / 'dirty.txt').read_text(), 'dirty\n')

    def test_intent_to_add_has_plain_hold_reason(self):
        task, _, _, _ = self.slug_job()
        target = self.target(task)
        (target / 'ita.txt').write_text('intent to add content\n')
        self.git('add', '-N', 'ita.txt', cwd=target)
        closed = self.close(task)
        self.assertEqual((closed['copy'], closed['copy_reason']),
                         ('held', 'index has an intent-to-add entry: ita.txt'))
        self.assertEqual((target / 'ita.txt').read_text(), 'intent to add content\n')

    def test_worktree_ref_reflog_only_commit_is_saved(self):
        task = self.job()
        target = self.target(task)
        lost = self.git('commit-tree', self.base + '^{tree}', '-p', self.base, '-m', 'worktree reflog only').stdout.strip()
        ref = 'refs/worktree/transient'
        self.git('update-ref', '--create-reflog', ref, lost, cwd=target)
        self.git('update-ref', ref, self.base, cwd=target)
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'cleared', closed['copy_reason'])
        self.assert_reflog_saved(task, lost)

    def test_reflog_pin_failure_holds_copy_with_reason(self):
        task = self.job()
        target = self.target(task)
        self.git('switch', '-c', 'side', cwd=target)
        lost = self.commit(target, 'side only\n')
        self.git('switch', 'torii/task-%s' % task['id'], cwd=target)
        self.git('branch', '-D', 'side', cwd=target)
        original = close_saves.git

        async def locked_pin(cwd, *args, **options):
            if args[0] == 'update-ref' and '/reflog/' in str(args[1]):
                raise RuntimeError('fixture ref lock')
            return await original(cwd, *args, **options)

        with patch.object(close_saves, 'git', side_effect=locked_pin):
            closed = self.close(task)
        self.assertEqual((closed['copy'], closed['copy_reason']),
                         ('held', 'could not pin worktree reflog commits: fixture ref lock'))
        self.assertTrue(target.is_dir())
        self.assertIn(lost, self.git('rev-list', '--reflog', cwd=target).stdout.split())

    def test_reflog_pin_is_rechecked_before_and_after_staging(self):
        task = self.job()
        target = self.target(task)
        self.git('switch', '-c', 'side', cwd=target)
        self.commit(target, 'side only\n')
        self.git('switch', 'torii/task-%s' % task['id'], cwd=target)
        self.git('branch', '-D', 'side', cwd=target)
        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        asyncio.run(close_saves.save_job(self.store, task['id']))
        saved = self.manifest(task)
        self.assertTrue(saved['reflog_refs'])
        ref, oid = next(iter(saved['reflog_refs'].items()))
        stage, record = next(workspaces._deletion_stages(self.state))
        repository = self.git_directory(self.project)
        for replacement in (None, self.base):
            with self.subTest(replacement=replacement):
                if replacement is None:
                    self.git('update-ref', '-d', ref)
                else:
                    self.git('update-ref', ref, replacement)
                with self.assertRaisesRegex(ValueError, 'saved reflog commit is missing or changed'):
                    asyncio.run(close_saves.verify(repository, self.folder(task), saved))
                with self.assertRaisesRegex(ValueError, 'saved reflog commit is missing or changed'):
                    asyncio.run(close_saves.verify_staged(self.store, stage, record))
                self.assertTrue((stage / 'copy' / 'README.md').is_file())
                self.assertFalse(record.get('deletion_started'))
                self.git('update-ref', ref, oid)

    def test_late_staged_reflog_is_resaved_before_deletion(self):
        task = self.job()
        lost = self.git('commit-tree', self.base + '^{tree}', '-p', self.base, '-m', 'late reflog only').stdout.strip()
        original = close_saves.stage

        async def late_reflog(*args):
            await original(*args)
            stage, record = next(workspaces._deletion_stages(self.state))
            env = {'GIT_DIR': str(stage / 'git'), 'GIT_COMMON_DIR': record['repository']}
            await close_saves.git(stage / 'copy', 'update-ref', '--create-reflog', 'refs/worktree/transient', lost, env=env)
            await close_saves.git(stage / 'copy', 'update-ref', 'refs/worktree/transient', self.base, env=env)

        with patch.object(close_saves, 'stage', side_effect=late_reflog):
            self.close(task)
        self.assertTrue(self.target(task).is_dir())
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'saving')
        self.drain()
        self.assert_cleared(task)
        self.assert_reflog_saved(task, lost)

    def test_pr3_slug_branch_bundle_wip_and_reopen_without_tree_maker(self):
        task, branch, head, expected = self.slug_job()
        target = self.target(task)
        self.git('add', '-A', cwd=target)
        dirty_tree = self.git('write-tree', cwd=target).stdout.strip()
        self.git('reset', '-q', cwd=target)
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'cleared', closed['copy_reason'] or str(list(workspaces._deletion_stages(self.state))))
        self.assert_cleared(task)
        saved = self.manifest(task)
        self.assertEqual((saved['branch'], saved['branch_head'], saved['head']), ('refs/heads/' + branch, head, head))
        self.assertEqual(self.git('rev-parse', branch).stdout.strip(), head)
        self.assertEqual(self.git('rev-parse', saved['wip'] + '^{tree}').stdout.strip(), dirty_tree)
        self.assertEqual(saved['tree'], dirty_tree)
        self.assertIn(head + ' refs/heads/' + branch, self.git('bundle', 'list-heads', self.folder(task) / 'branch.bundle').stdout)
        self.store.put('cow_worktrees', True)
        optional = extension.optional
        with patch.object(extension, 'optional', lambda name: None if name == 'tree_maker' else optional(name)):
            self.reopen(task)
        self.assertEqual(self.git('symbolic-ref', '--short', 'HEAD', cwd=target).stdout.strip(), branch)
        self.assertEqual({str(p.relative_to(target)): p.read_bytes()
                          for p in target.rglob('*') if p.is_file() and p.name != '.git'}, expected)
        self.call('worktree.create', task=task['id'])
        self.assertEqual((target / 'README.md').read_bytes(), expected['README.md'])

    def test_pr3_deleted_slug_still_verifies_and_reopen_pauses_with_saved_path(self):
        task, branch, _, _ = self.slug_job()
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'cleared', closed['copy_reason'])
        saved = self.manifest(task)
        self.git('branch', '-D', branch)
        repository = self.git_directory(self.project)
        asyncio.run(close_saves.verify(repository, self.folder(task), saved, staged=True))
        self.call('tasks.update', task=task['id'], status='open')
        result = control_api.call(self.store, 'worktree.create', {'task': task['id']}, topic=task['topic'])
        self.assertFalse(result.ok)
        self.assertIn('Rebuild paused', result.text)
        self.assertIn(str(self.folder(task)), result.text)
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'held')
        self.assertTrue((self.folder(task) / 'saved.json').is_file())

    def test_pr3_deleted_job_branch_saves_and_reopens_slug(self):
        task, branch, _, expected = self.slug_job()
        self.git('branch', '-D', 'torii/task-' + str(task['id']))
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'cleared', closed['copy_reason'])
        self.assert_cleared(task)
        self.reopen(task)
        self.assertEqual(self.git('symbolic-ref', '--short', 'HEAD', cwd=self.target(task)).stdout.strip(), branch)
        self.assertEqual((self.target(task) / 'README.md').read_bytes(), expected['README.md'])

    def test_pr3_keep_blocks_requeue_and_retry(self):
        task, _, _, expected = self.slug_job()
        self.git('worktree', 'lock', str(self.target(task)))
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'held')
        self.call('cleanup.close_rules', keep=str(task['id']))
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'held')
        result = control_api.call(self.store, 'cleanup.retry', {'task': task['id']})
        self.assertEqual(result.state, 'refused')
        self.drain()
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'held')
        self.assertEqual((self.target(task) / 'README.md').read_bytes(), expected['README.md'])

    def test_pr3_index_only_bytes_hold_with_copy_intact(self):
        task, _, _, _ = self.slug_job()
        target = self.target(task)
        (target / 'staged.bin').write_bytes(b'precious index-only bytes\xff')
        self.git('add', 'staged.bin', cwd=target)
        (target / 'staged.bin').write_bytes(b'different working bytes\x00')
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'held', closed['copy_reason'])
        self.assertIn('index holds unsaved changes', closed['copy_reason'])
        self.assertTrue(target.is_dir())
        self.assertEqual(subprocess.run(('git', '-C', str(target), 'show', ':staged.bin'),
                                        capture_output=True, check=True, timeout=30).stdout, b'precious index-only bytes\xff')

    def test_pr3_bundle_includes_divergent_job_branch(self):
        task = self.job()
        target = self.target(task)
        job_head = self.commit(target, 'job branch unique commit\n')
        branch = 'torii/task-%s-slug' % task['id']
        self.git('switch', '-c', branch, self.base, cwd=target)
        slug_head = self.commit(target, 'slug branch unique commit\n')
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'cleared', closed['copy_reason'])
        bundle = self.folder(task) / 'branch.bundle'
        heads = self.git('bundle', 'list-heads', bundle).stdout
        self.assertIn(job_head + ' refs/heads/torii/task-' + str(task['id']), heads)
        self.assertIn(slug_head + ' refs/heads/' + branch, heads)

    def test_pr3_detached_head_and_slug_tip_are_bundled(self):
        task, branch, slug_head, _ = self.slug_job()
        target = self.target(task)
        self.git('add', '-A', cwd=target)
        self.git('commit', '-qm', 'another slug commit', cwd=target)
        slug_head = self.git('rev-parse', 'HEAD', cwd=target).stdout.strip()
        self.git('switch', '--detach', self.base, cwd=target)
        detached_head = self.commit(target, 'detached unique commit\n')
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'cleared', closed['copy_reason'])
        saved = self.manifest(task)
        self.assertEqual(saved['head'], detached_head)
        self.assertIn(detached_head + ' ' + saved['head_ref'],
                      self.git('bundle', 'list-heads', self.folder(task) / 'branch.bundle').stdout)

    def test_pr3_slug_checked_out_elsewhere_pauses_then_retries(self):
        task, branch, _, expected = self.slug_job()
        self.assertEqual(self.close(task)['copy'], 'cleared')
        other = self.root / 'other-copy'
        self.git('worktree', 'add', other, branch)
        self.call('tasks.update', task=task['id'], status='open')
        result = control_api.call(self.store, 'worktree.create', {'task': task['id']}, topic=task['topic'])
        self.assertFalse(result.ok)
        self.assertIn(str(self.folder(task)), result.text)
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'held')
        self.assertEqual((other / 'README.md').read_bytes(), b'unpushed slug commit\n')
        self.git('switch', '--detach', cwd=other)
        self.call('worktree.create', task=task['id'])
        self.assertEqual((self.target(task) / 'README.md').read_bytes(), expected['README.md'])

    def test_pr3_partial_ignored_restore_retries_on_slug(self):
        task, branch, _, expected = self.slug_job()
        target = self.target(task)
        (target / '.gitignore').write_text('.env\nnotes.private\n')
        (target / 'notes.private').write_bytes(b'ignored\xff')
        self.assertEqual(self.close(task)['copy'], 'cleared')
        self.call('tasks.update', task=task['id'], status='open')
        original = close_saves.copy_file

        def interrupted(source, destination, anchor):
            if destination == target / 'notes.private':
                raise OSError('fixture rebuild interruption')
            return original(source, destination, anchor)

        with patch.object(close_saves, 'copy_file', side_effect=interrupted):
            result = control_api.call(self.store, 'worktree.create', {'task': task['id']}, topic=task['topic'])
        self.assertFalse(result.ok)
        self.assertIn(str(self.folder(task)), result.text)
        self.assertEqual(self.git('symbolic-ref', '--short', 'HEAD', cwd=target).stdout.strip(), branch)
        self.call('worktree.create', task=task['id'])
        self.assertEqual((target / 'README.md').read_bytes(), expected['README.md'])
        self.assertEqual((target / 'notes.private').read_bytes(), b'ignored\xff')

    def test_pr3_staged_head_change_is_resaved_before_deletion(self):
        task, branch, head, _ = self.slug_job()
        original = close_saves.stage
        changed = []

        async def late_commit(*args):
            await original(*args)
            if not changed:
                changed.append(True)
                new_head = self.git('commit-tree', head + '^{tree}', '-p', head, '-m', 'late commit').stdout.strip()
                self.git('update-ref', 'refs/heads/' + branch, new_head, head)

        with patch.object(close_saves, 'stage', side_effect=late_commit):
            self.close(task)
        self.assertTrue(self.target(task).is_dir())
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'saving')
        self.drain()
        self.assert_cleared(task)
        self.assertEqual(self.manifest(task)['head'], self.git('rev-parse', branch).stdout.strip())

    def test_pr3_staged_index_only_bytes_block_deletion(self):
        task, _, _, _ = self.slug_job()
        content = self.root / 'index-only.bin'
        content.write_bytes(b'late index bytes')
        oid = self.git('hash-object', '-w', content).stdout.strip()
        original = close_saves.stage

        async def late_index(*args):
            await original(*args)
            stage, record = next(workspaces._deletion_stages(self.state))
            env = {'GIT_DIR': record['repository'], 'GIT_WORK_TREE': str(stage / 'copy'),
                   'GIT_INDEX_FILE': str(stage / 'git' / 'index')}
            await close_saves.git(stage / 'copy', 'update-index', '--add', '--cacheinfo',
                                   '100644,' + oid + ',late.bin', env=env)

        with patch.object(close_saves, 'stage', side_effect=late_index):
            self.close(task)
        stage, record = next(workspaces._deletion_stages(self.state))
        self.assertTrue((stage / 'copy' / 'README.md').is_file())
        self.assertIn('index holds unsaved changes', record['error'])
        self.assertNotEqual(self.store.task_get(task['id'])['copy'], 'cleared')
        env = {'GIT_DIR': record['repository'], 'GIT_WORK_TREE': str(stage / 'copy'),
               'GIT_INDEX_FILE': str(stage / 'git' / 'index')}
        _, saved_bytes = asyncio.run(close_saves.git(stage / 'copy', 'show', ':late.bin', env=env))
        self.assertEqual(saved_bytes, 'late index bytes')

    def test_pr3_slug_with_uninitialized_submodule_reopens(self):
        task, _ = self.submodule_job(initialized=False, closed=False)
        target = self.target(task)
        branch = 'torii/task-%s-submodule-slug' % task['id']
        self.git('switch', '-c', branch, cwd=target)
        self.commit(target)
        gitlink = self.git('ls-tree', 'HEAD', '--', 'sub', cwd=target).stdout
        (target / 'README.md').write_bytes(b'dirty slug with gitlink\xff')
        self.assertEqual(self.close(task)['copy'], 'cleared')
        self.reopen(task)
        self.assertEqual(self.git('symbolic-ref', '--short', 'HEAD', cwd=target).stdout.strip(), branch)
        self.assertEqual((target / 'README.md').read_bytes(), b'dirty slug with gitlink\xff')
        self.assertEqual(self.git('ls-tree', 'HEAD', '--', 'sub', cwd=target).stdout, gitlink)
        self.assertEqual(list((target / 'sub').iterdir()), [])



    def test_pr2_clean_uninitialized_submodule_saves_head_tree_and_reopens(self):
        task, _ = self.submodule_job(initialized=False, closed=False)
        target = self.target(task)
        head = self.git('rev-parse', 'HEAD', cwd=target).stdout.strip()
        head_tree = self.git('rev-parse', 'HEAD^{tree}', cwd=target).stdout.strip()
        self.assertEqual(self.git('write-tree', cwd=target).stdout.strip(), head_tree)
        gitlink = self.git('ls-tree', 'HEAD', '--', 'sub', cwd=target).stdout
        self.assertTrue(gitlink.startswith('160000 commit '), gitlink)
        expected = {name: (target / name).read_bytes() for name in ('.gitmodules', 'README.md', 'nested/keep.txt')}
        self.assertEqual(list((target / 'sub').iterdir()), [])
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'cleared', closed['copy_reason'])
        saved = self.manifest(task)
        self.assertEqual(saved['head'], head)
        self.assertEqual(saved['tree'], head_tree)
        self.assertIsNone(saved['wip'])
        self.assertEqual(self.git('ls-tree', saved['tree'], '--', 'sub').stdout, gitlink)
        self.assert_cleared(task)
        self.reopen(task)
        self.assertEqual(self.git('rev-parse', 'HEAD', cwd=target).stdout.strip(), head)
        self.assertEqual(self.git('write-tree', cwd=target).stdout.strip(), head_tree)
        self.assertEqual({name: (target / name).read_bytes() for name in expected}, expected)
        self.assertTrue((target / 'sub').is_dir())
        self.assertEqual(list((target / 'sub').iterdir()), [])

    def test_pr2_dirty_uninitialized_submodule_saves_wip_gitlink_and_reopens(self):
        task, _ = self.submodule_job(initialized=False, closed=False)
        target = self.target(task)
        gitlink = self.git('ls-tree', 'HEAD', '--', 'sub', cwd=target).stdout
        expected = {'README.md': b'dirty\r\nbytes\x00\xff\n', 'new.bin': b'new\x00\x80\n',
                    '.gitmodules': (target / '.gitmodules').read_bytes()}
        for name, content in expected.items():
            (target / name).write_bytes(content)
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'cleared', closed['copy_reason'])
        saved = self.manifest(task)
        self.assertTrue(saved['wip'])
        self.assertEqual(self.git('rev-parse', saved['wip'] + '^{tree}').stdout.strip(), saved['tree'])
        self.assertEqual(self.git('ls-tree', saved['tree'], '--', 'sub').stdout, gitlink)
        for name, content in expected.items():
            self.assertEqual(subprocess.run(('git', '-C', str(self.project), 'show', saved['wip'] + ':' + name),
                                            capture_output=True, check=True, timeout=30).stdout, content)
        self.assert_cleared(task)
        self.reopen(task)
        self.assertEqual({name: (target / name).read_bytes() for name in expected}, expected)
        self.assertEqual(self.git('ls-files', '--stage', '--', 'sub', cwd=target).stdout.split()[1],
                         gitlink.split()[2])
        self.assertTrue((target / 'sub').is_dir())
        self.assertEqual(list((target / 'sub').iterdir()), [])

    def test_pr2_initialized_submodule_stays_held(self):
        task, tip = self.submodule_job(closed=False)
        target = self.target(task)
        content = (target / 'sub' / 'library.txt').read_bytes()
        closed = self.close(task)
        self.assertEqual((closed['copy'], closed['copy_reason']), ('held', 'worktree has initialized submodules'))
        self.assertEqual(self.git('rev-parse', 'HEAD', cwd=target / 'sub').stdout.strip(), tip)
        self.assertEqual((target / 'sub' / 'library.txt').read_bytes(), content)

    def test_pr2_nonempty_uninitialized_submodule_keeps_unsaved_files(self):
        task, _ = self.submodule_job(initialized=False, closed=False)
        pending = self.target(task) / 'sub' / 'pending.bin'
        pending.write_bytes(b'unsaved\x00bytes')
        closed = self.close(task)
        self.assertEqual((closed['copy'], closed['copy_reason']),
                         ('held', 'Uninitialized submodule folder contains unsaved files'))
        self.assertEqual(pending.read_bytes(), b'unsaved\x00bytes')

    def test_pr2_orphan_submodule_git_data_stays_held(self):
        task, tip = self.submodule_job(closed=False)
        target = self.target(task)
        sub_git = self.git_directory(target / 'sub')
        self.git('submodule', 'deinit', '-f', 'sub', cwd=target)
        closed = self.close(task)
        self.assertEqual((closed['copy'], closed['copy_reason']),
                         ('held', 'Uninitialized or orphan submodule Git data needs preservation'))
        self.assertTrue(target.is_dir())
        self.assertEqual(self.git('--git-dir=' + str(sub_git), 'rev-parse', 'HEAD').stdout.strip(), tip)

    def test_review3030_a_removed_gitlink_with_files_switch_on(self):
        task, _ = self.submodule_job(initialized=False, closed=False)
        target = self.target(task)
        self.git('rm', '-q', '--cached', 'sub', cwd=target)
        pending = target / 'sub' / 'vendored.py'
        pending.write_bytes(b'precious worker work\n')
        closed = self.close(task)
        self.assertEqual((closed['copy'], closed['copy_reason']),
                         ('held', 'submodule folder holds unsaved files: sub'))
        self.assertEqual(pending.read_bytes(), b'precious worker work\n')
        self.assertTrue((target / '.gitmodules').is_file())

    def test_review3030_b_staged_gitlink_pointer_change_stays_held(self):
        task, _ = self.submodule_job(initialized=False, closed=False)
        target = self.target(task)
        new = 'deadbeef' * 5
        self.git('update-index', '--cacheinfo', '160000,%s,sub' % new, cwd=target)
        closed = self.close(task)
        self.assertEqual((closed['copy'], closed['copy_reason']),
                         ('held', 'submodule pointer changed: sub'))
        self.assertTrue(target.is_dir())
        self.assertEqual(self.git('ls-files', '--stage', '--', 'sub', cwd=target).stdout.split()[1], new)

    def test_review3030_d_removed_gitlink_and_gitmodules_switch_off(self):
        task, _ = self.submodule_job(initialized=False, closed=False)
        target = self.target(task)
        self.git('rm', '-q', '--cached', 'sub', cwd=target)
        self.git('rm', '-q', '.gitmodules', cwd=target)
        pending = target / 'sub' / 'vendored.py'
        pending.write_bytes(b'precious worker work\n')
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'held', closed['copy_reason'])
        self.assertEqual(pending.read_bytes(), b'precious worker work\n')
        self.assertFalse((target / '.gitmodules').exists())

    def test_reopened_saving_rows_starve_a_closed_job(self):
        stuck = [self.closed_then_reopened() for _ in range(5)]
        waiting = self.job()
        self.call('tasks.update', task=waiting['id'], status='done', summary='Closed in test.')
        results = [asyncio.run(workspaces.cleanup_closed_copies(self.store)) for _ in range(3)]
        print('cleanup results', results)
        print('stuck copies', [self.store.task_get(t['id'])['copy'] for t in stuck])
        self.assertEqual(self.store.task_get(waiting['id'])['copy'], 'cleared',
                         'closed job behind five reopened saving rows must still be saved')

    def test_reopened_saving_row_makes_cleanup_loop_spin(self):
        self.closed_then_reopened()
        storage = SimpleNamespace(reconcile=AsyncMock(), schedule=lambda: None)
        real = workspaces.cleanup_closed_copies
        passes = []

        async def counted(store, **options):
            passes.append(1)
            return await real(store, **options)

        async def exercise():
            event = asyncio.Event()
            fake = SimpleNamespace(store=self.store, wake=lambda name: event)
            try:
                await asyncio.wait_for(Service.worktree_cleanup_loop(fake), 1.0)
            except asyncio.TimeoutError:
                pass

        with patch('coordinator.worker_storage.storage_for', return_value=storage), \
                patch.object(workspaces, 'cleanup_closed_copies', new=counted):
            asyncio.run(exercise())
        print('cleanup passes in 1 s with one reopened saving row:', len(passes))
        self.assertLessEqual(len(passes), 3, 'loop must not rerun back to back for a row that cannot progress')

    def test_review3023_no_progress_batch_waits(self):
        task = self.job()
        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        with patch.object(close_saves, 'save_job', new=AsyncMock()) as save:
            self.assertFalse(asyncio.run(close_saves.drain(self.store)))
            self.assertTrue(close_saves.pending(self.store))
            self.assertFalse(asyncio.run(workspaces.cleanup_closed_copies(self.store)))
        self.assertEqual(save.await_count, 2)
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'saving')

    def test_drain_does_not_repeat_when_only_unrelated_state_changes(self):
        task = self.job()
        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')

        async def no_progress(store, task_id):
            with store.db:
                store.put('unrelated_cleanup_setting', time.time())

        with patch.object(close_saves, 'save_job', side_effect=no_progress) as save:
            self.assertFalse(asyncio.run(close_saves.drain(self.store)))
        self.assertEqual(save.await_count, 1)
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'saving')

    def test_pr1_git_timeout_has_command_and_duration(self):
        task = self.job()
        (self.target(task) / 'untracked').write_bytes(b'pending snapshot')
        original = workspaces._git

        async def timeout(cwd, *args, **options):
            if args[0] == 'add':
                await asyncio.wait_for(asyncio.Event().wait(), 0.01)
            return await original(cwd, *args, **options)

        with patch.object(workspaces, '_git', side_effect=timeout):
            closed = self.close(task)
        self.assertEqual(closed['copy_reason'], 'git add timed out after 30s')
        self.assertEqual(closed['copy'], 'held')
        self.assertTrue(self.target(task).is_dir())

    def test_pr1_blank_save_error_uses_exception_name(self):
        task = self.job()
        with patch.object(close_saves, 'snapshot', side_effect=ValueError()):
            closed = self.close(task)
        self.assertEqual(closed['copy_reason'], 'ValueError')

    def test_pr1_untracked_cap_holds_before_wip_objects(self):
        task = self.job()
        output = self.target(task) / 'results' / 'large.bin'
        output.parent.mkdir()
        size = 300 * 1024 * 1024
        with output.open('wb') as handle:
            handle.truncate(size)
        before = self.git('count-objects', '-v').stdout
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'held')
        self.assertEqual(closed['copy_reason'], 'untracked files exceed 200 MB: results (%d total)' % size)
        self.assertEqual(self.git('count-objects', '-v').stdout, before)
        self.assertEqual(self.git('show-ref', '--verify', '--quiet', 'refs/torii/wip/task-' + str(task['id']), check=False).returncode, 1)

    def test_pr1_untracked_cap_sums_files_and_excludes_symlinks(self):
        task = self.job()
        target = self.target(task)
        (target / 'largest').mkdir()
        (target / 'largest' / 'one').write_bytes(b'a' * 30)
        (target / 'largest' / 'two').write_bytes(b'b' * 30)
        (target / 'smaller').write_bytes(b'c' * 20)
        with patch.object(cleanup, 'UNTRACKED_LIMIT', 70):
            closed = self.close(task)
        self.assertIn('largest (80 total)', closed['copy_reason'] or '')
        outside = self.root / 'large-outside'
        with outside.open('wb') as handle:
            handle.truncate(300 * 1024 * 1024)
        (target / 'link').symlink_to(outside)
        with patch.object(cleanup, 'UNTRACKED_LIMIT', 80):
            self.call('tasks.update', task=task['id'], note='retry with exact limit')
            self.drain()
        self.assert_cleared(task)
        self.assertEqual(outside.stat().st_size, 300 * 1024 * 1024)

    def test_pr1_hold_problem_is_once_per_task_and_new_reason(self):
        task = self.job()
        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        for reason in ('first reason', 'first reason', 'second reason', 'second reason', 'first reason'):
            with self.store.db:
                close_saves.request(self.store, task['id'])
            with patch.object(close_saves, 'snapshot', side_effect=ValueError(reason)):
                self.drain()
        rows = self.store.db.execute("SELECT detail,topic,task FROM problems WHERE area='cleanup' AND code='copy-held' ORDER BY id").fetchall()
        self.assertEqual([tuple(row) for row in rows],
                         [('first reason', task['topic'], task['id']), ('second reason', task['topic'], task['id'])])

    def test_pr1_finish_reports_nonblank_reason_once(self):
        task = self.job()
        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        for failure in (RuntimeError(), RuntimeError(), asyncio.TimeoutError()):
            with self.store.db:
                self.store.db.execute("UPDATE tasks SET copy='saving' WHERE id=?", (task['id'],))
            with patch.object(close_saves, 'clear_extras', side_effect=failure):
                asyncio.run(close_saves.finish(self.store))
            self.assertTrue(self.store.task_get(task['id'])['copy_reason'])
        rows = self.store.db.execute("SELECT detail FROM problems WHERE area='cleanup' AND code='copy-held' ORDER BY id").fetchall()
        self.assertEqual([row['detail'] for row in rows], ['RuntimeError', 'TimeoutError'])

    def test_pr1_seven_saves_release_lock_between_two_batches(self):
        tasks = [self.job() for _ in range(7)]
        for task in tasks:
            self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        with patch.object(close_saves, 'DRAIN_SECONDS', 0):
            more = asyncio.run(workspaces.cleanup_closed_copies(self.store))
        self.assertTrue(more)
        self.assertEqual([self.store.task_get(task['id'])['copy'] for task in tasks], ['cleared'] * 5 + ['saving'] * 2)

        async def acquire_between_batches():
            async with workspaces._repository_lock(self.state, 'closed-copy-deletion'):
                return True

        self.assertTrue(asyncio.run(asyncio.wait_for(acquire_between_batches(), 1)))
        self.assertFalse(asyncio.run(workspaces.cleanup_closed_copies(self.store)))
        for task in tasks:
            self.assert_cleared(task)

    def test_pr1_drain_limit_and_backlog_result(self):
        tasks = [self.job() for _ in range(3)]
        for task in tasks:
            self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')

        async def hold(store, task_id):
            with store.db:
                store.db.execute("UPDATE tasks SET copy='held' WHERE id=?", (task_id,))

        with patch.object(close_saves, 'save_job', side_effect=hold) as save:
            self.assertTrue(asyncio.run(close_saves.drain(self.store, limit=2)))
            self.assertFalse(asyncio.run(close_saves.drain(self.store, limit=2)))
        self.assertEqual([call.args[1] for call in save.call_args_list], [task['id'] for task in tasks])

    def unrecorded_job(self):
        task = self.job()
        with self.store.db:
            self.store.db.execute('UPDATE tasks SET worktree=NULL WHERE id=?', (task['id'],))
        return self.store.task_get(task['id'])

    def test_unrecorded_bare_worktree_is_saved_and_cleared(self):
        task = self.unrecorded_job()
        target = self.target(task)
        self.git('rm', '-rq', '.', cwd=target)
        self.git('commit', '-qm', 'empty tree', cwd=target)
        head = self.git('rev-parse', 'HEAD', cwd=target).stdout.strip()
        self.assertEqual([path.name for path in target.iterdir()], ['.git'])
        self.assert_cleared(self.close(task))
        self.assertEqual(self.manifest(task)['head'], head)

    def test_unrecorded_folder_with_broken_link_goes_to_the_trash(self):
        task = self.call('tasks.create', title='Research').data
        target = self.target(task)
        target.mkdir(parents=True)
        (target / '.git').write_text('gitdir: ' + str(self.root / 'gone' / '.git' / 'worktrees' / target.name) + '\n')
        current = self.close(task)
        self.assertEqual(current['copy'], 'cleared', current['copy_reason'])
        self.assertFalse(target.exists())
        self.assertTrue((self.trash / (target.name + '-unrecorded') / '.git').is_file())
        self.assertIn('broken Git link', current['notes'])

    def test_unrecorded_folder_with_files_holds_the_job(self):
        task = self.unrecorded_job()
        current = self.close(task)
        self.assertEqual(current['copy'], 'held')
        self.assertIn('no recorded worktree', current['copy_reason'])
        self.assertEqual((self.target(task) / 'README.md').read_text(), 'base\n')

    def test_unexpected_error_backs_off_one_job_and_the_drain_goes_on(self):
        bad, good = self.job(), self.job()
        for task in (bad, good):
            self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        real = close_saves.prepare_preconditions

        async def preconditions(store, task):
            if task['id'] == bad['id']:
                raise TypeError('fixture unexpected failure')
            return await real(store, task)

        with patch.object(close_saves, 'prepare_preconditions', side_effect=preconditions):
            self.drain()
        self.assertEqual(self.store.task_get(bad['id'])['copy'], 'saving')
        [detail] = [row[0] for row in self.store.db.execute(
            "SELECT detail FROM problems WHERE area='cleanup' AND code='close-save-failed' AND task=?", (bad['id'],))]
        self.assertRegex(detail, r'^item=%d failures=1 TypeError: fixture unexpected failure at '
                                 r'test_close_saves\.py:\d+ preconditions < ' % bad['id'])
        self.assertTrue(self.target(bad).exists())
        self.assert_cleared(self.store.task_get(good['id']))
        with patch.object(close_saves, 'save_job') as save:
            self.assertFalse(asyncio.run(close_saves.drain(self.store)))
        save.assert_not_called()

    def test_worktree_record_failure_asks_for_a_retry_that_adopts_the_folder(self):
        task = self.call('tasks.create', title='Dev').data
        with patch.object(self.store, 'task_update', side_effect=control_api.sqlite3.OperationalError('database is locked')):
            with self.store.db:
                result = control_api.call(self.store, 'worktree.create', {'task': task['id']}, topic=fixtures.TOPIC)
        self.assertFalse(result.ok)
        self.assertIn('Run worktree.create again', result.text)
        self.assertTrue(self.target(task).is_dir())
        self.assertIsNone(self.store.task_get(task['id'])['worktree'])
        self.call('worktree.create', task=task['id'])
        self.assertEqual(self.store.task_get(task['id'])['worktree'], str(self.target(task)))

    def test_pr1_staged_retries_do_not_starve_batches_or_force_busy_loop(self):
        tasks = [self.job() for _ in range(6)]
        for task in tasks:
            self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        with patch.object(workspaces, 'delete_tree', side_effect=OSError('fixture delayed deletion')), \
                patch('coordinator.workspaces.time.time', return_value=time.time()), \
                patch.object(close_saves, 'DRAIN_SECONDS', 0):
            self.assertTrue(asyncio.run(workspaces.cleanup_closed_copies(self.store)))
            self.assertEqual([close_saves.load(self.store, task)[0] for task in tasks], ['staged'] * 5 + ['pending'])
            self.assertFalse(asyncio.run(workspaces.cleanup_closed_copies(self.store)))
            self.assertEqual([close_saves.load(self.store, task)[0] for task in tasks], ['staged'] * 6)
            self.assertGreater(asyncio.run(close_saves.deletion_delay(self.store)), 0)

    def test_pr1_service_continues_backlog_without_waiting_for_wake(self):
        storage = SimpleNamespace(reconcile=AsyncMock(), schedule=lambda: None)
        delays = []

        async def sleep(delay):
            delays.append(delay)
            raise asyncio.CancelledError()

        async def exercise():
            event = asyncio.Event()
            fake = SimpleNamespace(store=self.store, wake=lambda name: event)
            await asyncio.wait_for(Service.worktree_cleanup_loop(fake), 0.2)

        with patch('coordinator.worker_storage.storage_for', return_value=storage), \
                patch.object(workspaces, 'cleanup_closed_copies', new=AsyncMock(return_value=True)), \
                patch.object(close_saves, 'deletion_delay', return_value=None), \
                patch.object(close_saves, 'fade_results', new=AsyncMock(return_value=False)), \
                patch('coordinator.service.asyncio.sleep', side_effect=sleep):
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(exercise())
        self.assertEqual(delays, [0])

    def test_close_save_backlog_runs_fade_and_idle_trim_once_an_hour(self):
        from coordinator import idle_trim
        self.store.put('idle_trim', 24)
        storage = SimpleNamespace(reconcile=AsyncMock(), schedule=lambda: None)
        real_sleep = asyncio.sleep
        fades, trims = [], []

        async def sleep(delay, *args):
            if delay:
                raise asyncio.CancelledError()
            return await real_sleep(0)

        async def fade(store):
            fades.append(1)
            return False

        async def exercise():
            event = asyncio.Event()
            fake = SimpleNamespace(store=self.store, wake=lambda name: event)
            await asyncio.wait_for(Service.worktree_cleanup_loop(fake), 5)

        with patch('coordinator.worker_storage.storage_for', return_value=storage), \
                patch.object(workspaces, 'cleanup_closed_copies', new=AsyncMock(side_effect=[True, True, True, False])), \
                patch.object(close_saves, 'fade_results', new=fade), \
                patch.object(idle_trim, 'sweep', side_effect=lambda directory: trims.append(1)), \
                patch.object(close_saves, 'deletion_delay', return_value=None), \
                patch('coordinator.service.asyncio.sleep', side_effect=sleep):
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(exercise())
        self.assertEqual((len(fades), len(trims)), (2, 2))

    def test_newly_closed_job_clears_while_slow_sweeps_run(self):
        from coordinator import idle_trim
        self.store.put('idle_trim', 24)
        task = self.job()
        started, release = threading.Event(), threading.Event()

        def slow_sweep(directory):
            started.set()
            release.wait()

        service = Service.__new__(Service)
        service.store = self.store
        storage = SimpleNamespace(reconcile=AsyncMock(), schedule=lambda: None)

        async def exercise():
            loops = [asyncio.create_task(service.worktree_cleanup_loop())]
            try:
                loops.append(asyncio.create_task(service.close_save_loop()))
                async def wait_until(predicate):
                    while not predicate():
                        await asyncio.sleep(0.02)

                await asyncio.wait_for(wait_until(started.is_set), 120)
                self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
                service.wake('worktree_cleanup').set()
                service.wake('close_save').set()
                await asyncio.wait_for(wait_until(lambda: self.store.task_get(task['id'])['copy'] == 'cleared'), 120)
                return self.store.task_get(task['id'])['copy']
            finally:
                release.set()
                for loop in loops:
                    loop.cancel()
                await asyncio.gather(*loops, return_exceptions=True)

        with patch('coordinator.worker_storage.storage_for', return_value=storage), \
                patch.object(idle_trim, 'sweep', side_effect=slow_sweep):
            self.assertEqual(asyncio.run(exercise()), 'cleared')
        self.assert_cleared(task)

    def idle_open_job(self, hours):
        task = self.job(closed=False)
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        (scratch / 'proofs').mkdir()
        (scratch / 'proofs' / 'shot.png').write_bytes(b'png')
        (scratch / 'notes.txt').write_text('notes')
        with self.store.db:
            self.store.db.execute('UPDATE tasks SET updated=? WHERE id=?', (time.time() - hours * 3600, task['id']))
        return self.store.task_get(task['id']), scratch

    def assert_park_kept(self, task, scratch, detail, copy='present'):
        self.assertEqual(self.store.task_get(task['id'])['copy'], copy)
        self.assertEqual((scratch / 'notes.txt').read_text(), 'notes')
        self.assertTrue(self.target(task).is_dir())
        rows = self.store.db.execute("SELECT detail FROM problems WHERE area='cleanup' AND code='park-in-use' AND task=?",
                                     (task['id'],)).fetchall()
        self.assertEqual([row['detail'] for row in rows], [detail])

    def test_idle_park_and_trim_keep_job_with_live_scratch_cwd(self):
        task, scratch = self.idle_open_job(49)
        with subprocess.Popen(['/bin/sleep', '60'], cwd=scratch) as process:
            try:
                with patch.object(job_limits, 'low_disk', return_value=False):
                    job_limits.sweep(self.state, now=time.time() + 49 * 3600)
                self.drain()
                self.assertFalse(close_saves.parked(self.store, task))
                self.assert_park_kept(task, scratch, 'process pid %d' % process.pid)
            finally:
                process.terminate()

    def test_parked_save_keeps_job_when_a_process_starts_after_parking(self):
        task, scratch = self.idle_open_job(49)
        self.assertEqual(close_saves.park_idle(self.store), [task['id']])
        with subprocess.Popen(['/bin/sleep', '60'], cwd=scratch) as process:
            try:
                self.drain()
                self.assert_park_kept(task, scratch, 'process pid %d' % process.pid, copy='held')
            finally:
                process.terminate()

    def test_idle_park_keeps_job_referenced_by_user_timer_service(self):
        task, scratch = self.idle_open_job(49)
        config = self.root / 'config'
        units = config / 'systemd/user'
        units.mkdir(parents=True)
        unit = units / 'watchdog.service'
        (units / 'watchdog.timer').write_text('[Timer]\nOnUnitActiveSec=1min\n')
        enabled = units / 'timers.target.wants'
        enabled.mkdir()
        (enabled / 'watchdog.timer').symlink_to('../watchdog.timer')
        with patch.object(job_limits.host_os, 'linux', return_value=True), \
                patch.object(job_limits, 'open_paths', return_value={}), \
                patch.object(job_limits.shutil, 'which', return_value=None), \
                patch.dict(os.environ, XDG_CONFIG_HOME=str(config), XDG_RUNTIME_DIR=str(self.root / 'runtime'),
                           DBUS_SESSION_BUS_ADDRESS=''):
            for directive in ('ExecStart=/usr/bin/python3 "%s/msa2_watch.py"', 'WorkingDirectory=-%s'):
                with self.subTest(directive=directive):
                    unit.write_text('[Service]\n' + (directive % scratch) + '\n')
                    self.assertEqual(close_saves.park_idle(self.store), [])
        self.assert_park_kept(task, scratch, 'systemd user unit watchdog.service')

    def test_idle_park_keeps_job_when_process_scan_fails(self):
        task, scratch = self.idle_open_job(49)
        with patch.object(job_limits, 'open_paths', side_effect=OSError('probe failed')):
            for _ in range(2):
                self.assertEqual(close_saves.park_idle(self.store), [])
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'present')
        self.assertTrue((scratch / 'notes.txt').is_file())
        self.assertTrue(self.target(task).is_dir())
        rows = self.store.db.execute("SELECT code,detail FROM problems WHERE area='cleanup'")
        self.assertEqual([tuple(row) for row in rows], [('path-use-scan-failed', 'process path use scan failed (OSError)')])

    def test_open_job_idle_for_48_hours_is_parked_then_rebuilt(self):
        recent, recent_scratch = self.idle_open_job(47)
        kept, kept_scratch = self.idle_open_job(49)
        task, scratch = self.idle_open_job(49)
        with self.store.db:
            self.store.put('close_saves_keep', [kept['id']])
        job_limits.sweep(self.state)
        self.drain()
        for other, other_scratch in ((recent, recent_scratch), (kept, kept_scratch)):
            self.assertEqual(self.store.task_get(other['id'])['copy'], 'present')
            self.assertTrue((other_scratch / 'notes.txt').exists())
            self.assertTrue(self.target(other).is_dir())
        current = self.store.task_get(task['id'])
        self.assertEqual((current['status'], current['copy']), ('open', 'cleared'))
        self.assertFalse(scratch.exists())
        self.assertFalse(self.target(task).exists())
        entry = next(p for p in self.manifest(task)['proofs'] if p['source'] == str(scratch / 'proofs' / 'shot.png'))
        self.assertEqual((self.folder(task) / entry['path']).read_bytes(), b'png')
        self.call('worktree.create', task=task['id'])
        self.assertEqual((self.target(task) / 'README.md').read_text(), 'base\n')
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'present')
        self.assertIsNone(self.store.db.execute('SELECT 1 FROM task_parks WHERE task=?', (task['id'],)).fetchone())

    def test_reopened_saving_job_is_parked_and_rebuilt_on_its_branch(self):
        task = self.closed_then_reopened()
        head = self.commit(self.target(task), 'reopened work\n')
        with self.store.db:
            self.store.db.execute('UPDATE tasks SET updated=? WHERE id=?', (time.time() - 49 * 3600, task['id']))
        with patch.object(job_limits, 'open_paths', return_value={}), \
                patch.object(job_limits, 'systemd_references', return_value=()):
            job_limits.sweep(self.state)
            self.drain()
            self.drain()
        current = self.store.task_get(task['id'])
        self.assertEqual(current['copy'], 'cleared', current['copy_reason'])
        self.assertFalse(self.target(task).exists())
        self.call('worktree.create', task=task['id'])
        target = self.target(task)
        self.assertEqual(self.git('branch', '--show-current', cwd=target).stdout.strip(), 'torii/task-%s' % task['id'])
        self.assertEqual(self.git('rev-parse', 'HEAD', cwd=target).stdout.strip(), head)
        self.assertEqual((target / 'README.md').read_text(), 'reopened work\n')

    def test_new_worker_unparks_a_job_before_its_clear(self):
        from coordinator import idle_trim, job_limits
        task, scratch = self.idle_open_job(49)
        job_limits.sweep(self.state)
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'saving')
        worker = self.worker(task['id'], self.target(task), status='queued')
        with self.store.db:
            idle_trim.mark_worker(self.store, task['id'], worker)
        self.drain()
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'present')
        self.assertTrue((scratch / 'notes.txt').exists())
        self.assertTrue(self.target(task).is_dir())

    def park(self, task, scratch):
        from coordinator import job_limits
        stamp = time.time() - 49 * 3600
        with self.store.db:
            self.store.db.execute('UPDATE tasks SET updated=? WHERE id=?', (stamp, task['id']))
        job_limits.sweep(self.state)
        self.drain()
        self.drain()

    def scratch_git_saves(self, task):
        current = self.store.task_get(task['id'])
        records = sorted(self.folder(current).glob('scratch-git/*/scratch-git.json'))
        self.assertEqual(len(records), 1)
        return records[0].parent, json.loads(records[0].read_text())

    def bundle_has(self, bundle, commit, base=None):
        clone = self.root / ('bundle-check-' + commit[:8])
        subprocess.run(('git', 'clone', '-q', str(base), str(clone)) if base else ('git', 'init', '-q', str(clone)), check=True)
        self.git('fetch', '-q', bundle, '+refs/*:refs/saved/*', 'HEAD:refs/saved/HEAD', cwd=clone, check=False)
        return self.git('cat-file', '-e', commit + '^{commit}', cwd=clone, check=False).returncode == 0

    def test_park_saves_scratch_clone_commits_before_the_clear(self):
        task, scratch = self.idle_open_job(49)
        repo = scratch / 'model'
        repo.mkdir()
        for args in (('init', '-q'), ('config', 'user.name', 'T'), ('config', 'user.email', 't@example.com')):
            self.git(*args, cwd=repo)
        (repo / 'run.txt').write_text('run 13\n')
        self.git('add', '.', cwd=repo)
        self.git('commit', '-qm', 'run 13', cwd=repo)
        commit = self.git('rev-parse', 'HEAD', cwd=repo).stdout.strip()
        for text in ('stash 1\n', 'stash 2\n'):
            (repo / 'run.txt').write_text(text)
            self.git('stash', '-q', cwd=repo)
        older_stash = self.git('rev-parse', 'stash@{1}', cwd=repo).stdout.strip()
        (repo / 'run.txt').write_text('run 14\n')
        (repo / 'new.txt').write_text('untracked\n')
        self.park(task, scratch)
        self.assertFalse(scratch.exists())
        folder, record = self.scratch_git_saves(task)
        [tree] = record['trees']
        self.assertEqual((tree['path'], tree['head']), ('model', commit))
        self.assertTrue(self.bundle_has(folder / tree['bundle'], commit))
        self.assertIn(older_stash, tree['stashes'])
        self.assertTrue(self.bundle_has(folder / tree['bundle'], older_stash))
        patch_text = (folder / tree['patch']).read_text()
        self.assertIn('+run 14', patch_text)
        self.assertIn('+untracked', patch_text)
        self.assertIn(str(folder), self.store.task_get(task['id'])['notes'])

    def test_park_saves_detached_scratch_worktree_commit_before_the_clear(self):
        task, scratch = self.idle_open_job(49)
        stage = scratch / 'stage3-work'
        self.git('worktree', 'add', '-q', '--detach', stage)
        (stage / 'README.md').write_text('stage 3\n')
        self.git('commit', '-qam', 'stage 3', cwd=stage)
        head = self.git('rev-parse', 'HEAD', cwd=stage).stdout.strip()
        (stage / 'README.md').write_text('stage 3 edit\n')
        self.park(task, scratch)
        self.assertFalse(scratch.exists())
        folder, record = self.scratch_git_saves(task)
        [tree] = record['trees']
        self.assertEqual((tree['path'], tree['head']), ('stage3-work', head))
        self.assertTrue(self.bundle_has(folder / tree['bundle'], head, base=self.project))
        self.assertIn('+stage 3 edit', (folder / tree['patch']).read_text())

    def test_park_pins_scratch_reflog_only_commits_and_clears(self):
        task, scratch = self.idle_open_job(49)
        repo = scratch / 'wt'
        repo.mkdir()
        for args in (('init', '-q'), ('config', 'user.name', 'T'), ('config', 'user.email', 't@example.com')):
            self.git(*args, cwd=repo)
        (repo / 'run.txt').write_text('base\n')
        self.git('add', '.', cwd=repo)
        self.git('commit', '-qm', 'base', cwd=repo)
        (repo / 'run.txt').write_text('amended away\n')
        self.git('commit', '-qam', 'lost', cwd=repo)
        lost = self.git('rev-parse', 'HEAD', cwd=repo).stdout.strip()
        self.git('reset', '-q', '--hard', 'HEAD~1', cwd=repo)
        self.park(task, scratch)
        self.assertFalse(scratch.exists())
        folder, record = self.scratch_git_saves(task)
        [tree] = record['trees']
        self.assertEqual(tree['reflog_refs'], [close_saves.SCRATCH_REFLOG + lost])
        self.assertTrue(self.bundle_has(folder / tree['bundle'], lost))

    def test_park_pins_linked_scratch_worktree_reflog_commit_in_its_repository(self):
        task, scratch = self.idle_open_job(49)
        stage = scratch / 'review'
        self.git('worktree', 'add', '-q', '--detach', stage)
        lost = self.commit(stage, 'amended away\n')
        self.git('reset', '-q', '--hard', 'HEAD~1', cwd=stage)
        self.park(task, scratch)
        self.assertFalse(scratch.exists())
        _, record = self.scratch_git_saves(task)
        self.assertEqual(record['trees'][0]['reflog_refs'], [close_saves.SCRATCH_REFLOG + lost])
        self.assertEqual(self.git('rev-parse', close_saves.SCRATCH_REFLOG + lost).stdout.strip(), lost)

    def scratch_repo(self, scratch, name, text='run 13\n'):
        repo = scratch / name
        repo.mkdir()
        for args in (('init', '-q'), ('config', 'user.name', 'T'), ('config', 'user.email', 't@example.com')):
            self.git(*args, cwd=repo)
        (repo / 'run.txt').write_text(text)
        self.git('add', '.', cwd=repo)
        self.git('commit', '-qm', 'run', cwd=repo)
        return repo, self.git('rev-parse', 'HEAD', cwd=repo).stdout.strip()

    def test_park_keeps_scratch_when_a_git_save_fails(self):
        from coordinator import idle_trim
        task, scratch = self.idle_open_job(49)
        self.scratch_repo(scratch, 'model')
        broken = scratch / 'router'
        broken.mkdir()
        (broken / '.git').write_text('gitdir: ' + str(self.root / 'missing-git-dir') + '\n')
        (broken / 'work.txt').write_text('work')

        def kept(copy):
            current = self.store.task_get(task['id'])
            self.assertEqual((current['status'], current['copy']), ('open', copy))
            self.assertEqual(list(self.folder(current).glob('scratch-git/*')), [])
            self.assertTrue((broken / 'work.txt').exists())
            self.assertTrue((scratch / 'notes.txt').exists())
            self.assertFalse(self.target(task).exists())
            return current

        self.park(task, scratch)
        held = kept('held')
        self.assertIn('router', held['copy_reason'])
        parked = self.store.db.execute('SELECT parked FROM task_parks WHERE task=?', (task['id'],)).fetchone()['parked']
        rows = self.store.db.execute("SELECT detail FROM problems WHERE area='cleanup' AND code='copy-held' AND task=?",
                                     (task['id'],)).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertIn('router', rows[0]['detail'])
        for copy in ('held', 'cleared'):
            with self.subTest(copy=copy):
                with self.store.db:
                    self.store.db.execute('UPDATE tasks SET copy=? WHERE id=?', (copy, task['id']))
                self.assertEqual(close_saves.park_idle(self.store, now=time.time() + 49 * 3600), [])
                self.park(task, scratch)
                kept(copy)
                self.assertEqual(self.store.db.execute('SELECT parked FROM task_parks WHERE task=?',
                                                       (task['id'],)).fetchone()['parked'], parked)
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET copy='held' WHERE id=?", (task['id'],))
        since = self.store.get('close_save_holds')[str(task['id'])]['since']
        asyncio.run(close_saves.retry_holds(self.store, now=since + 3599))
        kept('held')
        asyncio.run(close_saves.retry_holds(self.store, now=since + 3600))
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'saving')
        self.drain()
        kept('held')
        asyncio.run(close_saves.retry_holds(self.store, now=since + 3600 + 7199))
        kept('held')
        self.assertEqual(len(self.store.db.execute(
            "SELECT 1 FROM problems WHERE area='cleanup' AND code='copy-held' AND task=?", (task['id'],)).fetchall()), 1)
        worker = self.worker(task['id'], self.target(task), status='queued')
        with self.store.db:
            idle_trim.mark_worker(self.store, task['id'], worker)
        self.assertFalse(close_saves.parked(self.store, self.store.task_get(task['id'])))
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'cleared')
        self.assertTrue((broken / 'work.txt').exists())

    def test_park_clears_scratch_with_an_empty_git_marker_file(self):
        task, scratch = self.idle_open_job(49)
        cache = scratch / 'uvcache' / 'sdists-v10'
        cache.mkdir(parents=True)
        (cache / '.git').write_bytes(b'')
        self.assertEqual(close_saves.scan_git_trees(scratch), [])
        self.park(task, scratch)
        self.assertFalse(scratch.exists())
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'cleared')
        self.assertIsNone(self.store.db.execute("SELECT 1 FROM problems WHERE code='scratch-git-kept'").fetchone())

    def test_repeated_scratch_save_links_the_bundle_and_only_a_redundant_save_goes(self):
        task, scratch = self.idle_open_job(49)
        repo, commit = self.scratch_repo(scratch, 'model')
        (repo / 'draft.txt').write_text('uncommitted work')
        folder = self.folder(task)
        first = Path(asyncio.run(close_saves.save_scratch_git(self.store, scratch, folder)))
        second = Path(asyncio.run(close_saves.save_scratch_git(self.store, scratch, folder)))
        self.assertEqual((second / '0.bundle').stat().st_ino, (first / '0.bundle').stat().st_ino)
        repacked, half, changed = (folder / 'scratch-git' / name for name in ('0-repacked', '0-half', '0-changed'))
        for path in (repacked, half, changed):
            path.mkdir()
            shutil.copy(second / '0.patch', path / '0.patch')
        self.git('-c', 'pack.compression=0', 'bundle', 'create', '-q', str(repacked / '0.bundle'), '--all', cwd=repo)
        self.assertNotEqual((repacked / '0.bundle').read_bytes(), (second / '0.bundle').read_bytes())
        (half / '0.bundle').write_bytes(b'half')
        shutil.copy(second / '0.bundle', changed / '0.bundle')
        (changed / '0.patch').write_text('another draft')
        (repo / 'run.txt').write_text('run 14\n')
        self.git('commit', '-qam', 'run 14', cwd=repo)
        third = Path(asyncio.run(close_saves.save_scratch_git(self.store, scratch, folder)))
        self.assertNotEqual((third / '0.bundle').stat().st_ino, (first / '0.bundle').stat().st_ino)
        with self.store.db:
            self.store.db.execute("INSERT INTO task_space_saves(task,k,phase,data) VALUES (?,?,'extras',?)",
                                  (task['id'], task['iteration'], json.dumps({'scratch_git': [str(second)]})))
        asyncio.run(close_saves.discard_scratch_saves(self.store))
        self.assertEqual(sorted(folder.glob('scratch-git/*')), sorted((second, third, half, changed)))
        self.assertTrue(self.bundle_has(second / '0.bundle', commit))
        [trash] = self.trash.glob('torii-trim-task-%d-results-*' % task['id'])
        moved = trash / 'rounds' / str(task['iteration']) / 'scratch-git'
        self.assertEqual(sorted(path.name for path in moved.iterdir()), sorted((first.name, repacked.name)))
        self.assertTrue((moved / first.name / 'scratch-git.json').is_file())

    def test_newer_partial_save_never_stands_in_for_the_only_complete_save(self):
        sets = self.state / 'results' / 'task-1' / 'rounds' / '1' / 'scratch-git'
        old, new, newest = (sets / name for name in ('20261101-000000-aaaaaa', '20261102-000000-bbbbbb',
                                                     '20261103-000000-cccccc'))
        for save in (old, new):
            save.mkdir(parents=True)
            (save / '0.patch').write_text('diff')
        (old / 'scratch-git.json').write_text('{"trees": [{"path": "repo", "head": "abc"}]}')
        self.assertEqual(close_saves.redundant_scratch_saves(self.state / 'results', set()), {})
        newest.mkdir()
        (newest / '0.patch').write_text('diff')
        (newest / 'scratch-git.json').write_text('{"trees": [{"path": "repo", "head": "abc"}]}')
        self.assertEqual(close_saves.redundant_scratch_saves(self.state / 'results', set()), {1: [str(new), str(old)]})

    def test_finish_keeps_the_only_scratch_save_of_a_cleared_job_without_its_record(self):
        task, scratch = self.idle_open_job(49)
        repo, commit = self.scratch_repo(scratch, 'model')
        (repo / 'draft.txt').write_text('uncommitted work')
        self.park(task, scratch)
        current = self.store.task_get(task['id'])
        self.assertEqual(current['copy'], 'cleared')
        self.assertFalse(scratch.exists())
        save, _ = self.scratch_git_saves(task)
        row = self.store.db.execute('SELECT k,data FROM task_space_saves WHERE task=?', (task['id'],)).fetchone()
        data = json.loads(row['data'])
        data.pop('scratch_git')
        with self.store.db:
            self.store.db.execute('UPDATE task_space_saves SET data=? WHERE task=? AND k=?',
                                  (json.dumps(data), task['id'], row['k']))
        self.drain()
        self.assertTrue((save / 'scratch-git.json').is_file())
        self.assertTrue(self.bundle_has(save / '0.bundle', commit))
        self.assertEqual(list(self.trash.glob('torii-trim-task-%d-results-*' % task['id'])), [])

    def test_second_park_of_a_round_keeps_the_record_of_the_first_scratch_save(self):
        task, scratch = self.idle_open_job(49)
        self.scratch_repo(scratch, 'model')
        self.park(task, scratch)
        save, _ = self.scratch_git_saves(task)
        self.call('worktree.create', task=task['id'])
        self.park(task, scratch)
        current = self.store.task_get(task['id'])
        self.assertEqual((current['copy'], current['iteration']), ('cleared', task['iteration']), current['copy_reason'])
        row = self.store.db.execute('SELECT data FROM task_space_saves WHERE task=? AND k=?',
                                    (task['id'], task['iteration'])).fetchone()
        self.assertEqual(json.loads(row['data'])['scratch_git'], [str(save)])

    def test_worker_after_scratch_only_stage_does_not_leave_copy_saving(self):
        from coordinator import idle_trim, job_limits
        task = self.call('tasks.create', title='Research').data
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        (scratch / 'notes.txt').write_text('notes')
        with self.store.db:
            self.store.db.execute('UPDATE tasks SET updated=? WHERE id=?', (time.time() - 49 * 3600, task['id']))
        job_limits.sweep(self.state)
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'saving')
        asyncio.run(close_saves.drain(self.store))
        phase = self.store.db.execute('SELECT phase FROM task_space_saves WHERE task=?', (task['id'],)).fetchone()[0]
        self.assertEqual(phase, 'staged')
        worker = self.worker(task['id'], scratch, status='queued')
        with self.store.db:
            idle_trim.mark_worker(self.store, task['id'], worker)
        self.drain()
        self.assertTrue((scratch / 'notes.txt').exists())
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'present')

    def test_worker_after_a_parked_worktree_was_moved_marks_the_copy_cleared(self):
        from coordinator import idle_trim
        task, scratch = self.idle_open_job(49)
        self.park(task, scratch)
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'cleared')
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET copy='saving' WHERE id=?", (task['id'],))
            self.store.db.execute("UPDATE task_space_saves SET phase='staged' WHERE task=?", (task['id'],))
        worker = self.worker(task['id'], self.root, status='queued', provider='claude')
        with self.store.db:
            idle_trim.mark_worker(self.store, task['id'], worker)
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'cleared')

    def test_pr1_rule_defaults_are_off_and_empty(self):
        data = self.call('settings.get').data
        for name in ('discard_names', 'keep'):
            self.assertEqual(data['close_saves_' + name], [])
        self.assertFalse(close_saves.kept(self.store, 1))

    def test_pr1_rules_and_retry_are_local_cli_only(self):
        for source in ('mcp', 'telegram', 'service', 'other'):
            for op, params in (('cleanup.close_rules', {'keep': '1'}), ('cleanup.retry', {'task': 1})):
                with self.subTest(source=source, op=op):
                    result = control_api.call(self.store, op, params, source=source)
                    self.assertFalse(result.ok)
                    self.assertEqual(result.state, 'refused')
                    self.assertIn('local CLI', result.text)
        self.assertIsNone(self.store.get('close_saves_keep'))

    def test_pr1_rules_requeue_only_matching_closed_holds(self):
        tasks = []
        reasons = ('ignored files exceed 200 MB: data', 'unrelated')
        for status in (*close_saves.CLOSED, 'open'):
            for reason in reasons:
                task = self.job()
                with self.store.db:
                    self.store.db.execute('UPDATE tasks SET status=? WHERE id=?', (status, task['id']))
                    self.store.db.execute("UPDATE tasks SET copy='held',copy_reason=? WHERE id=?", (reason, task['id']))
                tasks.append((task, status, reason))
        self.call('cleanup.close_rules', discard_names='.outputs', requeue=False)
        self.assertTrue(all(self.store.task_get(task['id'])['copy'] == 'held' for task, _, _ in tasks))
        self.call('cleanup.close_rules', discard_names=' .outputs , .scratch ')
        self.assertEqual(self.store.get('close_saves_discard_names'), ['.outputs', '.scratch'])
        for task, status, reason in tasks:
            current = self.store.task_get(task['id'])
            match = status in close_saves.CLOSED and reason == reasons[0]
            self.assertEqual((current['copy'], current['copy_reason']), ('saving', None) if match else ('held', reason))

    def test_pr1_retry_queues_one_closed_job_and_refuses_open(self):
        first, second = self.job(), self.job()
        self.assertFalse(control_api.call(self.store, 'cleanup.retry', {'task': first['id']}).ok)
        self.assertFalse(control_api.call(self.store, 'cleanup.retry', {'task': 999999}).ok)
        for task in (first, second):
            with self.store.db:
                self.store.db.execute("UPDATE tasks SET status='done' WHERE id=?", (task['id'],))
                self.store.db.execute("UPDATE tasks SET copy='held',copy_reason='fixture hold' WHERE id=?", (task['id'],))
        self.call('cleanup.retry', task=first['id'])
        self.assertEqual(self.store.task_get(first['id'])['copy'], 'saving')
        self.assertEqual(self.store.task_get(second['id'])['copy'], 'held')

    def test_pr1_canary_rule_can_explicitly_requeue(self):
        task = self.job()
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET status='done' WHERE id=?", (task['id'],))
            self.store.db.execute("UPDATE tasks SET copy='held',copy_reason='ignored files exceed 200 MB: data' WHERE id=?", (task['id'],))
        self.call('cleanup.close_rules', discard_names='.outputs', requeue=False)
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'held')
        self.call('cleanup.close_rules', discard_names='.outputs', requeue=True)
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'saving')

    def test_pr1_keep_list_blocks_rule_requeue_and_retry_then_can_be_cleared(self):
        kept, retry = self.job(), self.job()
        for task in (kept, retry):
            with self.store.db:
                self.store.db.execute("UPDATE tasks SET status='done' WHERE id=?", (task['id'],))
                self.store.db.execute("UPDATE tasks SET copy='held',copy_reason='ignored files exceed 200 MB: data' WHERE id=?", (task['id'],))
        self.call('cleanup.close_rules', keep=' %d, %d, %d ' % (kept['id'], 999999, kept['id']), discard_names='.outputs')
        self.assertEqual(self.store.get('close_saves_keep'), [kept['id'], 999999])
        self.assertTrue(close_saves.kept(self.store, kept['id']))
        self.assertFalse(close_saves.kept(self.store, retry['id']))
        self.assertEqual(self.store.task_get(kept['id'])['copy'], 'held')
        self.assertEqual(self.store.task_get(retry['id'])['copy'], 'saving')
        result = control_api.call(self.store, 'cleanup.retry', {'task': kept['id']})
        self.assertEqual(result.state, 'refused')
        self.assertIn('close_saves_keep', result.text)
        self.assertEqual(self.store.task_get(kept['id'])['copy'], 'held')
        self.call('cleanup.close_rules', keep='')
        self.assertEqual(self.store.get('close_saves_keep'), [])
        self.call('cleanup.retry', task=kept['id'])
        self.assertEqual(self.store.task_get(kept['id'])['copy'], 'saving')

    def test_rebuildable_ignored_files_matching_the_published_hash_are_not_saved(self):
        import hashlib
        published, unpublished = b'p' * 1000, b'new art'
        (self.project / '.gitignore').write_text('.env\npublic/art/\n')
        (self.project / 'art.json').write_text(json.dumps({
            '/art/room.webp': {'sha256': hashlib.sha256(published).hexdigest()},
            '/art/new.webp': {'sha256': hashlib.sha256(b'old').hexdigest()}}))
        self.git('add', '.')
        self.git('commit', '-qm', 'manifest')
        task = self.job()
        art = self.target(task) / 'public' / 'art'
        art.mkdir(parents=True)
        (art / 'room.webp').write_bytes(published)
        (art / 'new.webp').write_bytes(unpublished)
        rule = [{'project': str(self.project), 'paths': ['public/art/*'], 'manifest': 'art.json', 'prefix': 'public',
                 'ref': self.git('rev-parse', 'HEAD').stdout.strip(), 'command': 'npm run art:fetch'}]
        with patch.object(close_saves, 'IGNORED_LIMIT', 100):
            closed = self.close(task)
            self.assertIn('ignored files exceed', closed['copy_reason'])
            self.call('cleanup.close_rules', rebuildable=json.dumps(rule))
            self.drain()
        self.assert_cleared(task)
        saved = self.manifest(task)['ignored']
        self.assertEqual(saved['discarded'], [{'name': 'public/art/room.webp', 'bytes': 1000, 'rebuild': 'npm run art:fetch'}])
        self.assertEqual([entry['name'] for entry in saved['copied']], ['public/art/new.webp'])
        self.reopen(task)
        self.assertEqual((art / 'new.webp').read_bytes(), unpublished)
        self.assertFalse((art / 'room.webp').exists())

    def test_rebuildable_rule_validates(self):
        for value in ('not json', '{}', '[{"project": "relative", "paths": ["a"], "manifest": "m", "ref": "r", "command": "c"}]',
                      '[{"project": "/p", "paths": [], "manifest": "m", "ref": "r", "command": "c"}]',
                      '[{"project": "/p", "paths": ["a"], "manifest": "m", "ref": "r"}]'):
            with self.subTest(value=value):
                result = control_api.call(self.store, 'cleanup.close_rules', {'rebuildable': value})
                self.assertEqual(result.state, 'refused')
                self.assertIsNone(self.store.get('close_saves_rebuildable'))

    def test_pr1_rule_lists_validate_before_any_mutation(self):
        for params in ({'keep': '1,not-an-id'}, {'keep': '0'}, {'keep': '-3'},
                       {'discard_names': '../data'}, {'discard_names': '/data'}, {'discard_names': '.'}):
            with self.subTest(params=params):
                result = control_api.call(self.store, 'cleanup.close_rules', dict({'keep': '5', 'discard_names': 'x'}, **params))
                self.assertEqual(result.state, 'refused')
                self.assertIsNone(self.store.get('close_saves_keep'))
                self.assertIsNone(self.store.get('close_saves_discard_names'))

    def test_pr1_discard_names_apply_to_ignored_and_survive_verification(self):
        task = self.job()
        target = self.target(task)
        (target / '.gitignore').write_text('.outputs/\n')
        (target / '.outputs').mkdir()
        (target / '.outputs' / 'generated.bin').write_bytes(b'generated')
        with patch.object(close_saves, 'IGNORED_LIMIT', 1):
            closed = self.close(task)
            self.assertIn('ignored files exceed', closed['copy_reason'])
            self.call('cleanup.close_rules', discard_names='.outputs')
            self.drain()
        self.assert_cleared(task)
        self.assertEqual([entry['name'] for entry in self.manifest(task)['ignored']['discarded']], ['.outputs'])
        self.reopen(task)
        self.assertFalse((target / '.outputs').exists())

    def test_pr1_freed_categories_equal_existing_total(self):
        task = self.job()
        target = self.target(task)
        (target / '.gitignore').write_text('node_modules/\n')
        (target / 'node_modules').mkdir()
        dependency_size = 1234
        (target / 'node_modules' / 'package').write_bytes(b'd' * dependency_size)
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        scratch_size = 456
        (scratch / 'temp').write_bytes(b's' * scratch_size)
        self.close(task)
        self.assert_cleared(task)
        _, saved = close_saves.load(self.store, task)
        freed = saved['freed']
        total = self.store.db.execute('SELECT bytes_freed FROM task_iterations WHERE task=? AND k=?', (task['id'], task['iteration'])).fetchone()[0]
        self.assertEqual(set(freed), {'copy', 'deps', 'extras', 'prune'})
        self.assertEqual(sum(freed.values()), total)
        self.assertEqual(freed['deps'], dependency_size)
        self.assertEqual(freed['extras'], scratch_size)
        self.assertEqual(freed['prune'], 0)
        self.assertGreater(freed['copy'], 0)

    def test_dirty_copy_wip_and_rebuild_are_byte_identical(self):
        task = self.job()
        target = self.target(task)
        (target / 'README.md').write_bytes(b'edited\n')
        (target / 'nested' / 'keep.txt').unlink()
        (target / 'binary').write_bytes(bytes(range(256)) + b'\x00binary')
        (target / 'untracked').write_bytes(b'not added\xff\n')
        before = {str(p.relative_to(target)): p.read_bytes() for p in target.rglob('*') if p.is_file() and p.name != '.git'}
        index = (self.git_directory(target) / 'index').read_bytes()
        self.close(task)
        self.assert_cleared(task)
        saved = self.manifest(task)
        self.assertTrue(saved['wip'])
        self.assertEqual(self.git('rev-parse', 'torii/task-' + str(task['id'])).stdout.strip(), self.base)
        self.assertEqual(self.git('rev-parse', 'refs/torii/wip/task-' + str(task['id'])).stdout.strip(), saved['wip'])
        self.reopen(task)
        after = {str(p.relative_to(target)): p.read_bytes() for p in target.rglob('*') if p.is_file() and p.name != '.git'}
        self.assertEqual(after, before)
        self.assertEqual(self.git('diff', '--cached', '--name-only', cwd=target).stdout, '')
        self.assertIn('?? untracked', self.git('status', '--porcelain', cwd=target).stdout)
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'present')
        self.assertTrue(index)

    def test_reopen_after_results_shrink_rebuilds_copy(self):
        task = self.job()
        target = self.target(task)
        (target / 'README.md').write_bytes(b'edited\n')
        (target / 'untracked').write_bytes(b'not added\n')
        before = {str(p.relative_to(target)): p.read_bytes() for p in target.rglob('*') if p.is_file() and p.name != '.git'}
        proofs = close_saves.scratch(self.store, task['id'], create=True) / 'proofs'
        proofs.mkdir()
        (proofs / 'shot.png').write_bytes(b'png')
        result = self.folder(task).parent.parent
        result.mkdir(parents=True, exist_ok=True)
        (result / 'bulk.bin').write_bytes(b'bulk')
        self.close(task)
        self.assert_cleared(task)
        with patch('coordinator.close_saves.pwd.getpwuid', return_value=SimpleNamespace(pw_dir=str(self.root))):
            asyncio.run(close_saves.fade_results(self.store))
        self.assertFalse((result / 'bulk.bin').exists())
        self.assertIn('shot.png', [p.name for p in close_saves.files(self.folder(task) / 'proofs')])
        self.reopen(task)
        after = {str(p.relative_to(target)): p.read_bytes() for p in target.rglob('*') if p.is_file() and p.name != '.git'}
        self.assertEqual(after, before)
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'present')

    def test_unpushed_commits_are_in_a_verified_bundle(self):
        task = self.job()
        head = self.commit(self.target(task))
        self.close(task)
        self.assert_cleared(task)
        bundle = self.folder(task) / 'branch.bundle'
        self.assertEqual(self.git('bundle', 'verify', bundle).returncode, 0)
        self.assertIn(head, self.git('bundle', 'list-heads', bundle).stdout)
        self.assertEqual(self.manifest(task)['bundle']['sha256'], hashlib.sha256(bundle.read_bytes()).hexdigest())

    def requeue_at_extras(self, task):
        with self.store.db:
            self.store.db.execute("UPDATE task_space_saves SET phase='extras' WHERE task=?", (task['id'],))
            self.store.db.execute("UPDATE tasks SET copy='saving' WHERE id=?", (task['id'],))
        self.drain()
        return self.store.task_get(task['id'])

    def lost_problems(self, task):
        return [row['detail'] for row in self.store.db.execute(
            "SELECT detail FROM problems WHERE code='saved-work-lost' AND task=?", (task['id'],))]

    def test_missing_bundle_is_rebuilt_while_the_repository_holds_its_commits(self):
        task = self.job()
        head = self.commit(self.target(task))
        self.close(task)
        bundle = self.folder(task) / 'branch.bundle'
        first = self.manifest(task)['bundle']
        bundle.unlink()
        current = self.requeue_at_extras(task)
        self.assertEqual((current['copy'], current['copy_reason']), ('cleared', None))
        self.assertEqual(self.git('bundle', 'verify', bundle).returncode, 0)
        self.assertIn(head, self.git('bundle', 'list-heads', bundle).stdout)
        saved = self.manifest(task)['bundle']
        self.assertEqual(saved['sha256'], hashlib.sha256(bundle.read_bytes()).hexdigest())
        self.assertEqual(close_saves.load(self.store, task)[1]['bundle'], saved)
        self.assertEqual(set(saved), set(first))
        self.assertFalse((self.folder(task) / 'branch.bundle.tmp').exists())
        self.assertEqual(self.lost_problems(task), [])

    def test_missing_bundle_does_not_block_the_rebuild_of_a_job_copy(self):
        task = self.job()
        head = self.commit(self.target(task), 'unpushed work\n')
        self.close(task)
        bundle = self.folder(task) / 'branch.bundle'
        bundle.unlink()
        self.reopen(task)
        self.assertEqual(self.git('rev-parse', 'HEAD', cwd=self.target(task)).stdout.strip(), head)
        self.assertEqual((self.target(task) / 'README.md').read_text(), 'unpushed work\n')
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'present')
        self.assertEqual(self.manifest(task)['bundle']['sha256'], hashlib.sha256(bundle.read_bytes()).hexdigest())

    def lose_saved_work(self):
        task = self.job()
        head = self.commit(self.target(task))
        self.close(task)
        (self.folder(task) / 'branch.bundle').unlink()
        refs = self.git('for-each-ref', '--format=%(refname)', 'refs/torii', 'refs/heads/torii').stdout.split()
        for ref in refs:
            self.git('update-ref', '-d', ref)
        self.git('reflog', 'expire', '--expire=now', '--all')
        self.git('gc', '-q', '--prune=now')
        self.assertNotEqual(self.git('cat-file', '-e', head, check=False).returncode, 0)
        return task, head

    def test_job_copy_rebuild_names_lost_commits_once(self):
        task = self.job()
        (self.target(task) / 'README.md').write_text('saved edit\n')
        self.close(task)
        wip = close_saves.load(self.store, task)[1]['wip']
        (self.folder(task) / 'branch.bundle').unlink()
        for ref in self.git('for-each-ref', '--format=%(refname)', 'refs/torii').stdout.split():
            self.git('update-ref', '-d', ref)
        self.git('reflog', 'expire', '--expire=now', '--all')
        self.git('gc', '-q', '--prune=now')
        self.call('tasks.update', task=task['id'], status='open')
        for _ in range(2):
            with self.store.db:
                result = control_api.call(self.store, 'worktree.create', {'task': task['id']}, topic=task['topic'])
            self.assertFalse(result.ok)
            self.assertIn('Job %s: the saved bundle is gone' % task['number'], result.text)
            self.assertIn(wip, result.text)
            self.assertNotIn('saved work is kept', result.text)
        self.assertEqual(len(self.lost_problems(task)), 1)

    def test_missing_bundle_with_lost_commits_records_one_problem_and_stops_retries(self):
        task, head = self.lose_saved_work()
        current = self.requeue_at_extras(task)
        self.assertEqual(current['copy'], 'held')
        self.assertIn('Job %s: the saved bundle is gone' % task['number'], current['copy_reason'])
        self.assertIn(head, current['copy_reason'])
        self.assertEqual(len(self.lost_problems(task)), 1)
        self.assertTrue(self.store.get('close_save_holds')[str(task['id'])]['lost'])
        asyncio.run(close_saves.retry_holds(self.store, now=time.time() + 30 * 86400))
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'held')
        self.requeue_at_extras(task)
        self.assertEqual(len(self.lost_problems(task)), 1)

    def test_parked_job_held_by_a_timeout_or_a_race_is_retried_with_backoff(self):
        from coordinator import job_limits, scan_process
        errors = (ValueError('git add timed out after 30s'),
                  scan_process.ScanError('close-save state changed during checkpoint'))
        for error in errors:
            with self.subTest(error=str(error)):
                task, scratch = self.idle_open_job(49)
                job_limits.sweep(self.state)
                with patch.object(close_saves, 'snapshot', side_effect=error):
                    self.drain()
                    held = self.store.task_get(task['id'])
                    self.assertEqual((held['status'], held['copy'], held['copy_reason']), ('open', 'held', str(error)))
                    since = self.store.get('close_save_holds')[str(task['id'])]['since']
                    asyncio.run(close_saves.retry_holds(self.store, now=since + 3599))
                    self.assertEqual(self.store.task_get(task['id'])['copy'], 'held')
                    asyncio.run(close_saves.retry_holds(self.store, now=since + 3600))
                    self.assertEqual(self.store.task_get(task['id'])['copy'], 'saving')
                    self.drain()
                    self.assertEqual(self.store.task_get(task['id'])['copy'], 'held')
                    asyncio.run(close_saves.retry_holds(self.store, now=since + 3600 + 7199))
                    self.assertEqual(self.store.task_get(task['id'])['copy'], 'held')
                asyncio.run(close_saves.retry_holds(self.store, now=since + 3600 + 7200))
                self.assertEqual(self.store.task_get(task['id'])['copy'], 'saving')
                self.drain()
                self.drain()
                current = self.store.task_get(task['id'])
                self.assertEqual((current['status'], current['copy']), ('open', 'cleared'))
                self.assertFalse(scratch.exists())
                self.assertFalse(self.target(task).exists())
                asyncio.run(close_saves.retry_holds(self.store, now=since + 86400))
                self.assertNotIn(str(task['id']), self.store.get('close_save_holds'))

    def test_hold_retry_wait_stops_growing_at_one_day(self):
        task, _ = self.idle_open_job(49)
        with self.store.db:
            self.store.db.execute('INSERT INTO task_parks(task,k,parked) VALUES (?,?,?)', (task['id'], task['iteration'], 0))
            self.store.db.execute("UPDATE tasks SET copy='held',copy_reason='untracked files exceed 200 MB' WHERE id=?",
                                  (task['id'],))
            self.store.put('close_save_holds', {str(task['id']): {'k': task['iteration'], 'since': 0, 'retried': 0, 'attempts': 40}})
        asyncio.run(close_saves.retry_holds(self.store, now=86399))
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'held')
        asyncio.run(close_saves.retry_holds(self.store, now=86400))
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'saving')

    def test_reopened_job_with_a_hold_from_its_closed_round_is_parked_and_saved(self):
        from coordinator import job_limits
        task = self.job()
        paused = self.job()
        with patch.object(close_saves, 'snapshot', side_effect=ValueError('git add timed out after 30s')):
            for job in (task, paused):
                self.assertEqual(self.close(job)['copy'], 'held')
        with self.store.db:
            for job in (task, paused):
                self.store.db.execute("UPDATE tasks SET status='open',updated=? WHERE id=?",
                                      (time.time() - 49 * 3600, job['id']))
            self.store.db.execute("UPDATE task_space_saves SET phase='restore_failed' WHERE task=?", (paused['id'],))
        asyncio.run(close_saves.retry_holds(self.store, now=time.time() + 30 * 86400))
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'held')
        job_limits.sweep(self.state)
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'saving')
        self.assertEqual(self.store.task_get(paused['id'])['copy'], 'held')
        self.assertIsNone(self.store.db.execute('SELECT 1 FROM task_parks WHERE task=?', (paused['id'],)).fetchone())
        self.drain()
        self.drain()
        current = self.store.task_get(task['id'])
        self.assertEqual((current['status'], current['copy']), ('open', 'cleared'))
        self.assertFalse(self.target(task).exists())
        self.assertTrue(self.target(paused).is_dir())

    def test_paused_rebuild_of_an_earlier_round_keeps_its_hold(self):
        from coordinator import job_limits
        task = self.job()
        target = self.target(task)
        (target / 'README.md').write_text('saved edit\n')
        self.close(task)
        branch = 'torii/task-' + str(task['id'])
        (self.project / 'moved.txt').write_text('new branch content\n')
        self.git('add', 'moved.txt')
        self.git('commit', '-qm', 'branch moved')
        moved = self.git('rev-parse', 'HEAD').stdout.strip()
        self.git('update-ref', 'refs/heads/' + branch, moved, self.base)
        self.call('tasks.update', task=task['id'], status='open')
        with self.store.db:
            self.store.db.execute("INSERT INTO task_iterations(task,k,opened,opened_by) VALUES (?,?,?,'owner')",
                                  (task['id'], task['iteration'] + 1, time.time()))
            self.store.db.execute('UPDATE tasks SET iteration=? WHERE id=?', (task['iteration'] + 1, task['id']))
        with self.store.db:
            result = control_api.call(self.store, 'worktree.create', {'task': task['id']}, topic=task['topic'])
        self.assertFalse(result.ok)
        held = self.store.task_get(task['id'])
        self.assertEqual(held['copy'], 'held')
        self.assertTrue(held['copy_reason'].startswith('Rebuild paused'))
        with self.store.db:
            self.store.db.execute('UPDATE tasks SET updated=? WHERE id=?', (time.time() - 49 * 3600, task['id']))
        job_limits.sweep(self.state)
        current = self.store.task_get(task['id'])
        self.assertEqual((current['copy'], current['copy_reason']), ('held', held['copy_reason']))
        self.drain()
        self.drain()
        rows = self.store.db.execute('SELECT k,phase FROM task_space_saves WHERE task=? ORDER BY k', (task['id'],)).fetchall()
        self.assertEqual([tuple(row) for row in rows], [(task['iteration'], 'restore_failed')])
        self.assertIsNone(self.store.db.execute('SELECT 1 FROM task_parks WHERE task=?', (task['id'],)).fetchone())

    def test_parked_job_hold_waits_for_backoff_across_the_hourly_park_sweep(self):
        from coordinator import job_limits
        task, _ = self.idle_open_job(49)
        job_limits.sweep(self.state)
        with patch.object(close_saves, 'snapshot', side_effect=ValueError('git add timed out after 30s')) as snapshot:
            self.drain()
            self.assertEqual(self.store.task_get(task['id'])['copy'], 'held')
            for _ in range(3):
                job_limits.sweep(self.state)
                self.assertEqual(self.store.task_get(task['id'])['copy'], 'held')
                self.drain()
            self.assertEqual(snapshot.call_count, 1)
            since = self.store.get('close_save_holds')[str(task['id'])]['since']
            asyncio.run(close_saves.retry_holds(self.store, now=since + 3600))
            self.drain()
            self.assertEqual(snapshot.call_count, 2)

    def test_rebuild_refuses_a_saved_ref_that_moved(self):
        task = self.job()
        self.commit(self.target(task))
        self.close(task)
        bundle = self.folder(task) / 'branch.bundle'
        bundle.unlink()
        ref = close_saves.load(self.store, task)[1]['head_ref']
        self.git('update-ref', ref, self.base)
        current = self.requeue_at_extras(task)
        self.assertEqual((current['copy'], current['copy_reason']), ('held', 'saved ref changed: ' + ref))
        self.assertEqual(self.git('rev-parse', ref).stdout.strip(), self.base)
        self.assertFalse(bundle.exists())
        self.assertFalse((self.folder(task) / 'branch.bundle.tmp').exists())
        self.assertEqual(self.lost_problems(task), [])

    def backup_then_lose(self):
        """Lose the saved commits of a closed job in the project, after a push to a bare repository whose tracking ref is pruned."""
        task = self.job()
        head = self.commit(self.target(task))
        self.close(task)
        backup = self.root / 'backup.git'
        subprocess.run(('git', 'init', '-q', '--bare', str(backup)), check=True, timeout=30)
        self.git('push', '-q', str(backup), head + ':refs/heads/keep')
        (self.folder(task) / 'branch.bundle').unlink()
        for ref in self.git('for-each-ref', '--format=%(refname)', 'refs/torii', 'refs/heads/torii').stdout.split():
            self.git('update-ref', '-d', ref)
        self.git('reflog', 'expire', '--expire=now', '--all')
        self.git('gc', '-q', '--prune=now')
        self.assertNotEqual(self.git('cat-file', '-e', head, check=False).returncode, 0)
        return task, head, backup

    def unverified_problems(self, task):
        return [row['detail'] for row in self.store.db.execute(
            "SELECT detail FROM problems WHERE code='saved-work-unverified' AND task=?", (task['id'],))]

    def test_missing_bundle_is_rebuilt_from_commits_a_remote_still_holds(self):
        task, head, backup = self.backup_then_lose()
        self.git('remote', 'add', 'backup', backup)
        current = self.requeue_at_extras(task)
        self.assertEqual((current['copy'], current['copy_reason']), ('cleared', None))
        bundle = self.folder(task) / 'branch.bundle'
        self.assertIn(head, self.git('bundle', 'list-heads', bundle).stdout)
        self.assertEqual(self.manifest(task)['bundle']['sha256'], hashlib.sha256(bundle.read_bytes()).hexdigest())
        self.assertEqual(self.git('rev-parse', close_saves.load(self.store, task)[1]['head_ref']).stdout.strip(), head)
        self.assertEqual(self.lost_problems(task) + self.unverified_problems(task), [])

    def test_missing_bundle_is_rebuilt_from_another_clone_the_record_names(self):
        task, head, backup = self.backup_then_lose()
        saved = close_saves.load(self.store, task)[1]
        repository = Path(asyncio.run(workspaces.project_identity(self.project))['project_key'])
        for clones in ((), (backup,)):
            record = dict(saved, repository=str(backup)) if not clones else dict(saved)
            temporary = asyncio.run(close_saves.rebuild_bundle(self.store, task, repository, self.folder(task), record, clones=clones))
            self.assertIn(head, self.git('bundle', 'list-heads', temporary).stdout)
            temporary.unlink()
            for ref in self.git('for-each-ref', '--format=%(refname)', 'refs/torii').stdout.split():
                self.git('update-ref', '-d', ref)
            self.git('gc', '-q', '--prune=now')
            self.assertNotEqual(self.git('cat-file', '-e', head, check=False).returncode, 0)

    def test_remote_that_answers_without_the_commits_confirms_lost_work(self):
        task, head = self.lose_saved_work()
        empty = self.root / 'empty.git'
        subprocess.run(('git', 'init', '-q', '--bare', str(empty)), check=True, timeout=30)
        self.git('remote', 'add', 'empty', empty)
        current = self.requeue_at_extras(task)
        self.assertEqual(current['copy'], 'held')
        self.assertIn('also asked: empty', current['copy_reason'])
        self.assertEqual(len(self.lost_problems(task)), 1)
        self.assertEqual(self.unverified_problems(task), [])

    def test_remote_that_gives_no_answer_keeps_the_save_retrying_and_is_not_lost_work(self):
        task, head, backup = self.backup_then_lose()
        self.git('remote', 'add', 'backup', self.root / 'offline.git')
        for _ in range(2):
            current = self.requeue_at_extras(task)
            self.assertEqual(current['copy'], 'held')
            self.assertIn('cannot verify that they are lost: no answer from backup', current['copy_reason'])
            self.assertIn(head, current['copy_reason'])
        self.assertEqual(self.lost_problems(task), [])
        self.assertEqual(len(self.unverified_problems(task)), 1)
        entry = self.store.get('close_save_holds')[str(task['id'])]
        self.assertNotIn('lost', entry)
        asyncio.run(close_saves.retry_holds(self.store, now=entry['since'] + 3599))
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'held')
        self.git('remote', 'set-url', 'backup', backup)
        asyncio.run(close_saves.retry_holds(self.store, now=entry['since'] + 3600))
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'saving')
        self.drain()
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'cleared')
        self.assertIn(head, self.git('bundle', 'list-heads', self.folder(task) / 'branch.bundle').stdout)
        self.assertEqual(self.lost_problems(task), [])

    def test_job_copy_rebuild_with_a_silent_remote_pauses_without_naming_lost_work(self):
        task = self.job()
        (self.target(task) / 'README.md').write_text('saved edit\n')
        self.close(task)
        wip = close_saves.load(self.store, task)[1]['wip']
        backup = self.root / 'backup.git'
        subprocess.run(('git', 'init', '-q', '--bare', str(backup)), check=True, timeout=30)
        self.git('push', '-q', str(backup), wip + ':refs/heads/keep')
        (self.folder(task) / 'branch.bundle').unlink()
        for ref in self.git('for-each-ref', '--format=%(refname)', 'refs/torii').stdout.split():
            self.git('update-ref', '-d', ref)
        self.git('reflog', 'expire', '--expire=now', '--all')
        self.git('gc', '-q', '--prune=now')
        self.assertNotEqual(self.git('cat-file', '-e', wip, check=False).returncode, 0)
        self.git('remote', 'add', 'backup', self.root / 'offline.git')
        self.call('tasks.update', task=task['id'], status='open')
        for _ in range(2):
            with self.store.db:
                result = control_api.call(self.store, 'worktree.create', {'task': task['id']}, topic=task['topic'])
            self.assertFalse(result.ok)
            self.assertIn('cannot verify that they are lost: no answer from backup', result.text)
            self.assertIn(wip, result.text)
            self.assertNotIn('saved work is kept', result.text)
        self.assertEqual(self.lost_problems(task), [])
        self.assertEqual(len(self.unverified_problems(task)), 1)
        self.git('remote', 'set-url', 'backup', backup)
        self.call('worktree.create', task=task['id'])
        self.assertEqual((self.target(task) / 'README.md').read_text(), 'saved edit\n')
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'present')

    def test_git_conversion_holds_instead_of_losing_raw_file_bytes(self):
        task = self.job()
        target = self.target(task)
        self.git('config', 'core.safecrlf', 'false', cwd=target)
        (target / '.gitattributes').write_text('converted.txt text eol=lf\n')
        original = b'fixture raw CRLF\r\n'
        (target / 'converted.txt').write_bytes(original)
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'held')
        self.assertIn('Git conversion', closed['copy_reason'])
        self.assertEqual((target / 'converted.txt').read_bytes(), original)
        self.assertEqual(self.git('rev-parse', 'torii/task-' + str(task['id'])).stdout.strip(), self.base)
        self.assertFalse(self.trash.exists())

    def test_nested_ignored_file_is_counted_and_copied_once(self):
        task = self.job()
        target = self.target(task)
        (target / '.gitignore').write_text('*.private\n')
        ignored = target / 'art' / 'tiles' / 'nested' / 'proof.private'
        ignored.parent.mkdir(parents=True)
        ignored.write_bytes(b'keep')
        with patch.object(close_saves, 'IGNORED_LIMIT', 4):
            closed = self.close(task)
        self.assertEqual(closed['copy'], 'cleared', closed['copy_reason'])
        self.assert_cleared(task)
        copied = self.manifest(task)['ignored']['copied']
        self.assertEqual(len(copied), 1)
        self.assertEqual(copied[0]['name'], 'art/tiles/nested/proof.private')
        self.assertEqual((self.folder(task) / copied[0]['path']).read_bytes(), b'keep')

    def test_regenerable_discarded_other_ignored_saved_and_env_held(self):
        task = self.job()
        target = self.target(task)
        (target / '.gitignore').write_text('node_modules/\nnotes.private\n.env\n')
        (target / 'node_modules').mkdir()
        (target / 'node_modules' / 'pkg').write_bytes(b'regenerable')
        (target / 'notes.private').write_bytes(b'keep ignored\xff')
        (target / '.env').write_text('MADE_UP_FIXTURE=only\n')
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'held')
        self.assertIn('.env', closed['copy_reason'])
        self.assertIn('.env', self.call('tasks.get', task=task['id']).text)
        self.assertIn('copy kept because', self.call('tasks.list').text)
        self.assertTrue(target.is_dir())
        (target / '.env').unlink()
        self.call('tasks.update', task=task['id'], note='fixture env removed')
        self.drain()
        self.assert_cleared(task)
        saved = self.manifest(task)
        self.assertIn('node_modules', [entry['name'] for entry in saved['ignored']['discarded']])
        self.assertEqual((self.folder(task) / 'ignored' / 'notes.private').read_bytes(), b'keep ignored\xff')
        self.reopen(task)
        self.assertFalse((target / 'node_modules').exists())
        self.assertEqual((target / 'notes.private').read_bytes(), b'keep ignored\xff')

    def test_ignored_copies_expire_seven_days_after_close(self):
        task = self.job()
        target = self.target(task)
        (target / '.gitignore').write_text('notes.private\n')
        (target / 'notes.private').write_bytes(b'keep ignored')
        self.close(task)
        self.assert_cleared(task)
        ignored = self.folder(task) / 'ignored'
        self.assertEqual((ignored / 'notes.private').read_bytes(), b'keep ignored')
        closed = self.store.db.execute('SELECT closed FROM task_iterations WHERE task=? AND k=?',
                                       (task['id'], task['iteration'])).fetchone()['closed']
        asyncio.run(close_saves.expire_ignored(self.store, now=closed + close_saves.IGNORED_SECONDS - 60))
        self.assertTrue((ignored / 'notes.private').exists())
        asyncio.run(close_saves.expire_ignored(self.store, now=closed + close_saves.IGNORED_SECONDS + 60))
        self.assertFalse(ignored.exists())
        self.assertEqual(self.manifest(task)['ignored']['copied'], [])
        self.assertEqual([entry['name'] for entry in self.manifest(task)['ignored']['expired']], ['notes.private'])
        self.reopen(task)
        self.assertFalse((target / 'notes.private').exists())
        self.assertEqual((target / 'README.md').read_text(), 'base\n')

    def test_hold_retry_failure_skips_one_job_and_backs_off(self):
        tasks = [self.job() for _ in range(2)]
        for task in tasks:
            (self.target(task) / '.gitignore').write_text('.env\n')
            (self.target(task) / '.env').write_text('MADE_UP_FIXTURE=only\n')
            self.assertEqual(self.close(task)['copy'], 'held')
        bad, good = tasks
        real = close_saves.kept

        def kept(store, task_id):
            if task_id == bad['id']:
                raise RuntimeError('fixture hold check failure')
            return real(store, task_id)

        later = time.time() + 2 * 86400
        with patch.object(close_saves, 'kept', side_effect=kept), patch.object(close_saves, 'async_cleanup_scan', return_value=0):
            asyncio.run(close_saves.retry_holds(self.store, now=later))
        self.assertEqual(self.store.task_get(bad['id'])['copy'], 'held')
        self.assertEqual(self.store.task_get(good['id'])['copy'], 'saving')
        [detail] = [row[0] for row in self.store.db.execute(
            "SELECT detail FROM problems WHERE code='hold-retry-failed' AND task=?", (bad['id'],))]
        self.assertRegex(detail, r'^item=%d failures=1 RuntimeError: fixture hold check failure at test_close_saves\.py' % bad['id'])
        with patch('coordinator.sweeps.time.time', return_value=time.time() + 61):
            asyncio.run(close_saves.retry_holds(self.store, now=later + 3600))
        self.assertEqual(self.store.task_get(bad['id'])['copy'], 'saving')
        self.assertIsNone(self.store.db.execute("SELECT 1 FROM sweep_failures WHERE sweep='hold-retry'").fetchone())

    def test_failed_or_waiting_hold_retry_keeps_the_hold_entry(self):
        task = self.job()
        (self.target(task) / '.gitignore').write_text('.env\n')
        (self.target(task) / '.env').write_text('MADE_UP_FIXTURE=only\n')
        self.assertEqual(self.close(task)['copy'], 'held')
        start = time.time()
        with patch.object(close_saves, 'async_cleanup_scan', return_value=0):
            asyncio.run(close_saves.retry_holds(self.store, now=start))
            with self.store.db:
                self.store.db.execute("UPDATE tasks SET copy='held' WHERE id=?", (task['id'],))
            asyncio.run(close_saves.retry_holds(self.store, now=start + 2 * 86400))
        entry = self.store.get('close_save_holds')[str(task['id'])]
        self.assertEqual(entry['attempts'], 1)
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET copy='held' WHERE id=?", (task['id'],))
        with patch.object(close_saves, 'kept', side_effect=RuntimeError('fixture: one transient failure')):
            asyncio.run(close_saves.retry_holds(self.store, now=start + 2 * 86400 + 3600))
        self.assertEqual(self.store.get('close_save_holds').get(str(task['id'])), entry)
        asyncio.run(close_saves.retry_holds(self.store, now=start + 2 * 86400 + 7200))
        self.assertEqual(self.store.get('close_save_holds').get(str(task['id'])), entry, 'a job under backoff keeps its entry')

    def test_ignored_expiry_failure_skips_one_round_and_backs_off(self):
        tasks = [self.job() for _ in range(2)]
        for task in tasks:
            (self.target(task) / '.gitignore').write_text('notes.private\n')
            (self.target(task) / 'notes.private').write_bytes(b'keep ignored')
            self.close(task)
        later = time.time() + close_saves.IGNORED_SECONDS + 60
        real = close_saves.ws.delete_tree

        async def delete(path, **options):
            if 'task-%d/' % tasks[0]['id'] in str(path):
                raise RuntimeError('fixture delete failure')
            return await real(path, **options)

        with patch.object(close_saves.ws, 'delete_tree', delete):
            asyncio.run(close_saves.expire_ignored(self.store, now=later))
        self.assertTrue((self.folder(tasks[0]) / 'ignored').exists())
        self.assertFalse((self.folder(tasks[1]) / 'ignored').exists())
        [detail] = [row[0] for row in self.store.db.execute(
            "SELECT detail FROM problems WHERE code='ignored-expiry-failed' AND task=?", (tasks[0]['id'],))]
        self.assertRegex(detail, r'^item=%d/%d failures=1 RuntimeError: fixture delete failure at test_close_saves\.py'
                         % (tasks[0]['id'], tasks[0]['iteration']))
        asyncio.run(close_saves.expire_ignored(self.store, now=later))
        self.assertTrue((self.folder(tasks[0]) / 'ignored').exists())
        with patch('coordinator.sweeps.time.time', return_value=time.time() + 120):
            asyncio.run(close_saves.expire_ignored(self.store, now=later))
        self.assertFalse((self.folder(tasks[0]) / 'ignored').exists())
        self.assertIsNone(self.store.db.execute('SELECT 1 FROM sweep_failures').fetchone())

    def test_service_sweep_expires_ignored_copies(self):
        service = Service.__new__(Service)
        service.store = self.store
        storage = SimpleNamespace(reconcile=AsyncMock(side_effect=asyncio.CancelledError()), schedule=lambda: None)
        with patch('coordinator.worker_storage.storage_for', return_value=storage), \
                patch.object(close_saves, 'expire_ignored', new=AsyncMock()) as expire, \
                self.assertRaises(asyncio.CancelledError):
            asyncio.run(service.worktree_cleanup_loop())
        expire.assert_awaited_once_with(self.store)

    def test_rebase_in_progress_holds_the_copy(self):
        task = self.job()
        (self.git_directory(self.target(task)) / 'rebase-merge').mkdir()
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'held')
        self.assertIn('rebase-merge', closed['copy_reason'])
        self.assertTrue(self.target(task).is_dir())

    def test_tracked_agent_named_folders_are_saved_and_cleared(self):
        task = self.job()
        target = self.target(task)
        for name in ('.claude', '.codex'):
            (target / name).mkdir()
            (target / name / 'project.md').write_text('repo fixture, not native history\n')
        self.git('add', '.', cwd=target)
        self.git('commit', '-qm', 'repository agent files', cwd=target)
        self.close(task)
        self.assert_cleared(task)
        self.reopen(task)
        for name in ('.claude', '.codex'):
            self.assertEqual((target / name / 'project.md').read_text(), 'repo fixture, not native history\n')

    def test_symlink_node_modules_never_follows_shared_target(self):
        task = self.job()
        shared = self.root / 'shared-packages'
        shared.mkdir()
        (shared / 'keep').write_bytes(b'shared')
        target = self.target(task)
        (target / '.gitignore').write_text('node_modules\n')
        (target / 'node_modules').symlink_to(shared, target_is_directory=True)
        self.close(task)
        self.assert_cleared(task)
        self.assertEqual((shared / 'keep').read_bytes(), b'shared')

    def test_note_proof_copied_with_sha256_before_scratch_and_temp_clear(self):
        task = self.job()
        target = self.target(task)
        scratch = self.state / 'scratch' / ('task-' + str(task['id']))
        scratch.mkdir(parents=True, exist_ok=True)
        proof = scratch / 'check report (final).txt'
        proof.write_bytes(b'proof\x00\xff')
        code_proof = target / proof.name
        code_proof.write_bytes(b'code proof')
        all_proofs = scratch / 'proofs'
        all_proofs.mkdir()
        (all_proofs / 'extra.txt').write_text('extra proof')
        worker = self.worker(task['id'], target, status='done')
        from coordinator.worker_storage import temp_directory
        temporary = temp_directory(worker)
        temporary.mkdir(exist_ok=True)
        (temporary / 'proof.txt').write_text('temp proof')
        self.call('tasks.update', task=task['id'], note='\n'.join([
            'proof="' + str(proof) + '"', "proof='" + str(proof) + "'",
            '`' + str(proof) + '`', '[proof](<' + str(proof) + '>)',
            'proof="' + str(code_proof) + '"',
            'proof=' + str(temporary / 'proof.txt')]))
        self.close(task)
        self.assert_cleared(task)
        saved = self.manifest(task)
        entry = next(p for p in saved['proofs'] if p['source'] == str(proof))
        self.assertEqual(entry['sha256'], hashlib.sha256(b'proof\x00\xff').hexdigest())
        self.assertEqual((self.folder(task) / entry['path']).read_bytes(), b'proof\x00\xff')
        code_entry = next(p for p in saved['proofs'] if p['source'] == str(code_proof))
        self.assertNotEqual(entry['path'], code_entry['path'])
        self.assertEqual((self.folder(task) / code_entry['path']).read_bytes(), b'code proof')
        self.assertEqual(code_entry['sha256'], hashlib.sha256(b'code proof').hexdigest())
        self.assertEqual(len(saved['proofs']), 4)
        self.assertFalse(scratch.exists())
        self.assertFalse(temporary.exists())

    def test_missing_listed_proof_clears_and_records_missing(self):
        for listing in ('note', 'delivery'):
            with self.subTest(listing=listing):
                task = self.job()
                missing = self.target(task) / '.owner-delivery' / 'gone.png'
                present = self.state / 'scratch' / ('task-' + str(task['id'])) / 'proofs' / 'present.bin'
                present.parent.mkdir(parents=True)
                content = bytes(range(256)) + b'\x00proof\xff'
                present.write_bytes(content)
                if listing == 'note':
                    self.call('tasks.update', task=task['id'], note='proof=' + str(missing))
                else:
                    with self.store.db:
                        delivery = self.store.enqueue_report(task['topic'], 'fixture delivery', image=str(missing))
                    self.store.delivered(delivery, 123)
                closed = self.close(task)
                self.assertEqual(closed['copy'], 'cleared', closed['copy_reason'])
                self.assert_cleared(task)
                saved = self.manifest(task)
                self.assertIn({'source': str(missing), 'missing': True}, saved['proofs'])
                copied = next(entry for entry in saved['proofs'] if entry['source'] == str(present))
                self.assertEqual((self.folder(task) / copied['path']).read_bytes(), content)
                self.assertEqual(copied['sha256'], hashlib.sha256(content).hexdigest())
                self.assertEqual(close_saves.load(self.store, task)[1]['proofs'], saved['proofs'])

    def test_proof_disappearing_during_copy_clears_and_records_missing(self):
        for removal in ('before_open', 'after_copy'):
            with self.subTest(removal=removal):
                task = self.job()
                proof = self.state / 'scratch' / ('task-' + str(task['id'])) / 'proofs' / 'gone.mp4'
                proof.parent.mkdir(parents=True)
                proof.write_bytes(b'fixture video')
                original = close_saves.digest
                reads = []

                def digest(path):
                    if path == proof:
                        reads.append(path)
                        if removal == 'after_copy' and len(reads) == 2:
                            proof.unlink()
                    result = original(path)
                    if path == proof and removal == 'before_open' and len(reads) == 1:
                        proof.unlink()
                    return result

                original_scan = close_saves.async_cleanup_scan

                async def scan(store, function, *args):
                    if function is close_saves.copy_proof_file:
                        return await asyncio.to_thread(function, *args)
                    return await original_scan(store, function, *args)

                with patch.object(close_saves, 'digest', side_effect=digest), \
                        patch.object(close_saves, 'async_cleanup_scan', side_effect=scan):
                    self.close(task)
                self.assertTrue(reads)
                self.assert_cleared(task)
                self.assertEqual(self.manifest(task)['proofs'], [{'source': str(proof), 'missing': True}])

    def test_missing_worker_proof_is_retained_through_close(self):
        task = self.job()
        worker = self.worker(task['id'], self.target(task), status='done')
        from coordinator.worker_storage import temp_directory
        proof = temp_directory(worker) / 'gone.png'
        self.call('tasks.update', task=task['id'], note='proof=' + str(proof))
        row = dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (worker,)).fetchone())
        self.assertTrue(asyncio.run(close_saves.preserve_worker_proofs(self.store, row)))
        expected = [{'source': str(proof), 'missing': True}]
        self.assertEqual(json.loads((self.folder(task) / 'worker-proofs.json').read_text()), expected)
        self.close(task)
        self.assert_cleared(task)
        self.assertEqual(self.manifest(task)['proofs'], expected)

    def test_retained_proof_keeps_saved_bytes_when_source_is_gone(self):
        task = self.job()
        worker = self.worker(task['id'], self.target(task), status='done')
        from coordinator.worker_storage import temp_directory
        proof = temp_directory(worker) / 'saved.png'
        proof.parent.mkdir(parents=True, exist_ok=True)
        proof.write_bytes(b'already saved proof\x00\xff')
        self.call('tasks.update', task=task['id'], note='proof=' + str(proof))
        row = dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (worker,)).fetchone())
        self.assertTrue(asyncio.run(close_saves.preserve_worker_proofs(self.store, row)))
        proof.unlink()
        self.close(task)
        self.assert_cleared(task)
        entry, = self.manifest(task)['proofs']
        self.assertNotIn('missing', entry)
        self.assertEqual((self.folder(task) / entry['path']).read_bytes(), b'already saved proof\x00\xff')

    def test_missing_proof_reappearance_is_saved_on_late_writer_retry(self):
        task = self.job()
        proof = self.state / 'scratch' / ('task-' + str(task['id'])) / 'reappeared.png'
        self.call('tasks.update', task=task['id'], note='proof=' + str(proof))
        original = close_saves.stage
        calls = []

        async def stage(*args):
            calls.append(True)
            if len(calls) == 1:
                proof.write_bytes(b'late proof bytes')
            return await original(*args)

        with patch.object(close_saves, 'stage', side_effect=stage):
            self.close(task)
        self.assert_cleared(task)
        self.assertEqual(len(calls), 2)
        entry, = self.manifest(task)['proofs']
        self.assertNotIn('missing', entry)
        self.assertEqual((self.folder(task) / entry['path']).read_bytes(), b'late proof bytes')

    def test_proof_destination_and_permission_errors_still_hold(self):
        for failure in ('destination', 'permission'):
            with self.subTest(failure=failure):
                task = self.job()
                proof = self.target(task) / 'present.png'
                proof.write_bytes(b'present proof')
                self.call('tasks.update', task=task['id'], note='proof=' + str(proof))

                def copy_file(source, destination, anchor):
                    if failure == 'destination':
                        source.unlink()
                        raise FileNotFoundError(2, 'No such file or directory', str(destination))
                    raise PermissionError(13, 'Permission denied', str(source))

                original_scan = close_saves.async_cleanup_scan

                async def scan(store, function, *args):
                    if function is close_saves.copy_proof_file:
                        return await asyncio.to_thread(function, *args)
                    return await original_scan(store, function, *args)

                with patch.object(close_saves, 'copy_file', side_effect=copy_file), \
                        patch.object(close_saves, 'async_cleanup_scan', side_effect=scan):
                    closed = self.close(task)
                self.assertEqual(closed['copy'], 'held')
                self.assertIn('Errno 2' if failure == 'destination' else 'Permission denied', closed['copy_reason'])
                self.assertTrue(self.target(task).is_dir())

    def test_missing_symlink_proof_is_still_held(self):
        task = self.job()
        proof = self.target(task) / 'link.png'
        proof.symlink_to(self.target(task) / 'absent.png')
        self.call('tasks.update', task=task['id'], note='proof=' + str(proof))
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'held')
        self.assertIn('symbolic link', closed['copy_reason'])
        self.assertTrue(proof.is_symlink())

    def test_reused_note_proof_path_is_saved_in_each_round(self):
        task = self.job()
        proof = self.state / 'scratch' / ('task-' + str(task['id'])) / 'reused.txt'
        retained = []
        for index, content in enumerate((b'first round proof', b'second round proof')):
            proof.parent.mkdir(parents=True, exist_ok=True)
            proof.write_bytes(content)
            self.call('tasks.update', task=task['id'], note='proof=' + str(proof))
            self.close(task)
            self.assert_cleared(task)
            matches = [entry for entry in self.manifest(task)['proofs'] if entry['source'] == str(proof)]
            self.assertEqual(len(matches), 1)
            self.assertEqual(matches[0]['sha256'], hashlib.sha256(content).hexdigest())
            saved = self.folder(task) / matches[0]['path']
            self.assertEqual(saved.read_bytes(), content)
            retained.append(saved)
            if index == 0:
                self.reopen(task)
                task = self.store.task_get(task['id'])
        self.assertNotEqual(retained[0], retained[1])
        self.assertEqual(retained[0].read_bytes(), b'first round proof')

    def test_crash_after_every_checkpoint_resumes(self):
        from coordinator import close_saves
        for phase in ('proofs', 'snapshot', 'ignored', 'manifest', 'verified', 'staged', 'extras', 'cleared'):
            with self.subTest(phase=phase):
                task = self.job()
                (self.target(task) / 'README.md').write_text('saved across ' + phase)
                original = close_saves.checkpoint_encoded
                original_async = close_saves.async_checkpoint
                crashed = []

                def checkpoint(store, job, current, data):
                    original(store, job, current, data)
                    if current in ('extras', 'cleared') and current == phase and not crashed:
                        crashed.append(True)
                        raise SystemExit('fixture crash')

                async def async_checkpoint(store, job, current, data):
                    await original_async(store, job, current, data)
                    if current == phase and not crashed:
                        crashed.append(True)
                        raise SystemExit('fixture crash')

                self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
                with patch.object(close_saves, 'checkpoint_encoded', side_effect=checkpoint), \
                        patch.object(close_saves, 'async_checkpoint', side_effect=async_checkpoint):
                    with self.assertRaises(SystemExit):
                        self.drain()
                self.store.close()
                self.store = fixtures.Store(self.state, token_file=self.root / 'no-token')
                self.drain()
                self.assert_cleared(task)
                self.reopen(task)
                self.assertEqual((self.target(task) / 'README.md').read_text(), 'saved across ' + phase)

    def test_checkpoint_size_limit_stops_before_process_transfer(self):
        from coordinator import scan_process
        task = self.job()
        with patch.object(scan_process, 'run_async', new_callable=AsyncMock) as run:
            with self.assertRaises(scan_process.ScanError):
                asyncio.run(close_saves.async_checkpoint(self.store, task, 'proofs', {'proofs': ['x'] * 50001}))
        run.assert_not_awaited()
        self.assertEqual(close_saves.load(self.store, task), ('pending', {}))

    def test_failed_scan_reporting_never_runs_loose_save(self):
        from coordinator import problems, scan_process
        task = self.job()
        target = self.target(task)
        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        original_record = problems.record

        def record(store, area, code, *args, **kwargs):
            if code == 'file-scan-failed':
                raise RuntimeError('problem database unavailable')
            return original_record(store, area, code, *args, **kwargs)

        with patch.object(close_saves, 'TERMINAL', ('done', 'dropped')), \
                patch.object(problems, 'record', side_effect=record), \
                patch.object(scan_process, 'run_async', new_callable=AsyncMock,
                             side_effect=scan_process.ScanError('scan timed out')), \
                patch.object(close_saves, 'loose_save', new_callable=AsyncMock) as loose:
            asyncio.run(close_saves.save_job(self.store, task['id']))
        loose.assert_not_awaited()
        self.assertTrue(target.is_dir())
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'held')

    def test_scan_reporting_failure_preserves_scan_error(self):
        from coordinator import problems, scan_process
        task = self.job()
        with patch.object(problems, 'record', side_effect=RuntimeError('problem database unavailable')), \
                patch.object(close_saves.ws.logger, 'warning', side_effect=RuntimeError('log unavailable')):
            with self.assertRaises(scan_process.ScanError):
                asyncio.run(close_saves.check_json_size(self.store, {'proofs': ['x'] * 50001}))
            with patch.object(scan_process, 'run_async', new_callable=AsyncMock,
                              side_effect=scan_process.ScanError('scan timed out')):
                with self.assertRaises(scan_process.ScanError):
                    asyncio.run(close_saves.async_scan_task_references(self.store, task['id'], {}))

    def test_reference_scan_allows_more_than_thirty_seconds(self):
        from coordinator import scan_process
        expected = {'123': {'results/task-123/keep'}}

        def scan(function, *args, **kwargs):
            if kwargs.get('timeout', scan_process.TIMEOUT) <= 30:
                raise scan_process.ScanError('scan timed out')
            return expected

        with patch.object(scan_process, 'run', side_effect=scan):
            self.assertEqual(close_saves.scan_references(self.store), expected)
        with patch.object(scan_process, 'run_async', side_effect=scan):
            self.assertEqual(asyncio.run(close_saves.async_scan_references(self.store)), expected)

    def test_checkpoint_is_read_in_child_process(self):
        from coordinator import scan_process
        task = self.job()
        close_saves.checkpoint(self.store, task, 'proofs', {'proofs': ['saved']})
        with patch.object(scan_process, 'run_async', wraps=scan_process.run_async) as run:
            phase, saved = asyncio.run(close_saves.async_load(self.store, task))
        self.assertEqual(phase, 'proofs')
        self.assertEqual(saved['proofs'], ['saved'])
        self.assertIs(run.call_args.args[0], close_saves.read_checkpoint)
        self.assertEqual(run.call_args.args[1:], (self.state, task['id'], task['iteration']))

    def test_manifest_preparation_failure_keeps_scratch(self):
        from coordinator import scan_process
        task = self.job()
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        (scratch / 'keep.txt').write_text('keep')
        original = close_saves.async_cleanup_scan

        async def scan(store, function, *args):
            if function is close_saves.prepare_json:
                raise scan_process.ScanError('fixture failure')
            return await original(store, function, *args)

        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        with patch.object(close_saves, 'async_cleanup_scan', side_effect=scan):
            self.drain()
        self.assertEqual((scratch / 'keep.txt').read_text(), 'keep')

    def test_checkpoint_change_during_manifest_preparation_keeps_scratch(self):
        task = self.job()
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        (scratch / 'keep.txt').write_text('keep')
        original = close_saves.async_cleanup_scan
        changed = []

        async def scan(store, function, *args):
            result = await original(store, function, *args)
            if function is close_saves.prepare_json:
                with store.db:
                    store.db.execute("UPDATE task_space_saves SET phase='proofs' WHERE task=?", (task['id'],))
                changed.append(True)
            return result

        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        with patch.object(close_saves, 'async_cleanup_scan', side_effect=scan):
            self.drain()
        self.assertTrue(changed)
        self.assertEqual((scratch / 'keep.txt').read_text(), 'keep')

    def test_late_writer_causes_one_resnapshot(self):
        task = self.job()
        (self.target(task) / 'README.md').write_text('first')
        from coordinator import close_saves
        original = close_saves.stage
        calls = []

        async def stage(*args):
            calls.append(True)
            if len(calls) == 1:
                (self.target(task) / 'README.md').write_text('late writer')
            return await original(*args)

        with patch.object(close_saves, 'stage', side_effect=stage):
            self.close(task)
        self.assert_cleared(task)
        self.assertEqual(len(calls), 2)
        self.reopen(task)
        self.assertEqual((self.target(task) / 'README.md').read_text(), 'late writer')

    def test_undelivered_images_block_copy_scratch_and_temp_until_sent(self):
        task = self.job()
        target = self.target(task)
        scratch = self.state / 'scratch' / ('task-' + str(task['id']))
        scratch.mkdir(parents=True, exist_ok=True)
        image = scratch / 'image.png'
        image.write_bytes(b'fake image')
        with self.store.db:
            outbox = self.store.enqueue_report(fixtures.TOPIC, 'image', image=str(image))
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'held')
        self.assertIn("hasn't been sent", closed['copy_reason'])
        self.assertTrue(target.is_dir())
        self.assertTrue(image.exists())
        self.store.delivered(outbox, 123)
        self.drain()
        self.assert_cleared(task)
        self.assertFalse(scratch.exists())
        self.assertEqual(len(self.manifest(task)['proofs']), 1)

    def test_sent_staged_attachment_is_saved_as_proof_after_source_disappears(self):
        task = self.job()
        scratch = close_saves.scratch(self.store, task['id'])
        scratch.mkdir(parents=True, exist_ok=True)
        source = scratch / 'image.png'
        source.write_bytes(b'sent proof bytes')
        self.store.put('owner', 1)
        self.store.put('group', 1)
        message = self.call('telegram.send', text='Proof', image=str(source)).data['outbox']
        staged = Path(self.store.db.execute('SELECT image FROM outbox WHERE id=?', (message,)).fetchone()[0])
        source.rename(source.with_name('removed.png'))
        self.store.delivered(message, 100)
        self.assertFalse(staged.exists())
        self.assertEqual(self.close(task)['copy'], 'cleared')
        proof = next(entry for entry in self.manifest(task)['proofs'] if entry['source'] == str(source))
        self.assertEqual((self.folder(task) / proof['path']).read_bytes(), b'sent proof bytes')

    def test_queued_staged_attachment_survives_close_and_saves_original_source(self):
        task = self.job()
        scratch = close_saves.scratch(self.store, task['id'])
        scratch.mkdir(parents=True, exist_ok=True)
        source = scratch / 'image.png'
        source.write_bytes(b'queued proof bytes')
        self.store.put('owner', 1)
        self.store.put('group', 1)
        message = self.call('telegram.send', text='Proof', image=str(source)).data['outbox']
        staged = Path(self.store.db.execute('SELECT image FROM outbox WHERE id=?', (message,)).fetchone()[0])
        self.assertEqual(self.close(task)['copy'], 'cleared')
        self.assertTrue(staged.exists())
        proof = next(entry for entry in self.manifest(task)['proofs'] if entry['source'] == str(source))
        self.assertEqual((self.folder(task) / proof['path']).read_bytes(), b'queued proof bytes')
        self.store.delivered(message, 100)
        self.assertFalse(staged.exists())

    def test_rebuild_seconds_recorded_on_current_round(self):
        task = self.job()
        self.close(task)
        self.assert_cleared(task)
        self.reopen(task)
        rows = self.store.task_iterations(task['id'])
        self.assertIsInstance(rows[-1]['rebuild_seconds'], float)
        self.assertGreater(rows[-1]['rebuild_seconds'], 0)
        self.assertEqual(rows[0]['head'], self.base)
        self.assertEqual(rows[0]['saved'], str(self.folder(task) / 'saved.json'))

    def test_shared_huggingface_cache_outside_owned_folders_is_untouched(self):
        shared = self.root / 'shared-cache' / 'huggingface'
        shared.mkdir(parents=True)
        (shared / 'voice-model').write_bytes(b'keep voice model')
        task = self.job()
        target = self.target(task)
        (target / '.gitignore').write_text('.cache\n')
        (target / '.cache').symlink_to(shared, target_is_directory=True)
        self.close(task)
        self.assert_cleared(task)
        self.assertEqual((shared / 'voice-model').read_bytes(), b'keep voice model')

    def test_manifest_tamper_holds_before_deletion(self):
        task = self.job()
        proof = self.target(task) / 'proof.txt'
        proof.write_text('original')
        self.call('tasks.update', task=task['id'], note='proof=' + str(proof))
        from coordinator import close_saves
        original = close_saves.checkpoint_encoded

        def checkpoint(store, job, phase, data):
            original(store, job, phase, data)
            if phase == 'manifest':
                entry = json.loads(data)['proofs'][0]
                (self.folder(task) / entry['path']).write_text('tampered')

        with patch.object(close_saves, 'checkpoint_encoded', side_effect=checkpoint):
            closed = self.close(task)
        self.assertEqual(closed['copy'], 'held')
        self.assertIn('checksum', closed['copy_reason'])
        self.assertTrue(self.target(task).is_dir())

    def test_scratch_returned_and_chat_scratch_clears_only_without_open_jobs(self):
        task = self.job()
        response = self.call('tasks.get', task=task['id'])
        self.assertEqual(response.data.get('scratch'), str(self.state / 'scratch' / ('task-' + str(task['id']))))
        from coordinator import close_saves
        chat = close_saves.scratch(self.store, topic=fixtures.TOPIC, create=True)
        (chat / 'temp').write_text('chat temp')
        asyncio.run(close_saves.clear_chat_scratch(self.store, fixtures.TOPIC))
        self.assertTrue(chat.exists())
        self.close(task)
        asyncio.run(close_saves.clear_chat_scratch(self.store, fixtures.TOPIC))
        self.assertFalse(chat.exists())

    def test_close_saves_and_clears_by_default(self):
        task = self.job()
        self.close(task)
        self.assert_cleared(task)

    def test_crash_between_copy_rename_and_detach_recovers(self):
        task = self.job()
        target = self.target(task)
        (target / 'README.md').write_text('rename crash')
        rename = Path.rename
        crashed = []

        def crash(path, destination):
            result = rename(path, destination)
            if path == target and not crashed:
                crashed.append(True)
                raise SystemExit('fixture crash after rename')
            return result

        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        with patch.object(Path, 'rename', crash):
            with self.assertRaises(SystemExit):
                self.drain()
        self.store.close()
        self.store = fixtures.Store(self.state, token_file=self.root / 'no-token')
        self.drain()
        self.assert_cleared(task)
        self.reopen(task)
        self.assertEqual((target / 'README.md').read_text(), 'rename crash')

    def test_crash_after_partial_deletion_resumes_from_saved_tree(self):
        task = self.job()
        target = self.target(task)
        (target / 'README.md').write_text('partial delete crash')
        remove = workspaces.delete_tree
        crashed = []

        async def crash(path, **options):
            if path.name == 'copy' and not crashed:
                crashed.append(True)
                (path / 'README.md').unlink()
                raise SystemExit('fixture crash during delete')
            return await remove(path, **options)

        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        with patch.object(workspaces, 'delete_tree', side_effect=crash):
            with self.assertRaises(SystemExit):
                self.drain()
        self.store.close()
        self.store = fixtures.Store(self.state, token_file=self.root / 'no-token')
        self.drain()
        self.assert_cleared(task)
        self.reopen(task)
        self.assertEqual((target / 'README.md').read_text(), 'partial delete crash')

    def test_staged_late_writer_is_saved_again_before_deletion(self):
        task = self.job()
        target = self.target(task)
        (target / '.gitignore').write_text('notes.private\n')
        (target / 'notes.private').write_text('first ignored contents')
        from coordinator import close_saves
        original = close_saves.checkpoint_encoded
        changed = []

        def checkpoint(store, job, phase, data):
            original(store, job, phase, data)
            if phase == 'staged' and not changed:
                changed.append(True)
                stage = next(stage for stage, record in workspaces._deletion_stages(self.state) if record['task'] == task['id'])
                (stage / 'copy' / 'notes.private').write_text('late after rename')

        with patch.object(close_saves, 'checkpoint_encoded', side_effect=checkpoint):
            self.close(task)
        self.drain()
        self.assert_cleared(task)
        self.reopen(task)
        self.assertEqual((target / 'notes.private').read_text(), 'late after rename')

    def test_second_late_writer_holds_the_original_copy(self):
        task = self.job()
        from coordinator import close_saves
        original = close_saves.stage
        calls = []

        async def stage(*args):
            calls.append(True)
            (self.target(task) / 'README.md').write_text('late ' + str(len(calls)))
            return await original(*args)

        with patch.object(close_saves, 'stage', side_effect=stage):
            closed = self.close(task)
        self.assertEqual(closed['copy'], 'held')
        self.assertEqual(len(calls), 2)
        self.assertTrue(self.target(task).is_dir())

    def test_snapshot_does_not_change_the_branch_real_index_or_stash(self):
        task = self.job()
        target = self.target(task)
        (target / 'README.md').write_text('staged edit')
        self.git('add', 'README.md', cwd=target)
        index = self.git_directory(target) / 'index'
        before = index.read_bytes()
        from coordinator import close_saves
        original = close_saves.checkpoint_encoded
        observed = []

        def checkpoint(store, job, phase, data):
            original(store, job, phase, data)
            if phase == 'snapshot':
                observed.append(True)
                self.assertEqual(index.read_bytes(), before)
                self.assertEqual(self.git('rev-parse', 'HEAD', cwd=target).stdout.strip(), self.base)
                self.assertEqual(self.git('stash', 'list', cwd=target).stdout, '')

        with patch.object(close_saves, 'checkpoint_encoded', side_effect=checkpoint):
            self.close(task)
        self.assertTrue(observed)
        self.assert_cleared(task)

    def test_unignored_env_is_held_before_git_snapshot(self):
        task = self.job()
        (self.target(task) / '.gitignore').write_text('')
        (self.target(task) / '.env').write_text('ONLY_MADE_UP=fixture')
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'held')
        self.assertIn('.env', closed['copy_reason'])
        self.assertNotEqual(self.git('show-ref', '--verify', '--quiet', 'refs/torii/wip/task-' + str(task['id']), check=False).returncode, 0)

    def test_waiting_and_interrupted_rounds_use_the_same_save_queue(self):
        for status in ('waiting', 'interrupted'):
            with self.subTest(status=status):
                task = self.job()
                (self.target(task) / 'README.md').write_text(status)
                worker = self.worker(task['id'], self.target(task), status='waiting_for_quota')
                with self.store.db:
                    self.store.db.execute('UPDATE tasks SET status=? WHERE id=?', (status, task['id']))
                self.drain()
                current = self.store.task_get(task['id'])
                self.assertEqual((current['copy'], current['copy_reason']), ('held', 'job has an unfinished worker'))
                self.assertTrue(self.target(task).is_dir())
                with self.store.db:
                    self.store.db.execute("UPDATE workers SET status='done' WHERE id=?", (worker,))
                    close_saves.request(self.store, task['id'])
                self.drain()
                self.assert_cleared(task)
                self.assertEqual(self.store.task_get(task['id'])['status'], status)

    def test_symbolic_scratch_root_is_held_without_touching_shared_data(self):
        task = self.job()
        scratch = self.state / 'scratch' / ('task-' + str(task['id']))
        scratch.rmdir()
        shared = self.root / 'outside-scratch'
        shared.mkdir()
        (shared / 'keep').write_text('shared')
        scratch.symlink_to(shared, target_is_directory=True)
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'held')
        self.assertEqual((shared / 'keep').read_text(), 'shared')
        self.assertTrue(self.target(task).exists())

    def test_cleanup_loop_failure_keeps_delivery_alive_and_queues_one_notice(self):
        from coordinator.service import LOOP_NAMES, Service
        service = Service.__new__(Service)
        service.store = self.store
        service.loop_names = dict(LOOP_NAMES)
        service.wakes = {}
        service.task_restart_limit = 0
        service.task_restart_backoff = 0
        calls = []

        async def fail():
            calls.append(True)
            if len(calls) < 3:
                raise RuntimeError('fixture cleanup failure')
            return True

        with patch.object(service, 'home_topic', return_value=fixtures.TOPIC), \
                patch('coordinator.service.asyncio.sleep', new_callable=AsyncMock):
            self.assertTrue(asyncio.run(service.supervise('worktree_cleanup', fail)))
        self.assertEqual(len(calls), 3)
        self.assertTrue(service.wake('deliver').is_set())
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 1)

    def test_deletion_pauses_within_entry_batch_limit(self):
        task = self.job()
        from coordinator import host_os
        tree = self.root / 'paced-tree'
        tree.mkdir()
        for index in range(1100):
            (tree / str(index)).touch()
        sleeps = []
        original = asyncio.sleep
        counts = {'files': 0, 'bytes': 0}

        async def sleep(delay):
            sleeps.append((delay, counts['files']))
            await original(0)

        with patch.object(host_os.asyncio, 'sleep', side_effect=sleep):
            asyncio.run(host_os.delete_tree(tree, progress=counts, paced=True))
        self.assertFalse(tree.exists())
        self.assertTrue(sleeps)
        self.assertTrue(all(delay == 0.2 for delay, _ in sleeps))
        self.assertLessEqual(sleeps[0][1], 1000)
        self.close(task)
        self.assert_cleared(task)

    def test_short_crash_retry_keeps_copy_until_its_resume_event(self):
        task = self.job()
        with self.store.db:
            self.store.db.execute("UPDATE task_iterations SET cause='crash',resume_at=? WHERE task=?", (time.time() + 300, task['id']))
            self.store.db.execute("UPDATE tasks SET status='interrupted' WHERE id=?", (task['id'],))
        self.drain()
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'held')
        self.assertIn('short crash retry', self.store.task_get(task['id'])['copy_reason'])
        self.assertTrue(self.target(task).exists())

    def test_opencode_private_temp_and_cache_clear_after_proof_save(self):
        task = self.job()
        worker = self.worker(task['id'], self.target(task), status='done', provider='opencode')
        launch = self.state / 'opencode' / 'workers' / str(worker) / 'launches' / ('a' * 32)
        temporary = launch / 'tmp'
        cache = launch / 'home' / 'Library' / 'Caches'
        temporary.mkdir(parents=True)
        cache.mkdir(parents=True)
        (temporary / 'proof.txt').write_text('private temp proof')
        (cache / 'scratch').write_text('regenerable')
        native = launch / 'home' / 'kept-native-fixture'
        native.write_text('native fixture stays')
        self.call('tasks.update', task=task['id'], note='proof=' + str(temporary / 'proof.txt'))
        with self.store.db:
            image = self.store.enqueue_report(fixtures.TOPIC, 'fixture file', image=str(temporary / 'proof.txt'))
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'held')
        self.assertTrue(temporary.exists())
        self.store.delivered(image, 567)
        self.drain()
        self.assert_cleared(task)
        self.assertFalse(temporary.exists())
        self.assertFalse(cache.exists())
        self.assertEqual(native.read_text(), 'native fixture stays')
        self.assertEqual(len(self.manifest(task)['proofs']), 1)

    def test_reopen_during_scratch_clear_keeps_new_round_scratch(self):
        task = self.job()
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        (scratch / 'old-round.txt').write_text('old round scratch\n')
        (scratch / 'later').mkdir()
        (scratch / 'later' / 'x.txt').write_text('x\n')
        original = close_saves.stopped_workers
        reopened = []

        async def concurrent_reopen(store, row, **options):
            await original(store, row, **options)
            if 'clear_extras' in [frame.function for frame in inspect.stack()] and not reopened:
                reopened.append(True)
                self.call('tasks.update', task=task['id'], status='open')
                path = self.call('tasks.get', task=task['id']).data['scratch']
                (close_saves.Path(path) / 'later' / 'brief-for-new-round.md').write_text('new brief\n')

        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        with patch('coordinator.close_saves.stopped_workers', concurrent_reopen):
            self.drain()
        self.assertTrue(reopened, 'the reopen hook did not run')
        current = self.store.task_get(task['id'])
        self.assertEqual(current['status'], 'open')
        self.assertTrue((scratch / 'later' / 'brief-for-new-round.md').exists(),
                        'a file written after the job reopened was deleted; copy=%s reason=%s' %
                        (current['copy'], current['copy_reason']))

    def test_notes_naming_the_worktree_folder_never_clear(self):
        task = self.job()
        self.call('tasks.update', task=task['id'], note='Worktree: ' + str(self.target(task)))
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'cleared', closed['copy_reason'])

    def test_nested_regenerable_name_inside_ignored_folder_is_saved(self):
        task = self.job()
        target = self.target(task)
        (target / '.gitignore').write_text('out/\n')
        (target / 'out' / 'build').mkdir(parents=True)
        (target / 'out' / 'build' / 'release-notes.pdf').write_bytes(b'only copy of a built deliverable\n')
        (target / 'out' / 'keep.txt').write_bytes(b'kept\n')
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'cleared', closed['copy_reason'])
        saved = self.manifest(task)
        self.call('tasks.update', task=task['id'], status='open')
        self.call('worktree.create', task=task['id'])
        self.assertEqual((target / 'out' / 'keep.txt').read_bytes(), b'kept\n')
        self.assertTrue((target / 'out' / 'build' / 'release-notes.pdf').exists(),
                        'discarded: %s' % saved['ignored']['discarded'])

    def test_restore_branch_movement_holds_before_edits_and_can_retry(self):
        task = self.job()
        target = self.target(task)
        (target / 'README.md').write_text('saved edit\n')
        self.close(task)
        branch = 'torii/task-' + str(task['id'])
        (self.project / 'moved.txt').write_text('new branch content\n')
        self.git('add', 'moved.txt')
        self.git('commit', '-qm', 'branch moved')
        moved = self.git('rev-parse', 'HEAD').stdout.strip()
        self.git('update-ref', 'refs/heads/' + branch, moved, self.base)
        self.call('tasks.update', task=task['id'], status='open')
        with self.store.db:
            result = control_api.call(
                self.store, 'worktree.create', {'task': task['id']}, topic=task['topic'])
        self.assertFalse(result.ok)
        self.assertIn('branch changed', result.text)
        phase = self.store.db.execute('SELECT phase FROM task_space_saves WHERE task=?', (task['id'],)).fetchone()[0]
        self.assertNotEqual(phase, 'restoring')
        self.assertEqual(self.git('status', '--porcelain', cwd=target).stdout, '')
        self.git('reset', '--hard', self.base, cwd=target)
        self.call('worktree.create', task=task['id'])
        self.assertEqual((target / 'README.md').read_text(), 'saved edit\n')

    def test_reopen_after_scratch_staging_keeps_new_files_and_restored_phase(self):
        task = self.job()
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        (scratch / 'old.txt').write_text('old')
        original = workspaces.delete_tree
        reopened = []

        async def reopen_staged(path, **kwargs):
            if path.parent == self.folder(task) / 'extras' and not reopened:
                self.assertFalse(scratch.exists())
                self.call('tasks.update', task=task['id'], status='open')
                with self.store.db:
                    self.store.db.execute("UPDATE task_space_saves SET phase='restored' WHERE task=?", (task['id'],))
                close_saves.scratch(self.store, task['id'], create=True)
                (scratch / 'new.txt').write_text('new')
                reopened.append(True)
            await original(path, **kwargs)

        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        with patch('coordinator.workspaces.delete_tree', reopen_staged):
            self.drain()
        self.assertTrue(reopened)
        self.assertEqual((scratch / 'new.txt').read_text(), 'new')
        self.assertNotEqual(self.store.task_get(task['id'])['copy'], 'cleared')
        self.assertEqual(close_saves.load(self.store, task)[0], 'restored')

    def test_scratch_staging_resumes_after_delete_failure(self):
        task = self.job()
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        (scratch / 'old.txt').write_text('old')
        original = workspaces.delete_tree

        async def fail_staged(path, **kwargs):
            if path.parent == self.folder(task) / 'extras':
                raise OSError('fixture interrupted scratch delete')
            await original(path, **kwargs)

        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        with patch('coordinator.workspaces.delete_tree', fail_staged):
            self.drain()
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'held')
        self.assertFalse(scratch.exists())
        self.call('tasks.update', task=task['id'], note='retry staged scratch')
        self.drain()
        self.assert_cleared(task)
        self.assertFalse(list((self.folder(task) / 'extras').iterdir()))

    def test_failed_ignored_restore_can_retry(self):
        task = self.job()
        target = self.target(task)
        (target / '.gitignore').write_text('output.private\n')
        (target / 'output.private').write_text('saved ignored bytes')
        self.close(task)
        self.call('tasks.update', task=task['id'], status='open')
        original = close_saves.copy_file

        def fail_restore(source, destination, anchor):
            if anchor == target:
                raise OSError('fixture restore interrupted')
            return original(source, destination, anchor)

        with patch('coordinator.close_saves.copy_file', fail_restore):
            with self.store.db:
                result = control_api.call(self.store, 'worktree.create', {'task': task['id']}, topic=task['topic'])
        self.assertFalse(result.ok)
        self.assertIn('Rebuild paused', result.text)
        self.assertEqual(close_saves.load(self.store, task)[0], 'restore_failed')
        self.call('worktree.create', task=task['id'])
        self.assertEqual((target / 'output.private').read_text(), 'saved ignored bytes')
        self.assertEqual(close_saves.load(self.store, task)[0], 'restored')

    def test_scratch_rename_resumes_after_checkpoint_transaction_rolls_back(self):
        task = self.job()
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        (scratch / 'old.txt').write_text('old')
        original = close_saves.checkpoint_encoded
        interrupted = []

        def fail_after_rename(store, row, phase, encoded):
            saved = json.loads(encoded)
            entry = saved.get('extras', {}).get(str(scratch))
            original(store, row, phase, encoded)
            if phase == 'extras' and entry and entry['staged'] and not interrupted:
                interrupted.append(True)
                raise OSError('fixture interrupted after rename before commit')

        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        with patch('coordinator.close_saves.checkpoint_encoded', fail_after_rename):
            self.drain()
        self.assertTrue(interrupted)
        self.assertFalse(scratch.exists())
        phase, saved = close_saves.load(self.store, task)
        self.assertEqual(phase, 'extras')
        entry = saved['extras'][str(scratch)]
        self.assertFalse(entry['staged'])
        self.assertEqual((Path(entry['path']) / 'old.txt').read_text(), 'old')
        self.call('tasks.update', task=task['id'], note='retry interrupted rename')
        self.drain()
        self.assert_cleared(task)
        self.assertFalse(list((self.folder(task) / 'extras').iterdir()))

    def test_absent_copy_with_blocked_scratch_reports_cleanup_held(self):
        task = self.job()
        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        self.drain()
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        image = scratch / 'pending.png'
        image.write_bytes(b'fixture image')
        with self.store.db:
            self.store.enqueue_report(task['topic'], 'pending image', image=str(image))
        self.call('tasks.update', task=task['id'], note='clear old scratch')
        self.drain()
        self.assertFalse(self.target(task).exists())
        listing = self.call('tasks.list').text
        self.assertIn('cleanup held', listing)
        self.assertNotIn('copy kept', listing)
        self.assertIn('Cleanup held', self.call('tasks.get', task=task['id']).text)
        self.assertTrue(image.exists())


class ResultReferencePathTests(unittest.TestCase):
    def test_unclosed_quote_with_deep_path_is_scanned_in_linear_time(self):
        text = 'run "results/task-12' + '/segment' * 26 + ' and stop here\n'
        started = time.monotonic()
        paths = list(close_saves.result_reference_paths([text]))
        self.assertLess(time.monotonic() - started, 1)
        self.assertIn(('12', 'results/task-12' + '/segment' * 26, 'results/task-12' + '/segment' * 26), paths)

    def test_quoted_and_bare_paths_keep_their_matches(self):
        text = 'see `scratch/task-7/a b/c.png` and results/task-8/out/x.json, then results/task-9/'
        paths = {(task, path) for task, path, _ in close_saves.result_reference_paths([text])}
        self.assertIn(('7', 'scratch/task-7/a b/c.png'), paths)
        self.assertIn(('8', 'results/task-8/out/x.json'), paths)
        self.assertIn(('9', 'results/task-9'), paths)
