import asyncio
import json
import os
from pathlib import Path
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from coordinator import close_saves, scan_process
from coordinator.store import Store
from tests import test_worktree_cleanup as fixtures


class ReferenceScanTests(unittest.TestCase):
    """One shared, bounded reference scan per state folder."""

    setUp = fixtures.WorktreeCleanupTests.setUp
    tearDown = fixtures.WorktreeCleanupTests.tearDown
    git = fixtures.WorktreeCleanupTests.git
    call = fixtures.WorktreeCleanupTests.call

    def job(self):
        return fixtures.WorktreeCleanupTests.job(self, closed=False)

    def home(self):
        return patch('coordinator.close_saves.pwd.getpwuid', return_value=SimpleNamespace(pw_dir=str(self.root)))

    def scratch(self, task, *parts):
        path = (self.state / 'scratch' / ('task-' + str(task['id']))).joinpath(*parts)
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def test_concurrent_callers_never_overlap_and_share_one_result(self):
        active = []
        peak = []
        calls = []
        lock = threading.Lock()

        def scan(function, *args, **kwargs):
            with lock:
                active.append(1)
                peak.append(len(active))
                calls.append(function)
            time.sleep(0.5)
            with lock:
                active.pop()
            return {'7': {'results/task-7/data'}}

        results = []

        def caller():
            store = Store(self.state, token_file=self.root / 'no-token', existing=True)
            try:
                results.append(close_saves.scan_references(store))
            finally:
                store.close()

        with self.home(), patch.object(scan_process, 'run', side_effect=scan):
            threads = [threading.Thread(target=caller) for _ in range(3)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        self.assertEqual(max(peak), 1)
        self.assertEqual(calls, [close_saves.result_references])
        self.assertEqual(results, [{'7': {'results/task-7/data'}}] * 3)

    def test_async_caller_waits_for_a_scan_in_another_thread(self):
        started = threading.Event()
        release = threading.Event()
        calls = []

        def scan(function, *args, **kwargs):
            calls.append(function)
            started.set()
            release.wait(10)
            return {'8': {'scratch/task-8/input'}}

        async def waiting():
            await asyncio.to_thread(started.wait, 10)
            task = asyncio.create_task(close_saves.async_scan_references(self.store))
            await asyncio.sleep(0.5)
            self.assertFalse(task.done())
            release.set()
            return await task

        with self.home(), patch.object(scan_process, 'run', side_effect=scan), \
                patch.object(scan_process, 'run_async', side_effect=AssertionError('second scan')):
            def caller():
                store = Store(self.state, token_file=self.root / 'no-token', existing=True)
                try:
                    close_saves.scan_references(store)
                finally:
                    store.close()

            thread = threading.Thread(target=caller)
            thread.start()
            result = asyncio.run(waiting())
            thread.join()
        self.assertEqual(result, {'8': {'scratch/task-8/input'}})
        self.assertEqual(len(calls), 1)

    def test_result_is_reused_only_inside_the_window(self):
        calls = []

        def scan(function, *args, **kwargs):
            calls.append(kwargs['timeout'])
            return {'9': {'results/task-9/a'}}

        with self.home(), patch.object(scan_process, 'run', side_effect=scan):
            self.assertEqual(close_saves.scan_references(self.store), {'9': {'results/task-9/a'}})
            self.assertEqual(close_saves.scan_references(self.store), {'9': {'results/task-9/a'}})
            self.assertEqual(len(calls), 1)
            later = time.time() + close_saves.REFERENCE_REUSE_SECONDS + 1
            with patch('coordinator.close_saves.wall_time', return_value=later):
                close_saves.scan_references(self.store)
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0], close_saves.REFERENCE_SCAN_TIMEOUT)
        self.assertLessEqual(close_saves.REFERENCE_SCAN_TIMEOUT, 15 * 60)

    def test_skip_or_loose_scan_is_not_shared(self):
        calls = []

        def scan(function, *args, **kwargs):
            calls.append(args)
            return {}

        with self.home(), patch.object(scan_process, 'run', side_effect=scan):
            close_saves.scan_references(self.store)
            close_saves.scan_references(self.store, skip=5)
            close_saves.scan_references(self.store, strict=False)
        self.assertEqual(len(calls), 3)
        self.assertIsNone(calls[1][5])
        self.assertIsNone(calls[2][6])

    def test_timeout_skips_the_pass_for_every_caller(self):
        calls = []

        def scan(function, *args, **kwargs):
            calls.append(function)
            raise scan_process.ScanError('scan timed out')

        with self.home(), patch.object(scan_process, 'run', side_effect=scan):
            with self.assertRaises(scan_process.ScanError):
                close_saves.scan_references(self.store)
            with self.assertRaises(scan_process.ScanError):
                close_saves.scan_references(self.store)
            with self.assertRaises(scan_process.ScanError):
                asyncio.run(close_saves.async_scan_references(self.store))
        self.assertEqual(len(calls), 1)

    def test_unreadable_folder_is_reported_and_does_not_stop_the_scan(self):
        task = self.job()
        self.scratch(task, 'run.py').write_text("DATA = 'results/task-42/found'\n")
        locked = self.scratch(task, 'fix', 'work')
        locked.mkdir()
        locked.chmod(0)
        try:
            with self.home():
                refs = close_saves.scan_references(self.store)
                again = close_saves.scan_references(self.store)
        finally:
            locked.chmod(0o700)
        self.assertIn('results/task-42/found', refs['42'])
        self.assertEqual(refs[close_saves.UNREADABLE], {str(locked)})
        self.assertEqual(again, refs)
        rows = self.store.db.execute("SELECT task,detail FROM problems WHERE code='reference-scan-unreadable'").fetchall()
        self.assertEqual([tuple(row) for row in rows], [(task['id'], 'scratch/task-%d/fix/work' % task['id'])])
        script = self.scratch(task, 'tool.py')
        script.write_text("DATA = 'results/task-43/found'\n")
        with self.home(), patch('coordinator.close_saves.os.fdopen', side_effect=OSError(5, 'read failed')):
            with self.assertRaises(OSError):
                close_saves.result_references(self.state, True)

    def test_dependency_and_build_trees_are_skipped(self):
        task = self.job()
        for name in ('node_modules', '.git', 'dist', '.venv', '.vite', 'build', '__pycache__'):
            self.scratch(task, 'repo', name, 'tool.py').write_text("DATA = 'results/task-41/skipped'\n")
        self.scratch(task, 'repo', 'run.py').write_text("DATA = 'results/task-42/found'\n")
        with self.home():
            refs = close_saves.result_references(self.state, True)
        self.assertNotIn('41', refs)
        self.assertIn('results/task-42/found', refs['42'])

    def test_reference_in_normal_text_files_is_found(self):
        task = self.job()
        self.scratch(task, 'deep', 'a', 'b', 'c', 'job.sh').write_text('cp /x/scratch/task-43/in.csv .\n')
        self.scratch(task, 'run').write_text('#!/bin/sh\ncat results/task-44/notes\n')
        os.chmod(self.scratch(task, 'run'), 0o755)
        memory = self.state / 'chats' / 'one' / 'memory'
        memory.mkdir(parents=True)
        (memory / 'notes.md').write_text('Input lives in results/task-45/set\n')
        with self.home():
            refs = close_saves.result_references(self.state, True, None, None, (), self.root / 'clean')
            again = close_saves.result_references(self.state, True, None, None, (), self.root / 'clean')
        for refs in (refs, again):
            self.assertIn('scratch/task-43/in.csv', refs['43'])
            self.assertIn('results/task-44/notes', refs['44'])
            self.assertIn('results/task-45/set', refs['45'])

    def test_unchanged_clean_files_are_not_read_again(self):
        task = self.job()
        quiet = self.scratch(task, 'quiet.py')
        quiet.write_text('print(1)\n')
        clean = self.root / 'clean'
        opened = []
        original = os.open

        def counting(path, *args, **kwargs):
            if str(path).endswith('quiet.py'):
                opened.append(path)
            return original(path, *args, **kwargs)

        with self.home(), patch('coordinator.close_saves.os.open', side_effect=counting):
            close_saves.result_references(self.state, True, None, None, (), clean)
            close_saves.result_references(self.state, True, None, None, (), clean)
            self.assertEqual(len(opened), 1)
            quiet.write_text('print(2)\n')
            close_saves.result_references(self.state, True, None, None, (), clean)
            self.assertEqual(len(opened), 2)
            stamp = quiet.stat()
            quiet.write_text("x = 'results/task-46/x'")
            os.utime(quiet, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
            refs = close_saves.result_references(self.state, True, None, None, (), clean)
        self.assertEqual(len(opened), 3)
        self.assertIn('results/task-46/x', refs['46'])

    def test_read_budget_stops_the_scan_and_keeps_progress(self):
        task = self.job()
        for index in range(4):
            self.scratch(task, 'f%d.py' % index).write_text('x' * 1000 + '\n')
        self.scratch(task, 'z.py').write_text("y = 'results/task-47/z'\n")
        clean = self.root / 'clean'
        scans = 0
        with self.home():
            while True:
                scans += 1
                self.assertLess(scans, 10)
                try:
                    refs = close_saves.result_references(self.state, True, None, None, (), clean, 2500)
                except scan_process.ScanError as error:
                    self.assertIn('budget', str(error))
                    continue
                break
        self.assertGreater(scans, 1)
        self.assertIn('results/task-47/z', refs['47'])

    def test_invalid_clean_file_reads_everything(self):
        task = self.job()
        self.scratch(task, 'a.py').write_text("a = 'results/task-48/a'\n")
        clean = self.root / 'clean'
        clean.write_bytes(b'garbage')
        with self.home():
            refs = close_saves.result_references(self.state, True, None, None, (), clean)
        self.assertIn('results/task-48/a', refs['48'])

    def test_shared_scan_uses_the_clean_file_and_budget(self):
        seen = []

        def scan(function, *args, **kwargs):
            seen.append(args)
            return {}

        with self.home(), patch.object(scan_process, 'run', side_effect=scan):
            close_saves.scan_references(self.store)
        self.assertEqual(seen[0][5], self.state / 'reference-scan' / 'clean-files')
        self.assertEqual(seen[0][6], close_saves.REFERENCE_READ_BUDGET)
        self.assertFalse(seen[0][7])
        saved = json.loads((self.state / 'reference-scan' / 'result.json').read_text())
        self.assertEqual(saved['references'], {})

    def test_real_child_scan_finds_reference(self):
        task = self.job()
        self.scratch(task, 'use.py').write_text("p = 'results/task-49/out'\n")
        with self.home():
            refs = close_saves.scan_references(self.store)
            again = close_saves.scan_references(self.store)
        self.assertIn('results/task-49/out', refs['49'])
        self.assertEqual(refs, again)
        self.assertTrue((self.state / 'reference-scan' / 'clean-files').exists())

    def test_markdown_in_the_results_of_an_open_job_is_a_reference(self):
        task = self.job()
        results = self.state / 'results' / ('task-' + str(task['id']))
        results.mkdir(parents=True, exist_ok=True)
        (results / 'HANDOFF.md').write_text('Inputs are in scratch/task-%d/inputs\n' % task['id'])
        self.scratch(task, 'notes.md').write_text('See scratch/task-%d/other\n' % task['id'])
        with self.home():
            refs = close_saves.scan_references(self.store)
        self.assertEqual(refs[str(task['id'])], {'scratch/task-%d/inputs' % task['id']})

    def test_reused_file_scan_reads_database_references_fresh(self):
        task = self.job()
        self.call('tasks.update', task=task['id'], note='Uses results/task-50/kept')
        with self.home():
            first = close_saves.scan_references(self.store)
            self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
            other = self.job()
            self.call('tasks.update', task=other['id'], note='Uses results/task-51/new')
            with patch.object(scan_process, 'run', side_effect=AssertionError('second scan')):
                second = close_saves.scan_references(self.store)
        self.assertIn('results/task-50/kept', first['50'])
        self.assertNotIn('50', second)
        self.assertIn('results/task-51/new', second['51'])


if __name__ == '__main__':
    unittest.main()
