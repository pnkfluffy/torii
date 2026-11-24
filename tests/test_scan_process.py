import asyncio
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from unittest.mock import AsyncMock, patch

from coordinator import close_saves, scan_process
from tests import test_worktree_cleanup as fixtures
from tests.support import real_scans


def setUpModule():
    real_scans()


def scan_with_writer(directory, task_id, skip):
    with sqlite3.connect(str(directory / 'state.sqlite'), timeout=0) as other:
        other.execute('BEGIN IMMEDIATE')
    return close_saves.task_reference_index(directory, task_id, skip)


def write_after_delay(path, started, release):
    started.write_text('started')
    while not release.exists():
        time.sleep(0.01)
    path.write_text('completed')


def oversized_scan_result():
    return b'x' * scan_process.MAX_OUTPUT_BYTES


def parent_death_scan(started, finished):
    started.write_text('started')
    time.sleep(3)
    finished.write_text('finished')


def blocked_parent_death_scan(started):
    started.write_text(str(os.getpid()))
    re.search('(a+)+$', 'a' * 35 + '!')


class ScanProcessTests(unittest.TestCase):
    def test_timed_out_scan_is_killed(self):
        started = time.monotonic()
        with self.assertRaises(scan_process.ScanError):
            scan_process.run(time.sleep, 60, timeout=0.2)
        self.assertLess(time.monotonic() - started, 10)

    def test_failed_scans_back_off_until_a_scan_succeeds(self):
        scan_process._scan_succeeded()
        with self.assertRaises(scan_process.ScanError):
            scan_process.run(time.sleep, 60, timeout=0.5)
        first = scan_process.backoff_delay()
        self.assertGreater(first, 0.3)
        started = time.monotonic()
        with self.assertRaises(scan_process.ScanError):
            scan_process.run(time.sleep, 60, timeout=0.2)
        self.assertGreater(time.monotonic() - started, first)
        second = scan_process.backoff_delay()
        self.assertGreater(second, 1.5 * first)
        started = time.monotonic()
        self.assertEqual(asyncio.run(scan_process.run_async(len, 'ab', timeout=5)), 2)
        self.assertGreater(time.monotonic() - started, second - 0.1)
        self.assertEqual(scan_process.backoff_delay(), 0)
        started = time.monotonic()
        self.assertEqual(scan_process.run(len, 'abc', timeout=5), 3)
        self.assertLess(time.monotonic() - started, 5)

    def test_cancel_waits_for_mutating_scan_to_finish(self):
        with tempfile.TemporaryDirectory() as temporary:
            marker = Path(temporary) / 'marker'
            started = Path(temporary) / 'started'
            release = Path(temporary) / 'release'

            async def cancel_scan():
                operation = asyncio.create_task(scan_process.run_async(
                    write_after_delay, marker, started, release, timeout=30))

                async def wait_started():
                    while not started.exists():
                        await asyncio.sleep(0.01)

                try:
                    await asyncio.wait_for(wait_started(), 20)
                    operation.cancel()
                    await asyncio.sleep(0.1)
                    self.assertFalse(operation.done())
                finally:
                    release.touch()
                with self.assertRaises(asyncio.CancelledError):
                    await operation
                self.assertEqual(marker.read_text(), 'completed')

            asyncio.run(cancel_scan())

    def test_oversized_scan_result_fails_closed(self):
        with self.assertRaises(scan_process.ScanError):
            scan_process.run(oversized_scan_result, timeout=5)

    def test_scan_stops_when_parent_is_killed(self):
        with tempfile.TemporaryDirectory() as temporary:
            started, finished = Path(temporary) / 'started', Path(temporary) / 'finished'
            code = ('from pathlib import Path; from coordinator import scan_process; '
                    'from tests.test_scan_process import parent_death_scan; '
                    'scan_process.run(parent_death_scan, Path(%r), Path(%r), timeout=10)' % (str(started), str(finished)))
            parent = subprocess.Popen((sys.executable, '-c', code))
            try:
                deadline = time.monotonic() + 10
                while not started.exists() and time.monotonic() < deadline:
                    time.sleep(0.05)
                self.assertTrue(started.exists())
                parent.kill()
                parent.wait(timeout=5)
                time.sleep(3.5)
                self.assertFalse(finished.exists())
            finally:
                if parent.poll() is None:
                    parent.kill()
                    parent.wait(timeout=5)

    def test_scan_stops_when_parent_dies_during_blocked_regex(self):
        with tempfile.TemporaryDirectory() as temporary:
            started = Path(temporary) / 'started'
            code = ('from pathlib import Path; from coordinator import scan_process; '
                    'from tests.test_scan_process import blocked_parent_death_scan; '
                    'scan_process.run(blocked_parent_death_scan, Path(%r), timeout=30)' % str(started))
            parent = subprocess.Popen((sys.executable, '-c', code))
            child = None
            try:
                deadline = time.monotonic() + 10
                while not started.exists() and time.monotonic() < deadline:
                    time.sleep(0.05)
                self.assertTrue(started.exists())
                child = int(started.read_text())
                time.sleep(0.2)
                parent.kill()
                parent.wait(timeout=5)
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    result = subprocess.run(('ps', '-o', 'stat=', '-p', str(child)), capture_output=True, text=True)
                    if result.returncode or result.stdout.strip().startswith('Z'):
                        break
                    time.sleep(0.05)
                else:
                    self.fail('scan survived its parent during a blocked regex')
            finally:
                if parent.poll() is None:
                    parent.kill()
                    parent.wait(timeout=5)
                if child is not None:
                    try:
                        command = subprocess.run(('ps', '-o', 'command=', '-p', str(child)),
                                                 capture_output=True, text=True).stdout
                        if 'coordinator.scan_process' in command and os.getpgid(child) == child:
                            os.killpg(child, 9)
                    except ProcessLookupError:
                        pass


class CleanupScanTests(unittest.TestCase):
    setUp = fixtures.WorktreeCleanupTests.setUp
    tearDown = fixtures.WorktreeCleanupTests.tearDown
    git = fixtures.WorktreeCleanupTests.git
    call = fixtures.WorktreeCleanupTests.call
    target = fixtures.WorktreeCleanupTests.target
    job = fixtures.WorktreeCleanupTests.job

    def test_interrupted_late_writer_restore_resumes_without_double_counting(self):
        task = self.job(closed=False)
        stage = self.root / 'restore-stage'
        (stage / 'git').mkdir(parents=True)
        (stage / 'copy').mkdir()
        (stage / 'copy' / 'late.txt').write_text('late work')
        target, git_dir = self.root / 'restored-copy', self.root / 'restored-git'
        copy_info, git_info = (stage / 'copy').stat(), (stage / 'git').stat()
        record = dict(task=task['id'], space_k=task['iteration'], target=str(target), git_dir=str(git_dir),
                      git_inode=[git_info.st_dev, git_info.st_ino],
                      restoring_copy_inode=[copy_info.st_dev, copy_info.st_ino], detached=True, restoring=True)
        close_saves.checkpoint(self.store, task, 'staged', {})
        async def interrupted(*args):
            raise asyncio.CancelledError()

        with patch.object(close_saves.ws, '_save_deletion_bounded', side_effect=interrupted):
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(close_saves.restore_staged(self.store, stage, record))
        self.assertEqual((target / 'late.txt').read_text(), 'late work')
        asyncio.run(close_saves.restore_staged(self.store, stage, record))
        phase, data = close_saves.load(self.store, task)
        self.assertEqual((phase, data['resnapshots']), ('verified', 1))
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'saving')
        self.assertFalse(record['restoring'])
        self.assertFalse(record['detached'])

    def test_failed_scan_preserves_result(self):
        task = self.job(closed=False)
        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        root = self.state / 'results' / ('task-' + str(task['id']))
        root.mkdir(parents=True, exist_ok=True)
        result = root / 'artifact.bin'
        result.write_bytes(b'keep')
        original = scan_process.run
        def hung_scan(*args, **kwargs):
            return original(time.sleep, 60, timeout=0.2)
        with patch.object(scan_process, 'run', side_effect=hung_scan):
            asyncio.run(close_saves.fade_results(self.store))
        self.assertTrue(result.exists())
        self.assertTrue(self.store.db.execute("SELECT 1 FROM problems WHERE code='reference-scan-failed'").fetchone())

    def test_timed_out_retained_proof_scan_preserves_proof(self):
        task = self.job(closed=False)
        scratch = self.state / 'scratch' / ('task-' + str(task['id']))
        proof = scratch / 'proofs' / 'evidence.txt'
        proof.parent.mkdir(parents=True)
        proof.write_text('retained proof')
        folder = self.state / 'results' / ('task-' + str(task['id'])) / 'rounds' / str(task['iteration'])
        folder.mkdir(parents=True)
        original = scan_process.run

        def hung_manifest(function, *args, **kwargs):
            if function is close_saves.scan_retained_proofs:
                return original(time.sleep, 60, timeout=0.2)
            return original(function, *args, **kwargs)

        with patch.object(scan_process, 'run', side_effect=hung_manifest):
            with self.assertRaises(scan_process.ScanError):
                asyncio.run(close_saves.proofs(self.store, task, [scratch], folder))
        self.assertEqual(proof.read_text(), 'retained proof')
        self.assertTrue(self.store.db.execute("SELECT 1 FROM problems WHERE code='file-scan-failed'").fetchone())

    def test_timed_out_close_scan_does_not_try_loose_deletion(self):
        task = self.job(closed=False)
        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        original = scan_process.run

        def hung_manifest(function, *args, **kwargs):
            if function is close_saves.scan_retained_proofs:
                return original(time.sleep, 60, timeout=0.2)
            return original(function, *args, **kwargs)

        with patch.object(scan_process, 'run', side_effect=hung_manifest), \
                patch.object(close_saves, 'loose_save', new_callable=AsyncMock) as loose:
            asyncio.run(close_saves.save_job(self.store, task['id']))
        loose.assert_not_awaited()
        self.assertTrue(self.target(task).exists())
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'held')
        self.assertTrue(self.store.db.execute("SELECT 1 FROM problems WHERE code='file-scan-failed'").fetchone())

    def test_failed_close_scan_does_not_try_loose_deletion(self):
        task = self.job(closed=False)
        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        original = scan_process.run

        def failed_manifest(function, *args, **kwargs):
            if function is close_saves.scan_retained_proofs:
                raise ValueError('invalid retained proof manifest')
            return original(function, *args, **kwargs)

        with patch.object(scan_process, 'run', side_effect=failed_manifest), \
                patch.object(close_saves, 'loose_save', new_callable=AsyncMock) as loose:
            asyncio.run(close_saves.save_job(self.store, task['id']))
        loose.assert_not_awaited()
        self.assertTrue(self.target(task).exists())
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'held')
        self.assertTrue(self.store.db.execute("SELECT 1 FROM problems WHERE code='file-scan-failed'").fetchone())

    def test_task_scan_does_not_hold_write_lock(self):
        task = self.job(closed=False)
        original = scan_process.run
        def check_writer(*args, **kwargs):
            return original(scan_with_writer, *args[1:], **kwargs)
        with patch.object(scan_process, 'run', side_effect=check_writer):
            _, stamp = close_saves.scan_task_references(self.store, task['id'], {})
        with self.store.transaction():
            self.assertTrue(close_saves.references_current(self.store, stamp))

    def test_new_reference_invalidates_snapshot(self):
        task = self.job(closed=False)
        _, stamp = close_saves.scan_task_references(self.store, task['id'], {})
        with sqlite3.connect(str(self.state / 'state.sqlite')) as other:
            other.execute('UPDATE tasks SET notes=? WHERE id=?', ('results/task-%d/file' % task['id'], task['id']))
        with self.store.transaction():
            self.assertFalse(close_saves.references_current(self.store, stamp))

    def test_changed_journal_blocks_result_move(self):
        task = self.job(closed=False)
        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        task = self.store.task_get(task['id'])
        root = self.state / 'results' / ('task-' + str(task['id']))
        root.mkdir(parents=True, exist_ok=True)
        result = root / 'large.bin'
        with result.open('wb') as handle:
            handle.truncate(11 * 1024 * 1024)
        original = close_saves.async_scan_task_references
        async def change_journal(*args, **kwargs):
            value = await original(*args, **kwargs)
            (root / 'moved-to-scratch.txt').write_text('new record\n')
            return value
        with patch.object(close_saves, 'async_scan_task_references', side_effect=change_journal):
            asyncio.run(close_saves.trim_results(self.store, task, self.project, {}))
        self.assertTrue(result.exists())
        self.assertEqual((root / 'moved-to-scratch.txt').read_text(), 'new record\n')

    def test_new_worker_after_precondition_scan_blocks_result_move(self):
        task = self.job(closed=False)
        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        task = self.store.task_get(task['id'])
        root = self.state / 'results' / ('task-' + str(task['id']))
        root.mkdir(parents=True, exist_ok=True)
        result = root / 'large.bin'
        with result.open('wb') as handle:
            handle.truncate(11 * 1024 * 1024)
        original = close_saves.async_scan_task_references
        async def start_worker(*args, **kwargs):
            with self.store.db:
                self.store.db.execute('''INSERT INTO workers(task,topic,provider,prompt,created,updated,status)
                    VALUES (?,?,'codex','fixture',1,1,'running')''', (task['id'], task['topic']))
            return await original(*args, **kwargs)
        with patch.object(close_saves, 'async_scan_task_references', side_effect=start_worker):
            asyncio.run(close_saves.trim_results(self.store, task, self.project, {}))
        self.assertTrue(result.exists())
        self.assertFalse((root / 'moved-to-scratch.txt').exists())

    def test_changed_file_after_digest_blocks_result_move(self):
        task = self.job(closed=False)
        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        task = self.store.task_get(task['id'])
        root = self.state / 'results' / ('task-' + str(task['id']))
        root.mkdir(parents=True, exist_ok=True)
        result = root / 'large.bin'
        with result.open('wb') as handle:
            handle.truncate(11 * 1024 * 1024)
        original = close_saves.async_cleanup_scan
        async def replace_after_digest(store, function, *args):
            value = await original(store, function, *args)
            if function is close_saves.digests_with_stamps:
                with result.open('r+b') as handle:
                    handle.write(b'changed')
            return value
        with patch.object(close_saves, 'async_cleanup_scan', side_effect=replace_after_digest):
            asyncio.run(close_saves.trim_results(self.store, task, self.project, {}))
        self.assertTrue(result.exists())
        with result.open('rb') as handle:
            self.assertEqual(handle.read(7), b'changed')
        self.assertFalse((root / 'moved-to-scratch.txt').exists())

    def test_protected_result_loop_yields_to_event_loop(self):
        task = self.job(closed=False)
        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        task = self.store.task_get(task['id'])
        root = self.state / 'results' / ('task-' + str(task['id']))
        root.mkdir(parents=True, exist_ok=True)
        entries = [(root / ('file-%d' % i), 11 * 1024 * 1024) for i in range(64)]
        seen = []
        async def scan(store, function, *args):
            if function is close_saves.file_entries:
                return entries
            if function is close_saves.trim_groups:
                asyncio.get_running_loop().call_soon(seen.append, 'tick')
            return function(*args)
        async def check():
            with patch.object(close_saves, 'async_cleanup_scan', side_effect=scan):
                await close_saves.trim_results(self.store, task, self.project,
                                               {str(task['id']): {'results/task-%d' % task['id']}})
            self.assertEqual(seen, ['tick'])
        asyncio.run(check())

    def test_uncommitted_fade_claim_recovers_staged_file(self):
        task = self.job(closed=False)
        staging = self.state / 'scratch' / ('task-' + str(task['id'])) / 'results-fade'
        name = 'results-fade-claim-' + uuid.uuid4().hex
        claim = staging.parent / name
        claim.mkdir(parents=True)
        (claim / 'data').write_text('retained')
        with self.store.db:
            self.store.db.execute('INSERT INTO results_fade_claims VALUES (?,?,0)', (task['id'], name))
        asyncio.run(close_saves.recover_fade_claim(self.store, task['id']))
        self.assertEqual((staging / 'data').read_text(), 'retained')
        self.assertFalse(claim.exists())
        self.assertIsNone(self.store.db.execute('SELECT 1 FROM results_fade_claims WHERE task=?', (task['id'],)).fetchone())

    def test_fade_claim_collision_preserves_both_directories(self):
        task = self.job(closed=False)
        staging = self.state / 'scratch' / ('task-' + str(task['id'])) / 'results-fade'
        name = 'results-fade-claim-' + uuid.uuid4().hex
        claim = staging.parent / name
        staging.mkdir(parents=True)
        claim.mkdir()
        with self.store.db:
            self.store.db.execute('INSERT INTO results_fade_claims VALUES (?,?,0)', (task['id'], name))
        with self.assertRaises(ValueError):
            asyncio.run(close_saves.recover_fade_claim(self.store, task['id']))
        self.assertTrue(staging.exists())
        self.assertTrue(claim.exists())

    def test_reference_added_during_fade_claim_restores_file(self):
        task = self.job(closed=False)
        other = self.job(closed=False)
        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        root = self.state / 'results' / ('task-' + str(task['id']))
        root.mkdir(parents=True, exist_ok=True)
        result = root / 'artifact.bin'
        result.write_bytes(b'retained')
        original = close_saves.async_cleanup_scan
        count = 0
        async def add_reference(store, function, *args):
            nonlocal count
            value = await original(store, function, *args)
            if function is close_saves.scan_fade_restore:
                count += 1
                if count == 2:
                    with self.store.db:
                        self.store.db.execute('UPDATE tasks SET notes=? WHERE id=?', (str(result), other['id']))
            return value
        with patch.object(close_saves, 'async_cleanup_scan', side_effect=add_reference):
            asyncio.run(close_saves.fade_results(self.store))
        self.assertIsNone(self.store.db.execute('SELECT 1 FROM results_fade_claims WHERE task=?', (task['id'],)).fetchone())
        asyncio.run(close_saves.fade_results(self.store))
        self.assertEqual(result.read_bytes(), b'retained')

    def test_fade_claim_recovers_when_job_open(self):
        task = self.job(closed=False)
        root = self.state / 'results' / ('task-' + str(task['id']))
        root.mkdir(parents=True, exist_ok=True)
        staging = self.state / 'scratch' / ('task-' + str(task['id'])) / 'results-fade'
        name = 'results-fade-claim-' + uuid.uuid4().hex
        claim = staging.parent / name
        record = {'path': 'artifact.bin', 'bytes': 4}
        staged = close_saves.fade_stage(claim, record)
        staged.mkdir(parents=True)
        (staged / 'intent.json').write_text(json.dumps(record))
        (staged / 'data').write_bytes(b'kept')
        with self.store.db:
            self.store.db.execute('INSERT INTO results_fade_claims VALUES (?,?,1)', (task['id'], name))
        asyncio.run(close_saves.fade_results(self.store))
        self.assertEqual((root / 'artifact.bin').read_bytes(), b'kept')
        self.assertIsNone(self.store.db.execute('SELECT 1 FROM results_fade_claims WHERE task=?', (task['id'],)).fetchone())

    def test_precondition_scan_survives_sibling_writes_in_shared_parent(self):
        task = self.job(closed=False)
        with self.store.db:
            worker = self.store.db.execute('''INSERT INTO workers(id,task,topic,provider,prompt,status,created,updated)
                VALUES (?,?,?,'claude','work','done',1,1)''',
                (900000 + uuid.uuid4().int % 90000, task['id'], task['topic'])).lastrowid
        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        task = self.store.task_get(task['id'])
        target = close_saves.ws._workspace_target(self.store.directory, task['id'])
        temp = close_saves.roots(self.store, task)[2]
        self.assertEqual(temp.name, 'torii-w' + str(worker))
        self.assertFalse(temp.exists())
        scratch = close_saves.scratch(self.store, task['id'])
        scratch.parent.mkdir(exist_ok=True)
        scratch.rmdir()
        stamp, identities = asyncio.run(close_saves.prepare_preconditions(self.store, task))
        (target.parent / 'task-sibling').mkdir()
        sibling = temp.parent / ('torii-sibling-' + uuid.uuid4().hex)
        sibling.mkdir()
        try:
            with self.store.transaction():
                self.assertTrue(close_saves.preconditions_current(self.store, stamp, identities, target))
                self.assertTrue(close_saves.preconditions_current(self.store, stamp, identities, temp))
        finally:
            sibling.rmdir()
        scratch.parent.rename(scratch.parent.with_name('scratch-moved'))
        scratch.parent.mkdir()
        with self.store.transaction():
            self.assertFalse(close_saves.preconditions_current(self.store, stamp, identities, scratch))

    def test_precondition_scan_is_invalidated_by_database_change(self):
        task = self.job(closed=False)
        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        task = self.store.task_get(task['id'])
        path = close_saves.ws._workspace_target(self.store.directory, task['id'])
        stamp, identities = asyncio.run(close_saves.prepare_preconditions(self.store, task))
        with sqlite3.connect(str(self.state / 'state.sqlite')) as other:
            other.execute('UPDATE tasks SET notes=? WHERE id=?', ('changed', task['id']))
        with self.store.transaction():
            self.assertFalse(close_saves.preconditions_current(self.store, stamp, identities, path))
