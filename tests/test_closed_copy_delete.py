import asyncio
from contextlib import ExitStack
import json
import os
from pathlib import Path
import sys
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import control_api, host_os, mcp, topics, workspaces
from coordinator.service import Service
from coordinator.workers import WorkerPool
from tests import test_worktree_cleanup as fixtures


def _gated_unlink(root, original, name, *args, **kwargs):
    if name == 'slow-file':
        (root / 'entered').touch()
        while not (root / 'release').exists():
            time.sleep(0.01)
        (root / 'returned').touch()
    return original(name, *args, **kwargs)


def _gated_delete_process():
    root, original = Path(sys.argv[2]), os.unlink
    with patch.object(host_os.os, 'unlink', side_effect=lambda *args, **kwargs:
                      _gated_unlink(root, original, *args, **kwargs)):
        host_os._delete_tree_main()


class _DeletionGate:
    def __init__(self, root):
        self.root = root
        self.patches = ExitStack()

    def __enter__(self):
        self.root.mkdir()
        original = os.unlink
        self.patches.enter_context(patch.object(host_os.os, 'unlink', side_effect=lambda *args, **kwargs:
                                   _gated_unlink(self.root, original, *args, **kwargs)))
        self.patches.enter_context(patch.object(host_os, '_delete_tree_command', side_effect=self.command))
        self.watchdog = threading.Timer(30, self.expire)
        self.watchdog.start()
        return self

    def __exit__(self, *error):
        self.release()
        self.watchdog.cancel()
        self.watchdog.join()
        self.patches.close()

    def command(self, path):
        return [sys.executable, '-c',
                'from tests.test_closed_copy_delete import _gated_delete_process; _gated_delete_process()',
                str(path), str(self.root)]

    def expire(self):
        (self.root / 'expired').touch()
        self.release()

    def release(self):
        (self.root / 'release').touch()

    def assert_waiting(self, case):
        case.assertFalse((self.root / 'expired').exists(),
                         'The event loop or shutdown waited for the stalled delete until the watchdog released it')
        case.assertFalse((self.root / 'returned').exists(), 'The blocked filesystem call returned before loop progress')

    async def entered(self, case, deletion):
        while not (self.root / 'entered').exists():
            case.assertFalse(deletion.done(), 'Deletion ended before reaching the controlled filesystem stall')
            self.assert_waiting(case)
            await asyncio.sleep(0.001)
        self.assert_waiting(case)


class ClosedCopyDeleteTests(unittest.TestCase):
    setUp = fixtures.WorktreeCleanupTests.setUp
    tearDown = fixtures.WorktreeCleanupTests.tearDown
    git = fixtures.WorktreeCleanupTests.git
    call = fixtures.WorktreeCleanupTests.call
    target = fixtures.WorktreeCleanupTests.target
    worker = fixtures.WorktreeCleanupTests.worker
    commit = fixtures.WorktreeCleanupTests.commit
    registrations = fixtures.WorktreeCleanupTests.registrations
    git_directory = fixtures.WorktreeCleanupTests.git_directory
    submodule_job = fixtures.WorktreeCleanupTests.submodule_job

    def job(self, closed=False, **fields):
        return fixtures.WorktreeCleanupTests.job(self, closed=closed, **fields)

    def test_writable_app_bundle_deletion_does_not_require_chmod_permission(self):
        target = self.root / 'copy'
        bundle = target / '.cache' / 'TestDriver.app' / 'Contents'
        bundle.mkdir(parents=True)
        (bundle / 'payload').write_bytes(b'cache')
        for path in (target, bundle.parent.parent, bundle.parent, bundle):
            path.chmod(0o755)
        with patch('coordinator.host_os.os.fchmod', side_effect=PermissionError('App metadata change denied')):
            removed = asyncio.run(host_os.delete_tree(target))
        self.assertEqual(removed, {'bytes': 5, 'files': 1})
        self.assertFalse(target.exists())

    def enable(self):
        with self.store.db:
            self.store.put('closed_copy_delete', True)

    def close_job(self, task, status='done'):
        return self.call('tasks.update', task=task['id'], status=status)

    def drain(self, sweep=False):
        asyncio.run(workspaces.cleanup_closed_copies(self.store, sweep=sweep))

    def stages(self):
        root = self.state / 'worktree-deletions'
        stages = []
        for stage in root.iterdir() if root.is_dir() else []:
            try:
                if (stage / 'record.json').is_file():
                    stages.append((stage, json.loads((stage / 'record.json').read_text())))
            except FileNotFoundError:
                continue
        return stages

    def service(self):
        service = Service.__new__(Service)
        service.store = self.store
        service.wakes = {}
        service.control_tasks = {}
        service.stop_tasks = {}
        return service

    def assert_kept(self, task, reason):
        answer = self.close_job(task)
        self.assertIn('kept because', answer.text)
        self.assertIn(reason, answer.text)
        self.assertTrue(self.target(task).is_dir())
        self.assertIn('worktree ' + str(self.target(task)) + '\n', self.registrations())

    def test_stage_helper_skips_stage_removed_between_check_and_read(self):
        self.enable()
        task = self.job()
        self.close_job(task)
        stage, record = self.stages()[0]
        original = Path.read_text
        raced = []

        def disappear(path, *args, **kwargs):
            if path == stage / 'record.json':
                raced.append(path)
                asyncio.run(host_os.delete_tree(stage))
            return original(path, *args, **kwargs)

        with patch.object(Path, 'read_text', disappear):
            self.assertEqual(self.stages(), [])
        self.assertEqual(raced, [stage / 'record.json'])
        self.assertFalse(stage.exists())

    def test_product_scan_skips_stage_removed_between_check_and_read(self):
        self.enable()
        first, second = self.job(), self.job()
        self.close_job(first)
        self.close_job(second)
        stages = self.stages()
        stage = next(path for path, record in stages if record['task'] == first['id'])
        original = Path.read_text
        raced, errors = [], []

        def disappear(path, *args, **kwargs):
            if path == stage / 'record.json':
                raced.append(path)
                asyncio.run(host_os.delete_tree(stage))
            return original(path, *args, **kwargs)

        with patch.object(Path, 'read_text', disappear):
            remaining = list(workspaces._deletion_stages(self.state, on_error=lambda *error: errors.append(error)))
        self.assertEqual(raced, [stage / 'record.json'])
        self.assertEqual(errors, [])
        self.assertEqual([record['task'] for path, record in remaining], [second['id']])
        self.drain()
        self.assertEqual(self.stages(), [])

    def test_closed_copy_switch_defaults_off_independently_of_worker_temp(self):
        self.assertIs(self.call('settings.get').data['closed_copy_delete'], False)
        self.store.put('worker_temp', False)
        for enabled in (True, False):
            self.call('cleanup.delete', enabled=enabled)
            settings = self.call('settings.get').data
            self.assertIs(settings['closed_copy_delete'], enabled)
            self.assertNotIn('worker_temp', settings)

    def test_both_cleanup_loops_remove_owned_copies_and_stop_with_service(self):
        self.call('cleanup.delete', enabled=True)
        task = self.job()
        target = self.target(task)
        worker = self.worker(task['id'], target)
        self.assert_kept(task, 'unfinished worker')
        results = Path(self.call('tasks.get', task=task['id']).data['results'])
        (results / 'proof').write_text('retained')
        temp = self.root / 'worker-temp'
        temp.mkdir()
        scratch = temp / ('torii-w' + str(worker))
        scratch.mkdir()
        (scratch / 'cache').write_text('disposable')
        self.store.worker_complete(worker, {'success': True})
        service = self.service()
        service.workers = WorkerPool(self.store, SimpleNamespace(), self.root)
        service.pair_only = True
        service.feed_tasks = {}
        service.worker_tasks = {}
        service.sessions = {}
        service.extension = SimpleNamespace(close=AsyncMock())

        async def run():
            loops = [asyncio.create_task(service.worktree_cleanup_loop()),
                     asyncio.create_task(service.workers_loop())]
            try:
                deadline = time.monotonic() + 5
                while target.exists() or scratch.exists() or self.stages() or service.workers.storage.tasks:
                    self.assertLess(time.monotonic(), deadline)
                    await asyncio.sleep(0.001)
                self.assertEqual(list(temp.iterdir()), [])
                self.assertEqual((results / 'proof').read_text(), 'retained')
                self.assertEqual(self.git('rev-parse', 'torii/task-' + str(task['id'])).stdout.strip(), self.base)
            finally:
                await service.shutdown(loops)
                self.assertTrue(all(loop.cancelled() for loop in loops))
                self.store.close()
                self.assertTrue(service.workers.storage.closed)

        with patch('coordinator.worker_storage.temp_base', return_value=temp):
            asyncio.run(run())
        self.assertFalse(self.trash.exists())

    def test_clean_close_deletes_without_age_and_preserves_git_and_outside_link(self):
        self.enable()
        task = self.job()
        target = self.target(task)
        (target / '.gitignore').write_text('.env\n/ leading /\n')
        self.git('add', '.gitignore', cwd=target)
        tip = self.commit(target)
        spaced = target / ' leading '
        spaced.mkdir()
        (spaced / 'cache').write_text('ignored cache')
        (target / 'README.md').write_text('stash content\n')
        self.git('stash', 'push', '-qm', 'shared', cwd=target)
        stash = self.git('rev-parse', 'refs/stash').stdout.strip()
        outside = self.root / 'outside'
        outside.mkdir()
        (outside / 'keep').write_text('outside bytes')
        ignored = target / '.env'
        ignored.mkdir()
        (ignored / 'external').symlink_to(outside, target_is_directory=True)
        readonly = ignored / 'readonly'
        readonly.mkdir()
        (readonly / 'file').write_text('ignored bytes')
        readonly.chmod(0o555)
        answer = self.close_job(task)
        self.assertIn('code copy was deleted', answer.text)
        self.assertFalse(target.exists())
        self.assertNotIn('worktree ' + str(target) + '\n', self.registrations())
        self.assertEqual(self.git('rev-parse', 'torii/task-' + str(task['id'])).stdout.strip(), tip)
        self.assertEqual(self.git('rev-parse', 'refs/stash').stdout.strip(), stash)
        self.assertEqual(self.store.task_get(task['id'])['worktree'], str(target))
        with self.assertLogs('coordinator.workspaces', 'INFO') as logs:
            self.drain()
        line = next(line for line in logs.output if 'worktree cleanup deleted' in line)
        self.assertIn('ignored=[" leading ", ".env"]', line)
        self.assertRegex(line, r'bytes=[1-9][0-9]* files=[1-9][0-9]*')
        self.assertEqual((outside / 'keep').read_text(), 'outside bytes')
        self.assertEqual(self.stages(), [])
        self.assertFalse(self.trash.exists())

    def test_reopen_recreates_same_path_branch_and_head_before_byte_removal(self):
        self.enable()
        task = self.job(nested=True)
        target = self.target(task)
        tip = self.commit(target)
        saved = self.store.task_get(task['id'])['worktree']
        self.close_job(task)
        self.assertFalse(target.exists(), 'Close must remove the old copy before reopen')
        self.call('tasks.update', task=task['id'], status='open')
        result = self.call('worktree.create', task=task['id'])
        self.assertEqual(result.data['cwd'], saved)
        self.assertEqual(result.data['base_commit'], tip)
        self.assertEqual(result.data['branch'], 'torii/task-' + str(task['id']))
        self.assertFalse(result.data['fetched'])
        self.drain()
        self.assertEqual(self.git('rev-parse', 'HEAD', cwd=target).stdout.strip(), tip)
        self.assertEqual((target / 'README.md').read_text(), 'job commit\n')
        self.assertFalse(self.trash.exists())

    def test_pending_image_keeps_copy_until_delivered_or_retired_then_swept(self):
        self.enable()
        for status, settlement in (('done', 'delivered'), ('dropped', 'retired')):
            with self.subTest(status=status, settlement=settlement):
                task = self.job()
                target = self.target(task)
                image = target / '.env' / 'image.png'
                image.parent.mkdir()
                image.write_bytes(b'queued image')
                with self.store.db:
                    row_id = self.store.enqueue_report(fixtures.TOPIC, 'Queued image', image=str(image))
                answer = self.close_job(task, status=status)
                self.assertEqual(answer.text, 'Job %d updated.\nIts code copy is kept because '
                                 "a Telegram file queued from it hasn't been sent yet." % task['number'])
                self.assertEqual(self.store.get('closed_copy_state:' + str(task['id'])),
                                 {'state': 'kept', 'reason': "a Telegram file queued from it hasn't been sent yet"})
                self.drain()
                self.assertEqual(image.read_bytes(), b'queued image')
                self.assertEqual(self.store.get('closed_copy_cleanup_pending'), {})
                if settlement == 'delivered':
                    self.store.delivered(row_id, 100)
                else:
                    with self.store.db:
                        self.store.db.execute('UPDATE outbox SET retired=1 WHERE id=?', (row_id,))
                self.drain()
                self.assertTrue(target.is_dir())
                self.drain(sweep=True)
                self.assertFalse(target.exists())
                self.assertEqual(self.stages(), [])
                self.assertNotIn('worktree ' + str(target) + '\n', self.registrations())
                self.assertEqual(self.git('rev-parse', 'torii/task-' + str(task['id'])).stdout.strip(), self.base)

    def test_unavailable_image_stops_blocking_copy_cleanup(self):
        self.enable()
        task = self.job()
        target = self.target(task)
        with self.store.db:
            row_id = self.store.enqueue_report(fixtures.TOPIC, 'Missing image', image=str(target / 'missing.png'))
        self.assert_kept(task, "a Telegram file queued from it hasn't been sent yet")
        self.drain()
        self.store.image_unavailable(row_id)
        self.assertEqual(tuple(self.store.db.execute('SELECT delivered,retired,image FROM outbox WHERE id=?',
                                                    (row_id,)).fetchone()), (0, 0, None))
        self.drain(sweep=True)
        self.assertFalse(target.exists())
        self.assertEqual(self.stages(), [])

    def test_pending_image_paths_resolve_symlinks_and_parent_components(self):
        self.enable()
        for path_kind in ('symlink', 'parent'):
            with self.subTest(path_kind=path_kind):
                task = self.job()
                target = self.target(task)
                if path_kind == 'symlink':
                    link = self.root / ('image-link-' + str(task['id']))
                    link.symlink_to(target, target_is_directory=True)
                    image = link / 'README.md'
                else:
                    image = target / 'nested' / '..' / 'README.md'
                with self.store.db:
                    self.store.enqueue_report(fixtures.TOPIC, 'Queued image', image=str(image))
                self.assert_kept(task, "a Telegram file queued from it hasn't been sent yet")
                self.assertEqual(image.read_text(), 'base\n')

    def test_outside_images_and_settled_rows_do_not_block_copy_cleanup(self):
        self.enable()
        task = self.job()
        target = self.target(task)
        outside = target.with_name(target.name + '-outside')
        outside.mkdir()
        (outside / 'image.png').write_bytes(b'outside image')
        (target / '.env').symlink_to(outside, target_is_directory=True)
        with self.store.db:
            self.store.enqueue_report(fixtures.TOPIC, 'Outside image', image=str(outside / 'image.png'))
            self.store.enqueue_report(fixtures.TOPIC, 'Outside link', image=str(target / '.env' / 'image.png'))
            delivered = self.store.enqueue_report(fixtures.TOPIC, 'Delivered image', image=str(target / 'README.md'))
            retired = self.store.enqueue_report(fixtures.TOPIC, 'Retired image', image=str(target / 'README.md'))
            self.store.db.execute('UPDATE outbox SET retired=1 WHERE id=?', (retired,))
            self.store.enqueue_report(fixtures.TOPIC, 'Pending text')
        self.store.delivered(delivered, 100)
        answer = self.close_job(task)
        self.assertIn('code copy was deleted', answer.text)
        self.assertFalse(target.exists())
        self.drain(sweep=True)
        self.assertEqual((outside / 'image.png').read_bytes(), b'outside image')

    def test_image_queued_during_git_checks_is_rechecked_before_rename(self):
        self.enable()
        task = self.job()
        original = workspaces._cleanup_git_directory
        checked = 0

        async def queue_late(repository, target):
            nonlocal checked
            result = await original(repository, target)
            checked += 1
            if checked == 2:
                with self.store.db:
                    self.store.enqueue_report(fixtures.TOPIC, 'Late image', image=str(target / 'README.md'))
            return result

        with patch('coordinator.workspaces._cleanup_git_directory', side_effect=queue_late):
            self.assert_kept(task, "a Telegram file queued from it hasn't been sent yet")
        self.assertEqual(checked, 2)
        self.drain(sweep=True)
        self.assertEqual((self.target(task) / 'README.md').read_text(), 'base\n')
        self.assertEqual(self.stages(), [])

    def test_uncommitted_change_is_kept_then_manual_commit_is_swept(self):
        self.enable()
        task = self.job()
        target = self.target(task)
        (target / 'README.md').write_text('uncommitted\n')
        with self.assertLogs('coordinator.workspaces', 'INFO') as logs:
            answer = self.close_job(task)
            self.assertIn('uncommitted changes', answer.text)
            self.drain()
        self.assertIn('uncommitted changes', answer.text)
        self.assertIn('committed or discarded', answer.text)
        self.assertTrue(target.exists())
        self.assertEqual(sum('worktree cleanup kept' in line for line in logs.output), 1)
        tip = self.commit(target)
        self.drain(sweep=True)
        self.assertFalse(target.exists())
        self.assertEqual(self.git('rev-parse', 'torii/task-' + str(task['id'])).stdout.strip(), tip)

    def test_untracked_file_is_kept_then_discard_is_swept(self):
        self.enable()
        task = self.job()
        target = self.target(task)
        marker = target / 'unsaved'
        marker.write_text('unsaved')
        answer = self.close_job(task, status='dropped')
        self.assertIn('untracked files', answer.text)
        self.assertTrue(marker.exists())
        marker.rename(self.root / 'discarded-test-file')
        self.drain(sweep=True)
        self.assertFalse(target.exists())
        self.assertFalse(self.trash.exists())

    def test_last_worker_completion_triggers_deletion_without_sweep(self):
        self.enable()
        task = self.job()
        target = self.target(task)
        first = self.worker(task['id'], target)
        last = self.worker(task['id'], target)
        self.assert_kept(task, 'unfinished worker')
        self.store.worker_complete(first, {'success': True})
        self.drain()
        self.assertTrue(target.exists())
        self.store.worker_complete(last, {'success': True})
        service = self.service()
        asyncio.run(service.controls_once())
        self.assertTrue(service.wake('worktree_cleanup').is_set())
        self.drain()
        self.assertFalse(target.exists())
        self.assertNotIn(str(target), self.registrations())

    def test_worker_for_another_job_inside_copy_triggers_deletion(self):
        self.enable()
        task, other = self.job(), self.job()
        target = self.target(task)
        worker = self.worker(other['id'], target / 'nested')
        self.assert_kept(task, 'unfinished worker uses')
        self.drain()
        self.store.worker_complete(worker, {'success': True})
        self.drain()
        self.assertFalse(target.exists())
        self.assertTrue(self.target(other).exists())

    def test_locked_copy_is_kept(self):
        self.enable()
        task = self.job()
        self.git('worktree', 'lock', self.target(task))
        self.assert_kept(task, 'locked')

    def test_merge_in_progress_is_kept(self):
        self.enable()
        task = self.job()
        (self.git_directory(self.target(task)) / 'MERGE_HEAD').write_text(self.base + '\n')
        self.assert_kept(task, 'Git operation is in progress')

    def test_submodule_is_salvaged_before_staging(self):
        self.enable()
        task, tip = self.submodule_job(closed=False)
        self.call('tasks.update', task=task['id'], status='done')
        self.drain()
        self.assertFalse(self.target(task).exists())
        saved = next((self.state / 'results' / self.target(task).name / 'worktree-salvage').glob('salvage-*'))
        self.assertIn(tip, self.git('bundle', 'list-heads', saved / '1' / 'commits.bundle').stdout)

    def test_uninitialized_submodule_with_files_is_kept(self):
        self.enable()
        task, _ = self.submodule_job(initialized=False, closed=False)
        target = self.target(task)
        (target / 'sub' / 'notes.bin').write_bytes(b'unsaved\x00\xff')
        self.call('tasks.update', task=task['id'], status='done')
        self.drain()
        self.assertTrue(target.is_dir())
        self.assertEqual((target / 'sub' / 'notes.bin').read_bytes(), b'unsaved\x00\xff')
        self.assertIn('uninitialized submodule', self.store.get('closed_copy_state:' + str(task['id']))['reason'].lower())

    def test_saved_branch_hold_creates_no_salvage_on_repeated_sweeps(self):
        self.enable()
        task, _ = self.submodule_job(closed=False)
        target = self.target(task)
        (target / 'sub' / 'local.bin').write_bytes(b'x' * 100000)
        self.git('checkout', '-qb', 'side-branch', cwd=target)
        self.call('tasks.update', task=task['id'], status='done')
        self.drain(sweep=True)
        self.drain(sweep=True)
        self.assertTrue(target.is_dir())
        saved = self.state / 'results' / target.name / 'worktree-salvage'
        self.assertEqual(list(saved.glob('salvage-*')), [])

    def test_submodule_with_older_stash_keeps_closed_copy(self):
        self.enable()
        task, _ = self.submodule_job(closed=False)
        target = self.target(task)
        sub = target / 'sub'
        source = next(path for path in sub.iterdir() if path.name != '.git' and path.is_file())
        source.write_text('older local work\n')
        self.git('stash', 'push', '-qm', 'older work', cwd=sub)
        older = self.git('rev-parse', 'refs/stash', cwd=sub).stdout.strip()
        source.write_text('newer local work\n')
        self.git('stash', 'push', '-qm', 'newer work', cwd=sub)
        self.call('tasks.update', task=task['id'], status='done')
        self.drain()
        self.assertTrue(target.is_dir())
        self.assertIn(older, self.git('rev-list', '--reflog', cwd=sub).stdout)
        self.assertFalse(list((self.state / 'results' / target.name / 'worktree-salvage').glob('salvage-*')))

    def test_detached_head_is_kept(self):
        self.enable()
        task = self.job()
        self.git('checkout', '--detach', '-q', cwd=self.target(task))
        self.assert_kept(task, 'no attached local branch')

    def test_changed_inode_is_kept(self):
        self.enable()
        task = self.job()
        target = self.target(task)
        original = workspaces._cleanup_git_directory
        changed = False

        async def replace(repository, path):
            nonlocal changed
            result = await original(repository, path)
            if not changed:
                changed = True
                old = self.root / 'old-copy'
                path.rename(old)
                path.mkdir()
                for entry in old.iterdir():
                    entry.rename(path / entry.name)
            return result

        with patch('coordinator.workspaces._cleanup_git_directory', side_effect=replace):
            self.assert_kept(task, 'folder changed')
        self.assertTrue((target / 'README.md').is_file())

    def test_root_symlink_is_kept_without_touching_its_target(self):
        self.enable()
        task = self.job()
        target = self.target(task)
        outside = self.root / 'moved-copy'
        target.rename(outside)
        target.symlink_to(outside, target_is_directory=True)
        answer = self.close_job(task)
        self.assertIn('kept because', answer.text)
        self.assertTrue(target.is_symlink())
        self.assertEqual((outside / 'README.md').read_text(), 'base\n')
        self.assertFalse(self.trash.exists())

    def crash_after_copy_rename(self, task):
        self.close_job(task)
        self.assertFalse(self.target(task).exists(), 'Close did not stage the copy before a simulated crash')
        stage, record = self.stages()[0]
        (stage / 'git').rename(record['git_dir'])
        record['detached'] = False
        workspaces._save_deletion(stage, record)
        return stage

    def test_crash_between_renames_is_recovered_on_next_pass(self):
        self.enable()
        task = self.job()
        stage = self.crash_after_copy_rename(task)
        self.assertIn(str(self.target(task)), self.registrations())
        self.store.close()
        self.store = fixtures.Store(self.state, token_file=self.root / 'no-token')
        self.drain()
        self.assertFalse(stage.exists())
        self.assertNotIn(str(self.target(task)), self.registrations())
        self.assertEqual(self.git('rev-parse', 'torii/task-' + str(task['id'])).stdout.strip(), self.base)

    def test_crash_between_renames_is_recovered_before_reopen(self):
        self.enable()
        task = self.job()
        self.crash_after_copy_rename(task)
        self.call('tasks.update', task=task['id'], status='open')
        result = self.call('worktree.create', task=task['id'])
        self.assertEqual(result.data['base_commit'], self.base)
        self.drain()
        self.assertEqual((self.target(task) / 'README.md').read_text(), 'base\n')
        self.assertFalse(self.trash.exists())

    def test_pruned_registration_after_crash_allows_unrelated_create_and_cleanup(self):
        self.enable()
        task = self.job()
        stage = self.crash_after_copy_rename(task)
        record = json.loads((stage / 'record.json').read_text())
        self.git('worktree', 'prune', '--expire=now')
        self.assertFalse(Path(record['git_dir']).exists())
        self.store.close()
        self.store = fixtures.Store(self.state, token_file=self.root / 'no-token')
        other = self.job()
        self.assertEqual((self.target(other) / 'README.md').read_text(), 'base\n')
        self.drain()
        self.assertFalse(stage.exists())
        self.assertEqual(self.store.get('closed_copy_state:' + str(task['id']))['state'], 'gone')
        self.assertEqual(self.git('rev-parse', 'torii/task-' + str(task['id'])).stdout.strip(), self.base)

    def test_topic_close_triggers_dropped_copies_after_commit_only_when_enabled(self):
        for enabled in (True, False):
            with self.subTest(enabled=enabled):
                with self.store.db:
                    self.store.put('closed_copy_delete', enabled)
                    self.store.put('closed_copy_cleanup_pending', {})
                first, second = self.job(), self.job()
                original_closed = self.job()
                self.close_job(original_closed)
                with self.store.db:
                    self.store.put('closed_copy_cleanup_pending', {})
                    params = {'topic': fixtures.TOPIC, '_chat': self.store.chat(), '_topic': fixtures.TOPIC}
                    progress = {'phase': 'stopped'}
                    request_id = self.store.db.execute('''INSERT INTO service_requests
                        (op,params,result,state,created,updated) VALUES (?,?,?,?,?,?)''',
                        ('topic.close', json.dumps(params), json.dumps(progress), 'queued',
                         time.time(), time.time())).lastrowid
                request = dict(self.store.db.execute(
                    'SELECT * FROM service_requests WHERE id=?', (request_id,)).fetchone())
                observed = []
                original = workspaces.request_closed_copy_cleanup

                def trigger(store, task_id=None, cwd=None):
                    observed.append((task_id, store.db.in_transaction))
                    return original(store, task_id, cwd)

                with patch.object(workspaces, 'request_closed_copy_cleanup', side_effect=trigger):
                    asyncio.run(topics.advance(self.service(), request))
                self.assertEqual(self.store.task_get(first['id'])['status'], 'dropped')
                self.assertEqual(self.store.task_get(second['id'])['status'], 'dropped')
                if enabled:
                    self.assertEqual(observed, [(first['id'], False), (second['id'], False)])
                    self.assertEqual(set(self.store.get('closed_copy_cleanup_pending')),
                                     {str(first['id']), str(second['id'])})
                    self.drain()
                    self.assertFalse(self.target(first).exists())
                    self.assertFalse(self.target(second).exists())
                else:
                    self.assertEqual(self.store.get('closed_copy_cleanup_pending'), {})
                    self.drain()
                    self.assertTrue(self.target(first).exists())
                    self.assertTrue(self.target(second).exists())

    def test_service_start_recovers_staging_and_sweeps_young_closed_copy(self):
        self.enable()
        staged = self.job()
        stage = self.crash_after_copy_rename(staged)
        young = self.job()
        self.store.task_update(young['id'], status='done')
        service = self.service()

        async def run():
            task = asyncio.create_task(service.worktree_cleanup_loop())
            try:
                deadline = time.monotonic() + 5
                while stage.exists() or self.target(young).exists() or self.stages():
                    self.assertLess(time.monotonic(), deadline)
                    await asyncio.sleep(0.001)
                self.assertFalse(self.target(staged).exists())
                self.assertEqual(self.git('rev-parse', 'torii/task-' + str(young['id'])).stdout.strip(), self.base)
            finally:
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
        asyncio.run(run())
        self.assertFalse(self.trash.exists())

    def test_large_deletion_yields_and_service_step_cancels_promptly(self):
        self.enable()
        task = self.job()
        ignored = self.target(task) / '.env'
        ignored.mkdir()
        (ignored / 'slow-file').write_text('controlled filesystem stall')
        for index in range(6000):
            (ignored / str(index)).write_text('regenerable\n')
        self.close_job(task)
        self.assertFalse(self.target(task).exists(), 'Close must stage the copy before background deletion')
        stage, record = self.stages()[0]
        service = self.service()
        service.feed_tasks = {}
        service.worker_tasks = {}
        service.sessions = {}
        service.extension = SimpleNamespace(close=AsyncMock())
        children = []

        async def run():
            spawn = asyncio.create_subprocess_exec

            async def spawned(*args, **kwargs):
                child = await spawn(*args, **kwargs)
                children.append(child)
                return child

            loop = None
            try:
                with patch('coordinator.host_os.asyncio.create_subprocess_exec', side_effect=spawned):
                    loop = asyncio.create_task(service.worktree_cleanup_loop())
                    await gate.entered(self, loop)
                for turn in range(4):
                    await asyncio.sleep(0)
                    gate.assert_waiting(self)
                self.assertFalse(loop.done())
                await service.shutdown([loop])
                self.assertTrue(loop.cancelled())
                while any(child.returncode is None for child in children):
                    gate.assert_waiting(self)
                    await asyncio.sleep(0.001)
                gate.assert_waiting(self)
                self.assertTrue((stage / 'copy').exists())
            finally:
                if loop:
                    loop.cancel()
                    await asyncio.gather(loop, return_exceptions=True)
        with _DeletionGate(self.root / 'delete-gate') as gate:
            asyncio.run(run())
            gate.assert_waiting(self)
            self.assertTrue(children)
            self.assertTrue(all(child.returncode is not None for child in children))
        self.drain()
        self.assertFalse(stage.exists())

    def test_delete_helper_refuses_root_link_and_never_traverses_child_link(self):
        outside = self.root / 'outside'
        outside.mkdir()
        (outside / 'keep').write_text('keep')
        link = self.root / 'link'
        link.symlink_to(outside, target_is_directory=True)
        self.assertTrue(callable(getattr(host_os, 'delete_tree', None)), 'Missing cancellable no-follow tree deletion')
        with self.assertRaises(OSError):
            asyncio.run(host_os.delete_tree(link))
        self.assertTrue(link.is_symlink())
        tree = self.root / 'owned'
        tree.mkdir()
        (tree / 'child').symlink_to(outside, target_is_directory=True)
        counts = asyncio.run(host_os.delete_tree(tree))
        self.assertEqual(counts['files'], 1)
        self.assertEqual((outside / 'keep').read_text(), 'keep')
        self.assertFalse(tree.exists())

    def test_disabled_switch_preserves_young_copy_and_old_cleanup(self):
        self.enable()
        self.call('cleanup.delete', enabled=False)
        task = self.job()
        answer = self.close_job(task)
        self.assertEqual(answer.text, 'Job 1 updated.')
        asyncio.run(workspaces.cleanup_closed_worktrees(self.store))
        self.assertTrue(self.target(task).exists())
        self.assertFalse(self.trash.exists())
        with self.store.db:
            self.store.db.execute('UPDATE tasks SET updated=? WHERE id=?', (time.time() - 86401, task['id']))
        asyncio.run(workspaces.cleanup_closed_worktrees(self.store))
        self.assertTrue((self.trash / self.target(task).name).is_dir())

    def test_cli_switch_settings_and_mcp_refusal_leave_tools_unchanged(self):
        before = mcp.tools_hash()
        self.assertIs(self.call('settings.get').data.get('closed_copy_delete'), False)
        self.call('cleanup.delete', enabled=True)
        self.assertIs(self.call('settings.get').data['closed_copy_delete'], True)
        with self.store.db:
            denied = control_api.call(self.store, 'cleanup.delete', {'enabled': False}, source='mcp')
        self.assertFalse(denied.ok)
        self.assertIn('local CLI', denied.text)
        self.assertIs(self.store.get('closed_copy_delete'), True)
        self.assertNotIn('cleanup_delete', mcp.TOOLS)
        self.assertEqual(mcp.tools_hash(), before)
        self.assertEqual(control_api.BY_ID['cleanup.delete'].kind, control_api.WRITE)

    def test_close_over_mcp_reports_kept_reason_and_deleted_copy(self):
        self.enable()
        task = self.job()
        target = self.target(task)
        (target / 'README.md').write_text('dirty')
        with self.store.db:
            result = control_api.call(self.store, 'tasks.update', {'task': task['id'], 'status': 'done'},
                                      source='mcp', topic=fixtures.TOPIC)
        self.assertTrue(result.ok, result.text)
        self.assertIn('uncommitted changes', result.text)
        self.git('restore', 'README.md', cwd=target)
        with self.store.db:
            result = control_api.call(self.store, 'tasks.update', {'task': task['id'], 'status': 'dropped'},
                                      source='mcp', topic=fixtures.TOPIC)
        self.assertTrue(result.ok, result.text)
        self.assertIn('code copy was deleted', result.text)
        self.assertFalse(target.exists())
        self.drain()
        self.assertFalse(self.trash.exists())

    def test_staging_on_another_device_keeps_copy_and_registration(self):
        self.enable()
        task = self.job()
        target = self.target(task)
        original = Path.stat

        def other_device(path, *args, **kwargs):
            info = original(path, *args, **kwargs)
            if path.parent.name == 'worktree-deletions':
                return SimpleNamespace(st_dev=info.st_dev + 1)
            return info

        with patch.object(Path, 'stat', other_device):
            self.assert_kept(task, 'another filesystem')
        self.drain()
        self.assertEqual((target / 'README.md').read_text(), 'base\n')
        self.assertFalse(self.trash.exists())

    def test_failed_admin_rename_restores_whole_copy(self):
        self.enable()
        task = self.job()
        target = self.target(task)
        git_dir = self.git_directory(target)
        original = Path.rename

        def refuse(path, destination):
            if path == git_dir:
                raise OSError('registration rename refused')
            return original(path, destination)

        with patch.object(Path, 'rename', refuse):
            self.assert_kept(task, 'registration rename refused')
        self.drain()
        self.assertEqual((target / 'README.md').read_text(), 'base\n')
        self.assertIn(str(target), self.registrations())
        self.assertFalse(self.trash.exists())

    def test_late_git_lock_is_rechecked_before_rename(self):
        self.enable()
        task = self.job()
        original = workspaces._cleanup_git_directory
        checked = 0

        async def lock_late(repository, target):
            nonlocal checked
            result = await original(repository, target)
            checked += 1
            if checked == 2:
                (result[0] / 'locked').write_text('owner lock')
            return result

        with patch('coordinator.workspaces._cleanup_git_directory', side_effect=lock_late):
            self.assert_kept(task, 'lock changed')
        self.assertFalse(self.trash.exists())

    def test_disabled_switch_finishes_already_staged_bytes(self):
        self.enable()
        task = self.job()
        self.close_job(task)
        self.assertFalse(self.target(task).exists(), 'Close must stage the copy before background deletion')
        stage, record = self.stages()[0]
        self.call('cleanup.delete', enabled=False)
        asyncio.run(workspaces.cleanup_closed_worktrees(self.store))
        self.assertFalse(stage.exists())
        self.assertFalse(self.target(task).exists())
        self.assertEqual(self.git('rev-parse', 'torii/task-' + str(task['id'])).stdout.strip(), self.base)

    def test_enabled_sweep_has_no_ten_copy_limit(self):
        self.enable()
        tasks = [self.job() for _ in range(12)]
        for task in tasks:
            self.store.task_update(task['id'], status='done')
        asyncio.run(workspaces.cleanup_closed_worktrees(self.store))
        for task in tasks:
            self.assertFalse(self.target(task).exists())
            self.assertEqual(self.git('rev-parse', 'torii/task-' + str(task['id'])).stdout.strip(), self.base)
        self.assertFalse(self.trash.exists())

    def test_full_large_deletion_keeps_coroutine_running(self):
        self.enable()
        task = self.job()
        ignored = self.target(task) / '.env'
        ignored.mkdir()
        (ignored / 'slow-file').write_text('controlled filesystem stall')
        for index in range(4000):
            (ignored / str(index)).write_bytes(b'regenerable')
        self.close_job(task)
        self.assertFalse(self.target(task).exists(), 'Close must stage the large copy before byte removal')

        async def run():
            deletion = asyncio.create_task(workspaces.cleanup_closed_copies(self.store))
            try:
                await gate.entered(self, deletion)
                for turn in range(50):
                    await asyncio.sleep(0)
                    gate.assert_waiting(self)
                    self.assertFalse(deletion.done())
                deletion.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await deletion
                gate.assert_waiting(self)
            finally:
                deletion.cancel()
                await asyncio.gather(deletion, return_exceptions=True)
        with _DeletionGate(self.root / 'delete-gate') as gate:
            asyncio.run(run())
            gate.assert_waiting(self)
        self.drain()
        self.assertFalse(self.target(task).exists())
        self.assertEqual(self.stages(), [])

    def test_workers_spawn_recreates_deleted_copy_without_clone(self):
        self.enable()
        task = self.job()
        target = self.target(task)
        tip = self.commit(target)
        self.close_job(task)
        self.assertFalse(target.exists())
        self.call('tasks.update', task=task['id'], status='open')
        result = self.call('workers.spawn', task=task['id'], provider='codex', prompt='fixture prompt')
        worker = self.store.db.execute('SELECT cwd,status FROM workers WHERE id=?',
                                      (result.data['worker'],)).fetchone()
        self.assertEqual(worker['cwd'], str(target))
        self.assertEqual(worker['status'], 'queued')
        self.assertEqual(self.git('rev-parse', 'HEAD', cwd=target).stdout.strip(), tip)
        self.assertEqual(self.git('branch', '--show-current', cwd=target).stdout.strip(),
                         'torii/task-' + str(task['id']))

    def test_closing_job_cancels_queued_worker_using_another_closed_copy(self):
        self.enable()
        closed, owner = self.job(), self.job()
        worker = self.worker(owner['id'], self.target(closed), status='queued')
        self.assert_kept(closed, 'unfinished worker uses')
        self.close_job(owner)
        row = self.store.db.execute('SELECT status FROM workers WHERE id=?', (worker,)).fetchone()
        self.assertEqual(row['status'], 'interrupted')
        self.drain()
        self.assertFalse(self.target(closed).exists())
        self.assertFalse(self.target(owner).exists())

    def test_enabled_events_wake_service_already_waiting_under_old_policy(self):
        task = self.job()
        service = self.service()

        async def run():
            loop = asyncio.create_task(service.worktree_cleanup_loop())
            try:
                for _ in range(5):
                    await asyncio.sleep(0)
                self.assertTrue(self.target(task).is_dir())
                self.enable()
                self.store.task_update(task['id'], status='done')
                await service.controls_once()
                deadline = time.monotonic() + 5
                while self.target(task).exists() or self.stages():
                    self.assertLess(time.monotonic(), deadline)
                    await asyncio.sleep(0.001)
                self.assertNotIn(str(self.target(task)), self.registrations())
            finally:
                loop.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await loop
        asyncio.run(run())
        self.assertFalse(self.trash.exists())

    def test_worker_completion_during_close_does_not_lose_its_trigger(self):
        self.enable()
        task = self.job()
        worker = self.worker(task['id'], self.target(task))
        original = workspaces._cleanup_task_reason

        def complete_after_check(store, *args, **kwargs):
            reason = original(store, *args, **kwargs)
            if reason == 'job has an unfinished worker':
                store.worker_complete(worker, {'success': True})
            return reason

        with patch('coordinator.workspaces._cleanup_task_reason', side_effect=complete_after_check):
            self.assert_kept(task, 'unfinished worker')
        self.drain()
        self.assertFalse(self.target(task).exists())
        self.assertNotIn(str(self.target(task)), self.registrations())

    def test_switch_disabled_during_git_check_keeps_copy(self):
        self.enable()
        task = self.job()
        original = workspaces._cleanup_git_directory
        checked = 0

        async def disable_late(repository, target):
            nonlocal checked
            result = await original(repository, target)
            checked += 1
            if checked == 2:
                with self.store.db:
                    self.store.put('closed_copy_delete', False)
            return result

        with patch('coordinator.workspaces._cleanup_git_directory', side_effect=disable_late):
            self.assert_kept(task, 'closed copy deletion is off')
        self.assertFalse(self.trash.exists())
