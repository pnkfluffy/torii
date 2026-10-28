import asyncio
import json
import os
from pathlib import Path
import sys
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from coordinator import host_os, problems, workspaces
from coordinator.accounts import AccountBroker
from coordinator.providers import RunResult
from coordinator.service import Service
from coordinator.workers import WorkerPool
from tests import test_closed_copy_delete as copies
from tests import test_worktree_cleanup as fixtures


def _refused_delete_process():
    original = os.unlink

    def unlink(name, *args, **kwargs):
        if name == 'blocked-file':
            raise PermissionError(1, 'fixture deletion refused', name)
        return original(name, *args, **kwargs)

    with patch.object(host_os.os, 'unlink', side_effect=unlink):
        host_os._delete_tree_main()


class ClosedCopyRecoveryTests(unittest.TestCase):
    setUp = fixtures.WorktreeCleanupTests.setUp
    git = fixtures.WorktreeCleanupTests.git
    call = fixtures.WorktreeCleanupTests.call
    target = fixtures.WorktreeCleanupTests.target
    worker = fixtures.WorktreeCleanupTests.worker
    commit = fixtures.WorktreeCleanupTests.commit
    registrations = fixtures.WorktreeCleanupTests.registrations
    job = copies.ClosedCopyDeleteTests.job
    enable = copies.ClosedCopyDeleteTests.enable
    close_job = copies.ClosedCopyDeleteTests.close_job
    drain = copies.ClosedCopyDeleteTests.drain
    stages = copies.ClosedCopyDeleteTests.stages
    service = copies.ClosedCopyDeleteTests.service

    def tearDown(self):
        for directory, names, files in os.walk(str(self.root)):
            for name in names:
                path = Path(directory) / name
                if not path.is_symlink():
                    path.chmod(0o700)
        fixtures.WorktreeCleanupTests.tearDown(self)

    def test_delete_tree_removes_empty_and_full_mode_zero_without_following_links(self):
        outside = self.root / 'outside'
        outside.mkdir()
        (outside / 'keep').write_text('outside')
        outside_mode = outside.stat().st_mode
        for name, full in (('empty', False), ('full', True), ('root', True)):
            with self.subTest(shape=name):
                tree = self.root / name
                tree.mkdir()
                (tree / 'link').symlink_to(outside, target_is_directory=True)
                locked = tree / 'locked'
                locked.mkdir()
                if full:
                    (locked / 'bytes').write_text('saved bytes')
                locked.chmod(0)
                if name == 'root':
                    tree.chmod(0)
                asyncio.run(host_os.delete_tree(tree))
                self.assertFalse(tree.exists())
                self.assertEqual((outside / 'keep').read_text(), 'outside')
                self.assertEqual(outside.stat().st_mode, outside_mode)

    def test_delete_tree_rechecks_inode_after_chmod(self):
        tree = self.root / 'tree'
        child = tree / 'child'
        child.mkdir(parents=True)
        (child / 'keep').write_text('do not delete a moved directory')
        child.chmod(0o500)
        inode = child.stat().st_ino
        original = os.fchmod
        moved = self.root / 'moved'

        def replace(fd, mode):
            original(fd, mode)
            if os.fstat(fd).st_ino == inode:
                child.rename(moved)
                child.mkdir()

        with patch('coordinator.host_os.os.fchmod', side_effect=replace):
            with self.assertRaisesRegex(OSError, 'changed'):
                asyncio.run(host_os.delete_tree(tree))
        self.assertEqual((moved / 'keep').read_text(), 'do not delete a moved directory')

    def test_delete_tree_rechecks_device_after_chmod(self):
        tree = self.root / 'tree'
        child = tree / 'child'
        child.mkdir(parents=True)
        (child / 'keep').write_text('mounted bytes')
        child.chmod(0o500)
        inode = child.stat().st_ino
        original_chmod, original_stat = os.fchmod, os.fstat
        changed = False

        def chmod(fd, mode):
            nonlocal changed
            original_chmod(fd, mode)
            if original_stat(fd).st_ino == inode:
                changed = True

        def stat(fd):
            info = original_stat(fd)
            if changed and info.st_ino == inode:
                return SimpleNamespace(st_dev=info.st_dev + 1, st_ino=info.st_ino, st_mode=info.st_mode)
            return info

        with patch('coordinator.host_os.os.fchmod', side_effect=chmod), patch(
                'coordinator.host_os.os.fstat', side_effect=stat):
            with self.assertRaises(OSError):
                asyncio.run(host_os.delete_tree(tree))
        self.assertEqual((child / 'keep').read_text(), 'mounted bytes')

    def test_chmod_denial_names_the_operation_and_full_path(self):
        tree = self.root / 'tree'
        child = tree / 'child'
        child.mkdir(parents=True)
        child.chmod(0o500)
        with patch('coordinator.host_os.os.fchmod', side_effect=PermissionError(1, 'metadata denied')):
            with self.assertRaises(PermissionError) as failure:
                asyncio.run(host_os.delete_tree(tree))
        self.assertEqual(failure.exception.operation, 'fchmod')
        self.assertEqual(failure.exception.filename, str(child))
        self.assertIn('metadata denied', str(failure.exception))

    def test_background_delete_error_records_operation_and_filename(self):
        first, second, failed, later = self.failed_stage(True)
        copy = failed / 'copy'
        saved = failed / 'saved-copy'
        copy.rename(saved)
        copy.symlink_to(saved, target_is_directory=True)
        self.drain()
        record = next(record for stage, record in self.stages() if stage == failed)
        self.assertIn('open-directory', record['error'])
        self.assertIn(str(copy), record['error'])
        problem = self.store.db.execute("SELECT detail FROM problems WHERE code='closed-copy-delete-failed' ORDER BY id DESC LIMIT 1").fetchone()
        self.assertIn('open-directory', problem['detail'])
        self.assertIn(str(copy), problem['detail'])
        self.assertTrue((saved / 'README.md').is_file())
        self.assertFalse(later.exists())
        with self.assertRaises(OSError) as failure:
            asyncio.run(host_os.delete_tree_background(copy))
        self.assertEqual(failure.exception.operation, 'open-directory')
        self.assertEqual(failure.exception.filename, str(copy))

    def failed_stage(self, enabled):
        self.enable()
        first, second = self.job(), self.job()
        cache = self.target(first) / '.env'
        cache.mkdir()
        (cache / 'partial').write_text('partial progress')
        self.close_job(first)
        self.close_job(second)
        stages = {record['task']: stage for stage, record in self.stages()}
        with self.store.db:
            self.store.put('closed_copy_delete', enabled)
        return first, second, stages[first['id']], stages[second['id']]

    def test_delete_errors_save_progress_continue_throttle_and_retry_only_on_sweep(self):
        for enabled in (True, False):
            with self.subTest(enabled=enabled):
                first, second, failed, later = self.failed_stage(enabled)
                attempts = []

                async def refuse(path, *, progress=None):
                    if path == failed / 'copy':
                        attempts.append(path)
                        await host_os.delete_tree(path / '.env', progress=progress)
                        raise PermissionError('fixture deletion refused')
                    return await host_os.delete_tree(path, progress=progress)

                with patch.object(workspaces, 'delete_tree', side_effect=refuse):
                    self.drain()
                    self.assertFalse(later.exists(), 'One failed stage must not block the later clean copy')
                    record = json.loads((failed / 'record.json').read_text())
                    self.assertEqual(record['removed'], {'bytes': 16, 'files': 1})
                    state = self.store.get('closed_copy_state:' + str(first['id']))
                    self.assertIn('fixture deletion refused', state['reason'])
                    self.drain()
                    self.assertEqual(len(attempts), 1, 'Event passes must not retry a failed stage')
                    self.drain(sweep=True)
                    self.assertEqual(len(attempts), 2)
                    self.assertEqual(self.store.db.execute(
                        "SELECT COUNT(*) FROM problems WHERE code='closed-copy-delete-failed'").fetchone()[0], 1)
                self.drain(sweep=True)
                self.assertFalse(failed.exists())
                self.assertEqual(self.store.get('closed_copy_state:' + str(first['id']))['state'], 'gone')

    def test_registration_error_is_contained_before_delete_and_later_stage_runs(self):
        first, second, failed, later = self.failed_stage(True)
        record = json.loads((failed / 'record.json').read_text())
        record['detached'] = False
        workspaces._save_deletion(failed, record)
        original = workspaces._detach_staged

        def refuse(stage, record):
            if stage == failed:
                raise ValueError('fixture registration changed')
            return original(stage, record)

        with patch.object(workspaces, '_detach_staged', side_effect=refuse):
            self.drain()
        self.assertFalse(later.exists())
        self.assertIn('fixture registration changed', self.store.get(
            'closed_copy_state:' + str(first['id']))['reason'])

    def test_git_data_and_final_stage_errors_are_contained_and_recoverable(self):
        for phase in ('git', 'stage'):
            with self.subTest(phase=phase):
                first, second, failed, later = self.failed_stage(True)
                refused = failed / 'git' if phase == 'git' else failed

                async def refuse(path, *, progress=None):
                    if path == refused:
                        if phase == 'stage':
                            (path / 'record.json').unlink()
                        raise PermissionError('fixture ' + phase + ' refusal')
                    return await host_os.delete_tree(path, progress=progress)

                with patch.object(workspaces, 'delete_tree', side_effect=refuse):
                    self.drain()
                self.assertFalse(later.exists())
                record = json.loads((failed / 'record.json').read_text())
                self.assertGreater(record['removed']['files'], 0)
                self.assertIn('fixture ' + phase + ' refusal', self.store.get(
                    'closed_copy_state:' + str(first['id']))['reason'])
                self.drain(sweep=True)
                self.assertFalse(failed.exists())

    def test_bad_stage_records_are_skipped_without_blocking_valid_stages(self):
        self.enable()
        task = self.job()
        self.close_job(task)
        valid, record = self.stages()[0]
        for name, contents in (('a-invalid-json', '{'), ('b-incomplete', '{}'),
                               ('c-unsafe', json.dumps(dict(record, target='/outside')))):
            bad = valid.parent / name
            bad.mkdir()
            (bad / 'record.json').write_text(contents)
        self.drain()
        self.assertFalse(valid.exists())
        self.assertTrue((valid.parent / 'a-invalid-json' / 'record.json').exists())
        self.assertEqual(self.store.get('closed_copy_state:' + str(task['id']))['state'], 'gone')

    def test_deep_stage_record_is_skipped_and_cleanup_service_keeps_running(self):
        self.enable()
        task = self.job()
        self.close_job(task)
        valid, record = self.stages()[0]
        bad = valid.parent / 'a-deep-record'
        bad.mkdir()
        (bad / 'record.json').write_text('[' * 200000 + '0' + ']' * 200000)
        service = self.service()
        service.task_restart_limit = 0

        async def run():
            loop = asyncio.create_task(service.supervise('worktree_cleanup', service.worktree_cleanup_loop))
            try:
                deadline = time.monotonic() + 5
                while not loop.done():
                    if not valid.exists() and self.store.get('closed_copy_state:' + str(task['id']))['state'] == 'gone':
                        break
                    self.assertLess(time.monotonic(), deadline)
                    await asyncio.sleep(0.001)
                self.assertFalse(loop.done(), 'A malformed stage must not crash the cleanup service')
                self.assertFalse(valid.exists())
            finally:
                loop.cancel()
                await asyncio.gather(loop, return_exceptions=True)

        asyncio.run(run())
        self.assertTrue((bad / 'record.json').exists())
        self.assertEqual(self.store.get('closed_copy_state:' + str(task['id']))['state'], 'gone')
        self.assertEqual(self.store.db.execute(
            "SELECT COUNT(*) FROM problems WHERE code='loop-crashed'").fetchone()[0], 0)

    def test_surrogate_failure_detail_is_saved_and_later_stage_is_deleted(self):
        first, second, failed, later = self.failed_stage(True)
        reason = 'fixture invalid filename ' + chr(0xdcff)

        async def refuse(path, *, progress=None):
            if path == failed / 'copy':
                raise PermissionError(reason)
            return await host_os.delete_tree(path, progress=progress)

        with patch.object(workspaces, 'delete_tree', side_effect=refuse):
            self.drain()
        self.assertFalse(later.exists())
        problem = self.store.db.execute(
            "SELECT detail FROM problems WHERE code='closed-copy-delete-failed'").fetchone()
        self.assertIsNotNone(problem)
        self.assertIn('fixture invalid filename', problem['detail'])
        problem['detail'].encode('utf-8')

    def test_problem_handler_failure_does_not_block_later_stage(self):
        first, second, failed, later = self.failed_stage(True)

        async def refuse(path, *, progress=None):
            if path == failed / 'copy':
                raise PermissionError('fixture deletion refused')
            return await host_os.delete_tree(path, progress=progress)

        with patch.object(workspaces, 'delete_tree', side_effect=refuse), patch.object(
                problems, 'record', side_effect=RuntimeError('fixture problem handler failed')):
            self.drain()
        self.assertFalse(later.exists())
        self.assertIn('fixture deletion refused', self.store.get(
            'closed_copy_state:' + str(first['id']))['reason'])

    def test_invalid_record_problem_handler_failure_does_not_block_valid_stage(self):
        self.enable()
        task = self.job()
        self.close_job(task)
        valid, record = self.stages()[0]
        bad = valid.parent / 'a-bad-record'
        bad.mkdir()
        (bad / 'record.json').write_text('{')
        with patch.object(problems, 'record', side_effect=RuntimeError('fixture problem handler failed')):
            self.drain()
        self.assertFalse(valid.exists())

    def test_progress_save_handler_failure_does_not_block_later_stage(self):
        first, second, failed, later = self.failed_stage(True)
        original = workspaces._save_deletion

        async def refuse(path, *, progress=None):
            if path == failed / 'copy':
                raise PermissionError('fixture deletion refused')
            return await host_os.delete_tree(path, progress=progress)

        def save(stage, record):
            if stage == failed:
                raise RecursionError('fixture progress handler failed')
            return original(stage, record)

        with patch.object(workspaces, 'delete_tree', side_effect=refuse), patch.object(
                workspaces, '_save_deletion', side_effect=save):
            self.drain()
        self.assertFalse(later.exists())

    def test_child_error_is_saved_and_reported_with_one_errno_prefix(self):
        self.enable()
        task = self.job()
        ignored = self.target(task) / '.env'
        ignored.mkdir()
        (ignored / 'blocked-file').write_text('ignored bytes')
        self.close_job(task)

        def command(path):
            return [sys.executable, '-c',
                    'from tests.test_closed_copy_recovery import _refused_delete_process; _refused_delete_process()',
                    str(path)]

        with patch.object(host_os, '_delete_tree_command', side_effect=command):
            self.drain()
        state = self.store.get('closed_copy_state:' + str(task['id']))
        stage = Path(state['stage'])
        record = json.loads((stage / 'record.json').read_text())
        problem = self.store.db.execute(
            "SELECT detail FROM problems WHERE code='closed-copy-delete-failed'").fetchone()
        answer = self.close_job(task)
        for text in (state['reason'], record['error'], problem['detail'], answer.text):
            with self.subTest(text=text):
                self.assertEqual(text.count('[Errno 1]'), 1)
                self.assertIn('fixture deletion refused', text)

    def test_each_stuck_stage_has_durable_state_even_when_problem_row_is_throttled(self):
        first, second, failed, later = self.failed_stage(True)

        async def refuse(path, *, progress=None):
            raise PermissionError('fixture stuck ' + path.parent.name)

        with patch.object(workspaces, 'delete_tree', side_effect=refuse):
            self.drain()
        self.assertEqual(self.store.db.execute(
            "SELECT COUNT(*) FROM problems WHERE code='closed-copy-delete-failed'").fetchone()[0], 1)
        self.store.close()
        self.store = fixtures.Store(self.state, token_file=self.root / 'no-token')
        for task, stage in ((first, failed), (second, later)):
            state = self.store.get('closed_copy_state:' + str(task['id']))
            self.assertEqual(state, {'state': 'staged', 'reason': 'fixture stuck ' + stage.name,
                                     'stage': str(stage)})
            self.assertEqual(json.loads((stage / 'record.json').read_text())['error'], state['reason'])

    def test_service_loop_survives_delete_errors_with_switch_on_and_off(self):
        for enabled in (True, False):
            with self.subTest(enabled=enabled):
                first, second, failed, later = self.failed_stage(enabled)
                service = self.service()
                service.task_restart_limit = 0
                service.task_restart_backoff = 0.01

                async def refuse(path, *, progress=None):
                    if path == failed / 'copy':
                        raise PermissionError('fixture service refusal')
                    return await host_os.delete_tree(path, progress=progress)

                async def run():
                    loop = asyncio.create_task(service.supervise('worktree_cleanup', service.worktree_cleanup_loop))
                    try:
                        deadline = time.monotonic() + 5
                        while later.exists() and not loop.done():
                            self.assertLess(time.monotonic(), deadline)
                            await asyncio.sleep(0.001)
                        await asyncio.sleep(0)
                        self.assertFalse(loop.done(), 'A delete error must not terminate the cleanup service')
                        self.assertFalse(later.exists())
                    finally:
                        loop.cancel()
                        await asyncio.gather(loop, return_exceptions=True)

                with patch.object(workspaces, 'delete_tree', side_effect=refuse):
                    asyncio.run(run())
                self.assertEqual(self.store.db.execute(
                    "SELECT COUNT(*) FROM problems WHERE area='service' AND code='loop-crashed'").fetchone()[0], 0)
                self.drain(sweep=True)

    def test_git_status_warning_keeps_an_unreadable_untracked_directory(self):
        self.enable()
        task = self.job()
        target = self.target(task)
        locked = target / 'scratch'
        locked.mkdir()
        (locked / 'unsaved').write_text('not in git')
        locked.chmod(0)
        status = self.git('status', '--porcelain', '--untracked-files=all', cwd=target)
        self.assertEqual(status.stdout, '')
        self.assertIn('warning:', status.stderr)
        answer = self.close_job(task)
        self.assertIn('kept because', answer.text)
        self.assertIn('warning', answer.text)
        self.assertTrue(target.exists())
        self.assertIn(str(target), self.registrations())

    def test_service_start_skips_known_failed_stages_until_the_hourly_sweep(self):
        for enabled in (True, False):
            with self.subTest(enabled=enabled):
                first, second, failed, later = self.failed_stage(enabled)
                record = json.loads((failed / 'record.json').read_text())
                record['error'] = 'previous deletion failed'
                workspaces._save_deletion(failed, record)
                attempts = []

                async def refuse(path, *, progress=None):
                    if path == failed / 'copy':
                        attempts.append(path)
                        raise PermissionError('must wait for hourly retry')
                    return await host_os.delete_tree(path, progress=progress)

                async def run():
                    service = self.service()
                    loop = asyncio.create_task(service.worktree_cleanup_loop())
                    try:
                        deadline = time.monotonic() + 5
                        while later.exists() and not loop.done():
                            self.assertLess(time.monotonic(), deadline)
                            await asyncio.sleep(0.001)
                        self.assertEqual(attempts, [])
                        self.assertFalse(later.exists())
                        self.assertFalse(loop.done())
                    finally:
                        loop.cancel()
                        await asyncio.gather(loop, return_exceptions=True)

                with patch.object(workspaces, 'delete_tree', side_effect=refuse):
                    asyncio.run(run())
                self.drain(sweep=True)
                self.assertFalse(failed.exists())

    def finished_worker(self, task):
        accounts = self.root / 'primary'
        transcript = accounts / 'projects' / 'fixture' / '00000000-0000-4000-8000-000000000001.jsonl'
        transcript.parent.mkdir(parents=True)
        transcript.write_text('{}\n')
        with self.store.db:
            self.store.put('accounts', {'primary': {'config_dir': str(accounts), 'enabled': True}})
            self.store.put('account_status', {'primary': {
                'identity': {'email': 'primary@example.com', 'logged_in': True}, 'observed_at': time.time(),
                'usage': {'seven_day': {'utilization': 10, 'resets_at': time.time() + 3600}}}})
            worker = self.store.db.execute('''INSERT INTO workers
                (task,topic,provider,prompt,cwd,workspace,status,session,fresh,account_alias,created,updated)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',
                (task['id'], fixtures.TOPIC, 'claude', 'build this', task['worktree'],
                 json.dumps({'cwd': task['worktree']}), 'done', transcript.stem, 0, 'primary',
                 time.time(), time.time())).lastrowid
        return worker, transcript.stem

    def resume(self, task, worker, calls):
        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                calls.append((str(cwd), session, options['fresh']))
                return RunResult(session, text='done', success=True)

        async def run():
            service = self.service()
            service.workers = WorkerPool(self.store, Runner(), AccountBroker(self.store))
            service.worker_tasks = {}
            self.call('workers.steer', worker=worker, prompt='continue')
            await service.controls_once()
            current = service.workers.get(worker)
            self.assertEqual(current['status'], 'queued')
            await service.workers.run(current, self.store.topic(fixtures.TOPIC))
            return json.loads(service.workers.get(worker)['result'])
        return asyncio.run(run())

    def test_finished_worker_steer_restores_same_path_branch_head_and_session(self):
        self.enable()
        task = self.job()
        tip = self.commit(self.target(task))
        worker, session = self.finished_worker(task)
        self.close_job(task)
        self.drain()
        self.call('tasks.update', task=task['id'], status='open')
        calls = []
        result = self.resume(task, worker, calls)
        self.assertTrue(result['success'], result.get('error'))
        self.assertEqual(calls, [(task['worktree'], session, False)])
        self.assertEqual(self.git('rev-parse', 'HEAD', cwd=self.target(task)).stdout.strip(), tip)
        self.assertEqual(self.git('branch', '--show-current', cwd=self.target(task)).stdout.strip(),
                         'torii/task-' + str(task['id']))

    def test_failed_worker_restore_names_worktree_create(self):
        self.enable()
        task = self.job()
        worker, session = self.finished_worker(task)
        self.close_job(task)
        self.drain()
        self.call('tasks.update', task=task['id'], status='open')
        with self.store.db:
            self.store.db.execute('UPDATE topics SET cwd=? WHERE id=?', (str(self.root / 'missing'), fixtures.TOPIC))
        calls = []
        result = self.resume(task, worker, calls)
        self.assertFalse(result['success'])
        self.assertIn('worktree.create', result['error'])
        self.assertEqual(calls, [])

    def test_switch_off_pending_does_not_repeat_the_legacy_hourly_sweep(self):
        task = self.job(closed=True)
        service = self.service()
        original = workspaces.cleanup_closed_worktrees
        passes = []
        with self.store.db:
            self.store.put('closed_copy_delete', False)
            self.store.put('closed_copy_cleanup_pending', {'1': False})

        async def counted(*args, **kwargs):
            passes.append(time.monotonic())
            return await original(*args, **kwargs)

        async def run():
            loops = [asyncio.create_task(service.controls()), asyncio.create_task(service.worktree_cleanup_loop())]
            try:
                await asyncio.sleep(1.1)
            finally:
                for loop in loops:
                    loop.cancel()
                await asyncio.gather(*loops, return_exceptions=True)

        with patch.object(workspaces, 'cleanup_closed_worktrees', side_effect=counted):
            asyncio.run(run())
        self.assertEqual(len(passes), 1)

    def test_service_git_recheck_never_rolls_back_another_coroutines_writes(self):
        task = self.job(closed=True)
        self.enable()
        target = self.target(task)
        original = workspaces._cleanup_git_directory
        transactions = []

        async def other():
            problems.record(self.store, 'test', 'cleanup-canary', 'saved during git')
            with self.store.transaction():
                self.store.put('cleanup_canary', 'saved')
            (target / 'late').write_text('untracked')

        async def checked(repository, path):
            transactions.append(self.store.db.in_transaction)
            if len(transactions) == 2:
                await asyncio.create_task(other())
            return await original(repository, path)

        with patch.object(workspaces, '_cleanup_git_directory', side_effect=checked):
            state = asyncio.run(workspaces.stage_closed_copy(self.store, task['id']))
        self.assertEqual(state['state'], 'kept')
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM problems WHERE code='cleanup-canary'").fetchone()[0], 1)
        self.assertEqual(self.store.get('cleanup_canary'), 'saved')
        self.assertEqual(transactions, [False, False])

    def hidden_edit(self, flag):
        self.enable()
        task = self.job()
        target = self.target(task)
        self.git('update-index', '--' + flag, 'README.md', cwd=target)
        (target / 'README.md').write_text('hidden unsaved edit')
        self.assertEqual(self.git('status', '--porcelain', cwd=target).stdout, '')
        answer = self.close_job(task)
        self.assertIn('kept because', answer.text)
        self.assertIn(flag, answer.text)
        self.assertEqual((target / 'README.md').read_text(), 'hidden unsaved edit')

    def test_skip_worktree_hidden_edit_is_kept(self):
        self.hidden_edit('skip-worktree')

    def test_assume_unchanged_hidden_edit_is_kept(self):
        self.hidden_edit('assume-unchanged')

    def test_worker_end_resolves_state_directory_once_for_all_closed_jobs(self):
        self.enable()
        now = time.time()
        with self.store.db:
            self.store.db.executemany('''INSERT INTO tasks
                (id,topic,number,title,status,worktree,created,updated) VALUES (?,?,?,?,?,?,?,?)''',
                [(i, fixtures.TOPIC, i, 'closed', 'done', str(self.target({'id': i})), now, now)
                 for i in range(1, 25)])
        worker = self.worker(1, self.target({'id': 2}) / 'nested')
        original = Path.resolve
        resolutions = []

        def resolve(path, *args, **kwargs):
            if path == self.state:
                resolutions.append(path)
            return original(path, *args, **kwargs)

        with patch.object(Path, 'resolve', resolve):
            self.store.worker_complete(worker, {'success': True})
        self.assertLessEqual(len(resolutions), 1)
        self.assertEqual(set(self.store.get('closed_copy_cleanup_pending')), {'1', '2'})

    def test_slow_filesystem_delete_does_not_block_the_service_loop(self):
        self.enable()
        task = self.job()
        ignored = self.target(task) / '.env'
        ignored.mkdir()
        (ignored / 'slow-file').write_text('ignored bytes')
        self.close_job(task)
        async def run():
            deletion = asyncio.create_task(workspaces.cleanup_closed_copies(self.store))
            try:
                await gate.entered(self, deletion)
                for turn in range(4):
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

        with copies._DeletionGate(self.root / 'delete-gate') as gate:
            asyncio.run(run())
            gate.assert_waiting(self)
        self.drain()
        self.assertEqual(self.stages(), [])
