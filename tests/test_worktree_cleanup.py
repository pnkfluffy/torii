import asyncio
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import control_api, host_os, workspaces
from coordinator.service import Service
from coordinator.store import Store
from coordinator.workspaces import prepare_task_workspace
from tests.support import until_unlocked


TOPIC = '1:2'


class WorktreeCleanupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='w', dir=Path.home())
        self.root = Path(self.temp.name).resolve()
        self.project = self.root / 'project'
        self.project.mkdir()
        self.git('init', '-q')
        self.git('config', 'user.name', 'Test')
        self.git('config', 'user.email', 'test@example.com')
        (self.project / 'README.md').write_text('base\n')
        (self.project / '.gitignore').write_text('.env\n')
        (self.project / 'nested').mkdir()
        (self.project / 'nested' / 'keep.txt').write_text('nested\n')
        self.git('add', '.')
        self.git('commit', '-qm', 'base')
        self.base = self.git('rev-parse', 'HEAD').stdout.strip()
        self.store = Store(self.root / 'state', token_file=self.root / 'no-token')
        self.state = self.store.directory
        self.trash = self.root / 'Trash'
        self.trash_patch = patch('coordinator.workspaces.trash_directory', return_value=self.trash)
        self.trash_patch.start()
        self.addCleanup(self.trash_patch.stop)
        self.now = time.time()
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES (?,?,?,?,?)",
                                  (TOPIC, 1, 2, 'project', str(self.project)))

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def git(self, *args, cwd=None, check=True):
        return subprocess.run(('git', '-C', str(cwd or self.project), *map(str, args)),
                              capture_output=True, text=True, check=check, timeout=30)

    def call(self, op, **params):
        with self.store.db:
            result = control_api.call(self.store, op, params, topic=TOPIC)
        self.assertTrue(result.ok, result.text)
        return result

    def job(self, closed=True, nested=False, status='done', age=86401):
        if nested:
            with self.store.db:
                self.store.db.execute('UPDATE topics SET cwd=? WHERE id=?',
                                      (str(self.project / 'nested'), TOPIC))
        task = self.call('tasks.create', title='Keep job code').data
        self.call('worktree.create', task=task['id'])
        if closed:
            self.call('tasks.update', task=task['id'], status=status, **({'summary': 'Closed in test.'} if status in ('done', 'dropped') else {}), **({'note': 'No longer needed'} if status == 'dropped' else {}))
            with self.store.db:
                self.store.db.execute('UPDATE tasks SET updated=? WHERE id=?', (self.now - age, task['id']))
        return self.store.task_get(task['id'])

    def target(self, task):
        return self.state / 'worktrees' / ('task-' + str(task['id']))

    def commit(self, target, content='job commit\n'):
        (target / 'README.md').write_text(content)
        self.git('add', 'README.md', cwd=target)
        self.git('commit', '-qm', 'job commit', cwd=target)
        return self.git('rev-parse', 'HEAD', cwd=target).stdout.strip()

    def registrations(self):
        return self.git('worktree', 'list', '--porcelain').stdout

    def test_timed_out_deletion_stage_scan_preserves_copy(self):
        task = self.job()
        target = self.target(task)
        original = workspaces.scan_process.run
        def never(function, *args, **kwargs):
            if function is workspaces._scan_deletion_stages:
                return original(time.sleep, 60, timeout=0.2)
            return original(function, *args, **kwargs)
        with patch.object(workspaces.scan_process, 'run', side_effect=never):
            asyncio.run(workspaces.cleanup_closed_copies(self.store, sweep=True))
        self.assertTrue(target.exists())
        self.assertIsNotNone(self.store.db.execute(
            "SELECT 1 FROM problems WHERE code='closed-copy-scan-failed'").fetchone())

    def test_scan_failures_remain_closed_when_problem_reporting_fails(self):
        stage = self.state / 'worktree-deletions' / 'stage'
        operations = (
            lambda: workspaces._save_deletion_bounded(self.store, stage, {}),
            lambda: workspaces.async_prepare_deletion_stage(self.store, stage, {}),
            lambda: workspaces.async_detach_staged(self.store, stage, {}),
            lambda: workspaces.async_deletion_stages(self.store),
        )
        with patch.object(workspaces.scan_process, 'run_async', side_effect=workspaces.scan_process.ScanError('timeout')):
            with patch('coordinator.problems.record', side_effect=RuntimeError('report failed')):
                with patch.object(workspaces.logger, 'error', side_effect=RuntimeError('log failed')):
                    for operation in operations:
                        with self.subTest(operation=operation):
                            with self.assertRaises(workspaces.scan_process.ScanError):
                                asyncio.run(operation())
                    self.assertFalse(asyncio.run(workspaces._recover_finalization_stages(self.store)))
            with patch.object(workspaces.logger, 'warning', side_effect=RuntimeError('log failed')):
                with patch.object(workspaces.logger, 'error', side_effect=RuntimeError('log failed')):
                    with self.assertRaises(workspaces.scan_process.ScanError):
                        asyncio.run(workspaces._async_deletion_stages(self.state))
        records = [(stage, {'repository': 'repository', 'restoring': True})]
        with patch.object(workspaces, '_async_deletion_stages', new=AsyncMock(return_value=records)):
            with patch('coordinator.problems.record', side_effect=RuntimeError('report failed')):
                with patch.object(workspaces.logger, 'error', side_effect=RuntimeError('log failed')):
                    with self.assertRaises(workspaces.scan_process.ScanError):
                        asyncio.run(workspaces.async_recover_staged_registrations(self.store, 'repository'))

    def test_invalid_deletion_stage_blocks_cleanup(self):
        task = self.job()
        stage = self.state / 'worktree-deletions' / 'invalid'
        stage.mkdir(parents=True)
        (stage / 'record.json').write_text('{')
        missing = stage.parent / 'missing'
        missing.mkdir()
        directory = stage.parent / 'directory'
        (directory / 'record.json').mkdir(parents=True)
        symlink = stage.parent / 'symlink'
        symlink.mkdir()
        (symlink / 'record.json').symlink_to(stage / 'record.json')
        asyncio.run(workspaces.cleanup_closed_copies(self.store, sweep=True))
        self.assertTrue(self.target(task).exists())
        self.assertTrue((stage / 'record.json').exists())
        self.assertIsNotNone(self.store.db.execute(
            "SELECT 1 FROM problems WHERE code='closed-copy-record-invalid'").fetchone())
        self.assertEqual(self.store.db.execute(
            "SELECT count(*) FROM problems WHERE code='closed-copy-record-invalid'").fetchone()[0], 1)

    def test_finalization_preserves_unexpected_content(self):
        stage = self.state / 'worktree-deletion-finalizing' / 'unknown'
        stage.mkdir(parents=True)
        (stage / 'copy').mkdir()
        (stage / 'copy' / 'data').write_text('keep')
        asyncio.run(workspaces.cleanup_closed_copies(self.store))
        self.assertEqual((stage / 'copy' / 'data').read_text(), 'keep')
        self.assertIsNotNone(self.store.db.execute(
            "SELECT 1 FROM problems WHERE code='closed-copy-finalization-failed'").fetchone())

    def test_completed_stage_accounting_is_idempotent(self):
        task = self.job()
        stage = self.state / 'worktree-deletions' / 'accounted'
        record = {'task': task['id'], 'space_k': task['iteration'], 'removed': {'bytes': 42, 'files': 1}}
        self.assertTrue(asyncio.run(workspaces._account_completed_stage(self.store, stage, record)))
        self.assertTrue(asyncio.run(workspaces._account_completed_stage(self.store, stage, record)))
        row = self.store.db.execute('SELECT bytes_freed FROM task_iterations WHERE task=? AND k=?',
                                    (task['id'], task['iteration'])).fetchone()
        self.assertEqual(row['bytes_freed'], 42)

    def worker(self, task, cwd, status='running', provider='codex'):
        with self.store.db:
            cursor = self.store.db.execute('''INSERT INTO workers
                (task,topic,provider,prompt,cwd,status,created,updated) VALUES (?,?,?,?,?,?,?,?)''',
                (task, TOPIC, provider, 'fixture work', str(cwd), status, self.now, self.now))
        return cursor.lastrowid

    def missing_unrelated_worktree(self):
        path = self.root / 'disconnected-volume-copy'
        self.git('worktree', 'add', '-q', '-b', 'unrelated', path)
        path.rename(self.root / 'disconnected-volume-data')
        self.assertIn('worktree ' + str(path) + '\n', self.registrations())
        return path

    def files(self, root):
        return {str(path.relative_to(root)): path.read_bytes()
                for path in root.rglob('*') if path.is_file()}

    def git_directory(self, target):
        return Path(self.git('rev-parse', '--absolute-git-dir', cwd=target).stdout.strip())

    def submodule_job(self, initialized=True, closed=True):
        source = self.root / 'submodule-source'
        source.mkdir()
        self.git('init', '-q', cwd=source)
        self.git('config', 'user.name', 'Test', cwd=source)
        self.git('config', 'user.email', 'test@example.com', cwd=source)
        (source / 'library.txt').write_text('source commit\n')
        self.git('add', '.', cwd=source)
        self.git('commit', '-qm', 'source commit', cwd=source)
        self.git('-c', 'protocol.file.allow=always', 'submodule', 'add', '-q', source, 'sub')
        self.git('commit', '-qm', 'add submodule')
        task = self.job(closed=closed)
        target = self.target(task)
        if not initialized:
            return task, None
        self.git('-c', 'protocol.file.allow=always', 'submodule', 'update', '--init', '-q', cwd=target)
        sub = target / 'sub'
        self.git('config', 'user.name', 'Test', cwd=sub)
        self.git('config', 'user.email', 'test@example.com', cwd=sub)
        (sub / 'library.txt').write_text('unpublished submodule commit\n')
        self.git('commit', '-qam', 'unpublished submodule commit', cwd=sub)
        tip = self.git('rev-parse', 'HEAD', cwd=sub).stdout.strip()
        self.assertNotEqual(self.git('cat-file', '-e', tip, cwd=source, check=False).returncode, 0)
        self.assertNotEqual(self.git('cat-file', '-e', tip, cwd=self.project / 'sub',
                                     check=False).returncode, 0)
        self.git('add', 'sub', cwd=target)
        self.git('commit', '-qm', 'record unpublished submodule commit', cwd=target)
        return task, tip

    def slow_checkout_hook(self, marker):
        hook = self.project / '.git' / 'hooks' / 'post-checkout'
        hook.write_text('#!' + sys.executable + '\n'
                        'import time\n'
                        'from pathlib import Path\n'
                        'time.sleep(0.6)\n'
                        'Path(' + repr(str(marker)) + ').write_text("finished")\n')
        hook.chmod(0o700)

    def test_salvage_keeps_index_edits_and_detects_later_source_edits(self):
        from coordinator.worktree_salvage import preserve_worktree, verify_preserved_worktree
        task = self.job()
        target = self.target(task)
        (target / 'README.md').write_text('staged change\n')
        self.git('add', 'README.md', cwd=target)
        (target / 'README.md').write_text('base\n')
        manifest = asyncio.run(preserve_worktree(target, self.root / 'saved-source'))
        self.assertIn(b'staged change', (manifest.parent / '0' / 'staged.patch').read_bytes())
        self.assertIn(b'staged change', (manifest.parent / '0' / 'unstaged.patch').read_bytes())
        self.assertTrue(asyncio.run(verify_preserved_worktree(target, manifest)))
        (target / 'README.md').write_text('different staged change\n')
        self.git('add', 'README.md', cwd=target)
        (target / 'README.md').write_text('base\n')
        with self.assertRaisesRegex(ValueError, 'changed after preserving'):
            asyncio.run(verify_preserved_worktree(target, manifest))
        (target / 'README.md').write_text('late change\n')
        with self.assertRaisesRegex(ValueError, 'changed after preserving'):
            asyncio.run(verify_preserved_worktree(target, manifest))

    def test_failed_salvage_leaves_no_partial_destination(self):
        from coordinator.worktree_salvage import preserve_worktree
        task, _ = self.submodule_job()
        saved = self.root / 'saved-source'
        with patch('coordinator.worktree_salvage._snapshot', AsyncMock(side_effect=ValueError('failed snapshot'))):
            with self.assertRaisesRegex(ValueError, 'failed snapshot'):
                asyncio.run(preserve_worktree(self.target(task), saved))
        self.assertEqual(list(saved.glob('salvage-*')), [])

    def test_f2_reopen_preserves_old_admin_and_submodule_repository_in_trash(self):
        task, sub_tip = self.submodule_job(closed=False)
        target = self.target(task)
        tip = self.git('rev-parse', 'HEAD', cwd=target).stdout.strip()
        git_dir = self.git_directory(target)
        before = self.files(git_dir)
        unrelated = self.missing_unrelated_worktree()
        other_before = self.files(git_dir.parent / unrelated.name)
        self.trash.mkdir()
        collision = self.trash / (target.name + '.git-worktree')
        collision.mkdir()
        (collision / 'keep').write_text('existing admin Trash data')
        (self.trash / (target.name + '.git-worktree-1')).symlink_to(self.root / 'missing-admin-copy')
        target.rename(self.root / 'hand-moved-job')
        result = asyncio.run(prepare_task_workspace(
            self.project, self.state, task['id'], saved_cwd=task['worktree']))
        preserved = self.trash / (target.name + '.git-worktree-2')
        self.assertEqual(result['base_commit'], tip)
        self.assertEqual(self.git('rev-parse', 'HEAD', cwd=target).stdout.strip(), tip)
        self.assertEqual(self.git('symbolic-ref', '--short', 'HEAD', cwd=target).stdout.strip(),
                         'torii/task-' + str(task['id']))
        self.assertEqual(self.files(preserved), before)
        commit = self.git('--git-dir', preserved / 'modules' / 'sub', '--work-tree', self.root,
                          'cat-file', '-e', sub_tip + '^{commit}', check=False)
        self.assertEqual(commit.returncode, 0, commit.stderr)
        self.assertEqual(self.files(git_dir.parent / unrelated.name), other_before)
        self.assertIn('worktree ' + str(unrelated) + '\n', self.registrations())
        self.assertEqual((collision / 'keep').read_text(), 'existing admin Trash data')
        self.assertTrue((self.trash / (target.name + '.git-worktree-1')).is_symlink())

    def test_f4_new_job_checkout_can_exceed_sweep_git_limit(self):
        task = self.call('tasks.create', title='slow new checkout').data
        marker = self.root / 'new-hook-finished'
        self.slow_checkout_hook(marker)
        with patch('coordinator.workspaces.GIT_TIMEOUT', 0.25):
            result = asyncio.run(prepare_task_workspace(self.project, self.state, task['id']))
        self.assertTrue(self.target(task).is_dir())
        self.assertEqual(result['base_commit'], self.base)
        self.assertEqual(marker.read_text(), 'finished')

    def test_f4_reopen_checkout_can_exceed_sweep_git_limit(self):
        task = self.job(closed=False)
        target = self.target(task)
        tip = self.commit(target)
        target.rename(self.root / 'hand-moved-slow-job')
        marker = self.root / 'reopen-hook-finished'
        self.slow_checkout_hook(marker)
        with patch('coordinator.workspaces.GIT_TIMEOUT', 0.25):
            result = asyncio.run(prepare_task_workspace(
                self.project, self.state, task['id'], saved_cwd=task['worktree']))
        self.assertEqual(result['base_commit'], tip)
        self.assertEqual(self.git('rev-parse', 'HEAD', cwd=target).stdout.strip(), tip)
        self.assertEqual(marker.read_text(), 'finished')

    def test_f5_new_job_refuses_leftover_branch_without_changing_it(self):
        task = self.call('tasks.create', title='new job with leftover branch').data
        branch = 'torii/task-' + str(task['id'])
        self.git('branch', branch)
        self.commit(self.project, 'new main commit\n')
        before = self.registrations()
        with self.assertRaisesRegex(RuntimeError, 'already exists'):
            asyncio.run(prepare_task_workspace(self.project, self.state, task['id']))
        self.assertEqual(self.git('rev-parse', branch).stdout.strip(), self.base)
        self.assertEqual(self.registrations(), before)
        self.assertFalse(self.target(task).exists())

    def test_reopen_refuses_missing_or_ambiguous_admin_mapping_without_moving_data(self):
        for ambiguous in (False, True):
            with self.subTest(ambiguous=ambiguous):
                task = self.job(closed=False)
                target = self.target(task)
                git_dir = self.git_directory(target)
                target.rename(self.root / ('hand-moved-' + target.name))
                if ambiguous:
                    duplicate = git_dir.parent / ('duplicate-' + target.name)
                    duplicate.mkdir()
                    (duplicate / 'gitdir').write_bytes((git_dir / 'gitdir').read_bytes())
                    (duplicate / 'HEAD').write_bytes((git_dir / 'HEAD').read_bytes())
                    (duplicate / 'commondir').write_bytes((git_dir / 'commondir').read_bytes())
                real_git = workspaces._git
                snapshots = []

                async def lose_mapping_after_listing(cwd, *args, **kwargs):
                    result = await real_git(cwd, *args, **kwargs)
                    if args[:3] == ('worktree', 'list', '--porcelain'):
                        if not ambiguous:
                            (git_dir / 'gitdir').rename(self.root / ('saved-pointer-' + target.name))
                        snapshots.append(self.files(git_dir.parent))
                    return result

                with patch('coordinator.workspaces._git', side_effect=lose_mapping_after_listing), \
                        patch('coordinator.workspaces.move_to_trash') as move, \
                        self.assertRaisesRegex(RuntimeError, 'exactly one Git data folder'):
                    asyncio.run(prepare_task_workspace(
                        self.project, self.state, task['id'], saved_cwd=task['worktree']))
                move.assert_not_called()
                self.assertEqual(self.files(git_dir.parent), snapshots[0])
                self.assertFalse(target.exists())
                self.assertFalse(self.trash.exists())

    def test_reopen_admin_move_failure_preserves_registration_and_data(self):
        task = self.job(closed=False)
        target = self.target(task)
        git_dir = self.git_directory(target)
        target.rename(self.root / 'hand-moved-job')
        before = self.files(git_dir)
        registrations = self.registrations()
        with patch('coordinator.workspaces.move_to_trash', side_effect=OSError('admin rename failure')), \
                self.assertRaisesRegex(RuntimeError, 'Cannot preserve the saved worktree Git data'):
            asyncio.run(prepare_task_workspace(
                self.project, self.state, task['id'], saved_cwd=task['worktree']))
        self.assertEqual(self.files(git_dir), before)
        self.assertEqual(self.registrations(), registrations)
        self.assertFalse(target.exists())
        self.assertFalse(self.trash.exists())

    def test_reopen_cross_filesystem_admin_is_kept(self):
        task = self.job(closed=False)
        target = self.target(task)
        git_dir = self.git_directory(target)
        target.rename(self.root / 'hand-moved-job')
        before = self.files(git_dir)
        registrations = self.registrations()
        real_stat = Path.stat

        def different_device(path, *args, **kwargs):
            result = real_stat(path, *args, **kwargs)
            if path == self.trash:
                fields = list(result)
                fields[2] += 1
                return os.stat_result(fields)
            return result

        with patch.object(Path, 'stat', different_device), \
                self.assertRaisesRegex(RuntimeError, 'another filesystem'):
            asyncio.run(prepare_task_workspace(
                self.project, self.state, task['id'], saved_cwd=task['worktree']))
        self.assertEqual(self.files(git_dir), before)
        self.assertEqual(self.registrations(), registrations)
        self.assertFalse(target.exists())
        self.assertEqual(list(self.trash.iterdir()), [])

    def test_r1_reopen_repairs_only_its_stale_registration_and_keeps_commits(self):
        task = self.job(closed=False)
        target = self.target(task)
        tip = self.commit(target)
        unrelated = self.missing_unrelated_worktree()
        target.rename(self.root / 'manually-trashed-job')
        self.assertIn('worktree ' + str(target) + '\n', self.registrations())
        result = asyncio.run(prepare_task_workspace(
            self.project, self.state, task['id'], saved_cwd=task['worktree']))
        self.assertEqual(result['base_commit'], tip)
        self.assertEqual(self.git('rev-parse', 'HEAD', cwd=target).stdout.strip(), tip)
        self.assertEqual((target / 'README.md').read_text(), 'job commit\n')
        self.assertIn('worktree ' + str(unrelated) + '\n', self.registrations())

    def test_r2_reopen_uses_existing_branch_and_ignores_requested_base(self):
        task = self.job(closed=False)
        target = self.target(task)
        tip = self.commit(target)
        git_dir = self.git_directory(target)
        target.rename(self.root / 'removed-job-copy')
        git_dir.rename(self.root / 'preserved-job-git-data')
        result = asyncio.run(prepare_task_workspace(
            self.project, self.state, task['id'], base='missing-base', saved_cwd=task['worktree']))
        self.assertEqual((result['base_commit'], result['base_ref'], result['fetched']), (tip, 'HEAD', False))
        self.assertEqual(self.git('rev-parse', 'HEAD', cwd=target).stdout.strip(), tip)
        self.assertEqual((target / 'README.md').read_text(), 'job commit\n')

    def test_reopen_repairs_stale_registration_after_branch_change_or_detach(self):
        unrelated = self.missing_unrelated_worktree()
        for detached in (False, True):
            with self.subTest(detached=detached):
                task = self.job(closed=False)
                target = self.target(task)
                tip = self.commit(target)
                feature = 'feature-' + str(task['id'])
                self.git('checkout', '-qb', feature, cwd=target)
                feature_tip = self.commit(target, 'feature commit\n')
                if detached:
                    self.git('checkout', '--detach', cwd=target)
                branches = self.git('show-ref', '--heads').stdout
                moved = self.root / ('preserved-copy-' + str(task['id']))
                target.rename(moved)
                self.assertIn('worktree ' + str(target) + '\n', self.registrations())
                result = asyncio.run(prepare_task_workspace(
                    self.project, self.state, task['id'], saved_cwd=task['worktree']))
                self.assertEqual(result['base_commit'], tip)
                self.assertEqual(self.git('rev-parse', 'HEAD', cwd=target).stdout.strip(), tip)
                self.assertEqual(self.git('symbolic-ref', '--short', 'HEAD', cwd=target).stdout.strip(),
                                 'torii/task-' + str(task['id']))
                self.assertEqual((target / 'README.md').read_text(), 'job commit\n')
                self.assertEqual((moved / 'README.md').read_text(), 'feature commit\n')
                self.assertEqual(self.git('show-ref', '--heads').stdout, branches)
                self.assertEqual(self.git('rev-parse', feature).stdout.strip(), feature_tip)
                self.assertIn('worktree ' + str(unrelated) + '\n', self.registrations())

    def test_reopen_refuses_job_branch_elsewhere_before_removing_same_path_registration(self):
        for missing in (False, True):
            with self.subTest(missing=missing):
                task = self.job(closed=False)
                target = self.target(task)
                self.git('checkout', '-qb', 'feature-' + str(task['id']), cwd=target)
                elsewhere = self.root / ('other-copy-' + str(task['id']))
                self.git('worktree', 'add', '-q', elsewhere, 'torii/task-' + str(task['id']))
                target.rename(self.root / ('preserved-copy-' + str(task['id'])))
                if missing:
                    elsewhere.rename(self.root / ('missing-copy-' + str(task['id'])))
                before = self.registrations()
                branches = self.git('show-ref', '--heads').stdout
                with self.assertRaisesRegex(RuntimeError, 'already used'):
                    asyncio.run(prepare_task_workspace(
                        self.project, self.state, task['id'], saved_cwd=task['worktree']))
                self.assertEqual(self.registrations(), before)
                self.assertEqual(self.git('show-ref', '--heads').stdout, branches)
                self.assertFalse(target.exists())

    def test_r3_branch_at_another_existing_path_is_refused(self):
        task = self.call('tasks.create', title='Already in use').data
        elsewhere = self.root / 'other-job-copy'
        self.git('worktree', 'add', '-q', '-b', 'torii/task-' + str(task['id']), elsewhere)
        before = self.registrations()
        with self.assertRaisesRegex(RuntimeError, 'already used'):
            asyncio.run(prepare_task_workspace(self.project, self.state, task['id']))
        self.assertEqual(self.registrations(), before)
        self.assertTrue(elsewhere.is_dir())
        self.assertFalse(self.target(task).exists())

    def test_branch_at_another_missing_path_is_also_refused(self):
        task = self.call('tasks.create', title='Missing at another path').data
        elsewhere = self.root / 'other-job-copy'
        self.git('worktree', 'add', '-q', '-b', 'torii/task-' + str(task['id']), elsewhere)
        elsewhere.rename(self.root / 'preserved-job-copy')
        before = self.registrations()
        with self.assertRaisesRegex(RuntimeError, 'already used'):
            asyncio.run(prepare_task_workspace(self.project, self.state, task['id']))
        self.assertEqual(self.registrations(), before)
        self.assertFalse(self.target(task).exists())

    def test_externally_trashed_worktree_reopens_on_saved_branch_and_nested_cwd(self):
        task = self.job(nested=True)
        target = self.target(task)
        tip = self.commit(target)
        unrelated = self.missing_unrelated_worktree()
        (target / '.env').write_bytes(b'external cleaner preserved local data\x00\xff')
        self.trash.mkdir()
        moved = self.trash / target.name
        target.rename(moved)
        saved = {path.relative_to(moved): path.read_bytes() for path in moved.rglob('*') if path.is_file()}
        registrations = self.registrations()
        branches = self.git('show-ref', '--heads').stdout
        self.assertIn('worktree ' + str(target) + '\n', registrations)
        self.call('tasks.update', task=task['id'], status='open')
        worker = self.call('workers.spawn', task=task['id'], provider='codex', prompt='Resume external copy').data
        row = self.store.db.execute('SELECT cwd,status FROM workers WHERE id=?', (worker['worker'],)).fetchone()
        self.assertEqual((row['cwd'], row['status']), (str(target / 'nested'), 'queued'))
        self.assertTrue(Path(row['cwd']).is_dir())
        self.assertEqual(self.store.task_get(task['id'])['worktree'], str(target / 'nested'))
        self.assertEqual(self.git('symbolic-ref', '--short', 'HEAD', cwd=target).stdout.strip(),
                         'torii/task-' + str(task['id']))
        self.assertEqual(self.git('rev-parse', 'HEAD', cwd=target).stdout.strip(), tip)
        self.assertEqual((target / 'README.md').read_text(), 'job commit\n')
        self.assertEqual(self.git('show-ref', '--heads').stdout, branches)
        self.assertIn('worktree ' + str(unrelated) + '\n', self.registrations())
        self.assertIn('worktree ' + str(target) + '\n', self.registrations())
        self.assertEqual({path.relative_to(moved): path.read_bytes()
                          for path in moved.rglob('*') if path.is_file()}, saved)

    def test_cli_decodes_only_boolean_parameters(self):
        cli = [sys.executable, '-m', 'coordinator', '--state-dir', str(self.state),
               '--token-file', str(self.root / 'no-token'), 'ctl', 'call']
        for value in ('false', 'off', '0', 'no', 'FALSE'):
            result = subprocess.run(cli + ['delegation.codex', 'enabled=' + value],
                                    capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIs(self.store.get('codex_enabled'), False)
        task = self.call('tasks.create', title='old title').data
        result = subprocess.run(cli + ['tasks.update', 'task=' + str(task['id']), 'title=true'],
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.store.task_get(task['id'])['title'], 'true')

    def test_missing_custom_cwd_is_refused_by_creation_and_spawn(self):
        task = self.job(closed=False)
        self.store.task_update(task['id'], worktree=str(self.root / 'missing-custom-folder'))
        before = self.registrations()
        for op, extra in (('worktree.create', {}), ('workers.spawn', {'provider': 'codex', 'prompt': 'Resume'})):
            result = control_api.call(self.store, op, dict(task=task['id'], **extra))
            self.assertFalse(result.ok)
            if op == 'worktree.create':
                self.assertIn('original task folder', result.text)
            else:
                self.assertEqual(result.text, 'Create the job worktree first.')
        self.assertEqual(self.registrations(), before)

    def test_missing_saved_branch_does_not_create_a_replacement_history(self):
        task = self.job(closed=False)
        target = self.target(task)
        git_dir = self.git_directory(target)
        target.rename(self.root / 'preserved-job-data')
        git_dir.rename(self.root / 'preserved-job-git-data')
        self.git('branch', '-m', 'torii/task-' + str(task['id']), 'preserved-job-branch')
        result = control_api.call(self.store, 'worktree.create', {'task': task['id']})
        self.assertFalse(result.ok)
        self.assertIn('branch is missing', result.text)
        self.assertFalse(target.exists())

class DeleteTreeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()

    def tearDown(self):
        for directory, names, files in os.walk(str(self.root)):
            for name in names:
                path = Path(directory) / name
                if not path.is_symlink():
                    path.chmod(0o700)
        self.temp.cleanup()

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


class ServiceCleanupLoopTests(unittest.TestCase):
    def test_supervised_defers_session_storage_until_cleanup_loop_runs(self):
        with self.assertRaises(RuntimeError):
            asyncio.get_running_loop()
        service = Service.__new__(Service)
        service.store = SimpleNamespace(code_requests=None)
        service.pair_only = False
        service.workers = SimpleNamespace(runner=SimpleNamespace(binaries={}))
        service.updater = SimpleNamespace(run=None, freeze=None)
        service.signins = SimpleNamespace(run=None)
        cleanup = AsyncMock()
        with patch('coordinator.session_storage.storage_for', return_value=SimpleNamespace(run=cleanup)) as storage:
            loops = dict(service.supervised())
            storage.assert_not_called()

            async def run_cleanup():
                await loops['session_cleanup']()

            asyncio.run(run_cleanup())
            storage.assert_called_once_with(service.store)
            cleanup.assert_awaited_once()
