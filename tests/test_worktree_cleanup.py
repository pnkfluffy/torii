import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import control_api, workspaces
from coordinator.service import Service
from coordinator.store import Store
from coordinator.workspaces import cleanup_closed_worktrees, cleanup_task_workspace, prepare_task_workspace
from tests.support import until_unlocked


TOPIC = '1:2'


class WorktreeCleanupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='worktree-cleanup-', dir=Path.home())
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
            self.call('tasks.update', task=task['id'], status=status)
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

    def assert_retained(self, task):
        target = self.target(task)
        before = self.registrations()
        self.assertIsNone(asyncio.run(cleanup_task_workspace(self.store, task['id'], now=self.now)))
        self.assertTrue(target.exists() or target.is_symlink())
        self.assertEqual(self.registrations(), before)
        return target

    def test_pending_image_keeps_old_copy_until_delivered_then_swept(self):
        task = self.job()
        target = self.target(task)
        with self.store.db:
            row_id = self.store.enqueue_report(TOPIC, 'Queued image', image=str(target / 'README.md'))
        asyncio.run(cleanup_closed_worktrees(self.store, now=self.now))
        self.assertEqual((target / 'README.md').read_text(), 'base\n')
        self.assertIn('worktree ' + str(target) + '\n', self.registrations())
        self.store.delivered(row_id, 100)
        asyncio.run(cleanup_closed_worktrees(self.store, now=self.now))
        self.assertFalse(target.exists())
        self.assertEqual((self.trash / target.name / 'README.md').read_text(), 'base\n')
        self.assertEqual(self.git('rev-parse', 'torii/task-' + str(task['id'])).stdout.strip(), self.base)

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

    def test_initialized_submodule_is_salvaged_before_trash(self):
        task, tip = self.submodule_job()
        target = self.target(task)
        (target / 'sub' / 'library.txt').write_bytes(b'changed\x00binary')
        (target / 'sub' / 'untracked.bin').write_bytes(b'untracked\x00data')
        moved = asyncio.run(cleanup_task_workspace(self.store, task['id'], now=self.now))
        self.assertIsNotNone(moved)
        self.assertFalse(target.exists())
        saved = next((self.state / 'results' / target.name / 'worktree-salvage').glob('salvage-*'))
        self.assertEqual((saved / '1' / 'untracked' / 'untracked.bin').read_bytes(), b'untracked\x00data')
        self.assertIn(b'GIT binary patch', (saved / '1' / 'changes.patch').read_bytes())
        self.assertIn(tip, self.git('bundle', 'list-heads', saved / '1' / 'commits.bundle').stdout)

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

    def test_uninitialized_submodule_with_files_is_kept_before_trash(self):
        task, _ = self.submodule_job(initialized=False)
        target = self.target(task)
        (target / 'sub' / 'notes.bin').write_bytes(b'unsaved\x00\xff')
        self.assert_retained(task)
        self.assertEqual((target / 'sub' / 'notes.bin').read_bytes(), b'unsaved\x00\xff')

    def test_recursive_uninitialized_submodule_with_files_is_kept(self):
        task, tip = self.submodule_job()
        target = self.target(task)
        sub = target / 'sub'
        self.git('update-index', '--add', '--cacheinfo', '160000,' + tip + ',nested', cwd=sub)
        (sub / '.gitmodules').write_text('[submodule "nested"]\n\tpath = nested\n\turl = ../source\n')
        self.git('add', '.gitmodules', cwd=sub)
        self.git('commit', '-qm', 'add nested submodule', cwd=sub)
        self.git('add', 'sub', cwd=target)
        self.git('commit', '-qm', 'record nested submodule', cwd=target)
        (sub / 'nested').mkdir()
        (sub / 'nested' / 'notes.txt').write_text('nested source\n')
        self.assert_retained(task)
        self.assertEqual((sub / 'nested' / 'notes.txt').read_text(), 'nested source\n')

    def test_failed_salvage_leaves_no_partial_destination(self):
        from coordinator.worktree_salvage import preserve_worktree
        task, _ = self.submodule_job()
        saved = self.root / 'saved-source'
        with patch('coordinator.worktree_salvage._snapshot', AsyncMock(side_effect=ValueError('failed snapshot'))):
            with self.assertRaisesRegex(ValueError, 'failed snapshot'):
                asyncio.run(preserve_worktree(self.target(task), saved))
        self.assertEqual(list(saved.glob('salvage-*')), [])

    def test_remote_backed_clean_submodule_needs_no_commit_bundle(self):
        task, tip = self.submodule_job()
        target = self.target(task)
        self.git('push', '-q', 'origin', 'HEAD:refs/heads/saved', cwd=target / 'sub')
        moved = asyncio.run(cleanup_task_workspace(self.store, task['id'], now=self.now))
        self.assertIsNotNone(moved)
        saved = next((self.state / 'results' / target.name / 'worktree-salvage').glob('salvage-*'))
        self.assertFalse((saved / '1' / 'commits.bundle').exists())

    def test_failed_submodule_preservation_keeps_original_copy_and_registration(self):
        task, _ = self.submodule_job()
        with patch('coordinator.worktree_salvage.preserve_worktree',
                   AsyncMock(side_effect=OSError('preservation failed'))):
            self.assert_retained(task)
        self.assertFalse(self.trash.exists())

    def test_deinitialized_submodule_with_hidden_commits_is_kept(self):
        task, tip = self.submodule_job()
        target = self.target(task)
        self.git('submodule', 'deinit', '-f', 'sub', cwd=target)
        self.assert_retained(task)
        self.assertFalse(self.trash.exists())

    def test_lossy_untracked_git_listing_is_kept(self):
        task, _ = self.submodule_job()
        original = workspaces._git

        async def lossy_listing(repository, *args, **options):
            if 'ls-files' in args and '--others' in args:
                return 0, 'invalid-\ufffd\0'
            return await original(repository, *args, **options)

        with patch('coordinator.workspaces._git', side_effect=lossy_listing):
            self.assert_retained(task)
        self.assertFalse(self.trash.exists())

    def test_uninitialized_submodule_can_be_retired(self):
        task, _ = self.submodule_job(initialized=False)
        moved = asyncio.run(cleanup_task_workspace(self.store, task['id'], now=self.now))
        self.assertIsNotNone(moved)
        self.assertFalse(self.target(task).exists())

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

    def test_f3_sweep_preserves_checkout_admin_logs_index_branch_and_stash(self):
        task = self.job()
        target = self.target(task)
        tip = self.commit(target)
        (target / 'README.md').write_text('stashed bytes\n')
        self.git('stash', 'push', '-qm', 'retained stash', cwd=target)
        stash = self.git('stash', 'list').stdout
        git_dir = self.git_directory(target)
        admin_before = self.files(git_dir)
        self.assertIn('index', admin_before)
        self.assertIn('logs/HEAD', admin_before)
        (target / '.env').write_bytes(b'ignored bytes\x00\xff')
        checkout_before = self.files(target)
        moved = asyncio.run(cleanup_task_workspace(self.store, task['id'], now=self.now))
        self.assertEqual(moved, self.trash / target.name)
        self.assertEqual(self.files(moved), checkout_before)
        self.assertEqual(self.files(self.trash / (target.name + '.git-worktree')), admin_before)
        self.assertFalse(git_dir.exists())
        self.assertNotIn('worktree ' + str(target) + '\n', self.registrations())
        self.assertEqual(self.git('rev-parse', 'torii/task-' + str(task['id'])).stdout.strip(), tip)
        self.assertEqual(self.git('stash', 'list').stdout, stash)
        result = asyncio.run(prepare_task_workspace(
            self.project, self.state, task['id'], saved_cwd=task['worktree']))
        self.assertEqual(result['base_commit'], tip)
        self.assertEqual(self.git('rev-parse', 'HEAD', cwd=target).stdout.strip(), tip)

    def test_f3_admin_move_failure_restores_checkout_and_keeps_registration(self):
        task = self.job()
        target = self.target(task)
        git_dir = self.git_directory(target)
        before = self.files(git_dir)
        real_move = workspaces.move_to_trash

        def fail_admin(path, *args, **kwargs):
            if Path(path) == git_dir:
                self.assertFalse(target.exists())
                raise OSError('admin rename fixture failure')
            return real_move(path, *args, **kwargs)

        with patch('coordinator.workspaces.move_to_trash', side_effect=fail_admin):
            self.assert_retained(task)
        self.assertEqual(self.files(git_dir), before)
        self.assertEqual(list(self.trash.iterdir()), [])

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

    def test_empty_admin_modules_without_checkout_gitmodules_can_be_retired(self):
        task = self.job()
        target = self.target(task)
        (self.git_directory(target) / 'modules').mkdir()
        self.assertFalse((target / '.gitmodules').exists())
        moved = asyncio.run(cleanup_task_workspace(self.store, task['id'], now=self.now))
        self.assertIsNotNone(moved)
        self.assertFalse(target.exists())

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

    def test_sweep_refuses_missing_or_ambiguous_admin_mapping_before_either_move(self):
        for ambiguous in (False, True):
            with self.subTest(ambiguous=ambiguous):
                task = self.job()
                target = self.target(task)
                git_dir = self.git_directory(target)
                if ambiguous:
                    duplicate = git_dir.parent / ('duplicate-' + target.name)
                    duplicate.mkdir()
                    for name in ('gitdir', 'HEAD', 'commondir'):
                        (duplicate / name).write_bytes((git_dir / name).read_bytes())
                else:
                    (git_dir / 'gitdir').write_text(str(self.root / 'wrong-target' / '.git') + '\n')
                before = self.files(git_dir.parent)
                with patch('coordinator.workspaces.move_to_trash') as move:
                    self.assert_retained(task)
                move.assert_not_called()
                self.assertEqual(self.files(git_dir.parent), before)
                self.assertFalse(self.trash.exists())

    def test_sweep_checks_both_filesystems_before_either_move(self):
        for different in ('checkout', 'admin'):
            with self.subTest(different=different):
                task = self.job()
                target = self.target(task)
                git_dir = self.git_directory(target)
                source = target if different == 'checkout' else git_dir
                real_lstat = Path.lstat

                def different_device(path, *args, **kwargs):
                    result = real_lstat(path, *args, **kwargs)
                    if path == source:
                        fields = list(result)
                        fields[2] += 1
                        return os.stat_result(fields)
                    return result

                with patch.object(Path, 'lstat', different_device), \
                        patch('coordinator.workspaces.move_to_trash') as move, \
                        self.assertLogs('coordinator.workspaces', level='DEBUG') as logs:
                    self.assert_retained(task)
                move.assert_not_called()
                self.assertIn('another filesystem', logs.output[0])
                self.assertFalse(self.trash.exists())

    def test_checkout_move_failure_leaves_admin_and_registration_untouched(self):
        task = self.job()
        target = self.target(task)
        git_dir = self.git_directory(target)
        before = self.files(git_dir)
        with patch('coordinator.workspaces.move_to_trash', side_effect=OSError('checkout rename failure')) as move:
            self.assert_retained(task)
        move.assert_called_once_with(target, self.trash)
        self.assertEqual(self.files(git_dir), before)
        self.assertEqual(list(self.trash.iterdir()), [])

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

    def test_admin_directory_is_found_by_mapping_instead_of_folder_name(self):
        task = self.job()
        target = self.target(task)
        git_dir = self.git_directory(target)
        renamed = git_dir.with_name('renamed-admin-directory')
        git_dir.rename(renamed)
        (target / '.git').write_text('gitdir: ' + str(renamed) + '\n')
        before = self.files(renamed)
        moved = asyncio.run(cleanup_task_workspace(self.store, task['id'], now=self.now))
        self.assertEqual(moved, self.trash / target.name)
        self.assertEqual(self.files(self.trash / (target.name + '.git-worktree')), before)
        self.assertFalse(renamed.exists())
        self.assertNotIn('worktree ' + str(target) + '\n', self.registrations())

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

    def test_sweep_quietly_skips_externally_trashed_worktree_and_continues(self):
        task = self.job()
        target = self.target(task)
        tip = self.commit(target)
        payload = b'externally preserved ignored bytes\x00\xff'
        (target / '.env').write_bytes(payload)
        later = self.job()
        self.trash.mkdir()
        moved = self.trash / target.name
        target.rename(moved)
        registrations = self.registrations()
        branches = self.git('show-ref', '--heads').stdout
        with patch.object(workspaces, 'logger') as logger, \
                patch('coordinator.problems.record') as problem, \
                patch('coordinator.workspaces._git', new=AsyncMock()) as git, \
                patch('coordinator.workspaces.cleanup_task_workspace',
                      new=AsyncMock(wraps=cleanup_task_workspace)) as cleanup:
            self.assertIsNone(asyncio.run(workspaces.cleanup_task_workspace(self.store, task['id'], now=self.now)))
            cleanup.assert_awaited_once_with(self.store, task['id'], now=self.now)
            git.assert_not_awaited()
            problem.assert_not_called()
            self.assertEqual(logger.mock_calls, [])
        self.assertFalse(target.exists())
        self.assertEqual(self.store.task_get(task['id']), task)
        self.assertEqual(self.registrations(), registrations)
        self.assertEqual(self.git('show-ref', '--heads').stdout, branches)
        self.assertEqual(self.git('rev-parse', 'torii/task-' + str(task['id'])).stdout.strip(), tip)
        self.assertEqual((moved / 'README.md').read_text(), 'job commit\n')
        self.assertEqual((moved / '.env').read_bytes(), payload)
        with patch('coordinator.workspaces.cleanup_task_workspace',
                   new=AsyncMock(wraps=cleanup_task_workspace)) as cleanup:
            cursor = asyncio.run(cleanup_closed_worktrees(self.store, limit=1, now=self.now))
            self.assertEqual(cursor, later['id'])
            cleanup.assert_awaited_once_with(self.store, later['id'], now=self.now)
            self.assertEqual(asyncio.run(cleanup_closed_worktrees(
                self.store, limit=1, now=self.now)), 0)
            self.assertEqual(cleanup.await_count, 1)
        self.assertFalse(self.target(later).exists())
        self.assertIn('worktree ' + str(target) + '\n', self.registrations())

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

    def test_c1_cleanup_uses_suffix_keeps_branches_and_next_spawn_rebuilds(self):
        task = self.job()
        target = self.target(task)
        tip = self.commit(target)
        unrelated = self.missing_unrelated_worktree()
        branches = self.git('show-ref', '--heads').stdout
        collision = self.trash / target.name
        collision.mkdir(parents=True)
        (collision / 'keep').write_text('existing Trash data')
        second = self.trash / (target.name + '-1')
        second.symlink_to(self.root / 'missing-collision')
        with self.assertLogs('coordinator.workspaces', level='INFO') as logs:
            moved = asyncio.run(cleanup_task_workspace(self.store, task['id'], now=self.now))
        self.assertEqual(moved, self.trash / (target.name + '-2'))
        self.assertTrue(moved.is_dir())
        self.assertFalse(target.exists())
        self.assertEqual((moved / 'README.md').read_text(), 'job commit\n')
        self.assertEqual((collision / 'keep').read_text(), 'existing Trash data')
        self.assertTrue(second.is_symlink())
        self.assertNotIn('worktree ' + str(target) + '\n', self.registrations())
        self.assertIn('worktree ' + str(unrelated) + '\n', self.registrations())
        self.assertEqual(self.git('show-ref', '--heads').stdout, branches)
        self.assertEqual(self.store.task_get(task['id'])['updated'], task['updated'])
        self.assertEqual(len(logs.output), 1)
        for value in (str(task['number']), str(moved), str(self.project / '.git')):
            self.assertIn(value, logs.output[0])
        self.assertIsNone(asyncio.run(cleanup_task_workspace(self.store, task['id'], now=self.now)))
        self.call('tasks.update', task=task['id'], status='open')
        worker = self.call('workers.spawn', task=task['id'], provider='codex', prompt='Resume this job').data
        row = self.store.db.execute('SELECT cwd,status FROM workers WHERE id=?', (worker['worker'],)).fetchone()
        self.assertEqual((row['cwd'], row['status']), (str(target), 'queued'))
        self.assertEqual(self.git('rev-parse', 'HEAD', cwd=target).stdout.strip(), tip)
        self.assertIn('worktree ' + str(unrelated) + '\n', self.registrations())

    def test_c2_modified_tracked_file_is_kept(self):
        task = self.job()
        (self.target(task) / 'README.md').write_text('unsaved tracked bytes\n')
        with self.assertLogs('coordinator.workspaces', level='DEBUG') as logs:
            target = self.assert_retained(task)
        self.assertEqual((target / 'README.md').read_text(), 'unsaved tracked bytes\n')
        self.assertEqual(len(logs.output), 1)
        self.assertIn('changes', logs.output[0])

    def test_c3_untracked_file_is_kept_even_when_status_config_hides_it(self):
        task = self.job()
        target = self.target(task)
        self.git('config', 'status.showUntrackedFiles', 'no', cwd=target)
        (target / 'untracked.txt').write_bytes(b'unsaved untracked bytes\x00')
        self.assert_retained(task)
        self.assertEqual((target / 'untracked.txt').read_bytes(), b'unsaved untracked bytes\x00')

    def test_c4_detached_head_is_kept(self):
        task = self.job()
        self.git('checkout', '--detach', '-q', cwd=self.target(task))
        self.assert_retained(task)

    def test_c5_real_rebase_in_progress_is_kept(self):
        task = self.job()
        target = self.target(task)
        self.commit(target, 'job side\n')
        self.commit(self.project, 'source side\n')
        source_branch = self.git('symbolic-ref', '--short', 'HEAD').stdout.strip()
        self.assertNotEqual(self.git('rebase', source_branch, cwd=target, check=False).returncode, 0)
        self.assert_retained(task)

    def test_c6_locked_worktree_is_kept(self):
        task = self.job()
        self.git('worktree', 'lock', '--reason', 'owner keeps this copy', self.target(task))
        self.assert_retained(task)

    def test_c7_every_unfinished_status_and_provider_keeps_the_job(self):
        task = self.job()
        target = self.target(task)
        worker = self.worker(task['id'], target)
        for provider in ('claude', 'codex', 'opencode'):
            for status in ('queued', 'running', 'parked', 'waiting', 'waiting_for_secret',
                           'waiting_for_quota', 'future_unfinished'):
                with self.subTest(provider=provider, status=status), self.store.db:
                    self.store.db.execute('UPDATE workers SET provider=?,status=? WHERE id=?',
                                          (provider, status, worker))
                    self.assert_retained(task)

    def test_c8_recently_closed_job_is_kept(self):
        self.assert_retained(self.job(age=86399))

    def test_c9_shared_stash_survives_cleanup(self):
        task = self.job()
        target = self.target(task)
        (target / 'README.md').write_text('stashed job bytes\n')
        self.git('stash', 'push', '-qm', 'saved job work', cwd=target)
        stash = self.git('stash', 'list').stdout
        self.assertTrue(stash)
        self.assertIsNotNone(asyncio.run(cleanup_task_workspace(self.store, task['id'], now=self.now)))
        self.assertEqual(self.git('stash', 'list').stdout, stash)
        self.assertEqual(self.git('show', 'stash:README.md').stdout, 'stashed job bytes\n')

    def test_c10_ignored_env_bytes_go_to_trash(self):
        task = self.job()
        payload = b'fixture ignored bytes\r\n\x00\xff'
        (self.target(task) / '.env').write_bytes(payload)
        moved = asyncio.run(cleanup_task_workspace(self.store, task['id'], now=self.now))
        self.assertIsNotNone(moved)
        self.assertEqual((moved / '.env').read_bytes(), payload)

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

    def test_sweep_resumes_saved_cursor_after_service_and_store_restart(self):
        tasks = [self.job() for _ in range(12)]
        for task in tasks[:10]:
            (self.target(task) / 'unsaved.txt').write_text('keep early job')
        for restarted in (False, True):
            if restarted:
                self.store.close()
                self.store = Store(self.state, token_file=self.root / 'no-token')
            service = Service.__new__(Service)
            service.store = self.store
            with patch('coordinator.service.asyncio.sleep', side_effect=asyncio.CancelledError), \
                    self.assertRaises(asyncio.CancelledError):
                asyncio.run(service.worktree_cleanup_loop())
        self.assertTrue(all(self.target(task).is_dir() for task in tasks[:10]))
        for task in tasks[10:]:
            self.assertFalse(self.target(task).exists())
            self.assertTrue((self.trash / self.target(task).name).is_dir())
        self.assertEqual(self.store.get('closed_worktree_cleanup_cursor'), 0)

    def test_sweep_skips_more_than_ten_gone_folders_for_free(self):
        gone = [self.job() for _ in range(12)]
        for task in gone:
            self.target(task).rename(self.root / ('gone-' + str(task['id'])))
        eligible = self.job()
        asyncio.run(cleanup_closed_worktrees(self.store, now=self.now))
        self.assertFalse(self.target(eligible).exists())
        self.assertTrue((self.trash / self.target(eligible).name).is_dir())
        for task in gone:
            self.assertTrue((self.root / ('gone-' + str(task['id']))).is_dir())

    def test_sweep_caps_git_checked_jobs_and_next_call_continues(self):
        tasks = [self.job() for _ in range(12)]
        for task in tasks:
            (self.target(task) / 'unsaved.txt').write_text('keep dirty job')
        with patch('coordinator.workspaces._git', new=AsyncMock(wraps=workspaces._git)) as git:
            asyncio.run(cleanup_closed_worktrees(self.store, now=self.now))
            checked = [call.args[0] for call in git.await_args_list if call.args[1:2] == ('status',)]
            self.assertEqual(checked, [self.target(task) for task in tasks[:10]])
            self.assertEqual(self.store.get('closed_worktree_cleanup_cursor'), tasks[9]['id'])
            git.reset_mock()
            asyncio.run(cleanup_closed_worktrees(self.store, now=self.now))
            checked = [call.args[0] for call in git.await_args_list if call.args[1:2] == ('status',)]
            self.assertEqual(checked, [self.target(task) for task in tasks[10:]])
        self.assertTrue(all(self.target(task).is_dir() for task in tasks))
        self.assertEqual(self.store.get('closed_worktree_cleanup_cursor'), 0)

    def test_sweep_wraps_saved_cursor_after_last_job(self):
        tasks = [self.job() for _ in range(2)]
        for task in tasks:
            (self.target(task) / 'unsaved.txt').write_text('keep for next round')
        for expected in (tasks[0]['id'], tasks[1]['id'], 0, tasks[0]['id']):
            self.assertEqual(asyncio.run(cleanup_closed_worktrees(self.store, limit=1, now=self.now)), expected)
            self.assertEqual(self.store.get('closed_worktree_cleanup_cursor'), expected)
        self.assertTrue(all(self.target(task).is_dir() for task in tasks))

    def test_sweep_pre_git_rejections_do_not_spend_git_budget(self):
        recent = self.job(age=60)
        linked = self.job()
        preserved = self.root / 'symlink-target'
        self.target(linked).rename(preserved)
        self.target(linked).symlink_to(preserved, target_is_directory=True)
        busy = self.job()
        self.worker(busy['id'], self.target(busy))
        eligible = self.job()
        asyncio.run(cleanup_closed_worktrees(self.store, limit=1, now=self.now))
        self.assertFalse(self.target(eligible).exists())
        self.assertTrue((self.trash / self.target(eligible).name).is_dir())
        self.assertTrue(self.target(recent).is_dir())
        self.assertTrue(self.target(linked).is_symlink())
        self.assertTrue(preserved.is_dir())
        self.assertTrue(self.target(busy).is_dir())

    def test_c13_default_bound_and_cursor_advance_past_skipped_jobs(self):
        tasks = [self.job() for _ in range(12)]
        for task in tasks[:2]:
            (self.target(task) / 'unsaved.txt').write_text('keep early job')
        cursor = asyncio.run(cleanup_closed_worktrees(self.store, now=self.now))
        self.assertEqual(cursor, tasks[9]['id'])
        self.assertTrue(all(self.target(task).is_dir() for task in tasks[:2] + tasks[10:]))
        self.assertTrue(all(not self.target(task).exists() for task in tasks[2:10]))
        self.assertEqual(asyncio.run(cleanup_closed_worktrees(self.store, now=self.now)), 0)
        self.assertTrue(all(not self.target(task).exists() for task in tasks[10:]))
        self.assertTrue(all(self.target(task).is_dir() for task in tasks[:2]))

    def test_saved_nested_cwd_reopens_through_creation_and_spawn(self):
        task = self.job(nested=True)
        target = self.target(task)
        tip = self.commit(target)
        self.assertIsNotNone(asyncio.run(cleanup_task_workspace(self.store, task['id'], now=self.now)))
        self.call('tasks.update', task=task['id'], status='open')
        created = self.call('worktree.create', task=task['id'], base='missing-base')
        self.assertEqual(created.data['cwd'], str(target / 'nested'))
        self.assertEqual(created.data['base_commit'], tip)
        target.rename(self.root / 'removed-nested-copy')
        worker = self.call('workers.spawn', task=task['id'], provider='codex', prompt='Nested job').data
        row = self.store.db.execute('SELECT cwd FROM workers WHERE id=?', (worker['worker'],)).fetchone()
        self.assertEqual(row['cwd'], str(target / 'nested'))
        self.assertEqual(self.git('rev-parse', 'HEAD', cwd=target).stdout.strip(), tip)

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

    def test_open_jobs_are_kept_and_dropped_jobs_at_24_hours_are_cleaned(self):
        task = self.job(closed=False)
        with self.store.db:
            self.store.db.execute('UPDATE tasks SET updated=? WHERE id=?', (self.now - 86401, task['id']))
        self.assert_retained(task)
        dropped = self.job(status='dropped', age=86400)
        self.assertIsNotNone(asyncio.run(cleanup_task_workspace(self.store, dropped['id'], now=self.now)))

    def test_other_jobs_unfinished_worker_inside_the_folder_is_kept(self):
        task = self.job()
        other = self.job(closed=False)
        alias = self.root / 'worker-cwd-alias'
        alias.symlink_to(self.target(task), target_is_directory=True)
        self.worker(other['id'], alias / 'nested', status='parked', provider='opencode')
        self.assert_retained(task)

    def test_finished_workers_and_neighboring_path_prefix_do_not_block_cleanup(self):
        task = self.job()
        target = self.target(task)
        self.worker(task['id'], target, status='done')
        self.worker(task['id'], target / 'nested', status='interrupted', provider='claude')
        self.worker(None, str(target) + '-other', status='running')
        self.assertIsNotNone(asyncio.run(cleanup_task_workspace(self.store, task['id'], now=self.now)))

    def test_clean_git_operation_markers_keep_the_folder(self):
        for marker in ('rebase-merge', 'rebase-apply', 'MERGE_HEAD', 'CHERRY_PICK_HEAD',
                       'REVERT_HEAD', 'BISECT_LOG', 'BISECT_START', 'sequencer'):
            with self.subTest(marker=marker):
                task = self.job()
                target = self.target(task)
                git_dir = Path(self.git('rev-parse', '--absolute-git-dir', cwd=target).stdout.strip())
                if marker in ('rebase-merge', 'rebase-apply', 'sequencer'):
                    (git_dir / marker).mkdir()
                else:
                    (git_dir / marker).write_text(self.base + '\n')
                self.assert_retained(task)

    def test_attached_feature_branch_is_cleaned_and_every_branch_is_kept(self):
        task = self.job()
        target = self.target(task)
        self.git('checkout', '-qb', 'feat/closed-job-worktree-cleanup', cwd=target)
        tip = self.commit(target, 'feature branch commit\n')
        branches = self.git('for-each-ref', '--format=%(refname) %(objectname)', 'refs/heads').stdout
        moved = asyncio.run(cleanup_task_workspace(self.store, task['id'], now=self.now))
        self.assertEqual(moved, self.trash / target.name)
        self.assertFalse(target.exists())
        self.assertNotIn('worktree ' + str(target) + '\n', self.registrations())
        self.assertEqual(self.git('for-each-ref', '--format=%(refname) %(objectname)',
                                  'refs/heads').stdout, branches)
        self.assertEqual(self.git('rev-parse', 'feat/closed-job-worktree-cleanup').stdout.strip(), tip)
        self.assertEqual(self.git('rev-parse', 'torii/task-' + str(task['id'])).stdout.strip(), self.base)

    def test_foreign_repository_at_the_expected_path_is_kept(self):
        foreign = self.job()
        foreign_target = self.target(foreign)
        foreign_target.rename(self.root / 'preserved-original-worktree')
        foreign_target.mkdir()
        self.git('init', '-q', cwd=foreign_target)
        self.git('-c', 'user.name=Test', '-c', 'user.email=test@example.com',
                 'commit', '--allow-empty', '-qm', 'foreign repository', cwd=foreign_target)
        self.assert_retained(foreign)
        self.assertTrue((foreign_target / '.git').is_dir())

    def test_task_folder_and_parent_symlinks_are_kept(self):
        task = self.job()
        target = self.target(task)
        moved = self.root / 'preserved-task-folder'
        target.rename(moved)
        target.symlink_to(moved, target_is_directory=True)
        self.assert_retained(task)
        self.assertTrue(target.is_symlink())
        parent = target.parent
        relocated = self.root / 'preserved-worktrees-folder'
        parent.rename(relocated)
        parent.symlink_to(relocated, target_is_directory=True)
        self.assert_retained(task)
        self.assertTrue(parent.is_symlink())

    def test_cross_filesystem_trash_keeps_folder_and_registration(self):
        task = self.job()
        real_stat = Path.stat

        def different_device(path, *args, **kwargs):
            result = real_stat(path, *args, **kwargs)
            if path == self.trash:
                fields = list(result)
                fields[2] += 1
                return os.stat_result(fields)
            return result

        with patch.object(Path, 'stat', different_device):
            self.assert_retained(task)
        self.assertFalse((self.trash / self.target(task).name).exists())

    def test_git_error_before_move_keeps_folder_and_registration(self):
        task = self.job()
        real_git = workspaces._git

        async def fail_status(cwd, *args, **kwargs):
            if args[0] == 'status':
                raise RuntimeError('status fixture error')
            return await real_git(cwd, *args, **kwargs)

        with patch('coordinator.workspaces._git', side_effect=fail_status):
            self.assert_retained(task)
        self.assertFalse(self.trash.exists())

    def test_registration_removal_error_restores_folder_and_ignored_data(self):
        task = self.job()
        target = self.target(task)
        (target / '.env').write_bytes(b'keep ignored fixture bytes')
        git_dir = self.git_directory(target)
        real_move = workspaces.move_to_trash

        def fail_move(path, *args, **kwargs):
            if Path(path) == git_dir:
                self.assertFalse(target.exists())
                self.assertTrue((self.trash / target.name).is_dir())
                raise RuntimeError('registration fixture error')
            return real_move(path, *args, **kwargs)

        with patch('coordinator.workspaces.move_to_trash', side_effect=fail_move):
            self.assert_retained(task)
        self.assertEqual((target / '.env').read_bytes(), b'keep ignored fixture bytes')
        self.assertFalse((self.trash / target.name).exists())
        self.assertEqual(self.git('rev-parse', 'HEAD', cwd=target).stdout.strip(), self.base)

    def test_rollback_keeps_both_copies_when_another_path_prevents_restoration(self):
        task = self.job()
        target = self.target(task)
        (target / '.env').write_bytes(b'original ignored fixture bytes')
        git_dir = self.git_directory(target)
        real_move = workspaces.move_to_trash

        def replace_before_error(path, *args, **kwargs):
            if Path(path) == git_dir:
                target.mkdir()
                (target / 'replacement').write_text('new data')
                raise RuntimeError('registration fixture error')
            return real_move(path, *args, **kwargs)

        with patch('coordinator.workspaces.move_to_trash', side_effect=replace_before_error), \
                self.assertLogs('coordinator.workspaces', level='ERROR') as logs:
            self.assertIsNone(asyncio.run(cleanup_task_workspace(self.store, task['id'], now=self.now)))
        self.assertEqual((target / 'replacement').read_text(), 'new data')
        self.assertEqual((self.trash / target.name / '.env').read_bytes(), b'original ignored fixture bytes')
        self.assertIn('rollback failed', logs.output[0])
        self.assertIn('worktree ' + str(target) + '\n', self.registrations())

    def test_cancellation_during_registration_removal_restores_folder(self):
        task = self.job()
        target = self.target(task)
        before = self.registrations()
        git_dir = self.git_directory(target)
        real_move = workspaces.move_to_trash

        def cancel_move(path, *args, **kwargs):
            if Path(path) == git_dir:
                raise asyncio.CancelledError()
            return real_move(path, *args, **kwargs)

        with patch('coordinator.workspaces.move_to_trash', side_effect=cancel_move), \
                self.assertRaises(asyncio.CancelledError):
            asyncio.run(cleanup_task_workspace(self.store, task['id'], now=self.now))
        self.assertTrue(target.is_dir())
        self.assertEqual(self.registrations(), before)
        self.assertFalse((self.trash / target.name).exists())

    def test_reopen_or_worker_started_during_git_checks_is_seen_before_move(self):
        for change in ('reopen', 'worker', 'lock', 'operation', 'branch', 'gitmodules', 'modules'):
            with self.subTest(change=change):
                task = self.job()
                target = self.target(task)
                real_git = workspaces._git

                async def change_after_status(cwd, *args, **kwargs):
                    result = await real_git(cwd, *args, **kwargs)
                    if args[0] == 'status':
                        if change == 'reopen':
                            self.store.task_update(task['id'], status='open')
                        elif change == 'worker':
                            self.worker(None, target / 'nested', provider='opencode')
                        elif change == 'lock':
                            self.git('worktree', 'lock', target)
                        elif change == 'branch':
                            self.git('checkout', '-qb', 'changed-during-cleanup', cwd=target)
                        elif change == 'gitmodules':
                            (target / '.gitmodules').write_text('late submodule metadata\n')
                        elif change == 'modules':
                            (self.git_directory(target) / 'modules').mkdir()
                        else:
                            git_dir = Path(self.git('rev-parse', '--absolute-git-dir', cwd=target).stdout.strip())
                            (git_dir / 'rebase-apply').mkdir()
                    return result

                with patch('coordinator.workspaces._git', side_effect=change_after_status):
                    self.assertIsNone(asyncio.run(cleanup_task_workspace(self.store, task['id'], now=self.now)))
                self.assertTrue(target.is_dir())
                self.assertIn('worktree ' + str(target) + '\n', self.registrations())

    def test_git_timeout_stops_slow_status_and_preserves_the_folder(self):
        task = self.job()
        target = self.target(task)
        ready, lock = self.root / 'fsmonitor.ready', self.root / 'fsmonitor.lock'
        helper = self.root / 'slow-fsmonitor'
        helper.write_text('#!' + sys.executable + '\n'
                          'import fcntl,time\n'
                          'from pathlib import Path\n'
                          'handle = open(' + repr(str(lock)) + ', "a")\n'
                          'fcntl.flock(handle, fcntl.LOCK_EX)\n'
                          'Path(' + repr(str(ready)) + ').touch()\n'
                          'time.sleep(60)\n')
        helper.chmod(0o700)
        self.git('config', 'core.fsmonitor', str(helper), cwd=target)

        async def attempt():
            result = await cleanup_task_workspace(self.store, task['id'], now=self.now)
            self.assertIsNone(result)
            await until_unlocked(lock, 'the timed-out Git status process group to die')

        with patch('coordinator.workspaces.GIT_TIMEOUT', 1), self.assertLogs('coordinator.workspaces', 'DEBUG') as logs:
            asyncio.run(attempt())
        self.assertTrue(any('TimeoutError' in line for line in logs.output))
        self.assertTrue(ready.exists())
        self.assertTrue(target.is_dir())
        self.assertIn('worktree ' + str(target) + '\n', self.registrations())

    def test_service_startup_and_hourly_sweeps_cleanup_closed_worktrees(self):
        startup = self.job()
        hourly = self.job(age=0)
        tips = {task['id']: self.git('rev-parse', 'HEAD', cwd=self.target(task)).stdout.strip()
                for task in (startup, hourly)}
        service = Service.__new__(Service)
        service.store = self.store
        service.pair_only = False
        service.workers = SimpleNamespace(runner=SimpleNamespace(binaries={}))
        service.updater = SimpleNamespace(run=None, freeze=None)
        service.signins = SimpleNamespace(run=None)
        self.assertIn(('worktree_cleanup', service.worktree_cleanup_loop), service.supervised())
        delays = []

        async def next_hour(delay):
            delays.append(delay)
            task = startup if len(delays) == 1 else hourly
            target = self.target(task)
            self.assertFalse(target.exists())
            self.assertTrue((self.trash / target.name).is_dir())
            self.assertTrue((self.trash / (target.name + '.git-worktree')).is_dir())
            self.assertEqual(self.git('rev-parse', 'torii/task-' + str(task['id'])).stdout.strip(),
                             tips[task['id']])
            self.assertEqual(self.store.get('closed_worktree_cleanup_cursor'), 0)
            if len(delays) == 1:
                self.assertTrue(self.target(hourly).is_dir())
                with self.store.db:
                    self.store.db.execute('UPDATE tasks SET updated=? WHERE id=?',
                                          (self.now - 86401, hourly['id']))
            else:
                raise asyncio.CancelledError()

        with patch('coordinator.service.asyncio.sleep', side_effect=next_hour), \
                self.assertRaises(asyncio.CancelledError):
            asyncio.run(service.worktree_cleanup_loop())
        self.assertEqual(delays, [3600, 3600])


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
