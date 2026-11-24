import asyncio
import inspect
import json
from pathlib import Path
import shutil
import time
import unittest
import uuid
from unittest.mock import patch

from coordinator import close_saves, process_ownership, workspaces
from tests import test_worktree_cleanup as fixtures


class ClosedJobClearTests(unittest.TestCase):
    setUp = fixtures.WorktreeCleanupTests.setUp
    tearDown = fixtures.WorktreeCleanupTests.tearDown
    git = fixtures.WorktreeCleanupTests.git
    call = fixtures.WorktreeCleanupTests.call
    target = fixtures.WorktreeCleanupTests.target
    worker = fixtures.WorktreeCleanupTests.worker
    commit = fixtures.WorktreeCleanupTests.commit
    submodule_job = fixtures.WorktreeCleanupTests.submodule_job

    def job(self, closed=False):
        with self.store.db:
            self.store.db.execute('UPDATE topics SET enabled=1')
        return fixtures.WorktreeCleanupTests.job(self, closed=closed)

    def drain(self, sweep=False):
        asyncio.run(workspaces.cleanup_closed_copies(self.store, sweep=sweep))
        asyncio.run(workspaces.cleanup_closed_copies(self.store))

    def close(self, task, status='done'):
        self.call('tasks.update', task=task['id'], status=status, **({'summary': 'Closed in test.'} if status in ('done', 'dropped') else {}), **({'note': 'No longer needed'} if status == 'dropped' else {}))
        self.drain()
        return self.store.task_get(task['id'])

    def saved(self, task):
        folder = self.state / 'results' / ('task-' + str(task['id'])) / 'rounds' / str(task['iteration'])
        return json.loads((folder / 'saved.json').read_text())

    def assert_cleared(self, task, closed):
        self.assertEqual(closed['copy'], 'cleared', closed['copy_reason'])
        self.assertFalse(self.target(task).exists())
        self.assertNotIn(str(self.target(task)), self.git('worktree', 'list', '--porcelain').stdout)
        self.assertEqual(list(workspaces._deletion_stages(self.store.directory)), [])

    def wip_files(self, task):
        ref = 'refs/torii/saved/task-%s/%s/wip' % (task['id'], task['iteration'])
        return set(self.git('ls-tree', '-r', '--name-only', ref).stdout.split())

    def notices(self):
        return [row['text'] for row in self.store.db.execute("SELECT text FROM outbox WHERE text LIKE 'Job %'")]

    def test_missing_results_retires_stage_with_saved_ref_and_keeps_payload(self):
        for custom_branch in (False, True):
            for new_iteration in (False, True):
                with self.subTest(custom_branch=custom_branch, new_iteration=new_iteration):
                    self.assert_retired_copy_reopens(custom_branch, new_iteration)

    def assert_retired_copy_reopens(self, custom_branch, new_iteration):
        task = self.job()
        target = self.target(task)
        branch = 'feature-%s' % task['id'] if custom_branch else 'torii/task-%s' % task['id']
        if custom_branch:
            self.git('switch', '-c', branch, cwd=target)
        content = 'saved work\n'
        head = self.commit(target, content)
        if not new_iteration:
            self.git('update-ref', 'refs/remotes/origin/' + branch, head)
        self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
        asyncio.run(close_saves.save_job(self.store, task['id']))
        stage, record = next((stage, record) for stage, record in workspaces._deletion_stages(self.state)
                             if record['task'] == task['id'])
        saved = self.saved(task)
        self.assertEqual(bool(saved.get('bundle')), new_iteration)
        ref = saved['head_ref']
        stage_head_path = stage / 'git' / 'HEAD'
        stage_head = head
        if new_iteration:
            stage_head = self.git('commit-tree', head + '^{tree}', '-p', self.base, '-m', 'staged work').stdout.strip()
            stage_head_path.write_text(stage_head + '\n')
        stage_pointer = stage_head_path.read_text()
        del saved['ignored']
        close_saves.checkpoint(self.store, task, 'staged', saved)
        results = self.state / 'results' / ('task-' + str(task['id']))
        shutil.rmtree(results)
        record['loose'] = True
        workspaces._save_deletion(stage, record)
        payload = workspaces.staged_copy(stage, record) / 'retained-file'
        payload.write_bytes(b'only in staging')
        self.assertFalse(self.target(task).exists())

        self.drain(sweep=True)
        retired = self.store.task_get(task['id'])
        self.assertEqual(retired['copy'], 'cleared')
        self.call('cleanup.retry', task=task['id'])
        self.drain(sweep=True)

        self.assertEqual(self.store.task_get(task['id'])['copy'], 'cleared')
        self.assertEqual(close_saves.load(self.store, task)[0], 'retired')
        rows = self.store.db.execute("SELECT code,detail FROM problems WHERE task=? AND area='workspaces'",
                                     (task['id'],)).fetchall()
        self.assertEqual([row['code'] for row in rows], ['closed-copy-retired'])
        self.assertIn(ref, rows[0]['detail'])
        self.assertIn(stage.name, rows[0]['detail'])
        self.assertEqual(self.git('rev-parse', ref).stdout.strip(), head)
        self.assertEqual(payload.read_bytes(), b'only in staging')
        self.assertTrue((stage / 'git' / 'HEAD').is_file())
        self.assertFalse(results.exists())
        self.assertIsNone(asyncio.run(close_saves.deletion_delay(self.store)))

        self.call('tasks.update', task=task['id'], status='open')
        if new_iteration:
            with self.store.db:
                self.store.db.execute("INSERT INTO task_iterations(task,k,opened,opened_by) VALUES (?,?,?,'owner')",
                                      (task['id'], task['iteration'] + 1, time.time()))
                self.store.db.execute('UPDATE tasks SET iteration=? WHERE id=?', (task['iteration'] + 1, task['id']))
        self.call('worktree.create', task=task['id'])
        self.assertEqual(self.git('branch', '--show-current', cwd=target).stdout.strip(), branch)
        self.assertEqual(self.git('rev-parse', 'HEAD', cwd=target).stdout.strip(), head)
        self.assertEqual((target / 'README.md').read_text(), content)
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'present')
        self.assertEqual(close_saves.load(self.store, task)[0], 'restored')
        self.assertEqual(payload.read_bytes(), b'only in staging')
        self.assertEqual(stage_head_path.read_text(), stage_pointer)
        self.assertIn('staged HEAD ' + stage_head, retired['copy_reason'])
        self.assertIn('staged HEAD ' + stage_head, rows[0]['detail'])

    def test_missing_manifest_preserves_active_stage_without_retirement_evidence(self):
        for blocker in ('results exist', 'worktree exists', 'saved ref moved'):
            with self.subTest(blocker=blocker):
                task = self.job()
                self.commit(self.target(task), 'job work\n')
                self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
                asyncio.run(close_saves.save_job(self.store, task['id']))
                stage, record = next((stage, record) for stage, record in workspaces._deletion_stages(self.state)
                                     if record['task'] == task['id'])
                saved = self.saved(task)
                results = self.state / 'results' / ('task-' + str(task['id']))
                if blocker == 'results exist':
                    (results / 'rounds' / str(task['iteration']) / 'saved.json').unlink()
                else:
                    shutil.rmtree(results)
                if blocker == 'worktree exists':
                    self.target(task).mkdir()
                if blocker == 'saved ref moved':
                    self.git('update-ref', saved['head_ref'], self.base, saved['head'])

                self.drain()

                self.assertEqual(self.store.task_get(task['id'])['copy'], 'saving')
                self.assertEqual(close_saves.load(self.store, task)[0], 'staged')
                self.assertTrue(workspaces.staged_copy(stage, record).is_dir())

    def test_saved_tracked_tree_exceeds_checkpoint_limit_and_clears_with_ignored_art(self):
        task = self.job()
        target = self.target(task)
        (target / '.gitignore').write_text('*.private\n')
        tracked = target / 'tracked'
        tracked.mkdir()
        for index in range(1001):
            (tracked / ('%04d.txt' % index)).write_text('saved\n')
        self.git('add', '.', cwd=target)
        self.git('commit', '-qm', 'saved tree', cwd=target)
        head = self.git('rev-parse', 'HEAD', cwd=target).stdout.strip()
        self.git('update-ref', 'refs/remotes/origin/main', head)
        ignored = target / 'art' / 'tiles'
        ignored.mkdir(parents=True)
        for index in range(90):
            (ignored / ('%04d.private' % index)).write_bytes(b'xx')
        self.assertEqual(self.git('status', '--porcelain', cwd=target).stdout, '')
        limit = 700
        self.assertGreater(len(self.git('ls-files', cwd=target).stdout.splitlines()), limit)
        with patch.object(close_saves, 'CHECKPOINT_MAX_VALUES', limit), \
                patch.object(close_saves, 'IGNORED_LIMIT', 10):
            closed = self.close(task)
        self.assert_cleared(task, closed)
        saved = self.saved(task)
        self.assertEqual(saved['head'], head)
        self.assertIsNone(saved['wip'])
        self.assertIsNone(saved['bundle'])
        self.assertIn('ignored files exceed', saved['loose'])
        self.assertLess(sum(1 for _ in close_saves.checkpoint_values(saved)), limit)
        self.assertEqual(len(saved['ignored']['copied']), 5)
        self.assertEqual(len(saved['ignored']['discarded']), 85)
        phase, checkpoint = close_saves.load(self.store, task)
        self.assertEqual(phase, 'cleared')
        self.assertLess(sum(1 for _ in close_saves.checkpoint_values(checkpoint)), limit)

    def test_close_clears_siblings_only_for_exact_job(self):
        for _ in range(11):
            self.call('tasks.create', title='Other job')
        task = self.job()
        self.assertEqual(task['id'], 12)
        for kind in ('scratch', 'worktrees'):
            for name in ('task-12-', 'task-12-extra', 'task-123-extra'):
                path = self.state / kind / name
                path.mkdir(parents=True)
                (path / 'file').write_text(name)
        proofs = self.state / 'scratch' / 'task-12-extra' / 'proofs'
        proofs.mkdir()
        (proofs / 'proof.txt').write_text('proof')
        closed = self.close(task)
        self.assert_cleared(task, closed)
        self.assertTrue(any(entry['source'] == str(proofs / 'proof.txt') for entry in self.saved(task)['proofs']))
        for kind in ('scratch', 'worktrees'):
            self.assertFalse((self.state / kind / 'task-12-').exists())
            self.assertFalse((self.state / kind / 'task-12-extra').exists())
            self.assertEqual((self.state / kind / 'task-123-extra' / 'file').read_text(), 'task-123-extra')

    def test_close_clears_nested_sibling_worktree_registration(self):
        task = self.job()
        sibling = self.state / 'worktrees' / ('task-%s-release' % task['id'])
        sibling.mkdir()
        tree = sibling / 'branch'
        self.git('worktree', 'add', '--detach', tree, self.base)
        (tree / 'changes.txt').write_text('saved change\n')
        (tree / 'README.md').write_text('staged version\n')
        self.git('add', 'README.md', cwd=tree)
        (tree / 'README.md').write_text('working version\n')
        with (self.project / '.git' / 'info' / 'exclude').open('a') as handle:
            handle.write('saved-cache.txt\n')
        (tree / 'saved-cache.txt').write_text('ignored content\n')
        closed = self.close(task)
        self.assert_cleared(task, closed)
        self.assertFalse(sibling.exists())
        self.assertNotIn(str(tree), self.git('worktree', 'list', '--porcelain').stdout)
        _, saved = close_saves.load(self.store, task)
        saves = saved['scratch_git']
        self.assertEqual(len(saves), 1)
        self.assertIn('saved change', (Path(saves[0]) / '0.patch').read_text())
        self.assertIn('working version', (Path(saves[0]) / '0.patch').read_text())
        self.assertIn('staged version', (Path(saves[0]) / '0-index.patch').read_text())
        self.assertEqual((Path(saves[0]) / '0-ignored' / 'ignored' / 'saved-cache.txt').read_text(), 'ignored content\n')

    def test_running_worker_keeps_siblings(self):
        task = self.job()
        sibling = self.state / 'scratch' / ('task-%s-extra' % task['id'])
        sibling.mkdir(parents=True)
        self.worker(task['id'], sibling, status='running')
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'held')
        self.assertTrue(sibling.is_dir())

    def test_sibling_reflog_only_commit_is_pinned_before_the_clear(self):
        task = self.job()
        tree = self.state / 'worktrees' / ('task-%s-extra' % task['id'])
        self.git('worktree', 'add', '--detach', tree, self.base)
        lost = self.commit(tree, 'reflog only\n')
        self.git('switch', '--detach', self.base, cwd=tree)
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'cleared')
        self.assertFalse(tree.exists())
        self.assertEqual(self.git('rev-parse', close_saves.SCRATCH_REFLOG + lost).stdout.strip(), lost)

    def test_other_finished_worker_process_keeps_sibling(self):
        task = self.job()
        sibling = self.state / 'worktrees' / ('task-%s-extra' % task['id'])
        sibling.mkdir()
        other = self.call('tasks.create', title='Other job').data
        self.worker(other['id'], sibling, status='done')
        with patch('coordinator.worker_storage._processes_gone', return_value=False), \
                patch.object(close_saves, 'stop_leftovers', return_value=False):
            closed = self.close(task)
        self.assertEqual(closed['copy'], 'held')
        self.assertTrue(sibling.is_dir())

    def test_other_worker_through_symlinked_folder_blocks_close(self):
        task = self.job()
        other = self.call('tasks.create', title='Other job').data
        link = self.state / 'worktrees' / ('task-%s' % other['id'])
        link.symlink_to(self.target(task))
        (self.target(task) / 'sub').mkdir()
        nested = self.root / 'nested-link'
        nested.symlink_to(self.target(task) / 'sub')
        def alive(worker):
            return patch('coordinator.worker_storage._processes_gone', side_effect=lambda hosts: all(
                host.name != 'worker-%s' % worker for host in hosts))
        for cwd in (link, link / 'sub', nested):
            with self.subTest(cwd=cwd):
                worker = self.worker(other['id'], cwd, status='running')
                with alive(worker), self.assertRaisesRegex(ValueError, close_saves.RUNNING[1]):
                    asyncio.run(close_saves.stopped_workers(self.store, task))
        with alive(self.worker(other['id'], self.root / 'elsewhere', status='running')):
            asyncio.run(close_saves.stopped_workers(self.store, task))

    def test_resolve_folders_matches_path_resolve(self):
        folder = self.root / 'resolve'
        (folder / 'real' / 'deep').mkdir(parents=True)
        (folder / 'link').symlink_to(folder / 'real')
        (folder / 'loop').symlink_to(folder / 'link' / 'deep')
        (folder / 'file').write_text('x')
        names = [str(folder / name) for name in ('real', 'link', 'loop', 'missing', 'link/deep', 'link/gone',
                                                 'real/..', 'file/child', 'gone/child')] + ['relative', '/']
        self.assertEqual(close_saves.resolve_folders(names), {name: Path(name).resolve() for name in names})

    def test_link_named_in_other_case_or_unicode_form_is_followed(self):
        folder = self.root / 'fold'
        folder.mkdir()
        probe = folder / 'CaseProbe'
        probe.mkdir()
        insensitive = (folder / 'caseprobe').exists()
        probe.rmdir()
        if not insensitive:
            self.skipTest('file system is case-sensitive')
        target = self.root / 'target'
        target.mkdir()
        (folder / 'Link').symlink_to(target)
        (folder / 'café').symlink_to(target)
        names = [str(folder / 'link'), str(folder / 'LINK'), str(folder / 'café'), str(folder / 'CAFÉ')]
        self.assertEqual(close_saves.resolve_folders(names), {name: target.resolve() for name in names})
        self.assertEqual(close_saves.folders_inside(names, [target.resolve()]), names)

    def sibling_problems(self, task):
        return [row['detail'] for row in self.store.db.execute(
            "SELECT detail FROM problems WHERE code='sibling-git-kept' AND task=?", (task['id'],))]

    def test_sibling_clone_with_job_worktree_keeps_clone_and_saves_worktree(self):
        task = self.job()
        clone = self.state / 'scratch' / ('task-%s-clone' % task['id'])
        self.git('clone', '-q', self.project, clone)
        tree = self.state / 'worktrees' / ('task-%s-wt' % task['id'])
        self.git('worktree', 'add', '-q', '--detach', tree, cwd=clone)
        (tree / 'README.md').write_text('STAGED-ONLY-VERSION\n')
        self.git('add', 'README.md', cwd=tree)
        (tree / 'README.md').write_text('working version\n')
        closed = self.close(task)
        self.assert_cleared(task, closed)
        self.assertTrue((clone / '.git').is_dir())
        self.assertEqual(self.git('status', '--porcelain', cwd=clone).stdout, '')
        self.assertFalse(tree.exists())
        self.assertNotIn(str(tree), self.git('worktree', 'list', '--porcelain', cwd=clone).stdout)
        self.assertEqual(len(self.sibling_problems(task)), 1)
        self.assertIn('scratch/task-%s-clone has Git worktrees' % task['id'], self.sibling_problems(task)[0])
        saves = close_saves.load(self.store, task)[1]['scratch_git']
        self.assertEqual(len(saves), 1)
        self.assertIn('STAGED-ONLY-VERSION', (Path(saves[0]) / '0-index.patch').read_text())

    def test_sibling_clone_with_other_job_worktree_is_kept(self):
        task = self.job()
        other = self.call('tasks.create', title='Other job').data
        clone = self.state / 'scratch' / ('task-%s-clone' % task['id'])
        self.git('clone', '-q', self.project, clone)
        self.git('config', 'user.name', 'Test', cwd=clone)
        self.git('config', 'user.email', 'test@example.com', cwd=clone)
        tree = self.state / 'worktrees' / ('task-%s-x' % other['id'])
        self.git('worktree', 'add', '-q', '-b', 'other-work', tree, cwd=clone)
        commit = self.commit(tree, 'other job work\n')
        closed = self.close(task)
        self.assert_cleared(task, closed)
        self.assertTrue(clone.is_dir())
        self.assertEqual(self.git('rev-parse', 'HEAD', cwd=tree).stdout.strip(), commit)
        self.assertEqual(self.git('status', '--porcelain', cwd=tree).stdout, '')
        detail = self.sibling_problems(task)[0]
        self.assertIn('scratch/task-%s-clone has Git worktrees outside it: worktrees/task-%s-x' % (task['id'], other['id']), detail)
        self.assertFalse(detail.endswith('...'))

    def test_file_sibling_does_not_hold_close(self):
        task = self.job()
        files = [self.state / 'worktrees' / ('task-%s-prfiles.json' % task['id']),
                 self.state / 'scratch' / ('task-%s-squash.bundle' % task['id'])]
        for path in files:
            path.write_text('{}')
        closed = self.close(task)
        self.assert_cleared(task, closed)
        for path in files:
            self.assertEqual(path.read_text(), '{}')

    def test_close_and_clear_leave_records_in_place(self):
        import subprocess
        from coordinator import records
        task = self.job()
        records.write_chats(self.store)
        records.sync(self.store.directory)
        root = self.store.directory / 'records'
        tracked = subprocess.run(['git', '-C', str(root), 'ls-files'], check=True, capture_output=True, text=True).stdout.split()
        self.assertIn(str(records.path(self.store.directory, task).relative_to(root)), tracked)
        self.assert_cleared(task, self.close(task))
        self.assertEqual(records.sync(self.store.directory)['restored'], [])
        self.assertTrue(all((root / name).exists() for name in tracked))

    def test_large_untracked_files_are_cleared_and_small_ones_saved(self):
        task = self.job()
        target = self.target(task)
        (target / 'notes.txt').write_text('small work\n')
        large = target / 'scratch' / 'large.bin'
        large.parent.mkdir()
        with large.open('wb') as handle:
            handle.truncate(300 * 1024 * 1024)
        closed = self.close(task)
        self.assert_cleared(task, closed)
        self.assertEqual(self.wip_files(task), {'.gitignore', 'README.md', 'nested/keep.txt', 'notes.txt'})
        saved = self.saved(task)
        self.assertEqual(saved['excluded'], ['scratch'])
        self.assertTrue(saved['loose'].startswith('untracked files exceed 200 MB'))

    def test_large_ignored_and_secret_files_are_cleared(self):
        task = self.job()
        target = self.target(task)
        with (self.project / '.git' / 'info' / 'exclude').open('a') as handle:
            handle.write('cache.bin\n')
        (target / 'cache.bin').write_bytes(b'x' * 100)
        (target / '.env').write_text('TOKEN=1\n')
        with patch.object(close_saves, 'IGNORED_LIMIT', 10):
            closed = self.close(task)
        self.assert_cleared(task, closed)
        saved = self.saved(task)
        self.assertEqual(saved['ignored']['copied'], [])
        self.assertEqual({entry['name'] for entry in saved['ignored']['discarded']}, {'.env', 'cache.bin'})

    def test_intent_to_add_entry_is_saved_and_cleared(self):
        task = self.job()
        target = self.target(task)
        (target / 'ita.txt').write_text('intent to add content\n')
        self.git('add', '-N', 'ita.txt', cwd=target)
        closed = self.close(task)
        self.assert_cleared(task, closed)
        ref = 'refs/torii/saved/task-%s/%s/wip' % (task['id'], task['iteration'])
        self.assertEqual(self.git('show', ref + ':ita.txt').stdout, 'intent to add content\n')

    def test_locked_worktree_is_cleared(self):
        task = self.job()
        self.git('worktree', 'lock', self.target(task))
        self.assert_cleared(task, self.close(task))

    def test_detached_head_without_job_branch_is_saved_and_cleared(self):
        task = self.job()
        target = self.target(task)
        self.git('switch', '--detach', cwd=target)
        lost = self.commit(target, 'detached only\n')
        self.git('branch', '-D', 'torii/task-%s' % task['id'])
        closed = self.close(task)
        self.assert_cleared(task, closed)
        head_ref = 'refs/torii/saved/task-%s/%s/head' % (task['id'], task['iteration'])
        self.assertEqual(self.git('rev-parse', head_ref).stdout.strip(), lost)
        self.assertEqual(self.git('rev-parse', 'torii/task-%s' % task['id']).stdout.strip(), lost)

    def test_loose_save_restores_on_reopen(self):
        task = self.job()
        target = self.target(task)
        self.git('switch', '--detach', cwd=target)
        self.commit(target, 'detached only\n')
        (target / 'dirty.txt').write_text('dirty\n')
        self.git('branch', '-D', 'torii/task-%s' % task['id'])
        self.assert_cleared(task, self.close(task))
        self.call('tasks.update', task=task['id'], status='open')
        with self.store.db:
            self.store.db.execute("INSERT INTO task_iterations(task,k,opened,opened_by) VALUES (?,?,?,'owner')",
                                  (task['id'], task['iteration'] + 1, time.time()))
            self.store.db.execute('UPDATE tasks SET iteration=? WHERE id=?', (task['iteration'] + 1, task['id']))
        self.call('worktree.create', task=task['id'])
        self.assertEqual((target / 'README.md').read_text(), 'detached only\n')
        self.assertEqual((target / 'dirty.txt').read_text(), 'dirty\n')

    def test_tracked_edits_beside_large_untracked_files_are_saved(self):
        task = self.job()
        target = self.target(task)
        (target / 'nested' / 'keep.txt').write_text('edited\n')
        large = target / 'nested' / 'large.bin'
        with large.open('wb') as handle:
            handle.truncate(300 * 1024 * 1024)
        (target / 'nested' / 'small.txt').write_text('small\n')
        closed = self.close(task)
        self.assert_cleared(task, closed)
        ref = 'refs/torii/saved/task-%s/%s/wip' % (task['id'], task['iteration'])
        self.assertEqual(self.git('show', ref + ':nested/keep.txt').stdout, 'edited\n')
        self.assertNotIn('nested/large.bin', self.wip_files(task))
        self.assertEqual(self.saved(task)['excluded'], ['nested/large.bin'])
        self.assertIn('nested/small.txt', self.wip_files(task))

    def test_other_branch_is_saved_and_cleared(self):
        task = self.job()
        target = self.target(task)
        self.git('switch', '-q', '-c', 'side', cwd=target)
        tip = self.commit(target, 'side branch\n')
        closed = self.close(task)
        self.assert_cleared(task, closed)
        self.assertEqual(self.saved(task)['head'], tip)
        self.assertEqual(self.git('rev-parse', 'side').stdout.strip(), tip)

    def test_missing_registration_is_held(self):
        task = self.job()
        git_dir = fixtures.WorktreeCleanupTests.git_directory(self, self.target(task))
        git_dir.rename(self.root / 'moved-registration')
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'held', closed['copy_reason'])
        self.assertIn('Git cannot read', closed['copy_reason'])
        self.assertTrue((self.target(task) / '.git').is_file())
        self.assertTrue((self.root / 'moved-registration').is_dir())

    def test_copy_without_git_entry_is_cleared(self):
        task = self.job()
        (self.target(task) / '.git').unlink()
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'cleared', closed['copy_reason'])
        self.assertFalse(self.target(task).exists())
        self.assertNotIn('head', self.saved(task))

    def test_initialized_submodule_is_cleared(self):
        task, _ = self.submodule_job(closed=False)
        task = self.store.task_get(task['id'])
        self.assert_cleared(task, self.close(task))

    def test_file_exists_error_does_not_block(self):
        task = self.job()
        original = close_saves.snapshot
        calls = []

        async def snapshot(*args, **options):
            calls.append(options.get('loose', False))
            if len(calls) == 1:
                raise FileExistsError(17, 'File exists', str(self.state / 'results'))
            return await original(*args, **options)

        with patch.object(close_saves, 'snapshot', side_effect=snapshot):
            closed = self.close(task)
        self.assert_cleared(task, closed)
        self.assertEqual(calls, [False, True])

    def test_dropped_job_is_cleared_and_waiting_job_keeps_hold(self):
        dropped = self.job()
        self.git('worktree', 'lock', self.target(dropped))
        self.assert_cleared(dropped, self.close(dropped, status='dropped'))
        task = self.job()
        self.git('worktree', 'lock', self.target(task))
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET status='interrupted' WHERE id=?", (task['id'],))
        self.drain()
        current = self.store.task_get(task['id'])
        self.assertEqual((current['copy'], current['copy_reason']), ('held', 'worktree is locked or its registration is missing'))
        self.assertTrue(self.target(task).is_dir())

    def test_running_worker_process_holds_without_notice(self):
        task = self.job()
        other = self.call('tasks.create', title='Other job').data
        self.worker(other['id'], self.target(task), status='running')
        with patch('coordinator.worker_storage._processes_gone', return_value=False):
            closed = self.close(task)
            self.assertEqual(closed['copy'], 'held')
            self.assertIn(closed['copy_reason'], close_saves.RUNNING)
            self.assertTrue(self.target(task).is_dir())
            since = self.store.get('close_save_holds')[str(task['id'])]['since']
            asyncio.run(close_saves.retry_holds(self.store, now=since + 2 * close_saves.NOTICE_SECONDS))
        self.assertEqual(self.notices(), [])

    def test_finished_worker_leftover_is_stopped_then_cleared(self):
        task = self.job()
        worker = self.worker(task['id'], self.target(task), status='done')
        host = self.state / 'hosts' / ('worker-' + str(worker))
        host.mkdir(parents=True)
        (host / 'spec.json').write_text(json.dumps({'ownership': True}))
        (host / 'owner.json').write_text(json.dumps({'cleanup': 'failed'}))
        stopped = []

        def cleanup(directory, exclude=None):
            stopped.append(directory)
            (directory / 'owner.json').write_text(json.dumps({'cleanup': 'ok'}))
            return {'status': 'ok', 'remaining': 0, 'reason': ''}

        with patch.object(process_ownership, 'cleanup', side_effect=cleanup):
            closed = self.close(task)
        self.assertEqual(stopped, [host])
        self.assert_cleared(task, closed)

    def test_finished_worker_leftover_that_survives_holds(self):
        task = self.job()
        worker = self.worker(task['id'], self.target(task), status='done')
        host = self.state / 'hosts' / ('worker-' + str(worker))
        host.mkdir(parents=True)
        (host / 'spec.json').write_text(json.dumps({'ownership': True}))
        (host / 'owner.json').write_text(json.dumps({'cleanup': 'failed'}))
        failed = {'status': 'failed', 'remaining': 1, 'reason': 'still running'}
        with patch.object(process_ownership, 'cleanup', return_value=failed):
            closed = self.close(task)
        self.assertEqual((closed['copy'], closed['copy_reason']), ('held', 'finished worker processes still use job scratch'))
        self.assertTrue(self.target(task).is_dir())

    def test_save_failure_holds_retries_and_notifies_once(self):
        task = self.job()
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        (scratch / 'data.bin').write_bytes(b'x' * 4096)
        with patch.object(close_saves, 'snapshot', side_effect=ValueError('git add timed out after 30s')) as snapshot:
            closed = self.close(task)
            self.assertEqual((closed['copy'], closed['copy_reason']), ('held', 'git add timed out after 30s'))
            self.assertEqual(snapshot.call_count, 2)
            since = self.store.get('close_save_holds')[str(task['id'])]['since']
            asyncio.run(close_saves.retry_holds(self.store, now=since + 60))
            self.assertEqual(self.store.task_get(task['id'])['copy'], 'held')
            asyncio.run(close_saves.retry_holds(self.store, now=since + 3600))
            self.assertEqual(self.store.task_get(task['id'])['copy'], 'saving')
            self.drain()
            self.assertEqual(snapshot.call_count, 4)
            self.assertEqual(self.store.task_get(task['id'])['copy'], 'held')
            self.assertEqual(self.notices(), [])
            for hours in (25, 26):
                asyncio.run(close_saves.retry_holds(self.store, now=since + hours * 3600))
                self.drain()
            self.assertEqual(self.store.get('close_save_holds')[str(task['id'])]['since'], since)
        notices = self.notices()
        self.assertEqual(len(notices), 1)
        self.assertIn('Job %s in project' % task['number'], notices[0])
        self.assertIn('git add timed out after 30s', notices[0])
        self.assertRegex(notices[0], r'holds \d+\.\d\d KiB')
        self.assertTrue(scratch.exists())
        asyncio.run(close_saves.retry_holds(self.store, now=since + 27 * 3600))
        self.drain()
        self.assert_cleared(task, self.store.task_get(task['id']))
        self.assertFalse(scratch.exists())
        asyncio.run(close_saves.retry_holds(self.store, now=since + 28 * 3600))
        self.assertEqual(self.store.get('close_save_holds'), {})
        self.assertEqual(len(self.notices()), 1)

    def test_one_time_pass_clears_jobs_already_held(self):
        task = self.job()
        kept = self.job()
        self.git('worktree', 'lock', self.target(task))
        with self.store.db:
            self.store.put('close_saves_keep', [kept['id']])
        for job in (task, kept):
            self.call('tasks.update', task=job['id'], status='done', summary='Closed in test.')
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET copy='held',copy_reason='worktree is locked or its registration is missing'")
        self.drain(sweep=True)
        self.assert_cleared(task, self.store.task_get(task['id']))
        self.assertEqual(self.store.task_get(kept['id'])['copy'], 'held')
        self.assertTrue(self.target(kept).is_dir())

    def test_one_pass_clears_backlog_larger_than_a_batch_with_absent_folders(self):
        blocked = self.job()
        self.git('worktree', 'lock', self.target(blocked))
        with self.store.db:
            self.store.put('close_saves_keep', [blocked['id']])
        tasks = [self.job() for _ in range(close_saves.DRAIN_BATCH * 2 + 2)]
        for index, task in enumerate(tasks):
            if index % 2:
                shutil.rmtree(self.target(task))
            close_saves.scratch(self.store, task['id'], create=True)
            if index % 3 == 0:
                shutil.rmtree(close_saves.scratch(self.store, task['id']))
        self.call('tasks.update', task=blocked['id'], status='done', summary='Closed in test.')
        for index, task in enumerate(tasks):
            self.call('tasks.update', task=task['id'], status='dropped' if index % 4 == 0 else 'done', summary='Closed in test.', **({'note': 'No longer needed'} if index % 4 == 0 else {}))
        asyncio.run(workspaces.cleanup_closed_copies(self.store, sweep=True))
        self.assertEqual(self.store.task_get(blocked['id'])['copy'], 'held')
        for task in tasks:
            current = self.store.task_get(task['id'])
            with self.subTest(task=task['id']):
                self.assertEqual(current['copy'], 'cleared', current['copy_reason'])
                self.assertFalse(self.target(task).exists())
                self.assertFalse(close_saves.scratch(self.store, task['id']).exists())
        asyncio.run(workspaces.cleanup_closed_copies(self.store, sweep=True))
        self.assertTrue(all(self.store.task_get(task['id'])['copy'] == 'cleared' for task in tasks))


    def test_writes_in_shared_temp_folder_do_not_stall_the_clear(self):
        task = self.job()
        with self.store.db:
            self.store.db.execute('''INSERT INTO workers(id,task,topic,provider,prompt,cwd,status,created,updated)
                VALUES (?,?,?,'codex','fixture work',?,'done',?,?)''',
                (900000 + uuid.uuid4().int % 90000, task['id'], task['topic'], str(self.target(task)), self.now, self.now))
        temp = close_saves.roots(self.store, task)[2]
        self.assertFalse(temp.exists())
        original = close_saves.prepare_preconditions
        siblings = []

        async def busy_temp_folder(store, job):
            result = await original(store, job)
            sibling = temp.parent / ('torii-sibling-' + uuid.uuid4().hex)
            sibling.mkdir()
            siblings.append(sibling)
            return result

        try:
            with patch.object(close_saves, 'prepare_preconditions', new=busy_temp_folder):
                closed = self.close(task)
        finally:
            for sibling in siblings:
                sibling.rmdir()
        self.assertTrue(siblings)
        self.assert_cleared(task, closed)

    def test_deferred_clear_leaves_a_log_line(self):
        task = self.job()
        original = close_saves.preconditions_current

        def changed_during_clear(*args):
            if 'clear_extras' in [frame.function for frame in inspect.stack()]:
                return False
            return original(*args)

        with patch.object(close_saves, 'preconditions_current', side_effect=changed_during_clear), \
                self.assertLogs('coordinator.workspaces', 'INFO') as logs:
            closed = self.close(task)
        self.assertEqual(closed['copy'], 'saving')
        self.assertTrue(any('close save clear deferred job=%s' % task['id'] in line for line in logs.output), logs.output)
        self.assert_cleared(task, self.close(task))

    def assert_kept(self, task, reason):
        current = self.store.task_get(task['id'])
        self.assertEqual((current['copy'], current['copy_reason']), ('held', reason))
        self.assertTrue(self.target(task).is_dir())
        self.assertEqual([stage for stage, _ in workspaces._deletion_stages(self.store.directory) if (stage / 'copy').exists()], [])
        return current

    def test_job_reopened_during_strict_save_keeps_its_copy(self):
        """Review repro ReviewRace.test_waiting_job_copy_cleared_when_status_changes_during_strict_save."""
        task = self.job()
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        (scratch / 'data.bin').write_bytes(b'x')

        def reopen(*args, **options):
            with self.store.db:
                self.store.db.execute("UPDATE tasks SET status='waiting' WHERE id=?", (task['id'],))
            raise ValueError('git add timed out after 30s')

        with patch.object(close_saves, 'snapshot', side_effect=reopen) as snapshot:
            current = self.close(task)
        self.assertEqual(snapshot.call_count, 1)
        self.assertEqual(current['status'], 'waiting')
        self.assert_kept(task, 'git add timed out after 30s')
        self.assertTrue(scratch.is_dir())

    def test_job_reopened_during_loose_save_keeps_its_copy(self):
        task = self.job()
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        (scratch / 'data.bin').write_bytes(b'x')
        original = close_saves.snapshot

        async def snapshot(*args, **options):
            if not options.get('loose'):
                raise ValueError('git add timed out after 30s')
            with self.store.db:
                self.store.db.execute("UPDATE tasks SET status='waiting' WHERE id=?", (task['id'],))
            return await original(*args, **options)

        with patch.object(close_saves, 'snapshot', side_effect=snapshot):
            self.close(task)
        self.assert_kept(task, 'git add timed out after 30s')
        self.assertTrue(scratch.is_dir())

    def test_done_job_after_project_folder_move_is_saved_in_its_own_repository(self):
        """Review repro ReviewMoved.test_done_job_after_project_folder_move_is_cleared_without_git_save."""
        task = self.job()
        target = self.target(task)
        tip = self.commit(target, 'job work\n')
        (target / 'notes.txt').write_text('unsaved notes\n')
        moved = self.root / 'moved-project'
        self.git('clone', '-q', self.project, moved)
        with self.store.db:
            self.store.db.execute('UPDATE topics SET cwd=?', (str(moved),))
        closed = self.close(task)
        self.assert_cleared(task, closed)
        saved = self.saved(task)
        self.assertEqual(saved['head'], tip)
        self.assertEqual(saved['repository'], str(self.project / '.git'))
        self.assertEqual(self.git('rev-parse', saved['head_ref']).stdout.strip(), tip)
        self.assertIn('notes.txt', self.wip_files(task))
        bundle = self.state / 'results' / ('task-' + str(task['id'])) / 'rounds' / str(task['iteration']) / 'branch.bundle'
        self.assertEqual(saved['bundle']['path'], 'branch.bundle')
        self.git('bundle', 'verify', bundle)

    def test_done_job_after_real_project_folder_rename_is_held(self):
        task = self.job()
        target = self.target(task)
        self.commit(target, 'job work\n')
        (target / 'notes.txt').write_text('unsaved notes\n')
        moved = self.root / 'moved-project'
        self.project.rename(moved)
        self.project = moved
        with self.store.db:
            self.store.db.execute('UPDATE topics SET cwd=?', (str(moved),))
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'held', closed['copy_reason'])
        self.assertIn('Git cannot read', closed['copy_reason'])
        self.assertEqual((target / 'notes.txt').read_text(), 'unsaved notes\n')
        self.assertEqual([stage for stage, _ in workspaces._deletion_stages(self.store.directory) if (stage / 'copy').exists()], [])
        self.drain(sweep=True)
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'held')
        self.assertTrue((target / 'notes.txt').is_file())

    def test_kept_job_is_not_loose_saved(self):
        task = self.job()
        self.git('worktree', 'lock', self.target(task))
        with self.store.db:
            self.store.put('close_saves_keep', [task['id']])
        with patch.object(close_saves, 'loose_save') as loose:
            self.close(task)
        loose.assert_not_called()
        self.assert_kept(task, 'worktree is locked or its registration is missing')
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET copy='saving' WHERE id=?", (task['id'],))
        self.assertFalse(asyncio.run(close_saves.loose_save(self.store, task, ValueError('strict failed'))))
        self.assertTrue(self.target(task).is_dir())

    def test_waiting_worker_holds_and_is_not_stopped(self):
        for status in ('waiting_for_secret', 'waiting_for_quota'):
            with self.subTest(status=status):
                task = self.job()
                scratch = close_saves.scratch(self.store, task['id'], create=True)
                self.worker(task['id'], scratch, status=status)
                with patch.object(process_ownership, 'cleanup') as cleanup:
                    for gone in (True, False):
                        with self.store.db:
                            self.store.db.execute("UPDATE tasks SET status='done' WHERE id=?", (task['id'],))
                        with patch('coordinator.worker_storage._processes_gone', return_value=gone):
                            self.drain()
                        self.assert_kept(task, 'job has an unfinished worker')
                        self.assertTrue(scratch.is_dir())
                    with self.assertRaisesRegex(ValueError, close_saves.RUNNING[1]), \
                            patch('coordinator.worker_storage._processes_gone', return_value=False):
                        asyncio.run(close_saves.stopped_workers(self.store, task, reap=True))
                cleanup.assert_not_called()

    def test_loose_save_is_verified_before_checkpoint(self):
        task = self.job()
        self.git('worktree', 'lock', self.target(task))
        with patch.object(close_saves, 'verify', side_effect=ValueError('saved branch changed')):
            self.close(task)
        self.assert_kept(task, 'saved branch changed')
        self.assertNotEqual(close_saves.load(self.store, task)[0], 'verified')

if __name__ == '__main__':
    unittest.main()


class SharedLockTurnTests(unittest.IsolatedAsyncioTestCase):
    async def test_close_save_drain_lets_a_waiting_cleanup_take_the_lock(self):
        import tempfile
        from unittest.mock import AsyncMock
        from coordinator.store import Store
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        store = Store(Path(temporary.name))
        self.addCleanup(store.close)
        order, waiter = [], []

        async def wait_turn():
            async with workspaces._repository_lock(store.directory, 'closed-copy-deletion'):
                order.append('worker temp')

        async def drain(store):
            if not waiter:
                waiter.append(asyncio.create_task(wait_turn()))
            await asyncio.sleep(0.1)
            order.append('drain')
            return order.count('drain') < 3

        with patch.object(close_saves, 'drain', side_effect=drain), \
             patch.object(workspaces, '_recover_finalization_stages', new=AsyncMock(return_value=True)), \
             patch.object(workspaces, 'async_deletion_stages', new=AsyncMock(return_value=[])):
            await workspaces.cleanup_closed_copies(store)
        await waiter[0]
        self.assertEqual(order[:2], ['drain', 'worker temp'])
