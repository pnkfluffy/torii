import asyncio
import hashlib
import inspect
import subprocess
import json
from pathlib import Path
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import close_saves, control_api, workspaces
from coordinator.service import Service
from tests import test_worktree_cleanup as fixtures


class CloseSavesTests(unittest.TestCase):
    setUp = fixtures.WorktreeCleanupTests.setUp
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

    def enable(self):
        with self.store.db:
            self.store.put('close_clears_space', True)

    def closed_then_reopened(self):
        task = self.job()
        self.call('tasks.update', task=task['id'], status='done')
        self.call('tasks.update', task=task['id'], status='open')
        current = self.store.task_get(task['id'])
        self.assertEqual((current['status'], current['copy']), ('open', 'saving'))
        return current

    def drain(self):
        asyncio.run(workspaces.cleanup_closed_copies(self.store))

    def close(self, task):
        self.call('tasks.update', task=task['id'], status='done')
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

    def test_pr3_any_branch_off_keeps_old_hold(self):
        self.enable()
        task, branch, head, expected = self.slug_job()
        for explicit in (False, True):
            if explicit:
                self.call('cleanup.close_rules', any_branch=False)
            closed = self.close(task)
            self.assertEqual((closed['copy'], closed['copy_reason']),
                             ('held', 'worktree is not on the saved job branch'))
            self.assertEqual(self.git('rev-parse', branch).stdout.strip(), head)
            self.assertEqual((self.target(task) / 'README.md').read_bytes(), expected['README.md'])

    def test_reflog_only_commit_slug_on(self):
        self.enable()
        self.call('cleanup.close_rules', any_branch=True, requeue=False)
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
        self.enable()
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
        self.enable()
        self.call('cleanup.close_rules', any_branch=True, requeue=False)
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
        self.enable()
        self.call('cleanup.close_rules', any_branch=True, requeue=False)
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
        stage, record = next(workspaces._deletion_stages(self.state))
        record['retry_at'] = time.time() - 1
        workspaces._save_deletion(stage, record)
        self.assertLessEqual(self.loop_passes(), 3)

    def test_scope_shared_branch_reflog_and_stash(self):
        self.enable()
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
        self.enable()
        task = self.job()
        target = self.target(task)
        lost = [self.commit(target, 'reflog-only %d\n' % n) for n in range(6)]
        self.git('reset', '--hard', self.base, cwd=target)
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'cleared', closed['copy_reason'])
        self.assertEqual(list(self.manifest(task)['reflog_refs'].values()), [lost[-1]])
        self.assert_reflog_saved(task, lost[0])

    def test_reflog_old_oid_is_saved(self):
        self.enable()
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
        self.enable()
        self.call('cleanup.close_rules', submodules=True, requeue=False)
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
        self.enable()
        self.call('cleanup.close_rules', any_branch=True, requeue=False)
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
        self.enable()
        self.call('cleanup.close_rules', any_branch=True, requeue=False)
        task, _, _, _ = self.slug_job()
        target = self.target(task)
        (target / 'ita.txt').write_text('intent to add content\n')
        self.git('add', '-N', 'ita.txt', cwd=target)
        closed = self.close(task)
        self.assertEqual((closed['copy'], closed['copy_reason']),
                         ('held', 'index has an intent-to-add entry: ita.txt'))
        self.assertEqual((target / 'ita.txt').read_text(), 'intent to add content\n')

    def test_worktree_ref_reflog_only_commit_is_saved(self):
        self.enable()
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
        self.enable()
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
        self.enable()
        task = self.job()
        target = self.target(task)
        self.git('switch', '-c', 'side', cwd=target)
        self.commit(target, 'side only\n')
        self.git('switch', 'torii/task-%s' % task['id'], cwd=target)
        self.git('branch', '-D', 'side', cwd=target)
        self.call('tasks.update', task=task['id'], status='done')
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
        self.enable()
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

    def test_pr3_slug_branch_bundle_wip_and_reopen(self):
        self.enable()
        self.call('cleanup.close_rules', any_branch=True, requeue=False)
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
        self.reopen(task)
        self.assertEqual(self.git('symbolic-ref', '--short', 'HEAD', cwd=target).stdout.strip(), branch)
        self.assertEqual({str(p.relative_to(target)): p.read_bytes()
                          for p in target.rglob('*') if p.is_file() and p.name != '.git'}, expected)
        self.call('worktree.create', task=task['id'])
        self.assertEqual((target / 'README.md').read_bytes(), expected['README.md'])

    def test_pr3_deleted_slug_still_verifies_and_reopen_pauses_with_saved_path(self):
        self.enable()
        self.call('cleanup.close_rules', any_branch=True, requeue=False)
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
        self.enable()
        self.call('cleanup.close_rules', any_branch=True, requeue=False)
        task, branch, _, expected = self.slug_job()
        self.git('branch', '-D', 'torii/task-' + str(task['id']))
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'cleared', closed['copy_reason'])
        self.assert_cleared(task)
        self.reopen(task)
        self.assertEqual(self.git('symbolic-ref', '--short', 'HEAD', cwd=self.target(task)).stdout.strip(), branch)
        self.assertEqual((self.target(task) / 'README.md').read_bytes(), expected['README.md'])

    def test_pr3_keep_blocks_any_branch_requeue_and_retry(self):
        self.enable()
        task, _, _, expected = self.slug_job()
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'held')
        self.call('cleanup.close_rules', keep=str(task['id']), any_branch=True)
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'held')
        result = control_api.call(self.store, 'cleanup.retry', {'task': task['id']})
        self.assertEqual(result.state, 'refused')
        self.drain()
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'held')
        self.assertEqual((self.target(task) / 'README.md').read_bytes(), expected['README.md'])

    def test_pr3_rule_off_after_save_pauses_then_reenable_restores(self):
        self.enable()
        self.call('cleanup.close_rules', any_branch=True, requeue=False)
        task, branch, _, expected = self.slug_job()
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'cleared', closed['copy_reason'])
        self.call('cleanup.close_rules', any_branch=False)
        self.call('tasks.update', task=task['id'], status='open')
        result = control_api.call(self.store, 'worktree.create', {'task': task['id']}, topic=task['topic'])
        self.assertFalse(result.ok)
        self.assertIn('Rebuild paused', result.text)
        self.assertIn(str(self.folder(task)), result.text)
        self.call('cleanup.close_rules', any_branch=True, requeue=False)
        self.call('worktree.create', task=task['id'])
        self.assertEqual(self.git('symbolic-ref', '--short', 'HEAD', cwd=self.target(task)).stdout.strip(), branch)
        self.assertEqual((self.target(task) / 'README.md').read_bytes(), expected['README.md'])

    def test_pr3_index_only_bytes_hold_with_copy_intact(self):
        self.enable()
        self.call('cleanup.close_rules', any_branch=True, requeue=False)
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
        self.enable()
        self.call('cleanup.close_rules', any_branch=True, requeue=False)
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
        self.enable()
        self.call('cleanup.close_rules', any_branch=True, requeue=False)
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
        self.enable()
        self.call('cleanup.close_rules', any_branch=True, requeue=False)
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
        self.enable()
        self.call('cleanup.close_rules', any_branch=True, requeue=False)
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
        self.enable()
        self.call('cleanup.close_rules', any_branch=True, requeue=False)
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
        self.enable()
        self.call('cleanup.close_rules', any_branch=True, requeue=False)
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

    def test_pr3_rule_off_after_staging_keeps_copy_and_saved_path(self):
        self.enable()
        self.call('cleanup.close_rules', any_branch=True, requeue=False)
        task, _, _, expected = self.slug_job()
        original = close_saves.stage

        async def disable_after_stage(*args):
            await original(*args)
            with self.store.db:
                self.store.put('close_saves_any_branch', False)

        with patch.object(close_saves, 'stage', side_effect=disable_after_stage):
            self.close(task)
        stage, record = next(workspaces._deletion_stages(self.state))
        self.assertEqual((stage / 'copy' / 'README.md').read_bytes(), expected['README.md'])
        self.assertIn(str(self.folder(task)), record['error'])
        self.assertNotEqual(self.store.task_get(task['id'])['copy'], 'cleared')
        with self.store.db:
            self.store.put('close_saves_any_branch', True)
        record['retry_at'] = 0
        workspaces._save_deletion(stage, record)
        self.drain()
        self.assert_cleared(task)

    def test_pr3_slug_with_uninitialized_submodule_reopens(self):
        self.enable()
        self.call('cleanup.close_rules', any_branch=True, submodules=True, requeue=False)
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



    def test_pr2_submodules_default_off_keeps_metadata_hold(self):
        self.enable()
        task, _ = self.submodule_job(initialized=False, closed=False)
        closed = self.close(task)
        self.assertEqual((closed['copy'], closed['copy_reason']), ('held', 'worktree uses submodules'))
        self.assertTrue(self.target(task).is_dir())

    def test_pr2_submodules_off_keeps_gitlink_hold_without_gitmodules(self):
        self.enable()
        task, _ = self.submodule_job(initialized=False, closed=False)
        (self.target(task) / '.gitmodules').unlink()
        closed = self.close(task)
        self.assertEqual((closed['copy'], closed['copy_reason']), ('held', 'worktree uses submodules'))
        self.assertTrue(self.target(task).is_dir())

    def test_pr2_submodules_off_keeps_modules_directory_hold(self):
        self.enable()
        task = self.job()
        (self.git_directory(self.target(task)) / 'modules').mkdir()
        closed = self.close(task)
        self.assertEqual((closed['copy'], closed['copy_reason']), ('held', 'worktree uses submodules'))
        self.assertTrue(self.target(task).is_dir())

    def test_pr2_clean_uninitialized_submodule_saves_head_tree_and_reopens(self):
        self.enable()
        self.call('cleanup.close_rules', submodules=True, requeue=False)
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
        self.enable()
        self.call('cleanup.close_rules', submodules=True, requeue=False)
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
        self.enable()
        self.call('cleanup.close_rules', submodules=True, requeue=False)
        task, tip = self.submodule_job(closed=False)
        target = self.target(task)
        content = (target / 'sub' / 'library.txt').read_bytes()
        closed = self.close(task)
        self.assertEqual((closed['copy'], closed['copy_reason']), ('held', 'worktree has initialized submodules'))
        self.assertEqual(self.git('rev-parse', 'HEAD', cwd=target / 'sub').stdout.strip(), tip)
        self.assertEqual((target / 'sub' / 'library.txt').read_bytes(), content)

    def test_pr2_nonempty_uninitialized_submodule_keeps_unsaved_files(self):
        self.enable()
        self.call('cleanup.close_rules', submodules=True, requeue=False)
        task, _ = self.submodule_job(initialized=False, closed=False)
        pending = self.target(task) / 'sub' / 'pending.bin'
        pending.write_bytes(b'unsaved\x00bytes')
        closed = self.close(task)
        self.assertEqual((closed['copy'], closed['copy_reason']),
                         ('held', 'Uninitialized submodule folder contains unsaved files'))
        self.assertEqual(pending.read_bytes(), b'unsaved\x00bytes')

    def test_pr2_orphan_submodule_git_data_stays_held(self):
        self.enable()
        self.call('cleanup.close_rules', submodules=True, requeue=False)
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
        self.enable()
        self.call('cleanup.close_rules', submodules=True, requeue=False)
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
        self.enable()
        self.call('cleanup.close_rules', submodules=True, requeue=False)
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
        self.enable()
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
        self.enable()
        stuck = [self.closed_then_reopened() for _ in range(5)]
        waiting = self.job()
        self.call('tasks.update', task=waiting['id'], status='done')
        results = [asyncio.run(workspaces.cleanup_closed_copies(self.store)) for _ in range(3)]
        print('cleanup results', results)
        print('stuck copies', [self.store.task_get(t['id'])['copy'] for t in stuck])
        self.assertEqual(self.store.task_get(waiting['id'])['copy'], 'cleared',
                         'closed job behind five reopened saving rows must still be saved')

    def test_reopened_saving_row_makes_cleanup_loop_spin(self):
        self.enable()
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
        self.enable()
        task = self.job()
        self.call('tasks.update', task=task['id'], status='done')
        with patch.object(close_saves, 'save_job', new=AsyncMock()) as save:
            self.assertFalse(asyncio.run(close_saves.drain(self.store)))
            self.assertTrue(close_saves.pending(self.store))
            self.assertFalse(asyncio.run(workspaces.cleanup_closed_copies(self.store)))
        self.assertEqual(save.await_count, 2)
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'saving')

    def test_review3023_close_rules_do_not_requeue_when_disabled(self):
        tasks = [self.job() for _ in close_saves.CLOSED]
        for task, status in zip(tasks, close_saves.CLOSED):
            with self.store.db:
                self.store.db.execute('UPDATE tasks SET status=? WHERE id=?', (status, task['id']))
                self.store.db.execute("UPDATE tasks SET copy='held',copy_reason='worktree uses submodules' WHERE id=?", (task['id'],))
        for explicit in (False, True):
            with self.subTest(explicit_requeue=explicit):
                params = {'submodules': True}
                if explicit:
                    params['requeue'] = True
                result = self.call('cleanup.close_rules', **params)
                self.assertEqual(result.data['requeued'], 0)
                self.assertTrue(self.store.get('close_saves_submodules'))
                self.assertIn('cleanup.close_space', result.text)
                self.assertTrue(all(self.store.task_get(task['id'])['copy'] == 'held' for task in tasks))
        self.enable()
        result = self.call('cleanup.close_rules', submodules=True, requeue=True)
        self.assertEqual(result.data['requeued'], len(tasks))
        self.assertTrue(all(self.store.task_get(task['id'])['copy'] == 'saving' for task in tasks))

    def test_pr1_git_timeout_has_command_and_duration(self):
        self.enable()
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
        self.enable()
        task = self.job()
        with patch.object(close_saves, 'snapshot', side_effect=ValueError()):
            closed = self.close(task)
        self.assertEqual(closed['copy_reason'], 'ValueError')

    def test_pr1_untracked_cap_holds_before_wip_objects(self):
        self.enable()
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
        self.enable()
        task = self.job()
        target = self.target(task)
        (target / 'largest').mkdir()
        (target / 'largest' / 'one').write_bytes(b'a' * 30)
        (target / 'largest' / 'two').write_bytes(b'b' * 30)
        (target / 'smaller').write_bytes(b'c' * 20)
        with patch.object(close_saves, 'UNTRACKED_LIMIT', 70, create=True):
            closed = self.close(task)
        self.assertIn('largest (80 total)', closed['copy_reason'] or '')
        outside = self.root / 'large-outside'
        with outside.open('wb') as handle:
            handle.truncate(300 * 1024 * 1024)
        (target / 'link').symlink_to(outside)
        with patch.object(close_saves, 'UNTRACKED_LIMIT', 80, create=True):
            self.call('tasks.update', task=task['id'], note='retry with exact limit')
            self.drain()
        self.assert_cleared(task)
        self.assertEqual(outside.stat().st_size, 300 * 1024 * 1024)

    def test_pr1_hold_problem_is_once_per_task_and_new_reason(self):
        self.enable()
        task = self.job()
        self.call('tasks.update', task=task['id'], status='done')
        for reason in ('first reason', 'first reason', 'second reason', 'second reason', 'first reason'):
            with self.store.db:
                close_saves.request(self.store, task['id'])
            with patch.object(close_saves, 'snapshot', side_effect=ValueError(reason)):
                self.drain()
        rows = self.store.db.execute("SELECT detail,topic,task FROM problems WHERE area='cleanup' AND code='copy-held' ORDER BY id").fetchall()
        self.assertEqual([tuple(row) for row in rows],
                         [('first reason', task['topic'], task['id']), ('second reason', task['topic'], task['id'])])

    def test_pr1_finish_reports_nonblank_reason_once(self):
        self.enable()
        task = self.job()
        self.call('tasks.update', task=task['id'], status='done')
        for failure in (RuntimeError(), RuntimeError(), asyncio.TimeoutError()):
            with self.store.db:
                self.store.db.execute("UPDATE tasks SET copy='saving' WHERE id=?", (task['id'],))
            with patch.object(close_saves, 'clear_extras', side_effect=failure):
                asyncio.run(close_saves.finish(self.store))
            self.assertTrue(self.store.task_get(task['id'])['copy_reason'])
        rows = self.store.db.execute("SELECT detail FROM problems WHERE area='cleanup' AND code='copy-held' ORDER BY id").fetchall()
        self.assertEqual([row['detail'] for row in rows], ['RuntimeError', 'TimeoutError'])

    def test_pr1_seven_saves_release_lock_between_two_batches(self):
        self.enable()
        tasks = [self.job() for _ in range(7)]
        for task in tasks:
            self.call('tasks.update', task=task['id'], status='done')
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
        self.enable()
        tasks = [self.job() for _ in range(3)]
        for task in tasks:
            self.call('tasks.update', task=task['id'], status='done')

        async def hold(store, task_id):
            with store.db:
                store.db.execute("UPDATE tasks SET copy='held' WHERE id=?", (task_id,))

        with patch.object(close_saves, 'save_job', side_effect=hold) as save:
            self.assertTrue(asyncio.run(close_saves.drain(self.store, limit=2)))
            self.assertFalse(asyncio.run(close_saves.drain(self.store, limit=2)))
        self.assertEqual([call.args[1] for call in save.call_args_list], [task['id'] for task in tasks])

    def test_pr1_staged_retries_do_not_starve_batches_or_force_busy_loop(self):
        self.enable()
        tasks = [self.job() for _ in range(6)]
        for task in tasks:
            self.call('tasks.update', task=task['id'], status='done')
        with patch.object(workspaces, 'delete_tree', side_effect=OSError('fixture delayed deletion')), \
                patch('coordinator.workspaces.time.time', return_value=time.time()):
            self.assertTrue(asyncio.run(workspaces.cleanup_closed_copies(self.store)))
            self.assertEqual([close_saves.load(self.store, task)[0] for task in tasks], ['staged'] * 5 + ['pending'])
            self.assertFalse(asyncio.run(workspaces.cleanup_closed_copies(self.store)))
            self.assertEqual([close_saves.load(self.store, task)[0] for task in tasks], ['staged'] * 6)
            self.assertGreater(close_saves.deletion_delay(self.store), 0)

    def test_pr1_service_continues_backlog_without_waiting_for_wake(self):
        self.enable()
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
                patch('coordinator.service.asyncio.sleep', side_effect=sleep):
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(exercise())
        self.assertEqual(delays, [0])

    def test_pr1_rule_defaults_are_off_and_empty(self):
        data = self.call('settings.get').data
        for name in ('submodules', 'any_branch', 'prune_held'):
            self.assertIs(data['close_saves_' + name], False)
        for name in ('discard_names', 'keep'):
            self.assertEqual(data['close_saves_' + name], [])
        self.assertFalse(close_saves.kept(self.store, 1))

    def test_pr1_rules_and_retry_are_local_cli_only(self):
        for source in ('mcp', 'telegram', 'service', 'other'):
            for op, params in (('cleanup.close_rules', {'submodules': True}), ('cleanup.retry', {'task': 1})):
                with self.subTest(source=source, op=op):
                    result = control_api.call(self.store, op, params, source=source)
                    self.assertFalse(result.ok)
                    self.assertEqual(result.state, 'refused')
                    self.assertIn('local CLI', result.text)
        self.assertIsNone(self.store.get('close_saves_submodules'))

    def test_pr1_rules_requeue_only_matching_closed_holds(self):
        self.enable()
        tasks = []
        reasons = ('worktree uses submodules', 'worktree is not on the saved job branch', 'ignored files exceed 200 MB: data', 'unrelated')
        for status in (*close_saves.CLOSED, 'open'):
            for reason in reasons:
                task = self.job()
                with self.store.db:
                    self.store.db.execute('UPDATE tasks SET status=? WHERE id=?', (status, task['id']))
                    self.store.db.execute("UPDATE tasks SET copy='held',copy_reason=? WHERE id=?", (reason, task['id']))
                tasks.append((task, status, reason))
        self.call('cleanup.close_rules', submodules=True, requeue=False)
        self.assertTrue(all(self.store.task_get(task['id'])['copy'] == 'held' for task, _, _ in tasks))
        self.call('cleanup.close_rules', submodules=False)
        self.call('cleanup.close_rules', submodules=True)
        for task, status, reason in tasks:
            current = self.store.task_get(task['id'])
            match = status in close_saves.CLOSED and reason == reasons[0]
            self.assertEqual((current['copy'], current['copy_reason']), ('saving', None) if match else ('held', reason))
        self.call('cleanup.close_rules', any_branch=True)
        self.call('cleanup.close_rules', discard_names=' .outputs , .scratch ')
        self.assertEqual(self.store.get('close_saves_discard_names'), ['.outputs', '.scratch'])
        for task, status, reason in tasks:
            self.assertEqual(self.store.task_get(task['id'])['copy'],
                             'saving' if status in close_saves.CLOSED and reason in reasons[:3] else 'held')

    def test_pr1_retry_queues_one_closed_job_and_refuses_open_or_disabled(self):
        first, second = self.job(), self.job()
        result = control_api.call(self.store, 'cleanup.retry', {'task': first['id']})
        self.assertFalse(result.ok)
        self.assertIn('close_space', result.text)
        self.enable()
        self.assertFalse(control_api.call(self.store, 'cleanup.retry', {'task': first['id']}).ok)
        self.assertFalse(control_api.call(self.store, 'cleanup.retry', {'task': 999999}).ok)
        for task in (first, second):
            with self.store.db:
                self.store.db.execute("UPDATE tasks SET status='done' WHERE id=?", (task['id'],))
                self.store.db.execute("UPDATE tasks SET copy='held',copy_reason='fixture hold' WHERE id=?", (task['id'],))
        self.call('cleanup.retry', task=first['id'])
        self.assertEqual(self.store.task_get(first['id'])['copy'], 'saving')
        self.assertEqual(self.store.task_get(second['id'])['copy'], 'held')

    def test_pr1_canary_rule_can_explicitly_requeue_after_enable(self):
        self.enable()
        task = self.job()
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET status='done' WHERE id=?", (task['id'],))
            self.store.db.execute("UPDATE tasks SET copy='held',copy_reason='worktree uses submodules' WHERE id=?", (task['id'],))
        self.call('cleanup.close_rules', submodules=True, requeue=False)
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'held')
        self.call('cleanup.close_rules', submodules=True, requeue=True)
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'saving')

    def test_pr1_prune_held_switch_does_not_change_deletion_gates(self):
        self.enable()
        self.call('cleanup.close_rules', submodules=True, any_branch=False, prune_held=True)
        task = self.job()
        target = self.target(task)
        self.git('checkout', '-qb', 'worker-branch', cwd=target)
        (target / '.gitignore').write_text('node_modules/\n')
        (target / 'node_modules').mkdir()
        dependency = target / 'node_modules' / 'package'
        dependency.write_bytes(b'held dependency')
        closed = self.close(task)
        self.assertEqual(closed['copy_reason'], 'worktree is not on the saved job branch')
        self.assertEqual(dependency.read_bytes(), b'held dependency')

    def test_pr1_keep_list_blocks_rule_requeue_and_retry_then_can_be_cleared(self):
        self.enable()
        kept, retry = self.job(), self.job()
        for task in (kept, retry):
            with self.store.db:
                self.store.db.execute("UPDATE tasks SET status='done' WHERE id=?", (task['id'],))
                self.store.db.execute("UPDATE tasks SET copy='held',copy_reason='worktree uses submodules' WHERE id=?", (task['id'],))
        self.call('cleanup.close_rules', keep=' %d, %d, %d ' % (kept['id'], 999999, kept['id']), submodules=True)
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

    def test_pr1_rule_lists_validate_before_any_mutation(self):
        for params in ({'keep': '1,not-an-id'}, {'keep': '0'}, {'keep': '-3'},
                       {'discard_names': '../data'}, {'discard_names': '/data'}, {'discard_names': '.'}):
            with self.subTest(params=params):
                result = control_api.call(self.store, 'cleanup.close_rules', dict(params, any_branch=True))
                self.assertEqual(result.state, 'refused')
                self.assertIsNone(self.store.get('close_saves_any_branch'))

    def test_pr1_discard_names_apply_to_ignored_and_survive_verification(self):
        self.enable()
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
        self.enable()
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
        self.enable()
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

    def test_unpushed_commits_are_in_a_verified_bundle(self):
        self.enable()
        task = self.job()
        head = self.commit(self.target(task))
        self.close(task)
        self.assert_cleared(task)
        bundle = self.folder(task) / 'branch.bundle'
        self.assertEqual(self.git('bundle', 'verify', bundle).returncode, 0)
        self.assertIn(head, self.git('bundle', 'list-heads', bundle).stdout)
        self.assertEqual(self.manifest(task)['bundle']['sha256'], hashlib.sha256(bundle.read_bytes()).hexdigest())

    def test_git_conversion_holds_instead_of_losing_raw_file_bytes(self):
        self.enable()
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

    def test_regenerable_discarded_other_ignored_saved_and_env_held(self):
        self.enable()
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

    def test_rebase_in_progress_holds_the_copy(self):
        self.enable()
        task = self.job()
        (self.git_directory(self.target(task)) / 'rebase-merge').mkdir()
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'held')
        self.assertIn('rebase-merge', closed['copy_reason'])
        self.assertTrue(self.target(task).is_dir())

    def test_tracked_agent_named_folders_are_saved_and_cleared(self):
        self.enable()
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
        self.enable()
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
        self.enable()
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
        self.enable()
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
        self.enable()
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

                with patch.object(close_saves, 'digest', side_effect=digest):
                    self.close(task)
                self.assertTrue(reads)
                self.assert_cleared(task)
                self.assertEqual(self.manifest(task)['proofs'], [{'source': str(proof), 'missing': True}])

    def test_missing_worker_proof_is_retained_through_close(self):
        self.enable()
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
        self.enable()
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
        self.enable()
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
        self.enable()
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

                with patch.object(close_saves, 'copy_file', side_effect=copy_file):
                    closed = self.close(task)
                self.assertEqual(closed['copy'], 'held')
                self.assertIn('Errno 2' if failure == 'destination' else 'Permission denied', closed['copy_reason'])
                self.assertTrue(self.target(task).is_dir())

    def test_missing_symlink_proof_is_still_held(self):
        self.enable()
        task = self.job()
        proof = self.target(task) / 'link.png'
        proof.symlink_to(self.target(task) / 'absent.png')
        self.call('tasks.update', task=task['id'], note='proof=' + str(proof))
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'held')
        self.assertIn('symbolic link', closed['copy_reason'])
        self.assertTrue(proof.is_symlink())

    def test_reused_note_proof_path_is_saved_in_each_round(self):
        self.enable()
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
        self.enable()
        from coordinator import close_saves
        for phase in ('proofs', 'snapshot', 'ignored', 'manifest', 'verified', 'staged', 'extras', 'cleared'):
            with self.subTest(phase=phase):
                task = self.job()
                (self.target(task) / 'README.md').write_text('saved across ' + phase)
                original = close_saves.checkpoint
                crashed = []

                def checkpoint(store, job, current, data):
                    original(store, job, current, data)
                    if current == phase and not crashed:
                        crashed.append(True)
                        raise SystemExit('fixture crash')

                self.call('tasks.update', task=task['id'], status='done')
                with patch.object(close_saves, 'checkpoint', side_effect=checkpoint):
                    with self.assertRaises(SystemExit):
                        self.drain()
                self.store.close()
                self.store = fixtures.Store(self.state, token_file=self.root / 'no-token')
                self.drain()
                self.assert_cleared(task)
                self.reopen(task)
                self.assertEqual((self.target(task) / 'README.md').read_text(), 'saved across ' + phase)

    def test_late_writer_causes_one_resnapshot(self):
        self.enable()
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
        self.enable()
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

    def test_rebuild_seconds_recorded_on_current_round(self):
        self.enable()
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
        self.enable()
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
        self.enable()
        task = self.job()
        proof = self.target(task) / 'proof.txt'
        proof.write_text('original')
        self.call('tasks.update', task=task['id'], note='proof=' + str(proof))
        from coordinator import close_saves
        original = close_saves.checkpoint

        def checkpoint(store, job, phase, data):
            original(store, job, phase, data)
            if phase == 'manifest':
                entry = data['proofs'][0]
                (self.folder(task) / entry['path']).write_text('tampered')

        with patch.object(close_saves, 'checkpoint', side_effect=checkpoint):
            closed = self.close(task)
        self.assertEqual(closed['copy'], 'held')
        self.assertIn('checksum', closed['copy_reason'])
        self.assertTrue(self.target(task).is_dir())

    def test_scratch_returned_and_chat_scratch_clears_only_without_open_jobs(self):
        self.enable()
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

    def test_switch_defaults_off_and_local_control_enables_it(self):
        self.assertFalse(self.call('settings.get').data.get('close_clears_space', False))
        self.call('cleanup.close_space', enabled=True)
        self.assertTrue(self.call('settings.get').data['close_clears_space'])
        task = self.job()
        self.close(task)
        self.assert_cleared(task)

    def test_crash_between_copy_rename_and_detach_recovers(self):
        self.enable()
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

        self.call('tasks.update', task=task['id'], status='done')
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
        self.enable()
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

        self.call('tasks.update', task=task['id'], status='done')
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
        self.enable()
        task = self.job()
        target = self.target(task)
        (target / '.gitignore').write_text('notes.private\n')
        (target / 'notes.private').write_text('first ignored contents')
        from coordinator import close_saves
        original = close_saves.checkpoint
        changed = []

        def checkpoint(store, job, phase, data):
            original(store, job, phase, data)
            if phase == 'staged' and not changed:
                changed.append(True)
                stage = next(stage for stage, record in workspaces._deletion_stages(self.state) if record['task'] == task['id'])
                (stage / 'copy' / 'notes.private').write_text('late after rename')

        with patch.object(close_saves, 'checkpoint', side_effect=checkpoint):
            self.close(task)
        self.drain()
        self.assert_cleared(task)
        self.reopen(task)
        self.assertEqual((target / 'notes.private').read_text(), 'late after rename')

    def test_second_late_writer_holds_the_original_copy(self):
        self.enable()
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
        self.enable()
        task = self.job()
        target = self.target(task)
        (target / 'README.md').write_text('staged edit')
        self.git('add', 'README.md', cwd=target)
        (target / 'README.md').write_text('working edit')
        index = self.git_directory(target) / 'index'
        before = index.read_bytes()
        from coordinator import close_saves
        original = close_saves.checkpoint
        observed = []

        def checkpoint(store, job, phase, data):
            original(store, job, phase, data)
            if phase == 'snapshot':
                observed.append(True)
                self.assertEqual(index.read_bytes(), before)
                self.assertEqual(self.git('rev-parse', 'HEAD', cwd=target).stdout.strip(), self.base)
                self.assertEqual(self.git('stash', 'list', cwd=target).stdout, '')

        with patch.object(close_saves, 'checkpoint', side_effect=checkpoint):
            self.close(task)
        self.assertTrue(observed)
        self.assert_cleared(task)

    def test_unignored_env_is_held_before_git_snapshot(self):
        self.enable()
        task = self.job()
        (self.target(task) / '.gitignore').write_text('')
        (self.target(task) / '.env').write_text('ONLY_MADE_UP=fixture')
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'held')
        self.assertIn('.env', closed['copy_reason'])
        self.assertNotEqual(self.git('show-ref', '--verify', '--quiet', 'refs/torii/wip/task-' + str(task['id']), check=False).returncode, 0)

    def test_waiting_and_interrupted_rounds_use_the_same_save_queue(self):
        self.enable()
        for status in ('waiting', 'interrupted'):
            with self.subTest(status=status):
                task = self.job()
                (self.target(task) / 'README.md').write_text(status)
                self.worker(task['id'], self.target(task), status='waiting_for_quota')
                with self.store.db:
                    self.store.db.execute('UPDATE tasks SET status=? WHERE id=?', (status, task['id']))
                self.drain()
                self.assert_cleared(task)
                self.assertEqual(self.store.task_get(task['id'])['status'], status)

    def test_symbolic_scratch_root_is_held_without_touching_shared_data(self):
        self.enable()
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
        self.enable()
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
        self.enable()
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
        self.enable()
        task = self.job()
        with self.store.db:
            self.store.db.execute("UPDATE task_iterations SET cause='crash',resume_at=? WHERE task=?", (time.time() + 300, task['id']))
            self.store.db.execute("UPDATE tasks SET status='interrupted' WHERE id=?", (task['id'],))
        self.drain()
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'held')
        self.assertIn('short crash retry', self.store.task_get(task['id'])['copy_reason'])
        self.assertTrue(self.target(task).exists())

    def test_opencode_private_temp_and_cache_clear_after_proof_save(self):
        self.enable()
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
        self.enable()
        task = self.job()
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        (scratch / 'old-round.txt').write_text('old round scratch\n')
        (scratch / 'later').mkdir()
        (scratch / 'later' / 'x.txt').write_text('x\n')
        original = close_saves.stopped_workers
        reopened = []

        async def concurrent_reopen(store, row):
            await original(store, row)
            if 'clear_extras' in [frame.function for frame in inspect.stack()] and not reopened:
                reopened.append(True)
                self.call('tasks.update', task=task['id'], status='open')
                path = self.call('tasks.get', task=task['id']).data['scratch']
                (close_saves.Path(path) / 'later' / 'brief-for-new-round.md').write_text('new brief\n')

        self.call('tasks.update', task=task['id'], status='done')
        with patch('coordinator.close_saves.stopped_workers', concurrent_reopen):
            self.drain()
        self.assertTrue(reopened, 'the reopen hook did not run')
        current = self.store.task_get(task['id'])
        self.assertEqual(current['status'], 'open')
        self.assertTrue((scratch / 'later' / 'brief-for-new-round.md').exists(),
                        'a file written after the job reopened was deleted; copy=%s reason=%s' %
                        (current['copy'], current['copy_reason']))

    def test_notes_naming_the_worktree_folder_never_clear(self):
        self.enable()
        task = self.job()
        self.call('tasks.update', task=task['id'], note='Worktree: ' + str(self.target(task)))
        closed = self.close(task)
        self.assertEqual(closed['copy'], 'cleared', closed['copy_reason'])

    def test_nested_regenerable_name_inside_ignored_folder_is_saved(self):
        self.enable()
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

    def test_enabling_marks_old_closed_jobs_without_copies_as_kept(self):
        task = self.job()
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        (scratch / 'old.txt').write_text('old scratch')
        self.call('tasks.update', task=task['id'], status='done')
        subprocess.run(('git', '-C', str(self.project), 'worktree', 'remove', '--force', str(self.target(task))), check=True)
        self.assertFalse(self.target(task).exists())
        self.call('tasks.update', task=task['id'], note='Worktree: ' + str(self.target(task)))
        self.call('cleanup.close_space', enabled=True)
        self.drain()
        listing = self.call('tasks.list').text
        self.assertNotIn('copy kept', listing, listing)
        self.assertFalse(scratch.exists())
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'cleared')

    def test_restore_branch_movement_holds_before_edits_and_can_retry(self):
        self.enable()
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
        self.enable()
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

        self.call('tasks.update', task=task['id'], status='done')
        with patch('coordinator.workspaces.delete_tree', reopen_staged):
            self.drain()
        self.assertTrue(reopened)
        self.assertEqual((scratch / 'new.txt').read_text(), 'new')
        self.assertNotEqual(self.store.task_get(task['id'])['copy'], 'cleared')
        self.assertEqual(close_saves.load(self.store, task)[0], 'restored')

    def test_scratch_staging_resumes_after_delete_failure(self):
        self.enable()
        task = self.job()
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        (scratch / 'old.txt').write_text('old')
        original = workspaces.delete_tree

        async def fail_staged(path, **kwargs):
            if path.parent == self.folder(task) / 'extras':
                raise OSError('fixture interrupted scratch delete')
            await original(path, **kwargs)

        self.call('tasks.update', task=task['id'], status='done')
        with patch('coordinator.workspaces.delete_tree', fail_staged):
            self.drain()
        self.assertEqual(self.store.task_get(task['id'])['copy'], 'held')
        self.assertFalse(scratch.exists())
        self.call('tasks.update', task=task['id'], note='retry staged scratch')
        self.drain()
        self.assert_cleared(task)
        self.assertFalse(list((self.folder(task) / 'extras').iterdir()))

    def test_failed_ignored_restore_can_retry(self):
        self.enable()
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
        self.enable()
        task = self.job()
        scratch = close_saves.scratch(self.store, task['id'], create=True)
        (scratch / 'old.txt').write_text('old')
        original = close_saves.checkpoint
        interrupted = []

        def fail_after_rename(store, row, phase, saved):
            entry = saved.get('extras', {}).get(str(scratch))
            if phase == 'extras' and entry and entry['staged'] and not interrupted:
                interrupted.append(True)
                raise OSError('fixture interrupted after rename before commit')
            original(store, row, phase, saved)

        self.call('tasks.update', task=task['id'], status='done')
        with patch('coordinator.close_saves.checkpoint', fail_after_rename):
            self.drain()
        self.assertTrue(interrupted)
        self.assertFalse(scratch.exists())
        self.call('tasks.update', task=task['id'], note='retry interrupted rename')
        self.drain()
        self.assert_cleared(task)
        self.assertFalse(list((self.folder(task) / 'extras').iterdir()))

    def test_absent_copy_with_blocked_scratch_reports_cleanup_held(self):
        self.enable()
        task = self.job()
        self.call('tasks.update', task=task['id'], status='done')
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
