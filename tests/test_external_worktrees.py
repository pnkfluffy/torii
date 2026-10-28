import asyncio
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import external_worktrees as sweep
from coordinator import workspaces
from coordinator.store import Store


class ExternalWorktreeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='external-worktrees-')
        self.root = Path(self.temporary.name).resolve()
        self.project = self.root / 'project'
        self.project.mkdir()
        self.git('init', '-q')
        self.git('config', 'user.name', 'Test')
        self.git('config', 'user.email', 'test@example.com')
        (self.project / 'base').write_bytes(b'base\x00binary\n')
        self.git('add', '.')
        self.git('commit', '-qm', 'base')
        self.scratch = self.root / 'scratch'
        self.target = self.scratch / 'orphan'
        self.git('worktree', 'add', '-qb', 'scratch', self.target)
        self.store = Store(self.root / 'state', token_file=self.root / 'no-token')
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('1',1,1,'test',?)",
                                  (str(self.project),))
        self.now = time.time()
        self.processes = patch.object(sweep, '_process_paths', new=AsyncMock(return_value=set()))
        self.processes.start()
        self.addCleanup(self.processes.stop)
        self.age()

    def tearDown(self):
        self.store.close()
        self.temporary.cleanup()

    def git(self, *args, cwd=None):
        return subprocess.run(['git', '-C', str(cwd or self.project), *map(str, args)],
                              capture_output=True, text=True, check=True, timeout=30).stdout.strip()

    def age(self):
        git_dir = self.project / '.git' / 'worktrees' / self.target.name
        for root in (self.target, git_dir):
            for path in sorted(root.rglob('*'), key=lambda item: len(item.parts), reverse=True):
                os.utime(path, (self.now - 10 * sweep.DAY,) * 2, follow_symlinks=False)
            os.utime(root, (self.now - 10 * sweep.DAY,) * 2)

    def run_sweep(self, apply=False, **options):
        policy = sweep.Policy((self.scratch,), apply=apply, **options)
        report = asyncio.run(sweep.sweep(self.store.db, self.store.directory,
                                        [self.project], policy, now=self.now))
        self.assertEqual(len(report['roots']), 1, report)
        return report['roots'][0]

    def job(self, status='done'):
        with self.store.db:
            task = self.store.task_create('1', 'owner', worktree=str(self.target))
            self.store.db.execute('UPDATE tasks SET status=?,updated=? WHERE id=?',
                                  (status, self.now - 10 * sweep.DAY, task['id']))
        return task['id']

    def worker(self, task, status='queued', cwd=None):
        with self.store.db:
            self.store.db.execute('''INSERT INTO workers(task,topic,provider,prompt,cwd,status,created,updated)
                                    VALUES (?,'1','codex','fixture',?,?,?,?)''',
                                  (task, str(cwd or self.target), status, self.now - 10 * sweep.DAY,
                                   self.now - 10 * sweep.DAY))

    def test_report_default_does_not_remove_or_create_salvage(self):
        self.assertEqual(self.run_sweep()['state'], 'eligible')
        self.assertTrue(self.target.is_dir())
        self.assertFalse((self.store.directory / 'results').exists())
        self.assertIn(str(self.target), self.git('worktree', 'list', '--porcelain'))

    def test_apply_preserves_binary_changes_untracked_and_unpublished_commits(self):
        (self.target / 'new').write_text('unpublished\n')
        self.git('add', 'new', cwd=self.target)
        self.git('commit', '-qm', 'unpublished', cwd=self.target)
        commit = self.git('rev-parse', 'HEAD', cwd=self.target)
        (self.target / 'base').write_bytes(b'changed\x00binary\n')
        (self.target / 'untracked').write_bytes(b'keep\x00this')
        self.age()
        result = self.run_sweep(apply=True)
        self.assertEqual(result['state'], 'removed', result)
        self.assertFalse(self.target.exists())
        self.assertNotIn(str(self.target), self.git('worktree', 'list', '--porcelain'))
        saved = Path(result['salvage']).parent
        self.assertEqual((saved / '0' / 'untracked' / 'untracked').read_bytes(), b'keep\x00this')
        self.assertIn(b'GIT binary patch', (saved / '0' / 'changes.patch').read_bytes())
        self.assertIn(commit, self.git('bundle', 'list-heads', saved / '0' / 'commits.bundle'))
        repeated = asyncio.run(sweep.sweep(self.store.db, self.store.directory, [self.project],
                                          sweep.Policy((self.scratch,), apply=True), now=self.now))
        self.assertEqual(repeated['roots'], [])

    def test_apply_keeps_unrelated_missing_worktree_registration(self):
        missing = self.root / 'unplugged'
        self.git('worktree', 'add', '-q', '--detach', missing)
        (missing / 'local').write_text('detached work')
        self.git('add', 'local', cwd=missing)
        self.git('commit', '-qm', 'detached work', cwd=missing)
        commit = self.git('rev-parse', 'HEAD', cwd=missing)
        missing.rename(self.root / 'moved')
        result = self.run_sweep(apply=True)
        self.assertEqual(result['state'], 'removed', result)
        self.assertIn('worktree ' + str(missing) + '\n', self.git('worktree', 'list', '--porcelain'))
        self.assertIn(commit, self.git('rev-list', '--all', '--reflog'))

    def test_apply_keeps_ignored_files_without_creating_salvage(self):
        (self.target / '.gitignore').write_text('ignored/\n')
        self.git('add', '.gitignore', cwd=self.target)
        self.git('commit', '-qm', 'ignore local output', cwd=self.target)
        ignored = self.target / 'ignored' / 'only-copy.bin'
        ignored.parent.mkdir()
        ignored.write_bytes(b'only copy\x00')
        self.age()
        result = self.run_sweep(apply=True)
        self.assertEqual(result['state'], 'kept', result)
        self.assertEqual(ignored.read_bytes(), b'only copy\x00')
        self.assertFalse((self.store.directory / 'results').exists())

    def test_apply_keeps_nested_repository_without_creating_salvage(self):
        (self.target / '.gitignore').write_text('nested/\n')
        self.git('add', '.gitignore', cwd=self.target)
        self.git('commit', '-qm', 'ignore nested repository', cwd=self.target)
        nested = self.target / 'nested'
        self.git('clone', '-q', self.project, nested)
        self.git('config', 'user.name', 'Test', cwd=nested)
        self.git('config', 'user.email', 'test@example.com', cwd=nested)
        (nested / 'only-copy').write_text('nested work')
        self.git('add', 'only-copy', cwd=nested)
        self.git('commit', '-qm', 'nested work', cwd=nested)
        commit = self.git('rev-parse', 'HEAD', cwd=nested)
        self.age()
        result = self.run_sweep(apply=True)
        self.assertEqual(result['state'], 'kept', result)
        self.assertEqual(self.git('rev-parse', 'HEAD', cwd=nested), commit)
        self.assertFalse((self.store.directory / 'results').exists())

    def test_apply_keeps_reflog_only_commits(self):
        (self.target / 'local').write_text('lost reference')
        self.git('add', 'local', cwd=self.target)
        self.git('commit', '-qm', 'local work', cwd=self.target)
        commit = self.git('rev-parse', 'HEAD', cwd=self.target)
        self.git('reset', '--hard', 'HEAD~1', cwd=self.target)
        self.age()
        result = self.run_sweep(apply=True)
        self.assertEqual(result['state'], 'kept', result)
        self.assertIn(commit, self.git('rev-list', '--reflog', cwd=self.target))
        self.assertFalse((self.store.directory / 'results').exists())

    def test_reference_checks_work_without_rg_on_path(self):
        command = workspaces._command

        async def without_rg(*args, **kwargs):
            if args[0] == 'rg':
                raise FileNotFoundError('rg unavailable')
            return await command(*args, **kwargs)

        with patch('shutil.which', return_value=None), patch.object(workspaces, '_command', side_effect=without_rg):
            self.assertEqual(self.run_sweep()['state'], 'eligible')
            (self.project / 'reference.txt').write_text(str(self.target))
            self.assertEqual(self.run_sweep(apply=True)['reason'], 'level A external reference')
        self.assertTrue(self.target.is_dir())

    def test_open_owner_blocks_retirement(self):
        self.job(status='open')
        self.assertEqual(self.run_sweep(apply=True)['reason'], 'owning job is open')
        self.assertTrue(self.target.is_dir())

    def test_submodule_source_is_preserved_before_external_retirement(self):
        module = self.root / 'module'
        module.mkdir()
        self.git('init', '-q', cwd=module)
        self.git('config', 'user.name', 'Test', cwd=module)
        self.git('config', 'user.email', 'test@example.com', cwd=module)
        (module / 'source').write_text('module base\n')
        self.git('add', '.', cwd=module)
        self.git('commit', '-qm', 'module base', cwd=module)
        self.git('-c', 'protocol.file.allow=always', 'submodule', 'add', str(module), 'sub', cwd=self.target)
        self.git('commit', '-qm', 'add module', cwd=self.target)
        (self.target / 'sub' / 'source').write_text('module edit\n')
        self.age()
        result = self.run_sweep(apply=True)
        self.assertEqual(result['state'], 'removed', result)
        saved = Path(result['salvage']).parent
        manifest = json.loads(Path(result['salvage']).read_text())
        self.assertEqual([item['path'] for item in manifest['repositories']], ['.', 'sub'])
        self.assertIn('module edit', (saved / '1' / 'changes.patch').read_text())

    def test_closed_owner_with_queued_worker_elsewhere_blocks_retirement(self):
        owner = self.job()
        self.worker(owner, cwd=self.project)
        self.assertEqual(self.run_sweep(apply=True)['reason'], 'unfinished worker')

    def test_owners_found_in_later_worker_rows_protect_earlier_queued_worker(self):
        owner = self.job()
        with self.store.db:
            self.store.db.execute('UPDATE tasks SET worktree=NULL WHERE id=?', (owner,))
        self.worker(owner, cwd=self.project)
        self.worker(owner, status='done')
        self.assertEqual(self.run_sweep(apply=True)['reason'], 'unfinished worker')

    def test_recent_file_or_recent_closed_owner_is_kept(self):
        os.utime(self.target / 'base', None)
        self.assertEqual(self.run_sweep()['reason'], 'worktree is too recent')
        self.age()
        owner = self.job()
        with self.store.db:
            self.store.db.execute('UPDATE tasks SET updated=? WHERE id=?', (self.now, owner))
        self.assertEqual(self.run_sweep()['reason'], 'owner or worker is too recent')

    def test_live_process_or_unavailable_process_scan_keeps_worktree(self):
        with patch.object(sweep, '_process_paths', new=AsyncMock(return_value={self.target / 'base'})):
            self.assertEqual(self.run_sweep(apply=True)['reason'], 'live process uses worktree')
        with patch.object(sweep, '_process_paths', new=AsyncMock(side_effect=RuntimeError('process scan failed'))):
            self.assertEqual(self.run_sweep(apply=True)['reason'], 'process scan failed')
        self.assertTrue(self.target.is_dir())

    def test_git_lock_and_owner_protected_names_are_kept(self):
        self.git('worktree', 'lock', self.target)
        self.assertEqual(self.run_sweep(apply=True)['reason'], 'worktree is locked')
        self.git('worktree', 'unlock', self.target)
        self.age()
        result = self.run_sweep(apply=True, protected_names=frozenset(('orphan',)))
        self.assertEqual(result['reason'], 'protected name')

    def test_level_a_references_and_notes_keep_old_unowned_worktrees(self):
        reference = self.project / 'keep.txt'
        reference.write_text(str(self.target))
        self.assertEqual(self.run_sweep(apply=True)['reason'], 'level A external reference')
        reference.write_text('reference removed')
        with self.store.db:
            task = self.store.task_create('1', 'reference', notes='Keep ' + str(self.target))
        self.assertEqual(self.run_sweep(apply=True)['reason'], 'level A job note')
        with self.store.db:
            self.store.db.execute('UPDATE tasks SET notes=NULL WHERE id=?', (task['id'],))
        self.assertEqual(self.run_sweep()['state'], 'eligible')

    def test_failed_preservation_keeps_source_and_registration(self):
        with patch.object(sweep, 'preserve_worktree', new=AsyncMock(side_effect=OSError('cannot save'))):
            self.assertEqual(self.run_sweep(apply=True)['reason'], 'cannot save')
        self.assertTrue(self.target.is_dir())
        self.assertIn(str(self.target), self.git('worktree', 'list', '--porcelain'))

    def test_worker_started_during_preservation_blocks_removal(self):
        preserve = sweep.preserve_worktree

        async def with_worker(*args):
            saved = await preserve(*args)
            self.worker(None)
            return saved

        with patch.object(sweep, 'preserve_worktree', side_effect=with_worker):
            result = self.run_sweep(apply=True)
        self.assertEqual(result['reason'], 'unfinished worker')
        self.assertTrue(Path(result['salvage']).is_file())
        self.assertTrue(self.target.is_dir())

    def test_root_timeout_keeps_root_and_continues(self):
        second = self.scratch / 'second'
        self.git('worktree', 'add', '-qb', 'second', second)
        original = sweep._candidate

        async def slow_first(*args):
            if args[3]['worktree'] == str(self.target):
                await asyncio.sleep(2)
            return await original(*args)

        with patch.object(sweep, '_candidate', side_effect=slow_first):
            report = asyncio.run(sweep.sweep(self.store.db, self.store.directory, [self.project],
                                            sweep.Policy((self.scratch,), root_seconds=0.5), now=self.now))
        self.assertEqual(len(report['roots']), 2)
        self.assertEqual(report['roots'][0]['reason'], 'time limit exceeded')
        self.assertEqual(report['roots'][1]['reason'], 'worktree is too recent')

    def test_overall_deadline_stops_starting_roots(self):
        self.git('worktree', 'add', '-qb', 'second', self.scratch / 'second')

        async def slow(*args):
            await asyncio.sleep(2)

        with patch.object(sweep, '_candidate', side_effect=slow):
            report = asyncio.run(sweep.sweep(self.store.db, self.store.directory, [self.project],
                                            sweep.Policy((self.scratch,), total_seconds=0.1), now=self.now))
        self.assertTrue(report['deadline_reached'])
        self.assertEqual(len(report['roots']), 1)

    def test_invalid_age_and_limits_are_rejected(self):
        for age in (0, -1, float('nan'), float('inf')):
            with self.assertRaises(ValueError):
                sweep.Policy((self.scratch,), idle_days=age)

    def test_report_cli_requires_no_token_and_keeps_database_read_only(self):
        report = self.root / 'report.json'
        done = subprocess.run(['/usr/bin/python3', '-m', 'coordinator.external_worktrees',
                               '--state-dir', str(self.store.directory), '--repository', str(self.project),
                               '--root', str(self.root / 'outside'), '--report', str(report)],
                              capture_output=True, text=True, timeout=30)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertEqual(json.loads(report.read_text())['roots'], [])
        self.assertTrue(self.target.is_dir())
