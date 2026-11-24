from pathlib import Path
import subprocess
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import scratch_worktrees, worker_storage
from coordinator.store import Store


TOPIC = '1:2'


class ScratchWorktreeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='scratch-worktrees-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.origin = self.root / 'origin.git'
        self.project = self.root / 'project'
        subprocess.run(['git', 'init', '-q', '--bare', '-b', 'main', str(self.origin)], check=True)
        subprocess.run(['git', 'init', '-q', '-b', 'main', str(self.project)], check=True)
        self.git('config', 'user.name', 'Test')
        self.git('config', 'user.email', 'test@example.com')
        (self.project / '.gitignore').write_text('node_modules/\n*.local\n')
        (self.project / 'proof.txt').write_text('proof\n')
        self.git('add', '.')
        self.git('commit', '-qm', 'base')
        self.git('remote', 'add', 'origin', self.origin)
        self.git('push', '-q', 'origin', 'main')
        self.git('fetch', '-q', 'origin')
        self.store = Store(self.root / 'state', token_file=self.root / 'no-token')
        self.addCleanup(self.store.close)
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES (?,1,2,'test',?)",
                                  (TOPIC, str(self.project)))
        self.task = self.store.task_create(TOPIC, 'Build', worktree=str(self.project))
        self.scratch = self.store.directory / 'scratch' / ('task-%d' % self.task['id'])
        self.processes = patch.object(scratch_worktrees, '_process_paths', new=AsyncMock(return_value=set()))
        self.processes.start()
        self.addCleanup(self.processes.stop)

    def git(self, *args, cwd=None):
        return subprocess.run(['git', '-C', str(cwd or self.project), *map(str, args)],
                              capture_output=True, text=True, check=True, timeout=30).stdout.strip()

    def add(self, name='wt', parent=None):
        target = (parent or self.scratch) / name
        self.git('worktree', 'add', '-q', '-b', name, target, 'origin/main')
        return target

    def registered(self):
        return [Path(line[9:]) for line in self.git('worktree', 'list', '--porcelain').splitlines()
                if line.startswith('worktree ')]

    def add_worker(self, status):
        with self.store.db:
            return self.store.db.execute('''INSERT INTO workers(task,topic,provider,prompt,cwd,workspace,status,created,updated)
                VALUES (?,?,?,?,?,?,?,?,?)''', (self.task['id'], TOPIC, 'codex', 'Build', str(self.project), '{}',
                                                status, 1.0, 1.0)).lastrowid

    async def test_clean_merged_worktree_is_removed_with_ignored_build_output(self):
        target = self.add()
        (target / 'node_modules' / 'left-pad').mkdir(parents=True)
        (target / 'node_modules' / 'left-pad' / 'index.js').write_text('module.exports = 1\n')
        self.add_worker('done')
        report = await scratch_worktrees.retire(self.store, self.task['id'])
        self.assertEqual(report, {str(target): 'removed'})
        self.assertFalse(target.exists())
        self.assertNotIn(target, self.registered())
        self.assertEqual(self.git('rev-parse', '--verify', 'refs/heads/wt'), self.git('rev-parse', 'origin/main'))

    async def test_retiring_keeps_registrations_of_trees_on_an_unmounted_volume(self):
        volume = self.add('task-99', self.store.directory / 'cow-worktrees')
        away = self.root / 'unmounted'
        volume.rename(away)
        target = self.add()
        self.add_worker('done')
        report = await scratch_worktrees.retire(self.store, self.task['id'])
        self.assertEqual(report, {str(target): 'removed'})
        self.assertIn(volume, self.registered())
        away.rename(volume)
        self.assertEqual(self.git('status', '--porcelain', cwd=volume), '')

    async def test_worktree_on_another_file_system_stays(self):
        target = self.add()
        self.add_worker('done')
        with patch.object(scratch_worktrees.workspaces, 'on_volume', side_effect=lambda state, path: path == target):
            report = await scratch_worktrees.retire(self.store, self.task['id'])
        self.assertEqual(report, {str(target): 'worktree is on another file system'})
        self.assertIn(target, self.registered())

    async def test_dirty_untracked_and_unknown_ignored_worktrees_stay(self):
        cases = {'tracked': ('proof.txt', 'changed\n', 'uncommitted or untracked'),
                 'untracked': ('notes.md', 'draft\n', 'uncommitted or untracked'),
                 'ignored': ('settings.local', 'secret\n', 'ignored files need preservation')}
        targets = {}
        for name, (path, text, _) in cases.items():
            targets[name] = self.add(name)
            (targets[name] / path).write_text(text)
        report = await scratch_worktrees.retire(self.store, self.task['id'])
        for name, (_, _, reason) in cases.items():
            self.assertIn(reason, report[str(targets[name])])
            self.assertTrue(targets[name].is_dir())

    async def test_unmerged_head_stays(self):
        target = self.add()
        (target / 'change.txt').write_text('new\n')
        self.git('add', '.', cwd=target)
        self.git('commit', '-qm', 'unmerged', cwd=target)
        report = await scratch_worktrees.retire(self.store, self.task['id'])
        self.assertEqual(report, {str(target): 'HEAD is not on the remote main branch'})
        self.assertTrue(target.is_dir())

    async def test_keep_link_and_proof_references_keep_the_worktree(self):
        linked = self.add('linked')
        proved = self.add('proved')
        with self.store.db:
            self.store.db.execute("INSERT INTO keep_links(id,task,locator,reason) VALUES ('k1',?,?,'final')",
                                  (self.task['id'], str(linked / 'proof.txt')))
        self.store.task_update(self.task['id'], note='Result proof=%s' % (proved / 'proof.txt'))
        report = await scratch_worktrees.retire(self.store, self.task['id'])
        self.assertEqual(report, {str(linked): 'KEEP link uses worktree', str(proved): 'proof is inside worktree'})
        self.assertTrue(linked.is_dir() and proved.is_dir())

    async def test_live_process_lock_and_git_operation_keep_the_worktree(self):
        used = self.add('used')
        locked = self.add('locked')
        self.git('worktree', 'lock', locked)
        self.processes.stop()
        with patch.object(scratch_worktrees, '_process_paths', new=AsyncMock(return_value={used / 'proof.txt'})):
            report = await scratch_worktrees.retire(self.store, self.task['id'])
        self.processes.start()
        self.assertEqual(report, {str(locked): 'worktree is locked', str(used): 'live process uses worktree'})
        self.assertTrue(used.is_dir() and locked.is_dir())

    async def test_unfinished_worker_keeps_a_fresh_worktree_and_retires_a_stale_one(self):
        fresh = self.add('fresh')
        stale = self.add('stale')
        used = self.add('used')
        self.add_worker('running')
        with self.store.db:
            self.store.db.execute('UPDATE workers SET cwd=?', (str(used / 'sub'),))
        real = scratch_worktrees._latest_write

        async def latest(target, registered):
            return await real(target, registered) - (49 * 3600 if target != fresh else 0)

        for status in ('queued', 'running', 'waiting_for_secret', 'waiting_for_quota'):
            with self.subTest(status=status):
                with self.store.db:
                    self.store.db.execute('UPDATE workers SET status=?', (status,))
                if not stale.exists():
                    stale = self.add('stale-' + status)
                with patch.object(scratch_worktrees, '_latest_write', new=latest):
                    report = await scratch_worktrees.retire(self.store, self.task['id'])
                self.assertEqual(report, {str(fresh): 'job has an unfinished worker', str(stale): 'removed',
                                          str(used): 'an unfinished worker uses the worktree'})
                self.assertTrue(fresh.is_dir() and used.is_dir())
                self.assertFalse(stale.exists())
        with self.store.db:
            self.store.db.execute("UPDATE workers SET status='done'")
        self.assertEqual(await scratch_worktrees.retire(self.store, self.task['id']),
                         {str(fresh): 'removed', str(used): 'removed'})

    async def test_managed_job_worktree_is_never_considered(self):
        managed = self.add('managed', self.store.directory / 'worktrees' / ('task-%d' % self.task['id']))
        self.assertEqual(await scratch_worktrees.retire(self.store, self.task['id']), {})
        self.assertTrue(managed.is_dir())
        self.assertIn(managed, self.registered())

    async def test_one_failed_worktree_backs_off_and_the_others_retire(self):
        bad, good = self.add('bad'), self.add('good')
        self.add_worker('done')
        real = scratch_worktrees._retire

        async def retire(store, task_id, scratch, target):
            if target.name == 'bad':
                raise RuntimeError('boom')
            return await real(store, task_id, scratch, target)

        with patch.object(scratch_worktrees, '_retire', retire):
            report = await scratch_worktrees.retire(self.store, self.task['id'])
            self.assertEqual(report, {str(bad): 'failed', str(good): 'removed'})
            self.assertEqual(await scratch_worktrees.retire(self.store, self.task['id']), {})
        self.assertTrue(bad.is_dir())
        [row] = self.store.db.execute(
            "SELECT area,task,detail FROM problems WHERE code='scratch-worktree-failed'").fetchall()
        self.assertEqual((row['area'], row['task']), ('worker', self.task['id']))
        self.assertRegex(row['detail'], r'^item=%d:bad failures=1 RuntimeError: boom at test_scratch_worktrees\.py:'
                         % self.task['id'])
        with patch('coordinator.sweeps.time.time', return_value=time.time() + 120):
            self.assertEqual(await scratch_worktrees.retire(self.store, self.task['id']), {str(bad): 'removed'})
        self.assertIsNone(self.store.db.execute('SELECT 1 FROM sweep_failures').fetchone())

    async def test_last_worker_cleanup_retires_scratch_worktrees(self):
        target = self.add()
        worker = self.add_worker('done')
        storage = worker_storage.storage_for(self.store)
        with self.store.db:
            self.store.db.execute('INSERT INTO worker_temp_state(worker,finished,pending) VALUES (?,1,1) '
                                  'ON CONFLICT(worker) DO UPDATE SET finished=1,pending=1', (worker,))
        with patch.object(worker_storage, '_processes_gone', return_value=True), \
                patch('coordinator.close_saves.preserve_worker_proofs', new=AsyncMock(return_value=True)):
            await storage.cleanup(worker)
        self.assertFalse(target.exists())

if __name__ == '__main__':
    unittest.main()
