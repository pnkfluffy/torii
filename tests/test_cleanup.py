import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import cleanup, close_saves, workspaces
from coordinator.worker_storage import WorkerStorage
from tests import test_worktree_cleanup as fixtures


class WorkerEndSaveTests(unittest.TestCase):
    setUp = fixtures.WorktreeCleanupTests.setUp
    tearDown = fixtures.WorktreeCleanupTests.tearDown
    git = fixtures.WorktreeCleanupTests.git
    call = fixtures.WorktreeCleanupTests.call
    target = fixtures.WorktreeCleanupTests.target
    commit = fixtures.WorktreeCleanupTests.commit

    def job(self):
        return fixtures.WorktreeCleanupTests.job(self, closed=False)

    def worker(self, task, workspace=None):
        with self.store.db:
            worker = self.store.db.execute('''INSERT INTO workers(task,topic,provider,prompt,cwd,workspace,status,created,updated)
                VALUES (?,?,?,?,?,?,?,?,?)''', (task['id'], fixtures.TOPIC, 'codex', 'Work', str(self.target(task)),
                                                json.dumps(workspace or {}), 'done', self.now, self.now)).lastrowid
        return dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (worker,)).fetchone())

    def save(self, task, workspace=None):
        return asyncio.run(cleanup.save(self.store, self.worker(self.store.task_get(task['id']), workspace)))

    def ref(self, task):
        task = self.store.task_get(task['id'])
        return self.git('rev-parse', '--verify', '--quiet', cleanup.saved_ref(task), check=False).stdout.strip()

    def files(self, commit):
        return self.git('ls-tree', '-r', '--name-only', commit).stdout.split()

    def test_a_save_holds_changed_and_untracked_files_and_chains_every_earlier_save(self):
        task = self.job()
        target = self.target(task)
        (target / 'README.md').write_text('changed\n')
        (target / 'new.txt').write_text('new\n')
        (target / '.env').write_text('SECRET=1\n')
        (target / 'cert.pem').write_text('pem\n')
        first = self.save(task)
        self.assertEqual(first, self.ref(task))
        self.assertEqual(sorted(self.files(first)), ['.gitignore', 'README.md', 'nested/keep.txt', 'new.txt'])
        self.assertEqual(self.git('show', first + ':README.md').stdout, 'changed\n')
        self.assertIn('Left out: cert.pem', self.git('log', '-1', '--format=%B', first).stdout)
        self.assertEqual(self.save(task), first)
        dropped = self.commit(target, 'dropped later\n')
        second = self.save(task)
        self.git('reset', '-q', '--hard', self.base, cwd=target)
        (target / 'other.txt').write_text('other\n')
        third = self.save(task)
        self.assertEqual(self.git('rev-parse', third + '^1', third + '^2').stdout.split(), [self.base, second])
        self.assertEqual(self.git('rev-parse', second + '^1', second + '^2').stdout.split(), [dropped, first])
        self.git('reflog', 'expire', '--expire=now', '--all')
        self.git('gc', '-q', '--prune=now')
        self.assertEqual(self.git('merge-base', '--is-ancestor', dropped, third, check=False).returncode, 0)

    def test_nothing_is_saved_for_a_clean_tree_or_a_scratch_worker(self):
        task = self.job()
        self.assertIsNone(self.save(task))
        (self.target(task) / 'new.txt').write_text('new\n')
        self.assertIsNone(self.save(task, {'scratch': True}))
        self.assertEqual(self.ref(task), '')

    def test_the_close_save_keeps_the_worker_end_save_as_a_parent(self):
        task = self.job()
        target = self.target(task)
        (target / 'README.md').write_text('worker\n')
        first = self.save(task)
        (target / 'README.md').write_text('later\n')
        with patch.object(close_saves, 'TERMINAL', ()):
            self.call('tasks.update', task=task['id'], status='done', summary='Closed in test.')
            asyncio.run(workspaces.cleanup_closed_copies(self.store))
        saved = json.loads((self.state / 'results' / ('task-%d' % task['id']) / 'rounds' / '0' / 'saved.json').read_text())
        self.assertEqual(self.git('rev-parse', saved['wip'] + '^1', saved['wip'] + '^2').stdout.split(), [self.base, first])
        self.assertEqual(self.git('show', saved['wip'] + ':README.md').stdout, 'later\n')

    def test_worker_storage_save_reports_a_failure(self):
        task = self.job()
        worker = self.worker(task)
        storage = WorkerStorage(self.store)
        with patch.object(cleanup, 'save', AsyncMock(side_effect=ValueError('broken'))) as save:
            asyncio.run(storage.save(worker))
            save.assert_awaited_once()
        row = self.store.db.execute("SELECT task,worker FROM problems WHERE area='worker' AND code='save-failed'").fetchone()
        self.assertEqual(tuple(row), (task['id'], worker['id']))


if __name__ == '__main__':
    unittest.main()
