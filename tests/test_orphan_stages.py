import os
from pathlib import Path
import time
import unittest
from unittest.mock import patch
import uuid

from coordinator import orphan_stages
from tests import test_worktree_cleanup as fixtures


class OrphanStagesTests(unittest.TestCase):
    setUp = fixtures.WorktreeCleanupTests.setUp
    tearDown = fixtures.WorktreeCleanupTests.tearDown
    git = fixtures.WorktreeCleanupTests.git
    call = fixtures.WorktreeCleanupTests.call
    target = fixtures.WorktreeCleanupTests.target
    job = fixtures.WorktreeCleanupTests.job

    def make_orphan(self, folder, task, *, age=orphan_stages.AGE + 10):
        stage = 'task-%d-%s' % (task['id'], uuid.uuid4().hex)
        name = stage + '-' + uuid.uuid4().hex if folder == 'worktree-deletion-preparing' else stage
        path = self.state / folder / name
        path.mkdir(parents=True)
        (path / 'data').write_text('preserve')
        past = time.time() - age
        os.utime(path, (past, past))
        return path

    def sweep(self, paths=()):
        trash = self.root / 'trash'
        with patch.object(orphan_stages.stray_report, 'process_paths', return_value=list(paths)), \
                patch.object(orphan_stages.stray_report, 'launch_paths', return_value=[]), \
                patch.object(orphan_stages.host_os, 'trash_directory', return_value=trash):
            return orphan_stages.sweep(self.state)

    def test_moves_old_unowned_stages_to_trash(self):
        task = self.job(closed=False)
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET status='done' WHERE id=?", (task['id'],))
            self.store.db.execute("UPDATE tasks SET copy='cleared' WHERE id=?", (task['id'],))
        paths = [self.make_orphan(folder, task) for folder in ('worktree-deletion-preparing', 'idle-trim-quarantine')]
        self.assertEqual(self.sweep(), 2)
        self.assertTrue(all(not path.exists() for path in paths))
        self.assertEqual(sorted((path / 'data').read_text() for path in (self.root / 'trash').iterdir()),
                         ['preserve', 'preserve'])

    def test_one_failed_stage_does_not_stop_the_others(self):
        task = self.job(closed=False)
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET status='done' WHERE id=?", (task['id'],))
            self.store.db.execute("UPDATE tasks SET copy='cleared' WHERE id=?", (task['id'],))
        paths = [self.make_orphan('idle-trim-quarantine', task) for _ in range(2)]
        real = orphan_stages.idle_trim.rename_exclusive
        bad = min(paths, key=lambda path: path.name)

        def rename(source, target):
            if source == bad:
                raise OSError('fixture rename failure')
            return real(source, target)

        with patch.object(orphan_stages.idle_trim, 'rename_exclusive', rename), \
                self.assertLogs('coordinator.problems', 'WARNING'):
            self.assertEqual(self.sweep(), 1)
        self.assertEqual([path.exists() for path in paths], [path == bad for path in paths])
        [row] = self.store.db.execute("SELECT detail,task FROM problems WHERE code='orphan-stage-failed'").fetchall()
        self.assertEqual(row[1], task['id'])
        self.assertRegex(row[0], r'^item=%s failures=1 OSError: fixture rename failure at test_orphan_stages\.py:\d+ rename'
                         % bad.name)
        self.assertEqual(self.sweep(), 0)
        self.assertTrue(bad.exists())

    def test_keeps_new_live_and_owned_stages(self):
        task = self.job(closed=False)
        old = self.make_orphan('idle-trim-quarantine', task)
        new = self.make_orphan('worktree-deletion-preparing', task, age=1)
        self.assertEqual(self.sweep(), 0)
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET status='done' WHERE id=?", (task['id'],))
        (self.state / 'idle-trim-stages' / ('task-' + str(task['id']))).mkdir(parents=True)
        self.assertEqual(self.sweep(), 0)
        self.assertTrue(old.exists())
        self.assertTrue(new.exists())

    def test_keeps_stage_used_by_process_or_symlink(self):
        task = self.job(closed=False)
        other = self.job(closed=False)
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET status='done' WHERE id=?", (task['id'],))
        path = self.make_orphan('idle-trim-quarantine', task)
        self.assertEqual(self.sweep([path / 'data']), 0)
        scratch = self.state / 'scratch' / ('task-' + str(other['id']))
        scratch.mkdir(parents=True, exist_ok=True)
        (scratch / 'stage-link').symlink_to(path)
        self.assertEqual(self.sweep(), 0)
        self.assertTrue(path.exists())
