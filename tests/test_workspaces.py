import asyncio
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from coordinator import workspaces
from coordinator.workspaces import _command, prepare_task_workspace, project_identity
from tests.support import settle, until_exists, until_text, until_unlocked


class WorkspaceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.project = self.root / 'project'
        self.project.mkdir()
        self.git('init', '-q')
        self.git('config', 'user.email', 'test@example.com')
        self.git('config', 'user.name', 'Test')
        (self.project / 'README.md').write_text('source\n')
        (self.project / 'nested').mkdir()
        self.git('add', '.')
        self.git('commit', '-qm', 'base')
        self.base = self.git('rev-parse', 'HEAD').stdout.strip()
        self.state = self.root / 'state'

    def tearDown(self):
        self.temp.cleanup()

    def git(self, *args, cwd=None, check=True):
        return subprocess.run(('git', '-C', str(cwd or self.project), *args), check=check,
                              text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def linked_worktree(self, name):
        linked = self.root / name
        self.git('worktree', 'add', '-q', '-b', name, str(linked))
        self.assertTrue((linked / '.git').is_file())
        return linked

    async def test_task_worktree_branch_and_folder_use_the_global_task_id(self):
        result = await prepare_task_workspace(self.project / 'nested', self.state, 13)
        target = (self.state / 'worktrees' / 'task-13').resolve()
        self.assertEqual(result, {
            'cwd': str(target / 'nested'), 'branch': 'torii/task-13',
            'project_root': str(self.project.resolve()), 'isolated': True,
            'base_commit': self.base,
            'project_key': str((self.project / '.git').resolve())})
        self.assertEqual(self.git('branch', '--show-current', cwd=target).stdout.strip(), 'torii/task-13')
        (target / 'README.md').write_text('isolated\n')
        self.assertEqual((self.project / 'README.md').read_text(), 'source\n')
        self.assertFalse((target / 'node_modules').exists())

    async def test_two_topics_on_one_repository_get_separate_task_branches(self):
        first = await prepare_task_workspace(self.project, self.state, 9)
        second = await prepare_task_workspace(self.linked_worktree('other-topic'), self.state, 10)
        self.assertEqual((first['branch'], second['branch']), ('torii/task-9', 'torii/task-10'))
        self.assertNotEqual(first['cwd'], second['cwd'])
        self.assertEqual(first['project_key'], second['project_key'])

    async def test_existing_task_worktree_is_reused_without_reset(self):
        first = await prepare_task_workspace(self.project, self.state, 2)
        marker = Path(first['cwd']) / 'dirty.txt'
        marker.write_text('keep me')
        second = await prepare_task_workspace(self.project, self.state, 2)
        self.assertEqual(second, first)
        self.assertEqual(marker.read_text(), 'keep me')

    async def test_taken_branch_or_unrelated_folder_is_refused(self):
        self.linked_worktree('torii/task-3')
        with self.assertRaisesRegex(RuntimeError, 'already used'):
            await prepare_task_workspace(self.project, self.state, 3)
        collision = self.state / 'worktrees' / 'task-4'
        collision.mkdir(parents=True)
        subprocess.run(('git', '-C', str(collision), 'init', '-q'), check=True)
        with self.assertRaisesRegex(RuntimeError, 'collision|another repository'):
            await prepare_task_workspace(self.project, self.state, 4)

    async def test_non_git_project_is_shared_with_a_stated_reason(self):
        plain = self.root / 'plain'
        plain.mkdir()
        result = await project_identity(plain)
        self.assertIn('Not a Git repository', result.pop('reason'))
        self.assertEqual(result, {'cwd': str(plain.resolve()), 'project_root': str(plain.resolve()),
                                  'base_commit': None, 'project_key': str(plain.resolve())})
        with self.assertRaisesRegex(ValueError, 'Git repository with at least one commit'):
            await prepare_task_workspace(plain, self.state, 1)
        self.assertFalse((self.state / 'worktrees').exists())

    async def test_repository_without_a_commit_is_shared_with_a_stated_reason(self):
        empty = self.root / 'empty'
        empty.mkdir()
        subprocess.run(('git', '-C', str(empty), 'init', '-q'), check=True)
        result = await project_identity(empty)
        self.assertIsNone(result['base_commit'])
        self.assertIn('no commit to branch from', result['reason'])
        self.assertEqual(result['project_key'], str((empty / '.git').resolve()))

    async def test_linked_worktree_project_branches_from_its_own_head(self):
        linked = self.linked_worktree('feature')
        (linked / 'feature.md').write_text('feature\n')
        self.git('add', '.', cwd=linked)
        self.git('commit', '-qm', 'feature', cwd=linked)
        linked_head = self.git('rev-parse', 'HEAD', cwd=linked).stdout.strip()
        result = await prepare_task_workspace(linked, self.state, 12)
        self.assertEqual(result['base_commit'], linked_head)
        self.assertEqual(result['project_key'], str((self.project / '.git').resolve()))
        self.assertEqual(self.git('rev-parse', 'HEAD', cwd=result['cwd']).stdout.strip(), linked_head)

    async def test_project_identity_matches_for_canonical_and_task_worktree(self):
        workspace = await prepare_task_workspace(self.project / 'nested', self.state, 11)
        canonical = await project_identity(self.project / 'nested')
        isolated = await project_identity(Path(workspace['cwd']))
        self.assertEqual(canonical['project_key'], isolated['project_key'])
        self.assertEqual(isolated['base_commit'], self.base)

    async def test_task_worktree_adds_are_serialized_for_one_repository(self):
        real_git = workspaces._git
        active = 0
        peak = 0
        adds = 0

        async def tracked_git(cwd, *args, **kwargs):
            nonlocal active, peak, adds
            if args[:2] != ('worktree', 'add'):
                return await real_git(cwd, *args, **kwargs)
            adds += 1
            active += 1
            peak = max(peak, active)
            try:
                await asyncio.sleep(0.05)
                return await real_git(cwd, *args, **kwargs)
            finally:
                active -= 1

        with patch('coordinator.workspaces._git', new=tracked_git):
            first, second = await asyncio.gather(
                prepare_task_workspace(self.project, self.state, 1),
                prepare_task_workspace(self.project, self.state, 2))
        self.assertEqual(adds, 2)
        self.assertEqual(peak, 1)
        self.assertTrue(Path(first['cwd']).is_dir())
        self.assertTrue(Path(second['cwd']).is_dir())

        repeated = await asyncio.gather(
            prepare_task_workspace(self.project, self.state, 1),
            prepare_task_workspace(self.project, self.state, 1))
        self.assertEqual(repeated, [first, first])

    async def test_command_cancellation_terminates_child_process_group(self):
        """The background grandchild holds a file lock the kernel frees when it exits.

        `os.kill(pid, 0)` cannot prove this. It keeps answering for a process
        this test never reaps, and a freed process ID can come back.
        """
        lock = self.root / 'grandchild.lock'
        ready = self.root / 'grandchild.ready'
        program = ('import fcntl,sys,time\n'
                   'handle = open(sys.argv[1], "a")\n'
                   'fcntl.flock(handle, fcntl.LOCK_EX)\n'
                   'open(sys.argv[2], "w").write("up")\n'
                   'time.sleep(60)\n')
        script = self.root / 'blocking.sh'
        script.write_text('#!/usr/bin/env bash\n"$1" -c "$2" "$3" "$4" &\nexit 0\n')
        script.chmod(0o700)
        task = asyncio.create_task(
            _command('bash', script, sys.executable, program, lock, ready))
        await until_exists(ready)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await settle(task)
        await until_unlocked(lock, 'the cancelled process group to die')

    async def test_repeated_cancellation_waits_for_workspace_process_cleanup(self):
        ready = self.root / 'workspace.pid'
        terminated = self.root / 'term-seen'
        program = ('import os,signal,time; from pathlib import Path; '
                   'signal.signal(signal.SIGTERM, lambda *_: Path(' + repr(str(terminated)) + ').touch()); '
                   'Path(' + repr(str(ready)) + ').write_text(str(os.getpid())); time.sleep(60)')
        task = asyncio.create_task(_command(sys.executable, '-c', program))
        pid = None
        try:
            pid = int(await until_text(ready))
            task.cancel()
            await until_exists(terminated)
            task.cancel()
            await asyncio.sleep(.05)
            self.assertFalse(task.done())
            with self.assertRaises(asyncio.CancelledError):
                await settle(task)
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)
        finally:
            if pid:
                try:
                    os.killpg(pid, 9)
                except ProcessLookupError:
                    pass
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await asyncio.sleep(.05)


if __name__ == '__main__':
    unittest.main()
