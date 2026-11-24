"""Retire clean, merged Git worktrees in a job's shared scratch when a worker of the job ends.

While another worker of the job is unfinished, only a worktree that nothing wrote to for STALE seconds goes, so a
job that always has a worker does not keep every checkout it ever made.
"""

import asyncio
import os
from pathlib import Path
import time

from . import close_saves, sweeps, workspaces
from .external_worktrees import Policy, _latest_write, _process_paths, _safe_target, _within
from .idle_trim import NAMES, SCRATCH_NAMES


REGENERABLE = NAMES | SCRATCH_NAMES
TIMEOUT = 30
STALE = close_saves.PARK_SECONDS
MAINS = ('refs/remotes/origin/HEAD', 'refs/remotes/origin/main', 'refs/remotes/origin/master')


def _worktrees(scratch):
    found = []
    for directory, folders, files in os.walk(scratch, followlinks=False):
        folders[:] = sorted(name for name in folders if name != '.git' and name not in REGENERABLE)
        pointer = Path(directory) / '.git'
        if Path(directory) != scratch and '.git' in files and not pointer.is_symlink():
            found.append(Path(directory))
            folders[:] = []
    return found


def _nested(target):
    for directory, folders, files in os.walk(target, followlinks=False):
        if Path(directory) != target and ('.git' in files or '.git' in folders):
            return True
        folders[:] = [name for name in folders if name != '.git' and name not in REGENERABLE]
    return False


async def _busy(store, task_id, target, registered):
    active = False
    for worker in store.db.execute("SELECT task,cwd FROM workers WHERE status NOT IN ('done','interrupted')"):
        if worker['cwd'] and _within(Path(worker['cwd']).resolve(), target):
            return 'an unfinished worker uses the worktree'
        active = active or worker['task'] == task_id
    if active and time.time() - await _latest_write(target, registered) < STALE:
        return 'job has an unfinished worker'
    return None


async def _unsaved(target):
    _, status = await workspaces._git(target, 'status', '--porcelain=v1', '--untracked-files=all',
                                      '--ignore-submodules=none', '-z', strip=False, timeout=TIMEOUT)
    if status:
        return 'worktree has uncommitted or untracked changes'
    _, ignored = await workspaces._git(target, 'ls-files', '--others', '--ignored', '--exclude-standard',
                                       '--directory', '-z', strip=False, timeout=TIMEOUT)
    for name in filter(None, ignored.split('\0')):
        if not REGENERABLE.intersection(Path(name).parts) and not name.endswith('.pyc'):
            return 'ignored files need preservation'
    if await asyncio.to_thread(_nested, target):
        return 'nested repository needs preservation'
    return None


async def _unmerged(target):
    for ref in MAINS:
        code, _ = await workspaces._git(target, 'rev-parse', '--verify', '--quiet', ref + '^{commit}',
                                        check=False, timeout=TIMEOUT)
        if code == 0:
            break
    else:
        return 'no remote main branch'
    code, _ = await workspaces._git(target, 'merge-base', '--is-ancestor', 'HEAD', ref, check=False, timeout=TIMEOUT)
    return None if code == 0 else 'HEAD is not on the remote main branch'


def _referenced(store, task_id, target):
    state = Path(store.directory).resolve()
    names = {str(target), str(Path(store.directory) / target.relative_to(state)), target.relative_to(state).as_posix()}
    for row in store.db.execute('SELECT locator FROM keep_links'):
        if any(name in row['locator'] for name in names):
            return 'KEEP link uses worktree'
    candidates, _ = close_saves.proof_candidates(store, store.task_get(task_id), [target])
    if any(os.path.lexists(path) for path in candidates):
        return 'proof is inside worktree'
    if close_saves.pending_send(store, [target]):
        return 'queued delivery uses worktree'
    return None


async def _retire(store, task_id, scratch, target):
    reason = _safe_target(target, Policy((str(scratch),)), store.directory)
    if reason:
        return reason
    if workspaces.on_volume(store.directory, target) or target.stat().st_dev != scratch.stat().st_dev:
        return 'worktree is on another file system'
    _, git_dir = await workspaces._git(target, 'rev-parse', '--absolute-git-dir', timeout=TIMEOUT)
    _, common = await workspaces._git(target, 'rev-parse', '--git-common-dir', timeout=TIMEOUT)
    repository = workspaces._resolved_git_dir(target, common)
    if _within(repository, scratch):
        return 'repository is inside job scratch'
    async with workspaces._repository_lock(store.directory, str(repository), wait=False) as acquired:
        if not acquired:
            return 'repository lock is held'
        registered = await workspaces._registered_git_directory(repository, target, timeout=TIMEOUT)
        if registered != Path(git_dir).resolve():
            return 'worktree registration does not match checkout'
        records = [record for record in await workspaces._worktree_records(repository, timeout=TIMEOUT)
                   if Path(record['worktree']).resolve() == target]
        if len(records) != 1:
            return 'worktree is not registered once'
        if 'locked' in records[0] or (registered / 'locked').exists():
            return 'worktree is locked'
        if workspaces._workspace_operation(registered) or any(registered.glob('*.lock')):
            return 'Git operation or lock is present'
        reason = (await _busy(store, task_id, target, registered) or await _unsaved(target) or await _unmerged(target)
                  or _referenced(store, task_id, target))
        if reason:
            return reason
        if any(_within(path, target) or _within(path, registered) for path in await _process_paths()):
            return 'live process uses worktree'
        await workspaces._git(repository, 'worktree', 'remove', '--force', '--', target, timeout=TIMEOUT)
    return None


async def retire(store, task_id):
    """Return each scratch worktree with 'removed' or the reason it stays."""
    scratch = close_saves.scratch(store, task_id)
    if scratch.is_symlink() or not scratch.is_dir():
        return {}
    scratch = scratch.resolve()
    report = {}

    async def one(target):
        report[str(target)] = 'failed'
        report[str(target)] = await _retire(store, task_id, scratch, target) or 'removed'
    await sweeps.arun(store, 'scratch-worktree', await asyncio.to_thread(_worktrees, scratch), one, area='worker',
                      key=lambda target: '%s:%s' % (task_id, target.relative_to(scratch)),
                      refs=lambda target: {'task': task_id})
    return report
