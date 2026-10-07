"""Prepare task worktrees without changing the source checkout."""

import asyncio
import contextlib
import fcntl
import hashlib
import logging
import os
from pathlib import Path
import signal

logger = logging.getLogger(__name__)


@contextlib.asynccontextmanager
async def _repository_lock(state_dir, project_key):
    """Serialize worktree changes to one repository across service and MCP processes.
    Git fails when two worktree changes overlap: a prune or add reads the other's
    half-written administrative folder."""
    folder = Path(state_dir) / 'locks'
    folder.mkdir(parents=True, exist_ok=True, mode=0o700)
    name = 'worktree-' + hashlib.sha256(str(project_key).encode()).hexdigest()[:16] + '.lock'
    fd = os.open(str(folder / name), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                await asyncio.sleep(0.02)
        yield
    finally:
        os.close(fd)


def _signal_group(process, number):
    """A refused signal is a fact to record, not a reason to abandon the cleanup.

    This runs inside a shielded cleanup during cancellation. A PermissionError
    raised here would replace the caller's CancelledError and escape _command,
    leaving the rest of the cleanup undone.
    """
    try:
        os.killpg(process.pid, number)
    except ProcessLookupError:
        pass
    except PermissionError:
        logger.warning('cannot signal workspace process group pid=%s signal=%s', process.pid, number)


async def _stop_process_group(process):
    _signal_group(process, signal.SIGTERM)
    try:
        await asyncio.wait_for(process.wait(), 2)
    except asyncio.TimeoutError:
        pass
    _signal_group(process, signal.SIGKILL)
    if process.returncode is None:
        try:
            await asyncio.wait_for(process.wait(), 2)
        except asyncio.TimeoutError:
            pass


async def _stop_spawned(spawn):
    try:
        process = await spawn
    except Exception as error:
        logger.warning('workspace command did not start during stop type=%s', type(error).__name__)
        return
    await _stop_process_group(process)


async def _command(*args, cwd=None, check=True):
    spawn = asyncio.ensure_future(asyncio.create_subprocess_exec(
        *map(str, args), cwd=str(cwd) if cwd else None,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        start_new_session=True))
    try:
        process = await asyncio.shield(spawn)
        stdout, stderr = await process.communicate()
    except asyncio.CancelledError:
        cleanup = asyncio.create_task(_stop_spawned(spawn))
        while True:
            try:
                await asyncio.shield(cleanup)
                break
            except asyncio.CancelledError:
                if cleanup.cancelled():
                    raise
        raise
    text = stdout.decode(errors='replace').strip()
    if check and process.returncode:
        message = stderr.decode(errors='replace').strip() or text or 'command failed'
        raise RuntimeError(message)
    return process.returncode, text


async def _git(cwd, *args, check=True):
    return await _command('git', '-C', cwd, *args, check=check)


def _resolved_git_dir(cwd, value):
    path = Path(value)
    return (path if path.is_absolute() else Path(cwd) / path).resolve()


async def _validate_existing(source_root, target, branch):
    if not target.is_dir():
        raise RuntimeError(f'Workspace path collision: {target}')
    code, root_text = await _git(target, 'rev-parse', '--show-toplevel', check=False)
    if code or Path(root_text).resolve() != target.resolve():
        raise RuntimeError(f'Workspace path collision: {target}')
    _, source_common = await _git(source_root, 'rev-parse', '--git-common-dir')
    _, target_common = await _git(target, 'rev-parse', '--git-common-dir')
    if _resolved_git_dir(source_root, source_common) != _resolved_git_dir(target, target_common):
        raise RuntimeError(f'Workspace belongs to another repository: {target}')
    code, current_branch = await _git(target, 'symbolic-ref', '--quiet', '--short', 'HEAD', check=False)
    if code or current_branch != branch:
        raise RuntimeError(f'Workspace branch collision: expected {branch}')
    _, head = await _git(target, 'rev-parse', 'HEAD')
    return head


def _shared_identity(project_cwd, project_key, reason):
    """Describe a project Torii must share instead of isolating, and say why."""
    path = str(project_cwd)
    return {'cwd': path, 'project_root': path, 'base_commit': None,
            'project_key': project_key, 'reason': reason}


async def project_identity(project_cwd: Path) -> dict:
    """Return JSON-safe identity shared by a Git repository and its worktrees."""
    project_cwd = Path(project_cwd).expanduser().resolve(strict=True)
    if not project_cwd.is_dir():
        raise ValueError('Project working directory must be a directory')
    try:
        _, common = await _git(project_cwd, 'rev-parse', '--git-common-dir')
    except RuntimeError as error:
        return _shared_identity(project_cwd, str(project_cwd),
                                f'Not a Git repository: {error}')
    project_key = str(_resolved_git_dir(project_cwd, common))
    try:
        _, root_text = await _git(project_cwd, 'rev-parse', '--show-toplevel')
    except RuntimeError as error:
        return _shared_identity(project_cwd, project_key,
                                f'Git repository has no work tree: {error}')
    try:
        _, base_commit = await _git(project_cwd, 'rev-parse', 'HEAD')
    except RuntimeError as error:
        return _shared_identity(project_cwd, project_key,
                                f'Git repository has no commit to branch from: {error}')
    return {'cwd': str(project_cwd), 'project_root': str(Path(root_text).resolve()),
            'base_commit': base_commit, 'project_key': project_key}


async def prepare_task_workspace(project_cwd: Path, state_dir: Path, task_id: int) -> dict:
    """Name the branch and folder by the global task id, so topics sharing a repository never collide."""
    identity = await project_identity(project_cwd)
    if identity['base_commit'] is None:
        raise ValueError('Job worktrees require a Git repository with at least one commit. '
                         'Ask the owner before initializing this folder.')
    project_root = Path(identity['project_root'])
    relative_cwd = Path(identity['cwd']).relative_to(project_root)
    branch = f'torii/task-{task_id}'
    target = Path(state_dir).resolve() / 'worktrees' / f'task-{task_id}'
    async with _repository_lock(state_dir, identity['project_key']):
        if not target.exists():
            existing = await _registered_worktree(project_root, branch)
            if existing is not None:
                raise RuntimeError(f'Job branch is already used by {existing}')
            target.parent.mkdir(parents=True, exist_ok=True)
            await _git(project_root, 'worktree', 'add', '-b', branch, target, identity['base_commit'])
        await _validate_existing(project_root, target, branch)
    workspace_cwd = target / relative_cwd
    workspace_cwd.mkdir(parents=True, exist_ok=True)
    return {'cwd': str(workspace_cwd), 'branch': branch, 'project_root': str(project_root),
            'base_commit': identity['base_commit'], 'project_key': identity['project_key'],
            'isolated': True}


async def _registered_worktree(repository, branch):
    """Return the worktree the repository lists for a branch, or None."""
    code, listing = await _git(repository, 'worktree', 'list', '--porcelain', check=False)
    if code:
        return None
    path = None
    for line in listing.splitlines():
        if line.startswith('worktree '):
            path = line[len('worktree '):]
        elif line == f'branch refs/heads/{branch}' and path:
            return Path(path)
    return None
