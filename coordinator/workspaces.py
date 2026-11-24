"""Prepare task worktrees without changing the source checkout."""

import asyncio
import contextlib
import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
import shutil
import signal
import stat
import subprocess
import tempfile
import time
import uuid

from . import extension, host_os, scan_process
from .host_os import delete_tree_background as delete_tree
from .host_os import move_to_trash, trash_directory

logger = logging.getLogger(__name__)
FETCH_TIMEOUT = 30
LS_REMOTE_TIMEOUT = 10
DEFAULT_BRANCH_TTL = 300
_DEFAULT_BRANCHES = {}
GIT_TIMEOUT = 30
CLEANUP_INTERVAL = 60 * 60
LOCK_TURN = 0.1
COW_FOLDER = 'cow-worktrees'
COW_MIN_FREE = 5 * 1024 ** 3
COW_MIN_SHARE = 0.10
COW_IMAGE_FLOOR = 0.08
STOP_GRACE = 2
WORKTREE_OPERATIONS = ('rebase-merge', 'rebase-apply', 'MERGE_HEAD', 'CHERRY_PICK_HEAD',
                       'REVERT_HEAD', 'BISECT_LOG', 'BISECT_START', 'sequencer')


def _cleanup_problem(store, code, detail, level=logging.WARNING):
    from . import problems
    try:
        if store is None:
            logger.warning('worktree cleanup problem code=%s detail=%s', code, detail)
        else:
            problems.record(store, 'workspaces', code, detail, level=level, every=CLEANUP_INTERVAL)
    except Exception as error:
        try:
            logger.warning('worktree cleanup problem reporting failed code=%s type=%s', code, type(error).__name__)
        except Exception:
            pass


def repository_lock_path(state_dir, project_key):
    folder = Path(state_dir) / 'locks'
    folder.mkdir(parents=True, exist_ok=True, mode=0o700)
    return folder / ('worktree-' + hashlib.sha256(str(project_key).encode()).hexdigest()[:16] + '.lock')


@contextlib.asynccontextmanager
async def _repository_lock(state_dir, project_key, wait=True):
    """Serialize worktree changes to one repository across service and MCP processes.
    Git fails when two worktree changes overlap: a prune or add reads the other's
    half-written administrative folder."""
    fd = os.open(str(repository_lock_path(state_dir, project_key)), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if not wait:
                    yield False
                    return
                await asyncio.sleep(0.02)
        yield True
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


async def _stop_process_group(process, grace=STOP_GRACE):
    _signal_group(process, signal.SIGTERM)
    try:
        await asyncio.wait_for(process.wait(), grace)
    except asyncio.TimeoutError:
        pass
    _signal_group(process, signal.SIGKILL)
    if process.returncode is None:
        try:
            await asyncio.wait_for(process.wait(), 2)
        except asyncio.TimeoutError:
            pass


async def _stop_spawned(spawn, grace=STOP_GRACE):
    try:
        process = await spawn
    except Exception as error:
        logger.warning('workspace command did not start during stop type=%s', type(error).__name__)
        return
    await _stop_process_group(process, grace)


async def _command(*args, cwd=None, check=True, env=None, strip=True, reject_stderr=False, errors=False,
                   grace=STOP_GRACE):
    spawn = asyncio.ensure_future(asyncio.create_subprocess_exec(
        *map(str, args), cwd=str(cwd) if cwd else None, env=env,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        start_new_session=True))
    try:
        process = await asyncio.shield(spawn)
        stdout, stderr = await process.communicate()
    except asyncio.CancelledError:
        cleanup = asyncio.create_task(_stop_spawned(spawn, grace))
        while True:
            try:
                await asyncio.shield(cleanup)
                break
            except asyncio.CancelledError:
                if cleanup.cancelled():
                    raise
        raise
    text = stdout.decode(errors='replace')
    if strip:
        text = text.strip()
    if check and process.returncode:
        message = stderr.decode(errors='replace').strip() or text or 'command failed'
        raise RuntimeError(message)
    if reject_stderr and stderr.strip():
        raise RuntimeError('Git reported a warning: ' + stderr.decode(errors='replace').strip())
    if errors:
        return process.returncode, text, stderr.decode(errors='replace').strip()
    return process.returncode, text


async def _git(cwd, *args, check=True, env=None, timeout=None, strip=True, reject_stderr=False):
    command = _command('git', '-C', cwd, *args, check=check, env=env, strip=strip, reject_stderr=reject_stderr)
    return await command if timeout is None else await asyncio.wait_for(command, timeout)


def _resolved_git_dir(cwd, value):
    path = Path(value)
    return (path if path.is_absolute() else Path(cwd) / path).resolve()


async def _validate_existing(source_root, target, branch, *, timeout=None):
    if not target.is_dir():
        raise RuntimeError(f'Workspace path collision: {target}')
    code, root_text = await _git(target, 'rev-parse', '--show-toplevel', check=False, timeout=timeout)
    if code or Path(root_text).resolve() != target.resolve():
        raise RuntimeError(f'Workspace path collision: {target}')
    _, source_common = await _git(source_root, 'rev-parse', '--git-common-dir', timeout=timeout)
    _, target_common = await _git(target, 'rev-parse', '--git-common-dir', timeout=timeout)
    if _resolved_git_dir(source_root, source_common) != _resolved_git_dir(target, target_common):
        raise RuntimeError(f'Workspace belongs to another repository: {target}')
    code, current_branch = await _git(target, 'symbolic-ref', '--quiet', '--short', 'HEAD',
                                     check=False, timeout=timeout)
    if code or current_branch != branch:
        raise RuntimeError(f'Workspace branch collision: expected {branch}')
    _, head = await _git(target, 'rev-parse', 'HEAD', timeout=timeout)
    return head


def _shared_identity(project_cwd, project_key, reason):
    """Describe a project Torii must share instead of isolating, and say why."""
    path = str(project_cwd)
    return {'cwd': path, 'project_root': path, 'base_commit': None,
            'project_key': project_key, 'reason': reason}


async def project_identity(project_cwd: Path, *, timeout=None) -> dict:
    """Return JSON-safe identity shared by a Git repository and its worktrees."""
    project_cwd = Path(project_cwd).expanduser().resolve(strict=True)
    if not project_cwd.is_dir():
        raise ValueError('Project working directory must be a directory')
    try:
        _, common = await _git(project_cwd, 'rev-parse', '--git-common-dir', timeout=timeout)
    except RuntimeError as error:
        return _shared_identity(project_cwd, str(project_cwd),
                                f'Not a Git repository: {error}')
    project_key = str(_resolved_git_dir(project_cwd, common))
    try:
        _, root_text = await _git(project_cwd, 'rev-parse', '--show-toplevel', timeout=timeout)
    except RuntimeError as error:
        return _shared_identity(project_cwd, project_key,
                                f'Git repository has no work tree: {error}')
    try:
        _, base_commit = await _git(project_cwd, 'rev-parse', 'HEAD', timeout=timeout)
    except RuntimeError as error:
        return _shared_identity(project_cwd, project_key,
                                f'Git repository has no commit to branch from: {error}')
    return {'cwd': str(project_cwd), 'project_root': str(Path(root_text).resolve()),
            'base_commit': base_commit, 'project_key': project_key}


def ensure_project_commit(project_cwd: Path):
    """Give an empty folder or unborn repository a base without committing owner files."""
    path = Path(project_cwd)

    def git(*args, check=True, env=None):
        return subprocess.run(['git', '-C', str(path), *args], check=check, env=env,
                              capture_output=True, text=True, timeout=GIT_TIMEOUT)

    repository = git('rev-parse', '--is-inside-work-tree', check=False)
    if repository.returncode or repository.stdout.strip() != 'true':
        if not repository.returncode:
            raise ValueError('Job worktrees require a Git repository with a working tree.')
        if any(entry.name not in ('.DS_Store', '.localized') or not entry.is_file() or entry.is_symlink()
               for entry in path.iterdir()):
            raise ValueError('Job worktrees require a Git repository with at least one commit. '
                             'This folder contains files. In this folder, run git init, then choose '
                             'the files to track with git add <files> and run git commit -m "Initial commit". '
                             'Retry the job, or choose an empty project folder.')
        git('init', '-q')
    if not git('rev-parse', '--verify', 'HEAD^{commit}', check=False).returncode:
        return
    with tempfile.TemporaryDirectory(prefix='torii-initial-commit-') as temporary:
        env = dict(os.environ, GIT_INDEX_FILE=str(Path(temporary) / 'index'),
                   GIT_AUTHOR_NAME='Torii', GIT_AUTHOR_EMAIL='torii@localhost',
                   GIT_COMMITTER_NAME='Torii', GIT_COMMITTER_EMAIL='torii@localhost')
        git('-c', 'user.name=Torii', '-c', 'user.email=torii@localhost',
            '-c', 'core.hooksPath=/dev/null', '-c', 'commit.gpgSign=false',
            'commit', '--allow-empty', '-qm', 'Initial empty commit', env=env)


def _remote_env():
    return dict(os.environ, GIT_TERMINAL_PROMPT='0', GIT_SSH_COMMAND='ssh -o BatchMode=yes')


async def _remote_default(project_root, remote):
    """Return the remote's current default branch, asked fresh and kept briefly, or '' when it cannot be read."""
    key = (str(project_root), remote)
    cached = _DEFAULT_BRANCHES.get(key)
    if cached and time.monotonic() - cached[0] < DEFAULT_BRANCH_TTL:
        return cached[1]
    try:
        code, text = await asyncio.wait_for(_git(project_root, 'ls-remote', '--symref', '--', remote, 'HEAD',
                                                 check=False, env=_remote_env()), LS_REMOTE_TIMEOUT)
    except (asyncio.TimeoutError, OSError, RuntimeError):
        return ''
    name = next((line.split('\t')[0].removeprefix('ref: refs/heads/') for line in text.splitlines()
                 if line.startswith('ref: refs/heads/') and line.endswith('\tHEAD')), '') if not code else ''
    if name:
        _DEFAULT_BRANCHES[key] = (time.monotonic(), name)
    return name


async def _start_point(project_root, base):
    remote, upstream = '', ''
    code, current = await _git(project_root, 'symbolic-ref', '--quiet', 'HEAD', check=False)
    if not code:
        _, tracking = await _git(project_root, 'for-each-ref',
                                 '--format=%(upstream:remotename)%00%(upstream:remoteref)', current)
        remote, _, upstream = tracking.partition('\0')
    if base is None:
        _, configured = await _git(project_root, 'config', '--get', 'torii.defaultBase', check=False)
        if configured:
            base = configured
        elif remote or not (await _git(project_root, 'remote', 'get-url', 'origin', check=False))[0]:
            default = await _remote_default(project_root, remote or 'origin')
            if default:
                remote, upstream = remote or 'origin', 'refs/heads/' + default
    if base is None and not upstream:
        _, commit = await _git(project_root, 'rev-parse', 'HEAD')
        return {'base_ref': 'HEAD', 'base_commit': commit, 'fetched': False}
    remote = remote or 'origin'
    name = base if base is not None else upstream.removeprefix('refs/heads/')
    if base is not None:
        name = name.removeprefix(remote + '/')
        if remote != 'origin':
            name = name.removeprefix('origin/')
    code, _ = await _git(project_root, 'check-ref-format', 'refs/heads/' + name, check=False)
    if code:
        raise ValueError(f'Invalid base branch: {name}')
    base_ref = remote + '/' + name
    tracking_ref = 'refs/remotes/' + base_ref
    env = _remote_env()
    error = ''
    try:
        await asyncio.wait_for(_git(
            project_root, '-c', 'gc.auto=0', '-c', 'maintenance.auto=false',
            'fetch', '--no-tags', '--no-recurse-submodules', '--refmap=', '--', remote,
            f'+refs/heads/{name}:{tracking_ref}', env=env), FETCH_TIMEOUT)
    except asyncio.TimeoutError:
        error = f'git fetch timed out after {FETCH_TIMEOUT} seconds'
    except (OSError, RuntimeError) as failure:
        error = str(failure)
    code, commit = await _git(project_root, 'rev-parse', '--verify', tracking_ref + '^{commit}', check=False)
    if code:
        raise RuntimeError(f'Cannot start from {base_ref}: {error or "fetched branch has no commit"}')
    return {'base_ref': base_ref, 'base_commit': commit, 'fetched': not error}


def cow_root(state_dir):
    return Path(state_dir).resolve() / COW_FOLDER


def _mounted(path):
    return host_os.mount_point(path)


def volume_mounted(state_dir):
    return _mounted(cow_root(state_dir))


def on_volume(state_dir, path):
    return path is not None and Path(path).is_relative_to(cow_root(state_dir))


def check_volume(state_dir, path):
    """Refuse work on a volume tree while the volume is absent, so an empty mount point never reads as a cleared copy."""
    if on_volume(state_dir, path) and not volume_mounted(state_dir):
        raise ValueError('the copy-on-write worktree volume is not mounted')


def volume_side(state_dir, path, name):
    """Return the folder that holds moved data of path on path's own filesystem."""
    return cow_root(state_dir) / ('.' + name) if on_volume(state_dir, path) else Path(state_dir) / name


def trash_for(state_dir, path):
    if on_volume(state_dir, path):
        return host_os.volume_trash_directory(cow_root(state_dir))
    return trash_directory()


def _workspace_targets(state_dir, task_id):
    name = f'task-{task_id}'
    return Path(state_dir).resolve() / 'worktrees' / name, cow_root(state_dir) / name


def _workspace_target(state_dir, task_id, saved_cwd=None):
    """A job tree is on the copy-on-write volume when its folder is there; otherwise it is in worktrees."""
    plain, cow = _workspace_targets(state_dir, task_id)
    if saved_cwd is not None:
        return cow if Path(saved_cwd).is_relative_to(cow) else plain
    return cow if os.path.lexists(cow) else plain


def staged_copy(stage, record):
    return Path(record['copy']) if 'copy' in record else stage / 'copy'


def stage_fields(state_dir, target, stage):
    if not on_volume(state_dir, target):
        return {}
    return {'copy': str(cow_root(state_dir) / '.worktree-deletions' / stage.name / 'copy')}


def _cow_space(state_dir):
    """The volume needs free room, and so does the file system under its sparse image."""
    usage = shutil.disk_usage(cow_root(state_dir))
    if usage.free < COW_MIN_FREE or usage.free < usage.total * COW_MIN_SHARE:
        return 'the copy-on-write volume is low on space'
    info = os.stat(cow_root(state_dir))
    backing = Path('/sys/dev/block/%d:%d/loop/backing_file' % (os.major(info.st_dev), os.minor(info.st_dev)))
    if backing.is_file():
        image = Path(backing.read_text().strip())
        usage = shutil.disk_usage(image.parent)
        if usage.free < usage.total * COW_IMAGE_FLOOR:
            return 'the file system under the copy-on-write image is low on space'
    return None


async def _plain_worktree(project_root, target, commit, branch, new_branch):
    target.parent.mkdir(parents=True, exist_ok=True)
    await _git(project_root, 'worktree', 'add', *(('-b', branch, target, commit) if new_branch else (target, branch)))
    return target


async def _new_tree_target(state_dir, task_id, store):
    """A new job's tree is on the copy-on-write volume only while the volume is mounted and has room."""
    plain, cow = _workspace_targets(state_dir, task_id)
    if not volume_mounted(state_dir):
        return plain
    reason = await asyncio.to_thread(_cow_space, state_dir)
    if reason:
        _cleanup_problem(store, 'cow-worktree-fallback', reason)
        return plain
    return cow


async def _clear_failed_tree(state_dir, project_root, target, commit, branch, new_branch):
    """Undo what a failed or stopped tree maker call left, so git worktree add can run.
    Only the folder Torii chose for the call is cleared. A new branch goes only while it still
    points at its start commit and no worktree has it."""
    if any(Path(record['worktree']) == target for record in await _worktree_records(project_root, check=False)):
        await _git(project_root, 'worktree', 'remove', '--force', target, check=False, timeout=GIT_TIMEOUT)
    if os.path.lexists(target):
        try:
            move_to_trash(target, trash_for(state_dir, target), name=target.name + '.failed-tree')
        except OSError as error:
            raise RuntimeError('Cannot clear the failed job tree: ' + str(error)) from error
    if new_branch and not any(record.get('branch') == 'refs/heads/' + branch
                              for record in await _worktree_records(project_root, check=False)):
        await _git(project_root, 'update-ref', '-d', 'refs/heads/' + branch, commit, check=False, timeout=GIT_TIMEOUT)


async def _make_detached_worktree(store, repository, target, commit):
    """Try the job tree maker while the caller holds the repository lock."""
    state_dir = store.directory
    if store.get('cow_worktrees', False) is not True and not on_volume(state_dir, target):
        return False
    maker = extension.optional('tree_maker')
    if maker is None:
        return False
    records = await _worktree_records(repository, timeout=GIT_TIMEOUT)
    if any(Path(record['worktree']) == target for record in records):
        return False
    made = await maker.make(state_dir, repository, target, commit, None, False, store, lock_held=True)
    if made is None:
        await _clear_failed_tree(state_dir, repository, target, commit, None, False)
    return made is not None


def _saved_workspace_relative(target, saved_cwd):
    saved = Path(saved_cwd)
    if not saved.is_absolute() or saved.resolve() != saved:
        raise ValueError('The saved job worktree is not its original task folder.')
    try:
        return saved.relative_to(target)
    except ValueError as error:
        raise ValueError('The saved job worktree is not its original task folder.') from error


async def _finish_workspace(store, task_id, project_root, identity, target, branch, rebuilt, rebuild_started, start):
    commit = await _validate_existing(project_root, target, branch)
    if store is not None:
        from .close_saves import restore
        restored = await restore(store, task_id, target, Path(identity['project_key']))
        if rebuilt or restored:
            start['rebuilt'] = True
            with store.db:
                store.db.execute('UPDATE task_iterations SET rebuild_seconds=? WHERE task=? AND k=(SELECT iteration FROM tasks WHERE id=?)',
                                 (time.monotonic() - rebuild_started, task_id, task_id))
    return commit


async def prepare_task_workspace(project_cwd: Path, state_dir: Path, task_id: int, base=None,
                                 *, saved_cwd=None, store=None, cow=False) -> dict:
    """Name the branch and folder by the global task id, so topics sharing a repository never collide.
    With cow and the add-on tree maker, the maker makes the tree, on the copy-on-write volume when it is ready."""
    maker = extension.optional('tree_maker')
    cow = cow and maker is not None
    rebuild_started = time.monotonic()
    rebuilt = False
    identity = await project_identity(project_cwd)
    branch = f'torii/task-{task_id}'
    target = _workspace_target(state_dir, task_id, saved_cwd)
    relative_saved = _saved_workspace_relative(target, saved_cwd) if saved_cwd is not None else None
    check_volume(state_dir, target)
    lock_key = identity['project_key']
    if identity['base_commit'] is None and lock_key == identity['cwd']:
        lock_key = str(Path(lock_key) / '.git')
    pending = None
    async with _repository_lock(state_dir, lock_key):
        if identity['base_commit'] is None:
            try:
                await asyncio.to_thread(ensure_project_commit, Path(identity['cwd']))
            except subprocess.CalledProcessError as error:
                raise RuntimeError(error.stderr.strip() or str(error)) from error
            except subprocess.TimeoutExpired as error:
                raise RuntimeError('Project Git setup timed out.') from error
            identity = await project_identity(project_cwd)
            if identity['base_commit'] is None:
                raise ValueError(identity['reason'])
        project_root = Path(identity['project_root'])
        relative_cwd = (relative_saved if relative_saved is not None else
                        Path(identity['cwd']).relative_to(project_root))
        await _async_recover_staged_registrations(state_dir, identity['project_key'], store=store)
        start = {'base_ref': 'HEAD', 'fetched': False}
        if saved_cwd is not None and target.is_symlink():
            raise RuntimeError(f'Workspace path collision: {target}')
        if store is not None and saved_cwd is not None:
            from .close_saves import rebuild_branch
            saved_branch, restored = await rebuild_branch(store, task_id, target, Path(identity['project_key']))
            if saved_branch:
                branch = saved_branch
                rebuilt = restored
        if not target.exists():
            rebuilt = saved_cwd is not None
            records = await _worktree_records(project_root, check=saved_cwd is not None)
            for record in records:
                existing = Path(record['worktree'])
                if (record.get('branch') == 'refs/heads/' + branch
                        and (saved_cwd is None or existing != target)):
                    raise RuntimeError(f'Job branch is already used by {existing}')
            if saved_cwd is None:
                start = await _start_point(project_root, base)
                if cow:
                    target, pending = await _new_tree_target(state_dir, task_id, store), start['base_commit']
                else:
                    await _plain_worktree(project_root, target, start['base_commit'], branch, True)
            else:
                code, _ = await _git(project_root, 'show-ref', '--verify', '--quiet',
                                     'refs/heads/' + branch, check=False)
                if code:
                    raise RuntimeError('The saved job branch is missing.' if code == 1
                                       else 'Cannot check the saved job branch.')
                if any(Path(record['worktree']) == target for record in records):
                    git_dir = await _registered_git_directory(project_root, target)
                    try:
                        move_to_trash(git_dir, trash_directory(), name=target.name + '.git-worktree')
                    except OSError as error:
                        raise RuntimeError('Cannot preserve the saved worktree Git data: ' + str(error)) from error
                if cow or on_volume(state_dir, target):
                    _, pending = await _git(project_root, 'rev-parse', '--verify', 'refs/heads/' + branch + '^{commit}')
                else:
                    await _plain_worktree(project_root, target, None, branch, False)
        if pending is None:
            start['base_commit'] = await _finish_workspace(store, task_id, project_root, identity, target, branch,
                                                           rebuilt, rebuild_started, start)
    if pending is not None:
        new_branch = saved_cwd is None
        made = None if maker is None else await maker.make(state_dir, project_root, target, pending, branch, new_branch, store)
        async with _repository_lock(state_dir, lock_key):
            if made is None:
                await _clear_failed_tree(state_dir, project_root, target, pending, branch, new_branch)
                if new_branch:
                    target = Path(state_dir).resolve() / 'worktrees' / target.name
                await _plain_worktree(project_root, target, pending, branch, new_branch)
            start['base_commit'] = await _finish_workspace(store, task_id, project_root, identity, target, branch,
                                                           rebuilt, rebuild_started, start)
    workspace_cwd = target / relative_cwd
    workspace_cwd.mkdir(parents=True, exist_ok=True)
    return {'cwd': str(workspace_cwd), 'branch': branch, 'project_root': str(project_root),
            'project_key': identity['project_key'], 'isolated': True, **start}


async def _worktree_records(repository, *, check=True, timeout=None):
    code, listing = await _git(repository, 'worktree', 'list', '--porcelain', check=check, timeout=timeout)
    if code:
        return []
    records, record = [], {}
    for field in listing.splitlines():
        if not field:
            if record:
                records.append(record)
                record = {}
        else:
            key, _, value = field.partition(' ')
            record[key] = value
    if record:
        records.append(record)
    return records


async def _registered_git_directory(repository, target, *, timeout=None):
    _, common = await _git(repository, 'rev-parse', '--git-common-dir', timeout=timeout)
    directory = _resolved_git_dir(repository, common) / 'worktrees'
    matches = []
    if directory.is_dir():
        for candidate in directory.iterdir():
            if not candidate.is_dir() or candidate.is_symlink():
                continue
            pointer = candidate / 'gitdir'
            if pointer.is_file() and _resolved_git_dir(candidate, pointer.read_text().strip()) == target / '.git':
                matches.append(candidate)
    if len(matches) != 1:
        raise RuntimeError('Cannot identify exactly one Git data folder for the saved worktree.')
    return matches[0]


def _workspace_operation(git_dir):
    return next((name for name in WORKTREE_OPERATIONS
                 if (git_dir / name).exists() or (git_dir / name).is_symlink()), None)


def _save_deletion(stage, record):
    temporary = stage / 'record.tmp'
    with temporary.open('w') as handle:
        json.dump(record, handle)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(stage / 'record.json')
    fd = os.open(str(stage), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


async def _save_deletion_bounded(store, stage, record):
    try:
        await scan_process.run_async(_save_deletion, stage, record)
    except Exception as error:
        _cleanup_problem(store, 'closed-copy-record-write-failed', type(error).__name__)
        raise scan_process.ScanError('worktree deletion record write failed') from error


def _prepare_deletion_stage(state_dir, stage, record):
    root = Path(state_dir) / 'worktree-deletions'
    preparing = Path(state_dir) / 'worktree-deletion-preparing'
    if stage.parent != root or root.is_symlink() or preparing.is_symlink():
        raise ValueError('Unsafe deletion stage path')
    root.mkdir(mode=0o700, exist_ok=True)
    preparing.mkdir(mode=0o700, exist_ok=True)
    temporary = preparing / (stage.name + '-' + uuid.uuid4().hex)
    temporary.mkdir(mode=0o700)
    _save_deletion(temporary, record)
    if stage.exists() or stage.is_symlink():
        raise ValueError('Deletion stage already exists')
    if 'copy' in record:
        copy = Path(record['copy'])
        if copy != cow_root(state_dir) / '.worktree-deletions' / stage.name / 'copy':
            raise ValueError('Unsafe volume deletion stage path')
        copy.parent.parent.mkdir(mode=0o700, exist_ok=True)
        if copy.parent.parent.is_symlink():
            raise ValueError('Unsafe volume deletion stage path')
        copy.parent.mkdir(mode=0o700)
    temporary.rename(stage)
    for directory in (preparing, root):
        fd = os.open(str(directory), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    return stage


async def async_prepare_deletion_stage(store, stage, record):
    check_volume(store.directory, record.get('copy'))
    try:
        return await scan_process.run_async(_prepare_deletion_stage, store.directory, Path(stage), record)
    except Exception as error:
        _cleanup_problem(store, 'closed-copy-record-write-failed', type(error).__name__)
        raise scan_process.ScanError('worktree deletion stage preparation failed') from error


def _check_finalization_stage(stage):
    if not stat.S_ISDIR(stage.lstat().st_mode):
        raise ValueError('Deletion finalization stage is not a directory')
    with os.scandir(stage) as entries:
        for entry in entries:
            if entry.name not in ('record.json', 'record.tmp') or not entry.is_file(follow_symlinks=False):
                raise ValueError('Deletion finalization stage contains unexpected data')


def _move_finalization_stage(state_dir, stage):
    root = Path(state_dir) / 'worktree-deletion-finalizing'
    if stage.parent != Path(state_dir) / 'worktree-deletions' or root.is_symlink():
        raise ValueError('Unsafe deletion finalization path')
    _check_finalization_stage(stage)
    root.mkdir(mode=0o700, exist_ok=True)
    destination = root / stage.name
    if destination.exists() or destination.is_symlink():
        raise ValueError('Deletion finalization stage already exists')
    stage.rename(destination)
    return destination


def _scan_finalization_stages(state_dir):
    root = Path(state_dir) / 'worktree-deletion-finalizing'
    try:
        info = root.lstat()
    except FileNotFoundError:
        return []
    if not stat.S_ISDIR(info.st_mode):
        raise ValueError('Deletion finalization folder is not a directory')
    stages = sorted(root.iterdir())
    for stage in stages:
        _check_finalization_stage(stage)
    return stages


def _remove_volume_stage(record):
    folder = Path(record['copy']).parent
    if os.path.lexists(folder / 'copy'):
        raise ValueError('Staged volume copy still exists')
    with contextlib.suppress(FileNotFoundError):
        folder.rmdir()


async def _finish_stage(store, stage, options, record):
    if 'copy' in record:
        _require_mount(record)
        await scan_process.run_async(_remove_volume_stage, record)
    moved = await scan_process.run_async(_move_finalization_stage, store.directory, stage)
    await delete_tree(moved, **options)
    with store.db:
        store.db.execute('DELETE FROM settings WHERE key=?', ('closed_copy_finalized:' + stage.name,))


async def _recover_finalization_stages(store):
    """Finish each half-deleted stage. True once none is left waiting after a failure."""
    from . import sweeps

    async def finish(stage):
        await delete_tree(stage)
        with store.db:
            store.db.execute('DELETE FROM settings WHERE key=?', ('closed_copy_finalized:' + stage.name,))
    try:
        stages = await scan_process.run_async(_scan_finalization_stages, store.directory)
    except Exception as error:
        _cleanup_problem(store, 'closed-copy-finalization-failed', sweeps.describe(error))
        return False
    await sweeps.arun(store, 'closed-copy-finalization', stages, finish, area='workspaces', key=lambda stage: stage.name)
    return not any(sweeps.waiting(store, 'closed-copy-finalization', stage.name) for stage in stages)


async def _account_completed_stage(store, stage, record):
    from . import close_saves
    key = 'closed_copy_finalized:' + stage.name
    if store.db.execute('SELECT 1 FROM settings WHERE key=?', (key,)).fetchone():
        return True
    if 'space_k' in record:
        task = {'id': record['task'], 'iteration': record['space_k']}
        stamp = close_saves.checkpoint_stamp(store, task)
        phase, saved = await close_saves.async_load(store, task)
        saved.setdefault('freed', dict.fromkeys(('copy', 'deps', 'extras', 'prune'), 0))
        saved['freed']['copy'] += record['removed']['bytes'] - saved['freed']['deps']
        encoded = await close_saves.async_encode_checkpoint(store, saved)
    with store.db:
        if not store.db.in_transaction:
            store.db.execute('BEGIN IMMEDIATE')
        if store.db.execute('SELECT 1 FROM settings WHERE key=?', (key,)).fetchone():
            return True
        if 'space_k' in record:
            if not close_saves.references_current(store, stamp):
                return False
            store.db.execute('UPDATE task_iterations SET bytes_freed=COALESCE(bytes_freed,0)+? WHERE task=? AND k=?',
                             (record['removed']['bytes'], record['task'], record['space_k']))
            close_saves.checkpoint_encoded(store, task, phase, encoded)
        else:
            store.put('closed_copy_state:' + str(record['task']), {'state': 'gone', 'reason': ''})
        store.put(key, True)
    return True


def _deletion_stages(state_dir, on_error=None):
    root = Path(state_dir) / 'worktree-deletions'
    try:
        try:
            root_info = root.lstat()
        except FileNotFoundError:
            return
        if not stat.S_ISDIR(root_info.st_mode):
            raise ValueError('Deletion staging folder is not a directory')
        stages = sorted(root.iterdir())
    except (OSError, ValueError) as error:
        if on_error:
            on_error(root, error)
        return
    for stage in stages:
        try:
            info = stage.lstat()
            if not stat.S_ISDIR(info.st_mode):
                raise ValueError('Deletion stage is not a directory')
            pathname = stage / 'record.json'
            fd = os.open(str(pathname), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(fd) as handle:
                info = os.fstat(handle.fileno())
                if not stat.S_ISREG(info.st_mode):
                    raise ValueError('Deletion stage record is not a regular file')
                record = json.load(handle)
            if (not isinstance(record, dict)
                    or any(type(record.get(key)) is not int for key in ('task', 'number'))
                    or any(not isinstance(record.get(key), str) for key in ('target', 'repository', 'git_dir', 'branch'))
                    or type(record.get('detached')) is not bool
                    or not isinstance(record.get('git_inode'), list) or len(record['git_inode']) != 2
                    or any(type(value) is not int for value in record['git_inode'])
                    or not isinstance(record.get('ignored'), list)
                    or any(not isinstance(value, str) for value in record['ignored'])
                    or not isinstance(record.get('removed'), dict)
                    or any(type(record['removed'].get(key)) is not int or record['removed'][key] < 0
                           for key in ('bytes', 'files'))):
                raise ValueError('Staged deletion record is incomplete')
            targets = _workspace_targets(state_dir, record['task'])
            if Path(record['target']) not in targets:
                raise ValueError('Staged deletion has an unsafe task path')
            if (('copy' in record) != (Path(record['target']) == targets[1]) or 'copy' in record
                    and record['copy'] != str(cow_root(state_dir) / '.worktree-deletions' / stage.name / 'copy')):
                raise ValueError('Staged deletion has an unsafe copy path')
            git_dir = Path(record['git_dir'])
            if git_dir.parent != Path(record['repository']) / 'worktrees':
                raise ValueError('Staged deletion has an unsafe Git data path')
            yield stage, record
        except FileNotFoundError as error:
            if stage.exists() or stage.is_symlink():
                if on_error:
                    on_error(stage, error)
        except Exception as error:
            if on_error:
                on_error(stage, error)


def _scan_deletion_stages(state_dir):
    invalid = [None, 0]
    def record_error(stage, error):
        if invalid[0] is None:
            invalid[0] = (stage, type(error).__name__, str(error)[:500])
        invalid[1] += 1
    return list(_deletion_stages(state_dir, on_error=record_error)), invalid


async def _async_deletion_stages(state_dir, store=None):
    try:
        stages, invalid = await scan_process.run_async(_scan_deletion_stages, Path(state_dir))
    except Exception as error:
        _cleanup_problem(store, 'closed-copy-scan-failed', type(error).__name__)
        raise scan_process.ScanError('worktree deletion scan failed') from error
    representative, count = invalid
    if representative is not None:
        stage, kind, detail = representative
        _cleanup_problem(store, 'closed-copy-record-invalid',
                         str(stage) + ': ' + detail + ' invalid_records=' + str(count))
        raise scan_process.ScanError('worktree deletion record is invalid')
    if store is not None:
        stages = [(stage, record) for stage, record in stages
                  if not store.get('closed_copy_retired:' + stage.name, False)]
    return stages


async def async_deletion_stages(store):
    return await _async_deletion_stages(store.directory, store)


def _require_mount(record):
    if 'copy' in record and not _mounted(Path(record['copy']).parents[2]):
        raise ValueError('the copy-on-write worktree volume is not mounted')


def _detach_staged(stage, record):
    if record['detached'] or not staged_copy(stage, record).is_dir():
        return
    git_dir = Path(record['git_dir'])
    if not (stage / 'git').exists():
        try:
            info = git_dir.lstat()
        except FileNotFoundError:
            pass
        else:
            if (info.st_dev, info.st_ino) != tuple(record['git_inode']) or not stat.S_ISDIR(info.st_mode):
                raise ValueError('Staged deletion Git data folder changed')
            if (git_dir / 'gitdir').read_text().strip() != str(Path(record['target']) / '.git'):
                raise ValueError('Staged deletion Git registration changed')
            git_dir.rename(stage / 'git')
    record['detached'] = True
    _save_deletion(stage, record)


def _detach_scan_stage(stage, record):
    _detach_staged(stage, record)
    return stage, record


async def _async_detach_staged(stage, record, store=None):
    try:
        _, updated = await scan_process.run_async(_detach_scan_stage, stage, record)
        return updated
    except Exception as error:
        _cleanup_problem(store, 'closed-copy-recovery-failed', type(error).__name__)
        raise scan_process.ScanError('worktree registration recovery failed') from error


async def async_detach_staged(store, stage, record):
    return await _async_detach_staged(stage, record, store)


async def _async_recover_staged_registrations(state_dir, repository, store=None):
    stages = await _async_deletion_stages(state_dir, store)
    recovered = []
    for stage, record in stages:
        if record['repository'] != str(repository):
            continue
        if record.get('restoring'):
            _cleanup_problem(store, 'closed-copy-recovery-failed', 'restoration is incomplete')
            raise scan_process.ScanError('worktree restoration is incomplete')
        if record['detached']:
            continue
        recovered.append((stage, await _async_detach_staged(stage, record, store)))
    return recovered


async def async_recover_staged_registrations(store, repository):
    return await _async_recover_staged_registrations(store.directory, repository, store)


async def _delete_stage(store, stage, record):
    """Delete one staged copy. A failure keeps its reason on the stage and on the job, then goes to the sweep."""
    from . import close_saves
    try:
        if store.db.execute('SELECT 1 FROM settings WHERE key=?', ('closed_copy_finalized:' + stage.name,)).fetchone():
            await _finish_stage(store, stage, {}, record)
            return
        async with _repository_lock(store.directory, record['repository']):
            record = await async_detach_staged(store, stage, record)
        if not record['detached']:
            await _finish_stage(store, stage, {}, record)
            return
        options = {'paced': True} if 'space_k' in record else {}
        async with _repository_lock(store.directory, record['repository']):
            if not await close_saves.verify_staged(store, stage, record):
                return
        await delete_tree(staged_copy(stage, record), progress=record['removed'], **options)
        if 'space_k' in record:
            await delete_tree(stage / 'git', progress=record['removed'], **options)
        else:
            await delete_tree(stage / 'git')
        await _save_deletion_bounded(store, stage, record)
        if not await _account_completed_stage(store, stage, record):
            return
        await _finish_stage(store, stage, options, record)
    except Exception as error:
        reason = ' '.join(str(error).splitlines()) or type(error).__name__
        record['error'] = reason
        if stage.exists():
            await _save_deletion_bounded(store, stage, record)
        if isinstance(error, close_saves.ParkInUse):
            task = store.task_get(record['task'])
            if task and task['iteration'] == record['space_k']:
                close_saves.hold(store, task, error)
            raise
        with store.db:
            if not store.db.execute('SELECT 1 FROM settings WHERE key=?',
                                    ('closed_copy_finalized:' + stage.name,)).fetchone():
                store.put('closed_copy_state:' + str(record['task']),
                          {'state': 'staged', 'reason': reason, 'stage': str(stage)})
        raise
    logger.info('worktree cleanup deleted job=%s folder=%s bytes=%s files=%s ignored=%s',
                record['number'], record['target'], record['removed']['bytes'],
                record['removed']['files'], json.dumps(record['ignored']))


async def cleanup_closed_copies(store, *, sweep=False, retry_failed=True):
    """One service consumer drains durable events and crash leftovers without an age limit."""
    from . import close_saves, sweeps

    async with _repository_lock(store.directory, 'closed-copy-deletion'):
        if not await _recover_finalization_stages(store):
            return False
        try:
            await async_deletion_stages(store)
        except scan_process.ScanError:
            return False
        if sweep:
            await close_saves.retry_holds(store)
        deadline = time.monotonic() + close_saves.DRAIN_SECONDS
        more = await close_saves.drain(store)
    while more and time.monotonic() < deadline:
        await asyncio.sleep(LOCK_TURN)
        async with _repository_lock(store.directory, 'closed-copy-deletion'):
            more = await close_saves.drain(store)
    if more:
        logger.info('close save drain paused at its %s s budget; the rest waits for the next pass',
                    close_saves.DRAIN_SECONDS)
    async with _repository_lock(store.directory, 'closed-copy-deletion'):
        store.db.commit()
        try:
            stages = await async_deletion_stages(store)
        except scan_process.ScanError:
            return False
        await sweeps.arun(store, 'closed-copy-delete', [
            (stage, record) for stage, record in stages
            if 'space_k' in record or not record.get('error') or sweep and retry_failed],
            lambda item: _delete_stage(store, *item), area='workspaces', key=lambda item: item[0].name,
            refs=lambda item: {'task': item[1]['task']})
        await close_saves.finish(store)
        return more and close_saves.pending(store)
