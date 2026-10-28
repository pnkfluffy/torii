"""Prepare task worktrees without changing the source checkout."""

import asyncio
import contextlib
import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
import signal
import stat
import subprocess
import tempfile
import time
import uuid

from .host_os import delete_tree_background as delete_tree
from .host_os import move_to_trash, trash_directory

logger = logging.getLogger(__name__)
FETCH_TIMEOUT = 30
GIT_TIMEOUT = 30
CLEANUP_CURSOR_SETTING = 'closed_worktree_cleanup_cursor'
CLEANUP_AGE = 24 * 60 * 60
CLEANUP_LIMIT = 10
CLEANUP_INTERVAL = 60 * 60
WORKTREE_OPERATIONS = ('rebase-merge', 'rebase-apply', 'MERGE_HEAD', 'CHERRY_PICK_HEAD',
                       'REVERT_HEAD', 'BISECT_LOG', 'BISECT_START', 'sequencer')


@contextlib.asynccontextmanager
async def _repository_lock(state_dir, project_key, wait=True):
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


async def _command(*args, cwd=None, check=True, env=None, strip=True, reject_stderr=False):
    spawn = asyncio.ensure_future(asyncio.create_subprocess_exec(
        *map(str, args), cwd=str(cwd) if cwd else None, env=env,
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
    text = stdout.decode(errors='replace')
    if strip:
        text = text.strip()
    if check and process.returncode:
        message = stderr.decode(errors='replace').strip() or text or 'command failed'
        raise RuntimeError(message)
    if reject_stderr and stderr.strip():
        raise RuntimeError('Git reported a warning: ' + stderr.decode(errors='replace').strip())
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


async def _start_point(project_root, base):
    remote, upstream = '', ''
    code, current = await _git(project_root, 'symbolic-ref', '--quiet', 'HEAD', check=False)
    if not code:
        _, tracking = await _git(project_root, 'for-each-ref',
                                 '--format=%(upstream:remotename)%00%(upstream:remoteref)', current)
        remote, _, upstream = tracking.partition('\0')
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
    env = dict(os.environ, GIT_TERMINAL_PROMPT='0', GIT_SSH_COMMAND='ssh -o BatchMode=yes')
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


def _workspace_target(state_dir, task_id):
    return Path(state_dir).resolve() / 'worktrees' / f'task-{task_id}'


def _saved_workspace_relative(target, saved_cwd):
    saved = Path(saved_cwd)
    if not saved.is_absolute() or saved.resolve() != saved:
        raise ValueError('The saved job worktree is not its original task folder.')
    try:
        return saved.relative_to(target)
    except ValueError as error:
        raise ValueError('The saved job worktree is not its original task folder.') from error


async def prepare_task_workspace(project_cwd: Path, state_dir: Path, task_id: int, base=None,
                                 *, saved_cwd=None, store=None) -> dict:
    """Name the branch and folder by the global task id, so topics sharing a repository never collide."""
    rebuild_started = time.monotonic()
    rebuilt = False
    identity = await project_identity(project_cwd)
    branch = f'torii/task-{task_id}'
    target = _workspace_target(state_dir, task_id)
    relative_saved = _saved_workspace_relative(target, saved_cwd) if saved_cwd is not None else None
    lock_key = identity['project_key']
    if identity['base_commit'] is None and lock_key == identity['cwd']:
        lock_key = str(Path(lock_key) / '.git')
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
        _recover_staged_registrations(state_dir, identity['project_key'])
        start = {'base_ref': 'HEAD', 'fetched': False}
        if saved_cwd is not None and target.is_symlink():
            raise RuntimeError(f'Workspace path collision: {target}')
        if store is not None and store.get('close_saves_any_branch', False) is True:
            from .close_saves import rebuild_branch, enabled
            if enabled(store) and saved_cwd is not None:
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
                target.parent.mkdir(parents=True, exist_ok=True)
                await _git(project_root, 'worktree', 'add', '-b', branch, target, start['base_commit'])
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
                target.parent.mkdir(parents=True, exist_ok=True)
                await _git(project_root, 'worktree', 'add', target, branch)
        start['base_commit'] = await _validate_existing(project_root, target, branch)
        if store is not None:
            from .close_saves import restore, enabled
            if enabled(store):
                restored = await restore(store, task_id, target, Path(identity['project_key']))
                if rebuilt or restored:
                    start['rebuilt'] = True
                    with store.db:
                        store.db.execute('UPDATE task_iterations SET rebuild_seconds=? WHERE task=? AND k=(SELECT iteration FROM tasks WHERE id=?)',
                                         (time.monotonic() - rebuild_started, task_id, task_id))
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


def _cleanup_task_reason(store, task, target, now, *, immediate=False):
    if not task or task['status'] not in ('done', 'dropped'):
        return 'job is open or missing'
    if not immediate and now - task['updated'] < CLEANUP_AGE:
        return 'job closed less than 24 hours ago'
    if not task['worktree']:
        return 'job has no saved worktree'
    _saved_workspace_relative(target, task['worktree'])
    if target.resolve() != target or not stat.S_ISDIR(target.lstat().st_mode):
        return 'task folder is a symlink or is not a directory'
    if not Path(task['worktree']).is_dir():
        return 'saved working directory is missing'
    for worker in store.db.execute("SELECT task,cwd FROM workers WHERE status NOT IN ('done','interrupted')"):
        if worker['task'] == task['id']:
            return 'job has an unfinished worker'
        if worker['cwd'] and Path(worker['cwd']).resolve().is_relative_to(target):
            return 'an unfinished worker uses the task folder'
    for row in store.db.execute('SELECT image FROM outbox WHERE delivered=0 AND retired=0 AND image IS NOT NULL'):
        if row['image'] and Path(row['image']).resolve().is_relative_to(target):
            return "a Telegram file queued from it hasn't been sent yet"
    return None


def _workspace_operation(git_dir):
    return next((name for name in WORKTREE_OPERATIONS
                 if (git_dir / name).exists() or (git_dir / name).is_symlink()), None)


async def _cleanup_git_directory(repository, target):
    records = [record for record in await _worktree_records(repository, timeout=GIT_TIMEOUT)
               if Path(record['worktree']) == target]
    if len(records) != 1 or not records[0].get('branch', '').startswith('refs/heads/'):
        raise ValueError('worktree registration has no attached local branch')
    branch_ref = records[0]['branch']
    if 'locked' in records[0]:
        raise ValueError('worktree is locked')
    await _validate_existing(repository, target, branch_ref.removeprefix('refs/heads/'), timeout=GIT_TIMEOUT)
    _, git_dir = await _git(target, 'rev-parse', '--git-dir', timeout=GIT_TIMEOUT)
    git_dir = _resolved_git_dir(target, git_dir)
    if git_dir != await _registered_git_directory(repository, target, timeout=GIT_TIMEOUT):
        raise ValueError('worktree Git data folder does not match its registration')
    submodule_layout = ((target / '.gitmodules').exists(), (git_dir / 'modules').exists())
    operation = _workspace_operation(git_dir)
    if operation:
        raise ValueError('Git operation is in progress: ' + operation)
    _, changes = await _git(target, 'status', '--porcelain', '--untracked-files=all',
                            '--ignore-submodules=all', env=dict(os.environ, GIT_OPTIONAL_LOCKS='0'),
                            timeout=GIT_TIMEOUT, reject_stderr=True)
    if changes:
        raise ValueError('worktree has tracked or untracked changes')
    _, flags = await _git(target, 'ls-files', '-v', '-z', timeout=GIT_TIMEOUT, strip=False)
    if any(entry and (entry[0] == 'S' or entry[0].islower()) for entry in flags.split('\0')):
        raise ValueError('worktree has skip-worktree or assume-unchanged entries that can hide edits')
    _, entries = await _git(target, 'ls-files', '--stage', '-z', timeout=GIT_TIMEOUT, strip=False)
    if not any(submodule_layout) and any(entry.startswith('160000 ') for entry in entries.split('\0')):
        raise ValueError('submodule metadata is missing')
    if submodule_layout != ((target / '.gitmodules').exists(), (git_dir / 'modules').exists()):
        raise ValueError('Submodule layout changed during cleanup')
    return git_dir, branch_ref


async def cleanup_task_workspace(store, task_id, now=None):
    """Move one eligible closed job's code to Trash and keep all repository branches."""
    from .close_saves import enabled, save_job
    if enabled(store):
        return await save_job(store, task_id)
    if store.get('closed_copy_delete', False) is True:
        return await stage_closed_copy(store, task_id)
    now = time.time() if now is None else now
    task = store.task_get(task_id)
    number = task['number'] if task else task_id
    try:
        target = _workspace_target(store.directory, task_id)
        reason = _cleanup_task_reason(store, task, target, now)
        if reason:
            raise ValueError(reason)
        topic = store.topic(task['topic'])
        if not topic or not topic['cwd']:
            raise ValueError('project folder is unavailable')
        identity = await project_identity(Path(topic['cwd']), timeout=GIT_TIMEOUT)
        if identity['base_commit'] is None:
            raise ValueError('project has no Git worktree')
        repository = Path(identity['project_key'])
        async with _repository_lock(store.directory, identity['project_key']):
            task = store.task_get(task_id)
            reason = _cleanup_task_reason(store, task, target, now)
            if reason:
                raise ValueError(reason)
            original = target.lstat()
            git_dir, branch_ref = await _cleanup_git_directory(repository, target)
            trash = trash_directory()
            ancestor = trash
            while not ancestor.exists():
                ancestor = ancestor.parent
            device = ancestor.stat().st_dev
            if target.lstat().st_dev != device or git_dir.lstat().st_dev != device:
                raise ValueError('Trash is on another filesystem')
            reason = _cleanup_task_reason(store, store.task_get(task_id), target, now)
            if reason:
                raise ValueError(reason)
            manifest = None
            if (target / '.gitmodules').exists() or (git_dir / 'modules').exists():
                from .worktree_salvage import preserve_worktree, verify_preserved_worktree
                manifest = await preserve_worktree(target, store.directory / 'results' / target.name / 'worktree-salvage')
            if manifest is not None:
                await verify_preserved_worktree(target, manifest)
            with store.db:
                if not store.db.in_transaction:
                    store.db.execute('BEGIN IMMEDIATE')
                reason = _cleanup_task_reason(store, store.task_get(task_id), target, now)
                if reason:
                    raise ValueError(reason)
                current = target.lstat()
                if (current.st_dev, current.st_ino) != (original.st_dev, original.st_ino):
                    raise ValueError('task folder changed during cleanup')
                if ((git_dir / 'locked').exists() or _workspace_operation(git_dir)
                        or (git_dir / 'HEAD').read_text().strip() != 'ref: ' + branch_ref):
                    raise ValueError('Git branch, operation, or lock changed during cleanup')
                if manifest is None and ((target / '.gitmodules').exists() or (git_dir / 'modules').exists()):
                    raise ValueError('Submodule layout changed during cleanup')
                trash = trash_directory()
                trash.mkdir(mode=0o700, parents=True, exist_ok=True)
                device = trash.stat().st_dev
                if target.lstat().st_dev != device or git_dir.lstat().st_dev != device:
                    raise ValueError('Trash is on another filesystem')
                moved = move_to_trash(target, trash)
            try:
                move_to_trash(git_dir, trash, name=target.name + '.git-worktree')
            except BaseException:
                try:
                    if target.exists() or target.is_symlink():
                        raise FileExistsError('Cannot restore the task folder over another path.')
                    moved.rename(target)
                except OSError:
                    logger.exception('worktree cleanup rollback failed job=%s trash=%s repository=%s',
                                     number, moved, repository)
                raise
        logger.info('worktree cleanup moved job=%s trash=%s repository=%s', number, moved, repository)
        return moved
    except (OSError, RuntimeError, ValueError, asyncio.TimeoutError) as error:
        if (isinstance(error, FileNotFoundError) and error.filename == str(target)
                and not target.exists() and not target.is_symlink()):
            return None
        detail = ' '.join(str(error).splitlines()) or type(error).__name__
        logger.debug('worktree cleanup skipped job=%s reason=%s', number, detail)
        return None


async def cleanup_closed_worktrees(store, limit=CLEANUP_LIMIT, now=None, *, retry_failed=True):
    """Save sweep progress and bound Git checks while skipping unavailable jobs for free."""
    if store.get('closed_copy_delete', False) is True or store.get('close_clears_space', False) is True:
        await cleanup_closed_copies(store, sweep=True, retry_failed=retry_failed)
        return 0
    if list(_deletion_stages(store.directory)):
        await cleanup_closed_copies(store, sweep=True, retry_failed=retry_failed)
    after = store.get(CLEANUP_CURSOR_SETTING, 0)
    now = time.time() if now is None else now
    checked = 0
    while checked < limit:
        page_size = limit - checked
        tasks = store.closed_tasks_page(after, page_size)
        for task in tasks:
            try:
                target = _workspace_target(store.directory, task['id'])
                ready = _cleanup_task_reason(store, store.task_get(task['id']), target, now) is None
                if ready:
                    topic = store.topic(task['topic'])
                    ready = bool(topic and topic['cwd'] and Path(topic['cwd']).expanduser().is_dir())
            except (OSError, RuntimeError, ValueError):
                ready = False
            if ready:
                await cleanup_task_workspace(store, task['id'], now=now)
                checked += 1
            after = task['id']
            with store.db:
                store.put(CLEANUP_CURSOR_SETTING, after)
        if len(tasks) < page_size:
            with store.db:
                store.put(CLEANUP_CURSOR_SETTING, 0)
            return 0
    return after


def request_closed_copy_cleanup(store, task_id=None, cwd=None):
    from .close_saves import enabled, request
    if enabled(store):
        request(store, task_id, cwd)
        return
    if store.get('closed_copy_delete', False) is not True:
        return
    pending = store.get('closed_copy_cleanup_pending', {})
    root = Path(store.directory).resolve() / 'worktrees'
    current = Path(cwd).resolve() if cwd else None
    for task in store.db.execute("SELECT id FROM tasks WHERE status IN ('done','dropped')"):
        target = root / ('task-' + str(task['id']))
        if task['id'] == task_id or current is not None and current.is_relative_to(target):
            pending[str(task['id'])] = uuid.uuid4().hex
    store.put('closed_copy_cleanup_pending', pending)


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


def _deletion_stages(state_dir, on_error=None):
    root = Path(state_dir) / 'worktree-deletions'
    try:
        if root.is_symlink():
            raise ValueError('Deletion staging folder is a symlink')
        stages = sorted(root.iterdir()) if root.is_dir() else []
    except (OSError, ValueError) as error:
        if on_error:
            on_error(root, error)
        return
    for stage in stages:
        try:
            if stage.is_dir() and not stage.is_symlink() and (stage / 'record.json').is_file():
                record = json.loads((stage / 'record.json').read_text())
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
                if Path(record['target']) != _workspace_target(state_dir, record['task']):
                    raise ValueError('Staged deletion has an unsafe task path')
                git_dir = Path(record['git_dir'])
                if git_dir.parent != Path(record['repository']) / 'worktrees':
                    raise ValueError('Staged deletion has an unsafe Git data path')
                yield stage, record
        except FileNotFoundError:
            continue
        except Exception as error:
            if on_error:
                on_error(stage, error)


def _detach_staged(stage, record):
    if record['detached'] or not (stage / 'copy').is_dir():
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


def _recover_staged_registrations(state_dir, repository):
    for stage, record in list(_deletion_stages(state_dir)):
        if record['repository'] == str(repository) and not record['detached']:
            _detach_staged(stage, record)


def closed_copy_answer(state):
    if state['state'] == 'gone' and state['reason']:
        return 'Its code copy is already absent.'
    if state['state'] == 'staged' and state['reason']:
        return 'Its code copy was staged; its branch is kept. Background deletion needs attention: ' + state['reason'] + '.'
    if state['state'] in ('staged', 'gone'):
        return 'Its code copy was deleted; its branch is kept. Staged bytes are removed in the background.'
    if state['reason'] == 'worktree has tracked or untracked changes':
        return 'Its code copy has uncommitted changes or untracked files, so it stays until they are committed or discarded.'
    return 'Its code copy is kept because ' + state['reason'] + '.'


def _absent_copy_state(store, task_id):
    state = {'state': 'gone', 'reason': 'code copy is already absent'}
    for stage, record in _deletion_stages(store.directory):
        if record['task'] == task_id:
            state = {'state': 'staged', 'reason': record.get('error', ''), 'stage': str(stage)}
            break
    with store.db:
        store.put('closed_copy_state:' + str(task_id), state)
    return state


async def stage_closed_copy(store, task_id):
    """Release a proven saved copy and its registration before background byte removal."""
    task = store.task_get(task_id)
    target = _workspace_target(store.directory, task_id)
    number = task['number'] if task else task_id
    state_key = 'closed_copy_state:' + str(task_id)
    try:
        if not target.exists() and not target.is_symlink():
            return _absent_copy_state(store, task_id)
        reason = _cleanup_task_reason(store, task, target, time.time(), immediate=True)
        if reason:
            raise ValueError(reason)
        topic = store.topic(task['topic'])
        if not topic or not topic['cwd']:
            raise ValueError('project folder is unavailable')
        identity = await project_identity(Path(topic['cwd']), timeout=GIT_TIMEOUT)
        if identity['base_commit'] is None:
            raise ValueError('project has no Git worktree')
        repository = Path(identity['project_key'])
        async with _repository_lock(store.directory, repository):
            if not target.exists() and not target.is_symlink():
                return _absent_copy_state(store, task_id)
            original = target.lstat()
            git_dir, branch_ref = await _cleanup_git_directory(repository, target)
            if branch_ref != 'refs/heads/torii/task-' + str(task_id):
                raise ValueError('worktree is not on the saved job branch')
            reason = _cleanup_task_reason(store, store.task_get(task_id), target, time.time(), immediate=True)
            if reason:
                raise ValueError(reason)
            if store.get('closed_copy_delete', False) is not True:
                raise ValueError('closed copy deletion is off')
            manifest = None
            _, ignored = await _git(target, 'ls-files', '--others', '--ignored', '--exclude-standard',
                                    '--directory', '-z', timeout=GIT_TIMEOUT, strip=False)
            root = store.directory / 'worktree-deletions'
            if root.is_symlink():
                raise ValueError('Deletion staging folder is a symlink')
            root.mkdir(mode=0o700, exist_ok=True)
            info = git_dir.lstat()
            current = target.lstat()
            if (current.st_dev, current.st_ino) != (original.st_dev, original.st_ino):
                raise ValueError('task folder changed during cleanup')
            if current.st_dev != root.stat().st_dev or info.st_dev != root.stat().st_dev:
                raise ValueError('Deletion staging folder is on another filesystem')
            if (target / '.gitmodules').exists() or (git_dir / 'modules').exists():
                from .worktree_salvage import preserve_worktree, verify_preserved_worktree
                manifest = await preserve_worktree(target, store.directory / 'results' / target.name / 'worktree-salvage')
            stage = root / (target.name + '-' + uuid.uuid4().hex)
            stage.mkdir(mode=0o700)
            record = dict(task=task_id, number=number, target=str(target), repository=str(repository),
                          git_dir=str(git_dir), git_inode=[info.st_dev, info.st_ino], branch=branch_ref,
                          ignored=sorted({name.split('/')[0] for name in ignored.split('\0') if name}),
                          detached=False, removed={'bytes': 0, 'files': 0})
            if manifest is not None:
                record['salvage'] = str(manifest)
            _save_deletion(stage, record)
            checked_dir, checked_branch = await _cleanup_git_directory(repository, target)
            if manifest is not None:
                await verify_preserved_worktree(target, manifest)
            with store.db:
                if not store.db.in_transaction:
                    store.db.execute('BEGIN IMMEDIATE')
                if store.get('closed_copy_delete', False) is not True:
                    raise ValueError('closed copy deletion is off')
                reason = _cleanup_task_reason(store, store.task_get(task_id), target, time.time(), immediate=True)
                if reason:
                    raise ValueError(reason)
                current = target.lstat()
                if (current.st_dev, current.st_ino) != (original.st_dev, original.st_ino):
                    raise ValueError('task folder changed during cleanup')
                if checked_dir != git_dir or checked_branch != branch_ref:
                    raise ValueError('Git registration changed during cleanup')
                current_git = git_dir.lstat()
                if (current_git.st_dev, current_git.st_ino) != (info.st_dev, info.st_ino):
                    raise ValueError('Git data folder changed during cleanup')
                if ((git_dir / 'locked').exists() or _workspace_operation(git_dir)
                        or (git_dir / 'HEAD').read_text().strip() != 'ref: ' + branch_ref):
                    raise ValueError('Git branch, operation, or lock changed during cleanup')
                if manifest is None and ((target / '.gitmodules').exists() or (git_dir / 'modules').exists()):
                    raise ValueError('Submodule layout changed during cleanup')
                if current.st_dev != stage.stat().st_dev or info.st_dev != stage.stat().st_dev:
                    raise ValueError('Deletion staging folder is on another filesystem')
                target.rename(stage / 'copy')
                try:
                    _detach_staged(stage, record)
                except BaseException:
                    if (stage / 'git').exists():
                        (stage / 'git').rename(git_dir)
                    (stage / 'copy').rename(target)
                    raise
                state = {'state': 'staged', 'reason': '', 'stage': str(stage)}
                store.put(state_key, state)
        return state
    except (OSError, RuntimeError, ValueError, asyncio.TimeoutError) as error:
        reason = ' '.join(str(error).splitlines()) or type(error).__name__
        state = {'state': 'kept', 'reason': reason}
        with store.db:
            store.put(state_key, state)
        logger.info('worktree cleanup kept job=%s folder=%s reason=%s', number, target, reason)
        return state


async def cleanup_closed_copies(store, *, sweep=False, retry_failed=True):
    """One service consumer drains durable events and crash leftovers without an age limit."""
    from . import problems, close_saves

    def invalid(stage, error):
        try:
            problems.record(store, 'workspaces', 'closed-copy-record-invalid', str(stage) + ': ' + str(error),
                            every=CLEANUP_INTERVAL)
        except Exception as failure:
            logger.error('worktree cleanup invalid record reporting failed type=%s', type(failure).__name__)

    async with _repository_lock(store.directory, 'closed-copy-deletion'):
        more = False
        if close_saves.enabled(store):
            more = await close_saves.drain(store)
        with store.db:
            if not store.db.in_transaction:
                store.db.execute('BEGIN IMMEDIATE')
            pending = store.get('closed_copy_cleanup_pending', {})
            store.put('closed_copy_cleanup_pending', {})
        if store.get('closed_copy_delete', False) is True and not close_saves.enabled(store):
            tasks = ([row['id'] for row in store.db.execute(
                "SELECT id FROM tasks WHERE status IN ('done','dropped') ORDER BY id")]
                if sweep else [int(task) for task, check in pending.items() if check])
            for task_id in tasks:
                await stage_closed_copy(store, task_id)
        for stage, record in list(_deletion_stages(store.directory, on_error=invalid)):
            if 'space_k' in record:
                if record.get('retry_at', 0) > time.time():
                    continue
            elif record.get('error') and (not sweep or not retry_failed):
                continue
            try:
                async with _repository_lock(store.directory, record['repository']):
                    with store.db:
                        _detach_staged(stage, record)
                if not record['detached']:
                    await delete_tree(stage)
                    continue
                options = {'paced': True} if 'space_k' in record else {}
                async with _repository_lock(store.directory, record['repository']):
                    if not await close_saves.verify_staged(store, stage, record):
                        continue
                await delete_tree(stage / 'copy', progress=record['removed'], **options)
                if 'space_k' in record:
                    await delete_tree(stage / 'git', progress=record['removed'], **options)
                else:
                    await delete_tree(stage / 'git')
                await delete_tree(stage, **options)
            except asyncio.CancelledError:
                try:
                    _save_deletion(stage, record)
                except OSError:
                    pass
                raise
            except Exception as error:
                reason = ' '.join(str(error).splitlines()) or type(error).__name__
                record['error'] = reason
                if 'space_k' in record:
                    record['attempts'] = record.get('attempts', 0) + 1
                    record['retry_at'] = time.time() + min(300, 2 ** min(record['attempts'], 9))
                try:
                    _save_deletion(stage, record)
                except Exception as save_error:
                    reason += '; progress could not be saved: ' + str(save_error)
                try:
                    with store.db:
                        store.put('closed_copy_state:' + str(record['task']),
                                  {'state': 'staged', 'reason': reason, 'stage': str(stage)})
                    problems.record(store, 'workspaces', 'closed-copy-delete-failed', reason,
                                    task=record['task'], every=CLEANUP_INTERVAL)
                except Exception as failure:
                    logger.error('worktree cleanup failure reporting failed job=%s type=%s',
                                 record['number'], type(failure).__name__)
                continue
            logger.info('worktree cleanup deleted job=%s folder=%s bytes=%s files=%s ignored=%s',
                        record['number'], record['target'], record['removed']['bytes'],
                        record['removed']['files'], json.dumps(record['ignored']))
            with store.db:
                if 'space_k' in record:
                    store.db.execute('UPDATE task_iterations SET bytes_freed=COALESCE(bytes_freed,0)+? WHERE task=? AND k=?',
                                     (record['removed']['bytes'], record['task'], record['space_k']))
                    task = {'id': record['task'], 'iteration': record['space_k']}
                    phase, saved = close_saves.load(store, task)
                    saved.setdefault('freed', dict.fromkeys(('copy', 'deps', 'extras', 'prune'), 0))
                    saved['freed']['copy'] += record['removed']['bytes'] - saved['freed']['deps']
                    close_saves.checkpoint(store, task, phase, saved)
                else:
                    store.put('closed_copy_state:' + str(record['task']), {'state': 'gone', 'reason': ''})
        if close_saves.enabled(store):
            await close_saves.finish(store)
            return more and close_saves.pending(store)
    return False
