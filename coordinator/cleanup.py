"""Job work tree saves. A save is a commit on the round's saved ref, so a work tree is a cache of that commit.

Git runs in a thread with subprocess, as the other worker storage work does: cancelling an asyncio subprocess
while it starts can hang the event loop at shutdown."""

import asyncio
from contextlib import contextmanager
import fcntl
import fnmatch
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import time

from . import workspaces as ws

SECRETS = ('.env*', '*.pem', '*.key', '*.p12')
UNTRACKED_LIMIT = 200 * 1024 * 1024
LOCK_WAIT = 60
AUTHOR = dict(GIT_AUTHOR_NAME='Torii', GIT_AUTHOR_EMAIL='torii@localhost',
              GIT_COMMITTER_NAME='Torii', GIT_COMMITTER_EMAIL='torii@localhost')


def git(cwd, *args, check=True, env=None, strip=True):
    """(exit code, output) of one git command; a failure raises RuntimeError when check is set."""
    try:
        result = subprocess.run(['git', '-C', str(cwd), *map(str, args)], capture_output=True, env=env,
                                timeout=ws.GIT_TIMEOUT)
    except subprocess.TimeoutExpired as error:
        raise ValueError('git %s timed out after %gs' % (args[0], ws.GIT_TIMEOUT)) from error
    text = result.stdout.decode(errors='surrogateescape')
    if check and result.returncode:
        raise RuntimeError(result.stderr.decode(errors='replace').strip() or text or 'git %s failed' % args[0])
    return result.returncode, text.strip() if strip else text


@contextmanager
def repository_lock(state_dir, repository, wait=LOCK_WAIT):
    """The repository lock of workspaces._repository_lock, taken from a thread."""
    fd = os.open(str(ws.repository_lock_path(state_dir, repository)), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        deadline = time.monotonic() + wait
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise ValueError('repository lock is busy')
                time.sleep(0.05)
        yield
    finally:
        os.close(fd)


def saved_ref(task):
    return 'refs/torii/saved/task-%s/%s/wip' % (task['id'], task['iteration'])


def left_out(target, loose):
    """The untracked files a save leaves out; this is the one content rule of a save.
    A loose save leaves out secret-named files and the largest files past UNTRACKED_LIMIT.
    A strict save leaves out nothing and refuses past UNTRACKED_LIMIT. Ignored files are never in the commit."""
    _, names = git(target, 'ls-files', '--others', '--exclude-standard', '-z', strip=False)
    sizes = {}
    groups = {}
    excluded = []
    for name in names.split('\0'):
        if not name:
            continue
        if loose and any(fnmatch.fnmatch(part, pattern) for part in Path(name).parts for pattern in SECRETS):
            excluded.append(name)
            continue
        info = (target / name).lstat()
        if stat.S_ISLNK(info.st_mode):
            continue
        top = Path(name).parts[0]
        sizes[top] = sizes.get(top, 0) + info.st_size
        groups.setdefault(top, []).append((info.st_size, name))
    total = sum(sizes.values())
    if total > UNTRACKED_LIMIT and not loose:
        raise ValueError('untracked files exceed 200 MB: %s (%d total)' % (max(sizes, key=sizes.get), total))
    for top in sorted(sizes, key=lambda name: (-sizes[name], name)):
        if total <= UNTRACKED_LIMIT:
            break
        _, tracked = git(target, 'ls-files', '-z', '--', ':(literal)' + top, strip=False)
        if not tracked:
            excluded.append(top)
            total -= sizes[top]
            continue
        for size, name in sorted(groups[top], key=lambda item: (-item[0], item[1])):
            if total <= UNTRACKED_LIMIT:
                break
            excluded.append(name)
            total -= size
    return excluded


def write_tree(target, folder, exclude, git_env=None, base='HEAD'):
    """Write the tree of target with a temporary index in folder: base, then every change except the paths in exclude."""
    index = folder / 'snapshot-index'
    pathspecs = folder / 'snapshot-pathspecs'
    for temporary in (index, index.with_name(index.name + '.lock'), pathspecs):
        if temporary.exists() or temporary.is_symlink():
            temporary.unlink()
    env = dict(os.environ, **(git_env or {}), GIT_INDEX_FILE=str(index), GIT_OPTIONAL_LOCKS='0')
    git(target, 'read-tree', base, env=env)
    pathspecs.write_bytes(b'\0'.join(os.fsencode(name) for name in ['.', *(':(exclude,literal)' + name for name in exclude)]))
    git(target, 'add', '-A', '--pathspec-from-file=' + str(pathspecs), '--pathspec-file-nul', env=env)
    _, result = git(target, 'write-tree', env=env)
    index.unlink()
    pathspecs.unlink()
    return result


def commit(repository, task, tree, head, message, left=()):
    """Commit tree on the round's saved ref. Its parents are HEAD and the round's earlier saved commit,
    so no earlier save becomes unreachable. An earlier commit of the same tree on the same HEAD is reused."""
    ref = saved_ref(task)
    code, previous = git(repository, 'rev-parse', '--verify', '--quiet', ref, check=False)
    parents = ['-p', head]
    if not code:
        _, same = git(repository, 'rev-parse', previous + '^{tree}', previous + '^1')
        if same.split() == [tree, head]:
            return previous
        parents += ['-p', previous]
    if left:
        message += '\n\nLeft out: ' + ', '.join(left)
    _, wip = git(repository, 'commit-tree', tree, *parents, '-m', message, env=dict(os.environ, **AUTHOR))
    git(repository, 'update-ref', ref, wip, previous if not code else '0' * 40)
    return wip


def save_tree(directory, task, project, message):
    """Save the job work tree of task under the repository lock. Return the commit, or None when there is no job
    work tree or it equals HEAD."""
    target = ws._workspace_target(directory, task['id'], task['worktree'] or None)
    if not (target / '.git').is_file():
        return None
    _, common = git(project, 'rev-parse', '--git-common-dir')
    repository = ws._resolved_git_dir(project, common)
    with repository_lock(directory, repository):
        if not (target / '.git').is_file():
            return None
        _, common = git(target, 'rev-parse', '--git-common-dir')
        if ws._resolved_git_dir(target, common) != repository:
            raise ValueError('job work tree belongs to another repository')
        left = left_out(target, loose=True)
        with tempfile.TemporaryDirectory(prefix='torii-save-') as folder:
            tree = write_tree(target, Path(folder), left)
        _, head = git(target, 'rev-parse', 'HEAD')
        _, head_tree = git(target, 'rev-parse', 'HEAD^{tree}')
        if tree == head_tree:
            return None
        return commit(repository, task, tree, head, message, left)


async def save(store, worker):
    """Save the job work tree of a finished worker. Ignored files stay out of the commit; the close save still copies them."""
    task = store.task_get(worker['task']) if worker['task'] else None
    if not task or json.loads(worker['workspace'] or '{}').get('scratch'):
        return None
    project = Path(store.topic(task['topic'])['cwd']).expanduser().resolve()
    return await asyncio.to_thread(save_tree, store.directory, task, project, 'torii save job %s.%s worker %s'
                                   % (task['number'], task['iteration'], worker['id']))
