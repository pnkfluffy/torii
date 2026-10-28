"""Verified round saves, disposable job folders, and same-path workspace rebuilds."""

import asyncio
import fnmatch
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time
import uuid

from . import workspaces as ws, worktree_salvage

CLOSED = ('done', 'dropped', 'waiting', 'interrupted')
REGENERABLE = frozenset(('node_modules', 'dist', 'build', '.next', '.turbo', '.venv', 'venv',
                         '__pycache__', '.pytest_cache', '.mypy_cache', '.ruff_cache', 'coverage',
                         'target', '.gradle', '.parcel-cache', '.cache', '.DS_Store'))
IGNORED_LIMIT = 200 * 1024 * 1024
UNTRACKED_LIMIT = IGNORED_LIMIT
DRAIN_BATCH = 5
RULES = {
    'submodules': ('worktree uses submodules',),
    'any_branch': ('worktree is not on the saved job branch',),
    'discard_names': ('ignored files exceed',),
    'prune_held': (),
}


def enabled(store):
    return store.get('close_clears_space', False) is True


def kept(store, task_id):
    return task_id in store.get('close_saves_keep', [])


def pending(store):
    return bool(store.db.execute(f"""SELECT 1 FROM tasks t WHERE copy='saving'
        AND status IN ({','.join('?' for _ in CLOSED)}) AND NOT EXISTS (
        SELECT 1 FROM task_space_saves s WHERE s.task=t.id AND s.k=t.iteration AND s.phase IN ('staged','extras')) LIMIT 1""", CLOSED).fetchone())


async def git(cwd, *args, **options):
    try:
        return await ws._git(cwd, *args, timeout=ws.GIT_TIMEOUT, **options)
    except asyncio.TimeoutError as error:
        raise ValueError('git %s timed out after %gs' % (args[0], ws.GIT_TIMEOUT)) from error


def migrate(store):
    store.db.execute('''CREATE TABLE IF NOT EXISTS task_space_saves (
        task INTEGER NOT NULL REFERENCES tasks(id), k INTEGER NOT NULL,
        phase TEXT NOT NULL, data TEXT NOT NULL, PRIMARY KEY(task,k))''')
    for name, kind in (('head', 'TEXT'), ('saved', 'TEXT'), ('rebuild_seconds', 'REAL'), ('bytes_freed', 'INTEGER'), ('space_outbox_after', 'INTEGER NOT NULL DEFAULT 0')):
        if name not in store._columns('task_iterations'):
            store.db.execute('ALTER TABLE task_iterations ADD COLUMN ' + name + ' ' + kind)
    store.db.execute('''CREATE TRIGGER IF NOT EXISTS task_space_round AFTER INSERT ON task_iterations
        WHEN (SELECT value FROM settings WHERE key='close_clears_space')='true'
        BEGIN UPDATE task_iterations SET space_outbox_after=(SELECT COALESCE(MAX(id),0) FROM outbox)
        WHERE task=NEW.task AND k=NEW.k; END''')
    store.db.execute('''CREATE TRIGGER IF NOT EXISTS task_space_close AFTER UPDATE OF status ON tasks
        WHEN NEW.status IN ('done','dropped','waiting','interrupted')
        AND (SELECT value FROM settings WHERE key='close_clears_space')='true'
        BEGIN UPDATE tasks SET copy='saving',copy_reason=NULL WHERE id=NEW.id; END''')


def scratch(store, task_id=None, topic=None, create=False):
    path = store.directory / 'scratch' / ('task-' + str(task_id) if task_id is not None else 'topic-' + str(topic))
    if '/' in path.name or path.parent != store.directory / 'scratch':
        raise ValueError('Invalid scratch path')
    if create:
        directory(path, store.directory)
    return path


def directory(path, anchor):
    path, anchor = Path(path), Path(anchor).resolve()
    relative = path.relative_to(anchor)
    current = anchor
    for part in relative.parts:
        if part in ('.', '..'):
            raise ValueError('Unsafe saved path')
        current = current / part
        current.mkdir(mode=0o700, exist_ok=True)
        if current.is_symlink() or not current.is_dir():
            raise ValueError('Symbolic link in owned folder')
    return path


def request(store, task_id=None, cwd=None):
    if not enabled(store):
        return
    targets = {task_id} if task_id is not None else set()
    if cwd:
        from .worker_storage import temp_base
        for path in {Path(cwd).absolute(), Path(cwd).resolve()}:
            opencode = store.directory / 'opencode' / 'workers'
            for root in (store.directory / 'worktrees', store.directory / 'scratch', temp_base(), opencode):
                if not path.is_relative_to(root) or path == root:
                    continue
                name = path.relative_to(root).parts[0]
                match = re.fullmatch(r'task-([0-9]+)', name) if root != temp_base() else None
                if match:
                    targets.add(int(match[1]))
                match = (re.fullmatch(r'([0-9]+)', name) if root == opencode else
                         re.fullmatch(r'torii-w([0-9]+)', name) if root == temp_base() else None)
                if match:
                    row = store.db.execute('SELECT task FROM workers WHERE id=?', (int(match[1]),)).fetchone()
                    if row and row['task'] is not None:
                        targets.add(row['task'])
            targets.update(row['task'] for row in store.db.execute('''SELECT w.task
                FROM worker_temp_state s JOIN workers w ON w.id=s.worker
                WHERE w.task IS NOT NULL AND s.path IS NOT NULL AND (?=s.path OR instr(?,s.path || '/')=1)''',
                                                                   (str(path), str(path))))
    for target in targets:
        store.db.execute("""UPDATE tasks SET copy='saving',copy_reason=NULL WHERE id=?
            AND status IN ('done','dropped','waiting','interrupted')""", (target,))


def checkpoint(store, task, phase, data):
    data.setdefault('freed', dict.fromkeys(('copy', 'deps', 'extras', 'prune'), 0))
    with store.transaction():
        store.db.execute('''INSERT INTO task_space_saves(task,k,phase,data) VALUES (?,?,?,?)
            ON CONFLICT(task,k) DO UPDATE SET phase=excluded.phase,data=excluded.data''',
                         (task['id'], task['iteration'], phase, json.dumps(data)))


def load(store, task):
    row = store.db.execute('SELECT phase,data FROM task_space_saves WHERE task=? AND k=?',
                           (task['id'], task['iteration'])).fetchone()
    return (row['phase'], json.loads(row['data'])) if row else ('pending', {})


def atomic_json(path, value):
    temporary = path.with_name(path.name + '.tmp')
    fd = os.open(str(temporary), os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as handle:
        json.dump(value, handle, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)
    fd = os.open(str(path.parent), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def digest(path):
    fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode):
            raise ValueError('Save requires a regular file')
        sha = hashlib.sha256()
        for block in iter(lambda: handle.read(1024 * 1024), b''):
            sha.update(block)
        after = os.fstat(handle.fileno())
    if (info.st_size, info.st_mtime_ns, info.st_ctime_ns) != (after.st_size, after.st_mtime_ns, after.st_ctime_ns):
        raise ValueError('File changed during save')
    return {'bytes': info.st_size, 'sha256': sha.hexdigest()}


def copy_file(source, destination, anchor):
    directory(destination.parent, anchor)
    expected = digest(source)
    temporary = destination.with_name(destination.name + '.tmp')
    src = os.open(str(source), os.O_RDONLY | os.O_NOFOLLOW)
    dst = os.open(str(temporary), os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(src, 'rb') as reader, os.fdopen(dst, 'wb') as writer:
        for block in iter(lambda: reader.read(1024 * 1024), b''):
            writer.write(block)
        writer.flush()
        os.fsync(writer.fileno())
    if digest(temporary) != expected or digest(source) != expected:
        raise ValueError('File changed while copying proof')
    temporary.replace(destination)
    return dict(expected, path=str(destination.relative_to(anchor)))


def files(path, device=None):
    if path.exists() and device is not None and path.lstat().st_dev != device:
        raise ValueError('Owned folder crosses filesystems')
    if path.is_symlink():
        yield path
    elif path.is_dir():
        for child in sorted(path.iterdir()):
            yield from files(child, path.lstat().st_dev if device is None else device)
    elif path.exists():
        yield path


def roots(store, task):
    from .worker_storage import temp_directory
    result = [ws._workspace_target(store.directory, task['id']), scratch(store, task['id'])]
    for row in store.db.execute('SELECT id,provider FROM workers WHERE task=?', (task['id'],)):
        result.append(temp_directory(row['id']))
        saved = store.db.execute('SELECT path FROM worker_temp_state WHERE worker=?', (row['id'],)).fetchone()
        if saved and saved['path']:
            path = Path(saved['path'])
            if not path.is_absolute() or path.name != 'torii-w' + str(row['id']) or path.parent.resolve() != path.parent:
                raise ValueError('Unexpected saved worker temp path')
            result.append(path)
        if row['provider'] == 'opencode':
            launches = store.directory / 'opencode' / 'workers' / str(row['id']) / 'launches'
            if launches.resolve() != launches:
                raise ValueError('Symbolic link in worker launch folders')
            if launches.is_dir():
                for launch in launches.iterdir():
                    if re.fullmatch('[a-f0-9]{32}', launch.name):
                        if launch.is_symlink():
                            raise ValueError('Symbolic link in worker launch folders')
                        result.extend((launch / 'tmp', launch / 'home' / 'Library' / 'Caches'))
    return list(dict.fromkeys(result))


def closed_round(store, task):
    current = store.task_get(task['id'])
    return current and current['status'] in CLOSED and current['iteration'] == task['iteration']


def preconditions(store, task, owned):
    if not enabled(store):
        raise ValueError('close clears space is off')
    current = store.task_get(task['id'])
    if current['status'] not in CLOSED or current['iteration'] != task['iteration']:
        raise ValueError('job is open or its round changed')
    iteration = store.db.execute('SELECT * FROM task_iterations WHERE task=? AND k=?',
                                  (task['id'], task['iteration'])).fetchone()
    if current['status'] == 'interrupted' and {'cause', 'resume_at'}.issubset(iteration.keys()):
        if iteration['cause'] == 'crash' and iteration['resume_at'] is not None and 0 <= iteration['resume_at'] - time.time() <= 600:
            raise ValueError('a short crash retry keeps the copy until the resume event')
    if (store.get('topic_lifecycle:' + task['topic']) or {}).get('state') == 'closing':
        raise ValueError('chat is closing')
    for row in store.db.execute("SELECT task,cwd FROM workers WHERE status IN ('queued','running')"):
        if row['task'] == task['id'] or row['cwd'] and any(Path(row['cwd']).resolve().is_relative_to(p) for p in owned):
            raise ValueError('job has an unfinished worker')
    for row in store.db.execute('SELECT image FROM outbox WHERE delivered=0 AND retired=0 AND image IS NOT NULL'):
        if any(Path(row['image']).absolute().is_relative_to(p) or Path(row['image']).resolve().is_relative_to(p) for p in owned):
            raise ValueError("a Telegram file queued from it hasn't been sent yet")
    for path in owned:
        if path.is_symlink() or path.parent.resolve() != path.parent or path.exists() and not path.is_dir():
            raise ValueError('Symbolic link in owned folder')


async def proofs(store, task, owned, folder):
    job_scratch = scratch(store, task['id'])
    candidates = set(await asyncio.to_thread(lambda: list(files(job_scratch / 'proofs')))) if job_scratch in owned else set()
    retained_path = folder / 'worker-proofs.json'
    retained = json.loads(retained_path.read_text()) if retained_path.exists() else []
    manifest_path = folder / 'saved.json'
    if manifest_path.exists():
        retained.extend(json.loads(manifest_path.read_text()).get('proofs', []))
    retained = list({entry['source']: entry for entry in retained}.values())
    retained_by_source = {entry['source']: entry for entry in retained}
    notes = task.get('notes') or ''
    previous = store.db.execute('SELECT data FROM task_space_saves WHERE task=? AND k<? ORDER BY k DESC LIMIT 1',
                                (task['id'], task['iteration'])).fetchone()
    if previous:
        old_notes = json.loads(previous['data']).get('notes', '')
        if notes == old_notes:
            notes = ''
        elif old_notes and notes.startswith(old_notes + '\n'):
            notes = notes[len(old_notes) + 1:]
    names = {task['proof']} if task.get('proof') else set()

    def quoted_path(match):
        names.add(next(path for path in match.groups() if path is not None))
        return ''

    notes = re.sub(r'''"(/[^"\n]+)"|'(/[^'\n]+)'|<(/[^>\n]+)>|`(/[^`\n]+)`''', quoted_path, notes)
    names.update(name.rstrip('.,;)`') for name in re.findall(r'(?:proof=)?(/[^\s<>\"\x27]+)', notes))
    for name in names:
        path = Path(name)
        if not any(path.is_relative_to(root) for root in owned):
            continue
        if path.resolve() == path and not path.is_symlink():
            if path.is_dir():
                continue
        candidates.add(path)
    round_row = store.db.execute('SELECT space_outbox_after FROM task_iterations WHERE task=? AND k=?',
                                  (task['id'], task['iteration'])).fetchone()
    for row in store.db.execute('SELECT image FROM outbox WHERE image IS NOT NULL AND (delivered=0 OR id>?)',
                                (round_row['space_outbox_after'],)):
        path = Path(row['image'])
        if any(path.is_relative_to(root) for root in owned):
            candidates.add(path)
    result = list(retained)
    for path in sorted(candidates):
        root = next(root for root in owned if path.is_relative_to(root))
        if not path.exists() and str(path) in retained_by_source:
            continue
        if path.resolve() != path or path.is_symlink():
            raise ValueError('Proof path is a symbolic link')
        if any(fnmatch.fnmatch(part, pattern) for part in path.relative_to(root).parts
               for pattern in ('.env*', '*.pem', '*.key', '*.p12')):
            raise ValueError('secret-looking proof is held: ' + path.name)
        owner = hashlib.sha256(str(root).encode()).hexdigest()
        destination = folder / 'proofs' / owner / path.relative_to(root)
        result = [entry for entry in result if entry['source'] != str(path)]
        try:
            entry = dict(await asyncio.to_thread(copy_file, path, destination, folder), source=str(path))
        except FileNotFoundError as error:
            if error.filename != str(path) or path.exists() or path.is_symlink():
                raise
            entry = {'source': str(path), 'missing': True}
        result.append(entry)
    return sorted(result, key=lambda entry: entry['source'])


async def git_directory(store, repository, target, task):
    if (target / '.git').is_symlink() or not (target / '.git').is_file():
        raise ValueError('job Git pointer is not a regular file')
    _, common = await git(target, 'rev-parse', '--git-common-dir')
    _, toplevel = await git(target, 'rev-parse', '--show-toplevel')
    if ws._resolved_git_dir(target, common) != repository or Path(toplevel).resolve() != target:
        raise ValueError('worktree belongs to another repository')
    records = [r for r in await ws._worktree_records(repository, timeout=ws.GIT_TIMEOUT) if Path(r['worktree']) == target]
    if len(records) != 1 or 'locked' in records[0]:
        raise ValueError('worktree is locked or its registration is missing')
    branch = records[0].get('branch')
    expected = 'refs/heads/torii/task-' + str(task['id'])
    if branch and branch != expected:
        if store.get('close_saves_any_branch', False) is not True or not branch.startswith('refs/heads/'):
            raise ValueError('worktree is not on the saved job branch')
    if store.get('close_saves_any_branch', False) is True and 'prunable' in records[0]:
        raise ValueError('worktree registration is prunable')
    git_dir = await ws._registered_git_directory(repository, target, timeout=ws.GIT_TIMEOUT)
    if git_dir.is_symlink() or any((git_dir / name).exists() for name in ('locked', 'index.lock', 'HEAD.lock')):
        raise ValueError('worktree is locked')
    operation = ws._workspace_operation(git_dir)
    if operation:
        raise ValueError('Git operation is in progress: ' + operation)
    submodules = store.get('close_saves_submodules', False) is True
    if submodules:
        paths = await worktree_salvage.repositories(target)
        if len(paths) > 1 or (git_dir / 'modules').is_dir():
            raise ValueError('worktree has initialized submodules')
    elif (target / '.gitmodules').exists() or (git_dir / 'modules').exists():
        raise ValueError('worktree uses submodules')
    _, untracked = await git(target, 'ls-files', '--others', '--exclude-standard', '-z', strip=False)
    for name in untracked.split('\0'):
        if any(fnmatch.fnmatch(part, pattern) for part in Path(name).parts
               for pattern in ('.env*', '*.pem', '*.key', '*.p12')):
            raise ValueError('secret-looking untracked file is held: ' + name)
    _, entries = await git(target, 'ls-files', '--stage', '-z')
    if not submodules and any(entry.startswith('160000 ') for entry in entries.split('\0')):
        raise ValueError('worktree uses submodules')
    _, flags = await git(target, 'ls-files', '-v', '-z', strip=False)
    if any(entry and (entry[0] == 'S' or entry[0].islower()) for entry in flags.split('\0')):
        raise ValueError('worktree has skip-worktree or assume-unchanged entries')
    return git_dir, branch or expected


def gitlinks(entries):
    result = {}
    for entry in entries.split('\0'):
        if entry.startswith('160000 '):
            header, name = entry.split('\t', 1)
            result[name] = header.split()[1]
    return result


def check_raw_tree(target, entries, algorithm, submodules):
    for entry in entries.split('\0'):
        if not entry:
            continue
        header, name = entry.split('\t', 1)
        mode, expected, _ = header.split()
        if mode == '120000':
            continue
        path = target / name
        if mode == '160000' and submodules:
            try:
                info = path.lstat()
            except FileNotFoundError:
                continue
            if not stat.S_ISDIR(info.st_mode) or next(path.iterdir(), None) is not None:
                raise ValueError('submodule folder holds unsaved files: ' + name)
            continue
        fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as handle:
            before = os.fstat(handle.fileno())
            if not stat.S_ISREG(before.st_mode):
                raise ValueError('Git snapshot requires a regular file: ' + name)
            sha = hashlib.new(algorithm)
            sha.update(b'blob %d\0' % before.st_size)
            for block in iter(lambda: handle.read(1024 * 1024), b''):
                sha.update(block)
            after = os.fstat(handle.fileno())
        if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise ValueError('late writer changed raw file bytes: ' + name)
        if sha.hexdigest() != expected:
            raise ValueError('Git conversion or late writer changed raw file bytes: ' + name)


async def tree(store, target, folder, git_env=None, base='HEAD'):
    submodules = store.get('close_saves_submodules', False) is True
    index = folder / 'snapshot-index'
    for temporary in (index, index.with_name(index.name + '.lock')):
        if temporary.exists() or temporary.is_symlink():
            temporary.unlink()
    source_env = dict(os.environ, **(git_env or {}))
    env = dict(source_env, GIT_INDEX_FILE=str(index), GIT_OPTIONAL_LOCKS='0')
    await git(target, 'read-tree', base, env=env)
    await git(target, 'add', '-A', env=env, reject_stderr=True)
    _, result = await git(target, 'write-tree', env=env)
    _, entries = await git(target, 'ls-files', '--stage', '-z', strip=False, env=env)
    _, algorithm = await git(target, 'rev-parse', '--show-object-format', env=env)
    await asyncio.to_thread(check_raw_tree, target, entries, algorithm, submodules)
    if submodules:
        _, current_entries = await git(target, 'ls-files', '--stage', '-z', strip=False, env=source_env)
        current, saved = gitlinks(current_entries), gitlinks(entries)
        for name in sorted(current.keys() | saved.keys()):
            if current.get(name) != saved.get(name):
                raise ValueError('submodule pointer changed: ' + name)
    if store.get('close_saves_any_branch', False) is True:
        _, indexed = await git(target, 'ls-files', '--stage', '-z', strip=False, env=source_env)
        _, intent = await git(target, 'diff', '--name-only', '--diff-filter=A', '--ita-invisible-in-index',
                              '-z', strip=False, env=source_env)
        if intent:
            raise ValueError('index has an intent-to-add entry: ' + intent.split('\0')[0])
        _, committed = await git(target, 'ls-tree', '-r', '-z', base, strip=False, env=source_env)
        captured = {name: tuple(header.split()[:2]) for header, name in
                    (entry.split('\t', 1) for entry in entries.split('\0') if entry)}
        original = {name: (header.split()[0], header.split()[2]) for header, name in
                    (entry.split('\t', 1) for entry in committed.split('\0') if entry)}
        for entry in indexed.split('\0'):
            if not entry:
                continue
            header, name = entry.split('\t', 1)
            mode, oid, stage = header.split()
            if stage != '0' or (mode, oid) not in (original.get(name), captured.get(name)):
                raise ValueError('index holds unsaved changes: ' + name)
    index.unlink()
    return result


async def reflog_commits(repository, target, git_env=None):
    env = dict(os.environ, **(git_env or {}))
    _, git_dir = await git(target, 'rev-parse', '--absolute-git-dir', env=env)

    def read_logs():
        roots = set()
        for path in files(Path(git_dir) / 'logs'):
            fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(fd, 'rb') as handle:
                if not stat.S_ISREG(os.fstat(fd).st_mode):
                    raise ValueError('worktree reflog is not a regular file')
                for line in handle:
                    for oid in line.split(maxsplit=2)[:2]:
                        if re.fullmatch(b'[a-f0-9]{40}|[a-f0-9]{64}', oid) and set(oid) != {48}:
                            roots.add(oid.decode('ascii'))
        return roots

    roots = await asyncio.to_thread(read_logs)
    _, indexed = await git(target, 'ls-files', '--stage', '-z', strip=False, env=env)
    roots.update(entry.split()[1] for entry in indexed.split('\0') if entry.startswith('160000 '))
    if not roots:
        return []
    _, commits = await git(repository, 'rev-list', '--single-worktree', '--ignore-missing', '--children',
                           *sorted(roots), '--not', '--all')
    return [line for line in commits.splitlines() if ' ' not in line]


async def snapshot(store, repository, target, folder, task, branch_ref=None):
    _, names = await git(target, 'ls-files', '--others', '--exclude-standard', '-z', strip=False)
    sizes = {}
    for name in names.split('\0'):
        if not name:
            continue
        info = (target / name).lstat()
        if stat.S_ISLNK(info.st_mode):
            continue
        top = Path(name).parts[0]
        sizes[top] = sizes.get(top, 0) + info.st_size
    total = sum(sizes.values())
    if total > UNTRACKED_LIMIT:
        raise ValueError('untracked files exceed 200 MB: %s (%d total)' % (max(sizes, key=sizes.get), total))
    _, head = await git(target, 'rev-parse', 'HEAD')
    job_ref = 'refs/heads/torii/task-' + str(task['id'])
    if store.get('close_saves_any_branch', False) is True:
        branch_ref = branch_ref or job_ref
        code, branch = await git(repository, 'rev-parse', '--verify', branch_ref, check=False)
        if code:
            detached, _ = await git(target, 'symbolic-ref', '--quiet', 'HEAD', check=False)
            if detached:
                raise ValueError('worktree HEAD is detached and the saved job branch is missing')
            raise ValueError('saved branch is missing: ' + branch_ref)
    else:
        _, branch = await git(repository, 'rev-parse', job_ref, check=False)
    head_ref = 'refs/torii/saved/task-%s/%s/head' % (task['id'], task['iteration'])
    await git(repository, 'update-ref', head_ref, head)
    tree_id = await tree(store, target, folder)
    _, head_tree = await git(target, 'rev-parse', 'HEAD^{tree}')
    wip = None
    _, changes = await git(target, 'status', '--porcelain', '--untracked-files=all',
                           env=dict(os.environ, GIT_OPTIONAL_LOCKS='0'), reject_stderr=True)
    if changes or tree_id != head_tree:
        env = dict(os.environ, GIT_AUTHOR_NAME='Torii', GIT_AUTHOR_EMAIL='torii@localhost',
                   GIT_COMMITTER_NAME='Torii', GIT_COMMITTER_EMAIL='torii@localhost')
        _, wip = await git(repository, 'commit-tree', tree_id, '-p', head, '-m',
                               'torii wip job %s.%s' % (task['number'], task['iteration']), env=env)
        ref = 'refs/torii/wip/task-' + str(task['id'])
        code, old = await git(repository, 'rev-parse', '--verify', ref, check=False)
        await git(repository, 'update-ref', ref, wip, old if not code else '0' * 40)
        await git(repository, 'update-ref', 'refs/torii/saved/task-%s/%s/wip' % (task['id'], task['iteration']), wip)
    refs = [job_ref]
    if store.get('close_saves_any_branch', False) is True:
        refs = [branch_ref]
        if branch_ref != job_ref:
            code, _ = await git(repository, 'rev-parse', '--verify', job_ref, check=False)
            if not code:
                refs.append(job_ref)
    if head != branch:
        refs.append('refs/torii/saved/task-%s/%s/head' % (task['id'], task['iteration']))
    if wip:
        refs.append('refs/torii/wip/task-' + str(task['id']))
    prefix = 'refs/torii/saved/task-%s/%s/reflog/' % (task['id'], task['iteration'])
    try:
        _, pinned = await git(repository, 'for-each-ref', '--format=%(refname) %(objectname)', prefix)
        reflog_refs = dict(line.split() for line in pinned.splitlines())
        for oid in await reflog_commits(repository, target):
            ref = prefix + oid
            await git(repository, 'update-ref', ref, oid)
            reflog_refs[ref] = oid
    except (OSError, RuntimeError, ValueError) as error:
        raise ValueError('could not pin worktree reflog commits: ' + str(error)) from error
    refs.extend(reflog_refs)
    _, unreached = await git(repository, 'rev-list', *refs, '--not', '--remotes')
    bundle = None
    if unreached:
        temporary = folder / 'branch.bundle.tmp'
        await git(repository, 'bundle', 'create', temporary, *refs, '--not', '--remotes')
        await git(repository, 'bundle', 'verify', temporary)
        temporary.replace(folder / 'branch.bundle')
        bundle = dict(digest(folder / 'branch.bundle'), path='branch.bundle')
    return dict(head=head, head_ref=head_ref, branch_head=branch, tree=tree_id, wip=wip, bundle=bundle,
                reflog_refs=reflog_refs)


async def ignored(target, folder, git_env=None, *, discard_names=()):
    _, names = await git(target, 'ls-files', '--others', '--ignored', '--exclude-standard',
                             '--directory', '-z', strip=False, env=dict(os.environ, **(git_env or {})))
    result = {'copied': [], 'discarded': [], 'links': [], 'held': []}
    regenerable = REGENERABLE | set(discard_names)
    total = 0

    def visit(path, classify=False):
        nonlocal total
        relative = path.relative_to(target)
        if path.is_symlink():
            result['links'].append({'name': str(relative), 'target': os.readlink(path),
                                    'discarded': classify and path.name in regenerable})
        elif any(fnmatch.fnmatch(path.name, pattern) for pattern in ('.env*', '*.pem', '*.key', '*.p12')):
            result['held'].append(str(relative))
        elif classify and (path.name in regenerable or path.name.endswith('.pyc')):
            result['discarded'].append({'name': str(relative),
                                        'bytes': sum(p.lstat().st_size for p in files(path))})
        elif path.is_dir():
            for child in sorted(path.iterdir()):
                visit(child)
        else:
            info = path.lstat()
            total += info.st_size
            if total > IGNORED_LIMIT:
                raise ValueError('ignored files exceed 200 MB: %s (%d bytes total)' % (relative, total))
            result['copied'].append(dict(copy_file(path, folder / 'ignored' / relative, folder), name=str(relative)))
    for name in names.split('\0'):
        if name:
            await asyncio.to_thread(visit, target / name.rstrip('/'), True)
    return result


async def verify(repository, folder, saved, staged=False):
    _, head = await git(repository, 'rev-parse', saved['head_ref'] if staged else saved['branch'])
    if head != (saved['head'] if staged else saved['branch_head']):
        raise ValueError('saved branch changed')
    if saved.get('wip'):
        _, wip = await git(repository, 'rev-parse', saved['wip_saved_ref'] if staged else saved['wip_ref'])
        if wip != saved['wip']:
            raise ValueError('saved WIP changed')
    if saved.get('reflog_refs'):
        prefix = saved['head_ref'].removesuffix('head') + 'reflog/'
        _, current = await git(repository, 'for-each-ref', '--format=%(refname) %(objectname)', prefix)
        pinned = dict(line.split() for line in current.splitlines())
        for ref, oid in saved['reflog_refs'].items():
            if pinned.get(ref) != oid:
                raise ValueError('saved reflog commit is missing or changed: ' + ref)
    if saved.get('bundle'):
        await git(repository, 'bundle', 'verify', folder / 'branch.bundle')
    entries = [entry for entry in saved['proofs'] if not entry.get('missing')]
    entries += saved['ignored']['copied'] + ([saved['bundle']] if saved.get('bundle') else [])
    for entry in entries:
        path = folder / entry['path']
        if not path.is_relative_to(folder) or path.resolve() != path:
            raise ValueError('saved artifact path changed')
        if await asyncio.to_thread(digest, path) != {key: entry[key] for key in ('bytes', 'sha256')}:
            raise ValueError('saved artifact checksum changed')


async def stage(store, task, target, repository, git_dir, saved, folder):
    root = directory(store.directory / 'worktree-deletions', store.directory)
    stage_path = root / (target.name + '-' + uuid.uuid4().hex)
    stage_path.mkdir(mode=0o700)
    info = git_dir.lstat()
    record = dict(task=task['id'], number=task['number'], target=str(target), repository=str(repository),
                  git_dir=str(git_dir), git_inode=[info.st_dev, info.st_ino], branch=saved['branch'],
                  ignored=[entry['name'] for entry in saved['ignored']['discarded']], detached=False, removed={'bytes': 0, 'files': 0},
                  space_k=task['iteration'])
    saved['freed']['deps'] = sum(entry['bytes'] for entry in saved['ignored']['discarded'])
    checkpoint(store, task, 'verified', saved)
    ws._save_deletion(stage_path, record)
    preconditions(store, task, roots(store, task))
    _, branch = await git_directory(store, repository, target, task)
    if store.get('close_saves_any_branch', False) is True and branch != saved['branch']:
        raise ValueError('late writer changed the copy branch')
    current_ignored = await ignored(target, folder, discard_names=store.get('close_saves_discard_names', []))
    current_proofs = await proofs(store, task, roots(store, task), folder)
    if (await tree(store, target, folder) != saved['tree'] or current_ignored != saved['ignored']
            or current_proofs != saved['proofs']):
        raise ValueError('late writer changed the copy or proofs')
    await verify(repository, folder, saved)
    _, head = await git(target, 'rev-parse', 'HEAD')
    if head != saved['head']:
        raise ValueError('late writer changed the copy HEAD')
    if await reflog_commits(repository, target):
        raise ValueError('late writer added unsaved worktree reflog commits')
    preconditions(store, task, roots(store, task))
    if any((git_dir / name).exists() for name in ('locked', 'index.lock', 'HEAD.lock')) or ws._workspace_operation(git_dir):
        raise ValueError('Git operation or lock changed before staging')
    if target.stat().st_dev != stage_path.stat().st_dev or info.st_dev != stage_path.stat().st_dev:
        raise ValueError('Deletion staging folder is on another filesystem')
    target.rename(stage_path / 'copy')
    ws._detach_staged(stage_path, record)
    checkpoint(store, task, 'staged', saved)


def hold(store, task, error):
    from . import problems
    reason = ' '.join(str(error).splitlines()).strip() or type(error).__name__
    with store.transaction():
        if not closed_round(store, task):
            return
        store.db.execute("UPDATE tasks SET copy='held',copy_reason=? WHERE id=?", (reason, task['id']))
        if reason != task.get('copy_reason') and not store.db.execute(
                "SELECT 1 FROM problems WHERE area='cleanup' AND code='copy-held' AND task=? AND detail=? LIMIT 1",
                (task['id'], problems.sanitize(reason))).fetchone():
            problems.record(store, 'cleanup', 'copy-held', reason, task=task['id'], topic=task['topic'])


async def save_job(store, task_id):
    task = store.task_get(task_id)
    if not task:
        return
    target = ws._workspace_target(store.directory, task_id)
    try:
        owned = roots(store, task)
        preconditions(store, task, owned)
        await stopped_workers(store, task)
        phase, saved = load(store, task)
        if phase == 'cleared' and not target.exists():
            with store.db:
                store.db.execute("UPDATE tasks SET copy='cleared',copy_reason=NULL WHERE id=?", (task_id,))
            return
        topic = store.topic(task['topic'])
        identity = await ws.project_identity(Path(topic['cwd']), timeout=ws.GIT_TIMEOUT)
        repository = Path(identity['project_key'])
        async with ws._repository_lock(store.directory, repository):
            ws._recover_staged_registrations(store.directory, repository)
            folder = directory(store.directory / 'results' / ('task-' + str(task_id)) / 'rounds' / str(task['iteration']), store.directory)
            if target.exists():
                ws._saved_workspace_relative(target, task['worktree'])
                git_dir, branch = await git_directory(store, repository, target, task)
                for attempt in range(2):
                    resnapshots = saved.get('resnapshots', 0)
                    saved = {'proofs': await proofs(store, task, owned, folder), 'resnapshots': resnapshots}
                    checkpoint(store, task, 'proofs', saved)
                    if store.get('close_saves_any_branch', False) is True:
                        saved.update(await snapshot(store, repository, target, folder, task, branch_ref=branch))
                    else:
                        saved.update(await snapshot(store, repository, target, folder, task))
                    saved.update(branch=branch, wip_ref='refs/torii/wip/task-' + str(task_id),
                                 wip_saved_ref='refs/torii/saved/task-%s/%s/wip' % (task_id, task['iteration']),
                                 time=time.time(), notes=task.get('notes') or '')
                    checkpoint(store, task, 'snapshot', saved)
                    saved['ignored'] = await ignored(target, folder, discard_names=store.get('close_saves_discard_names', []))
                    checkpoint(store, task, 'ignored', saved)
                    if saved['ignored']['held']:
                        atomic_json(folder / 'held.json', saved)
                        raise ValueError('secret-looking ignored files are held: ' + ', '.join(saved['ignored']['held']))
                    atomic_json(folder / 'saved.json', saved)
                    checkpoint(store, task, 'manifest', saved)
                    with store.db:
                        store.db.execute('UPDATE task_iterations SET head=?,saved=? WHERE task=? AND k=?',
                                         (saved['head'], str(folder / 'saved.json'), task_id, task['iteration']))
                    await verify(repository, folder, saved)
                    checkpoint(store, task, 'verified', saved)
                    try:
                        await stage(store, task, target, repository, git_dir, saved, folder)
                        break
                    except ValueError as error:
                        if 'late writer' not in str(error) or attempt or saved['resnapshots']:
                            raise
                        saved['resnapshots'] += 1
                        checkpoint(store, task, 'snapshot', saved)
                with store.db:
                    store.db.execute('UPDATE task_iterations SET head=?,saved=? WHERE task=? AND k=?',
                                     (saved['head'], str(folder / 'saved.json'), task_id, task['iteration']))
            elif phase not in ('staged', 'extras'):
                if any(record.get('space_k') == task['iteration'] and record['task'] == task_id
                       for _, record in ws._deletion_stages(store.directory)):
                    checkpoint(store, task, 'staged', saved)
                else:
                    saved['proofs'] = await proofs(store, task, owned, folder)
                    saved.setdefault('ignored', {'copied': []})
                    atomic_json(folder / 'saved.json', saved)
                    checkpoint(store, task, 'staged', saved)
    except (OSError, RuntimeError, ValueError, asyncio.TimeoutError) as error:
        hold(store, task, error)


async def clear_extras(store, task):
    phase, saved = load(store, task)
    if phase not in ('staged', 'extras') or not closed_round(store, task):
        return
    if any(record['task'] == task['id'] and record.get('space_k') == task['iteration']
           for _, record in ws._deletion_stages(store.directory)):
        return
    preconditions(store, task, roots(store, task))
    counts = {'bytes': 0, 'files': 0}
    await stopped_workers(store, task)
    if not closed_round(store, task):
        return
    folder = directory(store.directory / 'results' / ('task-' + str(task['id'])) / 'rounds' / str(task['iteration']), store.directory)
    saved['proofs'] = await proofs(store, task, roots(store, task)[1:], folder)
    with store.transaction():
        if not closed_round(store, task):
            return
        atomic_json(folder / 'saved.json', saved)
        checkpoint(store, task, 'extras', saved)
    if saved.get('head'):
        topic = store.topic(task['topic'])
        identity = await ws.project_identity(Path(topic['cwd']), timeout=ws.GIT_TIMEOUT)
        await verify(Path(identity['project_key']), folder, saved, staged=True)
    stages = saved.setdefault('extras', {})
    for path in roots(store, task)[1:]:
        with store.transaction():
            if not closed_round(store, task):
                return
            preconditions(store, task, roots(store, task))
            entry = stages.get(str(path))
            if entry is None:
                if not path.exists():
                    continue
                staging = directory(folder / 'extras', store.directory) / uuid.uuid4().hex
                entry = {'path': str(staging), 'staged': False}
                stages[str(path)] = entry
                checkpoint(store, task, 'extras', saved)
        with store.transaction():
            if not closed_round(store, task):
                return
            preconditions(store, task, roots(store, task))
            staging = Path(entry['path'])
            if not staging.is_relative_to(folder / 'extras') or staging.parent.resolve() != staging.parent:
                raise ValueError('Invalid scratch deletion stage')
            if not entry['staged']:
                if not staging.exists() and path.exists():
                    path.rename(staging)
                entry['staged'] = True
                checkpoint(store, task, 'extras', saved)
        await ws.delete_tree(staging, progress=counts, paced=True)
    with store.transaction():
        if not closed_round(store, task):
            return
        store.host_logs_changed(task=task['id'])
        store.db.execute('UPDATE task_iterations SET bytes_freed=COALESCE(bytes_freed,0)+? WHERE task=? AND k=?',
                         (counts['bytes'], task['id'], task['iteration']))
        store.db.execute("UPDATE tasks SET copy='cleared',copy_reason=NULL WHERE id=?", (task['id'],))
        saved['freed']['extras'] += counts['bytes']
        checkpoint(store, task, 'cleared', saved)


async def drain(store, limit=DRAIN_BATCH):
    if store.get('disk_guard_dry_run', False) is True:
        from .extension import optional
        guard = optional('disk_guard')
        if guard is not None:
            await guard.observe_leases(store)
    changes_before = store.db.total_changes
    for row in store.db.execute(f"""SELECT id FROM tasks t WHERE copy='saving'
        AND status IN ({','.join('?' for _ in CLOSED)}) AND NOT EXISTS (
        SELECT 1 FROM task_space_saves s WHERE s.task=t.id AND s.k=t.iteration AND s.phase IN ('staged','extras'))
        ORDER BY id LIMIT ?""", (*CLOSED, limit)).fetchall():
        await save_job(store, row['id'])
    return store.db.total_changes > changes_before and pending(store)


async def finish(store):
    for row in store.db.execute("SELECT * FROM tasks WHERE copy='saving' ORDER BY id").fetchall():
        try:
            await clear_extras(store, dict(row))
        except (OSError, RuntimeError, ValueError, asyncio.TimeoutError) as error:
            hold(store, dict(row), error)


async def rebuild_branch(store, task_id, target, repository):
    if store.get('close_saves_any_branch', False) is not True:
        return None, False
    row = store.db.execute('SELECT phase,data FROM task_space_saves WHERE task=? ORDER BY k DESC LIMIT 1', (task_id,)).fetchone()
    if not row or row['phase'] not in ('cleared', 'staged', 'extras', 'restoring', 'restore_failed', 'restored'):
        return None, False
    saved = json.loads(row['data'])
    if not saved.get('head') or saved['branch'] == 'refs/heads/torii/task-' + str(task_id):
        return None, False
    restored = await restore(store, task_id, target, repository) if row['phase'] != 'restored' else False
    return saved['branch'].removeprefix('refs/heads/'), restored


async def restore(store, task_id, target, repository):
    row = store.db.execute('SELECT k,phase,data FROM task_space_saves WHERE task=? ORDER BY k DESC LIMIT 1', (task_id,)).fetchone()
    if not row or row['phase'] not in ('cleared', 'staged', 'extras', 'restoring', 'restore_failed'):
        return False
    saved = json.loads(row['data'])
    if not saved.get('head'):
        return False
    folder = store.directory / 'results' / ('task-' + str(task_id)) / 'rounds' / str(row['k'])
    try:
        await verify(repository, folder, saved, staged=True)
        with store.db:
            store.db.execute("UPDATE task_space_saves SET phase='restoring' WHERE task=? AND k=?", (task_id, row['k']))
        if saved['branch'] != 'refs/heads/torii/task-' + str(task_id):
            if store.get('close_saves_any_branch', False) is not True:
                raise ValueError('close_saves_any_branch is off')
            if not target.exists():
                target.parent.mkdir(parents=True, exist_ok=True)
                await git(repository, 'worktree', 'add', '--detach', target, saved['branch_head'])
            await git_directory(store, repository, target, {'id': task_id})
            await git(target, 'switch', '--no-guess', saved['branch'].removeprefix('refs/heads/'))
        _, branch_head = await git(target, 'rev-parse', 'HEAD')
        if branch_head != saved['branch_head']:
            raise ValueError('branch changed since save; restore the saved branch head before retrying')
        current = await tree(store, target, folder)
        if current != saved['tree']:
            _, head_tree = await git(target, 'rev-parse', 'HEAD^{tree}')
            if current != head_tree:
                raise ValueError('Rebuild found edits; the saved WIP is kept')
            if saved.get('wip') or saved['head'] != saved['branch_head']:
                patch = folder / 'restore.patch'
                await git(repository, 'diff', '--binary', '--full-index', '--output=' + str(patch), saved['branch_head'], saved.get('wip') or saved['head'])
                await git(target, 'apply', '--index', patch)
        await git(target, 'reset', '-q')
        if await tree(store, target, folder) != saved['tree']:
            raise ValueError('Rebuilt tree does not match the saved tree')
        for entry in saved['ignored']['copied']:
            destination = target / entry['name']
            if destination.exists() or destination.is_symlink():
                expected = {key: entry[key] for key in ('bytes', 'sha256')}
                if await asyncio.to_thread(digest, destination) != expected:
                    raise ValueError('ignored file changed during rebuild: ' + entry['name'])
                continue
            await asyncio.to_thread(copy_file, folder / entry['path'], destination, target)
        for entry in saved['ignored'].get('links', []):
            destination = target / entry['name']
            directory(destination.parent, target)
            if entry.get('discarded'):
                continue
            if destination.is_symlink():
                if os.readlink(destination) != entry['target']:
                    raise ValueError('ignored link changed during rebuild: ' + entry['name'])
            else:
                destination.symlink_to(entry['target'])
        if saved.get('wip'):
            await git(repository, 'update-ref', 'refs/torii/wip-restored/task-%s/%s' % (task_id, row['k']), saved['wip'])
            await git(repository, 'update-ref', '-d', saved['wip_ref'], saved['wip'])
        with store.db:
            store.db.execute("UPDATE task_space_saves SET phase='restored' WHERE task=? AND k=?", (task_id, row['k']))
            store.db.execute("UPDATE tasks SET copy='present',copy_reason=NULL WHERE id=?", (task_id,))
    except (OSError, RuntimeError, ValueError, asyncio.TimeoutError) as error:
        reason = 'Rebuild paused: ' + str(error) + '; saved work is kept; retry worktree.create after resolving it'
        if saved['branch'] != 'refs/heads/torii/task-' + str(task_id):
            reason += '; saved path: ' + str(folder)
        with store.db:
            store.db.execute("UPDATE task_space_saves SET phase='restore_failed' WHERE task=? AND k=?", (task_id, row['k']))
            store.db.execute("UPDATE tasks SET copy='held',copy_reason=? WHERE id=?", (reason, task_id))
        raise ValueError(reason) from error

    return True


async def clear_chat_scratch(store, topic):
    if not enabled(store) or store.db.execute("SELECT 1 FROM tasks WHERE topic=? AND status='open'", (topic,)).fetchone():
        return
    path = scratch(store, topic=topic)
    for row in store.db.execute('SELECT image FROM outbox WHERE delivered=0 AND retired=0 AND image IS NOT NULL'):
        if Path(row['image']).absolute().is_relative_to(path) or Path(row['image']).resolve().is_relative_to(path):
            return
    if path.is_symlink() or path.parent.resolve() != path.parent:
        return
    if path.exists():
        try:
            async with ws._repository_lock(store.directory, 'closed-copy-deletion', wait=False) as acquired:
                if acquired:
                    await ws.delete_tree(path, paced=True)
        except OSError as error:
            from . import problems
            problems.record(store, 'workspaces', 'chat-scratch-delete-failed', str(error), topic=topic, every=60)


async def preserve_worker_proofs(store, worker):
    if not enabled(store) or not worker['task']:
        return True
    async with ws._repository_lock(store.directory, 'closed-copy-deletion'):
        task = store.task_get(worker['task'])
        owned = roots(store, task)[2:]
        for row in store.db.execute('SELECT image FROM outbox WHERE delivered=0 AND retired=0 AND image IS NOT NULL'):
            if any(Path(row['image']).absolute().is_relative_to(path) for path in owned):
                return False
        folder = directory(store.directory / 'results' / ('task-' + str(task['id'])) / 'rounds' / str(task['iteration']), store.directory)
        entries = await proofs(store, task, owned, folder)
        for entry in entries:
            if entry.get('missing'):
                continue
            path = folder / entry['path']
            if path.resolve() != path or await asyncio.to_thread(digest, path) != {key: entry[key] for key in ('bytes', 'sha256')}:
                raise ValueError('retained worker proof checksum changed')
        atomic_json(folder / 'worker-proofs.json', entries)
        return True


async def verify_staged(store, stage_path, record):
    if 'space_k' not in record:
        return True
    folder = store.directory / 'results' / ('task-' + str(record['task'])) / 'rounds' / str(record['space_k'])
    saved = json.loads((folder / 'saved.json').read_text())
    repository = Path(record['repository'])
    if (saved['branch'] != 'refs/heads/torii/task-' + str(record['task'])
            and store.get('close_saves_any_branch', False) is not True):
        raise ValueError('close_saves_any_branch is off; saved path: ' + str(folder))
    if store.get('close_saves_any_branch', False) is True:
        task = store.task_get(record['task'])
        if record.get('error') and task and task['iteration'] == record['space_k'] and task['copy'] == 'held':
            return False
    await verify(repository, folder, saved, staged=True)
    if not (stage_path / 'copy').exists() or record.get('deletion_started'):
        return True
    env = {'GIT_DIR': str(repository), 'GIT_WORK_TREE': str(stage_path / 'copy'),
           'GIT_INDEX_FILE': str(stage_path / 'git' / 'index')}
    head_matches = True
    if store.get('close_saves_any_branch', False) is True:
        pointer = (stage_path / 'git' / 'HEAD').read_text().strip()
        branch = pointer.removeprefix('ref: ') if pointer.startswith('ref: ') else None
        if branch:
            code, head = await git(repository, 'rev-parse', '--verify', branch, check=False)
            if code:
                reason = 'saved branch was renamed or deleted after staging: ' + branch
                record['error'] = reason
                ws._save_deletion(stage_path, record)
                if task:
                    hold(store, dict(task, iteration=record['space_k']), reason)
                return False
        else:
            head = pointer
        head_matches = head == saved['head'] and (not branch or branch == saved['branch'])
    current_tree = await tree(store, stage_path / 'copy', folder, env, saved['head'])
    current_ignored = await ignored(stage_path / 'copy', folder, env, discard_names=store.get('close_saves_discard_names', []))
    reflog_env = dict(env, GIT_DIR=str(stage_path / 'git'), GIT_COMMON_DIR=str(repository))
    reflog_matches = not await reflog_commits(repository, stage_path / 'copy', reflog_env)
    if head_matches and reflog_matches and current_tree == saved['tree'] and current_ignored == saved['ignored']:
        record['deletion_started'] = True
        ws._save_deletion(stage_path, record)
        return True
    target, git_dir = Path(record['target']), Path(record['git_dir'])
    if target.exists() or git_dir.exists():
        raise ValueError('late writer changed a staged copy; another copy is already present')
    (stage_path / 'git').rename(git_dir)
    (stage_path / 'copy').rename(target)
    record['detached'] = False
    ws._save_deletion(stage_path, record)
    row = store.db.execute('SELECT data FROM task_space_saves WHERE task=? AND k=?', (record['task'], record['space_k'])).fetchone()
    data = json.loads(row['data'])
    retries = data.get('resnapshots', 0)
    data['resnapshots'] = retries + 1
    task = store.task_get(record['task'])
    checkpoint(store, task, 'verified', data)
    with store.db:
        store.db.execute("UPDATE tasks SET copy=?,copy_reason=? WHERE id=?",
                         ('held' if retries else 'saving', 'late writer changed the copy twice' if retries else None, task['id']))
    return False


def deletion_delay(store):
    due = []
    for _, record in ws._deletion_stages(store.directory):
        if 'space_k' not in record:
            continue
        if record.get('error'):
            task = store.task_get(record['task'])
            if (not task or task['iteration'] != record['space_k']
                    or task['status'] not in CLOSED or task['copy'] != 'saving'):
                continue
        due.append(record.get('retry_at', 0))
    return max(0, min(due) - time.time()) if due else None


async def stopped_workers(store, task):
    from .worker_storage import storage_for, _processes_gone
    storage = storage_for(store)
    target = str(ws._workspace_target(store.directory, task['id']))
    rows = store.db.execute('SELECT * FROM workers WHERE task=? OR cwd=? OR instr(cwd,?)=1',
                             (task['id'], target, target + '/')).fetchall()
    for row in rows:
        if not await storage.inspect(_processes_gone, await storage.host_paths(dict(row))):
            raise ValueError('finished worker processes still use job scratch')
