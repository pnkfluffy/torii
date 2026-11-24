"""Verified round saves, disposable job folders, and same-path workspace rebuilds."""

from array import array
import asyncio
import bisect
from contextlib import contextmanager
import copy
import errno
import fcntl
import filecmp
import sqlite3
from collections import Counter
import fnmatch
import hashlib
import json
import logging
import os
from pathlib import Path
import pwd
import re
import stat
import threading
import unicodedata
import time
from time import time as wall_time
from types import SimpleNamespace
import uuid

from . import cleanup, extension, host_os, problems, scan_process, sweeps, workspaces as ws, worktree_salvage
from .cleanup import SECRETS, UNTRACKED_LIMIT
from .host_os import TRIM_TRASH_PREFIX

CLOSED = ('done', 'dropped', 'waiting', 'interrupted')
PARK_SECONDS = 48 * 60 * 60
PARKED = "status='open' AND EXISTS (SELECT 1 FROM task_parks p WHERE p.task=t.id AND p.k=t.iteration)"
SAVING = ("copy='saving' AND (status IN ('done','dropped','waiting','interrupted') OR " + PARKED + ")")
REGENERABLE = frozenset(('node_modules', 'dist', 'build', '.next', '.turbo', '.venv', 'venv',
                         '__pycache__', '.pytest_cache', '.mypy_cache', '.ruff_cache', 'coverage',
                         'target', '.gradle', '.parcel-cache', '.cache', '.DS_Store'))
IGNORED_LIMIT = 200 * 1024 * 1024
IGNORED_SECONDS = 7 * 86400
RESULTS_SECONDS = 14 * 86400
RESULTS_LARGE = 2 * 1024 ** 3
BLOCKED_SECONDS = 24 * 60 * 60
DRAIN_BATCH = 5
DRAIN_SECONDS = 600
TERMINAL = ('done', 'dropped')
FINISHED = ('done', 'interrupted')
RUNNING = ('job has an unfinished worker', 'a running worker uses the job folder')
NOTICE_SECONDS = 24 * 60 * 60
RETRY_LIMIT = 24 * 60 * 60
FADE_SECONDS = 600
REFERENCE_SCAN_TIMEOUT = 600
REFERENCE_REUSE_SECONDS = 300
REFERENCE_READ_BUDGET = 2 * 1024 ** 3
REFERENCE_CHECKPOINT_BYTES = 512 * 1024 ** 2
REFERENCE_LOCK_POLL = 0.2
CLEAN_FILES_FORMAT = b'torii-clean-files-1\n'
DIGEST_BATCH_FILES = 32
DIGEST_BATCH_BYTES = 256 * 1024 * 1024
CHECKPOINT_MAX_VALUES = 50000
SCRATCH_REFLOG = 'refs/torii/scratch-reflog/'
REBUILD_SCAN_TIMEOUT = 300
DISCARD_SCAN_SECONDS = 15
UNREADABLE = 'unreadable'
UNREADABLE_REPORTS = 5
RECORDS = frozenset(('MANIFEST.md', 'recipe.md', 'REMOVED.md', 'moved-to-scratch.txt'))
SCRIPT_SCAN_IGNORED = REGENERABLE | frozenset(('.git', '.hg', '.svn', 'vendor', 'site-packages',
                                              '.tox', '.nox', '.pnpm-store', '.yarn', 'bower_components', '.vite'))
SCRIPT_SUFFIXES = frozenset(('.py', '.sh', '.zsh', '.bash', '.fish', '.ksh', '.command', '.js', '.jsx',
                             '.mjs', '.cjs', '.ts', '.tsx', '.rb', '.pl', '.php', '.lua', '.r', '.ps1',
                             '.swift', '.applescript'))
RULES = {
    'discard_names': ('ignored files exceed',),
    'rebuildable': ('ignored files exceed',),
}


class LostWork(ValueError):
    """A saved bundle is gone and the repository, its remotes and its other clones answered that they do not
    hold its commits, so a retry cannot help."""


class UnverifiedWork(ValueError):
    """A saved bundle is gone and a remote or another clone that may hold its commits gave no answer, so a retry can help."""


class UnrecordedCopy(ValueError):
    """The job folder exists, the job has no recorded worktree, and the folder is not a bare leftover."""


class ParkInUse(ValueError):
    """A parked job's files are in use or their use could not be checked."""

    def __init__(self, reason, scan_failed=False):
        super().__init__(reason)
        self.scan_failed = scan_failed


def kept(store, task_id):
    return task_id in store.get('close_saves_keep', [])


def pending(store):
    return bool(store.db.execute(f"""SELECT 1 FROM tasks t WHERE {SAVING} AND NOT EXISTS (
        SELECT 1 FROM task_space_saves s WHERE s.task=t.id AND s.k=t.iteration AND s.phase IN ('staged','extras')) LIMIT 1""").fetchone())


def parked(store, task):
    return task['status'] == 'open' and store.db.execute(
        'SELECT 1 FROM task_parks WHERE task=? AND k=?', (task['id'], task['iteration'])).fetchone() is not None


def park_folders(store, task_id):
    return ws._workspace_target(store.directory, task_id), scratch(store, task_id)


def park_idle(store, now=None, idle_seconds=PARK_SECONDS, protected=None):
    """Park each open job that had no queued, running or waiting worker for idle_seconds, as a waiting job is parked.
    A hold left from an earlier save is parked too, so its save runs again. A paused rebuild keeps its hold.
    A job this round already parked is never parked again, whatever its copy state; a held one waits for retry_holds."""
    from . import job_limits
    now = time.time() if now is None else now
    idle = now - idle_seconds
    parked_jobs = []
    candidates = """SELECT t.id,t.iteration FROM tasks t WHERE t.status='open'
            AND (t.copy IN ('present','cleared') OR t.copy IN ('saving','held')
                AND NOT EXISTS (SELECT 1 FROM task_space_saves s
                    WHERE s.task=t.id AND s.k=t.iteration AND s.phase IN ('staged','extras')))
            AND NOT EXISTS (SELECT 1 FROM task_parks p WHERE p.task=t.id AND p.k=t.iteration)
            AND NOT EXISTS (SELECT 1 FROM task_space_saves s WHERE s.task=t.id AND s.phase IN ('restoring','restore_failed')
                AND s.k=(SELECT MAX(k) FROM task_space_saves WHERE task=t.id))
            AND t.updated<? AND NOT EXISTS (SELECT 1 FROM workers w WHERE w.task=t.id
                AND (w.updated>=? OR w.status IN ('queued','running','waiting_for_secret','waiting_for_quota')))
            AND NOT EXISTS (SELECT 1 FROM wait_jobs j JOIN waits w ON w.id=j.wait WHERE j.task=t.id AND w.state='armed')
            ORDER BY t.id"""
    jobs = {}

    def collect(task_id):
        paths = park_folders(store, task_id)
        if not kept(store, task_id) and any(path.exists() for path in paths):
            jobs[task_id] = paths
    sweeps.run(store, 'park', [row['id'] for row in store.db.execute(candidates, (idle, idle))], collect,
               refs=lambda task_id: {'task': task_id})
    if not jobs:
        return []
    failures = []
    used = job_limits.path_users(store.directory, jobs, failures=failures)
    failure = failures[0] if failures else None
    if failure:
        job_limits.report_path_scan_failure(store, failure)
    if protected is not None:
        protected.update(used)
    with store.transaction():
        for row in store.db.execute(candidates, (idle, idle)).fetchall():
            if row['id'] not in jobs or kept(store, row['id']) or not any(path.exists() for path in jobs[row['id']]):
                continue
            if row['id'] in used:
                if used[row['id']] != failure:
                    problems.record(store, 'cleanup', 'park-in-use', used[row['id']], task=row['id'],
                                    every=PARK_SECONDS, scope=row['id'])
                continue
            store.db.execute('INSERT OR REPLACE INTO task_parks(task,k,parked) VALUES (?,?,?)',
                             (row['id'], row['iteration'], now))
            store.db.execute("UPDATE tasks SET copy='saving',copy_reason=NULL WHERE id=?", (row['id'],))
            parked_jobs.append(row['id'])
    return parked_jobs


async def check_park_use(store, task, *staged):
    from . import job_limits
    current = store.task_get(task['id'])
    if current and current['iteration'] == task['iteration'] and parked(store, current):
        job = {task['id']: (*park_folders(store, task['id']), *staged)}
        failures = []
        used = await asyncio.to_thread(job_limits.path_users, store.directory, job, failures=failures)
        failure = failures[0] if failures else None
        if failure:
            job_limits.report_path_scan_failure(store, failure)
        if task['id'] in used:
            raise ParkInUse(used[task['id']], scan_failed=used[task['id']] == failure)


def unpark(store, task_id):
    """A new worker ends a park. The copy is present unless its worktree was saved and moved away; then it is cleared."""
    if store.db.execute('DELETE FROM task_parks WHERE task=?', (task_id,)).rowcount:
        row = store.db.execute("""SELECT s.data FROM task_space_saves s JOIN tasks t ON t.id=s.task AND t.iteration=s.k
            WHERE s.task=? AND s.phase IN ('staged','extras','cleared')""", (task_id,)).fetchone()
        moved = bool(row and json.loads(row['data']).get('head')
                     and not ws._workspace_target(store.directory, task_id).exists())
        store.db.execute("""UPDATE tasks SET copy=?,copy_reason=NULL WHERE id=? AND status='open'
            AND copy IN ('saving','held')""", ('cleared' if moved else 'present', task_id))


async def git(cwd, *args, **options):
    try:
        return await ws._git(cwd, *args, timeout=ws.GIT_TIMEOUT, **options)
    except asyncio.TimeoutError as error:
        raise ValueError('git %s timed out after %gs' % (args[0], ws.GIT_TIMEOUT)) from error


def migrate(store):
    store.db.execute('''CREATE TABLE IF NOT EXISTS task_space_saves (
        task INTEGER NOT NULL REFERENCES tasks(id), k INTEGER NOT NULL,
        phase TEXT NOT NULL, data TEXT NOT NULL, PRIMARY KEY(task,k))''')
    store.db.execute('CREATE TABLE IF NOT EXISTS results_blocked (task INTEGER PRIMARY KEY, path TEXT NOT NULL, reason TEXT NOT NULL, stamp TEXT NOT NULL, since REAL)')
    store.db.execute('CREATE TABLE IF NOT EXISTS results_shrunk (task INTEGER NOT NULL, k INTEGER NOT NULL, closed REAL NOT NULL, PRIMARY KEY(task,k))')
    store.db.execute('CREATE TABLE IF NOT EXISTS results_fade_claims (task INTEGER PRIMARY KEY, stage TEXT NOT NULL, claimed INTEGER NOT NULL)')
    for row in store.db.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND name IN ('task_space_round','task_space_close') AND sql LIKE '%close_clears_space%'").fetchall():
        store.db.execute('DROP TRIGGER ' + row[0])
    store.db.execute('''CREATE TRIGGER IF NOT EXISTS task_space_round AFTER INSERT ON task_iterations
        BEGIN UPDATE task_iterations SET space_outbox_after=(SELECT COALESCE(MAX(id),0) FROM outbox)
        WHERE task=NEW.task AND k=NEW.k; END''')
    store.db.execute('''CREATE TRIGGER IF NOT EXISTS task_space_close AFTER UPDATE OF status ON tasks
        WHEN NEW.status IN ('done','dropped','waiting','interrupted')
        BEGIN UPDATE tasks SET copy='saving',copy_reason=NULL WHERE id=NEW.id; END''')
    store.db.execute('CREATE TABLE IF NOT EXISTS task_parks (task INTEGER PRIMARY KEY, k INTEGER NOT NULL, parked REAL NOT NULL)')
    store.db.execute('''CREATE TRIGGER IF NOT EXISTS task_park_end AFTER UPDATE OF status,iteration ON tasks
        WHEN NEW.status IS NOT OLD.status OR NEW.iteration IS NOT OLD.iteration
        BEGIN DELETE FROM task_parks WHERE task=NEW.id; END''')


def scratch(store, task_id=None, topic=None, create=False):
    path = store.directory / 'scratch' / ('task-' + str(task_id) if task_id is not None else 'topic-' + str(topic))
    if '/' in path.name or path.parent != store.directory / 'scratch':
        raise ValueError('Invalid scratch path')
    if create:
        if task_id is None:
            directory(path, store.directory)
        else:
            from .idle_trim import scratch_guard
            with scratch_guard(store, task_id):
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
    targets = {task_id} if task_id is not None else set()
    if cwd:
        from .worker_storage import temp_base
        for path in {Path(cwd).absolute(), Path(cwd).resolve()}:
            opencode = store.directory / 'opencode' / 'workers'
            for root in (store.directory / 'worktrees', ws.cow_root(store.directory), store.directory / 'scratch', temp_base(), opencode):
                if not path.is_relative_to(root) or path == root:
                    continue
                name = path.relative_to(root).parts[0]
                match = re.fullmatch(r'task-([0-9]+)(?:-.*)?', name) if root != temp_base() else None
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
            AND (status IN ('done','dropped','waiting','interrupted') OR status='open' AND EXISTS (
                SELECT 1 FROM task_parks p WHERE p.task=tasks.id AND p.k=tasks.iteration))""", (target,))


def checkpoint(store, task, phase, data):
    data.setdefault('freed', dict.fromkeys(('copy', 'deps', 'extras', 'prune'), 0))
    checkpoint_encoded(store, task, phase, json.dumps(data))


def checkpoint_encoded(store, task, phase, encoded):
    with store.transaction():
        store.db.execute('''INSERT INTO task_space_saves(task,k,phase,data) VALUES (?,?,?,?)
            ON CONFLICT(task,k) DO UPDATE SET phase=excluded.phase,data=excluded.data''',
                         (task['id'], task['iteration'], phase, encoded))


def encode_checkpoint(data):
    return json.dumps(data)


def checkpoint_values(data):
    stack = [iter((data,))]
    while stack:
        try:
            value = next(stack[-1])
        except StopIteration:
            stack.pop()
            continue
        yield value
        if isinstance(value, dict):
            stack.append(iter(value.values()))
            stack.append(iter(value.keys()))
        elif isinstance(value, (list, tuple)):
            stack.append(iter(value))


def checkpoint_size(values):
    count = size = 0
    for value in values:
        count += 1
        if isinstance(value, str):
            size += len(value)
        if count > CHECKPOINT_MAX_VALUES or size > 4 * 1024 * 1024:
            raise scan_process.ScanError('close-save state exceeds safe scan limit')
    return count


async def check_json_size(store, data):
    values = checkpoint_values(data)
    count = size = 0
    for value in values:
        count += 1
        if isinstance(value, str):
            size += len(value)
        if count > CHECKPOINT_MAX_VALUES or size > 4 * 1024 * 1024:
            report_scan_failure(store, 'file-scan-failed', scan_process.ScanError('close-save state exceeds safe scan limit'))
            raise scan_process.ScanError('close-save state exceeds safe scan limit')
        if count % 256 == 0:
            await asyncio.sleep(0)


async def async_encode_checkpoint(store, data):
    data.setdefault('freed', dict.fromkeys(('copy', 'deps', 'extras', 'prune'), 0))
    await check_json_size(store, data)
    return await async_cleanup_scan(store, encode_checkpoint, data)


async def async_checkpoint(store, task, phase, data):
    stamp = checkpoint_stamp(store, task)
    encoded = await async_encode_checkpoint(store, data)
    with store.transaction():
        if not references_current(store, stamp):
            raise scan_process.ScanError('close-save state changed during checkpoint')
        checkpoint_encoded(store, task, phase, encoded)


def load(store, task):
    row = store.db.execute('SELECT phase,data FROM task_space_saves WHERE task=? AND k=?',
                           (task['id'], task['iteration'])).fetchone()
    return (row['phase'], json.loads(row['data'])) if row else ('pending', {})


def read_checkpoint(directory, task_id, iteration):
    db = sqlite3.connect((directory / 'state.sqlite').as_uri() + '?mode=ro', uri=True)
    try:
        row = db.execute('SELECT phase,data FROM task_space_saves WHERE task=? AND k=?',
                         (task_id, iteration)).fetchone()
    finally:
        db.close()
    if not row:
        return 'pending', {}
    if len(row[1]) > 32 * 1024 * 1024:
        raise scan_process.ScanError('close-save state exceeds safe scan limit')
    data = json.loads(row[1])
    checkpoint_size(checkpoint_values(data))
    return row[0], data


async def async_load(store, task):
    return await async_cleanup_scan(store, read_checkpoint, store.directory, task['id'], task['iteration'])


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


async def async_atomic_json(store, path, value):
    await check_json_size(store, value)
    await async_cleanup_scan(store, atomic_json, path, value)


def prepare_json(path, encoded):
    temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    fd = os.open(str(temporary), os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    return temporary


def commit_json(temporary, path):
    temporary.replace(path)
    fd = os.open(str(path.parent), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def digest(path):
    return digest_with_stamp(path)[0]


def digest_with_stamp(path):
    fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode):
            raise ValueError('Save requires a regular file')
        sha = hashlib.sha256()
        for block in iter(lambda: handle.read(1024 * 1024), b''):
            sha.update(block)
        after = os.fstat(handle.fileno())
    stamp = file_stamp(after)
    if file_stamp(info) != stamp or path_stamp(path) != stamp:
        raise ValueError('File changed during save')
    return {'bytes': info.st_size, 'sha256': sha.hexdigest()}, stamp


def digests_with_stamps(paths):
    """Hash a batch in one scan child: each child start costs 0.1-0.4 s inside the busy service."""
    return [digest_with_stamp(path) for path in paths]


def digest_batches(items, size):
    batch, total = [], 0
    for item in items:
        if batch and (len(batch) >= DIGEST_BATCH_FILES or total + size(item) > DIGEST_BATCH_BYTES):
            yield batch
            batch, total = [], 0
        batch.append(item)
        total += size(item)
    if batch:
        yield batch


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


def file_entries(root):
    return [(path, path.lstat().st_size) for path in files(root) if stat.S_ISREG(path.lstat().st_mode)]


def sorted_result_entries(entries):
    return sum(size for _, size in entries), sorted(entries, key=lambda item: (-item[1], str(item[0])))


def file_stamp(info):
    return None if info is None else (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def path_stamp(path):
    try:
        return file_stamp(path.lstat())
    except FileNotFoundError:
        return None


def folder_identity(path):
    """A shared parent such as the system temp folder changes on every sibling write, so only its identity counts."""
    stamp = path_stamp(path)
    return stamp and stamp[:2]


def read_stamped_file(path):
    try:
        fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return '', None
    with os.fdopen(fd, 'r') as handle:
        before = os.fstat(handle.fileno())
        if not stat.S_ISREG(before.st_mode):
            raise ValueError('Cleanup record must be a regular file')
        content = handle.read()
        after = os.fstat(handle.fileno())
    if file_stamp(before) != file_stamp(after):
        raise ValueError('Cleanup record changed during scan')
    return content, file_stamp(after)


def read_stamped_bytes(path):
    try:
        fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return b'', None
    with os.fdopen(fd, 'rb') as handle:
        before = os.fstat(handle.fileno())
        if not stat.S_ISREG(before.st_mode):
            raise ValueError('Cleanup record must be a regular file')
        content = handle.read()
        after = os.fstat(handle.fileno())
    if file_stamp(before) != file_stamp(after):
        raise ValueError('Cleanup record changed during scan')
    return content, file_stamp(after)


def roots(store, task):
    from .worker_storage import temp_directory
    result = [ws._workspace_target(store.directory, task['id']), scratch(store, task['id'])]
    prefix = 'task-%s-' % task['id']
    for kind in ('scratch', 'worktrees'):
        parent = store.directory / kind
        if parent.is_dir() and not parent.is_symlink():
            result.extend(sorted(path for path in parent.iterdir() if path.name.startswith(prefix)
                                 and path.is_dir() and not path.is_symlink()))
    row = store.db.execute('SELECT data FROM task_space_saves WHERE task=? AND k=?',
                           (task['id'], task['iteration'])).fetchone()
    if row:
        for name in json.loads(row['data']).get('extras', {}):
            path = Path(name)
            if path.parent in (store.directory / 'scratch', store.directory / 'worktrees') and path.name.startswith(prefix):
                result.append(path)
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
    return current and (current['status'] in CLOSED or parked(store, current)) and current['iteration'] == task['iteration']


def pending_send(store, owned):
    return any(Path(row['image']).absolute().is_relative_to(p) or Path(row['image']).resolve().is_relative_to(p)
               for row in store.db.execute('SELECT image FROM outbox WHERE delivered=0 AND retired=0 AND image IS NOT NULL')
               for p in owned)


def result_database_references(db, skip=None):
    texts = [row[0] or '' for row in db.execute("SELECT notes FROM tasks WHERE status IN ('open','waiting','interrupted','proposed') AND id IS NOT ?", (skip,))]
    texts.extend(row[0] or '' for row in db.execute("SELECT k.locator FROM keep_links k JOIN tasks t ON t.id=k.task WHERE t.status IN ('open','waiting','interrupted','proposed') AND t.id IS NOT ?", (skip,)))
    texts.extend(row[0] or '' for row in db.execute("SELECT prompt FROM workers WHERE status IN ('queued','running','waiting_for_secret','waiting_for_quota')"))
    texts.extend(row[0] or '' for row in db.execute("SELECT j.then_prompt FROM wait_jobs j JOIN waits w ON w.id=j.wait WHERE w.state='armed'"))
    return texts


def task_reference_rows(db, task_id, skip):
    marker = 'task-' + str(task_id)
    queries = (
        ("SELECT notes FROM tasks WHERE status IN ('open','waiting','interrupted','proposed') AND id IS NOT ? AND instr(notes,?)", (skip, marker)),
        ("SELECT k.locator FROM keep_links k JOIN tasks t ON t.id=k.task WHERE t.status IN ('open','waiting','interrupted','proposed') AND t.id IS NOT ? AND instr(k.locator,?)", (skip, marker)),
        ("SELECT prompt FROM workers WHERE status IN ('queued','running','waiting_for_secret','waiting_for_quota') AND instr(prompt,?)", (marker,)),
        ("SELECT j.then_prompt FROM wait_jobs j JOIN waits w ON w.id=j.wait WHERE w.state='armed' AND instr(j.then_prompt,?)", (marker,)),
    )
    return tuple(Counter(row[0] for row in db.execute(query, args)) for query, args in queries)


def result_reference_paths(texts):
    for text in texts:
        for match in re.finditer(r"(?:results|scratch)/task-(\d+)(?:/[^\s\x22\x27`),;\]}]*)?", text):
            path = match.group(0).rstrip('/')
            yield match.group(1), path, path.rstrip(".:;,!?)>*|\x27\x22`")
        for match in re.finditer(r"[\x22\x27`](?:[^\x22\x27`\n]*?/)?((?:results|scratch)/task-(\d+)(?:/[^\x22\x27`\n]*)?)[\x22\x27`]", text):
            path = match.group(1).rstrip('/')
            yield match.group(2), path, path.rstrip(".:;,!?)>*|\x27\x22`")


def result_reference_index(texts, existing=None):
    result = {key: set(values) for key, values in (existing or {}).items()}
    for task_id, path, trimmed in result_reference_paths(texts):
        result.setdefault(task_id, set()).update((path, trimmed))
    return result


class CleanFiles:
    """Stamps of files that a reference scan read in full without a match.

    A stamp holds the path, device, inode, size, mtime and ctime, so any write,
    rename or replacement makes the next scan read the file again.
    """

    def __init__(self, path, budget=None):
        self.path = path
        self.budget = budget
        self.known = self.load(path)
        self.seen = array('Q')
        self.read = 0
        self.unsaved = 0

    @staticmethod
    def load(path):
        try:
            with path.open('rb') as handle:
                if handle.read(len(CLEAN_FILES_FORMAT)) != CLEAN_FILES_FORMAT:
                    return array('Q')
                values = array('Q')
                values.frombytes(handle.read())
        except (OSError, ValueError):
            return array('Q')
        if any(values[i] >= values[i + 1] for i in range(len(values) - 1)):
            return array('Q')
        return values

    @staticmethod
    def key(pathname, info):
        digest = hashlib.blake2b(os.fsencode(str(pathname)), digest_size=8, person=b'torii-refscan')
        digest.update(repr(file_stamp(info)).encode())
        return int.from_bytes(digest.digest(), 'little')

    def clean(self, key):
        index = bisect.bisect_left(self.known, key)
        return index < len(self.known) and self.known[index] == key

    def note(self, key):
        self.seen.append(key)

    def charge(self, size):
        if self.budget is not None and self.read + size > self.budget:
            self.save(complete=False)
            raise scan_process.ScanError('reference scan read budget exceeded')
        self.read += size
        self.unsaved += size
        if self.unsaved >= REFERENCE_CHECKPOINT_BYTES:
            self.save(complete=False)

    def save(self, complete):
        """Write the stamps; a failed write only makes the next scan read more."""
        self.unsaved = 0
        values = set(self.seen) if complete else set(self.seen).union(self.known)
        temporary = self.path.with_name(self.path.name + '.tmp')
        try:
            self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            with temporary.open('wb') as handle:
                handle.write(CLEAN_FILES_FORMAT)
                array('Q', sorted(values)).tofile(handle)
            temporary.replace(self.path)
        except OSError:
            pass


def result_script_references(root, limit=None, strict=False, notes=False, clean=None, unreadable=None, markdown=False):
    """Collect the lines of scripts under root that name a job folder; with markdown, also of .md files. A path this user may not read holds no script
    this user can run, so the scan lists it in unreadable and goes on; every other read error stops a strict scan."""
    texts = []

    def failed(error, path):
        if isinstance(error, PermissionError) and unreadable is not None:
            unreadable.add(str(error.filename or path))
        elif strict:
            raise error
    try:
        root_info = root.lstat()
    except FileNotFoundError:
        return texts
    except OSError:
        if strict:
            raise
        return texts
    if not stat.S_ISDIR(root_info.st_mode):
        return texts
    for base, dirs, names in os.walk(root, followlinks=False, onerror=lambda error: failed(error, root)):
        safe_dirs = []
        for name in dirs:
            try:
                if (limit is None or name not in SCRIPT_SCAN_IGNORED) and stat.S_ISDIR((Path(base) / name).lstat().st_mode):
                    safe_dirs.append(name)
            except OSError as error:
                failed(error, Path(base) / name)
        dirs[:] = safe_dirs
        for name in names:
            pathname = Path(base) / name
            try:
                info = pathname.lstat()
                if not stat.S_ISREG(info.st_mode):
                    continue
                if limit is not None and info.st_size > limit:
                    continue
                script = (notes or pathname.suffix.lower() in SCRIPT_SUFFIXES
                          or markdown and pathname.suffix.lower() == '.md')
                if not script and not info.st_mode & 0o111:
                    continue
                key = None
                if clean is not None:
                    key = clean.key(pathname, info)
                    if clean.clean(key):
                        clean.note(key)
                        continue
                fd = os.open(str(pathname), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                with os.fdopen(fd, 'rb') as handle:
                    if not script:
                        if handle.read(2) != b'#!':
                            continue
                        handle.seek(0)
                    if clean is not None:
                        clean.charge(info.st_size)
                    if limit is None:
                        lines = handle
                    else:
                        data = handle.read(limit + 1)
                        if len(data) > limit:
                            continue
                        lines = data.splitlines()
                    found = [line.decode('utf-8') for line in lines if b'results/task-' in line or b'scratch/task-' in line]
                    if key is not None and not found and file_stamp(os.fstat(handle.fileno())) == file_stamp(info):
                        clean.note(key)
                    texts.extend(found)
            except (OSError, UnicodeError) as error:
                failed(error, pathname)
    return texts


def result_references(directory, strict=False, skip=None, launch=None, extra_roots=None, clean=None, budget=None,
                      database=True):
    db = sqlite3.connect(str(directory / 'state.sqlite'), timeout=30)
    try:
        db.execute('BEGIN')
        texts = result_database_references(db, skip) if database else []
        open_jobs = [row[0] for row in db.execute("SELECT id FROM tasks WHERE status IN ('open','waiting','interrupted','proposed') AND id IS NOT ?", (skip,))]
    finally:
        db.close()
    files = None if clean is None else CleanFiles(clean, budget)
    launch = launch or host_os.user_launch_agents(Path(pwd.getpwuid(os.getuid()).pw_dir))
    texts.extend(host_os.launch_agent_texts(launch))
    unreadable = set()
    for root in (extension.active().extra_reference_roots(directory) if extra_roots is None else extra_roots):
        texts.extend(result_script_references(root, strict=strict, clean=files, unreadable=unreadable))
    chats = directory / 'chats'
    if chats.is_dir() and not chats.is_symlink():
        for chat in sorted(chats.iterdir()):
            texts.extend(result_script_references(chat / 'memory', 1024 * 1024, strict=strict, notes=True, clean=files,
                                                  unreadable=unreadable))
    for task_id in open_jobs:
        for folder in ('results', 'scratch', 'worktrees', ws.COW_FOLDER):
            texts.extend(result_script_references(directory / folder / ('task-' + str(task_id)), 1024 * 1024,
                                                  strict=strict, clean=files, unreadable=unreadable,
                                                  markdown=folder == 'results'))
    if files is not None:
        files.save(complete=True)
    references = result_reference_index(texts)
    if unreadable:
        references[UNREADABLE] = unreadable
    return references


def task_reference_index(directory, task_id, skip):
    db = sqlite3.connect(str(directory / 'state.sqlite'), timeout=30)
    try:
        db.execute('BEGIN')
        rows = task_reference_rows(db, task_id, skip)
        texts = (text for group in rows for text in group)
        return {str(task_id): result_reference_index(texts).get(str(task_id), set())}, rows
    finally:
        db.close()


def references_current(store, stamp):
    if len(stamp) == 4 and stamp[0] == 'task-references':
        return task_reference_rows(store.db, stamp[1], stamp[2]) == stamp[3]
    if len(stamp) == 4 and stamp[0] == 'proofs':
        return proof_rows(store.db, stamp[1], stamp[2]) == stamp[3]
    if len(stamp) == 4 and stamp[0] == 'checkpoint':
        return checkpoint_rows(store.db, stamp[1], stamp[2]) == stamp[3]
    if len(stamp) == 4 and stamp[0] == 'preconditions':
        return precondition_rows(store.db, stamp[1], stamp[2]) == stamp[3]
    raise ValueError('Invalid cleanup snapshot')


def checkpoint_rows(db, task_id, iteration):
    return tuple(tuple(row) for row in db.execute(
        'SELECT phase,data FROM task_space_saves WHERE task=? AND k=?', (task_id, iteration)))


def checkpoint_stamp(store, task):
    return ('checkpoint', task['id'], task['iteration'], checkpoint_rows(store.db, task['id'], task['iteration']))


def proof_rows(db, task_key, owned):
    task_id, iteration = task_key
    task = tuple(tuple(row) for row in db.execute('SELECT id,iteration,notes FROM tasks WHERE id=?', (task_id,)))
    previous = tuple(tuple(row) for row in db.execute(
        'SELECT data FROM task_space_saves WHERE task=? AND k<? ORDER BY k DESC LIMIT 1', (task_id, iteration)))
    round_row = tuple(tuple(row) for row in db.execute(
        'SELECT space_outbox_after FROM task_iterations WHERE task=? AND k=?', (task_id, iteration)))
    if not owned:
        return task, previous, round_row, ()
    condition = ' OR '.join('(instr(image,?) OR instr(image_source,?))' for _ in owned)
    args = tuple(value for root in owned for value in (str(root), str(root)))
    outbox = tuple(tuple(row) for row in db.execute(
        'SELECT id,image,image_source,delivered FROM outbox WHERE image IS NOT NULL AND (' + condition + ') ORDER BY id', args))
    return task, previous, round_row, outbox


def report_scan_failure(store, code, error, function=None, path=None):
    try:
        where = ' '.join(str(part) for part in (getattr(function, '__name__', None), path) if part is not None)
        detail = type(error).__name__ + (': ' + str(error) if str(error) else '')
        problems.record(store, 'cleanup', code, (where + ': ' if where else '') + detail, every=60)
    except Exception as failure:
        try:
            ws.logger.warning('cleanup problem reporting failed: %s', type(failure).__name__)
        except Exception:
            pass


def scan_failed(store, error):
    report_scan_failure(store, 'reference-scan-failed', error)


def reference_scan_folder(directory):
    return Path(directory) / 'reference-scan'


def reference_scan_args(store, strict, skip):
    launch = host_os.user_launch_agents(Path(pwd.getpwuid(os.getuid()).pw_dir))
    roots = tuple(extension.active().extra_reference_roots(store.directory))
    shared = strict is True and skip is None
    clean = reference_scan_folder(store.directory) / 'clean-files' if shared else None
    return shared, (result_references, store.directory, strict, skip, launch, roots, clean,
                    REFERENCE_READ_BUDGET if shared else None, not shared)


def with_database_references(store, references):
    """Database references are cheap, so every caller reads them fresh; only the file scan is shared.
    Each path the file scan could not read is reported, so that one such path never stops the cleanup of every job."""
    for path in sorted(references.get(UNREADABLE, ()))[:UNREADABLE_REPORTS]:
        job = re.search(r'/task-(\d+)', path)
        problems.record(store, 'cleanup', 'reference-scan-unreadable', path.removeprefix(str(store.directory) + '/'),
                        task=int(job[1]) if job else None, every=ws.CLEANUP_INTERVAL, scope=path)
    return result_reference_index(result_database_references(store.db, None), references)


def reference_lock_file(directory):
    folder = Path(directory) / 'locks'
    folder.mkdir(mode=0o700, parents=True, exist_ok=True)
    return os.open(str(folder / 'reference-scan.lock'), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)


def try_reference_lock(fd):
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    return True


def shared_reference_result(directory, now=None):
    """Return the result of a reference scan that started less than REFERENCE_REUSE_SECONDS ago, or None."""
    now = wall_time() if now is None else now
    try:
        data = json.loads((reference_scan_folder(directory) / 'result.json').read_text())
        started = data['started']
        if type(started) not in (int, float) or not 0 <= now - started < REFERENCE_REUSE_SECONDS:
            return None
        if data.get('failed') is True:
            raise scan_process.ScanError('reference scan failed recently')
        references = data['references']
        if not isinstance(references, dict) or not all(
                isinstance(key, str) and isinstance(values, list) and all(isinstance(value, str) for value in values)
                for key, values in references.items()):
            return None
    except (OSError, ValueError, KeyError, TypeError):
        return None
    return {key: set(values) for key, values in references.items()}


def save_reference_result(directory, started, references=None):
    folder = reference_scan_folder(directory)
    folder.mkdir(mode=0o700, parents=True, exist_ok=True)
    data = {'started': started, 'failed': True} if references is None else {
        'started': started, 'references': {key: sorted(values) for key, values in references.items()}}
    temporary = folder / 'result.json.tmp'
    temporary.write_text(json.dumps(data))
    temporary.replace(folder / 'result.json')


@contextmanager
def reference_scan_turn(directory):
    """Hold the one reference scan turn for this state folder across threads and processes."""
    fd = reference_lock_file(directory)
    try:
        deadline = time.monotonic() + REFERENCE_SCAN_TIMEOUT + 120
        while not try_reference_lock(fd):
            if time.monotonic() >= deadline:
                raise scan_process.ScanError('reference scan is busy')
            time.sleep(REFERENCE_LOCK_POLL)
        yield
    finally:
        os.close(fd)


def scan_references(store, strict=True, skip=None):
    try:
        shared, args = reference_scan_args(store, strict, skip)
        if not shared:
            return scan_process.run(*args, timeout=REFERENCE_SCAN_TIMEOUT)
        with reference_scan_turn(store.directory):
            reused = shared_reference_result(store.directory)
            if reused is not None:
                return with_database_references(store, reused)
            started = wall_time()
            try:
                references = scan_process.run(*args, timeout=REFERENCE_SCAN_TIMEOUT)
            except Exception:
                save_reference_result(store.directory, started)
                raise
            save_reference_result(store.directory, started, references)
            return with_database_references(store, references)
    except Exception as error:
        scan_failed(store, error)
        raise scan_process.ScanError('reference scan failed') from error


async def async_scan_references(store, strict=True, skip=None):
    try:
        shared, args = reference_scan_args(store, strict, skip)
        if not shared:
            return await scan_process.run_async(*args, timeout=REFERENCE_SCAN_TIMEOUT)
        fd = reference_lock_file(store.directory)
        try:
            deadline = time.monotonic() + REFERENCE_SCAN_TIMEOUT + 120
            while not try_reference_lock(fd):
                if time.monotonic() >= deadline:
                    raise scan_process.ScanError('reference scan is busy')
                await asyncio.sleep(REFERENCE_LOCK_POLL)
            reused = shared_reference_result(store.directory)
            if reused is not None:
                return with_database_references(store, reused)
            started = wall_time()
            try:
                references = await scan_process.run_async(*args, timeout=REFERENCE_SCAN_TIMEOUT)
            except Exception:
                save_reference_result(store.directory, started)
                raise
            save_reference_result(store.directory, started, references)
            return with_database_references(store, references)
        finally:
            os.close(fd)
    except Exception as error:
        scan_failed(store, error)
        raise scan_process.ScanError('reference scan failed') from error


async def async_cleanup_scan(store, function, *args, timeout=scan_process.TIMEOUT):
    try:
        return await scan_process.run_async(function, *args, timeout=timeout)
    except Exception as error:
        report_scan_failure(store, 'file-scan-failed', error, function, args[0] if args else None)
        if function is scan_ignored and isinstance(error, ValueError) and str(error).startswith('ignored files exceed 200 MB:'):
            raise
        if isinstance(error, scan_process.ScanError):
            raise
        raise scan_process.ScanError(str(error) or type(error).__name__) from error


def cleanup_scan(store, function, *args):
    try:
        return scan_process.run(function, *args)
    except Exception as error:
        report_scan_failure(store, 'file-scan-failed', error, function, args[0] if args else None)
        raise


def scan_task_references(store, task_id, references, skip=None):
    try:
        latest, rows = scan_process.run(task_reference_index, store.directory, task_id, skip)
        previous = scan_process.guard_reference_set(references.get(str(task_id), ()))
        current = scan_process.guard_reference_set(latest[str(task_id)])
        merged = scan_process.guard_reference_set(set(previous) | set(current))
    except Exception as error:
        scan_failed(store, error)
        raise scan_process.ScanError('reference scan failed') from error
    return {str(task_id): merged}, ('task-references', task_id, skip, rows)


async def async_scan_task_references(store, task_id, references, skip=None):
    try:
        latest, rows = await scan_process.run_async(task_reference_index, store.directory, task_id, skip)
        previous = scan_process.guard_reference_set(references.get(str(task_id), ()))
        current = scan_process.guard_reference_set(latest[str(task_id)])
        merged = scan_process.guard_reference_set(set(previous) | set(current))
    except Exception as error:
        scan_failed(store, error)
        raise scan_process.ScanError('reference scan failed') from error
    return {str(task_id): merged}, ('task-references', task_id, skip, rows)


def result_referenced(path, root, references):
    paths = references.get(root.name[5:], set())
    folder = root.parent.name + '/' + root.name
    relative = path.relative_to(root)
    return any(folder + ('/' + str(parent) if str(parent) != '.' else '') in paths
               for parent in (relative, *relative.parents))


def blocked_stamp(path):
    try:
        info = path.stat()
        return '%s:%s:%s:%s' % (info.st_dev, info.st_ino, info.st_mtime_ns, info.st_ctime_ns)
    except OSError:
        return 'missing'


def results_blocked(store, task_id):
    row = store.db.execute('SELECT path,stamp FROM results_blocked WHERE task=?', (task_id,)).fetchone()
    if not row:
        return False
    if blocked_stamp(Path(row['path']).parent) == row['stamp']:
        return True
    with store.db:
        store.db.execute('DELETE FROM results_blocked WHERE task=?', (task_id,))
    return False


def block_results(store, task_id, error):
    if not isinstance(error, PermissionError) and error.errno != errno.EACCES:
        return False
    path = Path(error.filename) if error.filename else store.directory / 'results' / ('task-' + str(task_id))
    with store.db:
        store.db.execute('''INSERT INTO results_blocked(task,path,reason,stamp,since) VALUES (?,?,?,?,?)
            ON CONFLICT(task) DO UPDATE SET path=excluded.path,reason=excluded.reason,stamp=excluded.stamp''',
                         (task_id, str(path), str(error), blocked_stamp(path.parent), time.time()))
    ws.logger.warning('results cleanup blocked job=%s path=%s reason=%s', task_id, path, error)
    return True


def keep_tokens(manifest):
    tokens = {value.rstrip('/') for value in re.findall(r'[\w/.-]+', manifest)}
    tokens.update(value.strip().strip('`').rstrip('/') for value in re.split(r'[|\n]', manifest))
    return tokens


def keep_listed(path, root, anchor, manifest, checksum):
    tokens = manifest if isinstance(manifest, set) else keep_tokens(manifest)
    parents = [p for p in path.parents if p.is_relative_to(root) and p != root]
    sources = {str(p) for p in [path, *parents, root]}
    sources.update(str(p.relative_to(anchor)) for p in [path, *parents, root])
    if sources & tokens or checksum and checksum['sha256'] in tokens:
        return True
    return any(parts and all(part.rstrip('/') in tokens for part in parts)
               for value in sources if re.search(r'[^\w/.-]', value)
               for parts in [re.findall(r'[\w/.-]+', value)])


def fade_stage(staging, record):
    return staging / hashlib.sha256((record['path'] + str(record.get('sha256', record.get('bytes')))).encode()).hexdigest()


def trim_groups(entries):
    groups = Counter((p.parent, re.sub(r'\d+(?=\D*$)', '#', p.name.lower()))
                     for p, _ in entries if re.search(r'\d', p.name))
    archive_bases = set()
    for path, _ in entries:
        match = re.fullmatch(r'(.+)\.([zr])\d+', path.name, re.I)
        if match:
            archive_bases.add((path.parent, match[1].lower() + ('.zip' if match[2].lower() == 'z' else '.rar')))
    return groups, archive_bases


def trim_keep(store, path, root, groups, archive_bases):
    relative = path.relative_to(root)
    parents = [p for p in path.parents if p.is_relative_to(root) and p != root]
    if (relative.parts[0] == 'rounds' or path.suffix == '.bundle'
            or path.name in ('moved-to-scratch.txt', 'trim-preview.txt')
            or any(p.suffix or (p / 'Info.plist').exists() or (p / 'Contents' / 'Info.plist').exists() for p in parents)
            or groups[(path.parent, re.sub(r'\d+(?=\D*$)', '#', path.name.lower()))] > 1
            or re.search(r'\.(?:(?:zip|7z)\.\d+|part\d+\.rar|[zr]\d+)$', path.name, re.I)
            or (path.parent, path.name.lower()) in archive_bases or pending_send(store, [path])):
        return True
    fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as handle:
        return handle.readline(32) in (b'# v2 git bundle\n', b'# v3 git bundle\n')


def saved_keeps(db, task_id):
    sources = {row[0] for row in db.execute('SELECT source FROM keep_queue WHERE task=?', (task_id,))}
    names = {Path(row[1]).name.removeprefix(row[0] + '-')
             for row in db.execute('SELECT id,locator FROM keep_links WHERE task=?', (task_id,))}
    return sources, names


def git_bundle(path):
    if path.suffix.lower() == '.bundle':
        return True
    fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as handle:
        return handle.readline(32) in (b'# v2 git bundle\n', b'# v3 git bundle\n')


def fade_plan(entries, root, anchor, tokens, keeps=(set(), set())):
    planned = []
    has_hash = any(re.fullmatch(r'[0-9a-fA-F]{64}', token) for token in tokens)
    sources, names = keeps
    for path, size in entries:
        relative = path.relative_to(root)
        if (relative.parts[0] == 'rounds' or len(relative.parts) == 1 and path.suffix.lower() == '.md'
                or path.name in RECORDS or str(path) in sources or path.name in names
                or keep_listed(path, root, anchor, tokens, None)):
            continue
        planned.append((path, size))
    return planned, has_hash


def fade_batch(store, task, root, manifest_path, manifest, tokens, entries, seen, references):
    worker = copy.copy(store)
    worker.db = sqlite3.connect(str(store.directory / 'state.sqlite'), timeout=30)
    worker.db.row_factory = sqlite3.Row
    journal_temp = None
    prepared = []
    try:
        candidates, has_hash = fade_plan(entries, root, store.directory, tokens, saved_keeps(worker.db, task['id']))
        planned = []
        for path, size in candidates:
            if result_referenced(path, root, references) or pending_send(worker, [path]) or git_bundle(path):
                continue
            before = path.lstat()
            if before.st_size != size:
                raise ValueError('Results file changed during fade')
            if has_hash and keep_listed(path, root, store.directory, tokens, cleanup_scan(worker, digest, path)):
                continue
            planned.append((path, before, {'bytes': size, 'path': str(path.relative_to(root))}))
        if not planned:
            return True
        journal = root / 'REMOVED.md'
        staging = scratch(store, task['id']) / 'results-fade'
        new = [record for _, _, record in planned if (record['path'], record['bytes']) not in seen]
        journal_before = None
        journal_temp = None
        if new:
            directory(staging, store.directory)
            content, journal_before = cleanup_scan(worker, read_stamped_bytes, journal)
            journal_temp = staging / ('journal-' + uuid.uuid4().hex)
            fd = os.open(str(journal_temp), os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, 'wb') as handle:
                handle.write(content)
                if content and not content.endswith(b'\n'):
                    handle.write(b'\n')
                for record in new:
                    handle.write(('- ' + json.dumps(record, sort_keys=True) + '\n').encode())
                handle.flush()
                os.fsync(handle.fileno())
        for _, _, record in planned:
            staged = directory(fade_stage(staging, record), store.directory)
            prepared.append(staged)
            intent = staged / 'intent.json'
            fd = os.open(str(intent), os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, 'w') as handle:
                json.dump(record, handle, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            fd = os.open(str(staged), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        fd = os.open(str(staging), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        fd = os.open(str(staging.parent), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        latest, manifest_stamp = cleanup_scan(worker, read_stamped_file, manifest_path)
        live, stamp = scan_task_references(worker, task['id'], references)
        keeps = saved_keeps(worker.db, task['id'])
        kept_now, _ = fade_plan([(p, record['bytes']) for p, _, record in planned], root, store.directory,
                                tokens, keeps)
        with type(worker).transaction(worker):
            current = worker.task_get(task['id'])
            iteration = worker.db.execute('SELECT closed,outcome FROM task_iterations WHERE task=? AND k=?',
                                          (task['id'], task['iteration'])).fetchone()
            if (not current or current['iteration'] != task['iteration']
                    or current['status'] not in ('done', 'dropped') or current['copy'] in ('held', 'saving')
                    or kept(worker, task['id']) or not iteration or iteration['outcome'] not in ('done', 'dropped')
                    or iteration['closed'] is None
                    or worker.db.execute("SELECT 1 FROM workers WHERE task=? AND status NOT IN ('done','interrupted') LIMIT 1",
                                         (task['id'],)).fetchone()):
                return False
            if latest != manifest or manifest_stamp != path_stamp(manifest_path) or not references_current(worker, stamp) or any(pending_send(worker, [p]) or
                    (lambda after: (after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns) !=
                     (before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns))(p.lstat())
                    for p, before, _ in planned):
                return False
            if (keeps != saved_keeps(worker.db, task['id']) or len(kept_now) != len(planned)
                    or any(result_referenced(p, root, live) for p, _, _ in planned)):
                return False
            if journal_temp is not None:
                if journal_before != path_stamp(journal):
                    return False
                journal_temp.replace(journal)
                seen.update((record['path'], record['bytes']) for record in new)
            for path, _, record in planned:
                staged = fade_stage(staging, record)
                path.rename(staged / 'data')
        fd = os.open(str(root), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        return True
    finally:
        if journal_temp is not None and journal_temp.exists():
            journal_temp.unlink()
        for staged in prepared:
            if not (staged / 'data').exists():
                intent = staged / 'intent.json'
                if intent.exists():
                    intent.unlink()
                staged.rmdir()
        worker.db.close()


def fade_restore(store, root, staging, tokens, references, keeps=(set(), set()), restore_all=False):
    journal = root / 'REMOVED.md'
    if journal.is_symlink():
        raise ValueError('Results fade record is a symbolic link')
    seen = set()
    lines = journal.read_text().splitlines() if journal.exists() else []
    recorded = set(lines)
    recorded_stages = set()
    for line in lines:
        try:
            record = json.loads(line[2:]) if line.startswith('- ') else None
            if isinstance(record, dict) and isinstance(record.get('path'), str) and isinstance(record.get('bytes'), int):
                recorded_stages.add(fade_stage(staging, record))
        except (ValueError, TypeError):
            continue
    recovered = []
    if staging.is_dir() and not staging.is_symlink():
        for stage in staging.iterdir():
            if stage.is_symlink() or not stage.is_dir() or not (stage / 'data').is_file():
                continue
            try:
                record = json.loads((stage / 'intent.json').read_text())
                if (not isinstance(record, dict) or not isinstance(record.get('path'), str)
                        or not isinstance(record.get('bytes'), int) or fade_stage(staging, record) != stage):
                    raise ValueError('Invalid results fade intent')
                line = '- ' + json.dumps(record, sort_keys=True)
                if line not in recorded:
                    recorded.add(line)
                    recovered.append(line)
            except (OSError, ValueError, TypeError):
                if stage not in recorded_stages:
                    raise ValueError('Staged result has no valid record') from None
    if recovered:
        fd = os.open(str(journal), os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'a') as handle:
            if lines:
                handle.write('\n')
            handle.write('\n'.join(recovered) + '\n')
            handle.flush()
            os.fsync(handle.fileno())
        fd = os.open(str(root), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        lines.extend(recovered)
    for line in lines:
        try:
            record = json.loads(line[2:]) if line.startswith('- ') else None
        except (ValueError, TypeError):
            continue
        if not isinstance(record, dict) or not isinstance(record.get('path'), str) or not isinstance(record.get('bytes'), int):
            continue
        seen.add((record['path'], record['bytes']))
        relative = Path(record['path'])
        if relative.is_absolute() or '..' in relative.parts or not relative.parts:
            continue
        staged = fade_stage(staging, record) / 'data'
        original = root / relative
        if staged.is_file() and (restore_all or result_referenced(original, root, references) or str(original) in keeps[0]
                                 or original.name in keeps[1] or
                                 keep_listed(original, root, store.directory, tokens, record if 'sha256' in record else None)):
            if original.exists():
                raise ValueError('Cannot restore staged results over an existing file')
            directory(original.parent, store.directory)
            staged.rename(original)
    return seen


def scan_fade_restore(directory, root, staging, tokens, references, keeps, restore_all=False):
    return fade_restore(SimpleNamespace(directory=directory), root, staging, tokens, references, keeps, restore_all)


async def recover_fade_claim(store, task_id, keep=False):
    row = store.db.execute('SELECT stage,claimed FROM results_fade_claims WHERE task=?', (task_id,)).fetchone()
    if not row:
        return
    staging = scratch(store, task_id) / 'results-fade'
    name = row['stage']
    if not re.fullmatch(r'results-fade-claim-[0-9a-f]{32}', name) or staging.parent.resolve() != staging.parent:
        raise ValueError('Invalid results fade claim')
    claimed = staging.parent / name
    if staging.is_symlink() or claimed.is_symlink():
        raise ValueError('Symbolic link in results fade claim')
    source_exists, claim_exists = staging.exists(), claimed.exists()
    if source_exists and claim_exists:
        raise ValueError('Conflicting results fade claim')
    if row['claimed']:
        if source_exists:
            raise ValueError('Claimed results fade stage was replaced')
        if claim_exists:
            with store.transaction():
                store.db.execute('UPDATE results_fade_claims SET claimed=0 WHERE task=? AND stage=?', (task_id, name))
            claimed.rename(staging)
    else:
        if not source_exists and not claim_exists:
            raise ValueError('Results fade claim is missing')
        if claim_exists:
            claimed.rename(staging)
    if not keep:
        with store.transaction():
            store.db.execute('DELETE FROM results_fade_claims WHERE task=? AND stage=?', (task_id, name))


async def claim_fade_stage(store, task, root, staging, tokens, references, manifest_path, manifest_stamp):
    task_id = task['id']
    name = 'results-fade-claim-' + uuid.uuid4().hex
    claimed = staging.parent / name
    with store.transaction():
        store.db.execute('INSERT INTO results_fade_claims(task,stage,claimed) VALUES (?,?,0)', (task_id, name))
    try:
        live, stamp = await async_scan_task_references(store, task_id, references)
        keeps = saved_keeps(store.db, task_id)
        await async_cleanup_scan(store, scan_fade_restore, store.directory, root, staging, tokens, live, keeps)
        stage_stamp = path_stamp(staging)
    except BaseException:
        with store.transaction():
            store.db.execute('DELETE FROM results_fade_claims WHERE task=? AND stage=? AND claimed=0', (task_id, name))
        raise
    with store.transaction():
        current = store.task_get(task_id)
        if (not references_current(store, stamp) or keeps != saved_keeps(store.db, task_id)
                or not current or current['iteration'] != task['iteration']
                or current['status'] not in ('done', 'dropped') or current['copy'] in ('held', 'saving')
                or kept(store, task_id) or pending_send(store, [root])
                or store.db.execute("SELECT 1 FROM workers WHERE task=? AND status NOT IN ('done','interrupted') LIMIT 1",
                                    (task_id,)).fetchone()
                or path_stamp(manifest_path) != manifest_stamp or path_stamp(staging) != stage_stamp
                or stage_stamp is None or staging.is_symlink() or claimed.exists() or claimed.is_symlink()):
            store.db.execute('DELETE FROM results_fade_claims WHERE task=? AND stage=? AND claimed=0', (task_id, name))
            return None
        staging.rename(claimed)
        store.db.execute('UPDATE results_fade_claims SET claimed=1 WHERE task=? AND stage=?', (task_id, name))
    try:
        await ws.delete_tree(claimed, paced=True)
    except BaseException:
        await recover_fade_claim(store, task_id, keep=True)
        raise
    with store.transaction():
        store.db.execute('DELETE FROM results_fade_claims WHERE task=? AND stage=? AND claimed=1', (task_id, name))
    return live


def fade_mark(store, task):
    worker = copy.copy(store)
    worker.db = sqlite3.connect(str(store.directory / 'state.sqlite'), timeout=30)
    worker.db.row_factory = sqlite3.Row
    try:
        with type(worker).transaction(worker):
            current = worker.task_get(task['id'])
            iteration = worker.db.execute('SELECT closed,outcome FROM task_iterations WHERE task=? AND k=?',
                                          (task['id'], task['iteration'])).fetchone()
            if (current and current['iteration'] == task['iteration'] and current['status'] in ('done', 'dropped')
                    and current['copy'] not in ('held', 'saving') and not kept(worker, task['id']) and iteration
                    and iteration['outcome'] in ('done', 'dropped') and iteration['closed'] is not None):
                worker.db.execute('INSERT OR REPLACE INTO results_shrunk VALUES (?,?,?)',
                                  (task['id'], task['iteration'], iteration['closed']))
    finally:
        worker.db.close()


async def fade_results(store):
    await sweeps.arun(store, 'fade-claim-recovery', [row['task'] for row in store.db.execute(
        'SELECT task FROM results_fade_claims ORDER BY task')], lambda task_id: recover_fade(store, task_id),
        refs=lambda task_id: {'task': task_id})
    try:
        references = await async_scan_references(store)
    except Exception as error:
        problems.record(store, 'cleanup', 'reference-scan-failed', type(error).__name__, level=logging.ERROR, every=3600)
        return False
    deadline = time.monotonic() + FADE_SECONDS
    rows = store.db.execute('''SELECT t.id FROM tasks t JOIN task_iterations i
        ON i.task=t.id AND i.k=t.iteration WHERE t.status IN ('done','dropped') AND t.copy NOT IN ('held','saving')
        AND i.outcome IN ('done','dropped') AND i.closed IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM results_shrunk f WHERE f.task=t.id AND f.k=i.k AND f.closed=i.closed)
        ORDER BY t.id''').fetchall()
    await sweeps.arun(store, 'fade', [row['id'] for row in rows],
                      lambda task_id: fade_job(store, task_id, references, deadline),
                      refs=lambda task_id: {'task': task_id}, deadline=deadline)
    return time.monotonic() >= deadline


async def recover_fade(store, task_id):
    await recover_fade_claim(store, task_id, keep=True)
    staging = scratch(store, task_id) / 'results-fade'
    root = store.directory / 'results' / ('task-' + str(task_id))
    if staging.exists():
        if root.resolve() != root or staging.resolve() != staging:
            raise ValueError('Unsafe results fade recovery path')
        await async_cleanup_scan(store, scan_fade_restore, store.directory, root, staging, set(), {},
                                 (set(), set()), True)
    with store.transaction():
        store.db.execute('DELETE FROM results_fade_claims WHERE task=?', (task_id,))


async def fade_job(store, task_id, references, deadline):
    task = store.task_get(task_id)
    root = store.directory / 'results' / ('task-' + str(task_id))
    if (sweeps.waiting(store, 'fade-claim-recovery', task_id) or task['copy'] in ('held', 'saving')
            or kept(store, task_id) or results_blocked(store, task_id)):
        return
    try:
        await recover_fade_claim(store, task['id'])
        if not root.exists():
            await asyncio.to_thread(fade_mark, store, task)
            return
        if not root.is_dir() or root.resolve() != root:
            return
        await stopped_workers(store, task)
        topic = store.topic(task['topic'])
        project = Path(topic['cwd'])
        try:
            identity = await ws.project_identity(project, timeout=ws.GIT_TIMEOUT)
            project = Path(identity['project_root'])
        except FileNotFoundError:
            if project.exists():
                raise
        manifest_path = project / 'KEEP' / 'MANIFEST.md'
        manifest, manifest_stamp = await async_cleanup_scan(store, read_stamped_file, manifest_path)
        tokens = await async_cleanup_scan(store, keep_tokens, manifest)
        staging = scratch(store, task['id']) / 'results-fade'
        keeps = saved_keeps(store.db, task['id'])
        seen = await async_cleanup_scan(store, scan_fade_restore, store.directory, root, staging, tokens, references, keeps)
        entries = await async_cleanup_scan(store, file_entries, root)
        complete = True
        for start in range(0, len(entries), 32):
            if time.monotonic() >= deadline:
                return
            complete = await asyncio.to_thread(fade_batch, store, task, root, manifest_path, manifest,
                                               tokens, entries[start:start + 32], seen, references)
            if not complete:
                break
        if not complete:
            return
        if staging.exists():
            if staging.resolve() != staging:
                raise ValueError('Invalid results fade stage')
            live = await claim_fade_stage(store, task, root, staging, tokens, references, manifest_path, manifest_stamp)
            if live is None:
                return
            try:
                staging.parent.rmdir()
            except OSError:
                pass
        else:
            live, stamp = await async_scan_task_references(store, task['id'], references)
            if not references_current(store, stamp):
                return
        protected = await asyncio.to_thread(
            lambda: any(result_referenced(path, root, live) for path, _ in entries))
        if not protected:
            await asyncio.to_thread(fade_mark, store, task)
    except OSError as error:
        if not block_results(store, task_id, error):
            raise


async def trim_results(store, task, project, references):
    if results_blocked(store, task['id']):
        return
    try:
        await trim_results_unblocked(store, task, project, references)
    except OSError as error:
        if not block_results(store, task['id'], error):
            raise


async def trim_results_unblocked(store, task, project, references):
    current = store.task_get(task['id'])
    if current['status'] not in ('done', 'dropped') or current['copy'] == 'held' or kept(store, task['id']):
        return
    root = directory(store.directory / 'results' / ('task-' + str(task['id'])), store.directory)
    entries = await async_cleanup_scan(store, file_entries, root)
    total, entries = await async_cleanup_scan(store, sorted_result_entries, entries)
    manifest_path = project / 'KEEP' / 'MANIFEST.md'
    manifest, manifest_stamp = await async_cleanup_scan(store, read_stamped_file, manifest_path)
    tokens = await async_cleanup_scan(store, keep_tokens, manifest)
    journal = root / 'moved-to-scratch.txt'
    groups, archive_bases = await async_cleanup_scan(store, trim_groups, entries)
    index = 0
    while True:
        chunk, budget = [], total
        while index < len(entries):
            path, size = entries[index]
            if index % 32 == 0:
                await asyncio.sleep(0)
            if size <= 10 * 1024 * 1024 and budget <= 50 * 1024 * 1024:
                break
            if chunk and (len(chunk) >= DIGEST_BATCH_FILES or sum(item[1] for item in chunk) + size > DIGEST_BATCH_BYTES):
                break
            index += 1
            if references is None or result_referenced(path, root, references) or trim_keep(store, path, root, groups, archive_bases):
                continue
            chunk.append((path, size))
            budget -= size
        if not chunk:
            break
        stamped = await async_cleanup_scan(store, digests_with_stamps, [path for path, _ in chunk])
        await prepare_preconditions(store, task)
        current = store.task_get(task['id'])
        if current['status'] not in ('done', 'dropped') or current['copy'] == 'held' or kept(store, task['id']):
            return
        destinations = [directory(scratch(store, task['id'], create=True) / 'results' / path.relative_to(root).parent,
                                  store.directory) / (path.name + '.' + uuid.uuid4().hex) for path, _ in chunk]
        content, journal_stamp = await async_cleanup_scan(store, read_stamped_file, journal)
        recorded_lines = set(content.splitlines(keepends=True))
        precondition_stamp, identities = await prepare_preconditions(store, task)
        live, stamp = await async_scan_task_references(store, task['id'], references)
        for position, ((path, size), (checksum, file_identity)) in enumerate(zip(chunk, stamped)):
            relative = path.relative_to(root)
            if keep_listed(path, root, store.directory, tokens, checksum):
                continue
            if pending_send(store, [path]):
                continue
            if checksum['bytes'] != size:
                raise ValueError('Results file changed during close')
            await asyncio.sleep(0)
            line = json.dumps(dict(checksum, path=str(relative)), sort_keys=True) + '\n'
            with store.transaction():
                if (not preconditions_current(store, precondition_stamp, identities, scratch(store, task['id']))
                        or not references_current(store, stamp) or result_referenced(path, root, live) or pending_send(store, [path])
                        or not closed_round(store, task) or kept(store, task['id']) or path_stamp(journal) != journal_stamp
                        or path_stamp(manifest_path) != manifest_stamp or path_stamp(path) != file_identity):
                    continue
                flags = os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW
                if journal_stamp is None:
                    flags |= os.O_CREAT | os.O_EXCL
                fd = os.open(str(journal), flags, 0o600)
                with os.fdopen(fd, 'a') as handle:
                    info = os.fstat(handle.fileno())
                    if not stat.S_ISREG(info.st_mode) or journal_stamp is not None and file_stamp(info) != journal_stamp:
                        raise ValueError('Results move record must be a regular file')
                    if line not in recorded_lines:
                        handle.write(line)
                        handle.flush()
                        os.fsync(handle.fileno())
                        recorded_lines.add(line)
                        total += len(line.encode())
                    journal_stamp = file_stamp(os.fstat(handle.fileno()))
                fd = os.open(str(root), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
                if path_stamp(path) != file_identity:
                    continue
                path.rename(destinations[position])
            total -= size


def preconditions(store, task, owned):
    current = store.task_get(task['id'])
    if not (current['status'] in CLOSED or parked(store, current)) or current['iteration'] != task['iteration']:
        raise ValueError('job is open or its round changed')
    iteration = store.db.execute('SELECT * FROM task_iterations WHERE task=? AND k=?',
                                  (task['id'], task['iteration'])).fetchone()
    if current['status'] == 'interrupted' and {'cause', 'resume_at'}.issubset(iteration.keys()):
        if iteration['cause'] == 'crash' and iteration['resume_at'] is not None and 0 <= iteration['resume_at'] - time.time() <= 600:
            raise ValueError('a short crash retry keeps the copy until the resume event')
    if (store.get('topic_lifecycle:' + task['topic']) or {}).get('state') == 'closing':
        raise ValueError('chat is closing')
    for row in store.db.execute('SELECT task,cwd FROM workers WHERE status NOT IN (?,?)', FINISHED):
        if row['task'] == task['id'] or row['cwd'] and any(Path(row['cwd']).resolve().is_relative_to(p) for p in owned):
            raise ValueError('job has an unfinished worker')
    if pending_send(store, owned):
        raise ValueError("a Telegram file queued from it hasn't been sent yet")
    for path in owned:
        if path.is_symlink() or path.parent.resolve() != path.parent or path.exists() and not path.is_dir():
            raise ValueError('Symbolic link in owned folder')


def scan_preconditions(directory, task):
    from .store import Store
    worker = Store(directory, token_file=directory / 'cleanup-no-token', read_only=True)
    try:
        worker.db.execute('BEGIN')
        owned = roots(worker, task)
        preconditions(worker, task, owned)
        identities = {str(path): (path_stamp(path), folder_identity(path.parent)) for path in owned}
        return identities, precondition_rows(worker.db, task['id'], task['iteration'])
    finally:
        worker.close()


async def prepare_preconditions(store, task):
    identities, rows = await async_cleanup_scan(store, scan_preconditions, store.directory, task)
    return ('preconditions', task['id'], task['iteration'], rows), identities


def precondition_rows(db, task_id, iteration):
    queries = (
        ('SELECT * FROM tasks WHERE id=?', (task_id,)),
        ('SELECT * FROM task_iterations WHERE task=? AND k=?', (task_id, iteration)),
        ("SELECT key,value FROM settings WHERE key='close_saves_keep' OR key=(SELECT 'topic_lifecycle:' || topic FROM tasks WHERE id=?) ORDER BY key", (task_id,)),
        ("SELECT id,task,cwd,status,provider FROM workers WHERE task=? OR status NOT IN ('done','interrupted') ORDER BY id", (task_id,)),
        ('SELECT worker,path FROM worker_temp_state WHERE worker IN (SELECT id FROM workers WHERE task=?) ORDER BY worker', (task_id,)),
        ('SELECT id,image FROM outbox WHERE delivered=0 AND retired=0 AND image IS NOT NULL ORDER BY id', ()),
    )
    return tuple(tuple(tuple(row) for row in db.execute(query, args)) for query, args in queries)


def preconditions_current(store, stamp, identities, path):
    expected = identities.get(str(path))
    return bool(expected is not None and references_current(store, stamp)
                and set(identities) == {str(root) for root in roots(store, store.task_get(stamp[1]))}
                and expected == (path_stamp(path), folder_identity(path.parent)))


def capture_sent_attachment(store, source, staged):
    if not source:
        return
    path = Path(source)
    for parent in (store.directory / 'worktrees', ws.cow_root(store.directory), store.directory / 'scratch'):
        if not path.is_relative_to(parent):
            continue
        parts = path.relative_to(parent).parts
        match = re.fullmatch(r'task-([0-9]+)(?:-.*)?', parts[0]) if parts else None
        if not match:
            return
        task = store.task_get(int(match[1]))
        if not task:
            return
        root = parent / parts[0]
        if any(fnmatch.fnmatch(part, pattern) for part in path.relative_to(root).parts
               for pattern in ('.env*', '*.pem', '*.key', '*.p12')):
            raise ValueError('secret-looking proof is held: ' + path.name)
        folder = directory(store.directory / 'results' / ('task-' + match[1]) / 'rounds' / str(task['iteration']), store.directory)
        manifest = folder / 'saved.json'
        if manifest.exists() and any(entry['source'] == source for entry in json.loads(manifest.read_text()).get('proofs', [])):
            return
        owner = hashlib.sha256(str(root).encode()).hexdigest()
        destination = folder / 'proofs' / owner / path.relative_to(root)
        entry = dict(copy_file(staged, destination, folder), source=source)
        retained_path = folder / 'worker-proofs.json'
        retained = json.loads(retained_path.read_text()) if retained_path.exists() else []
        retained = [item for item in retained if item['source'] != source] + [entry]
        atomic_json(retained_path, retained)
        return


def proof_candidates(store, task, owned, listed=None):
    job_scratch = scratch(store, task['id'])
    if listed is None:
        prefix = 'task-%s-' % task['id']
        listed = [path for root in owned if root == job_scratch or
                  root.parent == job_scratch.parent and root.name.startswith(prefix)
                  for path in files(root / 'proofs')]
    candidates = set(listed)
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
    copies = {}
    for row in store.db.execute('SELECT image,image_source FROM outbox WHERE image IS NOT NULL AND (delivered=0 OR id>?)',
                                (round_row['space_outbox_after'],)):
        path = Path(row['image_source'] or row['image'])
        if any(path.is_relative_to(root) for root in owned):
            candidates.add(path)
            if row['image_source']:
                copies[path] = Path(row['image'])
    return candidates, copies


def scan_proof_candidates(directory, task, owned):
    db = sqlite3.connect(str(directory / 'state.sqlite'), timeout=30)
    db.row_factory = sqlite3.Row
    try:
        db.execute('BEGIN')
        current = db.execute('SELECT * FROM tasks WHERE id=?', (task['id'],)).fetchone()
        if not current or current['iteration'] != task['iteration']:
            raise ValueError('Job changed during proof scan')
        candidates, copies = proof_candidates(SimpleNamespace(directory=directory, db=db), dict(current), owned)
        rows = proof_rows(db, (task['id'], task['iteration']), owned)
        return candidates, copies, rows
    finally:
        db.close()


def read_json(path):
    return json.loads(path.read_text())


def read_json_stamped(path):
    content, stamp = read_stamped_file(path)
    if stamp is None:
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(path))
    return json.loads(content), stamp


def scan_retained_proofs(folder):
    retained_path = folder / 'worker-proofs.json'
    retained = read_json(retained_path) if retained_path.exists() else []
    manifest_path = folder / 'saved.json'
    if manifest_path.exists():
        retained.extend(read_json(manifest_path).get('proofs', []))
    return list({entry['source']: entry for entry in retained}.values())


def copy_proof_file(source, destination, anchor):
    try:
        return copy_file(source, destination, anchor)
    except FileNotFoundError as error:
        return error


async def proofs(store, task, owned, folder, strict=True, with_stamp=False):
    candidates, copies, rows = await async_cleanup_scan(store, scan_proof_candidates, store.directory, task, owned)
    stamp = ('proofs', (task['id'], task['iteration']), tuple(owned), rows)
    if not references_current(store, stamp):
        raise ValueError('Database changed during proof scan')
    retained = await async_cleanup_scan(store, scan_retained_proofs, folder)
    retained_by_source = {entry['source']: entry for entry in retained}
    result = list(retained)
    for path in sorted(candidates):
        root = next(root for root in owned if path.is_relative_to(root))
        if str(path) in retained_by_source and (not path.exists() or path in copies and not copies[path].exists()):
            continue
        unsafe = ('Proof path is a symbolic link' if path.resolve() != path or path.is_symlink() else
                  'secret-looking proof is held: ' + path.name if any(
                      fnmatch.fnmatch(part, pattern) for part in path.relative_to(root).parts for pattern in SECRETS)
                  else None)
        if unsafe and strict:
            raise ValueError(unsafe)
        if unsafe:
            result = [entry for entry in result if entry['source'] != str(path)] + [{'source': str(path), 'missing': True}]
            continue
        owner = hashlib.sha256(str(root).encode()).hexdigest()
        destination = folder / 'proofs' / owner / path.relative_to(root)
        result = [entry for entry in result if entry['source'] != str(path)]
        try:
            copied = await async_cleanup_scan(store, copy_proof_file, copies.get(path, path), destination, folder)
            if isinstance(copied, FileNotFoundError):
                raise copied
            entry = dict(copied, source=str(path))
        except FileNotFoundError as error:
            if error.filename != str(path) or path.exists() or path.is_symlink():
                raise scan_process.ScanError(str(error)) from error
            entry = {'source': str(path), 'missing': True}
        result.append(entry)
    result = sorted(result, key=lambda entry: entry['source'])
    return (result, stamp) if with_stamp else result


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
    if branch and branch != expected and not branch.startswith('refs/heads/'):
        raise ValueError('worktree is not on the saved job branch')
    if 'prunable' in records[0]:
        raise ValueError('worktree registration is prunable')
    git_dir = await ws._registered_git_directory(repository, target, timeout=ws.GIT_TIMEOUT)
    if git_dir.is_symlink() or any((git_dir / name).exists() for name in ('locked', 'index.lock', 'HEAD.lock')):
        raise ValueError('worktree is locked')
    operation = ws._workspace_operation(git_dir)
    if operation:
        raise ValueError('Git operation is in progress: ' + operation)
    paths = await worktree_salvage.repositories(target)
    if len(paths) > 1 or (git_dir / 'modules').is_dir():
        raise ValueError('worktree has initialized submodules')
    _, untracked = await git(target, 'ls-files', '--others', '--exclude-standard', '-z', strip=False)
    for name in untracked.split('\0'):
        if any(fnmatch.fnmatch(part, pattern) for part in Path(name).parts
               for pattern in ('.env*', '*.pem', '*.key', '*.p12')):
            raise ValueError('secret-looking untracked file is held: ' + name)
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


def check_raw_tree(target, entries, algorithm):
    for entry in entries.split('\0'):
        if not entry:
            continue
        header, name = entry.split('\t', 1)
        mode, expected, _ = header.split()
        if mode == '120000':
            continue
        path = target / name
        if mode == '160000':
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


async def tree(store, target, folder, git_env=None, base='HEAD', exclude=None):
    """Write the copy's tree. A list in `exclude` is a done job's lossy save: it skips those paths and the strict checks."""
    index = folder / 'snapshot-index'
    for temporary in (index, index.with_name(index.name + '.lock')):
        if temporary.exists() or temporary.is_symlink():
            temporary.unlink()
    if exclude is not None:
        return await asyncio.to_thread(cleanup.write_tree, target, folder, exclude, git_env, base)
    source_env = dict(os.environ, **(git_env or {}))
    env = dict(source_env, GIT_INDEX_FILE=str(index), GIT_OPTIONAL_LOCKS='0')
    await git(target, 'read-tree', base, env=env)
    await git(target, 'add', '-A', env=env, reject_stderr=True)
    _, result = await git(target, 'write-tree', env=env)
    _, entries = await git(target, 'ls-files', '--stage', '-z', strip=False, env=env)
    _, algorithm = await git(target, 'rev-parse', '--show-object-format', env=env)
    await async_cleanup_scan(store, check_raw_tree, target, entries, algorithm)
    _, current_entries = await git(target, 'ls-files', '--stage', '-z', strip=False, env=source_env)
    current, saved = gitlinks(current_entries), gitlinks(entries)
    for name in sorted(current.keys() | saved.keys()):
        if current.get(name) != saved.get(name):
            raise ValueError('submodule pointer changed: ' + name)
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


def read_reflog_commits(git_dir):
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


async def reflog_commits(repository, target, git_env=None, store=None):
    env = dict(os.environ, **(git_env or {}))
    _, git_dir = await git(target, 'rev-parse', '--absolute-git-dir', env=env)
    if store is None:
        roots = await scan_process.run_async(read_reflog_commits, Path(git_dir))
    else:
        roots = await async_cleanup_scan(store, read_reflog_commits, Path(git_dir))
    _, indexed = await git(target, 'ls-files', '--stage', '-z', strip=False, env=env)
    roots.update(entry.split()[1] for entry in indexed.split('\0') if entry.startswith('160000 '))
    if not roots:
        return []
    _, commits = await git(repository, 'rev-list', '--single-worktree', '--ignore-missing', '--children',
                           *sorted(roots), '--not', '--all')
    return [line for line in commits.splitlines() if ' ' not in line]


async def snapshot(store, repository, target, folder, task, branch_ref=None, loose=False):
    excluded = await asyncio.to_thread(cleanup.left_out, target, loose)
    _, head = await git(target, 'rev-parse', 'HEAD')
    job_ref = 'refs/heads/torii/task-' + str(task['id'])
    if loose:
        branch_ref = job_ref
        code, branch = await git(repository, 'rev-parse', '--verify', job_ref, check=False)
        if code:
            await git(repository, 'update-ref', job_ref, head, '0' * 40)
            branch = head
    else:
        branch_ref = branch_ref or job_ref
        code, branch = await git(repository, 'rev-parse', '--verify', branch_ref, check=False)
        if code:
            detached, _ = await git(target, 'symbolic-ref', '--quiet', 'HEAD', check=False)
            if detached:
                raise ValueError('worktree HEAD is detached and the saved job branch is missing')
            raise ValueError('saved branch is missing: ' + branch_ref)
    head_ref = 'refs/torii/saved/task-%s/%s/head' % (task['id'], task['iteration'])
    await git(repository, 'update-ref', head_ref, head)
    tree_id = await tree(store, target, folder, exclude=sorted(excluded) if loose else None)
    _, head_tree = await git(target, 'rev-parse', 'HEAD^{tree}')
    wip = None
    _, changes = await git(target, 'status', '--porcelain', '--untracked-files=no' if loose else '--untracked-files=all',
                           env=dict(os.environ, GIT_OPTIONAL_LOCKS='0'), reject_stderr=not loose)
    if changes or tree_id != head_tree:
        wip = await asyncio.to_thread(cleanup.commit, repository, task, tree_id, head,
                                      'torii wip job %s.%s' % (task['number'], task['iteration']))
        ref = 'refs/torii/wip/task-' + str(task['id'])
        code, old = await git(repository, 'rev-parse', '--verify', ref, check=False)
        await git(repository, 'update-ref', ref, wip, old if not code else '0' * 40)
    refs = [job_ref]
    if loose:
        refs = [branch_ref]
    else:
        refs = [branch_ref]
        if branch_ref != job_ref:
            code, _ = await git(repository, 'rev-parse', '--verify', job_ref, check=False)
            if not code:
                refs.append(job_ref)
    if head != branch:
        refs.append(head_ref)
    if wip:
        refs.append('refs/torii/wip/task-' + str(task['id']))
    prefix = 'refs/torii/saved/task-%s/%s/reflog/' % (task['id'], task['iteration'])
    try:
        _, pinned = await git(repository, 'for-each-ref', '--format=%(refname) %(objectname)', prefix)
        reflog_refs = dict(line.split() for line in pinned.splitlines())
        for oid in await reflog_commits(repository, target, store=store):
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
        checksum, _ = await async_cleanup_scan(store, digest_with_stamp, folder / 'branch.bundle')
        bundle = dict(checksum, path='branch.bundle')
    result = dict(head=head, head_ref=head_ref, branch_head=branch, tree=tree_id, wip=wip, bundle=bundle,
                  reflog_refs=reflog_refs)
    if loose:
        result.update(branch=branch_ref, excluded=sorted(excluded))
    return result


def scan_ignored(target, folder, names, discard_names, loose, limit, rebuild=None):
    """Copy the ignored files of a work copy into the save. A file whose bytes match its published hash in rebuild
    is discarded with the command that fetches it again and does not count toward the limit."""
    result = {'copied': [], 'discarded': [], 'links': [], 'held': []}
    rebuild = rebuild or {}
    regenerable = REGENERABLE | set(discard_names)
    total = 0

    def visit(path, classify=False):
        nonlocal total
        relative = path.relative_to(target)
        if path.is_symlink():
            result['links'].append({'name': str(relative), 'target': os.readlink(path),
                                    'discarded': classify and path.name in regenerable})
        elif loose and (any(fnmatch.fnmatch(path.name, pattern) for pattern in SECRETS) or total > limit):
            result['discarded'].append({'name': str(relative), 'bytes': sum(p.lstat().st_size for p in files(path))})
        elif any(fnmatch.fnmatch(path.name, pattern) for pattern in SECRETS):
            result['held'].append(str(relative))
        elif classify and (path.name in regenerable or path.name.endswith('.pyc')):
            result['discarded'].append({'name': str(relative),
                                        'bytes': sum(p.lstat().st_size for p in files(path))})
        elif path.is_dir():
            for child in sorted(path.iterdir()):
                visit(child)
        else:
            info = path.lstat()
            published = rebuild.get(str(relative))
            if published and stat.S_ISREG(info.st_mode):
                hasher = hashlib.sha256()
                with open(path, 'rb') as handle:
                    for block in iter(lambda: handle.read(1024 * 1024), b''):
                        hasher.update(block)
                if hasher.hexdigest() == published[0]:
                    result['discarded'].append({'name': str(relative), 'bytes': info.st_size, 'rebuild': published[1]})
                    return
            total += info.st_size
            if total > limit and loose:
                result['discarded'].append({'name': str(relative), 'bytes': info.st_size})
                return
            if total > limit:
                raise ValueError('ignored files exceed 200 MB: %s (%d bytes total)' % (relative, total))
            result['copied'].append(dict(copy_file(path, folder / 'ignored' / relative, folder), name=str(relative)))
    for name in names.split('\0'):
        if name:
            visit(target / name.rstrip('/'), True)
    return result


async def rebuildable(target, env, rules):
    """Map each ignored path that a project command fetches again to its published sha256 and that command. Each rule in
    the close_saves_rebuildable setting names a project root, path patterns, a JSON manifest of {path: {sha256}} whose
    keys sit under prefix, the ref that holds the published manifest, and the fetch command."""
    if not rules:
        return {}
    _, common = await git(target, 'rev-parse', '--path-format=absolute', '--git-common-dir', env=env)
    project = Path(common).resolve().parent
    hashes = {}
    for rule in rules:
        if Path(rule['project']).resolve() != project:
            continue
        code, text = await git(target, 'show', '%s:%s' % (rule['ref'], rule['manifest']), check=False, env=env)
        if code:
            continue
        for key, value in json.loads(text).items():
            path = str(Path(rule.get('prefix', '')) / key.lstrip('/'))
            if (isinstance(value, dict) and isinstance(value.get('sha256'), str)
                    and any(fnmatch.fnmatch(path, pattern) for pattern in rule['paths'])):
                hashes[path] = (value['sha256'], rule['command'])
    return hashes


async def ignored(target, folder, git_env=None, *, discard_names=(), loose=False, store=None):
    env = dict(os.environ, **(git_env or {}))
    _, names = await git(target, 'ls-files', '--others', '--ignored', '--exclude-standard',
                         '--directory', '--no-empty-directory', '-z', strip=False, env=env)
    rebuild = await rebuildable(target, env, store.get('close_saves_rebuildable', []) if store is not None else [])
    args = (target, folder, names, discard_names, loose, IGNORED_LIMIT, rebuild)
    options = {'timeout': REBUILD_SCAN_TIMEOUT} if rebuild else {}
    if store is None:
        return await scan_process.run_async(scan_ignored, *args, **options)
    return await async_cleanup_scan(store, scan_ignored, *args, **options)


async def verify(repository, folder, saved, staged=False, store=None):
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
    entries += saved.get('ignored', {}).get('copied', []) + ([saved['bundle']] if saved.get('bundle') else [])
    checked = []
    for entry in entries:
        path = folder / entry['path']
        if not path.is_relative_to(folder) or path.resolve() != path:
            raise ValueError('saved artifact path changed')
        checked.append((path, entry))
    for batch in digest_batches(checked, lambda item: item[1]['bytes']):
        paths = [path for path, _ in batch]
        if store is None:
            results = await scan_process.run_async(digests_with_stamps, paths)
        else:
            results = await async_cleanup_scan(store, digests_with_stamps, paths)
        for (_, entry), (checksum, _) in zip(batch, results):
            if checksum != {key: entry[key] for key in ('bytes', 'sha256')}:
                raise ValueError('saved artifact checksum changed')


async def recover_commits(repository, gone, clones):
    """Fetch the saved commits the repository lacks from its remotes and from the other clones of the project.
    Return the commits no source holds, the sources asked, and the sources that gave no answer."""
    env = dict(os.environ, GIT_TERMINAL_PROMPT='0', GIT_SSH_COMMAND='ssh -o BatchMode=yes')
    _, remotes = await git(repository, 'remote')
    sources = remotes.split() + sorted({str(clone) for clone in clones if clone and Path(clone) != Path(repository)})
    gone, silent = list(gone), []
    for source in sources:
        for oid in list(gone):
            try:
                code, _ = await git(repository, '-c', 'gc.auto=0', '-c', 'maintenance.auto=false', 'fetch', '--no-tags',
                                    '--no-recurse-submodules', '--no-write-fetch-head', '--refmap=', '--', source, oid,
                                    check=False, env=env)
                if code:
                    await git(repository, 'ls-remote', '--', source, 'HEAD', env=env)
            except (OSError, RuntimeError, ValueError):
                silent.append(source)
                break
            if not code:
                gone.remove(oid)
    return gone, sources, silent


async def rebuild_bundle(store, task, repository, folder, saved, clones=()):
    """Rebuild a recorded branch.bundle that is gone from the saved commits the repository still holds or can fetch.
    Return the new bundle as a temporary file; the caller moves it into place together with the changed record.
    Return None when the record needs no file: the bundle is present, or every saved commit is now on a remote.
    A saved ref that moved stops the rebuild, so the record never follows a changed ref."""
    if not saved.get('bundle') or os.path.lexists(folder / 'branch.bundle'):
        return None
    pins = {saved['head_ref']: saved['head'], **(saved.get('reflog_refs') or {})}
    if saved.get('wip'):
        pins[saved['wip_saved_ref']] = saved['wip']
    if saved['branch_head'] != saved['head']:
        pins[saved['head_ref'].removesuffix('head') + 'branch'] = saved['branch_head']
    missing = []
    for ref, oid in pins.items():
        code, current = await git(repository, 'rev-parse', '--verify', '--quiet', ref, check=False)
        if code:
            missing.append(ref)
        elif current != oid:
            raise ValueError('saved ref changed: ' + ref)
    wanted = sorted(set(pins.values()))
    _, found = await git(repository, 'rev-list', '--no-walk', '--ignore-missing', *wanted)
    found = set(found.split())
    gone = [oid for oid in wanted if oid not in found]
    if gone:
        gone, sources, silent = await recover_commits(repository, gone, (saved.get('repository'), *clones))
        if gone and silent:
            raise UnverifiedWork('Job %s: the saved bundle is gone and %s does not hold its commits %s; '
                                 'cannot verify that they are lost: no answer from %s'
                                 % (task['number'], repository, ', '.join(gone), ', '.join(silent)))
        if gone:
            raise LostWork('Job %s: the saved bundle is gone and %s no longer holds its commits %s%s; this work cannot be rebuilt'
                           % (task['number'], repository, ', '.join(gone),
                              ' (also asked: %s)' % ', '.join(sources) if sources else ''))
    for ref in missing:
        await git(repository, 'update-ref', ref, pins[ref], '0' * 40)
    _, unreached = await git(repository, 'rev-list', *pins, '--not', '--remotes')
    if not unreached:
        saved['bundle'] = None
        return None
    temporary = directory(folder, store.directory) / 'branch.bundle.tmp'
    await git(repository, 'bundle', 'create', temporary, *pins, '--not', '--remotes')
    await git(repository, 'bundle', 'verify', temporary)
    checksum, _ = await async_cleanup_scan(store, digest_with_stamp, temporary)
    saved['bundle'] = dict(checksum, path='branch.bundle')
    return temporary


async def stage(store, task, target, repository, git_dir, saved, folder):
    root = directory(store.directory / 'worktree-deletions', store.directory)
    stage_path = root / (target.name + '-' + uuid.uuid4().hex)
    info = git_dir.lstat()
    record = dict(task=task['id'], number=task['number'], target=str(target), repository=str(repository),
                  git_dir=str(git_dir), git_inode=[info.st_dev, info.st_ino], branch=saved['branch'],
                  ignored=[entry['name'] for entry in saved['ignored']['discarded']], detached=False, removed={'bytes': 0, 'files': 0},
                  space_k=task['iteration'], **ws.stage_fields(store.directory, target, stage_path))
    saved['freed']['deps'] = sum(entry['bytes'] for entry in saved['ignored']['discarded'])
    await async_checkpoint(store, task, 'verified', saved)
    await ws.async_prepare_deletion_stage(store, stage_path, record)
    _, owned = await prepare_preconditions(store, task)
    _, branch = await git_directory(store, repository, target, task)
    if branch != saved['branch']:
        raise ValueError('late writer changed the copy branch')
    current_ignored = await ignored(target, folder, discard_names=store.get('close_saves_discard_names', []), store=store)
    _, owned = await prepare_preconditions(store, task)
    current_proofs, proof_stamp = await proofs(store, task, [Path(path) for path in owned], folder, with_stamp=True)
    if (await tree(store, target, folder) != saved['tree'] or current_ignored != saved['ignored']
            or current_proofs != saved['proofs']):
        raise ValueError('late writer changed the copy or proofs')
    await verify(repository, folder, saved, store=store)
    _, head = await git(target, 'rev-parse', 'HEAD')
    if head != saved['head']:
        raise ValueError('late writer changed the copy HEAD')
    if await reflog_commits(repository, target, store=store):
        raise ValueError('late writer added unsaved worktree reflog commits')
    precondition_stamp, owned = await prepare_preconditions(store, task)
    if any((git_dir / name).exists() for name in ('locked', 'index.lock', 'HEAD.lock')) or ws._workspace_operation(git_dir):
        raise ValueError('Git operation or lock changed before staging')
    copy = ws.staged_copy(stage_path, record)
    if target.stat().st_dev != copy.parent.stat().st_dev or info.st_dev != stage_path.stat().st_dev:
        raise ValueError('Deletion staging folder is on another filesystem')
    await check_park_use(store, task)
    with store.transaction():
        if (not preconditions_current(store, precondition_stamp, owned, target)
                or not references_current(store, proof_stamp)):
            raise scan_process.ScanError('close-save state changed before staging')
        target.rename(copy)
    record = await ws.async_detach_staged(store, stage_path, record)
    await async_checkpoint(store, task, 'staged', saved)


def hold(store, task, error):
    from . import problems
    reason = ' '.join(str(error).splitlines()).strip() or type(error).__name__
    with store.transaction():
        if not closed_round(store, task):
            return
        store.db.execute("UPDATE tasks SET copy='held',copy_reason=? WHERE id=?", (reason, task['id']))
        current = store.task_get(task['id'])
        holds = store.get('close_save_holds', {})
        entry = holds.get(str(task['id']))
        lost = isinstance(error, LostWork)
        if not entry or entry.get('k') != current['iteration'] or lost and not entry.get('lost'):
            if not entry or entry.get('k') != current['iteration']:
                since = time.time()
                entry = {'k': current['iteration'], 'since': since, 'retried': since}
            if lost:
                entry['lost'] = True
                problems.record(store, 'cleanup', 'saved-work-lost', reason,
                                task=task['id'], topic=task['topic'])
            holds[str(task['id'])] = entry
            store.put('close_save_holds', holds)
        code = ('saved-work-unverified' if isinstance(error, UnverifiedWork) else
                'park-in-use' if isinstance(error, ParkInUse) else 'copy-held')
        if (isinstance(error, ParkInUse) and error.scan_failed):
            return
        if not lost and reason != task.get('copy_reason') and not store.db.execute(
                "SELECT 1 FROM problems WHERE area='cleanup' AND code=? AND task=? AND detail=? LIMIT 1",
                (code, task['id'], problems.sanitize(reason))).fetchone():
            problems.record(store, 'cleanup', code, reason, task=task['id'], topic=task['topic'])


async def save_job(store, task_id):
    task = store.task_get(task_id)
    if not task:
        return
    target = ws._workspace_target(store.directory, task_id, task['worktree'] or None)
    try:
        ws.check_volume(store.directory, target)
        await check_park_use(store, task)
        _, identities = await prepare_preconditions(store, task)
        owned = [Path(path) for path in identities]
        await stopped_workers(store, task, reap=task['status'] in TERMINAL)
        antigravity = extension.optional('antigravity_runtime')
        if antigravity and not await antigravity.recover_job(store, task_id):
            hold(store, task, ValueError('Antigravity private branch recovery is incomplete'))
            return
        topic = store.topic(task['topic'])
        identity = await ws.project_identity(Path(topic['cwd']), timeout=ws.GIT_TIMEOUT)
        repository = Path(identity['project_key'])
        async with ws._repository_lock(store.directory, repository):
            await ws.async_recover_staged_registrations(store, repository)
            phase, saved = await async_load(store, task)
            if phase == 'retired' and path_stamp(target) is None:
                with store.transaction():
                    if loose_ready(store, task):
                        store.db.execute("UPDATE tasks SET copy='cleared' WHERE id=? AND copy='saving'", (task_id,))
                return
            if phase == 'cleared':
                saved.pop('extras', None)
            folder = directory(store.directory / 'results' / ('task-' + str(task_id)) / 'rounds' / str(task['iteration']), store.directory)
            if target.exists() and not task['worktree']:
                await unrecorded_leftover(store, task, target)
            if target.exists():
                if task['worktree']:
                    ws._saved_workspace_relative(target, task['worktree'])
                git_dir, branch = await git_directory(store, repository, target, task)
                for attempt in range(2):
                    carried = {key: saved[key] for key in ('scratch_git',) if key in saved}
                    saved = {'proofs': await proofs(store, task, owned, folder),
                             'resnapshots': saved.get('resnapshots', 0), **carried}
                    await async_checkpoint(store, task, 'proofs', saved)
                    saved.update(await snapshot(store, repository, target, folder, task, branch_ref=branch))
                    saved.update(branch=branch, wip_ref='refs/torii/wip/task-' + str(task_id),
                                 wip_saved_ref='refs/torii/saved/task-%s/%s/wip' % (task_id, task['iteration']),
                                 time=time.time(), notes=task.get('notes') or '')
                    await async_checkpoint(store, task, 'snapshot', saved)
                    saved['ignored'] = await ignored(target, folder, discard_names=store.get('close_saves_discard_names', []), store=store)
                    await async_checkpoint(store, task, 'ignored', saved)
                    if saved['ignored']['held']:
                        await async_atomic_json(store, folder / 'held.json', saved)
                        raise ValueError('secret-looking ignored files are held: ' + ', '.join(saved['ignored']['held']))
                    await async_atomic_json(store, folder / 'saved.json', saved)
                    await async_checkpoint(store, task, 'manifest', saved)
                    with store.db:
                        store.db.execute('UPDATE task_iterations SET head=?,saved=? WHERE task=? AND k=?',
                                         (saved['head'], str(folder / 'saved.json'), task_id, task['iteration']))
                    await verify(repository, folder, saved, store=store)
                    await async_checkpoint(store, task, 'verified', saved)
                    try:
                        await stage(store, task, target, repository, git_dir, saved, folder)
                        break
                    except ValueError as error:
                        if 'late writer' not in str(error) or attempt or saved['resnapshots']:
                            raise
                        saved['resnapshots'] += 1
                        await async_checkpoint(store, task, 'snapshot', saved)
                with store.db:
                    store.db.execute('UPDATE task_iterations SET head=?,saved=? WHERE task=? AND k=?',
                                     (saved['head'], str(folder / 'saved.json'), task_id, task['iteration']))
            elif phase not in ('staged', 'extras'):
                if any(record.get('space_k') == task['iteration'] and record['task'] == task_id
                       for _, record in await ws.async_deletion_stages(store)):
                    await async_checkpoint(store, task, 'staged', saved)
                else:
                    saved['proofs'] = await proofs(store, task, owned, folder)
                    saved.setdefault('ignored', {'copied': []})
                    await async_atomic_json(store, folder / 'saved.json', saved)
                    await async_checkpoint(store, task, 'staged', saved)
    except scan_process.ScanError as error:
        hold(store, task, error)
    except (OSError, RuntimeError, ValueError, asyncio.TimeoutError) as error:
        if str(error) not in RUNNING and not isinstance(error, UnrecordedCopy) and loose_ready(store, task):
            try:
                if await loose_save(store, task, error):
                    return
            except (OSError, RuntimeError, ValueError, asyncio.TimeoutError) as failure:
                error = failure
        hold(store, task, error)


async def unrecorded_leftover(store, task, target):
    """Check a job folder that exists while the job records no worktree.
    A bare folder with a live Git link goes on to the normal save. A bare folder with no link or a broken one
    goes to the Trash. Any other content holds the job, because Torii cannot tell whose files they are."""
    name = 'worktrees/' + target.name
    if target.is_symlink() or not target.is_dir():
        raise UnrecordedCopy(name + ' is not a folder and the job has no recorded worktree; it was kept for review')
    entries = await asyncio.to_thread(lambda: sorted(entry.name for entry in os.scandir(target)))
    pointer = target / '.git'
    if entries not in ([], ['.git']) or entries and (pointer.is_symlink() or not pointer.is_file()):
        raise UnrecordedCopy(name + ' holds files but the job has no recorded worktree; it was kept for review.'
                             ' Run worktree.create to adopt it, or move it away')
    if entries:
        text = pointer.read_text(errors='replace').strip()
        if text.startswith('gitdir:') and ws._resolved_git_dir(target, text[len('gitdir:'):].strip()).is_dir():
            return
    moved = ws.move_to_trash(target, ws.trash_for(store.directory, target), name=target.name + '-unrecorded')
    store.task_note(task['id'], 'The job folder %s had no recorded worktree and only a %s; it was moved to %s'
                    % (name, 'broken Git link' if entries else 'empty folder', moved))


def loose_ready(store, task):
    """A loose save may clear only a done or dropped job in the same round that is not on the keep list."""
    current = store.task_get(task['id'])
    return bool(current and current['status'] in TERMINAL and current['iteration'] == task['iteration']
                and not kept(store, task['id']))


async def loose_git_dir(target):
    """Return the job copy's own Git folder and repository, or (None, None) when Git cannot read the copy.
    The repository comes from the copy, so a save still works after the chat's project folder moved."""
    if not target.is_dir() or target.is_symlink() or not (target / '.git').exists():
        return None, None
    values = []
    for option in ('--git-common-dir', '--absolute-git-dir', '--show-toplevel'):
        code, value = await git(target, 'rev-parse', option, check=False)
        if code:
            return None, None
        values.append(value)
    if Path(values[2]).resolve() != target:
        return None, None
    repository = ws._resolved_git_dir(target, values[0])
    if repository == target or repository.is_relative_to(target):
        raise ValueError('job copy holds its own Git repository')
    return Path(values[1]), repository


def registration(git_dir, target, repository):
    if (git_dir is None or git_dir.parent != repository / 'worktrees' or git_dir.is_symlink()
            or not (git_dir / 'gitdir').is_file() or (git_dir / 'gitdir').read_text().strip() != str(target / '.git')):
        return None
    return git_dir


async def loose_save(store, task, error):
    """Save what Git can hold for a done or dropped job after a strict save failed, then stage its copy.
    Return False when the job is no longer done or dropped; the caller then keeps the strict hold."""
    if not loose_ready(store, task):
        return False
    task = store.task_get(task['id'])
    _, identities = await prepare_preconditions(store, task)
    owned = [Path(path) for path in identities]
    await stopped_workers(store, task, reap=True)
    antigravity = extension.optional('antigravity_runtime')
    if antigravity and not await antigravity.recover_job(store, task['id']):
        raise ValueError('Antigravity private branch recovery is incomplete')
    target = ws._workspace_target(store.directory, task['id'], task['worktree'] or None)
    ws.check_volume(store.directory, target)
    git_dir, repository = await loose_git_dir(target)
    if repository is None and os.path.lexists(target / '.git'):
        raise ValueError('job copy has a .git entry Git cannot read; copy kept for manual recovery')
    if repository is None:
        topic = store.topic(task['topic'])
        identity = await ws.project_identity(Path(topic['cwd']), timeout=ws.GIT_TIMEOUT)
        repository = Path(identity['project_key'])
    reason = ' '.join(str(error).splitlines()).strip() or type(error).__name__
    async with ws._repository_lock(store.directory, repository):
        await ws.async_recover_staged_registrations(store, repository)
        folder = directory(store.directory / 'results' / ('task-' + str(task['id'])) / 'rounds' / str(task['iteration']), store.directory)
        if any(record['task'] == task['id'] and record.get('space_k') == task['iteration'] and ws.staged_copy(stage, record).exists()
               for stage, record in await ws.async_deletion_stages(store)):
            _, saved = await async_load(store, task)
            await async_checkpoint(store, task, 'staged', saved)
            return True
        saved = {'proofs': await proofs(store, task, owned, folder, strict=False), 'loose': reason,
                 'ignored': {'copied': [], 'discarded': [], 'links': [], 'held': []}}
        if git_dir is not None:
            saved.update(await snapshot(store, repository, target, folder, task, loose=True))
            saved.update(wip_ref='refs/torii/wip/task-' + str(task['id']), repository=str(repository),
                         wip_saved_ref='refs/torii/saved/task-%s/%s/wip' % (task['id'], task['iteration']))
            saved['ignored'] = await ignored(target, folder, discard_names=store.get('close_saves_discard_names', []), loose=True, store=store)
        saved.update(time=time.time(), notes=task.get('notes') or '')
        await async_atomic_json(store, folder / 'saved.json', saved)
        if saved.get('head'):
            with store.db:
                store.db.execute('UPDATE task_iterations SET head=?,saved=? WHERE task=? AND k=?',
                                 (saved['head'], str(folder / 'saved.json'), task['id'], task['iteration']))
            await verify(repository, folder, saved, staged=True, store=store)
        await async_checkpoint(store, task, 'verified', saved)
        if target.exists() or target.is_symlink():
            root = directory(store.directory / 'worktree-deletions', store.directory)
            stage_path = root / (target.name + '-' + uuid.uuid4().hex)
            git_dir = await async_cleanup_scan(store, registration, git_dir, target, repository)
            info = git_dir.lstat() if git_dir is not None else None
            record = dict(task=task['id'], number=task['number'], target=str(target), repository=str(repository),
                          git_dir=str(git_dir or repository / 'worktrees' / (target.name + '-' + uuid.uuid4().hex)),
                          git_inode=[info.st_dev, info.st_ino] if info else [0, 0], branch=saved.get('branch', ''),
                          ignored=[entry['name'] for entry in saved['ignored']['discarded']], detached=False,
                          removed={'bytes': 0, 'files': 0}, space_k=task['iteration'], loose=True,
                          **ws.stage_fields(store.directory, target, stage_path))
            await ws.async_prepare_deletion_stage(store, stage_path, record)
            encoded = await async_encode_checkpoint(store, saved)
            stamp, identities = await prepare_preconditions(store, task)
            with store.transaction():
                if not loose_ready(store, task) or not preconditions_current(store, stamp, identities, target):
                    return False
                copy = ws.staged_copy(stage_path, record)
                if target.lstat().st_dev != copy.parent.stat().st_dev:
                    raise ValueError('Deletion staging folder is on another filesystem')
                checkpoint_encoded(store, task, 'staged', encoded)
                target.rename(copy)
            record = await ws.async_detach_staged(store, stage_path, record)
            return True
        encoded = await async_encode_checkpoint(store, saved)
        with store.transaction():
            if not loose_ready(store, task):
                return False
            checkpoint_encoded(store, task, 'staged', encoded)
        return True


def outside_worktrees(root):
    """List the live linked worktrees that a Git repository inside root registers outside root."""
    root, found = Path(root).resolve(), []
    for base, dirs, names in os.walk(root):
        if 'HEAD' in names and 'objects' in dirs and 'worktrees' in dirs:
            for registration in sorted((Path(base) / 'worktrees').iterdir()):
                pointer = registration / 'gitdir'
                if registration.is_symlink() or pointer.is_symlink() or not pointer.is_file():
                    continue
                tree = Path(pointer.read_text().strip())
                tree = (tree if tree.is_absolute() else registration / tree).parent.resolve()
                if tree.exists() and not tree.is_relative_to(root):
                    found.append(str(tree))
        dirs[:] = [name for name in dirs if name not in ('objects', 'node_modules')]
    return found


def git_pointer(path):
    """A .git file marks a Git tree only when it points at a Git directory. Tools such as uv leave an empty one."""
    try:
        with open(path, 'rb') as handle:
            return handle.read(7) == b'gitdir:'
    except OSError:
        return True


def scan_git_trees(root):
    found = []
    for base, dirs, names in os.walk(root):
        if '.git' in dirs or '.git' in names and git_pointer(Path(base) / '.git'):
            found.append(str(Path(base).relative_to(root)))
        dirs[:] = sorted(name for name in dirs if name not in ('.git', 'node_modules'))
    return found


def saved_bundles(rounds):
    """Map the key of each bundle that a complete scratch Git save of this job holds to its file."""
    found = {}
    for path in sorted(rounds.glob('*/scratch-git/*/scratch-git.json')):
        try:
            trees = json.loads(path.read_text())['trees']
        except (OSError, ValueError, KeyError, TypeError):
            continue
        for record in trees:
            bundle = path.parent / str(record.get('bundle'))
            if record.get('bundle_key') and bundle.is_file() and not bundle.is_symlink():
                found[record['bundle_key']] = bundle
    return found


async def link_bundle(tree, source, target, env):
    """Reuse the bundle that an earlier save wrote for the same commits. A hard link takes no space."""
    if source is None:
        return False
    try:
        os.link(source, target)
        await git(tree, 'bundle', 'verify', '--quiet', target, env=env)
    except (OSError, RuntimeError, ValueError):
        target.unlink(missing_ok=True)
        return False
    return True


def bundle_contents(path):
    """Read what names the objects of a bundle: its heads, the commits it leaves out, and its object count."""
    lines = []
    with open(path, 'rb') as handle:
        if not handle.readline().startswith(b'# v'):
            raise ValueError('Not a Git bundle')
        for line in handle:
            if line == b'\n':
                pack = handle.read(12)
                if len(pack) != 12 or pack[:4] != b'PACK':
                    break
                return sorted(line.split(b' ', 1)[0] if line.startswith(b'-') else line for line in lines), pack[8:]
            lines.append(line)
    raise ValueError('Git bundle has no pack')


def same_file(one, other):
    """Two bundles of the same heads, left-out commits and object count hold the same objects: Git packs them into
    different bytes on each run, and between two saves with the same heads only a dropped stash changes the objects,
    which also changes the count."""
    try:
        first, second = one.lstat(), other.lstat()
        if stat.S_ISLNK(first.st_mode) and stat.S_ISLNK(second.st_mode):
            return os.readlink(one) == os.readlink(other)
        if not stat.S_ISREG(first.st_mode) or not stat.S_ISREG(second.st_mode):
            return False
        if os.path.samestat(first, second) or first.st_size == second.st_size and filecmp.cmp(one, other, shallow=False):
            return True
        return one.suffix == '.bundle' and bundle_contents(one) == bundle_contents(other)
    except (OSError, ValueError):
        return False


def save_redundant(path, staying):
    """Each file of the save has the same contents under the same name in one save that stays."""
    for file in sorted(path.rglob('*')):
        relative = file.relative_to(path)
        if file.is_dir() and not file.is_symlink() or str(relative) == 'scratch-git.json':
            continue
        if not any(same_file(file, other / relative) for other in staying):
            return False
    return True


def redundant_scratch_saves(results, recorded, seconds=DISCARD_SCAN_SECONDS):
    """List, by job, the scratch Git saves that hold nothing which the other saves of the job do not hold. A save
    that a round records always stays. The newest save stays, and each older save stays unless every file in it is
    also in the saves that stay. A complete save, with its scratch-git.json, goes only for a complete save that stays.
    A missing record alone never makes a save redundant. The scan compares file
    contents, so it stops after the given seconds and the next pass goes on."""
    found = {}
    end = time.monotonic() + seconds
    for root in sorted(results.glob('task-*')):
        if not root.name[5:].isdigit():
            continue
        saves = sorted((path for path in root.glob('rounds/*/scratch-git/*') if path.is_dir() and not path.is_symlink()),
                       key=lambda path: path.name, reverse=True)
        staying = [path for path in saves if str(path) in recorded]
        for path in saves:
            if path in staying:
                continue
            if time.monotonic() > end:
                return found
            complete = (path / 'scratch-git.json').is_file()
            peers = [save for save in staying if not complete or (save / 'scratch-git.json').is_file()]
            if peers and save_redundant(path, peers):
                found.setdefault(int(root.name[5:]), []).append(str(path))
            else:
                staying.append(path)
    return found


async def discard_scratch_saves(store):
    """Move the redundant scratch Git saves of every job to the Torii Trash. A pass that stopped before its checkpoint
    leaves a save that the next pass writes again."""
    recorded = set()
    for row in store.db.execute("SELECT data FROM task_space_saves WHERE instr(data,'scratch_git')").fetchall():
        recorded.update(json.loads(row['data']).get('scratch_git', ()))
    results = store.directory / 'results'
    try:
        found = await async_cleanup_scan(store, redundant_scratch_saves, results, recorded)
    except scan_process.ScanError:
        return
    sweeps.run(store, 'scratch-git-discard', list(found), lambda task_id: discard_job_scratch_saves(
        store, results, task_id, found[task_id]), refs=lambda task_id: {'task': task_id})


def discard_job_scratch_saves(store, results, task_id, saves):
    root = results / ('task-' + str(task_id))
    trash = ws.trash_for(store.directory, root)
    folder = trash / ('%s%d-results-%s' % (TRIM_TRASH_PREFIX, task_id, uuid.uuid4().hex[:8]))
    trash.mkdir(mode=0o700, parents=True, exist_ok=True)
    if trash.stat().st_dev != root.stat().st_dev:
        raise OSError(errno.EXDEV, 'Trash is on another filesystem')
    for path in map(Path, saves):
        target = folder / path.relative_to(root)
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        path.rename(target)
    ws.logger.info('close save moved %d redundant scratch Git saves job=%s to %s', len(saves), task_id, folder)


async def save_git_tree(tree, root, out, index, bundles):
    """Pin commits that only the reflog holds under SCRATCH_REFLOG, bundle the commits of one scratch Git tree that no
    surviving ref holds, and write its changes and untracked files as a patch. The bundle key names the saved heads
    and commits: one bundle file serves every save with the same key."""
    env = dict(os.environ, GIT_OPTIONAL_LOCKS='0')
    _, common = await git(tree, 'rev-parse', '--git-common-dir', env=env)
    local = ws._resolved_git_dir(tree, common).is_relative_to(root)
    code, head = await git(tree, 'rev-parse', '--verify', '--quiet', 'HEAD', check=False, env=env)
    head = head if not code else None
    code, stashes = await git(tree, 'log', '-g', '--format=%H', 'refs/stash', check=False, env=env) if local else (1, '')
    stashes = stashes.split() if not code else []
    tips = ['--all', *stashes] if local else ['HEAD'] if head else []
    kept_refs = ['--remotes'] if local else ['--glob=refs/*']
    _, git_dir = await git(tree, 'rev-parse', '--absolute-git-dir', env=env)
    reflog = await scan_process.run_async(read_reflog_commits, Path(git_dir))
    pins = []
    if reflog:
        _, unsaved = await git(tree, 'rev-list', '--ignore-missing', *sorted(reflog), '--not',
                               '--all', *([head] if head else []), *stashes, env=env)
        pins = sorted(set(unsaved.split()) & reflog)
        for oid in pins:
            await git(tree, 'update-ref', SCRATCH_REFLOG + oid, oid, env=env)
    record = {'path': str(tree.relative_to(root)), 'head': head, 'stashes': stashes, 'bundle': None, 'patch': None,
              'index_patch': None, 'reflog_refs': [SCRATCH_REFLOG + oid for oid in pins]}
    _, missing = await git(tree, 'rev-list', *tips, '--not', *kept_refs, env=env) if tips else (0, '')
    if missing:
        record['bundle'] = '%d.bundle' % index
        _, listing = await git(tree, 'for-each-ref', '--format=%(objectname) %(refname)', env=env) if local else (0, '')
        key = hashlib.sha256('\n'.join((head or '', *stashes, listing, missing)).encode()).hexdigest()
        record['bundle_key'] = key
        if not await link_bundle(tree, bundles.get(key), out / record['bundle'], env):
            await git(tree, 'bundle', 'create', '--quiet', out / record['bundle'], *tips, '--not', *kept_refs, env=env)
            await git(tree, 'bundle', 'verify', '--quiet', out / record['bundle'], env=env)
        bundles[key] = out / record['bundle']
    _, names = await git(tree, 'ls-files', '--others', '--exclude-standard', '-z', strip=False, env=env)
    size = sum((tree / name).lstat().st_size for name in names.split('\0') if name)
    if size > UNTRACKED_LIMIT:
        raise ValueError('untracked files exceed 200 MB in %s' % record['path'])
    _, conflicts = await git(tree, 'ls-files', '--unmerged', '-z', strip=False, env=env)
    if conflicts:
        raise ValueError('Git index has unresolved conflicts in %s' % record['path'])
    index_patch = out / ('%d-index.patch' % index)
    await git(tree, 'diff', '--cached', '--binary', '--output=' + str(index_patch), env=env)
    if index_patch.stat().st_size:
        record['index_patch'] = index_patch.name
    else:
        index_patch.unlink()
    staged = dict(env, GIT_INDEX_FILE=str(out / ('%d.index' % index)))
    await git(tree, 'read-tree', *([head] if head else ['--empty']), env=staged)
    await git(tree, 'add', '-A', env=staged)
    patch = out / ('%d.patch' % index)
    await git(tree, 'diff', '--cached', '--binary', '--output=' + str(patch), env=staged)
    (out / ('%d.index' % index)).unlink()
    if patch.stat().st_size:
        record['patch'] = patch.name
    else:
        patch.unlink()
    return record


async def save_scratch_git(store, path, folder, preserve_ignored=False):
    """Before a parked job's scratch is cleared, save each Git tree in it. Raise when any tree cannot be saved; the
    part of the save that this pass wrote is then removed, because the scratch it copies stays in place."""
    root = path.resolve()
    trees = await async_cleanup_scan(store, scan_git_trees, root)
    if not trees:
        return None
    bundles = await async_cleanup_scan(store, saved_bundles, folder.parent)
    out = directory(folder / 'scratch-git' / (time.strftime('%Y%m%d-%H%M%S') + '-' + uuid.uuid4().hex[:6]), store.directory)
    records = []
    try:
        for index, relative in enumerate(trees):
            try:
                record = await save_git_tree(root / relative, root, out, index, bundles)
                if preserve_ignored:
                    ignored_folder = directory(out / ('%d-ignored' % index), store.directory)
                    record['ignored'] = await ignored(root / relative, ignored_folder,
                                                      discard_names=store.get('close_saves_discard_names', []), store=store)
                    if record['ignored']['held']:
                        raise ValueError('secret-looking ignored files are held: ' + ', '.join(record['ignored']['held']))
                records.append(record)
            except (OSError, RuntimeError, ValueError, asyncio.TimeoutError) as error:
                raise ValueError('%s: %s' % (relative, ' '.join(str(error).splitlines()))) from error
        await async_atomic_json(store, out / 'scratch-git.json', {'scratch': str(path), 'trees': records})
    except Exception:
        await ws.delete_tree(out)
        raise
    return str(out)


def keep_sibling(store, task, path, outside):
    """Name folders inside the state directory by their relative path, so long paths fit the problem detail."""
    def short(value):
        for root in (store.directory, store.directory.resolve()):
            try:
                return str(Path(value).relative_to(root))
            except ValueError:
                pass
        return str(value)
    reason = 'Sibling kept: %s has Git worktrees outside it: %s' % (short(path), ', '.join(map(short, outside)))
    if not store.db.execute("SELECT 1 FROM problems WHERE area='cleanup' AND code='sibling-git-kept' AND task=? AND detail=? LIMIT 1",
                            (task['id'], reason)).fetchone():
        problems.record(store, 'cleanup', 'sibling-git-kept', reason, task=task['id'], topic=task['topic'])


def clearable(store, task, saved):
    """A strict save clears any closed round. A loose save clears only a done or dropped job off the keep list."""
    return closed_round(store, task) and ('loose' not in saved or loose_ready(store, task))


def clear_deferred(task, reason):
    ws.logger.info('close save clear deferred job=%s reason=%s', task['id'], reason)


async def clear_extras(store, task, references):
    from .worker_storage import _prune_scratch_registrations, _scratch_repositories
    saved_stamp = checkpoint_stamp(store, task)
    _, reference_snapshot = await async_scan_task_references(store, task['id'], references)
    phase, saved = await async_load(store, task)
    if phase not in ('staged', 'extras') or not clearable(store, task, saved):
        return
    if any(record['task'] == task['id'] and record.get('space_k') == task['iteration']
           for _, record in await ws.async_deletion_stages(store)):
        return
    await check_park_use(store, task)
    await prepare_preconditions(store, task)
    counts = {'bytes': 0, 'files': 0}
    await stopped_workers(store, task, reap=task['status'] in TERMINAL)
    if not clearable(store, task, saved):
        return clear_deferred(task, 'job reopened or its round changed')
    folder = directory(store.directory / 'results' / ('task-' + str(task['id'])) / 'rounds' / str(task['iteration']), store.directory)
    topic = store.topic(task['topic'])
    project = Path(topic['cwd'])
    try:
        identity = await ws.project_identity(project, timeout=ws.GIT_TIMEOUT)
        project = Path(identity['project_root'])
    except FileNotFoundError:
        if project.exists() or (saved.get('head') and not saved.get('repository')):
            raise
        identity = {}
    repository = Path(saved.get('repository') or identity['project_key']) if saved.get('head') else None
    bundle = (await rebuild_bundle(store, task, repository, folder, saved, clones=(identity.get('project_key'),))
              if repository else None)
    _, identities = await prepare_preconditions(store, task)
    owned = [Path(path) for path in identities]
    saved['proofs'], proof_stamp = await proofs(store, task, owned[1:], folder, strict='loose' not in saved,
                                                with_stamp=True)
    encoded = await async_encode_checkpoint(store, saved)
    temporary = await async_cleanup_scan(store, prepare_json, folder / 'saved.json', encoded)
    with store.transaction():
        if (not clearable(store, task, saved) or not references_current(store, saved_stamp)
                or not references_current(store, proof_stamp) or not references_current(store, reference_snapshot)):
            return clear_deferred(task, 'state changed before saved.json')
        if bundle:
            bundle.replace(folder / 'branch.bundle')
        commit_json(temporary, folder / 'saved.json')
        checkpoint_encoded(store, task, 'extras', encoded)
    saved_stamp = checkpoint_stamp(store, task)
    if repository:
        await verify(repository, folder, saved, staged=True, store=store)
    await trim_results(store, task, project, references)
    stages = saved.setdefault('extras', {})
    _, identities = await prepare_preconditions(store, task)
    for path in [Path(value) for value in identities][1:]:
        entry = stages.get(str(path))
        new_entry = entry is None and path.exists()
        sibling = path.parent in (store.directory / 'scratch', store.directory / 'worktrees') and path.name.startswith('task-%s-' % task['id'])
        if sibling and entry and entry['staged'] and path.exists():
            raise ValueError('Sibling folder reappeared during cleanup')
        if new_entry and sibling:
            outside = await async_cleanup_scan(store, outside_worktrees, path)
            if outside:
                keep_sibling(store, task, path, outside)
                continue
        if new_entry and (sibling or path == scratch(store, task['id']) and parked(store, task)):
            try:
                saves = await save_scratch_git(store, path, folder,
                                               preserve_ignored=sibling and path.parent == store.directory / 'worktrees')
            except (OSError, RuntimeError, ValueError, asyncio.TimeoutError, scan_process.ScanError) as error:
                raise ValueError(('Sibling' if sibling else 'Scratch') + ' Git tree could not be saved: ' + str(error)) from error
            if saves:
                saved.setdefault('scratch_git', []).append(saves)
        if new_entry:
            staging = directory(folder / 'extras', store.directory) / uuid.uuid4().hex
            repositories = await async_cleanup_scan(store, _scratch_repositories, [path]) if sibling else []
            entry = {'path': str(staging), 'staged': False, 'repositories': repositories}
            stages[str(path)] = entry
            encoded = await async_encode_checkpoint(store, saved)
        stamp, identities = await prepare_preconditions(store, task)
        with store.transaction():
            if (not clearable(store, task, saved) or not references_current(store, saved_stamp)
                    or not references_current(store, proof_stamp) or not references_current(store, reference_snapshot)
                    or not preconditions_current(store, stamp, identities, path)):
                return clear_deferred(task, 'state changed before scratch staging')
            if (path == scratch(store, task['id']) or sibling) and store.db.execute('''SELECT 1 FROM wait_jobs j
                    JOIN waits w ON w.id=j.wait WHERE j.task=? AND w.state='armed' LIMIT 1''',
                    (task['id'],)).fetchone():
                if new_entry:
                    stages.pop(str(path), None)
                continue
            if entry is None:
                continue
            if new_entry:
                if not path.exists():
                    stages.pop(str(path), None)
                    continue
                checkpoint_encoded(store, task, 'extras', encoded)
        saved_stamp = checkpoint_stamp(store, task)
        needs_stage = not entry['staged']
        if needs_stage:
            entry['staged'] = True
            encoded = await async_encode_checkpoint(store, saved)
        stamp, identities = await prepare_preconditions(store, task)
        staging = Path(entry['path'])
        if not staging.is_relative_to(folder / 'extras') or staging.parent.resolve() != staging.parent:
            raise ValueError('Invalid scratch deletion stage')
        staging_parent = path_stamp(staging.parent)
        await check_park_use(store, task, path, staging)
        with store.transaction():
            if (not clearable(store, task, saved) or not references_current(store, saved_stamp)
                    or not references_current(store, proof_stamp) or not references_current(store, reference_snapshot)
                    or not preconditions_current(store, stamp, identities, path)
                    or path_stamp(staging.parent) != staging_parent):
                return clear_deferred(task, 'state changed before scratch move')
            if needs_stage:
                if not staging.exists() and path.exists():
                    path.rename(staging)
                checkpoint_encoded(store, task, 'extras', encoded)
        saved_stamp = checkpoint_stamp(store, task)
        await ws.delete_tree(staging, progress=counts, paced=True)
        for repository in entry.get('repositories', []):
            async with ws._repository_lock(store.directory, repository):
                await async_cleanup_scan(store, _prune_scratch_registrations, repository, [path])
    saved['freed']['extras'] += counts['bytes']
    encoded = await async_encode_checkpoint(store, saved)
    with store.transaction():
        if (not clearable(store, task, saved) or not references_current(store, saved_stamp)
                or not references_current(store, proof_stamp) or not references_current(store, reference_snapshot)):
            return clear_deferred(task, 'state changed before clear')
        store.host_logs_changed(task=task['id'])
        store.db.execute('UPDATE task_iterations SET bytes_freed=COALESCE(bytes_freed,0)+? WHERE task=? AND k=?',
                         (counts['bytes'], task['id'], task['iteration']))
        store.db.execute("UPDATE tasks SET copy='cleared',copy_reason=NULL WHERE id=?", (task['id'],))
        checkpoint_encoded(store, task, 'cleared', encoded)
    for saves in saved.get('scratch_git', []):
        if saves not in (store.task_get(task['id'])['notes'] or ''):
            store.task_note(task['id'], 'Parked job: the Git trees in scratch were saved to ' + saves
                            + ' (bundles of unsaved commits, patches of changes and untracked files)')


async def drain(store, limit=DRAIN_BATCH):
    rows = [row['id'] for row in store.db.execute(f"""SELECT id FROM tasks t WHERE {SAVING} AND NOT EXISTS (
        SELECT 1 FROM task_space_saves s WHERE s.task=t.id AND s.k=t.iteration AND s.phase IN ('staged','extras'))
        ORDER BY id""")]
    ran = await sweeps.arun(store, 'close-save', rows, lambda task_id: save_job(store, task_id),
                            refs=lambda task_id: {'task': task_id}, limit=limit)
    completed = any(not store.db.execute(f"""SELECT 1 FROM tasks t WHERE id=? AND {SAVING} AND NOT EXISTS (
        SELECT 1 FROM task_space_saves s WHERE s.task=t.id AND s.k=t.iteration AND s.phase IN ('staged','extras'))""",
        (task_id,)).fetchone() for task_id in ran)
    return completed and pending(store)


async def finish(store):
    await discard_scratch_saves(store)
    try:
        references = await async_scan_references(store)
    except Exception as error:
        problems.record(store, 'cleanup', 'reference-scan-failed', type(error).__name__, level=logging.ERROR, every=3600)
        return
    await sweeps.arun(store, 'close-clear', [dict(row) for row in store.db.execute(
        "SELECT * FROM tasks WHERE copy='saving' ORDER BY id")], lambda task: clear_job(store, task, references),
        key=lambda task: task['id'], refs=lambda task: {'task': task['id']})


async def clear_job(store, task, references):
    try:
        await clear_extras(store, task, references)
    except (OSError, RuntimeError, ValueError, asyncio.TimeoutError) as error:
        hold(store, task, error)


async def rebuild_branch(store, task_id, target, repository):
    row = store.db.execute('SELECT phase,data FROM task_space_saves WHERE task=? ORDER BY k DESC LIMIT 1', (task_id,)).fetchone()
    if not row or row['phase'] not in ('cleared', 'staged', 'extras', 'restoring', 'restore_failed', 'restored', 'retired'):
        return None, False
    saved = json.loads(row['data'])
    if not saved.get('head') or saved['branch'] == 'refs/heads/torii/task-' + str(task_id):
        return None, False
    restored = await restore(store, task_id, target, repository) if row['phase'] != 'restored' else False
    return saved['branch'].removeprefix('refs/heads/'), restored


async def restore(store, task_id, target, repository):
    row = store.db.execute('SELECT k,phase,data FROM task_space_saves WHERE task=? ORDER BY k DESC LIMIT 1', (task_id,)).fetchone()
    if not row or row['phase'] not in ('cleared', 'staged', 'extras', 'restoring', 'restore_failed', 'retired'):
        return False
    saved = json.loads(row['data'])
    if not saved.get('head'):
        return False
    folder = store.directory / 'results' / ('task-' + str(task_id)) / 'rounds' / str(row['k'])
    try:
        recorded = saved.get('bundle')
        bundle = await rebuild_bundle(store, store.task_get(task_id), repository, folder, saved)
        if saved.get('bundle') != recorded:
            with store.db:
                if bundle:
                    bundle.replace(folder / 'branch.bundle')
                store.db.execute('UPDATE task_space_saves SET data=? WHERE task=? AND k=?',
                                 (json.dumps(saved), task_id, row['k']))
            if (folder / 'saved.json').is_file():
                await async_atomic_json(store, folder / 'saved.json', saved)
        await verify(repository, folder, saved, staged=True, store=store)
        directory(folder, store.directory)
        with store.db:
            store.db.execute("UPDATE task_space_saves SET phase='restoring' WHERE task=? AND k=?", (task_id, row['k']))
        if saved['branch'] != 'refs/heads/torii/task-' + str(task_id):
            if not target.exists() and not await ws._make_detached_worktree(store, repository, target, saved['branch_head']):
                target.parent.mkdir(parents=True, exist_ok=True)
                await git(repository, 'worktree', 'add', '--detach', target, saved['branch_head'])
            await git_directory(store, repository, target, {'id': task_id})
            await git(target, 'switch', '--no-guess', saved['branch'].removeprefix('refs/heads/'))
        _, branch_head = await git(target, 'rev-parse', 'HEAD')
        if branch_head != saved['branch_head']:
            raise ValueError('branch changed since save; restore the saved branch head before retrying')
        exclude = [] if saved.get('loose') else None
        current = await tree(store, target, folder, exclude=exclude)
        if current != saved['tree']:
            _, head_tree = await git(target, 'rev-parse', 'HEAD^{tree}')
            if current != head_tree:
                raise ValueError('Rebuild found edits; the saved WIP is kept')
            if saved.get('wip') or saved['head'] != saved['branch_head']:
                patch = folder / 'restore.patch'
                await git(repository, 'diff', '--binary', '--full-index', '--output=' + str(patch), saved['branch_head'], saved.get('wip') or saved['head'])
                await git(target, 'apply', '--index', patch)
        await git(target, 'reset', '-q')
        if await tree(store, target, folder, exclude=exclude) != saved['tree']:
            raise ValueError('Rebuilt tree does not match the saved tree')
        ignored_entries = saved.get('ignored', {})
        for entry in ignored_entries.get('copied', []):
            destination = target / entry['name']
            if destination.exists() or destination.is_symlink():
                expected = {key: entry[key] for key in ('bytes', 'sha256')}
                if await asyncio.to_thread(digest, destination) != expected:
                    raise ValueError('ignored file changed during rebuild: ' + entry['name'])
                continue
            await asyncio.to_thread(copy_file, folder / entry['path'], destination, target)
        for entry in ignored_entries.get('links', []):
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
            code, _ = await git(repository, 'rev-parse', '--verify', '--quiet', saved['wip_ref'], check=False)
            if not code:
                await git(repository, 'update-ref', '-d', saved['wip_ref'], saved['wip'])
        with store.db:
            store.db.execute("UPDATE task_space_saves SET phase='restored' WHERE task=? AND k=?", (task_id, row['k']))
            store.db.execute("UPDATE tasks SET copy='present',copy_reason=NULL WHERE id=?", (task_id,))
            store.db.execute('DELETE FROM task_parks WHERE task=?', (task_id,))
    except (OSError, RuntimeError, ValueError, asyncio.TimeoutError) as error:
        reason = 'Rebuild paused: ' + str(error) + '; saved work is kept; retry worktree.create after resolving it'
        if saved['branch'] != 'refs/heads/torii/task-' + str(task_id):
            reason += '; saved path: ' + str(folder)
        if isinstance(error, (LostWork, UnverifiedWork)):
            lost = isinstance(error, LostWork)
            reason = str(error) if lost else 'Rebuild paused: ' + str(error) + '; retry worktree.create after resolving it'
            code = 'saved-work-lost' if lost else 'saved-work-unverified'
            if not store.db.execute("SELECT 1 FROM problems WHERE area='cleanup' AND code=? AND task=? LIMIT 1",
                                    (code, task_id)).fetchone():
                problems.record(store, 'cleanup', code, reason, task=task_id)
        with store.db:
            store.db.execute("UPDATE task_space_saves SET phase='restore_failed' WHERE task=? AND k=?", (task_id, row['k']))
            store.db.execute("UPDATE tasks SET copy='held',copy_reason=? WHERE id=?", (reason, task_id))
        raise ValueError(reason) from error

    return True


async def clear_chat_scratch(store, topic):
    if store.db.execute("SELECT 1 FROM tasks WHERE topic=? AND status='open'", (topic,)).fetchone():
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
    if not worker['task']:
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
            if path.resolve() != path:
                raise ValueError('retained worker proof checksum changed')
            checksum, _ = await async_cleanup_scan(store, digest_with_stamp, path)
            if checksum != {key: entry[key] for key in ('bytes', 'sha256')}:
                raise ValueError('retained worker proof checksum changed')
        await async_atomic_json(store, folder / 'worker-proofs.json', entries)
        return True


def restore_staged_paths(stage_path, record):
    target, git_dir = Path(record['target']), Path(record['git_dir'])
    for source, destination, identity in (
            (stage_path / 'git', git_dir, record['git_inode']),
            (ws.staged_copy(stage_path, record), target, record['restoring_copy_inode'])):
        if source.exists() or source.is_symlink():
            if destination.exists() or destination.is_symlink():
                raise ValueError('late writer restoration destination is occupied')
            info = source.lstat()
            if [info.st_dev, info.st_ino] != identity:
                raise ValueError('late writer restoration source changed')
            source.rename(destination)
        else:
            info = destination.lstat()
            if [info.st_dev, info.st_ino] != identity:
                raise ValueError('late writer restoration destination changed')
    for directory in {stage_path, ws.staged_copy(stage_path, record).parent, target.parent, git_dir.parent}:
        fd = os.open(str(directory), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


async def restore_staged(store, stage_path, record):
    await async_cleanup_scan(store, restore_staged_paths, stage_path, record)
    task = store.task_get(record['task'])
    if not task or task['iteration'] != record['space_k']:
        raise ValueError('late writer restoration task changed')
    stamp = checkpoint_stamp(store, task)
    _, data = await async_load(store, task)
    if data.get('restoration_stage') != stage_path.name:
        retries = data.get('resnapshots', 0)
        data['resnapshots'] = retries + 1
        data['restoration_stage'] = stage_path.name
        encoded = await async_encode_checkpoint(store, data)
    else:
        retries = data['resnapshots'] - 1
        encoded = None
    with store.transaction():
        if not references_current(store, stamp):
            raise scan_process.ScanError('late writer restoration state changed')
        updated = store.db.execute('UPDATE tasks SET copy=?,copy_reason=? WHERE id=? AND iteration=?',
                                   ('held' if retries else 'saving', 'late writer changed the copy twice' if retries else None,
                                    task['id'], record['space_k']))
        if not updated.rowcount:
            raise ValueError('late writer restoration task changed')
        if encoded is not None:
            checkpoint_encoded(store, task, 'verified', encoded)
    finished = dict(record, detached=False, restoring=False)
    await ws._save_deletion_bounded(store, stage_path, finished)
    record.update(finished)


async def retire_missing_save(store, stage_path, record, results):
    task_id, iteration = record['task'], record['space_k']
    task = store.task_get(task_id)
    if not task or task['copy'] != 'saving' or task['iteration'] != iteration or not loose_ready(store, task):
        return False
    paths = (*ws._workspace_targets(store.directory, task_id), results)
    if task['worktree']:
        paths += (Path(task['worktree']),)
    if any(path_stamp(path) is not None for path in paths):
        return False
    ws.check_volume(store.directory, Path(record['target']))
    stamp = checkpoint_stamp(store, task)
    phase, saved = await async_load(store, task)
    ref = 'refs/torii/saved/task-%s/%s/head' % (task_id, iteration)
    if phase != 'staged' or saved.get('head_ref') != ref or not saved.get('head'):
        return False
    repository = Path(record['repository'])
    code, head = await git(repository, 'rev-parse', '--verify', '--quiet', ref + '^{commit}', check=False)
    if code or head != saved['head']:
        return False
    stage_git = {'GIT_DIR': str(stage_path / 'git'), 'GIT_COMMON_DIR': str(repository)}
    code, stage_branch = await git(repository, 'symbolic-ref', '--quiet', 'HEAD', check=False, env=stage_git)
    if code:
        code, stage_head = await git(repository, 'rev-parse', '--verify', '--quiet', 'HEAD', check=False, env=stage_git)
    else:
        code, stage_head = await git(repository, 'rev-parse', '--verify', '--quiet', stage_branch, check=False)
    reason = 'Worktree and results absent. Saved ref %s and staged files in %s (staged HEAD %s) are kept.' % (
        ref, stage_path.name, 'unavailable' if code else stage_head)
    with store.transaction():
        current = store.task_get(task_id)
        if (not loose_ready(store, task) or current['copy'] != 'saving' or current['worktree'] != task['worktree']
                or not references_current(store, stamp) or any(path_stamp(path) is not None for path in paths)
                or store.db.execute('SELECT 1 FROM workers WHERE task=? AND status NOT IN (?,?) LIMIT 1',
                                    (task_id, *FINISHED)).fetchone()):
            return False
        store.db.execute("UPDATE task_space_saves SET phase='retired' WHERE task=? AND k=?", (task_id, iteration))
        store.db.execute("UPDATE tasks SET copy='cleared',copy_reason=? WHERE id=?", (reason, task_id))
        store.put('closed_copy_retired:' + stage_path.name, True)
        store.put('closed_copy_state:' + str(task_id), {'state': 'retired', 'reason': reason, 'stage': str(stage_path)})
        if problems.record(store, 'workspaces', 'closed-copy-retired', reason, task=task_id, topic=current['topic']) is None:
            raise RuntimeError('Could not record retired copy')
    return True


async def verify_staged(store, stage_path, record):
    if 'space_k' not in record:
        return True
    if record.get('restoring'):
        await restore_staged(store, stage_path, record)
        return False
    parked_round = {'id': record['task'], 'iteration': record['space_k']}
    results = store.directory / 'results' / ('task-' + str(record['task']))
    if path_stamp(results) is None and await retire_missing_save(store, stage_path, record, results):
        return False
    folder = results / 'rounds' / str(record['space_k'])
    manifest_path = folder / 'saved.json'
    saved, manifest_stamp = await async_cleanup_scan(store, read_json_stamped, manifest_path)
    repository = Path(record['repository'])
    if record.get('loose'):
        if saved.get('head'):
            await verify(repository, folder, saved, staged=True, store=store)
        if path_stamp(manifest_path) != manifest_stamp:
            raise ValueError('Saved manifest changed during verification')
        return True
    task = store.task_get(record['task'])
    if record.get('error') and task and task['iteration'] == record['space_k'] and task['copy'] == 'held':
        return False
    await verify(repository, folder, saved, staged=True, store=store)
    copy = ws.staged_copy(stage_path, record)
    if not copy.exists() or record.get('deletion_started'):
        if path_stamp(manifest_path) != manifest_stamp:
            raise ValueError('Saved manifest changed during verification')
        await check_park_use(store, parked_round, copy)
        return True
    env = {'GIT_DIR': str(repository), 'GIT_WORK_TREE': str(copy),
           'GIT_INDEX_FILE': str(stage_path / 'git' / 'index')}
    pointer_path = stage_path / 'git' / 'HEAD'
    pointer, pointer_stamp = await async_cleanup_scan(store, read_stamped_file, pointer_path)
    if pointer_stamp is None:
        raise ValueError('Staged Git HEAD is missing')
    pointer = pointer.strip()
    branch = pointer.removeprefix('ref: ') if pointer.startswith('ref: ') else None
    if branch:
        code, head = await git(repository, 'rev-parse', '--verify', branch, check=False)
        if code:
            reason = 'saved branch was renamed or deleted after staging: ' + branch
            record['error'] = reason
            await ws._save_deletion_bounded(store, stage_path, record)
            if task:
                hold(store, dict(task, iteration=record['space_k']), reason)
            return False
    else:
        head = pointer
    head_matches = head == saved['head'] and (not branch or branch == saved['branch'])
    current_tree = await tree(store, copy, folder, env, saved['head'])
    current_ignored = await ignored(copy, folder, env, discard_names=store.get('close_saves_discard_names', []), store=store)
    reflog_env = dict(env, GIT_DIR=str(stage_path / 'git'), GIT_COMMON_DIR=str(repository))
    reflog_matches = not await reflog_commits(repository, copy, reflog_env, store=store)
    if (path_stamp(manifest_path) != manifest_stamp
            or path_stamp(pointer_path) != pointer_stamp):
        raise ValueError('Saved state changed during verification')
    if head_matches and reflog_matches and current_tree == saved['tree'] and current_ignored == saved['ignored']:
        await check_park_use(store, parked_round, copy)
        record['deletion_started'] = True
        await ws._save_deletion_bounded(store, stage_path, record)
        return True
    target, git_dir = Path(record['target']), Path(record['git_dir'])
    if target.exists() or git_dir.exists():
        raise ValueError('late writer changed a staged copy; another copy is already present')
    info = copy.lstat()
    record['restoring_copy_inode'] = [info.st_dev, info.st_ino]
    record['restoring'] = True
    await ws._save_deletion_bounded(store, stage_path, record)
    await restore_staged(store, stage_path, record)
    return False


async def deletion_delay(store):
    due = []
    for stage, record in await ws.async_deletion_stages(store):
        if 'space_k' not in record:
            continue
        if record.get('error'):
            task = store.task_get(record['task'])
            if (not task or task['iteration'] != record['space_k']
                    or not (task['status'] in CLOSED or parked(store, task)) or task['copy'] != 'saving'):
                continue
        due.append(sweeps.waiting(store, 'closed-copy-delete', stage.name))
    return max(0, min(due) - time.time()) if due else None


def stop_leftovers(hosts):
    """Stop a finished worker's leftover processes through its process ownership record."""
    from . import process_ownership
    from .worker_storage import processes_gone
    gone = True
    for host in hosts:
        if processes_gone(host):
            continue
        record = process_ownership.read_owner(host)
        if record is None or process_ownership.cleanup(host)['status'] not in ('ok', 'stale'):
            gone = False
    return gone


def fold_name(name):
    """Fold case and Unicode form so a name matches a link on case-insensitive or normalizing file systems."""
    return unicodedata.normalize('NFD', unicodedata.normalize('NFD', name).casefold())


def resolve_folders(names):
    """Resolve each folder, listing every parent folder once to find links instead of walking each path."""
    parents, links, result = {}, {}, {}
    for name in names:
        path = Path(name)
        parent = path.parent
        if not path.is_absolute() or path.name in ('', '..'):
            result[name] = path.resolve()
            continue
        if parent not in parents:
            parents[parent] = parent.resolve()
            try:
                with os.scandir(parent) as entries:
                    links[parent] = {fold_name(entry.name) for entry in entries if entry.is_symlink()}
            except OSError:
                links[parent] = None
        if links[parent] is None or fold_name(path.name) in links[parent]:
            result[name] = path.resolve()
        else:
            result[name] = parents[parent] / path.name
    return result


def folders_inside(names, roots):
    """Return the names whose resolved folder is inside one of the roots."""
    prefixes = [str(root).rstrip('/') + '/' for root in roots]
    return [name for name, path in resolve_folders(names).items()
            if any((str(path) + '/').startswith(prefix) for prefix in prefixes)]


async def stopped_workers(store, task, reap=False):
    from .worker_storage import storage_for, _processes_gone
    storage = storage_for(store)
    owned = roots(store, task)
    seen, inside = set(), []
    while fresh := [row['cwd'] for row in store.db.execute(
            "SELECT DISTINCT cwd FROM workers WHERE cwd IS NOT NULL AND cwd!=''") if row['cwd'] not in seen]:
        seen.update(fresh)
        inside += await storage.inspect(folders_inside, fresh, owned)
    rows = store.db.execute('SELECT * FROM workers WHERE task=? OR cwd IN (SELECT value FROM json_each(?)) ORDER BY id',
                            (task['id'], json.dumps(inside))).fetchall()
    for row in rows:
        hosts = await storage.host_paths(dict(row))
        if await storage.inspect(_processes_gone, hosts):
            continue
        if row['status'] not in FINISHED:
            raise ValueError(RUNNING[1])
        if not reap or not await storage.inspect(stop_leftovers, hosts):
            raise ValueError('finished worker processes still use job scratch')


def measure_retained_paths(paths):
    from . import stray_report
    total = 0
    for path in paths:
        if path.is_dir() and not path.is_symlink():
            total += stray_report.Metadata(threading.Event()).measure(path)[0]
    return total


async def retry_holds(store, now=None):
    """Requeue held done or dropped jobs each hour and tell the owner once when a hold passes 24 hours.
    Requeue the other held jobs of a closed round, such as a parked open job, after 1, 2, 4 hours and so on,
    at most RETRY_LIMIT apart. A hold on lost work is not requeued."""
    from . import stray_report
    now = time.time() if now is None else now
    holds = store.get('close_save_holds', {})
    current = {}

    async def retry(task):
        if (task['copy'] == 'saving' and str(task['id']) not in holds or kept(store, task['id'])
                or not closed_round(store, task)):
            return
        terminal = task['status'] in TERMINAL
        entry = holds.get(str(task['id']))
        if not entry or entry['k'] != task['iteration']:
            entry = {'k': task['iteration'], 'since': now}
        current[str(task['id'])] = entry
        if task['copy'] != 'held' or entry.get('lost'):
            return
        if terminal and not entry.get('noticed') and now - entry['since'] >= NOTICE_SECONDS and task['copy_reason'] not in RUNNING:
            topic = store.topic(task['topic'])
            target = (task['topic'] if topic and topic['enabled'] and (store.get('topic_lifecycle:' + task['topic']) or {}).get('state') is None
                      else store.get('coordinator_home_topic'))
            if target and (store.topic(target) or {}).get('enabled'):
                paths = [ws._workspace_target(store.directory, task['id']), scratch(store, task['id'])]
                paths += [ws.staged_copy(stage, record) for stage, record in await ws.async_deletion_stages(store) if record['task'] == task['id']]
                try:
                    total = await async_cleanup_scan(store, measure_retained_paths, paths)
                except (OSError, RuntimeError, stray_report.ScanLimit):
                    total = None
                text = 'Job %s in %s still holds %s after 24 hours: %s. Torii retries its close save every hour.' % (
                    task['number'], topic['name'] if topic else task['topic'],
                    'an unknown size' if total is None else stray_report.size(total), task['copy_reason'] or 'unknown reason')
                with store.transaction():
                    store.enqueue_report(target, text)
                entry['noticed'] = now
        wait = ws.CLEANUP_INTERVAL if terminal else min(ws.CLEANUP_INTERVAL * 2 ** min(entry.get('attempts', 0), 5), RETRY_LIMIT)
        if now - entry.get('retried', 0) >= wait:
            entry['retried'] = now
            entry['attempts'] = entry.get('attempts', 0) + 1
            with store.transaction():
                store.db.execute("UPDATE tasks SET copy='saving' WHERE id=? AND copy='held'", (task['id'],))
    finished = set()

    async def retry_job(task):
        await retry(task)
        finished.add(str(task['id']))
    tasks = [dict(row) for row in store.db.execute("SELECT * FROM tasks WHERE copy IN ('held','saving') ORDER BY id")]
    await sweeps.arun(store, 'hold-retry', tasks, retry_job, key=lambda task: task['id'],
                      refs=lambda task: {'task': task['id']})
    current.update({str(task['id']): holds[str(task['id'])] for task in tasks
                    if str(task['id']) not in finished and str(task['id']) in holds})
    with store.transaction():
        store.put('close_save_holds', current)


async def expire_ignored(store, now=None):
    """Delete the git-ignored file copies of a done or dropped round once it has been closed for IGNORED_SECONDS."""
    now = time.time() if now is None else now
    rows = store.db.execute("""SELECT s.task,s.k,s.data FROM task_space_saves s
        JOIN tasks t ON t.id=s.task JOIN task_iterations i ON i.task=s.task AND i.k=s.k
        WHERE t.status IN ('done','dropped') AND s.phase IN ('cleared','restored')
        AND i.closed IS NOT NULL AND i.closed<? ORDER BY s.task,s.k""", (now - IGNORED_SECONDS,)).fetchall()
    await sweeps.arun(store, 'ignored-expiry', rows, lambda row: expire_round_ignored(store, row),
                      key=lambda row: '%s/%s' % (row['task'], row['k']), refs=lambda row: {'task': row['task']})


async def expire_round_ignored(store, row):
    folder = store.directory / 'results' / ('task-' + str(row['task'])) / 'rounds' / str(row['k'])
    saved = json.loads(row['data'])
    copied = saved.get('ignored', {}).get('copied', [])
    if copied:
        saved['ignored']['expired'] = saved['ignored'].get('expired', []) + [
            {key: entry[key] for key in ('name', 'bytes') if key in entry} for entry in copied]
        saved['ignored']['copied'] = []
        with store.transaction():
            current = store.db.execute('SELECT data FROM task_space_saves WHERE task=? AND k=?',
                                       (row['task'], row['k'])).fetchone()
            if not current or current['data'] != row['data']:
                return
            store.db.execute('UPDATE task_space_saves SET data=? WHERE task=? AND k=?',
                             (json.dumps(saved), row['task'], row['k']))
        if (folder / 'saved.json').is_file():
            await async_atomic_json(store, folder / 'saved.json', saved)
    ignored_folder = folder / 'ignored'
    if ignored_folder.is_dir() and not ignored_folder.is_symlink() and ignored_folder.resolve() == ignored_folder:
        await ws.delete_tree(ignored_folder, paced=True)


def report_blocked(store, now):
    """Report each results folder that fade could not touch for BLOCKED_SECONDS once, so the owner can decide."""
    for row in store.db.execute('SELECT task,reason FROM results_blocked WHERE since<? ORDER BY task',
                                (now - BLOCKED_SECONDS,)).fetchall():
        if not store.db.execute("SELECT 1 FROM problems WHERE area='cleanup' AND code='results-blocked' AND task=? LIMIT 1",
                                (row['task'],)).fetchone():
            problems.record(store, 'cleanup', 'results-blocked', row['reason'], task=row['task'])


def expiring_entries(root):
    """The fewest paths that hold every expirable file: all but rounds/ and git bundles, as fade keeps them."""
    def walk(folder):
        claims, keeps = [], False
        for path in sorted(folder.iterdir()):
            if folder == root and path.name == 'rounds':
                continue
            if path.is_dir() and not path.is_symlink():
                inner, held = walk(path)
                claims.extend(inner if held else [path])
                keeps = keeps or held
            elif path.is_file() and not path.is_symlink() and git_bundle(path):
                keeps = True
            else:
                claims.append(path)
        return claims, keeps
    try:
        if root.is_symlink() or not root.is_dir() or root.resolve() != root:
            return []
        return walk(root)[0]
    except OSError:
        return []


def expiry_current(store, task, root):
    current = store.task_get(task['id'])
    return bool(current and current['iteration'] == task['iteration'] and current['status'] in TERMINAL
                and current['copy'] not in ('held', 'saving') and not kept(store, task['id'])
                and not pending_send(store, [root])
                and not store.db.execute('SELECT 1 FROM keep_queue WHERE task=? LIMIT 1', (task['id'],)).fetchone()
                and not store.db.execute("""SELECT 1 FROM workers WHERE task=?
                    AND status NOT IN ('done','interrupted') LIMIT 1""", (task['id'],)).fetchone())


async def expire_results(store, now=None):
    """Delete a faded done or dropped job's results, except the rounds/ saves, RESULTS_SECONDS after its close."""
    now = time.time() if now is None else now
    report_blocked(store, now)
    rows = store.db.execute("""SELECT t.id FROM tasks t JOIN task_iterations i ON i.task=t.id AND i.k=t.iteration
        JOIN results_shrunk f ON f.task=t.id AND f.k=i.k AND f.closed=i.closed
        WHERE t.status IN ('done','dropped') AND i.closed<? ORDER BY t.id""", (now - RESULTS_SECONDS,)).fetchall()
    tasks = []

    async def check(task_id):
        task = store.task_get(task_id)
        root = store.directory / 'results' / ('task-' + str(task_id))
        if await async_cleanup_scan(store, expiring_entries, root) and expiry_current(store, task, root) and not results_blocked(store, task_id):
            tasks.append(task)
    await sweeps.arun(store, 'results-expiry-check', [row['id'] for row in rows], check,
                      refs=lambda task_id: {'task': task_id})
    if not tasks:
        return
    try:
        references = await async_scan_references(store)
    except Exception as error:
        problems.record(store, 'cleanup', 'reference-scan-failed', type(error).__name__, level=logging.ERROR, every=3600)
        return
    await sweeps.arun(store, 'results-expiry', tasks, lambda task: expire_job_results(store, task, references),
                      key=lambda task: task['id'], refs=lambda task: {'task': task['id']})


async def expire_job_results(store, task, references):
    root = store.directory / 'results' / ('task-' + str(task['id']))
    try:
        project = Path(store.topic(task['topic'])['cwd'])
        try:
            project = Path((await ws.project_identity(project, timeout=ws.GIT_TIMEOUT))['project_root'])
        except FileNotFoundError:
            if project.exists():
                raise
        manifest, _ = await async_cleanup_scan(store, read_stamped_file, project / 'KEEP' / 'MANIFEST.md')
        if re.search(r'results/task-%d(?![0-9])' % task['id'], manifest):
            return
        total, entries = await async_cleanup_scan(store, sorted_result_entries,
                                                  await async_cleanup_scan(store, file_entries, root))
        claims = await async_cleanup_scan(store, expiring_entries, root)
        live, stamp = await async_scan_task_references(store, task['id'], references)
        stage = directory(scratch(store, task['id']) / ('results-expiry-' + uuid.uuid4().hex), store.directory)
        with store.transaction():
            if live[str(task['id'])] or not references_current(store, stamp) or not expiry_current(store, task, root):
                claims = []
            for path in claims:
                target = stage / path.relative_to(root)
                target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                try:
                    path.rename(target)
                except FileNotFoundError:
                    pass
        if total > RESULTS_LARGE and claims:
            ws.logger.info('results expiry large folder job=%s bytes=%d largest=%s', task['id'], total,
                           json.dumps([[str(path.relative_to(root)), size] for path, size in entries[:5]]))
        counts = {'bytes': 0, 'files': 0}
        await ws.delete_tree(stage, progress=counts, paced=True)
        try:
            stage.parent.rmdir()
        except OSError:
            pass
        if not claims:
            return
        with store.transaction():
            store.task_note(task['id'], 'Results expired %d days after close: removed %d files, %d bytes; rounds/ and git bundles are kept.'
                            % (RESULTS_SECONDS // 86400, counts['files'], counts['bytes']))
    except OSError as error:
        if not block_results(store, task['id'], error):
            raise
