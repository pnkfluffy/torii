"""Retain OpenCode usage before removing finished worker homes."""

import asyncio
import json
import os
from pathlib import Path
import pwd
import re
import sqlite3
import stat
import time

from . import close_saves, host_os, problems, stray_report, sweeps, worker_storage, workspaces
from .session_storage import named_lease

GRACE = 3 * 24 * 60 * 60
QUERY = """SELECT id,session_id,time_created,json_extract(data,'$.time.created'),
    json_extract(data,'$.time.completed'),json_extract(data,'$.modelID'),json_extract(data,'$.providerID'),
    json_extract(data,'$.tokens.input'),json_extract(data,'$.tokens.output'),
    json_extract(data,'$.tokens.reasoning'),json_extract(data,'$.tokens.cache.read'),
    json_extract(data,'$.tokens.cache.write'),json_extract(data,'$.tokens.total'),json_extract(data,'$.cost')
    FROM message WHERE json_extract(data,'$.role')='assistant'"""
FINISHED = ('done', 'interrupted')
FIELDS = ('id', 'session_id', 'time_created', 'api_created', 'completed', 'model', 'provider',
          'input', 'output', 'reasoning', 'cache_read', 'cache_write', 'total', 'cost')


def exists(path):
    try:
        path.lstat()
        return True
    except FileNotFoundError:
        return False


def live_links(directory, tasks):
    targets = set()
    for task in tasks:
        roots = [directory / name / ('task-' + str(task['id'])) for name in ('worktrees', 'scratch', 'results')]
        if task['worktree']:
            roots.append(Path(task['worktree']))
        for root in roots:
            if not exists(root):
                continue
            if stat.S_ISLNK(root.lstat().st_mode):
                targets.add(root.resolve())
                continue
            for base, dirs, names in os.walk(root, followlinks=False, onerror=lambda error: (_ for _ in ()).throw(error)):
                for name in dirs + names:
                    path = Path(base) / name
                    if stat.S_ISLNK(path.lstat().st_mode):
                        targets.add(path.resolve())
    return targets


def inventory(root, cutoff):
    if root.is_symlink() or root.resolve() != root or not root.is_dir():
        raise ValueError('Unsafe OpenCode worker folder')
    device = root.stat().st_dev
    seen = set()
    total = 0
    for base, dirs, names in os.walk(root, followlinks=False, onerror=lambda error: (_ for _ in ()).throw(error)):
        for name in ['.', *dirs, *names]:
            path = Path(base) / name
            info = path.lstat()
            if info.st_dev != device:
                raise ValueError('Worker folder crosses filesystems')
            if not stat.S_ISDIR(info.st_mode) and not name.endswith('-shm') and info.st_mtime > cutoff:
                return None
            key = (info.st_dev, info.st_ino)
            if key not in seen and info.st_nlink == 1:
                total += info.st_blocks * 512
            seen.add(key)
    return total


def save_usage(root, target, anchor, *, immutable=False):
    database = root / 'data' / 'opencode' / 'opencode.db'
    if database.is_symlink() or database.resolve() != database or not database.is_file():
        raise ValueError('OpenCode usage database is unavailable')
    wal = database.with_name(database.name + '-wal')
    mode = '?mode=ro' if not immutable and exists(wal) else '?mode=ro&immutable=1'
    con = sqlite3.connect(database.as_uri() + mode, uri=True)
    try:
        con.execute('BEGIN')
        sessions = list(con.execute('SELECT id,parent_id,directory,tokens_input,tokens_output,tokens_reasoning,tokens_cache_read,tokens_cache_write,cost FROM session'))
        messages = list(con.execute(QUERY))
    finally:
        con.close()
    close_saves.directory(target.parent, anchor)
    records = {}
    if target.is_symlink():
        raise ValueError('Unsafe OpenCode usage file')
    if target.exists():
        fd = os.open(str(target), os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd) as stream:
            for line in stream:
                record = json.loads(line)
                kind = record['type']
                if (kind not in ('session', 'message') or
                    (kind == 'session' and len(record['values']) != 9) or
                    (kind == 'message' and not set(FIELDS).issubset(record))):
                    raise ValueError('Unexpected OpenCode usage record')
                key = (kind, record['values'][0] if kind == 'session' else record['session_id'],
                       None if kind == 'session' else record['id'])
                records[key] = record
    for row in sessions:
        records[('session', row[0], None)] = {'type': 'session', 'values': row}
    for row in messages:
        records[('message', row[1], row[0])] = {'type': 'message', **dict(zip(FIELDS, row))}
    temporary = target.with_name(target.name + '.tmp')
    fd = os.open(str(temporary), os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as stream:
        for record in records.values():
            stream.write(json.dumps(record, separators=(',', ':')) + '\n')
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(target)
    fd = os.open(str(target.parent), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def eligible(db, directory, worker):
    row = db.execute('''SELECT w.status,t.status AS task_status,t.copy,t.iteration,t.worktree
        FROM workers w JOIN tasks t ON t.id=w.task WHERE w.id=? AND w.task=? AND w.provider='opencode' ''',
                     (worker['id'], worker['task'])).fetchone()
    if not row or row['status'] not in FINISHED or row['task_status'] not in ('done', 'dropped') or row['copy'] != 'cleared':
        return None
    paths = (workspaces._workspace_target(directory, worker['task']),
             directory / 'scratch' / ('task-' + str(worker['task'])))
    if any(exists(path) for path in paths) or (row['worktree'] and exists(Path(row['worktree']))):
        return None
    if not db.execute('SELECT 1 FROM task_iterations WHERE task=? AND k=?',
                      (worker['task'], row['iteration'])).fetchone():
        return None
    return row


def in_use(root, paths):
    return any(path == root or root in path.parents for path in paths)


def worker_home_lease(directory, worker_id):
    return named_lease(directory, 'opencode-worker-home-' + str(worker_id))


def database_was_trashed(db, root, worker_id):
    return db.execute('''SELECT 1 FROM session_deletions d JOIN native_sessions n ON n.id=d.native
        WHERE n.worker=? AND d.provider='opencode' AND d.home=? AND d.root=?
        AND d.path='opencode.db' AND d.state='trashed' LIMIT 1''',
        (worker_id, str(root), str(root / 'data' / 'opencode'))).fetchone() is not None


def remove(directory, worker, cutoff):
    with worker_home_lease(directory, worker['id']):
        remove_home(directory, worker, cutoff)


def remove_home(directory, worker, cutoff):
    root = directory / 'opencode' / 'workers' / str(worker['id'])
    if inventory(root, cutoff) is None:
        return
    paths = stray_report.process_paths()
    paths.extend(stray_report.launch_paths((host_os.user_launch_agents(Path(pwd.getpwuid(os.getuid()).pw_dir)),
                                           host_os.system_launch_agents())))
    if in_use(root, paths):
        return
    db = sqlite3.connect(str(directory / 'state.sqlite'), timeout=30)
    db.row_factory = sqlite3.Row
    try:
        if not eligible(db, directory, worker):
            return
        usage = directory / 'opencode' / 'usage' / (str(worker['id']) + '.jsonl')
        database = root / 'data' / 'opencode' / 'opencode.db'
        if exists(database) or not database_was_trashed(db, root, worker['id']):
            save_usage(root, usage, directory)
        elif usage.resolve() != usage or not usage.is_file():
            raise ValueError('Saved OpenCode usage is unavailable')
        size = inventory(root, cutoff)
        if size is None or in_use(root, stray_report.process_paths()):
            return
        db.execute('BEGIN IMMEDIATE')
        row = eligible(db, directory, worker)
        if not row:
            return
        host_os.move_to_trash(root, name='torii-opencode-worker-' + str(worker['id']))
        db.execute('UPDATE task_iterations SET bytes_freed=COALESCE(bytes_freed,0)+? WHERE task=? AND k=?',
                   (size, worker['task'], row['iteration']))
        db.commit()
    finally:
        db.close()


def remnants(workers):
    if not workers.is_dir():
        return []
    return sorted(path for path in workers.iterdir()
                  if stat.S_ISDIR(path.lstat().st_mode) and re.fullmatch(r'[0-9]+\.deleting-[a-f0-9]{32}', path.name))


async def sweep(store):
    storage = worker_storage.storage_for(store)
    found = []
    await sweeps.arun(store, 'opencode-remnants', [store.directory / 'opencode' / 'workers'],
                      lambda folder: asyncio.to_thread(lambda: found.extend(remnants(folder))), key=lambda folder: 'list')
    await sweeps.arun(store, 'opencode-remnant', found, lambda path: asyncio.to_thread(
        host_os.move_to_trash, path, name='torii-opencode-worker-' + path.name), key=lambda path: path.name)
    workers = [dict(row) for row in store.db.execute('''SELECT w.* FROM workers w JOIN tasks t ON t.id=w.task
        WHERE w.provider='opencode' AND w.status IN (?,?)
        AND t.status IN ('done','dropped') AND t.copy='cleared' ''', FINISHED)]
    if not any(os.path.lexists(store.directory / 'opencode' / 'workers' / str(worker['id'])) for worker in workers):
        return
    tasks = [dict(row) for row in store.db.execute("SELECT id,worktree FROM tasks WHERE status NOT IN ('done','dropped')")]
    try:
        links = await asyncio.to_thread(live_links, store.directory, tasks)
    except (OSError, RuntimeError) as error:
        problems.record(store, 'cleanup', 'opencode-worker-cleanup-links', type(error).__name__, every=3600)
        return

    async def clean(worker):
        await asyncio.sleep(0)
        root = store.directory / 'opencode' / 'workers' / str(worker['id'])
        if not root.exists() or any(path == root or root in path.parents or path in root.parents for path in links):
            return
        if await storage.inspect(worker_storage._processes_gone, await storage.host_paths(worker)):
            try:
                await asyncio.to_thread(remove, store.directory, worker, time.time() - GRACE)
            except BlockingIOError:
                return
    await sweeps.arun(store, 'opencode-worker-cleanup', workers, clean, key=lambda worker: worker['id'],
                      refs=lambda worker: {'task': worker['task'], 'worker': worker['id']})
