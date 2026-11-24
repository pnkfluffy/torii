"""Move old, unowned cleanup stages to Trash after checking live references."""

import fcntl
import os
from pathlib import Path
import re
import sqlite3
import stat
import time
from types import SimpleNamespace
import uuid

from . import host_os, idle_trim, stray_report, sweeps, workspaces


AGE = 24 * 60 * 60


def candidates(directory, now):
    found = []
    for root, pattern in (
            (directory / 'worktree-deletion-preparing', r'(task-(\d+)-[0-9a-f]{32})-[0-9a-f]{32}'),
            (directory / 'idle-trim-quarantine', r'(task-(\d+))-[0-9a-f]{32}'),
            (workspaces.cow_root(directory) / '.idle-trim-quarantine', r'(task-(\d+))-[0-9a-f]{32}')):
        if not root.exists() and not root.is_symlink():
            continue
        if root.is_symlink() or root.resolve() != root:
            raise ValueError('Unsafe orphan stage folder')
        for path in root.iterdir():
            match = re.fullmatch(pattern, path.name)
            info = path.lstat()
            if match and stat.S_ISDIR(info.st_mode) and info.st_mtime <= now - AGE:
                found.append((path, int(match[2]), match[1], (info.st_dev, info.st_ino, info.st_mtime_ns)))
    return found


def referenced(db, path, task):
    row = db.execute("SELECT status,copy FROM tasks WHERE id=?", (task,)).fetchone()
    if row and (row[0] not in ('done', 'dropped', 'expired') or row[1] == 'saving'):
        return True
    if db.execute("SELECT 1 FROM workers WHERE task=? AND status NOT IN ('done','interrupted') LIMIT 1", (task,)).fetchone():
        return True
    queries = (
        "SELECT 1 FROM tasks WHERE status IN ('open','waiting','interrupted','proposed') AND instr(notes,?) LIMIT 1",
        "SELECT 1 FROM keep_links WHERE instr(locator,?) LIMIT 1",
        "SELECT 1 FROM workers WHERE status NOT IN ('done','interrupted') AND (instr(prompt,?) OR instr(cwd,?)) LIMIT 1",
        "SELECT 1 FROM wait_jobs j JOIN waits w ON w.id=j.wait WHERE w.state='armed' AND instr(j.then_prompt,?) LIMIT 1",
        "SELECT 1 FROM outbox WHERE delivered=0 AND retired=0 AND (instr(image,?) OR instr(image_source,?)) LIMIT 1",
    )
    values = (str(path), str(path.relative_to(path.parent.parent)))
    return any(db.execute(query, (value,) * query.count('?')).fetchone() for value in values for query in queries)


def linked_from_open_jobs(db, directory, path):
    roots = []
    for row in db.execute("SELECT id,worktree FROM tasks WHERE status IN ('open','waiting','interrupted','proposed')"):
        roots.extend(directory / name / ('task-' + str(row[0])) for name in ('scratch', 'results', 'worktrees'))
        if row[1]:
            roots.append(Path(row[1]))
    for root in roots:
        if not os.path.lexists(root):
            continue
        if root.is_symlink():
            return True
        def fail(error):
            raise error
        for base, dirs, files in os.walk(root, followlinks=False, onerror=fail):
            for name in dirs + files:
                item = Path(base) / name
                if item.is_symlink():
                    target = item.resolve()
                    if target == path or target.is_relative_to(path):
                        return True
    return False


def sweep(directory, now=None):
    directory = Path(directory)
    now = time.time() if now is None else now
    entries = candidates(directory, now)
    if not entries:
        return 0
    paths = stray_report.process_paths()
    home = Path.home()
    paths.extend(stray_report.launch_paths((host_os.user_launch_agents(home),
                                            host_os.system_launch_agents(), host_os.system_launch_daemons())))
    paths = [path.resolve() for path in paths]
    db = sqlite3.connect((directory / 'state.sqlite').as_uri() + '?mode=rw', uri=True, timeout=30)
    moved = []
    try:
        sweeps.run(SimpleNamespace(directory=directory, db=db), 'orphan-stage', entries,
                   lambda entry: moved.extend(move_orphan(directory, db, paths, *entry)),
                   key=lambda entry: entry[0].name, refs=lambda entry: {'task': entry[1]})
    finally:
        db.close()
    return len(moved)


def move_orphan(directory, db, paths, path, task, stage, identity):
    fd = idle_trim.lock_file(directory, task)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return ()
        if any(value == path or value.is_relative_to(path) for value in paths):
            return ()
        if referenced(db, path, task) or linked_from_open_jobs(db, directory, path):
            return ()
        db.execute('BEGIN IMMEDIATE')
        try:
            owned = (os.path.lexists(directory / 'worktree-deletions' / stage)
                     or os.path.lexists(directory / 'worktree-deletion-finalizing' / stage)
                     or any(os.path.lexists(staged) for staged in idle_trim.stage_paths(directory, task)))
            info = path.lstat()
            if (owned or referenced(db, path, task) or path.parent.is_symlink()
                    or path.parent.resolve() != path.parent or path.is_symlink()
                    or (info.st_dev, info.st_ino, info.st_mtime_ns) != identity):
                db.rollback()
                return ()
            trash = (workspaces.trash_for(directory, path) if workspaces.on_volume(directory, path)
                     else host_os.trash_directory())
            trash.mkdir(mode=0o700, parents=True, exist_ok=True)
            if trash.is_symlink() or trash.resolve() != trash or trash.stat().st_dev != info.st_dev:
                db.rollback()
                return ()
            idle_trim.rename_exclusive(path, trash / (host_os.ORPHAN_TRASH_PREFIX + path.name + '-' + uuid.uuid4().hex))
            db.commit()
            return (path,)
        except BaseException:
            db.rollback()
            raise
    finally:
        os.close(fd)
