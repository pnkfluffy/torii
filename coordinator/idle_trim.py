"""Trim inactive job scratch and ignored work-copy dependencies."""

import asyncio
from contextlib import contextmanager
import errno
import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
import stat
import subprocess
import time
import uuid

from . import close_saves, extension, host_os, problems, scan_process, sweeps, worker_storage, workspaces
from .control_api import Refused, stale_tasks
from .host_os import rename_exclusive
from .store import Store

logger = logging.getLogger(__name__)
NAMES = frozenset(('node_modules', '.next', 'dist', 'build', '.turbo', '.vite', '.venv', 'venv',
                   'target', '__pycache__', '.pytest_cache', 'coverage', '.parcel-cache'))
SCRATCH_NAMES = frozenset(('node_modules', '.venv', 'venv', '__pycache__', '.pytest_cache',
                           '.turbo', '.vite', '.parcel-cache', '.cache'))
_orphan_sweeps = {}


def stage_path(directory, task, source=None):
    """Stage a trimmed folder on its own filesystem: a tree on the copy-on-write volume stages there."""
    return workspaces.volume_side(directory, source, 'idle-trim-stages') / ('task-' + str(task))


def stage_paths(directory, task):
    return stage_path(directory, task), workspaces.cow_root(directory) / '.idle-trim-stages' / ('task-' + str(task))


def quarantine_path(directory, stage):
    return workspaces.volume_side(directory, stage, 'idle-trim-quarantine')


def path_signature(path):
    info = path.lstat()
    return [info.st_dev, info.st_ino, info.st_mtime_ns, info.st_ctime_ns]


def optional_signature(path):
    try:
        return path_signature(path)
    except FileNotFoundError:
        return None


def tree_fingerprint(root):
    before = path_signature(root)
    pending = [(root, '')]
    combined = 0
    count = 0
    while pending:
        parent, prefix = pending.pop()
        with os.scandir(parent) as entries:
            for entry in entries:
                info = entry.stat(follow_symlinks=False)
                relative = prefix + entry.name
                value = (relative, info.st_dev, info.st_ino, info.st_mode, info.st_size,
                         info.st_mtime_ns, info.st_ctime_ns)
                combined ^= int.from_bytes(hashlib.sha256(repr(value).encode()).digest(), 'big')
                count += 1
                if stat.S_ISDIR(info.st_mode):
                    pending.append((Path(entry.path), relative + '/'))
    if before != path_signature(root):
        raise ValueError('Idle trim tree changed during scan')
    return count, combined


def prepare_stage(store, task, source, expected=None):
    stage = stage_path(store.directory, task, source)
    root = stage.parent
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.is_symlink() or root.resolve() != root:
        raise ValueError('Unsafe idle trim staging root')
    stage.mkdir(mode=0o700)
    try:
        root_fd = os.open(str(root), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(root_fd)
        finally:
            os.close(root_fd)
        parent = source.parent.stat()
        current = source.lstat()
        if source.parent.resolve() != source.parent:
            raise ValueError('Unsafe idle trim source parent')
        if not stat.S_ISDIR(current.st_mode) or current.st_dev != stage.stat().st_dev:
            raise ValueError('Idle trim staging requires a directory on the same filesystem')
        signature = path_signature(source)
        if expected is not None and signature != expected:
            raise ValueError('Idle trim source changed after its scan')
        stage_info = stage.lstat()
        record = {
            'task': task, 'source': str(source), 'parent': [parent.st_dev, parent.st_ino],
            'source_inode': [current.st_dev, current.st_ino], 'signature': signature,
            'stage_inode': [stage_info.st_dev, stage_info.st_ino], 'phase': 'ready'}
        close_saves.atomic_json(stage / 'intent.json', record)
    except BaseException:
        if not any(stage.iterdir()):
            stage.rmdir()
        raise
    return stage, record


def stage_move(store, task, source, stage, record):
    parent = source.parent.stat()
    current = source.lstat()
    stage_info = stage.lstat()
    if (source.parent.resolve() != source.parent or [parent.st_dev, parent.st_ino] != record['parent']
            or stage.is_symlink() or stage.resolve() != stage
            or [stage_info.st_dev, stage_info.st_ino] != record['stage_inode']
            or [current.st_dev, current.st_ino, current.st_mtime_ns, current.st_ctime_ns] != record['signature']):
        raise ValueError('Idle trim source changed')
    rename_exclusive(source, stage / 'data')
    store.db.execute('UPDATE idle_trims SET active=2 WHERE task=?', (task,))


def discard_stage(stage):
    (stage / 'intent.json').unlink()
    stage.rmdir()


def read_stage_record(stage):
    fd = os.open(str(stage / 'intent.json'), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd) as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > 8192:
            raise ValueError('Invalid idle trim stage intent')
        return json.load(handle)


def recover_stage(store, task):
    return all([recover_one(store, task, stage) for stage in stage_paths(store.directory, task)])


def recover_one(store, task, stage):
    if not stage.exists():
        return True
    if stage.is_symlink() or stage.resolve() != stage:
        raise ValueError('Unsafe idle trim stage')
    intent = stage / 'intent.json'
    if not intent.exists() and not (stage / 'data').exists():
        stage.rmdir()
        return True
    record = read_stage_record(stage)
    source = Path(record['source'])
    roots = (store.directory / 'scratch' / ('task-' + str(task)), *workspaces._workspace_targets(store.directory, task))
    if record['task'] != task or not any(source == root or source.is_relative_to(root) for root in roots):
        raise ValueError('Invalid idle trim stage source')
    data = stage / 'data'
    if record.get('phase') == 'deleting':
        info = stage.lstat()
        if [info.st_dev, info.st_ino] != record['stage_inode']:
            raise ValueError('Idle trim stage changed')
        quarantine = quarantine_path(store.directory, stage)
        quarantine.mkdir(mode=0o700, parents=True, exist_ok=True)
        if quarantine.is_symlink() or quarantine.resolve() != quarantine:
            raise ValueError('Unsafe idle trim quarantine')
        target = quarantine / (stage.name + '-' + uuid.uuid4().hex)
        rename_exclusive(stage, target)
        problems.record(store, 'cleanup', 'stage-quarantined', str(target), task=task, every=300)
        if store.db.in_transaction:
            store.db.execute('UPDATE idle_trims SET active=0 WHERE task=?', (task,))
        else:
            with store.transaction():
                store.db.execute('UPDATE idle_trims SET active=0 WHERE task=?', (task,))
        return True
    if source.parent.resolve() != source.parent:
        raise ValueError('Invalid idle trim stage source')
    parent = source.parent.stat()
    if [parent.st_dev, parent.st_ino] != record['parent']:
        raise ValueError('Idle trim source parent changed')
    if data.exists() or data.is_symlink():
        info = data.lstat()
        if not stat.S_ISDIR(info.st_mode) or [info.st_dev, info.st_ino] != record['source_inode']:
            raise ValueError('Idle trim staged data changed')
        if os.path.lexists(source):
            problems.record(store, 'cleanup', 'stage-collision', 'task=%s' % task, every=300)
            return False
        if record.get('phase') != 'ready':
            problems.record(store, 'cleanup', 'stage-incomplete', 'task=%s' % task, every=300)
            return False
        rename_exclusive(data, source)
    discard_stage(stage)
    if store.db.in_transaction:
        store.db.execute('UPDATE idle_trims SET active=0 WHERE task=?', (task,))
    else:
        with store.transaction():
            store.db.execute('UPDATE idle_trims SET active=0 WHERE task=?', (task,))
    return True


def delete_stage(stage, counts):
    record = read_stage_record(stage)
    record['phase'] = 'deleting'
    close_saves.atomic_json(stage / 'intent.json', record)
    remove(stage / 'data', counts)
    discard_stage(stage)


def pending_stages(root):
    if not root.exists():
        return []
    if root.is_symlink() or root.resolve() != root:
        raise ValueError('Unsafe staging root')
    return [int(stage.name[5:]) for stage in root.iterdir()
            if stage.name.startswith('task-') and stage.name[5:].isdigit()]


def recover_pending(store):
    now = time.monotonic()
    key = str(store.directory)
    if now >= _orphan_sweeps.get(key, 0):
        _orphan_sweeps[key] = now + 3600
        from . import orphan_stages
        try:
            bounded_scan(store, orphan_stages.sweep, store.directory)
        except scan_process.ScanError:
            pass
    tasks = set()
    for root in (store.directory / 'idle-trim-stages', workspaces.cow_root(store.directory) / '.idle-trim-stages'):
        if not root.exists() and not root.is_symlink():
            continue
        try:
            tasks.update(bounded_scan(store, pending_stages, root))
        except Exception:
            continue
    sweeps.run(store, 'stage-recovery', sorted(tasks), lambda task: recover_unlocked(store, task),
               refs=lambda task: {'task': task})


def recover_unlocked(store, task):
    fd = lock_file(store.directory, task)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        recover_stage(store, task)
    finally:
        os.close(fd)


def lock_file(directory, task):
    folder = directory / 'locks'
    folder.mkdir(mode=0o700, parents=True, exist_ok=True)
    return os.open(str(folder / ('idle-trim-%d.lock' % task)), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)


@contextmanager
def scratch_guard(store, task):
    fd = lock_file(store.directory, task)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise Refused('This job is being trimmed. Retry the worker spawn shortly.')
        if any(os.path.lexists(stage) for stage in stage_paths(store.directory, task)):
            try:
                recover_stage(store, task)
            except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as error:
                problems.record(store, 'cleanup', 'stage-invalid', type(error).__name__, task=task, every=300)
        for stage in stage_paths(store.directory, task):
            if not os.path.lexists(stage):
                continue
            if stage.is_symlink() or stage.resolve() != stage:
                raise Refused('This job has an unsafe cleanup stage.')
            try:
                record = read_stage_record(stage)
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                raise Refused('This job has an invalid cleanup stage.') from None
            if record.get('phase') != 'deleting':
                raise Refused('This job has a cleanup stage that needs recovery.')
            quarantine = quarantine_path(store.directory, stage)
            quarantine.mkdir(mode=0o700, parents=True, exist_ok=True)
            if quarantine.is_symlink() or quarantine.resolve() != quarantine:
                raise Refused('This job has an unsafe cleanup quarantine.')
            target = quarantine / (stage.name + '-' + uuid.uuid4().hex)
            rename_exclusive(stage, target)
            problems.record(store, 'cleanup', 'stage-quarantined', str(target), task=task, every=300)
        row = store.db.execute('SELECT active FROM idle_trims WHERE task=?', (task,)).fetchone()
        if row and row['active']:
            if store.db.in_transaction:
                store.db.execute('UPDATE idle_trims SET active=0 WHERE task=?', (task,))
            else:
                with store.transaction():
                    store.db.execute('UPDATE idle_trims SET active=0 WHERE task=?', (task,))
        yield
    finally:
        os.close(fd)


def spawn_guard(store, task):
    with scratch_guard(store, task):
        pass


def mark_worker(store, task, worker):
    close_saves.unpark(store, task)
    trimmed = store.db.execute('SELECT trimmed FROM idle_trims WHERE task=?', (task,)).fetchone()
    if trimmed and trimmed['trimmed']:
        store.db.execute('INSERT OR IGNORE INTO idle_trim_workers(worker) VALUES (?)', (worker,))
        store.db.execute('UPDATE idle_trims SET trimmed=0 WHERE task=?', (task,))


def _onerror(error):
    raise error


def _git(root, *args):
    result = subprocess.run(('git', '-C', str(root), *args), stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, timeout=30)
    if result.returncode != 0 and not (args[0] == 'check-ignore' and result.returncode == 1):
        raise OSError('Git check failed')
    return result


def protected_tree(root, protected=('.git',)):
    for base, dirs, names in os.walk(root, followlinks=False, onerror=_onerror):
        if any(name in dirs or name in names for name in protected):
            return True
        if 'HEAD' in names and 'objects' in dirs and 'refs' in dirs:
            return True
    return False


def dependency_candidates(root):
    if root.is_symlink() or not root.is_dir() or root.resolve() != root:
        raise ValueError('Unsafe job worktree')
    candidates = []
    for base, dirs, names in os.walk(root, followlinks=False, onerror=_onerror):
        for name in list(dirs):
            path = Path(base) / name
            if name == '.git' or stat.S_ISLNK(path.lstat().st_mode):
                dirs.remove(name)
            elif (path / '.git').exists():
                dirs.remove(name)
            elif name in NAMES:
                dirs.remove(name)
                relative = str(path.relative_to(root))
                ignored = _git(root, 'check-ignore', '-q', '--', relative)
                tracked = _git(root, 'ls-files', '-z', '--', relative)
                if ignored.returncode == 0 and not tracked.stdout and not protected_tree(path):
                    candidates.append(path)
    return candidates


def donor_links(roots, candidates):
    donors = set()
    for root in dict.fromkeys(roots):
        if not stat.S_ISDIR(root.lstat().st_mode):
            raise ValueError('Unsafe donor scan root')
        for base, dirs, names in os.walk(root, followlinks=False, onerror=_onerror):
            parent = Path(base)
            for name in dirs + names:
                path = parent / name
                if stat.S_ISLNK(path.lstat().st_mode):
                    resolved = path.resolve(strict=False)
                    donors.update(candidate for candidate in candidates
                                  if candidate not in path.parents and
                                  (resolved == candidate or candidate in resolved.parents))
            dirs[:] = [name for name in dirs if not stat.S_ISLNK((parent / name).lstat().st_mode)]
    return donors


def bounded_scan(store, function, *args):
    try:
        return scan_process.run(function, *args)
    except Exception as error:
        try:
            problems.record(store, 'cleanup', 'scan-failed', type(error).__name__, every=300)
        except Exception:
            logger.warning('cleanup scan failure could not be recorded', exc_info=True)
        raise scan_process.ScanError('cleanup scan failed') from error


def scratch_entries(root, refs):
    try:
        info = root.lstat()
    except FileNotFoundError:
        return []
    if not stat.S_ISDIR(info.st_mode) or root.resolve() != root:
        raise ValueError('Unsafe job scratch')
    selected = []
    for base, dirs, names in os.walk(root, followlinks=False, onerror=_onerror):
        parent = Path(base)
        for name in list(dirs):
            path = parent / name
            if stat.S_ISLNK(path.lstat().st_mode) or name in ('KEEP', 'rounds'):
                dirs.remove(name)
            elif name in SCRATCH_NAMES:
                dirs.remove(name)
                if (not scratch_referenced(path, root, refs)
                        and not protected_tree(path, ('.git', 'KEEP', 'rounds'))):
                    selected.append(path)
    return selected


def scan_candidates(scratch, refs, target):
    before = optional_signature(scratch), optional_signature(target)
    scratch_paths = scratch_entries(scratch, refs)
    try:
        target.lstat()
    except FileNotFoundError:
        candidates = []
    else:
        candidates = dependency_candidates(target)
    signatures = {str(path): path_signature(path) for path in scratch_paths + candidates}
    fingerprints = {str(path): tree_fingerprint(path) for path in scratch_paths + candidates}
    if before != (optional_signature(scratch), optional_signature(target)):
        raise ValueError('Idle trim roots changed during scan')
    return scratch_paths, candidates, signatures, fingerprints


def scratch_referenced(path, root, refs):
    relative = 'scratch/' + root.name + '/' + str(path.relative_to(root))
    return (close_saves.result_referenced(path, root, refs)
            or any(value.startswith(relative + '/') for value in refs.get(root.name[5:], set())))


def bounded_reference_values(store, refs, task_id):
    try:
        return scan_process.guard_reference_set(refs.get(str(task_id), ()))
    except scan_process.ScanError:
        try:
            problems.record(store, 'cleanup', 'scan-failed', 'reference set exceeds the cleanup input limit', every=300)
        except Exception:
            logger.warning('cleanup scan failure could not be recorded', exc_info=True)
        raise


def remove(path, counts):
    info = path.lstat()
    if stat.S_ISDIR(info.st_mode):
        asyncio.run(host_os.delete_tree(path, progress=counts, paced=True))
    else:
        path.unlink()
        counts['bytes'] += info.st_size
        counts['files'] += 1


def held(store, task_id):
    task = store.task_get(task_id)
    return bool(not task or close_saves.kept(store, task_id) or task['copy'] in ('held', 'saving')
                or store.db.execute('''SELECT 1 FROM wait_jobs j JOIN waits w ON w.id=j.wait
                                       WHERE j.task=? AND w.state='armed' LIMIT 1''', (task_id,)).fetchone())


def eligible(store, task_id, idle_since, hours):
    if store.get('idle_trim', 0) != hours or held(store, task_id):
        return False
    row = store.db.execute('''SELECT MAX(t.updated,COALESCE(MAX(w.updated),0)) AS idle_since
        FROM tasks t LEFT JOIN workers w ON w.task=t.id
        WHERE t.id=? AND t.status IN ('open','waiting') AND NOT EXISTS (
            SELECT 1 FROM workers a WHERE a.task=t.id AND
            a.status IN ('queued','waiting_for_secret','waiting_for_quota','running'))
        GROUP BY t.id''', (task_id,)).fetchone()
    return bool(row and row['idle_since'] == idle_since and idle_since < time.time() - hours * 3600)


def attempted(store, task_id, idle_since):
    row = store.db.execute('SELECT idle_since FROM idle_trims WHERE task=?', (task_id,)).fetchone()
    return row and row['idle_since'] == idle_since


def donor_roots(store, project):
    roots = [store.directory, project, *extension.active().extra_donor_roots()]
    for row in store.db.execute('SELECT DISTINCT task AS id FROM workers WHERE task IS NOT NULL'):
        roots.extend(worker_storage.lease_paths(store, row['id']))
    existing = []
    for root in dict.fromkeys(roots):
        try:
            root.lstat()
        except FileNotFoundError:
            continue
        existing.append(root)
    return existing


def run_job(store, task, idle_since, hours, refs, donor_scan):
    task_id = task['id']
    if not eligible(store, task_id, idle_since, hours) or attempted(store, task_id, idle_since):
        return
    scratch = close_saves.scratch(store, task_id)
    target = workspaces._workspace_target(store.directory, task_id)
    task_refs = {str(task_id): bounded_reference_values(store, refs, task_id)}
    scratch_paths, candidates, signatures, fingerprints = bounded_scan(store, scan_candidates, scratch, task_refs, target)
    topic = store.topic(task['topic'])
    project_cwd = Path(topic['cwd']).resolve()
    project_result = subprocess.run(('git', '-C', str(project_cwd), 'rev-parse', '--show-toplevel'),
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
    if project_result.returncode and b'not a git repository' not in project_result.stderr:
        raise OSError('Cannot find project root')
    project = (project_cwd if project_result.returncode else
               Path(os.fsdecode(project_result.stdout.strip())).resolve())
    all_paths = scratch_paths + candidates
    modules = [p for p in all_paths if p.name == 'node_modules']
    donors = set(modules) if donor_scan.get('failed') else set()
    if modules and not donor_scan.get('failed'):
        try:
            donors = bounded_scan(store, donor_links, donor_roots(store, project), modules)
        except Exception:
            donor_scan['failed'] = True
            raise
    fd = lock_file(store.directory, task_id)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        if not recover_stage(store, task_id):
            return
        with store.transaction():
            if not eligible(store, task_id, idle_since, hours) or attempted(store, task_id, idle_since):
                return
            store.db.execute("""INSERT INTO idle_trims(task,idle_since,active,trimmed) VALUES (?,?,1,0)
                ON CONFLICT(task) DO UPDATE SET idle_since=excluded.idle_since,active=1""", (task_id, idle_since))
        counts = {'bytes': 0, 'files': 0}
        completed = False
        try:
            for path in all_paths:
                if path in donors:
                    logger.info('idle trim skipped donor job=%s path=%s', task_id, path)
                    continue
                stage, record = prepare_stage(store, task_id, path, signatures[str(path)])
                try:
                    is_scratch = path in scratch_paths
                    live, stamp = close_saves.scan_task_references(store, task_id, refs)
                    is_referenced = is_scratch and bounded_scan(store, scratch_referenced, path, scratch, live)
                    with store.transaction():
                        if (not close_saves.references_current(store, stamp) or held(store, task_id)
                                or close_saves.pending_send(store, [path])
                                or is_referenced):
                            continue
                        stage_move(store, task_id, path, stage, record)
                    if bounded_scan(store, tree_fingerprint, stage / 'data') != fingerprints[str(path)]:
                        if not recover_stage(store, task_id):
                            break
                        with store.transaction():
                            store.db.execute('UPDATE idle_trims SET active=1 WHERE task=?', (task_id,))
                        continue
                    live, stamp = close_saves.scan_task_references(store, task_id, refs)
                    is_referenced = is_scratch and bounded_scan(store, scratch_referenced, path, scratch, live)
                    with store.transaction():
                        keep = (not close_saves.references_current(store, stamp)
                                or not eligible(store, task_id, idle_since, hours)
                                or close_saves.pending_send(store, [path])
                                or is_referenced)
                        if not keep:
                            store.db.execute('UPDATE idle_trims SET active=3,trimmed=1 WHERE task=?', (task_id,))
                    if keep:
                        if not recover_stage(store, task_id):
                            break
                    else:
                        delete_stage(stage, counts)
                    with store.transaction():
                        store.db.execute('UPDATE idle_trims SET active=1 WHERE task=?', (task_id,))
                finally:
                    if stage.exists() and not (stage / 'data').exists():
                        discard_stage(stage)
            completed = True
        finally:
            with store.transaction():
                if completed or counts['files']:
                    store.task_note(task_id, 'Idle trim at %s freed %d bytes of dependency, build and cache output.' % (
                        time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()), counts['bytes']))
                if not any(path.exists() for path in stage_paths(store.directory, task_id)):
                    store.db.execute('UPDATE idle_trims SET active=0 WHERE task=?', (task_id,))
    finally:
        os.close(fd)


def sweep(directory):
    store = Store(directory, token_file=Path(directory) / 'idle-trim-no-token', existing=True)
    try:
        store.db.execute('PRAGMA secure_delete=ON')
        recover_pending(store)
        hours = store.get('idle_trim', 0)
        if type(hours) is not int or hours < 1:
            return
        tasks = [task for task in stale_tasks(store, hours, ('open', 'waiting'))
                 if eligible(store, task['id'], task['idle_since'], hours)
                 and not attempted(store, task['id'], task['idle_since'])]
        if not tasks:
            return
        scanned = []
        sweeps.run(store, 'idle-trim', ['references'],
                   lambda _: scanned.append(close_saves.scan_references(store, strict=True)))
        if not scanned:
            return
        donor_scan = {}
        sweeps.run(store, 'idle-trim', tasks,
                   lambda task: run_job(store, task, task['idle_since'], hours, scanned[0], donor_scan),
                   key=lambda task: task['id'], refs=lambda task: {'task': task['id']})
    finally:
        store.close()
