"""Warn about large open jobs, park open jobs that stayed idle, and trim stale or oversized job folders."""

from itertools import chain
import logging
import os
from pathlib import Path
import re
import shutil
import sqlite3
import stat
import subprocess
import time
import uuid

from . import close_saves, host_os, idle_trim, problems, scan_process, stray_report, sweeps, workspaces
from .control_api import Refused, job_number, results_size
from .host_os import installed_systemd_references, open_paths, path_case_key, systemd_references
from .store import Store

logger = logging.getLogger(__name__)
LIMIT = 10 * 1024 ** 3
OPEN = ('open', 'waiting', 'interrupted')
WORKER_IDLE = 2 * 3600
SCRATCH_STALE = close_saves.PARK_SECONDS
SCRATCH_CAP = LIMIT
RESULTS_CAP = close_saves.RESULTS_LARGE
CAP_AGE = 12 * 3600
LOW_DISK = 0.10
LOW_DISK_PARK = WORKER_IDLE
TRIM_SCAN_TIMEOUT = 300
KEPT_NAMES = close_saves.RECORDS | {'KEEP', 'rounds', 'HANDOFF.md', 'results-fade'}


def child_commands(pid):
    if not pid:
        return []
    try:
        result = subprocess.run(['ps', '-A', '-o', 'ppid=,command='], capture_output=True,
                                text=True, timeout=5, check=True)
    except (OSError, subprocess.SubprocessError):
        return []
    commands = []
    for line in result.stdout.splitlines():
        fields = line.strip().split(None, 1)
        if len(fields) != 2 or fields[0] != str(pid):
            continue
        command = ' '.join(fields[1].split())
        match = re.search(r'\beval\s+[\'\"]', command)
        if match and ('shell-snapshot' in command or 'shell_snapshot' in command):
            command = command[match.start():]
        commands.append(command if len(command) <= 150 else command[:149] + '…')
        if len(commands) == 3:
            break
    return commands


def idle_workers(store, now):
    count = 0
    with store.transaction():
        previous = store.get('worker_idle_alerts', {})
        current = {}
        rows = store.db.execute('''SELECT w.*,t.number,t.iteration,t.title,t.status AS task_status
            FROM workers w JOIN tasks t ON t.id=w.task WHERE w.status='running' ''').fetchall()
        for row in rows:
            key = str(row['id'])
            progress = row['last_seq']
            prior = previous.get(key, {})
            old_progress = prior.get('progress')
            if isinstance(old_progress, list):
                old_progress = old_progress[0] if old_progress else None
            waiting = row['task_status'] == 'waiting' or store.db.execute('''SELECT 1 FROM wait_jobs j
                JOIN waits w ON w.id=j.wait WHERE j.task=? AND w.state='armed' LIMIT 1''',
                (row['task'],)).fetchone()
            since = (prior.get('since', now) if old_progress == progress else
                     row['created'] if old_progress is None and progress == 0 else now)
            if waiting:
                since = now
            last_alert = prior.get('alert', 0) if old_progress == progress else 0
            current[key] = {'progress': progress, 'since': since, 'alert': last_alert}
            topic = store.topic(row['topic'])
            if (waiting or now - since < WORKER_IDLE or now - last_alert < WORKER_IDLE or
                    not topic or not topic['enabled'] or store.get('topic_lifecycle:' + row['topic'])):
                continue
            minutes = int((now - since) // 60)
            text = 'Worker %s, job %s "%s", has had no progress for %dh %dm.' % (
                row['id'], job_number(row['number'], row['iteration']), row['title'], minutes // 60, minutes % 60)
            commands = child_commands(row['pid'])
            if commands:
                text += '\nChild processes:\n' + '\n'.join('- ' + command for command in commands)
            text += '\nIf it waits on a loop that cannot end, steer it or stop that loop.'
            store.message_event(row['topic'], text)
            current[key]['alert'] = now
            count += 1
        store.put('worker_idle_alerts', current)
    return count


def usage(label, root):
    total, newest, parts, seen = 0, None, {}, set()
    try:
        info = root.lstat()
    except FileNotFoundError:
        return total, newest, parts
    if not stat.S_ISDIR(info.st_mode):
        raise ValueError('Job folder is not a directory')
    newest = info.st_mtime
    stack = [(root, None)]
    while stack:
        folder, part = stack.pop()
        try:
            entries = list(os.scandir(folder))
        except FileNotFoundError:
            continue
        for entry in entries:
            try:
                info = entry.stat(follow_symlinks=False)
            except FileNotFoundError:
                continue
            name = part or label + '/' + entry.name
            newest = max(newest, info.st_mtime)
            if (info.st_dev, info.st_ino) not in seen:
                seen.add((info.st_dev, info.st_ino))
                total += info.st_blocks * 512
                parts[name] = parts.get(name, 0) + info.st_blocks * 512
            if stat.S_ISDIR(info.st_mode):
                stack.append((entry.path, name))
    return total, newest, parts


def gigabytes(size):
    return '%.1f GB' % (size / 1024 ** 3)


def alarm(store, task, size, parts):
    row = store.db.execute('SELECT base FROM job_size_alarms WHERE task=?', (task['id'],)).fetchone()
    if row and size < row['base']:
        with store.transaction():
            store.db.execute('UPDATE job_size_alarms SET base=? WHERE task=?', (size, task['id']))
        return False
    if size < (row['base'] + LIMIT if row else LIMIT):
        return False
    topic = store.topic(task['topic']) or {}
    biggest = ', '.join('%s %s' % (name, gigabytes(part)) for name, part in
                        sorted(parts.items(), key=lambda item: (-item[1], item[0]))[:3])
    text = 'Storage alarm: job %s "%s" in %s uses %s in scratch and worktree. Biggest: %s.' % (
        job_number(task['number'], task['iteration']), task['title'], topic.get('name') or task['topic'],
        gigabytes(size), biggest)
    home = store.get('coordinator_home_topic')
    with store.transaction():
        store.db.execute('INSERT INTO job_size_alarms(task,base) VALUES (?,?) ON CONFLICT(task) DO UPDATE SET base=excluded.base',
                         (task['id'], size))
        if not store.get('topic_lifecycle:' + task['topic']):
            store.message_event(task['topic'], text)
        if home and store.topic(home):
            store.enqueue_report(home, text)
    logger.info('job size alarm job=%s bytes=%s', task['id'], size)
    return True


def scan_usage(scratch, worktree, results):
    return usage('scratch', scratch), usage('worktree', worktree), usage('results', results)


def note_results(store, task, size, now):
    with store.transaction():
        before = results_size(store, task)
        store.db.execute('''INSERT INTO job_results_sizes(task,bytes,measured) VALUES (?,?,?)
            ON CONFLICT(task) DO UPDATE SET bytes=excluded.bytes,measured=excluded.measured''', (task['id'], size, now))
        if results_size(store, task) != before:
            store.record_mark_stale(task['id'])


def bounded_scan(store, function, *args):
    try:
        return scan_process.run(function, *args)
    except Exception as error:
        try:
            problems.record(store, 'cleanup', 'scan-failed', type(error).__name__, every=300)
        except Exception:
            logger.warning('cleanup scan failure could not be recorded', exc_info=True)
        raise scan_process.ScanError('cleanup scan failed') from error


def low_disk(store):
    usage = shutil.disk_usage(store.directory)
    return usage.free / usage.total < LOW_DISK


def mention_pattern(paths):
    alternatives = '|'.join(re.escape(str(path)) for path in paths)
    boundary = r'(?:(?<![\w./-])|(?<=[\s=+!:@|]-))'
    return re.compile(boundary + r'''(?:%s)(?=$|[/\s'";,):])''' % alternatives)


def caller_pids(commands):
    parents = {pid: ppid for _, pid, ppid, _ in commands}
    callers, pid = set(), str(os.getpid())
    while pid != '0' and pid not in callers:
        callers.add(pid)
        pid = parents.get(pid, '0')
    return callers


def path_users(root, jobs, *, per_entry=False, failures=None, installed_units=False, launch_agents=(),
               skip_caller_commands=False):
    """Return one reason to keep each job whose folders are in use."""
    used = {}
    probe = 'process'
    try:
        folders = {str(path): task for task, paths in jobs.items() for path in paths}
        commands = host_os.process_commands(include_linux=skip_caller_commands)
        skip_commands = caller_pids(commands) if skip_caller_commands else ()
        if not per_entry:
            open_folders = {path_case_key(path): task for path, task in folders.items()}
            for path, pid in open_paths(root, owners=True, skip_commands=skip_commands).items():
                opened = Path(path_case_key(path))
                for parent in (opened, *opened.parents):
                    if str(parent) in open_folders:
                        used.setdefault(open_folders[str(parent)], 'process pid ' + str(pid))
        pattern = mention_pattern(folders)
        for uid, pid, _, command in host_os.process_command_references(commands):
            if uid == str(os.getuid()) and pid not in skip_commands:
                for match in pattern.finditer(command):
                    used.setdefault(folders[match[0]], 'process pid ' + pid)
        probe = 'systemd'
        units = systemd_references(fallback_on_error=installed_units)
        if installed_units:
            units = chain(units, installed_systemd_references())
        for unit, content in units:
            for match in pattern.finditer(content.replace(r'\x20', ' ').replace(r'\ ', ' ')):
                used.setdefault(folders[match[0]], 'systemd user unit ' + unit)
        probe = 'LaunchAgent'
        for plist, values in stray_report.launch_references(launch_agents):
            for value in values:
                for match in pattern.finditer(value):
                    used.setdefault(folders[match[0]], 'LaunchAgent ' + plist.name)
        return used
    except Exception as error:
        reason = path_scan_reason(probe, error)
        if failures is not None:
            failures.append(reason)
        return {task: used.get(task, reason) for task in jobs}


def path_scan_reason(probe, error):
    return '%s path use scan failed (%s)' % (probe, type(error).__name__)


def report_path_scan_failure(store, reason):
    problems.record(store, 'cleanup', 'path-use-scan-failed', reason, every=3600)


def kept_name(name):
    return name in KEPT_NAMES or name.startswith('results-expiry-')


def scan_entry(path):
    """Return the bytes, the newest mtime or ctime, and whether the entry contains protected data."""
    info = path.lstat()
    size, newest, protected = info.st_blocks * 512, max(info.st_mtime, info.st_ctime), False
    stack = [path] if stat.S_ISDIR(info.st_mode) else []
    while stack:
        with os.scandir(stack.pop()) as entries:
            names = set()
            for entry in entries:
                item = entry.stat(follow_symlinks=False)
                size += item.st_blocks * 512
                newest = max(newest, item.st_mtime, item.st_ctime)
                names.add(entry.name)
                if kept_name(entry.name) or entry.name.startswith('report') and stat.S_ISREG(item.st_mode):
                    protected = True
                if stat.S_ISDIR(item.st_mode):
                    stack.append(entry.path)
        protected = protected or '.git' in names or {'HEAD', 'objects', 'refs'} <= names
    return size, newest, protected


def referenced(path, root, refs):
    relative = root.parent.name + '/' + root.name + '/' + str(path.relative_to(root))
    return (close_saves.result_referenced(path, root, refs)
            or any(value.startswith(relative + '/') for value in refs.get(root.name[5:], ())))


def trim_plan(root, refs, opened, now, stale, cap, floor):
    """Choose entries of one job folder to move to the Trash, oldest first: each entry untouched for stale seconds,
    then, while the folder exceeds cap, entries untouched for floor seconds. Records, reports, KEEP, rounds/, git
    bundles, Git trees, referenced paths and paths that a process has open stay."""
    try:
        info = root.lstat()
    except FileNotFoundError:
        return []
    if not stat.S_ISDIR(info.st_mode) or root.resolve() != root:
        raise ValueError('Unsafe job folder')
    if str(root) in opened:
        return []
    scratch = root.parent.name == 'scratch'
    if scratch and os.path.lexists(root / '.git'):
        return []
    entries = sorted(root.iterdir()) if scratch else close_saves.expiring_entries(root)
    busy = {str(folder) for path in map(Path, opened) for folder in (path, *path.parents)}
    items = []
    for path in entries:
        size, newest, protected = scan_entry(path)
        relative = path.relative_to(root)
        if (protected or any(kept_name(part) for part in relative.parts) or path.is_symlink()
                or path.name.startswith('report') and path.is_file() or referenced(path, root, refs)
                or str(path) in busy):
            continue
        items.append((newest, str(path), size, idle_trim.path_signature(path)))
    chosen = []
    total = sum(item[2] for item in items)
    for newest, path, size, signature in sorted(items):
        if newest < now - stale or total > cap and newest < now - floor:
            chosen.append((path, signature, size, newest))
            total -= size
    return chosen


def manifest_refs(store, task):
    cwd = Path(store.topic(task['topic'])['cwd'])
    result = subprocess.run(('git', '-C', str(cwd), 'rev-parse', '--show-toplevel'), capture_output=True, timeout=30)
    project = Path(os.fsdecode(result.stdout.strip())) if not result.returncode else cwd
    try:
        return close_saves.result_reference_index([(project / 'KEEP' / 'MANIFEST.md').read_text(errors='replace')])
    except FileNotFoundError:
        return {}


def trimmable(store, task_id):
    task = store.task_get(task_id)
    return bool(task and task['status'] in OPEN and task['copy'] != 'saving' and not close_saves.parked(store, task)
                and not close_saves.kept(store, task_id)
                and not store.db.execute("""SELECT 1 FROM workers WHERE task=?
                    AND status IN ('queued','waiting_for_secret','waiting_for_quota','running') LIMIT 1""",
                                         (task_id,)).fetchone()
                and not store.db.execute('SELECT 1 FROM keep_queue WHERE task=? LIMIT 1', (task_id,)).fetchone())


def folder_limits(kind):
    return (SCRATCH_STALE, SCRATCH_CAP) if kind == 'scratch' else (float('inf'), RESULTS_CAP)


def unchanged(path, signature, newest, device):
    """The entry is as the plan saw it and on the file system of its job folder, so a rename moves it whole."""
    try:
        return (path.lstat().st_dev == device and idle_trim.optional_signature(path) == signature
                and scan_entry(path)[1] == newest)
    except OSError:
        return False


def move_out(store, task, root, chosen, stamp):
    """Move the chosen entries into one Torii Trash folder while the job state and its references are unchanged.
    An entry stays when anything in it was written after the plan."""
    trash = host_os.trash_directory()
    trash.mkdir(mode=0o700, parents=True, exist_ok=True)
    device = root.stat().st_dev
    if trash.stat().st_dev != device:
        raise OSError('Trash is on another filesystem')
    folder = trash / ('%s%d-%s-%s' % (host_os.TRIM_TRASH_PREFIX, task['id'], root.parent.name, uuid.uuid4().hex[:8]))
    moved, size = [], 0
    with store.transaction():
        if not trimmable(store, task['id']) or not close_saves.references_current(store, stamp):
            return None
        if close_saves.pending_send(store, [Path(path) for path, _, _, _ in chosen]):
            return None
        folder.mkdir(mode=0o700)
        for path, signature, bytes_, newest in chosen:
            path = Path(path)
            if not unchanged(path, signature, newest, device):
                continue
            target = folder / path.relative_to(root)
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            os.rename(path, target)
            moved.append(str(path.relative_to(root)))
            size += bytes_
        if moved:
            store.task_note(task['id'], 'Storage trim moved %d entries of %s (%s) to %s: %s' % (
                len(moved), root.parent.name, gigabytes(size), folder, ', '.join(moved[:10])
                + (' and %d more' % (len(moved) - 10) if len(moved) > 10 else '')))
    if not moved:
        shutil.rmtree(folder)
        return None
    return size


def trim_open(store, now, protected=()):
    """Move stale scratch entries and oversized results of open jobs to the Trash. A cheap scan without references
    finds the jobs to check; only those pay for the reference scan and the process probe."""
    kinds = ('scratch', 'results')
    marks = ','.join('?' for _ in OPEN)
    folders = [(row['id'], kind, store.directory / kind / ('task-' + str(row['id'])))
               for row in store.db.execute('SELECT id FROM tasks WHERE status IN (%s) ORDER BY id' % marks, OPEN).fetchall()
               if row['id'] not in protected and trimmable(store, row['id']) for kind in kinds]
    due = []
    sweeps.run(store, 'open-trim-check', folders, lambda folder: check_folder(now, due, folder),
               key=trim_key, refs=lambda folder: {'task': folder[0]})
    if not due:
        return 0
    jobs = {task_id: set(close_saves.park_folders(store, task_id)) for task_id, _, _ in due}
    for task_id, _, root in due:
        jobs[task_id].add(root)
    failures = []
    used = path_users(store.directory, jobs, per_entry=True, failures=failures)
    if failures:
        report_path_scan_failure(store, failures[0])
    due = [entry for entry in due if entry[0] not in used]
    if not due:
        return 0
    references = close_saves.scan_references(store, strict=True)
    freed = []
    sweeps.run(store, 'open-trim', due, lambda folder: freed.append(trim_folder(store, now, references, *folder)),
               key=trim_key, refs=lambda folder: {'task': folder[0]})
    return sum(freed)


def trim_key(folder):
    return '%s/%s' % folder[:2]


def check_folder(now, due, folder):
    if scan_process.run(trim_plan, folder[2], {}, [], now, *folder_limits(folder[1]), CAP_AGE, timeout=TRIM_SCAN_TIMEOUT):
        due.append(folder)


def trim_folder(store, now, references, task_id, kind, root):
    try:
        with idle_trim.scratch_guard(store, task_id):
            if not trimmable(store, task_id):
                return 0
            task = store.task_get(task_id)
            live, stamp = close_saves.scan_task_references(store, task_id, references)
            refs = {str(task_id): set(live[str(task_id)]) | manifest_refs(store, task).get(str(task_id), set())}
            try:
                opened = open_paths(store.directory)
            except Exception as error:
                report_path_scan_failure(store, path_scan_reason('process', error))
                return 0
            chosen = scan_process.run(trim_plan, root, refs, opened, now,
                                      *folder_limits(kind), CAP_AGE, timeout=TRIM_SCAN_TIMEOUT)
            if not chosen:
                return 0
            return move_out(store, task, root, chosen, stamp) or 0
    except Refused:
        return 0


def measure(store, task, now):
    (size, _, parts), (more, _, other), (results, _, _) = bounded_scan(
        store, scan_usage, close_saves.scratch(store, task['id']),
        workspaces._workspace_target(store.directory, task['id']),
        store.directory / 'results' / ('task-' + str(task['id'])))
    alarms = alarm(store, task, size + more, dict(parts, **other))
    note_results(store, task, results, now)
    return alarms


def sweep(directory, now=None):
    now = time.time() if now is None else now
    store = Store(Path(directory), token_file=Path(directory) / 'job-limits-no-token', existing=True)
    alarms = 0
    try:
        idle_trim.recover_pending(store)
        alarms += idle_workers(store, now)
        marks = ','.join('?' for _ in OPEN)
        counted = []
        sweeps.run(store, 'job-size', [dict(row) for row in store.db.execute(
            'SELECT * FROM tasks WHERE status IN (%s) ORDER BY id' % marks, OPEN)],
            lambda task: counted.append(measure(store, task, now)),
            key=lambda task: task['id'], refs=lambda task: {'task': task['id']})
        alarms += sum(counted)
        low = low_disk(store)
        if low:
            problems.record(store, 'cleanup', 'low-disk', 'Free space on the state volume is below %d percent: idle jobs '
                            'park after %d hours and Torii Trash entries older than %d hours expire.' % (LOW_DISK * 100, LOW_DISK_PARK // 3600, host_os.TRASH_SECONDS // 3600),
                            every=3600)
        idle = LOW_DISK_PARK if low else close_saves.PARK_SECONDS
        protected = set()
        for task_id in close_saves.park_idle(store, now, idle, protected):
            logger.info('job parked after %d idle hours job=%s', idle // 3600, task_id)
        try:
            freed = trim_open(store, now, protected)
            if freed:
                logger.info('open job trim moved bytes=%d to the Trash', freed)
        except (OSError, ValueError, sqlite3.Error, scan_process.ScanError) as error:
            problems.record(store, 'cleanup', 'open-trim-failed', '%s: %s' % (type(error).__name__, error), every=3600)
        if low:
            try:
                logger.info('low disk Trash expiry freed bytes=%d', host_os.expire_trash(now))
                volume_trash = workspaces.trash_for(store.directory, workspaces.cow_root(store.directory))
                if workspaces.volume_mounted(store.directory) and volume_trash.is_dir():
                    logger.info('low disk volume Trash expiry freed bytes=%d', host_os.expire_trash(now, volume_trash))
            except OSError as error:
                problems.record(store, 'cleanup', 'trash-expiry-failed', '%s: %s' % (type(error).__name__, error), every=3600)
        for table in ('job_size_alarms', 'job_results_sizes'):
            stale = 'FROM %s WHERE task NOT IN (SELECT id FROM tasks WHERE status IN (%s))' % (table, marks)
            if store.db.execute('SELECT 1 ' + stale + ' LIMIT 1', OPEN).fetchone():
                with store.transaction():
                    store.db.execute('DELETE ' + stale, OPEN)
    finally:
        store.close()
    return alarms
