"""Check pending work and notify the home chat when it exceeds a budget."""

import errno
import os
from pathlib import Path
import shutil
import subprocess
import time

from . import host_os, problems, records, sweeps, workspaces
from .store import Store


DAY = 86400
GIB = 1024 ** 3
CHECKS = (
    ('saving', 3600, "SELECT MIN(i.closed) FROM tasks t JOIN task_iterations i ON i.task=t.id AND i.k=t.iteration WHERE t.status IN ('done','dropped') AND t.copy='saving'"),
    ('record-rewrite', 600, 'SELECT MIN(since) FROM stale_records'),
    ('outbox', 900, 'SELECT MIN(created) FROM outbox WHERE delivered=0 AND retired=0'),
    ('worker-temp', 3600, "SELECT MIN(w.updated) FROM worker_temp_state s JOIN workers w ON w.id=s.worker WHERE s.pending!=0 AND w.status IN ('done','interrupted')"),
    ('held', DAY, "SELECT MIN(t.updated) FROM tasks t WHERE t.copy='held'"),
    ('sweep', DAY, 'SELECT MIN(since) FROM sweep_failures WHERE failed>?'),
    ('finish', 21600, "SELECT MIN(s.created) FROM tasks t JOIN task_stamps s ON s.task=t.id AND s.iteration=t.iteration AND s.stage=t.finish WHERE t.status='open'"),
)
PENDING_TEXT = {
    'saving': 'Finished jobs not saved after 1 h',
    'record-rewrite': 'Job records not updated after 10 min',
    'outbox': 'Messages not delivered after 15 min',
    'worker-temp': 'Finished workers with temporary files not cleared after 1 h',
    'held': 'Job records not saved for over a day',
    'sweep': 'Periodic cleanup has failed for over a day',
    'finish': 'Jobs reached their finish stage but are still open after 6 h',
}
PENDING_JOBS = {
    'saving': "SELECT t.id, MIN(i.closed) AS since FROM tasks t JOIN task_iterations i ON i.task=t.id AND i.k=t.iteration WHERE t.status IN ('done','dropped') AND t.copy='saving' GROUP BY t.id",
    'record-rewrite': 'SELECT task AS id, since FROM stale_records',
    'outbox': 'SELECT task AS id, MIN(created) AS since FROM outbox WHERE delivered=0 AND retired=0 AND task IS NOT NULL GROUP BY task',
    'worker-temp': "SELECT w.task AS id, MIN(w.updated) AS since FROM worker_temp_state s JOIN workers w ON w.id=s.worker WHERE s.pending!=0 AND w.status IN ('done','interrupted') AND w.task IS NOT NULL GROUP BY w.task",
    'held': "SELECT id, updated AS since FROM tasks WHERE copy='held'",
    'finish': "SELECT t.id, MIN(s.created) AS since FROM tasks t JOIN task_stamps s ON s.task=t.id AND s.iteration=t.iteration AND s.stage=t.finish WHERE t.status='open' GROUP BY t.id",
}


def recent(now):
    """A failing sweep item tries again at least once per sweeps.LIMIT; an older row belongs to a gone item."""
    return now - sweeps.LIMIT - 3600


def job_list(rows):
    labels = ['#%s (%s)' % (row['number'], row['name']) for row in rows[:3]]
    if len(rows) > 3:
        labels.append('and %d more' % (len(rows) - 3))
    return ', '.join(labels)


def pending_text(store, code, budget, now):
    text = PENDING_TEXT.get(code, code + ' has exceeded its time budget')
    if code == 'sweep':
        names = [row[0] for row in store.db.execute(
            'SELECT DISTINCT sweep FROM sweep_failures WHERE failed>? AND since<=? ORDER BY sweep', (recent(now), now - budget)).fetchall()]
        return text + (': ' + ', '.join(names) if names else '') + '.'
    query = PENDING_JOBS.get(code)
    if query:
        query = ('SELECT t.number, p.name FROM (' + query + ') pending '
                 'JOIN tasks t ON t.id=pending.id JOIN topics p ON p.id=t.topic '
                 'WHERE pending.since<=? ORDER BY pending.since, t.id')
        labels = job_list(store.db.execute(query, (now - budget,)).fetchall())
        if labels:
            return text + ': ' + labels
    return text + '.'


def notice(store, code, text, now):
    topic = store.get('coordinator_home_topic')
    if not topic or not store.topic(topic):
        return False
    key = 'invariant_alarm:' + code
    with store.transaction():
        previous = store.get(key)
        if previous is not None and now - previous < DAY:
            return False
        if problems.record(store, 'invariants', code, text, every=DAY) is None:
            return False
        store.enqueue_report(topic, 'Torii alert: ' + text)
        store.put(key, now)
    return True


def pending_since(store, key, active, now):
    setting = 'invariant_pending:' + key
    previous = store.get(setting)
    if not active:
        if previous is not None:
            with store.transaction():
                store.put(setting, None)
        return None
    if previous is None:
        with store.transaction():
            store.put(setting, now)
        return now
    return previous


def large_tree(path, limit):
    total = 0
    if path.is_symlink():
        return False
    def failed(error):
        raise error
    for root, directories, files in os.walk(path, followlinks=False, onerror=failed):
        directories[:] = [name for name in directories if not (Path(root) / name).is_symlink()]
        for name in files:
            item = Path(root) / name
            try:
                if not item.is_symlink():
                    total += item.stat().st_size
            except OSError:
                raise
            if total > limit:
                return True
    return False


def check_pending(store, now):
    sweeps.run(store, 'check', CHECKS, lambda check: pending_check(store, now, *check), area='invariants',
               key=lambda check: check[0])


def pending_check(store, now, code, budget, query):
    oldest = store.db.execute(query, (recent(now),) if code == 'sweep' else ()).fetchone()[0]
    if oldest is not None and oldest <= now - budget:
        notice(store, code, pending_text(store, code, budget, now), now)


def check_git(store, now):
    root = store.directory / 'records'
    if not (root / '.git').is_dir():
        return
    result = subprocess.run(['git', '-C', str(root), 'status', '--porcelain', '--untracked-files=all', '--', *records.TRACKED],
                            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=30, check=True)
    since = pending_since(store, 'uncommitted', bool(result.stdout), now)
    if since is not None and since <= now - 3600:
        notice(store, 'uncommitted', 'Job records have remained uncommitted for over one hour.', now)
    status = store.get('records_sync_status', {}) or {}
    since = pending_since(store, 'unpushed', bool(store.get('records_sync', False) and status.get('ahead')), now)
    if since is not None and since <= now - DAY:
        notice(store, 'unpushed', 'Job records have remained unpushed for over one day.', now)


def check_disk(store, now):
    usage = shutil.disk_usage(store.directory)
    if usage.free / usage.total < 0.15:
        notice(store, 'disk', 'Free disk space is below 15 percent.', now)
    if workspaces.volume_mounted(store.directory):
        usage = shutil.disk_usage(workspaces.cow_root(store.directory))
        if usage.free / usage.total < 0.15:
            notice(store, 'cow-disk', 'Free space on the copy-on-write worktree volume is below 15 percent.', now)


def check_trash(store, now):
    if large_tree(host_os.trash_directory(), 100 * GIB):
        notice(store, 'trash', 'Trash exceeds 100 GiB.', now)


def check_idle(store, now):
    rows = store.db.execute("""SELECT t.* FROM tasks t WHERE t.status IN ('open','waiting','interrupted')
        AND t.updated<=? AND NOT EXISTS (SELECT 1 FROM workers w WHERE w.task=t.id AND w.updated>?)
        AND NOT EXISTS (SELECT 1 FROM workers w WHERE w.task=t.id AND w.status IN
        ('queued','running','waiting_for_secret','waiting_for_quota')) ORDER BY t.updated, t.id""", (now - DAY, now - DAY)).fetchall()
    large = []
    for row in rows:
        task = dict(row)
        name = 'task-' + str(task['id'])
        paths = [store.directory / 'scratch' / name, store.directory / 'results' / name,
                 Path(task.get('worktree') or store.directory / 'worktrees' / name)]
        errors = []
        sizes = [records.storage_bytes(path, errors) if os.path.lexists(path) else 0 for path in paths]
        if any(size is None for size in sizes):
            detail = '; '.join('%s (%s)' % (error.filename, errno.errorcode.get(error.errno, type(error).__name__))
                               for error in errors)
            problems.record(store, 'invariants', 'idle-size-failed', 'Job storage could not be measured: ' + detail,
                            task=task['id'], every=3600)
            continue
        if sum(sizes) > 5 * GIB:
            topic = store.topic(task['topic'])
            large.append({'number': task['number'], 'name': '%s, %.1f GiB' % (topic['name'], sum(sizes) / GIB)})
    if large:
        notice(store, 'idle-large', 'Idle open jobs over 5 GiB: ' + job_list(large), now)


def check_storage(store, now):
    """Disk space each run; Trash size and idle job sizes at most once an hour."""
    hourly = now - (store.get('invariant_storage_checked', 0) or 0) >= 3600
    sweeps.run(store, 'check', [('disk', check_disk)] + ([('trash', check_trash), ('idle', check_idle)] if hourly else []),
               lambda check: check[1](store, now), area='invariants', key=lambda check: check[0])
    if hourly:
        with store.transaction():
            store.put('invariant_storage_checked', now)


def run(directory):
    store = Store(Path(directory), existing=True)
    try:
        now = time.time()
        check_pending(store, now)
        sweeps.run(store, 'check', ['git'], lambda name: check_git(store, now), area='invariants')
        check_storage(store, now)
    finally:
        store.close()
