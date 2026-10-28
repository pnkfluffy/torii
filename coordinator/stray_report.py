"""Default-off metadata reports for folders outside active ownership."""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import plistlib
from pwd import getpwuid
import re
import stat
import subprocess
import threading
import time

from .close_saves import scratch
from .control_api import Refused, Result
from .worker_storage import FINAL, results_directory
from .workspaces import _workspace_target


MAX_ENTRIES = 5000000
SCAN_SECONDS = 1800
PROTECTED = frozenset(('.claude-accounts', '.claude', '.codex', '.codex-accounts',
                       '.grok-accounts', '.grok', '.opencode-accounts'))
TASK = re.compile(r'(?:^|[^a-z0-9])(?:task|job)[-_]?(\d+)(?=$|[^0-9])', re.I)
PATH_FIELDS = ('WorkingDirectory', 'Program', 'StandardOutPath', 'StandardErrorPath')


def configuration(store):
    homes = dict.fromkeys((Path.home(), Path(getpwuid(os.getuid()).pw_dir)))
    return dict(state_root=str(store.directory.parent),
                launch_agents=[str(home / 'Library' / 'LaunchAgents') for home in homes]
                + ['/Library/LaunchAgents', '/Library/LaunchDaemons'],
                service_paths=[], results_min_bytes=1024 ** 3, growth_bytes=2 * 1024 ** 3,
                keep=7) | (store.get('stray_report_config') or {})


def safe_path(value):
    path = Path(value)
    return path.is_absolute() and '..' not in path.parts and not any(part.casefold() in PROTECTED for part in path.parts)


def open_directory(path):
    if not safe_path(str(path)):
        raise ValueError('Protected scan root')
    fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in Path(path).parts[1:]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


def next_night(timestamp):
    current = datetime.fromtimestamp(timestamp)
    night = current.replace(hour=3, minute=0, second=0, microsecond=0)
    if night <= current:
        night += timedelta(days=1)
    return night.timestamp()


def op_report(store, ctx, enabled=None, config=None, run=False):
    current = configuration(store)
    if config is not None:
        try:
            update = json.loads(config)
        except (ValueError, TypeError):
            raise Refused('config must be a JSON object.')
        if not isinstance(update, dict) or update.keys() - current.keys():
            raise Refused('config contains unknown settings.')
        current.update(update)
        if not isinstance(current['state_root'], str) or not safe_path(current['state_root']):
            raise Refused('state_root must be an absolute path outside agent homes.')
        for key in ('launch_agents', 'service_paths'):
            if not isinstance(current[key], list) or any(not isinstance(p, str) or not safe_path(p) for p in current[key]):
                raise Refused(key + ' must contain absolute paths outside agent homes.')
        for key in ('results_min_bytes', 'growth_bytes', 'keep'):
            if type(current[key]) is not int or current[key] < (1 if key == 'keep' else 0):
                raise Refused(key + ' must be a nonnegative integer; keep must be positive.')
        if current['keep'] > 31:
            raise Refused('keep must be at most 31.')
    active = store.get('stray_report_enabled', False) is True if enabled is None else enabled
    if run and not active:
        raise Refused('Stray reports are off. Enable them before requesting a scan.')
    with store.transaction():
        if config is not None:
            store.put('stray_report_config', current)
        if enabled is not None:
            store.put('stray_report_enabled', enabled)
        if run or enabled is not None or config is not None:
            store.put('stray_report_requested', active)
            store.service_request('_cleanup.stray_report', {})
    history = store.get('stray_report_history', [])
    data = dict(enabled=active, config=current, latest=history[-1] if history else None)
    return Result(True, 'Stray reports are ' + ('on.' if active else 'off.') + ' Report only.', data)


@dataclass(frozen=True)
class Ownership:
    trees: tuple
    references: tuple
    tasks: dict
    active: frozenset


def ownership(store, settings):
    tasks = {task['id']: task for task in store.tasks_list()}
    active = frozenset(task['id'] for task in tasks.values() if task['status'] not in ('done', 'dropped'))
    trees = [Path(topic['cwd']) for topic in store.topics() if topic['cwd']] + list(map(Path, settings['service_paths']))
    references = [store.directory]
    for task_id in active:
        task = tasks[task_id]
        trees.extend((_workspace_target(store.directory, task_id), scratch(store, task_id),
                      Path(results_directory(store, task_id))))
        if task['worktree']:
            trees.append(Path(task['worktree']))
    for worker in store.db.execute('SELECT task,cwd,status FROM workers'):
        if worker['status'] not in FINAL:
            if worker['task'] is not None:
                active = active | {worker['task']}
                trees.append(Path(results_directory(store, worker['task'])))
            if worker['cwd']:
                trees.append(Path(worker['cwd']))
    references.extend(Path(row['image']) for row in store.db.execute(
        'SELECT image FROM outbox WHERE delivered=0 AND retired=0 AND image IS NOT NULL'))
    return Ownership(tuple(trees), tuple(references), tasks, active)


def process_paths():
    result = subprocess.run(['/usr/sbin/lsof', '-nP', '-a', '-u', str(os.getuid()), '-Fn'],
                            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                            env={'PATH': '/usr/bin:/bin:/usr/sbin:/sbin'}, text=True, timeout=30)
    if result.returncode:
        raise OSError('Process path probe failed')
    return [Path(line[1:]) for line in result.stdout.splitlines() if line.startswith('n/')]


def launch_paths(folders):
    paths = []
    for folder in map(Path, folders):
        try:
            descriptor = open_directory(folder)
        except FileNotFoundError:
            continue
        try:
            with os.scandir(descriptor) as entries:
                for entry in entries:
                    if not entry.name.endswith('.plist') or not entry.is_file(follow_symlinks=False):
                        continue
                    fd = os.open(entry.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=descriptor)
                    with os.fdopen(fd, 'rb') as handle:
                        data = handle.read(1024 * 1024 + 1)
                        if len(data) > 1024 * 1024:
                            raise ValueError('LaunchAgent metadata exceeds limit')
                        value = plistlib.loads(data)
                    if not isinstance(value, dict) or not isinstance(value.get('ProgramArguments', []), list):
                        raise ValueError('Invalid LaunchAgent path metadata')
                    values = [value.get(key) for key in PATH_FIELDS]
                    values.extend(value.get('ProgramArguments', []))
                    paths.extend(Path(v) for v in values if isinstance(v, str) and v.startswith('/'))
        finally:
            os.close(descriptor)
    return paths


def contains(parent, child):
    return parent == child or parent in child.parents


def origin(path, owners):
    match = TASK.search(path.name)
    task_id = int(match[1]) if match else None
    task = owners.tasks.get(task_id)
    return dict(task=task_id, topic=task['topic'] if task else None,
                chat=task.get('chat') if task else None, guessed=task_id is not None)


class ScanLimit(Exception):
    pass


class Metadata:
    def __init__(self, cancelled):
        self.cancelled = cancelled
        self.deadline = time.monotonic() + SCAN_SECONDS
        self.remaining = MAX_ENTRIES

    def check(self):
        self.remaining -= 1
        if self.remaining < 0 or time.monotonic() >= self.deadline or self.cancelled.is_set():
            raise ScanLimit('Metadata scan limit reached')

    def measure(self, path):
        total, newest, excluded = 0, None, 0
        seen = set()

        def visit(fd, depth):
            nonlocal total, newest, excluded
            if depth > 64:
                raise ScanLimit('Metadata depth limit reached')
            with os.scandir(fd) as entries:
                for entry in entries:
                    self.check()
                    if entry.name.casefold() in PROTECTED:
                        excluded += 1
                        continue
                    info = entry.stat(follow_symlinks=False)
                    identity = (info.st_dev, info.st_ino)
                    if identity in seen:
                        continue
                    seen.add(identity)
                    if stat.S_ISDIR(info.st_mode):
                        child = os.open(entry.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                        try:
                            opened = os.fstat(child)
                            if (opened.st_dev, opened.st_ino) != identity:
                                raise OSError('Directory changed during metadata scan')
                            if opened.st_dev != device:
                                raise OSError('Metadata scan crosses filesystems')
                            total += opened.st_blocks * 512
                            visit(child, depth + 1)
                        finally:
                            os.close(child)
                    else:
                        total += info.st_blocks * 512
                        if stat.S_ISREG(info.st_mode):
                            newest = info.st_mtime if newest is None else max(newest, info.st_mtime)
        fd = open_directory(path)
        try:
            info = os.fstat(fd)
            device = info.st_dev
            total = info.st_blocks * 512
            seen.add((info.st_dev, info.st_ino))
            visit(fd, 0)
        finally:
            os.close(fd)
        return total, newest, excluded


def scan(settings, owners, results, cancelled):
    report = dict(created=time.time(), entries=[], totals=dict(state=0, results=0), errors=[], excluded=0, complete=True)
    references = list(owners.references)
    for label, probe in (('process paths', process_paths), ('LaunchAgent paths', lambda: launch_paths(settings['launch_agents']))):
        try:
            references.extend(probe())
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            report['errors'].append(label + ': ' + type(error).__name__)
    metadata = Metadata(cancelled)
    for category, root in (('state', Path(settings['state_root'])), ('results', results)):
        if category == 'state' and contains(results.parent, root):
            continue
        try:
            fd = open_directory(root)
        except FileNotFoundError:
            if category == 'state':
                report['errors'].append('state root missing')
            continue
        except (OSError, ValueError) as error:
            report['errors'].append(category + ': ' + type(error).__name__)
            continue
        try:
            with os.scandir(fd) as entries:
                for entry in entries:
                    metadata.check()
                    if entry.name.casefold() in PROTECTED:
                        report['excluded'] += 1
                        continue
                    if not entry.is_dir(follow_symlinks=False):
                        continue
                    path = root / entry.name
                    if category == 'state' and (contains(path, results.parent) or contains(results.parent, path)):
                        continue
                    source = origin(path, owners)
                    if source['task'] in owners.active:
                        continue
                    if any(contains(path, p) or contains(p, path) for p in owners.trees):
                        continue
                    if any(contains(path, p) for p in references):
                        continue
                    try:
                        allocated, newest, excluded = metadata.measure(path)
                    except OSError as error:
                        report['errors'].append(category + ' metadata: ' + type(error).__name__)
                        continue
                    report['excluded'] += excluded
                    if category == 'results' and allocated <= settings['results_min_bytes']:
                        continue
                    report['entries'].append(dict(path=str(path), category=category, allocated_bytes=allocated,
                                                   newest_mtime=newest, origin=source))
                    report['totals'][category] += allocated
        except (OSError, ScanLimit) as error:
            report['errors'].append(category + ': ' + type(error).__name__)
        finally:
            os.close(fd)
    report['entries'].sort(key=lambda row: (-row['allocated_bytes'], row['path']))
    report['total_bytes'] = sum(report['totals'].values())
    report['complete'] = not report['errors']
    return report


def size(value):
    for divisor, unit in ((1024 ** 3, 'GiB'), (1024 ** 2, 'MiB'), (1024, 'KiB')):
        if abs(value) >= divisor:
            return '%.2f %s' % (value / divisor, unit)
    return '%d B' % value


def summary(report):
    return ('Unowned storage grew by %s to %s in %d folders. '
            'State %s; large results %s. Report only. '
            'Use cleanup.stray_report for paths and dates.') % (
                size(report['delta_bytes']), size(report['total_bytes']),
                len(report['entries']), size(report['totals']['state']), size(report['totals']['results']))


async def run_once(store):
    if store.get('stray_report_enabled', False) is not True:
        return None
    settings = configuration(store)
    owners = ownership(store, settings)
    names = {topic['id']: topic['name'] for topic in store.topics()}
    owners = Ownership(owners.trees, owners.references,
                       {key: dict(task, chat=names.get(task['topic'])) for key, task in owners.tasks.items()}, owners.active)
    cancelled = threading.Event()
    try:
        report = await asyncio.to_thread(scan, settings, owners, store.directory / 'results', cancelled)
    finally:
        cancelled.set()
    if store.get('stray_report_enabled', False) is not True:
        return None
    previous = store.get('stray_report_previous') or dict(total_bytes=0, totals=dict(state=0, results=0))
    report['delta_bytes'] = report['total_bytes'] - previous['total_bytes']
    report['deltas'] = {key: value - previous['totals'].get(key, 0) for key, value in report['totals'].items()}
    with store.transaction():
        history = store.get('stray_report_history', [])
        store.put('stray_report_history', (history + [report])[-settings['keep']:])
        if report['complete']:
            store.put('stray_report_previous', dict(total_bytes=report['total_bytes'], totals=report['totals']))
            topic = store.get('coordinator_home_topic')
            if topic and report['delta_bytes'] > settings['growth_bytes']:
                store.enqueue_report(topic, summary(report))
    return report
