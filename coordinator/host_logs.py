"""Event-driven reclamation of completed host journals."""

import asyncio
from collections import Counter
import errno
from itertools import chain
import json
import logging
import os
from pathlib import Path
import re
import select
import stat
import sys
from time import time
import uuid

from . import problems, process_ownership as ownership
from .host import _reap_launchers, _write, alive, host_state
from .worker_storage import DeleteFailed, _directory, delete


logger = logging.getLogger(__name__)
REQUEST = '_cleanup.host_logs'
ACTIVE = ('queued', 'running', 'waiting_for_secret', 'waiting_for_quota')
WORKER = re.compile(r'worker-([1-9][0-9]*)(?:\.[0-9]+)*(?:-resume-[0-9]+(?:\.[0-9]+)*)?\Z')
MARKER_LINK_LIMIT = 4096
RETRY_LIMIT = 24


def _marker_record(path):
    try:
        descriptor = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        return None
    try:
        record = os.fstat(descriptor)
        if stat.S_ISREG(record.st_mode) and record.st_size == 4 and os.read(descriptor, 4) == b'true':
            return record
    finally:
        os.close(descriptor)
    return None


def _link_marker(template, staging, record):
    source = os.open(str(template.parent), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    destination = None
    linked = None
    try:
        destination = os.open(str(staging.parent), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.link(template.name, staging.name, src_dir_fd=source, dst_dir_fd=destination, follow_symlinks=False)
            linked = staging.name
        except FileExistsError:
            linked = staging.name + '-' + uuid.uuid4().hex
            os.link(template.name, linked, src_dir_fd=source, dst_dir_fd=destination, follow_symlinks=False)
        current = os.stat(linked, dir_fd=destination, follow_symlinks=False)
        if not stat.S_ISREG(current.st_mode) or (current.st_dev, current.st_ino) != (record.st_dev, record.st_ino):
            raise OSError('Host marker payload changed while linking')
        if linked != staging.name:
            os.replace(linked, staging.name, src_dir_fd=destination, dst_dir_fd=destination)
        linked = None
    finally:
        if linked is not None and destination is not None:
            try:
                os.unlink(linked, dir_fd=destination)
            except FileNotFoundError:
                pass
        if destination is not None:
            os.close(destination)
        os.close(source)


class HostLogs:
    def __init__(self, store):
        self.store = store
        self.hosts = store.directory / 'hosts'
        self.quarantine = self.hosts / '.deleting'
        self.marker = store.directory / 'host-log-marker.json'
        self.queue = None
        self.pending = []
        self.queued = set()
        self.retry = store.get('host_log_delete_retries', {})
        self.retry_dirty = False
        self.watches = {}
        self.watch_hosts = {}
        self.exited = {}

    def request(self, target=None):
        target = dict(target or {})
        key = json.dumps(target, sort_keys=True)
        if key in self.queued:
            return
        self.queued.add(key)
        if self.queue is None:
            self.pending.append(target)
        else:
            self.queue.put_nowait(target)

    async def run(self):
        if self.queue is None:
            self.queue = asyncio.Queue()
            for target in self.pending:
                self.queue.put_nowait(target)
            self.pending.clear()
        try:
            while True:
                delay = min((entry['due'] for entry in self.retry.values()), default=None)
                queued = True
                try:
                    target = await asyncio.wait_for(self.queue.get(), max(0, delay - time())) if delay is not None else await self.queue.get()
                except asyncio.TimeoutError:
                    target, queued = {'retry': True}, False
                if queued:
                    self.queued.discard(json.dumps(target, sort_keys=True))
                try:
                    await self.clean(target)
                finally:
                    if queued:
                        self.queue.task_done()
        except Exception:
            for name in self.watch_hosts:
                self.request({'host': name})
            raise
        finally:
            self.close()

    def close(self):
        for key in list(self.watches):
            self._close_watch(key)
        self.exited.clear()

    def _process_key(self, directory, name, pid, unique=None):
        folder = directory.lstat()
        record = (directory / name).lstat()
        if not stat.S_ISDIR(folder.st_mode) or not stat.S_ISREG(record.st_mode):
            raise OSError('Unsafe process exit record')
        return (directory.name, name, pid, unique, (folder.st_dev, folder.st_ino),
                (record.st_dev, record.st_ino, record.st_mtime_ns, record.st_ctime_ns))

    def _close_watch(self, key):
        current = self.watches.pop(key, None)
        if current is not None:
            keys = self.watch_hosts[key[0]]
            keys.remove(key)
            if not keys:
                self.watch_hosts.pop(key[0])
            loop, descriptor, queue = current
            if not loop.is_closed():
                loop.remove_reader(descriptor)
            if queue is not None:
                queue.close()
            else:
                os.close(descriptor)

    def _process_exited(self, key, notify=True):
        self._close_watch(key)
        directory = self.hosts / key[0]
        try:
            if self._process_key(directory, key[1], key[2], key[3]) != key:
                return
        except OSError:
            return
        self.exited.setdefault(key[0], set()).add(key)
        _reap_launchers()
        if notify:
            self.request({'host': key[0]})

    def _exit_ready(self, key):
        current = self.watches.get(key)
        if current is None:
            return
        try:
            if current[2] is not None:
                events = current[2].control(None, 1, 0)
                if not any(event.fflags & select.KQ_NOTE_EXIT for event in events):
                    return
            self._process_exited(key)
        except OSError as error:
            self._close_watch(key)
            self._failed(self.hosts / key[0], error)
            self.request({'retry': True})

    def _watch_process(self, directory, name, pid, unique=None):
        key = self._process_key(directory, name, pid, unique)
        if key in self.exited.get(key[0], ()):
            return False
        if key in self.watches:
            return True
        queue = None
        descriptor = None
        try:
            if sys.platform == 'darwin':
                queue = select.kqueue()
                descriptor = queue.fileno()
                event = select.kevent(pid, filter=select.KQ_FILTER_PROC,
                                      flags=select.KQ_EV_ADD | select.KQ_EV_ONESHOT,
                                      fflags=select.KQ_NOTE_EXIT)
                queue.control([event], 0, 0)
            elif hasattr(os, 'pidfd_open'):
                descriptor = os.pidfd_open(pid, 0)
            else:
                raise OSError(errno.ENOSYS, 'Native process exit notifications are unavailable')
            loop = asyncio.get_running_loop()
            self.watches[key] = (loop, descriptor, queue)
            self.watch_hosts.setdefault(key[0], set()).add(key)
            loop.add_reader(descriptor, self._exit_ready, key)
            if sys.platform == 'darwin':
                value = ownership.Bsd()
                ownership.C.set_errno(0)
                size = ownership.C.sizeof(value)
                length = ownership.backend.load().proc_pidinfo(pid, 13, 1, ownership.C.byref(value), size)
                if length != size:
                    raise OSError(ownership.C.get_errno(), 'Cannot verify watched process status')
                if value.pid != pid:
                    raise OSError('Watched process identity changed')
                if value.status == 5:
                    self._process_exited(key, notify=False)
                    return False
            return True
        except OSError as error:
            if key in self.watches:
                self._close_watch(key)
            elif queue is not None:
                queue.close()
            elif descriptor is not None:
                os.close(descriptor)
            if error.errno == errno.ESRCH:
                self._process_exited(key, notify=False)
                return False
            raise

    def _watch_records(self, directory):
        for name in ('pid', 'child.pid'):
            path = directory / name
            if path.is_file():
                try:
                    pid = int(path.read_text())
                except ValueError:
                    continue
                if pid > 1 and alive(pid):
                    self._watch_process(directory, name, pid)

    def _receipts_pending(self, worker):
        for row in self.store.db.execute(
                "SELECT state,result FROM service_requests WHERE op IN ('workers.steer','workers.goal') "
                "AND json_extract(params,'$.worker')=?", (worker,)):
            result = json.loads(row['result'] or '{}')
            if (row['state'] in ('queued', 'sending') or
                    row['state'] == 'uncertain' and not result.get('noted') or
                    row['state'] == 'done' and result.get('receipt') == 'queued'):
                return True
        return False

    async def _ended(self, directory):
        exit_path = directory / 'exit.json'
        ended = json.loads(exit_path.read_text()) if exit_path.is_file() else None
        if ended is not None and (not isinstance(ended, dict) or type(ended.get('returncode')) is not int):
            return 'exit_record'
        failed = directory / 'launch-failed.json'
        failed = failed.is_file() and json.loads(failed.read_text()) is True
        record = ownership.read_owner(directory)
        if record is None and (((directory / 'job.plist').exists() and not (
                failed and (directory / 'job-booted-out').is_file())) or ended and 'cleanup' in ended):
            return 'ownership'
        if record is not None:
            if not isinstance(record, dict):
                return 'ownership'
            if record.get('cleanup') not in ('ok', 'stale'):
                return 'ownership'
            if ownership.recovery(directory) is not None:
                return 'ownership'
            if ownership.backend.boot() != record['boot']:
                return None
        pid = directory / 'pid'
        if not pid.is_file():
            return None if failed and record is None and not (directory / 'child.pid').exists() else 'process_record'
        try:
            host_pid = int(pid.read_text())
        except ValueError:
            return 'process_record'
        if host_pid <= 1:
            return 'process_record'
        _reap_launchers()
        if alive(host_pid) and self._watch_process(directory, 'pid', host_pid):
            return 'processes'
        if record is not None:
            remaining = await asyncio.to_thread(ownership.select, record)
            if any([self._watch_process(directory, 'owner.json', process.pid, process.unique)
                    for process in remaining]):
                return 'ownership'
        else:
            child = directory / 'child.pid'
            if child.is_file():
                try:
                    child_pid = int(child.read_text())
                except ValueError:
                    return 'process_record'
                if child_pid <= 1:
                    return 'process_record'
                if alive(child_pid) and self._watch_process(directory, 'child.pid', child_pid):
                    return 'processes'
                try:
                    os.killpg(child_pid, 0)
                except ProcessLookupError:
                    return None
                except PermissionError:
                    pass
                try:
                    members, ended = await ownership.group_members(child_pid)
                except OSError:
                    try:
                        os.killpg(child_pid, 0)
                    except ProcessLookupError:
                        return None
                    except PermissionError:
                        pass
                    raise
                if not ended:
                    watching = [self._watch_process(directory, 'child.pid', pid, identity)
                                for pid, identity in members]
                    if not any(watching):
                        try:
                            os.killpg(child_pid, 0)
                        except ProcessLookupError:
                            return None
                        if members:
                            self.request({'host': directory.name})
                        else:
                            raise OSError('Process group termination is unproved')
                    return 'processes'
            elif ended is None and not failed:
                return 'process_record'
        return None

    def _worker(self, worker_id):
        row = self.store.db.execute('SELECT * FROM workers WHERE id=?', (worker_id,)).fetchone()
        if row is None:
            return None, 'unknown_worker'
        if row['status'] in ACTIVE:
            return row, 'worker_active'
        if not isinstance(json.loads(row['result'] or 'null'), dict):
            return row, 'result_missing'
        if self._receipts_pending(worker_id):
            return row, 'receipts'
        return row, None

    def _job_closed(self, worker):
        job = self.store.task_get(worker['task'])
        return (job is not None and job['status'] in ('done', 'dropped') and
                not self.store.db.execute(
                    "SELECT 1 FROM workers WHERE task=? AND status IN (?,?,?,?)",
                    (job['id'], *ACTIVE)).fetchone())

    def _temp_pending(self, directory, worker):
        if worker is None:
            return False
        state = self.store.db.execute(
            'SELECT path,pending FROM worker_temp_state WHERE worker=?', (worker['id'],)).fetchone()
        if state is None or not state['pending']:
            return False
        if state['path'] is not None:
            return True
        spec = directory / 'spec.json'
        if spec.is_symlink():
            raise OSError('Host spec is a symbolic link')
        return spec.is_file() and json.loads(spec.read_text()).get('worker_temp') is True

    def _retention(self, directory):
        if directory.is_symlink():
            return None, False, 'symbolic_host'
        match = WORKER.fullmatch(directory.name)
        if match:
            worker, reason = self._worker(int(match[1]))
            if reason:
                return worker, False, reason
            return worker, self.store.get('close_clears_space', False) is True or self._job_closed(worker), None
        elif directory.name.startswith('coordinator-'):
            for row in self.store.db.execute("SELECT value FROM settings WHERE substr(key,-5)='_host'"):
                value = json.loads(row[0])
                if isinstance(value, dict) and value.get('id') == directory.name:
                    return None, False, 'parent_referenced'
            return None, True, None
        return None, False, 'unknown_kind'

    def _worker_run_result(self, directory, worker):
        pid = None
        pid_path = directory / 'pid'
        if pid_path.is_file():
            try:
                pid = int(pid_path.read_text())
            except ValueError:
                pass
        for row in self.store.db.execute(
                "SELECT text FROM messages WHERE source_worker=? AND kind='worker_result' ORDER BY id DESC",
                (worker['id'],)):
            try:
                result = json.loads(row['text'])
            except (TypeError, ValueError):
                continue
            if not isinstance(result, dict):
                continue
            log_path = result.get('log_path')
            if isinstance(log_path, str) and Path(log_path) == directory / 'stderr.log':
                return result
            if pid is not None and type(result.get('pid')) is int and result['pid'] == pid:
                return result
        if worker['host'] == directory.name:
            try:
                result = json.loads(worker['result'] or 'null')
            except (TypeError, ValueError):
                return None
            return result if isinstance(result, dict) else None
        return None

    def _retain_worker_logs_in_current_round(self, directory, worker):
        if worker is None:
            return False
        job = self.store.task_get(worker['task'])
        if (job is None or worker['iteration'] != job['iteration'] or
                job['status'] in ('done', 'dropped')):
            return False
        result = self._worker_run_result(directory, worker)
        if result is None:
            return True
        failed = (result.get('success') is False or bool(result.get('failure_code')) or
                  result.get('status') == 'interrupted')
        return failed

    def _save_resume(self, directory, worker):
        if worker is None or worker['provider'] != 'codex':
            return
        key = 'host_log_secret_names:' + str(worker['id'])
        names = set(self.store.get(key, []))
        spec = directory / 'spec.json'
        if spec.is_symlink():
            raise OSError('Host spec is a symbolic link')
        if spec.is_file():
            names.update(json.loads(spec.read_text()).get('secret_names', []))
        with self.store.db:
            if names:
                self.store.put(key, sorted(names))
            else:
                self.store.db.execute('DELETE FROM settings WHERE key=?', (key,))

    def _key(self, path):
        relative = path.relative_to(self.hosts)
        if len(relative.parts) == 2 and relative.parts[1] in ('events.jsonl', 'writes.jsonl'):
            return relative.parts[0]
        if len(relative.parts) == 1 or len(relative.parts) == 2 and relative.parts[0] == '.deleting':
            return str(relative)
        raise ValueError('Invalid host cleanup retry path')

    def _save_retries(self):
        self.retry_dirty = True

    def _clear_retry(self, path):
        if self.retry.pop(self._key(path), None) is not None:
            self._save_retries()

    def _failed(self, path, error):
        key = self._key(path)
        destination = self._key(error.path) if isinstance(error, DeleteFailed) else key
        attempt = self.retry.pop(key, {}).get('attempt', 0) + 1
        if attempt < RETRY_LIMIT:
            self.retry[destination] = {'attempt': attempt, 'due': time() + min(3600, 60 * 2 ** min(attempt - 1, 6))}
        else:
            problems.record(self.store, 'hosts', 'log-delete-failed', '%s: %s' % (destination, error),
                            level=logging.ERROR, every=86400)
        self._save_retries()

    async def _remove(self, path, counts, kept):
        key = self._key(path)
        if self.retry.get(key, {}).get('due', 0) > time():
            kept['retry_backoff'] += 1
            return
        try:
            if self.store.get('close_clears_space', False) is True:
                from .workspaces import _repository_lock
                async with _repository_lock(self.store.directory, 'closed-copy-deletion'):
                    await delete(path, self.quarantine, counts, paced=True)
            else:
                await delete(path, self.quarantine, counts)
        except OSError as error:
            self._failed(path, error)
            kept['delete_failed'] += 1
            return
        if (path.parent == self.hosts or path.parent == self.quarantine) and self.retry.pop(key, None) is not None:
            self._save_retries()

    def _remember(self, directories):
        names = []
        for directory in directories:
            match = WORKER.fullmatch(directory.name)
            if match and (directory.is_symlink() or directory.exists()):
                names.append((directory.name, int(match[1])))
        self.store.host_log_register_many(names)

    def _forget_processes(self, directory):
        for key in list(self.watch_hosts.get(directory.name, ())):
            self._close_watch(key)
        self.exited.pop(directory.name, None)

    def _forget(self, directory):
        self._forget_processes(directory)
        with self.store.db:
            self.store.db.execute('DELETE FROM host_log_hosts WHERE name=?', (directory.name,))

    async def _recover(self, directory):
        if not (directory / 'job.plist').is_file() or host_state(directory) == 'running':
            return
        if ownership.recovery(directory) is None:
            return
        code = ('import json,sys; from coordinator.host import settle_ownership; '
                'errors=[]; settle_ownership(sys.argv[1], lambda *items: errors.append(items)); '
                'print(json.dumps(errors))')
        child = await asyncio.create_subprocess_exec(
            sys.executable, '-c', code, str(directory), cwd=str(Path(__file__).resolve().parents[1]),
            env={'PATH': os.defpath, 'HOME': str(self.store.directory), 'TMPDIR': str(self.store.directory)},
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            output, _ = await child.communicate()
            if child.returncode:
                raise OSError('Host ownership recovery child failed')
            for area, code, detail in json.loads(output):
                problems.record(self.store, area, code, detail, every=60)
        finally:
            if child.returncode is None:
                try:
                    child.terminate()
                except ProcessLookupError:
                    pass
                try:
                    await asyncio.wait_for(child.wait(), 1)
                except asyncio.TimeoutError:
                    try:
                        child.kill()
                    except ProcessLookupError:
                        pass
                    await child.wait()

    def _new_marker(self, staging):
        _write(self.marker, True)
        record = _marker_record(self.marker)
        if record is None:
            raise OSError('Host marker payload is invalid')
        try:
            _link_marker(self.marker, staging, record)
        except OSError as error:
            if error.errno != errno.EXDEV:
                raise
            _write(staging, True)

    async def _prepare_marker(self, directory):
        staging = directory / 'logs-deleted.pending'
        if _marker_record(staging) is not None:
            return staging
        record = _marker_record(self.marker)
        cross_device = False
        if record is not None and record.st_nlink < MARKER_LINK_LIMIT:
            try:
                _link_marker(self.marker, staging, record)
                return staging
            except OSError as error:
                if error.errno == errno.EXDEV:
                    cross_device = True
                elif error.errno != errno.EMLINK:
                    raise
        preparing = asyncio.to_thread(_write, staging, True) if cross_device else asyncio.to_thread(self._new_marker, staging)
        writing = asyncio.create_task(preparing)
        try:
            await asyncio.shield(writing)
        except asyncio.CancelledError:
            await asyncio.gather(writing, return_exceptions=True)
            raise
        return staging

    async def _folder(self, directory, counts, kept):
        if directory.name == '.deleting':
            return
        if directory.is_symlink():
            kept['symbolic_host'] += 1
            self._clear_retry(directory)
            return
        worker, whole, reason = self._retention(directory)
        if reason and reason != 'unknown_kind':
            kept[reason] += 1
            self._clear_retry(directory)
            return
        parent = os.open(str(self.hosts), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.close(_directory(directory.name, parent))
        finally:
            os.close(parent)
        if any((directory / name).is_symlink() for name in (
                'exit.json', 'owner.json', 'pid', 'child.pid', 'job.plist', 'job-booted-out', 'launch-failed.json')):
            kept['symbolic_record'] += 1
            self._clear_retry(directory)
            return
        await self._recover(directory)
        if reason:
            kept[reason] += 1
            self._clear_retry(directory)
            return
        reason = await self._ended(directory)
        if reason:
            if reason in ('processes', 'ownership'):
                self._watch_records(directory)
            kept[reason] += 1
            self._clear_retry(directory)
            return
        worker, whole, reason = self._retention(directory)
        if reason:
            kept[reason] += 1
            self._clear_retry(directory)
            return
        if self._retain_worker_logs_in_current_round(directory, worker):
            kept['failed_worker'] += 1
            self._clear_retry(directory)
            return
        temp_pending = whole and self._temp_pending(directory, worker)
        if whole and not temp_pending:
            self._save_resume(directory, worker)
            await self._remove(directory, counts, kept)
            if not directory.exists() and not directory.is_symlink():
                self._forget(directory)
            return
        marker = directory / 'logs-deleted.json'
        if _marker_record(marker) is None:
            staging = await self._prepare_marker(directory)
            worker, whole, reason = self._retention(directory)
            if reason:
                kept[reason] += 1
                self._clear_retry(directory)
                return
            if self._retain_worker_logs_in_current_round(directory, worker):
                kept['failed_worker'] += 1
                self._clear_retry(directory)
                return
            os.replace(staging, marker)
        for name in ('events.jsonl', 'writes.jsonl'):
            path = directory / name
            if path.exists() or path.is_symlink():
                await self._remove(path, counts, kept)
        if not any((directory / name).exists() or (directory / name).is_symlink()
                   for name in ('events.jsonl', 'writes.jsonl')):
            self._forget_processes(directory)
            if self.retry.pop(directory.name, None) is not None:
                self._save_retries()
        kept['worker_temp_pending' if temp_pending else 'stderr_and_resume'] += 1

    def _candidates(self, target):
        if 'host' in target:
            name = target['host']
            if isinstance(name, str) and Path(name).name == name:
                yield self.hosts / name
        elif 'worker' in target or 'task' in target:
            where, identity = ('id', target['worker']) if 'worker' in target else ('task', target['task'])
            rows = self.store.db.execute('SELECT id,host FROM workers WHERE ' + where + '=?',
                                         (identity,)).fetchall()
            seen = set()
            for worker in rows:
                names = [row[0] for row in self.store.db.execute(
                    'SELECT name FROM host_log_hosts WHERE worker=?', (worker['id'],)).fetchall()]
                for name in chain(names, (worker['host'] or 'worker-' + str(worker['id']),)):
                    match = WORKER.fullmatch(name)
                    if match and int(match[1]) == worker['id'] and name not in seen:
                        seen.add(name)
                        yield self.hosts / name
        else:
            with os.scandir(self.hosts) as entries:
                for entry in entries:
                    yield Path(entry.path)

    async def clean(self, target=None):
        counts, kept = Counter(), Counter()
        pending = []
        try:
            if not self.hosts.is_dir():
                return
            if self.hosts.is_symlink() or self.quarantine.is_symlink():
                kept['unsafe_root'] += 1
                return
            if not target and self.quarantine.exists():
                with os.scandir(self.quarantine) as entries:
                    for entry in entries:
                        await self._remove(Path(entry.path), counts, kept)
                        if not Path(entry.path).exists() and not Path(entry.path).is_symlink():
                            counts['interrupted'] += 1
            if target and target.get('retry'):
                for key, entry in list(self.retry.items()):
                    if entry['due'] <= time():
                        path = self.hosts / key
                        try:
                            if path.parent == self.quarantine:
                                await self._remove(path, counts, kept)
                            elif path.exists() or path.is_symlink():
                                await self._folder(path, counts, kept)
                            else:
                                self.retry.pop(key, None)
                                self._save_retries()
                        except OSError as error:
                            self._failed(path, error)
                            kept['delete_failed'] += 1
                        except (ValueError, KeyError, TypeError):
                            self._clear_retry(path)
                            kept['invalid_record'] += 1
                        await asyncio.sleep(0)
                return
            for directory in self._candidates(target or {}):
                try:
                    if self.retry.get(self._key(directory), {}).get('due', 0) > time():
                        kept['retry_backoff'] += 1
                        await asyncio.sleep(0)
                        continue
                    if directory.exists() or directory.is_symlink():
                        if target:
                            self._remember((directory,))
                        await self._folder(directory, counts, kept)
                    else:
                        self._forget(directory)
                except OSError as error:
                    self._failed(directory, error)
                    kept['error'] += 1
                except (ValueError, KeyError, TypeError):
                    kept['invalid_record'] += 1
                if not target:
                    pending.append(directory)
                    if len(pending) == 32:
                        self._remember(pending)
                        pending.clear()
                await asyncio.sleep(0)
            if not target:
                self._remember(pending)
                pending.clear()
                for row in self.store.db.execute('SELECT name FROM host_log_hosts').fetchall():
                    directory = self.hosts / row['name']
                    if not directory.exists() and not directory.is_symlink():
                        self._forget(directory)
                    await asyncio.sleep(0)
        except OSError:
            kept['error'] += 1
        finally:
            if pending:
                self._remember(pending)
            if self.retry_dirty:
                with self.store.db:
                    self.store.put('host_log_delete_retries', self.retry)
                self.retry_dirty = False
            logger.info('host logs cleanup folders=%d logs=%d bytes=%d interrupted=%d kept=%s',
                        counts['folders'], counts['logs'], counts['bytes'], counts['interrupted'],
                        json.dumps(kept, sort_keys=True))
