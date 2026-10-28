"""Private worker scratch, retained results, and bounded cleanup work."""

import asyncio
import json
import logging
import os
from pathlib import Path
import re
import shutil
import stat
import tempfile
import threading
import time
import uuid

from . import process_ownership, problems
from .host import alive


FINAL = ('done', 'interrupted')
BATCH = 16
DELETE_BATCH = 8
RETRY_LIMIT = 20


def results_directory(store, task_id, create=False):
    path = store.directory / 'results' / ('task-' + str(task_id))
    if create:
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        path.chmod(0o700)
    return str(path)


def temp_base():
    return Path(tempfile.gettempdir()).resolve()


def temp_directory(worker_id):
    return temp_base() / ('torii-w' + str(worker_id))


def lease_paths(store, task_id):
    """List recorded worker roots without creating or detaching any folder."""
    paths = []
    for row in store.db.execute('SELECT id,provider FROM workers WHERE task=?', (task_id,)):
        paths.append(temp_directory(row['id']))
        saved = store.db.execute('SELECT path FROM worker_temp_state WHERE worker=?', (row['id'],)).fetchone()
        if saved and saved['path']:
            path = Path(saved['path'])
            if path.is_absolute() and '..' not in path.parts and path.name == 'torii-w' + str(row['id']):
                paths.append(path)
    return tuple(dict.fromkeys(paths))


class DeleteFailed(OSError):
    def __init__(self, path, error):
        super().__init__(str(error))
        self.path = path


def _scratch_repositories(paths):
    repositories = set()
    for root in paths:
        if root.is_symlink() or not root.is_dir():
            continue
        for directory, folders, files in os.walk(root, followlinks=False):
            folders[:] = [name for name in folders if name not in
                          ('.git', 'node_modules', '.venv', 'venv', '__pycache__')]
            if '.git' not in files:
                continue
            pointer = Path(directory) / '.git'
            if pointer.is_symlink():
                continue
            value = pointer.read_text().strip()
            if not value.startswith('gitdir: '):
                continue
            registration = Path(value[8:])
            if not registration.is_absolute():
                registration = pointer.parent / registration
            registration = registration.resolve()
            common_file = registration / 'commondir'
            if not common_file.is_file():
                continue
            common = Path(common_file.read_text().strip())
            if not common.is_absolute():
                common = registration / common
            common = common.resolve()
            if registration.parent == common / 'worktrees' and not common.is_relative_to(root):
                repositories.add(str(common))
    return sorted(repositories)


def _prune_scratch_registrations(repository, roots):
    directory = Path(repository) / 'worktrees'
    if not directory.exists():
        return
    if directory.is_symlink() or not directory.is_dir():
        raise OSError('Unexpected Git worktree registration directory')
    roots = tuple(Path(root).resolve() for root in roots)
    for registration in directory.iterdir():
        if registration.is_symlink() or not registration.is_dir():
            continue
        pointer = registration / 'gitdir'
        if pointer.is_symlink() or not pointer.is_file():
            continue
        value = Path(pointer.read_text().strip())
        if not value.is_absolute():
            value = registration / value
        value = value.resolve()
        if not any(value.is_relative_to(root) for root in roots) or value.exists():
            continue
        if (registration / 'locked').exists() or any(registration.glob('*.lock')):
            raise OSError('Worker scratch Git registration is locked')
        _remove_tree(registration)


def _directory(name, parent):
    try:
        descriptor = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
    except PermissionError:
        mode = stat.S_IMODE(os.stat(name, dir_fd=parent, follow_symlinks=False).st_mode) | 0o700
        try:
            os.chmod(name, mode, dir_fd=parent, follow_symlinks=False)
        except (NotImplementedError, ValueError):
            capability = os.open(name, os.O_PATH | os.O_NOFOLLOW, dir_fd=parent)
            try:
                if not stat.S_ISDIR(os.fstat(capability).st_mode):
                    raise OSError('Host deletion directory changed')
                os.chmod('/proc/self/fd/%d' % capability, mode)
            finally:
                os.close(capability)
        descriptor = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
    try:
        mode = stat.S_IMODE(os.fstat(descriptor).st_mode)
        if mode & 0o700 != 0o700:
            os.fchmod(descriptor, mode | 0o700)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


async def delete(path, quarantine, counts, proceed=None, paced=False):
    """Rename once, then unlink through directory descriptors without following links."""
    path, quarantine = Path(path), Path(quarantine)
    if proceed is not None and not proceed():
        return False
    target = path
    anchor, root, source, location, stack = None, None, None, None, []
    try:
        if '..' in path.parts or '..' in quarantine.parts or not quarantine.name:
            raise OSError('Host deletion path is outside the anchored root')
        try:
            relative = path.relative_to(quarantine.parent)
        except ValueError as error:
            raise OSError('Host deletion path is outside the anchored root') from error
        if len(relative.parts) not in (1, 2):
            raise OSError('Host deletion path is outside the anchored root')
        anchor = os.open(str(quarantine.parent), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.mkdir(quarantine.name, mode=0o700, dir_fd=anchor)
        except FileExistsError:
            pass
        root = _directory(quarantine.name, anchor)
        steps = 0
        started = time.monotonic()
        if path.parent != quarantine:
            source = anchor if len(relative.parts) == 1 else _directory(relative.parts[0], anchor)
            location = source
            target = _detach(path, quarantine / (path.name + '-' + uuid.uuid4().hex), allow_files=True,
                             src_dir_fd=source, dst_dir_fd=root)
            if target is None:
                return False
            steps = 1
        location = root
        stack = [(root, target.name, None)]
        while stack:
            if proceed is not None and not proceed():
                return False
            parent, name, opened = stack[-1]
            if opened is None:
                info = os.stat(name, dir_fd=parent, follow_symlinks=False)
                if not stat.S_ISDIR(info.st_mode):
                    os.unlink(name, dir_fd=parent)
                    counts['bytes'] += info.st_size
                    if name in ('events.jsonl', 'writes.jsonl') or name.startswith(('events.jsonl-', 'writes.jsonl-')):
                        counts['logs'] += 1
                    stack.pop()
                else:
                    descriptor = _directory(name, parent)
                    try:
                        entries = os.scandir(descriptor)
                    except BaseException:
                        os.close(descriptor)
                        raise
                    stack[-1] = (parent, name, (descriptor, entries))
            else:
                descriptor, entries = opened
                entry = next(entries, None)
                if entry is None:
                    entries.close()
                    os.close(descriptor)
                    stack[-1] = (parent, name, None)
                    os.rmdir(name, dir_fd=parent)
                    if len(stack) == 1:
                        counts['folders'] += 1
                    stack.pop()
                else:
                    stack.append((descriptor, entry.name, None))
            steps += 1
            if steps >= (1000 if paced else DELETE_BATCH) or paced and time.monotonic() - started >= 0.1:
                steps = 0
                await asyncio.sleep(0.2 if paced else 0)
                started = time.monotonic()
        if steps:
            await asyncio.sleep(0)
        return True
    except FileNotFoundError:
        if location is None:
            return False
        try:
            os.stat(target.name, dir_fd=location, follow_symlinks=False)
        except FileNotFoundError:
            return False
        raise
    except OSError as error:
        raise DeleteFailed(target, error) from error
    finally:
        for _, _, opened in reversed(stack):
            if opened is not None:
                opened[1].close()
                os.close(opened[0])
        for descriptor in {anchor, root, source} - {None}:
            os.close(descriptor)


def _remove_tree(path):
    """Remove a detached tree without traversing symbolic links."""
    if not shutil.rmtree.avoids_symlink_attacks:
        raise OSError('Safe directory removal is unavailable')

    def permissions(directory):
        descriptor = _directory(str(directory), None)
        try:
            os.fchmod(descriptor, 0o700)
        finally:
            os.close(descriptor)

    def writable(function, name, error):
        if isinstance(error[1], FileNotFoundError):
            return
        target = Path(name)
        if target != path:
            permissions(target.parent)
        try:
            mode = target.lstat().st_mode
        except FileNotFoundError:
            return
        if stat.S_ISDIR(mode):
            permissions(target)
            if function in (os.open, os.scandir, os.listdir):
                shutil.rmtree(target, onerror=writable)
                return
        function(name)

    try:
        if path.is_symlink():
            path.unlink()
        else:
            permissions(path)
            shutil.rmtree(path, onerror=writable)
    except FileNotFoundError:
        pass


async def _background(function, *args):
    loop = asyncio.get_running_loop()
    finished = loop.create_future()

    def settle(value, error):
        if not finished.done():
            if error is None:
                finished.set_result(value)
            else:
                finished.set_exception(error)

    def run():
        value, error = None, None
        try:
            value = function(*args)
        except Exception as failure:
            error = failure
        try:
            loop.call_soon_threadsafe(settle, value, error)
        except RuntimeError:
            pass

    threading.Thread(target=run, name='worker-temp-cleanup', daemon=True).start()
    return await finished


def _detach(path, destination=None, allow_files=False, src_dir_fd=None, dst_dir_fd=None):
    if (src_dir_fd is None) != (dst_dir_fd is None) or src_dir_fd is not None and destination is None:
        raise ValueError('Detached directory descriptors require an explicit source and destination')
    try:
        mode = (path.lstat() if src_dir_fd is None else
                os.stat(path.name, dir_fd=src_dir_fd, follow_symlinks=False)).st_mode
    except FileNotFoundError:
        return None
    if not allow_files and not (stat.S_ISDIR(mode) or stat.S_ISLNK(mode)):
        return None
    detached = destination if destination is not None else path.with_name(path.name + '.deleting-' + uuid.uuid4().hex)
    if src_dir_fd is None:
        path.rename(detached)
    else:
        os.rename(path.name, detached.name, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)
    return detached


def processes_gone(directory):
    spec_path = directory / 'spec.json'
    if not spec_path.exists():
        return not directory.exists()
    spec = json.loads(spec_path.read_text())
    if spec.get('ownership'):
        record = process_ownership.read_owner(directory)
        return record is not None and record['cleanup'] == 'ok'
    if (directory / 'exit.json').is_file():
        return True
    child = directory / 'child.pid'
    host = directory / 'pid'
    return ((not child.exists() or not alive(child.read_text().strip())) and
            (not host.exists() or not alive(host.read_text().strip())))


def _processes_gone(hosts):
    return all(processes_gone(host) for host in hosts)


def _host_index(directory):
    indexed = {}
    if directory.exists():
        with os.scandir(directory) as entries:
            for entry in entries:
                match = re.fullmatch(r'worker-(\d+)(?:[.-].*)?', entry.name)
                if match and entry.is_dir(follow_symlinks=False):
                    indexed.setdefault(int(match.group(1)), set()).add(Path(entry.path))
    return indexed


def _remnant_index(directory):
    indexed = {}
    if directory.exists():
        with os.scandir(directory) as entries:
            for entry in entries:
                match = re.fullmatch(r'(.+)\.deleting-[a-f0-9]{32}', entry.name)
                if match and (entry.is_symlink() or entry.is_dir(follow_symlinks=False)):
                    indexed.setdefault(match.group(1), set()).add(Path(entry.path))
    return indexed


def _scratch_paths(directory, worker, state, hosts):
    paths = {temp_directory(worker['id'])}
    if state['path']:
        paths.add(Path(state['path']))
    root = directory.resolve() / 'opencode' / 'workers' / str(worker['id']) / 'launches'
    for host in hosts:
        spec_path = host / 'spec.json'
        if not spec_path.exists():
            continue
        spec = json.loads(spec_path.read_text())
        if not spec.get('worker_temp'):
            continue
        if worker['provider'] != 'opencode':
            saved = Path(spec['env']['TMPDIR'])
            if (not saved.is_absolute() or saved.name != 'torii-w' + str(worker['id'])
                    or saved.parent != saved.parent.resolve()):
                raise OSError('Unexpected worker scratch path')
            paths.add(saved)
        else:
            launch = Path(spec['env']['TMPDIR']).parent
            if launch.parent != root or not re.fullmatch('[a-f0-9]{32}', launch.name):
                raise OSError('Unexpected OpenCode launch scratch path')
            for parent in (launch, *launch.parents):
                if parent == directory.resolve():
                    break
                if parent.is_symlink():
                    raise OSError('Symbolic link in OpenCode scratch path')
            paths.update((launch / 'tmp', launch / 'home' / 'Library' / 'Caches'))
    return paths


def _temp_index(roots):
    workers, failures = set(), []
    for root in roots:
        try:
            with os.scandir(root) as entries:
                for entry in entries:
                    match = re.fullmatch(r'torii-w(\d+)', entry.name)
                    if match and (entry.is_symlink() or entry.is_dir(follow_symlinks=False)):
                        workers.add(int(match[1]))
        except FileNotFoundError:
            continue
        except OSError as error:
            failures.append((str(root), str(error)))
    return workers, failures


class WorkerStorage:
    """One durable temp lifetime per worker and a bounded queue of detached trees."""

    def __init__(self, store):
        self.store = store
        self.tasks = {}
        self.retry_delays = {}
        self.hosts = None
        self.registered_hosts = {}
        self.host_loading = None
        self.remnants = {}
        self.remnant_loading = {}
        self.read_slots = None
        self.removal_slots = None
        self.closed = False

    def close(self):
        self.closed = True
        for task in (*self.tasks.values(), *self.remnant_loading.values(), self.host_loading):
            if task is not None and not task.done():
                task.cancel()

    def state(self, worker_id):
        row = self.store.db.execute('SELECT * FROM worker_temp_state WHERE worker=?', (worker_id,)).fetchone()
        return dict(row) if row else None

    def worker(self, worker_id):
        return dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (worker_id,)).fetchone())

    def register_host(self, worker_id, name):
        self.registered_hosts.setdefault(worker_id, set()).add(self.store.directory / 'hosts' / name)

    async def host_paths(self, worker):
        root = self.store.directory / 'hosts'
        if self.hosts is None:
            if self.host_loading is None:
                self.host_loading = asyncio.create_task(_background(_host_index, root))
            self.hosts = await asyncio.shield(self.host_loading)
        return self.hosts.get(worker['id'], set()) | self.registered_hosts.get(worker['id'], set()) | {
            root / (worker['host'] or 'worker-' + str(worker['id'])),
            root / ('worker-' + str(worker['id']))}

    async def leftover_paths(self, path):
        directory = path.parent
        if directory not in self.remnants:
            if directory not in self.remnant_loading:
                self.remnant_loading[directory] = asyncio.create_task(_background(_remnant_index, directory))
            self.remnants[directory] = await asyncio.shield(self.remnant_loading[directory])
        return self.remnants[directory].pop(path.name, set())

    def remember(self, worker_id, detached):
        if detached:
            state = self.state(worker_id)
            staged = set(json.loads(state['staged'])) | {str(path) for path in detached}
            with self.store.transaction():
                self.store.db.execute('UPDATE worker_temp_state SET staged=?,pending=1 WHERE worker=?',
                                      (json.dumps(sorted(staged)), worker_id))

    async def reconcile(self):
        if self.closed:
            return
        roots = {temp_base()}
        roots.update(Path(row['path']).parent for row in self.store.db.execute(
            'SELECT DISTINCT path FROM worker_temp_state WHERE path IS NOT NULL').fetchall())
        workers, failures = await _background(_temp_index, roots)
        for root, error in failures:
            problems.record(self.store, 'worker', 'temp-reconcile-failed', root + ': ' + error, every=60)
        with self.store.transaction():
            for worker_id in workers:
                self.store.db.execute('''UPDATE worker_temp_state SET pending=1
                    WHERE worker=? AND finished=1 AND pending=0 AND attempts<?
                    AND EXISTS (SELECT 1 FROM workers WHERE workers.id=worker_temp_state.worker AND status IN ('done','interrupted'))''',
                                      (worker_id, RETRY_LIMIT))

    def schedule(self, worker_id=None):
        if self.closed:
            return
        available = BATCH - len(self.tasks)
        if available <= 0:
            return
        if worker_id is None:
            rows = self.store.db.execute(
                'SELECT worker FROM worker_temp_state WHERE pending=1 AND retry_at<=? ORDER BY retry_at,worker LIMIT ?',
                (time.time(), available)).fetchall()
        else:
            rows = self.store.db.execute(
                'SELECT worker FROM worker_temp_state WHERE worker=? AND pending=1 AND retry_at<=?',
                (worker_id, time.time())).fetchall()
        for row in rows:
            worker_id = row['worker']
            if worker_id not in self.tasks or self.tasks[worker_id].done():
                with self.store.transaction():
                    self.store.db.execute('UPDATE worker_temp_state SET pending=2 WHERE worker=?', (worker_id,))
                task = asyncio.create_task(self.cleanup(worker_id))
                self.tasks[worker_id] = task
                task.add_done_callback(lambda done, key=worker_id: self.completed(key, done))

    def completed(self, worker_id, task):
        if self.tasks.get(worker_id) is task:
            self.tasks.pop(worker_id)
        if not self.closed and not task.cancelled():
            asyncio.get_running_loop().call_soon(self.schedule)

    def retry(self, worker_id, error=None):
        state = self.state(worker_id)
        attempts = state['attempts'] + int(error is not None)
        previous_delay = self.retry_delays.get(worker_id, 1) if state['retry_at'] else 1
        delay = min(300, max(2 ** min(attempts, 9), previous_delay * 2))
        self.retry_delays[worker_id] = delay
        with self.store.transaction():
            self.store.db.execute('UPDATE worker_temp_state SET pending=?,attempts=?,retry_at=? WHERE worker=?',
                                  (int(error is None or attempts < RETRY_LIMIT), attempts,
                                   time.time() + delay, worker_id))
        if error is not None:
            if attempts >= RETRY_LIMIT:
                self.retry_delays.pop(worker_id, None)
            worker = dict(self.store.db.execute('SELECT topic,task FROM workers WHERE id=?', (worker_id,)).fetchone() or {})
            problems.record(self.store, 'worker', 'temp-cleanup-failed', str(error),
                            level=logging.ERROR if attempts >= RETRY_LIMIT else logging.WARNING,
                            every=None if attempts >= RETRY_LIMIT else 3600,
                            topic=worker.get('topic'), task=worker.get('task'), worker=worker_id)

    async def inspect(self, function, *args):
        if self.read_slots is None:
            self.read_slots = asyncio.Semaphore(4)
        async with self.read_slots:
            return await _background(function, *args)

    async def paths(self, worker, state, hosts):
        paths = await self.inspect(_scratch_paths, self.store.directory, worker, state, hosts)
        detached = set()
        for path in paths:
            if any(parent.is_symlink() for parent in (path.parent, path.parent.parent)):
                raise OSError('Symbolic link in worker scratch path')
            detached.update(await self.leftover_paths(path))
        return paths, detached

    async def cleanup(self, worker_id):
        try:
            state = self.state(worker_id)
            worker = self.worker(worker_id)
            epoch = state['epoch']
            repository_key = 'worker_temp_repositories:' + str(worker_id)
            roots_key = 'worker_temp_registration_roots:' + str(worker_id)
            repositories = set(self.store.get(repository_key, []))
            registration_roots = set(self.store.get(roots_key, []))
            if state['finished'] and worker['status'] in FINAL:
                from .close_saves import preserve_worker_proofs
                if not await preserve_worker_proofs(self.store, worker):
                    self.retry(worker_id)
                    return
                hosts = await self.host_paths(worker)
                if not await self.inspect(_processes_gone, hosts):
                    self.retry(worker_id)
                    return
                paths, detached = await self.paths(worker, state, hosts)
                registration_roots.update(str(path) for path in paths | detached)
                repositories.update(await self.inspect(_scratch_repositories, paths | detached))
                with self.store.transaction():
                    if sorted(repositories) != self.store.get(repository_key, []):
                        self.store.put(repository_key, sorted(repositories))
                    if sorted(registration_roots) != self.store.get(roots_key, []):
                        self.store.put(roots_key, sorted(registration_roots))
                worker = self.worker(worker_id)
                state = self.state(worker_id)
                if worker['status'] in FINAL and state['epoch'] == epoch:
                    for path in paths:
                        renamed = _detach(path)
                        if renamed is not None:
                            detached.add(renamed)
                self.remember(worker_id, detached)
            state = self.state(worker_id)
            staged = [Path(name) for name in json.loads(state['staged'])]
            registration_roots.update(str(path) for path in staged)
            repositories.update(await self.inspect(_scratch_repositories, staged))
            with self.store.transaction():
                if sorted(repositories) != self.store.get(repository_key, []):
                    self.store.put(repository_key, sorted(repositories))
                if sorted(registration_roots) != self.store.get(roots_key, []):
                    self.store.put(roots_key, sorted(registration_roots))
            for name in json.loads(state['staged']):
                if self.removal_slots is None:
                    self.removal_slots = asyncio.Semaphore(4)
                async with self.removal_slots:
                    if self.store.get('close_clears_space', False) is True:
                        from . import workspaces
                        async with workspaces._repository_lock(self.store.directory, 'closed-copy-deletion'):
                            path = Path(name)
                            if path.is_symlink():
                                await _background(_remove_tree, path)
                            else:
                                await workspaces.delete_tree(path, paced=True)
                    else:
                        await _background(_remove_tree, Path(name))
                current = self.state(worker_id)
                remaining = set(json.loads(current['staged'])) - {name}
                with self.store.transaction():
                    self.store.db.execute('UPDATE worker_temp_state SET staged=? WHERE worker=?',
                                          (json.dumps(sorted(remaining)), worker_id))
            from . import workspaces
            for repository in sorted(repositories):
                async with workspaces._repository_lock(self.store.directory, repository):
                    await self.inspect(_prune_scratch_registrations, repository, registration_roots)
                repositories.remove(repository)
                with self.store.transaction():
                    if repositories:
                        self.store.put(repository_key, sorted(repositories))
                    else:
                        self.store.db.execute('DELETE FROM settings WHERE key=?', (repository_key,))
            current = self.state(worker_id)
            pending = bool(json.loads(current['staged'])) or current['epoch'] != epoch
            with self.store.transaction():
                self.store.db.execute('UPDATE worker_temp_state SET pending=?,retry_at=0,attempts=0 WHERE worker=?',
                                      (int(pending), worker_id))
                if not pending:
                    self.store.db.executemany('DELETE FROM settings WHERE key=?',
                                             ((key,) for key in (repository_key, roots_key)))
                    self.store.host_logs_changed(worker=worker_id)
            self.retry_delays.pop(worker_id, None)
        except Exception as error:
            self.retry(worker_id, error)

    async def prepare(self, worker_id):
        path = temp_directory(worker_id)
        with self.store.transaction():
            self.store.db.execute('INSERT OR IGNORE INTO worker_temp_state(worker) VALUES (?)', (worker_id,))
        state = self.state(worker_id)
        if state['finished']:
            hosts = await self.host_paths(self.worker(worker_id))
            if not await self.inspect(_processes_gone, hosts):
                raise OSError('Previous worker processes still need their temp folder')
            detached = _detach(path)
            self.remember(worker_id, [detached] if detached is not None else [])
        path.mkdir(mode=0o700, exist_ok=True)
        if path.is_symlink() or not path.is_dir():
            raise OSError('Unexpected worker scratch entry')
        path.chmod(0o700)
        with self.store.transaction():
            self.store.db.execute('UPDATE worker_temp_state SET path=?,finished=0,epoch=epoch+?,retry_at=0,attempts=0 WHERE worker=?',
                                  (str(path), int(bool(state['finished'])), worker_id))
        self.retry_delays.pop(worker_id, None)
        self.schedule(worker_id)
        return str(path)


def storage_for(store):
    if not hasattr(store, '_worker_storage'):
        store._worker_storage = WorkerStorage(store)
    return store._worker_storage


async def prepare_temp(store, worker_id):
    return await storage_for(store).prepare(worker_id)
