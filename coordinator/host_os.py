"""Operating-system integration for Torii and the standalone update watchdog."""

import asyncio
import ctypes as C
from dataclasses import dataclass
import errno
import json
import math
import os
from pathlib import Path
import platform
import plistlib
import pwd
import re
import select
import shutil
import signal
import stat
import subprocess
import sys
import time

SYSTEM = sys.platform
TRASH_SECONDS = 86400
TRIM_TRASH_PREFIX = 'torii-trim-task-'
ORPHAN_TRASH_PREFIX = 'torii-orphan-'


def _deletion_call(operation, location, function, *args, **kwargs):
    try:
        return function(*args, **kwargs)
    except OSError as error:
        if not getattr(error, 'operation', None):
            error.operation = operation
            error.strerror = operation + ': ' + (error.strerror or str(error))
        error.filename = str(location)
        raise


def _deletion_directory(parent, name, flags, device=None, info=None, path=None):
    path = path if path is not None else name
    info = info if info is not None else _deletion_call(
        'stat', path, os.stat, name, dir_fd=parent, follow_symlinks=False)
    if not stat.S_ISDIR(info.st_mode):
        raise OSError(errno.ENOTDIR, 'Deletion requires a real directory', name)
    if device is not None and info.st_dev != device:
        raise OSError(errno.EXDEV, 'Deletion cannot cross filesystems')
    try:
        fd = _deletion_call('open', path, os.open, name, flags, dir_fd=parent)
    except PermissionError:
        _deletion_call('chmod', path, os.chmod, name, stat.S_IMODE(info.st_mode) | 0o700,
                       dir_fd=parent, follow_symlinks=False)
        fd = _deletion_call('open', path, os.open, name, flags, dir_fd=parent)
    try:
        current = _deletion_call('fstat', path, os.fstat, fd)
        if (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino):
            raise OSError('Directory changed during deletion')
        if current.st_mode & 0o700 != 0o700:
            _deletion_call('fchmod', path, os.fchmod, fd, stat.S_IMODE(current.st_mode) | 0o700)
        current = _deletion_call('fstat', path, os.fstat, fd)
        named = _deletion_call('stat', path, os.stat, name, dir_fd=parent, follow_symlinks=False)
        if any((item.st_dev, item.st_ino) != (info.st_dev, info.st_ino) for item in (current, named)):
            raise OSError('Directory changed during deletion')
        if device is not None and current.st_dev != device:
            raise OSError(errno.EXDEV, 'Deletion cannot cross filesystems')
        return fd
    except BaseException:
        os.close(fd)
        raise


async def delete_tree(path, *, progress=None, report=None, paced=False):
    """Delete an owned directory in cancellable chunks using no-follow directory descriptors."""
    path = Path(path)
    progress = progress if progress is not None else {'bytes': 0, 'files': 0}
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    parent = _deletion_call('open', path.parent, os.open, str(path.parent), flags)
    stack = []
    try:
        try:
            root = _deletion_call('open-directory', path, _deletion_directory, parent, path.name, flags, path=path)
        except FileNotFoundError:
            return progress
        try:
            device = _deletion_call('fstat', path, os.fstat, root).st_dev
            entries = _deletion_call('scandir', path, os.scandir, root)
        except BaseException:
            os.close(root)
            raise
        stack.append((root, entries, parent, path.name, path))
        operations, started = 0, time.monotonic()
        while stack:
            fd, entries, owner, name, current_path = stack[-1]
            entry = _deletion_call('scandir', current_path, next, entries, None)
            if entry is None:
                entries.close()
                _deletion_call('rmdir', current_path, os.rmdir, name, dir_fd=owner)
                os.close(fd)
                stack.pop()
            else:
                entry_path = current_path / entry.name
                info = _deletion_call('stat', entry_path, os.stat, entry.name, dir_fd=fd, follow_symlinks=False)
                if stat.S_ISDIR(info.st_mode):
                    child = _deletion_call('open-directory', entry_path, _deletion_directory, fd,
                                           entry.name, flags, device, info, path=entry_path)
                    try:
                        children = _deletion_call('scandir', entry_path, os.scandir, child)
                    except BaseException:
                        os.close(child)
                        raise
                    stack.append((child, children, fd, entry.name, entry_path))
                else:
                    _deletion_call('unlink', entry_path, os.unlink, entry.name, dir_fd=fd)
                    progress['bytes'] += info.st_size
                    progress['files'] += 1
            operations += 1
            if operations >= (1000 if paced else 32) or time.monotonic() - started >= (0.1 if paced else 0.004):
                if report:
                    report(progress)
                await asyncio.sleep(0.2 if paced else 0)
                operations, started = 0, time.monotonic()
        return progress
    finally:
        for fd, entries, owner, name, current_path in reversed(stack):
            entries.close()
            os.close(fd)
        os.close(parent)


def _delete_tree_command(path):
    return [sys.executable, str(Path(__file__).resolve()), str(path)]


async def _read_deletion(process, progress):
    process.stdin.write((json.dumps(progress) + '\n').encode())
    await process.stdin.drain()
    process.stdin.close()
    error = None
    async for line in process.stdout:
        result = json.loads(line)
        progress.update(result['removed'])
        if result.get('error'):
            error = result['error']
    stderr = await process.stderr.read()
    code = await process.wait()
    if error:
        failure = OSError(error.get('errno') or errno.EIO, error['reason'], error.get('filename'))
        failure.operation = error.get('operation')
        raise failure
    if code:
        raise OSError(stderr.decode(errors='replace').strip() or 'Deletion process exited: ' + str(code))


async def _stop_deletion(spawn, reader):
    try:
        process = await spawn
    except Exception:
        return
    if process.returncode is None:
        try:
            process.terminate()
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(process.wait(), 0.08)
        except asyncio.TimeoutError:
            try:
                process.kill()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(process.wait(), 0.08)
            except asyncio.TimeoutError:
                process._transport.close()
    if reader:
        try:
            await asyncio.wait_for(asyncio.shield(reader), 0.04)
        except (Exception, asyncio.CancelledError):
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)


async def delete_tree_background(path, *, progress=None, paced=False):
    """Keep filesystem stalls outside the caller's loop and bound child shutdown."""
    progress = progress if progress is not None else {'bytes': 0, 'files': 0}
    env = {key: os.environ[key] for key in ('PATH', 'HOME', 'TMPDIR', 'PYTHONPYCACHEPREFIX',
                                          'PYTHONDONTWRITEBYTECODE') if key in os.environ}
    if paced:
        env['TORII_DELETE_PACED'] = '1'
    spawn = asyncio.ensure_future(asyncio.create_subprocess_exec(
        *_delete_tree_command(path), env=env, stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, start_new_session=True))
    reader = None
    try:
        process = await asyncio.shield(spawn)
        reader = asyncio.create_task(_read_deletion(process, progress))
        await asyncio.shield(reader)
        return progress
    except BaseException:
        cleanup = asyncio.create_task(_stop_deletion(spawn, reader))
        while True:
            try:
                await asyncio.shield(cleanup)
                break
            except asyncio.CancelledError:
                if cleanup.cancelled():
                    raise
        raise


def _delete_tree_main():
    progress = json.loads(sys.stdin.readline())

    def emit(state, error=None):
        print(json.dumps({'state': state, 'removed': progress, 'error': error}), flush=True)

    async def run():
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, asyncio.current_task().cancel)
        try:
            await delete_tree(sys.argv[1], progress=progress, report=lambda value: emit('running'),
                              paced=os.environ.get('TORII_DELETE_PACED') == '1')
        except asyncio.CancelledError:
            emit('cancelled')
        except Exception as error:
            emit('failed', {'reason': getattr(error, 'strerror', None) or str(error),
                            'errno': getattr(error, 'errno', None),
                            'operation': getattr(error, 'operation', None),
                            'filename': getattr(error, 'filename', None)})
        else:
            emit('done')

    asyncio.run(run())


def linux():
    return SYSTEM == 'linux'


def trash_directory():
    home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    return home / ('.local/share/Trash/files' if linux() else '.Trash')


def rename_exclusive(source, destination, *, source_fd=None):
    libc = C.CDLL(None, use_errno=True)
    if source_fd is not None and hasattr(libc, 'renameatx_np'):
        result = libc.renameatx_np(source_fd, os.fsencode(source), -2, os.fsencode(destination), 4)
    elif source_fd is None and hasattr(libc, 'renamex_np'):
        function = libc.renamex_np
        result = function(os.fsencode(source), os.fsencode(destination), 4)
    elif hasattr(libc, 'renameat2'):
        function = libc.renameat2
        result = function(-100 if source_fd is None else source_fd, os.fsencode(source), -100, os.fsencode(destination), 1)
    else:
        raise OSError(errno.ENOTSUP, 'Exclusive rename is unavailable')
    if result:
        number = C.get_errno()
        raise OSError(number, os.strerror(number), str(destination))


def move_to_trash(path, directory=None, *, name=None, allow_files=False, source_fd=None):
    """Rename into this filesystem's Trash without replacing a namesake."""
    path = Path(path)
    source = path.lstat() if source_fd is None else os.stat(path.name, dir_fd=source_fd, follow_symlinks=False)
    if not allow_files and not stat.S_ISDIR(source.st_mode):
        raise OSError(errno.ENOTDIR, 'Trash source must be a directory', str(path))
    directory = Path(directory) if directory is not None else trash_directory()
    ancestor = directory
    while not ancestor.exists() and ancestor != ancestor.parent:
        ancestor = ancestor.parent
    if source.st_dev != ancestor.stat().st_dev:
        raise OSError(errno.EXDEV, 'Trash is on another filesystem', str(path))
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    if source.st_dev != directory.stat().st_dev:
        raise OSError(errno.EXDEV, 'Trash is on another filesystem', str(path))
    name = path.name if name is None else name
    target = directory / name
    suffix = 1
    while True:
        try:
            if source_fd is None:
                rename_exclusive(path, target)
            else:
                rename_exclusive(path.name, target, source_fd=source_fd)
            return target
        except FileExistsError:
            target = directory / (name + '-' + str(suffix))
            suffix += 1


def expire_trash(now=None, directory=None):
    """Delete trim and orphan Trash entries once their move into the Trash is TRASH_SECONDS old.
    Age is the ctime of the top-level entry, which the move sets. Other entries are never touched. Return bytes freed."""
    now = time.time() if now is None else now
    directory = Path(directory) if directory is not None else trash_directory()
    counts = {'bytes': 0, 'files': 0}
    try:
        entries = list(os.scandir(directory))
    except FileNotFoundError:
        return 0
    metadata = directory.parent / 'info' if directory.name == 'files' else None
    for entry in entries:
        if not entry.name.startswith((TRIM_TRASH_PREFIX, ORPHAN_TRASH_PREFIX)):
            continue
        info = entry.stat(follow_symlinks=False)
        if info.st_ctime > now - TRASH_SECONDS:
            continue
        if stat.S_ISDIR(info.st_mode):
            asyncio.run(delete_tree(entry.path, progress=counts, paced=True))
        else:
            os.unlink(entry.path)
            counts['bytes'] += info.st_size
        if metadata is not None and not metadata.is_symlink():
            (metadata / (entry.name + '.trashinfo')).unlink(missing_ok=True)
    return counts['bytes']


def prepare_trash():
    if linux():
        directory = trash_directory()
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        directory.chmod(0o700)


def proc_text(name):
    try:
        return (Path('/proc') / name).read_text()
    except OSError:
        return ''


def macos():
    return SYSTEM == 'darwin'


def _linux_family():
    return SYSTEM.startswith('linux')


class Unique(C.Structure):
    _fields_ = [('uuid', C.c_ubyte * 16), ('unique', C.c_uint64),
                ('parent_unique', C.c_uint64), ('version', C.c_int32),
                ('parent_version', C.c_int32), ('reserved', C.c_uint64 * 2)]


class Coalition(C.Structure):
    _fields_ = [('ids', C.c_uint64 * 2), ('reserved', C.c_uint64 * 3)]


class Bsd(C.Structure):
    _fields_ = [('pid', C.c_uint32), ('ppid', C.c_uint32), ('pgid', C.c_uint32),
                ('status', C.c_uint32), ('comm', C.c_char * 16), ('flags', C.c_uint32),
                ('uid', C.c_uint32), ('gid', C.c_uint32), ('ruid', C.c_uint32),
                ('rgid', C.c_uint32), ('svuid', C.c_uint32), ('svgid', C.c_uint32),
                ('reserved', C.c_uint32)]


@dataclass(frozen=True)
class Process:
    pid: int
    unique: int
    version: int
    resource: int
    jetsam: int
    uid: int
    gid: int = 0


class DarwinBackend:
    def __init__(self):
        self._lib = None

    def load(self):
        if not macos():
            raise OSError('Darwin process ownership is unavailable')
        if self._lib is None:
            lib = C.CDLL(None, use_errno=True)
            lib.proc_pidinfo.argtypes = [C.c_int, C.c_int, C.c_uint64, C.c_void_p, C.c_int]
            lib.proc_pidinfo.restype = C.c_int
            lib.proc_listpids.argtypes = [C.c_uint32, C.c_uint32, C.c_void_p, C.c_int]
            lib.proc_listpids.restype = C.c_int
            lib.proc_signal_with_audittoken.argtypes = [C.c_void_p, C.c_int]
            lib.proc_signal_with_audittoken.restype = C.c_int
            lib.sysctlbyname.argtypes = [C.c_char_p, C.c_void_p, C.POINTER(C.c_size_t),
                                         C.c_void_p, C.c_size_t]
            lib.sysctlbyname.restype = C.c_int
            self._lib = lib
        return self._lib

    def uid(self):
        return os.getuid()

    def pid(self):
        return os.getpid()

    def boot(self):
        value = C.create_string_buffer(128)
        size = C.c_size_t(len(value))
        if self.load().sysctlbyname(b'kern.bootsessionuuid', value, C.byref(size), None, 0):
            raise OSError(C.get_errno(), 'Cannot read boot identity')
        result = value.value.decode('ascii')
        if not result:
            raise OSError('Empty boot identity')
        return result

    def metadata(self, pid):
        lib = self.load()
        for attempt in range(METADATA_ATTEMPTS):
            unique, coalition, bsd, again = Unique(), Coalition(), Bsd(), Unique()
            for flavor, value in ((17, unique), (20, coalition), (13, bsd), (17, again)):
                C.set_errno(0)
                length = lib.proc_pidinfo(pid, flavor, 0, C.byref(value), C.sizeof(value))
                if length != C.sizeof(value):
                    code = C.get_errno()
                    if code == errno.ESRCH:
                        return None
                    raise OSError(code, 'Cannot read process identity')
            if (unique.unique, unique.version) == (again.unique, again.version):
                return Process(pid, unique.unique, unique.version, coalition.ids[0], coalition.ids[1],
                               bsd.uid, bsd.gid)
        raise OSError('Process identity changed during scan')

    def processes(self):
        lib = self.load()
        capacity = 4096
        while capacity <= 1048576:
            buf = (C.c_int * capacity)()
            length = lib.proc_listpids(4, self.uid(), buf, C.sizeof(buf))
            if length <= 0:
                raise OSError(C.get_errno(), 'Cannot enumerate processes')
            if length < C.sizeof(buf):
                return [info for pid in buf[:length // C.sizeof(C.c_int)] if pid > 0
                        for info in [self.metadata(pid)] if info is not None]
            capacity *= 2
        raise OSError('Process enumeration exceeded its bound')

    def signal(self, process, number):
        token = (C.c_uint32 * 8)(process.uid, process.uid, process.gid, process.uid,
                                process.gid, process.pid, 0, process.version)
        return self.load().proc_signal_with_audittoken(C.byref(token), number)

    def clock(self):
        return time.monotonic()

    def sleep(self, duration):
        time.sleep(duration)

    def _launchctl(self, *args):
        return subprocess.run(launchctl_command(*args, absolute=True), env={'PATH': '/usr/bin:/bin:/usr/sbin:/sbin'},
                              stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, timeout=15, text=True)

    def bootstrap(self, path):
        try:
            result = self._launchctl('bootstrap', 'gui/' + str(self.uid()), str(path))
        except subprocess.TimeoutExpired as error:
            raise OSError('Worker host launchd bootstrap timed out') from error
        if result.returncode:
            raise OSError('Worker host launchd bootstrap failed (%s)' % result.returncode)

    def job_state(self, label):
        try:
            result = self._launchctl('print', 'gui/%s/%s' % (self.uid(), label))
        except subprocess.TimeoutExpired:
            return 'missing'
        if result.returncode:
            return 'missing'
        lines = [line.strip() for line in result.stdout.splitlines()]
        if any(line.startswith('pid = ') for line in lines):
            return 'running'
        if any(line.startswith('last terminating signal = ') or
               (line.startswith('last exit code = ') and line != 'last exit code = (never exited)')
               for line in lines):
            return 'exited'
        return 'pending'

    def bootout(self, label):
        try:
            result = self._launchctl('bootout', 'gui/%s/%s' % (self.uid(), label))
            if result.returncode and not self._launchctl('print', 'gui/%s/%s' % (self.uid(), label)).returncode:
                raise OSError('Worker host launchd bootout failed (%s)' % result.returncode)
        except subprocess.TimeoutExpired as error:
            raise OSError('Worker host launchd bootout timed out') from error


_GROUP_MEMBER_LIMIT = 4096
_PROCESS_SCAN_LIMIT = 65536
_PROCESS_SCAN_CHUNK = 64
METADATA_ATTEMPTS = 8


def _proc_stat(pid):
    value = (Path('/proc') / str(pid) / 'stat').read_text()
    prefix, separator, fields = value.rpartition(')')
    fields = fields.split()
    if not separator or int(prefix.split('(', 1)[0]) != pid or len(fields) < 20:
        raise OSError('Invalid process group metadata')
    return fields[0], int(fields[2]), int(fields[19])


def _linux_group_member(pid, pgid):
    try:
        state, group, started = _proc_stat(pid)
    except (FileNotFoundError, ProcessLookupError):
        return None
    if group != pgid:
        return None
    ended = False
    if state in ('Z', 'X', 'x'):
        if not hasattr(os, 'pidfd_open'):
            raise OSError(errno.ENOSYS, 'Process termination proof is unavailable')
        descriptor = os.pidfd_open(pid, 0)
        try:
            ended = bool(select.select((descriptor,), (), (), 0)[0])
            again = _proc_stat(pid)
            if again[1:] != (group, started):
                raise OSError('Process group identity changed during scan')
            ended = ended and again[0] in ('Z', 'X', 'x')
        finally:
            os.close(descriptor)
    return (pid, ('linux', started)), ended


async def group_snapshot(pgid, backend):
    result = {}
    if macos():
        lib = backend.load()
        buf = (C.c_int * (_GROUP_MEMBER_LIMIT + 1))()
        C.set_errno(0)
        length = lib.proc_listpids(2, pgid, buf, C.sizeof(buf))
        if length < 0 or (not length and C.get_errno()) or length % C.sizeof(C.c_int):
            raise OSError(C.get_errno(), 'Cannot enumerate process group')
        if length >= C.sizeof(buf):
            raise OSError('Process group enumeration exceeded its bound')
        for index, pid in enumerate(buf[:length // C.sizeof(C.c_int)]):
            if index % _PROCESS_SCAN_CHUNK == 0:
                await asyncio.sleep(0)
            if pid <= 0:
                continue
            before, value, after = Unique(), Bsd(), Unique()
            for flavor, item in ((17, before), (13, value), (17, after)):
                C.set_errno(0)
                size = C.sizeof(item)
                if lib.proc_pidinfo(pid, flavor, 1, C.byref(item), size) != size:
                    code = C.get_errno()
                    if code == errno.ESRCH:
                        raise OSError(errno.EAGAIN, 'Process disappeared during group scan')
                    raise OSError(code, 'Cannot read process group identity')
            if (before.unique, before.version) != (after.unique, after.version) or value.pid != pid:
                raise OSError('Process group identity changed during scan')
            if value.pgid == pgid:
                result[(pid, ('darwin', before.unique, before.version))] = value.status == 5
    elif _linux_family():
        with os.scandir('/proc') as entries:
            for index, entry in enumerate(entries):
                if index >= _PROCESS_SCAN_LIMIT:
                    raise OSError('Process enumeration exceeded its bound')
                if index % _PROCESS_SCAN_CHUNK == 0:
                    await asyncio.sleep(0)
                if not entry.name.isdecimal():
                    continue
                try:
                    member = _linux_group_member(int(entry.name), pgid)
                except (FileNotFoundError, ProcessLookupError) as error:
                    raise OSError(errno.EAGAIN, 'Process disappeared during group scan') from error
                if member is not None:
                    result[member[0]] = member[1]
                    if len(result) > _GROUP_MEMBER_LIMIT:
                        raise OSError('Process group enumeration exceeded its bound')
    else:
        raise OSError(errno.ENOSYS, 'Process group enumeration is unavailable')
    return result


def coalitions_available(backend):
    if not macos():
        return False
    try:
        backend.load()
        return True
    except (OSError, AttributeError):
        return False


def open_process_exit_watch(pid):
    if macos():
        queue = select.kqueue()
        try:
            descriptor = queue.fileno()
            event = select.kevent(pid, filter=select.KQ_FILTER_PROC,
                                  flags=select.KQ_EV_ADD | select.KQ_EV_ONESHOT,
                                  fflags=select.KQ_NOTE_EXIT)
            queue.control([event], 0, 0)
            return descriptor, queue
        except BaseException:
            queue.close()
            raise
    if hasattr(os, 'pidfd_open'):
        return os.pidfd_open(pid, 0), None
    raise OSError(errno.ENOSYS, 'Native process exit notifications are unavailable')


def close_process_exit_watch(descriptor, queue):
    if queue is not None:
        queue.close()
    elif descriptor is not None:
        os.close(descriptor)


def process_exit_event_ready(queue):
    return queue is None or any(event.fflags & select.KQ_NOTE_EXIT for event in queue.control(None, 1, 0))


def watched_process_ended(pid, backend):
    if not macos():
        return False
    value = Bsd()
    C.set_errno(0)
    size = C.sizeof(value)
    length = backend.load().proc_pidinfo(pid, 13, 1, C.byref(value), size)
    if length != size:
        raise OSError(C.get_errno(), 'Cannot verify watched process status')
    if value.pid != pid:
        raise OSError('Watched process identity changed')
    return value.status == 5


def boot_time():
    if linux():
        match = re.search(r'^btime (\d+)$', proc_text('stat'), re.MULTILINE)
        return int(match.group(1)) if match else None
    try:
        output = subprocess.run(['sysctl', '-n', 'kern.boottime'], capture_output=True, text=True,
                                check=True, timeout=1).stdout
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None
    match = re.search(r'\bsec\s*=\s*(\d+)', output)
    return int(match.group(1)) if match else None


def _probe_output(*args):
    try:
        return subprocess.run(args, capture_output=True, text=True, timeout=1, check=True).stdout
    except (OSError, subprocess.SubprocessError, UnicodeError):
        return ''


def _nonnegative_number(value, convert=float):
    try:
        result = convert(value)
        return result if result >= 0 and math.isfinite(result) else None
    except (TypeError, ValueError, OverflowError):
        return None


def read_pressure():
    """Numeric host values. Unavailable readings stay None, independently of other probes."""
    result = dict.fromkeys(('load1', 'cores', 'memory_used', 'memory_total', 'swap_used', 'pressure'))
    try:
        result['load1'] = _nonnegative_number(os.getloadavg()[0])
    except OSError:
        pass
    if linux():
        cores = _nonnegative_number(os.cpu_count(), int)
        memory = {key: _nonnegative_number(value, int) for key, value in re.findall(
            r'^(MemTotal|MemAvailable|SwapTotal|SwapFree):\s*(\d+) kB$', proc_text('meminfo'), re.M)}
        total = memory.get('MemTotal')
        result['memory_total'] = total * 1024 if total else None
        if total and memory.get('MemAvailable') is not None:
            result['memory_used'] = max(0, total - memory['MemAvailable']) * 1024
        if memory.get('SwapTotal') is not None and memory.get('SwapFree') is not None:
            result['swap_used'] = max(0, memory['SwapTotal'] - memory['SwapFree']) * 1024
        pressure = re.search(r'^some avg10=(\d+(?:\.\d+)?)(?:\s|$)',
                             proc_text('pressure/memory'), re.M)
    else:
        cores = _nonnegative_number(_probe_output('sysctl', '-n', 'hw.ncpu').strip(), int)
        total = _nonnegative_number(_probe_output('sysctl', '-n', 'hw.memsize').strip(), int)
        result['memory_total'] = total if total else None
        stats = _probe_output('vm_stat')
        page = re.search(r'page size of (\d+) bytes', stats)
        page_size = _nonnegative_number(page.group(1), int) if page else None
        pages = {key: _nonnegative_number(value.rstrip('.'), int) for key, value in
                 re.findall(r'^Pages (free|inactive|speculative):\s+(\d+\.?)[\s]*$', stats, re.M)}
        if total and page_size and pages.get('free') is not None:
            available = sum(pages.get(key) or 0 for key in ('free', 'inactive', 'speculative')) * page_size
            result['memory_used'] = max(0, total - available)
        pressure = re.search(r'System-wide memory free percentage:\s*(\d+)%', _probe_output('memory_pressure', '-Q'))
        swapped = re.search(r'used\s*=\s*(\d+(?:\.\d+)?)([KMG])(?:\s|$)',
                            _probe_output('sysctl', '-n', 'vm.swapusage'))
        if swapped:
            amount = _nonnegative_number(swapped.group(1))
            if amount is not None:
                result['swap_used'] = _nonnegative_number(amount * 1024 ** ('KMG'.index(swapped.group(2)) + 1), int)
    result['cores'] = cores if cores else None
    if pressure:
        value = _nonnegative_number(pressure.group(1))
        result['pressure'] = value if value is not None and value <= 100 else None
    return result


def disk_usage_output(path):
    return _probe_output('df', '-kP', str(path))


def pressure_detail(value):
    if linux():
        return f', {value:.2f}% pressure stall'
    return f', {value:g}% pressure free'


def exchange_paths(left, right):
    library = C.CDLL(None, use_errno=True)
    if macos():
        rename = library.renamex_np
        rename.argtypes = [C.c_char_p, C.c_char_p, C.c_uint]
        args = (os.fsencode(left), os.fsencode(right), 2)
    elif _linux_family() and hasattr(library, 'renameat2'):
        rename = library.renameat2
        rename.argtypes = [C.c_int, C.c_char_p, C.c_int, C.c_char_p, C.c_uint]
        args = (-100, os.fsencode(left), -100, os.fsencode(right), 2)
    else:
        raise RuntimeError('Atomic tool installation is not supported on this host')
    rename.restype = C.c_int
    if rename(*args):
        error = C.get_errno()
        raise OSError(error, os.strerror(error))


def service_restart_command():
    if linux():
        return ['systemctl', '--user', 'restart', 'local.telegram-agent-coordinator.service']
    return launchctl_command('kickstart', '-k', launchd_gui_domain() + '/local.telegram-agent-coordinator')


def service_manager_status():
    if macos():
        return 'launchd', True
    if not _linux_family():
        return 'macOS or Linux required', False
    if os.getuid() == 0:
        return 'Linux root is refused', False
    try:
        working = subprocess.run(['systemctl', '--user', 'show-environment'],
                                 capture_output=True, timeout=10).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        working = False
    return ('systemd user service, linger on at install' if working else
            'unavailable; see scripts/run-service.sh'), working


def browser_command(url):
    browser = os.environ.get('BROWSER')
    if browser:
        return [browser, url]
    if macos():
        return ['open', url]
    if os.environ.get('DISPLAY') or os.environ.get('WAYLAND_DISPLAY'):
        return ['xdg-open', url]
    return None


def interactive_mac():
    return (macos() and sys.stdin.isatty() and sys.stdout.isatty()
            and not any(os.environ.get(key) for key in ('SSH_CONNECTION', 'SSH_CLIENT', 'SSH_TTY')))


def open_mac_url(url):
    try:
        subprocess.run(['/usr/bin/open', url], stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL, timeout=10, check=False)
    except (OSError, subprocess.SubprocessError):
        pass


def javascript_helper(path):
    return ['/usr/bin/osascript', '-l', 'JavaScript', str(path)]


def launchctl_command(*args, absolute=False):
    return ['/bin/launchctl' if absolute else 'launchctl', *args]


def launchd_gui_domain():
    return 'gui/%d' % os.getuid()


def user_launch_agents(home):
    return home / 'Library' / 'LaunchAgents'


def system_launch_agents():
    return Path('/Library/LaunchAgents')


def system_launch_daemons():
    return Path('/Library/LaunchDaemons')


def systemd_path(value):
    value = str(value)
    if any(char in value for char in ('\n', '\r', '\x00')):
        raise ValueError('Invalid systemd path or argument')
    return value.replace('%', '%%')


def systemd_quote(value):
    value = systemd_path(value)
    return '"' + value.replace('\\', '\\\\').replace('"', '\\"').replace('$', '$$') + '"'


def install_systemd(root, state, command, *, write_only):
    if os.getuid() == 0:
        raise SystemExit('Refusing to install Torii as root. Use a normal user with linger.')
    os.umask(0o077)
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    state.chmod(0o700)
    label = 'local.telegram-agent-coordinator.service'
    target = Path.home() / '.config/systemd/user' / label
    target.parent.mkdir(parents=True, exist_ok=True)
    unit = '\n'.join([
        '[Unit]', 'Description=Torii Telegram agent coordinator', '', '[Service]',
        'ExecStart=' + ' '.join(systemd_quote(arg) for arg in command),
        'WorkingDirectory=' + systemd_path(root), 'Restart=always', 'RestartSec=10',
        'KillMode=process', 'UMask=0077',
        'Environment="PATH=%h/.local/bin:/usr/local/bin:/usr/bin:/bin"',
        'StandardOutput=append:' + systemd_path(state / 'service.stdout.log'),
        'StandardError=append:' + systemd_path(state / 'service.stderr.log'),
        '', '[Install]', 'WantedBy=default.target', '',
    ])
    target.write_text(unit)
    target.chmod(0o600)
    print(f'Wrote {target}')
    if write_only:
        return
    subprocess.run(['loginctl', 'enable-linger'], check=True)
    subprocess.run(['systemctl', '--user', 'daemon-reload'], check=True)
    active = subprocess.run(['systemctl', '--user', 'is-active', '--quiet', label],
                            capture_output=True).returncode == 0
    subprocess.run(['systemctl', '--user', 'enable', '--now', label], check=True)
    if active:
        subprocess.run(['systemctl', '--user', 'restart', label], check=True)


def unload_launchd(domain, label):
    target = domain + "/" + label
    prior = subprocess.run(launchctl_command("print", target), capture_output=True, text=True)
    match = re.search(r"^\s*pid = (\d+)\s*$", prior.stdout, re.MULTILINE)
    pid = int(match.group(1)) if match else None
    subprocess.run(launchctl_command("bootout", target), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.monotonic() + 40
    while True:
        alive = False
        if pid:
            try:
                os.kill(pid, 0)
                alive = True
            except ProcessLookupError:
                pass
        registered = subprocess.run(launchctl_command("print", target),
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
        if not alive and not registered:
            return
        if time.monotonic() >= deadline:
            raise SystemExit("Old service has not stopped. Inspect its process before rerunning the installer.")
        time.sleep(0.2)


def external_drive_hint(state):
    paths = (state, Path.home() / '.config/telegram-agent-coordinator/bot-token',
             Path(sys.executable), Path(shutil.which('claude') or Path.home() / '.local/bin/claude'))
    if any(path.resolve().is_relative_to("/Volumes") for path in paths):
        return (' macOS stops background services from using files on external drives. '
                'Keep Torii folders and Claude Code on the internal disk, or run scripts/run-service.sh in a terminal.')
    return ''


def wait_for_launchd_start(state, domain, label, service_running, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if service_running(state):
            return
        time.sleep(0.25)
    result = subprocess.run(launchctl_command('print', domain + '/' + label), capture_output=True, text=True)
    detail = ''
    if result.returncode == 0:
        pid = re.search(r'^\s*pid = (\d+)\s*$', result.stdout, re.MULTILINE)
        exit_status = re.search(r'^\s*last exit (?:code|status) = (\d+)\s*$', result.stdout, re.MULTILINE)
        if pid:
            detail += ' launchd reported pid ' + pid.group(1) + '.'
        if exit_status:
            detail += ' Last exit status: ' + exit_status.group(1) + '.'
    subprocess.run(launchctl_command('bootout', domain + '/' + label),
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    raise ValueError('The background service did not start. Check the service log and rerun setup.'
                     + detail + external_drive_hint(state))


def _install_launchd(root, state, command, service_running, *, write_only):
    os.umask(0o077)
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    state.chmod(0o700)
    label = "local.telegram-agent-coordinator"
    target = user_launch_agents(Path.home()) / (label + ".plist")
    target.parent.mkdir(parents=True, exist_ok=True)
    environment = {"PATH": "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"}
    if "BROWSER" in os.environ:
        environment["BROWSER"] = os.environ["BROWSER"]
    data = {
        "Label": label, "ProgramArguments": command, "WorkingDirectory": str(root),
        "RunAtLoad": True, "KeepAlive": True, "ThrottleInterval": 10,
        "AbandonProcessGroup": True,
        "ProcessType": "Interactive", "Umask": 0o077,
        "EnvironmentVariables": environment,
        "StandardOutPath": str(state / "service.stdout.log"),
        "StandardErrorPath": str(state / "service.stderr.log"),
    }
    with target.open("wb") as stream:
        plistlib.dump(data, stream)
    target.chmod(0o600)
    print(f"Wrote {target}")
    if write_only:
        return
    domain = launchd_gui_domain()
    unload_launchd(domain, label)
    subprocess.run(launchctl_command("bootstrap", domain, str(target)), check=True)
    wait_for_launchd_start(state, domain, label, service_running)


def install_service(root, state, command, service_running, *, write_only):
    if linux():
        return install_systemd(root, state, command, write_only=write_only)
    if not macos():
        raise SystemExit("This installer requires macOS. Use run-service.sh on other systems.")
    return _install_launchd(root, state, command, service_running, write_only=write_only)


def path_case_key(path):
    path = str(path)
    return path.casefold() if macos() else path


def trash_path_key(path):
    path = str(path)
    return path if linux() else path.casefold()


def mount_point(path):
    return linux() and os.path.ismount(path)


def volume_trash_directory(root):
    return root / ('.Trash-%d' % os.getuid()) / 'files'


def short_socket_directory():
    return Path('/tmp')


def chmod_directory(name, mode, parent):
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


def user_lsof_paths(owners=False):
    binary = '/usr/sbin/lsof' if os.path.isfile('/usr/sbin/lsof') else '/usr/bin/lsof'
    result = subprocess.run([binary, '-nP', '-a', '-u', str(os.getuid()), '-Fn'],
                            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                            env={'PATH': '/usr/bin:/bin:/usr/sbin:/sbin'}, text=True, timeout=30)
    if result.returncode:
        raise OSError('Process path probe failed')
    if not owners:
        return [Path(line[1:]) for line in result.stdout.splitlines() if line.startswith('n/')]
    paths, pid = {}, None
    for line in result.stdout.splitlines():
        if line.startswith('p'):
            pid = line[1:]
        elif line.startswith('n/'):
            paths[Path(line[1:])] = pid
    return paths


def launch_agent_references(folders, open_directory):
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
                    values = [value.get(key) for key in _LAUNCH_PATH_FIELDS]
                    values.extend(value.get('ProgramArguments', []))
                    yield folder / entry.name, tuple(v for v in values if isinstance(v, str))
        finally:
            os.close(descriptor)


_LAUNCH_PATH_FIELDS = ('WorkingDirectory', 'Program', 'StandardOutPath', 'StandardErrorPath')


def launch_agent_texts(launch):
    try:
        launch_info = launch.lstat()
    except FileNotFoundError:
        launch_info = None
    if launch_info and stat.S_ISDIR(launch_info.st_mode):
        with os.scandir(launch) as entries:
            for entry in entries:
                if not entry.name.endswith('.plist') or not stat.S_ISREG(entry.stat(follow_symlinks=False).st_mode):
                    continue
                fd = os.open(entry.path, os.O_RDONLY | os.O_NOFOLLOW)
                with os.fdopen(fd, 'rb') as handle:
                    yield str(plistlib.load(handle))


def proc_links(process, manager):
    fd = process / 'fd'
    uid = os.getuid()
    try:
        if fd.stat().st_uid != uid:
            return
        for link in ['cwd', 'root', 'exe'] + ['fd/' + name for name in os.listdir(fd)]:
            try:
                yield link, os.readlink(process / link).removesuffix(' (deleted)')
            except (FileNotFoundError, ProcessLookupError):
                continue
    except PermissionError:
        if not manager and fd.stat().st_uid == uid:
            raise


def proc_paths(process, *, include_command=True):
    """Read accessible proc links and argv. The user manager's units are checked separately."""
    args = [os.fsdecode(arg) for arg in (process / 'cmdline').read_bytes().split(b'\0')]
    manager = Path(args[0]).name == 'systemd' and '--user' in args
    cwd = None
    for link, target in proc_links(process, manager):
        if link == 'cwd':
            cwd = target
        yield target
    if not include_command:
        return
    for path in args:
        if path.startswith('-') and '=' in path:
            path = path.split('=', 1)[1]
        if path.startswith('/'):
            yield os.path.normpath(path)
        elif cwd and '/' in path and not path.startswith('-'):
            yield os.path.normpath(os.path.join(cwd, path))


def open_paths(root, owners=False, *, skip_commands=()):
    """Find paths at or under root used by processes of this user."""
    root = path_case_key(root).rstrip('/') or '/'
    prefix = root.rstrip('/') + '/'
    if linux():
        found, uid = {}, os.getuid()
        with os.scandir('/proc') as entries:
            for entry in entries:
                if not entry.name.isdigit():
                    continue
                try:
                    if entry.stat().st_uid == uid:
                        for path in proc_paths(Path(entry.path), include_command=entry.name not in skip_commands):
                            if path == root or path.startswith(prefix):
                                found[path] = entry.name
                except (FileNotFoundError, ProcessLookupError):
                    continue
    else:
        found = {str(path): pid for path, pid in user_lsof_paths(owners=True).items()
                 if path_case_key(path) == root or path_case_key(path).startswith(prefix)}
    return found if owners else list(found)


SYSTEMD_TRIGGERS = ('.timer', '.path', '.socket')
SYSTEMD_UNITS = ('.service', '.mount', '.target') + SYSTEMD_TRIGGERS
SYSTEMD_ENABLED = ('enabled', 'enabled-runtime')


class SystemctlProbeError(RuntimeError):
    """The systemctl subprocess failed."""


def systemd_paths(home, runtime):
    config = Path(os.environ.get('XDG_CONFIG_HOME') or home / '.config')
    data = Path(os.environ.get('XDG_DATA_HOME') or home / '.local/share')
    return (config / 'systemd/user', config / 'systemd/user.control', data / 'systemd/user',
            Path('/etc/systemd/user'), Path('/run/systemd/user'), Path('/usr/local/lib/systemd/user'),
            Path('/usr/lib/systemd/user'), *(runtime / 'systemd' / name for name in
                                              ('user', 'user.control', 'generator', 'generator.early', 'generator.late')))


def systemd_files(directories):
    def check_error(error):
        if not isinstance(error, FileNotFoundError):
            raise error

    for directory in directories:
        for folder, _, names in os.walk(directory, onerror=check_error):
            for name in names:
                path = Path(folder) / name
                if path.suffix in SYSTEMD_UNITS:
                    yield name, path
                elif path.suffix == '.conf' and path.parent.name.endswith('.d'):
                    yield path.parent.name.removesuffix('.d'), path


def systemd_contents(paths, exclude=None):
    for path in dict.fromkeys(paths):
        try:
            resolved = path.resolve()
            if (exclude is None or not resolved.is_relative_to(exclude)) and stat.S_ISREG(resolved.stat().st_mode):
                yield str(resolved) + '\n' + resolved.read_text(errors='replace')
        except FileNotFoundError:
            continue


def systemctl_output(systemctl, *args):
    try:
        return subprocess.run([systemctl, '--user', '--no-pager', *args], capture_output=True,
                              text=True, errors='replace', timeout=2, check=True).stdout
    except (OSError, subprocess.SubprocessError) as error:
        raise SystemctlProbeError() from error


def systemd_states(systemctl, names):
    properties = ('Id,Names,LoadState,ActiveState,SubState,UnitFileState,Transient,Triggers,'
                  'FragmentPath,DropInPaths,ExecStart,WorkingDirectory,Accept')
    output = systemctl_output(systemctl, 'show', '--property=' + properties, '--', *names)
    states = {}
    for block in output.split('\n\n'):
        unit = dict(line.split('=', 1) for line in block.splitlines() if '=' in line)
        if unit.get('Id'):
            states[unit['Id']] = unit
    return states


def systemd_active(unit):
    return (unit.get('LoadState') == 'loaded' and unit.get('SubState') not in ('dead', 'failed')
            and unit.get('ActiveState') in ('active', 'activating', 'reloading', 'refreshing', 'deactivating'))


def systemd_trigger_target(name, content):
    socket = name.endswith('.socket')
    option = 'Service' if socket else 'Unit'
    explicit = re.findall(r'^[ \t]*' + option + r'[ \t]*=([^\r\n]*)', content, re.MULTILINE)
    if explicit and explicit[-1].strip():
        return explicit[-1].strip().strip('"\'')
    accept = re.findall(r'^[ \t]*Accept[ \t]*=([^\r\n]*)', content, re.MULTILINE) if socket else []
    template = '@' if accept and accept[-1].strip().lower() in ('yes', 'true', '1', 'on') else ''
    return name.rsplit('.', 1)[0] + template + '.service'


def systemd_file_references(directories, transient, targets=None):
    files, enabled = {}, set()
    for name, path in systemd_files(directories):
        files.setdefault(name, []).append(path)
        if name.endswith(SYSTEMD_TRIGGERS) and path.parent.name.endswith(('.wants', '.requires')) and path.is_symlink():
            enabled.add(name)
    pending = set(targets) if targets is not None else enabled
    seen = set()
    while pending:
        name = pending.pop()
        if name in seen:
            continue
        seen.add(name)
        content = '\n'.join(systemd_contents(files.get(name, ()), exclude=transient))
        if content:
            yield name, content
            if name.endswith(SYSTEMD_TRIGGERS):
                pending.add(systemd_trigger_target(name, content))


def systemd_live_references(systemctl, directories, transient):
    enabled = {line.split()[0] for line in systemctl_output(systemctl, 'list-unit-files', '--no-legend',
                '--type=timer,path,socket', '--state=enabled,enabled-runtime').splitlines() if line.strip()}
    units = systemd_states(systemctl, (*(f'*{suffix}' for suffix in SYSTEMD_UNITS),
                                       *(name for name in sorted(enabled) if '@.' not in name)))
    active = {name for name, unit in units.items() if systemd_active(unit)}
    pending = active | {name for name, unit in units.items() if name.endswith(SYSTEMD_TRIGGERS) and
                        (unit.get('UnitFileState') in SYSTEMD_ENABLED or enabled.intersection(unit.get('Names', name).split()))}
    fallback, seen = {name for name in enabled if '@.' in name}, set()
    while pending:
        names = sorted(pending - seen)
        seen.update(names)
        pending.clear()
        missing = [name for name in names if name not in units and '@.' not in name]
        if missing:
            loaded = systemd_states(systemctl, missing)
            units.update(loaded)
            units.update((alias, unit) for unit in loaded.values() for alias in unit.get('Names', '').split())
        for name in names:
            unit = units.get(name)
            if unit is None:
                fallback.add(name)
                continue
            if unit.get('LoadState') != 'loaded':
                continue
            fragment = Path(unit.get('FragmentPath') or '.')
            if not systemd_active(unit) and (unit.get('Transient') == 'yes' or fragment.is_relative_to(transient)):
                continue
            dropins = re.findall(r'(?:\\.|[^\s])+', unit.get('DropInPaths', ''))
            paths = ([fragment] if fragment.is_absolute() else []) + [Path(path.replace(r'\x20', ' ')) for path in dropins]
            content = '\n'.join([*systemd_contents(paths), *('%s=%s' % pair for pair in unit.items())])
            yield name, content
            if name.endswith(SYSTEMD_TRIGGERS):
                targets = unit.get('Triggers', '').split() or [systemd_trigger_target(name, content)]
                pending.update(targets)
    if fallback:
        yield from systemd_file_references(directories, transient, fallback)


def systemd_references(*, fallback_on_error=False):
    if not linux():
        return
    home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    runtime = Path(os.environ.get('XDG_RUNTIME_DIR') or '/run/user/%d' % os.getuid())
    directories = systemd_paths(home, runtime)
    transient = runtime / 'systemd/transient'
    systemctl = shutil.which('systemctl')
    online = systemctl and ((runtime / 'systemd/private').exists() or os.environ.get('DBUS_SESSION_BUS_ADDRESS'))

    def references():
        if online:
            try:
                yield from systemd_live_references(systemctl, directories, transient)
                return
            except SystemctlProbeError as error:
                if not fallback_on_error:
                    raise error.__cause__ from None
        yield from systemd_file_references(directories, transient)

    for unit, content in references():
        yield unit, content.replace('%h/', str(home) + '/').replace('~/', str(home) + '/')


def installed_systemd_references():
    if linux():
        home = Path(pwd.getpwuid(os.getuid()).pw_dir)
        runtime = Path(os.environ.get('XDG_RUNTIME_DIR') or '/run/user/%d' % os.getuid())
        system = tuple(map(Path, ('/etc/systemd/system', '/run/systemd/system', '/usr/local/lib/systemd/system',
                                  '/usr/lib/systemd/system')))
        for unit, path in systemd_files((*systemd_paths(home, runtime), *system)):
            for content in systemd_contents([path]):
                yield unit, content.replace('%h/', str(home) + '/').replace('~/', str(home) + '/')


def process_commands(*, include_linux=False):
    if linux() and not include_linux:
        return ()
    result = subprocess.run(['ps', '-ww', '-axo', 'uid=,pid=,ppid=,command='], capture_output=True,
                            text=True, errors='replace', timeout=5, check=True)
    return [fields for line in result.stdout.splitlines() if len(fields := line.split(None, 3)) == 4]


def process_command_references(commands):
    return () if linux() else commands


async def worktree_process_paths(run_command):
    if linux():
        return {Path(path).resolve() for path in await asyncio.to_thread(open_paths, '/')}
    _, listing = await asyncio.wait_for(run_command(
        '/usr/sbin/lsof', '-nP', '-Fpn', reject_stderr=True, strip=False), 30)
    if '\ufffd' in listing:
        raise ValueError('Process paths could not be decoded safely')
    return {Path(line[1:]).resolve() for line in listing.splitlines()
            if line.startswith('n/')}


def browser_stub_names():
    return ('xdg-open', 'sensible-browser', 'x-www-browser') + (('open',) if macos() else ())


def bubblewrap_supported():
    return linux()


def native_session_ps_command():
    flags = ('-A', '-o') if linux() else ('-axo',)
    return ('ps', *flags, 'pid=,command=')


def uses_worker_coalitions():
    return macos()


def worker_ownership(*, group_owned, allow_coalition_without_group):
    if (group_owned or allow_coalition_without_group) and uses_worker_coalitions():
        return {'ownership': 'coalition'}
    if group_owned:
        return {'group_owned': True}
    return {}


def check_saved_claude_login(prompt):
    return prompt is not None or macos()


def managed_preference_paths(identifier):
    if not macos():
        return ()
    directory = Path('/Library/Managed Preferences')
    user = pwd.getpwuid(os.getuid()).pw_name or 'user'
    return (directory / user / (identifier + '.plist'), directory / (identifier + '.plist'))


def codex_npm_target():
    architecture = {'arm64': 'aarch64', 'aarch64': 'aarch64',
                    'x86_64': 'x86_64', 'amd64': 'x86_64'}.get(platform.machine().lower())
    system = {'darwin': 'apple-darwin', 'linux': 'unknown-linux-musl'}.get(SYSTEM)
    if not architecture or not system:
        return None
    target = architecture + '-' + system
    package = 'codex-' + SYSTEM + '-' + ('arm64' if architecture == 'aarch64' else 'x64')
    return package, target


def keychain_command(*args):
    return ['/usr/bin/security', *args]


def credential_reader(file_reader, keychain_reader):
    return file_reader if linux() else keychain_reader


async def close_server_listener(server):
    server.close()
    if not linux():
        await server.wait_closed()


async def _run_sips(*arguments):
    process = await asyncio.create_subprocess_exec('/usr/bin/sips', *map(str, arguments),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
    try:
        output, _ = await asyncio.wait_for(process.communicate(), 30)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        try:
            process.kill()
        except ProcessLookupError:
            pass
        await process.communicate()
        raise
    if process.returncode:
        raise RuntimeError('image conversion failed')
    return output


async def image_dimensions(path):
    output = await _run_sips('-g', 'pixelWidth', '-g', 'pixelHeight', path)
    return [int(value) for value in re.findall(rb'^\s*pixel(?:Width|Height):\s*(\d+)\s*$', output, re.MULTILINE)]


async def resize_jpeg(source, destination, max_side):
    await _run_sips('-Z', max_side, '--setProperty', 'format', 'jpeg',
                    '--setProperty', 'formatOptions', '80', source, '--out', destination)


def apple_silicon():
    return platform.system() == 'Darwin' and platform.machine() == 'arm64'


def apple_silicon_choice(apple, fallback):
    return apple if apple_silicon() else fallback


def apple_silicon_at_least(major):
    return apple_silicon() and int(platform.mac_ver()[0].split('.')[0] or 0) >= major


def apple_voice_unsupported():
    return platform.system() == 'Darwin' and not apple_silicon_at_least(14)


def supported_voice_platform():
    return platform.system() in ('Darwin', 'Linux')


def host_archive(archives):
    return archives[(platform.system(), platform.machine())]


def copy_model_file(source, target):
    try:
        os.link(source, target)
    except OSError:
        try:
            subprocess.run(['/bin/cp', '-c', source, target], check=True,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except (OSError, subprocess.SubprocessError):
            shutil.copy2(source, target)
    return target


if __name__ == '__main__':
    _delete_tree_main()
