"""Run-scoped Darwin coalitions and identity-checked process cleanup."""

import asyncio
import ctypes as C
from dataclasses import dataclass, replace
import errno
import json
import os
from pathlib import Path
import plistlib
import select as polling
import signal
import subprocess
import sys
import time


LABEL_PREFIX = 'local.torii-host.'
FREEZE_ROUNDS = 16
STOP_TIMEOUT = 5
GROUP_MEMBER_LIMIT = 4096
PROCESS_SCAN_LIMIT = 65536
PROCESS_SCAN_CHUNK = 64


class OwnershipUnavailable(RuntimeError):
    pass


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
        if sys.platform != 'darwin':
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
        unique, coalition, bsd, again = Unique(), Coalition(), Bsd(), Unique()
        for flavor, value in ((17, unique), (20, coalition), (13, bsd), (17, again)):
            C.set_errno(0)
            length = lib.proc_pidinfo(pid, flavor, 0, C.byref(value), C.sizeof(value))
            if length != C.sizeof(value):
                code = C.get_errno()
                if code == errno.ESRCH:
                    return None
                raise OSError(code, 'Cannot read process identity')
        if (unique.unique, unique.version) != (again.unique, again.version):
            raise OSError('Process identity changed during scan')
        return Process(pid, unique.unique, unique.version, coalition.ids[0], coalition.ids[1],
                       bsd.uid, bsd.gid)

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
        return subprocess.run(['/bin/launchctl', *args], env={'PATH': '/usr/bin:/bin:/usr/sbin:/sbin'},
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


backend = DarwinBackend()


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
            ended = bool(polling.select((descriptor,), (), (), 0)[0])
            again = _proc_stat(pid)
            if again[1:] != (group, started):
                raise OSError('Process group identity changed during scan')
            ended = ended and again[0] in ('Z', 'X', 'x')
        finally:
            os.close(descriptor)
    return (pid, ('linux', started)), ended


async def _group_snapshot(pgid):
    result = {}
    if sys.platform == 'darwin':
        lib = backend.load()
        buf = (C.c_int * (GROUP_MEMBER_LIMIT + 1))()
        C.set_errno(0)
        length = lib.proc_listpids(2, pgid, buf, C.sizeof(buf))
        if length < 0 or (not length and C.get_errno()) or length % C.sizeof(C.c_int):
            raise OSError(C.get_errno(), 'Cannot enumerate process group')
        if length >= C.sizeof(buf):
            raise OSError('Process group enumeration exceeded its bound')
        for index, pid in enumerate(buf[:length // C.sizeof(C.c_int)]):
            if index % PROCESS_SCAN_CHUNK == 0:
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
    elif sys.platform.startswith('linux'):
        with os.scandir('/proc') as entries:
            for index, entry in enumerate(entries):
                if index >= PROCESS_SCAN_LIMIT:
                    raise OSError('Process enumeration exceeded its bound')
                if index % PROCESS_SCAN_CHUNK == 0:
                    await asyncio.sleep(0)
                if not entry.name.isdecimal():
                    continue
                try:
                    member = _linux_group_member(int(entry.name), pgid)
                except (FileNotFoundError, ProcessLookupError) as error:
                    raise OSError(errno.EAGAIN, 'Process disappeared during group scan') from error
                if member is not None:
                    result[member[0]] = member[1]
                    if len(result) > GROUP_MEMBER_LIMIT:
                        raise OSError('Process group enumeration exceeded its bound')
    else:
        raise OSError(errno.ENOSYS, 'Process group enumeration is unavailable')
    return result


async def group_members(pgid):
    if not isinstance(pgid, int) or isinstance(pgid, bool) or pgid <= 1:
        raise OSError('Invalid process group identity')
    try:
        before = await _group_snapshot(pgid)
        after = await _group_snapshot(pgid)
        if not any(not ended for ended in after.values()) and not all(before.values()):
            before, after = after, await _group_snapshot(pgid)
    except (ValueError, IndexError) as error:
        raise OSError('Invalid process group metadata') from error
    members = tuple(sorted(key for key, ended in after.items() if not ended))
    ended = not members and bool(before) and all(before.values()) and after.keys() <= before.keys()
    if not members and not ended:
        raise OSError(errno.EAGAIN, 'Process group termination proof is incomplete')
    return members, ended


def supported():
    if sys.platform != 'darwin':
        return False
    try:
        backend.load()
        return True
    except (OSError, AttributeError):
        return False


def write(path, value):
    temporary = path.with_name(path.name + '.tmp')
    descriptor = os.open(str(temporary), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, 'w') as stream:
        json.dump(value, stream)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def read_owner(directory):
    path = Path(directory) / 'owner.json'
    return json.loads(path.read_text()) if path.exists() else None


def job_label(directory):
    return plistlib.loads((Path(directory) / 'job.plist').read_bytes())['Label']


def create_job(directory, argv, cwd, pythonpath, suffix):
    path = Path(directory) / 'job.plist'
    value = {'Label': LABEL_PREFIX + suffix, 'ProgramArguments': argv, 'WorkingDirectory': str(cwd),
             'RunAtLoad': True, 'ProcessType': 'Interactive', 'StandardOutPath': '/dev/null',
             'StandardErrorPath': '/dev/null', 'EnvironmentVariables': {'PYTHONPATH': pythonpath}}
    temporary = path.with_name(path.name + '.tmp')
    descriptor = os.open(str(temporary), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, 'wb') as stream:
        plistlib.dump(value, stream)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    return path


@dataclass(frozen=True)
class Recovery:
    cleanup: bool


def recovery(directory):
    directory = Path(directory)
    if not (directory / 'job.plist').exists() or (directory / 'job-booted-out').exists():
        return None
    record = read_owner(directory)
    return Recovery(cleanup=not (directory / 'exit.json').exists() and record is not None
                    and record['cleanup'] in ('pending', 'failed'))


def establish(directory):
    try:
        host = backend.metadata(backend.pid())
        if host is None or host.uid != backend.uid() or not host.resource:
            raise OwnershipUnavailable('Host identity is unavailable')
        members = [item for item in backend.processes() if item.resource == host.resource]
        if members != [host]:
            raise OwnershipUnavailable('Host resource coalition is shared')
        if backend.signal(replace(host, version=host.version + 1), signal.SIGCONT) != errno.ESRCH:
            raise OwnershipUnavailable('Stale audit tokens are accepted')
        if backend.signal(host, signal.SIGCONT) != 0:
            raise OwnershipUnavailable('Audit token signals are unavailable')
        value = {'boot': backend.boot(), 'label': job_label(directory), 'pid': host.pid,
                 'unique': host.unique, 'resource': host.resource, 'jetsam': host.jetsam,
                 'cleanup': 'pending'}
        write(Path(directory) / 'owner.json', value)
        return value
    except (OSError, ValueError, KeyError, AttributeError) as error:
        raise OwnershipUnavailable('Worker process ownership is unavailable') from error


def select(record, exclude=None):
    return [item for item in backend.processes() if item.uid == backend.uid()
            and item.resource == record['resource'] and item.unique >= record['unique']
            and (item.pid, item.unique) != exclude]


def cleanup(directory, exclude=None):
    record = None
    remaining = []
    killed = set()
    result = {'status': 'failed', 'remaining': 0, 'reason': 'Owner record is unavailable'}
    try:
        record = read_owner(directory)
        if record is None:
            return result
        if backend.boot() != record['boot']:
            result = {'status': 'stale', 'remaining': 0, 'reason': 'Boot identity changed'}
        else:
            remaining = select(record, exclude)
            if remaining:
                stale = replace(remaining[0], version=remaining[0].version + 1)
                if backend.signal(stale, signal.SIGSTOP) != errno.ESRCH:
                    raise OSError('Stale audit tokens are accepted')
            frozen = set()
            for _ in range(FREEZE_ROUNDS):
                new = [item for item in remaining if (item.unique, item.version) not in frozen]
                if not new:
                    break
                for item in new:
                    code = backend.signal(item, signal.SIGSTOP)
                    if code not in (0, errno.ESRCH):
                        raise OSError(code, 'Audit token freeze failed')
                    if code == 0:
                        frozen.add((item.unique, item.version))
                remaining = select(record, exclude)
            else:
                raise OSError('Freeze rescan exceeded its bound')
            deadline = backend.clock() + STOP_TIMEOUT
            while remaining:
                for item in remaining:
                    code = backend.signal(item, signal.SIGKILL)
                    if code not in (0, errno.ESRCH):
                        raise OSError(code, 'Audit token stop failed')
                    if code == 0:
                        killed.add(item.pid)
                remaining = select(record, exclude)
                if not remaining:
                    break
                if backend.clock() >= deadline:
                    raise OSError('Coalition did not empty before the deadline')
                backend.sleep(.02)
                remaining = select(record, exclude)
            result = {'status': 'ok', 'remaining': 0, 'reason': ''}
    except Exception as error:
        result = {'status': 'failed', 'remaining': len(remaining), 'reason': str(error)}
    if record is not None:
        record.update(cleanup=result['status'], remaining=result['remaining'], reason=result['reason'],
                      remaining_pids=sorted(item.pid for item in remaining) if result['status'] == 'failed' else [])
        if killed:
            record['killed_pids'] = sorted(set(record.get('killed_pids', [])) | killed)
        try:
            write(Path(directory) / 'owner.json', record)
        except Exception:
            result = {'status': 'failed', 'remaining': result['remaining'],
                      'reason': 'Cannot save ownership cleanup result'}
    return result


def exit_cleanup(result):
    return 'ok' if result['status'] == 'ok' else result


def report(directory, problem):
    if problem is None:
        return
    record = read_owner(directory)
    if record and record['cleanup'] in ('failed', 'stale') and not record.get('reported'):
        problem('worker', 'ownership-cleanup', 'remaining=%s reason=%s killed_pids=%s' % (
            record.get('remaining', 0), record.get('reason', 'Unknown'), record.get('killed_pids', [])))
        record['reported'] = True
        write(Path(directory) / 'owner.json', record)
    if record and record['cleanup'] == 'ok' and record.get('killed_pids') and not record.get('kills_reported'):
        problem('worker', 'process-killed', 'SIGKILL pids=%s' % record['killed_pids'])
        record['kills_reported'] = True
        write(Path(directory) / 'owner.json', record)


def bootout(directory, mark=True):
    directory = Path(directory)
    if not (directory / 'job.plist').exists() or (directory / 'job-booted-out').exists():
        return
    backend.bootout(job_label(directory))
    if mark:
        write(directory / 'job-booted-out', True)
