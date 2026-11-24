"""Run-scoped Darwin coalitions and identity-checked process cleanup."""

from dataclasses import dataclass, replace
import errno
import json
import os
from pathlib import Path
import plistlib
import signal

from . import host_os
from .host_os import DarwinBackend


LABEL_PREFIX = 'local.torii-host.'
FREEZE_ROUNDS = 16
STOP_TIMEOUT = 5


class OwnershipUnavailable(RuntimeError):
    pass


backend = DarwinBackend()


async def group_members(pgid):
    if not isinstance(pgid, int) or isinstance(pgid, bool) or pgid <= 1:
        raise OSError('Invalid process group identity')
    try:
        before = await host_os.group_snapshot(pgid, backend)
        after = await host_os.group_snapshot(pgid, backend)
        if not any(not ended for ended in after.values()) and not all(before.values()):
            before, after = after, await host_os.group_snapshot(pgid, backend)
    except (ValueError, IndexError) as error:
        raise OSError('Invalid process group metadata') from error
    members = tuple(sorted(key for key, ended in after.items() if not ended))
    ended = not members and bool(before) and all(before.values()) and after.keys() <= before.keys()
    if not members and not ended:
        raise OSError(errno.EAGAIN, 'Process group termination proof is incomplete')
    return members, ended


def supported():
    return host_os.coalitions_available(backend)


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
