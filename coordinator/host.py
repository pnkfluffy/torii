"""Detached owner of one native provider process and its durable output."""

import asyncio
import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

from . import extension, native_ids, process_ownership as ownership
from .scrub import Scrubber


HOST_TIMEOUT = 10
_launchers = []


class SessionBusy(RuntimeError):
    """Another host owns the requested native session."""


class HostDied(RuntimeError):
    pass


class CleanupFailed(RuntimeError):
    def __init__(self, pids):
        self.pids = sorted(pids)
        super().__init__('Worker process cleanup failed after 3 attempts; leftover pids=%s' % self.pids)


def _reap_launchers():
    _launchers[:] = [launcher for launcher in _launchers if launcher.poll() is None]


def alive(pid):
    try:
        os.kill(int(pid), 0)
        return True
    except (ValueError, TypeError, ProcessLookupError):
        return False
    except PermissionError:
        return True


def host_state(directory):
    directory = Path(directory)
    if (directory / 'logs-deleted.json').is_file():
        return 'exited'
    if (directory / 'exit.json').exists():
        return 'exited'
    _reap_launchers()
    path = directory / 'pid'
    if path.exists() and alive(path.read_text().strip()):
        record = ownership.read_owner(directory)
        if record is not None:
            try:
                current = ownership.backend.metadata(int(path.read_text()))
                if (ownership.backend.boot() != record['boot'] or current is None
                        or current.unique != record['unique']):
                    return 'dead'
            except OSError:
                return 'dead'
        return 'running'
    return 'exited' if (directory / 'exit.json').exists() else 'dead'


def _check_session_busy(directory):
    path = directory / 'exit.json'
    failure = json.loads(path.read_text()).get('failure_code') if path.exists() else None
    if failure == 'session_busy':
        raise SessionBusy('Session is already owned by another provider host')
    if failure == 'ownership_unavailable':
        raise ownership.OwnershipUnavailable('Worker process ownership is unavailable on this host.')


def _write(path, value):
    temporary = path.with_name(path.name + '.tmp')
    descriptor = os.open(str(temporary), os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'w') as stream:
        json.dump(value, stream)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def _socket_path(directory):
    alias = Path('/tmp') / ('torii-' + hashlib.sha256(str(directory.resolve()).encode()).hexdigest()[:16])
    return alias, alias / 'sock'


async def _stop_failed_launch(launcher, directory):
    await asyncio.sleep(.05)
    path = directory / 'child.pid'
    child_pid = int(path.read_text()) if path.exists() else None
    if child_pid:
        try:
            os.killpg(child_pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            if launcher.poll() is not None and not alive(child_pid):
                break
            await asyncio.sleep(.05)
        try:
            os.killpg(child_pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + 2
        while launcher.poll() is None and time.monotonic() < deadline:
            await asyncio.sleep(.05)
    if launcher.poll() is None:
        try:
            launcher.terminate()
        except ProcessLookupError:
            pass
    if launcher.poll() is None:
        try:
            launcher.kill()
        except ProcessLookupError:
            pass
    await asyncio.to_thread(launcher.wait)
    _reap_launchers()


def _ownership_problem(directory, problem, code, error):
    if problem is not None:
        problem('worker', code, '%s: %s: %s' % (Path(directory).name, type(error).__name__, error))


def settle_ownership(directory, problem=None):
    try:
        recovery = ownership.recovery(directory)
        if recovery is None:
            return
        if recovery.cleanup:
            ownership.cleanup(directory)
        ownership.report(directory, problem)
    except Exception as error:
        _ownership_problem(directory, problem, 'ownership-recovery', error)
        return
    try:
        ownership.bootout(directory)
    except Exception as error:
        _ownership_problem(directory, problem, 'ownership-bootout', error)


def recover_ownership(hosts, problem=None):
    for path in sorted(Path(hosts).glob('*/job.plist')):
        directory = path.parent
        try:
            if ownership.recovery(directory) is not None and host_state(directory) != 'running':
                settle_ownership(directory, problem)
        except Exception as error:
            _ownership_problem(directory, problem, 'ownership-recovery', error)


async def _launch_owned(directory, private):
    alias, _ = _socket_path(directory)
    if not alias.exists():
        alias.symlink_to(directory, target_is_directory=True)
    if not alias.is_symlink() or alias.resolve() != directory:
        raise RuntimeError('Worker host socket alias is unavailable')
    channel = alias / 'private.sock'
    received = asyncio.get_running_loop().create_future()
    accepted = False

    async def handoff(reader, writer):
        nonlocal accepted
        if accepted:
            writer.close()
            return
        accepted = True
        server.close()
        try:
            writer.write(json.dumps(private or {}).encode() + b'\n')
            await writer.drain()
            if await reader.readline() != b'ok\n':
                raise RuntimeError('Worker host private handoff failed')
            received.set_result(None)
        except Exception as error:
            if not received.done():
                received.set_exception(error)
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_unix_server(handoff, path=str(channel))
    root = Path(__file__).resolve().parents[1]
    try:
        path = ownership.create_job(directory, [sys.executable, '-m', 'coordinator', 'host', str(directory)],
                                    root, str(root), alias.name[len('torii-'):])
        await asyncio.to_thread(ownership.backend.bootstrap, path)
        deadline = time.monotonic() + HOST_TIMEOUT
        while not received.done() and time.monotonic() < deadline:
            if await asyncio.to_thread(ownership.backend.job_state, ownership.job_label(directory)) in ('missing', 'exited'):
                raise RuntimeError('Provider host exited before private handoff')
            await asyncio.wait((received,), timeout=.02)
        if not received.done():
            raise RuntimeError('Worker host private handoff did not finish')
        await received
    finally:
        server.close()
        await server.wait_closed()
        channel_file = directory / 'private.sock'
        if channel_file.exists():
            channel_file.unlink()
        if not received.done():
            received.cancel()


async def _stop_failed_owned_launch(directory, problem):
    try:
        await asyncio.to_thread(ownership.bootout, directory, False)
        deadline = time.monotonic() + HOST_TIMEOUT
        path = directory / 'pid'
        while path.exists() and alive(path.read_text().strip()):
            if time.monotonic() >= deadline:
                raise RuntimeError('Worker host did not exit after launchd bootout')
            await asyncio.sleep(.02)
        recovery = ownership.recovery(directory)
        if recovery is not None:
            if recovery.cleanup:
                await asyncio.to_thread(ownership.cleanup, directory)
            ownership.report(directory, problem)
            ownership.write(directory / 'job-booted-out', True)
    finally:
        alias, _ = _socket_path(directory)
        if alias.is_symlink() and alias.resolve() == directory:
            alias.unlink()


async def _record_failed_launch(directory):
    names = ('pid', 'child.pid', 'child-starting.json', 'launch-failed.json',
             'owner.json', 'job.plist', 'job-booted-out')
    if any((directory / name).is_symlink() for name in names):
        return
    host = directory / 'pid'
    if host.is_file():
        host_pid = int(host.read_text())
        if host_pid <= 1 or alive(host_pid):
            return
    owned = (directory / 'job.plist').is_file()
    record = ownership.read_owner(directory)
    if owned and not (directory / 'job-booted-out').is_file():
        return
    if record is not None:
        if (record.get('cleanup') not in ('ok', 'stale') or record.get('remaining') != 0):
            return
        if ownership.backend.boot() == record['boot']:
            if await asyncio.to_thread(ownership.select, record):
                return
    child = directory / 'child.pid'
    starting = directory / 'child-starting.json'
    if child.is_file():
        child_pid = int(child.read_text())
        if child_pid <= 1 or alive(child_pid):
            return
        try:
            os.killpg(child_pid, 0)
        except ProcessLookupError:
            pass
        else:
            return
    elif starting.exists():
        return
    if record is None and owned and (child.exists() or starting.exists()):
        return
    _write(directory / 'launch-failed.json', True)


class HostClient:
    def __init__(self, directory, last_seq=0, on_seq=None, on_problem=None):
        self.directory = Path(directory).resolve()
        self.last_seq = last_seq
        self.on_seq = on_seq
        self.on_problem = on_problem
        self._settled = False
        self.pid = None
        self._file = None
        self._pending = None
        self.history_damaged = False
        self.stdin = HostStdin(self)

    @property
    def event_sequence(self):
        return self._pending

    @property
    def logs_deleted(self):
        return (self.directory / 'logs-deleted.json').is_file()

    @property
    def returncode(self):
        path = self.directory / 'exit.json'
        return json.loads(path.read_text())['returncode'] if path.exists() else None

    @property
    def child_pid(self):
        path = self.directory / 'child.pid'
        return int(path.read_text()) if path.exists() else None

    @property
    def state(self):
        return host_state(self.directory)

    @classmethod
    async def launch(cls, directory, spec, last_seq=0, on_seq=None, private=None, on_problem=None,
                     stop_timeout=5, record_failure=False):
        directory = Path(directory).resolve()
        directory.mkdir(mode=0o700, parents=True, exist_ok=False)
        directory.parent.chmod(0o700)
        directory.chmod(0o700)
        if record_failure:
            spec = dict(spec, cleanup_failed_launch=True)
        if stop_timeout != 5:
            spec = dict(spec, stop_timeout=stop_timeout)
        _write(directory / 'spec.json', spec)
        if spec.get('ownership') == 'coalition':
            try:
                await _launch_owned(directory, private)
                client = cls(directory, last_seq, on_seq, on_problem)
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline:
                    if client.state == 'exited':
                        client._settle()
                        _check_session_busy(directory)
                        client.pid = client.child_pid
                        return client
                    if client.state == 'running' and (directory / 'sock').exists() and client.child_pid:
                        client.pid = client.child_pid
                        return client
                    if await asyncio.to_thread(ownership.backend.job_state, ownership.job_label(directory)) in ('missing', 'exited'):
                        raise RuntimeError('Provider host exited before becoming ready')
                    await asyncio.sleep(.02)
                raise RuntimeError('Provider host did not become ready')
            except BaseException:
                try:
                    await _stop_failed_owned_launch(directory, on_problem)
                    if record_failure:
                        await _record_failed_launch(directory)
                except Exception as error:
                    _ownership_problem(directory, on_problem, 'ownership-recovery', error)
                raise
        env = {key: value for key, value in spec['env'].items() if key != 'PYTHONPATH'}
        env['PYTHONPATH'] = str(Path(__file__).resolve().parents[1])
        try:
            launcher = subprocess.Popen([sys.executable, '-m', 'coordinator', 'host', str(directory)],
                                        cwd=str(Path(__file__).resolve().parents[1]), env=env,
                                        stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                                        stderr=subprocess.DEVNULL, start_new_session=True, close_fds=True)
        except OSError:
            if record_failure:
                try:
                    _write(directory / 'launch-failed.json', True)
                except OSError:
                    pass
            raise
        _launchers.append(launcher)
        try:
            try:
                launcher.stdin.write(json.dumps(private or {}).encode())
                launcher.stdin.close()
            except BrokenPipeError:
                pass
            client = cls(directory, last_seq, on_seq, on_problem)
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                if client.state == 'running' and (directory / 'sock').exists() and client.child_pid:
                    client.pid = client.child_pid
                    return client
                if client.state == 'exited':
                    _check_session_busy(directory)
                    client.pid = client.child_pid
                    return client
                if launcher.poll() is not None:
                    raise RuntimeError('Provider host exited before becoming ready')
                await asyncio.sleep(.02)
            raise RuntimeError('Provider host did not become ready')
        except Exception:
            if record_failure:
                try:
                    await _stop_failed_launch(launcher, directory)
                    if launcher.poll() is not None:
                        await _record_failed_launch(directory)
                except Exception:
                    pass
            else:
                await _stop_failed_launch(launcher, directory)
            raise

    @classmethod
    def attach(cls, directory, last_seq=0, on_seq=None, on_problem=None):
        client = cls(directory, last_seq, on_seq, on_problem)
        if client.state == 'dead':
            client._settle()
            raise HostDied('Provider host died without an exit record')
        if client.state == 'exited':
            client._settle()
        _check_session_busy(client.directory)
        client.pid = client.child_pid
        return client

    def _settle(self):
        if not self._settled:
            settle_ownership(self.directory, self.on_problem)
            path = self.directory / 'stop.json'
            if path.is_file():
                record = json.loads(path.read_text())
                if record.get('killed_pids') and not record.get('reported'):
                    logging.getLogger(__name__).info('host children killed pids=%s', record['killed_pids'])
                    record['reported'] = True
                    _write(path, record)
            self._settled = True

    async def recover_state(self):
        if self.state != 'dead' or not (self.directory / 'spec.json').exists() or (self.directory / 'pid').exists():
            return self.state
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and self.state == 'dead':
            await asyncio.sleep(.05)
        return self.state

    async def connect(self):
        if self.state == 'exited':
            return
        _, socket_path = _socket_path(self.directory)
        deadline = time.monotonic() + HOST_TIMEOUT
        while time.monotonic() < deadline:
            state = self.state
            if state == 'exited':
                return
            if state == 'dead':
                break
            try:
                _, writer = await asyncio.open_unix_connection(str(socket_path))
            except (FileNotFoundError, ConnectionRefusedError):
                await asyncio.sleep(.05)
                continue
            writer.close()
            await writer.wait_closed()
            return
        raise RuntimeError('Provider host socket is unavailable')

    async def request(self, op, **fields):
        _, socket_path = _socket_path(self.directory)
        reader, writer = await asyncio.open_unix_connection(str(socket_path))
        try:
            writer.write((json.dumps({'op': op, **fields}) + '\n').encode())
            await writer.drain()
            line = await reader.readline()
            if not line:
                raise BrokenPipeError('Provider host closed the socket')
            response = json.loads(line)
            if not response.get('ok'):
                raise BrokenPipeError(response.get('error', 'Provider host rejected request'))
            return response
        finally:
            writer.close()
            await writer.wait_closed()

    def _read_record(self, stream, field='data'):
        while True:
            offset = stream.tell()
            first = stream.read(1)
            if first == b'\x00':
                self.history_damaged = True
                try:
                    stream.seek(offset, os.SEEK_DATA)
                except (AttributeError, OSError, ValueError):
                    stream.seek(offset)
                while True:
                    offset = stream.tell()
                    chunk = stream.read(65536)
                    tail = chunk.lstrip(b'\x00')
                    if tail or not chunk:
                        stream.seek(offset + len(chunk) - len(tail))
                        break
                continue
            stream.seek(offset)
            line = stream.readline()
            if not line.endswith(b'\n'):
                stream.seek(offset)
                return None
            try:
                record = json.loads(line)
                if (isinstance(record, dict) and isinstance(record.get(field), str)
                        and (field != 'data' or type(record.get('seq')) is int)):
                    return record
            except (ValueError, UnicodeError):
                pass
            self.history_damaged = True

    async def next_event(self):
        if self.logs_deleted:
            return None
        if self._file is None:
            self._file = (self.directory / 'events.jsonl').open('rb')
        while True:
            event = self._read_record(self._file)
            if event is not None:
                if event['seq'] <= self.last_seq:
                    continue
                self._pending = event['seq']
                return event['data']
            if self.returncode is not None:
                self._settle()
                _reap_launchers()
                return None
            if self.state == 'dead':
                self._settle()
                raise HostDied('Provider host died without an exit record')
            await asyncio.sleep(.05)

    def history(self, through_seq):
        if self.logs_deleted:
            return
        with (self.directory / 'events.jsonl').open('rb') as stream:
            while True:
                event = self._read_record(stream)
                if event is None:
                    break
                if event['seq'] > through_seq:
                    break
                yield event['data']

    def history_after(self, seq):
        """Events after seq that the host has written in full. A line still being written is left
        for the live reader."""
        if self.logs_deleted:
            return
        with (self.directory / 'events.jsonl').open('rb') as stream:
            while True:
                event = self._read_record(stream)
                if event is None:
                    break
                if event['seq'] > seq:
                    yield event['data']

    def written_lines(self):
        if self.logs_deleted:
            return []
        path = self.directory / 'writes.jsonl'
        if not path.exists():
            return []
        lines = []
        with path.open('rb') as stream:
            while True:
                record = self._read_record(stream, 'line')
                if record is None:
                    return lines
                lines.append(record['line'])

    def past_request(self, request_id):
        for line in self.written_lines():
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if isinstance(event, dict) and event.get('id') == request_id:
                return event
        return None

    def past_response(self, request_id):
        if self.logs_deleted:
            return None
        for raw in self.history_after(0):
            try:
                event = json.loads(raw)
            except ValueError:
                continue
            if isinstance(event, dict) and event.get('id') == request_id:
                if 'result' in event or 'error' in event:
                    return event
        return None

    def max_request_id(self):
        highest = 0
        for line in self.written_lines():
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if isinstance(event, dict) and type(event.get('id')) is int:
                highest = max(highest, event['id'])
        return highest

    def past_user(self, message_id):
        for line in self.written_lines():
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if isinstance(event, dict) and event.get('type') == 'user' and event.get('uuid') == message_id:
                return True
        return False

    def past_echo(self, message_id):
        if self.logs_deleted:
            return False
        for raw in self.history_after(0):
            try:
                event = json.loads(raw)
            except ValueError:
                continue
            if (isinstance(event, dict) and event.get('type') == 'user'
                    and event.get('uuid') == message_id and event.get('isReplay') is True):
                return True
        return False

    def ack(self):
        if self._pending is not None:
            if self.on_seq:
                self.on_seq(self._pending)
            self.last_seq = self._pending
            self._pending = None

    async def wait(self):
        while self.returncode is None:
            if self.state == 'dead':
                self._settle()
                raise HostDied('Provider host died without an exit record')
            await asyncio.sleep(.05)
        _reap_launchers()
        self._settle()
        return self.returncode

    async def stop(self):
        request_error = None
        if self.state == 'running':
            try:
                await asyncio.wait_for(self.request('stop'), HOST_TIMEOUT)
            except (FileNotFoundError, ConnectionError, asyncio.TimeoutError) as error:
                if isinstance(error, asyncio.TimeoutError):
                    error = RuntimeError('Worker host did not exit after stop')
                child_pid = self.child_pid
                if self.state == 'running' and (child_pid is None or alive(child_pid)):
                    if ownership.read_owner(self.directory) is None:
                        raise error
                    settle_ownership(self.directory, self.on_problem)
                request_error = error
        try:
            if request_error is None:
                await asyncio.wait_for(self.wait(), HOST_TIMEOUT + ownership.STOP_TIMEOUT)
            else:
                await asyncio.wait_for(self.wait(), HOST_TIMEOUT)
        except asyncio.TimeoutError:
            if self.state == 'running':
                if ownership.read_owner(self.directory) is None:
                    if request_error is not None:
                        raise request_error
                    raise RuntimeError('Worker host did not exit after stop') from None
                settle_ownership(self.directory, self.on_problem)
        except HostDied:
            pass
        record = ownership.read_owner(self.directory)
        if record is not None and record['cleanup'] not in ('ok', 'stale'):
            if self.state == 'running':
                raise RuntimeError('Worker process cleanup did not finish')
            for attempt in range(record.get('cleanup_attempts', 0), 3):
                await asyncio.to_thread(ownership.cleanup, self.directory)
                record = ownership.read_owner(self.directory)
                record['cleanup_attempts'] = attempt + 1
                ownership.write(self.directory / 'owner.json', record)
                if record['cleanup'] in ('ok', 'stale'):
                    break
                if attempt < 2:
                    await asyncio.sleep(.1 * 2 ** attempt)
            if record['cleanup'] not in ('ok', 'stale'):
                raise CleanupFailed(record.get('remaining_pids', []))
            ownership.report(self.directory, self.on_problem)
        path = self.directory / 'pid'
        if path.is_file():
            pid = int(path.read_text())
            deadline = time.monotonic() + HOST_TIMEOUT
            while alive(pid) and self.state != 'dead':
                _reap_launchers()
                if not alive(pid) or (self.directory / 'exit.json').exists():
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError('Worker host did not exit after stop')
                await asyncio.sleep(.02)

    def detach(self):
        if self._file:
            self._file.close()
            self._file = None


class HostStdin:
    def __init__(self, client):
        self.client = client
        self.buffer = bytearray()
        self._closing = None
        self._closed = asyncio.Event()

    def write(self, data):
        self.buffer.extend(data)

    async def drain(self):
        while b'\n' in self.buffer:
            line, _, tail = self.buffer.partition(b'\n')
            self.buffer = bytearray(tail)
            await self.client.request('write', line=line.decode())

    async def private(self, event, secrets=None):
        """Write a line that is never journaled. Its secrets are scrubbed from later output, because
        Claude echoes the control responses it receives."""
        await self.client.request('private_write', line=json.dumps(event), scrub=Scrubber(secrets or {}).forms)

    def close(self):
        if self._closing is None:
            self._closing = asyncio.create_task(self.client.request('close_stdin'))
            def settled(task):
                if not task.cancelled():
                    task.exception()
                self._closed.set()
            self._closing.add_done_callback(settled)

    async def wait_closed(self):
        await self._closed.wait()
        try:
            await self._closing
        except (BrokenPipeError, ConnectionError, FileNotFoundError):
            pass


async def _serve(directory, private):
    spec = json.loads((directory / 'spec.json').read_text())
    scrub = Scrubber.of(private.get('scrub', []))
    locks = []
    for name in spec.get('locks', []):
        lock = directory.parent.parent / 'locks' / (name + '.lock')
        lock.parent.mkdir(mode=0o700, exist_ok=True)
        fd = os.open(str(lock), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            if spec['provider'] != 'opencode':
                raise
            os.close(fd)
            for held in locks:
                os.close(held)
            _write(directory / 'exit.json', {'returncode': 1, 'failure_code': 'session_busy'})
            return
        locks.append(fd)
    sock = directory / 'sock'
    if sock.exists():
        sock.unlink()
    alias, short_sock = _socket_path(directory)
    if alias.is_symlink() and alias.resolve() == directory:
        alias.unlink()
    alias.symlink_to(directory, target_is_directory=True)
    _write(directory / 'pid', os.getpid())
    owner = None
    if spec.get('ownership') == 'coalition':
        try:
            owner = ownership.establish(directory)
        except ownership.OwnershipUnavailable:
            for fd in locks:
                os.close(fd)
            alias.unlink()
            _write(directory / 'exit.json', {'returncode': 1, 'time': time.time(),
                                             'failure_code': 'ownership_unavailable'})
            return
    stderr = (directory / 'stderr.log').open('wb')
    events = (directory / 'events.jsonl').open('w')
    writes = (directory / 'writes.jsonl').open('w')
    if spec.get('cleanup_failed_launch') is True:
        _write(directory / 'child-starting.json', True)
    try:
        child = await extension.active().launch_process(spec['argv'], {**spec['env'], **private.get('env', {})}, cwd=spec['cwd'],
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE if scrub.forms else stderr,
            start_new_session=True, limit=16 * 1024 * 1024)
    except OSError:
        if (spec.get('cleanup_failed_launch') is True and
                extension.active().launch_failure_has_no_child({**spec['env'], **private.get('env', {})})):
            try:
                _write(directory / 'launch-failed.json', True)
            except OSError:
                pass
        raise
    _write(directory / 'child.pid', child.pid)
    copy_stderr = None
    if scrub.forms:
        async def scrubbed_stderr():
            while True:
                try:
                    line = await child.stderr.readline()
                except ValueError:
                    line = await child.stderr.read(16 * 1024 * 1024)
                if not line:
                    break
                stderr.write(scrub(line.decode(errors='replace')).encode())
                stderr.flush()
        copy_stderr = asyncio.create_task(scrubbed_stderr())
    write_lock = asyncio.Lock()
    write_seq = 0
    stopping = None

    async def stop_child():
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            return
        deadline = time.monotonic() + spec.get('stop_timeout', 5)
        while time.monotonic() < deadline:
            try:
                os.killpg(child.pid, 0)
            except (ProcessLookupError, PermissionError):
                return
            await asyncio.sleep(.05)
        try:
            members, _ = await ownership.group_members(child.pid)
            killed = sorted(pid for pid, _ in members)
        except OSError:
            killed = [child.pid] if child.returncode is None else []
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        else:
            if killed:
                _write(directory / 'stop.json', {'killed_pids': killed})

    async def handle(reader, writer):
        nonlocal stopping, write_seq
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                try:
                    request = json.loads(line)
                    if not isinstance(request, dict):
                        raise ValueError('Host request must be an object')
                    op = request.get('op')
                    if (op == 'write' and isinstance(request.get('line'), str)
                            and '\n' not in request['line'] and '\r' not in request['line']):
                        async with write_lock:
                            write_seq += 1
                            writes.write(json.dumps({'seq': write_seq, 'line': request['line']}) + '\n')
                            writes.flush()
                            child.stdin.write((request['line'] + '\n').encode())
                            await child.stdin.drain()
                    elif (op == 'private_write' and isinstance(request.get('line'), str)
                          and '\n' not in request['line'] and '\r' not in request['line']):
                        scrub.extend(request.get('scrub') or [])
                        child.stdin.write((request['line'] + '\n').encode())
                        await child.stdin.drain()
                    elif op == 'close_stdin':
                        child.stdin.close()
                        await child.stdin.wait_closed()
                    elif op == 'stop':
                        if stopping is None:
                            stopping = asyncio.create_task(stop_child())
                    else:
                        raise ValueError('Unknown host operation')
                    response = {'ok': True}
                except (ValueError, BrokenPipeError, ConnectionResetError) as error:
                    response = {'ok': False, 'error': type(error).__name__}
                try:
                    writer.write((json.dumps(response) + '\n').encode())
                    await writer.drain()
                except (BrokenPipeError, ConnectionResetError):
                    break
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (BrokenPipeError, ConnectionResetError):
                pass

    options = {'cleanup_socket': False} if sys.version_info >= (3, 13) else {}
    server = await asyncio.start_unix_server(handle, path=str(short_sock), limit=16 * 1024 * 1024,
                                             **options)
    seq = 0
    cleanup = None
    native_lock = any(name.startswith(spec['provider'] + '-') for name in spec.get('locks', []))
    try:
        while True:
            if owner is None:
                line = await child.stdout.readline()
            else:
                reading = asyncio.create_task(child.stdout.readline())
                try:
                    while not reading.done():
                        await asyncio.wait((reading,), timeout=.05)
                        if (not reading.done() and child.returncode is not None and
                                (stopping is not None or spec['provider'] not in ('claude', 'codex'))):
                            reading.cancel()
                            break
                    line = b'' if reading.cancelled() or not reading.done() else reading.result()
                finally:
                    if not reading.done():
                        reading.cancel()
                    await asyncio.gather(reading, return_exceptions=True)
            if not line:
                break
            seq += 1
            raw = scrub.event(line.decode(errors='replace').rstrip('\n'))
            events.write(json.dumps({'seq': seq, 'data': raw}) + '\n')
            events.flush()
            if not native_lock:
                try:
                    event = json.loads(raw)
                    provider = spec['provider']
                    session_id = None
                    if provider == 'codex':
                        session_id = event.get('thread_id')
                        if event.get('method') == 'thread/started':
                            session_id = (event.get('params') or {}).get('thread', {}).get('id')
                    elif provider == 'opencode' and 'id' in event and 'method' not in event:
                        session_id = (event.get('result') or {}).get('sessionId')
                    elif provider == 'muse' and event.get('id') == 'torii-open' and 'method' not in event:
                        session_id = ((event.get('result') or {}).get('session') or {}).get('sessionId')
                    if native_ids.valid(provider, session_id):
                        lock = directory.parent.parent / 'locks' / (provider + '-' + session_id + '.lock')
                        lock.parent.mkdir(mode=0o700, exist_ok=True)
                        fd = os.open(str(lock), os.O_CREAT | os.O_RDWR, 0o600)
                        try:
                            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        except BlockingIOError:
                            os.close(fd)
                            if stopping is None:
                                stopping = asyncio.create_task(stop_child())
                        else:
                            locks.append(fd)
                            native_lock = True
                except (ValueError, TypeError, AttributeError):
                    pass
        if owner is None:
            await child.wait()
        if stopping:
            await stopping
        else:
            await stop_child()
        if owner is not None:
            cleanup = await asyncio.to_thread(ownership.cleanup, directory, (owner['pid'], owner['unique']))
            if cleanup['status'] == 'ok':
                await child.wait()
        if copy_stderr is not None:
            if cleanup is not None and cleanup['status'] != 'ok':
                copy_stderr.cancel()
                await asyncio.gather(copy_stderr, return_exceptions=True)
            else:
                await copy_stderr
    finally:
        if owner is not None and cleanup is None:
            await stop_child()
            cleanup = await asyncio.to_thread(ownership.cleanup, directory, (owner['pid'], owner['unique']))
        server.close()
        await server.wait_closed()
        events.close()
        writes.close()
        stderr.close()
        for fd in locks:
            os.close(fd)
        if alias.is_symlink() and alias.resolve() == directory:
            alias.unlink()
    result = {'returncode': child.returncode if child.returncode is not None else 1, 'time': time.time()}
    if cleanup is not None:
        result['cleanup'] = ownership.exit_cleanup(cleanup)
    _write(directory / 'exit.json', result)


def main(directory):
    os.umask(0o077)
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    directory = Path(directory)
    spec = json.loads((directory / 'spec.json').read_text())
    if spec.get('ownership') == 'coalition':
        _, path = _socket_path(directory)
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as channel:
            channel.settimeout(HOST_TIMEOUT)
            channel.connect(str(path.with_name('private.sock')))
            with channel.makefile('rb') as stream:
                private = json.loads(stream.readline())
            channel.sendall(b'ok\n')
    else:
        private = json.loads(sys.stdin.read() or '{}')
    asyncio.run(_serve(directory, private))
