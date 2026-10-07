"""Detached owner of one native provider process and its durable output."""

import asyncio
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid

from .scrub import Scrubber


_launchers = []


class HostDied(RuntimeError):
    pass


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
    if (directory / 'exit.json').exists():
        return 'exited'
    _reap_launchers()
    path = directory / 'pid'
    if path.exists() and alive(path.read_text().strip()):
        return 'running'
    return 'exited' if (directory / 'exit.json').exists() else 'dead'


def _write(path, value):
    temporary = path.with_name(path.name + '.tmp')
    descriptor = os.open(str(temporary), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
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


class HostClient:
    def __init__(self, directory, last_seq=0, on_seq=None):
        self.directory = Path(directory).resolve()
        self.last_seq = last_seq
        self.on_seq = on_seq
        self.pid = None
        self._file = None
        self._pending = None
        self.stdin = HostStdin(self)

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
    async def launch(cls, directory, spec, last_seq=0, on_seq=None, private=None):
        directory = Path(directory).resolve()
        directory.mkdir(mode=0o700, parents=True, exist_ok=False)
        directory.parent.chmod(0o700)
        directory.chmod(0o700)
        _write(directory / 'spec.json', spec)
        env = {key: value for key, value in spec['env'].items() if key != 'PYTHONPATH'}
        env['PYTHONPATH'] = str(Path(__file__).resolve().parents[1])
        launcher = subprocess.Popen([sys.executable, '-m', 'coordinator', 'host', str(directory)],
                                    cwd=str(Path(__file__).resolve().parents[1]), env=env,
                                    stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                                    stderr=subprocess.DEVNULL, start_new_session=True, close_fds=True)
        _launchers.append(launcher)
        try:
            try:
                launcher.stdin.write(json.dumps(private or {}).encode())
                launcher.stdin.close()
            except BrokenPipeError:
                pass
            client = cls(directory, last_seq, on_seq)
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                if client.state == 'running' and (directory / 'sock').exists() and client.child_pid:
                    client.pid = client.child_pid
                    return client
                if client.state == 'exited':
                    client.pid = client.child_pid
                    return client
                if launcher.poll() is not None:
                    raise RuntimeError('Provider host exited before becoming ready')
                await asyncio.sleep(.02)
            raise RuntimeError('Provider host did not become ready')
        except Exception:
            await _stop_failed_launch(launcher, directory)
            raise

    @classmethod
    def attach(cls, directory, last_seq=0, on_seq=None):
        client = cls(directory, last_seq, on_seq)
        if client.state == 'dead':
            raise HostDied('Provider host died without an exit record')
        client.pid = client.child_pid
        return client

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
        deadline = time.monotonic() + 10
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

    async def next_event(self):
        if self._file is None:
            self._file = (self.directory / 'events.jsonl').open('r')
        while True:
            line = self._file.readline()
            if line:
                event = json.loads(line)
                if event['seq'] <= self.last_seq:
                    continue
                self._pending = event['seq']
                return event['data']
            if self.returncode is not None:
                _reap_launchers()
                return None
            if self.state == 'dead':
                raise HostDied('Provider host died without an exit record')
            await asyncio.sleep(.05)

    def history(self, through_seq):
        with (self.directory / 'events.jsonl').open('r') as stream:
            for line in stream:
                event = json.loads(line)
                if event['seq'] > through_seq:
                    break
                yield event['data']

    def history_after(self, seq):
        """Events after seq that the host has written in full. A line still being written is left
        for the live reader."""
        with (self.directory / 'events.jsonl').open('r') as stream:
            for line in stream:
                try:
                    event = json.loads(line)
                except ValueError:
                    break
                if event['seq'] > seq:
                    yield event['data']

    def written_lines(self):
        path = self.directory / 'writes.jsonl'
        if not path.exists():
            return []
        with path.open('r') as stream:
            return [json.loads(line)['line'] for line in stream]

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
        with (self.directory / 'events.jsonl').open('r') as stream:
            for line in stream:
                try:
                    event = json.loads(json.loads(line)['data'])
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
        with (self.directory / 'events.jsonl').open('r') as stream:
            for line in stream:
                try:
                    event = json.loads(json.loads(line)['data'])
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
                raise HostDied('Provider host died without an exit record')
            await asyncio.sleep(.05)
        _reap_launchers()
        return self.returncode

    async def stop(self):
        if self.state == 'running':
            await self.request('stop')
        await self.wait()

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
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        locks.append(fd)
    sock = directory / 'sock'
    if sock.exists():
        sock.unlink()
    alias, short_sock = _socket_path(directory)
    if alias.is_symlink() and alias.resolve() == directory:
        alias.unlink()
    alias.symlink_to(directory, target_is_directory=True)
    _write(directory / 'pid', os.getpid())
    stderr = (directory / 'stderr.log').open('wb')
    events = (directory / 'events.jsonl').open('w')
    writes = (directory / 'writes.jsonl').open('w')
    child = await asyncio.create_subprocess_exec(*spec['argv'], cwd=spec['cwd'],
        env={**spec['env'], **private.get('env', {})}, stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE if scrub.forms else stderr,
        start_new_session=True, limit=16 * 1024 * 1024)
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
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                os.killpg(child.pid, 0)
            except (ProcessLookupError, PermissionError):
                return
            await asyncio.sleep(.05)
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass

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

    server = await asyncio.start_unix_server(handle, path=str(short_sock), limit=16 * 1024 * 1024)
    seq = 0
    native_lock = any(name.startswith(spec['provider'] + '-') for name in spec.get('locks', []))
    try:
        while True:
            line = await child.stdout.readline()
            if not line:
                break
            seq += 1
            raw = scrub.event(line.decode(errors='replace').rstrip('\n'))
            events.write(json.dumps({'seq': seq, 'data': raw}) + '\n')
            events.flush()
            if not native_lock and spec['provider'] == 'codex':
                try:
                    event = json.loads(raw)
                    session_id = event.get('thread_id')
                    if event.get('method') == 'thread/started':
                        session_id = (event.get('params') or {}).get('thread', {}).get('id')
                    if session_id and str(uuid.UUID(session_id)) == session_id:
                        lock = directory.parent.parent / 'locks' / ('codex-' + session_id + '.lock')
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
        await child.wait()
        if stopping:
            await stopping
        else:
            await stop_child()
        if copy_stderr is not None:
            await copy_stderr
    finally:
        server.close()
        await server.wait_closed()
        events.close()
        writes.close()
        stderr.close()
        for fd in locks:
            os.close(fd)
        if alias.is_symlink() and alias.resolve() == directory:
            alias.unlink()
    _write(directory / 'exit.json', {'returncode': child.returncode, 'time': time.time()})


def main(directory):
    os.umask(0o077)
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    private = json.loads(sys.stdin.read() or '{}')
    asyncio.run(_serve(Path(directory), private))
