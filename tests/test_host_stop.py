import asyncio
from contextlib import contextmanager
import os
import signal
import socket
import subprocess
import sys
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import host, parent_state
from coordinator.host import HostDied, alive
from coordinator.workers import WorkerPool, host_directory
from tests.test_session import CoordinatorSessionTestsSupport


class HostStopTests(CoordinatorSessionTestsSupport, unittest.IsolatedAsyncioTestCase):
    async def test_parent_spec_has_no_process_ownership_and_uses_popen(self):
        launch = host.HostClient.launch
        popen = subprocess.Popen
        with patch.object(host.HostClient, 'launch', AsyncMock(side_effect=launch)) as captured, \
                patch('coordinator.host.subprocess.Popen', wraps=popen) as started:
            session = await self.start()
        self.assertNotIn('ownership', captured.await_args.args[1])
        self.assertTrue(any(call.args[0][1:4] == ['-m', 'coordinator', 'host']
                            for call in started.call_args_list))
        await session.stop()

    @contextmanager
    def paused_exit(self, phase):
        read_fd, write_fd = os.pipe()
        marker = self.root / 'host-closing'
        launchers = []
        script = '''import os,sys
from pathlib import Path
from coordinator import host
directory=Path(sys.argv[1])
def gate():
    Path(sys.argv[3]).touch()
    os.read(int(sys.argv[2]),1)
write=host._write
def gated_write(path,value):
    if sys.argv[4]=='missing' and path.name=='exit.json':
        gate()
    write(path,value)
unlink=Path.unlink
def gated_unlink(path,*args,**kwargs):
    if sys.argv[4] in ('refusing','closed') and path==host._socket_path(directory)[0]:
        gate()
    return unlink(path,*args,**kwargs)
host._write=gated_write
Path.unlink=gated_unlink
host.main(str(directory))
'''
        popen = subprocess.Popen

        def launch(args, *fields, **options):
            is_host = args[1:4] == ['-m', 'coordinator', 'host']
            if is_host:
                args = [args[0], '-u', '-c', script, args[-1], str(read_fd), str(marker), phase]
                options['pass_fds'] = (read_fd,)
            process = popen(args, *fields, **options)
            if is_host:
                launchers.append(process)
            return process

        try:
            with patch('coordinator.host.subprocess.Popen', launch):
                yield marker, lambda: os.write(write_fd, b'x')
        finally:
            os.write(write_fd, b'x')
            os.close(write_fd)
            os.close(read_fd)
            for launcher in launchers:
                launcher.wait(timeout=40)

    async def assert_shutdown_during_exit(self, phase, error):
        with self.paused_exit(phase) as (marker, release):
            session = await self.start()
            stopping = None
            peer = None
            try:
                await session.send('exit-request', 'die')
                await self.until(marker.exists)
                self.assertTrue(alive(int((session.process.directory / 'pid').read_text())))
                self.assertFalse(alive(session.process.child_pid))
                self.assertEqual(session.process.state, 'running')
                self.assertIsNone(session.process.returncode)
                if phase == 'closed':
                    sock = session.process.directory / 'sock'
                    sock.rename(sock.with_name('sock.closed'))

                    async def close(reader, writer):
                        await reader.readline()
                        writer.close()
                        await writer.wait_closed()

                    _, short_sock = host._socket_path(session.process.directory)
                    peer = await asyncio.start_unix_server(close, path=str(short_sock))
                with self.assertRaises(error):
                    await session.process.request('stop')
                attempted = asyncio.Event()
                request = session.process.request

                async def requested(op, **fields):
                    try:
                        return await request(op, **fields)
                    finally:
                        if op == 'stop':
                            attempted.set()

                with patch.object(session.process, 'request', requested):
                    stopping = asyncio.create_task(session.stop())
                    await asyncio.wait_for(attempted.wait(), 40)
                    self.assertFalse(session._stop_task.done(), 'Stop failed before the host recorded its exit')
                    release()
                    await asyncio.wait_for(stopping, 40)
                self.assertTrue(session.closed)
                self.assertTrue(session._monitor_task.done())
                self.assertIsNone(session.process._file)
                self.assertIsNone(self.store.get(session.key + '_host'))
                self.assertNotIn((str((self.runner.state_dir / 'hosts').resolve()), session.key),
                                 parent_state._active_clients)
            finally:
                release()
                await asyncio.gather(*([stopping] if stopping else []), session._monitor_task,
                                     return_exceptions=True)
                if peer is not None:
                    peer.close()
                    await peer.wait_closed()
                self.sessions.remove(session)

    async def test_session_stop_finishes_when_exiting_host_socket_is_missing(self):
        await self.assert_shutdown_during_exit('missing', FileNotFoundError)

    async def test_session_stop_finishes_when_exiting_host_socket_refuses_connections(self):
        await self.assert_shutdown_during_exit('refusing', ConnectionRefusedError)

    async def test_session_stop_finishes_when_exiting_host_closes_the_stop_connection(self):
        await self.assert_shutdown_during_exit('closed', BrokenPipeError)

    async def test_relaunch_stop_finishes_after_host_dies_without_hiding_monitor_failure(self):
        session = await self.start()
        started, release = asyncio.Event(), asyncio.Event()
        stop = session.process.stop
        scheduled = []
        create_task = asyncio.create_task

        async def gated_stop():
            started.set()
            await release.wait()
            await stop()

        def schedule(coro):
            task = create_task(coro)
            scheduled.append(task)
            return task

        try:
            with patch.object(session.process, 'stop', gated_stop), patch('asyncio.create_task', schedule):
                session.relaunch()
            await asyncio.wait_for(started.wait(), 40)
            os.kill(int((session.process.directory / 'pid').read_text()), signal.SIGKILL)
            with self.assertRaisesRegex(HostDied, 'without an exit record'):
                await asyncio.wait_for(session.wait_closed(), 40)
            self.assertEqual(session.process.state, 'dead')
            release.set()
            self.assertEqual(await asyncio.gather(*scheduled, return_exceptions=True), [None])
            self.assertTrue(session.closed)
            self.assertIsNone(session.process._file)
            with self.assertRaises(HostDied):
                await session.process.wait()
            with self.assertRaises(HostDied):
                host.HostClient.attach(session.process.directory)
            with self.assertRaises(HostDied):
                await session.process.next_event()
        finally:
            release.set()
            await asyncio.gather(*scheduled, session._monitor_task, return_exceptions=True)
            session.process.detach()
            self.sessions.remove(session)

    async def test_worker_stop_reports_original_error_when_host_pid_was_reused(self):
        with self.store.db:
            self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                                  ('stale-host', 0, 1, 'Stale host', str(self.root)))
            worker_id = self.store.db.execute('''INSERT INTO workers
                (topic,provider,prompt,cwd,workspace,status,created,updated)
                VALUES (?,?,?,?,?,'running',1,1)''',
                ('stale-host', 'claude', 'work', str(self.root), '{}')).lastrowid
        pool = WorkerPool(self.store, self.runner, None)
        directory = host_directory(self.store.directory, pool.get(worker_id))
        directory.mkdir(parents=True)
        with subprocess.Popen([sys.executable, '-c', 'pass']) as child:
            child.wait(timeout=40)
        (directory / 'child.pid').write_text(str(child.pid))
        request = host.HostClient.request
        errors = []

        async def recorded_request(client, op, **fields):
            try:
                return await request(client, op, **fields)
            except FileNotFoundError as error:
                errors.append(error)
                raise

        with subprocess.Popen([sys.executable, '-c', 'import sys; sys.stdin.read()'],
                              stdin=subprocess.PIPE) as reused:
            try:
                (directory / 'pid').write_text(str(reused.pid))
                client = host.HostClient.attach(directory)
                self.assertEqual(client.state, 'running')
                self.assertFalse(alive(client.child_pid))
                self.assertIsNone(client.returncode)
                _, short_sock = host._socket_path(directory)
                self.assertFalse(short_sock.exists())
                with patch.object(host, 'HOST_TIMEOUT', .01, create=True), \
                        patch.object(host.HostClient, 'request', recorded_request):
                    with self.assertRaises(FileNotFoundError) as raised:
                        await asyncio.wait_for(pool.stop(worker_id), 2)
                self.assertEqual(len(errors), 1)
                self.assertIs(raised.exception, errors[0])
                self.assertNotIn(worker_id, pool.stopping)
                self.assertEqual(client.state, 'running')
            finally:
                reused.terminate()
                reused.wait(timeout=40)

    async def test_stop_reports_unavailable_socket_while_provider_is_still_running(self):
        session = await self.start()
        sock = session.process.directory / 'sock'
        original = sock.with_name('sock.listening')
        sock.rename(original)
        try:
            with self.assertRaises(FileNotFoundError):
                await session.process.stop()
            _, short_sock = host._socket_path(session.process.directory)
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as refused:
                refused.bind(str(short_sock))
            with self.assertRaises(ConnectionRefusedError):
                await session.process.stop()
            sock.rename(sock.with_name('sock.refused'))

            async def reject(reader, writer):
                await reader.readline()
                writer.write(b'{"ok":false,"error":"Stop refused"}\n')
                await writer.drain()
                writer.close()
                await writer.wait_closed()

            peer = await asyncio.start_unix_server(reject, path=str(short_sock))
            try:
                with self.assertRaisesRegex(BrokenPipeError, 'Stop refused'):
                    await session.process.stop()
            finally:
                peer.close()
                await peer.wait_closed()
            self.assertEqual(session.process.state, 'running')
            self.assertTrue(alive(session.process.child_pid))
        finally:
            if sock.exists():
                sock.rename(sock.with_name('sock.rejected'))
            original.rename(sock)
