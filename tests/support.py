"""Load-proof waiting for tests that observe real processes and services.

A budget here is a safety net, never an assertion. Each helper waits for the
real signal the code under test produces, so a loaded machine only makes the
wait longer, not the outcome different. Raise TORII_TEST_TIMEOUT_SCALE above 1
when a machine is slow enough to exhaust even these budgets.
"""

import asyncio
import fcntl
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time
from unittest.mock import patch


def _scale():
    try:
        value = float(os.environ.get('TORII_TEST_TIMEOUT_SCALE', '1'))
    except ValueError:
        return 1.0
    return value if value > 0 else 1.0


SCALE = _scale()
SETTLE = 20.0 * SCALE


def isolate_shared_mcp_home(test, root):
    home = Path(root) / 'shared_mcp-home'
    home.mkdir()
    environment = patch.dict(os.environ, {'HOME': str(home)})
    environment.start()
    test.addCleanup(environment.stop)


def stop_test_hosts(root):
    from coordinator.host import _launchers, _socket_path, _reap_launchers

    root = Path(root).resolve()
    temporary = Path(tempfile.gettempdir()).resolve()
    if os.path.commonpath((str(root), str(temporary))) != str(temporary) or root == temporary:
        raise ValueError('Host cleanup requires a temporary test root')
    launchers = {Path(launcher.args[-1]).resolve(): launcher for launcher in _launchers
                 if launcher.poll() is None and len(launcher.args) == 5
                 and launcher.args[1:4] == ['-m', 'coordinator', 'host']}
    for directory in (root / 'state' / 'hosts').glob('*'):
        host_path = directory / 'pid'
        child_path = directory / 'child.pid'
        launcher = launchers.get(directory.resolve())
        host_pid = launcher.pid if launcher else (int(host_path.read_text()) if host_path.exists() else None)
        child_pid = int(child_path.read_text()) if child_path.exists() else None
        if host_pid:
            command = subprocess.run(['ps', '-p', str(host_pid), '-o', 'command='],
                                     capture_output=True, text=True, check=False).stdout.strip()
            if launcher or command.endswith(' -m coordinator host ' + str(directory.resolve())):
                _stop_group(host_pid)
            else:
                host_pid = None
        if child_pid:
            _stop_group(child_pid)
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and any(_group_alive(pid) for pid in (host_pid, child_pid) if pid):
            time.sleep(.02)
        for pid in (host_pid, child_pid):
            if pid and _group_alive(pid):
                _stop_group(pid, signal.SIGKILL)
        _reap_launchers()
        alias, _ = _socket_path(directory)
        if alias.is_symlink() and alias.resolve() == directory.resolve():
            alias.unlink()


def _group_alive(pid):
    processes = subprocess.run(['ps', '-axo', 'pgid=,stat='], capture_output=True,
                               text=True, check=False).stdout.splitlines()
    return any(parts[0] == str(pid) and not parts[1].startswith('Z')
               for line in processes if (parts := line.split()))


def _stop_group(pid, sig=signal.SIGTERM):
    if _group_alive(pid):
        try:
            os.killpg(pid, sig)
        except ProcessLookupError:
            pass


async def settle(awaitable, timeout=None):
    """Await a real completion signal under a budget that machine load cannot exhaust."""
    return await asyncio.wait_for(awaitable, SETTLE if timeout is None else timeout * SCALE)


async def until(predicate, description='the expected state', timeout=None):
    """Poll for a condition the code under test must reach."""
    async def watch():
        while not predicate():
            await asyncio.sleep(0.005)

    try:
        await settle(watch(), timeout)
    except asyncio.TimeoutError:
        raise AssertionError('Timed out waiting for ' + description)


async def until_exists(path, timeout=None):
    return await until(path.exists, 'the process to create ' + str(path), timeout)


async def until_text(path, timeout=None):
    """Return the file's content once it is written.

    A shell redirect creates the file before the content lands, so existence
    alone is not the signal.
    """
    await until(lambda: path.exists() and path.read_text().strip(),
                'the process to write ' + str(path), timeout)
    return path.read_text().strip()


async def until_unlocked(path, description, timeout=None):
    """Wait for every process holding this file lock to exit.

    The kernel releases the lock when the last holder dies, so this answers for
    a process outside this process tree that nothing here can reap.
    """
    def free():
        with open(str(path), 'a') as handle:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                return False
            fcntl.flock(handle, fcntl.LOCK_UN)
            return True

    return await until(free, description, timeout)


def restart_work_rows(task_count, worker_count):
    title = ('job "quoted" é 🦊 ' + 'x' * 200)[:199]
    tasks = [{'id': number, 'topic': 'channel-%d' % (number % 7), 'number': number,
              'title': title, 'notes': 'job-notes-' + 'n' * 5990, 'status': 'open',
              'updated': (task_count - number) // 2} for number in range(1, task_count + 1)]
    workers = [{'id': number, 'task': number, 'topic': 'channel-0', 'provider': 'codex',
                'status': 'running', 'goal': 'g' * 742, 'cwd': '/long/' + 'w' * 500,
                'session': 'native-session', 'pid': 1000 + number}
               for number in range(1, worker_count + 1)]
    return tasks, workers
