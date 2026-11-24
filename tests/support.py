"""Load-proof waiting for tests that observe real processes and services.

A budget here is a safety net, never an assertion. Each helper waits for the
real signal the code under test produces, so a loaded machine only makes the
wait longer, not the outcome different. Raise TORII_TEST_TIMEOUT_SCALE above 1
when a machine is slow enough to exhaust even these budgets.
"""

import ast
import asyncio
from contextvars import ContextVar
import fcntl
from functools import wraps
import importlib
import json
import os
from pathlib import Path
import pickle
import re
import signal
import subprocess
import sys
import threading
import time
import unittest
from unittest.mock import AsyncMock, patch
from xml.etree import ElementTree

from coordinator.formatting import markdown_to_html
from coordinator.vault import VaultError, fingerprint


INTAKE_IDENTITY = re.compile(r'\[sender="(?:[^"\\]|\\.)*" job=(?:null|\d+) created=\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ\] ')


def plain(text):
    return INTAKE_IDENTITY.sub('', text)


def sent_parts(session):
    return [part for _, text in session.sent for part in re.split(r'\n\n(?=\[topic=)', plain(text))]


class FakeVault:
    KEY = bytes(range(32))

    def __init__(self):
        self.values = {}
        self.fail_next = None
        self.calls = []

    def _call(self, method, name=None):
        self.calls.append((method, name))
        if self.fail_next:
            code, self.fail_next = self.fail_next, None
            raise VaultError(code)

    def fingerprint(self, value):
        return fingerprint(self.KEY, value)

    def put(self, name, value):
        self._call('put', name)
        self.values[name] = value
        return len(value), self.fingerprint(value)

    def get(self, name):
        self._call('get', name)
        if name not in self.values:
            raise VaultError('missing')
        return self.values[name]

    def delete(self, name):
        self._call('delete', name)
        return self.values.pop(name, None) is not None

    def names(self):
        self._call('names')
        return sorted(self.values)


def telegram_text(markdown):
    return ''.join(ElementTree.fromstring('<text>' + markdown_to_html(markdown) + '</text>').itertext())


def _scale():
    try:
        value = float(os.environ.get('TORII_TEST_TIMEOUT_SCALE', '1'))
    except ValueError:
        return 1.0
    return value if value > 0 else 1.0


SCALE = _scale()
SETTLE = 20.0 * SCALE


def read_jsonl(path):
    try:
        lines = Path(path).read_text().splitlines(keepends=True)
    except FileNotFoundError:
        return []
    return [json.loads(line) for line in lines if line.endswith('\n') and line.strip()]


def fake_codex_hook_trust(test):
    trust = patch('coordinator.worker_hooks.codex_trust_override', new_callable=AsyncMock,
                  return_value='hooks.state."fixture".trusted_hash="sha256:fixture"')
    trust.start()
    test.addCleanup(trust.stop)
    inherited = patch('coordinator.shared_mcp.codex_inherited', new_callable=AsyncMock, return_value={})
    inherited.start()
    test.addCleanup(inherited.stop)
    for name in ('coordinator.providers.codex_inherited', 'coordinator.codex_session.codex_inherited'):
        item = patch(name, new_callable=AsyncMock, return_value={})
        item.start()
        test.addCleanup(item.stop)


def isolate_shared_mcp_home(test, root):
    home = Path(root) / 'shared_mcp-home'
    home.mkdir()
    values = {'HOME': str(home)}
    if 'CODEX_HOME' in os.environ:
        values['CODEX_HOME'] = str(home / '.codex')
    environment = patch.dict(os.environ, values)
    environment.start()
    test.addCleanup(environment.stop)


_running_host_tests = []
_launching_test_host = ContextVar('launching_test_host', default=None)
_process_popen = subprocess.Popen


def _process_table():
    args = ['ps', '-axo', 'pid=,ppid=,pgid=,stat=']
    with _process_popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as process:
        output, error = process.communicate()
        if process.returncode:
            raise subprocess.CalledProcessError(process.returncode, args, output, error)
    return {int(pid): (int(parent), int(group), status)
            for pid, parent, group, status in (line.split() for line in output.splitlines())}


class TestHost:
    def __init__(self, directory):
        self.directory = Path(directory).resolve()
        self.launchers = []
        self.groups = set()

    def capture(self):
        if not self.launchers:
            return
        table = _process_table()
        descendants = {process.pid for process in self.launchers if process.poll() is None}
        while True:
            found = {pid for pid, (parent, _, status) in table.items()
                     if parent in descendants and not status.startswith('Z')}
            if found.issubset(descendants):
                break
            descendants.update(found)
        self.groups.update(group for pid, (_, group, _) in table.items()
                           if pid in descendants and group in descendants)

    def stop(self):
        from coordinator.host import _reap_launchers, _socket_path
        from coordinator import process_ownership as ownership

        if (self.directory / 'job.plist').is_file():
            record = ownership.read_owner(self.directory)
            if record is not None and record['cleanup'] not in ('ok', 'stale'):
                result = ownership.cleanup(self.directory)
                if result['status'] not in ('ok', 'stale'):
                    raise AssertionError('Test host ownership cleanup failed: ' + str(result))
            ownership.bootout(self.directory)
        live = [process for process in self.launchers if process.poll() is None]
        for process in live:
            try:
                process.send_signal(signal.SIGSTOP)
            except PermissionError:
                pass
        try:
            self.capture()
        finally:
            for process in live:
                for number in (signal.SIGTERM, signal.SIGCONT):
                    try:
                        process.send_signal(number)
                    except PermissionError:
                        pass
        for group in self.groups:
            try:
                _stop_group(group)
            except PermissionError:
                pass
        deadline = time.monotonic() + 2
        remaining = self.remaining()
        while remaining and time.monotonic() < deadline:
            time.sleep(.02)
            remaining = self.remaining()
        for group in remaining:
            try:
                _stop_group(group, signal.SIGKILL)
            except PermissionError:
                pass
        for process in self.launchers:
            process.wait(timeout=SETTLE)
        deadline = time.monotonic() + SETTLE
        while self.remaining() and time.monotonic() < deadline:
            time.sleep(.02)
        if self.remaining():
            raise AssertionError('Test host process groups survived cleanup: ' + str(self.remaining()))
        self.groups.clear()
        self.launchers.clear()
        _reap_launchers()
        alias, _ = _socket_path(self.directory)
        if alias.is_symlink() and alias.resolve() == self.directory:
            alias.unlink()

    def remaining(self):
        if not self.groups:
            return set()
        return {group for _, group, status in _process_table().values()
                if group in self.groups and not status.startswith('Z')}


def _register_test_host(directory):
    if not _running_host_tests:
        return None
    _, hosts = _running_host_tests[-1]
    directory = Path(directory).resolve()
    if directory not in hosts:
        hosts[directory] = TestHost(directory)
    return hosts[directory]


def _stop_test_scan(process):
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=SETTLE)


def install_inprocess_scans():
    """Run cleanup scans on a thread of the test process; tests that need the real child call real_scans."""
    from coordinator import scan_process

    def run(function, *args, timeout=scan_process.TIMEOUT):
        delay = scan_process.backoff_delay()
        if delay:
            time.sleep(delay)
        started = time.monotonic()
        module, name, args = pickle.loads(pickle.dumps((function.__module__, function.__name__, args)))
        outcome = []

        def scan():
            try:
                outcome.append((True, getattr(importlib.import_module(module), name)(*args)))
            except Exception as error:
                outcome.append((False, error))
        thread = threading.Thread(target=scan, daemon=True)
        thread.start()
        thread.join(timeout)
        if thread.is_alive():
            scan_process._scan_failed(function, time.monotonic() - started, 'timed out')
            raise scan_process.ScanError('scan timed out')
        scan_process._scan_succeeded()
        succeeded, result = pickle.loads(pickle.dumps(outcome[0], protocol=pickle.HIGHEST_PROTOCOL))
        if not succeeded:
            raise result
        return result

    global _real_scan_run
    _real_scan_run = scan_process.run
    patch.object(scan_process, 'run', run).start()


def real_scans():
    """Restore the child-process scans for the calling test module; call it from setUpModule."""
    from coordinator import scan_process
    patcher = patch.object(scan_process, 'run', _real_scan_run)
    patcher.start()
    unittest.addModuleCleanup(patcher.stop)


def install_test_host_cleanup():
    from coordinator.host import HostClient

    launch = HostClient.launch.__func__
    initialize = subprocess.Popen.__init__

    def guard_run(run):
        @wraps(run)
        def run_test(test, *args, **kwargs):
            if _running_host_tests and _running_host_tests[-1][0] is test:
                return run(test, *args, **kwargs)
            hosts = {}
            result = None

            def stop_hosts():
                errors = []
                for tracked in list(hosts.values()):
                    try:
                        tracked.stop()
                    except Exception as error:
                        errors.append(str(error))
                if errors:
                    raise AssertionError('Test host cleanup failed: ' + '; '.join(errors))

            test.addCleanup(stop_hosts)
            _running_host_tests.append((test, hosts))
            try:
                result = run(test, *args, **kwargs)
                return result
            finally:
                try:
                    for tracked in list(hosts.values()):
                        try:
                            tracked.stop()
                        except Exception:
                            if result is None:
                                raise
                            result.addError(test, sys.exc_info())
                finally:
                    _running_host_tests.pop()
        return run_test

    @wraps(launch)
    async def launch_host(cls, directory, *args, **kwargs):
        tracked = _register_test_host(directory)
        token = _launching_test_host.set(tracked)
        try:
            return await launch(cls, directory, *args, **kwargs)
        finally:
            _launching_test_host.reset(token)
            if tracked is not None:
                tracked.capture()

    @wraps(initialize)
    def initialize_process(process, args, *fields, **options):
        initialize(process, args, *fields, **options)
        tracked = _launching_test_host.get() if options.get('start_new_session') else None
        if isinstance(args, (list, tuple)):
            if len(args) == 5 and list(args[1:4]) == ['-m', 'coordinator', 'host']:
                tracked = tracked or _register_test_host(args[-1])
            if (_running_host_tests and options.get('start_new_session')
                    and list(args[1:3]) == ['-m', 'coordinator.scan_process']):
                test, _ = _running_host_tests[-1]
                test.addCleanup(_stop_test_scan, process)
        if tracked is not None:
            tracked.launchers.append(process)

    unittest.TestCase.run = guard_run(unittest.TestCase.run)
    unittest.IsolatedAsyncioTestCase.run = guard_run(unittest.IsolatedAsyncioTestCase.run)
    HostClient.launch = classmethod(launch_host)
    subprocess.Popen.__init__ = initialize_process


async def finish_background_tasks():
    """Cancel and await the service tasks and executor threads a test left running, so no scan writes after its folder is removed."""
    for _ in range(20):
        pending = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
        if not pending:
            break
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        await asyncio.sleep(0)
    await asyncio.get_running_loop().shutdown_default_executor()


def stop_test_hosts(root):
    root = Path(root).resolve()
    for _, hosts in _running_host_tests:
        for directory, tracked in list(hosts.items()):
            if directory.is_relative_to(root):
                tracked.stop()


def short_host_stop(test, timeout=1):
    from coordinator.host import HostClient
    launch = HostClient.launch

    async def shorter(*args, **kwargs):
        return await launch(*args, stop_timeout=timeout, **kwargs)

    change = patch.object(HostClient, 'launch', side_effect=shorter)
    change.start()
    test.addCleanup(change.stop)


def _group_alive(pid):
    return any(group == pid and not status.startswith('Z')
               for _, group, status in _process_table().values())


def _stop_group(pid, sig=signal.SIGTERM):
    if _group_alive(pid):
        try:
            os.killpg(pid, sig)
        except ProcessLookupError:
            pass
        except PermissionError:
            if _group_alive(pid):
                raise


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


def origin_codex_skills_pointer():
    """Read the note assignment captured from origin/main at a40d3d29a2ba1ade88e452b260e8b94398691e0d."""
    source = 'CODEX_SKILLS_POINTER = (\n    "Skills: this launch has no list of installed skills, so read a skill\'s SKILL.md before you use it. "\n    "If CODEX_HOME is unset, use ~/.codex for the paths below. "\n    "A skill named <plugin>:<name> is normally at "\n    "$CODEX_HOME/plugins/cache/<source>/<plugin>/<version>/skills/<name>/SKILL.md; "\n    "use local if present, otherwise the newest version (semantic versions, else lexical order). "\n    "For pstack, <source>/<plugin> is pstack-claude/pstack. "\n    "Only enabled plugins supply skills; their manifests can set other skill folders. "\n    "Legacy plugins can also use <version>/.codex-plugin/migrated-command-skills/. "\n    "Before you start, list existing .agents/skills/ and .codex/skills/ "\n    "from your project root to your working folder, ~/.agents/skills/, "\n    "$CODEX_HOME/skills/, $CODEX_HOME/skills/.system/, and /etc/codex/skills/. "\n    "Read the SKILL.md of each skill there that fits your task."\n)'
    return ast.literal_eval(ast.parse(source).body[0].value)


def expected_codex_skills_pointer(home, codex_home):
    return (origin_codex_skills_pointer()
            .replace("If CODEX_HOME is unset, use ~/.codex for the paths below. ", "")
            .replace("$CODEX_HOME", str(codex_home)).replace("~/", str(home) + "/"))
