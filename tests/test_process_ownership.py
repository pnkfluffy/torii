"""Portable ownership contracts and opt-in launchd acceptance probes."""

import asyncio
from contextlib import ExitStack
from dataclasses import replace
import errno
import json
import os
from pathlib import Path
import plistlib
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import host, opencode_runtime, process_ownership as ownership, worker_storage
from coordinator.host import CleanupFailed, HostClient, HostDied


class FakeBackend:
    def __init__(self, processes=()):
        self.table = {item.pid: item for item in processes}
        self.signals = []
        self.accept_stale = False
        self.sticky = False
        self.on_signal = None
        self.now = 0
        self.boot_id = 'boot-one'
        self.bootouts = []

    def load(self):
        return self

    def uid(self):
        return 501

    def pid(self):
        return 100

    def metadata(self, pid):
        return self.table.get(pid)

    def processes(self):
        return list(self.table.values())

    def boot(self):
        return self.boot_id

    def clock(self):
        return self.now

    def sleep(self, duration):
        self.now += duration

    def signal(self, process, number):
        self.signals.append((process.pid, process.version, number))
        if self.on_signal:
            self.on_signal(process, number)
        current = self.table.get(process.pid)
        if current is None or current.version != process.version:
            return 0 if self.accept_stale else errno.ESRCH
        if number == signal.SIGKILL and not self.sticky:
            self.table.pop(process.pid)
        return 0

    def bootout(self, label):
        self.bootouts.append(label)


def member(pid=100, unique=1000, version=7, resource=90, uid=501):
    return ownership.Process(pid, unique, version, resource, 91, uid, 20)


def save_owner(directory, process=None, **fields):
    process = process or member()
    record = dict(boot='boot-one', label='local.torii-host.test', pid=process.pid,
                  unique=process.unique, resource=process.resource, jetsam=process.jetsam,
                  cleanup='pending', **fields)
    ownership.write(directory / 'owner.json', record)
    return record


class FakeHostBackend(FakeBackend):
    def __init__(self, directory, sticky=False, unavailable=False):
        self.directory = Path(directory)
        self.host = member(os.getpid(), uid=os.getuid())
        super().__init__([self.host])
        self.sticky = sticky
        self.unavailable = unavailable
        self.added = False

    def uid(self):
        return os.getuid()

    def pid(self):
        return os.getpid()

    def processes(self):
        if self.unavailable:
            return [self.host, replace(self.host, pid=self.host.pid + 1, unique=1001)]
        if (self.directory / 'child.pid').exists() and not self.added:
            self.table[9000000] = member(9000000, unique=1001, uid=self.uid())
            self.added = True
        return super().processes()

    def signal(self, process, number):
        result = super().signal(process, number)
        with (self.directory / 'signals.jsonl').open('a') as stream:
            stream.write(json.dumps([process.pid, process.version, number, result]) + '\n')
        return result


class FakeJobs(FakeBackend):
    def __init__(self, sticky=False, unavailable=False, early=False, no_handoff=False):
        super().__init__()
        self.children = []
        self.plists = []
        self.sticky_host = sticky
        self.unavailable = unavailable
        self.early = early
        self.no_handoff = no_handoff
        self.directories = {}

    def uid(self):
        return os.getuid()

    def metadata(self, pid):
        for child in self.children:
            if child.pid == pid and child.poll() is None:
                return member(pid, uid=self.uid())
        return super().metadata(pid)

    def bootstrap(self, path):
        value = plistlib.loads(path.read_bytes())
        self.plists.append(value)
        args = value['ProgramArguments']
        directory = Path(args[-1])
        self.directories[value['Label']] = directory
        script = ('import sys; from pathlib import Path; '
                  'from coordinator import host, process_ownership; '
                  'from tests.test_process_ownership import FakeHostBackend; '
                  'process_ownership.backend=FakeHostBackend(Path(sys.argv[1]),%r,%r); '
                  'host.main(sys.argv[1])') % (self.sticky_host, self.unavailable)
        if self.early:
            script = 'raise SystemExit(2)'
        if self.no_handoff:
            script = 'import time; time.sleep(30)'
        with (directory / 'fake-host-errors.log').open('wb') as errors:
            child = subprocess.Popen([args[0], '-c', script, str(directory)], cwd=value['WorkingDirectory'],
                                     env=value['EnvironmentVariables'], stdin=subprocess.DEVNULL,
                                     stdout=subprocess.DEVNULL, stderr=errors, start_new_session=True)
        self.children.append(child)

    def job_state(self, label):
        return 'running' if self.children[-1].poll() is None else 'exited'

    def bootout(self, label):
        super().bootout(label)
        for child in self.children:
            if child.poll() is None:
                child.terminate()
            child.wait(timeout=10)

    def close(self):
        for child in self.children:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=10)


def remove_test_alias(directory):
    alias, _ = host._socket_path(directory)
    if alias.is_symlink() and alias.resolve() == directory.resolve():
        alias.unlink()


class PrintBackend(ownership.DarwinBackend):
    def __init__(self, text='', returncode=0):
        super().__init__()
        self.result = subprocess.CompletedProcess([], returncode, text, '')

    def uid(self):
        return 501

    def _launchctl(self, *args):
        return self.result


class LaunchctlTests(unittest.TestCase):
    def test_T7_every_launchd_print_form(self):
        forms = {
            'pid = 123\nlast exit code = (never exited)': 'running',
            'last exit code = (never exited)': 'pending',
            'state = spawn scheduled': 'pending',
            'last exit code = (abandoned)': 'exited',
            'last exit code = (failed reap)': 'exited',
            'last terminating signal = Killed: 9': 'exited',
            'last exit code = 1: Operation not permitted': 'exited',
            'last exit code = 0': 'exited',
            'last exit code = (extension watchdogged)': 'exited',
        }
        for text, state in forms.items():
            with self.subTest(text=text):
                output = 'gui/501/test = {\n\t' + text.replace('\n', '\n\t') + '\n}\n'
                self.assertEqual(PrintBackend(output).job_state('test'), state)
        self.assertEqual(PrintBackend('pid = 123', 113).job_state('test'), 'missing')

    def test_launchctl_timeouts_have_operation_results(self):
        backend = ownership.DarwinBackend()
        for operation, argument in (('bootstrap', Path('test.plist')), ('job_state', 'test'), ('bootout', 'test')):
            with self.subTest(operation=operation), patch.object(backend, '_launchctl',
                    side_effect=subprocess.TimeoutExpired('launchctl', 15)):
                if operation == 'job_state':
                    self.assertEqual(backend.job_state(argument), 'missing')
                else:
                    with self.assertRaisesRegex(OSError, operation + ' timed out'):
                        getattr(backend, operation)(argument)
        with patch.object(ownership.subprocess, 'run', side_effect=subprocess.TimeoutExpired('launchctl', 15)):
            self.assertEqual(backend.job_state('test'), 'missing')
            with self.assertRaises(OSError):
                backend.bootstrap(Path('test.plist'))
            with self.assertRaises(OSError):
                backend.bootout('test')
        failed = subprocess.CompletedProcess([], 5, '', '')
        with patch.object(backend, '_launchctl', side_effect=[failed, subprocess.TimeoutExpired('launchctl', 15)]):
            with self.assertRaisesRegex(OSError, 'bootout timed out'):
                backend.bootout('test')


class OwnershipFakeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.fake = FakeBackend([member(), member(101, 1001)])
        backend = patch.object(ownership, 'backend', self.fake)
        backend.start()
        self.addCleanup(backend.stop)
        save_owner(self.root)

    async def test_01_selection_is_exact(self):
        self.fake.table.update({102: member(102, resource=99), 103: member(103, unique=999),
                                104: member(104, uid=502)})
        result = ownership.cleanup(self.root)
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(set(self.fake.table), {102, 103, 104})
        self.assertEqual({pid for pid, _, _ in self.fake.signals}, {100, 101})

    async def test_02_freeze_rescans_new_children(self):
        def fork(process, number):
            if number == signal.SIGSTOP and process.version == 7 and 102 not in self.fake.table:
                self.fake.table[102] = member(102, unique=1002)
        self.fake.on_signal = fork
        self.assertEqual(ownership.cleanup(self.root)['status'], 'ok')
        valid = [(pid, number) for pid, version, number in self.fake.signals if version == 7]
        self.assertIn((102, signal.SIGSTOP), valid)
        self.assertLess(valid.index((102, signal.SIGSTOP)),
                        next(index for index, (_, number) in enumerate(valid) if number == signal.SIGKILL))
        self.assertEqual({pid for pid, number in valid if number == signal.SIGKILL}, {100, 101, 102})

    async def test_03_version_race_never_signals_a_bare_pid(self):
        changed = []
        def race(process, number):
            if number == signal.SIGSTOP and process.pid == 101 and not changed:
                self.fake.table[101] = replace(self.fake.table[101], version=9)
                changed.append(True)
        self.fake.on_signal = race
        with patch.object(ownership.os, 'kill', side_effect=AssertionError('bare PID signal')) as bare:
            result = ownership.cleanup(self.root)
        self.assertEqual(result['status'], 'ok', result)
        bare.assert_not_called()
        self.assertIn((101, 7, signal.SIGSTOP), self.fake.signals)
        self.assertIn((101, 9, signal.SIGSTOP), self.fake.signals)
        self.assertIn((101, 9, signal.SIGKILL), self.fake.signals)

    async def test_04_bounded_failure_persists_and_reports_once(self):
        self.fake.table = {101: member(101, unique=1001)}
        self.fake.sticky = True
        result = ownership.cleanup(self.root)
        self.assertEqual((result['status'], result['remaining']), ('failed', 1))
        self.assertLess(self.fake.now, ownership.STOP_TIMEOUT + .1)
        self.assertEqual(ownership.read_owner(self.root)['cleanup'], 'failed')
        problems = []
        ownership.report(self.root, lambda *args: problems.append(args))
        ownership.report(self.root, lambda *args: problems.append(args))
        self.assertEqual(len(problems), 1)
        self.assertEqual(problems[0][:2], ('worker', 'ownership-cleanup'))
        self.assertIn('remaining=1', problems[0][2])

    async def test_05_stale_token_acceptance_refuses_all_valid_signals(self):
        self.fake.accept_stale = True
        result = ownership.cleanup(self.root)
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(self.fake.signals, [(100, 8, signal.SIGSTOP)])
        self.assertEqual(set(self.fake.table), {100, 101})

    async def test_06_boot_mismatch_signals_nothing(self):
        self.fake.boot_id = 'boot-two'
        self.assertEqual(ownership.cleanup(self.root)['status'], 'stale')
        self.assertEqual(self.fake.signals, [])
        self.assertEqual(ownership.read_owner(self.root)['cleanup'], 'stale')

    async def test_07_startup_refuses_shared_coalition_or_broken_tokens(self):
        for shared in (True, False):
            with self.subTest(shared=shared):
                directory = self.root / str(shared)
                directory.mkdir()
                ownership.write(directory / 'spec.json', {'provider': 'opencode', 'ownership': 'coalition',
                    'argv': [sys.executable, '-c', 'raise RuntimeError()'], 'cwd': str(self.root), 'env': {}})
                ownership.create_job(directory, [], self.root, '', 'test')
                self.fake.table = {100: member()}
                if shared:
                    self.fake.table[101] = member(101, 1001)
                self.fake.accept_stale = not shared
                with patch.object(host.asyncio, 'create_subprocess_exec',
                                  AsyncMock(side_effect=AssertionError('Provider started before ownership validation'))) as spawn:
                    await host._serve(directory, {})
                spawn.assert_not_awaited()
                self.assertEqual(json.loads((directory / 'exit.json').read_text())['failure_code'],
                                 'ownership_unavailable')
                self.assertFalse((directory / 'owner.json').exists())

    def spec(self, script='import sys; sys.stdin.read()'):
        return dict(provider='opencode', ownership='coalition', argv=[sys.executable, '-c', script],
                    cwd=str(self.root), env={'PATH': os.environ.get('PATH', '/usr/bin:/bin')})

    async def launch(self, private=None, **options):
        jobs = FakeJobs(**options)
        self.addCleanup(jobs.close)
        backend = patch.object(ownership, 'backend', jobs)
        backend.start()
        self.addCleanup(backend.stop)
        client = await HostClient.launch(self.root / 'owned', self.spec(), private=private)
        self.addAsyncCleanup(client.stop)
        return client, jobs

    async def test_08_launch_plist_and_private_handoff(self):
        secret = 'private-test-value-87'
        forms = [(secret, '[private]'), ('private%2Dtest%2Dvalue%2D87', '[private]')]
        jobs = FakeJobs()
        self.addCleanup(jobs.close)
        spec = self.spec('import os,sys; print("received" if os.environ.get("TEST_PRIVATE")==sys.stdin.readline().strip() else "bad",flush=True)')
        directory = self.root / 'long-host-folder-name-that-needs-the-short-socket-alias'
        with patch.object(ownership, 'backend', jobs):
            client = await HostClient.launch(directory, spec, private={'env': {'TEST_PRIVATE': secret}, 'scrub': forms})
            try:
                await client.request('private_write', line=secret, scrub=forms)
                try:
                    self.assertEqual(await client.next_event(), 'received')
                except HostDied:
                    self.fail((directory / 'fake-host-errors.log').read_text())
                await client.wait()
            finally:
                await client.stop()
                client.detach()
        value = jobs.plists[0]
        self.assertEqual(set(value), {'Label', 'ProgramArguments', 'WorkingDirectory', 'RunAtLoad',
            'ProcessType', 'StandardOutPath', 'StandardErrorPath', 'EnvironmentVariables'})
        self.assertEqual(value['Label'], 'local.torii-host.' + host._socket_path(directory)[0].name[6:])
        self.assertEqual(value['ProgramArguments'], [sys.executable, '-m', 'coordinator', 'host', str(directory)])
        self.assertEqual(value['EnvironmentVariables'], {'PYTHONPATH': str(Path(host.__file__).resolve().parents[1])})
        self.assertEqual((value['RunAtLoad'], value['ProcessType'], value['StandardOutPath'], value['StandardErrorPath']),
                         (True, 'Interactive', '/dev/null', '/dev/null'))
        for path in directory.iterdir():
            if path.is_file():
                for form, _ in forms:
                    self.assertNotIn(form.encode(), path.read_bytes(), str(path))
        self.assertFalse((directory / 'private.sock').exists())
        self.assertEqual(directory.stat().st_mode & 0o777, 0o700)
        self.assertEqual((directory / 'owner.json').stat().st_mode & 0o777, 0o600)

    async def test_09_owner_stop_cleans_before_exit_and_boots_out_once(self):
        client, jobs = await self.launch()
        await client.stop()
        self.assertEqual(json.loads((client.directory / 'exit.json').read_text())['cleanup'], 'ok')
        self.assertEqual(ownership.read_owner(client.directory)['cleanup'], 'ok')
        self.assertFalse(host.alive(client.child_pid))
        await client.stop()
        self.assertEqual(len(jobs.bootouts), 1)
        rows = [json.loads(line) for line in (client.directory / 'signals.jsonl').read_text().splitlines()]
        self.assertTrue(any(row[0] == 9000000 and row[2] == signal.SIGKILL for row in rows))

    def dead_host(self, name):
        directory = self.root / name
        directory.mkdir()
        save_owner(directory)
        ownership.create_job(directory, [], self.root, '', name)
        ownership.write(directory / 'pid', 100)
        (directory / 'events.jsonl').write_text('')
        return directory

    async def test_10_dead_host_cleanup_precedes_hostdied(self):
        for operation in ('wait', 'stop', 'attach', 'next_event'):
            with self.subTest(operation=operation):
                self.fake.table = {101: member(101, 1001)}
                directory = self.dead_host(operation)
                client = HostClient(directory)
                try:
                    with patch.object(host, 'alive', return_value=False):
                        if operation == 'stop':
                            await client.stop()
                        else:
                            with self.assertRaises(HostDied):
                                if operation == 'attach':
                                    HostClient.attach(directory)
                                else:
                                    await getattr(client, operation)()
                    self.assertEqual(self.fake.table, {})
                    self.assertEqual(ownership.read_owner(directory)['cleanup'], 'ok')
                    self.assertTrue((directory / 'job-booted-out').exists())
                finally:
                    client.detach()

    async def test_11_running_reattach_signals_nothing(self):
        directory = self.dead_host('running')
        with patch.object(host, 'alive', return_value=True):
            client = HostClient.attach(directory)
            self.assertEqual(client.state, 'running')
            client.detach()
        self.assertEqual(self.fake.signals, [])
        self.assertEqual(self.fake.bootouts, [])

    async def test_unanswered_stop_kills_only_owned_processes_and_records_the_pids(self):
        directory = self.dead_host('unanswered-stop')
        self.fake.table = {100: member(), 101: member(101, unique=1001),
                           102: member(102, unique=1002, resource=99)}
        recorded = []
        client = HostClient(directory, on_problem=lambda *args: recorded.append(args))
        with patch.object(client, 'request', AsyncMock(side_effect=asyncio.TimeoutError)), \
                patch.object(host, 'alive', side_effect=lambda pid: int(pid) in self.fake.table):
            await client.stop()
        self.assertEqual(set(self.fake.table), {102})
        self.assertEqual(ownership.read_owner(directory)['cleanup'], 'ok')
        self.assertEqual(ownership.read_owner(directory)['killed_pids'], [100, 101])
        self.assertEqual([entry[:2] for entry in recorded], [('worker', 'process-killed')])
        self.assertIn('[100, 101]', recorded[0][2])
        self.assertTrue((directory / 'job-booted-out').is_file())

    async def test_exited_host_retries_pending_and_failed_ownership_cleanup(self):
        for state in ('pending', 'failed'):
            with self.subTest(state=state):
                directory = self.dead_host('unverified-exit-' + state)
                record = ownership.read_owner(directory)
                record['cleanup'] = state
                ownership.write(directory / 'owner.json', record)
                ownership.write(directory / 'exit.json', {'returncode': 1, 'cleanup': {'status': 'failed'}})
                self.fake.table = {100: member(), 101: member(101, unique=1001)}
                client = HostClient(directory)
                with patch.object(host, 'alive', return_value=False):
                    await client.stop()
                self.assertEqual(ownership.read_owner(directory)['cleanup'], 'ok')
                self.assertEqual(self.fake.table, {})

    async def test_failed_exit_cleanup_has_three_attempts_with_backoff_and_leftover_pids(self):
        directory = self.dead_host('failed-exit')
        ownership.write(directory / 'exit.json', {'returncode': 1, 'cleanup': {'status': 'failed'}})
        self.fake.sticky = True
        sleep = asyncio.sleep
        with patch.object(host, 'alive', return_value=False), \
                patch.object(ownership, 'cleanup', wraps=ownership.cleanup) as cleanup, \
                patch.object(host.asyncio, 'sleep', AsyncMock(side_effect=sleep)) as backoff:
            for _ in range(2):
                with self.assertRaises(CleanupFailed) as raised:
                    await HostClient(directory).stop()
                self.assertEqual(raised.exception.pids, [100, 101])
            self.assertEqual(cleanup.call_count, 3)
            self.assertEqual([call.args[0] for call in backoff.await_args_list], [.1, .2])
        record = ownership.read_owner(directory)
        self.assertEqual(record['cleanup_attempts'], 3)
        self.assertEqual(record['remaining_pids'], [100, 101])

    async def test_exit_cleanup_can_succeed_on_a_later_attempt(self):
        directory = self.dead_host('retry-exit')
        ownership.write(directory / 'exit.json', {'returncode': 1})
        self.fake.sticky = True
        cleanup = ownership.cleanup
        attempts = []
        def retry(path):
            attempts.append(path)
            self.fake.sticky = len(attempts) == 1
            return cleanup(path)
        with patch.object(host, 'alive', return_value=False), patch.object(ownership, 'cleanup', side_effect=retry):
            await HostClient(directory).stop()
        self.assertEqual(len(attempts), 2)
        self.assertEqual(ownership.read_owner(directory)['cleanup'], 'ok')
        self.assertEqual(ownership.read_owner(directory)['remaining_pids'], [])

    async def test_12_recovery_handles_all_leftovers(self):
        running = self.dead_host('running')
        running_process = member(200, unique=2000, resource=190)
        save_owner(running, running_process)
        self.fake.table[200] = running_process
        exited = self.dead_host('exited')
        dead = self.dead_host('dead')
        unrecorded = self.root / 'unrecorded'
        unrecorded.mkdir()
        ownership.create_job(unrecorded, [], self.root, '', 'unrecorded')
        ownership.write(exited / 'exit.json', {'returncode': 0, 'cleanup': 'ok'})
        with patch.object(host, 'host_state', side_effect=lambda directory: {
                running: 'running', exited: 'exited'}.get(directory, 'dead')):
            host.recover_ownership(self.root)
            host.recover_ownership(self.root)
        self.assertEqual(self.fake.signals[0], (100, 8, signal.SIGSTOP))
        self.assertEqual(len(self.fake.bootouts), 3)
        self.assertFalse((running / 'job-booted-out').exists())
        self.assertEqual(self.fake.table, {200: running_process})
        self.assertFalse(any(pid == 200 for pid, _, _ in self.fake.signals))
        self.assertEqual(ownership.read_owner(dead)['cleanup'], 'ok')
        self.assertTrue((unrecorded / 'job-booted-out').exists())

    async def test_13_darwin_workers_use_ownership_and_parent_launches_stay_unchanged(self):
        from tests.test_opencode_runtime import OpenCodeRuntimeTests
        from tests.test_providers import NativeProviderTests
        for platform in ('darwin', 'linux'):
            case = OpenCodeRuntimeTests('test_final_launch_removes_only_regenerable_scratch')
            resources = ExitStack()
            case.addCleanup = resources.callback
            case.setUp()
            try:
                with patch('coordinator.providers.platform', platform), \
                        patch.object(HostClient, 'launch', AsyncMock(side_effect=RuntimeError('captured'))) as launch:
                    await case.run_worker()
                self.assertEqual(launch.await_args.args[1].get('ownership'),
                                 'coalition' if platform == 'darwin' else None)
            finally:
                resources.close()
        case = NativeProviderTests('test_public_claude_launch_env_is_the_profile_env')
        resources = ExitStack()
        case.addCleanup = resources.callback
        case.setUp()
        try:
            for provider in ('claude', 'codex'):
                for platform in ('darwin', 'linux'):
                    for worker_id in (None, 9):
                        with self.subTest(provider=provider, platform=platform, worker=worker_id), \
                                patch('coordinator.providers.platform', platform), \
                                patch.object(HostClient, 'launch', AsyncMock(side_effect=RuntimeError('captured'))) as launch:
                            await case.run_provider(provider, fresh=True, worker_id=worker_id)
                        self.assertEqual(launch.await_args.args[1].get('ownership'),
                                         'coalition' if platform == 'darwin' and worker_id is not None else None)
        finally:
            case.tearDown()
            resources.close()
        for provider in ('opencode', 'claude', 'codex'):
            spec = self.spec()
            spec.pop('ownership')
            spec['provider'] = provider
            popen = subprocess.Popen
            with ExitStack() as guards:
                for name in ('create_job', 'cleanup', 'report', 'bootout'):
                    guards.enter_context(patch.object(ownership, name, side_effect=AssertionError(name + ' used')))
                for name in ('bootstrap', 'job_state', 'bootout', '_launchctl'):
                    guards.enter_context(patch.object(ownership.backend, name, create=True,
                                                     side_effect=AssertionError(name + ' used')))
                launch = guards.enter_context(patch.object(host.subprocess, 'Popen', wraps=popen))
                client = await HostClient.launch(self.root / ('popen-' + provider), spec)
                await client.stop()
                client.detach()
                host.recover_ownership(self.root)
            self.assertEqual(launch.call_count, 1)

    async def test_14_opencode_launch_cleanup_waits_for_ok(self):
        from tests.test_opencode_runtime import OpenCodeRuntimeTests
        for failed in (False, True):
            with self.subTest(failed=failed):
                case = OpenCodeRuntimeTests('test_final_launch_removes_only_regenerable_scratch')
                resources = ExitStack()
                case.addCleanup = resources.callback
                case.setUp()
                jobs = FakeJobs(sticky=failed)
                problems = []
                removals = []
                remove = worker_storage._remove_tree
                def checked_remove(root):
                    records = list((case.store.directory / 'hosts').glob('*/owner.json'))
                    removals.extend(json.loads(path.read_text())['cleanup'] for path in records)
                    return remove(root)
                try:
                    case.scenario(scratch_files=True)
                    with patch('coordinator.providers.platform', 'darwin'), patch.object(ownership, 'backend', jobs), \
                            patch.object(worker_storage, '_remove_tree', checked_remove):
                        result = await case.run_worker(on_problem=lambda *args: problems.append(args))
                        await case.cleanup_storage(result)
                    self.assertTrue(result.success, result)
                    launch = case.launch_root(result)
                    directory = Path(result.log_path).parent
                    record = ownership.read_owner(directory)
                    self.assertEqual(record['cleanup'], 'failed' if failed else 'ok')
                    self.assertEqual((launch / 'tmp/scratch').exists(), failed)
                    self.assertEqual((launch / 'home/Library/Caches/scratch').exists(), failed)
                    self.assertEqual(len([p for p in problems if p[:2] == ('worker', 'ownership-cleanup')]), int(failed))
                    self.assertEqual(removals, [] if failed else ['ok', 'ok'])
                finally:
                    jobs.close()
                    resources.close()

    async def test_launch_notices_early_exit_and_requires_handoff(self):
        for options in ({'early': True}, {'no_handoff': True}):
            with self.subTest(options=options):
                jobs = FakeJobs(**options)
                try:
                    with patch.object(ownership, 'backend', jobs), patch.object(host, 'HOST_TIMEOUT', .2):
                        with self.assertRaises(RuntimeError):
                            await HostClient.launch(self.root / str(len(options)) / str(options), self.spec())
                    self.assertEqual(len(jobs.bootouts), 1)
                    self.assertTrue(all(child.poll() is not None for child in jobs.children))
                finally:
                    jobs.close()

    async def test_ownership_unavailable_is_a_plain_run_failure(self):
        from tests.test_opencode_runtime import OpenCodeRuntimeTests
        case = OpenCodeRuntimeTests('test_final_launch_removes_only_regenerable_scratch')
        resources = ExitStack()
        case.addCleanup = resources.callback
        case.setUp()
        jobs = FakeJobs(unavailable=True)
        try:
            with patch('coordinator.providers.platform', 'darwin'), patch.object(ownership, 'backend', jobs):
                result = await case.run_worker()
            self.assertFalse(result.success)
            self.assertEqual(result.failure_code, 'ownership_unavailable')
            self.assertEqual(result.error, 'Worker process ownership is unavailable on this host.')
            self.assertEqual(case.calls(), [])
        finally:
            jobs.close()
            resources.close()

    async def test_failed_launch_boots_out_and_waits_before_record_cleanup(self):
        jobs = FakeJobs()
        spec = self.spec()
        spec['argv'] = [str(self.root / 'missing-provider')]
        directory = self.root / 'failed-launch'
        cleanup = ownership.cleanup
        checked = []
        def check(path, *args):
            self.assertEqual(len(jobs.bootouts), 1)
            self.assertTrue(all(child.poll() is not None for child in jobs.children))
            self.assertTrue((path / 'owner.json').exists())
            self.assertFalse((path / 'job-booted-out').exists())
            checked.append(True)
            return cleanup(path, *args)
        try:
            with patch.object(ownership, 'backend', jobs), patch.object(ownership, 'cleanup', check):
                with self.assertRaisesRegex(RuntimeError, 'exited before becoming ready'):
                    await HostClient.launch(directory, spec)
            self.assertEqual(checked, [True])
            self.assertEqual(ownership.read_owner(directory)['cleanup'], 'ok')
            self.assertTrue((directory / 'job-booted-out').exists())
        finally:
            jobs.close()

    async def test_T1_interrupted_plist_write_is_invisible_to_recovery(self):
        for operation in ('dump', 'fsync'):
            with self.subTest(operation=operation):
                directory = self.root / operation
                directory.mkdir()
                failure = OSError(errno.ENOSPC, 'injected full disk')
                target = ownership.plistlib if operation == 'dump' else ownership.os
                problems = []
                with patch.object(target, operation, side_effect=failure):
                    with self.assertRaises(OSError) as raised:
                        ownership.create_job(directory, [], self.root, '', operation)
                self.assertIs(raised.exception, failure)
                self.assertFalse((directory / 'job.plist').exists())
                host.recover_ownership(self.root, lambda *args: problems.append(args))
                self.assertEqual(problems, [])
                self.assertEqual(self.fake.bootouts, [])
        path = ownership.create_job(self.root, [], self.root, '', 'complete')
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(ownership.job_label(self.root), 'local.torii-host.complete')

    async def test_T2_failed_launch_preserves_original_error_and_removes_alias(self):
        for stage in ('create', 'broken-plist', 'bootstrap', 'handoff', 'readiness'):
            with self.subTest(stage=stage):
                jobs = FakeJobs(no_handoff=stage == 'handoff')
                failure = OSError(errno.ENOSPC, 'original launch failure')
                directory = self.root / stage
                self.addCleanup(remove_test_alias, directory)
                problems = []
                spec = self.spec()
                if stage == 'readiness':
                    spec['argv'] = [str(self.root / 'missing-provider')]
                def broken(*args):
                    (directory / 'job.plist').write_bytes(b'broken')
                    raise failure
                try:
                    with ExitStack() as stack:
                        stack.enter_context(patch.object(ownership, 'backend', jobs))
                        stack.enter_context(patch.object(host, 'HOST_TIMEOUT', .2 if stage == 'handoff' else 2))
                        if stage == 'create':
                            stack.enter_context(patch.object(ownership.plistlib, 'dump', side_effect=failure))
                        elif stage == 'broken-plist':
                            stack.enter_context(patch.object(ownership, 'create_job', side_effect=broken))
                        elif stage == 'bootstrap':
                            stack.enter_context(patch.object(jobs, 'bootstrap', side_effect=failure))
                        if stage in ('bootstrap', 'handoff', 'readiness'):
                            stack.enter_context(patch.object(jobs, 'bootout', side_effect=OSError('rollback failed')))
                        with self.assertRaises((OSError, RuntimeError)) as raised:
                            await HostClient.launch(directory, spec, on_problem=lambda *args: problems.append(args))
                    if stage in ('create', 'broken-plist', 'bootstrap'):
                        self.assertIs(raised.exception, failure)
                        self.assertEqual(jobs.plists, [])
                    else:
                        self.assertIn('handoff did not finish' if stage == 'handoff' else 'exited before becoming ready',
                                      str(raised.exception))
                    self.assertFalse(host._socket_path(directory)[0].is_symlink())
                finally:
                    jobs.close()

    async def test_T3_recovery_isolates_each_damaged_folder(self):
        errors = {'bootout-oserror': OSError('bootout failed'),
                  'bootout-timeout': subprocess.TimeoutExpired('launchctl', 15)}
        for name, data in (('empty', b''), ('partial', b'<?xml version="1.0"?><plist>')):
            directory = self.root / name
            directory.mkdir()
            (directory / 'job.plist').write_bytes(data)
        for name in errors:
            self.dead_host(name)
        healthy = self.dead_host('healthy')
        ownership.write(healthy / 'exit.json', {'returncode': 0})
        problems = []
        bootout = self.fake.bootout
        def fail_some(label):
            name = label.rsplit('.', 1)[-1]
            if name in errors:
                raise errors[name]
            bootout(label)
        with patch.object(self.fake, 'bootout', side_effect=fail_some), \
                patch.object(host, 'alive', return_value=False):
            host.recover_ownership(self.root, lambda *args: problems.append(args))
        self.assertEqual(len(problems), 5)
        self.assertEqual([entry[2] for entry in problems if entry[1] == 'process-killed'],
                         ['SIGKILL pids=[100, 101]'])
        for name in ('empty', 'partial', *errors):
            self.assertEqual(sum(name + ':' in detail for _, _, detail in problems), 1, problems)
        self.assertTrue(all(area == 'worker' for area, _, _ in problems))
        self.assertTrue((healthy / 'job-booted-out').exists())
        self.assertEqual(self.fake.bootouts, ['local.torii-host.healthy'])

    async def test_recovery_contains_state_and_owner_errors(self):
        broken = self.dead_host('broken-owner')
        (broken / 'owner.json').write_text('{')
        healthy = self.dead_host('healthy')
        problems = []
        with patch.object(host, 'alive', return_value=False):
            host.recover_ownership(self.root, lambda *args: problems.append(args))
        self.assertEqual(len(problems), 2)
        self.assertEqual(problems[0][:2], ('worker', 'ownership-recovery'))
        self.assertIn('broken-owner', problems[0][2])
        self.assertEqual(problems[1], ('worker', 'process-killed', 'SIGKILL pids=[100, 101]'))
        self.assertTrue((healthy / 'job-booted-out').exists())
        with patch.object(host, 'host_state', side_effect=RuntimeError('state unreadable')):
            host.recover_ownership(self.root, lambda *args: problems.append(args))
        self.assertEqual(len(problems), 3)

    async def test_T5_clean_run_keeps_result_and_scratch_rules_when_bootout_fails(self):
        from tests.test_opencode_runtime import OpenCodeRuntimeTests
        for failure in (OSError('bootout failed'), subprocess.TimeoutExpired('launchctl', 15)):
            with self.subTest(failure=type(failure).__name__), ExitStack() as resources:
                case = OpenCodeRuntimeTests('test_final_launch_removes_only_regenerable_scratch')
                case.addCleanup = resources.callback
                case.setUp()
                jobs = FakeJobs()
                resources.callback(jobs.close)
                problems = []
                case.scenario(scratch_files=True)
                with patch('coordinator.providers.platform', 'darwin'), patch.object(ownership, 'backend', jobs):
                    with patch.object(jobs, 'bootout', side_effect=failure) as failed:
                        result = await case.run_worker(on_problem=lambda *args: problems.append(args))
                        self.assertEqual((result.success, result.failure_code), (True, None), result)
                        await case.cleanup_storage(result)
                        case.assert_launch_cleaned(result)
                        directory = Path(result.log_path).parent
                        self.assertFalse((directory / 'job-booted-out').exists())
                        self.assertEqual(failed.call_count, 1)
                    self.assertEqual([row[:2] for row in problems],
                                     [('worker', 'process-killed'), ('worker', 'ownership-bootout')])
                    self.assertIn('9000000', problems[0][2])
                    host.recover_ownership(case.store.directory / 'hosts', lambda *args: problems.append(args))
                    self.assertTrue((directory / 'job-booted-out').exists())
                    self.assertEqual(len(jobs.bootouts), 1)
                    self.assertEqual(len(problems), 2)

    async def test_clean_exit_next_event_wait_stop_and_attach_ignore_bootout_failure(self):
        for operation in ('next_event', 'wait', 'stop', 'attach'):
            for failure in (OSError('bootout failed'), subprocess.TimeoutExpired('launchctl', 15)):
                with self.subTest(operation=operation, failure=type(failure).__name__):
                    directory = self.dead_host(operation + type(failure).__name__)
                    record = ownership.read_owner(directory)
                    record['cleanup'] = 'ok'
                    ownership.write(directory / 'owner.json', record)
                    ownership.write(directory / 'exit.json', {'returncode': 23, 'failure_code': 'provider_error'})
                    problems = []
                    client = HostClient(directory, on_problem=lambda *args: problems.append(args))
                    try:
                        with patch.object(self.fake, 'bootout', side_effect=failure), \
                                patch.object(host, 'alive', return_value=False):
                            if operation == 'attach':
                                client = HostClient.attach(directory, on_problem=lambda *args: problems.append(args))
                            else:
                                result = await getattr(client, operation)()
                                self.assertEqual(result, 23 if operation == 'wait' else None)
                            await client.stop()
                        self.assertEqual(client.returncode, 23)
                        self.assertEqual(json.loads((directory / 'exit.json').read_text())['failure_code'], 'provider_error')
                        self.assertEqual([row[:2] for row in problems], [('worker', 'ownership-bootout')])
                        self.assertFalse((directory / 'job-booted-out').exists())
                    finally:
                        client.detach()

    async def test_T6_recovery_retries_only_unfinished_cleanup(self):
        for state in ('ok', 'stale', 'pending', 'failed'):
            with self.subTest(state=state):
                directory = self.dead_host(state)
                record = ownership.read_owner(directory)
                record['cleanup'] = state
                ownership.write(directory / 'owner.json', record)
                self.fake.table = {101: member(101, 1001)}
                self.fake.signals.clear()
                problems = []
                with patch.object(self.fake, 'bootout', side_effect=OSError('retry later')), \
                        patch.object(host, 'alive', return_value=False):
                    host.recover_ownership(self.root, lambda *args: problems.append(args))
                    first = list(self.fake.signals)
                    host.recover_ownership(self.root, lambda *args: problems.append(args))
                self.assertEqual(self.fake.signals, first)
                self.assertEqual(bool(first), state in ('pending', 'failed'))
                self.assertEqual(sum(row[1] == 'ownership-cleanup' for row in problems), int(state == 'stale'))
        failed = self.root / 'failed'
        record = ownership.read_owner(failed)
        record['cleanup'] = 'failed'
        ownership.write(failed / 'owner.json', record)
        self.fake.table = {101: member(101, 1001)}
        self.fake.sticky = True
        with patch.object(self.fake, 'bootout', side_effect=OSError('retry later')), \
                patch.object(host, 'alive', return_value=False):
            host.recover_ownership(self.root)
            count = len(self.fake.signals)
            host.recover_ownership(self.root)
        self.assertGreater(len(self.fake.signals), count)

    async def test_T7_launch_waits_through_spawn_scheduled_polls(self):
        class ScheduledJobs(FakeJobs):
            def bootstrap(self, path):
                self.path = path
                self.polls = 0

            def job_state(self, label):
                self.polls += 1
                if self.polls <= 2:
                    return PrintBackend('last exit code = (never exited)').job_state(label)
                if not self.children:
                    super().bootstrap(self.path)
                return super().job_state(label)
        jobs = ScheduledJobs()
        self.addCleanup(jobs.close)
        with patch.object(ownership, 'backend', jobs):
            client = await HostClient.launch(self.root / 'scheduled', self.spec())
            try:
                self.assertGreaterEqual(jobs.polls, 3)
                self.assertEqual(client.state, 'running')
                await client.stop()
            finally:
                client.detach()

    async def test_backend_failure_never_escapes_cleanup(self):
        with patch.object(self.fake, 'processes', side_effect=OSError('metadata denied')):
            self.assertEqual(ownership.cleanup(self.root)['status'], 'failed')

    async def test_support_is_lazy_and_linux_is_false(self):
        with patch.object(ownership.sys, 'platform', 'linux'), patch.object(self.fake, 'load') as load:
            self.assertFalse(ownership.supported())
            load.assert_not_called()


def register_probe(root, role):
    info = ownership.DarwinBackend().metadata(os.getpid())
    row = {'pid': info.pid, 'unique': info.unique, 'role': role}
    descriptor = os.open(str(Path(root) / 'pids.jsonl'), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(descriptor, (json.dumps(row) + '\n').encode())
    finally:
        os.close(descriptor)


def probe_records(root):
    path = Path(root) / 'pids.jsonl'
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


class GuardedDarwinBackend(ownership.DarwinBackend):
    def __init__(self, root):
        super().__init__()
        self.root = Path(root)

    def signal(self, process, number):
        known = {(row['pid'], row['unique']) for row in probe_records(self.root)}
        refused = (process.pid, process.unique) not in known
        row = {'pid': process.pid, 'unique': process.unique, 'version': process.version,
               'signal': number, 'refused': refused}
        descriptor = os.open(str(self.root / 'audit-signals.jsonl'),
                             os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(descriptor, (json.dumps(row) + '\n').encode())
        finally:
            os.close(descriptor)
        if refused:
            raise AssertionError('Refused a process outside this ownership probe')
        return super().signal(process, number)


def probe_host_main(directory, root):
    root = Path(root)
    register_probe(root, 'host')
    ownership.backend = GuardedDarwinBackend(root)
    write = host._write

    def observed_exit(path, value):
        if path.name == 'exit.json' and value.get('cleanup') == 'ok':
            writers = [row for row in probe_records(root)
                       if row['role'] in ('plain', 'group', 'setsid', 'double')]
            remaining = [row['pid'] for row in writers if host.alive(row['pid'])]
            observation = {'remaining_writers': remaining, 'exit_exists': path.exists(),
                           'owner_cleanup': ownership.read_owner(path.parent)['cleanup']}
            (root / 'before-exit.json').write_text(json.dumps(observation))
            if remaining:
                raise AssertionError('Host wrote exit before its writers were gone')
        write(path, value)

    host._write = observed_exit
    host.main(directory)


class DarwinJobs(GuardedDarwinBackend):
    def bootstrap(self, path):
        value = plistlib.loads(path.read_bytes())
        if not value['Label'].startswith('local.torii-host-test.'):
            raise AssertionError('Refused a non-test launchd label')
        args = value['ProgramArguments']
        script = ('import sys; from tests.test_process_ownership import probe_host_main; '
                  'probe_host_main(sys.argv[1],sys.argv[2])')
        value['ProgramArguments'] = [args[0], '-c', script, args[-1], str(self.root)]
        path.write_bytes(plistlib.dumps(value))
        super().bootstrap(path)

    def bootout(self, label):
        if not label.startswith('local.torii-host-test.'):
            raise AssertionError('Refused a non-test launchd label')
        super().bootout(label)

    def job_state(self, label):
        if not label.startswith('local.torii-host-test.'):
            raise AssertionError('Refused a non-test launchd label')
        return super().job_state(label)


PROVIDER_PROBE = '''import os,sys,time
from pathlib import Path
from tests.test_process_ownership import register_probe
root=Path(sys.argv[1])
mode=sys.argv[2]
def record(role):
    register_probe(root,role)
def ticker(role,recorded=False):
    if not recorded:
        record(role)
    for fd in (0,1,2):
        os.close(fd)
    end=time.monotonic()+60
    while time.monotonic()<end:
        with (root/(role+'.sentinel')).open('a') as stream:
            stream.write('tick\\n')
        time.sleep(.05)
    os._exit(0)
if mode=='control':
    ticker('control')
record('provider')
pid=os.fork()
if pid==0:
    ticker('plain')
pid=os.fork()
if pid==0:
    record('group')
    os.setpgid(0,0)
    ticker('group',True)
pid=os.fork()
if pid==0:
    record('setsid')
    os.setsid()
    ticker('setsid',True)
pid=os.fork()
if pid==0:
    record('first-fork')
    os.setsid()
    pid=os.fork()
    if pid==0:
        ticker('double')
    os._exit(0)
if mode=='exit':
    end=time.monotonic()+30
    while not (root/'exit-provider').exists() and time.monotonic()<end:
        time.sleep(.02)
else:
    sys.stdin.read()
'''


@unittest.skipUnless(sys.platform == 'darwin' and os.environ.get('TORII_OWNERSHIP_DARWIN') == '1',
                     'Set TORII_OWNERSHIP_DARWIN=1 for isolated launchd ownership checks')
class DarwinOwnershipTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        base = Path(os.environ['TMPDIR']).parent / 'darwin'
        base.mkdir(parents=True, exist_ok=True)
        self.root = Path(tempfile.mkdtemp(prefix=self._testMethodName + '-', dir=base))
        self.backend = DarwinJobs(self.root)
        self.backend_patch = patch.object(ownership, 'backend', self.backend)
        self.backend_patch.start()
        self.addCleanup(self.backend_patch.stop)
        prefix = patch.object(ownership, 'LABEL_PREFIX', 'local.torii-host-test.')
        prefix.start()
        self.addCleanup(prefix.stop)
        self.script = self.root / 'provider.py'
        self.script.write_text(PROVIDER_PROBE)
        self.client = None
        self.control = None
        self.addAsyncCleanup(self.cleanup_probe)

    async def until(self, check, seconds=15):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            value = check()
            if value:
                return value
            await asyncio.sleep(.02)
        self.fail('Ownership probe deadline expired')

    async def start(self, mode):
        env = {'PATH': '/usr/bin:/bin', 'PYTHONPATH': str(Path(host.__file__).resolve().parents[1])}
        self.control = subprocess.Popen([sys.executable, str(self.script), str(self.root), 'control'],
                                        env=env, start_new_session=True)
        spec = dict(provider='opencode', ownership='coalition', env=env, cwd=str(self.root),
                    argv=[sys.executable, str(self.script), str(self.root), mode])
        self.client = await HostClient.launch(self.root / 'host', spec)
        wanted = {'plain', 'group', 'setsid', 'double', 'control', 'provider', 'host', 'first-fork'}
        await self.until(lambda: wanted.issubset({row['role'] for row in probe_records(self.root)}))
        await self.until(lambda: all((self.root / (role + '.sentinel')).exists()
                                    for role in ('plain', 'group', 'setsid', 'double', 'control')))
        return self.client

    def sizes(self):
        return {role: (self.root / (role + '.sentinel')).stat().st_size
                for role in ('plain', 'group', 'setsid', 'double')}

    async def assert_cleaned(self, exited=True):
        rows = probe_records(self.root)
        writers = [row for row in rows if row['role'] in ('plain', 'group', 'setsid', 'double')]
        await self.until(lambda: all(not host.alive(row['pid']) for row in writers))
        before = self.sizes()
        control = (self.root / 'control.sentinel').stat().st_size
        await asyncio.sleep(3)
        self.assertEqual(self.sizes(), before)
        self.assertTrue(all(before.values()))
        self.assertIsNone(self.control.poll())
        self.assertGreater((self.root / 'control.sentinel').stat().st_size, control)
        if exited:
            self.assertEqual(json.loads((self.client.directory / 'exit.json').read_text())['cleanup'], 'ok')
            self.assertEqual(json.loads((self.root / 'before-exit.json').read_text()),
                             {'remaining_writers': [], 'exit_exists': False, 'owner_cleanup': 'ok'})
        self.assertEqual(ownership.read_owner(self.client.directory)['cleanup'], 'ok')
        self.assertEqual(self.backend.job_state(ownership.job_label(self.client.directory)), 'missing')
        signals = [json.loads(line) for line in (self.root / 'audit-signals.jsonl').read_text().splitlines()]
        self.assertTrue(signals)
        self.assertFalse(any(row['refused'] for row in signals))
        control_pid = next(row['pid'] for row in rows if row['role'] == 'control')
        self.assertFalse(any(row['pid'] == control_pid for row in signals))

    async def cleanup_probe(self):
        errors = []
        if self.client is not None:
            try:
                await self.client.stop()
            except Exception as error:
                errors.append(str(error))
            finally:
                self.client.detach()
        directory = self.root / 'host'
        if (directory / 'job.plist').exists():
            ownership.bootout(directory)
        for row in probe_records(self.root):
            current = self.backend.metadata(row['pid'])
            if current is not None and current.unique == row['unique']:
                self.backend.signal(current, signal.SIGKILL)
        if self.control is not None:
            await asyncio.to_thread(self.control.wait, 10)
        await self.until(lambda: all(not host.alive(row['pid']) for row in probe_records(self.root)))
        self.assertFalse(any(row['refused'] for row in [json.loads(line) for line in
            (self.root / 'audit-signals.jsonl').read_text().splitlines()])
            if (self.root / 'audit-signals.jsonl').exists() else False)
        print('DARWIN_AUDIT %s recorded_pids=%s remaining_pids=0 jobs=0' % (
            self._testMethodName, len({row['pid'] for row in probe_records(self.root)})))
        self.assertEqual(errors, [])
        remove_test_alias(directory)

    async def test_D1_owner_stop(self):
        client = await self.start('stop')
        await client.stop()
        await self.assert_cleaned()

    async def test_D2_host_killed(self):
        client = await self.start('stop')
        current = self.backend.metadata(int((client.directory / 'pid').read_text()))
        self.assertEqual(self.backend.signal(current, signal.SIGKILL), 0)
        await self.until(lambda: client.state == 'dead')
        with self.assertRaises(HostDied):
            await client.wait()
        await self.assert_cleaned(exited=False)

    async def test_D3_provider_exit(self):
        client = await self.start('exit')
        (self.root / 'exit-provider').touch()
        await client.wait()
        await self.assert_cleaned()

    async def test_D4_shared_coalition_refuses(self):
        directory = self.root / 'host'
        directory.mkdir()
        ownership.create_job(directory, [], self.root, '', self.root.name)
        ownership.write(directory / 'spec.json', dict(provider='opencode', ownership='coalition',
            argv=[sys.executable, str(self.script), str(self.root), 'stop'], env={}, cwd=str(self.root)))
        script = ('import asyncio,sys; from pathlib import Path; '
                  'from coordinator import host,process_ownership; '
                  'from tests.test_process_ownership import register_probe,GuardedDarwinBackend; '
                  'register_probe(sys.argv[2],"host"); '
                  'process_ownership.backend=GuardedDarwinBackend(sys.argv[2]); '
                  'asyncio.run(host._serve(Path(sys.argv[1]),{}))')
        env = {'PYTHONPATH': str(Path(host.__file__).resolve().parents[1])}
        child = subprocess.Popen([sys.executable, '-c', script, str(directory), str(self.root)], env=env)
        try:
            await asyncio.to_thread(child.wait, 10)
            self.assertEqual(child.returncode, 0)
            self.assertEqual(json.loads((directory / 'exit.json').read_text())['failure_code'], 'ownership_unavailable')
            self.assertFalse((directory / 'owner.json').exists())
            self.assertFalse((self.root / 'audit-signals.jsonl').exists())
            self.assertEqual({row['role'] for row in probe_records(self.root)}, {'host'})
        finally:
            if child.poll() is None:
                current = self.backend.metadata(child.pid)
                if current is not None:
                    self.backend.signal(current, signal.SIGKILL)
            await asyncio.to_thread(child.wait, 10)
