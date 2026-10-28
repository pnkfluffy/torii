import asyncio
import io
import os
import signal
import subprocess
import sys
import tempfile
from pathlib import Path
import unittest

from unittest.mock import patch

from coordinator.host import HostClient, _launchers
from tests.support import _group_alive, read_jsonl, stop_test_hosts


class ReadJsonlTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_reads_only_newline_terminated_json_lines(self):
        path = self.root / 'events.jsonl'
        path.write_text('{"message": "complete"}\n{"message": "unfinished')
        self.assertEqual(read_jsonl(path), [{'message': 'complete'}])

    def test_returns_empty_when_file_is_missing(self):
        self.assertEqual(read_jsonl(self.root / 'missing.jsonl'), [])

    def test_reads_every_line_when_file_ends_with_newline(self):
        path = self.root / 'events.jsonl'
        path.write_text('{"id": 1}\n\n{"id": 2}\n')
        self.assertEqual(read_jsonl(path), [{'id': 1}, {'id': 2}])


class TestHostCleanupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def run_case(self, outcome='pass', layout='state/hosts/worker', remove=False,
                 early=False, stubborn=False, rewrite=False, cancel=False, dead=False,
                 cleanup_error=False, late=False):
        root = self.root
        started = []

        class Case(unittest.IsolatedAsyncioTestCase):
            async def asyncSetUp(case):
                if early:
                    await case.start()
                    raise RuntimeError('injected setup error')

            async def start(case):
                script = ('import signal,sys,time; '
                          + ('signal.signal(signal.SIGTERM,signal.SIG_IGN); ' if stubborn else '')
                          + 'print("ready",flush=True); '
                          + ('time.sleep(60)' if stubborn else 'sys.stdin.read()'))
                spec = dict(argv=[sys.executable, '-u', '-c', script], cwd=str(root),
                            env={'PATH': os.environ['PATH']}, provider='codex', locks=[])
                client = await HostClient.launch(root / layout, spec)
                started.extend((int((client.directory / 'pid').read_text()), client.child_pid))
                await client.next_event()
                client.detach()
                return client

            async def test_host(case):
                if late:
                    async def launch_during_shutdown():
                        try:
                            await asyncio.Event().wait()
                        finally:
                            await case.start()
                    case.late_task = asyncio.create_task(launch_during_shutdown())
                    await asyncio.sleep(0)
                    return
                if cancel:
                    real_child_pid = HostClient.child_pid

                    def child_pid(client):
                        pid = real_child_pid.fget(client)
                        if pid and not started:
                            started.extend((int((client.directory / 'pid').read_text()), pid))
                            raise asyncio.CancelledError('injected startup cancellation')
                        return pid

                    with patch.object(HostClient, 'child_pid', property(child_pid)):
                        await case.start()
                else:
                    client = await case.start()
                    if cleanup_error:
                        def fail_cleanup():
                            raise RuntimeError('injected cleanup error')
                        case.addCleanup(fail_cleanup)
                    if remove:
                        for path in client.directory.iterdir():
                            if path.is_file() and not path.is_symlink():
                                path.unlink()
                        _launchers.clear()
                    if dead:
                        os.kill(started[0], signal.SIGKILL)
                    if outcome == 'fail':
                        case.fail('injected failure after launch')
                    if outcome == 'error':
                        raise ConnectionRefusedError('injected connection error')

        initialize = subprocess.Popen.__init__

        def rewritten(process, args, *fields, **options):
            if args[1:4] == ['-m', 'coordinator', 'host']:
                script = 'import sys; from coordinator.host import main; main(sys.argv[1])'
                args = [args[0], '-c', script, args[-1]]
            initialize(process, args, *fields, **options)

        with patch.object(subprocess.Popen, '__init__', rewritten if rewrite else initialize):
            result = unittest.TextTestRunner(stream=io.StringIO()).run(Case('test_host'))
        expected_errors = int(early or outcome == 'error' or cancel or cleanup_error)
        self.assertEqual(len(result.errors), expected_errors, result.errors)
        self.assertEqual(len(result.failures), int(outcome == 'fail'), result.failures)
        self.assertEqual(len(started), 2)
        self.assertFalse(any(_group_alive(pid) for pid in started), started)

    def test_unregistered_hosts_stop_after_pass_failure_and_error(self):
        for outcome in ('pass', 'fail', 'error'):
            with self.subTest(outcome=outcome):
                self.run_case(outcome=outcome, layout=outcome + '/state/hosts/worker')

    def test_host_outside_default_layout_stops(self):
        self.run_case(outcome='error', layout='custom/provider')

    def test_missing_pid_files_and_launcher_registry_do_not_hide_hosts(self):
        self.run_case(outcome='fail', remove=True)

    def test_setup_error_stops_host(self):
        self.run_case(early=True)

    def test_loop_shutdown_still_owns_hosts_started_by_cancelled_tasks(self):
        self.run_case(late=True)

    def test_cancelled_launch_stops_host_and_child(self):
        self.run_case(cancel=True)

    def test_provider_ignoring_term_is_killed(self):
        self.run_case(stubborn=True)

    def test_rewritten_host_command_is_still_owned(self):
        self.run_case(rewrite=True)

    def test_dead_host_does_not_hide_its_orphaned_provider(self):
        self.run_case(remove=True, dead=True)

    def test_another_cleanup_error_does_not_skip_host_cleanup(self):
        self.run_case(cleanup_error=True)

    def test_transient_signal_denial_does_not_interrupt_other_groups(self):
        killpg = os.killpg
        rejected = []

        def exiting(pid, number):
            if not rejected and number == signal.SIGTERM:
                rejected.append(pid)
                raise PermissionError('injected macOS exit-transition denial')
            return killpg(pid, number)

        with patch('tests.support.os.killpg', exiting):
            self.run_case(stubborn=True)
        self.assertEqual(len(rejected), 1)

    def test_cleanup_uses_launch_ownership_instead_of_pid_files(self):
        with subprocess.Popen([sys.executable, '-c', 'import sys; sys.stdin.read()'],
                              stdin=subprocess.PIPE, start_new_session=True) as unrelated:
            try:
                directory = self.root / 'state/hosts/stale'
                directory.mkdir(parents=True)
                (directory / 'pid').write_text(str(unrelated.pid))
                (directory / 'child.pid').write_text(str(unrelated.pid))
                stop_test_hosts(self.root)
                self.assertIsNone(unrelated.poll())
            finally:
                unrelated.terminate()
                unrelated.wait(timeout=20)

    def test_leak_check_fails_for_a_host_and_passes_after_cleanup(self):
        async def exercise():
            spec = dict(argv=[sys.executable, '-c', 'import sys; sys.stdin.read()'],
                        cwd=str(self.root), env={'PATH': os.environ['PATH']},
                        provider='codex', locks=[])
            directory = self.root / 'unusual/host with spaces'
            client = await HostClient.launch(directory, spec)
            client.detach()
            checker = Path(__file__).resolve().parents[1] / 'scripts/check-test-hosts.py'
            command = [sys.executable, str(checker), '--root', str(self.root)]
            leaked = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(leaked.returncode, 1, leaked.stdout + leaked.stderr)
            self.assertIn(str(directory.resolve()), leaked.stderr)
            other = subprocess.run(command + ['--root', str(self.root / 'another')],
                                   capture_output=True, text=True)
            self.assertEqual(other.returncode, 0, other.stderr)
            alias = self.root / 'alias'
            alias.symlink_to(self.root, target_is_directory=True)
            stop_test_hosts(alias)
            clean = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(clean.returncode, 0, clean.stderr)

        asyncio.run(exercise())

    def test_leak_check_tolerates_non_utf8_process_arguments(self):
        checker = Path(__file__).resolve().parents[1] / 'scripts/check-test-hosts.py'
        with subprocess.Popen([sys.executable, '-c', 'import sys; sys.stdin.read()', b'bad-argument-\xdc'],
                              stdin=subprocess.PIPE) as unrelated:
            try:
                checked = subprocess.run([sys.executable, str(checker), '--root', str(self.root)],
                                         capture_output=True, text=True)
                self.assertEqual(checked.returncode, 0, checked.stderr)
            finally:
                unrelated.terminate()
                unrelated.wait(timeout=20)
