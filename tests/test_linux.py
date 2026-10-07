import asyncio
import io
import os
from pathlib import Path
import runpy
import tempfile
from contextlib import redirect_stdout
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import health, host_os, policy, service
from coordinator.providers import ProviderRunner

ROOT = Path(__file__).resolve().parents[1]


class LinuxTestsSupport:
    def setUp(self):
        switch = patch.object(host_os, 'SYSTEM', 'linux')
        switch.start()
        self.addCleanup(switch.stop)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)


class LinuxTests(LinuxTestsSupport, unittest.TestCase):
    def test_linux_ps_checks_session_ownership(self):
        runner = ProviderRunner(self.home)
        for output, busy in ((b'123 /usr/bin/claude --resume exact-session\n', True),
                             (b'123 /usr/bin/claude --resume exact-session-other\n', False)):
            process = SimpleNamespace(returncode=0, communicate=AsyncMock(return_value=(output, b'')))
            with patch('asyncio.create_subprocess_exec', AsyncMock(return_value=process)) as launch:
                self.assertEqual(asyncio.run(runner._external_writer('claude', 'exact-session')), busy)
            self.assertEqual(launch.call_args.args, ('ps', '-A', '-o', 'pid=,command='))

    def test_linux_boot_time_and_missing_proc(self):
        with patch.object(host_os, 'proc_text', return_value='cpu 1 2 3\nbtime 123\n'):
            self.assertEqual(service.boot_time(), 123)
        with patch.object(host_os, 'proc_text', return_value=''):
            self.assertIsNone(service.boot_time())

    def test_linux_pressure_and_optional_psi(self):
        files = {'meminfo': 'MemTotal: 16777216 kB\nMemAvailable: 8388608 kB\nSwapTotal: 1048576 kB\nSwapFree: 786432 kB\n',
                 'pressure/memory': 'some avg10=0.25 avg60=0.10 avg300=0.00 total=123\n'}
        with patch.object(host_os, 'proc_text', side_effect=lambda name: files.get(name, '')), \
             patch('os.cpu_count', return_value=8), patch('os.getloadavg', return_value=(2.5, 1, 1)), \
             patch.object(health, '_disk', return_value='50.0G free / 100.0G'):
            self.assertEqual(health.system_pressure(self.home), [
                'CPU: 2.5 load / 8 cores', 'RAM: 8.0G used / 16.0G, 0.25% pressure stall, swap 0.2G',
                'Disk /: 50.0G free / 100.0G', 'Disk state: 50.0G free / 100.0G'])
            files.pop('pressure/memory')
            self.assertNotIn('stall', health.system_pressure(self.home)[1])

    def test_linux_trash_is_private_and_policies_use_it(self):
        with patch('pwd.getpwuid', return_value=SimpleNamespace(pw_dir=str(self.home))):
            directory = host_os.trash_directory()
            host_os.prepare_trash()
            directory.chmod(0o755)
            host_os.prepare_trash()
            self.assertEqual(directory.stat().st_mode & 0o777, 0o700)
            self.assertEqual(directory, self.home / '.local/share/Trash/files')
            captured = runpy.run_path(str(ROOT / 'coordinator/policy.py'), run_name='coordinator._capture')
            self.assertIn('Linux Trash at ' + str(directory), captured['COORDINATOR_POLICY'])
            with patch.object(policy, 'DELETION_POLICY', captured['DELETION_POLICY']):
                worker = runpy.run_path(str(ROOT / 'coordinator/workers.py'), run_name='coordinator._capture')
            self.assertIn(captured['DELETION_POLICY'], worker['WORKER_POLICY'])

    def test_linux_installer_unit_and_linger_commands(self):
        installer = runpy.run_path(str(ROOT / 'scripts/install-service.py'))
        old_umask = os.umask(0o077)
        try:
            with patch('pathlib.Path.home', return_value=self.home), patch('os.getuid', return_value=1000), \
                 patch('sys.executable', '/fixture/python'), patch('sys.argv', ['install-service.py']), \
                 patch('subprocess.run') as run, redirect_stdout(io.StringIO()):
                installer['main']()
            unit = self.home / '.config/systemd/user/local.telegram-agent-coordinator.service'
            expected = (ROOT / 'tests/fixtures/linux-unit.txt').read_text()
            self.assertEqual(unit.read_text().replace(str(ROOT), '/fixture/checkout').replace(str(self.home), '/fixture/home'), expected)
            self.assertEqual(unit.stat().st_mode & 0o777, 0o600)
            self.assertEqual([call.args[0] for call in run.call_args_list], [
                ['loginctl', 'enable-linger'], ['systemctl', '--user', 'daemon-reload'],
                ['systemctl', '--user', 'is-active', '--quiet', 'local.telegram-agent-coordinator.service'],
                ['systemctl', '--user', 'enable', '--now', 'local.telegram-agent-coordinator.service']])
        finally:
            os.umask(old_umask)

    def test_active_linux_service_is_restarted_with_agent_argv(self):
        installer = runpy.run_path(str(ROOT / 'scripts/install-service.py'))
        with patch('pathlib.Path.home', return_value=self.home), patch('os.getuid', return_value=1000), \
             patch('subprocess.run', return_value=SimpleNamespace(returncode=0)) as run, redirect_stdout(io.StringIO()):
            installer['install'](True)
        run.assert_any_call(['systemctl', '--user', 'restart', 'local.telegram-agent-coordinator.service'], check=True)
        self.assertNotIn('--pair-only', (self.home / '.config/systemd/user/local.telegram-agent-coordinator.service').read_text())

    def test_root_is_refused_before_writes(self):
        installer = runpy.run_path(str(ROOT / 'scripts/install-service.py'))
        with patch('os.getuid', return_value=0), patch('sys.argv', ['install-service.py', '--write-only']), \
             patch('pathlib.Path.mkdir', side_effect=AssertionError('Root must not write')):
            with self.assertRaisesRegex(SystemExit, 'Refusing.*root'):
                installer['main']()

    def test_unit_escaping_and_write_only(self):
        installer = runpy.run_path(str(ROOT / 'scripts/install-service.py'))
        self.assertEqual(installer['systemd_quote']('a"b%c\\d'), '"a\\"b%%c\\\\d"')
        with self.assertRaises(ValueError):
            installer['systemd_quote']('bad\npath')
        old_umask = os.umask(0o077)
        try:
            with patch('pathlib.Path.home', return_value=self.home), patch('os.getuid', return_value=1000), \
                 patch('sys.argv', ['install-service.py', '--write-only', '--enable-agents']), \
                 patch('subprocess.run', side_effect=AssertionError('Write only')), redirect_stdout(io.StringIO()):
                installer['main']()
            self.assertNotIn('--pair-only', (self.home / '.config/systemd/user/local.telegram-agent-coordinator.service').read_text())
        finally:
            os.umask(old_umask)

    def test_linux_shutdown_cancels_streams_before_waiting_for_closed_connections(self):
        async def check():
            ended = asyncio.Event()
            async def stream():
                try:
                    await asyncio.Future()
                finally:
                    ended.set()
            task = asyncio.create_task(stream())
            await asyncio.sleep(0)
            server = SimpleNamespace(close=lambda: None, wait_closed=ended.wait)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            self.assertTrue(task.cancelled())
        asyncio.run(check())
