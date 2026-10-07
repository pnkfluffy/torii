import asyncio
from contextlib import ExitStack, redirect_stdout
import hashlib
import io
import json
import os
from pathlib import Path
import plistlib
import runpy
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / 'tests/fixtures/mac-runtime-contract.json'
UNCHANGED = ('scripts/pin-release.py', 'coordinator/transcribe.py',
             'coordinator/transcription-macos.lock', 'coordinator/transcription-linux.lock')


def mac_outputs():
    from coordinator import health, policy, providers, service
    with ExitStack() as stack:
        if 'coordinator.host_os' in sys.modules:
            stack.enter_context(patch('coordinator.host_os.SYSTEM', 'darwin'))
        stack.enter_context(patch('sys.platform', 'darwin'))
        stack.enter_context(patch('pwd.getpwuid', return_value=SimpleNamespace(pw_dir='/fixture/home', pw_name='owner')))
        values = runpy.run_path(str(ROOT / 'coordinator/policy.py'), run_name='coordinator._capture')['COORDINATOR_POLICY']
        deletion = runpy.run_path(str(ROOT / 'coordinator/policy.py'), run_name='coordinator._capture')['DELETION_POLICY']
        with patch.object(policy, 'DELETION_POLICY', deletion):
            worker = runpy.run_path(str(ROOT / 'coordinator/workers.py'), run_name='coordinator._capture')['WORKER_POLICY']
        output = {'coordinator_policy': values, 'worker_policy': worker}
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            def normalize(value):
                return value.replace(str(home.resolve()), '/fixture/home').replace(str(home), '/fixture/home').replace(str(ROOT), '/fixture/checkout')
            installer = runpy.run_path(str(ROOT / 'scripts/install-service.py'))
            printed = io.StringIO()
            old_umask = os.umask(0o077)
            try:
                with patch('pathlib.Path.home', return_value=home), patch('sys.executable', '/fixture/python'), \
                     patch('sys.argv', ['install-service.py']), patch('os.getuid', return_value=501), \
                     patch.dict(os.environ, {}, clear=True), \
                     patch.dict(installer['main'].__globals__, unload=lambda *args: None,
                                wait_for_start=lambda *args: None), \
                     patch('subprocess.run') as run, redirect_stdout(printed):
                    installer['main']()
                output['installer_stdout'] = normalize(printed.getvalue())
                output['installer_commands'] = run.call_args.args[0]
                output['installer_commands'] = [normalize(item) for item in output['installer_commands']]
                plist = home / 'Library/LaunchAgents/local.telegram-agent-coordinator.plist'
                output['plist'] = normalize(plist.read_text())
            finally:
                os.umask(old_umask)
            release = runpy.run_path(str(ROOT / 'scripts/pin-release.py'))
            printed = io.StringIO()
            with patch('pathlib.Path.home', return_value=home), patch('os.getuid', return_value=501), \
                 patch.dict(release['main'].__globals__, git=lambda *args: 'a' * 40), \
                 patch('subprocess.run', return_value=SimpleNamespace(returncode=0)), \
                 patch('os.access', return_value=True), redirect_stdout(printed):
                release['main'](['main', '--home', str(ROOT), '--state-dir', str(home / 'state'),
                                 '--plist', str(plist)])
            output['pin_release_stdout'] = normalize(printed.getvalue())
        def command(*args):
            return {('sysctl', '-n', 'hw.ncpu'): '8', ('sysctl', '-n', 'hw.memsize'): '17179869184',
                    ('vm_stat',): 'page size of 4096 bytes\nPages free: 1048576.\nPages inactive: 1048576.\n',
                    ('memory_pressure', '-Q'): 'System-wide memory free percentage: 36%',
                    ('sysctl', '-n', 'vm.swapusage'): 'used = 256.00M'}.get(args, '')
        with patch.object(health, '_command', side_effect=command), \
             patch.object(health.os, 'getloadavg', return_value=(2.5, 1, 1)):
            pressure = health.system_pressure('/state')
        output['health'] = health.format_health([], [], pressure, 'No accounts')
        with patch('subprocess.run', return_value=SimpleNamespace(stdout='{ sec = 123, usec = 456 }')) as run:
            output['boot_time'] = service.boot_time()
            output['boot_command'] = run.call_args.args[0]
        with tempfile.TemporaryDirectory() as temporary:
            runner = providers.ProviderRunner(temporary)
            process = SimpleNamespace(returncode=0, communicate=AsyncMock(return_value=(b'', b'')))
            with patch('asyncio.create_subprocess_exec', AsyncMock(return_value=process)) as launch:
                asyncio.run(runner._external_writer('claude', 'fake-session'))
            output['ps_command'] = launch.call_args.args
        verify = runpy.run_path(str(ROOT / 'scripts/verify-local.py'))
        with patch.dict(os.environ, {'TORII_PYTHON': '/other/python'}), patch('os.access', return_value=True):
            output['verify_interpreter'] = verify['interpreter']()
        output['unchanged_sha256'] = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in UNCHANGED}
        return json.loads(json.dumps(output))


class MacIdentityTests(unittest.TestCase):
    def test_darwin_outputs_match_fixed_runtime_contract(self):
        expected = json.loads(FIXTURE.read_text())['outputs']
        expected['unchanged_sha256'] = {name: expected['unchanged_sha256'][name] for name in UNCHANGED}
        self.assertEqual(mac_outputs(), expected)

    def test_darwin_trash_startup_does_not_touch_filesystem(self):
        with patch('coordinator.host_os.SYSTEM', 'darwin'), \
             patch('pathlib.Path.mkdir', side_effect=AssertionError('Mac startup must not create Trash')), \
             patch('pathlib.Path.chmod', side_effect=AssertionError('Mac startup must not chmod Trash')):
            from coordinator.host_os import prepare_trash
            prepare_trash()
