import asyncio
import logging
import logging.handlers
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from coordinator import control_api, log
from coordinator.service import Service
from coordinator.store import Store


class FakeTelegram:
    async def send(self, row):
        raise AssertionError('The guard tests never deliver.')


class FakeRunner:
    async def run(self, *args, **kwargs):
        raise AssertionError('The guard tests never start a provider.')


class LogSetupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.path = log.configure(self.root / 'state', stderr=False)

    def tearDown(self):
        log.reset()
        self.temp.cleanup()

    def test_configure_writes_a_timestamped_line_into_the_state_dir(self):
        self.assertEqual(self.path, self.root / 'state' / 'logs' / 'service.log')
        log.logger('coordinator.sample').info('worker finish worker=%s topic=%s', 7, '-10042:4')
        line = self.path.read_text().splitlines()[-1]
        self.assertRegex(line, r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3} INFO '
                               r'coordinator\.sample worker finish worker=7 topic=-10042:4$')
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o600)

    def test_file_handler_rotates_at_five_megabytes_with_five_backups(self):
        rotating = [handler for handler in logging.getLogger('coordinator').handlers
                    if isinstance(handler, logging.handlers.RotatingFileHandler)]
        self.assertEqual([(h.maxBytes, h.backupCount) for h in rotating], [(5 * 1024 * 1024, 5)])

    def test_repeating_the_same_state_dir_does_not_double_every_line(self):
        self.assertEqual(log.configure(self.root / 'state', stderr=False), self.path)
        log.logger('coordinator.sample').info('once')
        self.assertEqual(self.path.read_text().count('once'), 1)

    def test_a_stderr_handler_joins_the_file_when_asked(self):
        log.configure(self.root / 'other', stderr=True)
        kinds = [type(handler) for handler in logging.getLogger('coordinator').handlers
                 if not isinstance(handler, logging.NullHandler)]
        self.assertEqual(kinds, [logging.handlers.RotatingFileHandler, logging.StreamHandler])

    def test_running_commit_reports_this_checkout_and_survives_a_plain_folder(self):
        root = Path(__file__).resolve().parents[1]
        expected = subprocess.run(['git', 'rev-parse', '--short', 'HEAD'], cwd=str(root),
                                  stdout=subprocess.PIPE, text=True).stdout.strip()
        self.assertEqual(log.running_commit(root), expected)
        self.assertEqual(log.running_commit(self.root / 'state'), 'unknown')

    def test_running_commit_reads_a_valid_archive_version_when_git_is_unavailable(self):
        (self.root / 'VERSION').write_text('084b547\n')
        for failure in (OSError('git absent'), subprocess.TimeoutExpired('git', 10)):
            with self.subTest(failure=failure), patch('coordinator.log.subprocess.run', side_effect=failure):
                self.assertEqual(log.running_commit(self.root), '084b547')
        with patch('coordinator.log.subprocess.run', return_value=subprocess.CompletedProcess([], 128, b'')):
            self.assertEqual(log.running_commit(self.root), '084b547')
            (self.root / 'VERSION').write_text('$Format:%h$\n')
            self.assertEqual(log.running_commit(self.root), 'unknown')

    def test_f_code_stamp_tracks_python_metadata_and_survives_a_missing_file(self):
        package = self.root / 'coordinator'
        (package / 'sub').mkdir(parents=True)
        first = package / 'a.py'
        second = package / 'sub' / 'b.py'
        first.write_text('a = 1\n')
        second.write_text('b = 2\n')
        original = log.code_stamp(self.root)
        self.assertRegex(original, r'^[0-9a-f]{64}$')
        self.assertEqual(log.code_stamp(self.root), original)
        metadata = second.stat()
        second.write_text('b = 222\n')
        os.utime(second, ns=(metadata.st_atime_ns, metadata.st_mtime_ns))
        grown = log.code_stamp(self.root)
        self.assertNotEqual(grown, original)
        second.rename(second.with_name('renamed.py'))
        renamed = log.code_stamp(self.root)
        self.assertNotEqual(renamed, grown)
        metadata = first.stat()
        os.utime(first, ns=(metadata.st_atime_ns, metadata.st_mtime_ns + 1000000000))
        touched = log.code_stamp(self.root)
        self.assertNotEqual(touched, renamed)
        other = package / 'notes.txt'
        other.write_text('first')
        self.assertEqual(log.code_stamp(self.root), touched)
        other.write_text('second and longer')
        self.assertEqual(log.code_stamp(self.root), touched)
        (package / 'c.py').symlink_to(package / 'missing.py')
        self.assertIsNone(log.code_stamp(self.root))

    def test_code_stamp_returns_none_when_listing_fails(self):
        with patch('coordinator.log.os.scandir', side_effect=OSError('listing failed')):
            self.assertIsNone(log.code_stamp(self.root))

    def test_i_module_entrypoint_records_startup_stamp_with_fake_dependencies(self):
        root = Path(__file__).resolve().parents[1]
        driver = self.root / 'driver.py'
        binary = self.root / 'bin' / 'codex'
        binary.parent.mkdir()
        binary.write_bytes(b'\xcf\xfa\xed\xfefake Codex')
        binary.chmod(0o700)
        driver.write_text('''from contextlib import ExitStack
import runpy
from unittest.mock import AsyncMock, Mock, patch
from coordinator.service import Service
later = False

async def telegram_call(method):
    global later
    if method == 'getMe':
        later = True
        return {'id': 99, 'username': 'test_bot'}
    return {}

async def run(service):
    service.record_start()

with ExitStack() as stack:
    for name in ('coordinator.accounts.discover_accounts', 'coordinator.codex_accounts.discover_codex_accounts',
                 'coordinator.host_os.prepare_trash', 'coordinator.providers.ProviderRunner'):
        stack.enter_context(patch(name))
    stack.enter_context(patch('coordinator.telegram.Telegram', return_value=Mock(
        call=AsyncMock(side_effect=telegram_call))))
    stack.enter_context(patch('coordinator.log.code_stamp', side_effect=lambda root: 'after' if later else 'before'))
    stack.enter_context(patch('coordinator.service.code_stamp',
                             side_effect=lambda root: 'after' if later else 'before', create=True))
    stack.enter_context(patch.object(Service, 'run', run))
    stack.enter_context(patch('coordinator.vault.vault_from_environment', return_value=None))
    runpy.run_module('coordinator', run_name='__main__')
''')
        completed = subprocess.run([sys.executable, str(driver), '--state-dir',
                                    str(self.root / 'child-state'), '--token-file', str(self.root / 'unused'), 'serve'],
                                   cwd=root, env=dict(os.environ,
                                   PATH=str(binary.parent), PYTHONPATH=str(root), TORII_HOME=str(self.root)),
                                   capture_output=True, text=True, timeout=20)
        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
        written = (self.root / 'child-state' / 'logs' / 'service.log').read_text()
        self.assertIn('coordinator.__main__ service start commit=', written)
        self.assertIn('coordinator.__main__ bridge running pair_only=False', written)
        self.assertIn('code=helper-missing', written)
        self.assertIn('Update or reinstall Codex 0.143.0 or newer the same way you installed it (for npm, without --omit=optional)', completed.stderr)
        store = Store(self.root / 'child-state')
        try:
            self.assertEqual(store.get('service_run')['code'], 'before')
            problem = store.db.execute("SELECT detail FROM problems WHERE code='helper-missing'").fetchone()
            self.assertIn('restart Torii', problem['detail'])
        finally:
            store.close()

class LogContentTests(unittest.TestCase):
    """A log line names what happened. It never carries the value that happened to it."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.path = log.configure(self.root / 'state', stderr=False)
        self.store = Store(self.root / 'state')

    def tearDown(self):
        log.reset()
        self.store.close()
        self.temp.cleanup()

    def test_control_lines_name_the_op_and_state_without_the_value_set(self):
        secret = 'sk-ant-notarealtokenbutlooksliketone'
        with self.store.db:
            saved = control_api.call(self.store, 'policy.set', {'text': 'Always use ' + secret},
                                     source='cli')
            refused = control_api.call(self.store, 'telegram.token', {}, source='coordinator')
        self.assertTrue(saved.ok)
        self.assertEqual(refused.state, 'refused')
        written = self.path.read_text()
        self.assertIn('control op=policy.set state=done ok=True source=cli', written)
        self.assertIn('setting changed key=USAGE.md length=', written)
        self.assertIn('control op=telegram.token state=refused ok=False source=coordinator', written)
        self.assertNotIn(secret, written)
        self.assertNotIn('Always use', written)




class RunGuardTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.path = log.configure(self.root / 'state', stderr=False)
        self.store = Store(self.root / 'state')
        self.service = Service(self.store, FakeTelegram(), FakeRunner(), self.root, pair_only=True)
        self.service.task_restart_limit = 2
        self.service.task_restart_backoff = 0

    async def asyncTearDown(self):
        log.reset()
        self.store.close()
        self.temp.cleanup()

    async def test_run_restarts_a_crashing_task_then_exits_for_relaunch(self):
        attempts = []

        async def flapping():
            attempts.append(1)
            raise ZeroDivisionError('loop bug')

        async def idle():
            await asyncio.sleep(3600)

        idle_task = asyncio.Event()

        async def watched_idle():
            idle_task.set()
            await idle()

        self.service.supervised = lambda: [('flapper', flapping), ('idle', watched_idle)]
        with self.assertRaises(RuntimeError) as caught:
            await self.service.run()
        self.assertEqual(len(attempts), 3)
        self.assertIn('flapper', str(caught.exception))
        self.assertTrue(idle_task.is_set())
        written = self.path.read_text()
        self.assertEqual(written.count('task crashed task=flapper'), 3)
        self.assertIn('ZeroDivisionError: loop bug', written)
        self.assertIn('Traceback (most recent call last)', written)
        self.assertIn('service run commit=', written)

    async def test_a_task_that_recovers_keeps_the_service_running(self):
        attempts = []

        async def flaky():
            attempts.append(1)
            if len(attempts) < 3:
                raise ValueError('transient')
            return 'settled'

        self.assertEqual(await self.service.supervise('flaky', flaky), 'settled')
        self.assertEqual(len(attempts), 3)
        self.assertEqual(self.path.read_text().count('task crashed task=flaky'), 2)

    async def test_the_guard_never_swallows_cancellation(self):
        async def cancelled():
            raise asyncio.CancelledError()

        with self.assertRaises(asyncio.CancelledError):
            await self.service.supervise('cancelled', cancelled)
        self.assertNotIn('task crashed', self.path.read_text())


if __name__ == '__main__':
    unittest.main()
