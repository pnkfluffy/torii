"""Offline Muse device sign-in through fake MSP and fake Telegram only."""

import asyncio
import io
import json
import logging
import os
from pathlib import Path
import shlex
import signal
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch
import uuid

from coordinator import control_api, host_os, mcp, muse_accounts, signin
from coordinator.service import Service
from coordinator.store import Store
from coordinator.telegram import TelegramError
from tests.support import SETTLE, read_jsonl, until
from tests.test_service import FakeRunner, FakeTelegram


URL = 'https://auth.meta.com/oauth/device/test'
CODE = 'ABCD-EFGH'
TOPIC = '-10042:4'
DARWIN = sys.platform == 'darwin'


class MuseSignInTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='muse-m3-')
        self.root = Path(self.temp.name).resolve()
        self.addCleanup(self.temp.cleanup)
        self.home = self.root / 'owner'
        self.home.mkdir()
        self.trash = self.root / 'Trash'
        self.scenario = self.root / 'scenario.json'
        self.scenario.write_text(json.dumps({'signin': True, 'hold': True, 'label': 'Muse Owner'}))
        self.fake = self.root / 'fake-muse'
        self.fake.write_text('#!/bin/sh\nexec ' + shlex.quote(sys.executable) + ' ' +
                             shlex.quote(str(Path(__file__).with_name('fake_muse.py').resolve())) +
                             ' --home "$HOME" --calls "$HOME/calls.jsonl" --scenario ' +
                             shlex.quote(str(self.scenario)) + ' "$@"\n')
        self.fake.chmod(0o700)
        for fixture in (patch.dict(os.environ, {'HOME': str(self.home)}, clear=True),
                        patch.object(muse_accounts, 'check_binary'),
                        patch.object(muse_accounts.sys, 'platform', 'darwin'),
                        patch.object(host_os, 'trash_directory', return_value=self.trash)):
            fixture.start()
            self.addCleanup(fixture.stop)
        self.real_boundary = muse_accounts.write_boundary
        boundary_patch = patch.object(muse_accounts, 'write_boundary', side_effect=self.wrap)
        self.boundary = boundary_patch.start()
        self.addCleanup(boundary_patch.stop)
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,-10042,4,'Test','',1)",
                                  (TOPIC,))
            self.store.put('owner', 7)
            self.store.put('group', -10042)
            self.store.put('bot_username', 'fake_bot')
            self.store.put('muse_enabled', True)
            self.store.put('muse_binary', str(self.fake))
        self.log = io.StringIO()
        handler = logging.StreamHandler(self.log)
        logger = logging.getLogger('coordinator')
        previous = logger.level
        logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        self.addCleanup(logger.setLevel, previous)
        self.addCleanup(logger.removeHandler, handler)
        self.task = None
        self.directory = None

    async def asyncTearDown(self):
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)

    def wrap(self, argv, roots):
        self.assertEqual(list(roots), [self.directory])
        return list(argv)

    def configure(self, **values):
        self.scenario.write_text(json.dumps(dict({'signin': True, 'label': 'Muse Owner'}, **values)))

    def call(self, operation='account.add', **params):
        with self.store.db:
            return control_api.call(self.store, operation, params or ({'provider': 'muse'} if operation == 'account.add' else {}),
                                    source='telegram', topic=TOPIC)

    async def start(self, **options):
        result = self.call()
        self.assertTrue(result.ok, result.text)
        self.directory = Path(self.store.get(signin.KEY)['config_dir'])
        self.driver = signin.SignIns(self.store, muse_binary=lambda: str(self.fake), poll=.01,
                                     link_seconds=SETTLE, exit_seconds=SETTLE, **options)
        self.telegram = FakeTelegram()
        self.service = SimpleNamespace(store=self.store, signins=self.driver, telegram=self.telegram,
                                       delivery_lock=asyncio.Lock(), restart_reason=None)
        self.task = asyncio.create_task(self.driver.run())

    async def settled(self):
        await until(lambda: self.store.get(signin.KEY) is None, 'Muse sign-in to settle')
        if self.task.done():
            self.task.result()
        else:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)

    async def card(self):
        await until(lambda: (self.store.get(signin.KEY) or {}).get('state') == 'approve', 'Muse sign-in card')
        while await Service.deliver_once(self.service):
            pass
        return next(row for row in self.telegram.sent if CODE in row['text'])

    def calls(self):
        path = self.directory / 'calls.jsonl'
        if not path.exists() and self.trash.exists():
            path = next(self.trash.glob('*/calls.jsonl'), path)
        return read_jsonl(path)

    def methods(self):
        return [event.get('method') for event in self.calls()]

    def assert_no_code(self, code=CODE):
        self.assertNotIn(code, self.log.getvalue())
        for row in self.store.db.iterdump():
            self.assertNotIn(code, row)
        for file in (self.root / 'state').iterdir():
            if file.is_file():
                self.assertNotIn(code.encode(), file.read_bytes(), str(file))

    def assert_no_account(self):
        self.assertFalse(self.store.get('muse_accounts'))
        self.assertFalse(self.store.get('muse_active_account'))

    def assert_group_gone(self, pid):
        with self.assertRaises(ProcessLookupError):
            os.killpg(pid, 0)

    async def start_with_group_child(self, ignore_term=False, **options):
        child = ('import os,signal,time\nfrom pathlib import Path\n' +
                 ('signal.signal(signal.SIGTERM, signal.SIG_IGN)\n' if ignore_term else
                  'def stop(signum, frame):\n'
                  '    Path(os.environ["HOME"], "child.term").write_text("TERM")\n'
                  '    raise SystemExit(0)\nsignal.signal(signal.SIGTERM, stop)\n') +
                 'Path(os.environ["HOME"], "child.pid").write_text(str(os.getpid()))\ntime.sleep(300)')
        command = shlex.quote(sys.executable) + ' -c ' + shlex.quote(child)
        self.fake.write_text(self.fake.read_text().replace(
            'exec ', command + ' </dev/null >/dev/null 2>&1 &\nexec ', 1))
        self.configure(hold=True)
        await self.start(**options)
        await self.card()
        pid = self.store.get(signin.KEY)['pid']
        def cleanup():
            try:
                os.killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        self.addCleanup(cleanup)
        await until(lambda: (self.directory / 'child.pid').is_file(), 'the fake Muse child to start')
        return pid, int((self.directory / 'child.pid').read_text())

    def launch_group_child(self, source, names=('child',)):
        script = self.root / 'child.py'
        script.write_text(source)
        command = ' '.join(shlex.quote(part) for part in [sys.executable, str(script), str(self.root)])
        self.fake.write_text(self.fake.read_text().replace(
            'exec ', command + ' </dev/null >/dev/null 2>&1 &\nexec ', 1))
        def cleanup():
            for name in names:
                path = self.root / (name + '.pid')
                if path.is_file():
                    try:
                        os.kill(int(path.read_text()), signal.SIGKILL)
                    except ProcessLookupError:
                        pass
        self.addCleanup(cleanup)

    async def child_pid(self, name='child'):
        path = self.root / (name + '.pid')
        await until(lambda: path.is_file() and path.read_text().strip(), 'the fake Muse child ' + name)
        return int(path.read_text())

    def process_state(self, pid):
        return subprocess.run(['ps', '-o', 'stat=', '-p', str(pid)],
                              capture_output=True, text=True).stdout.strip()

    async def start_service_with_group_child(self, initializing=False, restart=None):
        from coordinator.service import _Restart
        if initializing:
            silent = self.root / 'silent.py'
            silent.write_text('import os,sys\nfrom pathlib import Path\n'
                              'Path(sys.argv[1], "leader.pid").write_text(str(os.getpid()))\n'
                              'for line in sys.stdin:\n    pass\n')
            self.fake.write_text('#!/bin/sh\nexec ' + ' '.join(shlex.quote(part) for part in [
                sys.executable, str(silent), str(self.root)]) + '\n')
        self.launch_group_child('import os,signal,sys,time\nfrom pathlib import Path\n'
                                'signal.signal(signal.SIGTERM, signal.SIG_IGN)\n'
                                'Path(sys.argv[1], "child.pid").write_text(str(os.getpid()))\ntime.sleep(300)\n')
        self.configure(hold=True)
        result = self.call()
        self.assertTrue(result.ok, result.text)
        self.directory = Path(self.store.get(signin.KEY)['config_dir'])
        service = Service(self.store, FakeTelegram(), FakeRunner(), self.root, pair_only=True)
        driver = signin.SignIns(self.store, muse_binary=lambda: str(self.fake), poll=.01,
                                link_seconds=60 if initializing else SETTLE, exit_seconds=SETTLE)
        service.signins = driver
        async def idle():
            await asyncio.Event().wait()
        async def restart_watch():
            await restart.wait()
            raise _Restart()
        loops = [('signins', driver.run), ('idle', idle)]
        if restart is not None:
            loops.append(('restart_watch', restart_watch))
        for fixture in (patch.object(service, 'supervised', return_value=loops),
                        patch.object(service.extension, 'start', AsyncMock()),
                        patch.object(service.extension, 'close', AsyncMock()),
                        patch('coordinator.reactions.check_administrators', AsyncMock()),
                        patch.object(service.workers, 'cleanup_finished', AsyncMock())):
            fixture.start()
            self.addCleanup(fixture.stop)
        self.task = asyncio.create_task(service.run())
        await until(lambda: (self.store.get(signin.KEY) or {}).get('state') ==
                    ('connecting' if initializing else 'approve'), 'the fake Muse launch')
        child = await self.child_pid()
        leader = await self.child_pid('leader') if initializing else self.store.get(signin.KEY)['pid']
        return leader, child

    def assert_cancelled_signin(self):
        self.assertIsNone(self.store.get(signin.KEY))
        row = self.store.db.execute('SELECT text FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
        self.assertEqual(row['text'], signin.MUSE_CANCELLED)
        self.assertFalse(self.directory.exists())
        self.assert_no_account()

    async def test_signal_stop_before_initialize_kills_term_ignoring_group_child(self):
        leader, child = await self.start_service_with_group_child(initializing=True)
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)
        await until(lambda: not self.process_state(child), 'the TERM-ignoring child to exit after shutdown')
        self.assert_group_gone(leader)
        self.assert_cancelled_signin()

    async def test_restart_during_cancel_grace_kills_term_ignoring_group_child(self):
        restart = asyncio.Event()
        leader, child = await self.start_service_with_group_child(restart=restart)
        self.call('account.signin_cancel')
        await until(lambda: not self.process_state(leader), 'the fake Muse leader to exit on TERM')
        restart.set()
        await asyncio.wait_for(self.task, 10)
        await until(lambda: not self.process_state(child), 'the TERM-ignoring child to exit after restart')
        self.assert_group_gone(leader)
        self.assert_cancelled_signin()

    async def test_cancel_finishes_with_unreaped_zombie_in_group(self):
        self.launch_group_child('import os,signal,sys,time\nfrom pathlib import Path\n'
                                'root = Path(sys.argv[1])\nchild = os.fork()\nif child == 0:\n'
                                '    signal.signal(signal.SIGTERM, signal.SIG_IGN)\n'
                                '    (root / "child.pid").write_text(str(os.getpid()))\n'
                                '    time.sleep(300)\n    os._exit(0)\n'
                                'os.setsid()\n(root / "parent.pid").write_text(str(os.getpid()))\n'
                                'time.sleep(300)\n', names=('parent', 'child'))
        self.configure(hold=True)
        await self.start()
        await self.card()
        leader = self.store.get(signin.KEY)['pid']
        child = await self.child_pid()
        parent = await self.child_pid('parent')
        self.assertEqual(os.getpgid(child), leader)
        self.assertNotEqual(os.getpgid(parent), leader)
        self.call('account.signin_cancel')
        await until(lambda: self.process_state(child).startswith('Z'), 'the fake child to become a zombie')
        try:
            await asyncio.wait_for(self.settled(), 7)
            self.assertTrue(self.process_state(child).startswith('Z'))
            self.assertTrue(self.process_state(parent))
            self.assert_cancelled_signin()
        finally:
            os.kill(parent, signal.SIGKILL)

    async def assert_rejected_device_url(self, url):
        self.configure(url=url, hold=True)
        await self.start()
        await until(lambda: self.store.get(signin.KEY) is None or
                    self.store.get(signin.KEY).get('state') == 'approve', 'Muse to reject or post the URL')
        while await Service.deliver_once(self.service):
            pass
        if self.store.get(signin.KEY):
            self.call('account.signin_cancel')
        await self.settled()
        self.assertFalse(any('one-time code' in row['text'] for row in self.telegram.sent),
                         'an unsafe device URL reached the Telegram card')
        self.assert_no_account()
        self.assertIn('account/loginCancel', self.methods())
        self.assert_no_code()
        problem = self.store.db.execute('SELECT code FROM problems ORDER BY id DESC LIMIT 1').fetchone()
        self.assertEqual(problem['code'], 'signin_failed')

    async def test_card_has_url_code_cancel_and_no_account_before_grant(self):
        await self.start()
        row = await self.card()
        self.assertIn('Muse Code', row['text'])
        self.assertIn(URL, row['text'])
        self.assertEqual(json.loads(row['reply_markup'])['inline_keyboard'], [[
            {'text': 'Cancel sign-in', 'callback_data': signin.cancel_data(self.store.get(signin.KEY))}]])
        self.assert_no_account()
        self.assert_no_code()
        self.assertEqual(self.store.get(signin.KEY)['provider'], 'muse')
        self.assertEqual(self.directory.parent, self.home / '.muse-accounts')
        self.assertRegex(self.directory.name, r'^\.torii-[0-9a-f]{8}$')

    async def test_only_granted_then_account_login_adds_one_account_and_reads_usage(self):
        gate = self.root / 'grant'
        self.configure(release=str(gate))
        await self.start()
        await self.card()
        self.assert_no_account()
        pid = self.store.get(signin.KEY)['pid']
        gate.touch()
        await self.settled()
        accounts = self.store.get('muse_accounts')
        self.assertEqual(list(accounts), ['muse-Muse-Owner'])
        profile = accounts['muse-Muse-Owner']
        self.assertEqual(profile['config_dir'], str(self.directory))
        self.assertEqual(profile['label'], 'Muse Owner')
        self.assertIsInstance(profile['created'], float)
        self.assertEqual(self.store.get('muse_active_account'), 'muse-Muse-Owner')
        broker = muse_accounts.MuseBroker(self.store)
        self.assertTrue(broker.authenticated(broker.active()))
        self.assertEqual(broker.label(broker.active()), 'Muse Owner')
        self.assertIsNone(self.store.get('muse_account_status')[broker.active()]['observed'])
        self.assertEqual(self.methods(), ['initialize', 'initialized', 'account/loginStart', 'account/read', 'usage/read'])
        self.assert_no_code()
        self.assert_group_gone(pid)

    async def test_denied_expired_cancelled_failed_and_unknown_add_nothing(self):
        for outcome in ('denied', 'expired', 'cancelled', 'failed', 'new-outcome'):
            with self.subTest(outcome=outcome):
                self.configure(outcome=outcome)
                await self.start()
                await self.settled()
                self.assert_no_account()
                self.assertNotIn('account/read', self.methods())
                self.task.cancel()
                await asyncio.gather(self.task, return_exceptions=True)

    async def test_api_key_logged_out_and_unknown_billing_lanes_add_nothing(self):
        for lane in ('apiKey', 'loggedOut', 'new-lane'):
            with self.subTest(lane=lane):
                self.configure(lane=lane)
                await self.start()
                await self.settled()
                self.assert_no_account()
                self.assertNotIn('usage/read', self.methods())
                self.task.cancel()
                await asyncio.gather(self.task, return_exceptions=True)

    async def test_second_account_is_refused_even_if_signed_out_without_explicit_alias(self):
        self.store.put('muse_accounts', {'muse-owner': {'config_dir': str(self.home / 'existing')}})
        result = self.call()
        self.assertFalse(result.ok)
        self.assertIn('one Muse account', result.text)
        self.assertIsNone(self.store.get(signin.KEY))
        self.boundary.assert_not_called()

    async def test_cancel_tap_sends_login_cancel_and_stops_group(self):
        await self.start()
        await self.card()
        record = self.store.get(signin.KEY)
        pid = record['pid']
        result = self.store.accept({'update_id': 1, 'callback_query': {'id': '1', 'from': {'id': 7},
            'data': signin.cancel_data(record), 'message': {'message_id': 900, 'message_thread_id': 4,
            'chat': {'id': -10042, 'type': 'supergroup'}}}})
        self.assertEqual(result, 'control_callback')
        await self.settled()
        self.assertIn('account/loginCancel', self.methods())
        self.assert_no_account()
        self.assert_group_gone(pid)
        self.assertFalse(self.driver.muse_cards)

    async def test_timeout_sends_login_cancel_and_adds_nothing(self):
        await self.start(device_seconds=.08)
        await self.settled()
        self.assertIn('account/loginCancel', self.methods())
        self.assert_no_account()

    async def test_unsafe_urls_never_post_a_card_and_cancel_login(self):
        urls = ('http://auth.meta.com/oauth/device/', 'https://evil.example/oauth/device/',
                'https://auth.meta.com/other', 'https://user@auth.meta.com/oauth/device/',
                'https://auth.meta.com:443/oauth/device/', 'https://auth.meta.com:/oauth/device/',
                'https://auth.meta.com.evil.example/oauth/device/', 'https://auth.meta.com/oauth/device/\ntext',
                'https://auth.meta.com/oauth/device/../other')
        for url in urls:
            with self.subTest(url=url):
                self.configure(url=url)
                await self.start()
                await self.settled()
                self.assert_no_account()
                self.assertIn('account/loginCancel', self.methods())
                self.assertFalse(self.driver.muse_cards)
                self.assert_no_code()
                self.assertEqual(self.store.db.execute('SELECT code FROM problems ORDER BY id DESC LIMIT 1').fetchone()[0],
                                 'signin_failed')
                self.task.cancel()
                await asyncio.gather(self.task, return_exceptions=True)

    async def test_bad_codes_never_post_a_card_and_cancel_login(self):
        for code in ('abcd-efgh', 'A' * 40, '------', 'ABC12DEF', 'ABCDE'):
            with self.subTest(code=code):
                self.configure(code=code)
                await self.start()
                await self.settled()
                self.assert_no_account()
                self.assertIn('account/loginCancel', self.methods())
                self.assertFalse(self.driver.muse_cards)
                self.assert_no_code(code)
                self.task.cancel()
                await asyncio.gather(self.task, return_exceptions=True)

    async def test_serve_argv_and_requests_use_shared_msp_contract(self):
        await self.start()
        await self.card()
        launch = json.loads((self.directory / 'launch.json').read_text())
        self.assertEqual(launch['argv'], ['serve', '--provider', 'meta', '--contributor-mode', 'off',
                                          '--disable-request-user-input'])
        for event in self.calls():
            self.assertEqual(event['jsonrpc'], '2.0')
            if 'id' in event:
                self.assertEqual(uuid.UUID(event['id'].removeprefix('torii-')).version, 7)
        self.boundary.assert_called_once()

    async def test_code_is_absent_from_all_store_values_problem_records_and_logs(self):
        self.configure(hold=True, stderr_code=True)
        await self.start()
        await self.card()
        self.assert_no_code()
        self.call('account.signin_cancel')
        await self.settled()
        self.assert_no_code()

    async def test_failure_moves_nonempty_home_to_trash_and_preserves_namesake(self):
        self.configure(outcome='denied')
        await self.start()
        self.trash.mkdir()
        namesake = self.trash / self.directory.name
        namesake.mkdir()
        (namesake / 'keep').write_text('existing')
        await self.settled()
        self.assertFalse(self.directory.exists())
        moved = self.trash / (self.directory.name + '-1')
        self.assertTrue((moved / 'calls.jsonl').is_file())
        self.assertEqual((namesake / 'keep').read_text(), 'existing')
        self.assert_no_account()

    async def test_remove_signed_in_refuses_and_never_sends_logout(self):
        self.configure()
        await self.start()
        await self.settled()
        alias = self.store.get('muse_active_account')
        result = self.call('account.remove', alias=alias)
        self.assertFalse(result.ok)
        self.assertIn('Only signed-out', result.text)
        self.assertNotIn('account/logout', self.methods())
        self.assertIn(alias, self.store.get('muse_accounts'))

    async def test_remove_signed_out_keeps_home_records_it_and_clears_active(self):
        self.configure()
        await self.start()
        await self.settled()
        alias = self.store.get('muse_active_account')
        self.store.put('muse_account_status', {alias: {'identity': {'logged_in': False}}})
        result = self.call('account.remove', alias=alias)
        self.assertTrue(result.ok, result.text)
        self.assertTrue(self.directory.is_dir())
        self.assertIn(str(self.directory), self.store.get('account_dirs_removed'))
        self.assertIsNone(self.store.get('muse_active_account'))
        self.assertNotIn('account/logout', self.methods())

    async def test_off_switch_refuses_before_pin_platform_or_launch(self):
        self.store.put('muse_enabled', False)
        result = self.call()
        self.assertFalse(result.ok)
        self.assertEqual(result.text, 'Muse workers are off.')
        muse_accounts.check_binary.assert_not_called()
        self.boundary.assert_not_called()
        self.assertIsNone(self.store.get(signin.KEY))

    async def test_bad_pin_and_non_darwin_refuse_before_job_creation(self):
        muse_accounts.check_binary.side_effect = control_api.Refused('bad pin')
        result = self.call()
        self.assertFalse(result.ok)
        self.assertEqual(result.text, 'bad pin')
        muse_accounts.check_binary.side_effect = None
        with patch.object(muse_accounts.sys, 'platform', 'linux'):
            result = self.call()
        self.assertFalse(result.ok)
        self.assertIn('macOS', result.text)
        self.assertIsNone(self.store.get(signin.KEY))
        self.boundary.assert_not_called()

    async def test_initialize_mismatch_never_starts_login(self):
        for changes in ({'fingerprint': 'wrong'}, {'experimental_api': False}, {'grants': []},
                        {'muse_home': str(self.root / 'outside')}):
            with self.subTest(changes=changes):
                self.configure(**changes)
                await self.start()
                await self.settled()
                self.assertNotIn('account/loginStart', self.methods())
                self.assert_no_account()
                self.task.cancel()
                await asyncio.gather(self.task, return_exceptions=True)

    async def test_queued_job_rechecks_off_switch_and_pin_before_launch(self):
        self.assertTrue(self.call().ok)
        record = self.store.get(signin.KEY)
        self.store.put('muse_enabled', False)
        driver = signin.SignIns(self.store, muse_binary=lambda: str(self.fake))
        await driver.drive(record)
        self.assert_no_account()
        self.boundary.assert_not_called()
        self.assertIsNone(self.store.get(signin.KEY))

    async def test_explicit_signed_out_alias_replaces_only_that_account_and_keeps_old_home(self):
        old = self.home / 'old'
        old.mkdir()
        self.store.put('muse_accounts', {'muse-old': {'config_dir': str(old), 'label': 'Old'}})
        self.configure()
        result = self.call(alias='muse-old')
        self.assertTrue(result.ok, result.text)
        self.directory = Path(self.store.get(signin.KEY)['config_dir'])
        driver = signin.SignIns(self.store, muse_binary=lambda: str(self.fake))
        await driver.drive(self.store.get(signin.KEY))
        self.assertEqual(list(self.store.get('muse_accounts')), ['muse-old'])
        self.assertEqual(self.store.get('muse_accounts')['muse-old']['config_dir'], str(self.directory))
        self.assertTrue(old.is_dir())
        self.assertIn(str(old), self.store.get('muse_accounts')['muse-old']['previous_config_dirs'])

    async def test_recover_checks_marker_and_start_time_before_group_term(self):
        self.assertTrue(self.call().ok)
        record = dict(self.store.get(signin.KEY), state='approve', pid=1234, started='saved-start')
        self.store.put(signin.KEY, record)
        driver = signin.SignIns(self.store)
        with patch.object(signin, '_leftover', return_value=True) as leftover, \
                patch.object(signin, '_started', return_value='saved-start'), patch.object(os, 'killpg') as kill:
            driver.recover()
        leftover.assert_called_once_with(1234, ' serve --provider meta')
        kill.assert_called_once_with(1234, signal.SIGTERM)
        self.assert_no_account()

    async def test_recover_does_not_signal_reused_pid(self):
        self.assertTrue(self.call().ok)
        self.store.put(signin.KEY, dict(self.store.get(signin.KEY), state='approve', pid=1234, started='old-start'))
        with patch.object(signin, '_leftover', return_value=True), \
                patch.object(signin, '_started', return_value='new-start'), patch.object(os, 'killpg') as kill:
            signin.SignIns(self.store).recover()
        kill.assert_not_called()
        self.assertIsNone(self.store.get(signin.KEY))

    async def test_client_rejects_logout_before_any_write(self):
        client = muse_accounts.MuseClient(str(self.fake), self.home / '.muse-accounts/.torii-12345678')
        with self.assertRaises(ValueError):
            await client.request('account/logout')

    async def test_inline_boundary_has_no_mutable_profile_file(self):
        with patch.object(muse_accounts.os, 'access', return_value=True):
            argv = self.real_boundary(['/bin/true'], [self.home])
        self.assertEqual(argv[:2], ['/usr/bin/sandbox-exec', '-p'])
        self.assertIn('(deny file-write*', argv[2])
        self.assertIn('(subpath ' + json.dumps(str(self.home)) + ')', argv[2])
        self.assertEqual(argv[3:], ['/bin/true'])
        self.assertFalse(list(self.home.glob('.muse-boundary-*')))

    async def test_malformed_deep_frame_fails_without_leaving_a_process_or_new_home(self):
        self.configure(malformed_frame=True)
        await self.start()
        await self.settled()
        self.assert_no_account()
        self.assertFalse(self.directory.exists())
        self.assertTrue((self.trash / self.directory.name).is_dir())
        self.assertFalse(self.driver.muse_cards)
        launch = json.loads((self.trash / self.directory.name / 'launch.json').read_text())
        self.assert_group_gone(launch['pid'])

    async def test_muse_account_provider_and_merged_worker_tool_hash(self):
        self.store.put('muse_enabled', False)
        self.assertEqual(self.call().text, 'Muse workers are off.')
        self.assertEqual(mcp.tools_hash(), '86b8ed8b28e82bc028f0054fb83d5c3b2c4a2ecb8dbb0010e506a13059759efa')

    async def test_notification_before_login_start_reply_is_not_lost_and_missing_label_is_display_only(self):
        self.configure(completion_first=True, label=None)
        await self.start()
        await self.settled()
        self.assertEqual(list(self.store.get('muse_accounts')), ['muse-account'])
        self.assertTrue(muse_accounts.MuseBroker(self.store).authenticated('muse-account'))

    async def test_cancel_while_reading_granted_account_does_not_register_it(self):
        self.configure(hold_account=True)
        await self.start()
        await until(lambda: 'account/read' in self.methods(), 'account read after granted')
        pid = self.store.get(signin.KEY)['pid']
        self.call('account.signin_cancel')
        await self.settled()
        self.assertIn('account/loginCancel', self.methods())
        self.assert_no_account()
        self.assert_group_gone(pid)

    async def test_driver_cancellation_escalates_ignored_term_to_kill_and_reaps_group(self):
        self.configure(hold=True, ignore_term=True)
        await self.start()
        await self.card()
        pid = self.store.get(signin.KEY)['pid']
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)
        self.assertIn('account/loginCancel', self.methods())
        self.assert_no_account()
        self.assert_group_gone(pid)
        self.assertFalse(self.driver.muse_cards)
        self.assertFalse(self.directory.exists())

    async def test_crashed_signin_leader_still_ends_its_child_and_group(self):
        pid, child = await self.start_with_group_child()
        os.kill(pid, signal.SIGKILL)
        await self.settled()
        with self.assertRaises(ProcessLookupError, msg='the crashed sign-in left its child running'):
            os.kill(child, 0)
        self.assert_group_gone(pid)
        self.assertTrue((self.trash / self.directory.name / 'child.term').is_file(),
                        'the sign-in child did not receive group TERM')
        self.assert_no_account()

    async def test_timeout_kills_term_ignoring_child_after_the_leader_exits(self):
        pid, child = await self.start_with_group_child(ignore_term=True, device_seconds=1)
        await self.settled()
        with self.assertRaises(ProcessLookupError, msg='a TERM-ignoring child survived the sign-in timeout'):
            os.kill(child, 0)
        self.assert_group_gone(pid)
        self.assertIn('account/loginCancel', self.methods())
        self.assert_no_account()

    async def test_group_probe_permission_race_does_not_fail_cancellation(self):
        await self.start()
        await self.card()
        pid = self.store.get(signin.KEY)['pid']
        killpg = os.killpg
        observed = False
        def probe(group, number):
            nonlocal observed
            if group == pid and number == 0 and not observed:
                observed = True
                raise PermissionError('terminating group has not been reaped')
            return killpg(group, number)
        with patch.object(muse_accounts.os, 'killpg', side_effect=probe):
            self.call('account.signin_cancel')
            await self.settled()
        self.assertTrue(observed)
        self.assert_group_gone(pid)
        row = self.store.db.execute('SELECT text FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
        self.assertEqual(row['text'], signin.MUSE_CANCELLED)
        self.assert_no_account()

    async def test_device_url_rejects_markdown_link_in_path(self):
        await self.assert_rejected_device_url(URL + '/[Open%20Meta](https://evil.example/phish)')

    async def test_device_url_rejects_markdown_link_in_query(self):
        await self.assert_rejected_device_url(URL + '?next=[Tap](https://evil.example/phish)')

    async def test_device_url_rejects_markdown_link_in_fragment(self):
        await self.assert_rejected_device_url(URL + '#[Tap](https://evil.example/phish)')

    async def test_device_url_rejects_html_link_in_path(self):
        for separator in (' ', '_'):
            with self.subTest(separator=separator):
                await self.assert_rejected_device_url(
                    URL + '/<a' + separator + 'href="https://evil.example/phish">Open%20Meta</a>')

    async def test_device_url_rejects_html_link_in_query(self):
        for separator in (' ', '_'):
            with self.subTest(separator=separator):
                await self.assert_rejected_device_url(
                    URL + '?next=<a' + separator + 'href="https://evil.example/phish">Open%20Meta</a>')

    async def test_device_url_rejects_html_link_in_fragment(self):
        for separator in (' ', '_'):
            with self.subTest(separator=separator):
                await self.assert_rejected_device_url(
                    URL + '#<a' + separator + 'href="https://evil.example/phish">Open%20Meta</a>')

    async def test_device_url_rejects_markup_delimiters(self):
        for suffix in ('<a_href="https://evil.example">x</a>', '*b*`c`~~s~~||sp||',
                       '<', '>', '"', "'", '`', '[', ']', '(', ')'):
            with self.subTest(suffix=suffix):
                await self.assert_rejected_device_url(URL + '/' + suffix)

    async def test_device_url_rejects_bidi_and_other_format_or_control_characters(self):
        for character in ('\u202e', '\u202d', '\u2066', '\u2069', '\u200b', '\ufeff', '\x7f'):
            with self.subTest(character=repr(character)):
                await self.assert_rejected_device_url(URL + '/' + character + 'elpmaxe.live')

    async def test_device_url_rejects_5000_character_suffix(self):
        await self.assert_rejected_device_url(URL + '/' + 'a' * 5000)

    async def test_device_url_length_is_bounded_at_2048_characters(self):
        url = URL + 'a' * (2048 - len(URL))
        self.assertTrue(signin.muse_device({'verificationUrl': url, 'userCode': CODE}))
        self.assertFalse(signin.muse_device({'verificationUrl': url + 'a', 'userCode': CODE}))

    async def test_registration_rechecks_one_account_after_the_device_grant(self):
        gate = self.root / 'grant'
        self.configure(release=str(gate))
        await self.start()
        await self.card()
        existing = {'muse-existing': {'config_dir': str(self.home / 'existing'), 'label': 'Existing'}}
        self.store.put('muse_accounts', existing)
        self.store.put('muse_active_account', 'muse-existing')
        gate.touch()
        await self.settled()
        self.assertEqual(self.store.get('muse_accounts'), existing)
        self.assertEqual(self.store.get('muse_active_account'), 'muse-existing')
        self.assertFalse(self.directory.exists())
        self.assertTrue((self.trash / self.directory.name).is_dir())
        row = self.store.db.execute('SELECT text FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
        self.assertEqual(row['text'], 'Torii already has one Muse account. No account was added.')

    async def test_failed_muse_signin_cannot_offer_a_retry_for_another_provider(self):
        self.configure(outcome='denied')
        await self.start()
        await self.settled()
        self.assertIsNone(self.store.get(signin.RETRY_KEY))
        rows = self.store.db.execute('SELECT reply_markup FROM outbox WHERE retired=0').fetchall()
        self.assertTrue(rows)
        self.assertTrue(all(row['reply_markup'] is None for row in rows))

    async def test_telegram_retry_reuses_memory_card_without_restarting_device_flow(self):
        await self.start()
        await until(lambda: (self.store.get(signin.KEY) or {}).get('state') == 'approve', 'Muse sign-in card')
        async def fail(row):
            raise TelegramError(429, retry_after=0, reason='synthetic rate limit')
        with patch.object(self.telegram, 'send', side_effect=fail):
            await Service.deliver_once(self.service)
        row = self.store.db.execute('SELECT id FROM outbox WHERE delivered=0 ORDER BY id LIMIT 1').fetchone()
        self.store.db.execute('UPDATE outbox SET next_attempt=0 WHERE id=?', (row['id'],))
        await Service.deliver_once(self.service)
        self.assertIn(CODE, self.telegram.sent[-1]['text'])
        self.assertEqual(self.methods().count('account/loginStart'), 1)
        self.assert_no_code()

    async def test_restart_retires_unsent_memory_card_and_moves_new_home_to_trash(self):
        await self.start()
        await until(lambda: (self.store.get(signin.KEY) or {}).get('state') == 'approve', 'Muse sign-in card')
        record = self.store.get(signin.KEY)
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)
        self.store.put(signin.KEY, record)
        restarted = signin.SignIns(self.store)
        with patch.object(signin, '_leftover', return_value=False):
            restarted.recover()
        self.assertFalse(restarted.muse_cards)
        self.assertIsNone(self.store.get(signin.KEY))
        card = self.store.db.execute('SELECT retired FROM outbox WHERE id=?', (record['card_outbox'],)).fetchone()
        self.assertTrue(card['retired'])
        self.assert_no_code()

    async def test_fake_client_uses_real_inline_boundary_and_cannot_write_outside_home(self):
        if not DARWIN:
            with patch.object(muse_accounts.sys, 'platform', 'linux'), self.assertRaises(control_api.Refused):
                self.real_boundary(['/bin/true'], [self.home])
            self.assertTrue(hasattr(muse_accounts, 'MuseClient'))
            return
        outside = self.root / 'outside'
        self.configure(hold=True, outside_write=str(outside))
        self.boundary.side_effect = self.real_boundary
        await self.start()
        await self.card()
        self.assertFalse(outside.exists())
        self.assertFalse(list(self.directory.glob('.muse-boundary-*')))
        self.call('account.signin_cancel')
        await self.settled()
        self.assert_no_account()
