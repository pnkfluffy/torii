"""Offline Piece A checks with owner-file fixtures and a version-only fake."""

import builtins
import io
import json
import os
from pathlib import Path
import shlex
import stat
import subprocess
import tempfile
import unittest
import uuid
from unittest.mock import patch

from coordinator import control_api, mcp, opencode_accounts, signin
from coordinator.accounts import AccountBroker
from coordinator.opencode_accounts import PINNED_VERSION
from coordinator.providers import ProviderRunner
from coordinator.store import Store
from coordinator.workers import WorkerPool
from tests.fake_opencode import install_versioned_opencode
from tests.support import stop_test_hosts


class OpenCodeAccountsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='opencode-accounts-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.home = self.root / 'home'
        self.home.mkdir()
        self.environment = patch.dict(os.environ, {'HOME': str(self.home)})
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.store = Store(self.root / 'state')
        self.addCleanup(lambda: self.store.close())

    def call(self, op, **params):
        with self.store.db:
            return control_api.call(self.store, op, params)

    def register(self, alias='host', **changes):
        params = dict(alias=alias, base_url='https://api.example.test/v1', model='example-host/example-model',
                      context=200000, output=32000, price_in=0.7, price_cached=0.1, price_out=2.5)
        params.update(changes)
        return self.call('opencode.account', **params)

    def install_binary(self, version=PINNED_VERSION, exit_code=0):
        binary = self.root / 'opencode'
        calls = self.root / 'binary.calls'
        binary.write_text('#!/bin/sh\n'
                          '[ "$#" -eq 1 ] && [ "$1" = "--version" ] || exit 64\n'
                          'printf "%s\\n" "$1" >> ' + shlex.quote(str(calls)) + '\n'
                          'printf "%s\\n" ' + shlex.quote(version) + '\n'
                          'exit ' + str(exit_code) + '\n')
        binary.chmod(0o700)
        self.store.put('opencode_binary', str(binary))
        return binary

    def ready_account(self, key=b'offline-placeholder\n'):
        result = self.register()
        self.assertTrue(result.ok, result.text)
        result = self.call('opencode.use', alias='host')
        self.assertTrue(result.ok, result.text)
        path = self.home / '.opencode-accounts' / 'host' / 'key'
        path.write_bytes(key)
        path.chmod(0o600)
        return path

    def assert_refused(self, result, text):
        self.assertEqual((result.ok, result.state), (False, 'refused'), result.text)
        self.assertIn(text, result.text)
        self.assertIs(self.call('settings.get').data['opencode_enabled'], False)

    def test_binary_resolves_each_process_pin_without_changing_setting(self):
        paths = {v: install_versioned_opencode(self.root / 'versions', v) for v in ('1.19.0', '1.19.3')}
        for pin in paths:
            for selected in paths:
                with self.subTest(pin=pin, selected=selected), patch.object(opencode_accounts, 'PINNED_VERSION', pin):
                    self.store.put('opencode_binary', str(paths[selected]))
                    self.assertEqual(opencode_accounts.binary(self.store), str(paths[pin]))
                    opencode_accounts._check_binary(opencode_accounts.binary(self.store))
                    self.assertEqual(self.store.get('opencode_binary'), str(paths[selected]))

    def test_binary_returns_unresolved_setting_and_keeps_working_symlink_result(self):
        loop = self.root / 'loop'
        loop.symlink_to(loop)
        configured = install_versioned_opencode(self.root / 'versions', PINNED_VERSION)
        link = self.root / 'opencode-link'
        link.symlink_to(configured)
        cases = [('symlink loop', str(loop), str(loop)),
                 ('NUL byte', str(self.root) + '\x00opencode', str(self.root) + '\x00opencode'),
                 ('working symlink', str(link), str(configured))]
        for label, value, expected in cases:
            with self.subTest(setting=label):
                self.store.put('opencode_binary', value)
                self.assertEqual(opencode_accounts.binary(self.store), expected)

    def test_startup_builds_for_unresolved_settings_and_check_ready_refuses(self):
        loop = self.root / 'loop'
        loop.symlink_to(loop)
        cases = [('symlink loop', str(loop)), ('NUL byte', str(self.root) + '\x00opencode')]
        for index, (label, configured) in enumerate(cases):
            with self.subTest(setting=label):
                self.store.put('opencode_binary', configured)
                runner = ProviderRunner(self.root / ('pool-runner-%d' % index),
                                        binaries={'claude': 'claude', 'codex': 'codex'})
                WorkerPool(self.store, runner, AccountBroker(self.store))
                self.assertEqual(runner.binaries['opencode'], configured)
                provider_runner = ProviderRunner(self.root / ('provider-runner-%d' % index),
                                                 opencode_store=self.store)
                self.assertEqual(provider_runner.binaries['opencode'], configured)
                self.store.put('opencode_binary', str(self.root / 'missing'))
                with self.assertRaises(control_api.Refused) as expected:
                    opencode_accounts.check_ready(self.store)
                self.assertEqual(str(expected.exception),
                                 f'Set opencode_binary to the absolute path of the pinned OpenCode {PINNED_VERSION} binary.')
                self.store.put('opencode_binary', configured)
                with self.assertRaises(control_api.Refused) as actual:
                    opencode_accounts.check_ready(self.store)
                self.assertEqual(str(actual.exception), str(expected.exception))

    def test_binary_outside_versioned_layout_is_unchanged(self):
        for configured in (None, '', 'opencode', str(self.install_binary('1.19.0'))):
            with self.subTest(configured=configured):
                self.store.put('opencode_binary', configured)
                self.assertEqual(opencode_accounts.binary(self.store), configured)
        self.ready_account()
        with self.assertRaisesRegex(control_api.Refused, 'exactly version'):
            opencode_accounts.check_ready(self.store)

    def test_binary_missing_or_mismatched_sibling_keeps_today_refusal(self):
        configured = install_versioned_opencode(self.root / 'versions', '1.19.0')
        self.store.put('opencode_binary', str(configured))
        self.ready_account()
        sibling = self.root / 'versions/1.19.3'
        manifest = sibling / 'node_modules/opencode-ai/package.json'
        for state in ('missing', 'missing-binary', 'wrong-version', 'wrong-package', 'invalid-json'):
            with self.subTest(state=state):
                if state == 'missing-binary':
                    manifest.parent.mkdir(parents=True)
                    manifest.write_text(json.dumps({'name': 'opencode-ai', 'version': '1.19.3'}))
                elif state == 'wrong-version':
                    install_versioned_opencode(self.root / 'versions', '1.19.3')
                    manifest.write_text(json.dumps({'name': 'opencode-ai', 'version': '1.18.0'}))
                elif state == 'wrong-package':
                    manifest.write_text(json.dumps({'name': 'other', 'version': '1.19.3'}))
                elif state == 'invalid-json':
                    manifest.write_text('{')
                self.assertEqual(opencode_accounts.binary(self.store), str(configured))
                with self.assertRaisesRegex(control_api.Refused, 'exactly version'):
                    opencode_accounts.check_ready(self.store)

    def test_binary_unverified_source_prefix_does_not_select_sibling(self):
        configured = install_versioned_opencode(self.root / 'versions', '1.19.0')
        install_versioned_opencode(self.root / 'versions', '1.19.3')
        self.store.put('opencode_binary', str(configured))
        manifest = self.root / 'versions/1.19.0/node_modules/opencode-ai/package.json'
        manifest.write_text(json.dumps({'name': 'opencode-ai', 'version': '1.18.0'}))
        self.assertEqual(opencode_accounts.binary(self.store), str(configured))

    def test_defaults_are_off_without_creating_account_folders(self):
        result = self.call('settings.get')
        self.assertTrue(result.ok, result.text)
        self.assertEqual({key: value for key, value in result.data.items() if key.startswith('opencode_')},
                         {'opencode_enabled': False, 'opencode_binary': None, 'opencode_active_account': None,
                          'opencode_model': None, 'opencode_base_url_host': None})
        self.assertEqual(opencode_accounts.accounts(self.store), {})
        self.assertIsNone(self.store.get('opencode_binary'))
        self.assertFalse((self.home / '.opencode-accounts').exists())

    def test_account_metadata_is_stored_and_folders_hold_only_the_owner_key(self):
        with patch('coordinator.opencode_accounts.time.time', return_value=123):
            result = self.register()
        self.assertTrue(result.ok, result.text)
        root = self.home / '.opencode-accounts'
        home = root / 'host'
        self.assertEqual(stat.S_IMODE(root.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(home.stat().st_mode), 0o700)
        self.assertEqual(list(home.iterdir()), [])
        self.assertEqual(self.store.get('opencode_accounts'), {'host': {
            'base_url': 'https://api.example.test/v1', 'model': 'example-host/example-model', 'small_model': 'example-host/example-model',
            'context': 200000, 'output': 32000, 'price_in': 0.7, 'price_cached': 0.1, 'price_out': 2.5, 'created': 123}})
        key = home / 'key'
        key.write_bytes(b'offline-placeholder\n')
        key.chmod(0o600)
        home.chmod(0o755)
        with patch('coordinator.opencode_accounts.time.time', return_value=456):
            result = self.register(model='example-host/example-fast-model', small_model='host/title-model')
        self.assertTrue(result.ok, result.text)
        profile = self.store.get('opencode_accounts')['host']
        self.assertEqual((profile['created'], profile['model'], profile['small_model']),
                         (123, 'example-host/example-fast-model', 'host/title-model'))
        self.assertEqual(list(home.iterdir()), [key])
        self.assertEqual(key.read_bytes(), b'offline-placeholder\n')
        self.assertEqual(stat.S_IMODE(home.stat().st_mode), 0o700)
        self.assertIsNone(opencode_accounts.active(self.store))
        self.assertIs(self.call('settings.get').data['opencode_enabled'], False)

    def test_active_account_is_manual_and_persists_without_rotation(self):
        self.assert_refused(self.call('opencode.use', alias='missing'), 'Unknown OpenCode account')
        self.assertTrue(self.register().ok)
        self.assertTrue(self.register(alias='other').ok)
        self.assertIsNone(opencode_accounts.active(self.store))
        self.assertTrue(self.call('opencode.use', alias='host').ok)
        self.store.close()
        self.store = Store(self.root / 'state')
        self.assertEqual(self.store.get('opencode_active_account'), 'host')
        self.assertEqual(opencode_accounts.active(self.store), 'host')
        self.store.put('opencode_accounts', {'other': self.store.get('opencode_accounts')['other']})
        self.assertIsNone(opencode_accounts.active(self.store))

    def test_alias_validation_blocks_path_tricks_before_creating_folders(self):
        for alias in ('../escape', '/absolute', '.', '', '_host', 'host.name', 'a' * 65, 'host/child'):
            with self.subTest(alias=alias):
                self.assert_refused(self.register(alias=alias), 'account name')
                with self.assertRaises(control_api.Refused):
                    opencode_accounts.account_home(alias)
        self.assertFalse((self.home / '.opencode-accounts').exists())
        self.assertTrue(self.register(alias='Host_1-test').ok)

    def test_base_url_accepts_https_and_only_the_two_http_loopback_hosts(self):
        for url in ('https://api.example.test/v1', 'https://example.test:8443/',
                    'http://127.0.0.1:8080/v1', 'http://localhost/v1'):
            with self.subTest(url=url):
                self.assertTrue(self.register(base_url=url).ok)
                self.assertEqual(self.store.get('opencode_accounts')['host']['base_url'], url)
        for url in ('http://api.example.test/v1', 'http://localhost.example.test', 'http://127.0.0.2',
                    'http://[::1]', 'ftp://localhost', 'https://', 'https:///path', 'https://host:bad',
                    'https://user@example.test/v1', 'https://example.test?token=placeholder',
                    'https://example.test/#fragment', 'https://exa mple.test', 'https://example.test\\path'):
            with self.subTest(url=url):
                before = self.store.get('opencode_accounts')
                self.assert_refused(self.register(base_url=url), 'HTTPS base URL')
                self.assertEqual(self.store.get('opencode_accounts'), before)

    def test_limits_and_prices_accept_cli_numbers_and_refuse_invalid_values(self):
        result = self.register(context='200000', output='32000', price_in='0.7', price_cached='0', price_out='2.5')
        self.assertTrue(result.ok, result.text)
        profile = self.store.get('opencode_accounts')['host']
        self.assertEqual((profile['context'], profile['price_in'], profile['price_cached']), (200000, 0.7, 0))
        for changes in ({'context': 0}, {'output': -1}, {'context': True}, {'output': 1.5},
                        {'price_in': -0.1}, {'price_out': float('inf')}, {'price_cached': float('nan')},
                        {'price_in': True}, {'price_out': 'not-a-price'}, {'price_out': '1e999'}):
            with self.subTest(changes=changes):
                before = self.store.get('opencode_accounts')
                result = self.register(**changes)
                self.assertEqual((result.ok, result.state), (False, 'refused'), result.text)
                self.assertEqual(self.store.get('opencode_accounts'), before)

    def test_account_registration_refuses_other_provider_aliases(self):
        for key in ('accounts', 'codex_accounts'):
            with self.subTest(provider=key):
                self.store.put(key, {'host': {}})
                self.assert_refused(self.register(), 'another provider')
                self.assertIsNone(self.store.get('opencode_accounts'))
                self.assertFalse((self.home / '.opencode-accounts').exists())
                self.store.put(key, {})

    def test_account_add_refuses_opencode_aliases_before_signin_when_off_or_on(self):
        self.assertTrue(self.register().ok)
        self.assertIn('host', signin.taken_aliases(self.store))
        self.assertEqual(signin.account_name('host@example.test', signin.taken_aliases(self.store)), 'host-2')
        for enabled in (False, True):
            self.store.put('opencode_enabled', enabled)
            before = list(self.store.db.execute('SELECT key,value FROM settings ORDER BY key'))
            with patch('coordinator.signin.op_account_add') as signin_handler:
                for provider in ('claude', 'codex'):
                    result = self.call('account.add', alias='host', provider=provider)
                    self.assertEqual((result.ok, result.state), (False, 'refused'))
                    self.assertIn('OpenCode account', result.text)
                signin_handler.assert_not_called()
            self.assertEqual(list(self.store.db.execute('SELECT key,value FROM settings ORDER BY key')), before)

    def test_account_folders_refuse_symlinks_and_unrelated_contents(self):
        outside = self.root / 'outside'
        outside.mkdir(mode=0o755)
        outside.chmod(0o755)
        root = self.home / '.opencode-accounts'
        root.symlink_to(outside, target_is_directory=True)
        self.assert_refused(self.register(), 'real folders')
        self.assertEqual(stat.S_IMODE(outside.stat().st_mode), 0o755)
        root.unlink()
        root.mkdir()
        home = root / 'host'
        home.symlink_to(outside, target_is_directory=True)
        self.assert_refused(self.register(), 'real folders')
        home.unlink()
        home.mkdir()
        (home / 'unexpected').write_text('owner data')
        self.assert_refused(self.register(), 'only its owner-written key')
        self.assertEqual((home / 'unexpected').read_text(), 'owner data')

    def test_turn_on_refuses_missing_or_non_absolute_binary(self):
        self.ready_account()
        for binary in (None, '', 'opencode', str(self.root / 'missing'), str(self.root)):
            with self.subTest(binary=binary):
                self.store.put('opencode_binary', binary)
                self.assert_refused(self.call('delegation.opencode', enabled='on'), 'absolute path')

    def test_turn_on_refuses_wrong_version_or_failed_version_probe(self):
        self.ready_account()
        for version in ('1.18.0', '1.19.0', '2.0.29', 'OpenCode ' + PINNED_VERSION,
                        ' ' + PINNED_VERSION, PINNED_VERSION + ' ', PINNED_VERSION + '\nextra'):
            with self.subTest(version=version):
                self.install_binary(version)
                self.assert_refused(self.call('delegation.opencode', enabled=True), f'exactly version {PINNED_VERSION}')
        self.install_binary(exit_code=7)
        self.assert_refused(self.call('delegation.opencode', enabled=True), f'exactly version {PINNED_VERSION}')
        binary = self.install_binary()
        binary.chmod(0o600)
        self.assert_refused(self.call('delegation.opencode', enabled=True), 'could not report its version')
        with patch('coordinator.opencode_accounts.subprocess.run', side_effect=subprocess.TimeoutExpired('fake', 10)):
            self.assert_refused(self.call('delegation.opencode', enabled=True), 'could not report its version')

    def test_binary_op_refuses_invalid_paths_and_versions(self):
        for path in ('opencode', str(self.root / 'missing'), str(self.root)):
            with self.subTest(path=path):
                self.assert_refused(self.call('opencode.binary', path=path), 'absolute path')
                self.assertIsNone(self.store.get('opencode_binary'))
        binary = self.install_binary()
        self.store.put('opencode_binary', None)
        with patch('coordinator.opencode_accounts.subprocess.run', side_effect=OSError('unavailable')):
            self.assert_refused(self.call('opencode.binary', path=str(binary)), 'could not report its version')
        self.assertIsNone(self.store.get('opencode_binary'))
        for version in ('1.18.0', '1.19.0'):
            with self.subTest(version=version):
                binary = self.install_binary(version=version)
                self.store.put('opencode_binary', None)
                self.assert_refused(self.call('opencode.binary', path=str(binary)), f'exactly version {PINNED_VERSION}')
                self.assertIsNone(self.store.get('opencode_binary'))
        binary = self.install_binary(exit_code=7)
        self.store.put('opencode_binary', None)
        self.assert_refused(self.call('opencode.binary', path=str(binary)), f'exactly version {PINNED_VERSION}')
        self.assertIsNone(self.store.get('opencode_binary'))

    def test_binary_op_accepts_pinned_binary_and_node_modules_symlink(self):
        binary = self.install_binary()
        self.store.put('opencode_binary', None)
        result = self.call('opencode.binary', path=str(binary))
        self.assertTrue(result.ok, result.text)
        self.assertEqual(self.store.get('opencode_binary'), str(binary))
        self.assertIs(self.store.get('opencode_enabled', False), False)
        self.assertEqual(self.call('settings.get').data['opencode_binary'], str(binary))
        link = self.root / 'node_modules' / '.bin' / 'opencode'
        link.parent.mkdir(parents=True)
        link.symlink_to(binary)
        result = self.call('opencode.binary', path=str(link))
        self.assertTrue(result.ok, result.text)
        self.assertEqual(self.store.get('opencode_binary'), str(link))
        self.assertEqual(self.call('settings.get').data['opencode_binary'], str(link))
        self.assertIs(self.store.get('opencode_enabled', False), False)

    def test_binary_op_allows_turn_on_after_account_and_key_are_ready(self):
        self.ready_account()
        binary = self.install_binary()
        self.store.put('opencode_binary', None)
        result = self.call('opencode.binary', path=str(binary))
        self.assertTrue(result.ok, result.text)
        self.assertIs(self.store.get('opencode_enabled', False), False)
        result = self.call('delegation.opencode', enabled=True)
        self.assertTrue(result.ok, result.text)
        self.assertIs(self.store.get('opencode_enabled'), True)

    def test_turn_on_refuses_missing_or_stale_active_account(self):
        self.install_binary()
        self.assert_refused(self.call('delegation.opencode', enabled=True), 'Select an existing OpenCode account')
        self.assertTrue(self.register().ok)
        self.assert_refused(self.call('delegation.opencode', enabled=True), 'Select an existing OpenCode account')
        self.store.put('opencode_active_account', 'missing')
        self.assert_refused(self.call('delegation.opencode', enabled=True), 'Select an existing OpenCode account')

    def test_turn_on_refuses_missing_key(self):
        self.install_binary()
        self.assertTrue(self.register().ok)
        self.assertTrue(self.call('opencode.use', alias='host').ok)
        self.assert_refused(self.call('delegation.opencode', enabled=True), 'owner-written key file')

    def test_turn_on_refuses_wrong_key_mode_empty_key_and_wrong_owner(self):
        self.install_binary()
        key = self.ready_account()
        for mode in (0o644, 0o400, 0o700, 0o1600):
            with self.subTest(mode=oct(mode)):
                key.chmod(mode)
                self.assert_refused(self.call('delegation.opencode', enabled=True), 'mode 0600')
        key.chmod(0o600)
        with patch('coordinator.opencode_accounts.os.getuid', return_value=os.getuid() + 1):
            self.assert_refused(self.call('delegation.opencode', enabled=True), 'owned by this user')
        key.write_bytes(b'')
        self.assert_refused(self.call('delegation.opencode', enabled=True), 'non-empty')

    def test_turn_on_refuses_non_regular_keys_and_symlinked_account_homes(self):
        self.install_binary()
        key = self.ready_account()
        key.unlink()
        key.mkdir()
        self.assert_refused(self.call('delegation.opencode', enabled=True), 'regular file')
        key.rmdir()
        target = self.root / 'owner-file'
        target.write_bytes(b'offline-placeholder\n')
        target.chmod(0o600)
        key.symlink_to(target)
        self.assert_refused(self.call('delegation.opencode', enabled=True), 'regular file')
        key.unlink()
        os.mkfifo(key, 0o600)
        self.assert_refused(self.call('delegation.opencode', enabled=True), 'regular file')
        key.unlink()
        key.parent.rmdir()
        key.parent.symlink_to(target.parent, target_is_directory=True)
        self.assert_refused(self.call('delegation.opencode', enabled=True), 'real folders')

    def test_turn_on_succeeds_and_off_needs_no_binary_or_key(self):
        self.install_binary()
        key = self.ready_account()
        self.assertTrue(self.call('delegation.opencode', enabled='on').ok)
        self.assertIs(self.store.get('opencode_enabled'), True)
        self.assertEqual((self.root / 'binary.calls').read_text(), '--version\n')
        key.unlink()
        self.store.put('opencode_binary', None)
        result = self.call('delegation.opencode', enabled='off')
        self.assertTrue(result.ok, result.text)
        self.assertEqual(result.text, 'OpenCode delegation is off.')
        self.assertIs(self.store.get('opencode_enabled'), False)

    def test_key_is_never_opened_or_read_by_account_controls(self):
        self.install_binary()
        key = self.ready_account()
        with patch('builtins.open', wraps=builtins.open) as builtin_open, \
                patch('io.open', wraps=io.open) as io_open, \
                patch('os.open', wraps=os.open) as os_open, \
                patch.object(Path, 'open', autospec=True, side_effect=Path.open) as path_open, \
                patch.object(Path, 'read_bytes', autospec=True, side_effect=Path.read_bytes) as read_bytes, \
                patch.object(Path, 'read_text', autospec=True, side_effect=Path.read_text) as read_text:
            self.assertTrue(self.register().ok)
            self.assertTrue(self.call('opencode.use', alias='host').ok)
            self.assertTrue(self.call('delegation.opencode', enabled=True).ok)
            self.assertTrue(self.call('settings.get').ok)
            for probe in (builtin_open, io_open, os_open, path_open, read_bytes, read_text):
                self.assertEqual([call for call in probe.call_args_list if call.args and str(call.args[0]) == str(key)], [])

    def test_settings_show_only_alias_model_and_host_never_key_or_full_url(self):
        self.install_binary()
        key = self.ready_account()
        self.assertTrue(self.call('delegation.opencode', enabled=True).ok)
        result = self.call('settings.get')
        self.assertEqual({name: value for name, value in result.data.items() if name.startswith('opencode_')},
                         {'opencode_enabled': True, 'opencode_binary': str(self.root / 'opencode'),
                          'opencode_active_account': 'host',
                          'opencode_model': 'example-host/example-model', 'opencode_base_url_host': 'api.example.test'})
        rendered = result.text + json.dumps(result.data) + json.dumps(self.store.get('opencode_accounts'))
        self.assertNotIn('offline-placeholder', rendered)
        self.assertNotIn(str(key), rendered)
        self.assertNotIn('https://api.example.test/v1', result.text + json.dumps(result.data))

    def test_piece_a_control_manifest_keeps_opencode_out_of_worker_dispatch(self):
        for name in ('opencode.binary', 'opencode.account', 'opencode.use', 'delegation.opencode'):
            self.assertEqual(control_api.find(name).kind, control_api.WRITE)
        self.assertEqual(control_api.find('workers.spawn').params['provider'], 'worker_provider')
        for name, kind in (('topic.bind', 'provider'), ('model.parent', 'parent_provider')):
            self.assertEqual(control_api.find(name).params['provider'].rstrip('?'), kind)
        for enabled in (False, True):
            self.store.put('opencode_enabled', enabled)
            for name in ('workers.spawn', 'topic.bind', 'model.parent', 'account.add'):
                self.assertFalse(self.call(name, provider='opencode').ok)
        schema = mcp.input_schema(control_api.find('opencode.account'))
        self.assertEqual(schema['properties']['price_in'], {'type': 'number'})
        self.assertEqual(schema['properties']['context'], {'type': 'integer'})
        self.assertNotIn('small_model', schema['required'])
        self.assertNotIn('key', schema['properties'])


class OpenCodeBrokenPathStartupTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='opencode-startup-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        home = self.root / 'home'
        home.mkdir()
        environment = patch.dict(os.environ, {'HOME': str(home)})
        environment.start()
        self.addCleanup(environment.stop)
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        self.addCleanup(stop_test_hosts, self.root)
        profile = self.root / 'claude-profile'
        profile.mkdir()
        self.store.put('accounts', {'fixture': {'config_dir': str(profile), 'enabled': True}})

    async def assert_other_providers_launch(self, configured):
        session_id = str(uuid.uuid4())
        events = {
            'claude': [{'type': 'result', 'subtype': 'success', 'session_id': session_id,
                        'result': 'claude started'}],
            'codex': [{'type': 'thread.started', 'thread_id': session_id},
                      {'type': 'item.completed', 'item': {'type': 'agent_message', 'text': 'codex started'}},
                      {'type': 'turn.completed'}],
        }
        binaries = {}
        for provider, messages in events.items():
            binary = self.root / ('fake-' + provider)
            binary.write_text('#!/usr/bin/env python3\n'
                              'import sys\n'
                              'assert sys.stdin.read() == "startup fixture\\n"\n' +
                              ''.join('print(' + repr(json.dumps(message)) + ', flush=True)\n'
                                      for message in messages))
            binary.chmod(0o700)
            binaries[provider] = str(binary)
        self.store.put('opencode_binary', configured)
        runner = ProviderRunner(self.root / 'runner', binaries=binaries)
        pool = WorkerPool(self.store, runner, AccountBroker(self.store))
        with self.assertRaises(control_api.Refused) as refused:
            opencode_accounts.check_ready(self.store)
        self.assertEqual(str(refused.exception),
                         'Set opencode_binary to the absolute path of the pinned OpenCode 1.19.3 binary.')
        for provider in events:
            with self.subTest(provider=provider):
                result = await pool.runner.run(provider, 'startup fixture', self.root,
                                               session_id if provider == 'claude' else None,
                                               fresh=True, account_alias='fixture' if provider == 'claude' else None)
                self.assertTrue(result.success, result.error)
                self.assertEqual(result.text, provider + ' started')
                self.assertEqual(result.session_id, session_id)
        self.assertEqual(self.store.get('opencode_binary'), configured)

    async def test_symlink_loop_setting_keeps_claude_and_codex_launches_working(self):
        loop = self.root / 'opencode-loop'
        loop.symlink_to(loop)
        await self.assert_other_providers_launch(str(loop))

    async def test_nul_byte_setting_keeps_claude_and_codex_launches_working(self):
        await self.assert_other_providers_launch(str(self.root / 'opencode') + '\x00broken')


if __name__ == '__main__':
    unittest.main()
