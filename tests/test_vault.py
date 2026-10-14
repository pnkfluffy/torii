import base64
import hashlib
import hmac
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from coordinator import vault
from coordinator.providers import ProviderRunner
from coordinator.vault import FakeVault, FileVault, VaultError, resolve_master_key


MASTER = bytes(range(32))
NONCE = bytes(range(100, 116))
VALUE = 'FAKE-ENVELOPE-VALUE-0123456789'
PLAINTEXT = b'{"names": {"GITHUB_TOKEN": "FAKE-ENVELOPE-VALUE-0123456789"}}'
PINNED = {'v': 1, 'alg': 'hmac-sha256-ctr+hmac-sha256', 'nonce': 'ZGVmZ2hpamtsbW5vcHFycw==',
          'ct': 'mG6ap3CPiC1AhhPLteNYlF59prvNBmE9f7/CZyuAMTk0AmyKQ5vuAufHhTb9T8Zmds8HmBKomj/m0sLdzQ==',
          'tag': 'u71lyYCL6EFtAoYpWwUNQtcDAgPepAbjEDkTFPHT8AU='}
PINNED_FINGERPRINT = 'cccf99f4'


def fields(blob):
    return json.loads(blob)


def rebuild(blob, **changes):
    data = fields(blob)
    for name, raw in changes.items():
        data[name] = base64.b64encode(raw).decode()
    return json.dumps(data).encode()


def raw(blob, name):
    return base64.b64decode(fields(blob)[name])


def flip(data, index):
    return data[:index] + bytes([data[index] ^ 1]) + data[index + 1:]


class SealTests(unittest.TestCase):
    def test_round_trip_for_every_size_and_non_utf8_bytes(self):
        for plaintext in (b'', b'x', b'a' * 4096, os.urandom(100 * 1024), b'\xff\xfe\x00\x80'):
            self.assertEqual(vault.open(MASTER, vault.seal(MASTER, plaintext)), plaintext)

    def test_each_seal_uses_a_fresh_nonce(self):
        first, second = fields(vault.seal(MASTER, PLAINTEXT)), fields(vault.seal(MASTER, PLAINTEXT))
        self.assertNotEqual(first['nonce'], second['nonce'])
        self.assertNotEqual(first['ct'], second['ct'])

    def test_known_answer_is_pinned(self):
        with patch('coordinator.vault.secrets.token_bytes', return_value=NONCE):
            blob = vault.seal(MASTER, PLAINTEXT)
        self.assertEqual(fields(blob), PINNED)
        self.assertEqual(vault.fingerprint(MASTER, VALUE), PINNED_FINGERPRINT)
        self.assertEqual(vault.open(MASTER, blob), PLAINTEXT)

    def test_known_answer_matches_an_independent_construction(self):
        def prf(key, data):
            return hmac.new(key, data, hashlib.sha256).digest()
        k_enc = prf(MASTER, b'torii-envelope-v1/enc')
        k_mac = prf(MASTER, b'torii-envelope-v1/mac')
        stream = b''.join(prf(k_enc, NONCE + i.to_bytes(8, 'big')) for i in range(2))
        ciphertext = bytes(a ^ b for a, b in zip(PLAINTEXT, stream))
        tag = prf(k_mac, b'torii-envelope-v1' + NONCE + ciphertext)
        self.assertEqual(base64.b64encode(ciphertext).decode(), PINNED['ct'])
        self.assertEqual(base64.b64encode(tag).decode(), PINNED['tag'])
        k_fp = prf(MASTER, b'torii-envelope-v1/fingerprint')
        self.assertEqual(hmac.new(k_fp, VALUE.encode(), hashlib.sha256).hexdigest()[:8], PINNED_FINGERPRINT)

    def assert_integrity(self, blob):
        with self.assertRaises(VaultError) as caught:
            vault.open(MASTER, blob)
        self.assertEqual(caught.exception.code, 'integrity')

    def test_every_single_byte_change_fails_integrity(self):
        blob = vault.seal(MASTER, PLAINTEXT)
        for name in ('ct', 'tag', 'nonce'):
            data = raw(blob, name)
            for index in range(len(data)):
                self.assert_integrity(rebuild(blob, **{name: flip(data, index)}))

    def test_truncation_swapped_nonce_and_wrong_key_fail_integrity(self):
        blob = vault.seal(MASTER, PLAINTEXT)
        self.assert_integrity(rebuild(blob, ct=raw(blob, 'ct')[:-1]))
        self.assert_integrity(rebuild(blob, nonce=raw(vault.seal(MASTER, PLAINTEXT), 'nonce')))
        with self.assertRaises(VaultError) as caught:
            vault.open(bytes(32), blob)
        self.assertEqual(caught.exception.code, 'integrity')

    def test_malformed_blobs_fail_format(self):
        blob = vault.seal(MASTER, PLAINTEXT)
        data = fields(blob)
        cases = [dict(data, v=2), dict(data, v=True), dict(data, alg='aes-256-gcm'),
                 {key: value for key, value in data.items() if key != 'tag'}, dict(data, ct='not base64!'),
                 dict(data, nonce=base64.b64encode(b'short').decode())]
        blobs = [json.dumps(case).encode() for case in cases] + [b'not json', b'[1]', b'\xff\xfe']
        for candidate in blobs:
            with self.assertRaises(VaultError) as caught:
                vault.open(MASTER, candidate)
            self.assertEqual(caught.exception.code, 'format')

    def test_oversized_blob_is_refused_before_work(self):
        with self.assertRaises(VaultError) as caught:
            vault.open(MASTER, b' ' * (1024 * 1024 + 1))
        self.assertEqual(caught.exception.code, 'too_large')

    def test_error_carries_only_its_code(self):
        error = VaultError('integrity')
        self.assertIsInstance(error, RuntimeError)
        self.assertEqual(str(error), 'vault error: integrity')


class KeyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.key_path = self.root / 'state' / 'envelope' / 'master.key'

    def tearDown(self):
        self.temp.cleanup()

    def key_file(self, content, mode=0o600):
        path = self.root / 'external.key'
        path.write_bytes(content)
        path.chmod(mode)
        return path

    def assert_code(self, code, *args):
        with self.assertRaises(VaultError) as caught:
            resolve_master_key(*args)
        self.assertEqual(caught.exception.code, code)

    def test_explicit_key_file_accepts_hex_and_raw_bytes(self):
        self.assertEqual(resolve_master_key(self.root / 'state', self.key_file(MASTER.hex().encode() + b'\n')), MASTER)
        self.assertEqual(resolve_master_key(self.root / 'state', self.key_file(MASTER)), MASTER)
        self.assertFalse(self.key_path.exists())

    def test_explicit_key_file_problems_are_key_invalid(self):
        self.assert_code('key_invalid', self.root / 'state', self.root / 'missing.key')
        self.assert_code('key_invalid', self.root / 'state', self.key_file(b'abc'))
        self.assert_code('key_invalid', self.root / 'state', self.key_file(MASTER.hex().encode(), 0o640))

    def test_generation_creates_private_directory_and_key_once(self):
        first = resolve_master_key(self.root / 'state')
        self.assertEqual(len(first), 32)
        self.assertEqual(self.key_path.parent.stat().st_mode & 0o777, 0o700)
        self.assertEqual(self.key_path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.key_path.read_text(), first.hex())
        self.assertEqual(resolve_master_key(self.root / 'state'), first)
        self.assertEqual([path.name for path in self.key_path.parent.iterdir()], ['master.key'])

    def test_malformed_existing_key_is_refused_and_kept(self):
        self.key_path.parent.mkdir(parents=True, mode=0o700)
        self.key_path.write_bytes(b'not a key')
        self.assert_code('key_invalid', self.root / 'state')
        self.assertEqual(self.key_path.read_bytes(), b'not a key')

    def test_existing_vault_without_key_is_key_missing(self):
        self.key_path.parent.mkdir(parents=True, mode=0o700)
        (self.key_path.parent / 'vault.enc').write_bytes(b'{}')
        self.assert_code('key_missing', self.root / 'state')
        self.assertFalse(self.key_path.exists())

    def test_leftover_key_temporary_from_a_crash_is_replaced_by_a_complete_key(self):
        self.key_path.parent.mkdir(parents=True, mode=0o700)
        stale = self.key_path.parent / '.master-crash.tmp'
        stale.write_bytes(b'0123')
        master = resolve_master_key(self.root / 'state')
        self.assertFalse(stale.exists())
        self.assertEqual(self.key_path.read_text(), master.hex())

    def test_key_written_by_a_concurrent_generator_wins(self):
        other = bytes(range(1, 33))
        real_link = os.link

        def racing_link(source, target):
            Path(target).write_text(other.hex())
            return real_link(source, target)

        with patch('coordinator.vault.os.link', side_effect=racing_link):
            self.assertEqual(resolve_master_key(self.root / 'state'), other)
        self.assertEqual([path.name for path in self.key_path.parent.iterdir()], ['master.key'])


class FileVaultTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.state = Path(self.temp.name) / 'state'
        self.vault = FileVault(self.state)

    def tearDown(self):
        envelope = self.state / 'envelope'
        if envelope.exists():
            envelope.chmod(0o700)
        self.temp.cleanup()

    def assert_code(self, code, call, *args):
        with self.assertRaises(VaultError) as caught:
            call(*args)
        self.assertEqual(caught.exception.code, code)

    def test_construction_touches_nothing(self):
        FileVault(self.state)
        self.assertFalse(self.state.exists())

    def test_put_get_and_file_protection(self):
        length, fingerprint = self.vault.put('GITHUB_TOKEN', VALUE)
        self.assertEqual((length, fingerprint), (len(VALUE), self.vault.fingerprint(VALUE)))
        self.assertEqual(self.vault.get('GITHUB_TOKEN'), VALUE)
        self.assertEqual(FileVault(self.state).get('GITHUB_TOKEN'), VALUE)
        self.assertEqual(self.vault.path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(sorted(path.name for path in self.vault.directory.iterdir()), ['master.key', 'vault.enc'])
        data = self.vault.path.read_bytes()
        for form in (VALUE.encode(), VALUE.encode().hex().encode(), base64.b64encode(VALUE.encode())):
            self.assertNotIn(form, data)

    def test_each_write_reseals_with_a_new_nonce(self):
        self.vault.put('A_TOKEN', VALUE)
        first = fields(self.vault.path.read_bytes())['nonce']
        self.vault.put('B_TOKEN', VALUE)
        self.assertNotEqual(fields(self.vault.path.read_bytes())['nonce'], first)

    def test_delete_is_idempotent_and_names_are_sorted(self):
        self.vault.put('B_TOKEN', VALUE)
        self.vault.put('A_TOKEN', VALUE)
        self.assertEqual(self.vault.names(), ['A_TOKEN', 'B_TOKEN'])
        self.assertTrue(self.vault.delete('B_TOKEN'))
        self.assertFalse(self.vault.delete('B_TOKEN'))
        master = self.vault.master()
        self.assertEqual(json.loads(vault.open(master, self.vault.path.read_bytes())), {'names': {'A_TOKEN': VALUE}})
        self.assert_code('missing', self.vault.get, 'B_TOKEN')

    def test_readback_mismatch_is_raised(self):
        self.vault.put('A_TOKEN', VALUE)
        loads = [{'A_TOKEN': VALUE}, {'A_TOKEN': 'something-else-entirely'}]
        with patch.object(FileVault, '_load', side_effect=lambda: dict(loads.pop(0))):
            self.assert_code('readback_mismatch', self.vault.put, 'A_TOKEN', VALUE)

    def test_write_failure_is_io_and_keeps_the_old_file(self):
        self.vault.put('A_TOKEN', VALUE)
        before = self.vault.path.read_bytes()
        self.vault.directory.chmod(0o500)
        self.assert_code('io', self.vault.put, 'B_TOKEN', VALUE)
        self.vault.directory.chmod(0o700)
        self.assertEqual(self.vault.path.read_bytes(), before)
        self.assertEqual(self.vault.names(), ['A_TOKEN'])

    def test_leftover_vault_temporary_from_a_crash_is_removed_on_the_next_write(self):
        self.vault.put('A_TOKEN', VALUE)
        stale = self.vault.directory / '.vault-crash.tmp'
        stale.write_bytes(b'partial')
        self.assertEqual(self.vault.get('A_TOKEN'), VALUE)
        self.vault.put('B_TOKEN', VALUE)
        self.assertFalse(stale.exists())
        self.assertEqual(self.vault.names(), ['A_TOKEN', 'B_TOKEN'])

    def test_bounds(self):
        self.assert_code('too_large', self.vault.put, 'A_TOKEN', 'x' * 4097)
        with patch.object(FileVault, '_load', return_value={'N%d' % index: VALUE for index in range(256)}):
            self.assert_code('too_large', self.vault.put, 'EXTRA_NAME', VALUE)
        self.vault._save({'N%d' % index: VALUE for index in range(257)})
        self.assert_code('too_large', self.vault.get, 'N1')

    def test_tampered_file_and_wrong_key_never_return_values(self):
        self.vault.put('A_TOKEN', VALUE)
        data = fields(self.vault.path.read_bytes())
        ciphertext = base64.b64decode(data['ct'])
        data['ct'] = base64.b64encode(flip(ciphertext, 3)).decode()
        original = self.vault.path.read_bytes()
        self.vault.path.write_text(json.dumps(data))
        self.assert_code('integrity', self.vault.get, 'A_TOKEN')
        self.vault.path.write_bytes(original)
        other = Path(self.temp.name) / 'other.key'
        other.write_text(bytes(32).hex())
        other.chmod(0o600)
        self.assert_code('integrity', FileVault(self.state, other).get, 'A_TOKEN')


class ProviderEnvironmentTests(unittest.TestCase):
    def test_vault_key_variables_never_reach_a_child(self):
        with tempfile.TemporaryDirectory() as temporary:
            from coordinator.accounts import AccountBroker
            from coordinator.store import Store
            store = Store(Path(temporary))
            profile = Path(temporary) / 'profile'
            profile.mkdir()
            store.put('accounts', {'profile': {'config_dir': str(profile), 'enabled': True}})
            runner = ProviderRunner(Path(temporary), account_broker=AccountBroker(store))
            with patch.dict(os.environ, {'TORII_VAULT_KEY': 'k', 'TORII_VAULT_KEY_FILE': '/k',
                                         'TORII_STATE_DIR': temporary}):
                env = runner._environment('claude', 'profile')
        self.assertNotIn('TORII_VAULT_KEY', env)
        self.assertNotIn('TORII_VAULT_KEY_FILE', env)
        self.assertEqual(env['TORII_STATE_DIR'], temporary)


class FakeVaultTests(unittest.TestCase):
    def test_fail_next_raises_once_and_calls_are_recorded(self):
        fake = FakeVault()
        fake.fail_next = 'io'
        with self.assertRaises(VaultError):
            fake.put('A_TOKEN', VALUE)
        self.assertEqual(fake.put('A_TOKEN', VALUE), (len(VALUE), vault.fingerprint(FakeVault.KEY, VALUE)))
        self.assertEqual(fake.get('A_TOKEN'), VALUE)
        self.assertTrue(fake.delete('A_TOKEN'))
        self.assertFalse(fake.delete('A_TOKEN'))
        self.assertEqual(fake.calls, [('put', 'A_TOKEN'), ('put', 'A_TOKEN'), ('get', 'A_TOKEN'),
                                      ('delete', 'A_TOKEN'), ('delete', 'A_TOKEN')])
