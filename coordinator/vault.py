"""Encrypted file vault for envelope values. Only the service process opens it.

Python 3.9's standard library has no AEAD, so one blob is sealed with HMAC-SHA256
in counter mode for encryption and HMAC-SHA256 over the nonce and ciphertext for
integrity (encrypt-then-MAC). Subkeys are derived from one 32-byte master key.
"""

import base64
import binascii
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import secrets
import tempfile

LABEL = b'torii-envelope-v1'
VERSION = 1
ALGORITHM = 'hmac-sha256-ctr+hmac-sha256'
NONCE_BYTES = 16
MAX_BLOB = 1024 * 1024
MAX_NAMES = 256
MAX_VALUE = 4096
HEX_KEY = re.compile(r'[0-9a-fA-F]{64}')


class VaultError(RuntimeError):
    """A fixed short code. The message never carries plaintext, ciphertext, or key material."""

    def __init__(self, code):
        self.code = code
        super().__init__('vault error: ' + code)


def derive(master, purpose):
    return hmac.new(master, LABEL + b'/' + purpose, hashlib.sha256).digest()


def _keystream(key, nonce, length):
    blocks = (hmac.new(key, nonce + index.to_bytes(8, 'big'), hashlib.sha256).digest()
              for index in range((length + 31) // 32))
    return b''.join(blocks)[:length]


def _xor(data, stream):
    return (int.from_bytes(data, 'big') ^ int.from_bytes(stream, 'big')).to_bytes(len(data), 'big')


def _tag(master, nonce, ciphertext):
    return hmac.new(derive(master, b'mac'), LABEL + nonce + ciphertext, hashlib.sha256).digest()


def _encode(data):
    return base64.b64encode(data).decode('ascii')


def seal(master, plaintext):
    nonce = secrets.token_bytes(NONCE_BYTES)
    ciphertext = _xor(plaintext, _keystream(derive(master, b'enc'), nonce, len(plaintext)))
    return json.dumps({'v': VERSION, 'alg': ALGORITHM, 'nonce': _encode(nonce), 'ct': _encode(ciphertext),
                       'tag': _encode(_tag(master, nonce, ciphertext))}, separators=(',', ':')).encode()


def open(master, blob):
    if len(blob) > MAX_BLOB:
        raise VaultError('too_large')
    try:
        data = json.loads(blob)
        if type(data.get('v')) is not int or data['v'] != VERSION or data.get('alg') != ALGORITHM:
            raise VaultError('format')
        nonce, ciphertext, tag = (base64.b64decode(data[field], validate=True) for field in ('nonce', 'ct', 'tag'))
    except (ValueError, TypeError, KeyError, AttributeError, binascii.Error):
        raise VaultError('format') from None
    if len(nonce) != NONCE_BYTES:
        raise VaultError('format')
    if not hmac.compare_digest(_tag(master, nonce, ciphertext), tag):
        raise VaultError('integrity')
    return _xor(ciphertext, _keystream(derive(master, b'enc'), nonce, len(ciphertext)))


def fingerprint(master, value):
    return hmac.new(derive(master, b'fingerprint'), value.encode(), hashlib.sha256).hexdigest()[:8]


def _parse_key(raw):
    if len(raw) == 32:
        return raw
    text = raw.strip()
    if HEX_KEY.fullmatch(text.decode('ascii', 'replace')):
        return bytes.fromhex(text.decode('ascii'))
    raise VaultError('key_invalid')


def _fsync_directory(directory):
    descriptor = os.open(str(directory), os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _remove_stale(directory, prefix):
    for stale in directory.glob(prefix + '*.tmp'):
        stale.unlink()


def _write_temporary(directory, prefix, data):
    descriptor, name = tempfile.mkstemp(prefix=prefix, suffix='.tmp', dir=str(directory))
    try:
        with os.fdopen(descriptor, 'wb') as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        os.unlink(name)
        raise
    return name


def resolve_master_key(state_dir, key_file=None):
    """An explicit key file, else `envelope/master.key`, generated once and never overwritten."""
    if key_file is not None:
        try:
            path = Path(key_file)
            if path.stat().st_mode & 0o077:
                raise VaultError('key_invalid')
            return _parse_key(path.read_bytes())
        except OSError:
            raise VaultError('key_invalid') from None
    directory = Path(state_dir) / 'envelope'
    path = directory / 'master.key'
    try:
        return _parse_key(path.read_bytes())
    except FileNotFoundError:
        pass
    except OSError:
        raise VaultError('key_invalid') from None
    if (directory / 'vault.enc').exists():
        raise VaultError('key_missing')
    try:
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        directory.chmod(0o700)
        _remove_stale(directory, '.master-')
        master = secrets.token_bytes(32)
        temporary = _write_temporary(directory, '.master-', master.hex().encode('ascii'))
        try:
            os.link(temporary, str(path))
        except FileExistsError:
            master = _parse_key(path.read_bytes())
        finally:
            os.unlink(temporary)
        _fsync_directory(directory)
    except OSError:
        raise VaultError('io') from None
    return master


class FileVault:
    """One sealed JSON map in `<state_dir>/envelope/vault.enc`, re-sealed with a fresh nonce on every write."""

    def __init__(self, state_dir, key_file=None):
        self.state_dir = Path(state_dir)
        self.directory = self.state_dir / 'envelope'
        self.path = self.directory / 'vault.enc'
        self._key_file = key_file
        self._master = None

    def master(self):
        if self._master is None:
            self._master = resolve_master_key(self.state_dir, self._key_file)
        return self._master

    def fingerprint(self, value):
        return fingerprint(self.master(), value)

    def _load(self):
        master = self.master()
        try:
            if self.path.stat().st_size > MAX_BLOB:
                raise VaultError('too_large')
            blob = self.path.read_bytes()
        except FileNotFoundError:
            return {}
        except OSError:
            raise VaultError('io') from None
        try:
            names = json.loads(open(master, blob))['names']
        except (ValueError, TypeError, KeyError):
            raise VaultError('format') from None
        if not isinstance(names, dict) or not all(
                isinstance(name, str) and isinstance(value, str) for name, value in names.items()):
            raise VaultError('format')
        if len(names) > MAX_NAMES or any(len(value) > MAX_VALUE for value in names.values()):
            raise VaultError('too_large')
        return names

    def _save(self, names):
        blob = seal(self.master(), json.dumps({'names': names}, sort_keys=True).encode())
        try:
            if not self.directory.is_dir():
                self.directory.mkdir(mode=0o700, parents=True)
                self.directory.chmod(0o700)
            _remove_stale(self.directory, '.vault-')
            temporary = _write_temporary(self.directory, '.vault-', blob)
            try:
                os.replace(temporary, str(self.path))
            except BaseException:
                os.unlink(temporary)
                raise
            _fsync_directory(self.directory)
        except OSError:
            raise VaultError('io') from None

    def put(self, name, value):
        """Store one value and return (length, fingerprint) after reading it back."""
        names = self._load()
        names[name] = value
        if len(names) > MAX_NAMES or len(value) > MAX_VALUE:
            raise VaultError('too_large')
        self._save(names)
        stored = self._load().get(name)
        expected = self.fingerprint(value)
        if stored is None or not hmac.compare_digest(self.fingerprint(stored), expected):
            raise VaultError('readback_mismatch')
        return len(value), expected

    def get(self, name):
        names = self._load()
        if name not in names:
            raise VaultError('missing')
        return names[name]

    def delete(self, name):
        names = self._load()
        if name not in names:
            return False
        del names[name]
        self._save(names)
        return True

    def names(self):
        return sorted(self._load())


def vault_from_environment(state_dir):
    """Build the service vault and remove the key variables so no child process inherits them."""
    key_file = os.environ.pop('TORII_VAULT_KEY_FILE', None)
    os.environ.pop('TORII_VAULT_KEY', None)
    return FileVault(state_dir, key_file)


class FakeVault:
    """An in-memory backend for tests. `fail_next` makes the next call raise that code."""

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
