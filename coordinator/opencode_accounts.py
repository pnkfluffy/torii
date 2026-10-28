"""Owner-configured OpenCode accounts and stat-only readiness checks."""

import math
import os
from pathlib import Path
import stat
import subprocess
import time
from urllib.parse import urlsplit

from .control_api import Refused
from .controls import _ALIAS


PINNED_VERSION = '1.19.3'
MAX_KEY_BYTES = 16384


def accounts(store):
    return store.get('opencode_accounts', {}) or {}


def active(store):
    alias = store.get('opencode_active_account')
    return alias if alias in accounts(store) else None


def account_home(alias):
    if not isinstance(alias, str) or not _ALIAS.fullmatch(alias):
        raise Refused('Send a valid OpenCode account alias.')
    return Path.home() / '.opencode-accounts' / alias


def _base_url(value):
    try:
        parsed = urlsplit(value)
        allowed = parsed.scheme == 'https' or (parsed.scheme == 'http' and parsed.hostname in ('127.0.0.1', 'localhost'))
        if (not allowed or not parsed.hostname or parsed.username is not None or parsed.password is not None
                or parsed.query or parsed.fragment or '\\' in value or any(char.isspace() for char in value)):
            raise ValueError
        parsed.port
    except ValueError:
        raise Refused('Use an HTTPS base URL, or HTTP on 127.0.0.1 or localhost, without credentials, query or fragment.')
    return value


def op_account(store, ctx, alias, model, base_url=None, context=None, output=None, price_in=None,
               price_cached=None, price_out=None, small_model=None, provider=None):
    home = account_home(alias)
    subscription = provider == 'opencode-go'
    if provider is not None and not subscription:
        raise Refused('The optional provider must be opencode-go.')
    if subscription:
        from .controls import _MODEL
        if not _MODEL.fullmatch(model):
            raise Refused('Use an unprefixed OpenCode subscription model ID.')
        if any(value is not None for value in (base_url, context, output, price_in, price_cached, price_out, small_model)):
            raise Refused('An OpenCode subscription account takes only alias, provider and model.')
        if any(name != alias and profile.get('provider') == 'opencode-go' for name, profile in accounts(store).items()):
            raise Refused('Only one OpenCode subscription account is allowed. Update its existing alias.')
    else:
        if any(value is None for value in (base_url, context, output, price_in, price_cached, price_out)):
            raise Refused('A generic OpenCode account needs base_url, context, output and all three prices.')
        base_url = _base_url(base_url)
    for key in ('accounts', 'codex_accounts'):
        if alias in (store.get(key, {}) or {}):
            raise Refused('That alias belongs to another provider. Choose a different OpenCode account alias.')
    profiles = accounts(store)
    previous = profiles.get(alias, {})
    if previous and (previous.get('provider') == 'opencode-go') != subscription:
        raise Refused('Keep the existing OpenCode account provider. Use another alias for a generic host.')
    if not subscription and (type(context) is not int or type(output) is not int or context <= 0 or output <= 0):
        raise Refused('OpenCode context and output limits must be positive whole numbers.')
    prices = {'price_in': price_in, 'price_cached': price_cached, 'price_out': price_out}
    if not subscription and any(type(value) not in (int, float) or not math.isfinite(value) or value < 0 for value in prices.values()):
        raise Refused('OpenCode prices must be finite, non-negative numbers.')
    for folder in (home.parent, home):
        if folder.is_symlink() or (folder.exists() and not folder.is_dir()):
            raise Refused('The OpenCode account root and home must be real folders.')
    if home.exists() and any(entry.name != 'key' for entry in home.iterdir()):
        raise Refused('An OpenCode account folder may contain only its owner-written key file.')
    home.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    home.parent.chmod(0o700)
    home.mkdir(exist_ok=True, mode=0o700)
    home.chmod(0o700)
    if subscription:
        profiles[alias] = dict(provider='opencode-go', model=model, created=previous.get('created', time.time()))
    else:
        profiles[alias] = dict(base_url=base_url, model=model, small_model=small_model or model,
                           context=context, output=output, **prices, created=previous.get('created', time.time()))
    store.put('opencode_accounts', profiles)
    return 'OpenCode subscription account saved.' if subscription else f'OpenCode account {alias} saved.'


def op_use(store, ctx, alias):
    if alias not in accounts(store):
        raise Refused('Unknown OpenCode account. Configure it with opencode.account first.')
    store.put('opencode_active_account', alias)
    return ('OpenCode subscription selected for new work.' if accounts(store)[alias].get('provider') == 'opencode-go'
            else f'OpenCode account {alias} selected for new work.')


def check_key(alias):
    home = account_home(alias)
    if home.parent.is_symlink() or home.is_symlink():
        raise Refused('The OpenCode account root and home must be real folders.')
    try:
        info = (home / 'key').lstat()
    except OSError:
        raise Refused('The active OpenCode account needs an owner-written key file.')
    if not stat.S_ISREG(info.st_mode):
        raise Refused('The OpenCode key must be a regular file, not a link or folder.')
    if stat.S_IMODE(info.st_mode) != 0o600:
        raise Refused('The OpenCode key file must have mode 0600.')
    if info.st_uid != os.getuid():
        raise Refused('The OpenCode key file must be owned by this user.')
    if info.st_size <= 0:
        raise Refused('The OpenCode key file must be non-empty.')
    if info.st_size > MAX_KEY_BYTES:
        raise Refused('The OpenCode key file must be at most %d bytes.' % MAX_KEY_BYTES)


def _check_binary(binary):
    if not isinstance(binary, str):
        raise Refused(f'Set opencode_binary to the absolute path of the pinned OpenCode {PINNED_VERSION} binary.')
    path = Path(binary)
    if not path.is_absolute() or not path.is_file():
        raise Refused(f'Set opencode_binary to the absolute path of the pinned OpenCode {PINNED_VERSION} binary.')
    try:
        completed = subprocess.run([binary, '--version'], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                   stderr=subprocess.DEVNULL, text=True, timeout=10)
    except (OSError, subprocess.SubprocessError):
        raise Refused('The pinned OpenCode binary could not report its version.')
    if completed.returncode != 0 or completed.stdout not in (PINNED_VERSION, PINNED_VERSION + '\n'):
        raise Refused(f'OpenCode must report exactly version {PINNED_VERSION}.')


def op_binary(store, ctx, path):
    _check_binary(path)
    store.put('opencode_binary', path)
    return 'Pinned OpenCode binary path set.'


def binary(store):
    from .cli_identity import installation
    configured = store.get('opencode_binary')
    if not isinstance(configured, str) or not Path(configured).is_absolute():
        return configured
    try:
        executable = Path(configured).resolve()
        for ancestor in executable.parents:
            _, _, prefix = installation(configured, 'opencode-ai', ancestor.name)
            if prefix is None:
                continue
            prefix = Path(prefix)
            sibling = prefix.parent / PINNED_VERSION
            candidate = sibling / executable.relative_to(prefix)
            found, _, installed = installation(str(candidate), 'opencode-ai', PINNED_VERSION)
            return found if installed == str(sibling) else configured
    except (OSError, RuntimeError, ValueError):
        return configured
    return configured


_checked_binary = None


def check_ready(store):
    global _checked_binary
    executable = binary(store)
    try:
        info = Path(executable).stat()
        signature = (executable, info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
    except (OSError, TypeError, ValueError):
        signature = None
    if signature is None or signature != _checked_binary:
        _check_binary(executable)
        _checked_binary = signature
    alias = active(store)
    if alias is None:
        raise Refused('Select an existing OpenCode account with opencode.use first.')
    check_key(alias)


def owner_settings(store):
    alias = active(store)
    profile = accounts(store).get(alias, {})
    return dict(opencode_enabled=store.get('opencode_enabled', False) is True,
                opencode_binary=store.get('opencode_binary'),
                opencode_active_account=alias, opencode_model=profile.get('model'),
                opencode_base_url_host=urlsplit(profile['base_url']).hostname if profile.get('base_url') else None)
