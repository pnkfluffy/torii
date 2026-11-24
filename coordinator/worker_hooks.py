"""Install the worker secret-read hook without replacing account hooks."""

import asyncio
import fcntl
import hashlib
import json
import os
import re
from pathlib import Path
import shlex
import shutil
import stat
import sys
import tempfile
import time


def install(provider, home, state_dir, temp_dir=None):
    home = Path(home)
    home.mkdir(mode=0o700, parents=True, exist_ok=True)
    path = home / ('settings.json' if provider == 'claude' else 'hooks.json')
    if provider == 'codex':
        from .codex_accounts import default_home
        if home.resolve() in (default_home(), (Path(os.path.expanduser('~')) / '.codex').resolve()):
            raise RuntimeError('Codex worker hook requires a private account home')
    path = home.resolve() / path.name if provider == 'codex' else path.resolve()
    from .extension import available
    writes = available('docs')
    matcher = ('Bash|Read|Grep' + ('|Write' if writes else '') if provider == 'claude'
               else 'Bash|exec_command|unified_exec|shell|local_shell|shell_command' + ('|apply_patch' if writes else ''))
    command = (shlex.quote(sys.executable) + ' -I '
               + shlex.quote(str(Path(__file__).resolve().with_name('secret_guard.py'))) + ' --torii-worker-hook')
    hook = {'matcher': matcher, 'hooks': [{'type': 'command', 'command': command}]}
    lock_name = 'worker-hook-' + hashlib.sha256(os.fsencode(path)).hexdigest() + '.lock'
    with open(Path(state_dir) / lock_name, 'a+', encoding='utf-8') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            if provider == 'codex' and path.is_symlink():
                current = path.lstat()
                data, mode = {}, 0o600
                identity = (current.st_ino, current.st_mtime_ns, current.st_size)
            else:
                with path.open(encoding='utf-8') as source:
                    before = os.fstat(source.fileno())
                    data = json.load(source)
                    current = os.fstat(source.fileno())
                mode = stat.S_IMODE(current.st_mode)
                identity = (current.st_ino, current.st_mtime_ns, current.st_size)
                if identity != (before.st_ino, before.st_mtime_ns, before.st_size):
                    raise RuntimeError('Worker hook settings changed during installation')
        except FileNotFoundError:
            data, mode, identity = {}, 0o600, None
        if not isinstance(data, dict):
            raise ValueError('Worker hook settings must be a JSON object')
        hooks = data.setdefault('hooks', {})
        if not isinstance(hooks, dict):
            raise ValueError('Worker hooks must be a JSON object')
        entries = hooks.setdefault('PreToolUse', [])
        if not isinstance(entries, list):
            raise ValueError('Worker PreToolUse hooks must be an array')
        ours = [entry for entry in entries if isinstance(entry, dict)
                and isinstance(entry.get('hooks'), list)
                and len(entry['hooks']) == 1 and isinstance(entry['hooks'][0], dict)
                and entry['hooks'][0].get('type') == 'command'
                and str(entry['hooks'][0].get('command', '')).endswith(' --torii-worker-hook')]
        found = next((entry for entry in ours if entry.get('matcher') == matcher), ours[0] if ours else None)
        if (found is not None and len(ours) == 1 and found.get('matcher') == matcher
                and found['hooks'][0].get('command') == command):
            return path, matcher, command
        entries[:] = [entry for entry in entries if entry is found or not any(entry is item for item in ours)]
        if found is None:
            entries.append(hook)
        else:
            found['matcher'] = matcher
            found['hooks'][0]['command'] = command
        temp_dir = Path(temp_dir or tempfile.gettempdir())
        if temp_dir.stat().st_dev != path.parent.stat().st_dev:
            raise RuntimeError('Worker temporary folder is on another filesystem')
        temporary = None
        try:
            with tempfile.NamedTemporaryFile('w', encoding='utf-8', dir=temp_dir,
                                             prefix='.torii-hook-', delete=False) as stream:
                temporary = stream.name
                os.fchmod(stream.fileno(), mode)
                json.dump(data, stream, ensure_ascii=False, indent=2)
                stream.write('\n')
            try:
                current = path.lstat() if provider == 'codex' else path.stat()
                latest = (current.st_ino, current.st_mtime_ns, current.st_size)
            except FileNotFoundError:
                latest = None
            if latest != identity:
                raise RuntimeError('Worker hook settings changed during installation')
            os.replace(temporary, path)
        finally:
            if temporary is not None and os.path.exists(temporary):
                os.unlink(temporary)
        return path, matcher, command


_trust_cache = {}


async def codex_trust_override(binary, env, cwd, installed):
    """Trust only the installed guard definition for this Codex launch."""
    from .codex_accounts import AppServer
    path, matcher, command = installed
    binary_path = shutil.which(binary, path=env.get('PATH')) or binary
    binary_stat = os.stat(binary_path) if os.path.exists(binary_path) else None
    path_stat = path.stat()
    cache_key = (binary_path, binary_stat.st_mtime_ns if binary_stat else None, binary_stat.st_size if binary_stat else None,
                 str(path), path_stat.st_mtime_ns, path_stat.st_size, matcher, command)
    cached = _trust_cache.get(cache_key)
    if cached is not None:
        if isinstance(cached, tuple):
            if time.monotonic() - cached[0] < 300:
                raise RuntimeError('Codex secret hook trust check recently failed')
        else:
            return cached
    server = AppServer(binary, env)
    try:
        try:
            await asyncio.wait_for(server.start(cwd=str(cwd), experimental=True), 10)
            response = await asyncio.wait_for(server.request('hooks/list', {'cwds': [str(cwd)]}), 10)
        finally:
            await server.stop()
    except Exception:
        _trust_cache[cache_key] = (time.monotonic(),)
        raise
    try:
        groups = response.get('data', [])
        entries = [entry for group in groups if isinstance(group, dict)
                   for entry in group.get('hooks', []) if isinstance(entry, dict)
                   and entry.get('matcher') == matcher and entry.get('handlerType') == 'command'
                   and entry.get('command') == command
                   and entry.get('eventName') == 'preToolUse' and entry.get('enabled') is True
                   and isinstance(entry.get('sourcePath'), str)
                   and Path(entry['sourcePath']).resolve() == path]
        if len(entries) != 1:
            raise RuntimeError('Codex secret hook was not found or is disabled')
        entry = entries[0]
        key, digest = entry.get('key'), entry.get('currentHash')
        if not isinstance(key, str) or not re.fullmatch(r'sha256:[0-9a-f]{64}', str(digest)):
            raise RuntimeError('Codex secret hook trust identity is invalid')
        override = 'hooks.state={' + json.dumps(key, ensure_ascii=False) + '={trusted_hash=' + json.dumps(digest) + '}}'
    except Exception:
        _trust_cache[cache_key] = (time.monotonic(),)
        raise
    _trust_cache[cache_key] = override
    return override
