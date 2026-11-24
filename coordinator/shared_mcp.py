"""Copy explicitly shared Torii-home MCP entries into private launch configs."""

import fcntl
import asyncio
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time

from . import problems
from .scrub import Scrubber


def extend_scrubber(entries, scrub):
    values = {}
    for name, entry in entries.items():
        values[name + '-url'] = entry['url']
        values.update((name + '-header-' + str(index), value)
                      for index, value in enumerate(entry['headers'].values()) if value)
    scrub.extend(Scrubber(values).forms)


def codex_launch(entries):
    private = {}
    configs = {}
    for name, entry in entries.items():
        headers = {}
        for header, value in entry['headers'].items():
            key = 'TORII_MCP_HEADER_' + hashlib.sha256((name + '\x00' + header).encode()).hexdigest().upper()
            headers[header] = key
            private[key] = value
        configs[name] = {'url': entry['url'], 'env_http_headers': headers,
                         'default_tools_approval_mode': 'approve'}
    return configs, private


async def codex_inherited(binary, env, cwd, *, coordinator=False):
    """The names of the MCP servers that Codex loads from its own configuration. They stay on."""
    process = await asyncio.create_subprocess_exec(binary, 'mcp', 'list', '--json', cwd=str(cwd), env=env,
        stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        stdout, _ = await asyncio.wait_for(process.communicate(), 20)
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()
        raise RuntimeError('Could not inspect Codex MCP configuration.') from None
    except asyncio.CancelledError:
        process.kill()
        await process.wait()
        raise
    try:
        rows = json.loads(stdout)
        if process.returncode or not isinstance(rows, list) or any(
                not isinstance(row, dict) or not isinstance(row.get('name'), str) or not row['name'] for row in rows):
            raise ValueError()
        names = {row['name'] for row in rows}
    except (ValueError, UnicodeError):
        raise RuntimeError('Could not inspect Codex MCP configuration.') from None
    if coordinator and 'torii' in names:
        raise RuntimeError('Remove the torii MCP server from the shared Codex configuration before launch.')
    return names


def codex_selected(store, selected, inherited, topic=None):
    """The chat's servers that Codex can add to its own.

    A Codex override merges into a server of the same name and cannot replace it, so that server
    keeps its own configuration and Torii records a problem.
    """
    taken = sorted(set(selected) & set(inherited))
    if taken:
        problems.record(store, 'shared-mcp', 'codex-name-taken', 'entries=' + ','.join(taken), topic=topic)
    return {name: entry for name, entry in selected.items() if name not in taken}


def read_entries(store, scrub=None, *, topic=None, reserved_names=('torii',)):
    from .chat_mcp import selection
    names = selection(store, topic)
    if names is None:
        problems.record(store, 'shared-mcp', 'selection-invalid')
        return {}
    if not names:
        return {}
    source = Path.home() / '.claude.json'
    entries = {}
    try:
        document = json.loads(source.read_text())
    except FileNotFoundError:
        problems.record(store, 'shared-mcp', 'source-missing', 'entries=' + ','.join(names))
    except (OSError, UnicodeError):
        problems.record(store, 'shared-mcp', 'source-unreadable', 'entries=' + ','.join(names))
    except ValueError:
        problems.record(store, 'shared-mcp', 'source-invalid-json', 'entries=' + ','.join(names))
    else:
        servers = document.get('mcpServers') if isinstance(document, dict) else None
        for name in dict.fromkeys(names):
            if name in reserved_names:
                problems.record(store, 'shared-mcp', 'entry-reserved', 'entry=' + name)
                continue
            entry = servers.get(name) if isinstance(servers, dict) else None
            if entry is None:
                problems.record(store, 'shared-mcp', 'entry-missing', 'entry=' + name)
            elif (not isinstance(entry, dict) or entry.get('type') != 'http' or
                  not isinstance(entry.get('url'), str) or not entry['url'] or
                  not isinstance(entry.get('headers', {}), dict) or
                  not all(isinstance(key, str) and isinstance(value, str)
                          for key, value in entry.get('headers', {}).items())):
                problems.record(store, 'shared-mcp', 'entry-invalid', 'entry=' + name)
            else:
                entries[name] = dict(entry, headers=entry.get('headers', {}))
    if not entries:
        return {}
    if scrub is not None:
        extend_scrubber(entries, scrub)
    return entries


def write_config(store, scrub=None, *, topic=None):
    entries = read_entries(store, scrub, topic=topic)
    if not entries:
        return None
    content = json.dumps({'mcpServers': entries}, sort_keys=True, separators=(',', ':'))
    path = store.directory / ('shared-mcp-' + hashlib.sha256(content.encode()).hexdigest() + '.json')
    lock = store.directory / 'shared-mcp.lock'
    with os.fdopen(os.open(lock, os.O_CREAT | os.O_RDWR, 0o600), 'r+') as guard:
        fcntl.flock(guard, fcntl.LOCK_EX)
        if not path.exists():
            temporary = None
            try:
                with tempfile.NamedTemporaryFile('w', dir=store.directory, prefix='shared-mcp-',
                                                 suffix='.tmp', delete=False) as stream:
                    temporary = Path(stream.name)
                    os.chmod(temporary, 0o600)
                    stream.write(content)
                os.link(temporary, path)
            finally:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
        os.chmod(path, 0o600)
        os.utime(path, None)
        cutoff = time.time() - 3600
        for stale in (*store.directory.glob('shared-mcp-*.json'), store.directory / 'shared-mcp.json'):
            if stale != path and stale.exists() and stale.stat().st_mtime < cutoff:
                stale.unlink()
    return path
