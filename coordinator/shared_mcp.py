"""Copy explicitly shared Torii-home MCP entries into private launch configs."""

import fcntl
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time

from . import problems
from .scrub import Scrubber


def write_config(store, scrub=None):
    from .accounts import LOCAL_OVERRIDES_PATH

    try:
        overrides = json.loads(LOCAL_OVERRIDES_PATH.read_text())
        names = overrides.get('shared_mcp_servers', [])
        if not isinstance(names, list) or any(not isinstance(name, str) or not name for name in names):
            raise ValueError('invalid server names')
    except FileNotFoundError:
        return None
    except (OSError, UnicodeError, ValueError, AttributeError):
        problems.record(store, 'shared-mcp', 'selection-invalid')
        return None
    if not names:
        return None
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
            entry = servers.get(name) if isinstance(servers, dict) else None
            if entry is None:
                problems.record(store, 'shared-mcp', 'entry-missing', 'entry=' + name)
            elif (not isinstance(entry, dict) or entry.get('type') != 'http' or
                  not isinstance(entry.get('url'), str) or not entry['url'] or
                  not isinstance(entry.get('headers'), dict) or
                  not all(isinstance(key, str) and isinstance(value, str)
                          for key, value in entry['headers'].items())):
                problems.record(store, 'shared-mcp', 'entry-invalid', 'entry=' + name)
            else:
                entries[name] = entry
    if not entries:
        return None
    if scrub is not None:
        values = {}
        for name, entry in entries.items():
            values[name + '-url'] = entry['url']
            values.update((name + '-header-' + str(index), value)
                          for index, value in enumerate(entry['headers'].values()) if value)
        scrub.extend(Scrubber(values).forms)
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
