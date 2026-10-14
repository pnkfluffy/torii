"""Credential paths excluded from outbound attachments, without reading their contents."""

from dataclasses import dataclass
import os
import sys
from pathlib import Path


@dataclass(frozen=True)
class AttachmentPaths:
    directories: tuple
    files: tuple
    states: tuple

    def protected(self, path, metadata=None):
        path = Path(path).resolve()
        def normalized(value):
            value = str(value)
            return Path(value.casefold() if sys.platform == 'darwin' else value)
        candidate = normalized(path)
        if any(normalized(root) in (candidate, *candidate.parents) for root in self.directories):
            return True
        if candidate in tuple(normalized(file) for file in self.files):
            return True
        for state in self.states:
            state = normalized(state)
            if candidate.parent == state:
                if candidate.name.startswith('shared-mcp') and candidate.suffix in ('.json', '.tmp'):
                    return True
                if candidate.match('*-mcp-*.json'):
                    return True
            if candidate.name == 'spec.json' and state / 'hosts' in candidate.parents:
                return True
        if metadata is None:
            try:
                metadata = path.stat()
            except OSError:
                return False
        if metadata.st_nlink < 2:
            return False
        def same_file(file):
            try:
                info = file.stat()
            except OSError:
                return False
            return (info.st_dev, info.st_ino) == (metadata.st_dev, metadata.st_ino)
        if any(same_file(file) for file in self.files):
            return True
        for directory in (*self.directories, *(state / 'hosts' for state in self.states)):
            for root, _, names in os.walk(directory):
                for name in names:
                    file = Path(root) / name
                    if directory in self.directories or normalized(file).name == 'spec.json':
                        if same_file(file):
                            return True
        return False


def attachment_paths(store=None, token_file=None):
    """Snapshot registered paths on the store thread before a threaded upload."""
    from .accounts import LOCAL_OVERRIDES_PATH
    from .codex_accounts import default_home

    home = Path.home()
    states = [Path(os.environ.get('TORII_STATE_DIR') or home / '.local/state/telegram-agent-coordinator')]
    directories = [home / name for name in ('.claude', '.claude-accounts', '.codex', '.codex-accounts')]
    directories.append(default_home())
    files = [home / '.config/telegram-agent-coordinator/bot-token', home / '.claude.json', LOCAL_OVERRIDES_PATH]
    if token_file is not None:
        files.append(Path(token_file))
    if os.environ.get('TORII_VAULT_KEY_FILE'):
        files.append(Path(os.environ['TORII_VAULT_KEY_FILE']))
    if store is not None:
        states.append(store.directory)
        files.append(store.token_file)
        key_file = getattr(store.vault, '_key_file', None)
        if key_file is not None:
            files.append(Path(key_file))
        for setting in ('accounts', 'codex_accounts'):
            accounts = store.get(setting, {})
            if isinstance(accounts, dict):
                directories += [Path(profile['config_dir']) for profile in accounts.values()
                                if isinstance(profile, dict) and profile.get('config_dir')]
    directories += [state / 'envelope' for state in states]
    for state in states:
        if state.is_dir():
            for file in state.iterdir():
                name = file.name.casefold() if sys.platform == 'darwin' else file.name
                if (name.startswith('shared-mcp') and name.endswith(('.json', '.tmp')) or
                        '-mcp-' in name and name.endswith('.json')):
                    files.append(file)
    return AttachmentPaths(tuple(path.resolve() for path in directories),
                           tuple(path.resolve() for path in files), tuple(state.resolve() for state in states))
