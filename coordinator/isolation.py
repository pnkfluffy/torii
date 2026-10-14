"""Check the Codex CLI installation."""

import json
import os
from pathlib import Path
import platform
import re
import shutil
import sys


def codex_binary(store, default='codex'):
    return default


def codex_install_problem(binary):
    try:
        found = shutil.which(binary) if binary else None
        if not found:
            return None
        executable = Path(found).resolve()
        if executable.name == 'codex.js':
            root = executable.parent.parent
            try:
                version = json.loads((root / 'package.json').read_text()).get('version')
            except (FileNotFoundError, ValueError, AttributeError):
                version = None
            match = re.fullmatch(r'(\d+)\.(\d+)\.(\d+)(?:[-+].*)?', version) if isinstance(version, str) else None
            if match and tuple(map(int, match.groups())) < (0, 143, 0):
                return None
            architecture = {'arm64': 'aarch64', 'aarch64': 'aarch64',
                            'x86_64': 'x86_64', 'amd64': 'x86_64'}.get(platform.machine().lower())
            system = {'darwin': 'apple-darwin', 'linux': 'unknown-linux-musl'}.get(sys.platform)
            if not architecture or not system:
                return None
            target = architecture + '-' + system
            package = 'codex-' + ('darwin' if sys.platform == 'darwin' else 'linux') + '-' + (
                'arm64' if architecture == 'aarch64' else 'x64')
            vendors = [parent / 'node_modules' / '@openai' / package / 'vendor'
                       for parent in (root, *root.parents)] + [root / 'vendor']
            candidates = [vendor / target / 'bin' / 'codex' for vendor in vendors]
            executable = next((candidate for candidate in candidates if candidate.is_file()), candidates[0])
        else:
            if executable.name != 'codex':
                return None
            with executable.open('rb') as stream:
                magic = stream.read(4)
            if magic not in (b'\x7fELF', b'\xfe\xed\xfa\xce', b'\xce\xfa\xed\xfe',
                             b'\xfe\xed\xfa\xcf', b'\xcf\xfa\xed\xfe', b'\xca\xfe\xba\xbe',
                             b'\xbe\xba\xfe\xca', b'\xca\xfe\xba\xbf', b'\xbf\xba\xfe\xca'):
                return None
        helpers = [executable.parent.parent / 'codex-resources' / 'codex-code-mode-host',
                   executable.parent / 'codex-code-mode-host']
        if any(helper.is_file() and os.access(helper, os.X_OK) for helper in helpers):
            return None
    except OSError:
        return None
    return ('Codex code-mode tools cannot run because codex-code-mode-host is missing or not executable. '
            'Update or reinstall Codex 0.143.0 or newer the same way you installed it (for npm, without --omit=optional), then run setup again or restart Torii.')
