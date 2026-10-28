"""Read CLI identity without launching a provider or touching account state."""

import json
from pathlib import Path
import shutil


PACKAGES = {'claude': '@anthropic-ai/claude-code', 'codex': '@openai/codex'}


def installation(binary, package, version=None):
    found = shutil.which(binary) if binary else None
    if not found:
        return None, None, None
    executable = Path(found).resolve()
    if version is not None:
        for prefix in executable.parents:
            if prefix.name != version:
                continue
            try:
                data = json.loads((prefix / 'node_modules' / package / 'package.json').read_text())
            except (OSError, ValueError):
                continue
            if (isinstance(data, dict) and data.get('name') == package and data.get('version') == version
                    and executable.relative_to(prefix).parts[0] == 'node_modules'):
                return str(Path(found).absolute()), version, str(prefix)
        return str(Path(found).absolute()), None, None
    for root in executable.parents:
        manifest = root / 'package.json'
        try:
            data = json.loads(manifest.read_text())
        except (OSError, ValueError):
            continue
        if not isinstance(data, dict) or data.get('name') != package:
            continue
        modules = root.parent.parent if package.startswith('@') else root.parent
        prefix = modules.parent
        if modules.name == 'node_modules' and (prefix / 'package.json').is_file():
            return str(Path(found).absolute()), data.get('version'), str(prefix)
        return str(Path(found).absolute()), data.get('version'), None
    return str(Path(found).absolute()), None, None


def identity(binary, package):
    found, version, prefix = installation(binary, package)
    if not found:
        return {'binary': binary, 'missing': True}
    path = Path(found).resolve()
    try:
        stat = path.stat()
        stamp = [stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns]
    except OSError:
        stamp = None
    return {'binary': str(path), 'version': version, 'stat': stamp}
