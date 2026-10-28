"""One stdlib logging setup for the service: a rotating private file plus stderr.

A record carries operational identifiers such as topic, worker, pid, op, status,
and error code. A log record may include a filtered owner-supplied restart reason.
The filter removes selected token, email, and URL shapes but does not remove every
secret. Handlers attach to the `coordinator` logger, so
every module keeps using `logging.getLogger(__name__)` and inherits them.
"""

import hashlib
import json
import logging
import logging.handlers
import os
from pathlib import Path
import re
import subprocess

FORMAT = '%(asctime)s.%(msecs)03d %(levelname)s %(name)s %(message)s'
DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'
MAX_BYTES = 5 * 1024 * 1024
BACKUPS = 5
ROOT = 'coordinator'

_configured = None


def logger(name):
    return logging.getLogger(name)


def running_commit(project_root):
    """The commit this process runs, best effort. A non-Git checkout is not an error."""
    try:
        done = subprocess.run(['git', 'rev-parse', '--short', 'HEAD'], cwd=str(project_root),
                              stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=10)
    except (OSError, subprocess.SubprocessError):
        done = None
    if done is not None and not done.returncode:
        return done.stdout.decode(errors='replace').strip() or 'unknown'
    try:
        version = (Path(project_root) / 'VERSION').read_text().strip()
    except OSError:
        return 'unknown'
    return version if re.fullmatch(r'[0-9a-f]{7,40}', version) else 'unknown'


def code_stamp(project_root):
    def scan_error(error):
        raise error

    root = Path(project_root)
    files = []
    try:
        for directory, _, names in os.walk(root / 'coordinator', onerror=scan_error):
            for name in names:
                if name.endswith('.py'):
                    path = Path(directory) / name
                    metadata = path.stat()
                    files.append((path.relative_to(root).as_posix(), metadata.st_mtime_ns, metadata.st_size))
    except OSError:
        return None
    return hashlib.sha256(json.dumps(sorted(files), separators=(',', ':')).encode()).hexdigest()


def configure(state_dir, level=logging.INFO, stderr=True):
    """Install the handlers once and return the log file path.

    Repeating the same state directory is a no-op, so a second serve() in one
    process cannot double every line. A different directory replaces them, which
    is what a test with its own temporary state needs.
    """
    global _configured
    directory = Path(state_dir).expanduser() / 'logs'
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = directory / 'service.log'
    if _configured == path:
        return path
    reset()
    formatter = logging.Formatter(FORMAT, DATE_FORMAT)
    rotating = logging.handlers.RotatingFileHandler(path, maxBytes=MAX_BYTES, backupCount=BACKUPS,
                                                    encoding='utf-8')
    rotating.setFormatter(formatter)
    handlers = [rotating]
    if stderr:
        handlers.append(logging.StreamHandler())
        handlers[-1].setFormatter(formatter)
    root = logging.getLogger(ROOT)
    for handler in handlers:
        root.addHandler(handler)
    root.setLevel(level)
    root.propagate = False
    path.chmod(0o600)
    _configured = path
    return path


def reset():
    """Drop the installed handlers. Tests call this so a removed temporary directory
    leaves no open file behind."""
    global _configured
    root = logging.getLogger(ROOT)
    for handler in list(root.handlers):
        if not isinstance(handler, logging.NullHandler):
            root.removeHandler(handler)
            handler.close()
    _configured = None
