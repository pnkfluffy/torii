"""One stdlib logging setup for the service: a rotating private file plus stderr.

A record carries operational identifiers such as topic, worker, pid, op, status,
and error code. It never carries a token, credential path, settings value, or owner
message text beyond `preview`. Handlers attach to the `coordinator` logger, so
every module keeps using `logging.getLogger(__name__)` and inherits them.
"""

import logging
import logging.handlers
from pathlib import Path
import re
import subprocess

FORMAT = '%(asctime)s.%(msecs)03d %(levelname)s %(name)s %(message)s'
DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'
MAX_BYTES = 5 * 1024 * 1024
BACKUPS = 5
PREVIEW = 80

ROOT = 'coordinator'

_configured = None


def logger(name):
    return logging.getLogger(name)


def preview(text, limit=PREVIEW):
    """A short single-line excerpt. Owner text never reaches a handler in full."""
    if text is None:
        return ''
    flat = ' '.join(str(text).split())
    return flat if len(flat) <= limit else flat[:limit] + '...'


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
