"""Small injectable operating-system switch for host integration."""

import os
from pathlib import Path
import pwd
import sys

SYSTEM = sys.platform


def linux():
    return SYSTEM == 'linux'


def trash_directory():
    home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    return home / ('.local/share/Trash/files' if linux() else '.Trash')


def prepare_trash():
    if linux():
        directory = trash_directory()
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        directory.chmod(0o700)


def proc_text(name):
    try:
        return (Path('/proc') / name).read_text()
    except OSError:
        return ''
