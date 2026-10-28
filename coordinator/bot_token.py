"""Atomic private token storage for the terminal installers."""

import errno
import os
from pathlib import Path
import tempfile


def save_token(token: str, folder: Path, temporary_folder=None) -> Path:
    token = token.strip()
    if not token:
        raise ValueError("Empty token. Nothing saved.")
    if any(char.isspace() for char in token):
        raise ValueError("The token must not contain whitespace. Nothing saved.")
    if folder.is_symlink():
        raise ValueError("The configuration directory must not be a symbolic link.")
    folder.mkdir(parents=True, exist_ok=True, mode=0o700)
    folder.chmod(0o700)
    destination = folder / "bot-token"
    if destination.is_symlink():
        raise ValueError("The token file must not be a symbolic link.")
    fd, temporary_name = tempfile.mkstemp(prefix=".bot-token-", dir=temporary_folder or folder)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w") as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(token + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.replace(temporary, destination)
        except OSError as error:
            if error.errno != errno.EXDEV:
                raise
            local_fd, local_name = tempfile.mkstemp(prefix=".bot-token-", dir=folder)
            local_temporary = Path(local_name)
            try:
                with os.fdopen(local_fd, "w") as stream:
                    os.fchmod(stream.fileno(), 0o600)
                    stream.write(token + "\n")
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(local_temporary, destination)
            finally:
                local_temporary.unlink(missing_ok=True)
    finally:
        temporary.unlink(missing_ok=True)
    return destination
