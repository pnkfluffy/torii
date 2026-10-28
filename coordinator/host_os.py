"""Small injectable operating-system switch for host integration."""

import asyncio
import errno
import json
import os
from pathlib import Path
import pwd
import signal
import stat
import sys
import time

SYSTEM = sys.platform


def _deletion_call(operation, location, function, *args, **kwargs):
    try:
        return function(*args, **kwargs)
    except OSError as error:
        if not getattr(error, 'operation', None):
            error.operation = operation
            error.strerror = operation + ': ' + (error.strerror or str(error))
        error.filename = str(location)
        raise


def _deletion_directory(parent, name, flags, device=None, info=None, path=None):
    path = path if path is not None else name
    info = info if info is not None else _deletion_call(
        'stat', path, os.stat, name, dir_fd=parent, follow_symlinks=False)
    if not stat.S_ISDIR(info.st_mode):
        raise OSError(errno.ENOTDIR, 'Deletion requires a real directory', name)
    if device is not None and info.st_dev != device:
        raise OSError(errno.EXDEV, 'Deletion cannot cross filesystems')
    try:
        fd = _deletion_call('open', path, os.open, name, flags, dir_fd=parent)
    except PermissionError:
        _deletion_call('chmod', path, os.chmod, name, stat.S_IMODE(info.st_mode) | 0o700,
                       dir_fd=parent, follow_symlinks=False)
        fd = _deletion_call('open', path, os.open, name, flags, dir_fd=parent)
    try:
        current = _deletion_call('fstat', path, os.fstat, fd)
        if (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino):
            raise OSError('Directory changed during deletion')
        if current.st_mode & 0o700 != 0o700:
            _deletion_call('fchmod', path, os.fchmod, fd, stat.S_IMODE(current.st_mode) | 0o700)
        current = _deletion_call('fstat', path, os.fstat, fd)
        named = _deletion_call('stat', path, os.stat, name, dir_fd=parent, follow_symlinks=False)
        if any((item.st_dev, item.st_ino) != (info.st_dev, info.st_ino) for item in (current, named)):
            raise OSError('Directory changed during deletion')
        if device is not None and current.st_dev != device:
            raise OSError(errno.EXDEV, 'Deletion cannot cross filesystems')
        return fd
    except BaseException:
        os.close(fd)
        raise


async def delete_tree(path, *, progress=None, report=None, paced=False):
    """Delete an owned directory in cancellable chunks using no-follow directory descriptors."""
    path = Path(path)
    progress = progress if progress is not None else {'bytes': 0, 'files': 0}
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    parent = _deletion_call('open', path.parent, os.open, str(path.parent), flags)
    stack = []
    try:
        try:
            root = _deletion_call('open-directory', path, _deletion_directory, parent, path.name, flags, path=path)
        except FileNotFoundError:
            return progress
        try:
            device = _deletion_call('fstat', path, os.fstat, root).st_dev
            entries = _deletion_call('scandir', path, os.scandir, root)
        except BaseException:
            os.close(root)
            raise
        stack.append((root, entries, parent, path.name, path))
        operations, started = 0, time.monotonic()
        while stack:
            fd, entries, owner, name, current_path = stack[-1]
            entry = _deletion_call('scandir', current_path, next, entries, None)
            if entry is None:
                entries.close()
                _deletion_call('rmdir', current_path, os.rmdir, name, dir_fd=owner)
                os.close(fd)
                stack.pop()
            else:
                entry_path = current_path / entry.name
                info = _deletion_call('stat', entry_path, os.stat, entry.name, dir_fd=fd, follow_symlinks=False)
                if stat.S_ISDIR(info.st_mode):
                    child = _deletion_call('open-directory', entry_path, _deletion_directory, fd,
                                           entry.name, flags, device, info, path=entry_path)
                    try:
                        children = _deletion_call('scandir', entry_path, os.scandir, child)
                    except BaseException:
                        os.close(child)
                        raise
                    stack.append((child, children, fd, entry.name, entry_path))
                else:
                    _deletion_call('unlink', entry_path, os.unlink, entry.name, dir_fd=fd)
                    progress['bytes'] += info.st_size
                    progress['files'] += 1
            operations += 1
            if operations >= (1000 if paced else 32) or time.monotonic() - started >= (0.1 if paced else 0.004):
                if report:
                    report(progress)
                await asyncio.sleep(0.2 if paced else 0)
                operations, started = 0, time.monotonic()
        return progress
    finally:
        for fd, entries, owner, name, current_path in reversed(stack):
            entries.close()
            os.close(fd)
        os.close(parent)


def _delete_tree_command(path):
    return [sys.executable, str(Path(__file__).resolve()), str(path)]


async def _read_deletion(process, progress):
    process.stdin.write((json.dumps(progress) + '\n').encode())
    await process.stdin.drain()
    process.stdin.close()
    error = None
    async for line in process.stdout:
        result = json.loads(line)
        progress.update(result['removed'])
        if result.get('error'):
            error = result['error']
    stderr = await process.stderr.read()
    code = await process.wait()
    if error:
        failure = OSError(error.get('errno') or errno.EIO, error['reason'], error.get('filename'))
        failure.operation = error.get('operation')
        raise failure
    if code:
        raise OSError(stderr.decode(errors='replace').strip() or 'Deletion process exited: ' + str(code))


async def _stop_deletion(spawn, reader):
    try:
        process = await spawn
    except Exception:
        return
    if process.returncode is None:
        try:
            process.terminate()
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(process.wait(), 0.08)
        except asyncio.TimeoutError:
            try:
                process.kill()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(process.wait(), 0.08)
            except asyncio.TimeoutError:
                process._transport.close()
    if reader:
        try:
            await asyncio.wait_for(asyncio.shield(reader), 0.04)
        except (Exception, asyncio.CancelledError):
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)


async def delete_tree_background(path, *, progress=None, paced=False):
    """Keep filesystem stalls outside the caller's loop and bound child shutdown."""
    progress = progress if progress is not None else {'bytes': 0, 'files': 0}
    env = {key: os.environ[key] for key in ('PATH', 'HOME', 'TMPDIR', 'PYTHONPYCACHEPREFIX',
                                          'PYTHONDONTWRITEBYTECODE') if key in os.environ}
    if paced:
        env['TORII_DELETE_PACED'] = '1'
    spawn = asyncio.ensure_future(asyncio.create_subprocess_exec(
        *_delete_tree_command(path), env=env, stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, start_new_session=True))
    reader = None
    try:
        process = await asyncio.shield(spawn)
        reader = asyncio.create_task(_read_deletion(process, progress))
        await asyncio.shield(reader)
        return progress
    except BaseException:
        cleanup = asyncio.create_task(_stop_deletion(spawn, reader))
        while True:
            try:
                await asyncio.shield(cleanup)
                break
            except asyncio.CancelledError:
                if cleanup.cancelled():
                    raise
        raise


def _delete_tree_main():
    progress = json.loads(sys.stdin.readline())

    def emit(state, error=None):
        print(json.dumps({'state': state, 'removed': progress, 'error': error}), flush=True)

    async def run():
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, asyncio.current_task().cancel)
        try:
            await delete_tree(sys.argv[1], progress=progress, report=lambda value: emit('running'),
                              paced=os.environ.get('TORII_DELETE_PACED') == '1')
        except asyncio.CancelledError:
            emit('cancelled')
        except Exception as error:
            emit('failed', {'reason': getattr(error, 'strerror', None) or str(error),
                            'errno': getattr(error, 'errno', None),
                            'operation': getattr(error, 'operation', None),
                            'filename': getattr(error, 'filename', None)})
        else:
            emit('done')

    asyncio.run(run())


def linux():
    return SYSTEM == 'linux'


def trash_directory():
    home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    return home / ('.local/share/Trash/files' if linux() else '.Trash')


def move_to_trash(path, directory=None, *, name=None):
    """Rename a directory into this filesystem's Trash without replacing a namesake."""
    path = Path(path)
    source = path.lstat()
    if not stat.S_ISDIR(source.st_mode):
        raise OSError(errno.ENOTDIR, 'Trash source must be a directory', str(path))
    directory = Path(directory) if directory is not None else trash_directory()
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    if source.st_dev != directory.stat().st_dev:
        raise OSError(errno.EXDEV, 'Trash is on another filesystem', str(path))
    name = path.name if name is None else name
    target = directory / name
    suffix = 1
    while target.exists() or target.is_symlink():
        target = directory / (name + '-' + str(suffix))
        suffix += 1
    path.rename(target)
    return target


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


if __name__ == '__main__':
    _delete_tree_main()
