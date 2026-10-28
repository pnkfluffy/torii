"""Durable source recovery records for retired Git worktrees."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import tempfile
import uuid


async def _git(repository, *args, **options):
    from .workspaces import _git as command
    options['env'] = dict(options.get('env') or os.environ, GIT_OPTIONAL_LOCKS='0')
    if args and args[0] == 'diff':
        args = (args[0], '--no-ext-diff', '--no-textconv', '--submodule=short', *args[1:])
    return await command(repository, '-c', 'diff.autoRefreshIndex=false', *args, **options)


async def repositories(target):
    from .workspaces import GIT_TIMEOUT
    target = Path(target).resolve(strict=True)
    _, listing = await _git(target, 'submodule', 'foreach', '--quiet', '--recursive',
                            'printf "%s\\0" "$PWD"', timeout=GIT_TIMEOUT, strip=False)
    if '\ufffd' in listing:
        raise ValueError('Submodule path cannot be decoded without loss')
    paths = [target]
    for name in listing.split('\0'):
        if name:
            path = Path(name).resolve(strict=True)
            if not path.is_relative_to(target) or path == target:
                raise ValueError('Submodule path is outside the worktree')
            paths.append(path)
    initialized = set(paths)
    for repository in paths:
        _, entries = await _git(repository, 'ls-files', '--stage', '-z', timeout=GIT_TIMEOUT, strip=False)
        if '\ufffd' in entries:
            raise ValueError('Submodule path cannot be decoded without loss')
        for entry in entries.split('\0'):
            if not entry.startswith('160000 '):
                continue
            _, separator, name = entry.partition('\t')
            if not separator:
                raise ValueError('Submodule index entry is invalid')
            path = _source(repository, name)
            if path.resolve() in initialized:
                continue
            if path.is_symlink() or path.exists() and (not path.is_dir() or next(path.iterdir(), None) is not None):
                raise ValueError('Uninitialized submodule folder contains unsaved files')
    active = set()
    for path in paths:
        from .workspaces import _resolved_git_dir
        _, name = await _git(path, 'rev-parse', '--git-dir', timeout=GIT_TIMEOUT)
        active.add(_resolved_git_dir(path, name))
    for git_dir in active:
        modules = git_dir / 'modules'
        if not modules.is_dir():
            continue
        for directory, folders, files in os.walk(modules, followlinks=False):
            folders[:] = [name for name in folders if name not in ('objects', 'refs', 'logs', 'hooks')]
            module = Path(directory)
            if 'HEAD' in files and (module / 'objects').is_dir() and module.resolve() not in active:
                raise ValueError('Uninitialized or orphan submodule Git data needs preservation')
    return paths


def _source(repository, name):
    path = repository / name
    if '..' in Path(name).parts or not path.parent.resolve().is_relative_to(repository):
        raise ValueError('Source path is outside the worktree')
    return path


async def _snapshot(repository):
    from .workspaces import GIT_TIMEOUT
    values = []
    for args in [('rev-parse', 'HEAD'), ('show-ref', '--head'), ('status', '--porcelain=v1', '-z', '--untracked-files=all',
                  '--ignore-submodules=none'), ('diff', '--binary', 'HEAD', '--'),
                 ('diff', '--binary', '--cached', 'HEAD', '--'), ('diff', '--binary', '--')]:
        _, value = await _git(repository, *args, timeout=GIT_TIMEOUT, strip=False,
                              env=dict(os.environ, GIT_OPTIONAL_LOCKS='0'))
        values.append(value)
    _, names = await _git(repository, 'ls-files', '--others', '--exclude-standard', '-z',
                          timeout=GIT_TIMEOUT, strip=False)
    if '\ufffd' in names:
        raise ValueError('Untracked paths cannot be decoded without loss')
    values.append(names)
    _, tracked = await _git(repository, 'ls-files', '-z', timeout=GIT_TIMEOUT, strip=False)
    if '\ufffd' in tracked:
        raise ValueError('Tracked paths cannot be decoded without loss')
    for name in tracked.split('\0'):
        if name:
            path = _source(repository, name)
            if path.is_symlink():
                values.append(os.readlink(path))
            elif path.is_file():
                values.append(hashlib.sha256(path.read_bytes()).hexdigest())
            elif not path.exists():
                values.append('missing')
    for name in names.split('\0'):
        if name:
            path = _source(repository, name)
            mode = path.lstat().st_mode
            if not stat.S_ISREG(mode) and not stat.S_ISLNK(mode):
                raise ValueError('Untracked source is not a regular file or symbolic link')
            values.append(os.readlink(path) if stat.S_ISLNK(mode) else hashlib.sha256(path.read_bytes()).hexdigest())
    return hashlib.sha256('\0'.join(values).encode()).hexdigest(), names


async def preserve_worktree(target, destination):
    """Save each initialized repository before retirement; fail closed on changes."""
    target = Path(target).resolve(strict=True)
    destination = Path(destination)
    with tempfile.TemporaryDirectory(prefix='torii-worktree-salvage-') as temporary:
        saved = Path(temporary) / ('salvage-' + uuid.uuid4().hex)
        saved.mkdir(mode=0o700)
        await _preserve_worktree(target, saved)
        destination.mkdir(parents=True, exist_ok=True, mode=0o700)
        published = destination / saved.name
        saved.rename(published)
        descriptor = os.open(str(destination), os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return published / 'manifest.json'


async def _preserve_worktree(target, saved):
    from .workspaces import GIT_TIMEOUT
    paths = await repositories(target)
    records = []
    snapshots = []
    for index, repository in enumerate(paths):
        from .workspaces import _resolved_git_dir, _workspace_operation
        _, git_name = await _git(repository, 'rev-parse', '--git-dir', timeout=GIT_TIMEOUT)
        git_dir = _resolved_git_dir(repository, git_name)
        if (git_dir / 'locked').exists() or _workspace_operation(git_dir):
            raise ValueError('Repository lock or Git operation blocks source preservation')
        _, flags = await _git(repository, 'ls-files', '-v', '-z', timeout=GIT_TIMEOUT, strip=False)
        if any(entry and (entry[0] == 'S' or entry[0].islower()) for entry in flags.split('\0')):
            raise ValueError('Repository has flags that can hide source edits')
        if repository != target:
            _, hidden = await _git(repository, 'rev-list', '--reflog', '--not', '--all', timeout=GIT_TIMEOUT)
            if hidden:
                raise ValueError('Submodule has reflog-only commits requiring preservation')
        before, names = await _snapshot(repository)
        folder = saved / str(index)
        folder.mkdir(mode=0o700)
        await _git(repository, 'diff', '--binary', '--output=' + str(folder / 'changes.patch'),
                   'HEAD', '--', timeout=GIT_TIMEOUT)
        await _git(repository, 'diff', '--binary', '--cached',
                   '--output=' + str(folder / 'staged.patch'), 'HEAD', '--', timeout=GIT_TIMEOUT)
        await _git(repository, 'diff', '--binary', '--output=' + str(folder / 'unstaged.patch'),
                   '--', timeout=GIT_TIMEOUT)
        (folder / 'untracked.list').write_bytes(os.fsencode(names))
        for name in names.split('\0'):
            if not name:
                continue
            source = _source(repository, name)
            if not source.is_relative_to(repository) or '..' in Path(name).parts:
                raise ValueError('Untracked path is outside the worktree')
            output = folder / 'untracked' / name
            output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            if source.is_symlink():
                output.symlink_to(os.readlink(source))
            else:
                shutil.copy2(source, output, follow_symlinks=False)
        _, unpublished = await _git(repository, 'rev-list', '--all', 'HEAD', '--not', '--remotes',
                                    timeout=GIT_TIMEOUT)
        if unpublished:
            await _git(repository, 'bundle', 'create', folder / 'commits.bundle', '--all', 'HEAD',
                       '--not', '--remotes', timeout=GIT_TIMEOUT)
            await _git(repository, 'bundle', 'verify', folder / 'commits.bundle', timeout=GIT_TIMEOUT)
        after, _ = await _snapshot(repository)
        if before != after:
            raise ValueError('Worktree changed while preserving source')
        snapshots.append((repository, before))
        records.append({'path': str(repository.relative_to(target)), 'snapshot': before,
                        'directory': str(index)})
    if await repositories(target) != paths:
        raise ValueError('Submodules changed while preserving source')
    for repository, before in snapshots:
        after, _ = await _snapshot(repository)
        if before != after:
            raise ValueError('Worktree changed while preserving source')
    for repository, entry in zip(paths, records):
        folder = saved / entry['directory']
        names = os.fsdecode((folder / 'untracked.list').read_bytes())
        for name in names.split('\0'):
            if not name:
                continue
            source = _source(repository, name)
            copy = folder / 'untracked' / name
            if source.is_symlink():
                same = copy.is_symlink() and os.readlink(source) == os.readlink(copy)
            else:
                same = (not copy.is_symlink() and
                        hashlib.sha256(source.read_bytes()).digest() == hashlib.sha256(copy.read_bytes()).digest())
            if not same:
                raise ValueError('Preserved untracked content differs from its source')
    manifest = saved / 'manifest.json'
    manifest.write_text(json.dumps({'target': str(target), 'repositories': records}, indent=2) + '\n')
    for path in saved.rglob('*'):
        if path.is_file() and not path.is_symlink():
            with path.open('rb') as handle:
                os.fsync(handle.fileno())
    directories = [path for path in saved.rglob('*') if path.is_dir() and not path.is_symlink()]
    for directory in [*reversed(directories), saved]:
        descriptor = os.open(str(directory), os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return manifest


async def verify_preserved_worktree(target, manifest):
    target = Path(target).resolve(strict=True)
    record = json.loads(Path(manifest).read_text())
    if record['target'] != str(target):
        raise ValueError('Preserved source belongs to another worktree')
    paths = await repositories(target)
    if [str(path.relative_to(target)) for path in paths] != [entry['path'] for entry in record['repositories']]:
        raise ValueError('Submodules changed after preserving source')
    for repository, entry in zip(paths, record['repositories']):
        snapshot, _ = await _snapshot(repository)
        if snapshot != entry['snapshot']:
            raise ValueError('Worktree changed after preserving source')
    return True
