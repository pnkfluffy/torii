"""Report or retire idle worktrees in explicitly approved Torii scratch roots."""

import argparse
import asyncio
from dataclasses import dataclass, replace
import json
import math
import os
from pathlib import Path
import re
import shutil
import sqlite3
import time

from . import workspaces
from .worktree_salvage import preserve_worktree, repositories, verify_preserved_worktree


DAY = 86400
PROTECTED_NAMES = frozenset(('main', 'master', 'develop', 'development', 'production', 'release'))


@dataclass(frozen=True)
class Policy:
    roots: tuple
    idle_days: float = 7
    apply: bool = False
    protected_names: frozenset = PROTECTED_NAMES
    root_seconds: float = 120
    total_seconds: float = 900
    reference_roots: tuple = ()

    def __post_init__(self):
        if not self.roots or any(not Path(root).is_absolute() for root in self.roots):
            raise ValueError('At least one absolute scratch root is required')
        if any(not math.isfinite(value) or value <= 0 for value in
               (self.idle_days, self.root_seconds, self.total_seconds)):
            raise ValueError('Age and time limits must be finite positive numbers')


def _within(path, root):
    return path == root or root in path.parents


def _safe_target(target, policy, state_dir):
    if not any(target != Path(root) and _within(target, Path(root)) for root in policy.roots):
        return 'outside approved scratch roots'
    if target.resolve() != target or not target.is_dir():
        return 'missing or linked worktree'
    if _within(target, Path(state_dir).resolve() / 'worktrees'):
        return 'managed job worktree'
    if _within(Path(state_dir).resolve(), target):
        return 'worktree contains coordinator state'
    for protected in (Path.home() / '.codex', Path.home() / '.claude', Path.home() / '.codex-accounts'):
        if _within(target, protected):
            return 'protected native history'
    if any(part in policy.protected_names for part in target.parts) or any(
            Path(name).is_absolute() and (_within(target, Path(name)) or _within(Path(name), target))
            for name in policy.protected_names):
        return 'protected name'
    return None


def _owner_state(db, target, branch, now, idle):
    owners = set()
    match = re.fullmatch(r'refs/heads/torii/task-(\d+)', branch)
    if match:
        owners.add(int(match[1]))
    last = 0
    for task in db.execute('SELECT id,worktree,status,updated FROM tasks'):
        if task['worktree'] and _within(Path(task['worktree']).resolve(), target):
            owners.add(task['id'])
    workers = list(db.execute('SELECT task,cwd,status,updated FROM workers'))
    for worker in workers:
        uses = worker['cwd'] and _within(Path(worker['cwd']).resolve(), target)
        if uses and worker['task'] is not None:
            owners.add(worker['task'])
    for worker in workers:
        uses = worker['cwd'] and _within(Path(worker['cwd']).resolve(), target)
        if uses or worker['task'] in owners:
            if worker['status'] not in ('done', 'interrupted'):
                return 'unfinished worker', sorted(owners)
            last = max(last, worker['updated'])
    for owner in owners:
        task = db.execute('SELECT status,updated FROM tasks WHERE id=?', (owner,)).fetchone()
        if task is not None:
            if task['status'] not in ('done', 'dropped'):
                return 'owning job is open', sorted(owners)
            last = max(last, task['updated'])
    if now - last < idle:
        return 'owner or worker is too recent', sorted(owners)
    return None, sorted(owners)


async def _process_paths():
    _, listing = await asyncio.wait_for(workspaces._command(
        '/usr/sbin/lsof', '-nP', '-Fpn', reject_stderr=True, strip=False), 30)
    if '\ufffd' in listing:
        raise ValueError('Process paths could not be decoded safely')
    return {Path(line[1:]).resolve() for line in listing.splitlines()
            if line.startswith('n/')}


async def _latest_write(target, git_dir):
    latest = 0
    for root in (target, git_dir):
        pending = [root]
        while pending:
            folder = pending.pop()
            latest = max(latest, folder.lstat().st_mtime)
            with os.scandir(folder) as entries:
                for index, entry in enumerate(entries):
                    info = entry.stat(follow_symlinks=False)
                    latest = max(latest, info.st_mtime)
                    if root == git_dir and entry.name.endswith('.lock'):
                        raise ValueError('Git lock is present')
                    if entry.is_dir(follow_symlinks=False):
                        pending.append(Path(entry.path))
                    if index % 256 == 0:
                        await asyncio.sleep(0)
            await asyncio.sleep(0)
    return latest


async def _level_a_reason(db, target, policy, now):
    tokens = [str(target)]
    if target.name not in ('repo', 'baseline', 'worktree', 'implementation'):
        tokens.append(target.name)
    for row in db.execute('SELECT notes,status,updated FROM tasks WHERE notes IS NOT NULL'):
        if row['status'] in ('done', 'dropped') and now - row['updated'] > 14 * DAY:
            continue
        if any(token in row['notes'] for token in tokens):
            return 'level A job note'
    if shutil.which('rg') is None:
        return await _reference_fallback(target, policy.reference_roots, tokens)
    patterns = [value for token in tokens for value in ('-e', token)]
    for root in policy.reference_roots:
        code, listing = await asyncio.wait_for(workspaces._command(
            'rg', '--files-with-matches', '--fixed-strings', '--hidden', '--no-ignore',
            '--glob=!.git', '--glob=!node_modules', '--glob=!.venv', '--glob=!__pycache__',
            *patterns, '--', root, check=False, reject_stderr=True), 30)
        if code not in (0, 1):
            raise ValueError('Level A reference search failed')
        if '\ufffd' in listing:
            raise ValueError('Level A paths could not be decoded safely')
        if any(not _within(Path(path).resolve(), target) for path in listing.splitlines()):
            return 'level A external reference'
    return None


async def _reference_fallback(target, roots, tokens):
    needles = [token.encode() for token in tokens]
    overlap = max(map(len, needles)) - 1
    for root in roots:
        pending = [Path(root)]
        while pending:
            folder = pending.pop()
            with os.scandir(folder) as entries:
                for entry in entries:
                    if entry.name in ('.git', 'node_modules', '.venv', '__pycache__'):
                        continue
                    path = Path(entry.path)
                    if entry.is_dir(follow_symlinks=False):
                        pending.append(path)
                    elif entry.is_file(follow_symlinks=False) and not _within(path.resolve(), target):
                        with path.open('rb') as stream:
                            tail = b''
                            while chunk := stream.read(65536):
                                data = tail + chunk
                                if any(needle in data for needle in needles):
                                    return 'level A external reference'
                                tail = data[-overlap:] if overlap else b''
                                await asyncio.sleep(0)
                    await asyncio.sleep(0)
    return None


async def _unsaved_reason(target):
    paths = await repositories(target)
    for repository in paths:
        _, ignored = await workspaces._git(repository, 'ls-files', '--others', '--ignored',
                                           '--exclude-standard', '-z', strip=False, timeout=30)
        if ignored:
            return 'ignored files need preservation'
        _, commits = await workspaces._git(repository, 'rev-list', '--reflog', '--not', '--all', timeout=30)
        if commits:
            return 'reflog-only commits need preservation'
    known = set(paths)
    pending = [target]
    while pending:
        folder = pending.pop()
        with os.scandir(folder) as entries:
            for entry in entries:
                if entry.name == '.git':
                    if folder not in known:
                        return 'nested repository needs preservation'
                elif entry.is_dir(follow_symlinks=False):
                    pending.append(Path(entry.path))
                await asyncio.sleep(0)
    return None


async def _guard(db, target, git_dir, record, policy, state_dir, now):
    reason = _safe_target(target, policy, state_dir)
    if reason:
        return reason, []
    reason, owners = _owner_state(db, target, record.get('branch', ''), now, policy.idle_days * DAY)
    if reason:
        return reason, owners
    if 'locked' in record or (git_dir / 'locked').exists():
        return 'worktree is locked', owners
    if record.get('branch', '').removeprefix('refs/heads/') in policy.protected_names:
        return 'protected branch', owners
    if workspaces._workspace_operation(git_dir):
        return 'Git operation or lock is present', owners
    for row in db.execute('SELECT image FROM outbox WHERE delivered=0 AND retired=0 AND image IS NOT NULL'):
        if _within(Path(row['image']).resolve(), target):
            return 'queued delivery uses worktree', owners
    reason = await _unsaved_reason(target)
    if reason:
        return reason, owners
    reason = await _level_a_reason(db, target, policy, now)
    if reason:
        return reason, owners
    if now - await _latest_write(target, git_dir) < policy.idle_days * DAY:
        return 'worktree is too recent', owners
    if any(_within(path, target) or _within(path, git_dir) for path in await _process_paths()):
        return 'live process uses worktree', owners
    return None, owners


async def _candidate(db, state_dir, repository, record, policy, now, result):
    target = Path(record['worktree'])
    reason = _safe_target(target, policy, state_dir)
    if reason:
        return dict(result, reason=reason)
    async with workspaces._repository_lock(state_dir, str(repository), wait=False) as acquired:
        if not acquired:
            return dict(result, reason='repository lock is held')
        git_dir = await workspaces._registered_git_directory(repository, target, timeout=30)
        _, actual_git = await workspaces._git(target, 'rev-parse', '--absolute-git-dir', timeout=30)
        _, actual_root = await workspaces._git(target, 'rev-parse', '--show-toplevel', timeout=30)
        if Path(actual_git).resolve() != git_dir or Path(actual_root).resolve() != target:
            return dict(result, reason='worktree registration does not match checkout')
        reason, owners = await _guard(db, target, git_dir, record, policy, state_dir, now)
        result['owners'] = owners
        if reason:
            return dict(result, reason=reason)
        if not policy.apply:
            return dict(result, state='eligible', reason='report only')
        original = target.stat()
        saved = await preserve_worktree(target, Path(state_dir) / 'results' / 'external-worktrees')
        result['salvage'] = str(saved)
        current_records = [item for item in await workspaces._worktree_records(repository, timeout=30)
                           if item['worktree'] == str(target)]
        if len(current_records) != 1 or current_records[0] != record:
            return dict(result, reason='worktree registration changed')
        reason, _ = await _guard(db, target, git_dir, record, policy, state_dir, now)
        current = target.stat()
        if reason or (current.st_dev, current.st_ino) != (original.st_dev, original.st_ino):
            return dict(result, reason=reason or 'worktree changed')
        await verify_preserved_worktree(target, saved)
        reason, _ = _owner_state(db, target, record.get('branch', ''), now, policy.idle_days * DAY)
        if reason or (git_dir / 'locked').exists() or workspaces._workspace_operation(git_dir):
            return dict(result, reason=reason or 'Git operation or lock changed')
        result['state'] = 'removing'
        try:
            await workspaces._git(repository, 'worktree', 'remove', '--force', '--', target, timeout=30)
        except (OSError, RuntimeError, asyncio.TimeoutError) as error:
            return dict(result, state='removal_failed', reason=str(error) or 'removal time limit exceeded')
        result['state'] = 'removed'
        return dict(result, state='removed', reason='saved worktree retired')


async def sweep(db, state_dir, repositories, policy, now=None):
    """Bound every candidate and report failures without weakening eligibility guards."""
    now = time.time() if now is None else now
    repositories = tuple(Path(repository).resolve(strict=True) for repository in repositories)
    if not policy.reference_roots:
        policy = replace(policy, reference_roots=repositories)
    deadline = time.monotonic() + policy.total_seconds
    report = {'apply': policy.apply, 'idle_days': policy.idle_days, 'roots': [], 'deadline_reached': False}
    seen = set()
    for repository in repositories:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            report['deadline_reached'] = True
            break
        try:
            records = await asyncio.wait_for(workspaces._worktree_records(repository, timeout=30), remaining)
        except (OSError, RuntimeError, asyncio.TimeoutError) as error:
            report['roots'].append({'repository': str(repository), 'state': 'kept', 'reason': str(error) or 'deadline'})
            continue
        for record in records:
            target = Path(record['worktree'])
            if str(target) in seen or not any(target != Path(root) and _within(target, Path(root))
                                              for root in policy.roots):
                continue
            seen.add(str(target))
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                report['deadline_reached'] = True
                break
            result = {'path': str(target), 'repository': str(repository), 'state': 'kept'}
            try:
                result = await asyncio.wait_for(_candidate(db, state_dir, repository, record, policy, now, result),
                                                min(policy.root_seconds, remaining))
            except (OSError, RuntimeError, ValueError, asyncio.TimeoutError) as error:
                if result['state'] == 'removing':
                    result['state'] = 'interrupted'
                result['reason'] = str(error) or 'time limit exceeded'
            report['roots'].append(result)
        if report['deadline_reached']:
            break
    report['deadline_reached'] |= time.monotonic() >= deadline
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir', type=Path, required=True)
    parser.add_argument('--repository', type=Path, action='append', required=True)
    parser.add_argument('--root', type=Path, action='append', required=True)
    parser.add_argument('--idle-days', type=float, default=7)
    parser.add_argument('--root-seconds', type=float, default=120)
    parser.add_argument('--total-seconds', type=float, default=900)
    parser.add_argument('--protected-names', type=Path)
    parser.add_argument('--reference-root', type=Path, action='append', default=[])
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    if any(_within(args.report.resolve(), root.resolve()) for root in args.root):
        parser.error('--report must be outside the scratch roots')
    if args.apply and not args.protected_names:
        parser.error('--apply requires --protected-names with the owner protection list')
    names = PROTECTED_NAMES
    if args.protected_names:
        names |= frozenset(line.split('\t')[0] for line in args.protected_names.read_text().splitlines() if line)
    policy = Policy(tuple(args.root), args.idle_days, args.apply, names, args.root_seconds, args.total_seconds,
                    tuple(args.reference_root))
    database = args.state_dir.resolve() / 'state.sqlite'
    with sqlite3.connect(database.as_uri() + '?mode=ro', uri=True) as db:
        db.row_factory = sqlite3.Row
        report = asyncio.run(sweep(db, args.state_dir, args.repository, policy))
    args.report.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    args.report.write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
