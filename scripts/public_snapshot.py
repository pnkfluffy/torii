#!/usr/bin/python3
"""Build a scanned, local, one-commit repository from an explicit file list."""

import argparse
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import sys
from datetime import datetime, timezone


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = 'scripts/public_snapshot_files.json'


def git(repo, *args, env=None):
    result = subprocess.run(['git', '-c', 'core.hooksPath=/dev/null', '-c', 'commit.gpgsign=false',
                             '-c', 'init.templateDir=', *args], cwd=repo, env=env,
                            capture_output=True, timeout=120)
    if result.returncode:
        raise RuntimeError('Local Git operation failed: ' + args[0])
    return result.stdout


def entries(repo, revision):
    tree = {}
    for record in git(repo, 'ls-tree', '-rz', '--full-tree', revision).split(b'\0'):
        if record:
            metadata, name = record.split(b'\t', 1)
            mode, kind, oid = metadata.decode('ascii').split()
            tree[name.decode('utf-8')] = (mode, kind, oid)
    return tree


def manifest_at(repo, revision, tree):
    if MANIFEST not in tree:
        raise ValueError('The selected commit has no snapshot allowlist.')
    manifest = json.loads(git(repo, 'cat-file', 'blob', tree[MANIFEST][2]))
    if set(manifest) != {'keep', 'omit'}:
        raise ValueError('The snapshot allowlist needs keep and omit mappings.')
    for group in manifest.values():
        if not isinstance(group, dict):
            raise ValueError('Each allowlist group must map file paths to reasons.')
        for name, reason in group.items():
            path = PurePosixPath(name)
            if (not name or path.is_absolute() or '..' in path.parts or '.git' in path.parts
                    or str(path) != name or not isinstance(reason, str) or not reason.strip()):
                raise ValueError('Unsafe allowlist entry.')
    if set(manifest['keep']) & set(manifest['omit']):
        raise ValueError('A file cannot be both kept and omitted.')
    if not manifest['keep'] or set(manifest['keep']) - set(tree):
        raise ValueError('A required allowlisted file is missing at this commit.')
    return manifest


def identifiers(path):
    lines = [line.strip().encode('utf-8').lower() for line in path.read_text(encoding='utf-8').splitlines()
             if line.strip()]
    if not lines:
        raise ValueError('The denylist must contain at least one nonblank literal identifier.')
    return lines


def deny_check(data, denied, location):
    if any(value in data.lower() for value in denied):
        raise ValueError('Owner identifier matched in ' + location + '.')


def scan(output):
    commands = [
        ['gitleaks', 'dir', '--redact=100', '--no-banner', '--no-color',
         '--ignore-gitleaks-allow', str(output)],
        ['trufflehog', 'filesystem', '--no-verification', '--no-update', '--fail',
         '--fail-on-scan-errors', '--log-level=-1', str(output)],
    ]
    env = dict(os.environ)
    for key in ('GITLEAKS_CONFIG', 'GITLEAKS_CONFIG_TOML'):
        env.pop(key, None)
    for command in commands:
        binary = shutil.which(command[0])
        if not binary:
            raise RuntimeError(command[0] + ' is required on PATH.')
        command[0] = binary
        result = subprocess.run(command, cwd=output, env=env, stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL, timeout=1800)
        if result.returncode:
            raise RuntimeError(Path(binary).name + ' rejected the export or could not scan it '
                               + '(exit ' + str(result.returncode) + '). Scanner output is suppressed.')


def build(repo, commit, output, denylist, author_name, author_email):
    denied = identifiers(denylist)
    deny_check((author_name + '\n' + author_email).encode('utf-8'), denied, 'commit identity')
    if not author_name.strip() or not author_email.strip() or any(
            character in author_name + author_email for character in '\n\r\x00<>'):
        raise ValueError('Supply a nonempty author name and email without control characters or angle brackets.')
    revision = git(repo, 'rev-parse', '--verify', '--end-of-options', commit + '^{commit}').decode().strip()
    tree = entries(repo, revision)
    manifest = manifest_at(repo, revision, tree)
    unclassified = sorted(set(tree) - set(manifest['keep']) - set(manifest['omit']))
    if unclassified:
        raise ValueError('Tracked files are not classified by the snapshot allowlist: '
                         + ', '.join(unclassified))
    selected = []
    for name in sorted(manifest['keep']):
        mode, kind, oid = tree[name]
        if kind != 'blob' or mode not in ('100644', '100755'):
            raise ValueError('Only regular files can be exported: ' + name)
        deny_check(name.encode('utf-8'), denied, 'export filename')
        selected.append((name, mode, oid))
    output = output.expanduser().absolute()
    output.mkdir(parents=True, exist_ok=False, mode=0o700)
    for name, mode, oid in selected:
        data = (revision[:7] + '\n').encode() if name == 'VERSION' else git(repo, 'cat-file', 'blob', oid)
        if name == MANIFEST:
            data = (json.dumps({'keep': manifest['keep'], 'omit': {}}, indent=2) + '\n').encode()
        deny_check(data, denied, name)
        target = output / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        target.chmod(0o755 if mode == '100755' else 0o644)
    scan(output)
    env = dict(os.environ)
    for key in tuple(env):
        if key.startswith('GIT_'):
            env.pop(key)
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S +0000')
    env.update(GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL=os.devnull,
               GIT_AUTHOR_NAME=author_name, GIT_AUTHOR_EMAIL=author_email,
               GIT_COMMITTER_NAME=author_name, GIT_COMMITTER_EMAIL=author_email,
               GIT_AUTHOR_DATE=timestamp, GIT_COMMITTER_DATE=timestamp)
    git(output, 'init', '--initial-branch=main', env=env)
    git(output, 'add', '--all', env=env)
    git(output, 'commit', '-m', 'Public source snapshot', env=env)
    head = git(output, 'rev-parse', 'HEAD', env=env).decode().strip()
    if git(output, 'rev-list', '--count', 'HEAD', env=env).strip() != b'1':
        raise RuntimeError('The export must have exactly one commit.')
    if git(output, 'remote', env=env).strip():
        raise RuntimeError('The export must have no remote.')
    return {'source_commit': revision, 'snapshot_commit': head, 'output': str(output),
            'scanners': {'gitleaks': 'passed', 'trufflehog': 'passed'},
            'keep': manifest['keep'],
            'omit': {name: manifest['omit'].get(name, 'Not explicitly allowlisted.')
                     for name in sorted(set(tree) - set(manifest['keep']))}}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=ROOT)
    parser.add_argument('--commit', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--denylist', type=Path, required=True)
    parser.add_argument('--author-name', required=True)
    parser.add_argument('--author-email', required=True)
    args = parser.parse_args(argv)
    os.umask(0o077)
    try:
        report = build(args.repo, args.commit, args.output, args.denylist, args.author_name, args.author_email)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError):
        print('Snapshot failed. No push occurred. Any partial output is preserved; use a fresh output folder.',
              file=sys.stderr)
        return 1
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    sys.exit(main())
