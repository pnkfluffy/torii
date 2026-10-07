#!/usr/bin/env python3
"""Create a pinned Torii release worktree and print the LaunchAgent change that would run it.

This script never edits the plist and never starts, stops, or signals the service.
"""

import argparse
import os
from pathlib import Path
import plistlib
import shlex
import subprocess
import sys


LABEL = 'local.telegram-agent-coordinator'
SERVICE_PYTHON = '/usr/bin/python3'


def git(cwd, *args):
    done = subprocess.run(['git', '-C', str(cwd), *args], capture_output=True, text=True)
    if done.returncode:
        raise SystemExit('git ' + ' '.join(args) + ' failed: ' + (done.stderr.strip() or done.stdout.strip()))
    return done.stdout.strip()


def read_plist(path):
    if not path.is_file():
        return {}
    with path.open('rb') as stream:
        return plistlib.load(stream)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('commit', help='Commit, tag, or branch to pin')
    parser.add_argument('--home', type=Path,
                        help='Development checkout the coordinator works in. Default: the TORII_HOME or '
                             'working directory the plist names now, else this checkout')
    parser.add_argument('--state-dir', type=Path,
                        default=Path(os.environ.get('TORII_STATE_DIR') or
                                     Path.home() / '.local/state/telegram-agent-coordinator'))
    parser.add_argument('--plist', type=Path, default=Path.home() / 'Library/LaunchAgents' / (LABEL + '.plist'))
    args = parser.parse_args(argv)

    current = read_plist(args.plist)
    environment = current.get('EnvironmentVariables') or {}
    home = (args.home or Path(environment.get('TORII_HOME') or current.get('WorkingDirectory') or
                              Path(__file__).resolve().parents[1])).expanduser().resolve()
    if not home.is_dir():
        raise SystemExit('The home checkout does not exist: ' + str(home))
    commit = git(home, 'rev-parse', '--verify', args.commit + '^{commit}')
    releases = args.state_dir.expanduser().resolve() / 'releases'
    target = releases / commit[:12]
    if target.exists():
        if git(target, 'rev-parse', 'HEAD') != commit:
            raise SystemExit('Release folder exists at another commit: ' + str(target))
        print('Release exists: ' + str(target))
    else:
        releases.mkdir(parents=True, exist_ok=True, mode=0o700)
        git(home, 'worktree', 'add', '--detach', str(target), commit)
        print('Release created: ' + str(target))
    arguments = current.get('ProgramArguments') or []
    python = arguments[0] if arguments and os.access(arguments[0], os.X_OK) else (
        SERVICE_PYTHON if os.access(SERVICE_PYTHON, os.X_OK) else sys.executable)
    for check in (['-m', 'compileall', '-q', 'coordinator'],
                  ['-c', 'import coordinator.__main__, coordinator.service, coordinator.mcp; '
                   'from coordinator import extension; extension.load()']):
        loaded = subprocess.run([python, *check], cwd=str(target), capture_output=True, text=True,
                                env=dict(os.environ, PYTHONDONTWRITEBYTECODE='1'))
        if loaded.returncode:
            raise SystemExit('The release does not load with ' + python + ': ' +
                             (loaded.stderr.strip() or loaded.stdout.strip() or 'no output'))

    domain = 'gui/%d' % os.getuid()
    plist = shlex.quote(str(args.plist))
    buddy = '/usr/libexec/PlistBuddy -c '
    home_action = 'Set' if 'TORII_HOME' in environment else 'Add'
    home_value = str(home) if home_action == 'Set' else 'string ' + str(home)
    print('Commit: ' + commit)
    print('Checked with: ' + python)
    print('Home checkout: ' + str(home))
    print('')
    print('LaunchAgent change (not applied):')
    print('  WorkingDirectory: %s -> %s' % (current.get('WorkingDirectory', '(unset)'), target))
    print('  EnvironmentVariables.TORII_HOME: %s -> %s' % (environment.get('TORII_HOME', '(unset)'), home))
    print('')
    print('To apply it, check workers.list first, then run:')
    print('  ' + buddy + shlex.quote('Set :WorkingDirectory ' + str(target)) + ' ' + plist)
    print('  ' + buddy + shlex.quote(home_action + ' :EnvironmentVariables:TORII_HOME ' + home_value) + ' ' + plist)
    print('  launchctl bootout ' + domain + '/' + LABEL)
    print('  launchctl bootstrap ' + domain + ' ' + plist)
    return 0


if __name__ == '__main__':
    sys.exit(main())
