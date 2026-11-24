#!/usr/bin/env python3
"""Check the Torii CLI against a fresh scratch state: help, status twice, ctl list and table counts."""

import argparse
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]
TABLES = ('settings', 'topics', 'updates', 'outbox', 'attachments', 'tasks', 'messages', 'workers',
          'service_requests', 'envelopes', 'envelope_events', 'envelope_effects')
OPERATIONS = {'settings.show', 'service.restart', 'tasks.create', 'workers.goal'}
FRESH = {'owner': None, 'group': None, 'topics': [], 'tasks': [], 'workers': []}


def run_command(name, args, env, out):
    completed = subprocess.run(args, cwd=ROOT, env=env, capture_output=True, text=True, timeout=300)
    if out:
        (out / (name + '.stdout.txt')).write_text(completed.stdout)
        (out / (name + '.stderr.txt')).write_text(completed.stderr)
    if completed.returncode:
        raise RuntimeError('%s exited %d; inspect its output' % (name, completed.returncode))
    return completed.stdout


def smoke(temp, out):
    state = Path(tempfile.mkdtemp(prefix='state-', dir=temp))
    env = dict(os.environ, TMPDIR=str(temp))
    cli = [sys.executable, '-m', 'coordinator', '--state-dir', str(state), '--token-file', str(temp / 'no-token')]
    help_text = run_command('cli-help', cli + ['--help'], env, out)
    if '--state-dir' not in help_text or 'status' not in help_text:
        raise RuntimeError('CLI help does not expose expected commands')
    for name in ('cli-status', 'cli-status-repeat'):
        if json.loads(run_command(name, cli + ['status'], env, out)) != FRESH:
            raise RuntimeError('Unexpected fresh CLI state')
    operations = json.loads(run_command('cli-ctl-list', cli + ['ctl', 'list'], env, out))
    if not OPERATIONS.issubset({op['id'] for op in operations}) or \
            any(op['kind'] not in ('read', 'write') for op in operations):
        raise RuntimeError('The control manifest is missing expected operations')
    database = state / 'state.sqlite'
    with sqlite3.connect(database.as_uri() + '?mode=ro&immutable=1', uri=True) as db:
        counts = {table: db.execute('SELECT count(*) FROM ' + table).fetchone()[0] for table in TABLES}
        settings = db.execute('SELECT key,value FROM settings').fetchall()
    if out:
        (out / 'state-observation.json').write_text(json.dumps(
            {'database_created': database.exists(), 'rows': counts}, indent=2) + '\n')
    if counts != dict.fromkeys(TABLES, 0) or settings:
        raise RuntimeError('Fresh status command created unexpected data')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--temp', type=Path, required=True)
    parser.add_argument('--out', type=Path)
    args = parser.parse_args(argv)
    temp = args.temp.resolve()
    temp.mkdir(parents=True, exist_ok=True)
    if args.out:
        args.out.mkdir(parents=True, exist_ok=True)
    try:
        smoke(temp, args.out)
    except (OSError, ValueError, RuntimeError, sqlite3.Error, subprocess.SubprocessError) as error:
        print('FAIL: ' + str(error), flush=True)
        return 1
    print('PASS: CLI help, status twice, ctl list and fresh state', flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
