#!/usr/bin/env python3
"""Run Torii's offline checks and preserve an isolated CLI proof."""

import json
import os
import re
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
from datetime import datetime, timezone


SERVICE_PYTHON = '/usr/bin/python3'


def interpreter():
    """Use TORII_PYTHON when set, otherwise default to /usr/bin/python3.

    The installer records its invoking interpreter in the LaunchAgent. Set
    TORII_PYTHON to that path when it differs from this default. This script's
    shebang interpreter does not select the interpreter used for the checks.
    """
    override = os.environ.get('TORII_PYTHON')
    if override:
        if os.access(override, os.X_OK):
            return override
        raise RuntimeError('TORII_PYTHON must name an executable Python interpreter.')
    if os.access(SERVICE_PYTHON, os.X_OK):
        return SERVICE_PYTHON
    raise RuntimeError(
        SERVICE_PYTHON + ' is missing. Set TORII_PYTHON to an executable '
        'Python 3.9+ interpreter and run this again.')


def main():
    os.umask(0o077)
    root = Path(__file__).resolve().parents[1]
    proof_root = Path.home() / '.local/state/agent-workflow/proofs/torii'
    proof_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ-')
    proof = Path(tempfile.mkdtemp(prefix=stamp, dir=proof_root))
    actions = []
    result = {'mode': 'offline', 'passed': False, 'no_comments': False, 'scratch_removed': False}

    def save(name, value):
        (proof / name).write_text(json.dumps(value, indent=2) + '\n')

    try:
        python = interpreter()
        result['python_executable'] = python
        with tempfile.TemporaryDirectory(prefix='torii-verify-') as temporary:
            scratch = Path(temporary)
            env = dict(os.environ, PYTHONPYCACHEPREFIX=str(scratch / 'bytecode'))

            def run(name, args):
                completed = subprocess.run(args, cwd=root, env=env, text=True,
                                           capture_output=True, timeout=1800)
                (proof / (name + '.stdout.txt')).write_text(completed.stdout)
                (proof / (name + '.stderr.txt')).write_text(completed.stderr)
                actions.append({'name': name, 'argv': args, 'exit_code': completed.returncode})
                save('actions.json', actions)
                if completed.returncode:
                    raise RuntimeError(name + ' failed; inspect its proof output')
                return completed.stdout

            result['revision'] = run('revision', ['git', 'rev-parse', 'HEAD']).strip()
            result['python'] = run('python', [python, '--version']).strip()
            run('worktree-status', ['git', 'status', '--short'])
            cli = [python, '-m', 'coordinator', '--state-dir', str(scratch / 'state'),
                   '--token-file', str(scratch / 'no-token')]
            help_text = run('cli-help', cli + ['--help'])
            if '--state-dir' not in help_text or 'status' not in help_text:
                raise RuntimeError('CLI help does not expose expected commands')
            expected = {'owner': None, 'group': None, 'topics': [], 'tasks': [], 'workers': []}
            for name in ('cli-status', 'cli-status-repeat'):
                if json.loads(run(name, cli + ['status'])) != expected:
                    raise RuntimeError('Unexpected fresh CLI state')
            operations = json.loads(run('cli-ctl-list', cli + ['ctl', 'list']))
            ids = {op['id'] for op in operations}
            if not {'settings.show', 'service.restart', 'tasks.create', 'workers.goal'}.issubset(ids):
                raise RuntimeError('The control manifest is missing expected operations')
            if any(op['kind'] not in ('read', 'write') for op in operations):
                raise RuntimeError('The control manifest is missing expected operations')
            database = scratch / 'state' / 'state.sqlite'
            with sqlite3.connect(database.as_uri() + '?mode=ro&immutable=1', uri=True) as db:
                counts = {table: db.execute('SELECT count(*) FROM ' + table).fetchone()[0]
                          for table in ('settings', 'topics', 'updates', 'outbox', 'attachments',
                                        'tasks', 'messages', 'workers', 'service_requests',
                                        'envelopes', 'envelope_events', 'envelope_effects')}
                legacy = [row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('jobs','audit','job_questions','worker_controls','control_calls','task_reactions','pending_replies')")]
            save('state-observation.json', {'database_created': database.exists(), 'rows': counts,
                                            'legacy_tables': legacy})
            if any(counts.values()) or legacy:
                raise RuntimeError('Fresh status command created unexpected data')
            run('no-comments', [python, 'scripts/check-no-comments.py'])
            result['no_comments'] = True
            run('unittest', [python, '-m', 'unittest', 'discover', '-v'])
            test_output = (proof / 'unittest.stderr.txt').read_text()
            match = re.search(r'Ran (\d+) tests?', test_output)
            if not match:
                raise RuntimeError('The unittest output did not report a test count')
            result['test_count'] = int(match.group(1))
            run('compile', [python, '-m', 'compileall', '-q', 'coordinator', 'scripts', 'tests'])
            run('diff-check', ['git', 'diff', '--check'])
            result['passed'] = True
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        result['error'] = str(error)
    finally:
        result['scratch_removed'] = 'scratch' in locals() and not scratch.exists()
        save('result.json', result)
        print('Proof: ' + str(proof))
        print('PASS (offline only)' if result['passed'] else 'FAIL: ' + result.get('error', 'unknown error'))
    return 0 if result['passed'] and result['scratch_removed'] else 1


if __name__ == '__main__':
    sys.exit(main())
