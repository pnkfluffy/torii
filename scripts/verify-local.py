#!/usr/bin/env python3
"""Run Torii's offline checks and preserve an isolated CLI proof."""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
from datetime import datetime, timezone


SERVICE_PYTHON = '/usr/bin/python3'


def stop_detached_children(pid):
    output = subprocess.check_output(['ps', '-axo', 'pid=,ppid=,pgid='], text=True)
    processes = [tuple(map(int, line.split())) for line in output.splitlines()]
    descendants = {pid}
    while True:
        found = {child for child, parent, _ in processes if parent in descendants}
        if found.issubset(descendants):
            break
        descendants.update(found)
    groups = {group for child, _, group in processes if child in descendants and child == group and child != pid}
    denied = None
    for group in groups:
        try:
            os.killpg(group, signal.SIGTERM)
        except ProcessLookupError:
            pass
        except PermissionError as error:
            denied = error
    if denied is not None:
        raise denied


def run_process(args, root, env, timeout=1800, grace=20):
    process = subprocess.Popen(args, cwd=root, env=env, text=True,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               start_new_session=True)
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        stop_error = None
        try:
            os.killpg(process.pid, signal.SIGSTOP)
            try:
                stop_detached_children(process.pid)
            except (OSError, ValueError, subprocess.SubprocessError) as error:
                stop_error = error
            os.killpg(process.pid, signal.SIGINT)
        except ProcessLookupError:
            pass
        finally:
            try:
                os.killpg(process.pid, signal.SIGCONT)
            except ProcessLookupError:
                pass
        try:
            process.communicate(timeout=grace)
        except subprocess.TimeoutExpired:
            pass
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            process.communicate(timeout=grace)
        except subprocess.TimeoutExpired as error:
            for stream in (process.stdout, process.stderr):
                if stream is not None:
                    stream.close()
            raise RuntimeError('Timed out collecting test process output after termination') from error
        if stop_error is not None:
            raise RuntimeError('Could not stop detached test processes') from stop_error
        raise
    return process.returncode, stdout, stderr


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


def load_ci_tests():
    spec = importlib.util.spec_from_file_location('ci_tests', Path(__file__).with_name('ci-tests.py'))
    tool = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(tool)
    return tool


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--shard', type=load_ci_tests().parse_shard)
    args = parser.parse_args(argv or [])
    os.umask(0o077)
    root = Path(__file__).resolve().parents[1]
    proof_root = Path(os.environ['TORII_VERIFY_PROOF_ROOT']) if os.environ.get('TORII_VERIFY_PROOF_ROOT') else Path.home() / '.local/state/agent-workflow/proofs/torii'
    proof_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ-')
    proof = Path(tempfile.mkdtemp(prefix=stamp, dir=proof_root))
    actions = []
    result = {'mode': 'offline', 'passed': False, 'scratch_removed': False}
    first = not args.shard or args.shard[0] == 1
    if first:
        result['no_comments'] = False

    def save(name, value):
        (proof / name).write_text(json.dumps(value, indent=2) + '\n')

    try:
        python = interpreter()
        result['python_executable'] = python
        with tempfile.TemporaryDirectory(prefix='v') as temporary:
            scratch = Path(temporary)
            env = dict(os.environ, PYTHONPYCACHEPREFIX=str(scratch / 'bytecode'), TMPDIR=str(scratch))

            def run(name, args):
                code, stdout, stderr = run_process(args, root, env)
                (proof / (name + '.stdout.txt')).write_text(stdout)
                (proof / (name + '.stderr.txt')).write_text(stderr)
                actions.append({'name': name, 'argv': args, 'exit_code': code})
                save('actions.json', actions)
                if code:
                    raise RuntimeError(name + ' failed; inspect its proof output')
                return stdout

            if first:
                result['revision'] = run('revision', ['git', 'rev-parse', 'HEAD']).strip()
                result['python'] = run('python', [python, '--version']).strip()
                run('worktree-status', ['git', 'status', '--short'])
                run('cli-smoke', [python, str(Path(__file__).with_name('ci-smoke.py')),
                                  '--temp', str(scratch), '--out', str(proof)])
                run('no-comments', [python, 'scripts/check-no-comments.py'])
                run('error-problems', [python, 'scripts/check-error-problems.py'])
                result['no_comments'] = True
            command = [python, str(Path(__file__).with_name('ci-tests.py')), '--out', str(proof / 'suite')]
            if args.shard:
                command += ['--shard', '%d/%d' % args.shard]
            try:
                try:
                    run('unittest', command)
                finally:
                    suite = proof / 'suite' / 'result.json'
                    record = json.loads(suite.read_text()) if suite.is_file() else {}
                    keys = ('discovered_modules', 'discovered_tests', 'shard', 'modules') if args.shard else ()
                    result.update({key: record[key] for key in keys + ('test_count',) if key in record})
            finally:
                if first:
                    run('test-hosts', [python, 'scripts/check-test-hosts.py', '--root', str(scratch)])
            if args.shard and not result.get('modules'):
                raise ValueError('shard has no test modules')
            if 'test_count' not in result:
                raise RuntimeError('The test runner did not report a test count')
            if first:
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
    sys.exit(main(sys.argv[1:]))
