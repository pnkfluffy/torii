#!/usr/bin/env python3
"""Balance intact discovery suites and verify the tests recorded by all parts."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from importlib import import_module
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest


FAILED_IMPORT_PREFIX = 'unittest.loader._FailedTest.'
MODULE_SKIP_PREFIX = 'unittest.loader.ModuleSkipped.'


class ModuleLoadTest(unittest.TestCase):
    """Report named import outcomes with the same IDs as unittest discovery."""

    def __init__(self, module, error):
        super().__init__()
        prefix = MODULE_SKIP_PREFIX if isinstance(error, unittest.SkipTest) else FAILED_IMPORT_PREFIX
        self.name = prefix + module
        self.error = error

    def id(self):
        return self.name

    def __str__(self):
        return self.id()

    def runTest(self):
        raise self.error


def load_module(loader, name):
    try:
        module = import_module(name)
        return loader.loadTestsFromModule(module)
    except (Exception, SystemExit) as error:
        return loader.suiteClass([ModuleLoadTest(name, error)])


def discover(root, modules=()):
    root = str(Path(root).resolve())
    sys.path.insert(0, root)
    loader = unittest.TestLoader()
    if modules:
        return loader.suiteClass(load_module(loader, module) for module in modules)
    return loader.discover(root, pattern='test*.py', top_level_dir=root)


def module_of(name):
    for prefix in (FAILED_IMPORT_PREFIX, MODULE_SKIP_PREFIX):
        if name.startswith(prefix):
            return name[len(prefix):]
    return name.rsplit('.', 2)[0]


def test_ids(suite):
    if isinstance(suite, unittest.TestSuite):
        return [name for child in suite for name in test_ids(child)]
    return [suite.id()]


def individual_tests(suite):
    if isinstance(suite, unittest.TestSuite):
        return [test for child in suite for test in individual_tests(child)]
    return [suite]


def read_timings(path, suite):
    """Measured module seconds, or none when the timing file is absent, as in the public tree."""
    path = Path(path)
    return json.loads(path.read_text())[suite] if path.is_file() else {}


def plan(suite, parts, timings, split_modules=False):
    if parts < 1:
        raise ValueError('parts must be positive')
    groups = individual_tests(suite) if split_modules else list(suite)
    counts = Counter(module_of(name) for group in groups for name in test_ids(group))
    weights = []
    for group in groups:
        modules = {module_of(name) for name in test_ids(group)}
        weights.append(sum(timings.get(module, 1.0) / (counts[module] if split_modules else 1)
                           for module in modules) or 1.0)
    buckets = [[] for _ in range(parts)]
    loads = [0.0] * parts
    for index in sorted(range(len(groups)), key=lambda i: (-weights[i], i)):
        target = min(range(parts), key=lambda i: (loads[i], i))
        buckets[target].append(index)
        loads[target] += weights[index]
    return [unittest.TestSuite(groups[index] for index in sorted(bucket)) for bucket in buckets]


class RecordingResult(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ids = []
        self.failed = []

    def startTest(self, test):
        super().startTest(test)
        self.ids.append(test.id())

    def addError(self, test, err):
        super().addError(test, err)
        self.failed.append(test.id())

    def addFailure(self, test, err):
        super().addFailure(test, err)
        self.failed.append(test.id())

    def addSubTest(self, test, subtest, err):
        super().addSubTest(test, subtest, err)
        if err is not None:
            self.failed.append(test.id())

    def addUnexpectedSuccess(self, test):
        super().addUnexpectedSuccess(test)
        self.failed.append(test.id())


def run(args):
    suite = discover(args.root, args.modules)
    selected = plan(suite, args.parts, read_timings(args.timings, args.suite),
                    args.split_modules)[args.part - 1]
    runner = unittest.TextTestRunner(verbosity=2, resultclass=RecordingResult,
                                   warnings=None if sys.warnoptions else 'default')
    result = runner.run(selected)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({'part': args.part, 'parts': args.parts,
                                      'ids': result.ids, 'failed': sorted(set(result.failed))}, indent=2) + '\n')
    print('Part %d/%d: %d tests ran' % (args.part, args.parts, result.testsRun))
    return 0 if result.wasSuccessful() else 1


def signal_group(group, signum):
    try:
        os.killpg(group, signum)
        return True
    except ProcessLookupError:
        return False


def live_group_pids(process):
    process.poll()
    if not signal_group(process.pid, 0):
        return set()
    output = subprocess.check_output(['ps', '-axo', 'pid=,pgid=,stat='], text=True)
    return {int(pid) for pid, pgid, state in (line.split() for line in output.splitlines())
            if int(pgid) == process.pid and not state.startswith('Z')}


def stop_part(process):
    pids = live_group_pids(process)
    if not pids or not signal_group(process.pid, signal.SIGTERM):
        return []
    deadline = time.monotonic() + .5
    while True:
        time.sleep(.05)
        remaining = live_group_pids(process)
        if not remaining:
            return sorted(pids)
        if time.monotonic() >= deadline:
            break
    if signal_group(process.pid, signal.SIGKILL):
        pids.update(remaining)
    return sorted(pids)


def parallel(args):
    args.results.mkdir(parents=True, exist_ok=True)
    interrupted = None

    def interrupt(signum, frame):
        nonlocal interrupted
        if interrupted is None:
            interrupted = signum

    def run_part(part):
        output = args.results / ('part-%d.json' % part)
        command = [sys.executable, str(Path(__file__).resolve()), 'run',
                   '--root', str(args.root), '--suite', args.suite,
                   '--timings', str(args.timings), '--parts', str(args.parts),
                   '--part', str(part), '--output', str(output)]
        if args.modules:
            command += ['--modules', *args.modules]
        if args.split_modules:
            command.append('--split-modules')
        with tempfile.TemporaryDirectory(prefix='p%d' % part) as temporary:
            root = Path(temporary)
            paths = {'HOME': root / 'h', 'TMPDIR': root / 't',
                     'XDG_CONFIG_HOME': root / 'config', 'XDG_CACHE_HOME': root / 'cache',
                     'XDG_DATA_HOME': root / 'data', 'XDG_STATE_HOME': root / 'state',
                     'TORII_STATE_DIR': root / 'torii', 'CODEX_HOME': root / 'h/.codex',
                     'CLAUDE_CONFIG_DIR': root / 'h/.claude'}
            for path in paths.values():
                path.mkdir(mode=0o700, parents=True, exist_ok=True)
            environment = dict(os.environ, **{name: str(path) for name, path in paths.items()})
            environment.update(TEMP=str(paths['TMPDIR']), TMP=str(paths['TMPDIR']))
            with output.with_suffix('.log').open('w') as log, subprocess.Popen(
                    command, env=environment, stdout=log, stderr=subprocess.STDOUT,
                    start_new_session=True) as process:
                deadline = time.monotonic() + 1800
                try:
                    while interrupted is None and time.monotonic() < deadline:
                        try:
                            code = process.wait(timeout=.2)
                            break
                        except subprocess.TimeoutExpired:
                            pass
                    else:
                        code = 124 if interrupted is None else 128 + interrupted
                finally:
                    pids = stop_part(process)
        status = {'part': part, 'exit_code': code}
        if pids:
            status['warnings'] = [{'terminated_pids': pids}]
        return status

    previous = {}
    try:
        for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            previous[signum] = signal.signal(signum, interrupt)
        with ThreadPoolExecutor(max_workers=args.parts) as pool:
            statuses = list(pool.map(run_part, range(1, args.parts + 1)))
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
    if args.status_file:
        args.status_file.write_text(json.dumps(statuses, indent=2) + '\n')
    for part in range(1, args.parts + 1):
        print((args.results / ('part-%d.log' % part)).read_text(), end='')
    return 128 + interrupted if interrupted is not None else int(any(status['exit_code'] for status in statuses))


def check(args):
    expected = Counter(test_ids(discover(args.root, args.modules)))
    actual = Counter()
    parts = Counter()
    problems = []
    for path in sorted(args.results.glob('*.json')):
        try:
            record = json.loads(path.read_text())
            part = record['part']
            if type(part) is not int or not 1 <= part <= args.parts or record['parts'] != args.parts:
                raise ValueError('invalid part metadata')
            if not isinstance(record['ids'], list) or not all(isinstance(name, str) for name in record['ids']):
                raise ValueError('ids must be a list of strings')
            parts[part] += 1
            actual.update(record['ids'])
            print('Part %d/%d: %d recorded' % (part, args.parts, len(record['ids'])))
        except (OSError, ValueError, KeyError, TypeError) as error:
            problems.append('invalid result %s: %s' % (path.name, error))
    for part in range(1, args.parts + 1):
        if not parts[part]:
            problems.append('missing part: %d' % part)
        elif parts[part] > 1:
            problems.append('repeated part: %d' % part)
    for name in sorted(expected):
        if expected[name] > 1:
            problems.append('discovery repeats: ' + name)
        if not actual[name]:
            problems.append('missing: ' + name)
    for name in sorted(actual):
        if actual[name] > 1:
            problems.append('repeated: ' + name)
        if name not in expected:
            problems.append('extra: ' + name)
    recorded_modules = Counter(module_of(name) for name in actual.elements())
    for module in args.modules:
        count = recorded_modules[module]
        print('%s: %d recorded' % (module, count))
        if not count:
            problems.append('module did not run: ' + module)
    print('%d discovered; %d recorded' % (sum(expected.values()), sum(actual.values())), end='')
    if problems:
        print('; coverage FAILED')
        print('\n'.join(problems))
        return 1
    print('; every test ran exactly once')
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    running = commands.add_parser('run')
    concurrent = commands.add_parser('parallel')
    concurrent.add_argument('--status-file', type=Path)
    checking = commands.add_parser('check')
    for command in (running, concurrent, checking):
        command.add_argument('--root', type=Path, default=Path.cwd())
        command.add_argument('--parts', type=int, default=os.environ.get('TORII_TEST_PARTS'),
                             required='TORII_TEST_PARTS' not in os.environ)
        command.add_argument('--modules', nargs='+', default=[])
    running.add_argument('--part', type=int, required=True)
    for command in (running, concurrent):
        command.add_argument('--split-modules', action='store_true')
        command.add_argument('--suite', choices=('test', 'public'), required=True)
        command.add_argument('--timings', type=Path, default=Path(__file__).with_name('test-shard-timings.json'))
    running.add_argument('--output', type=Path, required=True)
    for command in (concurrent, checking):
        command.add_argument('--results', type=Path, required=True)
    args = parser.parse_args(argv)
    if args.parts < 1:
        parser.error('--parts must be positive')
    args.root = args.root.resolve()
    if args.command == 'run':
        args.output = args.output.resolve()
    else:
        args.results = args.results.resolve()
    if args.command != 'check':
        args.timings = args.timings.resolve()
    if args.command == 'parallel' and args.status_file:
        args.status_file = args.status_file.resolve()
    os.chdir(args.root)
    if args.command == 'run':
        if not 1 <= args.part <= args.parts:
            parser.error('--part must be between 1 and --parts')
        return run(args)
    if args.command == 'parallel':
        return parallel(args)
    return check(args)


if __name__ == '__main__':
    sys.exit(main())
