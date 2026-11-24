#!/usr/bin/env python3
"""Run Torii test modules in isolated parts and write one result row per module.

The result file is OUT/result.json. Each row is {spec, result, tests}: spec is the
test file, result is passed, failed, flaky, missing or infra, and tests are the
failing test IDs. An infra row marks a module skipped at import or a module with
unrecorded tests after a part lost its result.
A module that fails runs once more; it is flaky when the second run passes.
The exit code ignores the second run, so a first-run failure still fails.
Part warnings list PIDs terminated during cleanup. They do not affect the verdict.
"""

import argparse
import importlib.util
from itertools import chain
import json
import os
from pathlib import Path
import re
import subprocess
import sys


TOOL = Path(__file__).with_name('test-shards.py')
TIMINGS = Path(__file__).with_name('test-shard-timings.json')


def load_tool():
    spec = importlib.util.spec_from_file_location('test_shards', TOOL)
    tool = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(tool)
    return tool


SHARDS = load_tool()


def parse_shard(value):
    match = re.fullmatch(r'([1-9][0-9]*)/([1-9][0-9]*)', value)
    if not match or int(match[1]) > int(match[2]):
        raise argparse.ArgumentTypeError('shard must be i/n with 1 <= i <= n')
    return int(match[1]), int(match[2])


def default_parts():
    for name in ('HEAVY_CPUS', 'TORII_TEST_PARTS'):
        if os.environ.get(name, '').isdigit() and int(os.environ[name]) > 0:
            return int(os.environ[name])
    return min(8, os.cpu_count() or 1)


def module_name(value):
    return value[:-3].replace('/', '.') if value.endswith('.py') else value


def spec_name(module):
    return module.replace('.', '/') + '.py'


def owner(name, modules):
    head, _, rest = name.partition(' (')
    inner = rest.rstrip(')')
    for candidate in (SHARDS.module_of(head), inner, inner.rsplit('.', 1)[0]):
        if candidate in modules:
            return candidate
        found = [module for module in modules if module.endswith('.' + candidate)]
        if len(found) == 1:
            return found[0]
    return None


def shard_modules(modules, count, timings):
    buckets = [[] for _ in range(count)]
    loads = [0.0] * count
    for module in sorted(modules, key=lambda name: (-max(timings.get(name, 1.0), 0.001), name)):
        target = min(range(count), key=lambda index: (loads[index], len(buckets[index]), index))
        buckets[target].append(module)
        loads[target] += max(timings.get(module, 1.0), 0.001)
    return [sorted(bucket) for bucket in buckets]


def run_parts(root, suite, folder, parts, modules, split):
    folder.mkdir(parents=True, exist_ok=True)
    selection = ['--modules', *modules] if modules else []
    common = ['--root', str(root), '--parts', str(parts), '--results', str(folder / 'ids')]
    completed = subprocess.run([sys.executable, str(TOOL), 'parallel', *common, '--suite', suite,
                                '--status-file', str(folder / 'parts.json'),
                                *(['--split-modules'] if split else []), *selection], cwd=root, check=False)
    checked = subprocess.run([sys.executable, str(TOOL), 'check', *common, *selection], cwd=root, check=False)
    try:
        statuses = json.loads((folder / 'parts.json').read_text())
    except (OSError, ValueError):
        statuses = []
    records = {}
    for path in sorted((folder / 'ids').glob('part-*.json')):
        try:
            record = json.loads(path.read_text())
            records[record['part']] = {'ids': list(record['ids']), 'failed': list(record.get('failed', []))}
        except (OSError, ValueError, KeyError, TypeError):
            continue
    return completed.returncode, checked.returncode, statuses, records


def failures_by_module(records, modules):
    found, unowned = {}, []
    for record in records.values():
        for name in record['failed']:
            module = owner(name, modules)
            if module is None:
                unowned.append(name)
            else:
                found.setdefault(module, set()).add(name)
    return found, unowned


def recorded_by_module(records):
    found = {}
    for record in records.values():
        for name in record['ids']:
            found.setdefault(SHARDS.module_of(name), set()).add(name)
    return found


def build_rows(planned, expected, first, second, parts, skipped):
    first_failed, _ = failures_by_module(first, set(planned))
    second_failed, _ = failures_by_module(second, set(first_failed))
    first_ran, second_ran = recorded_by_module(first), recorded_by_module(second)
    rows = []
    for module in planned:
        spec, failed = spec_name(module), first_failed.get(module)
        if failed:
            again = second_failed.get(module, set())
            if not again and expected[module] <= second_ran.get(module, set()):
                rows.append({'spec': spec, 'result': 'flaky', 'tests': sorted(failed)})
            else:
                rows.append({'spec': spec, 'result': 'failed', 'tests': sorted(failed | again)})
        elif module in skipped:
            rows.append({'spec': spec, 'result': 'infra', 'tests': []})
        elif not expected[module] or not expected[module] <= first_ran.get(module, set()):
            result = 'infra' if expected[module] and len(first) < parts else 'missing'
            rows.append({'spec': spec, 'result': result, 'tests': []})
        else:
            rows.append({'spec': spec, 'result': 'passed', 'tests': []})
    return rows


def infrastructure(statuses, records, unowned, coverage_exit):
    reasons = []
    for status in statuses:
        part, code = status['part'], status['exit_code']
        if code == 124:
            reasons.append('part %d timed out' % part)
        elif code and not records.get(part, {}).get('failed'):
            reasons.append('part %d exited %d without a failing test' % (part, code))
        elif part not in records:
            reasons.append('part %d exited without a result' % part)
    reasons += ['failure outside the planned modules: ' + name for name in sorted(unowned)]
    if coverage_exit:
        reasons.append('exact-ID coverage check failed')
    return reasons


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--files', nargs='*', default=[])
    parser.add_argument('--shard', type=parse_shard)
    parser.add_argument('--parts', type=int, default=None)
    parser.add_argument('--suite', choices=('test', 'public'), default='test')
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args(argv)
    parts = default_parts() if args.parts is None else args.parts
    if parts < 1:
        parser.error('--parts must be positive')
    root, out = args.root.resolve(), args.out.resolve()
    named = [module_name(name) for value in args.files for name in value.split()]
    ids = SHARDS.test_ids(SHARDS.discover(root, named))
    discovered = sorted({SHARDS.module_of(name) for name in ids})
    planned = sorted(set(named)) if named else discovered
    if args.shard:
        timings = SHARDS.read_timings(TIMINGS, args.suite)
        planned = shard_modules(planned, args.shard[1], timings)[args.shard[0] - 1]
    expected = {module: set() for module in planned}
    for name in ids:
        module = SHARDS.module_of(name)
        if module in expected:
            expected[module].add(name)
    record = {'suite': args.suite, 'shard': '%d/%d' % args.shard if args.shard else None, 'files': args.files,
              'discovered_modules': discovered, 'discovered_tests': len(ids), 'modules': planned}
    out.mkdir(parents=True, exist_ok=True)
    selected = planned if named or args.shard else []
    split = bool(selected)
    if planned:
        run_exit, coverage_exit, statuses, first = run_parts(root, args.suite, out, parts, selected, split)
    else:
        run_exit, coverage_exit, statuses, first = 0, 0, [], {}
    failed, unowned = failures_by_module(first, set(planned))
    second = {}
    if failed:
        again = sorted(failed)
        second = run_parts(root, args.suite, out / 'rerun', min(parts, len(again)), again, True)[3]
    observed = chain(ids, *(part['ids'] for part in first.values()))
    skipped = {SHARDS.module_of(name) for name in observed if name.startswith(SHARDS.MODULE_SKIP_PREFIX)}
    skipped.intersection_update(planned)
    rows = build_rows(planned, expected, first, second, parts, skipped)
    reasons = infrastructure(statuses, first, unowned, coverage_exit)
    reasons.extend('module skipped during import: ' + spec_name(module) for module in sorted(skipped))
    record.update(parts=statuses, coverage_exit_code=coverage_exit,
                  test_count=sum(len(part['ids']) for part in first.values()),
                  rows=rows, infrastructure=reasons)
    (out / 'result.json').write_text(json.dumps(record, indent=2) + '\n')
    for status in statuses:
        print('Part %d/%d exit %d' % (status['part'], parts, status['exit_code']), flush=True)
        for warning in status.get('warnings', []):
            print('Warning: part %d stopped leftover test PIDs %s' %
                  (status['part'], ', '.join(map(str, warning['terminated_pids']))), flush=True)
    counts = {}
    for row in rows:
        counts[row['result']] = counts.get(row['result'], 0) + 1
    print('Rows: ' + (', '.join('%d %s' % (count, name) for name, count in sorted(counts.items())) or 'none'),
          flush=True)
    return 0 if run_exit == coverage_exit == 0 and not counts.get('infra') else 1


if __name__ == '__main__':
    sys.exit(main())
