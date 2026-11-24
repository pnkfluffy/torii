import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import runpy


ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / 'scripts/ci-tests.py'
CI_TESTS = runpy.run_path(str(TOOL))
PASS = 'self.assertTrue(True)'


def case(body, name='test_example'):
    return ('import os,unittest\nfrom pathlib import Path\n'
            'class Fixture(unittest.TestCase):\n def %s(self):\n  %s\n' % (name, body))


class CiTestsTests(unittest.TestCase):
    def run_tool(self, sources, *options):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        for name, source in sources.items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(source)
        out = self.out = root / 'out'
        completed = subprocess.run([sys.executable, str(TOOL), '--root', str(root), '--out', str(out),
                                    '--parts', '2', *options],
                                   capture_output=True, text=True, timeout=60)
        self.assertTrue((out / 'result.json').is_file(),
                        'no result.json (exit %d)\n%s%s' % (completed.returncode, completed.stdout, completed.stderr))
        record = json.loads((out / 'result.json').read_text())
        return completed, record, {row['spec']: row for row in record['rows']}

    def test_passing_modules_give_passed_rows_and_legacy_files(self):
        completed, record, rows = self.run_tool({'test_alpha.py': case(PASS), 'test_beta.py': case(PASS)})
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertEqual(rows, {name: {'spec': name, 'result': 'passed', 'tests': []}
                                for name in ('test_alpha.py', 'test_beta.py')})
        self.assertEqual(record['infrastructure'], [])
        self.assertEqual(record['coverage_exit_code'], 0)
        self.assertEqual(record['test_count'], 2)
        self.assertEqual(record['discovered_modules'], ['test_alpha', 'test_beta'])
        self.assertEqual(len(record['parts']), 2)

    def test_a_failure_that_passes_on_its_rerun_is_flaky_and_still_fails_the_exit(self):
        body = ('marker = Path(__file__).with_suffix(".ran")\n  first = not marker.exists()\n'
                '  marker.touch()\n  self.assertFalse(first, "first run fails")')
        completed, record, rows = self.run_tool({'test_flaky.py': case(body), 'test_fine.py': case(PASS)})
        self.assertEqual(completed.returncode, 1, completed.stdout)
        self.assertEqual(rows['test_flaky.py'], {'spec': 'test_flaky.py', 'result': 'flaky',
                                                 'tests': ['test_flaky.Fixture.test_example']})
        self.assertEqual(rows['test_fine.py']['result'], 'passed')
        self.assertEqual(record['infrastructure'], [])

    def test_a_failure_on_both_runs_is_failed(self):
        completed, record, rows = self.run_tool({'test_red.py': case('self.fail("red")')})
        self.assertEqual(completed.returncode, 1)
        self.assertEqual(rows['test_red.py'], {'spec': 'test_red.py', 'result': 'failed',
                                               'tests': ['test_red.Fixture.test_example']})
        self.assertTrue((self.out / 'rerun/parts.json').is_file())

    def test_class_setup_and_import_errors_name_their_module(self):
        setup = ('import unittest\nclass Fixture(unittest.TestCase):\n'
                 ' @classmethod\n def setUpClass(cls): raise RuntimeError("setup")\n'
                 ' def test_one(self): pass\n')
        completed, record, rows = self.run_tool({'test_setup.py': setup, 'test_broken.py': 'import missing_name_x\n'})
        self.assertEqual(completed.returncode, 1)
        self.assertEqual(rows['test_setup.py']['result'], 'failed')
        self.assertEqual(rows['test_broken.py']['result'], 'failed')
        self.assertEqual(record['infrastructure'], ['exact-ID coverage check failed'])

    def check_import_rows(self, source, result, tests, infrastructure=()):
        sources = {'sample/__init__.py': '', 'sample/test_import.py': source,
                   'sample/test_fine.py': case(PASS)}
        for options in ((), ('--shard', '1/1'), ('--files', 'sample/test_import.py', 'sample/test_fine.py')):
            with self.subTest(options=options):
                completed, record, rows = self.run_tool(sources, *options)
                self.assertEqual(completed.returncode, 1, completed.stdout + completed.stderr)
                self.assertEqual(rows['sample/test_import.py'],
                                 {'spec': 'sample/test_import.py', 'result': result, 'tests': tests})
                self.assertEqual(rows['sample/test_fine.py']['result'], 'passed')
                self.assertEqual(record['coverage_exit_code'], 0, completed.stdout)
                self.assertEqual(record['test_count'], 2, completed.stdout)
                self.assertEqual(record['infrastructure'], list(infrastructure))
                self.assertEqual(set(rows), {'sample/test_import.py', 'sample/test_fine.py'})

    def test_system_exit_at_import_is_failed_in_each_mode(self):
        self.check_import_rows('import sys\nsys.exit(0)\n', 'failed',
                               ['unittest.loader._FailedTest.sample.test_import'])

    def test_syntax_error_at_import_is_failed_in_each_mode(self):
        self.check_import_rows('def bad syntax\n', 'failed',
                               ['unittest.loader._FailedTest.sample.test_import'])

    def test_module_skip_is_infra_under_the_real_module_in_each_mode(self):
        self.check_import_rows('import unittest\nraise unittest.SkipTest("fixture")\n', 'infra', [],
                               ['module skipped during import: sample/test_import.py'])

    def test_module_skip_only_fails_the_shard_that_selects_it(self):
        completed, record, rows = self.run_tool({'test_alpha.py': case(PASS),
                                                'test_zulu.py': 'import unittest\nraise unittest.SkipTest("fixture")\n'},
                                                '--shard', '1/2')
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertEqual(rows, {'test_alpha.py': {'spec': 'test_alpha.py', 'result': 'passed', 'tests': []}})
        self.assertEqual(record['discovered_modules'], ['test_alpha', 'test_zulu'])
        self.assertEqual(record['infrastructure'], [])

    def test_a_skip_inside_a_test_keeps_the_module_passed(self):
        completed, record, rows = self.run_tool({'test_skip.py': case('self.skipTest("fixture")')},
                                                '--files', 'test_skip.py')
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertEqual(rows['test_skip.py']['result'], 'passed')
        self.assertEqual(record['infrastructure'], [])

    def test_a_module_without_a_part_receipt_is_infra(self):
        completed, record, rows = self.run_tool({'test_gone.py': case('os._exit(0)'), 'test_fine.py': case(PASS)},
                                                '--files', 'test_gone.py', 'test_fine.py')
        self.assertEqual(completed.returncode, 1)
        self.assertEqual(rows['test_gone.py'], {'spec': 'test_gone.py', 'result': 'infra', 'tests': []})
        self.assertEqual(rows['test_fine.py']['result'], 'passed')
        self.assertIn('exact-ID coverage check failed', record['infrastructure'])
        self.assertTrue(any(reason.endswith('exited without a result') for reason in record['infrastructure']))

    def test_a_selected_module_without_tests_is_missing(self):
        completed, record, rows = self.run_tool({'test_empty.py': ''}, '--files', 'test_empty.py')
        self.assertEqual(completed.returncode, 1)
        self.assertEqual(rows['test_empty.py']['result'], 'missing')
        self.assertEqual(record['infrastructure'], ['exact-ID coverage check failed'])

    def test_files_select_only_the_named_modules(self):
        completed, record, rows = self.run_tool({'test_alpha.py': case(PASS), 'test_other.py': case('self.fail()')},
                                                '--files', 'test_alpha.py')
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertEqual(list(rows), ['test_alpha.py'])
        self.assertEqual(record['modules'], ['test_alpha'])

    def test_files_accept_empty_and_space_joined_values(self):
        sources = {'test_alpha.py': case(PASS), 'test_beta.py': case(PASS), 'test_other.py': case('self.fail()')}
        completed, record, rows = self.run_tool(sources, '--files', '', 'test_alpha.py test_beta.py')
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertEqual(sorted(rows), ['test_alpha.py', 'test_beta.py'])
        completed, record, rows = self.run_tool(sources, '--files', '')
        self.assertEqual(sorted(rows), sorted(sources))

    def test_shards_split_the_modules_completely_and_disjointly(self):
        sources = {'test_%s.py' % name: case(PASS) for name in 'abcde'}
        seen = []
        for number in (1, 2):
            completed, record, rows = self.run_tool(sources, '--shard', '%d/2' % number)
            self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
            self.assertEqual(record['shard'], '%d/2' % number)
            seen += list(rows)
        self.assertEqual(sorted(seen), sorted(sources))

    def test_parallel_parts_have_separate_runtime_state_and_overlap(self):
        source = ('import json,os,tempfile,time,unittest\nfrom pathlib import Path\n'
                  'class Fixture(unittest.TestCase):\n'
                  ' def test_isolated(self):\n'
                  '  roots=[str(Path.home()),tempfile.gettempdir()]+[os.environ[key] for key in '
                  '("XDG_CONFIG_HOME","XDG_CACHE_HOME","XDG_DATA_HOME","XDG_STATE_HOME",'
                  '"TORII_STATE_DIR","CODEX_HOME","CLAUDE_CONFIG_DIR")]\n'
                  '  self.assertTrue(all(Path(root).is_dir() for root in roots))\n'
                  '  self.assertEqual(os.environ["TEMP"], roots[1])\n'
                  '  self.assertEqual(os.environ["TMP"], roots[1])\n'
                  '  Path(__file__).with_suffix(".started").write_text(json.dumps(roots))\n'
                  '  deadline=time.monotonic()+5\n'
                  '  while len(list(Path(__file__).parent.glob("*.started"))) < 2:\n'
                  '   self.assertLess(time.monotonic(),deadline,"parts did not overlap")\n'
                  '   time.sleep(.01)\n')
        names = ['test_isolation_%d.py' % part for part in range(2)]
        completed, record, rows = self.run_tool(dict({name: source for name in names},
                                                     **{'test_unrelated.py': 'raise RuntimeError("unrelated")\n'}),
                                                '--files', *names)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        roots = [json.loads(path.read_text()) for path in self.out.parent.glob('*.started')]
        self.assertEqual(len({root for group in roots for root in group}), 18)
        self.assertTrue(all(group[0] != str(Path.home()) for group in roots))
        self.assertEqual(sorted(rows), names)
        self.assertIn('2 discovered; 2 recorded; every test ran exactly once', completed.stdout)

    def test_shard_plan_without_timings_weighs_modules_equally(self):
        parts = CI_TESTS['shard_modules'](['tests.test_' + name for name in 'abcd'], 2, {})
        self.assertEqual([len(part) for part in parts], [2, 2])

    def test_parts_default_to_the_environment(self):
        with patch.dict(os.environ, TORII_TEST_PARTS='3'):
            os.environ.pop('HEAVY_CPUS', None)
            self.assertEqual(CI_TESTS['default_parts'](), 3)

    def test_parts_follow_the_cores_heavy_granted(self):
        with patch.dict(os.environ, HEAVY_CPUS='2', TORII_TEST_PARTS='4'):
            self.assertEqual(CI_TESTS['default_parts'](), 2)
        with patch.dict(os.environ, HEAVY_CPUS='', TORII_TEST_PARTS='4'):
            self.assertEqual(CI_TESTS['default_parts'](), 4)
        with patch.dict(os.environ, HEAVY_CPUS='0', TORII_TEST_PARTS='x'):
            self.assertEqual(CI_TESTS['default_parts'](), min(8, os.cpu_count() or 1))

    def test_failure_owner_reads_subtests_class_setup_and_import_failures(self):
        owner = CI_TESTS['owner']
        modules = {'tests.test_a', 'tests.test_b'}
        self.assertEqual(owner('tests.test_a.Case.test_x', modules), 'tests.test_a')
        self.assertEqual(owner('tests.test_a.Case.test_x (n=1)', modules), 'tests.test_a')
        self.assertEqual(owner('setUpClass (tests.test_b.Case)', modules), 'tests.test_b')
        self.assertEqual(owner('setUpModule (tests.test_b)', modules), 'tests.test_b')
        self.assertEqual(owner('unittest.loader._FailedTest.tests.test_a', modules), 'tests.test_a')
        self.assertEqual(owner('unittest.loader._FailedTest.test_b', modules), 'tests.test_b')
        self.assertIsNone(owner('other.Case.test_x', modules))


if __name__ == '__main__':
    unittest.main()
