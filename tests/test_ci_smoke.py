import io
import json
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / 'scripts/ci-smoke.py'


class CiSmokeTests(unittest.TestCase):
    def test_fresh_cli_passes_and_saves_each_output(self):
        with tempfile.TemporaryDirectory() as directory:
            temp, out = Path(directory) / 'temp', Path(directory) / 'out'
            completed = subprocess.run([sys.executable, str(TOOL), '--temp', str(temp), '--out', str(out)],
                                       capture_output=True, text=True, timeout=300)
            self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
            self.assertIn('PASS', completed.stdout)
            for name in ('cli-help', 'cli-status', 'cli-status-repeat', 'cli-ctl-list'):
                self.assertTrue((out / (name + '.stdout.txt')).is_file(), name)
            observation = json.loads((out / 'state-observation.json').read_text())
            self.assertTrue(observation['database_created'])
            self.assertEqual(observation['rows']['settings'], 0)
            self.assertEqual(sum(observation['rows'].values()), 0)

    def test_unexpected_status_fails(self):
        smoke = runpy.run_path(str(TOOL))

        def run(name, args, env, out):
            if name == 'cli-help':
                return '--state-dir status'
            return json.dumps({'owner': 1})

        with tempfile.TemporaryDirectory() as directory, \
                patch.dict(smoke['main'].__globals__, run_command=run), \
                patch('sys.stdout', new_callable=io.StringIO) as printed:
            self.assertEqual(smoke['main'](['--temp', directory]), 1)
        self.assertIn('FAIL: Unexpected fresh CLI state', printed.getvalue())

    def test_a_failing_command_fails(self):
        smoke = runpy.run_path(str(TOOL))
        failed = subprocess.CompletedProcess([], 2, '', 'broken')
        with tempfile.TemporaryDirectory() as directory, patch('subprocess.run', return_value=failed), \
                patch('sys.stdout', new_callable=io.StringIO) as printed:
            self.assertEqual(smoke['main'](['--temp', directory, '--out', directory]), 1)
            self.assertEqual((Path(directory) / 'cli-help.stderr.txt').read_text(), 'broken')
        self.assertIn('FAIL: cli-help exited 2', printed.getvalue())


if __name__ == '__main__':
    unittest.main()
