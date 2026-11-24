import importlib.util
from pathlib import Path
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / 'scripts' / 'check-error-problems.py'
SPEC = importlib.util.spec_from_file_location('check_error_problems', SCRIPT)
LINT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(LINT)


class ErrorProblemLintTests(unittest.TestCase):
    def test_direct_and_unpaired_errors_are_findings(self):
        self.assertEqual(LINT.findings("logger.error('failure')\n"), [1])
        self.assertEqual(LINT.findings("try:\n    run()\nexcept Exception:\n    logger.exception('failure')\n"), [4])
        self.assertEqual(LINT.findings("logging.getLogger('x').error('failure')\n"), [1])

    def test_error_logging_uses_rate_limited_problem(self):
        unbounded = "try:\n    run()\nexcept Exception:\n    logger.error('failure')\n    problems.record(store, 'a', 'b')\n"
        bounded = unbounded.replace("'a', 'b')", "'a', 'b', every=3600)")
        self.assertEqual(LINT.findings(unbounded), [4])
        self.assertEqual(LINT.findings(bounded), [4])
        self.assertEqual(LINT.findings("problems.record(store, 'a', 'b', level=logging.ERROR)\n"), [1])
        self.assertEqual(LINT.findings("problems.record(store, 'a', 'b', level=logging.ERROR, every=60)\n"), [])
