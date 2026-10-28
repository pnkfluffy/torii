import builtins
import importlib.util
import io
from pathlib import Path
import subprocess
import tempfile
import unittest
from contextlib import redirect_stdout


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('check_undefined_names', ROOT / 'scripts/check-undefined-names.py')
checker = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(checker)


class UndefinedNamesTests(unittest.TestCase):
    def check(self, source):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'sample.py'
            path.write_text(source)
            return [(file.name, line, name) for file, line, name in checker.findings([path])]

    def test_repository_has_no_undefined_names(self):
        found = ['{}:{}: {}'.format(checker.display(path), line, name)
                 for path, line, name in checker.findings()]
        self.assertEqual(found, [])

    def test_nested_function_reports_file_line_and_name(self):
        self.assertEqual(self.check('def outer():\n    def inner():\n        return misspeled\n'),
                         [('sample.py', 3, 'misspeled')])

    def test_enclosing_function_comprehension_and_lambda_bindings(self):
        self.assertEqual(self.check(
            'def outer(value):\n'
            '    def inner():\n'
            '        return value\n'
            '    return [item for item in range(2)], lambda arg: arg, inner\n'), [])

    def test_global_binding_in_another_function(self):
        self.assertEqual(self.check(
            'def install():\n    global shared\n    shared = 1\n'
            'def read():\n    return shared\n'), [])

    def test_conditional_and_import_error_bindings(self):
        self.assertEqual(self.check(
            'try:\n    import optional\nexcept ImportError:\n    optional = None\n'
            'if True:\n    conditional = 1\n'
            'def read():\n    return optional, conditional\n'), [])

    def test_class_body_bindings_are_not_visible_to_methods(self):
        self.assertEqual(self.check(
            'class Sample:\n    value = 1\n    copy = value\n'
            '    def read(self):\n        return value\n'), [('sample.py', 5, 'value')])

    def test_builtins_follow_running_python(self):
        newer = 'ExceptionGroup'
        expected = [] if hasattr(builtins, newer) else [('sample.py', 2, newer)]
        self.assertEqual(self.check('def read():\n    return len([]), ' + newer + '\n'),
                         expected)

    def test_module_attributes_and_implicit_class_cell(self):
        self.assertEqual(self.check(
            'class Sample:\n    def read(self):\n'
            '        return __class__, __file__, __name__, __package__, __doc__, __spec__, __loader__, __builtins__, super()\n'), [])

    def test_scope_entry_expressions_and_comprehension_iterables(self):
        self.assertEqual(self.check(
            'def read(value=missing_default):\n    return [item for item in missing_iterable]\n'),
                         [('sample.py', 1, 'missing_default'), ('sample.py', 2, 'missing_iterable')])

    def test_lambda_and_all_comprehension_scopes(self):
        self.assertEqual(self.check(
            'left, right = (lambda a: a), (lambda b: b)\n'
            'values = {key: value for key, value in []}\n'
            'unique = {item for item in []}\n'
            'stream = (part for part in [])\n'), [])

    def test_same_line_dictionary_lambdas_report_both_names(self):
        self.assertEqual(self.check(
            'value = {(lambda: missing_key)(): (lambda: missing_value)() for item in []}\n'),
                         [('sample.py', 1, 'missing_key'), ('sample.py', 1, 'missing_value')])

    def test_parameter_annotations_match_compiler_scope_order(self):
        self.assertEqual(self.check(
            'def read(a: (lambda: missing_a), *args: (lambda: missing_b), '
            'k: (lambda: missing_c), **kwargs: (lambda: missing_d)) -> (lambda: missing_e):\n'
            '    pass\n'),
                         [('sample.py', 1, name) for name in
                          ('missing_a', 'missing_b', 'missing_c', 'missing_d', 'missing_e')])

    def test_annotations_follow_compiler_reference_rules(self):
        self.assertEqual(self.check(
            'from __future__ import annotations\n'
            'def read(value: FutureType) -> FutureType:\n'
            '    local: LocalType\n    return value\n'), [])
        self.assertEqual(self.check('def read(value: MissingType):\n    return value\n'),
                         [('sample.py', 1, 'MissingType')])

    def test_only_tracked_python_files_are_checked(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            subprocess.run(['git', 'init', '-q', str(root)], check=True)
            tracked = root / 'tracked.py'
            tracked.write_text('missing_tracked\n')
            (root / 'untracked.py').write_text('missing_untracked\n')
            subprocess.run(['git', 'add', 'tracked.py'], cwd=root, check=True)
            self.assertEqual(checker.findings(root=root), [(tracked, 1, 'missing_tracked')])

    def test_main_prints_findings_and_fails(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'sample.py'
            path.write_text('missing\n')
            output = io.StringIO()
            with redirect_stdout(output):
                self.assertEqual(checker.main([str(path)]), 1)
            self.assertIn(str(path) + ':1: missing', output.getvalue())
            self.assertIn('1 undefined name finding(s)', output.getvalue())


if __name__ == '__main__':
    unittest.main()
