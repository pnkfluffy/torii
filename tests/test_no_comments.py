import importlib.util
from pathlib import Path
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('check_no_comments', ROOT / 'scripts' / 'check-no-comments.py')
checker = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(checker)


class NoCommentsTests(unittest.TestCase):
    def test_repository_has_no_comments(self):
        found = ['{}:{}: {}'.format(checker.display(path), line, text)
                 for path, line, text in checker.findings()]
        self.assertEqual(found, [])

    def test_checker_reports_comments_and_pragmas_and_allows_shebang_and_coding(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            (folder / 'sample.py').write_text(
                '#!/usr/bin/env python3\n'
                '# -*- coding: utf-8 -*-\n'
                'value = "# not a comment"\n'
                'other = 1  # inline\n'
                '# full line\n'
                'risky = 2  # noqa: BLE001\n'
                'bare = 3  # noqa\n')
            (folder / 'run.sh').write_text('#!/bin/sh\n  # shell comment\necho "#ok"\n')
            (folder / 'ci.yml').write_text('name: x\n# yaml comment\n')
            (folder / 'notes.md').write_text('# Heading\n')
            found = sorted((path.name, line) for path, line, _ in checker.findings([folder]))
        self.assertEqual(found, [('ci.yml', 2), ('run.sh', 2), ('sample.py', 4),
                                 ('sample.py', 5), ('sample.py', 6), ('sample.py', 7)])


if __name__ == '__main__':
    unittest.main()
