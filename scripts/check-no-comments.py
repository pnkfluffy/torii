#!/usr/bin/env python3
"""Fail when a tracked code or config file contains a comment.

Python files are read with tokenize, so a hash inside a string is not a
comment. Other covered files use the first non-blank character of each line.
Only a line-1 shebang and a PEP 263 coding line on line 1 or 2 are allowed.
Docstrings are not comments.
"""

import io
import os
from pathlib import Path
import re
import subprocess
import sys
import tokenize


ROOT = Path(__file__).resolve().parents[1]
HASH_SUFFIXES = {'.sh', '.bash', '.yml', '.yaml', '.toml', '.cfg', '.ini'}
HASH_NAMES = {'.gitignore', '.gitattributes', '.dockerignore', '.editorconfig'}
SKIPPED_DIRS = {'.git', '__pycache__', '.venv', 'venv', 'node_modules', 'build', 'dist'}
CODING = re.compile(r'^[ \t\f]*#.*?coding[:=][ \t]*[-\w.]+')


def kind(path):
    """Return 'python', 'hash', or None for a file the rule does not cover."""
    if path.suffix == '.py':
        return 'python'
    if path.suffix in HASH_SUFFIXES or path.name in HASH_NAMES:
        return 'hash'
    if not path.suffix and not path.name.startswith('.'):
        try:
            with path.open('rb') as handle:
                if handle.read(2) == b'#!':
                    return 'hash'
        except OSError:
            return None
    return None


def python_findings(text):
    """Yield (line, text) for each disallowed comment token."""
    try:
        tokens = list(tokenize.generate_tokens(io.StringIO(text).readline))
    except (tokenize.TokenError, IndentationError, SyntaxError) as error:
        yield 0, 'cannot tokenize: ' + str(error)
        return
    for token in tokens:
        if token.type != tokenize.COMMENT:
            continue
        line, comment = token.start[0], token.string
        if line == 1 and comment.startswith('#!'):
            continue
        if line <= 2 and CODING.match(comment):
            continue
        yield line, comment


def hash_findings(text):
    """Yield (line, text) for each line whose first non-blank character is '#'."""
    for line, content in enumerate(text.splitlines(), 1):
        if line == 1 and content.startswith('#!'):
            continue
        if content.lstrip().startswith('#'):
            yield line, content.strip()


def tracked_files(root):
    """Return the repository's files: git ls-files, else a filtered walk."""
    try:
        listed = subprocess.run(['git', 'ls-files', '-z'], cwd=root, capture_output=True,
                                check=True, timeout=60).stdout.decode('utf-8')
        return [root / name for name in listed.split('\0') if name]
    except (OSError, subprocess.SubprocessError, UnicodeDecodeError):
        return walk(root)


def walk(directory):
    found = []
    for current, dirs, names in os.walk(directory):
        dirs[:] = sorted(name for name in dirs
                         if name not in SKIPPED_DIRS and not name.endswith('.egg-info'))
        found.extend(Path(current) / name for name in sorted(names))
    return found


def findings(paths=None, root=ROOT):
    """Return (path, line, text) for every comment in the covered files."""
    if paths:
        candidates = []
        for path in map(Path, paths):
            candidates.extend(walk(path) if path.is_dir() else [path])
    else:
        candidates = tracked_files(root)
    results = []
    for path in candidates:
        if path.is_symlink() or not path.is_file():
            continue
        style = kind(path)
        if style is None:
            continue
        text = path.read_text(encoding='utf-8')
        scan = python_findings if style == 'python' else hash_findings
        results.extend((path, line, comment) for line, comment in scan(text))
    return results


def display(path, root=ROOT):
    try:
        return str(path.resolve().relative_to(root))
    except ValueError:
        return str(path)


def main(argv):
    results = findings(argv)
    for path, line, comment in results:
        print('{}:{}: {}'.format(display(path), line, comment))
    print('{} comment finding(s)'.format(len(results)))
    return 1 if results else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
