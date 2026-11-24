#!/usr/bin/env python3
"""Require error logs to use rate-limited problem records."""

import ast
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]


def is_record_call(node):
    return (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name) and node.func.value.id == 'problems'
            and node.func.attr == 'record')


def is_record(node):
    return is_record_call(node) and any(item.arg == 'every' for item in node.keywords)


def unbounded_error_record(node):
    levels = [item.value for item in node.keywords if item.arg == 'level'] if is_record_call(node) else []
    return bool(levels and not is_record(node) and any(
        isinstance(item, ast.Attribute) and isinstance(item.value, ast.Name)
        and item.value.id == 'logging' and item.attr == 'ERROR'
        for level in levels for item in ast.walk(level)))


def is_error(node):
    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
        return False
    if node.func.attr not in ('error', 'exception'):
        return False
    owner = node.func.value
    return (isinstance(owner, ast.Name) and owner.id in ('logger', 'logging')
            or isinstance(owner, ast.Attribute) and owner.attr == 'logger'
            or isinstance(owner, ast.Call) and isinstance(owner.func, ast.Attribute)
            and isinstance(owner.func.value, ast.Name) and owner.func.value.id == 'logging'
            and owner.func.attr == 'getLogger')


def findings(text):
    tree = ast.parse(text)
    return sorted(node.lineno for node in ast.walk(tree) if is_error(node) or unbounded_error_record(node))


def main():
    found = [(path, line) for path in sorted((ROOT / 'coordinator').glob('*.py'))
             for line in findings(path.read_text(encoding='utf-8'))]
    for path, line in found:
        print('%s:%d: error log has no rate-limited problem' % (path.relative_to(ROOT), line))
    print('%d error logging finding(s)' % len(found))
    return bool(found)


if __name__ == '__main__':
    sys.exit(main())
