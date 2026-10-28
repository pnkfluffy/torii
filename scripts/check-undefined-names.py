#!/usr/bin/env python3
"""Report unbound global reads using Python's compiler scope rules.

Bindings are flow insensitive. This does not detect uninitialized locals,
missing object attributes, dynamic namespace changes, or strings of code.
"""

import ast
import builtins
import os
from pathlib import Path
import subprocess
import symtable
import sys
import tokenize


ROOT = Path(__file__).resolve().parents[1]
MODULE_NAMES = {'__name__', '__doc__', '__package__', '__loader__', '__spec__',
                '__file__', '__cached__', '__builtins__', '__annotations__'}
SKIPPED_DIRS = {'.git', '__pycache__', '.venv', 'venv', 'node_modules', 'build', 'dist'}


def tracked_files(root):
    """Use tracked Python files, or a filtered walk outside a Git repository."""
    try:
        listed = subprocess.run(['git', 'ls-files', '-z'], cwd=root, capture_output=True,
                                check=True, timeout=60).stdout.decode('utf-8')
        return [root / name for name in listed.split('\0') if name.endswith('.py')]
    except (OSError, subprocess.SubprocessError, UnicodeDecodeError):
        paths = []
        for current, dirs, names in os.walk(root):
            dirs[:] = sorted(name for name in dirs if name not in SKIPPED_DIRS)
            paths.extend(Path(current) / name for name in sorted(names) if name.endswith('.py'))
        return paths


def bindings(table):
    """Collect module bindings and explicit global writes, including conditional ones."""
    found = set()
    for symbol in table.get_symbols():
        if (table.get_type() == 'module' or symbol.is_global()) and (
                symbol.is_assigned() or symbol.is_imported() or symbol.is_namespace()):
            found.add(symbol.get_name())
    for child in table.get_children():
        found.update(bindings(child))
    return found


class GlobalReads(ast.NodeVisitor):
    def __init__(self, table, allowed, future_annotations):
        self.table = table
        self.allowed = allowed
        self.future_annotations = future_annotations
        self.used_scopes = set()
        self.class_name = ''
        self.results = []

    def scope(self, node, name, body):
        parent = self.table
        child = next(child for child in parent.get_children()
                     if child.get_name() == name and child.get_lineno() == node.lineno
                     and child.get_id() not in self.used_scopes)
        self.used_scopes.add(child.get_id())
        self.table = child
        for item in body:
            self.visit(item)
        self.table = parent

    def visit_Name(self, node):
        if not isinstance(node.ctx, ast.Load):
            return
        name = node.id
        if self.class_name and name.startswith('__') and not name.endswith('__'):
            name = '_' + self.class_name.lstrip('_') + name
        try:
            symbol = self.table.lookup(name)
        except KeyError:
            return
        if symbol.is_referenced() and symbol.is_global() and name not in self.allowed:
            self.results.append((node.lineno, name))

    def visit_FunctionDef(self, node):
        for value in node.decorator_list + node.args.defaults + node.args.kw_defaults:
            if value is not None:
                self.visit(value)
        if not self.future_annotations:
            arguments = node.args.posonlyargs + node.args.args
            arguments += [arg for arg in (node.args.vararg, node.args.kwarg) if arg is not None]
            arguments += node.args.kwonlyargs
            for value in [arg.annotation for arg in arguments] + [node.returns]:
                if value is not None:
                    self.visit(value)
        self.scope(node, node.name, node.body)

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_ClassDef(self, node):
        for value in node.decorator_list + node.bases + [item.value for item in node.keywords]:
            self.visit(value)
        parent_class = self.class_name
        self.class_name = node.name
        self.scope(node, node.name, node.body)
        self.class_name = parent_class

    def visit_Lambda(self, node):
        for value in node.args.defaults + node.args.kw_defaults:
            if value is not None:
                self.visit(value)
        self.scope(node, 'lambda', [node.body])

    def comprehension(self, node, name, values):
        first, *rest = node.generators
        self.visit(first.iter)
        body = [first.target] + first.ifs
        for generator in rest:
            body += [generator.iter, generator.target] + generator.ifs
        self.scope(node, name, body + values)

    def visit_ListComp(self, node):
        self.comprehension(node, 'listcomp', [node.elt])

    def visit_SetComp(self, node):
        self.comprehension(node, 'setcomp', [node.elt])

    def visit_DictComp(self, node):
        self.comprehension(node, 'dictcomp', [node.value, node.key])

    def visit_GeneratorExp(self, node):
        self.comprehension(node, 'genexpr', [node.elt])

    def visit_AnnAssign(self, node):
        self.visit(node.target)
        if node.value is not None:
            self.visit(node.value)
        if not self.future_annotations and self.table.get_type() != 'function':
            self.visit(node.annotation)


def findings(paths=None, root=ROOT):
    """Return (path, line, name) for unresolved global reads in Python files."""
    candidates = tracked_files(root) if not paths else [Path(path) for path in paths]
    results = []
    for path in candidates:
        if path.is_dir():
            results.extend(findings(root=path))
            continue
        if path.suffix != '.py' or path.is_symlink() or not path.is_file():
            continue
        with tokenize.open(path) as source:
            text = source.read()
        tree = ast.parse(text, filename=str(path))
        table = symtable.symtable(text, str(path), 'exec')
        future_annotations = any(isinstance(node, ast.ImportFrom) and node.module == '__future__'
                                 and any(alias.name == 'annotations' for alias in node.names)
                                 for node in tree.body)
        allowed = bindings(table) | set(dir(builtins)) | MODULE_NAMES
        if path.name == '__init__.py':
            allowed.add('__path__')
        reads = GlobalReads(table, allowed, future_annotations)
        reads.visit(tree)
        results.extend((path, line, name) for line, name in sorted(set(reads.results)))
    return results


def display(path, root=ROOT):
    try:
        return str(path.resolve().relative_to(root))
    except ValueError:
        return str(path)


def main(argv):
    results = findings(argv)
    for path, line, name in results:
        print('{}:{}: {}'.format(display(path), line, name))
    print('{} undefined name finding(s)'.format(len(results)))
    return 1 if results else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
