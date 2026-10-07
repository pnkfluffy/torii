from coordinator import extension
import ast
import contextlib
import importlib.util
import io
import html
import json
import os
from pathlib import Path
import re
import tarfile
import zipfile
import subprocess
import tempfile
import unittest
import unicodedata
from unittest.mock import patch
from urllib.parse import unquote, urlsplit


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('public_snapshot', ROOT / 'scripts/public_snapshot.py')
snapshot = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(snapshot)


def heading_anchors(source):
    """GitHub heading slugs, including duplicate headings and fenced code."""
    anchors = set()
    fence = None
    previous = ''
    for line in source.splitlines():
        marker = re.match(r'^ {0,3}(`{3,}|~{3,})', line)
        if marker:
            if fence is None:
                fence = marker[1]
            elif marker[1][0] == fence[0] and len(marker[1]) >= len(fence):
                fence = None
            previous = ''
            continue
        if fence:
            continue
        heading = re.match(r'^ {0,3}#{1,6}\s+(.+?)\s*$', line)
        if heading:
            title = re.sub(r'\s+#+\s*$', '', heading[1])
        elif previous.strip() and re.fullmatch(r' {0,3}(?:=+|-+)\s*', line):
            title = previous.strip()
        else:
            previous = line
            continue
        title = re.sub(r'\[([^\]]*)\]\([^)]*\)', r'\1', title)
        title = re.sub(r'(?<!\w)(_+)(?=\S)(.+?)(?<=\S)\1(?!\w)', r'\2', title)
        title = html.unescape(re.sub(r'<[^>]*>', '', title)).lower()
        slug = ''.join(char for char in title if char in ' -_'
                       or not char.isspace() and unicodedata.category(char)[0] not in 'PSC').replace(' ', '-')
        anchor = slug
        suffix = 0
        while anchor in anchors:
            suffix += 1
            anchor = slug + '-' + str(suffix)
        anchors.add(anchor)
        previous = ''
    return anchors


class PublicSnapshotClosureTests(unittest.TestCase):
    def setUp(self):
        manifest = json.loads((ROOT / snapshot.MANIFEST).read_text())
        self.keep = set(manifest['keep'])
        self.classified = self.keep | set(manifest['omit'])
        self.tracked = set(subprocess.check_output(
            ['git', 'ls-files', '-z'], cwd=ROOT).decode().strip('\0').split('\0'))

    def test_every_tracked_path_is_classified(self):
        self.assertEqual(self.tracked - self.classified, set(), 'Classify tracked paths as keep or omit.')

    def test_kept_python_imports_keep_their_local_modules(self):
        modules = {name[:-3].replace('/', '.').removesuffix('.__init__'): name
                   for name in self.classified | self.tracked if name.endswith('.py')}
        for name in sorted(self.keep):
            if not name.endswith('.py'):
                continue
            package = str(Path(name).parent).replace('/', '.')
            for node in ast.walk(ast.parse((ROOT / name).read_text(), filename=name)):
                imported = []
                if isinstance(node, ast.Import):
                    imported = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom):
                    module = importlib.util.resolve_name('.' * node.level + (node.module or ''), package)
                    imported = [module] + [module + '.' + alias.name for alias in node.names
                                           if module + '.' + alias.name in modules]
                for module in imported:
                    if module.split('.')[0] not in ('coordinator', 'scripts'):
                        continue
                    if module in ('coordinator', 'scripts') and module not in modules:
                        continue
                    with self.subTest(source=name, module=module):
                        self.assertIn(modules.get(module, module.replace('.', '/') + '.py'), self.keep)

    def test_kept_files_keep_the_helper_assets_they_reference(self):
        assets = {name for name in self.classified | self.tracked if '/helpers/' in name}
        for name in sorted(self.keep - {snapshot.MANIFEST}):
            source = (ROOT / name).read_text(errors='replace')
            referenced = {asset for asset in assets if Path(asset).name in source}
            for match in re.finditer(r"['\"]([^'\"\n]+)['\"]", source):
                value = match[1]
                helper_relative = value.startswith('helpers/') or '/helpers/' in value
                helper_join = re.search(r'\bHELPERS\s*/\s*$', source[:match.start()])
                if Path(value).suffix and (helper_relative or helper_join):
                    candidates = {asset for asset in assets if asset == value or asset.endswith('/' + value)}
                    referenced.update(candidates or {
                        str(Path(name).parent / ('helpers' if helper_join else '') / value)})
            for asset in referenced:
                with self.subTest(source=name, asset=asset):
                    self.assertTrue(asset in self.keep, name + ' references an omitted helper: ' + asset)


class PublicSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repo = self.root / 'source'
        self.repo.mkdir()
        self.output = self.root / 'export'
        self.denylist = self.root / 'denylist.txt'
        self.denylist.write_text('harmless-private-marker\n')
        self.env = dict(os.environ, GIT_AUTHOR_NAME='Fixture', GIT_AUTHOR_EMAIL='fixture@example.invalid',
                        GIT_COMMITTER_NAME='Fixture', GIT_COMMITTER_EMAIL='fixture@example.invalid',
                        GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL=os.devnull)
        snapshot.git(self.repo, 'init', env=self.env)
        (self.repo / 'scripts').mkdir()
        (self.repo / 'README.md').write_text('public source\n')
        (self.repo / 'run.sh').write_text('#!/bin/sh\ntrue\n')
        (self.repo / 'run.sh').chmod(0o755)
        (self.repo / 'internal.txt').write_text('harmless-private-marker\n')
        self.manifest = {'keep': {'README.md': 'User guide.', 'run.sh': 'Launcher.'},
                         'omit': {'internal.txt': 'Private notes.', snapshot.MANIFEST: 'Fixture manifest.'}}
        self.commit = self.save()

    def save(self):
        (self.repo / snapshot.MANIFEST).write_text(json.dumps(self.manifest))
        snapshot.git(self.repo, 'add', '--all', env=self.env)
        snapshot.git(self.repo, 'commit', '-m', 'fixture', env=self.env)
        return snapshot.git(self.repo, 'rev-parse', 'HEAD').decode().strip()

    def build(self, commit=None):
        return snapshot.build(self.repo, commit or self.commit, self.output, self.denylist,
                              'Public Author', 'public@example.invalid')

    def test_export_uses_commit_allowlist_and_has_one_commit_no_remote_and_new_identity(self):
        (self.repo / 'README.md').write_text('dirty harmless-private-marker\n')
        (self.repo / 'untracked.txt').write_text('never ship\n')
        with patch.object(snapshot, 'scan') as scan:
            report = self.build()
        scan.assert_called_once_with(self.output)
        self.assertEqual((self.output / 'README.md').read_text(), 'public source\n')
        self.assertEqual(sorted(report['keep']), ['README.md', 'run.sh'])
        self.assertEqual(report['omit']['internal.txt'], 'Private notes.')
        self.assertFalse((self.output / 'internal.txt').exists())
        self.assertFalse((self.output / 'untracked.txt').exists())
        self.assertTrue(os.access(self.output / 'run.sh', os.X_OK))
        self.assertEqual(snapshot.git(self.output, 'rev-list', '--count', 'HEAD').strip(), b'1')
        self.assertEqual(snapshot.git(self.output, 'remote').strip(), b'')
        self.assertEqual(snapshot.git(self.output, 'log', '-1', '--format=%an <%ae>|%cn <%ce>').strip(),
                         b'Public Author <public@example.invalid>|Public Author <public@example.invalid>')
        dates = snapshot.git(self.output, 'log', '-1', '--format=%aI|%cI').decode().strip().split('|')
        self.assertEqual(dates[0], dates[1])
        self.assertTrue(dates[0].endswith('Z'))
        self.assertEqual(snapshot.git(self.output, 'status', '--porcelain').strip(), b'')

    def test_source_manifest_includes_group_live_test_plan(self):
        manifest = json.loads((ROOT / snapshot.MANIFEST).read_text())
        self.assertIn('docs/group-setup-live-test.md', manifest['keep'])

    def test_snapshot_records_the_source_revision_before_scanning(self):
        (self.repo / 'VERSION').write_text('$Format:%h$\n')
        self.manifest['keep']['VERSION'] = 'Source archive version.'
        revision = self.save()
        def scan(output):
            self.assertEqual((output / 'VERSION').read_text(), revision[:7] + '\n')
        with patch.object(snapshot, 'scan', side_effect=scan):
            self.build(revision)
        self.assertEqual(snapshot.git(self.output, 'show', 'HEAD:VERSION').decode(), revision[:7] + '\n')

    def test_tar_and_zip_archives_substitute_their_version_without_git(self):
        from coordinator.log import running_commit
        (self.repo / 'VERSION').write_text('$Format:%h$\n')
        (self.repo / '.gitattributes').write_bytes((ROOT / '.gitattributes').read_bytes())
        self.manifest['keep'].update({'VERSION': 'Version.', '.gitattributes': 'Archive substitution.'})
        revision = self.save()
        for format_ in ('tar', 'zip'):
            with self.subTest(format=format_):
                data = snapshot.git(self.repo, 'archive', '--format=' + format_, revision)
                if format_ == 'tar':
                    with tarfile.open(fileobj=io.BytesIO(data)) as archive:
                        version = archive.extractfile('VERSION').read()
                else:
                    with zipfile.ZipFile(io.BytesIO(data)) as archive:
                        version = archive.read('VERSION')
                plain = self.root / format_
                plain.mkdir()
                (plain / 'VERSION').write_bytes(version)
                self.assertEqual(running_commit(plain), revision[:7])

    def test_denylist_checks_every_nonblank_line_case_insensitively(self):
        self.denylist.write_text('unmatched-marker\n\nPUBLIC SOURCE\n')
        with patch.object(snapshot, 'scan') as scan, self.assertRaisesRegex(ValueError, 'Owner identifier'):
            self.build()
        scan.assert_not_called()
        self.assertFalse((self.output / '.git').exists())

    def test_denylist_checks_names_and_commit_identity(self):
        for denied in ('run.sh', 'PUBLIC AUTHOR', 'public@example.invalid'):
            self.denylist.write_text(denied)
            with self.assertRaisesRegex(ValueError, 'Owner identifier'):
                self.build()
            self.assertFalse(self.output.exists())

    def test_empty_denylist_refuses(self):
        self.denylist.write_text('\n  \n')
        with self.assertRaisesRegex(ValueError, 'at least one'):
            self.build()
        self.assertFalse(self.output.exists())

    def test_existing_output_is_preserved(self):
        self.output.mkdir()
        (self.output / 'keep').write_text('keep')
        with self.assertRaises(FileExistsError):
            self.build()
        self.assertEqual((self.output / 'keep').read_text(), 'keep')

    def test_unclassified_tracked_files_refuse_before_creating_output(self):
        (self.repo / 'unclassified.txt').write_text('not in either allowlist group\n')
        revision = self.save()
        with self.assertRaisesRegex(ValueError, 'unclassified.txt'):
            self.build(revision)
        self.assertFalse(self.output.exists())

    def test_scanner_failure_preserves_export_without_creating_git_history(self):
        with patch.object(snapshot, 'scan', side_effect=RuntimeError('scan rejected')), self.assertRaises(RuntimeError):
            self.build()
        self.assertTrue((self.output / 'README.md').exists())
        self.assertFalse((self.output / '.git').exists())

    def test_unsafe_allowlist_missing_files_and_symlinks_refuse(self):
        for name in ('../escape', '/absolute', '.git/config', 'missing.txt'):
            self.manifest['keep'] = {name: 'Invalid fixture.'}
            revision = self.save()
            with self.assertRaises(ValueError):
                self.build(revision)
            self.assertFalse(self.output.exists())
        (self.repo / 'link').symlink_to('README.md')
        self.manifest['keep'] = {'README.md': 'User guide.', 'run.sh': 'Launcher.',
                                 'link': 'Invalid symlink fixture.'}
        revision = self.save()
        with self.assertRaisesRegex(ValueError, 'regular files'):
            self.build(revision)
        self.assertFalse(self.output.exists())

    def test_scanners_are_local_fail_closed_and_do_not_expose_output(self):
        with patch.object(snapshot.shutil, 'which', side_effect=lambda name: '/tools/' + name), patch.object(
                snapshot.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0)) as run:
            snapshot.scan(self.output)
        self.assertEqual(run.call_count, 2)
        gitleaks, trufflehog = [call.args[0] for call in run.call_args_list]
        self.assertIn('--redact=100', gitleaks)
        for flag in ('--no-verification', '--no-update', '--fail', '--fail-on-scan-errors'):
            self.assertIn(flag, trufflehog)
        for call in run.call_args_list:
            self.assertEqual(call.kwargs['stdout'], subprocess.DEVNULL)
            self.assertEqual(call.kwargs['stderr'], subprocess.DEVNULL)
        for failure in (1, 183):
            with patch.object(snapshot.shutil, 'which', return_value='/tools/scanner'), patch.object(
                    snapshot.subprocess, 'run', return_value=subprocess.CompletedProcess([], failure)), self.assertRaises(RuntimeError):
                snapshot.scan(self.output)
        with patch.object(snapshot.shutil, 'which', return_value=None), self.assertRaises(RuntimeError):
            snapshot.scan(self.output)

    def test_cli_failure_prints_no_scanner_or_identifier_values(self):
        stream = io.StringIO()
        with patch.object(snapshot, 'build', side_effect=RuntimeError('sensitive-value')), contextlib.redirect_stderr(stream):
            result = snapshot.main(['--commit', 'HEAD', '--output', str(self.output), '--denylist', str(self.denylist),
                                    '--author-name', 'Public Author', '--author-email', 'public@example.invalid'])
        self.assertEqual(result, 1)
        self.assertNotIn('sensitive-value', stream.getvalue())


if __name__ == '__main__':
    unittest.main()


class PublicClosureTests(unittest.TestCase):
    def setUp(self):
        self.manifest = json.loads((ROOT / snapshot.MANIFEST).read_text())

    def test_kept_code_names_no_omitted_module(self):
        names = {}
        for path in self.manifest['omit']:
            if path.endswith('.py'):
                dotted = path[:-3].replace('/', '.')
                for value in (dotted, '.' + Path(path).stem, path):
                    names[value] = path
        for path in self.manifest['keep']:
            if not path.endswith('.py'):
                continue
            for node in ast.walk(ast.parse((ROOT / path).read_text())):
                if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                    continue
                if path == 'coordinator/extension.py' and node.value == '.' + extension.LOCAL.rsplit('.', 1)[1]:
                    continue
                self.assertNotIn(node.value, names, path)

    def test_kept_docs_link_only_kept_files(self):
        anchors = {}
        for path in self.manifest['keep']:
            if not path.endswith('.md'):
                continue
            source = (ROOT / path).read_text()
            targets = re.findall(r'\[[^\]]*\]\((<[^>]+>|[^\s)]+)(?:\s+"[^"]*")?\)', source)
            targets += re.findall(r'^ {0,3}\[[^\]]+\]:\s*(<[^>]+>|[^\s]+)', source, re.MULTILINE)
            targets += re.findall(r'\bhref\s*=\s*[\"\']([^\"\']+)[\"\']', source, re.IGNORECASE)
            for target in targets:
                link = urlsplit(html.unescape(target.strip('<>')))
                if link.scheme or link.netloc:
                    continue
                target_path = unquote(link.path)
                if target_path.startswith('/'):
                    resolved = ROOT.joinpath(target_path.lstrip('/')).resolve()
                else:
                    resolved = (ROOT / path).parent.joinpath(target_path).resolve() if target_path else ROOT / path
                with self.subTest(source=path, target=target):
                    name = resolved.relative_to(ROOT).as_posix()
                    self.assertIn(name, self.manifest['keep'], path)
                    if link.fragment and resolved.suffix == '.md':
                        if name not in anchors:
                            anchors[name] = heading_anchors(resolved.read_text())
                        self.assertIn(unquote(link.fragment), anchors[name], path)

    def test_docs_accept_formatted_unicode_and_duplicate_heading_links(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'guide.md').write_text(
                "# This'll be a _Helpful_ Section About the Greek Letter Θ!\n"
                '## Repeat\n## Repeat\n## Repeat-1\n'
                'Setext heading\n--------------\n'
                '```\n## Repeat\n```\n'
                '[unicode](#thisll-be-a-helpful-section-about-the-greek-letter-%CE%B8)\n'
                '[duplicate](#repeat-1)\n[collision](#repeat-1-1)\n'
                '[reference]: /guide.md#setext-heading\n'
                "<a href='guide.md#repeat'>repeat</a>\n")
            self.manifest = {'keep': {'guide.md': 'Fixture guide.'}, 'omit': {}}
            with patch(__name__ + '.ROOT', root):
                self.test_kept_docs_link_only_kept_files()


class ExportManifestTests(unittest.TestCase):
    setUp = PublicSnapshotTests.setUp
    save = PublicSnapshotTests.save
    build = PublicSnapshotTests.build
    def test_export_manifest_hides_omitted_paths(self):
        self.manifest['keep'][snapshot.MANIFEST] = 'Export file list.'
        self.manifest['omit'].pop(snapshot.MANIFEST)
        self.commit = self.save()
        with patch.object(snapshot, 'scan'):
            report = self.build()
        exported = json.loads((self.output / snapshot.MANIFEST).read_text())
        self.assertEqual(exported, {'keep': self.manifest['keep'], 'omit': {}})
        self.assertEqual(report['omit'], self.manifest['omit'])
