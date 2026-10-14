import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from coordinator.shared_mcp import write_config
from coordinator.providers import ProviderRunner
from coordinator.store import Store
from tests.support import stop_test_hosts


class SharedMcpConfigTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.home = self.root / 'home'
        self.home.mkdir()
        self.environment = patch.dict(os.environ, {'HOME': str(self.home)})
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.overrides = self.root / 'local-overrides.json'
        self.overrides.write_text(json.dumps({'shared_mcp_servers': ['example-mcp', 'other-mcp']}))
        selection = patch('coordinator.accounts.LOCAL_OVERRIDES_PATH', self.overrides)
        selection.start()
        self.addCleanup(selection.stop)
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        self.value = 'FAKE-SHARED-MCP-HEADER-VALUE'
        self.work = {'type': 'http', 'url': 'http://127.0.0.1:1/work',
                     'headers': {'Authorization': self.value}}
        self.personal = {'type': 'http', 'url': 'http://127.0.0.1:1/personal',
                         'headers': {'Authorization': self.value}}

    def source(self, servers):
        (self.home / '.claude.json').write_text(json.dumps({'mcpServers': servers}))

    def problems(self):
        return [tuple(row) for row in self.store.db.execute(
            'SELECT area,code,detail FROM problems ORDER BY id')]

    def test_exact_entries_mode_and_changed_source(self):
        self.source({'example-mcp': self.work, 'other-mcp': self.personal,
                     'unrelated': {'type': 'http', 'url': 'http://127.0.0.1:1/other', 'headers': {}}})
        path = write_config(self.store)
        self.assertEqual(path.parent, self.store.directory)
        self.assertTrue(path.name.startswith('shared-mcp-'))
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(json.loads(path.read_text()),
                         {'mcpServers': {'example-mcp': self.work, 'other-mcp': self.personal}})
        self.assertEqual(list(self.store.directory.glob('shared-mcp-*.tmp')), [])
        updated = dict(self.work, url='http://127.0.0.1:2/work')
        self.source({'example-mcp': updated, 'other-mcp': self.personal})
        updated_path = write_config(self.store)
        self.assertNotEqual(updated_path, path)
        self.assertEqual(json.loads(path.read_text())['mcpServers']['example-mcp'], self.work)
        self.assertEqual(json.loads(updated_path.read_text())['mcpServers']['example-mcp'], updated)
        self.assertEqual(self.problems(), [])

    def test_shared_mcp_only_preserves_output_without_problems(self):
        self.source({'example-mcp': self.work, 'other-mcp': self.personal})
        path = write_config(self.store)
        self.assertEqual(json.loads(path.read_text()),
                         {'mcpServers': {'example-mcp': self.work, 'other-mcp': self.personal}})
        self.assertEqual(self.problems(), [])

    def test_default_none_does_not_read_source_or_record_problems(self):
        self.overrides.write_text('{}')
        with patch('coordinator.shared_mcp.Path.home', side_effect=AssertionError('source read')):
            self.assertIsNone(write_config(self.store))
        self.overrides.rename(self.root / 'saved-overrides.json')
        self.assertIsNone(write_config(self.store))
        self.assertEqual(self.problems(), [])

    def test_empty_and_invalid_selection(self):
        for names in (None, 'example-mcp', [3], ['']):
            self.overrides.write_text(json.dumps({'shared_mcp_servers': names}))
            self.assertIsNone(write_config(self.store))
        self.assertEqual([row[:2] for row in self.problems()],
                         [('shared-mcp', 'selection-invalid')] * 4)
        self.overrides.write_text(json.dumps({'shared_mcp_servers': []}))
        self.assertIsNone(write_config(self.store))

    def test_concurrent_writes_use_unique_temporary_files(self):
        self.source({'example-mcp': self.work, 'other-mcp': self.personal})
        with ThreadPoolExecutor(max_workers=8) as pool:
            paths = list(pool.map(lambda _: write_config(self.store), range(16)))
        self.assertEqual(set(paths), {paths[0]})
        self.assertEqual(json.loads(paths[0].read_text()),
                         {'mcpServers': {'example-mcp': self.work, 'other-mcp': self.personal}})
        self.assertEqual(paths[0].stat().st_mode & 0o777, 0o600)
        self.assertEqual(list(self.store.directory.glob('shared-mcp-*.tmp')), [])

    def test_reuse_refreshes_file_and_removes_only_stale_versions(self):
        self.source({'example-mcp': self.work})
        first = write_config(self.store)
        self.source({'example-mcp': dict(self.work, url='http://127.0.0.1:2/work')})
        second = write_config(self.store)
        old = time.time() - 3700
        os.utime(first, (old, old))
        self.assertEqual(write_config(self.store), second)
        self.assertFalse(first.exists())
        os.utime(second, (old, old))
        self.assertEqual(write_config(self.store), second)
        self.assertGreater(second.stat().st_mtime, old)

    def test_missing_source_omits_stale_file(self):
        self.source({'example-mcp': self.work, 'other-mcp': self.personal})
        path = write_config(self.store)
        (self.home / '.claude.json').unlink()
        self.assertIsNone(write_config(self.store))
        self.assertTrue(path.exists())
        self.assertEqual(self.problems(),
                         [('shared-mcp', 'source-missing',
                           'entries=example-mcp,other-mcp')])

    def test_bad_json_and_unreadable_source(self):
        source = self.home / '.claude.json'
        source.write_text('{')
        self.assertIsNone(write_config(self.store))
        source.unlink()
        source.mkdir()
        self.assertIsNone(write_config(self.store))
        self.assertEqual(self.problems(), [
            ('shared-mcp', 'source-invalid-json',
             'entries=example-mcp,other-mcp'),
            ('shared-mcp', 'source-unreadable',
             'entries=example-mcp,other-mcp')])

    def test_partial_and_invalid_entries(self):
        self.source({'example-mcp': self.work})
        path = write_config(self.store)
        self.assertEqual(json.loads(path.read_text()), {'mcpServers': {'example-mcp': self.work}})
        self.source({'example-mcp': {'type': 'http', 'url': 3, 'headers': {}},
                     'other-mcp': self.personal})
        changed_path = write_config(self.store)
        self.assertNotEqual(changed_path, path)
        self.assertEqual(json.loads(changed_path.read_text()), {'mcpServers': {'other-mcp': self.personal}})
        self.source({'example-mcp': {'type': 'http', 'url': 3, 'headers': {}},
                     'other-mcp': {'type': 'http', 'url': 'x', 'headers': {'x': 3}}})
        self.assertIsNone(write_config(self.store))
        self.assertTrue(path.exists())
        self.assertEqual(self.problems(), [
            ('shared-mcp', 'entry-missing', 'entry=other-mcp'),
            ('shared-mcp', 'entry-invalid', 'entry=example-mcp'),
            ('shared-mcp', 'entry-invalid', 'entry=example-mcp'),
            ('shared-mcp', 'entry-invalid', 'entry=other-mcp')])
        self.assertNotIn(self.value, json.dumps(self.problems()))


class SharedMcpLaunchRaceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.addCleanup(stop_test_hosts, self.root)
        self.home = self.root / 'home'
        self.home.mkdir()
        environment = patch.dict(os.environ, {'HOME': str(self.home)})
        environment.start()
        self.addCleanup(environment.stop)
        overrides = self.root / 'local-overrides.json'
        overrides.write_text(json.dumps({'shared_mcp_servers': ['example-mcp', 'other-mcp']}))
        selection = patch('coordinator.accounts.LOCAL_OVERRIDES_PATH', overrides)
        selection.start()
        self.addCleanup(selection.stop)
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        self.binary = self.root / 'fake-claude'
        self.binary.write_text('''#!/usr/bin/env python3
import json,sys
from pathlib import Path
sys.stdin.read()
sid=sys.argv[sys.argv.index('--session-id')+1]
servers=json.loads(Path(sys.argv[sys.argv.index('--mcp-config')+1]).read_text())['mcpServers']
value=servers['example-mcp']['headers']['Authorization']
print(value,file=sys.stderr,flush=True)
print(json.dumps({'type':'result','subtype':'success','session_id':sid,
                  'result':json.dumps({'names':sorted(servers),'value':value})}),flush=True)
''')
        self.binary.chmod(0o700)

    def entry(self, value):
        return {'type': 'http', 'url': 'http://127.0.0.1:1/work',
                'headers': {'Authorization': value}}

    async def test_overlapping_launches_keep_each_config_and_scrubber_together(self):
        old = self.entry('DUMMY-OLD-RACE-TOKEN')
        new = self.entry('DUMMY-NEW-RACE-TOKEN')
        cases = (
            ('rotated', {'example-mcp': old},
             {'example-mcp': new, 'other-mcp': new}),
            ('degraded', {'example-mcp': old, 'other-mcp': old},
             {'example-mcp': old}),
        )
        for label, first_entries, second_entries in cases:
            with self.subTest(label=label):
                source = self.home / '.claude.json'
                source.write_text(json.dumps({'mcpServers': first_entries}))
                broker = SimpleNamespace(store=self.store)
                first_runner = ProviderRunner(self.store.directory, {'claude': str(self.binary)},
                                              account_broker=broker)
                second_runner = ProviderRunner(self.store.directory, {'claude': str(self.binary)},
                                               account_broker=broker)
                ready = asyncio.Event()
                release = asyncio.Event()

                async def delayed(env, secrets=None):
                    ready.set()
                    await release.wait()
                    return {}

                for runner in (first_runner, second_runner):
                    runner._external_writer = AsyncMock(return_value=False)
                    runner._environment = lambda *args: {'PATH': os.environ['PATH'], 'HOME': str(self.home)}
                first_runner.launch_secrets = delayed
                first = asyncio.create_task(first_runner.run(
                    'claude', 'hello', self.root, None, fresh=True, host_id='first-' + label))
                try:
                    await asyncio.wait_for(ready.wait(), 5)
                    source.write_text(json.dumps({'mcpServers': second_entries}))
                    second = await second_runner.run(
                        'claude', 'hello', self.root, None, fresh=True, host_id='second-' + label)
                    release.set()
                    result = await first
                    self.assertTrue(second.success, second.error)
                    self.assertTrue(result.success, result.error)
                    self.assertEqual(json.loads(result.text)['names'], sorted(first_entries))
                    new_value = 'DUMMY-NEW-RACE-TOKEN'
                    host = Path(result.log_path).parent
                    self.assertNotIn(new_value, result.text)
                    self.assertNotIn(new_value, Path(result.log_path).read_text())
                    self.assertNotIn(new_value, (host / 'events.jsonl').read_text())
                finally:
                    release.set()
                    await asyncio.gather(first, return_exceptions=True)
