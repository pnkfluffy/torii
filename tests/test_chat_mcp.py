import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from coordinator.chat_mcp import migrate, op_chat_mcp, op_tag_mcp, selection, settings_text
from coordinator.control_api import Context, Refused, call
from coordinator.shared_mcp import codex_inherited, codex_launch, codex_selected, read_entries
from coordinator.store import Store


class ChatMcpTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.home = self.root / 'home'
        self.home.mkdir()
        env = patch.dict(os.environ, {'HOME': str(self.home)})
        env.start()
        self.addCleanup(env.stop)
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        for topic, thread in (('1:2', 2), ('1:3', 3), ('1:4', 4)):
            self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                                  (topic, 1, thread, topic, str(self.root)))
        self.personal = {'type': 'http', 'url': 'https://example.invalid/personal',
                         'headers': {'Authorization': 'DUMMY-PERSONAL'}}
        self.work = {'type': 'http', 'url': 'https://example.invalid/work',
                     'headers': {'Authorization': 'DUMMY-WORK'}}
        (self.home / '.claude.json').write_text(json.dumps({'mcpServers': {
            'personal': self.personal, 'work': self.work}}))

    def test_selection_is_scoped_and_control_validates(self):
        op_chat_mcp(self.store, Context(), '1:2', '["personal"]')
        op_chat_mcp(self.store, Context(), '1:3', '["work"]')
        self.assertEqual(read_entries(self.store, topic='1:2'), {'personal': self.personal})
        self.assertEqual(read_entries(self.store, topic='1:3'), {'work': self.work})
        self.assertEqual(read_entries(self.store, topic='1:4'), {})
        self.assertEqual(read_entries(self.store), {})
        for value in ('["unknown"]', '["torii"]', '[1]', '{}'):
            with self.assertRaises(Refused):
                op_chat_mcp(self.store, Context(), '1:2', value)
        self.assertEqual(selection(self.store, '1:2'), ['personal'])
        self.assertFalse(call(self.store, 'chat.mcp', {'topic': '1:2', 'names': '[]'}, source='mcp').ok)
        op_chat_mcp(self.store, Context(), '1:2', '[]')
        self.assertEqual(selection(self.store, '1:2'), [])

    def test_chat_mcp_servers_are_named_in_the_fixed_rules(self):
        self.assertEqual(settings_text(self.store, '1:2'), '')
        op_chat_mcp(self.store, Context(), '1:3', '["work"]')
        self.assertEqual(settings_text(self.store, '1:3'), '\nMCP servers of this chat: `work`.')
        from coordinator import policy
        self.store.put('coordinator_home_topic', '1:2')
        for audience in ('worker', 'coordinator'):
            fixed, _ = policy.instructions(self.store, 'coordinator:1:3', audience, create_scratch=False)
            self.assertIn('MCP servers of this chat: `work`.', fixed)

    def test_http_entry_without_headers_is_supported(self):
        (self.home / '.claude.json').write_text(json.dumps({'mcpServers': {
            'public': {'type': 'http', 'url': 'https://example.invalid/mcp'}}}))
        op_chat_mcp(self.store, Context(), '1:2', '["public"]')
        self.assertEqual(read_entries(self.store, topic='1:2')['public']['headers'], {})

    def test_registered_control_sets_and_reads_selection(self):
        result = call(self.store, 'chat.mcp', {'topic': '1:2', 'names': '["personal"]'})
        self.assertTrue(result.ok, result.text)
        result = call(self.store, 'chat.mcp', {'topic': '1:2'})
        self.assertTrue(result.ok, result.text)
        self.assertEqual(result.data['mcp_servers'], ['personal'])

    def test_migration_is_once_and_preserves_existing_settings(self):
        self.store.put('chat_config:1:2', {'other': 'keep'})
        self.store.put('chat_config:1:3', {'mcp_servers': [], 'other': 'keep'})
        migrate(self.store, {'1:2': ['personal'], '1:3': ['work']})
        self.assertEqual(self.store.get('chat_config:1:2'), {'other': 'keep', 'mcp_servers': ['personal']})
        self.assertEqual(self.store.get('chat_config:1:3'), {'other': 'keep', 'mcp_servers': []})
        self.store.put('chat_config:1:2', {'mcp_servers': []})
        migrate(self.store, {'1:2': ['personal']})
        self.assertEqual(selection(self.store, '1:2'), [])

    def test_chats_inherit_tag_defaults_unless_they_override(self):
        self.store.put('chat_tags:1:2', ['personal_agent'])
        self.store.put('chat_tags:1:3', ['personal_agent', 'extra'])
        self.store.put('chat_tags:1:4', ['personal_agent'])
        op_tag_mcp(self.store, Context(), 'personal_agent', '["personal"]')
        op_tag_mcp(self.store, Context(), 'extra', '["work","personal"]')
        self.assertEqual(selection(self.store, '1:2'), ['personal'])
        self.assertEqual(selection(self.store, '1:3'), ['personal', 'work'])
        self.assertEqual(read_entries(self.store, topic='1:3'), {'personal': self.personal, 'work': self.work})
        op_chat_mcp(self.store, Context(), '1:2', '["work"]')
        op_chat_mcp(self.store, Context(), '1:4', '[]')
        self.assertEqual(selection(self.store, '1:2'), ['work'])
        self.assertEqual(selection(self.store, '1:4'), [])
        self.assertEqual(settings_text(self.store, '1:4'), '')
        self.assertFalse(op_chat_mcp(self.store, Context(), '1:4').data['inherited'])
        result = op_chat_mcp(self.store, Context(), '1:4', 'null')
        self.assertEqual(result.data, {'topic': '1:4', 'mcp_servers': ['personal'], 'inherited': True})
        self.assertEqual(settings_text(self.store, '1:4'), '\nMCP servers of this chat: `personal`.')
        self.assertEqual(op_tag_mcp(self.store, Context(), 'extra', '[]').data['mcp_servers'], [])
        self.assertEqual(selection(self.store, '1:3'), ['personal'])
        for tag, names in (('Bad Tag', '[]'), ('extra', '["torii"]'), ('extra', '["unknown"]')):
            with self.assertRaises(Refused):
                op_tag_mcp(self.store, Context(), tag, names)
        self.assertFalse(call(self.store, 'tag.mcp', {'tag': 'extra', 'names': '[]'}, source='mcp').ok)
        self.assertTrue(call(self.store, 'tag.mcp', {'tag': 'extra', 'names': '["work"]'}).ok)
        self.assertEqual(call(self.store, 'tag.mcp', {'tag': 'extra'}).data['mcp_servers'], ['work'])

    def test_workers_cannot_use_chat_mcp(self):
        job = self.store.task_create('1:2', 'Job')
        worker = self.store.db.execute('''INSERT INTO workers(task,topic,provider,prompt,created,updated)
            VALUES (?,?,?,?,?,?)''', (job['id'], '1:2', 'codex', 'Work', 1, 1)).lastrowid
        self.store.db.commit()
        for names in (None, '["personal"]'):
            with self.assertRaises(Refused):
                op_chat_mcp(self.store, Context(source_worker=worker), '1:2', names)
        result = call(self.store, 'chat.mcp', {'topic': '1:2', 'names': '["personal"]'}, source_worker=worker)
        self.assertFalse(result.ok)
        self.assertEqual(selection(self.store, '1:2'), [])
        self.assertTrue(call(self.store, 'chat.mcp', {'topic': '1:2', 'names': '["work"]'}).ok)
        self.assertEqual(selection(self.store, '1:2'), ['work'])

    def test_cli_refuses_worker_environment_and_allows_owner(self):
        job = self.store.task_create('1:2', 'Job')
        worker = self.store.db.execute('''INSERT INTO workers(task,topic,provider,prompt,created,updated)
            VALUES (?,?,?,?,?,?)''', (job['id'], '1:2', 'codex', 'Work', 1, 1)).lastrowid
        self.store.db.commit()
        clean = {key: value for key, value in os.environ.items() if key not in ('TORII_WORKER_ID', 'TORII_TASK_ID')}
        clean['PYTHONPATH'] = str(Path(__file__).resolve().parents[1])
        command = [sys.executable, '-m', 'coordinator', '--state-dir', str(self.store.directory), 'ctl', 'call',
                   'chat.mcp', 'topic=1:2', 'names=["personal"]']
        refused = subprocess.run(command, env=dict(clean, TORII_WORKER_ID=str(worker), TORII_TASK_ID=str(job['id'])),
                                 capture_output=True, text=True, timeout=120)
        self.assertEqual(refused.returncode, 3, refused.stdout + refused.stderr)
        self.assertEqual(selection(self.store, '1:2'), [])
        allowed = subprocess.run(command, env=clean, capture_output=True, text=True, timeout=120)
        self.assertEqual(allowed.returncode, 0, allowed.stdout + allowed.stderr)
        self.assertEqual(selection(self.store, '1:2'), ['personal'])

    def test_codex_headers_use_environment_names(self):
        config, private = codex_launch({'personal': self.personal})
        wire = json.dumps(config)
        self.assertNotIn('DUMMY-PERSONAL', wire)
        name = config['personal']['env_http_headers']['Authorization']
        self.assertEqual(private[name], 'DUMMY-PERSONAL')
        self.assertEqual(config['personal']['url'], self.personal['url'])


class CodexInheritedTests(unittest.IsolatedAsyncioTestCase):
    async def test_native_servers_stay_on_and_a_taken_name_keeps_its_native_server(self):
        process = AsyncMock()
        process.returncode = 0
        process.communicate.return_value = (json.dumps([{'name': 'unselected'}, {'name': 'personal'}]).encode(), b'')
        with patch('coordinator.shared_mcp.asyncio.create_subprocess_exec', return_value=process):
            names = await codex_inherited('codex', {}, '.')
        self.assertEqual(names, {'personal', 'unselected'})
        with tempfile.TemporaryDirectory() as root:
            store = Store(Path(root) / 'state')
            self.addCleanup(store.close)
            kept = codex_selected(store, {'personal': {'url': 'x'}, 'work': {'url': 'y'}}, names, topic='1:2')
            self.assertEqual(kept, {'work': {'url': 'y'}})
            problem = store.db.execute("SELECT code, detail FROM problems WHERE area='shared-mcp'").fetchone()
            self.assertEqual(tuple(problem), ('codex-name-taken', 'entries=personal'))

    async def test_a_native_torii_server_still_blocks_the_coordinator(self):
        process = AsyncMock()
        process.returncode = 0
        process.communicate.return_value = (json.dumps([{'name': 'torii'}]).encode(), b'')
        with patch('coordinator.shared_mcp.asyncio.create_subprocess_exec', return_value=process):
            self.assertEqual(await codex_inherited('codex', {}, '.'), {'torii'})
            with self.assertRaises(RuntimeError):
                await codex_inherited('codex', {}, '.', coordinator=True)

    async def test_probe_failure_fails_closed_without_exposing_output(self):
        process = AsyncMock()
        process.returncode = 1
        process.communicate.return_value = (b'DUMMY-SECRET', b'DUMMY-SECRET')
        with patch('coordinator.shared_mcp.asyncio.create_subprocess_exec', return_value=process):
            with self.assertRaises(RuntimeError) as error:
                await codex_inherited('codex', {}, '.')
        self.assertNotIn('DUMMY-SECRET', str(error.exception))


class SharedMcpUpgradeTests(unittest.TestCase):
    """A Torii 2.0 install shared local-overrides.json shared_mcp_servers with every chat."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.home = self.root / 'home'
        self.home.mkdir()
        env = patch.dict(os.environ, {'HOME': str(self.home)})
        env.start()
        self.addCleanup(env.stop)
        self.overrides = self.root / 'local-overrides.json'
        path = patch('coordinator.accounts.LOCAL_OVERRIDES_PATH', self.overrides)
        path.start()
        self.addCleanup(path.stop)
        self.entry = {'type': 'http', 'url': 'https://example.invalid/mcp', 'headers': {'Authorization': 'DUMMY'}}
        (self.home / '.claude.json').write_text(json.dumps({'mcpServers': {'example-mcp': self.entry}}))

    def version_2_0(self, name, *settings):
        directory = self.root / name
        directory.mkdir()
        db = sqlite3.connect(directory / 'state.sqlite')
        db.executescript((Path(__file__).parent / 'fixtures/store-2.0.sql').read_text())
        db.executemany('INSERT INTO settings(key,value) VALUES (?,?)', settings)
        db.commit()
        db.close()
        return directory

    def open(self, directory):
        store = Store(directory)
        self.addCleanup(store.close)
        return store

    def test_upgrade_keeps_the_shared_servers_in_every_existing_chat(self):
        self.overrides.write_text(json.dumps({'shared_mcp_servers': ['example-mcp', 'torii'],
                                              'account_switch_thresholds': {}}))
        directory = self.version_2_0('state')
        store = self.open(directory)
        for topic in ('-1001073001:2', '-1001073001:7'):
            self.assertEqual(selection(store, topic), ['example-mcp'])
            self.assertEqual(read_entries(store, topic=topic), {'example-mcp': self.entry})
        op_chat_mcp(store, Context(), '-1001073001:7', '[]')
        store.close()
        store = self.open(directory)
        self.assertEqual(selection(store, '-1001073001:7'), [])
        self.assertEqual(selection(store, '-1001073001:2'), ['example-mcp'])

    def test_upgrade_without_shared_servers_selects_nothing(self):
        for index, content in enumerate((None, '{}', '{"shared_mcp_servers": "example-mcp"}', 'not json')):
            with self.subTest(content=content):
                if content is not None:
                    self.overrides.write_text(content)
                store = self.open(self.version_2_0('state-' + str(index)))
                self.assertEqual(selection(store, '-1001073001:2'), [])
                self.assertIsNone(store.get('chat_mcp_initial_v1'))

    def test_an_install_that_already_migrated_keeps_its_selections(self):
        self.overrides.write_text(json.dumps({'shared_mcp_servers': ['example-mcp']}))
        store = self.open(self.version_2_0('state', ('chat_mcp_initial_v1', 'true')))
        self.assertEqual(selection(store, '-1001073001:2'), [])
