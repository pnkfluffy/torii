import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from coordinator import control_api, mcp
from coordinator.store import Store
from coordinator.mcp import tools_list


class McpTests(unittest.TestCase):
    def test_read_hints_match_operation_kinds_including_coordinator_reads(self):
        tools = {tool['name']: tool for tool in tools_list()}
        for name in ('tasks_get', 'tasks_list', 'workers_list', 'secret_list'):
            with self.subTest(tool=name):
                self.assertTrue(tools[name]['annotations']['readOnlyHint'])
        for name, op in mcp.TOOLS.items():
            with self.subTest(operation=op.id):
                self.assertEqual(tools[name]['annotations']['readOnlyHint'], op.kind == control_api.READ)

    def test_annotations_and_fingerprint_include_tool_safety(self):
        from unittest.mock import patch
        tools = tools_list()
        for tool in tools:
            op = mcp.TOOLS[tool['name']]
            self.assertEqual(tool['annotations'], {'readOnlyHint': op.kind == control_api.READ,
                                                   'destructiveHint': False, 'openWorldHint': False})
        original = mcp.tools_hash()
        tools[0]['annotations']['readOnlyHint'] = not tools[0]['annotations']['readOnlyHint']
        with patch('coordinator.mcp.tools_list', return_value=tools):
            self.assertNotEqual(mcp.tools_hash(), original)

    def test_telegram_send_schema_accepts_optional_image(self):
        tool = next(tool for tool in tools_list() if tool['name'] == 'telegram_send')
        schema = tool['inputSchema']
        self.assertEqual(schema['properties']['image'], {'type': 'string'})
        self.assertNotIn('image', schema['required'])

    def test_stdio_server_creates_and_lists_a_task(self):
        with tempfile.TemporaryDirectory() as root:
            store = Store(Path(root))
            with store.db:
                store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test','',1)")
            message = store.message_save('1:2', 'owner', 'Build it')['id']
            store.close()
            requests = [
                {'jsonrpc': '2.0', 'id': 1, 'method': 'initialize',
                 'params': {'protocolVersion': '2025-06-18', 'capabilities': {},
                            'clientInfo': {'name': 'test', 'version': '1'}}},
                {'jsonrpc': '2.0', 'method': 'notifications/initialized'},
                {'jsonrpc': '2.0', 'id': 2, 'method': 'tools/list'},
                {'jsonrpc': '2.0', 'id': 3, 'method': 'tools/call',
                 'params': {'name': 'tasks_create', 'arguments': {'message': message, 'title': 'Build'}}},
                {'jsonrpc': '2.0', 'id': 4, 'method': 'tools/call',
                 'params': {'name': 'tasks_list', 'arguments': {'topic': '1:2'}}},
            ]
            process = subprocess.Popen([sys.executable, '-m', 'coordinator', '--state-dir', root, 'mcp'],
                                       stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE, text=True)
            output, errors = process.communicate(''.join(json.dumps(row) + '\n' for row in requests), timeout=10)
            self.assertEqual(process.returncode, 0, errors)
            replies = [json.loads(line) for line in output.splitlines()]
            self.assertEqual([row['id'] for row in replies], [1, 2, 3, 4])
            self.assertEqual(replies[0]['result']['protocolVersion'], '2025-06-18')
            names = {tool['name'] for tool in replies[1]['result']['tools']}
            self.assertIn('tasks_create', names)
            self.assertIn('tasks_list', names)
            self.assertIn('"number":1', replies[2]['result']['content'][0]['text'])
            self.assertIn('"topic":"1:2"', replies[2]['result']['content'][0]['text'])
            self.assertIn('Build', replies[3]['result']['content'][0]['text'])


class McpSchemaTests(unittest.TestCase):
    def test_task_tools_describe_secret_declarations(self):
        import re
        from coordinator import mcp
        tools = {tool['name']: tool['inputSchema'] for tool in mcp.tools_list()}
        for name in ('tasks_create', 'tasks_update'):
            schema = tools[name]['properties']['secrets']
            self.assertEqual(schema['type'], 'string')
            pattern = re.compile(schema['pattern'])
            for valid in ('', 'GITHUB_TOKEN', 'GH_TOKEN=GITHUB_TOKEN_ORG, NPM_TOKEN'):
                self.assertIsNotNone(pattern.search(valid), valid)
            for invalid in ('lower', 'A', 'GH_TOKEN=', 'A_KEY;B_KEY'):
                self.assertIsNone(pattern.search(invalid), invalid)


class ToolSetTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name))
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test','',1)")
            self.store.put('accounts', {'work': {'config_dir': None, 'enabled': True},
                                        'other': {'config_dir': None, 'enabled': True}})

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def call(self, name, arguments=None):
        return mcp.dispatch(self.store, {'method': 'tools/call',
                                         'params': {'name': name, 'arguments': arguments or {}}})

    def test_tools_list_is_the_coordinator_ops_plus_read_only_views(self):
        names = {tool['name'] for tool in mcp.dispatch(self.store, {'method': 'tools/list'})['tools']}
        self.assertEqual(names, {
            'telegram_send', 'tasks_create', 'tasks_update', 'tasks_get', 'tasks_list', 'worktree_create',
            'workers_spawn', 'workers_steer', 'workers_stop', 'workers_goal', 'workers_list',
            'service_restart', 'settings_show', 'topics_list', 'topic_show', 'workers_get',
            'accounts_list', 'account_current', 'account_show', 'policy_show', 'projects_list',
            'secret_ask', 'secret_list', 'secret_rotate', 'secret_revoke', 'problems_list', 'problems_summary',
            'tasks_stale', 'account_use', 'account_redeem'})
        self.assertEqual(len(names), 30)
        for op_id in mcp.READ_OPS:
            self.assertEqual(control_api.BY_ID[op_id].kind, control_api.READ)

    def test_policy_show_returns_current_usage_file(self):
        policy = 'Use one worker.'
        (self.store.directory / 'USAGE.md').write_text(policy)
        reply = self.call('policy_show', {})
        self.assertEqual(reply['content'][0]['text'], policy + '\nnull')

    def test_owner_only_ops_are_not_callable_through_mcp(self):
        before = (self.store.directory / 'USAGE.md').read_bytes()
        for name, arguments in (('account_select', {'alias': 'other'}), ('account_disable', {'alias': 'work'}),
                                ('policy_set', {'text': 'Always use codex'}), ('delegation_codex', {'enabled': 'false'}),
                                ('topic_bind', {'topic': '1:2', 'cwd': self.temp.name, 'name': 'x'}),
                                ('model_worker', {'model': 'haiku'}), ('accounts_discover', {}),
                                ('accounts_codex_auto', {'enabled': 'true'})):
            with self.assertRaisesRegex(ValueError, 'Unknown tool'):
                self.call(name, arguments)
        self.assertTrue(self.store.get('accounts')['work']['enabled'])
        self.assertEqual((self.store.directory / 'USAGE.md').read_bytes(), before)
        self.assertEqual(self.store.topic('1:2')['name'], 'test')

    def test_read_views_and_workers_get_answer_through_mcp(self):
        task = self.store.task_create('1:2', 'Build', worktree=self.temp.name)
        spawned = self.call('workers_spawn', {'task': task['id'], 'provider': 'claude', 'prompt': 'Go'})
        self.assertFalse(spawned['isError'])
        worker = json.loads(spawned['content'][0]['text'].split('\n', 1)[1])['worker']
        got = self.call('workers_get', {'worker': worker})
        self.assertFalse(got['isError'])
        self.assertIn('"prompt":"Go"', got['content'][0]['text'])
        for name in ('settings_show', 'topics_list', 'account_current', 'policy_show', 'accounts_list'):
            self.assertFalse(self.call(name)['isError'], name)

    def test_tasks_create_through_mcp_needs_the_asking_message_and_keeps_its_topic(self):
        schema = next(tool for tool in mcp.tools_list() if tool['name'] == 'tasks_create')['inputSchema']
        self.assertEqual(schema['required'], ['title'])
        self.assertEqual(schema['properties']['message'], {'type': 'integer'})
        self.assertEqual(schema['properties']['cross_topic'], {'type': 'boolean'})
        self.assertIn('message=N', next(tool for tool in mcp.tools_list() if tool['name'] == 'tasks_create')['description'])
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:3',1,3,'test 2','',1)")
        asked = self.store.message_save('1:3', 'owner', 'Plan it')['id']
        refused = self.call('tasks_create', {'topic': '1:3', 'title': 'Plan'})
        self.assertTrue(refused['isError'])
        crossed = self.call('tasks_create', {'message': asked, 'topic': '1:2', 'title': 'Plan'})
        self.assertTrue(crossed['isError'])
        created = self.call('tasks_create', {'message': asked, 'title': 'Plan'})
        self.assertFalse(created['isError'])
        self.assertEqual([task['topic'] for task in self.store.tasks_list()], ['1:3'])
        listed = self.call('tasks_list', {'topic': '1:2'})
        self.assertIn('No jobs.', listed['content'][0]['text'])
