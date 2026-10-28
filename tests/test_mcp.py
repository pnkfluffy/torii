import asyncio
import json
from io import StringIO
import os
from pathlib import Path
import select
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from coordinator import control_api, log, mcp
from coordinator.store import Store
from coordinator.mcp import tools_list


class CopiedMcpServer:
    def __init__(self, directory):
        self.root = directory / 'project'
        shutil.copytree(Path(__file__).resolve().parents[1] / 'coordinator', self.root / 'coordinator',
                        ignore=shutil.ignore_patterns('__pycache__'))
        (self.root / 'scripts').mkdir()
        shutil.copy2(Path(__file__).resolve().parents[1] / 'scripts' / 'update-watchdog.py',
                     self.root / 'scripts' / 'update-watchdog.py')
        self.store = Store(directory / 'state')
        self.process = subprocess.Popen(
            [sys.executable, '-m', 'coordinator', '--state-dir', str(self.store.directory), 'mcp'],
            cwd=self.root, env=dict(os.environ, PYTHONPATH=str(self.root)),
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            start_new_session=True)
        self.sequence = 0
        self.pending = b''

    def request(self, method, params=None):
        self.sequence += 1
        request = {'jsonrpc': '2.0', 'id': self.sequence, 'method': method}
        if params is not None:
            request['params'] = params
        self.process.stdin.write(json.dumps(request) + '\n')
        self.process.stdin.flush()
        deadline = time.monotonic() + 30
        while b'\n' not in self.pending:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not select.select([self.process.stdout], [], [], remaining)[0]:
                raise AssertionError('MCP reply timed out after 30 seconds')
            chunk = os.read(self.process.stdout.fileno(), 65536)
            if not chunk:
                raise AssertionError('MCP server closed stdout before replying')
            self.pending += chunk
        line, self.pending = self.pending.split(b'\n', 1)
        response = json.loads(line)
        if response['id'] != self.sequence:
            raise AssertionError('MCP reply id does not match the request')
        return response

    def probe(self):
        return self.request('tools/call', {'name': 'tasks_get', 'arguments': {'task': 999999}})

    def edit(self):
        path = self.root / 'coordinator' / 'control_api.py'
        path.write_text(path.read_text().replace('Unknown job.', 'Unknown job (current code).'))

    def record_service(self, stamp):
        with self.store.db:
            self.store.put('service_run', {'state': 'running', 'started': 1.0, 'code': stamp})

    def close(self):
        self.process.stdin.close()
        try:
            self.process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            pass
        finally:
            try:
                os.killpg(self.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.process.wait(timeout=30)
            self.process.stdout.close()
            self.process.stderr.close()
            self.store.close()


def retained_tool_arguments(folder):
    return {
        'telegram_send': {'topic': '1:2', 'text': 'Report', 'task': 1, 'image': str(folder / 'image.png')},
        'tasks_create': {'title': 'Build', 'secrets': 'SAMPLE_KEY'},
        'tasks_update': {'task': 999999, 'notes': 'Updated'},
        'tasks_get': {'task': 1},
        'tasks_list': {'topic': '1:2'},
        'tasks_wait': {'topic': '1:2', 'kind': 'owner', 'ask': 'Choose the next step', 'key': 'owner:retained'},
        'pending_list': {'owner': True},
        'pending_fire': {'wait': 2, 'message': 1},
        'pending_cancel': {'wait': 2, 'reason': 'No longer needed'},
        'tasks_stale': {'hours': 1000000},
        'worktree_create': {'task': 2},
        'workers_spawn': {'task': 1, 'provider': 'codex', 'prompt': 'Build'},
        'workers_steer': {'worker': 1, 'prompt': 'Continue'},
        'workers_stop': {'worker': 2},
        'workers_goal': {'worker': 2, 'condition': 'Finish'},
        'workers_list': {'task': 1},
        'workers_get': {'worker': 1},
        'service_restart': {'reason': 'Test restart request'},
        'secret_ask': {'topic': '1:2', 'name': 'SAMPLE_KEY', 'reason': 'Build', 'consumer': 'test'},
        'secret_list': {},
        'secret_rotate': {'name': 'SAMPLE_KEY'},
        'secret_revoke': {'name': 'SAMPLE_KEY'},
        'account_use': {'alias': 'codex-test'},
        'accounts_claude_auto': {},
        'account_redeem': {},
        'settings_show': {'topic': '1:2'},
        'topics_list': {},
        'topic_show': {'topic': '1:2'},
        'topic_create': {'name': 'Research', 'message': 1},
        'model_parent': {'topic': '1:2', 'message': 1},
        'policy_chat': {'topic': '1:2', 'message': 1, 'text': 'Use Sol.'},
        'topic_close': {'topic': '1:2', 'message': 1},
        'accounts_list': {},
        'account_current': {},
        'account_show': {'alias': 'test'},
        'policy_show': {},
        'projects_list': {},
        'problems_list': {},
        'problems_summary': {'since': '2026-01-01T00:00:00Z'},
    }


def seed_retained_tools(server, folder):
    folder.mkdir(exist_ok=True)
    (folder / 'image.png').write_bytes(b'image')
    store = server.store
    (store.directory / 'USAGE.md').write_text('Test usage policy.\n')
    with store.db:
        store.put('waits_enabled', True)
        store.put('owner', 1)
        store.put('group', 1)
        store.put('bot_username', 'fixture_bot')
        store.put('projects_root', str(folder))
        store.put('accounts', {'test': {'config_dir': None, 'enabled': True}})
        store.put('codex_accounts', {'codex-test': {'config_dir': None, 'enabled': False}})
        store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test',?,1)",
                         (str(folder),))
        store.db.execute("INSERT INTO messages(id,topic,kind,telegram_message,text,created,updated) "
                         "VALUES (1,'1:2','owner',10,'Build',1,1)")
        for task, worktree in ((1, folder), (2, folder / 'missing')):
            store.db.execute('INSERT INTO tasks(id,topic,number,title,worktree,origin,created,updated) '
                             "VALUES (?,'1:2',?,'Build',?,1,1,1)", (task, task, str(worktree)))
        store.db.execute("INSERT INTO task_stamps(task,stage,created) VALUES (1,'ready',1)")
        for worker, status, fresh in ((1, 'done', 0), (2, 'running', 1)):
            store.db.execute('INSERT INTO workers(id,task,topic,provider,prompt,session,status,fresh,'
                             'account_alias,created,updated) '
                             "VALUES (?,1,'1:2','codex','Build','fixture-session',?,?,'test',1,1)",
                             (worker, status, fresh))
        store.db.execute('INSERT INTO envelopes(name,reason,consumer,topic,state,created,expires,updated) '
                         "VALUES ('SAMPLE_KEY','Build','test','1:2','cancelled',1,2,1)")
        store.db.execute('INSERT INTO envelopes(name,reason,consumer,topic,state,created,expires,updated) '
                         "VALUES ('CLAUDE_SIGNIN_CODE','Sign in','test','1:2','open',1,2,1)")
        store.db.execute("INSERT INTO outbox(id,topic,kind,text) VALUES (1,'1:2','wait-ask','Choose the next step')")
        store.db.execute('''INSERT INTO waits(id,kind,key,topic,ask,detail,ask_outbox,state,created,updated)
            VALUES (1,'owner','owner:retained','1:2','Choose the next step','{"buttons":[]}',1,'armed',1,1)''')
        store.db.execute('''INSERT INTO waits(id,kind,key,topic,ask,detail,state,outcome,fired_by,created,updated)
            VALUES (2,'owner','owner:retained-fired','1:2','Earlier decision','{"buttons":[]}',
                'fired','answered','1',1,1)''')
    server.record_service(log.code_stamp(server.root))


class McpRetainedCodeTests(unittest.TestCase):
    def test_first_tool_calls_keep_startup_code_after_source_swap(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            arguments = retained_tool_arguments(root / 'data')
            self.assertEqual(set(arguments), {tool['name'] for tool in tools_list()})
            for name, params in arguments.items():
                with self.subTest(tool=name):
                    servers = []
                    try:
                        for side in ('original', 'swapped'):
                            server = CopiedMcpServer(root / name / side)
                            servers.append(server)
                            seed_retained_tools(server, root / 'data')
                            self.assertIn('result', server.request('initialize'))
                            listed = server.request('tools/list')['result']['tools']
                            self.assertEqual(set(arguments), {tool['name'] for tool in listed})
                        before, swapped = servers
                        startup = log.code_stamp(swapped.root)
                        for path in (swapped.root / 'coordinator').glob('*.py'):
                            path.write_text("raise ImportError('source replaced during deploy')\n")
                        for cache in (swapped.root / 'coordinator').rglob('__pycache__'):
                            shutil.rmtree(cache)
                        self.assertNotEqual(log.code_stamp(swapped.root), startup)
                        self.assertEqual(swapped.store.get('service_run')['code'], startup)
                        request = {'name': name, 'arguments': params}
                        expected = before.request('tools/call', request)
                        actual = swapped.request('tools/call', request)
                        self.assertIn('result', expected)
                        if name in ('tasks_wait', 'pending_list', 'pending_fire', 'pending_cancel'):
                            self.assertFalse(expected['result'].get('isError', False), expected)
                            data = json.loads(expected['result']['content'][0]['text'].rsplit('\n', 1)[1])
                            if name == 'pending_list':
                                self.assertEqual(([item['id'] for item in data['waits']], data['held']), ([1], []))
                            else:
                                self.assertEqual((data['id'], data['state']),
                                                 (1, 'armed') if name == 'tasks_wait' else (2, 'fired'))
                        if name in ('tasks_get', 'workers_spawn'):
                            for response, server in ((expected, before), (actual, swapped)):
                                content = response['result']['content'][0]
                                text, payload = content['text'].rsplit('\n', 1)
                                data = json.loads(payload)
                                self.assertEqual(data['results'], str(server.store.directory / 'results/task-1'))
                                data['results'] = '<results>'
                                content['text'] = text + '\n' + json.dumps(data, separators=(',', ':'))
                        self.assertEqual(actual, expected)
                        self.assertIsNone(swapped.process.poll())
                        self.assertIn('result', swapped.request('initialize'))
                    finally:
                        for server in reversed(servers):
                            server.close()


class McpFreshCodeTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.server = CopiedMcpServer(Path(directory.name))
        self.addCleanup(self.server.close)
        self.assertIn('result', self.server.request('initialize'))

    def assert_probe(self, text):
        reply = self.server.probe()
        self.assertTrue(reply['result']['isError'])
        self.assertEqual(reply['result']['content'][0]['text'], text + '\nnull')

    def test_a_fresh_code_matches_the_running_service(self):
        self.assert_probe('Unknown job.')
        self.server.edit()
        self.server.record_service(log.code_stamp(self.server.root))
        self.assert_probe('Unknown job (current code).')

    def test_topic_handler_stays_on_startup_code_until_service_matches(self):
        (self.server.root / 'coordinator' / 'topics.py').write_text('def broken(:\n')
        reply = self.server.request('tools/call', {'name': 'topic_create',
                                                 'arguments': {'name': 'Research', 'message': 1}})
        self.assertIn('result', reply, reply)
        self.assertTrue(reply['result']['isError'])
        self.assertIn('scoped MCP tools', reply['result']['content'][0]['text'])

    def test_c_pulled_code_waits_for_the_service_record(self):
        earlier = log.code_stamp(self.server.root)
        self.server.edit()
        self.server.record_service(earlier)
        self.assert_probe('Unknown job.')
        with self.server.store.db:
            self.server.store.put('service_run', {'state': 'exited', 'cause': 'signal', 'ended': 1.0})
        self.assert_probe('Unknown job.')
        with self.server.store.db:
            self.server.store.put('service_run', None)
        self.assert_probe('Unknown job.')

    def test_e_broken_current_code_returns_an_error_and_keeps_serving(self):
        startup_stamp = log.code_stamp(self.server.root)
        (self.server.root / 'coordinator' / 'control_api.py').write_text('def broken(:\n')
        self.server.record_service(log.code_stamp(self.server.root))
        reply = self.server.probe()
        self.assertEqual(reply['error']['code'], -32603)
        self.assertTrue(reply['error']['message'].startswith('Tool call failed in the current Torii code:'))
        self.assertIn('SyntaxError', reply['error']['message'])
        self.assertIn('result', self.server.request('tools/list'))
        self.server.record_service(startup_stamp)
        self.assert_probe('Unknown job.')
        self.assertIsNone(self.server.process.poll())


class McpTests(unittest.TestCase):
    def test_ready_file_written_only_when_asked(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / 'state')
            self.addCleanup(store.close)
            ready = Path(directory) / 'mcp-ready-test'
            responses = StringIO()
            original = mcp.dispatch
            observations = []
            def dispatch(store, request):
                observations.append((request['method'], ready.exists()))
                if request.get('params', {}).get('fail'):
                    raise ValueError('tools/list failed')
                return original(store, request)
            def requests():
                for request_id, (method, fail) in enumerate((('initialize', False), ('tools/list', True),
                                                            ('tools/list', False), ('tools/list', False),
                                                            ('tools/call', False)), 1):
                    yield json.dumps({'jsonrpc': '2.0', 'id': request_id, 'method': method, 'params': {'fail': fail}}) + '\n'
                    response = json.loads(responses.getvalue().splitlines()[-1])
                    self.assertEqual(response['id'], request_id)
                    self.assertEqual(ready.exists(), request_id >= 3)
                    if request_id == 3:
                        self.assertEqual((ready.read_text(), ready.stat().st_mode & 0o777), ('{}', 0o600))
                        ready.write_text('written once')
            with patch.dict(os.environ, {'TORII_MCP_READY': str(ready)}), patch.object(mcp, 'dispatch', dispatch):
                mcp.serve(store, None, requests(), responses)
            self.assertEqual(ready.read_text(), 'written once')
            self.assertEqual(observations, [('initialize', False), ('tools/list', False), ('tools/list', False),
                                            ('tools/list', True), ('tools/call', True)])
            second = Path(directory) / 'without-variable'
            with patch.dict(os.environ, {}, clear=True):
                mcp.serve(store, None, StringIO(json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': 'tools/list'}) + '\n'),
                          StringIO())
            self.assertFalse(second.exists())
            ready.rename(second)
            with patch.dict(os.environ, {'TORII_MCP_READY': str(ready)}):
                mcp.serve(store, None, StringIO(json.dumps({'jsonrpc': '2.0', 'id': 2, 'method': 'tools/call'}) + '\n'),
                          StringIO())
            self.assertFalse(ready.exists())

    def test_stdio_server_uses_async_control_calls(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / 'state')
            self.addCleanup(store.close)
            with store.db:
                store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('1:2',1,2,'Proofs',?)",
                                 (directory,))
            task = store.task_create('1:2', 'Finish this', finish='merged', worktree=directory)
            request = {'jsonrpc': '2.0', 'id': 1, 'method': 'tools/call', 'params': {
                'name': 'tasks_update', 'arguments': {'task': task['id'], 'status': 'done',
                    'pr': 'https://github.com/example/project/pull/12'}}}
            target = StringIO()
            with patch.object(control_api, 'call', side_effect=AssertionError('sync control call')), \
                    patch.object(control_api, 'call_async', wraps=control_api.call_async) as caller, \
                    patch('coordinator.workspaces._command') as command:
                mcp.serve(store, None, StringIO(json.dumps(request) + '\n'), target)
            response = json.loads(target.getvalue())
            self.assertFalse(response['result']['isError'], response)
            self.assertEqual(store.task_get(task['id'])['status'], 'done')
            caller.assert_awaited_once()
            command.assert_not_called()

    def test_stdio_server_preserves_worktree_lookup(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / 'state')
            self.addCleanup(store.close)
            with store.db:
                store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('1:2',1,2,'Work',?)",
                                 (directory,))
            task = store.task_create('1:2', 'Existing worktree', worktree=directory)
            request = {'jsonrpc': '2.0', 'id': 1, 'method': 'tools/call', 'params': {
                'name': 'worktree_create', 'arguments': {'task': task['id']}}}
            target = StringIO()
            mcp.serve(store, None, StringIO(json.dumps(request) + '\n'), target)
            response = json.loads(target.getvalue())
            self.assertFalse(response['result']['isError'], response)
            data = json.loads(response['result']['content'][0]['text'].rsplit('\n', 1)[1])
            self.assertEqual(data['worktree'], directory)
            self.assertEqual(data['base_ref'], 'HEAD')

    def test_d7_chat_policy_tool_schema_description_and_loading(self):
        listed = {tool['name']: tool for tool in tools_list()}
        tool = listed['policy_chat']
        self.assertEqual(tool['description'],
            "Set or clear one chat's own delegation instructions: how its agent delegates work. They add to the usage "
            'policy and win where the two conflict, in that chat only. Coordinator rules and owner-only controls stay in force. '
            'Over MCP, topic and message=N for the owner message in the asking channel are required. '
            'Send text (at most 2000 characters) to set it; omit text to clear it. '
            "The chat's agent loads them with its next message. Workers keep their settings.")
        self.assertEqual(tool['inputSchema']['required'], ['message', 'topic'])
        self.assertNotIn('_meta', tool)
        self.assertIn('topic', listed['policy_show']['inputSchema']['properties'])
        self.assertNotIn('topic', listed['policy_show']['inputSchema']['required'])
        self.assertEqual(mcp.ALWAYS_LOADED_TOOLS, {'telegram_send', 'tasks_create', 'tasks_update', 'tasks_get',
            'tasks_wait', 'workers_spawn', 'workers_list', 'workers_steer', 'workers_get', 'worktree_create', 'accounts_list'})

    def test_request_exceptions_return_errors_and_keep_serving(self):
        for error_type in (ImportError, RuntimeError, OSError, SystemExit, KeyboardInterrupt):
            with self.subTest(error=error_type.__name__), tempfile.TemporaryDirectory() as root:
                store = Store(Path(root))
                try:
                    requests = [
                        {'jsonrpc': '2.0', 'id': 1, 'method': 'tools/call',
                         'params': {'name': 'workers_steer', 'arguments': {'worker': 1, 'prompt': 'Continue'}}},
                        {'jsonrpc': '2.0', 'id': 2, 'method': 'initialize'},
                    ]
                    target = StringIO()
                    with patch('coordinator.control_api._service_call', side_effect=error_type('handler failed')):
                        mcp.serve(store, log.code_stamp(Path(mcp.__file__).resolve().parents[1]),
                                  StringIO(''.join(json.dumps(row) + '\n' for row in requests)), target)
                    replies = [json.loads(line) for line in target.getvalue().splitlines()]
                    self.assertEqual(replies[0], {'jsonrpc': '2.0', 'id': 1, 'error': {
                        'code': -32602, 'message': 'handler failed'}})
                    self.assertEqual(replies[1]['id'], 2)
                    self.assertEqual(replies[1]['result']['serverInfo']['name'], 'torii')
                finally:
                    store.close()

    def test_c5_hot_reload_child_preserves_negative_topic_scope(self):
        with tempfile.TemporaryDirectory() as root:
            store = Store(Path(root))
            self.addCleanup(store.close)
            with store.db:
                store.put('group', -10042)
                store.put('mode', 'group')
                store.put('owner', 42)
                for thread in (2, 3):
                    store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                                     ('-10042:' + str(thread), -10042, thread, 'Parent', root))
            message = store.message_save('-10042:2', 'owner', 'Create a research chat')['id']
            request = json.dumps({'jsonrpc': '2.0', 'id': 7, 'method': 'tools/call', 'params': {
                'name': 'topic_create', 'arguments': {'name': 'Research', 'message': message}}}) + '\n'
            project = Path(__file__).resolve().parents[1]
            response = mcp.current_code_response(store, project, request, 7, topic='-10042:2')
            self.assertIn('result', response, response)
            self.assertFalse(response['result']['isError'], response)
            queued = store.db.execute('SELECT params FROM service_requests WHERE op=?', ('topic.create',)).fetchone()
            self.assertEqual(json.loads(queued['params'])['_topic'], '-10042:2')
            response = mcp.current_code_response(store, project, request, 7, topic='-10042:3')
            self.assertTrue(response['result']['isError'], response)
            self.assertIn('another channel', response['result']['content'][0]['text'])

    def test_j_serve_uses_the_stamp_from_process_start(self):
        request = {'jsonrpc': '2.0', 'id': 7, 'method': 'tools/call',
                   'params': {'name': 'tasks_get', 'arguments': {'task': 999999}}}
        line = json.dumps(request) + '\n'
        response = {'jsonrpc': '2.0', 'id': 7, 'result': {'current': True}}
        with tempfile.TemporaryDirectory() as root:
            store = Store(Path(root))
            self.addCleanup(store.close)
            with store.db:
                store.put('service_run', {'code': 'new'})
            target = StringIO()
            with patch('coordinator.mcp.code_stamp', return_value='new'), \
                    patch('coordinator.mcp.current_code_response', return_value=response) as child:
                mcp.serve(store, code='start', source=StringIO(line), target=target)
            child.assert_called_once_with(store, Path(mcp.__file__).resolve().parents[1], line, 7)
            self.assertEqual(json.loads(target.getvalue()), response)

    def test_k_current_child_dispatches_without_forking_again(self):
        request = {'jsonrpc': '2.0', 'id': 7, 'method': 'tools/call',
                   'params': {'name': 'tasks_get', 'arguments': {'task': 999999}}}
        with tempfile.TemporaryDirectory() as root:
            store = Store(Path(root))
            self.addCleanup(store.close)
            with store.db:
                store.put('service_run', {'code': 'new'})
            target = StringIO()
            with patch('coordinator.mcp.code_stamp', return_value='new'), \
                    patch('coordinator.mcp.current_code_response', side_effect=self.fail) as child:
                mcp.serve(store, code='new', source=StringIO(json.dumps(request) + '\n'), target=target)
            child.assert_not_called()
            self.assertEqual(json.loads(target.getvalue()), {'jsonrpc': '2.0', 'id': 7, 'result': {
                'content': [{'type': 'text', 'text': 'Unknown job.\nnull'}], 'isError': True}})

    def test_l_child_reply_requires_the_request_id_and_keeps_serving(self):
        following = '{"jsonrpc":"2.0","id":8,"method":"tools/list"}\n'
        with tempfile.TemporaryDirectory() as root:
            store = Store(Path(root))
            self.addCleanup(store.close)
            with store.db:
                store.put('service_run', {'code': 'new'})
            for request_id, stdout in ((7, '{}'), (7, '{"jsonrpc":"2.0","id":99,"result":{}}'),
                                       (None, '{}')):
                with self.subTest(request_id=request_id, stdout=stdout):
                    line = json.dumps({'jsonrpc': '2.0', 'id': request_id, 'method': 'tools/call'}) + '\n'
                    done = subprocess.CompletedProcess([], 0, stdout, '')
                    expected = {'jsonrpc': '2.0', 'id': request_id, 'error': {'code': -32603,
                                'message': 'Tool call failed in the current Torii code: exit code 0: '}}
                    with patch('coordinator.mcp.subprocess.run', return_value=done):
                        self.assertEqual(mcp.current_code_response(store, Path(root), line, request_id), expected)
                        target = StringIO()
                        with patch('coordinator.mcp.code_stamp', return_value='new'):
                            mcp.serve(store, code='start', source=StringIO(line + following), target=target)
                    replies = [json.loads(row) for row in target.getvalue().splitlines()]
                    self.assertEqual(replies[0], expected)
                    self.assertEqual(replies[1]['id'], 8)
                    self.assertIn('tools', replies[1]['result'])

    def test_child_failures_return_internal_errors(self):
        request = '{"jsonrpc":"2.0","id":7,"method":"tools/call"}\n'
        cases = ((0, '', ''), (0, '{broken', ''), (0, '[]', ''),
                 (0, '{}\n{}', ''), (9, '{}', '\nfirst\n' + 'last' * 60 + '\n\n'))
        with tempfile.TemporaryDirectory() as root:
            store = Store(Path(root) / 'state')
            self.addCleanup(store.close)
            for code, stdout, stderr in cases:
                with self.subTest(code=code, stdout=stdout, stderr=stderr):
                    done = subprocess.CompletedProcess([], code, stdout, stderr)
                    with patch('coordinator.mcp.subprocess.run', return_value=done) as run:
                        response = mcp.current_code_response(store, Path(root), request, 7)
                    last = next((row for row in reversed(stderr.splitlines()) if row), '')[:200]
                    self.assertEqual(response, {'jsonrpc': '2.0', 'id': 7, 'error': {
                        'code': -32603, 'message': 'Tool call failed in the current Torii code: '
                        + 'exit code %s: %s' % (code, last)}})
                    self.assertEqual(run.call_args.args[0], [sys.executable, '-m', 'coordinator',
                                     '--state-dir', str(store.directory), 'mcp'])
                    self.assertEqual(run.call_args.kwargs, {
                        'cwd': Path(root), 'env': dict(os.environ, PYTHONPATH=root),
                        'input': request, 'capture_output': True, 'text': True})
                    self.assertFalse(store.db.in_transaction)
            with patch('coordinator.mcp.subprocess.run', side_effect=OSError('cannot start child')):
                response = mcp.current_code_response(store, Path(root), request, 7)
            self.assertEqual(response['error'], {'code': -32603, 'message':
                             'Tool call failed in the current Torii code: cannot start child'})

    def test_stamp_errors_fall_back_to_dispatch(self):
        request = {'jsonrpc': '2.0', 'id': 1, 'method': 'tools/call',
                   'params': {'name': 'tasks_get', 'arguments': {'task': 999999}}}
        with tempfile.TemporaryDirectory() as root:
            store = Store(Path(root))
            self.addCleanup(store.close)
            for stamp in (OSError('scan failed'), None, 'startup'):
                with self.subTest(stamp=stamp), patch('coordinator.mcp.code_stamp', side_effect=[stamp]), \
                        patch('coordinator.mcp.current_code_response',
                              side_effect=AssertionError('stamp errors must dispatch in-process')):
                    target = StringIO()
                    mcp.serve(store, 'startup', StringIO(json.dumps(request) + '\n'), target)
                    response = json.loads(target.getvalue())
                    self.assertEqual(response['id'], 1)
                    self.assertEqual(response['result']['content'][0]['text'], 'Unknown job.\nnull')

    def test_read_hints_match_operation_kinds_including_coordinator_reads(self):
        tools = {tool['name']: tool for tool in tools_list()}
        for name in ('tasks_get', 'tasks_list', 'workers_list', 'secret_list', 'pending_list'):
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
                                                   'destructiveHint': op.id == 'topic.close', 'openWorldHint': False})
        original = mcp.tools_hash()
        tools[0]['annotations']['readOnlyHint'] = not tools[0]['annotations']['readOnlyHint']
        with patch('coordinator.mcp.tools_list', return_value=tools):
            self.assertNotEqual(mcp.tools_hash(), original)

    def test_telegram_send_schema_accepts_optional_image(self):
        tool = next(tool for tool in tools_list() if tool['name'] == 'telegram_send')
        schema = tool['inputSchema']
        self.assertEqual(schema['properties']['image'], {'type': 'string'})
        self.assertNotIn('image', schema['required'])

    def test_telegram_send_schema_accepts_optional_as_document(self):
        tool = next(tool for tool in tools_list() if tool['name'] == 'telegram_send')
        schema = tool['inputSchema']
        self.assertEqual(schema['properties']['as_document'], {'type': 'boolean'})
        self.assertNotIn('as_document', schema['required'])

    def test_worktree_create_schema_accepts_optional_base_branch(self):
        tool = next(tool for tool in tools_list() if tool['name'] == 'worktree_create')
        schema = tool['inputSchema']
        self.assertEqual(schema['properties']['base'], {'type': 'string'})
        self.assertEqual(schema['required'], ['task'])

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
    def test_tasks_update_lists_optional_note(self):
        tool = next(tool for tool in tools_list() if tool['name'] == 'tasks_update')
        schema = tool['inputSchema']
        self.assertEqual(schema['properties'].get('note'), {'type': 'string', 'maxLength': 16000})
        self.assertNotIn('note', schema['required'])
        self.assertIn('note appends one line to notes.', tool['description'])

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


class OpenCodePickTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        with self.store.db:
            self.store.put('owner', 1)
            self.store.put('group', 1)
            self.store.put('coordinator_home_topic', '1:2')
            self.store.put('opencode_enabled', True)
            self.store.put('opencode_accounts', {'example-host': {'model': 'example-model'}})
            self.store.put('opencode_active_account', 'example-host')
            for thread in (2, 3):
                self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES(?,?,?,?,?,1)',
                                     ('1:' + str(thread), 1, thread, 'Chat', str(self.root)))
        self.owner = self.store.message_save('1:2', 'owner', 'Pick OpenCode and create a chat.')['id']

    def call(self, name, arguments, topic='1:2'):
        return mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
            'name': name, 'arguments': arguments}}, topic=topic)

    def test_opencode_pick_over_mcp_needs_owner_message_from_this_chat(self):
        other = self.store.message_save('1:3', 'owner', 'Another chat.')['id']
        event = self.store.message_save('1:2', 'event', 'An agent event.')['id']
        for name, params in (
                ('model_parent', {'topic': '1:3', 'provider': 'opencode'}),
                ('topic_create', {'name': 'OpenCode', 'provider': 'opencode'})):
            for message, scope in ((None, '1:2'), (other, '1:2'), (event, '1:2'), (99999, '1:2'), (self.owner, None)):
                with self.subTest(tool=name, message=message, scope=scope):
                    before = [tuple(row) for row in self.store.db.execute('SELECT * FROM settings ORDER BY key')]
                    reply = self.call(name, dict(params, message=message), topic=scope)
                    self.assertTrue(reply['isError'], reply)
                    self.assertEqual([tuple(row) for row in self.store.db.execute('SELECT * FROM settings ORDER BY key')], before)
                    self.assertEqual(self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0], 0)
                    self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 0)
            reply = self.call(name, dict(params, message=self.owner))
            self.assertFalse(reply['isError'], reply)
            self.assertIn('OpenCode' if name == 'model_parent' else 'queued', reply['content'][0]['text'])

    def test_provider_enum_has_opencode_only_on_parent_tools(self):
        tools = {tool['name']: tool for tool in tools_list()}
        for name in ('model_parent', 'topic_create'):
            self.assertEqual(tools[name]['inputSchema']['properties']['provider']['enum'], ['claude', 'codex', 'opencode'])
            self.assertIn('OpenCode', tools[name]['description'])
        self.assertEqual(mcp.input_schema(control_api.BY_ID['topic.bind'])['properties']['provider']['enum'], ['claude', 'codex'])
        self.assertEqual(mcp.input_schema(control_api.BY_ID['effort.parent'])['properties']['provider']['enum'], ['claude', 'codex'])
        self.assertEqual(tools['workers_spawn']['inputSchema']['properties']['provider']['enum'],
                         ['claude', 'codex', 'opencode', 'muse'])
        self.assertIn('Muse workers are off by default.', tools['workers_spawn']['description'])
        self.assertNotIn('topic_bind', tools)

    def test_topic_create_opencode_writes_pick_and_delegation(self):
        from coordinator import topics
        from tests.test_group_setup import GroupTelegram
        from types import SimpleNamespace
        projects = self.root / 'projects'
        projects.mkdir()
        self.store.put('projects_root', str(projects))
        model = 'example-host/example-model'
        self.store.put('opencode_accounts', {'example-host': {'model': model}})
        params = {'name': 'OpenCode', 'provider': 'opencode', 'message': self.owner,
                  'delegation': '  Start only OpenCode (example-model) workers.  '}
        for setting, value, extra, expected in (
                ('opencode_enabled', False, {}, 'OpenCode is off. Turn on delegation.opencode first.'),
                ('opencode_active_account', None, {}, 'Select an OpenCode account with opencode.use first.'),
                ('opencode_enabled', True, {'effort': 'low'}, 'OpenCode chat agents have no thinking setting.'),
                ('opencode_enabled', True, {'model': 'wrong'},
                 'The OpenCode model comes from the account that opencode.use selects: ' + model + '.')):
            with self.subTest(setting=setting, value=value, extra=extra):
                previous = self.store.get(setting)
                self.store.put(setting, value)
                reply = self.call('topic_create', dict(params, **extra))
                self.assertTrue(reply['isError'], reply)
                self.assertEqual(reply['content'][0]['text'], expected + '\nnull')
                self.assertEqual(self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0], 0)
                self.assertEqual(list(projects.iterdir()), [])
                self.store.put(setting, previous)
        reply = self.call('topic_create', params)
        self.assertFalse(reply['isError'], reply)
        service = SimpleNamespace(store=self.store, telegram=GroupTelegram())
        request_id = json.loads(reply['content'][0]['text'].split('\n', 1)[1])['request']
        async def create():
            for unused in range(6):
                request = self.store.db.execute('SELECT * FROM service_requests WHERE id=?', (request_id,)).fetchone()
                if request['state'] == 'done':
                    break
                await topics.advance(service, request)
        asyncio.run(create())
        request = self.store.db.execute('SELECT * FROM service_requests WHERE id=?', (request_id,)).fetchone()
        self.assertEqual(request['state'], 'done', dict(request))
        topic = '1:' + str(service.telegram.thread)
        key = 'coordinator:' + topic
        self.assertEqual(self.store.get(key + '_selection'),
                         {'provider': 'opencode', 'model': None, 'effort': None, 'revision': 1})
        self.assertEqual(self.store.get(key + '_delegation'),
                         {'text': 'Start only OpenCode (example-model) workers.', 'revision': 1})
        ready = self.store.db.execute('SELECT text FROM outbox WHERE topic=? ORDER BY id DESC LIMIT 1', (topic,)).fetchone()[0]
        self.assertIn('Chat agent: OpenCode example-host ' + model + ' · picked for this chat.', ready)
        self.assertIn('Delegation updated.', ready)
        self.assertNotIn('Start only OpenCode (example-model) workers.', ready)
        self.assertIsNone(self.store.get('coordinator_selection'))


class ToolSetTests(unittest.TestCase):
    def test_coordinator_can_clear_owner_claude_pick(self):
        self.store.put('claude_account_pick', 'work')
        reply = self.call('accounts_claude_auto')
        self.assertIn('Claude account choice is automatic.', reply['content'][0]['text'])
        self.assertIsNone(self.store.get('claude_account_pick'))

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
            'tasks_stale', 'account_use', 'accounts_claude_auto', 'account_redeem', 'topic_create', 'topic_close', 'model_parent', 'policy_chat',
            'tasks_wait', 'pending_list', 'pending_fire', 'pending_cancel'})
        self.assertEqual(len(names), 39)
        for op_id in mcp.READ_OPS:
            self.assertEqual(control_api.BY_ID[op_id].kind, control_api.READ)

    def test_tools_list_keeps_only_eleven_tools_always_loaded(self):
        tools = mcp.dispatch(self.store, {'method': 'tools/list'})['tools']
        expected = {'telegram_send', 'tasks_create', 'tasks_update', 'tasks_get', 'tasks_wait',
                    'workers_spawn', 'workers_list', 'workers_steer', 'workers_get',
                    'worktree_create', 'accounts_list'}
        self.assertEqual({tool['name'] for tool in tools if '_meta' in tool}, expected)
        for tool in tools:
            with self.subTest(tool=tool['name']):
                if tool['name'] in expected:
                    self.assertEqual(tool['_meta'], {'anthropic/alwaysLoad': True})
                    self.assertIs(tool['_meta']['anthropic/alwaysLoad'], True)
                else:
                    self.assertNotIn('_meta', tool)

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

    def test_tasks_update_note_through_mcp_preserves_earlier_notes(self):
        task = self.store.task_create('1:2', 'Build', notes='a')
        reply = self.call('tasks_update', {'task': task['id'], 'note': 'b'})
        self.assertFalse(reply['isError'], reply)
        fetched = self.call('tasks_get', {'task': task['id']})
        self.assertFalse(fetched['isError'], fetched)
        data = json.loads(fetched['content'][0]['text'].split('\n', 1)[1])
        self.assertEqual(data['notes'], 'a\nb')

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
