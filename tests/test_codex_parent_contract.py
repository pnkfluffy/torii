"""Opt-in real Codex contract, without an account and with network restricted to loopback."""

import asyncio
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from coordinator.codex_accounts import AppServer
from coordinator.codex_session import CodexCoordinatorSession, codex_binary, codex_transcript_receipts, toml, transcript_path
from coordinator.session import message_tag, message_uuid
from coordinator.store import Store
from coordinator.providers import ProviderRunner
from coordinator.service import Service
from tests.support import until, stop_test_hosts


def exposed_tools(request):
    tools = list(request.get('tools', []))
    for item in request.get('input', []):
        if item.get('type') in ('additional_tools', 'tool_search_output'):
            tools.extend(item.get('tools', []))
    return [(namespace.get('name'), tool) if namespace.get('type') == 'namespace' else (None, namespace)
            for namespace in tools for tool in namespace.get('tools', [namespace])]


class ResponsesStub:
    def __init__(self):
        self.requests = []
        self.errors = []
        self.http = []
        self.calls = []
        self.started = threading.Event()
        self.release = threading.Event()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                owner.http.append({'method': 'GET', 'path': self.path, 'status': 404,
                                   'response': {'error': 'No remote catalog in this account-free contract'}})
                self.send_response(404)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(owner.http[-1]['response']).encode())

            def do_POST(self):
                try:
                    body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                    exchange = {'method': 'POST', 'path': self.path, 'request': body}
                    owner.http.append(exchange)
                    if self.path != '/v1/responses':
                        exchange.update(status=404, response={'error': 'Unexpected endpoint'})
                        self.send_response(404)
                        self.end_headers()
                        self.wfile.write(json.dumps(exchange['response']).encode())
                        return
                    owner.requests.append(body)
                    number = len(owner.requests)
                    if number == 1:
                        owner.started.set()
                        owner.release.wait(30)
                    call = not any(item.get('call_id', '').startswith('call-send-')
                                   and item.get('type', '').endswith('_output') for item in body.get('input', []))
                    if number > 1 and any('contract-second' in json.dumps(item) for item in body.get('input', [])):
                        call = False
                    if number > 6:
                        owner.errors.append('Script exceeded six Responses requests')
                        call = False
                    self.send_response(200)
                    exchange['status'] = 200
                    self.send_header('Content-Type', 'text/event-stream')
                    self.send_header('Connection', 'close')
                    self.end_headers()
                    response = {'id': 'resp_' + str(number), 'object': 'response', 'created_at': 1,
                                'status': 'in_progress', 'model': body.get('model', 'mock'), 'output': []}
                    events = [{'type': 'response.created', 'response': dict(response)}]
                    if call:
                        arguments = json.dumps({'topic': '1:1', 'text': 'contract-tool-executed'})
                        tools = exposed_tools(body)
                        direct = next(((namespace, tool) for namespace, tool in tools
                                       if tool.get('name') == 'telegram_send' and 'torii' in (namespace or '')), None)
                        executor = next(((namespace, tool) for namespace, tool in tools
                                         if tool.get('type') == 'custom' and tool.get('name') == 'exec'), None)
                        description = executor[1]['description'] if executor else ''
                        nested = re.search(r'### `([^`]*torii[^`]*telegram_send)`', description)
                        search = next((tool for namespace, tool in tools if tool.get('type') == 'tool_search'), None)
                        item = {'id': 'fc_' + str(number), 'status': 'completed'}
                        if direct:
                            namespace, tool = direct
                            item.update(type='function_call', call_id='call-send-' + str(number),
                                        namespace=namespace, name=tool['name'], arguments=arguments)
                        elif nested:
                            namespace, tool = executor
                            item.update(type='custom_tool_call', call_id='call-send-' + str(number),
                                        namespace=namespace, name=tool['name'],
                                        input='text(await tools.' + nested.group(1) + '(' + arguments + '));')
                        elif search:
                            item.update(type='tool_search_call', call_id='call-search-' + str(number),
                                        execution='client', arguments={'query': 'telegram_send', 'limit': 1})
                        elif executor:
                            namespace, tool = executor
                            item.update(type='custom_tool_call', call_id='call-send-' + str(number),
                                        namespace=namespace, name=tool['name'], input=(
                                            'const tool = ALL_TOOLS.find(({name}) => name.includes("torii") '
                                            '&& name.endsWith("telegram_send")); text({selectedTool: tool.name}); '
                                            'text(await tools[tool.name](' + arguments + '));'))
                        else:
                            raise AssertionError('Neither Torii send nor tool_search was advertised')
                        owner.calls.append(dict(item))
                        events.append({'type': 'response.output_item.added', 'output_index': 0, 'item': dict(item)})
                    else:
                        part = {'type': 'output_text', 'text': '', 'annotations': []}
                        item = {'type': 'message', 'id': 'msg_' + str(number), 'role': 'assistant',
                                'status': 'in_progress', 'content': []}
                        events.append({'type': 'response.output_item.added', 'output_index': 0, 'item': dict(item)})
                        events.append({'type': 'response.content_part.added', 'output_index': 0, 'content_index': 0,
                                       'item_id': item['id'], 'part': dict(part)})
                        events.append({'type': 'response.output_text.delta', 'output_index': 0, 'content_index': 0,
                                       'item_id': item['id'], 'delta': 'contract done'})
                        part['text'] = 'contract done'
                        events.append({'type': 'response.output_text.done', 'output_index': 0, 'content_index': 0,
                                       'item_id': item['id'], 'text': part['text']})
                        events.append({'type': 'response.content_part.done', 'output_index': 0, 'content_index': 0,
                                       'item_id': item['id'], 'part': dict(part)})
                        item.update(status='completed', content=[part])
                    events.append({'type': 'response.output_item.done', 'output_index': 0, 'item': item})
                    response.update(status='completed', output=[item],
                                    usage={'input_tokens': 10, 'output_tokens': 10, 'total_tokens': 20})
                    events.append({'type': 'response.completed', 'response': response})
                    exchange['response'] = events
                    for sequence, event in enumerate(events):
                        event['sequence_number'] = sequence
                        self.wfile.write(('event: ' + event['type'] + '\ndata: ' + json.dumps(event) + '\n\n').encode())
                        self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    pass
                except Exception as error:
                    owner.errors.append(type(error).__name__)
                finally:
                    self.close_connection = True

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.release.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


class ContractAppServer(AppServer):
    def __init__(self, binary, env):
        super().__init__(binary, env)
        self.events = []

    async def read(self):
        event = await super().read()
        self.events.append(event)
        if 'id' in event and 'method' in event:
            await self.send({'id': event['id'], 'error': {'code': -32601, 'message': 'Unsupported host request'}})
        return event


@unittest.skipUnless(os.environ.get('TORII_CODEX_CONTRACT') == '1', 'opt-in pinned Codex contract')
class CodexParentContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_stale_steer_requeues_then_answers_on_the_real_parent(self):
        with tempfile.TemporaryDirectory(prefix='torii-codex-stale-') as temporary:
            root = Path(temporary)
            self.addCleanup(stop_test_hosts, root)
            store = Store(root / 'state')
            self.addCleanup(store.close)
            binary, sandbox = codex_binary(store), shutil.which('sandbox-exec')
            self.assertTrue(binary and sandbox)
            mock = ResponsesStub()
            self.addCleanup(mock.close)
            home, account = root / 'home', root / 'codex'
            home.mkdir()
            account.mkdir()
            env = {'HOME': str(home), 'PATH': os.environ['PATH'],
                   'PYTHONPYCACHEPREFIX': str(root / 'bytecode')}
            settings = ['-c', 'cli_auth_credentials_store="file"', '-c', 'check_for_update_on_startup=false',
                        '-c', 'model_provider="mock"', '-c', 'model_providers.mock=' + toml({
                            'name': 'Mock', 'base_url': 'http://127.0.0.1:%d/v1' % mock.server.server_port,
                            'wire_api': 'responses', 'requires_openai_auth': False})]
            profile = ('(version 1)(allow default)(deny network*)'
                       '(allow network-outbound (remote ip "localhost:%d"))') % mock.server.server_port
            wrapper = root / 'pinned-codex'
            wrapper.write_text('#!/bin/sh\nexec ' + shlex.join([sandbox, '-p', profile, binary, *settings]) + ' "$@"\n')
            wrapper.chmod(0o700)
            with store.db:
                store.put('codex_accounts', {'mock': {'config_dir': str(account), 'enabled': True}})
                store.put('codex_account_status', {'mock': {'identity': {
                    'email': 'mock@example.test', 'logged_in': True}}})
                store.put('codex_model', 'gpt-6.1-sol')
                store.put('owner', 1)
                store.put('mode', 'group')
                store.put('group', 1)
                store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:1',1,1,'Contract',?,1)",
                                 (str(root),))
            runner = ProviderRunner(store.directory, {'codex': str(wrapper)})
            service = Service(store, None, runner, root)
            replies = []
            session = None
            rows = [store.message_save('1:1', 'owner', text) for text in
                    ('contract-first', 'contract-steer', 'contract-second-stale')]
            topic = store.topic('1:1')
            async def send(row):
                return await service._send_message(row, message_tag(topic, row) + row['text'], session)
            try:
                with patch.dict(os.environ, env, clear=True):
                    session = await asyncio.wait_for(CodexCoordinatorSession.start(
                        store, runner, root, 'ignored', 'TORII_CONTRACT_DEVELOPER_INSTRUCTIONS',
                        on_turn=replies.append), 60)
                    self.assertEqual(await send(rows[0]), 'received')
                    await until(mock.started.is_set, timeout=40)
                    active = session.protocol.turn_id
                    session.protocol.turn_id = 'stale-local-turn'
                    self.assertEqual(await send(rows[1]), 'received')
                    self.assertEqual(session.protocol.turn_id, active)
                    original = session.protocol.request
                    async def stale_request(method, params):
                        self.assertEqual(method, 'turn/start')
                        return await original('turn/steer', {**params, 'expectedTurnId': 'stale-local-turn'})
                    with patch.object(session.protocol, 'request', side_effect=stale_request):
                        self.assertEqual(await send(rows[2]), 'unsupported')
                    rejected = dict(store.db.execute('SELECT delivered,receipt FROM messages WHERE id=?',
                                                    (rows[2]['id'],)).fetchone())
                    self.assertEqual(rejected['delivered'], 'pending')
                    self.assertNotIn(rows[2]['id'], session._unreplayed)
                    events = [json.loads(line) for line in session.process.history_after(0)]
                    errors = [event['error'] for event in events if 'error' in event]
                    self.assertTrue(any('expected active turn id' in error.get('message', '') for error in errors))
                    mock.release.set()
                    await until(session.idle, timeout=90)
                    await session.detach()
                    session = await CodexCoordinatorSession.start(store, runner, root, 'ignored',
                        'TORII_CONTRACT_DEVELOPER_INSTRUCTIONS', on_turn=replies.append)
                    self.assertTrue(session.idle())
                    self.assertNotIn(rows[2]['id'], session._unreplayed)
                    self.assertEqual(await send(rows[2]), 'received')
                    await until(lambda: session.idle() and len(replies) == 2, timeout=60)
                    results = {'binary': binary, 'version': 'codex-cli 0.159.2',
                               'network': 'sandbox allows only the mock loopback port',
                               'rejection': errors, 'requeued': rejected, 'answered': replies,
                               'idle': session.idle(), 'unreplayed': sorted(session._unreplayed)}
                    events = [json.loads(line) for line in session.process.history_after(0)]
                    requests = [json.loads(line) for line in session.process.written_lines()]
                target = os.environ.get('TORII_CODEX_CONTRACT_OUTPUT')
                if target:
                    destination = Path(target) / 'stale_steer'
                    destination.mkdir(parents=True, exist_ok=True)
                    for name, value in (('results', results), ('notifications', events),
                                        ('requests', requests), ('http', mock.http)):
                        (destination / (name + '.json')).write_text(json.dumps(value, indent=2) + '\n')
                self.assertFalse(mock.errors, mock.errors)
                self.assertEqual(replies, ['contract done', 'contract done'])
            finally:
                mock.release.set()
                if session:
                    await session.stop()

    async def test_pinned_app_server_contract(self):
        for name, omissions, model in (('code_mode', ['deferred'], 'gpt-6.1-sol'),
                                       ('deferred', [], 'gpt-6.1-sol'),
                                       ('direct', ['deferred', 'code_mode'], 'gpt-6.1-sol'),
                                       ('deferred_search', [], 'gpt-5.5')):
            with self.subTest(configuration=name):
                await self.run_contract(name, omissions, model)

    async def run_contract(self, configuration, omissions, model):
        with tempfile.TemporaryDirectory(prefix='torii-codex-contract-') as temporary:
            root = Path(temporary)
            store = Store(root / 'state')
            self.addCleanup(store.close)
            binary = codex_binary(store)
            if not binary:
                self.skipTest('Pinned Codex binary is missing')
            sandbox = shutil.which('sandbox-exec')
            if not sandbox:
                self.fail('sandbox-exec is required to prevent non-loopback network access')
            mock = ResponsesStub()
            self.addCleanup(mock.close)
            home, account = root / 'home', root / 'codex'
            home.mkdir()
            account.mkdir()
            env = {'HOME': str(home), 'CODEX_HOME': str(account), 'PATH': os.environ.get('PATH', '/usr/bin:/bin'),
                   'PYTHONPYCACHEPREFIX': str(root / 'bytecode')}
            wrapper = root / 'pinned-codex'
            profile = ('(version 1)(allow default)(deny network*)'
                       '(allow network-outbound (remote ip "localhost:%d"))') % mock.server.server_port
            wrapper.write_text('#!/bin/sh\nexec ' + shlex.join([sandbox, '-p', profile, binary]) + ' "$@"\n')
            wrapper.chmod(0o700)
            settings = ['-c', 'cli_auth_credentials_store="file"', '-c', 'check_for_update_on_startup=false',
                        '-c', 'model_provider="mock"', '-c', 'model_providers.mock=' + toml({
                            'name': 'Mock', 'base_url': 'http://127.0.0.1:%d/v1' % mock.server.server_port,
                            'wire_api': 'responses', 'requires_openai_auth': False})]
            version = await asyncio.create_subprocess_exec(str(wrapper), *settings, '--version', env=env,
                        cwd=str(root), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            output, error = await asyncio.wait_for(version.communicate(), 30)
            self.assertEqual(version.returncode, 0, error.decode())
            self.assertEqual(output.decode().strip(), 'codex-cli 0.159.2')
            results = {'binary': binary, 'version': output.decode().strip(), 'network': 'sandbox allows only 127.0.0.1'}
            results['configuration'] = {'model': model, 'omit_tools_from': omissions,
                                        'approvalPolicy': 'never', 'sandbox': 'danger-full-access',
                                        'default_tools_approval_mode': 'approve'}
            with store.db:
                store.put('owner', 1)
                store.put('mode', 'group')
                store.put('group', 1)
                store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:1',1,1,'Contract',?,1)",
                                 (str(root),))
            topic = store.topic('1:1')
            rows = [store.message_save('1:1', 'owner', text, telegram_message=number)
                    for number, text in enumerate(('contract-first', 'contract-steer', 'contract-second'), 1)]
            server = ContractAppServer(str(wrapper), env)
            marker = 'TORII_CONTRACT_DEVELOPER_INSTRUCTIONS'
            try:
                overrides = CodexCoordinatorSession.overrides(store)
                override = 'mcp_servers.torii.omit_tools_from='
                if configuration != 'direct':
                    overrides = [override + toml(omissions) if value.startswith(override) else value
                                 for value in overrides]
                await asyncio.wait_for(server.start(settings=settings + overrides,
                                                    cwd=str(root)), 40)
                thread = (await asyncio.wait_for(server.request('thread/start', {
                    'model': model, 'cwd': str(root), 'developerInstructions': marker,
                    'approvalPolicy': 'never', 'sandbox': 'danger-full-access'}), 40))['thread']
                native = thread['id']
                turn = (await asyncio.wait_for(server.request('turn/start', {'threadId': native,
                    'clientUserMessageId': message_uuid(rows[0]['id']),
                    'input': [{'type': 'text', 'text': message_tag(topic, rows[0]) + rows[0]['text']}]}), 30))['turn']['id']
                async def wait_request():
                    while not mock.started.is_set():
                        await asyncio.sleep(.01)
                await asyncio.wait_for(wait_request(), 40)
                steered = await asyncio.wait_for(server.request('turn/steer', {'threadId': native,
                    'expectedTurnId': turn, 'clientUserMessageId': message_uuid(rows[1]['id']),
                    'input': [{'type': 'text', 'text': message_tag(topic, rows[1]) + rows[1]['text']}]}), 30)
                mock.release.set()
                completed = await asyncio.wait_for(server.notification('turn/completed',
                    lambda value: value.get('threadId') == native and value.get('turn', {}).get('id') == turn), 90)
                second = (await asyncio.wait_for(server.request('turn/start', {'threadId': native,
                    'clientUserMessageId': message_uuid(rows[2]['id']),
                    'input': [{'type': 'text', 'text': message_tag(topic, rows[2]) + rows[2]['text']}]}), 30))['turn']['id']
                ended = await asyncio.wait_for(server.notification('turn/completed',
                    lambda value: value.get('threadId') == native and value.get('turn', {}).get('id') == second), 60)
                tools = exposed_tools(mock.requests[0])
                names = [(namespace + '.' if namespace else '') + tool.get('name', tool['type'])
                         for namespace, tool in tools]
                direct = any(tool.get('name') == 'telegram_send' and 'torii' in (namespace or '')
                             for namespace, tool in tools)
                results['a'] = {'direct': direct, 'tool_search': 'tool_search' in names, 'tool_names': names,
                                'tool_location': 'tools' if 'tools' in mock.requests[0] else 'input.additional_tools',
                                'calls': mock.calls}
                approvals = [event['method'] for event in server.events
                             if 'requestApproval' in event.get('method', '')]
                sent = store.db.execute("SELECT COUNT(*) FROM outbox WHERE text='contract-tool-executed'").fetchone()[0]
                results['b'] = {'outbox_rows': sent, 'approval_requests': approvals}
                results['c'] = {'developer_instructions': marker in json.dumps(mock.requests)}
                results['d'] = {'steer_turn': steered.get('turnId'), 'first_turn': turn, 'second_turn': second,
                    'same_thread': True, 'statuses': [completed['turn']['status'], ended['turn']['status']]}
                user_items = [event['params']['item'] for event in server.notifications
                              if event.get('method') in ('item/started', 'item/completed') and
                              event.get('params', {}).get('item', {}).get('type') == 'userMessage']
                results['e'] = {'user_items': len(user_items), 'clientUserMessageId':
                                any(item.get('clientUserMessageId') for item in user_items)}
            finally:
                mock.release.set()
                await server.stop()
            path = transcript_path(account, native)
            results['f'] = {'rollout': str(path.relative_to(account)) if path else None,
                            'receipts': sorted(codex_transcript_receipts(path, [row['id'] for row in rows])) if path else [],
                            'expected': [row['id'] for row in rows]}
            target = os.environ.get('TORII_CODEX_CONTRACT_OUTPUT')
            if target:
                destination = Path(target) / configuration
                destination.mkdir(parents=True, exist_ok=True)
                (destination / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
                (destination / 'requests.json').write_text(json.dumps(mock.requests, indent=2) + '\n')
                (destination / 'notifications.json').write_text(json.dumps(server.events, indent=2) + '\n')
                (destination / 'http.json').write_text(json.dumps(mock.http, indent=2) + '\n')
                if path:
                    shutil.copy2(path, destination / 'rollout.jsonl')
            print('Codex parent contract: ' + json.dumps(results, sort_keys=True))
            self.assertFalse(mock.errors, mock.errors)
            self.assertGreater(sent, 0, 'BLOCKER: Torii MCP call did not execute')
            self.assertEqual(sent, 1)
            self.assertFalse(approvals, 'BLOCKER: Torii MCP call requested approval')
            self.assertEqual(direct, configuration == 'direct')
            self.assertEqual(results['a']['tool_search'], configuration == 'deferred_search')
            self.assertTrue(results['c']['developer_instructions'])
            self.assertEqual(steered.get('turnId'), turn)
            self.assertNotEqual(turn, second)
            self.assertEqual(results['d']['statuses'], ['completed', 'completed'])
            self.assertIsNotNone(path)
            self.assertEqual(results['f']['receipts'], sorted(results['f']['expected']))
