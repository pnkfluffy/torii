"""Opt-in pinned-binary contract and reproducible local fixture capture."""

import asyncio
from contextlib import contextmanager
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from coordinator import native_ids, opencode_runtime
from coordinator.native_protocol import RunControl
from coordinator.opencode_accounts import PINNED_VERSION
from coordinator.providers import ProviderRunner
from coordinator.store import Store
from tests.support import read_jsonl, stop_test_hosts


BINARY_ENV = 'TORII_OPENCODE_CONTRACT_BINARY'
DUMMY_KEY = 'torii-contract-dummy'
MARKER = 'TORII_CONTRACT_AGENTS_MARKER'
MODEL = 'example-contract-model'
FIXTURE = Path(__file__).parent / 'fixtures' / f'opencode-acp-{PINNED_VERSION}.jsonl'


@contextmanager
def stub_servers(rate_limited=False, mode="ok", cwd=None, on_main=None, shell_arguments=None, read_capture=False):
    mode = "429" if rate_limited else mode
    state = {"failed": False, "cwd": cwd, "attempts": 0}
    requests = []
    egress = []

    class Stub(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def log_message(self, *args):
            pass

        def do_GET(self):
            requests.append({'path': self.path, 'body': {}, 'authorized': False, 'at': time.monotonic()})
            content = b'{"config":{}}'
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        def do_POST(self):
            try:
                self.post()
            except (BrokenPipeError, ConnectionResetError):
                if mode != 'lifecycle':
                    raise

        def post(self):
            body = json.loads(self.rfile.read(int(self.headers.get('Content-Length', '0'))))
            requests.append({'path': self.path, 'body': body,
                             'authorized': self.headers.get('Authorization') == 'Bearer ' + DUMMY_KEY,
                             'at': time.monotonic()})
            messages = body.get('messages', [])
            main = body.get('stream') and any('CONTRACT_MAIN_PROMPT' in str(item.get('content', '')) for item in messages)
            status = int(mode) if mode in ('401', '402', '429', '400') and main else None
            if mode == 'key_blank' and not requests[-1]['authorized']:
                status = 401
            if status:
                content = json.dumps({'error': {'message': {401: 'Invalid API key provided', 402: 'Insufficient balance, please top up',
                                               429: 'Request capacity exhausted', 400: 'Request 401 could not be processed'}[status],
                                               'type': {401: 'invalid_request_error', 402: 'billing_error',
                                                        429: 'rate_limit_error', 400: 'invalid_request_error'}[status],
                                               'code': {401: 'invalid_api_key', 402: 'insufficient_balance',
                                                        429: 'rate_limit_exceeded', 400: 'bad_request'}[status]}}).encode()
                self.send_response(status)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Retry-After', '0')
                self.send_header('Content-Length', str(len(content)))
                self.end_headers()
                self.wfile.write(content)
                return
            messages = body.get('messages', [])
            main = bool(body.get('tools')) and any('CONTRACT_MAIN_PROMPT' in str(item.get('content', '')) for item in messages)
            if main:
                state['attempts'] += 1
                if on_main:
                    on_main(state['attempts'])
            tool_messages = [item for item in messages if item.get('role') == 'tool']
            read_output = main and read_capture and len(tool_messages) == 1
            use_tool = main and (not tool_messages or read_output) and mode not in ('fresh', 'incomplete_once', 'lifecycle')
            text = 'contract finished' if main else 'Local contract title'
            if main and mode == 'lifecycle':
                text = ('history carried; second answer' if any(item.get('role') == 'assistant' and
                        'first answer' in str(item.get('content')) for item in messages) else 'first answer')
            choice = {'index': 0, 'message': {'role': 'assistant', 'content': text}, 'finish_reason': 'stop'}
            usage = {'prompt_tokens': 23, 'completion_tokens': 7, 'total_tokens': 30,
                     'prompt_tokens_details': {'cached_tokens': 3}}
            if not body.get('stream'):
                content = json.dumps({'id': 'chatcmpl-local', 'object': 'chat.completion', 'created': 1,
                                      'model': body['model'], 'choices': [choice], 'usage': usage}).encode()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(content)))
                self.end_headers()
                self.wfile.write(content)
                return
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Connection', 'close')
            self.end_headers()
            incomplete = mode == 'incomplete_once' and state['attempts'] == 1 or mode == 'progress_reset' and state['attempts'] in (1, 2, 4, 5)
            if main and (mode in ('drop', 'empty', 'length', 'sse_then_ok') or incomplete):
                if mode == 'empty':
                    self.wfile.write(b'data: [DONE]\n\n')
                    self.wfile.flush()
                    self.close_connection = True
                    return
                if mode == 'drop' or incomplete or (mode == 'sse_then_ok' and body.get('tools') and not state['failed']):
                    state['failed'] = True
                    chunk = {'id': 'local', 'object': 'chat.completion.chunk', 'created': 1, 'model': body['model'],
                             'choices': [{'index': 0, 'delta': {'role': 'assistant', 'content': 'start '},
                                          'finish_reason': None}]}
                    self.wfile.write(('data: ' + json.dumps(chunk) + '\n\n').encode())
                    if mode == 'sse_then_ok':
                        self.wfile.write(b'data: {"error":{"message":"upstream overloaded mid stream","type":"server_error","code":"overloaded"}}\n\n')
                    self.wfile.flush()
                    self.close_connection = True
                    return
                use_tool = False
            if use_tool:
                name = 'edit' if mode == 'edit' else 'bash'
                arguments = ({'filePath': str(state['cwd'] / 'sample.py'), 'oldString': 'value = 1',
                              'newString': 'value = 2'} if mode == 'edit' else {
                              **(shell_arguments or {'command': "printf 'tool-ok'",
                                                     'description': 'Print a local contract marker'})})
                if read_output:
                    name = 'read'
                    path = tool_messages[0]['content'].split('Full output saved to: ', 1)[1].split('\n', 1)[0]
                    arguments = {'filePath': path, 'limit': 1}
                delta = {'role': 'assistant', 'tool_calls': [{'index': 0, 'id': 'call-local-2' if read_output else 'call-local-1', 'type': 'function',
                          'function': {'name': name, 'arguments': json.dumps(arguments)}}]}
            else:
                delta = {'role': 'assistant', 'content': text}
            for part in ({'choices': [{'index': 0, 'delta': delta, 'finish_reason': None}]},
                         {'choices': [{'index': 0, 'delta': {}, 'finish_reason': (None if mode == 'tool_unknown' else 'tool_calls') if use_tool else ('length' if mode == 'length' else 'stop')}]},
                         {'choices': [], 'usage': usage}):
                chunk = {'id': 'chatcmpl-local', 'object': 'chat.completion.chunk', 'created': 1,
                         'model': body['model'], **part}
                self.wfile.write(('data: ' + json.dumps(chunk) + '\n\n').encode())
            self.wfile.write(b'data: [DONE]\n\n')
            self.wfile.flush()
            self.close_connection = True

    class Recorder(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def record(self):
            egress.append({'method': self.command, 'target': self.path})
            self.send_error(502, 'External egress forbidden by contract')

        do_CONNECT = record
        do_GET = record
        do_POST = record

    servers = [ThreadingHTTPServer(('127.0.0.1', 0), handler) for handler in (Stub, Recorder)]
    for server in servers:
        server.daemon_threads = False
    threads = [threading.Thread(target=server.serve_forever, daemon=True) for server in servers]
    for thread in threads:
        thread.start()
    try:
        yield (f'http://127.0.0.1:{servers[0].server_port}/v1',
               f'http://127.0.0.1:{servers[1].server_port}', requests, egress)
    finally:
        for server in servers:
            server.shutdown()
            server.server_close()
        for thread in threads:
            thread.join()

def sanitize_capture(text, root):
    replacements = {str(Path(root).resolve()): '/contract', str(Path.home()): '/neutral/home',
                    socket.gethostname(): 'neutral-host'}
    for original, neutral in replacements.items():
        text = text.replace(original, neutral)
    return text


async def capture_run(root, binary, base_url, proxy, mode='ok', config_hook=None, on_initialize=None):
    root = Path(root)
    cwd = root / 'project'
    cwd.mkdir()
    skill = root / '.agents' / 'skills' / 'ancestor-marker'
    skill.mkdir(parents=True)
    (skill / 'SKILL.md').write_text('---\nname: forbidden-ancestor-skill\ndescription: MUST_NOT_LOAD_ANCESTOR_SKILL\n---\nDummy local skill.\n')
    (cwd / 'sample.py').write_text('value = 1\n')
    (cwd / 'AGENTS.md').write_text(MARKER + '\n')
    (cwd / 'opencode.json').write_text(json.dumps({'model': 'forbidden-project/model',
                                                 'instructions': ['MUST_NOT_LOAD_PROJECT_CONFIG']}))
    for directory in (cwd / '.opencode/plugins', root / '.opencode/plugins'):
        directory.mkdir(parents=True)
        (directory / 'hostile.js').write_text('import { writeFileSync } from "node:fs"; export default async () => { writeFileSync(' + json.dumps(str(root / 'hostile-loaded')) + ', "bad"); return {}; };')
    managed_paths = (root / 'test-home/managed/user.plist', root / 'test-home/managed/system.plist')
    if mode in ('managed_user', 'managed_system'):
        managed = managed_paths[mode == 'managed_system']
        managed.parent.mkdir(parents=True)
        managed.write_text('host policy must not be read')
    key_home = root / 'dummy-account'
    key_home.mkdir()
    key = key_home / 'key'
    key.write_text('  \n' if mode == 'key_blank' else '  ' + DUMMY_KEY + '\n\t')
    key.chmod(0o600)
    store = Store(root / 'state')
    store.put('opencode_accounts', {'local': {'base_url': base_url, 'model': MODEL,
              'context': 128000, 'output': 4096, 'price_in': 1, 'price_cached': 0.1, 'price_out': 2}})
    store.put('opencode_active_account', 'local')
    store.put('opencode_binary', str(binary))
    store.put('opencode_enabled', True)
    env = {name: proxy for name in ('HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'http_proxy', 'https_proxy', 'all_proxy')}
    env.update(NO_PROXY='127.0.0.1,localhost', no_proxy='127.0.0.1,localhost')
    original = opencode_runtime.write_config
    build_env = opencode_runtime.environment
    def write(*args):
        path = original(*args)
        if mode == 'key_missing':
            key.rename(key_home / 'saved-key')
        elif mode == 'key_unreadable':
            key.chmod(0o000)
        if config_hook is not None:
            config_hook(path)
        return path
    def environment(*args):
        return dict(build_env(*args), **env)
    factory = opencode_runtime.adapter
    def adapter(*args, **kwargs):
        result = factory(*args, **kwargs)
        if on_initialize is not None:
            original_auth = result.auth_step
            async def auth(protocol):
                on_initialize(Path(json.loads((protocol.process.directory / 'spec.json').read_text())['env']['XDG_CONFIG_HOME']).parent)
                if original_auth is not None:
                    await original_auth(protocol)
            result = replace(result, auth_step=auth)
        return result
    try:
        runner = ProviderRunner(store.directory, opencode_store=store)
        with patch('coordinator.providers.platform', 'linux'), \
                patch('coordinator.opencode_runtime.environment', environment), \
                patch('coordinator.opencode_runtime.write_config', write), \
                patch('coordinator.opencode_accounts.account_home', return_value=key_home), \
                patch('coordinator.opencode_runtime.managed_preference_paths', return_value=managed_paths, create=True), \
                patch.dict('coordinator.providers.ADAPTERS', {'opencode': adapter}):
            result = await asyncio.wait_for(runner.run('opencode',
                'CONTRACT_MAIN_PROMPT. Use one bash tool, then give the short final answer.', cwd, None,
                fresh=True, worker_id=1), 15 if mode in ('drop', 'empty') else 90)
        return result, cwd
    finally:
        store.close()
        if key.exists():
            key.chmod(0o600)
        stop_test_hosts(root)


class OpenCodeFixtureTests(unittest.TestCase):
    def test_fixture_has_real_wire_shapes_and_neutral_paths(self):
        rows = [json.loads(line) for line in FIXTURE.read_text().splitlines()]
        responses = [row['event'] for row in rows if row['direction'] == 'agent']
        initialized = next(event['result'] for event in responses if (event.get('result') or {}).get('agentInfo'))
        self.assertEqual(initialized['agentInfo']['version'], PINNED_VERSION)
        sid = next(event['result']['sessionId'] for event in responses if 'sessionId' in (event.get('result') or {}))
        self.assertTrue(native_ids.valid('opencode', sid))
        self.assertTrue(any((event.get('result') or {}).get('stopReason') == 'end_turn' for event in responses))
        kinds = [(event.get('params', {}).get('update') or {}).get('sessionUpdate') for event in responses]
        self.assertIn('tool_call', kinds)
        self.assertIn('usage_update', kinds)
        self.assertNotIn('/Users/', FIXTURE.read_text())


@unittest.skipUnless(os.environ.get(BINARY_ENV) and Path(os.environ.get(BINARY_ENV, '')).is_file(),
                     'Set TORII_OPENCODE_CONTRACT_BINARY to the pinned OpenCode binary')
class OpenCodeContractTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        platform = patch('coordinator.providers.platform', 'linux')
        platform.start()
        self.addCleanup(platform.stop)
        self.temp = tempfile.TemporaryDirectory(prefix='opencode-contract-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.addCleanup(stop_test_hosts, self.root)
        self.binary = Path(os.environ[BINARY_ENV]).resolve()
        home = patch.dict(os.environ, {'HOME': str(self.root / 'test-home')})
        home.start()
        self.addCleanup(home.stop)

    async def test_real_shell_output_preserves_errors_nesting_and_timeout(self):
        for index, (command, extra, expected) in enumerate((
                ("printf out; printf err >&2; exit 7", {}, 'outerr'),
                ("printf one; bash -c 'printf nested'; printf end", {}, 'onenestedend'),
                ('if then', {}, 'syntax error'),
                ('printf partial; sleep 5', {'timeout': 20}, 'partial'),
                ('true', {}, '(no output)'))):
            with self.subTest(command=command):
                root = self.root / str(index)
                root.mkdir()
                args = dict(command=command, description='Exercise the actual shell output', **extra)
                with stub_servers(shell_arguments=args) as (url, proxy, requests, egress):
                    result, cwd = await capture_run(root, self.binary, url, proxy)
                self.assertTrue(result.success, result)
                outputs = [message['content'] for row in requests for message in row['body']['messages']
                           if message['role'] == 'tool']
                self.assertEqual(len(outputs), 1)
                if command == 'if then' or extra:
                    self.assertIn(expected, outputs[0])
                else:
                    self.assertEqual(outputs, [expected])
                if extra:
                    self.assertIn('exceeding timeout 20 ms', outputs[0])
                self.assertEqual(egress, [])

    async def test_real_truncated_shell_output_can_be_read_with_existing_permissions(self):
        args = {'command': "printf 'CAPTURE-START\\n'; printf 'line\\n%.0s' {1..2100}; printf 'CAPTURE-END'",
                'description': 'Print more than the shell preview line limit'}
        with stub_servers(shell_arguments=args, read_capture=True) as (url, proxy, requests, egress):
            result, cwd = await capture_run(self.root, self.binary, url, proxy)
        self.assertTrue(result.success, result)
        outputs = [message['content'] for row in requests for message in row['body']['messages']
                   if message['role'] == 'tool']
        self.assertIn('Full output saved to: ', outputs[0])
        self.assertNotIn('CAPTURE-START', outputs[0])
        self.assertIn('CAPTURE-END', outputs[0])
        self.assertIn('CAPTURE-START', outputs[-1])
        self.assertEqual(egress, [])

    async def test_pinned_fresh_tool_key_model_instructions_egress_and_eof(self):
        with stub_servers() as (url, proxy, requests, egress):
            result, cwd = await capture_run(self.root, self.binary, url, proxy)
            self.assertTrue(result.success, result)
            self.assertEqual(result.text, 'contract finished')
            self.assertTrue(native_ids.valid('opencode', result.session_id))
            self.assertGreaterEqual(len(requests), 3, 'Expected main prompt, tool continuation and title generation')
            self.assertTrue(all(row['authorized'] for row in requests), 'Dummy file substitution must trim whitespace')
            self.assertEqual({row['body']['model'] for row in requests}, {MODEL})
            self.assertTrue(any('title' in str(message.get('content', '')).lower()
                                for row in requests for message in row['body']['messages']
                                if message['role'] == 'system'), 'Expected an actual title-generation request')
            system = [message['content'] for row in requests for message in row['body']['messages']
                      if message['role'] == 'system']
            self.assertIn(MARKER, str(system))
            self.assertNotIn('MUST_NOT_LOAD_PROJECT_CONFIG', str(system))
            self.assertNotIn('MUST_NOT_LOAD_ANCESTOR_SKILL', str(system))
            self.assertEqual([message['content'] for row in requests for message in row['body']['messages']
                              if message['role'] == 'tool'], ['tool-ok'])
            self.assertEqual(egress, [], 'A proxy-aware request tried to leave the local stub')
            directory = Path(result.log_path).parent
            for name in ('spec.json', 'events.jsonl', 'writes.jsonl', 'stderr.log', 'exit.json'):
                text = (directory / name).read_text()
                self.assertNotIn(str(self.root / 'dummy-account' / 'key'), text, name)
                self.assertNotIn(DUMMY_KEY, text, name)
            self.assertNotIn('forbidden-ancestor-skill', (directory / 'events.jsonl').read_text())
            self.assertTrue((directory / 'exit.json').is_file(), 'stdin EOF must end the process')
            spec = json.loads((directory / 'spec.json').read_text())
            env = spec['env']
            debug = await asyncio.create_subprocess_exec(str(self.binary), 'debug', 'config',
                       cwd=cwd, env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            try:
                output, error = await asyncio.wait_for(debug.communicate(), 30)
            finally:
                if debug.returncode is None:
                    debug.kill()
                    await debug.wait()
            self.assertEqual(debug.returncode, 0, error.decode(errors='replace'))
            effective = json.loads(output)
            self.assertEqual(effective['enabled_providers'], ['torii-host'])
            self.assertEqual(effective['model'], effective['small_model'])
            self.assertEqual((effective['share'], effective['autoupdate'], effective['mcp']), ('disabled', False, {}))
            self.assertEqual(effective['permission']['external_directory'], 'deny')
            self.assertEqual(effective['permission']['bash'], 'allow')
            self.assertEqual(set(effective['provider']), {'torii-host'})
            self.assertEqual(effective['instructions'], [str(cwd / 'AGENTS.md')])
            launches = self.root / 'state/opencode/workers/1/launches'
            entries = list(launches.iterdir())
            self.assertEqual(len(entries), 1)
            worker_root = entries[0]
            self.assertTrue(worker_root.is_dir())
            self.assertEqual(env['XDG_CONFIG_HOME'], str(worker_root / 'config'))
            guard = worker_root / 'config/opencode/plugins/guard.js'
            self.assertEqual(effective['plugin'], [guard.as_uri()])
            self.assertEqual(guard.read_bytes(), (Path(opencode_runtime.__file__).parent / 'opencode_guard.js').read_bytes())
            self.assertEqual(guard.stat().st_mode & 0o777, 0o600)
            self.assertEqual(guard.parent.stat().st_mode & 0o777, 0o700)
            self.assertIs(effective['lsp'], False)
            self.assertIs(effective['formatter'], False)
            self.assertEqual(effective['shell'], '/bin/bash')
            self.assertNotIn('--pure', spec['argv'])
            self.assertEqual(env['OPENCODE_TEST_MANAGED_CONFIG_DIR'], str(worker_root / 'managed'))
            self.assertFalse((self.root / 'hostile-loaded').exists())
            self.assertEqual(list((worker_root / 'config').rglob('node_modules')), [])
            self.assertFalse((worker_root / 'guard-events.jsonl').exists())
            self.assertEqual(egress, [])
            capture = []
            for name, direction in (('writes.jsonl', 'client'), ('events.jsonl', 'agent')):
                for line in (directory / name).read_text().splitlines():
                    journal = json.loads(line)
                    capture.append({'direction': direction, 'event': json.loads(journal['line' if direction == 'client' else 'data'])})
            sanitized = sanitize_capture(''.join(json.dumps(row) + '\n' for row in capture), self.root)
            (self.root / 'captured.jsonl').write_text(sanitized)
            print(f'OpenCode {PINNED_VERSION} contract: fresh tool, trimmed dummy key, model/title, AGENTS, effective config, proxy recorder and EOF passed', flush=True)

    async def test_429_is_bounded_and_classified(self):
        with stub_servers(rate_limited=True) as (url, proxy, requests, egress):
            started = time.monotonic()
            result, cwd = await capture_run(self.root, self.binary, url, proxy)
            elapsed = time.monotonic() - started
            self.assertFalse(result.success)
            self.assertEqual(result.failure_code, 'rate_limited', result)
            self.assertLess(elapsed, 90)
            self.assertTrue(requests)
            self.assertEqual(egress, [])
            intervals = [round(requests[index]['at'] - requests[index - 1]['at'], 3) for index in range(1, len(requests))]
            print(f'OpenCode {PINNED_VERSION} 429: requests=%d elapsed=%.3fs intervals=%s code=%s' % (
                  len(requests), elapsed, intervals, result.failure_code), flush=True)

    async def test_real_error_statuses_and_blank_key(self):
        for mode, code, message in (
                ('401', 'auth', 'Invalid API key provided'),
                ('402', 'out_of_credit', 'Insufficient balance, please top up'),
                ('429', 'rate_limited', 'Request capacity exhausted'),
                ('400', 'provider_error', 'Request 401 could not be processed'),
                ('key_blank', 'auth', 'Invalid API key provided')):
            with self.subTest(mode=mode), stub_servers(mode=mode) as (url, proxy, requests, egress):
                root = self.root / mode
                root.mkdir()
                result, cwd = await capture_run(root, self.binary, url, proxy, mode)
                self.assertFalse(result.success)
                self.assertEqual(result.failure_code, code)
                self.assertIn('local', result.error)
                self.assertIn(message, result.error)
                self.assertNotIn(str(root / 'dummy-account/key'), result.error)
                self.assertEqual(egress, [])
                print('contract mode=%s requests=%d code=%s' % (mode, len(requests), code), flush=True)

    async def test_real_cutoff_is_max_tokens(self):
        with stub_servers(mode='length') as (url, proxy, requests, egress):
            result, cwd = await capture_run(self.root, self.binary, url, proxy, 'length')
            self.assertFalse(result.success)
            self.assertEqual(result.failure_code, 'max_tokens')
            self.assertEqual(egress, [])

    async def test_real_retried_stream_has_only_completed_text(self):
        with stub_servers(mode='sse_then_ok') as (url, proxy, requests, egress):
            result, cwd = await capture_run(self.root, self.binary, url, proxy, 'sse_then_ok')
            self.assertTrue(result.success, result)
            self.assertEqual(result.text, 'contract finished')
            self.assertGreaterEqual(sum(bool(row['body'].get('tools')) for row in requests), 2)
            self.assertEqual(egress, [])

    async def test_real_lost_keys_are_auth(self):
        for mode in ('key_missing', 'key_unreadable'):
            with self.subTest(mode=mode), stub_servers(mode=mode) as (url, proxy, requests, egress):
                root = self.root / mode
                root.mkdir()
                result, cwd = await capture_run(root, self.binary, url, proxy, mode)
                self.assertFalse(result.success)
                self.assertEqual(result.failure_code, 'auth')
                self.assertIn('local', result.error)
                self.assertIn('key file is missing or unreadable', result.error)
                self.assertNotIn(str(root / 'dummy-account/key'), result.error)
                self.assertEqual(requests, [])
                self.assertEqual(egress, [])

    async def test_incomplete_streams_end_after_three_attempts(self):
        for mode in ('drop', 'empty'):
            with self.subTest(mode=mode), stub_servers(mode=mode) as (url, proxy, requests, egress):
                root = self.root / mode
                root.mkdir()
                result, cwd = await capture_run(root, self.binary, url, proxy, mode)
                self.assertFalse(result.success)
                self.assertEqual(result.failure_code, 'provider_stream_incomplete')
                self.assertEqual(worker_requests(requests), 3)
                self.assertIn('cancelled', prompt_responses(result))
                self.assertEqual(egress, [])
                self.assertFalse((root / 'hostile-loaded').exists())
                print('contract mode=%s worker_requests=%d total_requests=%d code=%s prompt=cancelled' % (
                      mode, worker_requests(requests), len(requests), result.failure_code), flush=True)

    async def test_edit_tool_has_no_recorded_egress(self):
        cwd = self.root / 'project'
        with stub_servers(mode='edit', cwd=cwd) as (url, proxy, requests, egress):
            original = opencode_runtime.write_config
            def write(*args):
                (cwd / 'sample.py').write_text('value = 1\n')
                return original(*args)
            with patch('coordinator.opencode_runtime.write_config', write):
                result, cwd = await capture_run(self.root, self.binary, url, proxy, 'edit')
            self.assertTrue(result.success, result)
            self.assertEqual((cwd / 'sample.py').read_text(), 'value = 2\n')
            self.assertEqual(egress, [])
            env = json.loads((Path(result.log_path).parent / 'spec.json').read_text())['env']
            self.assertEqual(env['OPENCODE_DISABLE_LSP_DOWNLOAD'], '1')
            print('contract edit applied; proxy recorder requests=0 (proxy-aware traffic only)', flush=True)

    async def test_real_record_read_changes_no_source_file(self):
        original = opencode_runtime.records
        observations = []
        def read(root, sid):
            def snapshot():
                return {str(p.relative_to(root)): (p.read_bytes() if p.is_file() else None,
                                                   p.stat().st_mtime_ns, p.stat().st_mode)
                        for p in Path(root).rglob('*')}
            before = snapshot()
            value = original(root, sid)
            self.assertIsNotNone(value)
            self.assertEqual(snapshot(), before)
            observations.append(len(before))
            return value
        with stub_servers() as (url, proxy, requests, egress), patch('coordinator.opencode_runtime.records', read):
            result, cwd = await capture_run(self.root, self.binary, url, proxy)
            self.assertTrue(result.success, result)
            self.assertTrue(observations)
            self.assertEqual(egress, [])
            print('contract record reader left all worker file bytes, modes, mtimes and directory entries unchanged', flush=True)

    async def test_guard_loaded_before_initialize_response_without_model_requests(self):
        observed = []
        with stub_servers(mode='fresh') as (url, proxy, requests, egress):
            def initialized(root):
                self.assertEqual(requests, [])
                self.assertEqual(json.loads((root / 'guard-ready.json').read_text()),
                                 json.loads((root / 'guard-launch.json').read_text()))
                observed.append(True)
            result, cwd = await capture_run(self.root, self.binary, url, proxy, 'fresh', on_initialize=initialized)
        self.assertTrue(result.success, result)
        self.assertEqual(observed, [True])
        self.assertEqual(egress, [])

    async def test_broken_guard_and_stale_proof_fail_before_any_model_request(self):
        for stale in (False, True):
            with self.subTest(stale=stale), stub_servers(mode='fresh') as (url, proxy, requests, egress):
                root = self.root / str(stale)
                root.mkdir()
                def break_guard(config):
                    (config.parent / 'plugins/guard.js').write_text('export default async ( => syntax error')
                    if stale:
                        (config.parents[2] / 'guard-ready.json').write_text(json.dumps({'version': 1, 'launch': 'earlier-launch'}))
                result, cwd = await capture_run(root, self.binary, url, proxy, 'fresh', config_hook=break_guard)
                self.assertFalse(result.success)
                self.assertEqual(result.failure_code, 'provider_guard_unavailable')
                self.assertEqual(requests, [])
                self.assertEqual(egress, [])
                print('contract broken_guard stale=%s requests=0 code=%s' % (stale, result.failure_code), flush=True)

    async def test_abort_failure_still_ends_without_a_fourth_model_request(self):
        with stub_servers(mode='empty') as (url, proxy, requests, egress):
            def break_abort(config):
                guard = config.parent / 'plugins/guard.js'
                source = guard.read_text()
                self.assertEqual(source.count('client.session.abort('), 1)
                guard.write_text(source.replace('client.session.abort(', 'Promise.reject('))
            result, cwd = await capture_run(self.root, self.binary, url, proxy, 'empty', config_hook=break_abort)
            self.assertFalse(result.success)
            self.assertEqual(result.failure_code, 'provider_stream_incomplete')
            self.assertEqual(worker_requests(requests), 3)
            answers = prompt_answers(result)
            self.assertEqual(len(answers), 1)
            self.assertIn('error', answers[0])
            self.assertEqual(egress, [])
            print('contract abort_failure worker_requests=%d total_requests=%d code=%s prompt=%s' % (
                  worker_requests(requests), len(requests), result.failure_code, answers), flush=True)

    async def test_managed_preferences_block_before_launch(self):
        for mode in ('managed_user', 'managed_system'):
            with self.subTest(mode=mode), stub_servers(mode='fresh') as (url, proxy, requests, egress):
                root = self.root / mode
                root.mkdir()
                result, cwd = await capture_run(root, self.binary, url, proxy, mode)
                self.assertFalse(result.success)
                self.assertEqual(result.failure_code, 'provider_managed_config')
                self.assertEqual(requests, [])
                self.assertEqual(egress, [])
                self.assertFalse((root / 'state/hosts').exists())

    async def test_host_managed_directory_is_replaced_by_worker_directory(self):
        host = self.root / 'test-home/host-managed'
        host.mkdir(parents=True)
        plugin = host / 'hostile.js'
        plugin.write_text('import { writeFileSync } from "node:fs"; export default async () => { writeFileSync(' + json.dumps(str(self.root / 'managed-loaded')) + ', "bad"); return {}; };')
        for name in ('opencode.json', 'opencode.jsonc'):
            (host / name).write_text(json.dumps({'plugin': [plugin.as_uri()]}))
        with patch.dict(os.environ, OPENCODE_TEST_MANAGED_CONFIG_DIR=str(host)), \
                stub_servers(mode='fresh') as (url, proxy, requests, egress):
            result, cwd = await capture_run(self.root, self.binary, url, proxy, 'fresh')
        self.assertTrue(result.success, result)
        env = json.loads((Path(result.log_path).parent / 'spec.json').read_text())['env']
        launches = self.root / 'state/opencode/workers/1/launches'
        entries = list(launches.iterdir())
        self.assertEqual(len(entries), 1)
        launch = entries[0]
        self.assertTrue(launch.is_dir())
        self.assertEqual(env['XDG_CONFIG_HOME'], str(launch / 'config'))
        self.assertEqual(env['OPENCODE_TEST_MANAGED_CONFIG_DIR'], str(launch / 'managed'))
        self.assertFalse((self.root / 'managed-loaded').exists())
        self.assertEqual(egress, [])

    async def test_guard_preserves_recovery_and_tool_progress(self):
        for mode, attempts in (('fresh', 1), ('incomplete_once', 2), ('progress_reset', 6), ('tool_unknown', 2)):
            with self.subTest(mode=mode), stub_servers(mode=mode) as (url, proxy, requests, egress):
                root = self.root / mode
                root.mkdir()
                result, cwd = await capture_run(root, self.binary, url, proxy, mode)
                self.assertTrue(result.success, result)
                self.assertEqual(result.text, 'contract finished')
                self.assertEqual(worker_requests(requests), attempts)
                self.assertEqual(prompt_responses(result), ['end_turn'])
                self.assertEqual(egress, [])
                self.assertFalse((root / 'hostile-loaded').exists())
                print('contract mode=%s worker_requests=%d total_requests=%d success' % (
                      mode, attempts, len(requests)), flush=True)

    async def test_real_owner_stop_keeps_owner_stopped(self):
        from coordinator.acp_protocol import AcpProtocol
        loop = asyncio.get_running_loop()
        protocols = []
        def factory(*args, **kwargs):
            protocol = AcpProtocol(*args, **kwargs)
            protocols.append(protocol)
            return protocol
        def on_main(attempt):
            if attempt == 1:
                loop.call_soon_threadsafe(protocols[0].cancel)
        with stub_servers(mode='empty', on_main=on_main) as (url, proxy, requests, egress), \
                patch('coordinator.providers.AcpProtocol', factory):
            result, cwd = await capture_run(self.root, self.binary, url, proxy, 'owner_stop')
        self.assertFalse(result.success)
        self.assertEqual(result.failure_code, 'owner_stopped')
        self.assertEqual(result.text, '')
        self.assertEqual(prompt_responses(result), ['cancelled'])
        self.assertEqual(egress, [])

    @contextmanager
    def lifecycle_runner(self, url, proxy):
        cwd = self.root / 'project'
        cwd.mkdir()
        key_home = self.root / 'dummy-account'
        key_home.mkdir()
        key = key_home / 'key'
        key.write_text(DUMMY_KEY)
        key.chmod(0o600)
        store = Store(self.root / 'state')
        store.put('opencode_accounts', {'local': {'base_url': url, 'model': MODEL,
            'context': 128000, 'output': 4096, 'price_in': 1, 'price_cached': 0.1, 'price_out': 2}})
        store.put('opencode_active_account', 'local')
        store.put('opencode_binary', str(self.binary))
        store.put('opencode_enabled', True)
        original = opencode_runtime.environment
        def environment(*args):
            return dict(original(*args), HTTP_PROXY=proxy, HTTPS_PROXY=proxy, ALL_PROXY=proxy,
                        NO_PROXY='127.0.0.1,localhost')
        try:
            with patch('coordinator.opencode_runtime.environment', environment), \
                    patch('coordinator.opencode_accounts.account_home', return_value=key_home), \
                    patch('coordinator.opencode_runtime.managed_preference_paths', return_value=()):
                yield ProviderRunner(store.directory, opencode_store=store), cwd
        finally:
            stop_test_hosts(self.root)
            store.close()

    async def wait_main(self, started):
        async def wait():
            while not started.is_set():
                await asyncio.sleep(0.01)
        await asyncio.wait_for(wait(), 30)

    async def finish_lifecycle_run(self, run):
        if not run.done():
            run.cancel()
        await asyncio.gather(run, return_exceptions=True)

    async def test_contract_stop_mid_turn_sends_cancel(self):
        started, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        def main(attempt):
            started.set()
            release.wait(30)
        with stub_servers(mode='lifecycle', on_main=main) as (url, proxy, requests, egress), \
                self.lifecycle_runner(url, proxy) as (runner, cwd):
            cancel = []
            run = asyncio.create_task(runner.run('opencode', 'CONTRACT_MAIN_PROMPT first', cwd, None,
                fresh=True, worker_id=1, on_cancel=lambda value: cancel.append(value) if value else None))
            self.addAsyncCleanup(self.finish_lifecycle_run, run)
            try:
                await self.wait_main(started)
                await cancel[-1]()
                result = await asyncio.wait_for(run, 30)
            finally:
                release.set()
        self.assertEqual(result.failure_code, 'owner_stopped')
        self.assertFalse(result.success)
        self.assertEqual(prompt_responses(result), ['cancelled'])
        self.assertEqual(worker_requests(requests), 1)
        wire = [json.loads(json.loads(line)['line']) for line in
                (Path(result.log_path).parent / 'writes.jsonl').read_text().splitlines()]
        self.assertEqual([e['method'] for e in wire], ['initialize', 'session/new', 'session/prompt', 'session/cancel'])
        self.assertEqual(egress, [])
        print('CONTRACT stop: model_requests=1 prompt_responses=cancelled')

    async def test_contract_running_steer_is_one_next_prompt(self):
        started, release = threading.Event(), threading.Event()
        def main(attempt):
            if attempt == 1:
                started.set()
                release.wait(30)
        with stub_servers(mode='lifecycle', on_main=main) as (url, proxy, requests, egress), \
                self.lifecycle_runner(url, proxy) as (runner, cwd):
            control = RunControl()
            run = asyncio.create_task(runner.run('opencode', 'CONTRACT_MAIN_PROMPT first', cwd, None,
                fresh=True, worker_id=1, control=control))
            self.addAsyncCleanup(self.finish_lifecycle_run, run)
            try:
                await self.wait_main(started)
                self.assertEqual(await control.steer('first', 'next one'), 'queued')
                self.assertEqual(await control.steer('second', 'next two'), 'queued')
                self.assertEqual(worker_requests(requests), 1)
                release.set()
                result = await asyncio.wait_for(run, 60)
            finally:
                release.set()
        self.assertTrue(result.success, result)
        self.assertEqual(result.text, 'history carried; second answer')
        self.assertEqual(worker_requests(requests), 2)
        self.assertEqual(prompt_responses(result), ['end_turn', 'end_turn'])
        wire = [json.loads(json.loads(line)['line']) for line in
                (Path(result.log_path).parent / 'writes.jsonl').read_text().splitlines()]
        prompts = [e for e in wire if e.get('method') == 'session/prompt']
        self.assertEqual(len(prompts), 2)
        self.assertEqual(prompts[1]['params']['prompt'], [{'type': 'text', 'text': 'next one\n\nnext two'}])
        self.assertEqual(await control.steer('late', 'closed'), 'closed')
        self.assertEqual(egress, [])
        print('CONTRACT steer: model_requests=2 prompt_responses=end_turn,end_turn')

    async def test_contract_resume_loads_history_and_unknown_session_fails(self):
        with stub_servers(mode='lifecycle') as (url, proxy, requests, egress), \
                self.lifecycle_runner(url, proxy) as (runner, cwd):
            first = await asyncio.wait_for(runner.run('opencode', 'CONTRACT_MAIN_PROMPT first', cwd, None,
                fresh=True, worker_id=1), 60)
            self.assertTrue(first.success, first)
            spec = json.loads((Path(first.log_path).parent / 'spec.json').read_text())
            old = Path(spec['env']['XDG_CONFIG_HOME']).parent
            (old / 'stream-incomplete.json').write_text(json.dumps({'version': 1, 'consecutive': 3,
                'code': 'provider_stream_incomplete', 'session_id': first.session_id}))
            (old / 'config/opencode/plugins/hostile.js').write_text(
                'import {writeFileSync} from "node:fs"; export default async () => {writeFileSync(' +
                json.dumps(str(self.root / 'old-plugin-loaded')) + ',"bad");return {}};')
            (old / 'managed').mkdir(exist_ok=True)
            (old / 'managed/opencode.json').write_text('{"model":"forbidden/model"}')
            result = await asyncio.wait_for(runner.run('opencode', 'second prompt', cwd, first.session_id,
                fresh=False, worker_id=1, account_alias='local'), 60)
            self.assertTrue(result.success, result)
            self.assertEqual(result.text, 'history carried; second answer')
            self.assertEqual(worker_requests(requests), 2)
            self.assertFalse((self.root / 'old-plugin-loaded').exists())
            second_spec = json.loads((Path(result.log_path).parent / 'spec.json').read_text())
            self.assertEqual(spec['env']['XDG_DATA_HOME'], second_spec['env']['XDG_DATA_HOME'])
            wire = [json.loads(json.loads(line)['line']) for line in
                    (Path(result.log_path).parent / 'writes.jsonl').read_text().splitlines()]
            self.assertEqual([e['method'] for e in wire], ['initialize', 'session/load', 'session/prompt'])
            self.assertEqual(wire[1]['params']['sessionId'], first.session_id)
            missing = await asyncio.wait_for(runner.run('opencode', 'never sent', cwd, 'ses_' + 'a' * 26,
                fresh=False, worker_id=1, account_alias='local'), 60)
            self.assertEqual(missing.failure_code, 'session_missing',
                (Path(missing.log_path).parent / 'events.jsonl').read_text())
            self.assertEqual(worker_requests(requests), 2)
            missing_wire = [json.loads(json.loads(line)['line']) for line in
                    (Path(missing.log_path).parent / 'writes.jsonl').read_text().splitlines()]
            self.assertEqual([e['method'] for e in missing_wire], ['initialize', 'session/load'])
            missing_events = [json.loads(json.loads(line)['data']) for line in
                    (Path(missing.log_path).parent / 'events.jsonl').read_text().splitlines()]
            load_id = missing_wire[1]['id']
            load_error = next(event['error'] for event in missing_events if event.get('id') == load_id)
            print('CONTRACT session/load error: ' + json.dumps(load_error, sort_keys=True), flush=True)
            self.assertEqual(prompt_responses(result), ['end_turn'])
            self.assertEqual(prompt_responses(missing), [])
        self.assertEqual(egress, [])
        print('CONTRACT resume+unknown: model_requests=2 prompt_responses=end_turn,end_turn; unknown=0')

    async def test_contract_resume_ignores_saved_auth_remote_config(self):
        with stub_servers(mode='lifecycle') as (url, proxy, requests, egress), \
                self.lifecycle_runner(url, proxy) as (runner, cwd):
            first = await asyncio.wait_for(runner.run('opencode', 'CONTRACT_MAIN_PROMPT first', cwd, None,
                fresh=True, worker_id=1), 60)
            self.assertTrue(first.success, first)
            spec = json.loads((Path(first.log_path).parent / 'spec.json').read_text())
            auth = Path(spec['env']['XDG_DATA_HOME']) / 'opencode/auth.json'
            planted = json.dumps({url: {'type': 'wellknown', 'key': 'DUMMY_ONLY', 'token': 'dummy'}})
            auth.write_text(planted)
            result = await asyncio.wait_for(runner.run('opencode', 'next prompt', cwd, first.session_id,
                fresh=False, worker_id=1, account_alias='local'), 60)
            self.assertTrue(result.success, result)
            self.assertEqual(result.text, 'history carried; second answer')
            self.assertEqual(worker_requests(requests), 2)
            self.assertEqual(auth.read_text(), planted)
            self.assertEqual(sum(row['path'].endswith('/.well-known/opencode') for row in requests), 0)
        self.assertEqual(egress, [])
        print('CONTRACT saved auth ignored: model_requests=2 remote_config_requests=0')

    async def test_contract_reattach_uses_original_answer_without_second_prompt(self):
        started, release = threading.Event(), threading.Event()
        def main(attempt):
            started.set()
            release.wait(30)
        with stub_servers(mode='lifecycle', on_main=main) as (url, proxy, requests, egress), \
                self.lifecycle_runner(url, proxy) as (runner, cwd):
            run = asyncio.create_task(runner.run('opencode', 'CONTRACT_MAIN_PROMPT first', cwd, None,
                fresh=True, worker_id=1, host_id='restart'))
            self.addAsyncCleanup(self.finish_lifecycle_run, run)
            try:
                await self.wait_main(started)
                directory = runner.state_dir / 'hosts/restart'
                wire = [json.loads(row['line']) for row in read_jsonl(directory / 'writes.jsonl')]
                sid = next(e['params']['sessionId'] for e in wire if e.get('method') == 'session/prompt')
                run.cancel()
                await asyncio.gather(run, return_exceptions=True)
                run = asyncio.create_task(runner.run('opencode', 'ignored on attach', cwd, sid,
                    fresh=False, worker_id=1, host_id='restart', attach=True))
                self.addAsyncCleanup(self.finish_lifecycle_run, run)
                await asyncio.sleep(0.15)
                release.set()
                result = await asyncio.wait_for(run, 60)
            finally:
                release.set()
        self.assertTrue(result.success, result)
        self.assertEqual(result.text, 'first answer')
        self.assertEqual(worker_requests(requests), 1)
        self.assertEqual(prompt_responses(result), ['end_turn'])
        wire = [json.loads(json.loads(line)['line']) for line in (directory / 'writes.jsonl').read_text().splitlines()]
        self.assertEqual(sum(e.get('method') == 'session/prompt' for e in wire), 1)
        self.assertEqual(egress, [])
        print('CONTRACT reattach: model_requests=1 prompt_responses=end_turn')

    async def test_marker_finalization_preserves_all_worker_files(self):
        original = opencode_runtime.finish
        observations = []
        def finish(result, root, alias, error, **kwargs):
            def snapshot():
                return {str(p.relative_to(root)): (p.read_bytes() if p.is_file() else None,
                        p.stat().st_mode, p.stat().st_mtime_ns) for p in Path(root).rglob('*')}
            self.assertTrue((Path(result.log_path).parent / 'exit.json').is_file())
            self.assertEqual(prompt_responses(result), ['cancelled'])
            before = snapshot()
            original(result, root, alias, error, **kwargs)
            self.assertEqual(snapshot(), before)
            observations.append(True)
        with stub_servers(mode='empty') as (url, proxy, requests, egress), \
                patch('coordinator.opencode_runtime.finish', finish):
            result, cwd = await capture_run(self.root, self.binary, url, proxy, 'empty')
        self.assertEqual(result.failure_code, 'provider_stream_incomplete')
        self.assertEqual(observations, [True])
        self.assertEqual(egress, [])


def worker_requests(requests):
    return sum(bool(row['body'].get('stream') and row['body'].get('tools')) and
               any('CONTRACT_MAIN_PROMPT' in str(item.get('content', '')) for item in row['body'].get('messages', []))
               for row in requests)


def prompt_responses(result):
    directory = Path(result.log_path).parent
    return [event['result']['stopReason'] for line in (directory / 'events.jsonl').read_text().splitlines()
            if json.loads(line).get('data') for event in (json.loads(json.loads(line)['data']),)
            if 'stopReason' in (event.get('result') or {})]


def prompt_answers(result):
    directory = Path(result.log_path).parent
    sent = [json.loads(json.loads(line)['line']) for line in (directory / 'writes.jsonl').read_text().splitlines()]
    ids = {event['id'] for event in sent if event.get('method') == 'session/prompt'}
    return [event for line in (directory / 'events.jsonl').read_text().splitlines()
            if json.loads(line).get('data') for event in (json.loads(json.loads(line)['data']),)
            if event.get('id') in ids and 'method' not in event]
