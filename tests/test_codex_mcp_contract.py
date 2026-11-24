"""Opt-in local contract for Codex MCP headers and inherited configuration."""

import asyncio
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import shlex
import shutil
import tempfile
import threading
import unittest

from coordinator.codex_accounts import AppServer
from coordinator.codex_session import toml
from coordinator.shared_mcp import codex_inherited, codex_launch


@unittest.skipUnless(os.environ.get('TORII_CODEX_MCP_CONTRACT') == '1', 'opt-in Codex MCP contract')
class CodexMcpContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_private_header_reaches_the_selected_server_and_the_native_one_stays(self):
        received = []
        ready = threading.Event()

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                self.send_response(405)
                self.end_headers()

            def do_POST(self):
                data = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                received.append((self.path, self.headers.get('X-Torii-Test')))
                ready.set()
                if 'id' not in data:
                    self.send_response(202)
                    self.end_headers()
                    return
                result = ({'protocolVersion': '2025-03-26', 'capabilities': {'tools': {}},
                           'serverInfo': {'name': 'fixture', 'version': '1'}}
                          if data.get('method') == 'initialize' else {'tools': []})
                body = json.dumps({'jsonrpc': '2.0', 'id': data['id'], 'result': result}).encode()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        with tempfile.TemporaryDirectory(prefix='torii-codex-mcp-') as temporary:
            root = Path(temporary)
            home = root / 'home'
            account = root / 'account'
            home.mkdir()
            account.mkdir()
            binary = shutil.which('codex')
            sandbox = shutil.which('sandbox-exec')
            self.assertTrue(binary and sandbox)
            version = await asyncio.create_subprocess_exec(binary, '--version', stdout=asyncio.subprocess.PIPE,
                                                           stderr=asyncio.subprocess.DEVNULL)
            version_output, _ = await version.communicate()
            self.assertEqual(version.returncode, 0)
            print('Codex MCP contract binary:', version_output.decode().strip(), flush=True)
            profile = ('(version 1)(allow default)(deny network*)'
                       '(allow network-outbound (remote ip "localhost:%d"))') % server.server_port
            wrapper = root / 'codex'
            wrapper.write_text('#!/bin/sh\nexec ' + shlex.join([sandbox, '-p', profile, binary]) + ' "$@"\n')
            wrapper.chmod(0o700)
            url = 'http://127.0.0.1:%d' % server.server_port
            (account / 'config.toml').write_text('[mcp_servers.inherited]\nurl = ' + json.dumps(url + '/inherited') +
                                                 '\nrequired = true\n')
            env = {'HOME': str(home), 'CODEX_HOME': str(account), 'PATH': os.environ.get('PATH', '/usr/bin:/bin')}
            entries = {'selected': {'type': 'http', 'url': url + '/selected',
                                    'headers': {'X-Torii-Test': 'DUMMY-HEADER-CONTRACT'}}}
            selected, private = codex_launch(entries)
            self.assertEqual(await codex_inherited(str(wrapper), env, root), {'inherited'})
            command = ['-c', 'mcp_servers=' + toml(selected),
                       '-c', 'model_provider="mock"', '-c', 'model_providers.mock=' + toml({
                           'name': 'Mock', 'base_url': url + '/v1', 'wire_api': 'responses',
                           'requires_openai_auth': False})]
            self.assertNotIn('DUMMY-HEADER-CONTRACT', json.dumps(command))
            client = AppServer(str(wrapper), dict(env, **private))
            try:
                await asyncio.wait_for(client.start(settings=command, cwd=str(root)), 40)
                await asyncio.wait_for(client.request('thread/start', {
                    'model': 'gpt-5.5', 'cwd': str(root), 'approvalPolicy': 'never',
                    'sandbox': 'danger-full-access'}), 40)
                self.assertTrue(await asyncio.to_thread(ready.wait, 30))
                self.assertTrue(any(path == '/selected' and value == 'DUMMY-HEADER-CONTRACT'
                                    for path, value in received))
                self.assertTrue(any(path == '/inherited' for path, _ in received))
            finally:
                await client.stop()
