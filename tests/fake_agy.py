#!/usr/bin/env python3
"""Installable fake of the agy CLI, shaped by the live 1.5.12 capture.

scenario.json beside the executable selects behavior. HOME is the profile: a
file named signed-in marks a signed-in profile, and sessions.json keeps sessions.
TORII_FAKE_SESSIONS stands for the session folder that Torii binds for a worker: the fake writes its sessions there.
"""

from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import time
from urllib.parse import parse_qs, quote, urlsplit
import uuid


MODELS = [('model-a', 'Model A'), ('model-b', 'Model B (Thinking)')]
LEVELS = ('low', 'medium', 'high')
USAGE = {'inputTokens': 100, 'outputTokens': 20, 'cachedReadTokens': 300, 'cachedWriteTokens': 0,
         'thoughtTokens': 5, 'totalTokens': 425}
QUOTA = {'groups': [{'name': 'Group A', 'buckets': [
    {'id': 'a-5h', 'name': '5-Hour Limit Remaining', 'window': '5h', 'remaining_fraction': 0.75,
     'reset_time': '2030-01-01T05:00:00Z'},
    {'id': 'a-weekly', 'name': 'Weekly Limit Remaining', 'window': 'weekly', 'remaining_fraction': 0.5,
     'reset_time': '2030-01-07T00:00:00Z'}]}]}


def install_fake_agy(directory, **scenario):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    binary = directory / 'agy'
    shutil.copyfile(__file__, binary)
    binary.chmod(0o700)
    (directory / 'scenario.json').write_text(json.dumps(scenario))
    return binary


def sign_in(home):
    home = Path(home)
    home.mkdir(parents=True, exist_ok=True, mode=0o700)
    (home / 'signed-in').write_text('fixture\n')
    return home


def config(model, level):
    return [{'id': 'model', 'name': 'Model', 'category': 'model', 'type': 'select', 'currentValue': model,
             'options': [{'value': value, 'name': name} for value, name in MODELS]},
            {'id': 'thought_level', 'name': 'Thinking Level', 'category': 'thought_level', 'type': 'select',
             'currentValue': level, 'options': [{'value': value, 'name': value.title()} for value in LEVELS]},
            {'id': 'mode', 'name': 'Mode', 'category': 'mode', 'type': 'select', 'currentValue': 'default',
             'options': [{'value': 'default', 'name': 'Default'}]}]


class Agent:
    def __init__(self, here, scenario):
        self.here = here
        self.scenario = scenario
        self.home = Path(os.environ['HOME'])
        self.writes = Path(os.environ.get('TORII_FAKE_SESSIONS') or self.home / '.gemini' / 'antigravity-cli')
        self.lock = threading.Lock()
        self.pending = None
        self.model = MODELS[0][0]
        self.level = 'high'
        self.turns = 0
        self.servers = None

    def record(self, message):
        with (self.here / 'calls.jsonl').open('a') as stream:
            stream.write(json.dumps({'message': message, 'argv': sys.argv[1:], 'cwd': os.getcwd(),
                                     'home': str(self.home), 'tmpdir': os.environ.get('TMPDIR'),
                                     'display': os.environ.get('DISPLAY'), 'path': os.environ.get('PATH'),
                                     'env': {name: os.environ.get(name) for name in self.scenario.get('env', ())}}) + '\n')

    def send(self, event):
        with self.lock:
            sys.stdout.write(json.dumps({'jsonrpc': '2.0', **event}) + '\n')
            sys.stdout.flush()

    def update(self, session, update):
        self.send({'method': 'session/update', 'params': {'sessionId': session, 'update': update}})

    def sessions(self):
        try:
            return json.loads((self.writes / 'sessions.json').read_text())
        except (OSError, ValueError):
            return []

    def session_result(self, session):
        return {'sessionId': session, 'configOptions': config(self.model, self.level),
                '_meta': {'antigravity': {'tier': {'name': self.scenario.get('tier', 'Fixture Plan'), 'id': 'fixture'}}}}

    def finish(self, request, reason):
        self.update(request['params']['sessionId'], {'sessionUpdate': 'usage_update', 'used': 1, 'size': 1000})
        self.send({'id': request['id'], 'result': {'stopReason': reason, 'usage': USAGE}})

    def prompt(self, request):
        session = request['params']['sessionId']
        text = ''.join(block.get('text', '') for block in request['params']['prompt'])
        self.turns += 1
        if self.scenario.get('quota'):
            self.send({'id': request['id'], 'result': {'stopReason': 'refusal', 'usage': USAGE}})
            return
        if self.scenario.get('prompt_error'):
            self.send({'id': request['id'], 'error': self.scenario['prompt_error']})
            return
        if self.scenario.get('upstream_error'):
            self.update(session, {'sessionUpdate': 'agent_message_chunk',
                                  'content': {'type': 'text', 'text': self.scenario['upstream_error']}})
            self.send({'id': request['id'], 'result': {'stopReason': 'end_turn'}})
            return
        report = []
        if self.scenario.get('image'):
            brain = self.writes / 'brain' / session
            brain.mkdir(parents=True, exist_ok=True)
            (brain / ('picture_%d.jpg' % self.turns)).write_bytes(b'fixture-jpeg')
            (brain / 'notes.md').write_text('private notes')
            (brain / 'nested.png').mkdir(exist_ok=True)
            if not (brain / 'linked.png').is_symlink():
                (brain / 'linked.png').symlink_to(self.home / 'signed-in')
        if self.scenario.get('subagent'):
            brain = self.writes / 'brain' / session
            generated = brain / '.system_generated'
            child = '33333333-3333-4333-8333-333333333333'
            if self.turns == 1:
                (generated / 'subagents').mkdir(parents=True, exist_ok=True)
                (generated / 'subagents' / (child + '.json')).write_text(json.dumps(
                    {'conversationId': child, 'state': 'SUBAGENT_STATE_ALIVE'}))
            elif self.turns == self.scenario['subagent']:
                (generated / 'messages').mkdir(parents=True, exist_ok=True)
                (generated / 'messages' / 'message-1.json').write_text(json.dumps({'id': 'message-1', 'sender': child}))
                (generated / 'messages' / 'read.json').write_text(json.dumps({'message-1': True}))
                (brain / 'subagent_image.png').write_bytes(b'fixture-png')
        for target in self.scenario.get('writes', []):
            try:
                Path(target).write_text('written\n')
                report.append('wrote ' + target)
            except OSError as error:
                report.append('refused ' + target + ' ' + type(error).__name__)
        self.update(session, {'sessionUpdate': 'tool_call', 'toolCallId': 'call-' + str(self.turns),
                              'title': self.scenario.get('tool', 'fixture tool'), 'kind': 'execute', 'status': 'in_progress'})
        if self.scenario.get('echo_headers'):
            report.append(json.dumps(self.servers))
        reply = 'turn %d %s:%s %s' % (self.turns, self.model, self.level, ' | '.join(report) or text[-40:])
        self.update(session, {'sessionUpdate': 'agent_message_chunk', 'content': {'type': 'text', 'text': reply}})
        if self.scenario.get('hold') and not (self.here / 'release').exists():
            previous, self.pending = self.pending, request
        else:
            previous, self.pending = self.pending, None
            self.finish(request, 'end_turn')
        if previous is not None:
            self.finish(previous, 'end_turn')

    def release(self):
        while True:
            time.sleep(0.02)
            pending = self.pending
            if pending is not None and (self.here / 'release').exists():
                self.pending = None
                self.finish(pending, 'end_turn')

    def authenticate(self, request):
        """Mimic the live flow: a loopback listener, then the link goes to the browser launcher."""
        if (self.home / 'signed-in').exists():
            self.send({'id': request['id'], 'result': {}})
            return
        if not os.environ.get('DISPLAY'):
            self.send({'id': request['id'], 'error': {'code': -32603, 'message': 'No browser is available here '
                       '(no graphical display is available to open a browser).'}})
            return
        agent = self
        state = self.scenario.get('state', 'fixture-state')

        class Callback(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                parsed = urlsplit(self.path)
                query = parse_qs(parsed.query)
                agent.record({'method': 'callback', 'path': parsed.path, 'query': query,
                              'host': self.headers.get('host')})
                good = parsed.path == '/auth/callback' and query.get('state') == [state]
                self.send_response(200 if good else 400)
                self.end_headers()
                if not good:
                    return
                if query.get('code') == ['good-code']:
                    (agent.home / 'signed-in').write_text('fixture\n')
                    agent.send({'id': request['id'], 'result': {}})
                else:
                    agent.send({'id': request['id'], 'error': {'code': -32000, 'message': 'Sign-in failed.'}})

        server = HTTPServer(('127.0.0.1', 0), Callback)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        host = self.scenario.get('link_host', 'accounts.google.com')
        link = ('https://%s/o/oauth2/auth?access_type=offline&client_id=fixture&code_challenge=fixture'
                '&redirect_uri=%s&response_type=code&scope=openid&state=%s'
                % (host, quote('http://localhost:%d/auth/callback' % server.server_port, safe=''), state))
        subprocess.run(['xdg-open', link], check=False)

    def handle(self, request):
        method = request.get('method')
        params = request.get('params') or {}
        if method == 'authenticate':
            self.authenticate(request)
            return
        if method == 'initialize':
            self.send({'id': request['id'], 'result': {
                'protocolVersion': self.scenario.get('protocol', 1),
                'agentCapabilities': {'loadSession': True, 'sessionCapabilities': {'resume': {}, 'load': {}}},
                'agentInfo': {'name': 'Antigravity ACP Server', 'version': self.scenario.get('version', '1.5.12')},
                'authMethods': [{'id': 'oauth-personal', 'name': 'Log in with Google'}]}})
        elif method in ('session/new', 'session/resume'):
            if not (self.home / 'signed-in').exists():
                self.send({'id': request['id'], 'error': {'code': -32000, 'message': 'Authentication required'}})
                return
            self.servers = params.get('mcpServers')
            for server in self.servers or []:
                ready = next((item['value'] for item in server.get('env', []) if item['name'] == 'TORII_MCP_READY'), None)
                if ready and not self.scenario.get('mcp_missing'):
                    Path(ready).write_text('{}')
            known = self.sessions()
            session = params.get('sessionId') or str(uuid.uuid4())
            if method == 'session/resume' and session not in known:
                self.send({'id': request['id'], 'error': {'code': -32602, 'message': 'Session not found: ' + session}})
                return
            if session not in known:
                saved = self.writes / 'conversations'
                saved.mkdir(parents=True, exist_ok=True)
                (self.writes / 'sessions.json').write_text(json.dumps(known + [session]))
                (saved / (session + '.db')).write_bytes(b'fixture')
            self.send({'id': request['id'], 'result': self.session_result(session)})
            self.update(session, {'sessionUpdate': 'usage_update', 'used': 0, 'size': 1000})
        elif method == 'session/set_config_option':
            if params.get('configId') == 'model':
                self.model = params['value']
            elif params.get('configId') == 'thought_level':
                self.level = params['value']
            self.send({'id': request['id'], 'result': {'configOptions': config(self.model, self.level)}})
        elif method == 'session/prompt':
            self.prompt(request)
        elif method == 'session/cancel':
            pending, self.pending = self.pending, None
            if pending is not None:
                self.finish(pending, 'cancelled')
        elif 'id' in request:
            self.send({'id': request['id'], 'error': {'code': -32601, 'message': 'Method not found'}})


def main():
    here = Path(__file__).resolve().parent
    scenario = json.loads((here / 'scenario.json').read_text())
    home = Path(os.environ['HOME'])
    signed_in = (home / 'signed-in').exists()
    if sys.argv[1:2] == ['models']:
        print('Fetching available models...')
        if not signed_in:
            print('Error: Please sign in to view available models. Launch the CLI without arguments to sign in.')
            return 1
        if scenario.get('models_error'):
            print('Error: ' + scenario['models_error'])
            return 1
        for model, name in MODELS:
            print(model + '\t' + name)
        return 0
    if '-p' in sys.argv and sys.argv[sys.argv.index('-p') + 1] == '/usage':
        with (here / 'calls.jsonl').open('a') as stream:
            stream.write(json.dumps({'argv': sys.argv[1:], 'home': str(home)}) + '\n')
        if not signed_in:
            print(json.dumps({'status': 'ERROR', 'error': 'authentication failed or timed out'}))
            return 1
        if scenario.get('usage_text'):
            print(scenario['usage_text'])
            return scenario.get('usage_exit', 0)
        data = scenario.get('usage_by_home', {}).get(home.name) or scenario.get('usage', QUOTA)
        print(json.dumps({'conversation_id': '', 'status': 'SUCCESS', 'num_turns': 0,
                          'command': {'name': 'usage', 'data': data}}))
        return 0
    if sys.argv[1:2] != ['acp']:
        return 64
    agent = Agent(here, scenario)
    threading.Thread(target=agent.release, daemon=True).start()
    for line in sys.stdin:
        request = json.loads(line)
        agent.record(request)
        agent.handle(request)
    return 0


if __name__ == '__main__':
    sys.exit(main())
