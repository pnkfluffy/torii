#!/usr/bin/env python3
"""Installable ACP fake with scenario.json beside the executable."""

import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import uuid


SESSION_ID = 'ses_1b3d19f6fffeIxa5icm1pIrpgt'


def install_fake_opencode(directory):
    from coordinator.opencode_accounts import PINNED_VERSION
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    binary = directory / 'opencode'
    shutil.copyfile(__file__, binary)
    binary.chmod(0o700)
    (directory / 'scenario.json').write_text('{}')
    (directory / 'pinned-version').write_text(PINNED_VERSION)
    return binary


def install_versioned_opencode(directory, version):
    prefix = Path(directory) / version
    manifest = prefix / 'node_modules/opencode-ai/package.json'
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(json.dumps({'name': 'opencode-ai', 'version': version}))
    binary = install_fake_opencode(prefix / 'node_modules/opencode-darwin-arm64/bin')
    (binary.parent / 'scenario.json').write_text(json.dumps({'version': version}))
    return binary


def main():
    here = Path(__file__).resolve().parent
    scenario = json.loads((here / 'scenario.json').read_text())
    version = scenario.get('version', (here / 'pinned-version').read_text())
    if '--version' in sys.argv:
        print(version)
        return
    config = json.loads((Path(os.environ['XDG_CONFIG_HOME']) / 'opencode' / 'opencode.json').read_text())
    if scenario.get('stderr'):
        print(scenario['stderr'], file=sys.stderr, flush=True)
    pending = None
    threads = []
    stopping = threading.Event()
    servers = []
    registrations = set()
    worker_root = Path(os.environ['XDG_CONFIG_HOME']).parent
    if scenario.get('scratch_files'):
        for path in (Path(os.environ['TMPDIR']) / 'scratch',
                     Path(os.environ['HOME']) / 'Library/Caches/scratch',
                     Path(os.environ['HOME']) / 'retained', worker_root / 'managed/retained',
                     Path(os.environ['XDG_DATA_HOME']) / 'retained',
                     worker_root / 'stream-incomplete.json', worker_root / 'observed-parts.jsonl'):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('{}\n')

    def emit(event):
        print(json.dumps({'jsonrpc': '2.0', **event}), flush=True)

    def reply(event, result):
        emit({'id': event['id'], 'result': result})

    def update(sid, body):
        emit({'method': 'session/update', 'params': {'sessionId': sid, 'update': body}})

    def record(sid, data, parts=(), role='assistant'):
        if scenario.get('no_records'):
            return
        directory = Path(os.environ['XDG_DATA_HOME']) / 'opencode'
        directory.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(str(directory / 'opencode.db')) as db:
            db.execute('CREATE TABLE IF NOT EXISTS message (id TEXT, session_id TEXT, time_created INTEGER, data TEXT)')
            db.execute('CREATE TABLE IF NOT EXISTS part (id TEXT, message_id TEXT, session_id TEXT, data TEXT)')
            mid = 'msg_' + uuid.uuid4().hex
            db.execute('INSERT INTO message VALUES (?,?,?,?)', (mid, sid, time.time_ns(), json.dumps({'role': role, **data})))
            for index, part in enumerate(parts):
                db.execute('INSERT INTO part VALUES (?,?,?,?)', (mid + str(index), mid, sid, json.dumps(part)))

    def register(sid, entries):
        registration = (sid, json.dumps(entries, sort_keys=True))
        if registration in registrations:
            return
        registrations.add(registration)
        for entry in entries:
            env = dict(os.environ, **{item['name']: item['value'] for item in entry.get('env', [])})
            if scenario.get('mcp_real'):
                child = subprocess.Popen([entry['command'], *entry.get('args', [])], env=env,
                                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                servers.append(child)
                for request_id, method in enumerate(('initialize', 'tools/list'), 1):
                    child.stdin.write(json.dumps({'jsonrpc': '2.0', 'id': request_id, 'method': method}) + '\n')
                    child.stdin.flush()
                    child.stdout.readline()
            elif not scenario.get('mcp_missing') and env.get('TORII_MCP_READY'):
                ready = Path(env['TORII_MCP_READY'])
                descriptor = os.open(ready, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
                with os.fdopen(descriptor, 'w') as marker:
                    marker.write('{}')

    history_path = Path(os.environ['XDG_DATA_HOME']) / 'fake-sessions.json'
    sessions = json.loads(history_path.read_text()) if history_path.exists() else {}

    def save_sessions():
        history_path.write_text(json.dumps(sessions))

    def finish(event):
        sid = event['params']['sessionId']
        if scenario.get('release'):
            while not Path(scenario['release']).exists():
                if stopping.wait(0.01):
                    return
        if scenario.get('response_file'):
            scenario.update(json.loads(Path(scenario['response_file']).read_text()))
        text = '\n\n'.join(part['text'] for part in event['params']['prompt'])
        previous = list(sessions.get(sid, []))[:-1]
        if scenario.get('history_reply'):
            scenario['chunks'] = ['history: ' + ' | '.join(previous) + '; next: ' + text]
        chunks = [] if scenario.get('only_owner_text') and 'kind=owner' not in text else scenario.get('chunks', ['fresh ', 'answer'])
        for text in chunks:
            update(sid, {'sessionUpdate': 'agent_message_chunk', 'content': {'type': 'text', 'text': text}})
        if not scenario.get('no_cost'):
            update(sid, {'sessionUpdate': 'usage_update', 'cost': {'amount': scenario.get('cost', 0.12), 'currency': 'USD'}})
        if scenario.get('stream_incomplete'):
            marker = scenario.get('marker', {'version': 1, 'code': 'provider_stream_incomplete',
                                            'session_id': sid, 'consecutive': 3})
            marker_path = worker_root / 'stream-incomplete.json'
            kind = scenario.get('marker_kind')
            if kind == 'directory':
                marker_path.mkdir()
            elif kind == 'fifo':
                os.mkfifo(marker_path)
            elif kind != 'missing':
                target = worker_root.parent / 'outside-marker.json' if kind == 'symlink' else marker_path
                target.write_text(scenario.get('marker_raw', json.dumps(marker)))
                target.chmod(0o600)
                if kind == 'symlink':
                    marker_path.symlink_to(target)
        record(sid, {'finish': scenario.get('finish', 'stop')},
               scenario.get('parts', [{'type': 'text', 'text': ''.join(scenario.get('chunks', ['fresh ', 'answer'])),
                                       'time': {'start': 1, 'end': 2}}]))
        reply(event, {'stopReason': scenario.get('stop_reason', 'cancelled' if scenario.get('stream_incomplete') else 'end_turn'),
                      'usage': {'inputTokens': 123, 'outputTokens': 45, 'cachedReadTokens': 67}})

    for line in sys.stdin:
        event = json.loads(line)
        with (here / 'calls.jsonl').open('a') as calls:
            calls.write(json.dumps({'message': event, 'argv': sys.argv[1:], 'pid': os.getpid(),
                                    'env_names': sorted(os.environ), 'config': config}) + '\n')
        method = event.get('method')
        if method == 'initialize':
            launch = worker_root / 'guard-launch.json'
            if launch.is_file() and not scenario.get('guard_unavailable'):
                (worker_root / 'guard-ready.json').write_bytes(launch.read_bytes())
            reply(event, {'protocolVersion': 1, 'agentInfo': {'name': 'opencode', 'version': version}})
        elif method == 'session/new':
            sid = scenario.get('session_id', ('ses_' + uuid.uuid4().hex[:26]) if scenario.get('unique_sessions') else SESSION_ID)
            sessions[sid] = []
            save_sessions()
            register(sid, event['params'].get('mcpServers', []))
            reply(event, {'sessionId': sid})
        elif method == 'session/load':
            if scenario.get('load_chunks'):
                for text in scenario['load_chunks']:
                    update(event['params']['sessionId'], {'sessionUpdate': 'agent_message_chunk',
                           'content': {'type': 'text', 'text': text}})
            if scenario.get('load_missing') or event['params']['sessionId'] not in sessions:
                emit({'id': event['id'], 'error': {'code': -32603,
                      'message': 'Internal error: OpenCode service failure', 'data': {'service': 'session'}}})
            else:
                register(event['params']['sessionId'], event['params'].get('mcpServers', []))
                update(event['params']['sessionId'], {'sessionUpdate': 'agent_message_chunk',
                                                     'content': {'type': 'text', 'text': 'old history ' + ' | '.join(sessions[event['params']['sessionId']])}})
                reply(event, {})
        elif method == 'session/prompt':
            sid = event['params']['sessionId']
            text = '\n\n'.join(part['text'] for part in event['params']['prompt'])
            sessions.setdefault(sid, []).append(text)
            save_sessions()
            record(sid, {}, [{'type': 'text', 'text': text}], role='user')
            for tool_call in scenario.get('tool_calls', []):
                update(sid, tool_call)
            if scenario.get('compaction'):
                update(sid, {'sessionUpdate': 'agent_message_chunk', 'content': {'type': 'text', 'text': 'fake compaction'}})
            if scenario.get('crash'):
                raise SystemExit(7)
            if scenario.get('session_errors'):
                guard = worker_root / 'config/opencode/plugins/guard.mjs'
                shutil.copyfile(guard.with_suffix('.js'), guard)
                driver = '''
import { readFileSync } from 'node:fs'
import { pathToFileURL } from 'node:url'
const input = JSON.parse(readFileSync(0, 'utf8'))
const hooks = await (await import(pathToFileURL(input.guard).href)).default({ client: {} })
for (const error of input.errors) {
  await hooks.event({ event: { type: 'session.error', properties: {
    sessionID: error.session_id ?? input.session, error: error.error
  } } })
}
'''
                subprocess.run(['node', '--input-type=module', '-e', driver], check=True,
                               input=json.dumps({'guard': str(guard), 'session': sid,
                                                 'errors': scenario['session_errors']}), text=True)
            if scenario.get('context_overflow'):
                original = scenario.get('provider_error')
                marker_path = worker_root / 'provider-error.json'
                if original and marker_path.is_file():
                    marker = json.loads(marker_path.read_text())
                    marker['error'] = original
                    marker_path.write_text(json.dumps(marker))
                record(sid, {'error': {'name': 'ContextOverflowError', 'data': {
                    'message': 'Conversation history too large to compact - exceeds model context limit'}}})
                emit({'id': event['id'], 'error': {'code': -32603,
                      'message': 'Internal error: Conversation history too large to compact - exceeds model context limit',
                      'data': {'service': 'session', 'errorName': 'ContextOverflowError'}}})
            elif scenario.get('http_error'):
                status = scenario['http_error']
                info = {'error': {'name': 'APIError',
                       'data': {'statusCode': status, 'responseHeaders': scenario.get('response_headers', {}),
                                'responseBody': scenario.get('response_body', '')}}}
                info['error'] = scenario.get('saved_error', info['error'])
                record(event['params']['sessionId'], info)
                emit({'id': event['id'], 'error': {'code': -32603, 'message': 'Internal error: ' + scenario.get('error_message', {401: 'Invalid API key provided',
                         402: 'Insufficient balance, please top up', 403: 'OpenCode subscription subscription required',
                         429: 'Request capacity exhausted'}[status]),
                     'data': {'service': 'session', 'errorName': 'APIError'}}})
            elif scenario.get('permission'):
                pending = event
                emit({'id': 'permission-1', 'method': 'session/request_permission', 'params': {
                    'sessionId': event['params']['sessionId'], 'toolCall': {'title': 'outside file'},
                    'options': [{'optionId': 'deny-this', 'kind': 'reject_once'},
                                {'optionId': 'allow-this', 'kind': 'allow_once'}]}})
            elif scenario.get('release'):
                pending = event
                thread = threading.Thread(target=finish, args=(event,))
                threads.append(thread)
                thread.start()
            elif scenario.get('wait_cancel'):
                pending = event
            else:
                finish(event)
        elif method == 'session/cancel' and pending:
            mode = scenario.get('cancel_mode')
            if mode == 'exit':
                stopping.set()
                break
            if mode == 'close_then_exit':
                sys.stdout.close()
                (here / 'stdout-closed').touch()
                time.sleep(0.2)
                os._exit(0)
            if mode == 'ignore':
                continue
            if mode == 'close_stdout':
                sys.stdout.close()
                (here / 'stdout-closed').touch()
                continue
            if mode == 'finish':
                finish(pending)
            else:
                stopping.set()
                reply(pending, {'stopReason': 'cancelled'})
            stopping.set()
            pending = None
        elif event.get('id') == 'permission-1' and pending:
            finish(pending)
            pending = None
        elif method and 'id' in event:
            emit({'id': event['id'], 'error': {'code': -32601, 'message': 'Method not found'}})
    stopping.set()
    for thread in threads:
        thread.join()
    for child in servers:
        child.stdin.close()
        child.wait(timeout=20)
        child.stdout.close()
        child.stderr.close()
    raise SystemExit(scenario.get('eof_exit', 0))


if __name__ == '__main__':
    main()
