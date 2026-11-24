"""Newline-delimited JSON-RPC tool server for the persistent coordinator session.

It exposes the coordinator operations and read-only views. The coordinator may set the
active Codex account or Claude pick when the owner picks one, and clear the Claude
pick on request. Other owner settings stay on Telegram and the local CLI.
"""

import asyncio
import hashlib
import inspect
import json
import os
from pathlib import Path
import subprocess
import sys

from . import control_api
from . import chat_mcp, close_saves, controls, envelopes, health, idle_trim, lifecycle, loadout, onboarding, opencode_subscription, reactions, records, session_storage, signin, topics, updates, vault, workers, workspaces
from .extension import active, optional
from .log import code_stamp


PROTOCOL_VERSION = '2025-06-18'
SECRET_NAME = '^[A-Z][A-Z0-9_]{1,63}$'
_DECLARATION = '[A-Z][A-Z0-9_]{1,63}(=[A-Z][A-Z0-9_]{1,63})?'
SECRET_NAMES = '^$|^ *' + _DECLARATION + '( *, *' + _DECLARATION + ')* *$'

COORDINATOR_OPS = ('keep.save', 'keep.drop', 'telegram.send', 'tasks.create', 'tasks.update', 'tasks.get', 'tasks.list',
                   'followup.propose', 'followup.ready', 'tasks.note', 'tasks.link', 'tasks.wait', 'pending.fire', 'pending.cancel',
                   'worktree.create', 'workers.spawn', 'workers.steer', 'workers.stop', 'workers.goal',
                   'workers.list', 'service.restart', 'secret.ask', 'secret.list', 'secret.rotate', 'secret.revoke',
                   'account.use', 'accounts.claude_auto', 'account.redeem', 'topic.create', 'topic.close', 'model.parent', 'policy.chat')
READ_OPS = ('settings.show', 'topics.list', 'topic.show', 'workers.get', 'accounts.list',
            'account.current', 'account.show', 'policy.show', 'projects.list', 'problems.list', 'problems.summary',
            'tasks.stale', 'pending.list')
active()
optional('tree_maker')
if 'trash.move' in control_api.BY_ID:
    optional('trash_move')
    COORDINATOR_OPS += ('trash.move',)

TOOLS = {op_id.replace('.', '_'): control_api.BY_ID[op_id] for op_id in COORDINATOR_OPS + READ_OPS + ('loadout.show', 'loadout.swap', 'loadout.unswap')}

ALWAYS_LOADED_TOOLS = {'telegram_send', 'tasks_create', 'tasks_update', 'tasks_get',
                       'workers_spawn', 'workers_list', 'workers_steer', 'workers_get',
                       'worktree_create', 'accounts_list', 'tasks_wait'}


def input_schema(op):
    types = {'int': 'integer', 'number': 'number', 'bool': 'boolean', 'buttons': 'array', 'wait_next': 'object'}
    properties = {}
    required = []
    for name, kind in op.params.items():
        optional = kind.endswith('?')
        base = kind.rstrip('?')
        property_schema = {'type': types.get(base, 'string')}
        if base == 'status':
            property_schema['enum'] = ['open', 'done', 'dropped']
        if base == 'job_status':
            property_schema['enum'] = ['open', 'interrupted', 'waiting', 'done', 'dropped', 'proposed', 'expired']
        if base == 'wait_kind':
            property_schema['enum'] = list(control_api.WAIT_KINDS)
        if base == 'stage':
            property_schema['enum'] = list(control_api.STAGES)
        if base == 'finish':
            property_schema['enum'] = list(control_api.FINISHES)
        if base == 'parent_provider':
            property_schema['enum'] = ['claude', 'codex', 'opencode']
        if base == 'provider':
            property_schema['enum'] = ['claude', 'codex']
        if base == 'worker_provider':
            property_schema['enum'] = list(control_api.WORKER_PROVIDERS)
        if base == 'route_kind':
            property_schema['enum'] = list(control_api.route_kinds())
            property_schema['description'] = control_api.route_kinds_text()
        if base == 'work':
            property_schema['enum'] = ['dev', 'research']
        if base == 'effort':
            property_schema['enum'] = ['low', 'medium', 'high', 'xhigh', 'max']
        if base == 'model_group':
            property_schema['enum'] = ['fable', 'other']
        if base == 'close_folder':
            property_schema['enum'] = ['trash', 'keep']
        if base in control_api.LIMITS:
            property_schema['maxLength'] = control_api.LIMITS[base]
        if base == 'secret_name':
            property_schema['pattern'] = SECRET_NAME
        if base == 'secret_names':
            property_schema['pattern'] = SECRET_NAMES
        if base == 'buttons':
            property_schema['items'] = {'type': 'array', 'items': {'type': 'object'}}
        properties[name] = property_schema
        if (not optional or name == 'message' and op.id in ('topic.create', 'topic.close', 'model.parent', 'policy.chat')
                or name == 'topic' and op.id == 'model.parent'):
            required.append(name)
    return {'type': 'object', 'properties': properties, 'required': required,
            'additionalProperties': False}


def tools_list(store=None):
    tools = [{'name': name, 'description': op.description, 'inputSchema': input_schema(op),
              'annotations': {'readOnlyHint': op.kind == control_api.READ,
                              'destructiveHint': op.id in ('topic.close', 'trash.move'), 'openWorldHint': False}}
             for name, op in TOOLS.items()]
    tools.append({'name': 'events_post',
                  'description': "Message another chat's coordinator. The owner sees it. Use only for things that chat must act on now.",
                  'inputSchema': input_schema(control_api.BY_ID['events.post']),
                  'annotations': {'readOnlyHint': False, 'destructiveHint': False, 'openWorldHint': False}})
    for tool in tools:
        if tool['name'] in ALWAYS_LOADED_TOOLS:
            tool['_meta'] = {'anthropic/alwaysLoad': True}
    return tools


def tools_hash(store=None):
    definitions = sorted(tools_list(store), key=lambda tool: tool['name'])
    return hashlib.sha256(json.dumps(definitions, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def dispatch(store, request, topic=None):
    return _dispatch(store, request, topic, control_api.call)


async def dispatch_async(store, request, topic=None):
    result = _dispatch(store, request, topic, control_api.call_async)
    return await result if inspect.isawaitable(result) else result


def _dispatch(store, request, topic, caller):
    method = request.get('method')
    params = request.get('params') or {}
    if method == 'initialize':
        return {'protocolVersion': PROTOCOL_VERSION, 'capabilities': {'tools': {}},
                'serverInfo': {'name': 'torii', 'version': '2'}}
    if method == 'tools/list':
        return {'tools': tools_list(store)}
    if method == 'tools/call':
        name = params.get('name')
        op = TOOLS.get(name)
        if name == 'events_post':
            op = control_api.BY_ID['events.post']
        if not op:
            raise ValueError('Unknown tool: ' + str(name))
        arguments = params.get('arguments') or {}
        if op.id == 'tasks.list':
            arguments = dict(arguments)
            arguments.setdefault('status', 'open')
        with store.db:
            result = caller(store, op.id, arguments, source='mcp',
                                      topic=topic if op.id in ('keep.save', 'keep.drop', 'topic.create', 'topic.close', 'model.parent', 'policy.chat', 'followup.propose', 'followup.ready', 'loadout.swap', 'loadout.show', 'loadout.unswap', 'events.post') else None,
                            caller=topic)
        if inspect.isawaitable(result):
            return _await_tool_result(result, store, op)
        return _tool_result(result, store, op)
    raise ValueError('Unknown method: ' + str(method))


async def _await_tool_result(pending, store, op):
    try:
        result = await pending
        if store.db.in_transaction:
            if result.ok:
                store.db.commit()
            else:
                store.db.rollback()
        return _tool_result(result, store, op)
    except BaseException:
        if store.db.in_transaction:
            store.db.rollback()
        raise


def _tool_result(result, store, op):
    data = result.data
    text = result.text
    if op.kind == control_api.WRITE:
        keys = ('id', 'number', 'worktree', 'scratch', 'base_commit', 'outbox', 'worker', 'task', 'request', 'results', 'wait', 'message')
        if op.id == 'trash.move':
            keys = ('trash', 'bytes')
        data = {key: data[key] for key in keys if key in data and (data[key] is None or isinstance(data[key], (str, int, float)))} if isinstance(data, dict) else {}
    elif op.id == 'tasks.list' and result.ok:
        rows = []
        for row in data:
            stage = store.db.execute('SELECT stage FROM task_stamps WHERE task=? ORDER BY id DESC LIMIT 1', (row['id'],)).fetchone()
            count = store.db.execute("SELECT count(*) FROM workers WHERE task=? AND status IN ('queued','waiting_for_secret','waiting_for_quota','running')", (row['id'],)).fetchone()[0]
            rows.append(dict(id=row['id'], number=row['number'], topic=row['topic'], title=row['title'], status=row['status'], stage=stage['stage'] if stage else None, open_worker_count=count))
        data = rows
        text = 'Jobs.' if rows else 'No jobs.'
    encoded = json.dumps(data, default=str, separators=(',', ':'))
    return {'content': [{'type': 'text', 'text': text + '\n' + encoded}], 'isError': not result.ok}


def current_code_response(store, project_root, line, request_id, topic=None, *, timeout=None):
    try:
        done = subprocess.run(
            [sys.executable, '-m', 'coordinator', '--state-dir', str(store.directory), 'mcp'] +
            (['--topic=' + topic] if topic else []),
            cwd=project_root, env=dict(os.environ, PYTHONPATH=str(project_root)),
            input=line, capture_output=True, text=True, timeout=timeout)
        if done.returncode == 0:
            try:
                response = json.loads(done.stdout)
                if isinstance(response, dict) and 'id' in response and response['id'] == request_id:
                    return response
            except ValueError:
                pass
        last_error = next((row.strip() for row in reversed(done.stderr.splitlines()) if row.strip()), '')
        detail = 'exit code %s: %s' % (done.returncode, last_error[:200])
    except Exception as error:
        detail = str(error)
    return {'jsonrpc': '2.0', 'id': request_id,
            'error': {'code': -32603, 'message': 'Tool call failed in the current Torii code: ' + detail}}


def serve(store, code, source=None, target=None, topic=None):
    source = source or sys.stdin
    target = target or sys.stdout
    project_root = None
    server_stamp = code
    ready = os.environ.get('TORII_MCP_READY')
    marked = False
    try:
        project_root = Path(__file__).resolve().parents[1]
    except Exception:
        pass
    for line in source:
        request = None
        try:
            request = json.loads(line)
            if not isinstance(request, dict) or request.get('jsonrpc') != '2.0':
                raise ValueError('Expected a JSON-RPC 2.0 request')
            if 'id' not in request:
                continue
            fresh = False
            try:
                if request.get('method') == 'tools/call':
                    current_stamp = code_stamp(project_root)
                    fresh = (current_stamp is not None and current_stamp != server_stamp
                             and current_stamp == (store.get('service_run') or {}).get('code'))
            except Exception:
                pass
            if fresh:
                options = {'topic': topic} if topic else {}
                params = request.get('params')
                if isinstance(params, dict) and params.get('name') == 'trash_move':
                    options['timeout'] = 120
                response = current_code_response(store, project_root, line, request['id'], **options)
            else:
                if request.get('method') == 'tools/call' and (request.get('params') or {}).get('name') == 'tasks_update':
                    result = asyncio.run(dispatch_async(store, request, topic=topic) if topic else
                                         dispatch_async(store, request))
                else:
                    result = dispatch(store, request, topic=topic) if topic else dispatch(store, request)
                response = {'jsonrpc': '2.0', 'id': request['id'], 'result': result}
        except BaseException as error:
            response = {'jsonrpc': '2.0', 'id': request.get('id') if isinstance(request, dict) else None,
                        'error': {'code': -32602, 'message': str(error)}}
        target.write(json.dumps(response, separators=(',', ':'), default=str) + '\n')
        target.flush()
        if (ready and not marked and isinstance(request, dict)
                and request.get('method') == 'tools/list' and 'result' in response):
            from .opencode_runtime import write_private
            write_private(Path(ready), b'{}')
            marked = True
