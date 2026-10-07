"""Newline-delimited JSON-RPC tool server for the persistent coordinator session.

It exposes the coordinator operations and read-only views. The coordinator may set the
active Codex account when the owner picks one. Other account choices and owner settings
stay on Telegram and the local CLI.
"""

import hashlib
import json
import sys

from . import control_api


PROTOCOL_VERSION = '2025-06-18'
SECRET_NAME = '^[A-Z][A-Z0-9_]{1,63}$'
_DECLARATION = '[A-Z][A-Z0-9_]{1,63}(=[A-Z][A-Z0-9_]{1,63})?'
SECRET_NAMES = '^$|^ *' + _DECLARATION + '( *, *' + _DECLARATION + ')* *$'

COORDINATOR_OPS = ('telegram.send', 'tasks.create', 'tasks.update', 'tasks.get', 'tasks.list',
                   'worktree.create', 'workers.spawn', 'workers.steer', 'workers.stop', 'workers.goal',
                   'workers.list', 'service.restart', 'secret.ask', 'secret.list', 'secret.rotate', 'secret.revoke',
                   'account.use', 'account.redeem')
READ_OPS = ('settings.show', 'topics.list', 'topic.show', 'workers.get', 'accounts.list',
            'account.current', 'account.show', 'policy.show', 'projects.list', 'problems.list', 'problems.summary',
            'tasks.stale')
TOOLS = {op_id.replace('.', '_'): control_api.BY_ID[op_id] for op_id in COORDINATOR_OPS + READ_OPS}


def input_schema(op):
    types = {'int': 'integer', 'bool': 'boolean', 'buttons': 'array'}
    properties = {}
    required = []
    for name, kind in op.params.items():
        optional = kind.endswith('?')
        base = kind.rstrip('?')
        property_schema = {'type': types.get(base, 'string')}
        if base == 'status':
            property_schema['enum'] = ['open', 'done', 'dropped']
        if base == 'provider':
            property_schema['enum'] = ['claude', 'codex']
        if base == 'work':
            property_schema['enum'] = ['dev', 'research']
        if base == 'effort':
            property_schema['enum'] = ['low', 'medium', 'high', 'max']
        if base in control_api.LIMITS:
            property_schema['maxLength'] = control_api.LIMITS[base]
        if base == 'secret_name':
            property_schema['pattern'] = SECRET_NAME
        if base == 'secret_names':
            property_schema['pattern'] = SECRET_NAMES
        if base == 'buttons':
            property_schema['items'] = {'type': 'array', 'items': {'type': 'object'}}
        properties[name] = property_schema
        if not optional:
            required.append(name)
    return {'type': 'object', 'properties': properties, 'required': required,
            'additionalProperties': False}


def tools_list():
    return [{'name': name, 'description': op.description, 'inputSchema': input_schema(op),
             'annotations': {'readOnlyHint': op.kind == control_api.READ,
                             'destructiveHint': False, 'openWorldHint': False}}
            for name, op in TOOLS.items()]


def tools_hash():
    definitions = sorted(tools_list(), key=lambda tool: tool['name'])
    return hashlib.sha256(json.dumps(definitions, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def dispatch(store, request):
    method = request.get('method')
    params = request.get('params') or {}
    if method == 'initialize':
        return {'protocolVersion': PROTOCOL_VERSION, 'capabilities': {'tools': {}},
                'serverInfo': {'name': 'torii', 'version': '2'}}
    if method == 'tools/list':
        return {'tools': tools_list()}
    if method == 'tools/call':
        name = params.get('name')
        op = TOOLS.get(name)
        if not op:
            raise ValueError('Unknown tool: ' + str(name))
        arguments = params.get('arguments') or {}
        with store.db:
            result = control_api.call(store, op.id, arguments, source='mcp')
        data = json.dumps(result.data, default=str, separators=(',', ':'))
        return {'content': [{'type': 'text', 'text': result.text + '\n' + data}],
                'isError': not result.ok}
    raise ValueError('Unknown method: ' + str(method))


def serve(store, source=None, target=None):
    source = source or sys.stdin
    target = target or sys.stdout
    for line in source:
        request = None
        try:
            request = json.loads(line)
            if not isinstance(request, dict) or request.get('jsonrpc') != '2.0':
                raise ValueError('Expected a JSON-RPC 2.0 request')
            if 'id' not in request:
                continue
            result = dispatch(store, request)
            response = {'jsonrpc': '2.0', 'id': request['id'], 'result': result}
        except (TypeError, ValueError, KeyError) as error:
            response = {'jsonrpc': '2.0', 'id': request.get('id') if isinstance(request, dict) else None,
                        'error': {'code': -32602, 'message': str(error)}}
        target.write(json.dumps(response, separators=(',', ':'), default=str) + '\n')
        target.flush()
