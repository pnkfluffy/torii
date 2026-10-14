#!/usr/bin/env python3
"""Installable NDJSON ACP fake shared by Grok account, sign-in and worker tests.

accounts.json next to the executable maps GROK_HOME basenames to behavior.
Keys include signed_in, gate, used, period, reset, billing, email, tier,
device (approve, deny, never), device_delay, mode, auth_url, quota and hang.
hang may be true or a list of methods. prompt_delay leaves time for interject
and cancel. client_request asks the ACP client to reject an unsupported request.
Only messages, argv, home basenames and environment names enter calls.jsonl.
"""

import copy
import json
import os
from pathlib import Path
import select
import shutil
import signal
import sys
import time


FIXTURE = Path(__file__).parent / 'fixtures' / 'grok-acp-1.0.52.jsonl'


def install_fake_grok(directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    executable = directory / 'grok'
    shutil.copyfile(__file__, executable)
    executable.chmod(0o700)
    shutil.copyfile(FIXTURE, directory / FIXTURE.name)
    (directory / 'accounts.json').write_text('{}')
    return executable


def main():
    here = Path(__file__).resolve().parent
    home = Path(os.environ['GROK_HOME'])
    fixtures = {row['name']: row for row in
                (json.loads(line) for line in (here / 'grok-acp-1.0.52.jsonl').read_text().splitlines())}
    pending_auth = None
    pending_prompt = None
    client_wait = None
    authenticated = False

    def config():
        return json.loads((here / 'accounts.json').read_text()).get(home.name, {})

    def signed_in():
        return authenticated or config().get('signed_in', False) or (home / 'signed-in').exists()

    def emit(message):
        print(json.dumps(message), flush=True)

    def reply(name, request, result=None):
        message = copy.deepcopy(fixtures[name]['response'])
        message['id'] = request['id']
        if result is not None:
            message['result'] = result
        emit(message)

    def auth_result():
        result = copy.deepcopy(fixtures['authenticate']['response']['result'])
        cfg = config()
        result['_meta'].update(email=cfg.get('email', 'worker@example.test'), gate=cfg.get('gate'),
                               subscription_tier=cfg.get('tier', 'SuperGrok'))
        return result

    def error(request, code, message, data=None):
        detail = {'code': code, 'message': message}
        if data is not None:
            detail['data'] = data
        emit({'jsonrpc': '2.0', 'id': request['id'], 'error': detail})

    def terminate(number, frame):
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, terminate)
    if 'agent' not in sys.argv:
        emit(fixtures['headless_signed_out']['response'])
        return
    buffer = b''
    while True:
        cfg = config()
        now = time.monotonic()
        if pending_auth and now >= pending_auth[1]:
            outcome = cfg.get('device', 'never')
            if outcome == 'approve' or (here / ('approve-' + home.name)).exists():
                authenticated = True
                (home / 'signed-in').write_text('fake login')
                reply('authenticate', pending_auth[0], auth_result())
                pending_auth = None
            elif outcome == 'deny':
                error(pending_auth[0], -32000, 'Authentication failed')
                pending_auth = None
        if pending_prompt and now >= pending_prompt[1]:
            reply('prompt', pending_prompt[0], {'stopReason': cfg.get('stop_reason', 'end_turn')})
            pending_prompt = None
        readable, _, _ = select.select([sys.stdin], [], [], 0.01)
        if not readable:
            continue
        chunk = os.read(sys.stdin.fileno(), 65536)
        if not chunk:
            return
        buffer += chunk
        while b'\n' in buffer:
            line, buffer = buffer.split(b'\n', 1)
            message = json.loads(line)
            with (here / 'calls.jsonl').open('a') as calls:
                calls.write(json.dumps({'message': message, 'home': home.name, 'argv': sys.argv[1:],
                                        'env_names': sorted(os.environ)}) + '\n')
            cfg = config()
            method = message.get('method')
            params = message.get('params') or {}
            if method is None:
                if client_wait and message.get('id') == 'client-request':
                    reply('initialize', client_wait)
                    client_wait = None
                continue
            hang = cfg.get('hang')
            if hang is True or isinstance(hang, list) and method in hang:
                continue
            if method == 'initialize':
                if cfg.get('malformed'):
                    print('invalid-json', flush=True)
                elif cfg.get('client_request'):
                    client_wait = message
                    emit({'jsonrpc': '2.0', 'id': 'client-request', 'method': 'fs/read_text_file', 'params': {}})
                else:
                    reply('initialize', message)
            elif method == 'authenticate':
                if params.get('methodId') == 'cached_token':
                    if signed_in():
                        reply('authenticate', message, auth_result())
                    else:
                        reply('cached_signed_out', message)
                else:
                    pending_auth = (message, time.monotonic() + cfg.get('device_delay', 0.05))
            elif method == '_x.ai/auth/get_url':
                result = copy.deepcopy(fixtures['device_url']['response']['result'])
                result.update({key: cfg[key] for key in ('mode', 'auth_url') if key in cfg})
                reply('device_url', message, result)
            elif method == '_x.ai/auth/cancel':
                cancelled = bool(pending_auth and
                                 params.get('request_seq') == pending_auth[0].get('params', {}).get('_meta', {}).get('request_seq'))
                reply('auth_cancel', message, {'cancelled': cancelled})
                if cancelled:
                    error(pending_auth[0], -32000, 'Authentication cancelled')
                    pending_auth = None
            elif method == '_x.ai/auth/info':
                result = copy.deepcopy(fixtures['auth_info_signed_out']['response']['result'])
                if signed_in():
                    result.update(email=cfg.get('email', 'worker@example.test'), methodId='grok.com')
                result.update(cfg.get('auth_info', {}))
                reply('auth_info_signed_out', message, result)
            elif method == '_x.ai/billing':
                result = copy.deepcopy(cfg.get('billing', fixtures['billing']['response']['result']))
                if 'billing' not in cfg:
                    for option, key in (('used', 'creditUsagePercent'), ('cap', 'onDemandCap')):
                        if option in cfg:
                            result['config'][key] = cfg[option]
                    for option, key in (('period', 'type'), ('reset', 'end')):
                        if option in cfg:
                            result['config']['currentPeriod'][key] = cfg[option]
                    result['subscription_tier'] = cfg.get('tier', 'SuperGrok')
                reply('billing', message, result)
            elif method in ('session/new', 'session/load'):
                if method == 'session/load' and cfg.get('load_missing', not signed_in()):
                    reply('load_missing', message)
                elif not signed_in():
                    reply('new_signed_out', message)
                else:
                    reply('new_signed_in' if method == 'session/new' else 'load_signed_in', message)
            elif method == 'session/prompt':
                if cfg.get('quota'):
                    reply('quota', message)
                else:
                    for notification in copy.deepcopy(fixtures['prompt']['notifications']):
                        notification['params']['sessionId'] = params['sessionId']
                        emit(notification)
                    pending_prompt = (message, time.monotonic() + cfg.get('prompt_delay', 0.05))
            elif method == '_x.ai/interject':
                notification = copy.deepcopy(fixtures['interject']['notifications'][0])
                notification['params'] = {key: params[key] for key in ('sessionId', 'text', 'interjectionId') if key in params}
                reply('interject', message)
                emit(notification)
            elif method == 'session/cancel':
                if pending_prompt and pending_prompt[0]['params'].get('sessionId') == params.get('sessionId'):
                    reply('prompt', pending_prompt[0], {'stopReason': 'cancelled'})
                    pending_prompt = None
            elif 'id' in message:
                reply('unknown_method', message)


if __name__ == '__main__':
    main()
