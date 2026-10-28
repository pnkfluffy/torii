"""Offline Muse serve shapes with scripted replies and an optional terminal gate."""

import argparse
from copy import deepcopy
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading


FIXTURE = Path(__file__).with_name('fixtures') / 'muse-msp-1.4.9.jsonl'
FRAMES = [json.loads(line) for line in FIXTURE.read_text().splitlines()]
SESSION = next(frame['result']['session']['sessionId'] for frame in FRAMES if frame.get('id') == 'torii-open' and 'result' in frame)
TURN = next(frame['result']['turnId'] for frame in FRAMES if frame.get('id') == 'torii-turn' and 'result' in frame)
EXPERIMENTAL_FINGERPRINT = 'sha256:e8b3d24f3922af8e567838a9ebdf8a28b64e6e0ddb516689f1a760e46c2f7fa1'


def notification(method):
    return deepcopy(next(frame for frame in FRAMES if frame.get('method') == method and 'id' not in frame))


def result(request_id):
    return deepcopy(next(frame['result'] for frame in FRAMES if frame.get('id') == request_id and 'result' in frame))


class MuseServer:
    def __init__(self, home, scenario=None):
        self.home = Path(home)
        self.scenario = scenario or {}
        self.command = None
        self.interrupts = 0
        self.finished = False

    def finish_login(self):
        return [{'jsonrpc': '2.0', 'method': 'account/loginCompleted',
                 'params': {'outcome': self.scenario.get('outcome', 'granted')}}]

    def finish(self, terminal=None):
        if self.finished:
            return []
        self.finished = True
        completed = notification('turn/completed')
        completed['params']['terminal'] = terminal or self.scenario.get('terminal', 'completed')
        if 'error' in self.scenario:
            completed['params']['error'] = self.scenario['error']
        return [completed]

    def receive(self, event):
        method = event.get('method')
        def reply(value):
            return {'jsonrpc': '2.0', 'id': event['id'], 'result': value}
        if method == 'initialize':
            value = result('torii-initialize')
            value['museHome'] = self.scenario.get('muse_home', str(self.home / 'muse'))
            value['schema']['fingerprint'] = self.scenario.get('fingerprint', value['schema']['fingerprint'])
            value['experimentalApi'] = self.scenario.get('experimental_api', True)
            value['grantedCapabilities'] = self.scenario.get('grants', ['sessionMcp'])
            return [reply(value)]
        if method == 'account/read':
            if self.scenario.get('hold_account'):
                return []
            value = {'state': self.scenario.get('lane', 'accountLogin'), 'credentialRequired': True}
            if 'label' in self.scenario:
                value['label'] = self.scenario['label']
            if self.scenario.get('account_session_id'):
                value['session'] = {'sessionId': self.scenario['account_session_id']}
            return [reply(value)]
        if method == 'account/loginStart' and self.scenario.get('signin'):
            frames = [reply({'verificationUrl': self.scenario.get('url', 'https://auth.meta.com/oauth/device/test'),
                             'userCode': self.scenario.get('code', 'ABCD-EFGH')})]
            if self.scenario.get('stderr_code'):
                print(self.scenario.get('code', 'ABCD-EFGH'), file=sys.stderr, flush=True)
            if self.scenario.get('hold') or self.scenario.get('release'):
                return frames
            return self.finish_login() + frames if self.scenario.get('completion_first') else frames + self.finish_login()
        if method == 'account/loginCancel' and self.scenario.get('signin'):
            return [reply({'cancelled': self.scenario.get('cancelled', True)})]
        if method == 'usage/read' and self.scenario.get('signin'):
            return [reply({'usage': []})]
        if method == 'session/start':
            if 'open_error' in self.scenario:
                return [{'jsonrpc': '2.0', 'id': event['id'], 'error': self.scenario['open_error']}]
            value = result('torii-open')
            if self.scenario.get('legacy_session_id'):
                value['session']['id'] = value['session'].pop('sessionId')
            return [reply(value)]
        if method == 'turn/start':
            self.command = event['params']['commandId']
            value = result('torii-turn')
            value['commandId'] = self.command
            value['disposition'] = self.scenario.get('disposition', 'started')
            value['turnId'] = self.scenario.get('turn_id', TURN)
            frames = [reply(value)]
            if self.scenario.get('server_request'):
                frames.append(deepcopy(next(frame for frame in FRAMES if frame.get('method') == 'approval/request')))
            if self.scenario.get('hold'):
                return frames
            started = notification('turn/started')
            started['params']['commandId'] = self.command
            frames.append(started)
            frames.extend(notification(name) for name in ('item/started', 'item/delta', 'item/completed'))
            usage = notification('session/tokenUsage')
            usage['params']['modelId'] = self.scenario.get('served_model', 'muse-wonder-1.0')
            frames.append(usage)
            frames.extend(self.finish())
            return frames
        if method in ('turn/interrupt', 'turn/cancel'):
            if method == 'turn/interrupt' and self.scenario.get('drop_interrupt_ack'):
                return []
            if method == 'turn/interrupt':
                self.interrupts += 1
                if self.scenario.get('cancel_race') and self.interrupts == 1:
                    error = deepcopy(next(frame for frame in FRAMES if frame.get('id') == 'torii-interrupt' and 'error' in frame))
                    error['id'] = event['id']
                    started = notification('turn/started')
                    started['params']['commandId'] = self.command
                    return [error, started]
            value = {'commandId': event['params']['commandId'], 'status': 'accepted', 'turnId': TURN}
            frames = [reply(value)]
            ignore = self.scenario.get('ignore_cancel') or (method == 'turn/interrupt' and self.scenario.get('ignore_interrupt'))
            if not ignore:
                frames.extend(self.finish('cancelled'))
            return frames
        if method == 'task/stopAll':
            if self.scenario.get('drop_stop_all_ack'):
                return []
            return [reply({'commandId': event['params']['commandId'], 'status': 'accepted'})]
        if method is not None and 'id' in event:
            return [{'jsonrpc': '2.0', 'id': event['id'], 'error': {'code': -32601, 'message': 'Method not found'}}]
        return []


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--home', required=True)
    parser.add_argument('--calls', required=True)
    parser.add_argument('--scenario', required=True)
    args, extra = parser.parse_known_args()
    scenario = json.loads(Path(args.scenario).read_text())
    if scenario.get('signin'):
        Path(args.home, 'launch.json').write_text(json.dumps({'argv': extra, 'home': args.home, 'pid': os.getpid()}))
    if scenario.get('ignore_term'):
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    if scenario.get('outside_write'):
        try:
            Path(scenario['outside_write']).write_text('synthetic boundary probe')
        except PermissionError:
            pass
    server = MuseServer(args.home, scenario)
    stopping = threading.Event()
    lock = threading.Lock()
    threads = []
    def emit(frames):
        with lock:
            for frame in frames:
                print(json.dumps(frame), flush=True)
    def release():
        while not stopping.wait(.01):
            if Path(scenario['release']).exists():
                emit(server.finish_login() if scenario.get('signin') else server.finish())
                return
    try:
        for raw in sys.stdin:
            event = json.loads(raw)
            with Path(args.calls).open('a') as calls:
                calls.write(json.dumps(event) + '\n')
            if event.get('method') == scenario.get('exit_on'):
                return 0
            if event.get('method') == 'turn/start' and scenario.get('script'):
                subprocess.run([sys.executable, '-c', scenario['script']], check=True)
            if scenario.get('malformed_frame'):
                print('[' * 2000 + '0' + ']' * 2000, flush=True)
                continue
            emit(server.receive(event))
            if event.get('method') in ('turn/start', 'account/loginStart') and scenario.get('release'):
                thread = threading.Thread(target=release)
                threads.append(thread)
                thread.start()
    finally:
        if scenario.get('ignore_eof'):
            stopping.wait(30)
        stopping.set()
        for thread in threads:
            thread.join()
    return 0


if __name__ == '__main__':
    sys.exit(main())
