"""Journal-host agent whose prompt responses require an explicit test release."""

import json
import sys


SESSION = 'ses_1b3d19f6fffeIxa5icm1pIrpgt'


def send(event):
    print(json.dumps({'jsonrpc': '2.0', **event}), flush=True)


def main():
    mode = sys.argv[1]
    if mode == 'die-at-start':
        return 3
    pending = None
    for raw in sys.stdin:
        event = json.loads(raw)
        method = event.get('method')
        if method == 'initialize':
            send({'id': event['id'], 'result': {'protocolVersion': 1, 'agentInfo': {'version': 'test-1'}}})
        elif method == 'session/new':
            send({'id': event['id'], 'result': {'sessionId': SESSION}})
        elif method == 'session/prompt':
            pending = event['id']
            send({'method': 'session/update', 'params': {'sessionId': SESSION, 'update': {
                'sessionUpdate': 'agent_message_chunk', 'content': {'type': 'text', 'text': 'held answer'}}}})
            if mode == 'die-mid-prompt':
                return 1
        elif method == 'test/release' and pending is not None:
            send({'id': pending, 'result': {'stopReason': 'end_turn'}})
            pending = None
        elif method == 'session/cancel' and pending is not None:
            send({'id': pending, 'result': {'stopReason': 'cancelled'}})
            pending = None
    return 0


if __name__ == '__main__':
    sys.exit(main())
