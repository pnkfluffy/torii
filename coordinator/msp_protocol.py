"""Muse serve turns over the existing private, journaled host transport."""

import asyncio
from enum import Enum
import json
from pathlib import Path
import secrets
import threading
import time
import uuid

from .log import running_commit


STABLE_FINGERPRINT = 'sha256:5d3b43e5ccdb7dc4f5a32c380f5f2cde0f10e3059b93610afb2e73f73b1d914c'
METHODS = frozenset(('initialize', 'initialized', 'account/read', 'account/loginStart',
                     'account/loginCancel', 'usage/read', 'session/start', 'session/resume',
                     'turn/start', 'turn/steer', 'turn/interrupt', 'turn/cancel', 'task/stop', 'task/stopAll'))
FRAME_LIMIT = 10 * 1024 * 1024
_uuid_lock = threading.Lock()
_uuid_last = 0


def encode_frame(event):
    return (json.dumps(event) + '\n').encode()


def decode_frame(event):
    raw = event.encode() if isinstance(event, str) else event
    size = len(raw) if isinstance(raw, bytes) else len(json.dumps(event, ensure_ascii=False).encode())
    if size > FRAME_LIMIT:
        raise ValueError('MSP frame too large')
    if isinstance(event, (str, bytes)):
        event = json.loads(event)
    if not isinstance(event, dict) or event.get('jsonrpc') != '2.0':
        raise ValueError('Invalid MSP frame')
    if 'method' not in event and ('id' not in event or ('result' in event) == ('error' in event)):
        raise ValueError('Invalid MSP response')
    if 'method' in event and not isinstance(event['method'], str):
        raise ValueError('Invalid MSP method')
    if 'id' in event and event['id'] is not None and type(event['id']) not in (int, str):
        raise ValueError('Invalid MSP request id')
    return event


def initialized(result, account_home):
    schema = result.get('schema')
    home = result.get('museHome')
    if not isinstance(schema, dict) or schema.get('fingerprint') != STABLE_FINGERPRINT:
        return False
    grants = result.get('grantedCapabilities')
    if result.get('experimentalApi') is not True or not isinstance(grants, list) or 'sessionMcp' not in grants:
        return False
    if not isinstance(home, str) or not Path(home).is_absolute():
        return False
    try:
        Path(home).resolve().relative_to(Path(account_home))
    except (OSError, ValueError, RuntimeError):
        return False
    return True


def uuid7():
    """Return monotonic RFC 9562 UUIDs, including during a local clock rollback."""
    global _uuid_last
    with _uuid_lock:
        milliseconds = time.time_ns() // 1000000
        previous_ms = _uuid_last >> 80
        if milliseconds > previous_ms:
            random = secrets.randbits(74)
        else:
            milliseconds = previous_ms
            random = (((_uuid_last >> 64) & 0xfff) << 62) + (_uuid_last & ((1 << 62) - 1)) + 1
            if random >= 1 << 74:
                milliseconds += 1
                random = 0
        value = (milliseconds << 80) | (7 << 76) | ((random >> 62) << 64) | (2 << 62) | (random & ((1 << 62) - 1))
        _uuid_last = value
        return str(uuid.UUID(int=value))


def classify_error(error, model=None):
    """Classify typed turn errors without using message text to infer quota."""
    kind = error.get('kind', 'unknown')
    detail = ('%s: %s' % (kind, error.get('message', '')))[:500]
    if 'quota' in error:
        quota = error['quota']
        reset = quota.get('resetsAtMs') if isinstance(quota, dict) else None
        return {'code': 'quota_limited', 'detail': detail,
                'resets_at': reset / 1000 if type(reset) is int else None}
    named_model = model and (model in str(error.get('message', '')) or error.get('modelId') == model
                             or (error.get('details') or {}).get('modelId') == model)
    return {'code': ('login_required' if kind == 'authRequired' else
                     'model_unavailable' if kind in ('modelError', 'configError') and named_model else 'execution_failed'),
            'detail': detail, 'resets_at': None}


class MspState(Enum):
    STARTING = 'starting'
    RUNNING = 'running'
    STOPPING = 'stopping'
    TERMINAL = 'terminal'


class MspRequestError(Exception):
    def __init__(self, method, error):
        super().__init__('MSP request rejected: ' + method)
        self.method = method
        self.error = error


class MspProtocol:
    def __init__(self, process, control, session_id, observe_session, *, account_home, problem,
                 reattaching=False, text_limit=200000, cancel_grace=1):
        if text_limit < 1 or cancel_grace <= 0:
            raise ValueError('MSP limits must be positive')
        self.process = process
        self.control = control
        self.session_id = session_id
        self.observe_session = observe_session
        self.account_home = Path(account_home).resolve()
        self.problem = problem
        self.reattaching = reattaching
        self.text_limit = text_limit
        self.cancel_grace = cancel_grace
        self.state = MspState.STARTING
        self.text = ''
        self.spend = None
        self.failure = None
        self.detail = ''
        self.resets_at = None
        self.outcome = None
        self.goal = None
        self.goal_error = None
        self.background = []
        self.turn_id = None
        self.model = None
        self.closed = False
        self.requests = {}
        self._tasks = set()
        self._write_lock = asyncio.Lock()
        self._started = asyncio.Event()
        self._turn_ready = asyncio.Event()
        self._terminal = asyncio.Event()
        self._turn_frame = asyncio.Event()
        self._turn_frames = 0
        self._stop_requested = False
        self._stop_task = None
        self._close_task = None
        self._message_item = None
        self._usage = {}
        self._models = {}
        self._mismatches = set()
        self._answering = set()
        self._view_gap = False
        self.input_closed = asyncio.create_task(self.wait_input_closed())

    @property
    def terminal(self):
        return self.state == MspState.TERMINAL

    async def wait_terminal(self):
        await self._terminal.wait()

    async def wait_input_closed(self):
        try:
            await self.process.stdin.wait_closed()
        except ConnectionError:
            pass

    def track(self, coroutine):
        task = asyncio.create_task(coroutine)
        self._tasks.add(task)
        def finished(done):
            self._tasks.discard(done)
            if not done.cancelled() and done.exception() is not None:
                self.complete('failed', 'execution_failed')
        task.add_done_callback(finished)
        return task

    async def write(self, event, private=False):
        async with self._write_lock:
            if self.closed:
                raise BrokenPipeError()
            if private:
                await self.process.stdin.private(event)
            else:
                self.process.stdin.write(encode_frame(event))
                await self.process.stdin.drain()

    async def send(self, method, params=None, request_id=None, private=False):
        if method not in METHODS:
            raise ValueError('MSP method is not allowed: ' + method)
        event = {'jsonrpc': '2.0', 'method': method}
        if params is not None:
            event['params'] = params
        if method == 'initialized':
            if not any(json.loads(raw) == event for raw in self.process.written_lines()):
                await self.write(event)
            return None
        request_id = request_id or 'torii-' + uuid7()
        event['id'] = request_id
        old = self.process.past_request(request_id)
        if old is not None and old != event:
            raise ValueError('MSP host has a different prior request')
        response = self.process.past_response(request_id)
        if response is not None:
            return self.response(method, response)
        future = asyncio.get_running_loop().create_future()
        self.requests[request_id] = (method, future)
        try:
            if old is None:
                await self.write(event, private=private)
            if method == 'turn/start':
                self._started.set()
            return await asyncio.wait_for(asyncio.shield(future), max(.1, self.control.receipt_timeout))
        finally:
            self.requests.pop(request_id, None)
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                future.exception()

    def response(self, method, event):
        if 'error' in event:
            raise MspRequestError(method, event['error'])
        result = event.get('result')
        if not isinstance(result, dict):
            raise ValueError('MSP result must be an object')
        if method == 'turn/start':
            turn_id = result.get('turnId')
            if result.get('disposition') != 'started' or not isinstance(turn_id, str) or not turn_id:
                self.complete('failed', 'execution_failed')
            else:
                self.turn_id = turn_id
                self._turn_ready.set()
        return result

    def command(self, method, params, request_id):
        prior = self.process.past_request(request_id)
        if prior is not None:
            saved = prior.get('params', {})
            if prior.get('method') != method or {key: value for key, value in saved.items() if key != 'commandId'} != params:
                raise ValueError('MSP host has different prior command parameters')
            return saved
        return dict(params, commandId=uuid7())

    def initialized(self, result):
        return initialized(result, self.account_home)

    async def start(self, prompt, cwd, fresh, model, developer_instructions=None, thread_options=None):
        if not fresh and not self.reattaching:
            raise ValueError('MSP resume belongs to the Muse resume piece')
        effort = (thread_options or {}).get('effort', 'high')
        if effort not in ('low', 'medium', 'high', 'max'):
            raise ValueError('Unsupported MSP reasoning effort')
        self.model = model
        payload = '\n\n'.join(part for part in (developer_instructions, prompt) if part)
        task = self.track(self.run(payload, cwd, model, effort))
        started = asyncio.create_task(self._started.wait())
        try:
            await asyncio.wait((task, started), return_when=asyncio.FIRST_COMPLETED)
            if task.done() and not task.cancelled():
                task.result()
        finally:
            started.cancel()
            await asyncio.gather(started, return_exceptions=True)

    async def run(self, payload, cwd, model, effort):
        try:
            initialize = self.process.past_request('torii-initialize') if self.reattaching else None
            initialize_params = initialize['params'] if initialize is not None else {
                'clientInfo': {'name': 'torii', 'version': running_commit(Path(__file__).resolve().parents[1])},
                'capabilities': {'experimentalApi': True, 'userInputDialogs': False,
                                 'requestedCapabilities': ['sessionMcp']}}
            result = await self.send('initialize', initialize_params, 'torii-initialize')
            if not self.initialized(result):
                self.complete('failed', 'protocol_mismatch')
                return
            if self._stop_requested or self.terminal:
                return
            await self.send('initialized')
            account = await self.send('account/read', request_id='torii-account')
            lane = account.get('state')
            if lane != 'accountLogin':
                code = 'login_required' if lane == 'loggedOut' else 'billing_lane'
                if code == 'billing_lane':
                    self.problem('muse', 'billing-lane', 'Muse did not report accountLogin.')
                self.complete('failed', code)
                return
            if self._stop_requested or self.terminal:
                return
            opened = self.process.past_response('torii-open')
            if self.reattaching and opened is None:
                self.complete('failed', 'start_failed', 'Private session open has no recorded response.')
                return
            result = (self.response('session/start', opened) if opened is not None else
                      await self.send('session/start', {'commandId': uuid7(), 'workspaceRoot': str(cwd),
                          'modelId': model, 'approvalMode': 'allowAll'}, 'torii-open', private=True))
            session = result.get('session')
            session_id = session.get('sessionId') if isinstance(session, dict) else None
            if not isinstance(session_id, str) or not session_id:
                self.complete('failed', 'start_failed')
                return
            self.session_id = self.observe_session(session_id)
            if self._stop_requested or self.terminal:
                return
            self.state = MspState.RUNNING
            self.control.bind(self.steer)
            params = self.command('turn/start', {'sessionId': self.session_id,
                'input': [{'type': 'text', 'text': payload}], 'reasoningEffort': effort, 'ifBusy': 'queue'}, 'torii-turn')
            if self.reattaching and any(json.loads(raw).get('method') in ('turn/interrupt', 'turn/cancel')
                                        for raw in self.process.written_lines()):
                self._stop_requested = True
                self.state = MspState.STOPPING
                self.control.close()
            await self.send('turn/start', params, 'torii-turn')
            self._started.set()
            if self.reattaching:
                for raw in self.process.history_after(0):
                    self.handle(raw, replay=True)
                if self._stop_requested and not self.terminal:
                    self._stop_task = self.track(self.cancel_turn())
        except MspRequestError as error:
            data = error.error.get('data') or {}
            if error.method == 'initialize':
                code = 'protocol_mismatch'
            elif error.method == 'session/start':
                details = data.get('details') or {}
                named = details.get('modelId') == model or data.get('name') == model or model in str(error.error.get('message', ''))
                code = 'model_unavailable' if named else 'start_failed'
            else:
                code = 'execution_failed'
            self.complete('failed', code, str(error.error.get('message', ''))[:500])
        except (ValueError, TypeError, KeyError, OSError, RuntimeError, asyncio.TimeoutError):
            self.complete('failed', 'execution_failed')

    async def steer(self, message_id, text):
        return 'closed' if self.terminal or self.closed else 'unsupported'

    def complete(self, outcome, failure=None, detail=''):
        if self.terminal:
            return
        self.outcome = 'stopped' if self._stop_requested else outcome
        self.failure = 'owner_stopped' if self._stop_requested else failure
        self.detail = detail[:500]
        self.state = MspState.TERMINAL
        self._terminal.set()
        self._started.set()
        self.control.close()
        if not self.closed and self._close_task is None:
            self._close_task = self.track(self.close_input())

    async def close_input(self):
        try:
            if self.session_id is not None:
                params = self.command('task/stopAll', {'sessionId': self.session_id}, 'torii-stop-all')
                await self.send('task/stopAll', params, 'torii-stop-all')
        except (ConnectionError, OSError, asyncio.TimeoutError, MspRequestError, ValueError):
            pass
        finally:
            if not self.closed:
                self.closed = True
                self.process.stdin.close()

    def usage(self, turn_id, usage, model=None, record_model=False, view_cursor=None, params=None):
        if not isinstance(usage, dict):
            return
        counters = {target: value for target, key in (
            ('input_tokens', 'inputTokens'), ('output_tokens', 'outputTokens'),
            ('cached_tokens', 'cachedTokens'), ('reasoning_tokens', 'reasoningTokens'))
            for value in (usage.get(key),) if type(value) is int and value >= 0}
        if len(counters) != 4:
            return
        turn = self._usage.setdefault(turn_id, {'calls': {}, 'total': None, 'cumulative': {}})
        if record_model:
            for target, key in (('prompt_tokens', 'promptTokens'), ('total_tokens', 'totalTokens')):
                value = (params or {}).get(key)
                if type(value) is int and value >= 0:
                    counters[target] = value
            turn['calls'][view_cursor] = counters
            cumulative = (params or {}).get('cumulative') or {}
            for target, key in (('prompt_tokens', 'promptTokens'), ('total_tokens', 'totalTokens'),
                                ('output_tokens', 'outputTokens')):
                value = cumulative.get(key)
                if type(value) is int and value >= 0:
                    turn['cumulative'][target] = max(turn['cumulative'].get(target, 0), value)
            self._models[turn_id] = model
            if model is not None and model != self.model and turn_id not in self._mismatches:
                self._mismatches.add(turn_id)
                self.problem('muse', 'model-mismatch', 'Muse served a different model.')
        else:
            turn['total'] = counters
        self.spend = {key: sum(value.get(key, 0) for value in turn['calls'].values())
                      for key in ('input_tokens', 'output_tokens', 'cached_tokens', 'reasoning_tokens',
                                  'prompt_tokens', 'total_tokens')}
        if self._view_gap:
            for key, value in turn['cumulative'].items():
                self.spend[key] = max(self.spend[key], value)
        if turn['total'] is not None:
            self.spend.update(turn['total'])
        self.spend['models'] = dict(self._models)

    def handle(self, event, replay=False):
        try:
            return self.handle_frame(decode_frame(event), replay)
        except (ValueError, TypeError, KeyError, AttributeError):
            self.complete('failed', 'execution_failed')
            return None

    def handle_frame(self, event, replay):
        if 'method' in event and 'id' in event:
            answered = any(json.loads(raw).get('id') == event['id'] and 'method' not in json.loads(raw)
                           for raw in self.process.written_lines())
            if not answered and event['id'] not in self._answering:
                self._answering.add(event['id'])
                self.problem('muse', 'unexpected-request', str(event['method'])[:200])
                self.track(self.reject_request(event['id']))
            return None
        if 'id' in event:
            pending = self.requests.get(event['id'])
            if pending is not None and not pending[1].done():
                method, future = pending
                try:
                    future.set_result(self.response(method, event))
                except (MspRequestError, ValueError) as error:
                    future.set_exception(error)
            elif 'error' in event and not replay:
                self.complete('failed', 'execution_failed')
            return None
        method = event.get('method')
        params = event.get('params') or {}
        if params.get('sessionId') != self.session_id or self.session_id is None:
            return None
        if method == 'view/gap':
            self._view_gap = True
            return None
        if method == 'session/tokenUsage':
            cursor = params.get('viewCursor')
            if self.turn_id is None or params.get('turnId') != self.turn_id or not isinstance(cursor, str) or not cursor:
                return None
            self.usage(params['turnId'], params.get('usage'), params.get('modelId'),
                       record_model=True, view_cursor=cursor, params=params)
            return None
        if self.terminal:
            return None
        item = params.get('item') or {}
        frame_turn = params.get('turnId', item.get('turnId'))
        if method == 'item/delta' and params.get('itemId') == self._message_item:
            frame_turn = self.turn_id
        if frame_turn == self.turn_id and self.turn_id is not None:
            self._turn_frames += 1
            self._turn_frame.set()
        if method == 'turn/completed':
            if self.turn_id is None or params.get('turnId') != self.turn_id:
                return None
            self.usage(self.turn_id, params.get('usage'))
            terminal = params.get('terminal')
            if terminal == 'completed':
                self.complete('succeeded')
            elif terminal == 'failed':
                error = params.get('error')
                if not isinstance(error, dict):
                    raise ValueError('MSP failed turn has no error')
                value = classify_error(error, self.model)
                self.resets_at = value['resets_at']
                self.complete('failed', value['code'], value['detail'])
            elif terminal == 'cancelled':
                self.complete('failed', 'cancelled')
            else:
                self.complete('failed', 'execution_failed')
        elif method in ('item/started', 'item/completed'):
            item_id = item.get('itemId')
            if item.get('kind') != 'agentMessage' or item.get('turnId') != self.turn_id:
                return None
            if method == 'item/started':
                self._message_item = item_id
                self.text = ''
            elif isinstance(item.get('text'), str):
                self._message_item = None
                self.text = item['text'][-self.text_limit:]
                return {'type': 'item.completed', 'item': {'type': 'agent_message', 'text': self.text}}
        elif method == 'item/delta' and self._message_item is not None and params.get('itemId') == self._message_item:
            if params.get('field', 'text') == 'text' and isinstance(params.get('delta'), str):
                self.text = (self.text + params['delta'][-self.text_limit:])[-self.text_limit:]
                return {'type': 'item.completed', 'item': {'type': 'agent_message', 'text': self.text}}
        return None

    async def reject_request(self, request_id):
        try:
            await self.write({'jsonrpc': '2.0', 'id': request_id,
                              'error': {'code': -32601, 'message': 'Unexpected server request'}})
        finally:
            self._answering.discard(request_id)

    def cancel(self):
        if self.terminal or self.closed:
            return self._stop_task
        if self._stop_task is None:
            self._stop_requested = True
            self.state = MspState.STOPPING
            self.control.close()
            self._stop_task = self.track(self.cancel_turn())
        return self._stop_task

    async def interrupt_turn(self):
        await self._turn_ready.wait()
        if self.terminal:
            return
        params = self.command('turn/interrupt', {'sessionId': self.session_id, 'turnId': self.turn_id}, 'torii-interrupt')
        before = self._turn_frames
        try:
            await self.send('turn/interrupt', params, 'torii-interrupt')
        except MspRequestError as error:
            data = error.error.get('data') or {}
            if data.get('kind') == 'commandRejected' and data.get('reason') == 'missing_run':
                if self._turn_frames == before:
                    self._turn_frame.clear()
                    await self._turn_frame.wait()
                if not self.terminal:
                    params = self.command('turn/interrupt', {'sessionId': self.session_id, 'turnId': self.turn_id}, 'torii-interrupt-retry')
                    await self.send('turn/interrupt', params, 'torii-interrupt-retry')
        await self._terminal.wait()

    async def cancel_turn(self):
        try:
            await asyncio.wait_for(self.interrupt_turn(), self.cancel_grace)
            return
        except (asyncio.TimeoutError, MspRequestError, ConnectionError, OSError):
            pass
        if self.turn_id is not None and not self.terminal and not self.closed:
            try:
                params = self.command('turn/cancel', {'sessionId': self.session_id, 'turnId': self.turn_id}, 'torii-cancel')
                await asyncio.wait_for(self.send('turn/cancel', params, 'torii-cancel'), self.cancel_grace)
            except (asyncio.TimeoutError, MspRequestError, ConnectionError, OSError):
                pass

    def disconnect(self):
        """Detach local observers without writing to or stopping the host's child."""
        self.closed = True
        for task in self._tasks:
            task.cancel()
        for method, future in self.requests.values():
            if not future.done():
                future.cancel()
        self.control.close()
        self.input_closed.cancel()
        self.complete('failed', 'disconnected')
