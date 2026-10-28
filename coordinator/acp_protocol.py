"""Provider-neutral ACP turns over the host's journaled NDJSON transport."""

import asyncio
from collections import deque
from dataclasses import dataclass
from enum import Enum
import json
from typing import Awaitable, Callable, Optional

from .native_protocol import RunControl, steer_wire


INITIALIZE = {'protocolVersion': 1,
              'clientCapabilities': {'fs': {'readTextFile': False, 'writeTextFile': False}, 'terminal': False},
              'clientInfo': {'name': 'torii', 'version': '1'}}


def tool_name(update):
    return update.get('title')


@dataclass(frozen=True)
class AcpNativeSteer:
    """Build a native steer request and recognize its notification receipt.

    params(session_id, wire_id, text) returns request parameters.
    received(event, session_id, wire_id) matches an agent notification.
    A successful request alone does not acknowledge delivery.
    """

    method: str
    params: Callable[[str, str, str], dict]
    received: Callable[[dict, str, str], bool]


@dataclass(frozen=True)
class AcpAdapter:
    """Provider slots shared by launch code and the ACP core.

    argv and environment build launch values. The core never calls them.
    check_version receives the initialize result and returns a bool.
    allow_version_change_on_reattach preserves the version of an already accepted session.
    valid_session_id checks both new and resumed ids before observation.
    permission receives request params and asynchronously returns the result.
    classify_error(method, error) returns a failure code, including session_missing.
    auth_step(protocol) optionally authenticates through protocol.request.
    before_prompt(session_id) optionally resets provider error capture before a new prompt write.
    It can inspect the result and call protocol.complete to reject an account gate.
    usage(result, last_update) maps prompt usage and the last usage_update.
    It can also request a provider usage refresh. Token fields are per prompt and
    summed for the run. Other fields, including mapped cumulative cost, use the
    latest value. The adapter maps session cost to the current run's estimate.
    queued_receipt opts into queued receipts and journaled delivery ids.
    The default keeps uncertain receipts for adapters that do not opt in.
    """

    argv: Callable[..., list]
    environment: Callable[..., dict]
    check_version: Callable[[dict], bool]
    valid_session_id: Callable[[str], bool]
    permission: Callable[[dict], Awaitable[dict]]
    classify_error: Callable[[str, dict], str]
    usage: Callable[[dict, Optional[dict]], Optional[dict]]
    auth_step: Optional[Callable[['AcpProtocol'], Awaitable[None]]] = None
    native_steer: Optional[AcpNativeSteer] = None
    queued_receipt: str = 'uncertain'
    allow_version_change_on_reattach: bool = False
    before_prompt: Optional[Callable[[str], None]] = None


class AcpState(Enum):
    STARTING = 'starting'
    LOADING = 'loading'
    PROMPTING = 'prompting'
    STOPPING = 'stopping'
    TERMINAL = 'terminal'


class AcpRequestError(Exception):
    def __init__(self, method, error, failure_code):
        super().__init__('ACP request rejected: ' + method)
        self.method = method
        self.error = error
        self.failure_code = failure_code


class AcpProtocol:
    def __init__(self, adapter, process, control: RunControl, session_id, observe_session,
                 reattaching=False, text_limit=200000, cancel_grace=1, mcp_servers=(), persistent=False,
                 on_prompt=None, separate_prompt=None):
        if text_limit is not None and text_limit < 1:
            raise ValueError('ACP text limit must be positive')
        self.adapter = adapter
        self.process = process
        self.control = control
        self.session_id = session_id
        self.observe_session = observe_session
        self.text_limit = text_limit
        self.cancel_grace = cancel_grace
        self.mcp_servers = list(mcp_servers)
        self.persistent = persistent
        self.on_prompt = on_prompt
        self.separate_prompt = separate_prompt
        self.text = ''
        self.spend = None
        self.failure = None
        self.outcome = None
        self.closed = False
        self.state = AcpState.STARTING
        self.requests = {}
        self.messages = {}
        self.queued = []
        self._queued_ready = asyncio.Event()
        self._prompt_written = asyncio.Event()
        self.prompt_sent = False
        self._prompt_messages = ()
        self._completed_prompts = set()
        self.write_lock = asyncio.Lock()
        self.steer_lock = asyncio.Lock()
        self.counter = process.max_request_id()
        self._replay = deque()
        if reattaching:
            for line in process.written_lines():
                event = json.loads(line)
                if isinstance(event, dict) and 'method' in event and type(event.get('id')) is int:
                    self._replay.append(event)
        self._starting = True
        self._new_id = next((event['id'] for event in self._replay if event['method'] == 'session/new'), None)
        self._load_id = next((event['id'] for event in self._replay if event['method'] == 'session/load'), None)
        self._loading = self._load_id is not None
        self._usage_update = None
        self._prompt_future = None
        self._started = asyncio.get_running_loop().create_future()
        self._message_open = False
        self._stop_requested = False
        self._stop_task = None
        self._tasks = set()
        self._answering = set()
        self.input_closed = asyncio.create_task(self.wait_input_closed())

    @property
    def terminal(self):
        return self.state == AcpState.TERMINAL

    async def wait_input_closed(self):
        try:
            await self.process.stdin.wait_closed()
        except ConnectionError:
            pass

    async def write(self, event):
        async with self.write_lock:
            if self.closed:
                raise BrokenPipeError()
            self.process.stdin.write((json.dumps({**event, 'jsonrpc': '2.0'}) + '\n').encode())
            await self.process.stdin.drain()

    async def request(self, method, params, prior=None, steers=()):
        if prior is None and self._starting and self._replay:
            prior = self._replay.popleft()
        if prior is None:
            self.counter = max(self.counter, self.process.max_request_id()) + 1
            request_id = self.counter
        else:
            request_id = prior['id']
        event = {'jsonrpc': '2.0', 'id': request_id, 'method': method, 'params': params}
        old = self.process.past_request(request_id)
        if (prior is not None and old != prior) or (old is not None and old != event):
            raise RuntimeError('Provider host has a different prior request')
        if method == 'session/prompt':
            self._starting = False
            self._prompt_messages = tuple(message_id for message_id, text in steers)
        if method == 'session/load':
            self._load_id = request_id
        if method == 'session/new':
            self._new_id = request_id
        if old is not None:
            if method == 'session/prompt':
                self.prompt_sent = True
                if not self._started.done():
                    self._started.set_result(None)
            response = self.process.past_response(request_id)
            if response is not None:
                return self.response(method, response)
        future = asyncio.get_running_loop().create_future()
        self.requests[request_id] = (method, future)
        if method == 'session/prompt':
            self._prompt_future = future
        try:
            async def exchange():
                if old is None:
                    if method == 'session/prompt':
                        self._message_open = False
                        if self.persistent:
                            self.text = ''
                        if self.on_prompt is not None:
                            self.on_prompt(request_id, self._prompt_messages)
                        if self.adapter.before_prompt is not None:
                            self.adapter.before_prompt(self.session_id)
                    await self.write(event)
                if method == 'session/prompt':
                    self.prompt_sent = True
                    self._prompt_written.set()
                    if not self._started.done():
                        self._started.set_result(None)
                    if self.control.on_outcome is not None:
                        for message_id, text in steers:
                            self.control.on_outcome(message_id, 'received')
                return await asyncio.shield(future)

            if method == 'session/prompt':
                return await exchange()
            timeout = max(10, self.control.receipt_timeout)
            if method in ('initialize', 'session/new', 'session/load'):
                timeout *= 10
            else:
                timeout = self.control.receipt_timeout
            return await asyncio.wait_for(exchange(), timeout)
        finally:
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                future.exception()
            self.requests.pop(request_id, None)
            if self._prompt_future is future:
                self._prompt_future = None
            self.finish_input()

    def response(self, method, event):
        if 'error' in event:
            error = event['error']
            raise AcpRequestError(method, error, self.adapter.classify_error(method, error))
        return event['result']

    async def start(self, prompt, cwd, fresh, model, developer_instructions=None, thread_options=None):
        """Return after the session exists, or the worker's first prompt is written."""
        if self.persistent and (prompt != '' or developer_instructions is not None):
            raise ValueError('Persistent ACP start requires an empty prompt and no developer instructions')
        task = self.track(self.run(prompt, cwd, fresh, developer_instructions))
        await asyncio.wait((task, self._started), return_when=asyncio.FIRST_COMPLETED)
        if task.done() and not task.cancelled():
            task.result()

    async def run(self, prompt, cwd, fresh, developer_instructions):
        try:
            replayed_initialize = any(event['method'] == 'initialize'
                and self.process.past_response(event['id']) is not None for event in self._replay)
            initialized = await self.request('initialize', INITIALIZE)
            if self.state in (AcpState.STOPPING, AcpState.TERMINAL):
                return
            accepted_session = any(event['method'] in ('session/new', 'session/load') for event in self._replay)
            check_version = not (replayed_initialize and accepted_session and self.adapter.allow_version_change_on_reattach)
            if initialized.get('protocolVersion') != 1 or (check_version and not self.adapter.check_version(initialized)):
                self.complete('failed', 'version_mismatch')
                return
            if self.adapter.auth_step is not None:
                await self.adapter.auth_step(self)
            if self.state in (AcpState.STOPPING, AcpState.TERMINAL):
                return
            params = {'cwd': str(cwd), 'mcpServers': self.mcp_servers}
            if fresh:
                response = await self.request('session/new', params)
                session_id = response.get('sessionId')
            else:
                session_id = self.session_id
                if not self.adapter.valid_session_id(session_id):
                    self.complete('failed', 'invalid_session_id')
                    return
                self.state = AcpState.LOADING
                self._loading = True
                await self.request('session/load', {'sessionId': session_id, **params})
                self._loading = False
            if self.state in (AcpState.STOPPING, AcpState.TERMINAL):
                return
            if not self.adapter.valid_session_id(session_id):
                self.complete('failed', 'invalid_session_id')
                return
            self.session_id = self.observe_session(session_id)
            self.state = AcpState.PROMPTING
            self.control.bind(self.steer)
            text = '\n\n'.join(part for part in (developer_instructions, prompt) if part)
            params = {'sessionId': self.session_id, 'prompt': [{'type': 'text', 'text': text}]}
            if self.persistent:
                self._starting = False
                self._started.set_result(None)
                params = None
            prior = None
            steers = ()
            while not self.terminal:
                if params is None:
                    steers = ()
                    prior = next((event for event in self._replay if event['method'] == 'session/prompt'), None)
                    if prior is not None:
                        self._replay.remove(prior)
                        params = prior['params']
                        if params.get('sessionId') != self.session_id:
                            raise RuntimeError('Provider host has a different prior session')
                    elif self.queued:
                        count = len(self.queued)
                        if self.separate_prompt is not None:
                            count = 1
                            if not self.separate_prompt(self.queued[0][0]):
                                while count < len(self.queued) and not self.separate_prompt(self.queued[count][0]):
                                    count += 1
                        steers = tuple(self.queued[:count])
                        del self.queued[:count]
                        params = {'sessionId': self.session_id,
                                  'prompt': [{'type': 'text', 'text': '\n\n'.join(text for message_id, text in steers)}]}
                        if self.adapter.queued_receipt == 'queued':
                            params['_meta'] = {'toriiSteers': [message_id for message_id, text in steers]}
                    elif self.persistent:
                        self._queued_ready.clear()
                        await self._queued_ready.wait()
                        if self.state != AcpState.PROMPTING:
                            return
                        continue
                    else:
                        self.complete('succeeded')
                        return
                result = await self.request('session/prompt', params, prior=prior, steers=steers)
                if not self.persistent:
                    self.prompt_usage(result)
                reason = result.get('stopReason')
                if self._stop_requested:
                    self.complete('stopped', 'owner_stopped')
                    return
                if reason != 'end_turn':
                    code = reason if reason in ('cancelled', 'refusal', 'max_tokens', 'max_turn_requests') else 'invalid_stop_reason'
                    self.complete('failed', code)
                    return
                params = None
        except AcpRequestError as error:
            self.complete('failed', error.failure_code)
        except asyncio.TimeoutError:
            self.complete('failed', 'receipt_timeout')
        except (ConnectionError, FileNotFoundError):
            if self.state == AcpState.STOPPING:
                self.complete('stopped', 'owner_stopped')
            else:
                self.complete('failed', 'disconnected')

    async def steer(self, message_id, text):
        async with self.steer_lock:
            if self.state != AcpState.PROMPTING or self.closed:
                return 'closed'
            if self.persistent:
                idle = self._prompt_future is None and not self.queued and not self._replay
                if idle:
                    self._prompt_written.clear()
                self.queued.append((message_id, text))
                self._queued_ready.set()
                if idle:
                    await self._prompt_written.wait()
                    if self.terminal or self.closed:
                        return 'closed'
                return 'received'
            native = self.adapter.native_steer
            if native is None:
                self.queued.append((message_id, text))
                return self.adapter.queued_receipt
            wire = steer_wire(message_id)
            future = asyncio.get_running_loop().create_future()
            self.messages[wire] = future
            try:
                params = native.params(self.session_id, wire, text)
                prior = next((event for event in self._replay
                              if event['method'] == native.method and event['params'] == params), None)
                await self.request(native.method, params, prior=prior)
                if prior is not None and not future.done():
                    for raw in self.process.history_after(0):
                        if native.received(json.loads(raw), self.session_id, wire):
                            future.set_result('received')
                            break
                return await asyncio.wait_for(asyncio.shield(future), self.control.receipt_timeout)
            except AcpRequestError:
                return 'closed' if self.terminal else 'unsupported'
            except (asyncio.TimeoutError, ConnectionError, FileNotFoundError):
                return 'closed' if self.terminal else 'uncertain'
            finally:
                if not future.done():
                    future.cancel()
                self.messages.pop(wire, None)
                self.finish_input()

    def prompt_usage(self, result):
        usage = self.adapter.usage(result, self._usage_update)
        if usage is not None:
            spend = dict(self.spend or {})
            for key, value in usage.items():
                spend[key] = spend.get(key, 0) + value if key.endswith('_tokens') else value
            self.spend = spend
        return usage

    def handle(self, event, replay=False):
        """Return bounded text or activity events without retaining notifications."""
        if 'method' not in event and 'id' in event:
            if event['id'] == self._load_id:
                self._loading = False
            if event['id'] == self._new_id and 'result' in event:
                session_id = event['result'].get('sessionId')
                if self.adapter.valid_session_id(session_id):
                    self.session_id = session_id
            pending = self.requests.get(event['id'])
            prior = self.process.past_request(event['id']) if replay or self.persistent else None
            if (pending is not None and pending[0] == 'session/prompt') or (prior and prior['method'] == 'session/prompt'):
                self._message_open = False
            if pending is not None and not pending[1].done():
                method, future = pending
                try:
                    future.set_result(self.response(method, event))
                except AcpRequestError as error:
                    future.set_exception(error)
            if (self.persistent and prior and prior['method'] == 'session/prompt'
                    and event['id'] not in self._completed_prompts and not self.terminal
                    and not self._stop_requested and (event.get('result') or {}).get('stopReason') == 'end_turn'):
                self._completed_prompts.add(event['id'])
                usage = self.prompt_usage(event['result'])
                self._usage_update = None
                messages = prior['params'].get('_meta', {}).get('toriiSteers', self._prompt_messages)
                text, self.text = self.text, ''
                return {'type': 'turn.completed', 'messages': list(messages), 'usage': usage,
                        'text': text, 'turn_id': event['id']}
            return None
        if 'method' in event and 'id' in event:
            answered = replay and any('method' not in old and old.get('id') == event['id']
                                      for old in (json.loads(line) for line in self.process.written_lines()))
            if not answered and event['id'] not in self._answering:
                self._answering.add(event['id'])
                self.track(self.answer_request(event), 'permission_failed')
            return None
        params = event.get('params') or {}
        if self._loading or self.terminal or params.get('sessionId') != self.session_id:
            return None
        native = self.adapter.native_steer
        if native is not None:
            for wire, future in self.messages.items():
                if not future.done() and native.received(event, self.session_id, wire):
                    future.set_result('received')
        if event.get('method') != 'session/update':
            return None
        update = params.get('update') or {}
        kind = update.get('sessionUpdate')
        if kind in ('tool_call', 'tool_call_update', 'agent_thought_chunk', 'user_message_chunk'):
            self._message_open = False
        if kind == 'agent_message_chunk':
            content = update.get('content') or {}
            if content.get('type') == 'text' and isinstance(content.get('text'), str):
                if not self._message_open:
                    self.text = ''
                self._message_open = True
                self.text += content['text']
                if self.text_limit is not None:
                    self.text = self.text[-self.text_limit:]
                return {'type': 'item.completed', 'item': {'type': 'agent_message', 'text': self.text}}
        elif kind in ('tool_call', 'tool_call_update'):
            activity = {'type': 'activity'}
            name = tool_name(update)
            if isinstance(name, str) and name:
                activity['name'] = name
            return activity
        elif kind == 'usage_update':
            self._usage_update = update
        return None

    async def answer_request(self, event):
        try:
            if event['method'] == 'session/request_permission':
                result = await self.adapter.permission(event.get('params') or {})
                await self.write({'id': event['id'], 'result': result})
            else:
                await self.write({'id': event['id'], 'error': {'code': -32601, 'message': 'Method not found'}})
        finally:
            self._answering.discard(event['id'])

    def track(self, coroutine, failure='protocol_error'):
        task = asyncio.create_task(coroutine)
        self._tasks.add(task)
        task.add_done_callback(lambda done: self.task_finished(done, failure))
        return task

    def task_finished(self, task, failure):
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            self.complete('failed', failure)
        self.finish_input()

    def complete(self, outcome, failure=None):
        if self.terminal:
            return
        self.outcome = 'stopped' if self._stop_requested else outcome
        self.failure = 'owner_stopped' if self._stop_requested else failure
        self.state = AcpState.TERMINAL
        self._queued_ready.set()
        self._prompt_written.set()
        if self.adapter.queued_receipt == 'queued' and self.control.on_outcome is not None:
            for message_id, text in self.queued:
                self.control.on_outcome(message_id, 'uncertain')
        self.queued.clear()
        self.control.close()
        for future in self.messages.values():
            if not future.done():
                future.set_result('closed')
        for method, future in self.requests.values():
            if not future.done():
                future.set_exception(BrokenPipeError())
        self.finish_input()

    def finish_input(self):
        if self.terminal and not self.queued and not self.requests and not self.messages and not self._tasks:
            if not self.closed:
                self.closed = True
                self.process.stdin.close()

    def disconnect(self):
        """End this connection without sending input or stopping the host's child."""
        self.closed = True
        self.control.close()
        for task in self._tasks:
            task.cancel()
        for future in self.messages.values():
            if not future.done():
                future.set_result('uncertain')
        self.complete('failed', 'disconnected')
        for method, future in self.requests.values():
            if not future.done():
                future.set_exception(BrokenPipeError())

    def cancel(self):
        """Request an owner stop and return its bounded grace task."""
        if self.terminal or self.closed:
            return self._stop_task
        if self._stop_task is None:
            self._stop_requested = True
            self.state = AcpState.STOPPING
            self.control.close()
            if self.adapter.queued_receipt != 'queued':
                self.queued.clear()
            self._stop_task = self.track(self._cancel())
        return self._stop_task

    async def _cancel(self):
        try:
            if self.session_id is not None:
                await asyncio.wait_for(self.write({'method': 'session/cancel',
                                                  'params': {'sessionId': self.session_id}}), self.cancel_grace)
                if self._prompt_future is not None:
                    await asyncio.wait_for(asyncio.shield(self._prompt_future), self.cancel_grace)
        except (asyncio.TimeoutError, AcpRequestError, ConnectionError, FileNotFoundError):
            pass
        finally:
            self.complete('stopped', 'owner_stopped')
            for task in self._tasks:
                if task is not asyncio.current_task():
                    task.cancel()
            self.finish_input()
