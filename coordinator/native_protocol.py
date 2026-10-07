"""Live native input with provider receipts, scoped to one owned process."""

import asyncio
import json
import uuid


class SecretDeliveryRefused(ValueError):
    """A values-free explanation of why the native process cannot receive job secrets safely."""


def steer_wire(message_id):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, 'torii-steer:' + message_id))


class RunControl:
    def __init__(self, receipt_timeout=30, on_outcome=None):
        self.receipt_timeout = receipt_timeout
        self.on_outcome = on_outcome
        self._ready = asyncio.Event()
        self._sender = None
        self._goal_sender = None
        self._closed = False
        self._receipts = {}

    def begin_attempt(self):
        self._ready.clear()
        self._sender = None
        self._goal_sender = None
        self._closed = False

    def bind(self, sender, goal_sender=None):
        self._closed = False
        self._sender = sender
        self._goal_sender = goal_sender
        self._ready.set()

    def close(self):
        self._closed = True
        self._sender = None
        self._goal_sender = None
        self._ready.set()

    async def steer(self, message_id, prompt):
        return await self._deliver(message_id, prompt, goal=False)

    async def goal(self, message_id, condition):
        return await self._deliver(message_id, condition, goal=True)

    async def _deliver(self, message_id, prompt, goal):
        if message_id in self._receipts:
            return await asyncio.shield(self._receipts[message_id])
        if self._closed:
            return 'closed'
        await self._ready.wait()
        if self._closed:
            return 'closed'
        sender = self._goal_sender if goal else self._sender
        if sender is None:
            return 'unsupported'
        if message_id in self._receipts:
            return await asyncio.shield(self._receipts[message_id])
        future = asyncio.create_task(sender(str(message_id), prompt))
        self._receipts[message_id] = future

        def settled(done):
            if not done.cancelled() and done.exception() is None and done.result() in ('closed', 'unsupported'):
                self._receipts.pop(message_id, None)

        future.add_done_callback(settled)
        return await asyncio.shield(future)


class NativeProtocol:
    def __init__(self, provider, process, control, session_id, observe_session, persistent=False,
                 on_control_request=None, on_handoff=None, initial_goal=None, reattaching=False, secret_names=()):
        self.provider = provider
        self.process = process
        self.control = control
        self.session_id = session_id
        self.observe_session = observe_session
        self.persistent = persistent
        self.turn_id = None
        self._open_turns = {}
        self._completed_turns = set()
        self.thread = None
        self.initialized = False
        self.steer_lock = asyncio.Lock()
        self.requests = {}
        self.messages = {}
        self.counter = 0
        self.terminal = False
        self.closed = False
        self.background = []
        self.background_completion = False
        self._tasks = set()
        self.pending = set()
        self.unstarted = set()
        self.wires = {}
        self.write_lock = asyncio.Lock()
        self.input_closed = asyncio.create_task(self.wait_input_closed())
        self.on_handoff = on_handoff
        self.on_control_request = on_control_request
        self.failure = None
        self.initial_goal = initial_goal
        self.goal = None
        self.reattaching = reattaching
        self.start_finished = asyncio.Event()
        self.goal_lock = asyncio.Lock()
        self.goal_error = None
        self.secret_names = secret_names

    async def wait_input_closed(self):
        try:
            await self.process.stdin.wait_closed()
        except (BrokenPipeError, ConnectionResetError):
            pass

    async def write(self, event):
        async with self.write_lock:
            if self.closed:
                raise BrokenPipeError()
            self.process.stdin.write((json.dumps(event) + '\n').encode())
            await self.process.stdin.drain()
            if self.persistent and event.get('type') == 'user' and self.on_handoff:
                self.on_handoff(event['uuid'])

    async def exchange(self, event, future):
        await self.write(event)
        return await asyncio.shield(future)

    async def request(self, method, params):
        self.counter += 1
        request_id = self.counter
        event = {'id': request_id, 'method': method, 'params': params}
        old = self.process.past_request(request_id)
        if old is not None:
            if old != event:
                raise RuntimeError('Provider host has a different prior request')
            response = self.process.past_response(request_id)
            if response is not None:
                if 'error' in response:
                    raise RequestRejected()
                return response['result']
        future = asyncio.get_running_loop().create_future()
        self.requests[request_id] = future
        try:
            timeout = self.control.receipt_timeout if method == 'turn/steer' else max(10, self.control.receipt_timeout)
            if method in ('initialize', 'thread/start', 'thread/resume'):
                timeout *= 10
            if old is not None:
                return await asyncio.wait_for(asyncio.shield(future), timeout)
            return await asyncio.wait_for(self.exchange(event, future), timeout)
        finally:
            self.settle_future(future)
            self.requests.pop(request_id, None)
            self.finish_input()

    def user(self, message_id, prompt):
        return {'type': 'user', 'uuid': message_id, 'session_id': self.session_id,
                'parent_tool_use_id': None, 'message': {'role': 'user', 'content': prompt}}

    async def start(self, prompt, cwd, fresh, model, developer_instructions=None, thread_options=None):
        if self.provider == 'claude':
            if not self.process.written_lines():
                await asyncio.wait_for(self.write(self.user(str(uuid.uuid4()), prompt)),
                                       max(10, self.control.receipt_timeout))
            return
        if not self.initialized:
            await self.request('initialize', {'clientInfo': {'name': 'torii', 'version': '1'}})
            initialized = {'method': 'initialized', 'params': {}}
            if not any(json.loads(line) == initialized for line in self.process.written_lines()):
                await self.write(initialized)
            self.initialized = True
        if self.secret_names:
            response = await self.request('config/read', {'cwd': str(cwd), 'includeLayers': False})
            self.check_secret_config(response['config'])
        params = {'cwd': str(cwd), 'approvalPolicy': 'never', 'sandbox': 'danger-full-access'}
        if model:
            params['model'] = model
        if developer_instructions is not None:
            params['developerInstructions'] = developer_instructions
        params.update(thread_options or {})
        if not fresh:
            params['threadId'] = self.session_id
        response = await self.request('thread/start' if fresh else 'thread/resume', params)
        self.thread = response['thread']
        self.session_id = self.observe_session(self.thread['id'])
        if self.secret_names:
            response = await self.request('config/read', {'cwd': str(cwd), 'includeLayers': False})
            self.check_secret_config(response['config'])
        if self.reattaching and self.terminal:
            self.start_finished.set()
            return
        if self.persistent:
            self.counter = max(self.counter, self.process.max_request_id())
            self.control.bind(self.steer)
            return
        if self.initial_goal:
            response = await self.request('thread/goal/set', {'threadId': self.session_id,
                'objective': self.initial_goal, 'status': 'paused'})
            self.goal = response['goal']
        response = await self.request('turn/start', {'threadId': self.session_id,
            'input': [{'type': 'text', 'text': prompt}]})
        turn_id = response['turn']['id']
        if not self.reattaching:
            if self.turn_id is not None and self.turn_id != turn_id and self.turn_id not in self._completed_turns:
                raise RuntimeError('Provider changed the active turn')
            self.turn_id = None if turn_id in self._completed_turns else turn_id
        if self.initial_goal:
            response = await self.request('thread/goal/set', {'threadId': self.session_id, 'status': 'active'})
            self.goal = response['goal']
        self.counter = max(self.counter, self.process.max_request_id())
        if self.reattaching and (self.goal is not None or self.initial_goal):
            await self.get_goal()
        self.start_finished.set()
        if self.reattaching and self.turn_id is None and not self.goal_active():
            self.terminal = True
            self.control.close()
            self.finish_input()
        if not self.terminal and not self.closed:
            self.control.bind(self.steer, self.set_goal)

    def check_secret_config(self, config):
        policy = config.get('shell_environment_policy') or {}
        replaced = sorted(set(self.secret_names) & set(policy.get('set') or {}))
        if replaced:
            raise SecretDeliveryRefused('Codex shell configuration replaces declared secrets: ' + ', '.join(replaced))
        features = config.get('features') or {}
        multi_agent_v2 = features.get('multi_agent_v2')
        multi_agent_v2_disabled = (multi_agent_v2 is False or
                                  isinstance(multi_agent_v2, dict) and multi_agent_v2.get('enabled') is False)
        safe = (features.get('shell_snapshot') is False and features.get('shell_snapshot_v2') is False
                and features.get('multi_agent') is False and multi_agent_v2_disabled
                and features.get('memories') is False
                and (config.get('memories') or {}).get('dedicated_tools') is False
                and (config.get('memories') or {}).get('generate_memories') is False
                and ((config.get('otel') or {}).get('tool_result') or {}).get('max_bytes') == 0
                and (config.get('otel') or {}).get('log_agent_responses') is False
                and (config.get('otel') or {}).get('log_guardian_assessments') is False
                and policy.get('inherit') == 'all' and policy.get('ignore_default_excludes') is True
                and not policy.get('exclude') and not policy.get('include_only') and not policy.get('filters')
                and not policy.get('experimental_use_profile'))
        if not safe:
            raise SecretDeliveryRefused('Codex configuration prevents safe secret delivery; check its launch controls and requirements.')

    async def get_goal(self):
        response = await self.request('thread/goal/get', {'threadId': self.session_id})
        self.goal = response.get('goal')
        return self.goal

    async def set_goal(self, message_id, condition):
        if self.terminal or self.closed:
            return 'closed'
        try:
            async with self.goal_lock:
                if condition == 'clear':
                    await self.request('thread/goal/clear', {'threadId': self.session_id})
                    goal = await self.get_goal()
                    receipt = 'received' if goal is None else 'uncertain'
                else:
                    response = await self.request('thread/goal/set', {'threadId': self.session_id,
                        'objective': condition, 'status': 'active'})
                    self.goal = response['goal']
                    receipt = 'received' if self.goal.get('objective') == condition else 'uncertain'
                if self.turn_id is None and not self.goal_active():
                    self.terminal = True
                    self.control.close()
                    self.finish_input()
                return receipt
        except RequestRejected:
            return 'unsupported'
        except (asyncio.TimeoutError, BrokenPipeError, ConnectionResetError, FileNotFoundError):
            return 'uncertain'

    def goal_active(self):
        return self.goal is not None and self.goal.get('status') == 'active'

    async def finish_goal_turn(self):
        try:
            await self.start_finished.wait()
            async with self.goal_lock:
                await self.get_goal()
                if self.turn_id is None and not self.goal_active():
                    self.terminal = True
                    self.control.close()
                    self.finish_input()
        except (RequestRejected, asyncio.TimeoutError, BrokenPipeError, ConnectionResetError, FileNotFoundError):
            self.goal_error = 'Codex did not acknowledge the native goal state.'
            self.terminal = True
            self.control.close()
            self.finish_input()

    async def steer(self, message_id, prompt):
        if self.provider == 'codex' and self.persistent:
            async with self.steer_lock:
                return await self._steer(message_id, prompt)
        return await self._steer(message_id, prompt)

    async def _steer(self, message_id, prompt):
        if self.terminal or self.closed:
            return 'closed'
        try:
            if self.provider == 'codex':
                if self.persistent:
                    return await self._start_turn(message_id, prompt)
                expected = self.turn_id
                response = await self.request('turn/steer', {'threadId': self.session_id,
                    'expectedTurnId': expected, 'clientUserMessageId': message_id,
                    'input': [{'type': 'text', 'text': prompt}]})
                return 'received' if response.get('turnId') == expected else 'uncertain'
            if self.persistent:
                await asyncio.wait_for(self.write(self.user(message_id, prompt)), self.control.receipt_timeout)
                return 'received'
            wire_id = steer_wire(message_id)
            self.wires[wire_id] = message_id
            if self.process.past_user(wire_id):
                return 'received' if self.process.past_echo(wire_id) else 'uncertain'
            future = asyncio.get_running_loop().create_future()
            self.messages[wire_id] = future
            try:
                return await asyncio.wait_for(self.exchange(self.user(wire_id, prompt), future),
                                              self.control.receipt_timeout)
            finally:
                self.settle_future(future)
                self.messages.pop(wire_id, None)
                self.finish_input()
        except RequestRejected:
            return 'closed' if self.terminal else 'unsupported'
        except (asyncio.TimeoutError, BrokenPipeError, ConnectionResetError, FileNotFoundError):
            return 'uncertain'

    async def _start_turn(self, message_id, prompt):
        response = await self.request('turn/start', {'threadId': self.session_id,
            'clientUserMessageId': message_id, 'input': [{'type': 'text', 'text': prompt}]})
        turn_id = (response.get('turn') or {}).get('id')
        if not turn_id:
            return 'uncertain'
        self._open_turn(turn_id)
        return 'received'

    def _open_turn(self, turn_id):
        if turn_id and turn_id not in self._completed_turns:
            self._open_turns.setdefault(turn_id, None)
        self.turn_id = next(iter(self._open_turns), None)

    async def _reject_host_request(self, request_id):
        try:
            await self.write({'id': request_id, 'error': {
                'code': -32601, 'message': 'Torii does not support this host interaction'}})
        except (BrokenPipeError, ConnectionResetError, FileNotFoundError):
            pass

    @staticmethod
    def settle_future(future):
        if not future.done():
            future.cancel()
        elif not future.cancelled():
            future.exception()

    def finish_input(self):
        if self.terminal and not self.requests and not self.messages:
            self.closed = True
            self.process.stdin.close()

    def outcome(self, wire_id, state):
        message_id = self.wires.pop(wire_id, None)
        if message_id is not None and self.control.on_outcome is not None:
            self.control.on_outcome(message_id, state)

    def started(self, wire_id):
        self.unstarted.discard(wire_id)
        future = self.messages.get(wire_id)
        if future is not None and not future.done():
            future.set_result('received')
        self.outcome(wire_id, 'received')

    def handle(self, event, replay=False):
        """Handle a provider event, without repeating work from replayed history."""
        if (event.get('type') == 'control_request' and self.on_control_request is not None
                and self.on_control_request(self, event, replay)):
            return None
        if self.provider == 'claude':
            if (self.on_control_request is None and event.get('type') == 'control_request'
                    and (event.get('request') or {}).get('subtype') == 'oauth_token_refresh'):
                return None
            sid = event.get('session_id')
            hook = event.get('type') == 'system' and str(event.get('subtype', '')).startswith('hook_')
            if sid and not hook:
                self.session_id = self.observe_session(sid)
                if not self.terminal:
                    self.control.bind(self.steer)
            if (event.get('type') == 'user' and event.get('isReplay') is True
                    and event.get('parent_tool_use_id') is None and sid == self.session_id):
                future = self.messages.get(event.get('uuid'))
                if future is not None and not future.done() and self.terminal:
                    self.terminal = False
                    self.control.bind(self.steer)
                self.started(event.get('uuid'))
            if event.get('type') == 'command_lifecycle':
                wire_id = event.get('command_uuid')
                if event.get('state') in ('queued', 'started'):
                    self.pending.add(wire_id)
                else:
                    self.pending.discard(wire_id)
                if event.get('state') == 'queued' and wire_id in self.wires:
                    self.unstarted.add(wire_id)
                    future = self.messages.get(wire_id)
                    if future is not None and not future.done():
                        future.set_result('queued')
                elif event.get('state') == 'started':
                    self.started(wire_id)
                elif event.get('state') == 'cancelled' and wire_id in self.unstarted:
                    self.unstarted.discard(wire_id)
                    self.outcome(wire_id, 'uncertain')
            if event.get('type') == 'system' and event.get('subtype') == 'background_tasks_changed':
                tasks = list(event.get('tasks') or [])
                self.background_completion = bool(self.background) and not tasks
                self.background = tasks
            if event.get('type') == 'result':
                failed = event.get('is_error') or event.get('subtype') != 'success'
                if not self.persistent and (failed or not self.background and not self.unstarted):
                    self.terminal = True
                    self.control.close()
                    self.finish_input()
            return event
        if 'id' in event and ('result' in event or 'error' in event):
            result = event.get('result')
            if isinstance(result, dict) and isinstance(result.get('thread'), dict):
                self.thread = result['thread']
            if self.persistent and isinstance(result, dict) and isinstance(result.get('turn'), dict):
                self._open_turn(result['turn'].get('id'))
            future = self.requests.get(event['id'])
            if future is not None and not future.done():
                if 'error' in event:
                    future.set_exception(RequestRejected())
                else:
                    thread = event['result'].get('thread') if isinstance(event['result'], dict) else None
                    if self.session_id is None and isinstance(thread, dict) and thread.get('id'):
                        self.session_id = self.observe_session(thread['id'])
                    future.set_result(event['result'])
            return None
        if 'id' in event and 'method' in event:
            if self.persistent:
                if not replay:
                    self.track(self._reject_host_request(event['id']))
                return {'type': 'host.request', 'method': event['method']}
            raise RuntimeError('Provider requested unsupported host interaction')
        method = event.get('method')
        params = event.get('params') or {}
        if method == 'thread/started':
            if self.session_id is None:
                self.session_id = self.observe_session(params['thread']['id'])
            return None
        if params.get('threadId') and self.session_id is None:
            self.session_id = self.observe_session(params['threadId'])
        if params.get('threadId') and params['threadId'] != self.session_id:
            return None
        if method == 'turn/started':
            turn_id = params['turn']['id']
            if self.persistent:
                self._open_turn(turn_id)
            else:
                if self.turn_id is not None and self.turn_id != turn_id and self.turn_id not in self._completed_turns:
                    raise RuntimeError('Provider changed the active turn')
                self.turn_id = turn_id
        elif method == 'item/started' and self.persistent:
            if params.get('turnId') not in self._open_turns:
                return None
            item = params.get('item') or {}
            kind = item.get('type')
            name = ('mcp__%s__%s' % (item.get('server', 'torii'), item.get('tool', ''))
                    if kind == 'mcpToolCall' else kind)
            if kind in ('mcpToolCall', 'commandExecution', 'fileChange', 'webSearch', 'collabAgentToolCall'):
                return {'type': 'assistant', 'turn_id': params['turnId'],
                        'message': {'content': [{'type': 'tool_use', 'name': name}]}}
        elif method == 'item/completed':
            if self.persistent:
                if params.get('turnId') not in self._open_turns:
                    return None
            elif params.get('turnId') != self.turn_id:
                raise RuntimeError('Provider changed the active turn')
            item = params.get('item') or {}
            scope = {'turn_id': params['turnId']} if self.persistent else {}
            if self.persistent and item.get('type') == 'userMessage' and item.get('clientUserMessageId'):
                return {'type': 'user.received', **scope, 'message_id': item['clientUserMessageId']}
            if item.get('type') == 'agentMessage':
                return {'type': 'item.completed', **scope,
                        'item': {'type': 'agent_message', 'text': item.get('text', '')}}
        elif method == 'turn/completed':
            turn = params['turn']
            if self.persistent:
                if turn['id'] not in self._open_turns:
                    return None
                self._completed_turns.add(turn['id'])
                self._open_turns.pop(turn['id'], None)
                self.turn_id = next(iter(self._open_turns), None)
            else:
                if turn['id'] != self.turn_id:
                    raise RuntimeError('Provider changed the active turn')
                self._completed_turns.add(turn['id'])
                self.turn_id = None
                if turn.get('status') == 'completed' and (self.goal is not None or
                        self.initial_goal and not self.start_finished.is_set()):
                    if not replay:
                        self.track(self.finish_goal_turn())
                else:
                    self.terminal = True
                    self.control.close()
                    self.finish_input()
            scope = {'turn_id': turn['id']} if self.persistent else {}
            if turn.get('status') == 'completed':
                return {'type': 'turn.completed', **scope}
            error = turn.get('error') or {}
            quota = error.get('codexErrorInfo') == 'usageLimitExceeded'
            return {'type': 'turn.failed', **scope,
                    'error': {'codex_error_info': 'usage_limit_exceeded' if quota else None}}
        elif method == 'account/rateLimits/updated':
            return {'rate_limits': params.get('rateLimits') or {}}
        elif method in ('thread/goal/updated', 'thread/goal/cleared'):
            self.goal = params.get('goal') if method == 'thread/goal/updated' else None
            return {'type': 'goal_status', 'goal': self.goal}
        return None


    def track(self, coroutine):
        task = asyncio.create_task(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def disconnect(self):
        self.closed = True
        self.control.close()
        for wire_id in list(self.unstarted):
            self.outcome(wire_id, 'uncertain')
        self.unstarted.clear()
        for task in self._tasks:
            task.cancel()
        for future in self.messages.values():
            if not future.done():
                future.set_result('uncertain')
        for future in self.requests.values():
            if not future.done():
                future.set_exception(BrokenPipeError())


class RequestRejected(Exception):
    pass
