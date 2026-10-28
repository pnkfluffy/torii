"""Offline ACP lifecycle checks with a journaled, in-test scripted agent."""

import asyncio
from dataclasses import replace
import json
import os
import sys
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from coordinator.acp_protocol import AcpAdapter, AcpNativeSteer, AcpProtocol, INITIALIZE
from coordinator.host import HostClient
from coordinator.native_protocol import RunControl, steer_wire
from coordinator.opencode_runtime import environment
from tests.fake_opencode import SESSION_ID, install_fake_opencode
from tests.support import settle, stop_test_hosts


SESSION = 'session-a'
CWD = '/workspace'
TOOL_UPDATES = [
    {'sessionUpdate': 'tool_call', 'toolCallId': 'fake-workers-call', 'title': 'torii_workers_spawn',
     'kind': 'other', 'status': 'pending', 'locations': [], 'rawInput': {}},
    {'sessionUpdate': 'tool_call_update', 'toolCallId': 'fake-workers-call', 'status': 'in_progress',
     'kind': 'other', 'title': 'torii_workers_spawn', 'locations': [], 'rawInput': {'task': 'synthetic worker'}},
    {'sessionUpdate': 'tool_call_update', 'toolCallId': 'fake-workers-call', 'status': 'completed',
     'content': [{'type': 'content', 'content': {'type': 'text', 'text': 'FAKE_MCP_RESULT workers_spawn'}}],
     'rawOutput': {'output': 'FAKE_MCP_RESULT workers_spawn', 'metadata': {'truncated': False}}}]


def request(number, method, params):
    return {'jsonrpc': '2.0', 'id': number, 'method': method, 'params': params}


def prompt_params(text):
    return {'sessionId': SESSION, 'prompt': [{'type': 'text', 'text': text}]}


def chunk(text, session=SESSION, kind='agent_message_chunk'):
    return {'jsonrpc': '2.0', 'method': 'session/update',
            'params': {'sessionId': session, 'update': {'sessionUpdate': kind,
                                                      'content': {'type': 'text', 'text': text}}}}


class ScriptedStdin:
    def __init__(self, agent):
        self.agent = agent
        self.buffer = b''
        self.closed = asyncio.Event()
        self.close_count = 0

    def write(self, data):
        if self.closed.is_set():
            raise BrokenPipeError()
        self.buffer += data

    async def drain(self):
        while b'\n' in self.buffer:
            line, self.buffer = self.buffer.split(b'\n', 1)
            event = json.loads(line)
            self.agent.writes.append(event)
            await self.agent.receive(event)

    def close(self):
        self.close_count += 1
        self.closed.set()

    async def wait_closed(self):
        await self.closed.wait()


class ScriptedAgent:
    def __init__(self, writes=(), events=(), auth=False):
        self.writes = list(writes)
        self.events = list(events)
        self.stdin = ScriptedStdin(self)
        self.protocol = None
        self.auth = auth
        self.auth_result = {}
        self.version = 'test-1'
        self.session = SESSION
        self.errors = {}
        self.hang = set()
        self.auto_prompt = True
        self.cancel_reply = True
        self.echo = True
        self.load_history = []
        self.seen = {}
        self.responses = {}
        self.output = asyncio.Queue()

    def written_lines(self):
        return [json.dumps(event) for event in self.writes]

    def max_request_id(self):
        return max((event['id'] for event in self.writes if type(event.get('id')) is int), default=0)

    def past_request(self, number):
        self.seen.setdefault(number, asyncio.Event()).set()
        return next((event for event in self.writes if event.get('id') == number), None)

    def past_response(self, number):
        return next((event for event in self.events if event.get('id') == number
                     and ('result' in event or 'error' in event)), None)

    def history_after(self, seq):
        return (json.dumps(event) for event in self.events[seq:])

    def emit(self, event, replay=False):
        self.events.append(event)
        result = self.protocol.handle(event, replay=replay)
        if result is not None:
            self.output.put_nowait(result)
        if 'id' in event and 'method' not in event:
            self.responses.setdefault(event['id'], asyncio.Event()).set()
        return result

    def reply(self, number, result=None, error=None):
        event = {'jsonrpc': '2.0', 'id': number}
        event.update(error=error) if error is not None else event.update(result=result or {})
        return self.emit(event)

    async def reached(self, number):
        await asyncio.wait_for(self.seen.setdefault(number, asyncio.Event()).wait(), 1)

    async def receive(self, event):
        method = event.get('method')
        if method is None or method in self.hang:
            return
        if method in self.errors:
            self.reply(event['id'], error=self.errors[method])
        elif method == 'initialize':
            self.reply(event['id'], {'protocolVersion': 1, 'agentInfo': {'version': self.version},
                                     '_meta': {'agentVersion': self.version}})
        elif method == 'authenticate':
            self.auth = True
            self.reply(event['id'], self.auth_result)
        elif method == 'session/new':
            self.reply(event['id'], {'sessionId': self.session})
        elif method == 'session/load':
            for notification in self.load_history:
                self.emit(notification)
            self.reply(event['id'])
        elif method == 'session/prompt' and self.auto_prompt:
            self.emit(chunk('answer'))
            self.reply(event['id'], {'stopReason': 'end_turn'})
        elif method == 'test/interject':
            self.reply(event['id'], {'status': 'queued'})
            if self.echo:
                self.emit({'jsonrpc': '2.0', 'method': 'test/interjection', 'params': event['params']})
        elif method == 'session/cancel' and self.cancel_reply:
            prompts = [event for event in self.writes if event.get('method') == 'session/prompt']
            if prompts:
                self.reply(prompts[-1]['id'], {'stopReason': 'cancelled'})


class AcpProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.protocols = []
        self.tasks = []
        self.permissions = []
        self.classified = []
        self.usage = []
        self.host_chunks = {}
        self.host_events = {}

        async def permission(params):
            self.permissions.append(params)
            return {'outcome': {'outcome': 'cancelled'}}

        def classify(method, error):
            self.classified.append((method, error))
            return error.get('data', {}).get('failure_code', 'provider_error')

        def usage(result, update):
            self.usage.append((result, update))
            return {'observed': True}

        self.adapter = AcpAdapter(argv=lambda: ['scripted-agent'], environment=lambda: {},
                                  check_version=lambda result: result['agentInfo']['version'] == 'test-1',
                                  valid_session_id=lambda value: value == SESSION,
                                  permission=permission, classify_error=classify, usage=usage)

    async def asyncTearDown(self):
        for protocol in self.protocols:
            for method, future in protocol.requests.values():
                if not future.done():
                    future.cancel()
            for task in protocol._tasks:
                task.cancel()
            if protocol._stop_task is not None:
                self.tasks.append(protocol._stop_task)
            self.tasks.append(protocol.input_closed)
        for task in self.tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

    def build(self, agent=None, adapter=None, reattaching=False, session=None, **kwargs):
        agent = agent or ScriptedAgent()
        observed = []

        def observe(value):
            observed.append(value)
            return value

        control = RunControl(receipt_timeout=0.02)
        protocol = AcpProtocol(adapter or self.adapter, agent, control, session, observe,
                               reattaching=reattaching, **kwargs)
        agent.protocol = protocol
        self.protocols.append(protocol)
        return protocol, agent, observed

    def launch(self, protocol, text='hello', fresh=True, **kwargs):
        task = asyncio.create_task(protocol.start(text, Path(CWD), fresh, None, **kwargs))
        self.tasks.append(task)
        return task

    def saved(self, auth=False, load=False):
        writes = [request(1, 'initialize', INITIALIZE)]
        events = [{'jsonrpc': '2.0', 'id': 1,
                   'result': {'protocolVersion': 1, 'agentInfo': {'version': 'test-1'}}}]
        if auth:
            writes.append(request(2, 'authenticate', {'methodId': 'cached_token'}))
            events.append({'jsonrpc': '2.0', 'id': 2, 'result': {}})
        number = len(writes) + 1
        writes.append(request(number, 'session/load' if load else 'session/new',
                              {'cwd': CWD, 'mcpServers': [], **({'sessionId': SESSION} if load else {})}))
        events.append({'jsonrpc': '2.0', 'id': number, 'result': {} if load else {'sessionId': SESSION}})
        writes.append(request(number + 1, 'session/prompt', prompt_params('hello')))
        return writes, events

    async def authenticate(self, protocol):
        await protocol.request('authenticate', {'methodId': 'cached_token'})

    async def host_protocol(self, root, mode='end_turn', attach=False, persistent=False):
        directory = root / 'state' / 'hosts' / 'worker-acp'
        if attach:
            process = HostClient.attach(directory)
            await process.connect()
        else:
            spec = {'argv': [sys.executable, str(Path(__file__).with_name('fake_acp_core.py')), mode],
                    'cwd': str(root), 'env': {'PATH': os.environ['PATH'], 'HOME': str(root)},
                    'provider': 'opencode', 'locks': []}
            process = await HostClient.launch(directory, spec)
        adapter = replace(self.adapter, valid_session_id=lambda value: value == 'ses_1b3d19f6fffeIxa5icm1pIrpgt',
                          queued_receipt='queued' if persistent else 'uncertain')
        protocol = AcpProtocol(adapter, process, RunControl(receipt_timeout=0.02), None,
                               lambda value: value, reattaching=attach, persistent=persistent)
        self.protocols.append(protocol)

        chunks = asyncio.Queue()
        self.host_chunks[process] = chunks
        output = asyncio.Queue()
        self.host_events[process] = output

        def parse(raw, replay=False):
            event = json.loads(raw)
            parsed = protocol.handle(event, replay=replay)
            if parsed is not None:
                output.put_nowait(parsed)
            if event.get('params', {}).get('update', {}).get('sessionUpdate') == 'agent_message_chunk':
                chunks.put_nowait(event)

        async def read_stream():
            if attach:
                for raw in process.history(0):
                    parse(raw, replay=True)
            while True:
                raw = await process.next_event()
                if raw is None:
                    protocol.disconnect()
                    return
                parse(raw)
                process.ack()

        reader = asyncio.create_task(read_stream())
        self.tasks.append(reader)
        return protocol, process, reader

    async def release_host_prompt(self, process):
        process.stdin.write((json.dumps({'jsonrpc': '2.0', 'method': 'test/release'}) + '\n').encode())
        await process.stdin.drain()

    async def host_chunk(self, process):
        return await settle(self.host_chunks[process].get())

    async def test_agent_crashes_fail_without_an_owner_cancel(self):
        for mode in ('die-at-start', 'die-mid-prompt'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                try:
                    protocol, process, reader = await self.host_protocol(root, mode)
                    await settle(asyncio.gather(self.launch(protocol), reader))
                    await settle(process.wait())
                    protocol.disconnect()
                    self.assertEqual((protocol.outcome, protocol.failure), ('failed', 'disconnected'))
                    self.assertFalse(any(json.loads(raw).get('method') == 'session/cancel'
                                         for raw in process.written_lines()))
                    process.detach()
                finally:
                    stop_test_hosts(root)

    async def test_queued_steer_settles_only_after_its_prompt_is_written(self):
        for ending in ('end_turn', 'max_tokens', 'error429', 'disconnect'):
            with self.subTest(ending=ending):
                protocol, agent, _ = self.build()
                agent.auto_prompt = False
                outcomes = []
                protocol.control.on_outcome = lambda mid, state: outcomes.append((mid, state))
                task = self.launch(protocol)
                await agent.reached(3)
                self.assertEqual(await protocol.control.steer('one', 'first'), 'uncertain')
                self.assertEqual(await protocol.control.steer('two', 'second'), 'uncertain')
                self.assertEqual(outcomes, [])
                if ending == 'disconnect':
                    protocol.disconnect()
                elif ending == 'error429':
                    agent.reply(3, error={'code': 429, 'data': {'failure_code': 'rate_limited'}})
                else:
                    agent.reply(3, {'stopReason': ending})
                if ending == 'end_turn':
                    await agent.reached(4)
                    self.assertEqual(agent.writes[-1], request(4, 'session/prompt', prompt_params('first\n\nsecond')))
                    self.assertEqual(outcomes, [('one', 'received'), ('two', 'received')])
                    agent.reply(4, {'stopReason': 'end_turn'})
                await settle(task)
                if ending != 'disconnect':
                    await settle(protocol.input_closed)
                self.assertEqual(outcomes, [('one', 'received'), ('two', 'received')] if ending == 'end_turn' else [])
                self.assertEqual(await protocol.steer('late', 'too late'), 'closed')

    async def test_unsent_steer_stays_uncertain_after_host_reattach(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            try:
                old, process, reader = await self.host_protocol(root)
                self.launch(old)
                await self.host_chunk(process)
                outcomes = []
                old.control.on_outcome = lambda mid, state: outcomes.append((mid, state))
                self.assertEqual(await old.control.steer('lost', 'unsent text'), 'uncertain')
                old.disconnect()
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)
                process.detach()
                resumed, process, reader = await self.host_protocol(root, attach=True)
                start = self.launch(resumed)
                await self.release_host_prompt(process)
                await settle(asyncio.gather(start, reader))
                self.assertEqual(resumed.outcome, 'succeeded')
                self.assertEqual(outcomes, [])
                prompts = [json.loads(raw) for raw in process.written_lines()
                           if json.loads(raw).get('method') == 'session/prompt']
                self.assertEqual(len(prompts), 1)
                self.assertNotIn('unsent text', json.dumps(prompts))
                process.detach()
            finally:
                stop_test_hosts(root)

    async def test_written_joined_prompt_is_not_repeated_after_host_reattach(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            try:
                old, process, reader = await self.host_protocol(root)
                self.launch(old)
                await self.host_chunk(process)
                self.assertEqual(await old.control.steer('sent', 'joined text'), 'uncertain')
                await self.release_host_prompt(process)
                await self.host_chunk(process)
                old.disconnect()
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)
                process.detach()
                resumed, process, reader = await self.host_protocol(root, attach=True)
                start = self.launch(resumed)
                await self.release_host_prompt(process)
                await settle(asyncio.gather(start, reader))
                self.assertEqual(resumed.outcome, 'succeeded')
                prompts = [json.loads(raw) for raw in process.written_lines()
                           if json.loads(raw).get('method') == 'session/prompt']
                self.assertEqual(len(prompts), 2)
                self.assertEqual(prompts[-1]['params']['prompt'][0]['text'], 'joined text')
                process.detach()
            finally:
                stop_test_hosts(root)

    async def test_start_returns_and_run_cancels_with_prompt_response_held(self):
        protocol, agent, _ = self.build()
        agent.auto_prompt = False
        input_task = self.launch(protocol)
        stream = asyncio.Event()

        async def provider_run():
            try:
                await asyncio.shield(input_task)
                await stream.wait()
            except asyncio.CancelledError:
                while not input_task.done():
                    try:
                        await asyncio.shield(input_task)
                    except asyncio.CancelledError:
                        continue
                raise
            finally:
                protocol.disconnect()

        run = asyncio.create_task(provider_run())
        self.tasks.append(run)
        try:
            await agent.reached(3)
            await asyncio.wait_for(asyncio.shield(input_task), 1)
            self.assertIsNone(agent.past_response(3))
            self.assertFalse(protocol.terminal)
            self.assertTrue(protocol._tasks)
            run.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(asyncio.shield(run), 1)
            self.assertIsNone(agent.past_response(3))
            self.assertEqual((protocol.outcome, protocol.failure), ('failed', 'disconnected'))
            await asyncio.gather(*protocol._tasks, return_exceptions=True)
            self.assertFalse(protocol.requests)
            self.assertFalse(any(event.get('method') == 'session/cancel' for event in agent.writes))
        finally:
            agent.reply(3, {'stopReason': 'end_turn'})
            run.cancel()
            await asyncio.gather(run, return_exceptions=True)

    async def test_spend_sums_prompt_tokens_and_keeps_latest_cumulative_cost(self):
        def usage(result, update):
            return {**result['usage'], 'usd_estimate': update['cost']['amount'], 'source': 'adapter estimate'}
        protocol, agent, _ = self.build(adapter=replace(self.adapter, usage=usage))
        agent.auto_prompt = False
        task = self.launch(protocol)
        await agent.reached(3)
        await protocol.steer('follow', 'next turn')
        for number, tokens, cost in ((3, (12, 3, 4), 1.5), (4, (7, 2, 1), 2.5)):
            await agent.reached(number)
            agent.emit({'method': 'session/update', 'params': {'sessionId': SESSION, 'update': {
                'sessionUpdate': 'usage_update', 'cost': {'amount': cost, 'currency': 'USD'}}}})
            agent.reply(number, {'stopReason': 'end_turn', 'usage': dict(zip(
                ('input_tokens', 'output_tokens', 'cached_tokens'), tokens))})
        await settle(task)
        await settle(protocol.input_closed)
        self.assertEqual(protocol.spend, {'input_tokens': 19, 'output_tokens': 5, 'cached_tokens': 5,
                                          'usd_estimate': 2.5, 'source': 'adapter estimate'})

    async def test_result_keeps_last_message_without_gluing_messages_or_turns(self):
        protocol, agent, _ = self.build(text_limit=12)
        agent.auto_prompt = False
        task = self.launch(protocol)
        await agent.reached(3)
        agent.emit(chunk('first'))
        agent.emit(chunk('', kind='tool_call'))
        event = agent.emit(chunk('last'))
        self.assertEqual(event['item']['text'], 'last')
        agent.emit(chunk('', kind='usage_update'))
        agent.emit(chunk(' message'))
        self.assertEqual(protocol.text, 'last message')
        agent.emit(chunk('!'))
        self.assertEqual(protocol.text, 'ast message!')
        await protocol.steer('next', 'follow-up')
        agent.reply(3, {'stopReason': 'end_turn'})
        await agent.reached(4)
        agent.emit(chunk('turn two'))
        self.assertEqual(protocol.text, 'turn two')
        agent.reply(4, {'stopReason': 'end_turn'})
        await settle(task)
        await settle(protocol.input_closed)
        self.assertEqual(protocol.text, 'turn two')

    async def test_worker_keeps_previous_text_when_steered_prompt_emits_no_text(self):
        protocol, agent, _ = self.build()
        agent.auto_prompt = False
        task = self.launch(protocol)
        await agent.reached(3)
        agent.emit(chunk('Worker result'))
        await protocol.steer('next', 'follow-up')
        agent.reply(3, {'stopReason': 'end_turn'})
        await agent.reached(4)
        agent.reply(4, {'stopReason': 'end_turn'})
        await settle(task)
        await settle(protocol.input_closed)
        self.assertEqual(protocol.text, 'Worker result')

    async def test_agent_cancelled_without_owner_stop_is_a_named_failure(self):
        protocol, agent, _ = self.build()
        agent.auto_prompt = False
        task = self.launch(protocol)
        await agent.reached(3)
        agent.reply(3, {'stopReason': 'cancelled'})
        await settle(task)
        await settle(protocol.input_closed)
        self.assertEqual((protocol.outcome, protocol.failure), ('failed', 'cancelled'))

    async def test_refused_host_connection_fails_start_and_owner_cancel_is_stopped(self):
        for stopping in (False, True):
            with self.subTest(stopping=stopping):
                protocol, agent, _ = self.build(cancel_grace=0.01)
                agent.auto_prompt = False
                task = self.launch(protocol)
                if stopping:
                    await agent.reached(3)
                    with patch.object(agent.stdin, 'write', side_effect=ConnectionRefusedError):
                        await protocol.cancel()
                    await settle(task)
                    self.assertEqual((protocol.outcome, protocol.failure), ('stopped', 'owner_stopped'))
                else:
                    with patch.object(agent.stdin, 'write', side_effect=ConnectionRefusedError):
                        await settle(task)
                    self.assertEqual((protocol.outcome, protocol.failure), ('failed', 'disconnected'))

    async def test_handshake_without_auth_and_bounded_result_event(self):
        protocol, agent, observed = self.build()
        await self.launch(protocol, developer_instructions='instructions')
        self.assertEqual(agent.writes, [request(1, 'initialize', INITIALIZE),
                                       request(2, 'session/new', {'cwd': CWD, 'mcpServers': []}),
                                       request(3, 'session/prompt', prompt_params('instructions\n\nhello'))])
        self.assertEqual(observed, [SESSION])
        self.assertEqual(protocol.text, 'answer')
        self.assertEqual(protocol.outcome, 'succeeded')
        self.assertIsNone(protocol.failure)
        await protocol.input_closed
        protocol.finish_input()
        self.assertEqual(agent.stdin.close_count, 1)

    async def test_handshake_with_auth_and_provider_version_hook(self):
        adapter = replace(self.adapter, auth_step=self.authenticate,
                          check_version=lambda result: result['_meta']['agentVersion'] == 'test-1')
        protocol, agent, _ = self.build(adapter=adapter)
        await self.launch(protocol)
        self.assertEqual([event['method'] for event in agent.writes],
                         ['initialize', 'authenticate', 'session/new', 'session/prompt'])
        self.assertTrue(agent.auth)
        self.assertTrue(protocol.prompt_sent)

    async def test_auth_step_can_reject_a_successful_response_before_session_creation(self):
        async def authenticate(protocol):
            result = await protocol.request('authenticate', {'methodId': 'cached_token'})
            if result.get('_meta', {}).get('gate') is not None:
                protocol.complete('failed', 'quota')

        protocol, agent, _ = self.build(adapter=replace(self.adapter, auth_step=authenticate))
        agent.auth_result = {'_meta': {'gate': 'account needs a plan'}}
        await self.launch(protocol)
        self.assertEqual(protocol.failure, 'quota')
        self.assertEqual([event['method'] for event in agent.writes], ['initialize', 'authenticate'])
        self.assertFalse(protocol.prompt_sent)

    async def test_new_request_ids_start_after_host_maximum(self):
        agent = ScriptedAgent(writes=[request(42, 'old/request', {})])
        protocol, agent, _ = self.build(agent)
        await self.launch(protocol)
        self.assertEqual([event['id'] for event in agent.writes], [42, 43, 44, 45])

    async def test_reattach_waits_for_live_prompt_response_without_writing(self):
        writes, events = self.saved()
        protocol, agent, _ = self.build(ScriptedAgent(writes, events), reattaching=True)
        task = self.launch(protocol)
        await agent.reached(3)
        await settle(task)
        self.assertIsNone(agent.past_response(3))
        self.assertEqual(agent.writes, writes)
        agent.emit(chunk('live answer'))
        agent.reply(3, {'stopReason': 'end_turn'})
        await task
        await settle(protocol.input_closed)
        self.assertEqual(protocol.text, 'live answer')
        self.assertEqual(protocol.outcome, 'succeeded')
        self.assertEqual(agent.writes, writes)

    async def test_reattach_uses_journaled_prompt_response_without_writing(self):
        writes, events = self.saved()
        events.append({'jsonrpc': '2.0', 'id': 3, 'result': {'stopReason': 'end_turn'}})
        protocol, agent, _ = self.build(ScriptedAgent(writes, events), reattaching=True)
        await self.launch(protocol)
        self.assertEqual(protocol.outcome, 'succeeded')
        self.assertEqual(agent.writes, writes)

    async def test_reattach_replays_optional_auth_without_writing(self):
        writes, events = self.saved(auth=True)
        events.append({'jsonrpc': '2.0', 'id': 4, 'result': {'stopReason': 'end_turn'}})
        adapter = replace(self.adapter, auth_step=self.authenticate)
        protocol, agent, _ = self.build(ScriptedAgent(writes, events), adapter=adapter, reattaching=True)
        await self.launch(protocol)
        self.assertEqual(protocol.outcome, 'succeeded')
        self.assertEqual(agent.writes, writes)
        self.assertTrue(protocol.prompt_sent)

    async def test_reattach_replays_multiple_paid_turns_and_continues_ids(self):
        writes, events = self.saved()
        events.append({'jsonrpc': '2.0', 'id': 3, 'result': {'stopReason': 'end_turn'}})
        writes.extend([request(4, 'test/interject', {'old': True}),
                       request(5, 'session/prompt', prompt_params('saved follow-up'))])
        protocol, agent, _ = self.build(ScriptedAgent(writes, events), reattaching=True)
        agent.auto_prompt = False
        task = self.launch(protocol)
        await agent.reached(5)
        self.assertEqual(agent.writes, writes)
        self.assertEqual(await protocol.control.steer('next', 'new follow-up'), 'uncertain')
        agent.reply(5, {'stopReason': 'end_turn'})
        await agent.reached(6)
        self.assertEqual(agent.writes[-1], request(6, 'session/prompt', prompt_params('new follow-up')))
        agent.reply(6, {'stopReason': 'end_turn'})
        await task
        await settle(protocol.input_closed)
        self.assertEqual(sum(event.get('method') == 'session/prompt' for event in agent.writes), 3)

    async def test_prior_request_mismatches_raise_instead_of_writing(self):
        for index in range(3):
            with self.subTest(index=index):
                writes, events = self.saved()
                writes[index] = {**writes[index], 'params': {'different': True}}
                protocol, agent, _ = self.build(ScriptedAgent(writes, events), reattaching=True)
                with self.assertRaisesRegex(RuntimeError, 'different prior request'):
                    await self.launch(protocol)
                self.assertEqual(agent.writes, writes)

    async def test_changed_prompt_is_rejected_even_if_old_response_exists(self):
        writes, events = self.saved()
        events.append({'jsonrpc': '2.0', 'id': 3, 'result': {'stopReason': 'end_turn'}})
        protocol, agent, _ = self.build(ScriptedAgent(writes, events), reattaching=True)
        with self.assertRaisesRegex(RuntimeError, 'different prior request'):
            await self.launch(protocol, text='changed prompt')
        self.assertEqual(agent.writes, writes)

    async def test_partial_handshake_only_writes_unsent_requests(self):
        writes, events = self.saved()
        protocol, agent, _ = self.build(ScriptedAgent(writes[:1], events[:1]), reattaching=True)
        await self.launch(protocol)
        self.assertEqual(agent.writes, writes)
        self.assertEqual(protocol.outcome, 'succeeded')

    async def test_request_uses_real_host_journals(self):
        writes, events = self.saved()
        events.append({'jsonrpc': '2.0', 'id': 3, 'result': {'stopReason': 'end_turn'}})
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'writes.jsonl').write_text(''.join(json.dumps({'line': json.dumps(event)}) + '\n'
                                                     for event in writes))
            (root / 'events.jsonl').write_text(''.join(json.dumps({'seq': seq, 'data': json.dumps(event)}) + '\n'
                                                     for seq, event in enumerate(events, 1)))
            process = HostClient(root)
            process.stdin = ScriptedStdin(ScriptedAgent())
            protocol = AcpProtocol(self.adapter, process, RunControl(), None, lambda value: value, reattaching=True)
            self.protocols.append(protocol)
            await self.launch(protocol)
            self.assertEqual(protocol.outcome, 'succeeded')
            self.assertEqual(process.written_lines(), [json.dumps(event) for event in writes])

    async def test_load_suppresses_history_and_uses_exact_id(self):
        protocol, agent, observed = self.build(session=SESSION)
        agent.load_history = [chunk('old answer'), chunk('old cost', kind='usage_update')]
        await self.launch(protocol, fresh=False)
        self.assertEqual(agent.writes[1], request(2, 'session/load',
                                                {'sessionId': SESSION, 'cwd': CWD, 'mcpServers': []}))
        self.assertEqual(protocol.text, 'answer')
        self.assertEqual(observed, [SESSION])
        self.assertEqual(self.usage[-1][1], None)
        self.assertNotIn('session/new', [event['method'] for event in agent.writes])

    async def test_reattach_load_suppresses_history_before_start(self):
        writes, events = self.saved(load=True)
        agent = ScriptedAgent(writes, events)
        protocol, agent, _ = self.build(agent, reattaching=True, session=SESSION)
        agent.emit(chunk('old history'), replay=True)
        agent.emit(events[1], replay=True)
        agent.emit(chunk('saved answer'), replay=True)
        events.append({'jsonrpc': '2.0', 'id': 3, 'result': {'stopReason': 'end_turn'}})
        agent.events = events
        await self.launch(protocol, fresh=False)
        self.assertEqual(protocol.text, 'saved answer')
        self.assertEqual(agent.writes, writes)

    async def test_missing_session_fails_without_new_session_or_prompt(self):
        protocol, agent, _ = self.build(session=SESSION)
        error = {'code': -32603, 'message': 'not found', 'data': {'failure_code': 'session_missing'}}
        agent.errors['session/load'] = error
        await self.launch(protocol, fresh=False)
        self.assertEqual(protocol.failure, 'session_missing')
        self.assertEqual(protocol.outcome, 'failed')
        self.assertEqual(self.classified, [('session/load', error)])
        self.assertEqual([event['method'] for event in agent.writes], ['initialize', 'session/load'])

    async def test_invalid_session_ids_never_reach_prompt_or_observer(self):
        for fresh in (True, False):
            for session in (None, '', '../escape', 'different-session'):
                with self.subTest(fresh=fresh, session=session):
                    protocol, agent, observed = self.build(session=session)
                    agent.session = session
                    await self.launch(protocol, fresh=fresh)
                    self.assertEqual(protocol.failure, 'invalid_session_id')
                    self.assertEqual(observed, [])
                    self.assertNotIn('session/prompt', [event['method'] for event in agent.writes])
                    if not fresh:
                        self.assertEqual(len(agent.writes), 1)

    async def test_permissions_go_to_hook_and_other_agent_requests_are_rejected(self):
        protocol, agent, _ = self.build()
        agent.auto_prompt = False
        task = self.launch(protocol)
        await agent.reached(3)
        params = {'sessionId': SESSION, 'options': [{'kind': 'reject_once', 'optionId': 'reject'}]}
        agent.emit(request('permission', 'session/request_permission', params))
        agent.emit(request('read', 'fs/read_text_file', {'path': '/outside'}))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertEqual(self.permissions, [params])
        self.assertIn({'jsonrpc': '2.0', 'id': 'permission',
                       'result': {'outcome': {'outcome': 'cancelled'}}}, agent.writes)
        self.assertIn({'jsonrpc': '2.0', 'id': 'read',
                       'error': {'code': -32601, 'message': 'Method not found'}}, agent.writes)
        agent.reply(3, {'stopReason': 'end_turn'})
        await task
        await settle(protocol.input_closed)

    async def test_replayed_agent_requests_are_not_answered_again(self):
        replies = [{'jsonrpc': '2.0', 'id': 'old', 'result': {}},
                   {'jsonrpc': '2.0', 'id': 'old-read', 'error': {'code': -32601}}]
        protocol, agent, _ = self.build(ScriptedAgent(writes=replies))
        protocol.handle(request('old', 'session/request_permission', {}), replay=True)
        protocol.handle(request('old-read', 'fs/read_text_file', {}), replay=True)
        await asyncio.sleep(0)
        self.assertEqual(self.permissions, [])
        self.assertEqual(agent.writes, replies)

    async def test_unanswered_replayed_permission_is_answered_once(self):
        protocol, agent, _ = self.build()
        event = request('pending', 'session/request_permission', {'sessionId': SESSION})
        protocol.handle(event, replay=True)
        protocol.handle(event, replay=True)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        protocol.handle(event, replay=True)
        self.assertEqual(self.permissions, [{'sessionId': SESSION}])
        self.assertEqual(agent.writes, [{'jsonrpc': '2.0', 'id': 'pending',
                                        'result': {'outcome': {'outcome': 'cancelled'}}}])

    async def test_reattach_keeps_fresh_session_text_replayed_before_start(self):
        writes, events = self.saved()
        protocol, agent, _ = self.build(ScriptedAgent(writes, events), reattaching=True)
        protocol.handle(events[1], replay=True)
        protocol.handle(chunk('saved answer'), replay=True)
        agent.reply(3, {'stopReason': 'end_turn'})
        await self.launch(protocol)
        self.assertEqual(protocol.text, 'saved answer')
        self.assertEqual(agent.writes, writes)

    async def test_permission_hook_failure_ends_a_pending_prompt(self):
        async def permission(params):
            raise ValueError('policy rejected the request')

        protocol, agent, _ = self.build(adapter=replace(self.adapter, permission=permission))
        agent.auto_prompt = False
        task = self.launch(protocol)
        await agent.reached(3)
        agent.emit(request('permission', 'session/request_permission', {'sessionId': SESSION}))
        await asyncio.wait_for(task, 1)
        await settle(protocol.input_closed)
        self.assertEqual(protocol.failure, 'permission_failed')
        self.assertEqual(protocol.outcome, 'failed')
        self.assertEqual(agent.stdin.close_count, 1)

    async def test_queued_steers_join_in_order_and_run_after_end_turn(self):
        protocol, agent, _ = self.build()
        agent.auto_prompt = False
        task = self.launch(protocol)
        await agent.reached(3)
        self.assertEqual(await protocol.control.steer('one', 'first'), 'uncertain')
        self.assertEqual(await protocol.control.steer('one', 'first'), 'uncertain')
        self.assertEqual(await protocol.control.steer('two', 'second'), 'uncertain')
        self.assertEqual(len(agent.writes), 3)
        agent.reply(3, {'stopReason': 'end_turn'})
        await agent.reached(4)
        self.assertEqual(agent.writes[-1], request(4, 'session/prompt', prompt_params('first\n\nsecond')))
        self.assertEqual(agent.stdin.close_count, 0)
        agent.reply(4, {'stopReason': 'end_turn'})
        await task
        await settle(protocol.input_closed)
        self.assertEqual(await protocol.steer('late', 'too late'), 'closed')
        self.assertEqual(await protocol.control.steer('late', 'too late'), 'closed')
        self.assertEqual(agent.stdin.close_count, 1)

    def native_adapter(self):
        native = AcpNativeSteer('test/interject',
                                lambda session, wire, text: {'sessionId': session, 'wire': wire, 'text': text},
                                lambda event, session, wire: event.get('method') == 'test/interjection'
                                and event.get('params', {}).get('wire') == wire)
        return replace(self.adapter, native_steer=native)

    async def test_native_steer_requires_notification_receipt_and_uses_steer_wire(self):
        protocol, agent, _ = self.build(adapter=self.native_adapter())
        agent.auto_prompt = False
        task = self.launch(protocol)
        await agent.reached(3)
        self.assertEqual(await protocol.control.steer('native', 'change'), 'received')
        self.assertEqual(agent.writes[-1], request(4, 'test/interject',
                                                {'sessionId': SESSION, 'wire': steer_wire('native'), 'text': 'change'}))
        self.assertEqual(protocol.queued, [])
        agent.echo = False
        self.assertEqual(await protocol.steer('no-echo', 'another'), 'uncertain')
        agent.reply(3, {'stopReason': 'end_turn'})
        await task
        await settle(protocol.input_closed)
        self.assertEqual(await protocol.steer('late', 'change'), 'closed')

    async def test_native_steer_error_is_classified(self):
        protocol, agent, _ = self.build(adapter=self.native_adapter())
        agent.auto_prompt = False
        agent.errors['test/interject'] = {'code': -32601, 'message': 'Method not found'}
        task = self.launch(protocol)
        await agent.reached(3)
        self.assertEqual(await protocol.steer('unsupported', 'text'), 'unsupported')
        self.assertEqual(self.classified[-1], ('test/interject', agent.errors['test/interject']))
        agent.reply(3, {'stopReason': 'end_turn'})
        await task
        await settle(protocol.input_closed)

    async def test_native_steer_reattach_uses_saved_request_and_receipt(self):
        writes, events = self.saved()
        params = {'sessionId': SESSION, 'wire': steer_wire('saved'), 'text': 'change'}
        writes.append(request(4, 'test/interject', params))
        events.extend([{'jsonrpc': '2.0', 'id': 4, 'result': {'status': 'queued'}},
                       {'jsonrpc': '2.0', 'method': 'test/interjection', 'params': params}])
        protocol, agent, _ = self.build(ScriptedAgent(writes, events), adapter=self.native_adapter(), reattaching=True)
        task = self.launch(protocol)
        await agent.reached(3)
        self.assertEqual(await protocol.steer('saved', 'change'), 'received')
        self.assertEqual(agent.writes, writes)
        agent.reply(3, {'stopReason': 'end_turn'})
        await task
        await settle(protocol.input_closed)

    async def test_numeric_permission_reply_ids_do_not_collide_with_new_requests(self):
        protocol, agent, _ = self.build()
        agent.auto_prompt = False
        task = self.launch(protocol)
        await agent.reached(3)
        agent.emit(request(90, 'session/request_permission', {'sessionId': SESSION}))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertEqual(await protocol.steer('next', 'follow-up'), 'uncertain')
        agent.reply(3, {'stopReason': 'end_turn'})
        await agent.reached(91)
        self.assertEqual(agent.writes[-1], request(91, 'session/prompt', prompt_params('follow-up')))
        agent.reply(91, {'stopReason': 'end_turn'})
        await task
        await settle(protocol.input_closed)

    async def test_cancel_waits_for_prompt_response_and_never_stops_child(self):
        protocol, agent, _ = self.build(cancel_grace=0.05)
        agent.auto_prompt = False
        task = self.launch(protocol)
        await agent.reached(3)
        self.assertEqual(await protocol.steer('discard', 'queued'), 'uncertain')
        stopping = protocol.cancel()
        self.assertIs(stopping, protocol.cancel())
        await stopping
        await task
        await settle(protocol.input_closed)
        self.assertEqual(agent.writes[-1], {'jsonrpc': '2.0', 'method': 'session/cancel',
                                          'params': {'sessionId': SESSION}})
        self.assertEqual(protocol.outcome, 'stopped')
        self.assertEqual(protocol.failure, 'owner_stopped')
        self.assertEqual(sum(event.get('method') == 'session/prompt' for event in agent.writes), 1)
        self.assertEqual(await protocol.steer('late', 'new'), 'closed')

    async def test_cancel_grace_expires_without_prompt_response(self):
        protocol, agent, _ = self.build(cancel_grace=0.01)
        agent.auto_prompt = False
        agent.cancel_reply = False
        task = self.launch(protocol)
        await agent.reached(3)
        await protocol.cancel()
        await task
        await settle(protocol.input_closed)
        self.assertEqual(protocol.outcome, 'stopped')
        self.assertTrue(protocol.terminal)
        self.assertEqual(agent.stdin.close_count, 1)

    async def test_stop_during_each_handshake_phase_prevents_a_prompt(self):
        for method, number in (('initialize', 1), ('authenticate', 2), ('session/new', 3), ('session/load', 3)):
            with self.subTest(method=method):
                adapter = replace(self.adapter, auth_step=self.authenticate)
                protocol, agent, _ = self.build(adapter=adapter, session=SESSION if method == 'session/load' else None,
                                                cancel_grace=0.01)
                agent.hang.add(method)
                task = self.launch(protocol, fresh=method != 'session/load')
                await agent.reached(number)
                await protocol.cancel()
                await task
                await settle(protocol.input_closed)
                self.assertEqual(protocol.outcome, 'stopped')
                self.assertNotIn('session/prompt', [event['method'] for event in agent.writes])

    async def test_terminal_stop_does_not_send_cancel(self):
        protocol, agent, _ = self.build()
        await self.launch(protocol)
        protocol.disconnect()
        self.assertEqual(len(agent.writes), 3)
        self.assertEqual(protocol.outcome, 'succeeded')

    async def test_stop_reasons_have_named_outcomes(self):
        for reason, outcome, failure in [('end_turn', 'succeeded', None),
                                         ('cancelled', 'failed', 'cancelled'),
                                         ('refusal', 'failed', 'refusal'),
                                         ('max_tokens', 'failed', 'max_tokens'),
                                         ('max_turn_requests', 'failed', 'max_turn_requests'),
                                         ('unexpected', 'failed', 'invalid_stop_reason'),
                                         (None, 'failed', 'invalid_stop_reason')]:
            with self.subTest(reason=reason):
                protocol, agent, _ = self.build()
                agent.auto_prompt = False
                task = self.launch(protocol)
                await agent.reached(3)
                agent.reply(3, {'stopReason': reason})
                await task
                await settle(protocol.input_closed)
                self.assertEqual((protocol.outcome, protocol.failure), (outcome, failure))
                self.assertEqual(agent.stdin.close_count, 1)

    async def test_errors_in_each_phase_go_to_adapter_classifier(self):
        for method in ('initialize', 'authenticate', 'session/new', 'session/prompt'):
            with self.subTest(method=method):
                adapter = replace(self.adapter, auth_step=self.authenticate)
                protocol, agent, _ = self.build(adapter=adapter)
                error = {'code': -32000, 'message': 'rejected', 'data': {'failure_code': 'login_required'}}
                agent.errors[method] = error
                await self.launch(protocol)
                self.assertEqual(protocol.failure, 'login_required')
                self.assertEqual(self.classified[-1], (method, error))
                self.assertEqual(agent.writes[-1]['method'], method)

    async def test_initialize_new_and_load_have_handshake_timeouts(self):
        original = asyncio.wait_for
        timeouts = []

        async def shortened(awaitable, timeout):
            timeouts.append(timeout)
            return await original(awaitable, min(timeout, 0.01))

        for method in ('initialize', 'session/new', 'session/load'):
            with self.subTest(method=method):
                protocol, agent, _ = self.build(session=SESSION)
                protocol.control.receipt_timeout = 13
                agent.hang.add(method)
                with patch('coordinator.acp_protocol.asyncio.wait_for', shortened):
                    await self.launch(protocol, fresh=method != 'session/load')
                self.assertEqual(protocol.failure, 'receipt_timeout')
                self.assertEqual(timeouts[-1], 130)
                self.assertEqual(agent.writes[-1]['method'], method)
                self.assertEqual(agent.stdin.close_count, 1)

    async def test_prompt_has_no_receipt_timeout(self):
        protocol, agent, _ = self.build()
        agent.auto_prompt = False
        task = self.launch(protocol)
        await agent.reached(3)
        await settle(task)
        self.assertIsNone(agent.past_response(3))
        self.assertEqual(len(protocol.requests), 1)
        self.assertIsNone(protocol.failure)
        agent.reply(3, {'stopReason': 'end_turn'})
        await task
        await settle(protocol.input_closed)
        self.assertEqual(protocol.outcome, 'succeeded')

    async def test_text_is_bounded_and_non_message_notifications_are_not_kept(self):
        protocol, agent, _ = self.build(text_limit=12)
        agent.auto_prompt = False
        task = self.launch(protocol)
        await agent.reached(3)
        agent.emit(chunk('ignore', session='other-session'))
        agent.emit(chunk('thought', kind='agent_thought_chunk'))
        for _ in range(1000):
            event = agent.emit(chunk('abcdef'))
        self.assertEqual(event, {'type': 'item.completed', 'item': {'type': 'agent_message', 'text': 'abcdefabcdef'}})
        self.assertEqual(protocol.text, 'abcdefabcdef')
        agent.emit(chunk('0123456789abcdef'))
        self.assertEqual(protocol.text, '456789abcdef')
        self.assertFalse(hasattr(protocol, 'notifications'))
        self.assertEqual(agent.emit(chunk('', kind='tool_call')), {'type': 'activity'})
        agent.reply(3, {'stopReason': 'end_turn'})
        await task
        await settle(protocol.input_closed)

    async def test_usage_slot_receives_prompt_usage_and_last_update(self):
        protocol, agent, _ = self.build()
        agent.auto_prompt = False
        task = self.launch(protocol)
        await agent.reached(3)
        for amount in (1, 2):
            agent.emit({'jsonrpc': '2.0', 'method': 'session/update', 'params': {
                'sessionId': SESSION, 'update': {'sessionUpdate': 'usage_update', 'cost': {'amount': amount, 'currency': 'USD'}}}})
        result = {'stopReason': 'end_turn', 'usage': {'inputTokens': 12, 'outputTokens': 3}}
        agent.reply(3, result)
        await task
        await settle(protocol.input_closed)
        self.assertEqual(self.usage, [(result, {'sessionUpdate': 'usage_update', 'cost': {'amount': 2, 'currency': 'USD'}})])
        self.assertEqual(protocol.spend, {'observed': True})

    async def test_opencode_fresh_launch_still_requires_pin_for_both_adapters(self):
        from coordinator import opencode_runtime
        for factory in (opencode_runtime.adapter, opencode_runtime.parent_adapter):
            with self.subTest(factory=factory.__name__):
                adapter = factory(lambda *args: None, error={})
                protocol, agent, observed = self.build(adapter=adapter)
                agent.version = '1.19.0'
                await self.launch(protocol)
                self.assertEqual(protocol.failure, 'version_mismatch')
                self.assertEqual([event['method'] for event in agent.writes], ['initialize'])
                self.assertEqual(observed, [])

    async def test_opencode_reattach_without_saved_initialize_still_requires_pin(self):
        from coordinator import opencode_runtime
        protocol, agent, observed = self.build(adapter=opencode_runtime.adapter(lambda *args: None), reattaching=True)
        agent.version = '1.19.0'
        await self.launch(protocol)
        self.assertEqual(protocol.failure, 'version_mismatch')
        self.assertEqual([event['method'] for event in agent.writes], ['initialize'])
        self.assertEqual(observed, [])

    async def test_opencode_reattach_before_session_acceptance_still_requires_pin(self):
        from coordinator import opencode_runtime
        writes, events = self.saved()
        events[0]['result']['agentInfo']['version'] = '1.19.0'
        protocol, agent, observed = self.build(ScriptedAgent(writes[:1], events[:1]),
            adapter=opencode_runtime.adapter(lambda *args: None), reattaching=True)
        await self.launch(protocol)
        self.assertEqual(protocol.failure, 'version_mismatch')
        self.assertEqual(agent.writes, writes[:1])
        self.assertEqual(observed, [])

    async def test_opencode_replayed_initialize_still_requires_protocol_one(self):
        from coordinator import opencode_runtime
        writes, events = self.saved()
        events[0]['result'] = {'protocolVersion': 2, 'agentInfo': {'version': '1.18.0'}}
        protocol, agent, observed = self.build(ScriptedAgent(writes, events),
            adapter=opencode_runtime.adapter(lambda *args: None), reattaching=True)
        await self.launch(protocol)
        self.assertEqual(protocol.failure, 'version_mismatch')
        self.assertEqual(agent.writes, writes)
        self.assertEqual(observed, [])

    async def test_replayed_initialize_keeps_exact_adapter_version_check(self):
        writes, events = self.saved()
        events[0]['result']['agentInfo']['version'] = 'different-version'
        protocol, agent, observed = self.build(ScriptedAgent(writes, events),
            reattaching=True)
        await self.launch(protocol)
        self.assertEqual(protocol.failure, 'version_mismatch')
        self.assertEqual(agent.writes, writes)
        self.assertEqual(observed, [])

    async def test_version_mismatch_fails_before_auth_or_session_creation(self):
        adapter = replace(self.adapter, auth_step=self.authenticate)
        protocol, agent, observed = self.build(adapter=adapter)
        agent.version = 'different-version'
        await self.launch(protocol)
        self.assertEqual(protocol.failure, 'version_mismatch')
        self.assertEqual([event['method'] for event in agent.writes], ['initialize'])
        self.assertEqual(observed, [])

    async def test_every_write_is_jsonrpc_2_ndjson(self):
        protocol, agent, _ = self.build()
        agent.auto_prompt = False
        task = self.launch(protocol)
        await agent.reached(3)
        agent.emit(request('unknown', 'unknown', {}))
        await asyncio.sleep(0)
        await protocol.cancel()
        await task
        await settle(protocol.input_closed)
        self.assertTrue(all(event['jsonrpc'] == '2.0' for event in agent.writes))
        self.assertEqual(agent.stdin.buffer, b'')


    async def persistent_protocol(self, **kwargs):
        kwargs.setdefault('adapter', replace(self.adapter, queued_receipt='queued'))
        protocol, agent, observed = self.build(persistent=True, **kwargs)
        agent.auto_prompt = False
        await settle(self.launch(protocol, text=''))
        return protocol, agent, observed

    async def test_persistent_runs_many_prompts_on_one_session(self):
        protocol, agent, observed = await self.persistent_protocol()
        self.assertEqual(observed, [SESSION])
        self.assertEqual([event['method'] for event in agent.writes], ['initialize', 'session/new'])
        for index in range(3):
            mid = 'message-' + str(index)
            self.assertEqual(await settle(protocol.control.steer(mid, 'turn ' + str(index))), 'received')
            agent.emit(chunk('answer ' + str(index)))
            self.assertEqual((await settle(agent.output.get()))['item']['text'], 'answer ' + str(index))
            agent.reply(index + 3, {'stopReason': 'end_turn'})
            self.assertEqual(await settle(agent.output.get()),
                             {'type': 'turn.completed', 'messages': [mid], 'usage': {'observed': True},
                              'text': 'answer ' + str(index), 'turn_id': index + 3})
            self.assertFalse(protocol.terminal)
            self.assertIsNone(protocol.outcome)
        self.assertEqual(len(agent.writes), 5)
        await protocol.cancel()
        await settle(protocol.input_closed)
        self.assertEqual(await protocol.steer('late', 'text'), 'closed')

    async def test_persistent_idle_steer_writes_prompt_at_once(self):
        protocol, agent, _ = await self.persistent_protocol()
        entered = asyncio.Event()
        release = asyncio.Event()
        original = agent.stdin.drain

        async def held_drain():
            entered.set()
            await release.wait()
            await original()

        with patch.object(agent.stdin, 'drain', held_drain):
            steering = asyncio.create_task(protocol.steer('one', 'first'))
            self.tasks.append(steering)
            await settle(entered.wait())
            self.assertFalse(steering.done())
            self.assertEqual(len(agent.writes), 2)
            release.set()
            self.assertEqual(await settle(steering), 'received')
        self.assertEqual(agent.writes[-1], request(3, 'session/prompt',
                         {**prompt_params('first'), '_meta': {'toriiSteers': ['one']}}))
        self.assertIsNone(agent.past_response(3))
        protocol.disconnect()
        self.assertEqual(await protocol.steer('late', 'text'), 'closed')

    async def test_persistent_queued_steers_go_as_one_prompt_after_turn(self):
        protocol, agent, _ = await self.persistent_protocol()
        self.assertEqual(await settle(protocol.steer('one', 'first')), 'received')
        self.assertEqual(await settle(protocol.steer('two', 'second')), 'received')
        self.assertEqual(await settle(protocol.steer('three', 'third')), 'received')
        self.assertEqual(len(agent.writes), 3)
        agent.reply(3, {'stopReason': 'end_turn'})
        await agent.reached(4)
        self.assertEqual(agent.writes[-1], request(4, 'session/prompt',
                         {**prompt_params('second\n\nthird'), '_meta': {'toriiSteers': ['two', 'three']}}))
        agent.reply(4, {'stopReason': 'end_turn'})
        self.assertEqual((await settle(agent.output.get()))['messages'], ['one'])
        self.assertEqual((await settle(agent.output.get()))['messages'], ['two', 'three'])
        await protocol.cancel()
        await settle(protocol.input_closed)

    async def test_persistent_reattach_replays_journaled_prompts_without_writing(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            try:
                old, process, reader = await self.host_protocol(root, persistent=True)
                await settle(self.launch(old, text=''))
                for mid in ('one', 'two'):
                    self.assertEqual(await settle(old.steer(mid, mid + ' text')), 'received')
                    await self.host_chunk(process)
                    await self.release_host_prompt(process)
                    output = self.host_events[process]
                    self.assertEqual((await settle(output.get()))['type'], 'item.completed')
                    self.assertEqual((await settle(output.get()))['messages'], [mid])
                self.assertEqual(await settle(old.steer('three', 'held text')), 'received')
                await self.host_chunk(process)
                before = list(process.written_lines())
                old.disconnect()
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)
                process.detach()
                resumed, process, reader = await self.host_protocol(root, attach=True, persistent=True)
                await settle(self.launch(resumed, text=''))
                output = self.host_events[process]
                turns = []
                for _ in range(5):
                    event = await settle(output.get())
                    if event['type'] == 'turn.completed':
                        turns.append(event['messages'])
                self.assertEqual(turns, [['one'], ['two']])
                self.assertEqual(list(process.written_lines()), before)
                self.assertFalse(resumed.terminal)
                await self.release_host_prompt(process)
                self.assertEqual((await settle(output.get()))['messages'], ['three'])
                self.assertEqual(await settle(resumed.steer('four', 'new text')), 'received')
                await self.host_chunk(process)
                prompts = [json.loads(raw) for raw in process.written_lines()
                           if json.loads(raw).get('method') == 'session/prompt']
                self.assertEqual([event['params']['_meta']['toriiSteers'] for event in prompts],
                                 [['one'], ['two'], ['three'], ['four']])
                await resumed.cancel()
                await settle(reader)
                process.detach()
            finally:
                stop_test_hosts(root)

    async def test_persistent_turn_failure_completes_failed(self):
        for ending in ('refusal', 'max_tokens', 'max_turn_requests', 'cancelled', 'unexpected',
                       'error', 'disconnect', 'timeout'):
            with self.subTest(ending=ending):
                protocol, agent, _ = await self.persistent_protocol()
                self.assertEqual(await settle(protocol.steer('one', 'first')), 'received')
                if ending == 'error':
                    agent.reply(3, error={'code': -32000, 'data': {'failure_code': 'provider_error'}})
                elif ending == 'disconnect':
                    protocol.disconnect()
                elif ending == 'timeout':
                    for method, future in protocol.requests.values():
                        future.set_exception(asyncio.TimeoutError())
                else:
                    agent.reply(3, {'stopReason': ending})
                if ending != 'disconnect':
                    await settle(protocol.input_closed)
                self.assertEqual(protocol.outcome, 'failed')
                expected = {'error': 'provider_error', 'unexpected': 'invalid_stop_reason',
                            'timeout': 'receipt_timeout', 'disconnect': 'disconnected'}.get(ending, ending)
                self.assertEqual(protocol.failure, expected)
                self.assertTrue(agent.output.empty())
                self.assertEqual(await protocol.steer('late', 'text'), 'closed')

    async def test_persistent_start_refuses_prompt_or_instructions(self):
        for prompt, instructions in [('text', None), ('', ''), ('', 'instructions')]:
            with self.subTest(prompt=prompt, instructions=instructions):
                protocol, agent, _ = self.build(persistent=True)
                with self.assertRaises(ValueError):
                    await protocol.start(prompt, Path(CWD), True, None, developer_instructions=instructions)
                self.assertEqual(agent.writes, [])
                protocol.complete('failed')
                await settle(protocol.input_closed)

    async def test_persistent_complete_after_turn_closes_input(self):
        protocol, agent, _ = await self.persistent_protocol()
        self.assertEqual(await settle(protocol.steer('one', 'first')), 'received')
        agent.reply(3, {'stopReason': 'end_turn'})
        protocol.complete('succeeded')
        await settle(protocol.input_closed)
        self.assertEqual(protocol.outcome, 'succeeded')
        self.assertEqual(await protocol.steer('late', 'text'), 'closed')

    async def test_mcp_servers_sent_on_new_and_load(self):
        servers = [{'name': 'torii', 'command': '/python', 'args': ['-m', 'coordinator'],
                    'env': [{'name': 'PYTHONPATH', 'value': '/repo'}]}]
        for persistent in (False, True):
            for fresh in (False, True):
                with self.subTest(persistent=persistent, fresh=fresh):
                    protocol, agent, _ = self.build(mcp_servers=servers, persistent=persistent, session=SESSION)
                    await settle(self.launch(protocol, text='' if persistent else 'hello', fresh=fresh))
                    self.assertEqual(agent.writes[1], request(2, 'session/new' if fresh else 'session/load',
                                     {'cwd': CWD, 'mcpServers': servers, **({} if fresh else {'sessionId': SESSION})}))
                    if persistent:
                        await protocol.cancel()
                    await settle(protocol.input_closed)

    async def test_worker_mode_journal_unchanged(self):
        captures = {}
        for auth in (False, True):
            for fresh in (True, False):
                adapter = replace(self.adapter, auth_step=self.authenticate if auth else None,
                                  queued_receipt='uncertain' if auth else 'queued')
                protocol, agent, _ = self.build(adapter=adapter, session=None if fresh else SESSION)
                agent.auto_prompt = False
                await self.launch(protocol, text='owner request', fresh=fresh,
                                  developer_instructions='worker instructions')
                number = 4 if auth else 3
                await agent.reached(number)
                receipt = await protocol.steer('follow-up', 'next request')
                agent.reply(number, {'stopReason': 'end_turn'})
                await agent.reached(number + 1)
                agent.reply(number + 1, {'stopReason': 'end_turn'})
                await settle(protocol.input_closed)
                captures[('authenticated' if auth else 'opencode') + ('_fresh' if fresh else '_resume')] = {
                    'writes': agent.writes, 'receipt': receipt, 'outcome': protocol.outcome}
        fixture = Path(__file__).with_name('fixtures') / 'acp-worker-requests.json'
        self.assertEqual(captures, json.loads(fixture.read_text()))

    async def test_tool_call_activity_has_tool_name(self):
        protocol, agent, _ = await self.persistent_protocol()
        self.assertEqual(await settle(protocol.steer('one', 'first')), 'received')
        expected = [{'type': 'activity', 'name': 'torii_workers_spawn'},
                    {'type': 'activity', 'name': 'torii_workers_spawn'}, {'type': 'activity'}]
        for update, activity in zip(TOOL_UPDATES, expected):
            event = {'jsonrpc': '2.0', 'method': 'session/update',
                     'params': {'sessionId': SESSION, 'update': update}}
            self.assertEqual(agent.emit(event), activity)
        event = chunk('', kind='tool_call')
        event['params']['update']['kind'] = 'not-a-title'
        self.assertEqual(agent.emit(event), {'type': 'activity'})
        await protocol.cancel()
        await settle(protocol.input_closed)

    async def test_persistent_turn_usage_is_per_turn_and_may_be_absent(self):
        updates = []

        def usage(result, update):
            updates.append(update)
            return result.get('usage')
        protocol, agent, _ = await self.persistent_protocol(adapter=replace(self.adapter, usage=usage))
        for mid, amount in [('one', 2), ('two', 3), ('three', None)]:
            self.assertEqual(await settle(protocol.steer(mid, mid)), 'received')
            result = {'stopReason': 'end_turn'}
            if amount is not None:
                result['usage'] = {'input_tokens': amount}
            if mid == 'one':
                agent.emit({'method': 'session/update', 'params': {
                    'sessionId': SESSION, 'update': {'sessionUpdate': 'usage_update', 'cost': 1}}})
            agent.reply(agent.writes[-1]['id'], result)
            self.assertEqual(await settle(agent.output.get()),
                             {'type': 'turn.completed', 'messages': [mid], 'usage': result.get('usage'),
                              'text': '', 'turn_id': agent.writes[-1]['id']})
        self.assertEqual(protocol.spend, {'input_tokens': 5})
        self.assertEqual(updates, [{'sessionUpdate': 'usage_update', 'cost': 1}, None, None])
        await protocol.cancel()
        await settle(protocol.input_closed)

    async def test_fake_opencode_keeps_session_and_emits_scripted_tool_calls(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = install_fake_opencode(root / 'fake')
            (binary.parent / 'scenario.json').write_text(json.dumps({'tool_calls': TOOL_UPDATES}))
            env = environment(root / 'runtime', os.environ)
            config = Path(env['XDG_CONFIG_HOME']) / 'opencode'
            config.mkdir()
            (config / 'opencode.json').write_text('{}')
            servers = [{'name': 'torii', 'command': '/python', 'args': [], 'env': []}]
            writes = [request(1, 'initialize', INITIALIZE),
                      request(2, 'session/new', {'cwd': str(root), 'mcpServers': servers})]
            for number in (3, 4):
                writes.append(request(number, 'session/prompt',
                              {'sessionId': SESSION_ID, 'prompt': [{'type': 'text', 'text': str(number)}]}))
            process = await asyncio.create_subprocess_exec(sys.executable, str(binary), 'acp', env=env,
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            try:
                output, error = await settle(process.communicate(
                    ''.join(json.dumps(event) + '\n' for event in writes).encode()))
            finally:
                if process.returncode is None:
                    process.kill()
                    await process.wait()
            self.assertEqual(process.returncode, 0, error.decode())
            calls = [json.loads(raw)['message'] for raw in (binary.parent / 'calls.jsonl').read_text().splitlines()]
            self.assertEqual(calls, writes)
            events = [json.loads(raw) for raw in output.splitlines()]
            tools = [event['params']['update'] for event in events
                     if event.get('params', {}).get('update', {}).get('sessionUpdate') in ('tool_call', 'tool_call_update')]
            self.assertEqual(tools, TOOL_UPDATES * 2)
            self.assertEqual([event['id'] for event in events
                              if (event.get('result') or {}).get('stopReason') == 'end_turn'], [3, 4])


if __name__ == '__main__':
    unittest.main()
