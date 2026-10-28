"""MSP billing, private transport, terminal and replay contracts with offline Muse."""

import asyncio
from copy import deepcopy
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
import uuid
from unittest.mock import patch

from coordinator.host import HostClient
from coordinator.log import running_commit
from coordinator.msp_protocol import MspProtocol, MspRequestError, STABLE_FINGERPRINT, classify_error, uuid7
from coordinator.native_protocol import RunControl
from tests.fake_muse import EXPERIMENTAL_FINGERPRINT, FRAMES, MuseServer, SESSION, TURN, notification
from tests.support import settle, stop_test_hosts, until


MODEL = 'muse-wonder-1.0'


class MemoryStdin:
    def __init__(self, process):
        self.process = process
        self.buffer = b''
        self.closed = asyncio.Event()

    def write(self, data):
        self.buffer += data

    async def drain(self):
        while b'\n' in self.buffer:
            raw, self.buffer = self.buffer.split(b'\n', 1)
            event = json.loads(raw)
            self.process.writes.append(event)
            self.process.receive(event)

    async def private(self, event, secrets=None):
        self.process.private.append(event)
        self.process.receive(event)

    def close(self):
        self.closed.set()

    async def wait_closed(self):
        await self.closed.wait()


class MemoryHost:
    def __init__(self, home, scenario):
        self.server = MuseServer(home, scenario)
        self.writes = []
        self.events = []
        self.private = []
        self.stdin = MemoryStdin(self)
        self.protocol = None

    def written_lines(self):
        return [json.dumps(event) for event in self.writes]

    def past_request(self, request_id):
        return next((event for event in self.writes if event.get('id') == request_id), None)

    def past_response(self, request_id):
        return next((event for event in self.events if event.get('id') == request_id and 'method' not in event), None)

    def history_after(self, seq):
        return (json.dumps(event) for event in self.events[seq:])

    def emit(self, event):
        self.events.append(deepcopy(event))
        return self.protocol.handle(event)

    def receive(self, event):
        for response in self.server.receive(event):
            self.emit(response)


class MspProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='torii-msp-')
        self.root = Path(self.temporary.name)
        self.addCleanup(self.temporary.cleanup)
        self.addCleanup(stop_test_hosts, self.root)
        self.protocols = []
        self.tasks = []
        self.problems = []
        self.observed = []

    async def asyncTearDown(self):
        for protocol in self.protocols:
            protocol.disconnect()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await asyncio.sleep(0)

    def observe(self, value):
        self.observed.append(value)
        return value

    def make(self, scenario=None, host=None, reattaching=False, **options):
        host = host or MemoryHost(self.root, scenario or {})
        protocol = MspProtocol(host, RunControl(receipt_timeout=.3), None, self.observe,
                               account_home=self.root, problem=lambda *args: self.problems.append(args),
                               reattaching=reattaching, **options)
        if isinstance(host, MemoryHost):
            host.protocol = protocol
        self.protocols.append(protocol)
        return protocol, host

    async def start(self, scenario=None, **options):
        protocol, host = self.make(scenario, **options)
        await settle(protocol.start('fixture prompt', self.root, True, MODEL))
        return protocol, host

    async def done(self, protocol):
        await until(lambda: protocol.terminal, 'MSP terminal')
        await settle(protocol.input_closed)

    def methods(self, host):
        return [event.get('method') for event in host.writes + host.private]

    async def test_wrong_fingerprint_stops_before_account_and_session(self):
        protocol, host = await self.start({'fingerprint': 'sha256:' + '0' * 64})
        await self.done(protocol)
        self.assertEqual(protocol.failure, 'protocol_mismatch')
        self.assertEqual(self.methods(host), ['initialize'])

    async def test_experimental_fingerprint_is_refused(self):
        protocol, host = await self.start({'fingerprint': EXPERIMENTAL_FINGERPRINT})
        await self.done(protocol)
        self.assertEqual(protocol.failure, 'protocol_mismatch')
        self.assertEqual(self.methods(host), ['initialize'])

    async def test_response_requires_experimental_flag(self):
        protocol, host = await self.start({'experimental_api': False})
        await self.done(protocol)
        self.assertEqual(protocol.failure, 'protocol_mismatch')
        self.assertEqual(self.methods(host), ['initialize'])

    async def test_session_mcp_grant_is_required(self):
        protocol, host = await self.start({'grants': []})
        await self.done(protocol)
        self.assertEqual(protocol.failure, 'protocol_mismatch')
        self.assertEqual(self.methods(host), ['initialize'])

    async def test_only_account_login_lane_can_start_a_session(self):
        for lane in ('apiKey', 'envKey', 'x', None):
            with self.subTest(lane=lane):
                protocol, host = await self.start({'lane': lane})
                await self.done(protocol)
                self.assertEqual(protocol.failure, 'billing_lane')
                self.assertNotIn('session/start', self.methods(host))
                self.assertNotIn('turn/start', self.methods(host))
                self.assertTrue(any(problem[:2] == ('muse', 'billing-lane') for problem in self.problems))

    async def test_malformed_grants_are_protocol_mismatch(self):
        for grants in (None, 'sessionMcp', 7):
            protocol, host = await self.start({'grants': grants})
            await self.done(protocol)
            self.assertEqual(protocol.failure, 'protocol_mismatch')
            self.assertEqual(self.methods(host), ['initialize'])

    async def test_logged_out_requires_login(self):
        protocol, host = await self.start({'lane': 'loggedOut'})
        await self.done(protocol)
        self.assertEqual(protocol.failure, 'login_required')
        self.assertNotIn('session/start', self.methods(host))

    async def test_muse_home_must_be_inside_account_home(self):
        outside = self.root.with_name(self.root.name + '-other')
        link = self.root / 'escaped'
        link.symlink_to(outside)
        for home in (str(outside), str(link / 'muse'), 'relative/muse', ''):
            with self.subTest(home=home):
                protocol, host = await self.start({'muse_home': home})
                await self.done(protocol)
                self.assertEqual(protocol.failure, 'protocol_mismatch')
                self.assertEqual(self.methods(host), ['initialize'])

    async def test_initialize_and_private_open_have_schema_parameters(self):
        protocol, host = await self.start()
        await self.done(protocol)
        initialize = host.writes[0]
        self.assertEqual(initialize['params']['clientInfo'], {'name': 'torii', 'version': running_commit(Path(__file__).resolve().parents[1])})
        self.assertEqual(initialize['params']['capabilities'], {
            'experimentalApi': True, 'userInputDialogs': False, 'requestedCapabilities': ['sessionMcp']})
        self.assertEqual(host.writes[1]['method'], 'initialized')
        self.assertEqual(len(host.private), 1)
        opened = host.private[0]
        self.assertEqual(opened['id'], 'torii-open')
        self.assertEqual(opened['params']['workspaceRoot'], str(self.root))
        self.assertEqual(opened['params']['approvalMode'], 'allowAll')
        self.assertEqual(opened['params']['modelId'], MODEL)
        self.assertNotIn('config', opened['params'])
        self.assertEqual(uuid.UUID(opened['params']['commandId']).version, 7)
        self.assertNotIn('session/start', [event.get('method') for event in host.writes])

    async def test_session_id_uses_session_id_field(self):
        protocol, host = await self.start()
        await self.done(protocol)
        self.assertEqual(protocol.session_id, SESSION)
        self.assertEqual(self.observed, [SESSION])
        self.observed.clear()
        protocol, host = await self.start({'legacy_session_id': True})
        await self.done(protocol)
        self.assertIsNone(protocol.session_id)
        self.assertEqual(self.observed, [])
        self.assertEqual(protocol.failure, 'start_failed')
        self.assertNotIn('turn/start', self.methods(host))

    async def test_open_errors_are_bounded_and_model_data_is_classified(self):
        for data, expected in (({'kind': 'invalidParams'}, 'start_failed'),
                               ({'kind': 'invalidParams', 'details': {'modelId': MODEL}}, 'model_unavailable')):
            with self.subTest(data=data):
                protocol, host = await self.start({'open_error': {'code': -32602, 'message': 'x' * 10000, 'data': data}})
                await self.done(protocol)
                self.assertEqual(protocol.failure, expected)
                self.assertLessEqual(len(protocol.detail), 500)
                self.assertNotIn('turn/start', self.methods(host))

    async def test_turn_ack_is_not_completion(self):
        protocol, host = await self.start({'hold': True})
        self.assertEqual(protocol.turn_id, TURN)
        self.assertFalse(protocol.terminal)
        self.assertIsNone(protocol.outcome)
        self.assertFalse(host.stdin.closed.is_set())
        host.emit(notification('turn/completed'))
        await self.done(protocol)
        self.assertEqual(protocol.outcome, 'succeeded')

    async def test_acknowledged_turn_id_is_authoritative(self):
        actual = '01900000-0000-7000-8000-000000000077'
        protocol, host = await self.start({'hold': True, 'turn_id': actual})
        self.assertEqual(protocol.turn_id, actual)
        completed = notification('turn/completed')
        host.emit(completed)
        self.assertFalse(protocol.terminal)
        completed['params']['turnId'] = actual
        host.emit(completed)
        await self.done(protocol)
        self.assertEqual(protocol.outcome, 'succeeded')

    async def test_completion_for_another_turn_is_ignored(self):
        protocol, host = await self.start({'hold': True})
        completed = notification('turn/completed')
        completed['params']['turnId'] = '01900000-0000-7000-8000-000000000099'
        host.emit(completed)
        self.assertFalse(protocol.terminal)
        completed['params']['turnId'] = TURN
        completed['params']['sessionId'] = 'another-session'
        host.emit(completed)
        self.assertFalse(protocol.terminal)
        host.emit(notification('turn/completed'))
        await self.done(protocol)
        self.assertEqual(protocol.outcome, 'succeeded')

    async def test_turn_always_sends_queue_and_requested_effort(self):
        for effort in ('low', 'medium', 'high', 'max'):
            protocol, host = self.make()
            await settle(protocol.start('payload', self.root, True, MODEL, thread_options={'effort': effort}))
            await self.done(protocol)
            turn = next(event for event in host.writes if event.get('method') == 'turn/start')
            self.assertEqual(turn['params']['ifBusy'], 'queue')
            self.assertEqual(turn['params']['reasoningEffort'], effort)
            self.assertEqual(turn['params']['input'], [{'type': 'text', 'text': 'payload'}])
            self.assertEqual(turn['params']['sessionId'], SESSION)
            self.assertEqual(uuid.UUID(turn['params']['commandId']).version, 7)

    async def test_queued_disposition_fails(self):
        protocol, host = await self.start({'hold': True, 'disposition': 'queued'})
        self.assertTrue(protocol.terminal)
        await self.done(protocol)
        self.assertEqual(protocol.failure, 'execution_failed')
        self.assertNotEqual(protocol.outcome, 'succeeded')

    async def test_quota_is_structured_and_reset_is_seconds(self):
        protocol, host = await self.start({'terminal': 'failed', 'error': {
            'kind': 'modelError', 'message': 'limited', 'retryable': False,
            'quota': {'limitKind': 'tierLimit', 'resetsAtMs': 1900000000123}}})
        await self.done(protocol)
        self.assertEqual(protocol.failure, 'quota_limited')
        self.assertEqual(protocol.resets_at, 1900000000.123)
        protocol, host = await self.start({'terminal': 'failed', 'error': {
            'kind': 'modelError', 'message': 'quota', 'retryable': False}})
        await self.done(protocol)
        self.assertEqual(protocol.failure, 'execution_failed')
        self.assertIsNone(protocol.resets_at)

    async def test_auth_required_and_bounded_error_detail(self):
        self.assertEqual(classify_error({'kind': 'authRequired', 'message': 'sign in'})['code'], 'login_required')
        value = classify_error({'kind': 'futureError', 'message': 'x' * 10000})
        self.assertEqual(value['code'], 'execution_failed')
        self.assertIn('futureError', value['detail'])
        self.assertLessEqual(len(value['detail']), 500)

    async def test_text_uses_agent_items_and_stays_bounded(self):
        protocol, host = await self.start({'hold': True}, text_limit=8)
        host.emit(notification('item/started'))
        delta = notification('item/delta')
        delta['params']['delta'] = 'abcdefghijklmnop'
        parsed = host.emit(delta)
        self.assertEqual(parsed['item']['text'], 'ijklmnop')
        completed = notification('item/completed')
        completed['params']['item']['text'] = 'final answer'
        host.emit(completed)
        self.assertEqual(protocol.text, 'l answer')
        completed['params']['item']['kind'] = 'reasoning'
        completed['params']['item']['text'] = 'private thought'
        host.emit(completed)
        self.assertEqual(protocol.text, 'l answer')
        delta['params']['itemId'] = 'unknown-item'
        host.emit(delta)
        self.assertEqual(protocol.text, 'l answer')

    async def test_spend_counts_turn_once_and_records_served_model(self):
        protocol, host = await self.start({'served_model': 'other-model'})
        await self.done(protocol)
        self.assertEqual(protocol.spend['input_tokens'], 12)
        self.assertEqual(protocol.spend['output_tokens'], 5)
        self.assertEqual(protocol.spend['cached_tokens'], 3)
        self.assertEqual(protocol.spend['models'][TURN], 'other-model')
        self.assertEqual(sum(problem[:2] == ('muse', 'model-mismatch') for problem in self.problems), 1)
        self.problems.clear()
        protocol, host = await self.start({'served_model': None})
        await self.done(protocol)
        self.assertEqual(self.problems, [])
        self.assertIsNone(protocol.spend['models'][TURN])

    async def test_terminal_usage_without_session_usage_is_counted(self):
        protocol, host = await self.start({'hold': True})
        host.emit(notification('turn/completed'))
        await self.done(protocol)
        self.assertEqual(protocol.spend['input_tokens'], 12)

    def model_call_frames(self):
        frames = []
        for cursor, tokens, cached, reasoning, prompt, total in (
                ('fixture-call-2', 10, 3, 1, 10, 11), ('fixture-call-10', 20, 4, 2, 30, 32)):
            frame = notification('session/tokenUsage')
            frame['params'].update(viewCursor=cursor, promptTokens=tokens, totalTokens=tokens + 1,
                                   cumulative={'promptTokens': prompt, 'totalTokens': total},
                                   usage={'inputTokens': tokens, 'outputTokens': 1,
                                          'cachedTokens': cached, 'reasoningTokens': reasoning})
            frames.append(frame)
        return frames

    async def test_spend_sums_model_calls_without_terminal_usage(self):
        protocol, host = await self.start({'hold': True})
        for frame in self.model_call_frames():
            host.emit(frame)
        completed = notification('turn/completed')
        del completed['params']['usage']
        host.emit(completed)
        await self.done(protocol)
        self.assertEqual(protocol.outcome, 'succeeded')
        self.assertEqual(protocol.spend, {'input_tokens': 30, 'output_tokens': 2,
                                        'cached_tokens': 7, 'reasoning_tokens': 3, 'prompt_tokens': 30, 'total_tokens': 32,
                                        'models': {TURN: MODEL}})

    async def test_spend_reattach_replays_model_calls_without_double_counting(self):
        protocol, host = await self.start({'hold': True})
        for frame in self.model_call_frames():
            host.emit(frame)
        completed = notification('turn/completed')
        del completed['params']['usage']
        host.emit(completed)
        await self.done(protocol)
        protocol.disconnect()
        before = deepcopy(host.writes)
        replacement, host = self.make(host=host, reattaching=True)
        await settle(replacement.start('fixture prompt', self.root, True, MODEL))
        await self.done(replacement)
        self.assertEqual(replacement.spend, {'input_tokens': 30, 'output_tokens': 2,
                                           'cached_tokens': 7, 'reasoning_tokens': 3, 'prompt_tokens': 30, 'total_tokens': 32,
                                           'models': {TURN: MODEL}})
        for raw in host.history_after(0):
            replacement.handle(raw, replay=True)
        self.assertEqual(replacement.spend, {'input_tokens': 30, 'output_tokens': 2,
                                           'cached_tokens': 7, 'reasoning_tokens': 3, 'prompt_tokens': 30, 'total_tokens': 32,
                                           'models': {TURN: MODEL}})
        self.assertEqual(replacement.outcome, 'succeeded')
        self.assertEqual(host.writes, before)

    async def test_terminal_usage_replaces_model_call_sum_during_replay(self):
        protocol, host = await self.start({'hold': True})
        for frame in self.model_call_frames():
            host.emit(frame)
        completed = notification('turn/completed')
        completed['params']['usage'] = {'inputTokens': 40, 'outputTokens': 5,
                                        'cachedTokens': 9, 'reasoningTokens': 4}
        host.emit(completed)
        await self.done(protocol)
        self.assertEqual(protocol.spend, {'input_tokens': 40, 'output_tokens': 5,
                                        'cached_tokens': 9, 'reasoning_tokens': 4, 'prompt_tokens': 30, 'total_tokens': 32,
                                        'models': {TURN: MODEL}})
        for raw in host.history_after(0):
            protocol.handle(raw, replay=True)
        self.assertEqual(protocol.spend, {'input_tokens': 40, 'output_tokens': 5,
                                        'cached_tokens': 9, 'reasoning_tokens': 4, 'prompt_tokens': 30, 'total_tokens': 32,
                                        'models': {TURN: MODEL}})

    async def test_usage_without_cursor_is_skipped_and_turn_can_complete(self):
        protocol, host = await self.start({'hold': True})
        frame = notification('session/tokenUsage')
        del frame['params']['viewCursor']
        host.emit(frame)
        self.assertFalse(protocol.terminal)
        self.assertIsNone(protocol.spend)
        host.emit(notification('turn/completed'))
        await self.done(protocol)
        self.assertEqual(protocol.outcome, 'succeeded')

    async def test_empty_or_partial_terminal_usage_keeps_model_call_sum(self):
        for usage in ({}, {'inputTokens': 1}):
            protocol, host = await self.start({'hold': True})
            for frame in self.model_call_frames():
                host.emit(frame)
            completed = notification('turn/completed')
            completed['params']['usage'] = usage
            host.emit(completed)
            await self.done(protocol)
            self.assertEqual(protocol.spend['input_tokens'], 30)
            self.assertEqual(protocol.spend['output_tokens'], 2)

    async def test_other_turn_usage_never_counts_or_records_model_mismatch(self):
        protocol, host = await self.start({'hold': True})
        frame = notification('session/tokenUsage')
        frame['params'].update(turnId='another-turn', modelId='another-model')
        host.emit(frame)
        self.assertIsNone(protocol.spend)
        self.assertEqual(self.problems, [])
        host.emit(notification('turn/completed'))
        await self.done(protocol)

    async def test_prompt_and_total_tokens_follow_schema_and_deduplicate_replay(self):
        protocol, host = await self.start({'hold': True})
        frames = self.model_call_frames()
        for index, frame in enumerate(frames):
            frame['params'].update(promptTokens=50 + index, totalTokens=60 + index)
            frame['params'].pop('cumulative')
            host.emit(frame)
        for frame in frames:
            host.emit(frame)
        self.assertEqual(protocol.spend['prompt_tokens'], 101)
        self.assertEqual(protocol.spend['total_tokens'], 121)
        completed = notification('turn/completed')
        completed['params'].pop('usage')
        host.emit(completed)
        await self.done(protocol)

    async def test_view_gap_reconciles_cumulative_prompt_total_and_output(self):
        protocol, host = await self.start({'hold': True})
        first, last = self.model_call_frames()
        host.emit(first)
        host.emit({'jsonrpc': '2.0', 'method': 'view/gap', 'params': {'sessionId': SESSION}})
        last['params']['cumulative'] = {'promptTokens': 100, 'outputTokens': 10, 'totalTokens': 110}
        host.emit(last)
        host.emit(first)
        self.assertEqual(protocol.spend['prompt_tokens'], 100)
        self.assertEqual(protocol.spend['total_tokens'], 110)
        self.assertEqual(protocol.spend['output_tokens'], 10)
        completed = notification('turn/completed')
        completed['params'].pop('usage')
        host.emit(completed)
        await self.done(protocol)
        self.assertEqual(protocol.spend['total_tokens'], 110)

    async def test_invalid_session_observer_finishes_without_hanging(self):
        def invalid(value):
            raise RuntimeError('invalid native UUID')
        protocol, host = self.make()
        protocol.observe_session = invalid
        await protocol.start('fixture prompt', self.root, True, MODEL)
        await self.done(protocol)
        self.assertEqual(protocol.failure, 'execution_failed')
        self.assertNotIn('turn/start', self.methods(host))

    async def test_model_terminal_errors_only_map_when_the_requested_model_is_named(self):
        for kind in ('modelError', 'configError'):
            for message, expected in ((MODEL + ' unavailable', 'model_unavailable'),
                                      ('unrelated tool configuration', 'execution_failed')):
                protocol, host = await self.start({'terminal': 'failed', 'error': {'kind': kind, 'message': message}})
                await self.done(protocol)
                self.assertEqual(protocol.failure, expected)
                self.assertIn(message, protocol.detail)

    async def test_answered_server_request_is_not_answered_twice(self):
        protocol, host = await self.start({'hold': True, 'server_request': True})
        await until(lambda: any(event.get('id') == 'fixture-approval' for event in host.writes), 'first rejection')
        request = next(event for event in host.events if event.get('id') == 'fixture-approval')
        before = deepcopy(host.writes)
        host.emit(deepcopy(request))
        await asyncio.sleep(.01)
        self.assertEqual(host.writes, before)
        self.assertEqual(sum(problem[:2] == ('muse', 'unexpected-request') for problem in self.problems), 1)
        another = deepcopy(request)
        another['id'] = 'fixture-approval-2'
        host.emit(another)
        await until(lambda: any(event.get('id') == 'fixture-approval-2' for event in host.writes), 'second rejection')
        replies = [event for event in host.writes if event.get('id') in ('fixture-approval', 'fixture-approval-2')]
        self.assertEqual([event['error']['code'] for event in replies], [-32601, -32601])
        self.assertEqual(sum(problem[:2] == ('muse', 'unexpected-request') for problem in self.problems), 2)

    async def test_replayed_request_error_is_ignored_but_live_error_fails(self):
        protocol, host = await self.start({'hold': True})
        with self.assertRaises(MspRequestError):
            await protocol.send('usage/read', request_id='fixture-rejected-usage')
        self.assertFalse(protocol.terminal)
        error = next(event for event in host.events if event.get('id') == 'fixture-rejected-usage')
        protocol.disconnect()
        replacement, host = self.make(host=host, reattaching=True)
        await settle(replacement.start('fixture prompt', self.root, True, MODEL))
        self.assertFalse(replacement.terminal)
        self.assertIsNone(replacement.failure)
        self.assertEqual(replacement.turn_id, TURN)
        host.emit(deepcopy(error))
        await self.done(replacement)
        self.assertEqual(replacement.failure, 'execution_failed')

    async def test_server_request_gets_error_and_problem(self):
        protocol, host = await self.start({'hold': True, 'server_request': True})
        await until(lambda: any(event.get('id') == 'fixture-approval' for event in host.writes), 'request rejection')
        reply = next(event for event in host.writes if event.get('id') == 'fixture-approval')
        self.assertIn('error', reply)
        self.assertNotIn('result', reply)
        self.assertTrue(any(problem[:2] == ('muse', 'unexpected-request') for problem in self.problems))
        self.assertFalse(protocol.terminal)

    async def test_protocol_errors_oversize_and_unknown_terminal_fail(self):
        frames = [
            {'jsonrpc': '2.0', 'id': None, 'error': {'code': -32700, 'message': 'parse error'}},
            {'jsonrpc': '2.0', 'id': 'unknown', 'error': {'code': -32603, 'message': 'internal'}},
            {'jsonrpc': '2.0', 'method': 'item/delta', 'params': {'delta': 'x' * (10 * 1024 * 1024)}},
            b'x' * (10 * 1024 * 1024 + 1),
            {'jsonrpc': '2.0', 'method': 'turn/completed', 'params': {'sessionId': SESSION, 'turnId': TURN, 'terminal': 'unknown'}},
            {'jsonrpc': '2.0', 'method': 'turn/completed', 'params': {'sessionId': SESSION, 'turnId': TURN, 'terminal': 'failed'}},
            {'jsonrpc': '2.0', 'id': [], 'result': {}},
        ]
        for frame in frames:
            with self.subTest(shape=type(frame).__name__):
                protocol, host = await self.start({'hold': True})
                protocol.handle(frame)
                await self.done(protocol)
                self.assertEqual(protocol.failure, 'execution_failed')

    async def test_cancel_missing_run_retries_once_on_turn_frame(self):
        protocol, host = await self.start({'hold': True, 'cancel_race': True})
        await settle(protocol.cancel())
        await self.done(protocol)
        interrupts = [event for event in host.writes if event.get('method') == 'turn/interrupt']
        self.assertEqual(len(interrupts), 2)
        self.assertNotEqual(interrupts[0]['params']['commandId'], interrupts[1]['params']['commandId'])
        self.assertEqual(protocol.failure, 'owner_stopped')
        self.assertEqual(protocol.outcome, 'stopped')
        self.assertNotIn('turn/cancel', self.methods(host))

    async def test_cancel_missing_run_waits_for_next_matching_turn_frame(self):
        protocol, host = await self.start({'hold': True, 'cancel_race': True})
        receive = host.server.receive
        deferred = []
        def delayed(event):
            frames = receive(event)
            if event.get('method') == 'turn/interrupt' and host.server.interrupts == 1:
                deferred.extend(frame for frame in frames if frame.get('method') == 'turn/started')
                return [frame for frame in frames if frame.get('method') != 'turn/started']
            return frames
        with patch.object(host.server, 'receive', side_effect=delayed):
            stopping = protocol.cancel()
            await until(lambda: len(deferred) == 1, 'missing_run response')
            await asyncio.sleep(.01)
            self.assertEqual(sum(event.get('method') == 'turn/interrupt' for event in host.writes), 1)
            unrelated = deepcopy(deferred[0])
            unrelated['params']['turnId'] = 'fixture-other-turn'
            host.emit(unrelated)
            await asyncio.sleep(.01)
            self.assertEqual(sum(event.get('method') == 'turn/interrupt' for event in host.writes), 1)
            self.assertFalse(protocol.terminal)
            host.emit(deferred[0])
            await settle(stopping)
        await self.done(protocol)
        self.assertEqual(sum(event.get('method') == 'turn/interrupt' for event in host.writes), 2)
        self.assertEqual(protocol.failure, 'owner_stopped')

    async def test_unicode_frame_limit_counts_utf8_bytes(self):
        protocol, host = await self.start({'hold': True})
        frame = json.dumps({'jsonrpc': '2.0', 'method': 'item/delta',
                            'params': {'delta': '\u754c' * (4 * 1024 * 1024)}}, ensure_ascii=False)
        self.assertLess(len(frame), 10 * 1024 * 1024)
        self.assertGreater(len(frame.encode()), 10 * 1024 * 1024)
        protocol.handle(frame)
        self.assertTrue(protocol.terminal)
        await self.done(protocol)
        self.assertEqual(protocol.failure, 'execution_failed')

    async def test_cancel_escalates_and_waits_for_terminal_or_host_stop(self):
        protocol, host = await self.start({'hold': True, 'ignore_interrupt': True}, cancel_grace=.02)
        await settle(protocol.cancel())
        await self.done(protocol)
        self.assertIn('turn/cancel', self.methods(host))
        self.assertEqual(protocol.failure, 'owner_stopped')
        protocol, host = await self.start({'hold': True, 'ignore_cancel': True}, cancel_grace=.02)
        await settle(protocol.cancel())
        self.assertFalse(protocol.terminal)
        protocol.disconnect()
        self.assertTrue(protocol.terminal)
        self.assertEqual(protocol.failure, 'owner_stopped')

    async def test_cancel_deadline_applies_while_interrupt_ack_is_missing(self):
        protocol, host = await self.start({'hold': True, 'drop_interrupt_ack': True}, cancel_grace=.02)
        await asyncio.wait_for(protocol.cancel(), .2)
        await self.done(protocol)
        self.assertEqual(protocol.failure, 'owner_stopped')
        self.assertIn('turn/cancel', self.methods(host))

    async def test_external_cancel_is_not_owner_stopped(self):
        protocol, host = await self.start({'terminal': 'cancelled'})
        await self.done(protocol)
        self.assertEqual(protocol.failure, 'cancelled')
        self.assertEqual(protocol.outcome, 'failed')

    async def test_terminal_stops_all_tasks_then_closes_stdin(self):
        protocol, host = await self.start()
        await self.done(protocol)
        self.assertEqual(host.writes[-1]['method'], 'task/stopAll')
        self.assertEqual(host.writes[-1]['params']['sessionId'], SESSION)
        self.assertTrue(host.stdin.closed.is_set())

    async def test_method_allowlist_refuses_unsafe_methods(self):
        protocol, host = self.make()
        for method in ('account/logout', 'settings/write', 'session/setModel', 'task/background', 'goal/create', 'plugin/read'):
            with self.subTest(method=method), self.assertRaises(ValueError):
                await protocol.send(method)
        self.assertEqual(host.writes, [])
        self.assertEqual(host.private, [])

    async def test_reattach_after_ack_does_not_resend_turn(self):
        protocol, host = await self.start({'hold': True})
        protocol.disconnect()
        before = deepcopy(host.writes)
        replacement, host = self.make(host=host, reattaching=True)
        await settle(replacement.start('fixture prompt', self.root, True, MODEL))
        self.assertEqual(host.writes, before)
        self.assertEqual(replacement.session_id, SESSION)
        self.assertEqual(replacement.turn_id, TURN)
        self.assertFalse(replacement.terminal)
        host.emit(notification('turn/completed'))
        await self.done(replacement)
        self.assertEqual(sum(event.get('method') == 'turn/start' for event in host.writes), 1)

    async def test_reattach_before_ack_does_not_resend_journaled_turn(self):
        protocol, host = await self.start({'hold': True})
        ack = deepcopy(next(event for event in host.events if event.get('id') == 'torii-turn'))
        protocol.disconnect()
        host.events = [event for event in host.events if event.get('id') != 'torii-turn']
        before = deepcopy(host.writes)
        replacement, host = self.make(host=host, reattaching=True)
        await settle(replacement.start('fixture prompt', self.root, True, MODEL))
        self.assertEqual(host.writes, before)
        self.assertFalse(replacement.terminal)
        host.emit(ack)
        host.emit(notification('turn/completed'))
        await self.done(replacement)
        self.assertEqual(replacement.outcome, 'succeeded')
        self.assertEqual(sum(event.get('method') == 'turn/start' for event in host.writes), 1)

    async def test_reattach_uses_saved_client_stamp(self):
        protocol, host = await self.start({'hold': True})
        protocol.disconnect()
        before = deepcopy(host.writes)
        replacement, host = self.make(host=host, reattaching=True)
        with patch('coordinator.msp_protocol.running_commit', return_value='next-stamp'):
            await settle(replacement.start('fixture prompt', self.root, True, MODEL))
        self.assertEqual(host.writes, before)
        self.assertEqual(replacement.turn_id, TURN)
        self.assertFalse(replacement.terminal)

    async def test_reattach_recovers_owner_stop_without_repeating_commands(self):
        protocol, host = await self.start({'hold': True, 'ignore_cancel': True}, cancel_grace=.02)
        await settle(protocol.cancel())
        self.assertFalse(protocol.terminal)
        protocol.disconnect()
        before = deepcopy(host.writes)
        replacement, host = self.make(host=host, reattaching=True, cancel_grace=.02)
        await settle(replacement.start('fixture prompt', self.root, True, MODEL))
        self.assertFalse(replacement.terminal)
        await asyncio.sleep(.05)
        self.assertEqual(host.writes, before)
        completed = notification('turn/completed')
        completed['params']['terminal'] = 'cancelled'
        host.emit(completed)
        await self.done(replacement)
        self.assertEqual(replacement.outcome, 'stopped')
        self.assertEqual(replacement.failure, 'owner_stopped')

    async def test_reattach_rejects_changed_payload_without_sending_it(self):
        protocol, host = await self.start({'hold': True})
        protocol.disconnect()
        before = deepcopy(host.writes)
        replacement, host = self.make(host=host, reattaching=True)
        await settle(replacement.start('different prompt', self.root, True, MODEL))
        await self.done(replacement)
        self.assertEqual(replacement.failure, 'execution_failed')
        self.assertEqual(sum(event.get('method') == 'turn/start' for event in host.writes), 1)
        self.assertEqual([event for event in host.writes if event.get('method') != 'task/stopAll'], before)

    async def test_reattach_with_unacknowledged_private_open_fails_closed(self):
        protocol, host = await self.start({'hold': True})
        protocol.disconnect()
        host.events = [event for event in host.events if event.get('id') != 'torii-open']
        replacement, host = self.make(host=host, reattaching=True)
        await settle(replacement.start('fixture prompt', self.root, True, MODEL))
        await self.done(replacement)
        self.assertEqual(replacement.failure, 'start_failed')
        self.assertEqual(len(host.private), 1)

    async def test_reattach_replays_text_and_terminal_without_resending(self):
        protocol, host = await self.start()
        await self.done(protocol)
        before = deepcopy(host.writes)
        replacement, host = self.make(host=host, reattaching=True)
        await settle(replacement.start('fixture prompt', self.root, True, MODEL))
        await self.done(replacement)
        self.assertEqual(replacement.text, 'fixture answer')
        self.assertEqual(replacement.outcome, 'succeeded')
        self.assertEqual(host.writes, before)

    async def host_protocol(self, scenario, process=None):
        if process is None:
            scenario_path = self.root / 'scenario.json'
            scenario_path.write_text(json.dumps(scenario))
            spec = {'argv': [sys.executable, str(Path(__file__).with_name('fake_muse.py')),
                              '--home', str(self.root), '--scenario', str(scenario_path),
                              '--calls', str(self.root / 'calls.jsonl')],
                    'cwd': str(self.root), 'env': {'HOME': str(self.root), 'PATH': os.environ['PATH']},
                    'provider': 'muse', 'locks': []}
            process = await HostClient.launch(self.root / 'state/hosts/msp', spec)
            attach = False
        else:
            process = HostClient.attach(process.directory)
            await process.connect()
            attach = True
        protocol, process = self.make(host=process, reattaching=attach)
        async def read():
            while True:
                raw = await process.next_event()
                if raw is None:
                    protocol.disconnect()
                    return
                protocol.handle(json.loads(raw))
                process.ack()
        task = asyncio.create_task(read())
        self.tasks.append(task)
        await settle(protocol.start('fixture prompt', self.root, True, MODEL))
        return protocol, process, task

    async def test_host_private_open_response_is_matched_on_reattach(self):
        release = self.root / 'release'
        protocol, process, reader = await self.host_protocol({'hold': True, 'release': str(release)})
        await until(lambda: protocol.turn_id == TURN, 'host acknowledgement')
        self.assertNotIn('session/start', [json.loads(raw).get('method') for raw in process.written_lines()])
        self.assertIsNotNone(process.past_response('torii-open'))
        protocol.disconnect()
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)
        process.detach()
        replacement, attached, resumed_reader = await self.host_protocol({}, process)
        self.assertEqual(replacement.session_id, SESSION)
        await until(lambda: replacement.turn_id == TURN, 'replayed host acknowledgement')
        release.touch()
        await self.done(replacement)
        await settle(resumed_reader)
        await settle(attached.wait())
        calls = [json.loads(raw) for raw in (self.root / 'calls.jsonl').read_text().splitlines()]
        self.assertEqual(sum(frame.get('method') == 'session/start' for frame in calls), 1)
        self.assertEqual(sum(frame.get('method') == 'turn/start' for frame in calls), 1)
        self.assertEqual(replacement.outcome, 'succeeded')
        attached.detach()

    async def test_fake_host_cancel_race_and_cleanup(self):
        protocol, process, reader = await self.host_protocol({'hold': True, 'cancel_race': True})
        await until(lambda: protocol.turn_id == TURN, 'host turn')
        await settle(protocol.cancel())
        await self.done(protocol)
        await settle(reader)
        await settle(process.wait())
        self.assertEqual(protocol.failure, 'owner_stopped')
        calls = [json.loads(raw) for raw in (self.root / 'calls.jsonl').read_text().splitlines()]
        self.assertEqual(sum(frame.get('method') == 'turn/interrupt' for frame in calls), 2)
        process.detach()


class Uuid7Tests(unittest.TestCase):
    def test_uuid7_has_rfc_bits_and_rises_with_clock_rollback(self):
        with patch('coordinator.msp_protocol.time.time_ns', side_effect=[1900000000000000000] * 300 + [1800000000000000000] * 300):
            values = [uuid7() for _ in range(600)]
        self.assertEqual(values, sorted(set(values)))
        for value in values:
            parsed = uuid.UUID(value)
            self.assertEqual(parsed.version, 7)
            self.assertEqual(parsed.variant, uuid.RFC_4122)
            self.assertEqual(parsed.int >> 80, 1900000000000)

    def test_uuid7_preserves_variant_across_random_carry(self):
        import coordinator.msp_protocol as msp
        for random in ((1 << 62) - 1, (1 << 74) - 1):
            first = (1900000000000 << 80) | (7 << 76) | ((random >> 62) << 64) | (2 << 62) | (random & ((1 << 62) - 1))
            with patch.object(msp, '_uuid_last', first), patch('coordinator.msp_protocol.time.time_ns', return_value=1800000000000000000):
                value = uuid.UUID(uuid7())
            self.assertGreater(value.int, first)
            self.assertEqual(value.version, 7)
            self.assertEqual(value.variant, uuid.RFC_4122)

    def test_fixture_has_only_schema_shapes_and_placeholder_values(self):
        self.assertEqual(STABLE_FINGERPRINT, FRAMES[1]['result']['schema']['fingerprint'])
        self.assertEqual(next(frame['result']['session']['sessionId'] for frame in FRAMES if frame.get('id') == 'torii-open' and 'result' in frame), SESSION)
        for frame in FRAMES:
            self.assertEqual(frame['jsonrpc'], '2.0')
            if frame.get('method') == 'turn/start':
                self.assertEqual(frame['params']['ifBusy'], 'queue')
            self.assertNotIn('/Users/', json.dumps(frame))


if __name__ == '__main__':
    unittest.main()
