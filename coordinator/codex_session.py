"""A persistent ChatGPT parent on Codex's app-server protocol."""

import asyncio
import hashlib
import json
import logging
from pathlib import Path
import re
import shutil
import sys
import time
import uuid

from . import problems, parent_state
from .codex_accounts import CodexBroker
from .failures import internal_detail
from .native_protocol import NativeProtocol, RequestRejected, RunControl
from .policy import usage_policy
from .providers import ProviderRunner
from .session import AccountsUnavailable, message_uuid


logger = logging.getLogger(__name__)


def toml(value):
    if isinstance(value, dict):
        return '{' + ', '.join(json.dumps(key) + ' = ' + toml(item) for key, item in value.items()) + '}'
    return json.dumps(value)


def codex_binary(store, runner=None):
    default = runner.binaries['codex'] if runner else 'codex'
    return shutil.which(default)


def transcript_path(home, thread):
    if not home or not thread:
        return None
    return next(iter(sorted((Path(home) / 'sessions').glob('**/rollout-*-' + thread + '.jsonl'))), None)


def codex_transcript_receipts(path, rows):
    wanted = set(rows)
    found = set()
    with open(path, errors='replace') as stream:
        for line in stream:
            try:
                entry = json.loads(line)
            except ValueError:
                continue
            if not isinstance(entry, dict):
                continue
            payload = entry.get('payload') or {}
            if not isinstance(payload, dict):
                continue
            if entry.get('type') == 'response_item' and payload.get('role') == 'user':
                content = payload.get('content') or []
                text = '\n'.join(item.get('text', '') for item in content if isinstance(item, dict))
            elif entry.get('type') == 'event_msg' and payload.get('type') == 'user_message':
                text = payload.get('message') or ''
            else:
                continue
            if isinstance(text, str):
                found.update(int(row) for row in re.findall(r'\bmessage=(\d+) kind=', text) if int(row) in wanted)
    return found


class CodexCoordinatorSession:
    provider = 'codex'
    transcript_receipts = staticmethod(codex_transcript_receipts)
    _monitor = parent_state.monitor
    _track = parent_state.track
    restarted_prompt = staticmethod(parent_state.restarted_prompt)

    def __init__(self, store, runner, on_turn, receipt_timeout, key):
        self.store, self.runner, self.key = store, runner, key
        self.on_turn = on_turn
        self.control = RunControl(receipt_timeout)
        self.router = CodexBroker(store)
        self.session_id = None
        self.process = self.protocol = None
        self.host_id = self.log_path = self.transcript_path = None
        self.account_alias = self.model = self.usage_policy = None
        self.tasks = []
        self._monitor_task = self._stop_task = None
        self._stopping = self.closed = self.rejected = self.busy = self.turn_open = False
        self.stale = self.outdated = self.resume_failed = self.switched = False
        self._failure = None
        self._turn_rows, self._unreplayed, self._lost, self._relaunch_rows = set(), set(), set(), set()
        self._rows = {}
        self._unread_reattach_events = 0
        self._recovery_uncertain = False
        self.last_turn_at = 0
        self._replies = {}
        self._turn_messages = {}
        self._limits = {}
        self._rate_limited = False
        self._home = None

    @classmethod
    async def start(cls, store, runner_or_binaries, cwd, model, instructions, mcp_config=None,
                    on_turn=None, receipt_timeout=30, topic=None):
        runner = (runner_or_binaries if isinstance(runner_or_binaries, ProviderRunner)
                  else ProviderRunner(store.directory, binaries=runner_or_binaries))
        home = next((item['id'] for item in store.topics() if item['enabled'] and item['cwd'] == str(cwd)), None)
        main = store.get('coordinator_session_topic') or store.get('coordinator_home_topic') or home
        key = 'coordinator' if topic is None or topic == main else 'coordinator:' + topic
        session = cls(store, runner, on_turn, receipt_timeout, key)
        client_key = (str((runner.state_dir / 'hosts').resolve()), key)
        if client_key in parent_state._active_clients:
            raise BlockingIOError('Coordinator already has an attached client')
        binary = codex_binary(store, runner)
        if not binary:
            raise AccountsUnavailable(None, provider='codex', alias=session.router.active())
        selected_model = store.get('codex_model') or (
            runner.codex_model() if callable(runner.codex_model) else runner.codex_model) or 'gpt-6.1-sol'
        policy = usage_policy(store.directory)
        prompt = instructions + '\nCurrent usage policy:\n' + policy
        overrides = cls.overrides(store)
        from .mcp import tools_hash
        fingerprint = hashlib.sha256(json.dumps({'tools': tools_hash(), 'model': selected_model,
            'effort': 'xhigh', 'instructions': prompt, 'overrides': overrides, 'binary': binary},
            sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        session.usage_policy = policy
        session.session_id = store.get(key + '_session')
        if session.session_id and str(uuid.UUID(session.session_id)) != session.session_id:
            raise ValueError('Coordinator session ID must be a canonical UUID')
        saved_host = store.get(key + '_host')
        try:
            attached = await parent_state.attach(session, saved_host, fingerprint)
            if attached:
                session._home = (session.router.accounts().get(session.account_alias) or {}).get('config_dir')
                session._turn_rows = set(store.get(key + '_turn') or [])
                scope = None if key == 'coordinator' else topic
                session._rows = {message_uuid(row['id']): row['id'] for row in store.db.execute(
                    "SELECT id FROM messages WHERE (delivered IN ('sent','uncertain') OR "
                    "delivered='received' AND receipt='written') AND (? IS NULL OR topic=?)", (scope, scope))}
                session._rows.update({message_uuid(row): row for row in session._turn_rows})
            else:
                alias = session.router.parent_account()
                if alias is None:
                    raise AccountsUnavailable(session.router.account_state(session.router.active(), parent=True).get('until'),
                                              provider='codex', alias=session.router.active())
                if session.session_id and await runner._external_writer('codex', session.session_id):
                    raise RuntimeError('Session is already owned by an external native process')
                session.account_alias, session.model = alias, selected_model
                env = session.router.environment(alias)
                session._home = session.router.accounts()[alias]['config_dir']
                command = [binary, *overrides, 'app-server', '--listen', 'stdio://']
                locks = ['coordinator-' + key] + (['codex-' + session.session_id] if session.session_id else [])
                await parent_state.launch(session, {'argv': command, 'cwd': str(cwd), 'env': env,
                                          'provider': 'codex', 'locks': locks}, fingerprint)
            session.log_path = str(session.process.directory / 'events.jsonl')
            parent_state._active_clients.add(client_key)
            session.protocol = NativeProtocol('codex', session.process, session.control, session.session_id,
                                               session._observe_session, persistent=True)
            if attached:
                session._restore_state(attached['past'])
            session.tasks = [asyncio.create_task(session._read_stream()), session.protocol.input_closed]
            session._monitor_task = asyncio.create_task(session._monitor())
            if not attached:
                try:
                    await session.protocol.start('', cwd, not session.session_id, selected_model,
                                                 developer_instructions=prompt)
                except RequestRejected:
                    if not session.session_id:
                        raise
                    problems.record(store, 'coordinator', 'resume-lost', 'key=%s session=%s' %
                                    (key, session.session_id), level=logging.ERROR, topic=topic)
                    session.resume_failed = True
                    session.session_id = session.protocol.session_id = None
                    await session.protocol.start('', cwd, True, selected_model, developer_instructions=prompt)
            else:
                session.protocol.counter = session.process.max_request_id()
                session.control.bind(session.protocol.steer)
            session._find_transcript()
            lost = store.get(key + '_lost')
            if lost and attached:
                session.stale, session._lost = True, set(lost)
            elif lost:
                session._settle(lost)
            if session.movable() and session.idle():
                session.relaunch()
            return session
        except BaseException:
            parent_state._active_clients.discard(client_key)
            if session.protocol:
                session.protocol.disconnect()
            for task in [*session.tasks, session._monitor_task]:
                if task:
                    task.cancel()
            await asyncio.gather(*(task for task in [*session.tasks, session._monitor_task] if task),
                                 return_exceptions=True)
            if session.process:
                session.process.detach()
            raise

    @staticmethod
    def overrides(store):
        root = str(Path(__file__).resolve().parents[1])
        values = {'model_reasoning_effort': 'xhigh', 'mcp_servers.torii.command': sys.executable,
                  'mcp_servers.torii.args': ['-m', 'coordinator', '--state-dir', str(store.directory), 'mcp'],
                  'mcp_servers.torii.env': {'PYTHONPATH': root}, 'mcp_servers.torii.required': True,
                  'mcp_servers.torii.startup_timeout_sec': 30,
                  'mcp_servers.torii.default_tools_approval_mode': 'approve',
                  'mcp_servers.torii.omit_tools_from': ['deferred', 'code_mode']}
        return [part for key, value in values.items() for part in ('-c', key + '=' + toml(value))]

    def _observe_session(self, sid):
        if str(uuid.UUID(sid)) != sid or self.session_id and sid != self.session_id:
            raise RuntimeError('Provider changed the coordinator session ID')
        self.session_id = sid
        with self.store.db:
            self.store.put(self.key + '_session', sid)
            self.store.put(self.key + '_fresh', False)
        return sid

    def _find_transcript(self):
        thread = self.protocol.thread or {}
        path = thread.get('path') or thread.get('rolloutPath')
        self.transcript_path = path or transcript_path(self._home, self.session_id)

    def _event_consumed(self, seq):
        with self.store.db:
            self.store.put(self.key + '_host_seq', seq)

    def _restore_state(self, past):
        requests = {}
        for line in self.process.written_lines():
            request = json.loads(line)
            if request.get('method') not in ('turn/start', 'turn/steer'):
                continue
            wire = request['params'].get('clientUserMessageId')
            requests[wire] = request
        for wire, request in requests.items():
            row = self._rows.get(wire)
            if row is not None:
                response = self.process.past_response(request['id'])
                if response and 'result' in response:
                    self._confirm(wire)
                elif response and 'error' in response:
                    self.store.requeue_messages([row])
                    self._rows.pop(wire, None)
                    self._turn_rows.discard(row)
                else:
                    self._unreplayed.add(row)
        for line in past:
            raw_event = json.loads(line)
            event = self.protocol.handle(raw_event, replay=True)
            self._response_received(raw_event)
            if event is not None:
                self._apply_event(event, replay=True)
        if self.protocol.turn_id:
            self.busy = self.turn_open = True
        self._track(self._turn_rows)

    def _response_received(self, event):
        if 'id' not in event or 'result' not in event:
            return
        request = self.process.past_request(event['id'])
        if request and request.get('method') in ('turn/start', 'turn/steer'):
            params, result = request['params'], event['result']
            turn_id = ((result.get('turn') or {}).get('id') if request['method'] == 'turn/start'
                       else result.get('turnId'))
            if turn_id and (request['method'] == 'turn/start' or turn_id == params.get('expectedTurnId')):
                self._confirm(params.get('clientUserMessageId'), turn_id)

    def _confirm(self, wire, turn_id=None):
        row = self._rows.get(wire)
        if row is not None:
            if turn_id is not None:
                self._turn_messages.setdefault(turn_id, set()).add(row)
            from .reactions import handed_off
            with self.store.db:
                self.store.messages_confirm([row], 'replayed')
                if row in self._turn_rows:
                    handed_off(self.store, row)
            self._unreplayed.discard(row)

    def _apply_event(self, event, replay=False):
        kind = event.get('type')
        if kind == 'assistant' and not replay:
            from .reactions import parent_tools
            with self.store.db:
                parent_tools(self.store, self._turn_messages.get(event.get('turn_id'), set()),
                             event['message']['content'])
        elif kind == 'user.received':
            self._confirm(event['message_id'], event.get('turn_id'))
        elif kind == 'item.completed':
            self._replies[event.get('turn_id')] = event['item']['text']
        elif kind in ('turn.completed', 'turn.failed'):
            turn_id = event.get('turn_id')
            rows = self._turn_messages.pop(turn_id, set()) if turn_id is not None else set(self._turn_rows)
            reply = self._replies.pop(turn_id, '')
            self.busy = self.turn_open = self.protocol.turn_id is not None
            if replay:
                self._turn_rows -= rows
                self._rows = {wire: row for wire, row in self._rows.items() if row not in rows}
                return
            if kind == 'turn.failed' and event['error'].get('codex_error_info') == 'usage_limit_exceeded':
                with self.store.db:
                    self.router.record_rate_limit(self.account_alias, self._limits, rejected=True)
                self.relaunch(self._turn_rows | self._unreplayed)
                return
            from .reactions import finish_turn
            with self.store.db:
                finish_turn(self.store, rows)
            self._track(self._turn_rows - rows)
            self._rows = {wire: row for wire, row in self._rows.items() if row not in rows}
            self._recovery_uncertain = False
            if self.on_turn:
                try:
                    self.on_turn(reply)
                except Exception as error:
                    logger.warning('coordinator turn callback failed session=%s type=%s',
                                   self.session_id, type(error).__name__)
            if self._rate_limited and self.idle():
                self.relaunch(self._unreplayed)
        elif kind == 'host.request' and not replay:
            problems.record(self.store, 'coordinator', 'host-request', event['method'])
        if 'rate_limits' in event:
            self._limits = event['rate_limits']
        if 'rate_limits' in event and not replay:
            with self.store.db:
                self.router.record_rate_limit(self.account_alias, event['rate_limits'])
            if self.router.exhausted(self.account_alias, parent=True):
                with self.store.db:
                    self.router.record_rate_limit(self.account_alias, self._limits, rejected=True)
                self._rate_limited = True
                if self.idle():
                    self.relaunch(self._unreplayed)
        elif 'rate_limits' in event and replay:
            self._rate_limited = self.router.exhausted(self.account_alias, parent=True)

    async def _read_stream(self):
        try:
            while True:
                raw = await self.process.next_event()
                if raw is None:
                    return
                if self._unread_reattach_events:
                    self._unread_reattach_events -= 1
                raw_event = json.loads(raw)
                event = self.protocol.handle(raw_event)
                self._response_received(raw_event)
                if self.protocol.turn_id:
                    self.busy = self.turn_open = True
                if event is not None:
                    self._apply_event(event)
                    self.last_turn_at = time.monotonic()
                if self.idle() and self.movable():
                    self.relaunch()
                self.process.ack()
        except Exception as error:
            self._failure = internal_detail(error)
            problems.record(self.store, 'coordinator', 'reader-failed', self._failure, level=logging.ERROR)
            if self.process.state == 'running':
                asyncio.create_task(self.process.stop())

    async def send(self, message_id, text, row_id=None):
        if self.closed or self._stopping or self.rejected or self.process.returncode is not None:
            return 'closed'
        if self.movable() and self.idle():
            self.relaunch()
            return 'closed'
        wire = str(message_id)
        if row_id is not None:
            self._track(self._turn_rows | {row_id})
            self._rows[wire] = row_id
            self._unreplayed.add(row_id)
        self.busy = True
        receipt = await self.control.steer(wire, text)
        if receipt == 'received':
            self._confirm(wire)
        elif receipt == 'unsupported':
            self._unreplayed.discard(row_id)
            self._rows.pop(wire, None)
            self._track(self._turn_rows - {row_id})
            self.busy = self.turn_open = self.protocol.turn_id is not None
        if receipt == 'received' and self.closed and row_id in self._unreplayed:
            return 'uncertain'
        return receipt

    def idle(self, *, ignore_recovery=False):
        return not (self.busy or self.turn_open or self._unread_reattach_events or self._unreplayed or
                    self._recovery_uncertain and not ignore_recovery or self.protocol.background or self.protocol.pending)

    def movable(self):
        return self.stale or self.outdated or self._rate_limited or self.account_alias != self.router.active()

    def relaunch(self, rows=()):
        if self.rejected:
            return
        self.rejected = True
        self._relaunch_rows = set(rows) | self._unreplayed | self._lost
        with self.store.db:
            self.store.put(self.key + '_lost', sorted(self._relaunch_rows))
        self.protocol.disconnect()
        asyncio.create_task(self.process.stop())

    def _settle(self, rows):
        self._find_transcript()
        path = self.transcript_path
        parent_state.settle(self.store, self.key, rows,
                            lambda rows: codex_transcript_receipts(path, rows) if path and Path(path).is_file() else set())

    async def stop(self):
        if self._stop_task is None:
            self._stop_task = asyncio.create_task(self._stop())
        await asyncio.shield(self._stop_task)

    async def _stop(self):
        self._stopping = True
        self.control.close()
        if not self.closed:
            await self.process.stop()
        await self._monitor_task

    async def detach(self):
        self._stopping = True
        self.control.close()
        self._monitor_task.cancel()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(self._monitor_task, *self.tasks, return_exceptions=True)
        self.process.detach()
        parent_state._active_clients.discard((str((self.runner.state_dir / 'hosts').resolve()), self.key))

    async def wait_closed(self):
        await asyncio.shield(self._monitor_task)
