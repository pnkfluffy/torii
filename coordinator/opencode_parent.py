"""A persistent OpenCode parent with Torii tools on ACP."""

import asyncio
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import re
import sys
import tempfile
import time
import uuid

from . import native_ids, opencode_accounts, opencode_subscription, opencode_recovery, opencode_runtime, parent_state, problems, reactions
from .acp_protocol import AcpProtocol
from .native_protocol import RunControl
from .policy import chat_policy
from .providers import ProviderRunner
from .session import AccountsUnavailable, message_row_groups


MCP_READY_SECONDS = 30


def mcp_servers(store, topic, ready):
    defaults = {name: str(Path.home() / folder) if folder is not None else tempfile.gettempdir()
                for name, (_, folder) in opencode_runtime.PRIVATE_FOLDERS.items()}
    return [{'name': 'torii', 'command': sys.executable,
             'args': ['-m', 'coordinator', '--state-dir', str(store.directory), 'mcp', '--topic=' + topic],
             'env': [{'name': 'PYTHONPATH', 'value': str(Path(__file__).resolve().parents[1])},
                     {'name': 'TORII_MCP_READY', 'value': str(ready)},
                     *[{'name': name, 'value': os.environ.get(name) or value} for name, value in defaults.items()]]}]


def ready_path(servers):
    return next((Path(item['value']) for server in servers if server.get('name') == 'torii'
                 for item in server.get('env', []) if item.get('name') == 'TORII_MCP_READY'), None)


async def wait_mcp_ready(path, process):
    deadline = time.monotonic() + MCP_READY_SECONDS
    while path is not None and not path.is_file() and process.returncode is None and time.monotonic() < deadline:
        await asyncio.sleep(0.02)
    return path is not None and path.is_file() and process.returncode is None


def fingerprint(store, root, alias, account, binary, prompt, config, servers):
    from .mcp import tools_hash
    stable = json.loads(json.dumps(servers))
    for server in stable:
        for item in server.get('env', []):
            if item.get('name') == 'TORII_MCP_READY':
                item['value'] = str(root / 'mcp-ready')
    content = {'tools': tools_hash(), 'provider': 'opencode', 'account': alias, 'account_record': account,
               'model': account.get('model'), 'instructions': prompt, 'config': json.dumps(config) + '\n',
               'mcp_servers': stable, 'binary': binary, 'cli': opencode_accounts.PINNED_VERSION,
               'guard': hashlib.sha256(Path(opencode_runtime.__file__).with_name('opencode_guard.js').read_bytes()).hexdigest()}
    return hashlib.sha256(json.dumps(content, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


class OpenCodeParentSession:
    provider = 'opencode'
    _track = parent_state.track
    restarted_prompt = staticmethod(parent_state.restarted_prompt)

    def __init__(self, store, runner, on_turn, receipt_timeout, key):
        self.store, self.runner, self.key = store, runner, key
        self.on_turn = on_turn
        self.control = RunControl(receipt_timeout)
        self.session_id = self.process = self.protocol = None
        self.host_id = self.account_alias = self.model = self.usage_policy = None
        self.root = opencode_runtime.parent_root(store, key)
        self.transcript_path = self.root / 'data/opencode/opencode.db'
        self.tasks = []
        self._monitor_task = self._stop_task = None
        self.closed = self.rejected = self.busy = self.turn_open = self.switched = False
        self.stale = self.outdated = self.resume_failed = self._stopping = False
        self._parent_retiring = self._recovery_uncertain = False
        self.switch = self._failure = None
        self._failure_detail = None
        self._provider_error = {}
        self._turn_owner = None
        self._turn_rows, self._unreplayed, self._lost, self._relaunch_rows = set(), set(), set(), set()
        self._rows = {}
        self._unread_reattach_events = self._sending = 0
        self.last_turn_at = 0
        self.error = {}

    @classmethod
    async def start(cls, store, runner, cwd, model, instructions, mcp_config=None,
                    on_turn=None, receipt_timeout=30, topic=None):
        from .parent import session_key
        runner = runner if isinstance(runner, ProviderRunner) else ProviderRunner(store.directory, binaries=runner)
        key = session_key(store, cwd, topic)
        if key == 'coordinator':
            raise ValueError('The home chat cannot use OpenCode.')
        saved_host = store.get(key + '_host')
        reattach_only = not parent_state.topic_enabled(store, key)
        if reattach_only and not saved_host:
            return None
        client_key = (str((runner.state_dir / 'hosts').resolve()), key)
        if client_key in parent_state._active_clients:
            raise BlockingIOError('Coordinator already has an attached client')
        session = cls(store, runner, on_turn, receipt_timeout, key)
        alias = opencode_accounts.active(store)
        unavailable = None
        try:
            opencode_runtime.parent_ready(store)
        except AccountsUnavailable as error:
            unavailable = error
            if not saved_host:
                raise
        account = opencode_accounts.accounts(store).get(alias) or {}
        binary = opencode_accounts.binary(store)
        prompt = instructions + '\nCurrent usage policy:\n' + chat_policy(store, key)
        session.usage_policy = chat_policy(store, key)
        session.session_id = store.get(key + '_session')
        if session.session_id and not native_ids.valid('opencode', session.session_id):
            raise ValueError('Invalid OpenCode parent session ID')
        if not session.session_id:
            with store.db:
                store.put(key + '_opencode', None)
        servers = mcp_servers(store, topic, session.root / ('mcp-ready-' + uuid.uuid4().hex))
        config = (opencode_runtime.parent_config(account, opencode_accounts.account_home(alias) / 'key', cwd,
                                               session.root / 'instructions.md') if account else {})
        digest = fingerprint(store, session.root, alias, account, binary, prompt, config, servers)
        attached = None
        try:
            attached = await parent_state.attach(session, saved_host, digest)
            if opencode_recovery.state(store, key) or store.get(key + '_turn'):
                opencode_recovery.confirm(session)
            session._turn_owner = opencode_recovery.state(store, key).get('owner')
            if not attached and unavailable:
                raise unavailable
            if not attached and not reattach_only:
                if not opencode_recovery.prepare(session):
                    return None
                session.session_id = store.get(key + '_session')
            for attempt in range(2):
                if attached:
                    journal = [json.loads(line) for line in session.process.written_lines()]
                    creation = next((event for event in journal if event.get('method') in ('session/new', 'session/load')), None)
                    servers = creation['params']['mcpServers'] if creation else []
                    ready = ready_path(servers)
                    healthy = ready is not None and ready.is_file()
                    session._turn_rows = set(store.get(key + '_turn') or [])
                    session._rows = {str(anchor): members for anchor, members in
                                     message_row_groups(store, session._turn_rows).items()}
                    journaled = {int(row) for event in journal if event.get('method') == 'session/prompt'
                                 for part in event['params'].get('prompt', [])
                                 for row in re.findall(r'\bmessage=(\d+) kind=', part.get('text', ''))}
                    queued = session._turn_rows - journaled
                    store.requeue_messages(queued)
                    session._track(session._turn_rows - queued)
                    fresh = creation is not None and creation['method'] == 'session/new'
                else:
                    if reattach_only:
                        return None
                    opencode_runtime.parent_ready(store)
                    if session.session_id and await runner._external_writer('opencode', session.session_id):
                        raise RuntimeError('Session is already owned by an external native process')
                    session.account_alias, session.model = alias, account['model']
                    opencode_runtime.parent_environment(session.root, os.environ)
                    opencode_runtime.write_private(session.root / 'instructions.md', prompt.encode())
                    opencode_runtime.write_parent_config(session.root, account, opencode_accounts.account_home(alias) / 'key',
                                                         cwd, session.root / 'instructions.md')
                    servers = mcp_servers(store, topic, session.root / ('mcp-ready-' + uuid.uuid4().hex))
                    spec = {'argv': opencode_runtime.argv(binary, cwd), 'cwd': str(cwd),
                            'env': opencode_runtime.parent_environment(session.root, os.environ), 'provider': 'opencode',
                            'fresh': not session.session_id,
                            'locks': ['coordinator-' + key] + (['opencode-' + session.session_id] if session.session_id else []),
                            'account': alias}
                    if not await parent_state.launch(session, spec, digest):
                        return None
                    fresh = not session.session_id
                    healthy = True
                parent_state._active_clients.add(client_key)
                if healthy:
                    session.protocol = AcpProtocol(opencode_runtime.parent_adapter(session._problem, session.error),
                        session.process, session.control, session.session_id, session._observe_session,
                        reattaching=bool(attached), persistent=True, mcp_servers=servers,
                        on_prompt=session._prompt_started, text_limit=None, separate_prompt=session._separate_prompt)
                    session.protocol.background = None
                    if attached:
                        for raw in attached['past']:
                            event = session.protocol.handle(json.loads(raw), replay=True)
                            if event is not None:
                                session._apply_event(event, replay=True)
                        session.busy = session.turn_open = bool(session._turn_rows)
                    session.tasks = [asyncio.create_task(session._read_stream()), session.protocol.input_closed]
                    session._monitor_task = asyncio.create_task(session._monitor())
                    await session.protocol.start('', cwd, fresh, session.model)
                    if session.protocol.terminal:
                        session.resume_failed = session.protocol.failure == 'session_missing'
                        raise RuntimeError('OpenCode parent start failed: ' + str(session.protocol.failure))
                    if not attached:
                        healthy = await wait_mcp_ready(ready_path(servers), session.process)
                if healthy:
                    from .session_storage import confirm_parent
                    confirm_parent(session)
                    session._track(session._turn_rows)
                    lost = store.get(key + '_lost')
                    if lost and attached:
                        session.stale, session._lost = True, set(lost)
                    elif lost:
                        session._settle(lost)
                    if session.movable() and session.idle():
                        session.relaunch()
                    return session
                await session._close_attempt()
                attached = None
                if attempt == 1:
                    session._problem('coordinator', 'mcp-unavailable', 'Torii MCP did not complete tools/list in two hosts.')
                    raise RuntimeError('Torii MCP is unavailable for the OpenCode parent.')
            raise RuntimeError('OpenCode parent did not start.')
        except BaseException:
            if (attached and attached['state'] == 'exited' and
                    opencode_recovery.state(store, key).get('phase') == 'catchup'):
                opencode_recovery.failed(store, key, session.session_id, 'disconnected',
                    'The fresh catch-up host exited while Torii was offline.', session._turn_rows, session._turn_owner)
                session._track(set())
            await session._close_attempt()
            parent_state._active_clients.discard(client_key)
            raise

    def _problem(self, category, code, detail):
        problems.record(self.store, category, code, detail, level=logging.ERROR,
                        topic=self.key.split(':', 1)[1])

    async def _close_attempt(self):
        if self.process and self._monitor_task:
            await self.stop()
        elif self.process:
            await parent_state.retire(self)
            await self.process.wait()
            self.process.detach()
            if self._turn_rows:
                self._settle(self._turn_rows)
                self._track(set())
        self.tasks = []
        self.process = self.protocol = None
        self._monitor_task = self._stop_task = None
        self.closed = self._stopping = False
        self.control = RunControl(self.control.receipt_timeout)

    def _observe_session(self, sid):
        if not native_ids.valid('opencode', sid) or self.session_id and sid != self.session_id:
            raise RuntimeError('Provider changed the OpenCode parent session ID')
        self.session_id = sid
        saved = self.store.get(self.key + '_opencode') or {}
        with self.store.db:
            self.store.put(self.key + '_session', sid)
            self.store.put(self.key + '_provider', self.provider)
            self.store.put(self.key + '_fresh', False)
            self.store.put(self.key + '_opencode', {'account': self.account_alias, 'session': sid,
                           'usd': saved.get('usd') if saved.get('session') == sid else None})
            opencode_recovery.observed(self.store, self.key, sid)
        return sid

    def _event_consumed(self, seq):
        with self.store.db:
            self.store.put(self.key + '_host_seq', seq)

    def _prompt_started(self, turn, wires):
        from .parent_replies import started
        started(self, turn, {row for wire in wires for row in self._rows.get(wire, ())})

    def _separate_prompt(self, wire):
        return self.store.db.execute('SELECT 1 FROM messages WHERE id=? AND batch_id IS NOT NULL',
                                     (wire,)).fetchone() is not None

    def _prompt_rows(self):
        request = next((self.process.past_request(request_id) for request_id, (method, future)
                        in self.protocol.requests.items() if method == 'session/prompt'), None)
        wires = ((request or {}).get('params', {}).get('_meta', {}).get('toriiSteers')
                 or self.protocol._prompt_messages)
        return {row for wire in wires for row in self._rows.get(wire, ())}

    def _apply_event(self, event, replay=False):
        kind = event.get('type')
        if kind == 'activity' and not replay:
            name = event.get('name', '')
            if name.startswith('torii_'):
                with self.store.db:
                    reactions.parent_tools(self.store, self._prompt_rows(),
                        [{'type': 'tool_use', 'name': 'mcp__torii__' + name[len('torii_'):]}])
        elif kind == 'turn.completed':
            rows = {row for wire in event['messages'] for row in self._rows.get(wire, ())}
            with self.store.db:
                opencode_recovery.completed(self.store, self.key, rows)
                if not replay:
                    if not self._stopping:
                        from .parent_replies import completed
                        completed(self, event['turn_id'], rows, event.get('text') or '')
                    reactions.finish_turn(self.store, rows)
                    blocks = self.store.get('opencode_account_blocks', {}) or {}
                    block = blocks.get(self.account_alias) or {}
                    if block and block.get('until', 0) <= time.time():
                        blocks.pop(self.account_alias)
                        self.store.put('opencode_account_blocks', blocks)
                self._track(self._turn_rows - rows)
                usd = (event.get('usage') or {}).get('usd_estimate')
                if usd is not None:
                    saved = self.store.get(self.key + '_opencode') or {}
                    self.store.put(self.key + '_opencode', dict(saved, usd=usd))
            self._rows = {wire: members for wire, members in self._rows.items() if members.isdisjoint(rows)}
            self.busy = self.turn_open = bool(self._turn_rows)
            self._recovery_uncertain = False
            self.last_turn_at = time.monotonic()

    async def _read_stream(self):
        try:
            while True:
                raw = await self.process.next_event()
                if raw is None:
                    return
                if self._unread_reattach_events:
                    self._unread_reattach_events -= 1
                event = self.protocol.handle(json.loads(raw))
                if event is not None:
                    self._apply_event(event)
                if self.idle() and self.movable():
                    self.relaunch()
                self.process.ack()
        except Exception as error:
            self._failure = type(error).__name__
            self._problem('coordinator', 'reader-failed', self._failure)
            await parent_state.retire(self)
        finally:
            if not self._stopping and not self.rejected and not self.protocol.failure:
                self._failure = self._failure or 'disconnected'
            if self.protocol.failure not in (None, 'owner_stopped'):
                self._failure = self.protocol.failure
                self._provider_error = opencode_runtime.parent_error(self.root, self.session_id) or {}
                self._failure = opencode_recovery.classify(self._provider_error, self._failure)
                if self._provider_error:
                    self._failure_detail = 'upstream=%s status=%s code=%s %s' % (
                        self._provider_error.get('name'), self._provider_error.get('status'),
                        self._provider_error.get('code'), self._provider_error.get('message'))

    async def _monitor(self):
        await parent_state.monitor(self)
        if (self.error or self._provider_error) and self.process.returncode is not None:
            saved = opencode_runtime.records(self.root, self.session_id)
            info = saved[0] if saved is not None else {}
            data = (info.get('error') or {}).get('data') or {}
            retry_at = opencode_runtime.parent_retry_at(info)
            reset_at = self._provider_error.get('reset_at')
            if (self._failure == 'usage_limited' and type(reset_at) in (int, float) and
                    math.isfinite(reset_at) and reset_at > time.time()):
                retry_at = reset_at
            account = opencode_accounts.accounts(self.store).get(self.account_alias) or {}
            with self.store.db:
                if opencode_subscription.is_subscription(account):
                    limits = opencode_subscription.limit_info(data, data.get('message', ''))
                    if limits is not None:
                        opencode_subscription.record_limit(self.store, self.account_alias, dict(limits, retry_at=retry_at))
                else:
                    status = self._provider_error.get('status') or data.get('statusCode')
                    reason = ('usage_limited' if self._failure == 'usage_limited' else
                              {429: 'rate_limited', 402: 'out_of_credit', 401: 'auth', 403: 'auth'}.get(status)
                              if type(status) is int else None)
                    if type(status) is int and reason is not None:
                        opencode_runtime.record_parent_block(self.store, self.account_alias, reason, retry_at)
                        if reason == 'usage_limited' and self.account_alias in (self.store.get('opencode_account_blocks') or {}):
                            blocks = self.store.get('opencode_account_blocks') or {}
                            blocks[self.account_alias].update(detail=self._provider_error.get('message'),
                                                             reset_at=self._provider_error.get('reset_at'))
                            self.store.put('opencode_account_blocks', blocks)
        if self.process.returncode is not None and self._turn_rows and not self.rejected:
            if not self._stopping:
                opencode_recovery.failed(self.store, self.key, self.session_id,
                    self._failure or 'disconnected', self._failure_detail or 'OpenCode stopped before the turn finished.',
                    self._turn_rows, self._turn_owner)
            else:
                self._settle(self._turn_rows)
            self._track(set())

    async def send(self, message_id, text, row_id=None):
        if text.lstrip().startswith('/'):
            raise ValueError('OpenCode parent prompts must use the message envelope.')
        if self.closed or self._stopping or self.rejected or self.process.returncode is not None:
            return 'closed'
        if self.movable() and self.idle():
            self.relaunch()
            return 'closed'
        wire = str(row_id) if row_id is not None else str(message_id)
        if not self.turn_open:
            self._turn_owner = opencode_recovery.owner_cursor(self.store, self.key)
        if row_id is not None:
            rows = self.store.message_batch_rows(row_id)
            self._rows[wire] = rows
            self._track(self._turn_rows | rows)
            with self.store.db:
                for row in rows:
                    reactions.handed_off(self.store, row)
        self.busy = self.turn_open = True
        self._sending += 1
        try:
            return await self.control.steer(wire, text)
        finally:
            self._sending -= 1

    def idle(self, *, ignore_recovery=False):
        return not (self.busy or self.turn_open or self._sending or self._unread_reattach_events or
                    self._unreplayed or self._recovery_uncertain and not ignore_recovery or self.protocol.queued or
                    any(not future.done() for method, future in self.protocol.requests.values()))

    def movable(self):
        return (parent_state.selection_changed(self) or self.stale or self.outdated or
                self.account_alias != opencode_accounts.active(self.store))

    def relaunch(self, rows=()):
        if not parent_state.topic_enabled(self.store, self.key):
            return
        selected = parent_state.selection_changed(self)
        if self.rejected or selected and not self.idle():
            return
        self.rejected = True
        self._parent_retiring = selected
        self._relaunch_rows = set(rows) | self._turn_rows | self._lost
        with self.store.db:
            self.store.put(self.key + '_lost', sorted(self._relaunch_rows))
        self.protocol.disconnect()
        asyncio.create_task(parent_state.retire(self))

    def transcript_receipts(self, path, rows):
        return opencode_runtime.parent_receipts(Path(path).parents[2], self.session_id, rows)

    def _settle(self, rows):
        parent_state.settle(self.store, self.key, rows,
                            lambda wanted: opencode_runtime.parent_receipts(self.root, self.session_id, wanted))

    async def stop(self):
        if self._stop_task is None:
            self._stop_task = asyncio.create_task(self._stop())
        await asyncio.shield(self._stop_task)

    async def _stop(self):
        self._stopping = True
        self.control.close()
        if not self.closed:
            await parent_state.retire(self)
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
