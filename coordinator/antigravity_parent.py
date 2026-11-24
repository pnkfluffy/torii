"""A persistent Antigravity chat agent with Torii tools on agy's ACP server.

The chat agent runs like an Antigravity worker without the write boundary: the official
`agy acp --dangerously-skip-permissions` in the account home. Torii's rules go in as the
session's system prompt. The Torii MCP server and the chat's MCP servers go in as ACP mcpServers.
"""

import asyncio
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import re
import time
import uuid

from . import antigravity_accounts, antigravity_runtime, native_ids, parent_state, reactions
from .acp_protocol import AcpProtocol
from .native_protocol import RunControl
from .opencode_parent import AcpParentSession, mcp_servers as torii_servers, ready_path, wait_mcp_ready
from .policy import chat_policy
from .providers import ProviderRunner, RunResult, child_environment
from .session import AccountsUnavailable, message_row_groups
from .shared_mcp import read_entries
from .scrub import Scrubber


ROW = re.compile(r'\bmessage=(\d+) kind=')
TOOL = re.compile(r'Run mcp_torii_([A-Za-z0-9_]+)\Z')
QUOTA_FAILURES = ('quota_limited', 'refusal')


def account(store, key, model):
    """The account for this chat's Antigravity agent, or None. A saved session stays on its own account."""
    broker = antigravity_accounts.AntigravityBroker(store)
    saved = (store.get(key + '_antigravity') or {}).get('account')
    if saved and broker.usable(saved) and model in antigravity_accounts.offered(store, saved):
        return saved
    return broker.select(model=model)


def out(store, key):
    """Whether no Antigravity account can run this chat's picked model now."""
    return account(store, key, parent_state.effective_agent(store, key, 'antigravity')['model']) is None


def unavailable(store, key):
    broker = antigravity_accounts.AntigravityBroker(store)
    return AccountsUnavailable(broker.earliest_reset(), provider='antigravity',
                               alias=(store.get(key + '_antigravity') or {}).get('account') or broker.active())


def pick_problem(store):
    """Why chats cannot pick Antigravity, or None."""
    if not any(antigravity_accounts.AntigravityBroker(store).signed_in(alias) for alias in antigravity_accounts.accounts(store)):
        return 'No Antigravity account is signed in. Add one with antigravity.account first.'
    return None


def journal_rows(lines):
    """The message rows whose envelopes went out in a journaled prompt."""
    rows = set()
    for line in lines:
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if isinstance(event, dict) and event.get('method') == 'session/prompt':
            for part in (event.get('params') or {}).get('prompt') or []:
                if isinstance(part, dict) and isinstance(part.get('text'), str):
                    rows.update(int(row) for row in ROW.findall(part['text']))
    return rows


def journal_receipts(path, rows):
    """The rows a host journal shows as written to the agent. Torii does not open agy's own files."""
    from .host import HostClient
    return set(rows) & journal_rows(HostClient(Path(path).parent).written_lines())


def creation(process):
    """The journaled session/new or session/resume request, or None."""
    for line in process.written_lines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if isinstance(event, dict) and event.get('method') in ('session/new', 'session/resume'):
            return event
    return None


def fingerprint(store, root, alias, model, effort, binary, prompt, servers):
    from .mcp import tools_hash
    stable = json.loads(json.dumps(servers))
    for server in stable:
        for item in server.get('env', []):
            if item.get('name') == 'TORII_MCP_READY':
                item['value'] = str(root / 'mcp-ready')
    content = {'tools': tools_hash(store), 'provider': 'antigravity', 'account': alias, 'model': model,
               'effort': effort, 'instructions': prompt, 'mcp_servers': stable, 'binary': binary}
    return hashlib.sha256(json.dumps(content, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def parent_root(store, key):
    root = store.directory / 'antigravity' / 'parents' / hashlib.sha256(key.encode()).hexdigest()[:16]
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    root.chmod(0o700)
    return root


def quota_text(text):
    return text.lstrip().startswith('⚠') and any(marker in text.lower() for marker in antigravity_runtime.QUOTA_MARKERS)


class AntigravityParentSession(AcpParentSession):
    provider = 'antigravity'

    def __init__(self, store, runner, on_turn, receipt_timeout, key):
        self.store, self.runner, self.key = store, runner, key
        self.on_turn = on_turn
        self.control = RunControl(receipt_timeout)
        self.session_id = self.process = self.protocol = None
        self.host_id = self.account_alias = self.model = self.usage_policy = None
        self.root = parent_root(store, key)
        self.transcript_path = None
        self.tasks = []
        self._monitor_task = self._stop_task = None
        self.closed = self.rejected = self.busy = self.turn_open = self.switched = False
        self.stale = self.outdated = self.resume_failed = self._stopping = False
        self._parent_retiring = self._recovery_uncertain = self._quota = False
        self.switch = self._failure = None
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
        saved_host = store.get(key + '_host')
        reattach_only = not parent_state.topic_enabled(store, key)
        if reattach_only and not saved_host:
            return None
        client_key = (str((runner.state_dir / 'hosts').resolve()), key)
        if client_key in parent_state._active_clients:
            raise BlockingIOError('Coordinator already has an attached client')
        session = cls(store, runner, on_turn, receipt_timeout, key)
        agent = parent_state.effective_agent(store, key, 'antigravity')
        alias = account(store, key, agent['model'])
        binary = antigravity_accounts.binary(store)
        prompt = instructions + '\nCurrent usage policy:\n' + chat_policy(store, key)
        session.usage_policy = chat_policy(store, key)
        session.session_id = store.get(key + '_session')
        if session.session_id and not native_ids.valid('antigravity', session.session_id):
            raise ValueError('Invalid Antigravity parent session ID')
        scrub = Scrubber({})
        shared = antigravity_runtime.mcp_servers(read_entries(store, scrub, topic=topic))
        servers = torii_servers(store, topic, session.root / ('mcp-ready-' + uuid.uuid4().hex)) + shared
        scrub.extend(Scrubber(antigravity_runtime.mcp_secrets(shared)).forms)
        digest = fingerprint(store, session.root, alias, agent['model'], agent['effort'], binary,
                             instructions, servers)
        attached = None
        try:
            attached = await parent_state.attach(session, saved_host, digest)
            if attached:
                session.usage_policy = None
            for attempt in range(2):
                if attached:
                    request = creation(session.process)
                    servers = request['params'].get('mcpServers', []) if request else []
                    meta = request['params'].get('_meta') if request else None
                    ready = ready_path(servers)
                    healthy = ready is not None and ready.is_file()
                    session._turn_rows = set(store.get(key + '_turn') or [])
                    session._rows = {str(anchor): members for anchor, members in
                                     message_row_groups(store, session._turn_rows).items()}
                    queued = session._turn_rows - journal_rows(session.process.written_lines())
                    store.requeue_messages(queued)
                    session._track(session._turn_rows - queued)
                    fresh = request is not None and request['method'] == 'session/new'
                else:
                    if reattach_only:
                        return None
                    if alias is None:
                        raise unavailable(store, key)
                    saved = store.get(key + '_antigravity') or {}
                    if session.session_id and saved.get('account') != alias:
                        session.session_id = None
                    session.account_alias, session.model = alias, agent['model']
                    servers = torii_servers(store, topic, session.root / ('mcp-ready-' + uuid.uuid4().hex)) + shared
                    meta = {'antigravity': {'systemPrompt': {'append': prompt}}}
                    spec = {'argv': antigravity_runtime.argv(antigravity_accounts.check_binary(binary), cwd),
                            'cwd': str(cwd), 'env': antigravity_accounts.worker_environment(
                                antigravity_accounts.account_home(alias), child_environment()),
                            'provider': 'antigravity', 'fresh': not session.session_id, 'account': alias,
                            'locks': ['coordinator-' + key] + (['antigravity-' + session.session_id]
                                                               if session.session_id else [])}
                    if not await parent_state.launch(session, spec, digest, private={'scrub': scrub.forms}):
                        return None
                    fresh = not session.session_id
                    healthy = True
                session.transcript_path = str(session.process.directory / 'writes.jsonl')
                parent_state._active_clients.add(client_key)
                if healthy:
                    adapter = replace(antigravity_runtime.adapter(session._problem, session.error),
                                      steer_prompt=False, continue_prompt=None)
                    session.protocol = AcpProtocol(adapter, session.process, session.control, session.session_id,
                        session._observe_session, reattaching=bool(attached), persistent=True, mcp_servers=servers,
                        on_prompt=session._prompt_started, text_limit=None, separate_prompt=session._separate_prompt,
                        session_meta=meta)
                    if attached:
                        for raw in attached['past']:
                            event = session.protocol.handle(json.loads(raw), replay=True)
                            if event is not None:
                                session._apply_event(event, replay=True)
                        session.busy = session.turn_open = bool(session._turn_rows)
                    session.tasks = [asyncio.create_task(session._read_stream()), session.protocol.input_closed]
                    session._monitor_task = asyncio.create_task(session._monitor())
                    effort = saved_host.get('effort') if attached else agent['effort']
                    await session.protocol.start('', cwd, fresh, session.model, thread_options={'effort': effort})
                    if session.protocol.terminal:
                        session.resume_failed = session.protocol.failure == 'session_missing'
                        if session.resume_failed:
                            with store.db:
                                store.put(key + '_session', None)
                        raise RuntimeError('Antigravity parent start failed: ' + str(session.protocol.failure))
                    if not attached:
                        healthy = await wait_mcp_ready(ready_path(servers), session.process)
                if healthy:
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
                    raise RuntimeError('Torii MCP is unavailable for the Antigravity parent.')
            raise RuntimeError('Antigravity parent did not start.')
        except BaseException:
            await session._close_attempt()
            parent_state._active_clients.discard(client_key)
            raise

    def _observe_session(self, sid):
        if not native_ids.valid('antigravity', sid) or self.session_id and sid != self.session_id:
            raise RuntimeError('Provider changed the Antigravity parent session ID')
        self.session_id = sid
        saved = self.store.get(self.key + '_antigravity') or {}
        with self.store.db:
            self.store.put(self.key + '_session', sid)
            self.store.put(self.key + '_provider', self.provider)
            self.store.put(self.key + '_fresh', False)
            self.store.put(self.key + '_antigravity', {
                'account': self.account_alias, 'session': sid, 'host': self.host_id,
                'tokens': saved.get('tokens') if saved.get('session') == sid else None})
            from .session_storage import confirm_parent
            confirm_parent(self)
        return sid

    def _count(self, usage):
        if not usage:
            return
        saved = self.store.get(self.key + '_antigravity') or {}
        tokens = dict(saved.get('tokens') or {})
        for name, value in usage.items():
            if name.endswith('_tokens') and type(value) is int:
                tokens[name] = tokens.get(name, 0) + value
        self.store.put(self.key + '_antigravity', dict(saved, tokens=tokens))

    def _apply_event(self, event, replay=False):
        kind = event.get('type')
        if kind == 'activity' and not replay:
            match = TOOL.match(event.get('name', ''))
            if match:
                with self.store.db:
                    reactions.parent_tools(self.store, self._prompt_rows(),
                        [{'type': 'tool_use', 'name': 'mcp__torii__' + match[1]}])
        elif kind == 'turn.completed':
            rows = {row for wire in event['messages'] for row in self._rows.get(wire, ())}
            text = event.get('text') or ''
            billed = any(value for name, value in (event.get('usage') or {}).items() if name.endswith('_tokens'))
            limited = not replay and not billed and quota_text(text)
            with self.store.db:
                if not replay and not limited:
                    if not self._stopping:
                        from .parent_replies import completed
                        completed(self, event['turn_id'], rows, text)
                    reactions.finish_turn(self.store, rows)
                if not replay:
                    self._count(event.get('usage'))
                self._track(self._turn_rows - rows)
            self._rows = {wire: members for wire, members in self._rows.items() if members.isdisjoint(rows)}
            self.busy = self.turn_open = bool(self._turn_rows)
            self._recovery_uncertain = False
            self.last_turn_at = time.monotonic()
            if limited:
                self._quota = True
                self._failure = 'quota_limited'
                asyncio.create_task(self._hold())
                self.relaunch(rows)

    async def _hold(self):
        """Hold the account until its usage resets, as a refused worker run does."""
        try:
            await antigravity_accounts.AntigravityBroker(self.store).record_result(
                self.account_alias, RunResult(self.session_id, quota_limited=True))
        except Exception as error:
            self._problem('accounts', 'antigravity-usage-failed', type(error).__name__)

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
            if self.protocol.failure not in (None, 'owner_stopped', 'disconnected'):
                self._failure = self.protocol.failure
            if self._failure in QUOTA_FAILURES and not self._quota:
                self._quota = True
                await self._hold()

    async def _monitor(self):
        await parent_state.monitor(self)
        if self.process.returncode is not None and self._turn_rows and not self.rejected:
            self._settle(self._turn_rows)
            self._track(set())

    async def send(self, message_id, text, row_id=None):
        if text.lstrip().startswith('/'):
            raise ValueError('Antigravity parent prompts must use the message envelope.')
        if self.closed or self._stopping or self.rejected or self.process.returncode is not None:
            return 'closed'
        if self.movable() and self.idle():
            self.relaunch()
            return 'closed'
        wire = str(row_id) if row_id is not None else str(message_id)
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

    def movable(self):
        return (parent_state.selection_changed(self) or self.stale or self.outdated or
                not antigravity_accounts.AntigravityBroker(self.store).usable(self.account_alias))

    def receipt_rows(self, path, rows):
        return journal_receipts(path, rows)

    def transcript_receipts(self, path, rows):
        return journal_receipts(path, rows)

    def _settle(self, rows):
        directory = self.process.directory if self.process else None
        parent_state.settle(self.store, self.key, rows, lambda wanted: (
            journal_receipts(directory / 'writes.jsonl', wanted) if directory else set()))

