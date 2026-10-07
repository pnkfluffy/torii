import asyncio
from dataclasses import dataclass
import hashlib
import json
import logging
import os
from pathlib import Path
import sys
import time
import uuid

from . import problems, parent_state
from .parent_state import (LOST_TURN, _active_clients, RESTART_TASKS, RESTART_TASK_TITLE, RESTART_WORKERS,
                           RESTART_MESSAGES, RESTART_MESSAGE_CHARS, RESTART_MESSAGE_HEAD, uncertain_summary,
                           open_tasks_summary, workers_summary)
from .accounts import AccountBroker, DEFAULT_ACCOUNTS, account_label
from .claude_settings import claude_md_excludes
from .failures import internal_detail
from .shared_mcp import write_config as write_shared_mcp_config
from .native_protocol import NativeProtocol, RunControl
from .policy import usage_policy
from .providers import ProviderRunner
from .scrub import Scrubber


logger = logging.getLogger(__name__)
PARENT_FLAGS = ('--system-prompt-snapshot', 'off')


def message_uuid(row_id):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, 'torii-message:' + str(row_id)))


def message_tag(topic, row):
    """The one prefix for every message steered into the coordinator: topic id, name, message id, and kind."""
    return '[topic=%s name=%s message=%d kind=%s] ' % (
        topic['id'], json.dumps(topic['name'], ensure_ascii=False), row['id'], row['kind'])


def transcript_receipts(path, message_ids):
    """Return the message UUIDs the native transcript shows in the conversation.

    A message that starts a turn is a user entry with its UUID. A message that
    arrives during a turn is a queued_command attachment whose source_uuid is its UUID.
    """
    wanted = set(message_ids)
    found = set()
    with open(path, errors='replace') as stream:
        for line in stream:
            if not any(item in line for item in wanted):
                continue
            try:
                entry = json.loads(line)
            except ValueError:
                continue
            if not isinstance(entry, dict):
                continue
            attachment = entry.get('attachment') if isinstance(entry.get('attachment'), dict) else {}
            if entry.get('type') == 'user' and entry.get('uuid') in wanted:
                found.add(entry['uuid'])
            elif (entry.get('type') == 'attachment' and attachment.get('type') == 'queued_command'
                  and attachment.get('source_uuid') in wanted):
                found.add(attachment['source_uuid'])
    return found


class AccountsUnavailable(Exception):
    def __init__(self, reset_at, provider='claude', alias=None):
        self.reset_at, self.provider, self.alias = reset_at, provider, alias
        super().__init__('No agent account is available')


@dataclass(frozen=True)
class LaunchInputs:
    model: str
    effort: str
    settings: str
    instructions: str
    policy: str
    configs: tuple
    fingerprint: str
    scrub: Scrubber

    @property
    def prompt(self):
        return self.instructions + '\nCurrent usage policy:\n' + self.policy


class CoordinatorSession:
    provider = 'claude'

    @staticmethod
    def transcript_receipts(path, rows):
        found = transcript_receipts(path, [message_uuid(row) for row in rows])
        return {row for row in rows if message_uuid(row) in found}

    def __init__(self, store, runner, session_id, on_turn, receipt_timeout, key='coordinator'):
        self.store = store
        self.key = key
        self.runner = runner
        self.session_id = session_id
        self.on_turn = on_turn
        self.control = RunControl(receipt_timeout)
        self.process = None
        self.protocol = None
        self.log_path = None
        self.host_id = None
        self.tasks = []
        self._monitor_task = None
        self._stop_task = None
        self._stopping = False
        self._failure = None
        self._observed = False
        self._sent_ids = set()
        self._turn_rows = set()
        self._unread_reattach_events = 0
        self._rows = {}
        self._unreplayed = set()
        self._unconsumed_writes = set()
        self._recovery_result_seen = False
        self._recovery_uncertain = False
        self.transcript_path = None
        self.rejected = False
        self.launch_state = 'native'
        self.stale = False
        self.outdated = False
        self._lost = set()
        self._relaunch_rows = set()
        self.account_alias = None
        self.model = None
        self.usage_policy = None
        self.router = AccountBroker(store)
        self.busy = False
        self.turn_open = False
        self.closed = False
        self.last_turn_at = 0
        self.resume_failed = False

    @classmethod
    async def start(cls, store, runner_or_binaries, cwd, model, instructions, mcp_config=None,
                    on_turn=None, receipt_timeout=30, topic=None):
        runner = (runner_or_binaries if isinstance(runner_or_binaries, ProviderRunner)
                  else ProviderRunner(store.directory, binaries=runner_or_binaries))
        home = next((item['id'] for item in store.topics()
                     if item['enabled'] and item['cwd'] == str(cwd)), None)
        main = store.get('coordinator_session_topic') or store.get('coordinator_home_topic') or home
        key = 'coordinator' if topic is None or topic == main else 'coordinator:' + topic
        session = cls(store, runner, None, on_turn, receipt_timeout, key)
        runner.account_broker = session.router
        client_registered = False
        past = []
        later = []
        try:
            saved = store.get(key + '_session')
            if saved:
                if str(uuid.UUID(saved)) != saved:
                    raise ValueError('Coordinator session ID must be a canonical UUID')
                fresh = store.get(key + '_fresh', False)
                session.session_id = saved
            else:
                fresh = True
                session.session_id = str(uuid.uuid4())
            if not saved:
                with store.db:
                    store.put(key + '_session', session.session_id)
                    store.put(key + '_fresh', True)
            saved_host = store.get(key + '_host')
            directory = runner.state_dir / 'hosts'
            client_key = (str(directory.resolve()), key)
            if client_key in _active_clients:
                raise BlockingIOError('Coordinator already has an attached client')
            inputs = cls._launch_inputs(store, model, instructions, mcp_config,
                                        runner.extension.fingerprint())
            attached = await parent_state.attach(session, saved_host, inputs.fingerprint)
            saved_state = attached['state'] if attached else 'dead'
            if attached:
                session.turn_open = True
                env = attached['env']
                session.transcript_path = session.router.transcript(saved) if saved else None
                session._turn_rows = set(store.get(key + '_turn') or [])
                scope = None if key == 'coordinator' else topic
                session._rows = {message_uuid(row['id']): row['id'] for row in store.db.execute(
                    """SELECT id FROM messages WHERE
                    (delivered='uncertain' OR delivered='sent' OR
                     delivered='received' AND receipt='written') AND
                    (? IS NULL OR topic=?)""", (scope, scope))}
                past, later = attached['past'], attached['later']
            else:
                if saved_host:
                    with store.db:
                        store.put(key + '_host', None)
                if await runner._external_writer('claude', session.session_id):
                    raise RuntimeError('Session is already owned by an external native process')
                accounts = store.get('accounts', DEFAULT_ACCOUNTS)
                previous = store.get('coordinator_rotation_from')
                alias = session.router.activate(accounts)
                if alias is None:
                    raise AccountsUnavailable(session.router.earliest_reset(accounts))
                config_dir = accounts[alias].get('config_dir')
                resume_path = session.router.transcript(saved) if saved and not fresh else None
                if resume_path and config_dir:
                    resume_path = session.router.resume_transcript(alias, saved, resume_path)
                if saved and not fresh and config_dir and not resume_path:
                    problems.record(store, 'coordinator', 'resume-lost', 'key=%s session=%s' % (key, saved),
                                    level=logging.ERROR, topic=topic)
                    session.resume_failed = True
                    fresh = True
                    session.session_id = str(uuid.uuid4())
                    with store.db:
                        store.put(key + '_session', session.session_id)
                        store.put(key + '_fresh', True)
                session.transcript_path = resume_path
                command = [runner.binaries['claude'], '--print', '--verbose', '--output-format', 'stream-json',
                           '--input-format', 'stream-json', '--replay-user-messages',
                           '--dangerously-skip-permissions', '--model', inputs.model,
                           '--effort', inputs.effort, '--settings', inputs.settings,
                           '--session-id' if fresh else '--resume',
                           session.session_id if fresh else resume_path or session.session_id,
                           '--append-system-prompt', inputs.prompt] + list(PARENT_FLAGS)
                if inputs.configs:
                    command += ['--mcp-config'] + [str(config) for config in inputs.configs]
                env = runner._environment('claude', alias)
                private = await runner.launch_secrets(env)
                inputs.scrub.extend(Scrubber(private).forms)
                spec = {'argv': command, 'cwd': str(cwd), 'env': env,
                        'provider': 'claude', 'locks': ['coordinator-' + key, 'claude-' + session.session_id]}
                session.account_alias = alias
                session.model = inputs.model
                await parent_state.launch(session, spec, inputs.fingerprint,
                                          private={'env': private, 'scrub': inputs.scrub.forms})
                session.usage_policy = inputs.policy
                with store.db:
                    store.put('last_account', alias)
                    if previous:
                        store.put('coordinator_rotation_from', None)
                        if previous != alias:
                            problems.record(store, 'accounts', 'rotated',
                                            'coordinator from=%s to=%s model=%s' %
                                            (account_label(store, previous), account_label(store, alias), model))
            session.log_path = str(directory / session.host_id / 'events.jsonl')
            session.launch_state = runner.extension.state(env)
            lost = store.get(key + '_lost')
            background_lost = store.get(key + '_background_lost')
            if (lost or background_lost) and saved_state == 'running':
                session.stale = True
                session._lost = set(lost or [])
            elif lost or background_lost:
                session._settle(lost or [])
            _active_clients.add(client_key)
            client_registered = True
            session.protocol = NativeProtocol('claude', session.process, session.control,
                                              session.session_id, session._observe_session, persistent=True,
                                              on_control_request=runner.extension.control_handler(env), on_handoff=session._handed_off)
            if saved_host and saved_state != 'dead':
                session._restore_state(past)
            for request in runner.extension.unanswered(past, later):
                session.protocol.handle(request)
            session.control.bind(session.protocol.steer)
            session.tasks = [asyncio.create_task(session._read_stream()), session.protocol.input_closed]
            session._monitor_task = asyncio.create_task(session._monitor())
            if session.outdated and not later and session.idle():
                session.relaunch()
            return session
        except BaseException:
            if client_registered:
                _active_clients.discard(client_key)
            for task in session.tasks:
                task.cancel()
            if session.tasks:
                await asyncio.gather(*session.tasks, return_exceptions=True)
            if session.process:
                session.process.detach()
            raise

    def _event_consumed(self, seq):
        with self.store.db:
            self.store.put(self.key + '_host_seq', seq)

    @classmethod
    def _launch_inputs(cls, store, model, instructions, mcp_config, extra=None):
        memory_directory = store.directory / 'coordinator-memory'
        memory_directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        settings = json.dumps({'ultracode': False, 'autoMemoryDirectory': str(memory_directory.resolve()),
                               'claudeMdExcludes': claude_md_excludes(Path.home())})
        policy = usage_policy(store.directory)
        config = Path(mcp_config) if mcp_config is not None else cls.write_mcp_config(store)
        scrub = Scrubber({})
        shared_mcp_config = write_shared_mcp_config(store, scrub)
        configs = tuple(path for path in (config, shared_mcp_config) if path is not None)
        effort = 'max'
        from .mcp import tools_hash
        content = {'tools': tools_hash(), 'model': model, 'effort': effort, 'settings': settings, 'instructions': instructions,
                   'flags': PARENT_FLAGS,
                   'mcp_configs': [hashlib.sha256(path.read_bytes()).hexdigest() for path in configs]}
        content.update(extra or {})
        fingerprint = hashlib.sha256(json.dumps(content, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        return LaunchInputs(model, effort, settings, instructions, policy, configs, fingerprint, scrub)

    @staticmethod
    def write_mcp_config(store):
        path = store.directory / 'coordinator-mcp.json'
        temporary = store.directory / 'coordinator-mcp.json.tmp'
        config = {'mcpServers': {'torii': {'alwaysLoad': True,
                                          'command': sys.executable,
                                          'args': ['-m', 'coordinator', '--state-dir', str(store.directory), 'mcp'],
                                          'env': {'PYTHONPATH': str(Path(__file__).resolve().parents[1])}}}}
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            with os.fdopen(descriptor, 'w') as stream:
                json.dump(config, stream)
            os.replace(temporary, path)
        finally:
            if temporary.exists():
                temporary.unlink()
        return path

    def _observe_session(self, session_id):
        if str(uuid.UUID(session_id)) != session_id or session_id != self.session_id:
            raise RuntimeError('Provider changed the coordinator session ID')
        if not self._observed:
            self._observed = True
            with self.store.db:
                self.store.put(self.key + '_fresh', False)
        return session_id

    def _restore_state(self, past):
        turn_rows = set(self._turn_rows)
        try:
            for raw in self.process.written_lines():
                event = json.loads(raw)
                if isinstance(event, dict) and event.get('type') == 'user' and isinstance(event.get('uuid'), str):
                    self._sent_ids.add(event['uuid'])
                    self._unconsumed_writes.add(event['uuid'])
                    row = self._rows.get(event['uuid'])
                    if row is not None:
                        self._unreplayed.add(row)
                elif not isinstance(event, dict):
                    self._recovery_uncertain = True
        except (OSError, ValueError, KeyError, TypeError):
            self._recovery_uncertain = True
        for raw in past:
            try:
                event = json.loads(raw)
                if not isinstance(event, dict):
                    self._recovery_uncertain = True
                    continue
                event = self.protocol.handle(event, replay=True)
                if event is not None:
                    self._apply_event_state(event, replay=True)
            except (ValueError, KeyError, TypeError):
                self._recovery_uncertain = True
        self._turn_rows |= turn_rows
        if self._turn_rows:
            self.turn_open = True

    def _apply_event_state(self, event, replay=False):
        if (not replay and self.protocol.provider == 'claude' and event.get('type') == 'assistant'
                and event.get('parent_tool_use_id') is None):
            from .reactions import parent_tools
            with self.store.db:
                message = event.get('message')
                parent_tools(self.store, self._turn_rows,
                             message.get('content') if isinstance(message, dict) else None)
        if (self.launch_state == 'native' and event.get('type') == 'system'
                and event.get('subtype') == 'background_tasks_changed' and self.protocol.background_completion):
            self.turn_open = True
        if event.get('type') == 'assistant' or (event.get('type') == 'system' and
                                                event.get('subtype') in ('init', 'task_notification')):
            self.turn_open = True
        if event.get('type') == 'user' and event.get('isReplay') is True:
            self.busy = True
            self._unconsumed_writes.discard(event.get('uuid'))
            row = self._rows.get(event.get('uuid'))
            if row is not None:
                self._unreplayed.discard(row)
                if replay:
                    self._turn_rows.add(row)
                else:
                    self._track(self._turn_rows | {row})
                    self._handed_off(event['uuid'])
                    self.store.messages_confirm([row], 'replayed')
                self._rows.pop(event.get('uuid'), None)
        elif event.get('type') == 'result':
            self.busy = False
            self.turn_open = False
            if replay:
                self._turn_rows.clear()
            if not replay:
                self._recovery_result_seen = True

    async def _read_stream(self):
        try:
            while True:
                try:
                    raw = await self.process.next_event()
                except (OSError, ValueError, KeyError, TypeError):
                    self._recovery_uncertain = True
                    self._recovery_result_seen = False
                    await asyncio.sleep(.1)
                    continue
                if raw is None:
                    break
                if self._unread_reattach_events:
                    self._unread_reattach_events -= 1
                try:
                    event = json.loads(raw)
                except ValueError:
                    self._recovery_uncertain = True
                    self._recovery_result_seen = False
                    self.process.ack()
                    continue
                if not isinstance(event, dict):
                    self._recovery_uncertain = True
                    self._recovery_result_seen = False
                    self.process.ack()
                    continue
                if event.get('subtype') != 'commands_changed':
                    self.last_turn_at = time.monotonic()
                try:
                    event = self.protocol.handle(event)
                except (ValueError, KeyError, TypeError):
                    self._recovery_uncertain = True
                    self._recovery_result_seen = False
                    self.process.ack()
                    continue
                if event is not None:
                    self._apply_event_state(event)
                if self._recovery_uncertain and self._recovery_result_seen and self.idle(ignore_recovery=True):
                    self._recovery_uncertain = False
                if event is None:
                    self.process.ack()
                    continue
                if event.get('type') == 'rate_limit_event':
                    info = event.get('rate_limit_info')
                    if isinstance(info, dict):
                        with self.store.db:
                            if self.launch_state != 'managed':
                                self.router.record_rate_limit(self.account_alias, info,
                                                              info.get('status') == 'rejected')
                            self.router.activate()
                        if info.get('status') == 'rejected':
                            self.relaunch(self._turn_rows | self._unreplayed)
                    self.process.ack()
                    continue
                if event.get('type') == 'result':
                    if self.rejected:
                        self.process.ack()
                        continue
                    if event.get('is_error') and (self.protocol.failure or self.launch_state == 'detached'):
                        self.stale = True
                        self._lost |= self._turn_rows - self._unreplayed
                        with self.store.db:
                            self.store.put(self.key + '_lost', sorted(self._lost))
                    self.protocol.failure = None
                    from .reactions import finish_turn
                    with self.store.db:
                        finish_turn(self.store, self._turn_rows)
                    self._track(set())
                    text = event.get('result') or ''
                    logger.info('coordinator turn session=%s error=%s', self.session_id, bool(event.get('is_error')))
                    if self.on_turn:
                        try:
                            self.on_turn(text)
                        except Exception as exc:
                            logger.warning('coordinator turn callback failed session=%s type=%s',
                                           self.session_id, type(exc).__name__)
                if self.idle() and self.movable():
                    self.relaunch(refresh=self._account_move() is not None)
                self.process.ack()
        except Exception as exc:
            self._failure = internal_detail(exc)
            problems.record(self.store, 'coordinator', 'reader-failed', self._failure, level=logging.ERROR)
            if self.process.state == 'running':
                asyncio.create_task(self.process.stop())

    _monitor = parent_state.monitor

    def _handed_off(self, message_id):
        row = self._rows.get(message_id)
        if row is not None and row in self._turn_rows:
            from .reactions import handed_off
            with self.store.db:
                handed_off(self.store, row)

    async def send(self, message_id, text, row_id=None):
        if self.closed or self._stopping or self.rejected or self.process.returncode is not None:
            return 'closed'
        if self.movable() and (self.idle() or self.launch_state == 'detached'):
            self.relaunch(self._turn_rows | self._unreplayed, refresh=self._account_move() is not None)
            return 'closed'
        message_id = str(message_id)
        if row_id is not None:
            self._track(self._turn_rows | {row_id})
            self._rows[message_id] = row_id
            self._unreplayed.add(row_id)
        if message_id not in self._sent_ids:
            self._sent_ids.add(message_id)
            self.busy = True
        receipt = await self.control.steer(message_id, text)
        if receipt == 'received' and self.closed and row_id in self._unreplayed:
            return 'uncertain'
        return receipt

    def idle(self, *, ignore_recovery=False):
        return not (self.busy or self.turn_open or self._unread_reattach_events or self._unreplayed or
                    self._unconsumed_writes or (self._recovery_uncertain and not ignore_recovery) or
                    self.protocol.background or self.protocol.pending)

    def movable(self):
        """Move stale, outdated or no-longer-served parents, and ineligible native accounts, when idle."""
        return (self.stale or self.outdated or self.launch_state in ('stranded', 'detached')
                or self.launch_state == 'native' and self.runner.extension.state({}) == 'stranded'
                or self._account_move() is not None)

    def _account_move(self):
        """Stay on an eligible account, or when no other account is available."""
        if self.launch_state != 'native' or not self.account_alias:
            return None
        accounts = self.store.get('accounts', DEFAULT_ACCOUNTS)
        if self.router.available(accounts, self.account_alias):
            return None
        return self.router.select(accounts)

    _track = parent_state.track

    def relaunch(self, rows=(), *, refresh=False):
        """Stop this parent and settle lost turns after exit. Resume on the selected account."""
        if self.rejected:
            return
        self.rejected = True
        self._relaunch_rows = set(rows) | self._unreplayed | self._lost
        with self.store.db:
            if refresh:
                accounts = self.store.get('accounts', DEFAULT_ACCOUNTS)
                logger.info('coordinator account move session=%s from=%s to=%s %s',
                            self.session_id, self.account_alias, self._account_move(),
                            self.router.selection_reason(accounts, self.account_alias))
                self.store.put('account_status_refresh_requested', time.time())
            if self.launch_state == 'native' and self.protocol.background:
                self.store.put(self.key + '_background_lost', self.protocol.background)
            self.store.put('coordinator_rotation_from', self.account_alias)
            self.store.put(self.key + '_lost', sorted(self._relaunch_rows))
        self.protocol.disconnect()
        asyncio.create_task(self.process.stop())

    def _settle(self, rows):
        try:
            path = self.transcript_path or self.router.transcript(self.session_id)
        except Exception:
            path = None
        parent_state.settle(self.store, self.key, rows,
                            lambda rows: self.transcript_receipts(path, rows) if rows and path else set())

        background = self.store.get(self.key + '_background_lost')
        if background:
            descriptions = ', '.join(str(task.get('description') or task.get('task_id') or 'Unknown task')
                                     if isinstance(task, dict) else str(task) for task in background)
            with self.store.db:
                self.store.put(self.key + '_background_lost', None)
                self.store.message_save(
                    self.store.get('coordinator_session_topic') or self.store.get('coordinator_home_topic') if self.key == 'coordinator' else self.key.split(':', 1)[1],
                    'callback', 'Background tasks stopped when Torii moved this conversation to another account: '
                    + descriptions + '. Check their state before relying on their results.')

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
        self.tasks[0].cancel()
        self.tasks[1].cancel()
        await asyncio.gather(self._monitor_task, *self.tasks, return_exceptions=True)
        self.process.detach()
        _active_clients.discard((str((self.runner.state_dir / 'hosts').resolve()), self.key))

    async def wait_closed(self):
        await asyncio.shield(self._monitor_task)

    restarted_prompt = staticmethod(parent_state.restarted_prompt)
