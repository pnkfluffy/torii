"""Persistent coordinator input, background workers, and Telegram transport."""

import asyncio
from datetime import datetime, timezone
import json
import os
import logging
import re
import sqlite3
import subprocess
import time
from pathlib import Path

from . import intake, problems, parent, parent_state
from .accounts import AccountBroker, DEFAULT_ACCOUNTS
from .codex_accounts import CodexBroker, limit_windows, monitor_codex_accounts
from .control_api import BRIEF_LIMIT, WORKER_PROVIDERS
from .envelopes import FILL_START, waiting_secrets
from .failures import internal_detail
from .media import MediaError, TranscriptionSlots, attachment_instructions, is_audio, prepare_attachments, prepare_transcripts
from .policy import COORDINATOR_POLICY, REPLY_FIRST, chat_policy, default_coordinator_model
from .providers import ProviderRunner
from .session import AccountsUnavailable, CoordinatorSession, message_tag, message_uuid
from .signin import SignIns
from .telegram import ImageUnavailable, TelegramError
from .usage_notice import queue_limit_notice
from .workers import STEER_OPS, STEER_PREFIX, WorkerPool, can_resume_finished, host_directory, reconcile_steers
from .log import running_commit
from . import extension
from .extension import Native
from . import host_os

logger = logging.getLogger(__name__)
TASK_RESTART_LIMIT = 5
TASK_RESTART_BACKOFF = 2.0
LOOP_COOLDOWN = 15 * 60
LOOP_COOLDOWN_LIMIT = 2 * 60 * 60
LOOP_HEALTHY_SECONDS = 10 * 60
RESTART_GUARD_SECONDS = 120
REACTION_IDLE = 0.5
DELIVERY_IDLE = 0.1
FEED_IDLE = 0.1
PARENT_IDLE = 3600
PARENT_RETRY = 60
PARENT_RECHECK_WAIT = 1
WORKER_REATTACH_ATTEMPTS = 3
WORKER_REATTACH_TIMEOUT = 1
WORKER_REATTACH_RETRY = 5
LOOP_ESSENTIAL = {
    'poll': True,
    'deliver': True,
    'updates': True,
    'accounts': True,
    'codex_accounts': True,
    'opencode_accounts': True,
    'command_menu': False,
    'reactions': False,
    'restart_watch': True,
    'envelopes': True,
    'signins': False,
    'group_setup': True,
    'feed': True,
    'workers': True,
    'controls': True,
    'tldr': False,
    'code_request_intake': False,
    'code_request_sender': False,
    'worktree_cleanup': False,
    'host_logs': False,
    'session_cleanup': False,
    'stray_report': False,
    'waits': False,
}
LOOP_NAMES = {
    'command_menu': 'command menu',
    'reactions': 'reactions',
    'signins': 'account sign-in',
    'tldr': 'summaries',
    'worktree_cleanup': 'worktree cleanup',
    'host_logs': 'host log cleanup',
    'session_cleanup': 'native session cleanup',
    'stray_report': 'unowned storage report',
    'waits': 'wait triggers',
}

if extension.available('update_freeze'):
    LOOP_ESSENTIAL['update_freeze'] = False
    LOOP_NAMES['update_freeze'] = 'Signed updater status'


def boot_time():
    if host_os.linux():
        match = re.search(r'^btime (\d+)$', host_os.proc_text('stat'), re.MULTILINE)
        return int(match.group(1)) if match else None
    try:
        output = subprocess.run(['sysctl', '-n', 'kern.boottime'], capture_output=True, text=True,
                                check=True, timeout=1).stdout
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None
    match = re.search(r'\bsec\s*=\s*(\d+)', output)
    return int(match.group(1)) if match else None


def last_check(run):
    """When a start last decided the startup notice."""
    if not isinstance(run, dict):
        return None
    return run.get('checked')


async def pause(wake, seconds):
    try:
        await asyncio.wait_for(wake.wait(), seconds)
    except asyncio.TimeoutError:
        pass
    wake.clear()


SHUTDOWN_SECONDS = 15


class _Restart(Exception):
    """Leave run so launchd can start the service again."""


class _TaskFailed(RuntimeError):
    """A supervised loop exhausted its restart attempts."""


class Service:
    def __init__(self, store, telegram, runner, project_root: Path, pair_only=False,
                 coordinator_model=None, session_factory=None, vault=None, *, code=None):
        self.store = store
        self.loop_names = dict(LOOP_NAMES)
        if store.code_requests is not None:
            self.loop_names.update(store.code_requests.loop_names())
        from .host_logs import HostLogs
        self.host_logs = HostLogs(store)
        self.code = code
        self.vault = vault
        store.vault = vault
        self.bot_identity_retry = 0
        self.telegram = telegram
        self.runner = runner
        self.project_root = project_root
        self.pair_only = pair_only
        self.coordinator_model = coordinator_model or default_coordinator_model(store)
        self.session_factory = session_factory or parent.start
        self.sessions = {}
        self.wound_down = set()
        self.parent_retry = {}
        self.parent_rechecks = {}
        self.last_turn = {}
        self.restart_messages = {}
        self.restart_reason = None
        self._delivery_lock = None
        self.on_restart = None
        self.feed_tasks = {}
        self.audio_tasks = {}
        self.chat_scratch_tasks = {}
        self.audio_slots = None
        self.audio_barriers = {}
        self.worker_tasks = {}
        self.worker_reattach_retries = {}
        self.control_tasks = {}
        self.stop_tasks = {}
        self.wakes = {}
        self.task_restart_limit = TASK_RESTART_LIMIT
        self.task_restart_backoff = TASK_RESTART_BACKOFF
        self.accounts = AccountBroker(store)
        self.extension = getattr(runner, 'extension', None) or extension.active()
        self.extension.bind(self)
        if hasattr(runner, 'account_broker'):
            runner.account_broker = self.accounts
        self.codex_accounts = CodexBroker(store)
        self.workers = WorkerPool(store, runner, self.accounts, vault, self.codex_accounts)
        from .lifecycle import JobLifecycle
        self.jobs = JobLifecycle(self)
        self.network_available = True
        binaries = getattr(runner, 'binaries', {})
        from .muse_accounts import binary as muse_binary
        self.signins = SignIns(store, binaries.get('claude', 'claude'), codex_binary=binaries.get('codex', 'codex'),
                              muse_binary=lambda: muse_binary(store))
        from .updates import Updater
        self.updater = Updater(self)
        with store.db:
            store.put('pair_only', self.pair_only)
            store.put('forced_pair_only', pair_only)
            if store.get('accounts') is None:
                store.put('accounts', DEFAULT_ACCOUNTS)


    @property
    def network_available(self):
        return getattr(self.store, 'network_available', True)

    @network_available.setter
    def network_available(self, value):
        self.store.network_available = value

    def wake(self, name):
        if not hasattr(self, 'wakes'):
            self.wakes = {}
        if name not in self.wakes:
            self.wakes[name] = asyncio.Event()
        return self.wakes[name]

    def chat_settled(self, topic, callbacks=()):
        session = self.sessions.get(topic)
        if (session is not None and not session.closed and not session.idle()
                or self.store.get('topic_lifecycle:' + topic)):
            return False
        pending = self.store.db.execute("SELECT id FROM messages WHERE topic=? AND delivered IN ('pending','sending')", (topic,))
        if any(row['id'] not in callbacks for row in pending):
            return False
        return not any(not task.done() for message_id, task in self.feed_tasks.items()
                       if self.store.db.execute('SELECT topic FROM messages WHERE id=?', (message_id,)).fetchone()[0] == topic)

    @property
    def delivery_lock(self):
        if self._delivery_lock is None:
            self._delivery_lock = asyncio.Lock()
        return self._delivery_lock

    def home_topic(self):
        return self.store.get('coordinator_home_topic')

    @property
    def session(self):
        return self.sessions.get(self.home_topic())

    def session_key(self, topic):
        main = self.store.get('coordinator_session_topic') or self.home_topic()
        return 'coordinator' if topic == main else 'coordinator:' + topic

    def parent_available(self, topic=None):
        runner = self.runner if isinstance(self.runner, ProviderRunner) else None
        return parent.available(self.store, self.accounts, self.codex_accounts, runner,
                                self.session_key(topic or self.home_topic()))

    def parent_pause_key(self, topic):
        key = self.session_key(topic)
        return key + '_selection' if self.store.get(key + '_selection') else 'coordinator_account'

    def parent_owner_cursor(self, topic):
        return self.store.db.execute("SELECT coalesce(max(id),0) FROM messages WHERE topic=? AND kind='owner'",
                                     (topic,)).fetchone()[0]

    async def parent_recheck(self, topic, pause_key, provider):
        name = 'parent-recheck:%s' % provider
        if provider == 'opencode':
            from . import opencode_accounts
            name += ':%s' % opencode_accounts.active(self.store)
        task = next((task for task in self.parent_rechecks.values()
                     if task.get_name() == name and not task.done()), None)
        if task is None:
            task = asyncio.create_task(parent.retry_paused(self.store, pause_key), name=name)
            task.add_done_callback(lambda _: self.wake('feed').set())
        self.parent_rechecks[topic] = task
        await asyncio.wait({task}, timeout=PARENT_RECHECK_WAIT)
        return task.done()

    def parent_pause_unknown_reset(self, provider, alias):
        if provider != 'opencode':
            return False
        from . import opencode_accounts, opencode_subscription
        account = opencode_accounts.accounts(self.store).get(alias)
        block = (self.store.get('opencode_account_blocks', {}) or {}).get(alias) or {}
        return account is not None and not opencode_subscription.is_subscription(account) and block.get('reset_known') is False

    def paused_topic(self, topic, key='coordinator_account'):
        topic = self.home_topic()
        channel = self.store.topic(topic) if topic else None
        notice = self.store.get(key + '_notice')
        if not isinstance(notice, dict) or not channel or not channel['enabled'] or topic in notice['topics']:
            return
        provider, alias, reset, reason = notice['key']
        windows = (limit_windows(self.codex_accounts.snapshot(alias).get('usage') or {}, out=True)
                   if provider == 'codex' and alias and reason == 'out_of_usage' else
                   {'limit': None if self.parent_pause_unknown_reset(provider, alias) else reset})
        with self.store.db:
            if reason in ('unavailable', 'out_of_usage', 'rate_limited', 'out_of_credit', 'usage_limited'):
                queued = queue_limit_notice(self.store, provider.title(), alias, windows, notice['text'],
                                            reset_tolerance=30 * 60 if provider == 'codex' else 59)
            else:
                self.store.enqueue_report(topic, notice['text'])
                queued = True
            if queued:
                self.store.put(key + '_notice', dict(notice, topics=[topic]))

    def pause_parent(self, topic, error):
        key = self.parent_pause_key(topic)
        picked = key != 'coordinator_account'
        reset = (datetime.fromtimestamp(error.reset_at).astimezone().strftime('%Y-%m-%d %H:%M %Z')
                 if error.reset_at else None)
        reason = 'unavailable'
        if error.provider == 'opencode':
            from . import opencode_accounts, opencode_subscription
            alias = opencode_accounts.active(self.store)
            account = opencode_accounts.accounts(self.store).get(alias) or {}
            block = (self.store.get('opencode_account_blocks', {}) or {}).get(alias) or {}
            reason = block.get('reason', 'auth')
            if self.store.get('opencode_enabled', False) is not True:
                reason = 'off'
                text = 'This chat is paused: OpenCode is off. Turn on delegation.opencode to continue.'
            elif alias is None:
                reason = 'no_account'
                text = 'This chat is paused: no OpenCode account is selected. Select one with opencode.use to continue.'
            elif opencode_subscription.is_subscription(account):
                text = ("This chat is paused: OpenCode subscription account %s is out of usage until %s. "
                        "I'll continue at the reset.") % (alias, reset)
            elif reason == 'rate_limited':
                text = ("This chat is paused: the host of OpenCode account %s is rate limited. "
                        "I'll try again at %s.") % (alias, reset)
            elif reason == 'out_of_credit':
                text = ("This chat is paused: OpenCode account %s is out of credit at its host. Add credit there. "
                        "I'll check again every five minutes.") % alias
            elif reason == 'usage_limited':
                text = ('This chat is paused: The provider rejected this model because its weekly included input token '
                        'limit is exceeded. Open your host subscription settings and enable paid balance use after limits, '
                        'if available, or wait for the weekly reset. '
                        + ("I'll continue at %s." % reset if block.get('reset_known') and reset else
                           "I'll check again every five minutes."))
            else:
                text = ("This chat is paused: the key of OpenCode account %s failed. Fix the key file. "
                        "I'll check again every five minutes.") % alias
            text += ' Torii does not move this chat to another agent. Your messages are saved.'
        elif error.provider == 'codex' and error.alias and not self.codex_accounts.signed_in(error.alias):
            reason = 'signed_out_or_disabled'
            text = ('The main chat is paused: ChatGPT account %s is signed out or turned off. '
                    'Add it again or pick another account with /accounts. Your messages are saved.') % (
                    self.codex_accounts.label(error.alias))
        elif error.provider == 'codex' and self.codex_accounts.exhausted(error.alias, parent=True):
            reason = 'out_of_usage'
            text = ('The main chat is paused: ChatGPT account %s is out of usage%s. '
                    'Torii does not switch ChatGPT accounts on its own. Pick another account with /accounts '
                    '%s Your messages are saved.') % (
                    self.codex_accounts.label(error.alias), ' until ' + reset if reset else '',
                    "to continue now, or I'll continue at the reset." if reset else 'to continue.')
        elif error.provider == 'claude':
            reset = datetime.fromtimestamp(error.reset_at, timezone.utc).isoformat() if error.reset_at else None
            text = ('No Claude account is available.' +
                    (' Earliest reset: ' + reset + '. I will retry then.' if reset else '') +
                    ' Your messages are saved.')
        else:
            text = ('The main chat is paused: no Claude or ChatGPT account can take work now.' +
                    (' Earliest reset: ' + reset + '.' if reset else '') +
                    ' Add or pick an account with /accounts to continue sooner. Your messages are saved.')
        if picked:
            text = text.replace('The main chat is paused:', 'This chat is paused:')
        notice_key = [error.provider, error.alias, error.reset_at, reason]
        notice = self.store.get(key + '_notice')
        previous = notice.get('key') if isinstance(notice, dict) else None
        same_wait = bool(previous and previous[:2] == notice_key[:2] and previous[3:] == notice_key[3:] and (
            self.parent_pause_unknown_reset(error.provider, error.alias) or previous[2] == error.reset_at or
            previous[2] is not None and error.reset_at is not None and
            abs(previous[2] - error.reset_at) < 60))
        with self.store.db:
            self.store.put(key + '_retry_at', error.reset_at or time.time() + 60)
            owner_key = self.session_key(topic) + '_pause_owner'
            if not same_wait or self.store.get(owner_key) is None:
                self.store.put(owner_key, self.parent_owner_cursor(topic))
            if not same_wait:
                problems.record(self.store, 'accounts', 'unavailable', 'coordinator provider=%s reset_at=%s' % (
                    error.provider, error.reset_at), topic=topic if picked else None)
                self.store.put(key + '_notice', {'key': notice_key, 'text': text, 'topics': []})
            self.paused_topic(topic, key)

    async def start_session(self, topic=None, *, reattach_only=False):
        topic = topic or self.home_topic()
        if topic is None:
            return None
        from .topics import lifecycle
        if lifecycle(self.store, topic):
            return None
        from .setup_flow import blocked
        if self.pair_only or blocked(self.store):
            return None
        config = self.store.topic(topic)
        if (not config or config['chat'] == 0
                or (topic == self.store.get('control_topic') and not config['cwd'])):
            return None
        enabled = topic == self.home_topic() or config['enabled']
        if not enabled and not (reattach_only and self.store.get(self.session_key(topic) + '_host')):
            return None
        previous_session = self.sessions.get(topic)
        if previous_session and not previous_session.closed and getattr(previous_session, '_parent_retiring', False):
            return None
        if previous_session and not previous_session.closed and not getattr(previous_session, 'rejected', False):
            return previous_session
        if self.parent_retry.get(topic, 0) > time.monotonic():
            return None
        if previous_session:
            await previous_session.wait_closed()
        if not hasattr(self, 'parent_rechecks'):
            self.parent_rechecks = {}
        recheck = self.parent_rechecks.get(topic)
        if recheck is not None and not recheck.done():
            return None
        key = self.session_key(topic)
        pause_key = self.parent_pause_key(topic)
        retry_at = self.store.get(pause_key + '_retry_at')
        owner_key = key + '_pause_owner'
        if retry_at and self.store.get(owner_key) is None:
            recovery = self.store.get(key + '_opencode_recovery') or {}
            with self.store.db:
                self.store.put(owner_key, recovery.get('owner', self.parent_owner_cursor(topic)))
        owner = self.parent_owner_cursor(topic) if retry_at else 0
        owner_retry = enabled and owner > (self.store.get(owner_key) or 0)
        notice = self.store.get(pause_key + '_notice') or {}
        identity = notice.get('key') or [None, None]
        periodic_retry = (enabled and retry_at and retry_at <= time.time() and
                          self.parent_pause_unknown_reset(*identity[:2]))
        retry_pause = recheck is not None or owner_retry or periodic_retry
        if retry_pause and recheck is None:
            with self.store.db:
                self.store.put(owner_key, owner)
                self.store.put(pause_key + '_retry_at', None)
            if not await self.parent_recheck(topic, pause_key, identity[0]):
                return None
        if retry_pause:
            retry_at = None
        if enabled and retry_at and retry_at > time.time() and not self.parent_available(topic):
            self.paused_topic(topic, pause_key)
            self.parent_retry.setdefault(topic, 0)
            return None
        resumed = bool(self.store.get(key + '_session'))
        rotated = bool(self.store.get('coordinator_rotation_from') or
                       previous_session and getattr(previous_session, 'rejected', False))
        visible = [dict(item) for item in self.store.topics() if item['enabled'] and
                   (topic == self.home_topic() or item['id'] == topic)]
        details = [{'id': item['id'], 'name': item['name'], 'cwd': item['cwd']} for item in visible]
        instructions = (COORDINATOR_POLICY + '\nRegistered channels: ' +
                        json.dumps(details, separators=(',', ':')) + '\nConnected agents: ' +
                        ', '.join(parent.connected_agents(self.store)))
        if self.store.get('close_clears_space', False) is True:
            from .close_saves import scratch
            instructions += '\nChat scratch: ' + str(scratch(self.store, topic=topic, create=True))
        model = self.store.get('coordinator_model') or self.coordinator_model
        try:
            if retry_pause:
                self.parent_rechecks.pop(topic).result()
            session = await self.session_factory(self.store, self.runner, self.project_root,
                                                 model, instructions, topic=topic)
        except Exception as error:
            if not enabled:
                from .host import HostClient
                host = self.store.get(key + '_host')
                if host:
                    try:
                        await HostClient(self.store.directory / 'hosts' / host['id']).stop()
                    except Exception as stop_error:
                        logger.warning('parent stop failed topic=%s reason=disabled error=%s',
                                       topic, type(stop_error).__name__)
                        return None
                    self.store.put(key + '_host', None)
                    logger.info('parent stopped topic=%s reason=disabled state=unavailable error=%s',
                                topic, type(error).__name__)
                return None
            if isinstance(error, AccountsUnavailable):
                logger.warning('no parent account available provider=%s reset_at=%s', error.provider, error.reset_at)
                self.pause_parent(topic, error)
                self.parent_retry.setdefault(topic, 0)
                return None
            self.parent_retry[topic] = time.monotonic() + PARENT_RETRY
            problems.record(self.store, 'coordinator', 'start-failed', internal_detail(error), level=logging.ERROR,
                            topic=topic)
            return None
        if session is None:
            return None
        if not enabled:
            self.sessions[topic] = session
            return session
        with self.store.db:
            self.store.put(pause_key + '_retry_at', None)
            notice = self.store.get(pause_key + '_notice')
            notice_key = notice.get('key') if isinstance(notice, dict) else None
            identity = [getattr(session, 'provider', None), getattr(session, 'account_alias', None)]
            if not (notice_key and notice_key[:2] == identity and (
                    retry_pause or self.parent_pause_unknown_reset(*identity) or
                    identity[0] == 'opencode' and self.store.get(key + '_opencode_recovery'))):
                self.store.put(pause_key + '_notice', None)
        idle = topic in self.wound_down
        if not hasattr(session, 'usage_policy'):
            session.usage_policy = chat_policy(self.store, key)
        fallback = getattr(session, 'resume_failed', False)
        switched = getattr(session, 'switched', False)
        self.sessions[topic] = session
        self.wound_down.discard(topic)
        self.parent_retry.pop(topic, None)
        self.last_turn[topic] = time.monotonic()
        self.confirm_from_transcript(topic, session)
        self.store.requeue_messages([row['id'] for row in self.store.db.execute(
            "SELECT id FROM messages WHERE topic=? AND kind='secret_filled' AND delivered='uncertain'",
            (topic,))])
        from . import opencode_recovery
        recovery = opencode_recovery.state(self.store, key) if getattr(session, 'provider', None) == 'opencode' else {}
        if recovery:
            if recovery.get('notice'):
                self.restart_messages[topic] = recovery['notice']
        elif (resumed and not fallback and not switched and not rotated and not idle and
                getattr(getattr(session, 'process', None), 'returncode', None) is None):
            tasks = self.store.tasks_list(status='open', topic=None if topic == self.home_topic() else topic)
            uncertain = [dict(row) for row in self.store.db.execute(
                "SELECT id,topic,kind,text FROM messages WHERE delivered='uncertain' AND kind!='restarted' AND topic=? ORDER BY id", (topic,))]
            self.replace_notice(topic, CoordinatorSession.restarted_prompt(
                tasks, uncertain, store=self.store))
        elif fallback or switched or (not resumed and topic != self.home_topic()):
            tasks = self.store.tasks_list(status='open', topic=None if topic == self.home_topic() else topic)
            recent = [dict(row) for row in self.store.db.execute(
                "SELECT kind,text FROM messages WHERE topic=? AND kind!='restarted' AND delivered!='pending' "
                "AND (receipt IS NULL OR receipt!='superseded') "
                "ORDER BY id DESC LIMIT 5", (topic,))]
            messages = [dict(item, text=item['text'][:500]) for item in reversed(recent)]
            preface = ''
            if switched:
                change = session.switch
                labels = {'claude': 'Claude', 'codex': 'ChatGPT', 'opencode': 'OpenCode'}
                preface = ('This chat moved from %s to %s because %s. '
                           'Your earlier conversation is not in this context.\n\n') % (
                           labels[change['from']], labels[change['to']], change['reason'])
            encoded = json.dumps(messages)
            while len(encoded) > parent_state.RESTART_MESSAGE_CHARS and messages:
                messages.pop(0)
                encoded = json.dumps(messages)
            prefix = preface + 'Channel continuity: Open jobs: '
            suffix = '. Recent messages: ' + encoded
            summary = parent_state.jobs_summary(
                self.store, tasks, parent_state.RESTART_NOTICE_CHARS - len(prefix) - len(suffix))
            self.replace_notice(topic, prefix + summary + suffix if preface or tasks or recent else None)
            if switched:
                with self.store.db:
                    note = 'This chat now runs on %s (%s).' % (labels[change['to']], change['reason'])
                    selection = parent_state.effective_agent(self.store, key)['selection']
                    if change['to'] == 'codex' and not selection:
                        note += ' It returns to Claude at the next restart after Claude recovers.'
                    elif change['to'] == 'codex' and selection.get('provider') == 'claude':
                        note += ' It returns to your selected Claude model after Claude recovers and current work settles.'
                    if 'out of usage' not in change['reason'] and change['reason'] != 'Claude can take work again':
                        self.store.enqueue_report(topic, note)
                    self.store.put(key + '_switch', None)
        return session

    def replace_notice(self, topic, text):
        """Retire this topic's unsent notices, then queue the new one, so a parent never reads older state last."""
        with self.store.db:
            self.store.db.execute("UPDATE messages SET delivered='received',receipt='superseded',updated=? "
                                  "WHERE topic=? AND kind='restarted' AND delivered='pending'", (time.time(), topic))
        if text:
            self.restart_messages[topic] = self.store.message_save(topic, 'restarted', text)['id']

    def confirm_from_transcript(self, topic, session):
        path = getattr(session, 'transcript_path', None)
        scope = None if topic == self.home_topic() else topic
        rows = {message_uuid(row['id']): row['id'] for row in self.store.db.execute(
            "SELECT id FROM messages WHERE delivered='uncertain' AND (? IS NULL OR topic=?)", (scope, scope))}
        if not rows or not path or not Path(path).is_file():
            return
        scanner = getattr(session, 'receipt_rows', None) or getattr(
            session, 'transcript_receipts', CoordinatorSession.transcript_receipts)
        found = scanner(path, rows.values())
        self.store.messages_confirm(found, 'transcript')
        logger.info('uncertain messages confirmed from transcript confirmed=%s remaining=%s',
                    len(found), len(rows) - len(found))

    def start_audio(self, row):
        if row['kind'] not in ('owner', 'forwarded') or not row['images']:
            return None
        if row['id'] in self.audio_tasks:
            return self.audio_tasks[row['id']]
        attachments = self.store.db.execute('SELECT * FROM attachments WHERE topic=? AND message=?',
                                            (row['topic'], row['telegram_message'])).fetchall()
        if not any(is_audio(dict(attachment)) for attachment in attachments):
            return None
        if self.audio_slots is None:
            self.audio_slots = TranscriptionSlots()
        task = asyncio.create_task(self._prepare_audio(row))
        self.audio_tasks[row['id']] = task
        return task

    def resume_audio(self):
        for row in self.store.messages_pending():
            self.start_audio(row)

    async def _prepare_audio(self, row):
        attachments = await prepare_attachments(self.store, self.telegram, row)
        await prepare_transcripts(self.store, row, attachments, self.audio_slots)
        return attachments

    async def _prepare_message(self, row, session=None, batch=False):
        text = row['text']
        if row['kind'] in ('owner', 'forwarded') and row['images']:
            try:
                audio = self.start_audio(row)
                attachments = (await asyncio.shield(audio) if audio is not None else
                               await prepare_attachments(self.store, self.telegram, row))
            except MediaError as error:
                with self.store.db:
                    problems.record(self.store, 'attachment', 'unavailable', str(error), topic=row['topic'],
                                    message=row['id'])
                    self.store.enqueue_report(row['topic'], str(error), reply_to=row['telegram_message'])
                self.store.message_delivered(row['id'], 'uncertain')
                return None
            text += attachment_instructions(attachments, coordinator=True, provider=getattr(session, 'provider', 'claude'),
                                            forwarded=row['kind'] == 'forwarded')
        reply_first = REPLY_FIRST if row['kind'] == 'owner' else ''
        if row['kind'] == 'owner':
            from .waits import owner_header
            row = dict(row, waiting=self.store.wait_hook('header-failed', owner_header, self.store, row['topic']))
        identity = intake.identity(self.store, row) if batch else ''
        return message_tag(self.store.topic(row['topic']), row) + identity + reply_first + text

    async def _send_message(self, row, prompt, session, batch=None):
        message_ids = [item['id'] for item in batch] if batch is not None else [row['id']]
        try:
            claimed = (self.store.message_batch_sending(message_ids) if batch is not None else
                       self.store.message_sending(row['id']))
            if not claimed:
                return
            current_policy = chat_policy(self.store, session.key)
            if getattr(session, 'usage_policy', None) != current_policy:
                prompt += '\n\nCurrent usage policy:\n' + current_policy
            receipt = await session.send(message_uuid(row['id']), prompt, row_id=row['id'])
            if getattr(session, 'rejected', False) and receipt != 'closed':
                if batch is None:
                    return 'closed'
                receipt = 'closed'
            if receipt == 'closed':
                with self.store.db:
                    self.store.db.executemany(
                        "UPDATE messages SET delivered='pending',updated=? WHERE id=? AND delivered='sent'",
                        [(time.time(), message_id) for message_id in message_ids])
                return 'closed'
            if receipt == 'unsupported' and getattr(session, 'provider', None) == 'codex':
                self.store.requeue_messages(message_ids)
                problems.record(self.store, 'coordinator', 'steer-unsupported', None, topic=row['topic'],
                                message=row['id'])
                return receipt
            self.store.messages_delivered(message_ids, 'written' if receipt == 'received' else 'uncertain')
            if receipt == 'received':
                session.usage_policy = current_policy
            logger.info('coordinator steer message=%s receipt=%s after=%.2f', row['id'], receipt,
                        time.time() - row['created'])
            if receipt != 'received':
                problems.record(self.store, 'coordinator', 'steer-' + receipt, None, topic=row['topic'],
                                message=row['id'])
            return receipt
        except Exception as error:
            self.store.messages_delivered(message_ids, 'uncertain')
            problems.record(self.store, 'coordinator', 'steer-failed', internal_detail(error), topic=row['topic'],
                            message=row['id'])
            raise

    async def _deliver_audio(self, row, session, previous=None):
        if previous is not None:
            await asyncio.wait([previous])
        prompt = await self._prepare_message(row, session)
        if prompt is not None:
            self.last_turn[row['topic']] = time.monotonic()
            await self._send_message(row, prompt, session)

    def _intake_topic(self, row, enabled):
        return (self.home_topic() if row['topic'] not in enabled and row['kind'] == 'worker_result'
                else row['topic'])

    async def _deliver_batch(self, rows, session, topic, previous=None):
        if previous is not None:
            await asyncio.wait([previous])
        prepared = []
        size = 0
        for row in rows:
            prompt = await self._prepare_message(row, session, batch=True)
            if prompt is None:
                continue
            policy = chat_policy(self.store, session.key)
            extra = '\n\nCurrent usage policy:\n' + policy if getattr(session, 'usage_policy', None) != policy else ''
            added = len(prompt) + (2 if prepared else 0)
            if prepared and rows[0].get('batch_id') is None and size + added + len(extra) > intake.MAX_CHARACTERS:
                break
            prepared.append((row, prompt))
            size += added
        if not prepared:
            return
        pending = {row['id']: row for row in self.store.reconcile_message_batches()}
        prepared = [(pending[row['id']], text) for row, text in prepared if row['id'] in pending]
        if not prepared:
            return
        batch = [row for row, text in prepared]
        anchor = batch[0].get('batch_id') or batch[0]['id']
        first = next((row for row in batch if row['id'] == anchor), batch[0])
        self.last_turn[topic] = time.monotonic()
        await self._send_message(first, '\n\n'.join(text for row, text in prepared), session, batch=batch)

    async def _feed_batches(self, enabled, existing_only=False):
        self._finish_feed_tasks()
        pending = [row for row in self.store.reconcile_message_batches()
                   if not existing_only or row.get('batch_id') is not None]
        routes = {}
        for row in pending:
            if row.get('batch_id') is not None:
                routes.setdefault(row['batch_id'], set()).update((row['topic'], self._intake_topic(row, enabled)))
        rerouted = {batch for batch, destinations in routes.items() if len(destinations) > 1}

        def destination(row):
            return self.home_topic() if row.get('batch_id') in rerouted else self._intake_topic(row, enabled)

        topics = dict.fromkeys(destination(row) for row in pending)
        started = False
        for topic in topics:
            session = await self.start_session(topic)
            if session is None:
                continue
            pending = self.store.reconcile_message_batches()
            notice = self.restart_messages.pop(topic, None)
            if notice is not None:
                restart = next((item for item in pending if item['id'] == notice), None)
                if restart:
                    prompt = await self._prepare_message(restart, session)
                    if prompt is not None and await self._send_message(restart, prompt, session) == 'closed':
                        self.restart_messages[topic] = notice
                        continue
            rows = [row for row in self.store.reconcile_message_batches()
                    if destination(row) == topic and row['id'] not in self.feed_tasks
                    and (not existing_only or row.get('batch_id') is not None)]
            if getattr(session, 'provider', None) == 'opencode':
                from . import opencode_recovery
                recovery = opencode_recovery.state(self.store, session.key)
                if recovery.get('phase') == 'catchup':
                    rows = [row for row in rows if row['id'] == recovery.get('notice')]
            if not rows:
                continue
            rows.sort(key=intake.priority)
            anchor = rows[0].get('batch_id')
            rows = [row for row in rows if row.get('batch_id') == anchor]
            if anchor is not None:
                rows.sort(key=lambda row: row['id'] != anchor)
            rows = rows[:intake.MAX_ITEMS]
            barrier = self.audio_barriers.get(topic)
            task = asyncio.create_task(self._deliver_batch(rows, session, topic, barrier))
            for row in rows:
                self.feed_tasks[row['id']] = task
            if barrier is not None or any(row['id'] in self.audio_tasks for row in rows):
                self.audio_barriers[topic] = task
            started = True
        return started

    def _finish_feed_tasks(self):
        for message_id, task in list(self.feed_tasks.items()):
            if task.done():
                del self.feed_tasks[message_id]
                if self.audio_tasks.get(message_id) is not None and self.audio_tasks[message_id].done():
                    del self.audio_tasks[message_id]
                task.result()

    async def _clear_chat_scratch(self, topic):
        from .close_saves import clear_chat_scratch
        try:
            await clear_chat_scratch(self.store, topic)
        except Exception as error:
            problems.record(self.store, 'workspaces', 'chat-scratch-delete-failed',
                            internal_detail(error), topic=topic)
        finally:
            self.chat_scratch_tasks.pop(topic, None)

    async def feed_once(self):
        from .setup_flow import blocked
        self.resume_audio()
        if self.pair_only or blocked(self.store):
            return False
        enabled = {topic['id'] for topic in self.store.topics() if topic['enabled']} | {self.home_topic()}
        for topic in list(dict.fromkeys([*self.sessions, *(topic for topic in self.parent_retry if topic in enabled)])):
            from .topics import lifecycle
            if lifecycle(self.store, topic):
                continue
            session = self.sessions.get(topic)
            if session is None or session.closed:
                if topic not in self.wound_down:
                    await self.start_session(topic)
            elif topic not in enabled:
                uncertain = getattr(session, '_recovery_uncertain', False)
                if (uncertain or session.idle()) and not any(
                        not task.done() for message_id, task in self.feed_tasks.items()
                        if self.store.db.execute('SELECT topic FROM messages WHERE id=?',
                                                 (message_id,)).fetchone()[0] == topic):
                    await session.stop()
                    self.wound_down.add(topic)
                    logger.info('parent stopped topic=%s reason=disabled%s', topic,
                                ' state=unavailable' if uncertain else '')
            elif getattr(session, '_parent_retiring', False):
                continue
            elif ((parent_state.effective_agent(self.store, self.session_key(topic))['selection'] or
                   (self.store.get(self.session_key(topic) + '_host') or {}).get('picked')) and
                  parent_state.selection_changed(session) and session.idle() and
                  not any(not task.done() for message_id, task in self.feed_tasks.items()
                          if self.store.db.execute('SELECT topic FROM messages WHERE id=?',
                                                   (message_id,)).fetchone()[0] == topic)):
                session.relaunch()
            elif (not getattr(session, 'busy', False) and
                    not getattr(getattr(session, 'protocol', None), 'background', None) and
                    time.monotonic() - max(self.last_turn.get(topic, 0),
                                           getattr(session, 'last_turn_at', 0)) >= PARENT_IDLE and
                    not any(not task.done() for message_id, task in self.feed_tasks.items()
                            if self.store.db.execute('SELECT topic FROM messages WHERE id=?',
                                                     (message_id,)).fetchone()[0] == topic)):
                await session.stop()
                if self.store.get('close_clears_space', False) is True and topic not in self.chat_scratch_tasks:
                    self.chat_scratch_tasks[topic] = asyncio.create_task(self._clear_chat_scratch(topic))
                self.wound_down.add(topic)
        started = False
        previous = {topic: task for topic, task in self.audio_barriers.items() if not task.done()}
        self.audio_barriers = previous
        batching = self.store.get('coordinator_intake_batching', False) is True
        pending = self.store.messages_pending()
        if batching or any(row.get('batch_id') is not None for row in pending):
            started = await self._feed_batches(enabled, existing_only=not batching)
            if not batching:
                pending = self.store.messages_pending()
        for row in (() if batching else pending):
            if row.get('batch_id') is not None:
                continue
            if row['id'] in self.feed_tasks:
                continue
            topic = row['topic']
            if topic not in enabled and row['kind'] == 'worker_result':
                topic = self.home_topic()
            session = await self.start_session(topic)
            if session is None:
                continue
            notice = self.restart_messages.pop(topic, None)
            if notice is not None and notice != row['id']:
                restart = next((item for item in self.store.messages_pending() if item['id'] == notice), None)
                if restart:
                    prompt = await self._prepare_message(restart, session)
                    if prompt is not None and await self._send_message(restart, prompt, session) == 'closed':
                        self.restart_messages[topic] = notice
                        continue
            if getattr(session, 'provider', None) == 'opencode':
                from . import opencode_recovery
                recovery = opencode_recovery.state(self.store, session.key)
                if recovery.get('phase') == 'catchup' and row['id'] != recovery.get('notice'):
                    continue
            if row['id'] in self.audio_tasks or topic in previous:
                task = asyncio.create_task(self._deliver_audio(row, session, previous.get(topic)))
                self.feed_tasks[row['id']] = previous[topic] = task
                started = True
                continue
            prompt = await self._prepare_message(row, session)
            if prompt is None:
                continue
            self.last_turn[topic] = time.monotonic()
            task = asyncio.create_task(self._send_message(row, prompt, session))
            self.feed_tasks[row['id']] = task
            started = True
        self._finish_feed_tasks()
        if self.store.get('auto_resume', False) is True:
            for topic in enabled:
                if self.chat_settled(topic):
                    await self.jobs.settled(topic)
        return started

    async def feed(self):
        while True:
            if not await self.feed_once():
                retry_at = self.store.get('coordinator_account_retry_at')
                if retry_at:
                    await pause(self.wake('feed'), min(60, max(0.1, retry_at - time.time())))
                else:
                    await pause(self.wake('feed'), FEED_IDLE)
            else:
                await asyncio.sleep(0)

    async def workers_once(self):
        self.workers.storage.schedule()
        from .setup_flow import blocked
        if self.pair_only or blocked(self.store):
            return False
        await self.jobs.resume_due()
        started = False
        rows = self.store.db.execute(
            "SELECT * FROM workers WHERE status IN ('queued','running','waiting_for_secret','waiting_for_quota') ORDER BY id").fetchall()
        rows = [row for row in rows if row['provider'] in WORKER_PROVIDERS]
        running_hosts = {(row['id'], row['host']) for row in rows if row['status'] == 'running'}
        for key in set(self.worker_reattach_retries) - running_hosts:
            self.worker_reattach_retries.pop(key)
        writing_jobs = {row['task'] for row in rows if row['work'] == 'dev' and
                        (row['status'] == 'running' or
                         (row['id'] in self.worker_tasks and not self.worker_tasks[row['id']].done()))}
        for saved_row in rows:
            row = dict(saved_row)
            topic = self.store.topic(row['topic'])
            from .topics import lifecycle
            if lifecycle(self.store, row['topic']):
                continue
            if topic['chat'] == 0:
                continue
            prior = self.worker_tasks.get(row['id'])
            if prior and prior.done():
                del self.worker_tasks[row['id']]
                prior.result()
            elif prior:
                continue
            if row['status'] in ('waiting_for_quota', 'waiting_for_secret') and \
                    (self.store.task_get(row['task']) or {}).get('status') != 'open':
                with self.store.db:
                    self.store.db.execute("UPDATE workers SET status='interrupted',updated=? "
                                          "WHERE id=? AND status=?", (time.time(), row['id'], row['status']))
                continue
            if row['status'] == 'waiting_for_quota':
                broker = self.workers.broker(row['provider'])
                if row['provider'] == 'opencode':
                    if not await broker.ready(row.get('account_alias'), row['id']):
                        continue
                elif not broker.select():
                    continue
                with self.store.db:
                    self.store.db.execute("UPDATE workers SET status='queued',updated=? "
                                          "WHERE id=? AND status='waiting_for_quota'",
                                          (time.time(), row['id']))
                row['status'] = 'queued'
            if row['status'] == 'waiting_for_secret' and waiting_secrets(self.store, self.store.task_get(row['task'])):
                continue
            if row['status'] == 'running':
                from .host import CleanupFailed, HostClient
                host = HostClient(host_directory(self.store.directory, row))
                state = await host.recover_state()
                key = (row['id'], row['host'])
                unreachable = False
                if state == 'running':
                    attempts, retry_at = self.worker_reattach_retries.get(key, (0, 0))
                    if time.monotonic() < retry_at:
                        continue
                    try:
                        await asyncio.wait_for(host.connect(), WORKER_REATTACH_TIMEOUT)
                        self.worker_reattach_retries.pop(key, None)
                        self.store.wait_hook('worker-reattached-signal-failed', self.store.wait_signal,
                                             'worker.reattached', internal=True)
                    except (OSError, RuntimeError, asyncio.TimeoutError) as error:
                        state = host.state
                        if state == 'running':
                            attempts += 1
                            self.worker_reattach_retries[key] = (attempts, time.monotonic() + WORKER_REATTACH_RETRY)
                            problems.record(self.store, 'worker', 'reattach-unavailable', internal_detail(error), every=60,
                                            topic=row['topic'], task=row['task'], worker=row['id'])
                            if attempts < WORKER_REATTACH_ATTEMPTS:
                                continue
                            problems.record(self.store, 'worker', 'reattach-lost',
                                            'Host socket unavailable after %d attempts; resuming after cleanup.' % attempts,
                                            topic=row['topic'], task=row['task'], worker=row['id'])
                            unreachable = True
                self.worker_reattach_retries.pop(key, None)
                if state == 'dead' or unreachable:
                    host.on_problem = lambda area, code, detail: problems.record(
                        self.store, area, code, detail, topic=row['topic'], task=row['task'], worker=row['id'])
                    try:
                        await host.stop()
                    except CleanupFailed as error:
                        self.workers.cleanup_failed(row, error)
                        continue
                    except (OSError, RuntimeError) as error:
                        problems.record(self.store, 'worker', 'stop-failed', internal_detail(error), every=60,
                                        topic=row['topic'], task=row['task'], worker=row['id'])
                        if unreachable:
                            self.workers.cleanup_failed(row, error)
                        continue
                    if self.workers.requeue_lost_host(row, stopped=unreachable):
                        row.update(status='queued', pid=None, last_seq=0)
                    else:
                        self.store.worker_complete(row['id'], {'success': False, 'session_id': row['session'],
                            'error': 'The provider host stopped without an exit record.',
                            'failure_code': 'interrupted', 'pid': row['pid']}, 'interrupted')
                        started = True
                        continue
            if row['status'] != 'running' and row['work'] == 'dev' and row['task'] in writing_jobs:
                continue
            task = asyncio.create_task(self.workers.run(dict(row), self.store.topic(row['topic'])))
            self.worker_tasks[row['id']] = task
            if row['work'] == 'dev':
                writing_jobs.add(row['task'])
            started = True
        for worker_id, task in list(self.worker_tasks.items()):
            if task.done():
                del self.worker_tasks[worker_id]
                task.result()
        return started

    async def workers_loop(self):
        while True:
            if not await self.workers_once():
                await asyncio.sleep(0.1)
            else:
                await asyncio.sleep(0)

    async def controls_once(self):
        from . import close_saves
        from .topics import release_messages
        for pending in (self.control_tasks, self.stop_tasks):
            for request_id, task in list(pending.items()):
                if task.done():
                    del pending[request_id]
                    task.result()
        release_messages(self.store)
        space_enabled = self.store.get('close_clears_space', False) is True
        if getattr(self, '_close_space_enabled', False) != space_enabled:
            self._close_space_enabled = space_enabled
            self.wake('worktree_cleanup').set()
        if ((self.store.get('closed_copy_delete', False) is True
                and self.store.get('closed_copy_cleanup_pending', {})) or
                space_enabled and close_saves.pending(self.store)):
            self.wake('worktree_cleanup').set()
        requests = self.store.service_requests_pending()
        settled = False
        for request in requests:
            params = json.loads(request['params'])
            op = request['op']
            if op == '_cleanup.sessions':
                from .session_storage import storage_for
                storage_for(self.store).changed.set()
                with self.store.db:
                    self.store.service_request_settle(request['id'], 'done')
                settled = True
                continue
            if op == '_cleanup.stray_report':
                self.wake('stray_report').set()
                self.store.service_request_settle(request['id'], 'done')
                settled = True
                continue
            if op == 'waits.changed':
                self.wake('waits').set()
                await self.updater.waits_changed(armed=True)
                with self.store.db:
                    self.store.service_request_settle(request['id'], 'done')
                settled = True
                continue
            if op in ('topic.create', 'topic.close'):
                from .topics import advance
                await advance(self, request)
                settled = True
                continue
            if op == 'topic.rename':
                from .topics import rename
                await rename(self, request)
                settled = True
                continue
            if op == 'tldr':
                continue
            if op == 'service.restart':
                from .updates import KEY, watch
                update = self.store.get(KEY) or {}
                if update.get('phase') not in watch.TERMINAL and await asyncio.to_thread(watch.watchdog_alive, update):
                    with self.store.db:
                        self.store.service_request_settle(request['id'], 'refused',
                            {'text': 'An update is already in progress. It owns the next restart.'})
                    settled = True
                    continue
                with self.store.db:
                    last = self.store.get('last_restart_v2')
                    if last is not None and time.time() - last < RESTART_GUARD_SECONDS:
                        self.store.service_request_settle(request['id'], 'refused',
                                                          {'text': 'Torii restarted recently. Try again later.'})
                        problems.record(self.store, 'service', 'restart-refused', params['reason'])
                    else:
                        self.store.put('restart_requested_v2',
                                       {'reason': params['reason'].strip(), 'created': request['created']})
                        self.store.service_request_settle(request['id'], 'done')
                        problems.record(self.store, 'service', 'restart-requested', params['reason'])
                settled = True
                continue
            if op == 'secret.revoke':
                self.revoke(request['id'], params['name'])
                continue
            if op == 'secret.rename':
                self.rename(request['id'], params['name'], params['new_name'])
                settled = True
                continue
            if op == 'account.codex_reset':
                from .control_api import Refused
                from .controls import redeem_codex_account
                self.store.service_request_settle(request['id'], 'sending')
                try:
                    result = await redeem_codex_account(self.store, params['_topic'], params['alias'])
                except (Refused, ValueError) as error:
                    result = None
                    notice = str(error)
                except Exception as error:
                    result = None
                    notice = 'Codex banked reset could not finish. Check usage before trying again.'
                    logger.exception('banked reset failed type=%s', type(error).__name__)
                with self.store.db:
                    if result is None:
                        self.store.enqueue_report(params['_topic'], notice)
                    self.store.service_request_settle(request['id'],
                        'done' if result and result.ok else 'refused' if result is None else 'uncertain',
                        {'text': result.text if result else notice})
                settled = True
                continue
            from .host_logs import REQUEST
            if op == REQUEST:
                host_logs = getattr(self, 'host_logs', None)
                if host_logs is not None:
                    host_logs.request(params)
                self.store.service_request_settle(request['id'], 'done')
                settled = True
                continue
            worker = self.workers.get(params['worker'])
            if op != 'workers.stop' and params['worker'] in self.control_tasks:
                continue
            if not worker or worker['task'] is None:
                if op == 'workers.steer':
                    self.refuse_steer(request['id'], params, 'Unknown job worker.')
                else:
                    self.store.service_request_settle(request['id'], 'refused', {'text': 'Unknown job worker.'})
                settled = True
                continue
            if worker['provider'] not in WORKER_PROVIDERS:
                if op == 'workers.steer':
                    self.refuse_steer(request['id'], params, 'Worker provider is not supported.', worker)
                else:
                    self.store.service_request_settle(request['id'], 'refused',
                                                      {'text': 'Worker provider is not supported.'})
                settled = True
                continue
            if worker['status'] in ('queued', 'waiting_for_secret', 'waiting_for_quota'):
                with self.store.db:
                    current = self.workers.get(worker['id'])
                    if current['status'] in ('queued', 'waiting_for_secret', 'waiting_for_quota'):
                        if op == 'workers.steer':
                            prompt = current['prompt'] + '\n\nFollow-up:\n' + params['prompt']
                            if len(prompt) > BRIEF_LIMIT:
                                self.refuse_steer(request['id'], params,
                                                  'The queued brief would exceed %d characters.' % BRIEF_LIMIT,
                                                  worker)
                                settled = True
                                continue
                            self.store.db.execute('UPDATE workers SET prompt=?,updated=? WHERE id=?',
                                                  (prompt, time.time(), worker['id']))
                        elif op == 'workers.stop':
                            self.store.worker_complete(worker['id'], {'success': False,
                                'session_id': worker['session'], 'failure_code': 'owner_stopped',
                                'error': 'The owner stopped this worker.'}, 'interrupted')
                        self.store.service_request_settle(request['id'], 'done')
                        settled = True
                        continue
                    worker = current
            if op == 'workers.stop':
                if worker['status'] != 'running':
                    with self.store.db:
                        self.store.service_request_settle(request['id'], 'refused', {'text': 'Worker is not running.'})
                        self.store.message_save(worker['topic'], 'callback',
                                                'Stop request %d for worker %d was refused: Worker is not running.'
                                                % (request['id'], worker['id']))
                    settled = True
                    continue
                self.store.service_request_settle(request['id'], 'sending')
                try:
                    deferred = await self.workers.stop(worker['id'])
                except (OSError, RuntimeError) as error:
                    problems.record(self.store, 'worker', 'stop-failed', internal_detail(error), topic=worker['topic'],
                                    task=worker['task'], worker=worker['id'])
                    self.store.service_request_settle(request['id'], 'uncertain',
                                                      {'text': 'The provider host is unavailable.'})
                    settled = True
                    continue
                task = self.workers.tasks.get(worker['id'])
                if deferred:
                    self.stop_tasks[request['id']] = asyncio.create_task(self.worker_stopped(request['id'], task))
                else:
                    await self.worker_stopped(request['id'], task)
                settled = True
                continue
            if op == 'workers.steer' and worker['status'] == 'done':
                with self.store.db:
                    current = self.workers.get(worker['id'])
                    task = self.worker_tasks.get(worker['id'])
                    if (can_resume_finished(self.store, current)
                            and (task is None or task.done())):
                        self.store.db.execute("""UPDATE workers SET status='queued',prompt=?,host=?,last_seq=0,
                            fresh=0,pid=NULL,updated=? WHERE id=?""",
                            (params['prompt'], 'worker-%d-resume-%d' % (worker['id'], request['id']),
                             time.time(), worker['id']))
                        self.store.service_request_settle(request['id'], 'done', {'receipt': 'resumed'})
                        settled = True
                        continue
                    worker = current
            control = self.workers.controls.get(worker['id'])
            if not control:
                if worker['status'] != 'running':
                    if op == 'workers.steer':
                        self.refuse_steer(request['id'], params, 'Worker cannot receive input.', worker)
                    else:
                        self.store.service_request_settle(request['id'], 'refused',
                                                          {'text': 'Worker cannot receive input.'})
                    problems.record(self.store, 'worker', 'steer-refused',
                                    'op=%s request=%s' % (op, request['id']),
                                    topic=worker['topic'], task=worker['task'], worker=worker['id'])
                    settled = True
                continue
            self.store.service_request_settle(request['id'], 'sending')
            if worker['provider'] == 'codex':
                self.control_tasks[worker['id']] = asyncio.create_task(self.worker_control(request, params, worker, control))
            else:
                await self.worker_control(request, params, worker, control)
            settled = True
        return settled

    async def worker_stopped(self, request_id, task):
        if task:
            await asyncio.gather(task, return_exceptions=True)
        self.store.service_request_settle(request_id, 'done', {'stop': 'stopped'})

    async def worker_control(self, request, params, worker, control):
        op = request['op']
        prompt = params['prompt'] if op == 'workers.steer' else '/goal ' + params['condition']
        if op == 'workers.goal' and worker['provider'] == 'codex':
            outcome = await control.goal(STEER_PREFIX + str(request['id']), params['condition'])
        else:
            outcome = await control.steer(STEER_PREFIX + str(request['id']), prompt)
        state = ('done' if outcome in ('received', 'queued') else 'uncertain' if outcome == 'uncertain'
                 else 'refused')
        if state != 'done':
            problems.record(self.store, 'worker', 'steer-' + outcome, 'op=%s request=%s' % (op, request['id']),
                            topic=worker['topic'], task=worker['task'], worker=worker['id'])
        if state == 'refused' and op == 'workers.steer':
            reason = ('Its turn had ended.' if outcome == 'closed' else
                      'The worker control refused the request (%s).' % outcome)
            self.refuse_steer(request['id'], params, reason, worker, {'receipt': outcome})
        else:
            self.store.service_request_settle(request['id'], state, {'receipt': outcome})

    def refuse_steer(self, request_id, params, reason, worker=None, result=None):
        topic = worker['topic'] if worker else params.get('_topic')
        with self.store.db:
            self.store.service_request_settle(request_id, 'refused', result or {'text': reason})
            if topic:
                self.store.message_save(topic, 'callback',
                                        'Steer request %d for worker %d was not delivered: %s'
                                        % (request_id, params['worker'], reason))

    def revoke(self, request_id, name):
        from .envelopes import rename_reserved, revoke
        from .vault import VaultError
        if rename_reserved(self.store, name):
            self.store.service_request_settle(request_id, 'refused',
                {'text': 'A saved key rename is unfinished for this name. Run the same rename again first.'})
            return
        try:
            if self.vault is None:
                raise VaultError('unavailable')
            removed = self.vault.delete(name)
        except VaultError as error:
            logger.info('vault delete failed name=%s code=%s', name, error.code)
            problems.record(self.store, 'secrets', 'vault-delete-failed', 'name=%s code=%s' % (name, error.code))
            self.store.service_request_settle(request_id, 'refused', {'text': 'The vault delete failed. Nothing changed.'})
            return
        with self.store.db:
            text = revoke(self.store, name, removed)
            self.store.service_request_settle(request_id, 'done', {'text': text})

    def rename(self, request_id, name, new_name):
        from .control_api import Refused
        from .envelopes import RENAME_KEY, rename
        if self.store.db.in_transaction:
            self.store.db.commit()
        try:
            text = rename(self.store, self.vault, name, new_name)
        except Refused as refusal:
            self.store.service_request_settle(request_id, 'refused', {'text': str(refusal)})
            return
        except Exception as error:
            logger.info('saved key rename failed type=%s', type(error).__name__)
            self.store.service_request_settle(request_id, 'failed',
                {'text': 'Saved key rename could not finish. Run the same rename again.'})
            return
        with self.store.db:
            self.store.db.execute('DELETE FROM settings WHERE key=?', (RENAME_KEY,))
            self.store.service_request_settle(request_id, 'done', {'text': text})

    async def controls(self):
        while True:
            if not await self.controls_once():
                await asyncio.sleep(0.5)

    async def tldr_once(self):
        from .setup_flow import blocked
        if self.pair_only or blocked(self.store):
            return False
        from .tldr import summarize
        request = self.store.db.execute(
            "SELECT * FROM service_requests WHERE op='tldr' AND state='queued' ORDER BY id LIMIT 1").fetchone()
        if request is None:
            return False
        params = json.loads(request['params'])
        self.store.service_request_settle(request['id'], 'sending')
        try:
            from .codex_session import codex_binary
            reply = await summarize(self.store, params['topic'], params['message'], self.accounts,
                                    self.extension, self.signins.binary,
                                    codex=codex_binary(self.store, self.runner if isinstance(self.runner, ProviderRunner) else None))
        except asyncio.CancelledError:
            self.store.service_request_settle(request['id'], 'queued')
            raise
        except Exception as error:
            logger.warning('summary failed type=%s exit=%s', type(error).__name__,
                           getattr(error, 'returncode', None))
            reply = 'Summary failed: Torii could not complete the request.'
        with self.store.db:
            self.store.enqueue_report(params['topic'], reply, reply_to=params['message'])
            self.store.service_request_settle(request['id'], 'done')
        self.wake('deliver').set()
        return True

    async def tldr(self):
        while True:
            if not await self.tldr_once():
                await asyncio.sleep(0.5)

    async def deliver_once(self):
        async with self.delivery_lock:
            if self.restart_reason is not None:
                return False
            row = self.store.pending_delivery()
            if not row:
                return False
            row = self.signins.muse_card(row)
            wait_card = row['kind'] in ('wait-ask', 'owner-waiting')
            if wait_card:
                with self.store.db:
                    self.store.put('wait_sending:' + str(row['id']), True)
            try:
                if row.get('image'):
                    from .control_api import Refused, resolve_image
                    try:
                        resolve_image(self.store, row['image'])
                    except Refused as refusal:
                        problems.record(self.store, 'outbox', 'image-unavailable', 'outbox=%s %s' % (row['id'], refusal),
                                        topic=row['topic'])
                        self.store.image_unavailable(row['id'])
                        return True
                result = await self.telegram.send(row)
                self.store.delivered(row['id'], result['message_id'])
            except ImageUnavailable:
                problems.record(self.store, 'outbox', 'image-unavailable', 'outbox=%s file missing' % row['id'],
                                topic=row['topic'])
                self.store.image_unavailable(row['id'])
            except TelegramError as error:
                if row.get('edit_message') and error.code == 400:
                    problems.record(self.store, 'outbox', 'edit-failed', 'outbox=%s reason=%s' % (row['id'], error.reason),
                                    topic=row['topic'])
                    self.store.edit_failed(row['id'])
                    return True
                if error.reason in ('thread_not_found', 'topics_off'):
                    from .setup_flow import topic_unreachable
                    with self.store.db:
                        redirected = topic_unreachable(self.store, row, error.reason)
                    self.wake('group_setup').set()
                    if redirected:
                        return True
                if row.get('reply_markup') and error.code == 400:
                    with self.store.db:
                        self.store.db.execute('UPDATE outbox SET reply_markup=NULL WHERE id=?', (row['id'],))
                    return True
                logger.info('delivery failed outbox=%s topic=%s code=%s retry_after=%s',
                            row['id'], row['topic'], error.code, error.retry_after)
                problems.record(self.store, 'outbox', 'send-failed', 'outbox=%s kind=%s telegram=%s reason=%s '
                                'retry_after=%s attempts=%s' % (row['id'], row['kind'], error.code, error.reason,
                                                                error.retry_after, row['attempts'] + 1),
                                topic=row['topic'])
                self.store.delivery_failed(row, error.retry_after)
            finally:
                if wait_card:
                    with self.store.db:
                        self.store.db.execute('DELETE FROM settings WHERE key=?', ('wait_sending:' + str(row['id']),))
                    self.store.wait_hook('card-refresh-failed', self.store.refresh_owner_waiting)
            return True

    async def deliver(self):
        while True:
            if not await self.deliver_once():
                await pause(self.wake('deliver'), DELIVERY_IDLE)

    async def answer_callback(self, query_id, outcome):
        from .control_api import Result
        text = outcome.text if isinstance(outcome, Result) else {
                'topics_off': 'Topics are still off.', 'topics_on': 'Topics are on.',
                'topics_unauthorized': 'Only the Torii owner can do this.', 'unauthorized': 'Only the paired owner can change Torii settings.',
                'already_here': 'Already here.',
                'stale_callback': 'This menu has expired. Open the command again.',
                'already_done': 'This confirmation was already handled.',
                'wait_choice_received': 'Choice received.',
                'wait_already_answered': 'This question was already answered.',
                'duplicate': 'Already received.',
                'envelope_cancel': 'Envelope cancelled.',
                'envelope_arm': 'Send the value as your next message.',
                'envelope_busy': 'Another credential ask is waiting in your private chat. '
                                 'Send its value or tap Cancel on its card first.',
                'envelope_expired': 'This ask expired. Tap Ask again on its card.',
                'envelope_closed': 'This envelope is already closed.'}.get(outcome, 'See the next message.')
        parameters = ({'url': 'https://t.me/' + self.store.get('bot_username') + '?start=' + FILL_START}
                      if outcome == 'envelope_fill' else
                      {'text': text})
        try:
            await self.telegram.call('answerCallbackQuery', callback_query_id=query_id, **parameters)
        except TelegramError as error:
            logger.info('callback answer failed code=%s', error.code)
            problems.record(self.store, 'telegram', 'callback-failed', 'telegram=%s' % error.code, every=60)
            if outcome == 'envelope_fill':
                await self.answer_callback(query_id, Result(True, 'Send the value in your private chat with me.'))

    async def accept_update(self, update):
        from .telegram_updates import update_problem
        from .pairing import valid_code
        if not isinstance(update, dict) or type(update.get('update_id')) is not int or update_problem(update):
            return self.store.accept(update)
        if self.store.db.execute('SELECT 1 FROM updates WHERE id=?', (update['update_id'],)).fetchone():
            return 'duplicate'
        query = update.get('callback_query', {})
        if isinstance(query.get('data'), str):
            from .control_ui import _key
            message = query.get('message', {})
            chat = message.get('chat', {})
            topic = str(chat.get('id')) + ':' + str(message.get('message_thread_id', 0))
            state = self.store.get(_key(topic), {}) or {}
            parts = query['data'].split(':')
            actions = state.get('actions', [])
            if (len(parts) == 3 and parts[0] == 'torii' and parts[1] == state.get('token')
                    and parts[2].isascii() and parts[2].isdigit() and len(parts[2]) < 4 and int(parts[2]) < len(actions)
                    and actions[int(parts[2])].get('op') == 'setup.topics_check'):
                if query.get('from', {}).get('id') != self.store.get('owner') or chat.get('id') != self.store.chat():
                    self.store.accept(update)
                    return 'topics_unauthorized'
                outcome = self.store.accept(update)
                if outcome != 'control_callback':
                    return outcome
                from .setup_flow import tick
                await tick(self)
                return 'topics_on' if self.store.get('group_is_forum') and self.store.get('control_topic') else 'topics_off'
        message = update.get('message', {})
        parts = message.get('text', '').split(maxsplit=1)
        creator = False
        if (len(parts) == 2 and parts[0].split('@')[0] in ('/start', '/pair') and parts[1] != 'fix'
                and message.get('chat', {}).get('type') in ('group', 'supergroup')
                and type(message['chat'].get('id')) is int and type(message.get('message_id')) is int
                and not message.get('sender_chat') and not message.get('from', {}).get('is_bot')
                and type(message.get('from', {}).get('id')) is int
                and self.store.get('owner') in (None, message['from']['id'])
                and self.store.chat() in (None, message['chat']['id']) and valid_code(self.store, parts[1])):
            try:
                member = await self.telegram.call('getChatMember', chat_id=message['chat']['id'], user_id=message['from']['id'])
            except TelegramError as error:
                from .setup_flow import notice
                with self.store.db:
                    self.store.accept(update)
                    problems.record(self.store, 'pairing', 'member-check-failed', 'telegram=%s' % error.code)
                    notice(self.store, message['chat']['id'], "I couldn't check the group owner. Send /start again to retry.",
                           key='pair_retry:' + str(message['chat']['id']), every=30)
                self.wake('deliver').set()
                return 'pair_retry'
            creator = member.get('status') == 'creator'
            join = (self.store.get('group_joins', {}) or {}).get(str(message['chat']['id']), {})
            if join.get('performer') not in (None, message['from']['id']):
                logger.info('pairing performer differs chat=%s', message['chat']['id'])
        outcome = self.store.accept(update, creator=creator)
        if outcome == 'queued':
            topic = str(message.get('chat', {}).get('id')) + ':' + str(message.get('message_thread_id', 0))
            row = self.store.db.execute('SELECT * FROM messages WHERE topic=? AND telegram_message=?',
                                        (topic, message.get('message_id'))).fetchone()
            if row:
                self.start_audio(dict(row))
        deletion = self.store.get('pair_delete')
        if deletion:
            try:
                await self.telegram.call('deleteMessage', chat_id=deletion['chat'], message_id=deletion['message'])
            except TelegramError as error:
                problems.record(self.store, 'pairing', 'delete-failed', 'telegram=%s' % error.code)
                if error.code in (400, 403):
                    with self.store.db:
                        self.store.put('pair_delete', None)
            else:
                with self.store.db:
                    self.store.put('pair_delete', None)
        return outcome

    async def poll(self):
        failures = 0
        conflicts = 0
        while True:
            try:
                updates = await self.telegram.updates(self.store.get('offset', 0))
                failures = 0
                conflicts = 0
                self.poll_succeeded = True
                self.network_available = True
                from .updates import KEY, WATCHED, watch
                if self.updater.commit and (self.store.get(KEY) or {}).get('phase') in WATCHED:
                    process = await asyncio.to_thread(watch.process_identity, os.getpid())
                    self.updater.healthy(process)
                if self.store.get('poll_conflict_notified'):
                    with self.store.db:
                        self.store.put('poll_conflict_notified', False)
                answers = []
                for update in updates:
                    outcome = await self.accept_update(update)
                    query = update.get('callback_query') if isinstance(update, dict) else None
                    if isinstance(query, dict) and isinstance(query.get('id'), str):
                        answers.append((query['id'], outcome))
                if updates:
                    for name in ('reactions', 'deliver', 'feed', 'group_setup', 'waits'):
                        self.wake(name).set()
                for query_id, outcome in answers:
                    await self.answer_callback(query_id, outcome)
            except TelegramError as error:
                self.network_available = False
                if error.code == 409:
                    self.poll_succeeded = False
                    conflicts += 1
                    if conflicts < 3:
                        await asyncio.sleep(1)
                        continue
                    problems.record(self.store, 'telegram', 'poll-stopped', 'telegram=%s' % error.code,
                                    level=logging.ERROR)
                    with self.store.db:
                        if not self.store.get('poll_conflict_notified') and self.store.get('owner') is not None:
                            self.store.enqueue_report(self.store.get('control_topic') or self.home_topic(),
                                'Another program is reading this bot\'s updates. If this bot is only for Torii, restart Torii. Otherwise make a new bot with /newbot in @BotFather and rerun setup.')
                            self.store.put('poll_conflict_notified', True)
                    await asyncio.Event().wait()
                if error.code == 401:
                    self.poll_succeeded = False
                    problems.record(self.store, 'telegram', 'poll-stopped', 'telegram=401', level=logging.ERROR)
                    await asyncio.Event().wait()
                failures += 1
                logger.info('poll retry code=%s retry_after=%s', error.code, error.retry_after)
                problems.record(self.store, 'telegram', 'poll-%s' % error.code, 'retry_after=%s failures=%s' % (
                    error.retry_after, failures), every=600 if error.code == 'network-or-invalid-response' else 60)
                await asyncio.sleep(min(error.retry_after or (1 if failures == 1 else 5), 60))

    async def reactions(self):
        from .reactions import deliver_reaction
        while True:
            if not await deliver_reaction(self.store, self.telegram):
                await pause(self.wake('reactions'), REACTION_IDLE)

    async def command_menu(self):
        from .onboarding import command_menu
        installed = None
        while True:
            if not getattr(self, 'poll_succeeded', False):
                await asyncio.sleep(1)
                continue
            scope = (self.store.chat(), self.store.get('owner'))
            commands = command_menu(self.store)
            identity = (scope, self.store.get('telegram_updates_enabled', False) is True,
                        self.store.get('waits_enabled', False) is True)
            if all(scope) and identity != installed:
                try:
                    await self.telegram.call('setMyCommands', commands=commands,
                                             scope={'type': 'chat_member', 'chat_id': scope[0], 'user_id': scope[1]})
                    installed = identity
                except TelegramError as error:
                    logger.info('command menu retry code=%s', error.code)
                    problems.record(self.store, 'telegram', 'command-menu-failed', 'telegram=%s' % error.code,
                                    every=60)
            await asyncio.sleep(30)

    async def bot_identity(self):
        if self.store.get('bot_username') or time.time() < self.bot_identity_retry:
            return
        try:
            me = await self.telegram.call('getMe')
        except TelegramError as error:
            self.bot_identity_retry = time.time() + 30
            logger.info('bot identity retry code=%s', error.code)
            problems.record(self.store, 'telegram', 'bot-identity-failed', 'telegram=%s' % error.code, every=60)
            return
        with self.store.db:
            self.store.put('bot_username', me['username'])
            store = self.store
            store.put('bot_id', me['id'])

    async def envelopes_once(self):
        from .envelopes import run_effects, sweep
        await self.bot_identity()
        with self.store.db:
            sweep(self.store)
        return await run_effects(self.store, self.telegram)

    async def envelopes(self):
        while True:
            await self.envelopes_once()
            await asyncio.sleep(1)

    def exit_for_restart(self):
        if self.on_restart is not None:
            self.on_restart()
        raise _Restart()

    async def restart_once(self):
        request = self.store.get('restart_requested_v2')
        from .updates import KEY, watch
        update = self.store.get(KEY) or {}
        owned_update = bool(request and request.get('update_id') and request['update_id'] == update.get('id'))
        if owned_update:
            stamp = await asyncio.to_thread(watch.process_identity, os.getpid())
            if (update.get('phase') != 'restart'
                    or (update.get('old_pid'), update.get('old_process')) != (os.getpid(), stamp)):
                with self.store.db:
                    self.store.put('restart_requested_v2', None)
                return False
        if not request:
            return False
        now = time.time()
        if self.store.outbox_has_pending() and (not owned_update or now - request.get('created', now) < 60):
            return False
        async with self.delivery_lock:
            if self.store.outbox_has_pending() and not owned_update:
                return False
            last = self.store.get('last_restart_v2')
            if not owned_update and last is not None and now - last < RESTART_GUARD_SECONDS:
                if request.get('reason') == 'agents on':
                    return False
                with self.store.db:
                    self.store.put('restart_requested_v2', None)
                problems.record(self.store, 'service', 'restart-dropped', request.get('reason'))
                return False
            with self.store.db:
                self.store.put('restart_requested_v2', None)
                self.store.put('last_restart_v2', now)
            try:
                backup = self.store.backup_for_restart()
            except BaseException as error:
                with self.store.db:
                    self.store.put('restart_requested_v2', request)
                    self.store.put('last_restart_v2', last)
                problems.record(self.store, 'service', 'restart-backup-failed', internal_detail(error),
                                level=logging.ERROR)
                raise
            logger.info('service restart requested backup=%s', backup.name)
            if owned_update:
                from .__main__ import RESTART_DEADLINE_SECONDS
                if not watch.write(self.store.db, request['update_id'], expected='restart', restart_pending=True,
                                   restart_started=time.time(), restart_from=dict(pid=os.getpid(), process=stamp),
                                   shutdown_grace=RESTART_DEADLINE_SECONDS + SHUTDOWN_SECONDS + 5):
                    return False
            self.restart_reason = request.get('reason')
            self.exit_for_restart()
            return True

    async def restart_watch(self):
        while True:
            if await self.restart_once():
                return
            await asyncio.sleep(0.5)

    async def supervise(self, name, start):
        essential = LOOP_ESSENTIAL.get(name, True)
        attempt = 0
        cooldown = LOOP_COOLDOWN
        notified = False
        while True:
            started = time.monotonic()
            try:
                return await start()
            except (asyncio.CancelledError, _Restart):
                raise
            except Exception as error:
                if not essential and time.monotonic() - started >= LOOP_HEALTHY_SECONDS:
                    attempt = 0
                    cooldown = LOOP_COOLDOWN
                attempt += 1
                logger.exception('task crashed task=%s attempt=%s', name, attempt)
                problems.record(self.store, 'service', 'loop-crashed', 'loop=%s attempt=%s %s' % (
                    name, attempt, internal_detail(error)), level=logging.ERROR)
                if attempt <= self.task_restart_limit:
                    await asyncio.sleep(self.task_restart_backoff)
                    continue
                if essential:
                    raise _TaskFailed('The %s service loop failed %s times.' % (name, attempt))
                if not notified:
                    notified = True
                    try:
                        problems.record(self.store, 'service', 'loop-stopped', 'loop=%s %s' % (
                            name, internal_detail(error)), level=logging.ERROR)
                        if self.home_topic():
                            with self.store.db:
                                self.store.enqueue_report(self.home_topic(),
                                    'Torii paused its %s after repeated errors and will retry it by itself. '
                                    'Everything else keeps running.' % self.loop_names[name])
                            self.wake('deliver').set()
                    except sqlite3.Error:
                        logger.exception('loop pause reporting failed task=%s', name)
                await asyncio.sleep(cooldown)
                cooldown = min(cooldown * 2, LOOP_COOLDOWN_LIMIT)

    async def group_setup(self):
        from .setup_flow import tick
        while True:
            delay = 1
            try:
                await tick(self)
            except TelegramError as error:
                problems.record(self.store, 'telegram', 'group-setup-failed', 'telegram=%s' % error.code, every=60)
                delay = max(30, error.retry_after or 0)
            self.wake('deliver').set()
            await pause(self.wake('group_setup'), delay)

    async def worktree_cleanup_loop(self):
        from .worker_storage import storage_for
        from .workspaces import CLEANUP_INTERVAL, cleanup_closed_worktrees
        from .workspaces import cleanup_closed_copies
        sweep = True
        retry_failed = False
        next_sweep = time.monotonic() + CLEANUP_INTERVAL
        storage = storage_for(self.store)
        while True:
            event = self.wake('worktree_cleanup')
            event.clear()
            more = False
            if self.store.get('closed_copy_delete', False) is True or self.store.get('close_clears_space', False) is True:
                more = await cleanup_closed_copies(self.store, sweep=sweep, retry_failed=retry_failed)
            else:
                await cleanup_closed_worktrees(self.store, retry_failed=retry_failed)
            await storage.reconcile()
            storage.schedule()
            retry_failed = True
            delay = max(0, next_sweep - time.monotonic())
            if self.store.get('closed_copy_delete', False) is not True:
                delay = CLEANUP_INTERVAL
            if self.store.get('close_clears_space', False) is True:
                from .close_saves import deletion_delay
                delay = 0 if more else deletion_delay(self.store)
            timer = asyncio.create_task(asyncio.sleep(delay)) if delay is not None else asyncio.create_task(asyncio.Event().wait())
            wake = asyncio.create_task(event.wait())
            try:
                done, pending = await asyncio.wait((timer, wake), return_when=asyncio.FIRST_COMPLETED)
                for finished in done:
                    finished.result()
                sweep = timer in done
                if sweep:
                    next_sweep = time.monotonic() + CLEANUP_INTERVAL
            finally:
                timer.cancel()
                wake.cancel()
                await asyncio.gather(timer, wake, return_exceptions=True)

    async def host_log_cleanup_loop(self):
        await self.host_logs.run()

    async def stray_report_loop(self):
        from .stray_report import next_night, run_once
        pruned = 0
        while True:
            event = self.wake('stray_report')
            event.clear()
            now = time.time()
            if next_night(pruned) <= now:
                problems.prune(self.store)
                pruned = now
            delay = max(1, next_night(pruned) - time.time())
            if self.store.get('stray_report_enabled', False) is True:
                history = self.store.get('stray_report_history', [])
                due = next_night(history[-1]['created']) if history else 0
                if self.store.get('stray_report_requested', False) or due <= time.time():
                    with self.store.db:
                        self.store.put('stray_report_requested', False)
                    await run_once(self.store)
                    history = self.store.get('stray_report_history', [])
                if history:
                    delay = min(delay, max(1, next_night(history[-1]['created']) - time.time()))
            await pause(event, delay)

    async def waits_once(self):
        if self.store.get('waits_enabled', False) is not True:
            return
        from .waits import poll_github
        await poll_github(self.store)
        for item in self.store.wait_due():
            self.store.wait_hook('timer-trigger-failed', self.store.wait_fire, item['id'], outcome='due')
        self.store.wait_hook('unblocked-job-failed', self.store.wait_resume_unblocked)
        self.store.wait_hook('card-refresh-failed', self.store.refresh_owner_waiting)
        for name in ('feed', 'deliver', 'workers'):
            self.wake(name).set()

    async def waits_started(self):
        if self.store.get('waits_enabled', False) is not True:
            return
        self.store.wait_hook('service-started-signal-failed', self.store.wait_signal, 'service.started', internal=True)
        await self.updater.waits_changed(started=True)

    async def waits_loop(self):
        from .waits import run
        await run(self)

    def supervised(self):
        from .account_status import monitor_accounts
        from .opencode_subscription import monitor_accounts as monitor_opencode_accounts
        codex = getattr(self.workers.runner, 'binaries', {}).get('codex', 'codex')
        loops = [('poll', self.poll), ('deliver', self.deliver),
                 ('updates', self.updater.run),
                 ('accounts', lambda: monitor_accounts(self.store)),
                 ('codex_accounts', lambda: monitor_codex_accounts(self.store, codex)),
                 ('opencode_accounts', lambda: monitor_opencode_accounts(self.store)),
                 ('command_menu', self.command_menu),
                 ('reactions', self.reactions),
                 ('restart_watch', self.restart_watch), ('envelopes', self.envelopes),
                 ('signins', self.signins.run)]
        if self.updater.freeze is not None:
            loops.insert(3, ('update_freeze', lambda: self.updater.freeze.run()))
        loops.append(('group_setup', self.group_setup))
        if not self.pair_only:
            loops += [('feed', self.feed), ('workers', self.workers_loop), ('controls', self.controls),
                      ('tldr', self.tldr)]
            requests = self.store.code_requests
            if requests is not None:
                loops += [('code_request_intake', lambda: requests.intake_loop(self.store)),
                          ('code_request_sender', lambda: requests.sender_loop(self.store))]
            loops += [('worktree_cleanup', self.worktree_cleanup_loop), ('host_logs', self.host_log_cleanup_loop)]
            from .session_storage import storage_for
            loops.append(('session_cleanup', lambda: storage_for(self.store).run()))
            loops.append(('stray_report', self.stray_report_loop))
            loops.append(('waits', self.waits_loop))
        return loops

    def record_start(self):
        """One problem row per start, with the cause the previous run saved when it ended."""
        previous = self.store.get('service_run')
        problems.prune(self.store)
        if previous is None:
            problems.record(self.store, 'service', 'started', 'previous exit: unknown, no record')
        elif previous.get('state') == 'running':
            problems.record(self.store, 'service', 'unclean-exit', 'previous run started %s ended without an exit '
                            'record' % problems.stamp(previous['started']), level=logging.ERROR)
        else:
            problems.record(self.store, 'service', 'started', 'previous exit: ' + previous['cause'])
        with self.store.db:
            self.store.put('service_run', {'state': 'running', 'started': time.time(),
                                           'checked': last_check(previous),
                                           'code': self.code})

    def record_exit(self, cause):
        previous = self.store.get('service_run') or {}
        with self.store.db:
            self.store.put('service_run', {'state': 'exited', 'cause': cause, 'ended': time.time(),
                                           'checked': last_check(previous)})

    def reconcile_steers(self):
        for worker_id in sorted({json.loads(row['params']).get('worker') for row in self.store.db.execute(
                "SELECT op,params FROM service_requests WHERE state='uncertain'") if row['op'] in STEER_OPS} - {None}):
            reconcile_steers(self.store, worker_id)

    async def announce_start(self):
        from .setup_flow import blocked
        if self.pair_only or blocked(self.store):
            return
        await self.workers_once()
        run = self.store.get('service_run') or {}
        checked = last_check(run)
        known = type(checked) in (int, float)
        booted = boot_time() if known else None
        rebooted = booted is not None and booted > checked
        enabled = [topic['id'] for topic in self.store.topics() if topic['enabled']]
        home = self.home_topic()
        home = home if home in enabled else next(iter(enabled), None)
        stopped = {}
        stopped_jobs = {}
        for row in self.store.db.execute("SELECT topic,task,result FROM workers WHERE status='interrupted' AND updated>?",
                                         (checked if known else run.get('started', 0),)):
            if json.loads(row['result'] or '{}').get('failure_code') == 'interrupted':
                channel = row['topic'] if row['topic'] in enabled else home
                stopped[channel] = stopped.get(channel, 0) + 1
                stopped_jobs.setdefault(channel, set()).add(row['task'])
        with self.store.db:
            self.store.put('service_run', dict(run, checked=time.time()))
            for topic in enabled:
                count = stopped.get(topic, 0)
                if count:
                    if self.store.get('auto_resume', False) is True:
                        jobs = len(stopped_jobs[topic])
                        self.store.enqueue_report(topic, 'Torii is back; checking %d stopped %s for auto-resume.'
                                                  % (jobs, 'job' if jobs == 1 else 'jobs'))
                    else:
                        noun = 'agent was' if count == 1 else 'agents were'
                        self.store.enqueue_report(
                            topic, f'Torii just came back online - {count} {noun} stopped. Ask me to resume them.')
                elif rebooted:
                    self.store.enqueue_report(topic, 'Torii just came back online.')

    async def run(self):
        self.record_start()
        self.store.messages_uncertain()
        self.store.requests_uncertain()
        cause = 'returned'
        try:
            self.resume_audio()
            logger.info('claude launch extension=%s', 'native' if type(self.extension) is Native else 'local')
            await self.extension.start()
            from .reactions import check_administrators
            await check_administrators(self.store, self.telegram)
            self.reconcile_steers()
            from .host import recover_ownership
            if self.pair_only:
                recover_ownership(self.store.directory / 'hosts',
                                  lambda area, code, detail: problems.record(self.store, area, code, detail))
            await self.workers.cleanup_finished()
            if not self.pair_only:
                await self.start_session(self.home_topic())
                for topic in self.store.topics():
                    if (topic['id'] != self.home_topic() and
                            self.store.get(self.session_key(topic['id']) + '_host')):
                        await self.start_session(topic['id'], reattach_only=not topic['enabled'])
            logger.info('service run commit=%s pair_only=%s', running_commit(Path(__file__).resolve().parents[1]),
                        self.pair_only)
            await self.announce_start()
            await self.waits_started()
            if not self.pair_only:
                self.host_logs.request()
            tasks = [asyncio.create_task(self.supervise(name, start)) for name, start in self.supervised()]
        except BaseException as error:
            for task in self.audio_tasks.values():
                task.cancel()
            await asyncio.gather(*self.audio_tasks.values(), return_exceptions=True)
            self.record_exit('start failed: ' + type(error).__name__)
            raise
        try:
            await asyncio.gather(*tasks)
        except _Restart:
            logger.info('service restart requested')
            cause = 'restart reason=' + (self.restart_reason or 'unknown')
        except _TaskFailed as failure:
            logger.error('%s', failure)
            problems.record(self.store, 'service', 'loop-failed', str(failure), level=logging.ERROR)
            cause = 'loop failed: ' + str(failure)
            raise
        except asyncio.CancelledError:
            cause = 'signal'
            raise
        except BaseException as error:
            cause = 'error: ' + internal_detail(error)
            raise
        finally:
            self.record_exit(cause)
            try:
                await asyncio.wait_for(self.shutdown(tasks), SHUTDOWN_SECONDS)
            except asyncio.TimeoutError:
                logger.error('service shutdown timed out seconds=%s', SHUTDOWN_SECONDS)

    async def shutdown(self, tasks):
        children = (tasks + list(self.feed_tasks.values()) + list(self.worker_tasks.values())
                    + list(self.control_tasks.values()) + list(self.stop_tasks.values())
                    + list(getattr(self, 'audio_tasks', {}).values())
                    + list(getattr(self, 'chat_scratch_tasks', {}).values())
                    + list(getattr(self, 'parent_rechecks', {}).values()))
        if getattr(self, 'setup_accounts_task', None) is not None:
            children.append(self.setup_accounts_task)
        for task in children:
            task.cancel()
        await asyncio.gather(*children, return_exceptions=True)
        for session in self.sessions.values():
            if hasattr(session, 'detach'):
                await session.detach()
        await self.extension.close()
