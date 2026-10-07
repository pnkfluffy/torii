"""Persistent coordinator input, background workers, and Telegram transport."""

import asyncio
from datetime import datetime, timezone
import json
import logging
import re
import subprocess
import time
from pathlib import Path

from . import problems, parent
from .accounts import AccountBroker, DEFAULT_ACCOUNTS, account_label, switch_threshold
from .codex_accounts import CodexBroker, monitor_codex_accounts
from .control_api import BRIEF_LIMIT
from .envelopes import waiting_secrets
from .failures import internal_detail
from .media import AUDIO_FIELDS, MediaError, attachment_instructions, prepare_attachments, prepare_transcripts
from .policy import COORDINATOR_MODEL, COORDINATOR_POLICY, usage_policy
from .providers import ProviderRunner
from .session import AccountsUnavailable, CoordinatorSession, message_tag, message_uuid
from .signin import SignIns
from .telegram import ImageUnavailable, TelegramError
from .usage_notice import clear_low_capacity_warning, warn_low_capacity
from .workers import STEER_OPS, STEER_PREFIX, WorkerPool, host_directory, reconcile_steers
from .log import running_commit
from . import extension
from .extension import Native
from . import host_os

logger = logging.getLogger(__name__)
TASK_RESTART_LIMIT = 5
TASK_RESTART_BACKOFF = 2.0
RESTART_GUARD_SECONDS = 120
REACTION_IDLE = 0.5
DELIVERY_IDLE = 0.1
FEED_IDLE = 0.1
PARENT_IDLE = 3600
PARENT_RETRY = 60


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
    """When a start last decided the startup notice. Records from older versions give their start or end time."""
    if not isinstance(run, dict):
        return None
    return run['checked'] if 'checked' in run else run.get('started', run.get('ended'))


async def pause(wake, seconds):
    try:
        await asyncio.wait_for(wake.wait(), seconds)
    except asyncio.TimeoutError:
        pass
    wake.clear()


class _Restart(Exception):
    """Leave run so launchd can start the service again."""


class _TaskFailed(RuntimeError):
    """A supervised loop exhausted its restart attempts."""


class Service:
    def __init__(self, store, telegram, runner, project_root: Path, pair_only=False,
                 coordinator_model=COORDINATOR_MODEL, session_factory=None, vault=None):
        self.store = store
        self.vault = vault
        store.vault = vault
        self.bot_identity_retry = 0
        self.telegram = telegram
        self.runner = runner
        self.project_root = project_root
        self.forced_pair_only = pair_only
        self.pair_only = pair_only or store.get('mode') == 'private'
        self.coordinator_model = coordinator_model
        self.session_factory = session_factory or parent.start
        self.sessions = {}
        self.wound_down = set()
        self.parent_retry = {}
        self.last_turn = {}
        self.restart_messages = {}
        self.restart_reason = None
        self.feed_tasks = {}
        self.worker_tasks = {}
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
        binaries = getattr(runner, 'binaries', {})
        self.signins = SignIns(store, binaries.get('claude', 'claude'), codex_binary=binaries.get('codex', 'codex'))
        with store.db:
            store.put('pair_only', self.pair_only)
            store.put('forced_pair_only', pair_only)
            if store.get('accounts') is None:
                store.put('accounts', DEFAULT_ACCOUNTS)


    def wake(self, name):
        if name not in self.wakes:
            self.wakes[name] = asyncio.Event()
        return self.wakes[name]

    def home_topic(self):
        return self.store.get('coordinator_home_topic')

    def notice_last_account(self):
        accounts = self.store.get('accounts', {}) or {}
        usable = [alias for alias in accounts if self.accounts.available(accounts, alias)]
        if len(usable) > 1:
            clear_low_capacity_warning(self.store, 'Claude')
            return
        if len(usable) != 1:
            return
        alias = usable[0]
        usage = (self.store.get('account_status', {}).get(alias) or {}).get('usage') or {}
        names = (('five_hour', '5-hour'), ('seven_day', 'weekly'),
                 ('seven_day_fable', 'Fable weekly'))
        low = []
        for key, name in names:
            window = usage.get(key) or {}
            used = window.get('utilization')
            reset = window.get('resets_at')
            if (isinstance(used, (int, float)) and not isinstance(used, bool)
                    and (reset is None or isinstance(reset, (int, float)) and reset > time.time())):
                low.append((used, key, name, reset))
        if not low:
            return
        used, key, name, reset = max(low)
        others = {name: account for name, account in accounts.items() if name != alias}
        next_reset = self.accounts.earliest_reset(others) if others else None
        warn_low_capacity(self.store, 'Claude', account_label(self.store, alias), name, used,
                          reset, next_reset, self.home_topic(),
                          capacity_pct=switch_threshold(self.store, alias) * 100)

    @property
    def session(self):
        return self.sessions.get(self.home_topic())

    def session_key(self, topic):
        main = self.store.get('coordinator_session_topic') or self.home_topic()
        return 'coordinator' if topic == main else 'coordinator:' + topic

    def parent_available(self):
        runner = self.runner if isinstance(self.runner, ProviderRunner) else None
        return parent.available(self.store, self.accounts, self.codex_accounts, runner)

    def paused_topic(self, topic):
        notice = self.store.get('coordinator_account_notice')
        if not isinstance(notice, dict) or not topic or topic in notice['topics']:
            return
        with self.store.db:
            self.store.enqueue_report(topic, notice['text'])
            self.store.put('coordinator_account_notice', dict(notice, topics=[*notice['topics'], topic]))

    def pause_parent(self, topic, error):
        reset = (datetime.fromtimestamp(error.reset_at).astimezone().strftime('%Y-%m-%d %H:%M %Z')
                 if error.reset_at else None)
        reason = 'unavailable'
        if error.provider == 'codex' and error.alias and not self.codex_accounts.signed_in(error.alias):
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
        notice_key = [error.provider, error.alias, error.reset_at, reason]
        notice = self.store.get('coordinator_account_notice')
        previous = notice.get('key') if isinstance(notice, dict) else None
        same_wait = bool(previous and previous[:2] == notice_key[:2] and previous[3:] == notice_key[3:] and (
            previous[2] == error.reset_at or previous[2] is not None and error.reset_at is not None and
            abs(previous[2] - error.reset_at) < 60))
        with self.store.db:
            self.store.put('coordinator_account_retry_at', error.reset_at or time.time() + 60)
            if not same_wait:
                problems.record(self.store, 'accounts', 'unavailable', 'coordinator provider=%s reset_at=%s' % (
                    error.provider, error.reset_at))
                self.store.put('coordinator_account_notice', {'key': notice_key, 'text': text, 'topics': []})
            for target in dict.fromkeys((self.home_topic(), topic)):
                self.paused_topic(target)

    async def start_session(self, topic=None):
        topic = topic or self.home_topic()
        if topic is None:
            return None
        from .setup_flow import blocked
        if self.pair_only or blocked(self.store):
            return None
        config = self.store.topic(topic)
        if (not config or config['chat'] == 0
                or (topic == self.store.get('control_topic') and not config['cwd'])):
            return None
        previous_session = self.sessions.get(topic)
        if previous_session and not previous_session.closed and not getattr(previous_session, 'rejected', False):
            return previous_session
        if self.parent_retry.get(topic, 0) > time.monotonic():
            return None
        if previous_session:
            await previous_session.wait_closed()
        key = self.session_key(topic)
        retry_at = self.store.get('coordinator_account_retry_at')
        if retry_at and retry_at > time.time() and not self.parent_available():
            self.paused_topic(topic)
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
        model = self.store.get('coordinator_model') or self.coordinator_model
        try:
            session = await self.session_factory(self.store, self.runner, self.project_root,
                                                 model, instructions, topic=topic)
        except AccountsUnavailable as error:
            logger.warning('no parent account available provider=%s reset_at=%s', error.provider, error.reset_at)
            self.pause_parent(topic, error)
            self.parent_retry.setdefault(topic, 0)
            return None
        except Exception as error:
            self.parent_retry[topic] = time.monotonic() + PARENT_RETRY
            problems.record(self.store, 'coordinator', 'start-failed', internal_detail(error), level=logging.ERROR,
                            topic=topic)
            return None
        with self.store.db:
            self.store.put('coordinator_account_retry_at', None)
            self.store.put('coordinator_account_notice', None)
        idle = topic in self.wound_down
        if not hasattr(session, 'usage_policy'):
            session.usage_policy = usage_policy(self.store.directory)
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
        if (resumed and not fallback and not switched and not rotated and not idle and
                getattr(getattr(session, 'process', None), 'returncode', None) is None):
            tasks = self.store.tasks_list(status='open', topic=None if topic == self.home_topic() else topic)
            workers = [dict(row) for row in self.store.db.execute(
                "SELECT id,task,provider,status FROM workers WHERE status<>'done' AND topic=? ORDER BY id", (topic,))]
            uncertain = [dict(row) for row in self.store.db.execute(
                "SELECT id,topic,kind,text FROM messages WHERE delivered='uncertain' AND kind!='restarted' AND topic=? ORDER BY id", (topic,))]
            self.replace_notice(topic, CoordinatorSession.restarted_prompt(tasks, workers, uncertain))
        elif fallback or switched or (not resumed and topic != self.home_topic()):
            tasks = self.store.tasks_list(status='open', topic=None if topic == self.home_topic() else topic)
            workers = [dict(row) for row in self.store.db.execute(
                "SELECT id,task,provider,status FROM workers WHERE status<>'done' AND topic=? ORDER BY id", (topic,))]
            recent = [dict(row) for row in self.store.db.execute(
                "SELECT kind,text FROM messages WHERE topic=? AND kind!='restarted' AND delivered!='pending' "
                "ORDER BY id DESC LIMIT 5", (topic,))]
            summary = [{'topic': item['topic'], 'number': item['number'], 'title': item['title'],
                        'notes': (item['notes'] or '')[:500]} for item in tasks]
            messages = [dict(item, text=item['text'][:500]) for item in reversed(recent)]
            preface = ''
            if switched:
                change = session.switch
                labels = {'claude': 'Claude', 'codex': 'ChatGPT'}
                preface = ('The main chat moved from %s to %s because %s. '
                           'Your earlier conversation is not in this context.\n\n') % (
                           labels[change['from']], labels[change['to']], change['reason'])
            self.replace_notice(topic, preface + 'Channel continuity: open tasks ' + json.dumps(summary) +
                                '; workers ' + json.dumps(workers) + '; recent messages ' + json.dumps(messages)
                                if preface or tasks or workers or recent else None)
            if switched:
                with self.store.db:
                    home = self.home_topic()
                    if home:
                        note = 'Main chat now runs on %s (%s).' % (labels[change['to']], change['reason'])
                        if change['to'] == 'codex':
                            note += ' It returns to Claude at the next restart after Claude recovers.'
                        self.store.enqueue_report(home, note)
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
        scanner = getattr(session, 'transcript_receipts', CoordinatorSession.transcript_receipts)
        found = scanner(path, rows.values())
        self.store.messages_confirm(found, 'transcript')
        logger.info('uncertain messages confirmed from transcript confirmed=%s remaining=%s',
                    len(found), len(rows) - len(found))

    async def _prepare_message(self, row, session=None):
        text = row['text']
        if row['kind'] == 'owner' and row['images']:
            try:
                attachments = await prepare_attachments(self.store, self.telegram, row)
                await prepare_transcripts(self.store, row, attachments)
            except MediaError as error:
                with self.store.db:
                    problems.record(self.store, 'attachment', 'unavailable', str(error), topic=row['topic'],
                                    message=row['id'])
                    self.store.enqueue_report(row['topic'], str(error), reply_to=row['telegram_message'])
                self.store.message_delivered(row['id'], 'uncertain')
                return None
            text += attachment_instructions(attachments, coordinator=True, provider=getattr(session, 'provider', 'claude'))
        return message_tag(self.store.topic(row['topic']), row) + text

    async def _send_message(self, row, prompt, session):
        try:
            if not self.store.message_sending(row['id']):
                return
            current_policy = usage_policy(self.store.directory)
            if getattr(session, 'usage_policy', None) != current_policy:
                prompt += '\n\nCurrent usage policy:\n' + current_policy
            receipt = await session.send(message_uuid(row['id']), prompt, row_id=row['id'])
            if getattr(session, 'rejected', False):
                self.store.requeue_messages([row['id']])
                return 'closed'
            if receipt == 'closed':
                with self.store.db:
                    self.store.db.execute("UPDATE messages SET delivered='pending',updated=? WHERE id=? AND delivered='sent'",
                                          (time.time(), row['id']))
                return 'closed'
            if receipt == 'unsupported' and getattr(session, 'provider', None) == 'codex':
                self.store.requeue_messages([row['id']])
                problems.record(self.store, 'coordinator', 'steer-unsupported', None, topic=row['topic'],
                                message=row['id'])
                return receipt
            self.store.message_delivered(row['id'], 'written' if receipt == 'received' else 'uncertain')
            if receipt == 'received':
                session.usage_policy = current_policy
            logger.info('coordinator steer message=%s receipt=%s after=%.2f', row['id'], receipt,
                        time.time() - row['created'])
            if receipt != 'received':
                problems.record(self.store, 'coordinator', 'steer-' + receipt, None, topic=row['topic'],
                                message=row['id'])
            return receipt
        except Exception as error:
            self.store.message_delivered(row['id'], 'uncertain')
            problems.record(self.store, 'coordinator', 'steer-failed', internal_detail(error), topic=row['topic'],
                            message=row['id'])
            raise

    async def _deliver_audio(self, row, session):
        prompt = await self._prepare_message(row, session)
        if prompt is not None:
            await self._send_message(row, prompt, session)

    async def feed_once(self):
        from .setup_flow import blocked
        if self.pair_only or blocked(self.store):
            return False
        enabled = {topic['id'] for topic in self.store.topics() if topic['enabled']}
        for topic in list(dict.fromkeys([*self.sessions, *(topic for topic in self.parent_retry if topic in enabled)])):
            session = self.sessions.get(topic)
            if session is None or session.closed:
                if topic not in self.wound_down:
                    await self.start_session(topic)
            elif (not getattr(session, 'busy', False) and
                    not getattr(getattr(session, 'protocol', None), 'background', None) and
                    time.monotonic() - max(self.last_turn.get(topic, 0),
                                           getattr(session, 'last_turn_at', 0)) >= PARENT_IDLE and
                    not any(not task.done() for message_id, task in self.feed_tasks.items()
                            if self.store.db.execute('SELECT topic FROM messages WHERE id=?',
                                                     (message_id,)).fetchone()[0] == topic)):
                await session.stop()
                self.wound_down.add(topic)
        started = False
        for row in self.store.messages_pending():
            if row['id'] in self.feed_tasks:
                continue
            topic = row['topic']
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
            audio = row['kind'] == 'owner' and self.store.db.execute(
                'SELECT 1 FROM attachments WHERE topic=? AND message=? AND kind IN (?,?,?)',
                (topic, row['telegram_message'], *AUDIO_FIELDS)).fetchone()
            if audio:
                self.feed_tasks[row['id']] = asyncio.create_task(self._deliver_audio(row, session))
                started = True
                continue
            prompt = await self._prepare_message(row, session)
            if prompt is None:
                continue
            self.last_turn[topic] = time.monotonic()
            task = asyncio.create_task(self._send_message(row, prompt, session))
            self.feed_tasks[row['id']] = task
            started = True
        for message_id, task in list(self.feed_tasks.items()):
            if task.done():
                del self.feed_tasks[message_id]
                task.result()
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
        from .setup_flow import blocked
        if self.pair_only or blocked(self.store):
            return False
        started = False
        for saved_row in self.store.db.execute(
                "SELECT * FROM workers WHERE status IN ('queued','running','waiting_for_secret','waiting_for_quota') ORDER BY id").fetchall():
            row = dict(saved_row)
            topic = self.store.topic(row['topic'])
            if topic['chat'] == 0:
                continue
            prior = self.worker_tasks.get(row['id'])
            if prior and prior.done():
                del self.worker_tasks[row['id']]
                prior.result()
            elif prior:
                continue
            if row['status'] == 'waiting_for_quota':
                if (self.store.task_get(row['task']) or {}).get('status') != 'open':
                    with self.store.db:
                        self.store.db.execute("UPDATE workers SET status='interrupted',updated=? "
                                              "WHERE id=? AND status='waiting_for_quota'", (time.time(), row['id']))
                    continue
                if not (self.codex_accounts if row['provider'] == 'codex' else self.accounts).select():
                    continue
                with self.store.db:
                    self.store.db.execute("UPDATE workers SET status='queued',updated=? "
                                          "WHERE id=? AND status='waiting_for_quota'",
                                          (time.time(), row['id']))
                row['status'] = 'queued'
            if row['status'] == 'waiting_for_secret' and waiting_secrets(self.store, self.store.task_get(row['task'])):
                continue
            if row['status'] == 'running':
                from .host import HostClient
                host = HostClient(host_directory(self.store.directory, row))
                state = await host.recover_state()
                if state == 'running':
                    try:
                        await host.connect()
                    except RuntimeError:
                        state = 'dead'
                if state == 'dead':
                    self.store.worker_complete(row['id'], {'success': False, 'session_id': row['session'],
                        'error': 'The provider host stopped without an exit record.',
                        'failure_code': 'interrupted', 'pid': row['pid']}, 'interrupted')
                    started = True
                    continue
            task = asyncio.create_task(self.workers.run(dict(row), self.store.topic(row['topic'])))
            self.worker_tasks[row['id']] = task
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
        requests = self.store.service_requests_pending()
        settled = False
        for request in requests:
            params = json.loads(request['params'])
            op = request['op']
            if op == 'tldr':
                continue
            if op == 'service.restart':
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
            worker = self.workers.get(params['worker'])
            if not worker or worker['task'] is None:
                if op == 'workers.steer':
                    self.refuse_steer(request['id'], params, 'Unknown job worker.')
                else:
                    self.store.service_request_settle(request['id'], 'refused', {'text': 'Unknown job worker.'})
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
                    await self.workers.stop(worker['id'])
                except (OSError, RuntimeError) as error:
                    problems.record(self.store, 'worker', 'stop-failed', internal_detail(error), topic=worker['topic'],
                                    task=worker['task'], worker=worker['id'])
                    self.store.service_request_settle(request['id'], 'uncertain',
                                                      {'text': 'The provider host is unavailable.'})
                    settled = True
                    continue
                task = self.workers.tasks.get(worker['id'])
                if task:
                    await asyncio.gather(task, return_exceptions=True)
                self.store.service_request_settle(request['id'], 'done', {'stop': 'stopped'})
                settled = True
                continue
            if op == 'workers.steer' and worker['status'] == 'needs_input':
                with self.store.db:
                    current = self.workers.get(worker['id'])
                    task = self.worker_tasks.get(worker['id'])
                    if current['status'] == 'needs_input' and (task is None or task.done()):
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
            prompt = params['prompt'] if op == 'workers.steer' else '/goal ' + params['condition']
            self.store.service_request_settle(request['id'], 'sending')
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
            settled = True
        return settled

    def refuse_steer(self, request_id, params, reason, worker=None, result=None):
        topic = worker['topic'] if worker else params.get('_topic')
        with self.store.db:
            self.store.service_request_settle(request_id, 'refused', result or {'text': reason})
            if topic:
                self.store.message_save(topic, 'callback',
                                        'Steer request %d for worker %d was not delivered: %s'
                                        % (request_id, params['worker'], reason))

    def revoke(self, request_id, name):
        from .envelopes import revoke
        from .vault import VaultError
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
        row = self.store.pending_delivery()
        if not row:
            return False
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
            logger.info('delivery failed outbox=%s topic=%s code=%s retry_after=%s',
                        row['id'], row['topic'], error.code, error.retry_after)
            problems.record(self.store, 'outbox', 'send-failed', 'outbox=%s kind=%s telegram=%s reason=%s '
                            'retry_after=%s attempts=%s' % (row['id'], row['kind'], error.code, error.reason,
                                                            error.retry_after, row['attempts'] + 1),
                            topic=row['topic'])
            self.store.delivery_failed(row, error.retry_after)
        return True

    async def deliver(self):
        while True:
            if not await self.deliver_once():
                await pause(self.wake('deliver'), DELIVERY_IDLE)

    async def answer_callback(self, query_id, outcome):
        text = {'topics_off': 'Topics are still off.', 'topics_on': 'Topics are on.',
                'topics_unauthorized': 'Only the Torii owner can do this.', 'unauthorized': 'Only the paired owner can change Torii settings.',
                'already_here': 'Already here.',
                'stale_callback': 'This menu has expired. Open the command again.',
                'duplicate': 'Already received.',
                'envelope_cancel': 'Envelope cancelled.',
                'envelope_arm': 'Send the value as your next message.',
                'envelope_closed': 'This envelope is already closed.'}.get(outcome, 'See the next message.')
        try:
            await self.telegram.call('answerCallbackQuery', callback_query_id=query_id, text=text)
        except TelegramError as error:
            logger.info('callback answer failed code=%s', error.code)
            problems.record(self.store, 'telegram', 'callback-failed', 'telegram=%s' % error.code, every=60)

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
                    for name in ('reactions', 'deliver', 'feed', 'group_setup'):
                        self.wake(name).set()
                for query_id, outcome in answers:
                    await self.answer_callback(query_id, outcome)
            except TelegramError as error:
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
                    error.retry_after, failures), every=60)
                await asyncio.sleep(min(error.retry_after or (1 if failures == 1 else 5), 60))

    async def reactions(self):
        from .reactions import deliver_reaction
        while True:
            if not await deliver_reaction(self.store, self.telegram):
                await pause(self.wake('reactions'), REACTION_IDLE)

    async def command_menu(self):
        from .onboarding import COMMAND_MENU
        installed = None
        while True:
            if not getattr(self, 'poll_succeeded', False):
                await asyncio.sleep(1)
                continue
            scope = (self.store.chat(), self.store.get('owner'))
            if all(scope) and scope != installed:
                try:
                    await self.telegram.call('setMyCommands', commands=COMMAND_MENU,
                                             scope={'type': 'chat_member', 'chat_id': scope[0], 'user_id': scope[1]})
                    installed = scope
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
        raise _Restart()

    async def restart_once(self):
        request = self.store.get('restart_requested_v2')
        if not request or self.store.outbox_has_pending():
            return False
        now = time.time()
        last = self.store.get('last_restart_v2')
        if last is not None and now - last < RESTART_GUARD_SECONDS:
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
        self.restart_reason = request.get('reason')
        self.exit_for_restart()
        return True

    async def restart_watch(self):
        while True:
            if await self.restart_once():
                return
            await asyncio.sleep(0.5)

    async def supervise(self, name, start):
        for attempt in range(1, self.task_restart_limit + 2):
            try:
                return await start()
            except (asyncio.CancelledError, _Restart):
                raise
            except Exception as error:
                logger.exception('task crashed task=%s attempt=%s', name, attempt)
                problems.record(self.store, 'service', 'loop-crashed', 'loop=%s attempt=%s %s' % (
                    name, attempt, internal_detail(error)), level=logging.ERROR)
                if attempt > self.task_restart_limit:
                    raise _TaskFailed('The %s service loop failed %s times.' % (name, attempt))
                await asyncio.sleep(self.task_restart_backoff)

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

    def supervised(self):
        from .account_status import monitor_accounts
        codex = getattr(self.workers.runner, 'binaries', {}).get('codex', 'codex')
        loops = [('poll', self.poll), ('deliver', self.deliver),
                 ('accounts', lambda: monitor_accounts(self.store, self.notice_last_account)),
                 ('codex_accounts', lambda: monitor_codex_accounts(self.store, codex)),
                 ('command_menu', self.command_menu),
                 ('reactions', self.reactions),
                 ('restart_watch', self.restart_watch), ('envelopes', self.envelopes),
                 ('signins', self.signins.run)]
        loops.append(('group_setup', self.group_setup))
        if not self.pair_only:
            loops += [('feed', self.feed), ('workers', self.workers_loop), ('controls', self.controls),
                      ('tldr', self.tldr)]
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
            self.store.put('service_run', {'state': 'running', 'started': time.time(), 'checked': last_check(previous)})

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
        for row in self.store.db.execute("SELECT topic,result FROM workers WHERE status='interrupted' AND updated>?",
                                         (checked if known else run.get('started', 0),)):
            if json.loads(row['result'] or '{}').get('failure_code') == 'interrupted':
                channel = row['topic'] if row['topic'] in enabled else home
                stopped[channel] = stopped.get(channel, 0) + 1
        with self.store.db:
            self.store.put('service_run', dict(run, checked=time.time()))
            for topic in enabled:
                count = stopped.get(topic, 0)
                if count:
                    noun = 'agent was' if count == 1 else 'agents were'
                    self.store.enqueue_report(
                        topic, f'Torii just came back online - {count} {noun} stopped. Ask me to resume them.')
                elif rebooted:
                    self.store.enqueue_report(topic, 'Torii just came back online.')

    async def run(self):
        if self.store.get('mode') == 'private':
            from .setup_flow import PRIVATE_REFUSAL
            with self.store.db:
                self.store.put('setup_problem', 'private_mode_removed')
                self.store.put('pair_only', True)
            logger.error(PRIVATE_REFUSAL)
            await asyncio.Event().wait()
            return
        self.record_start()
        self.store.messages_uncertain()
        self.store.requests_uncertain()
        cause = 'returned'
        try:
            logger.info('claude launch extension=%s', 'native' if type(self.extension) is Native else 'local')
            await self.extension.start()
            from .reactions import check_administrators
            await check_administrators(self.store, self.telegram)
            self.reconcile_steers()
            if not self.pair_only:
                await self.start_session(self.home_topic())
                for topic in self.store.topics():
                    if (topic['enabled'] and topic['id'] != self.home_topic() and
                            self.store.get(self.session_key(topic['id']) + '_host')):
                        await self.start_session(topic['id'])
            logger.info('service run commit=%s pair_only=%s', running_commit(Path(__file__).resolve().parents[1]),
                        self.pair_only)
            await self.announce_start()
            tasks = [asyncio.create_task(self.supervise(name, start)) for name, start in self.supervised()]
        except BaseException as error:
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
            children = tasks + list(self.feed_tasks.values()) + list(self.worker_tasks.values())
            if getattr(self, 'setup_accounts_task', None) is not None:
                children.append(self.setup_accounts_task)
            for task in children:
                task.cancel()
            await asyncio.gather(*children, return_exceptions=True)
            for session in self.sessions.values():
                if hasattr(session, 'detach'):
                    await session.detach()
            await self.extension.close()
