"""Detached parent lifecycle and message recovery shared by parent providers."""

import asyncio
import json
import logging
import shutil
import sqlite3
import time
import uuid

from . import problems
from .accounts import AccountBroker
from .host import HostClient
from .policy import COORDINATOR_MODEL, PARENT_EFFORT


logger = logging.getLogger(__name__)
FAILED_START_SECONDS = 900
FAILED_START_MAX_SECONDS = 24 * 3600
RESTART_TASKS = 50
RESTART_TASK_TITLE = 200
RESTART_WORKERS = 30
RESTART_MESSAGES = 10
RESTART_MESSAGE_CHARS = 4000
RESTART_MESSAGE_HEAD = 500
RESTART_NOTICE_CHARS = 2 * RESTART_MESSAGE_CHARS
_active_clients = set()
LOST_TURN = ('Your last turn stopped before it finished. These messages are already in this conversation '
             'and have no reply: %s. Check what you already did for them, then answer them.')


def key_topic(store, key):
    """The chat topic of a parent session key."""
    return (store.get('coordinator_session_topic') or store.get('coordinator_home_topic')
            if key == 'coordinator' else key.split(':', 1)[1])


def topic_enabled(store, key):
    topic = key_topic(store, key)
    config = store.topic(topic) if topic else None
    return topic is None or topic == store.get('coordinator_home_topic') or bool(config and config['enabled'])


def opencode_pick_problem(store):
    from . import opencode_accounts
    if store.get('opencode_enabled') is not True:
        return 'OpenCode is off. Turn on delegation.opencode first.'
    if opencode_accounts.active(store) is None:
        return 'Select an OpenCode account with opencode.use first.'
    return None


def effective_agent(store, key='coordinator', provider=None, model=None):
    pick = store.get(key + '_selection')
    selection = pick or store.get('parent_selection') or {}
    provider = provider or selection.get('provider') or ('claude' if AccountBroker(store).select(model=effective_agent(store, key, 'claude', model)['model']) else 'codex')
    matched = selection if selection.get('provider') == provider else {}
    setting, default = (('coordinator_model', COORDINATOR_MODEL) if provider == 'claude'
                        else ('codex_model', 'gpt-6.2-sol'))
    selected_model = matched.get('model') or model or store.get(setting) or default
    if provider == 'antigravity':
        from .extension import optional
        DEFAULT_MODEL = optional('antigravity_accounts').DEFAULT_MODEL
        selected_model = matched.get('model') or DEFAULT_MODEL
    effort = matched.get('effort') or store.get(provider + '_parent_effort') or PARENT_EFFORT
    if provider in ('claude', 'codex'):
        from .loadout import effective, topic_for_key
        defaults = effective(store, topic_for_key(store, key), base={
            'coordinators.' + provider + '.model': selected_model,
            'coordinators.' + provider + '.effort': effort})[0]['coordinators'][provider]
        selected_model = defaults['model']
        effort = defaults['effort']
    if provider == 'opencode':
        from . import opencode_accounts
        selected_model = (opencode_accounts.accounts(store).get(opencode_accounts.active(store)) or {}).get('model')
        effort = None
    return {'provider': provider, 'model': selected_model, 'effort': effort,
            'revision': pick['revision'] if pick else store.get('parent_selection_revision', 0),
            'picked': bool(pick), 'selection': selection}


def failed_start(store, key, provider):
    """Count this chat's picked agent as out for a while after it did not start.

    The wait doubles with each failure in a row, up to a day.
    """
    with store.db:
        failed = store.get(key + '_start_failed') or {}
        count = failed.get('count', 1) + 1 if failed.get('provider') == provider and not failed.get('missing') else 1
        store.put(key + '_start_failed', {'provider': provider, 'count': count, 'until': time.time() + min(
            FAILED_START_SECONDS * 2 ** (count - 1), FAILED_START_MAX_SECONDS)})


def missing_cli(store, key, provider, binary):
    """Count this chat's picked agent as out while its CLI is missing."""
    failed = {'provider': provider, 'until': 0, 'missing': binary}
    if store.get(key + '_start_failed') != failed:
        with store.db:
            store.put(key + '_start_failed', failed)


def provider_out(store, key, provider, accounts=None, model=None):
    """Whether a provider cannot run this chat now. A ChatGPT pick that is signed out is not out:
    the chat waits for the owner."""
    failed = store.get(key + '_start_failed') or {}
    if failed.get('provider') == provider and (failed.get('until', 0) > time.time() or
                                               failed.get('missing') and not shutil.which(failed['missing'])):
        return True
    if provider == 'claude':
        return (accounts or AccountBroker(store)).select(
            model=effective_agent(store, key, 'claude', model)['model']) is None
    if provider == 'antigravity':
        from .extension import optional
        return optional('antigravity_parent').out(store, key)
    if provider == 'opencode':
        from . import opencode_runtime
        from .session import AccountsUnavailable
        try:
            opencode_runtime.parent_ready(store)
        except AccountsUnavailable:
            return True
        return False
    if provider == 'codex':
        from .codex_accounts import CodexBroker
        codex = CodexBroker(store)
        return (codex.signed_in(codex.active()) or key == 'coordinator') and codex.parent_account() is None
    return False


def providers(store, key):
    """The providers that may run this chat, in order: its selection, then the general route list.

    A chat moves to the next of them while its own provider is out. On each provider it runs
    that provider's chat-agent model and thinking. A chat with a plugin on skips Antigravity.
    """
    from . import moves
    from .claude_settings import plugin_switches
    from .extension import optional
    own = effective_agent(store, key)['selection'].get('provider') or 'claude'
    topic = key_topic(store, key)
    order = [own]
    for entry in moves.chain(store, {'provider': own, 'model': None, 'topic': topic}):
        name = entry['provider']
        if name not in order and not (name == 'antigravity' and (
                optional('antigravity_parent') is None or any(plugin_switches(store, topic).values()))):
            order.append(name)
    return order


def selection_changed(session):
    agent = effective_agent(session.store, session.key, model=session.model)
    selected = agent['selection']
    host = session.store.get(session.key + '_host') or {}
    if host.get('picked', False) != agent['picked']:
        return True
    provider = host.get('provider', 'claude')
    if not selected:
        return provider != 'claude' and not provider_out(session.store, session.key, 'claude')
    if provider == 'opencode':
        from . import opencode_accounts
        if host.get('account') != opencode_accounts.active(session.store):
            return True
    if (provider != selected['provider'] and provider_out(session.store, session.key, selected['provider'])
            and provider in providers(session.store, session.key)):
        backup = effective_agent(session.store, session.key, provider, session.model)
        if not agent['picked']:
            return backup['model'] != session.model
        return (backup['model'] != session.model or backup['effort'] != host.get('effort') or
                agent['revision'] != session.store.get(session.key + '_selection_revision'))
    if agent['picked']:
        return (agent['provider'] != provider or agent['model'] != session.model or
                agent['effort'] != host.get('effort') or
                agent['revision'] != session.store.get(session.key + '_selection_revision'))
    return agent['provider'] != provider or agent['model'] != session.model


async def retire(session):
    """Close idle native input and let the host record its exit without sending a signal."""
    try:
        await session.process.request('close_stdin')
    except Exception as error:
        problems.record(session.store, 'coordinator', 'parent-selection-pending',
                        'Native input close failed: ' + type(error).__name__, level=logging.ERROR, every=3600)


async def monitor(self):
    try:
        await self.process.wait()
        await asyncio.gather(self.tasks[0], return_exceptions=True)
    finally:
        self.closed = True
        self.busy = False
        self.protocol.disconnect()
        self.tasks[0].cancel()
        await asyncio.gather(self.tasks[0], return_exceptions=True)
        self.tasks[1].cancel()
        await asyncio.gather(self.tasks[1], return_exceptions=True)
        try:
            await self.control.wait_closed()
        finally:
            self.process.detach()
            if self.process.returncode is not None and (self.store.get(self.key + '_host') or {}).get('id') == self.host_id:
                with self.store.db:
                    self.store.put(self.key + '_host', None)
            _active_clients.discard((str((self.runner.state_dir / 'hosts').resolve()), self.key))
            if self.rejected and self.process.returncode is not None:
                try:
                    self._settle(self._relaunch_rows)
                except sqlite3.Error as error:
                    logger.warning('relaunched messages not settled session=%s type=%s',
                                   self.session_id, type(error).__name__)
            if self._unreplayed and not self.rejected and self.process.returncode is not None:
                try:
                    self.store.messages_unconfirmed(self._unreplayed, 'session_closed')
                except sqlite3.Error as error:
                    logger.warning('unreplayed messages not marked uncertain session=%s type=%s',
                                   self.session_id, type(error).__name__)
            if getattr(self, 'provider', None) == 'opencode':
                logger.info('coordinator session closed session=%s exit=%s failure=%s detail=%s',
                            self.session_id, self.process.returncode, self._failure or 'none',
                            getattr(self, '_failure_detail', None))
            else:
                logger.info('coordinator session closed session=%s exit=%s', self.session_id, self.process.returncode)
            if not self._stopping and not self.rejected:
                detail = 'exit=%s failure=%s' % (self.process.returncode, self._failure or 'none')
                if getattr(self, '_failure_detail', None):
                    detail += ' ' + self._failure_detail
                problems.record(self.store, 'coordinator', 'session-closed', detail, level=logging.ERROR, every=300)


def track(self, rows):
    """Keep the messages of the running turn in the store, so a parent reattached after a service
    restart still knows them if that turn fails."""
    self._turn_rows = rows
    with self.store.db:
        self.store.put(self.key + '_turn', sorted(rows))


def settle(store, key, rows, recorded):
    rows = set(rows)
    shown = sorted(rows & set(recorded(rows)))
    with store.db:
        recovery = store.get(key + '_lost_turns') or {}
        topics = {}
        messages = store.db.execute('SELECT id,topic FROM messages WHERE id IN (%s) ORDER BY id' %
                                    ','.join('?' * len(shown)), shown).fetchall() if shown else ()
        for row in messages:
            topics.setdefault(row['topic'], []).append(row['id'])
        for topic, ids in topics.items():
            callback = store.message_save(topic, 'callback', LOST_TURN % ', '.join('message=%d' % item for item in ids))
            recovery[str(callback['id'])] = sorted({row for item in ids for row in recovery.get(str(item), [item])})
        store.put(key + '_lost_turns', recovery or None)
        store.requeue_messages(rows - set(shown))
        store.put(key + '_lost', None)


def recover_lost_turns(store, key, switched):
    with store.db:
        recovery = store.get(key + '_lost_turns') or {}
        callbacks = list(recovery)
        pending = {str(row['id']) for row in store.messages_pending()}
        recovery = {callback: rows for callback, rows in recovery.items() if callback in pending}
        if switched:
            store.requeue_messages({row for rows in recovery.values() for row in rows})
            store.db.executemany("UPDATE messages SET delivered='received',receipt='superseded' WHERE id=?",
                                 [(int(callback),) for callback in callbacks])
            recovery = {}
        store.put(key + '_lost_turns', recovery or None)


async def attach(session, saved_host, fingerprint):
    if not saved_host:
        return None
    directory = session.runner.state_dir / 'hosts' / saved_host['id']
    state = await HostClient(directory).recover_state()
    if state == 'running':
        try:
            await HostClient(directory).connect()
        except RuntimeError:
            state = 'dead'
    if state == 'dead' or HostClient(directory).logs_deleted:
        with session.store.db:
            session.store.put(session.key + '_host', None)
        return None
    session.host_id = saved_host['id']
    session.account_alias = saved_host['account']
    session.model = saved_host['model']
    session.outdated = saved_host.get('fingerprint') != fingerprint
    key = session.key + '_host_seq'
    seq = max(session.store.get(key, 0), session.store.consumed.get(key, 0))
    session.process = HostClient.attach(directory, last_seq=seq, on_seq=session.store.consumer(key))
    past, later = [], []
    try:
        past = list(session.process.history(seq))
        later = list(session.process.history_after(seq))
        session._unread_reattach_events = len(later)
    except (OSError, ValueError, KeyError, TypeError):
        session._recovery_uncertain = True
    spec = json.loads((directory / 'spec.json').read_text())
    env = spec.get('env')
    if spec.get('provider') in ('claude', 'codex', 'opencode'):
        from .session_storage import launch_parent
        if 'fresh' not in spec:
            spec['fresh'] = ('--session-id' in spec.get('argv', []) if spec['provider'] == 'claude' else
                             not any(name.startswith(spec['provider'] + '-') for name in spec.get('locks', [])))
        launch_parent(session, spec)
    return {'state': state, 'past': past, 'later': later, 'env': env}


async def launch(session, spec, fingerprint, private=None):
    if not topic_enabled(session.store, session.key):
        return False
    session.host_id = 'coordinator-' + str(uuid.uuid4())
    agent = effective_agent(session.store, session.key, spec['provider'], session.model)
    host = {'id': session.host_id, 'account': session.account_alias, 'model': session.model,
            'provider': spec['provider'], 'fingerprint': fingerprint}
    if agent['picked']:
        host.update(picked=True, effort=agent['effort'])
    with session.store.db:
        session.store.put(session.key + '_host', host)
        session.store.put(session.key + '_selection_revision', agent['revision'])
        session.store.put(session.key + '_host_seq', 0)
        session.store.consumed.pop(session.key + '_host_seq', None)
        session.store.put(session.key + '_turn', None)
    from .session_storage import launch_parent
    launch_parent(session, spec)
    directory = session.runner.state_dir / 'hosts' / session.host_id
    session.process = await HostClient.launch(
        directory, spec, on_seq=session.store.consumer(session.key + '_host_seq'), private=private, record_failure=True)
    return True



def uncertain_summary(rows):
    """Bound the uncertain messages a restart prompt repeats: the newest few, each cut short."""
    shown = []
    size = 0
    for row in sorted(rows, key=lambda item: item['id'], reverse=True)[:RESTART_MESSAGES]:
        text = row['text']
        text = text if len(text) <= RESTART_MESSAGE_HEAD else text[:RESTART_MESSAGE_HEAD] + '…'
        if size + len(text) > RESTART_MESSAGE_CHARS:
            break
        shown.append(dict(row, text=text))
        size += len(text)
    return {'count': len(rows), 'shown': shown, 'not_shown': len(rows) - len(shown)}



def jobs_summary(store, open_tasks, limit=RESTART_NOTICE_CHARS):
    task_ids = {row['id'] for row in open_tasks}
    live = [dict(worker) for worker in store.db.execute(
        "SELECT id,task,provider,status,work FROM workers "
        "WHERE status IN ('queued','waiting_for_secret','waiting_for_quota','running') ORDER BY id DESC")
        if worker['task'] in task_ids] if task_ids else []
    live_tasks = {worker['task'] for worker in live}
    summary = {'live_workers': live[:RESTART_WORKERS], 'live_workers_not_shown': max(0, len(live) - RESTART_WORKERS),
               'count': len(open_tasks), 'shown': [], 'not_shown': len(open_tasks)}
    while len(json.dumps(summary, separators=(',', ':'), sort_keys=True)) > limit and summary['live_workers']:
        summary['live_workers'].pop()
        summary['live_workers_not_shown'] += 1
    ordered = sorted(open_tasks, key=lambda item: (item['id'] in live_tasks, item['updated'], item['id']), reverse=True)
    for row in ordered[:RESTART_TASKS]:
        stage = store.db.execute('SELECT stage FROM task_stamps WHERE task=? ORDER BY id DESC LIMIT 1',
                                 (row['id'],)).fetchone()
        finished = store.db.execute(
            "SELECT id,result FROM workers WHERE task=? AND status='done' ORDER BY updated DESC,id DESC LIMIT 1",
            (row['id'],)).fetchone()
        result = json.loads(finished['result'] or '{}') if finished else {}
        title = row['title']
        job = {'id': row['id'], 'topic': row['topic'], 'number': row['number'],
               'title': title if len(title) <= RESTART_TASK_TITLE else title[:RESTART_TASK_TITLE] + '…',
               'stage': stage['stage'] if stage else None, 'notes': (row['notes'] or '')[:500],
               'finished': {
                   'id': finished['id'], 'success': result.get('success'), 'failure_code': result.get('failure_code'),
                   'text': (result.get('text') or '')[:200]} if finished else None}
        summary['shown'].append(job)
        summary['not_shown'] -= 1
        encoded = json.dumps(summary, separators=(',', ':'), sort_keys=True)
        fields = [(job, 'notes')]
        if job['finished']:
            fields.append((job['finished'], 'text'))
        fields.append((job, 'title'))
        for item, field in fields:
            if len(encoded) <= limit:
                break
            text = item[field]
            item[field] = ''
            encoded = json.dumps(summary, separators=(',', ':'), sort_keys=True)
            if len(encoded) <= limit:
                low, high = -1, len(text)
                while low < high:
                    middle = (low + high + 1) // 2
                    item[field] = text[:middle] + '…'
                    candidate = json.dumps(summary, separators=(',', ':'), sort_keys=True)
                    if len(candidate) <= limit:
                        low = middle
                    else:
                        high = middle - 1
                item[field] = text[:low] + '…' if low >= 0 else ''
                encoded = json.dumps(summary, separators=(',', ':'), sort_keys=True)
        if len(encoded) > limit:
            summary['shown'].pop()
            summary['not_shown'] += 1
            break
    return json.dumps(summary, separators=(',', ':'), sort_keys=True)


def restarted_prompt(open_tasks, uncertain_messages=None, store=None):
    summary = json.loads(jobs_summary(store, open_tasks))
    jobs = {row['id']: row for row in open_tasks}
    def label(job):
        topic = store.topic(job['topic'])
        return 'Job %s (#%s, %s [%s])' % (job['id'], job['number'], topic['name'], job['topic'])
    lines = ['The service restarted. Open jobs:']
    for worker in summary['live_workers']:
        lines.append('Worker %s: %s, %s, %s' % (worker['id'], label(jobs[worker['task']]), worker['provider'], worker['status']))
    if summary['live_workers_not_shown']:
        lines.append('%d live workers not shown.' % summary['live_workers_not_shown'])
    for job in summary['shown']:
        line = '%s: %s' % (label(job), job['title'])
        if job['finished']:
            result = job['finished']
            line += ' | Worker %s: %s %s' % (result['id'], result['failure_code'] or ('succeeded' if result['success'] else 'failed'), result['text'].replace('\n', ' '))
        lines.append(line)
    if summary['not_shown']:
        lines.append('%d open jobs not shown.' % summary['not_shown'])
    guidance = 'Inspect current state before acting. Do not repeat completed work. Use tasks.get for job notes, and tasks.list and workers.list for the rest.'
    messages = []
    for row in uncertain_messages or []:
        row = dict(row)
        if row['kind'] == 'worker_result':
            try:
                result = json.loads(row['text'])
                row['text'] = 'Worker %s: %s %s' % (result.get('worker'), result.get('status'), result.get('text', ''))
            except (ValueError, AttributeError):
                pass
        messages.append(row)
    uncertain = uncertain_summary(messages)
    for row in uncertain['shown']:
        text = row['text']
        lines.append('Uncertain message %s (%s): %s' % (row['id'], row['kind'], text.replace('\n', ' ')))
    if uncertain['not_shown']:
        lines.append('%d uncertain messages not shown.' % uncertain['not_shown'])
    kept = []
    for line in lines:
        candidate = '\n'.join(kept + [line, 'Some entries omitted.', guidance])
        if len(candidate) > RESTART_NOTICE_CHARS:
            kept.append('Some entries omitted.')
            break
        kept.append(line)
    return '\n'.join(kept + [guidance])[:RESTART_NOTICE_CHARS]
