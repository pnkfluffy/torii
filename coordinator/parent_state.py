"""Detached parent lifecycle and message recovery shared by parent providers."""

import asyncio
import json
import logging
import sqlite3
import uuid

from . import problems
from .accounts import AccountBroker
from .host import HostClient
from .policy import PARENT_EFFORT, default_coordinator_model


logger = logging.getLogger(__name__)
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


def topic_enabled(store, key):
    topic = (store.get('coordinator_session_topic') or store.get('coordinator_home_topic')
             if key == 'coordinator' else key.split(':', 1)[1])
    config = store.topic(topic) if topic else None
    return topic is None or topic == store.get('coordinator_home_topic') or bool(config and config['enabled'])


def opencode_pick_problem(store, key):
    from . import opencode_accounts
    if key is None:
        return 'Pick OpenCode for one chat with topic.'
    if key == 'coordinator' or (key.startswith('coordinator:') and key.split(':', 1)[1] in (
            store.get('coordinator_home_topic'), store.get('coordinator_session_topic'))):
        return 'The home chat stays on Claude or ChatGPT.'
    if store.get('opencode_enabled') is not True:
        return 'OpenCode is off. Turn on delegation.opencode first.'
    if opencode_accounts.active(store) is None:
        return 'Select an OpenCode account with opencode.use first.'
    return None


def effective_agent(store, key='coordinator', provider=None, model=None):
    pick = store.get(key + '_selection')
    selection = pick or store.get('parent_selection') or {}
    provider = provider or selection.get('provider') or ('claude' if AccountBroker(store).select() else 'codex')
    matched = selection if selection.get('provider') == provider else {}
    setting, default = (('coordinator_model', default_coordinator_model(store)) if provider == 'claude'
                        else ('codex_model', 'gpt-6.1-sol'))
    selected_model = matched.get('model') or model or store.get(setting) or default
    effort = matched.get('effort') or store.get(provider + '_parent_effort') or PARENT_EFFORT
    if provider == 'opencode':
        from . import opencode_accounts
        selected_model = (opencode_accounts.accounts(store).get(opencode_accounts.active(store)) or {}).get('model')
        effort = None
    return {'provider': provider, 'model': selected_model, 'effort': effort,
            'revision': pick['revision'] if pick else store.get('parent_selection_revision', 0),
            'picked': bool(pick), 'selection': selection}


def selection_changed(session):
    agent = effective_agent(session.store, session.key, model=session.model)
    selected = agent['selection']
    host = session.store.get(session.key + '_host') or {}
    if host.get('picked', False) != agent['picked']:
        return True
    if not selected:
        return False
    provider = host.get('provider', 'claude')
    if provider == 'opencode':
        from . import opencode_accounts
        if host.get('account') != opencode_accounts.active(session.store):
            return True
    if selected['provider'] == 'claude' and provider == 'codex' and AccountBroker(session.store).select() is None:
        return False
    if agent['picked']:
        return (agent['provider'] != provider or agent['model'] != session.model or
                agent['effort'] != host.get('effort') or
                agent['revision'] != session.store.get(session.key + '_selection_revision'))
    return selected['provider'] != provider or selected['model'] != session.model


async def retire(session):
    """Close idle native input and let the host record its exit without sending a signal."""
    try:
        await session.process.request('close_stdin')
    except Exception as error:
        problems.record(session.store, 'coordinator', 'parent-selection-pending',
                        'Native input close failed: ' + type(error).__name__, level=logging.ERROR)


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
    seq = session.store.get(session.key + '_host_seq', 0)
    session.process = HostClient.attach(directory, last_seq=seq, on_seq=session._event_consumed)
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
        session.store.put(session.key + '_turn', None)
    from .session_storage import launch_parent
    launch_parent(session, spec)
    directory = session.runner.state_dir / 'hosts' / session.host_id
    session.process = await HostClient.launch(
        directory, spec, on_seq=session._event_consumed, private=private, record_failure=True)
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
    uncertain = uncertain_summary(uncertain_messages or [])
    encoded = json.dumps(uncertain, separators=(',', ':'), sort_keys=True)
    while len(encoded) > RESTART_MESSAGE_CHARS and uncertain['shown']:
        uncertain['shown'].pop()
        uncertain['not_shown'] += 1
        encoded = json.dumps(uncertain, separators=(',', ':'), sort_keys=True)
    prefix = 'The service restarted. Open jobs: '
    suffix = ('. Messages with uncertain delivery (newest %d at most, text cut to %d characters): ' %
              (RESTART_MESSAGES, RESTART_MESSAGE_HEAD) + encoded +
              '. Inspect current state before acting. Do not repeat completed work. '
              'Use tasks.get for job notes, and tasks.list and workers.list for the rest.')
    return prefix + jobs_summary(store, open_tasks, RESTART_NOTICE_CHARS - len(prefix) - len(suffix)) + suffix
