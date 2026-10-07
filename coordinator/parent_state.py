"""Detached parent lifecycle and message recovery shared by both providers."""

import asyncio
import json
import logging
import sqlite3
import uuid

from . import problems
from .host import HostClient


logger = logging.getLogger(__name__)
RESTART_TASKS = 50
RESTART_TASK_TITLE = 200
RESTART_WORKERS = 30
RESTART_MESSAGES = 10
RESTART_MESSAGE_CHARS = 4000
RESTART_MESSAGE_HEAD = 500
_active_clients = set()
LOST_TURN = ('Your last turn stopped before it finished. These messages are already in this conversation '
             'and have no reply: %s. Check what you already did for them, then answer them.')


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
        logger.info('coordinator session closed session=%s exit=%s', self.session_id, self.process.returncode)
        if not self._stopping and not self.rejected:
            problems.record(self.store, 'coordinator', 'session-closed', 'exit=%s failure=%s' % (
                self.process.returncode, self._failure or 'none'), level=logging.ERROR)


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
        topics = {}
        for row in store.db.execute('SELECT id,topic FROM messages WHERE id IN (%s) ORDER BY id' %
                                    ','.join('?' * len(shown)), shown) if shown else ():
            topics.setdefault(row['topic'], []).append(row['id'])
        for topic, ids in topics.items():
            store.message_save(topic, 'callback', LOST_TURN % ', '.join('message=%d' % item for item in ids))
        store.requeue_messages(rows - set(shown))
        store.put(key + '_lost', None)


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
    if state == 'dead':
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
    env = json.loads((directory / 'spec.json').read_text()).get('env')
    return {'state': state, 'past': past, 'later': later, 'env': env}


async def launch(session, spec, fingerprint, private=None):
    session.host_id = 'coordinator-' + str(uuid.uuid4())
    with session.store.db:
        session.store.put(session.key + '_host', {'id': session.host_id, 'account': session.account_alias,
                          'model': session.model, 'provider': spec['provider'], 'fingerprint': fingerprint})
        session.store.put(session.key + '_host_seq', 0)
        session.store.put(session.key + '_turn', None)
    directory = session.runner.state_dir / 'hosts' / session.host_id
    session.process = await HostClient.launch(directory, spec, on_seq=session._event_consumed, private=private)



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



def open_tasks_summary(rows):
    shown = []
    for row in sorted(rows, key=lambda item: (item['updated'], item['id']), reverse=True)[:RESTART_TASKS]:
        title = row['title']
        title = title if len(title) <= RESTART_TASK_TITLE else title[:RESTART_TASK_TITLE] + '…'
        shown.append({'id': row['id'], 'topic': row['topic'], 'number': row['number'], 'title': title})
    return {'count': len(rows), 'shown': shown, 'not_shown': len(rows) - len(shown)}



def workers_summary(rows):
    shown = [{key: row[key] for key in ('id', 'task', 'provider', 'status')}
             for row in sorted(rows, key=lambda item: item['id'], reverse=True)[:RESTART_WORKERS]]
    return {'count': len(rows), 'shown': shown, 'not_shown': len(rows) - len(shown)}



def restarted_prompt(open_tasks, workers, uncertain_messages=None):
    tasks = json.dumps(open_tasks_summary(open_tasks), separators=(',', ':'), sort_keys=True)
    active = json.dumps(workers_summary(workers), separators=(',', ':'), sort_keys=True)
    uncertain = json.dumps(uncertain_summary(uncertain_messages or []), separators=(',', ':'), sort_keys=True)
    return ('The service restarted. Open jobs: ' + tasks + '. Registered workers: ' + active +
            '. Messages with uncertain delivery (newest %d at most, text cut to %d characters): ' %
            (RESTART_MESSAGES, RESTART_MESSAGE_HEAD) + uncertain +
            '. Inspect current state before acting. Do not repeat completed work. '
            'Use tasks.get for job notes, and tasks.list and workers.list for the rest.')
