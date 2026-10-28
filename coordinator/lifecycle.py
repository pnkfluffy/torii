"""Disposition and bounded continuation of the current job round."""

import asyncio
import json
from pathlib import Path
import subprocess
import time

from .control_api import Refused, Result, job_number
from . import problems


RETRY_DELAYS = (0, 120, 600)
REQUIRE_RESULT_LINE = False
CONTINUATION = 'Continue the task; inspect the actual state before repeating effects.\n\n'
CAUSES = {
    'quota': frozenset(('quota_limited', 'out_of_credit')),
    'account': frozenset(('accounts_unavailable', 'auth', 'auth_failed', 'account_changed')),
    'config': frozenset(('model_unavailable', 'provider_managed_config', 'goal_feature_required', 'spawn_refused')),
    'unfinished': frozenset(('goal_unmet', 'goal_failed')),
}


def enabled(store):
    return store.get('auto_resume', False) is True


def install_schema(store):
    columns = store._columns('task_iterations')
    additions = {
        'cause': "TEXT CHECK(cause IN ('crash','unfinished','quota','account','offline','config'))",
        'attempts': 'INTEGER NOT NULL DEFAULT 0', 'resume_at': 'REAL', 'progress_head': 'TEXT',
        'nudged': 'INTEGER NOT NULL DEFAULT 0', 'last_worker': 'INTEGER REFERENCES workers(id)',
    }
    for name, declaration in additions.items():
        if name not in columns:
            store.db.execute('ALTER TABLE task_iterations ADD COLUMN ' + name + ' ' + declaration)
    if 'resume_of' not in store._columns('workers'):
        store.db.execute('ALTER TABLE workers ADD COLUMN resume_of INTEGER REFERENCES workers(id)')
    store.db.execute('CREATE UNIQUE INDEX IF NOT EXISTS workers_resume_of ON workers(resume_of) WHERE resume_of IS NOT NULL')


def op_auto_resume(store, ctx, enabled):
    if enabled and store.get('waits_enabled', False) is not True:
        raise Refused('Enable waits before auto-resume.')
    with store.transaction():
        store.put('auto_resume', enabled)
    return Result(True, 'Job auto-resume is ' + ('on.' if enabled else 'off.'), {'enabled': enabled})


def result_line(payload):
    lines = (payload.get('text') or '').strip().splitlines()
    last = lines[-1].strip() if lines else ''
    if last == 'RESULT: done':
        return 'done'
    for value in ('blocked', 'partial'):
        if last.startswith('RESULT: ' + value + ' ') and last[len('RESULT: ' + value):].strip():
            return value
    return None


def cause(worker, payload):
    if worker['status'] == 'waiting_for_secret':
        return None
    code = payload.get('failure_code')
    if code in ('owner_stopped', 'coordinator_stopped'):
        return code
    marker = result_line(payload)
    unfinished = not payload.get('success') or marker == 'partial' or REQUIRE_RESULT_LINE and marker is None
    finished = not code and not unfinished
    if payload.get('network_down') and not finished:
        return 'offline'
    if worker['status'] == 'waiting_for_quota':
        return 'account' if code in CAUSES['account'] else 'quota'
    for name, codes in CAUSES.items():
        if code in codes:
            return name
    if payload.get('quota_limited'):
        return 'quota'
    if code:
        return 'crash'
    if unfinished:
        return 'unfinished'
    return None


def _head(task):
    if not task['worktree'] or not Path(task['worktree']).is_dir():
        return None
    result = subprocess.run(['git', '-C', task['worktree'], 'rev-parse', 'HEAD'],
                            capture_output=True, text=True, timeout=10)
    return result.stdout.strip() if result.returncode == 0 else None


def held_copy_unavailable(task, worker):
    return task['copy'] == 'held' and (not task['worktree'] or not Path(worker['cwd']).is_dir()
        or Path(worker['cwd']).resolve() != Path(task['worktree']).resolve())


def queue_resume(store, task, worker, counted=False, automatic=False):
    from .host import HostClient
    from .workers import can_resume_finished, host_directory
    if (task['copy'] == 'saving' or held_copy_unavailable(task, worker)
            or HostClient(host_directory(store.directory, worker)).state == 'running'):
        return False
    now = time.time()
    store.db.execute("UPDATE tasks SET status='open',reason=NULL,updated=? WHERE id=?", (now, task['id']))
    resumable = can_resume_finished(store, worker)
    queued = store.db.execute('''INSERT INTO workers
        (task,topic,provider,prompt,cwd,workspace,goal,model,effort,work,session,fresh,account_alias,created,updated,iteration,resume_of)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
        (task['id'], task['topic'], worker['provider'], CONTINUATION + worker['prompt'].removeprefix(CONTINUATION),
         worker['cwd'], worker['workspace'], worker['goal'], worker['model'], worker['effort'], worker['work'],
         worker['session'] if resumable else None, int(not resumable), worker['account_alias'], now, now,
         task['iteration'], worker['id'])).lastrowid
    store.db.execute('UPDATE task_iterations SET attempts=attempts+?,resume_at=NULL WHERE task=? AND k=?',
                     (int(counted), task['id'], task['iteration']))
    if automatic:
        from .waits import _callback
        row = store.task_iterations(task['id'])[-1]
        _callback(store, task['topic'], 'Job %s: worker %d resumed after %s, attempt %d of 3'
                  % (job_number(task['number'], task['iteration']), queued, row['cause'], row['attempts']))
    return queued


class JobLifecycle:
    def __init__(self, service):
        self.service = service
        self.store = service.store

    def last_worker(self, task):
        row = self.store.db.execute('SELECT * FROM workers WHERE task=? AND iteration=? ORDER BY id DESC LIMIT 1',
                                    (task['id'], task['iteration'])).fetchone()
        return dict(row) if row else None

    def active(self, task):
        return self.store.db.execute("SELECT 1 FROM workers WHERE task=? AND status IN ('queued','running')",
                                     (task['id'],)).fetchone() is not None

    def parked(self, task, except_worker=None):
        return self.store.db.execute("""SELECT 1 FROM workers WHERE task=?
            AND status IN ('waiting_for_secret','waiting_for_quota') AND (? IS NULL OR id!=?)""",
            (task['id'], except_worker, except_worker)).fetchone() is not None

    def owner_wait(self, task, ask, key=None):
        with self.store.transaction():
            item = self.store.task_wait(task['topic'], 'owner', ask, task=task['id'],
                key=key or 'owner:disposition:%d:%d' % (task['id'], task['iteration']),
                buttons=[[{'text': 'Resume'}, {'text': 'Drop'}]])
            detail = item['detail']
            detail['auto_resume_jobs'] = sorted(set(detail.get('auto_resume_jobs', [])) | {task['id']})
            self.store.db.execute('UPDATE waits SET detail=? WHERE id=?', (json.dumps(detail), item['id']))
            from .close_saves import request
            request(self.store, task['id'])
            if task['copy'] == 'held':
                self.store.db.execute("UPDATE tasks SET copy='held',copy_reason=? WHERE id=?", (task['copy_reason'], task['id']))

    async def settled(self, topic):
        if not enabled(self.store):
            return
        if self.store.get('waits_enabled', False) is not True:
            return
        callbacks = set()
        for task in self.store.tasks_list(topic, 'open'):
            try:
                await self.settle_task(task, callbacks)
            except Exception as error:
                problems.record(self.store, 'jobs', 'settle-failed', type(error).__name__,
                                topic=topic, task=task['id'])

    async def settle_task(self, task, callbacks):
        topic = task['topic']
        if self.active(task) or self.parked(task) or not self.service.chat_settled(topic, callbacks):
            return
        worker = self.last_worker(task)
        payload = json.loads(worker['result'] or '{}') if worker else {}
        why = cause(worker, payload) if worker else None
        label = job_number(task['number'], task['iteration'])
        if why == 'owner_stopped':
            self.owner_wait(task, 'Job %s: Stopped by you. Resume or drop?' % label)
            return
        if why not in (None, 'coordinator_stopped', 'config'):
            await self.interrupt(task, worker, payload, why, callbacks=callbacks)
            return
        head = await asyncio.to_thread(_head, task)
        with self.store.transaction():
            if (not self.service.chat_settled(topic, callbacks) or self.active(task) or self.parked(task)
                    or self.store.task_get(task['id'])['status'] != 'open'):
                return
            row = self.store.task_iterations(task['id'])[-1]
            if why == 'config':
                self.store.db.execute("UPDATE task_iterations SET cause='config',last_worker=? WHERE id=?", (worker['id'], row['id']))
            if ((payload.get('success') and not payload.get('failure_code') and result_line(payload) in ('done', 'blocked'))
                    or head and row['progress_head'] and head != row['progress_head']):
                self.store.db.execute('UPDATE task_iterations SET attempts=0,progress_head=? WHERE id=?', (head, row['id']))
            if not row['nudged']:
                self.store.db.execute('UPDATE task_iterations SET nudged=1 WHERE id=?', (row['id'],))
                detail = (' Provider configuration needs attention.' if why == 'config' else '')
                from .waits import _callback
                callbacks.add(_callback(self.store, topic,
                    "Job %s has no worker and is still open.%s In this turn: spawn a worker, close it "
                    "(done or dropped with a note), or wait on something with tasks.wait. "
                    "Otherwise it goes on the owner's waiting list." % (label, detail)))
            else:
                self.owner_wait(task, 'Job %s was left undecided after its last result. Resume or drop?' % label)

    async def interrupt(self, task, worker, payload, why, callbacks=None):
        head = await asyncio.to_thread(_head, task)
        detail = problems.sanitize(payload.get('error') or why, 160)
        if why in ('quota', 'account', 'offline') and worker['resume_of'] is not None:
            why = 'crash'
        with self.store.transaction():
            current = self.store.task_get(task['id'])
            row = self.store.task_iterations(task['id'])[-1]
            if (current['status'] != 'open' or self.active(task) or self.parked(task, worker['id'])
                    or worker['status'] == 'waiting_for_secret'
                    or callbacks is not None and not self.service.chat_settled(task['topic'], callbacks)):
                return
            attempts = row['attempts']
            if head and row['progress_head'] and head != row['progress_head']:
                attempts = 0
            self.store.db.execute("UPDATE workers SET status='interrupted',updated=? WHERE id=? AND status='waiting_for_quota'",
                                  (time.time(), worker['id']))
            if why in ('crash', 'unfinished') and attempts >= len(RETRY_DELAYS):
                self.owner_wait(task, 'Job %s stopped 3 times without progress (last detail: %s). Resume or drop?'
                                % (job_number(task['number'], task['iteration']), detail))
                return
            if why == 'account':
                self.store.db.execute('UPDATE task_iterations SET cause=?,last_worker=? WHERE id=?', (why, worker['id'], row['id']))
                self.owner_wait(task, '%s workers need an available account. Pick an account, then resume or drop?'
                                % ('OpenCode' if worker['provider'] == 'opencode' else worker['provider'].capitalize()),
                                key='owner:account:' + worker['provider'])
                return
            when = (time.time() + RETRY_DELAYS[attempts] if why in ('crash', 'unfinished') else
                    payload.get('waiting_until') if why == 'quota' else None)
            self.store.db.execute("UPDATE tasks SET status='interrupted',reason=?,updated=? WHERE id=?", (detail, time.time(), task['id']))
            if why in ('crash', 'unfinished') and task['copy'] == 'present':
                self.store.db.execute("UPDATE tasks SET copy='present' WHERE id=?", (task['id'],))
            if task['copy'] == 'held':
                self.store.db.execute("UPDATE tasks SET copy='held',copy_reason=? WHERE id=?", (task['copy_reason'], task['id']))
            self.store.db.execute('''UPDATE task_iterations SET cause=?,attempts=?,resume_at=?,progress_head=?,last_worker=?,reason=?
                WHERE id=?''', (why, attempts, when, head, worker['id'], detail, row['id']))
            if why in ('quota', 'offline') and task['copy'] != 'held':
                from .close_saves import request
                request(self.store, task['id'])

    async def resume_due(self):
        if not enabled(self.store):
            return
        for item in self.store.waits_list(owner=True):
            try:
                if not item['key'].startswith('owner:account:') or not item['detail'].get('auto_resume_jobs'):
                    continue
                provider = item['key'][len('owner:account:'):]
                broker = self.service.workers.broker(provider)
                if provider == 'opencode':
                    ready = True
                    for link in item['jobs']:
                        source = self.store.db.execute('SELECT * FROM workers WHERE task=? ORDER BY id DESC LIMIT 1', (link['task'],)).fetchone()
                        if source and not await broker.ready(source['account_alias'], source['id']):
                            ready = False
                            break
                else:
                    ready = broker.available() if provider == 'muse' else broker.select()
                if ready:
                    self.store.wait_fire(item['id'], choice='Resume', outcome='account_available')
            except Exception as error:
                problems.record(self.store, 'jobs', 'account-resume-failed', type(error).__name__,
                                topic=item['topic'])
        for task in self.store.tasks_list(status='open'):
            try:
                worker = self.last_worker(task)
                if worker and worker['status'] == 'waiting_for_quota' and not self.active(task):
                    payload = json.loads(worker['result'] or '{}')
                    await self.interrupt(task, worker, payload, cause(worker, payload))
            except Exception as error:
                problems.record(self.store, 'jobs', 'resume-failed', type(error).__name__,
                                topic=task['topic'], task=task['id'])
        for task in self.store.tasks_list(status='interrupted'):
            try:
                from .topics import lifecycle
                if lifecycle(self.store, task['topic']):
                    continue
                row = self.store.task_iterations(task['id'])[-1]
                if not row['cause'] or self.active(task) or self.parked(task):
                    continue
                source = self.store.db.execute('SELECT * FROM workers WHERE id=? AND task=?', (row['last_worker'], task['id'])).fetchone()
                worker = dict(source) if source else None
                if not worker:
                    continue
                broker = self.service.workers.broker(worker['provider'])
                if row['cause'] in ('crash', 'unfinished'):
                    if row['resume_at'] is None or row['resume_at'] > time.time():
                        continue
                elif row['cause'] == 'offline':
                    if not self.service.network_available:
                        continue
                elif row['cause'] == 'quota':
                    if worker['provider'] == 'opencode':
                        ready = await broker.ready(worker.get('account_alias'), worker['id'])
                        if not ready:
                            continue
                    else:
                        ready = broker.available() if worker['provider'] == 'muse' else broker.select()
                    if not ready and (row['resume_at'] is None or row['resume_at'] > time.time()):
                        continue
                elif row['cause'] == 'account':
                    if row['resume_at'] != 0:
                        continue
                else:
                    continue
                if task['copy'] == 'saving':
                    continue
                if held_copy_unavailable(task, worker):
                    self.owner_wait(task, 'Job %s cannot resume its held copy: %s. Resume or drop?'
                        % (job_number(task['number'], task['iteration']),
                           problems.sanitize(task['copy_reason'] or 'The code copy is unavailable', 160).rstrip('.')))
                    continue
                with self.store.transaction():
                    current = self.store.task_get(task['id'])
                    if current['status'] != 'interrupted' or self.active(task):
                        continue
                    queue_resume(self.store, task, worker, row['cause'] in ('crash', 'unfinished'), automatic=True)
            except Exception as error:
                problems.record(self.store, 'jobs', 'resume-failed', type(error).__name__,
                                topic=task['topic'], task=task['id'])
