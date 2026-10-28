"""Durable triggers, their waiting jobs, and the owner's pending decisions."""

from datetime import datetime, timezone
from pathlib import Path
import asyncio
import hashlib
import json
import re
import time

from .control_api import WAIT_KINDS


COMMIT = re.compile(r'[a-fA-F0-9]{40,64}\Z')
SIGNAL = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,199}\Z')
FIXED_SIGNALS = frozenset(('topic.closed', 'worker.reattached', 'service.started', 'update.landed'))


WAIT_BUTTON = re.compile(r'wait:([0-9]{1,18}):([0-9]{1,18})\Z')


def install_schema(store):
    store.db.execute('''CREATE TABLE IF NOT EXISTS waits (
        id INTEGER PRIMARY KEY, kind TEXT NOT NULL
            CHECK(kind IN ('owner','time','job','pr','torii_update','signal')), key TEXT NOT NULL,
        topic TEXT NOT NULL REFERENCES topics(id), ask TEXT NOT NULL,
        detail TEXT NOT NULL DEFAULT '{}', ask_outbox INTEGER REFERENCES outbox(id),
        state TEXT NOT NULL DEFAULT 'armed' CHECK(state IN ('armed','fired','cancelled')),
        outcome TEXT, fired_by TEXT, reason TEXT, next_check REAL,
        checks INTEGER NOT NULL DEFAULT 0, created REAL NOT NULL, updated REAL NOT NULL)''')
    store.db.execute("CREATE UNIQUE INDEX IF NOT EXISTS waits_armed_key ON waits(key) WHERE state='armed'")
    store.db.execute('''CREATE INDEX IF NOT EXISTS waits_due ON waits(next_check)
        WHERE state='armed' AND next_check IS NOT NULL''')
    store.db.execute('''CREATE TABLE IF NOT EXISTS wait_jobs (
        wait INTEGER NOT NULL REFERENCES waits(id), task INTEGER NOT NULL REFERENCES tasks(id),
        blocking INTEGER NOT NULL DEFAULT 1 CHECK(blocking IN (0,1)),
        then_provider TEXT, then_prompt TEXT, then_model TEXT, then_effort TEXT, then_work TEXT,
        created REAL NOT NULL, PRIMARY KEY(wait,task))''')
    store.db.execute('CREATE INDEX IF NOT EXISTS wait_jobs_task ON wait_jobs(task,wait)')
    if 'wait' not in store._columns('task_iterations'):
        store.db.execute('ALTER TABLE task_iterations ADD COLUMN wait INTEGER REFERENCES waits(id)')


def _callback(store, topic, text):
    now = time.time()
    return store.db.execute('''INSERT INTO messages(topic,kind,text,created,updated)
        VALUES (?,'callback',?,?,?)''', (topic, text, now, now)).lastrowid


def _at(value):
    try:
        date = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if date.tzinfo is None:
            raise ValueError()
        return date.astimezone(timezone.utc).timestamp()
    except (ValueError, OverflowError, AttributeError):
        raise ValueError('at must be an ISO 8601 date with a timezone.') from None


def _short(text, limit):
    text = ' '.join(text.split())
    return text if len(text) <= limit else text[:limit - 1] + '…'


def _keyboard(wait, buttons):
    keyboard = []
    index = 0
    for row in buttons:
        keyboard.append([{'text': button['text'], 'callback_data': 'wait:%d:%d' % (wait, index + offset)}
                         for offset, button in enumerate(row)])
        index += len(row)
    return {'inline_keyboard': keyboard} if keyboard else None


def _wait_status(store, item):
    title = ', '.join(job['title'] for job in item['jobs']) or store.topic(item['topic'])['name']
    status = {'armed': 'Waiting', 'fired': 'Ready to continue', 'cancelled': 'No longer waiting'}[item['state']]
    return '%s: %s.' % (title, status)


class WaitStore:
    def wait_get(self, wait):
        row = self.db.execute('SELECT * FROM waits WHERE id=?', (wait,)).fetchone()
        if row is None:
            return None
        result = dict(row)
        result['detail'] = json.loads(result['detail'])
        result['jobs'] = [dict(row) for row in self.db.execute('''SELECT j.*,t.number,t.iteration,t.topic,t.status,t.title
            FROM wait_jobs j JOIN tasks t ON t.id=j.task WHERE j.wait=? ORDER BY j.task''', (wait,))]
        return result

    def waits_list(self, topic=None, owner=False):
        if not self.get('waits_enabled', False):
            return []
        rows = self.db.execute('''SELECT id FROM waits WHERE state='armed'
            AND (? IS NULL OR topic=? OR EXISTS (SELECT 1 FROM wait_jobs j JOIN tasks t ON t.id=j.task
                WHERE j.wait=waits.id AND t.topic=?)) AND (?=0 OR kind='owner') ORDER BY id''',
                               (topic, topic, topic, int(owner))).fetchall()
        return [self.wait_get(row['id']) for row in rows]

    def wait_due(self, now=None):
        if not self.get('waits_enabled', False):
            return []
        return [self.wait_get(row['id']) for row in self.db.execute('''SELECT id FROM waits
            WHERE state='armed' AND kind='time' AND next_check<=? ORDER BY next_check,id''',
            (time.time() if now is None else now,)).fetchall()]

    def task_wait(self, topic, kind, ask, task=None, key=None, at=None, job=None, on=None,
                  buttons=None, watch=False, then_provider=None, then_prompt=None,
                  then_model=None, then_effort=None, then_work=None, pr=None, until=None,
                  commit=None, signal=None, next=None):
        if not self.get('waits_enabled', False):
            raise ValueError("Waits aren't switched on yet.")
        if kind not in WAIT_KINDS:
            raise ValueError('Wait kind must be ' + ', '.join(WAIT_KINDS) + '.')
        if not isinstance(ask, str) or not ask.strip():
            raise ValueError('A wait needs an ask.')
        ask = ask.strip()
        if not self.topic(topic) or self.get('topic_lifecycle:' + topic):
            raise ValueError('This chat is unknown, closing or closed.')
        linked = self.task_get(task) if task is not None else None
        if task is not None and (not linked or linked['topic'] != topic):
            raise ValueError('Unknown job in this chat.')
        if then_provider is None and any(value is not None for value in
                                         (then_prompt, then_model, then_effort, then_work)):
            raise ValueError('Supply then_provider and then_prompt together.')
        if then_provider is not None and (not then_prompt or linked is None):
            raise ValueError('A declared worker needs a job and then_prompt.')
        supplied = {'at': at, 'job': job, 'on': on, 'buttons': buttons, 'pr': pr,
                    'until': until, 'commit': commit, 'signal': signal, 'next': next}
        allowed = {'owner': {'buttons'}, 'time': {'at'}, 'job': {'job', 'on'},
                   'pr': {'pr', 'until', 'next'}, 'torii_update': {'commit'}, 'signal': {'signal'}}
        if any(value is not None and name not in allowed[kind] for name, value in supplied.items()):
            raise ValueError('Unexpected fields for a %s wait.' % kind)
        detail = {}
        next_check = None
        waiter = 'task:%d' % task if task is not None else 'topic:' + topic
        ask_hash = hashlib.sha256(ask.encode('utf-8')).hexdigest()[:12]
        if kind == 'time':
            if any(value is not None for value in (job, on, buttons)):
                raise ValueError('A time wait takes at, not job, on or buttons.')
            next_check = _at(at)
            detail = {'at': datetime.fromtimestamp(next_check, timezone.utc).isoformat()}
            default_key = 'time:%s:%s:%s' % (waiter, detail['at'], ask_hash)
        elif kind == 'job':
            target = self.task_get(job) if job is not None else None
            if not target or job == task:
                raise ValueError('A job wait needs another job.')
            if on not in (None, 'done', 'closed') or at is not None or buttons is not None:
                raise ValueError('A job wait takes job and on=done or closed.')
            detail = {'job': job, 'on': on or 'done'}
            default_key = 'job:%s:%d:%s:%s' % (waiter, job, detail['on'], ask_hash)
        elif kind == 'pr':
            self.validate_task_pr(pr)
            from .store import PR
            match = PR.fullmatch(pr)
            if len(match[2]) > 10 or int(match[2]) > 2147483647:
                raise ValueError('PR number is too large.')
            if until not in (None, 'checks', 'merged'):
                raise ValueError('A PR wait needs until=checks or merged.')
            detail = {'url': 'https://github.com/%s/pull/%s' % (match[1].lower(), match[2]),
                      'until': until or 'merged'}
            if next is not None:
                if (not isinstance(next, dict) or next.get('kind') != 'torii_update'
                        or set(next) - {'kind', 'commit'}):
                    raise ValueError('next must declare kind=torii_update and an optional commit.')
                if 'commit' in next and (not isinstance(next['commit'], str) or not COMMIT.fullmatch(next['commit'])):
                    raise ValueError('The follow-up commit must be a full commit hash.')
                detail['next'] = dict(next)
            next_check = time.time()
            default_key = 'pr:%s:%s:%s:%s' % (waiter, detail['url'], detail['until'], ask_hash)
        elif kind == 'torii_update':
            if not isinstance(commit, str) or not COMMIT.fullmatch(commit):
                raise ValueError('A Torii update wait needs a full commit hash.')
            detail = {'commit': commit.lower()}
            default_key = 'update:%s:%s:%s' % (waiter, detail['commit'], ask_hash)
        elif kind == 'signal':
            if not isinstance(signal, str) or not SIGNAL.fullmatch(signal):
                raise ValueError('A signal wait needs a name of 1 to 200 letters, digits, dots, colons, underscores or hyphens.')
            detail = {'name': signal}
            default_key = 'signal:%s:%s:%s' % (waiter, signal, ask_hash)
        else:
            if any(value is not None for value in (at, job, on)):
                raise ValueError('An owner wait takes buttons, not at, job or on.')
            from .formatting import markdown_to_html
            if len(ask) > 4096 or len(markdown_to_html(ask)) > 4096:
                raise ValueError('The owner ask must fit in one Telegram message.')
            detail = {'buttons': buttons or []}
            default_key = None
        with self.transaction():
            if self.get('topic_lifecycle:' + topic):
                raise ValueError('This chat is closing or closed.')
            linked = self.task_get(task) if task is not None else None
            if watch and (not linked or linked['status'] not in ('done', 'dropped')):
                raise ValueError('A watch needs a done or dropped job.')
            if linked and linked['status'] in ('done', 'dropped') and not watch:
                raise ValueError('A done or dropped job needs a non-blocking watch.')
            if linked and self.db.execute("""SELECT 1 FROM workers WHERE task=?
                    AND status IN ('queued','running','waiting_for_secret','waiting_for_quota')""", (task,)).fetchone():
                raise ValueError('Finish or stop the job worker before waiting.')
            existing = self.db.execute("SELECT id,kind,detail FROM waits WHERE key=? AND state='armed'",
                                       (key or default_key,)).fetchone()
            trigger = json.loads(existing['detail']) if existing else {}
            for name in ('head_sha', 'pending_since', 'not_found', 'arm_checked'):
                trigger.pop(name, None)
            if existing and (existing['kind'] != kind or kind != 'owner' and trigger != detail):
                raise ValueError('That key already names a different trigger.')
            if linked and kind == 'owner' and not watch:
                self._replace_owner_waits(task, existing['id'] if existing else None)
            if task is None and kind == 'owner' and not key:
                for previous in self.waits_list(topic, owner=True):
                    if previous['topic'] == topic and not previous['jobs']:
                        self.wait_cancel(previous['id'], 'replaced by a new ask')
            now = time.time()
            if existing:
                wait = existing['id']
            else:
                outbox = self.enqueue_report(topic, ask, kind='wait-ask') if kind == 'owner' else None
                wait = self.db.execute('''INSERT INTO waits(kind,key,topic,ask,detail,next_check,created,updated)
                    VALUES (?,?,?,?,?,?,?,?)''', (kind, key or default_key or 'owner:%s:%d' % (topic, outbox), topic, ask.strip(),
                    json.dumps(detail), next_check, now, now)).lastrowid
                if kind in ('time', 'pr', 'torii_update'):
                    self.db.execute('''INSERT INTO service_requests(op,params,created,updated)
                        SELECT 'waits.changed','{}',?,? WHERE NOT EXISTS (SELECT 1 FROM service_requests
                            WHERE op='waits.changed' AND state='queued')''', (now, now))
                if kind == 'owner':
                    markup = _keyboard(wait, buttons or [])
                    self.db.execute('UPDATE outbox SET reply_markup=? WHERE id=?',
                                    (json.dumps(markup) if markup else None, outbox))
                    self.db.execute('UPDATE waits SET ask_outbox=?,key=? WHERE id=?',
                                    (outbox, key or 'owner:%s:%d' % (topic, outbox), wait))
            if linked:
                if kind == 'pr':
                    self.task_update(task, pr=detail['url'])
                self.db.execute('''INSERT OR IGNORE INTO wait_jobs
                    (wait,task,blocking,then_provider,then_prompt,then_model,then_effort,then_work,created)
                    VALUES (?,?,?,?,?,?,?,?,?)''', (wait, task, int(not watch), then_provider, then_prompt,
                                                   then_model, then_effort, then_work, now))
                if not watch:
                    self.db.execute("UPDATE tasks SET status='waiting',reason=?,updated=? WHERE id=?",
                                    (ask.strip(), now, task))
                    self.db.execute('''UPDATE task_iterations SET closed=?,outcome='waiting',reason=?
                        WHERE task=? AND k=? AND closed IS NULL''', (now, ask.strip(), task, linked['iteration']))
                    from .reactions import set_desired
                    round_row = self.task_iterations(task)[-1]
                    set_desired(self, round_row['origin'] or linked['origin'], None)
                    self.wait_task_changed(task, 'waiting')
            if kind == 'job':
                self.wait_task_changed(job, self.task_get(job)['status'])
            self.refresh_owner_waiting()
        return self.wait_get(wait)

    def _replace_owner_waits(self, task, keep):
        rows = self.db.execute('''SELECT w.id FROM waits w JOIN wait_jobs j ON j.wait=w.id
            WHERE j.task=? AND j.blocking=1 AND w.kind='owner' AND w.state='armed'
                AND w.id IS NOT ?''', (task, keep)).fetchall()
        for row in rows:
            count = self.db.execute('SELECT COUNT(*) FROM wait_jobs WHERE wait=?', (row['id'],)).fetchone()[0]
            if count > 1:
                self.db.execute('DELETE FROM wait_jobs WHERE wait=? AND task=?', (row['id'], task))
            else:
                self.wait_cancel(row['id'], 'replaced by a new ask', resume=False)

    def wait_end_blocking(self, task, reason):
        if not self.get('waits_enabled', False):
            return
        with self.transaction():
            rows = self.db.execute('''SELECT w.id FROM waits w JOIN wait_jobs j ON j.wait=w.id
                WHERE j.task=? AND j.blocking=1 AND w.state='armed' ORDER BY w.id''', (task,)).fetchall()
            for row in rows:
                count = self.db.execute('SELECT COUNT(*) FROM wait_jobs WHERE wait=?', (row['id'],)).fetchone()[0]
                if count > 1:
                    self.db.execute('DELETE FROM wait_jobs WHERE wait=? AND task=?', (row['id'], task))
                else:
                    self.wait_cancel(row['id'], reason, resume=False)
            self.refresh_owner_waiting()

    def wait_resume_unblocked(self, reason='The wait ended without another trigger.'):
        if not self.get('waits_enabled', False):
            return
        with self.transaction():
            rows = self.db.execute('''SELECT t.id,t.topic,t.title FROM tasks t WHERE t.status='waiting'
                AND NOT EXISTS (SELECT 1 FROM wait_jobs j JOIN waits w ON w.id=j.wait
                    WHERE j.task=t.id AND j.blocking=1 AND w.state='armed') ORDER BY t.id''').fetchall()
            for row in rows:
                if self.get('topic_lifecycle:' + row['topic']):
                    continue
                message = _callback(self, row['topic'], '%s is ready for a next step. %s' % (row['title'], reason))
                self.wait_open_round(row['id'], message, reason=reason)

    def wait_resolve_task(self, task, message):
        if not self.get('waits_enabled', False):
            return
        with self.transaction():
            row = self.db.execute('''SELECT w.id FROM waits w JOIN wait_jobs j ON j.wait=w.id
                WHERE j.task=? AND j.blocking=1 AND w.kind='owner' AND w.state='armed'
                ORDER BY w.id LIMIT 1''', (task,)).fetchone()
            if row is None:
                return
            item = self.wait_get(row['id'])
            if len(item['jobs']) == 1:
                self.wait_fire(item['id'], message, skip_worker=task)
            else:
                self.db.execute('DELETE FROM wait_jobs WHERE wait=? AND task=?', (item['id'], task))
                job = self.wait_open_round(task, message, wait=item['id'], reason='answered')
                _callback(self, job['topic'], '%s opened after your answer. Decide the next step.' % job['title'])
                self.refresh_owner_waiting()

    def _clear_owner_buttons(self, item):
        if not self.get('waits_enabled', False) or item['kind'] != 'owner' or item['ask_outbox'] is None:
            return
        row = self.db.execute('SELECT * FROM outbox WHERE id=?', (item['ask_outbox'],)).fetchone()
        if row is None:
            return
        if not row['delivered']:
            if row['edit_message'] is None:
                self.db.execute('UPDATE outbox SET retired=1 WHERE id=?', (row['id'],))
            return
        if row['reply_markup']:
            outbox = self.enqueue_report(item['topic'], row['text'], kind='wait-ask', edit=row['telegram_message'])
            self.db.execute('UPDATE waits SET ask_outbox=? WHERE id=?', (outbox, item['id']))

    def wait_open_round(self, task, message, wait=None, opened_by='wait', reason=None):
        with self.transaction():
            job = self.task_get(task)
            self.wait_end_blocking(task, reason or 'job round reopened')
            if job['status'] == 'open':
                return job
            now = time.time()
            k = job['iteration'] + 1
            self.db.execute('''INSERT INTO task_iterations(task,k,opened,opened_by,origin,wait,reason)
                VALUES (?,?,?,?,?,?,?)''', (task, k, now, opened_by, message, wait, reason))
            self.db.execute("UPDATE tasks SET status='open',iteration=?,reason=NULL,updated=? WHERE id=?", (k, now, task))
            from .reactions import set_desired
            set_desired(self, message, '👀')
        return self.task_get(task)

    def wait_fire(self, wait, message=None, choice=None, outcome=None, skip_worker=None, reuse_callback=False, proof=None, check=None):
        if not self.get('waits_enabled', False):
            raise ValueError("Waits aren't switched on yet.")
        with self.transaction():
            item = self.wait_get(wait)
            if item is None:
                raise ValueError('Unknown wait.')
            if item['state'] != 'armed':
                return item
            origin = None
            if message is not None:
                origin = self.db.execute('SELECT * FROM messages WHERE id=?', (message,)).fetchone()
                allowed_topics = {item['topic']} | {job['topic'] for job in item['jobs']}
                if not origin or origin['topic'] not in allowed_topics or origin['kind'] not in ('owner', 'callback', 'secret_filled'):
                    raise ValueError('Use an owner message or firing callback from the wait chat.')
            outcome = outcome or ('choice:' + choice if choice is not None else
                                  {'owner': 'answered', 'time': 'due', 'job': 'done', 'signal': 'seen'}.get(item['kind'], 'manual'))
            self.db.execute("UPDATE waits SET state='fired',outcome=?,fired_by=?,updated=?,next_check=NULL WHERE id=?",
                            (outcome, str(message) if message is not None else None, time.time(), wait))
            if proof is not None:
                item['detail']['proof'] = proof
                self.db.execute('UPDATE waits SET detail=? WHERE id=?', (json.dumps(item['detail']), wait))
            self._clear_owner_buttons(item)
            reason = 'dependency was dropped' if item['kind'] == 'job' and outcome == 'dropped' else outcome
            cause = {'answered': 'The question was answered.', 'due': 'The scheduled time arrived.',
                     'account_available': 'An account is available.',
                     'done': 'The dependency is done.', 'dropped': 'The dependency was dropped.',
                     'waiting': 'The dependency is waiting.'}.get(outcome,
                         'Your choice was received.' if choice is not None else 'The wait ended.')
            trigger_text = None
            if item['kind'] == 'pr':
                from .store import PR
                number = PR.fullmatch(item['detail']['url'])[2]
                trigger_text = {
                    'green': 'PR #%s checks passed', 'red': 'PR #%s CI failed',
                    'merged': 'PR #%s merged', 'closed_unmerged': 'PR #%s closed without merging',
                    'checks_stuck': 'PR #%s checks have been pending for 3 hours',
                }.get(outcome, 'PR #%s wait ended') % number
                if outcome == 'red' and check:
                    trigger_text += ' (check %s)' % _short(check, 120)
            elif item['kind'] == 'torii_update':
                trigger_text = ('Torii update containing %s is running' % item['detail']['commit'][:12]
                                if outcome == 'landed' else 'Torii update wait ended')
            elif item['kind'] == 'signal':
                trigger_text = 'Signal %s received' % item['detail']['name']
            callbacks = {}
            texts = {}
            if reuse_callback and origin is not None and origin['kind'] == 'callback':
                callbacks[origin['topic']] = message
                texts[origin['topic']] = [origin['text']]
            for link in item['jobs']:
                if self.get('topic_lifecycle:' + link['topic']):
                    continue
                if link['status'] != 'waiting' and not (not link['blocking'] and link['status'] in ('done', 'dropped')):
                    continue
                automatic = (self.get('auto_resume', False) is True and item['kind'] == 'owner'
                             and link['task'] in item['detail'].get('auto_resume_jobs', []))
                if automatic and choice == 'Drop':
                    from .control_api import job_number
                    self.task_update(link['task'], status='dropped', note='Dropped by the owner from the waiting list.')
                    _callback(self, link['topic'], 'Job %s dropped by the owner.' % job_number(link['number'], link['iteration']))
                    continue
                if proof is not None and self._wait_finish(item, link, outcome, proof, trigger_text):
                    continue
                if link['topic'] not in callbacks:
                    text = '' if trigger_text else cause + ' ' + item['ask']
                    callbacks[link['topic']] = _callback(self, link['topic'], text)
                    texts[link['topic']] = [text] if text else []
                firing = message if origin is not None else callbacks[link['topic']]
                previous_round = self.task_iterations(link['task'])[-1] if automatic else None
                job = self.wait_open_round(link['task'], firing, wait=wait, reason=reason)
                queued_worker = None
                if automatic and choice == 'Resume' and skip_worker != link['task']:
                    if outcome == 'account_available':
                        self.db.execute('''UPDATE task_iterations SET attempts=?,progress_head=?,cause=?
                            WHERE task=? AND k=?''', (previous_round['attempts'], previous_round['progress_head'],
                            previous_round['cause'], job['id'], job['iteration']))
                    previous = self.db.execute('SELECT * FROM workers WHERE task=? ORDER BY id DESC LIMIT 1', (job['id'],)).fetchone()
                    if previous:
                        from .lifecycle import queue_resume
                        queued_worker = queue_resume(self, job, dict(previous), automatic=outcome == 'account_available')
                        if not queued_worker:
                            self.db.execute("UPDATE tasks SET status='interrupted' WHERE id=?", (job['id'],))
                            self.db.execute("""UPDATE task_iterations SET cause='account',resume_at=0,last_worker=?
                                WHERE task=? AND k=?""", (previous['id'], job['id'], job['iteration']))
                from .control_api import Context, Refused, op_workers_spawn
                from .control_api import job_number
                text = ('Job %s opened: %s.' % (job_number(job['number'], job['iteration']), trigger_text)
                        if trigger_text else '%s opened.' % job['title'])
                if trigger_text:
                    text += ' ' + item['ask'] + ('' if item['ask'].endswith(('.', '!', '?')) else '.')
                if queued_worker:
                    text += ' Worker %d queued to continue.' % queued_worker
                elif link['then_provider'] and link['task'] != skip_worker:
                    try:
                        with self.transaction():
                            result = op_workers_spawn(self, Context(), link['task'], link['then_provider'],
                                link['then_prompt'], model=link['then_model'], effort=link['then_effort'], work=link['then_work'] or 'research')
                        text += ' Worker %d queued.' % result.data['worker']
                    except Exception as error:
                        from . import problems
                        problems.record(self, 'waits', 'worker-start-failed', type(error).__name__,
                                        topic=link['topic'], task=link['task'])
                        text += (' Worker could not start: %s. Decide the next step.' % error if isinstance(error, Refused)
                                 else ' Worker could not start. Decide the next step.')
                else:
                    text += ' Decide the next step.'
                texts[link['topic']].append(text)
            if not item['jobs'] and not callbacks and not self.get('topic_lifecycle:' + item['topic']):
                callbacks[item['topic']] = _callback(self, item['topic'], (trigger_text + '. ' if trigger_text else cause + ' ') + item['ask'])
            for topic, lines in texts.items():
                self.db.execute('UPDATE messages SET text=? WHERE id=?', ('\n'.join(lines), callbacks[topic]))
            if message is None and callbacks:
                self.db.execute('UPDATE waits SET fired_by=? WHERE id=?', (str(next(iter(callbacks.values()))), wait))
            self.refresh_owner_waiting()
        return self.wait_get(wait)

    def _wait_finish(self, item, link, outcome, proof, text):
        from .control_api import job_number
        task = self.task_get(link['task'])
        if not link['blocking']:
            return False
        merge = item['kind'] == 'pr' and outcome == 'merged'
        landed = item['kind'] == 'torii_update' and outcome == 'landed'
        if not merge and not landed:
            return False
        label = 'Job ' + job_number(task['number'], task['iteration'])
        if merge:
            url = item['detail']['url']
            if task['pr'] and task['pr'].casefold() != url.casefold():
                return False
            commit = proof['mergeCommit']['oid']
            self.task_update(task['id'], pr=url, stage='merged')
            if task['finish'] == 'live' and item['detail'].get('next'):
                followup = dict(item['detail']['next'])
                followup.setdefault('commit', commit)
                self.task_wait(task['topic'], task=task['id'], ask=item['ask'], **followup)
                callback = _callback(self, task['topic'], label + ': ' + text + '. Waiting for the declared follow-up containing '
                                     + followup['commit'][:12] + '.')
                self.db.execute('UPDATE waits SET fired_by=COALESCE(fired_by,?) WHERE id=?', (str(callback), item['id']))
                return True
            if task['finish'] not in (None, 'merged'):
                return False
            self.task_update(task['id'], status='done')
        else:
            if task['finish'] != 'live' or link['then_provider']:
                return False
            from .worker_storage import results_directory
            directory = Path(results_directory(self, task['id'], create=True))
            path = directory / ('wait-%d-round-%d-live.json' % (item['id'], task['iteration']))
            path.write_text(json.dumps(proof, indent=2) + '\n')
            self.task_update(task['id'], status='done', stage='live')
        callback = _callback(self, task['topic'], label + ' is done: ' + text + '.')
        self.db.execute('UPDATE waits SET fired_by=COALESCE(fired_by,?) WHERE id=?', (str(callback), item['id']))
        return True

    def wait_signal(self, name, *, internal=False):
        if not self.get('waits_enabled', False):
            return []
        if not isinstance(name, str) or not SIGNAL.fullmatch(name) or internal and name not in FIXED_SIGNALS:
            raise ValueError('Unknown signal name.')
        fired = []
        for item in self.waits_list():
            if item['kind'] == 'signal' and item['detail']['name'] == name:
                result = self.wait_hook('signal-trigger-failed', self.wait_fire, item['id'], outcome='seen')
                if result is not None and result['state'] == 'fired':
                    fired.append(item['id'])
        return fired

    def wait_cancel(self, wait, reason, *, resume=True):
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError('Cancelling a wait needs a reason.')
        with self.transaction():
            item = self.wait_get(wait)
            if item is None:
                raise ValueError('Unknown wait.')
            self.db.execute("""UPDATE waits SET state='cancelled',reason=?,updated=?,next_check=NULL
                WHERE id=? AND state='armed'""", (reason.strip(), time.time(), wait))
            if item['state'] == 'armed':
                self._clear_owner_buttons(item)
            if resume:
                self.wait_resume_unblocked(reason=reason.strip())
            self.refresh_owner_waiting()
        return self.wait_get(wait)

    def wait_task_changed(self, task, status):
        if not self.get('waits_enabled', False) or status not in ('done', 'dropped', 'waiting'):
            return
        rows = self.db.execute("SELECT id,detail FROM waits WHERE kind='job' AND state='armed'").fetchall()
        for row in rows:
            detail = json.loads(row['detail'])
            if detail['job'] == task and (status in ('done', 'dropped') or detail['on'] == 'closed'):
                self.wait_fire(row['id'], outcome=status)
        self.refresh_owner_waiting()

    def wait_close_topic(self, topic):
        if not self.get('waits_enabled', False):
            return
        with self.transaction():
            for item in self.waits_list():
                closing = [job for job in item['jobs'] if job['topic'] == topic]
                remaining = [job for job in item['jobs'] if job['topic'] != topic]
                if not closing and item['topic'] != topic:
                    continue
                if not remaining:
                    self.wait_cancel(item['id'], 'chat closed', resume=False)
                    continue
                for job in closing:
                    self.db.execute('DELETE FROM wait_jobs WHERE wait=? AND task=?', (item['id'], job['task']))
                if item['topic'] == topic:
                    destination = remaining[0]['topic']
                    if item['kind'] == 'owner':
                        self._clear_owner_buttons(item)
                        self.db.execute('UPDATE outbox SET retired=1 WHERE id=?', (item['ask_outbox'],))
                        outbox = self.enqueue_report(destination, item['ask'], kind='wait-ask',
                            reply_markup=_keyboard(item['id'], item['detail']['buttons']))
                        self.db.execute('UPDATE waits SET ask_outbox=? WHERE id=?', (outbox, item['id']))
                    self.db.execute('UPDATE waits SET topic=?,updated=? WHERE id=?',
                                    (destination, time.time(), item['id']))
            jobs = self.db.execute("SELECT id FROM tasks WHERE topic=? AND status IN ('open','waiting','interrupted')", (topic,)).fetchall()
            self.db.execute("UPDATE tasks SET status='dropped',reason='chat closed',updated=? WHERE topic=? AND status='waiting'",
                            (time.time(), topic))
            for job in jobs:
                self.wait_task_changed(job['id'], 'dropped')
            self.wait_resume_unblocked(reason='The chat holding the wait closed.')
            self.refresh_owner_waiting()

    def wait_secret_filled(self, topic, message):
        if not self.get('waits_enabled', False):
            return
        for item in self.waits_list(topic, owner=True):
            if not item['key'].startswith('secret:'):
                continue
            names = [name.strip() for name in item['key'][7:].split(',') if name.strip()]
            if names and all(self.db.execute("SELECT 1 FROM envelopes WHERE name=? AND state='filled'", (name,)).fetchone()
                             for name in names):
                self.wait_fire(item['id'], message)

    def refresh_owner_waiting(self):
        if not self.get('waits_enabled', False):
            return
        rows = self.db.execute("SELECT id FROM waits WHERE kind='owner' AND state!='armed'").fetchall()
        for row in rows:
            self._clear_owner_buttons(self.wait_get(row['id']))
        home = self.get('coordinator_home_topic')
        if not home or not self.topic(home) or self.get('topic_lifecycle:' + home):
            return
        data = pending_data(self, owner=True)
        previous = self.get('owner_waiting_card')
        if not previous and not data['waits'] and not data['held']:
            return
        text = pending_text(self, data, limit=4096)
        old = self.db.execute('SELECT * FROM outbox WHERE id=? AND topic=?',
                              (previous, home)).fetchone() if previous else None
        if old and old['text'] == text and not old['retired']:
            return
        edit = (old['telegram_message'] or old['edit_message']) if old else None
        if old and self.get('wait_sending:' + str(old['id']), False):
            self.db.execute('UPDATE outbox SET retired=1 WHERE id=?', (old['id'],))
            if edit is None:
                return
        elif old and not old['delivered'] and not old['retired']:
            self.db.execute('UPDATE outbox SET text=? WHERE id=?', (text, old['id']))
            return
        if old and not old['delivered']:
            self.db.execute('UPDATE outbox SET retired=1 WHERE id=?', (old['id'],))
        outbox = self.enqueue_report(home, text, kind='owner-waiting', edit=edit)
        self.put('owner_waiting_card', outbox)


def pending_data(store, topic=None, owner=False):
    if not store.get('waits_enabled', False):
        return {'waits': [], 'held': []}
    held = [dict(row) for row in store.db.execute("""SELECT id,topic,title,number,iteration,copy_reason
        FROM tasks WHERE copy='held' AND (? IS NULL OR topic=?) ORDER BY topic,number""", (topic, topic))]
    return {'waits': store.waits_list(topic, owner), 'held': held if owner else []}


def pending_text(store, data, limit=None):
    if not store.get('waits_enabled', False):
        return 'Nothing is waiting on you.'
    lines = []
    for item in data['waits']:
        ask = _short(item['ask'], 400)
        if item['jobs']:
            for job in item['jobs']:
                lines.append('%s · %s: %s' % (_short(store.topic(job['topic'])['name'], 80),
                                             _short(job['title'], 80), ask))
        else:
            lines.append('%s: %s' % (_short(store.topic(item['topic'])['name'], 80), ask))
    for job in data['held']:
        lines.append('%s · %s: %s' % (_short(store.topic(job['topic'])['name'], 80),
            _short(job['title'], 80), _short(job['copy_reason'] or 'Needs your decision.', 400)))
    if not lines:
        return 'Nothing is waiting on you.'
    text = 'Waiting on you'
    for index, line in enumerate(lines):
        if limit is not None:
            from .formatting import markdown_to_html
            suffix = '\nMore items: /waiting in the home chat.'
            candidate = text + '\n' + line + (suffix if index < len(lines) - 1 else '')
            if len(candidate) > limit or len(markdown_to_html(candidate)) > limit:
                return text + suffix
        text += '\n' + line
    return text


def owner_header(store, topic):
    if not store.get('waits_enabled', False):
        return ''
    lines = []
    for item in store.waits_list(topic, owner=True):
        values = (', '.join(job['title'] for job in item['jobs']) or 'Chat', item['ask'])
        title, ask = (_short(re.sub(r'\bmessage\s*=', 'message =', value.replace('[', '(').replace(']', ')')), 120)
                      for value in values)
        lines.append('%s: %s' % (title, ask))
    return '; '.join(lines)


def op_tasks_wait(store, ctx, kind, ask, task=None, topic=None, **params):
    from .control_api import Refused, Result
    if not store.get('waits_enabled', False):
        raise Refused("Waits aren't switched on yet.")
    if task is not None:
        job = store.task_get(task)
        if not job:
            raise Refused('Unknown job.')
        if topic is not None and topic != job['topic']:
            raise Refused('Job is in another chat.')
        topic = job['topic']
    if topic is None:
        raise Refused('A chat-level wait needs topic.')
    try:
        item = store.task_wait(topic, kind, ask, task=task, **params)
    except ValueError as error:
        raise Refused(str(error)) from error
    return Result(True, _wait_status(store, item), item)


def op_pending_signal(store, ctx, name):
    from .control_api import Refused, Result
    if ctx.source != 'cli':
        raise Refused('pending.signal is available only from the local CLI.')
    if not store.get('waits_enabled', False):
        raise Refused("Waits aren't switched on yet.")
    try:
        fired = store.wait_signal(name)
    except ValueError as error:
        raise Refused(str(error)) from error
    return Result(True, 'Signal %s received. %d %s fired.' %
                  (name, len(fired), 'wait' if len(fired) == 1 else 'waits'), {'fired': fired})


def op_pending_list(store, ctx, topic=None, owner=False):
    from .control_api import Result
    data = pending_data(store, topic, owner)
    if not store.get('waits_enabled', False):
        return Result(True, 'Nothing is waiting on you.', data)
    return Result(True, pending_text(store, data) if owner else
                  '\n'.join(_wait_status(store, item) + ' ' + item['ask'] for item in data['waits']) or 'No waits.', data)


def op_pending_fire(store, ctx, wait, message, choice=None):
    from .control_api import Refused, Result
    try:
        item = store.wait_fire(wait, message, choice)
    except ValueError as error:
        raise Refused(str(error)) from error
    return Result(True, _wait_status(store, item), item)


def op_pending_cancel(store, ctx, wait, reason):
    from .control_api import Refused, Result
    try:
        item = store.wait_cancel(wait, reason)
    except ValueError as error:
        raise Refused(str(error)) from error
    return Result(True, _wait_status(store, item), item)


def accept_reply(store, topic, reply, message, text=None):
    if not store.get('waits_enabled', False):
        return
    if text is None:
        origin = store.db.execute('SELECT text FROM messages WHERE id=?', (message,)).fetchone()
        if origin is None:
            return
        text = origin['text']
        if text.startswith('Replying to: '):
            text = text.partition('\n')[2]
    if text.lstrip().startswith('/'):
        return
    row = store.db.execute('''SELECT w.id FROM waits w JOIN outbox o ON o.id=w.ask_outbox
        WHERE w.state='armed' AND w.kind='owner' AND w.topic=? AND o.delivered=1 AND o.telegram_message=?''',
        (topic, reply)).fetchone()
    if row:
        store.wait_fire(row['id'], message)


def accept_button(store, topic, message, data):
    if not store.get('waits_enabled', False) or not isinstance(data, str):
        return 'stale_callback'
    match = WAIT_BUTTON.fullmatch(data)
    if match is None:
        return 'stale_callback'
    with store.transaction():
        item = store.wait_get(int(match.group(1)))
        index = int(match.group(2))
        if not item or item['kind'] != 'owner' or item['topic'] != topic:
            return 'stale_callback'
        outbox = store.db.execute('SELECT * FROM outbox WHERE id=?', (item['ask_outbox'],)).fetchone()
        buttons = [button for row in item['detail']['buttons'] for button in row]
        if (not outbox or (outbox['telegram_message'] or outbox['edit_message']) != message
                or not 0 <= index < len(buttons)):
            return 'stale_callback'
        if item['state'] != 'armed':
            return 'wait_already_answered'
        if not outbox['delivered']:
            return 'stale_callback'
        choice = buttons[index]['text']
        origin = _callback(store, topic, 'Owner chose %s.' % choice)
        store.wait_fire(item['id'], origin, choice, reuse_callback=True)
    return 'wait_choice_received'


def _pr_query(repository, numbers):
    owner, name = repository.split('/')
    fields = ('number url state title mergedAt mergeCommit { oid } autoMergeRequest { enabledAt } '
              'commits(last:1) { nodes { commit { oid statusCheckRollup { state '
              'contexts(first:100) { nodes { __typename ... on CheckRun { name status conclusion } '
              '... on StatusContext { context state } } } } } } }')
    pulls = ' '.join('pr%d:pullRequest(number:%d) { %s }' % (number, number, fields) for number in numbers)
    return 'query { repository(owner:%s,name:%s) { %s } }' % (json.dumps(owner), json.dumps(name), pulls)


def _pr_observation(item, result):
    from .store import PR
    expected = PR.fullmatch(item['detail']['url'])
    returned = PR.fullmatch(result.get('url', '')) if isinstance(result, dict) else None
    if (not returned or returned[1].casefold() != expected[1].casefold() or returned[2] != expected[2]
            or type(result.get('number')) is not int or result['number'] != int(expected[2])
            or result.get('state') not in ('OPEN', 'CLOSED', 'MERGED')):
        raise ValueError('Invalid GitHub PR observation.')
    if result['state'] == 'MERGED':
        commit = result.get('mergeCommit')
        if (not isinstance(commit, dict) or not isinstance(commit.get('oid'), str)
                or not COMMIT.fullmatch(commit['oid']) or not isinstance(result.get('title'), str)
                or not result['title'].strip() or not isinstance(result.get('mergedAt'), str)):
            raise ValueError('Incomplete GitHub merge proof.')
        _at(result['mergedAt'])
        return 'merged', None, dict(item['detail'])
    if result['state'] == 'CLOSED':
        return 'closed_unmerged', None, dict(item['detail'])
    nodes = result['commits']['nodes']
    head = nodes[-1]['commit']
    if not isinstance(head.get('oid'), str) or not COMMIT.fullmatch(head['oid']):
        raise ValueError('Invalid GitHub head commit.')
    rollup = head['statusCheckRollup']
    state = rollup['state'] if rollup else None
    if state not in (None, 'SUCCESS', 'FAILURE', 'ERROR', 'PENDING', 'EXPECTED'):
        raise ValueError('Invalid GitHub checks state.')
    contexts = rollup['contexts']['nodes'] if rollup else []
    detail = dict(item['detail'])
    pending = state in ('PENDING', 'EXPECTED') and bool(contexts)
    if detail.get('head_sha') != head['oid'] or not pending:
        detail.pop('pending_since', None)
    detail['head_sha'] = head['oid']
    if pending:
        detail.setdefault('pending_since', time.time())
    failed = next((context.get('name') or context.get('context') for context in contexts
                   if context.get('state') in ('FAILURE', 'ERROR') or
                   context.get('conclusion') in ('FAILURE', 'TIMED_OUT', 'CANCELLED', 'ACTION_REQUIRED',
                                                  'STARTUP_FAILURE', 'STALE')), None)
    if state in ('FAILURE', 'ERROR') or failed:
        detail.pop('pending_since', None)
        return ('red' if item['detail']['until'] == 'checks' else None), failed, detail
    if state == 'SUCCESS':
        return ('green' if item['detail']['until'] == 'checks' and not result.get('autoMergeRequest') else None), None, detail
    return ('checks_stuck' if pending and time.time() - detail['pending_since'] >= 3 * 3600 else None), None, detail


def _pr_checked(store, identifier, proof, outcome, check, detail, now):
    item = store.wait_get(identifier)
    if not item or item['state'] != 'armed' or not store.get('waits_enabled', False):
        return
    interval = 60 if now - item['created'] < 1800 else 300
    detail.pop('not_found', None)
    store.db.execute('UPDATE waits SET checks=checks+1,next_check=?,updated=?,detail=? WHERE id=?',
                     (now + interval, now, json.dumps(detail), identifier))
    if outcome:
        store.wait_fire(identifier, outcome=outcome, proof=proof, check=check)
    return True


def _pr_unavailable(store, identifier, now, not_found=False, moved=None):
    item = store.wait_get(identifier)
    if not item or item['state'] != 'armed' or not store.get('waits_enabled', False):
        return
    detail = item['detail']
    detail['not_found'] = detail.get('not_found', 0) + 1 if not_found else 0
    store.db.execute('UPDATE waits SET checks=checks+1,next_check=?,updated=?,detail=? WHERE id=?',
                     (now + 900, now, json.dumps(detail), identifier))
    if not moved and detail['not_found'] < 3:
        from . import problems
        problems.record(store, 'waits', 'github-pr-unavailable', 'Not found' if not_found else 'Invalid PR observation')
        return True
    from .store import PR
    number = PR.fullmatch(detail['url'])[2]
    reason = 'PR #%s moved to %s. Arm a new wait for this URL' % (number, moved) if moved else 'PR #%s was not found' % number
    store.wait_cancel(identifier, reason, resume=False)
    callbacks = {}
    for topic in dict.fromkeys([item['topic']] + [link['topic'] for link in item['jobs']]):
        if not store.get('topic_lifecycle:' + topic):
            callbacks[topic] = _callback(store, topic, reason + '. Decide the next step.')
    for link in item['jobs']:
        if not link['blocking'] or link['status'] != 'waiting' or link['topic'] not in callbacks:
            continue
        blocking = store.db.execute('''SELECT 1 FROM wait_jobs j JOIN waits w ON w.id=j.wait
            WHERE j.task=? AND j.blocking=1 AND w.state='armed' LIMIT 1''', (link['task'],)).fetchone()
        if not blocking:
            store.wait_open_round(link['task'], callbacks[link['topic']], wait=identifier, reason=reason)
    store.refresh_owner_waiting()
    return True


async def poll_github(store):
    if not store.get('waits_enabled', False):
        return
    from . import github
    from .store import PR
    runner = store.code_requests.run_github if store.code_requests is not None else github.run_github
    now = time.time()
    repositories = {}
    for item in store.waits_list():
        if item['kind'] == 'pr':
            repository = PR.fullmatch(item['detail']['url'])[1]
            repositories.setdefault(repository, []).append(item)
    for repository, items in repositories.items():
        due = [item for item in items if item['next_check'] is not None and item['next_check'] <= now]
        if not due:
            continue
        numbers = sorted({int(PR.fullmatch(item['detail']['url'])[2]) for item in items})
        try:
            response = await asyncio.wait_for(runner(
                ['gh', 'api', 'graphql', '-f', 'query=' + _pr_query(repository, numbers)],
                github.GITHUB_TIMEOUT), github.GITHUB_TIMEOUT)
            if not store.get('waits_enabled', False):
                return
            if (not isinstance(response, dict) or not isinstance(response.get('data'), dict)
                    or 'repository' not in response['data']):
                raise ValueError('GitHub query failed.')
            results = response['data']['repository']
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if not store.get('waits_enabled', False):
                return
            failed = time.time()
            with store.transaction():
                for item in items:
                    store.db.execute("""UPDATE waits SET next_check=?,checks=checks+1,updated=?
                        WHERE id=? AND state='armed'""", (failed + 900, failed, item['id']))
            from . import problems
            problems.record(store, 'waits', 'github-poll-failed', type(error).__name__)
            continue
        for item in due:
            number = PR.fullmatch(item['detail']['url'])[2]
            alias = 'pr' + number
            proof = results.get(alias) if isinstance(results, dict) else None
            errors = [error for error in (response.get('errors') or []) if isinstance(error, dict)
                      and isinstance(error.get('path'), list)
                      and (error['path'] == ['repository'] or error['path'][:2] == ['repository', alias])]
            not_found = proof is None and (any(error.get('type') == 'NOT_FOUND' for error in errors)
                or isinstance(results, dict) and alias in results and not errors)
            returned = PR.fullmatch(proof.get('url', '')) if isinstance(proof, dict) and isinstance(proof.get('url'), str) else None
            moved = proof['url'] if returned and returned[1].casefold() != repository.casefold() and returned[2] == number else None
            try:
                if not_found or moved:
                    raise ValueError('PR is unavailable.')
                outcome, check, detail = _pr_observation(item, proof)
            except (ValueError, KeyError, TypeError, IndexError, AttributeError):
                result = store.wait_hook('pr-trigger-failed', _pr_unavailable, store, item['id'], time.time(), not_found, moved)
            else:
                result = store.wait_hook('pr-trigger-failed', _pr_checked, store, item['id'], proof, outcome, check, detail, now)
            if result is None and store.get('waits_enabled', False):
                with store.transaction():
                    store.db.execute("UPDATE waits SET next_check=? WHERE id=? AND state='armed'",
                                     (time.time() + 900, item['id']))


async def update_landed(updater, armed=False):
    store = updater.store
    if not store.get('waits_enabled', False) or not updater.commit:
        return
    from .updates import git, KEY
    record = store.get(KEY) or {}
    proof = {'running_commit': updater.commit, 'observed': time.time(),
             'update': {key: record[key] for key in ('id', 'phase', 'old', 'target', 'health') if key in record}}
    for item in store.waits_list():
        if item['kind'] != 'torii_update' or armed and item['detail'].get('arm_checked'):
            continue
        detail = dict(item['detail'], arm_checked=True)
        with store.transaction():
            store.db.execute("UPDATE waits SET detail=? WHERE id=? AND state='armed'", (json.dumps(detail), item['id']))
        try:
            await asyncio.to_thread(git, updater.root, 'merge-base', '--is-ancestor', item['detail']['commit'], updater.commit)
        except (RuntimeError, OSError):
            continue
        if not store.get('waits_enabled', False):
            return
        store.wait_hook('update-trigger-failed', store.wait_fire, item['id'], outcome='landed', proof=proof)
    if not armed:
        store.wait_signal('update.landed', internal=True)
    for name in ('feed', 'deliver', 'workers'):
        updater.service.wake(name).set()


async def run(service):
    from .service import pause
    while True:
        event = service.wake('waits')
        event.clear()
        await service.waits_once()
        row = service.store.db.execute("""SELECT MIN(next_check) FROM waits
            WHERE state='armed' AND kind IN ('time','pr') AND ?""", (int(bool(service.store.get('waits_enabled', False))),)).fetchone()
        delay = min(60, max(0, row[0] - time.time())) if row[0] is not None else 60
        await pause(event, delay)
