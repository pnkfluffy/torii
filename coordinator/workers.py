"""Run durable task workers in their task worktrees."""

import asyncio
from dataclasses import asdict
import json
import logging
import os
from pathlib import Path
import time
import uuid

from . import problems
from .accounts import LIMIT_CONTINUATION
from .codex_accounts import CodexBroker
from .envelopes import SecretsPending, declared, load_for_worker, record_use
from .failures import TaskFailure, failure_state
from .host import HostClient
from .native_protocol import RunControl, steer_wire
from .providers import RunResult
from .policy import DELETION_POLICY, usage_policy

WORKER_POLICY = """This work comes from the sole authorized owner through Telegram.
Follow this repository's AGENTS.md and installed workflow instructions. Report changes,
checks actually run, artifacts, blockers, and remaining work. Preserve native history.
The coordinator handles Telegram delivery. Never access the bot token.
When resuming interrupted work, inspect actual state before repeating effects.
Credentials arrive only as environment variables named in the task instructions.
Never print, echo, log, write, commit, or report their values.
If you stop to ask the owner or the coordinator a question, end your final message with that question.
""" + "\n" + DELETION_POLICY


logger = logging.getLogger(__name__)
STEER_PREFIX = 'torii-v2-control-'
STEER_OPS = ('workers.steer', 'workers.goal')


def host_directory(state_dir, worker):
    return Path(state_dir) / 'hosts' / (worker['host'] or 'worker-' + str(worker['id']))


def asks_question(text):
    lines = [line.strip().strip('*_`>').strip() for line in (text or '').splitlines() if line.strip()]
    return bool(lines) and lines[-1].endswith('?')


def steer_outcome(store, message_id, state):
    """Apply a receipt that arrives after the control loop settled the steer request."""
    if not message_id.startswith(STEER_PREFIX):
        return
    request_id = int(message_id[len(STEER_PREFIX):])
    row = store.db.execute('SELECT state FROM service_requests WHERE id=?', (request_id,)).fetchone()
    if row and state == 'received' and row['state'] in ('done', 'uncertain'):
        store.service_request_settle(request_id, 'done', {'receipt': 'received'})
    elif row and state == 'uncertain' and row['state'] == 'done':
        store.service_request_settle(request_id, 'uncertain', {'receipt': 'uncertain'})


def _arrived(store, worker, wires):
    found = set()
    result = json.loads(worker['result']) if worker['result'] else {}
    path = result.get('transcript_path') if isinstance(result, dict) else None
    if path and Path(path).is_file():
        from .session import transcript_receipts
        found |= transcript_receipts(path, wires)
    events = host_directory(store.directory, worker) / 'events.jsonl'
    if events.is_file():
        with events.open(errors='replace') as stream:
            for line in stream:
                if not any(wire in line for wire in wires):
                    continue
                try:
                    event = json.loads(json.loads(line)['data'])
                except (ValueError, KeyError, TypeError):
                    continue
                if not isinstance(event, dict):
                    continue
                if event.get('type') == 'user' and event.get('isReplay') is True:
                    found.add(event.get('uuid'))
                elif event.get('type') == 'command_lifecycle' and event.get('state') == 'started':
                    found.add(event.get('command_uuid'))
    return found & set(wires)


def reconcile_steers(store, worker_id):
    """Settle uncertain steers from the transcript and host events. A finished worker's leftovers get one note."""
    worker = store.db.execute('SELECT * FROM workers WHERE id=?', (worker_id,)).fetchone()
    if not worker:
        return
    rows = [row for row in store.db.execute(
        "SELECT id,op,params,result FROM service_requests WHERE state='uncertain' ORDER BY id")
            if row['op'] in STEER_OPS and json.loads(row['params']).get('worker') == worker_id
            and not (json.loads(row['result'] or '{}') or {}).get('noted')]
    if not rows:
        return
    wires = {steer_wire(STEER_PREFIX + str(row['id'])): row['id'] for row in rows}
    arrived = _arrived(store, worker, wires)
    for wire, request_id in wires.items():
        if wire in arrived:
            store.service_request_settle(request_id, 'done', {'receipt': 'received', 'reconciled': True})
        elif worker['status'] not in ('queued', 'waiting_for_secret', 'running'):
            store.service_request_settle(request_id, 'uncertain', {'receipt': 'uncertain', 'noted': True})
            problems.record(store, 'worker', 'steer-not-arrived', 'request=%s status=%s' % (
                request_id, worker['status']), topic=worker['topic'], task=worker['task'], worker=worker_id)
            store.message_save(worker['topic'], 'callback',
                               'Steer request %d to worker %d may not have reached it: the worker ended '
                               'before the message showed in its transcript.' % (request_id, worker_id))


def process_alive(pid):
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


class WorkerPool:
    def __init__(self, store, runner, accounts, vault=None, codex_accounts=None):
        self.store = store
        self.vault = vault
        self.runner = runner
        self.accounts = accounts
        self.codex_accounts = codex_accounts or CodexBroker(store)
        if hasattr(runner, 'account_broker'):
            runner.account_broker = accounts
        if hasattr(runner, 'codex_broker'):
            runner.codex_broker = self.codex_accounts
        self.controls = {}
        self.tasks = {}
        self.stopping = set()

    async def codex_attached(self, worker, topic, result, options, account_selected):
        """Record a reattached Codex run. With automatic switching on, a rejected thread resumes on the next account."""
        rejected = result.quota_limited and not result.success
        with self.store.db:
            self.codex_accounts.record_rate_limit(worker.get('account_alias'), result.rate_limit_info, rejected,
                                                  topic['id'])
        if (not rejected or worker['id'] in self.stopping or not self.codex_accounts.automatic()
                or not self.codex_accounts.select()):
            return result
        options.pop('account_alias', None)
        task = self.store.task_get(worker['task'])
        options['secrets'], used = load_for_worker(self.store, self.vault, task)
        if options['secrets']:
            options['instructions'] = ('Secrets in the environment: ' + ', '.join(sorted(options['secrets'])) +
                                       '. Use them only through the environment. Never print, echo, log, write, '
                                       'commit, or report their values.')
        with self.store.db:
            record_use(self.store, used, worker['id'])
        options.update(attach=False, last_seq=0, fresh=False, on_account=account_selected)
        return await self.codex_accounts.run(
            self.runner.run, 'codex', LIMIT_CONTINUATION + worker['prompt'], Path(worker['cwd']),
            result.session_id or worker['session'], notice_topic=topic['id'],
            stopped=lambda: worker['id'] in self.stopping, **options)

    def codex_waiting(self, worker, until):
        broker = self.codex_accounts
        active = broker.active()
        label = broker.label(active) if active else 'none'
        state = ('is at its usage limit' if active and broker.signed_in(active) else 'is not signed in or is disabled')
        reset = (' until ' + time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(until))
                 if isinstance(until, (int, float)) else '')
        return ('Worker %d is waiting: the active Codex account %s %s%s. Automatic switching is off. When the owner '
                'picks another Codex account, switch with account.use and the worker resumes on it; it also resumes '
                'when the limit resets.' % (worker['id'], label, state, reset))

    async def stop(self, worker_id):
        host = HostClient.attach(host_directory(self.store.directory, self.get(worker_id)))
        self.stopping.add(worker_id)
        try:
            await host.stop()
        except BaseException:
            self.stopping.discard(worker_id)
            raise

    def get(self, worker_id):
        row = self.store.db.execute('SELECT * FROM workers WHERE id=?', (worker_id,)).fetchone()
        return dict(row) if row else None

    def _orphaned(self, worker):
        """Whether the launch extension no longer serves a running worker."""
        try:
            spec = json.loads((host_directory(self.store.directory, worker) / 'spec.json').read_text())
        except (OSError, ValueError):
            return False
        env = spec.get('env') or {}
        return self.runner.extension.state(env) == 'detached'

    async def run(self, worker, topic):
        worker = self.get(worker['id'])
        if worker['status'] not in ('queued', 'running', 'waiting_for_secret'):
            return worker
        attaching = worker['status'] == 'running'
        broker = self.codex_accounts if worker['provider'] == 'codex' else self.accounts
        if attaching and not worker.get('account_alias'):
            spec = json.loads((host_directory(self.store.directory, worker) / 'spec.json').read_text())
            worker['account_alias'] = broker.alias_for_environment(spec.get('env', {}))
        self.tasks[worker['id']] = asyncio.current_task()
        result = None
        try:
            task = self.store.task_get(worker['task']) if worker['task'] else None
            if not attaching and (not task or not task['worktree'] or not Path(task['worktree']).is_dir()):
                raise TaskFailure('The job worktree is unavailable.', 'Restore its saved folder before resuming.')
            if not attaching and worker['cwd'] != task['worktree']:
                raise TaskFailure('Worker folder differs from its job worktree.', 'Inspect the saved job before resuming.')
            if not attaching and worker['provider'] == 'codex':
                if not self.store.get('codex_enabled', True):
                    raise TaskFailure('Codex delegation is disabled.', 'Select Claude or enable Codex.')
            try:
                secrets, used = ({}, []) if attaching else load_for_worker(self.store, self.vault, task)
            except SecretsPending as pending:
                with self.store.db:
                    waiting = self.store.db.execute(
                        "UPDATE workers SET status='waiting_for_secret',updated=? WHERE id=? AND status='queued'",
                        (time.time(), worker['id'])).rowcount
                if waiting:
                    self.store.message_save(worker['topic'], 'callback', (
                        'Worker %d waits for %s. It starts when the owner fills the secret card.'
                        % (worker['id'], ', '.join(pending.names))))
                    logger.info('worker waiting for secrets worker=%s names=%s', worker['id'], ','.join(pending.names))
                return self.get(worker['id'])
            if not attaching and not worker['session'] and worker['provider'] == 'claude':
                worker['session'] = str(uuid.uuid4())
            control = RunControl(on_outcome=lambda message_id, state: steer_outcome(self.store, message_id, state))
            self.controls[worker['id']] = control
            if not attaching:
                with self.store.db:
                    current = self.get(worker['id'])
                    if current['status'] not in ('queued', 'waiting_for_secret'):
                        return current
                    self.store.db.execute("UPDATE workers SET status='running',session=?,updated=? WHERE id=?",
                                          (worker['session'], time.time(), worker['id']))
                    record_use(self.store, used, worker['id'])

            def started(pid):
                from .reactions import worker_started
                worker['pid'] = pid
                with self.store.db:
                    self.store.db.execute('UPDATE workers SET pid=?,updated=? WHERE id=?',
                                          (pid, time.time(), worker['id']))
                    if not attaching:
                        worker_started(self.store, worker)

            def host_started(name):
                worker.update(host=name, last_seq=0)
                with self.store.db:
                    self.store.db.execute('UPDATE workers SET host=?,last_seq=0,updated=? WHERE id=?',
                                          (name, time.time(), worker['id']))

            def problem(area, code, detail):
                problems.record(self.store, area, code, detail, topic=worker['topic'], task=worker['task'],
                                worker=worker['id'])

            def session_started(session):
                worker.update(session=session, fresh=0)
                with self.store.db:
                    self.store.db.execute('UPDATE workers SET session=?,fresh=0,updated=? WHERE id=?',
                                          (session, time.time(), worker['id']))

            def account_selected(alias):
                worker['account_alias'] = alias
                with self.store.db:
                    self.store.db.execute('UPDATE workers SET account_alias=?,updated=? WHERE id=?',
                                          (alias, time.time(), worker['id']))

            model = worker['model'] or (self.store.get('worker_model', 'opus') if worker['provider'] == 'claude'
                                        else self.store.get('codex_model') or 'gpt-6.1-sol')
            instructions = (WORKER_POLICY + '\nCurrent usage policy:\n' + usage_policy(self.store.directory) +
                            '\nJob: ' + json.dumps({'id': task['id'], 'number': task['number'],
                                                     'topic': topic['name'], 'worktree': task['worktree']})) if not attaching else None
            if secrets:
                instructions += ('\nSecrets in the environment: ' + ', '.join(sorted(secrets)) +
                                 '. Use them only through the environment. Never print, echo, log, write, '
                                 'commit, or report their values.')
            initial_steer = '/goal ' + worker['goal'] if worker['provider'] == 'claude' and worker['goal'] else None
            options = dict(fresh=bool(worker['fresh']), model=model, effort=worker['effort'], instructions=instructions,
                           on_start=started, on_session=session_started, control=control,
                           initial_steer=initial_steer,
                           host_id=worker['host'] or 'worker-' + str(worker['id']),
                           on_host=host_started,
                           on_account=account_selected,
                           attach=attaching, last_seq=worker['last_seq'] if attaching else 0,
                           on_seq=lambda seq: self.store.worker_event_consumed(worker['id'], seq),
                           secrets=secrets or None, on_problem=problem)
            if worker['provider'] == 'codex':
                secret_names = {declared(entry)[0] for entry in task['secrets']}
                hosts = self.store.directory / 'hosts'
                previous = {host_directory(self.store.directory, worker) / 'spec.json',
                            hosts / ('worker-' + str(worker['id'])) / 'spec.json'}
                previous.update(hosts.glob('worker-%d[.-]*/spec.json' % worker['id']))
                for spec in previous:
                    if spec.is_file():
                        secret_names.update(json.loads(spec.read_text()).get('secret_names', []))
                options.update(goal=worker['goal'], secret_names=sorted(secret_names),
                               current_goal=lambda: self.get(worker['id'])['goal'])
            if attaching and worker['provider'] == 'claude' and self._orphaned(worker):
                await HostClient(host_directory(self.store.directory, worker)).stop()
                result = RunResult(worker['session'], failure_code='launch_moved',
                                   error="The worker's launch is no longer current, so it resumes in a new process.")
            elif attaching:
                options.pop('on_account')
                result = await self.runner.run(worker['provider'], worker['prompt'], Path(worker['cwd']),
                                               worker['session'], **options)
                if worker['provider'] == 'claude' and result.quota_limited and not result.success:
                    with self.store.db:
                        if result.transcript_path:
                            paths = self.store.get('session_transcripts', {})
                            paths[worker['session']] = result.transcript_path
                            self.store.put('session_transcripts', paths)
                        if not result.managed:
                            self.accounts.record_rate_limit(worker.get('account_alias'),
                                                            result.rate_limit_info, True)
            else:
                waited = json.loads(worker['result'] or '{}').get('status') == 'waiting_for_quota'
                prompt = LIMIT_CONTINUATION + worker['prompt'] if waited and not worker['fresh'] else worker['prompt']
                result = await broker.run(self.runner.run, worker['provider'], prompt,
                                          Path(worker['cwd']), worker['session'], notice_topic=topic['id'],
                                          stopped=lambda: worker['id'] in self.stopping, **options)
            if attaching and worker['provider'] == 'codex':
                result = await self.codex_attached(worker, topic, result, options, account_selected)
            if worker['id'] in self.stopping:
                result.success = False
                result.failure_code = 'owner_stopped'
                result.error = 'The owner stopped this worker.'
            elif attaching and result.failure_code == 'start_failed' and HostClient(
                    host_directory(self.store.directory, worker)).state == 'dead':
                result.failure_code = 'interrupted'
                result.error = 'The provider host stopped without an exit record.'
        except asyncio.CancelledError:
            host_dir = host_directory(self.store.directory, worker)
            if not host_dir.exists():
                with self.store.db:
                    self.store.db.execute("UPDATE workers SET status='queued',pid=NULL,updated=? WHERE id=? AND status='running'",
                                          (time.time(), worker['id']))
            raise
        except Exception as error:
            logger.exception('worker failed worker=%s type=%s', worker['id'], type(error).__name__)
            failure = error if isinstance(error, TaskFailure) else TaskFailure('Worker could not start.', '')
            _, detail = failure_state(error, failure)
            result = RunResult(worker['session'], error=str(error) if isinstance(error, TaskFailure) else
                               'Worker could not start: ' + detail, failure_code='start_failed', failure_detail=detail)
        finally:
            self.controls.pop(worker['id'], None)
            self.tasks.pop(worker['id'], None)
            self.stopping.discard(worker['id'])
        result.session_id = result.session_id or worker['session']
        waits = worker['provider'] == 'claude' or (worker['provider'] == 'codex' and self.codex_accounts.managed()
                                                   and not self.codex_accounts.automatic())
        if (waits and not result.success and result.failure_code != 'owner_stopped' and
                (result.quota_limited or result.failure_code == 'accounts_unavailable' or
                 worker['provider'] == 'claude' and result.failure_code == 'launch_moved')):
            until = broker.earliest_reset()
            payload = asdict(result)
            payload.update(waiting_until=until, status='waiting_for_quota')
            with self.store.db:
                changed = self.store.db.execute(
                    "UPDATE workers SET status='waiting_for_quota',pid=NULL,session=?,fresh=fresh AND ?,result=?,"
                    "updated=? WHERE id=? AND status='running'",
                    (result.session_id, not result.transcript_path, json.dumps(payload), time.time(),
                     worker['id'])).rowcount
                if changed and not broker.select():
                    reset = (' at ' + time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(until))
                             if isinstance(until, (int, float)) else ' when an account becomes available')
                    self.store.message_save(worker['topic'], 'callback', (
                        'Worker %d is waiting for Claude usage to reset%s.' % (worker['id'], reset)
                        if worker['provider'] == 'claude' else self.codex_waiting(worker, until)))
            logger.info('worker waiting for quota worker=%s until=%s', worker['id'], until)
            return self.get(worker['id'])
        result.needs_input = result.success and asks_question(result.text)
        status = ('interrupted' if result.failure_code in ('interrupted', 'owner_stopped')
                  else 'needs_input' if result.needs_input else 'done')
        self.store.worker_complete(worker['id'], asdict(result), status)
        reconcile_steers(self.store, worker['id'])
        logger.info('worker finish worker=%s task=%s status=%s success=%s',
                    worker['id'], worker['task'], status, result.success)
        return self.get(worker['id'])
