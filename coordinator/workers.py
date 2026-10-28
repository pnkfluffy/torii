"""Run durable task workers in their task worktrees."""

import asyncio
from dataclasses import asdict
import json
import logging
import os
from pathlib import Path
import re
import time
import uuid

from . import extension, problems, opencode_accounts, process_ownership, worker_storage
from .accounts import LIMIT_CONTINUATION
from .codex_accounts import CODEX_SKILLS_POINTER, CodexBroker, limit_windows
from .envelopes import SecretsPending, declared, load_for_worker, record_use
from .failures import TaskFailure, failure_state, internal_detail
from .host import CleanupFailed, HostClient, HostDied, SessionBusy, alive
from .native_protocol import RunControl, steer_wire
from .providers import RunResult
from .usage_notice import clear_unknown_limit_notice, queue_limit_notice
from .opencode_runtime import OpenCodeBroker
from .muse_accounts import MuseBroker
from . import muse_runtime
from .control_api import WORKER_PROVIDERS, Refused
from .policy import DELETION_POLICY, JOB_STORAGE_POLICY, worker_usage_policy

WORKER_POLICY = """This work comes from the sole authorized owner through Telegram.
Follow this repository's AGENTS.md and installed workflow instructions. Report changes,
checks actually run, artifacts, blockers, and remaining work. Preserve native history.
The coordinator handles Telegram delivery. Never access the bot token.
When resuming interrupted work, inspect actual state before repeating effects.
Credentials arrive only as environment variables named in the task instructions.
Never print, echo, log, write, commit, or report their values.
Ending your turn ends your work, so do not stop to post a status update; keep working and wait for what you started until the task is done or truly blocked, then give one final report.
""" + extension.active().worker_policy + "\n" + DELETION_POLICY + "\n" + JOB_STORAGE_POLICY

RESTART_CONTINUATION = ('You were interrupted by a Torii restart and are being resumed. '
                        'Inspect your prior progress before continuing. Check actual state before repeating '
                        'actions with external effects, such as pushes, PR creation or sends.\n\n')


logger = logging.getLogger(__name__)
STEER_PREFIX = 'torii-v2-control-'
STEER_OPS = ('workers.steer', 'workers.goal')


def _opencode_account_available(store):
    alias = store.get('opencode_active_account')
    accounts = store.get('opencode_accounts', {}) or {}
    return bool(alias and isinstance(accounts.get(alias), dict))


SPAWN_REFUSALS = {
    'opencode': ('OpenCode', _opencode_account_available, 'No OpenCode account is active.'),
    'muse': ('Muse', lambda store: MuseBroker(store).available(), 'No Muse account is signed in.'),
}


def spawn_refusal(store, provider, task, goal=None, model=None, *, fresh=True):
    rule = SPAWN_REFUSALS.get(provider)
    if rule is None:
        return None
    label, account_available, account_refusal = rule
    if store.get(provider + '_enabled', False) is not True:
        return label + ' workers are off.'
    if goal is not None:
        return label + " workers don't take goals. Use Claude or Codex."
    if any(declared(entry)[0] for entry in task['secrets']):
        return label + " workers don't take vault secrets. Use a Claude worker."
    if provider == 'opencode' and not fresh:
        return None
    if provider == 'muse':
        if not fresh:
            return 'Muse workers require a fresh session until resume is supported.'
        if model is not None and model not in muse_runtime.MODELS:
            return 'Muse model is unavailable.'
        try:
            MuseBroker(store).run_ready()
        except Refused as error:
            return str(error)
    if not account_available(store):
        return account_refusal
    if provider == 'opencode':
        alias = opencode_accounts.active(store)
        try:
            opencode_accounts.check_key(alias)
        except Refused as error:
            return str(error)
        if model is not None and model != opencode_accounts.accounts(store)[alias].get('model'):
            return 'OpenCode model must match the active account.'
    return None


def host_directory(state_dir, worker):
    return Path(state_dir) / 'hosts' / (worker['host'] or 'worker-' + str(worker['id']))


def can_resume_finished(store, worker):
    if worker['provider'] == 'muse':
        return False
    if worker['provider'] == 'opencode' and store.get('opencode_enabled', False) is not True:
        return False
    auto = store.get('auto_resume', False) is True
    if not (worker['provider'] in WORKER_PROVIDERS and worker['status'] in (('done', 'interrupted') if auto else ('done',))
            and bool(worker['session']) and not worker['fresh'] and store.db.execute('''SELECT 1 FROM tasks
                WHERE id=? AND (status='open' OR (? AND status='interrupted')) AND NOT EXISTS (
                    SELECT 1 FROM workers WHERE task=? AND id>?)''',
                (worker['task'], auto, worker['task'], worker['id'])).fetchone() is not None):
        return False
    if store.db.execute('''SELECT 1 FROM session_deletions WHERE provider=? AND session=? LIMIT 1''',
                        (worker['provider'], worker['session'])).fetchone():
        return False
    directory = host_directory(store.directory, worker)
    if (directory / 'exit.json').exists():
        return True
    try:
        child = (directory / 'child.pid').read_text().strip()
    except FileNotFoundError:
        return True
    return not alive(child)


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
    if worker['provider'] == 'opencode':
        writes = host_directory(store.directory, worker) / 'writes.jsonl'
        if writes.is_file():
            with writes.open(errors='replace') as stream:
                for line in stream:
                    try:
                        event = json.loads(json.loads(line)['line'])
                    except (ValueError, KeyError, TypeError):
                        continue
                    if isinstance(event, dict) and event.get('method') == 'session/prompt':
                        found.update(steer_wire(mid) for mid in
                                     (event.get('params', {}).get('_meta') or {}).get('toriiSteers', []))
    return found & set(wires)


def reconcile_steers(store, worker_id):
    """Settle uncertain steers from the transcript and host events. A finished worker's leftovers get one note."""
    worker = store.db.execute('SELECT * FROM workers WHERE id=?', (worker_id,)).fetchone()
    if not worker or worker['provider'] not in WORKER_PROVIDERS:
        return
    rows = [row for row in store.db.execute(
        "SELECT id,op,params,result,state FROM service_requests WHERE (state='uncertain' OR (state='done' AND ?)) "
        "AND op IN (%s) AND json_extract(params,'$.worker')=? ORDER BY id"
        % ','.join('?' for _ in STEER_OPS), (worker['provider'] == 'opencode', *STEER_OPS, worker_id))
            if not (json.loads(row['result'] or '{}') or {}).get('noted')
            and (row['state'] == 'uncertain'
                 or (json.loads(row['result'] or '{}') or {}).get('receipt') == 'queued')]
    if not rows:
        return
    wires = {steer_wire(STEER_PREFIX + str(row['id'])): row['id'] for row in rows}
    arrived = _arrived(store, worker, wires)
    for wire, request_id in wires.items():
        if wire in arrived:
            store.service_request_settle(request_id, 'done', {'receipt': 'received', 'reconciled': True})
        elif worker['status'] not in ('queued', 'waiting_for_secret', 'running'):
            store.service_request_settle(request_id, 'uncertain', {'receipt': 'uncertain', 'noted': True})
            code = 'steer-uncertain' if worker['provider'] == 'opencode' else 'steer-not-arrived'
            problems.record(store, 'worker', code, 'request=%s status=%s' % (
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
        self.brokers = {'claude': self.accounts, 'codex': self.codex_accounts,
                        'opencode': OpenCodeBroker(store), 'muse': MuseBroker(store)}
        if hasattr(runner, 'opencode_store'):
            runner.opencode_store = store
            if store.get('opencode_binary'):
                runner.binaries['opencode'] = opencode_accounts.binary(store)
        if hasattr(runner, 'account_broker'):
            runner.account_broker = accounts
        if hasattr(runner, 'codex_broker'):
            runner.codex_broker = self.codex_accounts
        self.controls = {}
        self.tasks = {}
        self.launching = set()
        self.stopping = set()
        self.cancels = {}
        self.storage = worker_storage.storage_for(store)

    async def cleanup_finished(self):
        await self.storage.reconcile()
        self.storage.schedule()

    def broker(self, provider):
        if provider not in self.brokers:
            raise ValueError('Unknown provider')
        return self.brokers[provider]

    def requeue_lost_host(self, worker, *, stopped=False):
        if (worker['provider'] not in ('claude', 'codex') or not worker['session'] or worker['fresh']
                or worker['id'] in self.stopping):
            return False
        host = HostClient(host_directory(self.store.directory, worker))
        if (host.state not in (('dead', 'exited') if stopped else ('dead',))
                or host.child_pid and alive(host.child_pid)):
            return False
        with self.store.db:
            return bool(self.store.db.execute("""UPDATE workers SET status='queued',pid=NULL,last_seq=0,
                result=json_set(COALESCE(result,'{}'),'$.restart_resume',1),updated=?
                WHERE id=? AND status='running' AND host IS ? AND EXISTS (
                    SELECT 1 FROM tasks WHERE tasks.id=workers.task AND tasks.status='open')""",
                (time.time(), worker['id'], worker['host'])).rowcount)

    async def codex_attached(self, worker, topic, result, options, account_selected):
        """Record a reattached Codex run. With automatic switching on, a rejected thread resumes on the next account."""
        rejected = result.quota_limited and not result.success
        with self.store.db:
            self.codex_accounts.record_rate_limit(worker.get('account_alias'), result.rate_limit_info, rejected)
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
        options['instructions'] = (options['instructions'] + '\n' if options.get('instructions') else '') + CODEX_SKILLS_POINTER
        with self.store.db:
            record_use(self.store, used, worker['id'])
        options.update(attach=False, last_seq=0, fresh=False, on_account=account_selected)
        return await self.codex_accounts.run(
            self.runner.run, 'codex', LIMIT_CONTINUATION + worker['prompt'], Path(worker['cwd']),
            result.session_id or worker['session'], notice_topic=topic['id'], **options)

    def codex_waiting(self, worker, until):
        broker = self.codex_accounts
        reset = (' until ' + time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(until))
                 if isinstance(until, (int, float)) else '')
        if broker.automatic():
            return ('Worker %d is waiting: every enabled Codex account is unavailable%s. Automatic switching is on. '
                    'The worker resumes the same thread when an account becomes available.' % (worker['id'], reset))
        active = broker.active()
        label = broker.label(active) if active else 'none'
        state = ('is at its usage limit' if active and broker.signed_in(active) else 'is not signed in or is disabled')
        return ('Worker %d is waiting: the active Codex account %s %s%s. Automatic switching is off. When the owner '
                'picks another Codex account, switch with account.use and the worker resumes on it; it also resumes '
                'when the limit resets.' % (worker['id'], label, state, reset))

    def opencode_waiting(self, worker, result, reset):
        from . import opencode_accounts, opencode_subscription
        alias = worker.get('account_alias')
        if not opencode_subscription.is_subscription(opencode_accounts.accounts(self.store).get(alias, {})):
            reason = 'out of credit' if result.failure_code == 'out_of_credit' else 'limited'
            return ('Worker %d is paused on OpenCode account %s because it is %s. '
                    'Torii checks again%s, and resumes it automatically.' % (worker['id'], alias, reason, reset))
        if result.failure_code == 'accounts_unavailable':
            return ('Worker %d is paused on OpenCode subscription because usage is unavailable. '
                    'Torii will check again%s.' % (worker['id'], reset))
        return ('Worker %d is paused on OpenCode subscription%s. It resumes on the same account after a usage check.'
                % (worker['id'], reset))

    async def stop(self, worker_id):
        worker = self.get(worker_id)
        directory = host_directory(self.store.directory, worker)
        if worker_id in self.launching and (directory / 'spec.json').is_file():
            self.stopping.add(worker_id)
            return True
        def problem(area, code, detail):
            problems.record(self.store, area, code, detail, topic=worker['topic'],
                            task=worker['task'], worker=worker_id)
        try:
            host = HostClient.attach(host_directory(self.store.directory, worker), on_problem=problem)
        except HostDied:
            task = self.tasks.get(worker_id)
            control = self.controls.get(worker_id)
            if (worker['provider'] == 'codex' and control is not None and control.stop_before_launch
                    and task is not None and not task.done()):
                self.stopping.add(worker_id)
                return True
            raise
        except SessionBusy:
            return
        self.stopping.add(worker_id)
        try:
            cancel = self.cancels.get(worker_id)
            if cancel is not None:
                grace = cancel()
                if grace is not None:
                    await asyncio.wait((grace,))
                    if not grace.cancelled() and grace.exception() is not None:
                        worker = self.get(worker_id)
                        problems.record(self.store, 'worker', 'cancel-failed', internal_detail(grace.exception()),
                                        topic=worker['topic'], task=worker['task'], worker=worker_id)
            await host.stop()
        except CleanupFailed as error:
            self.stopping.discard(worker_id)
            self.cleanup_failed(worker, error)
        except BaseException:
            self.stopping.discard(worker_id)
            raise
        finally:
            host.detach()

    def cleanup_failed(self, worker, error):
        current = self.get(worker['id'])
        if current['status'] != 'interrupted' or json.loads(current['result'] or '{}').get('failure_code') != 'cleanup_failed':
            result = RunResult(worker['session'], success=False, failure_code='cleanup_failed',
                               error='Worker process cleanup failed.', failure_detail=str(error))
            self.store.worker_complete(worker['id'], asdict(result), 'interrupted')
            reconcile_steers(self.store, worker['id'])
            self.storage.schedule(worker['id'])
        return self.get(worker['id'])

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
        if worker['provider'] not in self.brokers:
            return worker
        if worker['status'] not in ('queued', 'running', 'waiting_for_secret'):
            return worker
        attaching = worker['status'] == 'running'
        previous_result = json.loads(worker['result'] or '{}')
        self.tasks[worker['id']] = asyncio.current_task()
        result = None
        cleanup_error = None
        try:
            task = self.store.task_get(worker['task']) if worker['task'] else None
            if not attaching and (not task or task['status'] != 'open'):
                with self.store.db:
                    self.store.db.execute("""UPDATE workers SET status='interrupted',updated=?
                        WHERE id=? AND status IN ('queued','waiting_for_secret')""", (time.time(), worker['id']))
                return self.get(worker['id'])
            if not attaching and (not task or not task['worktree'] or not Path(task['worktree']).is_dir()):
                from .workspaces import prepare_task_workspace
                try:
                    if not task or not task['worktree'] or not topic.get('cwd'):
                        raise ValueError('The saved job folder is unavailable')
                    workspace_store = {'store': self.store} if self.store.get('close_clears_space', False) is True else {}
                    await prepare_task_workspace(Path(topic['cwd']), self.store.directory, task['id'],
                                                 saved_cwd=task['worktree'], **workspace_store)
                except (OSError, RuntimeError, ValueError, asyncio.TimeoutError) as error:
                    raise TaskFailure('The job worktree is unavailable. Use worktree.create to restore it: ' + str(error),
                                      'Use worktree.create before resuming.') from error
            if not attaching and worker['cwd'] != task['worktree']:
                raise TaskFailure('Worker folder differs from its job worktree.', 'Inspect the saved job before resuming.')
            if not attaching:
                saved_subscription = (worker['provider'] == 'opencode' and
                            opencode_accounts.accounts(self.store).get(worker.get('account_alias'), {}).get('provider') == 'opencode-go')
                refusal = spawn_refusal(self.store, worker['provider'], task, worker['goal'], worker['model'],
                                        fresh=bool(worker['fresh']) and not saved_subscription)
                if refusal:
                    raise TaskFailure(refusal, 'Use a Claude or Codex worker.')
            broker = self.broker(worker['provider'])
            if attaching and not worker.get('account_alias'):
                spec = json.loads((host_directory(self.store.directory, worker) / 'spec.json').read_text())
                worker['account_alias'] = (spec.get('account') if worker['provider'] in ('opencode', 'muse')
                                           else broker.alias_for_environment(spec.get('env', {})))
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
                    running = self.store.db.execute("""UPDATE workers SET status='running',session=?,updated=?
                        WHERE id=? AND status IN ('queued','waiting_for_secret') AND EXISTS (
                            SELECT 1 FROM tasks WHERE tasks.id=workers.task AND tasks.status='open')""",
                        (worker['session'], time.time(), worker['id'])).rowcount
                    if not running:
                        if (self.store.task_get(worker['task']) or {}).get('status') != 'open':
                            self.store.db.execute("""UPDATE workers SET status='interrupted',updated=?
                                WHERE id=? AND status IN ('queued','waiting_for_secret')""",
                                (time.time(), worker['id']))
                        return self.get(worker['id'])
                    record_use(self.store, used, worker['id'])

            def started(pid):
                from .reactions import worker_started
                self.launching.discard(worker['id'])
                worker['pid'] = pid
                with self.store.db:
                    self.store.db.execute('UPDATE workers SET pid=?,updated=? WHERE id=?',
                                          (pid, time.time(), worker['id']))
                    if not attaching:
                        worker_started(self.store, worker)

            def host_started(name):
                self.launching.add(worker['id'])
                worker.update(host=name, last_seq=0)
                self.storage.register_host(worker['id'], name)
                with self.store.db:
                    self.store.db.execute('UPDATE workers SET host=?,last_seq=0,updated=? WHERE id=?',
                                          (name, time.time(), worker['id']))
                    self.store.host_log_register(name, worker['id'])

            def problem(area, code, detail):
                problems.record(self.store, area, code, detail, topic=worker['topic'], task=worker['task'],
                                worker=worker['id'])

            def session_started(session):
                worker.update(session=session, fresh=0)
                with self.store.db:
                    self.store.db.execute('UPDATE workers SET session=?,fresh=0,updated=? WHERE id=?',
                                          (session, time.time(), worker['id']))

            def cancel_ready(cancel):
                if cancel is None:
                    self.cancels.pop(worker['id'], None)
                else:
                    self.cancels[worker['id']] = cancel

            def account_selected(alias):
                worker['account_alias'] = alias
                with self.store.db:
                    self.store.db.execute('UPDATE workers SET account_alias=?,updated=? WHERE id=?',
                                          (alias, time.time(), worker['id']))

            opencode_alias = worker.get('account_alias')
            if (worker['fresh'] and opencode_accounts.accounts(self.store).get(
                    opencode_alias, {}).get('provider') != 'opencode-go'):
                opencode_alias = opencode_accounts.active(self.store)
            models = {'claude': self.store.get('worker_model', 'opus'),
                      'codex': self.store.get('codex_model') or 'gpt-6.1-sol',
                      'muse': muse_runtime.DEFAULT_MODEL,
                      'opencode': opencode_accounts.accounts(self.store).get(opencode_alias, {}).get('model')}
            model = worker['model'] or models[worker['provider']]
            if self.store.get('auto_resume', False) is True and worker['model'] is None:
                with self.store.db:
                    self.store.db.execute('UPDATE workers SET model=? WHERE id=?', (model, worker['id']))
            instructions = None
            if not attaching:
                job = {'id': task['id'], 'number': task['number'], 'topic': topic['name'],
                       'worktree': task['worktree']}
                from .close_saves import enabled, scratch
                if enabled(self.store):
                    job['scratch'] = str(scratch(self.store, task['id'], create=True))
                results = worker_storage.results_directory(self.store, task['id'], create=True)
                if worker['provider'] != 'opencode':
                    job['results'] = results
                instructions = (WORKER_POLICY + '\nCurrent usage policy:\n' + worker_usage_policy(self.store.directory) +
                                '\nJob: ' + json.dumps(job))
            if not attaching:
                instructions += ("\n$TMPDIR is this worker's own temp folder; Torii deletes it when the worker finishes. "
                                 "Files there may be deleted outright.")
                if worker['provider'] != 'opencode':
                    instructions += "\nKeep reports, logs and proofs in the job's results folder."
            if not attaching and self.store.get('close_clears_space', False) is True:
                saved_round = self.store.db.execute('SELECT rebuild_seconds FROM task_iterations WHERE task=? AND k=?',
                                                    (task['id'], task['iteration'])).fetchone()
                if saved_round and saved_round['rebuild_seconds'] is not None:
                    instructions += '\nThis code copy was rebuilt from the branch; installed packages are absent. Run the repo install step before checks.'
            if secrets:
                instructions += ('\nSecrets in the environment: ' + ', '.join(sorted(secrets)) +
                                 '. Use them only through the environment. Never print, echo, log, write, '
                                 'commit, or report their values.')
            if not attaching and worker['provider'] == 'codex':
                instructions += '\n' + CODEX_SKILLS_POINTER
            initial_steer = '/goal ' + worker['goal'] if worker['provider'] == 'claude' and worker['goal'] else None
            host_id = worker['host'] or 'worker-' + str(worker['id'])
            if not attaching:
                host_id = re.sub(r'(?:\.[0-9]+)+\Z', '', host_id)
            options = dict(fresh=bool(worker['fresh']), model=model, effort=worker['effort'], instructions=instructions,
                           worker_id=worker['id'], on_start=started, on_session=session_started, control=control,
                           initial_steer=initial_steer,
                           host_id=host_id,
                           on_host=host_started,
                           on_account=account_selected,
                           attach=attaching, last_seq=worker['last_seq'] if attaching else 0,
                           on_seq=lambda seq: self.store.worker_event_consumed(worker['id'], seq),
                           secrets=secrets or None, on_problem=problem)
            if worker['provider'] == 'codex':
                secret_names = {declared(entry)[0] for entry in task['secrets']}
                secret_names.update(self.store.get('host_log_secret_names:' + str(worker['id']), []))
                hosts = self.store.directory / 'hosts'
                previous = {host_directory(self.store.directory, worker) / 'spec.json',
                            hosts / ('worker-' + str(worker['id'])) / 'spec.json'}
                previous.update(hosts.glob('worker-%d[.-]*/spec.json' % worker['id']))
                for spec in previous:
                    if spec.is_file():
                        secret_names.update(json.loads(spec.read_text()).get('secret_names', []))
                options.update(goal=worker['goal'], secret_names=sorted(secret_names),
                               stopped=lambda: worker['id'] in self.stopping,
                               current_goal=lambda: self.get(worker['id'])['goal'],
                               reload_secrets=lambda: load_for_worker(self.store, self.vault, task)[0])
            if worker['provider'] in ('opencode', 'muse'):
                options.update(account_alias=worker.get('account_alias'),
                               stopped=lambda: worker['id'] in self.stopping,
                               on_cancel=cancel_ready)
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
                waited = previous_result.get('status') == 'waiting_for_quota'
                continuation = (RESTART_CONTINUATION if previous_result.get('restart_resume')
                                else LIMIT_CONTINUATION if waited else '')
                prompt = continuation + worker['prompt'] if not worker['fresh'] else worker['prompt']
                if worker['provider'] == 'opencode' and waited and not worker['fresh']:
                    options['instructions'] = None
                    remaining = worker['prompt'][previous_result.get('prompt_chars', 0):]
                    prompt = ('Continue the current task from where it stopped.' +
                              ('\n\n' + remaining if remaining else ''))
                if worker['provider'] in ('claude', 'codex'):
                    options['stopped'] = lambda: worker['id'] in self.stopping
                result = await broker.run(self.runner.run, worker['provider'], prompt,
                                          Path(worker['cwd']), worker['session'], notice_topic=topic['id'], **options)
            if attaching and worker['provider'] == 'codex':
                result = await self.codex_attached(worker, topic, result, options, account_selected)
            if attaching and worker['provider'] == 'opencode':
                broker.record_result(worker.get('account_alias'), result, worker['id'])
            if attaching and result.failure_code == 'start_failed' and HostClient(
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
            if self.store.get('auto_resume', False) is True and isinstance(error, TaskFailure):
                result.failure_code = 'spawn_refused'
        finally:
            try:
                directory = host_directory(self.store.directory, worker)
                if result is not None and (directory / 'spec.json').is_file():
                    def cleanup_problem(area, code, detail):
                        problems.record(self.store, area, code, detail, topic=worker['topic'],
                                        every=60 if code == 'stop-failed' else None,
                                        task=worker['task'], worker=worker['id'])
                    host = HostClient(directory, on_problem=cleanup_problem)
                    try:
                        record = process_ownership.read_owner(directory)
                        if host.state == 'running' or (record is not None and record['cleanup'] in ('pending', 'failed')):
                            await host.stop()
                        else:
                            try:
                                await host.wait()
                            except HostDied:
                                pass
                    except Exception as error:
                        cleanup_error = error
                        if not isinstance(error, CleanupFailed):
                            cleanup_problem('worker', 'stop-failed', internal_detail(error))
                    finally:
                        host.detach()
                if result is not None and worker['id'] in self.stopping:
                    result.success = False
                    result.failure_code = 'owner_stopped'
                    result.error = 'The owner stopped this worker.'
            finally:
                self.cancels.pop(worker['id'], None)
                self.controls.pop(worker['id'], None)
                self.tasks.pop(worker['id'], None)
                self.launching.discard(worker['id'])
                self.stopping.discard(worker['id'])
        if isinstance(cleanup_error, CleanupFailed):
            return self.cleanup_failed(worker, cleanup_error)
        current = self.get(worker['id'])
        if cleanup_error is not None or (current['status'] == 'interrupted' and
                                        json.loads(current['result'] or '{}').get('failure_code') == 'cleanup_failed'):
            return current
        if (attaching and result.failure_code in ('start_failed', 'host_died', 'interrupted')
                and HostClient(host_directory(self.store.directory, worker)).state == 'dead'
                and self.requeue_lost_host(worker)):
            return self.get(worker['id'])
        result.session_id = result.session_id or worker['session']
        waits = worker['provider'] in ('claude', 'opencode') or (worker['provider'] == 'codex'
                                                               and self.codex_accounts.managed())
        if (waits and not result.success and result.failure_code != 'owner_stopped' and
                (result.quota_limited or result.failure_code == 'accounts_unavailable' or
                 worker['provider'] == 'claude' and result.failure_code == 'launch_moved')):
            until = (broker.earliest_reset(worker.get('account_alias'), worker['id']) if worker['provider'] == 'opencode'
                     else broker.earliest_reset())
            payload = asdict(result)
            payload.update(waiting_until=until, status='waiting_for_quota')
            if previous_result.get('restart_resume'):
                payload['restart_resume'] = True
            if self.store.get('auto_resume', False) is True and not getattr(self.store, 'network_available', True):
                payload['network_down'] = True
            if worker['provider'] == 'opencode':
                payload['prompt_chars'] = (len(worker['prompt']) if result.prompt_sent else
                                           previous_result.get('prompt_chars', 0))
                block = broker.block(worker.get('account_alias'), worker['id'])
                payload['retry_seconds'] = (block.get('retry_seconds', 0) if block.get('worker_id') == worker['id']
                                             else previous_result.get('retry_seconds', 0))
            with self.store.db:
                changed = self.store.db.execute(
                    "UPDATE workers SET status='waiting_for_quota',pid=NULL,session=?,fresh=fresh AND ?,result=?,"
                    "updated=? WHERE id=? AND status='running'",
                    (result.session_id, not (result.session_id if worker['provider'] == 'opencode' else result.transcript_path),
                     json.dumps(payload), time.time(),
                     worker['id'])).rowcount
                selected = (broker.select(worker.get('account_alias'), worker['id'])
                            if worker['provider'] == 'opencode' else broker.select())
                if changed and not selected and not (worker['provider'] == 'opencode'
                                                     and previous_result.get('status') == 'waiting_for_quota'):
                    reset = (' at ' + time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(until))
                             if isinstance(until, (int, float)) else ' when an account becomes available')
                    text = (
                        'Worker %d is waiting for Claude usage to reset%s.' % (worker['id'], reset)
                        if worker['provider'] == 'claude' else (
                            self.opencode_waiting(worker, result, reset)
                            if worker['provider'] == 'opencode' else self.codex_waiting(worker, until)))
                    alias = worker.get('account_alias')
                    if worker['provider'] == 'codex' and alias is None:
                        alias = self.codex_accounts.active()
                    notice_reset = (None if worker['provider'] == 'opencode' and
                                    not block.get('reset_known') else until)
                    windows = (limit_windows(self.codex_accounts.snapshot(alias).get('usage') or {}, out=True)
                               if worker['provider'] == 'codex' and alias else {'limit': notice_reset})
                    queue_limit_notice(self.store, worker['provider'].title(), alias, windows, text,
                                       reset_tolerance=30 * 60)
            logger.info('worker waiting for quota worker=%s until=%s', worker['id'], until)
            return self.get(worker['id'])
        if result.success:
            clear_unknown_limit_notice(self.store, worker['provider'].title(), worker.get('account_alias'))
        if (result.success and isinstance(result.goal_status, dict)
                and isinstance(result.goal_status.get('status'), str) and result.goal_status['status']
                and result.goal_status['status'] not in ('complete', 'met')):
            result.success = False
            result.failure_code = 'goal_unmet'
            result.failure_detail = result.goal_status['status']
        status = ('interrupted' if result.failure_code in ('interrupted', 'owner_stopped')
                  else 'done')
        self.store.worker_complete(worker['id'], asdict(result), status)
        reconcile_steers(self.store, worker['id'])
        self.storage.schedule(worker['id'])
        logger.info('worker finish worker=%s task=%s status=%s success=%s',
                    worker['id'], worker['task'], status, result.success)
        return self.get(worker['id'])
