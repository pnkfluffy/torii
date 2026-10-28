"""Native CLI execution. History remains owned by the installed provider CLI."""
import asyncio
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
from sys import platform
from typing import Any, Optional
import uuid

from . import native_ids, problems, process_ownership
from .claude_settings import claude_md_excludes
from .failures import internal_detail
from .host import CleanupFailed, HostClient, HostDied, SessionBusy
from .shared_mcp import write_config as write_shared_mcp_config
from .shared_mcp import read_entries as read_shared_mcp_entries
from .shared_mcp import extend_scrubber as scrub_shared_mcp_entries
from .native_protocol import NativeProtocol, RunControl, SecretDeliveryRefused
from .scrub import Scrubber
from . import extension
from . import host_os
from . import opencode_runtime
from .acp_protocol import AcpProtocol
from . import muse_accounts, muse_runtime
from .msp_protocol import MspProtocol


ADAPTERS = opencode_runtime.ADAPTERS.copy()
WORKER_EXIT_GRACE = 5


def codex_reasoning_effort(effort):
    return 'xhigh' if effort == 'max' else effort


@dataclass
class RunResult:
    session_id: Optional[str]
    text: str = ''
    success: bool = False
    error: Optional[str] = None
    structured: Any = None
    log_path: Optional[str] = None
    pid: Optional[int] = None
    transcript_path: Optional[str] = None
    quota_limited: bool = False
    rate_limit_info: Any = None
    failure_code: Optional[str] = None
    failure_detail: Optional[str] = None
    goal_status: Any = None
    managed: bool = False
    spend: Any = None
    prompt_sent: bool = False


def _scrub_result(result, scrub):
    """Every captured field leaves `run` scrubbed. Torii's own ids and paths stay as they are."""
    for name in ('text', 'error', 'failure_detail'):
        setattr(result, name, scrub(getattr(result, name)))
    for name in ('structured', 'rate_limit_info', 'goal_status'):
        setattr(result, name, scrub.data(getattr(result, name)))


def child_environment():
    """The service environment without Telegram, vault, and parent-session variables."""
    return {key: value for key, value in os.environ.items()
            if not ('TELEGRAM' in key.upper() or key.upper().startswith('CMUX_') or
                    key.upper() in ('BOT_TOKEN', 'CLAUDECODE', 'CLAUDE_CODE_SESSION_ID', 'CODEX_THREAD_ID',
                                    'TORII_VAULT_KEY', 'TORII_VAULT_KEY_FILE'))}


CODEX_SECRET_OVERRIDES = (
    'features.shell_snapshot=false', 'features.shell_snapshot_v2=false',
    'features.multi_agent=false', 'features.multi_agent_v2=false',
    'features.memories=false',
    'memories.dedicated_tools=false',
    'memories.generate_memories=false', 'otel.tool_result.max_bytes=0',
    'otel.log_agent_responses=false', 'otel.log_guardian_assessments=false',
    'shell_environment_policy.inherit="all"',
    'shell_environment_policy.ignore_default_excludes=true',
    'shell_environment_policy.exclude=[]', 'shell_environment_policy.include_only=[]',
    'shell_environment_policy.experimental_use_profile=false')


class ProviderRunner:
    def __init__(self, state_dir, binaries=None, codex_model=None, account_broker=None, opencode_store=None):
        self.state_dir = Path(state_dir).resolve()
        self.binaries = {'claude': 'claude', 'codex': 'codex', **(binaries or {})}
        self.codex_model = codex_model
        self.codex_broker = None
        self.account_broker = account_broker
        self.opencode_store = opencode_store
        if opencode_store and opencode_store.get('opencode_binary'):
            from . import opencode_accounts
            self.binaries['opencode'] = opencode_accounts.binary(opencode_store)
        self.extension = extension.active()
        for directory in (self.state_dir, self.state_dir / 'logs', self.state_dir / 'locks'):
            directory.mkdir(parents=True, exist_ok=True, mode=0o700)
            directory.chmod(0o700)

    async def _external_writer(self, provider, session_id):
        flags = ('-A', '-o') if host_os.linux() else ('-axo',)
        process = await asyncio.create_subprocess_exec('ps', *flags, 'pid=,command=', stdout=asyncio.subprocess.PIPE)
        output, _ = await process.communicate()
        if process.returncode:
            raise RuntimeError('Cannot inspect native session ownership')
        pattern = re.compile(r'(?<![\w-])' + re.escape(session_id) + r'(?![\w-])')
        for line in output.decode(errors='replace').splitlines():
            if pattern.search(line) and re.search(r'(^|[/\s])' + provider + r'(\s|$)', line):
                return True
        return False

    def _environment(self, provider, account_alias=None):
        if provider == 'codex':
            if self.codex_broker and account_alias:
                return self.codex_broker.environment(account_alias)
            return child_environment()
        elif provider == 'claude':
            if not self.account_broker or not account_alias:
                raise ValueError('Claude launch requires an account broker assignment')
            return self.extension.environment(self.account_broker.environment(account_alias), account_alias)
        else:
            raise ValueError('Unknown provider')

    async def launch_secrets(self, env, secrets=None):
        """Given secrets plus private environment values from the launch extension."""
        values = dict(secrets or {})
        values.update(await self.extension.private(env))
        return values

    def _host_owns(self, provider, session_id):
        for directory in (self.state_dir / 'hosts').glob('*'):
            spec = directory / 'spec.json'
            if not spec.exists() or HostClient(directory).state != 'running':
                continue
            if provider + '-' + session_id in json.loads(spec.read_text()).get('locks', []):
                return True
        return False

    async def run(self, provider, prompt, cwd, session_id, **options):
        control = options.get('control')
        try:
            if control is not None:
                control.stop_before_launch = options.get('goal_only', False) or provider == 'codex'
            result = await self._run(provider, prompt, cwd, session_id, **options)
            if result.failure_code != 'goal_feature_required':
                return result
            if options.get('stopped') and options['stopped']():
                result.success = False
                result.failure_code = 'owner_stopped'
                result.error = 'The owner stopped this worker.'
                control.close()
                return result
            objective = result.goal_status['objective']
            previous = json.loads(Path(result.log_path).with_name('spec.json').read_text())
            options['secret_names'] = previous.get('secret_names', [])
            if options.get('attach') and options.get('reload_secrets'):
                options['secrets'] = options['reload_secrets']()
            options.update(fresh=False, attach=False, last_seq=0, goal=None if objective == 'clear' else objective,
                           goal_only=True)
            continued = await self._run(provider, prompt, cwd, result.session_id, **options)
            if not continued.text:
                continued.text = result.text
            if continued.rate_limit_info is None:
                continued.rate_limit_info = result.rate_limit_info
            return continued
        except BaseException:
            if control is not None:
                control.close()
            raise

    async def _run(self, provider, prompt, cwd, session_id, *, fresh, model=None,
                  effort='medium',
                  instructions=None, on_start=None, account_alias=None, resume_path=None,
                  control=None, on_session=None, initial_steer=None, host_id=None,
                  attach=False, last_seq=0, on_seq=None, secrets=None, on_host=None, on_problem=None,
                  goal=None, secret_names=(), current_goal=None, worker_id=None, on_cancel=None, stopped=None,
                  goal_only=False, reload_secrets=None):
        cwd = Path(cwd).resolve()
        acp_factory = ADAPTERS.get(provider)
        if provider not in ('claude', 'codex') and ((acp_factory is None and provider != 'muse') or self.opencode_store is None):
            raise ValueError('Unknown provider')
        if not attach and acp_factory is None and provider != 'muse' and provider not in self.binaries:
            raise ValueError('Unknown provider')
        if provider == 'muse' and attach and host_id:
            process = HostClient(self.state_dir / 'hosts' / host_id, on_problem=on_problem)
            try:
                await process.stop()
                return RunResult(session_id, pid=process.child_pid,
                                 log_path=str(process.directory / 'stderr.log'), failure_code='interrupted',
                                 error='Muse was stopped after restart because fresh workers cannot reattach.')
            finally:
                process.detach()
                if control is not None:
                    control.close()
                if on_cancel:
                    on_cancel(None)
        if provider == 'codex' and model is None and self.codex_model is not None:
            model = self.codex_model()
        if session_id is not None:
            if not native_ids.valid(provider, session_id):
                raise ValueError('Native session ID must be a canonical UUID')
        if not fresh and not session_id:
            raise ValueError('Resume requires an exact native session UUID')
        if fresh and provider == 'codex' and session_id:
            raise ValueError('Codex assigns fresh session IDs')
        if fresh and provider == 'claude' and not session_id:
            session_id = str(uuid.uuid4())
        if resume_path:
            path = Path(resume_path)
            if provider != 'claude' or fresh or not path.is_absolute() or not path.is_file() or path.name != session_id + '.jsonl':
                raise ValueError('Resume requires the exact existing native transcript path')
        if control is not None:
            control.begin_attempt()
        result = RunResult(session_id=session_id)
        if control is not None:
            control.stop_before_launch = goal_only or provider == 'codex'
        if (goal_only or provider == 'codex') and stopped and stopped():
            if control is not None:
                control.close()
            result.failure_code = 'owner_stopped'
            result.error = 'The owner stopped this worker.'
            return result
        scrub = Scrubber(secrets or {})
        problem = on_problem or (lambda area, code, detail: None)
        process = None
        tasks = []
        input_task = None
        terminal = False
        terminal_ready = asyncio.Event()
        terminal_drained = False
        job_store = getattr(self.account_broker, 'store', None) or getattr(self.codex_broker, 'store', None)
        auto_finish = worker_id is not None and job_store is not None and job_store.get('auto_resume', False) is True
        provider_failed = False
        observed_id = None
        claude_prompt = None
        claude_report = None
        claude_late_notes = 0
        protocol = None
        acp_error = {}
        acp_adapter = acp_factory(problem, error=acp_error) if acp_factory is not None else None
        if provider == 'muse':
            acp_adapter = muse_runtime.adapter(problem, error=acp_error)

        def observe_session(sid):
            nonlocal observed_id
            if not native_ids.valid(provider, sid):
                raise RuntimeError('Provider returned an invalid session ID')
            if (session_id and sid != session_id) or (observed_id and sid != observed_id):
                raise RuntimeError('Provider changed the native session ID; execution stopped')
            if not observed_id:
                observed_id = sid
                result.session_id = sid
                if self.opencode_store is not None and worker_id is not None:
                    from .session_storage import home_for, remember
                    remember(self.opencode_store, provider, sid, home_for(provider, spec.get('env') or {}),
                             spec['cwd'], worker=worker_id, host=host_name, created=spec.get('fresh') is True)
                if on_session:
                    on_session(sid)
            return sid
        try:
            if provider == 'muse':
                store = self.opencode_store
                if store.get('muse_enabled', False) is not True:
                    raise ValueError('Muse workers are off.')
                if goal is not None or current_goal is not None or goal_only or secrets or secret_names:
                    raise ValueError('Muse workers do not take goals or secrets.')
                if not fresh or attach or session_id is not None:
                    raise ValueError('Muse workers require a fresh session until resume is supported.')
                if platform != 'darwin':
                    raise ValueError('Muse requires macOS.')
                model = muse_runtime.DEFAULT_MODEL if model is None else model
                if model not in muse_runtime.MODELS:
                    raise ValueError('Muse model is unavailable.')
                binary = muse_accounts.binary(store)
                muse_accounts.check_binary(binary)
                broker = muse_accounts.MuseBroker(store)
                account_alias = broker.run_ready(account_alias)
                muse_home = muse_accounts.account_home(broker.accounts()[account_alias]['config_dir'])
                self.binaries['muse'] = binary
            if provider == 'opencode' and not attach:
                from . import opencode_accounts
                store = self.opencode_store
                if store.get('opencode_enabled', False) is not True:
                    raise ValueError('OpenCode workers are off.')
                if goal is not None or secrets or secret_names:
                    raise ValueError('OpenCode workers do not take goals or secrets.')
                saved_subscription = opencode_accounts.accounts(store).get(account_alias, {}).get('provider') == 'opencode-go'
                alias = (account_alias if saved_subscription else opencode_accounts.active(store)) if fresh else account_alias
                if not fresh and not opencode_runtime.resume_account_ready(store, alias):
                    result.failure_code = 'account_changed'
                    result.error = 'The saved OpenCode account is no longer ready.'
                    return result
                if fresh and (alias is None or account_alias not in (None, alias)):
                    raise ValueError('OpenCode requires the active account.')
                account_alias = alias
                opencode_accounts.check_key(alias)
                if not opencode_runtime.managed_config_clear():
                    result.failure_code = 'provider_managed_config'
                    result.error = ('OpenCode subscription' if saved_subscription and alias == account_alias else 'OpenCode account ' + alias) + (
                        ' failed: provider_managed_config.')
                    return result
                account = opencode_accounts.accounts(store)[alias]
                if model is not None and model != account['model']:
                    raise ValueError('OpenCode model must match the active account.')
                model = account['model']
                binary = opencode_accounts.binary(store)
                if not binary or not Path(binary).is_absolute() or not Path(binary).is_file():
                    raise ValueError('OpenCode requires the pinned binary path.')
                self.binaries['opencode'] = binary
            if provider == 'codex':
                from .envelopes import valid_name
                secret_names = sorted(set(secret_names) | set(secrets or {}))
                invalid = [name for name in secret_names if not valid_name(name)]
                if invalid:
                    raise SecretDeliveryRefused('Secret environment names are stripped or control the runtime: ' + ', '.join(invalid))
                if secret_names and control is None:
                    raise SecretDeliveryRefused('Codex secret delivery requires a private worker app-server.')
            if session_id and not attach:
                if self._host_owns(provider, session_id):
                    result.failure_code = 'session_busy'
                    result.error = 'Session is already owned by another provider host'
                    return result
                if await self._external_writer(provider, session_id):
                    result.failure_code = 'session_busy'
                    result.error = 'Session is already owned by an external native process'
                    return result
            command = [] if attach else [self.binaries[provider]]
            if acp_factory is not None:
                command = [] if attach else acp_adapter.argv(self.binaries[provider], cwd)
            elif provider == 'muse':
                command = [] if attach else muse_runtime.argv(binary)
            elif provider == 'claude':
                command += ['--print', '--verbose', '--output-format', 'stream-json', '--dangerously-skip-permissions']
                command += ['--effort', effort, '--settings', json.dumps({'ultracode': False,
                    'claudeMdExcludes': claude_md_excludes(Path.home())})]
                if control is not None:
                    command += ['--input-format', 'stream-json', '--replay-user-messages']
                command += ['--session-id' if fresh else '--resume', resume_path or session_id]
            elif provider == 'codex':
                codex_effort = codex_reasoning_effort(effort)
                command += ['-c', 'model_reasoning_effort=' + codex_effort]
                if control is not None and goal_only:
                    command += ['-c', 'features.goals=true']
                if secret_names:
                    for override in CODEX_SECRET_OVERRIDES:
                        command += ['-c', override]
                if self.codex_broker:
                    command += ['-c', 'skills.include_instructions=false']
                command += ['app-server', '--listen', 'stdio://'] if control is not None else ['exec']
                if control is None:
                    if not fresh:
                        command += ['resume']
                    command += ['--json', '--dangerously-bypass-approvals-and-sandbox', '--skip-git-repo-check']
            else:
                raise ValueError('Unknown provider')
            if model and acp_factory is None and provider != 'muse' and (provider == 'claude' or control is None):
                command += ['--model', model]
            if provider == 'codex' and control is None:
                if not fresh:
                    command += [session_id]
                command += ['-']
            if provider == 'claude' and not attach:
                shared_mcp_config = write_shared_mcp_config(
                    self.account_broker.store, scrub)
                if shared_mcp_config is not None:
                    command += ['--mcp-config', str(shared_mcp_config)]
            payload = ((instructions + '\n\n') if instructions else '') + prompt
            if provider == 'muse':
                payload = muse_runtime.AGENTS_LINE + '\n\n' + payload
            start_fresh = fresh
            start_model = model
            host_name = host_id or 'worker-' + str(uuid.uuid4())
            version = 1
            while not attach and (self.state_dir / 'hosts' / host_name).exists():
                version += 1
                host_name = host_id + '.' + str(version)
            directory = self.state_dir / 'hosts' / host_name
            if on_host and not attach:
                on_host(host_name)
            if attach:
                spec = json.loads((directory / 'spec.json').read_text())
                if provider == 'opencode':
                    config = Path(spec['env']['XDG_CONFIG_HOME']) / 'opencode' / 'opencode.json'
                    try:
                        restored = json.loads(config.read_text())
                        scrub_shared_mcp_entries(restored['mcp'], scrub)
                    except (OSError, UnicodeError, ValueError, KeyError, TypeError, AttributeError):
                        from . import opencode_accounts
                        entries = read_shared_mcp_entries(self.opencode_store, scrub, reserved_names=('torii',))
                        restored = opencode_runtime.worker_config(
                            opencode_accounts.accounts(self.opencode_store)[spec['account']],
                            opencode_accounts.account_home(spec['account']) / 'key', Path(spec['cwd']), entries)
                    if 'torii' in restored['mcp']:
                        del restored['mcp']['torii']
                        restored['permission'].pop('torii_*', None)
                        problems.record(self.opencode_store, 'shared-mcp', 'entry-reserved', 'entry=torii')
                    restored['$schema'] = 'https://opencode.ai/config.json'
                    config.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                    opencode_runtime.write_private(config, (json.dumps(restored) + '\n').encode())
                if provider == 'codex' and (secret_names or spec.get('secret_names')):
                    overrides = {spec['argv'][index + 1] for index, arg in enumerate(spec['argv'][:-1])
                                 if arg == '-c'}
                    if not set(CODEX_SECRET_OVERRIDES).issubset(overrides):
                        raise SecretDeliveryRefused('Cannot reattach a Codex secret worker without its safe launch controls.')
                process = HostClient.attach(directory, last_seq=last_seq, on_seq=on_seq, on_problem=problem)
                await process.connect()
                payload = spec['initial_payload']
                start_fresh = spec['fresh']
                start_model = spec['model']
                cwd = Path(spec['cwd'])
            else:
                if provider == 'opencode':
                    root = self.state_dir / 'opencode' / 'workers' / str(worker_id if worker_id is not None else host_name)
                    env = acp_adapter.environment(root, child_environment())
                    shared_mcp_entries = read_shared_mcp_entries(store, scrub, reserved_names=('torii',))
                    opencode_runtime.write_config(Path(env['XDG_CONFIG_HOME']).parent, account,
                        opencode_accounts.account_home(account_alias) / 'key', cwd, shared_mcp_entries)
                elif provider in ('claude', 'codex'):
                    env = self._environment(provider, account_alias)
                elif provider == 'muse':
                    env = muse_accounts.environment(muse_home)
                else:
                    raise ValueError('Unknown provider')
                if provider == 'codex' and self.codex_broker:
                    from .codex_accounts import CODEX_SKILLS_POINTER, codex_skills_pointer
                    payload = payload.replace(CODEX_SKILLS_POINTER, codex_skills_pointer(env))
                worker_temp = worker_id is not None and self.opencode_store is not None
                if worker_temp and provider != 'opencode':
                    from .worker_storage import prepare_temp
                    env['TMPDIR'] = await prepare_temp(self.opencode_store, worker_id)
                private_names = set(secrets or {}) | set(secret_names if provider == 'codex' else ())
                env = {key: value for key, value in env.items() if key not in private_names}
                secrets = {} if provider == 'muse' or acp_factory is not None else await self.launch_secrets(env, secrets)
                scrub.extend(Scrubber(secrets).forms)
                spec = {'argv': command, 'cwd': str(cwd), 'env': env,
                        'provider': provider, 'locks': [provider + '-' + session_id] if session_id else [],
                        'initial_payload': payload, 'fresh': fresh, 'model': model}
                if worker_temp:
                    spec['worker_temp'] = True
                if provider == 'codex':
                    spec.update(initial_goal=current_goal() if current_goal else goal, secret_names=secret_names)
                    if control is not None and spec['initial_goal'] and 'features.goals=true' not in command:
                        command[3:3] = ['-c', 'features.goals=true']
                    if goal_only:
                        spec['goal_only'] = True
                if acp_factory is not None:
                    spec['account'] = account_alias
                if provider == 'opencode' and platform == 'darwin':
                    spec['ownership'] = 'coalition'
                elif provider in ('claude', 'codex') and worker_id is not None and platform == 'darwin':
                    spec['ownership'] = 'coalition'
                elif provider == 'muse':
                    from .worker_storage import results_directory
                    worker = store.db.execute('SELECT task FROM workers WHERE id=?', (worker_id,)).fetchone()
                    results = (results_directory(store, worker['task'], create=True) if worker else
                               str(store.directory / 'results' / host_name))
                    Path(results).mkdir(mode=0o700, parents=True, exist_ok=True)
                    roots = muse_runtime.write_roots(cwd, env, results)
                    git = await muse_runtime.git_writes(store, worker, cwd, roots)
                    spec['argv'] = muse_accounts.write_boundary(command, [*roots, *git.roots], files=git.files)
                    spec.update(account=account_alias, ownership='coalition',
                                write_boundary={'mode': 'inline', 'profile': spec['argv'][2]})
                process = await HostClient.launch(directory, spec, on_seq=on_seq,
                                                  private={'env': secrets, 'scrub': scrub.forms}, on_problem=problem,
                                                  record_failure=bool(self.opencode_store))
            if stopped and stopped():
                await process.stop()
                if control is not None:
                    control.close()
                result.failure_code = 'owner_stopped'
                result.error = 'The owner stopped this worker.'
                return result
            result.pid = process.child_pid
            result.log_path = str(directory / 'stderr.log')
            result.managed = provider == 'claude' and self.extension.state(spec['env']) == 'managed'
            if on_start:
                on_start(result.pid)

            if acp_factory is not None:
                control = control or RunControl()
                protocol = AcpProtocol(acp_adapter, process, control, session_id, observe_session, reattaching=attach)
                if on_cancel:
                    on_cancel(protocol.cancel)
                if stopped and stopped():
                    protocol.cancel()
            elif provider == 'muse':
                control = control or RunControl()
                protocol = MspProtocol(process, control, session_id, observe_session,
                                       account_home=muse_home, problem=problem)
                if on_cancel:
                    on_cancel(protocol.cancel)
                if stopped and stopped():
                    protocol.cancel()
            elif control is not None:
                protocol = NativeProtocol(provider, process, control, session_id, observe_session,
                                          on_control_request=self.extension.control_handler(spec['env']),
                                          initial_goal=spec.get('initial_goal'), reattaching=attach,
                                          secret_names=spec.get('secret_names', secret_names),
                                          goals_enabled='features.goals=true' in spec.get('argv', []))
                if provider == 'codex' and attach and not protocol.goals_enabled:
                    saved_goal = current_goal() if current_goal else goal
                    if saved_goal:
                        protocol.goal_restart = saved_goal
                        control.suspend()

            async def write_input():
                if protocol is not None:
                    if provider == 'codex':
                        await protocol.start(payload, cwd, start_fresh, start_model,
                                             goal_only=spec.get('goal_only', False))
                    elif provider == 'muse':
                        await protocol.start(payload, cwd, start_fresh, start_model, thread_options={'effort': effort})
                    else:
                        await protocol.start(payload, cwd, start_fresh, start_model)
                    if initial_steer:
                        receipt = await control.steer('torii-goal-' + str(session_id), initial_steer)
                        if receipt != 'received':
                            problem('worker', 'goal-' + receipt, None)
                    return
                try:
                    if not process.written_lines():
                        process.stdin.write((payload + '\n').encode())
                        await process.stdin.drain()
                except (BrokenPipeError, ConnectionResetError):
                    pass
                finally:
                    process.stdin.close()
                    await process.stdin.wait_closed()

            def parse_event(raw, replay=False):
                nonlocal terminal, provider_failed, observed_id
                nonlocal claude_prompt, claude_report, claude_late_notes
                try:
                    event = json.loads(raw)
                except ValueError:
                    return
                if not isinstance(event, dict):
                    return
                hook_event = (provider == 'claude' and event.get('type') == 'system'
                              and str(event.get('subtype', '')).startswith('hook_'))
                sid = None if hook_event else (event.get('session_id') if provider == 'claude' else event.get('thread_id'))
                if sid:
                    observe_session(sid)
                if protocol is not None:
                    event = protocol.handle(event, replay)
                    if event is None:
                        return
                kind = event.get('type')
                if provider == 'claude':
                    prompt_id = (event.get('uuid') if kind == 'user' and event.get('isReplay') is True
                                 and event.get('parent_tool_use_id') is None else
                                 event.get('command_uuid') if kind == 'command_lifecycle'
                                 and event.get('state') == 'started' else None)
                    if prompt_id and process.past_user(prompt_id):
                        claude_prompt = prompt_id
                if provider == 'claude' and kind == 'goal_status':
                    result.goal_status = {key: event.get(key) for key in
                                          ('status', 'condition', 'reason', 'iterations')}
                if provider == 'codex' and kind == 'goal_status':
                    result.goal_status = event.get('goal')
                if provider == 'claude' and kind == 'rate_limit_event':
                    info = event.get('rate_limit_info', {})
                    if isinstance(info, dict):
                        result.rate_limit_info = {k: info[k] for k in ('status', 'resetsAt', 'rateLimitType', 'utilization') if k in info}
                        result.quota_limited = info.get('status') == 'rejected'
                if provider == 'claude' and kind == 'result':
                    terminal = True
                    text = event.get('result') or ''
                    if claude_report is None or claude_report[0] != claude_prompt or not claude_report[1]:
                        claude_report = (claude_prompt, text)
                        claude_late_notes = 0
                        result.text = text
                    elif text and text != claude_report[1]:
                        claude_late_notes += 1
                        label = 'Later, after a background task finished'
                        if claude_late_notes > 1:
                            label += ' (%d earlier note%s left out)' % (
                                claude_late_notes - 1, 's' if claude_late_notes > 2 else '')
                        result.text = claude_report[1] + '\n\n' + label + ': ' + text
                    result.structured = event.get('structured_output')
                    provider_failed = bool(event.get('is_error')) or event.get('subtype') != 'success'
                    if provider_failed:
                        result.error = 'Claude reported an unsuccessful terminal result; inspect the private log'
                        detail = text.lower()
                        if protocol is not None and protocol.failure == 'accounts_unavailable':
                            result.failure_code = 'accounts_unavailable'
                            result.error = 'No enabled Claude account is available.'
                        elif protocol is not None and protocol.failure:
                            result.failure_code = 'auth_failed'
                            result.error = 'Claude could not sign in; see Torii problems'
                        elif any(phrase in detail for phrase in ('not logged in', 'authentication failed',
                                                                 'failed to authenticate', 'invalid api key')):
                            result.failure_code = 'auth_failed'
                        elif 'model' in detail and any(phrase in detail for phrase in ('not available', 'not found', 'does not exist', 'do not have access')):
                            result.failure_code = 'model_unavailable'
                elif provider == 'codex':
                    limits = event.get('rate_limits') or (event.get('info') or {}).get('rate_limits')
                    if isinstance(limits, dict):
                        result.rate_limit_info = limits
                    if kind == 'item.completed':
                        item = event.get('item', {})
                        if item.get('type') == 'agent_message':
                            result.text = item.get('text', '')
                    elif kind == 'turn.completed':
                        terminal = True
                    elif kind == 'turn.failed':
                        terminal = True
                        provider_failed = True
                        result.error = 'Codex reported a failed turn; inspect the private log'
                        result.failure_detail = (event.get('error') or {}).get('detail')
                        if (event.get('error') or {}).get('codex_error_info') == 'usage_limit_exceeded':
                            result.quota_limited = True
                            result.failure_code = 'quota_limited'
                if terminal and (protocol is None or protocol.terminal):
                    terminal_ready.set()

            async def read_stream():
                if attach:
                    past = list(process.history(last_seq))
                    for old in past:
                        parse_event(old, replay=True)
                    if protocol is not None:
                        for request in self.extension.unanswered(past, process.history_after(last_seq)):
                            protocol.handle(request)
                while True:
                    raw = await process.next_event()
                    if raw is None:
                        if protocol is not None:
                            if provider == 'codex':
                                protocol.disconnect(preserve_control=protocol.goal_restart is not None)
                            else:
                                protocol.disconnect()
                        break
                    parse_event(raw)
                    process.ack()
            reader_task = asyncio.create_task(read_stream())
            tasks = [reader_task]
            input_task = asyncio.create_task(write_input())
            tasks.append(input_task)
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            if reader_task.done():
                reader_task.result()
            await asyncio.shield(input_task)
            if provider == 'muse':
                terminal_task = asyncio.create_task(protocol.wait_terminal())
                tasks.append(terminal_task)
                await asyncio.wait((reader_task, terminal_task), return_when=asyncio.FIRST_COMPLETED)
                try:
                    await asyncio.wait_for(asyncio.shield(reader_task), acp_adapter.exit_seconds)
                except asyncio.TimeoutError:
                    await process.stop()
                    await reader_task
            elif auto_finish and provider in ('claude', 'codex'):
                terminal_task = asyncio.create_task(terminal_ready.wait())
                tasks.append(terminal_task)
                await asyncio.wait((reader_task, terminal_task), return_when=asyncio.FIRST_COMPLETED)
                if terminal_task.done() and not reader_task.done():
                    try:
                        await asyncio.wait_for(asyncio.shield(reader_task), WORKER_EXIT_GRACE)
                    except asyncio.TimeoutError:
                        terminal_drained = True
                        await process.stop()
                        await reader_task
            else:
                await reader_task
            await process.wait()
            if provider == 'codex' and protocol is not None:
                result.goal_status = protocol.goal
                if protocol.goal_error:
                    provider_failed = True
                    result.failure_code = 'goal_failed'
                    result.error = protocol.goal_error
            if acp_adapter is not None:
                result.text = protocol.text
                result.spend = protocol.spend
                result.failure_code = protocol.failure
                result.success = protocol.outcome == 'succeeded' and bool(observed_id)
                if stopped and stopped():
                    result.success = False
                    result.failure_code = 'owner_stopped'
                if provider == 'opencode':
                    from . import opencode_accounts
                    root = Path(spec['env']['XDG_DATA_HOME']).parent
                    opencode_runtime.finish(result, root, spec.get('account'), acp_error,
                                            launch_root=Path(spec['env']['XDG_CONFIG_HOME']).parent,
                                            subscription=opencode_accounts.accounts(self.opencode_store).get(
                                                spec.get('account'), {}).get('provider') == 'opencode-go')
                elif provider == 'muse':
                    acp_error.update(account=account_alias, detail=protocol.detail, resets_at=protocol.resets_at)
                    muse_runtime.finish(result, acp_error, store)
                else:
                    raise ValueError('Unknown provider')
            else:
                result.success = ((process.returncode == 0 or terminal_drained) and (terminal or goal_only and protocol.terminal)
                                  and not provider_failed and bool(observed_id)
                                  and (protocol is None or protocol.terminal))
            if provider == 'codex' and protocol is not None and protocol.goal_restart is not None and result.success:
                result.failure_code = 'goal_feature_required'
                result.goal_status = {'objective': protocol.goal_restart}
            if provider == 'claude':
                if resume_path:
                    result.transcript_path = str(Path(resume_path).resolve())
                else:
                    from .accounts import find_transcript
                    result.transcript_path = find_transcript(
                        result.session_id, self.account_broker.transcript_roots() if self.account_broker else [])
            if not result.success and protocol is not None and acp_adapter is None and protocol.background and not protocol.terminal:
                result.failure_code = 'pending_work'
                result.error = 'The worker exited while its background work was still running.'
            if not result.success and not result.error:
                result.error = 'Provider exited without a verified successful terminal result (exit %s)' % process.returncode
            return result
        except asyncio.CancelledError:
            if process is not None and host_id is None:
                cleanup = asyncio.create_task(process.stop())
                while not cleanup.done():
                    try:
                        await asyncio.shield(cleanup)
                    except asyncio.CancelledError:
                        continue
            elif input_task is not None:
                while not input_task.done():
                    try:
                        await asyncio.shield(input_task)
                    except asyncio.CancelledError:
                        continue
                    except Exception as error:
                        problem('worker', 'input-failed', internal_detail(error))
                        break
            raise
        except Exception as exc:
            result.failure_code = ('ownership_unavailable' if isinstance(exc, process_ownership.OwnershipUnavailable)
                                   else 'session_busy' if isinstance(exc, SessionBusy) else 'start_failed' if process is None else 'host_died' if isinstance(exc, HostDied)
                                   else 'execution_failed')
            result.error = ('The provider host stopped without an exit record.' if isinstance(exc, HostDied)
                            else str(exc) if isinstance(exc, (SecretDeliveryRefused, SessionBusy, process_ownership.OwnershipUnavailable))
                            else 'Provider execution failed: ' + type(exc).__name__)
            result.failure_detail = internal_detail(exc)
            if process is not None:
                try:
                    await process.stop()
                except CleanupFailed as error:
                    result.failure_code = 'cleanup_failed'
                    result.failure_detail = str(error)
                except (OSError, RuntimeError) as error:
                    problem('worker', 'stop-failed', internal_detail(error))
            if (provider not in ('claude', 'codex', 'opencode') and isinstance(exc, ValueError)
                    and str(exc) == 'Unknown provider'):
                raise
            return result
        finally:
            _scrub_result(result, scrub)
            if (acp_factory is not None or provider == 'muse') and on_cancel:
                on_cancel(None)
            if protocol is not None:
                if provider == 'codex':
                    protocol.disconnect(preserve_control=result.failure_code == 'goal_feature_required')
                else:
                    protocol.disconnect()
                if not protocol.input_closed.done():
                    protocol.input_closed.cancel()
                    await asyncio.gather(protocol.input_closed, return_exceptions=True)
            elif control is not None:
                control.close()
            for task in tasks:
                if not task.done():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            if acp_factory is not None and protocol is not None:
                result.prompt_sent = protocol.prompt_sent
            if process is not None:
                process.detach()
