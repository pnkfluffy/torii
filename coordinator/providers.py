"""Native CLI execution. History remains owned by the installed provider CLI."""
import asyncio
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
from typing import Any, Optional
import uuid

from . import problems
from .claude_settings import claude_md_excludes
from .failures import internal_detail
from .host import HostClient, HostDied
from .shared_mcp import write_config as write_shared_mcp_config
from .native_protocol import NativeProtocol, SecretDeliveryRefused
from .scrub import Scrubber
from . import extension
from . import host_os


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
    needs_input: bool = False
    managed: bool = False


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
    def __init__(self, state_dir, binaries=None, codex_model=None, account_broker=None):
        self.state_dir = Path(state_dir).resolve()
        self.binaries = {'claude': 'claude', 'codex': 'codex', **(binaries or {})}
        self.codex_model = codex_model
        self.codex_broker = None
        self.account_broker = account_broker
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
        if provider != 'claude':
            if self.codex_broker and account_alias:
                return self.codex_broker.environment(account_alias)
            return child_environment()
        if not self.account_broker or not account_alias:
            raise ValueError('Claude launch requires an account broker assignment')
        return self.extension.environment(self.account_broker.environment(account_alias), account_alias)

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

    async def run(self, provider, prompt, cwd, session_id, *, fresh, model=None,
                  effort='medium',
                  schema=None, instructions=None, on_start=None, account_alias=None, resume_path=None,
                  control=None, on_session=None, initial_steer=None, host_id=None,
                  attach=False, last_seq=0, on_seq=None, secrets=None, on_host=None, on_problem=None,
                  goal=None, secret_names=(), current_goal=None):
        cwd = Path(cwd).resolve()
        if provider not in ('claude', 'codex') or (not attach and provider not in self.binaries):
            raise ValueError('Unknown provider')
        if provider == 'codex' and model is None and self.codex_model is not None:
            model = self.codex_model()
        if session_id is not None:
            if str(uuid.UUID(session_id)) != session_id:
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
        scrub = Scrubber(secrets or {})
        problem = on_problem or (lambda area, code, detail: None)
        process = None
        tasks = []
        input_task = None
        terminal = False
        provider_failed = False
        observed_id = None
        protocol = None

        def observe_session(sid):
            nonlocal observed_id
            if str(uuid.UUID(sid)) != sid:
                raise RuntimeError('Provider returned an invalid session ID')
            if (session_id and sid != session_id) or (observed_id and sid != observed_id):
                raise RuntimeError('Provider changed the native session ID; execution stopped')
            if not observed_id:
                observed_id = sid
                result.session_id = sid
                if on_session:
                    on_session(sid)
            return sid
        try:
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
            if provider == 'claude':
                command += ['--print', '--verbose', '--output-format', 'stream-json', '--dangerously-skip-permissions']
                command += ['--effort', effort, '--settings', json.dumps({'ultracode': False,
                    'claudeMdExcludes': claude_md_excludes(Path.home())})]
                if control is not None and schema is None:
                    command += ['--input-format', 'stream-json', '--replay-user-messages']
                command += ['--session-id' if fresh else '--resume', resume_path or session_id]
                if schema is not None:
                    command += ['--json-schema', json.dumps(schema), '--tools', '']
            else:
                if schema is not None:
                    raise ValueError('Structured coordinator decisions use Claude')
                codex_effort = 'xhigh' if effort == 'max' else effort
                command += ['-c', 'model_reasoning_effort=' + codex_effort]
                if control is not None:
                    command += ['-c', 'features.goals=true']
                if secret_names:
                    for override in CODEX_SECRET_OVERRIDES:
                        command += ['-c', override]
                command += ['app-server', '--listen', 'stdio://'] if control is not None else ['exec']
                if control is None:
                    if not fresh:
                        command += ['resume']
                    command += ['--json', '--dangerously-bypass-approvals-and-sandbox', '--skip-git-repo-check']
            if model and (provider == 'claude' or control is None):
                command += ['--model', model]
            if provider == 'codex' and control is None:
                if not fresh:
                    command += [session_id]
                command += ['-']
            if provider == 'claude' and schema is None and not attach:
                shared_mcp_config = write_shared_mcp_config(
                    self.account_broker.store, scrub)
                if shared_mcp_config is not None:
                    command += ['--mcp-config', str(shared_mcp_config)]
            payload = ((instructions + '\n\n') if instructions else '') + prompt
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
                if provider == 'codex' and (secret_names or spec.get('secret_names')):
                    overrides = {spec['argv'][index + 1] for index, arg in enumerate(spec['argv'][:-1])
                                 if arg == '-c'}
                    if not set(CODEX_SECRET_OVERRIDES).issubset(overrides):
                        raise SecretDeliveryRefused('Cannot reattach a Codex secret worker without its safe launch controls.')
                process = HostClient.attach(directory, last_seq=last_seq, on_seq=on_seq)
                await process.connect()
                payload = spec['initial_payload']
                start_fresh = spec['fresh']
                start_model = spec['model']
                cwd = Path(spec['cwd'])
            else:
                env = self._environment(provider, account_alias)
                private_names = set(secrets or {}) | set(secret_names if provider == 'codex' else ())
                env = {key: value for key, value in env.items() if key not in private_names}
                secrets = await self.launch_secrets(env, secrets)
                scrub.extend(Scrubber(secrets).forms)
                spec = {'argv': command, 'cwd': str(cwd), 'env': env,
                        'provider': provider, 'locks': [provider + '-' + session_id] if session_id else [],
                        'initial_payload': payload, 'fresh': fresh, 'model': model}
                if provider == 'codex':
                    spec.update(initial_goal=current_goal() if current_goal else goal, secret_names=secret_names)
                process = await HostClient.launch(directory, spec, on_seq=on_seq,
                                                  private={'env': secrets, 'scrub': scrub.forms})
            result.pid = process.child_pid
            result.log_path = str(directory / 'stderr.log')
            result.managed = provider == 'claude' and self.extension.state(spec['env']) == 'managed'
            if on_start:
                on_start(result.pid)

            if control is not None and schema is None:
                protocol = NativeProtocol(provider, process, control, session_id, observe_session,
                                          on_control_request=self.extension.control_handler(spec['env']),
                                          initial_goal=spec.get('initial_goal'), reattaching=attach,
                                          secret_names=spec.get('secret_names', secret_names))
            elif control is not None:
                control.close()

            async def write_input():
                if protocol is not None:
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
                    result.text = event.get('result') or ''
                    result.structured = event.get('structured_output')
                    provider_failed = bool(event.get('is_error')) or event.get('subtype') != 'success'
                    if provider_failed:
                        result.error = 'Claude reported an unsuccessful terminal result; inspect the private log'
                        detail = result.text.lower()
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
                        if (event.get('error') or {}).get('codex_error_info') == 'usage_limit_exceeded':
                            result.quota_limited = True
                            result.failure_code = 'quota_limited'

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
            await reader_task
            await process.wait()
            if provider == 'codex' and protocol is not None:
                result.goal_status = protocol.goal
                if protocol.goal_error:
                    provider_failed = True
                    result.failure_code = 'goal_failed'
                    result.error = protocol.goal_error
            result.success = (process.returncode == 0 and terminal and not provider_failed and bool(observed_id)
                              and (protocol is None or protocol.terminal))
            if provider == 'claude':
                if resume_path:
                    result.transcript_path = str(Path(resume_path).resolve())
                else:
                    from .accounts import find_transcript
                    result.transcript_path = find_transcript(
                        result.session_id, self.account_broker.transcript_roots() if self.account_broker else [])
            if not result.success and protocol is not None and protocol.background and not protocol.terminal:
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
            result.failure_code = ('start_failed' if process is None else 'host_died' if isinstance(exc, HostDied)
                                   else 'execution_failed')
            result.error = ('The provider host stopped without an exit record.' if isinstance(exc, HostDied)
                            else str(exc) if isinstance(exc, SecretDeliveryRefused)
                            else 'Provider execution failed: ' + type(exc).__name__)
            result.failure_detail = internal_detail(exc)
            if process is not None and not attach:
                try:
                    await process.stop()
                except (OSError, RuntimeError) as error:
                    problem('worker', 'stop-failed', internal_detail(error))
            return result
        finally:
            _scrub_result(result, scrub)
            if protocol is not None:
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
            if process is not None:
                process.detach()
