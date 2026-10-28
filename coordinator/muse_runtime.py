"""Fresh, one-turn Muse workers over the pinned MSP host."""

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

from . import problems
from .control_api import Refused


MODELS = ('muse-spark-1.3', 'muse-wonder-1.0', 'muse-wonder-full-1.0')
DEFAULT_MODEL = 'muse-wonder-1.0'
AGENTS_LINE = 'Read AGENTS.md in the working folder before you start.'


def argv(binary):
    return [binary, 'serve', '--provider', 'meta', '--contributor-mode', 'off',
            '--disable-request-user-input', '--disable-sandbox']


def adapter(problem, error):
    return SimpleNamespace(exit_seconds=15, queued_receipt=None)


def write_roots(cwd, env, results):
    return [Path(env['HOME']), Path(cwd), Path(env['TMPDIR']), Path(results)]


@dataclass(frozen=True)
class GitWrites:
    roots: tuple = ()
    files: tuple = ()


async def git_writes(store, worker, cwd, roots):
    """Trust the saved project binding, never the child's worktree gitfile."""
    if worker is None:
        if (Path(cwd) / '.git').is_dir():
            raise Refused('Muse requires a job worktree and its saved project binding.')
        return GitWrites()
    task = store.task_get(worker['task'])
    topic = store.topic(task['topic'])
    project = Path(topic['cwd']).resolve()
    if any(project.is_relative_to(Path(root).resolve()) for root in roots):
        if (project / '.git').exists():
            raise Refused('Muse requires a project binding outside its write roots.')
        return GitWrites()
    from .workspaces import project_identity
    identity = await project_identity(project, timeout=10)
    if identity['base_commit'] is None:
        return GitWrites()
    common = Path(identity['project_key'])
    if any(common.is_relative_to(Path(root).resolve()) for root in roots):
        raise Refused('Muse cannot write the project Git directory.')
    matches = []
    for candidate in (common / 'worktrees').iterdir():
        if candidate.is_symlink() or not candidate.is_dir():
            continue
        pointer = candidate / 'gitdir'
        if not pointer.is_symlink() and pointer.is_file():
            target = Path(pointer.read_text().strip())
            if target.is_absolute() and target.name == '.git' and Path(cwd).is_relative_to(target.parent):
                matches.append(candidate)
    if len(matches) != 1:
        raise Refused('Muse requires exactly one registered job worktree.')
    branch = 'torii/task-' + str(task['id'])
    ref = common / 'refs' / 'heads' / branch
    log = common / 'logs' / 'refs' / 'heads' / branch
    for parent in (ref.parent, log.parent):
        parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    return GitWrites((common / 'objects', matches[0]),
                     (ref, Path(str(ref) + '.lock'), log, Path(str(log) + '.lock')))


def finish(result, error, store):
    detail = error.get('detail', '')[:500]
    result.failure_detail = detail or None
    if result.failure_code == 'quota_limited':
        result.quota_limited = True
        result.rate_limit_info = {'status': 'rejected', 'resets_at': error.get('resets_at')}
    elif result.failure_code == 'login_required':
        alias = error.get('account')
        with store.db:
            statuses = store.get('muse_account_status', {}) or {}
            status = statuses.setdefault(alias, {})
            status.setdefault('identity', {})['logged_in'] = False
            store.put('muse_account_status', statuses)
    elif result.failure_code in ('billing_lane', 'protocol_mismatch'):
        problems.record(store, 'muse', result.failure_code.replace('_', '-'), detail or result.failure_code)
    if not result.success:
        result.error = ('Muse failed: ' + (result.failure_code or 'execution_failed') +
                        (': ' + detail if detail else '.'))[:600]
