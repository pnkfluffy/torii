"""Antigravity workers on Google's official agy ACP server.

Workers run like Claude and Codex workers, on Linux and macOS. With the antigravity.sandbox setting,
they run inside a Linux write boundary; macOS refuses that setting.
"""

import asyncio
import json
import os
from pathlib import Path
import pwd
import re
import shutil
import sqlite3
import stat
import subprocess
import time

from . import antigravity_accounts, host_os, native_ids, problems
from .acp_protocol import REDACTED, AcpAdapter
from .control_api import Refused


SOURCE = 'Antigravity token counts'
READ_ONLY_STATE = ('worktrees', 'cow-worktrees', 'scratch', 'results', 'hosts', 'envelope', 'backups', 'bin')
QUOTA_MARKERS = ('quota_exhausted', 'resource_exhausted', 'quota exceeded', 'usage limit', '"code": 429')
LOGIN_MARKERS = ('authentication required', 'no browser is available', 'please sign in', 'not signed in')
IMAGE_SUFFIXES = ('.jpg', '.jpeg', '.png', '.webp', '.gif')
IMAGE_BYTES = 32 * 1024 * 1024
SUBAGENT_BYTES = 1024 * 1024
SUBAGENT_NUDGE = ('A subagent you started is still working, and this run ends with it. Do not end your turn yet: '
                  'wait for its message, save its output, check that the output exists, then report.')
SESSION_DATA = ('conversations', 'brain', 'annotations', 'implicit')
AGY_DATA = ('.gemini', 'antigravity-cli')
PRIVATE_REFS = (('heads', 'refs/heads/torii'), ('head-logs', 'logs/refs/heads/torii'),
                ('remotes', 'refs/remotes'), ('remote-logs', 'logs/refs/remotes'))
GIT_SECONDS = 20
RECOVERY_REQUEST = '_cleanup.antigravity_refs'
_COMMIT = re.compile(r'[0-9a-f]{40}(?:[0-9a-f]{24})?\Z')
_HOST = re.compile(r'worker-([0-9]+)(?:\.[0-9]+)?\Z')
DATABASE = ('state.sqlite', 'state.sqlite-wal', 'state.sqlite-shm', 'state.sqlite-journal')
USAGE_FIELDS = (('input_tokens', 'inputTokens'), ('output_tokens', 'outputTokens'),
                ('cached_tokens', 'cachedReadTokens'), ('thought_tokens', 'thoughtTokens'))
GENERATION_FIELDS = {2: 'input_tokens', 3: 'output_tokens', 5: 'cached_tokens', 9: 'thought_tokens'}


def argv(binary, cwd):
    return [str(binary), 'acp', '--dangerously-skip-permissions']


def sandboxed(store):
    """True when the owner turned on the write boundary for Antigravity workers."""
    return store.get('antigravity_sandbox', False) is True


def check_sandbox():
    """Return bwrap, or refuse when this host cannot run the write boundary."""
    if not host_os.bubblewrap_supported():
        raise Refused('The Antigravity sandbox needs Linux and bubblewrap (bwrap). On macOS, keep antigravity.sandbox '
                      'off: Antigravity workers run without it, like Claude and Codex workers.')
    bwrap = shutil.which('bwrap')
    if not bwrap:
        raise Refused('The Antigravity sandbox needs bubblewrap (bwrap). Install it, or keep antigravity.sandbox off.')
    return bwrap


def op_sandbox(store, ctx, enabled):
    if enabled:
        check_sandbox()
        write_boundary(['true'], [antigravity_accounts.accounts_root()])
    store.put('antigravity_sandbox', enabled)
    return ('New Antigravity workers run inside the write boundary.' if enabled
            else 'New Antigravity workers run without the write boundary, like Claude and Codex workers.')


def account_sessions(home):
    """The agy data folder of an account home, where a worker without the boundary keeps its sessions."""
    return Path(home).joinpath(*AGY_DATA)


def mcp_servers(entries):
    return [{'type': 'http', 'name': name, 'url': entry['url'],
             'headers': [{'name': header, 'value': value} for header, value in entry['headers'].items()]}
            for name, entry in entries.items()]


def saved_mcp_servers(process):
    """The servers of the journaled session request, for a reattach."""
    for line in process.written_lines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if isinstance(event, dict) and event.get('method') in ('session/new', 'session/resume'):
            servers = (event.get('params') or {}).get('mcpServers')
            return servers if isinstance(servers, list) else []
    return []


def mcp_secrets(servers):
    values = {}
    for server in servers:
        if isinstance(server, dict):
            name = str(server.get('name'))
            values[name + '-url'] = server.get('url')
            values.update((name + '-header-' + str(index), header.get('value'))
                          for index, header in enumerate(server.get('headers') or []) if isinstance(header, dict))
    return {name: value for name, value in values.items() if isinstance(value, str) and value and value != REDACTED}


def volatile_root():
    return Path('/dev/shm')


def overlay_root():
    return volatile_root() / ('torii-agy-%d' % os.getuid())


def session_folder(alias, name):
    """A new private folder for the sessions of one worker launch.

    agy's session data (SESSION_DATA) is kept here, on disk, so a session can be resumed.
    Workers cannot see this folder from another job.
    """
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,99}', str(name)):
        raise Refused('Antigravity needs a plain worker name.')
    folder = antigravity_accounts.jobs_root() / alias / str(name)
    try:
        for path in (folder.parent, folder, *(folder / data for data in SESSION_DATA)):
            antigravity_accounts._private_directory(path)
    except OSError as error:
        raise Refused('Antigravity needs a writable private worker folder.') from error
    return folder


def _holds(folder, session_id):
    try:
        return stat.S_ISREG((Path(folder) / 'conversations' / (session_id + '.db')).lstat().st_mode)
    except OSError:
        return False


def find_session_folder(alias, session_id, sandbox=True):
    """The folder that holds a saved session, or None. Only names are checked.

    Without the boundary, the session is in the account home.
    """
    if not native_ids.valid('antigravity', session_id):
        return None
    if not sandbox:
        try:
            folder = account_sessions(antigravity_accounts.account_home(alias))
        except Refused:
            return None
        return folder if not folder.is_symlink() and _holds(folder, session_id) else None
    try:
        folders = sorted(os.scandir(antigravity_accounts.jobs_root() / alias), key=lambda entry: entry.name)
    except OSError:
        return None
    for entry in folders:
        try:
            if entry.is_dir(follow_symlinks=False) and _holds(entry.path, session_id):
                return Path(entry.path)
        except OSError:
            continue
    return None


def scratch_home(name):
    """New memory-backed folders (upper, work) for one launch: every other write to the account home lands here.

    The real account home stays as Torii and the owner left it. So a worker cannot plant agy
    configuration that Torii's own agy calls, or another job, would run. The folders are in
    memory because the kernel syncs the whole disk when it removes an overlay on a disk.
    """
    try:
        root = antigravity_accounts._private_directory(overlay_root())
        folder = root / (str(name) + '-' + os.urandom(4).hex())
        for path in (folder, folder / 'upper', folder / 'work'):
            path.mkdir(mode=0o700)
    except OSError as error:
        raise Refused('Antigravity needs a memory-backed folder for the worker home.') from error
    return folder


def remove_scratch_home(folder):
    """Delete the memory-backed folders of a finished launch. agy made everything in them again at each start."""
    if not folder:
        return
    folder = Path(folder)
    if folder.parent != overlay_root() or folder.parent.is_symlink() or folder.is_symlink():
        return
    remove_tree(folder)


def remove_tree(folder):
    """Delete a folder that held an overlay. The kernel leaves a folder with no permissions in it; open it first."""
    for base, names, _ in os.walk(folder):
        for name in names:
            if not os.path.islink(os.path.join(base, name)):
                os.chmod(os.path.join(base, name), 0o700)
    shutil.rmtree(folder, ignore_errors=True)


def session_binds(home, folder):
    return [(Path(folder) / data, Path(home).joinpath(*AGY_DATA, data)) for data in SESSION_DATA]


async def secret_places(store):
    """The places a worker must not read: what the worker secret guard and the attachment guard protect,
    every account home, other agy profiles and worker sessions, and the worker host folders.

    The state database stays, because the worker's CLI calls write it.
    """
    from .attachment_paths import attachment_paths
    protected = attachment_paths(store)
    homes = dict.fromkeys((Path.home(), Path(pwd.getpwuid(os.getuid()).pw_dir)))
    places = [*protected.directories, *(path for path in protected.files if path.name not in DATABASE),
              Path(store.directory) / 'hosts', antigravity_accounts.accounts_root(), antigravity_accounts.jobs_root(),
              *(home / '.grok' for home in homes)]
    found = await asyncio.to_thread(protected_places, homes, skip=places)
    return list(dict.fromkeys(places + [Path(path) for path in found]))


def protected_places(homes, depth=3, skip=()):
    """Existing paths at most depth levels below the homes that the worker secret guard protects.

    The walk does not enter a hit, a skipped folder or a link. Deeper files that only a name pattern
    names stay with the guard.
    """
    from .secret_guard import protected_path
    skipped = {os.fspath(path) for path in skip}
    found, pending = [], [(os.fspath(home), 0) for home in homes]
    while pending:
        folder, level = pending.pop()
        try:
            entries = list(os.scandir(folder))
        except OSError:
            continue
        for entry in entries:
            if entry.path in skipped:
                continue
            if protected_path(entry.path):
                found.append(entry.path)
            elif level + 1 < depth and entry.is_dir(follow_symlinks=False):
                pending.append((entry.path, level + 1))
    return sorted(set(found))


def write_boundary(argv, roots, *, state=None, hidden=(), home=None, binds=(), mounts=()):
    """Wrap a command in bubblewrap. Only the roots are writable.

    state is the Torii state folder. The worker's CLI calls write its database,
    so the folder is writable. Its job folders, host journals and vault are read-only
    again, and the roots are then bound on top. mounts are more bwrap mounts, such as
    the add-on's boundary_mounts(). hidden folders appear empty and hidden files read as empty.
    home is (account home, upper, work): the account home appears writable, and the writes
    go to upper. binds are (private folder, place) pairs: the place shows the private folder.
    Binds come after the home, so a place can be inside it.
    """
    bwrap = check_sandbox()
    if not argv or not roots:
        raise Refused('Antigravity needs a command and explicit worker write roots.')
    try:
        owner_home = Path.home().resolve()
        allowed = []
        for value in roots:
            path = Path(value)
            if not path.is_absolute():
                raise Refused('Antigravity write roots must be absolute folders.')
            path = path.resolve()
            if (not path.is_dir() or path == Path(path.anchor) or path == owner_home or path in owner_home.parents
                    or path.stat().st_uid != os.getuid()):
                raise Refused('Antigravity write roots must be worker folders owned by this user.')
            if path not in allowed:
                allowed.append(path)
        command = [bwrap, '--die-with-parent', '--ro-bind', '/', '/', '--dev', '/dev', '--proc', '/proc']
        if state is not None:
            state = Path(state).resolve()
            command += ['--bind', str(state), str(state)]
            for name in READ_ONLY_STATE:
                if (state / name).is_dir() and not (state / name).is_symlink():
                    command += ['--ro-bind', str(state / name), str(state / name)]
        command += list(mounts)
        kept, covered = ([] if state is None else [state]), []
        for value in hidden:
            path = Path(value).resolve()
            if (any(path == place or path in place.parents for place in kept)
                    or any(place in path.parents for place in covered)):
                continue
            if path.is_dir():
                covered.append(path)
                command += ['--tmpfs', str(path)]
            elif path.exists():
                command += ['--ro-bind', os.devnull, str(path)]
        if home is not None:
            lower, upper, work = (Path(value).resolve() for value in home)
            if not all(path.is_dir() and path.stat().st_uid == os.getuid() for path in (lower, upper, work)):
                raise Refused('Antigravity needs its account home and private worker folders.')
            command += ['--overlay-src', str(lower), '--overlay', str(upper), str(work), str(lower)]
        for path in allowed:
            command += ['--bind', str(path), str(path)]
        for source, place in binds:
            source, place = Path(source).resolve(), Path(place)
            if not source.is_dir() or not place.is_absolute() or source.stat().st_uid != os.getuid():
                raise Refused('Antigravity needs its private worker folders.')
            command += ['--bind', str(source), str(place)]
        return command + ['--', *argv]
    except (OSError, ValueError, RuntimeError) as error:
        raise Refused('Antigravity write boundary could not be created. Antigravity was not started.') from error


def _git(common, *arguments):
    done = subprocess.run(['git', '--no-replace-objects', '--git-dir', str(common), *arguments], stdin=subprocess.DEVNULL,
                          capture_output=True, text=True, timeout=GIT_SECONDS)
    return done.stdout.strip() if done.returncode == 0 else None


async def _git_write_roots(store, worker, cwd, roots):
    """Trust the saved project binding, never the child's worktree gitfile."""
    if worker is None:
        if (Path(cwd) / '.git').is_dir():
            raise Refused('Antigravity requires a job worktree and its saved project binding.')
        return ()
    task = store.task_get(worker['task'])
    topic = store.topic(task['topic'])
    project = Path(topic['cwd']).resolve()
    if any(project.is_relative_to(Path(root).resolve()) for root in roots):
        if (project / '.git').exists():
            raise Refused('Antigravity requires a project binding outside its write roots.')
        return ()
    from .workspaces import project_identity
    identity = await project_identity(project, timeout=10)
    if identity['base_commit'] is None:
        return ()
    common = Path(identity['project_key'])
    if any(common.is_relative_to(Path(root).resolve()) for root in roots):
        raise Refused('Antigravity cannot write the project Git directory.')
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
        raise Refused('Antigravity requires exactly one registered job worktree.')
    return common / 'objects', matches[0]


async def write_roots(store, worker, cwd, env, results, private=None):
    """Return (roots, binds, sync) for one worker launch.

    roots are writable as they are: the job folder, temp, results, scratch, the Git object
    store and the worktree's own Git folder. The account home is not a root; write_boundary
    takes it as home.
    Git cannot update a branch through a single bound file, because it creates a lock file
    beside it. So the worker gets private copies of the torii branch folder and the remote
    folders (binds). private must be a new folder name, so nothing in it comes from a worker.
    sync records the job's own branch file and its starting commit.
    """
    from .close_saves import scratch
    roots = [Path(cwd), Path(env['TMPDIR']), Path(results)]
    if worker is not None and worker['task'] is not None:
        roots.append(scratch(store, worker['task'], create=True))
    git_roots = await _git_write_roots(store, worker, cwd, [Path(env['HOME']), *roots])
    roots.extend(git_roots)
    if not git_roots or private is None:
        return roots, [], None
    Path(private).mkdir(mode=0o700)
    common = git_roots[0].parent
    branch = 'torii/task-' + str(worker['task'])
    binds = []
    for name, place in PRIVATE_REFS:
        source = antigravity_accounts._private_directory(Path(private) / name)
        (common / place).mkdir(mode=0o700, parents=True, exist_ok=True)
        if name == 'remotes':
            shutil.copytree(common / place, source, symlinks=True, dirs_exist_ok=True)
        binds.append((source, common / place))
    current = _git(common, 'rev-parse', '--verify', '--quiet', 'refs/heads/' + branch + '^{commit}')
    own = Path(private) / 'heads' / branch.removeprefix('torii/')
    if current is None or own.parent != Path(private) / 'heads':
        raise Refused('Antigravity requires the job branch torii/task-<id>.')
    own.write_text(current + '\n')
    return roots, binds, {'common': str(common), 'branch': branch, 'file': str(own), 'start': current}


def _read_tip(path):
    heads = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=heads)
        with os.fdopen(descriptor) as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise ValueError('Private job ref must be a regular file')
            commit = stream.read(100).strip()
    finally:
        os.close(heads)
    if not _COMMIT.fullmatch(commit):
        raise ValueError('Invalid private job ref')
    return commit


def _tip(common, reference):
    return _git(common, 'rev-parse', '--verify', '--quiet', reference)


def _update_tip(common, reference, commit, previous, reason):
    return _git(common, 'update-ref', '--no-deref', '-m', reason, reference, commit, previous) is not None


def sync_branch(sync):
    """Publish a finished launch's branch only if the real ref still matches its starting commit."""
    if not sync:
        return None
    try:
        path = Path(sync['file'])
        commit = _read_tip(path)
        common, reference = sync['common'], 'refs/heads/' + sync['branch']
        if _git(common, 'cat-file', '-t', commit) != 'commit':
            return None
        start = sync.get('start')
        if start and _update_tip(common, reference, commit, start, 'torii: antigravity worker'):
            shutil.rmtree(path.parent.parent)
            return commit if commit != start else None
    except (OSError, ValueError, KeyError, subprocess.SubprocessError):
        return None


def request_recovery(store, task):
    params, now = json.dumps({'task': task}), time.time()
    store.db.execute("""INSERT INTO service_requests(op,params,created,updated)
        SELECT ?,?,?,? WHERE EXISTS (SELECT 1 FROM workers WHERE task=? AND provider='antigravity')""",
                     (RECOVERY_REQUEST, params, now, now, task))


def _directories(root, pattern='*'):
    return sorted(path for path in root.glob(pattern) if not path.is_symlink() and path.is_dir())


def _recover_ref(common, branch, host, folder):
    try:
        commit = _read_tip(folder / 'heads' / branch)
    except FileNotFoundError:
        shutil.rmtree(folder)
        return None
    if common is None or _git(common, 'cat-file', '-t', commit) != 'commit':
        raise ValueError('Private branch commit is unavailable in the project repository')
    reference = 'refs/heads/torii/' + branch
    for _ in range(2):
        current = _tip(common, reference)
        if not current or _git(common, 'merge-base', '--is-ancestor', current, commit) is None:
            backup = reference + '-recovered-' + host
            if _update_tip(common, backup, commit, '', 'torii: preserve antigravity worker') or _tip(common, backup) == commit:
                return backup.removeprefix('refs/heads/')
            raise ValueError('Could not save ' + backup.removeprefix('refs/heads/'))
        if _update_tip(common, reference, commit, current, 'torii: recover antigravity worker'):
            shutil.rmtree(folder)
            return None
    raise ValueError('Could not fast-forward the job branch')


def _recovery_folders():
    overlay = overlay_root()
    jobs = antigravity_accounts.jobs_root()
    overlays = [] if overlay.is_symlink() else _directories(overlay)
    refs = [folder for alias in _directories(jobs) for session in _directories(alias)
            for folder in _directories(session, 'refs-*')]
    return overlays, refs


def _recover_files(state, project, task, folders, workers, owners):
    from .host import alive, host_state

    retired = {}

    def stopped(name):
        match = _HOST.fullmatch(name)
        worker = owners.get(name, int(match[1]) if match else None)
        if workers.get(worker, True):
            return False
        if name not in retired:
            directory = state / 'hosts' / name
            if directory.is_symlink():
                return False
            child = directory / 'child.pid'
            try:
                status = host_state(directory)
                retired[name] = (status == 'exited' or
                                 status == 'dead' and not (child.is_file() and alive(child.read_text().strip())))
            except OSError:
                return False
        return retired[name]

    overlays, refs = folders
    for path in overlays:
        name, _, suffix = path.name.rpartition('-')
        if re.fullmatch(r'[0-9a-f]{8}', suffix) and stopped(name):
            remove_scratch_home(path)

    leftovers = [(folder.name[5:], folder) for folder in refs if stopped(folder.name[5:])]
    if not leftovers:
        return []
    common = None
    if project is not None:
        found = subprocess.run(['git', '-C', project, 'rev-parse', '--git-common-dir'],
                               stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=GIT_SECONDS)
        if found.returncode == 0:
            common = Path(project) / found.stdout.strip()
    notes = []
    for host, folder in leftovers:
        try:
            backup = _recover_ref(common, 'task-' + str(task), host, folder)
            if backup:
                notes.append(('branch-recovered-as-backup', backup, folder))
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            detail = str(error) if isinstance(error, ValueError) else type(error).__name__
            notes.append(('branch-recovery-failed', host + ': ' + detail, None))
    return notes


async def recover_job(store, task, *, launching=None):
    """Recover stopped hosts for this job across accounts and resumed session folders."""
    from .workspaces import _repository_lock

    if not store.db.execute("SELECT 1 FROM workers WHERE task=? AND provider='antigravity'", (task,)).fetchone():
        return True
    async with _repository_lock(store.directory, 'antigravity-recovery:%d' % task):
        try:
            folders = await asyncio.to_thread(_recovery_folders)
            rows = store.db.execute("""SELECT w.id,w.host,w.status,h.name FROM workers w
                LEFT JOIN host_log_hosts h ON h.worker=w.id WHERE w.task=? AND w.provider='antigravity'""", (task,))
            workers, owners = {}, {}
            for row in rows:
                workers[row['id']] = row['status'] == 'running' and row['id'] != launching
                owners.update((name, row['id']) for name in (row['host'], row['name']) if name)
            job = store.task_get(task)
            topic = store.topic(job['topic']) if job else None
            notes = await asyncio.to_thread(_recover_files, store.directory, (topic or {}).get('cwd'),
                                            task, folders, workers, owners)
        except (OSError, Refused, subprocess.SubprocessError) as error:
            notes = [('branch-recovery-failed', type(error).__name__, None)]
        complete = True
        for code, detail, folder in notes:
            recorded = problems.record(store, 'antigravity', code, detail, task=task)
            if folder is None or recorded is None:
                complete = False
                continue
            try:
                await asyncio.to_thread(shutil.rmtree, folder)
            except OSError as error:
                problems.record(store, 'antigravity', 'branch-recovery-failed', type(error).__name__, task=task)
                complete = False
        return complete


def unbilled(result):
    """A prompt result with no token count. agy reports an upstream failure this way, as a normal turn end."""
    tokens = result.get('usage')
    return not isinstance(tokens, dict) or not tokens.get('totalTokens')


def usage(result, last_update):
    tokens = result.get('usage') or {}
    spend = {'usd_estimate': None, 'source': SOURCE}
    for target, name in USAGE_FIELDS:
        value = tokens.get(name, 0)
        spend[target] = value if type(value) is int and value >= 0 else 0
    return spend


def classify_error(method, error):
    text = json.dumps(error).lower()
    if error.get('code') == -32000 or any(marker in text for marker in LOGIN_MARKERS):
        return 'login_required'
    if method == 'session/resume' and 'not found' in text:
        return 'session_missing'
    if any(marker in text for marker in QUOTA_MARKERS):
        return 'quota_limited'
    return 'provider_error'


def effort_level(effort, levels):
    """The thinking level for a Torii effort. xhigh and max use the highest level offered."""
    if effort in levels:
        return effort
    return levels[-1] if levels and effort in ('xhigh', 'max') else None


def model_level(model, families, levels):
    """(session model, thinking level) for a model id, or (None, None) when the session offers neither.

    agy models lists ids such as gemini-4-neon-high. The session offers the family
    gemini-4-neon and the level high as two options.
    """
    if not model or model in families:
        return model, None
    family, _, level = str(model).rpartition('-')
    return (family, level) if family in families and level in levels else (None, None)


def _options(response):
    return {option.get('id'): option for option in response.get('configOptions') or []
            if isinstance(option, dict) and option.get('type') == 'select'}


def _values(option):
    return [item.get('value') for item in (option or {}).get('options') or [] if isinstance(item, dict)]


def adapter(problem, error=None):
    async def permission(params):
        tool = params.get('toolCall') or {}
        option = None
        if not str(tool.get('toolCallId', '')).startswith('interaction_'):
            option = next((item.get('optionId') for kind in ('allow_once', 'allow_always')
                           for item in params.get('options', []) if item.get('kind') == kind), None)
        return ({'outcome': {'outcome': 'selected', 'optionId': option}} if option is not None
                else {'outcome': {'outcome': 'cancelled'}})

    def classify(method, value):
        if error is not None:
            error.update(value)
        return classify_error(method, value)

    async def session_ready(protocol, response, model, options):
        config = _options(response)
        tier = ((response.get('_meta') or {}).get('antigravity') or {}).get('tier') or {}
        if error is not None and isinstance(tier.get('name'), str):
            error['tier'] = tier['name']

        async def choose(name, value):
            result = await protocol.request('session/set_config_option', {
                'sessionId': protocol.session_id, 'configId': name, 'value': value})
            return _options(result) or config

        family, named = model_level(model, _values(config.get('model')), _values(config.get('thought_level')))
        if model:
            offered = _values(config.get('model'))
            if family is None:
                if error is not None:
                    error.update(message='Model %s is not offered to this account. Offered: %s.'
                                 % (model, ', '.join(str(value) for value in offered) or 'none'))
                protocol.complete('failed', 'model_unavailable')
                return
            if config['model'].get('currentValue') != family:
                config = await choose('model', family)
        current = model if named else ((config.get('model') or {}).get('currentValue') or model)
        if error is not None and isinstance(current, str):
            error['model'] = current[:128]
        thought = config.get('thought_level')
        level = named or effort_level(options.get('effort'), _values(thought))
        if level is not None and thought.get('currentValue') != level:
            await choose('thought_level', level)

    def prompt_usage(result, last_update):
        if error is not None:
            error['unbilled'] = unbilled(result)
        return usage(result, last_update)

    return AcpAdapter(argv=argv, environment=antigravity_accounts.environment, check_version=lambda result: True,
                      valid_session_id=lambda sid: native_ids.valid('antigravity', sid),
                      permission=permission, classify_error=classify, usage=prompt_usage,
                      queued_receipt='queued', allow_version_change_on_reattach=True,
                      resume_method='session/resume', session_ready=session_ready, steer_prompt=True)


def _open_folder(name, parent=None):
    return os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)


def collect_images(result, session_home, results):
    """Copy the images that agy generated in this session into the job results.

    agy saves them under brain/<session id>. session_home is the worker's own session folder.
    The worker can write both ends, so no symbolic link is followed on either side: each
    folder is opened by name from its parent, and each new file is created exclusively.
    Only real image files of that one folder are copied.
    """
    if not session_home or not results or not native_ids.valid('antigravity', result.session_id):
        return []
    copied = []
    folders = []
    try:
        folders.append(_open_folder(str(session_home)))
        for name in ('brain', result.session_id):
            folders.append(_open_folder(name, folders[-1]))
        source = folders[-1]
        names = sorted(name for name in os.listdir(source) if name.lower().endswith(IMAGE_SUFFIXES))
        if not names:
            return []
        folders.append(_open_folder(str(results)))
        try:
            os.mkdir('images', 0o700, dir_fd=folders[-1])
        except FileExistsError:
            pass
        folders.append(_open_folder('images', folders[-1]))
        target = folders[-1]
        for name in names:
            try:
                with os.fdopen(os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=source), 'rb') as image:
                    info = os.fstat(image.fileno())
                    if not stat.S_ISREG(info.st_mode) or info.st_size > IMAGE_BYTES:
                        continue
                    created = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=target)
                    with os.fdopen(created, 'wb') as copy:
                        copy.write(image.read(IMAGE_BYTES))
                copied.append(str(Path(results) / 'images' / name))
            except OSError:
                continue
    except OSError:
        return copied
    finally:
        for folder in folders:
            os.close(folder)
    if copied:
        result.text = (result.text + '\n\n' if result.text else '') + 'Generated images:\n' + '\n'.join(copied)
    return copied


def _read_json(name, folder):
    try:
        with os.fdopen(os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=folder), 'rb') as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                return None
            value = json.loads(stream.read(SUBAGENT_BYTES))
    except (OSError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _generated(session_home, session_id, folders):
    folders.append(_open_folder(str(session_home)))
    for name in ('brain', session_id, '.system_generated'):
        folders.append(_open_folder(name, folders[-1]))
    return folders[-1]


def subagent_ids(session_home, session_id, since=0):
    """The conversations of the subagents that this session started at or after since.

    agy writes brain/<session id>/.system_generated/subagents/<conversation>.json when a
    subagent starts.
    """
    if not session_home or not native_ids.valid('antigravity', session_id):
        return set()
    folders = []
    try:
        folders.append(_open_folder('subagents', _generated(session_home, session_id, folders)))
        return {str((_read_json(name, folders[-1]) or {}).get('conversationId') or name[:-5])
                for name in os.listdir(folders[-1])
                if name.endswith('.json') and os.stat(name, dir_fd=folders[-1], follow_symlinks=False).st_mtime >= since}
    except OSError:
        return set()
    finally:
        for folder in folders:
            os.close(folder)


def pending_subagents(session_home, session_id, since=0):
    """The subagents of this session, started at or after since, that have sent no message that the session has read.

    agy writes messages/<id>.json when a subagent sends its result. read.json marks the
    messages that the session has read. A subagent stays alive after its result, so only
    a read message shows that its work has reached the session. A subagent of an earlier
    run died with that run and keeps its old file, so since leaves it out.
    """
    started = subagent_ids(session_home, session_id, since)
    if not started:
        return started
    folders = []
    try:
        folders.append(_open_folder('messages', _generated(session_home, session_id, folders)))
        messages = folders[-1]
        read = _read_json('read.json', messages) or {}
        for name in os.listdir(messages):
            if name.endswith('.json') and read.get(name[:-5]) is True:
                started.discard(str((_read_json(name, messages) or {}).get('sender')))
        return started
    except FileNotFoundError:
        return started
    except OSError:
        return set()
    finally:
        for folder in folders:
            os.close(folder)


def _varint(data, index):
    value = shift = 0
    while True:
        byte = data[index]
        index += 1
        value |= (byte & 0x7f) << shift
        shift += 7
        if byte < 0x80:
            return value, index


def _fields(data):
    """The fields of one protobuf message: (number, value), with bytes for length-delimited values."""
    index = 0
    while index < len(data):
        key, index = _varint(data, index)
        number, wire = key >> 3, key & 7
        if wire == 0:
            value, index = _varint(data, index)
        elif wire == 2:
            size, index = _varint(data, index)
            value, index = data[index:index + size], index + size
            if index > len(data):
                raise ValueError('truncated field')
        elif wire in (1, 5):
            value, index = None, index + (8 if wire == 1 else 4)
        else:
            raise ValueError('unsupported wire type')
        yield number, value


def generation_usage(data):
    """Token counts and model of one agy gen_metadata record.

    Field 1 holds the generation. Its field 19 names the model and its field 4 holds the
    counters: 2 input, 3 output (thinking included), 5 cache read, 9 thinking.
    """
    counts, model = dict.fromkeys(GENERATION_FIELDS.values(), 0), None
    for number, generation in _fields(data):
        if number != 1 or not isinstance(generation, bytes):
            continue
        for inner, value in _fields(generation):
            if inner == 19 and isinstance(value, bytes):
                model = value.decode('utf-8', 'replace')[:120]
            elif inner == 4 and isinstance(value, bytes):
                for field, count in _fields(value):
                    if field in GENERATION_FIELDS and type(count) is int:
                        counts[GENERATION_FIELDS[field]] += count
    return counts, model


def subagent_usage(session_home, session_id):
    """The token counts of each subagent conversation of this session, from agy's own records.

    The prompt result counts the main conversation only. Each subagent has its own
    conversations/<id>.db. Only numbers and the model name are read.
    """
    usage = {}
    for conversation in sorted(subagent_ids(session_home, session_id)):
        path = Path(session_home) / 'conversations' / (conversation + '.db')
        if not native_ids.valid('antigravity', conversation):
            continue
        try:
            if not stat.S_ISREG(path.lstat().st_mode):
                continue
            database = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)
            try:
                rows = database.execute('SELECT data FROM gen_metadata').fetchall()
            finally:
                database.close()
            counts = dict.fromkeys(GENERATION_FIELDS.values(), 0)
            for (data,) in rows:
                values, model = generation_usage(data if isinstance(data, bytes) else b'')
                for key, value in values.items():
                    counts[key] += value
                counts['model'] = model or counts.get('model')
        except (OSError, sqlite3.Error, ValueError, IndexError):
            continue
        usage[conversation] = counts
    return usage


def subagent_nudge(session_home):
    """The continuation prompt for a turn that ended while a subagent of this launch still works.

    agy ends the run with its subagents. Without this, a worker that ends its turn
    before an async subagent reports loses the subagent's work.
    """
    since = time.time()

    def nudge(session_id):
        return SUBAGENT_NUDGE if pending_subagents(session_home, session_id, since) else None
    return nudge


def finish(result, error, store, alias):
    detail = str(error.get('message') or '')[:500]
    if result.success and error.get('unbilled') and result.text.lstrip().startswith('⚠'):
        result.success = False
        result.failure_code = 'provider_error'
        detail = ' '.join(result.text.split())[:500]
    if result.failure_code == 'refusal':
        result.failure_code = 'quota_limited'
    if result.failure_code == 'quota_limited':
        result.quota_limited = True
    statuses = store.get('antigravity_account_status', {}) or {}
    status = dict(statuses.get(alias) or {})
    changed = dict(status, **({'logged_in': False} if result.failure_code == 'login_required' else {}),
                   **({'tier': error['tier'][:80]} if error.get('tier') else {}))
    if changed != status and alias in antigravity_accounts.accounts(store):
        with store.db:
            statuses[alias] = changed
            store.put('antigravity_account_status', statuses)
    result.failure_detail = detail or None
    if error.get('model'):
        result.spend = dict(result.spend or {}, model=error['model'])
    if not result.success:
        result.error = ('Antigravity failed: ' + (result.failure_code or 'execution_failed') +
                        (': ' + detail if detail else '.'))[:600]
