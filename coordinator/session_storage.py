"""Keep owned native sessions for 14 days before moving them to the Trash."""

import asyncio
from contextlib import closing, contextmanager, nullcontext
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import subprocess
import time

from . import extension, host_os, stray_report, sweeps

from . import accounts, codex_accounts, native_ids, problems


DAY = 86400
RETENTION = 14 * DAY
TRASH_PREFIX = 'torii-session-'
PROVIDERS = ('claude', 'codex', 'antigravity', 'opencode')
SID = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|ses_[0-9A-Za-z]{26}')
REQUEST = '_cleanup.sessions'


def install(store):
    store.db.execute('''CREATE TABLE IF NOT EXISTS native_sessions (
        id INTEGER PRIMARY KEY, provider TEXT NOT NULL, session TEXT NOT NULL,
        home TEXT NOT NULL, root TEXT NOT NULL, cwd TEXT NOT NULL, host TEXT,
        worker INTEGER REFERENCES workers(id), parent TEXT, generation INTEGER NOT NULL DEFAULT 0,
        planned INTEGER NOT NULL DEFAULT 0, retry_at REAL NOT NULL DEFAULT 0,
        CHECK ((worker IS NULL) != (parent IS NULL)),
        UNIQUE(provider,session,home,worker,parent))''')
    store.db.execute('''CREATE INDEX IF NOT EXISTS native_sessions_identity
        ON native_sessions(provider,session)''')
    store.db.execute('''CREATE INDEX IF NOT EXISTS native_sessions_parent
        ON native_sessions(parent,generation)''')
    store.db.execute('''CREATE INDEX IF NOT EXISTS workers_native_session ON workers(provider,session)''')
    store.db.execute('''CREATE TABLE IF NOT EXISTS native_session_hosts (
        native INTEGER NOT NULL REFERENCES native_sessions(id), host TEXT NOT NULL,
        PRIMARY KEY(native,host))''')
    store.db.execute('''CREATE TABLE IF NOT EXISTS session_deletions (
        id INTEGER PRIMARY KEY, native INTEGER NOT NULL REFERENCES native_sessions(id),
        provider TEXT NOT NULL, session TEXT NOT NULL, home TEXT NOT NULL, root TEXT NOT NULL,
        path TEXT NOT NULL, device INTEGER, inode INTEGER, bytes INTEGER NOT NULL DEFAULT 0,
        state TEXT NOT NULL DEFAULT 'pending', retry_at REAL NOT NULL DEFAULT 0,
        attempts INTEGER NOT NULL DEFAULT 0, error TEXT,
        UNIQUE(root,path,provider,session))''')
    store.db.execute('''CREATE TABLE IF NOT EXISTS session_cleanup_homes (
        provider TEXT NOT NULL, root TEXT NOT NULL, attempts INTEGER NOT NULL,
        PRIMARY KEY(provider,root))''')
    store.db.execute('''CREATE INDEX IF NOT EXISTS session_deletions_pending
        ON session_deletions(state,retry_at,id)''')


def request(store):
    if not store.db.execute("SELECT 1 FROM service_requests WHERE op=? AND state='queued'", (REQUEST,)).fetchone():
        now = time.time()
        store.db.execute('INSERT INTO service_requests(op,params,created,updated) VALUES (?,\'{}\',?,?)',
                         (REQUEST, now, now))


def home_for(provider, env, session_home=None):
    if provider == 'claude':
        return accounts.claude_home_for_environment(env)
    if provider == 'codex':
        return codex_accounts.codex_home_for_environment(env)
    if provider == 'antigravity':
        home = env.get('HOME')
        if session_home:
            return session_home
        runtime = extension.optional('antigravity_runtime')
        return str(runtime.account_sessions(home)) if runtime and home else None
    data = env.get('XDG_DATA_HOME') if provider == 'opencode' else None
    return str(Path(data).parent) if data and Path(data).name == 'data' else None


def launch_spec(path):
    try:
        with directory(path.parent) as parent:
            descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
            with os.fdopen(descriptor) as stream:
                info = os.fstat(stream.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_size > 8 * 1024 * 1024:
                    return {}
                value = json.load(stream)
                return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def remember(store, provider, session, home, cwd, *, worker=None, parent=None, host=None, created=False):
    nested = True
    try:
        nested = store.db.in_transaction
        with store.transaction():
            return _remember(store, provider, session, home, cwd, worker=worker, parent=parent,
                             host=host, created=created)
    except (sqlite3.Error, ValueError, OSError, RuntimeError) as error:
        if not nested:
            try:
                store.db.rollback()
            except sqlite3.Error:
                pass
        problems.record(store, 'cleanup', 'session-record-failed', type(error).__name__, every=300, worker=worker)
        return None


def _remember(store, provider, session, home, cwd, *, worker=None, parent=None, host=None, created=False):
    if provider not in PROVIDERS or not native_ids.valid(provider, session) or not home:
        return None
    if (worker is None) == (parent is None) or not Path(home).is_absolute():
        raise ValueError('Native session needs one Torii owner and an absolute home')
    if worker is not None and not store.db.execute('SELECT 1 FROM workers WHERE id=?', (worker,)).fetchone():
        return None
    owned = store.db.execute('SELECT 1 FROM native_sessions WHERE provider=? AND session=?',
                             (provider, session)).fetchone()
    if not created and not owned:
        return None
    home = str(Path(home).resolve())
    root = session_root(provider, home)
    existing = store.db.execute('''SELECT id FROM native_sessions WHERE provider=? AND session=?
        AND home=? AND worker IS ? AND parent IS ?''', (provider, session, home, worker, parent)).fetchone()
    if existing:
        if host:
            store.db.execute('INSERT OR IGNORE INTO native_session_hosts(native,host) VALUES (?,?)', (existing['id'], host))
        return existing['id']
    generation = 0
    if parent is not None:
        row = store.db.execute('SELECT MAX(generation) FROM native_sessions WHERE parent=? AND provider=? AND session=?',
                                (parent, provider, session)).fetchone()
        generation = row[0]
        if generation is None:
            generation = store.db.execute('SELECT COALESCE(MAX(generation),0)+1 FROM native_sessions WHERE parent=?',
                                           (parent,)).fetchone()[0]
    cursor = store.db.execute('''INSERT INTO native_sessions(provider,session,home,root,cwd,host,worker,parent,generation)
        VALUES (?,?,?,?,?,?,?,?,?)''', (provider, session, home, str(root), str(cwd), host, worker, parent, generation))
    if host:
        store.db.execute('INSERT INTO native_session_hosts(native,host) VALUES (?,?)', (cursor.lastrowid, host))
    return cursor.lastrowid


def launch_parent(session, spec):
    home = home_for(spec['provider'], spec.get('env') or {})
    if not home:
        return
    with session.store.db:
        session.store.put(session.key + '_native_launch', {
            'provider': spec['provider'], 'home': home, 'cwd': spec['cwd'], 'host': session.host_id,
            'created': spec.get('fresh', False) is True})


def confirm_parent(session):
    saved = session.store.get(session.key + '_native_launch') or {}
    if saved.get('host') != session.host_id or saved.get('provider') != session.provider:
        return
    remember(session.store, session.provider, session.session_id, saved['home'], saved['cwd'],
             parent=session.key, host=session.host_id, created=saved['created'])


@contextmanager
def directory(path):
    path = Path(path)
    if not path.is_absolute() or '..' in path.parts:
        raise ValueError('Session root must be an absolute anchored path')
    descriptor = os.open('/', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in path.parts[1:]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        yield descriptor
    finally:
        os.close(descriptor)


def session_root(provider, home):
    suffix = {'claude': 'projects', 'codex': 'sessions', 'antigravity': '.', 'opencode': 'data/opencode'}
    root = Path(home) / suffix[provider]
    return root.resolve() if provider == 'codex' else root


def file_entry(path, info):
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise ValueError('Session artifact is not a private regular file')
    return dict(path=str(path), device=info.st_dev, inode=info.st_ino, bytes=info.st_size, mtime=info.st_mtime)


def file_catalog(provider, root, archived=False):
    found = {}

    def walk(descriptor, prefix, depth):
        with os.scandir(descriptor) as entries:
            for entry in entries:
                if depth:
                    if not entry.is_dir(follow_symlinks=False):
                        continue
                    if provider == 'codex' and not re.fullmatch(r'\d{4}' if depth == 3 else r'\d{2}', entry.name):
                        continue
                    with child_directory(descriptor, entry.name) as child:
                        walk(child, prefix / entry.name, depth - 1)
                    continue
                if not entry.name.endswith('.jsonl') or not entry.is_file(follow_symlinks=False):
                    continue
                stem = entry.name[:-6]
                if provider == 'codex' and not stem.startswith('rollout-'):
                    continue
                sid = stem[-36:] if provider == 'codex' else stem
                if native_ids.valid(provider, sid):
                    found.setdefault(sid, []).append(str(prefix / entry.name))
    try:
        with directory(root) as descriptor:
            walk(descriptor, Path(), 1 if provider == 'claude' else 0 if archived else 3)
    except FileNotFoundError:
        pass
    return found


@contextmanager
def child_directory(parent, name):
    descriptor = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
    try:
        yield descriptor
    finally:
        os.close(descriptor)


def inspect_file(root, relative):
    path = Path(relative)
    if path.is_absolute() or not path.parts or '..' in path.parts:
        raise ValueError('Session path leaves its root')
    try:
        with directory(Path(root) / path.parent) as descriptor:
            info = os.stat(path.name, dir_fd=descriptor, follow_symlinks=False)
            return file_entry(path, info)
    except FileNotFoundError:
        return None


def session_files(record):
    provider, root, sid = record['provider'], record['root'], record['session']
    found = file_catalog(provider, root).get(sid, [])
    if len(found) > 1:
        raise ValueError('Native session has ambiguous file locations')
    return [entry for path in found if (entry := inspect_file(root, path)) is not None]


@contextmanager
def session_lease(store, record, *, read_only=False):
    provider, sid = record['provider'], record['session']
    if not native_ids.valid(provider, sid):
        raise ValueError('Invalid native session identity')
    with named_lease(store.directory, provider + '-' + sid, read_only=read_only):
        yield


@contextmanager
def named_lease(state_dir, name, *, read_only=False):
    root = state_dir / 'locks'
    if read_only and not root.exists():
        yield
        return
    if not read_only:
        root.mkdir(mode=0o700, exist_ok=True)
    with directory(root) as parent:
        try:
            flags = os.O_RDONLY if read_only else os.O_CREAT | os.O_RDWR
            descriptor = os.open(name + '.lock', flags | os.O_NOFOLLOW, 0o600, dir_fd=parent)
        except FileNotFoundError:
            if not read_only:
                raise
            yield
            return
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield
        finally:
            os.close(descriptor)


def trash_file(root, plan, trash):
    relative = Path(plan['path'])
    if relative.is_absolute() or not relative.parts or '..' in relative.parts:
        raise ValueError('Session path leaves its root')
    path = Path(root) / relative
    with directory(path.parent) as descriptor:
        info = os.stat(path.name, dir_fd=descriptor, follow_symlinks=False)
        if file_entry(relative, info) != {key: plan[key] for key in ('path', 'device', 'inode', 'bytes', 'mtime')}:
            raise ValueError('Session changed after inspection')
        target = host_os.move_to_trash(path, trash, name=plan['trash_name'], allow_files=True, source_fd=descriptor)
        os.fsync(descriptor)
        with directory(target.parent.resolve()) as trash_fd:
            os.fsync(trash_fd)


def database_entry(root, path, cutoff):
    entry = inspect_file(root, path)
    if entry is None or entry['mtime'] >= cutoff:
        return None
    for suffix in ('-wal', '-journal', '-shm'):
        sidecar = inspect_file(root, path + suffix)
        if sidecar and (sidecar['mtime'] >= cutoff or (suffix != '-shm' and sidecar['bytes'])):
            return None
    return entry


def opencode_sessions(root):
    with directory(root):
        source = Path(root) / 'opencode.db'
        with closing(sqlite3.connect(source.as_uri() + '?mode=ro&immutable=1', uri=True)) as db:
            return dict(db.execute('SELECT id,parent_id FROM session'))


def host_exited(path):
    from . import process_ownership
    from .worker_storage import alive
    if not os.path.lexists(path):
        return True
    if not stat.S_ISDIR(path.lstat().st_mode):
        return False
    owner = process_ownership.read_owner(path)
    if owner is not None:
        return owner.get('cleanup') in ('ok', 'stale')
    if (path / 'exit.json').is_file():
        return True
    return ((path / 'launch-failed.json').is_file()
            and all(not pid.exists() or not alive(pid.read_text().strip())
                    for pid in (path / 'pid', path / 'child.pid')))


class SessionScanError(Exception):
    """The safety probes could not establish which sessions to keep."""


def handoff_sessions(store, tasks):
    protected = set()
    for task in tasks:
        root = store.directory / 'results' / ('task-' + str(task['id']))
        try:
            with directory(root) as descriptor:
                fd = os.open('HANDOFF.md', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=descriptor)
                with os.fdopen(fd, 'rb') as stream:
                    info = os.fstat(stream.fileno())
                    if not stat.S_ISREG(info.st_mode) or info.st_size > 1024 * 1024:
                        raise ValueError('Cannot inspect job handoff')
                    data = stream.read(1024 * 1024 + 1)
                    if len(data) > 1024 * 1024:
                        raise ValueError('Cannot inspect job handoff')
                    protected.update(SID.findall(data.decode('utf-8', 'replace')))
        except FileNotFoundError:
            continue
    return protected


def backfill_records(store):
    accounts_by_provider = {provider: store.get(key, {}) for provider, key in
                            (('claude', 'accounts'), ('codex', 'codex_accounts'))}
    rows = store.db.execute('''SELECT w.id,w.provider,w.session,w.account_alias,w.cwd,w.host,
        CASE WHEN json_valid(w.result) THEN json_extract(w.result,'$.transcript_path') END AS transcript
        FROM workers w JOIN tasks t ON t.id=w.task
        WHERE t.status IN ('done','dropped') AND w.status IN ('done','interrupted')
        AND w.session IS NOT NULL AND CASE WHEN json_valid(w.result) THEN json_extract(w.result,'$.session_id') END=w.session
        AND NOT EXISTS (SELECT 1 FROM native_sessions n WHERE n.worker=w.id AND n.provider=w.provider AND n.session=w.session)
        ORDER BY w.id''').fetchall()
    for worker in rows:
        provider, sid = worker['provider'], worker['session']
        if provider not in PROVIDERS or not native_ids.valid(provider, sid):
            continue
        if provider == 'opencode':
            homes = [store.directory / 'opencode' / 'workers' / str(worker['id'])]
        elif provider == 'antigravity':
            homes = antigravity_worker_roots(worker)
        else:
            account = accounts_by_provider[provider].get(worker['account_alias'], {})
            homes = [Path(home) for home in (account.get('config_dir'), *account.get('previous_config_dirs', []))
                     if home and Path(home).is_absolute()]
            if provider == 'claude' and worker['transcript']:
                transcript = Path(worker['transcript'])
                if len(transcript.parents) > 2 and transcript.name == sid + '.jsonl' and transcript.parent.parent.name == 'projects':
                    homes = [home for home in homes if home.resolve() == transcript.parents[2].resolve()]
        for home in dict.fromkeys(homes):
            home = home.resolve()
            yield dict(id=None, worker=worker['id'], parent=None, generation=0, provider=provider, session=sid,
                       home=str(home), root=str(session_root(provider, home)), cwd=worker['cwd'], host=worker['host'])


def antigravity_worker_roots(worker):
    runtime = extension.optional('antigravity_runtime')
    alias = worker['account_alias']
    if runtime is None or not isinstance(alias, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]*', alias):
        return []
    base = Path.home() / '.local/share/agy'
    try:
        with directory(base / 'jobs' / alias) as descriptor, os.scandir(descriptor) as entries:
            worker_name = 'worker-' + str(worker['id'])
            folders = [base / 'jobs' / alias / entry.name for entry in entries
                       if (entry.name == worker_name or entry.name.startswith(worker_name + '.'))
                       and entry.is_dir(follow_symlinks=False)]
    except FileNotFoundError:
        folders = []
    return [runtime.account_sessions(base / 'accounts' / alias), *folders]


class SessionStorage:
    def __init__(self, store):
        self.store = store
        self.changed = asyncio.Event()
        self.changed.set()

    def protected(self, provider, sid):
        db = self.store.db
        return bool(db.execute('''SELECT 1 FROM workers w LEFT JOIN tasks t ON t.id=w.task
            WHERE w.provider=? AND w.session=? AND (w.status NOT IN ('done','interrupted') OR w.result IS NULL
            OR t.status IS NULL OR t.status NOT IN ('done','dropped')) LIMIT 1''', (provider, sid)).fetchone()
            or db.execute('''SELECT 1 FROM settings WHERE substr(key,-8)='_session' AND value=? LIMIT 1''',
                          (json.dumps(sid),)).fetchone()
            or db.execute('''SELECT 1 FROM native_sessions n WHERE provider=? AND session=?
                AND parent IS NOT NULL AND generation IN (SELECT DISTINCT generation FROM native_sessions p
                    WHERE p.parent=n.parent ORDER BY generation DESC LIMIT 2) LIMIT 1''', (provider, sid)).fetchone())

    def finished_workers(self, cutoff, worker=None):
        query = '''SELECT w.id FROM workers w JOIN tasks t ON t.id=w.task
            JOIN task_iterations i ON i.task=t.id AND i.k=w.iteration
            WHERE t.status IN ('done','dropped') AND i.outcome IN ('done','dropped')
            AND i.closed<? AND w.updated<? AND w.status IN ('done','interrupted') AND w.result IS NOT NULL
            AND EXISTS (SELECT 1 FROM messages m WHERE m.source_worker=w.id AND m.kind='worker_result')
            AND NOT EXISTS (SELECT 1 FROM workers other WHERE other.task=t.id AND other.iteration=w.iteration
                AND (other.status NOT IN ('done','interrupted') OR other.result IS NULL OR NOT EXISTS (
                    SELECT 1 FROM messages m WHERE m.source_worker=other.id AND m.kind='worker_result')))
            AND w.id NOT IN (SELECT handoff.last_worker FROM task_iterations handoff JOIN tasks active ON active.id=handoff.task
                WHERE handoff.last_worker IS NOT NULL AND active.status NOT IN ('done','dropped'))'''
        return self.store.db.execute(query + ' AND w.id=?', (cutoff, cutoff, worker)) if worker is not None else (
            self.store.db.execute(query, (cutoff, cutoff)))

    def eligible(self, record, cutoff):
        if self.protected(record['provider'], record['session']):
            return False
        return record['worker'] is None or self.finished_workers(cutoff, record['worker']).fetchone() is not None

    def protected_sessions(self):
        db = self.store.db
        current = {json.loads(row[0]) for row in db.execute('''SELECT value FROM settings
            WHERE substr(key,-8)='_session' AND json_type(value)='text' ''')}
        workers = {tuple(row) for row in db.execute('''SELECT w.provider,w.session FROM workers w LEFT JOIN tasks t ON t.id=w.task
            WHERE w.session IS NOT NULL AND (w.status NOT IN ('done','interrupted') OR w.result IS NULL
            OR t.status IS NULL OR t.status NOT IN ('done','dropped'))''')}
        parents = {tuple(row) for row in db.execute('''SELECT n.provider,n.session FROM native_sessions n
            WHERE n.parent IS NOT NULL AND n.generation IN (SELECT DISTINCT generation FROM native_sessions p
                WHERE p.parent=n.parent ORDER BY generation DESC LIMIT 2)''')}
        return current, workers | parents

    async def settled(self, record):
        from .worker_storage import storage_for as worker_storage_for
        store = self.store
        root = store.directory / 'hosts'
        names = {record['host']} if record['host'] else set()
        names.update(row[0] for row in store.db.execute('''SELECT h.host FROM native_session_hosts h
            JOIN native_sessions n ON n.id=h.native WHERE (n.provider=? AND n.session=?)
                OR (n.provider='opencode' AND n.root=?)''', (record['provider'], record['session'], record['root'])))
        if any(not isinstance(name, str) or Path(name).name != name or name == '..' for name in names):
            return False
        hosts = {root / name for name in names}
        if record['worker'] is not None:
            worker = store.db.execute('SELECT id,host FROM workers WHERE id=?', (record['worker'],)).fetchone()
            if worker is None:
                return False
            hosts.update(await worker_storage_for(store).host_paths(worker))
        if record['provider'] == 'opencode':
            for row in store.db.execute("SELECT key,value FROM settings WHERE substr(key,-14)='_native_launch'"):
                saved = json.loads(row['value'])
                if isinstance(saved, dict) and saved.get('provider') == 'opencode' and saved.get('home') == record['home']:
                    active = store.get(row['key'][:-14] + '_host') or {}
                    if saved.get('host') and active.get('id') == saved['host']:
                        return False
        return await asyncio.to_thread(lambda: all(path.parent == root and host_exited(path) for path in hosts))

    async def candidates(self, cutoff):
        from . import opencode_cleanup
        store = self.store
        tasks = [dict(row) for row in store.db.execute(
            "SELECT id,worktree FROM tasks WHERE status NOT IN ('done','dropped')")]
        try:
            handoffs = await asyncio.to_thread(handoff_sessions, store, tasks)
            process_paths = set(await asyncio.to_thread(stray_report.process_paths))
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            raise SessionScanError('Could not inspect job handoffs or open files') from error
        trash = host_os.trash_directory()
        ancestor = trash
        while not ancestor.exists() and ancestor != ancestor.parent:
            ancestor = ancestor.parent
        trash_device = ancestor.stat().st_dev
        catalog = {}
        seen = set()
        links = None
        finished = {row[0] for row in self.finished_workers(cutoff)}
        current, protected = self.protected_sessions()
        pending = {tuple(row) for row in store.db.execute(
            "SELECT provider,session FROM session_deletions WHERE state='trash_pending'")}
        rows = [dict(row) for row in store.db.execute('SELECT * FROM native_sessions')]
        rows.extend(backfill_records(store))
        for record in rows:
            provider, sid = record['provider'], record['session']
            if (sid in current or sid in handoffs or (provider, sid) in protected or (provider, sid) in pending
                    or record['worker'] is not None and record['worker'] not in finished):
                continue
            try:
                if provider in ('claude', 'codex'):
                    roots = [(record['root'], False)]
                    if provider == 'codex':
                        roots.append((str((Path(record['home']) / 'archived_sessions').resolve()), True))
                    matches = []
                    for root, archived in roots:
                        key = (provider, root, archived)
                        if key not in catalog:
                            catalog[key] = await asyncio.to_thread(file_catalog, provider, root, archived)
                        matches.extend((root, path) for path in catalog[key].get(sid, []))
                    if len(matches) != 1:
                        continue
                    root, path = matches[0]
                    entry = await asyncio.to_thread(inspect_file, root, path)
                elif provider in ('opencode', 'antigravity'):
                    root = record['root']
                    path = 'opencode.db' if provider == 'opencode' else 'conversations/' + sid + '.db'
                    entry = await asyncio.to_thread(database_entry, root, path, cutoff)
                else:
                    continue
                if (entry is None or entry['mtime'] >= cutoff or entry['device'] != trash_device
                        or Path(root) / path in process_paths or (root, path) in seen
                        or store.db.execute("SELECT 1 FROM session_deletions WHERE root=? AND path=? AND provider=? AND session=? AND state IN ('trashed','deleted','refused')",
                                            (root, path, provider, sid)).fetchone()):
                    continue
                if not await self.settled(record):
                    continue
                if provider == 'opencode':
                    if links is None:
                        links = await asyncio.to_thread(opencode_cleanup.live_links, store.directory, tasks)
                    if any(Path(root).is_relative_to(link) or link.is_relative_to(Path(root)) for link in links):
                        continue
                    sessions = await asyncio.to_thread(opencode_sessions, root)
                    if not self.opencode_owned(record, sessions, handoffs):
                        continue
                    entry['sessions'] = sessions
                seen.add((root, path))
                yield record, dict(entry, root=root)
            except (OSError, ValueError, sqlite3.Error) as error:
                yield record, error

    def opencode_owned(self, record, sessions, handoffs):
        sid = record['session']
        if record['worker'] is None:
            return set(sessions) == {sid}
        home = self.store.directory / 'opencode' / 'workers' / str(record['worker'])
        if home != Path(record['home']) or sessions.get(sid, sid) is not None:
            return False
        reached = {sid}
        while len(reached) < len(sessions):
            more = {child for child, parent in sessions.items() if parent in reached} - reached
            if not more or any(child in handoffs or self.protected('opencode', child) for child in more):
                return False
            reached.update(more)
        return True

    def trash_name(self, plan):
        install = hashlib.sha256(os.fsencode(self.store.directory)).hexdigest()[:12]
        return f"{TRASH_PREFIX}{install}-{plan['id']}-{Path(plan['path']).name}"

    def recover(self, trash):
        store = self.store
        with store.transaction():
            store.db.execute("DELETE FROM session_deletions WHERE state='pending'")
            for plan in store.db.execute("SELECT * FROM session_deletions WHERE state='trash_pending'").fetchall():
                name = self.trash_name(plan)
                for path in trash.glob(name + '*'):
                    info = path.lstat()
                    if (info.st_dev, info.st_ino) == (plan['device'], plan['inode']):
                        store.db.execute("UPDATE session_deletions SET state='trashed' WHERE id=?", (plan['id'],))
                        break
                else:
                    try:
                        source = inspect_file(plan['root'], plan['path'])
                    except (OSError, ValueError):
                        continue
                    if source and (source['device'], source['inode']) == (plan['device'], plan['inode']):
                        store.db.execute('DELETE FROM session_deletions WHERE id=?', (plan['id'],))

    async def move(self, record, entry, cutoff, trash):
        from . import opencode_cleanup
        store = self.store
        provider = record['provider']
        original = {key: entry[key] for key in ('path', 'device', 'inode', 'bytes', 'mtime')}
        worker_database = provider == 'opencode' and record['worker'] is not None
        home_lease = opencode_cleanup.worker_home_lease(store.directory, record['worker']) if worker_database else nullcontext()
        with session_lease(store, record), home_lease:
            if worker_database:
                if database_entry(entry['root'], entry['path'], cutoff) != original:
                    return
                target = store.directory / 'opencode' / 'usage' / (str(record['worker']) + '.jsonl')
                await asyncio.to_thread(opencode_cleanup.save_usage, Path(record['home']), target, store.directory, immutable=True)
            with store.transaction():
                if store.get('session_delete', False) is not True or not self.eligible(record, cutoff):
                    return
                if provider in ('opencode', 'antigravity'):
                    if database_entry(entry['root'], entry['path'], cutoff) != original:
                        return
                    if provider == 'opencode' and not self.opencode_owned(record, entry['sessions'], set()):
                        return
                elif inspect_file(entry['root'], entry['path']) != original:
                    return
                native = record['id']
                if native is None:
                    native = _remember(store, record['provider'], record['session'], record['home'], record['cwd'],
                                       worker=record['worker'], host=record['host'], created=True)
                plan = store.db.execute('''INSERT OR IGNORE INTO session_deletions
                    (native,provider,session,home,root,path,device,inode,bytes,state) VALUES (?,?,?,?,?,?,?,?,?,'trash_pending')''',
                    (native, provider, record['session'], record['home'], entry['root'],
                     entry['path'], entry['device'], entry['inode'], entry['bytes']))
            if not plan.rowcount:
                return
            plan_id = plan.lastrowid
            try:
                with store.transaction():
                    if store.get('session_delete', False) is not True or not self.eligible(record, cutoff):
                        store.db.execute('DELETE FROM session_deletions WHERE id=?', (plan_id,))
                        return
                    trash_file(entry['root'], dict(entry, trash_name=self.trash_name(dict(id=plan_id, **entry))), trash)
                    store.db.execute("UPDATE session_deletions SET state='trashed' WHERE id=?", (plan_id,))
            except (OSError, ValueError):
                source = inspect_file(entry['root'], entry['path'])
                if source and (source['device'], source['inode']) == (entry['device'], entry['inode']):
                    with store.transaction():
                        store.db.execute('DELETE FROM session_deletions WHERE id=?', (plan_id,))
                raise

    async def sweep(self):
        store = self.store
        if store.get('session_delete', False) is not True:
            return
        cutoff = time.time() - RETENTION
        trash = host_os.trash_directory()
        self.recover(trash)
        try:
            await self.sweep_candidates(cutoff, trash)
        except SessionScanError:
            return

    async def sweep_candidates(self, cutoff, trash):
        """Check and move each session in one step, so nothing can reopen it between the checks and the move.
        A failure goes to the `session-trash` sweep state; a busy session lease is skipped."""
        store = self.store
        async for record, entry in self.candidates(cutoff):
            if store.get('session_delete', False) is not True:
                return
            key = '%s:%s' % (record['provider'], record['session'])

            async def trash_session(record):
                if isinstance(entry, Exception):
                    raise entry
                try:
                    await self.move(record, entry, cutoff, trash)
                except BlockingIOError:
                    return
            await sweeps.arun(store, 'session-trash', [record], trash_session, key=lambda record: key,
                              refs=lambda record: {'worker': record['worker']})
            await asyncio.sleep(0)

    async def run(self):
        store = self.store
        while True:
            self.changed.clear()
            if store.get('session_delete', False) is not True:
                await self.changed.wait()
                continue
            delay = store.get('session_cleanup_last_sweep', 0) + DAY - time.time()
            if delay <= 0:
                await self.sweep()
                if store.get('session_delete', False) is True:
                    with store.transaction():
                        store.put('session_cleanup_last_sweep', time.time())
                continue
            try:
                await asyncio.wait_for(self.changed.wait(), delay)
            except asyncio.TimeoutError:
                pass


def storage_for(store):
    if not hasattr(store, '_session_storage'):
        store._session_storage = SessionStorage(store)
    return store._session_storage
