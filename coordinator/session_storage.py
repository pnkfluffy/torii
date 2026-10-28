"""Durable, opt-in reclamation of native sessions created by Torii."""

import asyncio
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import time

from . import accounts, codex_accounts, native_ids, problems


BATCH = 16
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
    if store.get('session_delete', False) is not True:
        return
    if not store.db.execute("SELECT 1 FROM service_requests WHERE op=? AND state='queued'", (REQUEST,)).fetchone():
        now = time.time()
        store.db.execute('INSERT INTO service_requests(op,params,created,updated) VALUES (?,\'{}\',?,?)',
                         (REQUEST, now, now))


def home_for(provider, env):
    if provider == 'claude':
        return accounts.claude_home_for_environment(env)
    if provider == 'codex':
        return codex_accounts.codex_home_for_environment(env)
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
    if provider not in ('claude', 'codex', 'opencode') or not native_ids.valid(provider, session) or not home:
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
    suffix = {'claude': 'projects', 'codex': 'sessions', 'opencode': 'data/opencode'}[provider]
    root = Path(home) / suffix
    if provider == 'codex':
        root = root.resolve()
    existing = store.db.execute('''SELECT id FROM native_sessions WHERE provider=? AND session=?
        AND home=? AND worker IS ? AND parent IS ?''', (provider, session, home, worker, parent)).fetchone()
    if existing:
        if host:
            store.db.execute('INSERT OR IGNORE INTO native_session_hosts(native,host) VALUES (?,?)', (existing['id'], host))
        request(store)
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
    request(store)
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


def session_files(record):
    root = Path(record['root'])
    if record['provider'] == 'opencode':
        return [{'path': record['session'], 'device': None, 'inode': None, 'bytes': 0}]
    found = []

    def walk(descriptor, prefix, depth):
        with os.scandir(descriptor) as entries:
            for entry in entries:
                if depth:
                    if not entry.is_dir(follow_symlinks=False):
                        continue
                    if record['provider'] == 'codex' and not re.fullmatch(r'\d{4}' if depth == 3 else r'\d{2}', entry.name):
                        continue
                    child = os.open(entry.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
                    try:
                        walk(child, prefix + [entry.name], depth - 1)
                    finally:
                        os.close(child)
                elif (entry.name == record['session'] + '.jsonl' if record['provider'] == 'claude' else
                      entry.name.startswith('rollout-') and entry.name.endswith('-' + record['session'] + '.jsonl')):
                    info = entry.stat(follow_symlinks=False)
                    if stat.S_ISREG(info.st_mode):
                        found.append({'path': '/'.join(prefix + [entry.name]), 'device': info.st_dev,
                                      'inode': info.st_ino, 'bytes': info.st_size})
    try:
        with directory(root) as descriptor:
            walk(descriptor, [], 1 if record['provider'] == 'claude' else 3)
    except FileNotFoundError:
        pass
    if len(found) > 1:
        raise ValueError('Native session has ambiguous file locations')
    return found


def remove_file(root, plan):
    relative = Path(plan['path'])
    if relative.is_absolute() or not relative.parts or any(part in ('.', '..') for part in relative.parts):
        raise ValueError('Session deletion path leaves its session root')
    try:
        with directory(Path(root) / relative.parent) as descriptor:
            try:
                info = os.stat(relative.name, dir_fd=descriptor, follow_symlinks=False)
            except FileNotFoundError:
                return
            if not stat.S_ISREG(info.st_mode) or (info.st_dev, info.st_ino) != (plan['device'], plan['inode']):
                raise ValueError('Session deletion path has a different owner')
            os.unlink(relative.name, dir_fd=descriptor)
            os.fsync(descriptor)
    except FileNotFoundError:
        return


@contextmanager
def session_lease(store, plan):
    if not native_ids.valid(plan['provider'], plan['session']):
        raise ValueError('Invalid native session deletion identity')
    root = store.directory / 'locks'
    root.mkdir(mode=0o700, exist_ok=True)
    with directory(root) as parent:
        descriptor = os.open(plan['provider'] + '-' + plan['session'] + '.lock',
                             os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600, dir_fd=parent)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield
        finally:
            os.close(descriptor)


def opencode_ids(plan):
    path = Path(plan['root']) / 'opencode.db'
    try:
        info = path.lstat()
    except FileNotFoundError:
        return set()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError('OpenCode session database is not a regular file')
    with directory(plan['root']):
        db = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)
        try:
            rows = db.execute('''WITH RECURSIVE descendants(id) AS (
                SELECT id FROM session WHERE id=? UNION SELECT s.id FROM session s
                JOIN descendants d ON s.parent_id=d.id) SELECT id FROM descendants''', (plan['session'],)).fetchall()
            return {row[0] for row in rows}
        finally:
            db.close()


async def delete_opencode(store, plan):
    from . import opencode_runtime
    from .worker_storage import _remove_tree
    cleanup = store.directory / 'session-cleanup' / str(plan['id'])
    try:
        home = Path(plan['home'])
        root = home / 'data/opencode'
        if str(root) != plan['root']:
            raise ValueError('OpenCode data root changed')
        descendants = await asyncio.to_thread(opencode_ids, plan)
        if not descendants:
            return
        if descendants - {plan['session']}:
            raise ValueError('OpenCode deletion would include sessions with unknown ownership')
        binary = store.get('opencode_binary')
        if not binary or not Path(binary).is_absolute():
            raise OSError('Native OpenCode cleanup needs the configured binary')
        with directory(root):
            env = opencode_runtime.environment(cleanup, {'PATH': os.defpath})
            env['XDG_DATA_HOME'] = str(home / 'data')
            process = await asyncio.create_subprocess_exec(binary, 'session', 'delete', plan['session'],
                cwd=str(cleanup), env=env, stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            try:
                output, error = await asyncio.wait_for(process.communicate(), 30)
            except BaseException:
                if process.returncode is None:
                    process.kill()
                await process.wait()
                raise
            if process.returncode and ('Session not found: ' + plan['session']).encode() not in output + error:
                raise OSError('Native OpenCode session deletion failed')
            if await asyncio.to_thread(opencode_ids, plan):
                raise OSError('Native OpenCode session still exists after deletion')
    finally:
        await asyncio.to_thread(_remove_tree, cleanup)


class SessionStorage:
    def __init__(self, store):
        self.store = store
        self.changed = asyncio.Event()
        self.changed.set()

    def eligible(self, record):
        store = self.store
        if store.get('session_delete', False) is not True:
            return False
        if record['worker'] is not None:
            worker = store.db.execute('SELECT * FROM workers WHERE id=?', (record['worker'],)).fetchone()
            if worker is None or worker['task'] is None:
                return False
            job = store.task_get(worker['task'])
            if not job or job['status'] not in ('done', 'dropped'):
                return False
            closed = store.db.execute('''SELECT 1 FROM task_iterations WHERE task=? AND k=?
                AND closed IS NOT NULL AND outcome IN ('done','dropped')''', (worker['task'], worker['iteration'])).fetchone()
            unfinished = store.db.execute('''SELECT 1 FROM workers w WHERE task=? AND iteration=? AND
                (status NOT IN ('done','interrupted') OR result IS NULL OR NOT EXISTS (
                    SELECT 1 FROM messages m WHERE m.source_worker=w.id AND m.kind='worker_result')) LIMIT 1''',
                (worker['task'], worker['iteration'])).fetchone()
            if not closed or unfinished:
                return False
        else:
            generations = store.db.execute('SELECT DISTINCT generation FROM native_sessions WHERE parent=? ORDER BY generation DESC LIMIT 2',
                                            (record['parent'],)).fetchall()
            if not generations or record['generation'] >= min(row[0] for row in generations):
                return False
        active = store.db.execute('''SELECT 1 FROM workers w LEFT JOIN tasks t ON t.id=w.task WHERE
            w.provider=? AND w.session=? AND (w.status NOT IN ('done','interrupted') OR w.result IS NULL
            OR t.status NOT IN ('done','dropped')) LIMIT 1''', (record['provider'], record['session'])).fetchone()
        current = store.db.execute('''SELECT 1 FROM settings WHERE substr(key,-8)='_session' AND value=? LIMIT 1''',
                                    (json.dumps(record['session']),)).fetchone()
        retained_parent = store.db.execute('''SELECT 1 FROM native_sessions n WHERE provider=? AND session=?
            AND parent IS NOT NULL AND generation IN (SELECT DISTINCT generation FROM native_sessions p
                WHERE p.parent=n.parent ORDER BY generation DESC LIMIT 2) LIMIT 1''',
            (record['provider'], record['session'])).fetchone()
        return not active and not current and not retained_parent

    async def settled(self, record):
        from .worker_storage import processes_gone, storage_for as worker_storage_for
        names = {record['host']} if record['host'] else set()
        names.update(row['host'] for row in self.store.db.execute(
            'SELECT host FROM native_session_hosts WHERE native=?', (record['id'],)))
        if record['provider'] == 'opencode':
            names.update(row['host'] for row in self.store.db.execute(
                "SELECT host FROM native_sessions WHERE provider='opencode' AND root=? AND host IS NOT NULL",
                (record['root'],)))
            names.update(row['host'] for row in self.store.db.execute(
                """SELECT h.host FROM native_session_hosts h JOIN native_sessions n ON n.id=h.native
                    WHERE n.provider='opencode' AND n.root=?""", (record['root'],)))
            for row in self.store.db.execute("SELECT key,value FROM settings WHERE substr(key,-14)='_native_launch'"):
                saved = json.loads(row['value'])
                if (isinstance(saved, dict) and saved.get('provider') == 'opencode'
                        and saved.get('home') == record['home'] and saved.get('host')):
                    active = self.store.get(row['key'][:-14] + '_host') or {}
                    if active.get('id') == saved['host']:
                        return False
                    names.add(saved['host'])
        if any(not isinstance(name, str) or Path(name).name != name for name in names):
            return False
        hosts = {self.store.directory / 'hosts' / name for name in names}
        if record['worker'] is not None:
            worker = dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (record['worker'],)).fetchone())
            hosts.update(await worker_storage_for(self.store).host_paths(worker))
        for path in hosts:
            if path.parent != self.store.directory / 'hosts':
                return False
            if not await asyncio.to_thread(processes_gone, path):
                return False
        return True

    async def backlog(self):
        from .worker_storage import storage_for as worker_storage_for
        store = self.store
        cursor = store.get('session_cleanup_cursor', 0)
        rows = store.db.execute('SELECT * FROM workers WHERE id>? ORDER BY id LIMIT ?', (cursor, BATCH)).fetchall()
        for worker in rows:
            if worker['session']:
                hosts = await worker_storage_for(store).host_paths(dict(worker))
                found = False
                for path in sorted(hosts):
                    if path.parent != store.directory / 'hosts':
                        continue
                    spec = await asyncio.to_thread(launch_spec, path / 'spec.json')
                    if spec.get('provider') != worker['provider'] or spec.get('fresh') is not True:
                        continue
                    home = home_for(worker['provider'], spec.get('env') or {})
                    if home:
                        with store.db:
                            remember(store, worker['provider'], worker['session'], home, spec.get('cwd') or worker['cwd'],
                                     worker=worker['id'], host=path.name, created=True)
                        found = True
                if not found and worker['account_alias']:
                    try:
                        result = json.loads(worker['result'] or '{}')
                    except ValueError:
                        result = {}
                    setting = {'claude': 'accounts', 'codex': 'codex_accounts'}.get(worker['provider'])
                    account = store.get(setting, {}).get(worker['account_alias'], {}) if setting else {}
                    homes = [account.get('config_dir'), *account.get('previous_config_dirs', [])]
                    transcript = result.get('transcript_path')
                    if (worker['provider'] == 'claude' and transcript and Path(transcript).is_absolute()
                            and Path(transcript).name == worker['session'] + '.jsonl'
                            and Path(transcript).parent.parent.name == 'projects'):
                        homes = [str(Path(transcript).parents[2])]
                    elif worker['provider'] == 'opencode':
                        homes = [str(store.directory / 'opencode/workers' / str(worker['id']))]
                    if result.get('session_id') == worker['session']:
                        for home in homes:
                            if home:
                                with store.db:
                                    remember(store, worker['provider'], worker['session'], home, worker['cwd'],
                                             worker=worker['id'], created=True)
            with store.db:
                store.put('session_cleanup_cursor', worker['id'])
        return len(rows) == BATCH

    def home_failed(self, record, error):
        store = self.store
        row = store.db.execute('SELECT attempts FROM session_cleanup_homes WHERE provider=? AND root=?',
                               (record['provider'], record['root'])).fetchone()
        attempts = row['attempts'] + 1 if row else 1
        retry_at = time.time() + min(86400, 300 * 2 ** min(attempts - 1, 9))
        with store.db:
            store.db.execute("""INSERT INTO session_cleanup_homes(provider,root,attempts) VALUES (?,?,?)
                ON CONFLICT(provider,root) DO UPDATE SET attempts=excluded.attempts""",
                (record['provider'], record['root'], attempts))
            store.db.execute('UPDATE native_sessions SET retry_at=? WHERE provider=? AND root=? AND planned=0',
                             (retry_at, record['provider'], record['root']))
            store.db.execute("""UPDATE session_deletions SET retry_at=MAX(retry_at,?)
                WHERE provider=? AND root=? AND state='pending'""", (retry_at, record['provider'], record['root']))
            identity = hashlib.sha256((record['provider'] + ':' + record['root']).encode()).hexdigest()[:16]
            problems.record(store, 'cleanup', 'session-cleanup-home-' + identity, type(error).__name__, every=3600)

    async def sweep(self):
        store = self.store
        if store.get('session_delete', False) is not True:
            return False
        more = await self.backlog()
        cursor = store.get('session_cleanup_scan', 0)
        rows = store.db.execute('''SELECT * FROM native_sessions WHERE id>? AND planned=0 AND retry_at<=?
            ORDER BY id LIMIT ?''', (cursor, time.time(), BATCH)).fetchall()
        for raw in rows:
            record = dict(raw)
            cursor = record['id']
            if store.db.execute('SELECT retry_at FROM native_sessions WHERE id=?', (record['id'],)).fetchone()[0] > time.time():
                continue
            eligible = self.eligible(record)
            settled = await self.settled(record) if eligible else False
            if eligible and not settled:
                with store.db:
                    store.db.execute('UPDATE native_sessions SET retry_at=? WHERE id=?', (time.time() + 300, record['id']))
            if eligible and settled:
                try:
                    files = await asyncio.to_thread(session_files, record)
                    with store.db:
                        for entry in files:
                            store.db.execute('''INSERT OR IGNORE INTO session_deletions
                                (native,provider,session,home,root,path,device,inode,bytes) VALUES (?,?,?,?,?,?,?,?,?)''',
                                (record['id'], record['provider'], record['session'], record['home'], record['root'],
                                 entry['path'], entry['device'], entry['inode'], entry['bytes']))
                        store.db.execute('UPDATE native_sessions SET planned=1 WHERE id=?', (record['id'],))
                        store.db.execute('DELETE FROM session_cleanup_homes WHERE provider=? AND root=?',
                                         (record['provider'], record['root']))
                except (OSError, ValueError) as error:
                    self.home_failed(record, error)
        with store.db:
            store.put('session_cleanup_scan', cursor if len(rows) == BATCH else 0)
        plans = store.db.execute('''SELECT * FROM session_deletions WHERE state='pending' AND retry_at<=?
            ORDER BY id LIMIT ?''', (time.time(), BATCH)).fetchall()
        for raw in plans:
            plan = dict(raw)
            if store.db.execute('SELECT retry_at FROM session_deletions WHERE id=?', (plan['id'],)).fetchone()[0] > time.time():
                continue
            record = store.db.execute('SELECT * FROM native_sessions WHERE id=?', (plan['native'],)).fetchone()
            if not record or not self.eligible(record) or not await self.settled(record) or not self.eligible(record):
                with store.db:
                    store.db.execute('UPDATE session_deletions SET retry_at=? WHERE id=?', (time.time() + 300, plan['id']))
                continue
            try:
                with session_lease(store, plan):
                    if plan['provider'] == 'opencode':
                        await delete_opencode(store, plan)
                    else:
                        remove_file(plan['root'], plan)
                with store.db:
                    store.db.execute("UPDATE session_deletions SET state='deleted',error=NULL WHERE id=?", (plan['id'],))
            except ValueError as error:
                with store.db:
                    store.db.execute("UPDATE session_deletions SET state='refused',error=? WHERE id=?",
                                     (str(error), plan['id']))
                    problems.record(store, 'cleanup', 'session-cleanup-refused', str(error))
            except (OSError, RuntimeError, sqlite3.Error, asyncio.TimeoutError) as error:
                with store.db:
                    store.db.execute('''UPDATE session_deletions SET attempts=attempts+1,retry_at=?,error=? WHERE id=?''',
                                     (time.time() + 300, type(error).__name__, plan['id']))
                self.home_failed(plan, error)
            await asyncio.sleep(0)
        return more or len(rows) == BATCH or len(plans) == BATCH

    async def run(self):
        while True:
            if self.store.get('session_delete', False) is True:
                delay = self.store.db.execute('''SELECT MIN(retry_at) FROM (
                    SELECT retry_at FROM session_deletions WHERE state='pending' AND retry_at>0
                    UNION ALL SELECT retry_at FROM native_sessions WHERE planned=0 AND retry_at>0)''').fetchone()[0]
            else:
                delay = None
            try:
                if delay is None:
                    await self.changed.wait()
                else:
                    await asyncio.wait_for(self.changed.wait(), max(0.2, delay - time.time()))
            except asyncio.TimeoutError:
                pass
            self.changed.clear()
            while await self.sweep():
                await asyncio.sleep(0.2)


def storage_for(store):
    if not hasattr(store, '_session_storage'):
        store._session_storage = SessionStorage(store)
    return store._session_storage
