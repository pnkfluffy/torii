"""Job records and comparison with the live index."""

import json
import hashlib
import fcntl
import logging
import os
import re
from pathlib import Path
import shutil
import sqlite3
import stat
import tempfile
import time
import math
import asyncio
import sys
import uuid
from collections import Counter
from datetime import datetime, timezone
from urllib.parse import urlsplit

from .control_api import Refused, Result, author, job_number, results_size
from .failures import internal_detail
from . import problems, sweeps


logger = logging.getLogger(__name__)
FIELDS = ('id', 'uid', 'chat', 'number', 'title', 'status', 'finish', 'created', 'closed',
          'parent', 'blocked_by', 'discovered_from', 'related', 'replaces', 'follow_ups', 'pr', 'refs', 'log', 'workers',
          'links', 'stage', 'iterations', 'results_size')
LISTS = {'blocked_by', 'discovered_from', 'related', 'replaces', 'follow_ups', 'refs', 'links', 'iterations'}
ITERATION = ('k', 'opened', 'closed', 'opened_by', 'outcome', 'reason')
INFORMATIONAL = {'workers', 'follow_ups', 'links', 'results_size'}
CLOSED = ('done', 'dropped', 'expired')
REFS = 20
SUMMARY_LIMIT = 4000
OUTCOME_LIMIT = 4000
SINGLE = ('chats.yaml', 'ledger.json', 'settings.json', 'rules.json')
JOURNAL_SPEC = ':(glob)chat-*/[0-9]*.jsonl'
TRACKED = (*SINGLE, ':(glob)chat-*/[0-9]*.md', JOURNAL_SPEC, ':(glob)rules/*.md', ':(glob)rules/tags/*.md')
INSTALL = re.compile(r'[a-z][a-z0-9-]{1,30}')
BULK = 200
CHUNK = 1000
SYNC_LIMIT = 600
GC_AUTO = 1000
ZERO = '0' * 40


PATTERNS = (
    ('private_key', re.compile(r'-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?(?:-----END [A-Z ]*PRIVATE KEY-----|\Z)')),
    ('sk', re.compile(r'\bsk-[A-Za-z0-9_-]{16,}')),
    ('github', re.compile(r'\b(?:ghp_|gho_|ghs_|github_pat_)[A-Za-z0-9_]{16,}')),
    ('slack', re.compile(r'\bxox[abp]-[A-Za-z0-9-]{16,}')),
    ('aws', re.compile(r'\bAKIA[0-9A-Z]{16}\b')),
    ('google', re.compile(r'\bAIza[A-Za-z0-9_-]{20,}')),
    ('gitlab', re.compile(r'\bglpat-[A-Za-z0-9_-]{16,}')),
    ('tailscale', re.compile(r'\btskey-[A-Za-z0-9_-]{16,}')),
    ('npm', re.compile(r'\bnpm_[A-Za-z0-9_-]{16,}')),
    ('jwt', re.compile(r'\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}')),
    ('bearer', re.compile(r'(?i)\bbearer[ \t]+[A-Za-z0-9_./+\-=]{20,}')),
    ('telegram', re.compile(r'(?<!\d)\d{5,}:[A-Za-z0-9_-]{30,}')),
)
ASSIGNMENT = re.compile(r'''(?i)\b([\w-]{0,64}(?:token|secret|password|passwd|api[_-]?key|credential)[\w-]{0,64}["']?[ \t]*[:=][ \t]*["']?)([^\s"'<>,;]+)''')
RULE_NAME_ASSIGNMENT = re.compile(r'\b[A-Za-z_][A-Za-z0-9_-]*[ \t]*[:=][ \t]*[A-Z][A-Z0-9_]{1,63}(?:=[A-Z][A-Z0-9_]{1,63})?(?:,[A-Z][A-Z0-9_]{1,63}(?:=[A-Z][A-Z0-9_]{1,63})?)*(?=$|[\s`"\'<>;]|[)\].,:;!?]+(?:$|[\s`"\'<>]))')


def entropy(value):
    counts = Counter(value)
    return -sum((n / len(value)) * math.log2(n / len(value)) for n in counts.values())


def opaque(value):
    if len(value) < 20 or value.startswith('[redacted]'):
        return False
    return entropy(value) >= 3 or (any(c.islower() for c in value) and any(c.isupper() for c in value)
                            and any(c.isdigit() for c in value))


def opaque_bearer(value):
    return opaque(value) and (not value.isalpha() or not (value.islower() or value.isupper()) or entropy(value) >= 4)


OUTBOUND_OVERRIDES = {
    'sk': re.compile(r'\bsk-[A-Za-z0-9_-]{20,}'),
    'github': re.compile(r'\b(?:gh[pousr]_[A-Za-z0-9_]{36,}|github_pat_[A-Za-z0-9_]{16,})'),
    'npm': re.compile(r'\bnpm_[A-Za-z0-9]{36,}'),
}
OUTBOUND_PATTERNS = tuple((label, OUTBOUND_OVERRIDES.get(label, pattern)) for label, pattern in PATTERNS)


def redact(text, counts=None, replacement='[redacted]', assignments=True, outbound=False):
    found = Counter()
    for label, pattern in OUTBOUND_PATTERNS if outbound else PATTERNS:
        def replace(match):
            if label == 'bearer' and not opaque_bearer(match.group(0).split()[-1]):
                return match.group(0)
            found[label] += 1
            return replacement
        text = pattern.sub(replace, text)
    def assignment(match):
        value = match.group(2)
        if not opaque(value):
            return match.group(0)
        found['assignment'] += 1
        return match.group(1) + replacement
    if assignments:
        text = ASSIGNMENT.sub(assignment, text)
    if counts is not None:
        counts.update(found)
    return text, sum(found.values())


def redact_data(value):
    if isinstance(value, str):
        return redact(value)[0]
    if isinstance(value, list):
        return [redact_data(item) for item in value]
    if isinstance(value, dict):
        return {key: redact_data(item) for key, item in value.items()}
    return value


def slug(topic):
    return 'chat-' + hashlib.sha256(topic.encode()).hexdigest()[:16]


def path(directory, task):
    return Path(directory) / 'records' / slug(task['topic']) / ('%04d.md' % task['number'])


NOTE_LIMIT = 2000
JOURNAL_LIMIT = 1_000_000
JOURNAL_PART = re.compile(r'[0-9]+(?:\.([1-9][0-9]*))?\.jsonl')
KINDS = ('note', 'result', 'report')


def journal_path(directory, task):
    return path(directory, task).with_suffix('.jsonl')


def journal_part_number(part):
    match = JOURNAL_PART.fullmatch(part.name)
    return int(match[1] or 0) if match else None


def journal_part_path(target, number):
    return target.with_name(f'{target.stem}.{number}.jsonl') if number else target


def journal_parts(target):
    if journal_part_number(target):
        target = target.with_name(target.name.partition('.')[0] + '.jsonl')
    parts = sorted((part for part in target.parent.glob(target.stem + '.*.jsonl') if journal_part_number(part)),
                   key=journal_part_number)
    return [target, *parts] if target.exists() else parts


def clip(text, limit=NOTE_LIMIT):
    return text if len(text) <= limit else text[:limit] + '…[+%d chars]' % (len(text) - limit)


def scrub(store, text, known=None):
    from .store import _known_token_scrubber
    text, count = redact(text)
    clean = (known or _known_token_scrubber(store.vault))(text)
    return clean, count + clean.count('[token]') - text.count('[token]')


def write_lock(directory):
    """Hold `records-write.lock` while the file is open. Journal appends and the recovery merge take it."""
    stream = (Path(directory) / 'records-write.lock').open('a')
    fcntl.flock(stream, fcntl.LOCK_EX)
    return stream


def journal_append(store, task, entries, limit=JOURNAL_LIMIT):
    """Keep whole entries in numbered parts under the write lock. An oversized entry gets its own part."""
    target = journal_path(store.directory, task)
    lines = [(json.dumps(entry, ensure_ascii=False) + '\n').encode() for entry in entries]
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with write_lock(store.directory):
        return append_journal_parts(target, lines, limit)


def append_journal_parts(target, lines, limit):
    parts = journal_parts(target)
    part = parts[-1] if parts else target
    start = 0
    while start < len(lines):
        descriptor = os.open(part, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o600)
        try:
            size = os.fstat(descriptor).st_size
            separator = b'\n' if size and os.pread(descriptor, 1, size - 1) != b'\n' else b''
            size += len(separator)
            end = start
            while end < len(lines) and (limit is None or not size or size + len(lines[end]) <= limit):
                size += len(lines[end])
                end += 1
            if end > start:
                data = memoryview(separator + b''.join(lines[start:end]))
                while data:
                    data = data[os.write(descriptor, data):]
                os.fsync(descriptor)
        finally:
            os.close(descriptor)
        start = end
        if start < len(lines):
            part = journal_part_path(target, journal_part_number(part) + 1)
    return part


def storage_bytes(path, errors=None):
    """Return the bytes of the files under path, or None when a walk or stat fails; the error is added to errors."""
    path = Path(path)
    if path.is_symlink():
        return 0
    try:
        if path.is_file():
            return path.stat().st_size
        total = 0
        def failed(error):
            raise error
        for root, directories, files in os.walk(path, followlinks=False, onerror=failed):
            directories[:] = [name for name in directories if not (Path(root) / name).is_symlink()]
            for name in files:
                item = Path(root) / name
                if not item.is_symlink():
                    total += item.stat().st_size
        return total
    except OSError as error:
        if errors is not None:
            errors.append(error)
        return None


def result_storage(store, task):
    name = 'task-' + str(task['id'])
    worktree = task.get('worktree') or store.directory / 'worktrees' / name
    return dict(scratch=storage_bytes(store.directory / 'scratch' / name),
                worktree=storage_bytes(worktree),
                results=storage_bytes(store.directory / 'results' / name))


def recorded_keep(entries):
    seen = set()
    for entry in entries:
        storage = entry.get('storage')
        if not isinstance(storage, dict):
            continue
        for field in ('kept', 'kept_added'):
            items = storage.get(field)
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict) and all(isinstance(item.get(key), str) for key in ('path', 'reason')):
                        seen.add((item['path'], item['reason']))
    return seen


def result_keep(store, task, entries):
    from .store import _known_token_scrubber
    known = _known_token_scrubber(store.vault, strict=True)
    seen = recorded_keep(entries)
    count = 0
    kept = []
    for row in store.db.execute('SELECT locator,reason FROM keep_links WHERE task=? ORDER BY rowid', (task['id'],)):
        count += 1
        locator, _ = scrub(store, row['locator'], known)
        reason, _ = scrub(store, row['reason'], known)
        if (locator, reason) not in seen:
            kept.append(dict(path=locator, reason=reason))
            seen.add((locator, reason))
    return dict(kept_count=count, kept_added=kept) if kept else dict(kept_count=count)


def system_entry(store, task, kind, text, **extra):
    from .store import _known_token_scrubber
    from .vault import VaultError
    try:
        known = _known_token_scrubber(store.vault, strict=True)
        clean, _ = scrub(store, str(text or ''), known)
        def clean_extra(value):
            if isinstance(value, str):
                return scrub(store, value, known)[0]
            if isinstance(value, list):
                return [clean_extra(item) for item in value]
            if isinstance(value, dict):
                return {key: clean_extra(item) for key, item in value.items()}
            return value
        entry = dict(at=timestamp(time.time()), by='torii', kind=kind, text=clip(clean), **clean_extra(extra))
        journal_append(store, task, [entry])
        return True
    except (OSError, ValueError, VaultError) as error:
        problems.record(store, 'records', 'journal-failed', type(error).__name__, task=task['id'], every=3600)
        return False


def journal_part_entries(target, kind):
    try:
        with target.open(encoding='utf-8', errors='replace') as stream:
            for line in stream:
                entry = entry_of(line)
                if (entry and entry.get('kind') in KINDS and (kind is None or entry['kind'] == kind)
                        and isinstance(entry.get('text'), str)):
                    yield entry
    except FileNotFoundError:
        pass


def journal(target, limit=None, kind=None):
    parts = journal_parts(target)
    if not limit:
        return [entry for part in parts for entry in journal_part_entries(part, kind)]
    entries = []
    for part in reversed(parts):
        entries[:0] = list(journal_part_entries(part, kind))[-(limit - len(entries)):]
        if len(entries) >= limit:
            break
    return entries


LATER = re.compile(r'\n\nLater, after a background task finished(?: \(\d+ earlier notes? left out\))?: ')


def final_text(text):
    """A worker result without the interim report that came before a background task finished."""
    parts = LATER.split(text)
    return parts[-1] if len(parts) > 1 and parts[-1].strip() else text


REF = re.compile(r"""https://github\.com/[\w.-]+/[\w.-]+/pull/[0-9]+|\b[0-9a-f]{40}\b|"""
                 r"""[^\s'"`<>()\[\]{}|,;]*?(?<![\w.-])(?:(?:results|scratch)/task-[0-9]+(?:/[^\s'"`<>()\[\]{}|,;]+)?(?![\w-])"""
                 r"""|KEEP/[^\s'"`<>()\[\]{}|,;]+)""")
FOLDER = re.compile(r'(?<![\w.-])(?:results|scratch)/task-([0-9]+)')
PROOF = re.compile(r'https://github\.com/[\w.-]+/[\w.-]+/pull/[0-9]+|[0-9a-f]{40}')
NO_PR = re.compile(r'(?im)^[ \t*-]*no pr:[ \t]*\S.{9,}')
TEXT_SUFFIXES = frozenset(('.md', '.py', '.sh', '.zsh', '.bash', '.fish', '.ksh', '.command', '.js', '.jsx', '.mjs',
                           '.cjs', '.ts', '.tsx', '.rb', '.pl', '.php', '.lua', '.r', '.ps1', '.swift', '.applescript'))
TEXT_SKIPPED = frozenset(('.git', '.hg', '.svn', 'node_modules', 'dist', 'build', '.next', '.turbo', '.venv', 'venv',
                          'vendor', 'site-packages', '.tox', '.nox', '.pnpm-store', '.yarn', 'bower_components', '.vite'))
TEXT_LIMIT = 1024 * 1024


def refs_in(text):
    """PR URLs, 40-hex commits and results/, scratch/ or KEEP paths in text, in order and without repeats."""
    home = str(Path.home())
    found = []
    for match in REF.finditer(text or ''):
        value = match.group(0)
        value = value.rstrip('.:*_') if value.startswith('https://') else value.rstrip('.:*_/')
        folder = FOLDER.search(value)
        if folder and not value.startswith('https://'):
            value = value[folder.start():]
        elif not value.startswith('https://') and 'KEEP/' in value:
            value = value.removeprefix('dir:')
            value = '~' + value[len(home):] if home != '/' and value.startswith(home + '/') else value
        if len(value) <= 300 and value not in found:
            found.append(value)
    return found


def folder_refs(text, skip=None):
    """The results/ and scratch/ job folder paths that text names, except those of job `skip`.
    Cleanup keeps each one while the job that holds the reference is open."""
    return [value for value in refs_in(text) if (match := FOLDER.match(value)) and match[1] != str(skip)]


def text_refs(root, since, skip=None):
    """Job folder paths named in the text files under root that changed at or after `since`:
    Markdown, scripts and executables with a #! line, each at most TEXT_LIMIT bytes."""
    found = []
    for base, dirs, names in os.walk(root):
        dirs[:] = [name for name in dirs if name not in TEXT_SKIPPED]
        for name in names:
            pathname = os.path.join(base, name)
            try:
                info = os.lstat(pathname)
                suffix = os.path.splitext(name)[1].lower()
                if (not stat.S_ISREG(info.st_mode) or info.st_size > TEXT_LIMIT or info.st_ctime < since
                        or suffix not in TEXT_SUFFIXES and not info.st_mode & 0o111):
                    continue
                fd = os.open(pathname, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                with os.fdopen(fd, 'rb') as handle:
                    data = handle.read(TEXT_LIMIT)
            except OSError:
                continue
            if suffix not in TEXT_SUFFIXES and not data.startswith(b'#!'):
                continue
            if b'results/task-' in data or b'scratch/task-' in data:
                found.extend(value for value in folder_refs(data.decode('utf-8', 'replace'), skip) if value not in found)
    return found


def add_refs(store, task_id, values):
    """Add refs to a job as a set. The caller holds the write transaction."""
    before = store.db.total_changes
    now = time.time()
    for value in values:
        store.db.execute('INSERT OR IGNORE INTO task_refs(task,ref,added) VALUES (?,?,?)', (task_id, value, now))
    if store.db.total_changes != before:
        store.record_mark_stale(task_id)


def install_history(store):
    if 'log' not in store._columns('tasks'):
        store.db.execute('ALTER TABLE tasks ADD COLUMN log TEXT')
    if 'summary' not in store._columns('task_iterations'):
        store.db.execute('ALTER TABLE task_iterations ADD COLUMN summary TEXT')
    store.db.execute('''CREATE TABLE IF NOT EXISTS task_refs (task INTEGER NOT NULL REFERENCES tasks(id),
        ref TEXT NOT NULL, added REAL NOT NULL, PRIMARY KEY(task,ref))''')
    store.db.execute('''CREATE TABLE IF NOT EXISTS record_compactions (
        task INTEGER PRIMARY KEY REFERENCES tasks(id), since REAL NOT NULL)''')


def log_command(store, task):
    """The command that prints the full journal of a closed job, or None. `repo@sha` names the archive repository."""
    if not task.get('log'):
        return None
    name = journal_path(Path('.'), task).relative_to('records')
    repo, _, sha = task['log'].rpartition('@')
    archive = store.get('records_archive') or {}
    if repo and archive.get('repo') == repo:
        return 'git --git-dir %s show %s:%s' % (archive['git_dir'], sha, name)
    if repo:
        return 'git -C <clone of %s> show %s:%s' % (repo, sha, name)
    return 'git -C %s show %s:%s' % (store.directory / 'records', sha, name)


def record_result(store, row):
    task = store.task_get(row['task'])
    with store.transaction():
        store.db.execute('UPDATE worker_ledger_pending SET attempted=? WHERE message=?', (time.time(), row['message']))
    storage = result_storage(store, task)
    since = store.db.execute('SELECT MAX(created) FROM workers WHERE task=? AND id<?',
                             (task['id'], row['worker'])).fetchone()[0] or 0
    named = text_refs(store.directory / 'results' / ('task-' + str(task['id'])), since, task['id'])
    payload = json.loads(row['text'])
    full = payload.get('text') or payload.get('error') or ''
    text = final_text(full)
    with store.transaction():
        if not store.db.execute('SELECT 1 FROM worker_ledger_pending WHERE message=?', (row['message'],)).fetchone():
            return
        entries = journal(journal_path(store.directory, task), kind='result')
        exists = any(entry.get('receipt') == row['message'] for entry in entries)
        if not exists:
            storage.update(result_keep(store, task, entries))
            add_refs(store, task['id'], refs_in(full) + named)
        if exists or system_entry(store, task, 'result', text, worker=row['worker'],
                                  receipt=row['message'], storage=storage):
            store.db.execute('DELETE FROM worker_ledger_pending WHERE message=?', (row['message'],))
            store.record_mark_stale(task['id'])


def result_entries_in_thread(directory, vault=None, limit=32):
    from .store import Store
    store = Store(Path(directory), existing=True)
    store.vault = vault
    try:
        rows = store.db.execute("""SELECT q.message,q.worker,w.task,m.text FROM worker_ledger_pending q
            JOIN workers w ON w.id=q.worker JOIN messages m ON m.id=q.message WHERE q.attempted<=?
            ORDER BY q.attempted,q.message""", (time.time() - 60,)).fetchall()
        sweeps.run(store, 'result', [dict(row) for row in rows if store.task_get(row['task'])],
                   lambda row: record_result(store, row), area='records', key=lambda row: row['message'],
                   refs=lambda row: {'task': row['task']}, limit=limit)
    finally:
        store.close()


def entry_of(line):
    """One journal line as a dict, or None for a torn or foreign line."""
    try:
        entry = json.loads(line)
    except ValueError:
        return None
    return entry if isinstance(entry, dict) else None


def migration_entries(store, notes, known=None):
    from .store import _known_token_scrubber
    known = known or _known_token_scrubber(store.vault)
    at = timestamp(time.time())
    return [dict(at=at, by='migration', kind='note', text=scrub(store, line, known)[0])
            for line in notes.splitlines() if line.strip()]


def import_notes(store, task, entries=None):
    """Copy each line of tasks.notes into a job without a journal. The caller holds the write transaction."""
    target = journal_path(store.directory, task)
    if not task['notes'] or target.exists() or journal_parts(target):
        return 0
    entries = migration_entries(store, task['notes']) if entries is None else entries
    journal_append(store, task, entries, limit=None)
    return len(entries)


def import_all_notes(store, limit=200, pause=0.0):
    """Import one batch of old notes, and retry jobs whose import failed. Scrub outside the write lock and commit
    each job alone, so no lock is held long. Wait `pause` seconds after each job, so other writers get the lock."""
    from .store import _known_token_scrubber
    state = store.get('journal_import') or {}
    if state.get('done'):
        return 0
    cursor = state.get('cursor', 0)
    known = {}
    failed = [int(row[0]) for row in store.db.execute("SELECT item FROM sweep_failures WHERE sweep='journal-import'")]
    rows = store.db.execute("SELECT id FROM tasks WHERE id>? AND notes IS NOT NULL AND notes!='' ORDER BY id LIMIT ?",
                            (cursor, limit)).fetchall()
    count = [0]

    def import_one(task_id):
        task = store.task_get(task_id)
        entries = None
        if task['notes'] and not journal_parts(journal_path(store.directory, task)):
            known.setdefault('scrubber', _known_token_scrubber(store.vault))
            entries = migration_entries(store, task['notes'], known['scrubber'])
        with store.transaction():
            current = store.task_get(task_id)
            count[0] += import_notes(store, current, entries if current['notes'] == task['notes'] else None)
        time.sleep(pause)
    sweeps.run(store, 'journal-import', failed + [row['id'] for row in rows], import_one, area='records',
               refs=lambda task_id: {'task': task_id})
    with store.transaction():
        store.put('journal_import', {'cursor': rows[-1]['id'] if rows else cursor, 'done': len(rows) < limit and not
                                     store.db.execute("SELECT 1 FROM sweep_failures WHERE sweep='journal-import'").fetchone()})
    return count[0]


def import_all_notes_in_thread(directory, vault=None):
    from .store import Store
    store = Store(directory, existing=True)
    store.vault = vault
    try:
        return import_all_notes(store, pause=0.02)
    finally:
        store.close()


def cap(value, limit, label):
    value = str(value or '')
    if len(value) > limit:
        logger.warning('Record %s truncated', label)
        for label, pattern in PATTERNS:
            for match in pattern.finditer(value):
                if label == 'bearer' and not opaque_bearer(match.group(0).split()[-1]):
                    continue
                if match.start() < limit < match.end():
                    limit = min(limit, match.start())
        for match in ASSIGNMENT.finditer(value):
            if opaque(match.group(2)) and match.start(2) < limit < match.end(2):
                limit = min(limit, match.start(2))
        marker = value.rfind('[redacted]', 0, min(len(value), limit + len('[redacted]')))
        if 0 <= marker < limit < marker + len('[redacted]'):
            limit = marker
        return value[:limit]
    return value


def cap_marked(value, limit, label):
    original = str(value or '')
    shortened = cap(original, limit, label)
    return shortened + ('…[+%d chars]' % (len(original) - len(shortened)) if len(shortened) < len(original) else '')


def install_keep(store):
    store.db.execute('''CREATE TABLE IF NOT EXISTS keep_queue (
        id TEXT PRIMARY KEY, task INTEGER NOT NULL, source TEXT NOT NULL, name TEXT NOT NULL,
        reason TEXT NOT NULL, destination TEXT NOT NULL, created REAL NOT NULL,
        next_attempt REAL NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, notified INTEGER NOT NULL DEFAULT 0)''')
    store.db.execute('''CREATE TABLE IF NOT EXISTS keep_links (
        id TEXT PRIMARY KEY, task INTEGER NOT NULL, locator TEXT NOT NULL, reason TEXT NOT NULL)''')


def keep_destination(store, topic):
    cwd = (store.topic(topic) or {}).get('cwd')
    return store.get('keep_destination:' + topic) or 'dir:' + str(Path(cwd) / 'KEEP' if cwd else store.directory / 'keep' / slug(topic))


def validate_destination(value):
    if not value.startswith('dir:') or not Path(value[4:]).is_absolute():
        raise ValueError('Destination must be an absolute dir: path.')
    return value


def keep_job(store, ctx, task_id):
    if task_id is not None:
        job = store.task_get(task_id)
        if job and (not ctx.topic or job['topic'] == ctx.topic):
            return job
    elif ctx.topic:
        opened = store.tasks_list(topic=ctx.topic, status='open')
        if len(opened) == 1:
            return opened[0]
    raise ValueError('Cannot identify one job. Supply task= with its global id.')


def op_keep_destination(store, ctx, to=None):
    from .control_api import Refused, Result
    if not ctx.topic:
        raise Refused('Choose a chat first.')
    if to is not None:
        if ctx.source not in ('telegram', 'cli'):
            raise Refused('Only the owner can change a chat destination.')
        try:
            to = validate_destination(to)
            target = Path(to[4:])
            if not target.is_dir() or not os.access(target, os.W_OK):
                raise ValueError('Destination directory must exist and be writable.')
        except ValueError as error:
            raise Refused(str(error)) from None
        with store.transaction():
            store.put('keep_destination:' + ctx.topic, to)
    destination = keep_destination(store, ctx.topic)
    return Result(True, 'Keep destination: ' + destination, {'destination': destination})


QUEUE_COUNT_LIMIT = 100
QUEUE_BYTES_LIMIT = 1_000_000_000


def clean_keep_queue(store, folder):
    rows = store.db.execute('SELECT task,id FROM keep_queue').fetchall()
    live = {}
    for row in rows:
        name = '%s-%s' % (row['task'], row['id'])
        snapshot = folder / name
        if not snapshot.exists():
            with store.transaction():
                store.db.execute('DELETE FROM keep_queue WHERE id=?', (row['id'],))
            logger.warning('Dropped keep queue entry %s because its snapshot is missing', row['id'])
        else:
            live[name] = snapshot.stat().st_size
    for item in folder.iterdir():
        if item.name not in live and re.fullmatch(r'(?:\.[0-9a-f]{32}|[0-9]+-[0-9a-f]{32})', item.name) and item.is_file():
            item.unlink()
    return live


def op_keep_save(store, ctx, path, reason, to=None, large=False, task=None):
    from .control_api import Refused, Result
    try:
        source = Path(path).expanduser().absolute()
        if not source.is_file() or source.is_symlink():
            raise ValueError('Supply a regular file, not a symbolic link.')
        if not reason.strip():
            raise ValueError('A reason is required.')
        job = keep_job(store, ctx, task)
        destination = validate_destination(to) if to else ''
        default = keep_destination(store, job['topic'])
        if not to and not store.get('keep_destination:' + job['topic']):
            try:
                Path(default[4:]).mkdir(parents=True, exist_ok=True)
            except OSError:
                pass
        from .close_saves import directory
        folder = directory(store.directory / 'keep' / 'queue', store.directory)
        token = uuid.uuid4().hex
        snapshot = folder / ('%s-%s' % (job['id'], token))
        temporary = folder / ('.' + token)
        with (folder.parent / '.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            live = clean_keep_queue(store, folder)
            total = sum(live.values())
            descriptor = os.open(str(source), os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(descriptor, 'rb') as incoming:
                before = os.fstat(incoming.fileno())
                if before.st_size > 10_000_000 and not large:
                    raise ValueError('Files over 10 MB need large=true and a reason explaining why the large file is needed.')
                if len(live) >= QUEUE_COUNT_LIMIT or total + before.st_size > QUEUE_BYTES_LIMIT:
                    raise ValueError('Keep queue is full (100 entries or 1 GB). Drop an entry with keep.drop.')
                with temporary.open('xb') as outgoing:
                    shutil.copyfileobj(incoming, outgoing)
                    outgoing.flush()
                    os.fsync(outgoing.fileno())
                after = os.fstat(incoming.fileno())
                if (before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino):
                    raise ValueError('Source changed during save; retry with a stable file.')
            now = time.time()
            with store.transaction():
                store.db.execute('INSERT INTO keep_queue VALUES (?,?,?,?,?,?,?,?,0,0)',
                                 (token, job['id'], str(source), source.name, reason.strip(), destination, now, now))
                temporary.rename(snapshot)
        locator = keep_now(store, token)
        if locator:
            return Result(True, 'Saved ' + locator, {'locator': locator})
        return Result(True, 'Save queued for retry.', {'queued': token}, state='queued')
    except (ValueError, OSError) as error:
        if 'temporary' in locals() and temporary.exists():
            temporary.unlink()
        raise Refused(str(error)) from None


def move_snapshot(store, row):
    snapshot = store.directory / 'keep' / 'queue' / ('%s-%s' % (row['task'], row['id']))
    snapshot.unlink(missing_ok=True)


def op_keep_drop(store, ctx, id):
    from .control_api import Refused, Result
    folder = store.directory / 'keep'
    folder.mkdir(parents=True, exist_ok=True)
    with (folder / '.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        row = store.db.execute('SELECT q.*,t.topic FROM keep_queue q JOIN tasks t ON t.id=q.task WHERE q.id=?', (id,)).fetchone()
        if not row or ctx.topic and row['topic'] != ctx.topic:
            raise Refused('Unknown queued keep entry in this chat.')
        with store.transaction():
            store.db.execute('DELETE FROM keep_queue WHERE id=?', (id,))
        try:
            move_snapshot(store, row)
        except OSError as error:
            raise Refused(str(error)) from None
    return Result(True, 'Dropped keep entry ' + id + '.')


def deliver_keep(store, token):
    folder = store.directory / 'keep'
    with (folder / '.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        row = store.db.execute('SELECT q.*,t.topic FROM keep_queue q JOIN tasks t ON t.id=q.task WHERE q.id=?', (token,)).fetchone()
        if not row:
            return None
        snapshot = folder / 'queue' / ('%s-%s' % (row['task'], token))
        if not snapshot.exists():
            with store.transaction():
                store.db.execute('DELETE FROM keep_queue WHERE id=?', (token,))
            logger.warning('Dropped keep queue entry %s because its snapshot is missing', token)
            return None
        target = Path((row['destination'] or keep_destination(store, row['topic']))[4:])
        destination = target / (token + '-' + row['name'])
        temporary = target / ('.' + token + '.tmp')
        if not target.is_dir():
            raise OSError('Destination directory is unavailable')
        with snapshot.open('rb') as incoming, temporary.open('wb') as outgoing:
            shutil.copyfileobj(incoming, outgoing)
            outgoing.flush()
            os.fsync(outgoing.fileno())
        os.replace(temporary, destination)
        cwd = (store.topic(row['topic']) or {}).get('cwd')
        if cwd and target.resolve() == (Path(cwd) / 'KEEP').resolve():
            manifest = target / 'MANIFEST.md'
            line = '- %s | %s | job %s, %s | %s\n' % (
                destination.name.replace('\n', ' '), ' '.join(row['reason'].splitlines()),
                row['task'], ' '.join(row['source'].splitlines()), datetime.now(timezone.utc).date().isoformat())
            existing = manifest.read_text(encoding='utf-8') if manifest.exists() else ''
            if not any(entry.startswith('- ' + destination.name + ' | ') for entry in existing.splitlines()):
                with manifest.open('a', encoding='utf-8') as stream:
                    if existing and not existing.endswith('\n'):
                        stream.write('\n')
                    stream.write(line)
                    stream.flush()
                    os.fsync(stream.fileno())
        locator = 'dir:' + str(destination)
        with store.transaction():
            store.db.execute('INSERT OR IGNORE INTO keep_links VALUES (?,?,?,?)', (token, row['task'], locator, row['reason']))
            store.db.execute('DELETE FROM keep_queue WHERE id=?', (token,))
            store.record_mark_stale(row['task'])
        try:
            move_snapshot(store, row)
        except OSError:
            logger.warning('Saved keep snapshot could not be deleted')
        return locator


def keep_now(store, token):
    """Deliver one queued keep save. A failure is a sweep failure, so its retries back off."""
    delivered = []
    sweeps.run(store, 'keep-delivery', [token], lambda token: delivered.append(deliver_keep(store, token)),
               area='records')
    return delivered[0] if delivered else None


def retry_keep(store):
    folder = store.directory / 'keep' / 'queue'
    if folder.is_dir():
        with (folder.parent / '.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            clean_keep_queue(store, folder)
    sweeps.run(store, 'keep-delivery', [row['id'] for row in store.db.execute('SELECT id FROM keep_queue ORDER BY created')],
               lambda token: deliver_keep(store, token), area='records', limit=1)
    for row in store.db.execute('''SELECT q.id,t.topic,t.number FROM keep_queue q JOIN tasks t ON t.id=q.task
        WHERE q.notified=0 AND q.created<=?''', (time.time() - 7 * 86400,)).fetchall():
        with store.transaction():
            updated = store.db.execute('UPDATE keep_queue SET notified=1 WHERE id=? AND notified=0', (row['id'],))
            if updated.rowcount:
                store.enqueue_report(row['topic'], 'A keep save for job %s has waited 7 days. Use keep.drop id=%s to drop it.' % (row['number'], row['id']))


def retry_keep_in_thread(directory):
    from .store import Store
    store = Store(directory, existing=True)
    try:
        store.db.execute('PRAGMA foreign_keys=ON')
        retry_keep(store)
    finally:
        store.close()


def timestamp(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace('+00:00', 'Z') if value is not None else None


def origin_text(origin):
    if origin['kind'] not in ('owner', 'forwarded'):
        envelope = entry_of(origin['text'])
        if envelope is not None:
            text = envelope.get('text')
            return text if isinstance(text, str) else ''
    return origin['text']


def saved_closed_body(store, task, closed):
    """Find the saved body for this closure, so maintenance can keep the original projection."""
    try:
        data, body = read(path(store.directory, task))
    except (OSError, ValueError, KeyError, TypeError):
        return {}
    iteration = data['iterations'][-1]['k'] if data['iterations'] else 0
    if (data['uid'], data['status'], data['closed'], iteration) == (
            task['uid'], task['status'], closed, task.get('iteration', 0)):
        return body
    return {}


def project(store, task, stage=None, known=None):
    """The front matter and body of one job record. Title, ask and outcome lose token shapes and known values."""
    from .store import _known_token_scrubber
    db = store.db
    known = known or _known_token_scrubber(store.vault)
    clean = lambda value: known(redact(str(value or ''))[0])
    origin = db.execute('SELECT kind,text FROM messages WHERE id=?', (task.get('origin'),)).fetchone()
    restored_ask = db.execute('SELECT text FROM restored_record_asks WHERE task=?', (task['id'],)).fetchone()
    counts = {row[0]: row[1] for row in db.execute('SELECT provider,COUNT(*) FROM workers WHERE task=? GROUP BY provider', (task['id'],))}
    children = [row[0] for row in db.execute('SELECT id FROM tasks WHERE parent=? ORDER BY id', (task['id'],))]
    last_stage = db.execute('SELECT stage FROM task_stamps WHERE task=? ORDER BY id DESC LIMIT 1', (task['id'],)).fetchone()
    current = db.execute('SELECT status FROM tasks WHERE id=?', (task['id'],)).fetchone()
    closed_row = db.execute('SELECT closed,summary,reason FROM task_iterations WHERE task=? AND k=?',
                            (task['id'], task.get('iteration', 0))).fetchone()
    closed = None
    summary = reason = None
    if task['status'] in CLOSED:
        closed = (closed_row[0] if current and current[0] == task['status'] and closed_row and closed_row[0] is not None
                  else task['updated'])
        if closed_row:
            summary, reason = closed_row['summary'], closed_row['reason']
    data = dict(id=task['id'], uid=task['uid'], chat=cap(task['topic'], 512, 'chat'), number=task['number'],
                title=clean(cap(task['title'], 1000, 'title')), status=task['status'], finish=task.get('finish'),
                created=iso(task['created']), closed=iso(closed),
                parent=task.get('parent'), blocked_by=task.get('blocked_by', []),
                discovered_from=[row[0] for row in db.execute("SELECT target FROM task_links WHERE task=? AND kind='discovered_from' ORDER BY target", (task['id'],))],
                related=[row[0] for row in db.execute("SELECT target FROM task_links WHERE task=? AND kind='related' ORDER BY target", (task['id'],))],
                replaces=[row[0] for row in db.execute("SELECT target FROM task_links WHERE task=? AND kind='replaces' ORDER BY target", (task['id'],))],
                follow_ups=children, pr=task.get('pr'),
                refs=[row[0] for row in db.execute('''SELECT ref FROM (SELECT ref,added,rowid AS row FROM task_refs WHERE task=?
                    ORDER BY added DESC,rowid DESC LIMIT ?) ORDER BY added,row''', (task['id'], REFS))],
                log=task.get('log'), workers=counts,
                links=[{'locator': row['locator'], 'reason': row['reason']} for row in db.execute(
                    'SELECT locator,reason FROM keep_links WHERE task=? ORDER BY rowid', (task['id'],))],
                stage=stage or (last_stage[0] if last_stage else None),
                iterations=[dict(k=row['k'], opened=iso(row['opened']), closed=iso(row['closed']), opened_by=row['opened_by'],
                                 outcome=row['outcome'], reason=cap(row['reason'], 280, 'reason') if row['reason'] else row['reason'])
                            for row in store.task_iterations(task['id'])],
                results_size=results_size(store, task))
    ask_text = origin_text(origin) if origin else restored_ask[0] if restored_ask else task['title']
    ask = clean(cap_marked(ask_text, 4000, 'ask'))
    close_reason = reason if not summary else None
    outcome = clean(cap_marked(summary or close_reason or open_outcome(store, task), OUTCOME_LIMIT, 'outcome'))
    legacy_ask = origin['text'] if origin and ask_text != origin['text'] else None
    if closed is not None and (legacy_ask is not None or close_reason):
        saved = saved_closed_body(store, task, data['closed'])
        if legacy_ask is not None and saved.get('ask') == clean(cap_marked(legacy_ask, 4000, 'ask')):
            ask = saved['ask']
        if close_reason and 'outcome' in saved and saved['outcome'] != outcome:
            if saved['outcome'] == clean(cap_marked(open_outcome(store, task), OUTCOME_LIMIT, 'outcome')):
                outcome = saved['outcome']
    return redact_data(data), dict(ask=ask, outcome=outcome)


def open_outcome(store, task):
    """The last report about the job, else the last close summary, else the last worker result, else the saved outcome."""
    reports = journal(journal_path(store.directory, task), 1, kind='report')
    if reports:
        return reports[-1]['text']
    summary = store.db.execute("""SELECT summary FROM task_iterations WHERE task=? AND summary IS NOT NULL AND summary!=''
        ORDER BY k DESC LIMIT 1""", (task['id'],)).fetchone()
    if summary:
        return summary[0]
    for outcome in store.db.execute("""SELECT m.text FROM messages m JOIN workers w ON w.id=m.source_worker
            WHERE w.task=? AND m.kind='worker_result' ORDER BY m.id DESC""", (task['id'],)):
        try:
            payload = json.loads(outcome[0])
            text = final_text(payload.get('text') or '') if isinstance(payload, dict) else ''
        except (ValueError, TypeError):
            text = outcome[0]
        if text:
            return text
    try:
        return read(path(store.directory, task))[1]['outcome']
    except (OSError, ValueError, KeyError):
        return ''


def iso(value):
    """A time as ISO seconds in UTC, from epoch seconds or an ISO string."""
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()
    return datetime.fromtimestamp(value, timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')


def seconds(value):
    """Epoch seconds from an ISO string or a number."""
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()
    return value


def normal(data):
    """Front matter in one shape for record formats 1 and 2: every field, ISO seconds, no commit field."""
    data = dict(data)
    data.pop('commit', None)
    if set(data) - set(FIELDS) or data.get('created') is None:
        raise ValueError('Invalid record fields')
    for key in FIELDS:
        data.setdefault(key, [] if key in LISTS else {} if key == 'workers' else None)
    data['created'], data['closed'] = iso(data['created']), iso(data['closed'])
    data['iterations'] = [{key: iso(item.get(key)) if key in ('opened', 'closed') else item.get(key) for key in ITERATION}
                          for item in data['iterations']]
    return data


def front_lines(data):
    def shown(key):
        if key != 'iterations':
            return data[key]
        return [{name: value for name, value in item.items() if value is not None} for item in data[key]]
    return '\n'.join(key + ': ' + json.dumps(shown(key), ensure_ascii=False, separators=(',', ':'))
                     for key in FIELDS if shown(key) not in (None, [], {}))


def render(data, body):
    """Format 2: empty fields are left out, times are ISO seconds, and the body is the ask and the outcome."""
    data = normal(data)
    def make():
        quote = lambda value: '\n'.join('> ' + line for line in value.split('\n'))
        return ('---\n' + front_lines(data) + '\n---\n## Ask\n' + quote(body['ask']) + '\n\n## Outcome\n' +
                quote(body['outcome']) + '\n')
    result = make()
    while len(result.encode()) > 16384 and data['follow_ups']:
        data['follow_ups'].pop()
        logger.warning('Record size truncated follow-ups')
        result = make()
    while len(result.encode()) > 16384 and data['workers']:
        data['workers'].pop(next(reversed(data['workers'])))
        logger.warning('Record size truncated worker counts')
        result = make()
    for key, keep in (('links', 0), ('refs', 0), ('iterations', 1), ('blocked_by', 0), ('related', 0), ('discovered_from', 0), ('replaces', 0)):
        dropped = 0
        while len(result.encode()) > 16384 and len(data[key]) > keep:
            size = len(data[key])
            data[key] = data[key][max((size + 1) // 2, keep):]
            dropped += size - len(data[key])
            result = make()
        if dropped:
            logger.warning('Record size truncated %s: dropped %d', key, dropped)
    if len(result.encode()) > 16384 and data['iterations']:
        data['iterations'] = []
        logger.warning('Record size truncated iterations')
        result = make()
    while len(result.encode()) > 16384:
        candidates = [(len(str(value).encode()), key) for key, value in data.items() if isinstance(value, str) and key == 'title']
        candidates += [(len(body[key].encode()), key) for key in ('ask', 'outcome')]
        _, key = max(candidates)
        target = data if key in data else body
        if not target[key]:
            raise ValueError('Record metadata exceeds size limit')
        target[key] = cap(target[key], len(target[key]) // 2, key)
        logger.warning('Record size truncated %s', key)
        result = make()
    return result


def write(store, task, stage=None, known=None):
    with write_lock(store.directory):
        task = store.task_get(task['id'])
        if task is None:
            raise ValueError('Unknown job')
        target = path(store.directory, task)
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        replace(target, render(*project(store, task, stage, known)))
        return target


def replace(target, content):
    fd, name = tempfile.mkstemp(prefix='.record-', dir=str(target.parent))
    with os.fdopen(fd, 'w', encoding='utf-8') as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(name, target)


def read(pathname):
    return parse(pathname.read_text(encoding='utf-8'))


def parse(text):
    front, rest = text.removeprefix('---\n').split('\n---\n', 1)
    data = normal(dict((key, json.loads(value)) for key, value in (line.split(': ', 1) for line in front.splitlines())))
    ask, rest = rest.removeprefix('## Ask\n').split('\n\n## Outcome\n', 1)
    outcome = rest.split('\n\n## Notes\n', 1)[0].rstrip('\n')
    def unquote(value):
        lines = value.split('\n')
        if any(not line.startswith('> ') for line in lines):
            raise ValueError('Invalid record body')
        return '\n'.join(line[2:] for line in lines)
    return data, dict(ask=unquote(ask), outcome=unquote(outcome))


DURABLE_SETTINGS = frozenset((
    'antigravity_enabled', 'antigravity_sandbox', 'close_saves_discard_names', 'close_saves_rebuildable',
    'close_saves_keep',
    'codex_auto_switch', 'codex_enabled', 'cow_worktrees',
    'codex_model', 'codex_parent_effort', 'codex_worker_effort', 'claude_parent_effort', 'claude_worker_effort',
    'coordinator_home_topic', 'coordinator_model',
    'coordinator_selection',
    'idle_trim',
    'opencode_binary', 'opencode_enabled', 'parent_selection', 'plugin_chats', 'projects_root',
    'records_sync', 'records_sync_install', 'records_sync_remote',
    'session_delete', 'stray_report_config', 'stray_report_enabled',
    'telegram_updates_enabled',
    'worker_model'))


def durable_key(key):
    return key in DURABLE_SETTINGS or bool(re.fullmatch(
        r'(?:finish_default|keep_destination|chat_tags):[-0-9:]+|coordinator:[-0-9:]+_selection', key))


def safe_setting(value, known, rules=False):
    if isinstance(value, dict):
        return all(not re.search(r'(?i)(token|secret|password|passwd|api[_-]?key|credential)', str(key))
                   and safe_setting(str(key), known) and safe_setting(item, known) for key, item in value.items())
    if isinstance(value, list):
        return all(safe_setting(item, known) for item in value)
    if not isinstance(value, str):
        return value is None or type(value) in (bool, int, float)
    parsed = urlsplit(value)
    if parsed.scheme and parsed.netloc and (parsed.username or parsed.password or parsed.query):
        return False
    from .scrub import CREDENTIAL, URL_CREDENTIALS
    scanned = RULE_NAME_ASSIGNMENT.sub('', value) if rules else value
    return (not CREDENTIAL.search(scanned) and not URL_CREDENTIALS.search(value)
            and not redact(scanned)[1] and not redact(value, assignments=False)[1] and known(value) == value)


def rule_file(name):
    return 'rules/' + (name if name == 'personal' or name.startswith('tags/') else slug(name)) + '.md'


def config_snapshot(store, known=None):
    from .policy import chat_path, personal_path, read_rule, valid_tag, valid_tags
    from .store import _known_token_scrubber
    known = known or _known_token_scrubber(store.vault, strict=True, min_length=1)
    settings = {}
    for row in store.db.execute('SELECT key,value FROM settings ORDER BY key'):
        if durable_key(row['key']):
            value = json.loads(row['value'])
            if (not row['key'].startswith('chat_tags:') or valid_tags(value)) and safe_setting(value, known):
                settings[row['key']] = value
    rules = {}
    sources = [(topic['id'], chat_path(store, topic['id'])) for topic in store.topics()]
    tag_folder = store.directory / 'tags'
    if not tag_folder.is_symlink():
        sources.extend(('tags/' + path.stem, path) for path in sorted(tag_folder.glob('*.md')) if valid_tag(path.stem))
    sources.append(('personal', store.directory / 'personal.md'))
    for name, source in sources:
        try:
            content = read_rule(personal_path(store, create=False) if name == 'personal' else source)
        except FileNotFoundError:
            continue
        except (OSError, UnicodeError):
            content = None
        if content is not None and safe_setting(content, known, rules=True):
            rules[name] = content
            continue
        problems.record(store, 'records', 'rules-skipped', str(source.relative_to(store.directory)),
                        every=86400, scope=name)
    return settings, rules


def config_files(store, known=None):
    settings, rules = config_snapshot(store, known)
    mapping = {name: rule_file(name) for name in rules}
    files = {'settings.json': json.dumps(settings, ensure_ascii=False, indent=2, sort_keys=True) + '\n',
             'rules.json': json.dumps(mapping, ensure_ascii=False, indent=2, sort_keys=True) + '\n'}
    files.update({mapping[name]: content for name, content in rules.items()})
    return files


def write_config(store, known=None):
    root = store.directory / 'records'
    for name, content in config_files(store, known).items():
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if not target.exists() or target.read_text(encoding='utf-8') != content:
            replace(target, content)


def backfill(store):
    from .store import _known_token_scrubber
    tasks = store.tasks_list()
    known = _known_token_scrubber(store.vault)
    for task in tasks:
        write(store, task, known=known)
    write_chats(store)
    write_config(store)
    return {'records': len(tasks)}


def write_chats(store):
    root = store.directory / 'records'
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    fields = ('id', 'chat', 'thread', 'name', 'cwd', 'provider')
    content = json.dumps([dict({key: topic[key] for key in fields}, dir=slug(topic['id']),
                               project=Path(topic['cwd']).name if topic.get('cwd') else '')
                          for topic in store.topics()], ensure_ascii=False, indent=2) + '\n'
    target = root / 'chats.yaml'
    if target.exists() and target.read_text(encoding='utf-8') == content:
        return
    fd, name = tempfile.mkstemp(prefix='.chats-', dir=str(root))
    with os.fdopen(fd, 'w', encoding='utf-8') as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(name, target)


def restore(store, source):
    source = Path(source).resolve()
    target = store.directory / 'records'
    if target.exists() or store.db.execute('SELECT 1 FROM tasks LIMIT 1').fetchone() or store.topics():
        raise ValueError('Restore requires a fresh state directory without records or topics')
    if not source.is_dir() or any(p.is_symlink() for p in source.rglob('*')):
        raise ValueError('Records source must be a directory without symbolic links')
    chats = json.loads((source / 'chats.yaml').read_text(encoding='utf-8'))
    jobs = [(filename, *read(filename)) for filename in sorted(source.glob('chat-*/*.md'))]
    jobs.sort(key=lambda job: job[1]['id'])
    for filename, data, body in jobs:
        if filename.relative_to(source) != path(Path('.'), {'topic': data['chat'], 'number': data['number']}).relative_to('records'):
            raise ValueError('Record path does not match its identity')
    config = source / 'settings.json'
    settings = json.loads(config.read_text(encoding='utf-8')) if config.is_file() else {}
    manifest = source / 'rules.json'
    rules = []
    if manifest.is_file():
        from .policy import chat_path, valid_tag
        topics = {chat['id'] for chat in chats}
        for name, relative in json.loads(manifest.read_text(encoding='utf-8')).items():
            if relative != rule_file(name):
                raise ValueError('Invalid rules manifest')
            if name == 'personal' or name.startswith('tags/') and valid_tag(name.removeprefix('tags/')):
                destination = store.directory / (name + '.md')
            elif name in topics:
                destination = chat_path(store, name)
            else:
                raise ValueError('Invalid rules manifest')
            if destination in (target for target, _ in rules):
                raise ValueError('Invalid rules manifest')
            rules.append((destination, (source / relative).read_text(encoding='utf-8')))
    stage = Path(tempfile.mkdtemp(prefix='.records-restore-', dir=str(store.directory)))
    written = []
    try:
        shutil.copytree(source, stage / 'records')
        restore_rows(store, chats, jobs, settings, rules, written, stage / 'records', target)
    except BaseException:
        if written and written[0] == (target, None):
            os.replace(target, stage / 'records')
            written.pop(0)
        for destination, previous in reversed(written):
            if previous is None:
                destination.unlink(missing_ok=True)
            else:
                replace(destination, previous)
        raise
    finally:
        shutil.rmtree(stage, ignore_errors=True)
    return {'records': len(jobs), 'topics': len(chats)}


def restore_rows(store, chats, jobs, settings, rules, written, staged, target):
    """Insert rows, write rule files and move the staged records in place in one transaction.

    `written` collects each change outside the database so the caller can undo it on failure.
    """
    from .policy import read_rule, valid_tags
    with store.transaction():
        for chat in chats:
            store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,provider,enabled) VALUES (?,?,?,?,?,?,0)',
                             tuple(chat[key] for key in ('id', 'chat', 'thread', 'name', 'cwd', 'provider')))
        for filename, data, body in jobs:
            created = datetime.fromisoformat(data['created'].replace('Z', '+00:00')).timestamp()
            updated = datetime.fromisoformat(data['closed'].replace('Z', '+00:00')).timestamp() if data['closed'] else created
            status = 'interrupted' if data['status'] in ('open', 'waiting', 'interrupted') else data['status']
            store.db.execute('INSERT INTO restored_record_asks(task,text) VALUES (?,?)', (data['id'], body['ask']))
            iterations = data['iterations'] or [dict(k=0, opened=created, closed=updated if data['closed'] else None,
                                                      opened_by='migration', outcome=data['status'] if data['closed'] else None, reason=None)]
            record_path = str(path(store.directory, {'topic': data['chat'], 'number': data['number']}).relative_to(store.directory))
            notes = [entry['text'] for entry in journal(filename.with_suffix('.jsonl'), kind='note')]
            store.db.execute('''INSERT INTO tasks(id,uid,topic,number,title,status,finish,created,updated,parent,pr,notes,origin,iteration,record_path,log)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (data['id'], data['uid'], data['chat'], data['number'], data['title'], status, data['finish'], created,
                 updated, data['parent'], data['pr'], '\n'.join(notes) or None, None, iterations[-1]['k'], record_path, data['log']))
            store.db.executemany('INSERT OR IGNORE INTO task_refs(task,ref,added) VALUES (?,?,?)',
                                 ((data['id'], ref, created + index) for index, ref in enumerate(data['refs'])))
            for link in data['links']:
                store.db.execute('INSERT INTO keep_links VALUES (?,?,?,?)',
                                 (uuid.uuid4().hex, data['id'], link['locator'], link['reason']))
            for row in iterations:
                summary = body['outcome'] or None if row is iterations[-1] and data['status'] in CLOSED else None
                store.db.execute('''INSERT INTO task_iterations(task,k,opened,closed,opened_by,outcome,reason,summary)
                    VALUES (?,?,?,?,?,?,?,?)''', (data['id'], row['k'], seconds(row['opened']), seconds(row['closed']),
                                                  row['opened_by'], row['outcome'], row['reason'], summary))
            if data['stage']:
                store.db.execute('INSERT INTO task_stamps(task,stage,created,iteration) VALUES (?,?,?,?)',
                                 (data['id'], data['stage'], updated, iterations[-1]['k']))
            store.record_mark_stale(data['id'])
        for filename, data, body in jobs:
            for kind in ('blocked_by', 'related', 'discovered_from', 'replaces'):
                store.db.executemany('INSERT OR IGNORE INTO task_links(task,target,kind) VALUES (?,?,?)',
                                     ((data['id'], target, kind) for target in data[kind]))
        for key, value in settings.items():
            if (durable_key(key) and (not key.startswith('chat_tags:') or valid_tags(value))
                    and safe_setting(value, lambda text: text)):
                store.put(key, value)
        for destination, content in rules:
            destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            try:
                previous = read_rule(destination)
            except FileNotFoundError:
                previous = None
            written.append((destination, previous))
            replace(destination, content)
        os.replace(staged, target)
        written.insert(0, (target, None))


def sweep(store, limit=32, task_ids=None, known=None):
    """Rewrite stale job records, newest first. Never raises, because a store commit calls it."""
    from .store import _known_token_scrubber
    if store.db.in_transaction:
        return
    scrubber = {'known': known}

    def rewrite(row):
        task = store.task_get(row['task'])
        if task is not None:
            scrubber['known'] = scrubber['known'] or _known_token_scrubber(store.vault)
            write(store, task, known=scrubber['known'])
            keep_documents(store, task)
        with store.db:
            store.db.execute('DELETE FROM stale_records WHERE task=? AND version=?', (row['task'], row['version']))
    try:
        if task_ids is None:
            rows = store.db.execute('SELECT task,version FROM stale_records ORDER BY version DESC, task DESC').fetchall()
        else:
            rows = [store.db.execute('SELECT task,version FROM stale_records WHERE task=?', (task_id,)).fetchone()
                    for task_id in task_ids]
        sweeps.run(store, 'rewrite', [row for row in rows if row is not None], rewrite, area='records',
                   key=lambda row: row['task'], refs=lambda row: {'task': row['task']},
                   limit=limit if task_ids is None else None)
    except Exception as error:
        problems.record(store, 'records', 'sweep-failed', sweeps.describe(error), level=logging.ERROR, every=3600)


def check(store, known=None, config_known=None):
    from .store import _known_token_scrubber
    known = known or _known_token_scrubber(store.vault, strict=True)
    config_known = config_known or _known_token_scrubber(store.vault, strict=True, min_length=1)
    db = sqlite3.connect(':memory:')
    db.execute('''CREATE TABLE tasks (id INTEGER PRIMARY KEY, uid TEXT, chat TEXT, number INTEGER,
        title TEXT, status TEXT, finish TEXT, created TEXT, closed TEXT, parent INTEGER, pr TEXT,
        payload TEXT NOT NULL)''')
    differences = []
    root = store.directory / 'records'
    for filename in sorted(root.glob('chat-*/*.md')):
        try:
            data, body = read(filename)
            db.execute('INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                       (data['id'], data['uid'], data['chat'], data['number'], data['title'], data['status'],
                        data['finish'], data['created'], data['closed'], data['parent'], data['pr'],
                        json.dumps([data, body, str(filename)])))
        except (ValueError, KeyError, sqlite3.IntegrityError, OSError) as error:
            differences.append({'path': str(filename.relative_to(root)), 'error': type(error).__name__})
    for task in store.tasks_list():
        expected, body = project(store, task, known=known)
        expected, body = read_rendered(expected, body)
        row = db.execute('SELECT payload FROM tasks WHERE id=?', (task['id'],)).fetchone()
        if not row:
            differences.append({'id': task['id'], 'fields': ['missing']})
            continue
        actual, actual_body, filename = json.loads(row[0])
        fields = [key for key in FIELDS if key not in INFORMATIONAL and actual.get(key) != expected.get(key)]
        fields += [key for key in body if actual_body.get(key) != body[key]]
        if filename != str(path(store.directory, task)):
            fields.append('path')
        if task.get('record_path') != str(path(store.directory, task).relative_to(store.directory)):
            fields.append('record_path')
        if fields:
            differences.append({'id': task['id'], 'fields': fields})
        db.execute('DELETE FROM tasks WHERE id=?', (task['id'],))
    differences.extend({'id': row[0], 'fields': ['extra']} for row in db.execute('SELECT id FROM tasks'))
    db.close()
    if not (root / 'settings.json').exists() and not (root / 'rules.json').exists() and store.get('records_config_projection') is None:
        write_config(store, config_known)
        with store.transaction():
            store.put('records_config_projection', hashlib.sha256(
                json.dumps(config_files(store, config_known), sort_keys=True, ensure_ascii=False).encode()).hexdigest())
    for name, content in config_files(store, config_known).items():
        try:
            actual = (root / name).read_text(encoding='utf-8')
        except (OSError, UnicodeError):
            actual = None
        if actual != content:
            differences.append({'path': name, 'fields': ['configuration']})
    return {'differences': differences, 'count': len(differences),
            'bytes': sum(p.stat().st_size for p in root.glob('chat-*/*.md'))}


def read_rendered(data, body):
    rendered = render(data, body)
    front = rendered.split('\n---\n', 1)[0].removeprefix('---\n')
    return normal(dict((key, json.loads(value)) for key, value in (line.split(': ', 1) for line in front.splitlines()))), body


def op_propose(store, ctx, parent, title, why, blocked_by=None):
    job = store.task_get(parent)
    if not job:
        raise Refused('Unknown parent job.')
    if ctx.topic and ctx.topic != job['topic']:
        raise Refused('Parent job is in another chat.')
    try:
        with store.transaction():
            count = store.db.execute("SELECT COUNT(*) FROM tasks WHERE topic=? AND status='proposed'", (job['topic'],)).fetchone()[0]
            linked = "AND NOT EXISTS (SELECT 1 FROM code_requests r WHERE r.job=t.id)" if store.code_requests is not None else ''
            old = store.db.execute("""SELECT t.id FROM tasks t WHERE t.topic=? AND t.status='proposed' AND t.id!=?
                AND NOT EXISTS (SELECT 1 FROM workers w WHERE w.task=t.id)
                AND NOT EXISTS (SELECT 1 FROM tasks child WHERE child.parent=t.id)
                AND NOT EXISTS (SELECT 1 FROM task_links b WHERE b.target=t.id)
                """ + linked + " ORDER BY t.created,t.id LIMIT ?", (job['topic'], parent, max(0, count - 49))).fetchall()
            for row in old:
                store.task_update(row['id'], status='dropped', note='Dropped to make room for a new follow-up proposal.')
            proposed = store.task_create(job['topic'], title, parent=parent, status='proposed',
                                         blocked_by=() if blocked_by is None else (blocked_by,))
            store.task_link(proposed['id'], parent, 'discovered_from')
            store.task_note(proposed['id'], why, author(store, ctx))
            proposed = store.task_get(proposed['id'])
    except ValueError as error:
        raise Refused(str(error)) from error
    return Result(True, 'Follow-up %s proposed.' % job_number(proposed['number'], proposed['iteration']), proposed)


def op_ready(store, ctx, topic=None, project=None):
    rows = [dict(row) for row in store.db.execute("""SELECT t.* FROM tasks t
        WHERE t.status='proposed' OR (t.status='open'
            AND NOT EXISTS (SELECT 1 FROM task_links b JOIN tasks d ON d.id=b.target
                WHERE b.kind='blocked_by' AND b.task=t.id AND d.status IN ('open','waiting','interrupted'))
            AND NOT EXISTS (SELECT 1 FROM workers w WHERE w.task=t.id
                AND w.status IN ('queued','waiting_for_secret','waiting_for_quota','running')))
        ORDER BY CASE WHEN t.topic=? THEN 0 ELSE 1 END,t.topic,t.number""", (topic,))]
    if project is not None:
        rows = [row for row in rows if Path((store.topic(row['topic']) or {}).get('cwd') or '').name == project]
    lines = ['%s job %s: %s (%s)' % (row['topic'], job_number(row['number'], row['iteration'],),
                                     row['title'], row['status']) for row in rows]
    return Result(True, '\n'.join(lines) or 'No ready jobs or proposed follow-ups.', rows)


def scan_content(text, filename):
    if filename.startswith('rules/'):
        return bool(redact(RULE_NAME_ASSIGNMENT.sub('', text))[1]
                    or redact(text, assignments=False)[1])
    return bool(redact(text)[1])


def gitleaks_blocked(report, content):
    findings = json.loads(report)
    if not isinstance(findings, list) or not findings:
        raise RuntimeError('Gitleaks returned no findings for a failed scan')
    for finding in findings:
        lines = content(finding).splitlines()
        line_number = finding.get('StartLine', 0)
        if finding.get('RuleID') == 'generic-api-key' and 0 < line_number <= len(lines):
            line = lines[line_number - 1]
            start = max(0, finding.get('StartColumn', 1) - 2)
            end = finding.get('EndColumn', len(line))
            match = line[start:end]
            if re.search(r'(?i)\bkey[^:=\r\n]{0,40}[:=]', match) and not redact(match)[1]:
                continue
            if line.startswith('refs: ') and not redact(line)[1]:
                continue
        return finding.get('File', 'unknown file')
    return None


def github_repository(remote):
    scp = re.fullmatch(r'(?:[^/@:]+@)?([^/:]+):(.+)', remote) if '://' not in remote else None
    if scp:
        host, path = scp.groups()
    else:
        parsed = urlsplit(remote)
        host, path = parsed.hostname or '', parsed.path.lstrip('/')
    if host.lower().rstrip('.') != 'github.com':
        return None
    path = path.removesuffix('.git')
    if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', path):
        raise ValueError('Invalid GitHub records remote')
    return path


def remote_host(remote):
    """The host of a remote URL or scp-style address. A local path has the host ''."""
    if '://' in remote:
        return (urlsplit(remote).hostname or '').lower().rstrip('.')
    scp = re.fullmatch(r'(?:[^/@:]+@)?([^/:]+):(.+)', remote)
    return scp.group(1).lower().rstrip('.') if scp else ''


class GitFailed(RuntimeError):
    """A command failed. `detail` holds its error output for `Remote.reason` only; it is never reported."""

    def __init__(self, message, detail=''):
        super().__init__(message)
        self.detail = detail


class PushRefused(ValueError):
    """Torii refused the push. The message is the reason code."""


class Remote:
    """One host of record remotes. A subclass gives `git_env`, `check_private` and `reason`."""

    name = 'remote'
    hosts = ()
    codes = (('auth_failed', r'authentication failed|could not read username|permission denied \(publickey\)|'
                             r'invalid credentials|gh auth login|not logged in'),
             ('rate_limited', r'rate limit'),
             ('forbidden', r'\b403\b|permission to \S+ denied|forbidden'),
             ('not_found', r'not found|does not appear to be a git repository|could not resolve to a repository'),
             ('diverged', r'non-fast-forward|\[rejected\]|fetch first'),
             ('offline', r'could not resolve host|connection (?:refused|timed out|reset)|network is unreachable'))

    def __init__(self, remote):
        self.remote = remote

    def git_env(self):
        """Authentication for git, as environment only. Never on the command line or in `.git/config`."""
        return {}

    def check_private(self, run):
        """Return 'private' only when only the owner's accounts can read the repository."""
        raise PushRefused('not_private')

    def reason(self, error):
        """A short code for a failure. It carries no provider text and no token."""
        if isinstance(error, PushRefused):
            return str(error)
        if isinstance(error, TimeoutError):
            return 'timed_out'
        text = getattr(error, 'detail', '').lower()
        for code, pattern in self.codes:
            if re.search(pattern, text):
                return code
        return 'push_failed'


class GitHubRemote(Remote):
    """GitHub: git uses the Mac's gh login, and `gh repo view` proves that the repository is private."""

    name = 'GitHub'
    hosts = ('github.com',)

    def __init__(self, remote):
        super().__init__(remote)
        self.repository = github_repository(remote)

    def check_private(self, run):
        output, _ = run(['gh', 'repo', 'view', self.repository, '--json', 'visibility', '--jq', '.visibility'])
        return output.strip().lower()


PROVIDERS = (GitHubRemote,)


def provider(remote):
    """The provider for the host of a remote. A host with no provider class is refused."""
    host = remote_host(remote)
    for kind in PROVIDERS:
        if host in kind.hosts:
            return kind(remote)
    raise ValueError('unknown_host')


def documents():
    """The project documents module, or None where it is not installed."""
    from .extension import optional
    return optional('docs')


def keep_documents(store, task):
    """At a job close, keep the plan-like files of its results in the ledger. A failure never fails the record."""
    docs = documents()
    if docs is None or task['status'] not in ('done', 'dropped', 'expired'):
        return
    try:
        docs.keep_results(store, task)
    except Exception as error:
        problems.record(store, 'records', 'documents-failed', type(error).__name__, task=task['id'], every=3600)


def group_of(filename):
    match = re.fullmatch(r'(chat-[^/]+/[0-9]+)\.[1-9][0-9]*\.jsonl', filename)
    if match:
        return match[1]
    return filename if filename in SINGLE else filename.rsplit('.', 1)[0]


def journal_merge_lines(saved, source, copied):
    """Find local lines absent from the remote journal and from a previous recovery attempt."""
    lines = {line for content in saved for line in content.split('\n') if line}
    texts = {entry['text'] for entry in map(entry_of, lines) if entry and isinstance(entry.get('text'), str)}
    transferred = Counter(line for part in copied for line in part.read_text(encoding='utf-8', errors='replace').split('\n'))
    added = []
    for part in source:
        for line in part.read_text(encoding='utf-8', errors='replace').split('\n'):
            if not line or line in lines:
                continue
            entry = entry_of(line) or {}
            content = entry.get('text')
            if entry.get('by') == 'migration' and isinstance(content, str) and content in texts:
                continue
            if transferred[line]:
                transferred[line] -= 1
            else:
                added.append((line + '\n').encode())
    return added


def front_matter(text):
    data = {}
    for line in text.removeprefix('---\n').split('\n---\n', 1)[0].splitlines():
        key, separator, value = line.partition(': ')
        if separator:
            try:
                data[key] = json.loads(value)
            except ValueError:
                pass
    return data


def commit_message(group, content):
    docs = documents()
    if docs and docs.owns(group):
        return docs.commit_message(group, content)
    if group in SINGLE:
        return group.split('.')[0]
    data = front_matter(content)
    status = str(data.get('status') or 'unknown')
    stage = ' ' + str(data['stage']) if data.get('stage') else ''
    title = ' '.join(str(data.get('title') or '').split())[:60]
    return 'job %s %s%s: %s' % (data.get('id', group.rsplit('/', 1)[-1]), status, stage, title)


def check_drift(directory, values, config_values=None):
    from .scrub import Scrubber
    from .store import Store
    store = Store(Path(directory), existing=True)
    try:
        now = time.time()
        if now - (store.get('records_drift_checked', 0) or 0) < 86400:
            return
        known = Scrubber.of((value, '[token]') for value in values)
        config_known = Scrubber.of((value, '[token]') for value in (config_values if config_values is not None else values))
        try:
            result = check(store, known, config_known)
        except (OSError, ValueError, sqlite3.Error) as error:
            problems.record(store, 'records', 'drift-check-failed', type(error).__name__, every=3600)
            return
        if result['count']:
            problems.record(store, 'records', 'drift', '%d differences' % result['count'], every=86400)
            ids = [item['id'] for item in result['differences'] if 'id' in item and item.get('fields') != ['extra']]
            with store.transaction():
                for task_id in ids:
                    store.record_mark_stale(task_id)
            sweep(store, limit=len(ids), task_ids=ids, known=known)
            write_config(store, config_known)
            write_chats(store)
        with store.transaction():
            store.put('records_drift_checked', now)
    finally:
        store.close()


def sync(directory, remote=None, install=None, push=False, known=(), known_seen=False, drift=False, config_known=None,
         compact=()):
    """Commit changed records in place, one commit per job, and push to install/<name> when asked.

    A staged job that matches a scan pattern, gitleaks or a known token value stays uncommitted.
    A push needs a provider, gitleaks, a private repository, a remote branch that HEAD contains, and a clean
    scan of each unpushed commit (the full history before the first push) by gitleaks and for the known values.
    With `known_seen` false the known values are checked over the full history, because a value can join the vault
    after its commit. A new records repository adopts install/<name> only when each job id there has this
    install's uid. Each group in `compact` (a closed job) loses its committed, unchanged journal parts in one commit;
    `compacted` maps each such group to the commit that still holds them and the outcome its record had there,
    or None when it had none."""
    import subprocess

    directory = Path(directory)
    try:
        host = provider(remote) if remote else None
        unknown = None
    except ValueError:
        host, unknown = None, 'unknown_host'
    if remote and not (install and INSTALL.fullmatch(install)):
        host, unknown = None, 'install_missing'
    branch = 'refs/heads/install/%s' % install
    tracking = 'refs/remotes/origin/install/%s' % install
    root = directory / 'records'
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    deadline = time.monotonic() + SYNC_LIMIT
    env = dict(os.environ, GIT_TERMINAL_PROMPT='0', GIT_CONFIG_NOSYSTEM='1')
    env.pop('GITLEAKS_CONFIG', None)
    env['GITLEAKS_CONFIG_TOML'] = '[extend]\nuseDefault = true\n'
    env.update(GIT_AUTHOR_NAME='Torii', GIT_COMMITTER_NAME='Torii',
               GIT_AUTHOR_EMAIL='torii@localhost', GIT_COMMITTER_EMAIL='torii@localhost')
    pins = (('log.showRoot', 'true'), ('color.ui', 'never'), ('color.diff', 'never'), ('diff.renames', 'true'))
    first = int(env.get('GIT_CONFIG_COUNT') or 0)
    for index, (key, value) in enumerate(pins, first):
        env['GIT_CONFIG_KEY_%d' % index], env['GIT_CONFIG_VALUE_%d' % index] = key, value
    env['GIT_CONFIG_COUNT'] = str(first + len(pins))

    def run(command, allowed=(0,), input_data=None, extra=None, binary=False, limit=180, errors=False):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('Records commit ran past its time limit')
        result = subprocess.run(command, cwd=root, env=dict(env, **(extra or {})), stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, timeout=min(limit, remaining), input=input_data)
        if result.returncode not in allowed:
            raise GitFailed('Failed: ' + command[0], result.stderr.decode('utf-8', errors='replace')[-2000:])
        output = result.stdout if binary else result.stdout.decode('utf-8', errors='replace')
        if errors:
            return output, result.returncode, result.stderr.decode('utf-8', errors='replace')
        return output, result.returncode

    def git(*args, allowed=(0,), input_data=None, extra=None):
        output, code = run(['git', '-c', 'core.hooksPath=/dev/null', '-c', 'commit.gpgsign=false', *args],
                           allowed, input_data, extra)
        return output.strip('\n'), code

    def network(*args, allowed=(0,)):
        return git(*args, allowed=allowed, extra=host.git_env())

    def origin():
        current, absent = git('remote', 'get-url', 'origin', allowed=(0, 2))
        if absent:
            git('remote', 'add', 'origin', remote)
        elif current != remote:
            git('remote', 'set-url', 'origin', remote)

    def remote_head():
        listing, code = network('ls-remote', '--exit-code', 'origin', branch, allowed=(0, 2))
        return listing.split()[0] if not code and listing else None

    def leaks(span):
        """Run gitleaks on the commits of `span` and return the first finding that blocks, or None.
        Gitleaks exits 0 on some git errors, so its commit count must equal git's own count of the commits
        it scans: those whose `git log -p` diff has a hunk in a file that is not deleted. Merges and commits
        that only delete files, rename or change modes have none. A binary file counts here but gitleaks
        reads only archives, so a binary commit refuses the push rather than pass unscanned. Torii writes
        only single-parent commits, and gitleaks cannot see content added in a merge, so a merge refuses.
        The env pins the log config that gitleaks' git inherits, so repo or global config cannot hide
        the root commit or colour the diff."""
        merges, _ = git('rev-list', '--merges', '--count', span)
        if int(merges):
            raise PushRefused('scan_incomplete')
        report, code, errors = run([scanner, 'git', '--log-opts=' + span, '--redact=100', '--no-banner', '--no-color',
                                    '--ignore-gitleaks-allow', '--gitleaks-ignore-path=/dev/null', '--report-format=json',
                                    '--report-path=-', str(root)], allowed=(0, 1), limit=SYNC_LIMIT, errors=True)
        counted = re.findall(r'\b(\d+) commits scanned', errors)
        if not counted or int(counted[-1]) != scanned_commits(span):
            raise PushRefused('scan_incomplete')
        if not code:
            return None
        findings = json.loads(report)
        if not isinstance(findings, list) or not findings:
            raise RuntimeError('Gitleaks returned no findings for a failed scan')
        specs = ['%s:%s' % (item.get('Commit'), item.get('File')) for item in findings]
        content = blobs([spec for spec in specs if not spec.startswith('None:')])
        for item, spec in zip(findings, specs):
            if gitleaks_blocked(json.dumps([item]), lambda finding: content.get(spec) or ''):
                return item.get('File', 'unknown file')
        return None

    def scanned_commits(span):
        """How many commits of `span` gitleaks 8 must scan: each with a changed line or a binary file
        outside the files it deletes. Gitleaks counts a commit when it reads a hunk of such a file."""
        listing, _ = git('log', '--format=tformat:commit', '--numstat', '--diff-filter=d', span)
        commits, current = 0, False
        for line in listing.splitlines():
            if line == 'commit':
                current = False
            elif line and not current:
                added, deleted, _ = line.split('\t', 2)
                if (added, deleted) != ('0', '0'):
                    commits, current = commits + 1, True
        return commits

    def blobs(specs):
        """Read many objects with one cat-file. A missing object reads as None."""
        if not specs:
            return {}
        output, _ = run(['git', 'cat-file', '--batch'], input_data=''.join(spec + '\n' for spec in specs).encode(), binary=True)
        values, position = {}, 0
        for spec in specs:
            end = output.index(b'\n', position)
            header = output[position:end].split()
            position = end + 1
            if len(header) != 3 or header[1] != b'blob':
                values[spec] = None
                continue
            size = int(header[2])
            values[spec] = output[position:position + size].decode('utf-8', errors='replace')
            position += size + 1
        return values

    def known_in(span):
        """The first file whose blob in `span` holds a known value, or None. The values stay in memory."""
        if not known:
            return None
        listing, _ = git('rev-list', '--objects', '--filter=object:type=blob', *span)
        found = [line.split(' ', 1) for line in listing.splitlines() if ' ' in line]
        for start in range(0, len(found), CHUNK):
            part = found[start:start + CHUNK]
            content = blobs([sha for sha, _ in part])
            for sha, name in part:
                if any(form in (content[sha] or '') for form in known):
                    return name
        return None

    def taken():
        """True when a job record on install/<name> has a job id that this install's store gives another uid."""
        listing, _ = git('ls-tree', '-r', '-z', '--name-only', tracking)
        paths = [name for name in listing.split('\0') if re.fullmatch(r'chat-[^/]+/[0-9]+\.md', name)]
        theirs = {}
        for start in range(0, len(paths), CHUNK):
            for text in blobs([tracking + ':' + name for name in paths[start:start + CHUNK]]).values():
                data = front_matter(text or '')
                theirs[data.get('id')] = data.get('uid')
        if not theirs:
            return False
        database = sqlite3.connect((directory / 'state.sqlite').as_uri() + '?mode=ro', uri=True)
        try:
            ours = dict(database.execute('SELECT id,uid FROM tasks'))
        finally:
            database.close()
        return any(ours.get(job) != uid for job, uid in theirs.items())

    def index_info(entries):
        return ''.join('%s %s\t%s\0' % ((mode, sha, name) if sha != ZERO else ('0', ZERO, name))
                       for mode, sha, name in entries).encode()

    def restore_missing():
        deleted, _ = git('ls-files', '--deleted', '-z')
        paths = [name for name in deleted.split('\0') if name]
        if paths:
            git('checkout', '--', *paths)
        return paths

    def remove_journals(groups):
        """Remove the clean, committed journals of closed jobs in one commit. Return the parent commit per group."""
        head, missing = git('rev-parse', '--verify', '-q', 'HEAD', allowed=(0, 1))
        if missing or not groups:
            return {}
        with write_lock(directory):
            listing, _ = git('ls-files', '-z', '--', JOURNAL_SPEC)
            tracked = {}
            for name in filter(None, listing.split('\0')):
                tracked.setdefault(group_of(name), []).append(name)
            dirty, _ = git('ls-files', '-z', '--modified', '--others', '--deleted', '--', JOURNAL_SPEC)
            dirty = {group_of(name) for name in filter(None, dirty.split('\0'))}
            ready = [group for group in groups if group not in dirty]
            names = [name for group in ready for name in tracked.get(group, [])]
            if names:
                staged, _ = git('diff', '--cached', '--name-only', '-z', '--', *names)
                if staged.strip('\0'):
                    return {}
                paths = ''.join(name + '\0' for name in names).encode()
                git('read-tree', head, extra=scratch)
                git('update-index', '-z', '--force-remove', '--stdin', input_data=paths, extra=scratch)
                tree, _ = git('write-tree', extra=scratch)
                jobs = [group for group in ready if tracked.get(group)]
                message = 'Journals of %d closed job%s leave the tree' % (len(jobs), '' if len(jobs) == 1 else 's')
                commit, _ = git('commit-tree', tree, '-p', head, '-m', message)
                git('update-ref', 'HEAD', commit, head)
                git('update-index', '-z', '--force-remove', '--stdin', input_data=paths)
                for name in names:
                    (root / name).unlink(missing_ok=True)
                result['commits'] += 1
            saved = blobs(['%s:%s.md' % (head, group) for group in ready if tracked.get(group)])
            def outcome(group):
                try:
                    return parse(saved.get('%s:%s.md' % (head, group)) or '')[1]['outcome']
                except (ValueError, KeyError):
                    return None
            return {group: dict(log=head, outcome=outcome(group)) if tracked.get(group) else None for group in ready}

    def recover_journals():
        """Transfer local entries past the remote parts before restoring overlapping paths."""
        checkpoint = root / '.git' / 'torii-recover-journal'
        pending = json.loads(checkpoint.read_text(encoding='utf-8')) if checkpoint.exists() else {}
        changed, _ = git('ls-files', '--modified', '--others', '-z', '--', JOURNAL_SPEC)
        changed_groups = {}
        for name in filter(None, changed.split('\0')):
            part = root / name
            if journal_part_number(part) is not None and part.is_file() and not part.is_symlink():
                changed_groups.setdefault(group_of(name), []).append(part)
        groups = {group: [] for group in changed_groups}
        if pending:
            groups.setdefault(pending['group'], [])
        if not groups:
            return
        tracked, _ = git('ls-files', '-z', '--', JOURNAL_SPEC)
        for name in filter(None, tracked.split('\0')):
            group = group_of(name)
            if group in groups and journal_part_number(Path(name)) is not None:
                groups[group].append(name)
        for group in sorted(groups, key=lambda group: group != pending.get('group')):
            tracked_parts = groups[group]
            if not tracked_parts:
                continue
            remote_end = max(journal_part_number(Path(name)) for name in tracked_parts)
            changed_paths = changed_groups.get(group, [])
            resuming = group == pending.get('group')
            if not resuming and all(journal_part_number(part) > remote_end for part in changed_paths):
                continue
            base = root / (group + '.jsonl')
            parts = [part for part in journal_parts(base) if part.is_file() and not part.is_symlink()]
            end = pending['end'] if resuming else max(remote_end, journal_part_number(parts[-1]))
            source = [part for part in parts if journal_part_number(part) <= end]
            copied = [part for part in parts if journal_part_number(part) > end]
            saved = blobs(['HEAD:' + name for name in tracked_parts])
            added = journal_merge_lines((content or '' for content in saved.values()), source, copied)
            if added:
                if not resuming:
                    replace(checkpoint, json.dumps({'group': group, 'end': end}) + '\n')
                if not copied:
                    journal_part_path(base, end + 1).touch(mode=0o600, exist_ok=False)
                append_journal_parts(base, added, JOURNAL_LIMIT)
            for part in copied:
                with part.open('rb') as stream:
                    os.fsync(stream.fileno())
            changed_source = set(changed_paths).intersection(source)
            overwritten = [name for name in tracked_parts if root / name in changed_source]
            if overwritten:
                git('checkout', '--', *overwritten)
            for part in changed_source.difference(root / name for name in overwritten):
                part.unlink()
            checkpoint.unlink(missing_ok=True)

    scanner = shutil.which('gitleaks')
    with (directory / 'records-sync.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if drift and (directory / 'state.sqlite').is_file():
            check_drift(directory, known, config_known)
        recover = root / '.git' / 'torii-recover'
        if not (root / '.git').exists():
            git('init', '-q', '-b', 'main')
            if host:
                recover.touch()
        if not (root / '.git').is_dir() or (root / '.git').is_symlink():
            raise ValueError('Records repository must own its Git directory')
        for stale in [root / '.git' / 'index.lock', root / '.git' / 'HEAD.lock', root / '.git' / 'packed-refs.lock',
                      root / '.git' / 'config.lock', *(root / '.git' / 'refs').rglob('*.lock')]:
            try:
                if time.time() - stale.stat().st_mtime > 600:
                    stale.unlink()
            except FileNotFoundError:
                pass
        for leftover in (root / '.git').glob('torii-index*'):
            leftover.unlink(missing_ok=True)
        result = {'commits': 0, 'blocked': [], 'restored': []}
        if recover.exists() and host and (push or not recover.read_text()):
            origin()
            head = remote_head()
            if head:
                network('fetch', '-q', 'origin', '+%s:%s' % (branch, tracking))
            if head and taken():
                git('update-ref', '-d', tracking)
                recover.write_text('install_name_taken\n')
                result['install_taken'] = True
            elif head:
                git('reset', '-q', '--mixed', tracking)
                result['recovered'] = head
                with write_lock(directory):
                    recover_journals()
                    restore_missing()
            if not result.get('install_taken'):
                recover.unlink()
        if install:
            ledger = json.dumps({'format': 2, 'install': install}) + '\n'
            try:
                current = (root / 'ledger.json').read_text(encoding='utf-8')
            except (OSError, UnicodeError):
                current = None
            if current != ledger:
                replace(root / 'ledger.json', ledger)
        if not result.get('recovered'):
            with write_lock(directory):
                result['restored'] = restore_missing()
        docs = documents()
        changed, _ = git('ls-files', '-z', '--others', '--modified', '--', *TRACKED, *(docs.TRACKED if docs else ()))
        if changed.strip('\0'):
            git('add', '--ignore-removal', '--pathspec-from-file=-', '--pathspec-file-nul', input_data=changed.encode())
        raw, _ = git('diff', '--cached', '--raw', '-z', '--no-renames', '--no-abbrev')
        fields = raw.split('\0')
        staged = {}
        for meta, filename in zip(fields[0::2], fields[1::2]):
            old_mode, new_mode, old_sha, new_sha = meta.lstrip(':').split()[:4]
            staged[filename] = ((old_mode, old_sha, filename), (new_mode, new_sha, filename))
        groups = {}
        for filename in staged:
            groups.setdefault(group_of(filename), []).append(filename)
        head, missing = git('rev-parse', '--verify', '-q', 'HEAD', allowed=(0, 1))
        bulk = missing or len(groups) > BULK
        order = sorted(groups)
        scratch = {'GIT_INDEX_FILE': str(root / '.git' / ('torii-index-%d' % os.getpid()))}
        try:
            while order:
                if result['commits'] and deadline - time.monotonic() < SYNC_LIMIT / 4:
                    break
                chunk, order = (order[:CHUNK], order[CHUNK:]) if bulk else (order, [])
                files = [name for group in chunk for name in groups[group]]
                specs = [staged[name][1][1] for name in files] + [
                    'HEAD:' + group + '.md' for group in chunk if group + '.md' not in staged and not missing]
                content = blobs(specs)
                text = {name: content[staged[name][1][1]] or '' for name in files}
                blocked = {group for group in chunk if any(
                    scan_content(text[name], name) or any(form in text[name] for form in known) for name in groups[group])}
                if bulk or scanner:
                    git('read-tree', *(('--empty',) if missing else (head,)), extra=scratch)
                    git('update-index', '-z', '--index-info', input_data=index_info(staged[name][1] for name in files),
                        extra=scratch)
                if scanner:
                    report, code = run([scanner, 'git', '--staged', '--redact=100', '--no-banner', '--no-color',
                                        '--ignore-gitleaks-allow', '--gitleaks-ignore-path=/dev/null', '--report-format=json',
                                        '--report-path=-', str(root)], allowed=(0, 1), extra=scratch)
                    if code:
                        findings = json.loads(report)
                        if not isinstance(findings, list) or not findings:
                            raise RuntimeError('Gitleaks returned no findings for a failed scan')
                        for finding in findings:
                            name = finding.get('File', '')
                            if gitleaks_blocked(json.dumps([finding]), lambda item: text.get(item.get('File'), '')):
                                group = group_of(name)
                                if group not in chunk:
                                    raise RuntimeError('Gitleaks reported an unknown file')
                                blocked.add(group)
                for group in sorted(blocked):
                    git('reset', '-q', '--', *groups[group])
                    result['blocked'].append(group)
                chunk = [group for group in chunk if group not in blocked]
                if not chunk:
                    continue
                if bulk:
                    if blocked:
                        git('update-index', '-z', '--index-info', extra=scratch, input_data=index_info(
                            staged[name][0] for group in blocked for name in groups[group]))
                    commits = [(chunk, 'Import records' if missing else 'Update %d records' % len(chunk))]
                else:
                    commits = [([group], commit_message(group, text.get(group + '.md') or content.get('HEAD:' + group + '.md') or ''))
                               for group in chunk]
                for selected, message in commits:
                    if not bulk:
                        git('read-tree', head, extra=scratch)
                        git('update-index', '-z', '--index-info', extra=scratch,
                            input_data=index_info(staged[name][1] for name in groups[selected[0]]))
                    tree, _ = git('write-tree', extra=scratch)
                    commit, _ = git('commit-tree', tree, *(() if missing else ('-p', head)), '-m', message)
                    git('update-ref', 'HEAD', commit, '' if missing else head)
                    head, missing = commit, False
                    result['commits'] += 1
            result['compacted'] = remove_journals(list(compact))
        finally:
            Path(scratch['GIT_INDEX_FILE']).unlink(missing_ok=True)
        if result['commits']:
            try:
                counts, _ = git('count-objects', '-v')
                if int(dict(line.split(': ', 1) for line in counts.splitlines())['count']) > GC_AUTO:
                    git('repack', '-d', '-q')
                git('-c', 'gc.autoDetach=false', 'gc', '--auto', '--quiet')
            except (OSError, RuntimeError, ValueError, KeyError, subprocess.SubprocessError):
                result['gc_failed'] = True
        head, missing = git('rev-parse', '--verify', '-q', 'HEAD', allowed=(0, 1))
        result['sha'] = None if missing else head
        if missing or not remote:
            return result
        _, untracked = git('rev-parse', '--verify', '-q', tracking, allowed=(0, 1))
        count, _ = git('rev-list', '--count', 'HEAD' if untracked else tracking + '..HEAD')
        result['ahead'] = int(count)
        if not push or not result['ahead']:
            return result
        try:
            if not host:
                raise PushRefused(unknown)
            if recover.exists():
                raise PushRefused('install_name_taken')
            others, _ = git('for-each-ref', '--format=%(refname)', 'refs/torii/install/')
            if any(name != 'refs/torii/install/' + install for name in others.split()):
                raise PushRefused('install_changed')
            if not scanner:
                raise PushRefused('gitleaks_missing')
            origin()
            if host.check_private(run) != 'private':
                raise PushRefused('not_private')
            current = remote_head()
            if current:
                _, absent = git('cat-file', '-e', current + '^{commit}', allowed=(0, 1, 128))
                if absent:
                    network('fetch', '-q', 'origin', branch)
                _, behind = git('merge-base', '--is-ancestor', current, 'HEAD', allowed=(0, 1, 128))
                if behind:
                    raise PushRefused('diverged')
            _, unscanned = git('rev-parse', '--verify', '-q', 'refs/torii/scanned', allowed=(0, 1))
            found = leaks(head if unscanned else 'refs/torii/scanned..' + head) or known_in(
                [head] if unscanned or not known_seen else [head, '^refs/torii/scanned'])
            if found:
                result['history_blocked'] = found
                raise PushRefused('secret_in_history')
            git('update-ref', 'refs/torii/scanned', head)
            result['known_scanned'] = True
            network('push', '-q', 'origin', 'HEAD:' + branch)
        except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as error:
            result['push_error'] = host.reason(error) if host else str(error)
            return result
        git('update-ref', tracking, head)
        git('update-ref', 'refs/torii/install/' + install, head)
        result.update(ahead=0, pushed=timestamp(time.time()))
        return result


async def sync_async(directory, remote=None, install=None, push=False, known=(), known_seen=False, drift=False, config_known=None,
                     compact=()):
    process = await asyncio.create_subprocess_exec(sys.executable, '-m', 'coordinator.records',
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
    payload = json.dumps([str(directory), remote, install, push, list(known), known_seen, drift, config_known,
                          list(compact)]).encode()
    try:
        output, _ = await process.communicate(payload)
    except BaseException:
        if process.returncode is None:
            process.kill()
            await process.wait()
        raise
    if process.returncode:
        raise RuntimeError('Records commit subprocess failed')
    result = json.loads(output)
    if 'error' in result:
        raise RuntimeError('Records commit failed: ' + str(result['error']))
    return result


CLEAN_STEP = ('to clean it: rotate the value, remove it from that file, delete branch install/%s on the remote '
              'if it is there, then move records/.git to the Trash')
TAKEN_STEP = 'another install uses install/%s; choose another name with records.sync enabled=true install=NAME'


def closed_groups(store, limit=200):
    """Closed jobs whose record is written and whose journal can leave the tree, by records group."""
    rows = store.db.execute("""SELECT t.id,t.topic,t.number FROM record_compactions c JOIN tasks t ON t.id=c.task
        WHERE t.status IN ('done','dropped','expired') AND NOT EXISTS (SELECT 1 FROM stale_records s WHERE s.task=t.id)
        ORDER BY c.since LIMIT ?""", (limit,)).fetchall()
    return {str(journal_path(Path('.'), dict(row)).relative_to('records').with_suffix('')): row['id'] for row in rows}


def record_logs(store, groups, compacted):
    """Store the commit that holds each removed journal and the outcome the record had then, and end the queue entry."""
    if not compacted:
        return
    with store.transaction():
        for group, saved in compacted.items():
            task = groups.get(group)
            if task is None:
                continue
            if saved:
                store.db.execute('UPDATE tasks SET log=? WHERE id=?', (saved['log'], task))
                store.db.execute("""UPDATE task_iterations SET summary=COALESCE(summary,?) WHERE task=? AND k=(SELECT
                    iteration FROM tasks WHERE id=? AND status IN ('done','dropped','expired'))""", (saved['outcome'], task, task))
            store.db.execute('DELETE FROM record_compactions WHERE task=?', (task,))
            store.record_mark_stale(task)


async def commit_tick(store, state, now=None):
    """One committer tick: commit changed records, push when due, and keep status and problem rows current.
    A vault that cannot load refuses the push. The known values pass a full-history check once per process
    and again whenever they change."""
    now = time.monotonic() if now is None else now
    if now < state.get('next', 0):
        return None
    remote = store.get('records_sync_remote') if store.get('records_sync', False) is True else None
    install = store.get('records_sync_install')
    push = bool(remote and install) and now >= state.get('next_push', 0)
    status = store.get('records_sync_status', {})
    try:
        from .store import _known_token_scrubber
        from .vault import VaultError
        write_chats(store)
        try:
            known, vault_failed = [form for form, _ in _known_token_scrubber(store.vault, strict=True).forms], False
            config_known = [form for form, _ in _known_token_scrubber(store.vault, strict=True, min_length=1).forms]
        except VaultError:
            known, vault_failed = [form for form, _ in _known_token_scrubber(store.vault).forms], True
        if not vault_failed:
            from .scrub import Scrubber
            scrubber = Scrubber.of((value, '[token]') for value in config_known)
            try:
                files = config_files(store, scrubber)
                fingerprint = hashlib.sha256(json.dumps(files, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
                if store.get('records_config_projection') != fingerprint:
                    write_config(store, scrubber)
                    with store.db:
                        store.put('records_config_projection', fingerprint)
            except (OSError, ValueError, UnicodeError) as error:
                problems.record(store, 'records', 'config-failed', type(error).__name__, every=3600)
        digest = hashlib.sha256('\0'.join(sorted(known)).encode()).hexdigest()
        closing = closed_groups(store)
        result = await sync_async(store.directory, remote, install, push and not vault_failed, known,
                                  state.get('known_seen') == digest, not vault_failed, config_known if not vault_failed else None,
                                  list(closing))
        record_logs(store, closing, result.get('compacted') or {})
        if push and vault_failed and result.get('ahead'):
            result['push_error'] = 'vault_unavailable'
    except Exception as error:
        retry = state.get('retry', 60)
        state.update(next=now + retry, retry=min(retry * 2, 3600))
        status['error'] = 'commit failed; retrying'
        with store.db:
            store.put('records_sync_status', status)
        problems.record(store, 'records', 'commit-failed', internal_detail(error), every=3600)
        return None
    state.pop('next', None)
    state['retry'] = 60
    if push:
        if result.get('push_error'):
            retry = state.get('push_retry', 60)
            state.update(next_push=now + retry, push_retry=min(retry * 2, 3600))
            code = {'diverged': 'push-diverged', 'secret_in_history': 'push-blocked',
                    'install_name_taken': 'install-name-taken'}.get(result['push_error'], 'push-failed')
            detail = result['push_error']
            if result.get('history_blocked'):
                detail = '%s: %s' % (result['history_blocked'], CLEAN_STEP % install)
            elif detail == 'install_name_taken':
                detail = TAKEN_STEP % install
            problems.record(store, 'records', code, detail, every=3600)
        else:
            state.update(next_push=now + 60, push_retry=60)
        if result.get('known_scanned'):
            state['known_seen'] = digest
    for path in result.get('blocked', []):
        problems.record(store, 'records', 'commit-blocked', path, every=3600)
    for path in result.get('restored', []):
        problems.record(store, 'records', 'record-restored', path, every=3600)
    if result.get('recovered'):
        problems.record(store, 'records', 'records-recovered', 'install/%s at %s' % (install, result['recovered'][:12]))
    if result.get('gc_failed'):
        problems.record(store, 'records', 'gc-failed', 'git gc --auto failed or timed out', every=3600)
    status.pop('error', None)
    status.update(sha=result.get('sha'), blocked=result.get('blocked', []), ahead=result.get('ahead'))
    if result.get('commits'):
        status['last_commit'] = timestamp(time.time())
    if result.get('pushed'):
        status['last_push'] = result['pushed']
    if result.get('push_error'):
        status['error'] = 'push failed: ' + result['push_error']
    with store.db:
        store.put('records_sync_status', status)
    return result


if __name__ == '__main__':
    try:
        response = sync(*json.load(sys.stdin))
    except Exception as error:
        response = {'error': type(error).__name__}
    print(json.dumps(response))
