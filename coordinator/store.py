"""Durable owner intake, tasks, messages, and reports."""

import hashlib
import json
import logging
from pathlib import Path
import secrets
import sqlite3
import time
import re

from . import problems
from .formatting import FENCE, entities_to_markdown, markdown_to_html


COMMAND = re.compile(r'/[A-Za-z0-9_]{1,32}\Z')

SERVICE_EVENTS = ('forum_topic_edited', 'forum_topic_closed', 'forum_topic_reopened',
                  'general_forum_topic_hidden', 'general_forum_topic_unhidden',
                  'pinned_message', 'new_chat_members', 'left_chat_member', 'new_chat_title',
                  'new_chat_photo', 'delete_chat_photo', 'message_auto_delete_timer_changed')

logger = logging.getLogger(__name__)

LEGACY_TABLES = ('job_questions', 'worker_controls', 'control_calls', 'task_reactions',
                 'pending_replies', 'audit', 'jobs')


def retry_delay(attempts, retry_after=None):
    return retry_after if retry_after is not None else min(300, 2 ** min(attempts + 1, 8))


MESSAGE_LIMIT = 4096


def _open_fence_at(text, cut):
    opening = None
    position = 0
    for line in text.splitlines(keepends=True):
        if position >= cut:
            break
        fence = FENCE.fullmatch(line.rstrip('\r\n'))
        if fence:
            if opening is None:
                opening = (position, fence.group(1), fence.group(2) or '')
            elif fence.group(1)[0] == opening[1][0] and len(fence.group(1)) >= len(opening[1]):
                opening = None
        position += len(line)
    return opening


def _cut_parts(text, cut):
    opening = _open_fence_at(text, cut)
    if opening:
        marker, language = opening[1:]
        separator = 1 if text[cut:cut + 1] == '\n' else 0
        left = text[:cut] + '\n' + marker
        right = marker + language + '\n' + text[cut + separator:]
        return left, right
    return text[:cut].rstrip(), text[cut:].lstrip()


def split_message(text, limit=MESSAGE_LIMIT):
    """Split text into Telegram-sized parts at the last paragraph, line, or word break that fits."""
    parts = []
    while len(text) > limit:
        window = text[:limit]
        cut = next((found for found in (window.rfind(mark) for mark in ('\n\n', '\n', ' '))
                    if found > limit // 2), limit)
        opening = _open_fence_at(text, cut)
        if opening and opening[0] > 0 and text[:opening[0]].strip():
            cut = opening[0]
        elif opening:
            cut = min(cut, limit - len(opening[1]) - 1)
            first, text = _cut_parts(text, cut)
            parts.append(first)
            continue
        parts.append(text[:cut].rstrip())
        text = text[cut:].lstrip()
    parts.append(text)
    return parts


def _fit_rendered(text, limit):
    if len(markdown_to_html(text)) <= limit:
        return [text]
    lines = [match.start() for match in re.finditer('\n', text) if match.start() > 0]
    outside = [cut for cut in lines if _open_fence_at(text, cut) is None]
    inside = [cut for cut in lines if _open_fence_at(text, cut) is not None]
    for cut in list(reversed(outside)) + list(reversed(inside)):
        left, right = _cut_parts(text, cut)
        if len(right) < len(text) and len(markdown_to_html(left)) <= limit:
            return _fit_rendered(left, limit) + _fit_rendered(right, limit)
    low, high = 1, len(text) - 1
    while low < high:
        middle = (low + high + 1) // 2
        if len(markdown_to_html(_cut_parts(text, middle)[0])) <= limit:
            low = middle
        else:
            high = middle - 1
    left, right = _cut_parts(text, low)
    return [left] + _fit_rendered(right, limit)


def _split_rendered(text, limit):
    return [fitted for part in split_message(text, limit)
            for fitted in _fit_rendered(part, limit)]

class Store:
    def __init__(self, directory: Path, token_file=None, read_only=False):
        self.directory = directory.resolve()
        self.vault = None
        self.token_file = Path(token_file or Path.home() / '.config/telegram-agent-coordinator/bot-token').resolve()
        if read_only:
            self.db = sqlite3.connect((self.directory / 'state.sqlite').as_uri() + '?mode=ro', uri=True)
            self.db.row_factory = sqlite3.Row
            return
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        directory.chmod(0o700)
        usage = self.directory / 'USAGE.md'
        if not usage.exists():
            usage.write_text((Path(__file__).parent / 'defaults' / 'USAGE.md').read_text())
            usage.chmod(0o600)
        path = directory / "state.sqlite"
        self.db = sqlite3.connect(path)
        self.db.execute('PRAGMA busy_timeout=30000')
        path.chmod(0o600)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA secure_delete=ON')
        self.db.execute('PRAGMA foreign_keys=OFF')
        try:
            self.db.execute('BEGIN IMMEDIATE')
            schema = """
                CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS topics (
                    id TEXT PRIMARY KEY, chat INTEGER NOT NULL, thread INTEGER NOT NULL,
                    name TEXT NOT NULL, telegram_title TEXT, cwd TEXT NOT NULL, provider TEXT,
                    session TEXT, enabled INTEGER NOT NULL DEFAULT 0,
                    waiting_job INTEGER, source_pid INTEGER);
                CREATE TABLE IF NOT EXISTS updates (id INTEGER PRIMARY KEY);
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY, topic TEXT NOT NULL REFERENCES topics(id),
                    number INTEGER NOT NULL, title TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open','done','dropped')),
                    worktree TEXT, notes TEXT, created REAL NOT NULL, updated REAL NOT NULL,
                    UNIQUE(topic,number));
                CREATE TABLE IF NOT EXISTS workers (
                    id INTEGER PRIMARY KEY, task INTEGER REFERENCES tasks(id),
                    topic TEXT NOT NULL REFERENCES topics(id), provider TEXT NOT NULL,
                    prompt TEXT NOT NULL, session TEXT, cwd TEXT, workspace TEXT,
                    fresh INTEGER NOT NULL DEFAULT 1, status TEXT NOT NULL DEFAULT 'queued',
                    pid INTEGER, result TEXT, goal TEXT, model TEXT, effort TEXT NOT NULL DEFAULT 'medium', account_alias TEXT,
                    last_seq INTEGER NOT NULL DEFAULT 0,
                    created REAL NOT NULL, updated REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY, topic TEXT NOT NULL REFERENCES topics(id),
                    kind TEXT NOT NULL CHECK(kind IN ('owner','worker_result','restarted','callback','secret_filled')),
                    telegram_message INTEGER, text TEXT NOT NULL,
                    source_worker INTEGER REFERENCES workers(id), images TEXT,
                    source_envelope INTEGER REFERENCES envelopes(id),
                    delivered TEXT NOT NULL DEFAULT 'pending'
                        CHECK(delivered IN ('pending','sent','received','uncertain')),
                    receipt TEXT, reaction_state TEXT NOT NULL DEFAULT 'sent',
                    reaction_desired TEXT, reaction_sent TEXT, turn_started REAL,
                    turn_outbox INTEGER, turn_eyes INTEGER NOT NULL DEFAULT 0,
                    turn_tools INTEGER NOT NULL DEFAULT 0, reacted_to INTEGER,
                    reaction_retry REAL NOT NULL DEFAULT 0,
                    reaction_attempts INTEGER NOT NULL DEFAULT 0,
                    created REAL NOT NULL, updated REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS outbox (
                    id INTEGER PRIMARY KEY, topic TEXT NOT NULL REFERENCES topics(id),
                    kind TEXT NOT NULL, text TEXT NOT NULL, reply_to INTEGER,
                    delivered INTEGER NOT NULL DEFAULT 0, telegram_message INTEGER,
                    attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL NOT NULL DEFAULT 0,
                    reply_markup TEXT, image TEXT);
                CREATE TABLE IF NOT EXISTS attachments (
                    id INTEGER PRIMARY KEY, topic TEXT NOT NULL REFERENCES topics(id),
                    message INTEGER NOT NULL, file_id TEXT NOT NULL, mime TEXT NOT NULL,
                    size INTEGER, group_id TEXT, path TEXT, created REAL NOT NULL,
                    name TEXT, error TEXT, kind TEXT, UNIQUE(topic,message));
                CREATE TABLE IF NOT EXISTS service_requests (
                    id INTEGER PRIMARY KEY, op TEXT NOT NULL, params TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'queued', result TEXT,
                    created REAL NOT NULL, updated REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS envelopes (
                    id INTEGER PRIMARY KEY, name TEXT NOT NULL, reason TEXT NOT NULL, consumer TEXT NOT NULL,
                    task INTEGER REFERENCES tasks(id), topic TEXT NOT NULL REFERENCES topics(id),
                    token_hash TEXT, chat INTEGER, card_outbox INTEGER,
                    state TEXT NOT NULL CHECK(state IN ('open','armed','filled','superseded','cancelled','expired','revoked')),
                    created REAL NOT NULL, expires REAL NOT NULL, armed_at REAL, filled_at REAL,
                    length INTEGER, fingerprint TEXT, updated REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS envelope_events (
                    id INTEGER PRIMARY KEY, envelope INTEGER REFERENCES envelopes(id), name TEXT,
                    event TEXT NOT NULL CHECK(event IN ('ask','arm','fill','reject','use','cancel','expire','revoke','delete_failed')),
                    reason TEXT, worker INTEGER REFERENCES workers(id), source TEXT, created REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS envelope_effects (
                    id INTEGER PRIMARY KEY, envelope INTEGER REFERENCES envelopes(id),
                    kind TEXT NOT NULL CHECK(kind IN ('delete','reply')), chat INTEGER NOT NULL,
                    message INTEGER, text TEXT, attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt REAL NOT NULL DEFAULT 0,
                    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','done','failed')),
                    created REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS problems (
                    id INTEGER PRIMARY KEY, created REAL NOT NULL, area TEXT NOT NULL, code TEXT NOT NULL,
                    detail TEXT, topic TEXT, task INTEGER, worker INTEGER, message INTEGER, attachment INTEGER);
            """
            for statement in schema.split(';'):
                if statement.strip():
                    self.db.execute(statement)
            self._migrate_legacy()
            self._migrate_mode()
            for table, column in (('envelope_effects', 'thread'), ('envelopes', 'arm_thread')):
                if column not in self._columns(table):
                    self.db.execute('ALTER TABLE ' + table + ' ADD COLUMN ' + column + ' INTEGER')
            if 'telegram_title' not in self._columns('topics'):
                self.db.execute('ALTER TABLE topics ADD COLUMN telegram_title TEXT')
            for column in ('name', 'error', 'kind'):
                if column not in self._columns('attachments'):
                    self.db.execute('ALTER TABLE attachments ADD COLUMN ' + column + ' TEXT')
            if 'last_seq' not in self._columns('workers'):
                self.db.execute('ALTER TABLE workers ADD COLUMN last_seq INTEGER NOT NULL DEFAULT 0')
            if 'account_alias' not in self._columns('workers'):
                self.db.execute('ALTER TABLE workers ADD COLUMN account_alias TEXT')
            if 'host' not in self._columns('workers'):
                self.db.execute('ALTER TABLE workers ADD COLUMN host TEXT')
            if 'effort' not in self._columns('workers'):
                self.db.execute("ALTER TABLE workers ADD COLUMN effort TEXT NOT NULL DEFAULT 'medium'")
            if 'origin' not in self._columns('tasks'):
                self.db.execute('ALTER TABLE tasks ADD COLUMN origin INTEGER REFERENCES messages(id)')
            if 'work' not in self._columns('workers'):
                self.db.execute("ALTER TABLE workers ADD COLUMN work TEXT NOT NULL DEFAULT 'research'")
            if 'reaction_desired' not in self._columns('messages'):
                self.db.execute('ALTER TABLE messages ADD COLUMN reaction_desired TEXT')
                self.db.execute('ALTER TABLE messages ADD COLUMN reaction_sent TEXT')
                self.db.execute('''UPDATE messages SET reaction_desired=CASE WHEN reaction_state='sent' THEN '👀' END,
                    reaction_sent=CASE WHEN reaction_state='sent' THEN '👀' END,
                    reaction_state=CASE WHEN reaction_state='pending' THEN 'sent' ELSE reaction_state END''')
            for name, definition in (('turn_started', 'REAL'), ('turn_outbox', 'INTEGER'),
                                     ('turn_eyes', 'INTEGER NOT NULL DEFAULT 0'),
                                     ('turn_tools', 'INTEGER NOT NULL DEFAULT 0'), ('reacted_to', 'INTEGER')):
                if name not in self._columns('messages'):
                    self.db.execute('ALTER TABLE messages ADD COLUMN ' + name + ' ' + definition)
            for name, definition in (('retired', 'INTEGER NOT NULL DEFAULT 0'),
                                     ('reaction_desired', 'TEXT'), ('reaction_sent', 'TEXT'),
                                     ('reaction_state', "TEXT NOT NULL DEFAULT 'sent'"),
                                     ('reaction_retry', 'REAL NOT NULL DEFAULT 0'),
                                     ('reaction_attempts', 'INTEGER NOT NULL DEFAULT 0')):
                if name not in self._columns('outbox'):
                    self.db.execute('ALTER TABLE outbox ADD COLUMN ' + name + ' ' + definition)
            message_schema = self.db.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='messages'").fetchone()[0]
            if ('source_worker INTEGER UNIQUE' in message_schema or
                    'secret_filled' not in message_schema or 'source_envelope' not in self._columns('messages')):
                self.db.execute('''CREATE TABLE messages_new (
                    id INTEGER PRIMARY KEY, topic TEXT NOT NULL REFERENCES topics(id),
                    kind TEXT NOT NULL CHECK(kind IN ('owner','worker_result','restarted','callback','secret_filled')),
                    telegram_message INTEGER, text TEXT NOT NULL,
                    source_worker INTEGER REFERENCES workers(id), images TEXT,
                    source_envelope INTEGER REFERENCES envelopes(id),
                    delivered TEXT NOT NULL DEFAULT 'pending'
                        CHECK(delivered IN ('pending','sent','received','uncertain')),
                    receipt TEXT, reaction_state TEXT NOT NULL DEFAULT 'sent',
                    reaction_desired TEXT, reaction_sent TEXT, turn_started REAL,
                    turn_outbox INTEGER, turn_eyes INTEGER NOT NULL DEFAULT 0,
                    turn_tools INTEGER NOT NULL DEFAULT 0, reacted_to INTEGER,
                    reaction_retry REAL NOT NULL DEFAULT 0,
                    reaction_attempts INTEGER NOT NULL DEFAULT 0,
                    created REAL NOT NULL, updated REAL NOT NULL)''')
                columns = ','.join(self._columns('messages'))
                self.db.execute('INSERT INTO messages_new (' + columns + ') SELECT ' + columns + ' FROM messages')
                self.db.execute('DROP TABLE messages')
                self.db.execute('ALTER TABLE messages_new RENAME TO messages')
            worker_index = self.db.execute(
                "SELECT sql FROM sqlite_master WHERE type='index' AND name='messages_workers'").fetchone()
            if worker_index and worker_index['sql'] != 'CREATE INDEX messages_workers ON messages(source_worker)':
                self.db.execute('DROP INDEX messages_workers')
            for statement in (
                'CREATE UNIQUE INDEX IF NOT EXISTS messages_telegram ON messages(topic,telegram_message) WHERE telegram_message IS NOT NULL',
                'CREATE INDEX IF NOT EXISTS messages_workers ON messages(source_worker)',
                'CREATE UNIQUE INDEX IF NOT EXISTS messages_envelopes ON messages(source_envelope) WHERE source_envelope IS NOT NULL',
                'CREATE INDEX IF NOT EXISTS workers_tasks ON workers(task,id)',
                'CREATE INDEX IF NOT EXISTS workers_states ON workers(status,id)',
                'CREATE INDEX IF NOT EXISTS attachment_groups ON attachments(topic,group_id)',
                'CREATE INDEX IF NOT EXISTS outbox_messages ON outbox(telegram_message,topic) WHERE delivered=1',
                'CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(id) WHERE delivered=0',
                'CREATE INDEX IF NOT EXISTS outbox_topic_pending ON outbox(topic,id) WHERE delivered=0',
                "CREATE UNIQUE INDEX IF NOT EXISTS envelopes_live_name ON envelopes(name) WHERE state IN ('open','armed')",
                "CREATE UNIQUE INDEX IF NOT EXISTS envelopes_filled_name ON envelopes(name) WHERE state='filled'",
                "CREATE UNIQUE INDEX IF NOT EXISTS envelopes_armed_chat ON envelopes(chat) WHERE state='armed'",
                'CREATE UNIQUE INDEX IF NOT EXISTS envelopes_token ON envelopes(token_hash) WHERE token_hash IS NOT NULL',
                'CREATE INDEX IF NOT EXISTS problems_created ON problems(created)',
            ):
                self.db.execute(statement)
            self.db.execute("""UPDATE workers SET status='interrupted',updated=?
                WHERE status='needs_input' AND (NOT EXISTS (
                    SELECT 1 FROM tasks t WHERE t.id=workers.task AND t.status='open')
                OR EXISTS (SELECT 1 FROM workers newer WHERE newer.task=workers.task
                    AND newer.id>workers.id))""", (time.time(),))
            problems = self.db.execute('PRAGMA foreign_key_check').fetchall()
            if problems:
                raise sqlite3.IntegrityError('Migration left invalid foreign keys')
            self.db.commit()
        except Exception:
            self.db.rollback()
            self.db.execute('PRAGMA foreign_keys=ON')
            self.db.close()
            raise
        self.db.execute('PRAGMA foreign_keys=ON')

    def _migrate_mode(self):
        if self.get('mode') is None and self.get('group') is not None:
            self.put('mode', 'group')
        from .setup_flow import restore_control_topic
        restore_control_topic(self)

    def chat(self):
        return self.get('group')

    def _columns(self, table):
        return {row['name'] for row in self.db.execute('PRAGMA table_info(' + table + ')')}

    def _migrate_legacy(self):
        if 'reply_markup' not in self._columns('outbox'):
            self.db.execute('ALTER TABLE outbox ADD COLUMN reply_markup TEXT')
        if 'edit_message' not in self._columns('outbox'):
            self.db.execute('ALTER TABLE outbox ADD COLUMN edit_message INTEGER')
        if 'secrets' not in self._columns('tasks'):
            self.db.execute('ALTER TABLE tasks ADD COLUMN secrets TEXT')
        if 'image' not in self._columns('outbox'):
            self.db.execute('ALTER TABLE outbox ADD COLUMN image TEXT')
        for name, definition in (
            ('source_worker', 'INTEGER REFERENCES workers(id)'),
            ('reaction_state', "TEXT NOT NULL DEFAULT 'pending'"),
            ('reaction_retry', 'REAL NOT NULL DEFAULT 0'),
            ('reaction_attempts', 'INTEGER NOT NULL DEFAULT 0'),
        ):
            if name not in self._columns('messages'):
                self.db.execute('ALTER TABLE messages ADD COLUMN ' + name + ' ' + definition)
        existing = {row['name'] for row in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if 'jobs' in existing:
            number = 'number' if 'number' in self._columns('jobs') else 'id'
            rows = self.db.execute(f"""SELECT id,topic,prompt,created,updated,{number} AS number
                FROM jobs WHERE status IN ('queued','running','working','held','stopped') ORDER BY id""").fetchall()
            for row in rows:
                title = (row['prompt'].splitlines() or [''])[0].strip() or 'Job ' + str(row['number'])
                self.db.execute("""INSERT INTO tasks(topic,number,title,notes,created,updated)
                    VALUES (?,?,?,?,?,?)""", (row['topic'], row['number'], title,
                    'migrated from job ' + str(row['number']), row['created'], row['updated']))
            logger.info('migrated legacy jobs count=%d', len(rows))
        worker_columns = self._columns('workers')
        if 'task' not in worker_columns:
            self.db.execute('ALTER TABLE workers ADD COLUMN task INTEGER REFERENCES tasks(id)')
        if 'jobs' in existing and 'job' in worker_columns:
            number = 'number' if 'number' in self._columns('jobs') else 'id'
            self.db.execute(f"""UPDATE workers SET task=(SELECT t.id FROM jobs j
                JOIN tasks t ON t.topic=j.topic AND t.number=j.{number} WHERE j.id=workers.job)
                WHERE job IS NOT NULL AND task IS NULL""")
        if 'job' in worker_columns or 'work_kind' in worker_columns or 'role' in worker_columns:
            for name in ('goal', 'model'):
                if name not in worker_columns:
                    self.db.execute('ALTER TABLE workers ADD COLUMN ' + name + ' TEXT')
            self.db.execute("""CREATE TABLE workers_new (
                id INTEGER PRIMARY KEY, task INTEGER REFERENCES tasks(id),
                topic TEXT NOT NULL REFERENCES topics(id), provider TEXT NOT NULL,
                prompt TEXT NOT NULL, session TEXT, cwd TEXT, workspace TEXT,
                fresh INTEGER NOT NULL DEFAULT 1, status TEXT NOT NULL DEFAULT 'queued',
                pid INTEGER, result TEXT, goal TEXT, model TEXT,
                created REAL NOT NULL, updated REAL NOT NULL)""")
            fields = ('id', 'task', 'topic', 'provider', 'prompt', 'session', 'cwd', 'workspace',
                      'fresh', 'status', 'pid', 'result', 'goal', 'model', 'created', 'updated')
            names = ','.join(fields)
            self.db.execute('INSERT INTO workers_new (' + names + ') SELECT ' + names + ' FROM workers')
            self.db.execute('DROP TABLE workers')
            self.db.execute('ALTER TABLE workers_new RENAME TO workers')
        for table, fields in (
            ('outbox', ('id', 'topic', 'kind', 'text', 'reply_to', 'delivered', 'telegram_message',
                        'attempts', 'next_attempt', 'reply_markup', 'image')),
            ('attachments', ('id', 'topic', 'message', 'file_id', 'mime', 'size', 'group_id', 'path', 'created')),
        ):
            if 'job' not in self._columns(table):
                continue
            if table == 'outbox':
                create = """CREATE TABLE outbox_new (
                    id INTEGER PRIMARY KEY, topic TEXT NOT NULL REFERENCES topics(id),
                    kind TEXT NOT NULL, text TEXT NOT NULL, reply_to INTEGER,
                    delivered INTEGER NOT NULL DEFAULT 0, telegram_message INTEGER,
                    attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL NOT NULL DEFAULT 0,
                    reply_markup TEXT, image TEXT)"""
            else:
                create = """CREATE TABLE attachments_new (
                    id INTEGER PRIMARY KEY, topic TEXT NOT NULL REFERENCES topics(id),
                    message INTEGER NOT NULL, file_id TEXT NOT NULL, mime TEXT NOT NULL,
                    size INTEGER, group_id TEXT, path TEXT, created REAL NOT NULL,
                    UNIQUE(topic,message))"""
            self.db.execute(create)
            names = ','.join(fields)
            self.db.execute('INSERT INTO ' + table + '_new (' + names + ') SELECT ' + names + ' FROM ' + table)
            self.db.execute('DROP TABLE '+table)
            self.db.execute('ALTER TABLE '+table+'_new RENAME TO '+table)
        for table in LEGACY_TABLES:
            self.db.execute('DROP TABLE IF EXISTS ' + table)

    def close(self):
        self.db.close()

    def backup_for_restart(self):
        folder = self.directory / 'backups'
        folder.mkdir(mode=0o700, exist_ok=True)
        folder.chmod(0o700)
        target = folder / ('state-before-restart-%d.sqlite' % time.time_ns())
        copy = sqlite3.connect(str(target))
        try:
            target.chmod(0o600)
            self.db.backup(copy)
        except BaseException:
            copy.close()
            target.unlink()
            raise
        copy.close()
        for old in sorted(folder.glob('state-before-restart-*.sqlite'))[:-5]:
            old.unlink()
        return target

    def get(self, key, default=None):
        row = self.db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def put(self, key, value):
        self.db.execute("INSERT OR REPLACE INTO settings VALUES (?,?)", (key, json.dumps(value)))

    def pairing_code(self):
        code = secrets.token_urlsafe(18)
        with self.db:
            self.put("pairing", {"hash": hashlib.sha256(code.encode()).hexdigest(),
                                 "expires": time.time() + 1800})
        return code

    def envelope_event(self, envelope, name, event, reason=None, worker=None, source=None):
        """Insert one audit row. The caller owns the transaction."""
        self.db.execute('''INSERT INTO envelope_events(envelope,name,event,reason,worker,source,created)
            VALUES (?,?,?,?,?,?,?)''', (envelope, name, event, reason, worker, source, time.time()))

    def topic(self, topic_id):
        row = self.db.execute("SELECT * FROM topics WHERE id=?", (topic_id,)).fetchone()
        return dict(row) if row else None

    def topics(self):
        return [dict(r) for r in self.db.execute("SELECT * FROM topics ORDER BY id")]

    def task_create(self, topic, title, worktree=None, notes=None, secrets=None, origin=None):
        if not self.topic(topic):
            raise ValueError('Unknown channel')
        if not isinstance(title, str) or not title.strip():
            raise ValueError('Job title is required')
        now = time.time()
        with self.db:
            row = self.db.execute('''INSERT INTO tasks(topic,number,title,worktree,notes,secrets,origin,created,updated)
                VALUES (?,(SELECT COALESCE(MAX(number),0)+1 FROM tasks WHERE topic=?),?,?,?,?,?,?,?)''',
                                  (topic, topic, title.strip(), worktree, notes,
                                   json.dumps(secrets) if secrets else None, origin, now, now))
            from .reactions import set_desired
            set_desired(self, origin, '👀')
        return self.task_get(row.lastrowid)

    def task_update(self, task_id, **fields):
        allowed = {'title', 'status', 'worktree', 'notes', 'secrets'}
        if not fields or set(fields) - allowed:
            raise ValueError('Unknown job field')
        if 'status' in fields and fields['status'] not in ('open', 'done', 'dropped'):
            raise ValueError('Job status must be open, done, or dropped')
        if 'title' in fields and (not isinstance(fields['title'], str) or not fields['title'].strip()):
            raise ValueError('Job title is required')
        task = self.task_get(task_id)
        if not task:
            raise ValueError('Unknown job')
        if 'title' in fields:
            fields['title'] = fields['title'].strip()
        if 'secrets' in fields:
            fields['secrets'] = json.dumps(fields['secrets']) if fields['secrets'] else None
        fields['updated'] = time.time()
        assignments = ','.join(name + '=?' for name in fields)
        with self.db:
            self.db.execute('UPDATE tasks SET ' + assignments + ' WHERE id=?',
                            (*fields.values(), task_id))
            if 'status' in fields and fields['status'] != task['status']:
                from .reactions import set_desired
                set_desired(self, task['origin'], {'open': '👀', 'done': '👌', 'dropped': None}[fields['status']])
            if fields.get('status') in ('done', 'dropped'):
                self.db.execute("""UPDATE workers SET status='interrupted',updated=?
                    WHERE task=? AND status IN ('needs_input','waiting_for_quota')""", (time.time(), task_id))
        return self.task_get(task_id)

    @staticmethod
    def _task(row):
        task = dict(row)
        task['secrets'] = json.loads(task['secrets']) if task.get('secrets') else []
        return task

    def task_get(self, task_id):
        row = self.db.execute('SELECT * FROM tasks WHERE id=?', (task_id,)).fetchone()
        return self._task(row) if row else None

    def tasks_list(self, topic=None, status=None):
        if status is not None and status not in ('open', 'done', 'dropped'):
            raise ValueError('Job status must be open, done, or dropped')
        return [self._task(row) for row in self.db.execute(
            'SELECT * FROM tasks WHERE (? IS NULL OR topic=?) AND (? IS NULL OR status=?) ORDER BY topic,number',
            (topic, topic, status, status))]

    def message_save(self, topic, kind, text, telegram_message=None, images=None, reacted_to=None):
        if kind not in ('owner', 'worker_result', 'restarted', 'callback', 'secret_filled'):
            raise ValueError('Unknown message kind')
        if not self.topic(topic):
            raise ValueError('Unknown channel')
        now = time.time()
        encoded = json.dumps(images) if images is not None else None
        with self.db:
            self.db.execute('''INSERT OR IGNORE INTO messages
                (topic,kind,telegram_message,text,images,reacted_to,reaction_state,created,updated) VALUES (?,?,?,?,?,?,'sent',?,?)''',
                            (topic, kind, telegram_message, text, encoded, reacted_to, now, now))
            row = self.db.execute('''SELECT * FROM messages WHERE topic=? AND telegram_message=?''',
                                  (topic, telegram_message)).fetchone() if telegram_message is not None else None
            if row is None:
                row = self.db.execute('SELECT * FROM messages WHERE id=last_insert_rowid()').fetchone()
        return dict(row)

    def messages_pending(self):
        return [dict(row) for row in self.db.execute(
            "SELECT * FROM messages WHERE delivered='pending' ORDER BY id")]

    def message_secret_filled(self, topic, envelope, text):
        now = time.time()
        self.db.execute('''INSERT OR IGNORE INTO messages
            (topic,kind,text,source_envelope,created,updated) VALUES (?,'secret_filled',?,?,?,?)''',
                        (topic, text, envelope, now, now))
        row = self.db.execute('SELECT * FROM messages WHERE source_envelope=?', (envelope,)).fetchone()
        return dict(row)

    def message_sending(self, message_id):
        with self.db:
            cursor = self.db.execute("UPDATE messages SET delivered='sent',updated=? WHERE id=? AND delivered='pending'",
                                     (time.time(), message_id))
        return bool(cursor.rowcount)

    def requeue_messages(self, message_ids):
        if not message_ids:
            return
        with self.db:
            self.db.executemany('''UPDATE messages SET delivered='pending',receipt=NULL,updated=?
                WHERE id=? AND delivered IN ('sent','received','uncertain')''',
                                [(time.time(), message_id) for message_id in message_ids])

    def messages_uncertain(self):
        with self.db:
            self.db.execute("""UPDATE messages SET delivered='uncertain',receipt='service_restarted',updated=?
                WHERE delivered='sent' OR delivered='received' AND receipt='written'""",
                            (time.time(),))

    def messages_confirm(self, message_ids, receipt):
        """Mark messages received once the native session shows them, including ones left uncertain."""
        with self.db:
            self.db.executemany('''UPDATE messages SET delivered='received',receipt=?,updated=?
                WHERE id=? AND delivered IN ('sent','received','uncertain')''',
                                [(receipt, time.time(), message_id) for message_id in message_ids])

    def messages_unconfirmed(self, message_ids, receipt):
        """A written message the session never replayed before it closed may not be in the conversation."""
        with self.db:
            self.db.executemany("""UPDATE messages SET delivered='uncertain',receipt=?,updated=?
                WHERE id=? AND delivered='received'""",
                                [(receipt, time.time(), message_id) for message_id in message_ids])

    def message_delivered(self, message_id, receipt):
        state = ('uncertain' if receipt == 'uncertain' or
                 isinstance(receipt, dict) and receipt.get('status') == 'uncertain' else 'received')
        saved = receipt if isinstance(receipt, str) else json.dumps(receipt)
        with self.db:
            self.db.execute('''UPDATE messages SET delivered=?,receipt=?,updated=?
                WHERE id=? AND delivered IN ('pending','sent')''', (state, saved, time.time(), message_id))
        row = self.db.execute('SELECT * FROM messages WHERE id=?', (message_id,)).fetchone()
        return dict(row) if row else None

    def worker_complete(self, worker_id, result, status='done'):
        with self.db:
            if not self.db.in_transaction:
                self.db.execute('BEGIN IMMEDIATE')
            worker = self.db.execute('SELECT * FROM workers WHERE id=?', (worker_id,)).fetchone()
            if not worker:
                raise ValueError('Unknown worker')
            saved = self.db.execute('SELECT * FROM messages WHERE source_worker=? ORDER BY id DESC LIMIT 1',
                                    (worker_id,)).fetchone()
            if saved and saved['created'] >= worker['updated']:
                return dict(saved)
            now = time.time()
            payload = dict(result)
            if status == 'needs_input' and worker['task'] is not None and self.db.execute('''
                    SELECT EXISTS(SELECT 1 FROM tasks WHERE id=? AND status IN ('done','dropped'))
                        OR EXISTS(SELECT 1 FROM workers WHERE task=? AND id>?)''',
                    (worker['task'], worker['task'], worker_id)).fetchone()[0]:
                status = 'interrupted'
                payload['needs_input'] = False
            payload.update(worker=worker_id, task=worker['task'], topic=worker['topic'],
                           provider=worker['provider'], status=status, cwd=worker['cwd'])
            self.db.execute('UPDATE workers SET status=?,pid=NULL,session=?,fresh=?,result=?,updated=? WHERE id=?',
                            (status, payload.get('session_id') or worker['session'],
                             int(bool(worker['fresh']) and not payload.get('success') and not payload.get('transcript_path')),
                             json.dumps(payload), now, worker_id))
            cursor = self.db.execute('''INSERT INTO messages
                (topic,kind,text,source_worker,created,updated) VALUES (?,?,?,?,?,?)''',
                (worker['topic'], 'worker_result', json.dumps(payload, default=str), worker_id, now, now))
            if not payload.get('success'):
                problems.record(self, 'worker', payload.get('failure_code') or 'unsuccessful',
                                payload.get('failure_detail') or payload.get('error'),
                                topic=worker['topic'], task=worker['task'], worker=worker_id)
            return dict(self.db.execute('SELECT * FROM messages WHERE id=?', (cursor.lastrowid,)).fetchone())

    def worker_event_consumed(self, worker_id, seq):
        with self.db:
            self.db.execute('UPDATE workers SET last_seq=MAX(last_seq,?),updated=? WHERE id=?',
                            (seq, time.time(), worker_id))

    def requests_uncertain(self):
        with self.db:
            self.db.execute("UPDATE service_requests SET state='queued',updated=? WHERE state='sending' AND op='tldr'",
                            (time.time(),))
            self.db.execute("UPDATE service_requests SET state='uncertain',updated=? WHERE state='sending'",
                            (time.time(),))

    def service_request(self, op, params):
        now = time.time()
        with self.db:
            row = self.db.execute('INSERT INTO service_requests(op,params,created,updated) VALUES (?,?,?,?)',
                                  (op, json.dumps(params), now, now))
        return row.lastrowid

    def service_requests_pending(self):
        return [dict(row) for row in self.db.execute(
            "SELECT * FROM service_requests WHERE state='queued' ORDER BY id")]

    def service_request_settle(self, request_id, state, result=None):
        with self.db:
            self.db.execute('UPDATE service_requests SET state=?,result=?,updated=? WHERE id=?',
                            (state, json.dumps(result) if result is not None else None, time.time(), request_id))

    def bind(self, topic_id, cwd, name, provider=None, session=None, enabled=False, source_pid=None):
        cwd = Path(cwd).resolve()
        if not cwd.is_dir():
            raise ValueError("Project directory does not exist")
        if provider not in (None, "claude", "codex"):
            raise ValueError("Unknown provider")
        if session and not provider:
            raise ValueError("An existing session requires its provider")
        if not self.topic(topic_id):
            raise ValueError("Pair or link this channel first")
        busy = self.db.execute("SELECT 1 FROM tasks WHERE topic=? AND status='open'",
                               (topic_id,)).fetchone()
        busy = busy or self.db.execute("SELECT 1 FROM workers WHERE topic=? AND status IN ('queued','running')",
                                       (topic_id,)).fetchone()
        if self.topic(topic_id)['cwd'] and self.topic(topic_id)['cwd'] != str(cwd):
            busy = busy or self.db.execute("SELECT 1 FROM messages WHERE topic=? AND delivered='pending'",
                                           (topic_id,)).fetchone()
        if busy:
            raise ValueError("Cannot change a channel with active or waiting work")
        title = self.topic(topic_id).get('telegram_title')
        with self.db:
            self.db.execute("UPDATE topics SET cwd=?,name=?,provider=?,session=?,enabled=?,source_pid=? WHERE id=?",
                            (str(cwd), title or name, provider, session, int(enabled), source_pid, topic_id))
            if enabled and not self.get('coordinator_home_topic'):
                self.put('coordinator_home_topic', topic_id)
            logger.info("topic bound topic=%s cwd=%s", topic_id, cwd)

    def rename_topic(self, topic_id, name):
        previous = self.topic(topic_id)
        self.db.execute("UPDATE topics SET name=? WHERE id=?", (name, topic_id))
        logger.info("topic renamed topic=%s", topic_id)
        return previous["name"]

    def enqueue_report(self, topic, text, kind="report", reply_to=None, reply_markup=None, edit=None,
                       image=None):
        """Queue text for a topic. `edit` names a delivered card to replace in place when the text fits one message."""
        text = text.strip() or "Finished with no text result."
        if image:
            first = split_message(text, 1024)[0]
            remaining = text[len(first):].lstrip()
            parts = _fit_rendered(first, 1024)
            if remaining:
                parts.extend(_split_rendered(remaining, MESSAGE_LIMIT))
        else:
            parts = _split_rendered(text, MESSAGE_LIMIT)
        edit = edit if len(parts) == 1 and not image else None
        for index, part in enumerate(parts):
            markup = json.dumps(reply_markup) if reply_markup and index == len(parts) - 1 else None
            cursor = self.db.execute("""INSERT INTO outbox
                (topic,kind,text,reply_to,reply_markup,edit_message,image) VALUES (?,?,?,?,?,?,?)""",
                                     (topic, kind, part, None if edit else reply_to, markup, edit,
                                      image if index == 0 else None))
        return cursor.lastrowid

    def accept(self, update, *, creator=False):
        """Store update and cursor atomically. Return an admission outcome."""
        if not isinstance(update, dict):
            return 'invalid'
        uid = update.get("update_id")
        if type(uid) is not int:
            return "invalid"
        with self.db:
            if self.db.execute("SELECT 1 FROM updates WHERE id=?", (uid,)).fetchone():
                return "duplicate"
            from .telegram_updates import update_problem
            problem = update_problem(update)
            if problem:
                self.db.execute('INSERT INTO updates VALUES (?)', (uid,))
                self.put('offset', max(self.get('offset', 0), uid + 1))
                problems.record(self, 'telegram', 'invalid-update' if problem != 'unknown-update-type'
                                else 'unknown-update', 'update=%s shape=%s' % (uid, problem))
                return 'ignored' if problem == 'unknown-update-type' else 'invalid'
            private = update.get("message")
            private = (private if not isinstance(update.get('callback_query'), dict) and isinstance(private, dict)
                       and isinstance(private.get("chat"), dict) and private["chat"].get("type") == "private" else None)
            if private is not None:
                from .envelopes import accept_private, prepare_private
                intake = (prepare_private(self, private) if self.get('owner') is not None
                          else None)
            self.db.execute("INSERT INTO updates VALUES (?)", (uid,))
            self.put("offset", max(self.get("offset", 0), uid + 1))
            if private is not None:
                if self.get('owner') is None:
                    return 'ignored'
                return accept_private(self, private, intake)
            if isinstance(update.get('callback_query'), dict):
                from .control_ui import accept_callback
                return accept_callback(self, update['callback_query'])
            if isinstance(update.get('message_reaction'), dict):
                from .reactions import accept_reaction
                return accept_reaction(self, update['message_reaction'])
            if 'my_chat_member' in update:
                from .setup_flow import membership
                return membership(self, update['my_chat_member'])
            if 'message' not in update:
                return 'ignored'
            message = update['message']
            sender = message.get("from", {})
            chat = message.get("chat", {})
            thread = message.get("message_thread_id")
            text = message.get("text", "")
            edited = message.get('forum_topic_edited')
            if (isinstance(edited, dict) and isinstance(edited.get('name'), str) and edited['name'].strip()
                    and chat.get('id') == self.chat() and chat.get('type') == 'supergroup'
                    and type(thread) is int and type(message.get('message_id')) is int):
                topic = f"{chat['id']}:{thread}"
                if self.topic(topic):
                    name = edited['name'].strip()[:128]
                    self.db.execute("UPDATE topics SET name=?,telegram_title=? WHERE id=?", (name, name, topic))
                    logger.info("topic renamed topic=%s", topic)
                    return 'service_event'
            parts = text.split(maxsplit=1)
            command = parts[0].split('@')[0] if parts else ''
            from .setup_flow import ANONYMOUS, GENERAL, describe_sender, general_topic, migrate, notice, owner_event
            if chat.get('type') not in ('group', 'supergroup') or type(chat.get('id')) is not int:
                return 'ignored'
            if type(message.get('migrate_to_chat_id')) is int:
                return 'service_event' if migrate(self, chat['id'], message['migrate_to_chat_id']) else 'ignored'
            if type(message.get('migrate_from_chat_id')) is int:
                return 'service_event' if migrate(self, message['migrate_from_chat_id'], chat['id']) else 'ignored'
            if owner_event(self, message):
                return 'service_event'
            if command in ('/pair', '/start') and len(parts) == 2:
                if parts[1] == 'fix':
                    return 'ignored'
                if message.get('sender_chat'):
                    notice(self, chat['id'], ANONYMOUS, every=600)
                    return 'unauthorized'
                if sender.get('is_bot') or type(sender.get('id')) is not int or type(message.get('message_id')) is not int:
                    return 'unauthorized'
                if self.get('owner') not in (None, sender['id']) or self.chat() not in (None, chat['id']):
                    return 'unauthorized'
                from .pairing import valid_code
                if not valid_code(self, parts[1]):
                    notice(self, chat['id'], 'That setup link has expired. Run setup again on your Mac.', every=600)
                    return 'unauthorized'
                if not creator:
                    return 'unauthorized'
                if self.get('owner') is None and self.get('execution') is None:
                    self.put('execution', 'pairing')
                self.put('owner', sender['id'])
                self.put('owner_name', describe_sender(sender))
                self.put('group', chat['id'])
                self.put('group_name', chat.get('title', 'Telegram group'))
                self.put('mode', 'group')
                self.put('group_type', chat['type'])
                self.put('pairing', None)
                self.put('group_check_requested', True)
                self.put('pair_delete', {'chat': chat['id'], 'message': message['message_id']})
                if type(thread) is int and thread > 1:
                    topic = f"{chat['id']}:{thread}"
                    self.db.execute('INSERT OR IGNORE INTO topics(id,chat,thread,name,cwd) VALUES (?,?,?,?,?)',
                                    (topic, chat['id'], thread, 'Unbound project', ''))
                    from .onboarding import setup_guide
                    from .control_ui import setup_report
                    setup_report(self, topic, setup_guide(self, topic), reply_to=message['message_id'])
                logger.info('owner paired chat=%s', chat['id'])
                return 'paired'
            if (sender.get('is_bot') or message.get('sender_chat') or type(sender.get('id')) is not int
                    or type(message.get('message_id')) is not int):
                return 'ignored'
            if sender['id'] != self.get('owner') or chat['id'] != self.chat():
                return 'unauthorized'
            if type(thread) is not int or thread <= 1:
                if command == '/project' and parts[1:]:
                    from .controls import handle_control
                    if handle_control(self, general_topic(self, chat['id']), message['message_id'], text):
                        return 'control'
                if self.get('control_topic'):
                    notice(self, chat['id'], GENERAL, every=3600)
                return 'ignored'
            topic = f"{chat['id']}:{thread}"
            if topic == self.get('control_topic'):
                from .signin import intercept_topic_code, setup_signin
                from .envelopes import intercept_topic_reply
                if intercept_topic_code(self, topic, message) or intercept_topic_reply(self, topic, message):
                    return 'envelope_intercept'
                if command in ('/setup', '/start') and len(parts) == 1:
                    from .setup_flow import account_check, setup_status
                    if self.get('accounts') and not (self.get('setup_requests', {}) or {}).get('check_accounts'):
                        account_check(self)
                    if not setup_signin(self, topic):
                        setup_status(self, force=True, in_place=False)
                    return 'control'
            from .onboarding import first_project
            outcome = first_project(self, topic, message) if self.topic(topic) else None
            if outcome:
                return outcome
            return self._accept_topic_message(message, topic, thread, text, command)

    def _accept_topic_message(self, message, topic, thread, text, command):
        config = self.topic(topic)
        if not config:
            title = message.get('forum_topic_created', {}).get('name') or 'Unbound project'
            self.db.execute("INSERT INTO topics(id,chat,thread,name,telegram_title,cwd) VALUES (?,?,?,?,?,?)",
                            (topic, message["chat"]["id"], thread, str(title)[:128],
                             str(title)[:128] if title != 'Unbound project' else None, ""))
            config = self.topic(topic)
        if message.get('forum_topic_created'):
            title = message.get('forum_topic_created', {}).get('name')
            if isinstance(title, str) and title.strip():
                title = title.strip()[:128]
                self.db.execute('UPDATE topics SET name=?,telegram_title=? WHERE id=?', (title, title, topic))
            from .onboarding import setup_guide
            from .control_ui import setup_report
            setup_report(self, topic, setup_guide(self, topic), reply_to=message['message_id'])
            return 'setup'
        if any(key in message for key in SERVICE_EVENTS):
            return 'service_event'
        from .signin import intercept_topic_code
        from .envelopes import intercept_topic_reply
        if intercept_topic_code(self, topic, message) or intercept_topic_reply(self, topic, message):
            return 'envelope_intercept'
        from .controls import handle_control
        if handle_control(self, topic, message["message_id"], text):
            return "control"
        if command == "/ping":
            self.enqueue_report(topic, "Bridge received your message. No agent was started.", reply_to=message["message_id"])
            return "ping"
        from .control_ui import accept_text, setup_report
        if accept_text(self, topic, text, message['message_id'], message.get('reply_to_message', {}).get('message_id')):
            return 'control'
        if not config["enabled"] and command != '/goal':
            from .signin import topic_code_pointer
            if not text.startswith('/') and topic_code_pointer(self, topic, message):
                return 'envelope_intercept'
            from .onboarding import setup_reply
            reply = setup_reply(self, topic, text) if not text.startswith('/') else 'Unknown command. Send /help.'
            setup_report(self, topic, reply, reply_to=message["message_id"])
            return "disabled"
        if COMMAND.fullmatch(command) and command != '/goal':
            self.enqueue_report(topic, 'Unknown command. Send /help.',
                                reply_to=message['message_id'])
            return 'unknown_command'
        from .media import attachment_metadata, is_image, MediaError
        try:
            media = attachment_metadata(message)
        except MediaError as error:
            problems.record(self, 'attachment', 'unsupported', str(error), topic=topic)
            self.enqueue_report(topic, str(error), reply_to=message["message_id"])
            return "unsupported"
        if media:
            text = message.get("caption") or text or (
                "Inspect the attached image." if is_image(media) else "Read the attached file.")
            self.db.execute("""INSERT INTO attachments
                (topic,message,file_id,mime,size,group_id,name,kind,created) VALUES (?,?,?,?,?,?,?,?,?)""",
                (topic, message["message_id"], media['file_id'], media['mime'], media['size'],
                 media['group_id'], media['name'], media['kind'], time.time()))
        if not text:
            self.enqueue_report(topic, "Send text, a photo, or a file.",
                                reply_to=message["message_id"])
            return "unsupported"
        reply = message.get("reply_to_message", {}).get("message_id")
        if reply == thread:
            reply = None
        if media and message.get('caption'):
            text = entities_to_markdown(text, message.get('caption_entities'))
        elif message.get('text'):
            text = entities_to_markdown(text, message.get('entities'))
        row = self._save_owner_message(topic, message["message_id"], text, reply,
                                       [media] if media else None)
        if self.get('execution') == 'pairing':
            sent = self.get('setup_pointer_sent', {}) or {}
            now = time.time()
            if now - sent.get(topic, 0) >= 3600:
                control = self.get('control_topic')
                pointer = 'Saved. No agent is connected yet, so nothing can answer. '
                if topic == control:
                    pointer += 'Use the buttons on the setup card below.'
                else:
                    card_topic = self.topic(control) if control else None
                    pointer += ('Connect Claude or ChatGPT on the setup card in the %s topic, '
                                "and I'll answer this then.") % (card_topic['name'] if card_topic else 'Torii')
                self.enqueue_report(topic, pointer, reply_to=message['message_id'])
                self.put('setup_pointer_sent', dict(sent, **{topic: now}))
                if topic == control:
                    from .setup_flow import setup_status
                    setup_status(self, force=True, in_place=False)
        date = message.get('date')
        logger.info('owner message saved message=%s telegram_age=%s', row['id'],
                    '%.1f' % (time.time() - date) if type(date) is int else 'unknown')
        return "queued"

    def _save_owner_message(self, topic, message, text, reply=None, images=None, reacted_to=None):
        if images is None:
            images = [dict(row) for row in self.db.execute(
                'SELECT file_id,mime,size,group_id,name,path FROM attachments WHERE topic=? AND message=?',
                (topic, message))] or None
        if reply is not None:
            row = self.db.execute('''SELECT text FROM outbox WHERE topic=? AND telegram_message=?''',
                                  (topic, reply)).fetchone()
            if row is None:
                row = self.db.execute('SELECT text FROM messages WHERE topic=? AND telegram_message=?',
                                      (topic, reply)).fetchone()
            if row:
                quote = ' '.join(row['text'].split())[:300]
                text = 'Replying to: ' + quote + '\n' + text
        return self.message_save(topic, 'owner', text, telegram_message=message, images=images, reacted_to=reacted_to)


    def pending_delivery(self):
        if self.get('setup_problem') == 'removed' or self.get('mode') == 'private':
            return None
        row = self.db.execute("""SELECT o.*,t.chat,t.thread FROM outbox o JOIN topics t ON t.id=o.topic
            WHERE o.delivered=0 AND o.retired=0 AND (t.chat=? OR o.kind='group-setup') AND o.next_attempt<=?
            AND (? != 'topics_off' OR t.thread=0) AND NOT EXISTS
            (SELECT 1 FROM outbox p WHERE p.topic=o.topic AND p.delivered=0 AND p.retired=0 AND p.id<o.id)
            ORDER BY o.id LIMIT 1""", (self.chat(), time.time(), self.get('setup_problem') or '')).fetchone()
        if row is None:
            return None
        result = dict(row)
        return result

    def outbox_has_pending(self):
        if self.get('owner') is None or self.chat() is None:
            return False
        return self.db.execute('''SELECT 1 FROM outbox o JOIN topics t ON t.id=o.topic
            WHERE o.delivered=0 AND o.retired=0 AND t.chat=? LIMIT 1''',
                               (self.chat(),)).fetchone() is not None

    def delivered(self, row_id, telegram_message):
        with self.db:
            previous = self.db.execute("""SELECT reaction_desired,reaction_sent,reaction_state,
                reaction_retry,reaction_attempts FROM outbox WHERE telegram_message=? AND id!=?
                AND topic=(SELECT topic FROM outbox WHERE id=?) ORDER BY id DESC LIMIT 1""",
                                       (telegram_message, row_id, row_id)).fetchone()
            if previous:
                self.db.execute("""UPDATE outbox SET reaction_desired=?,reaction_sent=?,reaction_state=?,
                    reaction_retry=?,reaction_attempts=? WHERE id=?""", (*previous, row_id))
            self.db.execute("""UPDATE outbox SET telegram_message=NULL WHERE telegram_message=? AND id!=?
                               AND topic=(SELECT topic FROM outbox WHERE id=?)""", (telegram_message, row_id, row_id))
            self.db.execute("UPDATE outbox SET delivered=1,telegram_message=? WHERE id=?", (telegram_message, row_id))
            logger.info("outbox delivered id=%s", row_id)

    def effect_failed(self, effect, retry_after=None):
        """Schedule an envelope effect again. The caller owns the transaction."""
        self.db.execute('UPDATE envelope_effects SET attempts=attempts+1,next_attempt=? WHERE id=?',
                        (time.time() + retry_delay(effect['attempts'], retry_after), effect['id']))

    def edit_failed(self, row_id):
        """The card cannot be edited any more; send the page as a new message instead."""
        with self.db:
            self.db.execute("UPDATE outbox SET edit_message=NULL WHERE id=?", (row_id,))

    def image_unavailable(self, row_id):
        with self.db:
            self.db.execute("""UPDATE outbox SET image=NULL,edit_message=NULL,
                text='The image could not be sent because the file is unavailable.' || char(10,10) || text
                WHERE id=? AND image IS NOT NULL""", (row_id,))

    def delivery_failed(self, row, retry_after=None):
        delay = retry_delay(row["attempts"], retry_after)
        with self.db:
            self.db.execute("UPDATE outbox SET attempts=attempts+1,next_attempt=? WHERE id=?", (time.time() + delay, row["id"]))
            logger.info("outbox retry id=%s delay=%s", row["id"], delay)
