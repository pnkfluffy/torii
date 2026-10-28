"""Durable owner intake, tasks, messages, and reports."""

import hashlib
from contextlib import contextmanager
import json
import logging
from pathlib import Path
import secrets
import sqlite3
import time
import re

from . import extension, problems
from .control_api import FINISHES, STAGES
from .formatting import FENCE, entities_to_markdown, html_to_text, inline_spans, markdown_to_html
from .waits import WaitStore


COMMAND = re.compile(r'/[A-Za-z0-9_]{1,32}\Z')
PR = re.compile(r'https://github\.com/([A-Za-z0-9][A-Za-z0-9-]{0,38}/[A-Za-z0-9_.-]+)/pull/([1-9][0-9]*)\Z')

SERVICE_EVENTS = ('forum_topic_edited', 'forum_topic_closed', 'forum_topic_reopened',
                  'general_forum_topic_hidden', 'general_forum_topic_unhidden',
                  'pinned_message', 'new_chat_members', 'left_chat_member', 'new_chat_title',
                  'new_chat_photo', 'delete_chat_photo', 'message_auto_delete_timer_changed')

logger = logging.getLogger(__name__)


def retry_delay(attempts, retry_after=None):
    return retry_after if retry_after is not None else min(300, 2 ** min(attempts + 1, 8))


MESSAGE_LIMIT = 4096
CLI_CALL_LIMIT = 1000


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


def _rendered_length(text):
    return len(html_to_text(markdown_to_html(text), link_urls=False))


def _take_rendered(text, limit, next_limit=None):
    length = _rendered_length(text)
    if length <= limit:
        return text, ''
    next_limit = limit if next_limit is None else next_limit
    remaining_capacity = ((max(0, length - limit) + next_limit - 1) // next_limit) * next_limit
    end = min(len(text), limit * 2)
    while True:
        spans = list(inline_spans(text, end))
        end = max([end] + [finish for _, finish in spans])
        window = text[:end]
        if end == len(text) or _rendered_length(window) > limit:
            break
        end = min(len(text), end * 2)
    boundaries = []
    span = 0
    for match in re.finditer(r'\s+', window):
        cut = match.start()
        while span < len(spans) and spans[span][1] <= cut:
            span += 1
        if cut and not (span < len(spans) and spans[span][0] < cut < spans[span][1]):
            boundaries.append((cut, match[0]))
    low, high = 0, len(boundaries)
    while low < high:
        middle = (low + high) // 2
        if _rendered_length(_cut_parts(window, boundaries[middle][0])[0]) <= limit:
            low = middle + 1
        else:
            high = middle
    if low:
        fitting = boundaries[:low]
        for marker in ('\n\n', '\n'):
            candidate = next((cut for cut, separator in reversed(fitting)
                              if marker in separator), None)
            if candidate is not None:
                left, right = _cut_parts(text, candidate)
                if (limit // 2 < _rendered_length(left) <= limit
                        and _rendered_length(right) <= remaining_capacity):
                    return left, right
        return _cut_parts(text, fitting[-1][0])
    low, high = 1, len(window) - 1
    while low < high:
        middle = (low + high + 1) // 2
        if _rendered_length(_cut_parts(window, middle)[0]) <= limit:
            low = middle
        else:
            high = middle - 1
    return _cut_parts(text, low)


def _split_rendered(text, limit):
    parts = []
    while text:
        part, text = _take_rendered(text, limit)
        parts.append(part)
    return parts


def finish_default(store, topic):
    return store.get('finish_default:' + topic)


class Store(WaitStore):
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
                    secrets TEXT, origin INTEGER REFERENCES messages(id),
                    UNIQUE(topic,number));
                CREATE TABLE IF NOT EXISTS workers (
                    id INTEGER PRIMARY KEY, task INTEGER REFERENCES tasks(id),
                    topic TEXT NOT NULL REFERENCES topics(id), provider TEXT NOT NULL,
                    prompt TEXT NOT NULL, session TEXT, cwd TEXT, workspace TEXT,
                    fresh INTEGER NOT NULL DEFAULT 1, status TEXT NOT NULL DEFAULT 'queued',
                    pid INTEGER, result TEXT, goal TEXT, model TEXT, effort TEXT NOT NULL DEFAULT 'medium', account_alias TEXT,
                    last_seq INTEGER NOT NULL DEFAULT 0,
                    created REAL NOT NULL, updated REAL NOT NULL,
                    host TEXT, work TEXT NOT NULL DEFAULT 'research');
                CREATE TABLE IF NOT EXISTS worker_temp_state (
                    worker INTEGER PRIMARY KEY REFERENCES workers(id), path TEXT,
                    finished INTEGER NOT NULL DEFAULT 0, pending INTEGER NOT NULL DEFAULT 0,
                    epoch INTEGER NOT NULL DEFAULT 0, staged TEXT NOT NULL DEFAULT '[]',
                    retry_at REAL NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0);
                CREATE INDEX IF NOT EXISTS worker_temp_pending ON worker_temp_state(pending,retry_at,worker);
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY, topic TEXT NOT NULL REFERENCES topics(id),
                    kind TEXT NOT NULL CHECK(kind IN ('owner','forwarded','worker_result','restarted','callback','secret_filled','event')),
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
                    created REAL NOT NULL, updated REAL NOT NULL, from_name TEXT,
                    reaction_first_failed REAL, batch_id INTEGER);
                CREATE TABLE IF NOT EXISTS outbox (
                    id INTEGER PRIMARY KEY, topic TEXT NOT NULL REFERENCES topics(id),
                    kind TEXT NOT NULL, text TEXT NOT NULL, reply_to INTEGER,
                    delivered INTEGER NOT NULL DEFAULT 0, telegram_message INTEGER,
                    attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL NOT NULL DEFAULT 0,
                    reply_markup TEXT, image TEXT, edit_message INTEGER,
                    retired INTEGER NOT NULL DEFAULT 0, reaction_desired TEXT, reaction_sent TEXT,
                    reaction_state TEXT NOT NULL DEFAULT 'sent', reaction_retry REAL NOT NULL DEFAULT 0,
                    reaction_attempts INTEGER NOT NULL DEFAULT 0, reaction_first_failed REAL,
                    as_document INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS attachments (
                    id INTEGER PRIMARY KEY, topic TEXT NOT NULL REFERENCES topics(id),
                    message INTEGER NOT NULL, file_id TEXT NOT NULL, mime TEXT NOT NULL,
                    size INTEGER, group_id TEXT, path TEXT, created REAL NOT NULL,
                    name TEXT, error TEXT, kind TEXT, UNIQUE(topic,message));
                CREATE TABLE IF NOT EXISTS service_requests (
                    id INTEGER PRIMARY KEY, op TEXT NOT NULL, params TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'queued', result TEXT,
                    created REAL NOT NULL, updated REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS cli_calls (
                    id INTEGER PRIMARY KEY, created REAL NOT NULL, op TEXT NOT NULL,
                    from_name TEXT, topic TEXT, state TEXT NOT NULL, process TEXT);
                CREATE TABLE IF NOT EXISTS envelopes (
                    id INTEGER PRIMARY KEY, name TEXT NOT NULL, reason TEXT NOT NULL, consumer TEXT NOT NULL,
                    task INTEGER REFERENCES tasks(id), topic TEXT NOT NULL REFERENCES topics(id),
                    token_hash TEXT, chat INTEGER, card_outbox INTEGER,
                    state TEXT NOT NULL CHECK(state IN ('open','armed','filled','superseded','cancelled','expired','revoked')),
                    created REAL NOT NULL, expires REAL NOT NULL, armed_at REAL, filled_at REAL,
                    length INTEGER, fingerprint TEXT, updated REAL NOT NULL, arm_thread INTEGER);
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
                    created REAL NOT NULL, thread INTEGER);
                CREATE TABLE IF NOT EXISTS problems (
                    id INTEGER PRIMARY KEY, created REAL NOT NULL, area TEXT NOT NULL, code TEXT NOT NULL,
                    detail TEXT, topic TEXT, task INTEGER, worker INTEGER, message INTEGER, attachment INTEGER);
                CREATE TABLE IF NOT EXISTS host_log_hosts (
                    name TEXT PRIMARY KEY, worker INTEGER NOT NULL REFERENCES workers(id));
                CREATE INDEX IF NOT EXISTS host_log_hosts_worker ON host_log_hosts(worker);
                CREATE INDEX IF NOT EXISTS host_log_receipts ON service_requests(json_extract(params,'$.worker'),op)
                    WHERE op IN ('workers.steer','workers.goal');
                CREATE INDEX IF NOT EXISTS settings_host_refs ON settings(key) WHERE substr(key,-5)='_host';
                CREATE TABLE IF NOT EXISTS task_stamps (
                    id INTEGER PRIMARY KEY, task INTEGER NOT NULL REFERENCES tasks(id),
                    stage TEXT NOT NULL CHECK(stage IN ('ready','reviewed','ci','merged','live')),
                    created REAL NOT NULL);
                CREATE INDEX IF NOT EXISTS task_stamps_task ON task_stamps(task,id);
                CREATE INDEX IF NOT EXISTS task_stamps_created ON task_stamps(created);
            """
            for statement in schema.split(';'):
                if statement.strip():
                    self.db.execute(statement)
            for event in ('INSERT', 'UPDATE OF status'):
                self.db.execute('''CREATE TRIGGER IF NOT EXISTS worker_temp_%s AFTER %s ON workers
                    WHEN NEW.status IN ('done','interrupted') BEGIN
                    INSERT INTO worker_temp_state(worker,finished,pending,epoch) VALUES (NEW.id,1,1,1)
                    ON CONFLICT(worker) DO UPDATE SET finished=1,pending=1,retry_at=0,attempts=0,
                        epoch=worker_temp_state.epoch+1;
                    END''' % ('insert' if event == 'INSERT' else 'finish', event))
            self.db.execute('UPDATE worker_temp_state SET pending=1 WHERE pending=2')
            self.code_requests = extension.optional('code_requests')
            if self.code_requests is not None:
                self.code_requests.install_schema(self)
            if 'pr' not in self._columns('tasks'):
                self.db.execute('ALTER TABLE tasks ADD COLUMN pr TEXT')
            self._migrate_job_iterations()
            from .session_storage import install as install_session_storage
            install_session_storage(self)
            from .close_saves import migrate
            migrate(self)
            from .waits import install_schema as install_waits
            install_waits(self)
            if 'duration' not in self._columns('attachments'):
                self.db.execute('ALTER TABLE attachments ADD COLUMN duration INTEGER')
            self._migrate_mode()
            message_schema = self.db.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='messages'").fetchone()[0]
            if 'forwarded' not in message_schema:
                indexes = [row[0] for row in self.db.execute(
                    "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name='messages' AND sql IS NOT NULL")]
                self.db.execute('''CREATE TABLE messages_new (
                    id INTEGER PRIMARY KEY, topic TEXT NOT NULL REFERENCES topics(id),
                    kind TEXT NOT NULL CHECK(kind IN ('owner','forwarded','worker_result','restarted','callback','secret_filled','event')),
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
                    created REAL NOT NULL, updated REAL NOT NULL, from_name TEXT,
                    reaction_first_failed REAL, batch_id INTEGER)''')
                columns = ','.join(self._columns('messages'))
                self.db.execute('INSERT INTO messages_new (' + columns + ') SELECT ' + columns + ' FROM messages')
                self.db.execute('DROP TABLE messages')
                self.db.execute('ALTER TABLE messages_new RENAME TO messages')
                for statement in indexes:
                    self.db.execute(statement)
            if 'from_name' not in self._columns('messages'):
                self.db.execute('ALTER TABLE messages ADD COLUMN from_name TEXT')
            for table in ('messages', 'outbox'):
                if 'reaction_first_failed' not in self._columns(table):
                    self.db.execute('ALTER TABLE ' + table + ' ADD COLUMN reaction_first_failed REAL')
            if 'batch_id' not in self._columns('messages'):
                self.db.execute('ALTER TABLE messages ADD COLUMN batch_id INTEGER')
            if 'as_document' not in self._columns('outbox'):
                self.db.execute('ALTER TABLE outbox ADD COLUMN as_document INTEGER NOT NULL DEFAULT 0')
            if 'created' not in self._columns('outbox'):
                self.db.execute('ALTER TABLE outbox ADD COLUMN created REAL NOT NULL DEFAULT 0')
            self.db.execute('''CREATE TABLE IF NOT EXISTS parent_reply_turns (
                host TEXT NOT NULL, turn TEXT NOT NULL, provider TEXT NOT NULL,
                topic TEXT NOT NULL REFERENCES topics(id) ON UPDATE CASCADE, outbox_start INTEGER NOT NULL,
                messages TEXT NOT NULL, checked INTEGER NOT NULL DEFAULT 0 CHECK(checked IN (0,1)),
                outbox INTEGER REFERENCES outbox(id), PRIMARY KEY(host,turn))''')
            self.db.execute('CREATE INDEX IF NOT EXISTS parent_reply_outbox ON parent_reply_turns(outbox)')
            self.db.execute('''CREATE TABLE IF NOT EXISTS parent_reply_owners (
                message INTEGER PRIMARY KEY REFERENCES messages(id), host TEXT NOT NULL, turn TEXT NOT NULL,
                FOREIGN KEY(host,turn) REFERENCES parent_reply_turns(host,turn))''')
            self.db.execute('CREATE INDEX IF NOT EXISTS parent_reply_owner_turn ON parent_reply_owners(host,turn)')
            for statement in (
                'CREATE UNIQUE INDEX IF NOT EXISTS messages_telegram ON messages(topic,telegram_message) WHERE telegram_message IS NOT NULL',
                'CREATE INDEX IF NOT EXISTS messages_workers ON messages(source_worker)',
                'CREATE INDEX IF NOT EXISTS messages_batches ON messages(batch_id) WHERE batch_id IS NOT NULL',
                'CREATE UNIQUE INDEX IF NOT EXISTS messages_envelopes ON messages(source_envelope) WHERE source_envelope IS NOT NULL',
                'CREATE INDEX IF NOT EXISTS workers_tasks ON workers(task,id)',
                'CREATE INDEX IF NOT EXISTS workers_states ON workers(status,id)',
                'CREATE INDEX IF NOT EXISTS workers_task_status ON workers(task,status)',
                'CREATE INDEX IF NOT EXISTS service_requests_states ON service_requests(state,id)',
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
            self.db.execute("""UPDATE workers SET status='done',updated=?
                WHERE status='needs_input'""", (time.time(),))
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

    def _migrate_job_iterations(self):
        if 'iteration' not in self._columns('tasks'):
            objects = [row[0] for row in self.db.execute(
                "SELECT sql FROM sqlite_master WHERE tbl_name='tasks' AND type IN ('index','trigger') AND sql IS NOT NULL")]
            self.db.execute('''CREATE TABLE tasks_new (
                id INTEGER PRIMARY KEY, topic TEXT NOT NULL REFERENCES topics(id),
                number INTEGER NOT NULL, title TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open','interrupted','waiting','done','dropped')),
                worktree TEXT, notes TEXT, created REAL NOT NULL, updated REAL NOT NULL,
                secrets TEXT, origin INTEGER REFERENCES messages(id), pr TEXT,
                iteration INTEGER NOT NULL DEFAULT 0 CHECK(iteration>=0),
                finish TEXT CHECK(finish IN ('merged','live','answered','delivered')), reason TEXT,
                copy TEXT NOT NULL DEFAULT 'present' CHECK(copy IN ('present','saving','held','cleared')),
                copy_reason TEXT, parent INTEGER REFERENCES tasks(id), UNIQUE(topic,number))''')
            columns = ','.join(self._columns('tasks'))
            self.db.execute('INSERT INTO tasks_new (' + columns + ') SELECT ' + columns + ' FROM tasks')
            self.db.execute('DROP TABLE tasks')
            self.db.execute('ALTER TABLE tasks_new RENAME TO tasks')
            for statement in objects:
                self.db.execute(statement)
        self.db.execute('''CREATE TABLE IF NOT EXISTS task_iterations (
            id INTEGER PRIMARY KEY, task INTEGER NOT NULL REFERENCES tasks(id),
            k INTEGER NOT NULL CHECK(k>=0), opened REAL NOT NULL, closed REAL,
            opened_by TEXT NOT NULL CHECK(opened_by IN ('create','owner','wait','coordinator','resume','migration')),
            origin INTEGER REFERENCES messages(id),
            outcome TEXT CHECK(outcome IN ('done','waiting','interrupted','dropped')), reason TEXT,
            UNIQUE(task,k))''')
        self.db.execute('''INSERT OR IGNORE INTO task_iterations(task,k,opened,closed,opened_by,origin,outcome)
            SELECT id,0,created,CASE WHEN status IN ('done','dropped') THEN updated END,'migration',origin,
                CASE WHEN status IN ('done','dropped') THEN status END FROM tasks''')
        for table in ('workers', 'task_stamps'):
            if 'iteration' not in self._columns(table):
                self.db.execute('ALTER TABLE ' + table + ' ADD COLUMN iteration INTEGER NOT NULL DEFAULT 0 CHECK(iteration>=0)')
        from .lifecycle import install_schema
        install_schema(self)

    def _migrate_mode(self):
        from .setup_flow import restore_control_topic
        restore_control_topic(self)

    def chat(self):
        return self.get('group')

    def _columns(self, table):
        return {row['name'] for row in self.db.execute('PRAGMA table_info(' + table + ')')}

    def close(self):
        storage = getattr(self, '_worker_storage', None)
        if storage is not None:
            storage.close()
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
        if key == 'transcription_model' and value not in ('parakeet', 'whisper'):
            raise ValueError('transcription_model must be parakeet or whisper')
        previous = self.get(key) if key.endswith('_host') else None
        self.db.execute("INSERT OR REPLACE INTO settings VALUES (?,?)", (key, json.dumps(value)))
        if isinstance(previous, dict) and previous.get('id') != (value or {}).get('id'):
            self.host_logs_changed(host=previous['id'])

    def host_log_register(self, name, worker):
        self.host_log_register_many(((name, worker),))

    def host_log_register_many(self, hosts):
        with self.db:
            self.db.executemany('''INSERT OR IGNORE INTO host_log_hosts(name,worker)
                SELECT ?,id FROM workers WHERE id=?''', hosts)

    def host_logs_changed(self, **target):
        from .host_logs import REQUEST
        params = json.dumps(target, sort_keys=True)
        if not self.db.execute("SELECT 1 FROM service_requests WHERE op=? AND params=? AND state='queued'",
                               (REQUEST, params)).fetchone():
            now = time.time()
            self.db.execute('INSERT INTO service_requests(op,params,created,updated) VALUES (?,?,?,?)',
                            (REQUEST, params, now, now))

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

    @contextmanager
    def transaction(self):
        nested = self.db.in_transaction
        self.db.execute('SAVEPOINT store_change' if nested else 'BEGIN IMMEDIATE')
        try:
            yield
        except BaseException:
            if nested:
                self.db.execute('ROLLBACK TO store_change')
                self.db.execute('RELEASE store_change')
            else:
                self.db.rollback()
            raise
        else:
            if nested:
                self.db.execute('RELEASE store_change')
            else:
                self.db.commit()

    def topics(self):
        return [dict(r) for r in self.db.execute("SELECT * FROM topics ORDER BY id")]

    def wait_hook(self, code, action, *args, **kwargs):
        if self.get('waits_enabled', False) is not True:
            return None
        try:
            with self.transaction():
                return action(*args, **kwargs)
        except Exception as error:
            problems.record(self, 'waits', code, type(error).__name__)
            return None

    def task_create(self, topic, title, worktree=None, notes=None, secrets=None, origin=None, finish=None, parent=None):
        if not self.topic(topic):
            raise ValueError('Unknown channel')
        if not isinstance(title, str) or not title.strip():
            raise ValueError('Job title is required')
        if finish is None:
            finish = finish_default(self, topic)
        if finish is not None and finish not in FINISHES:
            raise ValueError('Job finish must be merged, live, answered, or delivered')
        if parent is not None and (type(parent) is not int or not self.task_get(parent)):
            raise ValueError('Unknown parent job')
        now = time.time()
        with self.transaction():
            if self.get('topic_lifecycle:' + topic):
                raise ValueError('This chat is closing or closed')
            row = self.db.execute('''INSERT INTO tasks(topic,number,title,worktree,notes,secrets,origin,created,updated,finish,parent)
                VALUES (?,(SELECT COALESCE(MAX(number),0)+1 FROM tasks WHERE topic=?),?,?,?,?,?,?,?,?,?)''',
                                  (topic, topic, title.strip(), worktree, notes,
                                   json.dumps(secrets) if secrets else None, origin, now, now, finish, parent))
            self.db.execute('''INSERT INTO task_iterations(task,k,opened,opened_by,origin)
                VALUES (?,0,?,'create',?)''', (row.lastrowid, now, origin))
            from .reactions import set_desired
            set_desired(self, origin, '👀')
        if self.get('close_clears_space', False) is True:
            from .close_saves import scratch
            scratch(self, row.lastrowid, create=True)
        return self.task_get(row.lastrowid)

    @staticmethod
    def validate_task_pr(pr):
        if (not isinstance(pr, str) or not PR.fullmatch(pr) or len(pr) > 200
                or PR.fullmatch(pr).group(1).split('/')[-1] in ('.', '..')):
            raise ValueError('pr must be a GitHub pull request URL.')

    def task_update(self, task_id, **fields):
        with self.transaction():
            return self._task_update(task_id, **fields)

    def _task_update(self, task_id, **fields):
        allowed = {'title', 'status', 'worktree', 'notes', 'note', 'secrets', 'stage', 'pr', 'finish'}
        if not fields or set(fields) - allowed:
            raise ValueError('Unknown job field')
        if 'note' in fields and 'notes' in fields:
            raise ValueError('Use either note or notes, not both')
        if 'note' in fields and not fields['note'].strip():
            raise ValueError('Job note must not be empty')
        stage = fields.pop('stage', None)
        if stage is not None and stage not in STAGES:
            raise ValueError('Job stage must be ready, reviewed, ci, merged, or live')
        if 'status' in fields and fields['status'] not in ('open', 'done', 'dropped'):
            raise ValueError('Job status must be open, done, or dropped')
        if 'title' in fields and (not isinstance(fields['title'], str) or not fields['title'].strip()):
            raise ValueError('Job title is required')
        task = self.task_get(task_id)
        if not task:
            raise ValueError('Unknown job')
        if task['status'] == 'waiting' and fields.get('status') == 'open':
            raise ValueError('Use workers.spawn or pending.fire to reopen a waiting job.')
        if 'finish' in fields:
            if task['status'] != 'open':
                raise ValueError('Job finish can change only while the job is open')
            if fields['finish'] is not None and fields['finish'] not in FINISHES:
                raise ValueError('Job finish must be merged, live, answered, or delivered')
        if 'pr' in fields:
            self.validate_task_pr(fields['pr'])
        link = self.code_requests.validate_job_update(self, task, fields, stage) if self.code_requests is not None else None
        reason = fields.get('note')
        if 'note' in fields:
            note = fields.pop('note').strip()
            old = task['notes']
            fields['notes'] = (old + '\n' if old else '') + note
        if 'title' in fields:
            fields['title'] = fields['title'].strip()
        if 'secrets' in fields:
            fields['secrets'] = json.dumps(fields['secrets']) if fields['secrets'] else None
        fields['updated'] = time.time()
        assignments = ','.join(name + '=?' for name in fields)
        with self.transaction():
            self.db.execute('UPDATE tasks SET ' + assignments + ' WHERE id=?',
                            (*fields.values(), task_id))
            if stage is not None:
                self.db.execute('INSERT INTO task_stamps(task,stage,created,iteration) VALUES (?,?,?,?)',
                                (task_id, stage, fields['updated'], task['iteration']))
            if 'status' in fields and fields['status'] != task['status']:
                closed = fields['status'] in ('done', 'dropped')
                self.db.execute('''UPDATE task_iterations SET closed=?,outcome=?,reason=? WHERE task=? AND k=?''',
                                (fields['updated'] if closed else None, fields['status'] if closed else None,
                                 reason if closed else None, task_id, task['iteration']))
                from .reactions import set_desired
                set_desired(self, task['origin'], {'open': '👀', 'done': '👌', 'dropped': None}[fields['status']])
                current = self.task_iterations(task_id)[-1]
                if current['origin'] != task['origin']:
                    set_desired(self, current['origin'], {'open': '👀', 'done': '👌', 'dropped': None}[fields['status']])
            if fields.get('status') in ('done', 'dropped'):
                from .session_storage import request as request_session_cleanup
                request_session_cleanup(self)
                cancelled = self.db.execute("""SELECT * FROM workers WHERE task=?
                    AND status IN ('queued','waiting_for_secret','waiting_for_quota')""", (task_id,)).fetchall()
                self.db.execute("""UPDATE workers SET status='interrupted',updated=?
                    WHERE task=? AND status IN ('queued','waiting_for_secret','waiting_for_quota')""",
                    (time.time(), task_id))
                if self.get('session_delete', False) is True:
                    for worker in cancelled:
                        if worker['result'] is not None:
                            continue
                        result = {}
                        result.update(success=False, session_id=worker['session'], failure_code='owner_stopped',
                                      error='The job closed before this worker completed.')
                        self.worker_complete(worker['id'], result, 'interrupted')
                self.host_logs_changed(task=task_id)
                from .workspaces import request_closed_copy_cleanup
                request_closed_copy_cleanup(self, task_id)
                for worker in cancelled:
                    request_closed_copy_cleanup(self, cwd=worker['cwd'])
            from .close_saves import request
            request(self, task_id)
            if self.code_requests is not None:
                self.code_requests.queue_job_update(self, link, self.task_get(task_id), stage, fields.get('status'), reason)
            if fields.get('status') in ('done', 'dropped'):
                self.wait_hook('round-end-failed', self.wait_end_blocking, task_id, fields['status'])
                self.wait_hook('job-trigger-failed', self.wait_task_changed, task_id, fields['status'])
            self.wait_hook('card-refresh-failed', self.refresh_owner_waiting)
        return self.task_get(task_id)

    @staticmethod
    def _task(row):
        task = dict(row)
        task['secrets'] = json.loads(task['secrets']) if task.get('secrets') else []
        return task

    def task_get(self, task_id):
        row = self.db.execute('SELECT * FROM tasks WHERE id=?', (task_id,)).fetchone()
        return self._task(row) if row else None

    def task_iterations(self, task_id):
        return [dict(row) for row in self.db.execute(
            'SELECT * FROM task_iterations WHERE task=? ORDER BY k', (task_id,))]

    def tasks_list(self, topic=None, status=None):
        if status is not None and status not in ('open', 'interrupted', 'waiting', 'done', 'dropped'):
            raise ValueError('Job status must be open, interrupted, waiting, done, or dropped')
        return [self._task(row) for row in self.db.execute(
            'SELECT * FROM tasks WHERE (? IS NULL OR topic=?) AND (? IS NULL OR status=?) ORDER BY topic,number',
            (topic, topic, status, status))]

    def closed_tasks_page(self, after, limit):
        return [self._task(row) for row in self.db.execute(
            """SELECT * FROM tasks WHERE id>? AND status IN ('done','dropped')
                AND worktree IS NOT NULL AND worktree!='' ORDER BY id LIMIT ?""", (after, limit))]

    def message_save(self, topic, kind, text, telegram_message=None, images=None, reacted_to=None):
        if kind not in ('owner', 'forwarded', 'worker_result', 'restarted', 'callback', 'secret_filled', 'event'):
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
        self.wait_hook('secret-trigger-failed', self.wait_secret_filled, topic, row['id'])
        return dict(row)

    def message_event(self, topic, text, from_name=None, preview=True):
        if not self.db.in_transaction:
            with self.db:
                self.db.execute('BEGIN IMMEDIATE')
                return self.message_event(topic, text, from_name, preview)
        if not self.topic(topic):
            raise ValueError('Unknown channel')
        now = time.time()
        self.db.execute("INSERT INTO messages(topic,kind,text,created,updated,from_name) VALUES (?,'event',?,?,?,?)",
                        (topic, text, now, now, from_name))
        event = dict(self.db.execute('SELECT * FROM messages WHERE id=last_insert_rowid()').fetchone())
        if preview and from_name is not None and not self.get('topic_lifecycle:' + topic):
            preview = text
            if len(preview) > 800:
                cut = max(preview.rfind('\n', 400, 799), preview.rfind(' ', 400, 799))
                preview = preview[:cut if cut > 0 else 799].rstrip() + '…'
            self.db.execute("INSERT INTO outbox(topic,kind,text) VALUES (?,'local-note',?)",
                            (topic, '🤖 ' + from_name + '\n' + preview))
        return event

    def record_cli_call(self, op, from_name, topic, state, process):
        with self.db:
            if not self.db.in_transaction:
                self.db.execute('BEGIN IMMEDIATE')
            self.db.execute('INSERT INTO cli_calls(created,op,from_name,topic,state,process) VALUES (?,?,?,?,?,?)',
                            (time.time(), op, from_name, topic, state, process))
            self.db.execute('DELETE FROM cli_calls WHERE id NOT IN (SELECT id FROM cli_calls ORDER BY id DESC LIMIT ?)',
                            (CLI_CALL_LIMIT,))

    def message_sending(self, message_id):
        with self.db:
            cursor = self.db.execute("UPDATE messages SET delivered='sent',updated=? WHERE id=? AND delivered='pending'",
                                     (time.time(), message_id))
        return bool(cursor.rowcount)

    def message_batch_ids(self, message_ids):
        return {row['id']: row['batch_id'] or row['id'] for message_id in message_ids
                for row in self.db.execute('SELECT id,batch_id FROM messages WHERE id=?', (message_id,))}

    def message_batch_rows(self, message_id):
        saved = self.db.execute('SELECT batch_id FROM messages WHERE id=?', (message_id,)).fetchone()
        if saved is None or saved['batch_id'] is None:
            return {message_id}
        anchor = saved['batch_id']
        return {row[0] for row in self.db.execute(
            'SELECT id FROM messages WHERE batch_id=? OR id=?', (anchor, anchor))}

    def reconcile_message_batches(self):
        with self.transaction():
            pending = self.messages_pending()
            batches = {}
            for row in pending:
                if row['batch_id'] is not None:
                    batches.setdefault(row['batch_id'], set()).add(row['id'])
            partial = {anchor for anchor, members in batches.items() if members != self.message_batch_rows(anchor)}
            self.db.executemany("UPDATE messages SET batch_id=NULL WHERE batch_id=? AND delivered='pending'",
                                [(anchor,) for anchor in partial])
            for row in pending:
                if row['batch_id'] in partial:
                    row['batch_id'] = None
        return pending

    def message_batch_sending(self, message_ids):
        if not message_ids:
            return False
        anchor = message_ids[0]
        with self.transaction():
            rows = [self.db.execute('SELECT id,delivered,batch_id FROM messages WHERE id=?', (message_id,)).fetchone()
                    for message_id in message_ids]
            if len(set(message_ids)) != len(rows) or any(row is None or row['delivered'] != 'pending' for row in rows):
                return False
            assigned = {row['batch_id'] for row in rows if row['batch_id'] is not None}
            if assigned and (assigned != {anchor} or self.message_batch_rows(anchor) != set(message_ids)):
                return False
            if not assigned:
                self.db.execute('UPDATE messages SET batch_id=NULL WHERE batch_id=?', (anchor,))
            self.db.executemany("UPDATE messages SET delivered='sent',batch_id=?,updated=? WHERE id=?",
                                [(anchor, time.time(), message_id) for message_id in message_ids])
        return True

    def messages_delivered(self, message_ids, receipt):
        state = ('uncertain' if receipt == 'uncertain' or
                 isinstance(receipt, dict) and receipt.get('status') == 'uncertain' else 'received')
        saved = receipt if isinstance(receipt, str) else json.dumps(receipt)
        with self.db:
            self.db.executemany('''UPDATE messages SET delivered=?,receipt=?,updated=?
                WHERE id=? AND delivered IN ('pending','sent')''',
                                [(state, saved, time.time(), message_id) for message_id in message_ids])

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
        self.messages_delivered([message_id], receipt)
        row = self.db.execute('SELECT * FROM messages WHERE id=?', (message_id,)).fetchone()
        return dict(row) if row else None

    def worker_complete(self, worker_id, result, status='done'):
        with self.transaction():
            worker = self.db.execute('SELECT * FROM workers WHERE id=?', (worker_id,)).fetchone()
            if not worker:
                raise ValueError('Unknown worker')
            saved = self.db.execute('SELECT * FROM messages WHERE source_worker=? ORDER BY id DESC LIMIT 1',
                                    (worker_id,)).fetchone()
            if saved and saved['created'] >= worker['updated']:
                return dict(saved)
            now = time.time()
            payload = dict(result)
            if self.get('auto_resume', False) is True and not getattr(self, 'network_available', True):
                payload['network_down'] = True
            if self.get('auto_resume', False) is True and payload.get('failure_code') == 'owner_stopped':
                stop = self.db.execute("""SELECT params FROM service_requests WHERE op='workers.stop'
                    AND json_extract(params,'$.worker')=? ORDER BY id DESC LIMIT 1""", (worker_id,)).fetchone()
                if stop and json.loads(stop['params']).get('_stop_source') == 'mcp':
                    payload.update(failure_code='coordinator_stopped', error='The coordinator stopped this worker.')
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
                                every=60 if payload.get('failure_code') == 'cleanup_failed' else None,
                                topic=worker['topic'], task=worker['task'], worker=worker_id)
            job = self.task_get(worker['task'])
            from .session_storage import request as request_session_cleanup
            request_session_cleanup(self)
            self.host_logs_changed(**({'task': worker['task']} if job and job['status'] in ('done', 'dropped')
                                      else {'worker': worker_id}))
            if status in ('done', 'interrupted'):
                from .workspaces import request_closed_copy_cleanup
                request_closed_copy_cleanup(self, worker['task'], worker['cwd'])
            return dict(self.db.execute('SELECT * FROM messages WHERE id=?', (cursor.lastrowid,)).fetchone())

    def worker_event_consumed(self, worker_id, seq):
        with self.db:
            self.db.execute('UPDATE workers SET last_seq=MAX(last_seq,?),updated=? WHERE id=?',
                            (seq, time.time(), worker_id))

    def requests_uncertain(self):
        with self.db:
            self.db.execute("DELETE FROM settings WHERE key LIKE 'wait_sending:%'")
            self.db.execute("UPDATE service_requests SET state='queued',updated=? WHERE state='sending' AND op IN ('tldr','topic.rename')",
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
            row = self.db.execute('SELECT op,params FROM service_requests WHERE id=?', (request_id,)).fetchone()
            if row and row['op'] in ('workers.steer', 'workers.goal'):
                self.host_logs_changed(worker=json.loads(row['params'])['worker'])

    def link_created_topic(self, request, topic, chat, thread, name, cwd, settings, progress):
        with self.db:
            self.db.execute('INSERT INTO topics(id,chat,thread,name,telegram_title,cwd,enabled) VALUES (?,?,?,?,?,?,1)',
                            (topic, chat, thread, name, name, cwd))
            for key, value in settings.items():
                self.put(key, value)
            self.db.execute('UPDATE service_requests SET result=?,updated=? WHERE id=?',
                            (json.dumps(progress), time.time(), request))

    def bind(self, topic_id, cwd, name, provider=None, session=None, enabled=False, source_pid=None):
        if self.get('topic_lifecycle:' + topic_id):
            raise ValueError('This chat is closing or closed')
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
            if not self.db.in_transaction:
                self.db.execute('BEGIN IMMEDIATE')
            if self.get('topic_lifecycle:' + topic_id):
                raise ValueError('This chat is closing or closed')
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
                       image=None, as_document=False):
        """Queue text for a topic. `edit` names a delivered card to replace in place when the text fits one message."""
        text = text.strip() or "Finished with no text result."
        if image:
            first, remaining = _take_rendered(text, 1024, next_limit=MESSAGE_LIMIT)
            parts = [first]
            if remaining:
                parts.extend(_split_rendered(remaining, MESSAGE_LIMIT))
        else:
            parts = _split_rendered(text, MESSAGE_LIMIT)
        if kind == 'parent-reply' and len(parts) > 4:
            notice = "\n\n(Cut here. The rest is in this chat's own history.)"
            parts = parts[:3] + [_split_rendered(parts[3], MESSAGE_LIMIT - len(notice))[0] + notice]
        edit = edit if len(parts) == 1 and not image else None
        for index, part in enumerate(parts):
            markup = json.dumps(reply_markup) if reply_markup and index == len(parts) - 1 else None
            cursor = self.db.execute("""INSERT INTO outbox
                (topic,kind,text,reply_to,reply_markup,edit_message,image,as_document,created,retired) VALUES (?,?,?,?,?,?,?,?,?,
                EXISTS(SELECT 1 FROM settings WHERE key=? AND json_extract(value,'$.state') IS NOT NULL))""",
                                     (topic, kind, part, None if edit else reply_to, markup, edit,
                                      image if index == 0 else None, int(bool(as_document and image and index == 0)),
                                      time.time(), 'topic_lifecycle:' + topic))
        return cursor.lastrowid

    def _forwarded_from(self, message):
        if not any(key in message for key in (
                'forward_origin', 'forward_from', 'forward_from_chat', 'forward_sender_name', 'forward_date')):
            return None
        origin = message.get('forward_origin')
        origin = origin if isinstance(origin, dict) else {}
        sender = origin.get('sender_user') if origin.get('type') == 'user' else None
        legacy_sender = message.get('forward_from')
        owner = self.get('owner')
        if owner is not None and any(isinstance(user, dict) and user.get('id') == owner
                                     for user in (sender, legacy_sender)):
            return None
        sender = sender if isinstance(sender, dict) else legacy_sender
        if isinstance(sender, dict):
            from .pairing import describe_sender
            return describe_sender(sender)
        chat = origin.get('chat') or origin.get('sender_chat') or message.get('forward_from_chat')
        if isinstance(chat, dict) and chat.get('title'):
            return str(chat['title'])
        return str(origin.get('sender_user_name') or message.get('forward_sender_name') or 'Unknown sender')

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
                row = self.topic(topic)
                if row:
                    name = edited['name'].strip()[:128]
                    if row['name'] != name or row['telegram_title'] != name:
                        self.db.execute("UPDATE topics SET name=?,telegram_title=? WHERE id=?", (name, name, topic))
                        logger.info("topic renamed topic=%s", topic)
                    return 'service_event'
            forwarded_from = self._forwarded_from(message)
            parts = text.split(maxsplit=1) if forwarded_from is None else []
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
            from .updates import accept_reply
            outcome = accept_reply(self, topic, message)
            if outcome:
                return outcome
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
            from .topics import hold_message
            held = hold_message(self, topic, message)
            if held:
                return held
            from .onboarding import first_project
            outcome = first_project(self, topic, message) if self.topic(topic) and forwarded_from is None else None
            if outcome:
                return outcome
            return self._accept_topic_message(message, topic, thread, text, command)

    def _accept_topic_message(self, message, topic, thread, text, command):
        forwarded_from = self._forwarded_from(message)
        if forwarded_from is not None:
            command = ''
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
        if forwarded_from is None and handle_control(self, topic, message["message_id"], text):
            return "control"
        if command == "/ping":
            self.enqueue_report(topic, "Bridge received your message. No agent was started.", reply_to=message["message_id"])
            return "ping"
        from .control_ui import accept_text, setup_report
        if forwarded_from is None and accept_text(
                self, topic, text, message['message_id'], message.get('reply_to_message', {}).get('message_id')):
            return 'control'
        if not config["enabled"] and command != '/goal':
            from .signin import topic_code_pointer
            if not text.startswith('/') and topic_code_pointer(self, topic, message):
                return 'envelope_intercept'
            from .onboarding import setup_guide, setup_reply
            reply = (setup_guide(self, topic) if forwarded_from is not None else
                     setup_reply(self, topic, text) if not text.startswith('/') else 'Unknown command. Send /help.')
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
                (topic,message,file_id,mime,size,group_id,name,kind,duration,created) VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (topic, message["message_id"], media['file_id'], media['mime'], media['size'],
                 media['group_id'], media['name'], media['kind'], media['duration'], time.time()))
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
                                       [media] if media else None, forwarded_from=forwarded_from)
        if forwarded_from is None and reply is not None and not text.lstrip().startswith('/'):
            from .waits import accept_reply
            self.wait_hook('reply-trigger-failed', accept_reply, self, topic, reply, row['id'], text)
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
        logger.info('%s message saved message=%s telegram_age=%s', row['kind'], row['id'],
                    '%.1f' % (time.time() - date) if type(date) is int else 'unknown')
        return "queued"

    def _save_owner_message(self, topic, message, text, reply=None, images=None, reacted_to=None, forwarded_from=None):
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
        kind = 'owner'
        if forwarded_from is not None:
            kind = 'forwarded'
            text = 'Forwarded from ' + json.dumps(forwarded_from, ensure_ascii=False) + ':\n' + text
        return self.message_save(topic, kind, text, telegram_message=message, images=images, reacted_to=reacted_to)


    def pending_delivery(self):
        if self.get('setup_problem') == 'removed':
            return None
        row = self.db.execute("""SELECT o.*,t.chat,t.thread FROM outbox o JOIN topics t ON t.id=o.topic
            WHERE o.delivered=0 AND o.retired=0 AND (t.chat=? OR o.kind='group-setup') AND o.next_attempt<=?
            AND (o.kind NOT IN ('wait-ask','owner-waiting') OR ?=1)
            AND (o.kind!='local-note' OR NOT EXISTS
                (SELECT 1 FROM settings WHERE key='topic_lifecycle:'||o.topic AND json_extract(value,'$.state') IS NOT NULL))
            AND (? != 'topics_off' OR t.thread=0) AND NOT EXISTS
            (SELECT 1 FROM outbox p WHERE p.topic=o.topic AND p.delivered=0 AND p.retired=0 AND p.id<o.id
                AND (p.kind NOT IN ('wait-ask','owner-waiting') OR ?=1))
            ORDER BY o.id LIMIT 1""", (self.chat(), time.time(), int(self.get('waits_enabled', False) is True),
                                       self.get('setup_problem') or '', int(self.get('waits_enabled', False) is True))).fetchone()
        if row is None:
            return None
        result = dict(row)
        return result

    def outbox_has_pending(self):
        if self.get('owner') is None or self.chat() is None:
            return False
        return self.db.execute('''SELECT 1 FROM outbox o JOIN topics t ON t.id=o.topic
            WHERE o.delivered=0 AND o.retired=0 AND t.chat=?
                AND (o.kind NOT IN ('wait-ask','owner-waiting') OR ?=1) LIMIT 1''',
                               (self.chat(), int(self.get('waits_enabled', False) is True))).fetchone() is not None

    def delivered(self, row_id, telegram_message):
        with self.db:
            previous = self.db.execute("""SELECT reaction_desired,reaction_sent,reaction_state,
                reaction_retry,reaction_attempts,reaction_first_failed FROM outbox WHERE telegram_message=? AND id!=?
                AND topic=(SELECT topic FROM outbox WHERE id=?) ORDER BY id DESC LIMIT 1""",
                                       (telegram_message, row_id, row_id)).fetchone()
            if previous:
                self.db.execute("""UPDATE outbox SET reaction_desired=?,reaction_sent=?,reaction_state=?,
                    reaction_retry=?,reaction_attempts=?,reaction_first_failed=? WHERE id=?""", (*previous, row_id))
            self.db.execute("""UPDATE outbox SET telegram_message=NULL WHERE telegram_message=? AND id!=?
                               AND topic=(SELECT topic FROM outbox WHERE id=?)""", (telegram_message, row_id, row_id))
            self.db.execute("UPDATE outbox SET delivered=1,telegram_message=? WHERE id=?", (telegram_message, row_id))
            from .close_saves import request
            image = self.db.execute('SELECT image FROM outbox WHERE id=?', (row_id,)).fetchone()
            if image and image['image']:
                request(self, cwd=image['image'])
            from .parent_replies import delivered
            delivered(self, row_id)
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
