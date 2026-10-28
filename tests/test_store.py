import json
import hashlib
import re
import sqlite3
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch
from xml.etree import ElementTree

from coordinator.store import Store, _split_rendered
from coordinator.formatting import markdown_to_html
from coordinator.controls import handle_control
from tests.support import telegram_text


def update(number, text, user=7, thread=4, **extra):
    return {'update_id': number, 'message': {
        'message_id': number, 'from': {'id': user, 'is_bot': False},
        'chat': {'id': -10042, 'type': 'supergroup'},
        'message_thread_id': thread, 'text': text, **extra}}


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store = Store(self.root)
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('-10042:4',-10042,4,'test','',1)")
            self.store.put('owner', 7)
            self.store.put('group', -10042)
            self.store.put('mode', 'group')

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def test_delivery_excludes_stale_chats_and_retired_rows(self):
        self.store.put('mode', 'group')
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('7:0',7,0,'private','')")
        self.store.enqueue_report('7:0', 'Wrong chat')
        retired = self.store.enqueue_report('-10042:4', 'Retired report')
        self.store.db.execute('UPDATE outbox SET retired=1 WHERE id=?', (retired,))
        self.store.db.execute("UPDATE topics SET enabled=0 WHERE id='-10042:4'")
        self.store.enqueue_report('-10042:4', 'Current status')
        row = self.store.pending_delivery()
        self.assertEqual((row['chat'], row['text']), (-10042, 'Current status'))
        self.assertTrue(self.store.outbox_has_pending())
        self.store.delivered(row['id'], 100)
        self.assertIsNone(self.store.pending_delivery())
        self.assertFalse(self.store.outbox_has_pending())
        self.store.put('owner', None)
        self.assertIsNone(self.store.pending_delivery())
        self.store.close()
        self.store = Store(self.root)
        self.assertEqual(self.store.db.execute('SELECT retired FROM outbox WHERE id=?',
                                              (retired,)).fetchone()[0], 1)

    def test_group_migration_keeps_recorded_admission_and_topics(self):
        recorded = [update(901, 'Build it'), update(902, '/ping'),
                    update(903, 'stranger', user=99), update(904, '', forum_topic_edited={'name': 'Renamed'}),
                    update(905, 'root reply', reply_to_message={'message_id': 4})]
        expected = ['queued', 'ping', 'unauthorized', 'service_event', 'queued']
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('-10042:5',-10042,5,'second','',1)")
        topics = self.store.topics()
        self.store.close()
        self.store = Store(self.root)
        self.assertEqual(self.store.get('mode'), 'group')
        self.assertEqual(self.store.chat(), -10042)
        self.assertEqual(self.store.topics(), topics)
        self.assertEqual([self.store.accept(item) for item in recorded], expected)
        self.store.close()
        self.store = Store(self.root)
        self.assertEqual(self.store.get('mode'), 'group')
        self.assertEqual(len(self.store.topics()), 2)

    def test_unpaired_migration_keeps_mode_absent(self):
        with tempfile.TemporaryDirectory() as temp:
            fresh = Store(Path(temp))
            self.assertIsNone(fresh.get('mode'))
            fresh.close()

    def test_fresh_install_keeps_finish_unset_after_home_is_configured_and_reopened(self):
        with self.store.db:
            self.store.put('coordinator_home_topic', '-10042:4')
        self.assertIsNone(self.store.task_create('-10042:4', 'Before restart')['finish'])
        self.store.close()
        self.store = Store(self.root)
        self.assertIsNone(self.store.task_create('-10042:4', 'After restart')['finish'])
        self.assertIsNone(self.store.get('finish_default:-10042:4'))

    def test_basic_group_upgrade_migrates_code_request_links_and_settings(self):
        from coordinator.setup_flow import migrate
        with self.store.db:
            self.store.db.execute('''CREATE TABLE IF NOT EXISTS code_requests (
                id INTEGER PRIMARY KEY, tenant TEXT, server TEXT, agent TEXT, page_id TEXT,
                job INTEGER REFERENCES tasks(id), topic TEXT REFERENCES topics(id),
                repo TEXT, state TEXT, created REAL)''')
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES ('-42:35',-42,35,'Code requests','',1)''')
            self.store.put('group', -42)
            self.store.put('code_requests', {'topic': '-42:35'})
        job = self.store.task_create('-42:35', 'sample request: x')
        with self.store.db:
            self.store.db.execute('''INSERT INTO code_requests(tenant,server,agent,page_id,job,topic,repo,state,created)
                VALUES ('t','https://k.example','a','p',?,'-42:35','o/r','linked',0)''', (job['id'],))
            self.store.db.execute('''INSERT INTO code_requests(tenant,server,agent,page_id,topic,repo,state,created)
                VALUES ('t','https://k.example','a','ignored','-42:35','o/r','ignored',0)''')
        with self.store.db:
            self.assertTrue(migrate(self.store, -42, -10042))
        self.assertEqual(self.store.chat(), -10042)
        self.assertEqual(self.store.task_get(job['id'])['topic'], '-10042:35')
        self.assertEqual([row['topic'] for row in self.store.db.execute('SELECT topic FROM code_requests')],
                         ['-10042:35', '-10042:35'])
        self.assertEqual(self.store.get('code_requests'), {'topic': '-10042:35'})
        self.assertEqual(self.store.db.execute('PRAGMA foreign_key_check').fetchall(), [])
        self.store.close()
        self.store = Store(self.root)
        self.assertIsNotNone(self.store.topic('-10042:35'))

    def test_reopen_keeps_the_current_schema(self):
        version = self.store.db.execute('PRAGMA schema_version').fetchone()[0]
        self.store.close()
        self.store = Store(self.root)
        self.assertEqual(self.store.db.execute('PRAGMA schema_version').fetchone()[0], version)

    def test_event_kind_is_accepted_and_persistent(self):
        direct = self.store.message_save('-10042:4', 'event', 'Lane stopped.')
        event = self.store.message_event('-10042:4', 'Approval needed.')
        self.assertEqual([row['id'] for row in self.store.messages_pending()], [direct['id'], event['id']])
        self.assertEqual((event['kind'], event['text'], event['telegram_message'], event['source_worker'],
                          event['reaction_desired'], event['turn_started'], event['turn_tools']),
                         ('event', 'Approval needed.', None, None, None, None, 0))
        self.store.close()
        self.store = Store(self.root)
        self.assertEqual(self.store.messages_pending(), [direct, event])

    def test_event_rolls_back_with_the_callers_transaction(self):
        with self.assertRaisesRegex(RuntimeError, 'request failed'):
            with self.store.db:
                self.store.put('event_request', 'finishing')
                self.store.message_event('-10042:4', 'Request finished.')
                raise RuntimeError('request failed')
        self.assertIsNone(self.store.get('event_request'))
        self.assertEqual(self.store.messages_pending(), [])

    def test_forwarded_kind_upgrades_event_schema_without_losing_rows_or_indexes(self):
        owner = self.store.message_save('-10042:4', 'owner', 'Keep owner text.', telegram_message=101,
                                        images=[{'file_id': 'fixture-image'}])
        event = self.store.message_event('-10042:4', 'Keep event text.')
        self.store.db.commit()
        self.store.task_create('-10042:4', 'Keep origin.', origin=owner['id'])
        self.store.db.execute('UPDATE messages SET reaction_first_failed=123 WHERE id=?', (owner['id'],))
        self.store.db.execute('CREATE INDEX messages_history ON messages(topic,created,id)')
        self.store.db.commit()
        before = [tuple(row) for row in self.store.db.execute('SELECT * FROM messages ORDER BY id')]
        columns = [tuple(row) for row in self.store.db.execute('PRAGMA table_info(messages)')]
        indexes = list(self.store.db.execute(
            "SELECT name,sql FROM sqlite_master WHERE type='index' AND tbl_name='messages' ORDER BY name"))
        old_schema = self.store.db.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='messages'").fetchone()[0].replace("'forwarded',", '')
        self.store.close()
        with sqlite3.connect(self.root / 'state.sqlite') as db:
            db.execute('DROP TABLE messages')
            db.execute(old_schema)
            for row in before:
                db.execute('INSERT INTO messages VALUES (' + ','.join('?' for _ in row) + ')', row)
            for _, sql in indexes:
                db.execute(sql)
        self.store = Store(self.root)
        self.assertEqual([tuple(row) for row in self.store.db.execute('SELECT * FROM messages ORDER BY id')], before)
        self.assertEqual([tuple(row) for row in self.store.db.execute('PRAGMA table_info(messages)')], columns)
        self.assertEqual([tuple(row) for row in self.store.db.execute(
            "SELECT name,sql FROM sqlite_master WHERE type='index' AND tbl_name='messages' ORDER BY name")],
                         [tuple(row) for row in indexes])
        self.assertEqual(self.store.db.execute('SELECT origin FROM tasks').fetchone()[0], owner['id'])
        self.assertEqual(self.store.db.execute('PRAGMA foreign_key_check').fetchall(), [])
        forwarded = self.store.message_save('-10042:4', 'forwarded', 'Forwarded from "Fixture":\nContent.')
        self.assertEqual(forwarded['id'], event['id'] + 1)
        version = self.store.db.execute('PRAGMA schema_version').fetchone()[0]
        self.store.close()
        self.store = Store(self.root)
        self.assertEqual(self.store.db.execute('PRAGMA schema_version').fetchone()[0], version)
        self.assertEqual(self.store.messages_pending(),
                         [dict(zip((column[1] for column in columns), row)) for row in before] + [forwarded])

    def test_old_message_schema_rebuild_keeps_rows_columns_indexes_and_ids(self):
        self.store.close()
        with sqlite3.connect(self.root / 'state.sqlite') as db:
            db.execute('DROP TABLE messages')
            db.execute('''CREATE TABLE messages (
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
            worker = db.execute('''INSERT INTO workers (topic,provider,prompt,created,updated)
                VALUES ('-10042:4','codex','fixture',1,2)''').lastrowid
            envelope = db.execute('''INSERT INTO envelopes
                (name,reason,consumer,topic,state,created,expires,updated)
                VALUES ('FIXTURE','fixture','test','-10042:4','filled',1,2,3)''').lastrowid
            for message_id, kind, delivered in ((3, 'owner', 'pending'), (7, 'worker_result', 'sent'),
                                               (19, 'restarted', 'received'), (31, 'callback', 'uncertain'),
                                               (95, 'secret_filled', 'received')):
                db.execute('''INSERT INTO messages VALUES
                    (?,'-10042:4',?,?,?,?,'[]',?,?,'fixture receipt','sent','👀','👀',4,5,1,2,6,7,8,9,10)''',
                           (message_id, kind, message_id if kind == 'owner' else None, 'saved ' + kind,
                            worker if kind == 'worker_result' else None,
                            envelope if kind == 'secret_filled' else None, delivered))
            for statement in (
                    'CREATE UNIQUE INDEX messages_telegram ON messages(topic,telegram_message) WHERE telegram_message IS NOT NULL',
                    'CREATE INDEX messages_workers ON messages(source_worker)',
                    'CREATE UNIQUE INDEX messages_envelopes ON messages(source_envelope) WHERE source_envelope IS NOT NULL',
                    'CREATE INDEX messages_history ON messages(topic,created,id)'):
                db.execute(statement)
            db.execute("INSERT INTO tasks(topic,number,title,origin,created,updated) VALUES ('-10042:4',1,'fixture',3,1,2)")
            before = db.execute('SELECT * FROM messages ORDER BY id').fetchall()
            columns = db.execute('PRAGMA table_info(messages)').fetchall()
            indexes = db.execute("SELECT name,sql FROM sqlite_master WHERE type='index' AND tbl_name='messages' ORDER BY name").fetchall()
        self.store = Store(self.root)
        self.assertEqual([tuple(row) for row in self.store.db.execute('SELECT * FROM messages ORDER BY id')],
                         [row + (None, None, None) for row in before])
        self.assertEqual([tuple(row) for row in self.store.db.execute('PRAGMA table_info(messages)')],
                         columns + [(len(columns), 'from_name', 'TEXT', 0, None, 0),
                                    (len(columns) + 1, 'reaction_first_failed', 'REAL', 0, None, 0),
                                    (len(columns) + 2, 'batch_id', 'INTEGER', 0, None, 0)])
        self.assertEqual([tuple(row) for row in self.store.db.execute(
            "SELECT name,sql FROM sqlite_master WHERE type='index' AND tbl_name='messages' ORDER BY name")],
                         sorted(indexes + [('messages_batches',
                             'CREATE INDEX messages_batches ON messages(batch_id) WHERE batch_id IS NOT NULL')]))
        self.assertEqual(self.store.db.execute('SELECT origin FROM tasks').fetchone()[0], 3)
        self.assertEqual(self.store.db.execute('PRAGMA foreign_key_check').fetchall(), [])
        event = self.store.message_save('-10042:4', 'event', 'New event.')
        self.assertEqual(event['id'], 96)
        self.store.close()
        self.store = Store(self.root)
        self.assertEqual(self.store.message_event('-10042:4', 'Next event.')['id'], 97)

    def test_failed_checkpoint_open_rolls_back_the_entire_migration(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fixture = Path(__file__).parent / 'fixtures/store-checkpoint.sql'
            db = sqlite3.connect(root / 'state.sqlite')
            try:
                db.executescript(fixture.read_text())
                with db:
                    db.execute('UPDATE tasks SET origin=999999 WHERE id=1')
                self.assertEqual(db.execute('PRAGMA foreign_key_check').fetchall(),
                                 [('tasks', 1, 'messages', 0)])
                before_dump = list(db.iterdump())
                before_master = db.execute(
                    'SELECT type,name,tbl_name,rootpage,sql FROM sqlite_master ORDER BY type,name').fetchall()
                self.assertNotIn("'event'", db.execute(
                    "SELECT sql FROM sqlite_master WHERE name='messages'").fetchone()[0])
            finally:
                db.close()
            with self.assertRaisesRegex(sqlite3.IntegrityError, 'Migration left invalid foreign keys'):
                Store(root)
            db = sqlite3.connect(root / 'state.sqlite')
            try:
                self.assertEqual(list(db.iterdump()), before_dump)
                self.assertEqual(db.execute(
                    'SELECT type,name,tbl_name,rootpage,sql FROM sqlite_master ORDER BY type,name').fetchall(),
                    before_master)
                self.assertNotIn("'event'", db.execute(
                    "SELECT sql FROM sqlite_master WHERE name='messages'").fetchone()[0])
            finally:
                db.close()

    def test_task_create_without_origin_keeps_null_with_owner_messages(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fixture = Path(__file__).parent / 'fixtures/store-checkpoint.sql'
            db = sqlite3.connect(root / 'state.sqlite')
            try:
                db.executescript(fixture.read_text())
            finally:
                db.close()
            upgraded = Store(root)
            try:
                for shape, store, topic in (('fresh', self.store, '-10042:4'),
                                            ('upgraded', upgraded, '-1001073001:2')):
                    with self.subTest(shape=shape):
                        store.message_save(topic, 'owner', 'Earlier owner request')
                        store.message_save(topic, 'owner', 'Newest owner request')
                        job = store.task_create(topic, 'Job without an origin')
                        self.assertIsNone(job['origin'])
                        self.assertIsNone(store.db.execute(
                            'SELECT origin FROM tasks WHERE id=?', (job['id'],)).fetchone()[0])
            finally:
                upgraded.close()

    def test_owner_only_intake_and_duplicate_cursor(self):
        self.assertEqual(self.store.accept(update(1, 'intrusion', user=8)), 'unauthorized')
        self.assertEqual(self.store.accept(update(2, 'Build it')), 'queued')
        self.assertEqual(self.store.accept(update(2, 'changed')), 'duplicate')
        self.assertEqual(self.store.get('offset'), 3)
        self.assertEqual([row['text'] for row in self.store.messages_pending()], ['Build it'])

    def test_tasks_number_per_topic_and_reopen(self):
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('-10042:5',-10042,5,'other','',1)")
        first = self.store.task_create('-10042:4', 'First')
        second = self.store.task_create('-10042:4', 'Second')
        other = self.store.task_create('-10042:5', 'Other')
        self.assertEqual([first['number'], second['number'], other['number']], [1, 2, 1])
        changed = self.store.task_update(first['id'], status='done', notes='Verified')
        self.assertEqual((changed['status'], changed['notes']), ('done', 'Verified'))
        self.store.close()
        self.store = Store(self.root)
        self.assertEqual(self.store.task_get(first['id'])['status'], 'done')
        self.assertEqual([row['id'] for row in self.store.tasks_list(status='open')],
                         [second['id'], other['id']])

    def test_task_pr_validation_with_and_without_private_addon(self):
        task = self.store.task_create('-10042:4', 'First')
        for addon in (self.store.code_requests, None):
            with self.subTest(private=addon is not None), patch.object(self.store, 'code_requests', addon):
                for pr in ('not-a-github-url', '', None, 1, 'https://github.com/team/repo/pull/0',
                           'https://github.com/team/../pull/1', 'https://github.com/team/repo/pull/1?x=1',
                           'https://github.com/team/' + 'a' * 180 + '/pull/1'):
                    with self.subTest(pr=pr), self.assertRaisesRegex(
                            ValueError, r'^pr must be a GitHub pull request URL\.$'):
                        self.store.task_update(task['id'], pr=pr, status='done', stage='ready')
                    self.assertEqual(self.store.task_get(task['id']), task)
                self.assertEqual(self.store.db.execute('SELECT count(*) FROM task_stamps').fetchone()[0], 0)
        changed = self.store.task_update(task['id'], pr='https://github.com/team/repo/pull/1')
        self.assertEqual(changed['pr'], 'https://github.com/team/repo/pull/1')

    def test_task_update_note_appends_to_existing_notes(self):
        task = self.store.task_create('-10042:4', 'First', notes='a')
        changed = self.store.task_update(task['id'], note=' b \n')
        self.assertEqual(changed['notes'], 'a\nb')
        self.store.close()
        self.store = Store(self.root)
        self.assertEqual(self.store.task_get(task['id'])['notes'], 'a\nb')

    def test_concurrent_task_note_appends_keep_both_notes(self):
        task = self.store.task_create('-10042:4', 'First', notes='original')
        other = Store(self.root)
        self.addCleanup(other.close)
        other.db.execute('PRAGMA busy_timeout=0')
        read = self.store.task_get
        attempted = blocked = False

        def read_with_other_writer(task_id):
            nonlocal attempted, blocked
            row = read(task_id)
            if not attempted:
                attempted = True
                try:
                    other.task_update(task_id, note='second')
                except sqlite3.OperationalError as error:
                    self.assertEqual(str(error), 'database is locked')
                    blocked = True
            return row

        with patch.object(self.store, 'task_get', side_effect=read_with_other_writer):
            self.store.task_update(task['id'], note='first')
        if blocked:
            other.task_update(task['id'], note='second')
        self.assertEqual(self.store.task_get(task['id'])['notes'], 'original\nfirst\nsecond')

    def test_task_update_note_on_empty_notes_has_no_leading_newline(self):
        for notes in (None, ''):
            with self.subTest(notes=notes):
                task = self.store.task_create('-10042:4', 'First', notes=notes)
                changed = self.store.task_update(task['id'], note='b')
                self.assertEqual(changed['notes'], 'b')

    def test_task_update_note_and_notes_are_refused_without_mutation(self):
        task = self.store.task_create('-10042:4', 'First', notes='a')
        with self.assertRaises(ValueError):
            self.store.task_update(task['id'], note='b', notes='c', status='done')
        self.assertEqual(self.store.task_get(task['id']), task)

    def test_task_update_empty_note_is_refused_without_mutation(self):
        task = self.store.task_create('-10042:4', 'First', notes='a')
        for note in ('', ' \t\n'):
            with self.subTest(note=note), self.assertRaises(ValueError):
                self.store.task_update(task['id'], note=note, status='done', stage='ready')
            self.assertEqual(self.store.task_get(task['id']), task)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM task_stamps').fetchone()[0], 0)

    def test_task_update_note_applies_status_title_and_stage(self):
        task = self.store.task_create('-10042:4', 'First', notes='a')
        changed = self.store.task_update(task['id'], note='b', status='done', title='Second', stage='ready')
        self.assertEqual((changed['notes'], changed['status'], changed['title']), ('a\nb', 'done', 'Second'))
        self.assertEqual(self.store.db.execute('SELECT task,stage FROM task_stamps').fetchone()[:],
                         (task['id'], 'ready'))

    def test_closing_task_interrupts_waiting_workers(self):
        task = self.store.task_create('-10042:4', 'Waiting')
        with self.store.db:
            for status in ('done', 'waiting_for_quota'):
                self.store.db.execute("INSERT INTO workers (task,topic,provider,prompt,status,created,updated) "
                                      "VALUES (?, '-10042:4', 'claude', 'Question', ?, 1, 1)",
                                      (task['id'], status))
        self.store.task_update(task['id'], status='done')
        self.assertEqual([row[0] for row in self.store.db.execute('SELECT status FROM workers ORDER BY id')],
                         ['done', 'interrupted'])

    def test_startup_migrates_all_legacy_needs_input_workers_to_done(self):
        closed = self.store.task_create('-10042:4', 'Closed')
        open_task = self.store.task_create('-10042:4', 'Superseded')
        newest = self.store.task_create('-10042:4', 'Newest on open job')
        with self.store.db:
            for task in (closed, open_task, newest):
                self.store.db.execute("INSERT INTO workers (task,topic,provider,prompt,status,created,updated) "
                                      "VALUES (?, '-10042:4', 'claude', 'Question', 'needs_input', 1, 1)",
                                      (task['id'],))
            self.store.db.execute("INSERT INTO workers (task,topic,provider,prompt,status,created,updated) "
                                  "VALUES (?, '-10042:4', 'claude', 'Follow up', 'done', 2, 2)",
                                  (open_task['id'],))
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET status='dropped' WHERE id=?", (closed['id'],))
        unchanged = {}
        with self.store.db:
            for index, status in enumerate(('queued', 'waiting_for_secret', 'waiting_for_quota',
                                            'running', 'done', 'interrupted')):
                session, fresh, updated = 'session-' + status, index % 2, 100 + index
                worker = self.store.db.execute('''INSERT INTO workers
                    (task,topic,provider,prompt,status,session,fresh,created,updated)
                    VALUES (?, '-10042:4', 'claude', 'Keep unchanged', ?, ?, ?, 1, ?)''',
                    (newest['id'], status, session, fresh, updated)).lastrowid
                unchanged[worker] = (status, session, fresh, updated)
        for opening in (1, 2):
            with self.subTest(opening=opening):
                self.store.close()
                self.store = Store(self.root)
                self.assertEqual([row[0] for row in self.store.db.execute(
                    'SELECT status FROM workers WHERE id<? ORDER BY id', (min(unchanged),))],
                    ['done', 'done', 'done', 'done'])
                for worker, expected in unchanged.items():
                    with self.subTest(status=expected[0]):
                        row = self.store.db.execute('SELECT status,session,fresh,updated FROM workers WHERE id=?',
                                                    (worker,)).fetchone()
                        self.assertEqual(tuple(row), expected)

    def test_topic_names_can_repeat_across_channels(self):
        with self.store.db:
            self.store.db.executemany("INSERT INTO topics(id,chat,thread,name,cwd) VALUES (?,?,?,?,'')",
                                      [('-10042:5', -10042, 5, 'Idea'), ('-99:5', -99, 5, 'Elsewhere')])
        self.store.bind('-10042:4', self.root, 'Torii', enabled=True)
        self.store.bind('-10042:5', self.root, ' torii ')
        self.assertEqual(self.store.topic('-10042:5')['cwd'], str(self.root.resolve()))
        self.store.rename_topic('-10042:5', 'TORII')
        self.assertEqual(self.store.topic('-10042:5')['name'], 'TORII')
        self.store.bind('-99:5', self.root, 'Torii')
        self.assertEqual(self.store.rename_topic('-10042:4', 'torii'), 'Torii')

    def test_telegram_topic_rename_to_a_taken_name_updates_the_stored_name(self):
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('-10042:5',-10042,5,'Idea','')")
        self.store.bind('-10042:4', self.root, 'Torii', enabled=True)
        self.store.bind('-10042:5', self.root, 'Torii brainstorm', enabled=True)
        edited = {'update_id': 9, 'message': {'message_id': 9, 'from': {'id': 7}, 'message_thread_id': 5,
                                              'chat': {'id': -10042, 'type': 'supergroup'},
                                              'forum_topic_edited': {'name': 'Torii'}}}
        self.assertEqual(self.store.accept(edited), 'service_event')
        self.assertEqual(self.store.topic('-10042:5')['name'], 'Torii')
        edited['update_id'] = edited['message']['message_id'] = 10
        edited['message']['forum_topic_edited'] = {'name': 'Torii ideas'}
        self.store.accept(edited)
        self.assertEqual(self.store.topic('-10042:5')['name'], 'Torii ideas')

    def test_topic_edit_from_another_admin_updates_health_name_without_admitting_work(self):
        self.assertEqual(self.store.accept(update(8, 'Build it', user=8)), 'unauthorized')
        edited = update(9, '', user=8, forum_topic_edited={'name': '⛩️ Torii'})
        self.assertEqual(self.store.accept(edited), 'service_event')
        self.assertEqual(self.store.topic('-10042:4')['name'], '⛩️ Torii')
        with patch('coordinator.health.system_pressure', return_value=[]):
            self.assertTrue(handle_control(self.store, '-10042:4', 100, '/health'))
        health = self.store.db.execute('SELECT text FROM outbox ORDER BY id DESC LIMIT 1').fetchone()[0]
        self.assertIn('**⛩️ Torii**: 0 running', health)
        self.assertEqual(self.store.messages_pending(), [])
        anonymous = update(10, '', forum_topic_edited={'name': '⛩️ Torii - Brainstorming'})
        anonymous['message'].pop('from')
        anonymous['message']['sender_chat'] = {'id': -10042, 'type': 'supergroup'}
        self.assertEqual(self.store.accept(anonymous), 'service_event')
        self.assertEqual(self.store.topic('-10042:4')['name'], '⛩️ Torii - Brainstorming')
        other_group = update(11, '', user=8, forum_topic_edited={'name': 'Wrong group'})
        other_group['message']['chat']['id'] = -99
        self.assertEqual(self.store.accept(other_group), 'unauthorized')
        self.assertEqual(self.store.topic('-10042:4')['name'], '⛩️ Torii - Brainstorming')

    def test_bind_prefers_the_last_telegram_title_over_the_folder_name(self):
        created = {'update_id': 10, 'message': {'message_id': 10, 'from': {'id': 7}, 'message_thread_id': 5,
                                               'chat': {'id': -10042, 'type': 'supergroup'},
                                               'forum_topic_created': {'name': 'SharedMcp brainstorm'}}}
        self.store.accept(created)
        self.store.bind('-10042:5', self.root, 'shared_mcp')
        self.assertEqual(self.store.topic('-10042:5')['name'], 'SharedMcp brainstorm')

    def test_message_save_deduplicates_and_receipt_is_individual(self):
        first = self.store.message_save('-10042:4', 'owner', 'first', telegram_message=7,
                                        images=[{'file_id': 'image'}])
        duplicate = self.store.message_save('-10042:4', 'owner', 'changed', telegram_message=7)
        second = self.store.message_save('-10042:4', 'owner', 'second', telegram_message=8)
        self.assertEqual(duplicate, first)
        self.assertEqual(json.loads(first['images']), [{'file_id': 'image'}])
        self.assertTrue(self.store.message_sending(first['id']))
        self.assertFalse(self.store.message_sending(first['id']))
        self.assertEqual(self.store.message_delivered(first['id'], 'received')['delivered'], 'received')
        self.assertEqual([row['id'] for row in self.store.messages_pending()], [second['id']])

    def test_sent_input_becomes_uncertain_after_restart(self):
        first = self.store.message_save('-10042:4', 'owner', 'first', telegram_message=1)
        second = self.store.message_save('-10042:4', 'owner', 'second', telegram_message=2)
        self.store.message_sending(first['id'])
        self.store.messages_uncertain()
        self.assertEqual(self.store.db.execute('SELECT delivered FROM messages WHERE id=?',
                                               (first['id'],)).fetchone()[0], 'uncertain')
        self.assertEqual([row['id'] for row in self.store.messages_pending()], [second['id']])

    def test_accept_saves_reply_quote_without_job_or_question_flow(self):
        self.assertEqual(self.store.accept(update(1, 'Build it')), 'queued')
        outbox = self.store.enqueue_report('-10042:4', 'Which target?\nPlease choose.')
        self.store.delivered(outbox, 99)
        self.assertEqual(self.store.accept(update(2, 'Production', reply_to_message={'message_id': 99})), 'queued')
        self.assertEqual(self.store.accept(update(3, 'Follow-up', reply_to_message={'message_id': 1})), 'queued')
        rows = self.store.messages_pending()
        self.assertEqual([row['telegram_message'] for row in rows], [1, 2, 3])
        self.assertEqual(rows[1]['text'], 'Replying to: Which target? Please choose.\nProduction')
        self.assertEqual(rows[2]['text'], 'Replying to: Build it\nFollow-up')
        self.assertEqual(self.store.tasks_list(), [])

    def test_image_caption_splits_at_1024_and_never_edits(self):
        text = 'a' * 1024 + 'b' * 4000 + 'c' * 100
        with self.store.db:
            self.store.enqueue_report('-10042:4', text, image='/tmp/image.png', edit=44,
                                      reply_markup={'inline_keyboard': []})
        rows = self.store.db.execute('SELECT text,image,edit_message,reply_markup FROM outbox ORDER BY id').fetchall()
        self.assertEqual([len(row['text']) for row in rows], [1024, 4096, 4])
        self.assertEqual(''.join(row['text'] for row in rows), text)
        self.assertEqual([row['image'] for row in rows], ['/tmp/image.png', None, None])
        self.assertTrue(all(row['edit_message'] is None for row in rows))
        self.assertIsNotNone(rows[-1]['reply_markup'])

    def test_goal_is_plain_owner_input(self):
        self.assertEqual(self.store.accept(update(1, '/goal finish the work')), 'queued')
        self.assertEqual(self.store.messages_pending()[0]['text'], '/goal finish the work')

    def test_removed_status_and_unknown_commands_point_to_help_in_all_topics(self):
        for number, (enabled, command) in enumerate(
                ((True, '/status'), (True, '/unknown'), (False, '/status'), (False, '/unknown')), 1):
            with self.subTest(enabled=enabled, command=command), self.store.db:
                self.store.db.execute("UPDATE topics SET enabled=? WHERE id='-10042:4'", (enabled,))
                self.assertEqual(self.store.accept(update(number, command)),
                                 'unknown_command' if enabled else 'disabled')
                reply = self.store.db.execute('SELECT text FROM outbox WHERE reply_to=?', (number,)).fetchone()[0]
                self.assertEqual(reply, 'Unknown command. Send /help.')
        self.assertEqual(self.store.messages_pending(), [])
        self.assertEqual(self.store.tasks_list(), [])

    def test_ping_still_answers_without_an_agent_in_enabled_and_disabled_topics(self):
        for number, enabled in enumerate((True, False), 1):
            with self.subTest(enabled=enabled), self.store.db:
                self.store.db.execute("UPDATE topics SET enabled=? WHERE id='-10042:4'", (enabled,))
                self.assertEqual(self.store.accept(update(number, '/ping@torii_test_bot')), 'ping')
                reply = self.store.db.execute('SELECT text FROM outbox WHERE reply_to=?', (number,)).fetchone()[0]
                self.assertEqual(reply, 'Bridge received your message. No agent was started.')
        self.assertEqual(self.store.messages_pending(), [])

    def test_hidden_setup_and_start_in_control_topic_show_setup_status(self):
        from coordinator.setup_flow import setup_status
        with self.store.db:
            self.store.put('control_topic', '-10042:4')
        with patch('coordinator.setup_flow.setup_status', wraps=setup_status) as status:
            for number, command in enumerate(('/setup', '/start'), 1):
                with self.subTest(command=command):
                    status.reset_mock()
                    self.assertEqual(self.store.accept(update(number, command)), 'control')
                    status.assert_called_once_with(self.store, force=True, in_place=False)
                    self.assertIsNotNone(self.store.get('setup_card'))
                    self.assertEqual(self.store.get('control_ui:-10042:4')['where'], 'group_setup')
        self.assertEqual(self.store.messages_pending(), [])

    def test_goal_bypasses_onboarding_unknown_command(self):
        with self.store.db:
            self.store.db.execute("UPDATE topics SET enabled=0 WHERE id='-10042:4'")
        self.assertEqual(self.store.accept(update(1, '/goal finish the work')), 'queued')
        self.assertEqual(self.store.messages_pending()[0]['text'], '/goal finish the work')

    def test_accept_keeps_image_metadata_with_owner_message(self):
        incoming = update(1, '', caption='Use this image', photo=[
            {'file_id': 'small', 'width': 10, 'height': 10},
            {'file_id': 'large', 'width': 100, 'height': 100}])
        self.assertEqual(self.store.accept(incoming), 'queued')
        row = self.store.messages_pending()[0]
        self.assertEqual(row['text'], 'Use this image')
        self.assertEqual(json.loads(row['images'])[0]['file_id'], 'large')

    def test_accept_converts_text_and_caption_entities(self):
        self.assertEqual(self.store.accept(update(1, '😀bold', entities=[
            {'type': 'bold', 'offset': 2, 'length': 4}])), 'queued')
        self.assertEqual(self.store.accept(update(2, '', caption='Use image', caption_entities=[
            {'type': 'italic', 'offset': 4, 'length': 5}], photo=[
            {'file_id': 'image', 'width': 10, 'height': 10}])), 'queued')
        self.assertEqual([row['text'] for row in self.store.messages_pending()],
                         ['😀**bold**', 'Use *image*'])

    def test_worker_completion_is_atomic_and_deduplicated(self):
        task = self.store.task_create('-10042:4', 'Task', worktree=str(self.root))
        with self.store.db:
            worker = self.store.db.execute('''INSERT INTO workers
                (task,topic,provider,prompt,created,updated)
                VALUES (?,?,?,?,?,?)''',
                (task['id'], '-10042:4', 'claude', 'work', 1, 1)).lastrowid
        first = self.store.worker_complete(worker, {'success': True, 'goal_status': {'status': 'met'}})
        duplicate = self.store.worker_complete(worker, {'success': False})
        self.assertEqual(first, duplicate)
        self.assertEqual(json.loads(first['text'])['goal_status']['status'], 'met')
        self.assertEqual(self.store.db.execute('SELECT status FROM workers WHERE id=?', (worker,)).fetchone()[0], 'done')

    def envelope(self, name, state):
        with self.store.db:
            return self.store.db.execute('''INSERT INTO envelopes(name,reason,consumer,topic,state,created,expires,updated)
                VALUES (?,'r','c','-10042:4',?,1,601,1)''', (name, state)).lastrowid

    def test_fresh_store_has_envelope_tables_and_no_audit_table(self):
        names = {row[0] for row in self.store.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertTrue({'envelopes', 'envelope_events', 'envelope_effects'}.issubset(names))
        self.assertNotIn('audit', names)
        self.assertIn('secrets', {row['name'] for row in self.store.db.execute('PRAGMA table_info(tasks)')})

    def test_one_live_envelope_and_one_filled_envelope_per_name(self):
        self.envelope('GITHUB_TOKEN', 'open')
        with self.assertRaises(sqlite3.IntegrityError):
            self.envelope('GITHUB_TOKEN', 'armed')
        self.envelope('GITHUB_TOKEN', 'filled')
        with self.assertRaises(sqlite3.IntegrityError):
            self.envelope('GITHUB_TOKEN', 'filled')
        self.envelope('GITHUB_TOKEN', 'superseded')
        self.envelope('OTHER_TOKEN', 'open')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM envelopes').fetchone()[0], 4)

    def test_envelope_event_leaves_commit_to_the_caller(self):
        envelope = self.envelope('GITHUB_TOKEN', 'open')
        self.store.envelope_event(envelope, 'GITHUB_TOKEN', 'ask', source='test')
        self.store.db.rollback()
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM envelope_events').fetchone()[0], 0)
        with self.store.db:
            self.store.envelope_event(envelope, 'GITHUB_TOKEN', 'reject', reason='too_long')
        row = self.store.db.execute('SELECT envelope,name,event,reason FROM envelope_events').fetchone()
        self.assertEqual(tuple(row), (envelope, 'GITHUB_TOKEN', 'reject', 'too_long'))


class ReplyBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store(Path(self.temp.name))
        self.addCleanup(self.store.close)
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test','',1)")

    def queued(self, text, image=None):
        self.store.db.execute('DELETE FROM outbox')
        self.store.enqueue_report('1:2', text, image=image)
        return [row['text'] for row in self.store.db.execute('SELECT text FROM outbox ORDER BY id')]

    def filler(self, count):
        words = 'review merged checks passed on the mini and the owner can open it now'.split()
        return ' '.join(words[index % len(words)] for index in range(count))

    def test_caption_keeps_the_paragraph_before_a_link_near_its_limit(self):
        first = self.filler(200)[:900].rstrip() + '.'
        link = '[the review notes](https://github.com/example/project/pull/395)'
        head = first + '\n\n' + self.filler(100)[:107].rstrip() + ' See ' + link + ' for details. '
        text = head + self.filler(400)[:2040 - len(telegram_text(head))].rstrip()
        parts = self.queued(text, image='/tmp/image.png')
        self.assertEqual(parts[0], first)
        self.assertEqual(len(parts), 2)
        self.assertEqual(telegram_text(parts[1]), telegram_text(text[len(first):].strip()))

    def test_review_caption_and_message_link_examples_keep_words_and_links(self):
        first = self.filler(110)[:600].rstrip() + '.'
        url = 'https://github.com/example/project/pull/395'
        link = 'See [the review notes](' + url + ') for details.'
        for image, befores, totals in (('/tmp/image.png', range(380, 425), (2040, 2044, 3068, 3070)),
                                        (None, range(3420, 3520, 2), (8150, 8180, 8190, 8192, 9000))):
            broken, midword = [], []
            for before in befores:
                for total in totals:
                    head = first + '\n\n' + self.filler(700)[:before].rstrip() + ' ' + link + ' '
                    text = head + self.filler(2000)[:total - len(telegram_text(head))].rstrip()
                    parts = self.queued(text, image=image)
                    html = [ElementTree.fromstring('<text>' + markdown_to_html(part) + '</text>')
                            for part in parts]
                    links = [(node.attrib['href'], ''.join(node.itertext()))
                             for root in html for node in root.iter('a')]
                    if links != [(url, 'the review notes')]:
                        broken.append((before, total))
                    normalize = lambda value: re.sub(r'\s+', ' ', value).strip()
                    if normalize(' '.join(parts)) != normalize(text):
                        midword.append((before, total))
                    self.assertTrue(all(len(''.join(root.itertext())) <=
                                        (1024 if image and index == 0 else 4096)
                                        for index, root in enumerate(html)))
            with self.subTest(image=image):
                self.assertEqual((len(broken), len(midword)), (0, 0), (broken[:3], midword[:3]))

    def test_inline_atoms_and_words_move_whole_to_the_next_message(self):
        atoms = ['[the review notes](https://example.test/' + 'a' * 100 + ')',
                 '`inline code with spaces`', r'\\[the review notes](https://example.test/' + 'a' * 100 + ')',
                 r'\\`inline code with spaces`', 'https://example.test/' + 'a' * 100, 'longword' * 20]
        for atom in atoms:
            with self.subTest(atom=atom[:30]):
                text = 'x' * 4080 + ' ' + atom + ' ' + 'tail ' * 850
                parts = self.queued(text)
                self.assertEqual(parts[0], 'x' * 4080)
                self.assertTrue(parts[1].startswith(atom + ' '))
                self.assertEqual(' '.join(parts).split(), text.split())

    def test_paragraph_is_preferred_to_a_later_line_break(self):
        first = 'a ' * 1500
        text = first.rstrip() + '\n\n' + 'b ' * 250 + '\n' + 'c ' * 1000
        parts = self.queued(text)
        self.assertEqual(parts[0], first.rstrip())
        self.assertEqual(len(parts), 2)

    def test_large_formatted_reply_keeps_its_content_and_limits(self):
        lines = []
        size = 0
        while size < 200000:
            number = len(lines)
            line = '- **Job %d**: café 日本語 😀 review merged checks passed on the mini [link](https://e.test/%d) `code`' % (number, number)
            lines.append(line)
            size += len(line) + 1
        text = '\n'.join(lines)
        started = time.perf_counter()
        parts = self.queued(text)
        elapsed = time.perf_counter() - started
        print('SPLITTER_BENCHMARK ' + json.dumps({'characters': len(text),
              'sha256': hashlib.sha256(text.encode()).hexdigest(), 'chunks': len(parts),
              'seconds': elapsed}), flush=True)
        self.assertEqual(re.sub(r'\s+', '', ''.join(parts)), re.sub(r'\s+', '', text))
        self.assertTrue(all(0 < len(telegram_text(part)) <= 4096 for part in parts))


class SplitMessageTests(unittest.TestCase):
    def test_long_help_reply_has_no_isolated_job_line(self):
        title = ('P1-E frontend lane: canvas ids (boxId, groupId, groupBoxId, '
                 'outputBoxId, slotId), café 日本語 😀')
        lines = ['- Job %d: %s' % (number, title) for number in range(406, 438)]
        lines[0] += ' & checks'
        isolated = '- Job 438: P1-E widgetId lane: widgets/* (excluding ImageWidget)'
        first = '**Open jobs**\n' + '\n'.join(lines + [isolated])
        lines[0] += '.' * (4090 - len(first))
        text = '**Open jobs**\n' + '\n'.join(lines + [isolated] + [
            '- Job %d: %s' % (number, title) for number in range(439, 457)])
        with tempfile.TemporaryDirectory() as temp:
            store = Store(Path(temp))
            self.addCleanup(store.close)
            with store.db, patch('coordinator.controls._HELP', text):
                store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test','',1)")
                self.assertTrue(handle_control(store, '1:2', 42, '/help'))
            rows = store.db.execute('SELECT text,reply_to FROM outbox ORDER BY id').fetchall()
            parts = [telegram_text(row['text']) for row in rows]
            lengths = [len(part) for part in parts]
            self.assertEqual(len(parts), 2, lengths)
            self.assertTrue(all(0 < length <= 4096 for length in lengths), lengths)
            self.assertTrue(all(len(part.splitlines()) > 1 for part in parts))
            self.assertEqual('\n'.join(parts), telegram_text(text))
            self.assertEqual({row['reply_to'] for row in rows}, {42})

    def test_rendered_limit_counts_characters_after_entities_not_utf16_units(self):
        cases = [('&' * 4096, [4096]), ('😀' * 4096, [4096]),
                 ('😀' * 5000, [4096, 904]),
                 ('**' + 'x' * 4096 + '**', [4096]),
                 ('[details](https://example.test/' + 'x' * 5000 + ')', [7])]
        for text, expected in cases:
            with self.subTest(text=text[:20]), tempfile.TemporaryDirectory() as temp:
                store = Store(Path(temp))
                try:
                    store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test','',1)")
                    store.enqueue_report('1:2', text)
                    parts = [telegram_text(row['text']) for row in store.db.execute(
                        'SELECT text FROM outbox ORDER BY id')]
                    self.assertEqual([len(part) for part in parts], expected)
                    self.assertEqual(''.join(parts), telegram_text(text))
                    if text == '😀' * 4096:
                        self.assertEqual(len(parts[0].encode('utf-16-le')) // 2, 8192)
                finally:
                    store.close()

    def test_short_heading_stays_whole_before_an_oversized_word(self):
        with tempfile.TemporaryDirectory() as temp:
            store = Store(Path(temp))
            try:
                store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test','',1)")
                store.enqueue_report('1:2', 'Open jobs\n' + 'x' * 5000)
                parts = [row['text'] for row in store.db.execute('SELECT text FROM outbox ORDER BY id')]
                self.assertEqual(parts, ['Open jobs', 'x' * 4096, 'x' * 904])
            finally:
                store.close()

    def test_caption_uses_its_capacity_before_continuing_in_full_messages(self):
        for text, expected in (('😀' * 6000, [1024, 4096, 880]),
                               ('&' * 5000, [1024, 3976])):
            with self.subTest(text=text[:20]), tempfile.TemporaryDirectory() as temp:
                store = Store(Path(temp))
                try:
                    store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test','',1)")
                    store.enqueue_report('1:2', text, image='/tmp/image.png')
                    rows = store.db.execute('SELECT text,image FROM outbox ORDER BY id').fetchall()
                    parts = [telegram_text(row['text']) for row in rows]
                    self.assertEqual([len(part) for part in parts], expected)
                    self.assertEqual(''.join(parts), text)
                    self.assertEqual([row['image'] for row in rows], ['/tmp/image.png'] + [None] * (len(rows) - 1))
                finally:
                    store.close()

    def test_line_boundaries_prevent_words_from_being_cut(self):
        text = '\n'.join(['x' * 2500] * 3)
        with tempfile.TemporaryDirectory() as temp:
            store = Store(Path(temp))
            try:
                store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test','',1)")
                store.enqueue_report('1:2', text)
                parts = [row['text'] for row in store.db.execute('SELECT text FROM outbox ORDER BY id')]
                self.assertEqual(parts, ['x' * 2500] * 3)
                self.assertEqual('\n'.join(parts), text)
            finally:
                store.close()

    def test_no_word_is_cut(self):
        words = ['word%d' % i for i in range(2000)]
        parts = _split_rendered(' '.join(words), 4096)
        self.assertTrue(all(len(part) <= 4096 for part in parts))
        self.assertEqual(' '.join(parts).split(), words)

    def test_fence_is_reopened_in_next_part(self):
        text = 'a' * 3000 + '\n```python\n' + 'x ' * 1000 + '\n```'
        parts = _split_rendered(text, 4096)
        self.assertEqual(telegram_text(parts[0]).strip(), 'a' * 3000)
        self.assertTrue(parts[1].startswith('```python\n'))

    def test_rendered_parts_fit_and_outbox_keeps_markdown(self):
        with tempfile.TemporaryDirectory() as temp:
            store = Store(Path(temp))
            try:
                with store.db:
                    store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test','',1)")
                    store.enqueue_report('1:2', '&' * 3000)
                rows = store.db.execute('SELECT text FROM outbox ORDER BY id').fetchall()
                self.assertEqual(''.join(row['text'] for row in rows), '&' * 3000)
                self.assertTrue(all(len(telegram_text(row['text'])) <= 4096 for row in rows))
            finally:
                store.close()

    def test_split_rendered_parts_have_balanced_tags(self):
        with tempfile.TemporaryDirectory() as temp:
            store = Store(Path(temp))
            try:
                with store.db:
                    store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test','',1)")
                    store.enqueue_report('1:2', ('**bold & <x>**\n' * 500).strip())
                rows = store.db.execute('SELECT text FROM outbox ORDER BY id').fetchall()
                self.assertGreater(len(rows), 1)
                for row in rows:
                    rendered = markdown_to_html(row['text'])
                    self.assertLessEqual(len(telegram_text(row['text'])), 4096)
                    self.assertEqual(rendered.count('<b>'), rendered.count('</b>'))
                    self.assertNotIn('<x>', rendered)
            finally:
                store.close()

    def test_oversized_fenced_code_stays_code_in_every_part(self):
        with tempfile.TemporaryDirectory() as temp:
            store = Store(Path(temp))
            try:
                with store.db:
                    store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test','',1)")
                    store.enqueue_report('1:2', '```python\n' + '**code** & <x>\n' * 350 + '```')
                rows = store.db.execute('SELECT text FROM outbox ORDER BY id').fetchall()
                self.assertGreater(len(rows), 1)
                for row in rows:
                    rendered = markdown_to_html(row['text'])
                    self.assertLessEqual(len(telegram_text(row['text'])), 4096)
                    self.assertIn('<pre><code class="language-python">', rendered)
                    self.assertNotIn('<b>', rendered)
                    self.assertEqual(rendered.count('<pre>'), rendered.count('</pre>'))
            finally:
                store.close()

    def test_image_caption_fits_rendered_limit(self):
        with tempfile.TemporaryDirectory() as temp:
            store = Store(Path(temp))
            try:
                with store.db:
                    store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test','',1)")
                    store.enqueue_report('1:2', '&' * 300, image='/tmp/image.png')
                rows = store.db.execute('SELECT text,image FROM outbox ORDER BY id').fetchall()
                self.assertEqual(''.join(row['text'] for row in rows), '&' * 300)
                self.assertEqual(rows[0]['image'], '/tmp/image.png')
                self.assertLessEqual(len(telegram_text(rows[0]['text'])), 1024)
            finally:
                store.close()
