import ast
from contextlib import closing
import json
from pathlib import Path
import re
import sqlite3
import tempfile
import unittest

from coordinator import control_api, store as store_module
from coordinator.store import Store


def tables(db):
    return [row[0] for row in db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]


def counts(db):
    return {name: db.execute('SELECT COUNT(*) FROM "' + name + '"').fetchone()[0]
            for name in tables(db)}


def rows(db):
    return {name: [dict(row) for row in db.execute('SELECT * FROM "' + name + '" ORDER BY rowid')]
            for name in tables(db)}


def schema(db):
    return {
        'normalized_sql': sorted(re.sub(r'"([A-Za-z_][A-Za-z_0-9]*)"', r'\1', ' '.join(row[0].split()))
                                 for row in db.execute('SELECT sql FROM sqlite_master WHERE sql IS NOT NULL')),
        'columns': {name: [tuple(row) for row in db.execute('PRAGMA table_info("' + name + '")')]
                    for name in tables(db)},
        'indexes': [tuple(row) for row in db.execute(
            "SELECT name,tbl_name,sql FROM sqlite_master WHERE type='index' ORDER BY name")],
    }


def message_kinds():
    node = ast.parse(Path(store_module.__file__).read_text())
    for item in ast.walk(node):
        if isinstance(item, ast.Compare) and isinstance(item.left, ast.Name) and item.left.id == 'kind':
            if any(isinstance(operator, ast.NotIn) for operator in item.ops):
                value = ast.literal_eval(item.comparators[0])
                if 'owner' in value and 'worker_result' in value:
                    return set(value)
    raise AssertionError('No authoritative message kind validation found')


class StoreUpgradeTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        fixture = Path(__file__).parent / 'fixtures/store-checkpoint.sql'
        with closing(sqlite3.connect(self.root / 'state.sqlite')) as db:
            db.executescript(fixture.read_text())
            db.row_factory = sqlite3.Row
            self.original_rows = rows(db)
        self.store = Store(self.root)
        self.addCleanup(lambda: self.store.close())

    def test_job_iterations_preserve_checkpoint_identity_and_round_zero(self):
        rounds = [dict(row) for row in self.store.db.execute('SELECT * FROM task_iterations ORDER BY task,k')]
        self.assertEqual(len(rounds), len(self.original_rows['tasks']))
        for old, iteration in zip(self.original_rows['tasks'], rounds):
            with self.subTest(task=old['id']):
                task = self.store.task_get(old['id'])
                self.assertEqual((task['id'], task['number'], task['worktree'], task['pr']),
                                 (old['id'], old['number'], old['worktree'], old.get('pr')))
                self.assertEqual((task['iteration'], task['finish'], task['parent']), (0, None, None))
                self.assertEqual((iteration['task'], iteration['k'], iteration['opened'], iteration['origin'],
                                  iteration['opened_by']), (old['id'], 0, old['created'], old['origin'], 'migration'))
                for table in ('workers', 'task_stamps'):
                    self.assertEqual(self.store.db.execute(
                        'SELECT COUNT(*) FROM ' + table + ' WHERE task=? AND iteration!=0', (old['id'],)).fetchone()[0], 0)
        self.assertEqual(self.store.db.execute('PRAGMA foreign_key_check').fetchall(), [])
        self.store.close()
        self.store = Store(self.root)
        self.assertEqual([dict(row) for row in self.store.db.execute('SELECT * FROM task_iterations ORDER BY task,k')], rounds)

    def test_job_iterations_migrate_closed_jobs_and_keep_indexes_and_branches(self):
        directory = self.root / 'closed-checkpoint'
        directory.mkdir()
        fixture = Path(__file__).parent / 'fixtures/store-checkpoint.sql'
        with closing(sqlite3.connect(directory / 'state.sqlite')) as db:
            db.executescript(fixture.read_text())
            db.execute('ALTER TABLE tasks ADD COLUMN pr TEXT')
            db.execute("UPDATE tasks SET status='done',updated=200,pr='https://example.invalid/pull/1' WHERE id=1")
            db.execute("UPDATE tasks SET status='dropped',updated=300 WHERE id=2")
            db.execute('CREATE INDEX tasks_saved_worktree ON tasks(worktree)')
            db.execute('''CREATE TABLE task_stamps (
                id INTEGER PRIMARY KEY, task INTEGER NOT NULL REFERENCES tasks(id),
                stage TEXT NOT NULL CHECK(stage IN ('ready','reviewed','ci','merged','live')),
                created REAL NOT NULL)''')
            db.execute("INSERT INTO task_stamps(id,task,stage,created) VALUES (41,1,'ready',100)")
            db.execute("UPDATE workers SET workspace=? WHERE task=1",
                       (json.dumps({'branch': 'torii/task-1', 'cwd': '/tmp/torii-checkpoint/path-02'}),))
            db.commit()
        upgraded = Store(directory)
        self.addCleanup(upgraded.close)
        actual = [tuple(row) for row in upgraded.db.execute(
            'SELECT task,k,closed,outcome FROM task_iterations WHERE task IN (1,2) ORDER BY task')]
        self.assertEqual(actual, [(1, 0, 200, 'done'), (2, 0, 300, 'dropped')])
        self.assertEqual(upgraded.task_get(1)['pr'], 'https://example.invalid/pull/1')
        worker = upgraded.db.execute('SELECT workspace,iteration FROM workers WHERE task=1').fetchone()
        self.assertEqual((json.loads(worker['workspace'])['branch'], worker['iteration']), ('torii/task-1', 0))
        self.assertEqual(tuple(upgraded.db.execute('SELECT id,task,stage,created,iteration FROM task_stamps').fetchone()),
                         (41, 1, 'ready', 100, 0))
        self.assertEqual(upgraded.db.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='tasks_saved_worktree'").fetchone()[0],
                         'tasks_saved_worktree')
        self.assertEqual(upgraded.db.execute('PRAGMA foreign_key_check').fetchall(), [])

    def test_checkpoint_schema_matches_fresh_store(self):
        fresh = Store(self.root / 'fresh')
        self.addCleanup(fresh.close)
        upgraded_schema, fresh_schema = schema(self.store.db), schema(fresh.db)
        for part in ('normalized_sql', 'columns', 'indexes'):
            with self.subTest(part=part):
                self.assertEqual(upgraded_schema[part], fresh_schema[part])

    def test_checkpoint_upgrade_passes_integrity_and_foreign_key_checks(self):
        self.assertEqual([tuple(row) for row in self.store.db.execute('PRAGMA integrity_check')], [('ok',)])
        self.assertEqual(self.store.db.execute('PRAGMA foreign_key_check').fetchall(), [])

    def test_checkpoint_upgrade_preserves_all_rows(self):
        actual_counts = counts(self.store.db)
        expected_rows = dict(self.original_rows)
        expected_counts = {table: len(original) for table, original in expected_rows.items()}
        expected_counts.update({name: 0 for name in actual_counts if name not in expected_counts})
        final_workers = {worker['id'] for worker in self.original_rows['workers']
                         if worker['status'] == 'needs_input'}
        expected_counts['worker_temp_state'] = len(final_workers)
        expected_counts['task_iterations'] = len(self.original_rows['tasks'])
        self.assertEqual(actual_counts, expected_counts)
        queued = self.store.db.execute(
            'SELECT worker,finished,pending,path FROM worker_temp_state').fetchall()
        self.assertEqual({tuple(row) for row in queued},
                         {(worker, 1, 1, None) for worker in final_workers})
        migrated_rows = rows(self.store.db)
        for table, original in expected_rows.items():
            self.assertEqual(len(migrated_rows[table]), len(original), table)
            for old, new in zip(original, migrated_rows[table]):
                with self.subTest(table=table, row=old.get('id', old.get('key'))):
                    expected = dict(old)
                    if table == 'workers' and old['status'] == 'needs_input':
                        expected.update(status='done', updated=new['updated'])
                    self.assertEqual({key: new[key] for key in old}, expected)

    def test_checkpoint_pairing_and_group_mode_are_preserved(self):
        settings = {row['key']: json.loads(row['value']) for row in self.original_rows['settings']}
        keys = ('owner', 'group', 'group_type', 'mode')
        self.assertEqual({key: self.store.get(key) for key in keys},
                         {key: settings.get(key) for key in keys})
        self.assertEqual(self.store.get('mode'), 'group')
        self.assertIn(self.store.get('group_type'), ('group', 'supergroup'))
        self.assertIsNotNone(self.store.get('owner'))
        for topic in self.store.topics():
            if 'mode' in topic:
                self.assertEqual(topic['mode'], 'group')

    def test_checkpoint_worker_statuses_can_be_listed(self):
        statuses = {row[0] for row in self.store.db.execute('SELECT DISTINCT status FROM workers')}
        self.assertTrue(statuses <= set(control_api.WORKER_STATUSES))
        for status in sorted(statuses):
            with self.subTest(status=status):
                result = control_api.op_workers_list(self.store, control_api.Context(), status=status)
                self.assertTrue(result.ok)
                expected = {row['id'] for row in self.store.db.execute(
                    'SELECT id FROM workers WHERE status=?', (status,))}
                self.assertTrue(expected)
                self.assertEqual({row['id'] for row in result.data['workers']}, expected)

    def test_checkpoint_accepts_every_handled_message_kind(self):
        handled_kinds = message_kinds()
        saved_kinds = {row[0] for row in self.store.db.execute('SELECT DISTINCT kind FROM messages')}
        self.assertTrue(saved_kinds <= handled_kinds)
        topic = next(topic['id'] for topic in self.store.topics() if topic['thread'] == 2)
        for kind in sorted(handled_kinds):
            with self.subTest(kind=kind):
                before = self.store.db.execute('SELECT COUNT(*) FROM messages').fetchone()[0]
                text = 'Upgrade probe ' + kind
                row = self.store.message_save(topic, kind, text)
                self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM messages').fetchone()[0], before + 1)
                self.assertEqual((row['kind'], row['text']), (kind, text))
                saved = self.store.db.execute('SELECT kind,text FROM messages WHERE id=?', (row['id'],)).fetchone()
                self.assertEqual(tuple(saved), (kind, text))

    def test_second_checkpoint_open_changes_neither_schema_nor_rows(self):
        before_schema, before_rows = schema(self.store.db), rows(self.store.db)
        self.store.close()
        self.store = Store(self.root)
        self.assertEqual(schema(self.store.db), before_schema)
        self.assertEqual(rows(self.store.db), before_rows)
