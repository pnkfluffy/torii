import asyncio
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from coordinator import control_api, mcp, topics
from coordinator.store import Store


class JobIterationTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.store = Store(self.root / 'state')
        self.addCleanup(lambda: self.store.close())
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('1:2',1,2,'Rounds',?)",
                                  (str(self.root),))
        self.origin = self.store.message_save('1:2', 'owner', 'Build the job', telegram_message=405)

    def call(self, op, **params):
        result = control_api.call(self.store, op, params, topic='1:2')
        self.assertTrue(result.ok, result.text)
        return result

    def job(self):
        task = self.store.task_create('1:2', 'Build', worktree=str(self.root), origin=self.origin['id'])
        with self.store.db:
            self.store.db.execute('UPDATE tasks SET number=405 WHERE id=?', (task['id'],))
        return self.store.task_get(task['id'])

    def round_one(self, task):
        with self.store.db:
            self.store.db.execute("UPDATE task_iterations SET closed=200,outcome='done' WHERE task=? AND k=0",
                                  (task['id'],))
            self.store.db.execute("""INSERT INTO task_iterations(task,k,opened,opened_by,origin,reason)
                VALUES (?,1,300,'owner',?,'Follow-up')""", (task['id'], self.origin['id']))
            self.store.db.execute('UPDATE tasks SET iteration=1 WHERE id=?', (task['id'],))

    def spawn(self, task, prompt):
        return self.call('workers.spawn', task=task['id'], provider='codex', prompt=prompt).data['worker']

    def test_new_job_records_round_zero_and_optional_metadata(self):
        with patch('coordinator.store.time.time', return_value=100):
            task = self.job()
        self.assertEqual((task['iteration'], task['finish'], task['parent'], task['copy'], task['copy_reason']),
                         (0, None, None, 'present', None))
        rounds = [dict(row) for row in self.store.db.execute('SELECT * FROM task_iterations WHERE task=?',
                                                           (task['id'],))]
        self.assertEqual([(row['k'], row['opened'], row['closed'], row['outcome'], row['reason'],
                           row['origin'], row['opened_by']) for row in rounds],
                         [(0, 100, None, None, None, self.origin['id'], 'create')])
        with self.assertRaisesRegex(RuntimeError, 'rollback'):
            with self.store.transaction():
                self.store.task_create('1:2', 'Rolled back')
                raise RuntimeError('rollback')
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM task_iterations').fetchone()[0], 1)

    def test_finish_and_parent_are_recorded_without_done_proof(self):
        parent = self.job()
        for finish in ('merged', 'live', 'answered', 'delivered'):
            with self.subTest(finish=finish):
                task = self.call('tasks.create', title='Child', message=self.origin['id'],
                                 finish=finish, parent=parent['id']).data
                self.assertEqual((task['finish'], task['parent']), (finish, parent['id']))
                closed = self.call('tasks.update', task=task['id'], status='done')
                self.assertEqual((closed.data['status'], closed.data['iteration']), ('done', 0))
        for finish in ('code', 'answer', 'deliver', 'unknown'):
            result = control_api.call(self.store, 'tasks.create', {'title': 'Invalid', 'topic': '1:2',
                                      'finish': finish})
            self.assertFalse(result.ok)
        with self.assertRaises(sqlite3.IntegrityError):
            with self.store.db:
                self.store.db.execute("UPDATE tasks SET finish='unknown' WHERE id=?", (parent['id'],))
        result = control_api.call(self.store, 'tasks.create', {'title': 'Missing parent', 'topic': '1:2',
                                  'parent': 999999})
        self.assertFalse(result.ok)
        self.assertEqual(len(self.store.tasks_list()), 5)

    def test_parent_description_uses_global_job_id(self):
        description = control_api.BY_ID['tasks.create'].description
        self.assertIn("parent takes the job's global id", description)
        self.assertIn('not its per-chat number', description)

    def test_legacy_status_updates_record_the_same_round_without_new_transitions(self):
        task = self.job()
        for status in ('done', 'open', 'dropped'):
            with self.subTest(status=status):
                with patch('coordinator.store.time.time', return_value=200):
                    self.call('tasks.update', task=task['id'], status=status)
                row = self.store.db.execute('SELECT k,closed,outcome FROM task_iterations WHERE task=?',
                                             (task['id'],)).fetchone()
                self.assertEqual(tuple(row), (0, None, None) if status == 'open' else (0, 200, status))
        for status in ('waiting', 'interrupted'):
            self.assertFalse(control_api.call(self.store, 'tasks.update', {'task': task['id'], 'status': status}).ok)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM task_iterations').fetchone()[0], 1)

    def test_spawn_and_stamps_keep_the_round_they_were_created_in(self):
        task = self.job()
        first = self.spawn(task, 'First round')
        self.call('tasks.update', task=task['id'], stage='ready')
        self.round_one(task)
        second = self.spawn(task, 'Next round')
        self.call('tasks.update', task=task['id'], stage='ci')
        self.assertEqual([tuple(row) for row in self.store.db.execute('SELECT id,iteration FROM workers ORDER BY id')],
                         [(first, 0), (second, 1)])
        self.assertEqual([tuple(row) for row in self.store.db.execute('SELECT stage,iteration FROM task_stamps ORDER BY id')],
                         [('ready', 0), ('ci', 1)])
        self.store.close()
        self.store = Store(self.root / 'state')
        self.assertEqual([row[0] for row in self.store.db.execute('SELECT iteration FROM workers ORDER BY id')], [0, 1])
        self.assertEqual(self.store.db.execute('PRAGMA foreign_key_check').fetchall(), [])

    def test_task_texts_and_json_use_the_current_round(self):
        task = self.job()
        self.assertEqual(self.call('tasks.get', task=task['id']).text.splitlines()[0], 'Job 405: Build')
        self.assertEqual(self.call('tasks.list').text, '1:2 job 405: Build (open)')
        self.round_one(task)
        fetched = self.call('tasks.get', task=task['id'])
        listed = self.call('tasks.list')
        self.assertEqual(fetched.text.splitlines()[0], 'Job 405.1: Build')
        self.assertEqual(listed.text, '1:2 job 405.1: Build (open)')
        self.assertEqual(self.call('tasks.update', task=task['id'], note='Keep working').text, 'Job 405.1 updated.')
        with self.store.db:
            self.store.db.execute('UPDATE tasks SET updated=0 WHERE id=?', (task['id'],))
        self.assertIn('job 405.1: Build', self.call('tasks.stale', hours=1).text)
        for rows in (fetched.data['iterations'], listed.data[0]['iterations']):
            self.assertEqual([(row['k'], row['closed'], row['outcome']) for row in rows],
                             [(0, 200, 'done'), (1, None, None)])
            self.assertEqual(rows[1]['opened'], 300)
        self.assertIn('405.1', '\n'.join(fetched.text.splitlines()[1:]))

    def test_worker_texts_use_each_workers_round_and_the_job_number(self):
        task = self.job()
        first = self.spawn(task, 'First round')
        self.round_one(task)
        second = self.spawn(task, 'Next round')
        self.assertEqual(self.call('workers.get', worker=first).text,
                         'Worker %d: queued, job %d, effort medium' % (first, task['id']))
        self.assertEqual(self.call('workers.get', worker=second).text,
                         'Worker %d: queued, job %d (405.1), effort medium' % (second, task['id']))
        listed = self.call('workers.list', task=task['id'])
        self.assertEqual(listed.text.splitlines(), [
            'Worker %d: queued, job %d (405.1), effort medium' % (second, task['id']),
            'Worker %d: queued, job %d, effort medium' % (first, task['id'])])
        self.assertEqual([(row['id'], row['iteration']) for row in listed.data['workers']], [(second, 1), (first, 0)])
        for worker, label in ((first, str(task['id'])), (second, '%d (405.1)' % task['id'])):
            with self.subTest(worker=worker):
                completed = subprocess.run(
                    [sys.executable, '-m', 'coordinator', '--state-dir', str(self.store.directory),
                     'ctl', 'call', 'workers.get', 'worker=%d' % worker],
                    capture_output=True, text=True, timeout=120)
                self.assertEqual(completed.returncode, 0, completed.stderr)
                self.assertEqual(json.loads(completed.stdout)['text'],
                                 'Worker %d: queued, job %s, effort medium' % (worker, label))

    def test_label_collision_copied_global_id_closes_only_the_intended_job(self):
        def tool(name, **arguments):
            result = mcp.dispatch(self.store, {'method': 'tools/call',
                                  'params': {'name': name, 'arguments': arguments}})
            self.assertFalse(result.get('isError', False), result)
            return result['content'][0]['text']

        with self.store.transaction():
            for thread in (1, 2, 3):
                self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd) VALUES (?,-100,?,?,?)',
                                      ('-100:%d' % thread, thread, 'Collision', str(self.root)))
            self.store.put('group', -100)
            for index in range(1, 1401):
                topic = '-100:2' if index % 2 else '-100:3'
                origin = self.store.message_save(topic, 'owner', 'Other job', telegram_message=index)
                other = self.store.task_create(topic, 'Other job', origin=origin['id'], worktree=str(self.root))
                if index != 409:
                    self.store.db.execute("UPDATE tasks SET status='done' WHERE id=?", (other['id'],))
            for index in range(1, 410):
                origin = self.store.message_save('-100:1', 'owner', 'Torii job', telegram_message=5000 + index)
                intended = self.store.task_create('-100:1', 'Torii job', origin=origin['id'], worktree=str(self.root))
        self.assertEqual((intended['id'], intended['number']), (1809, 409))
        other = self.store.task_get(409)
        self.assertEqual((other['topic'], other['number'], other['status']), ('-100:2', 205, 'open'))
        intended_worker = json.loads(tool('workers_spawn', task=1809, provider='codex', prompt='Build it')
                                     .splitlines()[-1])['worker']
        other_worker = json.loads(tool('workers_spawn', task=409, provider='codex', prompt='Other work')
                                 .splitlines()[-1])['worker']
        listed = tool('workers_list', task=1809)
        fetched = tool('workers_get', worker=intended_worker)
        self.assertEqual(json.loads(listed.splitlines()[-1])['workers'][0]['task'], 1809)
        self.assertEqual(listed.splitlines()[0], fetched.splitlines()[0])
        copied = int(listed.splitlines()[0].split(', job ')[1].split(',')[0])
        selected = json.loads(tool('tasks_get', task=copied).splitlines()[-1])
        tool('tasks_update', task=copied, status='done', note='Merged PR #12; verified')
        self.assertEqual(self.store.task_get(1809)['status'], 'done')
        self.assertEqual(selected['id'], 1809)
        self.assertEqual(self.store.task_get(409)['status'], 'open')
        self.assertIsNone(self.store.task_get(409)['notes'])
        statuses = dict(self.store.db.execute('SELECT id,status FROM workers'))
        self.assertEqual((statuses[intended_worker], statuses[other_worker]), ('interrupted', 'queued'))
        reactions = dict(self.store.db.execute('SELECT id,reaction_desired FROM messages'))
        self.assertEqual((reactions[intended['origin']], reactions[other['origin']]), ('👌', '👀'))

    def test_chat_close_round_closes_open_rounds_with_dropped_and_chat_closed(self):
        first = self.job()
        second = self.store.task_create('1:2', 'Next round')
        self.round_one(second)
        closed = self.store.task_create('1:2', 'Already done')
        self.store.task_update(closed['id'], status='done')
        prior = self.store.task_iterations(closed['id'])
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('1:3',1,3,'Other',?)",
                                  (str(self.root),))
        other = self.store.task_create('1:3', 'Other chat')
        request = self.store.service_request('topic.close', {'topic': '1:2'})
        row = dict(self.store.db.execute('SELECT * FROM service_requests WHERE id=?', (request,)).fetchone())
        service = SimpleNamespace(store=self.store)
        with patch('coordinator.topics.time.time', return_value=400):
            asyncio.run(topics._close(service, row, {'topic': '1:2'}, {'phase': 'stopped'}))
        for task, k in ((first, 0), (second, 1)):
            with self.subTest(task=task['id']):
                self.assertEqual(self.store.task_get(task['id'])['status'], 'dropped')
                current = self.store.task_iterations(task['id'])[-1]
                self.assertEqual((current['k'], current['closed'], current['outcome'], current['reason']),
                                 (k, 400, 'dropped', 'chat closed'))
        history = self.store.task_iterations(second['id'])[0]
        self.assertEqual((history['closed'], history['outcome']), (200, 'done'))
        self.assertEqual(self.store.task_iterations(closed['id']), prior)
        self.assertEqual(self.store.task_get(other['id'])['status'], 'open')
        self.assertIsNone(self.store.task_iterations(other['id'])[0]['closed'])

    def test_mcp_reads_expose_round_history(self):
        task = self.job()
        self.round_one(task)
        for name, params in (('tasks_get', {'task': task['id']}), ('tasks_list', {'topic': '1:2'})):
            with self.subTest(tool=name):
                result = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {'name': name, 'arguments': params}})
                self.assertFalse(result.get('isError', False), result)
                self.assertIn('405.1', result['content'][0]['text'])
                data = json.loads(result['content'][0]['text'].splitlines()[-1])
                row = data[0] if name == 'tasks_list' else data
                self.assertEqual([item['k'] for item in row['iterations']], [0, 1])
