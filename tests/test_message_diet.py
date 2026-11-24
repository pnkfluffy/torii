import asyncio
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from coordinator import control_api, mcp, parent_state
from coordinator.service import Service
from coordinator.store import Store


class MessageDietTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name))
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test','',1)")
            self.store.db.execute("INSERT INTO messages(id,topic,kind,text,created,updated) VALUES (1,'1:2','owner','Make a job',1,1)")
            self.store.put('owner', 7)
            self.store.put('group', 1)
        self.task = self.store.task_create('1:2', 'Example', notes='private notes')

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def call(self, name, arguments):
        reply = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {'name': name, 'arguments': arguments}})
        self.assertFalse(reply['isError'], reply)
        return reply['content'][0]['text']

    def test_write_and_list_results(self):
        update = self.call('tasks_update', {'task': self.task['id'], 'note': 'more notes'})
        data = json.loads(update.split('\n')[-1])
        self.assertEqual(set(data), {'id', 'number', 'worktree'})
        self.assertNotIn('notes', update)
        rows = json.loads(self.call('tasks_list', {'topic': '1:2'}).split('\n')[-1])
        self.assertEqual(set(rows[0]), {'id', 'number', 'title', 'status', 'stage', 'open_worker_count', 'topic'})
        self.assertIn('private notes', self.call('tasks_get', {'task': self.task['id']}))
        closed = self.store.task_create('1:2', 'Closed')
        self.store.task_update(closed['id'], status='done')
        self.assertEqual(len(json.loads(self.call('tasks_list', {'topic': '1:2'}).split('\n')[-1])), 1)
        self.assertEqual(len(json.loads(self.call('tasks_list', {'topic': '1:2', 'status': 'done'}).split('\n')[-1])), 1)

    def test_worker_intake_and_full_result(self):
        with self.store.db:
            self.store.db.execute("INSERT INTO workers(id,task,topic,provider,prompt,status,created,updated) VALUES (1,?,'1:2','codex','brief','running',1,1)", (self.task['id'],))
        result = dict(success=True, text='done', error='error detail', failure_code=None, failure_detail=None, results='/results', session_id='session', rate_limit_info={'used': 2})
        row = self.store.worker_complete(1, result)
        service = Service.__new__(Service)
        service.store = self.store
        rendered = asyncio.run(service._prepare_message(row))
        payload = json.loads(rendered[rendered.index('{'):])
        self.assertEqual(set(payload), {'worker', 'task', 'status', 'success', 'text', 'error', 'failure_code', 'failure_detail', 'results'})
        full = json.loads(self.call('workers_get', {'worker': 1}).split('\n')[-1])
        self.assertEqual(full['result']['rate_limit_info'], {'used': 2})

    def test_restart_notice(self):
        rows = self.store.tasks_list(status='open')
        uncertain = [{'id': 1, 'kind': 'worker_result', 'text': json.dumps({'worker': 2, 'status': 'done', 'text': 'finished', 'cwd': '/secret'})}]
        rendered = parent_state.restarted_prompt(rows, uncertain, self.store)
        self.assertIn('Worker 2: done finished', rendered)
        self.assertNotIn('"shown"', rendered)
        self.assertNotIn('/secret', rendered)

    def test_create_and_send_write_results(self):
        created = self.call('tasks_create', {'topic': '1:2', 'message': 1, 'title': 'Another', 'notes': 'hidden'})
        data = json.loads(created.split('\n')[-1])
        self.assertEqual(set(data), {'id', 'number', 'worktree', 'scratch'})
        self.assertNotIn('hidden', created)
        sent = json.loads(self.call('telegram_send', {'topic': '1:2', 'text': 'test'}).split('\n')[-1])
        self.assertEqual(set(sent), {'outbox'})

    def test_cross_chat_restart_and_limit(self):
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:3',1,3,'other','',1)")
        other = self.store.task_create('1:3', 'Other')
        with self.store.db:
            self.store.db.execute("INSERT INTO workers(id,task,topic,provider,prompt,status,created,updated) VALUES (1,?,'1:3','codex','brief','running',1,1)", (other['id'],))
        rows = self.store.tasks_list(status='open')
        rendered = parent_state.restarted_prompt(rows, [], self.store)
        self.assertIn('Job %s (#1, test [1:2])' % self.task['id'], rendered)
        self.assertIn('Job %s (#1, other [1:3])' % other['id'], rendered)
        self.assertIn('Worker 1: Job %s (#1, other [1:3])' % other['id'], rendered)
        messages = [{'id': n, 'kind': 'owner', 'text': 'x' * 10000} for n in range(100)]
        with patch.object(parent_state, 'RESTART_NOTICE_CHARS', 500):
            self.assertLessEqual(len(parent_state.restarted_prompt(rows, messages, self.store)), 500)

    def test_write_fields(self):
        for op_id, data in (('tasks.create', {'id': 1, 'scratch': '/scratch', 'notes': 'private'}),
                            ('worktree.create', {'worktree': '/work', 'scratch': '/scratch', 'base_commit': 'abc'}),
                            ('workers.spawn', {'worker': 1, 'results': '/results'})):
            result = mcp._tool_result(control_api.Result(True, 'ok', data), self.store, control_api.BY_ID[op_id])
            shaped = json.loads(result['content'][0]['text'].split('\n')[-1])
            self.assertEqual(shaped, {k: v for k, v in data.items() if k != 'notes'})

    def test_worktree_and_spawn_calls(self):
        workspace = {'cwd': self.temp.name, 'worktree': self.temp.name, 'base_commit': 'abc', 'base_ref': 'main'}
        with patch('coordinator.workspaces.prepare_task_workspace', return_value=workspace), \
             patch('coordinator.close_saves.scratch', return_value=Path('/scratch')):
            created = json.loads(self.call('tasks_create', {'topic': '1:2', 'message': 1, 'title': 'Scratch job', 'notes': 'hidden'}).split('\n')[-1])
            self.assertEqual(created['scratch'], '/scratch')
            self.assertNotIn('notes', created)
            data = json.loads(self.call('worktree_create', {'task': self.task['id']}).split('\n')[-1])
            self.assertEqual(data['scratch'], '/scratch')
            self.assertEqual(data['base_commit'], 'abc')
        with patch('coordinator.workers.spawn_refusal', return_value=None):
            data = json.loads(self.call('workers_spawn', {'task': self.task['id'], 'kind': 'general', 'prompt': 'Go'}).split('\n')[-1])
            self.assertEqual(set(data), {'worker', 'results'})
