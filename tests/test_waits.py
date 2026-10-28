import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from coordinator import control_api, mcp, topics
from coordinator.service import Service
from coordinator.store import Store
from tests.test_service import ServiceTestsSupport, TOPIC, owner_update


class WaitTests(ServiceTestsSupport, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.store.put('waits_enabled', True)
        self.origin = self.store.message_save(TOPIC, 'owner', 'Build the job', telegram_message=405)

    def call(self, op, context_topic=TOPIC, **params):
        result = control_api.call(self.store, op, params, topic=context_topic)
        self.assertTrue(result.ok, result.text)
        return result

    def job(self, title='Build', topic=TOPIC):
        origin = self.origin if topic == TOPIC else self.store.message_save(topic, 'owner', title)
        return self.store.task_create(topic, title, worktree=str(self.root), origin=origin['id'])

    def wait(self, task=None, kind='owner', ask='Pick A or B', **params):
        arguments = dict(kind=kind, ask=ask, **params)
        if task is not None:
            arguments['task'] = task['id']
        return self.call('tasks.wait', **arguments).data

    def outbox(self, identifier):
        return dict(self.store.db.execute('SELECT * FROM outbox WHERE id=?', (identifier,)).fetchone())

    async def deliver_ask(self, wait):
        for unused in range(20):
            row = self.outbox(wait['ask_outbox'])
            if row['delivered']:
                return row
            self.assertTrue(await self.service.deliver_once())
        self.fail('The wait ask was not delivered after 20 deterministic passes.')

    def other_topic(self):
        topic = '-10042:9'
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', (topic, -10042, 9, 'Other', str(self.root)))
        return topic

    def callback(self, wait, telegram_message, update_id=902, user=7, topic=TOPIC, index=0):
        target = self.store.topic(topic)
        return {'update_id': update_id, 'callback_query': {
            'id': str(update_id), 'from': {'id': user},
            'data': 'wait:%d:%d' % (wait['id'], index),
            'message': {'message_id': telegram_message, 'message_thread_id': target['thread'],
                        'chat': {'id': target['chat'], 'type': 'supergroup'}}}}

    async def test_reply_to_ask_fires_owner_wait_once(self):
        task = self.job()
        wait = self.wait(task)
        self.assertEqual((wait['kind'], wait['state'], self.store.task_get(task['id'])['status']),
                         ('owner', 'armed', 'waiting'))
        ask = await self.deliver_ask(wait)
        self.assertEqual(ask['text'], 'Pick A or B')
        update = owner_update(901, 'Use A', reply=ask['telegram_message'])
        self.assertEqual(self.store.accept(update), 'queued')
        reply = dict(self.store.db.execute('SELECT * FROM messages WHERE telegram_message=901').fetchone())
        fired = self.store.wait_get(wait['id'])
        self.assertEqual((fired['state'], fired['outcome']), ('fired', 'answered'))
        self.assertEqual((self.store.task_get(task['id'])['status'], self.store.task_get(task['id'])['iteration']),
                         ('open', 1))
        current = self.store.task_iterations(task['id'])[-1]
        self.assertEqual((current['opened_by'], current['wait'], current['origin']),
                         ('wait', wait['id'], reply['id']))
        self.assertEqual(self.store.accept(update), 'duplicate')
        self.assertEqual(len(self.store.task_iterations(task['id'])), 2)

    async def test_button_fires_owner_wait_with_choice_and_rejects_other_users(self):
        task = self.job()
        wait = self.wait(task, buttons=[[{'text': 'A', 'callback_data': 'A'},
                                        {'text': 'B', 'callback_data': 'B'}]])
        ask = await self.deliver_ask(wait)
        keyboard = json.loads(ask['reply_markup'])['inline_keyboard']
        self.assertEqual([button for row in keyboard for button in row], [
            {'text': 'A', 'callback_data': 'wait:%d:0' % wait['id']},
            {'text': 'B', 'callback_data': 'wait:%d:1' % wait['id']}])
        self.assertEqual(self.store.accept(self.callback(wait, ask['telegram_message'], user=8)), 'unauthorized')
        self.assertEqual(self.store.wait_get(wait['id'])['state'], 'armed')
        self.assertEqual(self.store.accept(self.callback(wait, ask['telegram_message'], update_id=905,
                                                        topic=self.other_topic())), 'stale_callback')
        self.assertEqual(self.store.wait_get(wait['id'])['state'], 'armed')
        self.store.accept(self.callback(wait, ask['telegram_message'], update_id=903, index=1))
        fired = self.store.wait_get(wait['id'])
        self.assertEqual((fired['state'], fired['outcome']), ('fired', 'choice:B'))
        current = self.store.task_iterations(task['id'])[-1]
        origin = dict(self.store.db.execute('SELECT * FROM messages WHERE id=?', (current['origin'],)).fetchone())
        self.assertEqual((current['opened_by'], current['wait'], origin['kind']), ('wait', wait['id'], 'callback'))
        self.assertIn('B', origin['text'])
        self.store.accept(self.callback(wait, ask['telegram_message'], update_id=904))
        self.assertEqual(len(self.store.task_iterations(task['id'])), 2)

    async def test_shared_key_joins_jobs_across_chats_without_another_ask(self):
        first = self.job()
        second = self.job('Other job', self.other_topic())
        wait = self.wait(first, key='owner:account:codex')
        joined = self.wait(second, key='owner:account:codex')
        self.assertEqual((joined['id'], joined['ask_outbox']), (wait['id'], wait['ask_outbox']))
        self.assertEqual(sorted((job['task'], job['topic'], job['blocking']) for job in joined['jobs']),
                         [(first['id'], TOPIC, 1), (second['id'], '-10042:9', 1)])
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM waits').fetchone()[0], 1)
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM outbox WHERE text='Pick A or B'").fetchone()[0], 1)
        answer = self.store.message_save(TOPIC, 'owner', 'Codex is available', telegram_message=906)
        self.call('pending.fire', wait=wait['id'], message=answer['id'])
        self.assertEqual([(self.store.task_get(job['id'])['status'],
                           self.store.task_get(job['id'])['iteration']) for job in (first, second)],
                         [('open', 1), ('open', 1)])
        callbacks = [row['topic'] for row in self.store.messages_pending() if row['kind'] == 'callback']
        self.assertEqual(sorted(callbacks), sorted([TOPIC, '-10042:9']))
        self.assertEqual([self.store.task_iterations(job['id'])[-1]['origin'] for job in (first, second)],
                         [answer['id'], answer['id']])

    async def test_reply_with_an_owner_command_keeps_wait_armed_and_runs_command(self):
        task = self.job()
        wait = self.wait(task)
        ask = await self.deliver_ask(wait)
        self.assertEqual(self.store.accept(owner_update(921, '/waiting', reply=ask['telegram_message'])), 'control')
        self.assertEqual(self.store.wait_get(wait['id'])['state'], 'armed')
        self.assertEqual(self.store.task_get(task['id'])['status'], 'waiting')
        self.assertEqual(len(self.store.task_iterations(task['id'])), 1)
        reply = self.store.db.execute('SELECT text FROM outbox WHERE reply_to=921').fetchone()[0]
        self.assertEqual(reply, 'Waiting on you\nTorii · Build: Pick A or B')

    async def test_forwarded_reply_does_not_resolve_owner_wait(self):
        wait = self.wait(self.job())
        ask = await self.deliver_ask(wait)
        update = owner_update(922, 'Forwarded answer', reply=ask['telegram_message'])
        update['message']['forward_origin'] = {'type': 'hidden_user', 'sender_user_name': 'Someone else', 'date': 1}
        self.assertEqual(self.store.accept(update), 'queued')
        self.assertEqual(self.store.wait_get(wait['id'])['state'], 'armed')

    async def test_time_insert_wakes_sleeping_loop_through_durable_service_request(self):
        self.wait(self.job(), kind='time', at='2030-01-01T00:00:00Z')
        self.assertFalse(self.service.wake('waits').is_set())
        self.assertTrue(await self.service.controls_once())
        self.assertTrue(self.service.wake('waits').is_set())
        request = self.store.db.execute("SELECT state FROM service_requests WHERE op='waits.changed'").fetchone()
        self.assertEqual(request['state'], 'done')

    async def test_closed_dependency_fires_when_target_enters_waiting(self):
        target = self.job('Dependency')
        task = self.job('Dependent')
        wait = self.wait(task, kind='job', ask='Target round closes', job=target['id'], on='closed')
        self.wait(target, kind='time', at='2030-01-01T00:00:00Z')
        self.assertEqual((self.store.wait_get(wait['id'])['state'], self.store.wait_get(wait['id'])['outcome']),
                         ('fired', 'waiting'))
        self.assertEqual(self.store.task_get(task['id'])['status'], 'open')

    async def test_declared_worker_refusal_still_fires_and_wakes_coordinator(self):
        task = self.job()
        wait = self.wait(task, kind='time', at='1970-01-01T00:00:01Z',
                         then_provider='opencode', then_prompt='Continue the check')
        await self.service.waits_once()
        self.assertEqual(self.store.wait_get(wait['id'])['state'], 'fired')
        self.assertEqual(self.store.task_get(task['id'])['status'], 'open')
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM workers').fetchone()[0], 0)
        callbacks = [row for row in self.store.messages_pending() if row['kind'] == 'callback']
        self.assertEqual(len(callbacks), 1)
        self.assertIn('OpenCode workers are off.', callbacks[0]['text'])

    async def test_failed_round_creation_rolls_back_firing_and_callback(self):
        task = self.job()
        wait = self.wait(task)
        answer = self.store.message_save(TOPIC, 'owner', 'A')
        with patch.object(self.store, 'wait_open_round', side_effect=RuntimeError('round write failed')):
            with self.assertRaisesRegex(RuntimeError, 'round write failed'):
                self.store.wait_fire(wait['id'], message=answer['id'])
        self.assertEqual(self.store.wait_get(wait['id'])['state'], 'armed')
        self.assertEqual(self.store.task_get(task['id'])['status'], 'waiting')
        self.assertEqual(len(self.store.task_iterations(task['id'])), 1)
        self.assertEqual([row for row in self.store.messages_pending() if row['kind'] == 'callback'], [])

    async def test_time_wait_survives_store_and_service_restart(self):
        task = self.job()
        wait = self.wait(task, kind='time', ask='Try again later', at='1970-01-01T00:33:20+00:00')
        with patch('coordinator.store.time.time', return_value=1999):
            await self.service.waits_once()
        self.assertEqual(self.store.wait_get(wait['id'])['state'], 'armed')
        self.store.close()
        self.store = Store(self.root / 'state')
        self.service = Service(self.store, self.telegram, self.runner, self.root,
                               session_factory=self.service.session_factory)
        with patch('coordinator.store.time.time', return_value=2000):
            await self.service.waits_once()
            await self.service.waits_once()
        self.assertEqual((self.store.wait_get(wait['id'])['state'], self.store.wait_get(wait['id'])['outcome']),
                         ('fired', 'due'))
        self.assertEqual((self.store.task_get(task['id'])['status'], self.store.task_get(task['id'])['iteration']),
                         ('open', 1))
        self.assertEqual(len(self.store.task_iterations(task['id'])), 2)

    async def test_done_dependency_fires_waiters_and_dropped_dependency_wakes_them(self):
        for status in ('done', 'dropped'):
            with self.subTest(status=status):
                dependency = self.job('Dependency ' + status)
                task = self.job('Waiter ' + status)
                wait = self.wait(task, kind='job', ask='Wait for the dependency',
                                 job=dependency['id'], on='done')
                self.call('tasks.update', task=dependency['id'], status=status, note='Dependency ' + status)
                fired = self.store.wait_get(wait['id'])
                self.assertEqual((fired['state'], fired['outcome']), ('fired', status))
                self.assertEqual((self.store.task_get(task['id'])['status'],
                                  self.store.task_get(task['id'])['iteration']), ('open', 1))
                current = self.store.task_iterations(task['id'])[-1]
                self.assertEqual((current['opened_by'], current['wait']), ('wait', wait['id']))
                callback = dict(self.store.db.execute('SELECT * FROM messages WHERE id=?',
                                                      (current['origin'],)).fetchone())
                self.assertEqual((callback['topic'], callback['kind']), (TOPIC, 'callback'))
                self.assertIn(status, callback['text'])

    async def test_closing_chat_cancels_armed_job_and_chat_waits_only_in_that_chat(self):
        task = self.job()
        owner = self.wait(task)
        chat = self.wait(kind='time', topic=TOPIC, ask='Chat reminder', at='1970-01-01T00:33:20+00:00')
        other = self.wait(self.job('Other job', self.other_topic()), key='owner:other')
        request = self.store.service_request('topic.close', {'topic': TOPIC})
        row = dict(self.store.db.execute('SELECT * FROM service_requests WHERE id=?', (request,)).fetchone())
        await topics._close(SimpleNamespace(store=self.store), row, {'topic': TOPIC}, {'phase': 'stopped'})
        self.assertEqual([self.store.wait_get(wait['id'])['state'] for wait in (owner, chat, other)],
                         ['cancelled', 'cancelled', 'armed'])
        self.assertEqual(self.store.task_get(task['id'])['status'], 'dropped')
        with patch('coordinator.store.time.time', return_value=3000):
            await self.service.waits_once()
        self.assertEqual(self.store.wait_get(chat['id'])['state'], 'cancelled')
        self.assertEqual(len(self.store.task_iterations(task['id'])), 1)

    async def test_spawn_on_waiting_job_resolves_owner_wait_and_queues_next_round(self):
        task = self.job()
        wait = self.wait(task)
        answer = self.store.message_save(TOPIC, 'owner', 'Continue with A', telegram_message=907)
        spawned = self.call('workers.spawn', task=task['id'], provider='codex', prompt='Implement A',
                            message=answer['id'])
        worker = dict(self.store.db.execute('SELECT * FROM workers WHERE id=?',
                                            (spawned.data['worker'],)).fetchone())
        self.assertEqual((worker['task'], worker['status'], worker['iteration']), (task['id'], 'queued', 1))
        self.assertEqual(self.store.wait_get(wait['id'])['state'], 'fired')
        current = self.store.task_iterations(task['id'])[-1]
        self.assertEqual((current['opened_by'], current['wait'], current['origin']),
                         ('wait', wait['id'], answer['id']))
        self.assertEqual((self.store.task_get(task['id'])['status'], self.store.task_get(task['id'])['iteration']),
                         ('open', 1))

    async def test_pending_fire_opens_round_with_owner_origin_and_is_idempotent(self):
        task = self.job()
        wait = self.wait(task)
        closed = self.store.task_iterations(task['id'])[0]
        self.assertEqual((closed['outcome'], closed['reason']), ('waiting', 'Pick A or B'))
        self.assertIsNotNone(closed['closed'])
        answer = self.store.message_save(TOPIC, 'owner', 'A', telegram_message=908)
        result = self.call('pending.fire', wait=wait['id'], message=answer['id'])
        self.assertEqual((result.data['id'], result.data['state']), (wait['id'], 'fired'))
        current = self.store.task_iterations(task['id'])[-1]
        self.assertEqual((current['k'], current['opened_by'], current['wait'], current['origin'], current['closed']),
                         (1, 'wait', wait['id'], answer['id'], None))
        self.call('pending.fire', wait=wait['id'], message=answer['id'])
        self.assertEqual(self.store.task_iterations(task['id'])[0], closed)
        self.assertEqual(len(self.store.task_iterations(task['id'])), 2)
        callback = dict(self.store.db.execute("SELECT * FROM messages WHERE kind='callback'").fetchone())
        self.store.message_delivered(callback['id'], 'read')
        next_wait = self.wait(task)
        self.call('pending.fire', wait=next_wait['id'], message=callback['id'])
        original = dict(self.store.db.execute('SELECT * FROM messages WHERE id=?', (callback['id'],)).fetchone())
        self.assertEqual((original['text'], original['delivered']), (callback['text'], 'received'))
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM messages WHERE kind='callback' AND delivered='pending'").fetchone()[0], 1)
        current = self.store.task_iterations(task['id'])[-1]
        self.assertEqual((current['k'], current['origin']), (2, callback['id']))

    async def test_waiting_command_and_owner_header_show_armed_wait(self):
        task = self.job()
        wait = self.wait(task)
        listed = self.call('pending.list', owner=True)
        expected = 'Waiting on you\nTorii · Build: Pick A or B'
        self.assertEqual(listed.text, expected)
        self.assertEqual([row['id'] for row in listed.data['waits']], [wait['id']])
        self.assertEqual(listed.data['held'], [])
        self.assertEqual(self.store.accept(owner_update(909, '/waiting')), 'control')
        reply = dict(self.store.db.execute('SELECT * FROM outbox WHERE reply_to=909 ORDER BY id DESC').fetchone())
        self.assertEqual(reply['text'], expected)
        self.assertIsNone(self.store.db.execute('SELECT 1 FROM messages WHERE telegram_message=909').fetchone())
        self.assertEqual(self.store.accept(owner_update(910, 'What is next?')), 'queued')
        message = dict(self.store.db.execute('SELECT * FROM messages WHERE telegram_message=910').fetchone())
        prepared = await self.service._prepare_message(message)
        self.assertIn('waiting="Build: Pick A or B"', prepared.split('] ', 1)[0])
        self.call('pending.fire', wait=wait['id'], message=message['id'])
        self.assertEqual(self.call('pending.list', owner=True).text, 'Nothing is waiting on you.')
        prepared = await self.service._prepare_message(message)
        self.assertNotIn('waiting=', prepared.split('] ', 1)[0])

    async def test_chat_wait_fires_without_creating_a_job(self):
        wait = self.wait(topic=TOPIC, ask='Choose the chat schedule')
        self.assertEqual(wait['jobs'], [])
        answer = self.store.message_save(TOPIC, 'owner', 'Use Tuesdays', telegram_message=911)
        self.call('pending.fire', wait=wait['id'], message=answer['id'])
        fired = self.store.wait_get(wait['id'])
        self.assertEqual((fired['state'], fired['outcome'], fired['jobs']), ('fired', 'answered', []))
        callbacks = [row for row in self.store.messages_pending() if row['kind'] == 'callback']
        self.assertEqual([(row['topic'], row['kind']) for row in callbacks], [(TOPIC, 'callback')])
        self.assertIn('Choose the chat schedule', callbacks[0]['text'])
        self.assertEqual(self.store.tasks_list(), [])
        secret = self.wait(topic=TOPIC, key='secret:SAMPLE_TOKEN', ask='Fill the sample card')
        with self.store.transaction():
            envelope = self.store.db.execute('''INSERT INTO envelopes
                (name,reason,consumer,topic,state,created,expires,updated)
                VALUES ('SAMPLE_TOKEN','Test card','test',?,'filled',1,2,1)''', (TOPIC,)).lastrowid
            filled = self.store.message_secret_filled(TOPIC, envelope, 'The sample card is filled.')
        fired = self.store.wait_get(secret['id'])
        self.assertEqual((fired['state'], fired['fired_by']), ('fired', str(filled['id'])))

    async def test_owner_wait_replacement_stays_waiting_and_cancel_opens_one_round(self):
        task = self.job()
        original = self.wait(task)
        replacement = self.wait(task, ask='Pick C or D')
        self.assertNotEqual(original['id'], replacement['id'])
        self.assertEqual(self.store.wait_get(original['id'])['state'], 'cancelled')
        self.assertEqual([row['id'] for row in self.store.waits_list(owner=True)], [replacement['id']])
        self.assertEqual(len(self.store.task_iterations(task['id'])), 1)
        self.assertEqual(self.store.task_get(task['id'])['status'], 'waiting')
        rejected = control_api.call(self.store, 'tasks.update', {'task': task['id'], 'status': 'open'}, topic=TOPIC)
        self.assertFalse(rejected.ok)
        self.assertIn('workers.spawn or pending.fire', rejected.text)
        result = self.call('pending.cancel', wait=replacement['id'], reason='No decision is needed')
        self.assertEqual((result.data['state'], result.data['reason']), ('cancelled', 'No decision is needed'))
        self.call('pending.cancel', wait=replacement['id'], reason='No decision is needed')
        self.assertEqual(len(self.store.task_iterations(task['id'])), 2)
        self.assertEqual(self.store.task_get(task['id'])['status'], 'open')
        callbacks = [row for row in self.store.messages_pending() if row['kind'] == 'callback']
        self.assertEqual(len(callbacks), 1)
        self.assertEqual(self.store.task_iterations(task['id'])[-1]['origin'], callbacks[0]['id'])

    async def test_declared_worker_is_queued_by_time_trigger(self):
        task = self.job()
        wait = self.wait(task, kind='time', ask='Run the planned check', at='1970-01-01T00:33:20+00:00',
                         then_provider='codex', then_prompt='Check the saved result',
                         then_model='gpt-6.1-sol', then_effort='max', then_work='research')
        with patch('coordinator.store.time.time', return_value=2000):
            await self.service.waits_once()
            await self.service.waits_once()
        workers = [dict(row) for row in self.store.db.execute('SELECT * FROM workers WHERE task=?', (task['id'],))]
        self.assertEqual([(row['provider'], row['prompt'], row['model'], row['effort'], row['work'],
                           row['iteration'], row['status']) for row in workers],
                         [('codex', 'Check the saved result', 'gpt-6.1-sol', 'max', 'research', 1, 'queued')])
        self.assertEqual(self.store.wait_get(wait['id'])['state'], 'fired')
        current = self.store.task_iterations(task['id'])[-1]
        origin = dict(self.store.db.execute('SELECT * FROM messages WHERE id=?', (current['origin'],)).fetchone())
        self.assertEqual((current['opened_by'], current['wait'], origin['kind']), ('wait', wait['id'], 'callback'))

    async def test_owner_list_filters_time_waits_and_includes_held_copies(self):
        owner = self.wait(self.job())
        timer = self.wait(self.job('Timer'), kind='time', ask='Later', at='1970-01-01T00:33:20+00:00')
        held = self.job('Held copy')
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET copy='held',copy_reason='Save verification failed' WHERE id=?",
                                  (held['id'],))
        listed = self.call('pending.list', owner=True)
        self.assertEqual([row['id'] for row in listed.data['waits']], [owner['id']])
        self.assertEqual([(row['id'], row['copy_reason']) for row in listed.data['held']],
                         [(held['id'], 'Save verification failed')])
        self.assertIn('Save verification failed', listed.text)
        all_waits = self.call('pending.list')
        self.assertEqual(sorted(row['id'] for row in all_waits.data['waits']), sorted([owner['id'], timer['id']]))

    async def test_home_waiting_message_is_updated_in_place(self):
        task = self.job()
        wait = self.wait(task)
        for unused in range(20):
            if not await self.service.deliver_once():
                break
        home = [dict(row) for row in self.store.db.execute(
            "SELECT * FROM outbox WHERE topic=? AND text LIKE 'Waiting on you%'", (TOPIC,))]
        self.assertEqual(len(home), 1)
        self.assertEqual((home[0]['delivered'], home[0]['text']),
                         (1, 'Waiting on you\nTorii · Build: Pick A or B'))
        replacement = self.wait(task, ask='Pick C or D')
        edits = [dict(row) for row in self.store.db.execute('SELECT * FROM outbox WHERE edit_message=?',
                                                         (home[0]['telegram_message'],))]
        self.assertEqual([(row['topic'], row['text']) for row in edits],
                         [(TOPIC, 'Waiting on you\nTorii · Build: Pick C or D')])
        answer = self.store.message_save(TOPIC, 'owner', 'C', telegram_message=912)
        self.call('pending.fire', wait=replacement['id'], message=answer['id'])
        self.store.refresh_owner_waiting()
        edits = [dict(row) for row in self.store.db.execute('SELECT * FROM outbox WHERE edit_message=?',
                                                         (home[0]['telegram_message'],))]
        self.assertEqual([(row['topic'], row['text']) for row in edits], [(TOPIC, 'Nothing is waiting on you.')])

    async def test_home_waiting_command_lists_all_chats_and_other_chats_list_their_own(self):
        home = self.wait(self.job())
        other_topic = self.other_topic()
        other = self.wait(self.job('Other job', other_topic), ask='Choose the other job')
        listed = self.call('pending.list', owner=True, context_topic=other_topic)
        self.assertEqual([row['id'] for row in listed.data['waits']], [home['id'], other['id']])
        scoped = self.call('pending.list', owner=True, topic=other_topic)
        self.assertEqual([row['id'] for row in scoped.data['waits']], [other['id']])
        self.assertEqual(self.store.accept(owner_update(913, '/waiting')), 'control')
        all_text = self.store.db.execute('SELECT text FROM outbox WHERE reply_to=913').fetchone()[0]
        self.assertEqual(all_text, 'Waiting on you\nTorii · Build: Pick A or B\n'
                         'Other · Other job: Choose the other job')
        update = owner_update(914, '/waiting')
        update['message']['message_thread_id'] = 9
        self.assertEqual(self.store.accept(update), 'control')
        other_text = self.store.db.execute('SELECT text FROM outbox WHERE reply_to=914').fetchone()[0]
        self.assertEqual(other_text, 'Waiting on you\nOther · Other job: Choose the other job')

    async def test_time_watch_reopens_done_job_without_changing_its_previous_outcome(self):
        task = self.job()
        self.call('tasks.update', task=task['id'], status='done')
        previous = self.store.task_iterations(task['id'])[0]
        wait = self.wait(task, kind='time', ask='Check the delivered result',
                         at='1970-01-01T00:33:20+00:00', watch=True)
        self.assertEqual((self.store.task_get(task['id'])['status'], wait['jobs'][0]['blocking']), ('done', 0))
        with patch('coordinator.store.time.time', return_value=2000):
            await self.service.waits_once()
        self.assertEqual((self.store.task_get(task['id'])['status'], self.store.task_get(task['id'])['iteration']),
                         ('open', 1))
        self.assertEqual(self.store.task_iterations(task['id'])[0], previous)

    async def test_invalid_waits_and_unrelated_fire_messages_are_refused_without_mutation(self):
        task = self.job()
        wait = self.wait(task)
        invalid = [
            {'kind': 'signal', 'ask': 'P4 trigger'},
            {'kind': 'owner', 'ask': 'Wrong fields', 'at': '1970-01-01T00:33:20+00:00'},
            {'kind': 'time', 'ask': 'Missing timezone', 'at': '2026-10-20T12:00:00'},
            {'kind': 'job', 'ask': 'Self dependency', 'job': task['id']},
            {'kind': 'owner', 'ask': 'Missing worker prompt', 'then_provider': 'codex'},
        ]
        for params in invalid:
            with self.subTest(params=params):
                result = control_api.call(self.store, 'tasks.wait', dict(params, task=task['id']), topic=TOPIC)
                self.assertFalse(result.ok, result.text)
                self.assertEqual(self.store.wait_get(wait['id'])['state'], 'armed')
        unrelated = self.store.message_save(self.other_topic(), 'owner', 'Answer for a different chat')
        result = control_api.call(self.store, 'pending.fire', {'wait': wait['id'], 'message': unrelated['id']}, topic=TOPIC)
        self.assertFalse(result.ok, result.text)
        self.assertEqual(self.store.wait_get(wait['id'])['state'], 'armed')
        self.assertEqual(len(self.store.waits_list()), 1)
        self.assertEqual(len(self.store.task_iterations(task['id'])), 1)

    async def test_wait_operations_are_available_through_mcp(self):
        task = self.job()
        result = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
            'name': 'tasks_wait', 'arguments': {'task': task['id'], 'kind': 'owner', 'ask': 'Pick A or B'}}}, topic=TOPIC)
        self.assertFalse(result.get('isError', False), result)
        wait = json.loads(result['content'][0]['text'].splitlines()[-1])
        self.assertEqual((wait['kind'], wait['state']), ('owner', 'armed'))
        other = self.wait(self.job('Other job', self.other_topic()))
        listed = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
            'name': 'pending_list', 'arguments': {'owner': True}}}, topic=TOPIC)
        self.assertFalse(listed.get('isError', False), listed)
        self.assertEqual([row['id'] for row in json.loads(listed['content'][0]['text'].splitlines()[-1])['waits']],
                         [wait['id'], other['id']])
        scoped = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
            'name': 'pending_list', 'arguments': {'owner': True, 'topic': TOPIC}}}, topic=TOPIC)
        self.assertFalse(scoped.get('isError', False), scoped)
        self.assertEqual([row['id'] for row in json.loads(scoped['content'][0]['text'].splitlines()[-1])['waits']],
                         [wait['id']])


if __name__ == '__main__':
    unittest.main()
