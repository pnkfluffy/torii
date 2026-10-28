import asyncio
from contextlib import redirect_stdout
from io import StringIO
import json
import re
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from coordinator import control_api, mcp, onboarding, topics
from coordinator.telegram import Telegram
from coordinator.waits import owner_header
from coordinator.formatting import markdown_to_html
from tests.test_service import ServiceTestsSupport, TOPIC, owner_update


class WaitReviewSupport(ServiceTestsSupport):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.origin = self.store.message_save(TOPIC, 'owner', 'Build the job', telegram_message=405)

    def call(self, op, context_topic=TOPIC, **params):
        return control_api.call(self.store, op, params, topic=context_topic)

    def ok(self, op, context_topic=TOPIC, **params):
        result = self.call(op, context_topic, **params)
        self.assertTrue(result.ok, result.text)
        return result

    def job(self, title='Build', topic=TOPIC):
        origin = self.origin if topic == TOPIC else self.store.message_save(topic, 'owner', title)
        return self.store.task_create(topic, title, worktree=str(self.root), origin=origin['id'])

    def wait(self, task=None, kind='owner', ask='Pick A or B', **params):
        arguments = dict(kind=kind, ask=ask, **params)
        if task is not None:
            arguments['task'] = task['id']
        return self.ok('tasks.wait', **arguments).data

    def other_topic(self, thread=9, name='Other'):
        topic = '-10042:%d' % thread
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', (topic, -10042, thread, name, str(self.root)))
        return topic

    def outbox(self, identifier):
        return dict(self.store.db.execute('SELECT * FROM outbox WHERE id=?', (identifier,)).fetchone())

    def callbacks(self, topic=None):
        return [dict(row) for row in self.store.db.execute('''SELECT * FROM messages
            WHERE kind='callback' AND (? IS NULL OR topic=?) ORDER BY id''', (topic, topic))]

    def callback(self, wait, message, update_id=902, topic=TOPIC, index=0):
        target = self.store.topic(topic)
        return {'update_id': update_id, 'callback_query': {
            'id': str(update_id), 'from': {'id': 7}, 'data': 'wait:%d:%s' % (wait['id'], index),
            'message': {'message_id': message, 'message_thread_id': target['thread'],
                        'chat': {'id': target['chat'], 'type': 'supergroup'}}}}

    async def deliver_all(self):
        for unused in range(100):
            if not await self.service.deliver_once():
                return
        self.fail('Delivery did not settle after 100 deterministic passes.')

    async def close_topic(self, topic):
        request = self.store.service_request('topic.close', {'topic': topic})
        row = dict(self.store.db.execute('SELECT * FROM service_requests WHERE id=?', (request,)).fetchone())
        await topics._close(SimpleNamespace(store=self.store), row, {'topic': topic}, {'phase': 'stopped'})
        saved = self.store.db.execute('SELECT result FROM service_requests WHERE id=?', (request,)).fetchone()
        return json.loads(saved['result'])

    def use_telegram_sender(self):
        original_call = self.telegram.call

        async def call(method, **params):
            result = await original_call(method, **params)
            if method == 'sendMessage':
                return {'message_id': 1000 + len(self.telegram.sent)}
            return result

        async def send(row):
            self.telegram.sent.append(dict(row))
            return await Telegram.send(self.telegram, row)

        self.telegram.call = call
        self.telegram.send = send


class WaitReviewTests(WaitReviewSupport, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.store.put('waits_enabled', True)

    async def test_b1_parked_workers_refuse_blocking_waits(self):
        for status, kind, params in (
                ('waiting_for_quota', 'time', {'at': '2030-01-01T00:00:00Z'}),
                ('waiting_for_secret', 'owner', {'key': 'secret:SAMPLE_TOKEN'})):
            with self.subTest(status=status):
                task = self.job('Parked ' + status)
                with self.store.db:
                    worker = self.store.db.execute('''INSERT INTO workers
                        (task,topic,provider,prompt,cwd,workspace,status,created,updated)
                        VALUES (?,?,?,?,?,?,?,1,1)''', (task['id'], TOPIC, 'codex', 'Do the check',
                        str(self.root), json.dumps({'cwd': str(self.root)}), status)).lastrowid
                result = self.call('tasks.wait', task=task['id'], kind=kind, ask='Wait for ' + status, **params)
                self.assertFalse(result.ok, result.text)
                self.assertEqual(self.store.task_get(task['id'])['status'], 'open')
                self.assertEqual(self.store.db.execute('SELECT status FROM workers WHERE id=?',
                                                     (worker,)).fetchone()[0], status)
                self.assertEqual(len(self.store.task_iterations(task['id'])), 1)
        self.assertEqual(self.store.waits_list(), [])

    async def test_b2_shared_owner_wait_moves_ask_to_remaining_chat(self):
        other = self.other_topic()
        first, second = self.job(), self.job('Other job', other)
        wait = self.wait(first, key='owner:shared-account', ask='Choose the account')
        self.wait(second, key='owner:shared-account', ask='Choose the account')
        await self.deliver_all()
        original_ask = self.outbox(wait['ask_outbox'])
        await self.close_topic(TOPIC)
        moved = self.store.wait_get(wait['id'])
        self.assertEqual((moved['state'], moved['topic']), ('armed', other))
        self.assertEqual([(link['task'], link['topic']) for link in moved['jobs']], [(second['id'], other)])
        self.assertNotEqual(moved['ask_outbox'], original_ask['id'])
        self.assertEqual(self.store.task_get(first['id'])['status'], 'dropped')
        self.assertEqual(self.store.task_get(second['id'])['status'], 'waiting')
        await self.deliver_all()
        ask = self.outbox(moved['ask_outbox'])
        self.assertEqual((ask['topic'], ask['text'], ask['delivered']), (other, 'Choose the account', 1))
        update = owner_update(930, 'Use account two', reply=ask['telegram_message'])
        update['message']['message_thread_id'] = 9
        self.assertEqual(self.store.accept(update), 'queued')
        self.assertEqual((self.store.task_get(second['id'])['status'],
                          self.store.task_get(second['id'])['iteration']), ('open', 1))
        self.assertEqual([row['topic'] for row in self.callbacks()], [other])

    async def test_b2_shared_job_wait_survives_creating_chat_close(self):
        other = self.other_topic()
        deploy = self.job('Deploy', self.other_topic(10, 'Deploys'))
        first, second = self.job(), self.job('Other job', other)
        wait = self.wait(first, kind='job', key='job:shared-deploy', ask='After the deploy', job=deploy['id'])
        joined = self.wait(second, kind='job', key='job:shared-deploy', ask='After the deploy', job=deploy['id'])
        self.assertEqual(joined['id'], wait['id'])
        await self.close_topic(TOPIC)
        self.assertEqual((self.store.wait_get(wait['id'])['state'],
                          self.store.wait_get(wait['id'])['topic']), ('armed', other))
        self.ok('tasks.update', task=deploy['id'], status='done')
        self.assertEqual((self.store.task_get(second['id'])['status'],
                          self.store.task_get(second['id'])['iteration']), ('open', 1))
        self.assertEqual([row['topic'] for row in self.callbacks()], [other])

    async def test_b2_waiting_job_without_blocking_wait_gets_one_callback(self):
        other = self.other_topic()
        task = self.job('Stranded job', other)
        wait = self.wait(task)
        with self.store.db:
            self.store.db.execute("UPDATE waits SET state='cancelled' WHERE id=?", (wait['id'],))
        await self.service.waits_once()
        await self.service.waits_once()
        self.assertEqual((self.store.task_get(task['id'])['status'],
                          self.store.task_get(task['id'])['iteration']), ('open', 1))
        self.assertEqual([row['topic'] for row in self.callbacks()], [other])
        self.assertEqual(self.store.task_iterations(task['id'])[-1]['origin'], self.callbacks()[0]['id'])

    async def test_b3_done_and_dropped_retire_blocking_waits_keep_watches(self):
        for status in ('done', 'dropped'):
            with self.subTest(status=status):
                task = self.job('Round ending ' + status)
                self.ok('tasks.update', task=task['id'], status='done')
                watch = self.wait(task, kind='time', ask='Watch the delivered result',
                                  at='2030-01-02T00:00:00Z', watch=True)
                origin = self.store.message_save(TOPIC, 'owner', 'Continue the job')
                spawned = self.ok('workers.spawn', task=task['id'], provider='codex',
                                  prompt='Continue', message=origin['id'])
                with self.store.db:
                    self.store.db.execute("UPDATE workers SET status='done' WHERE id=?", (spawned.data['worker'],))
                owner = self.wait(task, ask='Approve this round?')
                timer = self.wait(task, kind='time', ask='Retry this round', at='2030-01-03T00:00:00Z')
                self.ok('tasks.update', task=task['id'], status=status)
                self.assertEqual(self.store.task_get(task['id'])['status'], status)
                self.assertEqual([self.store.wait_get(item['id'])['state'] for item in (owner, timer, watch)],
                                 ['cancelled', 'cancelled', 'armed'])
                self.assertEqual(self.store.task_iterations(task['id'])[-1]['outcome'], status)
                self.assertNotIn('Approve this round?', self.ok('pending.list', owner=True).text)
                self.assertNotIn('Approve this round?', owner_header(self.store, TOPIC))
                card = self.outbox(self.store.get('owner_waiting_card'))
                self.assertNotIn('Approve this round?', card['text'])

    async def test_b3_firing_or_spawn_clears_old_blocking_waits(self):
        dependency = self.job('Dependency')
        task = self.job('Waiter')
        owner = self.wait(task, ask='Approve the plan?')
        timer = self.wait(task, kind='time', ask='Check tomorrow', at='1970-01-01T00:33:20Z')
        self.wait(task, kind='job', ask='After the dependency', job=dependency['id'])
        self.ok('tasks.update', task=dependency['id'], status='done')
        self.assertEqual([self.store.wait_get(item['id'])['state'] for item in (owner, timer)],
                         ['cancelled', 'cancelled'])
        self.assertEqual(self.store.task_get(task['id'])['status'], 'open')
        old_timer = self.wait(task, kind='time', ask='Check later', at='1970-01-01T00:33:20Z')
        go = self.store.message_save(TOPIC, 'owner', 'Do it now', telegram_message=990)
        spawned = self.ok('workers.spawn', task=task['id'], provider='codex', prompt='Do it', message=go['id'])
        with self.store.db:
            self.store.db.execute("UPDATE workers SET status='done' WHERE id=?", (spawned.data['worker'],))
        current = self.wait(task, ask='Ship this result?')
        with patch('coordinator.store.time.time', return_value=2000):
            await self.service.waits_once()
        self.assertEqual((self.store.wait_get(old_timer['id'])['state'],
                          self.store.wait_get(current['id'])['state']), ('cancelled', 'armed'))
        self.assertEqual((self.store.task_get(task['id'])['status'],
                          self.store.task_get(task['id'])['iteration']), ('waiting', 2))
        self.assertEqual(len(self.store.task_iterations(task['id'])), 3)

    async def test_b3_ending_shared_round_detaches_only_finished_job(self):
        other = self.other_topic()
        for status in ('done', 'dropped'):
            with self.subTest(status=status):
                first, second = self.job('Finishing ' + status), self.job('Still waiting ' + status, other)
                wait = self.wait(first, key='owner:shared-' + status)
                self.wait(second, key='owner:shared-' + status)
                self.ok('tasks.update', task=first['id'], status=status)
                remaining = self.store.wait_get(wait['id'])
                self.assertEqual((remaining['state'], [link['task'] for link in remaining['jobs']]),
                                 ('armed', [second['id']]))
                self.assertEqual(self.store.task_get(second['id'])['status'], 'waiting')
                self.assertEqual(self.store.task_iterations(first['id'])[-1]['outcome'], status)

    async def test_b4_default_time_keys_keep_each_ask(self):
        first = self.wait(topic=TOPIC, kind='time', ask='Check the deploy', at='2030-01-01T00:00:00Z')
        second = self.wait(topic=TOPIC, kind='time', ask='Ping the vendor', at='2030-01-01T09:00:00+09:00')
        task = self.job()
        third = self.wait(task, kind='time', ask='Re-run CI', at='2030-01-02T00:00:00Z')
        fourth = self.wait(task, kind='time', ask='Ask about pricing', at='2030-01-02T00:00:00Z')
        self.assertEqual(len({item['id'] for item in (first, second, third, fourth)}), 4)
        self.assertEqual([item['ask'] for item in self.store.waits_list()],
                         ['Check the deploy', 'Ping the vendor', 'Re-run CI', 'Ask about pricing'])
        repeated = self.wait(task, kind='time', ask='Re-run CI', at='2030-01-02T09:00:00+09:00')
        self.assertEqual(repeated['id'], third['id'])

    async def test_b4_default_job_keys_keep_each_waiter_and_ask(self):
        other = self.other_topic()
        dependency = self.job('Deploy', self.other_topic(10, 'Deploys'))
        first, second = self.job(), self.job('Other job', other)
        wait = self.wait(first, kind='job', ask='After the deploy', job=dependency['id'])
        different_waiter = self.wait(second, kind='job', ask='After the deploy', job=dependency['id'])
        different_ask = self.wait(first, kind='job', ask='Continue once deploy is live', job=dependency['id'])
        self.assertEqual(len({item['id'] for item in (wait, different_waiter, different_ask)}), 3)
        self.assertEqual([item['ask'] for item in self.store.waits_list()],
                         ['After the deploy', 'After the deploy', 'Continue once deploy is live'])
        await self.close_topic(TOPIC)
        self.ok('tasks.update', task=dependency['id'], status='done')
        self.assertEqual(self.store.task_get(second['id'])['status'], 'open')
        self.assertEqual([row['topic'] for row in self.callbacks()], [other])
        self.assertIn('After the deploy', self.callbacks()[0]['text'])

    async def test_n1_html_card_fits_one_message_and_edits_same_id(self):
        self.use_telegram_sender()
        task = self.job()
        self.wait(task, ask='Compare ' + '<' * 1015)
        await self.deliver_all()
        cards = [dict(row) for row in self.store.db.execute("SELECT * FROM outbox WHERE kind='owner-waiting'")]
        self.assertEqual(len(cards), 1)
        self.assertIn('Torii · Build: Compare ', cards[0]['text'])
        self.assertLessEqual(len(markdown_to_html(cards[0]['text'])), 4096)
        message = cards[0]['telegram_message']
        self.wait(self.job('Second'), ask='Pick C or D')
        await self.deliver_all()
        self.wait(task, ask='Decide ' + 'x' * 3600)
        await self.deliver_all()
        cards = [dict(row) for row in self.store.db.execute(
            "SELECT * FROM outbox WHERE kind='owner-waiting' ORDER BY id")]
        self.assertEqual(sum(row['edit_message'] is None for row in cards), 1)
        self.assertEqual([row['edit_message'] for row in cards[1:]], [message, message])
        self.assertEqual(cards[-1]['telegram_message'], message)
        self.assertIn('Torii · Build: Decide ', cards[-1]['text'])
        self.assertIn('Torii · Second: Pick C or D', cards[-1]['text'])
        self.assertTrue(all(len(markdown_to_html(row['text'])) <= 4096 for row in cards))

    async def test_n2_card_change_during_delivery_keeps_sent_row_and_queues_fresh_edit(self):
        self.use_telegram_sender()
        self.wait(self.job())
        await self.deliver_all()
        home_message = self.outbox(self.store.get('owner_waiting_card'))['telegram_message']
        second = self.wait(self.job('Second'), ask='Pick C or D')
        original_send = self.telegram.send
        sent_rows = []

        async def racing_send(row):
            if row['kind'] == 'owner-waiting' and not sent_rows:
                sent_rows.append(dict(row))
                self.ok('pending.cancel', wait=second['id'], reason='Answered elsewhere')
                self.assertEqual(self.outbox(row['id'])['text'], row['text'])
            return await original_send(row)

        self.telegram.send = racing_send
        await self.deliver_all()
        self.assertEqual(len(sent_rows), 1)
        card = self.outbox(self.store.get('owner_waiting_card'))
        self.assertNotEqual(card['id'], sent_rows[0]['id'])
        self.assertEqual((card['text'], card['edit_message'], card['telegram_message']),
                         ('Waiting on you\nTorii · Build: Pick A or B', home_message, home_message))
        self.assertEqual(self.outbox(sent_rows[0]['id'])['retired'], 1)
        shown = [params for method, params in self.telegram.calls if method == 'editMessageText']
        self.assertIn('Pick C or D', shown[-2]['text'])
        self.assertEqual((shown[-1]['message_id'], shown[-1]['text']),
                         (home_message, 'Waiting on you\nTorii · Build: Pick A or B'))
        self.store.refresh_owner_waiting()
        self.assertFalse(await self.service.deliver_once())

    async def test_n2_initial_card_change_during_delivery_edits_the_returned_message(self):
        self.use_telegram_sender()
        first = self.wait(self.job())
        second = self.wait(self.job('Second'), ask='Pick C or D')
        original_send = self.telegram.send
        initial_rows = []
        returned_messages = []

        async def racing_send(row):
            initial = row['kind'] == 'owner-waiting' and not initial_rows
            if initial:
                initial_rows.append(dict(row))
                self.assertIsNone(row['edit_message'])
                self.ok('pending.cancel', wait=second['id'], reason='Answered elsewhere')
                self.assertEqual(self.outbox(row['id'])['text'], row['text'])
                self.assertEqual(self.outbox(row['id'])['retired'], 1)
            result = await original_send(row)
            if initial:
                returned_messages.append(result['message_id'])
            return result

        self.telegram.send = racing_send
        await self.deliver_all()
        self.assertEqual((len(initial_rows), len(returned_messages)), (1, 1))
        initial = self.outbox(initial_rows[0]['id'])
        self.assertEqual((initial['text'], initial['retired'], initial['delivered']),
                         (initial_rows[0]['text'], 1, 1))
        sent = [params for method, params in self.telegram.calls
                if method == 'sendMessage' and params['text'].startswith('Waiting on you')]
        self.assertEqual(len(sent), 1)
        self.assertIn('Torii · Second: Pick C or D', sent[0]['text'])
        edits = [params for method, params in self.telegram.calls if method == 'editMessageText']
        self.assertEqual([(row['message_id'], row['text']) for row in edits],
                         [(returned_messages[0], 'Waiting on you\nTorii · Build: Pick A or B')])
        latest = self.outbox(self.store.get('owner_waiting_card'))
        self.assertNotEqual(latest['id'], initial['id'])
        self.assertEqual((latest['text'], latest['edit_message'], latest['telegram_message'], latest['delivered']),
                         ('Waiting on you\nTorii · Build: Pick A or B', returned_messages[0], returned_messages[0], 1))
        self.assertEqual([self.store.wait_get(item['id'])['state'] for item in (first, second)],
                         ['armed', 'cancelled'])
        self.store.refresh_owner_waiting()
        self.assertFalse(await self.service.deliver_once())

    async def test_n3_wait_button_bounds_record_updates(self):
        task = self.job()
        wait = self.wait(task, buttons=[[{'text': 'A', 'callback_data': 'A'}]])
        await self.deliver_all()
        ask = self.outbox(wait['ask_outbox'])['telegram_message']
        values = ('wait:99999999999999999999:0', 'wait:9223372036854775808:0',
                  'wait:%d:99999999999999999999' % wait['id'], 'wait:%d:-1' % wait['id'])
        for update_id, data in enumerate(values, 960):
            with self.subTest(data=data):
                update = self.callback(wait, ask, update_id=update_id)
                update['callback_query']['data'] = data
                self.assertEqual(self.store.accept(update), 'stale_callback')
                self.assertEqual(self.store.db.execute('SELECT id FROM updates WHERE id=?',
                                                     (update_id,)).fetchone()[0], update_id)
                self.assertEqual(self.store.wait_get(wait['id'])['state'], 'armed')
        self.assertEqual(self.store.accept(self.callback(wait, ask, update_id=965)), 'wait_choice_received')
        self.assertEqual(self.store.task_get(task['id'])['status'], 'open')

    async def test_n3_group_migration_moves_live_and_cancelled_waits(self):
        live = self.wait(self.job())
        cancelled = self.wait(self.job('Cancelled'), ask='Old question')
        self.ok('pending.cancel', wait=cancelled['id'], reason='Not needed')
        update = {'update_id': 995, 'message': {'message_id': 995, 'from': {'id': 7, 'is_bot': False},
                  'chat': {'id': -10042, 'type': 'group', 'title': 'Test group'}, 'migrate_to_chat_id': -10077}}
        self.assertEqual(self.store.accept(update), 'service_event')
        self.assertEqual(self.store.accept(update), 'duplicate')
        self.assertEqual(self.store.chat(), -10077)
        self.assertEqual([(self.store.wait_get(item['id'])['topic'], self.store.wait_get(item['id'])['state'])
                          for item in (live, cancelled)],
                         [('-10077:4', 'armed'), ('-10077:4', 'cancelled')])
        self.assertEqual(self.store.wait_get(live['id'])['jobs'][0]['topic'], '-10077:4')
        self.assertIsNone(self.store.topic(TOPIC))
        self.assertEqual(self.store.db.execute('PRAGMA foreign_key_check').fetchall(), [])
        self.assertEqual(self.store.db.execute('SELECT id FROM updates WHERE id=995').fetchone()[0], 995)

    async def test_n3_hook_spawn_errors_record_problem_without_crashing_intake(self):
        for offset, trigger in enumerate(('reply', 'button', 'secret', 'job', 'manual')):
            with self.subTest(trigger=trigger):
                task = self.job('Hook ' + trigger)
                params = {'then_provider': 'codex', 'then_prompt': 'Continue the check'}
                dependency = self.job('Dependency') if trigger == 'job' else None
                if trigger == 'button':
                    params['buttons'] = [[{'text': 'A', 'callback_data': 'A'}]]
                if trigger == 'secret':
                    params['key'] = 'secret:SAMPLE_TOKEN'
                if dependency:
                    params['job'] = dependency['id']
                wait = self.wait(task, kind='job' if dependency else 'owner', **params)
                await self.deliver_all()
                ask = self.outbox(wait['ask_outbox']) if wait['ask_outbox'] else None
                with patch('coordinator.control_api.op_workers_spawn',
                           side_effect=RuntimeError('Fixture worker start failed')):
                    if trigger == 'reply':
                        self.assertEqual(self.store.accept(owner_update(1100 + offset, 'Use A',
                                                                       reply=ask['telegram_message'])), 'queued')
                    elif trigger == 'button':
                        self.assertEqual(self.store.accept(self.callback(wait, ask['telegram_message'],
                                                                        update_id=1100 + offset)), 'wait_choice_received')
                    elif trigger == 'secret':
                        with self.store.transaction():
                            envelope = self.store.db.execute('''INSERT INTO envelopes
                                (name,reason,consumer,topic,state,created,expires,updated)
                                VALUES ('SAMPLE_TOKEN','Test card','test',?,'filled',1,2,1)''', (TOPIC,)).lastrowid
                            self.store.message_secret_filled(TOPIC, envelope, 'The sample card is filled.')
                    elif trigger == 'job':
                        self.ok('tasks.update', task=dependency['id'], status='done')
                    else:
                        answer = self.store.message_save(TOPIC, 'owner', 'Use A')
                        self.ok('pending.fire', wait=wait['id'], message=answer['id'])
                self.assertEqual((self.store.wait_get(wait['id'])['state'],
                                  self.store.task_get(task['id'])['status']), ('fired', 'open'))
                problems = [dict(row) for row in self.store.db.execute(
                    'SELECT * FROM problems WHERE task=?', (task['id'],))]
                self.assertEqual(len(problems), 1)
                self.assertEqual((problems[0]['area'], problems[0]['code'], problems[0]['topic']),
                                 ('waits', 'worker-start-failed', TOPIC))
                self.assertIn('RuntimeError', problems[0]['detail'])
                self.assertIn('Decide the next step.', self.callbacks()[-1]['text'])

    async def test_n3_reply_and_button_hook_errors_preserve_intake_and_allow_retry(self):
        for offset, trigger, code, refused in (
                (0, 'reply', 'reply-trigger-failed', 'queued'),
                (1, 'button', 'button-trigger-failed', 'stale_callback')):
            with self.subTest(trigger=trigger):
                task = self.job('Boundary ' + trigger)
                wait = self.wait(task, buttons=[[{'text': 'A', 'callback_data': 'A'}]])
                await self.deliver_all()
                ask = self.outbox(wait['ask_outbox'])['telegram_message']
                update_id = 1300 + offset
                update = (owner_update(update_id, 'Use A', reply=ask) if trigger == 'reply' else
                          self.callback(wait, ask, update_id=update_id))
                before = len(self.callbacks())
                with patch.object(self.store, 'wait_fire', side_effect=RuntimeError('Fixture trigger failed')):
                    self.assertEqual(self.store.accept(update), refused)
                self.assertEqual(self.store.db.execute('SELECT id FROM updates WHERE id=?',
                                                     (update_id,)).fetchone()[0], update_id)
                self.assertEqual((self.store.wait_get(wait['id'])['state'],
                                  self.store.task_get(task['id'])['status']), ('armed', 'waiting'))
                self.assertEqual(len(self.callbacks()), before)
                problem = self.store.db.execute('SELECT area,code,detail FROM problems WHERE code=?', (code,)).fetchone()
                self.assertEqual(tuple(problem), ('waits', code, 'RuntimeError'))
                retry_id = 1310 + offset
                retry = (owner_update(retry_id, 'Use A', reply=ask) if trigger == 'reply' else
                         self.callback(wait, ask, update_id=retry_id))
                self.assertEqual(self.store.accept(retry), 'queued' if trigger == 'reply' else 'wait_choice_received')
                self.assertEqual((self.store.wait_get(wait['id'])['state'],
                                  self.store.task_get(task['id'])['status']), ('fired', 'open'))
                self.assertEqual(len(self.store.task_iterations(task['id'])), 2)

    async def test_n4_declared_worker_error_does_not_block_other_due_waits(self):
        first, second = self.job('First'), self.job('Second')
        declared = self.wait(first, kind='time', ask='Run the planned check', at='1970-01-01T00:33:20Z',
                             then_provider='codex', then_prompt='Check')
        plain = self.wait(second, kind='time', ask='Plain reminder', at='1970-01-01T00:33:21Z')
        with patch('coordinator.control_api.op_workers_spawn', side_effect=OSError(28, 'No space left on device')):
            await self.service.waits_once()
        self.assertEqual([self.store.wait_get(item['id'])['state'] for item in (declared, plain)], ['fired', 'fired'])
        self.assertEqual([self.store.task_get(task['id'])['status'] for task in (first, second)], ['open', 'open'])
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM workers').fetchone()[0], 0)
        self.assertEqual(len(self.callbacks(TOPIC)), 2)
        problem = dict(self.store.db.execute('SELECT * FROM problems').fetchone())
        self.assertEqual((problem['area'], problem['code'], problem['task']),
                         ('waits', 'worker-start-failed', first['id']))
        self.assertIn('OSError', problem['detail'])

    async def test_n4_failed_due_wait_does_not_block_other_waits(self):
        first, second = self.job('First'), self.job('Second')
        bad = self.wait(first, kind='time', ask='Run the planned check', at='1970-01-01T00:33:20Z',
                        then_provider='codex', then_prompt='Check')
        good = self.wait(second, kind='time', ask='Plain reminder', at='1970-01-01T00:33:21Z')
        original = self.store.wait_fire

        def fire(identifier, **params):
            if identifier == bad['id']:
                raise RuntimeError('Fixture wait write failed')
            return original(identifier, **params)

        with patch.object(self.store, 'wait_fire', side_effect=fire):
            await self.service.waits_once()
        self.assertEqual((self.store.wait_get(good['id'])['state'],
                          self.store.task_get(second['id'])['status']), ('fired', 'open'))
        self.assertEqual(self.store.wait_get(bad['id'])['state'], 'armed')
        self.assertEqual([row['topic'] for row in self.callbacks()], [TOPIC])
        problems = [dict(row) for row in self.store.db.execute('SELECT * FROM problems')]
        self.assertEqual(len(problems), 1)
        self.assertEqual((problems[0]['area'], problems[0]['code'], problems[0]['detail']),
                         ('waits', 'timer-trigger-failed', 'RuntimeError'))

    async def test_n5_spawn_releases_only_target_shared_link(self):
        other = self.other_topic()
        first, second = self.job(), self.job('Other job', other)
        wait = self.wait(first, key='owner:shared-account', ask='Choose the account')
        self.wait(second, key='owner:shared-account', ask='Choose the account')
        unrelated = self.store.message_save(TOPIC, 'owner', 'Go ahead with the docs fix', telegram_message=940)
        self.ok('workers.spawn', task=first['id'], provider='codex', prompt='Fix the docs', message=unrelated['id'])
        self.assertEqual((self.store.task_get(first['id'])['status'],
                          self.store.task_get(first['id'])['iteration']), ('open', 1))
        self.assertEqual((self.store.task_get(second['id'])['status'],
                          self.store.task_get(second['id'])['iteration']), ('waiting', 0))
        remaining = self.store.wait_get(wait['id'])
        self.assertEqual(remaining['state'], 'armed')
        self.assertEqual([link['task'] for link in remaining['jobs']], [second['id']])
        self.assertEqual(self.callbacks(other), [])
        answer = self.store.message_save(other, 'owner', 'Use the other account')
        self.ok('pending.fire', wait=wait['id'], message=answer['id'])
        self.assertEqual((self.store.task_get(second['id'])['status'],
                          self.store.task_iterations(second['id'])[-1]['origin']), ('open', answer['id']))

    async def test_n6_repeated_cancel_wakes_once(self):
        task = self.job()
        wait = self.wait(task)
        self.ok('pending.cancel', wait=wait['id'], reason='Not needed')
        self.ok('pending.cancel', wait=wait['id'], reason='Not needed')
        await self.service.waits_once()
        self.assertEqual((self.store.task_get(task['id'])['status'],
                          self.store.task_get(task['id'])['iteration']), ('open', 1))
        self.assertEqual(len(self.store.task_iterations(task['id'])), 2)
        self.assertEqual(len(self.callbacks(TOPIC)), 1)
        self.assertEqual(self.store.task_iterations(task['id'])[-1]['origin'], self.callbacks()[0]['id'])
        self.assertEqual((self.store.wait_get(wait['id'])['state'], self.store.wait_get(wait['id'])['reason']),
                         ('cancelled', 'Not needed'))

    async def test_n7_closed_job_refuses_blocking_wait_and_allows_watch(self):
        for status in ('done', 'dropped'):
            with self.subTest(status=status):
                task = self.job('Closed ' + status)
                self.ok('tasks.update', task=task['id'], status=status)
                previous = self.store.task_iterations(task['id'])[-1]
                result = self.call('tasks.wait', task=task['id'], kind='time', ask='Try later',
                                   at='1970-01-01T00:33:20Z')
                self.assertFalse(result.ok, result.text)
                self.assertEqual(self.store.task_get(task['id'])['status'], status)
                self.assertEqual(self.store.task_iterations(task['id'])[-1], previous)
                watch = self.wait(task, kind='time', ask='Watch the result', at='1970-01-01T00:33:20Z', watch=True)
                self.assertEqual((self.store.task_get(task['id'])['status'], watch['jobs'][0]['blocking']), (status, 0))
                await self.service.waits_once()
                self.assertEqual((self.store.wait_get(watch['id'])['state'],
                                  self.store.task_get(task['id'])['status']), ('fired', 'open'))
                self.assertEqual(self.store.task_iterations(task['id'])[0], previous)

    async def test_n8_slash_reply_keeps_wait_armed(self):
        task = self.job()
        wait = self.wait(task)
        await self.deliver_all()
        ask = self.outbox(wait['ask_outbox'])['telegram_message']
        self.assertEqual(self.store.accept(owner_update(970, '/waiting', reply=ask)), 'control')
        self.assertEqual((self.store.wait_get(wait['id'])['state'],
                          self.store.task_get(task['id'])['status']), ('armed', 'waiting'))
        self.assertEqual(self.store.db.execute('SELECT text FROM outbox WHERE reply_to=970').fetchone()[0],
                         'Waiting on you\nTorii · Build: Pick A or B')
        self.assertEqual(self.store.accept(owner_update(971, 'Use A', reply=ask)), 'queued')
        self.assertEqual(self.store.wait_get(wait['id'])['state'], 'fired')
        self.assertEqual(len(self.store.task_iterations(task['id'])), 2)

    async def test_n9_header_is_bounded_and_single_line(self):
        tricky = 'Use "fast" path?\nSee [A] or [B] ] done \\ end message=7 kind=owner'
        self.wait(self.job(), ask=tricky)
        self.wait(self.job('Two'), ask='Second ask ' + 'y' * 3900)
        self.wait(self.job('Three'), ask='Third ask ' + 'z' * 3900)
        message = self.store.message_save(TOPIC, 'owner', 'Hello', telegram_message=950)
        prepared = await self.service._prepare_message(message)
        tag = prepared[:prepared.index('] Owner message:') + 1]
        self.assertIn('waiting=', tag)
        header, unused = json.JSONDecoder().raw_decode(tag.split(' waiting=', 1)[1])
        self.assertIn('Use "fast" path?', header)
        self.assertLessEqual(len(tag), 1024)
        self.assertNotIn('\n', tag)
        self.assertNotIn('\r', tag)
        self.assertEqual(re.findall(r'\bmessage=(\d+) kind=', prepared), [str(message['id'])])

    async def test_n10_button_clears_keyboard_and_explains_repeat(self):
        self.use_telegram_sender()
        task = self.job()
        wait = self.wait(task, buttons=[[{'text': 'Ship', 'callback_data': 'x'},
                                        {'text': 'Hold', 'callback_data': 'y'}]])
        await self.deliver_all()
        ask = self.outbox(wait['ask_outbox'])['telegram_message']
        first = self.store.accept(self.callback(wait, ask, update_id=980, index=1))
        await self.service.answer_callback('980', first)
        await self.deliver_all()
        again = self.store.accept(self.callback(wait, ask, update_id=981, index=1))
        await self.service.answer_callback('981', again)
        self.assertEqual((first, again), ('wait_choice_received', 'wait_already_answered'))
        self.assertEqual([params['text'] for method, params in self.telegram.calls if method == 'answerCallbackQuery'],
                         ['Choice received.', 'This question was already answered.'])
        edits = [params for method, params in self.telegram.calls
                 if method == 'editMessageText' and params['message_id'] == ask]
        self.assertEqual(len(edits), 1)
        self.assertEqual(edits[0]['reply_markup'], {'inline_keyboard': []})
        self.assertEqual(self.store.wait_get(wait['id'])['outcome'], 'choice:Hold')
        self.assertEqual(len(self.store.task_iterations(task['id'])), 2)

    async def test_n10_owner_list_uses_names_and_plain_held_copy_text(self):
        self.wait(self.job())
        held = self.job('Review the saved result')
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET copy='held',copy_reason='Save verification failed' WHERE id=?",
                                  (held['id'],))
        listed = self.ok('pending.list', owner=True)
        self.assertIn('Torii · Build: Pick A or B', listed.text)
        self.assertIn('Review the saved result', listed.text)
        self.assertIn('Save verification failed', listed.text)
        self.assertNotRegex(listed.text, r'\bW\d+\b')
        self.assertNotIn('copy held', listed.text)

    async def test_n11_chat_close_snapshot_and_round_include_waiting_jobs(self):
        other = self.other_topic()
        opened, waiting = self.job('Open job', other), self.job('Waiting job', other)
        self.wait(waiting)
        with patch('coordinator.topics._git', return_value='false'):
            snapshot = topics.snapshot(self.store, other)
        self.assertEqual([(job['id'], job['title']) for job in snapshot['jobs']],
                         [(opened['id'], 'Open job'), (waiting['id'], 'Waiting job')])
        closed = await self.close_topic(other)
        self.assertEqual([(job['id'], job['title']) for job in closed['dropped_jobs']],
                         [(opened['id'], 'Open job'), (waiting['id'], 'Waiting job')])
        self.assertEqual([self.store.task_get(task['id'])['status'] for task in (opened, waiting)],
                         ['dropped', 'dropped'])
        self.assertEqual(self.store.task_iterations(waiting['id'])[-1]['outcome'], 'dropped')
        self.assertEqual(self.store.task_iterations(waiting['id'])[-1]['reason'], 'chat closed')


class WaitSwitchTests(WaitReviewSupport, unittest.IsolatedAsyncioTestCase):
    async def test_waits_are_off_by_default_and_public_calls_refuse_plainly(self):
        task = self.job()
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET copy='held',copy_reason='Save verification failed' WHERE id=?",
                                  (task['id'],))
        refused = self.call('tasks.wait', task=task['id'], kind='owner', ask='Choose a plan')
        fire = self.call('pending.fire', wait=1, message=self.origin['id'])
        self.assertEqual((refused.ok, refused.text), (False, "Waits aren't switched on yet."))
        self.assertEqual((fire.ok, fire.text), (False, "Waits aren't switched on yet."))
        for owner in (False, True):
            listed = self.ok('pending.list', owner=owner)
            self.assertEqual((listed.text, listed.data), ('Nothing is waiting on you.', {'waits': [], 'held': []}))
        response = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
            'name': 'tasks_wait', 'arguments': {'task': task['id'], 'kind': 'owner', 'ask': 'Choose a plan'}}}, topic=TOPIC)
        self.assertTrue(response['isError'])
        self.assertEqual(response['content'][0]['text'], "Waits aren't switched on yet.\nnull")
        self.assertEqual(self.store.task_get(task['id'])['status'], 'open')
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM waits').fetchone()[0], 0)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], 0)

    async def test_off_switch_keeps_existing_hooks_card_header_and_timer_inert(self):
        self.store.put('waits_enabled', True)
        owner_task, timer_task, secret_task = self.job(), self.job('Timer'), self.job('Secret')
        dependency, dependent = self.job('Dependency'), self.job('Dependent')
        owner = self.wait(owner_task, buttons=[[{'text': 'A', 'callback_data': 'A'}]])
        timer = self.wait(timer_task, kind='time', ask='Retry', at='1970-01-01T00:33:20Z')
        secret = self.wait(secret_task, key='secret:SAMPLE_TOKEN', ask='Fill the sample card')
        job_wait = self.wait(dependent, kind='job', job=dependency['id'], ask='Continue after dependency')
        await self.deliver_all()
        ask = self.outbox(owner['ask_outbox'])['telegram_message']
        self.store.put('waits_enabled', False)
        before = [tuple(row) for row in self.store.db.execute('SELECT * FROM outbox ORDER BY id')]
        self.assertEqual(self.store.accept(owner_update(1200, 'Use A', reply=ask)), 'queued')
        self.assertEqual(self.store.accept(self.callback(owner, ask, update_id=1201)), 'stale_callback')
        with self.store.transaction():
            envelope = self.store.db.execute('''INSERT INTO envelopes
                (name,reason,consumer,topic,state,created,expires,updated)
                VALUES ('SAMPLE_TOKEN','Test card','test',?,'filled',1,2,1)''', (TOPIC,)).lastrowid
            self.store.message_secret_filled(TOPIC, envelope, 'The sample card is filled.')
        await self.service.waits_once()
        self.ok('tasks.update', task=dependency['id'], status='done')
        self.store.refresh_owner_waiting()
        self.assertEqual([self.store.wait_get(item['id'])['state'] for item in (owner, timer, secret, job_wait)],
                         ['armed', 'armed', 'armed', 'armed'])
        self.assertEqual([self.store.task_get(task['id'])['status']
                          for task in (owner_task, timer_task, secret_task, dependent)],
                         ['waiting', 'waiting', 'waiting', 'waiting'])
        self.assertEqual(self.callbacks(), [])
        self.assertEqual([tuple(row) for row in self.store.db.execute('SELECT * FROM outbox ORDER BY id')], before)
        self.assertEqual(owner_header(self.store, TOPIC), '')
        self.assertEqual(self.ok('pending.list', owner=True).text, 'Nothing is waiting on you.')
        self.assertEqual(self.store.accept(owner_update(1202, '/waiting')), 'control')
        self.assertEqual(self.store.db.execute('SELECT text FROM outbox WHERE reply_to=1202').fetchone()[0],
                         'Nothing is waiting on you.')

    async def test_command_menu_follows_local_wait_switch(self):
        self.service.poll_succeeded = True
        passes = []

        def sleep(seconds):
            passes.append(seconds)
            if len(passes) == 1:
                self.store.put('waits_enabled', True)
            elif len(passes) == 2:
                self.store.put('waits_enabled', False)
            else:
                raise asyncio.CancelledError()

        with patch('coordinator.service.asyncio.sleep', side_effect=sleep):
            with self.assertRaises(asyncio.CancelledError):
                await self.service.command_menu()
        menus = [params['commands'] for method, params in self.telegram.calls if method == 'setMyCommands']
        self.assertEqual(len(menus), 3)
        self.assertEqual([any(item['command'] == 'waiting' for item in menu) for menu in menus], [False, True, False])
        self.assertEqual(next(item['description'] for item in menus[1] if item['command'] == 'waiting'),
                         'What is waiting on you')
        self.assertNotIn('waiting', [item['command'] for item in onboarding.command_menu(self.store)])

    async def test_local_cli_waits_set_enabled_toggles_and_other_transports_refuse(self):
        from coordinator.__main__ import ctl
        for enabled, expected in (('true', 'Waits are switched on.'), ('false', 'Waits are switched off.')):
            with self.subTest(enabled=enabled):
                output = StringIO()
                args = SimpleNamespace(operation='call', op='waits.set_enabled', topic=None,
                                       pairs=['enabled=' + enabled])
                with patch('coordinator.__main__.calling_app', return_value='Fixture program'), redirect_stdout(output):
                    status = ctl(args, self.store)
                result = json.loads(output.getvalue())
                self.assertEqual((status, result['ok'], result['text']), (0, True, expected))
                self.assertEqual(self.store.get('waits_enabled'), enabled == 'true')
                self.assertEqual(tuple(self.store.db.execute(
                    'SELECT op,state FROM cli_calls ORDER BY id DESC LIMIT 1').fetchone()),
                    ('waits.set_enabled', 'done'))
        for source in ('mcp', 'telegram'):
            with self.subTest(source=source):
                result = control_api.call(self.store, 'waits.set_enabled', {'enabled': True}, source=source)
                self.assertFalse(result.ok, result.text)
                self.assertEqual(self.store.get('waits_enabled'), False)


if __name__ == '__main__':
    unittest.main()
