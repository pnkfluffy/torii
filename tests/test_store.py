import json
import sqlite3
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from coordinator.store import Store, split_message
from coordinator.formatting import markdown_to_html
from coordinator.controls import handle_control


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
            self.store.db.execute('ALTER TABLE envelope_effects DROP COLUMN thread')
            self.store.db.execute('ALTER TABLE envelopes DROP COLUMN arm_thread')
        topics = self.store.topics()
        self.store.close()
        self.store = Store(self.root)
        self.assertEqual(self.store.get('mode'), 'group')
        self.assertEqual(self.store.chat(), -10042)
        self.assertEqual(self.store.topics(), topics)
        self.assertEqual([self.store.accept(item) for item in recorded], expected)
        self.assertIn('thread', self.store._columns('envelope_effects'))
        self.assertIn('arm_thread', self.store._columns('envelopes'))
        self.store.close()
        self.store = Store(self.root)
        self.assertEqual(self.store.get('mode'), 'group')
        self.assertEqual(len(self.store.topics()), 2)

    def test_unpaired_migration_keeps_mode_absent(self):
        with tempfile.TemporaryDirectory() as temp:
            fresh = Store(Path(temp))
            self.assertIsNone(fresh.get('mode'))
            fresh.close()

    def test_reopen_keeps_the_current_schema(self):
        version = self.store.db.execute('PRAGMA schema_version').fetchone()[0]
        self.store.close()
        self.store = Store(self.root)
        self.assertEqual(self.store.db.execute('PRAGMA schema_version').fetchone()[0], version)

    def test_migrates_message_tool_count_without_changing_handoff(self):
        from coordinator.reactions import handed_off, parent_tools
        message = self.store.message_save('-10042:4', 'owner', 'Look this up', telegram_message=501)['id']
        with self.store.db:
            handed_off(self.store, message)
            self.store.db.execute('ALTER TABLE messages DROP COLUMN turn_tools')
        before = dict(self.store.db.execute('SELECT * FROM messages WHERE id=?', (message,)).fetchone())
        self.store.close()
        self.store = Store(self.root)
        after = dict(self.store.db.execute('SELECT * FROM messages WHERE id=?', (message,)).fetchone())
        self.assertEqual(after.pop('turn_tools'), 0)
        self.assertEqual(after, before)
        with self.store.db:
            parent_tools(self.store, [message], [{'type': 'tool_use', 'name': 'Read'}])
            parent_tools(self.store, [message], [{'type': 'tool_use', 'name': 'WebSearch'}])
        self.assertEqual(self.store.db.execute('SELECT reaction_desired FROM messages WHERE id=?',
                                              (message,)).fetchone()[0], '👀')

    def test_reopen_replaces_old_worker_message_indexes(self):
        with self.store.db:
            worker = self.store.db.execute('''INSERT INTO workers
                (topic,provider,prompt,created,updated) VALUES ('-10042:4','codex','work',1,1)''').lastrowid
        for definition in ('UNIQUE INDEX messages_workers ON messages(source_worker)',
                           'INDEX messages_workers ON messages(topic)',
                           'INDEX messages_workers ON messages(source_worker) WHERE source_worker IS NULL'):
            with self.subTest(definition=definition):
                with self.store.db:
                    self.store.db.execute('DROP INDEX messages_workers')
                    self.store.db.execute('CREATE ' + definition)
                self.store.close()
                self.store = Store(self.root)
                with self.store.db:
                    for _ in range(2):
                        self.store.db.execute('''INSERT INTO messages
                            (topic,kind,text,source_worker,created,updated)
                            VALUES ('-10042:4','worker_result','saved',?,1,1)''', (worker,))
                self.assertEqual(self.store.db.execute('SELECT count(*) FROM messages').fetchone()[0], 2)
                self.assertEqual(self.store.db.execute(
                    "SELECT sql FROM sqlite_master WHERE name='messages_workers'").fetchone()[0],
                    'CREATE INDEX messages_workers ON messages(source_worker)')
                with self.store.db:
                    self.store.db.execute('DELETE FROM messages')

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

    def test_closing_task_interrupts_waiting_workers(self):
        task = self.store.task_create('-10042:4', 'Waiting')
        with self.store.db:
            for status in ('needs_input', 'waiting_for_quota'):
                self.store.db.execute("INSERT INTO workers (task,topic,provider,prompt,status,created,updated) "
                                      "VALUES (?, '-10042:4', 'claude', 'Question', ?, 1, 1)",
                                      (task['id'], status))
        self.store.task_update(task['id'], status='done')
        self.assertEqual([row[0] for row in self.store.db.execute('SELECT status FROM workers ORDER BY id')],
                         ['interrupted', 'interrupted'])

    def test_startup_reconciles_closed_and_superseded_waiters(self):
        closed = self.store.task_create('-10042:4', 'Closed')
        open_task = self.store.task_create('-10042:4', 'Superseded')
        with self.store.db:
            for task in (closed, open_task):
                self.store.db.execute("INSERT INTO workers (task,topic,provider,prompt,status,created,updated) "
                                      "VALUES (?, '-10042:4', 'claude', 'Question', 'needs_input', 1, 1)",
                                      (task['id'],))
            self.store.db.execute("INSERT INTO workers (task,topic,provider,prompt,status,created,updated) "
                                  "VALUES (?, '-10042:4', 'claude', 'Follow up', 'done', 2, 2)",
                                  (open_task['id'],))
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET status='dropped' WHERE id=?", (closed['id'],))
        self.store.close()
        self.store = Store(self.root)
        self.assertEqual([row[0] for row in self.store.db.execute(
            'SELECT status FROM workers ORDER BY id')], ['interrupted', 'interrupted', 'done'])
        self.store.close()
        self.store = Store(self.root)
        self.assertEqual([row[0] for row in self.store.db.execute(
            'SELECT status FROM workers ORDER BY id')], ['interrupted', 'interrupted', 'done'])

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

    def test_late_question_after_task_closes_is_interrupted(self):
        for closed_status in ('done', 'dropped'):
            with self.subTest(closed_status=closed_status):
                task = self.store.task_create('-10042:4', 'Task ' + closed_status)
                with self.store.db:
                    worker = self.store.db.execute('''INSERT INTO workers
                        (task,topic,provider,prompt,status,created,updated)
                        VALUES (?,'-10042:4','codex','work','running',1,1)''',
                        (task['id'],)).lastrowid
                self.store.task_update(task['id'], status=closed_status)
                message = self.store.worker_complete(worker, {
                    'success': True, 'needs_input': True, 'text': 'Which schema?'}, 'needs_input')
                payload = json.loads(message['text'])
                saved = self.store.db.execute('SELECT status,result FROM workers WHERE id=?', (worker,)).fetchone()
                self.assertEqual(saved['status'], 'interrupted')
                self.assertEqual(json.loads(saved['result']), payload)
                self.assertEqual(payload['status'], 'interrupted')
                self.assertFalse(payload['needs_input'])
                self.assertEqual(payload['text'], 'Which schema?')

    def test_late_question_after_replacement_is_interrupted(self):
        task = self.store.task_create('-10042:4', 'Task')
        with self.store.db:
            worker = self.store.db.execute('''INSERT INTO workers
                (task,topic,provider,prompt,status,created,updated)
                VALUES (?,'-10042:4','codex','work','running',1,1)''',
                (task['id'],)).lastrowid
            replacement = self.store.db.execute('''INSERT INTO workers
                (task,topic,provider,prompt,status,created,updated)
                VALUES (?,'-10042:4','codex','replacement','queued',2,2)''',
                (task['id'],)).lastrowid
        message = self.store.worker_complete(worker, {
            'success': True, 'needs_input': True, 'text': 'Which schema?'}, 'needs_input')
        payload = json.loads(message['text'])
        self.assertEqual(self.store.db.execute('SELECT status FROM workers WHERE id=?',
                                               (worker,)).fetchone()[0], 'interrupted')
        self.assertEqual(self.store.db.execute('SELECT status FROM workers WHERE id=?',
                                               (replacement,)).fetchone()[0], 'queued')
        self.assertEqual(payload['status'], 'interrupted')
        self.assertFalse(payload['needs_input'])
        self.assertEqual(payload['text'], 'Which schema?')

    def test_current_worker_question_still_needs_input(self):
        task = self.store.task_create('-10042:4', 'Task')
        with self.store.db:
            worker = self.store.db.execute('''INSERT INTO workers
                (task,topic,provider,prompt,status,created,updated)
                VALUES (?,'-10042:4','codex','work','running',1,1)''',
                (task['id'],)).lastrowid
        message = self.store.worker_complete(worker, {
            'success': True, 'needs_input': True, 'text': 'Which schema?'}, 'needs_input')
        payload = json.loads(message['text'])
        self.assertEqual(self.store.db.execute('SELECT status FROM workers WHERE id=?',
                                               (worker,)).fetchone()[0], 'needs_input')
        self.assertEqual(payload['status'], 'needs_input')
        self.assertTrue(payload['needs_input'])
        self.assertEqual(payload['text'], 'Which schema?')

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
            self.store.envelope_event(envelope, 'GITHUB_TOKEN', 'reject', reason='too_short')
        row = self.store.db.execute('SELECT envelope,name,event,reason FROM envelope_events').fetchone()
        self.assertEqual(tuple(row), (envelope, 'GITHUB_TOKEN', 'reject', 'too_short'))


OLD_SCHEMA = """
CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE topics (id TEXT PRIMARY KEY, chat INTEGER NOT NULL, thread INTEGER NOT NULL,
 name TEXT NOT NULL, cwd TEXT NOT NULL, provider TEXT, session TEXT,
 enabled INTEGER NOT NULL DEFAULT 0, waiting_job INTEGER, source_pid INTEGER);
CREATE TABLE updates (id INTEGER PRIMARY KEY);
CREATE TABLE jobs (id INTEGER PRIMARY KEY, topic TEXT NOT NULL REFERENCES topics(id),
 message INTEGER NOT NULL, prompt TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'queued',
 result TEXT, pid INTEGER, created REAL NOT NULL, updated REAL NOT NULL,
 reply_to INTEGER, input_version INTEGER NOT NULL DEFAULT 0,
 failure_code TEXT, failure_detail TEXT, number INTEGER);
CREATE TABLE workers (id INTEGER PRIMARY KEY, job INTEGER NOT NULL REFERENCES jobs(id),
 topic TEXT NOT NULL REFERENCES topics(id), provider TEXT NOT NULL, prompt TEXT NOT NULL,
 work_kind TEXT NOT NULL, role TEXT NOT NULL, predecessor INTEGER REFERENCES workers(id),
 session TEXT, cwd TEXT, workspace TEXT, fresh INTEGER NOT NULL DEFAULT 1,
 status TEXT NOT NULL DEFAULT 'queued', pid INTEGER, result TEXT,
 created REAL NOT NULL, updated REAL NOT NULL);
CREATE TABLE outbox (id INTEGER PRIMARY KEY, topic TEXT NOT NULL REFERENCES topics(id),
 job INTEGER, kind TEXT NOT NULL, text TEXT NOT NULL, reply_to INTEGER,
 delivered INTEGER NOT NULL DEFAULT 0, telegram_message INTEGER,
 attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL NOT NULL DEFAULT 0,
 reply_markup TEXT);
CREATE TABLE attachments (id INTEGER PRIMARY KEY, topic TEXT NOT NULL REFERENCES topics(id),
 message INTEGER NOT NULL, job INTEGER REFERENCES jobs(id), file_id TEXT NOT NULL,
 mime TEXT NOT NULL, size INTEGER, group_id TEXT, path TEXT, created REAL NOT NULL,
 UNIQUE(topic,message));
CREATE TABLE audit (id INTEGER PRIMARY KEY, job INTEGER, kind TEXT NOT NULL,
 data TEXT NOT NULL, created REAL NOT NULL);
CREATE TABLE job_questions (job INTEGER PRIMARY KEY REFERENCES jobs(id), first_outbox INTEGER NOT NULL);
CREATE TABLE worker_controls (id INTEGER PRIMARY KEY, job INTEGER NOT NULL REFERENCES jobs(id),
 worker INTEGER NOT NULL REFERENCES workers(id), prompt TEXT NOT NULL,
 state TEXT NOT NULL DEFAULT 'queued', created REAL NOT NULL, updated REAL NOT NULL);
CREATE TABLE control_calls (id INTEGER PRIMARY KEY, job INTEGER REFERENCES jobs(id),
 topic TEXT REFERENCES topics(id), source TEXT NOT NULL, op TEXT NOT NULL,
 params TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'done', result TEXT,
 created REAL NOT NULL, updated REAL NOT NULL);
CREATE TABLE task_reactions (job INTEGER PRIMARY KEY REFERENCES jobs(id),
 message INTEGER NOT NULL, state TEXT NOT NULL DEFAULT '',
 retry_at REAL NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
 disabled INTEGER NOT NULL DEFAULT 0);
CREATE TABLE pending_replies (message INTEGER NOT NULL, topic TEXT NOT NULL REFERENCES topics(id),
 reply INTEGER NOT NULL, text TEXT NOT NULL, created REAL NOT NULL,
 PRIMARY KEY(topic,message));
"""


class MigrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = sqlite3.connect(self.root / 'state.sqlite')
        self.db.executescript(OLD_SCHEMA)
        self.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('1:2',1,2,'test','/project')")
        self.db.execute("INSERT INTO settings VALUES ('offset','12')")
        self.db.execute('INSERT INTO updates VALUES (11)')
        for job_id, status, number in ((1, 'queued', 4), (2, 'running', 5),
                                       (3, 'working', 6), (4, 'held', 7),
                                       (5, 'stopped', 8), (6, 'done', 9)):
            self.db.execute('''INSERT INTO jobs(id,topic,message,prompt,status,number,created,updated)
                VALUES (?,'1:2',?,'First line\nsecond line',?,?,10,11)''',
                (job_id, job_id, status, number))
            self.db.execute('''INSERT INTO workers(id,job,topic,provider,prompt,work_kind,role,created,updated)
                VALUES (?,?,'1:2','claude','work','normal','work',10,11)''', (job_id, job_id))
        self.db.execute("INSERT INTO outbox(id,topic,job,kind,text,delivered,telegram_message,attempts,next_attempt) VALUES (9,'1:2',6,'report','saved reply',1,40,2,100)")
        self.db.execute("INSERT INTO attachments(topic,message,job,file_id,mime,path,created) VALUES ('1:2',8,6,'file','image/png','/saved/image',10)")
        self.db.commit()
        self.db.close()

    def tearDown(self):
        self.temp.cleanup()

    def test_migrates_open_jobs_and_preserves_transport_rows_on_repeat_open(self):
        for attempt in range(2):
            store = Store(self.root)
            try:
                tasks = store.tasks_list()
                self.assertEqual([(task['number'], task['title'], task['notes']) for task in tasks],
                                 [(4, 'First line', 'migrated from job 4'),
                                  (5, 'First line', 'migrated from job 5'),
                                  (6, 'First line', 'migrated from job 6'),
                                  (7, 'First line', 'migrated from job 7'),
                                  (8, 'First line', 'migrated from job 8')])
                links = [tuple(row) for row in store.db.execute('SELECT id,task FROM workers ORDER BY id')]
                self.assertEqual(links, [(index, tasks[index - 1]['id']) for index in range(1, 6)] + [(6, None)])
                self.assertEqual(tuple(store.db.execute('SELECT text,delivered,telegram_message,attempts,next_attempt FROM outbox WHERE id=9').fetchone()),
                                 ('saved reply', 1, 40, 2, 100))
                self.assertIn('image', {row['name'] for row in store.db.execute('PRAGMA table_info(outbox)')})
                self.assertIsNone(store.db.execute('SELECT image FROM outbox WHERE id=9').fetchone()[0])
                self.assertEqual(tuple(store.db.execute('SELECT file_id,path FROM attachments WHERE message=8').fetchone()),
                                 ('file', '/saved/image'))
                self.assertEqual(store.get('offset'), 12)
                self.assertEqual(store.db.execute('SELECT id FROM updates').fetchone()[0], 11)
                self.assertEqual(store.db.execute('PRAGMA foreign_key_check').fetchall(), [])
                names = {row[0] for row in store.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                self.assertFalse(names.intersection(('jobs','audit','job_questions','worker_controls',
                                                     'control_calls','task_reactions','pending_replies')))
            finally:
                store.close()

    def test_v2_tasks_gain_secrets_column_and_keep_rows(self):
        other = self.root / 'v2'
        other.mkdir()
        with sqlite3.connect(other / 'state.sqlite') as db:
            db.execute("CREATE TABLE topics (id TEXT PRIMARY KEY, chat INTEGER NOT NULL, thread INTEGER NOT NULL, name TEXT NOT NULL, cwd TEXT NOT NULL)")
            db.execute('''CREATE TABLE tasks (id INTEGER PRIMARY KEY, topic TEXT NOT NULL REFERENCES topics(id),
                number INTEGER NOT NULL, title TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'open',
                worktree TEXT, notes TEXT, created REAL NOT NULL, updated REAL NOT NULL, UNIQUE(topic,number))''')
            db.execute("INSERT INTO topics VALUES ('1:2',1,2,'test','/project')")
            db.execute("INSERT INTO tasks(topic,number,title,created,updated) VALUES ('1:2',3,'kept',10,11)")
        store = Store(other)
        try:
            row = store.db.execute('SELECT number,title,secrets FROM tasks').fetchone()
            self.assertEqual(tuple(row), (3, 'kept', None))
        finally:
            store.close()

    def test_collision_rolls_back_copy_and_drop_together(self):
        self.db = sqlite3.connect(self.root / 'state.sqlite')
        self.db.execute('''CREATE TABLE tasks (id INTEGER PRIMARY KEY, topic TEXT NOT NULL REFERENCES topics(id),
            number INTEGER NOT NULL, title TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'open',
            worktree TEXT, notes TEXT, created REAL NOT NULL, updated REAL NOT NULL, UNIQUE(topic,number))''')
        self.db.execute("INSERT INTO tasks(topic,number,title,created,updated) VALUES ('1:2',5,'existing',10,11)")
        self.db.commit()
        self.db.close()
        with self.assertRaises(sqlite3.IntegrityError):
            Store(self.root)
        with sqlite3.connect(self.root / 'state.sqlite') as db:
            self.assertEqual(db.execute('SELECT count(*) FROM jobs').fetchone()[0], 6)
            self.assertEqual(db.execute('SELECT count(*) FROM tasks').fetchone()[0], 1)
            self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='messages'").fetchone())


class SplitMessageTests(unittest.TestCase):
    def test_short_text_is_one_message(self):
        self.assertEqual(split_message('x' * 4096), ['x' * 4096])

    def test_long_text_breaks_at_a_paragraph_before_a_word(self):
        text = 'a ' * 1500 + '\n\n' + 'b ' * 1500
        parts = split_message(text)
        self.assertEqual(len(parts), 2)
        self.assertTrue(parts[0].endswith('a'))
        self.assertTrue(parts[1].startswith('b'))

    def test_no_word_is_cut(self):
        words = ['word%d' % i for i in range(2000)]
        parts = split_message(' '.join(words))
        self.assertTrue(all(len(part) <= 4096 for part in parts))
        self.assertEqual(' '.join(parts).split(), words)

    def test_text_without_breaks_is_cut_at_the_limit(self):
        self.assertEqual([len(part) for part in split_message('x' * 5000)], [4096, 904])

    def test_fence_starts_in_next_part_when_it_can(self):
        text = 'a' * 3000 + '\n```python\n' + 'x ' * 1000 + '\n```'
        parts = split_message(text)
        self.assertEqual(parts[0], 'a' * 3000)
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
                self.assertTrue(all(len(markdown_to_html(row['text'])) <= 4096 for row in rows))
            finally:
                store.close()

    def test_split_rendered_parts_have_balanced_tags(self):
        with tempfile.TemporaryDirectory() as temp:
            store = Store(Path(temp))
            try:
                with store.db:
                    store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test','',1)")
                    store.enqueue_report('1:2', ('**bold & <x>**\n' * 250).strip())
                rows = store.db.execute('SELECT text FROM outbox ORDER BY id').fetchall()
                self.assertGreater(len(rows), 1)
                for row in rows:
                    rendered = markdown_to_html(row['text'])
                    self.assertLessEqual(len(rendered), 4096)
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
                    self.assertLessEqual(len(rendered), 4096)
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
                self.assertTrue(len(markdown_to_html(rows[0]['text'])) <= 1024)
            finally:
                store.close()
