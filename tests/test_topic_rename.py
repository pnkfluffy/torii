import json
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import control_api
from coordinator.telegram import TelegramError
from tests.test_service import ServiceTestsSupport, TOPIC


class TopicRenameTests(ServiceTestsSupport, unittest.IsolatedAsyncioTestCase):
    def rename(self, name, topic=TOPIC):
        with self.store.db:
            return control_api.call(self.store, 'topic.rename', {'name': name}, topic=topic)

    def request(self, result):
        return self.store.db.execute('SELECT * FROM service_requests WHERE id=?',
                                     (result.data['request'],)).fetchone()

    def edited(self, name, number=8):
        return {'update_id': number, 'message': {
            'message_id': number, 'from': {'id': 99}, 'message_thread_id': 4,
            'chat': {'id': -10042, 'type': 'supergroup'}, 'forum_topic_edited': {'name': name}}}

    async def test_rename_saves_only_after_telegram_accepts_and_reports_completion(self):
        result = self.rename('⛓️‍💥 Workspace')
        self.assertEqual((result.ok, result.state), (True, 'queued'))
        self.assertEqual(self.store.topic(TOPIC)['name'], 'Torii')

        async def accept(method, **params):
            self.assertEqual(self.store.topic(TOPIC)['name'], 'Torii')
            self.assertEqual(self.request(result)['state'], 'sending')
            return True

        with patch.object(self.telegram, 'call', AsyncMock(side_effect=accept)) as call:
            await self.service.controls_once()
        call.assert_awaited_once_with('editForumTopic', chat_id=-10042, message_thread_id=4,
                                      name='⛓️‍💥 Workspace')
        row = self.store.topic(TOPIC)
        self.assertEqual((row['name'], row['telegram_title']), ('⛓️‍💥 Workspace', '⛓️‍💥 Workspace'))
        self.assertEqual(self.request(result)['state'], 'done')
        text = json.loads(self.request(result)['result'])['text']
        self.assertIn('Telegram topic renamed', text)
        self.assertIn('⛓️‍💥 Workspace', text)
        self.assertEqual(self.store.db.execute('SELECT text FROM outbox ORDER BY id DESC').fetchone()[0], text)

    async def test_telegram_refusal_preserves_both_saved_titles_and_reports_reason(self):
        with self.store.db:
            self.store.db.execute('UPDATE topics SET telegram_title=? WHERE id=?', ('Torii', TOPIC))
        result = self.rename('Workspace')
        with patch.object(self.telegram, 'call', AsyncMock(
                side_effect=TelegramError(400, reason='topic_manage_required'))):
            await self.service.controls_once()
        row = self.store.topic(TOPIC)
        self.assertEqual((row['name'], row['telegram_title']), ('Torii', 'Torii'))
        self.assertEqual(self.request(result)['state'], 'refused')
        text = json.loads(self.request(result)['result'])['text']
        self.assertIn('can_manage_topics', text)
        self.assertIn('400', text)
        self.assertIn('unchanged', text)

    async def test_unknown_telegram_error_reports_code_without_changing_label(self):
        result = self.rename('Workspace')
        with patch.object(self.telegram, 'call', AsyncMock(side_effect=TelegramError(403))):
            await self.service.controls_once()
        self.assertEqual(self.store.topic(TOPIC)['name'], 'Torii')
        self.assertEqual(self.request(result)['state'], 'refused')
        text = json.loads(self.request(result)['result'])['text']
        self.assertIn('403', text)
        self.assertNotIn('  ', text)

    async def test_timeout_requeues_then_completes_the_rename_once(self):
        result = self.rename('Workspace')
        with patch.object(self.telegram, 'call', AsyncMock(side_effect=TelegramError(
                'network-or-invalid-response', reason='timeout'))):
            await self.service.controls_once()
        self.assertEqual(self.store.topic(TOPIC)['name'], 'Torii')
        self.assertEqual(self.request(result)['state'], 'queued')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 0)
        await self.service.controls_once()
        self.assertEqual(self.request(result)['state'], 'done')
        self.assertEqual(self.store.topic(TOPIC)['name'], 'Workspace')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 1)

    async def test_cross_topic_rename_reports_and_wakes_only_the_asking_chat(self):
        other = '-10042:9'
        with self.store.db:
            self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                                  (other, -10042, 9, 'Other', str(self.root)))
            result = control_api.call(self.store, 'topic.rename', {'topic': other, 'name': 'Renamed'}, topic=TOPIC)
        self.assertIn('here', result.text)
        await self.service.controls_once()
        self.assertEqual(self.store.topic(other)['name'], 'Renamed')
        text = json.loads(self.request(result)['result'])['text']
        self.assertEqual([tuple(row) for row in self.store.db.execute('SELECT topic,text FROM outbox')],
                         [(TOPIC, text)])
        self.assertEqual([tuple(row) for row in self.store.db.execute("SELECT topic,text FROM messages WHERE kind='event'")],
                         [(TOPIC, text)])

    async def test_rename_without_an_asking_chat_reports_to_the_renamed_topic(self):
        with self.store.db:
            result = control_api.call(self.store, 'topic.rename', {'topic': TOPIC, 'name': 'Renamed'})
        await self.service.controls_once()
        self.assertEqual(self.request(result)['state'], 'done')
        self.assertEqual([row['topic'] for row in self.store.db.execute('SELECT topic FROM outbox')], [TOPIC])

    async def test_restart_mid_send_requeues_then_reports_completion_once(self):
        result = self.rename('Workspace')
        self.store.service_request_settle(result.data['request'], 'sending')
        self.store.requests_uncertain()
        self.assertEqual(self.request(result)['state'], 'queued')
        self.assertEqual(self.store.topic(TOPIC)['name'], 'Torii')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 0)
        self.store.requests_uncertain()
        await self.service.controls_once()
        self.assertEqual(self.request(result)['state'], 'done')
        self.assertEqual(self.store.topic(TOPIC)['name'], 'Workspace')
        self.assertEqual(self.telegram.calls, [('editForumTopic', {
            'chat_id': -10042, 'message_thread_id': 4, 'name': 'Workspace'})])
        await self.service.controls_once()
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 1)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM messages WHERE kind='event'").fetchone()[0], 1)

    async def test_rate_limit_reports_the_wait_and_keeps_the_saved_label(self):
        for retry_after, hint in ((5, 'in 5 s'), (None, 'later')):
            with self.subTest(retry_after=retry_after):
                result = self.rename('Workspace')
                with patch.object(self.telegram, 'call', AsyncMock(side_effect=TelegramError(429, retry_after))):
                    await self.service.controls_once()
                self.assertEqual(self.request(result)['state'], 'refused')
                self.assertEqual(json.loads(self.request(result)['result'])['text'],
                                 f'Telegram rate-limited the rename; try again {hint}. The saved label is unchanged.')
                self.assertEqual(self.store.topic(TOPIC)['name'], 'Torii')

    async def test_server_error_requeues_and_an_already_applied_rename_succeeds(self):
        result = self.rename('Workspace')
        with patch.object(self.telegram, 'call', AsyncMock(side_effect=TelegramError(500))):
            await self.service.controls_once()
        self.assertEqual(self.request(result)['state'], 'queued')
        self.assertEqual(self.store.topic(TOPIC)['name'], 'Torii')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 0)
        with patch.object(self.telegram, 'call', AsyncMock(
                side_effect=TelegramError(400, reason='topic_not_modified'))):
            await self.service.controls_once()
        self.assertEqual(self.request(result)['state'], 'done')
        self.assertEqual(self.store.topic(TOPIC)['name'], 'Workspace')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 1)

    async def test_echoed_title_is_a_noop_and_does_not_queue_another_rename(self):
        result = self.rename('Workspace')
        await self.service.controls_once()
        self.assertEqual(self.request(result)['state'], 'done')
        with self.store.db:
            self.store.db.execute('CREATE TABLE title_writes (name TEXT)')
            self.store.db.execute('''CREATE TRIGGER record_title_write AFTER UPDATE OF name,telegram_title
                ON topics BEGIN INSERT INTO title_writes VALUES (NEW.name); END''')
        calls = list(self.telegram.calls)
        outbox = [dict(row) for row in self.store.db.execute('SELECT * FROM outbox')]
        self.assertEqual(self.store.accept(self.edited('Workspace')), 'service_event')
        await self.service.controls_once()
        self.assertEqual(self.store.topic(TOPIC)['name'], 'Workspace')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM title_writes').fetchone()[0], 0)
        self.assertEqual(self.telegram.calls, calls)
        self.assertEqual([dict(row) for row in self.store.db.execute('SELECT * FROM outbox')], outbox)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0], 1)

    async def test_telegram_side_rename_still_updates_the_label_without_an_api_call(self):
        self.assertEqual(self.store.accept(self.edited('Telegram title')), 'service_event')
        await self.service.controls_once()
        row = self.store.topic(TOPIC)
        self.assertEqual((row['name'], row['telegram_title']), ('Telegram title', 'Telegram title'))
        self.assertEqual(self.telegram.calls, [])

    async def test_emoji_names_use_utf16_units_at_the_128_character_boundary(self):
        name = '⛓️‍💥 Workspace' + 'a' * 113
        result = self.rename(name)
        self.assertEqual(result.state, 'queued')
        await self.service.controls_once()
        self.assertEqual(self.request(result)['state'], 'done')
        self.assertEqual(self.store.topic(TOPIC)['name'], name)
        self.assertEqual(self.telegram.calls, [('editForumTopic', {
            'chat_id': -10042, 'message_thread_id': 4, 'name': name})])
        refused = self.rename(name + 'a')
        self.assertEqual(refused.state, 'refused')
        self.assertIn('128', refused.text)
        self.assertEqual(self.store.topic(TOPIC)['name'], name)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0], 1)

    async def test_empty_name_and_general_topic_are_refused_without_side_effects(self):
        self.assertEqual(self.rename('   ').state, 'refused')
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('-10042:1',-10042,1,'General','')")
        result = self.rename('New General', topic='-10042:1')
        self.assertEqual(result.state, 'refused')
        self.assertIn('General', result.text)
        self.assertEqual(self.store.topic('-10042:1')['name'], 'General')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0], 0)

    async def test_unchanged_telegram_topic_is_success_and_repeating_is_a_noop(self):
        result = self.rename('Workspace')
        with patch.object(self.telegram, 'call', AsyncMock(
                side_effect=TelegramError(400, reason='topic_not_modified'))):
            await self.service.controls_once()
        self.assertEqual(self.request(result)['state'], 'done')
        self.assertEqual(self.store.topic(TOPIC)['name'], 'Workspace')
        again = self.rename('Workspace')
        self.assertEqual(again.state, 'done')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0], 1)

    async def test_group_change_before_execution_refuses_without_telegram_call(self):
        result = self.rename('Workspace')
        with self.store.db:
            self.store.put('group', -99)
        await self.service.controls_once()
        self.assertEqual(self.request(result)['state'], 'refused')
        self.assertEqual(self.store.topic(TOPIC)['name'], 'Torii')
        self.assertEqual(self.telegram.calls, [])

    async def test_closing_topic_is_refused_without_telegram_call(self):
        result = self.rename('Workspace')
        with self.store.db:
            self.store.put('topic_lifecycle:' + TOPIC, {'state': 'closing'})
        await self.service.controls_once()
        self.assertEqual(self.request(result)['state'], 'refused')
        self.assertEqual(self.store.topic(TOPIC)['name'], 'Torii')
        self.assertEqual(self.telegram.calls, [])
