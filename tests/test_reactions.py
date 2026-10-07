import asyncio
from dataclasses import replace
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import control_api, mcp
from coordinator.native_protocol import NativeProtocol, RunControl
from coordinator.reactions import (EMOJI, accept_reaction, check_administrators, deliver_reaction,
                                   finish_turn, handed_off, parent_tools, set_desired, worker_started)
from coordinator.service import Service
from coordinator.session import CoordinatorSession
from coordinator.store import Store
from coordinator.telegram import Telegram, TelegramError


TOPIC = '-10042:4'
OTHER = '-10042:5'


class ReactionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name))
        self.addCleanup(self.store.close)
        with self.store.db:
            self.store.put('owner', 7)
            self.store.put('group', -10042)
            for topic, chat, thread in ((TOPIC, -10042, 4), (OTHER, -10042, 5), ('-22:4', -22, 4)):
                self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                                      (topic, chat, thread, topic, self.tmp.name))
        self.telegram = AsyncMock()

    def message(self, text='Build it', topic=TOPIC, telegram=501, kind='owner'):
        return self.store.message_save(topic, kind, text, telegram_message=telegram)['id']

    def row(self, message):
        return dict(self.store.db.execute('SELECT * FROM messages WHERE id=?', (message,)).fetchone())

    def call(self, op, params, topic=TOPIC, source='mcp'):
        with self.store.db:
            return control_api.call(self.store, op, params, topic=topic, source=source)

    def reaction_update(self, message=501, new=None, old=None, user=7, chat=-10042):
        return {'message_id': message, 'chat': {'id': chat, 'type': 'supergroup'}, 'user': {'id': user},
                'old_reaction': old or [], 'new_reaction': new if new is not None else [{'type': 'emoji', 'emoji': '👍'}]}

    async def test_new_messages_never_get_an_instant_reaction(self):
        for kind in ('owner', 'worker_result', 'callback', 'restarted', 'secret_filled'):
            message = self.message(kind=kind, telegram=None)
            self.assertIsNone(self.row(message)['reaction_desired'])
        self.message(telegram=501)
        self.assertFalse(await deliver_reaction(self.store, self.telegram))
        self.telegram.call.assert_not_called()

    async def test_delivery_replaces_with_one_standard_emoji_and_removes_with_an_empty_list(self):
        message = self.message()
        for emoji in (*EMOJI, None):
            with self.store.db:
                set_desired(self.store, message, emoji)
            self.assertTrue(await deliver_reaction(self.store, self.telegram))
            self.telegram.call.assert_awaited_with('setMessageReaction', chat_id=-10042, message_id=501,
                reaction=[{'type': 'emoji', 'emoji': emoji}] if emoji else [])
            self.assertEqual(self.row(message)['reaction_sent'], emoji)
            self.assertEqual(self.row(message)['reaction_state'], 'sent')
            self.assertFalse(await deliver_reaction(self.store, self.telegram))
        self.assertEqual(EMOJI, ('👀', '👨\u200d💻', '🤔', '👌'))
        with self.assertRaises(ValueError):
            set_desired(self.store, message, '🚀')

    async def test_only_owner_messages_with_telegram_numbers_can_get_reactions(self):
        for kind, telegram in (('owner', None), ('callback', 9), ('worker_result', 10)):
            message = self.message(kind=kind, telegram=telegram)
            with self.store.db:
                set_desired(self.store, message, '👀')
            self.assertIsNone(self.row(message)['reaction_desired'])
        self.assertFalse(await deliver_reaction(self.store, self.telegram))

    async def test_desired_change_during_delivery_is_not_lost(self):
        message = self.message()
        set_desired(self.store, message, '👀')
        entered, release = asyncio.Event(), asyncio.Event()

        async def send(*args, **kwargs):
            entered.set()
            await release.wait()
        self.telegram.call.side_effect = send
        task = asyncio.create_task(deliver_reaction(self.store, self.telegram))
        await asyncio.wait_for(entered.wait(), 2)
        with self.store.db:
            set_desired(self.store, message, None)
        release.set()
        await task
        self.assertEqual(self.row(message)['reaction_state'], 'pending')
        self.assertEqual(self.row(message)['reaction_sent'], '👀')
        await deliver_reaction(self.store, self.telegram)
        self.telegram.call.assert_awaited_with('setMessageReaction', chat_id=-10042, message_id=501, reaction=[])

    async def test_retry_after_backoff_and_disable_still_record_problems(self):
        message = self.message()
        set_desired(self.store, message, '👀')
        self.telegram.call.side_effect = TelegramError(429, retry_after=25)
        with patch('coordinator.reactions.time.time', return_value=100):
            await deliver_reaction(self.store, self.telegram)
            self.assertEqual(self.row(message)['reaction_retry'], 125)
            self.assertFalse(await deliver_reaction(self.store, self.telegram))
        self.telegram.call.side_effect = TelegramError('network-or-invalid-response', reason='timeout')
        with patch('coordinator.reactions.time.time', return_value=126):
            await deliver_reaction(self.store, self.telegram)
            self.assertEqual(self.row(message)['reaction_retry'], 130)
        for code in (400, 403):
            with self.store.db:
                self.store.db.execute('UPDATE messages SET reaction_retry=0,reaction_state=\'pending\'')
            self.telegram.call.side_effect = TelegramError(code)
            await deliver_reaction(self.store, self.telegram)
            self.assertEqual(self.row(message)['reaction_state'], 'disabled')
            set_desired(self.store, message, '👌' if code == 400 else None)
            self.assertEqual(self.row(message)['reaction_state'], 'disabled')
        codes = [row[0] for row in self.store.db.execute('SELECT code FROM problems ORDER BY id')]
        self.assertEqual(codes, ['reaction-failed', 'reaction-timeout', 'reaction-failed', 'reaction-failed'])

    async def test_fifth_failure_disables_delivery(self):
        message = self.message()
        set_desired(self.store, message, '👀')
        self.telegram.call.side_effect = TelegramError(500)
        for attempt in range(5):
            self.store.db.execute('UPDATE messages SET reaction_retry=0')
            await deliver_reaction(self.store, self.telegram)
            self.assertEqual(self.row(message)['reaction_attempts'], attempt + 1)
        self.assertEqual(self.row(message)['reaction_state'], 'disabled')
        self.assertFalse(await deliver_reaction(self.store, self.telegram))

    def test_task_create_stores_origin_and_all_task_results_show_it(self):
        message = self.message()
        created = self.call('tasks.create', {'message': message, 'title': 'Build'})
        self.assertTrue(created.ok, created.text)
        self.assertEqual(created.data['origin'], message)
        self.assertEqual(self.row(message)['reaction_desired'], '👀')
        for op, params in (('tasks.get', {'task': created.data['id']}),
                           ('tasks.update', {'task': created.data['id'], 'notes': 'Waiting for the owner'})):
            result = self.call(op, params)
            self.assertTrue(result.ok, result.text)
            self.assertEqual(result.data['origin'], message)
        self.assertEqual(self.call('tasks.list', {}).data[0]['origin'], message)
        self.assertEqual(self.row(message)['reaction_desired'], '👀')

    def test_legacy_task_calls_without_origins_work_and_do_not_react(self):
        created = self.call('tasks.create', {'title': 'Old tools'}, source='cli')
        self.assertTrue(created.ok, created.text)
        self.assertIsNone(created.data['origin'])
        for status in ('done', 'dropped', 'open'):
            self.assertTrue(self.call('tasks.update', {'task': created.data['id'], 'status': status}).ok)

    def test_status_map_done_dropped_reopened_and_waiting(self):
        message = self.message()
        task = self.store.task_create(TOPIC, 'Build', origin=message)
        for status, emoji in (('done', '👌'), ('open', '👀'), ('dropped', None), ('open', '👀')):
            self.store.task_update(task['id'], status=status)
            self.assertEqual(self.row(message)['reaction_desired'], emoji)
        set_desired(self.store, message, '🤔')
        self.store.task_update(task['id'], notes='Waiting on the owner')
        self.assertEqual(self.row(message)['reaction_desired'], '🤔')

    def test_worker_work_defaults_to_research_and_is_validated_at_the_boundary(self):
        self.store.put('accounts', {'work': {'config_dir': None, 'enabled': True}})
        message = self.message()
        task = self.store.task_create(TOPIC, 'Build', origin=message, worktree=self.tmp.name)
        for work in (None, 'dev', 'research'):
            params = {'task': task['id'], 'provider': 'claude', 'prompt': 'Do it'}
            if work is not None:
                params['work'] = work
            result = self.call('workers.spawn', params)
            self.assertTrue(result.ok, result.text)
            worker = dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (result.data['worker'],)).fetchone())
            self.assertEqual(worker['work'], work or 'research')
            self.assertEqual(self.row(message)['reaction_desired'], '👀')
        result = self.call('workers.spawn', dict(params, work='writing code'))
        self.assertFalse(result.ok)
        self.assertEqual(mcp.input_schema(control_api.BY_ID['workers.spawn'])['properties']['work']['enum'],
                         ['dev', 'research'])

    def test_latest_started_worker_wins_and_finished_jobs_stay_finished(self):
        message = self.message()
        task = self.store.task_create(TOPIC, 'Build', origin=message)
        for work, emoji in (('dev', '👨\u200d💻'), ('research', '🤔'), ('dev', '👨\u200d💻')):
            worker_started(self.store, {'task': task['id'], 'work': work})
            self.assertEqual(self.row(message)['reaction_desired'], emoji)
        for status, emoji in (('done', '👌'), ('dropped', None)):
            self.store.task_update(task['id'], status=status)
            worker_started(self.store, {'task': task['id'], 'work': 'dev'})
            self.assertEqual(self.row(message)['reaction_desired'], emoji)
        worker_started(self.store, {'task': None, 'work': 'research'})

    def test_queued_reports_in_same_channel_suppress_eyes_even_before_delivery(self):
        message = self.message()
        with patch('coordinator.reactions.time.time', return_value=100):
            self.store.enqueue_report(TOPIC, 'Previous answer')
            handed_off(self.store, message)
            self.store.enqueue_report(TOPIC, 'Fast answer')
        parent_tools(self.store, [message], [{'type': 'tool_use', 'name': 'Agent'}])
        finish_turn(self.store, [message])
        self.assertIsNone(self.row(message)['reaction_desired'])

    def test_parent_tools_ignores_malformed_assistant_content(self):
        message = self.message()
        with patch('coordinator.reactions.time.time', return_value=100):
            handed_off(self.store, message)
        for content in ('text', None, ['x'], [{'type': 'tool_use'}],
                        [{'type': 'tool_use', 'name': None}]):
            with self.subTest(content=content):
                parent_tools(self.store, [message], content)
                self.assertEqual(self.row(message)['turn_tools'], 0)
                self.assertIsNone(self.row(message)['reaction_desired'])

    def test_other_channels_and_reports_before_handoff_do_not_suppress_eyes(self):
        message = self.message()
        with patch('coordinator.reactions.time.time', return_value=100):
            self.store.enqueue_report(TOPIC, 'Previous answer')
            handed_off(self.store, message)
            self.store.enqueue_report(OTHER, 'Another channel')
        parent_tools(self.store, [], [{'type': 'tool_use', 'name': 'Agent'}])
        self.assertIsNone(self.row(message)['reaction_desired'])
        parent_tools(self.store, [message], [{'type': 'tool_use', 'name': 'Agent'}])
        self.assertEqual(self.row(message)['reaction_desired'], '👀')

    def test_job_created_during_active_turn_owns_its_reaction(self):
        message = self.message()
        with patch('coordinator.reactions.time.time', return_value=100):
            handed_off(self.store, message)
        parent_tools(self.store, [message], [{'type': 'tool_use', 'name': 'Agent'}])
        task = self.store.task_create(TOPIC, 'Build', origin=message)
        worker_started(self.store, {'task': task['id'], 'work': 'dev'})
        parent_tools(self.store, [message], [{'type': 'tool_use', 'name': 'Agent'}])
        finish_turn(self.store, [message])
        self.assertEqual(self.row(message)['reaction_desired'], '👨\u200d💻')

    def parent(self, provider='claude'):
        session = CoordinatorSession(self.store, None, 'test-session', None, 30)
        session.protocol = SimpleNamespace(provider=provider)
        session.busy = True
        return session

    def handoff(self, session, telegram=501, topic=TOPIC):
        message = self.message(telegram=telegram, topic=topic)
        session._turn_rows.add(message)
        session._rows[str(message)] = message
        session._handed_off(str(message))
        return message

    def tools(self, session, *names, replay=False, parent=None):
        event = {'type': 'assistant', 'parent_tool_use_id': parent, 'message': {'content': [
            {'type': 'tool_use', 'id': str(i), 'name': name, 'input': {}} for i, name in enumerate(names)]}}
        session._apply_event_state(event, replay=replay)

    async def reaction_cycle(self, session):
        service = object.__new__(Service)
        service.store = self.store
        service.telegram = self.telegram
        service.sessions = {TOPIC: session}
        service.wake = lambda name: asyncio.Event()
        with patch('coordinator.service.pause', side_effect=asyncio.CancelledError):
            with self.assertRaises(asyncio.CancelledError):
                await service.reactions()

    async def test_activity_zero_or_one_calls_never_get_eyes_with_elapsed_time(self):
        for names in ((), ('WebSearch',)):
            with self.subTest(names=names):
                session = self.parent()
                with patch('coordinator.reactions.time.time', return_value=100):
                    message = self.handoff(session, telegram=501 + len(names))
                self.tools(session, *names)
                with patch('coordinator.reactions.time.time', return_value=10000):
                    await self.reaction_cycle(session)
                self.assertIsNone(self.row(message)['reaction_desired'])
                finish_turn(self.store, [message])

    def test_activity_two_calls_before_send_get_eyes(self):
        session = self.parent()
        message = self.handoff(session)
        self.tools(session, 'WebSearch')
        self.assertIsNone(self.row(message)['reaction_desired'])
        self.tools(session, 'mcp__example__query')
        self.assertEqual(self.row(message)['reaction_desired'], '👀')

    def test_activity_reply_send_is_excluded_and_sent_reply_suppresses_eyes(self):
        session = self.parent()
        message = self.handoff(session)
        self.tools(session, 'mcp__torii__telegram_send', 'WebSearch')
        self.assertIsNone(self.row(message)['reaction_desired'])
        self.tools(session, 'Read')
        self.assertEqual(self.row(message)['reaction_desired'], '👀')
        finish_turn(self.store, [message])
        message = self.handoff(session, telegram=502)
        self.store.enqueue_report(TOPIC, 'Reply sent first')
        self.tools(session, 'mcp__torii__telegram_send', 'Read', 'WebSearch', 'Agent')
        self.assertIsNone(self.row(message)['reaction_desired'])

    def test_activity_one_delegation_gets_eyes(self):
        for index, name in enumerate(('Agent', 'Task', 'Workflow', 'mcp__torii__workers_spawn')):
            with self.subTest(name=name):
                session = self.parent()
                message = self.handoff(session, telegram=501 + index)
                self.tools(session, name)
                self.assertEqual(self.row(message)['reaction_desired'], '👀')
                finish_turn(self.store, [message])

    def test_activity_replayed_events_never_count_or_set_eyes(self):
        session = self.parent()
        message = self.handoff(session)
        self.tools(session, 'WebSearch')
        self.tools(session, 'Read', 'Agent', replay=True)
        self.assertIsNone(self.row(message)['reaction_desired'])
        self.tools(session, 'Read')
        self.assertEqual(self.row(message)['reaction_desired'], '👀')

    def test_activity_job_origin_keeps_worker_and_completion_reactions(self):
        session = self.parent()
        message = self.handoff(session)
        self.tools(session, 'Agent')
        self.assertEqual(self.row(message)['reaction_desired'], '👀')
        task = self.store.task_create(TOPIC, 'Build', origin=message)
        for work, emoji in (('dev', '👨‍💻'), ('research', '🤔')):
            worker_started(self.store, {'task': task['id'], 'work': work})
            self.tools(session, 'Agent', 'Read', 'WebSearch')
            self.assertEqual(self.row(message)['reaction_desired'], emoji)
        self.store.task_update(task['id'], status='done')
        self.tools(session, 'Agent')
        finish_turn(self.store, [message])
        self.assertEqual(self.row(message)['reaction_desired'], '👌')

    def test_activity_mid_turn_handoff_counts_only_later_calls(self):
        session = self.parent()
        first = self.handoff(session)
        self.tools(session, 'Read')
        later = self.handoff(session, telegram=502)
        self.tools(session, 'WebSearch')
        self.assertEqual(self.row(first)['reaction_desired'], '👀')
        self.assertIsNone(self.row(later)['reaction_desired'])
        self.tools(session, 'Read')
        self.assertEqual(self.row(later)['reaction_desired'], '👀')

    async def test_activity_finish_turn_still_removes_eyes(self):
        session = self.parent()
        message = self.handoff(session)
        self.tools(session, 'Read', 'WebSearch')
        self.assertEqual(self.row(message)['reaction_desired'], '👀')
        await deliver_reaction(self.store, self.telegram)
        self.telegram.call.assert_awaited_with('setMessageReaction', chat_id=-10042, message_id=501,
                                              reaction=[{'type': 'emoji', 'emoji': '👀'}])
        finish_turn(self.store, [message])
        await deliver_reaction(self.store, self.telegram)
        self.telegram.call.assert_awaited_with('setMessageReaction', chat_id=-10042, message_id=501, reaction=[])

    def test_activity_only_live_parent_tool_use_blocks_count(self):
        session = self.parent()
        message = self.handoff(session)
        self.tools(session, 'Read', 'Agent', parent='parent-agent-call')
        session._apply_event_state({'type': 'tool_progress', 'name': 'Agent'})
        session._apply_event_state({'type': 'assistant', 'message': {'content': [
            {'type': 'text', 'text': 'Agent'}, {'type': 'thinking', 'thinking': 'Read'}]}})
        self.assertIsNone(self.row(message)['reaction_desired'])
        self.tools(session, 'Read')
        self.assertIsNone(self.row(message)['reaction_desired'])
        self.tools(session, 'WebSearch')
        self.assertEqual(self.row(message)['reaction_desired'], '👀')
        finish_turn(self.store, [message])
        session = self.parent(provider='codex')
        message = self.handoff(session, telegram=502)
        self.tools(session, 'Agent', 'Read', 'WebSearch')
        self.assertIsNone(self.row(message)['reaction_desired'])

    def test_activity_count_survives_store_reopen_without_counting_replay(self):
        session = self.parent()
        message = self.handoff(session)
        self.tools(session, 'Read')
        self.store.close()
        self.store = Store(Path(self.tmp.name))
        self.addCleanup(self.store.close)
        session = self.parent()
        session._turn_rows.add(message)
        self.tools(session, 'Read', 'Agent', replay=True)
        self.assertIsNone(self.row(message)['reaction_desired'])
        self.tools(session, 'WebSearch')
        self.assertEqual(self.row(message)['reaction_desired'], '👀')

    async def test_handoff_waits_for_the_native_write_and_ignores_closed_input(self):
        release = asyncio.Event()
        stdin = AsyncMock()
        stdin.write = lambda data: None
        stdin.drain.side_effect = release.wait
        process = AsyncMock()
        process.stdin = stdin
        handed = []
        protocol = NativeProtocol('claude', process, RunControl(), 'session', lambda value: value,
                                  persistent=True, on_handoff=handed.append)
        task = asyncio.create_task(protocol.steer('one', 'Build'))
        await asyncio.sleep(0)
        self.assertEqual(handed, [])
        release.set()
        self.assertEqual(await task, 'received')
        self.assertEqual(handed, ['one'])
        protocol.closed = True
        self.assertEqual(await protocol.steer('two', 'Held'), 'closed')
        self.assertEqual(handed, ['one'])

    async def test_telegram_send_task_uses_channel_job_number_and_reply_to_wins(self):
        self.store.task_create(OTHER, 'Make database ID differ from job number')
        message = self.message()
        task = self.store.task_create(TOPIC, 'Build', origin=message)
        self.assertNotEqual(task['id'], task['number'])
        result = self.call('telegram.send', {'text': 'Done', 'task': task['number']})
        self.assertTrue(result.ok, result.text)
        self.assertTrue(result.data['quoted'])
        outbox = self.store.db.execute('SELECT reply_to FROM outbox WHERE id=?', (result.data['outbox'],)).fetchone()
        self.assertEqual(outbox['reply_to'], 501)
        override = self.message(telegram=902)
        result = self.call('telegram.send', {'text': 'Answer', 'task': 999, 'reply_to': override})
        self.assertTrue(result.ok, result.text)
        self.assertEqual(self.store.db.execute('SELECT reply_to FROM outbox WHERE id=?',
                                              (result.data['outbox'],)).fetchone()[0], 902)
        result = self.call('telegram.send', {'text': 'Answer', 'task': task['number'], 'reply_to': 999})
        self.assertFalse(result.ok)
        self.assertEqual(self.call('telegram.send', {'text': 'Lost', 'task': 999}).state, 'refused')

    def test_job_report_without_valid_owner_origin_sends_without_quote_and_says_so(self):
        for origin in (None, self.message(topic=OTHER), self.message(kind='worker_result', telegram=502),
                       self.message(telegram=None)):
            task = self.store.task_create(TOPIC, 'Build', origin=origin)
            result = self.call('telegram.send', {'text': 'Done', 'task': task['number']})
            self.assertTrue(result.ok, result.text)
            self.assertFalse(result.data['quoted'])
            self.assertIn('without a quote', result.text)
            self.assertIsNone(self.store.db.execute('SELECT reply_to FROM outbox WHERE id=?',
                                                    (result.data['outbox'],)).fetchone()[0])

    def test_owner_reactions_route_by_chat_and_message_to_bot_or_owner_messages(self):
        self.message(text='Owner request', topic=OTHER)
        with self.store.db:
            outbox = self.store.enqueue_report(TOPIC, 'Bot reply\n' + 'x' * 350)
            self.store.delivered(outbox, 600)
        for update, topic, text in ((self.reaction_update(), OTHER, 'Replying to: Owner request\nReacted 👍'),
                                   (self.reaction_update(message=600), TOPIC, 'Replying to: ' + ('Bot reply ' + 'x' * 350)[:300] + '\nReacted 👍')):
            uid = 50 if topic == OTHER else 51
            self.assertEqual(self.store.accept({'update_id': uid, 'message_reaction': update}), 'queued')
            self.assertEqual(self.store.accept({'update_id': uid, 'message_reaction': update}), 'duplicate')
            row = self.store.messages_pending()[-1]
            self.assertEqual((row['topic'], row['kind'], row['text']), (topic, 'owner', text))
            self.assertIsNone(row['telegram_message'])
            self.assertIsNone(row['reaction_desired'])
        self.assertEqual(self.store.get('offset'), 52)

    def test_reaction_additions_only_and_custom_emoji_share_one_message(self):
        self.message()
        old = [{'type': 'emoji', 'emoji': '👍'}]
        new = old + [{'type': 'emoji', 'emoji': '🤔'}, {'type': 'custom_emoji', 'custom_emoji_id': '99'},
                     {'type': 'paid'}]
        self.assertEqual(accept_reaction(self.store, self.reaction_update(new=new, old=old)), 'queued')
        self.assertEqual(self.store.messages_pending()[-1]['text'], 'Replying to: Build it\nReacted 🤔 (custom emoji) ⭐')
        self.assertEqual(len([row for row in self.store.messages_pending() if row['reacted_to'] is not None]), 1)

    def test_ignored_reactions_do_not_queue_anything(self):
        self.message()
        same = [{'type': 'emoji', 'emoji': '👍'}]
        tests = [self.reaction_update(new=[], old=same), self.reaction_update(new=same, old=same),
                 self.reaction_update(user=8),
                 self.reaction_update(message=999), self.reaction_update(chat=-22),
                 dict(self.reaction_update(), user={'id': 7, 'is_bot': True}),
                 dict(self.reaction_update(), user=None, actor_chat={'id': -10042})]
        for update in tests:
            self.assertEqual(accept_reaction(self.store, update), 'ignored')
        self.assertEqual(len(self.store.messages_pending()), 1)

    async def test_bot_message_reaction_as_job_origin_drives_all_reactions_and_job_quotes(self):
        with self.store.db:
            outbox = self.store.enqueue_report(TOPIC, 'Bot report')
            self.store.delivered(outbox, 600)
            accept_reaction(self.store, self.reaction_update(message=600))
        reaction = self.store.messages_pending()[-1]
        self.assertEqual(reaction['kind'], 'owner')
        self.assertIsNone(reaction['telegram_message'])
        self.assertEqual(reaction['reacted_to'], 600)
        task = self.call('tasks.create', {'title': 'Follow up', 'message': reaction['id']}).data
        self.assertEqual(task['origin'], reaction['id'])
        await deliver_reaction(self.store, self.telegram)
        self.telegram.call.assert_awaited_with('setMessageReaction', chat_id=-10042, message_id=600,
                                              reaction=[{'type': 'emoji', 'emoji': '👀'}])
        for work, emoji in (('dev', '👨\u200d💻'), ('research', '🤔')):
            worker_started(self.store, {'task': task['id'], 'work': work})
            await deliver_reaction(self.store, self.telegram)
            self.telegram.call.assert_awaited_with('setMessageReaction', chat_id=-10042, message_id=600,
                                                  reaction=[{'type': 'emoji', 'emoji': emoji}])
        for status, emoji in (('done', '👌'), ('dropped', None), ('open', '👀')):
            self.store.task_update(task['id'], status=status)
            await deliver_reaction(self.store, self.telegram)
            self.telegram.call.assert_awaited_with('setMessageReaction', chat_id=-10042, message_id=600,
                                                  reaction=[{'type': 'emoji', 'emoji': emoji}] if emoji else [])
        report = self.call('telegram.send', {'task': task['number'], 'text': 'Follow up done'})
        self.assertTrue(report.ok, report.text)
        self.assertTrue(report.data['quoted'])
        self.assertEqual(self.store.db.execute('SELECT reply_to FROM outbox WHERE id=?',
                                              (report.data['outbox'],)).fetchone()[0], 600)
        self.assertIsNone(self.row(reaction['id'])['reaction_desired'])

    async def test_bot_message_edit_preserves_the_pending_reaction_on_the_same_telegram_target(self):
        with self.store.db:
            outbox = self.store.enqueue_report(TOPIC, 'Bot report')
            self.store.delivered(outbox, 600)
            accept_reaction(self.store, self.reaction_update(message=600))
        reaction = self.store.messages_pending()[-1]
        self.store.task_create(TOPIC, 'Follow up', origin=reaction['id'])
        with self.store.db:
            replacement = self.store.enqueue_report(TOPIC, 'Updated bot report', edit=600)
            self.store.delivered(replacement, 600)
        await deliver_reaction(self.store, self.telegram)
        self.telegram.call.assert_awaited_with('setMessageReaction', chat_id=-10042, message_id=600,
                                              reaction=[{'type': 'emoji', 'emoji': '👀'}])
        self.assertEqual(self.store.db.execute('SELECT reaction_sent FROM outbox WHERE id=?',
                                              (replacement,)).fetchone()[0], '👀')
        self.assertFalse(await deliver_reaction(self.store, self.telegram))

    async def test_reaction_to_owner_message_updates_the_original_row(self):
        original = self.message()
        accept_reaction(self.store, self.reaction_update())
        reaction = self.store.messages_pending()[-1]
        task = self.store.task_create(TOPIC, 'Follow up', origin=reaction['id'])
        self.assertEqual(self.row(original)['reaction_desired'], '👀')
        await deliver_reaction(self.store, self.telegram)
        self.telegram.call.assert_awaited_with('setMessageReaction', chat_id=-10042, message_id=501,
                                              reaction=[{'type': 'emoji', 'emoji': '👀'}])
        self.assertIsNone(self.row(reaction['id'])['reaction_desired'])
        report = self.call('telegram.send', {'task': task['number'], 'text': 'Done'})
        self.assertTrue(report.data['quoted'])

    async def test_reaction_reply_uses_the_turn_rule_on_its_bot_target(self):
        with self.store.db:
            outbox = self.store.enqueue_report(TOPIC, 'Bot report')
            self.store.delivered(outbox, 600)
            accept_reaction(self.store, self.reaction_update(message=600))
        reaction = self.store.messages_pending()[-1]
        with patch('coordinator.reactions.time.time', return_value=100):
            handed_off(self.store, reaction['id'])
        parent_tools(self.store, [reaction['id']], [{'type': 'tool_use', 'name': 'Agent'}])
        await deliver_reaction(self.store, self.telegram)
        self.telegram.call.assert_awaited_with('setMessageReaction', chat_id=-10042, message_id=600,
                                              reaction=[{'type': 'emoji', 'emoji': '👀'}])
        finish_turn(self.store, [reaction['id']])
        await deliver_reaction(self.store, self.telegram)
        self.telegram.call.assert_awaited_with('setMessageReaction', chat_id=-10042, message_id=600,
                                              reaction=[])
        self.assertIsNone(self.row(reaction['id'])['reaction_desired'])

    def test_unknown_reaction_logs_a_drop_without_a_problem(self):
        with self.assertLogs('coordinator.reactions', 'INFO') as logs:
            result = accept_reaction(self.store, self.reaction_update(message=999))
        self.assertEqual(result, 'ignored')
        self.assertIn('owner reaction dropped unknown', logs.output[0])
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM problems').fetchone()[0], 0)

    async def test_long_poll_explicitly_requests_reaction_updates(self):
        telegram = Telegram.__new__(Telegram)
        with patch.object(telegram, '_call', return_value=[]) as call:
            await telegram.updates(42)
        self.assertEqual(call.call_args.args[1]['allowed_updates'], ['message', 'callback_query', 'message_reaction', 'my_chat_member'])

    async def test_startup_checks_bot_membership_once_per_linked_chat(self):
        self.telegram.call.side_effect = [{'id': 99}, {'status': 'member'}, {'status': 'administrator'}]
        await check_administrators(self.store, self.telegram)
        self.assertEqual(self.telegram.call.await_count, 3)
        self.assertEqual(self.telegram.call.await_args_list[0].args, ('getMe',))
        self.telegram.call.assert_any_await('getChatMember', chat_id=-10042, user_id=99)
        self.telegram.call.assert_any_await('getChatMember', chat_id=-22, user_id=99)
        rows = self.store.db.execute('SELECT code,detail FROM problems').fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['code'], 'reaction-admin-required')
        self.assertIn('status=member', rows[0]['detail'])

    async def test_administrator_check_errors_record_problems_and_do_not_abort_startup(self):
        self.telegram.call.side_effect = TelegramError(403)
        await check_administrators(self.store, self.telegram)
        self.telegram.call.side_effect = [{'id': 99}, TelegramError(403), {'status': 'creator'}]
        await check_administrators(self.store, self.telegram)
        self.assertEqual([row[0] for row in self.store.db.execute('SELECT code FROM problems')],
                         ['reaction-admin-check-failed', 'reaction-admin-check-failed'])

    def test_mcp_tool_hash_is_stable_and_tracks_descriptions_names_and_schemas(self):
        original = mcp.tools_hash()
        self.assertEqual(original, mcp.tools_hash())
        self.assertEqual(len(original), 64)
        name = 'telegram_send'
        op = mcp.TOOLS[name]
        for changed in (replace(op, description=op.description + ' New description.'),
                        replace(op, params=dict(op.params, extra='str?'))):
            with patch.dict(mcp.TOOLS, {name: changed}):
                self.assertNotEqual(original, mcp.tools_hash())
        with patch.dict(mcp.TOOLS, {'new_name': op}):
            self.assertNotEqual(original, mcp.tools_hash())
        with patch.dict(mcp.TOOLS, dict(reversed(list(mcp.TOOLS.items()))), clear=True):
            self.assertEqual(original, mcp.tools_hash())


class ReactionMigrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_upgrade_preserves_sent_reactions_and_cancels_pending_without_telegram_changes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with sqlite3.connect(root / 'state.sqlite') as db:
                db.executescript("""CREATE TABLE topics (
                    id TEXT PRIMARY KEY,chat INTEGER,thread INTEGER,name TEXT,cwd TEXT,
                    enabled INTEGER DEFAULT 1,provider TEXT,session TEXT,waiting_job INTEGER,source_pid INTEGER);
                    INSERT INTO topics VALUES ('-10042:4',-10042,4,'Torii','',1,NULL,NULL,NULL,NULL);
                    CREATE TABLE messages (
                    id INTEGER PRIMARY KEY,topic TEXT REFERENCES topics(id),
                    kind TEXT CHECK(kind IN ('owner','worker_result','restarted','callback','secret_filled')),
                    telegram_message INTEGER,text TEXT,source_worker INTEGER,images TEXT,source_envelope INTEGER,
                    delivered TEXT DEFAULT 'pending',receipt TEXT,reaction_state TEXT DEFAULT 'pending',
                    reaction_retry REAL DEFAULT 0,reaction_attempts INTEGER DEFAULT 0,created REAL,updated REAL);
                    """)
                for number, state in enumerate(('sent', 'pending', 'disabled'), 1):
                    db.execute("INSERT INTO messages (id,topic,kind,telegram_message,text,reaction_state,created,updated) "
                               "VALUES (?,'-10042:4','owner',?,'Old message',?,1,1)", (number, number + 500, state))
            store = Store(root)
            telegram = AsyncMock()
            try:
                self.assertFalse(await deliver_reaction(store, telegram))
                telegram.call.assert_not_called()
                rows = store.db.execute('SELECT reaction_desired,reaction_sent,reaction_state FROM messages ORDER BY id').fetchall()
                self.assertEqual([tuple(row) for row in rows], [('👀', '👀', 'sent'), (None, None, 'sent'),
                                                               (None, None, 'disabled')])
                self.assertIsNone(store.task_create(TOPIC, 'Existing tools')['origin'])
                self.assertEqual(store.message_save(TOPIC, 'owner', 'New', telegram_message=900)['reaction_state'], 'sent')
                self.assertEqual(store.message_save(TOPIC, 'owner', 'Reacted 👍', reacted_to=501)['reacted_to'], 501)
                self.assertEqual(store.db.execute('PRAGMA foreign_key_check').fetchall(), [])
            finally:
                store.close()
            reopened = Store(root)
            try:
                self.assertFalse(await deliver_reaction(reopened, telegram))
                self.assertEqual(reopened.db.execute('SELECT reaction_desired FROM messages WHERE id=1').fetchone()[0], '👀')
            finally:
                reopened.close()
