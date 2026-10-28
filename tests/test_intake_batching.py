import asyncio
from collections import Counter
import json
import re
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from coordinator import control_api
from coordinator.media import MediaError
from coordinator.policy import chat_policy
from coordinator.session import CoordinatorSession, message_uuid
from tests.test_service import FakeSession, ServiceTestsSupport, TOPIC


class BusyParent(FakeSession):
    provider = 'claude'

    def __init__(self):
        super().__init__('fake-busy-parent')
        self.busy = True
        self.queue = []
        self.turns = []

    def idle(self):
        return not self.busy

    async def send(self, message_id, text, row_id=None):
        self.sent.append((message_id, text))
        self.queue.append(text)
        self.busy = True
        return 'received'

    def next_turn(self):
        text = self.queue.pop(0)
        ids = [int(value) for value in re.findall(r'\[topic=[^\n]*? message=(\d+) kind=', text)]
        self.turns.append(ids)
        return ids


class IntakeBatchingTests(ServiceTestsSupport, unittest.IsolatedAsyncioTestCase):
    async def parent(self, enabled=True):
        self.store.put('coordinator_intake_batching', enabled)
        session = BusyParent()
        session.usage_policy = chat_policy(self.store, session.key)
        self.service.sessions[TOPIC] = session
        self.sessions.append(session)
        return session

    async def flush(self):
        await self.service.feed_once()
        if self.service.feed_tasks:
            await asyncio.gather(*set(self.service.feed_tasks.values()))

    def result(self, number, success=True, text='finished'):
        return self.store.message_save(TOPIC, 'worker_result', json.dumps(
            {'worker': number, 'task': number, 'success': success, 'text': text}))

    def rows(self):
        return [dict(row) for row in self.store.db.execute('SELECT * FROM messages ORDER BY id')]

    async def test_busy_flood_answers_owner_in_next_turn_and_keeps_every_row_once(self):
        session = await self.parent()
        workers = [self.result(number + 1, success=number not in (4, 12)) for number in range(30)]
        owner = self.store.message_save(TOPIC, 'owner', 'Answer this request.', telegram_message=501)
        await self.flush()
        self.assertEqual(len(session.sent), 1)
        first = session.next_turn()
        urgent = [workers[index]['id'] for index in (4, 12)]
        routine = [row['id'] for row in workers if row['id'] not in urgent]
        expected = [owner['id']] + urgent + routine
        self.assertEqual(first, expected[:20])
        reply = control_api.call(self.store, 'telegram.send', {'topic': TOPIC, 'text': 'Answer.', 'reply_to': owner['id']})
        self.assertTrue(reply.ok, reply.text)
        self.assertEqual(self.store.db.execute('SELECT reply_to FROM outbox').fetchone()[0], 501)
        await self.flush()
        self.assertEqual(len(session.sent), 2)
        self.assertTrue(session.busy)
        self.assertEqual(session.next_turn(), expected[20:])
        self.assertEqual(Counter(item for turn in session.turns for item in turn), Counter(expected))
        self.assertEqual([(row['delivered'], row['receipt']) for row in self.rows()], [('received', 'written')] * 31)
        self.assertEqual([len(turn) for turn in session.turns], [20, 11])
        self.assertTrue(all(len(text) <= 8000 for _, text in session.sent))
        for row in self.rows():
            prompt = next(text for _, text in session.sent if 'message=%d kind=' % row['id'] in text)
            self.assertIn(row['text'], prompt)
            self.assertIn('sender=', prompt)
            self.assertIn('job=', prompt)
            self.assertIn('created=', prompt)
        print('flood on: 31 rows, 2 batches (20, 11), owner answered in turn 1, each row received once')

    async def test_switch_off_keeps_main_fifo_admission_while_busy(self):
        session = await self.parent(enabled=False)
        workers = [self.result(number + 1) for number in range(30)]
        owner = self.store.message_save(TOPIC, 'owner', 'Answer this request.')
        await self.flush()
        self.assertEqual([identity for identity, _ in session.sent], [message_uuid(row['id']) for row in workers + [owner]])
        turns = [session.next_turn() for _ in range(31)]
        self.assertEqual(turns, [[row['id']] for row in workers + [owner]])
        self.assertTrue(all('sender=' not in text for _, text in session.sent))
        print('flood off: 31 rows, 31 inputs, owner at FIFO position and fixture turn 31')

    async def test_routine_flood_is_admitted_while_busy_and_owner_heads_next_batch(self):
        session = await self.parent()
        workers = [self.result(number + 1) for number in range(30)]
        await self.flush()
        self.assertEqual(len(session.sent), 1)
        self.assertEqual(session.next_turn(), [row['id'] for row in workers[:20]])
        self.assertEqual(len(self.store.messages_pending()), 10)
        owner = self.store.message_save(TOPIC, 'owner', 'Owner arrives while busy.')
        await self.flush()
        self.assertEqual(session.next_turn(), [owner['id']] + [row['id'] for row in workers[20:]])
        self.assertTrue(session.busy)
        self.assertEqual([(row['delivered'], row['receipt']) for row in self.rows()], [('received', 'written')] * 31)

    async def test_priority_is_oldest_within_owner_failure_and_routine_classes(self):
        session = await self.parent()
        routine = self.store.message_event(TOPIC, 'Done. No blockers.', 'Builder', preview=False)
        failed = self.store.message_event(TOPIC, 'Failed: checks did not pass.', 'Checker', preview=False)
        older = self.store.message_save(TOPIC, 'owner', 'First separate ask.')
        blocked = self.store.message_event(TOPIC, 'Job 12 blocked: waiting for input.', 'Builder', preview=False)
        newer = self.store.message_save(TOPIC, 'owner', 'Second separate ask.')
        passed = self.result(7, text='Previously blocked. No failure remains.')
        await self.flush()
        self.assertEqual(session.next_turn(), [row['id'] for row in (older, newer, failed, blocked, routine, passed)])
        text = session.sent[0][1]
        self.assertEqual(text.count('First separate ask.'), 1)
        self.assertEqual(text.count('Second separate ask.'), 1)
        self.assertIn('from="Checker"', text)

    async def test_waiting_owner_header_keeps_one_original_tag_per_item(self):
        session = await self.parent()
        self.store.put('waits_enabled', True)
        waiting = control_api.call(self.store, 'tasks.wait', {'kind': 'owner', 'ask': 'Pick A or B'}, topic=TOPIC)
        self.assertTrue(waiting.ok, waiting.text)
        owner = self.store.message_save(TOPIC, 'owner', 'What is next?')
        worker = self.result(1)
        await self.flush()
        self.assertEqual(session.next_turn(), [owner['id'], worker['id']])
        text = session.sent[0][1]
        self.assertEqual(text.count('waiting="Chat: Pick A or B"'), 1)
        self.assertEqual(text.count('sender='), 2)
        self.assertEqual(text.count(owner['text']), 1)
        self.assertEqual(text.count(worker['text']), 1)
        self.assertTrue(text.startswith('[topic='))

    async def test_character_cap_keeps_whole_items_and_single_oversized_item(self):
        session = await self.parent()
        first = self.store.message_save(TOPIC, 'owner', 'a' * 7300)
        second = self.store.message_save(TOPIC, 'owner', 'b' * 900)
        large = self.store.message_save(TOPIC, 'owner', 'c' * 9000)
        await self.flush()
        self.assertEqual(session.next_turn(), [first['id']])
        self.assertLessEqual(len(session.sent[0][1]), 8000)
        session.busy = False
        await self.flush()
        self.assertEqual(session.next_turn(), [second['id']])
        session.busy = False
        await self.flush()
        self.assertEqual(session.next_turn(), [large['id']])
        self.assertIn(large['text'], session.sent[-1][1])

    async def test_media_preparation_is_included_in_cap_and_audio_stays_a_barrier(self):
        session = await self.parent()
        audio = self.store.message_save(TOPIC, 'owner', 'Audio ask.', images=[{'file_id': 'fake-audio'}])
        release = asyncio.Event()
        preparing = asyncio.Event()
        original = self.service._prepare_message

        async def prepare(row, parent=None, batch=False):
            if row['id'] == audio['id']:
                preparing.set()
                await release.wait()
                return await original(dict(row, images=None, text='prepared ' + 'a' * 7300), parent, batch=batch)
            return await original(row, parent, batch=batch)

        with patch.object(self.service, '_prepare_message', side_effect=prepare):
            self.service.audio_tasks[audio['id']] = asyncio.create_task(asyncio.sleep(0))
            await self.service.feed_once()
            await asyncio.wait_for(preparing.wait(), 2)
            later = self.store.message_save(TOPIC, 'owner', 'Later ask.')
            await self.service.feed_once()
            await asyncio.sleep(0)
            self.assertEqual(session.sent, [])
            release.set()
            await asyncio.gather(*set(self.service.feed_tasks.values()))
        self.assertEqual(len(session.sent), 2)
        self.assertIn('a' * 7300, session.sent[0][1])
        self.assertIn('message=%d kind=owner' % later['id'], session.sent[1][1])

    async def test_claim_and_receipt_are_atomic_and_membership_survives_requeue(self):
        await self.parent()
        first, second = self.result(1), self.result(2)
        self.assertTrue(self.store.message_batch_sending([second['id'], first['id']]))
        self.assertFalse(self.store.message_batch_sending([second['id'], first['id']]))
        self.assertEqual(self.store.message_batch_rows(first['id']), {first['id'], second['id']})
        self.store.messages_delivered([first['id'], second['id']], 'written')
        self.store.messages_confirm([first['id'], second['id']], 'replayed')
        self.store.messages_delivered([first['id'], second['id']], 'uncertain')
        self.assertEqual([row['receipt'] for row in self.rows()], ['replayed', 'replayed'])
        self.store.requeue_messages([first['id'], second['id']])
        self.assertFalse(self.store.message_batch_sending([first['id']]))
        self.assertTrue(self.store.message_batch_sending([second['id'], first['id']]))

    async def test_existing_batch_is_not_regrouped_with_new_owner_and_off_still_recovers(self):
        session = await self.parent()
        a, b = self.result(1), self.result(2)
        self.assertTrue(self.store.message_batch_sending([a['id'], b['id']]))
        self.store.requeue_messages([a['id'], b['id']])
        owner = self.store.message_save(TOPIC, 'owner', 'New independent ask.')
        await self.flush()
        self.assertEqual(session.next_turn(), [owner['id']])
        self.store.put('coordinator_intake_batching', False)
        session.busy = False
        await self.flush()
        self.assertEqual(session.next_turn(), [a['id'], b['id']])
        self.assertEqual(self.rows()[0]['batch_id'], a['id'])

    async def test_closed_batch_requeues_all_members_and_uncertain_is_not_retried(self):
        session = await self.parent()
        a = self.store.message_save(TOPIC, 'owner', 'A.')
        b = self.result(1)
        session.send = AsyncMock(return_value='closed')
        await self.flush()
        self.assertEqual([row['delivered'] for row in self.rows()], ['pending', 'pending'])
        session.send = AsyncMock(return_value='uncertain')
        await self.flush()
        self.assertEqual([row['delivered'] for row in self.rows()], ['uncertain', 'uncertain'])
        await self.flush()
        self.assertEqual(session.send.await_count, 1)

    async def test_rerouted_saved_batch_keeps_all_members_and_original_tags(self):
        session = await self.parent()
        session.busy = False
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('other',1,9,'Other','',0)")
        owner = self.store.message_save('other', 'owner', 'Separate original ask.')
        worker = self.store.message_save('other', 'worker_result', '{"success":true,"text":"finished"}')
        ids = [owner['id'], worker['id']]
        self.assertTrue(self.store.message_batch_sending(ids))
        self.store.requeue_messages(ids)
        await self.flush()
        self.assertEqual(session.next_turn(), ids)
        self.assertTrue(all(row['delivered'] == 'received' for row in self.rows()))
        self.assertEqual(session.sent[0][1].count('[topic=other '), 2)
        self.assertNotIn('other', self.service.sessions)

    async def test_reenabled_topic_keeps_mixed_saved_batch_in_home_chat(self):
        session = await self.parent()
        session.busy = False
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('other',1,9,'Other','',0)")
        owner = self.store.message_save(TOPIC, 'owner', 'Home ask.')
        worker = self.store.message_save('other', 'worker_result', '{"success":true,"text":"finished"}')
        ids = [owner['id'], worker['id']]
        self.assertTrue(self.store.message_batch_sending(ids))
        self.store.requeue_messages(ids)
        with self.store.db:
            self.store.db.execute("UPDATE topics SET enabled=1 WHERE id='other'")
        self.store.put('coordinator_intake_batching', False)
        with patch.object(self.service, 'start_session', AsyncMock(return_value=session)) as start:
            await self.flush()
        self.assertEqual([call.args[0] for call in start.await_args_list], [TOPIC])
        self.assertEqual(session.next_turn(), ids)
        self.assertEqual([(row['delivered'], row['receipt']) for row in self.rows()], [('received', 'written')] * 2)
        self.assertIn('[topic=other ', session.sent[0][1])

    async def check_failed_media_retry(self, anchor):
        session = await self.parent()
        session.busy = False
        if not anchor:
            worker = self.result(1)
        owner = self.store.message_save(TOPIC, 'owner', 'Original attachment ask.',
                                        images=[{'file_id': 'fake-image'}])
        if anchor:
            worker = self.result(1)
        ids = sorted([owner['id'], worker['id']])
        self.assertTrue(self.store.message_batch_sending(ids))
        self.store.requeue_messages(ids)
        with patch('coordinator.service.prepare_attachments', AsyncMock(side_effect=MediaError('Fixture media unavailable'))):
            await self.flush()
        self.assertEqual(len(session.sent), 1)
        self.assertEqual(session.next_turn(), [worker['id']])
        rows = {row['id']: row for row in self.rows()}
        self.assertEqual((rows[owner['id']]['delivered'], rows[owner['id']]['receipt']), ('uncertain', 'uncertain'))
        self.assertEqual((rows[worker['id']]['delivered'], rows[worker['id']]['receipt']), ('received', 'written'))
        self.assertEqual(self.store.message_batch_rows(worker['id']), {worker['id']})
        self.assertFalse(await self.service.feed_once())

    async def test_failed_media_anchor_leaves_other_saved_batch_members_deliverable(self):
        await self.check_failed_media_retry(True)

    async def test_failed_media_member_leaves_saved_anchor_deliverable(self):
        await self.check_failed_media_retry(False)

    async def test_background_and_uncertain_claude_admit_routine_and_failures_without_idle_wait(self):
        session = await self.parent()
        session.idle = lambda: CoordinatorSession.idle(session)
        session.turn_open = False
        session._sending = 0
        session._unread_reattach_events = 0
        session._unreplayed = set()
        session._unconsumed_writes = set()
        session.protocol = SimpleNamespace(background=set(), pending=set())
        for background, uncertain in (({'background-task'}, False), (set(), True)):
            with self.subTest(background=bool(background), uncertain=uncertain):
                session.busy = False
                session.protocol.background = background
                session._recovery_uncertain = uncertain
                self.assertFalse(session.idle())
                routine = self.result(1)
                failure = self.result(2, success=False)
                before = len(session.sent)
                await self.flush()
                self.assertEqual(len(session.sent), before + 1)
                self.assertEqual(session.next_turn(), [failure['id'], routine['id']])
                self.assertTrue(all(row['delivered'] == 'received' for row in self.rows()))

    async def test_foreground_busy_parent_admits_failure_and_routine_on_the_next_feed(self):
        session = await self.parent()
        self.assertFalse(session.idle())
        routine = self.result(1)
        failure = self.result(2, success=False)
        await self.flush()
        self.assertEqual(len(session.sent), 1)
        self.assertEqual(session.next_turn(), [failure['id'], routine['id']])
        self.assertEqual([(row['delivered'], row['receipt']) for row in self.rows()], [('received', 'written')] * 2)

    async def test_secret_filled_bypasses_busy_parent_without_an_owner_row(self):
        session = await self.parent()
        routine = self.result(1)
        secret = self.store.message_save(TOPIC, 'secret_filled', 'Secret FIXTURE was filled.')
        await self.flush()
        self.assertEqual(len(session.sent), 1)
        self.assertEqual(session.next_turn(), [secret['id'], routine['id']])

    async def test_secret_filled_has_owner_priority_oldest_first(self):
        session = await self.parent()
        routine = self.result(1)
        failure = self.result(2, success=False)
        secret = self.store.message_save(TOPIC, 'secret_filled', 'Secret FIXTURE was filled.')
        owner = self.store.message_save(TOPIC, 'owner', 'Next separate ask.')
        forwarded = self.store.message_save(TOPIC, 'forwarded', 'Forwarded separate ask.')
        await self.flush()
        self.assertEqual(session.next_turn(), [row['id'] for row in (secret, owner, forwarded, failure, routine)])

    async def test_restart_notice_precedes_batch_and_catchup_gates_owner(self):
        from coordinator import opencode_recovery
        session = await self.parent()
        session.provider = 'opencode'
        notice = self.store.message_save(TOPIC, 'restarted', 'Recovery notice.')
        self.service.restart_messages[TOPIC] = notice['id']
        owner = self.store.message_save(TOPIC, 'owner', 'Ask after recovery.')
        opencode_recovery.save(self.store, session.key, {'phase': 'catchup', 'notice': notice['id']})
        await self.flush()
        self.assertEqual(session.next_turn(), [notice['id']])
        self.assertEqual([row['id'] for row in self.store.messages_pending()], [owner['id']])
        opencode_recovery.save(self.store, session.key, None)
        await self.flush()
        self.assertEqual(session.next_turn(), [owner['id']])

    async def test_switch_is_persisted_off_by_default_and_exposed_to_mcp(self):
        self.assertIsNone(self.store.get('coordinator_intake_batching'))
        result = control_api.call(self.store, 'settings.get', {})
        self.assertIs(result.data['coordinator_intake_batching'], False)
        for enabled in (True, False):
            result = control_api.call(self.store, 'service.intake_batching', {'enabled': enabled},
                                      topic=TOPIC, source='mcp')
            self.assertTrue(result.ok, result.text)
            self.assertIs(self.store.get('coordinator_intake_batching'), enabled)
        self.assertFalse(control_api.call(self.store, 'service.intake_batching', {'enabled': 'invalid'}).ok)
