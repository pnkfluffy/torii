import asyncio
from collections import Counter
import json
import re
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import opencode_recovery, parent_state
from coordinator.policy import chat_policy
from coordinator.session import CoordinatorSession, message_uuid
from tests.test_service import FakeSession, ServiceTestsSupport, TOPIC


class RecoveryParent(FakeSession):
    provider = 'claude'

    def __init__(self):
        super().__init__('saved-batch-parent')
        self.receipt = 'received'

    def idle(self):
        return True

    async def send(self, message_id, text, row_id=None):
        self.sent.append((message_id, text))
        return self.receipt


class IntakeRecoveryTests(ServiceTestsSupport, unittest.IsolatedAsyncioTestCase):
    batching = True

    async def parent(self, topic=TOPIC):
        self.store.put('coordinator_intake_batching', True)
        if topic != TOPIC:
            with self.store.db:
                self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                                      (topic, 1, 9, 'Other', str(self.root)))
        session = RecoveryParent()
        session.key = self.service.session_key(topic)
        session.usage_policy = chat_policy(self.store, session.key)
        self.service.sessions[topic] = session
        self.sessions.append(session)
        return session

    def result(self, topic=TOPIC):
        return self.store.message_save(topic, 'worker_result', json.dumps(
            {'worker': 1, 'task': 1, 'success': True, 'text': 'Saved worker result.'}))

    def saved(self, row):
        return dict(self.store.db.execute('SELECT * FROM messages WHERE id=?', (row['id'],)).fetchone())

    async def flush(self):
        started = await self.service.feed_once()
        if self.service.feed_tasks:
            await asyncio.gather(*set(self.service.feed_tasks.values()))
        return started

    async def drain(self):
        self.store.put('coordinator_intake_batching', self.batching)
        started = [await self.flush() for _ in range(5)]
        self.assertEqual(started[-2:], [False, False])
        self.assertEqual(self.store.messages_pending(), [])

    def assert_received_once(self, session, rows):
        ids = [int(value) for _, text in session.sent
               for value in re.findall(r'\[topic=[^\n]*? message=(\d+) kind=', text)]
        self.assertEqual(Counter(ids), Counter(row['id'] for row in rows))
        for row in rows:
            saved = self.saved(row)
            self.assertEqual((saved['delivered'], saved['receipt']), ('received', 'written'))
            self.assertEqual(sum(row['text'] in text for _, text in session.sent), 1)

    async def native_tracking(self, row, prompt):
        native = CoordinatorSession(self.store, self.runner, 'recovered-parent', None, 30)
        native.process = SimpleNamespace(returncode=None)
        native.control.steer = AsyncMock(return_value='received')
        wire = message_uuid(row['id'])
        with patch.object(native, 'movable', return_value=False):
            self.assertEqual(await native.send(wire, prompt, row_id=row['id']), 'received')
        return native._rows[wire], native._turn_rows

    async def test_start_session_delivers_secret_requeued_from_uncertain_batch(self):
        session = await self.parent()
        worker = self.result()
        secret = self.store.message_save(TOPIC, 'secret_filled', 'Secret SAMPLE_KEY is filled.')
        self.assertTrue(self.store.message_batch_sending([worker['id'], secret['id']]))
        self.store.requeue_messages([worker['id'], secret['id']])
        session.receipt = 'uncertain'
        await self.flush()
        self.assertEqual([self.saved(row)['delivered'] for row in (worker, secret)], ['uncertain', 'uncertain'])
        self.assertEqual(self.saved(secret)['batch_id'], worker['id'])
        await session.stop()
        self.service.sessions.pop(TOPIC)
        resumed = await self.service.start_session(TOPIC)
        self.assertIsNotNone(resumed)
        self.assertEqual(self.saved(secret)['delivered'], 'pending')
        await self.drain()
        self.assert_received_once(resumed, [secret])
        self.assertEqual((self.saved(worker)['delivered'], self.saved(worker)['receipt']), ('uncertain', 'uncertain'))

    async def test_requeued_secret_anchor_does_not_track_uncertain_worker(self):
        session = await self.parent()
        secret = self.store.message_save(TOPIC, 'secret_filled', 'Secret OTHER_KEY is filled.')
        worker = self.result()
        session.receipt = 'uncertain'
        await self.flush()
        self.assertEqual(self.saved(worker)['batch_id'], secret['id'])
        await session.stop()
        self.service.sessions.pop(TOPIC)
        resumed = await self.service.start_session(TOPIC)
        self.assertIsNotNone(resumed)
        await self.drain()
        self.assert_received_once(resumed, [secret])
        self.assertEqual(await self.native_tracking(secret, resumed.sent[0][1]), ({secret['id']}, {secret['id']}))
        self.assertEqual((self.saved(worker)['delivered'], self.saved(worker)['receipt']), ('uncertain', 'uncertain'))

    async def test_replace_notice_delivers_remaining_member_after_closed_batch(self):
        session = await self.parent()
        worker = self.result()
        notice = self.store.message_save(TOPIC, 'restarted', 'Old restart notice.')
        session.receipt = 'closed'
        await self.flush()
        self.assertEqual(self.saved(notice)['batch_id'], worker['id'])
        self.service.replace_notice(TOPIC, 'Current restart notice.')
        current = dict(self.store.db.execute('SELECT * FROM messages WHERE id=?',
                                             (self.service.restart_messages[TOPIC],)).fetchone())
        session.sent.clear()
        session.receipt = 'received'
        await self.drain()
        self.assert_received_once(session, [current, worker])
        self.assertEqual((self.saved(notice)['delivered'], self.saved(notice)['receipt']), ('received', 'superseded'))

    async def test_provider_switch_delivers_original_and_worker_after_callback_superseded(self):
        session = await self.parent()
        owner = self.store.message_save(TOPIC, 'owner', 'Original request from a lost turn.')
        self.store.message_delivered(owner['id'], 'written')
        parent_state.settle(self.store, session.key, [owner['id']], lambda rows: set(rows))
        callback = dict(self.store.db.execute("SELECT * FROM messages WHERE kind='callback'").fetchone())
        worker = self.result()
        session.receipt = 'closed'
        await self.flush()
        self.assertEqual(self.saved(worker)['batch_id'], callback['id'])
        parent_state.recover_lost_turns(self.store, session.key, switched=True)
        session.sent.clear()
        session.receipt = 'received'
        await self.drain()
        self.assert_received_once(session, [owner, worker])
        self.assertEqual((self.saved(callback)['delivered'], self.saved(callback)['receipt']), ('received', 'superseded'))

    async def test_reclaimed_anchor_tracks_only_new_batch_members(self):
        session = await self.parent()
        worker = self.result()
        notice = self.store.message_save(TOPIC, 'restarted', 'Retired restart notice.')
        session.receipt = 'closed'
        await self.flush()
        self.service.replace_notice(TOPIC, None)
        session.sent.clear()
        session.receipt = 'received'
        await self.drain()
        self.assert_received_once(session, [worker])
        self.assertEqual(await self.native_tracking(worker, session.sent[0][1]), ({worker['id']}, {worker['id']}))
        self.assertEqual((self.saved(notice)['delivered'], self.saved(notice)['receipt']), ('received', 'superseded'))

    async def test_opencode_unanswered_delivers_worker_after_restart_member_superseded(self):
        topic = 'other'
        session = await self.parent(topic)
        session.provider = 'opencode'
        notice = self.store.message_save(topic, 'restarted', 'Old provider restart notice.')
        worker = self.result(topic)
        session.receipt = 'closed'
        await self.flush()
        self.assertEqual(self.saved(worker)['batch_id'], notice['id'])
        self.assertEqual(opencode_recovery.unanswered(self.store, session.key, [notice['id'], worker['id']]),
                         [worker['id']])
        session.sent.clear()
        session.receipt = 'received'
        await self.drain()
        self.assert_received_once(session, [worker])
        self.assertEqual((self.saved(notice)['delivered'], self.saved(notice)['receipt']), ('received', 'superseded'))

    async def test_missing_anchor_delivery_returns_and_feed_delivers_survivor(self):
        session = await self.parent()
        anchor = self.store.message_save(TOPIC, 'owner', 'Already received request.')
        worker = self.result()
        self.assertTrue(self.store.message_batch_sending([anchor['id'], worker['id']]))
        self.store.messages_delivered([anchor['id'], worker['id']], 'written')
        self.store.requeue_messages([worker['id']])
        await self.service._deliver_batch([self.saved(worker)], session, TOPIC)
        await self.drain()
        self.assert_received_once(session, [worker])
        self.assertEqual((self.saved(anchor)['delivered'], self.saved(anchor)['receipt']), ('received', 'written'))


class IntakeRecoverySwitchOffTests(IntakeRecoveryTests):
    batching = False
