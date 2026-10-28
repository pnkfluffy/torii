"""OpenCode parent refusals recover without growing or changing providers."""

import asyncio
import json
import os
import signal
import time
import unittest
from unittest.mock import patch

from coordinator import opencode_accounts, opencode_recovery, opencode_runtime
from coordinator.service import Service
from tests.support import until
from tests import test_opencode_parent as fixtures
from tests.test_service import FakeTelegram


WEEKLY_BODY = {'error': {'message': 'Weekly included input token limit exceeded. Enable paid balance use after limits '
    'in your subscription settings at the host subscription page to continue using this model from your balance, '
    'use a paid model, or wait for the weekly reset.', 'type': 'rate_limit_error',
    'code': 'weekly_included_input_token_limit_exceeded', 'status': 429},
    'resetAt': int(time.time() + 86400) * 1000}


class OpenCodeRecoveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.chat = fixtures.OpenCodeParentTests()
        self.chat.addCleanup = self.addCleanup
        self.chat.setUp()
        self.store = self.chat.store
        self.key = self.chat.session_key
        self.service = Service(self.store, FakeTelegram(), self.chat.runner, self.chat.cwd)

    async def asyncTearDown(self):
        await self.chat.asyncTearDown()

    def recovery(self):
        return self.store.get(self.key + '_opencode_recovery') or {}

    async def start(self):
        session = await self.service.start_session('chat')
        if session is not None:
            self.chat.parents.append(session)
        return session

    async def failed_turn(self, **scenario):
        self.chat.scenario(unique_sessions=True, **scenario)
        session = await self.start()
        self.assertIsNotNone(session)
        row = self.chat.row('Unanswered request')
        await self.chat.send(session, row)
        await session.wait_closed()
        return session, row

    def expire(self):
        current = self.recovery()
        current['retry_at'] = 0
        self.store.put(self.key + '_opencode_recovery', current)

    async def test_real_overflow_uses_one_bounded_catchup_then_original_rows(self):
        session, owner = await self.failed_turn(context_overflow=True)
        result = self.chat.row('worker result ' + 'x' * 20000, 'worker_result')
        native = session.session_id
        self.assertEqual(self.recovery().get('code'), 'context_overflow')
        self.assertIsNone(await self.start())
        self.expire()
        release = self.chat.root / 'release'
        self.chat.scenario(unique_sessions=True, release=str(release))
        fresh = await self.start()
        self.assertIsNotNone(fresh)
        self.assertNotEqual(fresh.session_id, native)
        self.assertEqual((fresh.provider, fresh.account_alias, fresh.model),
                         (session.provider, session.account_alias, session.model))
        await self.service.feed_once()
        await until(lambda: len(self.chat.calls('session/prompt')) == 2)
        prompt = self.chat.calls('session/prompt')[-1]['message']['params']['prompt']
        text = '\n'.join(part['text'] for part in prompt)
        self.assertLess(len(text), 8500)
        self.assertIn('earlier conversation was too long', text)
        self.assertIn('Unanswered request', text)
        self.assertNotIn(result['text'], text)
        self.assertEqual(self.chat.persisted(owner)['delivered'], 'pending')
        self.assertEqual(self.chat.persisted(result)['delivered'], 'pending')
        release.touch()
        await until(fresh.idle)
        self.assertEqual(self.recovery()['phase'], 'resuming')
        await self.service.feed_once()
        await until(lambda: not self.recovery())
        self.assertEqual(self.chat.persisted(owner)['delivered'], 'received')
        self.assertEqual(self.chat.persisted(result)['delivered'], 'received')

    async def test_failed_catchup_pauses_until_owner_writes(self):
        session, row = await self.failed_turn(context_overflow=True)
        self.assertEqual(self.recovery().get('phase'), 'fresh')
        self.expire()
        fresh = await self.start()
        self.assertIsNotNone(fresh)
        await self.service.feed_once()
        await fresh.wait_closed()
        self.assertEqual(self.recovery()['phase'], 'paused')
        self.expire()
        for _ in range(4):
            self.assertIsNone(await self.start())
        self.assertEqual(len(self.chat.calls('session/new')), 2)
        reports = self.store.db.execute('SELECT text FROM outbox').fetchall()
        self.assertEqual(sum('fresh catch-up failed' in item['text'] for item in reports), 1)
        self.assertEqual(self.chat.persisted(row)['delivered'], 'pending')
        self.chat.row('Try again after fixing the host')
        self.chat.scenario(unique_sessions=True)
        self.assertIsNotNone(await self.start())

    async def test_repeated_failures_back_off_persist_and_do_not_add_notices(self):
        session, row = await self.failed_turn(crash=True)
        self.assertGreater(self.recovery().get('retry_at', 0), time.time() + 20)
        self.assertIsNone(await self.start())
        before = self.store.db.execute('SELECT count(*) FROM messages').fetchone()[0]
        self.service = Service(self.store, FakeTelegram(), self.chat.runner, self.chat.cwd)
        self.assertIsNone(await self.start())
        for attempt in range(2, 5):
            self.expire()
            resumed = await self.start()
            self.assertIsNotNone(resumed)
            self.assertEqual(resumed.session_id, session.session_id)
            notice = next(item for item in self.store.messages_pending() if item['kind'] == 'callback')
            await self.chat.send(resumed, notice)
            await resumed.wait_closed()
            self.assertEqual(self.recovery()['attempts'], attempt)
            self.assertGreater(self.recovery()['retry_at'] - time.time(), min(300, 30 * 2 ** (attempt - 1)) - 2)
            self.assertEqual(self.store.db.execute('SELECT count(*) FROM messages').fetchone()[0], before)
        wake = self.chat.row('Owner wakes backoff', 'forwarded')
        self.chat.scenario(unique_sessions=True)
        resumed = await self.start()
        self.assertIsNotNone(resumed)
        await self.chat.send(resumed, wake)
        await until(resumed.idle)
        self.assertFalse(self.recovery())

    async def test_legacy_notice_pile_recovers_only_unanswered_originals(self):
        old = self.chat.row('Already answered')
        self.store.message_delivered(old['id'], 'written')
        answered = self.chat.row('Your last turn stopped before it finished. message=%d' % old['id'], 'callback')
        self.store.message_delivered(answered['id'], 'written')
        owner = self.chat.row('Saved owner request')
        worker = self.chat.row('Saved worker result', 'worker_result')
        turn = [owner['id'], worker['id']]
        recovery = {}
        for _ in range(331):
            notice = self.chat.row('Old restart ' + 'x' * 3000, 'restarted')
            callback = self.chat.row('Your last turn stopped before it finished. message=%d' % owner['id'], 'callback')
            recovery[str(callback['id'])] = [owner['id'], worker['id']]
            self.store.message_delivered(notice['id'], 'written')
            self.store.message_delivered(callback['id'], 'written')
            turn.extend([notice['id'], callback['id']])
        self.store.put(self.key + '_lost_turns', recovery)
        self.chat.scenario(context_overflow=True, unique_sessions=True)
        session = await self.start()
        before = self.store.db.execute('SELECT count(*) FROM messages').fetchone()[0]
        session._track(set(turn))
        await self.chat.send(session, owner)
        await session.wait_closed()
        self.assertEqual(self.recovery().get('rows'), [owner['id'], worker['id']])
        self.assertEqual([row['id'] for row in self.store.messages_pending()], [owner['id'], worker['id']])
        self.assertEqual(self.chat.persisted(old)['delivered'], 'received')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM messages').fetchone()[0], before)
        self.expire()
        fresh = await self.start()
        self.assertIsNotNone(fresh)
        current = [row for row in self.store.messages_pending() if row['kind'] == 'restarted']
        self.assertEqual(len(current), 1)
        self.assertLessEqual(len(current[0]['text']), 8000)

    async def test_masked_provider_weekly_429_pauses_and_rechecks_same_session(self):
        original = {'name': 'ContextOverflowError', 'status': 429,
                    'code': WEEKLY_BODY['error']['code'], 'message': WEEKLY_BODY['error']['message'],
                    'reset_at': WEEKLY_BODY['resetAt'] / 1000}
        session, row = await self.failed_turn(context_overflow=True, provider_error=original)
        self.assertEqual(self.recovery().get('code'), 'usage_limited')
        self.assertEqual(self.recovery()['phase'], 'retry')
        self.assertEqual(session._failure, 'usage_limited')
        block = self.store.get('opencode_account_blocks')['one']
        self.assertEqual(block['reason'], 'usage_limited')
        self.assertTrue(block['reset_known'])
        self.assertEqual(block['until'], WEEKLY_BODY['resetAt'] / 1000)
        self.assertEqual(block['reset_at'], WEEKLY_BODY['resetAt'] / 1000)
        self.assertIsNone(await self.start())
        self.assertIsNone(await self.start())
        reports = [item['text'] for item in self.store.db.execute('SELECT text FROM outbox')]
        self.assertEqual(sum('paid balance use after limits' in text for text in reports), 1)
        detail = self.store.db.execute("SELECT detail FROM problems WHERE code='session-closed' ORDER BY id DESC").fetchone()[0]
        self.assertIn('failure=usage_limited', detail)
        self.assertIn('status=429', detail)
        self.assertIn(WEEKLY_BODY['error']['code'], detail)
        self.assertIn('paid balance use after limits', detail)
        self.chat.scenario(unique_sessions=True)
        with patch('time.time', return_value=block['until'] + 1):
            resumed = await self.start()
        self.assertIsNotNone(resumed)
        self.assertEqual(resumed.session_id, session.session_id)
        await self.service.feed_once()
        await until(lambda: not self.recovery())
        self.assertFalse(self.recovery())
        self.assertIsNone(json.loads((self.chat.data / 'provider-error.json').read_text())['error'])
        self.assertEqual(len(self.chat.calls('session/new')), 1)

    async def test_legacy_masked_error_is_rechecked_without_rotating_history(self):
        session, row = await self.failed_turn(context_overflow=True)
        self.store.put(self.key + '_opencode_recovery', None)
        self.store.put(self.key + '_turn', [row['id']])
        self.assertIsNone(await self.start())
        self.assertEqual(self.recovery().get('phase'), 'retry')
        self.expire()
        self.chat.scenario(unique_sessions=True)
        resumed = await self.start()
        self.assertEqual(resumed.session_id, session.session_id)

    async def test_masked_http_refusals_without_native_records_do_not_rotate_history(self):
        for status, reason in ((402, 'out_of_credit'), (429, 'rate_limited'), (401, 'auth'), (403, 'auth')):
            with self.subTest(status=status):
                self.store.put('opencode_account_blocks', {})
                self.chat.row('A new owner request')
                session, row = await self.failed_turn(context_overflow=True, no_records=True, provider_error={
                    'name': 'ContextOverflowError', 'status': status, 'code': 'synthetic_refusal',
                    'message': 'synthetic refusal'})
                self.assertEqual(session._failure, reason)
                self.assertEqual(self.recovery()['phase'], 'retry')
                self.assertEqual(self.store.get('opencode_account_blocks')['one']['reason'], reason)
                self.assertEqual(self.chat.persisted(row)['delivered'], 'pending')
                self.assertIsNone(await self.start())
                self.assertEqual(self.store.get(self.key + '_session'), session.session_id)


WEEKLY = {"name": "ContextOverflowError", "status": 429, "code": WEEKLY_BODY["error"]["code"],
          "message": WEEKLY_BODY["error"]["message"], "reset_at": WEEKLY_BODY["resetAt"] / 1000}


def probe(name, **values):
    print("PROBE", name, json.dumps(values, sort_keys=True), flush=True)


def _dead(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    try:
        waited, _ = os.waitpid(pid, os.WNOHANG)
        return waited == pid
    except ChildProcessError:
        return False


class RecoveryRestartTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.chat = fixtures.OpenCodeParentTests()
        self.chat.addCleanup = self.addCleanup
        self.chat.setUp()
        self.store = self.chat.store
        self.key = self.chat.session_key
        self.service = Service(self.store, FakeTelegram(), self.chat.runner, self.chat.cwd)

    async def asyncTearDown(self):
        for session in list(self.service.sessions.values()):
            if session not in self.chat.parents and session.process is not None:
                self.chat.parents.append(session)
        for session in self.chat.parents:
            if session.process is None:
                continue
            if not session._stopping or session.process.returncode is not None:
                try:
                    await asyncio.wait_for(session.stop(), 30)
                except Exception:
                    pass

    def recovery(self):
        return self.store.get(self.key + '_opencode_recovery') or {}

    def expire(self):
        if self.recovery():
            current = self.recovery()
            current['retry_at'] = 0
            self.store.put(self.key + '_opencode_recovery', current)

    def prompts(self):
        return ['\n'.join(part['text'] for part in call['message']['params']['prompt'])
                for call in self.chat.calls('session/prompt')]

    def tags(self):
        import re
        return [re.findall(r'message=\d+ kind=\w+', text) or [text[:60]] for text in self.prompts()]

    def tagged(self, row):
        return sum(('message=%d kind=%s' % (row['id'], row['kind'])) in text for text in self.prompts())

    def current(self):
        return self.service.sessions.get('chat')

    async def drain(self, rounds=60):
        for _ in range(rounds):
            await self.service.feed_once()
            session = self.current()
            pending = [row for row in self.store.messages_pending() if row['topic'] == 'chat']
            if session is not None and not session.closed and session.idle() and not pending and not self.service.feed_tasks:
                return True
            await asyncio.sleep(0.1)
        return False

    def native_copies(self, row):
        sessions = json.loads((self.chat.data / 'data' / 'fake-sessions.json').read_text())
        tag = 'message=%d kind=%s' % (row['id'], row['kind'])
        return {sid[:12]: sum(tag in text for text in texts) for sid, texts in sessions.items()
                if any(tag in text for text in texts)}

    def state(self, row):
        saved = self.chat.persisted(row)
        return '%s/%s' % (saved['delivered'], saved['receipt'])

    def restart(self):
        self.store.messages_uncertain()
        self.service = Service(self.store, FakeTelegram(), self.chat.runner, self.chat.cwd)

    async def start(self, tries=3):
        session = None
        for _ in range(tries):
            self.expire()
            session = await self.service.start_session('chat')
            if session is not None:
                break
        return session

    async def crash_next_turn(self, text):
        """Stop the idle parent, then make the next real turn crash, as round 1 p1b did."""
        session = self.current()
        if session is not None and not session.closed:
            await session.stop()
        self.chat.scenario(unique_sessions=True, crash=True)
        row = self.chat.row(text)
        for _ in range(40):
            await self.service.feed_once()
            session = self.current()
            if session is not None and session.process is not None and session.process.returncode is not None:
                break
            await asyncio.sleep(0.05)
        await asyncio.wait_for(self.current().wait_closed(), 30)
        return row

    async def test_b1_switch_then_restart_then_failure(self):
        """Owner switches the OpenCode account; later a deploy restart; later any failed turn."""
        self.chat.scenario(unique_sessions=True)
        first = await self.service.start_session('chat')
        answered = self.chat.row('ANSWERED-ON-ACCOUNT-ONE')
        self.assertTrue(await self.drain())
        before_switch = self.state(answered)
        await first.stop()
        self.store.put('opencode_active_account', 'two')
        moved = await self.start()
        self.assertTrue(await self.drain())
        second = self.current()
        self.assertNotEqual(second.session_id, first.session_id)
        before_restart = self.state(answered)
        await second.detach()
        self.restart()
        attached = await self.start()
        self.chat.parents.append(attached)
        after_restart = self.state(answered)
        self.assertTrue(await self.drain())
        later = await self.crash_next_turn('LATER-TURN-CRASHES')
        after_failure = self.state(answered)
        self.chat.scenario(unique_sessions=True)
        await self.start()
        drained = await self.drain()
        probe('b1-switch', switched=bool(getattr(moved, 'switched', False)), before_switch=before_switch,
              before_restart=before_restart, after_restart=after_restart, after_failure=after_failure,
              answered_sent_times=self.tagged(answered), later_sent_times=self.tagged(later),
              native_copies=self.native_copies(answered), drained=drained, prompts=self.tags())
        self.assertEqual(self.tagged(answered), 1, 'an answered owner message was delivered to the parent again')
        self.assertTrue(drained)
        self.assertEqual(sum(self.native_copies(answered).values()), 1)
        self.assertEqual(self.state(answered), 'received/transcript')
        callbacks = [text for text in self.prompts() if 'Your last turn stopped before it finished' in text]
        self.assertFalse(any('message=%d' % answered['id'] in text for text in callbacks),
                         'an answered owner message was named in a continuation callback')

    async def test_b1_switch_live_relaunch_then_restart_then_failure(self):
        """Same as the switch probe, but the live parent relaunches itself after opencode.use, as in real use."""
        self.chat.scenario(unique_sessions=True)
        first = await self.service.start_session('chat')
        answered = self.chat.row('ANSWERED-ON-ACCOUNT-ONE')
        self.assertTrue(await self.drain())
        self.store.put('opencode_active_account', 'two')
        next_owner = self.chat.row('FIRST-MESSAGE-AFTER-OPENCODE-USE')
        self.assertTrue(await self.drain(rounds=80))
        second = self.current()
        self.assertNotEqual(second.session_id, first.session_id)
        self.assertEqual(second.account_alias, 'two')
        before_restart = self.state(answered)
        await second.detach()
        self.restart()
        attached = await self.start()
        self.chat.parents.append(attached)
        after_restart = self.state(answered)
        self.assertTrue(await self.drain())
        later = await self.crash_next_turn('LATER-TURN-CRASHES')
        after_failure = self.state(answered)
        self.chat.scenario(unique_sessions=True)
        await self.start()
        drained = await self.drain()
        probe('b1-switch-live', switched=bool(getattr(second, 'switched', False)), before_restart=before_restart,
              after_restart=after_restart, after_failure=after_failure, answered_sent_times=self.tagged(answered),
              next_owner_sent_times=self.tagged(next_owner), later_sent_times=self.tagged(later),
              native_copies=self.native_copies(answered), drained=drained, prompts=self.tags())
        self.assertEqual(self.tagged(answered), 1, 'an answered owner message was delivered to the parent again')
        self.assertTrue(drained)
        self.assertEqual(sum(self.native_copies(answered).values()), 1)
        self.assertEqual(self.state(answered), 'received/transcript')
        callbacks = [text for text in self.prompts() if 'Your last turn stopped before it finished' in text]
        self.assertFalse(any('message=%d' % answered['id'] in text for text in callbacks),
                         'an answered owner message was named in a continuation callback')

    async def test_b1_manual_rotation_then_restart_then_failure(self):
        """Old native session retired by hand (like the 18:00 rotation), new session healthy, restart, failure."""
        self.chat.scenario(unique_sessions=True)
        first = await self.service.start_session('chat')
        answered = self.chat.row('ANSWERED-BEFORE-ROTATION')
        self.assertTrue(await self.drain())
        await first.stop()
        self.store.put(self.key + '_session', None)
        self.store.put(self.key + '_opencode', None)
        healthy = self.chat.row('ANSWERED-ON-NEW-SESSION')
        self.assertTrue(await self.drain())
        second = self.current()
        self.assertNotEqual(second.session_id, first.session_id)
        await second.detach()
        self.restart()
        attached = await self.start()
        self.chat.parents.append(attached)
        after_restart = self.state(answered)
        self.assertTrue(await self.drain())
        later = await self.crash_next_turn('LATER-TURN-CRASHES')
        after_failure = self.state(answered)
        self.chat.scenario(unique_sessions=True)
        await self.start()
        drained = await self.drain()
        probe('b1-rotation', after_restart=after_restart, after_failure=after_failure,
              answered_sent_times=self.tagged(answered), healthy_sent_times=self.tagged(healthy),
              later_sent_times=self.tagged(later), native_copies=self.native_copies(answered),
              drained=drained, prompts=self.tags())
        self.assertEqual(self.tagged(answered), 1, 'an answered owner message was delivered to the parent again')
        self.assertTrue(drained)
        self.assertEqual(sum(self.native_copies(answered).values()), 1)
        self.assertEqual(self.state(answered), 'received/transcript')
        callbacks = [text for text in self.prompts() if 'Your last turn stopped before it finished' in text]
        self.assertFalse(any('message=%d' % answered['id'] in text for text in callbacks),
                         'an answered owner message was named in a continuation callback')

    async def test_b1_switch_restart_then_attached_host_dies_mid_turn(self):
        """Restart the switched idle host, then lose its child during the next owner turn."""
        release = self.chat.root / 'release-attached'
        release.touch()
        self.chat.scenario(unique_sessions=True, release=str(release))
        first = await self.service.start_session('chat')
        answered = self.chat.row('ANSWERED-ON-ACCOUNT-ONE')
        self.assertTrue(await self.drain())
        self.store.put('opencode_active_account', 'two')
        self.chat.row('FIRST-MESSAGE-AFTER-OPENCODE-USE')
        self.assertTrue(await self.drain(rounds=80))
        second = self.current()
        self.assertNotEqual(second.session_id, first.session_id)
        directory = second.process.directory
        await second.detach()
        self.restart()
        attached = await self.service.start_session('chat')
        self.chat.parents.append(attached)
        same_host = attached.host_id == second.host_id
        after_restart = self.state(answered)
        self.assertTrue(await self.drain())
        release.unlink()
        inflight = self.chat.row('OWNER-TURN-WHEN-HOST-DIES')
        await self.service.feed_once()
        await until(lambda: opencode_runtime.parent_receipts(self.chat.data, second.session_id, [inflight['id']])
                    == {inflight['id']})
        child_pid = int((directory / 'child.pid').read_text())
        try:
            os.killpg(child_pid, signal.SIGKILL)
        except ProcessLookupError:
            os.kill(child_pid, signal.SIGKILL)
        await asyncio.wait_for(attached.wait_closed(), 30)
        after_failure = self.state(answered)
        release.touch()
        self.chat.scenario(unique_sessions=True)
        await self.start()
        drained = await self.drain()
        probe('b1-switch-attached', same_host=same_host, failure=attached._failure, after_restart=after_restart,
              after_failure=after_failure, answered_sent_times=self.tagged(answered),
              inflight_sent_times=self.tagged(inflight), native_copies=self.native_copies(answered),
              drained=drained, prompts=self.tags())
        self.assertEqual(self.tagged(answered), 1, 'an answered owner message was delivered to the parent again')
        self.assertTrue(drained)
        self.assertEqual(sum(self.native_copies(answered).values()), 1)
        self.assertEqual(self.state(answered), 'received/transcript')
        callbacks = [text for text in self.prompts() if 'Your last turn stopped before it finished' in text]
        self.assertFalse(any('message=%d' % answered['id'] in text for text in callbacks),
                         'an answered owner message was named in a continuation callback')

    async def test_p1a_restore_resends_answered_rows_left_uncertain_by_deploy_restart(self):
        """restore.md path: a pre-fix failed turn, a deploy restart, then topic.enable."""
        self.chat.scenario(unique_sessions=True)
        first = await self.service.start_session('chat')
        answered = self.chat.row('ANSWERED-EARLIER-TODAY')
        self.assertTrue(await self.drain())
        state = self.chat.persisted(answered)
        self.assertEqual((state['delivered'], state['receipt']), ('received', 'written'))
        await first.stop()
        self.chat.scenario(unique_sessions=True, context_overflow=True)
        failing = self.chat.row('UNANSWERED-AT-OUTAGE')
        await self.service.feed_once()
        second = self.current()
        await second.wait_closed()
        self.store.put(self.key + '_opencode_recovery', None)
        self.store.put(self.key + '_turn', [failing['id']])
        self.store.messages_uncertain()
        self.assertEqual(self.chat.persisted(answered)['delivered'], 'uncertain')
        self.service = Service(self.store, FakeTelegram(), self.chat.runner, self.chat.cwd)
        self.chat.scenario(unique_sessions=True)
        for _ in range(3):
            self.expire()
            if await self.service.start_session('chat') is not None:
                break
        after_requeue = self.chat.persisted(answered)['delivered']
        self.assertTrue(await self.drain())
        probe('p1a', answered_state_after_enable=after_requeue, answered_sent_times=self.tagged(answered),
              failing_sent_times=self.tagged(failing), final_answered=self.chat.persisted(answered)['delivered'],
              ids={'answered': answered['id'], 'failing': failing['id']}, prompts=self.tags())
        self.assertEqual(self.tagged(answered), 1, 'an answered owner message was delivered to the parent again')

    async def test_p1b_fresh_session_then_restart_then_any_failure_resends_old_rows(self):
        self.chat.scenario(unique_sessions=True)
        first = await self.service.start_session('chat')
        answered = self.chat.row('ANSWERED-ON-OLD-SESSION')
        self.assertTrue(await self.drain())
        await first.stop()
        self.chat.scenario(unique_sessions=True, context_overflow=True)
        overflowing = self.chat.row('OVERFLOWS')
        await self.service.feed_once()
        await self.current().wait_closed()
        self.assertEqual(self.recovery()['phase'], 'fresh')
        self.expire()
        self.chat.scenario(unique_sessions=True)
        fresh = await self.service.start_session('chat')
        self.assertNotEqual(fresh.session_id, first.session_id)
        self.assertTrue(await self.drain())
        self.assertFalse(self.recovery())
        await fresh.detach()
        self.store.messages_uncertain()
        self.service = Service(self.store, FakeTelegram(), self.chat.runner, self.chat.cwd)
        attached = await self.service.start_session('chat')
        self.chat.parents.append(attached)
        stale = self.chat.persisted(answered)['delivered']
        await attached.stop()
        self.chat.scenario(unique_sessions=True, crash=True)
        later = self.chat.row('LATER-CRASHES')
        await self.service.feed_once()
        await self.current().wait_closed()
        resurrected = self.chat.persisted(answered)['delivered']
        self.expire()
        self.chat.scenario(unique_sessions=True)
        await self.service.start_session('chat')
        self.assertTrue(await self.drain())
        probe('p1b', answered_after_restart=stale, answered_after_crash=resurrected,
              answered_sent_times=self.tagged(answered), overflowing_sent=self.tagged(overflowing),
              later_sent=self.tagged(later))
        self.assertEqual(self.tagged(answered), 1, 'an answered owner message was delivered to the parent again')

    async def test_p2_catchup_host_dead_across_restart_never_delivers_again(self):
        self.chat.scenario(unique_sessions=True, context_overflow=True)
        await self.service.start_session('chat')
        owner = self.chat.row('OVERFLOWS')
        await self.service.feed_once()
        await self.current().wait_closed()
        self.assertEqual(self.recovery()['phase'], 'fresh')
        self.expire()
        release = self.chat.root / 'release-p2'
        self.chat.scenario(unique_sessions=True, release=str(release))
        fresh = await self.service.start_session('chat')
        await self.service.feed_once()
        await until(lambda: len(self.chat.calls('session/prompt')) == 2)
        self.assertEqual(self.recovery()['phase'], 'catchup')
        directory = fresh.process.directory
        await fresh.detach()
        host_pid = int((directory / 'pid').read_text())
        child_pid = int((directory / 'child.pid').read_text())
        os.kill(host_pid, signal.SIGKILL)
        try:
            os.killpg(child_pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        await until(lambda: not (directory / 'exit.json').exists() and _dead(host_pid) and _dead(child_pid))
        self.store.messages_uncertain()
        self.service = Service(self.store, FakeTelegram(), self.chat.runner, self.chat.cwd)
        self.chat.scenario(unique_sessions=True)
        relaunched = await self.service.start_session('chat')
        prompts_before = len(self.chat.calls('session/prompt'))
        wake = self.chat.row('NEW-OWNER-MESSAGE-AFTER-RESTART')
        delivered = await self.drain(rounds=50)
        probe('p2', relaunched=relaunched is not None, phase=self.recovery().get('phase'),
              prompts_before=prompts_before, prompts_after=len(self.chat.calls('session/prompt')),
              owner_state=self.chat.persisted(owner)['delivered'], wake_state=self.chat.persisted(wake)['delivered'],
              drained=delivered)
        self.assertEqual(self.chat.persisted(wake)['delivered'], 'received',
                         'a new owner message never reaches the parent after the restart')

    async def test_p3_account_switch_during_backoff_keeps_switch_notice(self):
        self.chat.scenario(unique_sessions=True)
        first = await self.service.start_session('chat')
        self.assertTrue(await self.drain())
        await first.stop()
        self.chat.scenario(unique_sessions=True, crash=True)
        row = self.chat.row('CRASHES')
        await self.service.feed_once()
        await self.current().wait_closed()
        self.store.put('opencode_active_account', 'two')
        self.chat.scenario(unique_sessions=True)
        moved = None
        for _ in range(3):
            moved = await self.service.start_session('chat')
            if moved is not None:
                break
        self.assertIsNotNone(moved)
        self.assertTrue(await self.drain())
        notices = [text for text in self.prompts() if 'This chat moved from OpenCode to OpenCode' in text]
        reports = [item['text'] for item in self.store.db.execute('SELECT text FROM outbox')
                   if 'now runs on OpenCode' in item['text']]
        probe('p3', switched=bool(getattr(moved, 'switched', False)), switch_notice_prompts=len(notices),
              now_runs_reports=len(reports), leftover_switch=self.store.get(self.key + '_switch') is not None,
              row_sent_times=self.tagged(row), account=moved.account_alias)
        self.assertEqual(len(reports), 1, 'the owner was not told the chat moved to the other account')
        self.assertEqual(len(notices), 1, 'the new session got no continuity notice')

    async def test_p4_provider_error_resends_rows_already_in_native_history(self):
        from coordinator import opencode_runtime
        self.chat.scenario(unique_sessions=True, http_error=402)
        await self.service.start_session('chat')
        row = self.chat.row('HITS-402-THEN-OWNER-TOPS-UP')
        await self.service.feed_once()
        failed = self.current()
        await failed.wait_closed()
        in_history = opencode_runtime.parent_receipts(self.chat.data, failed.session_id, [row['id']])
        recorded = self.chat.persisted(row)['delivered']
        probe('p4-failure', failure=failed._failure, protocol_failure=failed.protocol.failure,
              recovery={k: v for k, v in self.recovery().items() if k in ('code', 'phase', 'attempts', 'rows', 'notice')},
              problems=[r['detail'] for r in self.store.db.execute("SELECT detail FROM problems WHERE code='session-closed'")])
        self.chat.scenario(unique_sessions=True)
        self.store.put('opencode_account_blocks', {})
        self.store.put(self.service.parent_pause_key('chat') + '_retry_at', None)
        for _ in range(3):
            self.expire()
            if await self.service.start_session('chat') is not None:
                break
        self.assertTrue(await self.drain())
        callbacks = [text for text in self.prompts() if 'Your last turn stopped before it finished' in text]
        self.assertEqual(self.tagged(row), 1)
        self.assertEqual(len(callbacks), 1)
        probe('p4', in_native_history_after_failure=sorted(in_history), state_after_failure=recorded,
              row_sent_times=self.tagged(row), lost_turn_callbacks=len(callbacks), prompts=self.tags())

    async def test_p6_backoff_poll_spawns_version_checks(self):
        self.chat.scenario(unique_sessions=True, crash=True)
        await self.service.start_session('chat')
        self.chat.row('CRASHES')
        await self.service.feed_once()
        await self.current().wait_closed()
        self.assertGreater(self.recovery()['retry_at'], time.time() + 20)
        for index in range(6):
            self.chat.row('worker result %d' % index, 'worker_result')
        real = opencode_accounts._check_binary
        calls = []

        def counted(binary):
            calls.append(time.monotonic())
            return real(binary)
        with patch.object(opencode_accounts, '_check_binary', side_effect=counted):
            started = time.monotonic()
            try:
                await asyncio.wait_for(self.service.feed(), 3)
            except asyncio.TimeoutError:
                pass
            elapsed = time.monotonic() - started
        self.assertLessEqual(len(calls), 1)
        probe('p6', pending_rows=len([r for r in self.store.messages_pending() if r['topic'] == 'chat']),
              version_spawns=len(calls), seconds=round(elapsed, 2), per_second=round(len(calls) / elapsed, 1))

    async def overflow_then_catchup_held(self, release):
        self.chat.scenario(unique_sessions=True, context_overflow=True)
        await self.service.start_session('chat')
        owner = self.chat.row('OVERFLOWS')
        await self.service.feed_once()
        await self.current().wait_closed()
        self.expire()
        self.chat.scenario(unique_sessions=True, release=str(release))
        fresh = await self.service.start_session('chat')
        await self.service.feed_once()
        await until(lambda: len(self.chat.calls('session/prompt')) == 2)
        return owner, fresh

    async def test_p2b_restart_mid_catchup_with_live_host_recovers(self):
        release = self.chat.root / 'release-p2b'
        owner, fresh = await self.overflow_then_catchup_held(release)
        self.assertEqual(self.recovery()['phase'], 'catchup')
        await fresh.detach()
        self.store.messages_uncertain()
        self.service = Service(self.store, FakeTelegram(), self.chat.runner, self.chat.cwd)
        attached = await self.service.start_session('chat')
        release.touch()
        wake = self.chat.row('NEW-OWNER-MESSAGE-AFTER-RESTART')
        drained = await self.drain(rounds=80)
        self.assertTrue(drained)
        self.assertEqual(attached.host_id, fresh.host_id)
        self.assertEqual(self.chat.persisted(wake)['delivered'], 'received')
        probe('p2b', attached_same_host=attached.host_id == fresh.host_id, phase=self.recovery().get('phase'),
              owner=self.chat.persisted(owner)['delivered'], wake=self.chat.persisted(wake)['delivered'], drained=drained)

    async def test_p2c_restart_mid_catchup_with_exited_host_pauses(self):
        release = self.chat.root / 'release-p2c'
        owner, fresh = await self.overflow_then_catchup_held(release)
        directory = fresh.process.directory
        await fresh.detach()
        from coordinator.host import HostClient
        await HostClient(directory).stop()
        self.store.messages_uncertain()
        self.service = Service(self.store, FakeTelegram(), self.chat.runner, self.chat.cwd)
        self.chat.scenario(unique_sessions=True)
        turn_before = self.store.get(self.key + '_turn')
        first = await self.service.start_session('chat')
        if first is not None:
            self.chat.parents.append(first)
            try:
                await asyncio.wait_for(first.wait_closed(), 30)
            except asyncio.TimeoutError:
                pass
        for _ in range(20):
            await self.service.feed_once()
            await asyncio.sleep(0.05)
        reports = [item['text'] for item in self.store.db.execute('SELECT text FROM outbox')]
        probe('p2c-detail', first_none=first is None, same_host=getattr(first, 'host_id', None) == fresh.host_id,
              first_closed=getattr(first, 'closed', None), turn_before=turn_before,
              turn_after=self.store.get(self.key + '_turn'), failure=getattr(first, '_failure', None),
              problems=[r['code'] + ':' + (r['detail'] or '')[:80] for r in self.store.db.execute('SELECT code,detail FROM problems')],
              prompts=self.tags(), current_is_first=self.current() is first,
              current_closed=getattr(self.current(), 'closed', None))
        self.service.parent_retry.clear()
        wake = self.chat.row('NEW-OWNER-MESSAGE-AFTER-RETRY')
        drained = await self.drain(rounds=50)
        probe('p2c-after-retry', drained=drained, phase=self.recovery().get('phase'), prompts=self.tags(),
              wake=self.chat.persisted(wake)['delivered'], owner=self.chat.persisted(owner)['delivered'],
              pending=[(r['id'], r['kind']) for r in self.store.messages_pending() if r['topic'] == 'chat'],
              session_running=bool(self.current() and not self.current().closed))
        self.assertEqual(sum('fresh catch-up failed' in item for item in reports), 1)
        probe('p2c', exit_json=(directory / 'exit.json').exists(), phase=self.recovery().get('phase'),
              paused_reports=sum('fresh catch-up failed' in text for text in reports),
              owner=self.chat.persisted(owner)['delivered'])
        self.assertEqual(self.chat.persisted(wake)['delivered'], 'received',
                         'a new owner message never reaches the parent after the restart')

    async def test_p7_weekly_limit_rechecks_then_quota_clears(self):
        self.chat.scenario(unique_sessions=True, context_overflow=True, provider_error=WEEKLY)
        await self.service.start_session('chat')
        owner = self.chat.row('ASKS-DURING-WEEKLY-LIMIT')
        await self.service.feed_once()
        first = self.current()
        await first.wait_closed()
        block = dict(self.store.get('opencode_account_blocks')['one'])
        rechecks = 0
        for cycle in range(3):
            blocks = self.store.get('opencode_account_blocks')
            blocks['one']['until'] = time.time() - 1
            self.store.put('opencode_account_blocks', blocks)
            self.expire()
            self.chat.scenario(unique_sessions=True, context_overflow=True, provider_error=WEEKLY)
            session = None
            for _ in range(3):
                session = await self.service.start_session('chat')
                if session is not None:
                    break
            if session is None:
                continue
            rechecks += 1
            for _ in range(100):
                await self.service.feed_once()
                if session.closed:
                    break
                await asyncio.sleep(0.05)
            await asyncio.wait_for(session.wait_closed(), 30)
        sent_while_limited = self.tagged(owner)
        blocks = self.store.get('opencode_account_blocks')
        blocks['one']['until'] = time.time() - 1
        self.store.put('opencode_account_blocks', blocks)
        self.expire()
        self.chat.scenario(unique_sessions=True)
        resumed = None
        for _ in range(3):
            resumed = await self.service.start_session('chat')
            if resumed is not None:
                break
        drained = await self.drain()
        reports = [item['text'] for item in self.store.db.execute('SELECT text FROM outbox')]
        native_copies = len([text for text in json.loads(
            (self.chat.data / 'data' / 'fake-sessions.json').read_text())[first.session_id]
            if ('message=%d kind=owner' % owner['id']) in text])
        self.assertTrue(block['reset_known'])
        self.assertEqual(block['until'], WEEKLY['reset_at'])
        self.assertEqual(self.tagged(owner), 1)
        self.assertEqual(native_copies, 1)
        self.assertEqual(sum('paid balance use after limits' in text for text in reports), 1)
        self.assertEqual(len(self.chat.calls('session/new')), 1)
        self.assertFalse(self.recovery())
        probe('p7', block_until_minus_observed=round(block['until'] - block['observed_at']), block_reset_at=block.get('reset_at'),
              reset_known=block.get('reset_known'), rechecks=rechecks, owner_sent_while_limited=sent_while_limited,
              owner_copies_in_native_session=native_copies, session_new=len(self.chat.calls('session/new')),
              usage_notices=sum('paid balance use after limits' in text for text in reports), drained=drained,
              recovery_after=self.recovery(), owner_final=self.chat.persisted(owner)['delivered'],
              pause_notice_left=self.store.get(self.service.parent_pause_key('chat') + '_notice') is not None)

    async def test_owner_arriving_after_error_before_monitor_ends_backoff(self):
        self.chat.scenario(unique_sessions=True, crash=True)
        session = await self.service.start_session('chat')
        row = self.chat.row('Fails while owner is writing')
        real = opencode_recovery.failed

        def receive_owner(*args, **kwargs):
            self.chat.row('Owner arrives during failure handling')
            return real(*args, **kwargs)
        with patch.object(opencode_recovery, 'failed', side_effect=receive_owner):
            await self.chat.send(session, row)
            await session.wait_closed()
        self.chat.scenario(unique_sessions=True)
        self.assertIsNotNone(await self.service.start_session('chat'))

    async def test_removed_account_during_weekly_failure_is_safe(self):
        session = await self.service.start_session('chat')
        row = self.chat.row('Account removed during prompt')
        session._track({row['id']})
        session.error = {'error': 'synthetic'}
        session._provider_error = dict(WEEKLY)
        session._failure = 'usage_limited'
        self.store.put('opencode_accounts', {'two': self.store.get('opencode_accounts')['two']})
        await session.stop()

    async def test_one_callback_names_every_recorded_row(self):
        self.chat.scenario(unique_sessions=True)
        session = await self.service.start_session('chat')
        rows = [self.chat.row('Unfinished %d' % i) for i in range(40)]
        from coordinator import opencode_runtime
        with patch.object(opencode_runtime, 'parent_receipts', return_value={r['id'] for r in rows}):
            opencode_recovery.failed(self.store, self.key, session.session_id, 'disconnected', 'fake',
                                     [row['id'] for row in rows])
        notice = self.store.db.execute('SELECT text FROM messages WHERE id=?',
                                      (self.recovery()['notice'],)).fetchone()[0]
        for row in rows:
            self.assertIn('message=%d' % row['id'], notice)

    async def test_already_superseded_notices_keep_their_timestamp(self):
        old = self.chat.row('Old noise', 'restarted')
        self.store.db.execute("UPDATE messages SET delivered='received',receipt='superseded',updated=1 WHERE id=?", (old['id'],))
        row = self.chat.row('Pending')
        opencode_recovery.unanswered(self.store, self.key, [row['id']])
        self.assertEqual(self.chat.persisted(old)['updated'], 1)


    async def test_completed_catchup_exited_offline_resumes_without_a_failure_notice(self):
        release = self.chat.root / 'release-completed-offline'
        owner, fresh = await self.overflow_then_catchup_held(release)
        directory = fresh.process.directory
        await fresh.detach()
        release.touch()
        from coordinator.host import HostClient
        host = HostClient(directory)
        await until(lambda: any('stopReason' in raw for raw in host.history_after(0)))
        await host.stop()
        self.store.messages_uncertain()
        self.service = Service(self.store, FakeTelegram(), self.chat.runner, self.chat.cwd)
        self.chat.scenario(unique_sessions=True)
        await self.service.start_session('chat')
        self.service.parent_retry.clear()
        self.expire()
        self.assertTrue(await self.drain())
        self.assertEqual(self.chat.persisted(owner)['delivered'], 'received')
        self.assertFalse(self.recovery())
        self.assertEqual(len(self.chat.calls('session/new')), 2)
        self.assertFalse(any('fresh catch-up failed' in row['text']
                             for row in self.store.db.execute('SELECT text FROM outbox')))


    async def test_legacy_rotation_preserves_answered_receipts_from_older_native_session(self):
        self.chat.scenario(unique_sessions=True)
        first = await self.service.start_session('chat')
        answered = self.chat.row('Answered before the legacy session rotation')
        self.assertTrue(await self.drain())
        await first.stop()
        self.store.put(self.key + '_session', None)
        self.store.put(self.key + '_opencode', None)
        self.chat.scenario(unique_sessions=True, context_overflow=True)
        failing = self.chat.row('Fails after legacy rotation')
        await self.service.feed_once()
        await self.current().wait_closed()
        self.store.messages_uncertain()
        self.assertEqual(self.chat.persisted(answered)['delivered'], 'uncertain')
        self.service = Service(self.store, FakeTelegram(), self.chat.runner, self.chat.cwd)
        self.expire()
        self.chat.scenario(unique_sessions=True)
        await self.service.start_session('chat')
        self.assertTrue(await self.drain())
        self.assertEqual(self.tagged(answered), 1)
        self.assertEqual(self.chat.persisted(failing)['delivered'], 'received')


    async def test_attach_failure_keeps_the_original_start_error(self):
        with patch('coordinator.parent_state.attach', side_effect=RuntimeError('synthetic attach failure')):
            self.assertIsNone(await self.service.start_session('chat'))
        detail = self.store.db.execute("SELECT detail FROM problems WHERE code='start-failed' ORDER BY id DESC").fetchone()[0]
        self.assertIn('RuntimeError: synthetic attach failure', detail)
        self.assertNotIn('UnboundLocalError', detail)


class RecoveryBoundsTests(unittest.TestCase):
    def setUp(self):
        self.chat = fixtures.OpenCodeParentTests()
        self.chat.addCleanup = self.addCleanup
        self.chat.setUp()
        self.store = self.chat.store
        self.key = self.chat.session_key

    def catchup(self, count, text):
        rows = [self.chat.row(text) for _ in range(count)]
        notice = opencode_recovery.catchup(self.store, self.key, {'rows': [r['id'] for r in rows]})
        return self.chat.persisted({'id': notice})['text']

    def previews(self, text):
        return json.loads(text.split('. Unanswered messages: ', 1)[1])['shown']

    def test_catchup_total_never_exceeds_eight_thousand(self):
        for index in range(20):
            self.store.task_create('chat', 'Job %d ' % index + 'x' * 3000, notes='y' * 6000)
        text = self.catchup(10, 'z' * 2000)
        self.assertLessEqual(len(text), 8000)
        self.assertIn('Open jobs:', text)

    def test_catchup_preview_is_cut_at_five_hundred(self):
        shown = self.previews(self.catchup(1, 'x' * 900))
        self.assertEqual(len(shown), 1)
        self.assertEqual(shown[0]['preview'], 'x' * 500)

    def test_catchup_preview_json_stays_within_four_thousand(self):
        shown = self.previews(self.catchup(10, 'x' * 500))
        self.assertGreaterEqual(len(shown), 1)
        self.assertLessEqual(len(json.dumps(shown)), 4000)

    def test_uncertain_unrecorded_row_is_requeued(self):
        row = self.chat.row('Uncertain with no native receipt')
        self.store.message_delivered(row['id'], 'uncertain')
        self.assertEqual(opencode_recovery.unanswered(self.store, self.key, []), [row['id']])
        self.assertEqual(self.chat.persisted(row)['delivered'], 'pending')

    def test_ready_checks_version_once_and_rechecks_replaced_binary(self):
        real = opencode_accounts._check_binary
        with patch.object(opencode_accounts, '_check_binary', wraps=real) as check:
            for _ in range(8):
                opencode_accounts.check_ready(self.store)
            self.assertEqual(check.call_count, 1)
            self.chat.binary.write_text(self.chat.binary.read_text() + '\n')
            opencode_accounts.check_ready(self.store)
            self.assertEqual(check.call_count, 2)
