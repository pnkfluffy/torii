import asyncio
import io
import json
import logging
import os
import time
import unittest
from unittest.mock import patch

from coordinator import envelopes
from coordinator.failures import TaskFailure
from coordinator.service import Service
from coordinator.store import Store
from tests.test_envelopes import BOT, OWNER, TOPIC, VALUE, IntakeFixture, private


def prompt(name):
    return ('Send the value for ' + name +
            ' now in one message. I will delete it after I store it. Send /cancel to stop.')


class ValueDateFixture(IntakeFixture):
    def setUp(self):
        super().setUp()
        self.number = 0
        self.capture = io.StringIO()
        handler = logging.StreamHandler(self.capture)
        root = logging.getLogger()
        self.addCleanup(root.removeHandler, handler)
        self.addCleanup(root.setLevel, root.level)
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)

    def send(self, text, date=1800000001):
        self.number += 1
        extra = {} if date is None else {'date': date}
        return self.store.accept(private(self.number, text, **extra))

    def loop(self):
        async def run():
            service = Service(self.store, self.telegram, object(), self.root, vault=self.vault)
            await service.envelopes_once()
            while await service.deliver_once():
                pass
        asyncio.run(run())

    def expire(self):
        with self.store.db:
            self.store.db.execute("UPDATE envelopes SET expires=created-1 WHERE name='GITHUB_TOKEN'")
        self.telegram.date = 1800000010

    def reopen(self):
        self.store.close()
        self.store = Store(self.root / 'state')
        self.store.vault = self.vault

    def worker(self, name):
        task = self.store.task_create(TOPIC, 'Uses ' + name, secrets=[name])
        return envelopes.load_for_worker(self.store, self.vault, task)[0]

    def assert_nowhere(self, value):
        self.assertNotIn(value, self.vault.values.values())
        for name in ('GITHUB_TOKEN', 'NPM_TOKEN'):
            if not self.store.db.execute('SELECT 1 FROM envelopes WHERE name=?', (name,)).fetchone():
                continue
            try:
                values = self.worker(name)
            except (envelopes.SecretsPending, TaskFailure):
                pass
            else:
                self.assertNotIn(value, values.values())
        for row in self.store.db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall():
            for record in self.store.db.execute('SELECT * FROM "' + row[0] + '"'):
                self.assertNotIn(value, json.dumps(list(record), default=str), row[0])
        self.assertNotIn(value, self.capture.getvalue())
        self.assertNotIn(value, json.dumps(self.telegram.calls, default=str))

    def assert_refused(self, value, date=1800000001, deliver=True):
        before = self.store.db.execute("SELECT * FROM envelopes WHERE state='armed' AND expires>?",
                                       (time.time(),)).fetchone()
        calls = list(self.vault.calls)
        self.assertEqual(self.send(value, date), 'envelope_reject')
        self.assertEqual(self.vault.calls, calls)
        if before:
            after = self.envelope(before['name'])
            for key in ('state', 'armed_at', 'expires'):
                self.assertEqual(after[key], before[key], key)
        self.assertEqual(self.deletes()[-1], 100 + self.number)
        reply = self.replies()[-1]
        self.assertIn('deleted', reply)
        self.assertNotRegex(reply, r'\d')
        self.assertNotIn('length', reply.lower())
        self.assertNotIn('fingerprint', reply.lower())
        self.assertNotIn(self.vault.fingerprint(value), reply)
        if deliver:
            self.loop()
            self.assertIn(('deleteMessage', {'chat_id': OWNER, 'message_id': 100 + self.number}), self.telegram.calls)
        self.assert_nowhere(value)

    def assert_no_misfile(self):
        self.assertEqual(self.vault.values, {})
        self.assertFalse(any(' stored.' in text for text in self.replies()))
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM envelopes WHERE state='filled'").fetchone()[0], 0)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM messages WHERE kind='secret_filled'").fetchone()[0], 0)
        self.assertFalse(any('Filled.' in row[0] for row in self.store.db.execute('SELECT text FROM outbox')))
        self.assert_nowhere(VALUE)


class ValueDateRaceTests(ValueDateFixture):
    def begin(self, waiter=True):
        self.ask()
        if waiter:
            self.ask('NPM_TOKEN')
        self.loop()

    def finish(self, value=VALUE):
        self.loop()
        self.assert_refused(value)
        self.assert_no_misfile()

    def test_R0_start_read_before_expiry_then_value_after(self):
        self.begin()
        self.assertEqual(self.send('/start'), 'envelope_arm')
        self.expire()
        self.finish()
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'open')

    def test_R1_start_and_value_in_flight_across_expiry(self):
        self.begin()
        self.expire()
        self.assertEqual(self.send('/start'), 'envelope_arm')
        self.finish()

    def test_R2_loop_sweeps_between_sending_and_reading_start(self):
        self.begin()
        self.expire()
        self.loop()
        self.assertEqual(self.send('/start'), 'envelope_arm')
        self.finish()

    def test_R3_new_ask_sweeps_then_start_and_value_in_flight(self):
        self.begin(waiter=False)
        self.expire()
        self.ask('NPM_TOKEN')
        self.assertEqual(self.send('/start'), 'envelope_arm')
        self.finish()

    def test_R4_restart_with_start_and_value_queued(self):
        self.begin()
        self.expire()
        self.reopen()
        self.loop()
        self.assertEqual(self.send('/start'), 'envelope_arm')
        self.finish()

    def test_R5_start_read_then_new_ask_then_value(self):
        self.begin(waiter=False)
        self.expire()
        self.assertEqual(self.send('/start'), 'envelope_start')
        self.ask('NPM_TOKEN')
        self.finish()

    def test_R6_refused_paste_then_start_poke_and_second_paste(self):
        self.begin()
        self.expire()
        self.assert_refused(VALUE, deliver=False)
        self.assertEqual(self.send('/start'), 'envelope_arm')
        self.finish(VALUE + '-second-paste')


class ValueDateBoundaryTests(ValueDateFixture):
    def test_same_second_is_refused(self):
        self.ask()
        self.loop()
        self.assert_refused(VALUE, self.telegram.date)

    def test_earlier_second_is_refused(self):
        self.ask()
        self.loop()
        self.assert_refused(VALUE, self.telegram.date - 1)

    def test_undelivered_prompt_is_refused(self):
        self.ask()
        self.assert_refused(VALUE, self.telegram.date + 1)

    def test_owner_message_without_date_is_refused(self):
        self.ask()
        self.loop()
        self.assert_refused(VALUE, None)

    def test_invalid_owner_dates_are_refused(self):
        self.ask()
        self.loop()
        for date in (True, '1800000001', 1800000001.0, -1):
            with self.subTest(date=date):
                self.assert_refused(VALUE, date)

    def test_prompt_without_date_is_refused_until_a_dated_resend(self):
        self.telegram.date = None
        self.ask()
        self.loop()
        self.assert_refused(VALUE, 1800000001)
        self.telegram.date = 1800000010
        self.assertEqual(self.send('/start', None), 'envelope_arm')
        self.loop()
        self.assertEqual(self.send(VALUE + '-retry', 1800000011), 'envelope_fill')

    def test_one_second_later_is_stored(self):
        self.ask()
        self.loop()
        self.assertEqual(self.send(VALUE, self.telegram.date + 1), 'envelope_fill')
        self.assertEqual(self.vault.values, {'GITHUB_TOKEN': VALUE})
        self.assertEqual(self.worker('GITHUB_TOKEN'), {'GITHUB_TOKEN': VALUE})
        self.assertEqual(self.envelope()['state'], 'filled')
        self.assertTrue(self.replies()[-1].startswith('GITHUB_TOKEN stored.'))
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM messages WHERE kind='secret_filled'").fetchone()[0], 1)

    def test_prompt_date_survives_restart(self):
        self.ask()
        self.loop()
        self.reopen()
        self.assert_refused(VALUE, self.telegram.date)
        self.assertEqual(self.send(VALUE + '-later', self.telegram.date + 1), 'envelope_fill')

    def test_resending_prompt_preserves_the_first_delivered_date(self):
        self.ask()
        self.loop()
        self.telegram.date += 10
        self.assertEqual(self.send('/start', None), 'envelope_arm')
        self.loop()
        self.assertEqual(self.send(VALUE, self.telegram.date - 9), 'envelope_fill')


class IntakeClockTests(ValueDateFixture):
    def test_waiting_reply_uses_intake_now(self):
        self.ask()
        self.ask('NPM_TOKEN')
        with self.store.db:
            envelopes.close(self.store, self.envelope(), 'cancelled', 'cancel', source='test')
            self.store.db.execute("UPDATE envelopes SET expires=? WHERE name='NPM_TOKEN'", (2000000005,))
        message = private(1, '/cancel')['message']
        with patch('coordinator.envelopes.time.time', return_value=2000000000):
            intake = envelopes.prepare_private(self.store, message)
        with patch('coordinator.envelopes.time.time', return_value=2000000010), self.store.db:
            self.assertEqual(envelopes.accept_private(self.store, message, intake), 'envelope_reject')
        self.assertEqual(self.replies()[-1],
                         'No credential ask is taking a value. Another ask is waiting. Tap Fill privately on its card.')

    def test_waiting_card_skips_expired_armed_asks(self):
        self.ask()
        self.ask('NPM_TOKEN')
        with patch('coordinator.envelopes.time.time', return_value=self.envelope()['expires'] + 1):
            text = envelopes.ask_text(self.store, self.envelope('NPM_TOKEN'))
        self.assertNotIn('behind another', text)
        self.assertIn('Tap Fill privately within 10 minutes', text)


class DeliveredIntakeFixture(IntakeFixture):
    def dated_update(self, number, text=None, **extra):
        if not extra and text not in ('/start', '/cancel') and not (text or '').startswith('/start '):
            self.run_effects()
        return private(number, text, date=self.telegram.date + 1, **extra)


class DirectPromptTests(DeliveredIntakeFixture):
    def accept(self, number, text=None, **extra):
        message = self.dated_update(number, text, **extra)['message']
        intake = envelopes.prepare_private(self.store, message)
        with self.store.db:
            return envelopes.accept_private(self.store, message, intake)

    def assert_fills_next(self, number=50):
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'armed')
        self.assertEqual(self.replies()[-1], prompt('NPM_TOKEN'))
        self.assertEqual(self.accept(number, VALUE), 'envelope_fill')
        self.assertEqual(self.vault.values['NPM_TOKEN'], VALUE)
        self.assertNotIn('GITHUB_TOKEN', self.vault.values)
        self.assert_value_absent()

    def test_T2_ask_prompts_directly_and_the_next_private_message_fills(self):
        task = self.store.task_create(TOPIC, 'Release')
        self.assertTrue(self.ask(task=task['id']).ok)
        envelope = self.envelope()
        self.assertEqual((envelope['state'], envelope['chat']), ('armed', OWNER))
        self.assertEqual(self.effects(), [('reply', OWNER, None, prompt('GITHUB_TOKEN'), 'pending')])
        card = self.outbox(envelope['card_outbox'])
        self.assertEqual(self.count('outbox'), 1)
        self.assertIn('Waiting for the value in the private chat.', card['text'])
        fill, cancel = card['markup']['inline_keyboard']
        self.assertEqual(fill, [{'text': 'Fill privately', 'url': 'https://t.me/' + BOT}])
        self.assertEqual(cancel, [{'text': 'Cancel', 'callback_data': 'envelope:%d:cancel' % envelope['id']}])
        self.assertEqual(self.accept(1, VALUE), 'envelope_fill')
        self.assertEqual(self.vault.values, {'GITHUB_TOKEN': VALUE})
        self.assertEqual(self.deletes(), [101])
        self.assertEqual(self.replies()[-1], 'GITHUB_TOKEN stored. Length %d, fingerprint %s.' %
                         (len(VALUE), self.vault.fingerprint(VALUE)))
        notice, = self.store.db.execute("SELECT * FROM messages WHERE kind='secret_filled'").fetchall()
        self.assertEqual((notice['topic'], notice['source_envelope'], notice['text']),
                         (TOPIC, envelope['id'], 'Secret GITHUB_TOKEN, declared on job 1, is filled.'))
        self.assert_value_absent()
        self.run_effects()
        self.assertEqual([method for method, _ in self.telegram.calls],
                         ['sendMessage', 'deleteMessage', 'sendMessage'])
        self.assertTrue(all(row[4] == 'done' for row in self.effects()))

    def test_T3_waits_and_fills_each_name_in_oldest_first_order(self):
        for name in ('GITHUB_TOKEN', 'NPM_TOKEN', 'DEPLOY_TOKEN'):
            self.assertTrue(self.ask(name).ok)
        self.assertEqual(self.replies(), [prompt('GITHUB_TOKEN')])
        for name in ('NPM_TOKEN', 'DEPLOY_TOKEN'):
            self.assertEqual(self.envelope(name)['state'], 'open')
            card = self.outbox(self.envelope(name)['card_outbox'])
            self.assertIn('Waiting behind another credential ask', card['text'])
            self.assertEqual(card['markup']['inline_keyboard'][0],
                             [{'text': 'Fill privately', 'callback_data': 'envelope:%d:fill' % self.envelope(name)['id']}])
            self.assertEqual(card['markup']['inline_keyboard'][1],
                             [{'text': 'Cancel', 'callback_data': 'envelope:%d:cancel' % self.envelope(name)['id']}])
        for number, name in enumerate(('GITHUB_TOKEN', 'NPM_TOKEN', 'DEPLOY_TOKEN'), 1):
            self.assertEqual(self.envelope(name)['state'], 'armed')
            self.assertEqual(self.replies()[-1], prompt(name))
            self.assertEqual(self.accept(number, VALUE + name), 'envelope_fill')
            self.assertEqual(self.envelope(name)['state'], 'filled')
        self.assertEqual(self.vault.values, {name: VALUE + name for name in
                                          ('GITHUB_TOKEN', 'NPM_TOKEN', 'DEPLOY_TOKEN')})
        self.assertEqual(self.deletes(), [101, 102, 103])
        for name in self.vault.values:
            self.assert_value_absent(VALUE + name)

    def test_T4_private_cancel_prompts_the_next_waiter(self):
        self.ask()
        self.ask('NPM_TOKEN')
        self.assertEqual(self.accept(1, '/cancel'), 'envelope_cancel')
        self.assertEqual(self.envelope()['state'], 'cancelled')
        self.assertEqual(self.replies()[-2:], ['Cancelled GITHUB_TOKEN.', prompt('NPM_TOKEN')])
        self.assert_fills_next()

    def test_T4_card_cancel_prompts_the_next_waiter(self):
        self.ask()
        self.ask('NPM_TOKEN')
        self.assertEqual(self.click_cancel(1, self.envelope()), 'envelope_cancel')
        self.assertEqual(self.envelope()['state'], 'cancelled')
        self.assert_fills_next()

    def expire_first(self):
        with self.store.db:
            self.store.db.execute('UPDATE envelopes SET expires=? WHERE name=?',
                                  (self.envelope()['created'] - 1, 'GITHUB_TOKEN'))

    def service(self):
        return Service(self.store, self.telegram, object(), self.root, vault=self.vault)

    def assert_waits_for_start(self, state):
        self.assertEqual(self.envelope()['state'], state)
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'open')
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM envelopes WHERE state='armed'").fetchone()[0], 0)
        notice = ("GITHUB_TOKEN %s, so I won't take a value for it. "
                  'To give the next one, tap Fill privately on its card.') % state
        self.assertEqual(self.replies()[-1], notice)
        self.run_effects()
        self.assertIn(('sendMessage', {'chat_id': OWNER, 'text': notice}), self.telegram.calls)
        self.assertEqual(self.accept(40, VALUE + '-for-github'), 'envelope_reject')
        self.assertEqual(self.vault.values, {})
        self.assertNotIn(prompt('NPM_TOKEN'), self.replies())
        self.assertEqual(self.accept(41, '/start'), 'envelope_arm')
        self.assert_fills_next()

    def test_T4_fill_prompts_the_next_waiter(self):
        self.ask()
        self.ask('NPM_TOKEN')
        self.assertEqual(self.accept(1, VALUE + '-first'), 'envelope_fill')
        self.assertEqual(self.replies()[-1], prompt('NPM_TOKEN'))
        self.assertEqual(self.accept(2, VALUE + '-second'), 'envelope_fill')
        self.assertEqual(self.vault.values, {'GITHUB_TOKEN': VALUE + '-first', 'NPM_TOKEN': VALUE + '-second'})

    def test_T4_expiry_waits_for_start_before_prompting_the_next_waiter(self):
        self.ask()
        self.ask('NPM_TOKEN')
        self.expire_first()
        with self.store.db:
            self.assertEqual(envelopes.sweep(self.store), 1)
        self.assert_waits_for_start('expired')
        with self.store.db:
            self.assertEqual(envelopes.sweep(self.store), 0)

    def test_failed_rotate_preserves_the_armed_request_and_waiters(self):
        self.ask()
        self.ask('NPM_TOKEN')
        before = self.effects()
        with patch.dict(os.environ, {'GITHUB_TOKEN': 'fixture-only'}):
            self.assertEqual(self.call('secret.rotate', name='GITHUB_TOKEN').state, 'refused')
        self.assertEqual(self.envelope()['state'], 'armed')
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'open')
        self.assertEqual(self.effects(), before)

    def test_T4_service_revoke_waits_for_start_before_prompting_the_next_waiter(self):
        self.ask()
        self.ask('NPM_TOKEN')
        self.assertTrue(self.call('secret.revoke', name='GITHUB_TOKEN').ok)
        asyncio.run(self.service().controls_once())
        self.assert_waits_for_start('revoked')

    def test_T9_P1_service_expiry_never_files_the_first_value_under_the_waiter(self):
        self.ask()
        self.ask('NPM_TOKEN')
        self.run_effects()
        self.expire_first()
        asyncio.run(self.service().envelopes_once())
        self.assertEqual(self.store.accept(self.dated_update(1, VALUE)), 'envelope_reject')
        self.assertEqual(self.vault.values, {})
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'open')
        self.assertEqual(self.store.accept(self.dated_update(2, '/start')), 'envelope_arm')
        self.assertEqual(self.store.accept(self.dated_update(3, VALUE + '-npm')), 'envelope_fill')
        self.assertEqual(self.vault.values, {'NPM_TOKEN': VALUE + '-npm'})

    def test_T9_P2_live_rotate_never_files_the_first_value_under_the_waiter(self):
        self.ask()
        self.ask('NPM_TOKEN')
        self.run_effects()
        self.assertEqual(self.call('secret.rotate', name='GITHUB_TOKEN').state, 'refused')
        self.assertEqual(self.store.accept(self.dated_update(1, VALUE)), 'envelope_fill')
        self.assertEqual(self.vault.values, {'GITHUB_TOKEN': VALUE})
        self.assertEqual(self.store.accept(self.dated_update(2, VALUE + '-npm')), 'envelope_fill')
        self.assertEqual(self.vault.values['NPM_TOKEN'], VALUE + '-npm')

    def test_T9_P3_another_ask_sweeps_expiry_without_prompting_the_waiter(self):
        self.ask()
        self.ask('NPM_TOKEN')
        self.run_effects()
        self.expire_first()
        self.assertTrue(self.ask('DEPLOY_TOKEN').ok)
        self.assertEqual(self.store.accept(self.dated_update(1, VALUE)), 'envelope_reject')
        self.assertEqual(self.vault.values, {})
        self.assertNotIn(prompt('NPM_TOKEN'), self.replies())
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'open')
        self.assertEqual(self.envelope('DEPLOY_TOKEN')['state'], 'open')
        self.assertEqual(self.store.accept(self.dated_update(2, '/start')), 'envelope_arm')
        self.assertEqual(self.replies()[-1], prompt('NPM_TOKEN'))
        self.assertEqual(self.store.accept(self.dated_update(3, VALUE + '-npm')), 'envelope_fill')
        self.assertEqual(self.vault.values, {'NPM_TOKEN': VALUE + '-npm'})

    def test_T9_P4_service_revoke_never_files_the_first_value_under_the_waiter(self):
        self.ask()
        self.ask('NPM_TOKEN')
        self.run_effects()
        self.assertTrue(self.call('secret.revoke', name='GITHUB_TOKEN').ok)
        asyncio.run(self.service().controls_once())
        self.assertEqual(self.store.accept(self.dated_update(1, VALUE)), 'envelope_reject')
        self.assertEqual(self.vault.values, {})
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'open')
        self.assertEqual(self.store.accept(self.dated_update(2, '/start')), 'envelope_arm')
        self.assertEqual(self.store.accept(self.dated_update(3, VALUE + '-npm')), 'envelope_fill')
        self.assertEqual(self.vault.values, {'NPM_TOKEN': VALUE + '-npm'})

    def test_T9_owner_away_answers_old_prompts_in_order_without_misfiling(self):
        names = ('GITHUB_TOKEN', 'NPM_TOKEN', 'DEPLOY_TOKEN')
        for name in names:
            self.assertTrue(self.ask(name).ok)
        asyncio.run(self.service().envelopes_once())
        self.expire_first()
        asyncio.run(self.service().envelopes_once())
        for number, name in enumerate(names, 1):
            self.assertEqual(self.store.accept(self.dated_update(number, VALUE + name)), 'envelope_reject')
        self.assertEqual(self.vault.values, {})
        self.assertEqual(self.replies().count(prompt('GITHUB_TOKEN')), 1)
        self.assertNotIn(prompt('NPM_TOKEN'), self.replies())
        self.assertNotIn(prompt('DEPLOY_TOKEN'), self.replies())
        self.assertEqual(self.store.accept(self.dated_update(4, '/start')), 'envelope_arm')
        for number, name in enumerate(names[1:], 5):
            self.assertEqual(self.replies()[-1], prompt(name))
            self.assertEqual(self.store.accept(self.dated_update(number, VALUE + name)), 'envelope_fill')
        self.assertEqual(self.vault.values, {name: VALUE + name for name in names[1:]})

    def test_T12_waiter_gets_ten_minutes_from_its_prompt(self):
        self.ask()
        self.ask('NPM_TOKEN')
        now = self.envelope()['created'] + 300
        with self.store.db:
            self.store.db.execute("UPDATE envelopes SET expires=? WHERE name='NPM_TOKEN'", (now + 20,))
        with patch('coordinator.envelopes.time.time', return_value=now):
            self.assertEqual(self.accept(1, '/cancel'), 'envelope_cancel')
        armed = self.envelope('NPM_TOKEN')
        self.assertEqual(armed['expires'], now + 600)
        self.assertEqual(armed['armed_at'], now)
        self.assertEqual(self.replies()[-1], prompt('NPM_TOKEN'))
        with self.store.db:
            self.assertEqual(envelopes.sweep(self.store, now + 599), 0)
            self.assertEqual(envelopes.sweep(self.store, now + 600), 1)
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'expired')
        self.assertEqual(self.replies()[-1], "NPM_TOKEN expired, so I won't take a value for it.")
        self.assertEqual(self.accept(2, VALUE), 'envelope_reject')
        self.assertEqual(self.vault.values, {})

    def test_T13_live_rotate_is_refused_and_the_queue_is_unchanged(self):
        self.ask()
        self.ask('NPM_TOKEN')
        tables = ('envelopes', 'envelope_events', 'envelope_effects', 'outbox')
        before = {table: [tuple(row) for row in self.store.db.execute('SELECT * FROM ' + table)] for table in tables}
        for name in ('GITHUB_TOKEN', 'NPM_TOKEN'):
            with self.subTest(name=name):
                result = self.call('secret.rotate', name=name)
                self.assertEqual(result.state, 'refused')
                self.assertEqual(result.text, 'An envelope for ' + name + ' is already open.')
                after = {table: [tuple(row) for row in self.store.db.execute('SELECT * FROM ' + table)] for table in tables}
                self.assertEqual(after, before)

    def test_T14_no_envelope_reply_points_to_the_card(self):
        self.assertEqual(self.accept(1, VALUE), 'envelope_reject')
        self.assertEqual(self.replies()[-1], 'No envelope is waiting for a value, so I deleted your message. '
                         'Tap Fill privately on an Envelope card first.')

    def test_cancel_after_unswept_expiry_never_cancels_the_waiter(self):
        self.ask()
        self.ask('NPM_TOKEN')
        self.expire_first()
        self.assertEqual(self.accept(1, '/cancel'), 'envelope_reject')
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'open')
        self.assertNotIn(prompt('NPM_TOKEN'), self.replies())
        self.assertEqual(self.vault.values, {})

    def test_cancel_after_service_expiry_never_cancels_the_waiter(self):
        self.ask()
        self.ask('NPM_TOKEN')
        self.expire_first()
        asyncio.run(self.service().envelopes_once())
        self.assertEqual(self.accept(1, '/cancel'), 'envelope_reject')
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'open')
        self.assertNotIn(prompt('NPM_TOKEN'), self.replies())

    def test_start_after_unswept_expiry_prompts_the_waiter_only_once(self):
        self.ask()
        self.ask('NPM_TOKEN')
        self.expire_first()
        self.assertEqual(self.accept(1, '/start'), 'envelope_arm')
        self.assertEqual(self.replies().count(prompt('NPM_TOKEN')), 1)
        self.assertEqual(self.accept(2, VALUE + '-npm'), 'envelope_fill')
        self.assertEqual(self.vault.values, {'NPM_TOKEN': VALUE + '-npm'})

    def test_value_after_unswept_expiry_stores_nothing_and_prompts_no_waiter(self):
        self.ask()
        self.ask('NPM_TOKEN')
        self.expire_first()
        self.assertEqual(self.accept(1, VALUE), 'envelope_reject')
        self.assertEqual(self.vault.values, {})
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'open')
        self.assertNotIn(prompt('NPM_TOKEN'), self.replies())

    def test_T5_bare_start_resends_the_armed_prompt(self):
        self.ask()
        self.assertEqual(self.accept(1, '/start'), 'envelope_arm')
        self.assertEqual(self.replies(), [prompt('GITHUB_TOKEN')] * 2)
        self.assertEqual([event for event, _, _ in self.events()], ['ask', 'arm'])

    def test_T5_bare_start_arms_a_waiter_or_gives_the_original_reply(self):
        self.ask()
        self.expire_first()
        with self.store.db:
            envelopes.sweep(self.store)
        self.ask('NPM_TOKEN')
        self.assertEqual(self.accept(1, '/start'), 'envelope_arm')
        self.assertEqual(self.replies()[-1], prompt('NPM_TOKEN'))
        self.assertEqual(self.accept(2, VALUE), 'envelope_fill')
        self.assertEqual(self.accept(3, '/start'), 'envelope_start')
        self.assertEqual(self.replies()[-1], envelopes.REPLIES['bare_start'])

    def test_T7_handoff_neither_joins_nor_blocks_the_queue(self):
        with self.store.db:
            handoff = envelopes.ask(self.store, TOPIC, envelopes.HANDOFF, 'Sign in', 'Claude')
        self.assertEqual(self.effects(), [])
        self.assertEqual(self.envelope(envelopes.HANDOFF)['state'], 'open')
        card = self.outbox(self.envelope(envelopes.HANDOFF)['card_outbox'])
        self.assertEqual(card['text'], 'Sign in\n3. Paste it here as one message within 10 minutes. I delete it at once.')
        self.assertEqual(card['markup']['inline_keyboard'],
                         [[{'text': 'Cancel', 'callback_data': 'envelope:%d:cancel' % handoff}]])
        self.ask()
        self.ask('NPM_TOKEN')
        self.accept(1, VALUE)
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'armed')
        self.assertEqual(self.envelope(envelopes.HANDOFF)['state'], 'open')
        self.accept(2, VALUE + '-second')
        self.assertEqual(self.replies()[-1].split(' stored.')[0], 'NPM_TOKEN')
        self.assertEqual(self.envelope(envelopes.HANDOFF)['state'], 'open')
        self.assertEqual([reply for reply in self.replies() if reply.startswith('Send')],
                         [prompt('GITHUB_TOKEN'), prompt('NPM_TOKEN')])

    def test_T8_non_owner_and_other_chat_fill_nothing(self):
        self.ask()
        before = self.effects()
        for number, extra in enumerate(({'user': 99}, {'chat': 55}, {'sender_chat': {'id': 1}}), 1):
            self.assertEqual(self.accept(number, VALUE, **extra), 'ignored')
        group = private(4, VALUE)['message']
        group['chat'] = {'id': OWNER, 'type': 'supergroup'}
        intake = envelopes.prepare_private(self.store, group)
        with self.store.db:
            self.assertEqual(envelopes.accept_private(self.store, group, intake), 'ignored')
        self.assertEqual(self.effects(), before)
        self.assertEqual(self.vault.calls, [])
        self.assertEqual(self.envelope()['state'], 'armed')
        self.assert_value_absent()

    def test_invalid_value_and_vault_failure_keep_the_current_slot(self):
        self.ask()
        self.ask('NPM_TOKEN')
        for number, value in enumerate(('two\nlines-of-value', '/not-a-value-at-all', 'x' * 4097), 1):
            self.assertEqual(self.accept(number, value), 'envelope_reject')
        self.vault.fail_next = 'io'
        self.assertEqual(self.accept(4, VALUE), 'envelope_reject')
        self.assertEqual(self.envelope()['state'], 'armed')
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'open')
        self.assertEqual(self.deletes(), [101, 102, 103, 104])
        self.assertEqual(self.accept(5, VALUE), 'envelope_fill')
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'armed')
        self.assert_value_absent()

    def test_queue_and_prompt_survive_store_reopen(self):
        self.ask()
        self.ask('NPM_TOKEN')
        self.store.close()
        self.store = Store(self.root / 'state')
        self.store.vault = self.vault
        self.assertEqual(self.replies(), [prompt('GITHUB_TOKEN')])
        self.assertEqual(self.accept(1, VALUE), 'envelope_fill')
        self.assertEqual(self.replies()[-1], prompt('NPM_TOKEN'))
        self.assertEqual(self.accept(2, VALUE + '-second'), 'envelope_fill')
        self.assertEqual(self.vault.values, {'GITHUB_TOKEN': VALUE, 'NPM_TOKEN': VALUE + '-second'})

    def test_duplicate_update_does_not_fill_the_next_envelope(self):
        self.ask()
        self.ask('NPM_TOKEN')
        update = self.dated_update(1, VALUE)
        self.assertEqual(self.store.accept(update), 'envelope_fill')
        self.assertEqual(self.store.accept(update), 'duplicate')
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'armed')
        self.assertEqual(self.vault.calls, [('put', 'GITHUB_TOKEN')])


class SelfCloseTests(DeliveredIntakeFixture):
    def setUp(self):
        super().setUp()
        self.number = 0

    def send(self, text, **extra):
        self.number += 1
        return self.store.accept(self.dated_update(self.number, text, **extra))

    def expire(self, name='GITHUB_TOKEN'):
        with self.store.db:
            self.store.db.execute("UPDATE envelopes SET expires=? WHERE name=? AND state IN ('open','armed')",
                                  (self.envelope(name)['created'] - 1, name))

    def service(self):
        return Service(self.store, self.telegram, object(), self.root, vault=self.vault)

    def loop_once(self):
        asyncio.run(self.service().envelopes_once())

    def revoke(self):
        self.assertTrue(self.call('secret.revoke', name='GITHUB_TOKEN').ok)
        asyncio.run(self.service().controls_once())

    def assert_rejected_value(self, value=VALUE):
        before = list(self.vault.calls)
        notices = self.count_notices()
        self.assertEqual(self.send(value), 'envelope_reject')
        self.assertEqual(self.vault.calls, before)
        self.assertEqual(self.count_notices(), notices)
        self.assertEqual(self.deletes()[-1], 100 + self.number)
        self.assertIn('deleted your message', self.replies()[-1])
        self.assert_value_absent(value)

    def count_notices(self):
        return self.store.db.execute("SELECT count(*) FROM messages WHERE kind='secret_filled'").fetchone()[0]

    def worker_values(self, name='NPM_TOKEN'):
        task = self.store.task_create(TOPIC, 'Uses ' + name, secrets=[name])
        return envelopes.load_for_worker(self.store, self.vault, task)[0]

    def assert_waits(self, name='NPM_TOKEN'):
        self.assertEqual(self.envelope(name)['state'], 'open')
        replies = [row[0] for row in self.store.db.execute(
            "SELECT text FROM envelope_effects WHERE envelope=? AND kind='reply'", (self.envelope(name)['id'],))]
        self.assertNotIn(prompt(name), replies)
        with self.assertRaises(envelopes.SecretsPending):
            self.worker_values(name)

    def resume_and_fill(self, name='NPM_TOKEN'):
        self.assertEqual(self.send('/start'), 'envelope_arm')
        self.assertEqual(self.replies()[-1], prompt(name))
        self.assertEqual(self.send(VALUE + '-for-' + name), 'envelope_fill')
        self.assertEqual(self.worker_values(name), {name: VALUE + '-for-' + name})

    def test_N1_owner_away_expiry_then_new_ask_cannot_take_either_answer(self):
        self.ask()
        self.expire()
        self.loop_once()
        self.assertTrue(self.ask('NPM_TOKEN').ok)
        self.assert_rejected_value(VALUE + '-for-github')
        self.assert_rejected_value(VALUE + '-for-npm')
        self.assert_waits()
        self.resume_and_fill()

    def test_N2_in_flight_value_after_new_ask_sweeps_expiry_is_not_misfiled(self):
        self.ask()
        self.expire()
        self.assertTrue(self.ask('NPM_TOKEN').ok)
        self.assert_rejected_value()
        self.assert_waits()
        self.resume_and_fill()

    def test_N3_revoke_then_new_ask_cannot_take_the_closed_prompts_value(self):
        self.ask()
        self.revoke()
        self.assertTrue(self.ask('NPM_TOKEN').ok)
        self.assert_rejected_value()
        self.assert_waits()
        self.resume_and_fill()

    def test_N4_rotate_keeps_the_good_value_until_the_owner_resumes(self):
        good = VALUE + '-original-npm'
        self.ask('NPM_TOKEN')
        self.assertEqual(self.send(good), 'envelope_fill')
        self.ask()
        self.expire()
        self.loop_once()
        self.assertTrue(self.call('secret.rotate', name='NPM_TOKEN').ok)
        self.assert_rejected_value()
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'open')
        self.assertEqual(self.worker_values(), {'NPM_TOKEN': good})
        self.assertEqual(self.count_notices(), 1)
        self.resume_and_fill()

    def test_a2_new_ask_after_loop_expiry_does_not_send_a_prompt(self):
        self.ask()
        self.expire()
        self.loop_once()
        self.ask('NPM_TOKEN')
        self.assert_waits()

    def test_a3_new_ask_that_sweeps_expiry_does_not_send_a_prompt(self):
        self.ask()
        self.expire()
        self.ask('NPM_TOKEN')
        self.assert_waits()

    def test_a4_new_ask_after_revoke_does_not_send_a_prompt(self):
        self.ask()
        self.revoke()
        self.ask('NPM_TOKEN')
        self.assert_waits()

    def test_a6_waiting_cards_only_say_behind_while_another_ask_is_armed(self):
        self.ask()
        self.ask('NPM_TOKEN')
        self.assertIn('behind another', self.outbox(self.envelope('NPM_TOKEN')['card_outbox'])['text'])
        self.expire()
        self.ask('DEPLOY_TOKEN')
        for name in ('NPM_TOKEN', 'DEPLOY_TOKEN'):
            card = self.outbox(self.envelope(name)['card_outbox'])
            self.assertNotIn('behind another', card['text'])
            self.assertIn('Tap Fill privately within 10 minutes', card['text'])
            self.assertEqual([button['text'] for row in card['markup']['inline_keyboard'] for button in row],
                             ['Fill privately', 'Cancel'])
        self.assert_rejected_value()
        self.assertEqual(self.send('/start'), 'envelope_arm')
        self.assertIn('behind another', self.outbox(self.envelope('DEPLOY_TOKEN')['card_outbox'])['text'])

    def test_b3_start_twice_with_no_waiter_is_truthful_and_lifts_the_pause(self):
        self.ask()
        self.expire()
        self.loop_once()
        for _ in range(2):
            self.assertEqual(self.send('/start'), 'envelope_start')
            self.assertEqual(self.replies()[-1], 'No credential asks are waiting.')
        self.ask('NPM_TOKEN')
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'armed')
        self.assertEqual(self.send(VALUE), 'envelope_fill')

    def test_e2_only_waiter_expires_before_start_and_no_missing_button_is_offered(self):
        self.ask()
        self.ask('NPM_TOKEN')
        self.expire()
        self.loop_once()
        self.expire('NPM_TOKEN')
        for _ in range(2):
            self.assertEqual(self.send('/start'), 'envelope_start')
            self.assertEqual(self.replies()[-1], 'No credential asks are waiting.')
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'expired')
        self.ask('DEPLOY_TOKEN')
        self.assertEqual(self.envelope('DEPLOY_TOKEN')['state'], 'armed')

    def assert_cancel_after_expiry(self):
        self.assertEqual(self.send('/cancel'), 'envelope_reject')
        self.assertEqual(self.replies()[-1],
                         'No credential ask is taking a value. Another ask is waiting. Tap Fill privately on its card.')
        self.assert_waits()
        self.resume_and_fill()

    def test_c1_cancel_just_after_unswept_expiry_mentions_the_waiter(self):
        self.ask()
        self.ask('NPM_TOKEN')
        self.expire()
        self.assert_cancel_after_expiry()

    def test_c2_cancel_after_loop_expiry_mentions_the_waiter(self):
        self.ask()
        self.ask('NPM_TOKEN')
        self.expire()
        self.loop_once()
        self.assert_cancel_after_expiry()

    def test_pause_survives_a_store_reopen_before_a_new_ask(self):
        self.ask()
        self.expire()
        self.loop_once()
        self.store.close()
        self.store = Store(self.root / 'state')
        self.store.vault = self.vault
        self.ask('NPM_TOKEN')
        self.assert_rejected_value()
        self.assert_waits()
        self.resume_and_fill()

    def test_pause_survives_the_last_waiter_expiring_before_a_new_ask(self):
        self.ask()
        self.ask('NPM_TOKEN')
        self.expire()
        self.loop_once()
        self.expire('NPM_TOKEN')
        self.ask('DEPLOY_TOKEN')
        self.assert_rejected_value()
        self.assert_waits('DEPLOY_TOKEN')
        self.resume_and_fill('DEPLOY_TOKEN')

    def test_same_name_rotation_also_requires_an_owner_exit(self):
        self.ask()
        self.expire()
        self.loop_once()
        self.assertTrue(self.call('secret.rotate', name='GITHUB_TOKEN').ok)
        self.assert_rejected_value()
        self.assert_waits('GITHUB_TOKEN')
        self.resume_and_fill('GITHUB_TOKEN')

    def test_nonowner_commands_cannot_lift_the_pause(self):
        self.ask()
        self.expire()
        self.loop_once()
        self.assertEqual(self.send('/start', user=99), 'ignored')
        self.assertEqual(self.send('/cancel', user=99), 'ignored')
        self.ask('NPM_TOKEN')
        self.assert_waits()
        self.assert_rejected_value()

    def test_owner_cancel_acknowledges_closed_prompt_before_a_later_new_ask(self):
        self.ask()
        self.expire()
        self.loop_once()
        self.assertEqual(self.send('/cancel'), 'envelope_reject')
        self.assertEqual(self.replies()[-1], 'No credential asks are waiting.')
        self.ask('NPM_TOKEN')
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'armed')
        self.assertEqual(self.send(VALUE), 'envelope_fill')


class CardButtonTests(ValueDateFixture):
    def card(self, name='GITHUB_TOKEN'):
        return self.outbox(self.envelope(name)['card_outbox'])

    def assert_callback(self, name):
        envelope = self.envelope(name)
        self.assertEqual(self.card(name)['markup']['inline_keyboard'], [
            [{'text': 'Fill privately', 'callback_data': 'envelope:%d:fill' % envelope['id']}],
            [{'text': 'Cancel', 'callback_data': 'envelope:%d:cancel' % envelope['id']}]])

    def pause(self):
        self.ask()
        self.expire()
        with self.store.db:
            envelopes.sweep(self.store)

    def test_F5_group_reply_to_waiting_card_still_points_to_its_button(self):
        from tests.test_envelopes import topic_reply
        self.pause()
        self.ask('NPM_TOKEN')
        self.assert_callback('NPM_TOKEN')
        card = self.envelope('NPM_TOKEN')['card_outbox']
        self.store.delivered(card, 903)
        self.assertEqual(self.store.accept(topic_reply(1, 903)), 'envelope_intercept')
        self.assertEqual(self.vault.values, {})
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'open')
        notice = self.store.db.execute("SELECT text FROM outbox WHERE kind='report' ORDER BY id DESC").fetchone()[0]
        self.assertEqual(notice, 'I removed your reply. Use Fill privately to send values.')
        self.assert_callback('NPM_TOKEN')


if __name__ == '__main__':
    unittest.main()
