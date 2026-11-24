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
from coordinator.telegram import TelegramError
from tests.test_envelopes import BOT, GROUP, OWNER, TOPIC, VALUE, IntakeFixture, private, topic_reply


def prompt(name, job='Job: any job'):
    return ('Envelope: %s\nWhy: Push the release branch\nUsed by: release worker\n%s\n'
            'Reply to this message with the value in one message within 24 hours. I delete it after I store it. '
            'Reply /cancel to stop.') % (name, job)


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

    def test_start_after_delivery_keeps_the_first_prompt_date(self):
        self.ask()
        self.loop()
        self.telegram.date += 10
        self.assertEqual(self.send('/start', None), 'envelope_arm')
        self.loop()
        self.assertEqual(self.send(VALUE, self.telegram.date - 9), 'envelope_fill')



class DeliveredIntakeFixture(IntakeFixture):
    def setUp(self):
        super().setUp()
        self.number = 0

    def service(self):
        return Service(self.store, self.telegram, object(), self.root, vault=self.vault)

    def send(self, text, date=None, **extra):
        self.number += 1
        self.run_effects()
        return self.store.accept(private(self.number, text, date=self.telegram.date + 1 if date is None else date,
                                         **extra))

    def reply_to(self, name, text):
        self.run_effects()
        return self.send(text, reply_to_message={'message_id': self.envelope(name)['prompt_message'],
                                                 'chat': {'id': OWNER, 'type': 'private'}})

    def card(self, name='GITHUB_TOKEN'):
        return self.outbox(self.envelope(name)['card_outbox'])

    def stored(self, name, value=VALUE):
        return '%s stored. Length %d, fingerprint %s.' % (name, len(value), self.vault.fingerprint(value))


class EveryAskPromptsTests(DeliveredIntakeFixture):
    def test_every_ask_posts_its_own_prompt_at_once(self):
        task = self.store.task_create(TOPIC, 'Release')
        self.assertTrue(self.ask(task=task['id']).ok)
        self.assertTrue(self.ask('NPM_TOKEN').ok)
        self.assertTrue(self.ask('DEPLOY_TOKEN').ok)
        self.assertEqual(self.effects(), [('reply', OWNER, None, prompt('GITHUB_TOKEN', 'Job: 1 Release'), 'pending'),
                                          ('reply', OWNER, None, prompt('NPM_TOKEN'), 'pending'),
                                          ('reply', OWNER, None, prompt('DEPLOY_TOKEN'), 'pending')])
        for name in ('GITHUB_TOKEN', 'NPM_TOKEN', 'DEPLOY_TOKEN'):
            envelope = self.envelope(name)
            self.assertEqual((envelope['state'], envelope['chat']), ('armed', OWNER))
            card = self.card(name)
            self.assertIn('The prompt is in your private chat with me. Reply to it there', card['text'])
            self.assertNotIn('behind', card['text'])
            self.assertEqual(card['markup']['inline_keyboard'], [
                [{'text': 'Fill privately', 'url': 'https://t.me/' + BOT}],
                [{'text': 'Cancel', 'callback_data': 'envelope:%d:cancel' % envelope['id']}]])
        self.run_effects()
        sent = [params['text'] for method, params in self.telegram.calls if method == 'sendMessage']
        self.assertEqual(sent, [row[3] for row in self.effects()])
        for name in ('GITHUB_TOKEN', 'NPM_TOKEN', 'DEPLOY_TOKEN'):
            envelope = self.envelope(name)
            self.assertEqual(envelope['prompt_date'], self.telegram.date)
            self.assertIsInstance(envelope['prompt_message'], int)

    def test_handoff_gets_no_private_prompt_and_blocks_nothing(self):
        with self.store.db:
            handoff = envelopes.ask(self.store, TOPIC, envelopes.HANDOFF, 'Sign in', 'Claude')
        self.assertEqual(self.effects(), [])
        row = self.envelope(envelopes.HANDOFF)
        self.assertEqual((row['state'], row['expires'] - row['created']), ('open', 600))
        card = self.outbox(row['card_outbox'])
        self.assertEqual(card['text'], 'Sign in\n3. Paste it here as one message within 10 minutes. I delete it at once.')
        self.assertEqual(card['markup']['inline_keyboard'],
                         [[{'text': 'Cancel', 'callback_data': 'envelope:%d:cancel' % handoff}]])
        self.ask()
        with self.store.db:
            envelopes.sweep(self.store)
        self.assertEqual(self.envelope(envelopes.HANDOFF)['state'], 'open')
        self.assertEqual(self.send(VALUE), 'envelope_fill')
        self.assertEqual(self.vault.values, {'GITHUB_TOKEN': VALUE})
        self.assertEqual(self.envelope(envelopes.HANDOFF)['state'], 'open')

    def test_prompt_of_a_closed_ask_is_not_sent(self):
        self.ask()
        self.assertEqual(self.click_cancel(1, self.envelope()), 'envelope_cancel')
        self.run_effects()
        self.assertEqual([method for method, _ in self.telegram.calls], [])
        self.assertEqual(self.effects()[0][4], 'done')

    def test_rows_from_before_the_upgrade_get_their_prompts(self):
        self.ask()
        self.ask('NPM_TOKEN')
        with self.store.db:
            self.store.db.execute('DELETE FROM envelope_effects')
            self.store.db.execute("UPDATE envelopes SET prompt_effect=NULL WHERE name='GITHUB_TOKEN'")
            self.store.db.execute("UPDATE envelopes SET state='open',chat=NULL,prompt_effect=NULL WHERE name='NPM_TOKEN'")
        self.store.close()
        self.store = Store(self.root / 'state')
        self.store.vault = self.vault
        asyncio.run(self.service().envelopes_once())
        self.assertEqual(self.replies(), [prompt('GITHUB_TOKEN'), prompt('NPM_TOKEN')])
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'armed')
        self.assertEqual(self.reply_to('NPM_TOKEN', VALUE), 'envelope_fill')
        self.assertEqual(self.vault.values, {'NPM_TOKEN': VALUE})


class FillTargetTests(DeliveredIntakeFixture):
    def two(self):
        self.ask()
        self.ask('NPM_TOKEN')
        self.run_effects()

    def test_reply_to_a_prompt_fills_that_ask(self):
        self.two()
        self.assertEqual(self.reply_to('NPM_TOKEN', VALUE), 'envelope_fill')
        self.assertEqual(self.vault.values, {'NPM_TOKEN': VALUE})
        self.assertEqual(self.envelope()['state'], 'armed')
        self.assertEqual(self.replies()[-1], self.stored('NPM_TOKEN') + ' Still waiting: GITHUB_TOKEN.')
        self.assertEqual(self.deletes(), [101])
        self.assertEqual(self.reply_to('GITHUB_TOKEN', VALUE + '-gh'), 'envelope_fill')
        self.assertEqual(self.replies()[-1], self.stored('GITHUB_TOKEN', VALUE + '-gh'))
        self.assertEqual(self.vault.values, {'NPM_TOKEN': VALUE, 'GITHUB_TOKEN': VALUE + '-gh'})
        self.assert_value_absent()
        self.assert_value_absent(VALUE + '-gh')

    def test_plain_message_fills_the_oldest_and_names_it(self):
        self.two()
        self.assertEqual(self.send(VALUE), 'envelope_fill')
        self.assertEqual(self.replies()[-1], self.stored('GITHUB_TOKEN') + ' Still waiting: NPM_TOKEN.')
        self.assertEqual(self.send(VALUE + '-npm'), 'envelope_fill')
        self.assertEqual(self.replies()[-1], self.stored('NPM_TOKEN', VALUE + '-npm'))
        self.assertEqual(self.vault.values, {'GITHUB_TOKEN': VALUE, 'NPM_TOKEN': VALUE + '-npm'})

    def test_reply_to_the_owners_own_message_counts_as_plain(self):
        self.two()
        self.assertEqual(self.send(VALUE, reply_to_message={'message_id': 1, 'from': {'id': OWNER}}), 'envelope_fill')
        self.assertEqual(self.vault.values, {'GITHUB_TOKEN': VALUE})

    def test_reply_to_a_bot_message_that_names_no_waiting_ask_stores_nothing(self):
        self.two()
        bot = {'id': 1, 'is_bot': True}
        for number, replied in enumerate(({'message_id': 1, 'from': bot, 'text': 'GITHUB_TOKEN stored.'},
                                          {'message_id': 2, 'from': bot, 'text': 'Envelope: DEPLOY_TOKEN\nWhy: x'},
                                          {'message_id': 3}), 1):
            with self.subTest(replied=replied):
                self.assertEqual(self.send(VALUE, reply_to_message=replied), 'envelope_reject')
                self.assertEqual(self.events()[-1], ('reject', 'unknown_reply', 'intake'))
                self.assertEqual(self.replies()[-1], 'That reply does not match a waiting credential ask, so I deleted '
                                 'it and stored nothing. Waiting for GITHUB_TOKEN, NPM_TOKEN. Reply to its prompt.')
                self.assertEqual(self.deletes()[-1], 100 + self.number)
        self.assertEqual(self.vault.calls, [])
        self.assert_value_absent()

    def test_reply_to_a_lost_copy_of_a_prompt_fills_its_own_ask(self):
        self.ask()
        self.ask('NPM_TOKEN')
        real = self.telegram.call
        lost = {}

        async def call(method, **params):
            if method == 'sendMessage' and params['text'].startswith('Envelope: NPM_TOKEN') and not lost:
                lost['id'] = (await real(method, **params))['message_id']
                raise TelegramError('network-or-invalid-response', reason='timeout')
            return await real(method, **params)

        self.telegram.call = call
        self.run_effects()
        with self.store.db:
            self.store.db.execute('UPDATE envelope_effects SET next_attempt=0')
        self.run_effects()
        self.assertNotIn(self.envelope('NPM_TOKEN')['prompt_message'], (None, lost['id']))
        copy = {'message_id': lost['id'], 'from': {'id': 1, 'is_bot': True}, 'text': prompt('NPM_TOKEN')}
        self.assertEqual(self.send(VALUE, reply_to_message=copy), 'envelope_fill')
        self.assertEqual(self.vault.values, {'NPM_TOKEN': VALUE})
        self.assertEqual(self.envelope()['state'], 'armed')
        self.assert_value_absent()

    def test_reply_to_an_old_format_prompt_fills_the_ask_it_names(self):
        self.two()
        old = {'message_id': 4, 'from': {'id': 1, 'is_bot': True},
               'text': 'Send the value for NPM_TOKEN now in one message. I will delete it after I store it. '
                       'Send /cancel to stop.'}
        self.assertEqual(self.send(VALUE, reply_to_message=old), 'envelope_fill')
        self.assertEqual(self.vault.values, {'NPM_TOKEN': VALUE})

    def test_reply_to_a_closed_prompt_stores_nothing(self):
        self.two()
        self.assertEqual(self.click_cancel(900, self.envelope('NPM_TOKEN')), 'envelope_cancel')
        self.assertEqual(self.reply_to('NPM_TOKEN', VALUE), 'envelope_reject')
        self.assertEqual(self.replies()[-1], envelopes.REPLIES['closed_prompt'])
        self.assertEqual(self.events()[-1], ('reject', 'closed_prompt', 'intake'))
        self.assertEqual(self.deletes()[-1], 100 + self.number)
        self.assertEqual(self.envelope()['state'], 'armed')
        self.assertEqual(self.reply_to('GITHUB_TOKEN', VALUE + '-gh'), 'envelope_fill')
        self.assertEqual(self.reply_to('GITHUB_TOKEN', VALUE + '-again'), 'envelope_reject')
        self.assertEqual(self.vault.values, {'GITHUB_TOKEN': VALUE + '-gh'})
        self.assert_value_absent()
        self.assert_value_absent(VALUE + '-again')

    def test_reply_to_an_expired_prompt_before_the_sweep_stores_nothing(self):
        self.two()
        with self.store.db:
            self.store.db.execute("UPDATE envelopes SET expires=? WHERE name='NPM_TOKEN'", (time.time() - 1,))
        self.assertEqual(self.reply_to('NPM_TOKEN', VALUE), 'envelope_reject')
        self.assertEqual(self.vault.values, {})
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'expired')

    def test_cancel_by_reply_or_plain(self):
        self.two()
        self.ask('DEPLOY_TOKEN')
        self.assertEqual(self.reply_to('NPM_TOKEN', '/cancel'), 'envelope_cancel')
        self.assertEqual(self.replies()[-1], 'Cancelled NPM_TOKEN.')
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'cancelled')
        self.assertEqual(self.reply_to('NPM_TOKEN', '/cancel'), 'envelope_reject')
        self.assertEqual(self.envelope()['state'], 'armed')
        self.assertEqual(self.send('/cancel'), 'envelope_cancel')
        self.assertEqual(self.replies()[-1], 'Cancelled GITHUB_TOKEN.')
        self.assertEqual(self.envelope('DEPLOY_TOKEN')['state'], 'armed')
        self.assertEqual(self.send(VALUE), 'envelope_fill')
        self.assertEqual(self.vault.values, {'DEPLOY_TOKEN': VALUE})
        self.assertEqual(self.send('/cancel'), 'envelope_reject')
        self.assertEqual(self.replies()[-1], envelopes.REPLIES['nothing_to_cancel'])

    def test_invalid_value_and_vault_failure_keep_every_ask_open(self):
        self.two()
        for value in ('two\nlines-of-value', '/not-a-value-at-all', 'x' * 4097):
            self.assertEqual(self.reply_to('NPM_TOKEN', value), 'envelope_reject')
        self.vault.fail_next = 'io'
        self.assertEqual(self.send(VALUE), 'envelope_reject')
        self.assertEqual((self.envelope()['state'], self.envelope('NPM_TOKEN')['state']), ('armed', 'armed'))
        self.assertEqual(self.deletes(), [101, 102, 103, 104])
        self.assertEqual(self.send(VALUE), 'envelope_fill')
        self.assertEqual(self.vault.values, {'GITHUB_TOKEN': VALUE})
        self.assert_value_absent()

    def test_non_owner_and_other_chat_fill_nothing(self):
        self.two()
        before = self.effects()
        for extra in ({'user': 99}, {'chat': 55}, {'sender_chat': {'id': 1}}):
            self.assertEqual(self.send(VALUE, **extra), 'ignored')
        group = private(9, VALUE, date=self.telegram.date + 1)['message']
        group['chat'] = {'id': OWNER, 'type': 'supergroup'}
        intake = envelopes.prepare_private(self.store, group)
        with self.store.db:
            self.assertEqual(envelopes.accept_private(self.store, group, intake), 'ignored')
        self.assertEqual(self.effects(), before)
        self.assertEqual(self.vault.calls, [])
        self.assert_value_absent()

    def test_duplicate_update_fills_once(self):
        self.two()
        update = private(1, VALUE, date=self.telegram.date + 1)
        self.assertEqual(self.store.accept(update), 'envelope_fill')
        self.assertEqual(self.store.accept(update), 'duplicate')
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'armed')
        self.assertEqual(self.vault.calls, [('put', 'GITHUB_TOKEN')])

    def test_prompts_survive_store_reopen(self):
        self.two()
        self.store.close()
        self.store = Store(self.root / 'state')
        self.store.vault = self.vault
        self.assertEqual(self.reply_to('NPM_TOKEN', VALUE), 'envelope_fill')
        self.assertEqual(self.send(VALUE + '-gh'), 'envelope_fill')
        self.assertEqual(self.vault.values, {'NPM_TOKEN': VALUE, 'GITHUB_TOKEN': VALUE + '-gh'})

    def test_no_ask_reply(self):
        self.assertEqual(self.send(VALUE), 'envelope_reject')
        self.assertEqual(self.replies()[-1], 'No credential ask is waiting for a value, so I deleted your message.')

    def test_start_resends_only_missing_prompts_or_lists_the_asks(self):
        self.assertEqual(self.send('/start'), 'envelope_start')
        self.assertEqual(self.replies()[-1], envelopes.REPLIES['bare_start'])
        self.two()
        self.assertEqual(self.send('/start'), 'envelope_arm')
        self.assertEqual(self.replies()[-1], 'Waiting for GITHUB_TOKEN, NPM_TOKEN. Reply to its prompt above with the value.')
        with self.store.db:
            self.store.db.execute("UPDATE envelopes SET prompt_message=NULL,prompt_date=NULL WHERE name='NPM_TOKEN'")
        self.assertEqual(self.send('/start'), 'envelope_arm')
        self.assertEqual(self.replies()[-1], prompt('NPM_TOKEN'))
        self.assertEqual(self.replies().count(prompt('GITHUB_TOKEN')), 1)
        self.assertEqual(self.reply_to('NPM_TOKEN', VALUE), 'envelope_fill')

    def test_live_rotate_is_refused_and_nothing_changes(self):
        self.two()
        tables = ('envelopes', 'envelope_events', 'envelope_effects', 'outbox')
        before = {table: [tuple(row) for row in self.store.db.execute('SELECT * FROM ' + table)] for table in tables}
        for name in ('GITHUB_TOKEN', 'NPM_TOKEN'):
            with self.subTest(name=name):
                result = self.call('secret.rotate', name=name)
                self.assertEqual(result.state, 'refused')
                self.assertEqual(result.text, 'An envelope for ' + name + ' is already open.')
                after = {table: [tuple(row) for row in self.store.db.execute('SELECT * FROM ' + table)]
                         for table in tables}
                self.assertEqual(after, before)


class MisfileTests(DeliveredIntakeFixture):
    """A plain message sent while an older prompted ask was open never fills a newer one."""

    def setUp(self):
        super().setUp()
        self.telegram.date = int(time.time()) - 1000
        self.ask()
        self.ask('NPM_TOKEN')
        self.run_effects()
        self.sent = self.telegram.date + 5
        self.closed = self.telegram.date + 10

    def close_first(self, how):
        if how == 'expire':
            with self.store.db:
                self.store.db.execute("UPDATE envelopes SET expires=? WHERE name='GITHUB_TOKEN'", (self.closed,))
        elif how == 'sweep':
            with self.store.db:
                self.store.db.execute("UPDATE envelopes SET expires=? WHERE name='GITHUB_TOKEN'", (self.closed,))
            asyncio.run(self.service().envelopes_once())
        else:
            with patch('coordinator.envelopes.time.time', return_value=self.closed), \
                    patch('coordinator.store.time.time', return_value=self.closed):
                if how == 'revoke':
                    self.assertTrue(self.call('secret.revoke', name='GITHUB_TOKEN').ok)
                    asyncio.run(self.service().controls_once())
                else:
                    with self.store.db:
                        envelopes.cancel(self.store, self.envelope()['id'], 'cli')

    def test_value_sent_while_the_older_ask_was_open_is_refused(self):
        for how in ('expire', 'sweep', 'revoke', 'cli'):
            with self.subTest(how=how):
                self.setUp()
                self.close_first(how)
                self.assertEqual(self.send(VALUE, date=self.sent), 'envelope_reject')
                self.assertEqual(self.events()[-1], ('reject', 'sent_while_open', 'intake'))
                self.assertEqual(self.replies()[-1], 'That message was sent while I was still waiting for GITHUB_TOKEN. '
                                 'I deleted it and stored nothing. Reply to the prompt you mean.')
                self.assertNotIn(('put', 'NPM_TOKEN'), self.vault.calls)
                self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'armed')
                self.assertEqual(self.send(VALUE + '-npm', date=self.closed + 1), 'envelope_fill')
                self.assertEqual(self.vault.values, {'NPM_TOKEN': VALUE + '-npm'})
                self.assert_value_absent()
                self.tearDown()
                self.doCleanups()

    def test_closed_ask_posts_a_notice_in_the_private_chat(self):
        self.close_first('sweep')
        self.assertEqual(self.replies()[-1], "GITHUB_TOKEN expired, so I won't take a value for it.")

    def test_owner_cancel_then_value_for_the_next_ask_fills_it(self):
        self.assertEqual(self.send('/cancel', date=self.sent), 'envelope_cancel')
        self.assertEqual(self.send(VALUE, date=int(time.time()) + 1), 'envelope_fill')
        self.assertEqual(self.vault.values, {'NPM_TOKEN': VALUE})

    def test_value_sent_before_the_older_ask_was_filled_is_refused(self):
        self.assertEqual(self.send(VALUE, date=self.sent), 'envelope_fill')
        self.assertEqual(self.send(VALUE + '-corrected', date=self.sent), 'envelope_reject')
        self.assertEqual(self.events()[-1], ('reject', 'sent_while_open', 'intake'))
        self.assertEqual(self.vault.values, {'GITHUB_TOKEN': VALUE})
        self.assert_value_absent(VALUE + '-corrected')
        self.assertEqual(self.send(VALUE + '-npm', date=int(time.time()) + 1), 'envelope_fill')
        self.assertEqual(self.vault.values, {'GITHUB_TOKEN': VALUE, 'NPM_TOKEN': VALUE + '-npm'})

    def test_value_in_flight_before_a_new_prompt_is_refused(self):
        self.close_first('sweep')
        self.ask('DEPLOY_TOKEN')
        self.assertEqual(self.reply_to('NPM_TOKEN', VALUE + '-npm'), 'envelope_fill')
        self.assertEqual(self.send(VALUE, date=self.telegram.date), 'envelope_reject')
        self.assertEqual(self.events()[-1], ('reject', 'before_prompt', 'intake'))
        self.assertEqual(self.vault.values, {'NPM_TOKEN': VALUE + '-npm'})


class LifetimeTests(DeliveredIntakeFixture):
    def test_ask_stays_open_for_a_day_then_expires_and_can_be_asked_again(self):
        self.assertEqual(envelopes.LIFETIME, 24 * 3600)
        self.ask()
        envelope = self.envelope()
        self.assertEqual(envelope['expires'], envelope['armed_at'] + envelopes.LIFETIME)
        self.assertIn('within 24 hours', self.card()['text'])
        self.assertIn('within 24 hours', self.replies()[-1])
        with self.store.db:
            self.assertEqual(envelopes.sweep(self.store, envelope['expires'] - 1), 0)
            self.assertEqual(envelopes.sweep(self.store, envelope['expires']), 1)
        self.assertEqual(self.envelope()['state'], 'expired')
        self.assertEqual(self.replies()[-1], "GITHUB_TOKEN expired, so I won't take a value for it.")
        card = self.card()
        self.assertEqual(card['text'], 'Envelope: GITHUB_TOKEN\nExpired. Tap Ask again to request it again.')
        self.assertEqual(card['markup']['inline_keyboard'],
                         [[{'text': 'Ask again', 'callback_data': 'envelope:%d:again' % envelope['id']}]])
        with self.store.db:
            self.assertEqual(envelopes.again(self.store, envelope['id']), 'envelope_again')
        self.assertEqual(self.envelope()['state'], 'armed')
        self.assertEqual(self.replies()[-1], prompt('GITHUB_TOKEN'))
        self.assertEqual(self.send(VALUE), 'envelope_fill')


class CardTests(DeliveredIntakeFixture):
    def test_reply_in_the_topic_is_deleted_and_points_to_the_private_prompt(self):
        self.ask()
        self.store.delivered(self.envelope()['card_outbox'], 903)
        self.assertEqual(self.store.accept(topic_reply(1, 903)), 'envelope_intercept')
        self.assertEqual(self.vault.values, {})
        self.assertIn(('delete', GROUP, 201, None, 'pending'), self.effects())
        notice = self.store.db.execute("SELECT text FROM outbox WHERE kind='report' ORDER BY id DESC").fetchone()[0]
        self.assertEqual(notice, 'I removed your reply. Reply to its prompt in your private chat with me, '
                         'or tap Fill privately.')
        self.assertEqual(self.count('messages'), 0)
        self.assert_value_absent()


if __name__ == '__main__':
    unittest.main()
