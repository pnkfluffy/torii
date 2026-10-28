import asyncio
import json
import re
import unittest
from unittest.mock import patch

from coordinator import envelopes
from coordinator.service import Service
from coordinator.telegram import TelegramError
from tests.test_envelopes import BOT, GROUP, OWNER, TOPIC, VALUE, EnvelopeFixture, FakeTelegram, private


class FillTelegram(FakeTelegram):
    def __init__(self):
        super().__init__()
        self.private_started = True

    async def call(self, method, **params):
        if method == 'sendMessage' and params.get('chat_id') == OWNER and not self.private_started:
            self.calls.append((method, params))
            raise TelegramError(403)
        if (method == 'answerCallbackQuery' and 'url' in params
                and not re.fullmatch(r'https://t\.me/' + BOT + r'\?start=[A-Za-z0-9_-]{1,64}', params['url'])):
            self.calls.append((method, params))
            raise TelegramError(400)
        return await super().call(method, **params)


class FillFixture(EnvelopeFixture):
    def setUp(self):
        super().setUp()
        self.telegram = FillTelegram()
        self.service = Service(self.store, self.telegram, object(), self.root, vault=self.vault)
        self.number = 0

    def tap(self, envelope_id, action='fill', user=OWNER, bot=False, chat=GROUP):
        self.number += 1
        query = {'id': str(self.number), 'from': {'id': user, 'is_bot': bot},
                 'data': 'envelope:%d:%s' % (envelope_id, action),
                 'message': {'message_id': 900, 'message_thread_id': 4,
                             'chat': {'id': chat, 'type': 'supergroup'}}}

        async def run():
            outcome = await self.service.accept_update({'update_id': self.number, 'callback_query': query})
            await self.service.answer_callback(query['id'], outcome)
            return outcome

        return asyncio.run(run())

    def send(self, text, **extra):
        self.number += 1
        return asyncio.run(self.service.accept_update(private(self.number, text, **extra)))

    def deliver(self):
        async def run():
            await envelopes.run_effects(self.store, self.telegram)
            while await self.service.deliver_once():
                pass
        asyncio.run(run())

    def expire(self, name='GITHUB_TOKEN', sweep=True):
        with self.store.db:
            self.store.db.execute('UPDATE envelopes SET expires=created-1 WHERE id=?', (self.envelope(name)['id'],))
            if sweep:
                envelopes.sweep(self.store)
        return self.envelope(name)

    def pause(self):
        self.ask()
        self.expire()
        self.assertFalse(self.store.get('envelope_prompt_exit')['acknowledged'])

    def card(self, name='GITHUB_TOKEN'):
        return self.outbox(self.envelope(name)['card_outbox'])

    def replies(self):
        return [row[0] for row in self.store.db.execute("SELECT text FROM envelope_effects WHERE kind='reply' ORDER BY id")]

    def fail_private_prompt(self):
        self.telegram.private_started = False
        envelope = self.envelope('NPM_TOKEN')
        self.assertEqual(self.tap(envelope['id']), 'envelope_fill')
        for _ in range(5):
            effect = self.store.db.execute("""SELECT * FROM envelope_effects
                WHERE envelope=? AND kind='reply' ORDER BY id DESC LIMIT 1""", (envelope['id'],)).fetchone()
            with patch('time.time', return_value=max(effect['created'], effect['next_attempt'])):
                asyncio.run(envelopes.run_effects(self.store, self.telegram))
        effect = self.store.db.execute('SELECT state,attempts FROM envelope_effects WHERE id=?',
                                       (effect['id'],)).fetchone()
        self.assertEqual(tuple(effect), ('failed', 5))
        self.telegram.private_started = True

    def answers(self):
        return [params for method, params in self.telegram.calls if method == 'answerCallbackQuery']

    def snapshot(self):
        tables = ('envelopes', 'envelope_events', 'envelope_effects', 'outbox', 'settings')
        data = {table: [dict(row) for row in self.store.db.execute('SELECT * FROM ' + table + ' ORDER BY 1')]
                for table in tables}
        data['settings'] = [row for row in data['settings'] if row['key'] != 'offset']
        return data

    def assert_buttons(self, name):
        envelope = self.envelope(name)
        self.assertEqual(self.card(name)['markup']['inline_keyboard'], [
            [{'text': 'Fill privately', 'callback_data': 'envelope:%d:fill' % envelope['id']}],
            [{'text': 'Cancel', 'callback_data': 'envelope:%d:cancel' % envelope['id']}]])


class FillButtonTests(FillFixture):
    def test_fill_start_resends_failed_prompt_and_stores_next_value(self):
        self.ask()
        self.deliver()
        previous_prompt = self.store.get('envelope_prompt_date')
        self.expire()
        self.ask('NPM_TOKEN')
        self.fail_private_prompt()
        self.assertEqual(self.store.get('envelope_prompt_date'), previous_prompt)
        envelope = self.envelope('NPM_TOKEN')
        before = len(self.replies())
        mark = len(self.telegram.calls)
        self.assertEqual(self.send('/start fill'), 'envelope_start')
        self.assertEqual(self.envelope('NPM_TOKEN'), envelope)
        self.assertEqual(self.replies()[before:], ['Send the value for NPM_TOKEN now in one message. '
                         'I will delete it after I store it. Send /cancel to stop.'])
        self.deliver()
        sent = [params['text'] for method, params in self.telegram.calls[mark:]
                if method == 'sendMessage' and params['chat_id'] == OWNER]
        self.assertEqual(sent, self.replies()[before:])
        self.assertEqual(self.store.get('envelope_prompt_date'),
                         {'envelope': envelope['id'], 'date': self.telegram.date})
        self.assertIn(('deleteMessage', {'chat_id': OWNER, 'message_id': 100 + self.number}), self.telegram.calls)
        self.assertEqual(self.send(VALUE, date=self.telegram.date + 1), 'envelope_fill')
        self.assertEqual(self.vault.values, {'NPM_TOKEN': VALUE})
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'filled')

    def test_fill_start_keeps_one_queued_or_sending_prompt(self):
        for failed in (False, True):
            for sending in (False, True):
                with self.subTest(failed=failed, sending=sending):
                    f = FillFixture()
                    f.setUp()
                    try:
                        f.pause()
                        f.ask('NPM_TOKEN')
                        if failed:
                            f.fail_private_prompt()
                        else:
                            self.assertEqual(f.tap(f.envelope('NPM_TOKEN')['id']), 'envelope_fill')
                        mark = len(f.telegram.calls)
                        self.assertEqual(f.send('/start fill'), 'envelope_start')
                        prompt = 'Send the value for NPM_TOKEN now in one message. I will delete it after I store it. Send /cancel to stop.'
                        self.assertEqual(f.replies().count(prompt), 2 if failed else 1)
                        before = f.snapshot()

                        def assert_unchanged(before):
                            after = f.snapshot()
                            effect = after['envelope_effects'].pop()
                            self.assertEqual((effect['kind'], effect['state']), ('delete', 'pending'))
                            self.assertEqual(after, before)

                        if sending:
                            original = f.telegram.call

                            async def during_send(method, **params):
                                if method == 'sendMessage' and params.get('text') == prompt:
                                    before = f.snapshot()
                                    f.number += 1
                                    outcome = await f.service.accept_update(private(f.number, '/start fill'))
                                    self.assertEqual(outcome, 'envelope_start')
                                    assert_unchanged(before)
                                return await original(method, **params)

                            with patch.object(f.telegram, 'call', side_effect=during_send):
                                f.deliver()
                        else:
                            self.assertEqual(f.send('/start fill'), 'envelope_start')
                            assert_unchanged(before)
                            f.deliver()
                        sent = [params['text'] for method, params in f.telegram.calls[mark:]
                                if method == 'sendMessage' and params.get('text') == prompt]
                        self.assertEqual(sent, [prompt])
                        self.assertEqual(f.send(VALUE, date=f.telegram.date + 1), 'envelope_fill')
                        self.assertEqual(f.vault.values, {'NPM_TOKEN': VALUE})
                    finally:
                        f.tearDown()
                        f.doCleanups()

    def test_fill_start_resend_keeps_after_prompt_rule(self):
        self.pause()
        self.ask('NPM_TOKEN')
        self.fail_private_prompt()
        self.assertEqual(self.send(VALUE, date=self.telegram.date + 1), 'envelope_reject')
        self.assertEqual(self.events()[-1], ('reject', 'before_prompt', 'intake'))
        self.assertEqual(self.vault.calls, [])
        self.assertEqual(self.send('/start fill'), 'envelope_start')
        self.assertEqual(self.replies()[-1], 'Send the value for NPM_TOKEN now in one message. '
                         'I will delete it after I store it. Send /cancel to stop.')
        self.assertEqual(self.send(VALUE, date=self.telegram.date + 1), 'envelope_reject')
        self.assertEqual(self.events()[-1], ('reject', 'before_prompt', 'intake'))
        self.assertEqual(self.vault.calls, [])
        self.deliver()
        self.assertEqual(self.send(VALUE, date=self.telegram.date), 'envelope_reject')
        self.assertEqual(self.events()[-1], ('reject', 'before_prompt', 'intake'))
        self.assertEqual(self.vault.calls, [])
        self.assertEqual(self.send(VALUE, date=self.telegram.date + 1), 'envelope_fill')
        self.assertEqual(self.vault.values, {'NPM_TOKEN': VALUE})

    def test_fill_callback_uses_supported_start_link(self):
        self.pause()
        self.ask('NPM_TOKEN')
        self.assertEqual(self.tap(self.envelope('NPM_TOKEN')['id']), 'envelope_fill')
        self.assertEqual(self.answers(), [{'callback_query_id': '1', 'url': 'https://t.me/' + BOT + '?start=fill'}])
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM problems WHERE code='callback-failed'").fetchone()[0], 0)

    def test_fill_start_preserves_armed_prompt_and_next_value(self):
        self.pause()
        self.ask('NPM_TOKEN')
        self.ask('DEPLOY_TOKEN')
        self.assertEqual(self.tap(self.envelope('NPM_TOKEN')['id']), 'envelope_fill')
        self.deliver()
        before = self.snapshot()
        prompt = self.store.get('envelope_prompt_date')
        self.assertEqual(self.send('/start fill', date=self.telegram.date + 1), 'envelope_start')
        after = self.snapshot()
        effect = after['envelope_effects'].pop()
        self.assertEqual((effect['kind'], effect['chat'], effect['message'], effect['state']),
                         ('delete', OWNER, 100 + self.number, 'pending'))
        self.assertEqual(after, before)
        self.assertEqual(self.store.get('envelope_prompt_date'), prompt)
        self.deliver()
        self.assertIn(('deleteMessage', {'chat_id': OWNER, 'message_id': 100 + self.number}), self.telegram.calls)
        self.assertEqual(self.send(VALUE, date=self.telegram.date + 2), 'envelope_fill')
        self.assertEqual(self.vault.values, {'NPM_TOKEN': VALUE})
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'filled')

    def test_fill_start_keeps_after_prompt_rule(self):
        self.pause()
        self.ask('NPM_TOKEN')
        self.assertEqual(self.tap(self.envelope('NPM_TOKEN')['id']), 'envelope_fill')
        before = self.replies()
        self.assertEqual(self.send('/start fill', date=self.telegram.date + 1), 'envelope_start')
        self.assertEqual(self.replies(), before)
        self.assertEqual(self.send(VALUE, date=self.telegram.date + 1), 'envelope_reject')
        self.assertEqual(self.events()[-1], ('reject', 'before_prompt', 'intake'))
        self.assertEqual(self.vault.calls, [])
        self.deliver()
        self.assertEqual(self.send(VALUE, date=self.telegram.date), 'envelope_reject')
        self.assertEqual(self.events()[-1], ('reject', 'before_prompt', 'intake'))
        self.assertEqual(self.vault.calls, [])
        self.assertEqual(self.send(VALUE, date=self.telegram.date + 1), 'envelope_fill')
        self.assertEqual(self.vault.values, {'NPM_TOKEN': VALUE})

    def test_fill_start_without_armed_ask_matches_bare_start(self):
        for waiting in (False, True):
            results = []
            for text in ('/start', '/start fill'):
                with self.subTest(waiting=waiting, text=text):
                    f = FillFixture()
                    f.setUp()
                    try:
                        if waiting:
                            f.ask()
                            f.expire()
                            f.ask('NPM_TOKEN')
                            f.ask('DEPLOY_TOKEN')
                        mark = f.count('envelope_effects')
                        events = len(f.events())
                        outcome = f.send(text)
                        effects = [dict(row) for row in f.store.db.execute(
                            'SELECT kind,chat,message,text FROM envelope_effects WHERE id>? ORDER BY id', (mark,))]
                        deletes = [row for row in effects if row['kind'] == 'delete']
                        results.append((outcome, [row for row in effects if row['kind'] != 'delete'],
                                        f.events()[events:], [tuple(row) for row in f.store.db.execute(
                                            'SELECT name,state,chat FROM envelopes ORDER BY id')]))
                        self.assertEqual(deletes, [{'kind': 'delete', 'chat': OWNER, 'message': 101, 'text': None}]
                                         if text == '/start fill' else [])
                    finally:
                        f.tearDown()
                        f.doCleanups()
            self.assertEqual(results[1], results[0])

    def test_other_start_text_while_paused_does_not_change_state_or_block_fill(self):
        self.pause()
        self.ask('NPM_TOKEN')
        paused = self.store.get('envelope_prompt_exit')
        self.assertFalse(paused['acknowledged'])
        before = self.snapshot()
        self.assertEqual(self.send('/start arbitrary text'), 'envelope_start')
        after = self.snapshot()
        reply = after['envelope_effects'].pop()
        self.assertEqual((reply['kind'], reply['text']),
                         ('reply', 'Open an Envelope card in your Torii channel and tap Fill privately.'))
        self.assertEqual(after, before)
        self.assertEqual(self.store.get('envelope_prompt_exit'), paused)
        envelope = self.envelope('NPM_TOKEN')
        self.assertEqual(self.tap(envelope['id']), 'envelope_fill')
        self.deliver()
        self.assertEqual(self.send(VALUE, date=self.telegram.date + 1), 'envelope_fill')
        self.assertEqual(self.vault.values, {'NPM_TOKEN': VALUE})
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'filled')

    def test_fill_start_with_only_handoff_matches_bare_start(self):
        with self.store.db:
            envelopes.ask(self.store, TOPIC, envelopes.HANDOFF, 'Sign in', 'Claude')
            self.store.put('account_signin', {'provider': 'claude', 'state': 'code'})
        row = self.envelope(envelopes.HANDOFF)
        with self.store.db:
            self.assertIsNone(envelopes._arm(self.store, row, OWNER, None, row['created'], 'intake'))
        self.assertEqual(self.send('/start'), 'envelope_start')
        expected = self.replies()[-1]
        self.assertEqual(expected, envelopes.SIGNIN_REPLIES['bare_start'])
        before = self.snapshot()
        self.assertEqual(self.send('/start fill'), 'envelope_start')
        after = self.snapshot()
        effects = after['envelope_effects'][len(before['envelope_effects']):]
        self.assertEqual([effect['kind'] for effect in effects], ['delete', 'reply'])
        self.assertEqual(effects[0]['message'], 100 + self.number)
        self.assertEqual(effects[1]['text'], expected)
        after['envelope_effects'] = before['envelope_effects']
        self.assertEqual(after, before)
        with self.store.db:
            envelopes.cancel(self.store, row['id'], 'button')

    def test_F1_waiting_paused_and_asked_again_cards_have_callbacks(self):
        self.ask()
        self.ask('NPM_TOKEN')
        self.assert_buttons('NPM_TOKEN')
        old = self.expire()
        self.assertFalse(self.store.get('envelope_prompt_exit')['acknowledged'])
        self.ask('DEPLOY_TOKEN')
        self.assert_buttons('DEPLOY_TOKEN')
        self.assertEqual(self.tap(old['id'], 'again'), 'envelope_again')
        self.assert_buttons('GITHUB_TOKEN')
        self.assertNotEqual(self.envelope()['id'], old['id'])
        with self.store.db:
            envelopes.ask(self.store, TOPIC, envelopes.HANDOFF, 'Sign in', 'Claude')
        self.assertEqual(self.card(envelopes.HANDOFF)['markup'], {'inline_keyboard': [[
            {'text': 'Cancel', 'callback_data': 'envelope:%d:cancel' % self.envelope(envelopes.HANDOFF)['id']}]]})

    def test_F2_paused_tap_arms_prompts_opens_chat_and_fills_end_to_end(self):
        self.pause()
        self.ask('NPM_TOKEN')
        self.deliver()
        old = self.envelope('NPM_TOKEN')
        self.assert_buttons('NPM_TOKEN')
        message = envelopes.card_message(self.store, old['card_outbox'])
        before = len(self.replies())
        now = old['created'] + 100
        with patch('coordinator.envelopes.time.time', return_value=now):
            self.assertEqual(self.tap(old['id']), 'envelope_fill')
        armed = self.envelope('NPM_TOKEN')
        self.assertEqual((armed['state'], armed['chat'], armed['arm_thread'], armed['armed_at'], armed['expires']),
                         ('armed', OWNER, None, now, now + 600))
        self.assertIsNone(self.store.get('envelope_prompt_exit'))
        self.assertEqual(self.events()[-1], ('arm', None, 'button'))
        self.assertEqual(self.replies()[before:], ['Send the value for NPM_TOKEN now in one message. '
                         'I will delete it after I store it. Send /cancel to stop.'])
        effect = self.store.db.execute('SELECT * FROM envelope_effects ORDER BY id DESC LIMIT 1').fetchone()
        self.assertEqual((effect['envelope'], effect['chat'], effect['thread'], effect['state']),
                         (old['id'], OWNER, None, 'pending'))
        card = self.card('NPM_TOKEN')
        self.assertEqual((card['edit_message'], card['text'], card['markup']),
                         (message, 'Envelope: NPM_TOKEN\nWhy: Push the release branch\nUsed by: release worker\n'
                          'Job: any job\nWaiting for the value in the private chat. Tap Fill privately to open it.\n'
                          'Never paste it in this channel.', {'inline_keyboard': [
                              [{'text': 'Fill privately', 'url': 'https://t.me/' + BOT}],
                              [{'text': 'Cancel', 'callback_data': 'envelope:%d:cancel' % old['id']}]]}))
        self.assertEqual(self.answers(), [{'callback_query_id': '1', 'url': 'https://t.me/' + BOT + '?start=fill'}])
        self.deliver()
        self.assertEqual(self.store.get('envelope_prompt_date'), {'envelope': old['id'], 'date': self.telegram.date})
        self.assertEqual(self.send(VALUE, date=self.telegram.date + 1), 'envelope_fill')
        self.assertEqual(self.vault.values, {'NPM_TOKEN': VALUE})
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'filled')
        self.assertIn('Filled.', self.card('NPM_TOKEN')['text'])
        self.assertIsNone(self.card('NPM_TOKEN')['markup'])

    def test_F3_busy_tap_changes_no_credential_row(self):
        self.ask()
        self.ask('NPM_TOKEN')
        self.assert_buttons('NPM_TOKEN')
        before = self.snapshot()
        self.assertEqual(self.tap(self.envelope('NPM_TOKEN')['id']), 'envelope_busy')
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.answers(), [{'callback_query_id': '1', 'text':
                         'Another credential ask is waiting in your private chat. '
                         'Send its value or tap Cancel on its card first.'}])

    def test_F4_expired_closed_unknown_handoff_and_already_armed_are_inert(self):
        self.pause()
        expired = self.envelope()
        self.ask('CANCEL_TOKEN')
        cancelled = self.envelope('CANCEL_TOKEN')
        self.assertEqual(self.tap(cancelled['id'], 'cancel'), 'envelope_cancel')
        self.ask('REVOKE_TOKEN')
        revoked = self.envelope('REVOKE_TOKEN')
        self.assertTrue(self.call('secret.revoke', name='REVOKE_TOKEN').ok)
        asyncio.run(self.service.controls_once())
        self.ask('FILL_TOKEN')
        filled = self.envelope('FILL_TOKEN')
        self.assertEqual(self.tap(filled['id']), 'envelope_fill')
        self.deliver()
        self.assertEqual(self.send(VALUE, date=self.telegram.date + 1), 'envelope_fill')
        self.ask('ARMED_TOKEN')
        armed = self.envelope('ARMED_TOKEN')
        self.assertEqual(armed['state'], 'armed')
        with self.store.db:
            handoff = envelopes.ask(self.store, TOPIC, envelopes.HANDOFF, 'Sign in', 'Claude')
        for envelope_id, outcome, text in (
                (expired['id'], 'envelope_expired', 'This ask expired. Tap Ask again on its card.'),
                (cancelled['id'], 'envelope_closed', 'This envelope is already closed.'),
                (revoked['id'], 'envelope_closed', 'This envelope is already closed.'),
                (filled['id'], 'envelope_closed', 'This envelope is already closed.'),
                (999999, 'envelope_closed', 'This envelope is already closed.'),
                (handoff, 'envelope_closed', 'This envelope is already closed.'),
                (armed['id'], 'envelope_fill', None)):
            with self.subTest(envelope_id=envelope_id):
                before = self.snapshot()
                self.assertEqual(self.tap(envelope_id), outcome)
                self.assertEqual(self.snapshot(), before)
                answer = {'callback_query_id': str(self.number)}
                answer.update({'url': 'https://t.me/' + BOT + '?start=fill'} if text is None else {'text': text})
                self.assertEqual(self.answers()[-1], answer)

    def test_F4_tap_sweeps_an_unswept_expiry(self):
        self.pause()
        self.ask('NPM_TOKEN')
        expired = self.expire('NPM_TOKEN', sweep=False)
        before = self.replies()
        self.assertEqual(self.tap(expired['id']), 'envelope_expired')
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'expired')
        self.assertEqual(self.replies(), before)
        self.assertEqual(self.events()[-1], ('expire', None, 'sweep'))

    def test_F5_other_users_bots_and_chats_cannot_arm(self):
        self.pause()
        self.ask('NPM_TOKEN')
        for sender in ({'user': 99}, {'bot': True}, {'chat': GROUP - 1}):
            with self.subTest(sender=sender):
                before = self.snapshot()
                self.assertEqual(self.tap(self.envelope('NPM_TOKEN')['id'], **sender), 'unauthorized')
                self.assertEqual(self.snapshot(), before)
                self.assertEqual(self.answers()[-1], {'callback_query_id': str(self.number),
                                 'text': 'Only the paired owner can change Torii settings.'})
        self.assertEqual(self.tap(self.envelope('NPM_TOKEN')['id']), 'envelope_fill')

    def test_F6_rejected_url_gets_one_text_fallback_and_keeps_prompt(self):
        self.pause()
        self.ask('NPM_TOKEN')
        self.telegram.failures['answerCallbackQuery'] = [TelegramError(400)]
        before = len(self.replies())
        self.assertEqual(self.tap(self.envelope('NPM_TOKEN')['id']), 'envelope_fill')
        self.assertEqual(self.answers(), [
            {'callback_query_id': '1', 'url': 'https://t.me/' + BOT + '?start=fill'},
            {'callback_query_id': '1', 'text': 'Send the value in your private chat with me.'}])
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'armed')
        self.assertEqual(self.replies()[before:], ['Send the value for NPM_TOKEN now in one message. '
                         'I will delete it after I store it. Send /cancel to stop.'])
        effect = self.store.db.execute('SELECT * FROM envelope_effects ORDER BY id DESC LIMIT 1').fetchone()
        self.assertEqual(effect['state'], 'pending')
        self.assertEqual(self.store.db.execute("SELECT detail FROM problems WHERE code='callback-failed'").fetchone()[0],
                         'telegram=400')

    def test_F7_expiry_and_unarmed_replies_point_to_the_card(self):
        self.ask()
        self.ask('NPM_TOKEN')
        before = len(self.replies())
        self.expire()
        self.assertEqual(self.replies()[before:], ["GITHUB_TOKEN expired, so I won't take a value for it. "
                         'To give the next one, tap Fill privately on its card.'])
        self.assertEqual(self.send(VALUE), 'envelope_reject')
        self.assertEqual(self.replies()[-1], 'No envelope is waiting for a value, so I deleted your message. '
                         'Tap Fill privately on an Envelope card first.')
        self.assertEqual(self.send('/cancel'), 'envelope_reject')
        self.assertEqual(self.replies()[-1],
                         'No credential ask is taking a value. Another ask is waiting. Tap Fill privately on its card.')


if __name__ == '__main__':
    unittest.main()
