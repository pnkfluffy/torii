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

    def card(self, name='GITHUB_TOKEN'):
        return self.outbox(self.envelope(name)['card_outbox'])

    def replies(self):
        return [row[0] for row in self.store.db.execute("SELECT text FROM envelope_effects WHERE kind='reply' ORDER BY id")]

    def fail_private_prompt(self, name='NPM_TOKEN'):
        self.telegram.private_started = False
        envelope = self.envelope(name)
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

    def assert_buttons(self, name, fill):
        envelope = self.envelope(name)
        self.assertEqual(self.card(name)['markup']['inline_keyboard'], [
            [dict({'text': 'Fill privately'}, **fill)],
            [{'text': 'Cancel', 'callback_data': 'envelope:%d:cancel' % envelope['id']}]])


def prompt(name):
    return ('Envelope: %s\nWhy: Push the release branch\nUsed by: release worker\nJob: any job\n'
            'Reply to this message with the value in one message within 24 hours. I delete it after I store it. '
            'Reply /cancel to stop.') % name


class FillButtonTests(FillFixture):
    def test_tap_on_an_armed_ask_opens_the_private_chat_and_changes_nothing(self):
        self.ask()
        self.assert_buttons('GITHUB_TOKEN', {'url': 'https://t.me/' + BOT})
        before = self.snapshot()
        self.assertEqual(self.tap(self.envelope()['id']), 'envelope_fill')
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.answers(), [{'callback_query_id': '1', 'url': 'https://t.me/' + BOT + '?start=fill'}])
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM problems WHERE code='callback-failed'").fetchone()[0], 0)

    def test_ask_before_the_owner_is_known_gets_its_prompt_later(self):
        with self.store.db:
            self.store.db.execute("DELETE FROM settings WHERE key='owner'")
        self.ask()
        self.assertEqual(self.envelope()['state'], 'open')
        self.assert_buttons('GITHUB_TOKEN', {'callback_data': 'envelope:%d:fill' % self.envelope()['id']})
        self.assertIn('Tap Fill privately within 24 hours', self.card()['text'])
        self.assertEqual(self.replies(), [])
        with self.store.db:
            self.store.put('owner', OWNER)
        self.assertEqual(self.tap(self.envelope()['id']), 'envelope_fill')
        self.assertEqual(self.envelope()['state'], 'armed')
        self.assertEqual(self.replies(), [prompt('GITHUB_TOKEN')])
        self.assert_buttons('GITHUB_TOKEN', {'url': 'https://t.me/' + BOT})
        self.deliver()
        self.assertEqual(self.send(VALUE, date=self.telegram.date + 1), 'envelope_fill')
        self.assertEqual(self.vault.values, {'GITHUB_TOKEN': VALUE})

    def test_fill_start_resends_a_failed_prompt_and_stores_the_next_value(self):
        self.ask()
        self.deliver()
        self.ask('NPM_TOKEN')
        self.fail_private_prompt('NPM_TOKEN')
        before = len(self.replies())
        mark = len(self.telegram.calls)
        self.assertEqual(self.send('/start fill'), 'envelope_arm')
        self.assertEqual(self.replies()[before:], [prompt('NPM_TOKEN')])
        self.deliver()
        sent = [params['text'] for method, params in self.telegram.calls[mark:]
                if method == 'sendMessage' and params['chat_id'] == OWNER]
        self.assertEqual(sent, [prompt('NPM_TOKEN')])
        self.assertIn(('deleteMessage', {'chat_id': OWNER, 'message_id': 100 + self.number}), self.telegram.calls)
        self.assertEqual(self.envelope('NPM_TOKEN')['prompt_date'], self.telegram.date)
        prompt_message = self.envelope('NPM_TOKEN')['prompt_message']
        self.assertEqual(self.send(VALUE, date=self.telegram.date + 1,
                                   reply_to_message={'message_id': prompt_message}), 'envelope_fill')
        self.assertEqual(self.vault.values, {'NPM_TOKEN': VALUE})

    def test_failed_prompt_refuses_plain_values_until_it_is_sent(self):
        self.ask()
        self.fail_private_prompt('GITHUB_TOKEN')
        self.assertEqual(self.send(VALUE, date=self.telegram.date + 1), 'envelope_reject')
        self.assertEqual(self.events()[-1], ('reject', 'before_prompt', 'intake'))
        self.assertEqual(self.send('/start fill'), 'envelope_arm')
        self.assertEqual(self.send(VALUE, date=self.telegram.date + 1), 'envelope_reject')
        self.assertEqual(self.events()[-1], ('reject', 'before_prompt', 'intake'))
        self.deliver()
        self.assertEqual(self.send(VALUE, date=self.telegram.date), 'envelope_reject')
        self.assertEqual(self.send(VALUE, date=self.telegram.date + 1), 'envelope_fill')
        self.assertEqual(self.vault.values, {'GITHUB_TOKEN': VALUE})

    def test_fill_start_keeps_one_queued_or_sending_prompt(self):
        self.ask()
        self.assertEqual(self.send('/start fill'), 'envelope_arm')
        self.assertEqual(self.replies().count(prompt('GITHUB_TOKEN')), 1)
        original = self.telegram.call

        async def during_send(method, **params):
            if method == 'sendMessage' and params.get('text') == prompt('GITHUB_TOKEN'):
                self.number += 1
                self.assertEqual(await self.service.accept_update(private(self.number, '/start fill')), 'envelope_arm')
            return await original(method, **params)

        with patch.object(self.telegram, 'call', side_effect=during_send):
            self.deliver()
        sent = [params['text'] for method, params in self.telegram.calls if method == 'sendMessage']
        self.assertEqual(sent.count(prompt('GITHUB_TOKEN')), 1)
        self.assertEqual(self.send(VALUE, date=self.telegram.date + 1), 'envelope_fill')

    def test_fill_start_without_an_ask_matches_bare_start(self):
        results = []
        for text in ('/start', '/start fill'):
            f = FillFixture()
            f.setUp()
            try:
                f.ask()
                f.expire()
                mark = f.count('envelope_effects')
                outcome = f.send(text)
                effects = [dict(row) for row in f.store.db.execute(
                    'SELECT kind,chat,message,text FROM envelope_effects WHERE id>? ORDER BY id', (mark,))]
                self.assertEqual([row for row in effects if row['kind'] == 'delete'],
                                 [{'kind': 'delete', 'chat': OWNER, 'message': 101, 'text': None}]
                                 if text == '/start fill' else [])
                results.append((outcome, [row for row in effects if row['kind'] != 'delete']))
            finally:
                f.tearDown()
                f.doCleanups()
        self.assertEqual(results[1], results[0])
        self.assertEqual(results[0][0], 'envelope_start')

    def test_other_start_text_changes_nothing(self):
        self.ask()
        before = self.snapshot()
        self.assertEqual(self.send('/start arbitrary text'), 'envelope_start')
        after = self.snapshot()
        reply = after['envelope_effects'].pop()
        self.assertEqual((reply['kind'], reply['text']), ('reply', envelopes.REPLIES['bare_start']))
        self.assertEqual(after, before)

    def test_asked_again_and_handoff_cards(self):
        old = self.ask() and self.expire()
        self.assertEqual(self.tap(old['id'], 'again'), 'envelope_again')
        self.assertNotEqual(self.envelope()['id'], old['id'])
        self.assert_buttons('GITHUB_TOKEN', {'url': 'https://t.me/' + BOT})
        with self.store.db:
            envelopes.ask(self.store, TOPIC, envelopes.HANDOFF, 'Sign in', 'Claude')
        self.assertEqual(self.card(envelopes.HANDOFF)['markup'], {'inline_keyboard': [[
            {'text': 'Cancel', 'callback_data': 'envelope:%d:cancel' % self.envelope(envelopes.HANDOFF)['id']}]]})

    def test_expired_closed_unknown_and_handoff_taps_are_inert(self):
        self.ask()
        expired = self.expire()
        self.ask('CANCEL_TOKEN')
        cancelled = self.envelope('CANCEL_TOKEN')
        self.assertEqual(self.tap(cancelled['id'], 'cancel'), 'envelope_cancel')
        self.ask('REVOKE_TOKEN')
        revoked = self.envelope('REVOKE_TOKEN')
        self.assertTrue(self.call('secret.revoke', name='REVOKE_TOKEN').ok)
        asyncio.run(self.service.controls_once())
        self.ask('FILL_TOKEN')
        filled = self.envelope('FILL_TOKEN')
        self.deliver()
        self.assertEqual(self.send(VALUE, date=self.telegram.date + 1), 'envelope_fill')
        with self.store.db:
            handoff = envelopes.ask(self.store, TOPIC, envelopes.HANDOFF, 'Sign in', 'Claude')
        for envelope_id, outcome, text in (
                (expired['id'], 'envelope_expired', 'This ask expired. Tap Ask again on its card.'),
                (cancelled['id'], 'envelope_closed', 'This envelope is already closed.'),
                (revoked['id'], 'envelope_closed', 'This envelope is already closed.'),
                (filled['id'], 'envelope_closed', 'This envelope is already closed.'),
                (999999, 'envelope_closed', 'This envelope is already closed.'),
                (handoff, 'envelope_closed', 'This envelope is already closed.')):
            with self.subTest(envelope_id=envelope_id):
                before = self.snapshot()
                self.assertEqual(self.tap(envelope_id), outcome)
                self.assertEqual(self.snapshot(), before)
                self.assertEqual(self.answers()[-1], {'callback_query_id': str(self.number), 'text': text})

    def test_tap_sweeps_an_unswept_expiry(self):
        self.ask('NPM_TOKEN')
        expired = self.expire('NPM_TOKEN', sweep=False)
        self.assertEqual(self.tap(expired['id']), 'envelope_expired')
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'expired')
        self.assertEqual(self.events()[-1], ('expire', None, 'sweep'))

    def test_other_users_bots_and_chats_cannot_tap(self):
        self.ask('NPM_TOKEN')
        for sender in ({'user': 99}, {'bot': True}, {'chat': GROUP - 1}):
            with self.subTest(sender=sender):
                before = self.snapshot()
                self.assertEqual(self.tap(self.envelope('NPM_TOKEN')['id'], **sender), 'unauthorized')
                self.assertEqual(self.snapshot(), before)
                self.assertEqual(self.answers()[-1], {'callback_query_id': str(self.number),
                                 'text': 'Only the paired owner can change Torii settings.'})

    def test_rejected_url_gets_one_text_fallback(self):
        self.ask('NPM_TOKEN')
        self.telegram.failures['answerCallbackQuery'] = [TelegramError(400)]
        self.assertEqual(self.tap(self.envelope('NPM_TOKEN')['id']), 'envelope_fill')
        self.assertEqual(self.answers(), [
            {'callback_query_id': '1', 'url': 'https://t.me/' + BOT + '?start=fill'},
            {'callback_query_id': '1', 'text': 'Send the value in your private chat with me.'}])
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'armed')
        self.assertEqual(self.store.db.execute("SELECT detail FROM problems WHERE code='callback-failed'").fetchone()[0],
                         'telegram=400')


if __name__ == '__main__':
    unittest.main()
