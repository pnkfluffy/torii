"""Offline Antigravity sign-in from Telegram against the fake agy CLI and fake Telegram."""

import asyncio
import json
import signal
import sys
import time
import unittest
from unittest.mock import patch

from coordinator import antigravity_accounts, antigravity_signin, control_api, signin
from coordinator.store import Store
from tests.fake_agy import sign_in
from tests.support import SETTLE, until
from tests.test_antigravity_runtime import AntigravityCase


TOPIC = '-10042:4'
LINK = ('https://accounts.google.com/o/oauth2/auth?access_type=offline&client_id=fixture&code_challenge=fixture'
        '&redirect_uri=http%3A%2F%2Flocalhost%3A39999%2Fauth%2Fcallback&response_type=code&scope=openid&state=fixture-state')


class AntigravityLinkTests(unittest.TestCase):
    def test_only_a_google_link_with_a_loopback_return_is_accepted(self):
        link = antigravity_signin.parse_link(LINK + '\n')
        self.assertEqual(link, {'url': LINK, 'port': 39999, 'path': '/auth/callback', 'state': 'fixture-state'})
        for broken in (LINK.replace('accounts.google.com', 'accounts.google.com.example.test'),
                       LINK.replace('https://', 'http://'), LINK.replace('localhost', 'example.test'),
                       LINK.replace('%3A39999', '%3A80'), LINK.replace('%3A39999', ''), LINK.replace('&state=fixture-state', ''),
                       LINK.replace('response_type=code', 'response_type=token'), LINK + '&state=two', LINK + ' x',
                       LINK.replace('/o/oauth2/auth', '/signin'), '', None):
            with self.subTest(broken=broken):
                self.assertIsNone(antigravity_signin.parse_link(broken))

    def test_a_return_address_must_belong_to_the_link(self):
        link = antigravity_signin.parse_link(LINK)
        good = 'http://localhost:39999/auth/callback?state=fixture-state&code=4/abc-DEF_1&scope=openid'
        self.assertEqual(antigravity_signin.parse_callback(good, link),
                         '/auth/callback?state=fixture-state&code=4/abc-DEF_1&scope=openid')
        self.assertEqual(antigravity_signin.parse_callback(' ' + good.replace('localhost', '127.0.0.1') + '\n', link),
                         '/auth/callback?state=fixture-state&code=4/abc-DEF_1&scope=openid')
        self.assertEqual(antigravity_signin.parse_callback(
            'http://localhost:39999/auth/callback?error=access_denied&state=fixture-state', link),
            '/auth/callback?error=access_denied&state=fixture-state')
        for broken in (good.replace('39999', '40000'), good.replace('fixture-state', 'other'),
                       good.replace('/auth/callback', '/other'), good.replace('http://', 'https://'),
                       good.replace('localhost', 'example.test'), good.replace('&code=4/abc-DEF_1', ''),
                       good + '&code=two', good + '&error=x', good + '#fragment', good + ' and text',
                       good + '\r\nX-Injected: 1', 'not a url'):
            with self.subTest(broken=broken):
                self.assertIsNone(antigravity_signin.parse_callback(broken, link))


class AntigravitySignInTests(AntigravityCase):
    def setUp(self):
        super().setUp()
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,-10042,4,'Test','',1)",
                                  (TOPIC,))
            self.store.put('owner', 7)
            self.store.put('group', -10042)
            self.store.put('bot_username', 'fake_bot')
            self.store.put('antigravity_enabled', True)
        self.task = None
        self.driver = signin.SignIns(self.store, poll=.01, link_seconds=SETTLE, exit_seconds=SETTLE)

    async def asyncTearDown(self):
        while self.task and not self.task.done():
            self.task.cancel()
            await asyncio.wait((self.task,), timeout=0.2)

    def add(self, **params):
        with self.store.db:
            return control_api.call(self.store, 'account.add', dict({'provider': 'antigravity'}, **params),
                                    source='telegram', topic=TOPIC)

    async def card(self):
        self.task = self.task or asyncio.create_task(self.driver.run())
        await until(lambda: (self.store.get(signin.KEY) or {}).get('state') == 'code', 'the Antigravity sign-in card')
        return self.store.db.execute('SELECT text,reply_markup FROM outbox WHERE id=?',
                                     (self.store.get(signin.KEY)['card_outbox'],)).fetchone()

    async def settled(self):
        await until(lambda: self.store.get(signin.KEY) is None or self.task.done(), 'the Antigravity sign-in to settle')
        if self.task.done():
            self.task.result()

    def paste(self, text, message=77, **extra):
        with self.store.db:
            return signin.intercept_topic_code(self.store, TOPIC, dict(
                {'text': text, 'chat': {'id': -10042}, 'message_id': message, 'message_thread_id': 4}, **extra))

    def reports(self):
        return [row['text'] for row in self.store.db.execute('SELECT text FROM outbox ORDER BY id')]

    def deleted(self):
        return [row['message'] for row in self.store.db.execute(
            "SELECT message FROM envelope_effects WHERE kind='delete' ORDER BY id")]

    def address(self, card, code='good-code', state='fixture-state'):
        link = antigravity_signin.parse_link(next(line for line in card['text'].splitlines() if line.startswith('https://')))
        return 'http://localhost:%d/auth/callback?state=%s&code=%s&scope=openid' % (link['port'], state, code)

    async def test_owner_signs_in_with_a_pasted_return_address(self):
        started = self.add()
        self.assertEqual((started.ok, started.state), (True, 'queued'))
        record = self.store.get(signin.KEY)
        home = self.account.parent / 'antigravity'
        self.assertEqual((record['alias'], record['config_dir'], record['provider']), ('antigravity', str(home), 'antigravity'))
        card = await self.card()
        self.assertIn('https://accounts.google.com/o/oauth2/auth?', card['text'])
        self.assertIn('(it starts with http://localhost:', card['text'])
        self.assertEqual(json.loads(card['reply_markup'])['inline_keyboard'][0][0]['callback_data'],
                         signin.cancel_data(self.store.get(signin.KEY)))
        launch = self.calls('initialize')[0]
        self.assertEqual((launch['argv'], launch['home'], launch['display']), (['acp'], str(home), ':0'))
        self.assertFalse(self.paste('What is the status of the build?'))
        self.assertTrue(self.paste(self.address(card)))
        await self.settled()
        self.assertEqual(self.deleted(), [77])
        callback = self.calls('callback')[0]['message']
        self.assertEqual((callback['query']['code'], callback['query']['state'], callback['host'].split(':')[0]),
                         (['good-code'], ['fixture-state'], 'localhost'))
        broker = antigravity_accounts.AntigravityBroker(self.store)
        self.assertEqual((broker.active(), broker.signed_in('antigravity'), broker.label('antigravity')),
                         ('antigravity', True, 'antigravity'))
        self.assertEqual(self.reports()[-1], 'Antigravity account antigravity is signed in.')
        saved = json.dumps([dict(row) for row in self.store.db.execute('SELECT * FROM outbox')]
                           + [dict(row) for row in self.store.db.execute('SELECT * FROM settings')], default=str)
        self.assertNotIn('good-code', saved)
        self.assertEqual(list((home / 'tmp').glob('torii-signin-*')), [])

    async def test_wrong_addresses_are_deleted_and_refused(self):
        self.add()
        card = await self.card()
        good = self.address(card)
        cases = ((good.replace('fixture-state', 'other-state'), {}, "It isn't the return address of this sign-in."),
                 (good.replace('/auth/callback', '/other'), {}, "It isn't the return address of this sign-in."),
                 (good, {'forward_date': 1}, 'I deleted that forwarded message.'))
        for number, (text, extra, reply) in enumerate(cases):
            with self.subTest(text=text, extra=extra):
                self.assertTrue(self.paste(text, message=100 + number, **extra))
                self.assertIn(reply, self.reports()[-1])
        self.assertEqual(self.deleted(), [100, 101, 102])
        self.assertEqual(self.calls('callback'), [])
        self.assertEqual(self.store.get(signin.KEY)['state'], 'code')
        self.assertTrue(self.paste(self.address(card, code='bad-code'), message=200))
        await self.settled()
        self.assertEqual(self.reports()[-1], 'Google did not grant the sign-in. No account was added.')
        self.assertEqual(antigravity_accounts.accounts(self.store), {})

    async def test_cancel_stops_the_process_and_adds_no_account(self):
        self.add()
        await self.card()
        pid = self.store.get(signin.KEY)['pid']
        with self.store.db:
            self.assertTrue(control_api.call(self.store, 'account.signin_cancel', {}, source='telegram', topic=TOPIC).ok)
        await self.settled()
        self.assertEqual(self.reports()[-1], 'Antigravity sign-in cancelled. No account was added.')
        self.assertEqual(antigravity_accounts.accounts(self.store), {})
        await until(lambda: not signin._leftover(pid, ' acp'), 'the sign-in process to stop')
        self.assertFalse(self.paste('http://localhost:1/auth/callback?code=x&state=y'))

    async def test_expired_and_unexpected_links_end_the_sign_in(self):
        self.driver.code_seconds = 0.2
        self.add()
        await self.card()
        await self.settled()
        self.assertEqual(self.reports()[-1], 'The Antigravity sign-in link expired. No account was added.')
        self.scenario(link_host='accounts.example.test')
        self.add()
        self.task = self.task or asyncio.create_task(self.driver.run())
        await self.settled()
        self.assertEqual(self.reports()[-1], 'Antigravity returned an unexpected sign-in link. No account was added.')

    async def test_signed_out_account_signs_in_again_in_its_own_profile(self):
        self.register()
        self.assertEqual(self.add(alias='one').text, 'one is already signed in.')
        (self.account / 'signed-in').unlink()
        statuses = self.store.get('antigravity_account_status')
        statuses['one']['logged_in'] = False
        self.store.put('antigravity_account_status', statuses)
        self.store.put('antigravity_account_blocks', {'one': time.time() + 600})
        self.assertTrue(self.add(alias='one').ok)
        self.assertEqual(self.store.get(signin.KEY)['config_dir'], str(self.account))
        card = await self.card()
        self.assertIn('Sign in to Antigravity account one again', card['text'])
        self.paste(self.address(card))
        await self.settled()
        broker = antigravity_accounts.AntigravityBroker(self.store)
        self.assertEqual((sorted(broker.accounts()), broker.signed_in('one'), broker.held('one')), (['one'], True, False))

    async def test_the_starting_message_shows_at_once_and_the_card_replaces_it(self):
        self.add()
        record = self.store.get(signin.KEY)
        placeholder = self.store.db.execute('SELECT id,text FROM outbox WHERE id=?', (record['card_outbox'],)).fetchone()
        self.assertEqual(placeholder['text'], 'Starting the Antigravity sign-in…')
        with self.store.db:
            self.store.delivered(placeholder['id'], 555)
        card = await self.card()
        self.assertEqual(self.store.db.execute('SELECT edit_message FROM outbox WHERE id=?',
                                               (self.store.get(signin.KEY)['card_outbox'],)).fetchone()[0], 555)
        self.assertIn('1. Open this link and sign in to the Google account:\nhttps://accounts.google.com/', card['text'])
        self.assertIn("2. After you approve, the page doesn't load. That is expected. Copy that page's address "
                      '(it starts with http://localhost:', card['text'])

    async def test_an_unsent_starting_message_is_dropped_for_the_card(self):
        self.add()
        placeholder = self.store.get(signin.KEY)['card_outbox']
        await self.card()
        row = self.store.db.execute('SELECT retired FROM outbox WHERE id=?', (placeholder,)).fetchone()
        self.assertEqual(row['retired'], 1)
        self.assertIsNone(self.store.db.execute('SELECT edit_message FROM outbox WHERE id=?',
                                                (self.store.get(signin.KEY)['card_outbox'],)).fetchone()[0])

    async def test_a_cancelled_new_sign_in_moves_its_new_folder_to_the_trash(self):
        trash = self.root / 'Trash'
        with patch('coordinator.host_os.trash_directory', return_value=trash):
            self.add()
            await self.card()
            home = self.account.parent / 'antigravity'
            self.assertTrue(home.is_dir())
            with self.store.db:
                self.assertTrue(control_api.call(self.store, 'account.signin_cancel', {}, source='telegram', topic=TOPIC).ok)
            await self.settled()
            await until(lambda: not home.exists(), 'the new folder to leave')
        self.assertTrue((trash / 'antigravity').is_dir())
        self.assertTrue(self.account.is_dir())

    async def test_a_failed_sign_in_again_keeps_the_registered_folder(self):
        self.register()
        (self.account / 'signed-in').unlink()
        statuses = self.store.get('antigravity_account_status')
        statuses['one']['logged_in'] = False
        self.store.put('antigravity_account_status', statuses)
        self.driver.code_seconds = 0.2
        with patch('coordinator.host_os.trash_directory', return_value=self.root / 'Trash'):
            self.add(alias='one')
            await self.card()
            await self.settled()
        self.assertTrue(self.account.is_dir())
        self.assertFalse((self.root / 'Trash').exists())

    def test_registered_accounts_show_on_the_accounts_card_with_remove(self):
        from coordinator import control_ui
        self.register()
        text, rows = control_ui._accounts_view(self.store, 0)
        self.assertIn('**Antigravity** (left: week)\n', text)
        self.assertIn(' one: ', text)
        labels = [label for row in rows for label, _ in row]
        self.assertIn('Remove one (Antigravity)', labels)
        self.assertEqual(dict(label_action for row in rows for label_action in row)['Remove one (Antigravity)'],
                         {'view': 'remove:one'})
        detail = control_ui._remove_view(self.store, 'one')[0]
        self.assertTrue(detail.startswith('Antigravity one · active\n'))
        self.assertTrue(detail.endswith('Remove one from Torii? New work moves to your other accounts. '
                                        'Its profile folder goes to the Trash.'))
        statuses = self.store.get('antigravity_account_status')
        statuses['one']['logged_in'] = False
        self.store.put('antigravity_account_status', statuses)
        rows = control_ui._accounts_view(self.store, 0)[1]
        self.assertIn('Sign in one (Antigravity)', [label for row in rows for label, _ in row])
        self.assertEqual([label for row in control_ui._account_view(self.store, 'one')[1] for label, _ in row],
                         ['Sign in', 'Remove'])

    def test_the_browser_setting_is_gone(self):
        self.assertIsNone(control_api.find('antigravity.browser'))
        self.assertNotIn('browser', control_api.find('antigravity.signin').params)

    async def test_a_child_that_cannot_be_reaped_does_not_block_the_end_of_a_sign_in(self):
        signals = []

        class Process:
            pid = 2 ** 22 + 12345
            stdin = None

            def send_signal(self, number):
                signals.append(number)

            async def wait(self):
                await asyncio.sleep(3600)

        with patch('coordinator.antigravity_accounts.os.killpg', side_effect=PermissionError):
            await asyncio.wait_for(antigravity_accounts.end_group(Process(), seconds=0.05), 5)
            self.assertEqual(signals, [signal.SIGTERM, signal.SIGKILL])
            ending = asyncio.ensure_future(antigravity_accounts.end_group(Process(), seconds=3600))
            await asyncio.sleep(0.05)
            ending.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await ending

    def test_sign_in_needs_the_switch_and_one_open_sign_in(self):
        self.store.put('antigravity_enabled', False)
        self.assertEqual(self.add().text, 'Antigravity workers are off.')
        self.store.put('antigravity_enabled', True)
        self.assertTrue(self.add().ok)
        self.assertEqual(self.add().text, 'A Antigravity sign-in is still open. Finish it or tap Cancel on its card first.')
        self.assertEqual(antigravity_signin.free_alias({'antigravity', 'antigravity-2'}), 'antigravity-3')

    def test_restart_ends_an_open_sign_in_and_remove_moves_the_profile_folder_to_the_trash(self):
        self.register()
        self.add()
        self.store.put(signin.KEY, dict(self.store.get(signin.KEY), state='code', pid=None))
        signin.SignIns(self.store).recover()
        self.assertIsNone(self.store.get(signin.KEY))
        self.assertIsNone(self.store.get(signin.RETRY_KEY))
        self.assertEqual(self.reports()[-1], 'The Antigravity sign-in stopped because Torii restarted. No account was added.')
        trash = self.root / 'Trash'
        with self.store.db, patch('coordinator.host_os.trash_directory', return_value=trash):
            removed = control_api.call(self.store, 'account.remove', {'alias': 'one'}, source='telegram', topic=TOPIC)
        self.assertEqual((removed.ok, antigravity_accounts.accounts(self.store), self.store.get('antigravity_active_account')),
                         (True, {}, None))
        self.assertFalse(self.account.exists())
        self.assertTrue((trash / 'one' / 'signed-in').is_file())


if __name__ == '__main__':
    unittest.main()
