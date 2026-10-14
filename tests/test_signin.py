import asyncio
import io
import json
import logging
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from coordinator import control_api, control_ui
from coordinator.accounts import discover_accounts
from coordinator.envelopes import HANDOFF, sweep
from coordinator.signin import KEY, RETRY_DATA, RETRY_KEY, SignIns, account_name, looks_like_code, sign_in_link
from coordinator.store import Store
from tests.support import until
from tests.test_envelopes import private
from tests.test_store import update
from tests import test_codex_accounts as codex_tests


GOOD = 'GOOD-CODE-4711-abcdefgh#state-0123'
WRONG = 'WRONG-CODE-0001-abcdefgh#state-0123'
EMAIL = 'Jane.Doe+work@example.com'
TOPIC = '-10042:4'
OWNER = 7
BOT = 'torii_test_bot'
FAKE = '''#!/usr/bin/python3
import json
import os
import sys

home = os.environ['CLAUDE_CONFIG_DIR']
marker = os.path.join(home, 'fake-login')
if sys.argv[1:] == ['auth', 'status']:
    status = {'loggedIn': os.path.exists(marker)}
    if os.environ.get('FAKE_EMAIL'):
        status['email'] = os.environ['FAKE_EMAIL']
    print(json.dumps(status))
    sys.exit(0)
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'calls.txt'), 'a') as calls:
    calls.write(json.dumps({'argv': sys.argv[1:], 'home': home, 'pid': os.getpid(),
                            'token': 'ANTHROPIC_AUTH_TOKEN' in os.environ}) + '\\n')
if os.environ.get('FAKE_WRITES'):
    open(os.path.join(home, '.claude.json'), 'w').close()
print('Opening browser to sign in...')
print('\\x1b[1mBrowser did not open? Use the url below to sign in:\\x1b[0m')
print(os.environ.get('FAKE_LINK', 'https://claude.ai/oauth/authorize?code=true&state=state-0123'), flush=True)
sys.stdout.write('Paste code here if prompted > ')
sys.stdout.flush()
line = sys.stdin.readline().strip()
if line == os.environ['FAKE_GOOD']:
    open(marker, 'w').close()
    print('Login successful.')
    sys.exit(0)
print('Invalid code. Please make sure the full code was copied.')
sys.exit(1)
'''


class SignInTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.store = Store(self.root / 'state')
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,-10042,4,'Test','',1)",
                                  (TOPIC,))
            self.store.put('owner', OWNER)
            self.store.put('group', -10042)
            self.store.put('bot_username', BOT)
            self.store.put('accounts', {'default': {'config_dir': None, 'enabled': True}})
        self.accounts = self.root / 'accounts'
        root_patch = patch('coordinator.signin.accounts_root', return_value=self.accounts)
        root_patch.start()
        self.addCleanup(root_patch.stop)
        environment = patch.dict(os.environ, {'ANTHROPIC_AUTH_TOKEN': 'outer-token', 'FAKE_EMAIL': EMAIL,
                                              'FAKE_GOOD': GOOD})
        environment.start()
        self.addCleanup(environment.stop)
        self.fake = self.root / 'bin' / 'claude'
        self.fake.parent.mkdir()
        self.fake.write_text(FAKE)
        self.fake.chmod(0o755)
        self.log = io.StringIO()
        handler = logging.StreamHandler(self.log)
        logger = logging.getLogger('coordinator')
        logger.addHandler(handler)
        previous = logger.level
        logger.setLevel(logging.DEBUG)
        self.addCleanup(logger.removeHandler, handler)
        self.addCleanup(logger.setLevel, previous)
        self.task = None
        self.number = 100

    async def asyncTearDown(self):
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        self.store.close()
        self.temp.cleanup()

    def start(self, **options):
        self.driver = SignIns(self.store, str(self.fake), poll=0.01, **options)
        self.task = asyncio.ensure_future(self.driver.run())

    def next_number(self):
        self.number += 1
        return self.number

    def send(self, text):
        return self.store.accept(update(self.next_number(), text))

    def account_control(self, op='account.add'):
        number = self.next_number()
        with self.store.db:
            result = control_api.call(self.store, op, {}, topic=TOPIC, source='telegram', message=number)
            control_ui.view(self.store, TOPIC, 'accounts:0', number, prefix=result.text)
        return result

    def send_private(self, text):
        return self.store.accept(private(self.next_number(), text))

    def click(self, data):
        number = self.next_number()
        return self.store.accept({'update_id': number, 'callback_query': {
            'id': str(number), 'from': {'id': OWNER}, 'data': data,
            'message': {'message_id': 900, 'message_thread_id': 4, 'chat': {'id': -10042, 'type': 'supergroup'}}}})

    def texts(self):
        return [row[0] for row in self.store.db.execute('SELECT text FROM outbox ORDER BY id')]

    def calls(self):
        path = self.fake.parent / 'calls.txt'
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def envelopes(self):
        return [dict(row) for row in self.store.db.execute('SELECT * FROM envelopes WHERE name=? ORDER BY id',
                                                           (HANDOFF,))]

    def card(self, envelope):
        row = self.store.db.execute('SELECT text,reply_markup FROM outbox WHERE id=?',
                                    (envelope['card_outbox'],)).fetchone()
        return row['text'], json.loads(row['reply_markup']) if row['reply_markup'] else None

    def effects(self, kind):
        column = 'text' if kind == 'reply' else 'message'
        return [row[0] for row in self.store.db.execute(
            'SELECT ' + column + ' FROM envelope_effects WHERE kind=? ORDER BY id', (kind,))]

    async def card_posted(self, count=1):
        await until(lambda: len(self.envelopes()) >= count, 'a sign-in card')
        envelope = self.envelopes()[count - 1]
        text, markup = self.card(envelope)
        return envelope, text, markup

    async def arm(self, count=1):
        envelope, _, _ = await self.card_posted(count)
        return envelope

    def paste(self, code):
        self.assertEqual(self.send(code), 'envelope_intercept')

    async def settled(self):
        await until(lambda: self.store.get(KEY) is None, 'the sign-in to end')

    def assert_absent(self, value):
        for table in ('messages', 'attachments', 'outbox', 'updates', 'settings', 'envelopes', 'envelope_events',
                      'envelope_effects', 'tasks', 'workers', 'service_requests'):
            for row in self.store.db.execute('SELECT * FROM ' + table):
                self.assertNotIn(value, json.dumps(list(row)), table)
        self.assertNotIn(value, self.log.getvalue())
        for path in (self.root / 'state').iterdir():
            if path.is_file():
                self.assertNotIn(value.encode(), path.read_bytes(), path.name)

    async def test_add_account_posts_the_link_and_uses_login_email_for_display(self):
        self.start()
        self.assertTrue(self.account_control().ok)
        self.assertIn('Starting Claude sign-in', self.texts()[-1])
        envelope, text, markup = await self.card_posted()
        self.assertIn('\nhttps://claude.ai/oauth/authorize?code=true&state=', text)
        self.assertIn('\n3. Paste it here as one message', text)
        self.assertNotIn('\x1b', text)
        self.assertEqual(markup, {'inline_keyboard': [[{'text': 'Cancel',
                                                        'callback_data': 'envelope:%d:cancel' % envelope['id']}]]})
        self.paste(GOOD)
        self.assertEqual(self.effects('delete'), [self.number])
        self.assertIn('Code received. Checking it with Claude.', self.card(self.envelopes()[0])[0])
        await self.settled()
        folder = Path(self.calls()[0]['home'])
        self.assertEqual(folder.parent, self.accounts)
        self.assertTrue(folder.name.startswith('.torii-'))
        self.assertEqual(self.store.get('accounts')['Jane-Doe-work'], {'config_dir': str(folder), 'enabled': True})
        from coordinator.accounts import account_label
        self.assertEqual(account_label(self.store, 'Jane-Doe-work'), EMAIL)
        self.assertEqual(discover_accounts(self.store, self.accounts)['Jane-Doe-work']['config_dir'], str(folder))
        self.assertEqual(sorted(self.store.get('accounts')), ['Jane-Doe-work'])
        final, buttons = self.card(self.envelopes()[-1])
        self.assertIn('Claude account ' + EMAIL + ' is signed in and enabled', final)
        self.assertIsNone(buttons)
        self.assertIsNotNone(self.store.get('account_status_refresh_requested'))
        self.assertEqual([(call['argv'], call['token']) for call in self.calls()],
                         [(['auth', 'login', '--claudeai'], False)])
        self.assertEqual(oct(folder.stat().st_mode & 0o777), '0o700')
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM messages').fetchone()[0], 0)
        self.assertEqual(self.envelopes()[-1]['state'], 'filled')
        self.assertIsNone(self.envelopes()[-1]['fingerprint'])
        self.assertIsNone(self.driver.code)
        with self.store.db:
            listed = control_api.call(self.store, 'secret.list', {}, source='cli')
        self.assertEqual(listed.data, [])
        self.assert_absent(GOOD)

    async def test_a_taken_name_gets_a_number_and_no_email_gives_account(self):
        with self.store.db:
            self.store.put('accounts', {'default': {'config_dir': None, 'enabled': True},
                                        'Jane-Doe-work': {'config_dir': '/elsewhere', 'enabled': True}})
        self.start()
        self.account_control()
        await self.arm()
        self.paste(GOOD)
        await self.settled()
        self.assertIn('Jane-Doe-work-2', self.store.get('accounts'))
        self.assertEqual(self.store.get('accounts')['Jane-Doe-work']['config_dir'], '/elsewhere')
        self.assertEqual(account_name(None, ['default']), 'account')
        self.assertEqual(account_name('', ['account', 'account-2']), 'account-3')
        self.assertEqual(account_name('default@example.com', []), 'default-2')
        self.assertEqual(account_name('..@example.com', []), 'account')
        self.assertEqual(account_name('x' * 80 + '@example.com', []), 'x' * 56)

    async def test_signing_in_a_known_account_again_keeps_it_and_adds_nothing(self):
        directory = self.root / 'known'
        directory.mkdir()
        existing = {'default': {'config_dir': None, 'enabled': True},
                    'Jane': {'config_dir': str(directory), 'enabled': False}}
        with self.store.db:
            self.store.put('accounts', existing)
            self.store.put('account_status', {'Jane': {'identity': {'email': EMAIL.upper(), 'logged_in': True},
                                                       'observed_at': 1}})
        self.start()
        self.account_control()
        await self.arm()
        self.paste(GOOD)
        await self.settled()
        self.assertEqual(self.store.get('accounts'), existing)
        final, buttons = self.card(self.envelopes()[-1])
        self.assertIn('This Claude account is already added as ' + EMAIL.upper() + '. Torii kept ' + EMAIL.upper() +
                      ' and added no new account.', final)
        self.assertIn('The new profile folder holds files, so Torii kept it.', final)
        self.assertIsNone(buttons)
        self.assertIn(EMAIL.upper(), final)
        self.assertTrue(Path(self.calls()[0]['home']).is_dir())

    def register_profile(self, alias='Jane', email=EMAIL, logged_in=False):
        directory = self.accounts / alias
        directory.mkdir(parents=True)
        (directory / '.claude.json').write_text('{}')
        with self.store.db:
            profiles = self.store.get('accounts')
            profiles[alias] = {'config_dir': str(directory), 'enabled': False, 'awaiting_login': True}
            snapshots = self.store.get('account_status', {})
            snapshots[alias] = {'identity': {'email': email, 'logged_in': logged_in}, 'error': 'login_required'}
            if logged_in:
                snapshots[alias].pop('error')
            self.store.put('accounts', profiles)
            self.store.put('account_status', snapshots)
        return directory

    def add(self, alias=None, provider=None):
        with self.store.db:
            result = control_api.call(self.store, 'account.add', {'alias': alias, 'provider': provider}, topic=TOPIC)
        self.assertTrue(result.ok, result.text)
        return self.store.get(KEY)

    async def test_matching_claude_target_repoints_and_keeps_prior_folders(self):
        old = self.register_profile(email=EMAIL.upper())
        history = old / 'history.jsonl'
        history.write_text('saved native history\n')
        earlier = self.accounts / 'earlier'
        earlier.mkdir()
        profiles = self.store.get('accounts')
        profiles['Jane']['previous_config_dirs'] = [str(earlier), str(old), str(earlier)]
        with self.store.db:
            self.store.put('accounts', profiles)
            self.store.put('active_account', 'Jane')
        self.start()
        record = self.add('Jane', provider='codex')
        self.assertEqual(record['target'], 'Jane')
        self.assertNotEqual(record.get('provider'), 'codex')
        _, text, _ = await self.card_posted()
        self.assertIn('Sign in %s again' % EMAIL.upper(), text)
        self.assertIn('1. Tap this link and sign in to %s:' % EMAIL.upper(), text)
        self.paste(GOOD)
        await self.settled()
        new = Path(record['config_dir'])
        profile = self.store.get('accounts')['Jane']
        self.assertEqual(profile, {'config_dir': str(new), 'enabled': True,
                                    'previous_config_dirs': [str(earlier), str(old)]})
        self.assertEqual(self.store.get('active_account'), 'Jane')
        self.assertEqual(self.store.get('account_status')['Jane'],
                         {'identity': {'email': EMAIL, 'logged_in': True, 'type': 'Claude (plan unknown)'}})
        self.assertIsNotNone(self.store.get('account_status_refresh_requested'))
        self.assertFalse(self.driver.remove_folder(dict(record, created=True)))
        self.assertTrue(new.is_dir())
        self.assertEqual(history.read_text(), 'saved native history\n')
        self.assertEqual(self.card(self.envelopes()[-1])[0],
                         '%s is signed in again. Torii uses it by the same rules as before.' % EMAIL)
        self.assertEqual(sorted(discover_accounts(self.store, self.accounts)), ['Jane'])

    async def test_claude_target_without_email_takes_priority_over_known_email(self):
        old = self.register_profile(email=None)
        self.register_profile('other', logged_in=True)
        self.start()
        record = self.add('Jane')
        _, text, _ = await self.card_posted()
        self.assertIn('Sign in Jane again', text)
        self.paste(GOOD)
        await self.settled()
        self.assertEqual(self.store.get('accounts')['Jane']['config_dir'], record['config_dir'])
        self.assertEqual(self.store.get('accounts')['Jane']['previous_config_dirs'], [str(old)])
        self.assertEqual(self.store.get('accounts')['other']['config_dir'], str(self.accounts / 'other'))
        self.assertTrue(old.is_dir())

    def test_registered_empty_claude_folder_is_never_removed(self):
        old = self.register_profile()
        record = self.add('Jane')
        directory = Path(record['config_dir'])
        directory.mkdir()
        driver = SignIns(self.store)
        driver.succeed(dict(record, created=True), {'email': EMAIL})
        self.assertFalse(driver.remove_folder(dict(record, created=True)))
        self.assertTrue(directory.is_dir())
        self.assertTrue(old.is_dir())

    async def test_different_new_claude_email_adds_account_and_keeps_target(self):
        self.register_profile(email='carol@example.com')
        original = self.store.get('accounts')['Jane']
        snapshot = self.store.get('account_status')['Jane']
        self.start()
        record = self.add('Jane')
        await self.arm()
        self.paste(GOOD)
        await self.settled()
        self.assertEqual(self.store.get('accounts')['Jane'], original)
        self.assertEqual(self.store.get('account_status')['Jane'], snapshot)
        self.assertEqual(self.store.get('accounts')['Jane-Doe-work']['config_dir'], record['config_dir'])
        self.assertEqual(self.card(self.envelopes()[-1])[0],
                         'You signed in as %s, not carol@example.com. Torii added %s as a new account. '
                         'carol@example.com is still signed out.' % (EMAIL, EMAIL))

    def test_different_signed_in_claude_email_discards_empty_new_folder(self):
        self.register_profile(email='carol@example.com')
        self.register_profile('other', logged_in=True)
        original = self.store.get('accounts')
        record = self.add('Jane')
        directory = Path(record['config_dir'])
        directory.mkdir()
        self.driver = SignIns(self.store)
        self.driver.succeed(dict(record, created=True), {'email': EMAIL})
        self.assertEqual(self.store.get('accounts'), original)
        self.assertFalse(directory.exists())
        self.assertIsNone(self.store.get(KEY))
        self.assertEqual(self.texts()[-1],
                         'You signed in as %s, which Torii already has. Nothing changed. '
                         'carol@example.com is still signed out.' % EMAIL)

    async def test_add_matching_signed_out_claude_email_repoints_instead_of_duplicating(self):
        old = self.register_profile()
        self.start()
        record = self.add()
        self.assertIsNone(record['target'])
        await self.arm()
        self.paste(GOOD)
        await self.settled()
        self.assertEqual(self.store.get('accounts')['Jane']['config_dir'], record['config_dir'])
        self.assertEqual(self.store.get('accounts')['Jane']['previous_config_dirs'], [str(old)])
        self.assertNotIn('Jane-Doe-work', self.store.get('accounts'))
        self.assertTrue(old.is_dir())
        self.assertEqual(self.card(self.envelopes()[-1])[0],
                         '%s is signed in again. Torii uses it by the same rules as before.' % EMAIL)

    async def test_different_signed_out_claude_email_repoints_matching_alias(self):
        self.register_profile(email='carol@example.com')
        other = self.register_profile('other')
        target = self.store.get('accounts')['Jane']
        self.start()
        record = self.add('Jane')
        await self.arm()
        self.paste(GOOD)
        await self.settled()
        self.assertEqual(self.store.get('accounts')['Jane'], target)
        self.assertEqual(self.store.get('accounts')['other']['config_dir'], record['config_dir'])
        self.assertEqual(self.store.get('accounts')['other']['previous_config_dirs'], [str(other)])
        self.assertNotIn('Jane-Doe-work', self.store.get('accounts'))

    async def test_failed_claude_signin_preserves_provider_and_target_for_retry(self):
        self.register_profile()
        self.start(attempts=1)
        first = self.add('Jane')
        await self.arm()
        self.paste(WRONG)
        await self.settled()
        retry = self.store.get(RETRY_KEY)
        self.assertEqual(retry, {'provider': 'claude', 'target': 'Jane'})
        self.assertEqual(self.card(self.envelopes()[-1])[1]['inline_keyboard'][0][0]['text'], 'Try again')
        second = self.add(retry['target'], retry['provider'])
        self.assertEqual(second['target'], 'Jane')
        self.assertNotEqual(second['config_dir'], first['config_dir'])
        self.assertIsNone(self.store.get(RETRY_KEY))
        await self.arm(2)
        self.paste(GOOD)
        await self.settled()
        self.assertEqual(self.store.get('accounts')['Jane']['config_dir'], second['config_dir'])

    async def test_wrong_code_posts_a_new_card_and_the_next_code_succeeds(self):
        self.start()
        self.account_control()
        await self.arm()
        self.paste(WRONG)
        second = await self.arm(2)
        first_text, _ = self.card(self.envelopes()[0])
        self.assertIn('rejected that code', first_text)
        self.assertIn('It may be wrong or expired. Try again with the new sign-in card below.', first_text)
        self.assertIn('(new link)', self.card(second)[0])
        self.paste(GOOD)
        await self.settled()
        self.assertTrue(self.store.get('accounts')['Jane-Doe-work']['enabled'])
        self.assertEqual(len(self.calls()), 2)
        self.assertEqual(len({call['home'] for call in self.calls()}), 1)
        self.assert_absent(WRONG)
        self.assert_absent(GOOD)

    async def test_too_many_wrong_codes_add_nothing_and_offer_a_retry(self):
        self.start(attempts=2)
        self.account_control()
        await self.arm()
        self.paste(WRONG)
        await self.arm(2)
        self.paste(WRONG)
        await self.settled()
        text, markup = self.card(self.envelopes()[-1])
        self.assertIn('did not accept 2 codes', text)
        self.assertEqual(markup, {'inline_keyboard': [[{'text': 'Try again', 'callback_data': RETRY_DATA}]]})
        self.assertEqual(sorted(self.store.get('accounts')), ['default'])
        self.assertEqual(list(self.accounts.iterdir()), [])

    async def test_expired_link_stops_the_process_and_try_again_starts_over(self):
        self.start(code_seconds=0.3)
        self.account_control()
        await self.card_posted()
        await self.settled()
        text, markup = self.card(self.envelopes()[0])
        self.assertIn('expired', text)
        self.assertEqual(markup['inline_keyboard'][0][0]['text'], 'Try again')
        self.assertEqual(self.envelopes()[0]['state'], 'expired')
        self.assertEqual(sorted(self.store.get('accounts')), ['default'])
        self.assertEqual(list(self.accounts.iterdir()), [])
        with self.assertRaises(ProcessLookupError):
            os.kill(self.calls()[0]['pid'], 0)
        self.assertEqual(self.send_private(GOOD), 'envelope_reject')
        self.assertTrue(self.account_control().ok)
        await self.card_posted(2)
        self.assertEqual(len(self.calls()), 2)

    async def test_card_cancel_stops_the_sign_in_and_removes_only_an_empty_folder(self):
        self.start()
        self.account_control()
        envelope, _, _ = await self.card_posted()
        self.assertEqual(self.click('envelope:%d:cancel' % envelope['id']), 'envelope_cancel')
        await self.settled()
        self.assertIn('cancelled', self.card(self.envelopes()[0])[0])
        self.assertIsNone(self.card(self.envelopes()[0])[1])
        self.assertEqual(list(self.accounts.iterdir()), [])
        await until(lambda: self.pid_gone(self.calls()[0]['pid']), 'the sign-in process to stop')
        with patch.dict(os.environ, {'FAKE_WRITES': '1'}):
            self.account_control()
            await self.card_posted(2)
            self.assertIn('Stopping the Claude sign-in', self.account_control('account.signin_cancel') and self.texts()[-1])
            await self.settled()
        kept = list(self.accounts.iterdir())
        self.assertEqual([path.name for path in kept[0].iterdir()], ['.claude.json'])
        self.assertEqual(self.envelopes()[1]['state'], 'cancelled')
        self.assertNotIn(kept[0].name, discover_accounts(self.store, self.accounts))

    def pid_gone(self, pid):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        return False

    async def test_card_lists_the_three_steps_and_has_no_private_chat_button(self):
        self.start()
        self.account_control()
        envelope, text, markup = await self.card_posted()
        self.assertRegex(text, r'\A\S.*\n1\. Tap this link and sign in to the Claude account to add:\nhttps://\S+\n'
                               r'2\. Copy the code it shows\.\n3\. Paste it here as one message within 10 minutes\. '
                               r'I delete it at once\.\Z')
        cancel = {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': 'envelope:%d:cancel' % envelope['id']}]]}
        self.assertEqual(markup, cancel)
        with self.store.db:
            self.store.put('control_topic', TOPIC)
        self.assertEqual(self.send('/setup'), 'control')
        self.assertEqual(self.card(self.envelopes()[0]), (text, cancel))
        self.assertNotIn('t.me/', json.dumps([row[0] for row in self.store.db.execute('SELECT reply_markup FROM outbox')]))

    async def test_code_pasted_in_the_topic_is_deleted_at_once_and_used_even_if_the_delete_fails(self):
        from coordinator import envelopes
        from coordinator.telegram import TelegramError
        self.start()
        self.account_control()
        await self.card_posted()
        self.assertEqual(self.store.accept(update(self.next_number(), GOOD, user=OWNER + 1)), 'unauthorized')
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('-10042:9',-10042,9,'Other','',1)")
        self.assertEqual(self.store.accept(update(self.next_number(), 'hello there', thread=9)), 'queued')
        self.assertIsNone(self.driver.code)
        self.paste(GOOD)
        self.assertEqual(self.effects('delete'), [self.number])

        class Telegram:
            async def call(self, method, **parameters):
                raise TelegramError(400)
        await envelopes.run_effects(self.store, Telegram())
        await self.settled()
        self.assertTrue(self.store.get('accounts')['Jane-Doe-work']['enabled'])
        self.assertIn('I could not delete the message with the sign-in code. Delete it yourself.', self.texts())
        self.assertIn('account signin topic message kind=code', self.log.getvalue())
        self.assertEqual([row[0] for row in self.store.db.execute('SELECT text FROM messages')], ['hello there'])
        self.assertEqual(self.envelopes()[0]['length'], len(GOOD))
        self.assert_absent(GOOD)

    async def test_other_topic_text_during_sign_in_routes_to_coordinator(self):
        self.start()
        self.account_control()
        await self.card_posted()
        chat = ['please check the usage page', 'sure thing', 'abcdefgh#12345678 is the issue',
                'https://claude.ai/oauth/authorize?code=true', 'a' * 60, '0123456789abcdef' * 4]
        for state in ('code', 'checking'):
            with self.store.db:
                self.store.put(KEY, dict(self.store.get(KEY), state=state))
            for text in chat:
                self.assertEqual(self.send(text), 'queued')
        self.assertEqual(self.effects('delete'), [])
        self.assertEqual([row[0] for row in self.store.db.execute('SELECT text FROM messages')], chat * 2)
        self.assertEqual({row[0] for row in self.store.db.execute('SELECT topic FROM messages')}, {TOPIC})
        self.assertEqual(len(self.texts()), 2)
        self.assertIsNone(self.driver.code)
        for text in chat:
            self.assertNotIn(text, self.log.getvalue())
        self.assertTrue(all(looks_like_code(code) for code in (GOOD, WRONG, 'Ab1' + 'x' * 60)))

    async def assert_malformed_code_deleted(self, text, photo=False):
        self.start()
        self.account_control()
        envelope, card_text, markup = await self.card_posted()
        record = self.store.get(KEY)
        incoming = update(self.next_number(), text)
        if photo:
            incoming['message'].pop('text')
            incoming['message']['caption'] = text
            incoming['message']['photo'] = [{'file_id': 'f', 'file_unique_id': 'u', 'width': 1, 'height': 1}]
        self.assertEqual(self.store.accept(incoming), 'envelope_intercept')
        self.assertEqual(self.effects('delete'), [self.number])
        self.assertEqual(self.texts()[-1],
                         'I deleted that message. Paste only the code, as one message, with nothing around it.')
        self.assertEqual(self.store.db.execute('SELECT topic FROM outbox ORDER BY id DESC').fetchone()[0], TOPIC)
        self.assertIsNone(self.driver.code)
        self.assertEqual(self.store.get(KEY), record)
        self.assertEqual(self.envelopes()[0]['state'], 'open')
        self.assertEqual(self.card(envelope), (card_text, markup))
        for table in ('messages', 'attachments', 'tasks'):
            self.assertEqual(self.store.db.execute('SELECT count(*) FROM ' + table).fetchone()[0], 0, table)
        self.assert_absent(text)
        self.assert_absent(GOOD)

    async def test_code_with_label_is_deleted_without_handoff_or_attempt(self):
        await self.assert_malformed_code_deleted('code: ' + GOOD)

    async def test_code_with_surrounding_words_is_deleted_without_handoff_or_attempt(self):
        await self.assert_malformed_code_deleted('here ' + GOOD + ' thanks')

    async def test_code_in_backticks_is_deleted_without_handoff_or_attempt(self):
        await self.assert_malformed_code_deleted('`' + GOOD + '`')

    async def test_code_with_internal_space_is_deleted_without_handoff_or_attempt(self):
        await self.assert_malformed_code_deleted(GOOD[:10] + ' ' + GOOD[10:])

    async def test_code_with_internal_newline_is_deleted_without_handoff_or_attempt(self):
        await self.assert_malformed_code_deleted(GOOD[:10] + '\n' + GOOD[10:])

    async def test_split_code_half_with_state_is_deleted_without_handoff_or_attempt(self):
        await self.assert_malformed_code_deleted(GOOD[len(GOOD) // 2:])

    async def test_caption_code_with_words_is_deleted_without_attachment_or_handoff(self):
        await self.assert_malformed_code_deleted('here ' + GOOD + ' thanks', photo=True)

    async def test_code_word_with_quotes_and_punctuation_is_deleted_without_live_state(self):
        value = 'Ab1' + 'x' * 60
        with patch.dict(os.environ, {'FAKE_LINK': 'https://claude.ai/oauth/authorize?code=true'}):
            await self.assert_malformed_code_deleted('here "`' + value + '`", thanks')
        self.assert_absent(value)

    async def test_non_code_reply_to_signin_card_keeps_usual_topic_handling(self):
        self.start()
        self.account_control()
        envelope, _, _ = await self.card_posted()
        self.store.delivered(envelope['card_outbox'], 555)
        self.assertEqual(self.store.accept(update(self.next_number(), 'please continue',
                                                 reply_to_message={'message_id': 555})), 'control')
        self.assertEqual(self.effects('delete'), [])
        self.assertEqual(self.texts()[-1], 'This form has expired. Open the command again.')

    async def test_code_reply_to_signin_card_in_project_topic_signs_in(self):
        self.start()
        self.account_control()
        envelope, _, _ = await self.card_posted()
        self.store.delivered(envelope['card_outbox'], 555)
        self.assertEqual(self.store.accept(update(self.next_number(), GOOD,
                                                 reply_to_message={'message_id': 555})), 'envelope_intercept')
        self.assertEqual(self.effects('delete'), [self.number])
        await self.settled()
        self.assertTrue(self.store.get('accounts')['Jane-Doe-work']['enabled'])
        self.assert_absent(GOOD)

    async def test_forwarded_codes_are_deleted_without_handoff(self):
        self.start()
        self.account_control()
        await self.card_posted()
        for state in ('code', 'checking'):
            with self.store.db:
                self.store.put(KEY, dict(self.store.get(KEY), state=state))
            for key in ('forward_origin', 'forward_from', 'forward_from_chat', 'forward_sender_name', 'forward_date'):
                self.assertEqual(self.store.accept(update(self.next_number(), GOOD, **{key: 'forwarded'})),
                                 'envelope_intercept')
                self.assertEqual(self.effects('delete')[-1], self.number)
                self.assertEqual(self.texts()[-1],
                                 'I deleted that forwarded message. Paste the code as your own message.')
        self.assertIsNone(self.driver.code)
        self.assertEqual(self.store.get(KEY)['attempt'], 1)
        self.assert_absent(GOOD)

    async def test_secret_prefixes_are_deleted_without_handoff_or_attempt(self):
        self.start()
        self.account_control()
        await self.card_posted()
        values = [prefix + suffix for prefix in
                  ('sk-ant-', 'sk-', 'ghp_', 'gho_', 'github_pat_', 'xoxb-', 'xoxp-', 'AKIA')
                  for suffix in ('AbCdEfGh12345678#state-0123', 'credential')]
        for state in ('code', 'checking'):
            with self.store.db:
                self.store.put(KEY, dict(self.store.get(KEY), state=state))
            for value in values:
                self.paste(value)
                self.assertEqual(self.effects('delete')[-1], self.number)
                self.assertEqual(self.texts()[-1], "I deleted that message. It isn't the code for this sign-in. "
                                 'Paste the code the current link shows.')
                self.assertIsNone(self.driver.code)
                self.assertEqual(self.store.get(KEY)['attempt'], 1)
                self.assertEqual(self.envelopes()[0]['state'], 'open')
                self.assert_absent(value)

    async def test_wrong_oauth_state_is_deleted_without_attempt_and_right_code_signs_in(self):
        self.start()
        self.account_control()
        await self.card_posted()
        value = 'OTHER-CODE-abcdefgh#state-9999'
        for state in ('code', 'checking'):
            with self.store.db:
                self.store.put(KEY, dict(self.store.get(KEY), state=state))
            self.paste(value)
            self.assertEqual(self.effects('delete')[-1], self.number)
            self.assertEqual(self.texts()[-1], "I deleted that message. It isn't the code for this sign-in. "
                             'Paste the code the current link shows.')
            self.assertIsNone(self.driver.code)
            self.assertEqual(self.store.get(KEY)['attempt'], 1)
            self.assertEqual(self.envelopes()[0]['state'], 'open')
            self.assert_absent(value)
        with self.store.db:
            self.store.put(KEY, dict(self.store.get(KEY), state='code'))
        self.paste(GOOD)
        await self.settled()
        self.assertTrue(self.store.get('accounts')['Jane-Doe-work']['enabled'])
        self.assert_absent(GOOD)

    async def test_caption_code_is_deleted_without_handoff(self):
        self.start()
        self.account_control()
        await self.card_posted()
        for state in ('code', 'checking'):
            with self.store.db:
                self.store.put(KEY, dict(self.store.get(KEY), state=state))
            incoming = update(self.next_number(), '')
            incoming['message'].pop('text')
            incoming['message']['caption'] = GOOD
            incoming['message']['photo'] = [{'file_id': 'f', 'file_unique_id': 'u', 'width': 1, 'height': 1}]
            self.assertEqual(self.store.accept(incoming), 'envelope_intercept')
            self.assertEqual(self.effects('delete')[-1], self.number)
            self.assertEqual(self.texts()[-1], "I deleted that message. It isn't the code for this sign-in. "
                             'Paste the code the current link shows.')
        self.assertIsNone(self.driver.code)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM attachments').fetchone()[0], 0)
        self.assert_absent(GOOD)

    async def test_url_decoded_oauth_state_accepts_code(self):
        with patch.dict(os.environ, {'FAKE_LINK': 'https://claude.ai/oauth/authorize?state=state%2D0123'}):
            self.start()
            self.account_control()
            await self.card_posted()
            self.paste(GOOD)
            await self.settled()
        self.assertTrue(self.store.get('accounts')['Jane-Doe-work']['enabled'])
        self.assert_absent(GOOD)

    async def test_url_without_oauth_state_accepts_code_shape(self):
        value = 'Ab1' + 'x' * 60
        with patch.dict(os.environ, {'FAKE_LINK': 'https://claude.ai/oauth/authorize?code=true', 'FAKE_GOOD': value}):
            self.start()
            self.account_control()
            await self.card_posted()
            self.paste(value)
            await self.settled()
        self.assertTrue(self.store.get('accounts')['Jane-Doe-work']['enabled'])
        self.assert_absent(value)

    async def test_oauth_state_requires_the_code_state_suffix(self):
        self.start()
        self.account_control()
        await self.card_posted()
        value = 'Ab1' + 'x' * 60
        self.paste(value)
        self.assertEqual(self.effects('delete'), [self.number])
        self.assertIsNone(self.driver.code)
        self.assertEqual(self.store.get(KEY)['attempt'], 1)
        self.assert_absent(value)

    async def test_pointer_only_when_topic_has_no_enabled_agent(self):
        from coordinator.signin import CODE_POINTER, CHECKING_POINTER
        self.start()
        self.account_control()
        await self.card_posted()
        with self.store.db:
            self.store.put('control_topic', TOPIC)
            self.store.db.execute('UPDATE topics SET cwd=? WHERE id=?', (str(self.root), TOPIC))
        self.assertEqual(self.send('hello home coordinator'), 'queued')
        with self.store.db:
            self.store.db.execute('UPDATE topics SET enabled=0 WHERE id=?', (TOPIC,))
        for state, pointer in (('code', CODE_POINTER), ('checking', CHECKING_POINTER)):
            with self.store.db:
                self.store.put(KEY, dict(self.store.get(KEY), state=state))
            self.assertEqual(self.send('hello'), 'envelope_intercept')
            self.assertEqual(self.texts()[-1], pointer)
        self.assertEqual(self.effects('delete'), [])

    async def test_delete_failure_after_setup_reports_in_paste_topic(self):
        from coordinator import envelopes
        from coordinator.telegram import TelegramError
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('-10042:9',-10042,9,'Torii','',1)")
            self.store.put('control_topic', '-10042:9')
        self.start()
        self.account_control()
        await self.card_posted()
        self.assertEqual(self.store.accept(update(self.next_number(), '/setup', thread=9)), 'control')
        self.assertEqual(self.store.accept(update(self.next_number(), GOOD, thread=9)), 'envelope_intercept')

        class Telegram:
            async def call(self, method, **parameters):
                raise TelegramError(400)
        await envelopes.run_effects(self.store, Telegram())
        row = self.store.db.execute("SELECT topic,text FROM outbox WHERE text LIKE 'I could not delete the message%'").fetchone()
        self.assertEqual(tuple(row), ('-10042:9', 'I could not delete the message with the sign-in code. Delete it yourself.'))
        await self.settled()
        self.assert_absent(GOOD)

    def test_account_add_description_matches_topic_handoff(self):
        description = next(op.description for op in control_api.OPS if op.id == 'account.add')
        self.assertIn('The card is posted in the topic and the owner pastes the code there as one message.', description)
        self.assertNotIn('private Envelope', description)
        self.assertIn('device-code sign-in with provider codex', description)

    async def test_late_code_in_the_topic_is_deleted_and_not_used(self):
        self.start()
        self.account_control()
        await self.card_posted()
        self.driver.waiting = None
        self.paste(GOOD)
        self.assertEqual(self.effects('delete'), [self.number])
        self.assertIn('This sign-in is not waiting for a code', self.texts()[-1])
        self.assertIsNone(self.driver.code)
        self.assertEqual(self.envelopes()[0]['state'], 'open')
        self.assert_absent(GOOD)

    async def test_expired_sign_in_card_offers_try_again(self):
        self.start(code_seconds=60)
        self.account_control()
        envelope, _, _ = await self.card_posted()
        with self.store.db:
            sweep(self.store, envelope['expires'] + 1)
        text, markup = self.card(self.envelopes()[0])
        self.assertEqual(text, 'The sign-in link expired. No account was added.')
        self.assertEqual(markup, {'inline_keyboard': [[{'text': 'Try again', 'callback_data': RETRY_DATA}]]})
        await self.settled()
        text, markup = self.card(self.envelopes()[0])
        self.assertIn('expired', text)
        self.assertEqual(markup, {'inline_keyboard': [[{'text': 'Try again', 'callback_data': RETRY_DATA}]]})

    async def test_private_chat_replies_point_back_to_the_card(self):
        self.start()
        self.account_control()
        await self.card_posted()
        self.assertEqual(self.send_private(GOOD), 'envelope_reject')
        self.assertEqual(self.effects('delete'), [self.number + 100])
        self.send_private('/start')
        self.send_private('/cancel')
        self.assertEqual(self.effects('reply'), [
            'I deleted your message. Paste the Claude sign-in code in your Torii group, in the topic with the '
            'sign-in card, not here.',
            'To finish the Claude sign-in, paste the code in your Torii group, in the topic with the sign-in card.',
            'Nothing is waiting here. To stop the Claude sign-in, tap Cancel on its card in your Torii group.'])
        self.assertIsNone(self.driver.code)
        self.assertEqual(self.store.get(KEY)['state'], 'code')
        self.assert_absent(GOOD)

    async def test_one_sign_in_at_a_time(self):
        self.start()
        self.assertTrue(self.account_control().ok)
        await self.card_posted()
        pending = self.store.get(KEY)
        calls = self.calls()
        self.assertEqual(len(calls), 1)
        self.assertFalse(self.account_control().ok)
        self.assertIn('still open', self.texts()[-1])
        self.assertEqual(self.store.get(KEY), pending)
        self.assertEqual(self.calls(), calls)
        self.assertEqual(len(self.envelopes()), 1)
        self.assertTrue(self.account_control('account.signin_cancel').ok)
        await self.settled()
        with self.store.db:
            self.store.put('bot_username', None)
        self.assertFalse(self.account_control().ok)
        self.assertIn('bot identity', self.texts()[-1])
        self.assertIsNone(self.store.get(KEY))

    async def test_restart_while_waiting_stops_the_leftover_and_reports(self):
        folder = self.accounts / '.torii-half'
        folder.mkdir(parents=True)
        process = subprocess.Popen([str(self.fake), 'auth', 'login', '--claudeai'], stdin=subprocess.PIPE,
                                   stdout=subprocess.DEVNULL, env=dict(os.environ, CLAUDE_CONFIG_DIR=str(folder)),
                                   start_new_session=True)
        self.addCleanup(process.wait)
        self.addCleanup(process.stdin.close)
        await until(lambda: self.calls(), 'the leftover process to start')
        with self.store.db:
            self.store.put(KEY, {'topic': TOPIC, 'config_dir': str(folder), 'state': 'code',
                                 'attempt': 1, 'created': True, 'pid': process.pid, 'envelope': None})
        self.start()
        await self.settled()
        await until(lambda: process.poll() is not None, 'the leftover process to stop')
        self.assertIn('stopped because Torii restarted', self.texts()[-1])
        self.assertFalse(folder.exists())
        self.assertEqual(sorted(self.store.get('accounts')), ['default'])

    def test_link_parser_needs_a_complete_claude_url(self):
        self.assertIsNone(sign_in_link('visit https://claude.ai/oauth/authorize?state=1'))
        self.assertIsNone(sign_in_link('visit https://evil.example/claude.ai \n'))
        self.assertEqual(sign_in_link('\x1b[2mhttps://claude.ai/a?b=1\x1b[0m\n'), 'https://claude.ai/a?b=1')


class TargetCodexSignInTests(codex_tests.CodexFixture):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.driver = SignIns(self.store, 'claude', codex_binary=str(self.binary), poll=0.01)
        self.task = asyncio.ensure_future(self.driver.run())

    async def asyncTearDown(self):
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)
        await super().asyncTearDown()

    def texts(self):
        return [row[0] for row in self.store.db.execute('SELECT text FROM outbox ORDER BY id')]

    def register_profile(self, alias='codex', email='owner@example.com', logged_in=False):
        directory = self.default if alias == 'codex' else self.home(alias)
        with self.store.db:
            profiles = self.store.get('codex_accounts', {})
            profiles[alias] = {'config_dir': str(directory), 'enabled': logged_in, 'awaiting_login': True}
            snapshots = self.store.get('codex_account_status', {})
            snapshots[alias] = {'identity': {'email': email, 'logged_in': logged_in}, 'error': 'login_required',
                                'usage': {'seven_day': {'utilization': 98}}}
            if logged_in:
                snapshots[alias].pop('error')
            self.store.put('codex_accounts', profiles)
            self.store.put('codex_account_status', snapshots)
        return directory

    async def add(self, alias=None):
        with self.store.db:
            result = control_api.call(self.store, 'account.add',
                                      {'alias': alias, 'provider': 'claude' if alias else 'codex'}, topic=TOPIC)
        self.assertTrue(result.ok, result.text)
        record = self.store.get(KEY)
        self.assertEqual(record['provider'], 'codex')
        await until(lambda: any(codex_tests.DEVICE_CODE in text for text in self.texts()), 'the device-code card')
        return record

    async def approve(self, record, email='owner@example.com'):
        self.behave({Path(record['config_dir']).name: {'email': email}})
        (self.bin / 'approve').write_text('1')
        await until(lambda: self.store.get(KEY) is None, 'the sign-in to end')

    async def assert_default_repoint(self, picked):
        old = self.register_profile()
        history = old / 'sessions' / 'native-session.jsonl'
        history.write_text('original native history\n')
        earlier = self.home('earlier')
        profiles = self.store.get('codex_accounts')
        profiles['codex']['previous_config_dirs'] = [str(earlier), str(old), str(earlier)]
        with self.store.db:
            self.store.put('codex_accounts', profiles)
            self.store.put('codex_active_account', 'codex' if picked else None)
        self.register_profile('codex-other', email='other@example.com', logged_in=True)
        record = await self.add('codex')
        self.assertIn('Sign in owner@example.com again', self.texts()[-1])
        self.assertIn('1. Open this link and sign in to the ChatGPT account for owner@example.com:',
                      self.texts()[-1])
        await self.approve(record, 'OWNER@example.com')
        profile = self.store.get('codex_accounts')['codex']
        self.assertEqual(profile, {'config_dir': record['config_dir'], 'enabled': True,
                                    'previous_config_dirs': [str(earlier), str(old)]})
        self.assertEqual(self.store.get('codex_active_account'), 'codex' if picked else None)
        self.assertEqual(self.store.get('codex_account_status')['codex'],
                         {'identity': {'email': 'OWNER@example.com', 'logged_in': True, 'type': 'ChatGPT Pro'}})
        self.assertIsNotNone(self.store.get('codex_account_status_refresh_requested'))
        broker = codex_tests.CodexBroker(self.store)
        self.assertEqual(broker.active(), 'codex')
        self.assertEqual(broker.select(), 'codex')
        self.assertEqual(history.read_text(), 'original native history\n')
        self.assertTrue(Path(record['config_dir']).is_dir())
        self.assertFalse(self.driver.remove_folder(dict(record, created=True)))
        self.assertEqual(self.texts()[-1],
                         'OWNER@example.com is signed in again. Torii uses it by the same rules as before.')

    async def test_default_codex_target_repoints_without_a_pick_and_keeps_active_alias(self):
        await self.assert_default_repoint(False)

    async def test_default_codex_target_repoints_with_a_pick_and_keeps_active_alias(self):
        await self.assert_default_repoint(True)

    async def test_codex_target_without_email_takes_priority_over_known_email(self):
        old = self.register_profile(email=None)
        self.register_profile('codex-other', logged_in=True)
        record = await self.add('codex')
        self.assertIn('Sign in codex again', self.texts()[-1])
        await self.approve(record)
        self.assertEqual(self.store.get('codex_accounts')['codex']['config_dir'], record['config_dir'])
        self.assertEqual(self.store.get('codex_accounts')['codex']['previous_config_dirs'], [str(old)])
        self.assertEqual(self.store.get('codex_accounts')['codex-other']['config_dir'],
                         str(self.added / 'codex-other'))

    async def test_different_new_codex_email_adds_account_and_keeps_target(self):
        self.register_profile(email='carol@example.com')
        original = self.store.get('codex_accounts')['codex']
        snapshot = self.store.get('codex_account_status')['codex']
        record = await self.add('codex')
        await self.approve(record)
        self.assertEqual(self.store.get('codex_accounts')['codex'], original)
        self.assertEqual(self.store.get('codex_account_status')['codex'], snapshot)
        self.assertEqual(self.store.get('codex_accounts')['codex-owner']['config_dir'], record['config_dir'])
        self.assertEqual(self.texts()[-1],
                         'You signed in as owner@example.com, not carol@example.com. Torii added owner@example.com '
                         'as a new account. carol@example.com is still signed out.')

    async def test_different_signed_in_codex_email_discards_new_folder(self):
        self.register_profile(email='carol@example.com')
        self.register_profile('codex-other', logged_in=True)
        with self.store.db:
            self.store.put('codex_active_account', 'codex-other')
        original = self.store.get('codex_accounts')
        record = await self.add('codex')
        await self.approve(record)
        self.assertEqual(self.store.get('codex_accounts'), original)
        self.assertEqual(self.store.get('codex_active_account'), 'codex-other')
        self.assertFalse(Path(record['config_dir']).exists())
        self.assertEqual(self.texts()[-1],
                         'You signed in as owner@example.com, which Torii already has. Nothing changed. '
                         'carol@example.com is still signed out.')

    async def test_add_matching_signed_out_codex_email_repoints_instead_of_duplicating(self):
        old = self.register_profile()
        with self.store.db:
            self.store.put('codex_active_account', 'codex')
        record = await self.add()
        self.assertIsNone(record['target'])
        await self.approve(record)
        self.assertEqual(self.store.get('codex_accounts')['codex']['config_dir'], record['config_dir'])
        self.assertEqual(self.store.get('codex_accounts')['codex']['previous_config_dirs'], [str(old)])
        self.assertEqual(self.store.get('codex_active_account'), 'codex')
        self.assertEqual(list(self.store.get('codex_accounts')), ['codex'])
        self.assertTrue(old.is_dir())
        self.assertEqual(self.texts()[-1],
                         'owner@example.com is signed in again. Torii uses it by the same rules as before.')

    async def test_different_signed_out_codex_email_repoints_matching_alias(self):
        self.register_profile(email='carol@example.com')
        old = self.register_profile('codex-other')
        target = self.store.get('codex_accounts')['codex']
        record = await self.add('codex')
        await self.approve(record)
        self.assertEqual(self.store.get('codex_accounts')['codex'], target)
        self.assertEqual(self.store.get('codex_accounts')['codex-other']['config_dir'], record['config_dir'])
        self.assertEqual(self.store.get('codex_accounts')['codex-other']['previous_config_dirs'], [str(old)])
        self.assertEqual(sorted(self.store.get('codex_accounts')), ['codex', 'codex-other'])

    async def test_failed_codex_signin_preserves_provider_and_target_for_retry(self):
        self.register_profile()
        self.driver.device_seconds = 0.2
        await self.add('codex')
        await until(lambda: self.store.get(KEY) is None, 'the device-code sign-in to expire')
        self.assertEqual(self.store.get(RETRY_KEY), {'provider': 'codex', 'target': 'codex'})
        self.assertIn('expired', self.texts()[-1])
