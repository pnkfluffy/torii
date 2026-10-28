import asyncio
import base64
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch

from coordinator import control_api, envelopes, mcp
from coordinator.account_status import collect_status
from coordinator.service import Service
from coordinator.store import Store
from coordinator.telegram import TelegramError
from coordinator.vault import FileVault, vault_from_environment
from tests.support import FakeVault, isolate_shared_mcp_home, stop_test_hosts


TOPIC = '-10042:4'
OWNER = 7
GROUP = -10042
BOT = 'torii_test_bot'
VALUE = 'FAKE-ENVELOPE-VALUE-0123456789'


class FakeTelegram:
    def __init__(self):
        self.calls = []
        self.failures = {}
        self.date = 1800000000

    async def call(self, method, **params):
        self.calls.append((method, params))
        failure = self.failures.get(method)
        if failure:
            error = failure.pop(0)
            if not failure:
                del self.failures[method]
            raise error
        if method == 'getMe':
            return {'id': 1, 'username': BOT}
        if method == 'sendMessage':
            return {'message_id': 5000 + len(self.calls), 'date': self.date}
        return True

    async def send(self, row):
        self.calls.append(('send', dict(row)))
        return {'message_id': row.get('edit_message') or 5000 + row['id'], 'date': self.date}


class EnvelopeFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)
        self.addCleanup(stop_test_hosts, self.root)
        self.store = Store(self.root / 'state')
        self.vault = FakeVault()
        self.store.vault = self.vault
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', (TOPIC, GROUP, 4, 'Torii', str(self.root)))
            self.store.put('owner', OWNER)
            self.store.put('group', GROUP)
            self.store.put('bot_username', BOT)

    def tearDown(self):
        self.store.close()

    def call(self, op, **params):
        with self.store.db:
            return control_api.call(self.store, op, params, topic=TOPIC, source='mcp')

    def ask(self, name='GITHUB_TOKEN', **extra):
        params = dict(name=name, reason='Push the release branch', consumer='release worker', **extra)
        return self.call('secret.ask', **params)

    def envelope(self, name='GITHUB_TOKEN'):
        return dict(self.store.db.execute('SELECT * FROM envelopes WHERE name=? ORDER BY id DESC LIMIT 1',
                                          (name,)).fetchone())

    def outbox(self, row_id):
        row = dict(self.store.db.execute('SELECT * FROM outbox WHERE id=?', (row_id,)).fetchone())
        row['markup'] = json.loads(row['reply_markup']) if row['reply_markup'] else None
        return row

    def count(self, table):
        return self.store.db.execute('SELECT count(*) FROM ' + table).fetchone()[0]

    def events(self):
        return [tuple(row) for row in self.store.db.execute('SELECT event,reason,source FROM envelope_events ORDER BY id')]

    def click_cancel(self, number, envelope, user=OWNER):
        return self.store.accept({'update_id': number, 'callback_query': {
            'id': str(number), 'from': {'id': user}, 'data': 'envelope:%d:cancel' % envelope['id'],
            'message': {'message_id': 900, 'message_thread_id': 4, 'chat': {'id': GROUP, 'type': 'supergroup'}}}})


class AskTests(EnvelopeFixture):

    def test_ask_posts_one_armed_card_without_a_token(self):
        result = self.ask()
        self.assertTrue(result.ok, result.text)
        envelope = self.envelope()
        self.assertEqual(result.data, {'envelope': envelope['id'], 'name': 'GITHUB_TOKEN'})
        self.assertEqual(self.count('outbox'), 1)
        card = self.outbox(envelope['card_outbox'])
        self.assertEqual(card['markup']['inline_keyboard'], [
            [{'text': 'Fill privately', 'url': 'https://t.me/' + BOT}],
            [{'text': 'Cancel', 'callback_data': 'envelope:%d:cancel' % envelope['id']}]])
        self.assertIsNone(envelope['token_hash'])
        self.assertEqual((envelope['state'], envelope['chat']), ('armed', OWNER))
        self.assertAlmostEqual(envelope['expires'] - envelope['armed_at'], 600)
        self.assertEqual(self.events(), [('ask', None, 'mcp'), ('arm', None, 'direct_prompt')])
        self.assertEqual(card['text'], 'Envelope: GITHUB_TOKEN\nWhy: Push the release branch\n'
                         'Used by: release worker\nJob: any job\n'
                         'Waiting for the value in the private chat. Tap Fill privately to open it.\n'
                         'Never paste it in this channel.')
        self.assertEqual(self.vault.calls, [])

    def test_ask_names_its_task(self):
        task = self.store.task_create(TOPIC, 'Release')
        self.ask(task=task['id'])
        self.assertIn('Job: 1 Release', self.outbox(self.envelope()['card_outbox'])['text'])

    def test_second_live_ask_for_a_name_is_refused_without_changes(self):
        self.ask()
        before = [self.count(table) for table in ('envelopes', 'envelope_events', 'outbox')]
        result = self.ask()
        self.assertEqual(result.state, 'refused')
        self.assertIn('already open', result.text)
        self.assertEqual([self.count(table) for table in ('envelopes', 'envelope_events', 'outbox')], before)

    def test_invalid_and_reserved_names_are_refused_before_the_handler(self):
        for name in ('PATH', 'ANTHROPIC_API_KEY', 'lower', 'A', 'GIT_DIR', 'NODE_OPTIONS', 'SSL_CERT_DIR',
                     'TORII_VAULT_KEY_FILE', 'CLAUDE_CONFIG_DIR', 'BOT_TOKEN', 'X' * 65, 'HAS-DASH'):
            result = self.ask(name)
            self.assertEqual(result.state, 'refused', name)
            self.assertIn('secret name', result.text)
        self.assertEqual(self.count('envelopes'), 0)

    def test_a_name_set_in_the_service_environment_is_refused(self):
        with patch.dict(os.environ, {'DEPLOY_TOKEN': 'x'}):
            result = self.ask('DEPLOY_TOKEN')
        self.assertEqual(result.state, 'refused')
        self.assertIn('service environment', result.text)
        self.assertEqual(self.count('envelopes'), 0)

    def test_ask_before_bot_identity_or_for_unknown_task_is_refused(self):
        with self.store.db:
            self.store.put('bot_username', None)
        self.assertIn('bot identity', self.ask().text)
        with self.store.db:
            self.store.put('bot_username', BOT)
        self.assertEqual(self.ask(task=99).text, 'Unknown job.')
        self.assertEqual(self.count('envelopes'), 0)

    def test_mcp_schema_constrains_secret_names(self):
        tool = next(tool for tool in mcp.tools_list() if tool['name'] == 'secret_ask')
        self.assertEqual(tool['inputSchema']['properties']['name']['pattern'], '^[A-Z][A-Z0-9_]{1,63}$')
        self.assertEqual(set(tool['inputSchema']['required']), {'topic', 'name', 'reason', 'consumer'})


class CancelAndExpiryTests(EnvelopeFixture):
    def test_owner_cancel_closes_the_card_and_removes_its_buttons(self):
        self.ask()
        envelope = self.envelope()
        original = envelope['card_outbox']
        self.store.delivered(original, 900)
        self.assertEqual(self.click_cancel(1, envelope), 'envelope_cancel')
        closed = self.envelope()
        self.assertEqual(closed['state'], 'cancelled')
        self.assertEqual(closed['token_hash'], envelope['token_hash'])
        self.assertIsNone(self.outbox(original)['reply_markup'])
        edit = self.outbox(closed['card_outbox'])
        self.assertEqual((edit['edit_message'], edit['markup'], edit['text']),
                         (900, None, 'Envelope: GITHUB_TOKEN\nCancelled.'))
        self.assertEqual(self.events()[-1], ('cancel', None, 'button'))
        self.assertEqual(self.click_cancel(2, envelope), 'envelope_closed')

    def test_cancel_from_another_user_changes_nothing(self):
        self.ask()
        envelope = self.envelope()
        self.assertEqual(self.click_cancel(1, envelope, user=99), 'unauthorized')
        self.assertEqual(self.envelope()['state'], 'armed')
        self.assertEqual(self.count('outbox'), 1)

    def test_setup_menu_does_not_invalidate_the_card(self):
        from coordinator.controls import handle_control
        self.ask()
        envelope = self.envelope()
        with self.store.db:
            handle_control(self.store, TOPIC, 2, '/setup')
        self.assertEqual(self.click_cancel(3, envelope), 'envelope_cancel')
        self.assertEqual(self.envelope()['state'], 'cancelled')

    def test_sweep_expires_live_envelopes_and_clears_the_link(self):
        self.ask()
        envelope = self.envelope()
        with self.store.db:
            self.assertEqual(envelopes.sweep(self.store, envelope['created'] + 599), 0)
            self.assertEqual(envelopes.sweep(self.store, envelope['created'] + 601), 1)
        expired = self.envelope()
        self.assertEqual(expired['state'], 'expired')
        self.assertIsNone(self.outbox(envelope['card_outbox'])['reply_markup'])
        self.assertIn('Expired', self.outbox(expired['card_outbox'])['text'])
        self.assertEqual(self.events()[-1], ('expire', None, 'sweep'))

    def click_again(self, number, envelope, user=OWNER, bot=False, chat=GROUP):
        return self.store.accept({'update_id': number, 'callback_query': {
            'id': str(number), 'from': {'id': user, 'is_bot': bot},
            'data': 'envelope:%d:again' % envelope['id'],
            'message': {'message_id': 900, 'message_thread_id': 4,
                        'chat': {'id': chat, 'type': 'supergroup'}}}})

    def expire_card(self, name='GITHUB_TOKEN'):
        envelope = self.envelope(name)
        with self.store.db:
            self.store.db.execute('UPDATE envelopes SET expires=created-1 WHERE id=?', (envelope['id'],))
            envelopes.sweep(self.store)
        return self.envelope(name)

    def test_F4_expired_card_asks_once_with_the_same_metadata(self):
        task = self.store.task_create(TOPIC, 'Release')
        number = 1
        name = 'GITHUB_TOKEN'
        self.ask(name, task=task['id'])
        self.store.delivered(self.envelope(name)['card_outbox'], 900)
        old = self.expire_card(name)
        card = self.outbox(old['card_outbox'])
        self.assertEqual((card['text'], card['markup'], card['edit_message']),
                         ('Envelope: ' + name + '\nExpired. Tap Ask again to request it again.',
                          {'inline_keyboard': [[{'text': 'Ask again',
                                                'callback_data': 'envelope:%d:again' % old['id']}]]}, 900))
        before = self.count('envelopes')
        self.assertEqual(self.click_again(number * 10, old), 'envelope_again')
        self.assertEqual(self.count('envelopes'), before + 1)
        fresh = self.envelope(name)
        self.assertNotEqual(fresh['id'], old['id'])
        for key in ('name', 'reason', 'consumer', 'task', 'topic'):
            self.assertEqual(fresh[key], old[key], key)
        new_card = self.outbox(fresh['card_outbox'])
        self.assertIsNone(new_card['edit_message'])
        self.assertIn('Envelope: ' + name, new_card['text'])
        old = dict(self.store.db.execute('SELECT * FROM envelopes WHERE id=?', (old['id'],)).fetchone())
        edit = self.outbox(old['card_outbox'])
        self.assertEqual((edit['text'], edit['markup'], edit['edit_message']),
                         ('Envelope: ' + name + '\nExpired. Asked again.', None, 900))
        self.assertIn(('ask', None, 'button'), self.events())
        self.assert_inert_again(number * 10 + 1, old)
        self.assertEqual(self.count('envelopes'), before + 1)
        with self.store.db:
            envelopes.cancel(self.store, fresh['id'], 'button')

    def assert_inert_again(self, number, envelope, **sender):
        tables = ('envelopes', 'outbox', 'envelope_events', 'envelope_effects', 'settings')
        before = {t: [tuple(r) for r in self.store.db.execute('SELECT * FROM ' + t)] for t in tables}
        self.assertIn(self.click_again(number, envelope, **sender), ('envelope_closed', 'unauthorized'))
        after = {t: [tuple(r) for r in self.store.db.execute('SELECT * FROM ' + t)] for t in tables}
        before['settings'] = [r for r in before['settings'] if r[0] != 'offset']
        after['settings'] = [r for r in after['settings'] if r[0] != 'offset']
        self.assertEqual(after, before)

    def assert_stale_again(self, number, envelope):
        tables = ('envelopes', 'outbox', 'envelope_events', 'envelope_effects', 'settings')
        before = {t: [dict(r) for r in self.store.db.execute('SELECT * FROM ' + t)] for t in tables}
        self.assertEqual(self.click_again(number, envelope), 'envelope_closed')
        current = dict(self.store.db.execute('SELECT * FROM envelopes WHERE id=?', (envelope['id'],)).fetchone())
        edit = self.outbox(current['card_outbox'])
        self.assertEqual((edit['text'], edit['markup'], edit['edit_message']),
                         ('Envelope: ' + envelope['name'] + '\nExpired. Asked again.', None,
                          envelopes.card_message(self.store, envelope['card_outbox'])))
        for row in before['envelopes']:
            if row['id'] == envelope['id']:
                row['card_outbox'] = current['card_outbox']
        for row in before['outbox']:
            if row['id'] == envelope['card_outbox']:
                row['reply_markup'] = None
        after = {t: [dict(r) for r in self.store.db.execute('SELECT * FROM ' + t)] for t in tables}
        self.assertEqual(after['outbox'].pop()['id'], current['card_outbox'])
        for snapshot in (before, after):
            snapshot['settings'] = [r for r in snapshot['settings'] if r['key'] != 'offset']
        self.assertEqual(after, before)
        self.assert_inert_again(number + 1, current)

    def test_review_F3_stale_expired_cards_acknowledge_a_newer_ask(self):
        self.ask()
        first = self.expire_card()
        self.ask()
        latest = self.expire_card()
        self.assert_stale_again(1, first)
        self.ask()
        self.assert_stale_again(3, latest)

    def test_review_F3_agent_rotation_retires_the_expired_button(self):
        self.ask()
        self.store.delivered(self.envelope()['card_outbox'], 900)
        old = self.expire_card()
        self.assertTrue(self.call('secret.rotate', name=old['name']).ok)
        self.assert_stale_again(1, old)

    def test_F4_cancelled_and_live_cards_are_inert(self):
        self.ask()
        cancelled = self.envelope()
        with self.store.db:
            envelopes.cancel(self.store, cancelled['id'], 'button')
        self.assert_inert_again(2, cancelled)
        self.ask()
        self.assert_inert_again(3, self.envelope())

    def test_F4_live_name_guard_runs_before_ask_even_when_latest_is_expired(self):
        self.ask()
        old = self.expire_card()
        self.ask()
        latest = self.expire_card()
        with self.store.db:
            self.store.db.execute("UPDATE envelopes SET state='open',expires=? WHERE id=?",
                                  (time.time() + 600, old['id']))
        self.assert_stale_again(1, latest)

    def test_F4_other_users_bots_and_chats_send_nothing(self):
        self.ask()
        old = self.expire_card()
        for number, sender in enumerate(({'user': 99}, {'bot': True}, {'chat': GROUP - 1}), 1):
            with self.subTest(sender=sender):
                self.assert_inert_again(number, old, **sender)

    def test_F4_again_clears_pause_and_prompts_the_oldest_waiter(self):
        self.ask()
        self.ask('NPM_TOKEN')
        old = self.expire_card()
        self.assertFalse(self.store.get('envelope_prompt_exit')['acknowledged'])
        self.assertEqual(self.click_again(1, old), 'envelope_again')
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'armed')
        self.assertEqual(self.envelope()['state'], 'open')
        replies = [r[0] for r in self.store.db.execute("SELECT text FROM envelope_effects WHERE kind='reply' ORDER BY id")]
        self.assertEqual(replies[-1], 'Send the value for NPM_TOKEN now in one message. '
                         'I will delete it after I store it. Send /cancel to stop.')
        self.assertIsNone(self.store.get('envelope_prompt_exit'))

    def test_review_B1_again_with_a_waiter_posts_only_the_tracked_card(self):
        self.ask()
        self.ask('NPM_TOKEN')
        for row in self.store.db.execute('SELECT id FROM outbox WHERE delivered=0').fetchall():
            self.store.delivered(row['id'], 5000 + row['id'])
        old = self.expire_card()
        for row in self.store.db.execute('SELECT id FROM outbox WHERE delivered=0').fetchall():
            self.store.delivered(row['id'], 5000 + row['id'])
        mark = self.store.db.execute('SELECT max(id) FROM outbox').fetchone()[0]
        self.assertEqual(self.click_again(1, old), 'envelope_again')
        fresh = self.envelope()
        posts = self.store.db.execute('''SELECT id FROM outbox WHERE id>? AND edit_message IS NULL
            AND text LIKE 'Envelope: GITHUB_TOKEN%' ORDER BY id''', (mark,)).fetchall()
        self.assertEqual([row['id'] for row in posts], [fresh['card_outbox']])
        card = self.outbox(fresh['card_outbox'])
        self.assertEqual(card['text'], 'Envelope: GITHUB_TOKEN\nWhy: Push the release branch\n'
                         'Used by: release worker\nJob: any job\n'
                         'Waiting behind another credential ask in the private chat.\nNever paste it in this channel.')
        self.assertEqual(card['markup'], {'inline_keyboard': [
            [{'text': 'Fill privately', 'callback_data': 'envelope:%d:fill' % fresh['id']}],
            [{'text': 'Cancel', 'callback_data': 'envelope:%d:cancel' % fresh['id']}]]})

    def test_review_F1_refused_again_is_answered_recorded_and_does_not_stop_polling(self):
        self.ask()
        self.ask('NPM_TOKEN')
        old = self.expire_card()
        telegram = FakeTelegram()
        service = Service(self.store, telegram, object(), self.root, vault=self.vault)
        tables = ('envelopes', 'outbox', 'envelope_events', 'envelope_effects', 'settings')
        before = {t: [tuple(r) for r in self.store.db.execute('SELECT * FROM ' + t)] for t in tables}
        update = {'update_id': 41, 'callback_query': {
            'id': 'q41', 'from': {'id': OWNER, 'is_bot': False}, 'data': 'envelope:%d:again' % old['id'],
            'message': {'message_id': 900, 'message_thread_id': 4,
                        'chat': {'id': GROUP, 'type': 'supergroup'}}}}
        offsets = []

        async def updates(offset):
            offsets.append(offset)
            if len(offsets) == 3:
                raise asyncio.CancelledError()
            return [update] if offset <= update['update_id'] else []

        telegram.updates = updates
        service.task_restart_limit = 0
        with patch.dict(os.environ, {'GITHUB_TOKEN': 'synthetic-environment-fixture'}):
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(asyncio.wait_for(service.supervise('poll', service.poll), 5))
        self.assertEqual(offsets, [0, 42, 42])
        self.assertIsNotNone(self.store.db.execute('SELECT 1 FROM updates WHERE id=41').fetchone())
        self.assertEqual(self.store.get('offset'), 42)
        self.assertEqual(telegram.calls, [('answerCallbackQuery', {
            'callback_query_id': 'q41',
            'text': 'GITHUB_TOKEN is set in the service environment. Choose another name.'})])
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM problems WHERE code='loop-crashed'").fetchone()[0], 0)
        after = {t: [tuple(r) for r in self.store.db.execute('SELECT * FROM ' + t)] for t in tables}
        for snapshot in (before, after):
            snapshot['settings'] = [r for r in snapshot['settings'] if r[0] != 'offset']
        self.assertEqual(after, before)

    def test_F4_again_while_busy_keeps_the_current_prompt(self):
        self.ask()
        old = self.expire_card()
        self.store.accept(private(1, '/start'))
        self.ask('NPM_TOKEN')
        before = self.count('envelope_effects')
        self.assertEqual(self.click_again(2, old), 'envelope_again')
        self.assertEqual(self.envelope('NPM_TOKEN')['state'], 'armed')
        self.assertEqual(self.envelope()['state'], 'open')
        self.assertEqual(self.count('envelope_effects'), before)


class ServiceWiringTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)
        self.addCleanup(stop_test_hosts, self.root)
        self.store = Store(self.root / 'state')
        self.telegram = FakeTelegram()

    async def asyncTearDown(self):
        self.store.close()

    def service(self, vault=None):
        async def factory(*args, **kwargs):
            raise AssertionError('no session in this test')
        return Service(self.store, self.telegram, object(), self.root, session_factory=factory, vault=vault)

    async def test_envelopes_loop_confirms_bot_identity_and_retries_after_failure(self):
        service = self.service()
        self.telegram.failures['getMe'] = [TelegramError('network-or-invalid-response')]
        await service.envelopes_once()
        self.assertIsNone(self.store.get('bot_username'))
        self.assertFalse(self.store.db.in_transaction)
        await service.envelopes_once()
        self.assertEqual([call[0] for call in self.telegram.calls], ['getMe'])
        service.bot_identity_retry = 0
        await service.envelopes_once()
        self.assertEqual(self.store.get('bot_username'), BOT)
        await service.envelopes_once()
        self.assertEqual([call[0] for call in self.telegram.calls], ['getMe', 'getMe'])
        self.assertIn(('envelopes', service.envelopes), service.supervised())
        pair_only = Service(self.store, self.telegram, object(), self.root, pair_only=True)
        self.assertIn('envelopes', [name for name, _ in pair_only.supervised()])

    async def test_service_passes_its_vault_to_the_store_and_workers(self):
        vault = FakeVault()
        service = self.service(vault)
        self.assertIs(self.store.vault, vault)
        self.assertIs(service.workers.vault, vault)
        self.assertIsNone(Store(self.root / 'other').vault)

    async def test_vault_key_variables_leave_the_service_environment(self):
        key = self.root / 'master.key'
        key.write_text(bytes(range(32)).hex())
        key.chmod(0o600)
        binary = self.root / 'fake-claude'
        seen = self.root / 'environment.json'
        binary.write_text('#!/usr/bin/env python3\nimport json,os\n'
                          'open(%r,"w").write(json.dumps(sorted(os.environ)))\nprint("{}")\n' % str(seen))
        binary.chmod(0o700)
        with patch.dict(os.environ, {'TORII_VAULT_KEY_FILE': str(key), 'TORII_VAULT_KEY': 'ignored'}):
            vault = vault_from_environment(self.root / 'state')
            self.assertNotIn('TORII_VAULT_KEY_FILE', os.environ)
            self.assertNotIn('TORII_VAULT_KEY', os.environ)
            await collect_status({'config_dir': str(self.root)}, binary=str(binary), timeout=10)
        names = json.loads(seen.read_text())
        self.assertNotIn('TORII_VAULT_KEY_FILE', names)
        self.assertNotIn('TORII_VAULT_KEY', names)
        self.assertIsInstance(vault, FileVault)
        self.assertEqual(vault.master(), bytes(range(32)))
        self.assertFalse((self.root / 'state' / 'envelope').exists())


def private(update_id, text=None, user=OWNER, chat=None, **extra):
    message = {'message_id': 100 + update_id, 'from': {'id': user, 'is_bot': False},
               'chat': {'id': user if chat is None else chat, 'type': 'private'}, **extra}
    if text is not None:
        message['text'] = text
    return {'update_id': update_id, 'message': message}


class IntakeFixture(EnvelopeFixture):
    def setUp(self):
        super().setUp()
        self.telegram = FakeTelegram()

    def run_effects(self):
        asyncio.run(envelopes.run_effects(self.store, self.telegram))

    def effects(self):
        return [tuple(row) for row in self.store.db.execute(
            'SELECT kind,chat,message,text,state FROM envelope_effects ORDER BY id')]

    def replies(self):
        return [row[3] for row in self.effects() if row[0] == 'reply']

    def deletes(self):
        return [row[2] for row in self.effects() if row[0] == 'delete']

    def armed(self, name='GITHUB_TOKEN'):
        self.ask(name)
        self.run_effects()
        self.assertEqual(self.envelope(name)['state'], 'armed')

    def dated_private(self, number, text=None, **extra):
        return private(number, text, date=self.telegram.date + 1, **extra)

    def assert_value_absent(self, value=VALUE):
        for table in ('messages', 'outbox', 'updates', 'settings', 'envelopes', 'envelope_events',
                      'envelope_effects', 'tasks', 'workers', 'service_requests'):
            for row in self.store.db.execute('SELECT * FROM ' + table):
                self.assertNotIn(value, json.dumps(list(row)), table)


class IntakeTests(IntakeFixture):
    def test_owner_fills_an_envelope_privately(self):
        value = 'FAKE-ENVELOPE-VALUE-0123456789-abcdefghi'
        self.assertEqual(len(value), 40)
        task = self.store.task_create(TOPIC, 'Release')
        self.ask(task=task['id'])
        self.run_effects()
        armed = self.envelope()
        self.assertEqual((armed['state'], armed['chat']), ('armed', OWNER))
        self.assertEqual(self.outbox(armed['card_outbox'])['markup']['inline_keyboard'][0][0],
                         {'text': 'Fill privately', 'url': 'https://t.me/' + BOT})
        self.assertEqual(self.store.accept(self.dated_private(2, value)), 'envelope_fill')
        self.assertEqual(self.vault.values, {'GITHUB_TOKEN': value})
        fingerprint = self.vault.fingerprint(value)
        filled = self.envelope()
        self.assertEqual((filled['state'], filled['length'], filled['fingerprint']), ('filled', 40, fingerprint))
        self.assertEqual(self.deletes(), [102])
        self.assertEqual(self.replies()[-1], 'GITHUB_TOKEN stored. Length 40, fingerprint %s.' % fingerprint)
        card = self.outbox(filled['card_outbox'])
        self.assertIn('Filled. Length 40, fingerprint ' + fingerprint, card['text'])
        self.assertIsNone(card['markup'])
        self.assertEqual([event for event, _, _ in self.events()], ['ask', 'arm', 'fill', 'use'])
        self.assertEqual(self.events()[-1], ('use', None, 'intake:readback'))
        notice, = self.store.db.execute("SELECT * FROM messages WHERE kind='secret_filled'").fetchall()
        self.assertEqual(notice['topic'], TOPIC)
        self.assertEqual(notice['source_envelope'], filled['id'])
        self.assertEqual(notice['text'], 'Secret GITHUB_TOKEN, declared on job 1, is filled.')
        self.assertNotIn(value, notice['text'])
        self.assertNotIn('Length', notice['text'])
        self.assertNotIn('fingerprint', notice['text'])
        self.assertEqual(self.store.accept(self.dated_private(2, value)), 'duplicate')
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM messages WHERE kind='secret_filled'").fetchone()[0], 1)
        self.assert_value_absent(value)
        self.run_effects()
        self.assertEqual([call[0] for call in self.telegram.calls], ['sendMessage', 'deleteMessage', 'sendMessage'])
        self.assertEqual(self.telegram.calls[1][1], {'chat_id': OWNER, 'message_id': 102})
        self.assertEqual({row[4] for row in self.effects()}, {'done'})
        self.assertFalse(self.store.db.in_transaction)

    def assert_short_value_stored(self, name, value):
        self.assertTrue(self.ask(name).ok)
        self.run_effects()
        self.assertEqual(self.store.accept(self.dated_private(2, value)), 'envelope_fill')
        self.assertEqual(self.vault.values[name], value)
        fingerprint = self.vault.fingerprint(value)
        filled = self.envelope(name)
        self.assertEqual((filled['state'], filled['length'], filled['fingerprint']),
                         ('filled', len(value), fingerprint))
        self.assertEqual(self.replies()[-1], '%s stored. Length %d, fingerprint %s.' %
                         (name, len(value), fingerprint))
        self.assertEqual(self.deletes(), [102])

    def test_eight_character_credential_is_stored(self):
        self.assert_short_value_stored('SHORT_EIGHT', 'Ab3dEf9h')

    def test_one_character_credential_is_stored(self):
        self.assert_short_value_stored('SHORT_ONE', 'x')

    def test_filled_notification_survives_store_restart(self):
        self.armed()
        self.store.accept(self.dated_private(2, VALUE))
        self.store.close()
        self.store = Store(self.root / 'state')
        rows = self.store.messages_pending()
        self.assertEqual([(row['topic'], row['kind'], row['text']) for row in rows],
                         [(TOPIC, 'secret_filled', 'Secret GITHUB_TOKEN, declared on any job, is filled.')])

    def test_fill_notifies_only_the_envelope_channel_parent(self):
        other = '-10042:5'
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', (other, GROUP, 5, 'Ideas', str(self.root)))
        task = self.store.task_create(other, 'Ideas work')
        with self.store.db:
            envelope = envelopes.ask(self.store, other, 'GITHUB_TOKEN', 'Read only', 'worker', task['id'])
        self.run_effects()
        self.store.accept(self.dated_private(2, VALUE))
        rows = self.store.db.execute("SELECT topic,kind,text FROM messages WHERE kind='secret_filled'").fetchall()
        self.assertEqual([tuple(row) for row in rows],
                         [(other, 'secret_filled', 'Secret GITHUB_TOKEN, declared on job 1, is filled.')])

    def test_one_accept_commits_once(self):
        self.armed()
        statements = []
        self.store.db.set_trace_callback(statements.append)
        self.store.accept(self.dated_private(2, VALUE))
        self.store.db.set_trace_callback(None)
        self.assertEqual([statement for statement in statements if statement.strip().upper() == 'COMMIT'], ['COMMIT'])
        self.assertFalse(self.store.db.in_transaction)

    def test_stranger_and_mismatched_chats_are_ignored_silently(self):
        self.ask()
        before = self.effects()
        self.assertEqual(self.store.accept(self.dated_private(1, '/start', user=99)), 'ignored')
        self.assertEqual(self.store.accept(self.dated_private(2, '/start', chat=55)), 'ignored')
        bot = private(3, '/start')
        bot['message']['from']['is_bot'] = True
        self.assertEqual(self.store.accept(bot), 'ignored')
        self.assertEqual(self.store.accept(self.dated_private(4, '/start', sender_chat={'id': 1})), 'ignored')
        self.assertEqual((self.effects(), self.envelope()['state'], self.vault.calls), (before, 'armed', []))
        self.assertEqual([event for event, _, _ in self.events()], ['ask', 'arm'])
        self.assertEqual(self.store.get('offset'), 5)
        self.assertEqual(self.store.accept(self.dated_private(5, '/start')), 'envelope_arm')

    def test_group_messages_never_arm_or_fill(self):
        self.ask()
        self.ask('NPM_TOKEN')
        before = self.envelope('NPM_TOKEN')
        group = {'update_id': 1, 'message': {'message_id': 1, 'message_thread_id': 4, 'text': '/start arbitrary',
                                         'from': {'id': OWNER}, 'chat': {'id': GROUP, 'type': 'supergroup'}}}
        self.store.accept(group)
        self.assertEqual(self.envelope('NPM_TOKEN'), before)
        self.assertEqual(self.envelope()['state'], 'armed')
        self.assertEqual(self.vault.calls, [])

    def test_rejected_values_are_deleted_and_keep_the_envelope_armed(self):
        self.armed()
        cases = ['x' * 4097, 'two\nlines-of-value', '/not-a-value-at-all',
                 '12345678901234567890', 'tab\there-is-a-value']
        for number, text in enumerate(cases, 2):
            self.assertEqual(self.store.accept(self.dated_private(number, text)), 'envelope_reject')
        self.store.accept(self.dated_private(20, photo=[{'file_id': 'x', 'width': 1, 'height': 1}]))
        reasons = [reason for event, reason, _ in self.events() if event == 'reject']
        self.assertEqual(reasons, ['too_long', 'multi_line', 'command', 'numeric', 'not_printable', 'not_text'])
        self.assertEqual(self.deletes(), [102, 103, 104, 105, 106, 120])
        self.assertEqual(self.envelope()['state'], 'armed')
        self.assertEqual(self.vault.values, {})

    def test_value_without_an_armed_envelope_is_deleted(self):
        self.assertEqual(self.store.accept(self.dated_private(1, VALUE)), 'envelope_reject')
        self.assertEqual(self.deletes(), [101])
        self.assertEqual(self.vault.calls, [])
        self.assertIn('No envelope is waiting', self.replies()[-1])

    def test_vault_failure_keeps_the_envelope_armed_until_a_retry_succeeds(self):
        self.armed()
        self.vault.fail_next = 'io'
        self.assertEqual(self.store.accept(self.dated_private(2, VALUE)), 'envelope_reject')
        self.assertEqual(self.events()[-1], ('reject', 'vault_failed:io', 'intake'))
        self.assertEqual(self.envelope()['state'], 'armed')
        self.assertEqual(self.replies()[-1], 'The vault write failed. Your message was deleted. Send the value again.')
        self.assertEqual(self.store.accept(self.dated_private(3, VALUE)), 'envelope_fill')
        self.assertEqual(self.deletes(), [102, 103])

    def test_missing_vault_is_a_vault_failure(self):
        self.armed()
        self.store.vault = None
        self.store.accept(self.dated_private(2, VALUE))
        self.assertEqual(self.events()[-1], ('reject', 'vault_failed:unavailable', 'intake'))

    def test_duplicate_update_writes_the_vault_once(self):
        self.armed()
        self.assertEqual(self.store.accept(self.dated_private(2, VALUE)), 'envelope_fill')
        self.assertEqual(self.store.accept(self.dated_private(2, VALUE)), 'duplicate')
        self.assertEqual(self.vault.calls, [('put', 'GITHUB_TOKEN')])

    def test_failure_after_the_vault_write_still_deletes_and_commits_the_cursor(self):
        self.armed()
        with patch('coordinator.envelopes._fill', side_effect=KeyError('boom')):
            self.assertEqual(self.store.accept(self.dated_private(2, VALUE)), 'envelope_failed')
        self.assertEqual(self.vault.calls, [('put', 'GITHUB_TOKEN')])
        self.assertEqual(self.deletes(), [102])
        self.assertEqual(self.events()[-1], ('reject', 'internal', 'intake'))
        self.assertEqual(self.store.get('offset'), 3)
        self.assertEqual(self.envelope()['state'], 'armed')
        self.assertFalse(self.store.db.in_transaction)

    def test_integrity_error_is_handled_like_a_vault_error(self):
        self.ask()
        with patch('coordinator.envelopes._start', side_effect=sqlite3.IntegrityError('conflict')):
            self.assertEqual(self.store.accept(self.dated_private(1, '/start')), 'envelope_failed')
        self.assertEqual(self.deletes(), [])
        self.assertEqual(self.store.get('offset'), 2)

    def test_private_cancel_closes_the_armed_envelope(self):
        self.armed()
        self.assertEqual(self.store.accept(self.dated_private(2, '/cancel')), 'envelope_cancel')
        cancelled = self.envelope()
        self.assertEqual(cancelled['state'], 'cancelled')
        self.assertEqual(self.outbox(cancelled['card_outbox'])['text'], 'Envelope: GITHUB_TOKEN\nCancelled.')
        self.assertEqual(self.store.accept(self.dated_private(3, '/cancel')), 'envelope_reject')
        self.assertEqual(self.deletes(), [])

    def test_a_second_fill_supersedes_the_first(self):
        self.armed()
        self.store.accept(self.dated_private(2, VALUE))
        first = self.envelope()['id']
        self.ask()
        self.run_effects()
        self.assertEqual(self.store.accept(self.dated_private(4, VALUE + '-rotated')), 'envelope_fill')
        states = dict(self.store.db.execute('SELECT id,state FROM envelopes').fetchall())
        self.assertEqual(states[first], 'superseded')
        self.assertEqual(sorted(states.values()), ['filled', 'superseded'])
        self.assertEqual(self.vault.values['GITHUB_TOKEN'], VALUE + '-rotated')

    def test_fill_against_a_real_file_vault(self):
        self.store.vault = FileVault(self.root / 'state')
        self.armed()
        self.assertEqual(self.store.accept(self.dated_private(2, VALUE)), 'envelope_fill')
        data = (self.root / 'state' / 'envelope' / 'vault.enc').read_bytes()
        self.assertNotIn(VALUE.encode(), data)
        self.assertEqual(FileVault(self.root / 'state').get('GITHUB_TOKEN'), VALUE)
        self.assertEqual(self.envelope()['fingerprint'], self.store.vault.fingerprint(VALUE))


class ArmedCardTests(IntakeFixture):
    def test_armed_card_says_the_value_goes_to_the_private_chat(self):
        self.armed()
        card = self.outbox(self.envelope()['card_outbox'])
        self.assertTrue(card['text'].endswith('\nWaiting for the value in the private chat. Tap Fill privately to open it.\nNever paste it in this channel.'), card['text'])


class EffectTests(IntakeFixture):
    def filled(self):
        self.armed()
        card = self.envelope()['card_outbox']
        self.store.delivered(self.store.db.execute(
            "SELECT id FROM outbox WHERE kind='envelope' ORDER BY id LIMIT 1").fetchone()[0], 900)
        self.store.delivered(card, 900)
        self.store.accept(self.dated_private(2, VALUE))

    def test_delete_refused_with_400_fails_once_and_notifies(self):
        self.filled()
        filled_card = self.envelope()['card_outbox']
        self.assertEqual(self.outbox(filled_card)['edit_message'], 900)
        self.store.delivered(filled_card, 900)
        self.telegram.failures['deleteMessage'] = [TelegramError(400)]
        self.run_effects()
        self.assertEqual([row[4] for row in self.effects() if row[0] == 'delete'], ['failed'])
        self.assertEqual(self.envelope()['state'], 'filled')
        self.assertEqual(self.events()[-1][0], 'delete_failed')
        self.assertEqual(self.replies()[-1],
                         'I could not delete your message. Delete it yourself and consider rotating GITHUB_TOKEN.')
        card = self.outbox(self.envelope()['card_outbox'])
        self.assertEqual(card['edit_message'], 900)
        self.assertIn('Filled.', card['text'])
        self.assertIn('consider rotating GITHUB_TOKEN', card['text'])
        self.run_effects()
        self.assertEqual([row[4] for row in self.effects()], ['done', 'failed', 'done', 'done'])

    def test_message_already_gone_counts_as_deleted(self):
        self.filled()
        self.telegram.failures['deleteMessage'] = [TelegramError(400, reason='message_not_found')]
        self.run_effects()
        self.assertEqual([row[4] for row in self.effects() if row[0] == 'delete'], ['done'])
        self.assertNotIn('delete_failed', [event for event, _, _ in self.events()])

    def test_transient_errors_retry_and_replies_fail_finally(self):
        self.filled()
        self.telegram.failures['deleteMessage'] = [TelegramError('network-or-invalid-response', reason='timeout')]
        self.telegram.failures['sendMessage'] = [TelegramError(500)] * 10
        started = time.time()
        self.run_effects()
        delete = self.store.db.execute("SELECT * FROM envelope_effects WHERE kind='delete'").fetchone()
        self.assertEqual((delete['state'], delete['attempts']), ('pending', 1))
        self.assertGreaterEqual(delete['next_attempt'], started + 2)
        self.assertLess(delete['next_attempt'], started + 3)
        self.assertNotIn('delete_failed', [event for event, _, _ in self.events()])
        for _ in range(5):
            with self.store.db:
                self.store.db.execute('UPDATE envelope_effects SET next_attempt=0')
            self.run_effects()
        states = {row['kind']: row['state'] for row in self.store.db.execute('SELECT kind,state FROM envelope_effects')}
        self.assertEqual(states, {'delete': 'done', 'reply': 'failed'})
        self.assertFalse(self.store.db.in_transaction)


class TelegramReasonTests(unittest.TestCase):
    def test_only_the_not_found_description_becomes_a_reason(self):
        from coordinator.telegram import _reason
        self.assertEqual(_reason({'description': 'Bad Request: message to delete not found'}), 'message_not_found')
        self.assertIsNone(_reason({'description': "Bad Request: message can't be deleted"}))
        self.assertIsNone(_reason(None))


def topic_reply(update_id, reply, text=VALUE):
    return {'update_id': update_id, 'message': {
        'message_id': 200 + update_id, 'message_thread_id': 4, 'text': text, 'from': {'id': OWNER},
        'chat': {'id': GROUP, 'type': 'supergroup'}, 'reply_to_message': {'message_id': reply}}}


class TopicReplyTests(IntakeFixture):
    def delivered_card(self):
        self.ask()
        self.store.delivered(self.envelope()['card_outbox'], 900)

    def test_reply_to_a_live_card_is_deleted_and_never_saved(self):
        self.delivered_card()
        for number, text in ((1, VALUE), (2, '/settings-looking-value')):
            self.assertEqual(self.store.accept(topic_reply(number, 900, text)), 'envelope_intercept')
        self.assertEqual(self.count('messages'), 0)
        self.assertEqual([row[:3] for row in self.effects() if row[0] == 'delete'],
                         [('delete', GROUP, 201), ('delete', GROUP, 202)])
        notices = [row[0] for row in self.store.db.execute("SELECT text FROM outbox WHERE kind='report'")]
        self.assertEqual(notices, ['I removed your reply. Use Fill privately to send values.'] * 2)
        self.assertEqual(self.events()[-1], ('reject', 'topic_reply', 'intake'))
        self.assertEqual(self.envelope()['state'], 'armed')
        self.assert_value_absent()

    def test_reply_to_an_armed_card_after_its_edit_is_intercepted(self):
        self.delivered_card()
        self.run_effects()
        self.assertEqual(self.store.accept(topic_reply(2, 900)), 'envelope_intercept')

    def test_reply_to_a_closed_card_and_plain_messages_pass_through(self):
        self.delivered_card()
        before = self.effects()
        self.click_cancel(1, self.envelope())
        self.assertEqual(self.store.accept(topic_reply(2, 900, 'thanks')), 'queued')
        plain = topic_reply(3, 900, 'hello')
        del plain['message']['reply_to_message']
        self.assertEqual(self.store.accept(plain), 'queued')
        self.assertEqual(self.effects(), before)

    def test_group_delete_refusal_tells_the_owner_in_the_topic(self):
        self.delivered_card()
        self.store.accept(topic_reply(1, 900))
        self.telegram.failures['deleteMessage'] = [TelegramError(400)]
        self.run_effects()
        notices = [row[0] for row in self.store.db.execute("SELECT text FROM outbox WHERE kind='report'")]
        self.assertEqual(notices[-1], 'I could not delete your reply in this channel. '
                                      'Delete it yourself and consider rotating GITHUB_TOKEN.')
        card = self.outbox(self.envelope()['card_outbox'])
        self.assertIn('consider rotating GITHUB_TOKEN', card['text'])
        self.assertEqual(card['markup']['inline_keyboard'][1][0]['text'], 'Cancel')


class OperationTests(IntakeFixture):
    def fill(self, name='GITHUB_TOKEN', number=1):
        self.ask(name)
        self.run_effects()
        self.store.accept(self.dated_private(number + 1, VALUE))

    def service(self, pair_only=False):
        return Service(self.store, self.telegram, object(), self.root, pair_only=pair_only, vault=self.vault)

    def requests(self):
        return [tuple(row) for row in self.store.db.execute('SELECT op,state,result FROM service_requests ORDER BY id')]

    def test_list_shows_state_and_facts_but_never_values_or_tokens(self):
        self.fill()
        self.ask('NPM_TOKEN')
        legacy_hash = 'fake-legacy-token-hash'
        with self.store.db:
            self.store.db.execute("UPDATE envelopes SET token_hash=? WHERE name='NPM_TOKEN'", (legacy_hash,))
        result = self.call('secret.list')
        self.assertEqual([row['name'] for row in result.data], ['GITHUB_TOKEN', 'NPM_TOKEN'])
        filled = result.data[0]
        self.assertEqual((filled['state'], filled['length'], filled['fingerprint']),
                         ('filled', len(VALUE), self.vault.fingerprint(VALUE)))
        self.assertEqual(filled['last_use_source'], 'intake:readback')
        self.assertIn('GITHUB_TOKEN: filled, length 30, fingerprint', result.text)
        self.assertIn('NPM_TOKEN: armed', result.text)
        rendered = result.text + json.dumps(result.data)
        for secret in (VALUE, legacy_hash):
            self.assertNotIn(secret, rendered)
        self.assertEqual(self.call('secret.list', name='NPM_TOKEN').data[0]['name'], 'NPM_TOKEN')

    def test_revoke_is_queued_for_the_service_and_changes_nothing_yet(self):
        self.fill()
        before = self.envelope()
        queued = self.call('secret.revoke', name='GITHUB_TOKEN')
        self.assertEqual(queued.state, 'queued')
        self.assertEqual(self.requests(), [('secret.revoke', 'queued', None)])
        self.assertEqual(self.envelope(), before)
        self.assertEqual(self.vault.values, {'GITHUB_TOKEN': VALUE})
        unknown = self.call('secret.revoke', name='NEVER_ASKED')
        self.assertEqual(unknown.state, 'refused')
        with self.store.db:
            self.store.put('pair_only', True)
        self.assertIn('pair-only', self.call('secret.revoke', name='GITHUB_TOKEN').text)
        self.assertEqual(len(self.requests()), 1)

    def test_service_revokes_idempotently_and_reports(self):
        self.fill()
        self.ask('GITHUB_TOKEN')
        live = self.envelope()
        service = self.service()
        self.call('secret.revoke', name='GITHUB_TOKEN')
        asyncio.run(service.controls_once())
        self.assertFalse(self.store.db.in_transaction)
        self.assertEqual(self.vault.values, {})
        states = [row[0] for row in self.store.db.execute("SELECT state FROM envelopes WHERE name='GITHUB_TOKEN'")]
        self.assertEqual(states, ['revoked', 'revoked'])
        self.assertIn('Revoked', self.outbox(self.envelope()['card_outbox'])['text'])
        self.assertIsNone(self.outbox(live['card_outbox'])['reply_markup'])
        revokes = [row for row in self.events() if row[0] == 'revoke']
        self.assertEqual(revokes, [('revoke', None, 'service:revoke')] * 2)
        self.assertEqual(self.requests()[-1], ('secret.revoke', 'done', json.dumps({'text': 'GITHUB_TOKEN revoked.'})))
        report = self.store.db.execute("SELECT text FROM outbox WHERE kind='report' ORDER BY id DESC").fetchone()[0]
        self.assertTrue(report.startswith('GITHUB_TOKEN revoked.'))
        self.call('secret.revoke', name='GITHUB_TOKEN')
        asyncio.run(service.controls_once())
        self.assertEqual(json.loads(self.requests()[-1][2])['text'], 'GITHUB_TOKEN had no stored value. Nothing changed.')
        self.assertEqual(self.requests()[-1][1], 'done')
        self.assertEqual(self.events()[-1], ('revoke', 'nothing_stored', 'service:revoke'))
        self.assertFalse(self.store.db.in_transaction)

    def test_vault_failure_during_revoke_changes_nothing(self):
        self.fill()
        before = [tuple(row) for row in self.store.db.execute('SELECT * FROM envelopes')]
        events = self.count('envelope_events')
        self.call('secret.revoke', name='GITHUB_TOKEN')
        self.vault.fail_next = 'io'
        asyncio.run(self.service().controls_once())
        self.assertEqual(self.requests()[-1][1:], ('refused', json.dumps({'text': 'The vault delete failed. Nothing changed.'})))
        self.assertEqual([tuple(row) for row in self.store.db.execute('SELECT * FROM envelopes')], before)
        self.assertEqual(self.count('envelope_events'), events)
        self.assertEqual(self.vault.values, {'GITHUB_TOKEN': VALUE})

    def test_rotate_asks_again_and_keeps_the_old_value_until_the_new_fill(self):
        self.fill()
        rotated = self.call('secret.rotate', name='GITHUB_TOKEN')
        self.assertTrue(rotated.ok, rotated.text)
        fresh = self.envelope()
        self.assertEqual((fresh['state'], fresh['reason'], fresh['consumer']),
                         ('armed', 'Push the release branch', 'release worker'))
        self.assertEqual(self.vault.values, {'GITHUB_TOKEN': VALUE})
        self.assertEqual(self.vault.calls, [('put', 'GITHUB_TOKEN')])
        self.assertEqual(self.call('secret.rotate', name='GITHUB_TOKEN').state, 'refused')
        self.assertEqual(self.call('secret.rotate', name='NEVER_ASKED').state, 'refused')
        self.assertFalse(self.store.db.in_transaction)

    def test_telegram_revoke_needs_confirmation(self):
        from coordinator.controls import handle_control
        number = 60

        def tap(label, data=None):
            nonlocal number
            card = self.store.db.execute('SELECT * FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
            if data is None:
                buttons = json.loads(card['reply_markup'])['inline_keyboard']
                data = next(button['callback_data'] for row in buttons for button in row if button['text'] == label)
            number += 1
            return self.store.accept({'update_id': number, 'callback_query': {
                'id': str(number), 'from': {'id': OWNER}, 'data': data,
                'message': {'message_id': 1000 + card['id'], 'message_thread_id': 4,
                            'chat': {'id': GROUP, 'type': 'supergroup'}}}})

        self.fill()
        with self.store.db:
            handle_control(self.store, TOPIC, 50, '/secrets')
        listing = self.store.db.execute('SELECT * FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
        labels = [button['text'] for row in json.loads(listing['reply_markup'])['inline_keyboard'] for button in row]
        self.assertEqual(labels, ['GITHUB_TOKEN'])
        self.assertEqual(tap('GITHUB_TOKEN'), 'control_callback')
        self.assertEqual(tap('Revoke'), 'control_callback')
        self.assertEqual(self.requests(), [])
        self.assertEqual(self.vault.values, {'GITHUB_TOKEN': VALUE})
        self.assertEqual(self.envelope()['state'], 'filled')
        confirm = self.store.db.execute('SELECT * FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
        self.assertEqual(confirm['text'], 'Revoke GITHUB_TOKEN? This deletes the stored value. '
                         'Workers already running keep their copy until they exit.')
        buttons = json.loads(confirm['reply_markup'])['inline_keyboard']
        self.assertEqual([row[0]['text'] for row in buttons], ['Revoke', 'Back'])
        data = buttons[0][0]['callback_data']
        self.assertEqual(tap('Revoke', data), 'control_callback')
        request, = self.store.service_requests_pending()
        self.assertEqual((request['op'], json.loads(request['params'])), ('secret.revoke', {'name': 'GITHUB_TOKEN'}))
        self.assertEqual(tap('Revoke', data), 'stale_callback')
        self.assertEqual(len(self.store.service_requests_pending()), 1)
        self.assertEqual(self.vault.values, {'GITHUB_TOKEN': VALUE})
        self.assertEqual(self.envelope()['state'], 'filled')

    def test_manifest_lists_the_four_secret_operations(self):
        ids = [op.id for op in control_api.OPS]
        for op_id in ('secret.ask', 'secret.list', 'secret.rotate', 'secret.revoke'):
            self.assertIn(op_id, ids)
        self.assertEqual(control_api.find('secret.revoke').transport, control_api.SERVICE)


FAKE_WORKER = '''#!/usr/bin/env python3
import json,os,sys
from pathlib import Path
sid=sys.argv[sys.argv.index('--session-id')+1]
value=os.environ['GITHUB_TOKEN']
Path(__file__).with_name('worker-saw').write_text(str(len(value)))
Path(__file__).with_name('worker-environment').write_text(json.dumps(sorted(os.environ)))
print(json.dumps({'type':'system','subtype':'init','session_id':sid}),flush=True)
for line in sys.stdin:
    event=json.loads(line)
    print(json.dumps({**event,'isReplay':True}),flush=True)
    print(json.dumps({'type':'assistant','session_id':sid,'message':{'content':'token '+value}}),flush=True)
    print('stderr '+value,file=sys.stderr,flush=True)
    print(json.dumps({'type':'result','subtype':'success','session_id':sid,'result':'used '+value,
        'structured_output':{'echo':value}}),flush=True)
'''


class EndToEndTests(unittest.IsolatedAsyncioTestCase):
    """R13: no persisted Torii file holds a value, its encodings, or the key outside master.key."""

    async def asyncSetUp(self):
        from coordinator import log
        from coordinator.providers import ProviderRunner
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)
        environment = patch.dict(os.environ, {'PATH': os.environ.get('PATH', '/usr/bin:/bin')}, clear=True)
        environment.start()
        self.addCleanup(environment.stop)
        isolate_shared_mcp_home(self, self.root)
        self.addCleanup(stop_test_hosts, self.root)
        self.state = self.root / 'state'
        self.work = self.root / 'work'
        self.work.mkdir()
        self.binary = self.work / 'fake-claude'
        self.binary.write_text(FAKE_WORKER)
        self.binary.chmod(0o700)
        log.configure(self.state, stderr=False)
        self.store = Store(self.state)
        profile = self.root / 'claude-profile'
        profile.mkdir()
        with self.store.db:
            self.store.put('accounts', {'test': {'config_dir': str(profile), 'enabled': True}})
            self.store.put('account_status', {'test': {'identity': {
                'email': 'test@example.com', 'logged_in': True}, 'observed_at': time.time(),
                'usage': {'seven_day': {'utilization': 10, 'resets_at': time.time() + 3600}}}})
        self.telegram = FakeTelegram()
        self.service = Service(self.store, self.telegram, ProviderRunner(self.state, {'claude': str(self.binary)}),
                               self.root, vault=FileVault(self.state))
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', (TOPIC, GROUP, 4, 'Torii', str(self.work)))
            self.store.put('owner', OWNER)
            self.store.put('group', GROUP)
            self.store.put('bot_username', BOT)

    async def asyncTearDown(self):
        from coordinator import log
        self.store.close()
        log.reset()

    def call(self, op, **params):
        with self.store.db:
            result = control_api.call(self.store, op, params, topic=TOPIC, source='mcp')
        self.assertTrue(result.ok, result.text)
        return result

    async def update(self, **params):
        result = await control_api.call_async(self.store, 'tasks.update', params, topic=TOPIC, source='mcp')
        self.assertTrue(result.ok, result.text)
        return result

    def files(self):
        return {str(path.relative_to(self.state)): path.read_bytes()
                for path in sorted(self.state.rglob('*')) if path.is_file()}

    def holding(self, needle):
        return {name for name, data in self.files().items() if needle in data}

    def forms(self, value):
        encoded = value.encode()
        return [encoded, encoded.hex().encode(), base64.b64encode(encoded), json.dumps(value)[1:-1].encode(),
                json.dumps(value, ensure_ascii=False)[1:-1].encode()]


    async def drive(self, value=VALUE):
        self.call('secret.ask', name='GITHUB_TOKEN', reason='Push the release branch', consumer='release worker')
        await self.service.envelopes_once()
        self.assertEqual(self.store.accept(private(2, value, date=self.telegram.date + 1)), 'envelope_fill')
        await self.service.envelopes_once()
        asked = self.store.message_save(TOPIC, 'owner', 'Release it')['id']
        task = self.call('tasks.create', message=asked, title='Release', secrets='GITHUB_TOKEN').data
        with self.store.db:
            self.store.task_update(task['id'], worktree=str(self.work))
        worker = self.call('workers.spawn', task=task['id'], provider='claude', prompt='release').data['worker']
        row = dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (worker,)).fetchone())
        await asyncio.wait_for(self.service.workers.run(row, self.store.topic(TOPIC)), 30)
        self.assertEqual((self.work / 'worker-saw').read_text(), str(len(value)))
        environment = json.loads((self.work / 'worker-environment').read_text())
        self.assertEqual([key for key in environment if key.startswith(('TORII_', 'CLAUDE_', 'CODEX_'))],
                         ['CLAUDE_CONFIG_DIR'])
        return worker

    async def test_no_file_holds_a_value_or_loose_key(self):
        worker = await self.drive()
        result = json.loads(self.store.db.execute('SELECT result FROM workers WHERE id=?', (worker,)).fetchone()[0])
        self.assertTrue(result['success'], result.get('error'))
        self.assertEqual(result['text'], 'used [secret GITHUB_TOKEN]')
        self.assertEqual(result['structured'], {'echo': '[secret GITHUB_TOKEN]'})
        self.assertIn(b'[secret GITHUB_TOKEN]', Path(result['log_path']).read_bytes())
        self.store.backup_for_restart()
        self.call('secret.revoke', name='GITHUB_TOKEN')
        await self.service.controls_once()
        self.store.db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        files = self.files()
        self.assertTrue(any(name.startswith('backups/') for name in files))
        self.assertIn('hosts/worker-%d/events.jsonl' % worker, files)
        self.assertIn('hosts/worker-%d/spec.json' % worker, files)
        self.assertIn('logs/service.log', files)
        for form in self.forms(VALUE):
            self.assertEqual(self.holding(form), set(), form)
        master = (self.state / 'envelope' / 'master.key').read_bytes()
        self.assertEqual(self.holding(master), {'envelope/master.key'})
        envelope = self.state / 'envelope'
        self.assertEqual(envelope.stat().st_mode & 0o777, 0o700)
        self.assertEqual((envelope / 'master.key').stat().st_mode & 0o777, 0o600)
        self.assertEqual((envelope / 'vault.enc').stat().st_mode & 0o777, 0o600)
        self.assertEqual(sorted(path.name for path in envelope.iterdir()), ['master.key', 'vault.enc'])
        self.assertEqual(FileVault(self.state).names(), [])
        self.assertEqual(self.store.db.execute('SELECT state FROM service_requests').fetchone()[0], 'done')

    async def test_worker_receives_another_jobs_key_without_a_sharing_setting(self):
        self.assertIsNone(self.store.db.execute("SELECT 1 FROM settings WHERE key='secret_sharing'").fetchone())
        other = self.store.task_create(TOPIC, 'Store the key')
        job = self.store.task_create(TOPIC, 'Use the key', worktree=str(self.work), secrets=['GITHUB_TOKEN'])
        self.binary.write_text(FAKE_WORKER.replace(
            "value=os.environ['GITHUB_TOKEN']", "value=os.environ['GITHUB_TOKEN']\nassert value == %r" % VALUE))
        self.call('secret.ask', name='GITHUB_TOKEN', reason='Release', consumer='release worker', task=other['id'])
        await self.service.envelopes_once()
        self.assertEqual(self.store.accept(private(2, VALUE, date=self.telegram.date + 1)), 'envelope_fill')
        worker = self.call('workers.spawn', task=job['id'], provider='claude', prompt='release').data['worker']
        await self.service.workers_once()
        await asyncio.gather(*self.service.worker_tasks.values())
        finished = self.service.workers.get(worker)
        self.assertEqual(finished['status'], 'done', finished['result'])
        result = json.loads(finished['result'])
        self.assertTrue(result['success'], result)
        self.assertEqual(result['text'], 'used [secret GITHUB_TOKEN]')
        self.assertIn('GITHUB_TOKEN', json.loads((self.work / 'worker-environment').read_text()))
        self.assertEqual((self.work / 'worker-saw').read_text(), str(len(VALUE)))
        self.assertIsNone(self.store.db.execute("SELECT 1 FROM settings WHERE key='secret_sharing'").fetchone())

    async def test_sharing_delivers_keys_from_two_jobs_and_any_job_to_one_worker(self):
        first = self.store.task_create(TOPIC, 'First')
        second = self.store.task_create(TOPIC, 'Second')
        job = self.store.task_create(TOPIC, 'Release', worktree=str(self.work))
        names = [('GITHUB_TOKEN', first['id'], VALUE), ('DEPLOY_KEY', second['id'], 'FAKE-DEPLOY-0123456789'),
                 ('NPM_TOKEN', None, 'FAKE-NPM-0123456789')]
        self.binary.write_text(FAKE_WORKER.replace(
            "value=os.environ['GITHUB_TOKEN']", "value=os.environ['GITHUB_TOKEN']\n"
            "assert os.environ['GH_TOKEN'] == 'FAKE-DEPLOY-0123456789'\n"
            "assert os.environ['NPM_TOKEN'] == 'FAKE-NPM-0123456789'"))
        ids = []
        for index, (name, task, value) in enumerate(names):
            self.call('secret.ask', name=name, reason='Release', consumer='release worker', task=task)
            envelope = self.store.db.execute('SELECT id,card_outbox FROM envelopes WHERE name=?', (name,)).fetchone()
            ids.append(envelope['id'])
            await self.service.envelopes_once()
            self.assertEqual(self.store.accept(private(index * 2 + 2, value, date=self.telegram.date + 1)), 'envelope_fill')
        await self.update(task=job['id'], secrets='GITHUB_TOKEN,GH_TOKEN=DEPLOY_KEY,NPM_TOKEN')
        worker = self.call('workers.spawn', task=job['id'], provider='claude', prompt='release').data['worker']
        await self.service.workers_once()
        await asyncio.gather(*self.service.worker_tasks.values())
        finished = self.service.workers.get(worker)
        self.assertEqual(finished['status'], 'done')
        self.assertTrue(json.loads(finished['result'])['success'], finished['result'])
        self.assertEqual((self.work / 'worker-saw').read_text(), str(len(VALUE)))
        environment = json.loads((self.work / 'worker-environment').read_text())
        self.assertEqual([name for name in ('GITHUB_TOKEN', 'GH_TOKEN', 'NPM_TOKEN') if name in environment],
                         ['GITHUB_TOKEN', 'GH_TOKEN', 'NPM_TOKEN'])
        uses = self.store.db.execute("SELECT envelope,worker FROM envelope_events WHERE event='use' AND worker IS NOT NULL ORDER BY envelope").fetchall()
        self.assertEqual([tuple(row) for row in uses], [(envelope, worker) for envelope in ids])

    async def test_sharing_releases_a_worker_after_another_jobs_card_is_filled(self):
        other = self.store.task_create(TOPIC, 'Ask')
        job = self.store.task_create(TOPIC, 'Use', worktree=str(self.work), secrets=['GITHUB_TOKEN'])
        self.call('secret.ask', name='GITHUB_TOKEN', reason='Release', consumer='release worker', task=other['id'])
        worker = self.call('workers.spawn', task=job['id'], provider='claude', prompt='release').data['worker']
        await self.service.workers_once()
        await asyncio.gather(*self.service.worker_tasks.values())
        self.assertEqual(self.service.workers.get(worker)['status'], 'waiting_for_secret')
        self.assertFalse((self.work / 'worker-saw').exists())
        await self.service.envelopes_once()
        self.assertEqual(self.store.accept(private(2, VALUE, date=self.telegram.date + 1)), 'envelope_fill')
        await self.service.workers_once()
        await asyncio.gather(*self.service.worker_tasks.values())
        finished = self.service.workers.get(worker)
        self.assertEqual(finished['status'], 'done')
        self.assertTrue(json.loads(finished['result'])['success'], finished['result'])
        self.assertEqual((self.work / 'worker-saw').read_text(), str(len(VALUE)))
        self.assertEqual([tuple(row) for row in self.store.db.execute(
            "SELECT name,worker FROM envelope_events WHERE event='use' AND worker IS NOT NULL")], [('GITHUB_TOKEN', worker)])

    async def test_scan_finds_the_value_when_the_scrubber_is_disabled(self):
        class Unscrubbed:
            forms = []

            def __init__(self, secrets):
                pass

            def extend(self, forms):
                pass

            def __call__(self, text):
                return text

            def data(self, value):
                return value

        with patch('coordinator.providers.Scrubber', Unscrubbed):
            await self.drive()
        self.assertTrue(any(name.startswith('hosts/worker-') for name in self.holding(VALUE.encode())))

    async def test_scan_finds_the_value_when_the_vault_is_not_sealed(self):
        with patch('coordinator.vault.seal', lambda master, plaintext: plaintext), \
                patch('coordinator.vault.open', lambda master, blob: blob):
            await self.drive()
        self.assertIn('envelope/vault.enc', self.holding(VALUE.encode()))


class ReplyCompatibilityTests(EnvelopeFixture):
    def test_F6_private_replies_match_origin_main_except_expired(self):
        expected = {'bare_start': 'Open an Envelope card in your Torii channel and tap Fill privately.',
         'command': 'The value cannot start with /. I deleted your message. Send the value '
                    'again.',
         'handoff_closed': 'That sign-in is no longer waiting for a code, so I deleted your '
                           'message.',
         'internal': 'Something failed while I handled your message. I deleted it. Send the '
                     'value again.',
         'multi_line': 'Send the value as one line. I deleted your message. Send the value '
                       'again.',
         'no_armed': 'No envelope is waiting for a value, so I deleted your message. Tap Fill '
                     'privately on an Envelope card first.',
         'not_printable': 'The value has characters that cannot be printed. I deleted your '
                          'message. Send the value again.',
         'not_text': 'Send the value as one text message. I deleted your message.',
         'nothing_to_cancel': 'No envelope is waiting for a value.',
         'numeric': 'A value made only of digits is refused. I deleted your message. Send the '
                    'value again.',
         'too_long': 'The value must have at most 4096 characters. I deleted your message. '
                     'Send the value again.',
         'vault_failed': 'The vault write failed. Your message was deleted. Send the value '
                         'again.'}
        signin = {'bare_start': 'To finish the Claude sign-in, paste the code in your Torii group, in '
                       'the topic with the sign-in card.',
         'no_armed': 'I deleted your message. Paste the Claude sign-in code in your Torii '
                     'group, in the topic with the sign-in card, not here.',
         'nothing_to_cancel': 'Nothing is waiting here. To stop the Claude sign-in, tap Cancel '
                              'on its card in your Torii group.'}
        self.assertEqual(envelopes.REPLIES, expected)
        self.assertEqual(envelopes.SIGNIN_REPLIES, signin)

    def test_F6_cards_match_origin_main_for_unchanged_states(self):
        expected = {'armed': {'markup': {'inline_keyboard': [[{'text': 'Fill privately', 'url': 'https://t.me/torii_test_bot'}],
                                                  [{'callback_data': 'envelope:1:cancel', 'text': 'Cancel'}]]},
                   'state': 'armed',
                   'text': 'Envelope: GITHUB_TOKEN\n'
                           'Why: Push the release branch\n'
                           'Used by: release worker\n'
                           'Job: any job\n'
                           'Waiting for the value in the private chat. Tap Fill privately to open it.\n'
                           'Never paste it in this channel.'},
         'cancelled': {'markup': None, 'state': 'cancelled', 'text': 'Envelope: GITHUB_TOKEN\nCancelled.'},
         'handoff': {'markup': {'inline_keyboard': [[{'callback_data': 'envelope:1:cancel', 'text': 'Cancel'}]]},
                     'state': 'open',
                     'text': 'Sign in\n3. Paste it here as one message within 10 minutes. I delete it at once.'},
         'handoff_expired': {'markup': {'inline_keyboard': [[{'callback_data': 'signin:retry',
                                                              'text': 'Try again'}]]},
                             'state': 'expired',
                             'text': 'The sign-in link expired. No account was added.'},
         'revoked': {'markup': None,
                     'state': 'revoked',
                     'text': 'Envelope: GITHUB_TOKEN\nRevoked. The stored value was removed.'},
         'open': {'state': 'open',
                  'text': 'Envelope: GITHUB_TOKEN\n'
                          'Why: Push the release branch\n'
                          'Used by: release worker\n'
                          'Job: any job\n'
                          'Tap Fill privately within 10 minutes and send the value there in one message. Never paste '
                          'it in this channel.',
                  'markup': {'inline_keyboard': [[{'text': 'Fill privately', 'callback_data': 'envelope:1:fill'}],
                                                 [{'text': 'Cancel', 'callback_data': 'envelope:1:cancel'}]]}}}
        for action, card in expected.items():
            with self.subTest(action=action):
                fixture = EnvelopeFixture()
                fixture.setUp()
                try:
                    with fixture.store.db:
                        if action.startswith('handoff'):
                            envelopes.ask(fixture.store, TOPIC, envelopes.HANDOFF, 'Sign in', 'Claude')
                            envelope = fixture.envelope(envelopes.HANDOFF)
                            if action == 'handoff_expired':
                                envelopes.close(fixture.store, envelope, 'expired', 'expire', source='sweep')
                        else:
                            if action == 'open':
                                fixture.store.put('envelope_prompt_exit', {'envelope': None, 'acknowledged': False})
                            fixture.ask()
                            envelope = fixture.envelope()
                            if action in ('cancelled', 'revoked'):
                                envelopes.close(fixture.store, envelope, action,
                                                {'cancelled': 'cancel', 'revoked': 'revoke'}[action], source='button')
                    envelope = fixture.envelope(envelope['name'])
                    actual = fixture.outbox(envelope['card_outbox'])
                    self.assertEqual({'state': envelope['state'], 'text': actual['text'], 'markup': actual['markup']}, card)
                    if action == 'handoff_expired':
                        self.assertEqual(self.click_handoff_again(fixture, envelope), 'envelope_closed')
                finally:
                    fixture.tearDown()
                    fixture.doCleanups()

    def click_handoff_again(self, fixture, envelope):
        before = fixture.count('outbox')
        result = fixture.store.accept({'update_id': 1, 'callback_query': {
            'id': '1', 'from': {'id': OWNER}, 'data': 'envelope:%d:again' % envelope['id'],
            'message': {'message_id': 900, 'message_thread_id': 4,
                        'chat': {'id': GROUP, 'type': 'supergroup'}}}})
        self.assertEqual(fixture.count('outbox'), before)
        return result
