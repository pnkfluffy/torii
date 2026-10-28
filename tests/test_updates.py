import asyncio
from contextlib import redirect_stderr
import errno
import io
import json
import os
import sqlite3
from pathlib import Path
from types import SimpleNamespace
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, call, patch

from coordinator import cli_identity, control_api, updates
from coordinator.controls import handle_control
from coordinator.control_ui import accept_callback
from coordinator.onboarding import COMMAND_MENU, command_menu
from coordinator.session import CoordinatorSession, message_uuid
from coordinator.store import Store
from tests.support import until_exists
from tests.test_service import ServiceTestsSupport, TOPIC, owner_update


def tool(name='Claude Code', current='1.0.0', latest='2.0.0', prefix='/temporary/runtime'):
    return dict(label=name, package='@anthropic-ai/claude-code' if name == 'Claude Code' else '@openai/codex',
                binary='/temporary/bin/cli', prefix=prefix, current=current, latest=latest,
                outdated=updates.newer(current, latest))


def plan(subjects=(), tools=()):
    return dict(old='a' * 40, target='b' * 40 if subjects else 'a' * 40,
                subjects=list(subjects), tools=list(tools))


class CardTests(unittest.TestCase):
    def test_current(self):
        value = plan(tools=[tool(current='2.0.0'), tool('Codex', current='2.0.0')])
        self.assertEqual(updates.card(value), 'Torii and its npm tools are current.')
        self.assertFalse(updates.available(value))

    def test_torii_only_and_cap(self):
        text = updates.card(plan(['change %d' % i for i in range(14)]))
        self.assertEqual(text.splitlines()[0], 'Update Torii?')
        self.assertIn('• change 9\nand 4 more', text)
        self.assertNotIn('• change 10', text)
        self.assertTrue(text.endswith('Running chats and workers keep going.'))

    def test_subjects_with_html_escaping_still_fit_one_card(self):
        value = plan(['&' * 400] * 10, [tool(), tool('Codex')])
        self.assertLess(len(updates.markdown_to_html(updates.card(value))), 4000)

    def test_tools_only_and_combined(self):
        value = plan(tools=[tool(), tool('Codex')])
        self.assertEqual(updates.card(value), 'Update 2 tools?\nClaude Code 1.0.0 → 2.0.0\n'
                         'Codex 1.0.0 → 2.0.0\nRunning chats and workers keep going.')
        value['subjects'] = ['fix a bug']
        self.assertTrue(updates.card(value).startswith('Update Torii and 2 tools?'))
        value['tools'].pop()
        self.assertTrue(updates.card(value).startswith('Update Torii and 1 tool?'))

    def test_non_npm_version_is_shown_without_update(self):
        value = plan(tools=[tool(prefix=None)])
        self.assertFalse(updates.available(value))
        self.assertIn('Claude Code 1.0.0 → 2.0.0 · not installed through npm', updates.card(value))

    def test_no_downgrade_for_newer_installed_version(self):
        self.assertFalse(updates.newer('3.0.0', '2.0.0'))

    def test_semver_never_offers_prerelease_or_unknown_downgrades(self):
        for current, latest, expected in (
                ('3.0.0-beta.1', '2.0.0', False), ('2.0.0', '2.0.0-beta.1', False),
                ('2.0.0-beta.10', '2.0.0-beta.2', False), ('2.0.0-beta.2', '2.0.0-beta.10', True),
                ('2.0.0-beta.1', '2.0.0', True), ('2.0.0+build.1', '2.0.0+build.2', False),
                ('2.0.0-beta.01', '2.0.0-beta.2', False), ('2.0.0-foo_bar', '2.0.0', False),
                ('unknown', '2.0.0', False)):
            with self.subTest(current=current, latest=latest):
                self.assertEqual(updates.newer(current, latest), expected)

    def test_cli_only_outcome_names_only_changed_tools(self):
        record = dict(phase='updated', result='Updated.', attempted=[tool()], **plan(tools=[tool()]))
        self.assertEqual(updates.outcome(record), 'Updated.\nClaude Code 1.0.0 → 2.0.0')


class TemporaryStore(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.store = Store(self.root / 'state')
        self.local_npm_patch = patch.object(updates.watch, 'local_npm', return_value='/opt/homebrew/bin/npm', create=True)
        self.local_npm_patch.start()
        self.addCleanup(self.local_npm_patch.stop)
        with self.store.db:
            self.store.put('owner', 7)
            self.store.put('group', -10042)
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) "
                                  "VALUES ('-10042:4',-10042,4,'Test','',1)")
        self.service = SimpleNamespace(store=self.store, runner=SimpleNamespace(binaries={}), wake=lambda name: Mock())
        self.updater = updates.Updater(self.service)
        self.updater.root = self.root / 'checkout'

    def tearDown(self):
        self.store.close()
        self.temporary.cleanup()

    def command(self, owner=7):
        return self.store.accept({'update_id': 123 + owner, 'message': {'message_id': 42, 'message_thread_id': 4,
            'from': {'id': owner}, 'chat': {'id': -10042, 'type': 'supergroup'}, 'text': '/update'}})

    def deliver(self):
        with self.store.db:
            self.store.db.execute('UPDATE outbox SET delivered=1,telegram_message=1000')

    def record(self, phase, **values):
        record = dict(id='test', topic='-10042:4', message=1000, phase=phase, root=str(self.updater.root),
                      **plan(['change'], [tool()]))
        if phase in ('armed', 'merge', 'tools', 'restart', 'rolling_back', 'rollback_restart'):
            stamp = updates.watch.process_identity(os.getpid())
            record.update(old_pid=os.getpid(), old_process=stamp,
                          watchdog_pid=os.getpid(), watchdog_process=stamp)
        record.update(values)
        with self.store.db:
            self.store.put(updates.KEY, record)
        return record

    def once(self):
        return asyncio.run(self.updater.once())

    def latest_text(self):
        return self.store.db.execute('SELECT text FROM outbox ORDER BY id DESC LIMIT 1').fetchone()[0]


class OwnerTests(TemporaryStore):
    def test_owner_only_and_off_by_default(self):
        self.assertEqual(self.command(owner=8), 'unauthorized')
        self.assertIsNone(self.store.get(updates.KEY))
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 0)
        self.assertEqual(self.command(), 'control')
        self.assertIn('Updates from Telegram are off', self.latest_text())
        self.assertIn('ctl call service.updates enabled=true', self.latest_text())
        self.assertIsNone(self.store.get(updates.KEY))
        self.assertNotIn('update', [item['command'] for item in COMMAND_MENU])
        self.assertEqual(command_menu(self.store), COMMAND_MENU)

    def test_setting_and_command_menu(self):
        with self.store.db:
            result = control_api.call(self.store, 'service.updates', {'enabled': True})
        self.assertTrue(result.ok)
        self.assertIn('update', [item['command'] for item in command_menu(self.store)])
        self.command()
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'checking')

    def test_check_edits_one_card_and_current_has_no_buttons(self):
        self.store.put('telegram_updates_enabled', True)
        self.command()
        self.assertFalse(self.once())
        self.deliver()
        with patch.object(updates, 'inventory', return_value=plan()):
            self.assertTrue(self.once())
        row = self.store.db.execute('SELECT * FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
        self.assertEqual(row['edit_message'], 1000)
        self.assertIsNone(row['reply_markup'])
        self.assertEqual(self.latest_text(), 'Torii and its npm tools are current.')

    def offer(self):
        self.store.put('telegram_updates_enabled', True)
        with self.store.db:
            handle_control(self.store, '-10042:4', 42, '/update')
        self.deliver()
        with patch.object(updates, 'inventory', return_value=plan(['fix'], [tool()])):
            self.once()
        self.deliver()
        row = self.store.db.execute('SELECT * FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
        return json.loads(row['reply_markup'])['inline_keyboard'][0]

    def callback(self, data, owner=7):
        with self.store.db:
            return accept_callback(self.store, {'from': {'id': owner}, 'data': data,
                'message': {'message_id': 1000, 'message_thread_id': 4,
                            'chat': {'id': -10042, 'type': 'supergroup'}}})

    def test_confirm_and_duplicate_are_owner_protected(self):
        buttons = self.offer()
        self.assertEqual([button['text'] for button in buttons], ['Update', 'Cancel'])
        self.assertEqual(self.callback(buttons[0]['callback_data'], owner=8), 'unauthorized')
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'offer')
        self.assertEqual(self.callback(buttons[0]['callback_data']), 'control_callback')
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'queued')
        self.assertEqual(self.callback(buttons[0]['callback_data']), 'stale_callback')

    def test_cancel_and_setting_turned_off(self):
        for disable in (False, True):
            buttons = self.offer()
            if disable:
                self.store.put('telegram_updates_enabled', False)
            self.callback(buttons[0 if disable else 1]['callback_data'])
            self.assertEqual(self.store.get(updates.KEY)['phase'], 'cancelled')
            self.assertIn('off' if disable else 'cancelled', self.latest_text())


class ReplyTests(ServiceTestsSupport, unittest.IsolatedAsyncioTestCase):
    async def offer(self):
        self.store.put('telegram_updates_enabled', True)
        with patch.object(updates.uuid, 'uuid4', return_value=SimpleNamespace(hex='reply-offer')):
            self.assertEqual(await self.service.accept_update(owner_update(self.next_id(), '/update')), 'control')
        self.assertTrue(await self.service.deliver_once())
        with patch.object(updates, 'inventory', return_value=plan(['fix'], [tool()])), \
                patch('coordinator.control_ui.secrets.token_hex', return_value='0123456789abcdef'):
            self.assertTrue(await self.service.updater.once())
        self.assertTrue(await self.service.deliver_once())
        self.offered = self.store.get(updates.KEY)
        self.card_message = self.store.db.execute('SELECT telegram_message FROM outbox WHERE id=?',
                                                 (self.offered['outbox'],)).fetchone()[0]
        self.buttons = json.loads(self.telegram.sent[-1]['reply_markup'])['inline_keyboard'][0]
        return self.telegram.sent[-1]

    def next_id(self):
        self.update_id = getattr(self, 'update_id', 2000) + 1
        return self.update_id

    def reply(self, text='Yes!', **fields):
        update = owner_update(self.next_id(), text, reply=self.card_message)
        update['message'].update(fields)
        return update

    def tap(self):
        return dict(update_id=self.next_id(), callback_query=dict(
            id='tap', **{'from': {'id': 7}}, data=self.buttons[0]['callback_data'],
            message=dict(message_id=self.card_message, message_thread_id=4,
                         chat=dict(id=-10042, type='supergroup'))))

    def edit(self):
        row = self.store.db.execute('SELECT * FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
        return {key: row[key] for key in ('topic', 'kind', 'text', 'edit_message', 'reply_to', 'reply_markup')}

    async def assert_parent_receives(self, update):
        self.assertEqual(await self.service.accept_update(update), 'queued')
        await self.drain_feed()
        row = self.store.db.execute('SELECT * FROM messages WHERE telegram_message=?',
                                    (update['message']['message_id'],)).fetchone()
        self.assertIsNotNone(row)
        self.assertTrue(any(message_id == message_uuid(row['id']) for session in self.sessions
                            for message_id, _ in session.sent))

    async def test_yes_reply_reuses_button_action_state_and_card_edit(self):
        await self.offer()
        with patch.object(updates, 'accept_action', wraps=updates.accept_action) as action:
            self.assertEqual(await self.service.accept_update(self.tap()), 'control_callback')
            button_call = action.call_args
            button_state = self.store.get(updates.KEY)
            button_state.pop('outbox')
            button_edit = self.edit()
            self.store.put(updates.KEY, self.offered)
            reply = self.reply()
            self.assertEqual(await self.service.accept_update(reply), 'control_callback')
            self.assertEqual(action.call_args, button_call)
        reply_state = self.store.get(updates.KEY)
        reply_state.pop('outbox')
        self.assertEqual(reply_state, button_state)
        self.assertEqual(reply_state['phase'], 'queued')
        self.assertEqual(self.edit(), button_edit)
        self.assertEqual(button_edit['edit_message'], self.card_message)
        self.assertEqual(button_edit['reply_markup'], '{"inline_keyboard": []}')
        self.assertTrue(await self.service.deliver_once())
        self.assertTrue(await self.service.deliver_once())
        self.assertEqual(self.telegram.sent[-1]['text'], button_edit['text'])
        self.assertEqual(await self.service.accept_update(reply), 'duplicate')
        self.assertEqual(self.store.get('offset'), reply['update_id'] + 1)
        await self.drain_feed()
        self.assertEqual(self.sessions, [])

    async def test_reply_and_tap_or_two_replies_queue_one_update(self):
        for order in ('reply-tap', 'tap-reply', 'reply-reply'):
            with self.subTest(order=order):
                await self.offer()
                before = self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0]
                first = self.tap() if order == 'tap-reply' else self.reply()
                second = self.tap() if order == 'reply-tap' else self.reply()
                outcomes = await asyncio.gather(self.service.accept_update(first), self.service.accept_update(second))
                self.assertEqual(outcomes, ['control_callback', 'stale_callback' if order == 'reply-tap' else 'queued'])
                self.assertEqual(self.store.get(updates.KEY)['phase'], 'queued')
                self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], before + 1)
                if order != 'reply-tap':
                    await self.drain_feed()
                    row = self.store.db.execute('SELECT id FROM messages WHERE telegram_message=?',
                                                (second['message']['message_id'],)).fetchone()
                    self.assertTrue(any(message_id == message_uuid(row['id']) for session in self.sessions
                                        for message_id, _ in session.sent))
                self.assertTrue(await self.service.deliver_once())
                self.store.put(updates.KEY, dict(self.offered, phase='cancelled'))

    async def test_only_surrounding_punctuation_whitespace_and_case_are_ignored(self):
        for text in ('yes', 'Yes!', '\t ! \nYeS ? \t', '「YES！」', '...yes...', '  yes  '):
            with self.subTest(text=text):
                await self.offer()
                self.assertEqual(await self.service.accept_update(self.reply(text)), 'control_callback')
                self.assertEqual(self.store.get(updates.KEY)['phase'], 'queued')
                self.assertTrue(await self.service.deliver_once())
                self.store.put(updates.KEY, dict(self.offered, phase='cancelled'))

    async def test_nonmatching_and_forwarded_replies_reach_parent(self):
        await self.offer()
        with self.store.db:
            self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                                  ('-10042:9', -10042, 9, 'Other', str(self.root)))
        messages = [self.reply('yes but wait'), self.reply('y!e!s'), self.reply('yes ✅'),
                    self.reply(reply_to_message={}), self.reply(reply_to_message={'message_id': self.card_message + 99}),
                    self.reply(reply_to_message={'message_id': self.offered['message']}),
                    self.reply(message_thread_id=9),
                    self.reply(via_bot={'id': 99}), self.reply(business_connection_id='program'),
                    self.reply(forward_origin={'type': 'user', 'sender_user': {'id': 8}}),
                    self.reply(forward_origin={'type': 'user', 'sender_user': {'id': 7}})]
        for field, value in (('forward_from', {'id': 7}), ('forward_from_chat', {'id': -500}),
                             ('forward_sender_name', 'Someone'), ('forward_date', 1)):
            messages.append(self.reply(**{field: value}))
        for update in messages:
            with self.subTest(message=update['message']):
                await self.assert_parent_receives(update)
                self.assertEqual(self.store.get(updates.KEY), self.offered)
        self.assertEqual(await self.service.accept_update(self.reply()), 'control_callback')
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'queued')

    async def test_nonoffering_replies_reach_parent(self):
        await self.offer()
        for phase in ('checking', 'queued', 'arming', *updates.WATCHED, *updates.TERMINAL):
            with self.subTest(phase=phase):
                record = dict(self.offered, phase=phase)
                self.store.put(updates.KEY, record)
                await self.assert_parent_receives(self.reply())
                self.assertEqual(self.store.get(updates.KEY), record)
        self.store.put(updates.KEY, None)
        await self.assert_parent_receives(self.reply())
        self.assertIsNone(self.store.get(updates.KEY))
        self.store.put(updates.KEY, self.offered)
        with self.store.db:
            self.store.db.execute('UPDATE outbox SET delivered=0 WHERE id=?', (self.offered['outbox'],))
        await self.assert_parent_receives(self.reply())
        self.assertEqual(self.store.get(updates.KEY), self.offered)
        with self.store.db:
            self.store.db.execute('UPDATE outbox SET delivered=1 WHERE id=?', (self.offered['outbox'],))
        self.assertEqual(await self.service.accept_update(self.reply()), 'control_callback')
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'queued')

    async def test_yes_reply_to_replaced_menu_matches_button_refusal(self):
        for command in ('/help', '/health', '/accounts', '/projects'):
            for button in (0, 1):
                with self.subTest(command=command, button=('Update', 'Cancel')[button]):
                    await self.offer()
                    try:
                        self.assertEqual(await self.service.accept_update(owner_update(self.next_id(), command)),
                                         'control')
                        while await self.service.deliver_once():
                            pass
                        self.assertNotEqual(self.store.get('control_ui:' + TOPIC)['outbox'], self.offered['outbox'])
                        tap = self.tap()
                        tap['callback_query']['data'] = self.buttons[button]['callback_data']
                        self.assertEqual(await self.service.accept_update(tap), 'stale_callback')
                        self.assertEqual(self.store.get(updates.KEY), self.offered)
                        reply = self.reply('yes')
                        outcome = await self.service.accept_update(reply)
                        self.assertEqual(self.store.get(updates.KEY), self.offered)
                        self.assertEqual(outcome, 'control')
                        self.assertEqual(self.edit()['text'], 'This form has expired. Open the command again.')
                        self.assertIsNone(self.store.db.execute('SELECT id FROM messages WHERE telegram_message=?',
                                                               (reply['message']['message_id'],)).fetchone())
                    finally:
                        while await self.service.deliver_once():
                            pass
                        self.store.put(updates.KEY, dict(self.offered, phase='cancelled'))
        await self.drain_feed()
        self.assertEqual(self.sessions, [])

    async def test_photo_caption_yes_reply_reaches_parent_without_starting_update(self):
        await self.offer()
        photo = self.reply()
        photo['message'].pop('text')
        photo['message'].update(photo=[{'file_id': 'fixture-photo', 'width': 1, 'height': 1}], caption='yes')
        self.telegram.download_file = AsyncMock(return_value=b'\x89PNG\r\n\x1a\n')
        await self.assert_parent_receives(photo)
        self.assertEqual(self.store.get(updates.KEY), self.offered)
        row = self.store.db.execute('SELECT text FROM messages WHERE telegram_message=?',
                                    (photo['message']['message_id'],)).fetchone()
        self.assertEqual(row['text'].splitlines()[-1], 'yes')
        self.assertEqual(await self.service.accept_update(self.reply()), 'control_callback')
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'queued')

    async def test_owner_and_chat_authentication_still_apply(self):
        await self.offer()
        for fields, outcome in (({'from': {'id': 8}}, 'unauthorized'),
                                ({'from': {'id': 7, 'is_bot': True}}, 'ignored'),
                                ({'sender_chat': {'id': -10042}}, 'ignored'),
                                ({'chat': {'id': -500, 'type': 'supergroup'}}, 'unauthorized')):
            with self.subTest(fields=fields):
                self.assertEqual(await self.service.accept_update(self.reply(**fields)), outcome)
                self.assertEqual(self.store.get(updates.KEY), self.offered)
        await self.drain_feed()
        self.assertEqual(self.sessions, [])
        self.assertEqual(await self.service.accept_update(self.reply()), 'control_callback')
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'queued')

    async def test_disabled_updates_reply_uses_button_refusal(self):
        await self.offer()
        self.store.put('telegram_updates_enabled', False)
        self.assertEqual(await self.service.accept_update(self.tap()), 'control_callback')
        button_edit = self.edit()
        self.store.put(updates.KEY, self.offered)
        self.assertEqual(await self.service.accept_update(self.reply()), 'control_callback')
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'cancelled')
        self.assertEqual(self.edit(), button_edit)
        self.assertEqual(button_edit['text'], updates.OFF)
        await self.drain_feed()
        self.assertEqual(self.sessions, [])

    async def test_offer_text_and_markup_stay_byte_identical(self):
        card = await self.offer()
        self.assertEqual(card['text'].encode(), ('Update Torii and 1 tool?\nTorii aaaaaaa → bbbbbbb\n'
                         '• fix\nClaude Code 1.0.0 → 2.0.0\nRunning chats and workers keep going.').encode())
        self.assertEqual(card['reply_markup'].encode(), b'{"inline_keyboard": [[{"text": "Update", '
                         b'"callback_data": "torii:0123456789abcdef:0"}, {"text": "Cancel", '
                         b'"callback_data": "torii:0123456789abcdef:1"}]]}')
        self.assertEqual(await self.service.accept_update(self.reply()), 'control_callback')
        row = self.store.db.execute('SELECT text,reply_markup FROM outbox WHERE id=?',
                                    (self.offered['outbox'],)).fetchone()
        self.assertEqual((row['text'], row['reply_markup']), (card['text'], card['reply_markup']))

    async def test_failed_reply_transaction_rolls_back_and_retry_starts_once(self):
        await self.offer()
        reply = self.reply()
        before = self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0]
        original_put = self.store.put
        def fail_record(key, value):
            if key == updates.KEY:
                raise sqlite3.OperationalError('test write failure')
            return original_put(key, value)
        with patch.object(self.store, 'put', side_effect=fail_record):
            with self.assertRaises(sqlite3.OperationalError):
                await self.service.accept_update(reply)
        self.assertEqual(self.store.get(updates.KEY), self.offered)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], before)
        self.assertIsNone(self.store.db.execute('SELECT 1 FROM updates WHERE id=?', (reply['update_id'],)).fetchone())
        self.assertEqual(await self.service.accept_update(reply), 'control_callback')
        self.assertEqual(await self.service.accept_update(reply), 'duplicate')
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'queued')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], before + 1)


class CheckoutTests(unittest.TestCase):
    def setUp(self):
        self.stores = []
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.remote = self.root / 'remote.git'
        self.source = self.root / 'source'
        self.live = self.root / 'checkout'
        self.git(self.root, 'init', '--bare', '--initial-branch=main', str(self.remote))
        self.git(self.root, 'clone', str(self.remote), str(self.source))
        self.git(self.source, 'config', 'user.name', 'Test')
        self.git(self.source, 'config', 'user.email', 'test@example.com')
        (self.source / 'release.py').write_text('value = 1\n')
        self.commit('old')
        self.old = self.git(self.source, 'rev-parse', 'HEAD')
        self.git(self.source, 'push', 'origin', 'main')
        self.git(self.root, 'clone', str(self.remote), str(self.live))
        self.git(self.live, 'config', 'user.name', 'Test')
        self.git(self.live, 'config', 'user.email', 'test@example.com')
        (self.source / 'release.py').write_text('value = 2\n')
        self.commit('new change')
        self.target = self.git(self.source, 'rev-parse', 'HEAD')
        self.git(self.source, 'push', 'origin', 'main')
        self.git(self.live, 'fetch', 'origin', 'main')

    def tearDown(self):
        for store in self.stores:
            store.close()
        self.temporary.cleanup()

    def git(self, root, *args):
        return subprocess.run(['git', '-C', str(root), *args], check=True,
                              capture_output=True, text=True).stdout.strip()

    def commit(self, subject):
        self.git(self.source, 'add', '.')
        self.git(self.source, 'commit', '-m', subject)

    def merge_history(self):
        self.git(self.source, 'reset', '--hard', self.old)
        self.git(self.source, 'push', '--force', 'origin', 'main')
        self.git(self.source, 'checkout', '-b', 'pr-2')
        for index in range(2):
            (self.source / ('pr-2-%d.txt' % index)).write_text('PR 2 commit %d\n' % index)
            self.commit('PR 2 commit %d' % index)
        self.git(self.source, 'checkout', 'main')
        self.git(self.source, 'checkout', '-b', 'pr-1')
        for index in range(2):
            (self.source / ('pr-1-%d.txt' % index)).write_text('PR 1 commit %d\n' % index)
            self.commit('PR 1 commit %d' % index)
        self.git(self.source, 'checkout', 'main')
        self.git(self.source, 'merge', '--no-ff', 'pr-1', '-m', 'Merge pull request #1')
        self.git(self.source, 'checkout', 'pr-2')
        self.git(self.source, 'merge', '--no-ff', 'main', '-m', 'Merge main into PR #2')
        self.git(self.source, 'checkout', 'main')
        self.git(self.source, 'merge', '--no-ff', 'pr-2', '-m', 'Merge pull request #2')
        target = self.git(self.source, 'rev-parse', 'HEAD')
        self.git(self.source, 'push', '--force', 'origin', 'main')
        self.git(self.live, 'fetch', 'origin', 'main')
        return target

    def squash_history(self):
        self.git(self.source, 'reset', '--hard', self.old)
        self.git(self.source, 'push', '--force', 'origin', 'main')
        for index in range(2):
            (self.source / ('squash-%d.txt' % index)).write_text('Squash PR %d\n' % index)
            self.commit('Squash PR #%d' % (index + 1))
        target = self.git(self.source, 'rev-parse', 'HEAD')
        self.git(self.source, 'push', '--force', 'origin', 'main')
        self.git(self.live, 'fetch', 'origin', 'main')
        return target

    def merge_queue_history(self):
        self.git(self.source, 'reset', '--hard', self.old)
        self.git(self.source, 'push', '--force', 'origin', 'main')
        self.git(self.source, 'checkout', '-b', 'pr-101')
        (self.source / 'pr-101.txt').write_text('PR 101 change\n')
        self.commit('PR 101 change')
        self.git(self.source, 'checkout', 'main')
        self.git(self.source, 'merge', '--no-ff', 'pr-101', '-m',
                 'Merge pull request #101 from example/task-101', '-m',
                 'fix(updates): restore merged PR titles in update card')
        (self.source / 'plain.txt').write_text('Plain commit\n')
        self.commit('docs: clarify local setup')
        self.git(self.source, 'checkout', '-b', 'pr-102')
        (self.source / 'pr-102.txt').write_text('PR 102 change\n')
        self.commit('PR 102 change')
        self.git(self.source, 'checkout', 'main')
        self.git(self.source, 'merge', '--no-ff', 'pr-102', '-m',
                 'Merge pull request #102 from example/task-102')
        self.git(self.source, 'checkout', '-b', 'pr-103')
        (self.source / 'pr-103.txt').write_text('PR 103 change\n')
        self.commit('PR 103 change')
        self.git(self.source, 'checkout', 'main')
        self.git(self.source, 'merge', '--no-ff', 'pr-103', '-m',
                 'Merge pull request #103 from example/task-103', '-m',
                 '\n\nfeat(service): preserve coordinator update ordering\n\n'
                 'Additional details do not appear.')
        target = self.git(self.source, 'rev-parse', 'HEAD')
        self.git(self.source, 'push', '--force', 'origin', 'main')
        self.git(self.live, 'fetch', 'origin', 'main')
        return target

    def test_local_remote_inventory_and_fast_forward(self):
        with patch.object(updates, 'installation', return_value=(None, None, None)):
            result = updates.inventory(self.live, {})
        self.assertEqual(result, dict(old=self.old, target=self.target, subjects=['new change'], tools=[]))
        self.assertEqual(updates.checkout(self.live, self.target), self.old)
        updates.git(self.live, 'merge', '--ff-only', self.target)
        self.assertEqual(self.git(self.live, 'rev-parse', 'HEAD'), self.target)

    def test_unmanaged_absolute_node_shebang_does_not_require_npm(self):
        node = Path(updates.watch.shutil.which('node') or '/opt/homebrew/bin/node').resolve()
        if not node.is_file():
            self.skipTest('Node is not installed')
        binary = self.root / 'unmanaged-cli.js'
        binary.write_text('#!' + str(node) + '\nconsole.log("1.2.3");\n')
        binary.chmod(0o755)
        commands = self.root / 'commands'
        commands.mkdir()
        (commands / 'git').symlink_to(updates.watch.shutil.which('git'))
        known_npm = {'/opt/homebrew/bin/npm', '/opt/homebrew/opt/node@22/bin/npm',
                     '/usr/local/bin/npm', '/usr/bin/npm'}
        real_access = os.access
        def access(path, mode, *args, **kwargs):
            return False if str(path) in known_npm else real_access(path, mode, *args, **kwargs)
        with patch.dict(os.environ, PATH=str(commands)), patch.object(os, 'access', side_effect=access):
            self.assertIsNone(updates.watch.shutil.which('node'))
            self.assertEqual(updates.watch.command([str(binary), '--version']), '1.2.3')
            with self.assertRaisesRegex(RuntimeError, 'local npm installation'):
                updates.watch.local_npm()
            offer = updates.inventory(self.live, {'claude': '/not-installed/claude', 'codex': str(binary)})
        self.assertTrue(updates.available(offer))
        self.assertEqual(offer['target'], self.target)
        self.assertEqual(offer['tools'][0]['current'], '1.2.3')
        self.assertIsNone(offer['tools'][0]['prefix'])

    def test_watchdog_still_rolls_back_when_log_is_full(self):
        self.git(self.live, 'merge', '--ff-only', self.target)
        child = subprocess.Popen([sys.executable, '-c', 'pass'])
        child.wait(timeout=5)
        record = dict(id='test', phase='tools', root=str(self.live), old=self.old, target=self.target,
                      tools=[], attempted=[], old_pid=child.pid, old_process='exited process', health=None)
        class FullLog(io.StringIO):
            def write(self, text):
                raise OSError(errno.ENOSPC, 'No space left on device')
        with sqlite3.connect(self.root / 'state.sqlite') as db, redirect_stderr(FullLog()):
            db.execute('CREATE TABLE settings(key TEXT PRIMARY KEY, value TEXT)')
            db.execute('INSERT INTO settings VALUES (?, ?)', (updates.KEY, json.dumps(record)))
            updates.watch.watchdog(db, 'test', service_control=[sys.executable, '-c', 'raise SystemExit(7)'],
                                   restart_timeout=0.01)
            self.assertEqual(updates.watch.read(db)['phase'], 'recovering')
        self.assertEqual(self.git(self.live, 'rev-parse', 'HEAD'), self.old)

    def test_first_parent_merge_count_is_two(self):
        self.merge_history()
        with patch.object(updates, 'installation', return_value=(None, None, None)):
            result = updates.inventory(self.live, {})
        self.assertEqual(len(result['subjects']), 2)

    def test_first_parent_merge_card_lists_only_merge_subjects(self):
        self.merge_history()
        with patch.object(updates, 'installation', return_value=(None, None, None)):
            result = updates.inventory(self.live, {})
        listed = [line[2:] for line in updates.card(result).splitlines() if line.startswith('• ')]
        self.assertEqual(listed, ['Merge pull request #2', 'Merge pull request #1'])

    def test_merge_queue_card_lists_titles_and_keeps_other_subjects(self):
        self.merge_queue_history()
        with patch.object(updates, 'installation', return_value=(None, None, None)):
            result = updates.inventory(self.live, {})
        listed = [line[2:] for line in updates.card(result).splitlines() if line.startswith('• ')]
        self.assertEqual(listed, [
            'feat(service): preserve coordinator update ordering (#103)',
            'Merge pull request #102 from example/task-102',
            'docs: clarify local setup',
            'fix(updates): restore merged PR titles in update card (#101)',
        ])

    def test_squash_history_inventory_is_unchanged(self):
        target = self.squash_history()
        with patch.object(updates, 'installation', return_value=(None, None, None)):
            result = updates.inventory(self.live, {})
        self.assertEqual(result, dict(old=self.old, target=target,
                                      subjects=['Squash PR #2', 'Squash PR #1'], tools=[]))

    def test_checkout_refuses_local_second_parent_commit_ahead_of_merge_main(self):
        target = self.merge_history()
        self.git(self.live, 'merge', '--ff-only', target)
        self.git(self.live, 'checkout', '-b', 'local-work')
        (self.live / 'local.txt').write_text('local work\n')
        self.git(self.live, 'add', 'local.txt')
        self.git(self.live, 'commit', '-m', 'Local work')
        self.git(self.live, 'checkout', 'main')
        self.git(self.live, 'merge', '--no-ff', 'local-work', '-m', 'Local merge')
        with self.assertRaisesRegex(RuntimeError, 'ahead of origin/main'):
            updates.checkout(self.live, target)

    def test_checkout_refuses_diverged_commit_with_merge_main(self):
        target = self.merge_history()
        (self.live / 'local.txt').write_text('local work\n')
        self.git(self.live, 'add', 'local.txt')
        self.git(self.live, 'commit', '-m', 'Local work')
        with self.assertRaisesRegex(RuntimeError, 'diverged from origin/main'):
            updates.checkout(self.live, target)

    def test_non_npm_tools_show_versions_without_invoking_npm(self):
        with patch.object(updates, 'installation', return_value=('/temporary/cli', None, None)):
            with patch.object(updates.watch, 'version', return_value='1.0.0'):
                with patch.object(updates.watch, 'command', wraps=updates.watch.command) as command:
                    result = updates.inventory(self.live, {})
        self.assertFalse(any(call.args[0][0] == 'npm' for call in command.call_args_list))
        self.assertIn('Claude Code 1.0.0 · not installed through npm.', updates.card(result))

    def test_untracked_files_do_not_block_update_or_rollback(self):
        (self.live / 'untracked').write_text('preserve me')
        self.assertEqual(updates.checkout(self.live, self.target), self.old)
        self.git(self.live, 'merge', '--ff-only', self.target)
        updates.watch.rollback_code(dict(root=str(self.live), old=self.old, target=self.target))
        self.assertEqual((self.live / 'untracked').read_text(), 'preserve me')
        self.assertEqual(self.git(self.live, 'rev-parse', 'HEAD'), self.old)

    def test_detached_and_untracked_branches_are_refused(self):
        self.git(self.live, 'checkout', '--detach', self.old)
        with self.assertRaisesRegex(RuntimeError, 'tracked main branch'):
            updates.checkout(self.live, self.target)
        self.git(self.live, 'checkout', '-b', 'pinned')
        with self.assertRaisesRegex(RuntimeError, 'tracked main branch'):
            updates.checkout(self.live, self.target)
        self.assertEqual(self.git(self.live, 'rev-parse', 'HEAD'), self.old)

    def test_dirty_ahead_and_diverged_change_nothing(self):
        for kind in ('dirty', 'ahead', 'diverged'):
            with self.subTest(kind=kind):
                if kind == 'dirty':
                    (self.live / 'release.py').write_text('local work')
                    target = self.target
                else:
                    if kind == 'ahead':
                        self.git(self.live, 'merge', '--ff-only', self.target)
                        target = self.target
                    else:
                        target = self.old
                    (self.live / 'local').write_text(kind)
                    self.git(self.live, 'add', 'local')
                    self.git(self.live, 'commit', '-m', 'local work')
                    if kind == 'diverged':
                        (self.source / 'other').write_text('remote branch')
                        self.commit('another remote commit')
                        self.git(self.source, 'push', 'origin', 'main')
                        self.git(self.live, 'fetch', 'origin', 'main')
                        target = self.git(self.source, 'rev-parse', 'HEAD')
                before = self.git(self.live, 'rev-parse', 'HEAD')
                with self.assertRaisesRegex(RuntimeError, kind):
                    updates.checkout(self.live, target)
                self.assertEqual(self.git(self.live, 'rev-parse', 'HEAD'), before)
                if kind == 'dirty':
                    self.assertEqual((self.live / 'release.py').read_text(), 'local work')
                    self.git(self.live, 'restore', 'release.py')

    def unverified_target(self):
        (self.source / 'scripts').mkdir()
        (self.source / 'scripts/check-no-comments.py').write_text(
            (updates.ROOT / 'scripts/check-no-comments.py').read_text())
        (self.source / 'coordinator').mkdir()
        (self.source / 'coordinator/__init__.py').write_text('')
        (self.source / 'coordinator/rejected.py').write_text('# rejected comment\nvalue =\n')
        (self.source / 'tests').mkdir()
        (self.source / 'tests/__init__.py').write_text('')
        (self.source / 'tests/test_release.py').write_text('import unittest\n'
            'class ReleaseTest(unittest.TestCase):\n'
            '    def test_release(self):\n        self.fail("candidate suite must not run")\n')
        self.commit('unverified candidate')
        target = self.git(self.source, 'rev-parse', 'HEAD')
        self.git(self.source, 'push', 'origin', 'main')
        return target

    def preparation_record(self, phase='checks', old=None, target=None, state=None, tools=()):
        store = Store(state or self.root / 'state')
        self.stores.append(store)
        with store.db:
            store.put('owner', 7)
            store.put('group', -10042)
            store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) "
                             "VALUES ('-10042:4',-10042,4,'Test','',1)")
            store.put(updates.KEY, dict(id='test', topic='-10042:4', message=1000, phase=phase,
                                       root=str(self.live), old=old or self.old, target=target or self.target,
                                       subjects=['unverified candidate'], tools=list(tools)))
        service = SimpleNamespace(store=store, runner=SimpleNamespace(binaries={}), wake=lambda name: Mock())
        with patch.object(updates, 'ROOT', self.live):
            updater = updates.Updater(service)
        return store, updater

    def retry_preparation(self, store, **values):
        record = store.get(updates.KEY)
        record.update(phase='checks', **values)
        for key in ('reported', 'outbox', 'result', 'restored'):
            record.pop(key, None)
        with store.db:
            store.put(updates.KEY, record)

    def assert_prepares_without_verification(self, store, updater):
        record = store.get(updates.KEY)
        before = self.git(self.live, 'worktree', 'list', '--porcelain')
        with patch.object(updates.subprocess, 'run', wraps=subprocess.run) as run:
            if record['phase'] == 'queued':
                self.assertTrue(asyncio.run(updater.once()))
                with store.db:
                    store.db.execute('UPDATE outbox SET delivered=1,telegram_message=1000')
            self.assertTrue(asyncio.run(updater.once()))
        commands = [call.args[0] for call in run.call_args_list]
        self.assertEqual(store.get(updates.KEY)['phase'], 'arming', 'Commands run: ' + repr(commands))
        self.assertIn(['git', 'fetch', 'origin', 'main'], commands)
        for command in commands:
            self.assertEqual(command[0], 'git', commands)
            self.assertNotIn('worktree', command, commands)
        self.assertEqual(self.git(self.live, 'worktree', 'list', '--porcelain'), before)
        self.assertEqual(self.git(self.live, 'rev-parse', 'HEAD'), record['old'])
        self.assertEqual(store.db.execute('SELECT text FROM outbox ORDER BY id DESC LIMIT 1').fetchone()[0],
                         'Preparing the update…')

    def assert_preparation_refused(self, store, updater, message):
        head = self.git(self.live, 'rev-parse', 'HEAD')
        status = self.git(self.live, 'status', '--porcelain')
        worktrees = self.git(self.live, 'worktree', 'list', '--porcelain')
        self.assertTrue(asyncio.run(updater.once()))
        record = store.get(updates.KEY)
        self.assertEqual(record['phase'], 'failed')
        self.assertEqual(record['result'], 'Update failed: ' + message)
        self.assertEqual(store.db.execute('SELECT text FROM outbox ORDER BY id DESC LIMIT 1').fetchone()[0].strip(),
                         'Update failed: ' + message)
        self.assertEqual(self.git(self.live, 'rev-parse', 'HEAD'), head)
        self.assertEqual(self.git(self.live, 'status', '--porcelain'), status)
        self.assertEqual(self.git(self.live, 'worktree', 'list', '--porcelain'), worktrees)
        self.assertFalse((store.directory / 'update-checks').exists())

    def test_update_reaches_arming_without_running_candidate_checks(self):
        self.target = self.unverified_target()
        store, updater = self.preparation_record(phase='queued')
        self.assert_prepares_without_verification(store, updater)
        self.assertFalse((store.directory / 'update-checks').exists())
        self.assertEqual([row[0] for row in store.db.execute('SELECT text FROM outbox ORDER BY id')],
                         ['Fetching Torii…', 'Preparing the update…'])
        self.assertEqual((self.live / 'release.py').read_text(), 'value = 1\n')

    def test_saved_checks_phase_resumes_without_verification(self):
        self.target = self.unverified_target()
        for existing in (False, True):
            with self.subTest(existing_checks=existing):
                state = self.root / ('previous-checks' if existing else 'fresh-checks')
                store, updater = self.preparation_record(state=state)
                log = state / 'update-checks/test/checks.log'
                candidate = log.parent / 'checkout/rejected.py'
                worktrees = self.git(self.live, 'worktree', 'list', '--porcelain')
                if existing:
                    candidate.parent.parent.mkdir(parents=True)
                    self.git(self.live, 'worktree', 'add', '--detach', str(candidate.parent), self.target)
                    candidate.write_text('previous candidate remains untouched\n')
                    log.write_text('previous offline check output\n')
                    record = store.get(updates.KEY)
                    record['checks_log'] = str(log)
                    with store.db:
                        store.put(updates.KEY, record)
                store.close()
                resumed = Store(state)
                self.stores.append(resumed)
                updater.service.store = resumed
                with patch.object(updates, 'ROOT', self.live):
                    updater = updates.Updater(updater.service)
                self.assert_prepares_without_verification(resumed, updater)
                if existing:
                    def tick():
                        with resumed.db:
                            resumed.db.execute('UPDATE outbox SET delivered=1,telegram_message=1000')
                        return asyncio.run(updater.once())

                    with patch.object(updates, 'start_watchdog', return_value=os.getpid()) as spawn:
                        tick()
                    self.assertTrue(spawn.called)
                    with resumed.db:
                        record = resumed.get(updates.KEY)
                        record['watchdog_armed'] = True
                        resumed.put(updates.KEY, record)
                    tick()
                    tick()
                    tick()
                    self.assertEqual(resumed.get(updates.KEY)['phase'], 'restart')
                    record = resumed.get(updates.KEY)
                    with resumed.db:
                        updates.watch.write(resumed.db, record['id'], restart_started=updates.time.time(),
                                            restart_from=dict(pid=os.getpid(), process=record['old_process']))
                    with patch.object(updates, 'ROOT', self.live):
                        new_service = updates.Updater(updater.service)
                    with patch.object(updates.os, 'getpid', return_value=222):
                        with patch.object(updates.watch, 'process_identity', return_value='new'):
                            new_service.healthy('new')
                            updates.watch.watchdog(resumed.db, record['id'], service_control=['/usr/bin/true'],
                                                   restart_timeout=1)
                    updater = new_service
                    tick()
                    self.assertEqual(resumed.get(updates.KEY)['phase'], 'updated')
                    self.assertEqual(log.read_text(), 'previous offline check output\n')
                    self.assertEqual(candidate.read_text(), 'previous candidate remains untouched\n')
                    self.assertEqual(resumed.get(updates.KEY)['checks_log'], str(log))
                    registered_worktrees = self.git(self.live, 'worktree', 'list', '--porcelain')
                    self.assertEqual(registered_worktrees.count('worktree '), worktrees.count('worktree ') + 1)
                    self.assertIn('worktree ' + str(candidate.parent), registered_worktrees)
                    result = updates.watch.rollback_code(dict(root=str(self.live), old=self.old, target=self.target))
                    self.assertEqual(result, 'Torii restored to ' + self.old[:7] + '.')
                    self.assertEqual(self.git(self.live, 'rev-parse', 'HEAD'), self.old)
                    self.assertIn(str(candidate.parent), self.git(self.live, 'worktree', 'list', '--porcelain'))
                    self.assertEqual(log.read_text(), 'previous offline check output\n')
                    self.assertEqual(candidate.read_text(), 'previous candidate remains untouched\n')
                else:
                    self.assertFalse((state / 'update-checks').exists())

    def test_prepare_refuses_dirty_checkout_without_merging_or_installing_outdated_cli(self):
        self.target = self.unverified_target()
        store, updater = self.preparation_record(tools=[tool()])
        (self.live / 'release.py').write_text('local work\n')
        with patch.object(updates.watch, 'install') as install:
            self.assert_preparation_refused(store, updater, 'Checkout is dirty. Nothing changed.')
        self.assertEqual(self.git(self.live, 'rev-parse', 'HEAD'), self.old)
        install.assert_not_called()

    def test_prepare_refuses_moved_remote_then_updates(self):
        self.target = self.unverified_target()
        store, updater = self.preparation_record()
        (self.source / 'next').write_text('another upstream change\n')
        self.commit('remote moved')
        self.git(self.source, 'push', 'origin', 'main')
        self.assert_preparation_refused(store, updater,
            'origin/main changed since this card. Run /update again. Nothing changed.')
        self.retry_preparation(store, target=self.git(self.source, 'rev-parse', 'HEAD'))
        self.assert_prepares_without_verification(store, updater)
        self.assertFalse((store.directory / 'update-checks').exists())

    def test_prepare_refuses_dirty_checkout_then_updates(self):
        self.target = self.unverified_target()
        store, updater = self.preparation_record()
        (self.live / 'release.py').write_text('local work\n')
        self.assert_preparation_refused(store, updater, 'Checkout is dirty. Nothing changed.')
        self.git(self.live, 'restore', 'release.py')
        self.retry_preparation(store)
        self.assert_prepares_without_verification(store, updater)
        self.assertFalse((store.directory / 'update-checks').exists())

    def test_prepare_refuses_checkout_not_on_tracked_main_then_updates(self):
        self.target = self.unverified_target()
        store, updater = self.preparation_record()
        self.git(self.live, 'checkout', '--detach', self.old)
        self.assert_preparation_refused(store, updater,
            'Checkout must be on its tracked main branch. Nothing changed.')
        self.git(self.live, 'checkout', 'main')
        self.git(self.live, 'branch', '--unset-upstream', 'main')
        self.retry_preparation(store)
        self.assert_preparation_refused(store, updater,
            'Checkout must be on its tracked main branch. Nothing changed.')
        self.git(self.live, 'branch', '--set-upstream-to=origin/main', 'main')
        self.git(self.live, 'checkout', '-b', 'local', self.old)
        self.git(self.live, 'branch', '--set-upstream-to=origin/main', 'local')
        self.retry_preparation(store)
        self.assert_preparation_refused(store, updater,
            'Checkout must be on its tracked main branch. Nothing changed.')
        self.git(self.live, 'checkout', 'main')
        self.retry_preparation(store)
        self.assert_prepares_without_verification(store, updater)
        self.assertFalse((store.directory / 'update-checks').exists())

    def test_prepare_refuses_ahead_checkout_then_updates(self):
        self.target = self.unverified_target()
        store, updater = self.preparation_record()
        self.git(self.live, 'fetch', 'origin', 'main')
        self.git(self.live, 'merge', '--ff-only', self.target)
        (self.live / 'local').write_text('local work\n')
        self.git(self.live, 'add', 'local')
        self.git(self.live, 'commit', '-m', 'ahead')
        self.assert_preparation_refused(store, updater, 'Checkout is ahead of origin/main. Nothing changed.')
        self.git(self.live, 'reset', '--keep', self.old)
        self.retry_preparation(store)
        self.assert_prepares_without_verification(store, updater)
        self.assertFalse((store.directory / 'update-checks').exists())

    def test_prepare_refuses_diverged_checkout_then_updates(self):
        self.target = self.unverified_target()
        store, updater = self.preparation_record()
        (self.live / 'local').write_text('local work\n')
        self.git(self.live, 'add', 'local')
        self.git(self.live, 'commit', '-m', 'diverged')
        self.assert_preparation_refused(store, updater, 'Checkout has diverged from origin/main. Nothing changed.')
        self.git(self.live, 'reset', '--keep', self.old)
        self.retry_preparation(store)
        self.assert_prepares_without_verification(store, updater)
        self.assertFalse((store.directory / 'update-checks').exists())

    def test_prepare_refuses_changed_checkout_then_updates(self):
        intermediate = self.target
        self.target = self.unverified_target()
        store, updater = self.preparation_record()
        self.git(self.live, 'merge', '--ff-only', intermediate)
        self.assert_preparation_refused(store, updater, 'Checkout changed since this card. Nothing changed.')
        self.git(self.live, 'reset', '--keep', self.old)
        self.retry_preparation(store)
        self.assert_prepares_without_verification(store, updater)
        self.assertFalse((store.directory / 'update-checks').exists())

    def test_real_watchdog_rolls_back_with_injected_service_control(self):
        self.git(self.live, 'merge', '--ff-only', self.target)
        store = Store(self.root / 'watch-state')
        script = self.root / 'fake-service.py'
        marker = self.root / 'restart'
        script.write_text('''import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
if sys.argv[1] == 'control':
    Path(sys.argv[2]).write_text('restart')
else:
    deadline = time.monotonic() + 10
    while not Path(sys.argv[3]).exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    with sqlite3.connect(sys.argv[2], timeout=5) as db:
        row = db.execute("SELECT value FROM settings WHERE key='telegram_update_v1'").fetchone()
        record = json.loads(row[0])
        record['health'] = dict(pid=os.getpid(), process=subprocess.check_output(
            ['ps', '-p', str(os.getpid()), '-o', 'lstart='], text=True,
            env=dict(os.environ, LC_ALL='C', LANG='C', TZ='UTC')).strip(),
            commit=subprocess.check_output(['git', '-C', record['root'], 'rev-parse', 'HEAD'], text=True).strip(),
            restart_started=record.get('restart_started'))
        db.execute("UPDATE settings SET value=? WHERE key='telegram_update_v1'", (json.dumps(record),))
    time.sleep(10)
''')
        old_service = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(15)'])
        new_service = subprocess.Popen([sys.executable, str(script), 'service',
                                       str(store.directory / 'state.sqlite'), str(marker)])
        watchdog = None
        try:
            with store.db:
                store.put(updates.KEY, dict(id='real', phase='restart', root=str(self.live), old=self.old,
                    target=self.target, tools=[], attempted=[], old_pid=old_service.pid,
                    old_process=updates.watch.process_identity(old_service.pid),
                    restart_started=updates.time.time() - 51, restart_from=dict(pid=old_service.pid,
                    process=updates.watch.process_identity(old_service.pid)), health=None))
            watchdog = subprocess.Popen([sys.executable, str(updates.ROOT / 'scripts/update-watchdog.py'),
                str(store.directory / 'state.sqlite'), 'real', '--restart-timeout', '0.2',
                '--service-control', json.dumps([sys.executable, str(script), 'control', str(marker)])],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            output = watchdog.communicate(timeout=3)
            self.assertEqual(watchdog.returncode, 0, output)
            record = store.get(updates.KEY)
            self.assertEqual(record['phase'], 'rolled_back')
            self.assertEqual(record['health']['pid'], new_service.pid)
            self.assertEqual(self.git(self.live, 'rev-parse', 'HEAD'), self.old)
        finally:
            for process in (watchdog, new_service, old_service):
                if process is not None:
                    if process.poll() is None:
                        process.terminate()
                    process.communicate(timeout=5)
            store.close()

    def test_full_update_fetches_merges_restarts_and_edits_original_card(self):
        target = self.target
        store = Store(self.root / 'state')
        try:
            with store.db:
                store.put('owner', 7)
                store.put('group', -10042)
                store.put('telegram_updates_enabled', True)
                store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) "
                                 "VALUES ('-10042:4',-10042,4,'Test','',1)")
                updates.request(store, '-10042:4', 42)
            service = SimpleNamespace(store=store, runner=SimpleNamespace(binaries={}), wake=lambda name: Mock())
            updater = updates.Updater(service)
            updater.root = self.live
            def tick():
                with store.db:
                    store.db.execute('UPDATE outbox SET delivered=1,telegram_message=1000')
                return asyncio.run(updater.once())
            with patch.object(updates, 'installation', return_value=(None, None, None)):
                tick()
            record = store.get(updates.KEY)
            with store.db:
                updates.accept_action(store, '-10042:4', 1000, dict(id=record['id'], action='confirm'))
            tick()
            tick()
            self.assertEqual(self.git(self.live, 'rev-parse', 'HEAD'), self.old)
            with patch.object(updates, 'start_watchdog', return_value=os.getpid()):
                tick()
            with store.db:
                record = store.get(updates.KEY)
                record['watchdog_armed'] = True
                store.put(updates.KEY, record)
            tick()
            tick()
            tick()
            self.assertEqual(self.git(self.live, 'rev-parse', 'HEAD'), target)
            self.assertEqual(store.get('restart_requested_v2')['reason'], 'Telegram update')
            with store.db:
                stamp = record['old_process']
                updates.watch.write(store.db, record['id'], restart_started=updates.time.time(),
                                    restart_from=dict(pid=os.getpid(), process=stamp))
            with patch.object(updates, 'ROOT', self.live):
                new_service = updates.Updater(service)
            with patch.object(updates.os, 'getpid', return_value=222):
                with patch.object(updates.watch, 'process_identity', return_value='new'):
                    new_service.healthy('new')
                    updates.watch.watchdog(store.db, record['id'])
            updater = new_service
            tick()
            row = store.db.execute('SELECT * FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
            self.assertEqual(row['edit_message'], 1000)
            self.assertEqual(row['text'], 'Updated.\nTorii ' + self.old[:7] + ' → ' + target[:7])
        finally:
            store.close()

    def test_rollback_reset_keep_preserves_old_commit(self):
        self.git(self.live, 'merge', '--ff-only', self.target)
        record = dict(root=str(self.live), old=self.old, target=self.target)
        self.assertIn('Torii restored', updates.watch.rollback_code(record))
        self.assertEqual(self.git(self.live, 'rev-parse', 'HEAD'), self.old)


class StagingTests(TemporaryStore):
    def test_service_cannot_move_record_back_from_watchdog_rollback(self):
        self.record('rolling_back')
        self.updater.save(phase='tools')
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'rolling_back')

    def test_dead_watchdog_settles_success_rollback_and_partial_versions_from_facts(self):
        for phase, commit, actual, expected in (
                ('restart', 'b' * 40, '2.0.0', 'updated'),
                ('rollback_restart', 'a' * 40, '1.0.0', 'rolled_back'),
                ('restart', 'b' * 40, '1.0.0', 'failed')):
            with self.subTest(phase=phase, actual=actual):
                self.record(phase, old_pid=-1, old_process='gone', watchdog_pid=999999,
                            watchdog_process='gone', restart_started=updates.time.time(), attempted=[tool()])
                self.updater.commit = commit
                self.updater.healthy(updates.watch.process_identity(os.getpid()))
                with patch.object(updates, 'git', return_value=commit), patch.object(updates.watch, 'version', return_value=actual):
                    self.assertTrue(self.once())
                self.assertEqual(self.store.get(updates.KEY)['phase'], expected)
                self.assertIn('Claude Code is at ' + actual, self.latest_text())

    def test_restart_command_cannot_interrupt_an_update(self):
        self.record('tools')
        result = control_api.call(self.store, 'service.restart', {'reason': 'other restart'})
        self.assertFalse(result.ok)
        self.assertIn('update is already in progress', result.text)
        self.assertEqual(self.store.service_requests_pending(), [])

    def test_resumed_service_does_not_repeat_mutations(self):
        self.record('tools', old_pid=-1, old_process='gone', watchdog_pid=999999,
                    watchdog_process='gone', attempted=[])
        with patch.object(updates.watch, 'install') as install, patch.object(updates, 'git', return_value='a' * 40):
            self.assertTrue(self.once())
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'failed')
        self.assertTrue(control_api.call(self.store, 'service.restart', {'reason': 'deploy'}).ok)
        install.assert_not_called()

    def test_disable_settles_dead_record_and_finishes_original_card(self):
        self.record('restart', old_pid=-1, old_process='gone', watchdog_pid=999999,
                    watchdog_process='gone', attempted=[])
        with patch.object(updates, 'git', return_value='a' * 40):
            control_api.call(self.store, 'service.updates', {'enabled': False})
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'failed')
        self.assertEqual(self.store.db.execute('SELECT edit_message FROM outbox ORDER BY id DESC LIMIT 1').fetchone()[0], 1000)
        self.assertTrue(control_api.call(self.store, 'service.restart', {'reason': 'deploy'}).ok)

    def test_disable_cancels_work_before_watchdog_launch(self):
        for phase in ('checking', 'queued', 'checks', 'arming'):
            with self.subTest(phase=phase):
                self.record(phase)
                control_api.call(self.store, 'service.updates', {'enabled': False})
                self.assertEqual(self.store.get(updates.KEY)['phase'], 'cancelled')
                self.assertIn('Updates from Telegram are off', self.latest_text())

    def test_watchdog_pid_reuse_does_not_hold_restart_lock(self):
        self.record('tools', watchdog_pid=444, watchdog_process='old watchdog')
        with patch.object(updates.watch, 'process_identity', return_value='new unrelated process'):
            self.assertTrue(control_api.call(self.store, 'service.restart', {'reason': 'deploy'}).ok)

    def test_reused_old_service_pid_does_not_resume_mutations(self):
        self.record('tools', old_pid=os.getpid(), old_process='previous service', watchdog_pid=444,
                    watchdog_process='watchdog', attempted=[])
        with patch.object(updates.watch, 'process_identity', return_value='watchdog'):
            with patch.object(updates.watch, 'install') as install:
                self.assertFalse(self.once())
        install.assert_not_called()

    def test_save_cannot_overwrite_interleaved_watchdog_verdict(self):
        self.record('restart', old_pid=-1)
        self.updater.commit = 'b' * 40
        other = sqlite3.connect(str(self.store.directory / 'state.sqlite'), timeout=5)
        real_get = self.store.get
        reads = []
        def get(key, default=None):
            record = real_get(key, default)
            if key == updates.KEY:
                reads.append(key)
                if len(reads) == 2:
                    updates.watch.write(other, 'test', phase='updated', result='Updated.')
            return record
        try:
            with patch.object(self.store, 'get', side_effect=get):
                self.updater.healthy()
            self.assertEqual(real_get(updates.KEY)['phase'], 'updated')
        finally:
            other.close()

    def test_poll_health_does_not_run_ps_on_the_event_loop(self):
        self.record('restart', old_pid=-1)
        self.updater.commit = 'b' * 40
        with patch.object(updates.watch, 'process_identity', return_value='process') as identity:
            self.updater.healthy()
        identity.assert_not_called()

    def test_new_service_clears_only_its_update_restart_request(self):
        self.record('restart', old_pid=-1, old_process='old')
        self.store.put('restart_requested_v2', dict(update_id='test', reason='Telegram update'))
        with patch.object(updates, 'ROOT', self.updater.root), patch.object(updates, 'git', return_value='b' * 40):
            updates.Updater(self.service)
        self.assertIsNone(self.store.get('restart_requested_v2'))
        self.store.put('restart_requested_v2', dict(reason='someone else'))
        with patch.object(updates, 'ROOT', self.updater.root), patch.object(updates, 'git', return_value='b' * 40):
            updates.Updater(self.service)
        self.assertEqual(self.store.get('restart_requested_v2')['reason'], 'someone else')

    def test_waits_for_outbox_and_restart_guard(self):
        self.record('queued')
        self.store.enqueue_report('-10042:4', 'pending')
        self.assertFalse(self.once())
        self.deliver()
        self.store.put('last_restart_v2', updates.time.time())
        with patch.object(updates, 'prepare') as prepare:
            self.once()
        prepare.assert_not_called()
        self.assertIn('restarted recently', self.latest_text())

    def test_failed_cli_install_restores_previous_version(self):
        self.record('tools', watchdog_armed=True, attempted=[])
        with patch.object(updates, 'installation', return_value=('/temporary/bin/cli', '1.0.0', '/temporary/runtime')):
            self.failed_install()

    def failed_install(self):
        with patch.object(updates.watch, 'version', return_value='1.0.0'):
            with patch.object(updates.watch, 'install', side_effect=RuntimeError('npm failed')) as install:
                with patch.object(updates.watch, 'rollback_code', return_value='Torii restored to aaaaaaa.'), patch.object(
                        updates, 'git', return_value='b' * 40):
                    self.once()
        self.assertEqual([call.args[1] for call in install.call_args_list], ['2.0.0'])
        self.assertIn('Claude Code restored to 1.0.0.', self.latest_text())
        self.assertIn('Torii restored to aaaaaaa.', self.latest_text())
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'failed')

    def test_cli_only_skips_checks_and_uses_existing_restart_request(self):
        self.record('queued', **plan(tools=[tool()]))
        with patch.object(updates, 'prepare') as checks:
            self.once()
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'arming')
        checks.assert_not_called()
        self.deliver()
        self.record('tools', attempted=[tool()], **plan(tools=[tool()]))
        self.once()
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'restart')
        self.assertEqual(self.store.get('restart_requested_v2')['reason'], 'Telegram update')

    def test_watchdog_must_acknowledge_before_mutation(self):
        self.record('armed', armed_at=updates.time.time())
        with patch.object(updates, 'git') as git, patch.object(updates.watch, 'install') as install:
            self.assertFalse(self.once())
        git.assert_not_called()
        install.assert_not_called()

    def test_watchdog_is_copied_and_detached_before_checkout_changes(self):
        record = self.record('arming')
        with patch.object(updates.subprocess, 'Popen', return_value=SimpleNamespace(pid=444)) as spawn:
            self.assertEqual(updates.start_watchdog(record, self.store.directory), 444)
        args = spawn.call_args.args[0]
        copied = Path(args[1])
        self.assertEqual(copied.read_text(), (updates.ROOT / 'scripts/update-watchdog.py').read_text())
        self.assertEqual(args[0], sys.executable)
        self.assertEqual(args[2:], [str(self.store.directory / 'state.sqlite'), 'test'])
        self.assertTrue(spawn.call_args.kwargs['start_new_session'])
        self.assertTrue(spawn.call_args.kwargs['close_fds'])

    def test_service_interrupted_before_arming_reports_without_repeating(self):
        self.record('arming', old_pid=-1)
        with patch.object(updates, 'start_watchdog') as spawn:
            self.assertTrue(self.once())
        spawn.assert_not_called()
        self.assertIn('Update interrupted', self.latest_text())

    def test_updated_and_rollback_results_edit_same_card_once(self):
        for phase, text in (('updated', 'Updated.'), ('rolled_back', 'Update failed: health deadline.')):
            self.record(phase, result=text, attempted=[tool()], restored=[] if phase == 'updated' else ['Torii restored.'])
            self.once()
            row = self.store.db.execute('SELECT * FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
            self.assertEqual(row['edit_message'], 1000)
            self.assertTrue(row['text'].startswith(text))
            if phase == 'updated':
                self.assertIn('Torii aaaaaaa → bbbbbbb', row['text'])
                self.assertIn('Claude Code 1.0.0 → 2.0.0', row['text'])
            self.assertFalse(self.once())

    def test_health_requires_new_service_commit_snapshot(self):
        self.record('restart', old_pid=123)
        self.updater.commit = None
        self.updater.healthy()
        self.assertIsNone(self.store.get(updates.KEY).get('health'))
        self.updater.commit = 'b' * 40
        with patch.object(updates.watch, 'process_identity', return_value='new process'):
            self.updater.healthy('new process')
        self.assertEqual(self.store.get(updates.KEY)['health']['commit'], 'b' * 40)


class WatchdogTests(TemporaryStore):
    def record_watch(self, **values):
        fields = dict(old_pid=111, old_process='old', tools=[], attempted=[], restart_started=updates.time.time() - 51,
                      restart_from=dict(pid=111, process='old'))
        fields.update(values)
        return self.record('restart', **fields)

    def test_success_requires_new_pid_and_commit(self):
        started = updates.time.time()
        self.record_watch(restart_started=started, health=dict(pid=222, process='new', commit='b' * 40, restart_started=started))
        with patch.object(updates.watch, 'process_identity', return_value='new'):
            with patch.object(updates.watch.os, 'kill') as kill, patch.object(updates.watch, 'command', return_value='b' * 40) as command:
                updates.watch.watchdog(self.store.db, 'test')
        kill.assert_not_called()
        self.assertTrue(all(call.args[0][0] in ('git', 'ps') for call in command.call_args_list))
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'updated')

    def test_kills_old_pid_after_deadline_then_accepts_health(self):
        started = updates.time.time() - 51
        self.record_watch(restart_started=started)
        def sleep(seconds):
            updates.watch.write(self.store.db, 'test', health=dict(pid=222, process='new', commit='b' * 40, restart_started=started))
        with patch.object(updates.watch.time, 'monotonic', side_effect=[1, 62]):
            with patch.object(updates.watch.time, 'sleep', side_effect=sleep):
                with patch.object(updates.watch, 'process_identity', side_effect=['watchdog', 'old', 'old', 'new']), patch.object(
                        updates.watch, 'command', return_value='b' * 40):
                    with patch.object(updates.watch.os, 'kill') as kill:
                        updates.watch.watchdog(self.store.db, 'test')
        kill.assert_called_once_with(111, updates.watch.signal.SIGKILL)
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'updated')

    def test_rollback_restores_tools_checkout_and_waits_for_old_poll(self):
        self.record_watch(attempted=[tool()])
        def restart(argv, *args, **kwargs):
            if argv[0] == 'git':
                return ('a' if self.store.get(updates.KEY)['phase'] in ('rolling_back', 'rollback_restart') else 'b') * 40
            self.assertEqual(argv[:3], ['launchctl', 'kickstart', '-k'])
            updates.watch.write(self.store.db, 'test', health=dict(pid=333, process='back', commit='a' * 40,
                restart_started=self.store.get(updates.KEY)['restart_started']))
            return ''
        with patch.object(updates.watch.time, 'monotonic', side_effect=[1, 1, 250, 251, 252]):
            with patch.object(updates.watch, 'stop_old'), patch.object(updates.watch, 'install') as install, patch.object(
                    updates.watch, 'version', return_value='1.0.0'):
                with patch.object(updates.watch, 'rollback_code', return_value='Torii restored.'):
                    with patch.object(updates.watch, 'command', side_effect=restart):
                        with patch.object(updates.watch, 'process_identity', return_value='back'), patch.object(
                                updates.watch.host_os, 'SYSTEM', 'darwin'):
                            updates.watch.watchdog(self.store.db, 'test')
        install.assert_not_called()
        record = self.store.get(updates.KEY)
        self.assertEqual(record['phase'], 'rolled_back')
        self.assertEqual(record['restored'], ['Claude Code restored to 1.0.0.', 'Torii restored.'])

    def test_reused_pid_is_never_killed(self):
        record = self.record_watch()
        with patch.object(updates.watch, 'process_identity', return_value='unrelated'):
            with patch.object(updates.watch.os, 'kill') as kill:
                self.assertFalse(updates.watch.stop_old(record))
        kill.assert_not_called()

    def test_rollback_refuses_a_changed_checkout(self):
        self.record_watch()
        with patch.object(updates.watch.time, 'monotonic', side_effect=[1, 1, 250]):
            with patch.object(updates.watch, 'stop_old'), patch.object(updates.watch, 'rollback_code',
                    side_effect=RuntimeError('Checkout changed. Left untouched.')):
                with patch.object(updates.watch, 'command', return_value='b' * 40) as command:
                    updates.watch.watchdog(self.store.db, 'test')
        self.assertTrue(all(call.args[0][0] in ('git', 'ps') for call in command.call_args_list))
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'recovering')
        self.assertIn('Left untouched', self.store.get(updates.KEY)['result'])

    def test_versioned_install_checks_its_destination_before_switch(self):
        selected = tool()
        selected['paths'] = {'2.0.0': dict(prefix='/temporary/2.0.0', binary='/temporary/2.0.0/cli')}
        with patch.object(updates.watch, 'command', side_effect=['', '2.0.0\n']) as command:
            updates.watch.install(selected, '2.0.0')
        self.assertEqual(command.call_args_list[0].args[0], ['/opt/homebrew/bin/npm', '--prefix', '/temporary/2.0.0', 'install',
                         '--save-exact', '--include=optional', '--global=false', '--no-audit', '--no-fund',
                         '@anthropic-ai/claude-code@2.0.0'])
        self.assertEqual(command.call_args_list[1].args[0], ['/temporary/2.0.0/cli', '--version'])

    def test_failed_version_check_refuses_versioned_tool(self):
        selected = tool()
        selected['paths'] = {'2.0.0': dict(prefix='/temporary/2.0.0', binary='/temporary/2.0.0/cli')}
        with patch.object(updates.watch, 'command', side_effect=['', '1.0.0\n']):
            with self.assertRaisesRegex(RuntimeError, 'Claude Code version check failed'):
                updates.watch.install(selected, '2.0.0')

    def test_install_process_is_tracked_and_cleared_after_verification(self):
        self.record('tools')
        process = Mock(pid=444, returncode=0)
        with patch.object(updates.watch.subprocess, 'Popen', return_value=process) as spawn:
            updates.watch.tracked_install(['/opt/homebrew/bin/npm'], str(self.store.directory / 'state.sqlite'), 'test')
        self.assertTrue(spawn.call_args.kwargs['start_new_session'])
        self.assertIsNone(self.store.get(updates.KEY).get('installation'))
        process.communicate.assert_called_once_with(timeout=3600)

    def test_install_timeout_stops_its_group_before_restore(self):
        self.record('tools')
        process = Mock(pid=444, returncode=-9)
        process.poll.return_value = None
        process.communicate.side_effect = [subprocess.TimeoutExpired('npm', 3600), ('', '')]
        with patch.object(updates.watch.subprocess, 'Popen', return_value=process):
            with patch.object(updates.watch.os, 'killpg') as kill:
                with self.assertRaisesRegex(RuntimeError, 'npm install timed out'):
                    updates.watch.tracked_install(['/opt/homebrew/bin/npm'], str(self.store.directory / 'state.sqlite'), 'test')
        kill.assert_called_once_with(444, updates.watch.signal.SIGKILL)
        self.assertIsNone(self.store.get(updates.KEY).get('installation'))

    def test_failed_installer_tracking_refuses_to_start_process(self):
        process = Mock(pid=444)
        process.poll.return_value = None
        with patch.object(updates.watch.subprocess, 'Popen', return_value=process) as spawn:
            with patch.object(updates.watch.sqlite3, 'connect', side_effect=updates.watch.sqlite3.OperationalError()):
                with patch.object(updates.watch.os, 'killpg') as kill:
                    with self.assertRaisesRegex(RuntimeError, 'npm installation tracking failed'):
                        updates.watch.tracked_install(['npm'], 'unused', 'test')
        kill.assert_not_called()
        spawn.assert_not_called()

    def test_orphan_installer_is_stopped_only_when_process_and_group_match(self):
        record = dict(installation=dict(pid=444, process='npm process'))
        for identity in ('npm process', 'unrelated process'):
            with patch.object(updates.watch, 'process_identity', return_value=identity):
                with patch.object(updates.watch.os, 'getpgid', return_value=444):
                    with patch.object(updates.watch.os, 'killpg') as kill:
                        updates.watch.stop_installation(record)
            self.assertEqual(kill.call_count, 1 if identity == 'npm process' else 0)


class IdentityTests(TemporaryStore):
    def package(self, name):
        prefix = self.root / 'runtime'
        root = prefix / 'node_modules' / cli_identity.PACKAGES[name]
        root.mkdir(parents=True, exist_ok=True)
        (prefix / 'package.json').write_text('{}')
        manifest = root / 'package.json'
        manifest.write_text(json.dumps(dict(name=cli_identity.PACKAGES[name], version='1.0.0')))
        binary = root / 'cli.js'
        binary.write_text('#!/usr/bin/env node\n')
        binary.chmod(0o755)
        bins = prefix / 'node_modules/.bin'
        bins.mkdir(exist_ok=True)
        link = bins / name
        link.symlink_to(binary)
        return link, manifest, prefix

    def test_identity_and_prefix_follow_service_binary_without_subprocess(self):
        for name in ('claude', 'codex'):
            link, manifest, prefix = self.package(name)
            with patch.object(subprocess, 'run') as run:
                before = cli_identity.identity(str(link), cli_identity.PACKAGES[name])
                manifest.write_text(json.dumps(dict(name=cli_identity.PACKAGES[name], version='2.0.0')))
                after = cli_identity.identity(str(link), cli_identity.PACKAGES[name])
                self.assertNotEqual(before, after)
                self.assertEqual(cli_identity.installation(str(link), cli_identity.PACKAGES[name])[2], str(prefix.resolve()))
            run.assert_not_called()

    def test_claude_launch_fingerprint_changes_without_setting(self):
        link, manifest, prefix = self.package('claude')
        before = CoordinatorSession._launch_inputs(self.store, 'model', 'prompt', None, binary=str(link)).fingerprint
        manifest.write_text(json.dumps(dict(name=cli_identity.PACKAGES['claude'], version='2.0.0')))
        after = CoordinatorSession._launch_inputs(self.store, 'model', 'prompt', None, binary=str(link)).fingerprint
        self.assertNotEqual(before, after)
        self.assertIsNone(self.store.get('telegram_updates_enabled'))


class WatchdogSafetyTests(TemporaryStore):
    def test_installer_records_its_identity_before_executing_the_command(self):
        record = self.record('tools')
        with patch.object(updates.subprocess, 'Popen', return_value=SimpleNamespace(pid=444)):
            updates.start_watchdog(record, self.store.directory)
        output = self.root / 'installer.json'
        database = str(self.store.directory / 'state.sqlite')
        query = "SELECT value FROM settings WHERE key='telegram_update_v1'"
        code = ('import json,os,sqlite3; from pathlib import Path; '
                'db=sqlite3.connect(' + repr(database) + '); '
                'record=json.loads(db.execute(' + repr(query) + ').fetchone()[0]); '
                'Path(' + repr(str(output)) + ').write_text(json.dumps(dict(pid=os.getpid(), installation=record.get("installation"))))')
        with patch.object(updates.watch, 'process_identity', return_value=None):
            updates.watch.tracked_install([sys.executable, '-c', code], database, 'test')
        observed = json.loads(output.read_text())
        self.assertEqual(observed['installation']['pid'], observed['pid'])
        self.assertTrue(observed['installation']['process'])
        self.assertIsNone(self.store.get(updates.KEY)['installation'])

    def owned_restart(self, **fields):
        defaults = dict(old_pid=111, old_process='old', tools=[], restart_started=updates.time.time() - 51,
                        restart_from=dict(pid=111, process='old'), attempted=[])
        defaults.update(fields)
        return self.record('restart', **defaults)

    def identity(self, pid):
        return {222: 'new', 333: 'back', os.getpid(): 'watchdog'}.get(pid)

    def test_linux_rollback_uses_systemd_user_unit(self):
        self.owned_restart()
        calls = []
        def command(argv, *args, **kwargs):
            if argv[0] == 'git':
                return ('a' if self.store.get(updates.KEY)['phase'] in ('rolling_back', 'rollback_restart') else 'b') * 40
            calls.append(argv)
            current = self.store.get(updates.KEY)
            updates.watch.write(self.store.db, 'test', health=dict(pid=333, process='back',
                commit='a' * 40, restart_started=current.get('restart_started')))
            return ''
        with patch.object(updates.watch, 'host_os', SimpleNamespace(linux=lambda: True), create=True):
            with patch.object(updates.watch, 'command', side_effect=command), patch.object(
                    updates.watch, 'rollback_code', return_value='Torii restored.'):
                with patch.object(updates.watch, 'process_identity', side_effect=self.identity):
                    with patch.object(updates.watch.time, 'monotonic', side_effect=[1, 1, 250, 251, 252]):
                        with patch.object(updates.watch, 'stop_old'):
                            updates.watch.watchdog(self.store.db, 'test')
        self.assertEqual(calls, [['systemctl', '--user', 'restart', 'local.telegram-agent-coordinator.service']])
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'rolled_back')

    def test_external_deploy_stands_down_before_health_or_rollback(self):
        self.owned_restart(health=dict(pid=222, process='new', commit='b' * 40))
        with patch.object(updates.watch, 'command', return_value='c' * 40):
            with patch.object(updates.watch, 'process_identity', side_effect=self.identity):
                with patch.object(updates.watch, 'restore_tools', wraps=updates.watch.restore_tools) as restore, patch.object(updates.watch, 'stop_old') as stop:
                    updates.watch.watchdog(self.store.db, 'test')
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'failed')
        self.assertIn('stood down', self.store.get(updates.KEY)['result'])
        restore.assert_called_once()
        stop.assert_not_called()

    def test_unowned_restart_cannot_count_as_update_success(self):
        self.owned_restart(restart_started=None, health=dict(pid=222, process='new', commit='b' * 40))
        def command(argv, *args, **kwargs):
            if argv[0] != 'git':
                current = self.store.get(updates.KEY)
                updates.watch.write(self.store.db, 'test', health=dict(pid=333, process='back',
                    commit='a' * 40, restart_started=current['restart_started']))
            return ('a' if self.store.get(updates.KEY)['phase'] in ('rolling_back', 'rollback_restart') else 'b') * 40
        with patch.object(updates.watch, 'command', side_effect=command):
            with patch.object(updates.watch, 'process_identity', side_effect=self.identity):
                with patch.object(updates.watch, 'rollback_code', return_value='Torii restored.'):
                    updates.watch.watchdog(self.store.db, 'test')
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'rolled_back')
        self.assertIn('outside this update', self.store.get(updates.KEY)['result'])

    def test_watchdog_cannot_kill_during_service_shutdown_grace(self):
        self.owned_restart(restart_started=updates.time.time())
        def sleep(seconds):
            updates.watch.write(self.store.db, 'test', phase='updated')
        with patch.object(updates.watch, 'command', return_value='b' * 40):
            with patch.object(updates.watch, 'process_identity', side_effect=lambda pid: 'old' if pid == 111 else self.identity(pid)):
                with patch.object(updates.watch.time, 'monotonic', side_effect=[1, 62]):
                    with patch.object(updates.watch.time, 'sleep', side_effect=sleep):
                        with patch.object(updates.watch.os, 'kill') as kill:
                            updates.watch.watchdog(self.store.db, 'test')
        kill.assert_not_called()

    def test_rollback_poll_from_old_service_is_not_healthy(self):
        self.owned_restart()
        def command(argv, *args, **kwargs):
            if argv[0] != 'git':
                current = self.store.get(updates.KEY)
                updates.watch.write(self.store.db, 'test', health=dict(pid=111, process='old',
                    commit='a' * 40, restart_started=current.get('restart_started')))
            return ('a' if self.store.get(updates.KEY)['phase'] in ('rolling_back', 'rollback_restart') else 'b') * 40
        with patch.object(updates.watch, 'command', side_effect=command), patch.object(
                updates.watch, 'rollback_code', return_value='Torii restored.'):
            with patch.object(updates.watch, 'process_identity', side_effect=self.identity):
                with patch.object(updates.watch.time, 'monotonic', side_effect=[1, 1, 250, 251, 252, 500]):
                    with patch.object(updates.watch.time, 'sleep'), patch.object(updates.watch, 'stop_old'):
                        updates.watch.watchdog(self.store.db, 'test')
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'failed')
        self.assertIn('poll was not healthy', self.store.get(updates.KEY)['result'])

    def test_installer_cannot_start_after_parent_dies_before_registration(self):
        record = self.record('tools')
        with patch.object(updates.subprocess, 'Popen', return_value=SimpleNamespace(pid=444)):
            updates.start_watchdog(record, self.store.directory)
        spawned = self.root / 'spawned'
        installed = self.root / 'installed'
        parent = self.root / 'install-parent.py'
        argv = [sys.executable, '-c', 'from pathlib import Path; import os,time; '
                'Path(' + repr(str(installed)) + ').write_text(str(os.getpid())); time.sleep(15)']
        parent.write_text('import sys\nsys.path.insert(0, ' + repr(str(updates.ROOT)) + ')\n'
            'from coordinator import updates\nfrom pathlib import Path\n'
            'real = updates.watch.subprocess.Popen\n'
            'def spawn(*args, **kwargs):\n    child = real(*args, **kwargs)\n'
            '    pending = Path(' + repr(str(spawned.with_suffix('.pending'))) + ')\n'
            '    pending.write_text(str(child.pid))\n'
            '    pending.replace(' + repr(str(spawned)) + ')\n    return child\n'
            'updates.watch.subprocess.Popen = spawn\nupdates.watch.tracked_install(' + repr(argv) + ', ' +
            repr(str(self.store.directory / 'state.sqlite')) + ", 'test')\n")
        self.store.db.execute('BEGIN IMMEDIATE')
        process = subprocess.Popen([sys.executable, str(parent)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        child_pid = None
        child_process = None
        try:
            asyncio.run(until_exists(spawned))
            child_pid = int(spawned.read_text())
            child_process = updates.watch.process_identity(child_pid)
            process.kill()
            process.communicate(timeout=3)
            record = self.store.get(updates.KEY)
            record['phase'] = 'failed'
            self.store.put(updates.KEY, record)
            self.store.db.commit()
            updates.time.sleep(0.2)
            self.assertFalse(installed.exists(), 'installer ran before recording ownership')
        finally:
            self.store.db.rollback()
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=3)
            if child_pid and child_process and updates.watch.process_identity(child_pid) == child_process:
                try:
                    os.kill(child_pid, updates.watch.signal.SIGKILL)
                except ProcessLookupError:
                    pass


class OpenCodeUpdateTests(TemporaryStore):
    def setUp(self):
        super().setUp()
        self.installs = self.root / 'opencode'
        self.relative = Path('node_modules/opencode-darwin-arm64/bin/opencode')
        self.binary = self.make_version('1.19.0')
        self.store.put('opencode_binary', str(self.binary))
        self.npm_log = self.root / 'npm.jsonl'
        self.npm_latest = self.root / 'npm-latest'
        self.npm_latest.write_text('1.19.4')
        self.npm_mode = self.root / 'npm-mode'
        self.npm_mode.write_text('ok')
        commands = self.root / 'bin'
        commands.mkdir()
        npm = commands / 'npm'
        npm.write_text('#!' + sys.executable + '\n' +
            'import json,sys\nfrom pathlib import Path\n' +
            'args=sys.argv[1:]\n' +
            'with Path(' + repr(str(self.npm_log)) + ').open("a") as log: log.write(json.dumps(args)+"\\n")\n' +
            'mode=Path(' + repr(str(self.npm_mode)) + ').read_text()\n' +
            'if args[0]=="view": print("2.0.0"); sys.exit()\n' +
            'if args[2]=="view":\n' +
            '    if mode=="unreadable": sys.exit(1)\n' +
            '    print(Path(' + repr(str(self.npm_latest)) + ').read_text())\n' +
            '    sys.exit()\n' +
            'if mode=="fail": sys.exit(1)\n' +
            'prefix=Path(args[1])\nversion=args[-1].split("@")[-1]\n' +
            'assert prefix.parent==Path(' + repr(str(self.installs)) + ')\n' +
            'manifest=prefix/"node_modules/opencode-ai/package.json"\n' +
            'manifest.parent.mkdir(parents=True,exist_ok=True)\n' +
            'manifest.write_text(json.dumps(dict(name="opencode-ai",version=version)))\n' +
            '(prefix/"package.json").write_text(json.dumps(dict(dependencies={"opencode-ai":version})))\n' +
            'binary=prefix/' + repr(str(self.relative)) + '\n' +
            'binary.parent.mkdir(parents=True,exist_ok=True)\n' +
            'output="1.19.99" if mode=="wrong" else version\n' +
            'if mode=="decorated": output="OpenCode "+version\n' +
            'if mode=="spaces": output=" "+version+" "\n' +
            'if mode=="extra-newline": output=version+"\\n"\n' +
            'binary.write_text(' + repr('#!' + sys.executable + '\n') + '+"print("+repr(output)+")\\n")\n' +
            'binary.chmod(0o755)\n')
        npm.chmod(0o755)
        self.local_npm_patch.stop()
        self.local_npm_patch = patch.object(updates.watch, 'local_npm', return_value=str(npm), create=True)
        self.local_npm_patch.start()
        self.addCleanup(self.local_npm_patch.stop)
        self.environment = patch.dict(os.environ, PATH=str(commands) + os.pathsep + os.environ['PATH'])
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def make_version(self, version):
        prefix = self.installs / version
        manifest = prefix / 'node_modules/opencode-ai/package.json'
        manifest.parent.mkdir(parents=True, exist_ok=True)
        manifest.write_text(json.dumps(dict(name='opencode-ai', version=version)))
        (prefix / 'package.json').write_text(json.dumps(dict(dependencies={'opencode-ai': version})))
        binary = prefix / self.relative
        binary.parent.mkdir(parents=True, exist_ok=True)
        binary.write_text('#!' + sys.executable + '\nprint(' + repr(version) + ')\n')
        binary.chmod(0o755)
        return binary

    def inventory(self, target='1.19.3', subjects=(), binaries=None):
        head = ('b' if subjects else 'a') * 40
        log_range = 'a' * 40 + '..' + head
        answers = {('fetch', 'origin', 'main'): '', ('rev-parse', 'HEAD'): 'a' * 40,
                   ('rev-parse', 'origin/main'): head,
                   ('log', '--first-parent', '--format=%s', log_range): '\n'.join(subjects),
                   ('log', '--first-parent', '--format=%s%x00%b%x1e', log_range):
                       ''.join(subject + '\x00\x1e' for subject in subjects),
                   ('show', head + ':coordinator/opencode_accounts.py'): 'PINNED_VERSION = ' + repr(target) + '\n'}
        binaries = binaries or dict(claude=str(self.root / 'missing-claude'), codex=str(self.root / 'missing-codex'))
        with patch.object(updates, 'git', side_effect=lambda root, *args: answers[args]):
            with patch.object(updates, 'checkout', return_value='a' * 40):
                return updates.inventory(self.updater.root, binaries, self.store.get('opencode_binary'))

    def start_install(self, value):
        self.record('tools', watchdog_armed=True, attempted=[], **value)
        with patch.object(updates.subprocess, 'Popen', return_value=SimpleNamespace(pid=444)):
            updates.start_watchdog(self.store.get(updates.KEY), self.store.directory)

    def tick(self):
        self.deliver()
        with patch.object(updates, 'git', return_value='a' * 40):
            with patch.object(updates.watch, 'rollback_code', return_value='Torii remains at aaaaaaa.'):
                return self.once()

    def npm_calls(self):
        return [json.loads(line) for line in self.npm_log.read_text().splitlines()]

    def test_switch_window_old_process_accepts_own_binary(self):
        from coordinator import opencode_accounts
        with patch.object(opencode_accounts, 'PINNED_VERSION', '1.19.0'):
            opencode_accounts._check_binary(self.store.get('opencode_binary'))
            self.start_install(self.inventory())
            self.tick()
            self.tick()
            self.assertEqual(self.store.get(updates.KEY)['phase'], 'restart')
            self.assertIsNotNone(self.store.get('restart_requested_v2'))
            self.assertEqual(self.store.get('opencode_binary'), str(self.installs / '1.19.3' / self.relative))
            with patch.object(opencode_accounts, 'check_key'):
                self.store.put('opencode_accounts', {'one': {'model': 'example-model'}})
                self.store.put('opencode_active_account', 'one')
                opencode_accounts.check_ready(self.store)
            self.assertEqual(opencode_accounts.binary(self.store), str(self.binary))

    def test_target_pin_card_and_versioned_install_preserve_old_folder(self):
        value = self.inventory()
        self.assertIn('OpenCode 1.19.0 → 1.19.3 · 1.19.4 is out but not tested with Torii yet.', updates.card(value))
        before = {str(p): p.read_bytes() for p in self.binary.parents[3].rglob('*') if p.is_file()}
        self.start_install(value)
        self.tick()
        self.assertEqual(self.store.get('opencode_binary'), str(self.binary))
        self.tick()
        target = self.installs / '1.19.3' / self.relative
        self.assertEqual(self.store.get('opencode_binary'), str(target))
        self.assertEqual(updates.watch.version(str(target)), '1.19.3')
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'restart')
        self.assertEqual(self.store.get('restart_requested_v2')['update_id'], 'test')
        self.assertEqual([call for call in self.npm_calls() if call[2] == 'install'], [[
            '--prefix', str(self.installs / '1.19.3'), 'install', '--save-exact', '--include=optional',
            '--global=false', '--no-audit', '--no-fund', 'opencode-ai@1.19.3']])
        self.assertEqual(before, {str(p): p.read_bytes() for p in self.binary.parents[3].rglob('*') if p.is_file()})
        record = self.store.get(updates.KEY)
        record.update(phase='updated', result='Updated.')
        self.assertEqual(updates.outcome(record), 'Updated.\nOpenCode 1.19.0 → 1.19.3')

    def test_equal_pin_current_card_notes_newer_npm_without_install(self):
        value = self.inventory('1.19.0')
        self.assertFalse(updates.available(value))
        self.store.put('telegram_updates_enabled', True)
        self.command()
        self.deliver()
        with patch.object(updates, 'inventory', return_value=value):
            self.once()
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'current')
        self.assertEqual(self.latest_text(), 'Torii and its npm tools are current.\n'
                         'OpenCode 1.19.0 · 1.19.4 is out but not tested with Torii yet.')
        self.assertTrue(all(call[2] == 'view' for call in self.npm_calls()))

    def test_unreadable_npm_omits_only_note(self):
        self.npm_mode.write_text('unreadable')
        value = self.inventory()
        self.assertTrue(updates.available(value))
        self.assertIn('OpenCode 1.19.0 → 1.19.3\n', updates.card(value))
        self.assertNotIn('not tested', updates.card(value))

    def test_equal_pin_without_newer_npm_still_shows_current_version(self):
        for released in ('1.18.0', '1.19.0', 'unreadable'):
            with self.subTest(released=released):
                self.npm_latest.write_text(released)
                self.assertEqual(updates.card(self.inventory('1.19.0')),
                                 'Torii and its npm tools are current.\nOpenCode 1.19.0')

    def test_exact_version_accepts_no_trailing_newline(self):
        self.binary.write_text('#!' + sys.executable + '\nimport sys\nsys.stdout.write("1.19.0")\n')
        value = self.inventory('1.19.0')
        self.assertFalse(updates.available(value))
        self.assertEqual(value['tools'][0]['current'], '1.19.0')

    def test_unset_binary_has_no_opencode_line(self):
        self.assertIn('OpenCode 1.19.0 → 1.19.3', updates.card(self.inventory()))
        calls_before = self.npm_calls()
        self.store.put('opencode_binary', None)
        value = self.inventory()
        self.assertEqual(value['tools'], [])
        self.assertNotIn('OpenCode', updates.card(value))
        self.assertEqual(self.npm_calls(), calls_before)

    def test_unmanaged_binary_is_shown_without_update_count(self):
        outside = self.root / 'opencode-host'
        outside.write_bytes(self.binary.read_bytes())
        outside.chmod(0o755)
        self.store.put('opencode_binary', str(outside))
        value = self.inventory()
        self.assertFalse(updates.available(value))
        self.assertIn("OpenCode 1.19.0 → 1.19.3 · not installed in Torii's OpenCode folder; update on the host.",
                      updates.card(value))
        self.assertTrue(all(call[2] == 'view' for call in self.npm_calls()))

    def test_manifest_version_mismatch_is_unmanaged(self):
        manifest = self.installs / '1.19.0/node_modules/opencode-ai/package.json'
        manifest.write_text(json.dumps(dict(name='opencode-ai', version='1.18.0')))
        value = self.inventory()
        self.assertFalse(updates.available(value))
        self.assertIn("not installed in Torii's OpenCode folder", updates.card(value))

    def test_install_failure_keeps_setting_and_reports_error(self):
        self.start_install(self.inventory())
        self.npm_mode.write_text('fail')
        self.tick()
        self.assertEqual(self.store.get('opencode_binary'), str(self.binary))
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'failed')
        self.assertIn('Update failed: npm install failed', self.latest_text())
        self.assertEqual(sum(call[2] == 'install' for call in self.npm_calls()), 1)

    def test_wrong_or_decorated_new_binary_is_refused(self):
        for mode in ('wrong', 'decorated', 'spaces', 'extra-newline'):
            with self.subTest(mode=mode):
                self.start_install(self.inventory())
                self.npm_mode.write_text(mode)
                self.tick()
                self.assertEqual(self.store.get('opencode_binary'), str(self.binary))
                self.assertEqual(self.store.get(updates.KEY)['phase'], 'failed')
                self.assertIn('version check failed', self.latest_text())
                self.npm_mode.write_text('ok')

    def test_downgrade_installs_the_target_pin(self):
        self.binary = self.make_version('1.19.4')
        self.store.put('opencode_binary', str(self.binary))
        value = self.inventory()
        self.assertIn('OpenCode 1.19.4 → 1.19.3', updates.card(value))
        self.start_install(value)
        self.tick()
        self.tick()
        self.assertEqual(self.store.get('opencode_binary'), str(self.installs / '1.19.3' / self.relative))

    def test_failed_restart_watchdog_restores_setting_without_npm(self):
        self.start_install(self.inventory())
        self.tick()
        self.tick()
        self.npm_mode.write_text('fail')
        calls_before = self.npm_calls()
        self.store.put('restart_requested_v2', None)
        record = self.store.get(updates.KEY)
        record.update(old_pid=111, old_process='old', restart_started=updates.time.time() - 51,
                      restart_from=dict(pid=111, process='old'))
        self.store.put(updates.KEY, record)
        def command(argv, *args, **kwargs):
            if argv[0] == 'git':
                return 'a' * 40
            current = self.store.get(updates.KEY)
            updates.watch.write(self.store.db, 'test', health=dict(pid=333, process='back', commit='a' * 40,
                                restart_started=current['restart_started']))
            return ''
        original = updates.watch.command
        def run(argv, *args, **kwargs):
            return original(argv, *args, **kwargs) if argv[0].startswith(str(self.installs)) else command(argv, *args, **kwargs)
        with patch.object(updates.watch, 'command', side_effect=run):
            with patch.object(updates.watch, 'process_identity', side_effect=lambda pid: {333: 'back', os.getpid(): 'watchdog'}.get(pid)):
                with patch.object(updates.watch.time, 'monotonic', side_effect=[1, 1, 250, 251, 252]):
                    updates.watch.watchdog(self.store.db, 'test', service_control=['fake-service'])
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'rolled_back')
        self.assertEqual(self.store.get('opencode_binary'), str(self.binary))
        self.assertEqual(updates.watch.version(str(self.binary)), '1.19.0')
        self.assertTrue((self.installs / '1.19.3' / self.relative).exists())
        self.assertEqual(self.npm_calls(), calls_before)

    def test_superseded_update_restores_setting_offline(self):
        self.start_install(self.inventory())
        self.tick()
        self.tick()
        real = updates.watch.command
        with patch.object(updates.watch, 'command', side_effect=lambda argv, *a, **kw:
                          'c' * 40 if argv[0] == 'git' else real(argv, *a, **kw)):
            self.assertTrue(updates.watch.superseded(self.store.db, self.store.get(updates.KEY)))
        self.assertEqual(self.store.get('opencode_binary'), str(self.binary))
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'failed')

    def test_dead_watchdog_reads_version_from_selected_setting(self):
        self.start_install(self.inventory())
        self.tick()
        self.tick()
        record = self.store.get(updates.KEY)
        record.update(watchdog_pid=999999, watchdog_process='gone', old_pid=-1, old_process='gone',
                      restart_started=updates.time.time())
        self.store.put(updates.KEY, record)
        self.updater.commit = 'a' * 40
        self.updater.healthy(updates.watch.process_identity(os.getpid()))
        self.tick()
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'updated')
        self.assertIn('OpenCode is at 1.19.3.', self.latest_text())
        self.assertEqual(self.store.get('opencode_binary'), str(self.installs / '1.19.3' / self.relative))

    def test_dead_watchdog_before_restart_restores_setting_offline(self):
        self.start_install(self.inventory())
        self.tick()
        self.tick()
        record = self.store.get(updates.KEY)
        record.update(watchdog_pid=999999, watchdog_process='gone')
        self.store.put(updates.KEY, record)
        self.tick()
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'failed')
        self.assertEqual(self.store.get('opencode_binary'), str(self.binary))
        self.assertIn('OpenCode is restored to 1.19.0.', self.latest_text())

    def test_dead_watchdog_before_first_healthy_poll_keeps_new_runtime_binary(self):
        self.start_install(self.inventory())
        self.tick()
        self.tick()
        record = self.store.get(updates.KEY)
        record.update(watchdog_pid=999999, watchdog_process='gone', old_pid=-1, old_process='gone',
                      health=None, restart_started=updates.time.time())
        self.store.put(updates.KEY, record)
        self.updater.commit = 'a' * 40
        self.tick()
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'failed')
        self.assertEqual(self.store.get('opencode_binary'), str(self.installs / '1.19.3' / self.relative))
        self.assertIn('OpenCode is at 1.19.3.', self.latest_text())
        self.assertIn('healthy poll after this update step is unverified', self.latest_text())

    def test_rollback_refuses_changed_previous_binary(self):
        self.start_install(self.inventory())
        self.tick()
        self.tick()
        target = self.store.get('opencode_binary')
        self.binary.write_text('#!' + sys.executable + '\nprint("1.18.0")\n')
        result = updates.watch.restore_tools(self.store.get(updates.KEY), database=str(self.store.directory / 'state.sqlite'))
        self.assertEqual(result, ['OpenCode restoration failed. Repair it on the host.'])
        self.assertEqual(self.store.get('opencode_binary'), target)

    def test_setting_change_since_card_refuses_install(self):
        self.start_install(self.inventory())
        self.store.put('opencode_binary', str(self.make_version('1.19.4')))
        self.tick()
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'failed')
        self.assertIn('changed since this card', self.latest_text())
        self.assertFalse(any(call[2] == 'install' for call in self.npm_calls()))

    def test_rollback_verdict_before_switch_keeps_previous_setting(self):
        self.start_install(self.inventory())
        self.tick()
        version = updates.watch.version
        def begin_rollback(*args, **kwargs):
            actual = version(*args, **kwargs)
            with sqlite3.connect(str(self.store.directory / 'state.sqlite')) as db:
                updates.watch.write(db, 'test', phase='rolling_back')
            return actual
        with patch.object(updates.watch, 'version', side_effect=begin_rollback):
            self.assertFalse(self.tick())
        self.assertEqual(self.store.get('opencode_binary'), str(self.binary))
        self.assertIsNone(self.store.get('restart_requested_v2'))
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'rolling_back')

    def test_bad_target_pin_fails_check(self):
        self.assertEqual(self.inventory()['tools'][0]['latest'], '1.19.3')
        self.store.put('opencode_binary', str(self.root / 'missing-opencode'))
        for pin in ('unknown', '../1.19.3'):
            with self.subTest(pin=pin):
                with self.assertRaisesRegex(RuntimeError, 'OpenCode.*could not be read'):
                    self.inventory(pin)

    def test_target_parser_reads_real_checkout_pin(self):
        from coordinator.opencode_accounts import PINNED_VERSION
        self.assertEqual(self.inventory(PINNED_VERSION)['tools'][0]['latest'], PINNED_VERSION)
        repository = self.root / 'pin-checkout'
        source = repository / 'coordinator/opencode_accounts.py'
        source.parent.mkdir(parents=True)
        source.write_bytes((updates.ROOT / 'coordinator/opencode_accounts.py').read_bytes())
        updates.git(repository, 'init', '-q')
        updates.git(repository, 'add', 'coordinator/opencode_accounts.py')
        updates.git(repository, '-c', 'user.name=Torii Test', '-c', 'user.email=test@example.com',
                    'commit', '-qm', 'OpenCode pin fixture')
        self.assertEqual(updates.pinned_version(repository, 'HEAD'), PINNED_VERSION)

    def assert_unreadable_binary_keeps_torii_update(self):
        selected = self.store.get('opencode_binary')
        value = self.inventory(subjects=['fix Torii'])
        text = updates.card(value)
        self.assertEqual(text.splitlines()[0], 'Update Torii?')
        self.assertIn('OpenCode · cannot read its binary at the saved path; left out of this update.', text.splitlines())
        self.assertTrue(updates.available(value))
        self.store.put('telegram_updates_enabled', True)
        self.command()
        self.deliver()
        with patch.object(updates, 'inventory', return_value=value):
            self.once()
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'offer')
        self.assertEqual(self.latest_text(), text)
        self.start_install(value)
        self.tick()
        record = self.store.get(updates.KEY)
        self.assertEqual(record['phase'], 'restart')
        self.assertEqual(record['attempted'], [])
        self.assertEqual(self.store.get('opencode_binary'), selected)
        self.assertFalse(self.npm_log.exists())

    def test_missing_binary_still_offers_torii_update(self):
        self.store.put('opencode_binary', str(self.root / 'gone/opencode'))
        self.assert_unreadable_binary_keeps_torii_update()

    def test_non_executable_binary_still_offers_torii_update(self):
        self.binary.chmod(0o644)
        self.assert_unreadable_binary_keeps_torii_update()

    def test_failing_binary_still_offers_torii_update(self):
        self.binary.write_text('#!' + sys.executable + '\nimport sys\nsys.exit(1)\n')
        self.assert_unreadable_binary_keeps_torii_update()

    def test_wrong_output_binary_still_offers_torii_update(self):
        self.binary.write_text('#!' + sys.executable + '\nprint("OpenCode 1.19.0")\n')
        self.assert_unreadable_binary_keeps_torii_update()

    def test_hanging_binary_still_offers_torii_update(self):
        self.binary.write_text('#!' + sys.executable + '\nimport time\ntime.sleep(30)\n')
        version = updates.watch.version
        with patch.object(updates.watch, 'version',
                          side_effect=lambda *args, **kwargs: version(*args, timeout=.5, **kwargs)):
            self.assert_unreadable_binary_keeps_torii_update()

    def test_current_unmanaged_binary_needs_no_host_update(self):
        outside = self.root / 'opencode-host'
        outside.write_bytes(self.binary.read_bytes())
        outside.chmod(0o755)
        self.store.put('opencode_binary', str(outside))
        self.npm_latest.write_text('1.19.0')
        value = self.inventory('1.19.0')
        self.assertFalse(updates.available(value))
        self.assertEqual(updates.card(value), 'Torii and its npm tools are current.\n'
                         "OpenCode 1.19.0 · not installed in Torii's OpenCode folder.")
        self.assertTrue(all(call[2] == 'view' for call in self.npm_calls()))

    def fail_opencode_restore(self):
        with self.store.db:
            self.store.db.execute("CREATE TRIGGER fail_opencode_restore BEFORE INSERT ON settings "
                                  "WHEN NEW.key='opencode_binary' BEGIN "
                                  "SELECT RAISE(FAIL, 'OpenCode setting unavailable'); END")

    def test_sqlite_restore_error_watchdog_still_rolls_back_code_and_other_tools(self):
        self.start_install(self.inventory())
        self.tick()
        self.tick()
        record = self.store.get(updates.KEY)
        record.update(old='a' * 40, target='b' * 40, old_pid=-1, old_process='gone',
                      attempted=[tool(), tool('Codex')] + record['attempted'])
        self.store.put(updates.KEY, record)
        self.fail_opencode_restore()
        head = 'b' * 40
        original = updates.watch.command
        def command(argv, *args, **kwargs):
            nonlocal head
            if argv[0].startswith(str(self.installs)):
                return original(argv, *args, **kwargs)
            if argv[:3] == ['git', 'rev-parse', 'HEAD']:
                return head
            if argv[:3] == ['git', 'reset', '--keep']:
                head = argv[3]
            return ''
        with patch.object(updates.watch, 'command', side_effect=command) as run:
            with patch.object(updates.watch, 'install') as install:
                updates.watch.watchdog(self.store.db, 'test', service_control=['fake-service'], restart_timeout=0)
        self.assertEqual(head, 'a' * 40)
        self.assertIn(unittest.mock.call(['git', 'reset', '--keep', 'a' * 40], str(self.updater.root)), run.call_args_list)
        install.assert_not_called()
        record = self.store.get(updates.KEY)
        self.assertEqual(record['phase'], 'recovering')
        self.assertIn('OpenCode restoration failed. Repair it on the host.', record['restored'])
        self.assertIn('Torii restored to aaaaaaa.', record['restored'])

    def test_sqlite_restore_error_recovery_reports_restored_code(self):
        self.start_install(self.inventory())
        self.tick()
        self.tick()
        record = self.store.get(updates.KEY)
        record.update(old='a' * 40, target='b' * 40, watchdog_pid=-1, watchdog_process='gone',
                      attempted=[tool(), tool('Codex')] + record['attempted'], phase='rollback_restart')
        self.store.put(updates.KEY, record)
        self.fail_opencode_restore()
        self.updater.commit = 'a' * 40
        self.tick()
        record = self.store.get(updates.KEY)
        self.assertEqual(record['phase'], 'failed')
        self.assertIn('Torii code is restored to aaaaaaa.', self.latest_text())
        self.assertIn('OpenCode restoration failed. Repair it on the host.', self.latest_text())

    def test_sqlite_read_error_recovery_still_reports_other_tools(self):
        record = self.record('rollback_restart', attempted=[self.inventory()['tools'][0], tool(), tool('Codex')])
        def version(selected, db=None):
            if selected.get('setting'):
                raise sqlite3.OperationalError('OpenCode setting locked')
            return selected['current']
        with patch.object(updates.watch, 'tool_version', side_effect=version):
            with patch.object(updates, 'git', return_value='a' * 40):
                facts = updates.recovery_facts(record, database=str(self.store.directory / 'state.sqlite'))
            self.assertFalse(updates.watch.tools_match(record, 'current', self.store.db))
        self.assertEqual(facts['phase'], 'failed')
        self.assertIn('Torii code is restored to aaaaaaa.', facts['restored'])
        self.assertIn('OpenCode restoration is unverified; previous version was 1.19.0.', facts['restored'])
        self.assertIn('Claude Code is restored to 1.0.0.', facts['restored'])
        self.assertIn('Codex is restored to 1.0.0.', facts['restored'])

    def test_sqlite_read_error_in_process_still_restores_code_and_other_tools(self):
        self.start_install(self.inventory())
        self.tick()
        record = self.store.get(updates.KEY)
        record['attempted'] = [tool(), tool('Codex')] + record['attempted']
        self.store.put(updates.KEY, record)
        self.fail_opencode_restore()
        read = self.store.get
        def get(key, *args):
            if key == 'opencode_binary':
                raise sqlite3.OperationalError('OpenCode setting locked')
            return read(key, *args)
        with patch.object(self.store, 'get', side_effect=get):
            with patch.object(updates.watch, 'install') as install:
                with patch.object(updates.watch, 'rollback_code', return_value='Torii restored to aaaaaaa.') as rollback:
                    self.deliver()
                    with patch.object(updates, 'git', return_value='a' * 40):
                        self.once()
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'failed')
        rollback.assert_called_once()
        install.assert_not_called()
        self.assertIn('OpenCode restoration failed. Repair it on the host.', self.latest_text())
        self.assertIn('Torii restored to aaaaaaa.', self.latest_text())

    def test_unreadable_binary_preserves_claude_and_codex_updates(self):
        binaries = {}
        for name, package in cli_identity.PACKAGES.items():
            prefix = self.root / name
            package_root = prefix / 'node_modules' / package
            binary = package_root / 'bin' / name
            binary.parent.mkdir(parents=True)
            (package_root / 'package.json').write_text(json.dumps(dict(name=package, version='1.0.0', bin={name: 'bin/' + name})))
            (prefix / 'package.json').write_text('{}')
            binary.write_text('#!' + sys.executable + '\nprint("1.0.0")\n')
            binary.chmod(0o755)
            binaries[name] = str(binary)
        self.store.put('opencode_binary', None)
        before = self.inventory(subjects=['fix Torii'], binaries=binaries)
        self.assertEqual([selected['label'] for selected in before['tools']], ['Claude Code', 'Codex'])
        self.store.put('opencode_binary', str(self.root / 'gone/opencode'))
        after = self.inventory(subjects=['fix Torii'], binaries=binaries)
        self.assertEqual(after['tools'][:2], before['tools'])
        self.assertEqual(updates.card(before).splitlines()[0], 'Update Torii and 2 tools?')
        self.assertEqual(updates.card(after).splitlines()[0], 'Update Torii and 2 tools?')
        self.assertEqual([selected for selected in after['tools'] if selected['outdated'] and selected['prefix']],
                         before['tools'])
        self.assertTrue(all('opencode-ai' not in ' '.join(call) for call in self.npm_calls()))

    def assert_broken_opencode_updates_other_tools(self, slow_codex=False):
        selected = self.store.get('opencode_binary')
        before = {str(path.relative_to(self.installs)): (path.stat().st_mode, path.read_bytes() if path.is_file() else None)
                  for path in self.installs.rglob('*')}
        binaries = {}
        for name, package in cli_identity.PACKAGES.items():
            prefix = self.root / name
            package_root = prefix / 'node_modules' / package
            binary = package_root / 'bin' / name
            binary.parent.mkdir(parents=True)
            (package_root / 'package.json').write_text(json.dumps(dict(name=package, version='1.0.0', bin={name: 'bin/' + name})))
            (prefix / 'package.json').write_text('{}')
            startup = 'import time\ntime.sleep(1)\n' if slow_codex and name == 'codex' else ''
            binary.write_text('#!' + sys.executable + '\n' + startup + 'print("1.0.0")\n')
            binary.chmod(0o755)
            binaries[name] = str(binary)
        npm = self.root / 'bin/npm'
        npm.write_text('#!' + sys.executable + '\n' +
            'import json,sys\nfrom pathlib import Path\n' +
            'args=sys.argv[1:]\n' +
            'with Path(' + repr(str(self.npm_log)) + ').open("a") as log: log.write(json.dumps(args)+"\\n")\n' +
            'if args[0]=="view": print("2.0.0"); sys.exit()\n' +
            'assert args[2]=="install"\n' +
            'prefix=Path(args[1])\npackage,version=args[-1].rsplit("@",1)\n' +
            'assert package in ("@anthropic-ai/claude-code","@openai/codex")\n' +
            'root=prefix/"node_modules"/package\n' +
            'name="codex" if package=="@openai/codex" else "claude"\n' +
            '(root/"bin").mkdir(parents=True,exist_ok=True)\n' +
            '(root/"package.json").write_text(json.dumps(dict(name=package,version=version,bin={name:"bin/"+name})))\n' +
            '(root/"bin"/name).write_text(' + repr('#!' + sys.executable + '\n') +
            '+"print("+repr(version)+")\\n")\n' +
            '(root/"bin"/name).chmod(0o755)\n')
        self.service.runner.binaries = binaries
        value = self.inventory(subjects=['fix Torii'], binaries=binaries)
        self.assertEqual(updates.card(value).splitlines(), [
            'Update Torii and 2 tools?', 'Torii aaaaaaa → bbbbbbb', '• fix Torii',
            'Claude Code 1.0.0 → 2.0.0', 'Codex 1.0.0 → 2.0.0',
            'OpenCode · cannot read its binary at the saved path; left out of this update.',
            'Running chats and workers keep going.'])
        self.store.put('telegram_updates_enabled', True)
        self.command()
        self.deliver()
        with patch.object(updates, 'inventory', return_value=value):
            self.assertTrue(self.once())
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'offer')
        self.assertEqual(self.latest_text(), updates.card(value))
        self.start_install(value)
        self.record('merge', watchdog_armed=True, attempted=[], **value)
        with patch.object(updates, 'git', return_value=value['old']) as git:
            with patch.object(updates, 'checkout', return_value=value['old']):
                self.assertTrue(self.once())
        self.assertIn(call(self.updater.root, 'merge', '--ff-only', value['target']), git.call_args_list)
        self.assertEqual(self.store.get(updates.KEY)['phase'], 'tools')
        for _ in range(3):
            self.assertTrue(self.tick())
        record = self.store.get(updates.KEY)
        self.assertEqual(record['phase'], 'restart')
        self.assertEqual([entry['label'] for entry in record['attempted']], ['Claude Code', 'Codex'])
        self.assertEqual([updates.watch.version(binaries[name]) for name in ('claude', 'codex')], ['2.0.0', '2.0.0'])
        self.assertEqual(self.store.get('opencode_binary'), selected)
        self.assertEqual({str(path.relative_to(self.installs)): (path.stat().st_mode, path.read_bytes() if path.is_file() else None)
                          for path in self.installs.rglob('*')}, before)
        self.assertIsNotNone(self.store.get('restart_requested_v2'))
        installs = [args for args in self.npm_calls() if args[0] == '--prefix']
        self.assertEqual([args[-1] for args in installs], ['@anthropic-ai/claude-code@2.0.0', '@openai/codex@2.0.0'])

    def test_missing_opencode_updates_torii_claude_and_codex(self):
        self.store.put('opencode_binary', str(self.root / 'gone/opencode'))
        self.assert_broken_opencode_updates_other_tools()

    def test_failed_opencode_version_updates_torii_claude_and_codex(self):
        self.binary.write_text('#!' + sys.executable + '\nimport sys\nsys.exit(1)\n')
        self.assert_broken_opencode_updates_other_tools()

    def test_hanging_opencode_updates_torii_claude_and_codex(self):
        self.binary.write_text('#!' + sys.executable + '\nimport time\ntime.sleep(30)\n')
        version = updates.watch.version
        def read_version(binary, *args, **kwargs):
            if str(binary) == str(self.binary):
                kwargs['timeout'] = .5
            return version(binary, *args, **kwargs)
        with patch.object(updates.watch, 'version', side_effect=read_version):
            self.assert_broken_opencode_updates_other_tools(slow_codex=True)

    def test_nul_opencode_path_updates_torii_claude_and_codex(self):
        self.store.put('opencode_binary', str(self.root) + '\x00opencode')
        self.assert_broken_opencode_updates_other_tools()

    def test_opencode_installation_read_errors_keep_torii_update_available(self):
        installation = updates.installation
        for error in (OSError('unreadable'), RuntimeError('Symlink loop'), ValueError('embedded null byte')):
            with self.subTest(error=type(error).__name__):
                def read(binary, package, *args):
                    if package == 'opencode-ai':
                        raise error
                    return installation(binary, package, *args)
                with patch.object(updates, 'installation', side_effect=read):
                    value = self.inventory(subjects=['fix Torii'])
                self.assertEqual(updates.card(value).splitlines(), [
                    'Update Torii?', 'Torii aaaaaaa → bbbbbbb', '• fix Torii',
                    'OpenCode · cannot read its binary at the saved path; left out of this update.',
                    'Running chats and workers keep going.'])
                self.assertEqual(self.store.get('opencode_binary'), str(self.binary))
