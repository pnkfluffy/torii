import json
import time
from unittest.mock import patch

from coordinator import control_api
from coordinator.formatting import markdown_to_html
from coordinator.health import _plain
from coordinator.store import Store
from tests.test_onboarding import OnboardingFixture
from tests.test_store import update


TOPIC = '-10042:4'


class ControlUITests(OnboardingFixture):
    def latest(self):
        row = dict(self.store.db.execute('SELECT * FROM outbox ORDER BY id DESC LIMIT 1').fetchone())
        row['markup'] = json.loads(row['reply_markup']) if row['reply_markup'] else None
        return row

    def click(self, number, label, user=7, row=None, thread=4):
        row = row or self.latest()
        button = next(button for line in row['markup']['inline_keyboard'] for button in line
                      if button['text'] == label)
        return self.store.accept({'update_id': number, 'callback_query': {
            'id': str(number), 'from': {'id': user}, 'data': button['callback_data'],
            'message': {'message_id': 1000 + row['id'], 'message_thread_id': thread,
                        'chat': {'id': -10042, 'type': 'supergroup'}}}})

    def bind_topic(self):
        with self.store.db:
            self.store.bind(TOPIC, self.projects, 'Test', enabled=True)

    def labels(self, row=None):
        row = row or self.latest()
        return [button['text'] for line in (row['markup'] or {'inline_keyboard': []})['inline_keyboard']
                for button in line]

    def action(self, label):
        row = self.latest()
        button = next(button for line in row['markup']['inline_keyboard'] for button in line
                      if button['text'] == label)
        index = int(button['callback_data'].rsplit(':', 1)[1])
        return self.store.get('control_ui:' + TOPIC)['actions'][index]

    def show(self, name, **kwargs):
        from coordinator.control_ui import view
        with self.store.db:
            return view(self.store, TOPIC, name, **kwargs)

    def deliver(self, row=None):
        row = row or self.latest()
        row = dict(row, telegram_message=row['edit_message'] or 1000 + row['id'])
        self.store.delivered(row['id'], row['telegram_message'])
        return dict(self.latest(), id=row['edit_message'] - 1000) if row['edit_message'] else row

    def account(self, alias='work', email='work@example.com', active=None, signed_in=True, enabled=True, usage=None):
        directory = self.root / alias
        directory.mkdir(exist_ok=True)
        with self.store.db:
            accounts = dict(self.store.get('accounts', {}) or {})
            accounts[alias] = {'config_dir': str(directory), 'enabled': enabled and signed_in and email is not None}
            self.store.put('accounts', accounts)
            status = dict(self.store.get('account_status', {}) or {})
            status[alias] = {'identity': {'email': email, 'logged_in': signed_in} if email else {},
                             'observed_at': time.time(),
                             'usage': usage or {'five_hour': {'utilization': 0, 'resets_at': None}}}
            self.store.put('account_status', status)

    def codex(self, alias='dave', email='dave@example.com', signed_in=True, enabled=True, usage=None, **snapshot):
        directory = self.root / ('codex-' + alias)
        directory.mkdir(exist_ok=True)
        with self.store.db:
            accounts = dict(self.store.get('codex_accounts', {}) or {})
            accounts[alias] = {'config_dir': str(directory), 'enabled': enabled}
            self.store.put('codex_accounts', accounts)
            status = dict(self.store.get('codex_account_status', {}) or {})
            status[alias] = dict({'identity': {'email': email, 'logged_in': True} if signed_in and email else {},
                                  'observed_at': time.time(),
                                  'usage': usage or {'five_hour': {'utilization': 20}, 'seven_day': {'utilization': 45}}},
                                 **snapshot)
            self.store.put('codex_account_status', status)

    def block(self, alias='work', until=None):
        with self.store.db:
            self.store.put('account_blocks',
                           {alias: {'until': until or time.time() + 3600, 'reason': 'quota'}})

    def owner_accounts(self):
        now = time.time()
        self.account('alice', 'alice@example.com', usage={'five_hour': {'utilization': 38},
                                                          'seven_day': {'utilization': 60},
                                                          'seven_day_fable': {'utilization': 12}})
        self.account('bob', 'bob@example.com', usage={'five_hour': {'utilization': 100},
                                                      'seven_day': {'utilization': 69},
                                                      'seven_day_fable': {'utilization': 30}})
        self.account('carol', 'carol@example.com', signed_in=False)
        self.block('bob', now + 2 * 3600 + 10 * 60 + 30)
        self.codex('dave', resets_available=2, reset_expires=now + 12 * 86400 + 30)
        self.codex('codex', signed_in=False)
        with self.store.db:
            self.store.put('codex_auto_switch', True)

    def envelope(self, name, state, created=None, filled=None, task=None, topic=TOPIC, consumer='Codex workers'):
        created = created or time.time()
        with self.store.db:
            cursor = self.store.db.execute(
                '''INSERT INTO envelopes (name,reason,consumer,task,topic,state,created,expires,filled_at,length,
                   fingerprint,updated) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',
                (name, 'Deploy needs it', consumer, task, topic, state, created, created + 600, filled,
                 51 if filled else None, '3f9a' if filled else None, created))
        return cursor.lastrowid

    def secrets(self):
        day = 86400
        start = 1790000000
        stored = self.envelope('DEPLOY_KEY', 'filled', start, start + 60)
        with self.store.db:
            self.store.envelope_event(stored, 'DEPLOY_KEY', 'use', source='worker:88')
            self.store.db.execute("UPDATE envelope_events SET created=? WHERE event='use'", (start + 3 * day,))
        self.envelope('STRIPE_KEY', 'open', start + 3 * day)
        self.envelope('GH_TOKEN', 'expired', start + day)
        self.envelope('OLD_KEY', 'revoked', start, start + 60)
        self.envelope('NPM_TOKEN', 'filled', start, start + 60)
        self.envelope('NPM_TOKEN', 'cancelled', start + 2 * day)

    def test_setup_buttons_reject_other_owner_and_stale_clicks(self):
        self.send(2, '/setup')
        old = self.latest()
        self.assertEqual(self.click(3, 'New project', user=99), 'unauthorized')
        self.assertEqual(self.click(4, 'New project', thread=9), 'stale_callback')
        self.assertEqual(self.click(5, 'New project'), 'control_callback')
        self.assertEqual(self.click(6, 'Existing project', row=old), 'stale_callback')
        self.send(7, 'Button App')
        self.assertEqual(self.store.topic(TOPIC)['cwd'], str(self.projects / 'button-app'))

    def test_setup_root_row_links_accounts_and_projects(self):
        self.send(2, '/setup')
        self.assertEqual(self.labels(), ['New project', 'Existing project', 'Accounts', 'Projects'])
        self.assertEqual(self.click(3, 'Projects'), 'control_callback')
        self.assertEqual(self.labels(), ['New project', 'Projects folder', 'Link this topic'])
        self.send(4, '/setup')
        self.click(5, 'Accounts')
        self.assertTrue(self.latest()['text'].startswith('**Accounts**'))

    def test_settings_account_and_model_forms_survive_reopen(self):
        with self.store.db:
            self.store.put('bot_username', 'torii_test_bot')
        self.send(2, '/setup')
        self.click(3, 'Accounts')
        self.click(4, 'Add Claude')
        self.assertEqual(self.store.get('account_signin')['state'], 'starting')
        self.store.close()
        self.store = Store(self.root / 'state')
        self.click(6, 'Cancel sign-in')
        self.assertEqual(self.store.get('account_signin')['state'], 'cancelled')
        self.click(8, 'Models & Codex ›')
        self.click(9, 'Worker model')
        self.click(10, 'Custom model')
        card = self.deliver()
        self.send(11, 'my-worker-model', reply_to_message={'message_id': card['telegram_message']})
        self.assertEqual(self.store.get('worker_model'), 'my-worker-model')

    def test_buttons_for_retired_commands_answer_that_the_menu_expired(self):
        self.bind_topic()
        self.send(2, '/setup')
        card = self.latest()
        state = self.store.get('control_ui:' + TOPIC)
        retired = [{'command': '/settings'}, {'command': '/settings tasks'}, {'command': '/selection soonest_reset'},
                   {'command': '/account orbit'}, {'command': '/policy'}, {'view': 'model:planning'},
                   {'view': 'account:nobody'}, {'view': 'reset:nobody'}, {'view': 'secret:lower'},
                   {'view': 'revoke:NOPE_KEY'}, {'view': 'accounts:x'}]
        with self.store.db:
            self.store.put('account_selection', 'manual')
            self.store.put('control_ui:' + TOPIC, dict(state, actions=retired))
        before = self.store.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0]
        for index in range(len(retired)):
            outcome = self.store.accept({'update_id': 3 + index, 'callback_query': {
                'id': str(3 + index), 'from': {'id': 7}, 'data': 'torii:%s:%d' % (state['token'], index),
                'message': {'message_id': 1000 + card['id'], 'message_thread_id': 4,
                            'chat': {'id': -10042, 'type': 'supergroup'}}}})
            self.assertEqual(outcome, 'stale_callback', retired[index])
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], before)
        self.assertEqual(self.store.get('account_selection'), 'manual')

    def test_retired_command_button_on_its_own_card_answers_that_the_menu_expired(self):
        self.bind_topic()
        self.send(2, '/setup')
        card = self.latest()
        state = self.store.get('control_ui:' + TOPIC)
        with self.store.db:
            self.store.put('control_ui:' + TOPIC, dict(state, where='/account orbit',
                                                       actions=[{'command': '/account orbit'}]))
        outcome = self.store.accept({'update_id': 3, 'callback_query': {
            'id': '3', 'from': {'id': 7}, 'data': 'torii:%s:0' % state['token'],
            'message': {'message_id': 1000 + card['id'], 'message_thread_id': 4,
                        'chat': {'id': -10042, 'type': 'supergroup'}}}})
        self.assertEqual(outcome, 'stale_callback')

    def test_settings_merges_models_and_codex(self):
        self.send(2, '/accounts')
        self.click(3, 'Models & Codex ›')
        self.assertEqual(self.latest()['text'], '\n'.join([
            '**Models & Codex**', 'Worker model: opus', 'Codex model: Codex CLI default',
            'Coordinator model: service default', 'Codex: on',
            'Worker and Codex changes apply to future dispatches. The coordinator change applies at its next launch.']))
        self.assertEqual(self.labels(), ['Worker model', 'Codex model', 'Coordinator model', 'Turn Codex off',
                                         'Usage policy', 'Back'])
        self.assertEqual([[button['text'] for button in row]
                          for row in self.latest()['markup']['inline_keyboard'][:3]],
                         [['Worker model'], ['Codex model'], ['Coordinator model']])
        self.assertEqual(self.action('Back'), {'view': 'accounts:0'})
        self.assertEqual(self.action('Turn Codex off'),
                         {'op': 'delegation.codex', 'params': {'enabled': False}, 'page': 'settings'})
        self.assertEqual(self.click(4, 'Turn Codex off'), 'control_callback')
        self.assertIs(self.store.get('codex_enabled'), False)
        self.assertIn('Codex: off', self.latest()['text'])
        self.assertIn('Turn Codex on', self.labels())
        self.click(5, 'Usage policy')
        self.assertEqual(self.action('Back'), {'view': 'settings'})
        self.click(6, 'Back')
        self.click(7, 'Worker model')
        self.assertEqual(self.action('Back'), {'view': 'settings'})
        self.click(8, 'Opus 5.5')
        self.assertEqual(self.store.get('worker_model'), 'claude-opus-5-5')
        self.assertIn('**Models & Codex**\nWorker model: claude-opus-5-5', self.latest()['text'])
        self.click(9, 'Back')
        self.assertTrue(self.latest()['text'].startswith('**Accounts**'))

    def test_old_models_and_delegation_views_render_settings(self):
        for name in ('models', 'delegation'):
            self.assertTrue(self.show(name))
            self.assertTrue(self.latest()['text'].startswith('**Models & Codex**'))
            self.assertEqual(self.store.get('control_ui:' + TOPIC)['where'], 'settings')

    def test_usage_policy_edit_and_long_view(self):
        self.bind_topic()
        self.show('settings')
        self.click(4, 'Usage policy')
        self.click(5, 'Edit')
        self.assertEqual(self.action('Cancel'), {'view': 'policy'})
        card = self.deliver()
        self.send(6, 'Use one worker.', reply_to_message={'message_id': card['telegram_message']})
        self.assertEqual((self.store.directory / 'USAGE.md').read_text(), 'Use one worker.')
        self.assertIn('Usage policy\nUse one worker.', self.latest()['text'])
        (self.store.directory / 'USAGE.md').write_text('a' * 5000)
        self.show('policy', prefix='Saved')
        self.assertLessEqual(len(self.latest()['text']), 4096)
        self.assertTrue(self.latest()['text'].endswith('…'))

    def test_policy_form_passes_plain_messages_as_work(self):
        self.bind_topic()
        policy = self.store.directory / 'USAGE.md'
        policy.write_text('Keep the existing policy.')
        self.show('policy')
        self.click(3, 'Edit')
        card = self.deliver()
        state = self.store.get('control_ui:' + TOPIC)
        replies = [{'reply_to_message': {'message_id': 4, 'forum_topic_created': {'name': 'test'}}}, {}]
        for number, extra in enumerate(replies, 5):
            with self.subTest(reply=extra):
                self.assertEqual(self.send(number, 'Please update the README.', **extra)[0], 'queued')
                self.assertEqual(policy.read_text(), 'Keep the existing policy.')
                self.assertEqual(self.store.get('control_ui:' + TOPIC), state)
        self.assertEqual([message['text'] for message in self.store.messages_pending()],
                         ['Please update the README.'] * 2)
        self.assertEqual(self.send(7, 'Use one worker.',
                                   reply_to_message={'message_id': card['telegram_message']})[0], 'control')
        self.assertEqual(policy.read_text(), 'Use one worker.')
        self.assertIsNone(self.store.get('control_ui:' + TOPIC)['pending'])

    def test_retired_primary_button_is_refused_cleanly(self):
        self.bind_topic()
        self.show('settings')
        card = self.latest()
        state = self.store.get('control_ui:' + TOPIC)
        with self.store.db:
            self.store.put('control_ui:' + TOPIC, dict(state, actions=[{
                'op': 'delegation.primary', 'params': {'provider': 'codex'}, 'page': 'delegation'}]))
        outcome = self.store.accept({'update_id': 4, 'callback_query': {
            'id': '4', 'from': {'id': 7}, 'data': 'torii:%s:0' % state['token'],
            'message': {'message_id': 1000 + card['id'], 'message_thread_id': 4,
                        'chat': {'id': -10042, 'type': 'supergroup'}}}})
        self.assertEqual(outcome, 'control_callback')
        self.assertIn('Unknown operation', self.latest()['text'])
        self.assertIn('Codex: on', self.latest()['text'])

    def test_codex_model_presets_and_other_choices(self):
        self.show('settings')
        self.click(4, 'Codex model')
        self.assertEqual(self.labels(), ['GPT-6.1 Sol (default)', 'GPT-6 Luna', 'GPT-6 Astra',
                                         'Custom model', 'Codex cli default', 'Back'])
        for number, (label, model) in enumerate((('GPT-6.1 Sol (default)', 'gpt-6.1-sol'),
                                                  ('GPT-6 Luna', 'gpt-6-luna'),
                                                  ('GPT-6 Astra', 'gpt-6-astra')), 5):
            self.click(number, label)
            self.assertEqual(self.store.get('codex_model'), model)
            self.click(number + 10, 'Codex model')
        self.click(18, 'Codex cli default')
        self.assertIsNone(self.store.get('codex_model'))

    def test_open_jobs_lists_the_open_jobs_of_this_topic(self):
        self.bind_topic()
        self.store.task_create(TOPIC, 'Ship the widget')
        finished = self.store.task_create(TOPIC, 'Old work')
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET status='done' WHERE id=?", (finished['id'],))
        self.show('project')
        self.assertEqual(self.click(3, 'Open jobs'), 'control_callback')
        self.assertEqual(self.latest()['text'], 'Open jobs\nJob 1: Ship the widget')
        self.assertEqual(self.labels(), ['Back'])
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET status='done'")
        self.click(4, 'Back')
        self.click(5, 'Open jobs')
        self.assertEqual(self.latest()['text'], 'Open jobs\nNo open jobs in this channel.')

    def test_open_jobs_stays_one_message_when_a_topic_has_many_open_jobs(self):
        self.bind_topic()
        for number in range(1, 31):
            self.store.task_create(TOPIC, 'Job title ' + 'x' * 150)
        self.show('jobs')
        lines = self.latest()['text'].split('\n')
        self.assertEqual(len(lines), 27)
        self.assertTrue(lines[25].startswith('Job 25: '))
        self.assertEqual(lines[26], '…and 5 more open jobs.')
        self.assertLess(len(self.latest()['text']), 4096)

    def test_setup_menu_has_no_worker_limit(self):
        self.send(2, '/setup')
        self.assertNotIn('Advanced settings', self.labels())
        self.assertNotIn('Worker limit', self.latest()['text'])
        self.assertIsNone(self.store.get('worker_limit'))

    def test_goal_bypasses_an_open_settings_form(self):
        self.bind_topic()
        self.show('model:worker')
        self.click(3, 'Custom model')
        card = self.deliver()
        state = self.store.get('control_ui:' + TOPIC)
        self.assertEqual(self.store.accept(update(5, '/goal finish checks')), 'queued')
        self.assertEqual(self.store.messages_pending()[0]['text'], '/goal finish checks')
        self.assertEqual(self.store.accept(update(6, '/goal finish checks',
                                                  reply_to_message={'message_id': card['telegram_message']})), 'queued')
        self.assertEqual(self.store.messages_pending()[1]['text'],
                         'Replying to: ' + card['text'] + '\n/goal finish checks')
        self.assertNotEqual(self.store.get('worker_model'), '/goal finish checks')
        self.assertEqual(self.store.get('control_ui:' + TOPIC), state)

    def test_form_navigation_edits_the_tapped_card_in_place(self):
        self.send(2, '/setup')
        card = self.latest()
        self.store.delivered(card['id'], 1000 + card['id'])
        self.click(3, 'Accounts')
        page = self.latest()
        self.assertEqual(page['edit_message'], 1000 + card['id'])
        self.assertIsNone(page['reply_to'])
        self.store.delivered(page['id'], 1000 + card['id'])
        self.assertIsNone(self.store.db.execute('SELECT telegram_message FROM outbox WHERE id=?',
                                                (card['id'],)).fetchone()[0])
        with self.store.db:
            self.store.put('bot_username', 'torii_test_bot')
        self.click(4, 'Add Claude', row=dict(page, id=card['id']))
        self.assertEqual(self.latest()['edit_message'], 1000 + card['id'])
        self.assertEqual(self.store.get('account_signin')['topic'], TOPIC)

    def test_new_command_still_sends_a_new_card(self):
        self.send(2, '/setup')
        card = self.latest()
        self.store.delivered(card['id'], 1000 + card['id'])
        self.send(3, '/setup')
        self.assertIsNone(self.latest()['edit_message'])

    def test_forum_reply_to_unknown_message_reaches_coordinator_during_open_form(self):
        self.bind_topic()
        self.show('model:worker')
        self.click(3, 'Custom model')
        root = {'message_id': 4, 'forum_topic_created': {'name': 'test'}}
        self.assertEqual(self.store.accept(update(5, 'analyse usage', reply_to_message=root)), 'queued')
        self.assertEqual(self.store.messages_pending()[0]['text'], 'analyse usage')

    def test_reply_to_non_card_message_reaches_coordinator_during_open_form(self):
        self.bind_topic()
        self.show('model:worker')
        self.click(3, 'Custom model')
        self.deliver()
        state = self.store.get('control_ui:' + TOPIC)
        with self.store.db:
            report = self.store.enqueue_report(TOPIC, 'Agent: done.')
        self.store.delivered(report, 900)
        for number, reply in ((5, 899), (6, 900)):
            with self.subTest(reply=reply):
                self.assertEqual(self.store.accept(update(number, 'analyse usage',
                                                          reply_to_message={'message_id': reply})), 'queued')
                self.assertEqual(self.store.get('control_ui:' + TOPIC), state)
        self.assertEqual([message['text'] for message in self.store.messages_pending()],
                         ['analyse usage', 'Replying to: Agent: done.\nanalyse usage'])

    def test_service_restart_queues_one_request_without_a_confirmation_card(self):
        self.bind_topic()
        outcome, text = self.send(2, '/service restart update code')
        self.assertEqual(outcome, 'control')
        self.assertIn('Restart requested', text)
        self.assertIsNone(self.store.get('restart_requested_v2'))
        self.assertIsNone(self.store.get('restart_card'))
        request = self.store.db.execute('SELECT op,params FROM service_requests').fetchone()
        self.assertEqual(request['op'], 'service.restart')
        self.assertEqual(json.loads(request['params'])['reason'], 'update code')

    def test_add_account_asks_for_no_name(self):
        self.bind_topic()
        with self.store.db:
            self.store.put('bot_username', 'torii_test_bot')
        self.send(2, '/accounts')
        self.click(3, 'Add Claude')
        self.assertTrue(self.latest()['text'].startswith('Starting Claude sign-in'))
        self.assertIsNone(self.store.get('control_ui:' + TOPIC)['pending'])
        self.assertEqual(self.store.accept(update(5, 'someone@example.com')), 'queued')
        self.assertIsNone(self.store.get('account_signin')['target'])
        self.assertEqual(self.store.get('accounts') or {}, {})

    def test_old_form_reply_cannot_change_new_form(self):
        self.bind_topic()
        self.show('model:worker')
        self.click(5, 'Custom model')
        old = self.latest()
        self.click(6, 'Cancel')
        self.click(7, 'Worker model')
        self.click(8, 'Custom model')
        self.store.delivered(old['id'], 900)
        outcome = self.store.accept(update(9, 'wrong', reply_to_message={'message_id': 900}))
        self.assertEqual(outcome, 'control')
        self.assertNotEqual(self.store.get('worker_model'), 'wrong')
        self.assertEqual(self.latest()['text'], 'This form has expired. Open the command again.')

    def test_every_tap_and_form_answer_edits_the_one_settings_message(self):
        self.send(2, '/accounts')
        card = self.deliver()
        message = 1000 + card['id']
        taps = [(3, 'Models & Codex ›'), (4, 'Coordinator model'), (5, 'Back'), (6, 'Back'), (7, 'Models & Codex ›')]
        for number, label in taps:
            self.assertEqual(self.click(number, label, row=card), 'control_callback')
            page = self.latest()
            self.assertEqual((page['edit_message'], page['reply_to']), (message, None), label)
            card = self.deliver(page)
        self.click(10, 'Worker model', row=card)
        card = self.deliver()
        self.click(11, 'Custom model', row=card)
        card = self.deliver()
        self.send(12, 'bad model id!', reply_to_message={'message_id': card['telegram_message']})
        self.assertEqual(self.latest()['edit_message'], message)
        card = self.deliver()
        self.send(13, 'my-worker-model', reply_to_message={'message_id': card['telegram_message']})
        self.assertEqual(self.store.get('worker_model'), 'my-worker-model')
        answer = self.latest()
        self.assertEqual((answer['edit_message'], answer['reply_to']), (message, None))
        self.assertIn('Worker model: my-worker-model', answer['text'])
        self.assertEqual(self.store.db.execute('SELECT COUNT(DISTINCT COALESCE(edit_message, telegram_message)) '
                                               'FROM outbox WHERE delivered=1').fetchone()[0], 1)

    def test_add_account_and_cancel_sign_in_edit_the_accounts_message(self):
        with self.store.db:
            self.store.put('bot_username', 'torii_test_bot')
        self.send(2, '/accounts')
        card = self.deliver()
        message = 1000 + card['id']
        self.click(4, 'Add ChatGPT', row=card)
        page = self.latest()
        self.assertEqual((page['edit_message'], page['reply_to']), (message, None))
        self.assertTrue(page['text'].startswith('Starting Codex sign-in'))
        self.assertIn('Sign-in open: ChatGPT, starting.', page['text'])
        self.assertEqual(self.store.get('account_signin')['provider'], 'codex')
        self.assertIn('Cancel sign-in', self.labels(page))
        self.assertNotIn('Add Claude', self.labels(page))
        card = self.deliver(page)
        self.click(5, 'Cancel sign-in', row=card)
        page = self.latest()
        self.assertEqual((page['edit_message'], page['reply_to']), (message, None))
        self.assertEqual(self.store.get('account_signin')['state'], 'cancelled')

    def test_accounts_card_lists_every_account_with_marks_and_tags(self):
        self.owner_accounts()
        with self.store.db:
            self.store.put('account_signin', {'topic': TOPIC, 'state': 'approve', 'provider': 'codex',
                                              'target': None})
        with patch('coordinator.accounts.temporary_local_switch_thresholds', return_value={'bob': 0.9}):
            self.send(2, '/accounts')
        self.assertEqual(self.latest()['text'], '\n'.join([
            '**Accounts** · Torii switches at 95% used',
            '',
            '**Claude** (left: 5h · week · Fable)',
            '🟢 alice@example.com: 62% · 40% · 88% · next',
            '🔴 bob@example.com: 0% · 31% · 70%, back in 2h 10m · reserve 10%',
            '⚪ carol@example.com: signed out',
            '',
            '**ChatGPT** · auto-switch on (left: 5h · week)',
            '🟢 dave@example.com: 80% · 55% · active · 2 resets, first expires in 12d',
            '⚪ codex: signed out',
            '',
            'Sign-in open: ChatGPT, waiting for the device code.']))
        self.assertEqual(self.labels(), ['Sign in carol@example.com', 'Sign in codex', 'Reset dave@example.com (2)',
                                         'Claude reset ↗', 'Cancel sign-in', 'Turn off ChatGPT auto-switch',
                                         'Models & Codex ›'])
        self.assertEqual(self.action('Sign in carol@example.com'), {'view': 'account:carol'})
        self.assertEqual(self.action('Reset dave@example.com (2)'), {'view': 'reset:dave'})
        self.assertEqual(self.action('Cancel sign-in'),
                         {'op': 'account.signin_cancel', 'params': {}, 'page': 'accounts:0'})
        self.assertEqual(self.action('Turn off ChatGPT auto-switch'),
                         {'op': 'accounts.codex_auto', 'params': {'enabled': False}, 'page': 'accounts:0'})
        self.assertEqual(self.store.get('control_ui:' + TOPIC)['where'], 'accounts:0')

    def test_claude_reset_is_a_url_button_that_uses_no_action(self):
        from coordinator.control_ui import CLAUDE_USAGE
        self.account('alice', 'alice@example.com', usage={'five_hour': {'utilization': 96}})
        self.account('erin', 'erin@example.com', signed_in=False)
        self.send(2, '/accounts')
        buttons = [button for line in self.latest()['markup']['inline_keyboard'] for button in line]
        link = next(button for button in buttons if button['text'] == 'Claude reset ↗')
        self.assertEqual(link, {'text': 'Claude reset ↗', 'url': CLAUDE_USAGE})
        data = [button['callback_data'] for button in buttons if 'callback_data' in button]
        self.assertEqual([int(each.rsplit(':', 1)[1]) for each in data], list(range(len(data))))
        self.assertEqual(len(self.store.get('control_ui:' + TOPIC)['actions']), len(data))
        self.account('alice', 'alice@example.com', usage={'five_hour': {'utilization': 50}})
        self.show('accounts:0')
        self.assertNotIn('Claude reset ↗', self.labels())

    def test_chatgpt_use_rows_appear_only_with_auto_switch_off(self):
        self.codex('dave')
        self.codex('frank', 'frank@example.com')
        with self.store.db:
            self.store.put('codex_active_account', 'dave')
        self.show('accounts:0')
        self.assertIn('**ChatGPT** · auto-switch off (left: 5h · week)', self.latest()['text'])
        self.assertIn('🟢 dave@example.com: 80% · 55% · active', self.latest()['text'])
        self.assertEqual(self.labels(), ['Use frank@example.com for ChatGPT', 'Add Claude', 'Add ChatGPT',
                                         'Turn on ChatGPT auto-switch', 'Models & Codex ›'])
        self.assertEqual(self.click(3, 'Use frank@example.com for ChatGPT'), 'control_callback')
        self.assertEqual(self.store.get('codex_active_account'), 'frank')
        self.assertIn('Use dave@example.com for ChatGPT', self.labels())
        self.click(4, 'Turn on ChatGPT auto-switch')
        self.assertIs(self.store.get('codex_auto_switch'), True)
        self.assertNotIn('Use dave@example.com for ChatGPT', self.labels())
        self.assertIn('Turn off ChatGPT auto-switch', self.labels())

    def test_accounts_empty_state(self):
        self.send(2, '/accounts')
        self.assertEqual(self.latest()['text'], '**Accounts**\nNo accounts yet. Add one to start work. '
                                                'Project setup works before sign-in.')
        self.assertEqual(self.labels(), ['Add Claude', 'Add ChatGPT', 'Models & Codex ›'])
        self.assertEqual(self.action('Add ChatGPT'),
                         {'op': 'account.add', 'params': {'provider': 'codex'}, 'page': 'accounts:0'})

    def test_no_claude_account_can_take_work_and_off_accounts(self):
        self.account('work', 'work@example.com')
        self.block('work', time.time() + 3600 + 30)
        self.account('spare', 'spare@example.com', enabled=False)
        self.show('accounts:0')
        text = self.latest()['text']
        self.assertIn('⚫ spare@example.com: off', text)
        self.assertIn('🔴 work@example.com: 100% · ? · ?, back in 1h 0m', text)
        self.assertTrue(text.endswith('\n\nNo Claude account can take work now.'))
        self.assertEqual(self.labels()[0], 'Turn on spare@example.com')

    def test_shared_claude_login_is_tagged(self):
        self.account('work', 'same@example.com')
        self.account('copy', 'same@example.com', signed_in=False)
        self.show('accounts:0')
        self.assertIn('⚪ same@example.com: signed out · same login as same@example.com', self.latest()['text'])

    def test_accounts_paginate_repair_rows_at_nine(self):
        for number in range(9):
            self.account('gone%d' % number, 'gone%d@example.com' % number, signed_in=False)
        self.send(2, '/accounts')
        self.assertEqual(self.labels(), ['Sign in gone%d@example.com' % number for number in range(8)]
                         + ['Add Claude', 'Add ChatGPT', 'Models & Codex ›', 'Next ›'])
        self.click(3, 'Next ›')
        self.assertEqual(self.labels(), ['Sign in gone8@example.com', 'Add Claude', 'Add ChatGPT',
                                         'Models & Codex ›', '‹ Previous'])
        self.assertEqual(self.store.get('control_ui:' + TOPIC)['where'], 'accounts:1')
        self.click(4, 'Add Claude')
        self.assertEqual(self.store.get('control_ui:' + TOPIC)['where'], 'accounts:1')

    def test_long_button_labels_are_clipped(self):
        self.account('long', 'a-very-long-mailbox-name-for-testing@example.com', signed_in=False)
        self.show('accounts:0')
        label = self.labels()[0]
        self.assertEqual(len(label), 40)
        self.assertTrue(label.startswith('Sign in a-very-long') and label.endswith('…'))

    def test_sixty_accounts_clip_on_whole_lines(self):
        for number in range(60):
            self.account('acct%02d' % number, 'owner-%02d-%s@example.com' % (number, 'x' * 40))
        self.show('accounts:0')
        text = self.latest()['text']
        self.assertLessEqual(len(text), 4000)
        last = text.split('\n')[-1]
        self.assertRegex(last, r'\A…and \d+ more\.\Z')
        shown = sum(1 for line in text.split('\n') if line.startswith('🟢'))
        self.assertEqual(int(last.split()[1]), 60 - shown)

    def test_signed_out_account_card_signs_in_or_removes(self):
        self.account('work', 'work@example.com')
        self.account('carol', 'carol@example.com', signed_in=False)
        self.show('accounts:0')
        self.click(3, 'Sign in carol@example.com')
        self.assertEqual(self.latest()['text'], 'carol@example.com (Claude) is signed out.\n'
                                                'Sign in with the same Claude account to bring it back. '
                                                'Its alias, history and settings stay.\n'
                                                'Remove takes it off this list. Its folder stays on this Mac.')
        self.assertEqual(self.labels(), ['Sign in', 'Remove', 'Back'])
        self.assertEqual(self.action('Sign in'),
                         {'op': 'account.add', 'params': {'alias': 'carol'}, 'page': 'accounts:0'})
        self.assertEqual(self.action('Back'), {'view': 'accounts:0'})
        self.click(4, 'Remove')
        self.assertEqual(self.latest()['text'], "Remove carol@example.com from Torii? It's signed out, so no work "
                                                "uses it. Its folder stays on this Mac and Torii won't re-add it on "
                                                'its own.')
        self.assertEqual(self.labels(), ['Remove', 'Back'])
        self.assertEqual(self.action('Back'), {'view': 'account:carol'})
        self.assertEqual(self.click(5, 'Remove'), 'control_callback')
        self.assertTrue(self.latest()['text'].startswith('Removed carol@example.com.\n\n**Accounts**'))
        self.assertNotIn('carol', self.store.get('accounts'))
        self.assertIn(str((self.root / 'carol').resolve()), self.store.get('account_dirs_removed'))
        self.assertTrue((self.root / 'carol').is_dir())
        self.assertFalse(self.show('remove:work'))
        self.assertFalse(self.show('account:work'))

    def test_off_account_card_turns_it_on(self):
        self.codex('dave', enabled=False)
        self.show('accounts:0')
        self.assertIn('⚫ dave@example.com: off', self.latest()['text'])
        self.click(3, 'Turn on dave@example.com')
        self.assertEqual(self.latest()['text'],
                         "dave@example.com (ChatGPT) is signed in but turned off, so Torii doesn't use it.")
        self.assertEqual(self.labels(), ['Turn on', 'Back'])
        self.assertFalse(self.show('remove:dave'))
        self.show('account:dave')
        self.click(4, 'Turn on')
        self.assertIs(self.store.get('codex_accounts')['dave']['enabled'], True)
        self.assertIn('🟢 dave@example.com', self.latest()['text'])

    def test_reset_confirm_when_the_rule_is_met(self):
        now = time.time()
        self.codex('dave', usage={'five_hour': {'utilization': 97, 'resets_at': now + 3600 + 12 * 60 + 30},
                                  'seven_day': {'utilization': 60}},
                   resets_available=2, reset_expires=now + 12 * 86400 + 30)
        with self.store.db:
            self.store.put('codex_auto_switch', True)
        self.show('accounts:0')
        self.click(3, 'Reset dave@example.com (2)')
        self.assertEqual(self.latest()['text'], 'Spend 1 of 2 banked resets on dave@example.com?\n'
                                                'Now: 5h 3% left · week 40% left. Limited, back in 1h 12m.\n'
                                                "It refills the 5-hour and weekly limits right away. It can't be "
                                                'undone.\nTorii spends the one that expires first (in 12d).')
        self.assertEqual(self.labels(), ['Spend reset', 'Back'])
        self.assertEqual(self.action('Back'), {'view': 'accounts:0'})
        self.assertEqual(self.click(4, 'Spend reset'), 'control_callback')
        self.assertTrue(self.latest()['text'].startswith(
            'Banked reset queued. Torii will send the result here.\n\n**Accounts**'))
        request = self.store.db.execute('SELECT op,params FROM service_requests').fetchone()
        self.assertEqual((request['op'], json.loads(request['params'])['alias']), ('account.codex_reset', 'dave'))

    def test_reset_confirm_without_expiry_lets_codex_pick(self):
        self.codex('dave', usage_allowed=False, resets_available=1)
        self.show('reset:dave')
        self.assertTrue(self.latest()['text'].startswith('Spend 1 of 1 banked reset on dave@example.com?'))
        self.assertTrue(self.latest()['text'].endswith('\nCodex picks which one.'))

    def test_reset_view_explains_when_no_reset_is_needed(self):
        self.codex('dave', usage={'five_hour': {'utilization': 38}, 'seven_day': {'utilization': 60}},
                   resets_available=2)
        self.show('reset:dave')
        self.assertEqual(self.latest()['text'], "dave@example.com doesn't need a reset yet: 5h 62% left · week 40% "
                                                'left.\nTorii spends a banked reset only when the account is limited '
                                                'or a meter is at 95% or more. Your 2 resets stay banked.')
        self.assertEqual(self.labels(), ['Back'])

    def test_reset_view_is_stale_without_a_banked_reset(self):
        self.codex('dave', resets_available=0)
        self.codex('frank', 'frank@example.com', signed_in=False, resets_available=2)
        self.account('work')
        for name in ('reset:dave', 'reset:frank', 'reset:work', 'reset:nobody'):
            self.assertFalse(self.show(name), name)

    def test_typed_commands_with_arguments_render_the_top_card_with_a_hint(self):
        from coordinator.control_ui import NO_ARGUMENTS, control_report
        self.account('work')
        with self.store.db:
            control_report(self.store, TOPIC, 'ignored', '/accounts', ['show', 'work'], 2)
        self.assertTrue(self.latest()['text'].startswith(NO_ARGUMENTS + '\n\n**Accounts** · Torii switches'))
        with self.store.db:
            control_report(self.store, TOPIC, 'ignored', '/secrets', ['rotate', 'DEPLOY_KEY'], 3)
        self.assertTrue(self.latest()['text'].startswith(NO_ARGUMENTS + '\n\n**Secrets**'))
        with self.store.db:
            control_report(self.store, TOPIC, 'New projects will be created under /x', '/projects', ['root', '/x'], 4)
        self.assertTrue(self.latest()['text'].startswith(NO_ARGUMENTS + '\n\n**Projects** in '))
        self.assertEqual(self.labels(), ['New project', 'Projects folder', 'Link this topic'])
        with self.store.db:
            control_report(self.store, TOPIC, 'ignored', '/help', ['me'], 5)
        self.assertTrue(self.latest()['text'].startswith(NO_ARGUMENTS + '\n\nTorii runs'))
        with self.store.db:
            control_report(self.store, TOPIC, 'ignored', '/health', ['now'], 6)
        self.assertTrue(self.latest()['text'].startswith(NO_ARGUMENTS + '\n\n'))
        self.assertNotIn('ignored', self.latest()['text'])
        self.assertEqual(self.labels(), ['Accounts'])

    def test_health_card_links_accounts(self):
        self.send(2, '/health')
        self.assertEqual(self.labels(), ['Accounts'])
        self.click(3, 'Accounts')
        self.assertTrue(self.latest()['text'].startswith('**Accounts**'))

    def test_projects_card_buttons_depend_on_the_topic(self):
        self.send(2, '/projects')
        self.assertEqual(self.labels(), ['New project', 'Projects folder', 'Link this topic'])
        self.assertEqual(self.action('Link this topic'), {'command': '/setup'})
        self.bind_topic()
        self.send(3, '/projects')
        self.assertEqual(self.labels(), ['New project', 'Projects folder', 'This topic ›'])
        with self.store.db:
            self.store.put('control_topic', TOPIC)
        self.send(4, '/projects')
        self.assertEqual(self.labels(), ['New project', 'Projects folder'])

    def test_projects_shows_no_link_when_the_topic_cannot_bind(self):
        self.store.task_create(TOPIC, 'Unlinked work')
        self.send(2, '/projects')
        self.assertEqual(self.labels(), ['New project', 'Projects folder'])

    def test_projects_escapes_folder_and_topic_names(self):
        root = self.projects / 'root_*[folder]*'
        root.mkdir()
        (root / 'a*b*c').mkdir()
        project = root / 'my__lib__'
        project.mkdir()
        with self.store.db:
            self.store.put('projects_root', str(root))
            self.store.bind(TOPIC, project, '*Beta* site_v2 | R&D', enabled=True)
        self.send(2, '/projects')
        self.assertEqual(markdown_to_html(self.latest()['text']), '\n'.join([
            '<b>Projects</b> in ' + str(root),
            '• a*b*c', '• my__lib__ — linked to *Beta* site_v2 | R&amp;D', '',
            'This topic: *Beta* site_v2 | R&amp;D · ' + str(project) + ' · accepting work']))
        self.show('project')
        self.assertEqual(markdown_to_html(self.latest()['text']),
                         '<b>This topic</b> · *Beta* site_v2 | R&amp;D\n' + str(project)
                         + '\nThis channel accepts work.')

    def check_new_project_form_passes_work(self, **extra):
        self.bind_topic()
        self.send(2, '/projects')
        card = self.deliver()
        self.click(3, 'New project', row=card)
        card = self.deliver()
        state = self.store.get('control_ui:' + TOPIC)
        topics = self.store.topics()
        with self.store.db:
            self.store.enqueue_report(TOPIC, 'Agent: done with the fix.')
        text = 'Thanks, please also update the README'
        self.assertEqual(self.send(9, text, **extra)[0], 'queued')
        self.assertEqual(self.store.messages_pending()[0]['text'], text)
        self.assertEqual(list(self.projects.iterdir()), [])
        self.assertEqual(self.store.get('group_projects', []), [])
        self.assertEqual(self.store.topics(), topics)
        self.assertEqual(self.store.get('control_ui:' + TOPIC), state)
        self.assertEqual(self.send(10, 'My Website',
                                   reply_to_message={'message_id': card['telegram_message']})[0], 'control')
        self.assertEqual(self.store.get('group_projects'), [
            {'name': 'My Website', 'cwd': str(self.projects / 'my-website'), 'source': TOPIC, 'release_held': False}])
        self.assertTrue((self.projects / 'my-website' / '.git').is_dir())
        page = self.latest()
        self.assertTrue(page['text'].startswith('Creating the My Website topic.\n\n**Projects** in '))
        self.assertEqual(page['edit_message'], card['telegram_message'])
        self.assertEqual(self.labels(page), ['New project', 'Projects folder', 'This topic ›'])
        self.assertIsNone(self.store.get('control_ui:' + TOPIC)['pending'])

    def test_new_project_form_passes_topic_root_reply_as_work(self):
        self.check_new_project_form_passes_work(
            reply_to_message={'message_id': 4, 'forum_topic_created': {'name': 'test'}})

    def test_new_project_form_passes_message_without_reply_as_work(self):
        self.check_new_project_form_passes_work()

    def test_new_project_creates_a_topic_and_redraws_projects(self):
        self.send(2, '/projects')
        card = self.deliver()
        self.click(3, 'New project', row=card)
        self.assertEqual(self.latest()['text'],
                         "Reply to this message with the new project's name, for example My Website. Torii makes a folder in "
                         + _plain(str(self.projects)) + ', runs git init, and opens a topic for it.')
        self.assertEqual(self.action('Cancel'), {'command': '/projects'})
        card = self.deliver()
        before = self.store.get('project_setup:' + TOPIC)
        self.send(4, 'bad/name', reply_to_message={'message_id': card['telegram_message']})
        self.assertTrue(self.latest()['text'].startswith(
            'Send a project name of 1–80 characters, without slashes. Example: My Website.\n\nReply to this message with the new'))
        self.assertEqual(self.latest()['edit_message'], card['id'] + 1000)
        self.assertEqual(self.store.get('project_setup:' + TOPIC), before)
        self.assertEqual(self.store.get('group_projects', []), [])
        card = self.deliver()
        self.send(5, 'My Website', reply_to_message={'message_id': card['telegram_message']})
        self.assertEqual([entry['name'] for entry in self.store.get('group_projects')], ['My Website'])
        self.assertTrue((self.projects / 'my-website' / '.git').is_dir())
        page = self.latest()
        self.assertTrue(page['text'].startswith('Creating the My Website topic.\n\n**Projects** in '))
        self.assertEqual(page['edit_message'], card['id'] + 1000)
        self.assertEqual(self.labels(page), ['New project', 'Projects folder', 'Link this topic'])
        self.assertIsNone(self.store.get('control_ui:' + TOPIC)['pending'])

    def test_projects_folder_runs_the_projects_root_op(self):
        other = self.root / 'elsewhere'
        other.mkdir()
        self.send(2, '/projects')
        self.click(3, 'Projects folder')
        self.assertEqual(self.action('Cancel'), {'command': '/projects'})
        card = self.deliver()
        self.send(4, str(self.root / 'missing'), reply_to_message={'message_id': card['telegram_message']})
        self.assertTrue(self.latest()['text'].startswith('That folder is unavailable.'))
        card = self.deliver()
        self.send(5, str(other), reply_to_message={'message_id': card['telegram_message']})
        self.assertEqual(self.store.get('projects_root'), str(other))
        self.assertTrue(self.latest()['text'].startswith('New projects will be created under '))
        self.assertEqual(self.labels(), ['New project', 'Projects folder', 'Link this topic'])
        self.assertEqual(self.store.get('control_ui:' + TOPIC)['where'], '/projects')

    def test_this_topic_changes_folder_with_the_topic_folder_op(self):
        self.bind_topic()
        other = self.root / 'other'
        other.mkdir()
        self.send(2, '/projects')
        self.click(3, 'This topic ›')
        self.assertEqual(self.latest()['text'], '**This topic** · Test\n' + _plain(str(self.projects))
                         + '\nThis channel accepts work.')
        self.assertEqual(self.labels(), ['Change folder', 'Open jobs', 'Back'])
        self.assertEqual(self.action('Back'), {'command': '/projects'})
        self.click(4, 'Change folder')
        self.assertEqual(self.action('Cancel'), {'view': 'project'})
        card = self.deliver()
        with patch('coordinator.control_api.call', wraps=control_api.call) as call:
            self.send(5, str(other), reply_to_message={'message_id': card['telegram_message']})
        self.assertEqual([each.args[1:3] for each in call.call_args_list],
                         [('topic.folder', {'project': str(other)})])
        self.assertEqual(self.store.topic(TOPIC)['cwd'], str(other))
        self.assertTrue(self.latest()['text'].endswith('**This topic** · Test\n' + _plain(str(other))
                                                       + '\nThis channel accepts work.'))
        self.click(6, 'Back')
        self.assertEqual(self.labels(), ['New project', 'Projects folder', 'This topic ›'])

    def test_secrets_list_marks_every_state(self):
        self.secrets()
        self.send(2, '/secrets')
        self.assertEqual(self.latest()['text'], '\n'.join([
            '**Secrets** · 5',
            '🟢 DEPLOY_KEY — filled 2026-09-21, last used 2026-09-24 by worker 88',
            '⚪ GH_TOKEN — expired, nothing stored',
            '⚪ NPM_TOKEN — cancelled, previous value still stored',
            '🔴 OLD_KEY — revoked',
            '🟡 STRIPE_KEY — waiting for you, asked 2026-09-24',
            'Tap a name to rotate, revoke or ask again.']))
        self.assertEqual([[button['text'] for button in line] for line in self.latest()['markup']['inline_keyboard']],
                         [['DEPLOY_KEY', 'GH_TOKEN'], ['NPM_TOKEN', 'OLD_KEY'], ['STRIPE_KEY']])
        self.assertEqual(self.action('DEPLOY_KEY'), {'view': 'secret:DEPLOY_KEY'})

    def test_secrets_empty_state(self):
        self.send(2, '/secrets')
        self.assertEqual(self.latest()['text'], '**Secrets**\nNo secrets yet. When a job needs a key, its agent asks '
                                                'with an envelope card and the name shows up here.')
        self.assertIsNone(self.latest()['markup'])

    def test_secrets_paginate_eight_names(self):
        for number in range(9):
            self.envelope('KEY_%d' % number, 'expired')
        self.show('secrets:0')
        self.assertEqual(self.labels(), ['KEY_%d' % number for number in range(8)] + ['Next ›'])
        self.click(3, 'Next ›')
        self.assertEqual(self.labels(), ['KEY_8', '‹ Previous'])
        self.assertNotIn('Back', self.labels())

    def test_eighty_secrets_clip_on_whole_lines(self):
        for number in range(80):
            self.envelope('SECRET_NUMBER_%02d_%s' % (number, 'X' * 30), 'filled', filled=time.time())
        self.show('secrets:0')
        text = self.latest()['text']
        self.assertLessEqual(len(text), 4000)
        lines = text.split('\n')
        self.assertEqual(lines[-1], 'Tap a name to rotate, revoke or ask again.')
        self.assertRegex(lines[-2], r'\A…and \d+ more\.\Z')

    def test_filled_secret_card_rotates_in_this_topic_and_confirms_revoke(self):
        self.bind_topic()
        task = self.store.task_create(TOPIC, 'Deploy')
        with self.store.db:
            self.store.put('bot_username', 'torii_test_bot')
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('-10042:9',-10042,9,'Other','')")
        start = 1790000000
        stored = self.envelope('DEPLOY_KEY', 'filled', start, start + 60, task=task['id'], topic='-10042:9')
        with self.store.db:
            self.store.envelope_event(stored, 'DEPLOY_KEY', 'use', source='worker:88')
            self.store.db.execute("UPDATE envelope_events SET created=? WHERE event='use'", (start + 3 * 86400,))
        self.show('secret:DEPLOY_KEY')
        self.assertEqual(self.latest()['text'], '**DEPLOY_KEY** · filled\nLength 51 · fingerprint 3f9a · filled '
                                                '2026-09-21\nFor: Codex workers · job %d\nLast use: 2026-09-24 by '
                                                'worker 88' % task['number'])
        self.assertEqual(self.labels(), ['Rotate', 'Revoke', 'Back'])
        self.assertEqual(self.action('Rotate'), {'op': 'secret.rotate', 'params': {'name': 'DEPLOY_KEY', 'topic': TOPIC},
                                                 'page': 'secret:DEPLOY_KEY'})
        self.assertEqual(self.action('Back'), {'view': 'secrets:0'})
        self.click(3, 'Revoke')
        self.assertEqual(self.latest()['text'], 'Revoke DEPLOY_KEY? This deletes the stored value. Workers already '
                                                'running keep their copy until they exit.')
        self.assertEqual(self.labels(), ['Revoke', 'Back'])
        self.assertEqual(self.action('Back'), {'view': 'secret:DEPLOY_KEY'})
        self.assertEqual(self.click(4, 'Revoke'), 'control_callback')
        self.assertTrue(self.latest()['text'].startswith('Queued for the running service.\n\n**DEPLOY_KEY** · filled'))
        request = self.store.db.execute('SELECT op,params FROM service_requests').fetchone()
        self.assertEqual((request['op'], json.loads(request['params'])), ('secret.revoke', {'name': 'DEPLOY_KEY'}))
        self.assertEqual(self.click(5, 'Rotate'), 'control_callback')
        latest = self.store.db.execute('SELECT topic,state FROM envelopes ORDER BY id DESC LIMIT 1').fetchone()
        self.assertEqual((latest['topic'], latest['state']), (TOPIC, 'open'))
        self.assertTrue(self.latest()['text'].startswith('Envelope for DEPLOY_KEY posted again.'))
        self.assertIn('Its envelope card is waiting in Test.', self.latest()['text'])
        self.assertEqual(self.labels(), ['Back'])

    def test_closed_secret_cards_offer_ask_again(self):
        self.secrets()
        self.show('secret:NPM_TOKEN')
        self.assertEqual(self.labels(), ['Ask again', 'Revoke', 'Back'])
        self.assertEqual(self.latest()['text'], '**NPM_TOKEN** · cancelled\nFor: Codex workers · any job')
        self.show('secret:GH_TOKEN')
        self.assertEqual(self.labels(), ['Ask again', 'Back'])
        self.assertFalse(self.show('revoke:GH_TOKEN'))
        self.show('secret:OLD_KEY')
        self.assertEqual(self.labels(), ['Ask again', 'Back'])
        self.assertFalse(self.show('revoke:OLD_KEY'))
        self.show('secret:STRIPE_KEY')
        self.assertEqual(self.labels(), ['Back'])
        self.assertIn('Its envelope card is waiting in Unbound project.', self.latest()['text'])
        for name in ('secret:lower', 'secret:MISSING_KEY', 'secret:PATH'):
            self.assertFalse(self.show(name), name)

    def test_revoke_in_pair_only_mode_shows_the_refusal(self):
        self.secrets()
        with self.store.db:
            self.store.put('pair_only', True)
        self.show('revoke:DEPLOY_KEY')
        self.click(3, 'Revoke')
        self.assertTrue(self.latest()['text'].startswith('The service runs in pair-only mode'))
        self.assertIsNone(self.store.db.execute('SELECT 1 FROM service_requests').fetchone())

    def test_signin_retry_keeps_the_target(self):
        from coordinator.signin import CODEX_RETRY_DATA, RETRY_DATA, RETRY_KEY
        self.bind_topic()
        self.account('carol', 'carol@example.com', signed_in=False)
        with self.store.db:
            self.store.put('bot_username', 'torii_test_bot')
            self.store.put(RETRY_KEY, {'provider': 'claude', 'target': 'carol'})
        outcome = self.store.accept({'update_id': 2, 'callback_query': {
            'id': '2', 'from': {'id': 7}, 'data': RETRY_DATA,
            'message': {'message_id': 50, 'message_thread_id': 4, 'chat': {'id': -10042, 'type': 'supergroup'}}}})
        self.assertEqual(outcome, 'control_callback')
        record = self.store.get('account_signin')
        self.assertEqual((record['target'], record.get('provider')), ('carol', None))
        self.assertTrue(self.latest()['text'].startswith('Starting Claude sign-in.'))
        with self.store.db:
            self.store.put('account_signin', None)
            self.store.put(RETRY_KEY, {'provider': 'claude', 'target': 'carol'})
        self.store.accept({'update_id': 3, 'callback_query': {
            'id': '3', 'from': {'id': 7}, 'data': CODEX_RETRY_DATA,
            'message': {'message_id': 51, 'message_thread_id': 4, 'chat': {'id': -10042, 'type': 'supergroup'}}}})
        record = self.store.get('account_signin')
        self.assertEqual((record['target'], record['provider']), (None, 'codex'))

    def test_every_callback_fits_telegram(self):
        self.owner_accounts()
        self.secrets()
        self.bind_topic()
        for number in range(12):
            self.account('gone%02d' % number, 'gone-%02d-%s@example.com' % (number, 'y' * 50), signed_in=False)
        with self.store.db:
            self.store.put('codex_auto_switch', False)
        names = ['accounts:0', 'accounts:1', 'account:carol', 'remove:carol', 'account:codex', 'reset:dave',
                 'settings', 'model:worker', 'model:codex', 'model:coordinator', 'policy', 'project', 'jobs',
                 'secrets:0', 'secret:DEPLOY_KEY', 'secret:NPM_TOKEN', 'secret:STRIPE_KEY', 'revoke:DEPLOY_KEY']
        for name in names:
            self.assertTrue(self.show(name), name)
            buttons = [button for line in self.latest()['markup']['inline_keyboard'] for button in line]
            self.assertTrue(buttons, name)
            for button in buttons:
                self.assertLessEqual(len(button['text']), 40, (name, button))
                if 'callback_data' in button:
                    self.assertLessEqual(len(button['callback_data'].encode()), 64, (name, button))
                else:
                    self.assertTrue(button['url'].startswith('https://'), name)
        self.send(2, '/projects')
        self.assertTrue(all(len(button['callback_data'].encode()) <= 64
                            for line in self.latest()['markup']['inline_keyboard'] for button in line))

    def test_account_menu_has_no_selection_or_rename_controls(self):
        from coordinator.control_api import find
        self.account('work')
        self.send(2, '/setup')
        self.click(3, 'Accounts')
        self.assertNotIn('Choose accounts automatically', self.labels())
        self.assertIsNone(find('account.select'))
        self.assertIsNone(find('account.rename'))
        self.assertFalse(self.store.get('account_selection'))
