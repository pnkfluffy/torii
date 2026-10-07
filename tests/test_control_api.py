import json
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import control_api, mcp
from coordinator.controls import handle_control
from coordinator.service import Service
from coordinator.store import Store


class ControlApiTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name))
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test',?,1)",
                                  (self.temp.name,))
            self.store.put('owner', 1)
            self.store.put('mode', 'group')
            self.store.put('group', 1)
            self.store.put('accounts', {'work': {'config_dir': None, 'enabled': True}})

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def call(self, op, params=None):
        with self.store.db:
            return control_api.call(self.store, op, params or {}, topic='1:2')

    def test_home_control_preserves_native_session_ownership(self):
        self.store.put('coordinator_home_topic', '1:2')
        self.store.put('coordinator_session', 'original-native-session')
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:3',1,3,'other',?,1)",
                              (self.temp.name,))
        self.assertTrue(self.call('topic.home', {'topic': '1:3'}).ok)
        self.assertEqual(self.store.get('coordinator_home_topic'), '1:3')
        self.assertEqual(self.store.get('coordinator_session_topic'), '1:2')
        self.assertEqual(self.store.get('coordinator_session'), 'original-native-session')
        service = Service(self.store, None, None, Path(self.temp.name))
        self.assertEqual(service.home_topic(), '1:3')
        self.assertEqual(service.session_key('1:2'), 'coordinator')
        self.assertEqual(service.session_key('1:3'), 'coordinator:1:3')
        self.assertTrue(self.call('topic.home', {'topic': '1:2'}).ok)
        self.assertEqual(self.store.get('coordinator_session_topic'), '1:2')
        self.assertFalse(self.call('topic.home', {'topic': '1:9'}).ok)
        self.store.db.execute("UPDATE topics SET enabled=0 WHERE id='1:3'")
        self.assertFalse(self.call('topic.home', {'topic': '1:3'}).ok)

    def test_claude_worker_is_refused_when_no_claude_account_is_configured(self):
        self.store.put('accounts', {})
        task = self.store.task_create('1:2', 'Work', worktree=self.temp.name)
        result = self.call('workers.spawn', {'task': task['id'], 'provider': 'claude', 'prompt': 'Go'})
        self.assertFalse(result.ok)
        self.assertIn('No Claude account is connected. Use provider codex.', result.text)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM workers').fetchone()[0], 0)

    def test_first_bind_selects_home_and_later_bind_preserves_it(self):
        self.assertIsNone(Service(self.store, None, None, Path(self.temp.name)).home_topic())
        self.store.bind('1:2', self.temp.name, 'test', enabled=True)
        self.assertEqual(self.store.get('coordinator_home_topic'), '1:2')
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('1:3',1,3,'other','')")
        self.store.bind('1:3', self.temp.name, 'other', enabled=True)
        self.assertEqual(self.store.get('coordinator_home_topic'), '1:2')

    def test_disabled_home_can_be_cleared_without_changing_session_ownership(self):
        self.store.put('coordinator_home_topic', '1:2')
        self.store.put('coordinator_session', 'native-session')
        self.store.put('coordinator_host', {'id': 'original'})
        self.assertTrue(self.call('topic.disable').ok)
        self.assertTrue(self.call('topic.home', {'topic': '1:2', 'clear': True}).ok)
        self.assertIsNone(self.store.get('coordinator_home_topic'))
        self.assertEqual(self.store.get('coordinator_session_topic'), '1:2')
        self.assertEqual(self.store.get('coordinator_session'), 'native-session')
        self.assertEqual(self.store.get('coordinator_host'), {'id': 'original'})
        self.assertEqual(Service(self.store, None, None, Path(self.temp.name)).session_key('1:2'), 'coordinator')
        self.assertFalse(self.call('topic.home', {'topic': '1:2', 'clear': True}).ok)

    def test_clearing_another_channel_refuses_and_preserves_home(self):
        self.store.put('coordinator_home_topic', '1:2')
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:3',1,3,'other',?,1)",
                              (self.temp.name,))
        self.assertFalse(self.call('topic.home', {'topic': '1:3', 'clear': True}).ok)
        self.assertEqual(self.store.get('coordinator_home_topic'), '1:2')

    def test_disabled_bind_does_not_select_home(self):
        self.assertTrue(self.call('topic.bind', {'topic': '1:2', 'cwd': self.temp.name,
                                               'name': 'test', 'enable': False}).ok)
        self.assertIsNone(self.store.get('coordinator_home_topic'))
        self.assertTrue(self.call('topic.bind', {'topic': '1:2', 'cwd': self.temp.name,
                                               'name': 'test', 'enable': True}).ok)
        self.assertEqual(self.store.get('coordinator_home_topic'), '1:2')

    def test_manifest_exposes_v2_and_menu_ops_without_removed_ops(self):
        ids = [op.id for op in control_api.OPS]
        self.assertEqual(len(ids), len(set(ids)))
        for required in ('telegram.send', 'tasks.create', 'tasks.update', 'tasks.get', 'tasks.list',
                         'worktree.create', 'workers.spawn', 'workers.steer', 'workers.stop',
                         'workers.list', 'workers.goal', 'service.restart', 'settings.show',
                         'account.enable', 'account.disable', 'topic.setup_new'):
            self.assertIn(required, ids)
        for removed in ('audit.list', 'held.list', 'topic.resume', 'service.restart_request',
                        'telegram.token', 'worker.stop', 'worker.steer', 'ops.list', 'workers.limit',
                        'model.planning', 'topic.provider', 'account.select', 'account.rename',
                        'account.selection_auto', 'account.access', 'account.threshold'):
            self.assertNotIn(removed, ids)
        for op in control_api.OPS:
            self.assertIn(op.kind, (control_api.READ, control_api.WRITE))
            self.assertIn(op.transport, ('store', control_api.SERVICE))
            for kind in op.params.values():
                self.assertIn(kind.rstrip('?'), control_api.TYPES)
            if op.transport == 'store':
                self.assertTrue(callable(control_api._handler(op)))

    def test_account_remove_and_targeted_add_are_in_control_manifest(self):
        remove = control_api.find('account.remove')
        self.assertEqual((remove.kind, remove.transport, remove.params),
                         (control_api.WRITE, 'store', {'alias': 'alias'}))
        self.assertEqual(mcp.input_schema(remove)['required'], ['alias'])
        schema = mcp.input_schema(control_api.find('account.add'))
        self.assertEqual(schema['properties']['alias'], {'type': 'string'})
        self.assertNotIn('alias', schema['required'])
        self.assertNotIn('account_remove', {tool['name'] for tool in mcp.tools_list()})
        self.assertIn('95%', control_api.find('account.codex_reset').description)

    def account_fixture(self, provider, logged_in=False):
        directory = Path(self.temp.name) / provider
        previous = Path(self.temp.name) / (provider + '-previous')
        directory.mkdir()
        previous.mkdir()
        for folder in (directory, previous):
            (folder / 'history.jsonl').write_text('native history\n')
        key, status_key, blocks_key = (('codex_accounts', 'codex_account_status', 'codex_account_blocks')
                                        if provider == 'codex' else ('accounts', 'account_status', 'account_blocks'))
        self.store.put(key, {'work': {'config_dir': str(directory), 'enabled': False,
                                     'previous_config_dirs': [str(previous), str(directory)]},
                             'other': {'config_dir': None, 'enabled': False}})
        self.store.put(status_key, {'work': {'identity': {'email': 'work@example.com', 'logged_in': logged_in}},
                                    'other': {'identity': {'logged_in': False}}})
        self.store.put(blocks_key, {'work': {'reason': 'quota', 'until': None},
                                    'other': {'reason': 'quota', 'until': None}})
        return directory, previous, key, status_key, blocks_key

    def test_targeted_account_add_infers_provider_and_uses_fresh_folder(self):
        self.store.put('bot_username', 'torii_test_bot')
        for provider in ('claude', 'codex'):
            with self.subTest(provider=provider):
                self.store.put('accounts', {})
                self.store.put('codex_accounts', {})
                directory, _, _, _, _ = self.account_fixture(provider)
                self.store.put('account_signin', None)
                with patch('coordinator.signin.accounts_root', return_value=Path(self.temp.name) / 'claude-new'), \
                     patch('coordinator.codex_accounts.accounts_root', return_value=Path(self.temp.name) / 'codex-new'):
                    result = self.call('account.add', {'alias': 'work',
                                                       'provider': 'claude' if provider == 'codex' else 'codex'})
                self.assertTrue(result.ok, result.text)
                record = self.store.get('account_signin')
                self.assertEqual(record['target'], 'work')
                self.assertEqual(record.get('provider', 'claude'), provider)
                new = Path(record['config_dir'])
                self.assertEqual(new.parent, Path(self.temp.name) / (provider + '-new'))
                self.assertTrue(new.name.startswith('.torii-'))
                self.assertFalse(new.exists())
                self.assertEqual((directory / 'history.jsonl').read_text(), 'native history\n')

    def test_account_add_and_remove_refuse_authenticated_accounts_even_when_disabled(self):
        self.store.put('bot_username', 'torii_test_bot')
        for provider in ('claude', 'codex'):
            with self.subTest(provider=provider):
                self.store.put('accounts', {})
                self.store.put('codex_accounts', {})
                self.account_fixture(provider, logged_in=True)
                before = list(self.store.db.execute('SELECT key,value FROM settings ORDER BY key'))
                added = self.call('account.add', {'alias': 'work'})
                self.assertEqual((added.state, added.text), ('refused', 'work@example.com is already signed in.'))
                removed = self.call('account.remove', {'alias': 'work'})
                self.assertEqual((removed.state, removed.text),
                                 ('refused', 'work@example.com is signed in. Only signed-out accounts can be removed.'))
                self.assertEqual(list(self.store.db.execute('SELECT key,value FROM settings ORDER BY key')), before)

    def test_account_remove_refuses_pending_target_without_mutation(self):
        for provider in ('claude', 'codex'):
            with self.subTest(provider=provider):
                self.store.put('accounts', {})
                self.store.put('codex_accounts', {})
                self.account_fixture(provider)
                self.store.put('account_signin', {'target': 'work', 'provider': provider, 'state': 'cancelled'})
                before = list(self.store.db.execute('SELECT key,value FROM settings ORDER BY key'))
                result = self.call('account.remove', {'alias': 'work'})
                self.assertEqual((result.state, result.text),
                                 ('refused', 'A sign-in for this account is open. Finish or cancel it first.'))
                self.assertEqual(list(self.store.db.execute('SELECT key,value FROM settings ORDER BY key')), before)

    def test_account_remove_clears_provider_records_and_keeps_all_folders(self):
        for provider in ('claude', 'codex'):
            with self.subTest(provider=provider):
                self.store.put('accounts', {})
                self.store.put('codex_accounts', {})
                directory, previous, key, status_key, blocks_key = self.account_fixture(provider)
                self.store.put('account_dirs_removed', [str(previous / '..' / previous.name)])
                self.store.put('codex_active_account', 'work')
                self.store.put('account_signin', {'target': 'other', 'state': 'starting'})
                result = self.call('account.remove', {'alias': 'work'})
                self.assertEqual((result.ok, result.text), (True, 'Removed work@example.com.'))
                self.assertEqual(self.store.get(key), {'other': {'config_dir': None, 'enabled': False}})
                self.assertEqual(self.store.get(status_key), {'other': {'identity': {'logged_in': False}}})
                self.assertEqual(self.store.get(blocks_key), {'other': {'reason': 'quota', 'until': None}})
                self.assertEqual(self.store.get('account_dirs_removed'), [str(previous.resolve()),
                                                                          str(directory.resolve())])
                self.assertEqual(self.store.get('codex_active_account'), None if provider == 'codex' else 'work')
                for folder in (directory, previous):
                    self.assertEqual((folder / 'history.jsonl').read_text(), 'native history\n')
                self.assertEqual(self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0], 0)

    def test_unknown_account_target_and_remove_refuse_without_mutation(self):
        before = list(self.store.db.execute('SELECT key,value FROM settings ORDER BY key'))
        for op in ('account.add', 'account.remove'):
            with self.subTest(op=op):
                result = self.call(op, {'alias': 'missing'})
                self.assertEqual((result.state, result.text),
                                 ('refused', 'Account is not registered. Use /accounts to list registered aliases.'))
                self.assertEqual(list(self.store.db.execute('SELECT key,value FROM settings ORDER BY key')), before)

    def test_local_task_create_without_a_channel_names_the_topic_parameter(self):
        result = control_api.call(self.store, 'tasks.create', {'title': 'Ship unit 2'})
        self.assertEqual((result.state, result.text), ('refused', 'tasks.create needs topic (a channel ID) or message.'))

    def test_reports_require_current_binding_even_for_disabled_topics(self):
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('8:0',8,0,'stale','')")
        for mode, chat in (('group', -10042),):
            with self.subTest(mode=mode):
                self.store.put('mode', mode)
                self.store.put('group', -10042)
                self.store.db.execute("UPDATE topics SET chat=?,enabled=0 WHERE id='1:2'", (chat,))
                stale = self.call('telegram.send', {'topic': '8:0', 'text': 'Private report'})
                self.assertFalse(stale.ok)
                self.assertIn('current Telegram binding', stale.text)
                self.assertTrue(self.call('telegram.send', {'text': 'Status'}).ok)
                row = self.store.pending_delivery()
                self.assertEqual(row['chat'], chat)
                self.store.delivered(row['id'], 100)
        self.store.put('owner', None)
        self.assertFalse(self.call('telegram.send', {'text': 'Unpaired'}).ok)
        self.assertIsNone(self.store.pending_delivery())

    def test_tasks_and_telegram_send_use_store(self):
        created = self.call('tasks.create', {'title': 'Ship unit 2', 'notes': 'Test it'})
        self.assertTrue(created.ok)
        task = created.data
        self.assertEqual((task['topic'], task['number'], task['status']), ('1:2', 1, 'open'))
        updated = self.call('tasks.update', {'task': task['id'], 'status': 'done'})
        self.assertEqual(updated.data['status'], 'done')
        self.assertEqual(self.call('tasks.get', {'task': task['id']}).data['title'], 'Ship unit 2')
        self.assertEqual(len(self.call('tasks.list', {'status': 'done'}).data), 1)
        self.assertFalse(self.call('tasks.update', {'task': task['id'], 'status': 'waiting'}).ok)
        sent = self.call('telegram.send', {'text': 'Done', 'buttons': [[{'text': 'Open', 'callback_data': 'open'}]]})
        row = self.store.db.execute('SELECT * FROM outbox WHERE id=?', (sent.data['outbox'],)).fetchone()
        self.assertEqual(row['text'], 'Done')
        self.assertEqual(json.loads(row['reply_markup'])['inline_keyboard'][0][0]['text'], 'Open')

    def test_telegram_send_translates_same_channel_message_number(self):
        message = self.store.message_save('1:2', 'owner', 'Question', telegram_message=5006)
        sent = self.call('telegram.send', {'text': 'Answer', 'reply_to': message['id']})
        row = self.store.db.execute('SELECT reply_to FROM outbox WHERE id=?', (sent.data['outbox'],)).fetchone()
        self.assertTrue(sent.ok)
        self.assertEqual(row['reply_to'], 5006)

    def test_telegram_send_refuses_invalid_reply_targets(self):
        self.two_topics()
        cross_channel = self.store.message_save('1:3', 'owner', 'Other channel', telegram_message=7001)
        no_telegram_id = self.store.message_save('1:2', 'worker_result', 'Worker result')
        for message in (cross_channel['id'], 999999, no_telegram_id['id']):
            result = self.call('telegram.send', {'text': 'Answer', 'reply_to': message})
            self.assertEqual((result.state, result.text), (
                'refused', 'reply_to must be the message=N number of a Telegram message in this channel; '
                'send without reply_to or use another number.'))

    def test_telegram_send_without_reply_target_keeps_none(self):
        sent = self.call('telegram.send', {'text': 'Broadcast'})
        row = self.store.db.execute('SELECT reply_to FROM outbox WHERE id=?', (sent.data['outbox'],)).fetchone()
        self.assertTrue(sent.ok)
        self.assertIsNone(row['reply_to'])

    def test_telegram_send_image_and_multipart_replies_use_telegram_id(self):
        message = self.store.message_save('1:2', 'owner', 'Question', telegram_message=5006)
        image = Path(self.store.directory) / 'reply.png'
        image.write_bytes(b'image')
        sent = self.call('telegram.send', {'text': 'A' * 5000, 'reply_to': message['id'], 'image': str(image)})
        self.assertTrue(sent.ok, sent.text)
        rows = self.store.db.execute('SELECT reply_to,image FROM outbox WHERE topic=? ORDER BY id',
                                     ('1:2',)).fetchall()
        self.assertGreater(len(rows), 1)
        self.assertEqual([row['reply_to'] for row in rows], [5006] * len(rows))
        self.assertEqual(rows[0]['image'], str(image))
        self.assertTrue(all(row['image'] is None for row in rows[1:]))

    def test_telegram_send_description_documents_message_tag_reply_to(self):
        description = control_api.BY_ID['telegram.send'].description
        self.assertIn('message=N number from a message tag in the same channel', description)

    def two_topics(self):
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:3',1,3,'other',?,1)",
                                  (self.temp.name,))
        return (self.store.message_save('1:2', 'owner', 'build it')['id'],
                self.store.message_save('1:3', 'owner', 'think')['id'])

    def mcp(self, op, params):
        with self.store.db:
            return control_api.call(self.store, op, params, source='mcp')

    def test_coordinator_task_is_bound_to_the_topic_of_the_message_that_asked(self):
        asked, other = self.two_topics()
        missing = self.mcp('tasks.create', {'topic': '1:3', 'title': 'Guess'})
        self.assertEqual(missing.state, 'refused')
        self.assertIn('Pass the ID of the owner message', missing.text)
        self.assertEqual(self.mcp('tasks.create', {'message': 999, 'title': 'Lost'}).state, 'refused')
        created = self.mcp('tasks.create', {'message': other, 'title': 'Brainstorm'})
        self.assertTrue(created.ok, created.text)
        self.assertEqual((created.data['topic'], created.data['number']), ('1:3', 1))
        crossed = self.mcp('tasks.create', {'message': other, 'topic': '1:2', 'title': 'Wrong list'})
        self.assertEqual(crossed.state, 'refused')
        self.assertIn('came from channel 1:3, not 1:2', crossed.text)
        self.assertEqual(self.store.tasks_list('1:2'), [])
        allowed = self.mcp('tasks.create', {'message': other, 'topic': '1:2', 'cross_topic': True, 'title': 'Asked'})
        self.assertEqual(allowed.data['topic'], '1:2')
        same = self.mcp('tasks.create', {'message': asked, 'topic': '1:2', 'title': 'Build'})
        self.assertEqual((same.data['topic'], same.data['number']), ('1:2', 2))

    def test_stale_tasks_are_open_tasks_idle_past_the_hours_with_no_active_worker(self):
        now = time.time()
        titles = ('Idle', 'Recent', 'Running', 'Done', 'Idle worker', 'Waiting for quota')
        tasks = {title: self.store.task_create('1:2', title) for title in titles}
        with self.store.db:
            for title, updated in (('Idle', now - 7 * 3600), ('Recent', now - 3600), ('Running', now - 9 * 3600),
                                   ('Done', now - 9 * 3600), ('Idle worker', now - 10 * 3600),
                                   ('Waiting for quota', now - 9 * 3600)):
                self.store.db.execute('UPDATE tasks SET updated=?,status=? WHERE id=?',
                                      (updated, 'done' if title == 'Done' else 'open', tasks[title]['id']))
            for title, status, updated in (('Running', 'running', now - 9 * 3600),
                                           ('Idle worker', 'done', now - 8 * 3600),
                                           ('Waiting for quota', 'waiting_for_quota', now - 9 * 3600)):
                self.store.db.execute('''INSERT INTO workers(task,topic,provider,prompt,status,created,updated)
                    VALUES (?,'1:2','claude','p',?,?,?)''', (tasks[title]['id'], status, updated, updated))
        stale = self.call('tasks.stale')
        self.assertEqual([row['title'] for row in stale.data['tasks']], ['Idle worker', 'Idle'])
        self.assertEqual(stale.data['tasks'][0]['workers'], 1)
        self.assertEqual([row['title'] for row in self.call('tasks.stale', {'hours': 1}).data['tasks']],
                         ['Idle worker', 'Idle', 'Recent'])
        self.assertIn('stale', {name.split('_')[1] for name in mcp.TOOLS if name.startswith('tasks_')})

    def test_task_reads_for_a_topic_show_only_that_topic(self):
        self.two_topics()
        mine = self.store.task_create('1:2', 'Mine')
        theirs = self.store.task_create('1:3', 'Theirs')
        self.assertEqual([row['title'] for row in self.mcp('tasks.list', {'topic': '1:3'}).data], ['Theirs'])
        self.assertEqual([row['title'] for row in self.call('tasks.list').data], ['Mine'])
        refused = self.mcp('tasks.get', {'task': mine['id'], 'topic': '1:3'})
        self.assertEqual(refused.state, 'refused')
        self.assertIsNone(refused.data)
        self.assertEqual(self.mcp('tasks.get', {'task': theirs['id'], 'topic': '1:3'}).data['title'], 'Theirs')
        self.assertEqual(self.mcp('tasks.get', {'task': mine['id']}).data['title'], 'Mine')

    def test_topic_names_are_display_only_and_ids_resolve_even_when_labels_repeat(self):
        self.two_topics()
        self.assertIsNone(control_api.resolve_topic(self.store, 'other'))
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:4',1,4,'Other',?,1)",
                                  (self.temp.name,))
        self.assertIsNone(control_api.resolve_topic(self.store, 'OTHER'))
        self.assertEqual(self.mcp('tasks.list', {'topic': 'other'}).state, 'refused')
        self.assertEqual(control_api.resolve_topic(self.store, '1:4'), '1:4')

    def test_topic_rename_allows_duplicate_labels_and_telegram_title_wins_later(self):
        self.two_topics()
        self.store.put('owner', 7)
        self.store.put('group', 1)
        self.assertTrue(self.call('topic.rename', {'name': 'other'}).ok)
        self.assertEqual(self.store.topic('1:2')['name'], 'other')
        edited = {'update_id': 8, 'message': {'message_id': 8, 'from': {'id': 7}, 'message_thread_id': 2,
                                              'chat': {'id': 1, 'type': 'supergroup'},
                                              'forum_topic_edited': {'name': 'Telegram title'}}}
        self.assertEqual(self.store.accept(edited), 'service_event')
        self.assertEqual(self.store.topic('1:2')['name'], 'Telegram title')

    def test_task_secret_declarations_are_sorted_mapped_and_validated(self):
        created = self.call('tasks.create', {'title': 'Release', 'secrets': 'B_TOKEN,A_KEY,A_KEY'})
        self.assertTrue(created.ok, created.text)
        self.assertEqual(created.data['secrets'], ['A_KEY', 'B_TOKEN'])
        task = created.data['id']
        updated = self.call('tasks.update', {'task': task, 'secrets': ' GH_TOKEN=GITHUB_TOKEN_ORG , A_KEY=A_KEY'})
        self.assertEqual(updated.data['secrets'], ['A_KEY', 'GH_TOKEN=GITHUB_TOKEN_ORG'])
        self.assertEqual(self.call('tasks.update', {'task': task, 'secrets': ['NPM_TOKEN']}).data['secrets'],
                         ['NPM_TOKEN'])
        for invalid in ('PATH', 'NODE_REPL_AUTH_TOKEN', 'RUST_LOG', 'GIT_TOKEN=GITHUB_TOKEN', 'GH_TOKEN=lower', 'A_KEY=B_KEY,A_KEY=C_KEY', 'A_KEY,,B_KEY',
                        'GH_TOKEN=', ['A_KEY', 3]):
            refused = self.call('tasks.update', {'task': task, 'secrets': invalid})
            self.assertEqual(refused.state, 'refused', invalid)
            self.assertIn('secret names', refused.text)
        self.assertEqual(self.store.task_get(task)['secrets'], ['NPM_TOKEN'])
        cleared = self.call('tasks.update', {'task': task, 'secrets': ''})
        self.assertEqual(cleared.data['secrets'], [])
        self.assertIsNone(self.store.db.execute('SELECT secrets FROM tasks WHERE id=?', (task,)).fetchone()[0])
        self.assertEqual(self.call('tasks.get', {'task': task}).data['secrets'], [])

    def test_both_providers_accept_tasks_with_secrets(self):
        task = self.store.task_create('1:2', 'Build', worktree=self.temp.name, secrets=['GITHUB_TOKEN'])
        spawned = self.call('workers.spawn', {'task': task['id'], 'provider': 'codex', 'prompt': 'Build it'})
        self.assertTrue(spawned.ok, spawned.text)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM workers').fetchone()[0], 1)
        self.assertTrue(self.call('workers.spawn', {'task': task['id'], 'provider': 'claude',
                                                    'prompt': 'Build it'}).ok)

    def test_telegram_image_path_roots_and_refusals(self):
        root = Path(self.temp.name)
        project = root / 'project'
        project.mkdir()
        with self.store.db:
            self.store.db.execute("UPDATE topics SET cwd=? WHERE id='1:2'", (str(project),))
        topic_file = project / 'topic.png'
        topic_file.write_bytes(b'photo')
        task_root = root.parent / (root.name + '-task')
        outside = root.parent / (root.name + '-outside.png')
        task_root.mkdir()
        try:
            worktree_file = task_root / 'worktree.pdf'
            worktree_file.write_bytes(b'document')
            state_file = self.store.directory / 'state-image.png'
            state_file.write_bytes(b'state')
            task = self.store.task_create('1:2', 'Image task', worktree=str(task_root))
            for path in (topic_file, worktree_file, state_file):
                result = self.call('telegram.send', {'text': 'Attached', 'image': str(path)})
                self.assertTrue(result.ok, result.text)
                row = self.store.db.execute('SELECT image FROM outbox WHERE id=?',
                                            (result.data['outbox'],)).fetchone()
                self.assertEqual(row['image'], str(path.resolve()))
            outside.write_bytes(b'outside')
            link = root / 'escape.png'
            link.symlink_to(outside)
            missing = root / 'missing.png'
            too_large = root / 'large.pdf'
            with too_large.open('wb') as output:
                output.truncate(50 * 1024 * 1024 + 1)
            refusals = ((outside, 'under'), (link, 'under'), (missing, 'does not exist'),
                        (too_large, 'too large'))
            for path, phrase in refusals:
                result = self.call('telegram.send', {'text': 'Attached', 'image': str(path)})
                self.assertFalse(result.ok)
                self.assertIn(phrase, result.text)
            relative = self.call('telegram.send', {'text': 'Attached', 'image': 'topic.png'})
            self.assertFalse(relative.ok)
            self.assertIn('absolute', relative.text)
            self.store.task_update(task['id'], status='done')
        finally:
            outside.unlink(missing_ok=True)
            worktree_file.unlink(missing_ok=True)
            task_root.rmdir()

    def test_telegram_image_refuses_token_and_agent_history(self):
        root = Path(self.temp.name)
        token = root / 'bot-token'
        token.write_text('dummy')
        self.store.token_file = token.resolve()
        history = root / '.claude' / 'history.txt'
        history.parent.mkdir()
        history.write_text('dummy')
        codex_history = root / '.codex' / 'history.txt'
        codex_history.parent.mkdir()
        codex_history.write_text('dummy')
        with patch('coordinator.control_api.Path.home', return_value=root):
            for path in (token, history, codex_history):
                result = self.call('telegram.send', {'text': 'Attached', 'image': str(path)})
                self.assertFalse(result.ok)

    def test_workers_require_worktree_and_both_providers_accept_goals(self):
        task = self.store.task_create('1:2', 'Build')
        self.assertFalse(self.call('workers.spawn', {'task': task['id'], 'provider': 'claude',
                                                       'prompt': 'Build it'}).ok)
        self.store.task_update(task['id'], worktree=self.temp.name)
        codex = self.call('workers.spawn', {'task': task['id'], 'provider': 'codex',
                                           'prompt': 'Build it', 'goal': 'Tests pass'})
        self.assertTrue(codex.ok, codex.text)
        self.assertTrue(self.call('workers.goal', {'worker': codex.data['worker'], 'condition': 'New goal'}).ok)
        claude = self.call('workers.spawn', {'task': task['id'], 'provider': 'claude',
                                            'prompt': 'Build it', 'goal': 'Tests pass'})
        self.assertTrue(claude.ok)
        worker = claude.data['worker']
        self.assertEqual(self.store.db.execute('SELECT task,goal FROM workers WHERE id=?',
                                               (worker,)).fetchone()[:], (task['id'], 'Tests pass'))
        self.assertFalse(self.call('workers.goal', {'worker': worker, 'condition': 'x' * (control_api.TEXT_LIMIT + 1)}).ok)
        self.assertTrue(self.call('workers.goal', {'worker': worker, 'condition': 'clear'}).ok)
        self.assertIsNone(self.store.db.execute('SELECT goal FROM workers WHERE id=?', (worker,)).fetchone()[0])
        self.assertEqual(self.store.db.execute('SELECT op FROM service_requests').fetchone()[0], 'workers.goal')
        self.assertEqual(len(self.call('workers.list', {'task': task['id']}).data['workers']), 2)

    def test_workers_list_pages_short_summaries_and_workers_get_returns_the_full_record(self):
        task = self.store.task_create('1:2', 'Build', worktree=self.temp.name)
        ids = [self.call('workers.spawn', {'task': task['id'], 'provider': 'claude',
                                           'prompt': 'P%d ' % number + 'x' * 30000}).data['worker']
               for number in range(5)]
        self.store.worker_complete(ids[0], {'success': True, 'text': 'R' * 30000})
        first = self.call('workers.list', {'limit': 2})
        self.assertEqual([row['id'] for row in first.data['workers']], [ids[4], ids[3]])
        self.assertEqual(first.data['next_before'], ids[3])
        self.assertLess(len(json.dumps(first.data)), 2000)
        self.assertEqual(set(first.data['workers'][0]), {
            'id', 'task', 'topic', 'provider', 'model', 'effort', 'work', 'status', 'pid', 'created', 'updated',
            'prompt_head', 'result_head'})
        self.assertTrue(first.data['workers'][0]['prompt_head'].startswith('P4 xxx'))
        rest = self.call('workers.list', {'limit': 2, 'before': ids[1] + 1})
        self.assertEqual([row['id'] for row in rest.data['workers']], [ids[1], ids[0]])
        self.assertIsNone(rest.data['next_before'])
        self.assertEqual(rest.data['workers'][1]['result_head'], 'R' * control_api.HEAD + '…')
        done = self.call('workers.list', {'status': 'done'})
        self.assertEqual([row['id'] for row in done.data['workers']], [ids[0]])
        self.assertFalse(self.call('workers.list', {'limit': 0}).ok)
        self.assertFalse(self.call('workers.list', {'limit': control_api.WORKER_PAGE_LIMIT + 1}).ok)
        self.assertFalse(self.call('workers.list', {'status': 'lost'}).ok)
        full = self.call('workers.get', {'worker': ids[0]})
        self.assertEqual(len(full.data['prompt']), 30003)
        self.assertEqual(full.data['result']['text'], 'R' * 30000)
        self.assertFalse(self.call('workers.get', {'worker': 999}).ok)

    def test_spawning_worker_supersedes_waiting_workers_on_the_task(self):
        task = self.store.task_create('1:2', 'Build', worktree=self.temp.name)
        older = self.call('workers.spawn', {'task': task['id'], 'provider': 'claude',
                                           'prompt': 'Question'}).data['worker']
        self.store.db.execute("UPDATE workers SET status='needs_input' WHERE id=?", (older,))
        newer = self.call('workers.spawn', {'task': task['id'], 'provider': 'claude', 'prompt': 'Continue'}).data['worker']
        self.assertEqual(self.store.db.execute('SELECT status FROM workers WHERE id=?', (older,)).fetchone()[0],
                         'interrupted')
        self.assertEqual(self.call('workers.list', {'task': task['id'], 'status': 'needs_input'}).data['workers'], [])
        self.assertEqual(self.call('workers.get', {'worker': older}).data['id'], older)
        self.assertEqual(self.store.db.execute('SELECT status FROM workers WHERE id=?', (newer,)).fetchone()[0],
                         'queued')

    def test_worker_briefs_have_their_own_limit_and_other_text_keeps_16000(self):
        task = self.store.task_create('1:2', 'Build', worktree=self.temp.name)
        spawn = {'task': task['id'], 'provider': 'claude'}
        self.assertTrue(self.call('workers.spawn', dict(spawn, prompt='b' * 13634)).ok)
        worker = self.call('workers.spawn', dict(spawn, prompt='b' * control_api.BRIEF_LIMIT)).data['worker']
        refused = self.call('workers.spawn', dict(spawn, prompt='b' * (control_api.BRIEF_LIMIT + 1)))
        self.assertFalse(refused.ok)
        self.assertIn('at most %d characters' % control_api.BRIEF_LIMIT, refused.text)
        self.assertEqual(self.call('workers.steer', {'worker': worker, 'prompt': 's' * 20000}).state, 'queued')
        self.assertFalse(self.call('workers.spawn', dict(spawn, prompt='Go', goal='g' * 16001)).ok)
        self.assertFalse(self.call('telegram.send', {'text': 't' * 16001}).ok)
        self.assertFalse(self.call('tasks.update', {'task': task['id'], 'notes': 'n' * 16001}).ok)
        self.assertTrue(self.call('tasks.update', {'task': task['id'], 'notes': 'n' * 16000}).ok)
        sent = self.call('telegram.send', {'text': ('word ' * 3200).strip()})
        self.assertTrue(sent.ok)
        parts = [row['text'] for row in self.store.db.execute('SELECT text FROM outbox ORDER BY id')]
        self.assertEqual(len(parts), 4)
        self.assertTrue(all(len(part) <= 4096 for part in parts))

    def test_mcp_schema_advertises_the_brief_and_text_limits(self):
        from coordinator import mcp
        spawn = mcp.input_schema(control_api.BY_ID['workers.spawn'])['properties']
        self.assertEqual(spawn['prompt']['maxLength'], control_api.BRIEF_LIMIT)
        self.assertEqual(spawn['goal']['maxLength'], control_api.TEXT_LIMIT)
        send = mcp.input_schema(control_api.BY_ID['telegram.send'])['properties']
        self.assertEqual(send['text']['maxLength'], control_api.TEXT_LIMIT)

    def test_service_operations_are_durable_requests(self):
        task = self.store.task_create('1:2', 'Build', worktree=self.temp.name)
        worker = self.call('workers.spawn', {'task': task['id'], 'provider': 'codex', 'prompt': 'Build'}).data['worker']
        self.assertEqual(self.call('workers.steer', {'worker': worker, 'prompt': 'Check tests'}).state, 'queued')
        self.assertEqual(self.call('workers.stop', {'worker': worker}).state, 'queued')
        self.assertEqual(self.call('service.restart', {'reason': 'New version'}).state, 'queued')
        self.assertEqual([row[0] for row in self.store.db.execute('SELECT op FROM service_requests ORDER BY id')],
                     ['workers.steer', 'workers.stop', 'service.restart'])
        self.assertIsNone(self.store.get('restart_requested_v2'))

    def test_mcp_steer_to_done_worker_returns_error_without_queueing(self):
        task = self.store.task_create('1:2', 'Build', worktree=self.temp.name)
        worker = self.call('workers.spawn', {'task': task['id'], 'provider': 'codex', 'prompt': 'Build'}).data['worker']
        for status in ('done', 'interrupted'):
            with self.subTest(status=status):
                with self.store.db:
                    self.store.db.execute('UPDATE workers SET status=? WHERE id=?', (status, worker))
                reply = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
                    'name': 'workers_steer', 'arguments': {'worker': worker, 'prompt': 'Continue'}}})
                self.assertTrue(reply['isError'])
                self.assertIn('Worker cannot receive input.', reply['content'][0]['text'])
                self.assertIn(status, reply['content'][0]['text'])
                self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM service_requests').fetchone()[0], 0)

    def test_mcp_stop_to_inactive_worker_returns_error_without_queueing(self):
        task = self.store.task_create('1:2', 'Build', worktree=self.temp.name)
        worker = self.call('workers.spawn', {'task': task['id'], 'provider': 'codex', 'prompt': 'Build'}).data['worker']
        for status in ('needs_input', 'done', 'interrupted'):
            with self.subTest(status=status):
                with self.store.db:
                    self.store.db.execute('UPDATE workers SET status=? WHERE id=?', (status, worker))
                reply = mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
                    'name': 'workers_stop', 'arguments': {'worker': worker}}})
                self.assertTrue(reply['isError'])
                self.assertIn('Worker is not running.', reply['content'][0]['text'])
                self.assertIn(status, reply['content'][0]['text'])
                self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM service_requests').fetchone()[0], 0)

    def test_worktree_handler_records_reused_path(self):
        task = self.store.task_create('1:2', 'Build')
        workspace = {'cwd': self.temp.name, 'branch': 'torii/task-1', 'isolated': True}
        with patch('coordinator.workspaces.prepare_task_workspace', new=AsyncMock(return_value=workspace)) as create:
            first = self.call('worktree.create', {'task': task['id']})
            second = self.call('worktree.create', {'task': task['id']})
        self.assertEqual(first.data['cwd'], self.temp.name)
        self.assertEqual(second.data['worktree'], self.temp.name)
        create.assert_awaited_once()

    def test_two_topics_on_one_repository_create_worktrees_for_their_first_tasks(self):
        repository = Path(self.temp.name) / 'repository'
        repository.mkdir()
        for args in (('init', '-q'), ('-c', 'user.email=t@example.com', '-c', 'user.name=T',
                                      'commit', '-q', '--allow-empty', '-m', 'base')):
            subprocess.run(('git', '-C', str(repository)) + args, check=True)
        with self.store.db:
            self.store.db.execute("UPDATE topics SET cwd=? WHERE id='1:2'", (str(repository),))
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:3',1,3,'twin',?,1)",
                                  (str(repository),))
        first = self.store.task_create('1:2', 'One')
        second = self.store.task_create('1:3', 'Two')
        self.assertEqual((first['number'], second['number']), (1, 1))
        made = [control_api.call(self.store, 'worktree.create', {'task': task['id']}) for task in (first, second)]
        self.assertTrue(all(result.ok for result in made), [result.text for result in made])
        self.assertEqual([result.data['branch'] for result in made],
                         ['torii/task-%d' % first['id'], 'torii/task-%d' % second['id']])

    def test_task_with_an_old_style_branch_keeps_its_saved_worktree(self):
        repository = Path(self.temp.name) / 'repository'
        repository.mkdir()
        subprocess.run(('git', '-C', str(repository), 'init', '-q'), check=True)
        subprocess.run(('git', '-C', str(repository), '-c', 'user.email=t@example.com', '-c', 'user.name=T',
                        'commit', '-q', '--allow-empty', '-m', 'base'), check=True)
        for number in range(4):
            task = self.store.task_create('1:2', 'Earlier %d' % number)
        legacy = Path(self.temp.name) / 'worktrees' / ('task-%d' % task['id'])
        subprocess.run(('git', '-C', str(repository), 'worktree', 'add', '-q', '-b',
                        'torii/task-%d' % task['number'], str(legacy)), check=True)
        self.store.task_update(task['id'], worktree=str(legacy))
        reused = self.call('worktree.create', {'task': task['id']})
        self.assertEqual(reused.data['worktree'], str(legacy))
        self.assertTrue(self.call('workers.spawn', {'task': task['id'], 'provider': 'claude', 'prompt': 'Go'}).ok)

    def test_account_menu_still_uses_available_ops(self):
        with self.store.db:
            self.assertTrue(handle_control(self.store, '1:2', 20, '/accounts'))
            self.assertTrue(control_api.call(self.store, 'model.worker', {'model': 'opus'}, topic='1:2').ok)
            self.assertTrue(handle_control(self.store, '1:2', 22, '/accounts'))
        self.assertEqual(self.store.get('worker_model'), 'opus')
        self.assertGreater(self.store.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], 0)


class RestartTests(unittest.IsolatedAsyncioTestCase):
    async def test_restart_waits_for_outbox_and_clears_durable_request(self):
        with tempfile.TemporaryDirectory() as root:
            store = Store(Path(root))
            try:
                with store.db:
                    store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test','',1)")
                    store.put('owner', 1)
                    store.put('mode', 'group')
                    store.put('group', 1)
                    control_api.call(store, 'service.restart', {'reason': 'New build'})
                    outbox = store.enqueue_report('1:2', 'Last reply')
                service = Service(store, object(), object(), Path(root))
                await service.controls_once()
                with patch.object(service, 'exit_for_restart') as exit_service:
                    self.assertFalse(await service.restart_once())
                    exit_service.assert_not_called()
                    store.delivered(outbox, 10)
                    self.assertTrue(await service.restart_once())
                    exit_service.assert_called_once()
                self.assertIsNone(store.get('restart_requested_v2'))
            finally:
                store.close()

    async def test_service_runs_queued_worker_controls_and_steers_running_goal(self):
        with tempfile.TemporaryDirectory() as root:
            store = Store(Path(root))
            store.put('accounts', {'work': {'config_dir': None, 'enabled': True}})
            try:
                with store.db:
                    store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test','',1)")
                task = store.task_create('1:2', 'Build', worktree=root)
                spawned = control_api.call(store, 'workers.spawn',
                                           {'task': task['id'], 'provider': 'claude', 'prompt': 'Build'})
                worker = spawned.data['worker']
                control_api.call(store, 'workers.steer', {'worker': worker, 'prompt': 'Check tests'})
                control_api.call(store, 'workers.goal', {'worker': worker, 'condition': 'All tests pass'})
                service = Service(store, object(), object(), Path(root))
                self.assertTrue(await service.controls_once())
                row = store.db.execute('SELECT prompt,goal FROM workers WHERE id=?', (worker,)).fetchone()
                self.assertIn('Check tests', row['prompt'])
                self.assertEqual(row['goal'], 'All tests pass')
                self.assertEqual([row[0] for row in store.db.execute('SELECT state FROM service_requests')],
                                 ['done', 'done'])
                with store.db:
                    store.db.execute("UPDATE workers SET status='running' WHERE id=?", (worker,))
                control = AsyncMock()
                control.steer.return_value = 'received'
                service.workers.controls[worker] = control
                control_api.call(store, 'workers.goal', {'worker': worker, 'condition': 'clear'})
                self.assertTrue(await service.controls_once())
                self.assertEqual(control.steer.await_args.args[1], '/goal clear')
                self.assertEqual(store.db.execute('SELECT state FROM service_requests ORDER BY id DESC LIMIT 1').fetchone()[0],
                                 'done')
            finally:
                store.close()


    async def test_running_codex_goals_use_the_native_control_and_wait_for_receipts(self):
        from coordinator.service import Service
        from unittest.mock import AsyncMock
        with tempfile.TemporaryDirectory() as root:
            store = Store(Path(root))
            self.addCleanup(store.close)
            with store.db:
                store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test','',1)")
            task = store.task_create('1:2', 'Build', worktree=root)
            worker = control_api.call(store, 'workers.spawn',
                {'task': task['id'], 'provider': 'codex', 'prompt': 'Build'}).data['worker']
            store.db.execute("UPDATE workers SET status='running' WHERE id=?", (worker,))
            service = Service(store, object(), object(), Path(root))
            control = AsyncMock()
            control.goal.return_value = 'received'
            service.workers.controls[worker] = control
            for condition in ('Tests pass', 'clear'):
                result = control_api.call(store, 'workers.goal', {'worker': worker, 'condition': condition})
                self.assertTrue(result.ok, result.text)
                self.assertTrue(await service.controls_once())
                self.assertEqual(control.goal.await_args.args[1], condition)
                self.assertEqual(store.db.execute('SELECT state FROM service_requests ORDER BY id DESC LIMIT 1')
                                 .fetchone()[0], 'done')
            control.steer.assert_not_awaited()

    async def test_steering_a_queued_worker_cannot_grow_its_brief_past_the_limit(self):
        with tempfile.TemporaryDirectory() as root:
            store = Store(Path(root))
            store.put('accounts', {'work': {'config_dir': None, 'enabled': True}})
            try:
                with store.db:
                    store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test','',1)")
                task = store.task_create('1:2', 'Build', worktree=root)
                brief = 'b' * (control_api.BRIEF_LIMIT - 100)
                worker = control_api.call(store, 'workers.spawn', {'task': task['id'], 'provider': 'claude',
                                                                   'prompt': brief}).data['worker']
                control_api.call(store, 'workers.steer', {'worker': worker, 'prompt': 'short follow-up'})
                control_api.call(store, 'workers.steer', {'worker': worker, 'prompt': 'x' * 200})
                service = Service(store, object(), object(), Path(root))
                self.assertTrue(await service.controls_once())
                prompt = store.db.execute('SELECT prompt FROM workers WHERE id=?', (worker,)).fetchone()[0]
                self.assertEqual(prompt, brief + '\n\nFollow-up:\nshort follow-up')
                states = [tuple(row) for row in store.db.execute('SELECT state,result FROM service_requests ORDER BY id')]
                self.assertEqual(states[0], ('done', None))
                self.assertEqual(states[1][0], 'refused')
                self.assertIn('would exceed', states[1][1])
            finally:
                store.close()

if __name__ == '__main__':
    unittest.main()
