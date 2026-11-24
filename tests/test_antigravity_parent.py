"""Offline Antigravity chat agent: selection, launch, MCP, resume, usage and fallback against the fake agy."""

import json
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import antigravity_parent, antigravity_runtime, control_api, parent, parent_state
from coordinator.accounts import AccountBroker
from coordinator.extension import available
from coordinator.service import Service
from coordinator.session import AccountsUnavailable, message_uuid
from tests.support import settle, until
from tests.test_antigravity_runtime import AntigravityCase


class ParentCase(AntigravityCase):
    def setUp(self):
        super().setUp()
        self.register()
        statuses = self.store.get('antigravity_account_status')
        statuses['one']['models'].append({'id': 'model-b-low', 'name': 'Model B (Low)'})
        with self.store.db:
            self.store.put('antigravity_account_status', statuses)
            self.store.put('coordinator_home_topic', 'home')
            for topic, thread in (('home', 1), ('chat', 2)):
                self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,1,?,?,?,1)',
                                      (topic, thread, topic, str(self.cwd)))
        self.parents = []

    async def asyncTearDown(self):
        for session in self.parents:
            if session.process is not None and session.process.returncode is None:
                await session.stop()

    def pick(self, topic='chat', **params):
        with self.store.db:
            return control_api.call(self.store, 'model.parent', {'topic': topic, 'provider': 'antigravity', **params})

    async def start(self, instructions='TORII_RULES_FOR_THIS_CHAT'):
        session = await parent.start(self.store, self.runner, self.cwd, 'claude-opus-5-5', instructions, topic='chat')
        if session is not None:
            self.parents.append(session)
        return session

    def row(self, text='Hello'):
        row = self.store.message_save('chat', 'owner', text)
        self.store.db.execute('UPDATE messages SET telegram_message=? WHERE id=?', (row['id'] + 100, row['id']))
        self.store.db.commit()
        return row

    async def send(self, session, row):
        prompt = await Service(self.store, None, self.runner, self.cwd)._prepare_message(row, session)
        self.store.message_sending(row['id'])
        receipt = await session.send(message_uuid(row['id']), prompt, row_id=row['id'])
        self.store.message_delivered(row['id'], receipt)
        self.assertEqual(receipt, 'received')
        await until(session.idle)
        return prompt


class SelectionTests(ParentCase):
    def test_model_parent_accepts_an_offered_model_with_effort(self):
        result = self.pick(model='model-b-low', effort='high')
        self.assertTrue(result.ok, result.text)
        self.assertEqual(result.data['selection'], {
            'provider': 'antigravity', 'model': 'model-b-low', 'effort': 'high', 'revision': 1})
        agent = parent_state.effective_agent(self.store, 'coordinator:chat')
        self.assertEqual((agent['provider'], agent['model'], agent['effort']), ('antigravity', 'model-b-low', 'high'))

    def test_a_chat_with_no_pick_does_not_reach_antigravity_by_default(self):
        self.assertEqual(parent_state.providers(self.store, 'coordinator:chat'), ['claude', 'codex'])

    @unittest.skipUnless(available('routing'), 'route lists name Antigravity')
    def test_a_chat_reaches_antigravity_only_through_the_general_list_and_plugin_chats_skip_it(self):
        key = 'coordinator:chat'
        self.store.put('routes', {'general': ['codex', 'antigravity/gemini-4-neon-high']})
        self.assertEqual(parent_state.providers(self.store, key), ['claude', 'codex', 'antigravity'])
        self.store.put('plugin_chats', {'plugin': ['another-chat']})
        self.assertEqual(parent_state.providers(self.store, key), ['claude', 'codex', 'antigravity'])
        self.store.put('plugin_chats', {'plugin': ['chat']})
        self.assertEqual(parent_state.providers(self.store, key), ['claude', 'codex'])
        self.assertTrue(self.pick().ok)
        self.assertEqual(parent_state.providers(self.store, key), ['antigravity', 'codex', 'claude'])

    def test_default_model_is_the_antigravity_default(self):
        self.assertTrue(self.pick().ok)
        self.assertEqual(parent_state.effective_agent(self.store, 'coordinator:chat')['model'], 'model-a')

    def test_model_parent_refusals(self):
        cases = [({'model': 'model-z'}, 'No Antigravity account offers model model-z')]
        for params, text in cases:
            with self.subTest(params=params):
                options = {'topic': 'chat', 'provider': 'antigravity', **params}
                with self.store.db:
                    result = control_api.call(self.store, 'model.parent',
                                              {name: value for name, value in options.items() if value is not None})
                self.assertFalse(result.ok)
                self.assertIn(text, result.text)
        self.assertIsNone(self.store.get('coordinator:chat_selection'))
        statuses = self.store.get('antigravity_account_status')
        statuses['one']['logged_in'] = False
        self.store.put('antigravity_account_status', statuses)
        self.assertIn('No Antigravity account is signed in', self.pick().text)

    def test_unknown_provider_text_is_unchanged(self):
        with self.store.db:
            result = control_api.call(self.store, 'model.parent', {'topic': 'chat', 'provider': 'other'})
        self.assertEqual(result.text, 'Send claude, codex or opencode for provider.')

    def test_topic_create_checks_the_antigravity_pick(self):
        statuses = self.store.get('antigravity_account_status')
        statuses['one']['logged_in'] = False
        self.store.put('antigravity_account_status', statuses)
        self.store.put('group', 1)
        asking = self.store.message_save('home', 'owner', 'Make a writing chat')
        result = control_api.call(self.store, 'topic.create', {'name': 'Writing', 'provider': 'antigravity',
                                                                'message': asking['id']}, topic='home')
        self.assertFalse(result.ok)
        self.assertIn('No Antigravity account is signed in', result.text)

    async def test_an_antigravity_chat_moves_along_its_queue_while_antigravity_is_out(self):
        out = set()
        claude = SimpleNamespace(select=lambda model=None: None if 'claude' in out else 'c',
                                 earliest_reset=lambda model=None: None)
        codex = SimpleNamespace(parent_account=lambda: None if 'codex' in out else 'g', active=lambda: 'g')
        self.assertTrue(self.pick().ok)
        key = 'coordinator:chat'

        async def choose():
            with patch('coordinator.parent.codex_binary', return_value='codex'):
                return await parent.choose_parent(self.store, claude, codex, key)
        self.assertEqual(await choose(), ('antigravity', 'one'))
        self.assertFalse(parent_state.provider_out(self.store, key, 'antigravity'))
        self.store.put('antigravity_account_blocks', {'one': time.time() + 3600})
        self.assertTrue(parent_state.provider_out(self.store, key, 'antigravity'))
        order = parent_state.providers(self.store, key)
        self.assertEqual((order[0], sorted(order[1:])), ('antigravity', ['claude', 'codex']))
        account = {'claude': 'c', 'codex': 'g'}
        self.assertEqual(await choose(), (order[1], account[order[1]]))
        out.add(order[1])
        self.assertEqual(await choose(), (order[2], account[order[2]]))
        self.assertEqual(parent.switch_reason(self.store, 'codex', 'claude', claude, codex, key),
                         'Antigravity is out of usage')
        session = SimpleNamespace(provider='claude', store=self.store, key=key, model='claude-sonnet-5-5')
        self.store.put(key + '_host', {'provider': 'claude', 'picked': True,
                                       'effort': parent_state.effective_agent(self.store, key, 'claude')['effort']})
        self.store.put(key + '_selection_revision', self.store.get(key + '_selection')['revision'])
        self.assertFalse(parent_state.selection_changed(session))
        self.store.put('antigravity_account_blocks', {})
        self.assertTrue(parent_state.selection_changed(session))
        self.store.put('antigravity_account_blocks', {'one': time.time() + 3600})
        self.store.put(key + '_host', None)
        out.update(('claude', 'codex'))
        with self.assertRaises(AccountsUnavailable) as raised:
            await choose()
        self.assertEqual((raised.exception.provider, raised.exception.alias), ('antigravity', 'one'))

    def test_model_level_splits_coordinator_ids(self):
        families = ['gemini-4-neon', 'gemini-pro-orchestrator-neon']
        levels = ['low', 'medium', 'high', 'xhigh']
        self.assertEqual(antigravity_runtime.model_level('gemini-4-neon-high', families, levels), ('gemini-4-neon', 'high'))
        self.assertEqual(antigravity_runtime.model_level('gemini-pro-orchestrator-neon', families, levels),
                         ('gemini-pro-orchestrator-neon', None))


class SessionTests(ParentCase):
    def setUp(self):
        super().setUp()
        self.assertTrue(self.pick(model='model-b-low').ok)

    async def test_launch_argv_env_rules_and_mcp_servers(self):
        entry = {'type': 'http', 'url': 'https://example.invalid/chat', 'headers': {'Authorization': 'Bearer chat-only'}}
        other = {'type': 'http', 'url': 'https://example.invalid/home', 'headers': {}}
        (self.home / '.claude.json').write_text(json.dumps({'mcpServers': {'notes': entry, 'work': other}}))
        self.store.put('chat_config:chat', {'mcp_servers': ['notes']})
        session = await self.start()
        self.assertEqual((session.provider, session.account_alias, session.model), ('antigravity', 'one', 'model-b-low'))
        spec = json.loads((session.process.directory / 'spec.json').read_text())
        self.assertEqual(spec['argv'], [str(self.binary), 'acp', '--dangerously-skip-permissions'])
        self.assertEqual(spec['env']['HOME'], str(self.account))
        self.assertNotIn('GEMINI_API_KEY', spec['env'])
        created = self.calls('session/new')[0]
        params = created['message']['params']
        self.assertEqual(created['home'], str(self.account))
        rules = params['_meta']['antigravity']['systemPrompt']['append']
        self.assertTrue(rules.startswith('TORII_RULES_FOR_THIS_CHAT\nCurrent usage policy:\n'))
        torii, notes = params['mcpServers']
        self.assertEqual(torii['name'], 'torii')
        self.assertEqual(torii['args'][-2:], ['mcp', '--topic=chat'])
        self.assertEqual(notes, {'type': 'http', 'name': 'notes', 'url': entry['url'],
                                 'headers': [{'name': 'Authorization', 'value': 'Bearer chat-only'}]})
        for path in session.process.directory.rglob('*'):
            if path.is_file():
                self.assertNotIn(b'chat-only', path.read_bytes(), path.name)
        options = [call['message']['params'] for call in self.calls('session/set_config_option')]
        self.assertEqual([(item['configId'], item['value']) for item in options],
                         [('model', 'model-b'), ('thought_level', 'low')])

    async def test_turn_reply_tool_reaction_and_token_count(self):
        self.scenario(tool='Run mcp_torii_tasks_list')
        session = await self.start()
        row = self.row('Answer me')
        prompt = await self.send(session, row)
        self.assertIn('message=%d kind=owner' % row['id'], prompt)
        reply = self.store.db.execute("SELECT text FROM outbox WHERE topic='chat' AND kind='parent-reply'").fetchone()
        self.assertIn('turn 1 model-b:low', reply['text'])
        saved = self.store.get('coordinator:chat_antigravity')
        self.assertEqual((saved['account'], saved['session'], saved['host']), ('one', session.session_id, session.host_id))
        self.assertEqual(saved['tokens'], {'input_tokens': 100, 'output_tokens': 20, 'cached_tokens': 300, 'thought_tokens': 5})
        self.assertEqual(session.receipt_rows(session.transcript_path, [row['id'], row['id'] + 99]), {row['id']})
        tools = self.store.db.execute('SELECT turn_tools FROM messages WHERE id=?', (row['id'],)).fetchone()[0]
        self.assertGreaterEqual(tools, 1)

    async def test_owner_image_instructions_name_view_file(self):
        session = await self.start()
        from coordinator.media import attachment_instructions
        text = attachment_instructions([{'mime': 'image/png', 'path': '/tmp/owner.png', 'size': 3}],
                                       coordinator=True, provider=session.provider)
        self.assertIn('Open each image with view_file', text)

    async def test_reattach_keeps_the_host_and_restart_resumes_the_session(self):
        entry = {'type': 'http', 'url': 'https://example.invalid/chat', 'headers': {'Authorization': 'Bearer chat-only'}}
        (self.home / '.claude.json').write_text(json.dumps({'mcpServers': {'notes': entry}}))
        self.store.put('chat_config:chat', {'mcp_servers': ['notes']})
        session = await self.start()
        sid, host = session.session_id, session.host_id
        await session.detach()
        again = await self.start()
        self.assertEqual((again.host_id, again.session_id), (host, sid))
        self.assertEqual(len(self.calls('session/new')), 1)
        await again.stop()
        resumed = await self.start()
        self.assertNotEqual(resumed.host_id, host)
        self.assertEqual(resumed.session_id, sid)
        resume = self.calls('session/resume')[-1]['message']['params']
        self.assertEqual(resume['sessionId'], sid)
        self.assertEqual(resume['mcpServers'][-1]['headers'], [{'name': 'Authorization', 'value': 'Bearer chat-only'}])
        self.assertIn('TORII_RULES_FOR_THIS_CHAT', resume['_meta']['antigravity']['systemPrompt']['append'])
        await self.send(resumed, self.row('After restart'))

    async def test_changed_personal_rules_keep_the_host_and_reach_the_next_turn(self):
        from coordinator.policy import personal_path
        session = await self.start()
        host = session.host_id
        await session.detach()
        personal_path(self.store).write_text('- Use one worker.\n')
        again = await self.start()
        self.assertFalse(again.outdated)
        self.assertEqual(again.host_id, host)
        self.assertIsNone(again.usage_policy)
        await again.stop()

    async def test_changed_rules_relaunch_on_the_same_session(self):
        session = await self.start()
        sid = session.session_id
        await session.detach()
        again = await self.start('CHANGED_RULES')
        self.assertTrue(again.outdated)
        await settle(again.wait_closed())
        resumed = await self.start('CHANGED_RULES')
        self.assertEqual(resumed.session_id, sid)
        self.assertIn('CHANGED_RULES', self.calls('session/resume')[-1]['message']['params']['_meta']['antigravity'][
            'systemPrompt']['append'])

    async def test_quota_refusal_holds_the_account_and_settles_the_turn(self):
        self.scenario(quota=True)
        session = await self.start()
        row = self.row('Out of usage')
        prompt = await Service(self.store, None, self.runner, self.cwd)._prepare_message(row, session)
        self.store.message_sending(row['id'])
        self.assertEqual(await session.send(message_uuid(row['id']), prompt, row_id=row['id']), 'received')
        await settle(session.wait_closed())
        self.assertGreater(self.store.get('antigravity_account_blocks')['one'], time.time())
        self.assertTrue(antigravity_parent.out(self.store, 'coordinator:chat'))
        callback = self.store.db.execute("SELECT text FROM messages WHERE topic='chat' AND kind='callback'").fetchone()
        self.assertIn('message=%d' % row['id'], callback['text'])
        with self.assertRaises(AccountsUnavailable) as raised:
            await self.start()
        self.assertEqual(raised.exception.provider, 'antigravity')

    async def test_account_switch_starts_a_new_session(self):
        session = await self.start()
        await session.stop()
        self.store.put('coordinator:chat_antigravity', dict(self.store.get('coordinator:chat_antigravity'), account='gone'))
        with patch.object(antigravity_parent, 'account', return_value='one'):
            fresh = await self.start()
        self.assertNotEqual(fresh.session_id, session.session_id)
        self.assertEqual(len(self.calls('session/new')), 2)
        self.assertEqual(fresh.switch['to'], 'antigravity')



class HomeTests(ParentCase):
    def pick_home(self, **params):
        with self.store.db:
            return control_api.call(self.store, 'model.parent', {'topic': 'home', 'provider': 'antigravity', **params})

    async def start_home(self):
        session = await parent.start(self.store, self.runner, self.cwd, 'claude-opus-5-5', 'HOME_RULES', topic='home')
        if getattr(session, 'protocol', None) is not None:
            self.parents.append(session)
        return session

    def claude(self):
        started = []

        async def fake(store, runner, cwd, model, instructions, **kwargs):
            started.append(model)
            return SimpleNamespace(provider='claude', host_id='claude-host', model=model)
        return started, patch.object(parent.CoordinatorSession, 'start', fake)

    async def test_home_chat_and_the_default_accept_antigravity(self):
        result = self.pick_home()
        self.assertTrue(result.ok, result.text)
        with self.store.db:
            default = control_api.call(self.store, 'model.parent', {'provider': 'antigravity'})
        self.assertTrue(default.ok, default.text)
        self.assertEqual(self.store.get('parent_selection'), {'provider': 'antigravity', 'model': 'model-a'})
        session = await self.start_home()
        self.assertEqual((session.provider, session.key, session.model), ('antigravity', 'coordinator', 'model-a'))
        self.assertIn('--topic=home', self.calls('session/new')[0]['message']['params']['mcpServers'][0]['args'])

    async def test_failed_home_start_runs_on_default_claude_until_antigravity_is_back(self):
        self.assertTrue(self.pick_home().ok)
        (self.account / 'signed-in').unlink()
        started, fake = self.claude()
        with fake, patch.object(AccountBroker, 'select', return_value='c'):
            session = await self.start_home()
            self.assertEqual(session.provider, 'claude')
            self.assertEqual(started, ['claude-opus-5-5'])
            self.assertEqual(self.store.get('coordinator_start_failed')['provider'], 'antigravity')
            self.assertTrue(parent_state.provider_out(self.store, 'coordinator', 'antigravity'))
            self.assertTrue(parent.available(self.store, AccountBroker(self.store),
                                             SimpleNamespace(parent_account=lambda: None), key='coordinator'))
            claude = SimpleNamespace(provider='claude', store=self.store, key='coordinator', model=started[0])
            self.store.put('coordinator_host', {'provider': 'claude', 'picked': True,
                                                'effort': parent_state.effective_agent(self.store, 'coordinator', 'claude')['effort']})
            self.store.put('coordinator_selection_revision', self.store.get('coordinator_selection')['revision'])
            self.assertFalse(parent_state.selection_changed(claude))
            failed = self.store.get('coordinator_start_failed')
            self.store.put('coordinator_start_failed', dict(failed, until=time.time() - 1))
            self.store.put('coordinator_host', None)
            (self.account / 'signed-in').write_text('fixture\n')
            self.assertFalse(parent_state.provider_out(self.store, 'coordinator', 'antigravity'))
            self.assertTrue(parent_state.selection_changed(claude))
            back = await self.start_home()
        self.assertEqual(back.provider, 'antigravity')
        self.assertIsNone(self.store.get('coordinator_start_failed'))
        self.assertEqual(back.switch['to'], 'antigravity')

    async def test_a_chat_out_of_usage_runs_on_the_next_agent_of_its_queue(self):
        self.assertTrue(self.pick().ok)
        self.store.put('antigravity_account_blocks', {'one': time.time() + 3600})
        started, fake = self.claude()
        with fake, patch.object(AccountBroker, 'select', return_value='c'):
            session = await parent.start(self.store, self.runner, self.cwd, 'claude-opus-5-5', 'RULES', topic='chat')
        self.assertEqual((session.provider, started), ('claude', ['claude-opus-5-5']))

    async def test_home_pause_says_no_other_agent_can_take_it(self):
        self.assertTrue(self.pick_home().ok)
        self.store.put('antigravity_account_blocks', {'one': time.time() + 3600})
        with patch.object(AccountBroker, 'select', return_value=None):
            with self.assertRaises(AccountsUnavailable) as raised:
                await self.start_home()
        Service(self.store, None, self.runner, self.cwd).pause_parent('home', raised.exception)
        text = self.store.get('coordinator_selection_notice')['text']
        self.assertIn('No other agent of its queue can take it now.', text)
        self.assertNotIn('fallback', text)

    async def test_a_chat_pauses_when_no_agent_of_its_queue_can_run(self):
        self.assertTrue(self.pick().ok)
        self.store.put('antigravity_account_blocks', {'one': time.time() + 3600})
        with patch.object(AccountBroker, 'select', return_value=None):
            with self.assertRaises(AccountsUnavailable) as raised:
                await self.start()
        self.assertEqual(raised.exception.provider, 'antigravity')


if __name__ == '__main__':
    unittest.main()
