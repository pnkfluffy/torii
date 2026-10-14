import asyncio
import json
import logging
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch
import uuid

from coordinator import control_api, parent, parent_state
from coordinator.providers import ProviderRunner
from coordinator.service import Service
from coordinator.store import Store
from tests.support import isolate_shared_mcp_home, settle, stop_test_hosts, until
from tests.test_codex_parent import APP_SERVER
from tests.test_parent_turns import FAKE


class ParentSelectionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.addCleanup(self.temporary.cleanup)
        self.addCleanup(stop_test_hosts, self.root)
        isolate_shared_mcp_home(self, self.root)
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        claude, codex = self.root / 'claude', self.root / 'codex'
        claude.write_text(FAKE)
        codex.write_text(APP_SERVER.replace(
            "assert 'policy' in params['developerInstructions'] and params['model']=='test-codex'",
            "assert params['model'] in ('test-codex','test-astra')"))
        claude.chmod(0o700)
        codex.chmod(0o700)
        for profile in ('claude-home', 'codex-home'):
            (self.root / profile).mkdir()
        now = time.time()
        with self.store.db:
            self.store.put('owner', 1)
            self.store.put('group', 1)
            self.store.put('accounts', {'c': {'config_dir': str(self.root / 'claude-home'), 'enabled': True}})
            self.store.put('account_status', {'c': {'identity': {'logged_in': True, 'email': 'claude@example.test'}, 'observed_at': now,
                           'usage': {'five_hour': {'utilization': 0},
                                     'seven_day': {'utilization': 0, 'resets_at': now + 3600}}}})
            self.store.put('codex_accounts', {'g': {'config_dir': str(self.root / 'codex-home'), 'enabled': True}})
            self.store.put('codex_account_status', {'g': {'identity': {'logged_in': True, 'email': 'codex@example.test'}}})
            self.store.put('codex_active_account', 'g')
            self.store.put('codex_model', 'test-codex')
            self.store.put('coordinator_home_topic', 'home')
            for topic in ('home', 'other'):
                self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES(?,?,?,?,?,1)',
                                      (topic, 1, 1 if topic == 'home' else 2, topic, str(self.root)))
        self.runner = ProviderRunner(self.store.directory, {'claude': str(claude), 'codex': str(codex)})
        self.service = Service(self.store, None, self.runner, self.root)
        self.parents = []

    async def asyncTearDown(self):
        for task in self.service.feed_tasks.values():
            task.cancel()
        await asyncio.gather(*self.service.feed_tasks.values(), return_exceptions=True)
        for session in self.parents:
            await session.stop()

    def select(self, provider, model):
        with self.store.db:
            result = control_api.call(self.store, 'model.parent', {'provider': provider, 'model': model})
        self.assertTrue(result.ok, result.text)

    async def start(self, topic=None):
        topic = topic or self.service.home_topic()
        session = await self.service.start_session(topic)
        self.assertIsNotNone(session)
        self.parents.append(session)
        if session.provider == 'claude':
            self.assertEqual(await session.send(str(uuid.uuid4()), 'ready'), 'received')
        await until(lambda: session.idle() or session.closed)
        return session

    def prepare_selection_scenario(self):
        with self.store.db:
            self.store.put('accounts', {'personal': next(iter(self.store.get('accounts').values()))})
            self.store.put('account_status', {'personal': {
                **next(iter(self.store.get('account_status').values())),
                'identity': {'logged_in': True, 'email': 'personal@example.test'}}})
            self.store.put('codex_accounts', {'team': next(iter(self.store.get('codex_accounts').values()))})
            self.store.put('codex_account_status', {'team': {
                'identity': {'logged_in': True, 'email': 'team@example.test'}}})
            self.store.put('codex_active_account', 'team')
            self.store.put('coordinator_home_topic', 'Atlas')
            self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES(?,?,?,?,?,1)',
                                  ('Atlas', 1, 3, 'Atlas', str(self.root)))

    def limit_claude(self, reset):
        status = self.store.get('account_status')
        for snapshot in status.values():
            snapshot['usage']['five_hour'] = {'utilization': 98, 'resets_at': reset}
        with self.store.db:
            self.store.put('account_status', status)

    def prepare_owner_pick_scenario(self):
        self.prepare_selection_scenario()
        profile = self.root / 'work-claude'
        profile.mkdir()
        now = time.time()
        with self.store.db:
            self.store.put('accounts', {**self.store.get('accounts'), 'work': {
                'config_dir': str(profile), 'enabled': True}})
            self.store.put('account_status', {**self.store.get('account_status'), 'work': {
                'identity': {'logged_in': True, 'email': 'work@example.test'}, 'observed_at': now,
                'usage': {'five_hour': {'utilization': 0},
                          'seven_day': {'utilization': 0, 'resets_at': now + 7200}}}})
            result = control_api.call(self.store, 'account.use', {'alias': 'work'}, topic='Atlas')
        self.assertTrue(result.ok, result.text)
        self.assertEqual(self.service.accounts.select(), 'work')

    async def feed_owner(self, session, text, telegram_message):
        completed = asyncio.Event()
        session.on_turn = lambda reply: completed.set()
        row = self.store.message_save('Atlas', 'owner', text, telegram_message=telegram_message)
        await self.service.feed_once()
        await settle(asyncio.gather(*self.service.feed_tasks.values()))
        await settle(completed.wait())
        self.assertEqual(tuple(self.store.db.execute('SELECT delivered,receipt FROM messages WHERE id=?',
                                                   (row['id'],)).fetchone()), ('received', 'replayed'))
        return row

    async def assert_documented_overrides_feed_is_quiet(self, accounts):
        self.prepare_selection_scenario()
        override = self.root / 'local-overrides.json'
        override.write_text(json.dumps({'shared_mcp_servers': []}))
        listed, status = self.store.get('accounts'), self.store.get('account_status')
        for index in range(1, accounts):
            alias = 'extra%d' % index
            profile = self.root / alias
            profile.mkdir()
            listed[alias] = {'config_dir': str(profile), 'enabled': True}
            status[alias] = json.loads(json.dumps(status['personal']))
            status[alias]['identity']['email'] = alias + '@example.test'
        with self.store.db:
            self.store.put('accounts', listed)
            self.store.put('account_status', status)
        self.limit_claude(time.time() + 7200)
        self.select('claude', 'claude-opus-5-5')
        with patch('coordinator.accounts.LOCAL_OVERRIDES_PATH', override), \
                patch.object(logging.getLogger('coordinator.accounts'), 'warning') as warning:
            session = await self.start()
            self.assertEqual(session.provider, 'codex')
            with patch.object(self.service, 'feed_once', wraps=self.service.feed_once) as ticks:
                feed = asyncio.create_task(self.service.feed())
                try:
                    await until(lambda: ticks.call_count >= 20)
                finally:
                    feed.cancel()
                    await asyncio.gather(feed, return_exceptions=True)
            self.assertIs(self.service.sessions.get('Atlas'), session)
            self.assertFalse(session.rejected)
            warning.assert_not_called()

    async def test_documented_overrides_fallback_feed_logs_no_warning_with_one_account(self):
        await self.assert_documented_overrides_feed_is_quiet(1)

    async def test_documented_overrides_fallback_feed_logs_no_warning_with_three_accounts(self):
        await self.assert_documented_overrides_feed_is_quiet(3)

    async def test_return_notice_names_claude_recovery_when_selection_is_unchanged(self):
        self.prepare_selection_scenario()
        self.select('claude', 'claude-opus-5-5')
        self.limit_claude(time.time() + 7200)
        before = [await self.start(topic) for topic in ('Atlas', 'other')]
        self.assertTrue(all(session.provider == 'codex' for session in before))
        self.select('claude', 'claude-opus-5-5')
        status = self.store.get('account_status')
        status['personal']['usage']['five_hour']['utilization'] = 0
        with self.store.db:
            self.store.put('account_status', status)
        with patch('coordinator.host.HostClient.stop', AsyncMock(side_effect=AssertionError('No signal stop'))):
            await self.service.feed_once()
            await settle(asyncio.gather(*(session.wait_closed() for session in before)))
        after = [await self.start(topic) for topic in ('Atlas', 'other')]
        self.assertTrue(all(session.provider == 'claude' for session in after))
        for topic in ('Atlas', 'other'):
            note = self.store.db.execute("SELECT text FROM outbox WHERE topic=? AND text LIKE "
                                         "'This chat now runs on%' ORDER BY id DESC LIMIT 1", (topic,)).fetchone()
            self.assertEqual(note['text'], 'This chat now runs on Claude (Claude can take work again).')

    async def test_provider_change_notice_names_owner_selection(self):
        self.select('codex', 'test-codex')
        before = await self.start()
        self.select('claude', 'claude-opus-5-5')
        with patch('coordinator.host.HostClient.stop', AsyncMock(side_effect=AssertionError('No signal stop'))):
            await self.service.feed_once()
            await settle(before.wait_closed())
        after = await self.start()
        self.assertEqual(after.provider, 'claude')
        note = self.store.db.execute("SELECT text FROM outbox WHERE topic='home' AND text LIKE "
                                     "'This chat now runs on%' ORDER BY id DESC LIMIT 1").fetchone()
        self.assertEqual(note['text'], 'This chat now runs on Claude (the owner selected claude parent chats).')

    async def test_owner_provider_change_during_fallback_keeps_owner_reason(self):
        self.prepare_selection_scenario()
        self.select('claude', 'claude-opus-5-5')
        self.limit_claude(time.time() + 7200)
        before = await self.start()
        self.assertEqual(before.provider, 'codex')
        self.select('codex', 'test-codex')
        await self.service.feed_once()
        self.assertIs(self.service.sessions.get('Atlas'), before)
        self.assertFalse(before.rejected)
        status = self.store.get('account_status')
        status['personal']['usage']['five_hour']['utilization'] = 0
        with self.store.db:
            self.store.put('account_status', status)
        self.select('claude', 'claude-opus-5-5')
        with patch('coordinator.host.HostClient.stop', AsyncMock(side_effect=AssertionError('No signal stop'))):
            await self.service.feed_once()
            await settle(before.wait_closed())
        after = await self.start()
        self.assertEqual(after.provider, 'claude')
        note = self.store.db.execute("SELECT text FROM outbox WHERE topic='Atlas' AND text LIKE "
                                     "'This chat now runs on%' ORDER BY id DESC LIMIT 1").fetchone()
        self.assertEqual(note['text'], 'This chat now runs on Claude (the owner selected claude parent chats).')

    async def test_claude_selection_falls_back_to_active_chatgpt_with_its_model_and_quota_notice(self):
        self.prepare_selection_scenario()
        self.select('claude', 'claude-opus-5-5')
        profile = self.root / 'work-claude'
        profile.mkdir()
        with self.store.db:
            self.store.put('accounts', {**self.store.get('accounts'), 'work': {
                'config_dir': str(profile), 'enabled': True}})
            self.store.put('account_status', {**self.store.get('account_status'),
                           'work': self.store.get('account_status')['personal']})
        reset = time.time() + 7200
        self.limit_claude(reset)
        with self.store.db:
            self.store.put('coordinator_provider', 'claude')
        self.service.pause_parent('Atlas', parent.unavailable(
            self.store, self.service.accounts, self.service.codex_accounts))
        before = await self.start()
        self.assertEqual((before.provider, before.account_alias, before.model), ('codex', 'team', 'test-codex'))
        self.assertFalse(before.rejected)
        self.assertEqual(before.switch['reason'], 'every Claude account is out of usage')
        self.assertIsNone(self.store.get('coordinator_account_retry_at'))
        notice = self.store.db.execute("SELECT text FROM outbox WHERE topic='Atlas' AND text LIKE 'This chat now runs on%'").fetchone()
        self.assertEqual(notice['text'], 'This chat now runs on ChatGPT (every Claude account is out of usage).'
                         ' It returns to your selected Claude model after Claude recovers and current work settles.')
        requests = [json.loads(line) for line in (self.root / 'requests').read_text().splitlines()]
        launches = [request['params']['model'] for request in requests if request.get('method') == 'thread/start']
        self.assertEqual(launches, ['test-codex'])
        self.assertEqual(self.store.get('parent_selection'), {'provider': 'claude', 'model': 'claude-opus-5-5'})

    async def test_fallback_returns_to_selected_claude_only_after_turn_and_background_settle(self):
        self.prepare_selection_scenario()
        binary = self.root / 'claude'
        binary.write_text(binary.read_text().replace("    text=event['message']['content']", """    text=event['message']['content']
    with (root/'claude-inputs').open('a') as log:
        log.write(json.dumps(event)+'\\n')"""))
        self.select('claude', 'claude-opus-5-5')
        self.limit_claude(time.time() + 7200)
        before = await self.start()
        row = self.store.message_save('Atlas', 'owner', 'hold', telegram_message=10)
        await self.service.feed_once()
        await settle(asyncio.gather(*self.service.feed_tasks.values()))
        self.assertTrue(before.busy)
        status = self.store.get('account_status')
        status['personal']['usage']['five_hour']['utilization'] = 0
        with self.store.db:
            self.store.put('account_status', status)
        before.protocol.background = [{'task_id': 'settling-work'}]
        await self.service.feed_once()
        self.assertFalse(before.rejected)
        completed = asyncio.Event()
        before.on_turn = lambda reply: completed.set()
        self.assertEqual(await before.send(str(uuid.uuid4()), 'release'), 'received')
        await settle(completed.wait())
        self.assertFalse(before.busy)
        await self.service.feed_once()
        self.assertFalse(before.rejected)
        text = 'Pending owner request ' + 'x' * 600 + '\nKeep the full final instruction.'
        pending = self.store.message_save('Atlas', 'owner', text, telegram_message=11)
        old_path = Path(before.transcript_path)
        with patch('coordinator.host.HostClient.stop', AsyncMock(side_effect=AssertionError('No signal stop'))):
            before.protocol.background = []
            await self.service.feed_once()
            await settle(before.wait_closed())
        after = await self.start()
        self.assertEqual((after.provider, after.model), ('claude', 'claude-opus-5-5'))
        self.assertTrue(after.switched)
        self.assertNotEqual(after.session_id, before.session_id)
        self.assertTrue(old_path.exists())
        completed = asyncio.Event()
        after.on_turn = lambda reply: completed.set() if reply.endswith(text) else None
        await self.service.feed_once()
        await settle(asyncio.gather(*self.service.feed_tasks.values()))
        await settle(completed.wait())
        events = [json.loads(line) for line in (self.root / 'claude-inputs').read_text().splitlines()]
        prompts = [event['message']['content'] for event in events if 'kind=owner]' in event['message']['content']]
        self.assertEqual(prompts, ['[topic=Atlas name="Atlas" message=%d kind=owner] ' % pending['id'] + text])
        for message in (row, pending):
            self.assertEqual(tuple(self.store.db.execute('SELECT delivered,receipt FROM messages WHERE id=?',
                                                       (message['id'],)).fetchone()), ('received', 'replayed'))

    async def test_fallback_idle_and_send_cycles_keep_the_same_host_while_claude_is_limited(self):
        self.prepare_selection_scenario()
        self.limit_claude(time.time() + 7200)
        before = await self.start()
        self.select('claude', 'claude-opus-5-5')
        identity = before.host_id, before.session_id, before.process.pid
        with patch('coordinator.host.HostClient.stop', AsyncMock(side_effect=AssertionError('No signal stop'))), \
                patch('coordinator.parent_state.retire', AsyncMock()) as retire:
            for index in range(3):
                await self.service.feed_once()
                self.assertFalse(before.rejected)
                self.assertIs(await self.service.start_session('Atlas'), before)
                await self.feed_owner(before, 'owner cycle %d' % index, 100 + index)
                self.assertTrue(before.idle())
                self.assertFalse(before.movable())
                self.assertEqual((before.host_id, before.session_id, before.process.pid), identity)
            retire.assert_not_called()
        self.assertEqual(len((self.root / 'launches').read_text().splitlines()), 1)

    async def test_chatgpt_fallback_returns_to_recovered_owner_pick_after_work_settles_and_keeps_history(self):
        self.prepare_owner_pick_scenario()
        self.select('claude', 'claude-opus-5-5')
        self.limit_claude(time.time() + 7200)
        before = await self.start()
        self.assertEqual((before.provider, before.account_alias), ('codex', 'team'))
        self.assertEqual(await before.send(str(uuid.uuid4()), 'hold'), 'received')
        self.assertTrue(before.busy)
        status = self.store.get('account_status')
        for snapshot in status.values():
            snapshot['usage']['five_hour']['utilization'] = 0
        with self.store.db:
            self.store.put('account_status', status)
        before.protocol.background = [{'task_id': 'settling-work'}]
        await self.service.feed_once()
        self.assertFalse(before.rejected)
        self.assertIs(await self.service.start_session('Atlas'), before)
        completed = asyncio.Event()
        before.on_turn = lambda reply: completed.set()
        self.assertEqual(await before.send(str(uuid.uuid4()), 'release'), 'received')
        await settle(completed.wait())
        await self.service.feed_once()
        self.assertFalse(before.busy)
        self.assertFalse(before.rejected)
        native = before.session_id
        history = Path(before.transcript_path)
        content = history.read_bytes()
        self.assertIn(native, history.name)
        self.assertIn(b'hold', content)
        with patch('coordinator.host.HostClient.stop', AsyncMock(side_effect=AssertionError('No signal stop'))):
            before.protocol.background = []
            await self.service.feed_once()
            await settle(before.wait_closed())
        after = await self.start()
        self.assertEqual((after.provider, after.account_alias, after.model),
                         ('claude', 'work', 'claude-opus-5-5'))
        self.assertNotEqual(after.session_id, native)
        self.assertEqual(history.read_bytes(), content)
        identity = after.host_id, after.session_id, after.process.pid
        for index in range(3):
            await self.service.feed_once()
            self.assertIs(await self.service.start_session('Atlas'), after)
            await self.feed_owner(after, 'picked account cycle %d' % index, 200 + index)
            self.assertFalse(after.movable())
            self.assertEqual((after.host_id, after.session_id, after.process.pid), identity)
        self.assertEqual(history.read_bytes(), content)
        self.assertEqual(len((self.root / 'launches').read_text().splitlines()), 1)
        self.assertEqual(len((self.root / 'launches.jsonl').read_text().splitlines()), 1)

    async def test_paused_owner_pick_uses_other_claude_then_returns_once_with_same_native_session(self):
        self.prepare_owner_pick_scenario()
        self.select('claude', 'claude-opus-5-5')
        status = self.store.get('account_status')
        status['work']['usage']['five_hour'] = {'utilization': 98, 'resets_at': time.time() + 7200}
        with self.store.db:
            self.store.put('account_status', status)
        before = await self.start()
        self.assertEqual((before.provider, before.account_alias), ('claude', 'personal'))
        completed = asyncio.Event()
        before.on_turn = lambda reply: completed.set()
        self.assertEqual(await before.send(str(uuid.uuid4()), 'slow'), 'received')
        await until(lambda: (self.root / 'slow.started').exists())
        status['work']['usage']['five_hour']['utilization'] = 0
        with self.store.db:
            self.store.put('account_status', status)
        before.protocol.background = [{'task_id': 'settling-work'}]
        await self.service.feed_once()
        self.assertTrue(before.busy)
        self.assertFalse(before.rejected)
        self.assertIs(await self.service.start_session('Atlas'), before)
        (self.root / 'slow.go').touch()
        await settle(completed.wait())
        await self.service.feed_once()
        self.assertFalse(before.busy)
        self.assertFalse(before.rejected)
        native = before.session_id
        history = Path(before.router.transcript(native))
        content = history.read_bytes()
        before.protocol.background = []
        pending = self.store.message_save('Atlas', 'owner', 'resume on the recovered pick', telegram_message=299)
        await self.service.feed_once()
        await settle(asyncio.gather(*self.service.feed_tasks.values()))
        await settle(before.wait_closed())
        after = await self.start()
        self.assertEqual((after.provider, after.account_alias, after.session_id), ('claude', 'work', native))
        spec = json.loads((self.runner.state_dir / 'hosts' / after.host_id / 'spec.json').read_text())
        self.assertTrue(Path(spec['argv'][spec['argv'].index('--resume') + 1]).samefile(history))
        self.assertEqual(history.read_bytes(), content)
        await self.service.feed_once()
        await self.service.feed_once()
        await settle(asyncio.gather(*self.service.feed_tasks.values()))
        await until(after.idle)
        self.assertEqual(tuple(self.store.db.execute('SELECT delivered,receipt FROM messages WHERE id=?',
                                                   (pending['id'],)).fetchone()), ('received', 'replayed'))
        identity = after.host_id, after.session_id, after.process.pid
        for index in range(3):
            await self.service.feed_once()
            self.assertIs(await self.service.start_session('Atlas'), after)
            await self.feed_owner(after, 'recovered pick cycle %d' % index, 300 + index)
            self.assertFalse(after.movable())
            self.assertEqual((after.host_id, after.session_id, after.process.pid), identity)
        self.assertEqual(len((self.root / 'launches.jsonl').read_text().splitlines()), 2)
        self.assertFalse((self.root / 'launches').exists())

    async def test_paused_owner_pick_does_not_ping_pong_the_chatgpt_fallback(self):
        self.prepare_owner_pick_scenario()
        self.select('claude', 'claude-opus-5-5')
        self.limit_claude(time.time() + 7200)
        session = await self.start()
        self.assertEqual((session.provider, session.account_alias), ('codex', 'team'))
        identity = session.host_id, session.session_id, session.process.pid
        for index in range(3):
            await self.service.feed_once()
            self.assertIs(await self.service.start_session('Atlas'), session)
            await self.feed_owner(session, 'paused pick cycle %d' % index, 400 + index)
            self.assertEqual(self.store.get('claude_account_pick'), 'work')
            self.assertFalse(session.movable())
            self.assertEqual((session.host_id, session.session_id, session.process.pid), identity)
        self.assertEqual(len((self.root / 'launches').read_text().splitlines()), 1)
        self.assertFalse((self.root / 'launches.jsonl').exists())

    async def test_claude_selection_with_both_providers_limited_pauses_at_the_earliest_reset(self):
        self.prepare_selection_scenario()
        self.select('claude', 'claude-opus-5-5')
        claude_reset, codex_reset = time.time() + 7200, time.time() + 3600
        self.limit_claude(claude_reset)
        profile = self.root / 'work-chatgpt'
        profile.mkdir()
        with self.store.db:
            self.store.put('codex_accounts', {**self.store.get('codex_accounts'), 'work': {
                'config_dir': str(profile), 'enabled': True}})
            self.store.put('codex_account_status', {**self.store.get('codex_account_status'), 'work': {
                'identity': {'logged_in': True, 'email': 'work@example.test'}}})
            self.store.put('codex_auto_switch', True)
            self.store.put('codex_account_blocks', {'team': {'reason': 'quota', 'until': codex_reset}})
        row = self.store.message_save('Atlas', 'owner', 'saved while both are limited', telegram_message=1)
        self.assertIsNone(await self.service.start_session('Atlas'))
        self.assertEqual(self.store.get('coordinator_account_retry_at'), codex_reset)
        self.assertEqual(self.store.get('coordinator_account_notice')['key'][:2], ['both', 'team'])
        for _ in range(3):
            await self.service.feed_once()
        notices = list(self.store.db.execute("SELECT text FROM outbox WHERE topic='Atlas'"))
        self.assertEqual(len(notices), 1)
        self.assertIn('no Claude or ChatGPT account can take work now', notices[0]['text'])
        self.assertIn('Your messages are saved.', notices[0]['text'])
        self.assertEqual(self.store.messages_pending()[0]['id'], row['id'])
        self.assertEqual(self.store.get('codex_active_account'), 'team')
        self.assertFalse((self.root / 'launches').exists())

    async def test_claude_selection_falls_back_when_usage_changes_during_launch(self):
        self.prepare_selection_scenario()
        self.select('claude', 'claude-opus-5-5')
        with self.store.db:
            self.store.put('coordinator_provider', 'claude')
        launch = parent.CoordinatorSession.start

        async def unavailable_at_launch(*args, **kwargs):
            self.limit_claude(time.time() + 7200)
            return await launch(*args, **kwargs)

        with patch.object(parent.CoordinatorSession, 'start', side_effect=unavailable_at_launch):
            session = await self.start()
        self.assertEqual((session.provider, session.account_alias, session.model), ('codex', 'team', 'test-codex'))
        self.assertEqual(session.switch['reason'], 'every Claude account is out of usage')
        self.assertFalse(session.rejected)

    async def test_codex_selection_does_not_fall_back_to_available_claude(self):
        self.prepare_selection_scenario()
        self.select('codex', 'test-astra')
        reset = time.time() + 3600
        with self.store.db:
            self.store.put('codex_account_blocks', {'team': {'reason': 'quota', 'until': reset}})
        self.assertIsNone(await self.service.start_session('Atlas'))
        self.assertEqual(self.store.get('coordinator_account_retry_at'), reset)
        self.assertEqual(self.store.get('coordinator_account_notice')['key'][:2], ['codex', 'team'])
        self.assertFalse((self.root / 'launches.jsonl').exists())

    async def assert_quota_handoff(self, provider, failed_close=False, same_provider_first=False):
        claude = self.root / 'claude'
        source = claude.read_text().replace("    text=event['message']['content']", """    text=event['message']['content']
    with (root/'claude-inputs').open('a') as log:
        log.write(json.dumps(dict(event,session_id=sid))+'\\n')
    transcript=Path(profile)/'projects'/'test'/(sid+'.jsonl')
    with transcript.open('a') as log:
        log.write(json.dumps({'type':'user','uuid':event['uuid']})+'\\n')""")
        if provider == 'claude':
            source = source.replace("    if text=='bg-work':", """    if (text.endswith('quota') and not (root/'claude-rejected').exists()) or ('kind=callback]' in text and profile.endswith('claude-next')):
        (root/'claude-rejected').touch()
        emit({'type':'assistant','message':{'role':'assistant','content':[{'type':'text','text':'working'}]}})
        emit({'type':'rate_limit_event','rate_limit_info':{'status':'rejected','resetsAt':time.time()+3600}})
        emit({'type':'result','subtype':'error','is_error':True,'result':"You're out of usage credits"})
        continue
    if text=='bg-work':""")
            codex = self.root / 'codex'
            codex.write_text(codex.read_text().replace("elif text.endswith('quota') or text.endswith('quota-hold'):",
                                                     "elif text.endswith('quota-hold'):"))
        else:
            self.store.put('account_blocks', {'c': {'reason': 'quota', 'until': time.time() + 3600}})
        if same_provider_first:
            profile = self.root / 'claude-next'
            profile.mkdir()
            self.store.put('accounts', {**self.store.get('accounts'), 'd': {
                'config_dir': str(profile), 'enabled': True}})
            self.store.put('account_status', {**self.store.get('account_status'), 'd': {
                'identity': {'logged_in': True, 'email': 'next@example.test'}, 'observed_at': time.time(),
                'usage': {'five_hour': {'utilization': 0},
                          'seven_day': {'utilization': 0, 'resets_at': time.time() + 7200}}}})
        claude.write_text(source)
        before = await self.start()
        self.assertEqual(before.provider, provider)
        text = 'Owner request ' + 'x' * 600 + '\nRequired final instruction beyond the excerpt. quota'
        row = self.store.message_save('home', 'owner', text, telegram_message=10)
        send = before.send

        async def delayed_receipt(message_id, prompt, row_id=None):
            receipt = await send(message_id, prompt, row_id=row_id)
            await before.wait_closed()
            return receipt

        failure = sqlite3.OperationalError('deterministic close settlement failure') if failed_close else None
        with patch.object(before, '_settle', wraps=before._settle, side_effect=failure), \
                patch.object(before, 'send', side_effect=delayed_receipt):
            await self.service.feed_once()
            await settle(asyncio.gather(*self.service.feed_tasks.values()))
        self.assertTrue(before.rejected)
        path = before.transcript_path or before.router.transcript(before.session_id)
        self.assertEqual(before.transcript_receipts(path, [row['id']]), {row['id']})
        saved = self.store.db.execute('SELECT delivered,receipt FROM messages WHERE id=?', (row['id'],)).fetchone()
        self.assertEqual(tuple(saved), ('received', 'replayed'))
        self.assertEqual(self.store.get(before.key + '_lost'), [row['id']] if failed_close else None)
        if provider == 'codex':
            self.store.put('account_blocks', {})
        if same_provider_first:
            rotated = await self.start()
            self.assertEqual((rotated.provider, rotated.account_alias, rotated.session_id),
                             ('claude', 'd', before.session_id))
            send = rotated.send

            async def rotated_receipt(message_id, prompt, row_id=None):
                receipt = await send(message_id, prompt, row_id=row_id)
                await rotated.wait_closed()
                return receipt

            with patch.object(rotated, 'send', side_effect=rotated_receipt):
                await self.service.feed_once()
                await settle(asyncio.gather(*self.service.feed_tasks.values()))
            events = [json.loads(line) for line in (self.root / 'claude-inputs').read_text().splitlines()]
            callbacks = [event['message']['content'] for event in events if 'kind=callback]' in event['message']['content']]
            self.assertEqual(callbacks, ['[topic=home name="home" message=2 kind=callback] ' +
                                        parent_state.LOST_TURN % 'message=1'])
        after = await self.start()
        self.assertEqual(after.provider, 'codex' if provider == 'claude' else 'claude')
        self.assertTrue(after.switched)
        self.assertNotEqual(after.session_id, before.session_id)
        self.assertEqual(tuple(self.store.db.execute('SELECT delivered,receipt FROM messages WHERE id=?',
                                                   (row['id'],)).fetchone()), ('pending', None))
        self.assertIsNone(self.store.get(before.key + '_lost'))
        for _ in range(3):
            self.assertIs(await self.service.start_session('home'), after)
            await self.service.feed_once()
            await settle(asyncio.gather(*self.service.feed_tasks.values()))
            await until(after.idle)
        native = after.session_id
        await after.detach()
        self.parents.remove(after)
        self.service.sessions.pop('home')
        after = await self.start()
        self.assertEqual(after.session_id, native)
        await self.service.feed_once()
        await settle(asyncio.gather(*self.service.feed_tasks.values()))
        await until(after.idle)
        if after.provider == 'codex':
            requests = [json.loads(line) for line in (self.root / 'requests').read_text().splitlines()]
            prompts = [request['params']['input'][0]['text'] for request in requests
                       if request.get('method') in ('turn/start', 'turn/steer')]
        else:
            events = [json.loads(line) for line in (self.root / 'claude-inputs').read_text().splitlines()]
            prompts = [event['message']['content'] for event in events if event['session_id'] == after.session_id]
        expected = '[topic=home name="home" message=1 kind=owner] ' + text
        self.assertEqual([prompt for prompt in prompts if 'kind=owner]' in prompt], [expected])
        self.assertFalse(any('already in this conversation' in prompt for prompt in prompts), prompts)
        self.assertEqual(tuple(self.store.db.execute('SELECT delivered,receipt FROM messages WHERE id=?',
                                                   (row['id'],)).fetchone()), ('received', 'replayed'))
        self.assertEqual(before.transcript_receipts(path, [row['id']]), {row['id']})

    async def test_claude_quota_handoff_to_codex_delivers_the_full_owner_message_once(self):
        await self.assert_quota_handoff('claude')

    async def test_codex_quota_handoff_to_claude_delivers_the_full_owner_message_once(self):
        await self.assert_quota_handoff('codex')

    async def test_claude_unsettled_quota_handoff_to_codex_delivers_the_full_owner_message_once(self):
        await self.assert_quota_handoff('claude', failed_close=True)

    async def test_codex_unsettled_quota_handoff_to_claude_delivers_the_full_owner_message_once(self):
        await self.assert_quota_handoff('codex', failed_close=True)

    async def test_rejected_same_provider_recovery_then_switch_delivers_the_full_owner_message_once(self):
        await self.assert_quota_handoff('claude', same_provider_first=True)

    async def assert_rotation_after_switch(self, failed_report):
        claude = self.root / 'claude'
        source = claude.read_text().replace("    text=event['message']['content']", """    text=event['message']['content']
    with (root/'claude-inputs').open('a') as log:
        log.write(json.dumps(dict(event,session_id=sid))+'\\n')
    transcript=Path(profile)/'projects'/'test'/(sid+'.jsonl')
    with transcript.open('a') as log:
        log.write(json.dumps({'type':'user','uuid':event['uuid']})+'\\n')""")
        source = source.replace("    if text=='bg-work':", """    if text.endswith('claude-quota') and profile.endswith('claude-home'):
        emit({'type':'assistant','message':{'role':'assistant','content':[{'type':'text','text':'working'}]}})
        emit({'type':'rate_limit_event','rate_limit_info':{'status':'rejected','resetsAt':time.time()+3600}})
        emit({'type':'result','subtype':'error','is_error':True,'result':"You're out of usage credits"})
        continue
    if text=='bg-work':""")
        claude.write_text(source)
        profile = self.root / 'claude-next'
        profile.mkdir()
        with self.store.db:
            self.store.put('accounts', {**self.store.get('accounts'), 'd': {
                'config_dir': str(profile), 'enabled': True}})
            self.store.put('account_status', {**self.store.get('account_status'), 'd': {
                'identity': {'logged_in': True, 'email': 'next@example.test'}, 'observed_at': time.time(),
                'usage': {'five_hour': {'utilization': 0},
                          'seven_day': {'utilization': 0, 'resets_at': time.time() + 7200}}}})
            self.store.put('account_blocks', {alias: {'reason': 'quota', 'until': time.time() + 3600}
                                              for alias in ('c', 'd')})
        first = await self.start()
        self.assertEqual(first.provider, 'codex')
        await first.stop()
        with self.store.db:
            self.store.put('account_blocks', {})
        report = self.store.enqueue_report
        failures = []

        def flaky_report(topic, text, *args, **kwargs):
            if failed_report and text.startswith('This chat now runs on') and not failures:
                failures.append(text)
                raise sqlite3.OperationalError('database is locked')
            return report(topic, text, *args, **kwargs)

        with patch.object(self.store, 'enqueue_report', side_effect=flaky_report):
            if failed_report:
                with self.assertRaisesRegex(sqlite3.OperationalError, 'database is locked'):
                    await self.service.start_session('home')
            else:
                await self.service.start_session('home')
        mid = self.service.sessions['home']
        self.parents.append(mid)
        self.assertEqual((mid.provider, mid.account_alias), ('claude', 'c'))
        self.assertEqual(len(failures), int(failed_report))
        self.assertEqual(self.store.get(mid.key + '_switch') is not None, failed_report)
        self.assertIs(await self.service.start_session('home'), mid)
        self.assertEqual(await mid.send(str(uuid.uuid4()), 'ready'), 'received')
        await until(lambda: mid.idle() or mid.closed)
        await self.service.feed_once()
        await settle(asyncio.gather(*self.service.feed_tasks.values()))
        await until(lambda: mid.idle() or mid.closed)
        text = 'Second owner request ' + 'y' * 40 + ' claude-quota'
        row = self.store.message_save('home', 'owner', text, telegram_message=20)
        send = mid.send

        async def delayed_receipt(message_id, prompt, row_id=None):
            receipt = await send(message_id, prompt, row_id=row_id)
            await mid.wait_closed()
            return receipt

        with patch.object(mid, 'send', side_effect=delayed_receipt):
            await self.service.feed_once()
            await settle(asyncio.gather(*self.service.feed_tasks.values()))
        self.assertTrue(mid.rejected)
        callback = self.store.db.execute("SELECT id,delivered,receipt FROM messages WHERE kind='callback'").fetchone()
        self.assertEqual(tuple(callback)[1:], ('pending', None))
        self.assertEqual(self.store.get(mid.key + '_lost_turns'), {str(callback['id']): [row['id']]})
        rotated = await self.start()
        self.assertEqual((rotated.provider, rotated.account_alias, rotated.session_id), ('claude', 'd', mid.session_id))
        for _ in range(2):
            await self.service.feed_once()
            await settle(asyncio.gather(*self.service.feed_tasks.values()))
            await until(lambda: rotated.idle() or rotated.closed)
        events = [json.loads(line) for line in (self.root / 'claude-inputs').read_text().splitlines()]
        prompts = [event['message']['content'] for event in events if event['session_id'] == mid.session_id]
        owners = [prompt for prompt in prompts if 'message=%d kind=owner]' % row['id'] in prompt]
        self.assertEqual(len(owners), 1, 'the resumed conversation received the owner message twice')
        self.assertEqual(owners, ['[topic=home name="home" message=%d kind=owner] ' % row['id'] + text])
        self.assertEqual([prompt for prompt in prompts if 'kind=callback]' in prompt],
                         ['[topic=home name="home" message=%d kind=callback] ' % callback['id'] +
                          parent_state.LOST_TURN % ('message=%d' % row['id'])])
        for item in (row, callback):
            self.assertEqual(tuple(self.store.db.execute('SELECT delivered,receipt FROM messages WHERE id=?',
                                                       (item['id'],)).fetchone()), ('received', 'replayed'))

    async def test_leftover_switch_then_same_provider_rotation_delivers_the_owner_message_once(self):
        await self.assert_rotation_after_switch(failed_report=True)

    async def test_clean_switch_then_same_provider_rotation_delivers_the_owner_message_once(self):
        await self.assert_rotation_after_switch(failed_report=False)

    async def test_idle_model_change_closes_input_resumes_history_and_keeps_workers(self):
        self.select('codex', 'test-codex')
        before = [await self.start(topic) for topic in ('home', 'other')]
        natives = [session.session_id for session in before]
        self.select('codex', 'test-astra')
        with patch('coordinator.host.HostClient.stop', AsyncMock(side_effect=AssertionError('No signal stop'))):
            await self.service.feed_once()
            await until(lambda: all(session.closed for session in before))
        after = [await self.start(topic) for topic in ('home', 'other')]
        self.assertEqual([session.model for session in after], ['test-astra', 'test-astra'])
        self.assertEqual([session.session_id for session in after], natives)
        self.assertTrue(all(old.host_id != new.host_id for old, new in zip(before, after)))
        self.assertEqual((self.store.get('codex_model'), self.store.get('codex_active_account')),
                         ('test-codex', 'g'))

    async def test_completed_owner_message_is_not_replayed_when_selection_changes_before_receipt(self):
        binary = self.root / 'codex'
        source = binary.read_text()
        anchor = "        if method=='turn/start' and turn is None:\n"
        gate = ("        if text.endswith('receipt-gate'):\n"
                "            (root/'request-ready').touch()\n"
                "            while not (root/'request-go').exists():\n"
                "                time.sleep(.01)\n")
        self.assertIn(anchor, source)
        binary.write_text(source.replace(anchor, gate + anchor))
        self.select('codex', 'test-codex')
        before = await self.start()
        row = self.store.message_save('home', 'owner', 'receipt-gate', telegram_message=1)
        await self.service.feed_once()
        await until(lambda: (self.root / 'request-ready').exists())
        self.select('codex', 'test-astra')
        (self.root / 'request-go').touch()
        await asyncio.gather(*self.service.feed_tasks.values())
        saved = self.store.db.execute('SELECT delivered,receipt FROM messages WHERE id=?', (row['id'],)).fetchone()
        self.assertEqual(tuple(saved), ('received', 'replayed'))
        await self.service.feed_once()
        await until(lambda: before.closed)
        await self.service.feed_once()
        await self.service.feed_once()
        after = self.service.sessions['home']
        self.parents.append(after)
        await asyncio.gather(*self.service.feed_tasks.values())
        await until(after.idle)
        requests = [json.loads(line) for line in (self.root / 'requests').read_text().splitlines()]
        turns = [request for request in requests if request.get('method') == 'turn/start'
                 and request['params']['input'][0]['text'].endswith('receipt-gate')]
        self.assertEqual(len(turns), 1)
        self.assertEqual((before.model, after.model), ('test-codex', 'test-astra'))

    async def test_cross_provider_change_waits_for_claude_turn_and_starts_fresh(self):
        self.select('claude', 'claude-opus-5-5')
        before = await self.start()
        self.assertEqual(await before.send(str(uuid.uuid4()), 'slow'), 'received')
        await until(lambda: (self.root / 'slow.started').exists() and before.busy)
        self.select('codex', 'test-astra')
        await self.service.feed_once()
        self.assertFalse(before.rejected)
        self.assertFalse(before.closed)
        with patch('coordinator.host.HostClient.stop', AsyncMock(side_effect=AssertionError('No signal stop'))):
            (self.root / 'slow.go').touch()
            await until(lambda: before.closed)
        after = await self.start()
        self.assertTrue(after.switched)
        self.assertNotEqual(after.session_id, before.session_id)
        self.assertEqual((self.store.get('coordinator_provider'), after.model), ('codex', 'test-astra'))
        self.assertTrue((self.root / 'claude-home' / 'projects' / 'test' / (before.session_id + '.jsonl')).exists())
        self.assertIn('owner selected', after.switch['reason'])

    async def test_background_tasks_and_pending_receipts_delay_selection(self):
        self.select('claude', 'claude-opus-5-5')
        before = await self.start()
        self.assertEqual(await before.send(str(uuid.uuid4()), 'bg-work'), 'received')
        await until(lambda: before.protocol.background and not before.busy)
        self.select('codex', 'test-astra')
        await self.service.feed_once()
        self.assertFalse(before.rejected)
        self.assertFalse(before.closed)
        (self.root / 'bg-finish').touch()
        await until(lambda: before.closed)
        after = await self.start()
        self.assertEqual(after.model, 'test-astra')
        self.assertTrue((self.root / 'auto.finished').exists())

    async def test_codex_return_to_claude_preserves_old_native_history(self):
        self.select('codex', 'test-astra')
        before = await self.start()
        old_native = before.session_id
        old_path = before.transcript_path
        self.select('claude', 'claude-opus-5-5')
        with patch('coordinator.host.HostClient.stop', AsyncMock(side_effect=AssertionError('No signal stop'))):
            await self.service.feed_once()
            await until(lambda: before.closed)
        after = await self.start()
        self.assertTrue(after.switched)
        self.assertNotEqual(after.session_id, old_native)
        self.assertEqual((after.model, self.store.get('coordinator_provider')), ('claude-opus-5-5', 'claude'))
        self.assertTrue(Path(old_path).exists())

    async def test_live_host_is_reattached_before_retirement(self):
        self.select('codex', 'test-codex')
        before = await self.start()
        await before.detach()
        self.parents.remove(before)
        self.select('codex', 'test-astra')
        with patch('coordinator.host.HostClient.stop', AsyncMock(side_effect=AssertionError('No signal stop'))):
            attached = await parent.start(self.store, self.runner, self.root, 'unused', 'policy', topic='home')
            self.parents.append(attached)
            self.assertEqual(attached.host_id, before.host_id)
            await until(lambda: attached.closed)

    async def test_close_failure_leaves_existing_writer_and_records_pending(self):
        self.select('codex', 'test-codex')
        session = await self.start()
        self.select('codex', 'test-astra')
        with patch.object(session.process, 'request', AsyncMock(side_effect=BrokenPipeError)):
            await parent_state.retire(session)
        self.assertFalse(session.closed)
        self.assertEqual(self.store.get('coordinator_host')['id'], session.host_id)

    async def test_unfinished_retirement_does_not_block_other_parents_or_start_another_writer(self):
        self.select('codex', 'test-codex')
        first = await self.start()
        other = await self.start('other')
        self.select('codex', 'test-astra')
        with patch.object(first.process, 'request', AsyncMock(return_value={'ok': True})), \
                patch('coordinator.service.PARENT_IDLE', 0):
            await self.service.feed_once()
            await until(lambda: other.closed)
            self.assertIsNone(await asyncio.wait_for(self.service.start_session('home'), 1))
            self.assertFalse(first.closed)
            self.assertEqual(self.store.get('coordinator_host')['id'], first.host_id)
        after = await self.start('other')
        self.assertEqual(after.model, 'test-astra')
