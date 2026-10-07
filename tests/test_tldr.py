import asyncio
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from coordinator.service import Service
from coordinator.store import Store
from coordinator.tldr import INPUT_LIMIT, input_prompt, summarize, window
from tests.test_service import FakeRunner, FakeTelegram
from tests.support import isolate_shared_mcp_home


CODEX_SUMMARY_STUB = '''#!/usr/bin/env python3
import json,sys
from pathlib import Path
root=Path(__file__).parent
assert 'mcp_servers={}' in sys.argv and 'model_reasoning_effort="low"' in sys.argv
def emit(event):
    print(json.dumps(event),flush=True)
for line in sys.stdin:
    event=json.loads(line)
    with (root/'summary-requests').open('a') as stream:
        stream.write(json.dumps(event)+'\\n')
    method=event.get('method')
    params=event.get('params') or {}
    if method=='initialize':
        emit({'id':event['id'],'result':{}})
    elif method=='thread/start':
        assert params['ephemeral'] is True and params['sandbox']=='read-only'
        assert params['approvalPolicy']=='never' and params['model']=='test-codex'
        emit({'id':event['id'],'result':{'thread':{'id':'summary-thread'}}})
    elif method=='turn/start':
        assert params['threadId']=='summary-thread' and 'One update' in params['input'][0]['text']
        emit({'id':event['id'],'result':{'turn':{'id':'summary-turn'}}})
        emit({'method':'item/completed','params':{'threadId':'summary-thread','turnId':'summary-turn',
             'item':{'type':'agentMessage','text':'## Done\\n- Sync is ready.'}}})
        emit({'method':'turn/completed','params':{'threadId':'summary-thread',
             'turn':{'id':'summary-turn','status':'completed'}}})
'''


class TldrTestsSupport:
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addAsyncCleanup(self.cleanup)
        self.root = Path(self.temp.name)
        isolate_shared_mcp_home(self, self.root)
        self.store = Store(self.root / 'state')
        account = self.root / 'profile'
        account.mkdir()
        with self.store.db:
            self.store.put('owner', 7)
            self.store.put('group', -10042)
            for topic, thread in (('-10042:4', 4), ('-10042:5', 5)):
                self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd) VALUES (?,?,?,?,?)',
                                      (topic, -10042, thread, 'Test', str(self.root)))
            self.store.put('accounts', {'test': {'config_dir': str(account), 'enabled': True}})
            self.store.put('account_status', {'test': {'identity': {
                'email': 'test@example.com', 'logged_in': True}}})
        self.topic = '-10042:4'
        self.runner = FakeRunner()
        self.runner.binaries = {'claude': '/runner/claude'}
        self.service = Service(self.store, FakeTelegram(), self.runner, self.root)

    async def cleanup(self):
        self.store.close()
        self.temp.cleanup()

    def owner(self, message, text, topic=None):
        return self.store.message_save(topic or self.topic, 'owner', text, telegram_message=message)

    def sent(self, message, text, topic=None, kind='report', edit=None):
        with self.store.db:
            row = self.store.enqueue_report(topic or self.topic, text, kind=kind, edit=edit)
            self.store.delivered(row, message)
        return row

    def command(self, message=90, text='/tldr'):
        return self.store.accept({'update_id': message, 'message': {
            'message_id': message, 'message_thread_id': 4, 'text': text,
            'from': {'id': 7}, 'chat': {'id': -10042, 'type': 'supergroup'}}})

    def chatgpt_only(self):
        profile = self.root / 'chatgpt'
        profile.mkdir()
        self.store.put('accounts', {})
        self.store.put('codex_accounts', {'active': {'config_dir': str(profile), 'enabled': True}})
        self.store.put('codex_account_status', {'active': {'identity': {
            'email': 'chatgpt@example.test', 'logged_in': True}}})
        self.store.put('codex_active_account', 'active')
        self.store.put('codex_auto_switch', True)
        self.store.put('codex_model', 'test-codex')
        self.store.db.commit()
        self.owner(20, 'Summarize')
        self.sent(21, 'One update')


class TldrTests(TldrTestsSupport, unittest.IsolatedAsyncioTestCase):
    async def test_window_uses_telegram_order_and_latest_edit_in_one_topic(self):
        self.owner(10, 'Earlier owner request')
        self.sent(11, 'Before newer owner message')
        self.owner(20, 'Latest owner request')
        self.sent(21, 'First delivered report')
        self.owner(22, '/help')
        pending = self.store.enqueue_report(self.topic, 'Never delivered')
        self.assertIsNotNone(pending)
        self.sent(23, 'Other topic report', '-10042:5')
        self.sent(24, 'Old text', kind='status')
        self.sent(24, 'Edited text', kind='status', edit=24)
        with self.store.db:
            self.store.db.execute("INSERT INTO outbox(topic,kind,text,delivered,telegram_message,reply_markup) "
                                  "VALUES (?, 'status', '', 1, 25, '{}')", (self.topic,))
        self.sent(30, 'Last delivered report')
        self.sent(95, 'Sent after the command')
        self.assertEqual(window(self.store, self.topic, 90),
                         ['First delivered report', 'Edited text', 'Last delivered report'])
        self.assertEqual(window(self.store, '-10042:5', 90), ['Other topic report'])

    async def test_empty_window_replies_without_an_account_or_model_call(self):
        self.owner(20, 'Latest request')
        self.assertEqual(self.command(), 'control')
        with patch('coordinator.tldr._native', new_callable=AsyncMock) as native:
            self.assertTrue(await self.service.tldr_once())
            native.assert_not_awaited()
        row = self.store.db.execute('SELECT text,reply_to FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
        self.assertEqual(tuple(row), ('Nothing new since your last message.', 90))

    async def test_input_cap_drops_oldest_messages_and_reports_count(self):
        messages = ['A' * 80_000, 'B' * 80_000, 'C' * 10]
        prompt, omitted, shortened = input_prompt(messages)
        self.assertLessEqual(len(prompt), INPUT_LIMIT)
        self.assertEqual((omitted, shortened), (1, False))
        self.assertNotIn('A' * 100, prompt)
        self.assertIn('B' * 100, prompt)
        huge, dropped, cut = input_prompt(['Z' * 160_000])
        self.assertEqual(len(huge), INPUT_LIMIT)
        self.assertEqual((dropped, cut), (0, True))
        self.owner(10, 'Summarize')
        for index in range(45):
            self.sent(11 + index, 'Message body %d ' % index + 'A' * 3900)
        with patch('coordinator.tldr._native', new_callable=AsyncMock,
                   return_value={'result': '## Done\n- A feature is ready.\n\n## Needs your push\nNothing.\n\n## How to unblock\nNothing.'}) as native:
            reply = await summarize(self.store, self.topic, 90, self.service.accounts,
                                    self.service.extension, self.runner.binaries['claude'])
        self.assertRegex(reply, r'\d+ earlier messages left out')
        self.assertLessEqual(len(native.await_args.kwargs['input_data']), INPUT_LIMIT)

    async def test_summary_failure_text_when_extension_fails(self):
        self.owner(20, 'Summarize')
        self.sent(21, 'One update')
        with patch.object(self.service.extension, 'oneshot', AsyncMock(side_effect=RuntimeError)), \
                patch('coordinator.tldr._native', new_callable=AsyncMock) as native:
            reply = await summarize(self.store, self.topic, 90, self.service.accounts,
                                    self.service.extension, self.runner.binaries['claude'])
        self.assertEqual(reply, 'Summary failed: Claude could not complete the request.')
        native.assert_not_awaited()

    async def test_failed_call_queues_a_reply_to_the_command(self):
        self.owner(20, 'Summarize')
        self.sent(21, 'One update')
        self.command()
        with patch('coordinator.tldr._native', new_callable=AsyncMock, side_effect=asyncio.TimeoutError):
            self.assertTrue(await self.service.tldr_once())
        row = self.store.db.execute('SELECT text,reply_to FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
        self.assertEqual(row['reply_to'], 90)
        self.assertEqual(row['text'], 'Summary failed: Claude took too long to respond.')

    async def test_unexpected_failure_also_queues_one_reply(self):
        self.owner(20, 'Summarize')
        self.sent(21, 'One update')
        self.command()
        failure = RuntimeError('private detail')
        failure.returncode = 17
        with patch('coordinator.tldr.summarize', new_callable=AsyncMock, side_effect=failure), \
                patch('coordinator.service.logger.warning') as warning:
            self.assertTrue(await self.service.tldr_once())
        self.assertEqual(warning.call_args.args, ('summary failed type=%s exit=%s', 'RuntimeError', 17))
        row = self.store.db.execute('SELECT text,reply_to FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
        self.assertEqual(tuple(row), ('Summary failed: Torii could not complete the request.', 90))

    async def test_other_controls_are_not_held_by_a_slow_summary(self):
        self.owner(20, 'Summarize')
        self.sent(21, 'One update')
        self.command()
        waiting = asyncio.Event()

        async def slow(*args, **kwargs):
            await waiting.wait()
            return {'result': '## Done\nNothing.\n\n## Needs your push\nNothing.\n\n## How to unblock\nNothing.'}

        with patch('coordinator.tldr._native', side_effect=slow):
            task = asyncio.create_task(self.service.tldr_once())
            await asyncio.sleep(0)
            self.assertFalse(await self.service.controls_once())
            self.assertEqual(self.command(91, '/help'), 'control')
            waiting.set()
            self.assertTrue(await task)

    async def test_chatgpt_summary_uses_an_ephemeral_read_only_thread_and_no_mcp(self):
        self.chatgpt_only()
        binary = self.root / 'codex'
        binary.write_text(CODEX_SUMMARY_STUB)
        binary.chmod(0o700)
        self.command()
        with patch('coordinator.codex_session.codex_binary', return_value=str(binary)), \
                patch('coordinator.tldr._native', new_callable=AsyncMock) as native:
            self.assertTrue(await self.service.tldr_once())
            native.assert_not_awaited()
        reply = self.store.db.execute('SELECT text,reply_to FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
        self.assertEqual(tuple(reply), ('## Done\n- Sync is ready.', 90))
        self.assertEqual(self.store.get('codex_active_account'), 'active')
        self.assertIsNone(self.store.get('coordinator_session'))
        requests = [json.loads(line) for line in (self.root / 'summary-requests').read_text().splitlines()]
        self.assertEqual([event['method'] for event in requests],
                         ['initialize', 'initialized', 'thread/start', 'turn/start'])

    async def test_chatgpt_summary_closes_its_server_on_failure(self):
        self.chatgpt_only()
        for error, expected in ((asyncio.TimeoutError(), 'took too long'), (ValueError(), 'could not complete')):
            server = AsyncMock()
            server.request.side_effect = error
            with patch('coordinator.tldr.AppServer', return_value=server):
                reply = await summarize(self.store, self.topic, 90, self.service.accounts, None, '', codex='/stub')
            self.assertIn(expected, reply)
            server.stop.assert_awaited_once()

    async def test_chatgpt_summary_does_not_launch_an_exhausted_active_account(self):
        self.chatgpt_only()
        self.store.put('codex_account_blocks', {'active': {'reason': 'quota', 'until': 9999999999}})
        with patch('coordinator.tldr.AppServer') as server:
            reply = await summarize(self.store, self.topic, 90, self.service.accounts, None, '', codex='/stub')
        server.assert_not_called()
        self.assertEqual(reply, 'Summary failed: no Claude or ChatGPT account is available.')


class NativeSummaryTests(TldrTestsSupport, unittest.IsolatedAsyncioTestCase):
    async def test_public_summary_runs_with_the_profile_env_only(self):
        self.owner(20, 'Summarize')
        self.sent(21, 'One update')
        expected = self.service.accounts.environment('test')
        with patch('coordinator.tldr._native', AsyncMock(return_value={'result': 'Done'})) as native:
            reply = await summarize(self.store, self.topic, 90, self.service.accounts,
                                    self.service.extension, self.runner.binaries['claude'])
        self.assertEqual(reply, 'Done')
        self.assertEqual(native.await_args.args[1], expected)
