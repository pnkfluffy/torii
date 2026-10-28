"""Offline parent reply forwarding across native providers and restarts."""

from contextlib import asynccontextmanager
import json
import sqlite3
import unittest
from unittest.mock import AsyncMock

from coordinator import control_api, control_ui, reactions
from coordinator.formatting import markdown_to_html
from coordinator.providers import ProviderRunner
from coordinator.reactions import deliver_reaction, set_desired
from coordinator.service import Service
from coordinator.session import CoordinatorSession
from coordinator.store import Store
from coordinator.telegram import Telegram, TelegramError
from tests import test_codex_parent, test_opencode_parent, test_parent_turns
from tests.support import telegram_text, until
from tests.test_service import FakeTelegram


PROVIDERS = ('opencode', 'claude', 'codex')


class ParentCase:
    def __init__(self, fixture, provider, topic, release, response):
        self.fixture, self.provider, self.topic = fixture, provider, topic
        self.release, self.response = release, response
        self.session = None
        self.telegram = FakeTelegram()
        self.service = None

    @property
    def store(self):
        return self.fixture.store

    async def start(self):
        if self.provider == 'claude':
            self.session = await CoordinatorSession.start(self.store, self.fixture.runner,
                self.fixture.root, 'test', 'policy', topic=self.topic, receipt_timeout=2)
            self.fixture.session = self.session
        else:
            self.session = await self.fixture.start()
        self.service = Service(self.store, self.telegram, self.fixture.runner, self.fixture.root)
        self.service.sessions[self.topic] = self.session

    def row(self, text='Owner asks', kind='owner'):
        number = self.store.db.execute('SELECT COALESCE(MAX(id),0)+1 FROM messages').fetchone()[0]
        return self.store.message_save(self.topic, kind, text,
                                       telegram_message=100 + number if kind == 'owner' else None)

    def output(self):
        return [dict(row) for row in self.store.db.execute(
            "SELECT * FROM outbox WHERE kind!='local-note' ORDER BY id")]

    def problems(self):
        return [dict(row) for row in self.store.db.execute(
            'SELECT area,code,detail,topic,task,worker,message FROM problems ORDER BY id')]

    async def send(self, row):
        prompt = await self.service._prepare_message(row, self.session)
        receipt = await self.service._send_message(row, prompt, self.session)
        if receipt != 'received':
            raise AssertionError(receipt)

    async def finish(self, text='Final answer', error=False):
        self.response.write_text(json.dumps({'text': text, 'chunks': [text] if text else [],
                                             'error': error, 'stop_reason': 'cancelled' if error else 'end_turn'}))
        self.release.touch()
        await until(lambda: self.session.closed or self.session.idle())

    async def next_turn(self):
        self.release.unlink(missing_ok=True)

    async def restart(self, sequence=None):
        identity = (self.session.session_id, self.session.host_id)
        key = self.session.key
        await self.session.detach()
        if sequence is not None:
            with self.store.db:
                self.store.put(key + '_host_seq', sequence)
        directory = self.store.directory
        self.store.close()
        self.fixture.store = Store(directory)
        self.fixture.addCleanup(self.store.close)
        self.fixture.runner = ProviderRunner(directory, getattr(self.fixture.runner, 'binaries', {}))
        await self.start()
        if (self.session.session_id, self.session.host_id) != identity:
            raise AssertionError('Restart changed the native session or host')


class ParentReplyTests(unittest.IsolatedAsyncioTestCase):
    @asynccontextmanager
    async def case(self, provider, tools=False, codex_hold_started=False):
        classes = {'opencode': test_opencode_parent.OpenCodeParentTests,
                   'claude': test_parent_turns.ParentTurnTests,
                   'codex': test_codex_parent.CodexParentTests}
        fixture = classes[provider]()
        if provider == 'opencode':
            fixture.setUp()
            topic = 'chat'
        else:
            await fixture.asyncSetUp()
            topic = 'home'
        try:
            with fixture.store.db:
                fixture.store.put('owner', 1)
                fixture.store.put('group', 1)
                fixture.store.put('mode', 'group')
                fixture.store.put('coordinator_home_topic', 'home')
                if provider == 'claude':
                    fixture.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) "
                                             "VALUES ('home',1,1,'Home',?,1)", (str(fixture.root),))
            release = fixture.root / 'forward-release'
            response = fixture.root / 'forward-response.json'
            response.write_text(json.dumps({'text': 'Final answer', 'chunks': ['Final answer']}))
            if provider == 'opencode':
                tool_calls = [{'sessionUpdate': 'agent_thought_chunk',
                               'content': {'type': 'text', 'text': 'PRIVATE THINKING'}},
                              {'sessionUpdate': 'tool_call', 'toolCallId': 'list', 'title': 'torii_tasks_list',
                               'rawInput': {'value': 'PRIVATE ARGUMENTS'}, 'status': 'pending'},
                              {'sessionUpdate': 'tool_call_update', 'toolCallId': 'list', 'status': 'completed',
                               'content': [{'type': 'content', 'content': {'type': 'text', 'text': 'PRIVATE OUTPUT'}}]}]
                fixture.scenario(release=str(release), response_file=str(response), tool_calls=tool_calls if tools else [])
            elif provider == 'claude':
                binary = fixture.root / 'claude'
                old = "    emit({'type':'result','subtype':'success','result':'done:'+text})"
                new = "\n".join([
                    "    while not (root/'forward-release').exists():",
                    "        time.sleep(.01)",
                    "    answer=json.loads((root/'forward-response.json').read_text())",
                    "    emit({'type':'assistant','message':{'role':'assistant','content':[{'type':'thinking','thinking':'PRIVATE THINKING'},{'type':'text','text':answer['text']}]}})",
                    "    emit({'type':'result','subtype':'error' if answer.get('error') else 'success','is_error':answer.get('error',False),'result':answer['text']})"])
                binary.write_text(test_parent_turns.FAKE.replace(old, new).replace(
                    "if text=='bg-work':", "if 'bg-work' in text:"))
            else:
                source = test_codex_parent.APP_SERVER.replace('    global turn\n', "\n".join([
                    '    global turn',
                    "    while not (root/'forward-release').exists():",
                    "        time.sleep(.01)",
                    "    answer=json.loads((root/'forward-response.json').read_text())", '']))
                source = source.replace("'text':'done'", "'text':answer['text']", 1)
                source = source.replace("'status':'completed'", "'status':'failed' if answer.get('error') else 'completed'", 1)
                if codex_hold_started:
                    marker = "\n            emit({'method':'turn/started','params':{'threadId':thread,'turn':{'id':turn}}})"
                    self.assertEqual(source.count(marker), 1)
                    source = source.replace(marker, "\n".join([
                        "",
                        "            while not (root/'started-go').exists():",
                        "                time.sleep(.01)", marker.lstrip('\n')]))
                fixture.binary.write_text(source)
            case = ParentCase(fixture, provider, topic, release, response)
            await case.start()
            yield case
        finally:
            await fixture.asyncTearDown()
            while fixture._cleanups:
                callback, args, kwargs = fixture._cleanups.pop()
                callback(*args, **kwargs)

    async def assert_forward(self, case, row=None, text='Final answer'):
        self.assertEqual([(item['topic'], item['text'], item['reply_to'], item['kind']) for item in case.output()],
                         [(case.topic, text, row['telegram_message'] if row else None, 'parent-reply')])
        self.assertEqual(case.store.messages_pending(), [])

    async def test_success_clears_matching_pause_notice_for_every_provider(self):
        for provider in PROVIDERS:
            with self.subTest(provider=provider):
                async with self.case(provider) as case:
                    session = case.session
                    pause_key = (session.key + '_selection' if case.store.get(session.key + '_selection')
                                 else 'coordinator_account')
                    case.store.put(pause_key + '_notice', {
                        'key': [provider, session.account_alias, None, 'unavailable'],
                        'text': 'Saved pause notice', 'topics': [case.topic]})
                    owner = case.row()
                    await case.send(owner)
                    await case.finish()
                    self.assertIsNone(case.store.get(pause_key + '_notice'))

    async def test_plain_owner_answer_forwards_for_every_provider(self):
        for provider in PROVIDERS:
            with self.subTest(provider=provider):
                async with self.case(provider, tools=True) as case:
                    owner = case.row()
                    await case.send(owner)
                    await case.finish()
                    await self.assert_forward(case, owner)

    async def test_reaction_only_turn_stays_private_and_next_owner_answer_forwards(self):
        for provider in PROVIDERS:
            with self.subTest(provider=provider):
                async with self.case(provider) as case:
                    target = case.row('Audit summary')
                    await case.send(target)
                    await case.finish('')
                    await case.next_turn()
                    self.assertEqual(reactions.accept_reaction(case.store, {
                        'user': {'id': 1}, 'chat': {'id': 1}, 'message_id': target['telegram_message'],
                        'old_reaction': [], 'new_reaction': [{'type': 'emoji', 'emoji': '👍'}]}), 'queued')
                    reaction = case.store.messages_pending()[0]
                    self.assertEqual(reaction['reacted_to'], target['telegram_message'])
                    await case.send(reaction)
                    await case.finish('The 👍 was on my audit summary, and it needs no reply.')
                    self.assertEqual(case.store.messages_pending(), [])
                    self.assertEqual(case.output(), [])
                    self.assertEqual(case.problems(), [])
                    await case.next_turn()
                    owner = case.row('Reacted 👍')
                    await case.send(owner)
                    await case.finish()
                    await self.assert_forward(case, owner)

    async def test_mixed_reaction_and_owner_turn_forwards_for_every_provider(self):
        for provider in PROVIDERS:
            for reaction_first in (True, False):
                with self.subTest(provider=provider, reaction_first=reaction_first):
                    async with self.case(provider) as case:
                        owner = case.row()
                        reaction = case.store.message_save(case.topic, 'owner', 'Reacted 👍',
                                                           reacted_to=owner['telegram_message'])
                        rows = [reaction, owner] if reaction_first else [owner, reaction]
                        await case.service._deliver_batch(rows, case.session, case.topic)
                        await case.finish()
                        self.assertEqual([(row['text'], row['reply_to']) for row in case.output()],
                                         [('Final answer', owner['telegram_message'])])
                        self.assertEqual(case.problems(), [])
                        self.assertEqual(case.store.messages_pending(), [])

    async def test_opencode_worker_result_and_event_answers_forward(self):
        for kind in ('worker_result', 'event'):
            with self.subTest(kind=kind):
                async with self.case('opencode') as case:
                    await case.send(case.row('Background result', kind))
                    await case.finish()
                    self.assertEqual([(row['text'], row['reply_to']) for row in case.output()], [('Final answer', None)])

    async def test_internal_event_and_worker_notes_stay_private_for_claude_and_codex(self):
        for provider in ('claude', 'codex'):
            with self.subTest(provider=provider):
                async with self.case(provider) as case:
                    for kind in ('worker_result', 'event'):
                        await case.send(case.row('Internal result', kind))
                        await case.finish('Private note')
                        self.assertEqual(case.output(), [])
                        await case.next_turn()
                    owner = case.row()
                    await case.send(owner)
                    await case.finish()
                    await self.assert_forward(case, owner)

    async def test_midturn_owner_reply_survives_unrelated_waiting_card(self):
        for kind, card in (('event', 'owner-waiting'), ('worker_result', 'owner-waiting'),
                           ('worker_result', 'status'), ('event', 'wait-ask')):
            with self.subTest(kind=kind, card=card):
                async with self.case('codex') as case:
                    with case.store.db:
                        case.store.put('waits_enabled', True)
                        case.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) "
                                              "VALUES ('other',1,9,'Other',?,1)", (str(case.fixture.root),))
                    await case.send(case.row('Background hold', kind))
                    await until(lambda: case.store.db.execute(
                        'SELECT COUNT(*) FROM parent_reply_turns WHERE checked=0').fetchone()[0] == 1)
                    if card == 'status':
                        control_ui.emit(case.store, case.topic, 'Status card', [])
                    else:
                        case.store.task_wait(case.topic if card == 'wait-ask' else 'other',
                                             'owner', 'Approve another job')
                    owner = case.row('Question during the running turn')
                    await case.send(owner)
                    await until(lambda: case.store.db.execute(
                        'SELECT turn_started FROM messages WHERE id=?', (owner['id'],)).fetchone()[0] is not None)
                    await case.finish('Owner answer')
                    replies = [row for row in case.output() if row['kind'] == 'parent-reply']
                    self.assertEqual([(row['topic'], row['text'], row['reply_to']) for row in replies],
                                     [(case.topic, 'Owner answer', owner['telegram_message'])])
                    self.assertEqual(case.problems(), [])

    async def test_turn_that_sent_does_not_forward_and_next_silent_turn_does(self):
        for provider in PROVIDERS:
            with self.subTest(provider=provider):
                async with self.case(provider) as case:
                    owner = case.row()
                    await case.send(owner)
                    await until(lambda: case.store.db.execute(
                        'SELECT turn_started FROM messages WHERE id=?', (owner['id'],)).fetchone()[0] is not None)
                    with case.store.db:
                        control_api.op_telegram_send(case.store, None, case.topic, 'Sent with tool', reply_to=owner['id'])
                    await case.finish('Do not duplicate this')
                    self.assertEqual([(row['text'], row['reply_to']) for row in case.output()],
                                     [('Sent with tool', owner['telegram_message'])])
                    await case.next_turn()
                    next_owner = case.row('Next question')
                    await case.send(next_owner)
                    await case.finish()
                    self.assertEqual([row['text'] for row in case.output()], ['Sent with tool', 'Final answer'])

    async def test_restart_before_completion_then_replay_after_commit_forwards_once(self):
        for provider in PROVIDERS:
            with self.subTest(provider=provider):
                async with self.case(provider) as case:
                    owner = case.row()
                    await case.send(owner)
                    await until(lambda: case.store.db.execute(
                        'SELECT turn_started FROM messages WHERE id=?', (owner['id'],)).fetchone()[0] is not None)
                    await case.restart()
                    sequence = case.store.get(case.session.key + '_host_seq', 0)
                    await case.finish()
                    await self.assert_forward(case, owner)
                    await case.restart(sequence)
                    await until(case.session.idle)
                    await case.service.feed_once()
                    await self.assert_forward(case, owner)

    async def test_opencode_session_load_history_is_not_forwarded(self):
        async with self.case('opencode') as case:
            owner = case.row()
            await case.send(owner)
            await case.finish()
            await self.assert_forward(case, owner)
            await case.session.stop()
            await case.start()
            await until(case.session.idle)
            self.assertEqual([row['text'] for row in case.output()], ['Final answer'])
            self.assertEqual(len(case.fixture.calls('session/load')), 1)

    async def test_final_message_renders_like_report_without_thinking_or_tool_output(self):
        text = '  **literal** <tag> & `code`\n'
        for provider in PROVIDERS:
            with self.subTest(provider=provider):
                async with self.case(provider, tools=True) as case:
                    owner = case.row()
                    await case.send(owner)
                    await case.finish(text)
                    await self.assert_forward(case, owner, text.strip())
                    api = object.__new__(Telegram)
                    api.call = AsyncMock(return_value={'message_id': 999})
                    await api.send(dict(case.output()[0], chat=1, thread=2))
                    self.assertEqual(api.call.call_args.kwargs['text'], markdown_to_html(text.strip()))
                    forwarded = api.call.call_args.kwargs['text']
                    self.assertNotIn('PRIVATE', api.call.call_args.kwargs['text'])
                    await api.send(dict(case.output()[0], kind='report', chat=1, thread=2))
                    self.assertEqual(api.call.call_args.kwargs['text'], forwarded)

    async def test_empty_final_text_does_not_forward_stale_text(self):
        for provider in PROVIDERS:
            with self.subTest(provider=provider):
                async with self.case(provider) as case:
                    owner = case.row()
                    await case.send(owner)
                    await case.finish()
                    await self.assert_forward(case, owner)
                    for text in ('', ' \n\t '):
                        await case.next_turn()
                        await case.send(case.row('Empty result'))
                        await case.finish(text)
                        self.assertEqual([row['text'] for row in case.output()], ['Final answer'])

    async def test_failed_and_interrupted_turns_do_not_forward(self):
        for provider in PROVIDERS:
            with self.subTest(provider=provider):
                async with self.case(provider) as case:
                    owner = case.row()
                    await case.send(owner)
                    await case.finish()
                    await self.assert_forward(case, owner)
                    await case.next_turn()
                    await case.send(case.row('Failure'))
                    await case.finish('Failure details', error=True)
                    self.assertEqual([row['text'] for row in case.output()], ['Final answer'])
                async with self.case(provider) as case:
                    owner = case.row()
                    await case.send(owner)
                    await case.session.stop()
                    self.assertEqual(case.output(), [])

    async def test_local_notes_and_other_topics_do_not_suppress_forwarding(self):
        async with self.case('opencode') as case:
            owner = case.row()
            await case.send(owner)
            with case.store.db:
                case.store.message_event(case.topic, 'Local preview', from_name='Test')
                case.store.enqueue_report('home', 'Different topic')
            await case.finish()
            self.assertEqual([(row['topic'], row['text']) for row in case.output()],
                             [('home', 'Different topic'), ('chat', 'Final answer')])

    async def test_delivered_forward_sets_owner_replied_after_retry_and_splits_safely(self):
        async with self.case('opencode') as case:
            owner = case.row()
            await case.send(owner)
            text = '  **Rendered** <&> 😀\n' * 300
            await case.finish(text)
            output = case.output()
            self.assertGreater(len(output), 1)
            with case.store.db:
                case.store.enqueue_report('home', text)
            normal = [row['text'] for row in case.output() if row['topic'] == 'home']
            self.assertEqual([row['text'] for row in output], normal)
            self.assertTrue(all(len(telegram_text(row['text'])) <= 4096 for row in output))
            self.assertEqual({row['reply_to'] for row in output}, {owner['telegram_message']})
            with case.store.db:
                set_desired(case.store, owner['id'], '👌')
            self.assertIsNone(case.store.db.execute('SELECT reaction_desired FROM messages WHERE id=?',
                                                   (owner['id'],)).fetchone()[0])
            original = case.telegram.send
            case.telegram.send = AsyncMock(side_effect=TelegramError(500, reason='synthetic failure'))
            await case.service.deliver_once()
            self.assertEqual(case.telegram.sent, [])
            case.telegram.send = original
            with case.store.db:
                case.store.db.execute('UPDATE outbox SET next_attempt=0')
            while await case.service.deliver_once():
                pass
            self.assertEqual([row['text'] for row in case.telegram.sent if row['topic'] == case.topic], normal)
            self.assertEqual(case.store.db.execute('SELECT reaction_desired FROM messages WHERE id=?',
                                                   (owner['id'],)).fetchone()[0], '👌')
            await deliver_reaction(case.store, case.telegram)
            self.assertIn(('setMessageReaction', {'chat_id': 1, 'message_id': owner['telegram_message'],
                               'reaction': [{'type': 'emoji', 'emoji': '👌'}]}), case.telegram.calls)

    async def test_job_origin_reaction_survives_forward(self):
        async with self.case('opencode') as case:
            owner = case.row('Please build X')
            await case.send(owner)
            task = case.store.task_create(case.topic, 'Build X', origin=owner['id'])
            with case.store.db:
                reactions.worker_started(case.store, {'task': task['id'], 'work': 'dev'})
            await case.finish('Started a worker on X.')
            while await case.service.deliver_once():
                pass
            self.assertEqual(case.store.db.execute('SELECT reaction_desired FROM messages WHERE id=?',
                                                   (owner['id'],)).fetchone()[0], '👨\u200d💻')

    async def test_job_origin_reaction_survives_empty_turn(self):
        async with self.case('opencode') as case:
            owner = case.row('Please build X')
            await case.send(owner)
            task = case.store.task_create(case.topic, 'Build X', origin=owner['id'])
            with case.store.db:
                reactions.worker_started(case.store, {'task': task['id'], 'work': 'dev'})
            await case.finish('')
            self.assertEqual(case.store.db.execute('SELECT reaction_desired FROM messages WHERE id=?',
                                                   (owner['id'],)).fetchone()[0], '👨\u200d💻')

    async def test_job_done_reaction_after_empty_origin_turn(self):
        async with self.case('opencode') as case:
            owner = case.row('Please build X')
            await case.send(owner)
            task = case.store.task_create(case.topic, 'Build X', origin=owner['id'])
            await case.finish('')
            await case.next_turn()
            await case.send(case.row('Worker finished X', 'worker_result'))
            with case.store.db:
                control_api.op_telegram_send(case.store, None, case.topic, 'X is built.')
            case.store.task_update(task['id'], status='done')
            await case.finish('Reported.')
            while await case.service.deliver_once():
                pass
            self.assertEqual(case.store.db.execute('SELECT reaction_desired FROM messages WHERE id=?',
                                                   (owner['id'],)).fetchone()[0], '👌')

    async def test_forward_has_no_whitespace_only_parts(self):
        for text in ('x' * 4095 + '\n\n', 'A' * 100 + '\n' * 10000 + 'B'):
            with self.subTest(size=len(text)):
                async with self.case('opencode') as case:
                    await case.send(case.row())
                    await case.finish(text)
                    self.assertTrue(case.output())
                    self.assertTrue(all(row['text'].strip() for row in case.output()))

    async def test_forward_caps_at_four_messages_with_history_notice(self):
        for text in ('x' * 25000, '```python\n' + 'print("<&>")\n' * 4000 + '```'):
            with self.subTest(fenced=text.startswith('```')):
                async with self.case('opencode') as case:
                    owner = case.row()
                    await case.send(owner)
                    await case.finish(text)
                    output = case.output()
                    self.assertEqual(len(output), 4)
                    notice = "(Cut here. The rest is in this chat's own history.)"
                    self.assertTrue(output[-1]['text'].endswith('\n' + notice))
                    self.assertTrue(all(len(telegram_text(row['text'])) <= 4096 for row in output))
                    self.assertEqual({row['reply_to'] for row in output}, {owner['telegram_message']})
                    while await case.service.deliver_once():
                        pass
                    self.assertEqual(case.store.db.execute('SELECT reaction_desired FROM messages WHERE id=?',
                                                           (owner['id'],)).fetchone()[0], '👌')

    async def test_codex_reply_during_restart_window_is_not_duplicated(self):
        async with self.case('codex', codex_hold_started=True) as case:
            owner = case.row('Owner question')
            await case.send(owner)
            await case.session.detach()
            (case.fixture.root / 'started-go').touch()
            other = Store(case.store.directory)
            try:
                with other.db:
                    control_api.op_telegram_send(other, None, case.topic, 'Parent reply via telegram_send',
                                                 reply_to=owner['id'])
            finally:
                other.close()
            case.store.messages_uncertain()
            await case.restart()
            await case.finish('Final answer')
            self.assertEqual([row['text'] for row in case.output()], ['Parent reply via telegram_send'])

    async def test_failed_forward_rolls_back_before_retry(self):
        async with self.case('opencode') as case:
            owner = case.row()
            await case.send(owner)
            await case.finish()
            await self.assert_forward(case, owner)
            await case.next_turn()
            owner = case.row('Next turn')
            await case.send(owner)
            await until(lambda: case.store.db.execute(
                'SELECT COUNT(*) FROM parent_reply_turns WHERE checked=0').fetchone()[0] == 1)
            turn = case.store.db.execute('SELECT turn FROM parent_reply_turns WHERE checked=0').fetchone()[0]
            from coordinator import parent_replies
            with case.store.db:
                case.store.db.execute("CREATE TRIGGER fail_forward BEFORE INSERT ON outbox "
                    "WHEN NEW.kind='parent-reply' BEGIN SELECT RAISE(ABORT,'synthetic failure'); END")
            with self.assertRaises(sqlite3.IntegrityError):
                parent_replies.completed(case.session, turn, {owner['id']}, 'Next answer')
            self.assertEqual([row['text'] for row in case.output()], ['Final answer'])
            self.assertEqual(case.store.db.execute(
                'SELECT checked FROM parent_reply_turns WHERE turn=?', (turn,)).fetchone()[0], 0)
            with case.store.db:
                case.store.db.execute('DROP TRIGGER fail_forward')
            await case.finish('Next answer')
            self.assertEqual([row['text'] for row in case.output()], ['Final answer', 'Next answer'])

    async def test_two_owners_in_one_opencode_prompt_reply_to_newest(self):
        async with self.case('opencode') as case:
            await case.send(case.row('Background only', 'event'))
            first, newest = case.row('First question'), case.row('Second question')
            await case.send(first)
            await case.send(newest)
            case.response.write_text(json.dumps({'chunks': ['Combined answer'], 'only_owner_text': True}))
            case.release.touch()
            await until(case.session.idle)
            self.assertEqual([(row['text'], row['reply_to']) for row in case.output()],
                             [('Combined answer', newest['telegram_message'])])
            while await case.service.deliver_once():
                pass
            states = case.store.db.execute("SELECT reaction_desired FROM messages WHERE kind='owner' ORDER BY id").fetchall()
            self.assertEqual([row[0] for row in states], ['👌', '👌'])

    async def test_codex_send_before_owner_steers_into_the_turn_suppresses_forward(self):
        async with self.case('codex') as case:
            owner = case.row()
            await case.send(owner)
            await case.finish()
            await self.assert_forward(case, owner)
            await case.next_turn()
            await case.send(case.row('Background hold', 'event'))
            with case.store.db:
                control_api.op_telegram_send(case.store, None, case.topic, 'Already sent during this turn')
            await case.send(case.row('Owner joins the running turn'))
            await case.finish('Do not forward')
            self.assertEqual([row['text'] for row in case.output()], ['Final answer', 'Already sent during this turn'])

    async def test_group_migration_preserves_forward_delivery_and_reactions(self):
        async with self.case('opencode') as case:
            owner = case.row()
            await case.send(owner)
            await case.finish()
            await self.assert_forward(case, owner)
            from coordinator.setup_flow import migrate
            with case.store.db:
                self.assertTrue(migrate(case.store, 1, 5))
            self.assertEqual(case.store.db.execute('SELECT topic FROM parent_reply_turns').fetchone()[0], '5:2')
            self.assertEqual(case.store.db.execute('PRAGMA foreign_key_check').fetchall(), [])
            while await case.service.deliver_once():
                pass
            self.assertEqual(len(case.telegram.sent), 1)
            self.assertEqual(case.store.db.execute('SELECT reaction_desired FROM messages WHERE id=?',
                                                   (owner['id'],)).fetchone()[0], '👌')

    async def test_claude_queued_owner_does_not_belong_to_previous_background_turn(self):
        async with self.case('claude') as case:
            root = case.fixture.root
            (root / 'hold-auto').touch()
            await case.send(case.row('bg-work', 'event'))
            await until(lambda: not case.session.busy and not case.session.turn_open)
            (root / 'bg-finish').touch()
            await until(lambda: (root / 'auto.started').exists())
            owner = case.row('Queued while background task finishes')
            await case.send(owner)
            (root / 'release-auto').touch()
            await until(lambda: case.store.db.execute('SELECT receipt FROM messages WHERE id=?',
                                                      (owner['id'],)).fetchone()[0] == 'replayed')
            self.assertEqual(case.output(), [])
            await case.finish('Owner answer')
            await self.assert_forward(case, owner, 'Owner answer')
