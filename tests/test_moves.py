import json
import tempfile
import time
from types import SimpleNamespace
import unittest
from pathlib import Path
from unittest.mock import patch

from coordinator import moves, worker_storage
from coordinator.extension import optional
from coordinator.opencode_runtime import OpenCodeBroker
from coordinator.providers import RunResult
from coordinator.store import Store


def without_table(name):
    return None if name == 'routing' else optional(name)


class MoveRuleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store(Path(self.temp.name))
        self.addCleanup(self.store.close)
        with self.store.db:
            self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                                  ('1:2', 1, 2, 'Chat', self.temp.name))
            self.store.put('coordinator_home_topic', '1:2')
            self.store.put('accounts', {'one': {}})
        self.task = self.store.task_create('1:2', 'Task', worktree=self.temp.name)
        self.out = set()
        self.pool = SimpleNamespace(
            store=self.store, brokers={},
            codex_accounts=SimpleNamespace(managed=lambda: True, automatic=lambda: True))
        self.blocks = {}
        opencode = OpenCodeBroker(self.store)
        opencode.select = lambda alias=None, worker_id=None: None if 'opencode' in self.out else 'o'
        opencode.block = lambda alias, worker_id=None: self.blocks
        self.pool.brokers = {
            'claude': self.broker('claude'), 'codex': self.broker('codex'), 'opencode': opencode}
        hidden = patch('coordinator.moves.optional', without_table)
        hidden.start()
        self.addCleanup(hidden.stop)

    def broker(self, provider):
        return SimpleNamespace(default_model=lambda: provider + '-default',
                               capacity=lambda model: provider not in self.out)

    def worker(self, provider='claude', model='claude-opus-5-5', fresh=1, session=None, routed_from=None):
        now = time.time()
        with self.store.db:
            cursor = self.store.db.execute(
                'INSERT INTO workers(task,topic,provider,prompt,model,fresh,session,routed_from,created,updated) '
                'VALUES (?,?,?,?,?,?,?,?,?,?)', (self.task['id'], '1:2', provider, 'Brief', model, fresh, session,
                                                 routed_from, now, now))
        return self.row(cursor.lastrowid)

    def row(self, worker_id):
        return dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (worker_id,)).fetchone())

    def route(self, worker, failed=False):
        entry = moves.next_route(self.pool, worker, self.store.task_get(self.task['id']), failed)
        return entry and moves.text(entry)

    def notices(self):
        return [row[0] for row in self.store.db.execute('SELECT text FROM outbox ORDER BY id')]

    def test_a_worker_that_has_not_started_moves_and_new_work_returns_after_the_reset(self):
        worker = self.worker()
        self.assertIsNone(self.route(worker))
        self.out.add('claude')
        entry = moves.next_route(self.pool, worker, self.store.task_get(self.task['id']))
        self.assertEqual(moves.text(entry), 'codex/gpt-6.2-sol')
        self.assertTrue(moves.move(self.pool, worker, entry))
        moved = self.row(worker['id'])
        self.assertEqual((moved['provider'], moved['model'], moved['session'], moved['fresh'], moved['prompt'],
                          moved['status'], moved['routed_from']),
                         ('codex', 'gpt-6.2-sol', None, 1, 'Brief', 'queued', 'claude/claude-opus-5-5'))
        self.assertIn('Claude is out; worker %d runs on ChatGPT gpt-6.2-sol.' % worker['id'],
                      self.store.task_get(self.task['id'])['notes'])
        self.assertEqual(self.notices(), ['Claude is out. New Claude work now runs on ChatGPT gpt-6.2-sol.'])
        self.assertFalse(moves.check_recovery(self.pool))
        self.out.discard('claude')
        self.assertTrue(moves.check_recovery(self.pool))
        self.assertEqual(self.notices()[1:], ['Claude is back. New Claude work runs on Claude again.'])
        self.assertIsNone(self.route(self.worker()))

    def test_work_on_either_text_provider_moves_to_the_same_tier_on_the_other(self):
        self.out.add('codex')
        self.assertEqual(self.route(self.worker(provider='codex', model=None)), 'claude/claude-opus-5-5')
        self.assertEqual(self.route(self.worker(provider='codex', model='gpt-6-luna')), 'claude/claude-haiku-5-5')
        self.out = {'claude'}
        self.assertEqual(self.route(self.worker(model='claude-sonnet-5-5')), 'codex/gpt-6.1-terra')

    def test_work_on_a_provider_with_no_route_list_entry_moves_out_and_nothing_moves_in(self):
        provider = 'opencode'
        self.store.put(provider + '_enabled', True)
        self.out = set()
        fresh = self.worker(provider=provider, model=None)
        started = self.worker(provider=provider, model=None, fresh=0, session='session-1')
        self.assertEqual([moves.text(entry) for entry in moves.chain(self.store, fresh)],
                         [provider, 'claude/claude-opus-5-5', 'codex/gpt-6.2-sol'])
        self.assertIsNone(self.route(fresh))
        self.assertIsNone(self.route(started, failed=True))
        self.out.add(provider)
        self.assertEqual(self.route(fresh), 'claude/claude-opus-5-5')
        self.assertEqual(self.route(started, failed=True), 'claude/claude-opus-5-5')
        entry = moves.next_route(self.pool, started, self.store.task_get(self.task['id']), True)
        self.assertTrue(moves.move(self.pool, started, entry))
        moved = self.row(started['id'])
        self.assertEqual((moved['provider'], moved['session'], moved['fresh'], moved['prompt']),
                         ('claude', None, 1, moves.HANDOFF + 'Brief'))
        self.out.add('claude')
        self.assertEqual(self.route(fresh), 'codex/gpt-6.2-sol')
        self.store.put(provider + '_enabled', False)
        self.assertIsNone(self.route(fresh))
        self.out = {'claude', 'codex'}
        chains = [moves.chain(self.store, self.worker(provider=provider)) for provider in ('claude', 'codex')]
        self.assertEqual({entry['provider'] for entries in chains for entry in entries}, {'claude', 'codex'})

    def test_a_started_worker_stays_on_its_provider_while_another_account_of_it_has_capacity(self):
        self.store.put('antigravity_enabled', True)
        self.pool.brokers['antigravity'] = self.broker('antigravity')
        worker = self.worker(provider='antigravity', model='model-a', fresh=0, session='session-1')
        with self.store.db:
            self.store.db.execute("UPDATE workers SET account_alias='a1' WHERE id=?", (worker['id'],))
        worker = self.row(worker['id'])
        self.assertIsNone(self.route(worker, failed=True))
        self.assertIsNone(self.route(worker))
        self.out.add('antigravity')
        self.assertEqual(self.route(worker, failed=True), 'claude/claude-opus-5-5')

    def test_a_short_opencode_rate_limit_is_not_an_outage(self):
        self.store.put('opencode_enabled', True)
        self.store.put('opencode_accounts', {'o': {'model': 'test'}})
        self.store.put('opencode_active_account', 'o')
        worker = self.worker(provider='opencode', model=None, fresh=0, session='session-1')
        self.out.add('opencode')
        self.blocks = {'reason': 'rate_limited'}
        self.assertIsNone(self.route(worker, failed=True))
        self.blocks = {'reason': 'out_of_credit'}
        self.assertEqual(self.route(worker, failed=True), 'claude/claude-opus-5-5')

    def test_a_started_worker_moves_only_when_its_own_route_is_out(self):
        started = self.worker(fresh=0, session='session-1')
        self.assertIsNone(self.route(started, failed=True))
        returned = self.worker(provider='codex', model='gpt-6.2-sol', fresh=0, session='thread-1',
                               routed_from='claude/claude-opus-5-5')
        self.assertIsNone(self.route(returned))
        self.out.add('claude')
        self.assertEqual(self.route(started, failed=True), 'codex/gpt-6.2-sol')
        self.assertEqual(self.route(started), 'codex/gpt-6.2-sol')

    def test_a_handoff_with_no_handoff_file_keeps_the_text_from_the_spawn(self):
        worker = self.worker(fresh=0, session='session-1')
        with self.store.db:
            self.store.db.execute("UPDATE workers SET handoff='Earlier notes.' WHERE id=?", (worker['id'],))
        self.out.add('claude')
        entry = moves.next_route(self.pool, worker, self.store.task_get(self.task['id']))
        self.assertTrue(moves.move(self.pool, self.row(worker['id']), entry))
        moved = self.row(worker['id'])
        self.assertEqual((moved['handoff'], moved['prompt'], moved['session'], moved['fresh']),
                         ('Earlier notes.', moves.HANDOFF + 'Brief', None, 1))

    def test_work_waits_when_no_route_has_capacity_or_torii_cannot_move_it(self):
        self.out.update({'claude', 'codex'})
        self.assertIsNone(self.route(self.worker()))
        self.assertIsNone(self.route(self.worker(fresh=0, session='session-1'), failed=True))
        self.out.discard('claude')
        self.pool.codex_accounts.automatic = lambda: False
        self.assertIsNone(self.route(self.worker(provider='codex', model=None)))
        self.assertIsNone(self.route(self.worker(provider='opencode', model=None), failed=True))
        self.store.put('codex_enabled', False)
        self.out = {'claude'}
        self.assertIsNone(self.route(self.worker()))


class CapacityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store(Path(self.temp.name))
        self.addCleanup(self.store.close)

    def test_each_broker_answers_capacity_for_a_model(self):
        from coordinator.accounts import AccountBroker
        from coordinator.codex_accounts import CodexBroker
        now = time.time()
        (Path(self.temp.name) / 'one').mkdir()
        with self.store.db:
            self.store.put('accounts', {'one': {'config_dir': str(Path(self.temp.name) / 'one'), 'enabled': True}})
            self.store.put('account_status', {'one': {
                'identity': {'email': 'one@example.com', 'logged_in': True}, 'observed_at': now,
                'usage': {'seven_day': {'utilization': 78, 'resets_at': now + 7200},
                          'seven_day_fable': {'utilization': 100, 'resets_at': now + 3600}}}})
        claude = AccountBroker(self.store)
        self.assertEqual((claude.capacity('opus'), claude.capacity('claude-fable-5-2')), (True, False))
        with self.assertRaises(TypeError):
            claude.capacity()
        codex = CodexBroker(self.store)
        for enabled, managed, selected, expected in ((True, True, 'c', True), (True, True, None, False),
                                                     (True, False, 'c', False), (False, True, 'c', False)):
            with self.subTest(enabled=enabled, managed=managed, selected=selected):
                self.store.put('codex_enabled', enabled)
                with patch.object(CodexBroker, 'managed', return_value=managed), \
                        patch.object(CodexBroker, 'select', return_value=selected):
                    self.assertIs(codex.capacity('gpt-6.2-sol'), expected)
        opencode = OpenCodeBroker(self.store)
        self.store.put('opencode_active_account', 'o')
        self.store.put('opencode_accounts', {'o': {'model': 'test'}})
        self.assertEqual(opencode.default_model(), 'test')
        for reason, selected, expected in (('rate_limited', None, True), ('out_of_credit', None, False),
                                           (None, 'o', True)):
            with self.subTest(reason=reason, selected=selected), \
                    patch.object(OpenCodeBroker, 'block', return_value={'reason': reason} if reason else {}), \
                    patch.object(OpenCodeBroker, 'select', return_value=selected):
                self.assertIs(opencode.capacity('test'), expected)
        antigravity = optional('antigravity_accounts')
        if antigravity is not None:
            broker = antigravity.AntigravityBroker(self.store)
            for selected, expected in (('a', True), (None, False)):
                with patch.object(antigravity.AntigravityBroker, 'select', return_value=selected):
                    self.assertIs(broker.capacity(antigravity.DEFAULT_MODEL), expected)


    def test_each_broker_answers_ready_and_reset_for_a_waiting_worker(self):
        import asyncio
        from coordinator.accounts import AccountBroker
        from coordinator.codex_accounts import CodexBroker
        now = time.time()
        (Path(self.temp.name) / 'one').mkdir()
        with self.store.db:
            self.store.put('accounts', {'one': {'config_dir': str(Path(self.temp.name) / 'one'), 'enabled': True}})
            self.store.put('account_status', {'one': {
                'identity': {'email': 'one@example.com', 'logged_in': True}, 'observed_at': now,
                'usage': {'seven_day': {'utilization': 78, 'resets_at': now + 7200},
                          'seven_day_fable': {'utilization': 100, 'resets_at': now + 3600}}}})
        worker = {'id': 7, 'model': 'claude-opus-5-5', 'account_alias': 'one', 'session': 's', 'fresh': 0}
        fable = dict(worker, model='claude-fable-5-2')
        claude = AccountBroker(self.store)
        self.assertEqual((asyncio.run(claude.ready(worker)), asyncio.run(claude.ready(fable))), (True, False))
        self.assertEqual((claude.reset(worker), claude.reset(fable)), (None, now + 3600))
        codex = CodexBroker(self.store)
        for selected, reset in (('c', None), (None, now + 60)):
            with patch.object(CodexBroker, 'select', return_value=selected), \
                    patch.object(CodexBroker, 'earliest_reset', return_value=reset):
                self.assertEqual((asyncio.run(codex.ready(worker)), codex.reset(worker)), (bool(selected), reset))
        opencode = OpenCodeBroker(self.store)
        with patch.object(OpenCodeBroker, 'earliest_reset', return_value=now + 90) as reset:
            self.assertEqual(opencode.reset(worker), now + 90)
        reset.assert_called_once_with('one', 7)
        antigravity = optional('antigravity_accounts')
        if antigravity is not None:
            broker = antigravity.AntigravityBroker(self.store)
            for row, pinned in ((worker, 'one'), (dict(worker, fresh=1), None), (dict(worker, session=None), None)):
                with patch.object(antigravity.AntigravityBroker, 'select', return_value=None) as select, \
                        patch.object(antigravity.AntigravityBroker, 'earliest_reset', return_value=now) as until:
                    self.assertFalse(asyncio.run(broker.ready(row)))
                    self.assertEqual(broker.reset(row), now)
                select.assert_called_once_with(pinned, model='claude-opus-5-5')
                until.assert_called_once_with(pinned)


class RunningWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from coordinator.service import Service
        from tests.support import isolate_shared_mcp_home, stop_test_hosts
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        isolate_shared_mcp_home(self, self.root)
        self.addCleanup(stop_test_hosts, self.root)
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        with self.store.db:
            self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                                  ('1:2', 1, 2, 'Chat', str(self.root)))
            (self.root / 'one').mkdir()
            self.store.put('accounts', {'one': {'config_dir': str(self.root / 'one'), 'enabled': True}})
            self.store.put('account_status', {'one': {'identity': {'email': 'one@example.com', 'logged_in': True},
                                                      'observed_at': time.time()}})
        self.task = self.store.task_create('1:2', 'Task', worktree=str(self.root))
        with self.store.db:
            self.worker = self.store.db.execute(
                'INSERT INTO workers(task,topic,provider,prompt,cwd,model,created,updated) VALUES (?,?,?,?,?,?,?,?)',
                (self.task['id'], '1:2', 'claude', 'Brief', str(self.root), 'claude-opus-5-5', 1.0, 1.0)).lastrowid
        self.launches = []
        self.out = set()
        self.limited = {'claude'}
        test = self

        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                test.launches.append({'provider': provider, 'prompt': prompt, 'session': session,
                                      'fresh': options['fresh'], 'model': options['model'],
                                      'instructions': options['instructions']})
                if provider in test.limited:
                    session = session or 'native-1'
                    options['on_session'](session)
                    test.out.add(provider)
                    return RunResult(session, quota_limited=True, failure_code='quota_limited', transcript_path=(
                        str(test.root / 'transcript.jsonl') if provider == 'claude' else None))
                return RunResult(session or 'thread-1', success=True, text='Done')

        self.service = Service(self.store, None, Runner(), self.root)
        for target in (patch('coordinator.moves.optional', without_table),
                       patch('coordinator.moves.capacity', lambda pool, entry: entry['provider'] not in self.out)):
            target.start()
            self.addCleanup(target.stop)

    def row(self):
        return self.service.workers.get(self.worker)

    async def test_a_running_worker_that_hits_its_limit_continues_on_the_next_route_from_a_handoff(self):
        from coordinator.workers import STEER_SEPARATOR, remember_steer
        with self.store.db:
            remember_steer(self.store, self.row(), 'Also update the docs.', 1)
        results = Path(worker_storage.results_directory(self.store, self.task['id'], create=True))
        (results / 'HANDOFF.md').write_text('goal: ship the fix\nnext step: run the tests\n')
        await self.service.workers.run(self.row(), self.store.topic('1:2'))
        first, second = self.launches
        self.assertEqual((first['provider'], first['prompt'], first['fresh']), ('claude', 'Brief', True))
        self.assertEqual((second['provider'], second['model'], second['session'], second['fresh']),
                         ('codex', 'gpt-6.2-sol', None, True))
        self.assertEqual(second['prompt'], moves.HANDOFF + 'Brief' + STEER_SEPARATOR + 'Also update the docs.')
        self.assertIn('Job handoff at spawn:\ngoal: ship the fix\nnext step: run the tests\n', second['instructions'])
        self.assertIn(str(results), second['instructions'])
        moved = self.row()
        self.assertEqual((moved['provider'], moved['session'], moved['status'], moved['routed_from']),
                         ('codex', 'thread-1', 'done', 'claude/claude-opus-5-5'))
        self.assertIn('Claude is out; worker %d runs on ChatGPT gpt-6.2-sol.' % self.worker,
                      self.store.task_get(self.task['id'])['notes'])

    async def test_a_running_worker_on_a_provider_that_no_list_names_moves_out_from_a_handoff(self):
        class Broker:
            async def run(self, run, provider, prompt, cwd, session, notice_topic=None, **options):
                return await run(provider, prompt, cwd, session, **options)

        provider = 'opencode'
        with self.store.db:
            worker = self.store.db.execute(
                'INSERT INTO workers(task,topic,provider,prompt,cwd,created,updated) VALUES (?,?,?,?,?,?,?)',
                (self.task['id'], '1:2', provider, 'Brief', str(self.root), 1.0, 1.0)).lastrowid
            self.store.put(provider + '_enabled', True)
        self.service.workers.brokers[provider] = Broker()
        self.limited, self.out = {provider}, set()
        self.launches.clear()
        with patch('coordinator.workers.spawn_refusal', return_value=None):
            await self.service.workers.run(self.service.workers.get(worker), self.store.topic('1:2'))
        self.assertEqual([(launch['provider'], launch['fresh']) for launch in self.launches],
                         [(provider, True), ('claude', True)])
        self.assertEqual(self.launches[1]['prompt'], moves.HANDOFF + 'Brief')
        moved = self.service.workers.get(worker)
        self.assertEqual((moved['provider'], moved['model'], moved['status'], moved['routed_from']),
                         ('claude', 'claude-opus-5-5', 'done', provider))
        self.assertTrue(json.loads(moved['result'])['success'])

    async def test_a_running_worker_waits_for_the_reset_when_no_route_has_capacity(self):
        self.out.add('codex')
        await self.service.workers.run(self.row(), self.store.topic('1:2'))
        self.assertEqual([launch['provider'] for launch in self.launches], ['claude'])
        waiting = self.row()
        self.assertEqual((waiting['provider'], waiting['status'], waiting['fresh'], waiting['session']),
                         ('claude', 'waiting_for_quota', 0, self.launches[0]['session']))
        self.assertFalse(self.service.workers.can_move(waiting))
        self.out.discard('codex')
        self.assertTrue(self.service.workers.can_move(waiting))


if __name__ == '__main__':
    unittest.main()
