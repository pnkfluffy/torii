import json
import tempfile
import time
from types import SimpleNamespace
import unittest
from pathlib import Path
from unittest.mock import patch

from coordinator import control_api, mcp, moves, routing
from coordinator.store import Store


class RoutingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store(Path(self.temp.name))
        self.addCleanup(self.store.close)
        with self.store.db:
            self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                                  ('1:2', 1, 2, 'Chat', self.temp.name))
            self.store.put('group', 1)
            self.store.put('mode', 'group')
            self.store.put('coordinator_home_topic', '1:2')
            self.store.put('accounts', {'one': {}})
        self.task = self.store.task_create('1:2', 'Task', worktree=self.temp.name)

    def call(self, op, source='cli', **params):
        with self.store.db:
            return control_api.call(self.store, op, params, topic='1:2', source=source)

    def spawn(self, **params):
        result = self.call('workers.spawn', task=self.task['id'], prompt='Brief', **params)
        if not result.ok:
            return result
        return dict(self.store.db.execute('SELECT provider,model,effort,kind FROM workers WHERE id=?',
                                          (result.data['worker'],)).fetchone())

    def test_entries_parse_and_the_google_plan_takes_gemini_models_only(self):
        self.assertEqual(routing.parse('codex/gpt-6.2-sol@high'),
                         {'provider': 'codex', 'model': 'gpt-6.2-sol', 'effort': 'high'})
        self.assertEqual(routing.parse('claude'), {'provider': 'claude', 'model': None, 'effort': None})
        self.assertEqual(routing.parse(' stop '), routing.STOP)
        for bad in ('antigravity', 'antigravity/claude-opus-5-5-high', 'antigravity/gpt-oss-120b-medium',
                    'claude/gpt-6.2-sol', 'codex/claude-opus-5-5', 'codex/gemini-4-neon-high', 'opencode/model-x',
                    'codex@huge'):
            with self.subTest(entry=bad), self.assertRaises(control_api.Refused):
                routing.parse(bad)
        for bad in ('stop', 'claude, stop, codex', ''):
            with self.subTest(order=bad), self.assertRaises(control_api.Refused):
                routing.parse_order(bad)
        for entries in routing.DEFAULT_ROUTES.values():
            self.assertTrue(all(isinstance(routing.parse(entry), dict) for entry in entries))

    def test_spawn_by_kind_uses_the_first_route_that_can_take_the_worker(self):
        self.assertEqual(self.spawn(kind='general'),
                         {'provider': 'codex', 'model': 'gpt-6.2-sol', 'effort': 'high', 'kind': 'general'})
        self.assertEqual(self.spawn(), self.spawn(kind='general'))
        self.assertEqual(self.spawn(kind='judgment', effort='max'),
                         {'provider': 'claude', 'model': 'claude-fable-5-2', 'effort': 'max', 'kind': 'judgment'})
        self.assertEqual(self.spawn(kind='writing'),
                         {'provider': 'claude', 'model': 'claude-opus-5-5', 'effort': 'medium', 'kind': 'writing'})
        self.assertEqual(self.spawn(kind='design', provider='codex', model='gpt-6-luna'),
                         {'provider': 'codex', 'model': 'gpt-6-luna', 'effort': 'high', 'kind': 'design'})
        self.assertEqual(self.spawn(provider='codex', effort='low')['kind'], None)
        for params, text in (({'kind': 'poetry'}, 'judgment, design'),
                             ({'kind': 'general', 'model': 'gpt-6-luna'}, 'Give model only with provider.')):
            with self.subTest(params=params):
                result = self.spawn(**params)
                self.assertFalse(result.ok)
                self.assertIn(text, result.text)
        self.store.put('codex_enabled', False)
        self.assertEqual(self.spawn(kind='image').text,
                         'No route of kind image can take this worker now. See routing.show.')

    def test_coordinator_spawn_requires_an_owner_citation_for_claude_or_codex_overrides(self):
        with self.store.db:
            self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                                  ('1:3', 1, 3, 'Other chat', self.temp.name))
            owner = self.store.message_save('1:2', 'owner', 'Use Claude for this job.')['id']
            callback = self.store.message_save('1:2', 'callback', 'Resume')['id']
            other = self.store.message_save('1:3', 'owner', 'Use Claude here.')['id']

        def spawn(topic='1:2', **params):
            return mcp.dispatch(self.store, {'method': 'tools/call', 'params': {
                'name': 'workers_spawn', 'arguments': dict(task=self.task['id'], prompt='Brief', **params)}}, topic=topic)

        for topic, params in (
                ('1:2', {'provider': 'claude', 'kind': 'general'}),
                ('1:2', {'provider': 'codex', 'model': 'gpt-6-luna'}),
                ('1:2', {'kind': 'design', 'model': 'haiku'}),
                ('1:2', {'provider': 'claude', 'message': callback}),
                ('1:2', {'provider': 'claude', 'message': other}),
                ('1:3', {'provider': 'claude', 'message': owner})):
            with self.subTest(topic=topic, params=params):
                result = spawn(topic, **params)
                self.assertTrue(result['isError'], result)
                self.assertIn('Pass kind and let Torii pick', result['content'][0]['text'])
        with patch('coordinator.workers.spawn_refusal', return_value=None):
            for params, expected in (
                    ({'provider': 'claude', 'kind': 'general', 'model': 'haiku', 'message': owner}, ('claude', 'haiku', 'general')),
                    ({'provider': 'antigravity', 'model': 'gemini-4-argon-high'}, ('antigravity', 'gemini-4-argon-high', None)),
                    ({'provider': 'opencode'}, ('opencode', None, None)),
                    ({'kind': 'design'}, ('claude', 'claude-opus-5-5', 'design')),
                    ({}, ('codex', 'gpt-6.2-sol', 'general'))):
                with self.subTest(params=params):
                    result = spawn(**params)
                    self.assertFalse(result['isError'], result)
                    worker = json.loads(result['content'][0]['text'].splitlines()[-1])['worker']
                    self.assertEqual(tuple(self.store.db.execute(
                        'SELECT provider,model,kind FROM workers WHERE id=?', (worker,)).fetchone()), expected)
        self.assertEqual(self.spawn(provider='claude', kind='general', model='haiku')['model'], 'haiku')

    def test_an_available_antigravity_route_takes_writing_without_goal_secret_or_plugin(self):
        with patch('coordinator.workers.spawn_refusal', return_value=None):
            self.assertEqual(self.spawn(kind='writing')['provider'], 'antigravity')
            self.assertEqual(self.spawn(kind='writing')['model'], 'gemini-4-argon-high')
            with patch('coordinator.claude_settings.plugin_switches', return_value={'p': True}):
                self.assertEqual(self.spawn(kind='image')['provider'], 'codex')
            self.store.put('plugin_chats', {'plugin': ['3:4']})
            self.assertEqual(self.spawn(kind='image')['provider'], 'antigravity')
            self.store.put('plugin_chats', {'plugin': ['1:2']})
            self.assertEqual(self.spawn(kind='image')['provider'], 'codex')

    def test_a_loadout_swap_sets_a_route_list_for_a_while(self):
        owner = self.store.message_save('1:2', 'owner', 'Use Claude for general work today', telegram_message=7)['id']

        def swap(value, key='routes.general'):
            return self.call('loadout.swap', key=key, value=value, until='1d', reason='Owner asked', message=owner)
        self.assertFalse(swap('opencode').ok)
        self.assertFalse(swap('claude', key='routes.poetry').ok)
        self.assertFalse(swap('claude', key='route.claude').ok)
        made = swap('claude/claude-opus-5-5, stop')
        self.assertTrue(made.ok, made.text)
        self.assertEqual(self.spawn(kind='general')['model'], 'claude-opus-5-5')
        from coordinator import loadout
        self.assertIn('route general work to "claude/claude-opus-5-5, stop"', loadout.render(self.store, '1:2'))
        self.assertTrue(self.call('loadout.unswap', id=made.data['id']).ok)
        self.assertEqual(self.spawn(kind='general')['provider'], 'codex')

    def test_set_and_show(self):
        self.assertFalse(self.call('routing.set', source='mcp', kind='general', order='claude').ok)
        result = self.call('routing.set', kind='general', order='claude/claude-opus-5-5@high, codex, stop')
        self.assertTrue(result.ok, result.text)
        self.assertEqual(self.store.get('routes'), {'general': ['claude/claude-opus-5-5@high', 'codex', 'stop']})
        self.assertEqual(self.spawn(kind='general')['model'], 'claude-opus-5-5')
        self.assertFalse(self.call('routing.set', kind='general', order='antigravity').ok)
        self.assertTrue(self.call('routing.set', kind='all', order='codex').ok)
        self.assertEqual(sorted(self.store.get('routes')), sorted(routing.TEXT_KINDS))
        shown = self.call('routing.show')
        self.assertIn('image: antigravity/gemini-4-neon-high → codex (default)', shown.text)
        self.assertEqual(shown.data['general']['routes'], ['codex'])
        self.assertTrue(self.call('routing.set', kind='all').ok)
        self.assertEqual(self.store.get('routes'), {})
        self.store.put('routes', {'general': ['opencode/model-x']})
        self.assertEqual(routing.routes(self.store)['general'], [routing.parse('codex/gpt-6.2-sol@high')])
        schema = mcp.input_schema(control_api.BY_ID['workers.spawn'])
        self.assertEqual(schema['properties']['kind']['enum'], list(routing.KINDS))
        self.assertNotIn('provider', schema['required'])


class FallbackTests(unittest.TestCase):
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
            store=self.store,
            accounts=SimpleNamespace(select=lambda model=None: None if 'claude' in self.out else 'one'),
            codex_accounts=SimpleNamespace(managed=lambda: True, automatic=lambda: True,
                                           select=lambda: None if 'codex' in self.out else 'c',
                                           uses_credits=lambda alias: 'credits' in self.out))
        self.pool.brokers = {
            'claude': SimpleNamespace(default_model=lambda: 'opus',
                                      capacity=lambda model: self.pool.accounts.select(model=model) is not None),
            'codex': SimpleNamespace(default_model=lambda: 'gpt-6.2-sol', capacity=lambda model: bool(
                self.store.get('codex_enabled', True) and self.pool.codex_accounts.managed()
                and self.pool.codex_accounts.select() is not None)),
            'antigravity': SimpleNamespace(default_model=lambda: 'g', capacity=lambda model: 'antigravity' not in self.out)}

    def worker(self, provider='codex', model=None, kind=None, fresh=1, session=None, goal=None, routed_from=None):
        now = time.time()
        with self.store.db:
            cursor = self.store.db.execute(
                'INSERT INTO workers(task,topic,provider,prompt,model,kind,fresh,session,goal,routed_from,created,updated) '
                'VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', (self.task['id'], '1:2', provider, 'Brief', model, kind, fresh,
                                                     session, goal, routed_from, now, now))
        return dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (cursor.lastrowid,)).fetchone())

    def route(self, worker, failed=False):
        entry = moves.next_route(self.pool, worker, self.store.task_get(self.task['id']), failed)
        return entry and moves.text(entry)

    def notices(self):
        return [row[0] for row in self.store.db.execute('SELECT text FROM outbox ORDER BY id')]

    def test_chain_is_the_own_route_then_the_kind_list_then_the_same_tier_on_the_other_text_provider(self):
        def chain(**worker):
            return [moves.text(entry) for entry in moves.chain(self.store, dict({'model': None, 'kind': None}, **worker))]
        self.assertEqual(chain(provider='codex'), ['codex', 'claude/claude-opus-5-5'])
        self.assertEqual(chain(provider='claude', model='claude-haiku-5-5'),
                         ['claude/claude-haiku-5-5', 'codex/gpt-6.2-sol@high'])
        self.assertEqual(chain(provider='claude', model='claude-haiku-5-5', kind='small'),
                         ['claude/claude-haiku-5-5', 'codex/gpt-6-luna'])
        self.assertEqual(chain(provider='codex', model='gpt-6.2-sol', kind='general'),
                         ['codex/gpt-6.2-sol', 'claude/claude-opus-5-5'])
        self.assertEqual(chain(provider='claude', model='claude-fable-5-2', kind='judgment'),
                         ['claude/claude-fable-5-2', 'claude/claude-opus-5-5@high', 'codex/gpt-6.2-sol'])
        self.assertEqual(chain(provider='claude', model='claude-opus-5-5', kind='judgment'),
                         ['claude/claude-opus-5-5', 'claude/claude-fable-5-2@high', 'codex/gpt-6.2-sol'])
        self.assertEqual(chain(provider='claude', model='claude-opus-5-5', kind='general', routed_from='codex'),
                         ['codex/gpt-6.2-sol@high', 'claude/claude-opus-5-5@high'])
        self.assertEqual(chain(provider='antigravity', model='gemini-4-argon-high', kind='writing'),
                         ['antigravity/gemini-4-argon-high', 'claude/claude-opus-5-5', 'codex/gpt-6.2-sol'])
        self.assertEqual(chain(provider='antigravity', model='gemini-4-neon-high', kind='image'),
                         ['antigravity/gemini-4-neon-high', 'codex'])
        self.assertEqual(chain(provider='antigravity', model='gemini-4-neon-high'),
                         ['antigravity/gemini-4-neon-high', 'codex/gpt-6.2-sol@high', 'claude/claude-opus-5-5'])
        self.assertEqual(chain(provider='opencode'), ['opencode', 'codex/gpt-6.2-sol@high', 'claude/claude-opus-5-5'])
        self.store.put('routes', {'general': ['codex/gpt-6.2-sol@high', 'stop']})
        self.assertEqual(chain(provider='codex', model='gpt-6.2-sol', kind='general'), ['codex/gpt-6.2-sol'])
        self.assertEqual(chain(provider='claude', model='claude-opus-5-5'), ['claude/claude-opus-5-5', 'codex/gpt-6.2-sol@high'])

    def test_work_with_no_kind_walks_the_general_list(self):
        self.store.put('routes', {'general': ['codex/gpt-6-luna', 'claude/claude-sonnet-5-5']})
        self.assertIsNone(self.route(self.worker(provider='claude', model='claude-opus-5-5')))
        self.out.add('claude')
        self.assertEqual(self.route(self.worker(provider='claude', model='claude-opus-5-5')), 'codex/gpt-6-luna')
        self.assertEqual(self.route(self.worker(provider='claude', model='claude-opus-5-5', fresh=0, session='s')),
                         'codex/gpt-6-luna')
        self.out.add('codex')
        self.assertIsNone(self.route(self.worker(provider='claude', model='claude-opus-5-5')))

    def test_antigravity_enters_only_through_a_list_that_names_it_and_plugin_chats_skip_it(self):
        for kind in (None, *routing.KINDS):
            for provider, model in (('claude', 'claude-opus-5-5'), ('codex', 'gpt-6.2-sol')):
                named = {entry['provider'] for entry in routing.routes(self.store)[kind or 'general']}
                found = {entry['provider'] for entry in moves.chain(
                    self.store, {'provider': provider, 'model': model, 'kind': kind})}
                with self.subTest(kind=kind, provider=provider):
                    self.assertEqual('antigravity' in found, 'antigravity' in named)
        self.assertIsNone(self.route(self.worker(provider='claude', kind='writing')))
        self.out.add('codex')
        worker = self.worker(provider='codex', kind='image')
        with patch('coordinator.workers.spawn_refusal', return_value=None):
            self.assertEqual(self.route(worker), 'antigravity/gemini-4-neon-high')
            with patch('coordinator.claude_settings.plugin_switches', return_value={'p': True}):
                self.assertIsNone(self.route(worker))
            self.out.add('antigravity')
            self.assertIsNone(self.route(worker))

    def test_a_started_worker_keeps_its_session_on_another_model_of_its_provider(self):
        fable_out = set()
        self.pool.accounts = SimpleNamespace(select=lambda model=None: None if model in fable_out else 'one')
        worker = self.worker(provider='claude', model='claude-fable-5-2', kind='judgment', fresh=0, session='session-1')
        self.assertIsNone(self.route(worker, failed=True))
        fable_out.add('claude-fable-5-2')
        entry = moves.next_route(self.pool, worker, self.store.task_get(self.task['id']), True)
        self.assertEqual(moves.text(entry), 'claude/claude-opus-5-5@high')
        self.assertTrue(moves.move(self.pool, worker, entry, {'session_id': 'session-1'}))
        moved = dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (worker['id'],)).fetchone())
        self.assertEqual((moved['provider'], moved['model'], moved['session'], moved['fresh'], moved['prompt'], moved['status']),
                         ('claude', 'claude-opus-5-5', 'session-1', 0, 'Brief', 'queued'))
        self.assertEqual(self.notices(), [])

    def test_a_worker_that_torii_cannot_move_stays(self):
        self.out.add('codex')
        self.assertEqual(self.route(self.worker()), 'claude/claude-opus-5-5')
        self.pool.codex_accounts.automatic = lambda: False
        self.assertIsNone(self.route(self.worker()))
        self.pool.codex_accounts.automatic = lambda: True
        self.out.add('claude')
        self.assertIsNone(self.route(self.worker()))
        self.out.discard('codex')
        self.assertIsNone(self.route(self.worker(provider='antigravity', model='gemini-4-neon-high')))
        self.assertIsNone(self.route(self.worker(provider='opencode'), failed=True))

    def test_credits_keep_chatgpt_work_on_chatgpt(self):
        self.out.add('credits')
        self.assertIsNone(self.route(self.worker(kind='general', model='gpt-6.2-sol')))
        from coordinator.workers import WorkerPool
        pool = WorkerPool.__new__(WorkerPool)
        pool.__dict__.update(vars(self.pool))
        self.assertFalse(pool.can_move(self.worker(kind='general', model='gpt-6.2-sol')))
        self.out.add('codex')
        self.assertTrue(pool.can_move(self.worker(kind='general', model='gpt-6.2-sol')))

    def test_a_queued_worker_returns_to_its_normal_route(self):
        worker = self.worker(provider='claude', model='claude-opus-5-5', kind='general', routed_from='codex')
        self.assertEqual(self.route(worker), 'codex/gpt-6.2-sol@high')
        self.assertIsNone(self.route(self.worker(provider='claude', model='claude-opus-5-5', kind='general')))
        self.assertIsNone(self.route(self.worker(provider='codex', model='gpt-6.2-sol', kind='general')))
        self.assertEqual(self.route(self.worker(provider='codex', model='gpt-6.2-sol', kind='general'), failed=True),
                         'claude/claude-opus-5-5')
        self.store.put('routes', {'general': ['codex/gpt-6.2-sol', 'claude/claude-opus-5-5']})
        self.assertIsNone(self.route(self.worker(provider='claude', model='claude-opus-5-5', kind='general')))
        self.assertIsNone(self.route(self.worker(provider='claude', model='claude-opus-5-5')))
        self.assertIsNone(self.route(self.worker(provider='claude')))

    def test_move_notifies_once_per_outage_and_once_at_recovery(self):
        self.out.add('codex')
        for _ in range(2):
            worker = self.worker(kind='general', model='gpt-6.2-sol')
            entry = moves.next_route(self.pool, worker, self.store.task_get(self.task['id']), True)
            self.assertTrue(moves.move(self.pool, worker, entry))
            moved = self.store.db.execute('SELECT provider,model,effort,routed_from,status FROM workers WHERE id=?',
                                          (worker['id'],)).fetchone()
            self.assertEqual(tuple(moved), ('claude', 'claude-opus-5-5', 'medium', 'codex/gpt-6.2-sol', 'queued'))
        self.assertEqual(self.notices(), ['ChatGPT is out. New ChatGPT work now runs on Claude Opus 5.5.'])
        self.assertFalse(moves.check_recovery(self.pool))
        self.out.discard('codex')
        self.assertTrue(moves.check_recovery(self.pool))
        self.assertFalse(moves.check_recovery(self.pool))
        self.assertEqual(self.notices()[1:], ['ChatGPT is back. New ChatGPT work runs on ChatGPT again.'])

    def test_a_model_window_outage_keeps_named_claude_work_and_sends_no_provider_notice(self):
        fable_out = set()
        self.pool.accounts = SimpleNamespace(select=lambda model=None: None if (
            'claude' in self.out or model in fable_out) else 'one')
        fable_out.add('claude-fable-5-2')
        self.store.put('routes', {'judgment': ['claude/claude-fable-5-2', 'codex/gpt-6.2-sol']})
        for _ in range(2):
            worker = self.worker(provider='claude', model='claude-fable-5-2', kind='judgment')
            entry = moves.next_route(self.pool, worker, self.store.task_get(self.task['id']))
            self.assertEqual(moves.text(entry), 'codex/gpt-6.2-sol')
            self.assertTrue(moves.move(self.pool, worker, entry))
            self.assertFalse(moves.check_recovery(self.pool))
        self.assertEqual(self.notices(), [])
        self.assertIsNone(self.store.get('fallback_since'))

    def test_owner_views_show_the_route_now_and_the_table(self):
        self.assertEqual(routing.order_line(self.pool), 'Route now: ChatGPT gpt-6.2-sol → Claude Opus 5.5')
        self.out.update({'codex'})
        self.assertEqual(routing.order_line(self.pool), 'Route now: Claude Opus 5.5 · out: ChatGPT')
        self.out.update({'claude'})
        self.assertEqual(routing.order_line(self.pool), 'Route now: none, work waits · out: ChatGPT, Claude')
        self.out.clear()
        self.out.add('credits')
        self.store.put('routes', {'small': ['codex']})
        self.assertEqual(routing.order_line(self.pool), 'Route now: ChatGPT credits → Claude Opus 5.5 · owner routes: small')
        lines = routing.table_lines(self.store)
        self.assertEqual(lines[0], 'Routes:')
        self.assertIn('judgment: Claude Fable 5.2 → Claude Opus 5.5', lines)
        self.assertIn('image: Antigravity gemini-4-neon-high → ChatGPT', lines)
        from coordinator import control_ui
        self.assertIn('\n'.join(lines), control_ui._settings_view(self.store)[0])
        self.assertIn('Route now: ', control_ui._accounts_view(self.store, 0)[0])


class OneMovePerLaunchTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_worker_that_moved_does_not_move_again_when_the_new_launch_fails(self):
        from coordinator.providers import RunResult
        from coordinator.service import Service
        from tests.support import isolate_shared_mcp_home, stop_test_hosts
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        isolate_shared_mcp_home(self, root)
        self.addCleanup(stop_test_hosts, root)
        store = Store(root / 'state')
        self.addCleanup(store.close)
        with store.db:
            store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                             ('1:2', 1, 2, 'Chat', str(root)))
            (root / 'one').mkdir()
            store.put('accounts', {'one': {'config_dir': str(root / 'one'), 'enabled': True}})
        task = store.task_create('1:2', 'Task', worktree=str(root))
        with store.db:
            row = store.db.execute('INSERT INTO workers(task,topic,provider,prompt,cwd,model,created,updated) '
                                   'VALUES (?,?,?,?,?,?,?,?)', (task['id'], '1:2', 'claude', 'Brief', str(root),
                                                                'claude-fable-5-2', 1.0, 1.0)).lastrowid
        launches = []

        class Runner:
            async def run(self, provider, prompt, cwd, session, **options):
                launches.append(provider)
                return RunResult(session, quota_limited=True)

        entries = iter([routing.parse('claude/claude-opus-5-5'), routing.parse('claude/claude-sonnet-5-5')] * 3)

        def next_route(pool, worker, task, failed=False):
            return next(entries, None) if failed else None

        service = Service(store, None, Runner(), root)
        with patch('coordinator.moves.next_route', side_effect=next_route), \
                patch('coordinator.moves.move', wraps=moves.move) as move:
            await service.workers.run(service.workers.get(row), store.topic('1:2'))
        self.assertEqual(move.call_count, 1)
        self.assertEqual(service.workers.get(row)['model'], 'claude-opus-5-5')


if __name__ == '__main__':
    unittest.main()
