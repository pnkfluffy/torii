import asyncio
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import control_api, mcp, native_ids
from coordinator.accounts import AccountBroker
from coordinator.providers import ProviderRunner, RunResult
from coordinator.service import Service
from coordinator.store import Store
from coordinator.workers import WorkerPool
from tests.support import isolate_shared_mcp_home


class WorkerDispatchTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        isolate_shared_mcp_home(self, self.root)
        dummy_account = self.root / 'opencode-dummy'
        dummy_account.mkdir()
        (dummy_account / 'key').write_text('dummy-only\n')
        (dummy_account / 'key').chmod(0o600)
        homes = patch('coordinator.opencode_accounts.account_home', return_value=dummy_account)
        homes.start()
        self.addCleanup(homes.stop)
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test',?,1)",
                                  (str(self.root),))
            self.store.put('owner', 1)
            self.store.put('mode', 'group')
            self.store.put('group', 1)
        self.task = self.store.task_create('1:2', 'Build', worktree=str(self.root))

    def ready(self, provider):
        with self.store.db:
            self.store.task_update(self.task['id'], secrets=[])
            self.store.put(provider + '_enabled', True)
            self.store.put(provider + '_active_account', 'one')
            self.store.put(provider + '_accounts', {'one': {'enabled': True, 'config_dir': str(self.root)}})
            if provider == 'codex':
                self.store.put('codex_account_status', {'one': {'identity': {
                    'logged_in': True, 'email': 'one@example.test'}}})
            if provider == 'opencode':
                self.store.put('opencode_accounts', {'one': {'model': 'example-model'}})

    def call(self, op, **params):
        with self.store.db:
            return control_api.call(self.store, op, params, topic='1:2')

    def spawn(self, provider, **options):
        return self.call('workers.spawn', task=self.task['id'], provider=provider, prompt='Build it', **options)

    def test_worker_schema_accepts_four_providers_and_parent_kind_is_separate(self):
        self.assertEqual(control_api.find('workers.spawn').params['provider'], 'worker_provider')
        self.assertEqual(control_api.find('topic.bind').params['provider'], 'provider?')
        self.assertEqual(control_api.find('model.parent').params['provider'], 'parent_provider?')
        schema = mcp.input_schema(control_api.find('workers.spawn'))
        self.assertEqual(schema['properties']['provider']['enum'], ['claude', 'codex', 'opencode', 'muse'])
        for provider in ('opencode', 'muse'):
            with self.subTest(provider=provider):
                self.assertEqual(control_api._coerce(self.store, 'worker_provider', provider), (provider, None))
                for op in ('topic.bind', 'model.parent'):
                    result = self.call(op, provider=provider)
                    self.assertFalse(result.ok)
                    self.assertEqual(result.state, 'refused')
        self.assertFalse(self.spawn('unknown').ok)

    def test_spawn_refuses_absent_false_and_non_boolean_switches(self):
        for provider, label in (('opencode', 'OpenCode'),):
            self.assertEqual(self.spawn(provider).text, label + ' workers are off.')
            self.ready(provider)
            for value in (False, 1, 'true'):
                with self.subTest(provider=provider, value=value):
                    self.store.put(provider + '_enabled', value)
                    result = self.spawn(provider)
                    self.assertEqual((result.ok, result.text), (False, label + ' workers are off.'))
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM workers').fetchone()[0], 0)

    def test_spawn_refuses_goals_and_declared_secrets_before_creating_a_worker(self):
        for provider, label in (('opencode', 'OpenCode'),):
            with self.subTest(provider=provider):
                self.ready(provider)
                result = self.spawn(provider, goal='Tests pass')
                self.assertEqual((result.ok, result.text),
                                 (False, label + " workers don't take goals. Use Claude or Codex."))
                self.assertFalse(self.spawn(provider, goal='').ok)
                self.store.task_update(self.task['id'], secrets=['SERVICE_ACCESS=VAULT_ACCESS'])
                result = self.spawn(provider)
                self.assertEqual((result.ok, result.text),
                                 (False, label + " workers don't take vault secrets. Use a Claude worker."))
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM workers').fetchone()[0], 0)

    def test_spawn_requires_a_registered_active_account(self):
        for provider, text in (('opencode', 'No OpenCode account is active.'),):
            for missing in ('active', 'unregistered', 'accounts'):
                with self.subTest(provider=provider, missing=missing):
                    self.ready(provider)
                    if missing == 'active':
                        self.store.put(provider + '_active_account', None)
                    elif missing == 'unregistered':
                        self.store.put(provider + '_active_account', 'unknown')
                    else:
                        self.store.put(provider + '_accounts', {})
                    result = self.spawn(provider)
                    self.assertEqual((result.ok, result.text), (False, text))
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM workers').fetchone()[0], 0)

    def test_ready_workers_queue_but_cannot_receive_goals(self):
        for provider in ('opencode',):
            with self.subTest(provider=provider):
                self.ready(provider)
                result = self.spawn(provider, effort='max')
                self.assertTrue(result.ok, result.text)
                worker = self.call('workers.get', worker=result.data['worker']).data
                self.assertEqual((worker['provider'], worker['status'], worker['effort'], worker['session']),
                                 (provider, 'queued', 'max', None))
                for condition in ('Tests pass', 'clear'):
                    refused = self.call('workers.goal', worker=worker['id'], condition=condition)
                    self.assertEqual((refused.ok, refused.text), (False, 'Worker goals require Claude or Codex.'))
                self.assertIsNone(self.call('workers.get', worker=worker['id']).data['goal'])
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0], 0)

    async def test_launch_rechecks_switch_goal_secrets_and_active_account(self):
        runner = SimpleNamespace(run=AsyncMock())
        pool = WorkerPool(self.store, runner, AccountBroker(self.store))
        reasons = {'off': 'workers are off.', 'goal': "workers don't take goals.",
                   'secrets': "workers don't take vault secrets.", 'account': 'No '}
        for provider in ('opencode',):
            for reason, text in reasons.items():
                with self.subTest(provider=provider, reason=reason):
                    self.ready(provider)
                    queued = self.spawn(provider)
                    self.assertTrue(queued.ok, queued.text)
                    worker = pool.get(queued.data['worker'])
                    if reason == 'off':
                        self.store.put(provider + '_enabled', False)
                    elif reason == 'goal':
                        self.store.db.execute('UPDATE workers SET goal=? WHERE id=?', ('Tests pass', worker['id']))
                    elif reason == 'secrets':
                        self.store.task_update(self.task['id'], secrets=['SERVICE_ACCESS=VAULT_ACCESS'])
                    else:
                        self.store.put(provider + '_active_account', None)
                    with patch('coordinator.workers.load_for_worker') as load:
                        done = await pool.run(worker, self.store.topic('1:2'))
                    load.assert_not_called()
                    result = json.loads(done['result'])
                    self.assertEqual((done['status'], result['success'], result['failure_code']),
                                     ('done', False, 'start_failed'))
                    self.assertIn(text, result['failure_detail'])
        runner.run.assert_not_awaited()

    def test_broker_lookup_is_explicit_including_open_code(self):
        pool = WorkerPool(self.store, object(), AccountBroker(self.store))
        self.assertIs(pool.broker('claude'), pool.accounts)
        self.assertIs(pool.broker('codex'), pool.codex_accounts)
        with self.assertRaisesRegex(ValueError, 'Unknown provider'):
            pool.broker('unknown')
        self.assertIs(pool.broker('opencode'), pool.brokers['opencode'])
        self.assertIs(pool.broker('opencode').store, self.store)

    async def test_open_code_dispatches_the_active_account_model(self):
        self.ready('opencode')
        runner = SimpleNamespace(run=AsyncMock(return_value=RunResult(
            'ses_1b3d19f6fffeIxa5icm1pIrpgt', text='done', success=True)))
        pool = WorkerPool(self.store, runner, AccountBroker(self.store))
        worker = pool.get(self.spawn('opencode').data['worker'])
        done = await pool.run(worker, self.store.topic('1:2'))
        self.assertTrue(json.loads(done['result'])['success'])
        runner.run.assert_awaited_once()
        args, options = runner.run.call_args
        self.assertEqual(args, ('opencode', 'Build it', self.root, None))
        self.assertEqual((options['account_alias'], options['model']), ('one', 'example-model'))
        self.assertFalse(options['stopped']())
        self.assertTrue(callable(options['on_cancel']))
        self.assertEqual(options['worker_id'], worker['id'])
        self.assertEqual(done['account_alias'], 'one')

    async def test_provider_runner_keeps_unknown_and_acp_providers_unavailable(self):
        runner = ProviderRunner(self.store.directory)
        for provider in ('unknown', 'opencode'):
            for attach in (False, True):
                with self.subTest(provider=provider, attach=attach):
                    with self.assertRaisesRegex(ValueError, 'Unknown provider'):
                        await runner.run(provider, 'Build it', self.root, None, fresh=True, attach=attach)
        self.assertFalse((self.store.directory / 'hosts').exists())

    async def test_service_reselects_a_waiting_worker_through_its_provider_broker(self):
        self.ready('codex')
        runner = SimpleNamespace(run=AsyncMock(return_value=RunResult(
            '00000000-0000-4000-8000-000000000001', text='done', success=True)))
        service = Service(self.store, None, runner, self.root)
        self.assertIs(service.workers.broker('codex'), service.codex_accounts)
        worker_id = self.spawn('codex').data['worker']
        self.store.db.execute("UPDATE workers SET status='waiting_for_quota' WHERE id=?", (worker_id,))
        self.store.put('codex_account_blocks', {'one': {'until': 9999999999, 'reason': 'quota'}})
        self.assertFalse(await service.workers_once())
        runner.run.assert_not_awaited()
        self.store.put('codex_account_blocks', {})
        self.assertTrue(await service.workers_once())
        await asyncio.gather(*service.worker_tasks.values())
        done = service.workers.get(worker_id)
        self.assertTrue(json.loads(done['result'])['success'])
        self.assertEqual((done['provider'], done['account_alias']), ('codex', 'one'))
        self.assertEqual(runner.run.call_args.kwargs['model'], 'gpt-6.1-sol')


class NativeIdTests(unittest.TestCase):
    def test_each_provider_accepts_only_its_canonical_native_ids(self):
        sid = '12345678-abcd-4000-8000-123456789abc'
        opencode = 'ses_1b3d19f6fffeIxa5icm1pIrpgt'
        for provider in ('claude', 'codex'):
            self.assertTrue(native_ids.valid(provider, sid))
            for invalid in (sid.upper(), sid.replace('-', ''), '{' + sid + '}', opencode, '../x', '', None, 12):
                with self.subTest(provider=provider, invalid=invalid):
                    self.assertFalse(native_ids.valid(provider, invalid))
        self.assertTrue(native_ids.valid('opencode', opencode))
        for invalid in (sid, '../x', '', None, 12, opencode[:-1], opencode + 'x', opencode + '\n',
                        'ses_' + '_' * 26, 'ses_' + 'é' * 26):
            with self.subTest(provider='opencode', invalid=invalid):
                self.assertFalse(native_ids.valid('opencode', invalid))
        self.assertFalse(native_ids.valid('unknown', sid))
