"""Every worker provider answers the same questions. Each case runs once per provider in the registry
and checks properties, never a pinned command line.

The provider-name baseline for session_storage.py includes the session cleanup changes in #694.
"""

import asyncio
from contextlib import contextmanager
import dataclasses
import json
import os
import re
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from coordinator import native_ids, workers
from coordinator.accounts import AccountBroker
from coordinator.adapters import CAPS, LABELS, Caps
from coordinator.control_api import WORKER_PROVIDERS
from coordinator.extension import optional
from coordinator.host import HostClient
from coordinator.native_protocol import RunControl
from coordinator.providers import ProviderRunner
from coordinator.store import Store
from coordinator.workers import WorkerPool, host_directory
from tests.support import _group_alive, read_jsonl, settle, until

ALLOWED = {'goals': (None, 'native', 'steer'), 'steer': ('native', 'queued')}
ROOT = Path(__file__).resolve().parents[1]
BRANCH = re.compile(r"(==|!=|\bin\b|\bis\b) *\(?[^#]*'(%s)'|'(%s)' *(==|!=|if|else|in\b)|provider(_name)? *(==|!=|in|not in)"
                    r"|\.get\('provider'\) *(==|!=|in)" % (('|'.join(CAPS),) * 2))
NAMED = {
    'close_saves.py': 1, 'codex_accounts.py': 2, 'control_api.py': 16, 'control_ui.py': 9, 'controls.py': 12,
    'envelopes.py': 1, 'health.py': 3, 'host.py': 4, 'host_logs.py': 1, 'isolation.py': 1, 'lifecycle.py': 3,
    'loadout.py': 1, 'media.py': 3, 'moves.py': 6, 'native_ids.py': 2, 'native_protocol.py': 4,
    'opencode_accounts.py': 4, 'opencode_runtime.py': 2, 'opencode_subscription.py': 1, 'parent.py': 31,
    'parent_state.py': 22, 'providers.py': 67, 'routing.py': 5, 'service.py': 18, 'session.py': 1,
    'session_storage.py': 26, 'setup_flow.py': 2, 'signin.py': 26, 'store.py': 2, 'topics.py': 4, 'updates.py': 1,
    'worker_hooks.py': 6, 'worker_storage.py': 1, 'workers.py': 40}


def worker(**values):
    return {'id': 1, 'model': None, 'account_alias': None, 'session': None, 'fresh': 1, **values}


class CapabilityTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.store = Store(Path(temp.name))
        self.addCleanup(self.store.close)

    def test_every_provider_declares_every_capability_with_an_allowed_value(self):
        self.assertTrue(set(WORKER_PROVIDERS) <= set(CAPS))
        for provider, caps in CAPS.items():
            with self.subTest(provider=provider):
                self.assertIsInstance(caps, Caps)
                self.assertEqual(LABELS[provider], caps.label)
                for field in dataclasses.fields(Caps):
                    value = getattr(caps, field.name)
                    if field.name in ALLOWED:
                        self.assertIn(value, ALLOWED[field.name])
                    elif field.name == 'label':
                        self.assertTrue(value)
                    else:
                        self.assertIsInstance(value, bool, field.name)

    def test_the_spawn_refusal_agrees_with_the_capabilities(self):
        task = {'secrets': ['TOKEN']}
        for provider in WORKER_PROVIDERS:
            caps = CAPS[provider]
            with self.subTest(provider=provider), \
                    patch.dict(workers.ACCOUNT_REFUSALS, {name: lambda store, model: None
                                                          for name in workers.ACCOUNT_REFUSALS}):
                self.store.put(provider + '_enabled', True)
                self.assertIsNone(self.refuse(provider))
                self.assertEqual(self.refuse(provider, goal='done') is not None, caps.goals is None)
                self.assertEqual(self.refuse(provider, task=task) is not None, not caps.secrets)
                self.assertIsNone(self.refuse(provider, fresh=False))
                if caps.switched:
                    self.store.put(provider + '_enabled', False)
                    self.assertEqual(self.refuse(provider), caps.label + ' workers are off.')

    def refuse(self, provider, task=None, **options):
        return workers.spawn_refusal(self.store, provider, task or {'secrets': []}, **options)


class BrokerTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        self.brokers = workers.WorkerPool(self.store, SimpleNamespace(), AccountBroker(self.store)).brokers

    def test_every_provider_has_a_broker_and_capacity_needs_a_model(self):
        self.assertEqual(set(self.brokers), set(WORKER_PROVIDERS))
        for provider, broker in self.brokers.items():
            with self.subTest(provider=provider):
                with self.assertRaises(TypeError):
                    broker.capacity()
                self.assertIsInstance(broker.capacity(broker.default_model()), bool)

    def test_with_every_account_out_no_broker_has_capacity_or_a_ready_worker(self):
        now = time.time()
        (self.root / 'one').mkdir()
        with self.store.db:
            self.store.put('accounts', {'one': {'config_dir': str(self.root / 'one'), 'enabled': True}})
            self.store.put('account_status', {'one': {'identity': {'email': 'one@example.com', 'logged_in': True},
                                                      'observed_at': now}})
            self.store.put('account_blocks', {'one': {'until': now + 600, 'reason': 'quota'}})
        for provider, broker in self.brokers.items():
            with self.subTest(provider=provider):
                waiting = worker(account_alias='one', session='s', fresh=0)
                self.assertFalse(broker.capacity(broker.default_model()))
                self.assertFalse(asyncio.run(broker.ready(waiting)))
                reset = broker.reset(waiting)
                self.assertTrue(reset is None or reset > now, reset)

    def test_a_model_scoped_limit_blocks_only_that_model(self):
        now = time.time()
        (self.root / 'one').mkdir()
        with self.store.db:
            self.store.put('accounts', {'one': {'config_dir': str(self.root / 'one'), 'enabled': True}})
            self.store.put('account_status', {'one': {
                'identity': {'email': 'one@example.com', 'logged_in': True}, 'observed_at': now,
                'usage': {'seven_day': {'utilization': 78, 'resets_at': now + 7200},
                          'seven_day_fable': {'utilization': 100, 'resets_at': now + 3600}}}})
        claude = self.brokers['claude']
        self.assertEqual((claude.capacity('claude-opus-5-5'), claude.capacity('claude-fable-5-2')), (True, False))
        self.assertEqual((asyncio.run(claude.ready(worker(model='claude-opus-5-5'))),
                          asyncio.run(claude.ready(worker(model='claude-fable-5-2')))), (True, False))
        antigravity = optional('antigravity_accounts')
        if 'antigravity' not in self.brokers or antigravity is None:
            return
        (self.root / 'agy').mkdir()
        with self.store.db:
            self.store.put('antigravity_accounts', {'a': {'home': str(self.root / 'agy')}})
            self.store.put('antigravity_account_status', {'a': {'logged_in': True, 'models': [{'id': 'model-a'}]}})
        broker = self.brokers['antigravity']
        with patch.object(antigravity, 'accounts', return_value={'a': {}}):
            self.assertEqual((asyncio.run(broker.ready(worker(model='model-a'))),
                              asyncio.run(broker.ready(worker(model='model-b')))), (True, False))


class ProviderNameTests(unittest.TestCase):
    def test_no_new_core_file_branches_on_a_provider_name(self):
        found = {}
        for path in sorted((ROOT / 'coordinator').rglob('*.py')):
            if path.name == 'adapters.py':
                continue
            count = sum(1 for line in path.read_text().splitlines() if BRANCH.search(line))
            if count:
                found[str(path.relative_to(ROOT / 'coordinator'))] = count
        grown = {name: (count, NAMED.get(name, 0)) for name, count in found.items() if count > NAMED.get(name, 0)}
        self.assertEqual(grown, {}, 'Read a capability in coordinator/adapters.py instead of testing the name.')


SECRET = 'conformance-secret-5e81c2'
STEER_RECEIPTS = {'native': ('received', 'uncertain'), 'queued': ('queued', 'received')}
CLAUDE = '''#!/usr/bin/env python3
import json, os, sys, threading
from pathlib import Path
here = Path(__file__).parent
scenario = json.loads((here / 'scenario.json').read_text())
sid = sys.argv[sys.argv.index('--session-id' if '--session-id' in sys.argv else '--resume') + 1]
lock = threading.Lock()
def emit(event):
    with lock:
        print(json.dumps(event), flush=True)
def finish():
    while scenario.get('hold') and not (here / 'release').exists():
        threading.Event().wait(0.01)
    if scenario.get('quota'):
        emit({'type': 'rate_limit_event', 'rate_limit_info': {'status': 'rejected', 'resetsAt': 2000000000,
                                                             'rateLimitType': 'seven_day'}})
        emit({'type': 'result', 'subtype': 'error_during_execution', 'is_error': True, 'session_id': sid})
    else:
        seen = ' '.join(os.environ.get(name, '') for name in scenario.get('env', []))
        emit({'type': 'result', 'subtype': 'success', 'session_id': sid, 'result': 'done ' + seen})
emit({'type': 'system', 'subtype': 'init', 'session_id': sid})
turn = None
for line in sys.stdin:
    event = json.loads(line)
    with (here / 'calls.jsonl').open('a') as calls:
        calls.write(json.dumps({'message': event, 'env': {name: os.environ.get(name)
                                                          for name in scenario.get('env', [])}}) + '\\n')
    emit(dict(event, isReplay=True))
    if turn is None:
        turn = threading.Thread(target=finish)
        turn.start()
if turn is not None:
    turn.join()
'''
CODEX_QUOTA = '''#!/usr/bin/env python3
import json, sys
def emit(value):
    print(json.dumps(value), flush=True)
for line in sys.stdin:
    request = json.loads(line)
    method = request.get('method')
    if method == 'initialize':
        emit({'id': request['id'], 'result': {}})
    elif method == 'thread/start':
        emit({'id': request['id'], 'result': {'thread': {'id': SID}}})
    elif method == 'turn/start':
        emit({'id': request['id'], 'result': {'turn': {'id': 'turn-1'}}})
        emit({'method': 'turn/started', 'params': {'threadId': SID, 'turn': {'id': 'turn-1'}}})
        emit({'method': 'turn/completed', 'params': {'threadId': SID, 'turn': {
            'id': 'turn-1', 'status': 'failed', 'error': {'codexErrorInfo': 'usageLimitExceeded'}}}})
'''


class Bench:
    """One provider's existing test fixture behind the few calls the process cases need."""

    provider = None
    options = {}

    def __init__(self, case):
        self.case = case
        case.setUp()
        self.store = case.store
        self.runner = case.runner

    def close(self):
        """Run the fixture's cleanups here: its own doCleanups needs the event loop of a running test."""
        failures = []
        for function, args, kwargs in [(self.case.tearDown, (), {})] + self.case._cleanups[::-1]:
            try:
                function(*args, **kwargs)
            except Exception as error:
                failures.append(error)
        self.case._cleanups.clear()
        if failures:
            raise failures[0]

    async def run(self, session=None, runner=None, **options):
        options = dict(self.options, control=options.pop('control', None) or RunControl(), **options)
        options.setdefault('fresh', session is None)
        return await settle((runner or self.runner).run(self.provider, 'conformance prompt', self.cwd, session,
                                                        **options))

    def worker(self):
        self.store.db.execute("INSERT OR IGNORE INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test',?,1)",
                              (str(self.cwd),))
        task = self.store.task_create('1:2', 'Build', worktree=str(self.cwd))
        return self.store.db.execute("INSERT INTO workers (task,topic,provider,prompt,cwd,workspace,created,updated) "
                                     "VALUES (?,'1:2',?,'conformance prompt',?,'{}',1,1)",
                                     (task['id'], self.provider, str(self.cwd))).lastrowid


class ClaudeBench(Bench):
    provider = 'claude'
    options = {'account_alias': 'profile', 'topic': 'home'}

    def __init__(self):
        from tests import test_providers
        super().__init__(test_providers.ProviderTests('setUp'))
        self.cwd = self.case.root
        self.case.binary.write_text(CLAUDE)
        self.case.binary.chmod(0o700)
        self.scenario = {}
        self.set()

    def set(self, **values):
        self.scenario.update(values)
        (self.cwd / 'scenario.json').write_text(json.dumps(self.scenario))

    def new_runner(self):
        return ProviderRunner(self.store.directory, binaries={'claude': str(self.case.binary)},
                              account_broker=self.case.broker)

    def hold(self):
        self.set(hold=True)

    def release(self):
        (self.cwd / 'release').touch()

    def started(self):
        return bool(read_jsonl(self.cwd / 'calls.jsonl'))

    def quota(self):
        self.set(quota=True)

    def secret(self):
        self.set(env=['FAKE_SECRET'])
        return {'FAKE_SECRET': SECRET}

    def secret_seen(self):
        return read_jsonl(self.cwd / 'calls.jsonl')[0]['env'] == {'FAKE_SECRET': SECRET}


class CodexBench(Bench):
    provider = 'codex'

    def __init__(self):
        from tests import test_codex_secrets_goals
        super().__init__(test_codex_secrets_goals.CodexSecretsGoalsTests('setUp'))
        self.cwd = self.case.root

    def new_runner(self):
        runner = ProviderRunner(self.store.directory, binaries={'codex': str(self.case.binary)})
        runner.opencode_store = self.store
        return runner

    def hold(self):
        held = patch.dict(os.environ, {'FAKE_HOLD_TURN': '1', 'FAKE_WAIT_FINISH': '1'})
        held.start()
        self.case.addCleanup(held.stop)

    def release(self):
        (self.cwd / 'finish-turn').touch()

    def started(self):
        return (self.cwd / 'turn-started').exists()

    def quota(self):
        self.case.binary.write_text(CODEX_QUOTA.replace('SID', repr(self.case.sid)))

    def secret(self):
        return {'FAKE_SECRET': SECRET}

    def secret_seen(self):
        return json.loads((self.cwd / 'shell-seen').read_text()) == {'FAKE_SECRET': len(SECRET)}


class OpenCodeBench(Bench):
    provider = 'opencode'
    options = {'worker_id': 31, 'topic': '1:2', 'account_alias': 'one'}

    def __init__(self):
        from tests import test_opencode_runtime
        super().__init__(test_opencode_runtime.OpenCodeRuntimeTests('setUp'))
        self.cwd = self.case.cwd
        self.scenario = {}

    def set(self, **values):
        self.scenario.update(values)
        self.case.scenario(**self.scenario)

    def new_runner(self):
        return ProviderRunner(self.store.directory, opencode_store=self.store)

    def hold(self):
        self.set(release=str(self.case.root / 'release'))

    def release(self):
        (self.case.root / 'release').touch()

    def started(self):
        return any(call['message'].get('method') == 'session/prompt' for call in self.case.calls())

    def quota(self):
        self.set(http_error=429)


class AntigravityBench(Bench):
    provider = 'antigravity'

    def __init__(self):
        from tests import test_antigravity_runtime
        super().__init__(test_antigravity_runtime.AntigravityDefaultTests('setUp'))
        self.cwd = self.case.cwd
        self.options = {'worker_id': self.case.worker, 'topic': '1:2', 'account_alias': 'one'}
        self.scenario = {}

    def set(self, **values):
        self.scenario.update(values)
        self.case.scenario(**self.scenario)

    def new_runner(self):
        return ProviderRunner(self.store.directory, opencode_store=self.store)

    def hold(self):
        self.set(hold=True)

    def release(self):
        (self.case.binary.parent / 'release').touch()

    def started(self):
        return bool(self.case.calls('session/prompt'))

    def quota(self):
        self.set(quota=True)

    def secret(self):
        self.set(env=['FAKE_SECRET'])
        return {'FAKE_SECRET': SECRET}

    def secret_seen(self):
        return self.case.calls('initialize')[0]['env'] == {'FAKE_SECRET': SECRET}

    def worker(self):
        return self.case.worker


BENCHES = {'claude': ClaudeBench, 'codex': CodexBench, 'opencode': OpenCodeBench, 'antigravity': AntigravityBench}


class ProcessTests(unittest.IsolatedAsyncioTestCase):
    """Tier B: each case drives the real ProviderRunner against the provider's existing fake binary."""

    @contextmanager
    def bench(self, provider, capability=None):
        if capability and not getattr(CAPS[provider], capability):
            self.skipTest('%s does not declare %s' % (provider, capability))
        bench = BENCHES[provider]()
        try:
            yield bench
        finally:
            bench.release()
            bench.close()

    def assert_usage(self, spend):
        if spend is None:
            return
        counts = {key: value for key, value in spend.items() if key.endswith('_tokens')}
        self.assertTrue(counts, spend)
        for key, value in counts.items():
            self.assertTrue(isinstance(value, int) and not isinstance(value, bool) and value >= 0, (key, value))

    async def test_a_fresh_run_observes_a_valid_session_and_reports_text_and_usage(self):
        for provider in WORKER_PROVIDERS:
            with self.subTest(provider=provider), self.bench(provider) as bench:
                seen = []
                result = await bench.run(on_session=seen.append)
                self.assertTrue(result.success, result.error)
                self.assertTrue(result.text)
                self.assertTrue(native_ids.valid(provider, result.session_id), result.session_id)
                self.assertEqual(seen, [result.session_id])
                self.assert_usage(result.spend)

    async def test_a_second_run_resumes_the_first_session(self):
        for provider in WORKER_PROVIDERS:
            with self.subTest(provider=provider), self.bench(provider) as bench:
                first = await bench.run()
                again = await bench.run(first.session_id)
                self.assertTrue(again.success, again.error)
                self.assertEqual(again.session_id, first.session_id)

    async def test_a_steer_to_a_running_turn_gets_a_receipt_the_capability_allows(self):
        for provider in WORKER_PROVIDERS:
            with self.subTest(provider=provider), self.bench(provider) as bench:
                bench.hold()
                control = RunControl()
                running = asyncio.create_task(bench.run(control=control))
                try:
                    await until(bench.started, provider + ' prompt')
                    receipt = await settle(control.steer('torii-conformance-1', 'change course'))
                    self.assertIn(receipt, STEER_RECEIPTS[CAPS[provider].steer])
                    bench.release()
                    result = await running
                    self.assertTrue(result.success, result.error)
                finally:
                    await asyncio.gather(running, return_exceptions=True)

    async def test_an_owner_stop_ends_the_run_and_its_process_group(self):
        for provider in WORKER_PROVIDERS:
            with self.subTest(provider=provider), self.bench(provider) as bench:
                bench.hold()
                pool = WorkerPool(bench.store, bench.runner, AccountBroker(bench.store))
                worker = pool.get(bench.worker())
                running = asyncio.create_task(pool.run(worker, bench.store.topic('1:2')))
                try:
                    await until(bench.started, provider + ' prompt')
                    child = HostClient(host_directory(bench.store.directory, worker)).child_pid
                    self.assertIsNotNone(child)
                    await settle(pool.stop(worker['id']))
                    done = await settle(running)
                    self.assertEqual(json.loads(done['result'])['failure_code'], 'owner_stopped')
                    await until(lambda: not _group_alive(child), provider + ' process group exit')
                finally:
                    await asyncio.gather(running, return_exceptions=True)

    async def test_a_quota_ending_is_marked_quota_limited(self):
        for provider in WORKER_PROVIDERS:
            with self.subTest(provider=provider), self.bench(provider) as bench:
                bench.quota()
                result = await bench.run()
                self.assertFalse(result.success)
                self.assertTrue(result.quota_limited, result.failure_code)

    async def test_a_secret_reaches_the_process_and_stays_out_of_the_host_files(self):
        for provider in WORKER_PROVIDERS:
            with self.subTest(provider=provider), self.bench(provider) as bench:
                if not CAPS[provider].secrets:
                    refused = await bench.run(secrets={'FAKE_SECRET': SECRET})
                    self.assertEqual((refused.success, refused.log_path), (False, None))
                    continue
                result = await bench.run(secrets=bench.secret())
                self.assertTrue(result.success, result.error)
                self.assertTrue(bench.secret_seen())
                host = Path(result.log_path).parent
                spec = json.loads((host / 'spec.json').read_text())
                self.assertNotIn(SECRET, ' '.join(spec['argv']))
                for name in ('spec.json', 'events.jsonl'):
                    self.assertNotIn(SECRET, (host / name).read_text(), name)

    async def test_a_new_runner_reattaches_to_a_running_host(self):
        for provider in WORKER_PROVIDERS:
            with self.subTest(provider=provider), self.bench(provider) as bench:
                bench.hold()
                seen = []
                running = asyncio.create_task(bench.run(host_id='conformance-host', on_session=seen.append))
                try:
                    await until(lambda: seen and bench.started(), provider + ' session and prompt')
                finally:
                    running.cancel()
                    await asyncio.gather(running, return_exceptions=True)
                attached = asyncio.create_task(bench.run(seen[0], runner=bench.new_runner(),
                                                         host_id='conformance-host', attach=True))
                try:
                    await asyncio.sleep(0.2)
                    self.assertFalse(attached.done())
                    bench.release()
                    result = await attached
                    self.assertTrue(result.success, result.error)
                    self.assertEqual(result.session_id, seen[0])
                finally:
                    await asyncio.gather(attached, return_exceptions=True)
