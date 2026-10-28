import asyncio
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from http.client import IncompleteRead
from io import BytesIO
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from coordinator import control_api, control_ui, opencode_subscription, opencode_runtime
from coordinator.accounts import AccountBroker
from coordinator.codex_accounts import refresh_codex_accounts
from coordinator.providers import ProviderRunner, RunResult
from coordinator.service import Service
from coordinator.store import Store
from coordinator.workers import WorkerPool
from tests.fake_opencode import SESSION_ID, install_fake_opencode
from tests.support import SETTLE, read_jsonl, settle, stop_test_hosts, until


GENERIC = {'base_url': 'https://api.example.test/v1', 'model': 'example-model', 'small_model': 'other-title-model',
           'context': 128000, 'output': 4096, 'price_in': 1, 'price_cached': 0.1, 'price_out': 2}
USAGE_ERRORS = json.loads((Path(__file__).with_name('fixtures') / 'opencode-subscription-errors.json').read_text())


class OpenCodeSubscriptionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.addCleanup(stop_test_hosts, self.root)
        self.home = self.root / 'home'
        self.home.mkdir()
        for target, value in (('coordinator.providers.platform', 'linux'),
                              ('coordinator.opencode_runtime.managed_preference_paths', ())):
            handle = patch(target, value) if isinstance(value, str) else patch(target, return_value=value)
            handle.start()
            self.addCleanup(handle.stop)
        environment = patch.dict(os.environ, HOME=str(self.home))
        environment.start()
        self.addCleanup(environment.stop)
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        self.assertTrue(self.call('opencode.account', alias='go', provider='opencode-go', model='example-model').ok)
        self.key = self.home / '.opencode-accounts/go/key'
        self.key.write_text('offline-go-placeholder\n')
        self.key.chmod(0o600)
        self.assertTrue(self.call('opencode.use', alias='go').ok)
        self.binary = install_fake_opencode(self.root / 'fake')
        self.assertTrue(self.call('opencode.binary', path=str(self.binary)).ok)
        self.assertTrue(self.call('delegation.opencode', enabled=True).ok)
        self.cwd = self.root / 'project'
        self.cwd.mkdir()
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test',?,1)",
                              (str(self.cwd),))
        self.store.put('coordinator_home_topic', '1:2')
        self.task = self.store.task_create('1:2', 'Build', worktree=str(self.cwd))
        self.runner = ProviderRunner(self.store.directory, opencode_store=self.store)
        self.pool = WorkerPool(self.store, self.runner, AccountBroker(self.store))
        self.service = Service(self.store, None, self.runner, self.root)
        self.service.workers = self.pool
        self.requests = []
        self.request_headers = []
        self.http_status = 200
        self.response_headers = []
        self.payload = self.usage_payload()
        self.http_delay = 0
        self.http_release = None
        self.raw_body = None
        self.request_received = threading.Event()
        test = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                test.requests.append((self.path, self.headers.get('Authorization')))
                test.request_headers.append(self.headers)
                test.request_received.set()
                if test.http_release is not None:
                    test.http_release.wait()
                if test.http_delay:
                    time.sleep(test.http_delay)
                self.send_response(test.http_status)
                for name, value in test.response_headers:
                    self.send_header(name, value)
                self.end_headers()
                body = test.raw_body if test.raw_body is not None else json.dumps(test.payload).encode()
                try:
                    self.wfile.write(body)
                except OSError:
                    pass

            def log_message(self, format, *args):
                pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=self.server.serve_forever)
        thread.start()

        def close_server():
            if self.http_release is not None:
                self.http_release.set()
            self.server.shutdown()
            self.server.server_close()
            thread.join()

        self.addCleanup(close_server)
        url = patch('coordinator.opencode_subscription.USAGE_URL', 'http://127.0.0.1:%d/usage' % self.server.server_port)
        url.start()
        self.addCleanup(url.stop)

    def call(self, operation, **params):
        with self.store.db:
            return control_api.call(self.store, operation, params, topic='1:2' if operation.startswith('workers.') else None)

    def usage_payload(self, limited=False, reset=True):
        when = datetime.fromtimestamp(time.time() + 3600, timezone.utc).isoformat() if reset else None
        return {'usage': {name: {'status': 'rate-limited' if limited else 'ok',
                                 'percent': 100 if limited else percent, 'resetsAt': when}
                          for name, percent in zip(opencode_subscription.WINDOWS, (10, 20, 30))}}

    def scenario(self, **values):
        (self.binary.parent / 'scenario.json').write_text(json.dumps(values))

    def calls(self):
        return read_jsonl(self.binary.parent / 'calls.jsonl')

    def spawn(self):
        result = self.call('workers.spawn', task=self.task['id'], provider='opencode', prompt='Build this')
        self.assertTrue(result.ok, result.text)
        return result.data['worker']

    async def run_worker(self, wid):
        await opencode_subscription.refresh_accounts(self.store)
        return await asyncio.wait_for(self.pool.run(self.pool.get(wid), self.store.topic('1:2')), 20)

    async def resume(self, wid):
        await opencode_subscription.refresh_accounts(self.store)
        await self.service.workers_once()
        running = self.service.worker_tasks.get(wid)
        if running is not None:
            await asyncio.wait_for(running, 20)
        return self.pool.get(wid)

    async def test_t1_subscription_config_references_key_and_existing_key_rules_apply(self):
        result = await self.runner.run('opencode', 'short prompt', self.cwd, None, fresh=True, worker_id=31)
        self.assertTrue(result.success, result)
        config = self.calls()[-1]['config']
        self.assertEqual(config['enabled_providers'], ['opencode-go'])
        self.assertEqual(config['model'], 'opencode-go/example-model')
        self.assertEqual(config['small_model'], config['model'])
        self.assertEqual(config['provider'], {'opencode-go': {'options': {'apiKey': '{file:' + str(self.key) + '}'}}})
        self.assertNotIn(self.key.read_text().strip(), json.dumps(config))
        for mode in (0o644, 0o400, 0o000):
            self.key.chmod(mode)
            result = self.call('delegation.opencode', enabled=True)
            self.assertFalse(result.ok)
            self.assertIn('0600', result.text)
        self.key.chmod(0o600)
        target = self.key.with_name('target')
        self.key.rename(target)
        self.key.symlink_to(target)
        self.assertFalse(self.call('delegation.opencode', enabled=True).ok)

    def test_t2_generic_config_matches_origin_main_golden_bytes(self):
        config = opencode_runtime.write_config(self.root / 'golden', GENERIC, Path('/torii-golden/key'),
                                               '/torii-golden-empty')
        golden = Path(__file__).with_name('fixtures') / 'opencode-generic-config.json'
        self.assertEqual(config.read_bytes(), golden.read_bytes())

    async def test_t3_refresh_path_and_every_account_view_show_three_meters(self):
        self.assertFalse(await opencode_subscription.refresh_accounts(self.store))
        self.assertEqual(self.requests, [('/usage', 'Bearer offline-go-placeholder')])
        views = [self.call('accounts.list').text, self.call('account.show', alias='go').text,
                 control_ui._accounts_view(self.store, 0)[0], control_ui._account_view(self.store, 'go')[0]]
        for text in views:
            for label, left in (('5-hour', 90), ('weekly', 80), ('monthly', 70)):
                self.assertIn('%s: %d%% left; resets ' % (label, left), text)
            self.assertEqual(text.count(' UTC'), 3)
            self.assertNotIn('offline-go-placeholder', text)

    async def test_u1_usage_request_sends_opencode_user_agent_and_existing_headers(self):
        self.assertFalse(await opencode_subscription.refresh_accounts(self.store))
        headers, = self.request_headers
        self.assertEqual(headers['User-Agent'], USAGE_ERRORS['user_agent'])
        self.assertEqual(headers['Authorization'], 'Bearer offline-go-placeholder')
        self.assertEqual(headers['Accept'], 'application/json')

    async def test_u2_edge_host_403_parks_launch_as_usage_unavailable(self):
        saved = USAGE_ERRORS['edge-host']
        self.http_status = saved['status']
        self.raw_body = saved['body'].encode()
        self.response_headers = saved['response_headers']
        self.assertTrue(await opencode_subscription.refresh_accounts(self.store))
        snapshot = self.store.get('opencode_account_status')['go']
        self.assertEqual(snapshot['error'], 'usage_unavailable')
        self.assertEqual(opencode_subscription.launch_state(self.store, 'go'), 'usage_unavailable')
        worker = await self.pool.run(self.pool.get(self.spawn()), self.store.topic('1:2'))
        self.assertEqual(worker['status'], 'waiting_for_quota')
        block = self.store.get('opencode_account_blocks')['go']
        self.assertEqual(block['reason'], 'usage_unavailable')
        self.assertFalse(block['reset_known'])
        self.assertLessEqual(block['until'], time.time() + opencode_subscription.RECHECK_SECONDS)
        notice = self.store.db.execute('SELECT text FROM outbox ORDER BY id DESC LIMIT 1').fetchone()[0]
        self.assertIn('because usage is unavailable. Torii will check again at ', notice)
        self.assertEqual(self.calls(), [])

    async def test_u3_entitlement_403_still_refuses_launch_as_auth_error(self):
        self.http_status = 403
        self.payload = USAGE_ERRORS['entitlement']
        self.assertTrue(await opencode_subscription.refresh_accounts(self.store))
        snapshot = self.store.get('opencode_account_status')['go']
        self.assertEqual(snapshot['error'], 'subscription_required')
        self.assertEqual(opencode_subscription.launch_state(self.store, 'go'), 'subscription_required')
        worker = await self.pool.run(self.pool.get(self.spawn()), self.store.topic('1:2'))
        self.assertEqual(worker['status'], 'done')
        result = json.loads(worker['result'])
        self.assertFalse(result['success'])
        self.assertEqual(result['failure_code'], 'auth')
        self.assertIn(opencode_subscription.ERRORS['subscription_required'], result['error'])
        self.assertFalse(result['quota_limited'])
        self.assertEqual(self.store.get('opencode_account_blocks', {}), {})
        self.assertEqual(self.calls(), [])

    async def test_u4_unexpected_403_is_unavailable_and_401_stays_invalid(self):
        for body in (b'', b'<html>Forbidden</html>', b'null', b'[]', b'{}', b'{"error":null}',
                     b'{"error":"EntitlementError"}', b'{"type":"error","error":{"type":"AuthError"}}',
                     b'{"error":{"type":"EntitlementError"}}', b'[' * 60000):
            for status, expected in ((403, 'usage_unavailable'), (401, 'key_invalid'),
                                     (429, 'usage_unavailable'), (503, 'usage_unavailable')):
                with self.subTest(status=status, body=body[:40]):
                    self.http_status = status
                    self.raw_body = body
                    self.assertTrue(await opencode_subscription.refresh_accounts(self.store))
                    self.assertEqual(self.store.get('opencode_account_status')['go']['error'], expected)

    def test_403_body_read_is_bounded_and_response_is_closed_even_on_read_failure(self):
        for failure in (None, IncompleteRead(b'incomplete'), OSError('read failed')):
            with self.subTest(failure=failure):
                body = BytesIO(b'x' * 70000)
                error = HTTPError(opencode_subscription.USAGE_URL, 403, 'Forbidden', {}, body)
                with patch('urllib.request.OpenerDirector.open', side_effect=error), \
                        patch.object(error, 'read', wraps=body.read, side_effect=failure) as read:
                    self.assertEqual(opencode_subscription.collect_status('go')['error'], 'usage_unavailable')
                read.assert_called_once_with(65537)
                self.assertTrue(body.closed)

    async def test_t3_bad_key_and_subscription_errors_replace_meters_without_remote_text(self):
        await opencode_subscription.refresh_accounts(self.store)
        for status, message in ((401, 'key is invalid'), (403, 'subscription required')):
            self.http_status = status
            self.payload = dict(USAGE_ERRORS['entitlement'], error=dict(USAGE_ERRORS['entitlement']['error'],
                                                                     message=self.key.read_text().strip()))
            self.assertTrue(await opencode_subscription.refresh_accounts(self.store))
            for text in (self.call('accounts.list').text, self.call('account.show', alias='go').text,
                         control_ui._accounts_view(self.store, 0)[0], control_ui._account_view(self.store, 'go')[0]):
                self.assertIn(message, text)
                self.assertNotIn('% left', text)
                self.assertNotIn('offline-go-placeholder', text)
            self.assertNotIn('usage', self.store.get('opencode_account_status')['go'])
            self.assertNotIn('offline-go-placeholder', json.dumps(self.store.get('opencode_account_status')))
            self.assertNotIn('offline-go-placeholder', str(self.store.db.execute('SELECT * FROM problems').fetchall()))

    async def test_t4_retry_after_parks_and_resumes_same_session_and_account(self):
        self.assertTrue(self.call('opencode.account', alias='other', **GENERIC).ok)
        other_key = self.home / '.opencode-accounts/other/key'
        other_key.write_text('other-offline-placeholder')
        other_key.chmod(0o600)
        self.scenario(http_error=429, response_headers={'retry-after': '60'},
                      response_body=json.dumps({'error': {'type': 'GoUsageLimitError', 'metadata': {'limitName': '5 hour'}}}))
        before = time.time()
        wid = self.spawn()
        worker = await self.run_worker(wid)
        self.assertEqual(worker['status'], 'waiting_for_quota')
        self.assertEqual((worker['session'], worker['account_alias'], worker['fresh']), (SESSION_ID, 'go', 0))
        block = self.store.get('opencode_account_blocks')['go']
        self.assertTrue(block['reset_known'])
        self.assertGreaterEqual(block['until'], before + 300)
        self.assertLess(block['until'], time.time() + 301)
        notice = self.store.db.execute('SELECT text FROM outbox ORDER BY id DESC LIMIT 1').fetchone()[0]
        self.assertRegex(notice, r'^Worker %d is paused on OpenCode subscription at [^.]+ UTC\. It resumes on the same account '
                                 r'after a usage check\.$' % wid)
        self.assertEqual(self.call('opencode.use', alias='other').text, 'OpenCode account other selected for new work.')
        count = len(self.calls())
        paused = await self.resume(wid)
        self.assertEqual(paused['status'], 'waiting_for_quota')
        self.assertEqual(json.loads(paused['result'])['error'], 'OpenCode subscription failed: quota_limited.')
        self.assertEqual((await opencode_runtime.OpenCodeBroker(self.store).run(
            None, 'opencode', 'Build this', str(self.cwd), SESSION_ID, account_alias='go')).error,
                         'OpenCode subscription is paused; usage will be checked again.')
        self.assertEqual(len(self.calls()), count)
        self.scenario(history_reply=True)
        with patch('coordinator.opencode_subscription.time.time', return_value=block['until'] + 1):
            worker = await self.resume(wid)
        self.assertEqual(worker['status'], 'done')
        self.assertTrue(json.loads(worker['result'])['success'], worker['result'])
        self.assertEqual((worker['account_alias'], worker['session']), ('go', SESSION_ID))
        self.assertEqual([call['message']['method'] for call in self.calls()],
                         ['initialize', 'session/new', 'session/prompt', 'initialize', 'session/load', 'session/prompt'])
        self.assertTrue(all(call['config']['model'] == 'opencode-go/example-model' for call in self.calls()))
        self.assertNotIn('go', self.store.get('opencode_account_blocks'))

    async def test_credit_refusal_uses_the_existing_subscription_wait_and_resume(self):
        self.scenario(http_error=402, response_headers={'Retry-After': '600'})
        wid = self.spawn()
        waiting = await self.run_worker(wid)
        self.assertEqual((waiting['status'], waiting['session'], waiting['account_alias']),
                         ('waiting_for_quota', SESSION_ID, 'go'))
        result = json.loads(waiting['result'])
        self.assertEqual((result['failure_code'], result['quota_limited']), ('out_of_credit', True))
        block = self.store.get('opencode_account_blocks')['go']
        self.assertTrue(block['reset_known'])
        self.assertGreater(block['until'], time.time() + 590)
        self.scenario(history_reply=True)
        with patch('coordinator.opencode_subscription.time.time', return_value=block['until'] + 1):
            done = await self.resume(wid)
        self.assertEqual((done['status'], done['session'], done['account_alias']), ('done', SESSION_ID, 'go'))
        self.assertTrue(json.loads(done['result'])['success'])
        self.assertEqual([call['message']['method'] for call in self.calls()],
                         ['initialize', 'session/new', 'session/prompt', 'initialize', 'session/load', 'session/prompt'])

    async def test_t5_unknown_reset_is_bounded_rechecked_and_recovers(self):
        self.payload = self.usage_payload(reset=False)
        self.scenario(http_error=429, response_body={'error': {'type': 'GoUsageLimitError'}})
        wid = self.spawn()
        worker = await self.run_worker(wid)
        self.assertEqual(worker['status'], 'waiting_for_quota')
        block = self.store.get('opencode_account_blocks')['go']
        self.assertFalse(block['reset_known'])
        self.assertLessEqual(block['until'], time.time() + 300)
        first = len(self.requests)
        self.http_status = 503
        with patch('coordinator.opencode_subscription.time.time', return_value=block['until'] + 1):
            self.assertEqual((await self.resume(wid))['status'], 'waiting_for_quota')
        self.assertGreater(len(self.requests), first)
        updated = self.store.get('opencode_account_blocks')['go']
        self.assertGreater(updated['until'], block['until'])
        self.assertEqual(updated['until'], block['until'] + 601)
        self.http_status = 200
        self.scenario()
        with patch('coordinator.opencode_subscription.time.time', return_value=updated['until'] + 1):
            worker = await self.resume(wid)
        self.assertEqual(worker['status'], 'done')
        self.assertTrue(json.loads(worker['result'])['success'])
        self.assertEqual(worker['account_alias'], 'go')

    async def test_t4_prelaunch_hold_pins_subscription_alias_and_model_after_active_changes(self):
        self.payload = self.usage_payload(limited=True)
        wid = self.spawn()
        worker = await self.run_worker(wid)
        self.assertEqual((worker['status'], worker['session'], worker['fresh']), ('waiting_for_quota', None, 1))
        self.assertEqual(self.calls(), [])
        self.assertTrue(self.call('opencode.account', alias='other', **GENERIC).ok)
        other_key = self.home / '.opencode-accounts/other/key'
        other_key.write_text('other-offline-placeholder')
        other_key.chmod(0o600)
        self.assertTrue(self.call('opencode.use', alias='other').ok)
        block = self.store.get('opencode_account_blocks')['go']
        self.payload = self.usage_payload()
        with patch('coordinator.opencode_subscription.time.time', return_value=block['until'] + 1):
            worker = await self.resume(wid)
        self.assertEqual(worker['status'], 'done')
        self.assertTrue(json.loads(worker['result'])['success'], worker['result'])
        self.assertEqual(worker['account_alias'], 'go')
        self.assertTrue(all(call['config']['model'] == 'opencode-go/example-model' for call in self.calls()))

    async def test_t5_unknown_reset_rechecks_while_usage_stays_limited(self):
        self.payload = self.usage_payload(limited=True, reset=False)
        wid = self.spawn()
        worker = await self.run_worker(wid)
        self.assertEqual(worker['status'], 'waiting_for_quota')
        block = self.store.get('opencode_account_blocks')['go']
        count = len(self.requests)
        with patch('coordinator.opencode_subscription.time.time', return_value=block['until'] + 1):
            self.assertEqual((await self.resume(wid))['status'], 'waiting_for_quota')
        self.assertGreater(len(self.requests), count)
        again = self.store.get('opencode_account_blocks')['go']
        self.assertGreater(again['until'], block['until'])
        self.assertLessEqual(again['until'], block['until'] + 301)
        self.payload = self.usage_payload()
        with patch('coordinator.opencode_subscription.time.time', return_value=again['until'] - 100):
            await opencode_subscription.refresh_accounts(self.store)
        self.assertEqual((await self.resume(wid))['status'], 'done')

    async def test_stale_usage_cannot_release_a_newer_quota_hold(self):
        await opencode_subscription.refresh_accounts(self.store)
        old = self.store.get('opencode_account_status')['go']
        with self.store.db:
            opencode_subscription.record_limit(self.store, 'go')
        block = self.store.get('opencode_account_blocks')['go']
        with patch('coordinator.opencode_subscription.collect_status', return_value=old):
            await opencode_subscription.refresh_accounts(self.store)
        self.assertEqual(self.store.get('opencode_account_blocks')['go'], block)

    async def test_interrupted_http_usage_is_a_safe_retryable_account_problem(self):
        with patch('urllib.request.OpenerDirector.open', side_effect=IncompleteRead(b'offline-go-placeholder')):
            self.assertTrue(await opencode_subscription.refresh_accounts(self.store))
        snapshot = self.store.get('opencode_account_status')['go']
        self.assertEqual(snapshot['error'], 'usage_unavailable')
        self.assertNotIn('offline-go-placeholder', json.dumps(snapshot))
        await opencode_subscription.refresh_accounts(self.store)
        self.assertIn('usage', self.store.get('opencode_account_status')['go'])

    async def test_generic_fresh_run_still_uses_active_account_instead_of_saved_alias(self):
        self.assertTrue(self.call('opencode.account', alias='first', **GENERIC).ok)
        self.assertTrue(self.call('opencode.account', alias='second', **GENERIC).ok)
        key = self.home / '.opencode-accounts/second/key'
        key.write_text('generic-offline-placeholder')
        key.chmod(0o600)
        self.assertTrue(self.call('opencode.use', alias='second').ok)
        used = []

        async def run(provider, prompt, cwd, session_id, **options):
            used.append(options['account_alias'])
            return RunResult(session_id, success=True)

        result = await self.pool.broker('opencode').run(run, 'opencode', 'test', self.cwd, None,
                                                     fresh=True, account_alias='first')
        self.assertTrue(result.success)
        self.assertEqual(used, ['second'])
        self.assertEqual(self.requests, [])

    async def test_usage_reset_and_documented_subscription_limit_text_without_retry_header(self):
        self.scenario(http_error=429, response_body={'error': {'type': 'GoUsageLimitError'}})
        wid = self.spawn()
        worker = await self.run_worker(wid)
        self.assertEqual(worker['status'], 'waiting_for_quota')
        self.payload = self.usage_payload(limited=True)
        expected = opencode_subscription.reset_time(self.payload['usage']['monthly']['resetsAt'])
        await opencode_subscription.refresh_accounts(self.store)
        self.assertEqual(self.store.get('opencode_account_blocks')['go']['until'], expected)
        for message in ('5-hour usage limit reached.', 'Weekly usage limit reached.', 'Monthly usage limit reached.'):
            self.assertIsNotNone(opencode_subscription.limit_info({'statusCode': 400}, message))

    async def test_t6_off_means_no_usage_no_meter_and_no_launch(self):
        await opencode_subscription.refresh_accounts(self.store)
        self.assertTrue(self.call('delegation.opencode', enabled=False).ok)
        self.requests.clear()
        self.assertFalse(await opencode_subscription.refresh_accounts(self.store))
        self.assertEqual(self.requests, [])
        for text in (self.call('accounts.list').text, self.call('account.show', alias='go').text,
                     control_ui._accounts_view(self.store, 0)[0]):
            self.assertNotIn('% left', text)
            self.assertNotIn('OpenCode subscription', text)
        self.assertFalse(self.call('workers.spawn', task=self.task['id'], provider='opencode', prompt='test').ok)
        result = await self.runner.run('opencode', 'test', self.cwd, None, fresh=True, worker_id=31)
        self.assertFalse(result.success)
        self.assertEqual(self.calls(), [])
        self.assertFalse(await self.pool.broker('opencode').ready('go'))
        self.assertEqual(self.requests, [])

    def test_subscription_account_is_shown_without_its_alias_and_generic_accounts_keep_names(self):
        self.assertTrue(self.call('opencode.account', alias='other', **GENERIC).ok)
        text, rows = control_ui._accounts_view(self.store, 0)
        self.assertEqual([action for row in rows for label, action in row if label == 'OpenCode subscription'],
                         [{'view': 'account:go'}])
        card = control_ui._account_view(self.store, 'go')[0]
        self.assertEqual(card.splitlines()[0], 'OpenCode subscription')
        self.assertIn('\nOpenCode subscription\n', text)
        self.assertNotIn('go (OpenCode subscription)', text)
        self.assertEqual(self.call('opencode.use', alias='go').text, 'OpenCode subscription selected for new work.')
        self.assertEqual(self.call('opencode.account', alias='go', provider='opencode-go', model='example-model').text,
                         'OpenCode subscription account saved.')
        self.assertIn('account OpenCode subscription', control_ui._opencode_line(self.store)[0])
        self.assertEqual(self.call('opencode.use', alias='other').text, 'OpenCode account other selected for new work.')
        self.assertEqual(self.call('opencode.account', alias='other', **GENERIC).text, 'OpenCode account other saved.')
        self.assertIn('account other', control_ui._opencode_line(self.store)[0])

    def test_only_one_subscription_account_and_no_host_fields_or_provider_conversion(self):
        self.assertFalse(self.call('opencode.account', alias='second', provider='opencode-go', model='example-model').ok)
        self.assertFalse(self.call('opencode.account', alias='go', **GENERIC).ok)
        self.assertFalse(self.call('opencode.account', alias='go', provider='opencode-go', **GENERIC).ok)
        self.assertFalse(self.call('opencode.account', alias='go', provider='opencode-go', model='opencode-go/example-model').ok)
        profile = self.store.get('opencode_accounts')['go']
        self.assertEqual(set(profile), {'provider', 'model', 'created'})

    def add_codex_account(self):
        directory = self.root / 'codex-home'
        directory.mkdir(exist_ok=True)
        with self.store.db:
            self.store.put('codex_accounts', {'chatgpt': {'config_dir': str(directory), 'enabled': True}})

    def codex_snapshot(self):
        return {'checked_at': time.time(), 'observed_at': time.time(),
                'identity': {'email': 'offline@example.test', 'logged_in': True},
                'usage': {'seven_day': {'utilization': 30, 'resets_at': time.time() + 3600}}}

    async def test_tb1_slow_subscription_refresh_never_delays_chatgpt_or_changes_retry_schedule(self):
        from types import SimpleNamespace
        from coordinator import codex_accounts
        self.add_codex_account()
        self.http_release = threading.Event()
        self.http_status = 500
        seen = []

        async def collect(account):
            seen.append(account)
            return self.codex_snapshot()

        with patch.object(opencode_subscription, 'HTTP_TIMEOUT', SETTLE):
            refresh = asyncio.create_task(opencode_subscription.refresh_accounts(self.store))
            try:
                await until(self.request_received.is_set, 'the subscription usage request')
                self.assertFalse(await settle(refresh_codex_accounts(self.store, collect)))
                self.assertEqual(len(seen), 1)
                self.assertFalse(refresh.done(), 'ChatGPT refresh waited for subscription usage')
            finally:
                self.http_release.set()
            self.assertTrue(await settle(refresh))
        self.assertEqual(self.store.get('opencode_account_status')['go']['error'], 'usage_unavailable')
        clock = [0]
        checks = []

        async def collect_codex(account, binary):
            checks.append(clock[0])
            return self.codex_snapshot()

        async def tick(delay):
            clock[0] += 10
            if clock[0] > 600:
                raise asyncio.CancelledError

        with patch.object(codex_accounts, 'time', SimpleNamespace(time=time.time, monotonic=lambda: clock[0])), \
                patch.object(codex_accounts, 'asyncio', SimpleNamespace(sleep=tick)), \
                patch.object(codex_accounts, 'collect_codex_status', collect_codex):
            with self.assertRaises(asyncio.CancelledError):
                await codex_accounts.monitor_codex_accounts(self.store)
        self.assertEqual(checks, [0, 300, 600])

    async def test_tb2_workers_once_starts_claude_while_subscription_usage_times_out(self):
        self.payload = self.usage_payload(limited=True, reset=False)
        wid = self.spawn()
        self.assertEqual((await self.run_worker(wid))['status'], 'waiting_for_quota')
        blocks = self.store.get('opencode_account_blocks')
        blocks['go']['until'] = time.time() - 1
        self.store.put('opencode_account_blocks', blocks)
        self.store.put('accounts', {'claude': {'config_dir': str(self.root / 'claude-home'), 'enabled': True}})
        other = self.call('workers.spawn', task=self.task['id'], provider='claude', prompt='Inspect independently')
        self.assertTrue(other.ok, other.text)
        started_workers = []

        async def run(worker, topic):
            started_workers.append(worker['id'])

        self.http_release = threading.Event()
        self.http_status = 500
        self.request_received.clear()
        with patch.object(opencode_subscription, 'HTTP_TIMEOUT', SETTLE), patch.object(self.pool, 'run', run):
            refresh = asyncio.create_task(opencode_subscription.refresh_accounts(self.store))
            try:
                await until(self.request_received.is_set, 'the subscription usage request')
                self.assertTrue(await settle(self.service.workers_once()))
                await settle(asyncio.gather(*self.service.worker_tasks.values()))
                self.assertEqual(started_workers, [other.data['worker']])
                self.assertEqual(self.pool.get(wid)['status'], 'waiting_for_quota')
                self.assertFalse(refresh.done(), 'Claude waited for subscription usage')
            finally:
                self.http_release.set()
            self.assertTrue(await settle(refresh))

    async def test_tf2_prelaunch_auth_fails_and_unavailable_parks_without_key(self):
        for status, expected in ((401, 'key'), (403, 'subscription'), (500, 'usage')):
            with self.subTest(status=status):
                self.http_status = status
                self.payload = dict(USAGE_ERRORS['entitlement'], error=dict(USAGE_ERRORS['entitlement']['error'],
                                                                         message='offline-go-placeholder'))
                await opencode_subscription.refresh_accounts(self.store)
                worker_id = self.call('workers.spawn', task=self.task['id'], provider='opencode',
                                      prompt='Check status %d' % status).data['worker']
                requests = len(self.requests)
                worker = await self.pool.run(self.pool.get(worker_id), self.store.topic('1:2'))
                result = json.loads(worker['result'])
                self.assertEqual(len(self.requests), requests)
                self.assertNotIn('offline-go-placeholder', worker['result'])
                if status in (401, 403):
                    self.assertEqual(worker['status'], 'done')
                    self.assertEqual(result['failure_code'], 'auth')
                    self.assertIn(expected, result['error'].lower())
                    self.assertEqual(result['error'], 'OpenCode subscription failed: ' + opencode_subscription.ERRORS[
                        'key_invalid' if status == 401 else 'subscription_required'])
                    self.assertFalse(result['quota_limited'])
                else:
                    self.assertEqual(worker['status'], 'waiting_for_quota')
                    self.assertLessEqual(self.store.get('opencode_account_blocks')['go']['until'], time.time() + 300)
                    notice = self.store.db.execute('SELECT text FROM outbox ORDER BY id DESC LIMIT 1').fetchone()[0]
                    self.assertIn('usage', notice.lower())
                    self.assertIn('unavailable', notice.lower())
                    self.assertNotIn('when an account becomes available', notice)
                    self.assertRegex(notice, r'^Worker %d is paused on OpenCode subscription because usage is unavailable\. '
                                             r'Torii will check again at [^.]+ UTC\.$' % worker_id)
        self.http_status = 200
        self.http_delay = 0.4
        with patch.object(opencode_subscription, 'HTTP_TIMEOUT', 0.1):
            self.assertTrue(await opencode_subscription.refresh_accounts(self.store))
        result = self.call('workers.spawn', task=self.task['id'], provider='opencode', prompt='Check timeout')
        worker = await self.pool.run(self.pool.get(result.data['worker']), self.store.topic('1:2'))
        self.assertEqual(worker['status'], 'waiting_for_quota')
        notice = self.store.db.execute('SELECT text FROM outbox ORDER BY id DESC LIMIT 1').fetchone()[0]
        self.assertIn('usage', notice.lower())
        self.assertIn('unavailable', notice.lower())
        self.assertEqual(self.calls(), [])

    async def test_tf4_deep_or_unreadable_usage_never_escapes_or_breaks_chatgpt(self):
        self.add_codex_account()
        for raw in (b'[' * 60000, b'null', b'{}', b'{"usage":[]}', b'not json'):
            with self.subTest(body=raw[:20]):
                self.raw_body = raw
                self.assertTrue(await opencode_subscription.refresh_accounts(self.store))
                self.assertEqual(self.store.get('opencode_account_status')['go']['error'], 'usage_unavailable')
                self.assertIn('usage is unavailable', self.call('account.show', alias='go').text)

                async def collect(account):
                    return self.codex_snapshot()

                self.assertFalse(await refresh_codex_accounts(self.store, collect))
                self.assertIn('usage', self.store.get('codex_account_status')['chatgpt'])

    async def test_tf5_huge_retry_and_resets_are_capped_and_accounts_render(self):
        self.scenario(http_error=429, response_headers={'retry-after': '1000000000000'})
        before = time.time()
        worker = await self.run_worker(self.spawn())
        self.assertEqual(worker['status'], 'waiting_for_quota')
        block = self.store.get('opencode_account_blocks')['go']
        self.assertLessEqual(block['until'], time.time() + 32 * 86400)
        self.assertGreaterEqual(block['until'], before + 32 * 86400)
        self.assertIn('Paused until', control_ui._accounts_view(self.store, 0)[0])
        self.assertLessEqual(opencode_subscription.retry_at({'responseHeaders': {
            'retry-after': 'Fri, 01 Jan 9999 00:00:00 GMT'}}), time.time() + 32 * 86400)
        self.payload = self.usage_payload(limited=True)
        for window in self.payload['usage'].values():
            window['resetsAt'] = '9999-01-01T00:00:00Z'
        await opencode_subscription.refresh_accounts(self.store)
        for window in self.store.get('opencode_account_status')['go']['usage'].values():
            self.assertLessEqual(window['resets_at'], time.time() + 32 * 86400)
        self.assertIn('Paused until', control_ui._accounts_view(self.store, 0)[0])

    def test_tf7_oversized_key_is_rejected_at_setup(self):
        from coordinator.opencode_accounts import check_key
        self.key.write_bytes(b'x' * 16385)
        with self.assertRaises(control_api.Refused):
            check_key('go')
        self.assertFalse(self.call('delegation.opencode', enabled=True).ok)

    async def test_ready_never_fetches_and_waits_for_reading_taken_after_reset(self):
        await opencode_subscription.refresh_accounts(self.store)
        now = time.time()
        self.store.put('opencode_account_blocks', {'go': {
            'until': now - 1, 'observed_at': now - 60, 'reason': 'quota', 'reset_known': True}})
        count = len(self.requests)
        self.assertFalse(await self.pool.broker('opencode').ready('go'))
        self.assertEqual(len(self.requests), count)
        old = dict(self.store.get('opencode_account_status')['go'], observed_at=now - 2)
        with patch.object(opencode_subscription, 'collect_status', return_value=old):
            await opencode_subscription.refresh_accounts(self.store)
        self.assertFalse(await self.pool.broker('opencode').ready('go'))
        await opencode_subscription.refresh_accounts(self.store)
        self.assertTrue(await self.pool.broker('opencode').ready('go'))

    async def test_subscription_monitor_schedule_owner_request_due_hold_and_gates(self):
        from types import SimpleNamespace
        clock = [0]
        ticks = [0]
        counts = []
        profile = self.store.get('opencode_accounts')['go']

        async def tick(delay):
            counts.append(len(self.requests))
            ticks[0] += 1
            clock[0] += 10
            if ticks[0] == 1:
                self.store.put('opencode_account_status_refresh_requested', time.time() + 1)
            elif ticks[0] == 3:
                self.store.put('opencode_account_blocks', {'go': {
                    'until': time.time() - 1, 'observed_at': time.time() - 60,
                    'reason': 'quota', 'reset_known': True}})
            elif ticks[0] == 4:
                self.store.put('opencode_enabled', False)
                clock[0] += 300
            elif ticks[0] == 5:
                self.store.put('opencode_enabled', True)
                self.store.put('opencode_accounts', {})
            elif ticks[0] == 6:
                self.store.put('opencode_accounts', {'go': profile})
            elif ticks[0] == 7:
                clock[0] += 300
            elif ticks[0] == 8:
                raise asyncio.CancelledError

        self.assertIn('opencode_accounts', dict(self.service.supervised()))
        with patch.object(opencode_subscription, 'time', SimpleNamespace(time=time.time, monotonic=lambda: clock[0])),\
                patch.object(opencode_subscription, 'asyncio', SimpleNamespace(sleep=tick, to_thread=asyncio.to_thread)):
            with self.assertRaises(asyncio.CancelledError):
                await opencode_subscription.monitor_accounts(self.store)
        self.assertEqual(counts, [1, 2, 2, 3, 3, 3, 4, 5])

    async def test_owner_check_requests_subscription_refresh_without_waiting_for_it(self):
        from coordinator.setup_flow import check_setup_accounts
        from unittest.mock import AsyncMock
        with patch('coordinator.account_status.check_accounts', new_callable=AsyncMock), \
                patch('coordinator.codex_accounts.refresh_codex_accounts', new_callable=AsyncMock), \
                patch.object(opencode_subscription, 'collect_status', side_effect=AssertionError('No inline subscription HTTP')):
            await check_setup_accounts(self.store, time.time())
        self.assertGreater(self.store.get('opencode_account_status_refresh_requested', 0), 0)

    async def test_unchecked_subscription_worker_parks_without_http_then_monitor_reading_releases_it(self):
        wid = self.spawn()
        with patch.object(opencode_subscription, 'collect_status', side_effect=AssertionError('No launch HTTP')):
            worker = await self.pool.run(self.pool.get(wid), self.store.topic('1:2'))
        self.assertEqual(worker['status'], 'waiting_for_quota')
        self.assertEqual(self.requests, [])
        self.assertEqual(self.calls(), [])
        self.assertGreater(self.store.get('opencode_account_status_refresh_requested', 0), 0)
        block = self.store.get('opencode_account_blocks')['go']
        self.assertLessEqual(block['until'], time.time() + 300)
        await opencode_subscription.refresh_accounts(self.store)
        with patch.object(opencode_subscription, 'collect_status', side_effect=AssertionError('No scheduling HTTP')):
            await self.service.workers_once()
            await asyncio.gather(*self.service.worker_tasks.values())
        self.assertEqual(self.pool.get(wid)['status'], 'done')
        self.assertTrue(json.loads(self.pool.get(wid)['result'])['success'])

    async def test_parked_worker_fails_on_new_auth_reading(self):
        self.http_status = 500
        wid = self.spawn()
        self.assertEqual((await self.run_worker(wid))['status'], 'waiting_for_quota')
        self.http_status = 401
        await opencode_subscription.refresh_accounts(self.store)
        with patch.object(opencode_subscription, 'collect_status', side_effect=AssertionError('No scheduling HTTP')):
            await self.service.workers_once()
            await asyncio.gather(*self.service.worker_tasks.values())
        worker = self.pool.get(wid)
        self.assertEqual(worker['status'], 'done')
        self.assertEqual(json.loads(worker['result'])['failure_code'], 'auth')
        self.assertEqual(self.calls(), [])
