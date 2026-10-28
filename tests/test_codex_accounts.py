"""Several Codex (ChatGPT) accounts: one CODEX_HOME per login, shared threads, rotation at 95% or a rejection."""
import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from coordinator import control_api, mcp
from coordinator.accounts import AccountBroker, LIMIT_CONTINUATION
from coordinator.codex_accounts import (RESET_CREDIT_SCHEMA, CodexBroker, collect_codex_status,
                                        discover_codex_accounts, link_home,
                                        refresh_codex_accounts)
from coordinator.providers import ProviderRunner, RunResult
from coordinator.signin import KEY, SignIns, _started
from coordinator.store import Store
from coordinator.workers import WorkerPool
from tests.support import read_jsonl, until
from tests.test_store import update


TOPIC = '-10042:4'
OWNER = 7
DEVICE_URL = 'https://auth.openai.com/codex/device'
DEVICE_CODE = 'ABCD-12345'
FAKE_CODEX = '''#!/usr/bin/env python3
import json, os, sys, threading, time, uuid
from pathlib import Path
here = Path(__file__).resolve().parent
home = Path(os.environ['CODEX_HOME'])
lock = threading.Lock()
state = {'cancelled': False}
redeemed = False
redeemed_credit = None


def emit(value):
    with lock:
        print(json.dumps(value), flush=True)


def log(method, params):
    with lock, (here / 'calls.jsonl').open('a') as calls:
        calls.write(json.dumps({'method': method, 'params': params, 'home': str(home), 'argv': sys.argv[1:],
                                'sqlite': os.environ.get('CODEX_SQLITE_HOME'),
                                'credentials': [name for name in ('CODEX_API_KEY', 'CODEX_ACCESS_TOKEN')
                                                if name in os.environ]}) + '\\n')


def config():
    return json.loads((here / 'accounts.json').read_text()).get(home.name, {})


def limits(used):
    weekly = {'usedPercent': used, 'windowDurationMins': 10080,
              'resetsAt': config().get('reset_at', int(time.time()) + 86400)}
    windows = ({'primary': {'usedPercent': 0 if redeemed else config()['five_hour'], 'windowDurationMins': 300,
                            'resetsAt': int(time.time()) + 3600}, 'secondary': weekly}
               if 'five_hour' in config() else {'primary': weekly, 'secondary': None})
    return dict(windows, limitId='codex', limitName=None, planType='pro',
                credits=config().get('credits', {'hasCredits': True, 'unlimited': False, 'balance': '12.5'}))


def reset_details():
    credits = config().get('reset_details', [])
    if not redeemed or not isinstance(credits, list):
        return credits
    return [dict(credit, status='redeemed') if credit['id'] == redeemed_credit else credit for credit in credits]


def approve():
    while not (here / 'approve').exists():
        if state['cancelled']:
            return
        time.sleep(0.01)
    (home / 'auth.json').write_text('{}')
    emit({'method': 'account/login/completed', 'params': {'loginId': 'login-1', 'success': True, 'error': None}})


for line in sys.stdin:
    message = json.loads(line)
    if 'id' not in message:
        continue
    method, params = message.get('method'), message.get('params') or {}
    log(method, params)
    signed_in = (home / 'auth.json').exists()
    reply = {}
    if method == 'initialize':
        (home / 'installation_id').write_text('install')
        (home / 'tmp').mkdir(exist_ok=True)
        if (here / 'slow').exists():
            time.sleep(0.5)
    elif method == 'account/read' and config().get('type', 'chatgpt') != 'chatgpt':
        reply = {'account': {'type': config()['type']} if signed_in else None, 'requiresOpenaiAuth': True}
    elif method == 'account/read':
        reply = {'account': {'type': 'chatgpt', 'email': config().get('email'), 'planType': 'pro'} if signed_in else None,
                 'requiresOpenaiAuth': True}
    elif method == 'account/rateLimits/read' and not signed_in:
        emit({'id': message['id'], 'error': {'code': -32600,
                                             'message': 'codex account authentication required to read rate limits'}})
        continue
    elif method == 'account/rateLimits/read':
        reply = {'ordinaryUsageAllowed': config().get('ordinary_allowed', True),
                 'rateLimits': limits(0 if redeemed else config().get('used', 10)),
                 'rateLimitsByLimitId': {}, 'accountId': 'acct',
                 'rateLimitResetCredits': {'availableCount': config().get('reset_credits', 0) - int(redeemed),
                                           'credits': reset_details()}}
    elif method == 'account/rateLimitResetCredit/consume':
        if config().get('reset_outcome', 'reset') != 'reset':
            reply = {'outcome': config()['reset_outcome']}
        elif config().get('reset_credits', 0) > 0 and not redeemed:
            redeemed = True
            redeemed_credit = params.get('creditId', config().get('backend_credit_id'))
            reply = {'outcome': 'reset'}
        else:
            reply = {'outcome': 'noCredit'}
    elif method == 'account/login/start':
        reply = {'type': 'chatgptDeviceCode', 'loginId': 'login-1', 'verificationUrl': '%s', 'userCode': '%s'}
    elif method == 'account/login/cancel':
        state['cancelled'] = True
        reply = {'status': 'canceled'}
    elif method == 'thread/start':
        reply = {'thread': {'id': str(uuid.uuid4())}}
    elif method == 'thread/resume':
        reply = {'thread': {'id': params['threadId']}}
    elif method == 'turn/start':
        reply = {'turn': {'id': 'turn-1'}}
    emit({'id': message['id'], 'result': reply})
    if method == 'account/login/start':
        threading.Thread(target=approve, daemon=True).start()
    elif method == 'account/login/cancel':
        emit({'method': 'account/login/completed',
              'params': {'loginId': 'login-1', 'success': False, 'error': 'Login was not completed'}})
    elif method == 'turn/start':
        thread = params['threadId']
        emit({'method': 'turn/started', 'params': {'threadId': thread, 'turn': {'id': 'turn-1'}}})
        emit({'method': 'account/rateLimits/updated',
              'params': {'rateLimits': limits(config().get('reject_used', 100) if config().get('reject') else config().get('used', 10))}})
        if config().get('reject'):
            turn = {'id': 'turn-1', 'status': 'failed',
                    'error': {'message': config().get('reject_message', 'You hit your usage limit.'),
                              'codexErrorInfo': 'usageLimitExceeded'}}
        else:
            emit({'method': 'item/completed', 'params': {'threadId': thread, 'turnId': 'turn-1',
                                                         'item': {'type': 'agentMessage', 'text': 'done on ' + home.name}}})
            turn = {'id': 'turn-1', 'status': 'completed'}
        emit({'method': 'turn/completed', 'params': {'threadId': thread, 'turn': turn}})
''' % (DEVICE_URL, DEVICE_CODE)


class CodexFixture(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.store = Store(self.root / 'state')
        self.default = self.root / 'codex-home'
        self.default.mkdir()
        (self.default / 'config.toml').write_text('model = "gpt-6-sol"\n')
        (self.default / 'auth.json').write_text('{}')
        (self.default / 'sessions').mkdir()
        self.added = self.root / 'codex-accounts'
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        self.binary = self.bin / 'codex'
        self.binary.write_text(FAKE_CODEX)
        self.binary.chmod(0o755)
        self.behave({})
        environment = patch.dict(os.environ, {'CODEX_HOME': str(self.default)})
        environment.start()
        self.addCleanup(environment.stop)
        root_patch = patch('coordinator.codex_accounts.accounts_root', return_value=self.added)
        root_patch.start()
        self.addCleanup(root_patch.stop)
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,-10042,4,'Torii',?,1)''', (TOPIC, str(self.root)))
            self.store.put('coordinator_home_topic', TOPIC)
            self.store.put('owner', OWNER)
            self.store.put('group', -10042)
            self.store.put('bot_username', 'torii_test_bot')

    async def asyncTearDown(self):
        self.store.close()
        self.temp.cleanup()

    def behave(self, accounts):
        (self.bin / 'accounts.json').write_text(json.dumps(accounts))

    def reset_details(self):
        now = int(time.time())
        return [{'id': 'late', 'status': 'available', 'expiresAt': now + 20 * 86400},
                {'id': 'early', 'status': 'available', 'expiresAt': now + 5 * 86400},
                {'id': 'never', 'status': 'available', 'expiresAt': None},
                {'id': 'spent', 'status': 'redeemed', 'expiresAt': now + 86400}]

    def calls(self):
        path = self.bin / 'calls.jsonl'
        return read_jsonl(path)

    def tap(self, label, data=None):
        row = self.store.db.execute('SELECT * FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
        if data is None:
            buttons = json.loads(row['reply_markup'])['inline_keyboard']
            data = next(button['callback_data'] for line in buttons for button in line if button['text'] == label)
        self.number = getattr(self, 'number', 350) + 1
        return self.store.accept({'update_id': self.number, 'callback_query': {
            'id': str(self.number), 'from': {'id': OWNER}, 'data': data,
            'message': {'message_id': 1000 + row['id'], 'message_thread_id': 4,
                        'chat': {'id': -10042, 'type': 'supergroup'}}}})

    def reset_card(self, alias, credits):
        status = self.store.get('codex_account_status')
        status[alias]['resets_available'] = credits
        with self.store.db:
            self.store.put('codex_account_status', status)
        self.store.accept(update(350, '/accounts'))
        self.assertEqual(self.tap('Reset ' + alias + '@example.com (' + str(credits) + ')'), 'control_callback')
        row = self.store.db.execute('SELECT * FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
        self.assertIn('Spend 1 of ' + str(credits) + ' banked reset', row['text'])
        self.assertEqual(self.store.service_requests_pending(), [])
        self.assertFalse(any(call['method'] == 'account/rateLimitResetCredit/consume' for call in self.calls()))
        buttons = json.loads(row['reply_markup'])['inline_keyboard']
        self.assertEqual([button['text'] for line in buttons for button in line], ['Spend reset', 'Back'])
        return buttons[0][0]['callback_data']

    def home(self, name, signed_in=True):
        """An added account home, signed in unless told otherwise."""
        path = self.added / name
        path.mkdir(parents=True)
        if signed_in:
            (path / 'auth.json').write_text('{}')
        return path

    def register(self, accounts, now=None):
        """Registered Codex accounts: alias -> (home, weekly used percent, weekly reset in hours)."""
        now = now or time.time()
        with self.store.db:
            self.store.put('codex_accounts', {alias: {'config_dir': str(home), 'enabled': True}
                                              for alias, (home, _, _) in accounts.items()})
            self.store.put('codex_account_status', {alias: {
                'identity': {'email': alias + '@example.com', 'logged_in': True}, 'observed_at': now,
                'usage': {'seven_day': {'utilization': used, 'resets_at': now + hours * 3600}}}
                for alias, (_, used, hours) in accounts.items()})

    def automatic(self, enabled=True):
        with self.store.db:
            self.store.put('codex_auto_switch', enabled)

    def use(self, alias):
        with self.store.db:
            return control_api.call(self.store, 'account.use', {'alias': alias}, source='mcp')

    def notices(self):
        return [row[0] for row in self.store.db.execute('SELECT text FROM outbox ORDER BY id')
                if row[0].startswith('Codex account ')]

    def callbacks(self):
        return [row[0] for row in self.store.db.execute("SELECT text FROM messages WHERE kind='callback' ORDER BY id")]

    def separate_home_topic(self):
        home = '-10042:5'
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,-10042,5,'Home',?,1)",
                                  (home, str(self.root)))
            self.store.put('coordinator_home_topic', home)
        return home

    def turns(self):
        return [(Path(call['home']).name, call['method'], call['params']) for call in self.calls()
                if call['method'] in ('thread/start', 'thread/resume', 'turn/start')]

    def worker(self):
        task = self.store.task_create(TOPIC, 'Build', worktree=str(self.root))
        with self.store.db:
            row = self.store.db.execute('''INSERT INTO workers (task,topic,provider,prompt,cwd,workspace,created,updated)
                VALUES (?,?,'codex','build this',?,?,1,1)''', (task['id'], TOPIC, str(self.root),
                                                               json.dumps({'cwd': str(self.root)})))
        return dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (row.lastrowid,)).fetchone())

    async def run_worker(self, worker=None):
        pool = WorkerPool(self.store, ProviderRunner(self.store.directory, binaries={'codex': str(self.binary)}),
                          AccountBroker(self.store), codex_accounts=CodexBroker(self.store))
        finished = await pool.run(worker or self.worker(), self.store.topic(TOPIC))
        return finished, json.loads(finished['result'])


class CodexHomeTests(CodexFixture):
    async def test_an_added_home_links_only_the_shared_entries_of_the_default_home(self):
        for name in ('AGENTS.md', 'history.jsonl', 'installation_id', 'models_cache.json', 'state_5.sqlite',
                     'state_5.sqlite-wal', 'unknown.json'):
            (self.default / name).write_text(name)
        for name in ('skills', 'log', 'plugins'):
            (self.default / name).mkdir()
        home = self.home('second', signed_in=False)
        (home / 'AGENTS.md').write_text('kept')
        link_home(home)
        link_home(home)
        linked = sorted(path.name for path in home.iterdir() if path.is_symlink())
        self.assertEqual(linked, ['archived_sessions', 'config.toml', 'history.jsonl', 'sessions', 'skills',
                                  'thread-writer-locks'])
        for name in linked:
            self.assertEqual((home / name).resolve(), (self.default / name).resolve())
        self.assertEqual((home / 'AGENTS.md').read_text(), 'kept')
        self.assertFalse((home / 'auth.json').exists())
        self.assertTrue((self.default / 'archived_sessions').is_dir())
        self.assertTrue((self.default / 'thread-writer-locks').is_dir())
        link_home(self.default)
        self.assertFalse(any(path.is_symlink() for path in self.default.iterdir()))
        self.assertEqual((self.default / 'auth.json').read_text(), '{}')

    async def test_each_codex_environment_names_its_home_shares_state_and_drops_inherited_codex_credentials(self):
        second = self.home('second')
        self.register({'codex': (self.default, 10, 24), 'codex-second': (second, 10, 24)})
        leaked = {'CODEX_API_KEY': 'x', 'CODEX_ACCESS_TOKEN': 'x', 'CODEX_SQLITE_HOME': '/elsewhere',
                  'TELEGRAM_BOT_TOKEN': 'x', 'TORII_VAULT_KEY': 'x'}
        with patch.dict(os.environ, leaked):
            first, other = (CodexBroker(self.store).environment(alias) for alias in ('codex', 'codex-second'))
        self.assertEqual((first['CODEX_HOME'], other['CODEX_HOME']), (str(self.default), str(second)))
        self.assertEqual({first['CODEX_SQLITE_HOME'], other['CODEX_SQLITE_HOME']}, {str(self.default)})
        for env in (first, other):
            self.assertEqual([name for name in leaked if name in env], ['CODEX_SQLITE_HOME'])
        self.assertEqual((second / 'sessions').resolve(), (self.default / 'sessions').resolve())


class CodexBlockRecoveryTests(CodexFixture):
    def limited_account(self, reason):
        now = time.time()
        self.register({'codex': (self.default, 1, 4 * 24)}, now=now)
        self.automatic()
        block = {'until': now + 4 * 86400, 'reason': reason, 'observed_at': now - 1}
        with self.store.db:
            self.store.put('codex_active_account', 'codex')
            self.store.put('codex_account_blocks', {'codex': block})
        return CodexBroker(self.store), block

    def assert_recovered(self, broker):
        self.assertEqual(broker.account_state('codex'), {'state': 'available'})
        self.assertFalse(broker.exhausted('codex'))
        self.assertEqual(broker.select(), 'codex')
        self.assertNotIn('codex', self.store.get('codex_account_blocks'))
        self.assertEqual(self.store.get('codex_active_account'), 'codex')
        self.assertIs(self.store.get('codex_auto_switch'), True)

    def assert_limited(self, broker, block):
        self.assertEqual(broker.account_state('codex'), {'state': 'limited', 'until': block['until']})
        self.assertTrue(broker.exhausted('codex'))
        self.assertIsNone(broker.select())
        self.assertEqual(self.store.get('codex_account_blocks'), {'codex': block})

    async def test_newer_low_usage_keeps_a_timed_quota_block(self):
        broker, block = self.limited_account('quota')
        self.assert_limited(broker, block)

    async def test_newer_low_usage_clears_a_timed_threshold_block(self):
        broker, _ = self.limited_account('threshold')
        self.assert_recovered(broker)

    async def test_refresh_recovers_the_account_after_an_automatic_95_percent_hold(self):
        broker, _ = self.limited_account('threshold')
        with self.store.db:
            broker.record_rate_limit('codex', {'primary': {
                'usedPercent': 95, 'windowDurationMins': 10080, 'resetsAt': time.time() + 4 * 86400}})
        block = self.store.get('codex_account_blocks')['codex']
        self.assertEqual(block['reason'], 'threshold')
        self.assert_limited(broker, block)

        async def collect(account):
            return {'observed_at': block['observed_at'] + 1, 'usage_allowed': True,
                    'usage': {'seven_day': {'utilization': 1, 'resets_at': block['until'] + 3 * 86400}}}

        self.assertFalse(await refresh_codex_accounts(self.store, collect))
        self.assert_recovered(broker)

    async def test_recovered_card_shows_99_percent_left_and_available(self):
        from coordinator.controls import _codex_card
        self.limited_account('threshold')
        card = _codex_card(self.store, 'codex', active=True)
        self.assertIn('99%', card)
        self.assertIn('Account state: available.', card)
        self.assertNotIn('Account state: limited', card)

    async def test_timed_quota_block_keeps_manual_codex_selection_limited(self):
        broker, block = self.limited_account('quota')
        self.automatic(False)
        self.assert_limited(broker, block)
        self.assertIsNone(broker.parent_account())
        self.assertEqual(self.store.get('codex_active_account'), 'codex')
        self.assertIs(self.store.get('codex_auto_switch'), False)

    async def test_older_equal_or_missing_snapshot_does_not_clear_a_block(self):
        for reason in ('quota', 'threshold'):
            for delta in (-1, 0, None):
                with self.subTest(reason=reason, delta=delta):
                    broker, block = self.limited_account(reason)
                    status = self.store.get('codex_account_status')
                    status['codex']['observed_at'] = block['observed_at'] + delta if delta is not None else None
                    with self.store.db:
                        self.store.put('codex_account_status', status)
                    self.assert_limited(broker, block)

    async def test_only_undated_legacy_blocks_clear_without_a_block_timestamp(self):
        for reason in ('quota', 'threshold'):
            for timed in (False, True):
                for missing in (False, True):
                    with self.subTest(reason=reason, timed=timed, missing=missing):
                        broker, block = self.limited_account(reason)
                        if missing:
                            block.pop('observed_at')
                        else:
                            block['observed_at'] = None
                        if not timed:
                            block['until'] = None
                        with self.store.db:
                            self.store.put('codex_account_blocks', {'codex': block})
                        if timed:
                            self.assert_limited(broker, block)
                        else:
                            self.assert_recovered(broker)

    async def test_newer_full_window_does_not_clear_a_block(self):
        for reason in ('quota', 'threshold'):
            for meter in ('five_hour', 'seven_day'):
                for used in (95, 100):
                    with self.subTest(reason=reason, meter=meter, used=used):
                        broker, block = self.limited_account(reason)
                        status = self.store.get('codex_account_status')
                        status['codex']['usage'][meter] = {'utilization': used, 'resets_at': block['until']}
                        with self.store.db:
                            self.store.put('codex_account_status', status)
                        self.assert_limited(broker, block)

    async def test_newer_error_or_denied_usage_does_not_clear_a_block(self):
        for reason in ('quota', 'threshold'):
            for until in (None, time.time() + 4 * 86400):
                for invalid in ({'error': 'timeout'}, {'usage_allowed': False}):
                    with self.subTest(reason=reason, until=until, invalid=invalid):
                        broker, block = self.limited_account(reason)
                        block['until'] = until
                        status = self.store.get('codex_account_status')
                        status['codex'].update(invalid)
                        with self.store.db:
                            self.store.put('codex_account_blocks', {'codex': block})
                            self.store.put('codex_account_status', status)
                        self.assert_limited(broker, block)

    async def test_newer_usage_below_95_clears_only_threshold_or_undated_blocks(self):
        for reason in ('quota', 'threshold'):
            for until in (None, time.time() + 4 * 86400):
                with self.subTest(reason=reason, until=until):
                    broker, block = self.limited_account(reason)
                    block['until'] = until
                    status = self.store.get('codex_account_status')
                    status['codex']['usage'] = {meter: {'utilization': 94, 'resets_at': time.time() + 3600}
                                               for meter in ('five_hour', 'seven_day')}
                    status['codex']['usage_allowed'] = True
                    with self.store.db:
                        self.store.put('codex_account_blocks', {'codex': block})
                        self.store.put('codex_account_status', status)
                    if reason == 'quota' and until is not None:
                        self.assert_limited(broker, block)
                    else:
                        self.assert_recovered(broker)

    async def test_a_rejection_after_a_clean_snapshot_blocks_again(self):
        broker, _ = self.limited_account('quota')
        with self.store.db:
            self.store.put('codex_account_blocks', {})
        observed = broker.snapshot('codex')['observed_at']
        with patch('coordinator.codex_accounts.time.time', return_value=observed + 1), self.store.db:
            broker.record_rate_limit('codex', None, rejected=True)
        block = self.store.get('codex_account_blocks')['codex']
        self.assertEqual(block['reason'], 'quota')
        self.assertGreater(block['observed_at'], observed)
        self.assert_limited(broker, block)


class CodexRejectionRecoveryTests(CodexFixture):
    def model_meter(self):
        return {'limitId': 'codex_model_x', 'primary': {
            'usedPercent': 100, 'windowDurationMins': 10080, 'resetsAt': time.time() + 3 * 86400}}

    async def clean_check(self, account):
        await asyncio.sleep(0.01)
        return {'checked_at': time.time(), 'observed_at': time.time(), 'usage_allowed': True,
                'usage': {'seven_day': {'utilization': 10, 'resets_at': time.time() + 5 * 86400}}}

    async def rejection_cycles(self, limits):
        broker = CodexBroker(self.store)
        relaunches = 0
        for _ in range(5):
            with self.store.db:
                broker.record_rate_limit('codex', limits, rejected=True)
            self.assertIsNone(broker.select())
            self.assertGreater(self.store.get('codex_account_status_refresh_requested', 0), 0)
            await refresh_codex_accounts(self.store, self.clean_check)
            if broker.select() == 'codex':
                relaunches += 1
        return relaunches

    async def test_model_meter_rejection_holds_the_active_account(self):
        self.register({'codex': (self.default, 10, 5 * 24)})
        self.automatic(False)
        self.assertEqual(await self.rejection_cycles(self.model_meter()), 0)

    async def test_rejection_without_limits_holds_five_minutes(self):
        self.register({'codex': (self.default, 10, 5 * 24)})
        self.automatic(False)
        self.assertEqual(await self.rejection_cycles(None), 0)

    async def test_automatic_does_not_pick_the_rejected_account_again(self):
        other = self.home('other')
        self.register({'codex': (self.default, 10, 2 * 24), 'other': (other, 10, 6 * 24)})
        self.automatic()
        broker = CodexBroker(self.store)
        self.assertEqual(broker.select(), 'codex')
        with self.store.db:
            broker.record_rate_limit('codex', self.model_meter(), rejected=True)
        self.assertEqual(broker.select(), 'other')
        await refresh_codex_accounts(self.store, self.clean_check)
        self.assertEqual(broker.select(), 'other')


class CodexSelectionTests(CodexFixture):
    async def test_stop_state_reaches_the_runner_in_every_account_mode(self):
        self.register({'codex': (self.default, 10, 24)})
        for managed, automatic in ((False, False), (True, False), (True, True)):
            with self.subTest(managed=managed, automatic=automatic):
                broker = CodexBroker(self.store)

                async def run(provider, prompt, cwd, session_id, stopped=None, **options):
                    return RunResult(session_id, failure_code='owner_stopped' if stopped and stopped() else None)

                with patch.object(broker, 'managed', return_value=managed), \
                        patch.object(broker, 'automatic', return_value=automatic):
                    result = await broker.run(run, 'codex', 'prompt', self.root, None, stopped=lambda: True)
                self.assertEqual(result.failure_code, 'owner_stopped')

    async def test_each_ordinary_meter_keeps_94_and_limits_at_95_percent(self):
        self.automatic()
        self.register({'codex': (self.default, 10, 24)})
        broker = CodexBroker(self.store)
        reset = time.time() + 120
        status = self.store.get('codex_account_status')
        for meter in ('five_hour', 'seven_day'):
            with self.subTest(meter=meter):
                status['codex']['usage'][meter] = {'utilization': 94, 'resets_at': reset}
                with self.store.db:
                    self.store.put('codex_account_status', status)
                self.assertEqual(broker.select(), 'codex', meter)
                self.assertEqual(broker.account_state('codex')['state'], 'available', meter)
                status['codex']['usage'][meter]['utilization'] = 95
                with self.store.db:
                    self.store.put('codex_account_status', status)
                self.assertIsNone(broker.select(), meter)
                self.assertEqual(broker.account_state('codex')['state'], 'limited', meter)
                status['codex']['usage'][meter]['utilization'] = 10

    async def test_selection_skips_accounts_at_95_percent_and_prefers_the_soonest_weekly_reset(self):
        self.automatic()
        broker = CodexBroker(self.store)
        homes = {name: self.home(name) for name in ('b', 'c', 'd')}
        self.register({'codex': (self.default, 40, 72), 'codex-b': (homes['b'], 96, 24),
                       'codex-c': (homes['c'], 10, 48), 'codex-d': (homes['d'], 10, 1)})
        accounts = self.store.get('codex_accounts')
        accounts['codex-d']['enabled'] = False
        with self.store.db:
            self.store.put('codex_accounts', accounts)
        self.assertEqual(broker.select(), 'codex-c')
        self.assertEqual(broker.account_state('codex-b')['state'], 'limited')
        self.assertEqual(broker.select(attempted={'codex-c'}), 'codex')
        status = self.store.get('codex_account_status')
        status['codex-c']['identity']['logged_in'] = False
        with self.store.db:
            self.store.put('codex_account_status', status)
        self.assertEqual(broker.select(), 'codex')

    async def test_a_quota_rejection_resumes_the_same_thread_on_the_next_account(self):
        self.automatic()
        second = self.home('second')
        reset = int(time.time()) + 5 * 86400
        self.behave({'codex-home': {'reject': True, 'reset_at': reset}, 'second': {'used': 20}})
        self.register({'codex': (self.default, 50, 24), 'codex-second': (second, 20, 48)})
        finished, result = await self.run_worker()
        self.assertTrue(result['success'], result.get('error'))
        self.assertEqual(result['text'], 'done on second')
        self.assertEqual(finished['account_alias'], 'codex-second')
        turns = [(Path(call['home']).name, call['method'], call['params']) for call in self.calls()
                 if call['method'] in ('thread/start', 'thread/resume', 'turn/start')]
        self.assertEqual([(home, method) for home, method, _ in turns],
                         [('codex-home', 'thread/start'), ('codex-home', 'turn/start'),
                          ('second', 'thread/resume'), ('second', 'turn/start')])
        self.assertEqual(turns[2][2]['threadId'], turns[1][2]['threadId'])
        self.assertEqual(finished['session'], turns[1][2]['threadId'])
        self.assertIn(LIMIT_CONTINUATION + 'build this', turns[3][2]['input'][0]['text'])
        self.assertNotIn(LIMIT_CONTINUATION, turns[1][2]['input'][0]['text'])
        self.assertEqual({call['sqlite'] for call in self.calls()}, {str(self.default)})
        self.assertEqual(self.store.get('codex_account_blocks')['codex']['until'], reset)
        self.assertEqual(CodexBroker(self.store).select(), 'codex-second')

    async def test_a_turn_that_reports_95_percent_moves_the_next_launch_to_another_account(self):
        self.automatic()
        second = self.home('second')
        self.behave({'codex-home': {'used': 96}, 'second': {'used': 20}})
        self.register({'codex': (self.default, 50, 24), 'codex-second': (second, 20, 48)})
        finished, result = await self.run_worker()
        self.assertTrue(result['success'], result.get('error'))
        self.assertEqual(finished['account_alias'], 'codex')
        self.assertEqual(CodexBroker(self.store).account_state('codex')['state'], 'limited')
        self.assertEqual(CodexBroker(self.store).select(), 'codex-second')

    async def test_codex_runs_on_the_service_login_until_a_chatgpt_account_is_confirmed(self):
        from coordinator import control_ui
        discover_codex_accounts(self.store)
        finished, result = await self.run_worker()
        self.assertTrue(result['success'], result.get('error'))
        self.assertIsNone(finished['account_alias'])
        self.behave({'codex-home': {'type': 'apiKey'}})
        await refresh_codex_accounts(self.store, lambda account: collect_codex_status(account, binary=str(self.binary)))
        self.assertTrue(self.store.get('codex_accounts')['codex']['awaiting_login'])
        finished, result = await self.run_worker()
        self.assertTrue(result['success'], result.get('error'))
        self.assertEqual(result['text'], 'done on codex-home')
        with self.store.db:
            control_ui.view(self.store, TOPIC, 'delegation')
        text = self.store.db.execute('SELECT text FROM outbox ORDER BY id DESC LIMIT 1').fetchone()[0]
        self.assertNotIn('Codex account', text)

    async def test_a_reattached_worker_resumes_on_the_next_account_after_a_rejection(self):
        self.automatic()
        second = self.home('second')
        self.register({'codex': (self.default, 50, 24), 'codex-second': (second, 20, 48)})
        calls = []

        class Runner:
            codex_broker = None

            async def run(self, provider, prompt, cwd, session_id, **options):
                calls.append((options.get('account_alias'), options.get('attach'), session_id, prompt))
                if options.get('attach'):
                    return RunResult(session_id, error='You hit your usage limit.', quota_limited=True)
                return RunResult(session_id, text='done', success=True)
        worker = self.worker()
        with self.store.db:
            self.store.db.execute("UPDATE workers SET status='running',session='thread-1',account_alias='codex',"
                                  "host='worker-x' WHERE id=?", (worker['id'],))
        pool = WorkerPool(self.store, Runner(), AccountBroker(self.store), codex_accounts=CodexBroker(self.store))
        finished = await pool.run(worker, self.store.topic(TOPIC))
        result = json.loads(finished['result'])
        self.assertTrue(result['success'], result.get('error'))
        self.assertEqual(finished['account_alias'], 'codex-second')
        self.assertEqual([call[:3] for call in calls], [(None, True, 'thread-1'), ('codex-second', False, 'thread-1')])
        self.assertEqual(calls[1][3], LIMIT_CONTINUATION + 'build this')
        self.assertEqual(CodexBroker(self.store).account_state('codex')['state'], 'limited')

    async def test_a_credit_rejection_resumes_the_same_thread_on_the_next_account(self):
        self.automatic()
        second = self.home('second')
        self.behave({'codex-home': {'reject': True, 'reject_used': 10,
                                    'reject_message': 'Your workspace is out of credits. Add credits to continue.',
                                    'credits': {'hasCredits': False, 'unlimited': False, 'balance': '0'}},
                     'second': {'used': 20}})
        self.register({'codex': (self.default, 50, 24), 'codex-second': (second, 20, 48)})
        finished, result = await self.run_worker()
        self.assertTrue(result['success'], result.get('error'))
        self.assertEqual((finished['account_alias'], result['text']), ('codex-second', 'done on second'))
        turns = self.turns()
        self.assertEqual([(home, method) for home, method, _ in turns],
                         [('codex-home', 'thread/start'), ('codex-home', 'turn/start'),
                          ('second', 'thread/resume'), ('second', 'turn/start')])
        self.assertEqual(turns[2][2]['threadId'], turns[1][2]['threadId'])
        self.assertEqual(finished['session'], turns[1][2]['threadId'])
        self.assertIn(LIMIT_CONTINUATION + 'build this', turns[3][2]['input'][0]['text'])
        self.assertEqual(self.notices(), [])

    async def test_a_codex_worker_waits_when_every_account_is_limited_then_starts_when_room_returns(self):
        from coordinator.service import Service
        self.automatic()
        home = self.separate_home_topic()
        self.register({'codex': (self.default, 97, 24)})
        service = Service(self.store, None, ProviderRunner(self.store.directory, binaries={'codex': str(self.binary)}),
                          self.root)
        worker = self.worker()
        waiting = await service.workers.run(worker, self.store.topic(TOPIC))
        result = json.loads(waiting['result'])
        self.assertEqual((waiting['status'], result['success'], result['failure_code']),
                         ('waiting_for_quota', False, 'accounts_unavailable'))
        self.assertTrue(waiting['fresh'])
        self.assertEqual(self.calls(), [])
        notices = self.store.db.execute('SELECT topic,text FROM outbox').fetchall()
        self.assertEqual([row['topic'] for row in notices], [home])
        self.assertIn('every enabled Codex account', notices[0]['text'])
        self.assertIn('Automatic switching is on', notices[0]['text'])
        self.assertIn('resumes the same thread', notices[0]['text'])
        self.assertEqual(self.callbacks(), [])
        await service.workers_once()
        self.assertEqual(service.workers.get(worker['id'])['status'], 'waiting_for_quota')
        self.register({'codex': (self.default, 10, 24)})
        await service.workers_once()
        await asyncio.gather(*service.worker_tasks.values())
        finished = service.workers.get(worker['id'])
        self.assertTrue(json.loads(finished['result'])['success'])
        self.assertEqual([method for _, method, _ in self.turns()], ['thread/start', 'turn/start'])

    async def test_rejected_accounts_park_the_worker_and_resume_its_thread_after_a_reset(self):
        from coordinator.service import Service
        self.automatic()
        home = self.separate_home_topic()
        second = self.home('second')
        reset = int(time.time()) + 3600
        self.behave({'codex-home': {'reject': True, 'reset_at': reset},
                     'second': {'reject': True, 'reset_at': reset}})
        self.register({'codex': (self.default, 50, 24), 'codex-second': (second, 20, 48)})
        service = Service(self.store, None, ProviderRunner(self.store.directory, binaries={'codex': str(self.binary)}),
                          self.root)
        worker = self.worker()
        waiting = await service.workers.run(worker, self.store.topic(TOPIC))
        self.assertEqual((waiting['status'], waiting['fresh']), ('waiting_for_quota', 0))
        self.assertEqual(json.loads(waiting['result'])['waiting_until'], reset)
        turns = self.turns()
        self.assertEqual([(home, method) for home, method, _ in turns],
                         [('codex-home', 'thread/start'), ('codex-home', 'turn/start'),
                          ('second', 'thread/resume'), ('second', 'turn/start')])
        self.assertEqual(turns[2][2]['threadId'], turns[1][2]['threadId'])
        self.assertEqual(len(self.notices()), 1)
        self.assertEqual(self.store.db.execute("SELECT topic FROM outbox WHERE text LIKE 'Codex account %'").fetchone()[0], home)
        self.assertIn('Automatic switching is on', self.notices()[0])
        self.assertEqual(self.callbacks(), [])
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM messages WHERE kind='worker_result'").fetchone()[0], 0)
        self.behave({'codex-home': {'used': 20}, 'second': {'used': 20}})
        with patch('coordinator.codex_accounts.time.time', return_value=reset + 1):
            await service.workers_once()
            await asyncio.gather(*service.worker_tasks.values())
        finished = service.workers.get(worker['id'])
        self.assertTrue(json.loads(finished['result'])['success'])
        self.assertEqual(finished['session'], waiting['session'])
        resumed = self.turns()[4:]
        self.assertEqual([method for _, method, _ in resumed], ['thread/resume', 'turn/start'])
        self.assertEqual(resumed[0][2]['threadId'], waiting['session'])
        self.assertIn(LIMIT_CONTINUATION + 'build this', resumed[1][2]['input'][0]['text'])
        self.assertEqual(self.callbacks(), [])


class CodexActiveAccountTests(CodexFixture):
    async def test_without_a_valid_pick_parent_and_workers_use_the_first_signed_in_account(self):
        second = self.home('second')
        third = self.home('third')
        self.register({'a': (self.default, 50, 72), 'c': (third, 10, 1), 'b': (second, 10, 1)})
        status = self.store.get('codex_account_status')
        status['a'].update(error='login_required')
        self.store.put('codex_account_status', status)
        broker = CodexBroker(self.store)
        for chosen in (None, 'missing'):
            with self.subTest(chosen=chosen):
                self.store.put('codex_active_account', chosen)
                self.assertEqual(broker.parent_account(), 'b')
                self.assertEqual(broker.active(), 'b')
                self.assertEqual(broker.current(), 'b')
                self.assertEqual(broker.select(), 'b')
                self.assertEqual(self.store.get('codex_active_account'), chosen)
        self.assertEqual(self.store.accept(update(101, '/accounts')), 'control')
        text = self.store.db.execute('SELECT text FROM outbox ORDER BY id DESC LIMIT 1').fetchone()[0]
        self.assertIn('🟢 b@example.com: ? · 90% · active', text)
        finished, result = await self.run_worker()
        self.assertTrue(result['success'], result.get('error'))
        self.assertEqual((finished['account_alias'], result['text']), ('b', 'done on second'))

    async def test_a_signed_in_default_home_wins_over_an_earlier_alias_without_a_pick(self):
        second = self.home('second')
        self.register({'z': (self.default, 50, 72), 'a': (second, 10, 1)})
        self.assertEqual(CodexBroker(self.store).parent_account(), 'z')

    async def test_without_a_signed_in_account_the_fallback_can_still_name_the_default_home(self):
        second = self.home('second')
        self.register({'z': (self.default, 50, 72), 'a': (second, 10, 1)})
        self.store.put('codex_account_status', {})
        broker = CodexBroker(self.store)
        self.assertEqual(broker.active(), 'z')
        self.assertIsNone(broker.parent_account())
        self.store.put('codex_accounts', {'a': self.store.get('codex_accounts')['a']})
        self.assertEqual(broker.active(), 'a')
        self.assertIsNone(broker.parent_account())

    async def test_a_signed_out_pick_stays_active_even_with_a_signed_in_account(self):
        second = self.home('second')
        self.register({'codex': (self.default, 50, 72), 'codex-second': (second, 10, 1)})
        status = self.store.get('codex_account_status')
        status['codex'].update(error='login_required')
        self.store.put('codex_account_status', status)
        self.store.put('codex_active_account', 'codex')
        broker = CodexBroker(self.store)
        self.assertEqual(broker.active(), 'codex')
        self.assertEqual(broker.current(), 'codex')
        self.assertIsNone(broker.parent_account())
        self.assertIsNone(broker.select())
        _, result = await self.run_worker()
        self.assertEqual(result['failure_code'], 'accounts_unavailable')
        self.assertEqual(self.calls(), [])

    async def test_manual_account_keeps_running_past_the_automatic_switch_point(self):
        second = self.home('second')
        self.behave({'codex-home': {'used': 96}, 'second': {'used': 20}})
        self.register({'codex': (self.default, 96, 24), 'codex-second': (second, 20, 48)})
        broker = CodexBroker(self.store)
        with self.store.db:
            broker.record_rate_limit('codex', {'limitId': 'codex', 'primary': {
                'usedPercent': 96, 'windowDurationMins': 10080, 'resetsAt': time.time() + 86400}})
        self.assertEqual(broker.account_state('codex')['state'], 'available')
        finished, result = await self.run_worker()
        self.assertTrue(result['success'], result.get('error'))
        self.assertEqual(finished['account_alias'], 'codex')
        self.assertEqual(broker.select(), 'codex')
        finished, result = await self.run_worker()
        self.assertTrue(result['success'], result.get('error'))
        self.assertEqual(finished['account_alias'], 'codex')

    async def test_by_default_codex_stays_on_the_active_account_until_it_is_switched_in_one_step(self):
        second = self.home('second')
        self.behave({'codex-home': {'used': 50}, 'second': {'used': 10}})
        self.register({'codex': (self.default, 50, 72), 'codex-second': (second, 10, 1)})
        self.assertIs(self.store.get('codex_auto_switch', False), False)
        finished, result = await self.run_worker()
        self.assertTrue(result['success'], result.get('error'))
        self.assertEqual(finished['account_alias'], 'codex')
        outcome = self.use('codex-second')
        self.assertTrue(outcome.ok, outcome.text)
        self.assertIn('codex-second@example.com is now the active Codex account', outcome.text)
        self.assertEqual(self.store.get('codex_active_account'), 'codex-second')
        finished, result = await self.run_worker()
        self.assertTrue(result['success'], result.get('error'))
        self.assertEqual((finished['account_alias'], result['text']), ('codex-second', 'done on second'))

    async def test_only_a_signed_in_codex_account_can_become_the_active_one(self):
        second = self.home('second')
        self.register({'codex': (self.default, 50, 72), 'codex-second': (second, 10, 1)})
        with self.store.db:
            self.store.put('accounts', {'work': {'config_dir': str(self.root / 'claude-work'), 'enabled': True}})
            accounts = self.store.get('codex_accounts')
            accounts['codex-second']['enabled'] = False
            self.store.put('codex_accounts', accounts)
        for alias, reason in (('work', 'not signed in'), ('nobody', 'not registered'),
                              ('codex-second', 'not signed in or is disabled')):
            outcome = self.use(alias)
            self.assertFalse(outcome.ok)
            self.assertIn(reason, outcome.text)
        self.assertIsNone(self.store.get('codex_active_account'))

    async def test_the_coordinator_can_switch_the_active_codex_account_but_not_the_automatic_setting(self):
        self.assertIn('account_use', mcp.TOOLS)
        self.assertNotIn('accounts_codex_auto', mcp.TOOLS)
        self.assertIn('only when the owner picks it', mcp.TOOLS['account_use'].description)
        self.assertIn('originating topic', mcp.TOOLS['account_use'].description)

    async def test_worker_crossing_80_percent_in_project_chat_queues_no_usage_card(self):
        home = '-10042:99'
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,-10042,99,'Home',?,1)''', (home, str(self.root)))
            self.store.put('coordinator_home_topic', home)
        self.behave({'codex-home': {'used': 85}})
        self.register({'codex': (self.default, 50, 24)})
        finished, result = await self.run_worker()
        self.assertTrue(result['success'], result.get('error'))
        self.assertEqual(finished['account_alias'], 'codex')
        project_cards = [tuple(row) for row in self.store.db.execute(
            "SELECT topic,text FROM outbox WHERE topic=? AND text LIKE 'Codex account %'", (TOPIC,))]
        self.assertEqual(project_cards, [])
        self.assertEqual(self.notices(), [])
        usage = self.store.get('codex_account_status')['codex']['usage']['seven_day']
        self.assertEqual(usage['utilization'], 85)

    async def test_worker_rejection_notice_goes_only_to_home_once(self):
        home = '-10042:99'
        with self.store.db:
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,-10042,99,'Home',?,1)''', (home, str(self.root)))
            self.store.put('coordinator_home_topic', home)
        self.behave({'codex-home': {'reject': True}})
        self.register({'codex': (self.default, 50, 24)})
        finished, result = await self.run_worker()
        self.assertEqual(finished['status'], 'waiting_for_quota')
        self.assertTrue(result['quota_limited'])
        cards = [tuple(row) for row in self.store.db.execute(
            "SELECT topic,text FROM outbox WHERE text LIKE 'Codex account %'")]
        self.assertEqual([topic for topic, text in cards], [home])
        self.assertIn('hit its usage limit', cards[0][1])
        another, blocked = await self.run_worker()
        self.assertEqual(another['status'], 'waiting_for_quota')
        self.assertEqual(len(self.notices()), 1)
        self.assertEqual(self.callbacks(), [])
        broker = CodexBroker(self.store)
        broker.record_rate_limit('codex', result['rate_limit_info'], rejected=True)
        self.assertEqual(len(self.notices()), 1)
        self.assertIn('codex', self.store.get('codex_account_blocks'))
        from coordinator.service import Service
        from coordinator.session import AccountsUnavailable
        service = Service(self.store, None, ProviderRunner(self.store.directory), self.root)
        until = self.store.get('codex_account_blocks')['codex']['until']
        service.pause_parent(TOPIC, AccountsUnavailable(until, provider='codex', alias='codex'))
        self.assertEqual(len(self.notices()), 1)
        self.assertEqual([row[0] for row in self.store.db.execute('SELECT topic FROM outbox')], [home])

    async def test_parent_rejection_defers_notice_until_the_service_confirms_a_hard_stop(self):
        from coordinator.service import Service
        from coordinator.session import AccountsUnavailable
        self.register({'codex': (self.default, 50, 24)})
        broker = CodexBroker(self.store)
        reset = time.time() + 86400
        broker.record_rate_limit('codex', {'primary': {'usedPercent': 100,
                                 'windowDurationMins': 10080, 'resetsAt': reset}}, rejected=True, parent=True)
        self.assertEqual(self.notices(), [])
        self.assertEqual(self.store.get('codex_account_blocks')['codex']['reason'], 'quota')
        self.assertEqual(self.store.get('codex_account_status')['codex']['usage']['seven_day']['utilization'], 100)
        service = Service(self.store, None, ProviderRunner(self.store.directory), self.root)
        for attempt in range(2):
            service.pause_parent(TOPIC, AccountsUnavailable(reset, provider='codex', alias='codex'))
        reports = [tuple(row) for row in self.store.db.execute('SELECT topic,text FROM outbox')]
        self.assertEqual([topic for topic, text in reports], [TOPIC])
        self.assertIn('The main chat is paused', reports[0][1])

    async def test_below_80_percent_used_the_owner_hears_nothing(self):
        second = self.home('second')
        self.behave({'codex-home': {'used': 79, 'five_hour': 79}, 'second': {'used': 20}})
        self.register({'codex': (self.default, 50, 24), 'codex-second': (second, 20, 48)})
        await self.run_worker()
        self.assertEqual(self.notices(), [])

    async def test_a_missing_topic_does_not_consume_the_usage_notice(self):
        self.register({'codex': (self.default, 85, 24)})
        broker = CodexBroker(self.store)
        with self.store.db:
            self.store.db.execute('UPDATE topics SET enabled=0 WHERE id=?', (TOPIC,))
            broker.notify_limit('codex', rejected=True)
        self.assertEqual(self.notices(), [])
        with self.store.db:
            self.store.db.execute('UPDATE topics SET enabled=1 WHERE id=?', (TOPIC,))
            broker.notify_limit('codex', rejected=True)
        self.assertEqual(len(self.notices()), 1)

    async def test_at_80_percent_used_work_stays_on_the_active_account_without_a_notice(self):
        second = self.home('second')
        self.behave({'codex-home': {'used': 85}, 'second': {'used': 20}})
        self.register({'codex': (self.default, 50, 24), 'codex-second': (second, 20, 48)})
        for attempt in range(2):
            finished, result = await self.run_worker()
            self.assertTrue(result['success'], result.get('error'))
            self.assertEqual(finished['account_alias'], 'codex')
            self.assertEqual(self.notices(), [])

    async def test_five_hour_and_weekly_notices_clear_by_window_after_recovery(self):
        self.register({'codex': (self.default, 70, 24)})
        broker = CodexBroker(self.store)
        status = self.store.get('codex_account_status')
        status['codex']['usage']['five_hour'] = {'utilization': 96, 'resets_at': time.time() + 3600}
        with self.store.db:
            self.store.put('codex_account_status', status)
            broker.notify_limit('codex', rejected=True)
        self.assertEqual(len(self.notices()), 1)
        status['codex']['usage']['seven_day']['utilization'] = 96
        with self.store.db:
            self.store.put('codex_account_status', status)
            broker.notify_limit('codex', rejected=True)
        self.assertEqual(len(self.notices()), 2)
        status['codex']['usage']['five_hour']['utilization'] = 10
        with self.store.db:
            self.store.put('codex_account_status', status)
            broker.notify_limit('codex', rejected=True)
        status['codex']['usage']['five_hour']['utilization'] = 96
        with self.store.db:
            self.store.put('codex_account_status', status)
            broker.notify_limit('codex', rejected=True)
        self.assertEqual(len(self.notices()), 3)

    async def test_a_rejection_parks_the_worker_until_the_owner_switches_and_then_resumes_the_thread(self):
        from coordinator.service import Service
        second = self.home('second')
        reset = int(time.time()) + 5 * 86400
        self.behave({'codex-home': {'reject': True, 'reset_at': reset}, 'second': {'used': 20}})
        self.register({'codex': (self.default, 50, 24), 'codex-second': (second, 20, 48)})
        service = Service(self.store, None, ProviderRunner(self.store.directory, binaries={'codex': str(self.binary)}),
                          self.root)
        worker = self.worker()
        parked = await service.workers.run(worker, self.store.topic(TOPIC))
        self.assertEqual((parked['status'], parked['account_alias']), ('waiting_for_quota', 'codex'))
        self.assertEqual([(home, method) for home, method, _ in self.turns()],
                         [('codex-home', 'thread/start'), ('codex-home', 'turn/start')])
        [notice] = self.notices()
        self.assertIn('Codex account codex@example.com hit its usage limit', notice)
        self.assertIn('resets in 4d 23h', notice)
        self.assertIn('codex-second@example.com · weekly 20% used', notice)
        self.assertEqual(self.callbacks(), [])
        await service.workers_once()
        self.assertEqual(service.workers.get(worker['id'])['status'], 'waiting_for_quota')
        self.assertTrue(self.use('codex-second').ok)
        await service.workers_once()
        await asyncio.gather(*service.worker_tasks.values())
        finished = service.workers.get(worker['id'])
        result = json.loads(finished['result'])
        self.assertTrue(result['success'], result.get('error'))
        self.assertEqual((finished['status'], finished['account_alias']), ('done', 'codex-second'))
        turns = self.turns()
        self.assertEqual([(home, method) for home, method, _ in turns[2:]],
                         [('second', 'thread/resume'), ('second', 'turn/start')])
        self.assertEqual(turns[2][2]['threadId'], turns[1][2]['threadId'])
        self.assertIn(LIMIT_CONTINUATION + 'build this', turns[3][2]['input'][0]['text'])
        self.assertEqual(len(self.notices()), 1)

    async def test_a_reattached_worker_waits_on_the_active_account_after_a_rejection(self):
        second = self.home('second')
        self.register({'codex': (self.default, 50, 24), 'codex-second': (second, 20, 48)})
        calls = []

        class Runner:
            codex_broker = None

            async def run(self, provider, prompt, cwd, session_id, **options):
                calls.append(options.get('attach'))
                return RunResult(session_id, error='You hit your usage limit.', quota_limited=True,
                                 failure_code='quota_limited')
        worker = self.worker()
        with self.store.db:
            self.store.db.execute("UPDATE workers SET status='running',session='thread-1',account_alias='codex',"
                                  "host='worker-x' WHERE id=?", (worker['id'],))
        pool = WorkerPool(self.store, Runner(), AccountBroker(self.store), codex_accounts=CodexBroker(self.store))
        finished = await pool.run(worker, self.store.topic(TOPIC))
        self.assertEqual((finished['status'], calls), ('waiting_for_quota', [True]))
        self.assertEqual(len(self.notices()), 1)

    async def test_a_credit_rejection_waits_on_the_active_account_and_resumes_after_its_hold(self):
        from coordinator.service import Service
        home = self.separate_home_topic()
        second = self.home('second')
        self.behave({'codex-home': {'reject': True, 'reject_used': 10,
                                    'reject_message': 'Your workspace is out of credits. Add credits to continue.',
                                    'credits': {'hasCredits': False, 'unlimited': False, 'balance': '0'}},
                     'second': {'used': 20}})
        self.register({'codex': (self.default, 50, 24), 'codex-second': (second, 20, 48)})
        service = Service(self.store, None, ProviderRunner(self.store.directory, binaries={'codex': str(self.binary)}),
                          self.root)
        worker = self.worker()
        waiting = await service.workers.run(worker, self.store.topic(TOPIC))
        self.assertEqual((waiting['status'], waiting['account_alias']), ('waiting_for_quota', 'codex'))
        self.assertEqual(self.store.db.execute("SELECT topic FROM outbox WHERE text LIKE 'Codex account %'").fetchone()[0], home)
        self.assertEqual(self.callbacks(), [])
        reset = json.loads(waiting['result'])['waiting_until']
        self.assertLessEqual(reset, time.time() + 300)
        await service.workers_once()
        self.assertEqual(service.workers.get(worker['id'])['status'], 'waiting_for_quota')
        self.assertEqual([home for home, method, _ in self.turns() if method == 'turn/start'], ['codex-home'])
        self.behave({'codex-home': {'used': 10}, 'second': {'used': 20}})
        with patch('coordinator.codex_accounts.time.time', return_value=reset + 1):
            await service.workers_once()
            await asyncio.gather(*service.worker_tasks.values())
        finished = service.workers.get(worker['id'])
        self.assertTrue(json.loads(finished['result'])['success'])
        self.assertEqual((finished['session'], finished['account_alias']), (waiting['session'], 'codex'))
        self.assertEqual([(home, method) for home, method, _ in self.turns()[2:]],
                         [('codex-home', 'thread/resume'), ('codex-home', 'turn/start')])
        self.assertEqual(self.callbacks(), [])

    async def test_the_usage_check_records_high_usage_without_a_notice(self):
        second = self.home('second')
        self.register({'codex': (self.default, 50, 24), 'codex-second': (second, 20, 48)})
        used = {'codex': 82, 'codex-second': 99}

        async def collect(account):
            alias = 'codex' if account['config_dir'] == str(self.default) else 'codex-second'
            return {'checked_at': time.time(), 'observed_at': time.time(),
                    'identity': {'email': alias + '@example.com', 'logged_in': True},
                    'usage': {'seven_day': {'utilization': used[alias], 'resets_at': time.time() + 3600}}}
        await refresh_codex_accounts(self.store, collect)
        used['codex'] = 90
        await refresh_codex_accounts(self.store, collect)
        self.assertEqual(self.notices(), [])
        self.assertEqual(self.store.get('codex_account_status')['codex']['usage']['seven_day']['utilization'], 90)
        used['codex'] = 10
        await refresh_codex_accounts(self.store, collect)
        used['codex'] = 81
        await refresh_codex_accounts(self.store, collect)
        self.assertEqual(self.notices(), [])

    async def test_with_automatic_switching_on_the_last_usable_account_runs_low_without_a_notice(self):
        self.automatic()
        second = self.home('second')
        self.behave({'codex-home': {'used': 85}, 'second': {'used': 20}})
        self.register({'codex': (self.default, 50, 24), 'codex-second': (second, 20, 48)})
        finished, _ = await self.run_worker()
        self.assertEqual(finished['account_alias'], 'codex')
        self.assertEqual(self.notices(), [])
        status = self.store.get('codex_account_status')
        status['codex-second']['usage']['seven_day']['utilization'] = 96
        with self.store.db:
            self.store.put('codex_account_status', status)
        finished, _ = await self.run_worker()
        self.assertEqual(finished['account_alias'], 'codex')
        self.assertEqual(self.notices(), [])


class CodexResetTests(CodexFixture):
    def test_installed_reset_credit_schema_support(self):
        self.assertEqual(RESET_CREDIT_SCHEMA, {'version': '0.159.2', 'details': True, 'credit_id': True})

    async def test_slow_redeem_does_not_hold_telegram_intake(self):
        from coordinator.service import Service
        self.register({'codex': (self.default, 95, 24)})
        self.behave({'codex-home': {'used': 95, 'reset_credits': 1}})
        (self.bin / 'slow').touch()
        with self.codex_path():
            queued = control_api.call(self.store, 'account.codex_reset', {'alias': 'codex'},
                                      topic=TOPIC, source='telegram')
            self.assertEqual(queued.state, 'queued')
            service = Service(self.store, object(), ProviderRunner(self.store.directory), self.root)
            task = asyncio.create_task(service.controls_once())
            await asyncio.sleep(.1)
            self.assertFalse(task.done())
            with self.store.db:
                self.store.message_save(TOPIC, 'owner', 'another message')
            await task
        consumed = [call for call in self.calls() if call['method'] == 'account/rateLimitResetCredit/consume']
        self.assertEqual(len(consumed), 1)
        self.assertTrue(any('Codex banked reset redeemed' in row[0]
                            for row in self.store.db.execute('SELECT text FROM outbox')))

    def codex_path(self):
        return patch.dict(os.environ, {'PATH': str(self.bin) + os.pathsep + os.environ['PATH']})

    async def test_fake_app_server_redeems_one_reset_and_returns_refreshed_limits(self):
        from coordinator.codex_accounts import redeem_codex_reset
        self.behave({'codex-home': {'email': 'owner@example.com', 'used': 97, 'five_hour': 96,
                                   'reset_credits': 2}})
        result = await redeem_codex_reset({'config_dir': str(self.default)}, binary=str(self.binary), guarded=True)
        self.assertTrue(result['redeemed'])
        self.assertEqual((result['usage']['five_hour']['utilization'],
                          result['usage']['seven_day']['utilization'], result['resets_available']), (0, 0, 1))
        calls = [call for call in self.calls() if call['method'] == 'account/rateLimitResetCredit/consume']
        self.assertEqual(len(calls), 1)
        self.assertIn('idempotencyKey', calls[0]['params'])
        self.assertEqual(Path(calls[0]['home']), self.default)

    async def test_reset_spends_the_earliest_expiring_available_credit_without_logging_ids(self):
        from coordinator.codex_accounts import redeem_codex_reset
        details = self.reset_details()
        self.behave({'codex-home': {'used': 97, 'reset_credits': 3, 'reset_details': details}})
        with self.assertLogs('coordinator.codex_accounts', level='INFO') as logs:
            result = await redeem_codex_reset({'config_dir': str(self.default)}, binary=str(self.binary), guarded=True)
        consumed = [call for call in self.calls() if call['method'] == 'account/rateLimitResetCredit/consume']
        self.assertEqual(len(consumed), 1)
        self.assertEqual(consumed[0]['params']['creditId'], 'early')
        self.assertIn('idempotencyKey', consumed[0]['params'])
        self.assertEqual(result['resets_available'], 2)
        self.assertEqual(result['reset_expires'], details[0]['expiresAt'])
        self.assertEqual([credit['id'] for credit in result['reset_credits']], ['late', 'never'])
        self.assertEqual(logs.output, ['INFO:coordinator.codex_accounts:reset spend ordered=yes'])

    async def test_reset_leaves_credit_selection_to_codex_when_schema_support_is_off(self):
        from coordinator.codex_accounts import redeem_codex_reset
        self.behave({'codex-home': {'used': 97, 'reset_credits': 3, 'reset_details': self.reset_details(),
                                   'backend_credit_id': 'late'}})
        with patch.dict(RESET_CREDIT_SCHEMA, credit_id=False), \
                self.assertLogs('coordinator.codex_accounts', level='INFO') as logs:
            result = await redeem_codex_reset({'config_dir': str(self.default)}, binary=str(self.binary), guarded=True)
        consumed = [call for call in self.calls() if call['method'] == 'account/rateLimitResetCredit/consume']
        self.assertEqual(len(consumed), 1)
        self.assertEqual(set(consumed[0]['params']), {'idempotencyKey'})
        self.assertTrue(result['redeemed'])
        self.assertEqual(logs.output, ['INFO:coordinator.codex_accounts:reset spend ordered=no'])

    async def test_count_only_reset_does_not_send_a_credit_id(self):
        from coordinator.codex_accounts import redeem_codex_reset
        self.behave({'codex-home': {'used': 97, 'reset_credits': 2, 'reset_details': None}})
        with self.assertLogs('coordinator.codex_accounts', level='INFO') as logs:
            result = await redeem_codex_reset({'config_dir': str(self.default)}, binary=str(self.binary), guarded=True)
        consumed = [call for call in self.calls() if call['method'] == 'account/rateLimitResetCredit/consume']
        self.assertEqual(len(consumed), 1)
        self.assertEqual(set(consumed[0]['params']), {'idempotencyKey'})
        self.assertEqual(result['resets_available'], 1)
        self.assertNotIn('reset_credits', result)
        self.assertIsNone(result['reset_expires'])
        self.assertEqual(logs.output, ['INFO:coordinator.codex_accounts:reset spend ordered=no'])

    async def test_guard_refuses_no_credit_and_usage_below_95_without_consuming(self):
        from coordinator.codex_accounts import redeem_codex_reset
        for credits, used, message in ((0, 97, 'no banked reset'), (1, 94, 'below 95%')):
            self.behave({'codex-home': {'used': used, 'reset_credits': credits}})
            with self.assertRaisesRegex(ValueError, message):
                await redeem_codex_reset({'config_dir': str(self.default)}, binary=str(self.binary), guarded=True)
        self.assertFalse(any(call['method'] == 'account/rateLimitResetCredit/consume' for call in self.calls()))

    async def test_guard_allows_a_limit_or_refused_ordinary_usage_below_95(self):
        from coordinator.codex_accounts import redeem_codex_reset
        for limited, allowed in ((True, True), (False, False)):
            with self.subTest(limited=limited, ordinary_allowed=allowed):
                self.behave({'codex-home': {'used': 10, 'reset_credits': 1, 'ordinary_allowed': allowed}})
                result = await redeem_codex_reset({'config_dir': str(self.default)}, binary=str(self.binary),
                                                  guarded=True, limited=limited)
                self.assertTrue(result['redeemed'])
        self.assertEqual(sum(call['method'] == 'account/rateLimitResetCredit/consume' for call in self.calls()), 2)

    async def test_reset_outcomes_keep_the_existing_error_texts(self):
        from coordinator.codex_accounts import redeem_codex_reset
        for outcome, message in (('nothingToReset', 'No Codex usage window can be reset'),
                                 ('noCredit', 'This Codex account has no banked reset'),
                                 ('alreadyRedeemed', 'This reset request was already redeemed'),
                                 ('unknown', 'Codex did not confirm the reset')):
            with self.subTest(outcome=outcome):
                self.behave({'codex-home': {'used': 97, 'reset_credits': 1, 'reset_outcome': outcome}})
                with self.assertRaisesRegex(ValueError, message):
                    await redeem_codex_reset({'config_dir': str(self.default)}, binary=str(self.binary), guarded=True)
        self.assertEqual(sum(call['method'] == 'account/rateLimitResetCredit/consume' for call in self.calls()), 4)

    async def test_owner_command_confirms_and_redeems_named_account_with_notice(self):
        second = self.home('second')
        self.register({'codex': (self.default, 20, 24), 'codex-second': (second, 95, 48)})
        self.behave({'codex-home': {'used': 20}, 'second': {'used': 95, 'reset_credits': 1}})
        with self.codex_path():
            self.reset_card('codex-second', 1)
            self.assertEqual(self.tap('Spend reset'), 'control_callback')
            queued, = self.store.service_requests_pending()
            self.assertEqual((queued['op'], json.loads(queued['params'])),
                             ('account.codex_reset', {'alias': 'codex-second', '_topic': TOPIC}))
            self.assertFalse(any(call['method'] == 'account/rateLimitResetCredit/consume' for call in self.calls()))
            from coordinator.service import Service
            from coordinator.codex_accounts import redeem_codex_reset
            service = Service(self.store, object(), ProviderRunner(self.store.directory), self.root)
            with patch('coordinator.codex_accounts.redeem_codex_reset', wraps=redeem_codex_reset) as redeem:
                self.assertTrue(await service.controls_once())
            self.assertIs(redeem.call_args.kwargs['guarded'], True)
        consumed = [call for call in self.calls() if call['method'] == 'account/rateLimitResetCredit/consume']
        self.assertEqual(len(consumed), 1)
        self.assertEqual(Path(consumed[0]['home']), second)
        self.assertEqual(self.store.get('codex_account_status')['codex-second']['resets_available'], 0)
        self.assertEqual(sum('Codex banked reset redeemed for codex-second@example.com' in row[0]
                             for row in self.store.db.execute('SELECT text FROM outbox')), 1)
        self.assertEqual(self.store.db.execute('SELECT state FROM service_requests WHERE id=?',
                                              (queued['id'],)).fetchone()[0], 'done')

    async def test_repeated_confirm_redeems_only_once(self):
        self.register({'codex': (self.default, 96, 24)})
        self.behave({'codex-home': {'used': 96, 'reset_credits': 2}})
        with self.codex_path():
            confirm = self.reset_card('codex', 2)
            self.assertEqual(self.tap('Spend reset', confirm), 'control_callback')
            self.assertEqual(self.tap('Spend reset', confirm), 'stale_callback')
            queued, = self.store.service_requests_pending()
            self.assertEqual((queued['op'], json.loads(queued['params'])),
                             ('account.codex_reset', {'alias': 'codex', '_topic': TOPIC}))
            from coordinator.service import Service
            from coordinator.codex_accounts import redeem_codex_reset
            service = Service(self.store, object(), ProviderRunner(self.store.directory), self.root)
            with patch('coordinator.codex_accounts.redeem_codex_reset', wraps=redeem_codex_reset) as redeem:
                self.assertTrue(await service.controls_once())
            self.assertIs(redeem.call_args.kwargs['guarded'], True)
            self.assertFalse(await service.controls_once())
            self.store.close()
            self.store = Store(self.root / 'state')
            service = Service(self.store, object(), ProviderRunner(self.store.directory), self.root)
            self.assertFalse(await service.controls_once())
        consumed = [call for call in self.calls() if call['method'] == 'account/rateLimitResetCredit/consume']
        self.assertEqual(len(consumed), 1)
        self.assertEqual(self.store.db.execute('SELECT id,state FROM service_requests').fetchall()[0][:],
                         (queued['id'], 'done'))
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM service_requests').fetchone()[0], 1)
        self.assertEqual(sum('Codex banked reset redeemed for codex@example.com' in row[0]
                             for row in self.store.db.execute('SELECT text FROM outbox')), 1)

    async def test_confirmed_reset_refuses_live_usage_below_95_without_spending(self):
        self.register({'codex': (self.default, 96, 24)})
        self.behave({'codex-home': {'used': 94, 'reset_credits': 2}})
        with self.codex_path():
            self.reset_card('codex', 2)
            self.assertEqual(self.tap('Spend reset'), 'control_callback')
            from coordinator.service import Service
            service = Service(self.store, object(), ProviderRunner(self.store.directory), self.root)
            self.assertTrue(await service.controls_once())
        request, = self.store.db.execute('SELECT op,state,result FROM service_requests').fetchall()
        self.assertEqual(request[:2], ('account.codex_reset', 'refused'))
        self.assertEqual(json.loads(request['result'])['text'],
                         'This Codex account is below 95% on its resettable meters')
        self.assertFalse(any(call['method'] == 'account/rateLimitResetCredit/consume' for call in self.calls()))

    async def test_ctl_operation_and_mcp_tool_use_their_respective_accounts(self):
        second = self.home('second')
        self.register({'codex': (self.default, 96, 24), 'codex-second': (second, 95, 48)})
        self.behave({'codex-home': {'used': 96, 'reset_credits': 1},
                     'second': {'used': 95, 'reset_credits': 1}})
        self.assertIn('account_redeem', mcp.TOOLS)
        self.assertEqual(mcp.TOOLS['account_redeem'].params, {})
        self.assertNotIn('account_codex_reset', mcp.TOOLS)
        with self.codex_path():
            result = subprocess.run([sys.executable, '-m', 'coordinator', '--state-dir', str(self.store.directory),
                                     'ctl', 'call', 'account.codex_reset', 'alias=codex-second'],
                                    capture_output=True, text=True, check=False, env=os.environ.copy())
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue(json.loads(result.stdout)['ok'])
            response = mcp.dispatch(self.store, {'method': 'tools/call',
                                                 'params': {'name': 'account_redeem', 'arguments': {}}})
        self.assertFalse(response['isError'])
        consumed = [Path(call['home']).name for call in self.calls()
                    if call['method'] == 'account/rateLimitResetCredit/consume']
        self.assertEqual(consumed, ['second', 'codex-home'])
class CodexStatusTests(CodexFixture):
    async def test_reset_details_keep_only_available_credits_in_expiry_order(self):
        details = self.reset_details()
        self.behave({'codex-home': {'reset_credits': 5, 'reset_details': details}})
        status = await collect_codex_status({'config_dir': str(self.default)}, binary=str(self.binary))
        self.assertEqual(status['resets_available'], 5)
        self.assertEqual(status['reset_credits'], [
            {'id': 'early', 'expires_at': float(details[1]['expiresAt'])},
            {'id': 'late', 'expires_at': float(details[0]['expiresAt'])},
            {'id': 'never', 'expires_at': None}])
        self.assertEqual(status['reset_expires'], details[1]['expiresAt'])

    async def test_nonexpiring_reset_credit_has_no_reset_expiry(self):
        self.behave({'codex-home': {'reset_credits': 1, 'reset_details': [
            {'id': 'never', 'status': 'available', 'expiresAt': None}]}})
        status = await collect_codex_status({'config_dir': str(self.default)}, binary=str(self.binary))
        self.assertEqual(status['reset_credits'], [{'id': 'never', 'expires_at': None}])
        self.assertIsNone(status['reset_expires'])

    async def test_reset_details_are_ignored_when_schema_support_is_off(self):
        self.behave({'codex-home': {'reset_credits': 3, 'reset_details': self.reset_details()}})
        with patch.dict(RESET_CREDIT_SCHEMA, details=False):
            status = await collect_codex_status({'config_dir': str(self.default)}, binary=str(self.binary))
        self.assertEqual(status['resets_available'], 3)
        self.assertNotIn('reset_credits', status)
        self.assertIsNone(status['reset_expires'])

    async def test_signed_out_refresh_keeps_the_last_email_and_drops_usage(self):
        identity = {'email': 'Owner@Example.com', 'type': 'ChatGPT Pro', 'name': 'Owner', 'logged_in': True}
        for signed_out in ({'logged_in': False}, {'email': '', 'logged_in': False}, {}, None):
            with self.subTest(identity=signed_out):
                self.register({'codex': (self.default, 97, 24)})
                with self.store.db:
                    self.store.put('codex_account_status', {'codex': {'identity': identity, 'observed_at': 100,
                        'usage': {'seven_day': {'utilization': 97}}, 'resets_available': 2}})

                async def collect(account):
                    result = {'checked_at': 200, 'error': 'login_required'}
                    if signed_out is not None:
                        result['identity'] = signed_out
                    return result

                await refresh_codex_accounts(self.store, collect)
                snapshot = self.store.get('codex_account_status')['codex']
                self.assertEqual(snapshot['identity'], dict(identity, logged_in=False))
                self.assertNotIn('usage', snapshot)
                self.assertNotIn('observed_at', snapshot)
                self.assertNotIn('resets_available', snapshot)
                self.assertEqual(snapshot['error'], 'login_required')
                self.assertFalse(CodexBroker(self.store).authenticated('codex'))
                self.assertEqual(CodexBroker(self.store).label('codex'), 'Owner@Example.com')

    async def test_a_failed_refresh_keeps_the_signed_in_identity_and_usage(self):
        self.register({'codex': (self.default, 97, 24)}, now=100)
        previous = self.store.get('codex_account_status')['codex']

        async def collect(account):
            return {'checked_at': 200, 'error': 'timeout'}

        await refresh_codex_accounts(self.store, collect)
        snapshot = self.store.get('codex_account_status')['codex']
        self.assertEqual(snapshot, dict(previous, checked_at=200, error='timeout'))
        self.assertTrue(CodexBroker(self.store).authenticated('codex'))

    async def test_discovery_does_not_readd_the_default_home_after_codex_is_repointed(self):
        new_home = self.home('repointed')
        self.register({'codex': (new_home, 10, 24)})
        with self.store.db:
            accounts = self.store.get('codex_accounts')
            accounts['codex']['previous_config_dirs'] = [str(self.default / '..' / self.default.name)]
            self.store.put('codex_accounts', accounts)
        for attempt in range(2):
            with self.subTest(attempt=attempt):
                self.assertEqual(discover_codex_accounts(self.store), accounts)
                self.assertEqual(self.store.get('codex_accounts'), accounts)
                self.assertEqual(CodexBroker(self.store).active(), 'codex')
                self.assertEqual(CodexBroker(self.store).select(), 'codex')
        self.assertTrue((self.default / 'auth.json').is_file())
        self.assertTrue((self.default / 'sessions').is_dir())

    async def test_discovery_skips_the_removed_default_home(self):
        with self.store.db:
            self.store.put('account_dirs_removed', [str(self.default / '..' / self.default.name)])
        self.assertEqual(discover_codex_accounts(self.store), {})
        self.assertEqual(self.store.get('codex_accounts', {}), {})
        self.assertTrue((self.default / 'auth.json').is_file())

    async def test_limit_notice_points_to_the_reset_button(self):
        self.register({'codex': (self.default, 80, 24)})
        CodexBroker(self.store).notify_limit('codex', rejected=True)
        self.assertEqual(len(self.notices()), 1)
        self.assertIn('tap Reset in /accounts', self.notices()[0])
        self.assertNotIn('/accounts codex reset', self.notices()[0])

    async def test_the_usage_check_reads_login_windows_credits_and_resets_without_a_model_turn(self):
        reset = int(time.time()) + 3 * 86400
        self.behave({'codex-home': {'email': 'Owner@Example.com', 'used': 58, 'reset_at': reset, 'reset_credits': 2}})
        status = await collect_codex_status({'config_dir': str(self.default)}, binary=str(self.binary))
        self.assertEqual(status['identity'], {'email': 'Owner@Example.com', 'type': 'ChatGPT Pro', 'logged_in': True})
        self.assertEqual(status['usage'], {'seven_day': {'utilization': 58, 'resets_at': reset}})
        self.assertEqual(status['credits'], {'has_credits': True, 'unlimited': False, 'balance': '12.5'})
        self.assertEqual(status['resets_available'], 2)
        self.assertIs(status['usage_allowed'], True)
        self.assertNotIn('error', status)
        self.assertEqual([call['method'] for call in self.calls()],
                         ['initialize', 'account/read', 'account/rateLimits/read'])

    async def test_the_usage_check_starts_codex_without_plugins_so_no_marketplace_refresh_is_left_behind(self):
        self.behave({'codex-home': {'email': 'owner@example.com', 'used': 10}})
        await collect_codex_status({'config_dir': str(self.default)}, binary=str(self.binary))
        self.assertEqual({tuple(call['argv']) for call in self.calls()},
                         {('app-server', '--listen', 'stdio://', '--disable', 'plugins')})

    async def test_the_usage_check_names_the_five_hour_and_weekly_windows_by_length(self):
        self.behave({'codex-home': {'email': 'owner@example.com', 'used': 40, 'five_hour': 96}})
        status = await collect_codex_status({'config_dir': str(self.default)}, binary=str(self.binary))
        self.assertEqual((status['usage']['five_hour']['utilization'], status['usage']['seven_day']['utilization']),
                         (96, 40))

    async def test_a_failed_first_check_is_retried_soon_but_a_missing_login_is_not(self):
        discover_codex_accounts(self.store)

        async def timeout(account):
            return {'checked_at': time.time(), 'error': 'timeout'}

        async def signed_out(account):
            return {'checked_at': time.time(), 'identity': {'logged_in': False}, 'error': 'login_required'}
        self.assertTrue(await refresh_codex_accounts(self.store, timeout))
        self.assertFalse(await refresh_codex_accounts(self.store, signed_out))

    async def test_the_existing_home_takes_a_free_name_when_a_claude_account_is_named_codex(self):
        with self.store.db:
            self.store.put('accounts', {'codex': {'config_dir': str(self.root / 'claude-codex'), 'enabled': True}})
        discover_codex_accounts(self.store)
        self.assertEqual(list(self.store.get('codex_accounts')), ['codex-2'])

    async def test_a_signed_out_home_reports_that_sign_in_is_needed(self):
        home = self.home('second', signed_in=False)
        status = await collect_codex_status({'config_dir': str(home)}, binary=str(self.binary))
        self.assertEqual((status['error'], status['identity']['logged_in']), ('login_required', False))

    async def test_the_existing_home_becomes_the_first_account_once_its_login_is_confirmed(self):
        discover_codex_accounts(self.store)
        self.assertEqual(self.store.get('codex_accounts'),
                         {'codex': {'config_dir': str(self.default), 'enabled': False, 'awaiting_login': True}})
        self.behave({'codex-home': {'email': 'owner@example.com'}})
        await refresh_codex_accounts(self.store, lambda account: collect_codex_status(account, binary=str(self.binary)))
        self.assertEqual(self.store.get('codex_accounts'),
                         {'codex': {'config_dir': str(self.default), 'enabled': True}})
        self.assertEqual(CodexBroker(self.store).select(), 'codex')
        discover_codex_accounts(self.store)
        self.assertEqual(list(self.store.get('codex_accounts')), ['codex'])

    async def test_a_default_home_without_a_login_is_not_registered(self):
        (self.default / 'auth.json').unlink()
        discover_codex_accounts(self.store)
        self.assertEqual(self.store.get('codex_accounts', {}), {})


class CodexSignInTests(CodexFixture):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.number = 100
        self.driver = SignIns(self.store, 'claude', codex_binary=str(self.binary), poll=0.01)
        self.task = asyncio.ensure_future(self.driver.run())
        self.behave({})

    async def asyncTearDown(self):
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)
        await super().asyncTearDown()

    def add(self):
        result = control_api.call(self.store, 'account.add', {'provider': 'codex'},
                                  topic=TOPIC, source='telegram')
        self.assertTrue(result.ok, result.text)
        return result

    def click(self, data):
        self.number += 1
        return self.store.accept({'update_id': self.number, 'callback_query': {
            'id': str(self.number), 'from': {'id': OWNER}, 'data': data,
            'message': {'message_id': 900, 'message_thread_id': 4, 'chat': {'id': -10042, 'type': 'supergroup'}}}})

    def texts(self):
        return [row[0] for row in self.store.db.execute('SELECT text FROM outbox ORDER BY id')]

    async def card(self):
        await until(lambda: any(DEVICE_CODE in text for text in self.texts()), 'the device-code card')
        row = self.store.db.execute('SELECT * FROM outbox WHERE text LIKE ? ORDER BY id DESC',
                                    ('%' + DEVICE_CODE + '%',)).fetchone()
        return row['text'], json.loads(row['reply_markup'])

    def folder(self):
        return next(Path(call['home']) for call in self.calls() if call['method'] == 'account/login/start')

    async def test_codex_sign_in_posts_the_device_link_and_code_and_adds_the_account_by_email(self):
        leaked = patch.dict(os.environ, {'CODEX_API_KEY': 'x', 'CODEX_ACCESS_TOKEN': 'x'})
        leaked.start()
        self.addCleanup(leaked.stop)
        started = self.add()
        self.assertIn('Starting Codex sign-in', started.text)
        text, markup = await self.card()
        self.assertIn(DEVICE_URL, text)
        self.assertIn(DEVICE_CODE, text)
        self.assertEqual(markup['inline_keyboard'][0][0]['text'], 'Cancel sign-in')
        folder = self.folder()
        self.behave({folder.name: {'email': 'Jane.Doe@example.com'}})
        (self.bin / 'approve').write_text('1')
        await until(lambda: self.store.get(KEY) is None, 'the sign-in to end')
        self.assertEqual(folder.parent, self.added)
        self.assertTrue(folder.name.startswith('.torii-'))
        self.assertEqual(self.store.get('codex_accounts'),
                         {'codex-Jane-Doe': {'config_dir': str(folder), 'enabled': True}})
        self.assertEqual((folder / 'sessions').resolve(), (self.default / 'sessions').resolve())
        self.assertEqual(oct(folder.stat().st_mode & 0o777), '0o700')
        self.assertIn('Codex account Jane.Doe@example.com is signed in and enabled', self.texts()[-1])
        self.assertIn('It is now the active Codex account.', self.texts()[-1])
        self.assertEqual(self.store.get('codex_active_account'), 'codex-Jane-Doe')
        self.assertEqual(CodexBroker(self.store).parent_account(), 'codex-Jane-Doe')
        self.assertEqual({tuple(call['credentials']) for call in self.calls()}, {()})
        self.assertEqual(self.store.get('accounts', {}), {})

    async def test_adding_a_codex_account_replaces_a_signed_out_pick(self):
        self.register({'codex': (self.default, 50, 72)})
        status = self.store.get('codex_account_status')
        status['codex'].update(error='login_required')
        self.store.put('codex_account_status', status)
        self.store.put('codex_active_account', 'codex')
        self.add()
        await self.card()
        self.behave({self.folder().name: {'email': 'new@example.com'}})
        (self.bin / 'approve').write_text('1')
        await until(lambda: self.store.get(KEY) is None, 'the sign-in to end')
        self.assertEqual(self.store.get('codex_active_account'), 'codex-new')
        self.assertEqual(CodexBroker(self.store).parent_account(), 'codex-new')
        self.assertIn('It is now the active Codex account.', self.texts()[-1])

    async def test_adding_a_codex_account_replaces_a_missing_pick(self):
        self.store.put('codex_active_account', 'missing')
        self.add()
        await self.card()
        self.behave({self.folder().name: {'email': 'new@example.com'}})
        (self.bin / 'approve').write_text('1')
        await until(lambda: self.store.get(KEY) is None, 'the sign-in to end')
        self.assertEqual(self.store.get('codex_active_account'), 'codex-new')
        self.assertEqual(CodexBroker(self.store).parent_account(), 'codex-new')
        self.assertIn('It is now the active Codex account.', self.texts()[-1])

    async def test_adding_a_codex_account_preserves_a_signed_in_pick(self):
        self.register({'codex': (self.default, 50, 72)})
        self.store.put('codex_active_account', 'codex')
        self.add()
        await self.card()
        self.behave({self.folder().name: {'email': 'new@example.com'}})
        (self.bin / 'approve').write_text('1')
        await until(lambda: self.store.get(KEY) is None, 'the sign-in to end')
        self.assertIn('codex-new', self.store.get('codex_accounts'))
        self.assertEqual(self.store.get('codex_active_account'), 'codex')
        self.assertEqual(CodexBroker(self.store).parent_account(), 'codex')
        self.assertNotIn('It is now the active Codex account.', self.texts()[-1])

    async def test_adding_a_codex_account_keeps_a_signed_in_default_without_a_pick(self):
        self.register({'codex': (self.default, 50, 72)})
        self.assertIsNone(self.store.get('codex_active_account'))
        self.assertEqual(CodexBroker(self.store).parent_account(), 'codex')
        self.add()
        await self.card()
        self.behave({self.folder().name: {'email': 'new@example.com'}})
        (self.bin / 'approve').write_text('1')
        await until(lambda: self.store.get(KEY) is None, 'the sign-in to end')
        self.assertIn('codex-new', self.store.get('codex_accounts'))
        self.assertIsNone(self.store.get('codex_active_account'))
        broker = CodexBroker(self.store)
        self.assertEqual(broker.parent_account(), 'codex')
        self.assertEqual(broker.current(), 'codex')
        self.assertNotIn('It is now the active Codex account.', self.texts()[-1])

    async def test_adding_a_codex_account_keeps_the_default_parent_with_automatic_switching(self):
        self.register({'codex': (self.default, 50, 72)})
        self.automatic()
        self.assertIsNone(self.store.get('codex_active_account'))
        self.assertEqual(CodexBroker(self.store).parent_account(), 'codex')
        self.add()
        await self.card()
        self.behave({self.folder().name: {'email': 'new@example.com'}})
        (self.bin / 'approve').write_text('1')
        await until(lambda: self.store.get(KEY) is None, 'the sign-in to end')
        self.assertIn('codex-new', self.store.get('codex_accounts'))
        self.assertIsNone(self.store.get('codex_active_account'))
        self.assertEqual(CodexBroker(self.store).parent_account(), 'codex')
        self.assertNotIn('It is now the active Codex account.', self.texts()[-1])

    async def test_adding_a_codex_account_replaces_a_signed_out_default_without_a_pick(self):
        self.register({'codex': (self.default, 50, 72)})
        self.store.put('codex_account_status', {
            'codex': {'identity': {'logged_in': False}, 'error': 'login_required'}})
        self.assertIsNone(self.store.get('codex_active_account'))
        self.assertIsNone(CodexBroker(self.store).parent_account())
        self.add()
        await self.card()
        self.behave({self.folder().name: {'email': 'new@example.com'}})
        (self.bin / 'approve').write_text('1')
        await until(lambda: self.store.get(KEY) is None, 'the sign-in to end')
        self.assertEqual(self.store.get('codex_active_account'), 'codex-new')
        self.assertEqual(CodexBroker(self.store).parent_account(), 'codex-new')
        self.assertIn('It is now the active Codex account.', self.texts()[-1])

    async def test_cancelling_a_codex_sign_in_stops_the_login_and_adds_nothing(self):
        self.add()
        _, markup = await self.card()
        self.assertEqual(self.click(markup['inline_keyboard'][0][0]['callback_data']), 'control_callback')
        await until(lambda: self.store.get(KEY) is None, 'the sign-in to end')
        self.assertIn('account/login/cancel', [call['method'] for call in self.calls()])
        self.assertEqual(self.store.get('codex_accounts', {}), {})
        self.assertIn('Codex sign-in cancelled', self.texts()[-1])
        self.assertFalse(self.folder().exists())
        self.assertTrue((self.default / 'sessions').is_dir())

    async def test_a_cancel_while_codex_starts_stops_the_sign_in_before_a_code_is_shown(self):
        (self.bin / 'slow').write_text('1')
        self.add()
        await until(lambda: any(call['method'] == 'initialize' for call in self.calls()), 'Codex to start')
        self.assertTrue(control_api.call(self.store, 'account.signin_cancel',
                                         topic=TOPIC, source='telegram').ok)
        await until(lambda: self.store.get(KEY) is None, 'the sign-in to end')
        self.assertFalse(any(DEVICE_CODE in text for text in self.texts()))
        self.assertIn('Codex sign-in cancelled', self.texts()[-1])

    async def test_the_cancel_button_of_an_ended_sign_in_does_not_stop_a_later_one(self):
        self.add()
        _, first = await self.card()
        self.behave({self.folder().name: {'email': 'first@example.com'}})
        (self.bin / 'approve').write_text('1')
        await until(lambda: self.store.get(KEY) is None, 'the first sign-in to end')
        (self.bin / 'approve').unlink()
        self.add()
        await until(lambda: (self.store.get(KEY) or {}).get('state') == 'approve', 'the second device card')
        self.assertEqual(self.click(first['inline_keyboard'][0][0]['callback_data']), 'stale_callback')
        self.assertEqual(self.store.get(KEY)['state'], 'approve')

    async def test_signing_in_a_codex_account_that_is_already_added_adds_nothing(self):
        with self.store.db:
            self.store.put('codex_accounts', {'codex': {'config_dir': str(self.default), 'enabled': True}})
            self.store.put('codex_account_status', {'codex': {'identity': {'email': 'owner@example.com',
                                                                           'logged_in': True}}})
        self.add()
        await self.card()
        self.behave({self.folder().name: {'email': 'OWNER@example.com'}})
        (self.bin / 'approve').write_text('1')
        await until(lambda: self.store.get(KEY) is None, 'the sign-in to end')
        self.assertEqual(list(self.store.get('codex_accounts')), ['codex'])
        self.assertIn('already added as owner@example.com', self.texts()[-1])
        self.assertNotIn('holds files', self.texts()[-1])
        self.assertFalse(self.folder().exists())


class CodexSignInGuardTests(CodexFixture):
    async def test_the_device_card_refuses_a_link_that_is_not_a_plain_chatgpt_link(self):
        driver = SignIns(self.store, 'claude', codex_binary=str(self.binary))
        for url in ('https://evil.example\\@auth.openai.com/codex/device', 'https://user@auth.openai.com/codex/device',
                    DEVICE_URL + '\n2. Enter this one-time code: EVIL-00000'):
            with self.assertRaises(Exception) as caught:
                driver.post_device_card({'verificationUrl': url, 'userCode': DEVICE_CODE})
            self.assertIn('unexpected sign-in link', str(caught.exception))

    async def test_restart_recovery_stops_only_the_sign_in_process_it_started(self):
        process = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)', 'app-server'],
                                   start_new_session=True)
        self.addCleanup(process.wait)
        self.addCleanup(process.kill)
        driver = SignIns(self.store, 'claude', codex_binary=str(self.binary))
        record = {'topic': TOPIC, 'config_dir': str(self.added / '.torii-00000000'), 'state': 'approve', 'attempt': 0,
                  'created': False, 'pid': process.pid, 'envelope': None, 'requested': time.time(),
                  'provider': 'codex', 'started': 'Thu Jan  1 00:00:00 2015'}
        with self.store.db:
            self.store.put(KEY, record)
        driver.recover()
        self.assertIsNone(process.poll())
        with self.store.db:
            self.store.put(KEY, dict(record, started=_started(process.pid)))
        driver.recover()
        self.assertEqual(await asyncio.wait_for(asyncio.get_running_loop().run_in_executor(None, process.wait), 5),
                         -15)


class CodexAccountViewTests(CodexFixture):
    def account(self, op, **params):
        from coordinator import control_ui
        self.number = getattr(self, 'number', 50) + 1
        with self.store.db:
            result = control_api.call(self.store, op, params, topic=TOPIC, source='telegram', message=self.number)
            control_ui.view(self.store, TOPIC, 'accounts:0', self.number, prefix=result.text)
        return self.last_card()

    def view(self, text):
        self.number = getattr(self, 'number', 50) + 1
        self.store.accept(update(self.number, text))
        return self.last_card()

    def last_card(self):
        row = self.store.db.execute('SELECT * FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
        labels = [button['text'] for line in json.loads(row['reply_markup'] or '{"inline_keyboard": []}')['inline_keyboard']
                  for button in line]
        return row['text'], labels

    def texts(self):
        return [row[0] for row in self.store.db.execute('SELECT text FROM outbox ORDER BY id')]

    async def test_accounts_lists_codex_accounts_by_email_and_offers_a_codex_sign_in(self):
        second = self.home('second')
        self.register({'codex': (self.default, 40, 72), 'codex-second': (second, 97, 24)})
        status = self.store.get('codex_account_status')
        status['codex'].update(credits={'has_credits': True, 'unlimited': False, 'balance': '12.5'},
                               resets_available=2, identity={'email': 'owner@example.com', 'logged_in': True,
                                                             'type': 'ChatGPT Pro'})
        with self.store.db:
            self.store.put('codex_account_status', status)
        from coordinator import control_ui
        with self.store.db:
            control_ui.view(self.store, TOPIC, 'accounts:0')
        row = self.store.db.execute('SELECT * FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
        labels = [button['text'] for line in json.loads(row['reply_markup'])['inline_keyboard'] for button in line]
        self.assertIn('**ChatGPT** · auto-switch off (left: 5h · week)\n🟢 owner@example.com: ? · 60% · active · 2 resets',
                      row['text'])
        self.assertIn('Turn on ChatGPT auto-switch', labels)
        self.assertIn('🟢 codex-second@example.com: ? · 3%', row['text'])
        self.assertEqual(labels[:2], ['Use codex-second@example.com for ChatGPT', 'Reset owner@example.com (2)'])
        self.assertIn('Add ChatGPT', labels)
        self.assertIn('Add Claude', labels)
        text, _ = self.view('/accounts')
        self.assertIn('auto-switch off (left: 5h · week)\n🟢 owner@example.com: ? · 60% · active', text)
        self.assertIn('🟢 codex-second@example.com: ? · 3%', text)
        card = control_api.call(self.store, 'account.show', {'alias': 'codex'}).text
        self.assertIn('Type: ChatGPT Pro', card)
        self.assertIn('Weekly: ', card)
        self.assertIn('Credits: 12.5', card)
        self.assertIn('Usage resets available: 2', card)
        reply, _ = self.account('account.disable', alias='codex-second')
        self.assertFalse(self.store.get('codex_accounts')['codex-second']['enabled'])
        self.assertIn('⚫ codex-second@example.com: off', reply)
        self.assertEqual(self.tap('Turn on codex-second@example.com'), 'control_callback')
        self.assertEqual(self.tap('Turn on'), 'control_callback')
        self.assertTrue(self.store.get('codex_accounts')['codex-second']['enabled'])
        self.assertIn('🟢 codex-second@example.com: ? · 3%', self.last_card()[0])
        reply, _ = self.account('account.add', alias='codex')
        self.assertIn('owner@example.com is already signed in.', reply)
        self.assertNotIn('codex login', reply)
        status = self.store.get('codex_account_status')
        status['codex-second']['error'] = 'login_required'
        with self.store.db:
            self.store.put('codex_account_status', status)
        reply, labels = self.view('/accounts')
        self.assertIn('⚪ codex-second@example.com: signed out', reply)
        self.assertIn('Sign in codex-second@example.com', labels)
        self.assertNotIn('/accounts enable', reply)
        self.assertEqual(self.tap('Sign in codex-second@example.com'), 'control_callback')
        self.assertEqual(self.tap('Sign in'), 'control_callback')
        record = self.store.get(KEY)
        self.assertEqual((record['provider'], record['target']), ('codex', 'codex-second'))

    async def test_the_owner_picks_the_active_codex_account_and_turns_automatic_switching_on_from_accounts(self):
        second = self.home('second')
        self.register({'codex': (self.default, 40, 72), 'codex-second': (second, 20, 24)})
        _, labels = self.view('/accounts')
        self.assertIn('Use codex-second@example.com for ChatGPT', labels)
        self.assertNotIn('Use codex@example.com for ChatGPT', labels)
        self.assertEqual(self.tap('Use codex-second@example.com for ChatGPT'), 'control_callback')
        self.assertIn('🟢 codex-second@example.com: ? · 80% · active', self.last_card()[0])
        self.assertEqual(self.store.get('codex_active_account'), 'codex-second')
        self.assertEqual(self.tap('Turn on ChatGPT auto-switch'), 'control_callback')
        reply, labels = self.last_card()
        self.assertIn('**ChatGPT** · auto-switch on', reply)
        self.assertNotIn('Use codex@example.com for ChatGPT', labels)
        self.assertIs(self.store.get('codex_auto_switch'), True)
        from coordinator import control_ui
        with self.store.db:
            control_ui.view(self.store, TOPIC, 'accounts:0')
        row = self.store.db.execute('SELECT * FROM outbox ORDER BY id DESC LIMIT 1').fetchone()
        self.assertIn('**ChatGPT** · auto-switch on', row['text'])
        self.assertIn('Turn off ChatGPT auto-switch',
                      [button['text'] for line in json.loads(row['reply_markup'])['inline_keyboard'] for button in line])
        self.assertEqual(self.tap('Turn off ChatGPT auto-switch'), 'control_callback')
        reply, labels = self.last_card()
        self.assertIn('auto-switch off (left: 5h · week)\n🟢 codex-second@example.com: ? · 80% · active', reply)
        self.assertIn('Use codex@example.com for ChatGPT', labels)
        self.assertIs(self.store.get('codex_auto_switch'), False)
        with self.store.db:
            settings = control_api.call(self.store, 'settings.get', {})
        self.assertEqual((settings.data['codex_auto_switch'], settings.data['codex_active_account']),
                         (False, 'codex-second@example.com'))

    async def test_adding_a_codex_account_shows_the_account_list_not_the_first_account(self):
        self.register({'codex': (self.default, 40, 72)})
        self.view('/accounts')
        self.assertEqual(self.tap('Add ChatGPT'), 'control_callback')
        text, labels = self.last_card()
        self.assertIn('Sign-in open: ChatGPT, starting.', text)
        self.assertNotIn('Account:', text)
        self.assertIn('Cancel sign-in', labels)
        self.assertNotIn('Disable account', labels)

    async def test_delegation_view_reports_when_every_codex_account_is_limited(self):
        from coordinator import control_ui
        self.automatic()
        self.register({'codex': (self.default, 97, 2)})
        with self.store.db:
            control_ui.view(self.store, TOPIC, 'delegation')
        self.assertIn('every Codex account is limited, back in 1h', self.texts()[-1])
        self.register({'codex': (self.default, 20, 2)})
        with self.store.db:
            control_ui.view(self.store, TOPIC, 'delegation')
        self.assertNotIn('limited', self.texts()[-1])
        self.automatic(False)
        with self.store.db:
            self.store.put('codex_account_blocks', {'codex': {'until': time.time() + 2 * 3600, 'reason': 'quota'}})
            control_ui.view(self.store, TOPIC, 'delegation')
        self.assertIn('the active Codex account codex@example.com is limited, back in 1h', self.texts()[-1])


if __name__ == '__main__':
    unittest.main()
