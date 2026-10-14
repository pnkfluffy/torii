import asyncio
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from coordinator.account_status import collect_status, normalize_usage, refresh_accounts, monitor_accounts, check_accounts
from coordinator.store import Store
from tests.support import until_text


class FileLoginAccountStatusTestsSupport:
    pass


class FileLoginAccountStatusTests(FileLoginAccountStatusTestsSupport, unittest.IsolatedAsyncioTestCase):
    pass


class AccountStatusTests(unittest.IsolatedAsyncioTestCase):
    def pick_store(self, directory):
        root = Path(directory)
        store = Store(root / 'state')
        accounts, status = {}, {}
        for alias in ('a', 'b'):
            profile = root / alias
            profile.mkdir()
            accounts[alias] = {'config_dir': str(profile), 'enabled': True}
            status[alias] = {'identity': {'email': alias + '@example.com', 'logged_in': True},
                             'observed_at': 1000, 'usage': {key: {'utilization': 98 if alias == 'b' else 1,
                                 'resets_at': 4600 if alias == 'a' else 8200}
                                 for key in ('five_hour', 'seven_day', 'seven_day_fable')}}
        with store.db:
            store.put('accounts', accounts)
            store.put('account_status', status)
            store.put('account_blocks', {'b': {'until': 8200, 'reason': 'quota', 'observed_at': 1000}})
            store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('home',1,1,'Atlas','',1)")
            store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('pick',1,2,'Docs','',1)")
            store.put('coordinator_home_topic', 'home')
        self.addCleanup(store.close)
        return store

    async def test_pick_fresh_read_replaces_same_window_high_usage_clears_hold_and_reports_to_origin(self):
        from coordinator import control_api
        from coordinator.accounts import AccountBroker
        with tempfile.TemporaryDirectory() as directory, patch('time.time', return_value=1000):
            store = self.pick_store(directory)
            result = control_api.call(store, 'account.use', {'alias': 'b'}, topic='pick')
            self.assertTrue(result.ok, result.text)
            async def collect(account):
                return {'observed_at': 1000, 'usage': {key: {'utilization': 1, 'resets_at': 8200}
                        for key in ('five_hour', 'seven_day', 'seven_day_fable')}}
            await refresh_accounts(store, collect, ['b'])
            self.assertEqual(store.get('account_status')['b']['usage']['seven_day']['utilization'], 1)
            self.assertEqual(store.get('account_blocks'), {})
            self.assertEqual(AccountBroker(store).select(), 'b')
            rows = [tuple(row) for row in store.db.execute('SELECT topic,text FROM outbox ORDER BY id')]
            self.assertEqual(rows, [('pick', 'b@example.com has room: Claude work now uses b@example.com; running Claude chats move after their current turn.')])
            await refresh_accounts(store, collect, ['b'])
            self.assertEqual(store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 1)
            AccountBroker(store).record_rate_limit('b', {'rateLimitType': 'five_hour', 'status': 'rejected',
                                                        'resetsAt': 8200}, rejected=True)
            self.assertEqual(AccountBroker(store).select(), 'a')
            self.assertIn('b', store.get('account_blocks'))
            store.close()

    async def test_pick_still_full_reports_fallback_once_without_duplicate_weekly_notice(self):
        from coordinator import control_api
        from coordinator.accounts import AccountBroker
        with tempfile.TemporaryDirectory() as directory, patch('time.time', return_value=1000):
            store = self.pick_store(directory)
            self.assertTrue(control_api.call(store, 'account.use', {'alias': 'b'}).ok)
            async def collect(account):
                return {'observed_at': 1000, 'usage': {key: {'utilization': 98, 'resets_at': 8200}
                        for key in ('five_hour', 'seven_day', 'seven_day_fable')}}
            await refresh_accounts(store, collect, ['b'])
            rows = [tuple(row) for row in store.db.execute('SELECT topic,text FROM outbox ORDER BY id')]
            self.assertEqual(rows, [('home', 'b@example.com is still at 98% of its all-model weekly limit after a fresh check, back in 2h 0m; Claude work stays on a@example.com until then and moves to b@example.com after.')])
            await refresh_accounts(store, collect, ['b'])
            AccountBroker(store).activate()
            self.assertEqual(store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 1)
            self.assertIn('b', store.get('account_blocks'))
            store.close()

    async def test_failed_pick_read_keeps_trust_request_until_success_without_clearing_hold(self):
        from coordinator import control_api
        from coordinator.accounts import AccountBroker
        with tempfile.TemporaryDirectory() as directory, patch('time.time', return_value=1000):
            store = self.pick_store(directory)
            self.assertTrue(control_api.call(store, 'account.use', {'alias': 'b'}, topic='pick').ok)
            request = store.get('claude_pick_refresh')
            async def failed(account):
                return {'error': 'timeout', 'checked_at': 1000}
            self.assertTrue(await refresh_accounts(store, failed, ['b']))
            self.assertEqual(store.get('claude_pick_refresh'), request)
            self.assertEqual(AccountBroker(store).select(), 'a')
            self.assertIn('b', store.get('account_blocks'))
            self.assertEqual(store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 0)
            async def recovered(account):
                return {'observed_at': 1000, 'usage': {key: {'utilization': 1, 'resets_at': 8200}
                        for key in ('five_hour', 'seven_day', 'seven_day_fable')}}
            await refresh_accounts(store, recovered, ['b'])
            self.assertEqual(AccountBroker(store).select(), 'b')
            self.assertIsNone(store.get('claude_pick_refresh'))
            self.assertEqual(store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 1)
            store.close()

    async def test_weekly_fresh_feedback_goes_to_origin_and_pause_notice_once_to_main(self):
        from coordinator import control_api
        from coordinator.accounts import AccountBroker
        with tempfile.TemporaryDirectory() as directory, patch('time.time', return_value=1000):
            store = self.pick_store(directory)
            self.assertTrue(control_api.call(store, 'account.use', {'alias': 'b'}, topic='pick').ok)
            async def collect(account):
                return {'observed_at': 1000, 'usage': {'seven_day': {'utilization': 98, 'resets_at': 8200}}}
            await refresh_accounts(store, collect, ['b'])
            rows = [tuple(row) for row in store.db.execute('SELECT topic,text FROM outbox ORDER BY id')]
            self.assertEqual(rows, [
                ('pick', 'b@example.com is still at 98% of its all-model weekly limit after a fresh check, back in 2h 0m; Claude work stays on a@example.com until then and moves to b@example.com after.'),
                ('home', "b@example.com's all-model weekly is used up, back in 2h 0m; a@example.com takes Claude work meanwhile.")])
            await refresh_accounts(store, collect, ['b'])
            AccountBroker(store).activate()
            self.assertEqual(store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 2)
            store.close()

    async def test_new_pick_during_read_keeps_new_request_and_prevents_old_pick_feedback(self):
        from coordinator import control_api
        with tempfile.TemporaryDirectory() as directory, patch('time.time', return_value=1000):
            store = self.pick_store(directory)
            self.assertTrue(control_api.call(store, 'account.use', {'alias': 'b'}, topic='pick').ok)
            async def collect(account):
                self.assertTrue(control_api.call(store, 'account.use', {'alias': 'a'}, topic='home').ok)
                return {'observed_at': 1000, 'usage': {'seven_day': {'utilization': 1, 'resets_at': 8200}}}
            await refresh_accounts(store, collect, ['b'])
            self.assertEqual(store.get('claude_account_pick'), 'a')
            self.assertEqual(store.get('claude_pick_refresh')['alias'], 'a')
            self.assertEqual(store.get('account_status')['b']['usage']['seven_day']['utilization'], 98)
            self.assertEqual(store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 0)
            store.close()

    async def test_unpicked_refresh_still_keeps_same_window_high_usage_and_hold(self):
        from coordinator import control_api
        with tempfile.TemporaryDirectory() as directory, patch('time.time', return_value=1000):
            store = self.pick_store(directory)
            self.assertTrue(control_api.call(store, 'account.use', {'alias': 'a'}).ok)
            async def collect(account):
                return {'observed_at': 1000, 'usage': {'seven_day': {'utilization': 1, 'resets_at': 8200}}}
            await refresh_accounts(store, collect, ['b'])
            self.assertEqual(store.get('account_status')['b']['usage']['seven_day']['utilization'], 98)
            self.assertTrue(store.get('account_status')['b']['usage']['seven_day']['blocked'])
            self.assertIn('b', store.get('account_blocks'))
            store.close()

    async def test_monitor_checks_pick_before_other_accounts_without_waiting_for_deadline(self):
        from coordinator import control_api
        with tempfile.TemporaryDirectory() as directory, patch('time.time', return_value=1000):
            store = self.pick_store(directory)
            probes = []
            async def refresh(current, aliases=None):
                probes.append(aliases)
            async def tick(seconds):
                if len(probes) == 1:
                    self.assertTrue(control_api.call(store, 'account.use', {'alias': 'b'}).ok)
                else:
                    raise asyncio.CancelledError
            with patch('coordinator.account_status.refresh_accounts', refresh), \
                    patch('coordinator.account_status.asyncio.sleep', tick):
                with self.assertRaises(asyncio.CancelledError):
                    await monitor_accounts(store)
            self.assertEqual(probes, [None, ['b']])
            store.close()

    async def test_refresh_started_before_pick_cannot_consume_its_trusted_read(self):
        from coordinator import control_api
        with tempfile.TemporaryDirectory() as directory, patch('time.time', return_value=1000):
            store = self.pick_store(directory)
            async def collect(account):
                self.assertTrue(control_api.call(store, 'account.use', {'alias': 'b'}, topic='pick').ok)
                return {'observed_at': 1000, 'usage': {'seven_day': {'utilization': 1, 'resets_at': 8200}}}
            await refresh_accounts(store, collect, ['b'])
            self.assertEqual(store.get('account_status')['b']['usage']['seven_day']['utilization'], 98)
            self.assertEqual(store.get('claude_pick_refresh')['alias'], 'b')
            self.assertEqual(store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 0)
            store.close()

    async def test_legacy_placeholder_is_removed_without_touching_its_files(self):
        from coordinator.accounts import discover_accounts
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            home = root / 'home'
            home.mkdir()
            marker = home / 'history.jsonl'
            marker.write_text('{}\n')
            store = Store(root / 'state')
            with store.db:
                store.put('accounts', {'default': {'config_dir': str(home), 'enabled': True},
                                       'home-login-needs-name': {'config_dir': str(home), 'enabled': False}})
                store.put('account_status', {'default': {'identity': {'logged_in': False}},
                                             'home-login-needs-name': {'identity': {'logged_in': False}}})
            async def forbidden(account):
                self.fail('Unnamed home profile must not be checked')
            discover_accounts(store, root / 'missing')
            await refresh_accounts(store, forbidden)
            self.assertEqual(store.get('account_status', {}), {})
            self.assertEqual(store.get('accounts'), {})
            self.assertTrue(marker.is_file())
            store.close()

    async def test_half_added_account_enables_only_after_confirmed_login(self):
        from coordinator.accounts import AccountBroker, discover_accounts
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            profile = root / 'profiles' / 'work'
            profile.mkdir(parents=True)
            (profile / '.claude.json').write_text('{}')
            store = Store(root / 'state')
            accounts = discover_accounts(store, root / 'profiles')
            self.assertFalse(AccountBroker(store).available(accounts, 'work', set()))
            self.assertTrue(accounts['work']['awaiting_login'])
            async def missing(account):
                return {'identity': {'logged_in': False}, 'error': 'login_required'}
            await refresh_accounts(store, missing)
            self.assertFalse(store.get('accounts')['work']['enabled'])
            async def signed_in(account):
                return {'identity': {'email': 'work@example.com', 'logged_in': True}, 'usage': {},
                        'observed_at': time.time()}
            await refresh_accounts(store, signed_in)
            self.assertTrue(store.get('accounts')['work']['enabled'])
            self.assertTrue(AccountBroker(store).available(store.get('accounts'), 'work', set()))
            with store.db:
                accounts = store.get('accounts')
                accounts['work']['enabled'] = False
                store.put('accounts', accounts)
            await refresh_accounts(store, signed_in)
            self.assertFalse(store.get('accounts')['work']['enabled'])
            store.close()

    async def test_enabling_a_signed_out_account_asks_for_a_check_that_enables_it_after_login(self):
        from coordinator import control_api
        from coordinator.accounts import signed_in
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'work').mkdir()
            store = Store(root / 'state')
            with store.db:
                store.put('accounts', {'work': {'config_dir': str(root / 'work'), 'enabled': True}})
                store.put('account_status', {'work': {'identity': {'logged_in': False}, 'error': 'login_required'}})
                store.put('account_status_refresh_requested', 1.0)
                control_api.call(store, 'account.disable', {'alias': 'work'})
                result = control_api.call(store, 'account.enable', {'alias': 'work'})
            self.assertEqual(result.state, 'done')
            self.assertEqual(result.text, 'Checking sign-in for work. Torii enables it when the check passes.')
            self.assertEqual(store.get('accounts')['work'], {'config_dir': str(root / 'work'), 'enabled': False,
                                                             'awaiting_login': True})
            self.assertGreater(store.get('account_status_refresh_requested'), 1.0)
            async def logged_in(account):
                return {'identity': {'email': 'work@example.com', 'logged_in': True}, 'usage': {},
                        'observed_at': time.time()}
            await refresh_accounts(store, logged_in)
            self.assertTrue(signed_in(store, 'work', store.get('accounts')['work']))
            store.close()


    def test_normalize_native_windows_without_guessing_fable(self):
        data = {'five_hour': {'utilization': 12, 'resets_at': '2030-03-17T12:00:00Z'},
                'seven_day': {'utilization': 40},
                'model_scoped': [{'display_name': 'Fable', 'utilization': 25},
                                 {'display_name': 'Opus', 'utilization': 90}]}
        result = normalize_usage(data)
        self.assertEqual(result['five_hour']['utilization'], 12)
        self.assertEqual(result['seven_day']['utilization'], 40)
        self.assertEqual(result['seven_day_fable']['utilization'], 25)
        self.assertNotIn('seven_day_fable', normalize_usage({'model_scoped': [{'display_name': 'Opus', 'utilization': 90}]}))
        for value in (True, -1, 101, float('nan'), '90'):
            self.assertNotIn('five_hour', normalize_usage({'five_hour': {'utilization': value}}))

    async def test_control_only_fetch_keeps_stdin_open_and_sanitizes(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / 'claude'
            binary.write_text('''#!/usr/bin/env python3
import json, sys, time
if sys.argv[1:] == ['auth', 'status']:
 print(json.dumps({'loggedIn': True, 'email':'work@example.com', 'orgName':'Work', 'subscriptionType':'max', 'private':'do-not-store'}))
else:
 assert '--safe-mode' in sys.argv and '--no-session-persistence' in sys.argv
 request = json.loads(sys.stdin.readline())
 assert request['request'] == {'subtype':'get_usage','skip_behaviors':True}
 print(json.dumps({'type':'control_response','response':{'subtype':'success','request_id':request['request_id'],'response':{'rate_limits_available':True,'rate_limits':{'five_hour':{'utilization':12},'model_scoped':[{'display_name':'Fable','utilization':25}]},'private':'do-not-store'}}}), flush=True)
 time.sleep(30)
''')
            binary.chmod(0o700)
            result = await collect_status({'config_dir': directory}, binary=str(binary), timeout=2)
            self.assertEqual(result['identity']['type'], 'Claude Max')
            self.assertEqual(result['identity']['name'], 'Work')
            self.assertEqual(result['usage']['five_hour']['utilization'], 12)
            self.assertNotIn('do-not-store', json.dumps(result))
            self.assertNotIn('error', result)

    async def test_an_empty_usage_read_is_an_error_so_the_last_usage_stays(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / 'claude'
            binary.write_text('''#!/usr/bin/env python3
import json, sys
if sys.argv[1:] == ['auth', 'status']:
 print(json.dumps({'loggedIn': True, 'email':'work@example.com', 'subscriptionType':'max'}))
else:
 request = json.loads(sys.stdin.readline())
 print(json.dumps({'type':'control_response','response':{'subtype':'success','request_id':request['request_id'],
                   'response':{'rate_limits_available':True,'rate_limits':None}}}), flush=True)
''')
            binary.chmod(0o700)
            result = await collect_status({'config_dir': directory}, binary=str(binary), timeout=5)
        self.assertEqual(result['error'], 'usage_unavailable')
        self.assertNotIn('usage', result)

    async def test_unavailable_native_cli_returns_safe_error(self):
        with tempfile.TemporaryDirectory() as directory:
            result = await collect_status({'config_dir': directory}, binary='/absent-claude')
        self.assertEqual(result['error'], 'cli_unavailable')
        self.assertNotIn('/absent', json.dumps(result))

    async def test_profile_without_a_folder_never_checks_the_home_login(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / 'claude'
            binary.write_text('#!/usr/bin/env python3\nfrom pathlib import Path\n'
                              'Path(__file__).with_name("ran").write_text("yes")\n')
            binary.chmod(0o700)
            result = await collect_status({'config_dir': None}, binary=str(binary))
            self.assertEqual(result['error'], 'login_required')
            self.assertFalse((Path(directory) / 'ran').exists())

    async def test_timeout_stops_native_process(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / 'claude'
            binary.write_text('#!/usr/bin/env python3\nimport time\ntime.sleep(30)\n')
            binary.chmod(0o700)
            result = await asyncio.wait_for(collect_status({'config_dir': directory}, binary=str(binary), timeout=.1), 4)
            self.assertEqual(result['error'], 'timeout')

    async def test_refresh_keeps_old_usage_on_failure_and_clears_error_on_recovery(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory))
            with store.db:
                store.put('accounts', {'work': {'config_dir': directory, 'enabled': True}})
                store.put('account_status', {'work': {'identity': {'logged_in': True}, 'usage': {'five_hour': {'utilization': 17}}, 'observed_at': 100}})
            async def failed(account):
                await asyncio.sleep(0)
                return {'checked_at': 200, 'error': 'timeout'}
            await refresh_accounts(store, failed)
            snapshot = store.get('account_status')['work']
            self.assertEqual(snapshot['observed_at'], 100)
            self.assertEqual(snapshot['usage']['five_hour']['utilization'], 17)
            self.assertEqual(snapshot['error'], 'timeout')
            async def recovered(account):
                return {'checked_at': 300, 'observed_at': 300, 'usage': {}}
            await refresh_accounts(store, recovered)
            self.assertNotIn('error', store.get('account_status')['work'])
            self.assertEqual(store.get('account_status')['work']['usage'], {})
            store.close()

    async def test_a_stale_usage_snapshot_does_not_lower_the_same_window(self):
        reset = time.time() + 3600
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory))
            with store.db:
                store.put('accounts', {'work': {'config_dir': directory, 'enabled': True}})
                store.put('account_status', {'work': {'identity': {'logged_in': True}, 'observed_at': time.time(),
                                                      'usage': {'five_hour': {'utilization': 97.0, 'resets_at': reset}}}})

            async def cached(account):
                return {'checked_at': 200, 'observed_at': 200, 'identity': {'logged_in': True},
                        'usage': {'five_hour': {'utilization': 60, 'resets_at': reset + .4}}}
            await refresh_accounts(store, cached)
            self.assertEqual(store.get('account_status')['work']['usage']['five_hour']['utilization'], 97.0)

            async def next_window(account):
                return {'checked_at': 300, 'observed_at': 300, 'identity': {'logged_in': True},
                        'usage': {'five_hour': {'utilization': 5, 'resets_at': reset + 18000}}}
            await refresh_accounts(store, next_window)
            self.assertEqual(store.get('account_status')['work']['usage']['five_hour']['utilization'], 5)
            store.close()

    async def test_a_lower_read_of_the_same_window_after_an_hour_is_a_real_drop(self):
        reset = time.time() + 5 * 86400
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory))
            with store.db:
                store.put('accounts', {'work': {'config_dir': directory, 'enabled': True}})
                store.put('account_status', {'work': {'identity': {'logged_in': True}, 'observed_at': time.time(),
                    'usage': {'seven_day': {'utilization': 96.0, 'resets_at': reset,
                                            'recorded_at': time.time() - 7200}}}})

            async def upgraded(account):
                return {'checked_at': 300, 'observed_at': 300, 'identity': {'logged_in': True},
                        'usage': {'seven_day': {'utilization': 24, 'resets_at': reset}}}
            await refresh_accounts(store, upgraded)
            self.assertEqual(store.get('account_status')['work']['usage']['seven_day']['utilization'], 24)
            store.close()

    async def test_a_cached_read_of_the_previous_window_keeps_the_current_window(self):
        now = time.time()
        current = {'utilization': 96.0, 'resets_at': now + 5 * 3600 - 1800, 'recorded_at': now}
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory))
            with store.db:
                store.put('accounts', {'work': {'config_dir': directory, 'enabled': True}})
                store.put('account_status', {'work': {'identity': {'logged_in': True}, 'observed_at': now,
                                                      'usage': {'five_hour': current}}})

            async def cached(account):
                return {'checked_at': 300, 'observed_at': 300, 'identity': {'logged_in': True},
                        'usage': {'five_hour': {'utilization': 20, 'resets_at': now - 1800}}}
            await refresh_accounts(store, cached)
            self.assertEqual(store.get('account_status')['work']['usage']['five_hour'], current)
            store.close()

    async def test_a_refresh_during_a_block_keeps_the_last_value_instead_of_a_fresh_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory))
            now = time.time()
            identity = {'email': 'work@example.com', 'logged_in': True}
            with store.db:
                store.put('accounts', {'work': {'config_dir': directory, 'enabled': True}})
                store.put('account_status', {'work': {'identity': identity, 'observed_at': now, 'usage': {
                    'seven_day': {'utilization': 100, 'resets_at': now + 86400}}}})
                store.put('account_blocks', {'work': {'until': now + 86400, 'reason': 'quota'}})
            async def fresh_zero(account):
                return {'identity': identity, 'observed_at': time.time(),
                        'usage': {'seven_day': {'utilization': 0, 'resets_at': now + 86400}}}
            await refresh_accounts(store, fresh_zero)
            self.assertEqual(store.get('account_status')['work']['usage']['seven_day']['utilization'], 100)
            store.close()

    async def test_slow_refresh_does_not_block_controls_or_overwrite_profile_change(self):
        from coordinator import control_api
        from coordinator.controls import handle_control
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory))
            with store.db:
                store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('1:2',1,2,'test','')")
                store.put('accounts', {'work': {'config_dir': directory, 'enabled': True}})
            started, finish = asyncio.Event(), asyncio.Event()
            async def blocked(account):
                started.set()
                await finish.wait()
                return {'usage': {'five_hour': {'utilization': 17}}}
            task = asyncio.create_task(refresh_accounts(store, blocked))
            await started.wait()
            with store.db:
                self.assertTrue(handle_control(store, '1:2', 42, '/accounts'))
                control_api.call(store, 'account.disable', {'alias': 'work'}, topic='1:2',
                                 source='telegram', message=43)
            finish.set()
            await task
            self.assertNotIn('work', store.get('account_status', {}))
            store.close()

    async def test_changed_login_never_inherits_previous_usage(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory))
            with store.db:
                store.put('accounts', {'default': {'config_dir': None, 'enabled': True}})
                store.put('account_status', {'default': {'identity': {'email': 'old@example.com'},
                            'usage': {'five_hour': {'utilization': 99}}, 'observed_at': 100}})
            async def changed(account):
                return {'identity': {'email': 'new@example.com'}, 'error': 'usage_unavailable', 'checked_at': 200}
            await refresh_accounts(store, changed)
            snapshot = store.get('account_status')['default']
            self.assertNotIn('usage', snapshot)
            self.assertNotIn('observed_at', snapshot)
            self.assertEqual(snapshot['identity']['email'], 'new@example.com')
            store.close()

    async def test_signed_out_refresh_keeps_the_last_email_and_drops_usage(self):
        from coordinator.accounts import account_label, authenticated
        identity = {'email': 'Work@Example.com', 'type': 'Claude Max', 'name': 'Work', 'logged_in': True}
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / 'state')
            self.addCleanup(store.close)
            profile = {'config_dir': directory, 'enabled': True}
            with store.db:
                store.put('accounts', {'work': profile})
            for signed_out in ({'logged_in': False}, {'email': '', 'logged_in': False}, {}, None):
                with self.subTest(identity=signed_out):
                    with store.db:
                        store.put('account_status', {'work': {'identity': identity, 'observed_at': 100,
                            'usage': {'five_hour': {'utilization': 97}}}})

                    async def collect(account):
                        result = {'checked_at': 200, 'error': 'login_required'}
                        if signed_out is not None:
                            result['identity'] = signed_out
                        return result

                    await refresh_accounts(store, collect)
                    snapshot = store.get('account_status')['work']
                    self.assertEqual(snapshot['identity'], dict(identity, logged_in=False))
                    self.assertNotIn('usage', snapshot)
                    self.assertNotIn('observed_at', snapshot)
                    self.assertEqual(snapshot['error'], 'login_required')
                    self.assertFalse(authenticated(store, 'work', profile))
                    self.assertEqual(account_label(store, 'work'), 'Work@Example.com')

    async def test_cancelled_status_check_reaps_native_process(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / 'claude'
            pidfile = Path(directory) / 'pid'
            binary.write_text('#!/usr/bin/env python3\nimport os,time\nfrom pathlib import Path\n'
                              'Path(__file__).with_name("pid").write_text(str(os.getpid()))\ntime.sleep(30)\n')
            binary.chmod(0o700)
            task = asyncio.create_task(collect_status({'config_dir': directory}, binary=str(binary)))
            try:
                pid = int(await until_text(pidfile))
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)

    async def test_refresh_request_runs_before_periodic_deadline(self):
        import time
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory))
            with store.db:
                store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('1:2',1,2,'test','')")
            refreshed = []
            async def refresh(current, aliases=None):
                refreshed.append(current)
            async def tick(seconds):
                if len(refreshed) == 1:
                    with store.db:
                        store.put('account_status_refresh_requested', time.time())
                else:
                    raise asyncio.CancelledError
            with patch('sys.platform', 'linux'), patch('coordinator.account_status.refresh_accounts', refresh), patch('coordinator.account_status.asyncio.sleep', tick):
                with self.assertRaises(asyncio.CancelledError):
                    await monitor_accounts(store)
            self.assertEqual(len(refreshed), 2)
            store.close()

    async def test_stale_usage_refresh_failure_stays_visible_and_records_problem(self):
        from coordinator.accounts import AccountBroker
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory))
            observed = time.time() - 600
            with store.db:
                store.put('accounts', {'work': {'config_dir': directory, 'enabled': True}})
                store.put('account_status', {'work': {'identity': {
                    'email': 'work@example.com', 'logged_in': True}, 'observed_at': observed,
                    'usage': {'seven_day': {'utilization': 12}}}})
            refresh_done = asyncio.Event()
            async def collect(account):
                return {'checked_at': time.time(), 'error': 'timeout'}
            async def tick(seconds):
                if refresh_done.is_set():
                    raise asyncio.CancelledError
                refresh_done.set()
            original_refresh = refresh_accounts
            async def refresh(current, aliases=None):
                await original_refresh(current, collect, aliases)
            with patch('coordinator.account_status.refresh_accounts', refresh), \
                    patch('coordinator.account_status.asyncio.sleep', tick):
                with self.assertRaises(asyncio.CancelledError):
                    await monitor_accounts(store)
            state = AccountBroker(store).account_state('work')
            self.assertEqual(state, {'state': 'stale', 'since': observed})
            self.assertEqual(store.get('account_status')['work']['error'], 'timeout')
            problem = store.db.execute('SELECT area,code,detail FROM problems ORDER BY id DESC LIMIT 1').fetchone()
            self.assertEqual(tuple(problem), ('accounts', 'status-timeout', 'account=[email]'))
            store.close()

    async def probes_in_fifteen_minutes(self, profiles, collect_result, active=None):
        import types
        from coordinator import account_status
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / 'state')
            accounts = {}
            for name, profile in profiles.items():
                (Path(directory) / name).mkdir()
                accounts[name] = dict(profile, config_dir=str(Path(directory) / name))
            with store.db:
                store.put('accounts', accounts)
                store.put('active_account', active)
            clock = [time.time()]
            end = clock[0] + 900
            probes = []

            async def collect(account):
                name = Path(account['config_dir']).name
                probes.append(name)
                clock[0] += 5
                return dict(collect_result(name), checked_at=clock[0])

            async def sleep(seconds):
                clock[0] += seconds
                if clock[0] > end:
                    raise asyncio.CancelledError

            original = account_status.refresh_accounts

            async def refresh(current, aliases=None):
                return await original(current, collect, aliases)

            fake = types.SimpleNamespace(time=lambda: clock[0], monotonic=lambda: clock[0])
            with patch.object(account_status, 'time', fake), \
                    patch.object(account_status, 'refresh_accounts', refresh), \
                    patch.object(account_status, 'asyncio', types.SimpleNamespace(sleep=sleep)):
                with self.assertRaises(asyncio.CancelledError):
                    await monitor_accounts(store)
            store.close()
            return probes

    async def test_a_profile_awaiting_sign_in_does_not_speed_up_the_periodic_refresh(self):
        def result(name):
            if name == 'spare':
                return {'identity': {'logged_in': False}, 'error': 'login_required'}
            return {'identity': {'email': 'work@example.com', 'logged_in': True},
                    'usage': {'five_hour': {'utilization': 1}}, 'observed_at': time.time()}
        probes = await self.probes_in_fifteen_minutes(
            {'work': {'enabled': True}, 'spare': {'enabled': False, 'awaiting_login': True}}, result)
        self.assertIn(probes.count('work'), (3, 4))

    async def test_a_failing_account_backs_off_instead_of_probing_every_30_seconds(self):
        probes = await self.probes_in_fifteen_minutes({'work': {'enabled': True}},
                                                      lambda name: {'error': 'timeout'})
        self.assertLessEqual(len(probes), 8)
        self.assertGreaterEqual(len(probes), 5)

    async def test_the_active_account_is_probed_every_minute_and_the_rest_every_five(self):
        def result(name):
            return {'identity': {'email': name + '@example.com', 'logged_in': True},
                    'usage': {'five_hour': {'utilization': 1}}, 'observed_at': time.time()}
        probes = await self.probes_in_fifteen_minutes(
            {'work': {'enabled': True}, 'spare': {'enabled': True}}, result, active='work')
        self.assertGreaterEqual(probes.count('work'), 12)
        self.assertIn(probes.count('spare'), (3, 4))

    async def test_a_failing_spare_account_does_not_slow_the_active_probe(self):
        def result(name):
            if name == 'spare':
                return {'error': 'timeout'}
            return {'identity': {'email': name + '@example.com', 'logged_in': True},
                    'usage': {'five_hour': {'utilization': 1}}, 'observed_at': time.time()}
        probes = await self.probes_in_fifteen_minutes(
            {'work': {'enabled': True}, 'spare': {'enabled': True}}, result, active='work')
        self.assertGreaterEqual(probes.count('work'), 12)

    def test_only_a_limit_asks_for_a_usage_refresh(self):
        from coordinator.accounts import AccountBroker
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory))
            broker = AccountBroker(store)
            with store.db:
                broker.record_rate_limit('work', {'status': 'allowed', 'rateLimitType': 'five_hour',
                                                  'utilization': .12})
                broker.record_rate_limit('work', None)
            self.assertIsNone(store.get('account_status_refresh_requested'))
            with store.db:
                broker.record_rate_limit('work', {'status': 'rejected', 'resetsAt': time.time() + 60}, True)
            self.assertIsNotNone(store.get('account_status_refresh_requested'))
            store.close()

    async def test_macos_monitor_refreshes_on_start(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory))
            store.put('account_status_refresh_requested', 123)
            calls = []
            async def refresh(current, aliases=None):
                calls.append(current)
            async def tick(seconds):
                raise asyncio.CancelledError
            with patch('sys.platform', 'darwin'), patch('coordinator.account_status.refresh_accounts', refresh), patch('coordinator.account_status.asyncio.sleep', tick):
                with self.assertRaises(asyncio.CancelledError):
                    await monitor_accounts(store)
            self.assertEqual(calls, [store])
            store.close()

    async def test_monitor_notifies_after_full_and_active_refreshes(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory))
            store.put('active_account', 'work')
            clock = [0]
            calls = []
            notices = []

            async def refresh(current, aliases=None):
                calls.append(aliases)
                return False

            async def tick(seconds):
                clock[0] += 61
                if len(calls) == 2:
                    raise asyncio.CancelledError

            with patch('coordinator.account_status.refresh_accounts', refresh), \
                    patch('coordinator.account_status.asyncio.sleep', tick), \
                    patch('coordinator.account_status.time.monotonic', lambda: clock[0]):
                with self.assertRaises(asyncio.CancelledError):
                    await monitor_accounts(store, lambda: notices.append(len(calls)))
            self.assertEqual(calls, [None, ['work']])
            self.assertEqual(notices, [1, 2])
            store.close()

    async def test_setup_checks_enabled_accounts_with_approval_time(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory))
            store.put('accounts', {'first': {'config_dir': 'first'},
                                   'disabled': {'config_dir': 'disabled', 'enabled': False},
                                   'second': {'config_dir': 'second'}})
            calls = []
            async def collect(account, timeout, binary='claude'):
                self.assertEqual(binary, 'claude')
                calls.append((account['config_dir'], timeout))
                return {'error': 'login_required'} if account['config_dir'] == 'second' else {}
            with patch('coordinator.account_status.collect_status', collect):
                result = await check_accounts(store)
            self.assertEqual(calls, [('first', 180), ('second', 180)])
            self.assertEqual(result, {'first': 'ready', 'second': 'login_required'})
            store.close()


class NativeLoginTests(unittest.IsolatedAsyncioTestCase):
    async def test_darwin_awaiting_login_needs_only_auth_status(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            profile = root / 'profile'
            profile.mkdir()
            store = Store(root / 'state')
            self.addCleanup(store.close)
            store.put('accounts', {'native': {'config_dir': str(profile), 'enabled': False, 'awaiting_login': True}})
            async def signed_in(account):
                return {'identity': {'logged_in': True, 'email': 'native@example.invalid'}, 'observed_at': time.time()}
            with patch('sys.platform', 'darwin'), patch('subprocess.run', side_effect=AssertionError('No security call')), \
                    patch('asyncio.create_subprocess_exec', side_effect=AssertionError('No security call')):
                self.assertFalse(await refresh_accounts(store, signed_in))
            self.assertTrue(store.get('accounts')['native']['enabled'])
            self.assertNotIn('awaiting_login', store.get('accounts')['native'])
