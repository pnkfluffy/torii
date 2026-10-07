import ast
import io
import json
import os
import tempfile
import time
import tokenize
import unittest
from pathlib import Path
from unittest.mock import patch

from coordinator import control_api
from coordinator.account_status import refresh_accounts
from coordinator.accounts import (LIMIT_CONTINUATION, AccountBroker, account_label, discover_accounts,
                                  listed_accounts, switch_threshold)
from coordinator.failures import TaskFailure
from coordinator.providers import RunResult
from coordinator.store import Store


class AccountTestsSupport:
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store = Store(self.root / 'state')
        self.sid = '20000000-0000-4000-8000-000000000001'
        now = time.time()
        accounts = {}
        status = {}
        for alias, email, reset in (('primary', 'primary@example.com', 3600),
                                    ('second', 'second@example.com', 7200)):
            directory = self.root / alias
            directory.mkdir()
            accounts[alias] = {'config_dir': str(directory), 'enabled': True}
            if alias == 'primary':
                transcript = directory / 'projects' / 'project-hash' / (self.sid + '.jsonl')
                transcript.parent.mkdir(parents=True)
                transcript.write_text('{}\n')
            status[alias] = {'identity': {'email': email, 'logged_in': True},
                             'observed_at': now,
                             'usage': {'five_hour': {'utilization': 10, 'resets_at': now + 1800},
                                       'seven_day': {'utilization': 10, 'resets_at': now + reset},
                                       'seven_day_fable': {'utilization': 10, 'resets_at': now + reset}}}
        with self.store.db:
            self.store.put('accounts', accounts)
            self.store.put('account_status', status)
        self.transcript = self.root / 'primary' / 'projects' / 'project-hash' / (self.sid + '.jsonl')
        with self.store.db:
            self.store.put('session_transcripts', {self.sid: str(self.transcript)})
        self.broker = AccountBroker(self.store)

    async def asyncTearDown(self):
        self.store.close()
        self.temp.cleanup()

    def meter(self, alias, key, utilization, resets_at):
        status = self.store.get('account_status')
        status[alias]['usage'][key] = {'utilization': utilization, 'resets_at': resets_at}
        with self.store.db:
            self.store.put('account_status', status)


class AccountTests(AccountTestsSupport, unittest.IsolatedAsyncioTestCase):
    async def test_local_threshold_override_is_optional_and_alias_scoped(self):
        override = self.root / 'local-overrides.json'
        with patch('coordinator.accounts.LOCAL_OVERRIDES_PATH', override):
            self.assertEqual(switch_threshold(self.store, 'orbit'), .95)
            override.write_text(json.dumps({'account_switch_thresholds': {'orbit': .8}}))
            self.assertEqual(switch_threshold(self.store, 'orbit'), .8)
            self.assertEqual(switch_threshold(self.store, 'primary'), .95)
            override.write_text('{broken')
            with self.assertLogs('coordinator.accounts', level='WARNING'):
                self.assertEqual(switch_threshold(self.store, 'orbit'), .95)
            override.write_text(json.dumps({'account_switch_thresholds': {'orbit': True}}))
            with self.assertLogs('coordinator.accounts', level='WARNING'):
                self.assertEqual(switch_threshold(self.store, 'orbit'), .95)

    async def test_retry_skip_and_empty_selection_logs_name_the_actual_cause(self):
        accounts = self.store.get('accounts')
        with self.store.db:
            self.store.put('active_account', 'primary')
        self.assertEqual(self.broker.selection_reason(accounts, 'primary', {'primary'}), 'reason=attempted')
        with self.assertLogs('coordinator.accounts', level='INFO') as logs:
            self.assertEqual(self.broker.activate(accounts, {'primary'}), 'second')
        self.assertIn('reason=attempted', logs.output[0])
        with self.store.db:
            self.store.put('active_account', 'primary')
        now = time.time()
        for alias in accounts:
            self.meter(alias, 'seven_day', 97, now + 120)
            self.meter(alias, 'five_hour', 10, now + 120)
            self.meter(alias, 'seven_day_fable', 10, now + 120)
        with self.assertLogs('coordinator.accounts', level='INFO') as logs:
            self.assertIsNone(self.broker.activate(accounts))
        self.assertIn('no eligible account from=primary@example.com', logs.output[0])
        self.assertNotIn('to=None', logs.output[0])
        for alias in accounts:
            self.meter(alias, 'seven_day', 10, now + 120)
        with self.assertLogs('coordinator.accounts', level='INFO') as logs:
            self.assertEqual(self.broker.activate(accounts), 'primary')
        self.assertIn('reason=account_available', logs.output[0])

    async def test_local_threshold_changes_fullness_selection_and_reset_for_each_window(self):
        override = self.root / 'local-overrides.json'
        override.write_text(json.dumps({'account_switch_thresholds': {'orbit': .8}}))
        accounts = self.store.get('accounts')
        accounts['orbit'] = dict(accounts['primary'])
        status = self.store.get('account_status')
        status['orbit'] = dict(status['primary'])
        with self.store.db:
            self.store.put('accounts', accounts)
            self.store.put('account_status', status)
        now = time.time()
        with patch('coordinator.accounts.LOCAL_OVERRIDES_PATH', override):
            for window in ('five_hour', 'seven_day', 'seven_day_fable'):
                self.meter('orbit', window, 80, now + 300)
                self.meter('primary', window, 80, now + 200)
                self.assertTrue(self.broker.is_full('orbit'), window)
                self.assertFalse(self.broker.is_full('primary'), window)
                self.assertFalse(self.broker.available(accounts, 'orbit'), window)
                self.assertNotEqual(self.broker.select(accounts), 'orbit')
                self.assertEqual(self.broker.earliest_reset({'orbit': accounts['orbit']}), now + 300)
                self.meter('orbit', window, 10, now + 300)
                self.meter('primary', window, 10, now + 200)

    async def test_removing_override_releases_threshold_block_and_reserve_label(self):
        from coordinator.control_ui import _accounts_view
        from coordinator.controls import _accounts
        override = self.root / 'local-overrides.json'
        override.write_text(json.dumps({'account_switch_thresholds': {'primary': .8}}))
        with patch('coordinator.accounts.LOCAL_OVERRIDES_PATH', override):
            self.meter('primary', 'five_hour', 80, time.time() + 300)
            self.broker.record_rate_limit('primary', {'rateLimitType': 'five_hour',
                                                       'utilization': .8, 'resetsAt': time.time() + 300})
            self.assertIn('reserve 20%', _accounts_view(self.store, 0)[0])
            self.assertIn('reserve 20% (local override)', _accounts(self.store))
            self.assertTrue(self.broker.is_full('primary'))
            override.unlink()
            self.assertFalse(self.broker.is_full('primary'))
            self.assertNotIn('reserve', _accounts_view(self.store, 0)[0])

    async def test_quota_worker_resumes_same_session_on_second_account(self):
        calls = []

        async def run(provider, prompt, cwd, sid, **options):
            calls.append((provider, prompt, sid, options.copy()))
            if len(calls) == 1:
                return RunResult(sid, pid=42, quota_limited=True,
                                 rate_limit_info={'status': 'rejected', 'rateLimitType': 'five_hour',
                                                  'resetsAt': time.time() + 3600},
                                 transcript_path=str(self.transcript))
            return RunResult(sid, success=True, transcript_path=str(self.transcript))

        result = await self.broker.run(run, 'claude', 'same task prompt', self.root, self.sid,
                                       fresh=False, model='opus')
        self.assertTrue(result.success)
        self.assertEqual(len(calls), 2)
        self.assertEqual([call[2] for call in calls], [self.sid, self.sid])
        self.assertEqual(calls[1][1], LIMIT_CONTINUATION + 'same task prompt')
        self.assertEqual(calls[0][3]['account_alias'], 'primary')
        self.assertEqual(calls[1][3]['account_alias'], 'second')
        target = Path(calls[1][3]['resume_path'])
        self.assertEqual(target.parent.parent.parent, self.root / 'second')
        self.assertTrue(target.is_symlink())
        self.assertEqual(target.resolve(), self.transcript.resolve())
        self.assertEqual(self.store.get('last_account'), 'second')
        self.assertIn('primary', self.store.get('account_blocks'))

    async def test_a_rotation_is_reported_as_an_accounts_problem(self):
        seen = []

        async def run(provider, prompt, cwd, sid, **options):
            if options['account_alias'] == 'primary':
                return RunResult(sid, pid=1, quota_limited=True, transcript_path=str(self.transcript),
                                 rate_limit_info={'status': 'rejected', 'resetsAt': time.time() + 3600})
            return RunResult(sid, success=True, transcript_path=str(self.transcript))
        await self.broker.run(run, 'claude', 'work', self.root, self.sid, fresh=False, model='opus',
                              on_problem=lambda *problem: seen.append(problem))
        self.assertEqual(seen, [('accounts', 'rotated',
                                 'from=primary@example.com to=second@example.com model=opus')])

    async def test_a_failed_launch_after_rotation_keeps_the_first_pid_and_resumes(self):
        fresh = []

        async def run(provider, prompt, cwd, sid, **options):
            fresh.append(options['fresh'])
            if options['account_alias'] == 'primary':
                return RunResult(sid, pid=123, quota_limited=True, transcript_path=str(self.transcript),
                                 rate_limit_info={'status': 'rejected', 'resetsAt': time.time() + 3600})
            return RunResult(sid, error='Account could not launch')
        result = await self.broker.run(run, 'claude', 'work', self.root, self.sid, fresh=True)
        self.assertEqual(result.pid, 123)
        self.assertEqual(fresh, [True, False])

    async def test_a_non_quota_failure_stays_on_its_account(self):
        calls = []

        async def run(provider, prompt, cwd, sid, **options):
            calls.append(options['account_alias'])
            return RunResult(sid, pid=1, error='provider failed')
        result = await self.broker.run(run, 'claude', 'work', self.root, self.sid, fresh=False)
        self.assertEqual(calls, ['primary'])
        self.assertEqual(result.error, 'provider failed')

    async def test_a_resume_without_its_transcript_never_starts_the_provider(self):
        async def run(*args, **kwargs):
            self.fail('Provider must not start without the saved transcript')
        self.transcript.unlink()
        with self.assertRaisesRegex(TaskFailure, 'conversation file is unavailable'):
            await self.broker.run(run, 'claude', 'work', self.root, self.sid, fresh=False)
        with self.store.db:
            self.store.put('session_transcripts', {})
        with patch('pathlib.Path.home', return_value=self.root / 'home'):
            result = await self.broker.run(run, 'claude', 'work', self.root, self.sid, fresh=False)
        self.assertEqual(result.failure_code, 'transcript_missing')

    async def test_a_warning_below_95_percent_records_usage_without_blocking(self):
        async def run(provider, prompt, cwd, sid, **options):
            return RunResult(sid, success=True, rate_limit_info={
                'status': 'allowed_warning', 'rateLimitType': 'seven_day', 'utilization': .8})
        await self.broker.run(run, 'claude', 'work', self.root, self.sid, fresh=False)
        self.assertEqual(self.store.get('account_status')['primary']['usage']['seven_day']['utilization'], 80)
        self.assertEqual(self.store.get('account_blocks', {}), {})
        self.assertEqual(self.broker.select(), 'primary')

    async def test_a_stream_five_hour_reading_at_96_percent_excludes_the_account(self):
        self.broker.record_rate_limit('primary', {'status': 'allowed_warning', 'rateLimitType': 'five_hour',
                                                  'utilization': .96, 'resetsAt': time.time() + 1800})
        self.assertEqual(self.store.get('account_status')['primary']['usage']['five_hour']['utilization'], 96)
        self.assertEqual(self.broker.select(), 'second')

    async def test_expired_blocks_are_pruned_and_committed_on_read(self):
        with self.store.db:
            self.store.put('account_blocks', {'primary': {'until': time.time() - 5},
                                              'second': {'until': time.time() + 600}})
        self.assertEqual(self.broker.select(), 'primary')
        self.assertFalse(self.store.db.in_transaction)
        self.assertEqual(list(self.store.get('account_blocks')), ['second'])

    async def test_pruning_inside_a_callers_transaction_joins_it_without_committing(self):
        expired = {'primary': {'until': time.time() - 10}}
        with self.store.db:
            self.store.put('account_blocks', expired)
        self.store.db.execute('BEGIN')
        self.store.put('marker', 'uncommitted')
        self.assertEqual(self.broker.blocks(), {})
        self.assertTrue(self.store.db.in_transaction)
        self.store.db.rollback()
        self.assertIsNone(self.store.get('marker'))
        self.assertEqual(self.store.get('account_blocks'), expired)

    async def test_legacy_home_transcript_resumes_after_its_placeholder_is_dropped(self):
        home = self.root / 'home'
        legacy = home / '.claude' / 'projects' / 'old-project' / (self.sid + '.jsonl')
        legacy.parent.mkdir(parents=True)
        legacy.write_text('{}\n')
        self.transcript.unlink()
        with self.store.db:
            self.store.put('accounts', {'home-login-needs-name': {'config_dir': str(home / '.claude'),
                                                                  'enabled': False, 'needs_name': True},
                                        **self.store.get('accounts')})
        discover_accounts(self.store, self.root / 'missing')
        calls = []

        async def run(provider, prompt, cwd, sid, **options):
            calls.append(options.copy())
            return RunResult(sid, success=True, transcript_path=options['resume_path'])

        for saved in ({self.sid: str(legacy)}, {}):
            calls.clear()
            with self.store.db:
                self.store.put('session_transcripts', saved)
            with patch('pathlib.Path.home', return_value=home):
                result = await self.broker.run(run, 'claude', 'continue', self.root, self.sid,
                                               fresh=False, model='opus')
            self.assertTrue(result.success, result.error)
            resume = Path(calls[0]['resume_path'])
            self.assertEqual(resume.resolve(), legacy.resolve())
            self.assertEqual(resume.parents[2], self.root / calls[0]['account_alias'])
        self.assertTrue(legacy.is_file())
        self.assertFalse(legacy.is_symlink())

    async def test_an_exhausted_pool_stops_after_each_account_once(self):
        calls = []

        async def run(provider, prompt, cwd, sid, **options):
            calls.append(options['account_alias'])
            return RunResult(sid, pid=1, quota_limited=True, transcript_path=str(self.transcript))

        result = await self.broker.run(run, 'claude', 'work', self.root, self.sid, fresh=False)
        self.assertFalse(result.success)
        self.assertEqual(sorted(calls), ['primary', 'second'])
        self.assertEqual(sorted(self.store.get('account_blocks')), ['primary', 'second'])
        self.assertIsNone(self.broker.select())

    async def test_a_single_model_weekly_warning_leaves_the_three_meters_alone(self):
        for limit_type in ('seven_day_opus', 'seven_day_sonnet', 'overage'):
            with self.store.db:
                self.broker.record_rate_limit('primary', {'status': 'allowed_warning', 'rateLimitType': limit_type,
                                                          'utilization': .96, 'resetsAt': time.time() + 5 * 86400})
            usage = self.store.get('account_status')['primary']['usage']
            self.assertEqual([usage[key]['utilization'] for key in ('five_hour', 'seven_day', 'seven_day_fable')],
                             [10, 10, 10], limit_type)
            self.assertEqual(self.broker.select(), 'primary', limit_type)
        with self.store.db:
            self.broker.record_rate_limit('primary', {'status': 'rejected', 'rateLimitType': 'seven_day_opus'}, True)
        self.assertEqual(self.broker.select(), 'second')

    async def test_a_fable_weekly_warning_at_96_percent_fills_the_fable_meter(self):
        with self.store.db:
            self.broker.record_rate_limit('primary', {'status': 'allowed_warning',
                                                      'rateLimitType': 'seven_day_overage_included',
                                                      'utilization': .96, 'resetsAt': time.time() + 3 * 86400})
        self.assertEqual(self.store.get('account_status')['primary']['usage']['seven_day_fable']['utilization'], 96)
        self.assertTrue(self.broker.is_full('primary'))
        self.assertEqual(self.broker.select(), 'second')

    async def test_the_first_event_of_a_new_window_does_not_bring_back_the_old_reading(self):
        now = time.time()
        self.meter('primary', 'five_hour', 97, now - 10)
        self.assertEqual(self.broker.select(), 'primary')
        with self.store.db:
            self.broker.record_rate_limit('primary', {'status': 'allowed', 'rateLimitType': 'five_hour',
                                                      'resetsAt': int(now) + 5 * 3600})
        self.assertEqual(self.store.get('account_status')['primary']['usage']['five_hour'],
                         {'resets_at': int(now) + 5 * 3600})
        self.assertFalse(self.broker.is_full('primary'))

    async def test_rejection_without_reset_time_holds_through_the_next_usage_check(self):
        with self.store.db:
            self.broker.record_rate_limit('primary', {'status': 'rejected'}, rejected=True)
        status = self.store.get('account_status')
        status['primary']['observed_at'] = time.time() + 1
        with self.store.db:
            self.store.put('account_status', status)
        self.assertEqual(self.broker.select(), 'second')
        later = time.time() + AccountBroker.REJECTION_HOLD_SECONDS + 1
        with patch('coordinator.accounts.time.time', return_value=later):
            self.assertEqual(self.broker.select(), 'primary')

    async def test_threshold_block_without_reset_clears_on_a_newer_usage_check(self):
        with self.store.db:
            self.broker.record_rate_limit('primary', {'status': 'allowed_warning', 'utilization': .96})
        self.assertEqual(self.broker.select(), 'second')
        status = self.store.get('account_status')
        status['primary']['observed_at'] = time.time() + 1
        status['primary']['usage']['seven_day']['utilization'] = 20
        with self.store.db:
            self.store.put('account_status', status)
        self.assertEqual(self.broker.select(), 'primary')

    async def test_any_meter_at_95_percent_excludes_account_and_94_stays_eligible(self):
        resets_at = time.time() + 120
        for key in ('five_hour', 'seven_day', 'seven_day_fable'):
            self.meter('primary', key, 94, resets_at)
            self.assertEqual(self.broker.select(), 'primary', key)
            self.meter('primary', key, 95, resets_at)
            self.assertEqual(self.broker.select(), 'second', key)
            self.assertEqual(self.broker.account_state('primary')['state'], 'limited', key)
            self.meter('primary', key, 10, resets_at)

    async def test_a_full_meter_whose_reset_has_passed_does_not_block(self):
        for key in ('five_hour', 'seven_day', 'seven_day_fable'):
            self.meter('primary', key, 99, time.time() - 1)
        self.assertEqual(self.broker.account_state('primary'), {'state': 'available'})
        self.assertTrue(self.broker.available(self.store.get('accounts'), 'primary'))

    async def test_earliest_reset_is_the_soonest_account_to_clear_all_its_limits(self):
        now = time.time()
        self.meter('primary', 'seven_day', 96, now + 300)
        self.meter('second', 'five_hour', 97, now + 200)
        with self.store.db:
            self.store.put('account_blocks', {'primary': {'until': now + 100, 'reason': 'quota'}})
        self.assertIsNone(self.broker.select())
        self.assertEqual(self.broker.earliest_reset(), now + 200)

    async def test_restart_discovery_ignores_live_like_unregistered_hidden_folder(self):
        root = self.root / 'hidden-profiles'
        profiles = {}
        status = {}
        for number in range(1, 5):
            directory = root / ('.torii-%08x' % number)
            directory.mkdir(parents=True)
            (directory / '.claude.json').touch()
            if number <= 3:
                alias = 'account%d' % number
                profiles[alias] = {'config_dir': str(directory.resolve()), 'enabled': True}
                status[alias] = {'identity': {'email': alias + '@example.test', 'logged_in': True}}
        with self.store.db:
            self.store.put('mode', 'group')
            self.store.put('accounts', profiles)
            self.store.put('account_status', status)
        self.store.close()
        self.store = Store(self.root / 'state')
        self.assertEqual(discover_accounts(self.store, root), profiles)

        async def collect(profile):
            return {'identity': {'email': 'account@example.test', 'logged_in': True}}

        await refresh_accounts(self.store, collect)
        self.assertEqual(sorted(self.store.get('accounts')), sorted(profiles))
        self.assertEqual(len(list(root.iterdir())), 4)

    async def test_discovery_only_adopts_recorded_installer_hidden_folder_once(self):
        root = self.root / 'hidden-profiles'
        for name in ('.torii-11111111', '.torii-22222222'):
            directory = root / name
            directory.mkdir(parents=True)
            (directory / '.claude.json').touch()
        recorded = str((root / '.torii-22222222').resolve())
        with self.store.db:
            self.store.put('setup_claude_dir', recorded)
        profiles = discover_accounts(self.store, root)
        self.assertEqual(profiles['torii-22222222']['config_dir'], recorded)
        self.assertNotIn('torii-11111111', profiles)
        self.assertIsNone(self.store.get('setup_claude_dir'))
        self.assertEqual(discover_accounts(self.store, root), profiles)

    async def test_discovery_skips_folders_without_a_login_and_keeps_an_owner_disable(self):
        root = self.root / 'profiles'
        for alias in ('ready', 'empty'):
            (root / alias).mkdir(parents=True)
        (root / 'ready' / '.claude.json').write_text('{}')
        self.assertEqual(sorted(discover_accounts(self.store, root)), ['primary', 'ready', 'second'])
        with self.store.db:
            control_api.call(self.store, 'account.disable', {'alias': 'ready'})
        discover_accounts(self.store, root)

        async def signed_in(account):
            return {'identity': {'email': Path(account['config_dir']).name + '@example.com', 'logged_in': True},
                    'usage': {}, 'observed_at': time.time()}
        await refresh_accounts(self.store, signed_in)
        self.assertFalse(self.store.get('accounts')['ready']['enabled'])

    def test_discovery_skips_previous_and_removed_profile_folders(self):
        root = self.root / 'profiles'
        for name in ('previous', 'removed', 'fresh'):
            directory = root / name
            directory.mkdir(parents=True)
            (directory / '.claude.json').write_text(name)
        profiles = self.store.get('accounts')
        profiles['primary']['previous_config_dirs'] = [str(root / 'previous' / '..' / 'previous')]
        with self.store.db:
            self.store.put('accounts', profiles)
            self.store.put('account_dirs_removed', [str(root / 'removed' / '..' / 'removed')])
        self.assertEqual(sorted(discover_accounts(self.store, root)), ['fresh', 'primary', 'second'])
        self.assertEqual((root / 'previous' / '.claude.json').read_text(), 'previous')
        self.assertEqual((root / 'removed' / '.claude.json').read_text(), 'removed')

    def test_remove_preserves_files_and_discovery_does_not_restore_account(self):
        old = self.root / 'older'
        old.mkdir()
        (old / '.claude.json').write_text('older login marker')
        (self.root / 'primary' / '.claude.json').write_text('current login marker')
        profiles = self.store.get('accounts')
        profiles['primary']['previous_config_dirs'] = [str(old)]
        snapshots = self.store.get('account_status')
        snapshots['primary']['identity']['logged_in'] = False
        with self.store.db:
            self.store.put('accounts', profiles)
            self.store.put('account_status', snapshots)
            result = control_api.call(self.store, 'account.remove', {'alias': 'primary'})
        self.assertTrue(result.ok, result.text)
        self.assertEqual(result.text, 'Removed primary@example.com.')
        self.assertEqual(self.store.get('account_dirs_removed'), [str((self.root / 'primary').resolve()),
                                                                  str(old.resolve())])
        self.assertEqual(sorted(discover_accounts(self.store, self.root)), ['second'])
        self.assertEqual(self.transcript.read_text(), '{}\n')
        self.assertEqual((old / '.claude.json').read_text(), 'older login marker')
        self.assertEqual((self.root / 'primary' / '.claude.json').read_text(), 'current login marker')

    def test_transcript_roots_find_history_in_previous_config_dirs(self):
        new = self.root / 'new-primary'
        new.mkdir()
        profiles = self.store.get('accounts')
        profiles['primary']['config_dir'] = str(new)
        profiles['primary']['previous_config_dirs'] = [str(self.root / 'primary'), str(self.root / 'primary')]
        with self.store.db:
            self.store.put('accounts', profiles)
            self.store.put('session_transcripts', {})
        with patch('coordinator.accounts.Path.home', return_value=self.root):
            roots = self.broker.transcript_roots()
            self.assertIn(str(new), roots)
            self.assertEqual(roots.count(str(self.root / 'primary')), 1)
            self.assertEqual(self.broker.transcript(self.sid), str(self.transcript.resolve()))
        resumed = Path(self.broker.resume_transcript('primary', self.sid, str(self.transcript)))
        self.assertTrue(resumed.is_symlink())
        self.assertEqual(resumed.resolve(), self.transcript.resolve())
        self.assertEqual(self.transcript.read_text(), '{}\n')

    async def test_a_weekly_reset_that_already_passed_does_not_rank_first(self):
        now = time.time()
        self.meter('primary', 'seven_day', 40, now - 60)
        self.meter('second', 'seven_day', 40, now + 2 * 86400)
        self.assertEqual(self.broker.select(), 'second')

    async def test_selection_uses_soonest_all_model_weekly_reset(self):
        status = self.store.get('account_status')
        now = time.time()
        status['primary']['usage']['seven_day']['resets_at'] = now + 5000
        status['second']['usage']['seven_day']['resets_at'] = now + 1000
        with self.store.db:
            self.store.put('account_status', status)
        self.assertEqual(self.broker.select(), 'second')

    async def test_stale_usage_still_chooses_by_last_known_meters_and_disabled_never_starts(self):
        status = self.store.get('account_status')
        for alias in ('primary', 'second'):
            status[alias]['observed_at'] = time.time() - 3600
        status['primary']['usage']['seven_day']['utilization'] = 97
        with self.store.db:
            self.store.put('account_status', status)
        self.assertEqual(self.broker.account_state('primary')['state'], 'limited')
        self.assertEqual(self.broker.account_state('second')['state'], 'stale')
        self.assertEqual(self.broker.select(), 'second')
        with self.store.db:
            accounts = self.store.get('accounts')
            accounts['second']['enabled'] = False
            self.store.put('accounts', accounts)
        self.assertIsNone(self.broker.select())
        self.assertEqual(self.broker.earliest_reset(), status['primary']['usage']['seven_day']['resets_at'])

    async def test_earliest_reset_survives_an_account_whose_usage_was_reset(self):
        from coordinator.controls import op_account_reset
        status = self.store.get('account_status')
        status['second']['usage']['five_hour']['utilization'] = 100
        with self.store.db:
            self.store.put('account_status', status)
            op_account_reset(self.store, None, 'primary')
        self.assertEqual(self.broker.select(), 'primary')
        self.assertEqual(self.broker.earliest_reset(), status['second']['usage']['five_hour']['resets_at'])

    async def test_legacy_selection_settings_migrate_idempotently_and_default_is_dropped(self):
        with self.store.db:
            self.store.put('accounts', {'default': {'config_dir': str(self.root), 'enabled': True},
                                        'home-login-needs-name': {'config_dir': str(self.root), 'enabled': False},
                                        **self.store.get('accounts')})
            self.store.put('account_status', {'default': {'identity': {'logged_in': False}},
                                              'home-login-needs-name': {'identity': {'logged_in': False}},
                                              **self.store.get('account_status')})
            self.store.put('account_selection', 'manual')
            self.store.put('active_account', 'primary')
            self.store.put('account_pin_fallback', 'primary')
            self.store.put('account_switch_threshold', .8)
            self.store.put('auto_rotate', True)
            self.store.put('model_account_blocks', {'second': {'opus': {'until': time.time() + 60}}})
            self.store.put('model_account_usage', {'second': {}})
            self.store.put('model_account_access', {'second': {'opus': False}})
        legacy = ('account_selection', 'account_pin_fallback', 'account_switch_threshold',
                  'auto_rotate', 'model_account_blocks', 'model_account_usage', 'model_account_access')

        def saved():
            return {row[0] for row in self.store.db.execute(
                'SELECT key FROM settings WHERE key IN (%s)' % ','.join('?' * len(legacy)), legacy)}

        AccountBroker(self.store).select()
        listed_accounts(self.store)
        self.assertEqual(saved(), set(legacy))
        discover_accounts(self.store, self.root / 'missing')
        self.assertNotIn('default', self.store.get('accounts'))
        self.assertNotIn('home-login-needs-name', self.store.get('accounts'))
        self.assertNotIn('default', self.store.get('account_status'))
        self.assertEqual(saved(), set())
        self.assertEqual(self.store.get('model_account_blocks', {}), {})
        discover_accounts(self.store, self.root / 'missing')
        self.assertEqual(saved(), set())

    def test_activate_reselects_free_accounts_and_logs_only_changes(self):
        with self.store.db:
            self.store.put('active_account', 'second')
        with self.assertLogs('coordinator.accounts', level='INFO') as logs:
            self.assertEqual(self.broker.activate(), 'primary')
            self.assertEqual(self.broker.activate(), 'primary')
        self.assertEqual(len(logs.output), 1)
        self.assertIn('from=second@example.com to=primary@example.com reason=sooner_reset', logs.output[0])
        self.assertEqual(listed_accounts(self.store)[1], 'primary')
        with patch.object(self.store, 'put', wraps=self.store.put) as put:
            self.assertEqual(self.broker.activate(), 'primary')
        put.assert_not_called()

    def test_each_meter_crossing_is_logged_once_and_rotation_explains_it(self):
        self.broker.activate()
        reset = time.time() + 3600
        with self.assertLogs('coordinator.accounts', level='INFO') as logs:
            for meter in ('five_hour', 'seven_day', 'seven_day_fable'):
                for used in (96, 97):
                    self.broker.record_usage('primary', {meter: {'utilization': used, 'resets_at': reset}})
            self.assertEqual(self.broker.activate(), 'second')
        crossings = [line for line in logs.output if 'switch point crossed' in line]
        self.assertEqual(len(crossings), 3)
        for meter in ('five_hour', 'seven_day', 'seven_day_fable'):
            self.assertEqual(sum('meter=' + meter + ' ' in line for line in crossings), 1)
        self.assertIn('reason=five_hour used_pct=97 switch_point_pct=95.0 reset=', logs.output[-1])
        self.assertTrue(all('account=primary@example.com' in line for line in crossings))

    def test_native_rate_limit_crossing_and_hold_are_logged_once(self):
        reset = time.time() + 3600
        info = {'rateLimitType': 'five_hour', 'utilization': .96, 'resetsAt': reset}
        with self.assertLogs('coordinator.accounts', level='INFO') as logs:
            self.broker.record_rate_limit('primary', info)
            self.broker.record_rate_limit('primary', info)
        self.assertEqual(sum('switch point crossed' in line for line in logs.output), 1)
        self.assertEqual(sum('account limited' in line for line in logs.output), 1)

    async def test_poll_and_native_rate_limit_share_crossing_detection(self):
        reset = time.time() + 3600
        async def collect(account):
            return {'usage': {'seven_day': {'utilization': 96, 'resets_at': reset}}}
        with self.assertLogs('coordinator.accounts', level='INFO') as logs:
            await refresh_accounts(self.store, collect, aliases=['primary'])
            await refresh_accounts(self.store, collect, aliases=['primary'])
            self.broker.record_rate_limit('primary', {'rateLimitType': 'seven_day', 'utilization': .96, 'resetsAt': reset})
        self.assertEqual(sum('switch point crossed' in line for line in logs.output), 1)

    def test_activate_logs_hold_disabled_and_signed_out_reasons(self):
        for reason in ('hold', 'disabled', 'signed_out'):
            with self.subTest(reason=reason):
                accounts = self.store.get('accounts')
                status = self.store.get('account_status')
                accounts['primary']['enabled'] = reason != 'disabled'
                status['primary']['identity']['logged_in'] = reason != 'signed_out'
                with self.store.db:
                    self.store.put('accounts', accounts)
                    self.store.put('account_status', status)
                    self.store.put('active_account', 'primary')
                    self.store.put('account_blocks', {'primary': {'reason': 'quota', 'until': time.time() + 600}} if reason == 'hold' else {})
                with self.assertLogs('coordinator.accounts', level='INFO') as logs:
                    self.assertEqual(self.broker.activate(), 'second')
                self.assertIn('reason=' + reason, logs.output[0])
                if reason == 'hold':
                    self.assertIn('hold_reason=quota until=', logs.output[0])

    async def test_discovery_reselects_the_soonest_account(self):
        self.assertEqual(self.broker.select(), 'primary')
        with self.store.db:
            self.store.put('active_account', 'second')
        discover_accounts(self.store, self.root / 'missing')
        self.assertEqual(AccountBroker(self.store).activate(), 'primary')

    async def test_accounts_display_email_and_an_unreadable_identity_is_not_listed(self):
        status = self.store.get('account_status')
        status['second'].pop('identity')
        with self.store.db:
            self.store.put('account_status', status)
        self.assertEqual(account_label(self.store, 'primary'), 'primary@example.com')
        self.assertNotIn('second', listed_accounts(self.store)[0])

    async def test_a_profile_without_a_signed_in_login_is_not_listed_or_chosen(self):
        spare = self.root / 'spare'
        spare.mkdir()
        with self.store.db:
            accounts = self.store.get('accounts')
            accounts['spare'] = {'config_dir': str(spare), 'enabled': False, 'awaiting_login': True}
            accounts['primary']['enabled'] = False
            self.store.put('accounts', accounts)
            status = self.store.get('account_status')
            status['second']['identity'] = {'logged_in': False}
            status['second']['error'] = 'login_required'
            self.store.put('account_status', status)
        self.assertEqual(listed_accounts(self.store), (['primary'], None))
        self.assertIsNone(self.broker.select())

    def test_every_claude_environment_names_the_profile_and_drops_service_and_login_variables(self):
        leaked = {'CLAUDE_CONFIG_DIR': '/home-login', 'CMUX_SOCKET_PATH': 'x', 'CODEX_THREAD_ID': 'x',
                  'TELEGRAM_BOT_TOKEN': 'x', 'TORII_VAULT_KEY': 'x', 'ANTHROPIC_API_KEY': 'x',
                  'CLAUDE_CODE_OAUTH_TOKEN': 'x', 'CLAUDECODE': 'x'}
        profile = str(self.root / 'primary')
        with patch.dict(os.environ, leaked):
            for env in (AccountBroker(self.store).environment('primary'),
                        AccountBroker.profile_environment({'config_dir': profile}),
                        AccountBroker.signin_environment(profile)):
                self.assertEqual(env['CLAUDE_CONFIG_DIR'], profile)
                self.assertEqual([name for name in leaked if name in env], ['CLAUDE_CONFIG_DIR'])

    def test_only_the_account_broker_sets_the_claude_account(self):
        root = Path(__file__).resolve().parents[1]
        found = []
        paths = [path for base in ('coordinator', 'scripts') for path in sorted((root / base).rglob('*.py'))]
        for path in paths:
            if path == root / 'coordinator' / 'accounts.py':
                continue
            text = path.read_text()
            docstrings = {(node.body[0].lineno, node.body[0].col_offset) for node in ast.walk(ast.parse(text))
                          if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
                          and ast.get_docstring(node, clean=False) is not None}
            for token in tokenize.generate_tokens(io.StringIO(text).readline):
                named = 'CLAUDE_CONFIG_DIR' in token.string or 'claude_environment' in token.string
                if named and token.start not in docstrings:
                    found.append('%s:%d %s' % (path.relative_to(root), token.start[0], token.string[:40]))
        self.assertEqual(found, [])
