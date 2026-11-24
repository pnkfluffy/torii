import asyncio
import json
import os
import shutil
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from coordinator import records
from coordinator.store import Store
from coordinator.vault import VaultError
from tests.test_records import fake_pat


TOKEN = 'test-provider-token-' + 'Zq9' * 8
GH = '#!/bin/sh\ncat "$FAKE_GH_VISIBILITY"\n'
GITLEAKS = '''#!/bin/sh
echo "$*" >> "$FAKE_GITLEAKS_LOG"
case "$*" in *--staged*) printf '[]'; exit 0;; esac
for arg; do case "$arg" in --log-opts=*) span="${arg#--log-opts=}";; esac; target="$arg"; done
count=$(git -C "$target" log --format=tformat:commit --numstat --diff-filter=d "$span" |
  awk '/^commit$/ {seen = 0; next} NF && !seen && !($1 == "0" && $2 == "0") {count++; seen = 1} END {print count + 0}')
echo "INF ${FAKE_GITLEAKS_COUNT:-$count} commits scanned." >&2
if [ -s "$FAKE_GITLEAKS_FINDING" ]; then cat "$FAKE_GITLEAKS_FINDING"; exit 1; fi
printf '[]'
'''


class LocalRemote(records.GitHubRemote):
    """A local bare repository behind the GitHub checks. The fake gh answers; a bearer token rides in git_env."""

    name = 'Local'
    hosts = ('',)

    def __init__(self, remote):
        records.Remote.__init__(self, remote)
        self.repository = 'owner/ledger'

    def git_env(self):
        return {'GIT_CONFIG_COUNT': '1', 'GIT_CONFIG_KEY_0': 'http.extraHeader',
                'GIT_CONFIG_VALUE_0': 'Authorization: Bearer ' + TOKEN}


class Vault:
    def __init__(self, values):
        self.values = values

    def scrub_values(self):
        return self.values


class BrokenVault:
    def scrub_values(self):
        raise VaultError('io')


async def direct(*args):
    return await asyncio.to_thread(records.sync, *args)


def script(path, text):
    path.write_text(text)
    path.chmod(0o755)


class RemoteTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.remote = self.base / 'remote.git'
        subprocess.run(['git', 'init', '-q', '--bare', str(self.remote)], check=True)
        self.bin = self.base / 'bin'
        self.bare = self.base / 'bare'
        for folder in (self.bin, self.bare):
            folder.mkdir()
            script(folder / 'gh', GH)
        script(self.bin / 'gitleaks', GITLEAKS)
        self.visibility = self.base / 'visibility'
        self.visibility.write_text('PRIVATE\n')
        self.finding = self.base / 'finding.json'
        self.finding.write_text('')
        self.leaklog = self.base / 'gitleaks.log'
        self.path = os.environ.get('PATH', '/usr/bin:/bin')
        environment = patch.dict(os.environ, {'PATH': str(self.bin) + ':' + self.path,
                                              'FAKE_GH_VISIBILITY': str(self.visibility),
                                              'FAKE_GITLEAKS_FINDING': str(self.finding),
                                              'FAKE_GITLEAKS_LOG': str(self.leaklog)})
        environment.start()
        self.addCleanup(environment.stop)
        providers = patch.object(records, 'PROVIDERS', (records.GitHubRemote, LocalRemote))
        providers.start()
        self.addCleanup(providers.stop)
        self.store = self.make_store('state')

    def make_store(self, name):
        store = Store(self.base / name)
        self.addCleanup(store.close)
        with store.db:
            store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd) VALUES ('-1:2',-1,2,'Test','')")
        return store

    def without_gitleaks(self):
        folders = [folder for folder in self.path.split(':') if folder and not (Path(folder) / 'gitleaks').exists()]
        return patch.dict(os.environ, {'PATH': ':'.join([str(self.bare), *folders])})

    def sync(self, store=None, install='alpha', push=True, remote=None, known=()):
        store = store or self.store
        records.write_chats(store)
        return records.sync(store.directory, remote or str(self.remote), install, push=push, known=known)

    def git(self, store, *args):
        return subprocess.run(['git', '-C', str(store.directory / 'records'), *args], check=True,
                              capture_output=True, text=True).stdout.strip()

    def head(self, store=None):
        return self.git(store or self.store, 'rev-parse', 'HEAD')

    def remote_git(self, *args, check=True):
        return subprocess.run(['git', '--git-dir', str(self.remote), *args], check=check,
                              capture_output=True, text=True).stdout.strip()

    def remote_ref(self, install='alpha'):
        return self.remote_git('rev-parse', '--verify', '-q', 'refs/heads/install/' + install, check=False)

    def relative(self, store, job, suffix='.md'):
        return str(records.path(store.directory, job).with_suffix(suffix).relative_to(store.directory / 'records'))

    def enable(self, store=None, install='alpha'):
        store = store or self.store
        with store.db:
            store.put('records_sync', True)
            store.put('records_sync_remote', str(self.remote))
            store.put('records_sync_install', install)

    def problems(self, code, store=None):
        store = store or self.store
        return store.db.execute("SELECT COUNT(*) FROM problems WHERE area='records' AND code=?", (code,)).fetchone()[0]

    def history_scans(self):
        return [line for line in self.leaklog.read_text().splitlines() if '--staged' not in line]

    def test_install_pushes_to_its_own_branch_with_ledger(self):
        self.store.task_create('-1:2', 'First')
        result = self.sync()
        self.assertEqual((result.get('push_error'), result['ahead']), (None, 0))
        self.assertEqual(self.remote_ref(), self.head())
        self.assertEqual(self.remote_git('for-each-ref', '--format=%(refname)', 'refs/heads/'), 'refs/heads/install/alpha')
        self.assertEqual(json.loads(self.remote_git('show', 'install/alpha:ledger.json')), {'format': 2, 'install': 'alpha'})

    def test_offline_for_three_ticks_then_online_delivers_all_commits_in_order(self):
        job = self.store.task_create('-1:2', 'First')
        self.enable()
        state = {}
        with patch.object(records, 'sync_async', direct):
            asyncio.run(records.commit_tick(self.store, state, now=0))
            self.assertEqual(self.remote_ref(), self.head())
            away = self.base / 'away.git'
            self.remote.rename(away)
            for index, now in enumerate((100, 1000, 2000)):
                self.store.task_update(job['id'], 'cli', note='offline %d' % index)
                result = asyncio.run(records.commit_tick(self.store, state, now=now))
                self.assertEqual((result['push_error'], result['ahead']), ('not_found', index + 1))
            self.assertEqual(self.store.get('records_sync_status')['error'], 'push failed: not_found')
            self.assertEqual(self.problems('push-failed'), 1)
            away.rename(self.remote)
            result = asyncio.run(records.commit_tick(self.store, state, now=10000))
        self.assertEqual((result.get('push_error'), result['ahead']), (None, 0))
        self.assertEqual(self.remote_git('log', '--format=%H', 'install/alpha'), self.git(self.store, 'log', '--format=%H'))
        self.assertEqual(len(self.remote_git('log', '--format=%H', 'install/alpha').split()), 4)
        self.assertNotIn('error', self.store.get('records_sync_status'))

    def test_public_or_unreadable_visibility_refuses_push(self):
        self.store.task_create('-1:2', 'First')
        for answer in ('PUBLIC\n', 'INTERNAL\n', ''):
            self.visibility.write_text(answer)
            result = self.sync()
            self.assertEqual((result['push_error'], result['ahead']), ('not_private', 1))
        self.visibility.unlink()
        self.assertEqual(self.sync()['push_error'], 'push_failed')
        self.assertEqual(self.remote_ref(), '')

    def test_check_private_that_answers_public_or_raises_gives_no_push(self):
        self.store.task_create('-1:2', 'First')
        with patch.object(LocalRemote, 'check_private', return_value='public'):
            self.assertEqual(self.sync()['push_error'], 'not_private')
        with patch.object(LocalRemote, 'check_private', side_effect=OSError('host api down')):
            self.assertEqual(self.sync()['push_error'], 'push_failed')
        self.assertEqual(self.remote_ref(), '')

    def test_github_remote_checks_visibility_with_gh(self):
        self.store.task_create('-1:2', 'First')
        records.write_chats(self.store)
        records.sync(self.store.directory)
        self.visibility.write_text('PUBLIC\n')
        for remote in ('https://github.com/example/records.git', 'git://github.com/example/records',
                       'git@github.com:example/records.git'):
            with self.subTest(remote=remote):
                result = self.sync(remote=remote)
                self.assertEqual(result['push_error'], 'not_private')
                self.assertGreater(result['ahead'], 0)

    def test_unknown_host_is_refused_without_network(self):
        self.store.task_create('-1:2', 'First')
        with patch.object(records, 'PROVIDERS', (records.GitHubRemote,)):
            result = self.sync()
        self.assertEqual((result['push_error'], result['ahead']), ('unknown_host', 1))
        result = self.sync(remote='https://git.example.invalid/owner/ledger.git')
        self.assertEqual(result['push_error'], 'unknown_host')
        self.assertFalse(self.leaklog.exists() and self.history_scans())
        with self.assertRaises(ValueError):
            records.provider('https://gitlab.com/owner/ledger.git')

    def test_wiped_records_recover_from_install_branch_and_continue_fast_forward(self):
        first = self.store.task_create('-1:2', 'First')
        second = self.store.task_create('-1:2', 'Second')
        self.store.task_update(first['id'], 'cli', note='before wipe')
        append = records.journal_append
        with patch.object(records, 'journal_append', side_effect=lambda store, task, entries: append(store, task, entries, 1)):
            self.store.task_update(first['id'], 'cli', note='in remote part')
        self.sync()
        pushed = self.head()
        target = records.journal_path(self.store.directory, first)
        self.assertTrue(records.journal_part_path(target, 1).exists())
        shutil.move(str(self.store.directory / 'records'), str(self.base / 'wiped'))
        self.store.task_update(first['id'], 'cli', note='after wipe')
        for text in ('in overlapping local part', 'in local-only part'):
            append(self.store, first, [dict(at='x', by='cli', kind='note', text=text)], 1)
        transfer = records.append_journal_parts

        def fail_transfer(path, lines, limit):
            transfer(path, lines[:1], limit)
            raise OSError('interrupted transfer')

        with patch.object(records, 'append_journal_parts', side_effect=fail_transfer):
            with self.assertRaisesRegex(OSError, 'interrupted transfer'):
                self.sync()
        result = self.sync()
        self.assertEqual((result['recovered'], result['restored'], result.get('push_error')), (pushed, [], None))
        self.assertGreaterEqual(result['commits'], 1)
        self.assertEqual(self.remote_ref(), self.head())
        self.git(self.store, 'merge-base', '--is-ancestor', pushed, 'HEAD')
        self.assertTrue(records.path(self.store.directory, second).exists())
        texts = [entry['text'] for entry in records.journal(target)]
        self.assertEqual(texts, ['before wipe', 'in remote part', 'after wipe', 'in overlapping local part',
                                 'in local-only part'])
        self.assertEqual(self.git(self.store, 'status', '--porcelain', '--', '*.md', '*.jsonl', 'chats.yaml', 'ledger.json'), '')

    def test_wiped_records_with_remote_offline_commit_nothing_until_recovered(self):
        self.store.task_create('-1:2', 'First')
        self.sync()
        pushed = self.head()
        shutil.move(str(self.store.directory / 'records'), str(self.base / 'wiped'))
        records.backfill(self.store)
        away = self.base / 'away.git'
        self.remote.rename(away)
        with self.assertRaises(records.GitFailed):
            self.sync()
        self.assertTrue((self.store.directory / 'records' / '.git' / 'torii-recover').exists())
        away.rename(self.remote)
        self.assertEqual(self.sync()['recovered'], pushed)
        self.assertEqual(self.remote_ref(), self.head())

    def test_diverged_remote_gives_no_push_and_one_problem_row(self):
        job = self.store.task_create('-1:2', 'First')
        self.sync()
        other = self.base / 'other'
        subprocess.run(['git', 'init', '-q', str(other)], check=True)
        (other / 'note').write_text('other history\n')
        subprocess.run(['git', '-C', str(other), 'add', 'note'], check=True)
        subprocess.run(['git', '-C', str(other), '-c', 'user.name=T', '-c', 'user.email=t@example.invalid',
                        'commit', '-qm', 'other'], check=True)
        subprocess.run(['git', '-C', str(other), 'push', '-q', '--force', str(self.remote), 'HEAD:refs/heads/install/alpha'],
                       check=True)
        moved = self.remote_ref()
        self.enable()
        state = {}
        with patch.object(records, 'sync_async', direct):
            for index, now in enumerate((0, 100, 1000)):
                self.store.task_update(job['id'], 'cli', note='change %d' % index)
                result = asyncio.run(records.commit_tick(self.store, state, now=now))
                self.assertEqual(result['push_error'], 'diverged')
        self.assertEqual(self.remote_ref(), moved)
        self.assertEqual(self.problems('push-diverged'), 1)
        self.assertEqual(self.problems('push-failed'), 0)
        self.assertEqual(self.store.get('records_sync_status')['error'], 'push failed: diverged')

    def test_second_store_with_the_same_name_is_not_pushed(self):
        self.store.task_create('-1:2', 'First')
        self.sync()
        pushed = self.remote_ref()
        other = self.make_store('other')
        other.task_create('-1:2', 'Other history')
        records.write_chats(other)
        records.sync(other.directory)
        self.enable(other)
        with patch.object(records, 'sync_async', direct):
            result = asyncio.run(records.commit_tick(other, {}, now=0))
        self.assertEqual(result['push_error'], 'diverged')
        self.assertEqual(self.remote_ref(), pushed)
        self.assertEqual(self.problems('push-diverged', other), 1)

    def test_second_store_with_another_name_pushes_its_own_branch(self):
        job = self.store.task_create('-1:2', 'First')
        other = self.make_store('other')
        peer = other.task_create('-1:2', 'Peer')
        for _ in range(2):
            self.assertIsNone(self.sync().get('push_error'))
            self.assertIsNone(self.sync(other, 'beta').get('push_error'))
            self.store.task_update(job['id'], 'cli', note='more')
            other.task_update(peer['id'], 'cli', note='more')
        self.assertIsNone(self.sync().get('push_error'))
        self.assertIsNone(self.sync(other, 'beta').get('push_error'))
        self.assertEqual(self.remote_ref('alpha'), self.head())
        self.assertEqual(self.remote_ref('beta'), self.head(other))
        self.assertEqual(self.remote_git('log', '--format=%H', 'install/beta'), self.git(other, 'log', '--format=%H'))
        self.assertEqual(json.loads(self.remote_git('show', 'install/beta:ledger.json'))['install'], 'beta')

    def test_install_change_after_a_push_is_refused(self):
        self.store.task_create('-1:2', 'First')
        self.sync()
        result = self.sync(install='beta')
        self.assertEqual(result['push_error'], 'install_changed')
        self.assertEqual(self.remote_ref('beta'), '')

    def test_restore_from_a_clone_of_the_install_branch_passes_check(self):
        job = self.store.task_create('-1:2', 'First')
        self.store.task_update(job['id'], 'cli', note='a note')
        closed = self.store.task_create('-1:2', 'Done', parent=job['id'])
        self.store.task_update(closed['id'], status='done')
        self.sync()
        clone = self.base / 'clone'
        subprocess.run(['git', 'clone', '-q', '-b', 'install/alpha', str(self.remote), str(clone)], check=True)
        recovered = Store(self.base / 'restored')
        self.addCleanup(recovered.close)
        self.assertEqual(records.restore(recovered, clone), {'records': 2, 'topics': 1})
        self.assertEqual(records.check(recovered)['count'], 0, records.check(recovered))
        self.assertEqual(recovered.task_get(job['id'])['notes'], 'a note')

    def test_provider_token_stays_out_of_git_arguments_and_config(self):
        self.store.task_create('-1:2', 'First')
        calls = []
        real = subprocess.run
        def spy(command, **kwargs):
            calls.append((list(command), dict(kwargs.get('env') or {})))
            return real(command, **kwargs)
        with patch('subprocess.run', side_effect=spy):
            result = self.sync()
        self.assertIsNone(result.get('push_error'))
        self.assertEqual(self.remote_ref(), self.head())
        self.assertFalse([command for command, _ in calls if any(TOKEN in argument for argument in command)])
        pushes = [env for command, env in calls if 'push' in command]
        self.assertEqual(len(pushes), 1)
        self.assertIn(TOKEN, pushes[0]['GIT_CONFIG_VALUE_0'])
        self.assertNotIn(TOKEN, (self.store.directory / 'records' / '.git' / 'config').read_text())
        self.assertNotIn(TOKEN, json.dumps(result))

    def test_push_without_gitleaks_is_refused_and_local_commits_go_on(self):
        job = self.store.task_create('-1:2', 'First')
        with self.without_gitleaks():
            result = self.sync()
            self.assertEqual((result['push_error'], result['commits'], result['ahead']), ('gitleaks_missing', 1, 1))
            self.store.task_update(job['id'], 'cli', note='more')
            self.assertEqual(self.sync()['ahead'], 2)
        self.assertEqual(self.remote_ref(), '')
        self.assertFalse(self.leaklog.exists())
        self.assertIsNone(self.sync().get('push_error'))
        self.assertEqual(self.remote_ref(), self.head())

    def test_full_history_scan_before_first_push_then_new_commits_only(self):
        job = self.store.task_create('-1:2', 'First')
        with self.without_gitleaks():
            records.write_chats(self.store)
            records.sync(self.store.directory)
        self.finding.write_text(json.dumps([{'RuleID': 'github-pat', 'File': self.relative(self.store, job),
                                             'Commit': 'HEAD', 'StartLine': 1}]))
        result = self.sync()
        self.assertEqual((result['push_error'], result['history_blocked']), ('secret_in_history', self.relative(self.store, job)))
        self.assertEqual(self.remote_ref(), '')
        self.finding.write_text('')
        self.assertIsNone(self.sync().get('push_error'))
        self.assertEqual(self.remote_ref(), self.head())
        self.assertEqual([('--log-opts=refs/torii/scanned..' in line) for line in self.history_scans()], [False, False])
        pushed = self.head()
        self.store.task_update(job['id'], 'cli', note='next')
        self.assertIsNone(self.sync().get('push_error'))
        self.assertIn('--log-opts=refs/torii/scanned..' + self.head(), self.history_scans()[-1])
        self.assertNotEqual(pushed, self.head())

    def test_history_secret_blocks_push_and_records_one_problem(self):
        job = self.store.task_create('-1:2', 'First')
        self.finding.write_text(json.dumps([{'RuleID': 'github-pat', 'File': self.relative(self.store, job),
                                             'Commit': 'HEAD', 'StartLine': 1}]))
        self.enable()
        with patch.object(records, 'sync_async', direct):
            result = asyncio.run(records.commit_tick(self.store, {}, now=0))
        self.assertEqual(result['push_error'], 'secret_in_history')
        self.assertEqual(self.problems('push-blocked'), 1)
        self.assertEqual(self.remote_ref(), '')

    @unittest.skipUnless(shutil.which('gitleaks'), 'gitleaks required')
    def test_real_gitleaks_finds_a_secret_committed_without_gitleaks(self):
        job = self.store.task_create('-1:2', 'First')
        secret = fake_pat()
        with self.without_gitleaks(), patch.object(records, 'scan_content', return_value=False):
            records.journal_append(self.store, job, [{'at': 'x', 'by': 'cli', 'kind': 'note', 'text': 'GITHUB=' + secret}])
            records.write_chats(self.store)
            records.sync(self.store.directory)
        with patch.dict(os.environ, {'PATH': str(self.bare) + ':' + self.path}):
            result = self.sync()
        self.assertEqual((result['push_error'], result['history_blocked']),
                         ('secret_in_history', self.relative(self.store, job, '.jsonl')))
        self.assertEqual(self.remote_ref(), '')

    def test_known_token_value_blocks_only_that_job(self):
        secret = 'plain-known-value-' + 'q' * 12
        self.assertEqual(records.redact('Deploy with ' + secret)[1], 0)
        clean = self.store.task_create('-1:2', 'Clean job')
        dirty = self.store.task_create('-1:2', 'Deploy with ' + secret)
        self.store.vault = Vault([secret])
        with patch.object(records, 'sync_async', direct):
            result = asyncio.run(records.commit_tick(self.store, {}))
        self.assertEqual(result['blocked'], [self.relative(self.store, dirty, '')])
        log = self.git(self.store, 'log', '-p')
        self.assertNotIn(secret, log)
        self.assertIn(self.relative(self.store, clean), log)
        self.assertEqual(self.problems('commit-blocked'), 1)

    def test_known_value_in_history_blocks_the_first_push(self):
        secret = 'plain-known-value-' + 'q' * 12
        self.assertEqual(records.redact('Deploy with ' + secret)[1], 0)
        job = self.store.task_create('-1:2', 'Deploy with ' + secret)
        records.write_chats(self.store)
        records.sync(self.store.directory)
        self.assertIn(secret, self.git(self.store, 'log', '-p'))
        result = self.sync(known=[secret])
        self.assertEqual((result['push_error'], result['history_blocked']), ('secret_in_history', self.relative(self.store, job)))
        self.assertEqual(self.remote_ref(), '')

    def test_known_value_in_new_commits_blocks_an_incremental_push(self):
        secret = 'plain-known-value-' + 'w' * 12
        job = self.store.task_create('-1:2', 'First')
        self.assertIsNone(self.sync().get('push_error'))
        pushed = self.remote_ref()
        self.store.task_update(job['id'], 'cli', note='use ' + secret)
        records.write_chats(self.store)
        records.sync(self.store.directory)
        result = records.sync(self.store.directory, str(self.remote), 'alpha', push=True, known=[secret], known_seen=True)
        self.assertEqual((result['push_error'], result['history_blocked']),
                         ('secret_in_history', self.relative(self.store, job, '.jsonl')))
        self.assertEqual(self.remote_ref(), pushed)
        self.assertNotIn(secret, self.remote_git('log', '-p', 'refs/heads/install/alpha'))

    def test_value_saved_in_the_vault_after_a_push_blocks_later_pushes(self):
        secret = 'plain-known-value-' + 'e' * 12
        job = self.store.task_create('-1:2', 'First')
        self.enable()
        state = {}
        with patch.object(records, 'sync_async', direct):
            asyncio.run(records.commit_tick(self.store, state, now=0))
            self.store.task_update(job['id'], 'cli', note='use ' + secret)
            self.assertIsNone(asyncio.run(records.commit_tick(self.store, state, now=100)).get('push_error'))
            pushed = self.remote_ref()
            self.store.vault = Vault([secret])
            self.store.task_create('-1:2', 'Second')
            result = asyncio.run(records.commit_tick(self.store, state, now=200))
        self.assertEqual((result['push_error'], result['history_blocked']),
                         ('secret_in_history', self.relative(self.store, job, '.jsonl')))
        self.assertEqual(self.remote_ref(), pushed)
        detail = self.store.db.execute("SELECT detail FROM problems WHERE area='records' AND code='push-blocked'").fetchall()
        self.assertEqual(len(detail), 1)
        self.assertIn('rotate the value', detail[0][0])
        self.assertIn('install/alpha', detail[0][0])
        self.assertNotIn(secret, detail[0][0])

    def test_vault_that_cannot_load_refuses_the_push(self):
        self.store.task_create('-1:2', 'First')
        self.enable()
        self.store.vault = BrokenVault()
        state = {}
        with patch.object(records, 'sync_async', direct):
            result = asyncio.run(records.commit_tick(self.store, state, now=0))
            self.assertEqual((result['push_error'], result['commits'], result['ahead']), ('vault_unavailable', 1, 1))
            self.assertEqual(self.remote_ref(), '')
            self.assertEqual(self.problems('push-failed'), 1)
            self.store.vault = Vault([])
            result = asyncio.run(records.commit_tick(self.store, state, now=1000))
        self.assertIsNone(result.get('push_error'))
        self.assertEqual(self.remote_ref(), self.head())

    def test_gitleaks_commit_count_that_differs_from_git_refuses_the_push(self):
        self.store.task_create('-1:2', 'First')
        with patch.dict(os.environ, {'FAKE_GITLEAKS_COUNT': '0'}):
            self.assertEqual(self.sync()['push_error'], 'scan_incomplete')
        with patch.dict(os.environ, {'FAKE_GITLEAKS_COUNT': 'none'}):
            self.assertEqual(self.sync()['push_error'], 'scan_incomplete')
        self.assertEqual(self.remote_ref(), '')
        self.assertIsNone(self.sync().get('push_error'))
        self.assertEqual(self.remote_ref(), self.head())

    def commit_by_hand(self, *args):
        self.git(self.store, '-c', 'user.name=Test', '-c', 'user.email=test@localhost', '-c', 'commit.gpgsign=false',
                 'commit', '-q', *args)

    def real_gitleaks(self, *log_opts):
        """The real gitleaks first on PATH. Extra `log_opts` join its span to make it skip commits."""
        real = shutil.which('gitleaks', path=self.path)
        script(self.bare / 'gitleaks', '#!/bin/sh\nfor arg; do case "$arg" in --log-opts=*) set -- "$@" "$arg %s";;'
               ' *) set -- "$@" "$arg";; esac; shift; done\nexec %s "$@"\n' % (' '.join(log_opts), real)
               if log_opts else '#!/bin/sh\nexec %s "$@"\n' % real)
        return patch.dict(os.environ, {'PATH': str(self.bare) + ':' + self.path})

    def add_and_delete_a_file(self):
        """A commit that adds a file, then one that only deletes it. Gitleaks counts no deletion-only commit."""
        records.write_chats(self.store)
        records.sync(self.store.directory)
        extra = self.store.directory / 'records' / 'extra.md'
        extra.write_text('one\ntwo\n')
        self.git(self.store, 'add', 'extra.md')
        self.commit_by_hand('-m', 'extra-added')
        self.git(self.store, 'rm', '-q', 'extra.md')
        self.commit_by_hand('-m', 'delete extra')
        self.commit_by_hand('--allow-empty', '-m', 'empty')

    @unittest.skipUnless(shutil.which('gitleaks'), 'gitleaks required')
    def test_real_gitleaks_passes_a_span_with_a_deletion_only_commit(self):
        self.store.task_create('-1:2', 'First')
        self.add_and_delete_a_file()
        with self.real_gitleaks():
            self.assertIsNone(self.sync().get('push_error'))
            self.assertEqual(self.remote_ref(), self.head())
            self.store.task_create('-1:2', 'Second')
            self.add_and_delete_a_file()
            self.assertIsNone(self.sync().get('push_error'))
        self.assertEqual(self.remote_ref(), self.head())

    @unittest.skipUnless(shutil.which('gitleaks'), 'gitleaks required')
    def test_real_gitleaks_that_skips_a_content_commit_refuses_the_push(self):
        self.store.task_create('-1:2', 'First')
        self.add_and_delete_a_file()
        with self.real_gitleaks('--invert-grep', '--grep=extra-added'):
            self.assertEqual(self.sync()['push_error'], 'scan_incomplete')
        self.assertEqual(self.remote_ref(), '')
        with self.real_gitleaks():
            self.assertIsNone(self.sync().get('push_error'))
            self.store.task_create('-1:2', 'Second')
            self.add_and_delete_a_file()
        with self.real_gitleaks('--invert-grep', '--grep=extra-added'):
            self.assertEqual(self.sync()['push_error'], 'scan_incomplete')
        self.assertNotEqual(self.remote_ref(), self.head())

    @unittest.skipUnless(shutil.which('gitleaks'), 'gitleaks required')
    def test_real_gitleaks_refuses_a_merge_that_adds_a_secret(self):
        self.store.task_create('-1:2', 'First')
        with self.real_gitleaks():
            self.assertIsNone(self.sync().get('push_error'))
        pushed = self.remote_ref()
        main = self.git(self.store, 'symbolic-ref', '--short', 'HEAD')
        records_dir = self.store.directory / 'records'
        self.git(self.store, 'checkout', '-q', '-b', 'side')
        (records_dir / 'side.md').write_text('side\n')
        self.git(self.store, 'add', 'side.md')
        self.commit_by_hand('-m', 'side')
        self.git(self.store, 'checkout', '-q', main)
        (records_dir / 'main.md').write_text('main\n')
        self.git(self.store, 'add', 'main.md')
        self.commit_by_hand('-m', 'main')
        self.git(self.store, '-c', 'user.name=Test', '-c', 'user.email=test@localhost', 'merge', '-q', '--no-commit', 'side')
        (records_dir / 'evil.txt').write_text('GITHUB=' + fake_pat() + '\n')
        self.git(self.store, 'add', 'evil.txt')
        self.commit_by_hand('-m', 'merge')
        self.store.task_create('-1:2', 'Second')
        with self.real_gitleaks():
            self.assertEqual(self.sync()['push_error'], 'scan_incomplete')
        self.assertEqual(self.remote_ref(), pushed)

    @unittest.skipUnless(shutil.which('gitleaks'), 'gitleaks required')
    def test_real_gitleaks_scans_the_root_commit_when_config_hides_it(self):
        job = self.store.task_create('-1:2', 'First')
        with self.without_gitleaks(), patch.object(records, 'scan_content', return_value=False):
            records.journal_append(self.store, job, [{'at': 'x', 'by': 'cli', 'kind': 'note', 'text': 'GITHUB=' + fake_pat()}])
            records.write_chats(self.store)
            records.sync(self.store.directory)
            self.store.task_create('-1:2', 'Second')
            records.write_chats(self.store)
            records.sync(self.store.directory)
        self.assertEqual(self.git(self.store, 'rev-list', '--count', 'HEAD'), '2')
        self.git(self.store, 'config', 'log.showRoot', 'false')
        with self.real_gitleaks():
            result = self.sync()
        self.assertEqual((result['push_error'], result['history_blocked']),
                         ('secret_in_history', self.relative(self.store, job, '.jsonl')))
        self.assertEqual(self.remote_ref(), '')

    def test_record_title_and_ask_hold_no_known_value(self):
        secret = 'plain-known-value-' + 'r' * 12
        self.store.vault = Vault([secret])
        job = self.store.task_create('-1:2', 'Deploy with ' + secret)
        text = records.path(self.store.directory, job).read_text()
        self.assertNotIn(secret, text)
        self.assertEqual(text.count('Deploy with [token]'), 2)

    def test_recovery_merge_keeps_a_journal_line_appended_during_the_merge(self):
        job = self.store.task_create('-1:2', 'First')
        self.store.task_update(job['id'], 'cli', note='before wipe')
        self.sync()
        shutil.move(str(self.store.directory / 'records' / '.git'), str(self.base / 'wiped.git'))
        self.store.task_update(job['id'], 'cli', note='after wipe')
        real, appends = records.entry_of, []

        def racing(line):
            if not appends:
                entry = {'at': 'x', 'by': 'cli', 'kind': 'note', 'text': 'during merge'}
                writer = threading.Thread(target=records.journal_append, args=(self.store, job, [entry]))
                writer.start()
                writer.join(0.3)
                appends.append((writer, writer.is_alive()))
            return real(line)
        with patch.object(records, 'entry_of', racing):
            result = self.sync()
        writer, waited = appends[0]
        writer.join(10)
        self.assertTrue(result.get('recovered'))
        self.assertTrue(waited, 'the append did not wait for the merge')
        texts = [entry['text'] for entry in records.journal(records.journal_path(self.store.directory, job))]
        self.assertEqual(texts, ['before wipe', 'after wipe', 'during merge'])

    def test_new_install_with_a_taken_name_is_refused_and_keeps_its_own_history(self):
        self.store.task_create('-1:2', 'Alpha job of install one')
        self.sync()
        before = self.remote_ref()
        other = self.make_store('other')
        mine = other.task_create('-1:2', 'Unrelated job of install two')
        self.enable(other)
        state = {}
        with patch.object(records, 'sync_async', direct):
            for now in (0, 30, 100, 1000):
                result = asyncio.run(records.commit_tick(other, state, now=now))
                self.assertNotIn('recovered', result)
            self.assertEqual(result['push_error'], 'install_name_taken')
            self.assertEqual(self.remote_ref(), before)
            self.assertEqual(self.problems('install-name-taken', other), 1)
            self.assertIn('records.sync enabled=true install=NAME', other.db.execute(
                "SELECT detail FROM problems WHERE code='install-name-taken'").fetchone()[0])
            self.assertIn('Unrelated job of install two', self.git(other, 'show', 'HEAD:' + self.relative(other, mine)))
            with other.db:
                other.put('records_sync_install', 'beta')
            result = asyncio.run(records.commit_tick(other, state, now=10000))
        self.assertIsNone(result.get('push_error'))
        self.assertEqual(self.remote_ref(), before)
        self.assertEqual(self.remote_ref('beta'), self.head(other))

    def test_reason_codes_carry_no_provider_text(self):
        remote = records.GitHubRemote('git@github.com:owner/ledger.git')
        cases = {'fatal: Authentication failed for https://github.com/owner/ledger.git': 'auth_failed',
                 'ERROR: Permission to owner/ledger.git denied to someone.': 'forbidden',
                 'remote: API rate limit exceeded': 'rate_limited',
                 'ERROR: Repository not found.': 'not_found',
                 ' ! [rejected] HEAD -> install/alpha (non-fast-forward)': 'diverged',
                 'ssh: Could not resolve host: github.com': 'offline',
                 'something else ' + TOKEN: 'push_failed'}
        for detail, code in cases.items():
            with self.subTest(code=code):
                self.assertEqual(remote.reason(records.GitFailed('Failed: git', detail)), code)
        self.assertEqual(remote.reason(records.PushRefused('not_private')), 'not_private')
        self.assertEqual(remote.reason(TimeoutError()), 'timed_out')


if __name__ == '__main__':
    unittest.main()
