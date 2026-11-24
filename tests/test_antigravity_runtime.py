"""Offline Antigravity worker checks against the fake agy CLI and the live 1.5.12 capture."""

import asyncio
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import uuid
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from coordinator import host_os
from coordinator import antigravity_accounts, antigravity_runtime, control_api
from coordinator.accounts import AccountBroker
from coordinator.control_api import Refused
from coordinator.extension import Native, optional
from coordinator.host import HostClient
from coordinator.native_protocol import RunControl
from coordinator.providers import ProviderRunner, RunResult
from coordinator.service import Service
from coordinator.store import Store
from coordinator.workers import WorkerPool
from tests.fake_agy import USAGE, install_fake_agy, sign_in
from tests.support import read_jsonl, stop_test_hosts, until


usage_events = optional('usage_events')
FIXTURE = Path(__file__).with_name('fixtures') / 'agy-acp-1.5.12.jsonl'
CHECK_SANDBOX = antigravity_runtime.check_sandbox


def capture(probe):
    return [json.loads(line) for line in FIXTURE.read_text().splitlines() if json.loads(line)['probe'] == probe]


def bwrap_works():
    binary = shutil.which('bwrap')
    if not host_os.linux() or not binary:
        return False
    try:
        return subprocess.run([binary, '--ro-bind', '/', '/', '--', '/bin/true'], capture_output=True,
                              timeout=20).returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


class AntigravityCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.addCleanup(stop_test_hosts, self.root)
        self.home = self.root / 'owner-home'
        self.home.mkdir()
        env = patch.dict(os.environ, {'HOME': str(self.home), 'GEMINI_API_KEY': 'ambient-key'})
        env.start()
        self.addCleanup(env.stop)
        self.binary = install_fake_agy(self.home / '.local/share/agy/bin')
        self.account = sign_in(self.home / '.local/share/agy/accounts/one')
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        self.cwd = self.root / 'worktree'
        self.cwd.mkdir()
        scratch = self.root / 'worker-temp'
        scratch.mkdir()
        temp_base = patch('coordinator.worker_storage.temp_base', return_value=scratch.resolve())
        temp_base.start()
        self.addCleanup(temp_base.stop)
        self.boundaries = []

        def boundary(argv, roots, *, state=None, hidden=(), home=None, binds=(), mounts=()):
            self.boundaries.append({'roots': [str(root) for root in roots], 'state': str(state),
                                    'hidden': [str(path) for path in hidden],
                                    'home': [str(path) for path in home or ()],
                                    'binds': [(str(source), str(place)) for source, place in binds]})
            sessions = next((str(Path(source).parent) for source, place in binds
                             if str(place).endswith('antigravity-cli/conversations')), None)
            return ['/usr/bin/env', 'TORII_FAKE_SESSIONS=' + sessions, *argv] if sessions else list(argv)

        (self.root / 'shm').mkdir()
        for fixture in (patch('coordinator.antigravity_runtime.write_boundary', boundary),
                        patch('coordinator.antigravity_runtime.check_sandbox', return_value='/usr/bin/bwrap'),
                        patch('coordinator.antigravity_runtime.volatile_root', return_value=self.root / 'shm'),
                        patch('coordinator.antigravity_runtime.pwd.getpwuid', return_value=SimpleNamespace(pw_dir=str(self.home))),
                        patch('coordinator.antigravity_accounts.DEFAULT_MODEL', 'model-a')):
            fixture.start()
            self.addCleanup(fixture.stop)
        self.runner = ProviderRunner(self.store.directory, opencode_store=self.store)

    def cli(self, op, **params):
        return control_api.call(self.store, op, params, source='cli')

    def register(self, alias='one', sandbox=True):
        result = self.cli('antigravity.account', alias=alias)
        self.assertTrue(result.ok, result.text)
        self.store.put('antigravity_enabled', True)
        self.store.put('antigravity_sandbox', sandbox)

    def scenario(self, **options):
        (self.binary.parent / 'scenario.json').write_text(json.dumps(options))

    def calls(self, method=None):
        rows = read_jsonl(self.binary.parent / 'calls.jsonl')
        return [row for row in rows if method is None or (row.get('message') or {}).get('method') == method]

    def job(self):
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test',?,1)",
                              (str(self.cwd),))
        self.store.put('coordinator_home_topic', '1:2')
        return self.store.task_create('1:2', 'Build', worktree=str(self.cwd))

    def spawn(self, task, **options):
        return control_api.call(self.store, 'workers.spawn', {'task': task['id'], 'provider': 'antigravity',
                                'prompt': 'short prompt', **options}, topic='1:2')

    async def run_worker(self, session=None, prompt='short prompt', **options):
        options.setdefault('fresh', session is None)
        return await asyncio.wait_for(self.runner.run('antigravity', prompt, self.cwd, session,
                                                      worker_id=self.worker, topic='1:2', **options), 30)


class AntigravityAccountTests(AntigravityCase):
    def test_environment_is_clean_and_selects_the_profile(self):
        env = antigravity_accounts.environment(self.account)
        self.assertEqual(env['HOME'], str(self.account))
        self.assertEqual(env['XDG_CONFIG_HOME'], str(self.account / 'xdg-config'))
        self.assertNotIn('GEMINI_API_KEY', env)
        self.assertNotIn('DISPLAY', env)
        stub = Path(env['PATH'].split(os.pathsep)[0]) / 'xdg-open'
        self.assertEqual(subprocess.run([str(stub), 'https://example.test'], env=env).returncode, 0)
        link = self.root / 'link'
        signin = antigravity_accounts.environment(self.account, display=True)
        self.assertEqual(signin['DISPLAY'], ':0')
        subprocess.run([str(stub), 'https://example.test/auth'], env=dict(signin, TORII_AGY_LINK=str(link)), check=True)
        self.assertEqual(link.read_text(), 'https://example.test/auth\n')

    def test_macos_open_is_a_browser_stub_too(self):
        with patch('coordinator.antigravity_accounts.host_os.SYSTEM', 'linux'):
            env = antigravity_accounts.environment(self.account)
        stubs = Path(env['PATH'].split(os.pathsep)[0])
        self.assertFalse((stubs / 'open').exists())
        with patch('coordinator.antigravity_accounts.host_os.SYSTEM', 'darwin'):
            env = antigravity_accounts.environment(self.account, display=True)
        self.assertEqual(env['HOME'], str(self.account))
        link = self.root / 'link'
        subprocess.run([str(stubs / 'open'), 'https://example.test/auth'], env=dict(env, TORII_AGY_LINK=str(link)),
                       check=True)
        self.assertEqual(link.read_text(), 'https://example.test/auth\n')

    def test_macos_refuses_the_sandbox_setting_and_keeps_workers_on(self):
        self.register(sandbox=False)
        with patch('coordinator.antigravity_runtime.host_os.linux', return_value=False), \
                patch('coordinator.antigravity_runtime.check_sandbox', CHECK_SANDBOX):
            refused = self.cli('antigravity.sandbox', enabled=True)
            self.assertFalse(refused.ok)
            self.assertIn('On macOS, keep antigravity.sandbox off', refused.text)
            self.assertFalse(self.store.get('antigravity_sandbox'))
            self.assertTrue(self.cli('antigravity.sandbox', enabled=False).ok)
            self.assertTrue(self.cli('delegation.antigravity', enabled=True).ok)

    def test_account_homes_are_real_private_folders(self):
        self.assertEqual(antigravity_accounts.account_home('one'), self.account)
        self.assertEqual(self.account.stat().st_mode & 0o777, 0o700)
        for alias in ('../one', '', 'missing'):
            with self.subTest(alias=alias), self.assertRaises(Refused):
                antigravity_accounts.account_home(alias)
        (self.account.parent / 'link').symlink_to(self.account)
        with self.assertRaises(Refused):
            antigravity_accounts.account_home('link')

    def test_registration_is_local_and_needs_a_signed_in_profile(self):
        refused = control_api.call(self.store, 'antigravity.account', {'alias': 'one'}, source='telegram')
        self.assertFalse(refused.ok)
        (self.account.parent / 'two').mkdir(mode=0o700)
        signed_out = self.cli('antigravity.account', alias='two')
        self.assertEqual((signed_out.ok, signed_out.text),
                         (False, 'Antigravity account two is not signed in. Sign in with the official CLI first.'))
        self.assertTrue(self.cli('antigravity.account', alias='one', label='Work').ok)
        broker = antigravity_accounts.AntigravityBroker(self.store)
        self.assertEqual((broker.active(), broker.label('one'), broker.signed_in('one')), ('one', 'Work', True))
        self.assertEqual(self.store.get('antigravity_account_status')['one']['models'],
                         [{'id': 'model-a', 'name': 'Model A'}, {'id': 'model-b', 'name': 'Model B (Thinking)'}])
        self.store.put('accounts', {'claude-one': {}})
        self.assertFalse(self.cli('antigravity.account', alias='claude-one').ok)

    def test_a_failed_model_check_is_not_a_sign_out(self):
        self.scenario(models_error='network unreachable')
        with self.assertRaises(Refused):
            antigravity_accounts.read_models(self.store, 'one')

    def test_workers_are_off_until_the_owner_turns_them_on(self):
        self.cli('antigravity.account', alias='one')
        task = self.job()
        refused = self.spawn(task)
        self.assertEqual((refused.ok, refused.text), (False, 'Antigravity workers are off.'))
        with patch('coordinator.antigravity_runtime.write_boundary', side_effect=Refused('no boundary')):
            self.assertFalse(self.cli('antigravity.sandbox', enabled=True).ok)
            self.assertFalse(antigravity_runtime.sandboxed(self.store))
            self.assertTrue(self.cli('delegation.antigravity', enabled=True).ok)
            self.store.put('antigravity_sandbox', True)
            self.assertFalse(self.cli('delegation.antigravity', enabled=True).ok)
        self.assertTrue(self.cli('delegation.antigravity', enabled=True).ok)
        self.assertTrue(self.cli('antigravity.sandbox', enabled=False).ok)
        self.assertTrue(self.spawn(task).ok)
        settings = self.cli('settings.get')
        self.assertTrue(settings.ok, settings.text)
        self.assertEqual((settings.data['antigravity_enabled'], settings.data['antigravity_sandbox'],
                          settings.data['antigravity_active_account']), (True, False, 'one'))

    def test_no_setting_chooses_the_binary(self):
        planted = self.root / 'state' / 'planted-agy'
        planted.write_text('#!/bin/sh\ntouch "$0.ran"\n')
        planted.chmod(0o700)
        self.store.put('antigravity_binary', str(planted))
        self.assertEqual(antigravity_accounts.binary(self.store), str(self.binary))
        self.assertIsNone(control_api.find('antigravity.binary'))
        self.assertTrue(self.cli('antigravity.account', alias='one').ok)
        self.assertFalse(Path(str(planted) + '.ran').exists())
        self.binary.chmod(0o777)
        with self.assertRaises(Refused):
            antigravity_accounts.check_binary(antigravity_accounts.binary(self.store))


class AntigravityBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        for name in ('job', 'state/hosts', 'state/worktrees/mine', 'state/worktrees/other', 'state/cow-worktrees/task-2', 'accounts/one', 'accounts/two',
                     'jobs/one/w1/brain', 'jobs/one/w1/refs', 'git/refs', 'outside'):
            (self.root / name).mkdir(parents=True)
        self.shm = Path(tempfile.mkdtemp(prefix='torii-test-', dir='/dev/shm' if Path('/dev/shm').is_dir() else None)).resolve()
        self.addCleanup(antigravity_runtime.remove_tree, self.shm)
        for name in ('upper', 'work'):
            (self.shm / name).mkdir()
        self.home = (self.root / 'accounts/one', self.shm / 'upper', self.shm / 'work')
        self.brain = (self.root / 'jobs/one/w1/brain', self.root / 'accounts/one/.gemini/antigravity-cli/brain')

    def test_command_binds_roots_after_the_read_only_and_hidden_folders(self):
        with patch('coordinator.antigravity_runtime.host_os.linux', return_value=True), \
                patch('coordinator.antigravity_runtime.shutil.which', return_value='/usr/bin/bwrap'):
            command = antigravity_runtime.write_boundary(
                ['agy', 'acp'], [self.root / 'job', self.root / 'state/worktrees/mine'],
                state=self.root / 'state', hidden=[self.root / 'accounts', self.root / 'jobs'], home=self.home,
                binds=[self.brain, (self.root / 'jobs/one/w1/refs', self.root / 'git/refs')])
        state = str(self.root / 'state')
        self.assertEqual(command[:9], ['/usr/bin/bwrap', '--die-with-parent', '--ro-bind', '/', '/',
                                       '--dev', '/dev', '--proc', '/proc'])
        self.assertEqual(command[9:], [
            '--bind', state, state, '--ro-bind', state + '/worktrees', state + '/worktrees',
            '--ro-bind', state + '/cow-worktrees', state + '/cow-worktrees',
            '--ro-bind', state + '/hosts', state + '/hosts', '--tmpfs', str(self.root / 'accounts'),
            '--tmpfs', str(self.root / 'jobs'),
            '--overlay-src', str(self.home[0]), '--overlay', str(self.home[1]), str(self.home[2]), str(self.home[0]),
            '--bind', str(self.root / 'job'), str(self.root / 'job'),
            '--bind', state + '/worktrees/mine', state + '/worktrees/mine',
            '--bind', str(self.brain[0]), str(self.brain[1]),
            '--bind', str(self.root / 'jobs/one/w1/refs'), str(self.root / 'git/refs'), '--', 'agy', 'acp'])

    def test_refuses_without_linux_bwrap_or_safe_roots(self):
        with patch('coordinator.antigravity_runtime.host_os.linux', return_value=False), self.assertRaises(Refused):
            antigravity_runtime.write_boundary(['agy'], [self.root / 'job'])
        with patch('coordinator.antigravity_runtime.host_os.linux', return_value=True), \
                patch('coordinator.antigravity_runtime.shutil.which', return_value=None), self.assertRaises(Refused):
            antigravity_runtime.write_boundary(['agy'], [self.root / 'job'])
        with patch('coordinator.antigravity_runtime.host_os.linux', return_value=True), \
                patch('coordinator.antigravity_runtime.shutil.which', return_value='/usr/bin/bwrap'):
            for roots in ([], ['relative'], [Path('/')], [Path.home()], [Path.home().parent], [self.root / 'missing']):
                with self.subTest(roots=roots), self.assertRaises(Refused):
                    antigravity_runtime.write_boundary(['agy'], roots)

    @unittest.skipUnless(bwrap_works(), 'needs Linux with a working bubblewrap')
    def test_bubblewrap_blocks_writes_outside_the_roots(self):
        script = ('for path in "$@"; do if /usr/bin/touch "$path/probe" 2>/dev/null; '
                  'then echo "ok $path"; else echo "no $path"; fi; done; ls "$0"; '
                  'touch "$0/one/.gemini/antigravity-cli/brain/image"')
        targets = [self.root / 'job', self.root / 'state', self.root / 'state/worktrees/mine',
                   self.root / 'state/worktrees/other', self.root / 'state/cow-worktrees/task-2',
                   self.root / 'state/hosts', self.root / 'accounts/one', self.root]
        command = antigravity_runtime.write_boundary(
            ['/bin/sh', '-c', script, str(self.root / 'accounts'), *map(str, targets)],
            [self.root / 'job', self.root / 'state/worktrees/mine'],
            state=self.root / 'state', hidden=[self.root / 'accounts', self.root / 'jobs'], home=self.home,
            binds=[self.brain])
        done = subprocess.run(command, capture_output=True, text=True, timeout=30)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertEqual(done.stdout.splitlines(), [
            'ok ' + str(self.root / 'job'), 'ok ' + str(self.root / 'state'),
            'ok ' + str(self.root / 'state/worktrees/mine'), 'no ' + str(self.root / 'state/worktrees/other'),
            'no ' + str(self.root / 'state/cow-worktrees/task-2'), 'no ' + str(self.root / 'state/hosts'), 'ok ' + str(self.root / 'accounts/one'),
            'no ' + str(self.root), 'one'])
        self.assertEqual([path.name for path in (self.root / 'accounts/one').iterdir()], [])
        self.assertEqual(sorted(path.name for path in self.home[1].iterdir()), ['.gemini', 'probe'])
        self.assertEqual([path.name for path in self.brain[0].iterdir()], ['image'])

    @unittest.skipUnless(bwrap_works(), 'needs Linux with a working bubblewrap')
    def test_planted_agent_configuration_stays_out_of_the_real_account_home(self):
        account = self.root / 'accounts/one'
        (account / '.gemini/config').mkdir(parents=True)
        (account / '.gemini/config/mcp_config.json').write_text('{}')
        plant = ('mkdir -p "$HOME/.gemini/config" "$HOME/p" && printf "#!/bin/sh\\ntouch %s/escaped\\n" > "$HOME/p/x" '
                 '&& chmod 700 "$HOME/p/x" && printf \'{"mcpServers":{"x":{"command":"%s"}}}\' "$HOME/p/x" '
                 '> "$HOME/.gemini/config/mcp_config.json" && printf hook > "$HOME/.gemini/config/hooks.json" '
                 '&& cat "$HOME/.gemini/config/mcp_config.json" && ls %s') % (
                     self.root / 'outside', '%s', self.root / 'jobs')
        command = antigravity_runtime.write_boundary(
            ['/bin/sh', '-c', plant], [self.root / 'job'], hidden=[self.root / 'accounts', self.root / 'jobs'],
            home=self.home)
        done = subprocess.run(command, capture_output=True, text=True, timeout=30, env={'HOME': str(account), 'PATH': os.defpath})
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn('"command":"%s/p/x"' % account, done.stdout)
        self.assertEqual((account / '.gemini/config/mcp_config.json').read_text(), '{}')
        self.assertEqual(sorted(path.name for path in account.iterdir()), ['.gemini'])
        self.assertEqual(sorted(path.name for path in (account / '.gemini/config').iterdir()), ['mcp_config.json'])
        self.assertTrue((self.home[1] / 'p/x').is_file())
        self.assertTrue((self.home[1] / '.gemini/config/hooks.json').is_file())


SENTINELS = {
    'torii-secrets': 'home/.config/torii-secrets/example',
    'passwd-home torii-secrets': 'passwd/.config/torii-secrets/example',
    'bot token': 'home/.config/telegram-agent-coordinator/bot-token',
    'router token': 'passwd/.config/example-router/router.token',
    'codex auth': 'home/.codex/auth.json',
    'other codex account': 'home/.codex-accounts/two/auth.json',
    'claude credentials': 'home/.claude/.credentials.json',
    'other claude account': 'home/.claude-accounts/two/.credentials.json',
    'claude settings': 'home/.claude.json',
    'opencode account': 'home/.opencode-accounts/two/key',
    'gh token': 'home/.config/gh/hosts.yml',
    'netrc': 'home/.netrc',
    'vault key': 'state/envelope/master.key',
    'vault': 'state/envelope/vault.enc',
    'shared mcp headers': 'state/shared-mcp.json',
    'worker environment': 'state/hosts/worker-9/spec.json',
    'other agy profile': 'home/.local/share/agy/accounts/two/.gemini/oauth_creds.json',
    'other agy worker session': 'home/.local/share/agy/jobs/two/w9/conversations/s.db',
}


class AntigravitySecretTests(unittest.TestCase):
    """Sentinel files in a scratch home: the boundary hides each protected place and keeps the worker's own."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.home = self.root / 'home'
        own = self.home / '.local/share/agy/accounts/one'
        for path in (own / '.gemini', self.root / 'job', self.root / 'tmp', self.root / 'results', self.root / 'passwd'):
            path.mkdir(parents=True)
        (own / '.gemini/oauth_creds.json').write_text('OWN-PROFILE')
        environment = patch.dict(os.environ, {'HOME': str(self.home)})
        environment.start()
        self.addCleanup(environment.stop)
        os.environ.pop('CODEX_HOME', None)
        os.environ.pop('TORII_STATE_DIR', None)
        os.environ.pop('TORII_VAULT_KEY_FILE', None)
        passwd = patch('coordinator.antigravity_runtime.pwd.getpwuid', return_value=SimpleNamespace(pw_dir=str(self.root / 'passwd')))
        passwd.start()
        self.addCleanup(passwd.stop)
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        for name, place in SENTINELS.items():
            path = self.root / place
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('SENTINEL ' + name)
        self.shm = Path(tempfile.mkdtemp(prefix='torii-test-', dir='/dev/shm' if Path('/dev/shm').is_dir() else None)).resolve()
        self.addCleanup(antigravity_runtime.remove_tree, self.shm)
        for name in ('upper', 'work'):
            (self.shm / name).mkdir()
        self.own = own

    def test_secret_places_come_from_the_guards(self):
        places = set(map(str, asyncio.run(antigravity_runtime.secret_places(self.store))))
        for path in (self.home / '.codex', self.home / '.claude-accounts', self.home / '.opencode-accounts',
                     self.root / 'state/envelope', self.root / 'state/hosts', self.home / '.config/torii-secrets',
                     self.root / 'passwd/.config/torii-secrets', self.root / 'passwd/.config/example-router',
                     self.home / '.config/gh/hosts.yml', self.home / '.netrc',
                     self.home / '.config/telegram-agent-coordinator/bot-token',
                     self.home / '.local/share/agy/accounts', self.home / '.local/share/agy/jobs'):
            self.assertIn(str(path), places)
        self.assertFalse(any(Path(place).name.startswith('state.sqlite') for place in places))

    @unittest.skipUnless(bwrap_works(), 'needs Linux with a working bubblewrap')
    def test_each_sentinel_is_unreadable_and_the_worker_keeps_its_own_folders(self):
        script = (
            'for path in "$@"; do if grep -qs SENTINEL "$path"; then echo "read $path"; else echo "hidden $path"; fi; done; '
            'cat "$HOME/.gemini/oauth_creds.json"; echo; '
            'for root in "$TORII_JOB" "$TMPDIR" "$TORII_RESULTS"; do touch "$root/probe" && echo "write $root"; done; '
            '/usr/bin/python3 -c "import sqlite3,sys; sqlite3.connect(sys.argv[1]).execute(\'CREATE TABLE IF NOT EXISTS probe(x)\'); print(\'database\')" "$TORII_STATE/state.sqlite"; '
            'git --version >/dev/null && echo tools')
        paths = [str(self.root / place) for place in SENTINELS.values()]
        command = antigravity_runtime.write_boundary(
            ['/bin/sh', '-c', script, 'boundary', *paths], [self.root / 'job', self.root / 'tmp', self.root / 'results'],
            state=self.store.directory, hidden=asyncio.run(antigravity_runtime.secret_places(self.store)),
            home=(self.own, self.shm / 'upper', self.shm / 'work'))
        env = {'HOME': str(self.own), 'PATH': os.defpath, 'TMPDIR': str(self.root / 'tmp'), 'TORII_JOB': str(self.root / 'job'),
               'TORII_RESULTS': str(self.root / 'results'), 'TORII_STATE': str(self.store.directory)}
        done = subprocess.run(command, capture_output=True, text=True, timeout=60, env=env)
        self.assertEqual(done.returncode, 0, done.stderr)
        lines = done.stdout.splitlines()
        self.assertEqual(lines[:len(paths)], ['hidden ' + path for path in paths])
        self.assertEqual(lines[len(paths):], ['OWN-PROFILE', 'write ' + str(self.root / 'job'), 'write ' + str(self.root / 'tmp'),
                                              'write ' + str(self.root / 'results'), 'database', 'tools'])
        for place in SENTINELS.values():
            self.assertTrue((self.root / place).read_text().startswith('SENTINEL'))


def git(cwd, *arguments):
    return subprocess.run(['git', '-c', 'user.email=fixture@example.invalid', '-c', 'user.name=fixture', *arguments],
                          cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


class AntigravityBranchTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.project = self.root / 'project'
        self.project.mkdir()
        git(self.project, 'init', '-q', '-b', 'main')
        git(self.project, 'commit', '-q', '--allow-empty', '-m', 'one')
        git(self.project, 'commit', '-q', '--allow-empty', '-m', 'two')
        git(self.project, 'branch', 'torii/task-2')
        git(self.project, 'pack-refs', '--all')
        git(self.project, 'update-ref', 'refs/heads/torii/task-2', 'HEAD')
        git(self.project, 'update-ref', 'refs/remotes/origin/main', 'HEAD')
        self.first = git(self.project, 'rev-parse', 'HEAD~1')
        self.head = git(self.project, 'rev-parse', 'HEAD')
        self.work = self.root / 'wt'
        git(self.project, 'worktree', 'add', '-q', '-b', 'torii/task-1', str(self.work))
        for name in ('home', 'tmp', 'results', 'scratch', 'private'):
            (self.root / name).mkdir()
        self.common = self.project / '.git'
        self.store = SimpleNamespace(task_get=lambda task: {'id': 1, 'topic': 1}, topic=lambda topic: {'cwd': str(self.project)})

    async def roots(self, name='refs-1'):
        with patch('coordinator.close_saves.scratch', lambda *args, **kwargs: self.root / 'scratch'):
            return await antigravity_runtime.write_roots(
                self.store, {'task': 1}, self.work, {'HOME': str(self.root / 'home'), 'TMPDIR': str(self.root / 'tmp')},
                self.root / 'results', self.root / 'private' / name)

    async def test_only_private_reference_folders_are_bound_and_the_home_is_no_root(self):
        roots, binds, sync = await self.roots()
        self.assertEqual([str(root) for root in roots], [
            str(self.work), str(self.root / 'tmp'), str(self.root / 'results'), str(self.root / 'scratch'),
            str(self.common / 'objects'), str(self.common / 'worktrees/wt')])
        private = self.root / 'private/refs-1'
        self.assertEqual([(str(source), str(place)) for source, place in binds], [
            (str(private / 'heads'), str(self.common / 'refs/heads/torii')),
            (str(private / 'head-logs'), str(self.common / 'logs/refs/heads/torii')),
            (str(private / 'remotes'), str(self.common / 'refs/remotes')),
            (str(private / 'remote-logs'), str(self.common / 'logs/refs/remotes'))])
        self.assertEqual(sorted(path.name for path in (private / 'heads').iterdir()), ['task-1'])
        self.assertEqual((private / 'heads/task-1').read_text().strip(), self.head)
        self.assertEqual((private / 'remotes/origin/main').read_text().strip(), self.head)
        self.assertEqual(sync, {'common': str(self.common), 'branch': 'torii/task-1',
                                'file': str(private / 'heads/task-1'), 'start': self.head})
        with self.assertRaises(FileExistsError):
            await self.roots()

    @unittest.skipUnless(bwrap_works(), 'needs Linux with a working bubblewrap')
    async def test_overlay_folders_are_readable_or_gone_after_a_run(self):
        shm = Path(tempfile.mkdtemp(prefix='torii-test-', dir='/dev/shm')).resolve()
        self.addCleanup(antigravity_runtime.remove_tree, shm)
        for name in ('upper', 'work'):
            (shm / name).mkdir()
        command = antigravity_runtime.write_boundary(['/bin/true'], [self.work], home=(self.root / 'home', shm / 'upper', shm / 'work'))
        self.assertEqual(subprocess.run(command, timeout=120).returncode, 0)
        self.assertEqual((shm / 'work/work').stat().st_mode & 0o777, 0)
        antigravity_runtime.remove_tree(shm)
        self.assertFalse(shm.exists())

    @unittest.skipUnless(bwrap_works(), 'needs Linux with a working bubblewrap')
    async def test_a_worker_moves_its_own_branch_and_no_other(self):
        roots, binds, sync = await self.roots()
        other = self.common / 'refs/heads/torii/task-2'
        script = ('echo %s > %s; echo %s > %s; git commit -q --allow-empty -m three && git rev-parse HEAD'
                  % (self.first, other, self.first, self.common / 'refs/remotes/origin/main'))
        command = antigravity_runtime.write_boundary(['/bin/sh', '-c', script], roots, binds=binds)
        env = dict(os.environ, GIT_AUTHOR_NAME='f', GIT_AUTHOR_EMAIL='f@example.invalid', GIT_COMMITTER_NAME='f',
                   GIT_COMMITTER_EMAIL='f@example.invalid')
        done = subprocess.run(command, cwd=self.work, capture_output=True, text=True, timeout=30, env=env)
        self.assertEqual(done.returncode, 0, done.stderr)
        made = done.stdout.strip()
        self.assertEqual(git(self.project, 'rev-parse', 'torii/task-2'), self.head)
        self.assertEqual(git(self.project, 'rev-parse', 'refs/remotes/origin/main'), self.head)
        self.assertEqual(git(self.project, 'rev-parse', 'torii/task-1'), self.head)
        self.assertEqual(antigravity_runtime.sync_branch(sync), made)
        self.assertEqual(git(self.project, 'rev-parse', 'torii/task-1'), made)
        self.assertEqual(git(self.work, 'status', '--short'), '')
        self.assertFalse(Path(sync['file']).parent.parent.exists())
        self.assertIsNone(antigravity_runtime.sync_branch(sync))

    async def test_sync_accepts_only_an_existing_commit_from_a_real_file(self):
        roots, binds, sync = await self.roots()
        own = Path(sync['file'])
        tree = git(self.project, 'rev-parse', 'HEAD^{tree}')
        for content in ('garbage', '0' * 40, tree, self.first + ' extra', ''):
            with self.subTest(content=content):
                own.write_text(content + '\n')
                self.assertIsNone(antigravity_runtime.sync_branch(sync))
                self.assertEqual(git(self.project, 'rev-parse', 'torii/task-1'), self.head)
        own.unlink()
        (self.root / 'elsewhere').write_text(self.first + '\n')
        own.symlink_to(self.root / 'elsewhere')
        self.assertIsNone(antigravity_runtime.sync_branch(sync))
        own.unlink()
        own.write_text(self.first + '\n')
        self.assertEqual(antigravity_runtime.sync_branch(sync), self.first)
        self.assertEqual(git(self.project, 'rev-parse', 'torii/task-1'), self.first)
        self.assertIsNone(antigravity_runtime.sync_branch(None))


    async def test_sync_uses_launch_tip_as_compare_and_swap(self):
        _, _, sync = await self.roots()
        ref = 'refs/heads/torii/task-1'
        commit = git(self.project, 'commit-tree', 'HEAD^{tree}', '-p', self.head, '-m', 'worker')
        own = Path(sync['file'])
        own.write_text(commit + '\n')
        update_tip = antigravity_runtime._update_tip

        def move_branch_then_update(*args):
            git(self.project, 'update-ref', ref, self.first)
            return update_tip(*args)

        with patch.object(antigravity_runtime, '_update_tip', side_effect=move_branch_then_update):
            self.assertIsNone(antigravity_runtime.sync_branch(sync))
        self.assertEqual(git(self.project, 'rev-parse', ref), self.first)
        self.assertEqual(own.read_text().strip(), commit)


class AntigravityRecoveryTests(AntigravityCase):
    def setUp(self):
        super().setUp()
        self.register()
        self.task = self.job()
        self.project = self.root / 'project'
        self.project.mkdir()
        git(self.project, 'init', '-q', '-b', 'main')
        git(self.project, 'commit', '-q', '--allow-empty', '-m', 'base')
        self.tip = git(self.project, 'rev-parse', 'HEAD')
        self.branch = 'torii/task-%d' % self.task['id']
        self.cwd.rmdir()
        git(self.project, 'worktree', 'add', '-q', '-b', self.branch, str(self.cwd))
        self.store.db.execute("UPDATE topics SET cwd=? WHERE id='1:2'", (str(self.project),))
        self.worker = self.spawn(self.task).data['worker']
        self.host = 'worker-%d' % self.worker
        self.journal = self.store.directory / 'hosts' / self.host
        self.journal.mkdir(parents=True)

    def commit(self, name):
        return git(self.project, 'commit-tree', 'HEAD^{tree}', '-p', self.tip, '-m', name)

    def leftover(self, commit):
        session = antigravity_runtime.session_folder('other', 'original-launch')
        folder = session / ('refs-' + self.host)
        (folder / 'heads').mkdir(parents=True)
        (folder / 'heads' / self.branch.split('/')[-1]).write_text(commit + '\n')
        return folder

    async def test_next_launch_starts_at_recovered_tip_and_removes_dead_overlay(self):
        commit = self.commit('worker')
        private = self.leftover(commit)
        dead_overlay = antigravity_runtime.scratch_home(self.host)
        (dead_overlay / 'work/work').mkdir(mode=0)
        other_overlay = antigravity_runtime.scratch_home('worker-99999')

        result = await self.run_worker(host_id=self.host + '.2')

        self.assertTrue(result.success, result.error)
        spec = json.loads(Path(result.log_path).with_name('spec.json').read_text())
        self.assertEqual(spec['branch_sync']['start'], commit)
        self.assertEqual(git(self.project, 'rev-parse', self.branch), commit)
        self.assertFalse(private.exists() or dead_overlay.exists())
        self.assertTrue((private.parent / 'conversations').is_dir())
        self.assertTrue(other_overlay.is_dir())

    async def test_job_close_preserves_diverged_tip_as_backup_and_records_problem(self):
        commit = self.commit('worker')
        private = self.leftover(commit)
        owner_tip = self.commit('owner')
        git(self.project, 'update-ref', 'refs/heads/' + self.branch, owner_tip)
        self.runner.extension = Native()
        service = Service(self.store, None, self.runner, self.project)

        closed = self.cli('tasks.update', task=self.task['id'], status='done', summary='Closed in test.')
        self.assertTrue(closed.ok, closed.text)
        await service.controls_once()

        backup = self.branch + '-recovered-' + self.host
        self.assertEqual(git(self.project, 'rev-parse', self.branch), owner_tip)
        self.assertEqual(git(self.project, 'rev-parse', backup), commit)
        self.assertFalse(private.exists())
        problem = self.store.db.execute('SELECT detail,task FROM problems WHERE code=?',
                                        ('branch-recovered-as-backup',)).fetchone()
        self.assertEqual((problem['detail'], problem['task']), (backup, self.task['id']))

    async def test_live_child_keeps_its_private_files_until_it_stops(self):
        commit = self.commit('worker')
        private = self.leftover(commit)
        overlay = antigravity_runtime.scratch_home(self.host)
        child = self.journal / 'child.pid'
        child.write_text(str(os.getpid()))

        self.assertTrue(await antigravity_runtime.recover_job(self.store, self.task['id']))
        self.assertEqual(git(self.project, 'rev-parse', self.branch), self.tip)
        self.assertTrue(private.is_dir() and overlay.is_dir())

        child.unlink()
        self.assertTrue(await antigravity_runtime.recover_job(self.store, self.task['id']))
        self.assertEqual(git(self.project, 'rev-parse', self.branch), commit)
        self.assertFalse(private.exists() or overlay.exists())


class AntigravityAdapterTests(unittest.IsolatedAsyncioTestCase):
    def test_usage_maps_the_live_prompt_result(self):
        result = next(row['msg']['result'] for row in capture('tools')
                      if row['dir'] == '<' and 'stopReason' in (row['msg'].get('result') or {}))
        self.assertEqual(antigravity_runtime.usage(result, None), {
            'usd_estimate': None, 'source': 'Antigravity token counts', 'input_tokens': 13303,
            'output_tokens': 115, 'cached_tokens': 13969, 'thought_tokens': 0})
        self.assertEqual(antigravity_runtime.usage({'usage': {'inputTokens': -1, 'outputTokens': 'x'}}, None)['input_tokens'], 0)

    def test_a_turn_end_without_tokens_is_an_upstream_failure(self):
        live = [row['msg'] for row in capture('upstream-404')]
        self.assertTrue(antigravity_runtime.unbilled(live[-1]['result']))
        self.assertTrue(live[0]['params']['update']['content']['text'].lstrip().startswith('\u26a0 NOT_FOUND'))
        billed = next(row['msg']['result'] for row in capture('tools')
                      if row['dir'] == '<' and 'stopReason' in (row['msg'].get('result') or {}))
        self.assertFalse(antigravity_runtime.unbilled(billed))

    def test_errors_are_classified(self):
        no_browser = {'code': -32603, 'message': 'No browser is available here (no graphical display is available '
                      'to open a browser). Run `agy acp --login` in a terminal on this machine, then retry.'}
        cases = ((no_browser, 'authenticate', 'login_required'),
                 ({'code': -32000, 'message': 'Authentication required'}, 'session/new', 'login_required'),
                 ({'code': -32602, 'message': 'Session not found: x'}, 'session/resume', 'session_missing'),
                 ({'code': -32603, 'message': 'Error 429, Message: slow down, Status: RESOURCE_EXHAUSTED'},
                  'session/prompt', 'quota_limited'),
                 ({'code': -32603, 'message': 'boom'}, 'session/prompt', 'provider_error'))
        for error, method, expected in cases:
            with self.subTest(expected=expected, method=method):
                self.assertEqual(antigravity_runtime.classify_error(method, error), expected)

    def test_subagent_usage_reads_only_the_counters_of_each_subagent_conversation(self):
        def varint(value):
            out = b''
            while value >= 0x80:
                out, value = out + bytes([value & 0x7f | 0x80]), value >> 7
            return out + bytes([value])

        def field(number, value):
            if isinstance(value, int):
                return varint(number << 3) + varint(value)
            return varint(number << 3 | 2) + varint(len(value)) + value

        def generation(input, output, cached, thought, model):
            counters = field(2, input) + field(3, output) + field(5, cached) + field(9, thought) + field(6, 7)
            return field(1, field(4, counters) + field(19, model.encode()) + field(17, field(2, counters))) + field(3, b'x')

        with tempfile.TemporaryDirectory() as folder:
            home, session, conversation = Path(folder), str(uuid.uuid4()), str(uuid.uuid4())
            listing = home / 'brain' / session / '.system_generated' / 'subagents'
            listing.mkdir(parents=True)
            (listing / 'a.json').write_text(json.dumps({'conversationId': conversation}))
            (home / 'conversations').mkdir()
            for name, rows in ((conversation, [generation(100, 20, 300, 5, 'gemini-4-neon'),
                                               generation(1, 2, 3, 4, 'gemini-4-argon')]),
                               (session, [generation(9, 9, 9, 9, 'main')])):
                database = sqlite3.connect(home / 'conversations' / (name + '.db'))
                database.execute('CREATE TABLE gen_metadata (idx INTEGER, data BLOB)')
                database.executemany('INSERT INTO gen_metadata VALUES (?,?)', enumerate(rows))
                database.commit()
                database.close()
            self.assertEqual(antigravity_runtime.subagent_usage(home, session), {conversation: {
                'input_tokens': 101, 'output_tokens': 22, 'cached_tokens': 303, 'thought_tokens': 9,
                'model': 'gemini-4-argon'}})
            self.assertEqual(antigravity_runtime.subagent_usage(home, conversation), {})
            (listing / 'a.json').write_text(json.dumps({'conversationId': '../escape'}))
            self.assertEqual(antigravity_runtime.subagent_usage(home, session), {})

    def test_effort_maps_to_an_offered_thinking_level(self):
        levels = ['low', 'medium', 'high']
        self.assertEqual([antigravity_runtime.effort_level(effort, levels) for effort in
                          ('low', 'medium', 'high', 'xhigh', 'max', None)],
                         ['low', 'medium', 'high', 'high', 'high', None])
        self.assertIsNone(antigravity_runtime.effort_level('max', []))

    async def test_session_ready_sets_model_and_level_from_the_live_options(self):
        response = next(row['msg']['result'] for row in capture('tools')
                        if row['dir'] == '<' and 'sessionId' in (row['msg'].get('result') or {}))
        requests = []

        class Protocol:
            session_id = response['sessionId']
            completed = None

            async def request(self, method, params):
                requests.append((method, params['configId'], params['value']))
                return {}

            def complete(self, outcome, failure=None):
                self.completed = (outcome, failure)

        error = {}
        ready = antigravity_runtime.adapter(None, error=error).session_ready
        protocol = Protocol()
        await ready(protocol, response, 'claude-sonnet-4-6', {'effort': 'max'})
        self.assertEqual(requests, [('session/set_config_option', 'model', 'claude-sonnet-4-6')])
        requests.clear()
        await ready(protocol, response, None, {'effort': 'low'})
        self.assertEqual(requests, [('session/set_config_option', 'thought_level', 'low')])
        requests.clear()
        await ready(protocol, response, 'missing-model', {'effort': 'low'})
        self.assertEqual((requests, protocol.completed), ([], ('failed', 'model_unavailable')))
        self.assertIn('Model missing-model is not offered to this account. Offered: ', error['message'])
        requests.clear()
        protocol.completed = None
        await ready(protocol, response, 'claude-sonnet-4-6-low', {'effort': 'high'})
        self.assertEqual((requests, protocol.completed, error['model']), ([
            ('session/set_config_option', 'model', 'claude-sonnet-4-6'),
            ('session/set_config_option', 'thought_level', 'low')], None, 'claude-sonnet-4-6-low'))
        requests.clear()
        await ready(protocol, response, 'claude-sonnet-4-6-ultra', {'effort': 'high'})
        self.assertEqual((requests, protocol.completed), ([], ('failed', 'model_unavailable')))

    async def test_permission_allows_tools_and_cancels_native_questions(self):
        request = next(row['msg']['params'] for row in capture('tools')
                       if row['msg'].get('method') == 'session/request_permission')
        permission = antigravity_runtime.adapter(None).permission
        self.assertEqual(await permission(request), {'outcome': {'outcome': 'selected', 'optionId': 'allow'}})
        question = dict(request, toolCall=dict(request['toolCall'], toolCallId='interaction_1'))
        self.assertEqual(await permission(question), {'outcome': {'outcome': 'cancelled'}})

    def test_mcp_servers_use_the_acp_http_shape(self):
        servers = antigravity_runtime.mcp_servers({'notes': {'url': 'https://example.test/mcp',
                                                             'headers': {'Authorization': 'Bearer secret-value'}}})
        sent = next(row['msg']['params']['mcpServers'] for row in capture('mcp-cancel')
                    if row['msg'].get('method') == 'session/new')
        self.assertEqual(set(servers[0]), set(sent[0]))
        self.assertEqual(servers, [{'type': 'http', 'name': 'notes', 'url': 'https://example.test/mcp',
                                    'headers': [{'name': 'Authorization', 'value': 'Bearer secret-value'}]}])
        self.assertEqual(antigravity_runtime.mcp_secrets(servers),
                         {'notes-url': 'https://example.test/mcp', 'notes-header-0': 'Bearer secret-value'})


class AntigravityWorkerTests(AntigravityCase):
    def setUp(self):
        super().setUp()
        self.register()
        self.task = self.job()
        self.worker = self.spawn(self.task).data['worker']

    async def test_fresh_worker_runs_inside_the_boundary_and_reports_usage(self):
        result = await self.run_worker(model='model-b', effort='low')
        self.assertTrue(result.success, result.error)
        self.assertEqual(result.text, 'turn 1 model-b:low short prompt')
        self.assertEqual(result.spend, {'usd_estimate': None, 'source': 'Antigravity token counts',
                                        'input_tokens': USAGE['inputTokens'], 'output_tokens': USAGE['outputTokens'],
                                        'cached_tokens': USAGE['cachedReadTokens'], 'thought_tokens': USAGE['thoughtTokens'],
                                        'model': 'model-b'})
        self.assertEqual([row['message']['method'] for row in self.calls()],
                         ['initialize', 'session/new', 'session/set_config_option', 'session/set_config_option',
                          'session/prompt'])
        launch = self.calls('initialize')[0]
        self.assertEqual((launch['argv'], launch['cwd'], launch['home'], launch['display']),
                         (['acp', '--dangerously-skip-permissions'], str(self.cwd), str(self.account), None))
        spec = json.loads(Path(result.log_path).with_name('spec.json').read_text())
        self.assertEqual((spec['account'], spec['provider']), ('one', 'antigravity'))
        self.assertEqual((spec.get('group_owned'), spec.get('ownership')),
                         (None, 'coalition') if host_os.macos() else (True, None))
        boundary = self.boundaries[-1]
        jobs = self.home / '.local/share/agy/jobs'
        sessions = Path(spec['session_home'])
        scratch = Path(spec['scratch_home'])
        self.assertEqual(boundary['roots'][:2], [str(self.cwd), launch['tmpdir']])
        self.assertNotIn(str(self.account), boundary['roots'])
        self.assertEqual(boundary['state'], str(self.store.directory))
        for place in (self.account.parent, jobs, self.store.directory / 'hosts', self.home / '.codex',
                      self.home / '.claude-accounts', self.store.directory / 'envelope'):
            self.assertIn(str(place), boundary['hidden'])
        self.assertEqual((sessions.parent, sessions.stat().st_mode & 0o777), (jobs / 'one', 0o700))
        self.assertEqual(scratch.parent, self.root / 'shm' / ('torii-agy-%d' % os.getuid()))
        self.assertEqual(boundary['home'], [str(self.account), str(scratch / 'upper'), str(scratch / 'work')])
        self.assertEqual(boundary['binds'], [
            (str(sessions / name), str(self.account / '.gemini/antigravity-cli' / name))
            for name in ('conversations', 'brain', 'annotations', 'implicit')])
        self.assertFalse(scratch.exists())
        self.assertNotEqual(launch['tmpdir'], str(self.account / 'tmp'))

    async def test_each_fresh_worker_has_its_own_overlay_and_resume_finds_it(self):
        first = await self.run_worker(host_id='worker-a')
        second = await self.run_worker(host_id='worker-b')
        self.assertEqual(len({boundary['home'][1] for boundary in self.boundaries}), 2)
        alias = self.home / '.local/share/agy/jobs/one'
        find = antigravity_runtime.find_session_folder
        self.assertEqual(find('one', first.session_id), alias / 'worker-a')
        self.assertEqual(find('one', second.session_id), alias / 'worker-b')
        again = await self.run_worker(session=first.session_id, account_alias='one', prompt='again', host_id='worker-c')
        self.assertTrue(again.success, again.error)
        self.assertEqual(self.boundaries[-1]['binds'][0][0], str(alias / 'worker-a/conversations'))
        self.assertEqual(len({boundary['home'][1] for boundary in self.boundaries}), 3)
        self.assertFalse((alias / 'worker-c').exists())
        self.assertFalse((self.account / '.gemini').exists() or (self.account / 'sessions.json').exists())
        self.assertEqual(list((self.root / 'shm' / ('torii-agy-%d' % os.getuid())).iterdir()), [])
        saved = alias / 'worker-b/conversations' / (second.session_id + '.db')
        saved.unlink()
        saved.symlink_to(alias / 'worker-a/sessions.json')
        self.assertIsNone(find('one', second.session_id))
        self.assertIsNone(find('one', '../' + first.session_id))
        self.assertIsNone(find('missing', first.session_id))

    def test_scratch_home_is_removed_and_no_other_folder(self):
        scratch = antigravity_runtime.scratch_home('worker-a')
        (scratch / 'work/work').mkdir(mode=0)
        (scratch / 'upper/file').write_text('x')
        antigravity_runtime.remove_scratch_home(scratch)
        self.assertFalse(scratch.exists())
        for path in (self.account, self.root / 'shm', None, ''):
            antigravity_runtime.remove_scratch_home(path)
        self.assertTrue(self.account.is_dir() and (self.root / 'shm').is_dir())

    async def test_resume_reopens_the_exact_session_on_its_account(self):
        first = await self.run_worker()
        second = await self.run_worker(session=first.session_id, account_alias='one', prompt='again')
        self.assertTrue(second.success, second.error)
        self.assertEqual(second.session_id, first.session_id)
        resume = self.calls('session/resume')[0]['message']['params']
        self.assertEqual((resume['sessionId'], resume['cwd']), (first.session_id, str(self.cwd)))
        missing = await self.run_worker(session='11111111-1111-4111-8111-111111111111', account_alias='one')
        self.assertEqual((missing.success, missing.failure_code), (False, 'session_missing'))
        self.assertTrue(missing.error.startswith('Antigravity failed: session_missing: '), missing.error)
        self.assertEqual(len(self.calls('initialize')), 2)

    async def test_steer_goes_out_in_flight_and_stop_cancels(self):
        self.scenario(hold=True)
        control = RunControl()
        cancels = []
        stopped = []
        running = asyncio.create_task(self.run_worker(control=control, on_cancel=cancels.append,
                                                      stopped=lambda: bool(stopped)))
        await until(lambda: len(self.calls('session/prompt')) == 1)
        self.assertEqual(await control.steer('torii-v2-control-7', 'change course'), 'queued')
        await until(lambda: len(self.calls('session/prompt')) == 2)
        steer = self.calls('session/prompt')[1]['message']['params']
        self.assertEqual((steer['prompt'][0]['text'], steer['_meta']), ('change course', {'toriiSteers': ['torii-v2-control-7']}))
        self.assertFalse(running.done())
        stopped.append(True)
        await asyncio.wait_for(cancels[-1](), 10)
        result = await asyncio.wait_for(running, 20)
        self.assertEqual((result.success, result.failure_code), (False, 'owner_stopped'))
        self.assertEqual(len(self.calls('session/cancel')), 1)
        self.assertEqual(result.spend['input_tokens'], 2 * USAGE['inputTokens'])

    async def test_steered_turn_finishes_with_the_new_report(self):
        self.scenario(hold=True)
        control = RunControl()
        running = asyncio.create_task(self.run_worker(control=control))
        await until(lambda: len(self.calls('session/prompt')) == 1)
        await control.steer('torii-v2-control-8', 'change course')
        await until(lambda: len(self.calls('session/prompt')) == 2)
        (self.binary.parent / 'release').touch()
        result = await asyncio.wait_for(running, 20)
        self.assertTrue(result.success, result.error)
        self.assertEqual(result.text, 'turn 2 model-a:medium change course')

    async def test_reattach_after_restart_keeps_the_running_turn(self):
        self.scenario(hold=True)
        running = asyncio.create_task(self.run_worker(host_id='worker-31'))
        await until(lambda: len(self.calls('session/prompt')) == 1)
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)
        before = len(self.calls())
        replacement = ProviderRunner(self.store.directory, opencode_store=self.store)
        attached = asyncio.create_task(replacement.run('antigravity', 'short prompt', self.cwd, None, fresh=True,
                                                       worker_id=self.worker, topic='1:2', host_id='worker-31', attach=True))
        await asyncio.sleep(0.2)
        self.assertFalse(attached.done())
        (self.binary.parent / 'release').touch()
        result = await asyncio.wait_for(attached, 20)
        self.assertTrue(result.success, result.error)
        self.assertEqual(len(self.calls()), before)
        self.assertEqual(result.text, 'turn 1 model-a:medium short prompt')

    async def test_shared_mcp_servers_reach_the_session_and_headers_are_scrubbed(self):
        self.scenario(echo_headers=True)
        entries = {'notes': {'type': 'http', 'url': 'https://example.test/mcp',
                             'headers': {'Authorization': 'Bearer header-secret-value'}}}

        def read(store, scrub=None, *, topic=None, reserved_names=('torii',)):
            from coordinator.shared_mcp import extend_scrubber
            extend_scrubber(entries, scrub)
            return entries

        with patch('coordinator.providers.read_shared_mcp_entries', read):
            result = await self.run_worker()
        self.assertTrue(result.success, result.error)
        self.assertEqual(self.calls('session/new')[0]['message']['params']['mcpServers'],
                         antigravity_runtime.mcp_servers(entries))
        self.assertNotIn('header-secret-value', result.text)
        for path in Path(result.log_path).parent.rglob('*'):
            if path.is_file():
                self.assertNotIn(b'header-secret-value', path.read_bytes(), path.name)

    async def test_a_turn_that_ends_while_a_subagent_works_gets_a_continuation_prompt(self):
        self.scenario(subagent=2)
        result = await self.run_worker()
        self.assertTrue(result.success, result.error)
        prompts = [row['message']['params']['prompt'][0]['text'] for row in self.calls('session/prompt')]
        self.assertEqual(len(prompts), 2)
        self.assertEqual(prompts[1], antigravity_runtime.SUBAGENT_NUDGE)
        spec = json.loads(Path(result.log_path).with_name('spec.json').read_text())
        self.assertTrue((Path(spec['results']) / 'images/subagent_image.png').is_file())
        self.assertIn('turn 2', result.text)

    async def test_continuation_prompts_stop_at_the_limit(self):
        self.scenario(subagent=99)
        result = await self.run_worker()
        self.assertTrue(result.success, result.error)
        self.assertEqual(len(self.calls('session/prompt')), 4)

    def test_pending_subagents_needs_a_read_message_from_each_subagent(self):
        session = '22222222-2222-4222-8222-222222222222'
        home = self.root / 'pending'
        generated = home / 'brain' / session / '.system_generated'
        nudge = antigravity_runtime.subagent_nudge(home)
        self.assertEqual(antigravity_runtime.pending_subagents(home, session), set())
        (generated / 'subagents').mkdir(parents=True)
        (generated / 'subagents/child-a.json').write_text(json.dumps({'conversationId': 'child-a'}))
        (generated / 'subagents/child-b.json').write_text('not json')
        for name in ('child-a', 'child-b'):
            os.utime(generated / 'subagents' / (name + '.json'), (time.time() + 60,) * 2)
        self.assertEqual(antigravity_runtime.pending_subagents(home, session), {'child-a', 'child-b'})
        (generated / 'messages').mkdir()
        (generated / 'messages/one.json').write_text(json.dumps({'sender': 'child-a'}))
        (generated / 'messages/two.json').write_text(json.dumps({'sender': 'child-b'}))
        (generated / 'messages/read.json').write_text(json.dumps({'one': True, 'two': False}))
        self.assertEqual(antigravity_runtime.pending_subagents(home, session), {'child-b'})
        self.assertEqual(nudge(session), antigravity_runtime.SUBAGENT_NUDGE)
        (generated / 'messages/read.json').write_text(json.dumps({'one': True, 'two': True}))
        self.assertIsNone(nudge(session))
        self.assertEqual(antigravity_runtime.pending_subagents(home, '../' + session), set())
        (generated / 'messages/read.json').unlink()
        (generated / 'messages/read.json').symlink_to(generated / 'messages/one.json')
        self.assertEqual(antigravity_runtime.pending_subagents(home, session), {'child-a', 'child-b'})
        os.utime(generated / 'subagents/child-a.json', (1, 1))
        self.assertEqual(antigravity_runtime.pending_subagents(home, session, since=2), {'child-b'})
        os.utime(generated / 'subagents/child-b.json', (1, 1))
        self.assertIsNone(nudge(session))

    async def test_generated_images_are_copied_to_the_job_results_and_nothing_else(self):
        self.scenario(image=True)
        first = await self.run_worker()
        self.assertTrue(first.success, first.error)
        spec = json.loads(Path(first.log_path).with_name('spec.json').read_text())
        images = Path(spec['results']) / 'images'
        self.assertEqual(sorted(path.name for path in images.iterdir()), ['picture_1.jpg'])
        self.assertEqual((images / 'picture_1.jpg').read_bytes(), b'fixture-jpeg')
        self.assertTrue(first.text.endswith('Generated images:\n' + str(images / 'picture_1.jpg')), first.text)
        second = await self.run_worker(session=first.session_id, account_alias='one', prompt='again')
        self.assertEqual(sorted(path.name for path in images.iterdir()), ['picture_1.jpg'])
        self.assertNotIn('Generated images', second.text)
        session_home = Path(spec['session_home'])
        other = session_home / 'brain/11111111-1111-4111-8111-111111111111'
        other.mkdir(parents=True)
        (other / 'foreign.jpg').write_bytes(b'other session')
        self.assertEqual(antigravity_runtime.collect_images(first, session_home, spec['results']), [])
        first.session_id = '../' + first.session_id
        self.assertEqual(antigravity_runtime.collect_images(first, session_home, spec['results']), [])

    def image_case(self, name):
        root = self.root / 'images' / name
        session = '22222222-2222-4222-8222-222222222222'
        brain = root / 'home/brain'
        for path in (brain, root / 'results', root / 'outside'):
            path.mkdir(parents=True)
        return root, brain / session, RunResult(session, text='done')

    def test_image_copy_follows_no_symbolic_link(self):
        root, brain, result = self.image_case('target-file')
        brain.mkdir()
        (brain / 'evil.png').write_bytes(b'worker bytes')
        (root / 'results/images').mkdir()
        (root / 'results/images/evil.png').symlink_to(root / 'outside/evil.png')
        self.assertEqual(antigravity_runtime.collect_images(result, root / 'home', root / 'results'), [])
        self.assertEqual(list((root / 'outside').iterdir()), [])

        root, brain, result = self.image_case('target-folder')
        brain.mkdir()
        (brain / 'evil.png').write_bytes(b'worker bytes')
        (root / 'results/images').symlink_to(root / 'outside')
        self.assertEqual(antigravity_runtime.collect_images(result, root / 'home', root / 'results'), [])
        self.assertEqual(list((root / 'outside').iterdir()), [])

        root, brain, result = self.image_case('source-folder')
        (root / 'outside/private.png').write_bytes(b'other account')
        brain.symlink_to(root / 'outside')
        self.assertEqual(antigravity_runtime.collect_images(result, root / 'home', root / 'results'), [])
        self.assertFalse((root / 'results/images').exists())

        root, brain, result = self.image_case('source-parent')
        (root / 'outside' / brain.name).mkdir()
        (root / 'outside' / brain.name / 'private.png').write_bytes(b'other account')
        brain.parent.rmdir()
        brain.parent.symlink_to(root / 'outside')
        self.assertEqual(antigravity_runtime.collect_images(result, root / 'home', root / 'results'), [])

        root, brain, result = self.image_case('results-folder')
        brain.mkdir()
        (brain / 'evil.png').write_bytes(b'worker bytes')
        (root / 'results').rmdir()
        (root / 'results').symlink_to(root / 'outside')
        self.assertEqual(antigravity_runtime.collect_images(result, root / 'home', root / 'results'), [])
        self.assertEqual(list((root / 'outside').iterdir()), [])
        self.assertEqual(result.text, 'done')

    async def test_failures_map_to_torii_codes(self):
        cases = (({'quota': True}, {}, 'quota_limited', True),
                 ({'prompt_error': {'code': -32603, 'message': 'backend exploded'}}, {}, 'provider_error', False),
                 ({'upstream_error': '\n\u26a0 NOT_FOUND (code 404): Requested entity was not found.\n'}, {},
                  'provider_error', False),
                 ({}, {'model': 'missing-model'}, 'model_unavailable', False),
                 ({'protocol': 2}, {}, 'version_mismatch', False))
        for scenario, options, code, quota in cases:
            with self.subTest(code=code):
                self.scenario(**scenario)
                result = await self.run_worker(**options)
                self.assertEqual((result.success, result.failure_code, result.quota_limited), (False, code, quota))
                self.assertTrue(result.error.startswith('Antigravity failed: ' + code), result.error)

    async def test_signed_out_profile_fails_and_marks_the_account(self):
        (self.account / 'signed-in').unlink()
        result = await self.run_worker()
        self.assertEqual((result.success, result.failure_code), (False, 'login_required'))
        broker = antigravity_accounts.AntigravityBroker(self.store)
        self.assertFalse(broker.signed_in('one'))
        self.assertIsNone(broker.select())

    async def test_goals_are_refused(self):
        result = await self.run_worker(goal='ship it')
        self.assertEqual((result.success, result.failure_code), (False, 'start_failed'))
        self.assertEqual(self.calls(), [])

    async def test_secrets_reach_a_sandboxed_worker_as_environment(self):
        self.scenario(env=['DUMMY_TOKEN'])
        result = await self.run_worker(secrets={'DUMMY_TOKEN': 'fake-secret-4b1d'})
        self.assertTrue(result.success, result.error)
        self.assertEqual(len(self.boundaries), 1)
        self.assertEqual(self.calls('initialize')[0]['env'], {'DUMMY_TOKEN': 'fake-secret-4b1d'})
        self.assertNotIn('fake-secret-4b1d', Path(result.log_path).with_name('spec.json').read_text())

    async def test_pool_runs_a_spawned_worker_and_pins_the_account(self):
        pool = WorkerPool(self.store, self.runner, AccountBroker(self.store))
        spawned = self.spawn(self.task, prompt='pool prompt', model='model-b', effort='high')
        self.assertTrue(spawned.ok, spawned.text)
        worker = await asyncio.wait_for(pool.run(pool.get(spawned.data['worker']), self.store.topic('1:2')), 30)
        result = json.loads(worker['result'])
        self.assertEqual((worker['status'], result['success'], worker['account_alias'], worker['fresh']),
                         ('done', True, 'one', 0))
        self.assertEqual(result['spend']['input_tokens'], USAGE['inputTokens'])
        self.assertIn('turn 1 model-b:high', result['text'])
        sign_in(self.account.parent / 'two')
        self.cli('antigravity.account', alias='two')
        self.cli('antigravity.use', alias='two')
        resumed = await pool.broker('antigravity').run(
            self.runner.run, 'antigravity', 'more', self.cwd, worker['session'], fresh=False, account_alias='one',
            worker_id=worker['id'], topic='1:2')
        self.assertTrue(resumed.success, resumed.error)
        self.assertEqual(self.calls('initialize')[-1]['home'], str(self.account))
        fresh = self.spawn(self.task, prompt='second job')
        second = await asyncio.wait_for(pool.run(pool.get(fresh.data['worker']), self.store.topic('1:2')), 30)
        self.assertEqual((second['status'], second['account_alias']), ('done', 'two'))
        self.assertEqual(self.calls('initialize')[-1]['home'], str(self.account.parent / 'two'))
        if usage_events is None:
            return
        rows = [tuple(row) for row in self.store.db.execute(
            'SELECT worker,host,provider,account,model,input,output,cached,thought FROM usage_events ORDER BY id')]
        self.assertEqual(rows, [
            (worker['id'], worker['host'], 'antigravity', 'one', 'model-b', USAGE['inputTokens'],
             USAGE['outputTokens'], USAGE['cachedReadTokens'], USAGE['thoughtTokens']),
            (second['id'], second['host'], 'antigravity', 'two', 'model-a', USAGE['inputTokens'],
             USAGE['outputTokens'], USAGE['cachedReadTokens'], USAGE['thoughtTokens'])])
        usage_events.record(self.store, second, RunResult(None, spend=json.loads(second['result'])['spend']), None)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM usage_events').fetchone()[0], 2)
        usage_events.record(self.store, dict(second, host=second['host'] + '.2'),
                     RunResult(None, spend={'source': 'x', 'input_tokens': 0}), None)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM usage_events').fetchone()[0], 2)
        for host, tokens in (('.3', 10), ('.4', 40), ('.5', 30)):
            usage_events.record(self.store, dict(second, host=second['host'] + host), RunResult(None, spend={
                'subagents': {'conversation-1': {'input_tokens': tokens, 'output_tokens': 1, 'model': 'gemini-4-neon'}}}),
                None)
        self.assertEqual([tuple(row) for row in self.store.db.execute(
            'SELECT host,model,input,output,conversation FROM usage_events WHERE conversation IS NOT NULL')],
            [(second['host'] + '.3', 'gemini-4-neon', 40, 1, 'conversation-1')])

    def test_spawn_refuses_a_model_no_account_offers(self):
        refused = self.spawn(self.task, model='gemini-missing')
        self.assertFalse(refused.ok)
        self.assertIn('No Antigravity account offers model gemini-missing. Offered: model-a, model-b.', refused.text)
        with patch('coordinator.antigravity_accounts.DEFAULT_MODEL', 'gemini-missing'):
            self.assertFalse(self.spawn(self.task).ok)
        self.assertTrue(self.spawn(self.task, model='model-b').ok)

    def test_the_account_pick_follows_the_model(self):
        sign_in(self.account.parent / 'two')
        self.cli('antigravity.account', alias='two')
        statuses = self.store.get('antigravity_account_status')
        statuses['two']['models'] = [{'id': 'model-c', 'name': 'Model C'}]
        self.store.put('antigravity_account_status', statuses)
        broker = antigravity_accounts.AntigravityBroker(self.store)
        self.assertEqual((broker.select(model='model-a'), broker.select(model='model-c'), broker.select(model='model-z')),
                         ('one', 'two', None))
        self.assertEqual(broker.run_ready(model='model-c'), 'two')
        with self.assertRaisesRegex(Refused, 'offers model-z'):
            broker.run_ready(model='model-z')
        self.assertTrue(self.spawn(self.task, model='model-c').ok)

    async def test_a_new_worker_is_not_ready_when_only_an_account_without_the_default_model_is_free(self):
        sign_in(self.account.parent / 'two')
        self.cli('antigravity.account', alias='two')
        statuses = self.store.get('antigravity_account_status')
        statuses['two']['models'] = [{'id': 'model-c', 'name': 'Model C'}]
        self.store.put('antigravity_account_status', statuses)
        self.store.put('antigravity_account_blocks', {'one': time.time() + 3600})
        broker = antigravity_accounts.AntigravityBroker(self.store)
        self.assertIsNone(self.store.db.execute('SELECT model FROM workers WHERE id=?', (self.worker,)).fetchone()[0])
        self.assertFalse(await broker.ready(dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (self.worker,)).fetchone())))
        with self.assertRaises(Refused):
            broker.run_ready(None, antigravity_accounts.DEFAULT_MODEL)
        self.store.put('antigravity_account_blocks', {})
        self.assertTrue(await broker.ready(dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (self.worker,)).fetchone())))

    async def test_an_old_worker_with_no_saved_model_resumes_on_an_account_without_the_default_model(self):
        pool = WorkerPool(self.store, self.runner, AccountBroker(self.store))
        worker = await asyncio.wait_for(pool.run(pool.get(self.worker), self.store.topic('1:2')), 30)
        self.assertEqual((worker['status'], worker['fresh']), ('done', 0))
        with self.store.db:
            self.store.db.execute("UPDATE workers SET model=NULL,status='queued' WHERE id=?", (self.worker,))
        with patch('coordinator.antigravity_accounts.DEFAULT_MODEL', 'model-z'):
            self.assertTrue(await antigravity_accounts.AntigravityBroker(self.store).ready(dict(self.store.db.execute('SELECT * FROM workers WHERE id=?', (self.worker,)).fetchone())))
            worker = await asyncio.wait_for(pool.run(pool.get(self.worker), self.store.topic('1:2')), 30)
        self.assertEqual((worker['status'], worker['model']), ('done', 'model-a'), worker['result'])
        self.assertEqual(worker['session'], self.calls('session/resume')[-1]['message']['params']['sessionId'])

    async def test_the_default_model_runs_and_the_model_that_ran_is_saved(self):
        pool = WorkerPool(self.store, self.runner, AccountBroker(self.store))
        worker = await asyncio.wait_for(pool.run(pool.get(self.worker), self.store.topic('1:2')), 30)
        self.assertEqual((worker['status'], worker['model']), ('done', 'model-a'))
        self.assertIn('turn 1 model-a', json.loads(worker['result'])['text'])
        chosen = self.spawn(self.task, model='model-b')
        worker = await asyncio.wait_for(pool.run(pool.get(chosen.data['worker']), self.store.topic('1:2')), 30)
        self.assertEqual((worker['status'], worker['model'], json.loads(worker['result'])['spend']['model']),
                         ('done', 'model-b', 'model-b'))


class AntigravityDefaultTests(AntigravityCase):
    """Without the antigravity.sandbox setting, workers run like Claude and Codex workers."""

    def setUp(self):
        super().setUp()
        self.register(sandbox=False)
        self.task = self.job()
        self.worker = self.spawn(self.task).data['worker']

    async def test_default_worker_runs_without_the_boundary_in_the_account_home(self):
        with patch.dict(os.environ, {'PATH': '/torii/bin' + os.pathsep + os.environ['PATH'], 'GOOGLE_API_KEY': 'ambient',
                                     'TORII_PARITY_FIXTURE': 'kept'}):
            first = await self.run_worker()
        self.assertTrue(first.success, first.error)
        self.assertEqual(self.boundaries, [])
        launch = self.calls('initialize')[0]
        self.assertEqual((launch['argv'], launch['cwd'], launch['home']),
                         (['acp', '--dangerously-skip-permissions'], str(self.cwd), str(self.account)))
        self.assertEqual(launch['path'].split(os.pathsep)[:2], [str(self.account / '.torii-bin'), '/torii/bin'])
        spec = json.loads(Path(first.log_path).with_name('spec.json').read_text())
        self.assertEqual(spec['argv'], [str(self.binary), 'acp', '--dangerously-skip-permissions'])
        self.assertEqual(spec['session_home'], str(self.account / '.gemini/antigravity-cli'))
        self.assertEqual(spec['env']['TORII_PARITY_FIXTURE'], 'kept')
        self.assertNotIn('GOOGLE_API_KEY', spec['env'])
        self.assertNotIn('GEMINI_API_KEY', spec['env'])
        for name in ('branch_sync', 'scratch_home'):
            self.assertIsNone(spec.get(name))
        self.assertFalse((self.home / '.local/share/agy/jobs/one').exists())
        find = antigravity_runtime.find_session_folder
        self.assertEqual(find('one', first.session_id, False), self.account / '.gemini/antigravity-cli')
        self.assertIsNone(find('one', first.session_id))
        self.assertIsNone(find('missing', first.session_id, False))
        again = await self.run_worker(session=first.session_id, account_alias='one', prompt='again')
        self.assertTrue(again.success, again.error)
        self.assertEqual(self.calls('session/resume')[0]['message']['params']['sessionId'], first.session_id)
        self.assertEqual(self.boundaries, [])

    async def test_macos_worker_is_owned_like_claude_and_codex_without_the_boundary(self):
        captured = AsyncMock(side_effect=RuntimeError('captured'))
        with patch('coordinator.host_os.uses_worker_coalitions', new=lambda: True), patch.object(HostClient, 'launch', captured), \
                patch('coordinator.antigravity_runtime.host_os.linux', return_value=False), \
                patch('coordinator.antigravity_runtime.check_sandbox', CHECK_SANDBOX):
            await self.run_worker()
            spec = captured.await_args.args[1]
            self.assertEqual(spec['ownership'], 'coalition')
            self.assertNotIn('group_owned', spec)
            self.assertEqual(spec['argv'], [str(self.binary), 'acp', '--dangerously-skip-permissions'])
            self.assertEqual(spec['env']['HOME'], str(self.account))
            self.assertEqual(self.boundaries, [])
            self.store.put('antigravity_sandbox', True)
            refused = await self.run_worker()
        self.assertFalse(refused.success)
        self.assertIn('On macOS', refused.failure_detail)
        self.assertEqual(captured.await_count, 1)
        self.assertEqual(list((self.root / 'shm').iterdir()), [])
        self.assertFalse((self.home / '.local/share/agy/jobs/one').exists())

    async def test_the_sandbox_setting_puts_new_workers_inside_the_boundary(self):
        self.assertTrue(self.cli('antigravity.sandbox', enabled=True).ok)
        checked = len(self.boundaries)
        result = await self.run_worker()
        self.assertTrue(result.success, result.error)
        self.assertEqual(len(self.boundaries), checked + 1)
        self.assertEqual(self.boundaries[-1]['roots'][0], str(self.cwd))
        spec = json.loads(Path(result.log_path).with_name('spec.json').read_text())
        self.assertTrue(Path(spec['session_home']).is_relative_to(self.home / '.local/share/agy/jobs/one'))

    async def test_generated_images_are_copied_from_the_account_home(self):
        self.scenario(image=True)
        result = await self.run_worker()
        self.assertTrue(result.success, result.error)
        spec = json.loads(Path(result.log_path).with_name('spec.json').read_text())
        self.assertEqual(sorted(path.name for path in (Path(spec['results']) / 'images').iterdir()), ['picture_1.jpg'])

    def test_spawn_takes_declared_vault_secrets(self):
        self.store.task_update(self.task['id'], secrets=['DUMMY_TOKEN=VAULT_DUMMY'])
        spawned = self.spawn(self.task)
        self.assertTrue(spawned.ok, spawned.text)

    async def test_declared_secrets_reach_the_worker_only_as_environment(self):
        self.store.task_update(self.task['id'], secrets=['DUMMY_TOKEN=VAULT_DUMMY'])
        self.scenario(env=['DUMMY_TOKEN'])
        pool = WorkerPool(self.store, self.runner, AccountBroker(self.store))
        with patch('coordinator.workers.load_for_worker', return_value=({'DUMMY_TOKEN': 'fake-secret-4b1d'}, [])), \
                patch('coordinator.workers.record_use'):
            worker = await asyncio.wait_for(pool.run(pool.get(self.worker), self.store.topic('1:2')), 30)
        self.assertEqual(worker['status'], 'done', worker['result'])
        self.assertEqual(self.calls('initialize')[0]['env'], {'DUMMY_TOKEN': 'fake-secret-4b1d'})
        prompt = self.calls('session/prompt')[0]['message']['params']['prompt'][0]['text']
        self.assertIn('Secrets in the environment: DUMMY_TOKEN.', prompt)
        self.assertNotIn('write boundary', prompt)
        self.assertNotIn('fake-secret-4b1d', prompt)
        for path in (self.store.directory / 'hosts').rglob('*'):
            if path.is_file():
                self.assertNotIn(b'fake-secret-4b1d', path.read_bytes(), path)


if __name__ == '__main__':
    unittest.main()
