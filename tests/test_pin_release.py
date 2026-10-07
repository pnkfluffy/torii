from pathlib import Path
from coordinator import extension
import plistlib
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / 'scripts' / 'pin-release.py'


class PinReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.home = self.root / 'checkout'
        (self.home / 'coordinator').mkdir(parents=True)
        (self.home / 'coordinator' / '__init__.py').write_text('')
        (self.home / 'coordinator' / '__main__.py').write_text('print("usage: coordinator")\n')
        for module in ('service', 'mcp'):
            (self.home / 'coordinator' / (module + '.py')).write_text('READY = True\n')
        (self.home / 'coordinator' / 'extension.py').write_bytes(
            (SCRIPT.parent.parent / 'coordinator' / 'extension.py').read_bytes())
        self.git('init', '-q')
        self.git('add', '.')
        self.commit('first')
        self.first = self.git('rev-parse', 'HEAD')
        (self.home / 'README.md').write_text('second\n')
        self.git('add', '.')
        self.commit('second')
        self.second = self.git('rev-parse', 'HEAD')
        self.state = self.root / 'state'
        self.plist = self.root / 'agent.plist'
        with self.plist.open('wb') as stream:
            plistlib.dump({'Label': 'local.telegram-agent-coordinator', 'WorkingDirectory': str(self.home),
                           'EnvironmentVariables': {'PATH': '/usr/bin:/bin'}}, stream)
        self.plist_bytes = self.plist.read_bytes()

    def tearDown(self):
        self.temp.cleanup()

    def git(self, *args, cwd=None):
        return subprocess.run(('git', '-C', str(cwd or self.home)) + args, check=True,
                              capture_output=True, text=True).stdout.strip()

    def commit(self, message):
        self.git('-c', 'user.email=t@example.com', '-c', 'user.name=T', 'commit', '-qm', message)

    def pin(self, *args):
        return subprocess.run([sys.executable, str(SCRIPT), *args, '--state-dir', str(self.state),
                               '--plist', str(self.plist)], capture_output=True, text=True)

    def test_creates_a_detached_release_and_prints_the_unapplied_launchd_change(self):
        done = self.pin(self.first[:10])
        self.assertEqual(done.returncode, 0, done.stderr)
        target = self.state / 'releases' / self.first[:12]
        self.assertEqual(self.git('rev-parse', 'HEAD', cwd=target), self.first)
        self.assertEqual(subprocess.run(['git', '-C', str(target), 'symbolic-ref', '-q', 'HEAD']).returncode, 1)
        self.assertEqual(self.git('rev-parse', 'HEAD'), self.second)
        self.assertIn('WorkingDirectory: %s -> %s' % (self.home, target), done.stdout)
        self.assertIn('TORII_HOME: (unset) -> %s' % self.home, done.stdout)
        self.assertIn('Add :EnvironmentVariables:TORII_HOME string %s' % self.home, done.stdout)
        self.assertIn('launchctl bootstrap', done.stdout)
        self.assertEqual(self.plist.read_bytes(), self.plist_bytes)

    def test_rerun_reuses_the_release_and_another_commit_gets_its_own_folder(self):
        self.assertEqual(self.pin(self.first).returncode, 0)
        again = self.pin(self.first)
        self.assertEqual(again.returncode, 0, again.stderr)
        self.assertIn('Release exists', again.stdout)
        self.assertEqual(self.pin(self.second).returncode, 0)
        self.assertEqual(sorted(path.name for path in (self.state / 'releases').iterdir()),
                         sorted([self.first[:12], self.second[:12]]))

    def test_home_comes_from_the_plist_after_a_release_is_applied(self):
        with self.plist.open('wb') as stream:
            plistlib.dump({'WorkingDirectory': str(self.root / 'old-release'),
                           'EnvironmentVariables': {'TORII_HOME': str(self.home)}}, stream)
        done = self.pin(self.second)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn('Home checkout: %s' % self.home, done.stdout)
        self.assertIn('Set :EnvironmentVariables:TORII_HOME %s' % self.home, done.stdout)

    def test_a_release_whose_service_module_does_not_compile_is_refused(self):
        (self.home / 'coordinator' / 'service.py').write_text('def broken(:\n')
        self.git('add', '.')
        self.commit('broken')
        done = self.pin('HEAD')
        self.assertNotEqual(done.returncode, 0)
        self.assertIn('does not load', done.stderr)
        self.assertNotIn('launchctl', done.stdout)

    def test_unknown_commit_or_mismatched_folder_is_refused(self):
        missing = self.pin('no-such-ref')
        self.assertNotEqual(missing.returncode, 0)
        self.assertFalse((self.state / 'releases').exists())
        (self.state / 'releases').mkdir(parents=True)
        self.git('worktree', 'add', '-q', '--detach', str(self.state / 'releases' / self.second[:12]), self.first)
        mismatch = self.pin(self.second)
        self.assertNotEqual(mismatch.returncode, 0)
        self.assertIn('another commit', mismatch.stderr)

    def test_a_release_with_a_broken_local_extension_is_refused(self):
        module = extension.LOCAL.rsplit('.', 1)[1]
        (self.home / 'coordinator' / (module + '.py')).write_text(
            'raise RuntimeError("broken local extension")\n')
        self.git('add', '.')
        self.commit('broken extension')
        done = self.pin('HEAD')
        self.assertNotEqual(done.returncode, 0)
        self.assertIn('does not load', done.stderr)
        self.assertIn('broken local extension', done.stderr)
        self.assertNotIn('launchctl', done.stdout)


if __name__ == '__main__':
    unittest.main()
