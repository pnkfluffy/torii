import asyncio
import os
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from coordinator.control_api import call
from coordinator.health import _plain
from coordinator.onboarding import op_projects_list, op_setup_new, projects_guide
from coordinator.store import Store
from coordinator.workspaces import prepare_task_workspace
from tests.test_group_setup import group_update, pair_group, GroupTelegram
from tests.test_store import update


class ProjectGitTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.environment = patch.dict(os.environ, {
            'PATH': os.environ.get('PATH', '/usr/bin:/bin'),
            'HOME': str(self.root), 'GIT_CONFIG_NOSYSTEM': '1',
            'GIT_CONFIG_GLOBAL': str(self.root / 'gitconfig'),
        }, clear=True)
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.home = patch('pathlib.Path.home', return_value=self.root)
        self.home.start()
        self.addCleanup(self.home.stop)
        self.trash = patch('coordinator.onboarding.trash_directory', return_value=self.root / '.Trash')
        self.trash.start()
        self.addCleanup(self.trash.stop)
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        pair_group(self.store)
        self.project = self.root / 'Projects' / 'torii' / 'website'

    def git(self, *args, cwd=None, check=True):
        return subprocess.run(('git', '-C', str(cwd or self.root), *args),
                              check=check, text=True, capture_output=True)

    def assert_ready(self, path=None, task=1):
        path = path or self.project
        self.assertTrue((path / '.git').is_dir())
        self.assertEqual(self.git('rev-list', '--count', 'HEAD', cwd=path).stdout.strip(), '1')
        self.assertEqual(self.git('ls-tree', '-r', '--name-only', 'HEAD', cwd=path).stdout, '')
        head = self.git('rev-parse', 'HEAD', cwd=path).stdout.strip()
        workspace = asyncio.run(prepare_task_workspace(path, self.root / 'jobs', task))
        self.assertTrue(workspace['isolated'])
        self.assertEqual(self.git('rev-parse', 'HEAD', cwd=workspace['cwd']).stdout.strip(), head)

    def tap_name(self, kind):
        state = self.store.get('control_ui:-10042:10')
        index = next(index for index, action in enumerate(state['actions'])
                     if (action.get('input') == 'first_project_name' if kind == 'type' else
                         action.get('command', '').startswith('/setup new ')))
        query = {'id': 'name', 'from': {'id': 7}, 'message': {'message_id': 90,
                 'chat': {'id': -10042, 'type': 'supergroup'}, 'message_thread_id': 10},
                 'data': 'torii:' + state['token'] + ':' + str(index)}
        self.assertEqual(self.store.accept({'update_id': 2, 'callback_query': query}), 'control_callback')

    def test_first_suggested_name_is_ready_without_git_identity(self):
        self.assertEqual(self.git('config', '--get', 'user.name', check=False).returncode, 1)
        self.assertEqual(self.git('config', '--get', 'user.email', check=False).returncode, 1)
        self.store.accept(group_update(1, 'build a website'))
        self.tap_name('suggested')
        self.assert_ready()
        self.assertFalse((self.root / 'gitconfig').exists())
        for key in ('user.name', 'user.email'):
            self.assertEqual(self.git('config', '--local', '--get', key,
                                      cwd=self.project, check=False).returncode, 1)

    def test_first_typed_name_is_ready(self):
        self.store.accept(group_update(1, 'build a website'))
        self.tap_name('type')
        self.store.accept(group_update(3, 'Website'))
        self.assert_ready()

    def test_setup_new_command_is_ready(self):
        self.store.accept(group_update(1, '/setup new Website'))
        self.assert_ready()

    def test_interactive_setup_is_ready(self):
        store = Store(self.root / 'group-state')
        self.addCleanup(store.close)
        store.accept(update(1, '/pair ' + store.pairing_code()), creator=True)
        store.accept(update(2, '/setup'))
        store.accept(update(3, 'New project'))
        store.accept(update(4, 'Website'))
        self.assert_ready()

    def test_setup_api_is_ready(self):
        self.assertIn('Creating', op_setup_new(self.store, None, topic='-10042:10', name='Website'))
        self.assert_ready()

    def test_first_default_project_is_listed_in_projects_and_catalog(self):
        self.assertIsNone(self.store.get('projects_root'))
        self.store.accept(group_update(1, '/setup new Website'))
        self.finish_project()
        reply = projects_guide(self.store)
        self.assertIn('**Projects** in ' + _plain(self.project.parent), reply)
        self.assertIn('• website — linked to Website', reply)
        self.assertEqual(op_projects_list(self.store, None).data,
                         [{'name': 'website', 'path': str(self.project), 'bound': 'Website'}])
        self.assertEqual(self.store.get('projects_root'), str(self.project.parent))


    def test_group_topic_creation_is_ready(self):
        store = Store(self.root / 'group-state')
        self.addCleanup(store.close)
        store.accept(update(1, '/pair ' + store.pairing_code()), creator=True)
        store.accept(update(2, '/setup new Website'))
        self.assert_ready()

    def test_configured_identity_and_default_branch_are_respected(self):
        self.git('config', '--global', 'user.name', 'Project Owner')
        self.git('config', '--global', 'user.email', 'owner@example.test')
        self.git('config', '--global', 'init.defaultBranch', 'owner-branch')
        before = (self.root / 'gitconfig').read_bytes()
        self.store.accept(group_update(1, '/setup new Website'))
        self.assert_ready()
        self.assertEqual(self.git('show', '-s', '--format=%an <%ae>|%cn <%ce>',
                                  cwd=self.project).stdout.strip(),
                         'Project Owner <owner@example.test>|Project Owner <owner@example.test>')
        self.assertEqual(self.git('branch', '--show-current', cwd=self.project).stdout.strip(), 'owner-branch')
        self.assertEqual((self.root / 'gitconfig').read_bytes(), before)

    def test_partial_configured_identity_is_preserved(self):
        self.git('config', '--global', 'user.name', 'Project Owner')
        self.store.accept(group_update(1, '/setup new Website'))
        self.assert_ready()
        self.assertEqual(self.git('show', '-s', '--format=%an', cwd=self.project).stdout.strip(), 'Project Owner')

    def test_existing_non_git_folder_is_untouched_and_refusal_explains_consent(self):
        self.project.mkdir(parents=True)
        marker = self.project / 'keep.txt'
        marker.write_text('owner content')
        self.store.accept(group_update(1, '/setup new Website'))
        self.assertFalse((self.project / '.git').exists())
        self.store.accept(group_update(2, '/setup use ' + str(self.project)))
        with self.assertRaises(ValueError) as refused:
            asyncio.run(prepare_task_workspace(self.project, self.root / 'jobs', 1))
        self.assertIn('Git repository with at least one commit', str(refused.exception))
        self.assertIn('Ask the owner before initializing', str(refused.exception))
        self.assertEqual(list(self.project.iterdir()), [marker])
        self.assertEqual(marker.read_text(), 'owner content')

    def test_existing_unborn_repository_is_not_committed(self):
        self.project.mkdir(parents=True)
        self.git('init', '-q', cwd=self.project)
        before = (self.project / '.git' / 'config').read_bytes()
        self.store.accept(group_update(1, '/setup use ' + str(self.project)))
        with self.assertRaisesRegex(ValueError, 'at least one commit'):
            asyncio.run(prepare_task_workspace(self.project, self.root / 'jobs', 1))
        self.assertNotEqual(self.git('rev-parse', '--verify', 'HEAD', cwd=self.project, check=False).returncode, 0)
        self.assertEqual((self.project / '.git' / 'config').read_bytes(), before)

    def hook(self, directory, name, body):
        directory.mkdir(parents=True, exist_ok=True)
        hook = directory / name
        hook.write_text('#!/bin/sh\n' + body + '\n')
        hook.chmod(0o700)

    def finish_project(self):
        from coordinator.setup_flow import tick
        service = SimpleNamespace(store=self.store, telegram=GroupTelegram(), wake=lambda name: asyncio.Event())
        asyncio.run(tick(service))

    def assert_linked(self):
        self.finish_project()
        topics = [topic for topic in self.store.topics() if topic['cwd'] == str(self.project)]
        self.assertEqual(len(topics), 1)
        self.assertTrue(topics[0]['enabled'])
        self.assert_ready()

    def test_initial_commit_ignores_ssh_signing_without_key(self):
        self.git('config', '--global', 'commit.gpgSign', 'true')
        self.git('config', '--global', 'gpg.format', 'ssh')
        self.store.accept(group_update(1, '/setup new Website'))
        self.assert_linked()

    def test_initial_commit_ignores_global_hooks(self):
        hooks = self.root / 'hooks'
        for name in ('pre-commit', 'prepare-commit-msg', 'post-commit'):
            self.hook(hooks, name, 'echo hook-ran > "' + str(self.root / name) + '"\nexit 1')
        self.hook(hooks, 'commit-msg', 'grep -Eq "^(feat|fix): " "$1"')
        self.git('config', '--global', 'core.hooksPath', str(hooks))
        self.store.accept(group_update(1, '/setup new Website'))
        self.assert_linked()
        for name in ('pre-commit', 'prepare-commit-msg', 'post-commit'):
            self.assertFalse((self.root / name).exists())

    def test_initial_commit_ignores_template_hooks(self):
        template = self.root / 'template'
        for name in ('pre-commit', 'prepare-commit-msg', 'commit-msg', 'post-commit'):
            self.hook(template / 'hooks', name, 'echo template-hook-ran > "' +
                      str(self.root / name) + '"\nexit 1')
        self.git('config', '--global', 'init.templateDir', str(template))
        self.store.accept(group_update(1, '/setup new Website'))
        self.assert_linked()
        for name in ('pre-commit', 'prepare-commit-msg', 'commit-msg', 'post-commit'):
            self.assertTrue((self.project / '.git' / 'hooks' / name).is_file())
            self.assertFalse((self.root / name).exists())

    def fail_commit(self, extra_file=False):
        run = subprocess.run

        def fail(args, **kwargs):
            if 'commit' in args:
                if extra_file:
                    (self.project / 'keep.txt').write_text('owner content')
                raise subprocess.CalledProcessError(1, args, stderr='fatal: synthetic commit failure\nDetails\n')
            return run(args, **kwargs)

        return patch('coordinator.onboarding.subprocess.run', side_effect=fail)

    def test_commit_failure_reply_includes_git_error(self):
        with self.fail_commit():
            self.store.accept(group_update(1, '/setup new Website'))
        reply = self.store.db.execute('SELECT text FROM outbox ORDER BY id DESC LIMIT 1').fetchone()[0]
        self.assertIn('Git setup failed', reply)
        self.assertIn('fatal: synthetic commit failure', reply)
        self.assertFalse(self.store.topic('-10042:10')['enabled'])
        self.assertEqual(self.store.topic('-10042:10')['cwd'], '')
        self.assertIsNone(self.store.get('projects_root'))

    def test_commit_failure_logs_git_stderr(self):
        with self.assertLogs('coordinator.onboarding', level='ERROR') as logs:
            with self.fail_commit():
                op_setup_new(self.store, None, topic='-10042:10', name='Website')
        self.assertIn('fatal: synthetic commit failure\nDetails', '\n'.join(logs.output))

    def test_commit_failure_api_reports_failure(self):
        with self.fail_commit():
            direct = op_setup_new(self.store, None, topic='-10042:10', name='Website')
        self.assertIs(getattr(direct, 'ok', None), False)
        with self.fail_commit():
            result = call(self.store, 'topic.setup_new', {'name': 'Blog'}, topic='-10042:10')
        self.assertFalse(result.ok)
        self.assertEqual(result.state, 'failed')
        self.assertIn('fatal: synthetic commit failure', result.text)

    def test_commit_failure_moves_only_new_git_folder_to_trash_and_allows_retry(self):
        trash = self.root / '.Trash'
        trash.mkdir()
        (trash / 'website').mkdir()
        (trash / 'website' / 'keep.txt').write_text('existing trash')
        with self.fail_commit():
            self.store.accept(group_update(1, '/setup new Website'))
        self.assertFalse(self.project.exists())
        self.assertEqual((trash / 'website' / 'keep.txt').read_text(), 'existing trash')
        moved = [path for path in trash.iterdir() if path.name != 'website']
        self.assertEqual(len(moved), 1)
        self.assertTrue((moved[0] / '.git').is_dir())
        self.store.accept(group_update(2, '/setup new Website'))
        reply = self.store.db.execute('SELECT text FROM outbox ORDER BY id DESC LIMIT 1').fetchone()[0]
        self.assertNotIn('That folder already exists', reply)
        self.assert_linked()

    def test_commit_failure_preserves_folder_with_other_content(self):
        with self.fail_commit(extra_file=True):
            self.store.accept(group_update(1, '/setup new Website'))
        reply = self.store.db.execute('SELECT text FROM outbox ORDER BY id DESC LIMIT 1').fetchone()[0]
        self.assertIn('Git setup failed', reply)
        self.assertIn('folder was left', reply)
        self.assertIn(str(self.project), reply)
        self.assertEqual((self.project / 'keep.txt').read_text(), 'owner content')
        self.assertTrue((self.project / '.git').is_dir())
        self.assertFalse(self.store.topic('-10042:10')['enabled'])
        self.assertFalse((self.root / '.Trash').exists())

    def test_missing_git_moves_empty_new_folder_to_trash(self):
        with patch('coordinator.onboarding.subprocess.run', side_effect=FileNotFoundError('git')):
            result = call(self.store, 'topic.setup_new', {'name': 'Website'}, topic='-10042:10')
        self.assertFalse(result.ok)
        self.assertIn('Git setup failed', result.text)
        self.assertFalse(self.project.exists())
        self.assertTrue((self.root / '.Trash' / 'website').is_dir())
        self.assertFalse(self.store.topic('-10042:10')['enabled'])
        self.assertEqual(self.store.topic('-10042:10')['cwd'], '')
        self.assertIsNone(self.store.get('projects_root'))
