import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from coordinator.health import _plain
from coordinator.store import Store
from tests.test_store import update


class OnboardingFixture(unittest.TestCase):
    def setUp(self):
        self.provider_tools = patch('shutil.which', return_value=None)
        self.provider_tools.start()
        self.addCleanup(self.provider_tools.stop)
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.environment = patch.dict(os.environ, {
            'PATH': os.environ.get('PATH', '/usr/bin:/bin'),
            'HOME': str(self.root), 'GIT_CONFIG_NOSYSTEM': '1',
            'GIT_CONFIG_GLOBAL': str(self.root / 'gitconfig'),
        }, clear=True)
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.home_patch = patch('coordinator.onboarding.Path.home', return_value=self.root)
        self.home_patch.start()
        self.addCleanup(self.home_patch.stop)
        self.store = Store(self.root / 'state')
        self.projects = self.root / 'projects'
        self.projects.mkdir()
        with self.store.db:
            self.store.put('projects_root', str(self.projects))
        self.store.accept(update(1, '/pair ' + self.store.pairing_code()), creator=True)

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def send(self, number, text, **kwargs):
        outcome = self.store.accept(update(number, text, **kwargs))
        rows = self.store.db.execute('SELECT text FROM outbox WHERE reply_to=? ORDER BY id', (number,))
        return outcome, '\n'.join(row[0] for row in rows)


class OnboardingTests(OnboardingFixture):
    def test_new_project_entirely_in_topic_survives_restart(self):
        popen = subprocess.Popen

        def setup_process(args, **kwargs):
            self.assertEqual(args[0], 'git', 'provider started during setup')
            return popen(args, **kwargs)

        with patch('subprocess.Popen', side_effect=setup_process):
            _, guide = self.send(2, '/setup')
            self.assertIn('New project', guide)
            self.assertNotIn('python3', guide)
            self.send(3, 'New project')
            self.store.close()
            self.store = Store(self.root / 'state')
            _, ready = self.send(4, 'A New App')
        self.assertIn('ready', ready.lower())
        topic = self.store.topic('-10042:4')
        self.assertEqual(topic['cwd'], str(self.projects / 'a-new-app'))
        self.assertTrue(Path(topic['cwd']).is_dir())
        self.assertTrue(topic['enabled'])
        self.assertEqual(self.store.get('coordinator_home_topic'), '-10042:4')
        self.assertIsNone(topic['session'])
        self.assertEqual(self.store.accept(update(4, 'A New App')), 'duplicate')
        self.assertEqual(self.store.tasks_list(), [])
        self.assertEqual(self.send(5, 'Build a landing page')[0], 'queued')
        self.assertEqual(self.store.messages_pending()[0]['text'], 'Build a landing page')

    def test_existing_project_selection_preserves_source(self):
        project = self.projects / 'existing'
        project.mkdir()
        (project / 'keep.txt').write_text('keep this')
        _, choices = self.send(2, '/setup existing')
        self.assertIn('Choose a project below', choices)
        self.assertEqual(self.store.get('project_setup:-10042:4')['choices'][0]['name'], 'existing')
        self.send(3, 'existing')
        self.assertEqual(self.store.topic('-10042:4')['cwd'], str(project))
        self.assertEqual((project / 'keep.txt').read_text(), 'keep this')

    def test_existing_project_symlink_saves_and_reports_resolved_path(self):
        project = self.projects / 'actual'
        project.mkdir()
        alias = self.root / 'alias'
        alias.symlink_to(project, target_is_directory=True)
        _, reply = self.send(2, '/setup use ' + str(alias))
        self.assertEqual(self.store.topic('-10042:4')['cwd'], str(project.resolve()))
        self.assertIn('Project folder: ' + str(project.resolve()), reply)
        self.assertNotIn('Project folder: ' + str(alias), reply)

    def test_topics_on_the_same_folder_keep_their_telegram_titles(self):
        project = self.projects / 'torii'
        project.mkdir()
        self.send(2, '/setup use ' + str(project))
        self.assertEqual(self.store.topic('-10042:4')['name'], 'torii')
        self.send(3, '', thread=9, forum_topic_created={'name': 'Torii brainstorm'})
        _, ready = self.send(4, '/setup use ' + str(project), thread=9)
        self.assertIn('Torii brainstorm is ready.', ready)
        brainstorm = self.store.topic('-10042:9')
        self.assertEqual((brainstorm['name'], brainstorm['cwd'], brainstorm['enabled']),
                         ('Torii brainstorm', str(project), 1))
        self.send(5, '', thread=11, forum_topic_created={'name': 'TORII'})
        _, ready = self.send(6, '/setup use ' + str(project), thread=11)
        self.assertIn('TORII is ready.', ready)
        self.assertEqual(self.store.topic('-10042:11')['cwd'], str(project))
        self.assertEqual([topic['name'].casefold() for topic in self.store.topics() if topic['cwd']].count('torii'), 2)

    def test_topic_creation_starts_setup_and_no_agent_task(self):
        _, guide = self.send(2, '', thread=9, forum_topic_created={'name': 'New idea'})
        self.assertIn('New project', guide)
        self.assertEqual(self.store.topic('-10042:9')['name'], 'New idea')
        self.assertEqual(self.store.tasks_list(), [])
        _, question = self.send(3, 'New project', thread=9)
        self.assertIn('name', question.lower())
        self.send(4, 'New idea', thread=9)
        self.assertTrue(self.store.topic('-10042:9')['enabled'])

    def test_owner_only_and_topic_isolation(self):
        self.assertEqual(self.send(2, '/setup new bad', user=99)[0], 'unauthorized')
        self.assertFalse((self.projects / 'bad').exists())
        self.send(3, '/setup new')
        self.send(4, '/setup', thread=9)
        self.send(5, 'Existing project', thread=9)
        self.send(6, 'first')
        self.assertTrue(self.store.topic('-10042:4')['enabled'])
        self.assertFalse(self.store.topic('-10042:9')['enabled'])

    def test_validation_and_existing_folder_collision(self):
        project = self.projects / 'taken'
        project.mkdir()
        (project / 'keep').write_text('unchanged')
        for i, name in enumerate(('../escape', '/absolute', 'taken'), 2):
            self.send(i, '/setup new ' + name)
            self.assertFalse(self.store.topic('-10042:4')['enabled'])
        self.assertEqual((project / 'keep').read_text(), 'unchanged')
        self.assertFalse((self.root / 'escape').exists())
        self.send(6, '/setup use taken')
        self.assertTrue(self.store.topic('-10042:4')['enabled'])

    def test_setup_cannot_reset_bound_or_interrupted_session(self):
        self.send(2, '/setup new original')
        with self.store.db:
            self.store.db.execute("UPDATE topics SET session='saved-session',enabled=0 WHERE id='-10042:4'")
        _, reply = self.send(3, '/setup new replacement')
        self.assertIn('saved', reply.lower())
        self.assertFalse((self.projects / 'replacement').exists())
        self.assertEqual(self.store.topic('-10042:4')['session'], 'saved-session')
        self.assertFalse(self.store.topic('-10042:4')['enabled'])

    def test_path_with_spaces_and_cancel(self):
        project = self.root / 'folder with spaces'
        project.mkdir()
        self.send(2, '/setup new')
        self.send(3, '/setup cancel')
        self.send(4, '/setup use ' + str(project))
        self.assertEqual(self.store.topic('-10042:4')['cwd'], str(project))

    def test_the_published_command_menu_is_the_exact_approved_list(self):
        from coordinator.controls import _USAGE
        from coordinator.onboarding import COMMAND_MENU
        self.assertEqual(COMMAND_MENU, [
            {'command': 'accounts', 'description': 'Usage, sign-ins, resets, models'},
            {'command': 'projects', 'description': "Projects, new projects, this topic's folder"},
            {'command': 'tldr', 'description': 'Catch up on this topic'},
            {'command': 'health', 'description': 'Running agents and system load'},
            {'command': 'secrets', 'description': 'Stored keys: rotate, revoke, ask again'},
            {'command': 'ping', 'description': 'Check that Torii is listening'},
            {'command': 'help', 'description': 'All commands'},
        ])
        for entry in COMMAND_MENU:
            if entry['command'] != 'ping':
                self.assertIn('/' + entry['command'], _USAGE)
            self.assertRegex(entry['command'], r'^[a-z0-9_]{1,32}$')
            self.assertTrue(0 < len(entry['description']) <= 256)

    def test_projects_lists_only_immediate_subdirectories_of_the_root(self):
        for name in ('alpha', 'beta', 'gamma'):
            (self.projects / name).mkdir()
        (self.projects / '.hidden').mkdir()
        (self.projects / 'a-file.md').write_text('not a project')
        (self.projects / 'alpha' / 'nested').mkdir()
        outside = self.root / 'elsewhere'
        outside.mkdir()
        _, reply = self.send(2, '/projects')
        listed = [line for line in reply.splitlines()
                  if line.removeprefix('• ').split(' — ')[0] in ('alpha', 'beta', 'gamma', '.hidden', 'a-file.md', 'nested', 'elsewhere')]
        self.assertEqual([line.removeprefix('• ').split(' — ')[0] for line in listed], ['alpha', 'beta', 'gamma'])
        self.assertNotIn(str(outside), reply)

    def test_a_topic_bound_outside_the_root_is_not_listed_but_stays_bound(self):
        outside = self.root / 'ssd-project'
        outside.mkdir()
        self.send(2, '/setup use ' + str(outside), thread=9)
        self.assertEqual(self.store.topic('-10042:9')['cwd'], str(outside))
        _, reply = self.send(3, '/projects')
        self.assertNotIn('ssd-project', reply)
        self.assertNotIn(str(outside), reply)
        self.assertTrue(self.store.topic('-10042:9')['enabled'])

    def test_a_project_bound_inside_the_root_is_marked(self):
        (self.projects / 'shared').mkdir()
        self.send(2, '/setup use ' + str(self.projects / 'shared'), thread=9)
        _, reply = self.send(3, '/projects')
        self.assertIn('• shared — linked to ', reply)
        (self.projects / 'free').mkdir()
        _, reply = self.send(4, '/projects')
        self.assertIn('• free', reply.splitlines())
        self.assertIn('• shared — linked to ', reply)

    def test_projects_says_so_when_no_root_is_set_instead_of_scanning(self):
        with self.store.db:
            self.store.put('projects_root', None)
        (self.root / 'hq').mkdir()
        (self.root / 'hq' / 'repos').mkdir()
        (self.root / 'hq' / 'repos' / 'stray').mkdir()
        _, reply = self.send(2, '/projects')
        self.assertEqual(reply, '**Projects** · new ones go in ' + _plain(self.root / 'Projects' / 'torii') +
                         "\nNo project folders there yet.\n\nThis topic isn't linked to a project yet.")
        self.assertNotIn('stray', reply)

    def test_root_change_only_affects_future_projects_and_unavailable_root_stops(self):
        self.send(2, '/setup new original')
        original = self.store.topic('-10042:4')['cwd']
        other = self.root / 'other projects'
        other.mkdir()
        from coordinator.control_api import call
        with self.store.db:
            result = call(self.store, 'projects.root', {'path': str(other)})
        self.assertTrue(result.ok)
        reply = result.text
        self.assertIn('other projects', reply)
        self.assertIn('Existing projects stay', reply)
        self.send(4, '/setup new next', thread=9)
        self.assertEqual(self.store.topic('-10042:9')['cwd'], str(other / 'next'))
        self.assertEqual(self.store.topic('-10042:4')['cwd'], original)
        with self.store.db:
            self.store.put('projects_root', str(self.root / 'missing'))
        _, reply = self.send(5, '/setup new unavailable', thread=10)
        self.assertIn('unavailable', reply)
        self.assertFalse(self.store.topic('-10042:10')['enabled'])
        self.assertFalse((self.root / 'missing').exists())

    def test_invalid_path_is_reported_without_breaking_admission(self):
        from coordinator.control_api import call
        self.send(2, '/setup use /bad\x00path')
        result = call(self.store, 'projects.root', {'path': '/bad\x00path'})
        self.assertFalse(result.ok)
        self.assertEqual(result.text, 'Send an absolute path to an existing folder for path.')
        self.assertFalse(self.store.topic('-10042:4')['enabled'])
        self.send(4, '/setup new valid')
        self.assertTrue(self.store.topic('-10042:4')['enabled'])

    def test_projects_guide_caps_catalog_lines_and_names_the_linked_topic(self):
        from coordinator.onboarding import projects_guide
        for number in range(23):
            (self.projects / ('project-%02d' % number)).mkdir()
        project = self.projects / 'project-00'
        self.send(2, '/setup use ' + str(project))
        reply = projects_guide(self.store, '-10042:4')
        self.assertEqual(reply.splitlines()[0], '**Projects** in ' + _plain(self.projects))
        self.assertIn('• project-00 — linked to project-00', reply.splitlines())
        self.assertEqual(len([line for line in reply.splitlines() if line.startswith('• ')]), 20)
        self.assertIn('…and 3 more.', reply.splitlines())
        self.assertNotIn('• project-20', reply)
        self.assertTrue(reply.endswith('This topic: project-00 · ' + _plain(project) + ' · accepting work'))
        _, command_reply = self.send(3, '/projects')
        self.assertEqual(command_reply, reply)
        with self.store.db:
            self.store.db.execute("UPDATE topics SET enabled=0 WHERE id='-10042:4'")
        self.assertTrue(projects_guide(self.store, '-10042:4').endswith(' · disabled'))

    def test_projects_guide_empty_and_control_topics_have_no_topic_line(self):
        from coordinator.onboarding import projects_guide
        expected = '**Projects** in ' + _plain(self.projects) + '\nNo project folders there yet.'
        self.assertEqual(projects_guide(self.store), expected)
        self.assertEqual(projects_guide(self.store, '-10042:4'),
                         expected + "\n\nThis topic isn't linked to a project yet.")
        with self.store.db:
            self.store.put('control_topic', '-10042:4')
        self.assertEqual(projects_guide(self.store, '-10042:4'), expected)
        self.send(2, '/setup new Main project')
        self.assertNotIn('This topic', projects_guide(self.store, '-10042:4'))

    def test_hidden_project_new_still_queues_a_new_topic_in_general(self):
        outcome, reply = self.send(2, '/project new General project', thread=1)
        self.assertEqual(outcome, 'control')
        self.assertIn('Creating the General project topic.', reply)
        self.assertEqual(self.store.get('group_projects'), [
            {'name': 'General project', 'cwd': str(self.projects / 'general-project'),
             'source': '-10042:0', 'release_held': False},
        ])
        self.assertTrue((self.projects / 'general-project' / '.git').is_dir())
        self.assertEqual(self.store.tasks_list(), [])

    def test_saved_numbered_selection_and_shared_project(self):
        project = self.projects / 'existing'
        project.mkdir()
        self.send(2, '/setup existing')
        self.store.close()
        self.store = Store(self.root / 'state')
        self.send(3, '1')
        self.send(5, '', thread=9, forum_topic_created={'name': 'Existing review'})
        self.send(4, '/setup use ' + str(project), thread=9)
        for topic_id in ('-10042:4', '-10042:9'):
            self.assertEqual(self.store.topic(topic_id)['cwd'], str(project))
            self.assertTrue(self.store.topic(topic_id)['enabled'])
        self.assertEqual([self.store.topic(topic_id)['name'] for topic_id in ('-10042:4', '-10042:9')],
                         ['existing', 'Existing review'])

    def test_setup_reports_global_execution_state(self):
        self.send(2, '/setup new ready')
        with self.store.db:
            self.store.put('pair_only', True)
        _, reply = self.send(3, '/setup')
        self.assertIn('pairing-only', reply)
        self.assertNotIn('paused', reply)


class LegacyTopicContractTests(unittest.TestCase):
    def test_c6_legacy_topic_creation_matches_origin_main(self):
        import hashlib
        import json
        import re
        from tests.test_control_ui import ControlUITests
        from tests.test_group_setup import GroupSetupTests

        expected = {'test_first_request_moves_to_new_project_topic': '1987b118cff6ccccb5416951d9cc076572f08e5464465a624ac447e50e1ee2e4', 'test_hidden_project_new_still_queues_a_new_topic_in_general': 'bf3875a32f5145b0e5636bf6b99d9c23615584981babede1fbf77f55b3ac7183', 'test_new_project_creates_a_topic_and_redraws_projects': '106145262a6b43506715047fb4a071fd4ba8ff148c5fbf6651519c5f34fabbc5', 'test_new_project_entirely_in_topic_survives_restart': '0c38bd2d1bcaff532a9705c5b7e8aeae792344eb39955e76ebd7f36dbe3b321f', 'test_project_new_in_general_creates_project_topic': '4493adfa2d198372a7ef59611d51bad1ca962e39da6afae9de5b440d77ba1ff2', 'test_project_new_keeps_pending_work_in_existing_project': '1379530dfa924c6728ca38d381bd260f4ccaade2de8eb6da14566afe4b2edbc0', 'test_topics_on_creates_home_and_exact_screens': '538a8a50bf4fb9c79534d4bca7ad809c7197bae0a3616e1ac08c71b7e043d9c9'}
        observed = {}
        values = {}

        def capture(case):
            volatile = {'created', 'updated', 'next_attempt', 'reaction_retry', 'turn_started'}
            value = {}
            for table in ('topics', 'tasks', 'workers', 'messages', 'outbox'):
                value[table] = [{key: item for key, item in dict(row).items() if key not in volatile}
                                for row in case.store.db.execute('SELECT * FROM ' + table + ' ORDER BY id')]
            for message in value['messages']:
                self.assertIsNone(message.pop('from_name'))
                self.assertIsNone(message.pop('batch_id'))
            for table in ('messages', 'outbox'):
                for row in value[table]:
                    self.assertIsNone(row.pop('reaction_first_failed'))
            for row in value['outbox']:
                self.assertEqual(row.pop('as_document'), 0)
            value['settings'] = {key: case.store.get(key) for key in (
                'control_topic', 'coordinator_home_topic', 'projects_root', 'group_projects', 'execution')}
            if hasattr(case, 'telegram'):
                value['telegram_calls'] = case.telegram.calls
                value['telegram_sent'] = case.telegram.sent
            raw = json.dumps(value, sort_keys=True).replace(json.dumps(_plain(case.root))[1:-1], '$ROOT')
            raw = raw.replace(str(case.root), '$ROOT')
            raw = re.sub(r'torii:[A-Za-z0-9_-]+:', 'torii:$TOKEN:', raw)
            raw = re.sub(r'[0-9]+\\u202fs ago', '$AGE ago', raw)
            observed[case._testMethodName] = hashlib.sha256(json.dumps(json.loads(raw), sort_keys=True).encode()).hexdigest()
            values[case._testMethodName] = json.loads(raw)

        class ObservedGroup(GroupSetupTests):
            async def asyncTearDown(self):
                capture(self)
                await super().asyncTearDown()

        class ObservedOnboarding(OnboardingTests):
            def tearDown(self):
                capture(self)
                super().tearDown()

        class ObservedUI(ControlUITests):
            def tearDown(self):
                capture(self)
                super().tearDown()

        suite = unittest.TestSuite([
            ObservedGroup('test_topics_on_creates_home_and_exact_screens'),
            ObservedGroup('test_first_request_moves_to_new_project_topic'),
            ObservedGroup('test_project_new_in_general_creates_project_topic'),
            ObservedGroup('test_project_new_keeps_pending_work_in_existing_project'),
            ObservedOnboarding('test_hidden_project_new_still_queues_a_new_topic_in_general'),
            ObservedOnboarding('test_new_project_entirely_in_topic_survives_restart'),
            ObservedUI('test_new_project_creates_a_topic_and_redraws_projects'),
        ])
        result = unittest.TestResult()
        suite.run(result)
        self.assertEqual(result.errors + result.failures, [])
        self.maxDiff = None
        self.assertEqual(observed, expected, json.dumps({name: value for name, value in values.items()
                                                      if observed[name] != expected[name]}, indent=2, sort_keys=True))
