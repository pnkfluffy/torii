from contextlib import closing
from hashlib import sha256
import json
import sqlite3
import tempfile
from pathlib import Path
import re
import time
import unittest
from unittest.mock import patch

from coordinator import control_api, mcp, policy
from coordinator.extension import available
from coordinator.legacy_rules import MARKER
from coordinator.store import Store


class LegacyRuleTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.usage = self.root / 'USAGE.md'
        self.personal = self.root / 'personal.md'
        self.chat = self.root / 'chats/1:2/chat.md'
        self.home = self.root / 'chats/1:1/chat.md'
        with closing(sqlite3.connect(self.root / 'state.sqlite')) as db, db:
            db.execute('CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
            db.executemany('INSERT INTO settings VALUES (?,?)', [
                ('coordinator_home_topic', json.dumps('1:1')),
                ('coordinator:1:2_delegation', json.dumps({'text': 'Use the chat rules.'})),
            ])

    def migration_notices(self, store):
        return [tuple(row) for row in store.db.execute(
            "SELECT detail,topic FROM problems WHERE area='policy' AND code='rule-file-migrated' ORDER BY id")]

    def test_custom_usage_moves_to_personal_and_legacy_sources_remain(self):
        text = '# Usage\n\nUse my custom rules.\n'
        self.usage.write_text(text)
        with closing(Store(self.root)) as store:
            self.assertEqual(self.personal.read_text(), text)
            self.assertEqual(self.usage.read_text(), text)
            self.assertEqual(store.get('coordinator:1:2_delegation'), {'text': 'Use the chat rules.'})
            self.assertEqual(self.migration_notices(store), [('personal.md', None), ('chat.md', '1:2')])

    def test_default_usage_does_not_create_personal(self):
        text = '# Default usage\n'
        self.usage.write_text(text)
        with patch('coordinator.legacy_rules.DEFAULT_USAGE_SHA256', sha256(text.encode()).hexdigest()):
            with closing(Store(self.root)) as store:
                self.assertFalse(self.personal.exists())
                self.assertEqual(self.usage.read_text(), text)
                self.assertIs(store.get(MARKER), True)
                self.assertEqual(self.migration_notices(store), [('chat.md', '1:2')])

    def test_existing_personal_and_chat_rules_are_kept(self):
        self.usage.write_text('# Edited usage\n')
        self.personal.write_text('')
        self.chat.parent.mkdir(parents=True)
        self.chat.write_text('# Chat\n\nExisting rule.\n')
        with closing(Store(self.root)) as store:
            self.assertEqual(self.personal.read_text(), '')
            self.assertEqual(self.chat.read_text(), '# Chat\n\nExisting rule.\n')
            self.assertEqual(self.migration_notices(store), [])

    def test_scoped_and_session_topic_rules_load_and_second_start_does_nothing(self):
        with closing(sqlite3.connect(self.root / 'state.sqlite')) as db, db:
            db.executemany('INSERT INTO settings VALUES (?,?)', [
                ('coordinator_session_topic', json.dumps('1:3')),
                ('coordinator_delegation', json.dumps({'text': 'Use the session topic rules.'})),
            ])
        session = self.root / 'chats/1:3/chat.md'
        session.parent.mkdir(parents=True)
        template = '# Chat\n\n<!-- Add chat rules here. -->\n'
        session.write_text(template)
        with closing(Store(self.root)) as store:
            self.assertEqual(self.chat.read_text(), '# Chat\n\nUse the chat rules.\n')
            self.assertEqual(session.read_text(), template + '\nUse the session topic rules.\n')
            self.assertFalse(self.home.exists())
            notices = self.migration_notices(store)
            self.assertCountEqual(notices, [('chat.md', '1:2'), ('chat.md', '1:3')])
            self.assertIsNone(store.db.execute('SELECT 1 FROM outbox').fetchone())
        self.chat.unlink()
        self.usage.write_text('An old source added later.\n')
        with closing(Store(self.root)) as store:
            self.assertFalse(self.chat.exists())
            self.assertFalse(self.personal.exists())
            self.assertEqual(self.migration_notices(store), notices)


class RuleFileTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name))
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test',?,1)", (self.temp.name,))
        self.store.put('group', 1)
        self.store.put('mode', 'group')
        self.store.put('coordinator_home_topic', '1:2')

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def test_order_and_worker_filter(self):
        policy.personal_path(self.store).write_text('# Personal\n- personal rule\n## Coordinators\n- personal coordinator\n')
        path = policy.chat_path(self.store, '1:2')
        path.parent.mkdir(parents=True)
        path.write_text('# Chat\n- chat rule\n## Coordinators\n- chat coordinator\n')
        fixed, layered = policy.instructions(self.store, 'coordinator', 'coordinator')
        worker_fixed, worker = policy.instructions(self.store, 'coordinator', 'worker')
        self.assertIn('## Owner channel', fixed)
        self.assertIn('Registered channels:', fixed)
        self.assertIn('Connected agents:', fixed)
        self.assertNotIn('## Owner channel', worker_fixed)
        self.assertIn('Declare it on the job', fixed)
        self.assertNotIn('Declare it on the job', worker_fixed)
        self.assertIn('Workers get secrets as environment variables', worker_fixed)
        self.assertLess(layered.index('personal rule'), layered.index('chat rule'))
        self.assertIn('personal coordinator', layered)
        self.assertIn('chat coordinator', layered)
        self.assertIn('personal rule', worker)
        self.assertIn('chat rule', worker)
        self.assertNotIn('coordinator', worker)

    def test_tag_rules_load_in_chat_order(self):
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:3',1,3,'other',?,1)", (self.temp.name,))
        directory = self.store.directory / 'tags'
        directory.mkdir()
        first = '- first tag rule\n## Coordinators\n- first coordinator rule\n'
        (directory / 'project_webapp.md').write_text(first)
        (directory / 'project_notes.md').write_text('- second tag rule\n')
        policy.personal_path(self.store).write_text('- personal rule\n')
        path = policy.chat_path(self.store, '1:2')
        path.parent.mkdir(parents=True)
        path.write_text('- chat rule\n')
        args = {'topic': '1:2', 'tags': 'project_webapp, project_notes'}
        result = control_api.call(self.store, 'chat.tags', args)
        self.assertTrue(result.ok, result.text)
        self.assertEqual(result.data['tags'], ['project_webapp', 'project_notes'])
        for audience in ('worker', 'coordinator'):
            with self.subTest(audience=audience):
                _, rules = policy.instructions(self.store, 'coordinator:1:2', audience)
                order = ['personal rule', 'first tag rule', 'second tag rule', 'chat rule']
                self.assertEqual(sorted(order, key=rules.index), order)
                self.assertEqual('first coordinator rule' in rules, audience == 'coordinator')
        self.assertNotIn('first tag rule', policy.chat_policy(self.store, 'coordinator:1:3'))
        shown = control_api.call(self.store, 'policy.show', {'topic': '1:2'}).text
        sizes = shown.split('\n\n', 1)[0].splitlines()
        self.assertTrue(sizes[0].startswith('Torii.md: '))
        self.assertEqual(sizes[1:], ['personal.md: 16 bytes', 'tags/project_webapp.md: %d bytes' % len(first),
                                    'tags/project_notes.md: 18 bytes', 'chat.md: 12 bytes', 'Swaps: 0 bytes'])
        self.assertTrue(control_api.call(self.store, 'chat.tags', dict(args, tags='')).ok)
        self.assertNotIn('first tag rule', policy.chat_policy(self.store, 'coordinator:1:2'))

    def test_missing_tag_file_is_ignored(self):
        self.assertTrue(control_api.call(self.store, 'chat.tags', {'topic': '1:2', 'tags': 'missing'}).ok)
        shown = control_api.call(self.store, 'policy.show', {'topic': '1:2'}).text
        self.assertIn('tags/missing.md: 0 bytes', shown)
        self.assertIsNone(self.store.db.execute("SELECT 1 FROM problems WHERE area='policy'").fetchone())

    def test_symlinked_tag_file_and_folder_are_refused(self):
        directory = self.store.directory / 'tags'
        directory.mkdir()
        outside = self.store.directory / 'outside'
        outside.mkdir()
        target = outside / 'legacy.md'
        target.write_text('- outside rule\n')
        tag = directory / 'legacy.md'
        tag.write_text('- saved rule\n')
        self.assertTrue(control_api.call(self.store, 'chat.tags', {'topic': '1:2', 'tags': 'legacy'}).ok)
        self.assertIn('saved rule', policy.chat_policy(self.store, 'coordinator'))
        tag.unlink()
        tag.symlink_to(target)
        rules = policy.chat_policy(self.store, 'coordinator')
        self.assertNotIn('outside rule', rules)
        self.assertNotIn('saved rule', rules)
        self.assertTrue(self.store.db.execute("SELECT 1 FROM problems WHERE area='policy'").fetchone())
        directory.rename(self.store.directory / 'saved-tags')
        directory.symlink_to(outside, target_is_directory=True)
        self.assertNotIn('outside rule', policy.chat_policy(self.store, 'coordinator'))

    def test_personal_rules_refuse_symlinks(self):
        outside = self.store.directory / 'outside.md'
        outside.write_text('- outside rule\n')
        personal = self.store.directory / 'personal.md'
        personal.symlink_to(outside)
        with self.assertRaises(OSError):
            policy.personal_path(self.store)
        self.assertTrue(personal.is_symlink())
        self.assertEqual(outside.read_text(), '- outside rule\n')

    def test_active_swap_follows_chat(self):
        self.store.put('loadout.swaps', [{'id': 1, 'key': 'workers.codex.effort', 'value': 'high',
                                          'topic': '1:2', 'until': time.time() + 3600, 'message': 1}])
        path = policy.chat_path(self.store, '1:2')
        path.parent.mkdir(parents=True)
        path.write_text('- Chat rule\n')
        _, layered = policy.instructions(self.store, 'coordinator', 'coordinator')
        self.assertLess(layered.index('Chat rule'), layered.index('workers.codex.effort'))

    def test_owner_message_required_and_append_is_loaded(self):
        self.assertFalse(control_api.call(self.store, 'policy.chat', {'topic': '1:2', 'append': '- rule'}).ok)
        owner = self.store.message_save('1:2', 'owner', 'add rule')
        result = control_api.call(self.store, 'policy.chat', {'topic': '1:2', 'message': owner['id'], 'append': '- rule'})
        self.assertTrue(result.ok, result.text)
        self.assertIn('- rule', policy.chat_policy(self.store, 'coordinator'))
        self.assertIn('- rule', policy.chat_path(self.store, '1:2').read_text())

    def test_rejects_rules_over_budget(self):
        owner = self.store.message_save('1:2', 'owner', 'add rules')
        result = control_api.call(self.store, 'policy.chat', {'topic': '1:2', 'message': owner['id'], 'append': '- ' + 'x' * 3999})
        self.assertFalse(result.ok)
        self.assertIn('HTML comments do not count', result.text)
        self.assertFalse(policy.chat_path(self.store, '1:2').exists())
        result = control_api.call(self.store, 'policy.set', {'text': '- ' + 'x' * 3999})
        self.assertFalse(result.ok)
        self.assertIn('HTML comments do not count', result.text)
        self.assertFalse(control_api.call(self.store, 'topic.create', {'name': 'New chat', 'message': owner['id'], 'rules': '- ' + 'x' * 3999}).ok)

    def test_many_short_rule_lines_are_accepted(self):
        owner = self.store.message_save('1:2', 'owner', 'add rules')
        lines = '\n'.join('- rule %d' % n for n in range(30))
        self.assertTrue(control_api.call(self.store, 'policy.chat', {'topic': '1:2', 'message': owner['id'], 'append': lines}).ok)
        self.assertEqual(policy.rule_lines(policy.chat_path(self.store, '1:2').read_text()), 30)
        self.assertTrue(control_api.call(self.store, 'policy.set', {'text': lines}).ok)
        self.assertEqual(policy.rule_lines(policy.personal_path(self.store).read_text()), 30)

    def test_budgets_and_provenance(self):
        root = Path(__file__).resolve().parents[1] / 'coordinator' / 'defaults'
        text = (root / 'Torii.md').read_text()
        personal = (root / 'personal.template.md').read_text()
        self.assertLessEqual(len(policy._clean(text).encode()), 6000)
        sections = {'Tools', 'Projects and service', 'When you are a worker', 'Secrets', 'Job folders', 'Deleting files'}
        parts = policy._clean(text).split('\n## ')
        fixed = '\n\n'.join('## ' + part for part in parts[1:] if part.split('\n', 1)[0] in sections)
        self.assertLessEqual(len(fixed.encode()), 3000)
        self.assertLessEqual(len(policy._clean(personal)), 4000)
        for content in (text, personal):
            self.assertIsNone(re.search(r'owner\s+\d+', policy._clean(content), re.I))
        descriptions = {t['name']: t['description'] for t in mcp.tools_list(self.store)}
        for name in ('policy_chat', 'topic_create'):
            self.assertLessEqual(len(descriptions[name]), 300)

    def test_owner_channel_rule_is_in_the_fixed_rules(self):
        rule = ("Message the owner only for an answer to their question, an action or decision only they can take, "
                "a result they asked for, or a significant milestone. Do not send routine 'landed' or "
                "'no action needed' updates.")
        fixed, _ = policy.instructions(self.store, 'coordinator', 'coordinator')
        owner_channel = fixed.split('## Owner channel\n', 1)[1].split('\n## ', 1)[0]
        self.assertIn(rule, owner_channel)

    def test_retired_switch_ops_are_gone(self):
        self.assertNotIn('events.mcp', control_api.BY_ID)
        self.assertNotIn('instructions.v2', control_api.BY_ID)

    def test_bad_rule_files_keep_last_good_layer(self):
        personal = policy.personal_path(self.store)
        personal.write_text('- good personal\n')
        path = policy.chat_path(self.store, '1:2')
        path.parent.mkdir(parents=True)
        path.write_text('- good chat\n')
        self.assertIn('good chat', policy.chat_policy(self.store, 'coordinator'))
        personal.write_bytes(b'bad \xff')
        path.write_bytes(b'bad \xff')
        layered = policy.chat_policy(self.store, 'coordinator')
        self.assertIn('good personal', layered)
        self.assertIn('good chat', layered)
        self.assertTrue(self.store.db.execute("SELECT 1 FROM problems WHERE area='policy'").fetchone())
        original = policy.read_rule

        def denied(target, *args, **kwargs):
            if target == personal:
                raise PermissionError('unreadable')
            return original(target, *args, **kwargs)
        with patch.object(policy, 'read_rule', denied):
            self.assertIn('good personal', policy.chat_policy(self.store, 'coordinator'))
        dangling = policy.chat_path(self.store, '1:3')
        dangling.parent.mkdir(parents=True)
        dangling.symlink_to(dangling.parent / 'missing')
        self.assertNotIn('good chat', policy.chat_policy(self.store, 'coordinator:1:3'))

    def test_coordinator_headings_and_nested_sections(self):
        for heading in ('# coordinator', '## COORDINATORS'):
            text = '- before\n' + heading + '\n- hidden\n### Nested\n- hidden nested\n## Other\n- after\n'
            output = policy._without_coordinators(text)
            self.assertIn('before', output)
            self.assertNotIn('hidden', output)
            if heading.startswith('##'):
                self.assertIn('after', output)
            self.assertEqual(policy._without_coordinators('- before\n' + heading + '\n'), '- before\n')
        self.assertEqual(policy._without_coordinators('- only rule\n'), '- only rule\n')

    def test_worker_rules_replace_the_coordinator_secret_steps(self):
        coordinator, _ = policy.instructions(self.store, 'coordinator', 'coordinator')
        worker, _ = policy.instructions(self.store, 'coordinator', 'worker')
        for sentence in ('Ask for a credential by name with `secret.ask`.',
                         'Declare it on the job just before the step that needs it, and clear it after.'):
            self.assertIn(sentence, coordinator)
            self.assertNotIn(sentence, worker)
        self.assertIn('Run `secret.list` to see stored names. Ask the coordinator to declare a stored name on '
                      'your job, or to ask the owner for a new one.', worker)
        self.assertIn('- Workers get secrets as environment variables', worker)

    def test_workers_do_not_get_coordinator_tools(self):
        fixed, _ = policy.instructions(self.store, 'coordinator', 'worker')
        for name in ('telegram.send', 'workers.spawn', 'secret.ask', 'policy.chat'):
            self.assertNotIn(name, fixed)
        self.assertIn('coordinator handles Telegram delivery', fixed)

    def test_comments_do_not_count_atomic_writes_and_schema(self):
        rules = ('- ' + 'r' * 97 + '\n') * 38 + '<!-- ' + 'c' * 791 + ' -->'
        self.assertEqual((len(rules), len(rules) - len(policy._clean(rules))), (4600, 800))
        self.assertTrue(control_api.call(self.store, 'policy.set', {'text': rules}).ok)
        self.assertEqual(policy.personal_path(self.store).read_text(), rules + '\n')
        owner = self.store.message_save('1:2', 'owner', 'add rule')
        result = control_api.call(self.store, 'policy.chat', {'topic': '1:2', 'message': owner['id'], 'append': rules})
        self.assertTrue(result.ok, result.text)
        self.assertEqual(policy.chat_path(self.store, '1:2').read_text(), rules + '\n')
        result = control_api.call(self.store, 'topic.create', {'name': 'New chat', 'message': owner['id'], 'rules': rules})
        self.assertTrue(result.ok, result.text)
        with patch('os.replace', wraps=__import__('os').replace) as replace:
            self.assertTrue(control_api.call(self.store, 'policy.set', {'text': '- rule'}).ok)
            self.assertTrue(control_api.call(self.store, 'policy.chat', {'topic': '1:2', 'message': owner['id'], 'append': '- chat'}).ok)
            self.assertEqual(replace.call_count, 2)
        tool = next(t for t in mcp.tools_list(self.store) if t['name'] == 'policy_chat')
        self.assertNotIn('text', tool['inputSchema']['properties'])
        self.assertIn('append', tool['inputSchema']['properties'])

    def test_policy_show_does_not_create_scratch(self):
        self.assertTrue(control_api.call(self.store, 'policy.show', {'topic': '1:2'}).ok)
        self.assertFalse((self.store.directory / 'scratch').exists())

    def test_tools_hash_is_pinned(self):
        (self.store.directory / 'personal.md').unlink(missing_ok=True)
        self.assertEqual(mcp.tools_hash(self.store), ('fb096b741c8fdd2d0573a04e036d5f6bb8ca06596e8976f3840dd6b2ecb8c99a' if available('trash_move')
                         else '3b38e441f2965d36270fdf0424e376d706c7e9e4ef9da45945b5588e64d06b9f'))
        self.assertFalse((self.store.directory / 'personal.md').exists())
