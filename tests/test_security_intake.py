import os
import sys
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from coordinator import control_api
from coordinator.store import Store
from coordinator.telegram import Telegram, TelegramError
from tests.test_store import update


class ProtectedAttachmentTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.home = self.root / 'home'
        self.home.mkdir()
        self.home_patch = patch('pathlib.Path.home', return_value=self.home)
        self.home_patch.start()
        self.addCleanup(self.home_patch.stop)
        self.token = self.root / 'token'
        self.token.write_text('fake-token')
        self.store = Store(self.root / 'state', token_file=self.token)
        with self.store.db:
            self.store.put('owner', 7)
            self.store.put('group', -10042)
            self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                                  ('-10042:4', -10042, 4, 'Test', str(self.root)))
        self.api = Telegram(self.token, store=self.store)

    async def asyncTearDown(self):
        self.store.close()
        self.temp.cleanup()

    def queue(self, path):
        return control_api.call(self.store, 'telegram.send',
                                {'topic': '-10042:4', 'text': 'Report', 'image': str(path)}, source='mcp')

    def file(self, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('fake-protected-content')
        return path

    async def test_all_protected_kinds_and_their_symlinks_are_refused_at_both_layers(self):
        external = self.file(self.root / 'external-key')
        self.store.vault = SimpleNamespace(_key_file=external)
        custom = [self.root / 'claude-profile', self.root / 'codex-profile']
        with self.store.db:
            self.store.put('accounts', {'test': {'config_dir': str(custom[0])}})
            self.store.put('codex_accounts', {'test': {'config_dir': str(custom[1])}})
        protected = [self.token, external,
                     self.home / '.config/telegram-agent-coordinator/bot-token',
                     self.home / '.claude.json', self.root / 'local-overrides.json',
                     self.store.directory / 'envelope/master.key', self.store.directory / 'envelope/vault.enc',
                     self.store.directory / 'shared-mcp.json', self.store.directory / 'shared-mcp-test.json',
                     self.store.directory / 'shared-mcp-test.tmp',
                     self.store.directory / 'legacy-mcp-test.json',
                     self.store.directory / 'hosts/worker-1/spec.json',
                     self.store.directory / 'hosts/parents/topic/spec.json']
        protected += [self.home / name / 'credential.txt'
                      for name in ('.claude', '.claude-accounts', '.codex', '.codex-accounts')]
        protected += [folder / 'credential.txt' for folder in custom]
        for path in protected:
            self.file(path)
        with patch('coordinator.accounts.LOCAL_OVERRIDES_PATH', self.root / 'local-overrides.json'), \
                patch.object(self.api, '_request') as request:
            for index, path in enumerate(protected):
                link = self.root / ('link-%d' % index)
                link.symlink_to(path)
                for candidate in (path, link):
                    with self.subTest(path=path, symlink=candidate == link):
                        self.assertFalse(self.queue(candidate).ok)
                        with self.assertRaises(TelegramError) as error:
                            self.api._call_multipart('sendDocument', {}, candidate)
                        self.assertEqual(error.exception.code, 'protected-image-path')
                        with self.assertRaises(TelegramError):
                            await self.api.call_multipart('sendDocument', {}, candidate)
            request.assert_not_called()
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 0)

    async def test_review_guard_paths_case_variants_are_refused_at_both_layers(self):
        paths = [self.store.directory / name for name in
                 ('envelope/vault.enc', 'envelope/../envelope/vault.enc',
                  'ENVELOPE/vault.enc', 'Envelope/Vault.enc', 'ENVELOPE/master.key',
                  'HOSTS/worker/spec.json', 'hosts/worker/Spec.JSON', 'Shared-MCP.json',
                  'Legacy-MCP-test.JSON')]
        paths += [self.home / '.CLAUDE/credentials.json', self.home / '.Codex/auth.json']
        with patch('coordinator.attachment_paths.sys.platform', 'darwin'), \
                patch.object(self.api, '_request') as request:
            for path in paths:
                self.file(path)
                if self.home in path.parents:
                    self.store.db.execute('UPDATE topics SET cwd=?', (str(self.home),))
                    self.store.db.commit()
                with self.subTest(path=path):
                    self.assertFalse(self.queue(path).ok)
                    with self.assertRaises(TelegramError) as error:
                        await self.api.call_multipart('sendDocument', {}, path)
                    self.assertEqual(error.exception.code, 'protected-image-path')
            request.assert_not_called()

    @unittest.skipUnless(sys.platform == 'darwin', 'macOS temporary-directory aliases')
    async def test_review_guard_paths_tmp_aliases_are_refused(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as temporary:
            state = Path(temporary)
            self.file(state / 'envelope/vault.enc')
            with patch.dict(os.environ, {'TORII_STATE_DIR': str(state)}), \
                    patch.object(self.api, '_request') as request:
                for root in (Path('/tmp'), Path('/private/tmp')):
                    with self.subTest(root=root):
                        with self.assertRaises(TelegramError) as error:
                            await self.api.call_multipart('sendDocument', {}, root / state.name / 'envelope/vault.enc')
                        self.assertEqual(error.exception.code, 'protected-image-path')
                request.assert_not_called()

    async def test_protected_hard_links_are_refused_at_both_layers(self):
        protected = [self.store.directory / 'envelope/vault.enc',
                     self.store.directory / 'hosts/worker/spec.json',
                     self.store.directory / 'shared-mcp-test.json',
                     self.store.directory / 'legacy-mcp-test.json',
                     self.home / '.claude/credentials.json', self.token]
        with patch.object(self.api, '_request') as request:
            for index, path in enumerate(protected):
                self.file(path)
                link = self.root / ('hard-link-%d.pdf' % index)
                os.link(path, link)
                with self.subTest(path=path):
                    self.assertFalse(self.queue(link).ok)
                    with self.assertRaises(TelegramError) as error:
                        await self.api.call_multipart('sendDocument', {}, link)
                    self.assertEqual(error.exception.code, 'protected-image-path')
            request.assert_not_called()

    async def test_upload_checks_identity_of_the_opened_file(self):
        path = self.file(self.root / 'report.txt')
        protected = self.file(self.store.directory / 'envelope/vault.enc')
        original_open = os.open
        def swap(candidate, flags, *args, **kwargs):
            if Path(candidate) == path:
                os.replace(path, self.root / 'original-report.txt')
                os.link(protected, path)
            return original_open(candidate, flags, *args, **kwargs)
        with patch('coordinator.telegram.os.open', side_effect=swap), \
                patch.object(self.api, '_request') as request:
            with self.assertRaises(TelegramError) as error:
                await self.api.call_multipart('sendDocument', {}, path)
            self.assertEqual(error.exception.code, 'protected-image-path')
            request.assert_not_called()

    async def test_report_upload_and_queue_remain_allowed(self):
        project = self.root / 'project'
        project.mkdir()
        worktree = self.root / 'worktrees/job'
        worktree.mkdir(parents=True)
        with self.store.db:
            self.store.db.execute('UPDATE topics SET cwd=?', (str(project),))
            task = self.store.task_create('-10042:4', 'Job')
            self.store.db.execute('UPDATE tasks SET worktree=? WHERE id=?', (str(worktree), task['id']))
        for path in (project / 'image.jpg', worktree / 'report.pdf', worktree / 'screenshot.png',
                     self.store.directory / 'report.txt'):
            self.file(path)
            self.assertTrue(self.queue(path).ok)
            with patch.object(self.api, '_request', return_value={'message_id': 2}) as request:
                await self.api.call_multipart('sendDocument', {}, path)
            self.assertIn(b'fake-protected-content', request.call_args.args[1])

    async def test_upload_rechecks_registered_paths_after_queueing(self):
        path = self.file(self.root / 'new-profile/credential.txt')
        self.assertTrue(self.queue(path).ok)
        self.store.put('accounts', {'new': {'config_dir': str(path.parent)}})
        with patch.object(self.api, '_request') as request:
            with self.assertRaises(TelegramError):
                await self.api.call_multipart('sendDocument', {}, path)
            request.assert_not_called()


class MalformedUpdateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name))
        self.store.put('owner', 7)
        self.store.put('group', -10042)
        self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                              ('-10042:4', -10042, 4, 'Test', self.temp.name))
        self.store.db.commit()

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def test_invalid_shapes_advance_once_and_do_not_poison_the_next_message(self):
        shapes = [{'message': []}, {'message': None}, {'message': {}},
                  {'message': {'from': [], 'chat': {}}}, {'message': {'from': {}}},
                  {'message': {'from': {}, 'chat': 'wrong'}},
                  {'message': {**update(1, '')['message'], 'text': []}},
                  {'message': {**update(1, '')['message'], 'reply_to_message': []}},
                  {'message': {**update(1, 'Text')['message'], 'reply_to_message': {'message_id': []}}},
                  {'message': {**update(1, '')['message'], 'photo': [None]}},
                  {'message': {**update(1, '')['message'], 'photo': [{'width': 'bad'}]}},
                  {'message': {**update(1, 'Text')['message'], 'entities': [{'type': []}]}},
                  {'callback_query': []}, {'callback_query': {'from': [], 'message': {}}},
                  {'callback_query': {'from': {}, 'message': {'chat': []}}},
                  {'message_reaction': []}, {'message_reaction': {'chat': []}},
                  {'message_reaction': {'chat': {}, 'user': {}, 'old_reaction': [], 'new_reaction': {}}},
                  {'message_reaction': {'chat': {}, 'user': {}, 'old_reaction': 'bad', 'new_reaction': []}},
                  {'message_reaction': {'chat': {}, 'user': [], 'old_reaction': [], 'new_reaction': []}},
                  {'message_reaction': {'chat': {}, 'user': {}, 'old_reaction': [], 'new_reaction': [None]}}]
        for uid, shape in enumerate(shapes, 1):
            with self.subTest(shape=shape):
                item = dict(shape, update_id=uid)
                self.assertEqual(self.store.accept(item), 'invalid')
                self.assertEqual(self.store.get('offset'), uid + 1)
                self.assertEqual(self.store.accept(item), 'duplicate')
                self.assertEqual(self.store.db.execute('SELECT count(*) FROM problems').fetchone()[0], uid)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM messages').fetchone()[0], 0)

        self.assertEqual(self.store.accept(update(len(shapes) + 1, 'Next owner message')), 'queued')
        self.assertEqual(self.store.messages_pending()[0]['text'], 'Next owner message')

    def test_owner_reactions_remain_authorized_after_quarantine(self):
        self.assertEqual(self.store.accept(update(1, 'Original')), 'queued')
        reaction = {'chat': {'id': -10042, 'type': 'supergroup'}, 'message_id': 1,
                    'user': {'id': 7}, 'old_reaction': [], 'new_reaction': [{'type': 'emoji', 'emoji': '👍'}]}
        item = {'update_id': 2, 'message_reaction': reaction}
        self.assertEqual(self.store.accept(item), 'queued')
        self.assertEqual(self.store.accept(item), 'duplicate')
        self.assertEqual(self.store.messages_pending()[-1]['text'], 'Replying to: Original\nReacted 👍')
        for uid, fields in ((3, {'user': {'id': 8}}), (4, {'chat': {'id': -999}})):
            self.assertEqual(self.store.accept({'update_id': uid, 'message_reaction': dict(reaction, **fields)}), 'ignored')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM messages').fetchone()[0], 2)

    def test_unknown_updates_are_durable_and_authorization_is_preserved(self):
        item = {'update_id': 1, 'future_update': {'anything': []}}
        self.assertEqual(self.store.accept(item), 'ignored')
        self.assertEqual(self.store.accept(item), 'duplicate')
        self.assertEqual(self.store.get('offset'), 2)
        self.assertEqual(self.store.db.execute('SELECT code FROM problems').fetchone()[0], 'unknown-update')
        for item in (update(2, 'Stranger', user=8),
                     update(3, 'Bot', **{'from': {'id': 7, 'is_bot': True}}),
                     update(4, 'Other group', chat={'id': -1, 'type': 'supergroup'}),
                     update(5, 'Anonymous', sender_chat={'id': -10042})):
            self.assertIn(self.store.accept(item), ('ignored', 'unauthorized'))
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM messages').fetchone()[0], 0)
