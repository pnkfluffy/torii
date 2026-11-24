import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import tests
from coordinator.store import Store


class OutboundRedactionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.home = self.root / 'home'
        self.home.mkdir()
        self.store = Store(self.root / 'state')
        self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'test','',1)")

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def enqueue(self, text, **kwargs):
        with patch('coordinator.store._secret_homes', return_value=[self.home], create=True):
            self.store.enqueue_report('1:2', text, **kwargs)
        return [row[0] for row in self.store.db.execute('SELECT text FROM outbox ORDER BY id')]

    def test_redacts_prefix_and_exact_token_file_value(self):
        folder = self.home / '.config' / 'torii-secrets'
        folder.mkdir(parents=True)
        value = 'opaque-example-value-for-test'
        (folder / 'x.token').write_text(value + '\n')
        prefix = 'ghs_' + 'Ab3' * 12
        text = 'Prefix: ' + prefix + '\nValue: ' + value
        parts = self.enqueue(text)
        self.assertEqual(parts, ['Prefix: [token]\nValue: [token]\n\nA credential was redacted. Rotate it.'])
        rows = self.store.db.execute('SELECT area,code,detail FROM problems').fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0][0], rows[0][1]), ('secrets', 'outbound-token'))
        self.assertEqual(rows[0][2], 'labels=github,known')

    def test_git_sha_digest_and_file_path_pass_unchanged(self):
        sha = 'f74ee5dd5f7ebff280a84e56c1ab976406047f7c'
        digest = '0123456789abcdef' * 4
        path_token = 'ghs_' + 'Ab3' * 12
        text = ('commit=' + sha + '\nsha256=' + digest + '\ntoken=' + sha +
                '\ntokenFile=/Users/example/.config/torii-secrets/x.token' +
                '\ntokenFile=dir/secret.token\ntoken=/Users/example/path\ntoken=/tmp/foo' +
                '\ntoken=/MixedCase/dir1')
        self.assertEqual(self.enqueue(text), [text])
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM problems').fetchone()[0], 0)

    def test_short_shapes_and_token_files_do_not_redact_but_paths_do(self):
        folder = self.home / '.config' / 'torii-secrets'
        folder.mkdir(parents=True)
        (folder / 'x.token').write_text('abc123')
        self.store.vault = SimpleNamespace(scrub_values=lambda: ['vault-short'])
        text = 'Installed sk-learn; ran sk-test; npm_config_userconfig; Commit abc123 vault-short'
        self.assertEqual(self.enqueue(text), [text])
        token = 'ghp_' + 'A1b' * 12
        self.assertEqual(self.enqueue('./' + token + ' file=reports/' + token + '.log')[-1],
                         './[token] file=reports/[token].log\n\nA credential was redacted. Rotate it.')

    def test_long_token_file_value_is_masked(self):
        folder = self.home / '.config' / 'torii-secrets'
        folder.mkdir(parents=True)
        value = 'long-token-' + 'x' * 70000
        (folder / 'long.token').write_text(value)
        self.assertEqual(self.enqueue(value), ['[token]\n\nA credential was redacted. Rotate it.'])

    def test_longest_known_value_is_masked_first(self):
        folder = self.home / '.config' / 'torii-secrets'
        folder.mkdir(parents=True)
        (folder / 'a.token').write_text('short-value-longer')
        (folder / 'b.token').write_text('short-value-longer-with-suffix')
        self.assertEqual(self.enqueue('short-value-longer-with-suffix'),
                         ['[token]\n\nA credential was redacted. Rotate it.'])

    def test_token_files_are_reloaded_for_each_report(self):
        folder = self.home / '.config' / 'torii-secrets'
        folder.mkdir(parents=True)
        (folder / 'x.token').write_text('first-value-long-enough')
        self.enqueue('ordinary report')
        (folder / 'x.token').write_text('second-value-long-enough')
        self.assertEqual(self.enqueue('second-value-long-enough')[-1], '[token]\n\nA credential was redacted. Rotate it.')

    def test_rotation_notice_survives_parent_reply_truncation(self):
        prefix = 'ghs_' + 'Ab3' * 12
        parts = self.enqueue(('ordinary output ' * 2000) + prefix, kind='parent-reply')
        self.assertEqual(len(parts), 4)
        self.assertEqual(sum(part.count('Rotate it.') for part in parts), 1)

    def test_shared_token_shapes_and_private_key_are_masked(self):
        for value in ('glpat-' + 'a' * 20, 'tskey-auth-' + 'a' * 20,
                      'npm_' + 'a' * 36, 'AIza' + 'a' * 30,
                      '-----BEGIN PRIVATE KEY-----\nsynthetic\n-----END PRIVATE KEY-----'):
            with self.subTest(value=value):
                self.store.db.execute('DELETE FROM outbox')
                self.assertEqual(self.enqueue(value), ['[token]\n\nA credential was redacted. Rotate it.'])

    def test_unknown_assignment_does_not_trigger_a_rotation(self):
        value = 'aZ3ExampleSecretValueWith/MixedChars123'
        self.assertEqual(self.enqueue('token=' + value), ['token=' + value])
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM problems').fetchone()[0], 0)

    def test_review_identifiers_and_declarations_do_not_trigger_rotation(self):
        digest = '0123456789abcdef' * 4
        values = ['SERVICE_API_TOKEN=SERVICE_DEPLOY_TOKEN',
                  '{"tokenHash":"' + digest + '"}', 'token_sha256: ' + digest,
                  'api_key_id: 123e4567-e89b-12d3-a456-426614174000',
                  'AbCdEf0123456789_-AbCdEf0123456789_-1234']
        text = '\n'.join(values)
        self.assertEqual(self.enqueue(text), [text])
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM problems').fetchone()[0], 0)

    def test_review_known_values_in_punctuation_and_urls(self):
        folder = self.home / '.config' / 'torii-secrets'
        folder.mkdir(parents=True)
        file_value = 'AbcDef0123456789_-example-file-value-1234'
        vault_value = 'VaultValue_0123456789-abcdefghijklmnopqr'
        (folder / 'x.token').write_text(file_value)
        self.store.vault = SimpleNamespace(scrub_values=lambda: [vault_value])
        text = ('the key is ' + file_value + '.\n' + file_value + ', ' + file_value + ')\n'
                'https://x.example/' + file_value + '\nurl=https://h/' + vault_value +
                '?key=' + vault_value + '\nK=' + vault_value + '.')
        expected = ('the key is [token].\n[token], [token])\n'
                    'https://x.example/[token]\nurl=https://h/[token]'
                    '?key=[token]\nK=[token].\n\nA credential was redacted. Rotate it.')
        self.assertEqual(self.enqueue(text), [expected])
        rows = self.store.db.execute('SELECT area,code,detail FROM problems').fetchall()
        self.assertEqual([tuple(row) for row in rows], [('secrets', 'outbound-token', 'labels=known')])

    def test_discovers_native_and_nested_home_without_using_native_for_other_homes(self):
        installed_patch = getattr(tests, '_test_secret_homes_patch', None)
        if installed_patch is None:
            self.skipTest('Secret-home discovery is not installed on this revision')
        discover = installed_patch.temp_original
        native = self.root / 'native'
        nested = native / 'torii'
        other = self.root / 'other'
        for path in (nested, other):
            path.mkdir(parents=True)
        with patch('coordinator.store.pwd.getpwuid', return_value=SimpleNamespace(pw_dir=str(native))):
            with patch('coordinator.store.Path.home', return_value=nested):
                self.assertEqual(discover(), {nested, native})
            with patch('coordinator.store.Path.home', return_value=other):
                self.assertEqual(discover(), {other})


if __name__ == '__main__':
    unittest.main()
