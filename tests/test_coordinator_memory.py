import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from coordinator import memory
from coordinator.session import CoordinatorSession
from coordinator.store import Store


class CoordinatorMemoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = Store(self.root / 'state')
        self.addCleanup(self.store.close)
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:2',1,2,'Chat',?,1)",
                                  (str(self.root),))
            self.store.put('coordinator_home_topic', '1:2')

    def inputs(self, topic='1:2'):
        return CoordinatorSession._launch_inputs(self.store, 'model', 'instructions', None, topic=topic)

    def test_launch_creates_empty_per_chat_folder_and_keeps_shared_archive(self):
        shared = self.store.directory / 'coordinator-memory'
        shared.mkdir()
        archive = shared / 'MEMORY.md'
        archive.write_text('shared archive\n')
        path = memory.directory(self.store, '1:2')
        old_umask = os.umask(0)
        try:
            self.assertEqual(json.loads(self.inputs().settings)['autoMemoryDirectory'], str(path.resolve()))
        finally:
            os.umask(old_umask)
        self.assertEqual(list(path.iterdir()), [])
        self.assertEqual(path.parent.stat().st_mode & 0o777, 0o700)
        self.assertEqual(path.parent.parent.stat().st_mode & 0o777, 0o700)
        self.assertEqual(archive.read_text(), 'shared archive\n')

    def test_over_cap_records_once_without_changing_index_and_resets_after_trim(self):
        path = memory.directory(self.store, '1:2')
        path.mkdir(parents=True)
        index = path / 'MEMORY.md'
        original = ''.join('line %d\n' % n for n in range(41))
        index.write_text(original)
        with self.assertLogs('coordinator.problems', level='WARNING') as captured:
            self.assertTrue(memory.check(self.store, '1:2'))
            self.assertFalse(self.store.db.in_transaction)
            self.assertTrue(memory.check(self.store, '1:2'))
        self.assertEqual(len(captured.output), 1)
        self.assertEqual(index.read_text(), original)
        rows = self.store.db.execute("SELECT detail,topic FROM problems WHERE code='index-over-cap'").fetchall()
        self.assertEqual([(row['detail'], row['topic']) for row in rows], [('topic=1:2 lines=41', '1:2')])
        index.write_text('short\n')
        self.assertFalse(memory.check(self.store, '1:2'))
        index.write_text(original)
        with self.assertLogs('coordinator.problems', level='WARNING'):
            memory.check(self.store, '1:2')
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM problems WHERE code='index-over-cap'").fetchone()[0], 2)

    def test_nightly_prunes_only_missing_links_and_preserves_notes(self):
        path = memory.directory(self.store, '1:2')
        path.mkdir(parents=True)
        note = path / 'note.md'
        note.write_text('saved note\n')
        index = path / 'MEMORY.md'
        original = '# Index\n- [kept](note.md)\n- [missing](missing.md)\n- [web](https://example.com/x)\n'
        index.write_text(original)
        memory.nightly(self.store, {'1:2'})
        self.assertEqual(index.read_text(), original)
        memory.nightly(self.store)
        self.assertEqual(index.read_text(), '# Index\n- [kept](note.md)\n- [web](https://example.com/x)\n')
        self.assertEqual(note.read_text(), 'saved note\n')

    def test_nightly_failure_in_one_chat_does_not_stop_the_next(self):
        with self.store.db:
            self.store.db.execute("INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES ('1:3',1,3,'Other',?,1)",
                                  (str(self.root),))
        checked = []

        def check(store, topic):
            checked.append(topic)
            if topic == '1:2':
                raise UnicodeError('fixture bad index')

        with patch.object(memory, 'check', side_effect=check), self.assertLogs('coordinator.problems', 'WARNING'):
            memory.nightly(self.store)
            memory.nightly(self.store)
        self.assertEqual(checked, ['1:2', '1:3', '1:3'])
        [row] = self.store.db.execute("SELECT * FROM problems WHERE area='memory' AND code='maintenance-failed'")
        self.assertEqual(row['topic'], '1:2')
        self.assertRegex(row['detail'], r'^item=1:2 failures=1 UnicodeError: fixture bad index at '
                                        r'test_coordinator_memory\.py:\d+ check < ')
        with patch('coordinator.sweeps.time.time', return_value=time.time() + 120):
            memory.nightly(self.store)
        self.assertIsNone(self.store.db.execute('SELECT 1 FROM sweep_failures').fetchone())

    def test_prune_preserves_other_bytes_and_skips_fragment_links(self):
        path = self.root / 'MEMORY.md'
        path.write_bytes(b'Header\r\n[section](#heading)\r\n[dead](missing.md)\r\n')
        self.assertEqual(memory.prune_index(path), 1)
        self.assertEqual(path.read_bytes(), b'Header\r\n[section](#heading)\r\n')

    def test_prune_keeps_live_links_and_ignores_code_and_web(self):
        path = self.root / 'MEMORY.md'
        (self.root / 'note (2).md').write_text('note')
        (self.root / 'my note.md').write_text('note')
        (self.root / 'folder').mkdir()
        (self.root / 'alias.md').symlink_to(self.root / 'my note.md')
        (self.root / 'f.md').write_text('home')
        lines = ['[live](<my note.md>) [dead](missing.md)', '[parentheses](note (2).md)',
                 '[angle](<my note.md>)', '[folder](folder)', '[home](~/f.md)',
                 '`[code](missing.md)`', '[alias](alias.md)', '[web](https://example.com/no)',
                 '[dead](missing.md) [unfinished](missing',
                 '[gone](missing.md) [also gone](missing2.md)']
        path.write_text('\n'.join(lines) + '\n')
        with patch.dict(os.environ, {'HOME': str(self.root)}):
            self.assertEqual(memory.prune_index(path), 1)
        self.assertEqual(path.read_text(), '\n'.join(lines[:-1]) + '\n')

    def test_prune_does_not_replace_symlink_index(self):
        target = self.root / 'original.md'
        target.write_text('[missing](missing.md)\n')
        index = self.root / 'MEMORY.md'
        index.symlink_to(target)
        self.assertEqual(memory.prune_index(index), 0)
        self.assertTrue(index.is_symlink())
        self.assertEqual(target.read_text(), '[missing](missing.md)\n')
