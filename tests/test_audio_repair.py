import asyncio
import hashlib
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, patch

from coordinator import media, transcribe, transcription
from coordinator.store import Store
from tests.test_media import ImageTelegram
from tests.test_store import update


class AudioRepairTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.store = Store(self.root / 'state')
        with self.store.db:
            self.store.put('owner', 7)
            self.store.put('group', -10042)
            self.store.put('transcription_model', 'whisper')
            self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                                  ('-10042:4', -10042, 4, 'Chat', str(self.root)))
        self.api = ImageTelegram()
        self.directory = self.store.directory / 'transcription'
        python = self.directory / 'venv/bin/python'
        python.parent.mkdir(parents=True)
        python.write_text('#!' + sys.executable + '''
from pathlib import Path
import sys
import time
directory = Path(__file__).resolve().parents[2]
if sys.argv[-1] == '--check':
    raise SystemExit(0 if (directory / 'complete').is_file() else 1)
if sys.argv[-1] in ('--setup', '--repair'):
    with (directory / 'fetches').open('a') as output:
        output.write('fetch\\n')
    time.sleep(0.05)
    if (directory / 'fail-download').exists():
        raise SystemExit(1)
    (directory / 'complete').write_text('fake weights')
else:
    print('Fixture transcript')
''')
        python.chmod(0o700)
        backend = 'macos' if transcribe.uses_mlx() else 'linux'
        revision = hashlib.sha256(Path(transcription.__file__).with_name(
            'transcription-' + backend + '.lock').read_bytes() + Path(transcribe.__file__).read_bytes()).hexdigest()
        (self.directory / 'ready').write_text(revision)
        self.environment = patch.object(transcription, 'environment', return_value={
            'HOME': str(self.root), 'PATH': '/usr/bin:/bin'})
        self.environment.start()
        self.addCleanup(self.environment.stop)

    async def asyncTearDown(self):
        self.store.close()
        self.temporary.cleanup()

    async def notes(self):
        work = []
        for number in (2, 3, 4):
            self.api.files['note-%d' % number] = b'fake audio'
            self.store.accept(update(number, '', voice={'file_id': 'note-%d' % number}))
        for message in self.store.messages_pending():
            rows = await media.prepare_attachments(self.store, self.api, message)
            work.append((message, rows))
        await asyncio.gather(*(media.prepare_transcripts(self.store, message, rows) for message, rows in work))
        return [rows[0] for _, rows in work]

    async def test_incomplete_ready_snapshot_is_fetched_once_for_three_parallel_notes(self):
        rows = await self.notes()
        self.assertTrue((self.directory / 'fetches').is_file(), 'missing model was not repaired')
        self.assertEqual((self.directory / 'fetches').read_text(), 'fetch\n')
        self.assertEqual([row.get('transcript') for row in rows], ['Fixture transcript'] * 3)
        problems = self.store.db.execute("SELECT area,code FROM problems WHERE area='transcription'").fetchall()
        self.assertEqual([tuple(row) for row in problems], [('transcription', 'model-repair')])

    async def test_failed_repair_is_single_flight_and_all_notes_have_a_clear_reason(self):
        (self.directory / 'fail-download').touch()
        rows = await self.notes()
        self.assertTrue((self.directory / 'fetches').is_file(), 'missing model was not repaired')
        self.assertEqual((self.directory / 'fetches').read_text(), 'fetch\n')
        self.assertEqual([row['error'] for row in rows],
                         ['transcription model unavailable; repair download failed'] * 3)
        self.assertEqual(self.store.db.execute(
            "SELECT count(*) FROM problems WHERE area='transcription' AND code='model-repair'").fetchone()[0], 1)
        for row in rows:
            self.assertIn('repair download failed', media.file_instructions([row]))

    async def test_note_deadline_uses_persisted_duration_or_size_and_caps_at_one_hour(self):
        cases = [('voice', {'duration': 120}, 9, 420),
                 ('audio', {'duration': 900}, 9, 1200),
                 ('video_note', {'duration': 99999}, 9, 3600),
                 ('document', {'file_name': 'recording.ogg'}, 20000, 310)]
        process = Mock(returncode=0)
        process.communicate = AsyncMock(return_value=(b'Fixture transcript', b''))
        with patch.object(media, 'setup_transcription', AsyncMock(return_value=('/fake/python', {}))), \
                patch.object(media.asyncio, 'create_subprocess_exec', AsyncMock(return_value=process)), \
                patch.object(media.asyncio, 'wait_for', wraps=asyncio.wait_for) as deadline:
            for number, (kind, metadata, size, expected) in enumerate(cases, 2):
                self.api.files['note-%d' % number] = b'a' * size
                self.store.accept(update(number, '', **{kind: dict(metadata, file_id='note-%d' % number)}))
                message = self.store.messages_pending()[-1]
                rows = await media.prepare_attachments(self.store, self.api, message)
                await media.prepare_transcripts(self.store, message, rows)
                self.assertEqual(rows[0].get('transcript'), 'Fixture transcript')
                self.assertEqual(deadline.call_args.args[1], expected)

    async def test_selected_parakeet_repairs_only_its_files_for_parallel_notes(self):
        parakeet = self.directory / 'parakeet'
        python = parakeet / 'venv/bin/python'
        python.parent.mkdir(parents=True)
        python.write_text((self.directory / 'venv/bin/python').read_text())
        python.chmod(0o700)
        revision = hashlib.sha256(Path(transcription.__file__).with_name(
            'transcription-parakeet.lock').read_bytes() + Path(transcribe.__file__).read_bytes()).hexdigest()
        (parakeet / 'ready').write_text(revision)
        (self.directory / 'complete').write_text('inactive whisper fixture')
        with self.store.db:
            self.store.put('transcription_model', 'parakeet')
        async def setup(state, on_repair=None, backend='parakeet'):
            env = {'HOME': str(self.root), 'PATH': '/usr/bin:/bin'}
            python = await asyncio.to_thread(transcription.prepare_environment, state, env, backend=backend)
            return python, env
        with patch.object(transcription.platform, 'system', return_value='Darwin'), \
                patch.object(transcription.platform, 'machine', return_value='arm64'), \
                patch.object(transcription.platform, 'mac_ver', return_value=('27.0', (), 'arm64')), \
                patch.object(transcription.shutil, 'which', return_value='/fake/ffmpeg'), \
                patch.object(media, 'setup_transcription', side_effect=setup):
            rows = await self.notes()
        self.assertEqual([row.get('transcript') for row in rows], ['Fixture transcript'] * 3)
        self.assertTrue((parakeet / 'fetches').is_file(), 'selected Parakeet snapshot was not repaired')
        self.assertEqual((parakeet / 'fetches').read_text(), 'fetch\n')
        self.assertFalse((self.directory / 'fetches').exists())
        self.assertEqual((self.directory / 'complete').read_text(), 'inactive whisper fixture')


class SnapshotCompletenessTests(unittest.TestCase):
    def test_repair_refetches_the_pinned_model_with_network_access(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            def fetch(**kwargs):
                self.assertFalse(kwargs['local_files_only'])
                self.assertTrue(kwargs.get('force_download'), 'repair must replace incomplete cached files')
                self.assertEqual(kwargs['revision'], transcribe.MAC_MODEL[1])
                for name in ('config.json', 'weights.safetensors'):
                    (directory / name).write_bytes(b'complete fixture')
                return str(directory)
            hub = Mock()
            hub.snapshot_download.side_effect = fetch
            with patch.dict('sys.modules', {'huggingface_hub': hub}), \
                    patch.object(transcribe, 'uses_mlx', return_value=True), \
                    patch.object(transcribe.sys, 'argv', ['transcribe.py', '--backend', 'whisper', '--repair']):
                transcribe.main()
            self.assertEqual((directory / 'weights.safetensors').read_bytes(), b'complete fixture')
            self.assertEqual(hub.snapshot_download.call_count, 1)

    def test_cached_snapshot_checks_required_files_before_returning_a_model_path(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            hub = Mock()
            hub.snapshot_download.return_value = str(directory)
            with patch.dict('sys.modules', {'huggingface_hub': hub}):
                for mlx, names in ((True, ('config.json', 'weights.safetensors')),
                                   (False, ('config.json', 'model.bin', 'tokenizer.json', 'vocabulary.json'))):
                    with self.subTest(mlx=mlx), patch.object(transcribe, 'uses_mlx', return_value=mlx):
                        for name in names:
                            if name == names[1]:
                                continue
                            (directory / name).write_bytes(b'fake model file')
                        with self.assertRaisesRegex(RuntimeError, 'incomplete'):
                            transcribe.model_path(backend='whisper')
                        (directory / names[1]).write_bytes(b'fake model file')
                        self.assertEqual(transcribe.model_path(backend='whisper'), str(directory))
                        (directory / names[1]).write_bytes(b'')
                        with self.assertRaisesRegex(RuntimeError, 'incomplete'):
                            transcribe.model_path(backend='whisper')
