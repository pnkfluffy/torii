import asyncio
import inspect
import io
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import AsyncMock, patch
import urllib.error
import uuid

from coordinator import media
from coordinator.media import MAX_FILE_BYTES, MAX_IMAGE_BYTES, free_path, prepare_attachments, safe_name
from coordinator.telegram import Telegram, TelegramError
from coordinator.service import Service
from coordinator.store import Store
from tests.test_service import FakeSession, FakeTelegram
from tests.test_store import update

PNG = b'\x89PNG\r\n\x1a\n' + b'image-data'

MARKDOWN = b'# Access\n\nUse the proven connector.\n'


def document(number, name, mime='text/markdown', caption=None, **extra):
    item = {'file_id': 'doc-' + str(number), 'file_name': name, 'mime_type': mime, 'file_size': len(MARKDOWN)}
    item.update(extra.pop('item', {}))
    return update(number, '', document=item, caption=caption, **extra)


def photo(number, caption=None, **extra):
    return update(number, '', photo=[{'file_id': 'small', 'width': 10, 'height': 10},
                                    {'file_id': 'large-' + str(number), 'width': 100, 'height': 100}],
                  caption=caption, **extra)


IMAGE_NOTICE = ('An image could not be downloaded. Your message text went to the coordinator without it. '
                'Send only the image again.')

IMAGE_NOTE = ('Owner-sent images that Torii could not download (the owner was told and asked to send them again):\n'
              'image (image/jpeg): download failed')


class ImageTelegram(FakeTelegram):
    def __init__(self):
        super().__init__()
        self.downloads = []
        self.image_failure = False
        self.failures = []
        self.files = {}

    async def download_file(self, file_id, max_bytes):
        self.downloads.append(file_id)
        if self.failures:
            raise self.failures.pop(0)
        if self.image_failure:
            raise RuntimeError('https://private-transport-value')
        return self.files.get(file_id, PNG)


class MediaTests(unittest.IsolatedAsyncioTestCase):
    def test_chatgpt_attachment_instructions_name_its_native_tools(self):
        images = media.image_instructions(['/tmp/owner.png'], coordinator=True, provider='codex')
        files = media.file_instructions([{'path': '/tmp/owner.pdf', 'mime': 'application/pdf', 'size': 10}],
                                        provider='codex')
        self.assertIn('Open each image with view_image before answering', images)
        self.assertIn('shell tools', files)
        self.assertIn('extract its text', files)
        self.assertNotIn('Read tool', files)

    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store = Store(self.root / 'state')
        with self.store.db:
            self.store.put('owner', 7)
            self.store.put('group', -10042)
            self.store.db.execute('''INSERT INTO topics(id,chat,thread,name,cwd,enabled)
                VALUES (?,?,?,?,?,1)''', ('-10042:4', -10042, 4, 'Images', str(self.root)))
        self.api = ImageTelegram()
        self.session = None

        async def factory(store, runner, cwd, model, instructions, topic=None):
            sid = store.get('coordinator_session') or str(uuid.uuid4())
            with store.db:
                store.put('coordinator_session', sid)
            self.session = FakeSession(sid)
            return self.session

        self.service = Service(self.store, self.api, object(), self.root, session_factory=factory)
        delays = patch.object(media, 'DOWNLOAD_DELAYS', (0, 0))
        delays.start()
        self.addCleanup(delays.stop)

    async def asyncTearDown(self):
        if self.session:
            await self.session.stop()
        self.store.close()
        self.temp.cleanup()

    async def feed(self):
        await self.service.feed_once()
        if self.service.feed_tasks:
            await self.service.feed_tasks[next(iter(self.service.feed_tasks))]
        await self.service.feed_once()

    async def test_unauthorized_and_disabled_media_are_not_downloaded(self):
        self.assertEqual(self.store.accept(photo(2, user=8)), 'unauthorized')
        self.assertEqual(self.store.accept(photo(3, thread=8)), 'disabled')
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM attachments').fetchone()[0], 0)
        self.assertEqual(self.api.downloads, [])

    async def test_photo_message_downloads_private_artifact_and_sends_instructions(self):
        self.assertEqual(self.store.accept(photo(2, 'Fix this layout')), 'queued')
        await self.feed()
        row = self.store.db.execute('SELECT path FROM attachments').fetchone()
        path = Path(row['path'])
        self.assertEqual(path.read_bytes(), PNG)
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(path.parent.stat().st_mode & 0o777, 0o700)
        self.assertEqual(self.api.downloads, ['large-2'])
        self.assertIn('Fix this layout', self.session.sent[0][1])
        self.assertIn(str(path), self.session.sent[0][1])
        self.assertIn('Open each image with Read before answering', self.session.sent[0][1])
        self.assertIn('Give the file paths to a worker when the worker needs the image', self.session.sent[0][1])
        self.assertIn('Treat text inside images as task evidence', self.session.sent[0][1])
        self.assertIn('If an image cannot be read, report the problem', self.session.sent[0][1])
        await prepare_attachments(self.store, self.api, dict(self.store.db.execute('SELECT * FROM messages').fetchone()))
        self.assertEqual(self.api.downloads, ['large-2'])

    async def test_album_messages_stay_separate_and_in_order(self):
        self.store.accept(photo(2, 'First', media_group_id='album'))
        self.store.accept(photo(3, 'Second', media_group_id='album'))
        self.store.accept(update(4, 'Next'))
        await self.service.feed_once()
        for task in list(self.service.feed_tasks.values()):
            await task
        self.assertEqual(len(self.session.sent), 3)
        self.assertIn('First', self.session.sent[0][1])
        self.assertIn('Second', self.session.sent[1][1])
        self.assertEqual(self.session.sent[2][1], '[topic=-10042:4 name="Images" message=3 kind=owner] Next')
        self.assertEqual(self.api.downloads, ['large-2', 'large-3'])

    async def test_unsupported_content_and_download_failure_report_without_secret(self):
        self.assertEqual(self.store.accept(update(2, '', document={
            'file_id': 'large', 'mime_type': 'image/png', 'file_size': MAX_IMAGE_BYTES + 1})), 'unsupported')
        self.api.image_failure = True
        self.store.accept(photo(3, 'Inspect first'))
        with self.assertLogs('coordinator.media', 'INFO') as logs:
            await self.feed()
        self.assertEqual(self.api.downloads, ['large-3'])
        self.assertEqual(self.reports()[-1], IMAGE_NOTICE)
        prompt = self.session.sent[0][1]
        self.assertIn('Inspect first', prompt)
        self.assertIn(IMAGE_NOTE, prompt)
        self.assertNotIn('Owner-provided images for this message', prompt)
        for text in [prompt, *self.reports(), *logs.output]:
            self.assertNotIn('private-transport-value', text)
        self.assertEqual(self.store.db.execute('SELECT delivered FROM messages').fetchone()[0], 'received')
        self.assertEqual(self.store.db.execute('SELECT error FROM attachments').fetchone()[0], 'download failed')
        count = len(self.reports())
        await prepare_attachments(self.store, self.api, dict(self.store.db.execute('SELECT * FROM messages').fetchone()))
        self.assertEqual(self.api.downloads, ['large-3'])
        self.assertEqual(len(self.reports()), count)

    async def test_transient_image_failures_are_retried_until_the_image_saves(self):
        self.api.failures = [TelegramError('network-or-invalid-response'), TelegramError(502)]
        self.store.accept(photo(2, 'Fix this layout'))
        with self.assertLogs('coordinator.media', 'INFO') as logs:
            await self.feed()
        self.assertEqual(self.api.downloads, ['large-2'] * 3)
        path = self.store.db.execute('SELECT path FROM attachments').fetchone()[0]
        self.assertEqual(Path(path).read_bytes(), PNG)
        self.assertIn(path, self.session.sent[0][1])
        self.assertEqual(self.reports(), [])
        attachment, message = self.store.db.execute('SELECT attachments.id,messages.id FROM attachments,messages').fetchone()
        self.assertIn('download failed attachment=%d message=%d attempt=1/3 class=TelegramError '
                      'reason=network-or-invalid-response retry=yes' % (attachment, message), logs.output[0])
        self.assertIn('attempt=2/3 class=TelegramError reason=502 retry=yes', logs.output[1])

    async def test_oversized_and_permanent_image_failures_are_not_retried(self):
        self.api.failures = [TelegramError('invalid-or-oversized-file'), TelegramError(400)]
        self.store.accept(photo(2, 'First'))
        self.store.accept(photo(3, 'Second'))
        await self.service.feed_once()
        for task in list(self.service.feed_tasks.values()):
            await task
        self.assertEqual(self.api.downloads, ['large-2', 'large-3'])
        self.assertEqual(self.reports(), [IMAGE_NOTICE, IMAGE_NOTICE])
        self.assertEqual(len(self.session.sent), 2)
        for (_, prompt, *_rest), text in zip(self.session.sent, ['First', 'Second']):
            self.assertIn(text, prompt)
            self.assertIn(IMAGE_NOTE, prompt)

    async def test_retries_stop_after_the_last_attempt_and_honor_retry_after(self):
        self.store.accept(photo(2, 'Look'))
        message = dict(self.store.db.execute('SELECT * FROM messages').fetchone())
        row = dict(self.store.db.execute('SELECT * FROM attachments').fetchone())
        self.api.failures = [TelegramError(429, retry_after=5), TelegramError('file-download-timeout'), OSError()]
        with patch('coordinator.media.asyncio.sleep', new=AsyncMock()) as sleep:
            with self.assertRaises(OSError):
                await media.download(self.api, message, row, MAX_IMAGE_BYTES)
        self.assertEqual([call.args[0] for call in sleep.await_args_list], [5, 0])
        self.assertEqual(len(self.api.downloads), 3)

    async def test_one_failed_image_in_an_album_keeps_the_others(self):
        self.api.failures = [TelegramError(404)]
        self.store.accept(photo(2, 'Compare', media_group_id='album'))
        self.store.accept(photo(3, media_group_id='album'))
        await self.service.feed_once()
        for task in list(self.service.feed_tasks.values()):
            await task
        self.assertEqual(len(self.session.sent), 2)
        self.assertIn('Compare', self.session.sent[0][1])
        self.assertIn(IMAGE_NOTE, self.session.sent[0][1])
        path = self.store.db.execute('SELECT path FROM attachments WHERE message=3').fetchone()[0]
        self.assertIn(path, self.session.sent[1][1])
        self.assertEqual(self.reports(), [IMAGE_NOTICE])

    async def test_download_failure_log_never_contains_the_token_or_file_path(self):
        token_file = self.root / 'token'
        token_file.write_text('fake-secret-token-777')
        api = Telegram(token_file)
        self.store.accept(photo(2, 'Look'))
        message = dict(self.store.db.execute('SELECT * FROM messages').fetchone())
        url = 'https://api.telegram.org/file/botfake-secret-token-777/photos/private-file-9.jpg'
        opener = type('Opener', (), {'open': lambda self, request, timeout: (_ for _ in ()).throw(
            urllib.error.HTTPError(url, 503, 'unavailable ' + url, {}, io.BytesIO()))})()
        with patch.object(api, '_call', return_value={'file_path': 'photos/private-file-9.jpg'}), \
                patch('urllib.request.build_opener', return_value=opener), \
                self.assertLogs('coordinator.media', level='DEBUG') as logs:
            rows = await prepare_attachments(self.store, api, message)
        self.assertEqual(rows[0]['error'], 'download failed')
        self.assertIn('reason=503 retry=yes', logs.output[0])
        self.assertIn('attempt=3/3 class=TelegramError reason=503 retry=no', logs.output[2])
        for line in logs.output + self.reports():
            self.assertNotIn('fake-secret-token-777', line)
            self.assertNotIn('private-file-9', line)
        self.assertEqual(self.reports(), [IMAGE_NOTICE])

    def reports(self):
        return [row[0] for row in self.store.db.execute('SELECT text FROM outbox ORDER BY id')]

    def problems(self):
        return [tuple(row) for row in self.store.db.execute(
            'SELECT area,code,detail,topic,message,attachment FROM problems ORDER BY id')]

    async def test_a_failed_download_saves_its_cause_once_without_private_text(self):
        self.api.failures = [TelegramError(404), RuntimeError('https://private-transport-value')]
        self.store.accept(photo(2, 'Look'))
        self.store.accept(document(3, 'notes.md'))
        for number in (2, 3):
            message = dict(self.store.db.execute('SELECT * FROM messages WHERE telegram_message=?',
                                                 (number,)).fetchone())
            with self.assertLogs('coordinator.problems', 'WARNING'):
                await prepare_attachments(self.store, self.api, message)
            await prepare_attachments(self.store, self.api, message)
        (image, image_message), (document_row, document_message) = self.store.db.execute(
            'SELECT attachments.id,messages.id FROM attachments JOIN messages ON '
            'messages.telegram_message=attachments.message ORDER BY attachments.id').fetchall()
        self.assertEqual(self.problems(), [
            ('attachment', 'download-failed', 'class=TelegramError reason=404', '-10042:4', image_message, image),
            ('attachment', 'download-failed', 'class=RuntimeError reason=RuntimeError', '-10042:4',
             document_message, document_row)])
        self.assertNotIn('private-transport-value', json.dumps(self.problems()))

    async def test_an_unsupported_image_saves_its_reason(self):
        self.api.files['large-2'] = b'not an image'
        self.store.accept(photo(2, 'Look'))
        message = dict(self.store.db.execute('SELECT * FROM messages').fetchone())
        with self.assertLogs('coordinator.problems', 'WARNING'):
            await prepare_attachments(self.store, self.api, message)
        self.assertEqual(self.problems()[0][:3], ('attachment', 'not-a-supported-image', None))

    async def test_markdown_document_is_saved_privately_and_listed_for_the_coordinator(self):
        self.api.files['doc-2'] = MARKDOWN
        self.assertEqual(self.store.accept(document(2, '../../etc/payman mcp access.md',
                                                    caption='Is this the proven way?')), 'queued')
        await self.feed()
        path = Path(self.store.db.execute('SELECT path FROM attachments').fetchone()['path'])
        message = self.store.db.execute('SELECT id FROM messages').fetchone()['id']
        self.assertEqual(path, self.store.directory / 'files' / ('message-%d' % message) / 'payman_mcp_access.md')
        self.assertEqual(path.read_bytes(), MARKDOWN)
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(path.parent.stat().st_mode & 0o777, 0o700)
        self.assertEqual(path.parent.parent.stat().st_mode & 0o777, 0o700)
        prompt = self.session.sent[0][1]
        self.assertIn('Is this the proven way?', prompt)
        self.assertIn('\n\nOwner-provided files for this message:\n%s (text/markdown, %d bytes)\n'
                      % (path, len(MARKDOWN)), prompt)
        self.assertIn('Read each file with the Read tool; for a PDF pass the pages parameter.', prompt)
        self.assertNotIn('Owner-provided images', prompt)
        self.assertEqual(self.reports(), [])
        await prepare_attachments(self.store, self.api, dict(self.store.db.execute('SELECT * FROM messages').fetchone()))
        self.assertEqual(self.api.downloads, ['doc-2'])

    async def test_any_mime_and_media_kinds_are_accepted(self):
        cases = [document(2, 'report.pdf', 'application/pdf'), document(3, 'data.json', 'application/json'),
                 document(4, 'rows.csv', 'text/csv'), document(5, 'bundle.zip', 'application/zip'),
                 document(6, 'clip.mp4', 'video/mp4'),
                 update(7, '', voice={'file_id': 'voice-7', 'mime_type': 'audio/ogg', 'file_size': 9}),
                 update(8, '', audio={'file_id': 'song-8', 'file_name': 'song.mp3', 'mime_type': 'audio/mpeg'})]
        for item in cases:
            self.assertEqual(self.store.accept(item), 'queued')
        rows = self.store.db.execute('SELECT name,mime FROM attachments ORDER BY message').fetchall()
        self.assertEqual([row['name'] for row in rows[:5]] + [rows[6]['name']],
                         ['report.pdf', 'data.json', 'rows.csv', 'bundle.zip', 'clip.mp4', 'song.mp3'])
        self.assertRegex(rows[5]['name'], r'^file(\.[a-z0-9]+)?$')
        texts = [row[0] for row in self.store.db.execute('SELECT text FROM messages ORDER BY id')]
        self.assertEqual(texts, ['Read the attached file.'] * 7)
        self.assertEqual(self.api.downloads, [])

    async def test_voice_transcript_is_saved_and_sent_with_file_path(self):
        self.api.files['voice-2'] = b'audio bytes'
        self.store.accept(update(2, '', voice={'file_id': 'voice-2', 'mime_type': 'audio/ogg'}))
        async def fake_engine(path, state=None):
            self.assertEqual(Path(path).read_bytes(), b'audio bytes')
            return 'Turn left at the next street.'
        with patch.object(media, 'transcribe_audio', side_effect=fake_engine):
            await self.feed()
        path = Path(self.store.db.execute('SELECT path FROM attachments').fetchone()[0])
        self.assertEqual((path.parent / 'transcript.txt').read_text(), 'Turn left at the next street.')
        self.assertEqual((path.parent / 'transcript.txt').stat().st_mode & 0o777, 0o600)
        self.assertIn(str(path), self.session.sent[0][1])
        self.assertIn('Voice note transcript: Turn left at the next street.', self.session.sent[0][1])
        message = dict(self.store.db.execute('SELECT * FROM messages').fetchone())
        rows = await prepare_attachments(self.store, self.api, message)
        with patch.object(media, 'transcribe_audio', side_effect=AssertionError('repeated transcription')):
            await media.prepare_transcripts(self.store, message, rows)
        self.assertEqual(rows[0]['transcript'], 'Turn left at the next street.')

    async def test_unsupported_mac_voice_refuses_with_owner_notice(self):
        from coordinator.transcription import UnsupportedVoice
        self.api.files['voice-2'] = b'audio bytes'
        self.store.accept(update(2, '', voice={'file_id': 'voice-2', 'mime_type': 'audio/ogg'}))
        with patch.object(media, 'transcribe_audio', side_effect=UnsupportedVoice(
                'Voice notes require macOS 14 or newer on Apple silicon.')):
            await self.feed()
        self.assertEqual(self.session.sent, [])
        self.assertTrue(any('Voice notes require macOS 14' in text for text in self.reports()))

    async def test_audio_notes_run_independently_of_other_messages(self):
        self.api.files = {'voice-2': b'first', 'voice-3': b'second'}
        gate = asyncio.Event()
        async def fake_engine(path, state=None):
            if Path(path).read_bytes() == b'first':
                await gate.wait()
                return 'First transcript'
            return 'Second transcript'
        self.store.accept(update(2, '', voice={'file_id': 'voice-2', 'mime_type': 'audio/ogg'}))
        self.store.accept(update(3, '', video_note={'file_id': 'voice-3', 'mime_type': 'video/mp4'}))
        self.store.accept(update(4, 'Text after notes'))
        with patch.object(media, 'transcribe_audio', side_effect=fake_engine):
            await self.service.feed_once()
            await asyncio.sleep(0)
            self.assertTrue(any('Text after notes' in sent[1] for sent in self.session.sent))
            gate.set()
            await asyncio.gather(*self.service.feed_tasks.values())
        prompts = [sent[1] for sent in self.session.sent]
        self.assertTrue(any('First transcript' in prompt for prompt in prompts))
        self.assertTrue(any('Second transcript' in prompt for prompt in prompts))

    async def test_audio_file_in_another_channel_is_transcribed(self):
        with self.store.db:
            self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                                  ('-10042:5', -10042, 5, 'Other', str(self.root)))
        self.api.files['song-2'] = b'audio bytes'
        self.store.accept(update(2, '', thread=5, audio={'file_id': 'song-2', 'mime_type': 'audio/mpeg'}))
        async def fake_engine(path, state=None):
            return 'Other channel transcript'
        with patch.object(media, 'transcribe_audio', side_effect=fake_engine):
            await self.feed()
        self.assertIn('Voice note transcript: Other channel transcript', self.session.sent[0][1])

    async def test_transcription_timeout_has_short_failure_note(self):
        self.api.files['voice-2'] = b'audio bytes'
        self.store.accept(update(2, '', voice={'file_id': 'voice-2', 'mime_type': 'audio/ogg'}))
        async def fake_engine(path, state=None):
            raise asyncio.TimeoutError()
        with patch.object(media, 'transcribe_audio', side_effect=fake_engine), \
                self.assertLogs('coordinator.problems', 'WARNING'):
            await self.feed()
        self.assertIn('Transcription failed', self.session.sent[0][1])
        self.assertEqual(self.problems()[0][:3],
                         ('attachment', 'transcription-timed-out', 'class=TimeoutError'))

    async def test_failed_transcription_sends_file_and_records_private_safe_problem(self):
        self.api.files['voice-2'] = b'audio bytes'
        self.store.accept(update(2, '', voice={'file_id': 'voice-2', 'mime_type': 'audio/ogg'}))
        async def fake_engine(path, state=None):
            raise RuntimeError('private spoken words')
        with patch.object(media, 'transcribe_audio', side_effect=fake_engine), \
                self.assertLogs('coordinator.problems', 'WARNING') as logs:
            await self.feed()
        prompt = self.session.sent[0][1]
        self.assertIn('Transcription failed', prompt)
        self.assertIn(self.store.db.execute('SELECT path FROM attachments').fetchone()[0], prompt)
        self.assertNotIn('private spoken words', prompt + json.dumps(self.problems()) + '\n'.join(logs.output))
        self.assertEqual(self.problems()[0][:3], ('attachment', 'transcription-failed', 'class=RuntimeError'))
        message = dict(self.store.db.execute('SELECT * FROM messages').fetchone())
        rows = await prepare_attachments(self.store, self.api, message)
        with patch.object(media, 'transcribe_audio', side_effect=AssertionError('repeated transcription')):
            await media.prepare_transcripts(self.store, message, rows)
        self.assertEqual(len(self.problems()), 1)

    def test_real_transcription_uses_no_live_media(self):
        source = inspect.getsource(self.test_real_transcription_command)
        self.assertNotIn('telegram-agent-coordinator' + '/files/', source)
        self.assertIn('TORII_TEST_REAL_TRANSCRIPTION', source)

    @unittest.skipUnless(os.environ.get('TORII_TEST_REAL_TRANSCRIPTION') == '1' and
                         shutil.which('say') and os.uname().sysname == 'Darwin',
                         'set TORII_TEST_REAL_TRANSCRIPTION=1 with say on macOS')
    async def test_real_transcription_command(self):
        path = self.root / 'synthetic.aiff'
        process = await asyncio.create_subprocess_exec('say', '-o', str(path),
            'The quick brown fox jumps over the lazy dog near the quiet river.')
        self.assertEqual(await process.wait(), 0)
        self.assertLess(path.stat().st_size, 1000000)
        transcript = await media.transcribe_audio(path, self.store.directory)
        self.assertGreater(len(transcript.split()), 5)

    async def test_image_document_keeps_the_image_path(self):
        self.store.accept(document(2, 'shot.png', 'image/png', caption='Look'))
        await self.feed()
        path = Path(self.store.db.execute('SELECT path FROM attachments').fetchone()['path'])
        self.assertEqual(path.parent.parent, self.store.directory / 'images')
        self.assertEqual(path.suffix, '.png')
        self.assertIn('Owner-provided images for this message:', self.session.sent[0][1])
        self.assertNotIn('Owner-provided files', self.session.sent[0][1])
        self.assertFalse((self.store.directory / 'files').exists())

    async def test_file_over_the_cap_is_not_downloaded_and_the_text_still_arrives(self):
        self.store.accept(document(2, 'huge.zip', 'application/zip', caption='Check the logs',
                                   item={'file_size': MAX_FILE_BYTES + 1}))
        await self.feed()
        self.assertEqual(self.api.downloads, [])
        self.assertEqual(self.reports(), [
            '`huge.zip` is larger than 20 MB, the most Telegram lets a bot download. Your message went to '
            'the coordinator without the file. Send a smaller file or a link.'])
        prompt = self.session.sent[0][1]
        self.assertIn('Check the logs', prompt)
        self.assertIn('could not download (the owner was told):\nhuge.zip (application/zip): larger than 20 MB',
                      prompt)
        self.assertNotIn('Owner-provided files', prompt)
        self.assertEqual(self.store.db.execute('SELECT delivered FROM messages').fetchone()[0], 'received')
        self.assertFalse((self.store.directory / 'files').exists())
        await prepare_attachments(self.store, self.api, dict(self.store.db.execute('SELECT * FROM messages').fetchone()))
        self.assertEqual(len(self.reports()), 1)

    async def test_file_download_failure_replies_once_without_secret(self):
        self.api.image_failure = True
        self.store.accept(document(2, 'notes.md'))
        await self.feed()
        self.assertEqual(self.reports(), [
            '`notes.md` could not be downloaded. Telegram lets a bot download files up to 20 MB. Your message went '
            'to the coordinator without the file. Send the file again, or a link if it is larger.'])
        prompt = self.session.sent[0][1]
        self.assertIn('Read the attached file.', prompt)
        self.assertIn('notes.md (text/markdown): download failed', prompt)
        self.assertNotIn('private-transport-value', prompt + self.reports()[0])
        await prepare_attachments(self.store, self.api, dict(self.store.db.execute('SELECT * FROM messages').fetchone()))
        self.assertEqual(self.api.downloads, ['doc-2'])
        self.assertEqual(len(self.reports()), 1)

    async def test_document_album_messages_stay_separate_and_in_order(self):
        self.api.files = {'doc-2': b'first', 'doc-3': b'second'}
        self.store.accept(document(2, 'a.md', caption='Compare these', media_group_id='album'))
        self.store.accept(document(3, 'a.md', media_group_id='album'))
        await self.service.feed_once()
        for task in list(self.service.feed_tasks.values()):
            await task
        self.assertEqual(self.api.downloads, ['doc-2', 'doc-3'])
        self.assertEqual(len(self.session.sent), 2)
        self.assertIn('Compare these', self.session.sent[0][1])
        self.assertIn('Read the attached file.', self.session.sent[1][1])
        paths = [Path(row[0]) for row in self.store.db.execute('SELECT path FROM attachments ORDER BY message')]
        self.assertEqual([path.read_bytes() for path in paths], [b'first', b'second'])
        self.assertNotEqual(paths[0].parent, paths[1].parent)
        self.assertIn(str(paths[1]), self.session.sent[1][1])

    def test_file_names_are_safe_basenames_that_keep_the_extension(self):
        cases = {'../../etc/passwd': 'passwd', '..\\evil\\run.sh': 'run.sh', '.env': 'env',
                 'my report (final).PDF': 'my_report_final.PDF', '': 'file.bin', None: 'file.bin',
                 '\u8cc7\u6599.md': '\u8cc7\u6599.md', 'a/b/.hidden.txt': 'hidden.txt', '...': 'file.bin',
                 'x' * 300 + '.md': 'x' * 120 + '.md', 'name.': 'name', 'tar.ball.tar.gz': 'tar.ball.tar.gz'}
        for raw, expected in cases.items():
            self.assertEqual(safe_name(raw, 'application/octet-stream'), expected, raw)
        self.assertEqual(safe_name(None, 'application/pdf'), 'file.pdf')
        directory = self.root / 'collide'
        directory.mkdir()
        (directory / 'notes.md').write_bytes(b'one')
        (directory / 'notes-2.md').write_bytes(b'two')
        self.assertEqual(free_path(directory, 'notes.md'), directory / 'notes-3.md')
        self.assertEqual(free_path(directory, 'README'), directory / 'README')
