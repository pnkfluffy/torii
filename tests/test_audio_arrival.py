import asyncio
from pathlib import Path
import unittest
from unittest.mock import patch

from coordinator import media
from coordinator.service import Service
from coordinator.store import Store
from tests.test_media import ImageTelegram
from tests.test_service import FakeSession
from tests.test_store import update


class AudioArrivalTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        import tempfile
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.store = Store(self.root / 'state')
        with self.store.db:
            self.store.put('owner', 7)
            self.store.put('group', -10042)
            self.store.put('coordinator_home_topic', '-10042:4')
            for thread in (4, 5):
                self.store.db.execute('INSERT INTO topics(id,chat,thread,name,cwd,enabled) VALUES (?,?,?,?,?,1)',
                                      ('-10042:' + str(thread), -10042, thread, 'Chat', str(self.root)))
        self.api = ImageTelegram()
        self.started = asyncio.Queue()
        self.gates = {}
        self.calls = []
        self.active = 0
        self.peak = 0
        async def engine(path, state, **kwargs):
            number = int(Path(path).read_text())
            self.calls.append(number)
            self.active += 1
            self.peak = max(self.peak, self.active)
            self.started.put_nowait(number)
            try:
                await self.gates[number].wait()
                return 'Transcript %d' % number
            finally:
                self.active -= 1
        self.backend = patch.object(media, 'default_backend', return_value='parakeet', create=True)
        self.backend.start()
        self.addCleanup(self.backend.stop)
        self.engine = patch.object(media, 'transcribe_audio', side_effect=engine)
        self.engine.start()
        self.addCleanup(self.engine.stop)
        self.service = self.make_service()

    def make_service(self):
        async def factory(store, runner, cwd, model, instructions, topic=None):
            return FakeSession('fixture-' + topic)
        return Service(self.store, self.api, object(), self.root, session_factory=factory)

    async def stop(self):
        tasks = list(self.service.feed_tasks.values()) + list(getattr(self.service, 'audio_tasks', {}).values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for session in self.service.sessions.values():
            await session.stop()

    async def asyncTearDown(self):
        await self.stop()
        self.store.close()
        self.temporary.cleanup()

    async def accept(self, number, thread=4, kind='voice', forwarded=False, **metadata):
        self.gates[number] = asyncio.Event()
        self.api.files['note-%d' % number] = str(number).encode()
        item = {'file_id': 'note-%d' % number, 'mime_type': 'audio/ogg', 'duration': 60}
        item.update(metadata)
        request = update(number, '', thread=thread, **{kind: item})
        if forwarded:
            request['message']['forward_origin'] = {
                'type': 'user', 'sender_user': {'id': 88, 'first_name': 'Fixture'}}
        self.assertEqual(await self.service.accept_update(request), 'queued')
        return request

    async def next_started(self):
        return await asyncio.wait_for(self.started.get(), 2)

    async def drain(self):
        await self.service.feed_once()
        await asyncio.wait_for(asyncio.gather(*self.service.feed_tasks.values()), 2)
        await self.service.feed_once()

    async def test_accept_starts_transcription_before_feed_and_deduplicates(self):
        request = await self.accept(2)
        self.assertEqual(await self.next_started(), 2)
        self.assertEqual(self.service.sessions, {})
        self.assertEqual(await self.service.accept_update(request), 'duplicate')
        self.gates[2].set()
        await self.drain()
        self.assertEqual(self.api.downloads, ['note-2'])
        self.assertIn('Transcript 2', self.service.sessions['-10042:4'].sent[0][1])

    async def test_two_chats_start_together_and_slow_chat_does_not_block_delivery(self):
        await self.accept(2)
        await self.accept(3, thread=5)
        self.assertEqual({await self.next_started(), await self.next_started()}, {2, 3})
        self.assertEqual(await self.service.accept_update(update(4, 'Other chat text', thread=5)), 'queued')
        self.gates[3].set()
        await self.service.feed_once()
        tasks = [task for key, task in self.service.feed_tasks.items()
                 if self.store.db.execute('SELECT topic FROM messages WHERE id=?', (key,)).fetchone()[0] == '-10042:5']
        await asyncio.wait_for(asyncio.gather(*tasks), 2)
        self.assertEqual(self.service.sessions['-10042:4'].sent, [])
        prompts = [entry[1] for entry in self.service.sessions['-10042:5'].sent]
        self.assertIn('Transcript 3', prompts[0])
        self.assertIn('Other chat text', prompts[1])
        self.gates[2].set()
        await self.drain()

    async def test_five_notes_share_four_slots_and_fifth_starts_when_one_finishes(self):
        for number in range(2, 7):
            await self.accept(number, thread=4 if number % 2 else 5)
        first = {await self.next_started() for _ in range(4)}
        await asyncio.sleep(0)
        self.assertEqual(len(first), 4)
        self.assertEqual(self.active, 4)
        self.assertTrue(self.started.empty())
        self.gates[min(first)].set()
        last = await self.next_started()
        self.assertEqual(first | {last}, {2, 3, 4, 5, 6})
        for gate in self.gates.values():
            gate.set()
        await self.drain()
        self.assertEqual(self.peak, 4)
        self.assertEqual(sum(len(session.sent) for session in self.service.sessions.values()), 5)

    async def test_long_then_short_then_text_deliver_in_send_order(self):
        await self.accept(2)
        self.assertEqual(await self.next_started(), 2)
        await self.service.feed_once()
        await self.accept(3)
        self.assertEqual(await self.next_started(), 3)
        self.gates[3].set()
        await self.service.accept_update(update(4, 'Text after notes'))
        await self.service.feed_once()
        await asyncio.sleep(0)
        self.assertEqual(self.service.sessions['-10042:4'].sent, [])
        self.gates[2].set()
        await self.drain()
        prompts = [entry[1] for entry in self.service.sessions['-10042:4'].sent]
        self.assertIn('Transcript 2', prompts[0])
        self.assertIn('Transcript 3', prompts[1])
        self.assertIn('Text after notes', prompts[2])

    async def test_restart_resumes_interrupted_note_once_and_reuses_finished_transcript(self):
        await self.accept(2)
        self.gates[2].set()
        await self.accept(3)
        self.assertEqual({await self.next_started(), await self.next_started()}, {2, 3})
        saved = self.store.db.execute('SELECT path FROM attachments WHERE message=2').fetchone()[0]
        self.assertEqual((Path(saved).parent / 'transcript.txt').read_text(), 'Transcript 2')
        await self.stop()
        self.store.close()
        self.store = Store(self.root / 'state')
        self.service = self.make_service()
        self.service.resume_audio()
        self.assertEqual(await self.next_started(), 3)
        self.gates[3].set()
        await self.drain()
        self.assertEqual(self.api.downloads, ['note-2', 'note-3'])
        self.assertTrue(self.started.empty())
        prompts = [entry[1] for entry in self.service.sessions['-10042:4'].sent]
        self.assertIn('Transcript 2', prompts[0])
        self.assertIn('Transcript 3', prompts[1])

    async def test_audio_documents_by_mime_and_each_extension_and_forwarded_notes(self):
        variants = [('recording.bin', 'audio/x-custom'), ('recording.MP3', 'image/png')] + [
            ('recording.' + extension.upper(), 'application/octet-stream')
            for extension in ('oga', 'ogg', 'opus', 'mp3', 'm4a', 'aac', 'wav', 'flac')]
        for number, (name, mime) in enumerate(variants, 2):
            await self.accept(number, kind='document', file_name=name, mime_type=mime,
                              file_size=11 * 1024 * 1024)
            self.assertEqual(await self.next_started(), number)
            self.gates[number].set()
        await self.accept(20, forwarded=True)
        self.assertEqual(await self.next_started(), 20)
        self.gates[20].set()
        self.api.files['plain'] = b'plain document'
        await self.service.accept_update(update(21, '', document={
            'file_id': 'plain', 'file_name': 'plain.txt', 'mime_type': 'text/plain'}))
        await self.drain()
        prompts = [entry[1] for entry in self.service.sessions['-10042:4'].sent]
        self.assertEqual(len(prompts), len(variants) + 2)
        for number, prompt in enumerate(prompts[:len(variants)], 2):
            self.assertIn('Voice note transcript: Transcript %d' % number, prompt)
        self.assertIn('Forwarded', prompts[-2])
        self.assertIn('Transcript 20', prompts[-2])
        self.assertNotIn('Voice note transcript:', prompts[-1])
        self.assertTrue(self.started.empty())

    async def test_multiple_notes_in_one_preparation_start_together(self):
        for number in (2, 3):
            self.gates[number] = asyncio.Event()
            self.api.files['note-%d' % number] = str(number).encode()
            self.store.accept(update(number, '', voice={'file_id': 'note-%d' % number, 'duration': 60}))
        messages = self.store.messages_pending()
        rows = []
        for message in messages:
            rows.extend(await media.prepare_attachments(self.store, self.api, message))
        task = asyncio.create_task(media.prepare_transcripts(self.store, messages[0], rows))
        try:
            self.assertEqual({await self.next_started(), await self.next_started()}, {2, 3})
        finally:
            for gate in self.gates.values():
                gate.set()
            await task
        self.assertEqual([row['transcript'] for row in rows], ['Transcript 2', 'Transcript 3'])

    async def test_default_parakeet_and_hidden_setting_switches_the_next_note(self):
        from unittest.mock import AsyncMock
        self.assertIsNone(self.store.get('transcription_model'))
        with self.assertRaisesRegex(ValueError, 'parakeet or whisper'):
            self.store.put('transcription_model', 'unknown')
        engine = AsyncMock(return_value='Selected transcript')
        with patch.object(media, 'transcribe_audio', engine):
            await self.accept(2)
            await self.drain()
            self.assertEqual(engine.call_args.kwargs['backend'], 'parakeet')
            with self.store.db:
                self.store.put('transcription_model', 'whisper')
            await self.accept(3)
            await self.drain()
            self.assertEqual(engine.call_args.kwargs['backend'], 'whisper')
        self.assertEqual(self.store.get('transcription_model'), 'whisper')
        self.store.close()
        self.store = Store(self.root / 'state')
        self.assertEqual(self.store.get('transcription_model'), 'whisper')

    async def test_three_short_notes_start_together_with_memory_budget(self):
        await self.accept(2, duration=1800)
        self.assertEqual(await self.next_started(), 2)
        for number in (3, 4, 5):
            await self.accept(number, duration=60)
        self.assertEqual(await self.next_started(), 3)
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(self.started.get(), 0.1)
        self.assertEqual(self.active, 2)
        self.gates[2].set()
        self.assertEqual({await self.next_started(), await self.next_started()}, {4, 5})
        self.assertEqual(self.active, 3)
        for gate in self.gates.values():
            gate.set()
        await self.drain()

    async def test_only_one_thirty_minute_note_and_one_short_note_fit_together(self):
        await self.accept(2, duration=1800)
        await self.accept(3, duration=1800)
        await self.accept(4, duration=60)
        self.assertEqual(await self.next_started(), 2)
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(self.started.get(), 0.1)
        self.gates[2].set()
        self.assertEqual(await self.next_started(), 3)
        self.assertEqual(await self.next_started(), 4)
        self.assertEqual(self.peak, 2)
        for gate in self.gates.values():
            gate.set()
        await self.drain()

    async def test_over_budget_note_runs_alone_and_releases_its_reservation(self):
        with patch.object(media, 'transcription_memory', side_effect=lambda row, backend: 13 if row['duration'] == 1800 else 3):
            await self.accept(2, duration=1800)
            self.assertEqual(await self.next_started(), 2)
            await self.accept(3, duration=60)
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(self.started.get(), 0.1)
            self.gates[2].set()
            self.assertEqual(await self.next_started(), 3)
            self.assertEqual(self.peak, 1)
            self.gates[3].set()
            await self.drain()

    async def test_whisper_memory_estimate_allows_two_long_notes(self):
        with self.store.db:
            self.store.put('transcription_model', 'whisper')
        for number in (2, 3, 4):
            await self.accept(number, duration=1800)
        self.assertEqual({await self.next_started(), await self.next_started()}, {2, 3})
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(self.started.get(), 0.1)
        self.gates[2].set()
        self.assertEqual(await self.next_started(), 4)
        for gate in self.gates.values():
            gate.set()
        await self.drain()

    async def test_long_note_starts_before_later_short_notes_can_starve_it(self):
        await self.accept(2, duration=60)
        await self.accept(3, duration=60)
        self.assertEqual({await self.next_started(), await self.next_started()}, {2, 3})
        await self.accept(4, duration=1800, thread=5)
        await asyncio.sleep(0)
        await self.accept(5, duration=60)
        self.gates[2].set()
        self.assertEqual(await self.next_started(), 4)
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(self.started.get(), 0.1)
        self.gates[4].set()
        self.assertEqual(await self.next_started(), 5)
        for gate in self.gates.values():
            gate.set()
        await self.drain()

    async def test_cancelling_waiting_long_note_unblocks_later_short_note(self):
        await self.accept(2, duration=1800)
        self.assertEqual(await self.next_started(), 2)
        await self.accept(3, duration=1800)
        await asyncio.sleep(0)
        await self.accept(4, duration=60)
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(self.started.get(), 0.1)
        row = self.store.db.execute('SELECT id FROM messages WHERE telegram_message=3').fetchone()
        waiting = self.service.audio_tasks[row['id']]
        waiting.cancel()
        await asyncio.gather(waiting, return_exceptions=True)
        self.assertEqual(await self.next_started(), 4)
        for gate in self.gates.values():
            gate.set()
        await asyncio.gather(*self.service.audio_tasks.values(), return_exceptions=True)
        self.assertEqual(self.service.audio_slots.used, 0)

    async def test_cancelled_reservation_releases_memory_for_waiting_note(self):
        await self.accept(2, duration=1800)
        self.assertEqual(await self.next_started(), 2)
        await self.accept(3, duration=1800)
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(self.started.get(), 0.1)
        first = min(self.service.audio_tasks)
        self.service.audio_tasks[first].cancel()
        await asyncio.gather(self.service.audio_tasks[first], return_exceptions=True)
        self.assertEqual(await self.next_started(), 3)
        self.gates[3].set()
        await asyncio.gather(*self.service.audio_tasks.values(), return_exceptions=True)

    async def assert_send_failure_recovers(self, chained_note):
        await self.accept(2)
        failed = self.store.db.execute('SELECT id FROM messages WHERE telegram_message=2').fetchone()[0]
        self.gates[2].set()
        if chained_note:
            await self.accept(3)
            self.assertEqual({await self.next_started(), await self.next_started()}, {2, 3})
        else:
            for number in range(3, 8):
                await self.service.accept_update(update(number, 'Follow-up %d' % number))
        original_send = FakeSession.send
        delivered = asyncio.Event()
        feed_starts = asyncio.Queue()
        async def send(session, message_id, text, row_id=None):
            if row_id == failed:
                raise RuntimeError('fixture send failure')
            result = await original_send(session, message_id, text, row_id)
            if len(session.sent) == (1 if chained_note else 5):
                delivered.set()
            return result
        async def feed():
            feed_starts.put_nowait(None)
            await self.service.feed()
        self.service.task_restart_backoff = 0.01
        with patch.object(FakeSession, 'send', send):
            loop = asyncio.create_task(self.service.supervise('feed', feed))
            try:
                await asyncio.wait_for(feed_starts.get(), 5)
                await asyncio.wait_for(feed_starts.get(), 5)
                if chained_note:
                    self.gates[3].set()
                await asyncio.wait_for(delivered.wait(), 5)
                if chained_note:
                    self.assertEqual(self.calls.count(3), 1)
                self.assertFalse(loop.done(), str(loop.exception()) if loop.done() else '')
                crashes = self.store.db.execute(
                    "SELECT count(*) FROM problems WHERE area='service' AND code='loop-crashed'").fetchone()[0]
                self.assertEqual(crashes, 1)
                prompts = [entry[1] for entry in self.service.sessions['-10042:4'].sent]
                if chained_note:
                    self.assertEqual(len(prompts), 1)
                    self.assertIn('Transcript 3', prompts[0])
                    self.assertEqual(self.api.downloads, ['note-2', 'note-3'])
                else:
                    self.assertEqual(len(prompts), 5)
                    for number, prompt in enumerate(prompts, 3):
                        self.assertIn('Follow-up %d' % number, prompt)
            finally:
                loop.cancel()
                await asyncio.gather(loop, return_exceptions=True)

    async def test_one_failed_audio_delivery_does_not_stop_feed_with_five_queued_texts(self):
        await self.assert_send_failure_recovers(chained_note=False)

    async def test_one_failed_audio_delivery_does_not_repeat_a_chained_transcription(self):
        await self.assert_send_failure_recovers(chained_note=True)
