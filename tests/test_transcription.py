import asyncio
import fcntl
import hashlib
import io
import os
import shlex
import signal
import subprocess
import sys
import time
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, patch

from coordinator import media, transcribe, transcription
from tests.support import SETTLE, _group_alive, _stop_group


class SetupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.mac_version = patch.object(transcription.platform, 'mac_ver', return_value=('14.0', ('', '', ''), 'arm64'))
        self.mac_version.start()

    def tearDown(self):
        self.mac_version.stop()
        self.temp.cleanup()

    def test_voice_refuses_unsupported_macs_before_install(self):
        for machine, version in (('x86_64', '14.0'), ('arm64', '13.6')):
            with patch.object(transcription.platform, 'system', return_value='Darwin'), \
                    patch.object(transcription.platform, 'machine', return_value=machine), \
                    patch.object(transcription.platform, 'mac_ver', return_value=(version, ('', '', ''), machine)):
                with self.assertRaisesRegex(transcription.UnsupportedVoice, 'macOS 14'):
                    transcription.prepare_environment(self.root, {'PATH': '/usr/bin:/bin'}, backend='whisper')
        self.assertFalse((self.root / 'transcription').exists())

    def archive(self):
        data = io.BytesIO()
        with tarfile.open(fileobj=data, mode='w:gz') as bundle:
            member = tarfile.TarInfo('uv-test/uv')
            member.size = 7
            bundle.addfile(member, io.BytesIO(b'fake-uv'))
        return data.getvalue()

    def test_missing_uv_is_verified_and_saved_without_extracting_paths(self):
        archive = self.archive()
        digest = hashlib.sha256(archive).hexdigest()
        with patch.object(transcription.shutil, 'which', return_value=None), \
                patch.object(transcription.platform, 'system', return_value='Darwin'), \
                patch.object(transcription.platform, 'machine', return_value='arm64'), \
                patch.dict(transcription.UV_ARCHIVES, {('Darwin', 'arm64'): ('test', digest)}), \
                patch.object(transcription.urllib.request, 'urlopen', return_value=io.BytesIO(archive)) as fetch:
            binary = Path(transcription.uv_binary(self.root, {'PATH': '/usr/bin:/bin'}))
            self.assertEqual(binary.read_bytes(), b'fake-uv')
            self.assertEqual(binary.stat().st_mode & 0o777, 0o700)
            self.assertEqual(transcription.uv_binary(self.root, {'PATH': '/usr/bin:/bin'}), str(binary))
            fetch.assert_called_once()
            self.assertIn('/0.11.18/uv-test.tar.gz', fetch.call_args.args[0])

    def test_bad_uv_hash_never_creates_an_executable(self):
        with patch.object(transcription.shutil, 'which', return_value=None), \
                patch.object(transcription.platform, 'system', return_value='Darwin'), \
                patch.object(transcription.platform, 'machine', return_value='arm64'), \
                patch.object(transcription.urllib.request, 'urlopen', return_value=io.BytesIO(self.archive())):
            with self.assertRaisesRegex(RuntimeError, 'checksum'):
                transcription.uv_binary(self.root, {'PATH': '/usr/bin:/bin'})
        self.assertFalse((self.root / 'uv').exists())

    def test_setup_syncs_hashes_and_model_once_with_a_separate_long_deadline(self):
        for system, machine, lock in (('Darwin', 'arm64', 'macos'), ('Linux', 'x86_64', 'linux')):
            state = self.root / system
            python = state / 'transcription/venv/bin/python'
            def run(command, **kwargs):
                if 'venv' in command:
                    python.parent.mkdir(parents=True)
                    python.write_text('fake-python')
            with self.subTest(system=system), \
                    patch.object(transcription.platform, 'system', return_value=system), \
                    patch.object(transcription.platform, 'machine', return_value=machine), \
                    patch.object(transcription, 'uv_binary', return_value='/fake/uv'), \
                    patch.object(transcription.subprocess, 'run', side_effect=run) as launch:
                self.assertEqual(transcription.prepare_environment(state, {'PATH': '/usr/bin:/bin'}, backend='whisper'), str(python))
                self.assertEqual(launch.call_count, 3)
                sync = launch.call_args_list[1].args[0]
                self.assertIn('--require-hashes', sync)
                self.assertIn('--only-binary', sync)
                self.assertTrue(sync[-1].endswith('transcription-' + lock + '.lock'))
                self.assertEqual(launch.call_args_list[2].args[0][-1], '--setup')
                for call in launch.call_args_list:
                    self.assertEqual(call.kwargs['timeout'], 3600)
                launch.reset_mock()
                transcription.prepare_environment(state, {'PATH': '/usr/bin:/bin'}, backend='whisper')
                launch.assert_called_once()
                self.assertEqual(launch.call_args.args[0], [str(python), str(Path(transcribe.__file__)), '--backend', 'whisper', '--check'])

    def test_failed_setup_is_retried_without_a_ready_marker(self):
        with patch.object(transcription.platform, 'system', return_value='Darwin'), \
                patch.object(transcription.platform, 'machine', return_value='arm64'), \
                patch.object(transcription, 'uv_binary', return_value='/fake/uv'), \
                patch.object(transcription.subprocess, 'run', side_effect=RuntimeError('fake failure')):
            with self.assertRaises(RuntimeError):
                transcription.prepare_environment(self.root, {'PATH': '/usr/bin:/bin'}, backend='whisper')
        self.assertFalse((self.root / 'transcription/ready').exists())

    def test_successful_rebuild_removes_the_old_environment_and_ready_marker(self):
        directory = self.root / 'transcription'
        python = directory / 'venv/bin/python'
        python.parent.mkdir(parents=True)
        python.write_text('old-python')
        (python.parent / 'old-package').write_text('old-package')
        (directory / 'ready').write_text('old-revision')
        def run(command, **kwargs):
            if 'venv' in command:
                self.assertFalse(python.exists())
                python.parent.mkdir(parents=True)
                python.write_text('new-python')
        with patch.object(transcription, 'uv_binary', return_value='/fake/uv'), \
                patch.object(transcription.subprocess, 'run', side_effect=run):
            self.assertEqual(transcription.prepare_environment(self.root, {'PATH': '/usr/bin:/bin'}, backend='whisper'), str(python))
        self.assertEqual(python.read_text(), 'new-python')
        self.assertNotEqual((directory / 'ready').read_text(), 'old-revision')
        self.assertEqual(list(directory.glob('incomplete-*')), [])

    def cancel_setup(self):
        child = """
import os
from pathlib import Path
import signal
Path('child.pgid').write_text(str(os.getpgrp()))
Path('child.pid').write_text(str(os.getpid()))
signal.pause()
"""
        uv = self.root / 'uv'
        uv.write_text("#!/bin/sh\nmkdir -p venv/bin\nprintf partial > venv/bin/python\n" +
                      shlex.quote(sys.executable) + ' -c ' + shlex.quote(child) + " &\nwait\n")
        uv.chmod(0o700)
        runner = """
import asyncio
import os
import sys
from pathlib import Path
from unittest.mock import patch
from coordinator import media, transcription
from tests.support import _group_alive, settle, until, until_text
state = Path(sys.argv[1])
async def serve():
    cancelled = asyncio.Event()
    release = asyncio.Event()
    communicate = asyncio.subprocess.Process.communicate
    async def hold_cancellation(process, *args, **kwargs):
        try:
            return await communicate(process, *args, **kwargs)
        except asyncio.CancelledError:
            cancelled.set()
            await release.wait()
            raise
    with patch.object(asyncio.subprocess.Process, 'communicate', hold_cancellation):
        task = asyncio.create_task(media.transcribe_audio(state / 'voice.oga', state, backend='whisper'))
        try:
            child = int(await until_text(state / 'transcription/child.pid'))
            group = int((state / 'transcription/child.pgid').read_text())
            assert os.getpgid(child) == group
            assert not task.done(), 'setup finished before cancellation'
            task.cancel()
            await settle(cancelled.wait())
            assert not task.done(), 'setup bypassed the cancellation gate'
            assert _group_alive(group), 'setup group exited before the cancellation gate opened'
            release.set()
            try:
                await settle(task)
            except asyncio.CancelledError:
                pass
            else:
                raise AssertionError('setup did not propagate cancellation')
            await until(lambda: not _group_alive(group), 'the setup process group to exit')
        finally:
            release.set()
            if not task.done():
                task.cancel()
            await settle(asyncio.gather(task, return_exceptions=True))
with patch.object(transcription, 'environment', return_value={'PATH': str(state) + ':/usr/bin:/bin'}):
    asyncio.run(serve())
print('loop closed')
"""
        group_file = self.root / 'transcription/child.pgid'
        try:
            result = subprocess.run([sys.executable, '-c', runner, str(self.root)],
                                    capture_output=True, text=True, timeout=3 * SETTLE)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), 'loop closed')
            self.assertFalse(_group_alive(int(group_file.read_text())), 'setup group survived shutdown')
            self.assertFalse((self.root / 'transcription/ready').exists())
        finally:
            if group_file.exists() and group_file.read_text().strip():
                _stop_group(int(group_file.read_text()), signal.SIGKILL)

    def test_shutdown_during_setup_closes_loop_and_stops_process_group(self):
        self.cancel_setup()

    def test_shutdown_while_waiting_for_setup_lock_closes_loop(self):
        directory = self.root / 'transcription'
        directory.mkdir()
        uv = self.root / 'uv'
        uv.write_text('#!/bin/sh\nexit 9\n')
        uv.chmod(0o700)
        async def cancel():
            task = asyncio.create_task(transcription.setup(self.root, backend='whisper'))
            await asyncio.sleep(0.2)
            started = time.monotonic()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            return started
        with (directory / 'setup.lock').open('a') as guard, \
                patch.object(transcription, 'environment',
                             return_value={'PATH': str(self.root) + ':/usr/bin:/bin'}):
            fcntl.flock(guard, fcntl.LOCK_EX)
            started = asyncio.run(cancel())
        self.assertLess(time.monotonic() - started, 1)
        self.assertFalse((directory / 'venv').exists())

    def test_failed_or_killed_setup_rebuilds_partial_environment(self):
        for failure in ('failed', 'killed'):
            state = self.root / failure
            python = state / 'transcription/venv/bin/python'
            partial = python.parent / 'partial-package'
            def run(command, **kwargs):
                if 'venv' in command:
                    self.assertFalse(partial.exists())
                    python.parent.mkdir(parents=True)
                    python.write_text('fake-python')
            with self.subTest(failure=failure), \
                    patch.object(transcription, 'uv_binary', return_value='/fake/uv'):
                if failure == 'failed':
                    def fail(command, **kwargs):
                        run(command, **kwargs)
                        if 'sync' in command:
                            partial.write_text('incomplete')
                            raise RuntimeError('sync failed')
                    with patch.object(transcription.subprocess, 'run', side_effect=fail):
                        with self.assertRaises(RuntimeError):
                            transcription.prepare_environment(state, {'PATH': '/usr/bin:/bin'}, backend='whisper')
                else:
                    python.parent.mkdir(parents=True)
                    python.write_text('interrupted-python')
                    partial.write_text('incomplete')
                self.assertFalse((state / 'transcription/ready').exists())
                with patch.object(transcription.subprocess, 'run', side_effect=run) as launch:
                    self.assertEqual(transcription.prepare_environment(state, {'PATH': '/usr/bin:/bin'}, backend='whisper'),
                                     str(python))
                    self.assertIn('venv', launch.call_args_list[0].args[0])
                    self.assertEqual(launch.call_count, 3)
                self.assertFalse(partial.exists())
                self.assertTrue((state / 'transcription/ready').is_file())
                self.assertEqual(list((state / 'transcription').glob('incomplete-*')), [])

    def test_cancelled_setup_retries_with_a_fresh_environment(self):
        self.cancel_setup()
        uv = self.root / 'uv'
        uv.write_text("#!/bin/sh\nif [ \"$2\" = venv ]; then\n"
                      "test ! -e venv/bin/python || exit 9\nmkdir -p venv/bin\n"
                      "printf '#!/bin/sh\\nexit 0\\n' > venv/bin/python\n"
                      "chmod 700 venv/bin/python\nfi\n")
        with patch.object(transcription, 'environment',
                          return_value={'PATH': str(self.root) + ':/usr/bin:/bin'}):
            python, _ = asyncio.run(transcription.setup(self.root, backend='whisper'))
        self.assertEqual(python, str(self.root / 'transcription/venv/bin/python'))
        self.assertTrue((self.root / 'transcription/ready').is_file())
        self.assertEqual(list((self.root / 'transcription').glob('incomplete-*')), [])

    def test_environment_omits_ambient_credentials(self):
        with patch.dict(os.environ, {'AWS_SECRET_ACCESS_KEY': 'fake', 'HF_TOKEN': 'fake'}):
            env = transcription.environment()
        self.assertEqual(set(env), {'HOME', 'PATH', 'UV_CACHE_DIR', 'HF_HOME', 'HF_HUB_DISABLE_XET'})
        self.assertEqual(env['HF_HUB_DISABLE_XET'], '1')

    def test_both_models_use_pinned_revisions_and_only_setup_can_download(self):
        hub = Mock()
        directory = self.root / 'snapshot'
        directory.mkdir()
        for name in ('config.json', 'weights.safetensors', 'model.bin', 'tokenizer.json', 'vocabulary.json'):
            (directory / name).write_bytes(b'fake model file')
        hub.snapshot_download.return_value = str(directory)
        with patch.dict('sys.modules', {'huggingface_hub': hub}):
            for system, machine, model in (('Darwin', 'arm64', transcribe.MAC_MODEL),
                                           ('Linux', 'x86_64', transcribe.LINUX_MODEL)):
                with patch.object(transcribe.platform, 'system', return_value=system), \
                        patch.object(transcribe.platform, 'machine', return_value=machine):
                    transcribe.model_path(setup=True, backend='whisper')
                    self.assertEqual(hub.snapshot_download.call_args.kwargs['revision'], model[1])
                    self.assertFalse(hub.snapshot_download.call_args.kwargs['local_files_only'])
                    transcribe.model_path(backend='whisper')
                    self.assertTrue(hub.snapshot_download.call_args.kwargs['local_files_only'])

    def test_both_backends_receive_decoded_samples_instead_of_an_audio_filename(self):
        samples = object()
        mlx = Mock()
        mlx.transcribe.return_value = {'text': 'Mac transcript'}
        faster = Mock()
        faster.WhisperModel.return_value.transcribe.return_value = ([Mock(text='Linux transcript')], None)
        with patch.dict('sys.modules', {'mlx_whisper': mlx, 'faster_whisper': faster}), \
                patch.object(transcribe, 'model_path', return_value='/cached/model'), \
                patch.object(transcribe, 'decode', return_value=samples) as decode, \
                patch.object(transcribe.sys, 'argv', ['transcribe.py', '--backend', 'whisper', '/audio.ogg']), \
                patch('builtins.print') as output:
            with patch.object(transcribe, 'uses_mlx', return_value=True):
                transcribe.main()
            mlx.transcribe.assert_called_once_with(samples, path_or_hf_repo='/cached/model')
            output.assert_called_with('Mac transcript')
            with patch.object(transcribe, 'uses_mlx', return_value=False):
                transcribe.main()
            faster.WhisperModel.return_value.transcribe.assert_called_once_with(samples)
            output.assert_called_with('Linux transcript')
            self.assertEqual(decode.call_count, 2)


class TranscriptionDeadlineTests(unittest.IsolatedAsyncioTestCase):
    async def test_setup_finishes_before_the_note_deadline_starts(self):
        entered = asyncio.Event()
        finish = asyncio.Event()
        async def setup(state, **kwargs):
            entered.set()
            await finish.wait()
            return '/fake/python', {'PATH': '/usr/bin:/bin'}
        process = Mock(returncode=0)
        process.communicate = AsyncMock(return_value=(b'Local transcript', b''))
        with patch.object(media, 'setup_transcription', side_effect=setup), \
                patch.object(media.asyncio, 'create_subprocess_exec', AsyncMock(return_value=process)) as launch, \
                patch.object(media.asyncio, 'wait_for', wraps=asyncio.wait_for) as deadline:
            task = asyncio.create_task(media.transcribe_audio('/fake/audio.ogg', '/fake/state'))
            await entered.wait()
            launch.assert_not_called()
            deadline.assert_not_called()
            finish.set()
            self.assertEqual(await task, 'Local transcript')
            self.assertEqual(deadline.call_args.args[1], 300)
            self.assertEqual(launch.call_args.args[0], '/fake/python')
            self.assertTrue(launch.call_args.args[1].endswith('/transcribe.py'))


class SelectedBackendTests(unittest.TestCase):
    def test_parakeet_uses_pinned_files_for_readiness_and_repair(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            hub = Mock()
            hub.snapshot_download.return_value = str(directory)
            with patch.dict('sys.modules', {'huggingface_hub': hub}):
                for name in ('config.json', 'model.safetensors'):
                    (directory / name).write_bytes(b'fixture')
                for mode in ('check', 'repair'):
                    if mode == 'repair':
                        (directory / 'model.safetensors').write_bytes(b'')
                        with self.assertRaisesRegex(RuntimeError, 'incomplete'):
                            transcribe.model_path(backend='parakeet')
                        def download(**kwargs):
                            (directory / 'model.safetensors').write_bytes(b'repaired')
                            return str(directory)
                        hub.snapshot_download.side_effect = download
                    self.assertEqual(transcribe.model_path(setup=mode == 'repair', repair=mode == 'repair',
                                                           backend='parakeet'), str(directory))
                    call = hub.snapshot_download.call_args.kwargs
                    self.assertEqual(call['repo_id'], 'mlx-community/parakeet-tdt-0.6b-v3')
                    self.assertEqual(call['revision'], 'ed2b7e8c15f9aaa0b5772e2efb986255eaef7e15')
                    self.assertEqual(call['local_files_only'], mode == 'check')
                    self.assertEqual(call['force_download'], mode == 'repair')
                    self.assertFalse(call['token'])
                    self.assertIn('model.safetensors', call['allow_patterns'])

    def test_parakeet_script_uses_bfloat16_greedy_and_explicit_chunks(self):
        engine = Mock()
        engine.from_pretrained.return_value.transcribe.return_value.text = 'Parakeet transcript'
        mx = Mock()
        with patch.dict('sys.modules', {'parakeet_mlx': engine, 'mlx': Mock(core=mx), 'mlx.core': mx}), \
                patch.object(transcribe, 'model_path', return_value='/pinned/parakeet') as snapshot, \
                patch.object(transcribe, 'decode', side_effect=AssertionError('Whisper decoder used')), \
                patch.object(transcribe.sys, 'argv', ['transcribe.py', '--backend', 'parakeet', '/audio.oga']), \
                patch('builtins.print') as output:
            transcribe.main()
        engine.from_pretrained.assert_called_once_with('/pinned/parakeet', dtype=mx.bfloat16)
        engine.from_pretrained.return_value.transcribe.assert_called_once_with(
            '/audio.oga', chunk_duration=120.0, overlap_duration=15.0)
        snapshot.assert_called_once_with(backend='parakeet')
        output.assert_called_once_with('Parakeet transcript')

    def test_selected_setup_keeps_backend_environments_and_readiness_separate(self):
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            def run(command, **kwargs):
                if 'venv' in command:
                    python = Path(command[-1]) / 'bin/python'
                    python.parent.mkdir(parents=True)
                    python.write_text('fixture')
            with patch.object(transcription.platform, 'system', return_value='Darwin'), \
                    patch.object(transcription.platform, 'machine', return_value='arm64'), \
                    patch.object(transcription.platform, 'mac_ver', return_value=('27.0', (), 'arm64')), \
                    patch.object(transcription.shutil, 'which', return_value='/fake/ffmpeg'), \
                    patch.object(transcription, 'uv_binary', return_value='/fake/uv'), \
                    patch.object(transcription.subprocess, 'run', side_effect=run) as launch:
                env = {'PATH': '/fake/bin'}
                parakeet = transcription.prepare_environment(state, env, backend='parakeet')
                self.assertIn('parakeet', parakeet)
                self.assertTrue(launch.call_args_list[1].args[0][-1].endswith('transcription-parakeet.lock'))
                self.assertIn('parakeet', launch.call_args.args[0])
                whisper = transcription.prepare_environment(state, env, backend='whisper')
                self.assertNotEqual(parakeet, whisper)
                self.assertTrue(launch.call_args_list[4].args[0][-1].endswith('transcription-macos.lock'))
                launch.reset_mock()
                transcription.prepare_environment(state, env, backend='parakeet')
                launch.assert_called_once()
                self.assertEqual(launch.call_args.args[0],
                                 [parakeet, str(Path(transcribe.__file__)), '--backend', 'parakeet', '--check'])

    def test_parakeet_refuses_unsupported_platform_before_setup(self):
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(transcription.platform, 'system', return_value='Linux'), \
                patch.object(transcription, 'uv_binary') as install:
            with self.assertRaisesRegex(transcription.UnsupportedVoice, 'Parakeet.*Apple silicon'):
                transcription.prepare_environment(temporary, {'PATH': '/fake/bin'}, backend='parakeet')
            install.assert_not_called()

    def test_memory_estimates_follow_backend_duration_and_unknown_length(self):
        for backend, expected in (('parakeet', [3, 3, 6, 8, 8]), ('whisper', [3, 3, 5, 6, 6])):
            actual = [media.transcription_memory({'duration': duration}, backend)
                      for duration in (60, 120, 600, 1800, None)]
            self.assertEqual(actual, expected)

    def test_missing_ffmpeg_has_a_clear_reason_before_any_setup_process(self):
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(transcription.platform, 'system', return_value='Darwin'), \
                patch.object(transcription.platform, 'machine', return_value='arm64'), \
                patch.object(transcription.platform, 'mac_ver', return_value=('27.0', (), 'arm64')), \
                patch.object(transcription.shutil, 'which', return_value=None), \
                patch.object(transcription.asyncio, 'create_subprocess_exec') as launch:
            with self.assertRaisesRegex(transcription.UnsupportedVoice, 'ffmpeg.*required.*Parakeet'):
                asyncio.run(transcription.setup(temporary))
            launch.assert_not_called()
