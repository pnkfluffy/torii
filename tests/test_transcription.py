import asyncio
import fcntl
import hashlib
import io
import os
import subprocess
import sys
import time
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, patch

from coordinator import media, transcribe, transcription


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
                    transcription.prepare_environment(self.root, {'PATH': '/usr/bin:/bin'})
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
                self.assertEqual(transcription.prepare_environment(state, {'PATH': '/usr/bin:/bin'}), str(python))
                self.assertEqual(launch.call_count, 3)
                sync = launch.call_args_list[1].args[0]
                self.assertIn('--require-hashes', sync)
                self.assertIn('--only-binary', sync)
                self.assertTrue(sync[-1].endswith('transcription-' + lock + '.lock'))
                self.assertEqual(launch.call_args_list[2].args[0][-1], '--setup')
                for call in launch.call_args_list:
                    self.assertEqual(call.kwargs['timeout'], 3600)
                launch.reset_mock()
                transcription.prepare_environment(state, {'PATH': '/usr/bin:/bin'})
                launch.assert_not_called()

    def test_failed_setup_is_retried_without_a_ready_marker(self):
        with patch.object(transcription.platform, 'system', return_value='Darwin'), \
                patch.object(transcription.platform, 'machine', return_value='arm64'), \
                patch.object(transcription, 'uv_binary', return_value='/fake/uv'), \
                patch.object(transcription.subprocess, 'run', side_effect=RuntimeError('fake failure')):
            with self.assertRaises(RuntimeError):
                transcription.prepare_environment(self.root, {'PATH': '/usr/bin:/bin'})
        self.assertFalse((self.root / 'transcription/ready').exists())

    def cancel_setup(self):
        uv = self.root / 'uv'
        uv.write_text("#!/bin/sh\nmkdir -p venv/bin\nprintf partial > venv/bin/python\n"
                      "sleep 2 &\necho $! > child.pid\nwait\n")
        uv.chmod(0o700)
        runner = """
import asyncio
import sys
import time
from pathlib import Path
from unittest.mock import patch
from coordinator import media, transcription
state = Path(sys.argv[1])
async def serve():
    task = asyncio.create_task(media.transcribe_audio(state / 'voice.oga', state))
    while not (state / 'transcription/child.pid').exists():
        await asyncio.sleep(0.01)
    started = time.monotonic()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    return started
with patch.object(transcription, 'environment', return_value={'PATH': str(state) + ':/usr/bin:/bin'}):
    started = asyncio.run(serve())
print(time.monotonic() - started)
"""
        result = subprocess.run([sys.executable, '-c', runner, str(self.root)],
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertLess(float(result.stdout.strip()), 1)
        child = int((self.root / 'transcription/child.pid').read_text())
        for _ in range(100):
            try:
                os.kill(child, 0)
            except ProcessLookupError:
                break
            status = subprocess.run(['ps', '-o', 'stat=', '-p', str(child)],
                                    capture_output=True, text=True).stdout.strip()
            if status.startswith('Z'):
                break
            time.sleep(0.01)
        else:
            self.fail('setup child survived shutdown')
        self.assertFalse((self.root / 'transcription/ready').exists())

    def test_shutdown_during_setup_closes_loop_and_stops_process_group(self):
        self.cancel_setup()

    def test_shutdown_while_waiting_for_setup_lock_closes_loop(self):
        directory = self.root / 'transcription'
        directory.mkdir()
        uv = self.root / 'uv'
        uv.write_text('#!/bin/sh\nexit 9\n')
        uv.chmod(0o700)
        async def cancel():
            task = asyncio.create_task(transcription.setup(self.root))
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
                            transcription.prepare_environment(state, {'PATH': '/usr/bin:/bin'})
                else:
                    python.parent.mkdir(parents=True)
                    python.write_text('interrupted-python')
                    partial.write_text('incomplete')
                self.assertFalse((state / 'transcription/ready').exists())
                with patch.object(transcription.subprocess, 'run', side_effect=run) as launch:
                    self.assertEqual(transcription.prepare_environment(state, {'PATH': '/usr/bin:/bin'}),
                                     str(python))
                    self.assertIn('venv', launch.call_args_list[0].args[0])
                    self.assertEqual(launch.call_count, 3)
                self.assertFalse(partial.exists())
                self.assertTrue((state / 'transcription/ready').is_file())
                self.assertEqual(len(list((state / 'transcription').glob('incomplete-*/venv'))), 1)

    def test_cancelled_setup_retries_with_a_fresh_environment(self):
        self.cancel_setup()
        uv = self.root / 'uv'
        uv.write_text("#!/bin/sh\nif [ \"$2\" = venv ]; then\n"
                      "test ! -e venv/bin/python || exit 9\nmkdir -p venv/bin\n"
                      "printf '#!/bin/sh\\nexit 0\\n' > venv/bin/python\n"
                      "chmod 700 venv/bin/python\nfi\n")
        with patch.object(transcription, 'environment',
                          return_value={'PATH': str(self.root) + ':/usr/bin:/bin'}):
            python, _ = asyncio.run(transcription.setup(self.root))
        self.assertEqual(python, str(self.root / 'transcription/venv/bin/python'))
        self.assertTrue((self.root / 'transcription/ready').is_file())
        self.assertEqual(len(list((self.root / 'transcription').glob('incomplete-*/venv'))), 1)

    def test_environment_omits_ambient_credentials(self):
        with patch.dict(os.environ, {'AWS_SECRET_ACCESS_KEY': 'fake', 'HF_TOKEN': 'fake'}):
            env = transcription.environment()
        self.assertEqual(set(env), {'HOME', 'PATH', 'UV_CACHE_DIR', 'HF_HOME', 'HF_HUB_DISABLE_XET'})
        self.assertEqual(env['HF_HUB_DISABLE_XET'], '1')

    def test_both_models_use_pinned_revisions_and_only_setup_can_download(self):
        hub = Mock()
        with patch.dict('sys.modules', {'huggingface_hub': hub}):
            for system, machine, model in (('Darwin', 'arm64', transcribe.MAC_MODEL),
                                           ('Linux', 'x86_64', transcribe.LINUX_MODEL)):
                with patch.object(transcribe.platform, 'system', return_value=system), \
                        patch.object(transcribe.platform, 'machine', return_value=machine):
                    transcribe.model_path(setup=True)
                    self.assertEqual(hub.snapshot_download.call_args.kwargs['revision'], model[1])
                    self.assertFalse(hub.snapshot_download.call_args.kwargs['local_files_only'])
                    transcribe.model_path()
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
                patch.object(transcribe.sys, 'argv', ['transcribe.py', '/audio.ogg']), \
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
        async def setup(state):
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
