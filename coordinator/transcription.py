"""Private, pinned speech-recognition setup, separate from the per-note deadline."""

import asyncio
import fcntl
import hashlib
import io
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.request

SETUP_TIMEOUT = 3600
REPAIR_RETRY = 60
UV_VERSION = '0.11.18'
UV_ARCHIVES = {
    ('Darwin', 'arm64'): ('aarch64-apple-darwin', '1a7adf8dadae3b55853115d13a8bf564d219597ad13824b93b213706933863e5'),
    ('Darwin', 'x86_64'): ('x86_64-apple-darwin', '00a61e3db99b53c927a7e6c4ccdccb898aa3253d07928822211e9dc570a25661'),
    ('Linux', 'aarch64'): ('aarch64-unknown-linux-gnu', '0f03c6648df1c159557f4222c0f37250f84733fb88d6fc3c16770e17c177a8c9'),
    ('Linux', 'x86_64'): ('x86_64-unknown-linux-gnu', '588f3e360f69ce02b6982aa99f2240e803933a6b7e176ac01617830adf955add'),
}


class UnsupportedVoice(RuntimeError):
    pass


class ModelUnavailable(RuntimeError):
    pass


def check_voice_support(backend='parakeet', env=None):
    if backend == 'parakeet':
        if (platform.system() != 'Darwin' or platform.machine() != 'arm64' or
                int(platform.mac_ver()[0].split('.')[0] or 0) < 26):
            raise UnsupportedVoice('Parakeet voice transcription requires Apple silicon and macOS 26 or newer. '
                                   'Select Whisper on this platform.')
        if not shutil.which('ffmpeg', path=(env or environment())['PATH']):
            raise UnsupportedVoice('ffmpeg is required for Parakeet voice transcription. Install ffmpeg and retry the note.')
    if platform.system() == 'Darwin' and (platform.machine() != 'arm64' or
                                          int(platform.mac_ver()[0].split('.')[0] or 0) < 14):
        raise UnsupportedVoice('Voice notes require macOS 14 or newer on Apple silicon.')


def default_backend():
    try:
        check_voice_support('parakeet')
    except UnsupportedVoice:
        return 'whisper'
    return 'parakeet'


def environment():
    home = Path.home()
    return {'HOME': str(home),
            'PATH': str(home / '.local/bin') + ':/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin',
            'UV_CACHE_DIR': str(home / '.cache/uv'),
            'HF_HOME': str(home / '.cache/huggingface'), 'HF_HUB_DISABLE_XET': '1'}


def uv_binary(directory, env):
    found = shutil.which('uv', path=env['PATH'])
    if found:
        return found
    binary = directory / 'uv'
    if binary.is_file():
        return str(binary)
    target, digest = UV_ARCHIVES[(platform.system(), platform.machine())]
    url = 'https://github.com/astral-sh/uv/releases/download/' + UV_VERSION + '/uv-' + target + '.tar.gz'
    with urllib.request.urlopen(url, timeout=120) as response:
        archive = response.read(32 * 1024 * 1024 + 1)
    if len(archive) > 32 * 1024 * 1024 or hashlib.sha256(archive).hexdigest() != digest:
        raise RuntimeError('uv archive checksum mismatch')
    with tarfile.open(fileobj=io.BytesIO(archive), mode='r:gz') as bundle:
        member = bundle.getmember('uv-' + target + '/uv')
        if not member.isfile() or member.size > 64 * 1024 * 1024:
            raise RuntimeError('invalid uv archive')
        data = bundle.extractfile(member).read()
    with tempfile.NamedTemporaryFile(dir=directory, prefix='uv-', delete=False) as stream:
        os.fchmod(stream.fileno(), 0o700)
        stream.write(data)
    os.replace(stream.name, binary)
    return str(binary)


def prepare_environment(state, env, backend='parakeet'):
    check_voice_support(backend, env)
    directory = Path(state) / 'transcription'
    if backend == 'parakeet':
        directory = directory / 'parakeet'
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    if platform.system() not in ('Darwin', 'Linux'):
        raise RuntimeError('unsupported platform')
    lock_name = ('parakeet' if backend == 'parakeet' else
                 'macos' if platform.system() == 'Darwin' and platform.machine() == 'arm64' else 'linux')
    lock = Path(__file__).with_name('transcription-' + lock_name + '.lock')
    script = Path(__file__).with_name('transcribe.py')
    revision = hashlib.sha256(lock.read_bytes() + script.read_bytes()).hexdigest()
    python = directory / 'venv/bin/python'
    ready = directory / 'ready'
    with (directory / 'setup.lock').open('a') as guard:
        fcntl.flock(guard, fcntl.LOCK_EX)
        def run(command):
            subprocess.run(command, env=env, cwd=directory, check=True, timeout=SETUP_TIMEOUT,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if python.is_file() and ready.is_file() and ready.read_text() == revision:
            try:
                run([str(python), str(script), '--backend', backend, '--check'])
            except (OSError, subprocess.SubprocessError):
                failed = directory / 'repair-failed'
                try:
                    last_failure = float(failed.read_text())
                except (OSError, ValueError):
                    last_failure = 0
                if time.time() - last_failure < REPAIR_RETRY:
                    raise ModelUnavailable('transcription model unavailable; repair download failed') from None
                print('model-repair', flush=True)
                try:
                    run([str(python), str(script), '--backend', backend, '--repair'])
                except (OSError, subprocess.SubprocessError):
                    failed.write_text(str(time.time()))
                    raise ModelUnavailable('transcription model unavailable; repair download failed') from None
                failed.write_text('0')
            return str(python)
        uv = uv_binary(directory, env)
        venv = directory / 'venv'
        incomplete = None
        if venv.exists() or ready.exists():
            incomplete = Path(tempfile.mkdtemp(prefix='incomplete-', dir=directory))
            for previous in (venv, ready):
                if previous.exists():
                    os.replace(previous, incomplete / previous.name)
        run([uv, '--no-config', 'venv', '--python', '3.11.14', str(venv)])
        run([uv, '--no-config', 'pip', 'sync', '--require-hashes', '--only-binary', ':all:',
             '--python', str(python), str(lock)])
        run([str(python), str(script), '--backend', backend, '--setup'])
        ready.write_text(revision)
        if incomplete is not None:
            shutil.rmtree(incomplete)
        return str(python)


async def setup(state, on_repair=None, backend='parakeet'):
    env = environment()
    check_voice_support(backend, env)
    env['XDG_CONFIG_HOME'] = str(Path(state).resolve() / 'config')
    process = await asyncio.create_subprocess_exec(
        sys.executable, '-X', 'utf8', str(Path(__file__).resolve()), str(Path(state).resolve()), backend,
        env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        start_new_session=True)
    try:
        output, _ = await process.communicate()
        lines = output.decode('utf-8').strip().splitlines()
        if 'model-repair' in lines and on_repair:
            on_repair()
        if process.returncode == 2:
            raise ModelUnavailable('transcription model unavailable; repair download failed')
        if process.returncode:
            raise RuntimeError('transcription setup failed')
        return lines[-1], env
    finally:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        await process.communicate()


if __name__ == '__main__':
    try:
        print(prepare_environment(sys.argv[1], dict(os.environ), backend=sys.argv[2]))
    except ModelUnavailable:
        sys.exit(2)
