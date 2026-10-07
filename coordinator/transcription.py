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
import urllib.request

SETUP_TIMEOUT = 3600
UV_VERSION = '0.11.18'
UV_ARCHIVES = {
    ('Darwin', 'arm64'): ('aarch64-apple-darwin', '1a7adf8dadae3b55853115d13a8bf564d219597ad13824b93b213706933863e5'),
    ('Darwin', 'x86_64'): ('x86_64-apple-darwin', '00a61e3db99b53c927a7e6c4ccdccb898aa3253d07928822211e9dc570a25661'),
    ('Linux', 'aarch64'): ('aarch64-unknown-linux-gnu', '0f03c6648df1c159557f4222c0f37250f84733fb88d6fc3c16770e17c177a8c9'),
    ('Linux', 'x86_64'): ('x86_64-unknown-linux-gnu', '588f3e360f69ce02b6982aa99f2240e803933a6b7e176ac01617830adf955add'),
}


class UnsupportedVoice(RuntimeError):
    pass


def check_voice_support():
    if platform.system() == 'Darwin' and (platform.machine() != 'arm64' or
                                          int(platform.mac_ver()[0].split('.')[0] or 0) < 14):
        raise UnsupportedVoice('Voice notes require macOS 14 or newer on Apple silicon.')


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


def prepare_environment(state, env):
    check_voice_support()
    directory = Path(state) / 'transcription'
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    if platform.system() not in ('Darwin', 'Linux'):
        raise RuntimeError('unsupported platform')
    backend = 'macos' if platform.system() == 'Darwin' and platform.machine() == 'arm64' else 'linux'
    lock = Path(__file__).with_name('transcription-' + backend + '.lock')
    script = Path(__file__).with_name('transcribe.py')
    revision = hashlib.sha256(lock.read_bytes() + script.read_bytes()).hexdigest()
    python = directory / 'venv/bin/python'
    ready = directory / 'ready'
    with (directory / 'setup.lock').open('a') as guard:
        fcntl.flock(guard, fcntl.LOCK_EX)
        if python.is_file() and ready.is_file() and ready.read_text() == revision:
            return str(python)
        uv = uv_binary(directory, env)
        def run(command):
            subprocess.run(command, env=env, cwd=directory, check=True, timeout=SETUP_TIMEOUT,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        venv = directory / 'venv'
        if venv.exists() or ready.exists():
            incomplete = Path(tempfile.mkdtemp(prefix='incomplete-', dir=directory))
            for previous in (venv, ready):
                if previous.exists():
                    os.replace(previous, incomplete / previous.name)
        run([uv, '--no-config', 'venv', '--python', '3.11.14', str(venv)])
        run([uv, '--no-config', 'pip', 'sync', '--require-hashes', '--only-binary', ':all:',
             '--python', str(python), str(lock)])
        run([str(python), str(script), '--setup'])
        ready.write_text(revision)
        return str(python)


async def setup(state):
    check_voice_support()
    env = environment()
    env['XDG_CONFIG_HOME'] = str(Path(state).resolve() / 'config')
    process = await asyncio.create_subprocess_exec(
        sys.executable, '-X', 'utf8', str(Path(__file__).resolve()), str(Path(state).resolve()),
        env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        start_new_session=True)
    try:
        output, _ = await process.communicate()
        if process.returncode:
            raise RuntimeError('transcription setup failed')
        return output.decode('utf-8').strip(), env
    finally:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        await process.communicate()


if __name__ == '__main__':
    print(prepare_environment(sys.argv[1], dict(os.environ)))
