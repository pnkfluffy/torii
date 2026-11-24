"""Run bounded cleanup scans outside the service process."""

import asyncio
import importlib
import logging
import os
from pathlib import Path
import pickle
import select
import signal
import subprocess
import sys
import threading
import time


logger = logging.getLogger(__name__)
TIMEOUT = 30
FAILURE_PAUSE_LIMIT = 60
MAX_OUTPUT_BYTES = 32 * 1024 * 1024
MAX_REFERENCE_COUNT = 10000
MAX_REFERENCE_CHARS = 1000000


class ScanError(RuntimeError):
    pass


def guard_reference_set(values):
    """Skip cleanup when validating references would exceed the parent work limit."""
    count = 0
    size = 0
    for value in values:
        count += 1
        if count > MAX_REFERENCE_COUNT or not isinstance(value, str):
            raise ScanError('reference set exceeds the cleanup input limit')
        size += len(value)
        if size > MAX_REFERENCE_CHARS:
            raise ScanError('reference set exceeds the cleanup input limit')
    return values


class BoundedOutput:
    def __init__(self):
        self.parts = []
        self.size = 0

    def write(self, data):
        self.size += len(data)
        if self.size > MAX_OUTPUT_BYTES:
            raise ScanError('scan result exceeds size limit')
        self.parts.append(data)


_backoff_lock = threading.Lock()
_backoff = {'pause': 0.0, 'until': 0.0}


def backoff_delay():
    with _backoff_lock:
        return max(0.0, _backoff['until'] - time.monotonic())


def _scan_failed(function, elapsed, reason):
    with _backoff_lock:
        pause = min(FAILURE_PAUSE_LIMIT, max(elapsed, 2 * _backoff['pause']))
        _backoff['pause'] = pause
        _backoff['until'] = time.monotonic() + pause
    logger.warning('cleanup scan %s function=%s seconds=%.1f; next scan waits %.1f s',
                   reason, function.__name__, elapsed, pause)


def _scan_succeeded():
    with _backoff_lock:
        _backoff['pause'] = 0.0
        _backoff['until'] = 0.0


def child_env():
    names = ('PATH', 'HOME', 'TMPDIR', 'PYTHONPYCACHEPREFIX', 'PYTHONDONTWRITEBYTECODE',
             'LANG', 'LC_ALL', 'LC_CTYPE')
    return {name: os.environ[name] for name in names if name in os.environ}


def run(function, *args, timeout=TIMEOUT):
    delay = backoff_delay()
    if delay:
        time.sleep(delay)
    started = time.monotonic()
    payload = pickle.dumps((function.__module__, function.__name__, args))
    read_fd, write_fd = os.pipe()
    try:
        process = subprocess.Popen(
            (sys.executable, '-m', 'coordinator.scan_process', str(read_fd)),
            cwd=str(Path(__file__).resolve().parent.parent),
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            start_new_session=True, env=child_env(), pass_fds=(read_fd,),
        )
    except BaseException:
        os.close(write_fd)
        raise
    finally:
        os.close(read_fd)
    try:
        try:
            output, _ = process.communicate(payload, timeout=timeout)
        except subprocess.TimeoutExpired as error:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.communicate()
            _scan_failed(function, time.monotonic() - started, 'timed out')
            raise ScanError('scan timed out') from error
    finally:
        os.close(write_fd)
    if process.returncode:
        _scan_failed(function, time.monotonic() - started, 'process failed')
        raise ScanError('scan process failed')
    _scan_succeeded()
    try:
        succeeded, result = pickle.loads(output)
    except (pickle.UnpicklingError, EOFError, ValueError, TypeError) as error:
        raise ScanError('invalid scan result') from error
    if not succeeded:
        if result == 'scan result exceeds size limit':
            raise ScanError(result)
        if not isinstance(result, Exception):
            raise ScanError('invalid scan error')
        raise result
    return result


async def run_async(function, *args, timeout=TIMEOUT):
    delay = backoff_delay()
    if delay:
        await asyncio.sleep(delay)
    operation = asyncio.create_task(asyncio.to_thread(run, function, *args, timeout=timeout))
    try:
        return await asyncio.shield(operation)
    except asyncio.CancelledError:
        while not operation.done():
            try:
                await asyncio.shield(operation)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        if not operation.cancelled():
            try:
                operation.result()
            except Exception:
                pass
        raise


def main():
    read_fd = int(sys.argv[1])
    scanner = os.getpid()
    guard = os.fork()
    if guard == 0:
        os.close(0)
        os.close(1)
        os.close(2)
        while os.getppid() == scanner:
            ready, _, _ = select.select([read_fd], [], [], 0.2)
            if ready and not os.read(read_fd, 1):
                os.killpg(os.getpgrp(), signal.SIGKILL)
        os._exit(0)
    os.close(read_fd)
    try:
        scan()
    finally:
        try:
            os.kill(guard, signal.SIGTERM)
        except ProcessLookupError:
            pass
        os.waitpid(guard, 0)


def scan():
    module, name, args = pickle.load(sys.stdin.buffer)
    try:
        result = getattr(importlib.import_module(module), name)(*args)
    except Exception as error:
        result = (False, error)
    else:
        result = (True, result)
    output = BoundedOutput()
    try:
        pickle.dump(result, output, protocol=pickle.HIGHEST_PROTOCOL)
    except ScanError:
        pickle.dump((False, 'scan result exceeds size limit'), sys.stdout.buffer, protocol=pickle.HIGHEST_PROTOCOL)
    else:
        for part in output.parts:
            sys.stdout.buffer.write(part)


if __name__ == '__main__':
    main()
