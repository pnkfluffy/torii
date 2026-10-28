"""Bounded GitHub CLI reads shared by wait triggers and optional integrations."""

import asyncio
import json
import os
from pathlib import Path
import pwd
import shutil


GITHUB_TIMEOUT = 30


class GitHubError(Exception):
    """A safe error code without GitHub output or credentials."""


async def run_github(argv, timeout):
    binary = shutil.which(argv[0])
    if not binary:
        raise GitHubError('github_unavailable')
    env = dict(os.environ)
    env.setdefault('GH_CONFIG_DIR', str(Path(pwd.getpwuid(os.getuid()).pw_dir) / '.config' / 'gh'))
    try:
        process = await asyncio.create_subprocess_exec(binary, *argv[1:], stdout=asyncio.subprocess.PIPE,
                                                       stderr=asyncio.subprocess.DEVNULL, env=env)
        try:
            stdout, _ = await asyncio.wait_for(process.communicate(), timeout)
        except BaseException:
            if process.returncode is None:
                process.kill()
            await process.wait()
            raise
        result = json.loads(stdout)
        if process.returncode and not (argv[1:3] == ['api', 'graphql'] and isinstance(result, dict)
                and isinstance(result.get('data'), dict) and 'repository' in result['data']):
            raise ValueError()
        return result
    except (OSError, ValueError, asyncio.TimeoutError):
        raise GitHubError('github_check_failed') from None
