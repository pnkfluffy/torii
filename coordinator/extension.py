"""Optional launch behavior. Native processes use their own profile sign-in."""

import asyncio
import importlib
import importlib.util


LOCAL = __package__ + '.extension_local'
_active = None


def available(name):
    return importlib.util.find_spec(__package__ + '.' + name) is not None


def optional(name):
    if available(name):
        return importlib.import_module('.' + name, __package__)
    return None


class Native:
    """Default launch behavior for native profiles."""

    worker_policy = ''

    def prepare(self, store):
        return None

    def bind(self, service):
        return None

    async def start(self):
        return None

    async def close(self):
        return None

    def environment(self, env, alias):
        return env

    async def private(self, env):
        return {}

    async def launch_process(self, argv, env, **options):
        return await asyncio.create_subprocess_exec(*argv, env=env, **options)

    def launch_failure_has_no_child(self, env):
        return True

    def state(self, env):
        return 'native'

    def control_handler(self, env):
        return None

    def unanswered(self, past, later=()):
        return []

    def fingerprint(self):
        return {}

    async def oneshot(self, env, alias):
        return env

    async def login_ready(self, config_dir, prompt=None):
        return True

    def health(self, store):
        return None


def load():
    try:
        module = importlib.import_module(LOCAL)
    except ModuleNotFoundError as error:
        if error.name != LOCAL:
            raise
        return Native()
    return module.Extension()


def active():
    global _active
    if _active is None:
        _active = load()
    return _active


def use(instance):
    """Replace the process-wide extension, including in isolated tests."""
    global _active
    _active = instance
