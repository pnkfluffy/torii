"""Default launch behavior and strict optional-module discovery."""

import unittest
from unittest.mock import patch

from coordinator import extension


class ExtensionTests(unittest.IsolatedAsyncioTestCase):
    def tearDown(self):
        extension.use(extension.Native())

    def test_absent_module_uses_native(self):
        with patch.object(extension.importlib, 'import_module',
                          side_effect=ModuleNotFoundError(name=extension.LOCAL)):
            self.assertIs(type(extension.load()), extension.Native)

    def test_missing_dependency_fails_loudly(self):
        with patch.object(extension.importlib, 'import_module',
                          side_effect=ModuleNotFoundError(name='coordinator.other')):
            with self.assertRaises(ModuleNotFoundError):
                extension.load()

    def test_broken_module_fails_loudly(self):
        with patch.object(extension.importlib, 'import_module', side_effect=ImportError('broken')):
            with self.assertRaises(ImportError):
                extension.load()

    def test_active_caches_and_use_replaces(self):
        extension.use(None)
        instance = extension.Native()
        with patch.object(extension, 'load', return_value=instance) as load:
            self.assertIs(extension.active(), instance)
            self.assertIs(extension.active(), instance)
        load.assert_called_once_with()
        other = extension.Native()
        extension.use(other)
        self.assertIs(extension.active(), other)

    async def test_native_hooks_leave_profile_launches_unchanged(self):
        native = extension.Native()
        env = {'CLAUDE_CONFIG_DIR': '/test/profile'}
        self.assertIs(native.environment(env, 'test'), env)
        self.assertIs(await native.oneshot(env, 'test'), env)
        self.assertEqual(env, {'CLAUDE_CONFIG_DIR': '/test/profile'})
        self.assertEqual(await native.private(env), {})
        self.assertEqual(native.state(env), 'native')
        self.assertIsNone(native.control_handler(env))
        self.assertEqual(native.unanswered(['invalid'], ['invalid']), [])
        self.assertEqual(native.fingerprint(), {})
        self.assertIsNone(native.bind(object()))
        self.assertIsNone(await native.start())
        self.assertIsNone(await native.close())
        self.assertIsNone(native.health(object()))
        self.assertTrue(await native.login_ready('/test/profile'))
