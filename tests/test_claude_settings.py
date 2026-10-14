import os
from pathlib import Path
import pwd
import unittest
import runpy
from unittest.mock import patch

from coordinator.claude_settings import claude_md_excludes


class ClaudeSettingsTests(unittest.TestCase):
    def test_excludes_only_ancestors_of_the_agent_home(self):
        self.assertEqual(claude_md_excludes('/outer/home'), [
            '/outer/CLAUDE.md', '/outer/CLAUDE.local.md', '/outer/.claude/CLAUDE.md',
            '/CLAUDE.md', '/CLAUDE.local.md', '/.claude/CLAUDE.md'])

    def test_both_policies_use_the_real_home_trash(self):
        root = Path(__file__).resolve().parents[1]
        with patch('coordinator.host_os.SYSTEM', 'darwin'):
            policy = runpy.run_path(str(root / 'coordinator/policy.py'), run_name='coordinator._capture')
        deletion = policy['DELETION_POLICY']
        from coordinator import policy as module
        with patch.object(module, 'DELETION_POLICY', deletion):
            worker = runpy.run_path(str(root / 'coordinator/workers.py'), run_name='coordinator._capture')
        trash = str(Path(pwd.getpwuid(os.getuid()).pw_dir) / '.Trash')
        with patch.dict(os.environ, {'HOME': '/agent/home'}):
            for policy in (policy['COORDINATOR_POLICY'], worker['WORKER_POLICY']):
                self.assertIn(deletion, policy)
                self.assertIn('macOS Trash at ' + trash, policy)
                self.assertNotIn('/agent/home/.Trash', policy)
                self.assertIn('add a suffix if that name already exists', policy)
                self.assertIn('owner insists after a reminder', policy)
                self.assertIn('regenerable git-ignored', policy)
                self.assertIn('Check what a target is before you move it', policy)
