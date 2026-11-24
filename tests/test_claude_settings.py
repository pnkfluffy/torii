import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from coordinator.claude_settings import claude_md_excludes
from coordinator.extension import available


class ClaudeSettingsTests(unittest.TestCase):
    def test_excludes_only_ancestors_of_the_agent_home(self):
        self.assertEqual(claude_md_excludes('/outer/home'), [
            '/outer/CLAUDE.md', '/outer/CLAUDE.local.md', '/outer/.claude/CLAUDE.md',
            '/CLAUDE.md', '/CLAUDE.local.md', '/.claude/CLAUDE.md'])

    def test_both_rule_sets_move_deletions_with_trash_move(self):
        from coordinator import policy
        from coordinator.store import Store
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'HOME': '/agent/home'}):
            store = Store(Path(directory))
            self.addCleanup(store.close)
            for audience in ('coordinator', 'worker'):
                fixed, _ = policy.instructions(store, 'coordinator', audience, create_scratch=False)
                self.assertIn('Move everything else with `trash.move`. Never hard-delete it.' if available('trash_move')
                              else 'Never hard-delete. Move the target to the Trash', fixed)
