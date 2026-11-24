from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[1]


class InstallDefaultsTests(unittest.TestCase):
    def open_store(self, directory):
        from coordinator.store import Store
        store = Store(directory)
        self.addCleanup(store.close)
        return store

    def service(self, store, model=None):
        from coordinator.service import Service
        return Service(store, None, SimpleNamespace(binaries={}), store.directory,
                       **({'coordinator_model': model} if model is not None else {}))

    def test_fresh_install_uses_neutral_model_and_effort_after_startup_and_reopen(self):
        from coordinator.parent_state import effective_agent
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp) / 'state'
            store = self.open_store(directory)
            for current in (store, self.open_store(directory)):
                self.assertEqual(self.service(current).coordinator_model, 'sonnet')
                self.assertEqual(effective_agent(current, provider='claude')['model'], 'sonnet')
                for provider in ('claude', 'codex'):
                    self.assertEqual(effective_agent(current, provider=provider)['effort'], 'medium')
            self.assertFalse((directory / 'USAGE.md').exists())


class AccountToolTests(unittest.TestCase):
    def test_account_tools_explain_owner_controls(self):
        from coordinator.mcp import tools_list
        descriptions = {tool['name']: tool['description'] for tool in tools_list()}
        self.assertIn('MCP requires owner message=N', descriptions['account_use'])
        for name in ('accounts_claude_auto', 'account_redeem'):
            self.assertIn('message=N for the owner message that asked is required', descriptions[name])


class AgentFileTests(unittest.TestCase):
    def test_repository_agent_files_stay_short(self):
        claude = (ROOT / 'CLAUDE.md').read_text()
        self.assertEqual([line for line in claude.splitlines() if line.strip()],
                         ['# Torii', 'Read and follow `AGENTS.md` in this directory.',
                          'Reply in normal Markdown; Torii converts it for Telegram, '
                          'and owner formatting arrives as Markdown.'])
        agents = (ROOT / 'AGENTS.md').read_text()
        for sentence in ('Questions and brainstorming', 'secret.ask', 'Python 3.9', '/usr/bin/python3'):
            self.assertNotIn(sentence, agents)


if __name__ == '__main__':
    unittest.main()
