from pathlib import Path
import subprocess
import unittest


ROOT = Path(__file__).resolve().parents[1]


class CheckoutHygieneTests(unittest.TestCase):
    def test_runtime_state_and_credentials_are_ignored_in_checkout(self):
        paths = ['.dev.vars', '.dev.vars.local', 'auth.json', 'nested/auth.json', 'service.stdout.log',
                 'state/state.sqlite', 'logs/service.log', 'hosts/parent/spec.json', 'locks/native',
                 'backups/state.sqlite', 'images/message-1/photo.png', 'files/message-1/report.txt',
                 'certs/ca.key', 'certs/server.pem', 'envelope/master.key', 'envelope/vault.enc',
                 'worktrees/task-1/README.md',
                 'releases/revision/coordinator/__init__.py', 'coordinator-memory/memory.md',
                 'coordinator-mcp.json', 'coordinator-mcp.json.tmp', 'shared-mcp-example.json',
                 'shared-mcp.lock', 'service.lock', 'USAGE.md', 'nested/.dev.vars.production']
        result = subprocess.run(['git', 'check-ignore', '--no-index', '--stdin'], cwd=ROOT,
                                input='\n'.join(paths) + '\n', text=True, capture_output=True, check=True)
        self.assertEqual(set(result.stdout.splitlines()), set(paths))
        source = subprocess.run(['git', 'check-ignore', '--no-index', 'coordinator/defaults/USAGE.md'],
                                cwd=ROOT, capture_output=True)
        self.assertEqual(source.returncode, 1)

    def test_shared_mcp_instructions_precede_license(self):
        readme = (ROOT / 'README.md').read_text()
        self.assertLess(readme.index('## Shared MCP servers'), readme.index('## License'))


if __name__ == '__main__':
    unittest.main()
