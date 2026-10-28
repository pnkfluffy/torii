"""Claude instruction files outside the agent home."""

from pathlib import Path


def claude_md_excludes(home):
    path = Path(home).absolute()
    excludes = [str(parent / name) for parent in path.parents
                for name in ('CLAUDE.md', 'CLAUDE.local.md', '.claude/CLAUDE.md')]
    return excludes
