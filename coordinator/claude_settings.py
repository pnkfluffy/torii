"""Claude instruction files outside the agent home."""

from pathlib import Path


def claude_md_excludes(home):
    path = Path(home).absolute()
    excludes = [str(parent / name) for parent in path.parents
                for name in ('CLAUDE.md', 'CLAUDE.local.md', '.claude/CLAUDE.md')]
    return excludes


def plugin_switches(store, topic):
    """Per-chat plugin switches from the `plugin_chats` setting. A listed plugin is on only in its listed chats."""
    chats = store.get('plugin_chats') or {}
    if not isinstance(chats, dict):
        return {}
    return {name: str(topic) in topics for name, topics in chats.items() if isinstance(topics, list)}
