"""Read and set topic-scoped MCP selections."""

import json
from pathlib import Path

from .control_api import Refused, Result

def _names(value):
    if not isinstance(value, list) or any(not isinstance(name, str) or not name for name in value):
        return None
    return list(dict.fromkeys(value))


def selection(store, topic):
    """Return the chat's own selection, else the union of its tags' defaults."""
    if topic is None:
        return []
    settings = store.get('chat_config:' + str(topic), {})
    if not isinstance(settings, dict):
        return None
    if 'mcp_servers' in settings:
        return _names(settings['mcp_servers'])
    from .policy import chat_tags
    names = []
    for tag in chat_tags(store, str(topic)):
        defaults = _names(store.get('tag_mcp:' + tag, []))
        if defaults is None:
            return None
        names.extend(defaults)
    return list(dict.fromkeys(names))


def _parse(names):
    try:
        selected = json.loads(names)
        if selected is None:
            return None
        if not isinstance(selected, list) or any(not isinstance(name, str) or not name or name == 'torii'
                                                  for name in selected):
            raise ValueError()
        if selected:
            document = json.loads((Path.home() / '.claude.json').read_text())
            servers = document['mcpServers']
            if not isinstance(servers, dict) or any(name not in servers for name in selected):
                raise ValueError()
    except (OSError, UnicodeError, ValueError, KeyError, TypeError):
        raise Refused('Send a JSON list of existing MCP server names from ~/.claude.json, or null.') from None
    return list(dict.fromkeys(selected))


def _local(ctx):
    if ctx.source_worker is not None:
        raise Refused('Only the coordinator or the owner can use MCP selection.')


def settings_text(store, topic):
    """Name the chat's MCP servers for the chat agent and its workers."""
    names = selection(store, topic)
    return '\nMCP servers of this chat: %s.' % ', '.join('`%s`' % name for name in names) if names else ''


def op_chat_mcp(store, ctx, topic, names=None):
    _local(ctx)
    if not store.topic(topic):
        raise Refused('Choose a linked channel first.')
    if names is not None:
        selected = _parse(names)
        with store.transaction():
            settings = store.get('chat_config:' + topic, {})
            if not isinstance(settings, dict):
                raise Refused('The chat configuration is invalid.')
            settings = {key: value for key, value in settings.items() if key != 'mcp_servers'}
            if selected is not None:
                settings['mcp_servers'] = selected
            store.put('chat_config:' + topic, settings)
    selected = selection(store, topic)
    if selected is None:
        raise Refused('The chat MCP selection is invalid.')
    settings = store.get('chat_config:' + topic, {})
    inherited = not (isinstance(settings, dict) and 'mcp_servers' in settings)
    text = 'Chat MCP selection saved for future launches.' if names is not None else 'Chat MCP selection.'
    return Result(True, text, {'topic': topic, 'mcp_servers': selected, 'inherited': inherited})


def op_tag_mcp(store, ctx, tag, names=None):
    from .policy import valid_tag
    _local(ctx)
    if not valid_tag(tag):
        raise Refused('Send a tag of 1 to 64 lowercase letters, digits, underscores or hyphens.')
    if names is not None:
        selected = _parse(names)
        with store.transaction():
            if selected:
                store.put('tag_mcp:' + tag, selected)
            else:
                store.db.execute('DELETE FROM settings WHERE key=?', ('tag_mcp:' + tag,))
    selected = _names(store.get('tag_mcp:' + tag, []))
    if selected is None:
        raise Refused('The tag MCP defaults are invalid.')
    text = 'Tag MCP defaults saved for future launches.' if names is not None else 'Tag MCP defaults.'
    return Result(True, text, {'tag': tag, 'mcp_servers': selected})


def migrate(store, initial):
    if store.get('chat_mcp_initial_v1') is True:
        return
    for topic, value in initial.items():
        settings = store.get('chat_config:' + topic, {})
        if isinstance(settings, dict) and 'mcp_servers' not in settings:
            store.put('chat_config:' + topic, dict(settings, mcp_servers=value))
    store.put('chat_mcp_initial_v1', True)


def migrate_shared(store):
    """Give each existing chat the servers that Torii 2.0 and 2.1 shared with every chat once.

    Those versions read the names from shared_mcp_servers in the checkout's local-overrides.json.
    With no names or no chats there is nothing to move, and nothing is written.
    """
    if store.get('chat_mcp_initial_v1') is True:
        return
    from .accounts import LOCAL_OVERRIDES_PATH
    try:
        names = _names(json.loads(LOCAL_OVERRIDES_PATH.read_text()).get('shared_mcp_servers'))
    except (OSError, UnicodeError, ValueError, AttributeError):
        names = None
    names = [name for name in names or () if name != 'torii']
    topics = store.topics()
    if names and topics:
        migrate(store, {topic['id']: names for topic in topics})


def inherit_tag_defaults(store, defaults, topics):
    """Set tag MCP defaults once and let the named chats inherit them."""
    if store.get('chat_mcp_tags_v1') is True:
        return
    for tag, names in defaults.items():
        if store.get('tag_mcp:' + tag) is None:
            store.put('tag_mcp:' + tag, names)
    for topic in topics:
        settings = store.get('chat_config:' + topic)
        if isinstance(settings, dict) and 'mcp_servers' in settings:
            store.put('chat_config:' + topic, {key: value for key, value in settings.items() if key != 'mcp_servers'})
    store.put('chat_mcp_tags_v1', True)
