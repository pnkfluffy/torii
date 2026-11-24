"""Preserve custom rules on the first store open after a Torii 2.0 upgrade."""

from hashlib import sha256
import logging

from . import problems
from .loadout import topic_for_key
from .policy import _clean, _rule_stat, atomic_rule_write, chat_path, read_rule, rule_lines


DEFAULT_USAGE_SHA256 = 'd3206ceadee74fecbf0b9c5eda3474943960e7c59fa28a4e41348473d52aaa1b'
MARKER = 'legacy_rules_v2'


def _personal(path, legacy):
    if _rule_stat(path) is not None:
        return False
    try:
        text = read_rule(legacy)
    except FileNotFoundError:
        return False
    if sha256(text.encode('utf-8')).hexdigest() == DEFAULT_USAGE_SHA256:
        return False
    atomic_rule_write(path, text, overwrite=False)
    return True


def _chat(path, text):
    try:
        current = read_rule(path)
    except FileNotFoundError:
        current = None
    if current is not None and rule_lines(current):
        return False
    heading = '' if '# Chat' in _clean(current or '').splitlines() else '# Chat\n\n'
    prefix = current + ('\n' if current.endswith('\n') else '\n\n') if current else ''
    suffix = '' if text.endswith('\n') else '\n'
    atomic_rule_write(path, prefix + heading + text + suffix, overwrite=current is not None)
    return True


def _sources(store):
    usage = store.directory / 'USAGE.md'
    if usage.exists():
        yield store.directory / 'personal.md', None, _personal, usage
    keys = store.db.execute(
        "SELECT key FROM settings WHERE key='coordinator_delegation' OR key GLOB 'coordinator:*_delegation' ORDER BY key"
    ).fetchall()
    for (key,) in keys:
        setting = store.get(key)
        text = setting.get('text') if isinstance(setting, dict) else None
        if isinstance(text, str) and text.strip():
            topic = topic_for_key(store, key.removesuffix('_delegation'))
            if topic:
                yield chat_path(store, topic), topic, _chat, text


def migrate(store):
    if store.get(MARKER) is True:
        return
    sources = list(_sources(store))
    if not sources:
        return
    complete = True
    for path, topic, migrate_file, source in sources:
        try:
            if migrate_file(path, source):
                problems.record(store, 'policy', 'rule-file-migrated', path.name, topic=topic, level=logging.INFO)
        except (OSError, UnicodeError) as error:
            complete = False
            problems.record(store, 'policy', 'rule-file-migration-failed',
                            path.name + ': ' + type(error).__name__, topic=topic)
    if complete:
        store.put(MARKER, True)
