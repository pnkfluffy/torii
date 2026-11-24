"""Per-chat Claude memory and index maintenance."""

import re
import os
from pathlib import Path
import tempfile
from urllib.parse import unquote, urlsplit

from . import problems, sweeps


def directory(store, topic):
    if not topic or topic in ('.', '..') or Path(topic).name != topic:
        raise ValueError('A valid topic is required for per-chat memory')
    return store.directory / 'chats' / topic / 'memory'


def check(store, topic):
    if not topic:
        return False
    path = directory(store, topic) / 'MEMORY.md'
    try:
        count = len(path.read_text(encoding='utf-8').splitlines())
    except FileNotFoundError:
        count = 0
    except (OSError, UnicodeError) as error:
        problems.record(store, 'memory', 'index-read-failed', type(error).__name__, topic=topic)
        return False
    key = 'memory.over_cap:' + topic
    over = count > 40
    with store.db:
        previous = store.get(key, False)
        if over and not previous:
            problems.record(store, 'memory', 'index-over-cap', 'topic=%s lines=%d' % (topic, count), topic=topic)
        if over != previous:
            store.put(key, over)
        if not over and store.get('memory.notified:' + topic, False):
            store.put('memory.notified:' + topic, False)
    return over


def claim_notice(store, topic):
    if not check(store, topic):
        return False
    key = 'memory.notified:' + topic
    with store.db:
        if store.get(key, False):
            return False
        store.put(key, True)
    return True


def release_notice(store, topic):
    with store.db:
        store.put('memory.notified:' + topic, False)


def local_links(line):
    links = []
    pos = 0
    while pos < len(line):
        if line[pos] == '\\':
            pos += 2
            continue
        if line[pos] == '`':
            end = pos
            while end < len(line) and line[end] == '`':
                end += 1
            closing = line.find(line[pos:end], end)
            if closing < 0:
                return None
            pos = closing + end - pos
            continue
        if line[pos] != '[' or pos and line[pos - 1] == '!':
            pos += 1
            continue
        end = pos + 1
        depth = 1
        while end < len(line) and depth:
            if line[end] == '\\':
                end += 2
                continue
            depth += (line[end] == '[') - (line[end] == ']')
            end += 1
        if depth:
            return None
        if end >= len(line) or line[end] != '(':
            pos = end
            continue
        start = end + 1
        end = start
        depth = 1
        angle = False
        while end < len(line) and depth:
            char = line[end]
            if char == '\\':
                end += 2
                continue
            if char == '<' and end == start:
                angle = True
            elif char == '>' and angle:
                angle = False
            elif not angle:
                depth += (char == '(') - (char == ')')
            end += 1
        if depth or angle:
            return None
        target = line[start:end - 1].strip()
        if target.startswith('<'):
            close = target.find('>')
            if close < 0:
                return None
            target = target[1:close]
        else:
            target = re.sub(r'\s+["\'].*["\']\s*$', '', target)
        try:
            parsed = urlsplit(target)
        except ValueError:
            return None
        if not parsed.scheme and not parsed.netloc and parsed.path:
            links.append(unquote(parsed.path).replace('\\(', '(').replace('\\)', ')'))
        pos = end
    return links


def exists_or_unknown(parent, link):
    try:
        return (parent / Path(link).expanduser()).exists()
    except (OSError, RuntimeError, ValueError):
        return True


def prune_index(path):
    if path.is_symlink():
        return 0
    try:
        text = path.read_bytes().decode('utf-8')
    except FileNotFoundError:
        return 0
    lines = text.splitlines(keepends=True)
    kept = []
    for line in lines:
        links = local_links(line)
        if not links or any(exists_or_unknown(path.parent, link) for link in links):
            kept.append(line)
    if len(kept) != len(lines):
        descriptor, name = tempfile.mkstemp(prefix='.memory-index-', dir=str(path.parent))
        temporary = Path(name)
        try:
            with os.fdopen(descriptor, 'w', encoding='utf-8', newline='') as stream:
                stream.write(''.join(kept))
            os.chmod(temporary, path.stat().st_mode & 0o777)
            if not path.is_symlink() and path.read_bytes().decode('utf-8') == text:
                os.replace(temporary, path)
                return len(lines) - len(kept)
        finally:
            if temporary.exists():
                temporary.unlink()
    return 0


def nightly(store, active=()):
    def maintain(topic_id):
        if topic_id not in active:
            prune_index(directory(store, topic_id) / 'MEMORY.md')
        check(store, topic_id)
    sweeps.run(store, 'maintenance', [topic['id'] for topic in store.topics()], maintain, area='memory',
               refs=lambda topic_id: {'topic': topic_id})
