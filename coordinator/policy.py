"""Instructions for the persistent coordinator and background workers."""

import errno
import os
from pathlib import Path
import re
import stat

TAG_NAME = re.compile(r'[a-z0-9][a-z0-9_-]{0,63}')


def valid_tag(name):
    return isinstance(name, str) and TAG_NAME.fullmatch(name) is not None


def valid_tags(names):
    return isinstance(names, list) and all(valid_tag(name) for name in names) and len(set(names)) == len(names)


def chat_tags(store, topic):
    names = store.get('chat_tags:' + topic, []) if topic else []
    return names if valid_tags(names) else []


def op_chat_tags(store, ctx, topic, tags=None):
    from .control_api import Refused, Result
    if ctx.source_worker is not None:
        raise Refused('Only the coordinator or owner can use chat tags from the local CLI.')
    if tags is not None:
        with store.transaction():
            store.put('chat_tags:' + topic, tags)
    names = chat_tags(store, topic)
    return Result(True, 'Chat tags: ' + (', '.join(names) or 'none'), {'topic': topic, 'tags': names})


def chat_policy(store, key):
    return join_layers(layers(store, key, 'coordinator'))


COORDINATOR_MODEL = 'sonnet'
PARENT_EFFORT = 'medium'


def _clean(text):
    import re
    return re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)


def rule_lines(text):
    return sum(bool(line.strip()) and not line.lstrip().startswith('#') for line in _clean(text).splitlines())


def _without_coordinators(text):
    import re
    lines = []
    level = None
    for line in text.splitlines(keepends=True):
        heading = re.match(r'^(#{1,6})\s+(.+?)\s*#*\s*$', line.strip())
        if heading and level is not None and len(heading.group(1)) <= level:
            level = None
        if heading and level is None and len(heading.group(1)) <= 2 and heading.group(2).casefold() in ('coordinator', 'coordinators'):
            level = len(heading.group(1))
        if level is None:
            lines.append(line)
    return ''.join(lines)


def atomic_rule_write(path, text, *, overwrite=True):
    import tempfile
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=path.parent,
                                         prefix='.rules-', delete=False) as target:
            temporary = Path(target.name)
            os.fchmod(target.fileno(), 0o600)
            target.write(text)
            target.flush()
            os.fsync(target.fileno())
        if overwrite:
            os.replace(temporary, path)
        else:
            os.link(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def read_rule(path):
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        with os.fdopen(descriptor, encoding='utf-8') as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise OSError('Rule file must be a regular file: ' + path.name)
            return stream.read()
    finally:
        os.close(directory)


def _read_layer(store, path, topic, shared=False):
    from . import problems
    cache = getattr(store, '_rule_file_cache', None)
    if cache is None:
        cache = store._rule_file_cache = {}
    try:
        text = _clean(read_rule(personal_path(store) if shared else path))
    except FileNotFoundError:
        cache.pop(str(path), None)
        return ''
    except (OSError, UnicodeError) as error:
        problems.record(store, 'policy', 'rule-file-unreadable',
                        detail=path.name + ': ' + type(error).__name__, topic=topic)
        if isinstance(error, OSError) and error.errno in (errno.ELOOP, errno.ENOTDIR):
            cache.pop(str(path), None)
        return cache.get(str(path), '')
    cache[str(path)] = text
    return text


def _rule_stat(path, dir_fd=None):
    try:
        details = os.stat(path, dir_fd=dir_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(details.st_mode):
        raise OSError(errno.ELOOP if stat.S_ISLNK(details.st_mode) else errno.EINVAL,
                      'Rule file must be a regular file', str(path))
    return details


def personal_path(store, create=True):
    path = store.directory / 'personal.md'
    if create and _rule_stat(path) is None:
        with store.transaction():
            if _rule_stat(path) is None:
                template = Path(__file__).parent / 'defaults' / 'personal.template.md'
                atomic_rule_write(path, template.read_text(encoding='utf-8'))
    return path


def chat_path(store, topic):
    if not topic or '/' in topic or topic in ('.', '..') or '\\' in topic:
        raise ValueError('Invalid topic')
    return Path(store.directory) / 'chats' / topic / 'chat.md'


def instructions(store, key, audience, create_scratch=True):
    return (torii_instructions(store, key, audience, create_scratch), join_layers(layers(store, key, audience)))


def torii_instructions(store, key, audience, create_scratch=True):
    import shlex
    import sys
    template = (Path(__file__).parent / 'defaults' / 'Torii.md').read_text()
    ctl = 'PYTHONPATH=%s %s -m coordinator --state-dir %s ctl call' % (
        shlex.quote(str(Path(__file__).resolve().parents[1])), shlex.quote(sys.executable),
        shlex.quote(str(store.directory)))
    fixed = _clean(template).replace('{ctl}', ctl)
    if audience == 'worker':
        sections = {'Tools', 'Projects and service', 'When you are a worker', 'Secrets', 'Job folders', 'Deleting files'}
        parts = fixed.split('\n## ')
        fixed = '\n\n'.join(('## Tools\n\nWorkers use the CLI: `' + ctl + ' <op> key=value …`. The coordinator handles Telegram delivery.' if part.split('\n', 1)[0] == 'Tools' else '## ' + part) for part in parts[1:] if part.split('\n', 1)[0] in sections)
        fixed = fixed.replace('Ask for a credential by name with `secret.ask`.', 'Run `secret.list` to see stored names. Ask the coordinator to declare a stored name on '
                              'your job, or to ask the owner for a new one.')
        fixed = fixed.replace('Declare it on the job just before the step that needs it, and clear it after. ', '')
    from .chat_mcp import settings_text
    from .loadout import topic_for_key
    fixed += settings_text(store, topic_for_key(store, key))
    if audience == 'coordinator':
        import json
        from . import parent
        from .loadout import topic_for_key
        topic = topic_for_key(store, key)
        visible = [dict(item) for item in store.topics() if item['enabled'] and
                   (topic == store.get('coordinator_home_topic') or item['id'] == topic)]
        details = [{'id': item['id'], 'name': item['name'], 'cwd': item['cwd']} for item in visible]
        fixed += ('\nRegistered channels: ' + json.dumps(details, separators=(',', ':')) +
                  '\nConnected agents: ' + ', '.join(parent.connected_agents(store)))
        from .close_saves import scratch
        fixed += '\nChat scratch: ' + str(scratch(store, topic=topic, create=create_scratch))
    return fixed


def join_layers(parts):
    return '\n\n'.join(text.strip() for _, text in parts if text.strip())


def layers(store, key, audience):
    from .loadout import render, topic_for_key
    topic = topic_for_key(store, key)
    rules = [('personal.md', _read_layer(store, store.directory / 'personal.md', topic, shared=True))]
    for tag in chat_tags(store, topic):
        filename = 'tags/' + tag + '.md'
        rules.append((filename, _read_layer(store, store.directory / filename, topic)))
    rules.append(('chat.md', _read_layer(store, chat_path(store, topic), topic) if topic else ''))
    if audience == 'worker':
        rules = [(name, _without_coordinators(text)) for name, text in rules]
    rules.append(('Swaps', render(store, topic)))
    return rules
