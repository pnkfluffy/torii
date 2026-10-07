"""Setup guidance derived from saved state; no provider connection is required."""

from pathlib import Path
import hashlib
import logging
import re
import subprocess

from .host_os import trash_directory


COMMAND_MENU = [
    {'command': command, 'description': description}
    for command, description in (
        ('accounts', 'Usage, sign-ins, resets, models'),
        ('projects', "Projects, new projects, this topic's folder"),
        ('tldr', 'Catch up on this topic'), ('health', 'Running agents and system load'),
        ('secrets', 'Stored keys: rotate, revoke, ask again'),
        ('ping', 'Check that Torii is listening'), ('help', 'All commands'),
    )
]


PROJECT_STOP_WORDS = frozenset(('a', 'an', 'and', 'are', 'be', 'build', 'can', 'could', 'create', 'do', 'for',
                              'i', 'is', 'it', 'make', 'me', 'my', 'of', 'please', 'some', 'that', 'the', 'this',
                              'to', 'want', 'with', 'would', 'you'))


def suggested_name(text):
    words = [word for word in re.findall(r'[a-z0-9]+(?:[._-][a-z0-9]+)*', text.casefold())
             if word not in PROJECT_STOP_WORDS][:3]
    return re.sub(r'[^a-z0-9]+', '-', '-'.join(words)).strip('-')[:80].rstrip('-') or 'project'


def first_project_question(store, topic_id):
    state = store.get(_key(topic_id), {}) or {}
    if state.get('name_armed'):
        return ('Send a project name on one line, up to 40 characters, as your next message. '
                'Anything else you send now is added to your request.')
    return ("Name this project? Tap Use '" + state['suggestion'] + "' or Type a name. "
            'Anything else you send now is added to your request.')


def first_project(store, topic_id, message):
    text = message.get('text') or ''
    if (topic_id != store.get('control_topic') or not text.strip() or text.startswith('/')
            or any(key.startswith('forum_topic_') for key in message)):
        return None
    topic = store.topic(topic_id)
    if topic['cwd'] or topic['session'] or topic['source_pid']:
        return None
    state = store.get(_key(topic_id), {}) or {}
    if state.get('stage') == 'creating':
        store._save_owner_message(topic_id, message['message_id'], text)
        return 'held'
    if state.get('stage') in ('new', 'existing'):
        return None
    from .control_ui import setup_report
    if state.get('stage') == 'first':
        if state.get('name_armed'):
            if len(text) <= 40 and '\n' not in text and '\r' not in text:
                reply = _create(store, topic_id, text.strip())
                if (store.get(_key(topic_id), {}) or {}).get('stage') != 'creating' and not store.topic(topic_id)['cwd']:
                    reply += '\nSend another name on one line, up to 40 characters, or tap the suggested name.'
                setup_report(store, topic_id, reply, reply_to=message['message_id'])
                return 'control'
            store.put(_key(topic_id), dict(state, name_armed=False))
        store._save_owner_message(topic_id, message['message_id'], text)
        setup_report(store, topic_id, first_project_question(store, topic_id), reply_to=message['message_id'])
        return 'held'
    store._save_owner_message(topic_id, message['message_id'], text)
    name = suggested_name(text)
    store.put(_key(topic_id), {'stage': 'first', 'suggestion': name})
    setup_report(store, topic_id, first_project_question(store, topic_id), reply_to=message['message_id'])
    return 'held'


def project_root(store):
    return Path(store.get('projects_root') or Path.home() / 'Projects' / 'torii').expanduser().resolve()


def _key(topic_id):
    return 'project_setup:' + topic_id


def configured_root(store):
    """The owner's project root, or None when it has never been set."""
    saved = store.get('projects_root')
    try:
        return Path(saved).expanduser().resolve() if saved else None
    except (OSError, ValueError, RuntimeError):
        return None


def _bound_topics(store):
    """Resolved folder to topic name, so the catalog can mark what is already in use."""
    bound = {}
    for topic in store.topics():
        if topic['cwd']:
            try:
                bound[str(Path(topic['cwd']).resolve())] = topic['name']
            except (OSError, ValueError, RuntimeError):
                continue
    return bound


def _catalog(store):
    """Immediate subdirectories of the project root. Folders outside it are never listed."""
    root = configured_root(store)
    if root is None:
        return []
    bound = _bound_topics(store)
    entries = []
    try:
        children = sorted(root.iterdir(), key=lambda child: (child.name.casefold(), child.name))
    except (OSError, ValueError, RuntimeError):
        return []
    for path in children[:500]:
        if path.name.startswith('.'):
            continue
        try:
            if not path.is_dir():
                continue
            resolved = str(path.resolve())
        except (OSError, ValueError, RuntimeError):
            continue
        entries.append({'name': path.name, 'path': resolved, 'bound': bound.get(resolved)})
        if len(entries) >= 100:
            break
    return entries


def projects_guide(store, topic_id=None):
    from .health import _plain
    root = configured_root(store)
    lines = ['**Projects** in ' + _plain(root) if root else
             '**Projects** · new ones go in ' + _plain(project_root(store))]
    entries = _catalog(store)
    lines += ['• ' + _plain(entry['name']) + (' — linked to ' + _plain(entry['bound']) if entry['bound'] else '')
              for entry in entries[:20]]
    if not entries:
        lines.append('No project folders there yet.')
    if len(entries) > 20:
        lines.append(f'…and {len(entries) - 20} more.')
    if topic_id and topic_id != store.get('control_topic'):
        topic = store.topic(topic_id) or {}
        if topic.get('cwd'):
            state = 'accepting work' if topic.get('enabled') else 'disabled'
            lines += ['', f"This topic: {_plain(topic['name'])} · {_plain(topic['cwd'])} · {state}"]
        elif _can_bind(store, topic_id):
            lines += ['', "This topic isn't linked to a project yet."]
    return '\n'.join(lines)


def _execution_hint(store):
    if store.get('pair_only', False):
        return '\nThe host is in pairing-only mode. Agent execution must be enabled on the host before work can run.'
    if store.get('execution') == 'pairing':
        return '\nQueued work runs once Claude or ChatGPT is connected.'
    return ''


def _account_hint(store):
    from .setup_flow import chatgpt_ready
    if chatgpt_ready(store):
        return ''
    accounts = store.get('accounts', {}) or {}
    if not accounts:
        return '\nNo Claude or ChatGPT account yet. Open /accounts to sign one in. Project setup works before sign-in.'
    if not any(account.get('enabled') is True for account in accounts.values()):
        return '\nNo Claude account is enabled. Use /accounts to finish account setup before sending work.'
    return ''


def _existing_prompt(store, topic_id, page=0):
    entries = _catalog(store)
    page = min(max(page, 0), max(0, (len(entries) - 1) // 8))
    store.put(_key(topic_id), {'stage': 'existing', 'choices': entries, 'page': page})
    if not entries:
        return 'No project folders in ' + str(project_root(store)) + '.\nChoose New project below.'
    return ('Choose a project below.\nFolder: ' + str(project_root(store)) +
            f'\nPage {page + 1} of {(len(entries) + 7) // 8}.\n'
            'You can also send its number or name, or an absolute folder path.')


def _can_bind(store, topic_id):
    topic = store.topic(topic_id) or {}
    return (topic and not topic.get('cwd') and not topic.get('session') and not topic.get('source_pid')
            and not store.db.execute('SELECT 1 FROM tasks WHERE topic=? LIMIT 1', (topic_id,)).fetchone())


def _activate(store, topic_id, path, name):
    if not _can_bind(store, topic_id) and (store.get(_key(topic_id), {}) or {}).get('stage') != 'project_new':
        return setup_guide(store, topic_id)
    if topic_id == store.get('control_topic') or (store.get(_key(topic_id), {}) or {}).get('stage') == 'project_new':
        queued = store.get('group_projects', [])
        if any(entry['name'].casefold() == name.casefold() for entry in queued):
            return 'A project with that name is already being created.'
        queued.append({'name': name, 'cwd': str(path), 'source': topic_id,
                       'release_held': topic_id == store.get('control_topic') and not store.topic(topic_id)['cwd']})
        store.put('group_projects', queued)
        store.put(_key(topic_id), {'stage': 'creating'})
        return 'Creating the ' + name + ' topic.'
    store.bind(topic_id, path, name, provider='claude', enabled=True)
    name = store.topic(topic_id)['name']
    store.put(_key(topic_id), None)
    logging.getLogger(__name__).info('project activated topic=%s', topic_id)
    return (f'{name} is ready. This channel is linked and accepts work.\nProject folder: {path}\n'
            'Send the first request here. It will start a new worker conversation in this folder. '
            'Later messages will continue that conversation. /projects shows this topic. /help lists commands.'
            + _execution_hint(store) + _account_hint(store))


def _create(store, topic_id, name):
    if not name or len(name) > 80 or any(character in name for character in '/\\\n\r\x00') or name in ('.', '..'):
        return 'Send a project name of 1–80 characters, without slashes. Example: My Website.'
    slug = re.sub(r'[^a-z0-9]+', '-', name.casefold()).strip('-')
    if not slug:
        slug = 'project-' + hashlib.sha256(name.encode('utf-8')).hexdigest()[:10]
    root = project_root(store)
    if store.get('projects_root') and not root.is_dir():
        return 'The configured project root is unavailable. Restore it or choose another with Projects folder in /projects.'
    path = root / slug
    try:
        root.mkdir(parents=True, exist_ok=True)
        path.mkdir(mode=0o700)
        created = path.stat()
    except FileExistsError:
        return f'That folder already exists: {path}\nUse /setup use {path} to select it, or send another name.'
    except OSError:
        return 'The host could not create the project folder. Choose an accessible root with Projects folder in /projects, then retry.'
    git = ['git', '-C', str(path)]
    try:
        subprocess.run(git + ['init', '-q'], check=True, capture_output=True, text=True, timeout=30)
        identity = []
        for key, fallback in (('user.name', 'Torii'), ('user.email', 'torii@localhost')):
            value = subprocess.run(git + ['config', '--get', key],
                                   capture_output=True, text=True, timeout=30)
            if value.returncode not in (0, 1):
                value.check_returncode()
            if not value.stdout.strip():
                identity.extend(['-c', key + '=' + fallback])
        subprocess.run(git + identity + ['-c', 'core.hooksPath=/dev/null', '-c', 'commit.gpgSign=false',
                                         'commit', '--allow-empty', '-qm', 'Initial commit'],
                       check=True, capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.SubprocessError) as error:
        stderr = getattr(error, 'stderr', '') or ''
        if isinstance(stderr, bytes):
            stderr = stderr.decode('utf-8', errors='replace')
        logging.getLogger(__name__).error('project Git setup failed path=%s stderr=%s error=%s',
                                         path, stderr, error)
        detail = (stderr.strip() or str(error)).splitlines()[0][:300]
        cleanup = f'The project folder was left at {path}. Check its contents before retrying.'
        try:
            current = path.lstat()
            entries = list(path.iterdir())
            git_dir = path / '.git'
            if ((current.st_dev, current.st_ino) == (created.st_dev, created.st_ino)
                    and not path.is_symlink()
                    and (not entries or (entries == [git_dir] and git_dir.is_dir() and not git_dir.is_symlink()))):
                trash = trash_directory()
                trash.mkdir(mode=0o700, parents=True, exist_ok=True)
                target = trash / path.name
                suffix = 1
                while target.exists() or target.is_symlink():
                    target = trash / (path.name + '-' + str(suffix))
                    suffix += 1
                path.rename(target)
                cleanup = 'The new folder was moved to Trash. Retry with the same project name.'
        except OSError:
            logging.getLogger(__name__).exception('project Git failure cleanup failed path=%s', path)
        return f'Git setup failed: {detail}\n{cleanup}'
    if not store.get('projects_root'):
        store.put('projects_root', str(root))
    return _activate(store, topic_id, path, name)


def _existing_path(value):
    try:
        path = Path(value).expanduser()
        if path.is_absolute() and path.is_dir():
            return path.resolve()
    except (OSError, ValueError, RuntimeError):
        pass
    return None


def _select(store, topic_id, target):
    state = store.get(_key(topic_id), {}) or {}
    choices = state.get('choices', [])
    match = None
    if target.isascii() and target.isdigit() and len(target) < 4 and 0 < int(target) <= len(choices):
        match = choices[int(target) - 1]
    else:
        matches = [entry for entry in _catalog(store) if entry['name'].casefold() == target.casefold()]
        if len(matches) > 1:
            return 'Several projects have that name. Send the full folder path.'
        match = matches[0] if matches else None
    path = _existing_path(match['path'] if match else target)
    if path is None:
        return 'Project folder not found. Send its absolute path, or use /setup new to create one.'
    path = path.resolve()
    return _activate(store, topic_id, path, match['name'] if match else path.name)


def setup_guide(store, topic_id):
    topic = store.topic(topic_id) or {}
    if not _can_bind(store, topic_id):
        if topic.get('enabled'):
            return (f"Channel: {topic['name']}\nProject folder: {topic['cwd']}\nThis channel is ready. Send work here. "
                    'Its saved conversation will continue. Create another channel for a different project.\n'
                    '/projects lists folders. /projects shows this topic. /help lists commands.'
                    + _execution_hint(store) + _account_hint(store))
        return ('This channel has a saved project or work history and is disabled. Setup will not replace its saved conversation. '
                '/projects shows this topic. Session recovery must verify the saved process before enabling it.')
    state = store.get(_key(topic_id), {}) or {}
    if state.get('stage') == 'first':
        return first_project_question(store, topic_id)
    if state.get('stage') == 'new':
        return 'What is the project name? Example: My Website.\nSend /setup cancel to stop setup.'
    if state.get('stage') == 'existing':
        return _existing_prompt(store, topic_id, state.get('page', 0))
    store.put(_key(topic_id), {'stage': 'choose'})
    return ('Link this channel to a project.\nChoose New project or Existing project below.\n\n'
            'New projects go in ' + str(project_root(store)) + '.\n'
            'You can also type New project or Existing project. /setup returns here.\n'
            '/projects lists folders. /help lists commands.' + _account_hint(store))


def setup_command(store, topic_id, arguments=''):
    if not _can_bind(store, topic_id):
        return setup_guide(store, topic_id)
    parts = arguments.strip().split(maxsplit=1)
    action = parts[0].lower() if parts else ''
    value = parts[1].strip() if len(parts) > 1 else ''
    if action == 'page' and value.isascii() and value.isdigit() and len(value) <= 3:
        return _existing_prompt(store, topic_id, int(value))
    if action == 'back':
        store.put(_key(topic_id), None)
        return setup_guide(store, topic_id)
    if action == 'cancel':
        store.put(_key(topic_id), None)
        return 'Setup cancelled. No agent was started. Send /setup to begin again.'
    if action == 'new':
        if not (store.get(_key(topic_id), {}) or {}).get('stage') == 'first':
            store.put(_key(topic_id), {'stage': 'new'})
        return _create(store, topic_id, value) if value else setup_guide(store, topic_id)
    if action in ('use', 'existing'):
        return _select(store, topic_id, value) if value else _existing_prompt(store, topic_id)
    if action:
        return 'Use /setup, /setup new NAME, /setup use PROJECT_OR_PATH, or /setup cancel.'
    return setup_guide(store, topic_id)


def setup_reply(store, topic_id, text):
    if not _can_bind(store, topic_id):
        return setup_guide(store, topic_id)
    state = store.get(_key(topic_id), {}) or {}
    if state.get('stage') == 'new' and text.strip():
        return _create(store, topic_id, text.strip())
    if state.get('stage') == 'existing' and text.strip():
        return _select(store, topic_id, text.strip())
    choice = text.strip().casefold()
    if choice in ('new', 'new project'):
        return setup_command(store, topic_id, 'new')
    if choice in ('existing', 'existing project'):
        return setup_command(store, topic_id, 'existing')
    return setup_guide(store, topic_id) + '\nChoose the project first, then send your request. No job has been queued.'


def op_projects_list(store, ctx):
    from .control_api import Result
    return Result(True, projects_guide(store, ctx.topic if ctx else None), _catalog(store))


def op_projects_root(store, ctx, path):
    from .control_api import Refused
    folder = _existing_path(path)
    if folder is None:
        raise Refused('Choose an existing absolute folder for the project root. Existing projects will not move.')
    store.put('projects_root', str(folder))
    logging.getLogger(__name__).info('project root changed')
    return 'New projects will be created under ' + str(folder) + '. Existing projects stay where they are.'


def op_setup_new(store, ctx, topic=None, name=''):
    from .control_api import Result
    reply = setup_command(store, topic, 'new ' + name)
    return Result(False, reply, state='failed') if reply.startswith('Git setup failed:') else reply


def op_setup_use(store, ctx, topic=None, project=''):
    return setup_command(store, topic, 'use ' + project)


def op_setup_cancel(store, ctx, topic=None):
    return setup_command(store, topic, 'cancel')
