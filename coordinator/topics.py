"""Durable service-owned chat creation, renaming, and owner-confirmed retirement."""

import asyncio
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import time

from . import host_os, onboarding
from .control_api import Refused, Result
from .telegram import TelegramError


def lifecycle(store, topic):
    return store.get('topic_lifecycle:' + topic) or {}


def _requests(store, op):
    return [dict(row) for row in store.db.execute(
        "SELECT * FROM service_requests WHERE op=? AND state NOT IN ('done','failed','refused','cancelled')",
        (op,))]


def _save(store, request, progress, state='queued'):
    store.db.execute('UPDATE service_requests SET state=?,result=?,updated=? WHERE id=?',
                     (state, json.dumps(progress), time.time(), request['id']))


def _asking(store, ctx, message):
    if ctx.source == 'mcp' and ctx.topic is None:
        raise Refused('This tool needs the asking parent channel. Restart the parent to load its scoped MCP tools.')
    if message is None:
        if ctx.source == 'mcp':
            raise Refused('Pass message=N for the owner message that asked.')
        target = store.get('coordinator_home_topic')
    else:
        row = store.db.execute('SELECT topic,kind FROM messages WHERE id=?', (message,)).fetchone()
        if not row or row['kind'] != 'owner':
            raise Refused('message=N must identify an owner message.')
        if ctx.topic and ctx.topic != row['topic']:
            raise Refused('The owner message came from another channel.')
        target = row['topic']
    config = store.topic(target) if target else None
    if not config or config['chat'] != store.chat():
        raise Refused('Choose an asking channel in the current Telegram group.')
    return target


def queue(store, ctx, op, params):
    if op == 'topic.close' and 'folder' in params and params.get('message') is None:
        raise Refused('Pass message=N for the owner message that confirms folder=trash or keep.')
    asking = _asking(store, ctx, params.get('message'))
    if op == 'topic.create':
        if params.get('provider') == 'opencode':
            from .parent_state import opencode_pick_problem
            problem = opencode_pick_problem(store, 'coordinator:new')
            if problem:
                raise Refused(problem)
        name = params['name']
        if len(name) > 80 or any(c in name for c in '/\\\n\r\x00') or name in ('.', '..'):
            raise Refused('Send a project name of 1–80 characters, without slashes. Example: My Website.')
        if any(json.loads(row['params'])['name'].casefold() == name.casefold()
               for row in _requests(store, op)):
            raise Refused('A chat with that name is already being created.')
        if 'delegation' in params:
            from .controls import _chat_policy_text
            params['delegation'] = _chat_policy_text(params['delegation'])
        params = dict(params, settings={})
    else:
        _check_close(store, params['topic'])
        pending = [row for row in _requests(store, op) if json.loads(row['params'])['topic'] == params['topic']]
        if pending and ('folder' not in params or any(
                row['state'] != 'confirming' or 'folder' in json.loads(row['params']) for row in pending)):
            raise Refused('This chat already has a close waiting for the owner.')
        if 'folder' in params:
            try:
                snap = snapshot(store, params['topic'])
            except (OSError, subprocess.SubprocessError) as error:
                raise Refused(str(error)) from None
            if params['folder'] == 'trash' and not snap['trash_allowed']:
                raise Refused('This folder cannot be moved to Trash. Choose folder=keep.')
        if not store.db.in_transaction:
            store.db.execute('BEGIN IMMEDIATE')
        _check_close(store, params['topic'])
        current = [row for row in _requests(store, op) if json.loads(row['params'])['topic'] == params['topic']]
        if current != pending:
            raise Refused('The pending close changed. Try closing this chat again.')
        for previous in pending:
            _cancel_card(store, previous)
    request = store.service_request(op, dict(params, _topic=asking, _chat=store.chat()))
    notice = ('Chat creation queued. ' if op == 'topic.create' else
              'Chat close queued. ' if 'folder' in params else 'Close confirmation queued. ')
    return Result(True, notice +
                  'Torii will send the result here.', {'request': request}, state='queued')


def _cancel_card(store, request):
    from .envelopes import card_message
    cards = store.db.execute("SELECT key FROM settings WHERE key LIKE 'topic_close_card:%' "
                             "AND json_extract(value,'$.request')=?", (request['id'],)).fetchall()
    for setting in cards:
        prefix = 'torii:' + setting['key'][len('topic_close_card:'):] + ':'
        for card in store.db.execute('SELECT id,topic,text FROM outbox WHERE instr(reply_markup,?)>0',
                                     (prefix,)).fetchall():
            store.db.execute('UPDATE outbox SET reply_markup=NULL WHERE id=?', (card['id'],))
            message = card_message(store, card['id'])
            if message:
                store.enqueue_report(card['topic'], card['text'], kind='status', edit=message,
                                     reply_markup={'inline_keyboard': []})
    progress = json.loads(request['result'] or '{}')
    _save(store, request, dict(progress, text='Chat close cancelled. An owner message confirmed a new close.'), 'cancelled')


def _check_close(store, topic, allow_workers=False):
    config = store.topic(topic)
    if not config or config['chat'] != store.chat():
        raise Refused('Unknown chat.')
    if config['thread'] <= 1:
        raise Refused('The General topic cannot be closed.')
    if topic in (store.get('coordinator_home_topic'), store.get('coordinator_session_topic')):
        raise Refused('The home chat cannot be closed.')
    if lifecycle(store, topic):
        raise Refused('This chat is already closing or closed.')
    workers = [dict(row) for row in store.db.execute(
        "SELECT id,provider,status FROM workers WHERE topic=? AND (status IN ('queued','running','parked') "
        "OR status LIKE 'waiting%') ORDER BY id", (topic,))]
    if workers and not allow_workers:
        raise Refused('Unfinished workers: ' + ', '.join('%s %s %s' %
                      (row['id'], row['provider'], row['status']) for row in workers) + '.')
    return config


def _git(path, *args, allowed=(0,)):
    result = subprocess.run(['git', '-C', str(path), *args], capture_output=True, timeout=30)
    if result.returncode not in allowed:
        raise Refused('The folder git state could not be checked.')
    return result.stdout.decode('utf-8', errors='replace').rstrip('\n')


def _shared_folders(store, topic, resolved):
    shared = []
    for other in store.topics():
        if other['id'] == topic or not other['cwd'] or resolved is None:
            continue
        candidate = Path(other['cwd']).resolve()
        if candidate == resolved or candidate in resolved.parents or resolved in candidate.parents:
            shared.append({'id': other['id'], 'name': other['name'], 'cwd': other['cwd']})
    return shared


def snapshot(store, topic):
    config = _check_close(store, topic, allow_workers=True)
    raw = config['cwd']
    path = Path(raw) if raw else None
    resolved = path.resolve() if path else None
    root = onboarding.project_root(store).resolve()
    shared = _shared_folders(store, topic, resolved)
    metadata = path.lstat() if path and (path.exists() or path.is_symlink()) else None
    folder = bool(path and path.is_dir() and not path.is_symlink())
    result = {'topic': topic, 'name': config['name'], 'cwd': raw, 'enabled': config['enabled'],
              'shared': shared, 'identity': [metadata.st_dev, metadata.st_ino] if metadata else None,
              'trash_allowed': bool(folder and root in resolved.parents and not shared),
              'workers': [dict(row) for row in store.db.execute(
                  "SELECT id,provider,status FROM workers WHERE topic=? AND (status IN ('queued','running','parked') "
                  "OR status LIKE 'waiting%') ORDER BY id", (topic,))],
              'jobs': [dict(row) for row in store.db.execute(
                  "SELECT id,number,title,notes FROM tasks WHERE topic=? AND status IN ('open','waiting') ORDER BY number", (topic,))]}
    git = {'repository': False}
    if folder and _git(path, 'rev-parse', '--is-inside-work-tree', allowed=(0, 128)) == 'true':
        status = _git(path, 'status', '--porcelain=v1', '-z', '--untracked-files=all')
        digest = hashlib.sha256(_git(path, 'diff', '--binary', 'HEAD', allowed=(0, 128)).encode())
        digest.update(_git(path, 'diff', '--binary', '--cached', allowed=(0, 128)).encode())
        for name in _git(path, 'ls-files', '--others', '--exclude-standard', '-z').split('\x00'):
            entry = path / name
            if name:
                digest.update(name.encode())
                if entry.is_symlink():
                    digest.update(str(entry.readlink()).encode())
                elif entry.is_file():
                    with entry.open('rb') as stream:
                        for chunk in iter(lambda: stream.read(65536), b''):
                            digest.update(chunk)
        git = {'repository': True, 'branch': _git(path, 'symbolic-ref', '--short', '-q', 'HEAD', allowed=(0, 1)),
               'head': _git(path, 'rev-parse', '--verify', 'HEAD', allowed=(0, 128)),
               'status': status, 'content': digest.hexdigest(),
               'refs': _git(path, 'show-ref', allowed=(0, 1)),
               'unpublished': _git(path, 'rev-list', '--branches', 'HEAD', '--not', '--remotes', allowed=(0, 128)).splitlines()}
    result['git'] = git
    return result


def _card(store, request, progress, prefix=''):
    from .control_ui import emit, _key
    params = json.loads(request['params'])
    snap = snapshot(store, params['topic'])
    revision = progress.get('revision', 0) + 1
    jobs_heading = 'Open and waiting jobs to mark dropped:' if store.get('waits_enabled', False) else 'Open jobs to mark dropped:'
    text = (prefix + 'Close "' + snap['name'] + '"?\nIts agent stops. The Telegram topic and all its messages '
            'will be deleted. This cannot be undone.\n\n' + jobs_heading + '\n' +
            ('\n'.join('#%s %s' % (job['number'], job['title']) for job in snap['jobs']) or 'None.') +
            '\n\nFolder: ' + (snap['cwd'] or 'No folder linked.') + '\nOther chats using this folder or a folder '
            'inside or above it: ' + (', '.join(row['name'] + ' (' + row['id'] + ')' for row in snap['shared']) or 'None.') + '\n')
    git = snap['git']
    if git['repository']:
        text += ('Git branch: ' + (git['branch'] or 'detached HEAD') + '\nUncommitted or untracked files: ' +
                 (git['status'].replace('\x00', '\n').rstrip() or 'None.') +
                 '\nCommits not on any remote: ' + str(len(git['unpublished'])) +
                 ('\n' + '\n'.join(git['unpublished']) if git['unpublished'] else ''))
    else:
        text += 'This folder is not a git repository.'
    if snap['workers']:
        text += '\nUnfinished workers must finish before closing: ' + ', '.join(
            '%s %s %s' % (row['id'], row['provider'], row['status']) for row in snap['workers'])
    choices = ([('Close and move folder to Trash', 'trash')] if snap['trash_allowed'] else [])
    choices += [('Close, keep folder', 'keep'), ('Cancel', 'cancel')]
    if snap['workers']:
        choices = [('Cancel', 'cancel')]
    rows = [[(label, {'topic_close': request['id'], 'choice': choice, 'revision': revision})]
            for label, choice in choices]
    emit(store, params['_topic'], text, rows)
    ui = store.get(_key(params['_topic']))
    store.put('topic_close_card:' + ui['token'], {'request': request['id'], 'revision': revision,
                                                'topic': params['_topic'], 'actions': ui['actions']})
    _save(store, request, dict(phase='card', snapshot=snap, revision=revision), 'confirming')


def accept_close(store, request_id, choice, revision):
    request = store.db.execute('SELECT * FROM service_requests WHERE id=? AND op=?',
                               (request_id, 'topic.close')).fetchone()
    if not request or request['state'] != 'confirming':
        return 'already_done'
    progress = json.loads(request['result'])
    if revision != progress['revision']:
        return 'already_done'
    if choice == 'cancel':
        store.enqueue_report(json.loads(request['params'])['_topic'], 'Chat close cancelled.')
        _save(store, request, dict(progress, text='Chat close cancelled.'), 'cancelled')
        return 'control_callback'
    if choice not in ('keep', 'trash'):
        return 'stale_callback'
    _confirm_close(store, request, progress, choice)
    return 'control_callback'


def _confirm_close(store, request, progress, choice):
    params = json.loads(request['params'])
    try:
        current = snapshot(store, params['topic'])
        if 'folder' in params:
            _check_close(store, params['topic'])
            if choice == 'trash' and not current['trash_allowed']:
                raise Refused('This folder cannot be moved to Trash. Choose folder=keep.')
        elif current != progress['snapshot'] or current['workers'] or choice == 'trash' and not current['trash_allowed']:
            _card(store, request, progress, 'The chat or folder changed. Review this fresh confirmation.\n\n')
            return
    except Refused as error:
        text = str(error) + ' Nothing was closed. Request a fresh confirmation when ready.'
        if 'folder' in params:
            _finish(store, request, progress, text, 'refused')
        else:
            store.enqueue_report(params['_topic'], text)
            _save(store, request, dict(progress, text=str(error)), 'refused')
        return
    store.db.execute('UPDATE topics SET enabled=0 WHERE id=?', (params['topic'],))
    store.put('topic_lifecycle:' + params['topic'], {'state': 'closing', 'request': request['id']})
    _save(store, request, dict(progress, phase='disabled', choice=choice, snapshot=current))


def hold_message(store, topic, message):
    if lifecycle(store, topic):
        return 'topic_closed'
    if store.topic(topic) and store.topic(topic)['cwd']:
        return None
    for request in _requests(store, 'topic.create'):
        progress = json.loads(request['result'] or '{}')
        if progress.get('phase') == 'creating' or progress.get('topic') == topic and progress.get('phase') == 'created':
            key = 'topic_create_held:' + topic
            store.put(key, (store.get(key) or []) + [message])
            return 'topic_creating'
    return None


def release_messages(store):
    creating = any(json.loads(request['result'] or '{}').get('phase') in ('creating', 'created')
                   for request in _requests(store, 'topic.create'))
    for row in list(store.db.execute("SELECT key,value FROM settings WHERE key LIKE 'topic_create_held:%'")):
        topic = row['key'][len('topic_create_held:'):]
        if creating and (not store.topic(topic) or not store.topic(topic)['cwd']):
            continue
        for message in json.loads(row['value']) or []:
            text = message.get('text', '')
            store._accept_topic_message(message, topic, message['message_thread_id'], text, text.split(' ', 1)[0])
        store.put(row['key'], [])


def _trash_plan(path):
    trash = host_os.trash_directory()
    trash.mkdir(mode=0o700, parents=True, exist_ok=True)
    target = trash / path.name
    suffix = 1
    while target.exists() or target.is_symlink():
        target = trash / (path.name + '-' + str(suffix))
        suffix += 1
    return str(target)


def _move_folder(progress):
    path, target = Path(progress['cwd']), Path(progress['trash'])
    identity = progress['identity']
    if not path.exists() and target.is_dir() and not target.is_symlink():
        stat = target.stat()
        if [stat.st_dev, stat.st_ino] == identity:
            return
    stat = path.lstat()
    if path.is_symlink() or not path.is_dir() or [stat.st_dev, stat.st_ino] != identity:
        raise Refused('The folder identity changed. It was kept.')
    if target.exists() or target.is_symlink():
        raise Refused('The reserved Trash destination is now occupied. The folder was kept.')
    path.rename(target)


def _finish(store, request, progress, text, state='done'):
    params = json.loads(request['params'])
    asking = params['_topic']
    if lifecycle(store, asking):
        asking = store.get('coordinator_home_topic')
    store.enqueue_report(asking, text)
    store.message_event(asking, text)
    _save(store, request, dict(progress, phase='finished', text=text), state)


async def rename(service, request):
    store = service.store
    params = json.loads(request['params'])
    row = store.topic(params['topic'])
    if params['_chat'] != store.chat() or not row or row['chat'] != store.chat():
        with store.db:
            _finish(store, request, {}, 'The Telegram group changed. The rename was refused.', 'refused')
        return
    if lifecycle(store, params['topic']):
        with store.db:
            _finish(store, request, {}, 'This chat is closing or closed. The rename was refused.', 'refused')
        return
    store.service_request_settle(request['id'], 'sending')
    try:
        await service.telegram.call('editForumTopic', chat_id=row['chat'], message_thread_id=row['thread'],
                                    name=params['name'])
    except TelegramError as error:
        if error.reason != 'topic_not_modified':
            if error.code == 'network-or-invalid-response' or isinstance(error.code, int) and error.code >= 500:
                store.service_request_settle(request['id'], 'queued')
                return
            reasons = {'topic_manage_required': 'The bot needs can_manage_topics permission.',
                       'topic_name_invalid': 'Telegram refused the topic name.',
                       'thread_not_found': 'Telegram could not find the topic.',
                       'topic_gone': 'The Telegram topic was deleted or is invalid.',
                       'topics_off': 'Telegram forum topics are unavailable in this group.'}
            if error.code == 429:
                wait = f'in {error.retry_after} s' if error.retry_after is not None else 'later'
                text = f'Telegram rate-limited the rename; try again {wait}.'
            else:
                text = f'Telegram topic rename failed ({error.code}).'
                if error.reason in reasons:
                    text += ' ' + reasons[error.reason]
            text += ' The saved label is unchanged.'
            with store.db:
                _finish(store, request, {}, text, 'refused')
            return
    with store.db:
        store.db.execute('UPDATE topics SET name=?,telegram_title=? WHERE id=?',
                         (params['name'], params['name'], params['topic']))
        _finish(store, request, {}, f'Telegram topic renamed: {row["name"]} → {params["name"]}.')


async def advance(service, request):
    store = service.store
    params = json.loads(request['params'])
    progress = json.loads(request['result'] or '{}')
    if params['_chat'] != store.chat():
        with store.db:
            text = 'The Telegram group changed. The request was refused.'
            if request['op'] == 'topic.close' and progress.get('phase') is None and 'folder' in params:
                text += ' Nothing was closed.'
            _finish(store, request, progress, text, 'refused')
        return
    if request['op'] == 'topic.create':
        await _create(service, request, params, progress)
    else:
        await _close(service, request, params, progress)


async def _create(service, request, params, progress):
    store = service.store
    phase = progress.get('phase')
    try:
        if phase is None:
            if params.get('cwd'):
                path = Path(params['cwd'])
            else:
                path = onboarding.project_root(store) / onboarding._project_slug(params['name'])
                with store.db:
                    _save(store, request, {'phase': 'folder_intent', 'cwd': str(path), 'owned': False})

                def on_created(folder, stat):
                    with store.db:
                        _save(store, request, {'phase': 'folder_initializing', 'cwd': str(folder), 'owned': True,
                                              'identity': [stat.st_dev, stat.st_ino]})

                path = onboarding._create_folder(store, params['name'], on_created)
            if not isinstance(path, Path):
                with store.db:
                    _save(store, request, {})
                raise Refused(path)
            if not path.is_dir():
                raise Refused('Project directory does not exist.')
            stat = path.stat()
            with store.db:
                _save(store, request, {'phase': 'folder', 'cwd': str(path.resolve()),
                      'owned': not bool(params.get('cwd')), 'identity': [stat.st_dev, stat.st_ino]})
        elif phase in ('folder_intent', 'folder_initializing'):
            raise Refused('Project folder creation was interrupted. No Telegram topic was created. ' +
                          ('The owned folder will be moved to Trash.' if progress.get('owned') else
                           'Any folder without a saved ownership record was left in place for inspection.'))
        elif phase == 'folder':
            with store.db:
                _save(store, request, dict(progress, phase='creating'))
            thread = await service.telegram.create_topic(params['_chat'], params['name'])
            with store.db:
                _save(store, request, dict(progress, phase='created', thread=thread,
                                          topic=str(params['_chat']) + ':' + str(thread)))
        elif phase == 'creating':
            raise Refused('Telegram may have created the topic, but its ID was not saved. '
                          'The request will not create a second topic. Check Telegram for an unlinked topic.')
        elif phase == 'created':
            from .parent import session_key
            settings = dict(params['settings'])
            key = session_key(store, progress['cwd'], progress['topic'])
            if params.get('provider'):
                settings[key + '_selection'] = {'provider': params['provider'],
                                                'model': None if params['provider'] == 'opencode' else params.get('model'),
                                                'effort': None if params['provider'] == 'opencode' else params.get('effort'), 'revision': 1}
            if params.get('delegation'):
                settings[key + '_delegation'] = {'text': params['delegation'], 'revision': 1}
            store.link_created_topic(request['id'], progress['topic'], params['_chat'], progress['thread'],
                                     params['name'], progress['cwd'], settings, dict(progress, phase='linked'))
        elif phase == 'linked':
            from .controls import _status_card
            card = _status_card(store, progress['topic']).splitlines()
            agent = next(line for line in card if line.startswith('Chat agent:'))
            ready = params['name'] + ' is ready.\nProject folder: ' + progress['cwd'] + '\n' + agent + '.'
            delegation = params.get('delegation')
            if delegation:
                ready += '\nDelegation updated.'
            with store.db:
                store.enqueue_report(progress['topic'], ready)
                _finish(store, request, progress, 'Chat created: topic=' + progress['topic'] +
                        '\nName: ' + params['name'] + '\nFolder: ' + progress['cwd'] + '\n' + agent + '.' +
                        ('\nDelegation: own instructions (%d characters).' % len(delegation) if delegation else '') +
                        ('\nCreated by local program: ' + params['_from'] + '.' if params.get('_from') else ''))
            release_messages(store)
    except (Refused, OSError, subprocess.SubprocessError, TelegramError, sqlite3.Error) as error:
        latest = store.db.execute('SELECT result FROM service_requests WHERE id=?', (request['id'],)).fetchone()
        progress = json.loads(latest['result'] or '{}')
        if progress.get('phase') in ('linked', 'finished'):
            raise
        with store.db:
            _save(store, request, dict(progress, phase='cleanup', error=str(error)))
        await _cleanup_create(service, request, params, dict(progress, phase='cleanup', error=str(error)))
    if phase == 'cleanup':
        await _cleanup_create(service, request, params, progress)


async def _cleanup_create(service, request, params, progress):
    store = service.store
    extra = ''
    if progress.get('topic') and not progress.get('deleted'):
        try:
            await service.telegram.call('deleteForumTopic', chat_id=params['_chat'], message_thread_id=progress['thread'])
        except TelegramError as error:
            extra = '\nThe topic stays unlinked; deleting it failed: ' + str(error)
        progress['deleted'] = True
    if progress.get('owned'):
        try:
            if not progress.get('trash'):
                progress['trash'] = _trash_plan(Path(progress['cwd']))
                with store.db:
                    _save(store, request, progress)
            _move_folder(progress)
            extra += '\nThe new folder was moved to Trash: ' + progress['trash']
        except (OSError, Refused) as error:
            extra += '\nFolder cleanup needs attention: ' + str(error)
    with store.db:
        _finish(store, request, progress, 'Chat creation failed: ' + progress['error'] + extra, 'failed')


async def _close(service, request, params, progress):
    store, topic = service.store, params['topic']
    phase = progress.get('phase')
    if phase is None:
        try:
            with store.db:
                if 'folder' in params:
                    _confirm_close(store, request, progress, params['folder'])
                else:
                    _card(store, request, progress)
        except (Refused, OSError, subprocess.SubprocessError) as error:
            with store.db:
                text = str(error)
                if 'folder' in params:
                    text += ' Nothing was closed. Request a fresh confirmation when ready.'
                _finish(store, request, progress, text, 'refused')
        return
    if phase == 'disabled':
        session = service.sessions.get(topic)
        if session is not None:
            await session.stop()
            service.sessions.pop(topic, None)
        else:
            from .host import HostClient
            key = service.session_key(topic)
            host = store.get(key + '_host')
            if host:
                await HostClient(store.directory / 'hosts' / host['id']).stop()
                with store.db:
                    store.put(key + '_host', None)
        tasks = [task for message_id, task in service.feed_tasks.items()
                 if (store.db.execute('SELECT topic FROM messages WHERE id=?', (message_id,)).fetchone() or [None])[0] == topic]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        with store.db:
            _save(store, request, dict(progress, phase='stopped'))
    elif phase == 'stopped':
        with store.db:
            dropped_jobs = [dict(row) for row in store.db.execute(
                "SELECT id,number,title FROM tasks WHERE topic=? AND status IN ('open','waiting') ORDER BY number", (topic,))]
            now = time.time()
            store.db.execute("""UPDATE task_iterations SET closed=COALESCE(closed,?),outcome='dropped',reason='chat closed'
                WHERE EXISTS (SELECT 1 FROM tasks
                    WHERE tasks.id=task_iterations.task AND tasks.iteration=task_iterations.k
                        AND tasks.topic=? AND tasks.status IN ('open','waiting'))""", (now, topic))
            store.wait_hook('chat-close-failed', store.wait_close_topic, topic)
            if store.code_requests is not None:
                store.code_requests.queue_chat_close(store, topic)
            store.db.execute("UPDATE tasks SET status='dropped',updated=? WHERE topic=? AND status IN ('open','waiting')", (now, topic))
            for job in store.db.execute('SELECT id FROM tasks WHERE topic=?', (topic,)).fetchall():
                store.host_logs_changed(task=job['id'])
            _save(store, request, dict(progress, phase='dropped', dropped_jobs=dropped_jobs))
        if store.get('closed_copy_delete', False) is True or store.get('close_clears_space', False) is True:
            from .workspaces import request_closed_copy_cleanup
            for job in dropped_jobs:
                with store.db:
                    request_closed_copy_cleanup(store, job['id'])
    elif phase == 'dropped':
        async with service.delivery_lock:
            with store.db:
                store.db.execute('UPDATE outbox SET retired=1,reaction_state=? WHERE topic=?', ('sent', topic))
                store.db.execute("UPDATE messages SET reaction_state='sent',reaction_desired=NULL WHERE topic=?", (topic,))
                _save(store, request, dict(progress, phase='retired'))
    elif phase == 'retired':
        async with service.delivery_lock:
            try:
                await service.telegram.call('deleteForumTopic', chat_id=params['_chat'],
                                            message_thread_id=store.topic(topic)['thread'])
                outcome = 'deleted'
            except TelegramError as error:
                outcome = 'deleted' if error.reason in ('thread_not_found', 'topic_gone') else 'closed'
                if outcome == 'closed':
                    progress['delete_error'] = str(error)
                    try:
                        await service.telegram.call('closeForumTopic', chat_id=params['_chat'],
                                                    message_thread_id=store.topic(topic)['thread'])
                    except TelegramError as close_error:
                        outcome = 'deleted' if close_error.reason in ('thread_not_found', 'topic_gone') else 'disabled'
                        progress['close_error'] = str(close_error)
            with store.db:
                store.put('topic_lifecycle:' + topic, {'state': outcome, 'request': request['id']})
                _save(store, request, dict(progress, phase='telegram', outcome=outcome))
                from .close_saves import request as request_space
                for job in progress.get('dropped_jobs', []):
                    request_space(store, job['id'])
    elif phase == 'telegram':
        if progress['choice'] == 'trash':
            snap = progress['snapshot']
            try:
                path = Path(snap['cwd'])
                if path.exists() and (onboarding.project_root(store) not in path.resolve().parents or
                                     _shared_folders(store, topic, path.resolve())):
                    raise Refused('The folder became shared or the projects root changed. It was kept.')
                if not progress.get('trash'):
                    progress.update(cwd=snap['cwd'], identity=snap['identity'], trash=_trash_plan(path))
                    with store.db:
                        _save(store, request, progress)
                _move_folder(progress)
            except (OSError, Refused) as error:
                progress['folder_error'] = str(error)
        with store.db:
            _save(store, request, dict(progress, phase='folder'))
    elif phase == 'folder':
        text = 'Chat "' + progress['snapshot']['name'] + '" ' + progress['outcome'] + '. Its agent stopped. Open jobs were marked dropped.'
        text += '\n' + ('\n'.join('#%s %s' % (job['number'], job['title'])
                                  for job in progress.get('dropped_jobs', progress['snapshot']['jobs'])) or 'None.')
        if progress['outcome'] != 'deleted':
            text += '\nThe topic still needs deleting by hand: ' + progress['delete_error']
            if progress.get('close_error'):
                text += '\nClosing it also failed: ' + progress['close_error']
        text += '\nFolder ' + ('moved to Trash: ' + progress['trash'] if progress.get('trash') and not progress.get('folder_error')
                              else 'kept: ' + progress['snapshot']['cwd'])
        if progress.get('folder_error'):
            text += '\n' + progress['folder_error']
        with store.db:
            _finish(store, request, progress, text)
            store.wait_hook('topic-closed-signal-failed', store.wait_signal, 'topic.closed', internal=True)
