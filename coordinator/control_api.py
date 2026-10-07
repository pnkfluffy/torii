"""Owner control operations shared by Telegram, the CLI, and MCP.

Ids dispatch. Kinds gate. A `params` value is a type name. A trailing `?` marks the
parameter optional.
Types include strings, bounded text, worker briefs with their own larger bound, numbers, topics, account aliases, providers,
task statuses, and Telegram button arrays.

A handler is named `module.function` and imported when it runs, so this module still
imports nothing from the package at load time and `policy.py` stays free of a cycle.
Handlers take `(store, ctx, **params)` and return owner-facing text or a `Result`.
"""

from dataclasses import dataclass
import asyncio
import importlib
import json
import logging
from pathlib import Path
import time

from .telegram import MAX_DOCUMENT_BYTES, MAX_PHOTO_BYTES, photo_path
from .attachment_paths import attachment_paths

logger = logging.getLogger(__name__)

READ, WRITE = 'read', 'write'
SERVICE = 'service'


@dataclass(frozen=True)
class Op:
    id: str
    kind: str
    params: dict
    description: str
    transport: str = 'store'
    handler: str = ''


@dataclass(frozen=True)
class Context:
    source: str = 'cli'
    topic: object = None
    message: object = None


@dataclass
class Result:
    ok: bool
    text: str
    data: object = None
    state: str = 'done'
    op: str = ''


class Refused(Exception):
    """A handler refuses with owner-facing text. No mutation happened."""


OPS = (
    Op('setup.topics_check', WRITE, {}, 'Check whether group Topics are on.', handler='setup_flow.op_setup_topics_check'),
    Op('setup.claude', WRITE, {}, 'Sign in to a dedicated Claude profile.', handler='setup_flow.op_setup_claude'),
    Op('setup.chatgpt', WRITE, {}, 'Sign in to ChatGPT.', handler='setup_flow.op_setup_chatgpt'),
    Op('telegram.send', WRITE, {'topic': 'topic', 'text': 'text', 'reply_to': 'int?', 'task': 'int?',
                                'buttons': 'buttons?', 'image': 'image?'},
       'Report on a job with task=NUMBER to quote its origin in this channel. reply_to overrides task and takes the message=N number from a message tag in the same channel.',
       handler='control_api.op_telegram_send'),
    Op('tasks.create', WRITE, {'title': 'str', 'message': 'int?', 'topic': 'topic?', 'cross_topic': 'bool?',
                               'notes': 'text?', 'secrets': 'secret_names?'},
       'Create a job in the channel of the owner message. Pass message=N for the message that asked. '
       'A different channel needs cross_topic=true. '
       'secrets lists NAME or ENV_NAME=VAULT_NAME entries for its workers.',
       handler='control_api.op_tasks_create'),
    Op('tasks.update', WRITE, {'task': 'int', 'title': 'str?', 'status': 'status?',
                               'notes': 'text?', 'secrets': 'secret_names?'},
       'Update a job. An empty secrets value clears the declared names.', handler='control_api.op_tasks_update'),
    Op('tasks.get', READ, {'task': 'int', 'topic': 'topic?'},
       'Read one job. A channel filter refuses jobs from another channel.', handler='control_api.op_tasks_get'),
    Op('tasks.list', READ, {'topic': 'topic?', 'status': 'status?'},
       'List jobs. A channel filter returns only jobs from that channel.', handler='control_api.op_tasks_list'),
    Op('tasks.stale', READ, {'hours': 'int?'},
       'List open jobs with no active worker and no job or worker change for hours (default 6).',
       handler='control_api.op_tasks_stale'),
    Op('worktree.create', WRITE, {'task': 'int'}, 'Create or return a job worktree.',
       handler='control_api.op_worktree_create'),
    Op('workers.spawn', WRITE, {'task': 'int', 'provider': 'provider', 'prompt': 'brief',
                                'model': 'model?', 'goal': 'text?', 'effort': 'effort?', 'work': 'work?'},
       'Queue a job worker. work=dev writes code; work=research (default) researches, plans or reviews.',
       handler='control_api.op_workers_spawn'),
    Op('workers.steer', WRITE, {'worker': 'int', 'prompt': 'brief'}, 'Steer a worker.',
       transport=SERVICE),
    Op('workers.stop', WRITE, {'worker': 'int'}, 'Stop a worker.', transport=SERVICE),
    Op('workers.list', READ, {'task': 'int?', 'status': 'str?', 'limit': 'int?', 'before': 'int?'},
       'List service workers, newest first, as a page of summaries. Pass next_before as before for the next page.',
       handler='control_api.op_workers_list'),
    Op('workers.get', READ, {'worker': 'int'}, 'Read one service worker with its full prompt and result.',
       handler='control_api.op_workers_get'),
    Op('workers.goal', WRITE, {'worker': 'int', 'condition': 'text'},
       'Set or clear a worker goal.', transport=SERVICE),
    Op('service.restart', WRITE, {'reason': 'text'}, 'Request service restart after outbox delivery.',
       transport=SERVICE),
    Op('settings.show', READ, {'topic': 'topic?'}, 'Show channel settings.',
       handler='controls.op_settings_show'),
    Op('health.show', READ, {}, 'Show running workers, host pressure, and cached subscription availability.',
       handler='health.op_health_show'),
    Op('settings.get', READ, {}, 'Read owner settings.', handler='controls.op_settings_get'),
    Op('topics.list', READ, {}, 'List linked channels.', handler='controls.op_topics_list'),
    Op('topic.show', READ, {'topic': 'topic?'}, 'Show one channel.', handler='controls.op_topic_show'),
    Op('projects.list', READ, {}, 'List projects.', handler='onboarding.op_projects_list'),
    Op('accounts.list', READ, {}, 'List local accounts.', handler='controls.op_accounts_list'),
    Op('account.show', READ, {'alias': 'alias'}, 'Show one account.', handler='controls.op_account_show'),
    Op('account.current', READ, {}, 'Show the next automatically selected account.', handler='controls.op_account_current'),
    Op('policy.show', READ, {}, 'Show the usage policy.', handler='controls.op_policy_show'),
    Op('topic.setup_new', WRITE, {'topic': 'topic?', 'name': 'str'}, 'Create a project and link this channel.',
       handler='onboarding.op_setup_new'),
    Op('topic.setup_use', WRITE, {'topic': 'topic?', 'project': 'project'}, 'Link this channel to an existing project.',
       handler='onboarding.op_setup_use'),
    Op('topic.setup_cancel', WRITE, {'topic': 'topic?'}, 'Cancel setup.',
       handler='onboarding.op_setup_cancel'),
    Op('topic.bind', WRITE, {'topic': 'topic', 'cwd': 'path', 'name': 'str',
                             'provider': 'provider?', 'session': 'str?', 'enable': 'bool?',
                             'source_pid': 'int?'}, 'Link a channel.', handler='controls.op_topic_bind'),
    Op('topic.home', WRITE, {'topic': 'topic', 'clear': 'bool?'}, 'Set or clear the linked home channel.',
       handler='controls.op_topic_home'),
    Op('topic.folder', WRITE, {'topic': 'topic?', 'project': 'project'}, 'Change project folder.',
       handler='controls.op_topic_folder'),
    Op('topic.rename', WRITE, {'topic': 'topic?', 'name': 'str'}, 'Rename a channel.',
       handler='controls.op_topic_rename'),
    Op('topic.enable', WRITE, {'topic': 'topic?'}, 'Enable a channel.', handler='controls.op_topic_enable'),
    Op('topic.disable', WRITE, {'topic': 'topic?'}, 'Disable a channel.', handler='controls.op_topic_disable'),
    Op('delegation.codex', WRITE, {'enabled': 'bool'}, 'Set Codex delegation.',
       handler='controls.op_delegation_codex'),
    Op('model.worker', WRITE, {'model': 'model?'}, 'Set worker model.', handler='controls.op_model_worker'),
    Op('model.codex', WRITE, {'model': 'model?'}, 'Set Codex model.', handler='controls.op_model_codex'),
    Op('model.coordinator', WRITE, {'model': 'model?'}, 'Set coordinator model for the next coordinator launch.',
       handler='controls.op_model_coordinator'),
    Op('policy.set', WRITE, {'text': 'text'}, 'Replace the usage policy.',
       handler='controls.op_policy_set'),
    Op('projects.root', WRITE, {'path': 'path'}, 'Set project parent folder.',
       handler='onboarding.op_projects_root'),
    Op('account.add', WRITE, {'topic': 'topic?', 'provider': 'provider?', 'alias': 'alias?'},
       'Start Claude sign-in for a new account. The card is posted in the topic and the owner pastes the code there '
       'as one message. Or start a Codex '
       'device-code sign-in with provider codex. Pass alias to sign in again to a registered signed-out account; '
       'its registry determines the provider.',
       handler='signin.op_account_add'),
    Op('account.remove', WRITE, {'alias': 'alias'},
       'Remove a signed-out account from Torii. Its folders stay on disk and are excluded from discovery.',
       handler='signin.op_account_remove'),
    Op('account.signin_cancel', WRITE, {}, 'Cancel the open account sign-in.',
       handler='signin.op_account_signin_cancel'),
    Op('account.enable', WRITE, {'alias': 'alias'}, 'Enable an account.',
       handler='controls.op_account_enable'),
    Op('account.disable', WRITE, {'alias': 'alias'}, 'Disable an account.',
       handler='controls.op_account_disable'),
    Op('account.reset', WRITE, {'alias': 'alias'}, 'Reset account quota cache.',
       handler='controls.op_account_reset'),
    Op('account.codex_reset', WRITE, {'alias': 'alias'}, 'Redeem one banked reset for a named Codex account. '
       'Torii refuses unless it is limited, ordinary usage is refused, or a resettable meter is at least 95% used.',
       handler='controls.op_account_codex_reset'),
    Op('account.redeem', WRITE, {}, 'Redeem one banked reset on the active Codex account, only when the owner asks for it. '
       'Torii refuses unless that account is limited or at least 95% used on a resettable Codex meter.', handler='controls.op_account_redeem'),
    Op('account.use', WRITE, {'alias': 'alias'},
       'Make a signed-in Codex account the active one, only when the owner picks it. New and resumed Codex '
       'work runs on it.',
       handler='controls.op_account_use'),
    Op('accounts.codex_auto', WRITE, {'enabled': 'bool'}, 'Turn automatic Codex account switching on or off.',
       handler='controls.op_accounts_codex_auto'),
    Op('accounts.discover', WRITE, {}, 'Discover local accounts.',
       handler='controls.op_accounts_discover'),
    Op('secret.ask', WRITE, {'topic': 'topic', 'name': 'secret_name', 'reason': 'str', 'consumer': 'str',
                             'task': 'int?'}, 'Post an Envelope card that asks the owner for a credential by name.',
       handler='envelopes.op_secret_ask'),
    Op('secret.list', READ, {'name': 'secret_name?'},
       'List secret names with state, length, fingerprint, consumer, and last use. Never values.',
       handler='envelopes.op_secret_list'),
    Op('secret.rotate', WRITE, {'name': 'secret_name', 'topic': 'topic?'},
       'Ask again for a secret with its previous reason and consumer.', handler='envelopes.op_secret_rotate'),
    Op('secret.revoke', WRITE, {'name': 'secret_name'}, 'Remove a secret from the vault and close its envelopes.',
       transport=SERVICE),
    Op('problems.list', READ, {'since': 'str?', 'area': 'str?', 'code': 'str?', 'limit': 'int?'},
       'List recorded problems, newest first. since is an ISO time; limit is 1 to 500, default 50.',
       handler='problems.op_problems_list'),
    Op('problems.summary', READ, {'since': 'str?'},
       'Count recorded problems by area and code since an ISO time, default the last 24 hours.',
       handler='problems.op_problems_summary'),
)

BY_ID = {op.id: op for op in OPS}

TEXT_LIMIT = 16000
BRIEF_LIMIT = 50000
LIMITS = {'text': TEXT_LIMIT, 'brief': BRIEF_LIMIT}

TYPES = ('str', 'text', 'brief', 'int', 'bool', 'path', 'image', 'topic', 'alias', 'model', 'provider', 'effort', 'work', 'project', 'status',
         'buttons', 'secret_name', 'secret_names')


def find(op_id):
    return BY_ID.get(op_id) if isinstance(op_id, str) else None


def resolve_topic(store, value):
    if not isinstance(value, str) or not value:
        return None
    return value if store.topic(value) else None


def _coerce(store, kind, value):
    """Return (value, error). Ranges and existence beyond the type belong to handlers."""
    from .controls import _ALIAS, _MODEL
    if kind == 'buttons':
        try:
            buttons = json.loads(value) if isinstance(value, str) else value
        except ValueError:
            return None, 'a button list'
        if (not isinstance(buttons, list) or not all(isinstance(row, list) and all(
                isinstance(button, dict) and isinstance(button.get('text'), str) and
                isinstance(button.get('callback_data'), str) for button in row) for row in buttons)):
            return None, 'a button list'
        return buttons, None
    if kind == 'secret_names':
        from .envelopes import parse_declarations
        try:
            return parse_declarations(value), None
        except ValueError:
            return None, ('secret names such as GITHUB_TOKEN or GH_TOKEN=GITHUB_TOKEN_ORG, separated by commas; '
                          'runtime-control and Codex-stripped names are refused')
    if kind == 'int':
        if type(value) is int:
            return value, None
        if isinstance(value, str) and value.lstrip('-').isascii() and value.lstrip('-').isdigit() and len(value) <= 19:
            return int(value), None
        return None, 'whole number'
    if kind == 'bool':
        if type(value) is bool:
            return value, None
        if isinstance(value, str) and value.casefold() in ('true', 'false', 'on', 'off', '1', '0', 'yes', 'no'):
            return value.casefold() in ('true', 'on', '1', 'yes'), None
        return None, 'true or false'
    if not isinstance(value, str):
        return None, 'text'
    if kind == 'secret_name':
        from .envelopes import valid_name
        return (value, None) if valid_name(value) else (None, 'a secret name such as GITHUB_TOKEN (not reserved)')
    if kind in LIMITS:
        limit = LIMITS[kind]
        if not value.strip():
            return None, 'text of 1 to %d characters' % limit
        if len(value) > limit:
            return None, 'text of at most %d characters (this one has %d)' % (limit, len(value))
        return value, None
    if kind == 'image':
        return (value, None) if value else (None, 'an absolute image path')
    if kind == 'str':
        value = value.strip()
        return (value, None) if 0 < len(value) <= 200 else (None, 'text of 1 to 200 characters')
    if kind == 'alias':
        return (value, None) if _ALIAS.fullmatch(value) else (None, 'an account name')
    if kind == 'model':
        return (value, None) if _MODEL.fullmatch(value) else (None, 'a model id')
    if kind == 'provider':
        return (value, None) if value in ('claude', 'codex') else (None, 'claude or codex')
    if kind == 'work':
        return (value, None) if value in ('dev', 'research') else (None, 'dev or research')
    if kind == 'effort':
        return (value, None) if value in ('low', 'medium', 'high', 'max') else (None, 'low, medium, high, or max')
    if kind == 'status':
        return (value, None) if value in ('open', 'done', 'dropped') else (None, 'open, done, or dropped')
    if kind == 'project':
        return (value.strip(), None) if value.strip() else (None, 'a project name or absolute path')
    if kind == 'path':
        try:
            candidate = Path(value).expanduser()
        except (OSError, ValueError, RuntimeError):
            return None, 'an absolute path to an existing folder'
        if not candidate.is_absolute() or not candidate.is_dir():
            return None, 'an absolute path to an existing folder'
        return str(candidate.resolve()), None
    if kind == 'topic':
        resolved = resolve_topic(store, value)
        return (resolved, None) if resolved else (None, 'a known channel ID')
    return None, 'a supported value'


def validate(store, op, params, ctx):
    """Return (clean, error). Missing or invalid values never reach a handler."""
    if not isinstance(params, dict):
        return None, 'Send parameters as name and value pairs.'
    clean = {}
    for name, value in params.items():
        if name not in op.params:
            return None, '%s does not take a parameter named %s.' % (op.id, name)
        if value is None:
            continue
        kind = op.params[name].rstrip('?')
        coerced, problem = _coerce(store, kind, value)
        if problem:
            return None, 'Send %s for %s.' % (problem, name)
        clean[name] = coerced
    if 'topic' in op.params and 'topic' not in clean and ctx.topic:
        resolved = resolve_topic(store, ctx.topic)
        if resolved:
            clean['topic'] = resolved
    for name, kind in op.params.items():
        if not kind.endswith('?') and name not in clean:
            return None, '%s needs %s.' % (op.id, name)
    return clean, None


def _handler(op):
    module, _, function = op.handler.partition('.')
    return getattr(importlib.import_module('.' + module, __package__), function)


def call(store, op, params=None, topic=None, source='cli', message=None):
    """Run one owner operation. The caller owns the transaction."""
    ctx = Context(source, resolve_topic(store, topic), message)
    params = {} if params is None else params
    found = find(op)
    op_id = op if isinstance(op, str) else str(op)
    if topic and ctx.topic is None:
        return _record(ctx, op_id, Result(False, 'Unknown channel: %s. Use topics.list.' % topic, state='refused'))
    if found is None:
        return _record(ctx, op_id, Result(False, 'Unknown operation: ' + op_id + '. Use ctl list.', state='refused'))
    clean, problem = validate(store, found, params, ctx)
    if problem:
        return _record(ctx, op_id, Result(False, problem, state='refused'))
    if found.transport == SERVICE or (source == 'telegram' and op_id == 'account.codex_reset'):
        try:
            queued = dict(clean, _topic=ctx.topic) if op_id == 'account.codex_reset' else clean
            result = _service_call(store, op_id, queued)
        except Refused as refusal:
            result = Result(False, str(refusal), state='refused')
        return _record(ctx, op_id, result)
    if clean.get('topic'):
        ctx = Context(ctx.source, clean['topic'], ctx.message)
    try:
        outcome = _handler(found)(store, ctx, **clean)
    except Refused as refusal:
        return _record(ctx, op_id, Result(False, str(refusal), state='refused'))
    except Exception as error:
        logger.exception('control handler failed op=%s source=%s type=%s',
                         found.id, ctx.source, type(error).__name__)
        return _record(ctx, op_id,
                       Result(False, found.id + ' could not finish.',
                              {'error': type(error).__name__}, state='failed'))
    result = outcome if isinstance(outcome, Result) else Result(True, str(outcome))
    return _record(ctx, op_id, result)


def _record(ctx, op_id, result):
    result.op = op_id
    logger.info('control op=%s state=%s ok=%s source=%s topic=%s',
                op_id, result.state, result.ok, ctx.source, ctx.topic)
    return result


def _service_call(store, op, params):
    if op == 'account.codex_reset':
        request = store.service_request(op, params)
        return Result(True, 'Banked reset queued. Torii will send the result here.',
                      {'request': request}, state='queued')
    if op.startswith('workers.'):
        worker = store.db.execute('SELECT id,provider,status,topic,task FROM workers WHERE id=?',
                                  (params['worker'],)).fetchone()
        if not worker:
            raise Refused('Unknown worker.')
        if op == 'workers.stop' and worker['status'] not in (
                'queued', 'running', 'waiting_for_secret', 'waiting_for_quota'):
            raise Refused('Worker is not running. Status: %s.' % worker['status'])
        if op == 'workers.steer':
            if worker['task'] is None or worker['status'] not in (
                    'queued', 'running', 'needs_input', 'waiting_for_secret', 'waiting_for_quota'):
                raise Refused('Worker cannot receive input. Status: %s.' % worker['status'])
            params = dict(params, _topic=worker['topic'])
        if op == 'workers.goal':
            if worker['status'] not in ('queued', 'running', 'waiting_for_secret', 'waiting_for_quota'):
                raise Refused('This worker cannot receive a goal.')
            condition = params['condition']
            if condition != 'clear' and not condition.strip():
                raise Refused('A goal condition is required.')
            store.db.execute('UPDATE workers SET goal=?,updated=? WHERE id=?',
                             (None if condition == 'clear' else condition, time.time(), worker['id']))
    if op == 'secret.revoke':
        if store.get('pair_only'):
            raise Refused('The service runs in pair-only mode and does not open the vault. '
                          'Start it normally, then revoke.')
        if not store.db.execute('SELECT 1 FROM envelopes WHERE name=?', (params['name'],)).fetchone():
            raise Refused('No envelope was asked for ' + params['name'] + '.')
    if op == 'service.restart':
        request = store.service_request(op, dict(params, reason=params['reason'].strip()))
        return Result(True, 'Restart requested after queued Telegram messages are sent.',
                      {'request': request}, state='queued')
    request = store.service_request(op, params)
    return Result(True, 'Queued for the running service.', {'request': request}, state='queued')


def _under(path, root):
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def resolve_image(store, image):
    try:
        supplied = Path(image)
        if not supplied.is_absolute():
            raise Refused('Image must be an absolute file path.')
        path = supplied.resolve(strict=True)
        if not path.is_file():
            raise Refused('Image must be a regular file.')
        if attachment_paths(store).protected(path):
            raise Refused('Image is a protected credential or launch file.')
        roots = [topic['cwd'] for topic in store.topics() if topic['cwd']]
        roots += [task['worktree'] for task in store.tasks_list() if task['worktree']]
        roots.append(store.directory)
        if not any(_under(path, Path(root).resolve()) for root in roots):
            raise Refused('Image must be under a linked project folder, job worktree, or service state directory.')
        size = path.stat().st_size
        limit = MAX_DOCUMENT_BYTES if not photo_path(path, size) else MAX_PHOTO_BYTES
        if size > limit:
            raise Refused('Image is too large. Photos may be 10 MB and documents 50 MB at most.')
    except FileNotFoundError:
        raise Refused('Image file does not exist.') from None
    except (OSError, RuntimeError, ValueError):
        raise Refused('Image file cannot be read.') from None
    return str(path)


def op_telegram_send(store, ctx, topic, text, reply_to=None, buttons=None, image=None, task=None):
    if store.get('owner') is None or store.topic(topic)['chat'] != store.chat():
        raise Refused('Report topic is outside the current Telegram binding.')
    quoted = False
    if reply_to is None and task is not None:
        job = store.db.execute('SELECT origin FROM tasks WHERE topic=? AND number=?', (topic, task)).fetchone()
        if not job:
            raise Refused('Unknown job number in this channel.')
        from .reactions import reaction_target
        origin = reaction_target(store, job['origin'])
        if origin and origin['topic'] == topic:
            reply_to = origin['telegram_message']
            quoted = True
    if reply_to is not None and not quoted:
        message = store.db.execute('SELECT topic,telegram_message FROM messages WHERE id=?', (reply_to,)).fetchone()
        if not message or message['topic'] != topic or message['telegram_message'] is None:
            raise Refused('reply_to must be the message=N number of a Telegram message in this channel; '
                          'send without reply_to or use another number.')
        reply_to = message['telegram_message']
        quoted = True
    if image:
        image = resolve_image(store, image)
    markup = {'inline_keyboard': buttons} if buttons else None
    row_id = store.enqueue_report(topic, text, reply_to=reply_to, reply_markup=markup, image=image)
    return Result(True, 'Telegram message queued.' + (' Sent without a quote.' if task is not None and not quoted else ''),
                  {'outbox': row_id, 'quoted': quoted})


def op_tasks_create(store, ctx, title, message=None, topic=None, cross_topic=False, notes=None, secrets=None):
    if message is not None:
        row = store.db.execute('SELECT topic FROM messages WHERE id=?', (message,)).fetchone()
        if not row:
            raise Refused('Unknown message %d. Pass the ID of the message that asked for the job.' % message)
        if topic is not None and topic != row['topic'] and not cross_topic:
            raise Refused('Message %d came from channel %s, not %s. Pass cross_topic=true only when the owner asked '
                          'for a job in another channel.' % (message, row['topic'], topic))
        topic = topic or row['topic']
    elif ctx.source == 'mcp':
        raise Refused('Pass the ID of the owner message that asked for the job.')
    elif topic is None:
        raise Refused('tasks.create needs topic (a channel ID) or message.')
    task = store.task_create(topic, title, notes=notes, secrets=secrets, origin=message)
    return Result(True, 'Job %d created.' % task['number'], task)


def op_tasks_update(store, ctx, task, **fields):
    try:
        updated = store.task_update(task, **fields)
    except ValueError as error:
        raise Refused(str(error)) from error
    return Result(True, 'Job %d updated.' % updated['number'], updated)


def op_tasks_get(store, ctx, task, topic=None):
    row = store.task_get(task)
    if not row:
        raise Refused('Unknown job.')
    if topic is not None and row['topic'] != topic:
        raise Refused('Job id %d belongs to channel %s, not %s.' % (task, row['topic'], topic))
    return Result(True, 'Job %d: %s' % (row['number'], row['title']), row)


def op_tasks_list(store, ctx, topic=None, status=None):
    rows = store.tasks_list(topic, status)
    return Result(True, '\n'.join('%s job %d: %s (%s)' %
                                  (row['topic'], row['number'], row['title'], row['status'])
                                  for row in rows) or 'No jobs.', rows)


STALE_HOURS = 6


def op_tasks_stale(store, ctx, hours=None):
    hours = STALE_HOURS if hours is None else hours
    if hours < 1:
        raise Refused('Send hours of 1 or more.')
    rows = [dict(row) for row in store.db.execute(
        '''SELECT t.id,t.topic,t.number,t.title,t.worktree,
                  MAX(t.updated,COALESCE(MAX(w.updated),0)) AS idle_since, COUNT(w.id) AS workers
           FROM tasks t LEFT JOIN workers w ON w.task=t.id
           WHERE t.status='open' AND NOT EXISTS (SELECT 1 FROM workers a WHERE a.task=t.id
                 AND a.status IN ('queued','waiting_for_secret','waiting_for_quota','running'))
           GROUP BY t.id HAVING idle_since < ? ORDER BY idle_since''', (time.time() - hours * 3600,))]
    lines = ['%s job %d: %s, idle %dh, %d workers' % (
        row['topic'], row['number'], row['title'], (time.time() - row['idle_since']) // 3600, row['workers'])
        for row in rows]
    return Result(True, '\n'.join(lines) or 'No open job has been idle for %d hours.' % hours, {'tasks': rows})


def op_worktree_create(store, ctx, task):
    row = store.task_get(task)
    if not row:
        raise Refused('Unknown job.')
    if row['worktree']:
        if not Path(row['worktree']).is_dir():
            raise Refused('The saved job worktree is missing.')
        return Result(True, row['worktree'], {'worktree': row['worktree']})
    topic = store.topic(row['topic'])
    from .workspaces import prepare_task_workspace
    try:
        workspace = asyncio.run(prepare_task_workspace(Path(topic['cwd']), store.directory, row['id']))
    except (OSError, RuntimeError, ValueError) as error:
        raise Refused(str(error)) from error
    updated = store.task_update(task, worktree=workspace['cwd'])
    return Result(True, updated['worktree'], workspace)


def op_workers_spawn(store, ctx, task, provider, prompt, model=None, goal=None, effort=None, work='research'):
    row = store.task_get(task)
    if not row:
        raise Refused('Unknown job.')
    if row['status'] != 'open':
        raise Refused('Workers require an open job.')
    if not row['worktree'] or not Path(row['worktree']).is_dir():
        raise Refused('Create the job worktree first.')
    if provider == 'claude' and not store.get('accounts'):
        raise Refused('No Claude account is connected. Use provider codex.')
    now = time.time()
    workspace = json.dumps({'cwd': row['worktree'], 'isolated': True})
    with store.db:
        store.db.execute("""UPDATE workers SET status='interrupted',updated=?
            WHERE task=? AND status='needs_input'""", (now, task))
        cursor = store.db.execute('''INSERT INTO workers
            (task,topic,provider,prompt,cwd,workspace,goal,model,effort,work,created,updated)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',
                                  (task, row['topic'], provider, prompt, row['worktree'],
                                   workspace, goal, model, effort or 'medium', work, now, now))
    return Result(True, 'Worker %d queued.' % cursor.lastrowid, {'worker': cursor.lastrowid})


WORKER_PAGE = 20
WORKER_PAGE_LIMIT = 100
HEAD = 200
WORKER_STATUSES = ('queued', 'waiting_for_secret', 'waiting_for_quota', 'running', 'done', 'needs_input', 'interrupted')


def _head(text):
    text = text or ''
    return text if len(text) <= HEAD else text[:HEAD] + '…'


def _result(saved):
    try:
        return json.loads(saved) if saved else None
    except ValueError:
        return saved


def _result_text(saved):
    result = _result(saved)
    if isinstance(result, dict):
        return result.get('text') or result.get('error') or ''
    return result or ''


def op_workers_list(store, ctx, task=None, status=None, limit=None, before=None):
    limit = WORKER_PAGE if limit is None else limit
    if not 1 <= limit <= WORKER_PAGE_LIMIT:
        raise Refused('Send a limit from 1 to %d.' % WORKER_PAGE_LIMIT)
    if status is not None and status not in WORKER_STATUSES:
        raise Refused('Send a status of ' + ', '.join(WORKER_STATUSES) + '.')
    rows = store.db.execute(
        '''SELECT id,task,topic,provider,model,effort,work,status,pid,created,updated,prompt,result FROM workers
        WHERE task IS NOT NULL AND (? IS NULL OR task=?) AND (? IS NULL OR status=?) AND (? IS NULL OR id<?)
        ORDER BY id DESC LIMIT ?''', (task, task, status, status, before, before, limit + 1)).fetchall()
    page = rows[:limit]
    workers = [{'id': row['id'], 'task': row['task'], 'topic': row['topic'], 'provider': row['provider'],
                'model': row['model'], 'effort': row['effort'], 'work': row['work'], 'status': row['status'], 'pid': row['pid'],
                'created': row['created'], 'updated': row['updated'],
                'prompt_head': _head(row['prompt']), 'result_head': _head(_result_text(row['result']))}
               for row in page]
    next_before = page[-1]['id'] if len(rows) > limit else None
    lines = ['Worker %d: %s, job %s, effort %s' % (row['id'], row['status'], row['task'], row['effort']) for row in workers]
    if next_before is not None:
        lines.append('More workers: pass before=%d.' % next_before)
    return Result(True, '\n'.join(lines) or 'No workers.', {'workers': workers, 'next_before': next_before})


def op_workers_get(store, ctx, worker):
    row = store.db.execute('SELECT * FROM workers WHERE id=?', (worker,)).fetchone()
    if not row:
        raise Refused('Unknown worker.')
    data = dict(row, result=_result(row['result']))
    alias = data.pop('account_alias', None)
    if alias:
        from .accounts import account_label, authenticated
        profile = (store.get('accounts', {}) or {}).get(alias)
        data['account'] = account_label(store, alias)
        if not authenticated(store, alias, profile):
            data['account'] += ' · not signed in'
    return Result(True, 'Worker %d: %s, job %s, effort %s' %
                  (row['id'], row['status'], row['task'], row['effort']), data)
