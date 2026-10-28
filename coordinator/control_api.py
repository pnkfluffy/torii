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
from datetime import datetime, timezone
import asyncio
import importlib
import inspect
import json
import logging
import math
from pathlib import Path
import time

from .telegram import MAX_DOCUMENT_BYTES, MAX_PHOTO_BYTES, photo_path
from .attachment_paths import attachment_paths
from .extension import available

logger = logging.getLogger(__name__)

READ, WRITE = 'read', 'write'
SERVICE = 'service'
STAGES = ('ready', 'reviewed', 'ci', 'merged', 'live')
FINISHES = ('merged', 'live', 'answered', 'delivered')
WAIT_KINDS = ('owner', 'time', 'job', 'pr', 'torii_update', 'signal')


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
    from_name: object = None


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
    Op('events.post', WRITE, {'topic': 'topic', 'text': 'text'},
       'Store a tool event for the channel coordinator. Local CLI only.', handler='control_api.op_events_post'),
    Op('code_requests.setup', WRITE, {'config': 'text'},
       'Validate and store one code-request configuration JSON document. Local CLI only.',
       handler='code_requests.op_setup'),
    Op('code_requests.show', READ, {},
       'Show code-request configuration and key file presence. Local CLI only.',
       handler='code_requests.op_show'),
    Op('code_requests.approve', WRITE, {'task': 'int', 'message': 'int'},
       'Record the owner message approving a waiting code request. Local CLI only.',
       handler='code_requests.op_approve'),
    Op('telegram.send', WRITE, {'topic': 'topic', 'text': 'text', 'reply_to': 'int?', 'task': 'int?',
                                'buttons': 'buttons?', 'image': 'image?', 'as_document': 'bool?'},
       'Report on a job with task=NUMBER to quote its origin in this channel. reply_to overrides task and takes the message=N number from a message tag in the same channel. '
       'as_document=true sends image as a file with its original bytes and name.',
       handler='control_api.op_telegram_send'),
    Op('tasks.create', WRITE, {'title': 'str', 'message': 'int?', 'topic': 'topic?', 'cross_topic': 'bool?',
                               'notes': 'text?', 'secrets': 'secret_names?', 'finish': 'finish?', 'parent': 'int?'},
       'Create a job in the channel of the owner message. Pass message=N for the message that asked. '
       'A different channel needs cross_topic=true. '
       'secrets lists NAME or ENV_NAME=VAULT_NAME entries for its workers. '
       "finish selects merged, live, answered or delivered, defaulting to the chat's finish_default; "
       "parent takes the job's global id, not its per-chat number.",
       handler='control_api.op_tasks_create'),
    Op('tasks.update', WRITE, {'task': 'int', 'title': 'str?', 'status': 'status?',
                               'notes': 'text?', 'note': 'text?', 'secrets': 'secret_names?', 'stage': 'stage?', 'pr': 'str?', 'finish': 'finish?'},
       'Update a job. An empty secrets value clears the declared names. '
       'note appends one line to notes. '
       'stage stamps a milestone: ready, reviewed, ci, merged or live. '
       'finish can change while the job is open.', handler='control_api.op_tasks_update'),
    Op('tasks.auto_resume', WRITE, {'enabled': 'bool'},
       'Turn job disposition and bounded auto-resume on or off. Off by default. Local CLI only.', handler='lifecycle.op_auto_resume'),
    Op('tasks.finish_default', WRITE, {'topic': 'topic', 'finish': 'finish?'},
       'Set the chat finish default; omit finish to clear its override. Local CLI only.',
       handler='control_api.op_tasks_finish_default'),
    Op('tasks.get', READ, {'task': 'int', 'topic': 'topic?'},
       'Read one job and its rounds. Labels use N.k after round 0. A channel filter refuses jobs from another channel.', handler='control_api.op_tasks_get'),
    Op('tasks.list', READ, {'topic': 'topic?', 'status': 'job_status?'},
       'List jobs with their rounds. Labels use N.k after round 0. A channel filter returns only jobs from that channel.', handler='control_api.op_tasks_list'),
    Op('tasks.wait', WRITE, {'task': 'int?', 'topic': 'topic?', 'kind': 'wait_kind', 'ask': 'text',
                            'job': 'int?', 'on': 'str?', 'at': 'str?', 'key': 'str?', 'buttons': 'buttons?',
                            'pr': 'str?', 'until': 'str?', 'commit': 'str?', 'signal': 'str?', 'next': 'wait_next?',
                            'watch': 'bool?', 'then_provider': 'worker_provider?', 'then_prompt': 'brief?',
                            'then_model': 'model?', 'then_effort': 'effort?', 'then_work': 'work?'},
       'Wait on an owner answer, time, job, GitHub PR, Torii commit or named signal. '
       'pr takes a GitHub PR URL; until is checks or merged. commit takes a full commit hash. '
       'signal takes a name. next declares a torii_update follow-up after merge. '
       'Omit task and supply topic for a chat-level item. '
       'Off by default. A shared key joins jobs to one ask. at is ISO 8601 with a timezone. '
       'watch leaves a done or dropped job closed.',
       handler='waits.op_tasks_wait'),
    Op('pending.list', READ, {'topic': 'topic?', 'owner': 'bool?'},
       'List armed waits. owner=true shows owner decisions and held copies. Omit topic to show every chat.',
       handler='waits.op_pending_list'),
    Op('pending.fire', WRITE, {'wait': 'int', 'message': 'int', 'choice': 'str?'},
       'Resolve a wait using an owner message or firing callback and open its next round.', handler='waits.op_pending_fire'),
    Op('pending.cancel', WRITE, {'wait': 'int', 'reason': 'text'},
       'Cancel an armed wait with a reason.', handler='waits.op_pending_cancel'),
    Op('pending.signal', WRITE, {'name': 'str'},
       'Fire armed waits for a named signal. Local CLI only.', handler='waits.op_pending_signal'),
    Op('waits.set_enabled', WRITE, {'enabled': 'bool'},
       'Switch waits on or off. Off by default. Local CLI only.', handler='controls.op_waits_set_enabled'),
    Op('tasks.stale', READ, {'hours': 'int?'},
       'List open jobs with no active worker and no job or worker change for hours (default 6).',
       handler='control_api.op_tasks_stale'),
    Op('worktree.create', WRITE, {'task': 'int', 'base': 'str?'},
       'Create or return a job worktree; base selects a remote branch.',
       handler='control_api.op_worktree_create'),
    Op('workers.spawn', WRITE, {'task': 'int', 'provider': 'worker_provider', 'prompt': 'brief',
                                'model': 'model?', 'goal': 'text?', 'effort': 'effort?', 'work': 'work?', 'message': 'int?'},
       'Queue a job worker. work=dev writes code; work=research (default) researches, plans or reviews. '
       'Muse workers are off by default. '
       'Repeating the exact request of an active worker returns that worker.',
       handler='control_api.op_workers_spawn'),
    Op('workers.steer', WRITE, {'worker': 'int', 'prompt': 'brief'}, 'Steer a worker.',
       transport=SERVICE),
    Op('workers.stop', WRITE, {'worker': 'int'}, 'Stop a worker.', transport=SERVICE),
    Op('workers.list', READ, {'task': 'int?', 'status': 'str?', 'limit': 'int?', 'before': 'int?'},
       'List service workers with their job rounds, newest first, as a page of summaries. Pass next_before as before for the next page.',
       handler='control_api.op_workers_list'),
    Op('workers.get', READ, {'worker': 'int'}, 'Read one service worker with its job round, full prompt and result.',
       handler='control_api.op_workers_get'),
    Op('workers.goal', WRITE, {'worker': 'int', 'condition': 'text'},
       'Set or clear a worker goal.', transport=SERVICE),
    Op('service.restart', WRITE, {'reason': 'text'}, 'Request service restart after outbox delivery.',
       transport=SERVICE),
    Op('cleanup.close_space', WRITE, {'enabled': 'bool'}, 'Save closed jobs and clear their owned folders. Off by default.',
       handler='controls.op_cleanup_close_space'),
    Op('cleanup.close_rules', WRITE, {'submodules': 'bool?', 'any_branch': 'bool?', 'prune_held': 'bool?',
                                    'discard_names': 'text?', 'keep': 'text?', 'requeue': 'bool?'},
       'Set close-save rules and keep task IDs. Switches default off; lists default empty. Local CLI only.',
       handler='controls.op_cleanup_close_rules'),
    Op('cleanup.retry', WRITE, {'task': 'int'}, 'Queue one closed job save unless kept. Local CLI only.',
       handler='controls.op_cleanup_retry'),
    Op('cleanup.sessions', WRITE, {'enabled': 'bool'},
       'Delete owned native sessions after their lifetime ends. Off by default. Local CLI only.',
       handler='controls.op_cleanup_sessions'),
    Op('cleanup.stray_report', WRITE, {'enabled': 'bool?', 'config': 'text?', 'run': 'bool?'},
       'Configure nightly unowned storage reports or request a scan. Report only. Off by default.',
       handler='stray_report.op_report'),
    Op('service.intake_batching', WRITE, {'enabled': 'bool'},
       'Batch queued coordinator messages per chat, owner first. Off by default.',
       handler='controls.op_service_intake_batching'),
    Op('service.updates', WRITE, {'enabled': 'bool'}, 'Enable Telegram updates. Off by default.',
       handler='controls.op_service_updates'),
    Op('service.update_freeze', WRITE, {'enabled': 'bool', 'url': 'str?', 'repo': 'str?', 'secret': 'str?'},
       'Configure signed updater status. secret is a vault name. Off by default.',
       handler='update_freeze.op_setup'),
    Op('cleanup.delete', WRITE, {'enabled': 'bool'}, 'Delete saved closed job copies. Local CLI only. Off by default.',
       handler='controls.op_cleanup_delete'),
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
    Op('policy.show', READ, {'topic': 'topic?'},
       "Show the usage policy. With topic, show it as that chat's agent gets it, with the chat's own delegation instructions.",
       handler='controls.op_policy_show'),
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
    Op('topic.rename', WRITE, {'topic': 'topic?', 'name': 'str'}, 'Rename a Telegram topic and its channel label.',
       handler='controls.op_topic_rename'),
    Op('topic.enable', WRITE, {'topic': 'topic?'}, 'Enable a channel.', handler='controls.op_topic_enable'),
    Op('topic.disable', WRITE, {'topic': 'topic?'}, 'Disable a channel.', handler='controls.op_topic_disable'),
    Op('topic.create', WRITE, {'name': 'str', 'cwd': 'path?', 'message': 'int?',
                              'provider': 'parent_provider?', 'model': 'model?', 'effort': 'effort?', 'delegation': 'text?'},
       "Create a Telegram chat and link its project folder. Optionally pick Claude, ChatGPT or OpenCode with provider=claude|codex|opencode before its first agent starts. Claude and ChatGPT accept optional model and effort. OpenCode uses the account model and has no thinking setting. Pass message=N for the owner message that asked; required over MCP. Optional delegation sets the new chat's own delegation instructions, as policy.chat does, before its first agent starts.",
       transport=SERVICE),
    Op('topic.close', WRITE, {'topic': 'topic', 'message': 'int?', 'folder': 'close_folder?'},
       'Close a chat and delete its Telegram topic and messages. Pass message=N for the owner message that asked; required over MCP. With folder=trash or keep, that message confirms the close without a card; message is also required on the CLI. Trash moves the folder to Trash; keep leaves it in place. Without folder, Torii sends an owner confirmation card.',
       transport=SERVICE),
    Op('delegation.codex', WRITE, {'enabled': 'bool'}, 'Set Codex delegation.',
       handler='controls.op_delegation_codex'),
    Op('delegation.opencode', WRITE, {'enabled': 'bool'}, 'Set OpenCode delegation. Off by default.',
       handler='controls.op_delegation_opencode'),
    Op('delegation.muse', WRITE, {'enabled': 'bool'},
       'Set Muse workers. Off by default. Only the pinned binary runs. Local CLI only.',
       handler='controls.op_delegation_muse'),
    Op('muse.binary', WRITE, {'path': 'str'},
       'Set the pinned Muse Code 1.4.9-R6297.1 binary. Local CLI only.', handler='muse_accounts.op_binary'),
    Op('opencode.binary', WRITE, {'path': 'str'}, 'Set the pinned OpenCode binary path.',
       handler='opencode_accounts.op_binary'),
    Op('opencode.account', WRITE, {'alias': 'alias', 'base_url': 'str?', 'model': 'str', 'small_model': 'str?',
                                   'context': 'int?', 'output': 'int?', 'price_in': 'number?',
                                   'price_cached': 'number?', 'price_out': 'number?', 'provider': 'str?'},
       'Create an OpenCode host or one provider=opencode-go account. The owner writes its key locally.',
       handler='opencode_accounts.op_account'),
    Op('opencode.use', WRITE, {'alias': 'alias'}, 'Select an OpenCode account for new work.',
       handler='opencode_accounts.op_use'),
    Op('model.worker', WRITE, {'model': 'model?'}, 'Set worker model.', handler='controls.op_model_worker'),
    Op('model.codex', WRITE, {'model': 'model?'}, 'Set Codex model.', handler='controls.op_model_codex'),
    Op('model.coordinator', WRITE, {'model': 'model?'}, 'Set coordinator model for the next coordinator launch.',
       handler='controls.op_model_coordinator'),
    Op('model.parent', WRITE, {'provider': 'parent_provider?', 'model': 'model?', 'topic': 'topic?',
                              'effort': 'effort?', 'message': 'int?'},
       'Pick or clear one chat agent. Over MCP, topic and message=N for the owner message in the asking channel are required. '
       'Use provider=claude (Claude), codex (ChatGPT) or opencode (OpenCode); omit provider, model and effort to clear the pick. '
       'Claude and ChatGPT accept optional model and effort. OpenCode uses the selected account model, has no thinking setting and cannot run the home chat. '
       'The chat moves to the new agent once its current turn and background work settle. Worker settings do not change. '
       'The default for all chats is the owner\'s to set through the local CLI: without topic, provider and model set it, '
       'and omitting both restores automatic selection. A model must belong to its provider, '
       'for example claude-opus-5-5 for claude and gpt-6.1-sol for codex.',
       handler='controls.op_model_parent'),
    Op('effort.parent', WRITE, {'provider': 'provider', 'effort': 'effort?'},
       'Set parent thinking for claude or codex: low, medium, high, max. Codex maps max to xhigh. '
       'Omit effort to restore Claude max or Codex xhigh. Applies at the next parent launch; '
       'a changed launch fingerprint resumes the same native session after active work settles.',
       handler='controls.op_effort_parent'),
    Op('policy.chat', WRITE, {'message': 'int?', 'topic': 'topic', 'text': 'text?'},
       "Set or clear one chat's own delegation instructions: how its agent delegates work. They add to the usage "
       'policy and win where the two conflict, in that chat only. Coordinator rules and owner-only controls stay in force. '
       'Over MCP, topic and message=N for the owner message in the asking channel are required. '
       'Send text (at most 2000 characters) to set it; omit text to clear it. '
       "The chat's agent loads them with its next message. Workers keep their settings.",
       handler='controls.op_policy_chat'),
    Op('policy.set', WRITE, {'text': 'text'}, 'Replace the usage policy.',
       handler='controls.op_policy_set'),
    Op('projects.root', WRITE, {'path': 'path'}, 'Set project parent folder.',
       handler='onboarding.op_projects_root'),
    Op('account.add', WRITE, {'topic': 'topic?', 'provider': 'account_provider?', 'alias': 'alias?'},
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
    Op('account.use', WRITE, {'alias': 'alias', 'topic': 'topic?'},
       'Use a signed-in, enabled account only when the owner picks it. A Claude pick gets a fresh usage check '
       'and takes new work whenever available; running chats move after their turn and background tasks settle. '
       'A Codex pick becomes the active account for new and resumed Codex work. Pass the originating topic '
       'for the Claude fresh-check reply when known.',
       handler='controls.op_account_use'),
    Op('accounts.claude_auto', WRITE, {}, 'Clear the owner Claude pick only when the owner asks. '
       'Select by soonest weekly reset; eligible running chats stay put.', handler='controls.op_accounts_claude_auto'),
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
    Op('secret.rename', WRITE, {'name': 'secret_name', 'new_name': 'secret_name'},
       'Rename a saved key inside the service and preserve job environment names. Local CLI only.',
       transport=SERVICE),
    Op('problems.list', READ, {'since': 'str?', 'area': 'str?', 'code': 'str?', 'limit': 'int?'},
       'List recorded problems, newest first. since is an ISO time; limit is 1 to 500, default 50.',
       handler='problems.op_problems_list'),
    Op('problems.summary', READ, {'since': 'str?'},
       'Count recorded problems by area and code since an ISO time, default the last 24 hours.',
       handler='problems.op_problems_summary'),
)

if not available('code_requests'):
    OPS = tuple(op for op in OPS if not op.id.startswith('code_requests.'))

if not available('update_freeze'):
    OPS = tuple(op for op in OPS if op.id != 'service.update_freeze')

BY_ID = {op.id: op for op in OPS}

TEXT_LIMIT = 16000
BRIEF_LIMIT = 50000
LIMITS = {'text': TEXT_LIMIT, 'brief': BRIEF_LIMIT}
WORKER_PROVIDERS = ('claude', 'codex', 'opencode', 'muse')

TYPES = ('str', 'text', 'brief', 'int', 'number', 'bool', 'path', 'image', 'topic', 'alias', 'model', 'provider', 'parent_provider', 'account_provider', 'worker_provider', 'effort', 'work', 'project', 'status', 'stage',
         'buttons', 'secret_name', 'secret_names', 'close_folder', 'finish', 'job_status', 'wait_kind', 'wait_next')


def find(op_id):
    return BY_ID.get(op_id) if isinstance(op_id, str) else None


def resolve_topic(store, value):
    if not isinstance(value, str) or not value:
        return None
    return value if store.topic(value) else None


def _coerce(store, kind, value):
    """Return (value, error). Ranges and existence beyond the type belong to handlers."""
    from .controls import _ALIAS, _MODEL
    if kind == 'wait_next':
        try:
            value = json.loads(value) if isinstance(value, str) and len(value) <= 1000 else value
        except ValueError:
            return None, 'a follow-up trigger object'
        return (value, None) if isinstance(value, dict) else (None, 'a follow-up trigger object')
    if kind == 'buttons':
        try:
            buttons = json.loads(value) if isinstance(value, str) else value
        except ValueError:
            return None, 'a button list'
        if (not isinstance(buttons, list) or not all(isinstance(row, list) and all(
                isinstance(button, dict) and isinstance(button.get('text'), str) and button['text'] and
                isinstance(button.get('callback_data'), str) and
                1 <= len(button['callback_data'].encode('utf-8')) <= 64 for button in row) for row in buttons)):
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
    if kind == 'number':
        try:
            number = float(value) if isinstance(value, str) else value
            if type(number) in (int, float) and math.isfinite(number):
                return number, None
        except (ValueError, OverflowError):
            pass
        return None, 'a finite number'
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
    if kind == 'parent_provider':
        return (value, None) if value in ('claude', 'codex', 'opencode') else (None, 'claude, codex or opencode')
    if kind == 'provider':
        return (value, None) if value in ('claude', 'codex') else (None, 'claude or codex')
    if kind == 'account_provider':
        return (value, None) if value in ('claude', 'codex', 'muse') else (None, 'claude, codex or muse')
    if kind == 'worker_provider':
        return (value, None) if value in WORKER_PROVIDERS else (None, 'claude, codex, opencode or muse')
    if kind == 'work':
        return (value, None) if value in ('dev', 'research') else (None, 'dev or research')
    if kind == 'effort':
        return (value, None) if value in ('low', 'medium', 'high', 'max') else (None, 'low, medium, high, or max')
    if kind == 'close_folder':
        return (value, None) if value in ('trash', 'keep') else (None, 'trash or keep')
    if kind == 'status':
        return (value, None) if value in ('open', 'done', 'dropped') else (None, 'open, done, or dropped')
    if kind == 'job_status':
        statuses = ('open', 'interrupted', 'waiting', 'done', 'dropped')
        return (value, None) if value in statuses else (None, ', '.join(statuses))
    if kind == 'wait_kind':
        return (value, None) if value in WAIT_KINDS else (None, ', '.join(WAIT_KINDS))
    if kind == 'stage':
        return (value, None) if value in STAGES else (None, 'ready, reviewed, ci, merged, or live')
    if kind == 'finish':
        return (value, None) if value in FINISHES else (None, 'merged, live, answered, or delivered')
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
        if op.id == 'cleanup.close_rules' and name in ('discard_names', 'keep') and value == '':
            clean[name] = value
            continue
        kind = op.params[name].rstrip('?')
        coerced, problem = _coerce(store, kind, value)
        if problem:
            return None, 'Send %s for %s.' % (problem, name)
        clean[name] = coerced
    if (op.id not in ('model.parent', 'policy.chat', 'pending.list') and not (op.id == 'tasks.wait' and 'task' in clean)
            and 'topic' in op.params and 'topic' not in clean and ctx.topic):
        resolved = resolve_topic(store, ctx.topic)
        if resolved:
            clean['topic'] = resolved
    for name, kind in op.params.items():
        if not kind.endswith('?') and name not in clean:
            return None, '%s needs %s.' % (op.id, name)
    if (op.id == 'topic.create' or op.id == 'model.parent' and 'topic' in clean) and 'provider' not in clean:
        if 'model' in clean or 'effort' in clean:
            return None, 'Supply provider with model or effort, or supply topic alone to clear the chat pick.'
    if op.id in ('model.parent', 'topic.create') and clean.get('provider') == 'opencode':
        from . import opencode_accounts
        if 'effort' in clean:
            return None, 'OpenCode chat agents have no thinking setting.'
        account = opencode_accounts.accounts(store).get(opencode_accounts.active(store)) or {}
        if 'model' in clean and account and clean['model'] != account.get('model'):
            return None, 'The OpenCode model comes from the account that opencode.use selects: %s.' % account.get('model')
    if op.id in ('model.parent', 'topic.create') and 'provider' in clean and 'model' in clean:
        claude_model = clean['model'].startswith('claude-') or clean['model'] in ('opus', 'sonnet', 'haiku', 'fable')
        if clean['provider'] == 'claude' and not claude_model:
            return None, 'Send a Claude model with provider=claude, for example claude-opus-5-5.'
        if clean['provider'] == 'codex' and claude_model:
            return None, 'Send a ChatGPT model with provider=codex, for example gpt-6.1-sol.'
    if op.id == 'account.add' and clean.get('alias') in (store.get('opencode_accounts', {}) or {}):
        return None, 'That alias belongs to an OpenCode account. OpenCode keys are configured locally.'
    return clean, None


def _handler(op):
    module, _, function = op.handler.partition('.')
    return getattr(importlib.import_module('.' + module, __package__), function)


def call(store, op, params=None, topic=None, source='cli', message=None, from_name=None):
    """Run one owner operation and return its result synchronously."""
    return _call(store, op, params, topic, source, message, from_name)


async def call_async(store, op, params=None, topic=None, source='cli', message=None, from_name=None):
    """Run one owner operation without blocking on asynchronous job checks."""
    result = _call(store, op, params, topic, source, message, from_name, async_tasks=True)
    return await result if inspect.isawaitable(result) else result


def _call(store, op, params, topic, source, message, from_name, async_tasks=False):
    ctx = Context(source, resolve_topic(store, topic), message, from_name if source == 'cli' else None)
    params = {} if params is None else params
    found = find(op)
    op_id = op if isinstance(op, str) else str(op)
    if from_name is not None and source != 'cli':
        return _record(ctx, op_id, Result(False, 'Only the local CLI can set a program name.', state='refused'))
    if topic and ctx.topic is None:
        return _record(ctx, op_id, Result(False, 'Unknown channel: %s. Use topics.list.' % topic, state='refused'))
    if found is None:
        return _record(ctx, op_id, Result(False, 'Unknown operation: ' + op_id + '. Use ctl list.', state='refused'))
    if op_id in ('tasks.finish_default', 'tasks.auto_resume') and source != 'cli':
        return _record(ctx, op_id, Result(False, op_id + ' is available only from the local CLI.', state='refused'))
    if op_id == 'cleanup.sessions' and source != 'cli':
        return _record(ctx, op_id, Result(False, 'Native session cleanup is a local CLI control.', state='refused'))
    if op_id == 'cleanup.sessions' and (not isinstance(params, dict) or type(params.get('enabled')) is not bool):
        return _record(ctx, op_id, Result(False, 'Native session cleanup needs enabled=true or enabled=false.', state='refused'))
    if op_id == 'cleanup.delete' and source != 'cli':
        return _record(ctx, op_id, Result(False, 'Closed copy deletion is a local CLI setting.', state='refused'))
    if op_id == 'pending.signal' and source != 'cli':
        return _record(ctx, op_id, Result(False, 'pending.signal is available only from the local CLI.', state='refused'))
    if op_id == 'waits.set_enabled' and source != 'cli':
        return _record(ctx, op_id, Result(False, 'The waits switch is a local CLI setting.', state='refused'))
    if op_id == 'secret.rename' and source != 'cli':
        return _record(ctx, op_id, Result(False, 'Saved key rename is a local CLI operation.', state='refused'))
    clean, problem = validate(store, found, params, ctx)
    if problem:
        return _record(ctx, op_id, Result(False, problem, state='refused'))
    if source == 'mcp' and op_id in ('model.parent', 'policy.chat'):
        from .topics import _asking
        try:
            if 'topic' not in clean:
                raise Refused('The default for all chats is the owner\'s to set. Supply topic for one chat.')
            _asking(store, ctx, clean.get('message'))
            if store.topic(clean['topic'])['chat'] != store.chat():
                raise Refused('Choose a linked channel in the current Telegram group.')
        except Refused as refusal:
            return _record(ctx, op_id, Result(False, str(refusal), state='refused'))
    if found.transport == SERVICE or (source == 'telegram' and op_id == 'account.codex_reset'):
        try:
            queued = dict(clean, _topic=ctx.topic) if op_id == 'account.codex_reset' else clean
            if ctx.from_name is not None:
                queued = dict(queued, _from=ctx.from_name)
            result = _service_call(store, op_id, queued, ctx)
        except Refused as refusal:
            result = Result(False, str(refusal), state='refused')
        return _record(ctx, op_id, result)
    if clean.get('topic') and op_id != 'topic.rename':
        ctx = Context(ctx.source, clean['topic'], ctx.message, ctx.from_name)
    try:
        handler = _op_tasks_update_async if async_tasks and found.id == 'tasks.update' else _handler(found)
        outcome = handler(store, ctx, **clean)
    except Refused as refusal:
        return _record(ctx, op_id, Result(False, str(refusal), state='refused'))
    except Exception as error:
        logger.exception('control handler failed op=%s source=%s type=%s',
                         found.id, ctx.source, type(error).__name__)
        return _record(ctx, op_id,
                       Result(False, found.id + ' could not finish.',
                              {'error': type(error).__name__}, state='failed'))
    if inspect.isawaitable(outcome):
        return _await_result(outcome, ctx, found)
    result = outcome if isinstance(outcome, Result) else Result(True, str(outcome))
    return _record(ctx, op_id, result)


async def _await_result(outcome, ctx, op):
    try:
        result = await outcome
    except Refused as refusal:
        result = Result(False, str(refusal), state='refused')
    except Exception as error:
        logger.exception('control handler failed op=%s source=%s type=%s', op.id, ctx.source, type(error).__name__)
        result = Result(False, op.id + ' could not finish.', {'error': type(error).__name__}, state='failed')
    return _record(ctx, op.id, result if isinstance(result, Result) else Result(True, str(result)))


def _record(ctx, op_id, result):
    result.op = op_id
    logger.info('control op=%s state=%s ok=%s source=%s topic=%s',
                op_id, result.state, result.ok, ctx.source, ctx.topic)
    return result


def _service_call(store, op, params, ctx=None):
    if op == 'workers.stop' and store.get('auto_resume', False) is True:
        params = dict(params, _stop_source=(ctx.source if ctx is not None else 'cli'))
    if op in ('topic.create', 'topic.close'):
        from .topics import queue
        return queue(store, ctx or Context(), op, params)
    if op == 'account.codex_reset':
        request = store.service_request(op, params)
        return Result(True, 'Banked reset queued. Torii will send the result here.',
                      {'request': request}, state='queued')
    if op.startswith('workers.'):
        worker = store.db.execute('SELECT id,provider,status,topic,task,session,fresh,host,iteration FROM workers WHERE id=?',
                                  (params['worker'],)).fetchone()
        if not worker:
            raise Refused('Unknown worker.')
        if op == 'workers.stop' and worker['status'] not in (
                'queued', 'running', 'waiting_for_secret', 'waiting_for_quota'):
            raise Refused('Worker is not running. Status: %s.' % worker['status'])
        if op == 'workers.steer':
            if worker['provider'] not in WORKER_PROVIDERS:
                raise Refused('Worker provider is not supported.')
            from .workers import can_resume_finished
            if (worker['provider'] == 'opencode' and worker['status'] not in (
                    'queued', 'running', 'waiting_for_secret', 'waiting_for_quota')
                    and store.get('opencode_enabled', False) is not True):
                raise Refused('OpenCode workers are off.')
            if worker['task'] is None or (worker['status'] not in (
                    'queued', 'running', 'waiting_for_secret', 'waiting_for_quota')
                    and not can_resume_finished(store, worker)):
                raise Refused('Worker cannot receive input. Status: %s.' % worker['status'])
            params = dict(params, _topic=worker['topic'])
        if op == 'workers.goal':
            if worker['provider'] not in ('claude', 'codex'):
                raise Refused('Worker goals require Claude or Codex.')
            if worker['status'] not in ('queued', 'running', 'waiting_for_secret', 'waiting_for_quota'):
                raise Refused('This worker cannot receive a goal.')
            condition = params['condition']
            if condition != 'clear' and not condition.strip():
                raise Refused('A goal condition is required.')
            store.db.execute('UPDATE workers SET goal=?,updated=? WHERE id=?',
                             (None if condition == 'clear' else condition, time.time(), worker['id']))
    if op == 'secret.rename':
        if store.get('pair_only'):
            raise Refused('The service runs in pair-only mode and does not open the vault. Start it normally, then rename.')
        for pending in store.service_requests_pending():
            values = json.loads(pending['params'])
            if (pending['op'] == op and values.get('name') == params['name']
                    and values.get('new_name') == params['new_name']):
                return Result(True, 'Saved key rename is queued for the service.',
                              {'request': pending['id']}, state='queued')
    if op == 'secret.revoke':
        from .envelopes import rename_reserved
        if rename_reserved(store, params['name']):
            raise Refused('A saved key rename is unfinished for this name. Run the same rename again first.')
        if store.get('pair_only'):
            raise Refused('The service runs in pair-only mode and does not open the vault. '
                          'Start it normally, then revoke.')
        if not store.db.execute('SELECT 1 FROM envelopes WHERE name=?', (params['name'],)).fetchone():
            raise Refused('No envelope was asked for ' + params['name'] + '.')
    if op == 'service.restart':
        from .updates import watch
        update = store.get('telegram_update_v1') or {}
        if update.get('phase') not in watch.TERMINAL and watch.watchdog_alive(update):
            raise Refused('An update is already in progress. It owns the next restart.')
        request = store.service_request(op, dict(params, reason=params['reason'].strip()))
        return Result(True, 'Restart requested after queued Telegram messages are sent.',
                      {'request': request}, state='queued')
    request = store.service_request(op, params)
    if op == 'workers.steer' and worker['provider'] == 'claude' and worker['status'] == 'running':
        return Result(True, "Steer queued; delivered at the worker's next input point.",
                      {'request': request}, state='queued')
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


def op_events_post(store, ctx, topic, text):
    if ctx.source != 'cli':
        raise Refused('events.post is available only from the local CLI.')
    configured = store.topic(topic)
    if not configured['enabled'] or configured['chat'] != store.chat():
        raise Refused('Event topic is not enabled in the current Telegram binding.')
    event = store.message_event(topic, text, from_name=ctx.from_name)
    return Result(True, 'Event stored for the channel coordinator.', {'message': event['id']})


def op_telegram_send(store, ctx, topic, text, reply_to=None, buttons=None, image=None, task=None, as_document=False):
    if store.get('owner') is None or store.topic(topic)['chat'] != store.chat():
        raise Refused('Report topic is outside the current Telegram binding.')
    from .topics import lifecycle
    if lifecycle(store, topic):
        raise Refused('This chat is closing or closed.')
    quoted = False
    quote_requested = reply_to is not None or task is not None
    if reply_to is None and task is not None:
        job = store.db.execute('''SELECT COALESCE(i.origin,t.origin) AS origin,t.origin AS line_origin
            FROM tasks t LEFT JOIN task_iterations i ON i.task=t.id AND i.k=t.iteration
            WHERE t.topic=? AND t.number=?''', (topic, task)).fetchone()
        if not job:
            raise Refused('Unknown job number in this channel.')
        from .reactions import reaction_target
        origin = reaction_target(store, job['origin'])
        if not origin or origin['topic'] != topic:
            origin = reaction_target(store, job['line_origin'])
        if origin and origin['topic'] == topic:
            reply_to = origin['telegram_message']
            quoted = True
    if reply_to is not None and not quoted:
        message = store.db.execute('SELECT topic,telegram_message FROM messages WHERE id=?', (reply_to,)).fetchone()
        if not message or message['topic'] != topic:
            raise Refused('reply_to must be the message=N number of a Telegram message in this channel; '
                          'send without reply_to or use another number.')
        reply_to = message['telegram_message']
        quoted = reply_to is not None
    if as_document and not image:
        raise Refused('as_document needs image.')
    if image:
        image = resolve_image(store, image)
    markup = {'inline_keyboard': buttons} if buttons else None
    row_id = store.enqueue_report(topic, text, reply_to=reply_to, reply_markup=markup, image=image,
                                as_document=bool(as_document))
    return Result(True, 'Telegram message queued.' + (' Sent without a quote.' if quote_requested and not quoted else ''),
                  {'outbox': row_id, 'quoted': quoted})


def op_tasks_create(store, ctx, title, message=None, topic=None, cross_topic=False, notes=None, secrets=None,
                    finish=None, parent=None):
    if message is not None:
        row = store.db.execute('SELECT topic,kind FROM messages WHERE id=?', (message,)).fetchone()
        if not row:
            raise Refused('Unknown message %d. Pass the ID of the message that asked for the job.' % message)
        if row['kind'] == 'forwarded':
            raise Refused("This message was forwarded from someone else. Pass the owner's own message that asked for the job.")
        if topic is not None and topic != row['topic'] and not cross_topic:
            raise Refused('Message %d came from channel %s, not %s. Pass cross_topic=true only when the owner asked '
                          'for a job in another channel.' % (message, row['topic'], topic))
        topic = topic or row['topic']
    elif ctx.source == 'mcp':
        raise Refused('Pass the ID of the owner message that asked for the job.')
    elif topic is None:
        raise Refused('tasks.create needs topic (a channel ID) or message.')
    from .topics import lifecycle
    if lifecycle(store, topic):
        raise Refused('This chat is closing or closed.')
    try:
        task = store.task_create(topic, title, notes=notes, secrets=secrets, origin=message, finish=finish, parent=parent)
    except ValueError as error:
        raise Refused(str(error)) from error
    from .close_saves import enabled, scratch
    if enabled(store):
        task['scratch'] = str(scratch(store, task['id'], create=True))
    return Result(True, 'Job %s created.' % job_number(task['number'], task['iteration']), task)


def job_number(number, iteration):
    return '%s.%d' % (number, iteration) if iteration else str(number)


def worker_job_label(task, number, iteration):
    return '%d (%s)' % (task, job_number(number, iteration)) if iteration else str(task)


def _apply_task_update(store, task, fields):
    try:
        updated = store.task_update(task, **fields)
    except ValueError as error:
        raise Refused(str(error)) from error
    text = 'Job %s updated.' % job_number(updated['number'], updated['iteration'])
    if fields.get('status') in ('done', 'dropped') and store.get('close_clears_space', False) is True:
        text += '\nIts work is queued for saving before its owned folders are cleared.'
    return Result(True, text, updated)


def op_tasks_update(store, ctx, task, **fields):
    closes_copy = (fields.get('status') in ('done', 'dropped')
                   and store.get('closed_copy_delete', False) is True
                   and store.get('close_clears_space', False) is not True)
    if not closes_copy:
        return _apply_task_update(store, task, fields)
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(_op_tasks_update_async(store, ctx, task, **fields))
    raise Refused('Job update needs asynchronous checks. Use await control_api.call_async for tasks.update.')


async def _op_tasks_update_async(store, ctx, task, **fields):
    result = _apply_task_update(store, task, fields)
    if (fields.get('status') in ('done', 'dropped') and store.get('closed_copy_delete', False) is True
            and store.get('close_clears_space', False) is not True):
        from .workspaces import closed_copy_answer, stage_closed_copy
        trigger = store.get('closed_copy_cleanup_pending', {}).get(str(task))
        state = await stage_closed_copy(store, task)
        with store.db:
            if not store.db.in_transaction:
                store.db.execute('BEGIN IMMEDIATE')
            pending = store.get('closed_copy_cleanup_pending', {})
            if pending.get(str(task)) == trigger:
                pending[str(task)] = False
                store.put('closed_copy_cleanup_pending', pending)
        result.text += '\n' + closed_copy_answer(state)
    return result


def op_tasks_finish_default(store, ctx, topic, finish=None):
    with store.transaction():
        if finish is None:
            store.db.execute('DELETE FROM settings WHERE key=?', ('finish_default:' + topic,))
        else:
            store.put('finish_default:' + topic, finish)
    from .store import finish_default
    return Result(True, 'Chat finish default updated.', {'topic': topic, 'finish': finish_default(store, topic)})


def stamp(created):
    return datetime.fromtimestamp(created, timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')


def copy_hold(store, row):
    target = store.directory / 'worktrees' / ('task-' + str(row['id']))
    subject = 'copy kept' if target.exists() or target.is_symlink() else 'cleanup held'
    return subject + ' because ' + (row['copy_reason'] or 'its save needs attention')


def op_tasks_get(store, ctx, task, topic=None):
    row = store.task_get(task)
    if not row:
        raise Refused('Unknown job.')
    if topic is not None and row['topic'] != topic:
        raise Refused('Job id %d belongs to channel %s, not %s.' % (task, row['topic'], topic))
    from .worker_storage import results_directory
    row['results'] = results_directory(store, task)
    from .close_saves import enabled, scratch
    if enabled(store):
        row['scratch'] = str(scratch(store, task))
    row['stamps'] = [dict(item) for item in store.db.execute(
        'SELECT * FROM task_stamps WHERE task=? ORDER BY id', (task,))]
    row['iterations'] = store.task_iterations(task)
    row['waits'] = [item for item in store.waits_list() if any(job['task'] == task for job in item['jobs'])]
    lines = ['Job %s: %s' % (job_number(row['number'], row['iteration']), row['title'])]
    if enabled(store) and row['copy'] == 'held':
        reason = copy_hold(store, row)
        lines.append(reason[0].upper() + reason[1:] + '.')
    lines.extend('%s: %s' % (item['stage'], stamp(item['created'])) for item in row['stamps'])
    lines.extend('Waiting for %s: %s' % (item['kind'], item['ask']) for item in row['waits'])
    if row['iteration']:
        lines.extend('Job %s: opened %s%s%s' % (
            job_number(row['number'], item['k']), stamp(item['opened']),
            ', closed ' + stamp(item['closed']) if item['closed'] is not None else '',
            ', ' + item['outcome'] if item['outcome'] else '') for item in row['iterations'])
    return Result(True, '\n'.join(lines), row)


def op_tasks_list(store, ctx, topic=None, status=None):
    rows = store.tasks_list(topic, status)
    waits = store.waits_list(topic)
    for row in rows:
        row['iterations'] = store.task_iterations(row['id'])
        row['waits'] = [item for item in waits if any(job['task'] == row['id'] for job in item['jobs'])]
    return Result(True, '\n'.join('%s job %s: %s (%s)' %
                                  (row['topic'], job_number(row['number'], row['iteration']), row['title'],
                                   row['status'] + ('; ' + copy_hold(store, row)
                                                    if store.get('close_clears_space', False) is True and row['copy'] == 'held' else ''))
                                  for row in rows) or 'No jobs.', rows)


STALE_HOURS = 6


def op_tasks_stale(store, ctx, hours=None):
    hours = STALE_HOURS if hours is None else hours
    if hours < 1:
        raise Refused('Send hours of 1 or more.')
    rows = [dict(row) for row in store.db.execute(
        '''SELECT t.id,t.topic,t.number,t.iteration,t.title,t.worktree,
                  MAX(t.updated,COALESCE(MAX(w.updated),0)) AS idle_since, COUNT(w.id) AS workers
           FROM tasks t LEFT JOIN workers w ON w.task=t.id
           WHERE t.status='open' AND NOT EXISTS (SELECT 1 FROM workers a WHERE a.task=t.id
                 AND a.status IN ('queued','waiting_for_secret','waiting_for_quota','running'))
           GROUP BY t.id HAVING idle_since < ? ORDER BY idle_since''', (time.time() - hours * 3600,))]
    lines = ['%s job %s: %s, idle %dh, %d workers' % (
        row['topic'], job_number(row['number'], row['iteration']), row['title'], (time.time() - row['idle_since']) // 3600, row['workers'])
        for row in rows]
    return Result(True, '\n'.join(lines) or 'No open job has been idle for %d hours.' % hours, {'tasks': rows})


def op_worktree_create(store, ctx, task, base=None):
    row = store.task_get(task)
    if not row:
        raise Refused('Unknown job.')
    from .worker_storage import results_directory
    results = results_directory(store, task, create=True)
    from .workspaces import prepare_task_workspace, project_identity
    ignored = f'\nRequested base {base} was not applied to the existing worktree.' if base is not None else ''
    if (row['worktree'] and Path(row['worktree']).is_dir()
            and store.get('close_clears_space', False) is not True):
        identity = asyncio.run(project_identity(Path(row['worktree'])))
        return Result(True, row['worktree'] + ignored,
                      {'worktree': row['worktree'], 'results': results, 'base_ref': 'HEAD',
                       'base_commit': identity['base_commit'], 'fetched': False})
    topic = store.topic(row['topic'])
    restore = {'saved_cwd': row['worktree']} if row['worktree'] else {}
    if store.get('close_clears_space', False) is True:
        restore['store'] = store
    try:
        workspace = asyncio.run(prepare_task_workspace(Path(topic['cwd']), store.directory, row['id'],
                                                       base=base, **restore))
    except (OSError, RuntimeError, ValueError, asyncio.TimeoutError) as error:
        raise Refused(str(error)) from error
    updated = store.task_update(task, worktree=workspace['cwd'])
    text = updated['worktree'] + (ignored if workspace.get('base_ref') == 'HEAD' else '')
    from .close_saves import enabled, scratch
    data = dict(workspace, results=results)
    if enabled(store):
        data['scratch'] = str(scratch(store, task, create=True))
    return Result(True, text, data)


def op_workers_spawn(store, ctx, task, provider, prompt, model=None, goal=None, effort=None, work='research', message=None):
    from .workers import spawn_refusal
    row = store.task_get(task)
    if not row:
        raise Refused('Unknown job.')
    from .topics import lifecycle
    if lifecycle(store, row['topic']):
        raise Refused('This chat is closing or closed.')
    if row['status'] != 'open' and message is None:
        raise Refused('Workers require an open job.')
    origin = store.db.execute('SELECT topic,kind FROM messages WHERE id=?', (message,)).fetchone() if message is not None else None
    if row['status'] != 'open':
        if (not origin or origin['topic'] != row['topic'] or origin['kind'] not in ('owner', 'callback')
                or row['status'] == 'dropped' and origin['kind'] != 'owner'):
            raise Refused('Reopening needs an owner message or firing callback from this chat.')
    if row['worktree'] and not Path(row['worktree']).is_dir():
        try:
            op_worktree_create(store, ctx, task)
        except Refused as error:
            raise Refused('Create the job worktree first.') from error
        row = store.task_get(task)
    if not row['worktree'] or not Path(row['worktree']).is_dir():
        raise Refused('Create the job worktree first.')
    if provider == 'claude' and not store.get('accounts'):
        raise Refused('No Claude account is connected. Use provider codex.')
    refusal = spawn_refusal(store, provider, row, goal, model)
    if refusal:
        raise Refused(refusal)
    from .worker_storage import results_directory
    results = results_directory(store, task, create=True)
    now = time.time()
    workspace = json.dumps({'cwd': row['worktree'], 'isolated': True})
    with store.transaction():
        if lifecycle(store, row['topic']):
            raise Refused('This chat is closing or closed.')
        current = store.task_get(task)
        if current['status'] != 'open':
            if message is None:
                raise Refused('Workers require an open job.')
            if (not origin or origin['topic'] != current['topic'] or origin['kind'] not in ('owner', 'callback')
                    or current['status'] == 'dropped' and origin['kind'] != 'owner'):
                raise Refused('Reopening needs an owner message or firing callback from this chat.')
            store.wait_hook('spawn-resolve-failed', store.wait_resolve_task, task, message)
            if store.task_get(task)['status'] != 'open':
                store.wait_open_round(task, message, opened_by='owner' if origin['kind'] == 'owner' else 'coordinator')
        twin = store.db.execute("""SELECT id,status FROM workers
            WHERE task=? AND provider=? AND prompt=? AND work=? AND model IS ? AND goal IS ? AND effort=?
            AND status IN ('queued','waiting_for_secret','waiting_for_quota','running')
            AND NOT EXISTS (SELECT 1 FROM service_requests
                WHERE op='workers.stop' AND state IN ('queued','sending')
                AND json_extract(params,'$.worker')=workers.id)
            AND NOT EXISTS (SELECT 1 FROM messages
                WHERE kind='worker_result' AND source_worker=workers.id)
            ORDER BY id LIMIT 1""",
                                (task, provider, prompt, work, model, goal, effort or 'medium')).fetchone()
        if twin:
            return Result(True, 'Worker %d is already %s for this request.' % (twin['id'], twin['status']),
                          {'worker': twin['id'], 'existing': True, 'results': results})
        cursor = store.db.execute('''INSERT INTO workers
            (task,topic,provider,prompt,cwd,workspace,goal,model,effort,work,created,updated,iteration)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,(SELECT iteration FROM tasks WHERE id=?))''',
                                  (task, row['topic'], provider, prompt, row['worktree'],
                                   workspace, goal, model, effort or 'medium', work, now, now, task))
    return Result(True, 'Worker %d queued.' % cursor.lastrowid, {'worker': cursor.lastrowid, 'results': results})


WORKER_PAGE = 20
WORKER_PAGE_LIMIT = 100
HEAD = 200
WORKER_STATUSES = ('queued', 'waiting_for_secret', 'waiting_for_quota', 'running', 'done', 'interrupted')


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
        '''SELECT w.id,w.task,w.topic,w.provider,w.model,w.effort,w.work,w.status,w.pid,w.created,w.updated,
                  w.prompt,w.result,w.iteration,t.number FROM workers w JOIN tasks t ON t.id=w.task
        WHERE (? IS NULL OR w.task=?) AND (? IS NULL OR w.status=?) AND (? IS NULL OR w.id<?)
        ORDER BY w.id DESC LIMIT ?''', (task, task, status, status, before, before, limit + 1)).fetchall()
    page = rows[:limit]
    queued_jobs = {row['task'] for row in page if row['status'] == 'queued' and row['work'] == 'dev'}
    writing_workers = {}
    if queued_jobs:
        writing_workers = {row['task']: row['worker'] for row in store.db.execute(
            "SELECT task,MIN(id) AS worker FROM workers WHERE task IN (%s) "
            "AND work='dev' AND status='running' GROUP BY task" % ','.join('?' for _ in queued_jobs),
            tuple(queued_jobs))}
    workers = [{'id': row['id'], 'task': row['task'], 'topic': row['topic'], 'provider': row['provider'],
                'model': row['model'], 'effort': row['effort'], 'work': row['work'], 'status': row['status'], 'pid': row['pid'],
                'created': row['created'], 'updated': row['updated'], 'iteration': row['iteration'],
                'prompt_head': _head(row['prompt']), 'result_head': _head(_result_text(row['result']))}
               for row in page]
    for worker in workers:
        if worker['status'] == 'queued' and worker['work'] == 'dev' and worker['task'] in writing_workers:
            worker['waits_for'] = writing_workers[worker['task']]
    next_before = page[-1]['id'] if len(rows) > limit else None
    lines = ['Worker %d: %s, job %s, effort %s' % (
        row['id'], row['status'], worker_job_label(row['task'], saved['number'], row['iteration']), row['effort']) +
        (', waits for worker %d' % row['waits_for'] if 'waits_for' in row else '') for row, saved in zip(workers, page)]
    if next_before is not None:
        lines.append('More workers: pass before=%d.' % next_before)
    return Result(True, '\n'.join(lines) or 'No workers.', {'workers': workers, 'next_before': next_before})


def op_workers_get(store, ctx, worker):
    row = store.db.execute('''SELECT w.*,t.number FROM workers w LEFT JOIN tasks t ON t.id=w.task
        WHERE w.id=?''', (worker,)).fetchone()
    if not row:
        raise Refused('Unknown worker.')
    data = dict(row, result=_result(row['result']))
    number = data.pop('number')
    alias = data.pop('account_alias', None)
    if alias:
        from .accounts import account_label, authenticated
        profile = (store.get('accounts', {}) or {}).get(alias)
        data['account'] = account_label(store, alias)
        if not authenticated(store, alias, profile):
            data['account'] += ' · not signed in'
    return Result(True, 'Worker %d: %s, job %s, effort %s' %
                  (row['id'], row['status'], worker_job_label(row['task'], number, row['iteration']), row['effort']), data)
