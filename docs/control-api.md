# Control API

`coordinator/control_api.py` defines the operations used by the coordinator's
MCP server, Telegram menus, and the local CLI. `coordinator/mcp.py` exposes the
18 coordinator tools below and these 12 read-only operations through `tools/list`:
`settings.show`, `topics.list`, `topic.show`, `workers.get`, `accounts.list`,
`account.current`, `account.show`, `policy.show`, `projects.list`,
`problems.list`, `problems.summary`, and `tasks.stale`. That is 30 MCP tools.
The account exceptions are `account.use`, only when the owner picks a Codex account, and `account.redeem`, only when the owner asks to
consume a banked reset on the active Codex account. Every other operation, including account enable and disable, delegation, model, policy, and topic
changes, is available only from Telegram and the local CLI. MCP `tools/call`
refuses it as an unknown tool. The service has no decision enum or blocked
operation list. Envelope events have audit rows (`envelope_events`), and
failures have rows in `problems` (see
[Problems and logging](operations.md#problems-and-logging)).

## Discover and call operations

```sh
python3 -m coordinator ctl list
python3 -m coordinator ctl list --kind read
python3 -m coordinator ctl describe tasks.create
python3 -m coordinator ctl call tasks.list
python3 -m coordinator ctl call model.worker model=opus
```

`ctl call` accepts `key=value` arguments. Use `--topic` with a topic ID or a
unique project name when the operation needs a topic. The CLI prints one JSON
object. Exit code `0` means done or queued, `1` means a handler failed, `2`
means CLI usage failed, and `3` means the operation was refused. The CLI refuses
service operations if no process holds the service lock.

The MCP server uses newline-delimited JSON-RPC. `tools/list` supplies the input
schemas. `tools/call` validates arguments, calls the handler, and returns the
handler text and data. An unknown tool is an error. Native tool calls are in the
coordinator transcript.

## Coordinator tools

| Operation | Effect |
| --- | --- |
| `account.use` | Select the active Codex account only when the owner picks it. |
| `account.redeem` | Redeem a banked reset on the active Codex account only when the owner asks and a resettable meter is limited or at least 95% used. |
| `telegram.send` | Save a message in the Telegram outbox. |
| `tasks.create`, `tasks.update`, `tasks.get`, `tasks.list` | Manage committed work. |
| `worktree.create` | Create or return the task's worktree. |
| `workers.spawn` | Queue a service worker in a task worktree. |
| `workers.steer`, `workers.stop`, `workers.goal` | Send a durable request to a managed worker. |
| `workers.list` | Read a page of worker summaries, newest first. |
| `workers.get` | Read one worker with its full prompt and result. |
| `service.restart` | Request exit after the outbox drains and a private SQLite backup succeeds. |
| `secret.ask` | Post an Envelope card that asks the owner for a credential by name. |
| `secret.list` | Read names, states, lengths, fingerprints, consumers, and last use. Never values. |
| `secret.rotate` | Ask again with the previous reason, consumer, and task. |
| `secret.revoke` | Service operation. Remove the value from the vault and close its envelopes. |

## Topic-bound tasks

Every message the service steers into the coordinator starts with
`[topic=ID name="NAME" message=N kind=KIND]`. `tasks.create` takes `title` and
the `message` number `N` from that tag. The service reads the saved message and
creates the task in its topic, so a task always lands in the task list of the
topic that asked for it. A call through MCP without `message` is refused. A
`topic` that differs from the message's topic is refused unless the call also
passes `cross_topic=true`; use that only when the owner asks for a task in
another topic. The local CLI and Telegram menus may pass `topic` instead of
`message`. Calls with `message` store that message ID as `tasks.origin`.
`tasks.create`, `tasks.get`, `tasks.update`, and `tasks.list` return `origin`,
which is null for jobs created without a message or before this feature.

The service, not the coordinator, supplies the binding. The MCP server is a
separate process with no view of which message a turn handles, and one turn can
carry messages from several topics. The message number is the one value that
names a single saved message, and its topic is durable.

`tasks.list` with `topic` returns only that topic's tasks. `tasks.get` with
`topic` refuses a task of another topic. Calls from the CLI with `--topic` or
from a Telegram topic fill `topic` from that context. A topic argument is a
topic ID or a name that no other topic shares, ignoring case.

Registered topic names are unique in each chat, ignoring case and outer spaces.
`topic.bind` and `topic.rename` refuse a name that another bound topic in the
chat uses. `topic.setup_new` and `topic.setup_use` keep the Telegram topic
title when the project name is taken, and refuse when that title is taken too.
A Telegram topic rename to a taken name leaves the saved name unchanged and
posts a notice in the topic. Names that already repeat in the store stay
distinct in the tag, because the tag always carries the topic ID.

`workers.spawn` accepts optional `work=dev` for code changes, or
`work=research` for research, plans, and reviews. The default is `research`.
The service selects the origin reaction when the worker process starts.
A queued worker does not change the reaction. The latest started worker wins.
The coordinator does not choose an emoji.

`workers.list` takes optional `task`, `status` (`queued`, `running`, `done`,
or `interrupted`), `limit` (default 20, at most 100), and `before`. Each summary
has the id, task, topic, provider, model, work, status, process ID, created and
updated times, and the first 200 characters of the prompt and the result. When
more rows exist, `next_before` names the value to pass as `before` for the next
page. `workers.get` returns the complete record.

## Text limits

A `text` parameter, such as `telegram.send` text, task notes, a worker goal, or a
restart reason, holds at most 16,000 characters. A worker brief, the `prompt` of
`workers.spawn` and `workers.steer`, holds at most 50,000 characters. A steer to
a queued worker is appended to its brief; the service refuses a steer that
would take the brief over 50,000 characters. `tools/list` gives each limit as
`maxLength`.

`workers.goal` applies to both providers. Claude receives its existing `/goal`
steer. Codex uses acknowledged native `thread/goal/set`, `thread/goal/get`, and
`thread/goal/clear` requests. Spawn stages a paused goal, submits the job prompt,
then activates the goal. An active goal keeps the worker open across completed
turns. A completed, paused, blocked, limited, or cleared goal ends continuation
at a turn boundary. The coordinator uses `/goal` for a
long task that needs extended verification. A service restart interrupts no
coordinator or worker process. After a restart, `workers.list` with `status`
set to `running` shows the workers that continued.
The service refuses a repeat restart within two minutes.

## Secret operations

`secret.ask` takes `topic`, `name`, `reason`, `consumer`, and an optional
`task`. It refuses a name that is invalid, reserved, set in the service
environment, or already open. It returns only the envelope ID and the name.
The deep-link token stays in the card's `outbox.reply_markup` until the owner
arms or closes the envelope; the `envelopes` table keeps only its SHA-256 hash.

A name matches `[A-Z][A-Z0-9_]{1,63}`. Reserved names are `PATH`, `HOME`,
`USER`, `SHELL`, `TMPDIR`, `PWD`, `LANG`, `BOT_TOKEN`, `NODE_OPTIONS`,
`NODE_EXTRA_CA_CERTS`, `NODE_REPL_AUTH_TOKEN`, `RUST_LOG`, `RUST_BACKTRACE`,
`ZDOTDIR`, `TMP`, `TEMP`, `BASH_ENV`, `ENV`, `PROMPT_COMMAND`, `IFS`, `SHELLOPTS`,
`HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, `NO_PROXY`, `SSL_CERT_FILE`,
`SSL_CERT_DIR`, `REQUESTS_CA_BUNDLE`, and every name that starts with `LC_`,
`PYTHON`, `DYLD_`, `LD_`, `CLAUDE`, `ANTHROPIC_`, `CODEX_`, `OPENAI_`,
`TORII_`, `TELEGRAM`, `CMUX_`, `GIT_`, `XDG_`, `PERL5`, or `RUBYOPT`.

`tasks.create` and `tasks.update` take `secrets` as a comma-separated string or
a list. Each entry is `NAME` or `ENV_NAME=VAULT_NAME`. Both sides follow the
name rules. The worker gets `ENV_NAME`; the vault stores `VAULT_NAME`. One vault
can therefore serve several repositories:

```sh
python3 -m coordinator ctl call tasks.update task=4 secrets=GH_TOKEN=GITHUB_TOKEN_ORG,NPM_TOKEN
```

The store keeps a sorted, de-duplicated JSON list. An empty value clears it. An
environment name that maps to two vault names is refused. Both providers receive
declared secrets through their private worker process environment. The parent
receives names and metadata only. `secret.ask` remains a parent-only operation.
Codex refuses stripped or runtime-control names, missing safety controls on
reattachment, effective configuration that defeats the controls, and configured
shell values that replace a declared secret. See [Credentials](operations.md#credentials).

`secret.revoke` uses the service transport. The CLI needs the running service.
Only the explicit `--pair-only` flag refuses it, because that service does not open the vault. A
repeated revoke is done and reports that nothing was stored.

## Setup and settings operations

`setup.topics_check` requests a group Topics check. `setup.claude` and
`setup.chatgpt` start the shared account sign-in flows.

`settings.show`, `settings.get`, `topics.list`, and `topic.show` read saved
state. `topic.setup_new`, `topic.setup_use`, `topic.setup_cancel`, `topic.bind`,
`topic.folder`, `topic.rename`, `topic.enable`, and `topic.disable` manage
project routing. `projects.list` and `projects.root` manage the project picker.

Account and delegation controls include `accounts.list`, `accounts.discover`,
`account.show`, `account.current`, `account.add`,
`account.signin_cancel`, `account.enable`,
`account.disable`, `account.reset`, `account.use`, `accounts.codex_auto`,
`delegation.codex`, `model.worker`, `model.codex`,
`model.coordinator`,
`policy.show`, and `policy.set`. `policy.show` reads USAGE.md, with the bundled
default when the file is missing. `policy.set` atomically replaces USAGE.md.
Coordinators load it with their next message; new workers load it at launch.
The other settings affect future dispatches.
`model.coordinator` takes effect at the next coordinator launch.
Claude account choice is automatic. Every selection uses the eligible account
whose all-model weekly reset comes soonest, with alias as the tie-break.
Signed-in accounts display their login email. The default 95% rule, or a local
per-account reserve, applies to the 5-hour, all-model weekly, and Fable weekly meters.
`account.use` makes a signed-in Codex account the active one; it is the one account
selection operation the coordinator may call, and only when the owner picks the
account. `account.redeem` consumes one banked reset on the active Codex account,
only when the owner asks and a resettable meter is limited or at least 95% used.
Claude or ChatGPT runs the main chat. Each new parent launch prefers an available Claude account, then
the active ChatGPT account. Parent ChatGPT accounts never switch automatically;
`account.use` clears their quota retry so saved messages can continue.

`accounts.codex_auto` turns automatic Codex switching on or off (default off). It is an
owner control on Telegram and the local CLI only.
`account.add` takes no name. With `provider=codex` it starts a Codex device-code
sign-in and posts the link and one-time code; see [account setup](operations.md#telegram-controls).
Otherwise it records one pending sign-in; the running service
starts it and posts a sign-in card, an envelope named `CLAUDE_SIGNIN_CODE`, with the
link. The owner pastes the code in the topic of that card. The store deletes the
message and hands the code to the waiting process in the service and stores nothing. No control operation accepts the code.
`secret.list` and the `/secrets` menu do not show sign-in cards.

## Persistence

`telegram.send` writes an `outbox` row before transport delivery. A transport
failure retries that row without rerunning work. Worker controls and restart
requests use `service_requests`; the service settles each request. Task and
worker changes use `tasks` and `workers`. See [Architecture](architecture.md)
for startup migration and recovery.

To report on a job, pass `task=NUMBER` to `telegram.send`, using the job's
`number` in the destination channel, not its database `id`. Torii replies to
that job's owner origin, or the message it reacted to, if the target has a
Telegram ID in the same channel. Otherwise, Torii sends without a quote and returns `quoted=false`
with an explanation. An unknown job number in that channel is refused.

`reply_to` overrides `task` when both are present. It takes the `message=N`
number from a message tag in the same channel. Torii resolves that row to its
Telegram message ID before delivery. A missing row, another channel's message,
or a message without a Telegram ID is refused. The same checks apply to a
job origin selected through `task`. Images and split reports keep the reply.

Owner reactions enter the queue as owner replies. The text uses the same
whitespace-collapsed, 300-character quote as other replies, followed by
`Reacted 👍` on the next line. Several added emoji share that line. Custom
emoji appear as `(custom emoji)`, and paid reactions appear as `⭐`.

These rows have no `telegram_message` of their own. `reacted_to` stores the
reacted-to Telegram message number. If `tasks.create message=N` names such a
row, `origin` retains that owner reply ID and the service resolves its target
through `reacted_to`. Job reactions, plain-turn tool reactions, and `telegram.send task`
apply to the original message, including a bot message in `outbox`.

`telegram.send` accepts one image or file per message through an absolute `image`
path under a registered topic folder, task worktree, or the service state directory.

## Home channel changes

`topic.home topic=ID` selects an enabled, linked channel as home.
`topic.home topic=ID clear=true` clears the current home channel, including a
disabled channel. It refuses to clear a different channel. Clearing home does
not disable channels or discard sessions. The next enabled `topic.bind` selects
home when none is saved; a disabled bind does not.

The original parent retains its saved session owner when home changes or clears.
Health reports follow that owner. A running parent keeps the registered-channel
view supplied at its native launch. The view updates at the next native launch,
not on a service reattachment to that process. Home changes do not stop a parent.
