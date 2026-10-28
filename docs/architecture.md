# Architecture

Torii is a service with detached provider hosts and a persistent Claude or ChatGPT
coordinator session per Telegram topic. An enabled OpenCode account can also run
a linked chat outside the home topic. Each coordinator owns its conversation.
SQLite keeps the state that a service relaunch must recover.
The native provider owns its conversation history.

At each new Claude parent, Claude worker, or OpenCode worker launch, Torii reads
`shared_mcp_servers` from the checkout's gitignored `local-overrides.json`.
The default is an empty list. Selected entries come from the Torii home
`~/.claude.json`. Each entry must have type `http`, a nonempty URL string, and
string headers. One reader selects and validates entries and adds their URLs and
header values to the launch scrubber. For Claude, Torii writes valid entries to
`<state_dir>/shared-mcp-<sha256>.json` with mode 600. The hash covers the exact JSON content.
Torii publishes each file atomically and never changes its content. The parent
passes that file with `coordinator-mcp.json`; the worker passes it alone.
Both Claude paths use `--mcp-config` with file paths. OpenCode workers receive
the entries in their private mode 600 `opencode.json`, as remote servers with
OAuth discovery disabled. Each selected server's sanitized tool prefix is
allowed without approval. The guard plugin permits these tool calls. The pinned
OpenCode records failed server connections and continues without their tools.
Missing or invalid sources or entries and invalid selections produce
`shared-mcp` problems without their values, once per launch. An absent or empty
selection adds no connections or problems. OpenCode chat parent configs stay separate
and do not receive shared entries.
An attached OpenCode worker restores these scrubber values from its original
private config. It does not read the current shared selection or start a second
provider process.
Claude parents keep the tools listed in `coordinator.mcp.ALWAYS_LOADED_TOOLS`
loaded through their `_meta["anthropic/alwaysLoad"] = true` definitions.
Other Torii tools and shared tools load on demand through tool search.
The ChatGPT parent keeps Torii tools loaded directly through its existing
`omit_tools_from = ["deferred", "code_mode"]` setting.
Parents launch with `--system-prompt-snapshot off` because Claude Code otherwise
reuses the system prompt and tool list recorded on the first request, leaving a
resumed parent without on-demand tools or updated instructions.
The host scrubs the selected URLs and header values from captured output.
When no selected entry is valid, Claude launches omit the shared file and OpenCode
workers keep an empty `mcp` block. Torii keeps the
older files for at least an hour so a launch already starting can read its own
file. Reusing a file refreshes its age. On service restart, Torii reattaches a
live parent and compares its saved launch fingerprint with the current model,
effort, settings, instructions, and MCP config contents. A changed or missing
fingerprint moves the parent to a new host when its turn is idle. The new host
resumes the same native conversation with the current MCP entries. Workers keep
their existing host on reattach. Codex launches do not use this file.

## Why coordinators stay open

The earlier service made each owner message a numbered job and ran one cold
coordinator turn for it. A decision schema constrained each turn to one action.
That serialized follow-ups, duplicated conversation state in prompts, and put
repair policy in service code. The persistent session receives each saved message
as native input, including input that arrives while a turn runs. It can read
state through MCP tools and delegate background work without holding the turn.
Context compaction is accepted; a memory without a durable record is not a fact.

## Request flow

1. `Service.poll` checks the numeric owner and group identity. It saves each
   Telegram update and poll cursor in one transaction. An owner message creates
   one `messages` row. A message does not create a task.
2. `Service.feed` reads pending messages in row ID order and sends each to its
   topic's coordinator.
   Each input has its own receipt and starts with one tag,
   `[topic=ID name="NAME" message=N kind=KIND]`, for every kind: `owner` text,
   images, and files, `worker_result`, `restarted`, and `callback`. `NAME` is JSON
   quoted. The topic ID identifies the topic, because two topics can share a
   folder. The feed can write while a turn runs. Authenticated mid-turn delivery
   timing still needs a separate live check.
3. The coordinator creates a `tasks` row when it commits to work. A task has a
   topic, number, title, status, notes, and one worktree path. The coordinator
   passes the `message` number from the tag to `tasks.create`, and the task
   belongs to that message's topic. Each topic has its own task list and task
   numbers. Questions and brainstorming do not need tasks.
4. `workers.spawn` records a service worker. The service starts queued workers
   in the background. The coordinator may also use native Claude subagents.
   Codex work runs only in workers started by Torii on its selected account.
   Several agents may share a task worktree; one process writes to each native
   session.
5. Worker completion saves the worker result and one `worker_result` message in
   one transaction. The message belongs to the task's topic. The coordinator receives it through the same feed and
   decides what to verify, continue, or report.
6. `telegram.send` adds an outbox row. Delivery retries do not rerun work.
   For an ordinary turn, Torii sends 👀 after two tool calls or delegation,
   if no report was queued in that channel since hand-off. Creating a job also
   triggers 👀. See [message reactions](operations.md#message-reactions-and-job-reports).

The feed keeps Telegram replies as individual messages with a short quote when
available. Images and files are saved in private state before the coordinator receives
their paths. Album messages retain their own rows and receipts.

## Recovery and migration

Each provider runs under a detached host in `state/hosts/<kind>-<id>/`. The host
holds the native session lock, writes numbered stdout events to `events.jsonl`,
and accepts input through a Unix socket with a 16 MiB request line limit.
The host records accepted input in
`writes.jsonl` before it writes to the provider. The service saves the last consumed
event sequence in SQLite. On relaunch, it attaches to live hosts and reads
events after that sequence. A finished worker host has `exit.json`; the service
reads its remaining events and saves the normal worker result. A dead host
without `exit.json` marks its worker interrupted.

`CoordinatorSession.start` attaches to the topic's saved coordinator host, or
launches a new host that resumes that topic's saved native session ID. An attached
parent with changed launch inputs relaunches only after its saved events, accepted
input, pending native commands, and background work show it is idle. If a host
record cannot be read or parsed as a JSON object, the parent stays attached until
the first idle point after a live result, including command completions that
follow that result. Missing write history means no input was written. Unhandled
native event types do not affect idle state. Input still awaiting native replay
joins the existing lost-message settlement if the parent must relaunch.
The usage policy is excluded from the fingerprint because the service sends policy
edits with the next owner message. On relaunch, the service first sends a
`restarted` message with bounded summaries of open jobs, registered workers,
and uncertain input. It shows the newest 50 jobs by update time, with ties by ID,
and the newest 30 workers by ID. Job entries contain ID, topic, number, and title
cut to 200 characters. Worker entries contain ID, task, provider, and status.
Each summary includes the total count and the number omitted. The coordinator
uses `tasks.get` for job notes and `tasks.list` and `workers.list` for the rest.
A message moves from `pending` to `sent` before native input.
The coordinator receipt is the write to the session input: a completed write
moves the message to `received` with the receipt `written`, even while a turn
runs. A write that fails or times out makes it `uncertain`. When the session
replays a message, the service records the receipt `replayed`, also for an
`uncertain` message. If the coordinator process exits before it replays a
written message, that message becomes `uncertain`. A quota rejection sends the
rows of the current turn and every unreplayed row again to the next account.
Startup marks each `sent` message and each `written` message without a replay
`uncertain`, and does not resend it blindly. A restart does not stop the
coordinator host, so a message written before the restart can still reach the
conversation. After an attach, a replay that the host saved after the last
consumed event still marks that message `received`. On a resume or an attach,
the service reads the native transcript and marks `received` each uncertain
message it finds there, as a user entry or as a queued command. The `restarted`
message excludes earlier `restarted` rows from its uncertain list. It lists
the remaining uncertain messages: the newest 10 at most, each cut
to 500 characters, with at most 4,000 characters of message text in total,
plus the total count. Queued workers stay queued. The coordinator checks prior
effects before it continues interrupted work.

`service.restart` exits after the outbox drains and a private SQLite backup.
The service refuses a second restart within two minutes. Detached hosts keep
running during a service restart.
The service has no worker limit, batch resume, hold card, general audit table, or
operation block list. It enforces delivery, persistence, and recovery. The
coordinator decides task scope, delegation, repair, and reporting.

The home topic uses the `coordinator` settings prefix; other topics use
`coordinator:<topic>`. Their native session IDs and hosts are separate, while
parent processes share the configured coordinator working directory. The home
parent receives the list of all enabled topics; each other parent receives only
its own topic. Parents share one memory folder at
`<state_dir>/coordinator-memory` on every Claude account; workers keep
per-account memory. Worker execution uses the task worktree.
Both Claude launch settings exclude `CLAUDE.md`, `CLAUDE.local.md`, and
`.claude/CLAUDE.md` in each directory above the inherited agent HOME; files at
HOME and below, including account and repository instructions, still load.
Workers and parents receive the shared deletion policy. It uses the OS user's
real Trash directory on macOS or Linux rather than the agent HOME.

New installs use a neutral usage policy without task-to-provider routing, the
Claude `sonnet` model alias, and `medium` parent effort. The usage policy lives
in the install's `USAGE.md` and is separate from coordinator instructions in
`coordinator/policy.py`. A one-time Store migration preserves existing implicit
model and effort defaults. Saved settings, chat selections, policy text, native
session IDs, and host records stay unchanged. The [example policy](usage-policy.example.md)
shows settings an installer can adapt.

## Stored state

| Table | Purpose |
| --- | --- |
| `settings` and `topics` | Owner, routing, project binding, and preferences. |
| `updates` and `messages` | Telegram poll cursor, ordered coordinator input, and receipts. |
| `tasks` | Committed work and its worktree. |
| `workers` and `service_requests` | Managed processes and durable control requests. |
| `outbox` | Telegram text and file delivery with retries. |
| `attachments` | Private image and file references, with each file's safe name and download error. |
| `envelopes`, `envelope_events`, `envelope_effects` | Credential requests, their audit rows, and pending Telegram deletes and replies. |

State lives under `~/.local/state/telegram-agent-coordinator/`. Private host
directories hold provider events and control sockets. Hosts hold session locks
for their lifetimes. The service checks host PIDs when it recovers.

## Envelope vault

The owner gives a credential through the private chat with the bot. Torii keeps
accepted card values out of work-message intake and the outbox. It supplies
declared values to Claude and ChatGPT job workers through their own process
environment. The parent receives names and metadata only. Codex secret workers
require a private app-server and effective configuration checks before thread
start or resume. They disable shell snapshots, memory generation, native
subagents, and tool-output telemetry. See [Credentials](operations.md#credentials)
for the controls and refused environment names.
Torii scrubs captured worker output, with exceptions for JSON object keys and
canonical UUID values under protocol identity keys. Native tools, transcripts, files, encoded output, and provider
requests are outside that filter. Torii stores accepted values in `envelope/vault.enc`.

| Path | Mode | Content |
| --- | --- | --- |
| `<state_dir>/envelope/` | 700 | Directory, created on first use. |
| `<state_dir>/envelope/master.key` | 600 | 64 hex characters, generated once and never overwritten. |
| `<state_dir>/envelope/vault.enc` | 600 | One sealed JSON map, rewritten through a temporary file and `os.replace`. |

Python 3.9's standard library has no AEAD, and Torii has no third-party
dependencies. `coordinator/vault.py` therefore seals the map with HMAC-SHA256
in counter mode and authenticates it with HMAC-SHA256 over the nonce and the
ciphertext (encrypt-then-MAC). Subkeys for encryption, the tag, and fingerprints
come from the master key with the labels `torii-envelope-v1/enc`, `/mac`, and
`/fingerprint`. Each write uses a new 16-byte random nonce. The tag check runs
before decryption, so a changed byte or a wrong key fails with `integrity`. A
known-answer test pins the format. A later AEAD is a new `alg` value.

The master key comes from `TORII_VAULT_KEY_FILE` (a mode-600 file with 64 hex
characters or 32 raw bytes) or from `envelope/master.key`. The service generates
that file once through a temporary file and `os.link`. It refuses to generate a
key while `vault.enc` exists without one (`key_missing`). At start, `serve`
removes `TORII_VAULT_KEY_FILE` and `TORII_VAULT_KEY` from its environment, so no
child process inherits them. Only the service opens the vault; the MCP server
and the CLI never hold a vault object. Vault calls never run inside a SQLite
transaction. The `service.lock` held by `serve` keeps a second writer away.

Intake runs in `Store.accept`. `prepare_private` checks the owner, the private
chat, and the value, and writes the vault before the update's transaction.
`accept_private` then writes the state, the audit rows, and the effects in a
savepoint, and the update cursor commits once. `ProviderRunner.run` sends a
fresh worker's declared values to its host through the launcher's stdin, never
through `spec.json`. The host adds them to the child's environment only. It
scrubs each value, and its JSON string forms, from every stdout event before it
writes `events.jsonl` and from every stderr line before it writes `stderr.log`.
JSON keys and the UUIDs that name sessions, threads, and messages keep their
form, so events still parse. `ProviderRunner.run` also scrubs every result
field. A worker that reattaches to its host after a restart reads no value and
records no use, because the host and its child already hold the values.
Restart backups copy only the database, never the vault or the key. The store
runs with `secure_delete` so that cleared card links do not stay in free pages.

The `CHECK` lists on `envelopes.state` and `envelope_events.event` are frozen
by `CREATE TABLE IF NOT EXISTS`. A new state or event name needs a table rebuild
in `Store.__init__`, like the `messages` rebuild.

Threat model summary: a stranger in Telegram cannot arm or fill an envelope. A
process of the same OS user that can read both `master.key` and `vault.enc` can
read every value, as it can read a Keychain item that `/usr/bin/security`
created. A worker can also read another worker's environment. Running the
service as its own OS user, or keeping the key file outside the state directory
through `TORII_VAULT_KEY_FILE`, gives more separation. Provider transcripts under
`~/.claude` and `~/.codex` are outside Torii's scrubbing. Encoded or split forms
of a value are not scrubbed. Telegram keeps its own copy of a deleted message
outside Torii's control.

## Code map

| File | Responsibility |
| --- | --- |
| `coordinator/store.py` | Schema migration, intake, tasks, messages, worker results, and outbox. |
| `coordinator/parent.py` and `coordinator/parent_state.py` | Parent selection, effective settings, and shared recovery. |
| `coordinator/session.py`, `coordinator/codex_session.py`, and `coordinator/opencode_parent.py` | Provider-specific parent clients and input receipts. |
| `coordinator/host.py` | Detached provider process, event file, and socket control. |
| `coordinator/native_protocol.py` | Claude input replay and Codex steering. |
| `coordinator/providers.py` and `coordinator/workers.py` | Native worker execution. |
| `coordinator/workspaces.py` | Task worktree creation and serialized Git worktree changes. |
| `coordinator/service.py` | Poll, feed, worker, control, delivery, and restart loops. |
| `coordinator/mcp.py` and `coordinator/control_api.py` | Coordinator tools and durable service requests. |
| `coordinator/extension.py` | Optional local launch behavior. The default leaves native Claude launches unchanged. |
| `coordinator/reactions.py` and `coordinator/telegram.py` | Admission reaction and Telegram transport. |
| `coordinator/envelopes.py` and `coordinator/vault.py` | Credential requests, private intake, effects, and the encrypted file vault. |
| `coordinator/scrub.py` | Removes injected values from captured worker output. |

Claude parents stay on an eligible account. When that account becomes
ineligible, Torii waits for the turn and background tasks to finish, then restarts
with the same transcript under the next eligible profile. Claude Code owns each
profile's sign-in. Torii never reads or passes Claude sign-in tokens. See
[Claude accounts](operations.md#telegram-controls).
