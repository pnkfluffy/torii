# Control API

`coordinator/control_api.py` defines the operations used by the coordinator's
MCP server, Telegram menus, and the local CLI. `coordinator/mcp.py` exposes the
23 coordinator tools below and these 12 read-only operations through `tools/list`:
`settings.show`, `topics.list`, `topic.show`, `workers.get`, `accounts.list`,
`account.current`, `account.show`, `policy.show`, `projects.list`,
`problems.list`, `problems.summary`, and `tasks.stale`. That is 35 MCP tools.
The account exceptions are `account.use`, only when the owner picks a Claude or Codex account,
`accounts.claude_auto`, only when the owner asks to clear the Claude pick, and `account.redeem`, only when the owner asks to
consume a banked reset on the active Codex account. Chat agents can also request
`topic.create`, `topic.close`, per-chat `model.parent`, and `policy.chat` with an owner message. Every other operation, including account enable and disable, delegation switches, global model, global policy, and other topic
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

`events.post topic=ID text=TEXT` is local CLI only. It stores a tool event even
when the service is stopped. The service sends it to that channel's coordinator
on its next feed drain, tagged `kind=event`. Text has a 16,000-character limit.
Unnamed events create no Telegram send or reaction. This operation is absent from MCP
and Telegram menus. When `reply_to` names an event in the same channel,
`telegram.send` sends without a quote and returns `quoted=false`.

Torii always cleans finished host logs.

Cleanup deletes a worker's event and write journals after its process exits, its
result is saved, and its steer receipts settle. A failed or interrupted worker
keeps its whole host folder until its job round closes as done or dropped, or a
newer round opens. Cleanup then applies the usual process, receipt, and worker-temp
checks. For other workers, Torii keeps `stderr.log` and resume records until the
job is done or dropped with all workers finished when `close_clears_space` is off.
A launch spec still needed for pending worker-temp cleanup stays until that
cleanup finishes and requests the remaining host deletion.
Cleanup removes ended parent hosts after their saved references clear. Startup and
those lifecycle events request background passes without an age rule or periodic
finished-worker scan. Explicit failed-launch records or valid dead host and child
records cover hosts without `exit.json`; missing proof keeps the folder. Failed
deletes record a throttled problem and retry with durable exponential back-off.
See [finished host logs](operations.md#finished-host-logs) for process proof,
resume data, interrupted deletion, and files that stay.

The MCP server uses newline-delimited JSON-RPC. `tools/list` supplies the input
schemas. `tools/call` validates arguments, calls the handler, and returns the
handler text and data. An unknown tool is an error. Native tool calls are in the
coordinator transcript.

## Wait for a PR, update or signal

All wait triggers require `waits_enabled=true`, controlled by the local CLI
operation `waits.set_enabled`. The default is off. With the switch off, Torii
makes no GitHub polling calls, performs no update-wait work, and refuses
`pending.signal`.

`tasks.wait kind=pr pr=URL until=checks|merged ask=TEXT task=ID` arms a GitHub
PR wait. `until` defaults to `merged`. Checks that pass reopen a checks wait
for the coordinator to merge. An automatic merge keeps it waiting for merge.
A merged wait keeps waiting when checks pass or fail, including failed queue
status checks. A checks wait reopens on failed checks. Both forms reopen on
closure without merge or checks observed pending for three hours on the same
head commit. A new head resets this clock. A PR with no configured
checks does not become stuck. Declared
`then_*` fields queue a worker when the round opens.

The waits loop makes one batched GraphQL request per repository with due
waits. It uses `code_requests.run_github` and its 30-second timeout. Each wait
is due every minute for its first 30 minutes, then every five minutes. A GitHub
transport failure or missing response data delays the repository's waits for
15 minutes and fires none. An invalid PR entry delays only its own waits.
Three consecutive not-found answers cancel that wait and notify its chat.
A renamed or transferred PR URL also cancels the wait and notifies its chat.
Default PR, update and signal keys include the waiting job or chat and its ask.
An explicit key can share a trigger between jobs.

A merged PR settles `finish=merged` in its current round with merge evidence.
For `finish=live`, it stamps `merged`. A declared
`next={"kind":"torii_update"}` then waits for the merge commit. The optional
`commit` in that object selects another full commit hash. With no follow-up,
Torii opens the next round. The local CLI accepts `next` as a quoted JSON object.

`tasks.wait kind=torii_update commit=HASH ask=TEXT task=ID` checks whether the
commit is an ancestor of the captured running Torii commit. It checks once when
the service processes the arming request, at start, and after a successful update,
including recovered updates. For `finish=live`,
it stamps live, writes update evidence to the job's results folder, and settles
the current round. A declared worker instead opens the next round for a live
check. Other finish lines also open the next round.

`tasks.wait kind=signal signal=NAME ask=TEXT task=ID` waits for a named signal.
`pending.signal name=NAME` is local CLI only, refused over MCP and from worker sources.
Telegram sources are also refused. Any process with local CLI access can call it.
Torii emits `topic.closed`,
`worker.reattached`, `service.started`, and `update.landed`. Signals are one-shot
and global. They notify waits armed when the event occurs. `owner.messages:<n>`
is not emitted.

## Post a note from a local bot

Run these commands as the same user that runs Torii on this Mac. No key or
setup step is needed. List chats, then use a chat ID from the result:

```sh
python3 -m coordinator --state-dir /path/to/state ctl call topics.list
python3 -m coordinator --state-dir /path/to/state ctl call --from "Research Bot" events.post topic=-10042:11 text="The research is ready."
```

Put `--from NAME` immediately after `call`. Torii trims outer whitespace. The
name must have 1 to 40 characters on one line. Control characters, double
quotes, `[` and `]` are refused before Torii opens the Store.

The chat agent sees `kind=event from="Research Bot"` in the message header. Torii
stores the name separately from the text. The name identifies what the program
calls itself. It does not identify the owner. The agent treats the event as a
note from another chat.

The owner sees `🤖 Research Bot` and an expandable quote in that Telegram topic.
The quote has at most 800 characters, with an ellipsis for longer text. The
agent receives the full event text. The Telegram note is silent and has no
buttons, reply quote, or reactions. Closing and closed chats receive no note.
Omit `--from` to keep the existing event behavior with no Telegram note.

Other local write calls can also use `--from`. A queued request keeps the name,
and its "Chat created" notice names the program. Torii keeps the newest 1000
CLI write call records in `cli_calls`, including unnamed and refused calls.
Each record has the time, operation, name or null, chat, result state, and
calling app's name from the kernel (`ps -o ucomm=`). Torii skips shell parents
(`sh`, `bash`, `zsh`, `dash`, `fish`, `ksh`, `csh`, `tcsh`, and `nu`), `env`, and
Python parents. It records the first other program up the parent chain. A Python
bot records the program that started it. A command typed in a macOS Terminal
shell records `login`.

On macOS, a Node bot records `node`, and macOS cuts this name at 16 bytes. On
Linux, `ucomm` is the kernel task name, which a program can rename. Node sets it
from `process.title`, so a Node bot that sets `process.title` may record up to 15
bytes of its title instead of `node`. If truncation cuts the name inside a
multi-byte character, the recorded name ends with U+FFFD. Records contain no
parameter values, process arguments, or environment. If recording fails, the
call keeps its own result and exit code. Read calls create no record. An invalid
name creates no record. The name is local CLI metadata and is unavailable over
MCP or Telegram.

## Coordinator tools

| Operation | Effect |
| --- | --- |
| `account.use` | Save the owner's Claude pick and check fresh usage, or select the active Codex account. |
| `accounts.claude_auto` | Clear the Claude pick when the owner asks; eligible running chats stay put. |
| `account.redeem` | Redeem a banked reset on the active Codex account only when the owner asks and a resettable meter is limited or at least 95% used. |
| `telegram.send` | Save a message in the Telegram outbox. |
| `topic.create` | Queue a new Telegram topic and project link. Requires `message=N` over MCP. |
| `model.parent` | Pick or clear one linked chat's Claude or ChatGPT agent. Requires `topic` and `message=N` over MCP. |
| `policy.chat` | Set or clear one linked chat's delegation instructions. Requires `topic` and `message=N` over MCP. |
| `topic.close` | Retire a chat. With `folder=trash` or `folder=keep`, `message=N` is the owner's confirmation, including on the CLI, and no card is sent. Without `folder`, Torii sends a confirmation card. Requires `message=N` over MCP. |
| `tasks.create`, `tasks.update`, `tasks.get`, `tasks.list` | Manage committed work. |
| `worktree.create` | Create or return the task's worktree. Optional `base` selects a remote branch. |
| `workers.spawn` | Queue a service worker in a task worktree. |
| `workers.steer`, `workers.stop`, `workers.goal` | Send a durable request to a managed worker. |
| `workers.list` | Read a page of worker summaries, newest first. |
| `workers.get` | Read one worker with its full prompt and result. |
| `service.restart` | Request exit after the outbox drains and a private SQLite backup succeeds. |
| `secret.ask` | Post an Envelope card that asks the owner for a credential by name. |
| `secret.list` | Read names, states, lengths, fingerprints, consumers, and last use. Never values. |
| `secret.rotate` | Ask again with the previous reason, consumer, and task. |
| `secret.revoke` | Service operation. Remove the value from the vault and close its envelopes. |
| `secret.rename` | Local CLI service operation. Rename a saved key and preserve each job's environment names. |

Torii cleans up closed jobs' code copies on startup and every hour. By default,
a clean done or dropped job becomes eligible after 24 hours without an update.
Torii moves that copy and its Git registration to the Trash.

The local CLI operation `cleanup.delete enabled=true` enables immediate permanent
deletion. It ships off. `settings.get` reports `closed_copy_delete`.
`cleanup.delete` is unavailable over MCP. With this switch on, closing a job
checks its copy immediately and reports whether it was deleted or kept, with a
reason. Uncommitted changes or untracked files keep the copy until they are
committed or discarded. An unfinished worker keeps it until that worker ends.
Startup and the hourly backstop also retry kept copies, without an age rule.

Deletion skips unsafe paths, links, submodules, detached HEADs, Git operations
in progress, locked worktrees, and moves across filesystems. Ignored files go
with the copy. A safe copy is renamed into private staging and its Git
registration is removed before the close answer. Background deletion releases
the bytes without using the Trash. Branches, commits, shared stashes, and the
saved worktree path stay. Reopening rebuilds the same local copy from its branch
without cloning. See [worktree cleanup](operations.md#telegram-controls).

`tasks.get`, `worktree.create`, and `workers.spawn` include the job's retained
`results` path, `<state>/results/task-<id>`. The first worktree creation or worker
spawn creates it with mode 700. Torii never deletes or moves this folder.

`cleanup.close_space enabled=true` selects the new close policy. It ships off.
To enable it, run `python3 -m coordinator ctl call cleanup.close_space enabled=true`
from the local CLI. `settings.get` reports `close_clears_space`. While it is off,
closed-job copy saving stays off. Finished host-log cleanup continues independently.

`cleanup.close_rules` and `cleanup.retry` are local CLI operations. Other callers
are refused. `settings.get` reports these settings.

| Parameter for `cleanup.close_rules` | Setting | Default | Behavior |
| --- | --- | --- | --- |
| `submodules` | `close_saves_submodules` | `false` | Requeues closed holds beginning with `worktree uses submodules` when enabled. The submodule save rule is a later change. |
| `any_branch` | `close_saves_any_branch` | `false` | Requeues closed holds beginning with `worktree is not on the saved job branch` when enabled. Saving other branches is a later change. |
| `prune_held` | `close_saves_prune_held` | `false` | Reserves the switch for a later held-copy prune rule. It does not prune or requeue jobs. |
| `discard_names` | `close_saves_discard_names` | `[]` | Comma-separated file or folder names to discard when Git lists them as ignored, using the existing regenerable-file rule. Supplying a nonempty list requeues closed `ignored files exceed` holds. |
| `keep` | `close_saves_keep` | `[]` | Comma-separated positive task IDs. These jobs are excluded from rule requeues and `cleanup.retry`. The later held-copy prune rule uses the same keep check. |
| `requeue` | Not stored | Enabled for new rules | `false` saves settings without requeueing. `true` also requeues matching holds for supplied rules already enabled. |

List parameters replace the saved list. An empty string clears a list. Names
must be single path components. Requeues affect only held copies of closed jobs,
leave job status unchanged, and clear the previous copy reason. Turning a rule
off does not requeue jobs. Enabling a rule does not change deletion gates in this
release. The discard list takes effect in ignored-file handling.

While `cleanup.close_space` is off, rule changes save their settings but do not
requeue jobs. The reply reports that close-space is disabled. Enable it before
using `requeue=true` to drain matching holds.

To try one held job before draining its matching backlog, set a rule with
`requeue=false`, then run `cleanup.retry task=<id>`. Retry needs
`cleanup.close_space` enabled and an existing closed job. If the job is in
`close_saves_keep`, retry refuses with that setting's name. After checking the
saved artifacts, repeat the rule with `requeue=true` to drain matching holds.
For example, `cleanup.close_rules submodules=true requeue=true` requeues
submodule holds. `cleanup.close_rules keep=2166,2741` keeps those task IDs out of
rule requeues and manual retries. These commands use the same
`python3 -m coordinator ctl call` prefix as `cleanup.close_space`.

With it on, closing queues a durable save. Torii copies named proofs, saves edits
through a temporary Git index to a WIP ref, and verifies a bundle of commits no
remote has. It does not push. It copies other ignored files up to 200 MB, discards
regenerable files, and holds secret-looking names such as `.env`. An active worker,
a queued image, a Git operation, a lock, hidden index flags, or submodules also
hold the copy with a reason. Git conversions that change raw working-file bytes
also hold it, since the snapshot could not restore those bytes exactly. A hold
leaves the job status unchanged.

Untracked regular files have a 200 MB total limit before Git writes a snapshot.
An over-limit hold names the largest top-level path and total bytes. Symlinks
do not count toward this limit. A Git timeout names its command and timeout.
Every hold has a nonempty reason. A new reason records one
`cleanup/copy-held` problem for that task; retrying the same reason does not
repeat the problem. Existing holds are not swept or requeued at startup by
these rule settings.

Only a verified save permits staging and deletion of the owned copy, job scratch,
and finished worker temp. Host-log cleanup and worker temp cleanup are enabled
under this policy. Shared caches, results, branches, and native history stay.
Chat scratch clears when its idle parent stops with no open job. Deletion uses
at most 1,000 entries or 100 ms per batch, then pauses for 200 ms. These are pilot
load limits, not a measured fix for macOS daemon memory.

The save queue drains at most five jobs per pass and releases the cleanup lock
between passes. Remaining jobs run immediately without a timer or owner action.
Staged deletion retries keep their existing retry delays. Each round's
`task_space_saves.data.freed` records bytes under `copy`, `deps`, `extras`, and
`prune`. `deps` covers discarded ignored files; `extras` covers scratch and
worker temp. `prune` stays zero until that later rule exists. The existing
`task_iterations.bytes_freed` total remains unchanged.

Reopening rebuilds at the same path, restores saved edits and ignored files,
and records `rebuild_seconds` on the current round. The worker receives an install
reminder. Dependencies are installed only when checks require them.

Workers always receive disposable temp folders. See [worker storage](operations.md#worker-temp-and-job-results-folders)
for launch, resume, process proof, and cleanup behavior.

Codex workers load goal tools when their launch has a goal.
A goal added later waits for the current turn to finish. Torii waits for that
app-server to exit, resumes the same native thread with goal tools, and applies
the goal without repeating the original prompt. Steers wait through this change.
A goal is received only after Codex acknowledges it. Reattachment keeps the
existing host and its launch options.

## Create and close chats

The service owns Telegram calls. Both operations return a queued request ID and
"Torii will send the result here". The asking chat receives an event with the
outcome, so its agent can report without checking the request repeatedly.

```sh
python3 -m coordinator ctl call topic.create name="Research"
python3 -m coordinator ctl call topic.create name="Research" cwd=/absolute/existing/folder message=123
python3 -m coordinator ctl call topic.create name="Research" provider=codex model=gpt-6-luna effort=low
python3 -m coordinator ctl call topic.close topic=-10042:11 message=124
python3 -m coordinator ctl call topic.close topic=-10042:11 message=124 folder=keep
```

`topic.create` requires `name`, with the same name rules and folder slug as
`/project new`. Optional `cwd` links an existing folder. Otherwise Torii creates
the folder under the projects root and initializes Git. It then creates the
Telegram topic, commits its link and carried settings in one transaction, and
queues a ready line with the folder, provider, and model shown by its status card.
The asking agent receives the new topic ID, name, folder, and chat agent. Creation has no switch.

Optional `provider=claude|codex|opencode`, `model`, and `effort` pick the new chat's agent.
OpenCode requires `delegation.opencode` to be on and an account selected with
`opencode.use`. OpenCode has no thinking setting, so Torii refuses `effort`.
An optional `model` must equal the active OpenCode account's model. Torii stores
null model and effort fields, so the account always supplies the model.
The types and refusals match `model.parent` with a topic. Model or effort requires
provider. For Claude and ChatGPT, a model must belong to its provider. Claude models start with `claude-`
or equal `opus`, `sonnet`, `haiku`, or `fable`, with exact, case-sensitive matching.
For `provider=codex`, other model IDs belong to ChatGPT. A mismatch returns one of these refusals:

- `Send a Claude model with provider=claude, for example claude-opus-5-5.`
- `Send a ChatGPT model with provider=codex, for example gpt-6.1-sol.`

Torii checks these inputs before queuing the request or making a folder.
The pick has revision 1 and commits with the link, before the ready line and the
first agent launch. Without these inputs, Torii writes no pick and uses the default.
For example, `topic_create` accepts
`{"name":"Research","message":123,"provider":"codex","model":"gpt-6-luna","effort":"low"}`.

Optional `delegation` sets the new chat's own delegation instructions before
its first agent starts. Torii strips the text and checks its 2000-character cap
before queuing, creating a folder, or calling Telegram. Revision 1 commits with
the link and optional agent pick. When you supply delegation, the ready notice
includes the line `Delegation updated.` The result event shows its character
count. Without it, creation uses the usage policy.
For example, `topic_create` accepts
`{"name":"Research","message":123,"delegation":"Use Codex for research."}`.

The MCP calls are `topic_create` with `{"name":"Research","message":123}` and
`topic_close` with `{"topic":"-10042:11","message":124,"folder":"keep"}`. Both require a saved
owner message in the calling parent's channel. Native parents receive a scoped
`mcp --topic=ID` configuration. A parent with an older unscoped configuration
must restart before using these tools. Workers receive neither operation.
Neither tool is always loaded. On the CLI, omitting `message` sends the result
to the home chat, except that closing with `folder` requires `message`.

Owner messages in an unlinked topic during creation wait outside parent intake
until the link commits. They do not trigger setup or a parent. Creation does not
use the owner's `/setup` or `/project new` form state.
Saved request phases survive service restarts. A failed topic or link moves a
newly owned folder to Trash. A failed link also deletes the new Telegram topic,
or reports that it remains unlinked if deletion fails. Ready-message delivery
uses normal outbox retries and leaves the link enabled.

Telegram has no creation idempotency key. If the service stops after issuing
`createForumTopic` and before saving its returned ID, it never repeats that call.
It reports the uncertain, possibly unlinked topic and moves its owned folder to
Trash. An interrupted folder without a saved ownership record stays in place
for inspection. These two acknowledgement windows require operator inspection.

Closing refuses General, home, unknown, closing, and closed chats, and names
unfinished workers of every provider. An already disabled chat can be closed.

Optional `folder=trash` or `folder=keep` uses the saved owner message as the
confirmation and sends no card. It requires `message=N` on the CLI too. The
message must be an owner message, and over MCP it must come from the calling
parent's chat. `trash` moves the folder to Trash; `keep` leaves it in place.
Torii refuses `trash` unless the folder is a real, unused folder strictly inside
the projects root. It checks again before disabling the chat. A refusal there
reports that nothing was closed.

Without `folder`, the confirmation card names the irreversible Telegram deletion, agent stop,
open jobs to drop, folder, sharing chats, Git branch, changed files, and commits
absent from all remotes. The owner's tap confirms this path. The Trash button appears
only for a real, unused folder strictly inside the projects root. Shared folders,
ancestors, descendants, symlinks, and external folders offer only keep or cancel.
A changed snapshot produces a fresh card with no close effects.

Confirmation disables the chat and stops its existing agent, including a
recorded host recovered after restart. Torii drops open jobs while keeping their
notes, retires outgoing messages and reactions, and deletes the Telegram topic.
The final report names each dropped job as `#number title`, or says there were none.
If deletion refuses, Torii tries closing it and reports why manual deletion is
needed. The chosen folder move uses Trash and a suffix for an occupied name.
No Store row or native history is deleted. The disabled topic cannot be enabled
again. Repeated taps receive an already-handled reply. Closing the asking chat
routes its final report and event to home.

## Job worktree start point

`worktree.create task=NUMBER base=BRANCH` starts a new job at the fetched tip of
`BRANCH`. `base` is optional. Both `feat/example` and `origin/feat/example` name
the same branch. The fetch uses the checkout branch's upstream remote, or
`origin` if the checkout branch has no upstream.

Without `base`, a new job starts at the fetched upstream tip of the checkout's
current branch. Without an upstream, it starts at checkout HEAD. The fetch
updates only the selected remote-tracking ref. It fetches no tags and disables
automatic garbage collection and maintenance. It cannot prompt for credentials
and has a 30-second timeout. A failed or timed-out fetch uses the existing
remote-tracking ref. If that ref is absent, the operation refuses with the
branch name and Git error, before it creates a job branch or folder.

The result includes `base_ref`, such as `origin/main` or `HEAD`, `base_commit`,
and `fetched`. `fetched` is true only when this call fetched the branch. The job
branch starts from the resolved commit and has no upstream. Creation preserves
the source checkout's HEAD, local branches, index, and working files.

An existing job worktree is returned without a fetch or reset. Its result has
`base_ref=HEAD`, its current commit as `base_commit`, and `fetched=false`.
If the call passes `base`, the result text says that it was not applied.

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

`tasks.update` accepts optional `stage=ready`, `reviewed`, `ci`, `merged`, or
`live`, alone or with other job fields. Each call appends a milestone with the
server's UTC time. Repeated stages remain in the history, so a CI rerun can
stamp `ci` again. Unknown stages and client timestamps are refused.
`tasks.get` returns `stamps` in append order and prints each stage and UTC time.
Each stamp contains `id`, `task`, `stage`, and `created` as Unix seconds.
`tasks.list` does not include stamps. Stage stamps have no retention limit.

Job disposition and automatic continuation also ship off. Enable waits first with
`python3 -m coordinator ctl call waits.set_enabled enabled=true`. Then enable
`auto_resume` with `python3 -m coordinator ctl call tasks.auto_resume enabled=true`.
These are local CLI controls. Turn auto-resume off before disabling waits.

With `auto_resume` on, a chat settles after its turn, background tasks and pending
messages finish. An open job without a worker gets one coordinator reminder, then
an owner wait with Resume and Drop buttons. An owner stop goes directly to that
wait. A coordinator stop remains held until the chat settles.
Jobs with a worker parked for a secret or quota are excluded from disposition.
Their existing secret or quota path remains responsible for the worker.

Failed or unfinished workers interrupt their job. The round records a cause of
`crash`, `unfinished`, `quota`, `account`, `offline`, or `config`. Crash and
unfinished failures get at most three automatic continuations, after zero,
120 and 600 seconds. A changed Git HEAD or a successful `RESULT: done` or
`RESULT: blocked` resets that count. Quota, account and offline conditions spend
no attempts. A condition failure from an automatic continuation counts as a
crash. An account becoming available preserves the previous attempt count.
Configuration failures get a coordinator reminder and then an owner wait.

Until worker policy requests a final result line, a successful result without
one gets normal disposition instead of an automatic continuation. A present
`RESULT: partial` line or an unsuccessful result counts as unfinished.
`REQUIRE_RESULT_LINE` in `coordinator/lifecycle.py` controls missing-line
enforcement and is false until that policy rollout.

Each automatic continuation posts its worker, cause and counted attempt to the
job's chat. A wait firing names a continuation that it already queued.
The owner's last failure detail uses the short error, limited to 160 characters.
A held code copy can resume in place. A missing or mismatched held copy creates
an owner wait with the copy's reason. A copy still being saved waits for saving.

Each continuation is a new worker row with the saved model, effort and work.
It reuses the native session when the previous writer has stopped and resume is
supported. Quota continuations rebuild a saved code copy when needed. A final
provider result gets a five-second exit grace period before its host is drained.
With the switch off, disposition, status and restart-notice behavior stay unchanged.

Use `python3 -m coordinator ctl call tasks.finish_default topic=1:2 finish=answered`
to set one chat's default. Omit `finish` to clear that override. An explicit chat
default wins. New installs have no implicit finish default in any chat.
This control is available only through the local CLI.
`tasks.update finish=` can change an open job's finish line.

## Topic filters

`tasks.list` with `topic` returns only that topic's tasks. `tasks.get` with
`topic` refuses a task of another topic. Calls from the CLI with `--topic` or
from a Telegram topic fill `topic` from that context. A topic argument is a
topic ID or a name that no other topic shares, ignoring case.

Topic names are display labels and can repeat. Use topic IDs when names repeat.
The message tag always carries the topic ID.

`topic.rename topic=ID name="⛓️‍💥 Workspace"` queues a Telegram `editForumTopic`
request for the service. It keeps the saved label until Telegram accepts the
rename, then saves both the label and the Telegram title. Torii reports completion
or refusal in the asking chat and stores the result in `service_requests`.
With no asking chat, the result goes to the renamed topic.
Names contain 1 to 128 UTF-16 code units after outer spaces are removed.
For example, `⛓️‍💥 Workspace` uses 15 units.
General topics, chats outside the current group, and closing or closed chats
are refused. Telegram can refuse the rename if the bot lacks
`can_manage_topics` permission. A Telegram refusal keeps the saved label and reports the error code
and known reason. A rate limit reports the retry delay when Telegram provides it.
A timeout, server error, or restart during the send keeps the saved label and
reports an uncertain result once. Torii does not repeat that send automatically.
Check the Telegram title before trying again.

A Telegram-side rename still updates the saved label, including a name already
used by another topic. An echoed title that matches the saved name and Telegram
title does not write the topic again or queue a rename.

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
page. `workers.get` returns the complete record. While the write lease is on,
a queued dev worker held by its job's running dev worker carries `waits_for`.

## Text limits

A `text` parameter, such as `telegram.send` text, task notes, a worker goal, or a
restart reason, holds at most 16,000 characters. A worker brief, the `prompt` of
`workers.spawn` and `workers.steer`, holds at most 50,000 characters. A steer to
a queued worker is appended to its brief; the service refuses a steer that
would take the brief over 50,000 characters. `tools/list` gives each limit as
`maxLength`.

`policy.chat` text and `topic.create` delegation are stripped and limited to
2000 characters. Blank text, surrogate characters, and control characters other
than newline and tab are refused before any write. Omit `policy.chat` text to clear it.

`workers.steer` also resumes a `done` worker when it is the newest worker of an
open job, has a saved session, its last run was not a failed fresh start, and no
orphan child is still alive. The service checks eligibility again and requires
the previous worker task to have finished. It requeues the same worker and
session with `fresh=0` under host `worker-N-resume-R`, where `N` is the worker ID
and `R` is the steer request ID. The final message's punctuation has no effect
on status or eligibility.

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
Credential asks prompt the owner privately, one at a time. A waiting card's
**Fill privately** callback starts its prompt when no other ask is taking a value.
New envelopes have no per-card token. Legacy token hashes stay in the Store and
are ignored.

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

`secret.rename name=OLD new_name=NEW` moves a saved key inside the running service.
For example, run `python3 -m coordinator ctl call secret.rename name=GITHUB_TOKEN new_name=GITHUB_PERSONAL_TOKEN`.
The owner does not need to send the value again. The request result names both
keys and counts the jobs whose declarations changed. It sends no Telegram
message and does not edit existing envelope cards.

Both names must be valid secret names and must differ. The source needs a filled
envelope and a stored value, with no open or armed envelope. The destination must
be absent from every envelope row, the vault, and the service environment.
The service verifies the copied value before it moves the name references and
removes the source. If interrupted, repeat the same command to finish it.
An unfinished rename reserves both names against new asks, rotations, and revokes.

The new name retains the key's metadata, use history, and sharing rules.
All jobs keep their environment names. `OLD` becomes `OLD=NEW`, and
`ENV_NAME=OLD` becomes `ENV_NAME=NEW`. New and resumed workers read the renamed
key; running workers keep their environment. This operation is absent from MCP
tools and the `/secrets` buttons.

Each job's `secrets` declarations can use stored keys asked for any
job, including keys asked for different jobs and `ENV_NAME=VAULT_NAME` mappings.
A declared key with a live unfilled card still makes the worker wait.
Unasked, expired or cancelled keys and vault read failures still fail.

`secret.list` returns distinct worker-used jobs in each key's
`jobs` list, most recently used first. Each entry has the global task `id`,
channel id (`topic`), channel name (`channel`) and channel-local `number`.
Its text and `/secrets` show up to three channel names with their job numbers
and a count of the remaining jobs on one short line. The key card labels this
list **Jobs**. The asked-for task remains in `task` and the card's **For** line.
A key name holds one value, and a new fill replaces it for every job, so
different accounts need different key names.
`secret.revoke` removes a key for every job. Running workers keep their copy
until they exit.

## Setup and settings operations

`setup.topics_check` requests a group Topics check. `setup.claude` and
`setup.chatgpt` start the shared account sign-in flows.

`settings.show`, `settings.get`, `topics.list`, and `topic.show` read saved
state. `topic.setup_new`, `topic.setup_use`, `topic.setup_cancel`, `topic.bind`,
`topic.folder`, `topic.enable`, and `topic.disable` manage project routing.
`topic.rename` renames the Telegram topic and saved label.
`projects.list` and `projects.root` manage the project picker.

Account and delegation controls include `accounts.list`, `accounts.discover`,
`account.show`, `account.current`, `account.add`,
`account.signin_cancel`, `account.enable`,
`account.disable`, `account.reset`, `account.use`, `accounts.claude_auto`, `accounts.codex_auto`,
`delegation.codex`, `model.worker`, `model.codex`,
`model.coordinator`, `model.parent`, `effort.parent`,
`policy.show`, `policy.chat`, and `policy.set`. `policy.show` reads USAGE.md, with the bundled
default when the file is missing. `policy.set` atomically replaces USAGE.md.
Coordinators load it with their next message; new workers load it at launch.
The other settings affect future dispatches.
`model.coordinator` takes effect at the next coordinator launch.

`policy.chat` sets or clears delegation instructions for one linked chat,
including home. They add to the usage policy and win on conflicts in that chat.
Coordinator rules and owner-only controls, including Codex account choice, stay
in force. The parent loads changes with its next message without interrupting
its turn or starting a new native session. Workers keep their current settings.
Torii queues `Delegation updated.` when you set instructions and
`Delegation cleared.` when you clear them in the target chat. Repeating the
same text changes no revision and queues no notice.

```sh
python3 -m coordinator ctl call policy.chat topic=-10042:11 text="Use Codex for research."
python3 -m coordinator ctl call policy.chat topic=-10042:11
python3 -m coordinator ctl call policy.show topic=-10042:11
python3 -m coordinator ctl call topic.create name="Research" delegation="Use Codex for research."
```

Chat agents call `policy_chat` with
`{"topic":"-10042:11","message":123,"text":"Use Codex for research."}`.
Omit `text` to clear it. Both `topic` and an owner `message=N` from the asking
parent's channel are required over MCP, as for `model_parent`. The target must
be linked in the current Telegram group. CLI `message` is optional and unused.
Torii refuses a chat that is closing or closed, with `This chat is closing or closed.`
Disabled chats remain valid targets when they have no lifecycle state.
The tool is deferred. `policy.set` stays owner-only.

`policy.show topic=ID` returns the exact combined policy that chat's agent gets.
Without `topic`, it returns USAGE.md unchanged. The status card shows a
whitespace-collapsed preview only when the chat has its own instructions.

`effort.parent provider=claude effort=high` sets parent thinking for that provider.
The provider is required and accepts `claude` or `codex`. Effort accepts `low`,
`medium`, `high`, or `max`; Codex maps `max` to `xhigh`, as workers do.
Omit effort to restore `medium` parent thinking. Each provider has its own
setting. It applies at the next parent launch. A changed launch fingerprint
relaunches the parent after active work settles and resumes the same native
session, subject to the existing recovery path if native resume fails.

`model.parent provider=codex model=gpt-6-astra` selects the provider and model
for every parent chat without its own pick. `model.parent provider=claude model=claude-opus-5-5`
selects Claude as the default. With `topic`, `model.parent` can pick or clear
one linked chat's Claude, ChatGPT or OpenCode agent. The home chat stays on
Claude or ChatGPT:

```sh
python3 -m coordinator ctl call model.parent topic=<id> provider=codex model=gpt-6-luna effort=low
python3 -m coordinator ctl call model.parent topic=<id>
```

The first command picks Luna at low thinking for that chat. The second clears
its pick and restores the default. A chat pick wins over the global pick.
For Claude and ChatGPT, omit `model` to use the picked provider's default model.
Omit `effort` to use its `effort.parent` setting, or `medium` when unset.
Existing installs retain their previous implicit `max` effort as saved settings
until the owner changes or clears them.
Codex maps `max` to `xhigh`.
With `topic`, supplying `model` or `effort` also requires `provider`.
A Claude or ChatGPT model must belong to its provider, with or without `topic`. Claude models
start with `claude-` or equal `opus`, `sonnet`, `haiku`, or `fable`, with exact,
case-sensitive matching. Other model IDs belong to ChatGPT. A mismatch returns
one of these refusals before Torii changes settings or queues a note:

- `Send a Claude model with provider=claude, for example claude-opus-5-5.`
- `Send a ChatGPT model with provider=codex, for example gpt-6.1-sol.`

`model.parent topic=<id> provider=opencode` picks OpenCode for one chat.
It uses the account selected by `opencode.use`. An optional `model` must equal
that account's model. The stored pick has null model and effort fields.
These OpenCode refusals apply before any settings change. The off, account,
model and effort refusals also apply to `topic.create` before it queues work.

- Home chat: `The home chat stays on Claude or ChatGPT.`
- No `topic`: `Pick OpenCode for one chat with topic.`
- OpenCode off: `OpenCode is off. Turn on delegation.opencode first.`
- No active account: `Select an OpenCode account with opencode.use first.`
- With `effort`: `OpenCode chat agents have no thinking setting.`
- Wrong model: `The OpenCode model comes from the account that opencode.use selects: <model>.`

Repeat the same provider, model and effort to keep the current revision and host.
Without `topic`, the operation still requires both `provider` and `model`,
or neither to restore automatic selection. It refuses `effort` without
`topic`; global thinking stays on the owner-only `effort.parent`.

Chat agents call `model_parent` with
`{"topic":"-10042:11","message":123,"provider":"codex","model":"gpt-6-luna","effort":"low"}`.
To clear the pick, they pass `{"topic":"-10042:11","message":123}`.
Both `topic` and `message=N` are required over MCP. The message must be a saved
owner message in the asking parent's channel, as with `topic.create`. The target
can be any linked chat in the current group, including the asking chat or home.
Torii refuses a chat that is closing or closed, with `This chat is closing or closed.`
Disabled chats remain valid targets when they have no lifecycle state.
The chat moves to the new agent after its current turn and background work settle.
Worker settings stay unchanged. The tool is deferred and workers do not receive it.
Calls without `topic` are refused over MCP because the default for all chats is
the owner's to set. On the CLI, `message` is optional and does not affect the call.

Worker models, account settings, and native history
stay unchanged. A ChatGPT selection waits for ChatGPT to be available.
A chat pick's usage pause and notice apply only to that chat. Other chats
keep their normal callback pace. The status card names the running agent
and shows a pending chat pick on the same line until it applies.
A Claude selection falls back to the active ChatGPT account when every Claude
account is out. It returns to the selected Claude model after Claude recovers
and current work settles, using the available owner pick or another eligible
Claude account.

Torii keeps each live host until its turn, native background tasks, pending
receipts, and recovery work settle. It then closes native input and waits for
the host to record its exit. A host that does not exit remains pending and
keeps ownership of its native session. Torii resumes the same native session
for a model change within one provider. A provider change starts a fresh
session with channel continuity from the existing task and message records.
`settings.get` returns `parent_selection`. A successful control call confirms
the saved selection; inspect each topic's running host to confirm application.
Omit both parameters to restore automatic selection on future launches.
Claude work uses the owner's pick whenever it is available. Otherwise selection
uses the eligible account whose all-model weekly reset comes soonest, with alias
as the tie-break. `accounts.claude_auto` clears the pick. Eligible running chats
stay on their account after clearing it.
Signed-in accounts display their login email. The default Claude 98% rule, or a
local per-account reserve, applies to the 5-hour, all-model weekly, and Fable weekly meters.
`account.use` accepts a registered, signed-in, enabled Claude or Codex alias only
when the owner picks the account. A Claude pick requests an immediate live usage
check. Its optional `topic` routes the fresh result to the originating channel.
That one reading replaces cached usage even when it is lower in the same
window, and clears the hold when every meter has room. The result goes to the
originating channel, or the main chat when no channel is known. Running Claude
chats move after their turn and background tasks settle, resuming the same native
session; running workers finish where they are. A paused pick remains preferred
and takes work again when available. One main-chat notice reports each weekly
pause; 5-hour pauses have no such notice.
A Codex pick makes that account active, with existing auto-switch behavior.
`account.redeem` consumes one banked reset on the active Codex account,
only when the owner asks and a resettable meter is limited or at least 95% used.
Claude or ChatGPT runs the main chat. Without an explicit parent selection,
each new parent launch prefers an available Claude account, then
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
Telegram message ID before delivery. A missing row or another channel's message
is refused. A message in the same channel without a Telegram ID sends without a
quote and returns `quoted=false`, even when `task` is also present. Images and
split reports keep the reply.

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
JPEG, PNG and WebP files up to 10 MB go as photos and other files as documents.
`as_document=true` sends any `image` as a document with its original name and bytes.

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

## Unowned storage reports

`cleanup.stray_report` is a report-only write operation available through the
owner controls. It is off by default. `enabled=true` enables a scan at 03:00
local time and requests an initial scan. `enabled=false` disables it. `run=true`
requests a scan while enabled. With no parameters it returns the switch,
configuration and latest report. `config` accepts a JSON object with
`state_root`, `launch_agents`, `service_paths`, `results_min_bytes`, `growth_bytes`
and `keep`. The default result size limit is 1 GiB. The growth threshold is
2 GiB, and the Store keeps the last 7 reports. The accepted retention range is
1 through 31.

Only complete reports whose allocated total grows by more than the threshold
queue a short summary to the configured home chat. Reports never remove or
change candidates. See [storage report behavior](operations.md#report-folders-outside-active-ownership)
for ownership, excluded paths and measurement limits.
